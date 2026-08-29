from __future__ import annotations

import asyncio
import os
import threading
import time
from concurrent.futures import Future

from flask import Flask, jsonify, request

app = Flask(__name__)

# ══════════════════════════════════════════════════════════════════
#  Config
# ══════════════════════════════════════════════════════════════════

WORKERS         = int(os.getenv("WORKERS", 40))
LOAD_SHED_PCT   = float(os.getenv("LOAD_SHED_PCT", 90))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 120))

# ══════════════════════════════════════════════════════════════════
#  Event loop في thread منفصل
# ══════════════════════════════════════════════════════════════════

_loop = asyncio.new_event_loop()
_active = 0
_lock   = threading.Lock()

def _start_loop():
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

threading.Thread(target=_start_loop, daemon=True, name="async-loop").start()


def _inc():
    global _active
    with _lock:
        _active += 1

def _dec():
    global _active
    with _lock:
        _active -= 1

def _load() -> float:
    return (_active / WORKERS * 100) if WORKERS else 0


def _run(coro):
    fut: Future = Future()

    async def _w():
        try:
            fut.set_result(await coro)
        except Exception as e:
            fut.set_exception(e)

    asyncio.run_coroutine_threadsafe(_w(), _loop)
    return fut.result(timeout=REQUEST_TIMEOUT)


# ══════════════════════════════════════════════════════════════════
#  Core check
# ══════════════════════════════════════════════════════════════════

async def _check(card: str, site: str, proxy: str | None) -> dict:
    from auto_async import AsyncTLSClient, check_card_async, normalize_proxy

    proxy_url = normalize_proxy(proxy) if proxy else None
    async with AsyncTLSClient(timeout=90, proxy_url=proxy_url) as client:
        r = await check_card_async(client, card, site)

    return {
        "Gateway":   "Shopify",
        "Status":    r.status.value in (0, 1),
        "Response":  r.status_code or (str(r.error) if r.error else ""),
        "Price":     r.amount or "0.00",
        "cc":        card,
        "site":      site,
        "retryable": r.retryable,
    }


# ══════════════════════════════════════════════════════════════════
#  Load shedding
# ══════════════════════════════════════════════════════════════════

@app.before_request
def _shed():
    if request.path in ("/health", "/stats"):
        return
    pct = _load()
    if pct >= LOAD_SHED_PCT:
        return jsonify({"error": "overloaded", "load": f"{pct:.1f}%", "retry_after": 5}), 503


# ══════════════════════════════════════════════════════════════════
#  Routes
# ══════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return jsonify({"ok": True, "load": f"{_load():.1f}%"})


@app.get("/stats")
def stats():
    return jsonify({
        "workers":  WORKERS,
        "active":   _active,
        "load_pct": round(_load(), 2),
    })


@app.route("/shopify", methods=["GET", "POST"])
def shopify():
    if request.method == "GET":
        card  = request.args.get("cc", "").strip()
        site  = request.args.get("site", "").strip()
        proxy = request.args.get("proxy", "").strip() or None
    else:
        b     = request.get_json(silent=True) or {}
        card  = (b.get("cc") or b.get("card") or "").strip()
        site  = (b.get("site") or "").strip()
        proxy = (b.get("proxy") or "").strip() or None

    if not card:
        return jsonify({"error": "cc required"}), 400
    if not site:
        return jsonify({"error": "site required"}), 400

    _inc()
    t0 = time.time()
    try:
        result = _run(_check(card, site, proxy))
        result["Time"] = f"{time.time() - t0:.2f}s"
        return jsonify(result)
    except TimeoutError:
        return jsonify({"error": "timeout", "retryable": True}), 504
    except Exception as e:
        return jsonify({"error": str(e), "retryable": True}), 500
    finally:
        _dec()


# ══════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"[START] workers={WORKERS} shed_at={LOAD_SHED_PCT}% port={port}", flush=True)
    try:
        from waitress import serve
        serve(app, host="0.0.0.0", port=port, threads=WORKERS * 2)
    except ImportError:
        app.run(host="0.0.0.0", port=port, threaded=True)
