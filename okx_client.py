"""
OKX V5 API trading client — works from Taiwan.
Supports spot market making with real orders.
API keys via env vars: OKX_API_KEY, OKX_SECRET, OKX_PASSPHRASE
"""
import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone

import requests

BASE_URL = os.getenv("OKX_BASE_URL", "https://www.okx.com")
API_KEY = os.getenv("OKX_API_KEY", "")
SECRET = os.getenv("OKX_SECRET", "")
PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")
SIMULATED = os.getenv("OKX_SIMULATED", "1")  # "1" for demo trading, "0" for real


class RateLimiter:
    """Token bucket rate limiter. OKX: 20 req/2s for trade endpoints."""
    def __init__(self, rate: int = 20, per: float = 2.0):
        self.rate = rate
        self.per = per
        self.tokens = rate
        self.last = time.time()

    def acquire(self):
        """Block until a token is available."""
        now = time.time()
        elapsed = now - self.last
        self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.per))
        self.last = now
        if self.tokens < 1:
            wait = (1 - self.tokens) * (self.per / self.rate)
            time.sleep(wait)
            self.tokens = 0
        else:
            self.tokens -= 1


_rate_limiter = RateLimiter(rate=18, per=2.0)  # 18/2s (safety margin from 20)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _sign(timestamp: str, method: str, path: str, body: str = "") -> str:
    msg = f"{timestamp}{method}{path}{body}"
    mac = hmac.new(SECRET.encode(), msg.encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


def _headers(method: str, path: str, body: str = "") -> dict:
    ts = _ts()
    return {
        "OK-ACCESS-KEY": API_KEY,
        "OK-ACCESS-SIGN": _sign(ts, method, path, body),
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": PASSPHRASE,
        "x-simulated-trading": SIMULATED,
        "Content-Type": "application/json",
    }


def _get(path: str, params: dict = None) -> dict:
    url = f"{BASE_URL}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url += f"?{qs}"
        path += f"?{qs}"
    r = requests.get(url, headers=_headers("GET", path), timeout=10)
    return r.json()


def _post(path: str, body: list | dict) -> dict:
    _rate_limiter.acquire()
    body_str = json.dumps(body)
    r = requests.post(f"{BASE_URL}{path}", headers=_headers("POST", path, body_str),
                      data=body_str, timeout=10)
    return r.json()


# --- Public API (no auth) ---

def get_tickers() -> list[dict]:
    r = requests.get(f"{BASE_URL}/api/v5/market/tickers", params={"instType": "SPOT"}, timeout=10)
    return r.json().get("data", [])


def get_orderbook(inst_id: str, depth: int = 5) -> dict:
    r = requests.get(f"{BASE_URL}/api/v5/market/books",
                     params={"instId": inst_id, "sz": str(depth)}, timeout=10)
    return r.json().get("data", [{}])[0]


def scan_spreads(min_bps: float = 15) -> list[tuple[str, float, float]]:
    """Scan all OKX spot USDT pairs, return (instId, spread_bps, vol24h) sorted by spread."""
    tickers = get_tickers()
    results = []
    for t in tickers:
        inst = t.get("instId", "")
        if not inst.endswith("-USDT"):
            continue
        bid, ask = float(t.get("bidPx") or 0), float(t.get("askPx") or 0)
        if bid > 0 and ask > 0:
            spread_bps = (ask - bid) / bid * 10000
            vol = float(t.get("volCcy24h") or 0)
            if spread_bps >= min_bps and vol > 5000:
                results.append((inst, spread_bps, vol))
    return sorted(results, key=lambda x: -x[1])


# --- Authenticated API ---

def get_balance(ccy: str = "USDT") -> float:
    data = _get("/api/v5/account/balance", {"ccy": ccy})
    details = data.get("data", [{}])[0].get("details", [])
    for d in details:
        if d.get("ccy") == ccy:
            return float(d.get("availBal", 0))
    return 0.0


def place_order(inst_id: str, side: str, sz: str, px: str = None,
                order_type: str = "limit", td_mode: str = "cash",
                pos_side: str = "") -> dict:
    """Place an order. td_mode='cash' for spot, 'cross'/'isolated' for perps."""
    body = {
        "instId": inst_id,
        "tdMode": td_mode,
        "side": side,
        "ordType": "post_only" if order_type == "post_only" else order_type,
        "sz": sz,
    }
    if px and order_type != "market":
        body["px"] = px
    if pos_side:
        body["posSide"] = pos_side
    return _post("/api/v5/trade/order", body)


def cancel_order(inst_id: str, order_id: str) -> dict:
    return _post("/api/v5/trade/cancel-order", {"instId": inst_id, "ordId": order_id})


def amend_order(inst_id: str, order_id: str, new_px: str = None, new_sz: str = None) -> dict:
    """Amend an existing order's price and/or size (lower latency than cancel+replace)."""
    body = {"instId": inst_id, "ordId": order_id}
    if new_px:
        body["newPx"] = new_px
    if new_sz:
        body["newSz"] = new_sz
    return _post("/api/v5/trade/amend-order", body)


def batch_amend(orders: list[dict]) -> dict:
    """Amend up to 20 orders in one API call.
    Each: {instId, ordId, newPx(optional), newSz(optional)}
    """
    return _post("/api/v5/trade/amend-batch-orders", orders)


def batch_orders(orders: list[dict]) -> dict:
    """Place up to 20 orders in a single API call.
    Each order dict: {instId, tdMode, side, ordType, sz, px(optional), posSide(optional)}
    """
    return _post("/api/v5/trade/batch-orders", orders)


def batch_cancel(orders: list[dict]) -> dict:
    """Cancel up to 20 orders in a single API call.
    Each order dict: {instId, ordId}
    """
    return _post("/api/v5/trade/cancel-batch-orders", orders)


def cancel_all(inst_id: str) -> dict:
    orders = get_open_orders(inst_id)
    results = []
    for o in orders:
        results.append(cancel_order(inst_id, o["ordId"]))
    return {"cancelled": len(results)}


def get_open_orders(inst_id: str = None) -> list:
    params = {"instType": "SPOT"}
    if inst_id:
        params["instId"] = inst_id
    data = _get("/api/v5/trade/orders-pending", params)
    return data.get("data", [])


def get_fills(inst_id: str = None, limit: int = 20) -> list:
    params = {"instType": "SPOT", "limit": str(limit)}
    if inst_id:
        params["instId"] = inst_id
    data = _get("/api/v5/trade/fills", params)
    return data.get("data", [])


if __name__ == "__main__":
    print("Scanning OKX spot spreads (accessible from Taiwan)...")
    pairs = scan_spreads(min_bps=10)
    print(f"Found {len(pairs)} pairs with spread > 10bps:\n")
    for inst, bps, vol in pairs[:20]:
        print(f"  {inst:16s} spread={bps:6.1f}bps  vol_24h=${vol:>12,.0f}")
