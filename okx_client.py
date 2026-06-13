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
                order_type: str = "limit") -> dict:
    """Place a spot order. side='buy'|'sell', order_type='limit'|'market'|'post_only'"""
    body = {
        "instId": inst_id,
        "tdMode": "cash",
        "side": side,
        "ordType": "post_only" if order_type == "post_only" else order_type,
        "sz": sz,
    }
    if px and order_type != "market":
        body["px"] = px
    return _post("/api/v5/trade/order", body)


def cancel_order(inst_id: str, order_id: str) -> dict:
    return _post("/api/v5/trade/cancel-order", {"instId": inst_id, "ordId": order_id})


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
