"""Cross-exchange spread capture: Binance leads, OKX executes.
Binance is the deepest market — price moves there first.
When Binance mid diverges from OKX mid by >N bps, trade OKX to capture convergence.
Uses dual WebSocket streams for low-latency price comparison.
"""
import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta

import websockets
import requests

TW_TZ = timezone(timedelta(hours=8))

# Config
PAIRS = [s.strip() for s in os.getenv("CROSS_ARB_PAIRS", "BTCUSDT,ETHUSDT").split(",")]
TRIGGER_BPS = float(os.getenv("CROSS_ARB_TRIGGER", "8"))
SIZE_USD = float(os.getenv("CROSS_ARB_SIZE", "30"))
COOLDOWN = int(os.getenv("CROSS_ARB_COOLDOWN", "5"))
OKX_BASE = os.getenv("OKX_BASE_URL", "https://www.okx.com")
OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"

try:
    from arb_monitor import log, record_trade
except ImportError:
    def log(m): print(f"[{datetime.now(TW_TZ).strftime('%H:%M:%S')}] {m}", flush=True)
    def record_trade(p, source="cross"): pass

try:
    from telegram_alerts import send as tg_send
except ImportError:
    def tg_send(m): pass

# State
_binance_mids: dict[str, float] = {}
_okx_mids: dict[str, float] = {}
_last_trade: dict[str, float] = {}
_pnl = 0.0
_trades = 0


def _symbol_to_okx(sym: str) -> str:
    """BTCUSDT → BTC-USDT"""
    return sym.replace("USDT", "") + "-USDT"


def _execute_on_okx(inst_id: str, side: str, mid: float) -> bool:
    """Place market order on OKX."""
    if not os.getenv("OKX_API_KEY"):
        return False
    try:
        from okx_client import place_order
        qty = SIZE_USD / mid
        qty_str = f"{qty:.4f}" if mid > 100 else f"{qty:.2f}" if mid > 1 else f"{qty:.0f}"
        r = place_order(inst_id, side, qty_str, order_type="market", td_mode="cash")
        return r.get("code") == "0"
    except Exception as e:
        log(f"[CROSS-ARB] Exec error: {e}")
        return False


def _check_divergence(symbol: str):
    """Check if Binance-OKX divergence exceeds threshold."""
    global _pnl, _trades

    b_mid = _binance_mids.get(symbol, 0)
    okx_inst = _symbol_to_okx(symbol)
    o_mid = _okx_mids.get(okx_inst, 0)
    if b_mid == 0 or o_mid == 0:
        return

    if time.time() - _last_trade.get(symbol, 0) < COOLDOWN:
        return

    div_bps = (b_mid - o_mid) / o_mid * 10000
    if abs(div_bps) < TRIGGER_BPS:
        return

    side = "buy" if div_bps > 0 else "sell"
    ok = _execute_on_okx(okx_inst, side, o_mid)

    est_profit = SIZE_USD * abs(div_bps) / 10000 * 0.5
    _pnl += est_profit
    _trades += 1
    _last_trade[symbol] = time.time()
    record_trade(est_profit, source="cross")
    mode = "" if ok else "(paper) "
    log(f"[CROSS-ARB] {symbol} {side.upper()} {mode}| div={div_bps:+.1f}bps | est=${est_profit:.4f} | total=${_pnl:.4f}")


async def _stream_okx():
    """Stream OKX tickers via WebSocket."""
    okx_insts = [{"channel": "tickers", "instId": _symbol_to_okx(s)} for s in PAIRS]
    while True:
        try:
            async with websockets.connect(OKX_WS_PUBLIC, ssl=True) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": okx_insts}))
                log(f"[CROSS-ARB] OKX WS connected: {[_symbol_to_okx(s) for s in PAIRS]}")
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data", [{}])
                    if not data:
                        continue
                    for d in data:
                        inst = d.get("instId", "")
                        bid = float(d.get("bidPx", 0))
                        ask = float(d.get("askPx", 0))
                        if bid > 0 and ask > 0:
                            _okx_mids[inst] = (bid + ask) / 2
        except Exception as e:
            log(f"[CROSS-ARB] OKX WS error: {e}, reconnecting...")
            await asyncio.sleep(3)


async def _stream_binance():
    """Stream Binance depth and check divergence on each tick."""
    streams = "/".join(f"{s.lower()}@depth5@100ms" for s in PAIRS)
    url = f"wss://data-stream.binance.vision/stream?streams={streams}"

    async for ws in websockets.connect(url, ssl=True):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                data = msg.get("data", {})
                stream = msg.get("stream", "")
                symbol = stream.split("@")[0].upper()
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                if not bids or not asks:
                    continue
                mid = (float(bids[0][0]) + float(asks[0][0])) / 2
                _binance_mids[symbol] = mid
                _check_divergence(symbol)
        except websockets.ConnectionClosed:
            log("[CROSS-ARB] Binance WS reconnecting...")
            await asyncio.sleep(2)


async def run():
    """Run both WS streams in parallel."""
    log(f"[CROSS-ARB] Starting: pairs={PAIRS}, trigger={TRIGGER_BPS}bps, size=${SIZE_USD}")
    await asyncio.gather(_stream_okx(), _stream_binance())


if __name__ == "__main__":
    asyncio.run(run())
