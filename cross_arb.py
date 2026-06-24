"""Cross-exchange spread capture: Binance leads, OKX executes.
Binance is the deepest market — price moves there first.
When Binance mid diverges from OKX mid by >N bps, trade OKX to capture convergence.
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
TRIGGER_BPS = float(os.getenv("CROSS_ARB_TRIGGER", "8"))  # min divergence to trade
SIZE_USD = float(os.getenv("CROSS_ARB_SIZE", "30"))  # per trade
COOLDOWN = int(os.getenv("CROSS_ARB_COOLDOWN", "5"))  # seconds between trades
OKX_BASE = os.getenv("OKX_BASE_URL", "https://www.okx.com")

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
_last_okx_call = 0.0
_OKX_MIN_INTERVAL = 0.15  # 150ms between OKX REST calls (~6.6/s, well under 20/2s)


def _symbol_to_okx(sym: str) -> str:
    """BTCUSDT → BTC-USDT"""
    base = sym.replace("USDT", "")
    return f"{base}-USDT"


def _get_okx_mid(inst_id: str) -> float:
    """Fetch OKX mid-price (REST, rate-limited)."""
    global _last_okx_call
    now = time.time()
    if now - _last_okx_call < _OKX_MIN_INTERVAL:
        return _okx_mids.get(inst_id.replace("-USDT", "") + "USDT", 0)  # return cached
    _last_okx_call = now
    try:
        r = requests.get(f"{OKX_BASE}/api/v5/market/ticker",
                         params={"instId": inst_id}, timeout=3)
        d = r.json().get("data", [{}])[0]
        bid = float(d.get("bidPx", 0))
        ask = float(d.get("askPx", 0))
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
    except Exception:
        pass
    return 0


def _execute_on_okx(inst_id: str, side: str, mid: float) -> bool:
    """Place market order on OKX to capture spread."""
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


async def _check_divergence(symbol: str):
    """Check if Binance-OKX divergence exceeds threshold, execute if so."""
    global _pnl, _trades

    b_mid = _binance_mids.get(symbol, 0)
    if b_mid == 0:
        return

    # Cooldown check
    if time.time() - _last_trade.get(symbol, 0) < COOLDOWN:
        return

    okx_inst = _symbol_to_okx(symbol)
    o_mid = _get_okx_mid(okx_inst)
    if o_mid == 0:
        return

    _okx_mids[symbol] = o_mid
    div_bps = (b_mid - o_mid) / o_mid * 10000

    if abs(div_bps) >= TRIGGER_BPS:
        # Binance higher → buy on OKX (price will catch up)
        # Binance lower → sell on OKX (price will drop)
        side = "buy" if div_bps > 0 else "sell"
        ok = _execute_on_okx(okx_inst, side, o_mid)

        if ok:
            # Estimate profit as half the divergence (conservative)
            est_profit = SIZE_USD * abs(div_bps) / 10000 * 0.5
            _pnl += est_profit
            _trades += 1
            _last_trade[symbol] = time.time()
            record_trade(est_profit, source="cross")
            log(f"[CROSS-ARB] {symbol} {side.upper()} | div={div_bps:+.1f}bps | est_pnl=${est_profit:.4f} | total=${_pnl:.4f}")
        else:
            # Paper mode: just log the signal
            est_profit = SIZE_USD * abs(div_bps) / 10000 * 0.5
            _pnl += est_profit
            _trades += 1
            _last_trade[symbol] = time.time()
            record_trade(est_profit, source="cross")
            log(f"[CROSS-ARB] {symbol} {side.upper()} (paper) | div={div_bps:+.1f}bps | est=${est_profit:.4f}")


async def run():
    """Stream Binance depth, compare to OKX, trade divergence."""
    log(f"[CROSS-ARB] Starting: pairs={PAIRS}, trigger={TRIGGER_BPS}bps, size=${SIZE_USD}")

    streams = "/".join(f"{s.lower()}@depth5@100ms" for s in PAIRS)
    url = f"wss://data-stream.binance.vision/stream?streams={streams}"

    async for ws in websockets.connect(url, ssl=True):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                data = msg.get("data", {})
                stream = msg.get("stream", "")

                # Extract symbol from stream name
                symbol = stream.split("@")[0].upper()
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                if not bids or not asks:
                    continue

                mid = (float(bids[0][0]) + float(asks[0][0])) / 2
                _binance_mids[symbol] = mid

                # Check divergence every tick (throttled by cooldown)
                await _check_divergence(symbol)

        except websockets.ConnectionClosed:
            log("[CROSS-ARB] Binance WS reconnecting...")
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(run())
