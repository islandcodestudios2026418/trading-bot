"""Cross-exchange spread capture: Binance leads, OKX executes.
Binance is the deepest market — price moves there first.
When Binance mid diverges from OKX mid by >N bps, trade OKX to capture convergence.
Uses dual WebSocket streams for low-latency price comparison.
"""
import asyncio
import json
import os
import time
from collections import deque
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
# Adaptive trigger: track recent divergence history
_div_history: dict[str, deque] = {}  # symbol → recent abs(div_bps)
_binance_depth: dict[str, tuple] = {}  # symbol → (bid_depth_usd, ask_depth_usd) at top 5
# Cointegration z-score: spread mean + std for z-score trigger
_spread_history: dict[str, deque] = {}  # symbol → recent signed spread (bps)
Z_SCORE_TRIGGER = float(os.getenv("CROSS_ARB_ZSCORE", "2.0"))  # enter when |z| > 2.0
Z_SCORE_EXIT = float(os.getenv("CROSS_ARB_ZEXIT", "0.5"))  # exit when |z| < 0.5


def _symbol_to_okx(sym: str) -> str:
    """BTCUSDT → BTC-USDT"""
    return sym.replace("USDT", "") + "-USDT"


LIMIT_THRESHOLD_BPS = float(os.getenv("CROSS_ARB_LIMIT_BPS", "12"))  # use limit below this


def _execute_on_okx(inst_id: str, side: str, mid: float, div_bps: float) -> bool:
    """Execute on OKX. Limit order for small div, market for large."""
    if not os.getenv("OKX_API_KEY"):
        return False
    try:
        from okx_client import place_order
        qty = SIZE_USD / mid
        qty_str = f"{qty:.4f}" if mid > 100 else f"{qty:.2f}" if mid > 1 else f"{qty:.0f}"

        if abs(div_bps) >= LIMIT_THRESHOLD_BPS:
            # Large divergence — market order for speed
            r = place_order(inst_id, side, qty_str, order_type="market", td_mode="cash")
        else:
            # Small divergence — limit at mid for better fill
            px = f"{mid:.2f}" if mid > 100 else f"{mid:.4f}" if mid > 1 else f"{mid:.6f}"
            r = place_order(inst_id, side, qty_str, px=px, order_type="post_only", td_mode="cash")
        return r.get("code") == "0"
    except Exception as e:
        log(f"[CROSS-ARB] Exec error: {e}")
        return False


def _check_divergence(symbol: str):
    """Check if Binance-OKX spread z-score exceeds threshold (cointegration mean-reversion)."""
    global _pnl, _trades

    b_mid = _binance_mids.get(symbol, 0)
    okx_inst = _symbol_to_okx(symbol)
    o_mid = _okx_mids.get(okx_inst, 0)
    if b_mid == 0 or o_mid == 0:
        return

    if time.time() - _last_trade.get(symbol, 0) < COOLDOWN:
        return

    div_bps = (b_mid - o_mid) / o_mid * 10000

    # Track spread history for z-score calculation
    if symbol not in _spread_history:
        _spread_history[symbol] = deque(maxlen=1000)
    _spread_history[symbol].append(div_bps)

    # Track abs divergence for adaptive trigger (fallback)
    if symbol not in _div_history:
        _div_history[symbol] = deque(maxlen=500)
    _div_history[symbol].append(abs(div_bps))

    # Z-score trigger: requires enough history
    hist = _spread_history[symbol]
    if len(hist) >= 100:
        mean_spread = sum(hist) / len(hist)
        var_spread = sum((x - mean_spread) ** 2 for x in hist) / len(hist)
        std_spread = var_spread ** 0.5
        if std_spread > 0:
            z_score = (div_bps - mean_spread) / std_spread
        else:
            z_score = 0
        # Only trade when z-score exceeds threshold (statistically significant deviation)
        if abs(z_score) < Z_SCORE_TRIGGER:
            return
    else:
        # Fallback to adaptive bps trigger during warmup
        avg_div = sum(_div_history[symbol]) / len(_div_history[symbol]) if len(_div_history[symbol]) >= 50 else TRIGGER_BPS
        adaptive_trigger = max(TRIGGER_BPS, avg_div * 1.5)
        if abs(div_bps) < adaptive_trigger:
            return
        z_score = div_bps / max(1, adaptive_trigger)  # pseudo z-score

    # Depth-aware sizing: don't trade more than 30% of available depth
    depth = _binance_depth.get(symbol, (0, 0))
    side = "buy" if div_bps > 0 else "sell"
    available_depth = depth[0] if side == "buy" else depth[1]
    if available_depth > 0:
        max_size = available_depth * 0.3
        trade_size = min(SIZE_USD, max_size)
    else:
        trade_size = SIZE_USD

    ok = _execute_on_okx(okx_inst, side, o_mid, div_bps)

    est_profit = trade_size * abs(div_bps) / 10000 * 0.5
    _pnl += est_profit
    _trades += 1
    _last_trade[symbol] = time.time()
    record_trade(est_profit, source="cross")
    mode = "" if ok else "(paper) "
    log(f"[CROSS-ARB] {symbol} {side.upper()} {mode}| z={z_score:+.2f} div={div_bps:+.1f}bps | sz=${trade_size:.0f} | est=${est_profit:.4f} | total=${_pnl:.4f}")


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
                # Track depth for sizing
                bid_depth = sum(float(b[0]) * float(b[1]) for b in bids)
                ask_depth = sum(float(a[0]) * float(a[1]) for a in asks)
                _binance_depth[symbol] = (bid_depth, ask_depth)
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
