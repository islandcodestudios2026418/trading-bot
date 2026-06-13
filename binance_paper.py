"""
Multi-pair Binance Market Maker (paper) — depth stream, OFI, volatility filter.
"""
import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

import websockets

TW_TZ = timezone(timedelta(hours=8))

try:
    from arb_monitor import record_trade, log
except ImportError:
    def record_trade(p): pass
    def log(m): print(m, flush=True)

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")]
MAX_POS_USD = float(os.getenv("MAX_POS_USD", "100"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "50"))
CAPITAL = float(os.getenv("CAPITAL", "2000"))
CONSEC_LOSS_PAUSE = 5
PAUSE_SECONDS = 300


@dataclass
class PairState:
    symbol: str
    position: float = 0.0
    entry_price: float = 0.0
    pnl: float = 0.0
    fills: int = 0
    wins: int = 0
    paused_until: float = 0.0
    consec_losses: int = 0
    mid_prices: deque = field(default_factory=lambda: deque(maxlen=100))
    last_atr: float = 0.0
    last_ofi: float = 0.0
    last_spread_bps: float = 0.0

    @property
    def wr(self):
        return f"{self.wins/self.fills*100:.0f}%" if self.fills else "-"


# Global state shared with dashboard
pair_states: dict[str, PairState] = {}
daily_pnl: float = 0.0
daily_fills: int = 0
daily_wins: int = 0
_halted = False


def _halt_check() -> bool:
    global _halted
    if daily_pnl <= -DAILY_LOSS_LIMIT:
        if not _halted:
            log(f"⛔ DAILY LOSS LIMIT hit: ${daily_pnl:.2f} <= -${DAILY_LOSS_LIMIT}")
            _halted = True
        return True
    return False


def _calc_atr(prices: deque) -> float:
    """Simple ATR proxy from recent mid-price changes."""
    if len(prices) < 20:
        return 0.0
    recent = list(prices)[-20:]
    changes = [abs(recent[i] - recent[i-1]) for i in range(1, len(recent))]
    return sum(changes) / len(changes)


def _record(ps: PairState, profit: float):
    global daily_pnl, daily_fills, daily_wins
    ps.pnl += profit
    ps.fills += 1
    daily_pnl += profit
    daily_fills += 1
    if profit > 0:
        ps.wins += 1
        daily_wins += 1
        ps.consec_losses = 0
    else:
        ps.consec_losses += 1
        if ps.consec_losses >= CONSEC_LOSS_PAUSE:
            ps.paused_until = time.time() + PAUSE_SECONDS
            log(f"{ps.symbol} paused {PAUSE_SECONDS}s after {CONSEC_LOSS_PAUSE} consecutive losses")
    record_trade(profit)


async def run_pair(symbol: str):
    """Run MM for one symbol using depth5 stream."""
    ps = PairState(symbol=symbol)
    pair_states[symbol] = ps
    url = f"wss://data-stream.binance.vision/ws/{symbol.lower()}@depth5@100ms"
    log(f"[{symbol}] Connecting depth stream...")

    async for ws in websockets.connect(url, ssl=True):
        try:
            async for raw in ws:
                if _halt_check():
                    await asyncio.sleep(60)
                    continue

                if time.time() < ps.paused_until:
                    continue

                msg = json.loads(raw)
                bids = msg.get("bids", [])
                asks = msg.get("asks", [])
                if not bids or not asks:
                    continue

                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                if best_bid <= 0 or best_ask <= 0:
                    continue

                mid = (best_bid + best_ask) / 2
                ps.mid_prices.append(mid)
                ps.last_spread_bps = (best_ask - best_bid) / mid * 10000

                # OFI: sum bid qty vs ask qty across top 5 levels
                bid_vol = sum(float(b[1]) * float(b[0]) for b in bids)
                ask_vol = sum(float(a[1]) * float(a[0]) for a in asks)
                total_vol = bid_vol + ask_vol
                ps.last_ofi = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0

                # ATR volatility filter
                atr = _calc_atr(ps.mid_prices)
                ps.last_atr = atr
                if len(ps.mid_prices) >= 20:
                    # Pause if ATR spikes > 3x its recent average
                    older = list(ps.mid_prices)[-60:-20] if len(ps.mid_prices) > 60 else list(ps.mid_prices)[:20]
                    if older:
                        old_changes = [abs(older[i] - older[i-1]) for i in range(1, len(older))]
                        base_atr = sum(old_changes) / len(old_changes) if old_changes else atr
                        if base_atr > 0 and atr > base_atr * 3:
                            continue  # skip — volatility spike

                # Position sizing: inverse vol (high ATR = smaller size)
                base_size = min(MAX_POS_USD * 0.2, CAPITAL * 0.01)  # 1% capital or 20% max_pos
                if atr > 0 and mid > 0:
                    vol_ratio = (atr / mid) * 10000  # vol in bps
                    size = base_size / max(1, vol_ratio / 5)
                    size = max(5, min(size, MAX_POS_USD * 0.3))
                else:
                    size = base_size

                # OFI-biased entry: only enter in OFI direction
                # OFI > 0.3 = strong bid pressure → buy bias
                # OFI < -0.3 = strong ask pressure → sell bias
                ofi_threshold = 0.3

                if ps.position == 0:
                    # Open position based on OFI
                    if ps.last_ofi > ofi_threshold and abs(ps.position) < MAX_POS_USD:
                        ps.position = size
                        ps.entry_price = best_ask  # we buy at ask
                        log(f"[{symbol}] BUY ${size:.0f} @ {best_ask} OFI={ps.last_ofi:.2f}")
                    elif ps.last_ofi < -ofi_threshold and abs(ps.position) < MAX_POS_USD:
                        ps.position = -size
                        ps.entry_price = best_bid  # we sell at bid
                        log(f"[{symbol}] SELL ${size:.0f} @ {best_bid} OFI={ps.last_ofi:.2f}")

                elif ps.position > 0:
                    # Long: take profit at 1 spread or stop at 2 spreads
                    spread = best_ask - best_bid
                    pnl_per_unit = (best_bid - ps.entry_price)
                    if pnl_per_unit >= spread * 0.8:
                        profit = pnl_per_unit / ps.entry_price * ps.position
                        _record(ps, profit)
                        log(f"[{symbol}] CLOSE_LONG +${profit:.4f} (total ${ps.pnl:.4f} wr={ps.wr})")
                        ps.position = 0
                    elif pnl_per_unit <= -spread * 2:
                        profit = pnl_per_unit / ps.entry_price * ps.position
                        _record(ps, profit)
                        log(f"[{symbol}] STOP_LONG ${profit:.4f} (total ${ps.pnl:.4f})")
                        ps.position = 0

                elif ps.position < 0:
                    spread = best_ask - best_bid
                    pnl_per_unit = (ps.entry_price - best_ask)
                    if pnl_per_unit >= spread * 0.8:
                        profit = pnl_per_unit / ps.entry_price * abs(ps.position)
                        _record(ps, profit)
                        log(f"[{symbol}] COVER_SHORT +${profit:.4f} (total ${ps.pnl:.4f} wr={ps.wr})")
                        ps.position = 0
                    elif pnl_per_unit <= -spread * 2:
                        profit = pnl_per_unit / ps.entry_price * abs(ps.position)
                        _record(ps, profit)
                        log(f"[{symbol}] STOP_SHORT ${profit:.4f} (total ${ps.pnl:.4f})")
                        ps.position = 0

        except websockets.ConnectionClosed:
            log(f"[{symbol}] Reconnecting...")
            await asyncio.sleep(2)


async def run():
    log(f"Multi-pair MM starting: {SYMBOLS}")
    log(f"  Max pos/pair: ${MAX_POS_USD}, Daily loss limit: -${DAILY_LOSS_LIMIT}, Capital: ${CAPITAL}")
    await asyncio.gather(*[run_pair(s) for s in SYMBOLS])


if __name__ == "__main__":
    asyncio.run(run())
