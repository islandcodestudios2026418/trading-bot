"""
Multi-pair market maker — monitors top N widest-spread pairs on Binance,
trades them simultaneously with inventory management.
"""
import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

import websockets

TW_TZ = timezone(timedelta(hours=8))

try:
    from arb_monitor import record_trade
except ImportError:
    def record_trade(p): pass


@dataclass
class PairState:
    symbol: str
    position: float = 0.0
    entry_price: float = 0.0
    pnl: float = 0.0
    fills: int = 0
    prev_bid: float = 0.0
    prev_ask: float = 0.0
    last_spread_bps: float = 0.0


@dataclass
class MultiMMState:
    capital: float = 500.0
    total_pnl: float = 0.0
    pairs: dict = field(default_factory=dict)
    start: float = field(default_factory=time.time)

    @property
    def equity(self): return self.capital + self.total_pnl


# Config
TOP_N = 5  # trade top 5 widest spread pairs
ORDER_SIZE_USD = 10
MAX_POS_PER_PAIR = 30
SCAN_INTERVAL = 300  # re-scan for best pairs every 5 min
MIN_SPREAD_BPS = 15  # minimum spread to trade (0.15%)


def log(msg):
    ts = datetime.now(TW_TZ).strftime("%H:%M:%S")
    print(f"[{ts}] MM: {msg}", flush=True)


async def scan_spreads() -> list[tuple[str, float]]:
    """Connect to all tickers stream, collect spreads for 5 seconds, return sorted."""
    spreads = {}
    url = "wss://data-stream.binance.vision/ws/!bookTicker"
    try:
        async with websockets.connect(url, ssl=True) as ws:
            deadline = time.time() + 5
            async for raw in ws:
                if time.time() > deadline:
                    break
                msg = json.loads(raw)
                bid, ask = float(msg.get("b", 0)), float(msg.get("a", 0))
                if bid > 0 and ask > 0:
                    sym = msg["s"]
                    # Filter: USDT pairs only, skip stablecoins
                    if sym.endswith("USDT") and sym not in ("BUSDUSDT", "USDCUSDT", "TUSDUSDT", "EURUSDT"):
                        spread_bps = (ask - bid) / bid * 10000
                        spreads[sym] = spread_bps
    except Exception as e:
        log(f"Scan error: {e}")

    ranked = sorted(spreads.items(), key=lambda x: -x[1])
    return [(s, bps) for s, bps in ranked if bps >= MIN_SPREAD_BPS][:TOP_N]


async def trade_pair(state: MultiMMState, symbol: str):
    """Market-make a single pair via WebSocket."""
    ps = state.pairs.setdefault(symbol, PairState(symbol=symbol))
    url = f"wss://data-stream.binance.vision/ws/{symbol.lower()}@bookTicker"

    async for ws in websockets.connect(url, ssl=True):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                bid, ask = float(msg["b"]), float(msg["a"])
                if bid <= 0 or ask <= 0:
                    continue

                ps.last_spread_bps = (ask - bid) / bid * 10000

                if ps.prev_bid > 0:
                    # Fill detection: price crossed our levels
                    if bid >= ps.prev_ask and ps.position > -MAX_POS_PER_PAIR:
                        ps.position -= ORDER_SIZE_USD
                        ps.entry_price = ps.prev_ask
                        ps.fills += 1

                    if ask <= ps.prev_bid and ps.position < MAX_POS_PER_PAIR:
                        ps.position += ORDER_SIZE_USD
                        ps.entry_price = ps.prev_bid
                        ps.fills += 1

                    # Take profit at 1 spread width
                    target_bps = max(ps.last_spread_bps * 0.8, 5)
                    if ps.position > 0 and bid >= ps.entry_price * (1 + target_bps / 10000):
                        profit = (bid - ps.entry_price) / ps.entry_price * ps.position
                        ps.pnl += profit
                        state.total_pnl += profit
                        record_trade(profit)
                        log(f"{symbol} CLOSE_LONG +${profit:.4f} (total ${state.total_pnl:+.4f})")
                        ps.position = 0

                    if ps.position < 0 and ask <= ps.entry_price * (1 - target_bps / 10000):
                        profit = (ps.entry_price - ask) / ps.entry_price * abs(ps.position)
                        ps.pnl += profit
                        state.total_pnl += profit
                        record_trade(profit)
                        log(f"{symbol} COVER_SHORT +${profit:.4f} (total ${state.total_pnl:+.4f})")
                        ps.position = 0

                ps.prev_bid = bid
                ps.prev_ask = ask

        except websockets.ConnectionClosed:
            await asyncio.sleep(1)
        except Exception:
            break  # exit to allow pair rotation


async def run():
    """Main loop: scan for best pairs, trade them, rotate every SCAN_INTERVAL."""
    state = MultiMMState()
    log(f"Multi-pair MM starting. Capital: ${state.capital}")

    while True:
        log("Scanning for widest spread pairs...")
        top_pairs = await scan_spreads()
        if not top_pairs:
            log("No pairs found above threshold, retrying in 30s...")
            await asyncio.sleep(30)
            continue

        symbols = [s for s, _ in top_pairs]
        log(f"Trading {len(symbols)} pairs: {', '.join(f'{s}({bps:.0f}bps)' for s, bps in top_pairs)}")

        # Trade all pairs concurrently with a timeout for rotation
        tasks = [asyncio.create_task(trade_pair(state, sym)) for sym in symbols]
        await asyncio.sleep(SCAN_INTERVAL)

        # Cancel and rotate
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Status report
        active = [ps for ps in state.pairs.values() if ps.fills > 0]
        log(f"Rotation: equity=${state.equity:.2f} pnl=${state.total_pnl:+.4f} fills={sum(p.fills for p in active)}")


if __name__ == "__main__":
    asyncio.run(run())
