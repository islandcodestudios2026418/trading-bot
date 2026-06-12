"""
Binance Market Making Simulator — live data, paper trades.
No account needed. Uses public WebSocket for real-time orderbook.
Simulates two-sided quoting and tracks PnL.
"""
import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import websockets

try:
    from arb_monitor import record_trade
except ImportError:
    def record_trade(p): pass


@dataclass
class SimState:
    capital: float = 500.0
    pnl: float = 0.0
    fills: int = 0
    wins: int = 0
    position: float = 0.0
    entry_price: float = 0.0
    start: float = field(default_factory=time.time)

    @property
    def equity(self): return self.capital + self.pnl

    @property
    def elapsed(self): return f"{int((time.time()-self.start)/60)}m"

    @property
    def wr(self): return f"{self.wins/self.fills*100:.0f}%" if self.fills else "-"


SPREAD_BPS = 50  # our spread: 0.5% (half each side = 0.25%)
MAX_POS_USD = 50  # max $50 position
ORDER_SIZE_USD = 10


async def run(symbol: str):
    state = SimState()
    ws_url = f"wss://data-stream.binance.vision/ws/{symbol.lower()}@bookTicker"

    print(f"Binance Paper Trading: {symbol.upper()}", flush=True)
    print(f"  Spread: {SPREAD_BPS}bps, Size: ${ORDER_SIZE_USD}, Max pos: ${MAX_POS_USD}", flush=True)
    print(f"  Connecting to live orderbook...\n", flush=True)

    last_mid = 0.0
    my_bid = 0.0
    my_ask = 0.0
    tick_count = 0

    async for ws in websockets.connect(ws_url, ssl=True):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                best_bid = float(msg["b"])
                best_ask = float(msg["a"])
                if best_bid <= 0 or best_ask <= 0:
                    continue

                mid = (best_bid + best_ask) / 2
                half_spread = mid * (SPREAD_BPS / 20000)

                # Our quotes
                my_bid = mid - half_spread
                my_ask = mid + half_spread
                tick_count += 1

                # Print status every 100 ticks
                if tick_count % 100 == 1:
                    mkt_spread = (best_ask - best_bid) / best_bid * 100
                    print(f"  tick#{tick_count} mkt={best_bid}/{best_ask} spr={mkt_spread:.2f}% | my_bid={my_bid:.6f} my_ask={my_ask:.6f} | pos=${state.position:.0f} pnl=${state.pnl:+.4f}", flush=True)

                # Simulate fill: market crosses our price
                # Buy fill: market ask <= our bid (someone sells to us)
                if best_ask <= my_bid and abs(state.position) < MAX_POS_USD:
                    qty = ORDER_SIZE_USD / my_bid
                    state.position += ORDER_SIZE_USD
                    state.entry_price = my_bid
                    state.fills += 1

                # Sell fill: market bid >= our ask
                if best_bid >= my_ask:
                    if state.position > 0:
                        profit = (my_ask - state.entry_price) / state.entry_price * ORDER_SIZE_USD
                        state.pnl += profit
                        state.position -= ORDER_SIZE_USD
                        state.fills += 1
                        if profit > 0:
                            state.wins += 1
                        record_trade(profit)
                        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                        print(f"  [{ts}] FILL #{state.fills} pnl={profit:+.4f} | Total: ${state.pnl:+.4f} | Equity: ${state.equity:.2f} | WR: {state.wr}", flush=True)
                    elif state.position > -MAX_POS_USD:
                        state.position -= ORDER_SIZE_USD
                        state.entry_price = my_ask
                        state.fills += 1

                # Buy back short
                if state.position < 0 and best_ask <= my_bid:
                    profit = (state.entry_price - my_bid) / state.entry_price * ORDER_SIZE_USD
                    state.pnl += profit
                    state.position += ORDER_SIZE_USD
                    state.fills += 1
                    if profit > 0:
                        state.wins += 1
                    record_trade(profit)
                    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    print(f"  [{ts}] FILL #{state.fills} pnl={profit:+.4f} | Total: ${state.pnl:+.4f} | Equity: ${state.equity:.2f} | WR: {state.wr}", flush=True)

                # Status every 30s
                if last_mid == 0 or abs(mid - last_mid) / last_mid > 0.001:
                    last_mid = mid

        except websockets.ConnectionClosed:
            print("  Reconnecting...")
            await asyncio.sleep(1)


if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "MBOXUSDT"
    print("Pairs with wide spreads: MBOXUSDT, HIGHUSDT, GTCUSDT, LSKUSDT, HFTUSDT")
    try:
        asyncio.run(run(symbol))
    except KeyboardInterrupt:
        print("\n\nDone.")
