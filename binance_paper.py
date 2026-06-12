"""
Binance Market Making Simulator — live data, paper trades.
No account needed. Uses public WebSocket for real-time orderbook.
Simulates two-sided quoting and tracks PnL.
"""
import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

TW_TZ = timezone(timedelta(hours=8))

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


SPREAD_BPS = 10  # our spread: 0.1% — tighter than market to get fills
MAX_POS_USD = 50
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
    prev_bid = 0.0
    prev_ask = 0.0

    async for ws in websockets.connect(ws_url, ssl=True):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                best_bid = float(msg["b"])
                best_ask = float(msg["a"])
                if best_bid <= 0 or best_ask <= 0:
                    continue

                tick_count += 1
                mkt_spread = (best_ask - best_bid) / best_bid * 100

                # Our strategy: post at best_bid and best_ask (top of book maker)
                # We get filled when price moves: new best_bid > our ask (prev_ask)
                # or new best_ask < our bid (prev_bid)
                my_bid = best_bid
                my_ask = best_ask

                if tick_count % 200 == 1:
                    print(f"  tick#{tick_count} bid={best_bid} ask={best_ask} spr={mkt_spread:.3f}% | pos=${state.position:.0f} pnl=${state.pnl:+.4f} fills={state.fills}", flush=True)

                if prev_bid > 0:
                    # Someone lifted our ask (bought from us): new bid >= our prev ask
                    if best_bid >= prev_ask and state.position >= 0 and state.position < MAX_POS_USD:
                        # We sold at prev_ask
                        state.position -= ORDER_SIZE_USD
                        state.entry_price = prev_ask
                        state.fills += 1
                        ts = datetime.now(TW_TZ).strftime("%H:%M:%S")
                        print(f"  [{ts}] SELL @ {prev_ask:.6f} | pos=${state.position:.0f}", flush=True)

                    # Someone hit our bid (sold to us): new ask <= our prev bid
                    if best_ask <= prev_bid and state.position <= 0 and state.position > -MAX_POS_USD:
                        # We bought at prev_bid
                        state.position += ORDER_SIZE_USD
                        state.entry_price = prev_bid
                        state.fills += 1
                        ts = datetime.now(TW_TZ).strftime("%H:%M:%S")
                        print(f"  [{ts}] BUY  @ {prev_bid:.6f} | pos=${state.position:.0f}", flush=True)

                    # Close position for profit when price reverts
                    if state.position < 0 and best_ask <= state.entry_price * (1 - SPREAD_BPS/10000):
                        # Cover short at lower price
                        profit = (state.entry_price - best_ask) / state.entry_price * abs(state.position)
                        state.pnl += profit
                        if profit > 0: state.wins += 1
                        state.fills += 1
                        record_trade(profit)
                        ts = datetime.now(TW_TZ).strftime("%H:%M:%S")
                        print(f"  [{ts}] COVER pnl={profit:+.4f} | Total: ${state.pnl:+.4f} | Eq: ${state.equity:.2f} | WR: {state.wr}", flush=True)
                        state.position = 0

                    if state.position > 0 and best_bid >= state.entry_price * (1 + SPREAD_BPS/10000):
                        # Sell long at higher price
                        profit = (best_bid - state.entry_price) / state.entry_price * state.position
                        state.pnl += profit
                        if profit > 0: state.wins += 1
                        state.fills += 1
                        record_trade(profit)
                        ts = datetime.now(TW_TZ).strftime("%H:%M:%S")
                        print(f"  [{ts}] CLOSE pnl={profit:+.4f} | Total: ${state.pnl:+.4f} | Eq: ${state.equity:.2f} | WR: {state.wr}", flush=True)
                        state.position = 0

                prev_bid = best_bid
                prev_ask = best_ask

        except websockets.ConnectionClosed:
            print("  Reconnecting...", flush=True)
            await asyncio.sleep(1)


if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "MBOXUSDT"
    print("Pairs with wide spreads: MBOXUSDT, HIGHUSDT, GTCUSDT, LSKUSDT, HFTUSDT")
    try:
        asyncio.run(run(symbol))
    except KeyboardInterrupt:
        print("\n\nDone.")
