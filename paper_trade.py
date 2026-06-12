"""
Paper Trading Simulator — uses live Polymarket orderbook via WebSocket.

Simulates fills: if market best_bid crosses our ask → we got filled on sell side (and vice versa).
Tracks PnL, win rate, fill count in real-time.

No wallet/API key needed. Public market data only.
"""
import asyncio
import json
import time
from dataclasses import dataclass, field

import websockets

import config

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class SimState:
    capital: float = config.CAPITAL
    pnl: float = 0.0
    fills: int = 0
    wins: int = 0
    position: float = 0.0
    entry_price: float = 0.0
    my_bid: float = 0.0
    my_ask: float = 0.0
    start_time: float = field(default_factory=time.time)

    @property
    def win_rate(self) -> str:
        return f"{(self.wins/self.fills*100):.0f}%" if self.fills else "N/A"

    @property
    def elapsed(self) -> str:
        m = int((time.time() - self.start_time) / 60)
        return f"{m}m"


def compute_quotes(mid: float, position: float) -> tuple[float, float]:
    half_spread = mid * (config.SPREAD_BPS / 10000)
    skew = (position / config.MAX_POSITION) * half_spread if config.MAX_POSITION > 0 else 0
    bid = max(0.01, min(0.99, round(mid - half_spread - skew, 2)))
    ask = max(0.01, min(0.99, round(mid + half_spread - skew, 2)))
    return bid, ask


async def run_sim(token_id: str):
    state = SimState()
    best_bid = 0.0
    best_ask = 0.0

    print(f"Paper Trading Started")
    print(f"  Token: {token_id[:16]}...")
    print(f"  Capital: ${state.capital}, Spread: {config.SPREAD_BPS}bps, Size: ${config.ORDER_SIZE_USDC}")
    print(f"  Waiting for market data...\n")

    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                await ws.send(json.dumps({
                    "assets_ids": [token_id],
                    "type": "market",
                    "custom_feature_enabled": True,
                }))

                async def heartbeat():
                    while True:
                        await ws.send("PING")
                        await asyncio.sleep(10)

                hb = asyncio.create_task(heartbeat())
                try:
                    async for raw in ws:
                        if raw == "PONG":
                            continue
                        msg = json.loads(raw)
                        evt = msg.get("event_type")

                        # Update market prices
                        if evt == "book":
                            bids = msg.get("bids", [])
                            asks = msg.get("asks", [])
                            if bids:
                                best_bid = float(bids[0]["price"])
                            if asks:
                                best_ask = float(asks[0]["price"])
                        elif evt == "price_change":
                            for pc in msg.get("price_changes", []):
                                if pc.get("best_bid"):
                                    best_bid = float(pc["best_bid"])
                                if pc.get("best_ask"):
                                    best_ask = float(pc["best_ask"])
                        else:
                            continue

                        if not best_bid or not best_ask:
                            continue

                        mid = (best_bid + best_ask) / 2

                        # Compute our quotes
                        state.my_bid, state.my_ask = compute_quotes(mid, state.position)

                        # Simulate fills:
                        # If market best_ask <= our bid → someone is selling at our bid price (we buy)
                        if best_ask <= state.my_bid and abs(state.position) < config.MAX_POSITION:
                            state.position += config.ORDER_SIZE_USDC
                            state.entry_price = state.my_bid
                            state.fills += 1
                            print(f"  ✓ BUY filled @ {state.my_bid:.2f} | pos={state.position:.0f}")

                        # If market best_bid >= our ask → someone is buying at our ask price (we sell)
                        if best_bid >= state.my_ask:
                            if state.position > 0:
                                # Closing long
                                profit = (state.my_ask - state.entry_price) * config.ORDER_SIZE_USDC
                                state.pnl += profit
                                state.position -= config.ORDER_SIZE_USDC
                                state.fills += 1
                                if profit > 0:
                                    state.wins += 1
                                print(f"  ✓ SELL filled @ {state.my_ask:.2f} | pnl={profit:+.2f} | pos={state.position:.0f}")
                            elif abs(state.position) < config.MAX_POSITION:
                                state.position -= config.ORDER_SIZE_USDC
                                state.entry_price = state.my_ask
                                state.fills += 1
                                print(f"  ✓ SELL filled @ {state.my_ask:.2f} | pos={state.position:.0f}")

                        # Stop-loss
                        if state.position != 0 and state.entry_price > 0:
                            unrealized = state.position * (mid - state.entry_price)
                            if unrealized < -config.MAX_LOSS_USDC:
                                state.pnl += unrealized
                                print(f"  ✗ STOP LOSS | loss={unrealized:.2f}")
                                state.position = 0.0
                                state.entry_price = 0.0

                        # Status every 10 fills
                        if state.fills > 0 and state.fills % 5 == 0:
                            print(f"\n  === {state.elapsed} | PnL: ${state.pnl:+.2f} | Fills: {state.fills} | WR: {state.win_rate} | Pos: {state.position:.0f} ===\n")

                finally:
                    hb.cancel()

        except (websockets.ConnectionClosed, ConnectionError):
            await asyncio.sleep(2)
        except Exception as e:
            print(f"  Error: {e}")
            await asyncio.sleep(3)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python paper_trade.py <token_id>")
        print("  Get token_ids from: python scanner.py")
        print("  (scanner needs wallet auth, but you can get token_ids from polymarket.com URL)")
        sys.exit(1)
    try:
        asyncio.run(run_sim(sys.argv[1]))
    except KeyboardInterrupt:
        print("\n\nFinal Results:")
        print("  (check output above for PnL)")
