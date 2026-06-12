"""
Polymarket HFT Market Maker v2 — speed-optimized.

Optimizations:
1. Precomputed quote table (no math on hot path)
2. Parallel cancel + post (don't wait for cancel response)
3. Persistent HTTP session (connection reuse)
4. Fire-and-forget requote (skip if one already in flight)
"""
import asyncio
import json
import traceback
from concurrent.futures import ThreadPoolExecutor

import requests
import websockets
from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side

import config
from client import get_client

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Precompute quote table: for each possible mid (0.01 to 0.99, step 0.01), store bid/ask
QUOTE_TABLE: dict[int, tuple[float, float]] = {}  # key = int(mid * 100)
for _mid_cents in range(1, 100):
    _mid = _mid_cents / 100
    _hs = _mid * (config.SPREAD_BPS / 10000)
    _bid = max(0.01, round(_mid - _hs, 2))
    _ask = min(0.99, round(_mid + _hs, 2))
    QUOTE_TABLE[_mid_cents] = (_bid, _ask)


def lookup_quotes(mid: float, position: float) -> tuple[float, float]:
    """O(1) quote lookup with inventory skew applied."""
    mid_cents = max(1, min(99, round(mid * 100)))
    bid, ask = QUOTE_TABLE[mid_cents]
    if position != 0 and config.MAX_POSITION > 0:
        skew = (position / config.MAX_POSITION) * (mid * config.SPREAD_BPS / 10000)
        bid = max(0.01, round(bid - skew, 2))
        ask = min(0.99, round(ask - skew, 2))
    return bid, ask


class MarketMaker:
    def __init__(self, token_ids: list[str]):
        self.token_ids = token_ids
        self.client = get_client()
        # Patch client's session for keep-alive (connection reuse)
        self.client.session = requests.Session()
        self.best_bid: dict[str, float] = {}
        self.best_ask: dict[str, float] = {}
        self.positions: dict[str, float] = {t: 0.0 for t in token_ids}
        self.entry_prices: dict[str, float] = {t: 0.0 for t in token_ids}
        self.tick_size: dict[str, str] = {t: "0.01" for t in token_ids}
        self._pool = ThreadPoolExecutor(max_workers=6)
        self._inflight = False
        self._loop: asyncio.AbstractEventLoop = None

    def _requote_sync(self):
        """Cancel + post in parallel threads for speed."""
        import concurrent.futures

        # Fire cancel (don't wait)
        cancel_fut = self._pool.submit(self._safe_cancel)

        # Compute new orders immediately (while cancel is in flight)
        orders = []
        for token_id in self.token_ids:
            bb = self.best_bid.get(token_id)
            ba = self.best_ask.get(token_id)
            if not bb or not ba:
                continue
            mid = (bb + ba) / 2
            pos = self.positions[token_id]

            # Stop-loss
            if pos != 0 and self.entry_prices[token_id] > 0:
                pnl = pos * (mid - self.entry_prices[token_id])
                if pnl < -config.MAX_LOSS_USDC:
                    print(f"  [{token_id[:8]}] STOP LOSS ${pnl:.2f}")
                    side = Side.SELL if pos > 0 else Side.BUY
                    orders.append((token_id, mid, side, abs(pos)))
                    continue

            bid, ask = lookup_quotes(mid, pos)

            if abs(pos) >= config.MAX_POSITION:
                side = Side.SELL if pos > 0 else Side.BUY
                price = ask if pos > 0 else bid
                orders.append((token_id, price, side, config.ORDER_SIZE_USDC))
            else:
                orders.append((token_id, bid, Side.BUY, config.ORDER_SIZE_USDC))
                orders.append((token_id, ask, Side.SELL, config.ORDER_SIZE_USDC))
                print(f"  [{token_id[:8]}] mid={mid:.3f} b={bid} a={ask}")

        # Wait for cancel to finish, then blast orders in parallel
        cancel_fut.result()
        futs = [self._pool.submit(self._order, *o) for o in orders]
        concurrent.futures.wait(futs)

    def _safe_cancel(self):
        try:
            self.client.cancel_all()
        except Exception:
            pass

    def _order(self, token_id, price, side, size):
        try:
            self.client.create_and_post_order(
                order_args=OrderArgs(token_id=token_id, price=price, side=side, size=size),
                options=PartialCreateOrderOptions(tick_size=self.tick_size[token_id]),
                order_type=OrderType.GTC,
            )
        except Exception as e:
            print(f"  Order err: {e}")

    async def _requote(self):
        if self._inflight:
            return  # skip — don't queue stale quotes
        self._inflight = True
        try:
            await self._loop.run_in_executor(None, self._requote_sync)
        finally:
            self._inflight = False

    async def run(self):
        self._loop = asyncio.get_event_loop()
        print(f"HFT Bot v2. Markets: {len(self.token_ids)}, spread: {config.SPREAD_BPS}bps")
        print(f"  Risk: max ${config.MAX_LOSS_USDC:.0f} loss/position ({config.MAX_LOSS_PCT}%)")
        print(f"  Quote table precomputed for 99 price levels")

        while True:
            try:
                async with websockets.connect(WS_URL) as ws:
                    await ws.send(json.dumps({
                        "assets_ids": self.token_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }))
                    print("  WS connected")

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

                            if evt == "book":
                                tid = msg.get("asset_id")
                                if tid in self.token_ids:
                                    bids = msg.get("bids", [])
                                    asks = msg.get("asks", [])
                                    if bids:
                                        self.best_bid[tid] = float(bids[0]["price"])
                                    if asks:
                                        self.best_ask[tid] = float(asks[0]["price"])
                                    asyncio.create_task(self._requote())

                            elif evt == "price_change":
                                changed = False
                                for pc in msg.get("price_changes", []):
                                    tid = pc.get("asset_id")
                                    if tid not in self.token_ids:
                                        continue
                                    if pc.get("best_bid"):
                                        self.best_bid[tid] = float(pc["best_bid"])
                                        changed = True
                                    if pc.get("best_ask"):
                                        self.best_ask[tid] = float(pc["best_ask"])
                                        changed = True
                                if changed:
                                    asyncio.create_task(self._requote())

                            elif evt == "tick_size_change":
                                tid = msg.get("asset_id")
                                if tid in self.token_ids:
                                    self.tick_size[tid] = msg["new_tick_size"]

                            elif evt == "last_trade_price":
                                print(f"  ★ {msg.get('side')} {msg.get('size')}@{msg.get('price')}")
                    finally:
                        hb.cancel()

            except (websockets.ConnectionClosed, ConnectionError) as e:
                print(f"  WS dropped: {e}")
                await asyncio.sleep(1)
            except Exception as e:
                print(f"  Error: {e}")
                traceback.print_exc()
                await asyncio.sleep(2)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python market_maker_ws.py <token_id> [token_id2 ...]")
        sys.exit(1)
    mm = MarketMaker(sys.argv[1:])
    try:
        asyncio.run(mm.run())
    except KeyboardInterrupt:
        print("\nShutting down...")
        mm._safe_cancel()
