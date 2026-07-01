"""
Order Book Reconstruction — full L2 book from Binance depth diff stream.
Replaces snapshot-only approach with incremental updates for:
- Lower latency (diff = only changes, not full 20 levels every time)
- Higher accuracy (maintains full book state between snapshots)
- Deeper visibility (can track up to 100+ levels)

Protocol:
1. Get initial depth snapshot via REST
2. Subscribe to diff stream (depthUpdate@100ms)
3. Apply diffs incrementally, maintaining sorted price levels
4. Periodic snapshot reconciliation to prevent drift

No numpy/pandas — pure Python sorted list for lean deployment.
"""
import asyncio
import bisect
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import websockets

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")]
SNAPSHOT_INTERVAL = 60  # re-snapshot every 60s to correct drift
MAX_LEVELS = 50  # maintain top 50 levels each side


@dataclass
class PriceLevel:
    """Single price level in the order book."""
    price: float
    qty: float


@dataclass
class L2OrderBook:
    """Full L2 order book maintained from diff stream.
    Bids sorted descending, asks sorted ascending.
    """
    symbol: str
    bids: list = field(default_factory=list)  # [(price, qty)] sorted by price DESC
    asks: list = field(default_factory=list)  # [(price, qty)] sorted by price ASC
    last_update_id: int = 0
    last_update_time: float = 0.0
    snapshot_time: float = 0.0
    _initialized: bool = False
    _buffered_events: deque = field(default_factory=lambda: deque(maxlen=500))

    # Analytics
    update_count: int = 0
    drift_corrections: int = 0
    avg_update_latency_ms: float = 0.0
    _latency_ema_alpha: float = 0.05

    @property
    def mid(self) -> float:
        if self.bids and self.asks:
            return (self.bids[0][0] + self.asks[0][0]) / 2
        return 0.0

    @property
    def spread_bps(self) -> float:
        if self.bids and self.asks and self.mid > 0:
            return (self.asks[0][0] - self.bids[0][0]) / self.mid * 10000
        return 0.0

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    def depth_at_bps(self, bps: float) -> tuple:
        """Total bid/ask USD volume within N bps of mid."""
        if not self.mid:
            return 0.0, 0.0
        low = self.mid * (1 - bps / 10000)
        high = self.mid * (1 + bps / 10000)
        bid_depth = sum(p * q for p, q in self.bids if p >= low)
        ask_depth = sum(p * q for p, q in self.asks if p <= high)
        return bid_depth, ask_depth

    def apply_snapshot(self, bids: list, asks: list, last_update_id: int):
        """Initialize or reset from REST snapshot."""
        self.bids = sorted([(float(b[0]), float(b[1])) for b in bids if float(b[1]) > 0],
                          key=lambda x: -x[0])[:MAX_LEVELS]
        self.asks = sorted([(float(a[0]), float(a[1])) for a in asks if float(a[1]) > 0],
                          key=lambda x: x[0])[:MAX_LEVELS]
        self.last_update_id = last_update_id
        self.snapshot_time = time.time()
        self._initialized = True

        # Apply buffered events that came after snapshot
        while self._buffered_events:
            event = self._buffered_events.popleft()
            if event["u"] <= last_update_id:
                continue  # skip events before snapshot
            if event["U"] <= last_update_id + 1:
                self._apply_diff(event)

    def apply_diff(self, event: dict):
        """Apply a depth diff update.
        event format: {U: first_update_id, u: final_update_id, b: [[price,qty],...], a: [[price,qty],...]}
        """
        if not self._initialized:
            self._buffered_events.append(event)
            return

        # Validate sequence
        first_id = event.get("U", 0)
        final_id = event.get("u", 0)

        if final_id <= self.last_update_id:
            return  # stale, skip

        # Check for gap (missed events)
        if first_id > self.last_update_id + 1:
            # Gap detected — need re-snapshot
            self._initialized = False
            self.drift_corrections += 1
            return

        self._apply_diff(event)
        self.update_count += 1
        self.last_update_time = time.time()

    def _apply_diff(self, event: dict):
        """Apply bid/ask changes from diff event."""
        final_id = event.get("u", 0)
        event_time = event.get("E", 0)

        # Track latency
        if event_time:
            latency_ms = time.time() * 1000 - event_time
            self.avg_update_latency_ms += self._latency_ema_alpha * (latency_ms - self.avg_update_latency_ms)

        # Apply bid updates
        for b in event.get("b", []):
            price = float(b[0])
            qty = float(b[1])
            self._update_bids(price, qty)

        # Apply ask updates
        for a in event.get("a", []):
            price = float(a[0])
            qty = float(a[1])
            self._update_asks(price, qty)

        self.last_update_id = final_id

        # Trim to max levels
        if len(self.bids) > MAX_LEVELS:
            self.bids = self.bids[:MAX_LEVELS]
        if len(self.asks) > MAX_LEVELS:
            self.asks = self.asks[:MAX_LEVELS]

    def _update_bids(self, price: float, qty: float):
        """Update or insert a bid level. qty=0 means remove."""
        # Binary search for price in descending-sorted bids
        # We store bids as [(price, qty)] sorted descending
        idx = None
        for i, (p, q) in enumerate(self.bids):
            if abs(p - price) < 1e-10:  # found existing level
                idx = i
                break
            if p < price:  # insert position (price is higher, should go before)
                idx = -i  # negative means insert at i
                break

        if idx is not None and idx >= 0:
            # Found existing level
            if qty == 0:
                self.bids.pop(idx)
            else:
                self.bids[idx] = (price, qty)
        elif idx is not None and idx < 0:
            # Insert at position -idx
            if qty > 0:
                self.bids.insert(-idx, (price, qty))
        else:
            # Append at end (lowest bid)
            if qty > 0:
                self.bids.append((price, qty))

    def _update_asks(self, price: float, qty: float):
        """Update or insert an ask level. qty=0 means remove."""
        idx = None
        for i, (p, q) in enumerate(self.asks):
            if abs(p - price) < 1e-10:
                idx = i
                break
            if p > price:  # insert position (price is lower, should go before)
                idx = -i
                break

        if idx is not None and idx >= 0:
            if qty == 0:
                self.asks.pop(idx)
            else:
                self.asks[idx] = (price, qty)
        elif idx is not None and idx < 0:
            if qty > 0:
                self.asks.insert(-idx, (price, qty))
        else:
            if qty > 0:
                self.asks.append((price, qty))

    def get_bids_list(self, n: int = 20) -> list:
        """Get top N bid levels as [[price_str, qty_str], ...] for compatibility."""
        return [[str(p), str(q)] for p, q in self.bids[:n]]

    def get_asks_list(self, n: int = 20) -> list:
        """Get top N ask levels as [[price_str, qty_str], ...] for compatibility."""
        return [[str(p), str(q)] for p, q in self.asks[:n]]

    def needs_snapshot(self) -> bool:
        """Check if a fresh snapshot is needed (drift correction or periodic)."""
        if not self._initialized:
            return True
        if time.time() - self.snapshot_time > SNAPSHOT_INTERVAL:
            return True
        return False

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        return {
            "symbol": self.symbol,
            "mid": round(self.mid, 2),
            "spread_bps": round(self.spread_bps, 2),
            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),
            "update_count": self.update_count,
            "drift_corrections": self.drift_corrections,
            "latency_ms": round(self.avg_update_latency_ms, 1),
            "last_update_id": self.last_update_id,
            "initialized": self._initialized,
        }


class L2BookManager:
    """Manages L2 order books for multiple symbols.
    Handles WS subscription to diff stream + periodic REST snapshots.
    """

    def __init__(self, symbols: list = None):
        self.symbols = symbols or SYMBOLS
        self.books: dict[str, L2OrderBook] = {}
        for sym in self.symbols:
            self.books[sym] = L2OrderBook(symbol=sym)
        self._running = False

    async def get_snapshot(self, symbol: str) -> dict:
        """Fetch depth snapshot via REST (aiohttp-free, using websockets hack)."""
        import urllib.request
        import ssl
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=50"
        ctx = ssl.create_default_context()
        loop = asyncio.get_event_loop()
        try:
            def _fetch():
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
                    return json.loads(resp.read().decode())
            data = await loop.run_in_executor(None, _fetch)
            return data
        except Exception as e:
            return {}

    async def initialize_book(self, symbol: str):
        """Get initial snapshot for a symbol."""
        data = await self.get_snapshot(symbol)
        if data and "bids" in data:
            self.books[symbol].apply_snapshot(
                data["bids"], data["asks"], data.get("lastUpdateId", 0)
            )

    async def snapshot_loop(self):
        """Periodically re-snapshot all books to correct drift."""
        while self._running:
            await asyncio.sleep(SNAPSHOT_INTERVAL)
            for sym in self.symbols:
                if self.books[sym].needs_snapshot():
                    await self.initialize_book(sym)
                    await asyncio.sleep(0.5)  # rate limit

    async def diff_stream(self):
        """Subscribe to combined depth diff stream for all symbols."""
        streams = "/".join(f"{s.lower()}@depth@100ms" for s in self.symbols)
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"

        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    # Initialize all books
                    for sym in self.symbols:
                        await self.initialize_book(sym)
                        await asyncio.sleep(0.2)

                    async for msg in ws:
                        if not self._running:
                            break
                        data = json.loads(msg)
                        payload = data.get("data", {})
                        stream = data.get("stream", "")
                        # Extract symbol from stream name (e.g., "btcusdt@depth@100ms")
                        sym = stream.split("@")[0].upper()
                        if sym in self.books:
                            self.books[sym].apply_diff(payload)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    await asyncio.sleep(5)

    async def run(self):
        """Start the L2 book manager."""
        self._running = True
        await asyncio.gather(
            self.diff_stream(),
            self.snapshot_loop(),
        )

    def stop(self):
        """Stop the manager."""
        self._running = False

    def get_book(self, symbol: str) -> Optional[L2OrderBook]:
        """Get the L2 book for a symbol."""
        return self.books.get(symbol)

    def get_all_metrics(self) -> dict:
        """Get metrics for all books."""
        return {sym: book.get_metrics() for sym, book in self.books.items()}


# Singleton for shared access across modules
_manager: Optional[L2BookManager] = None


def get_manager() -> L2BookManager:
    """Get or create the global L2 book manager."""
    global _manager
    if _manager is None:
        _manager = L2BookManager()
    return _manager


async def run():
    """Entrypoint for main.py supervised() integration."""
    mgr = get_manager()
    await mgr.run()
