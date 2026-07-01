"""
Fill-Time Analytics — tracks time-to-fill distribution per symbol.

Key insight: How quickly our orders get filled reveals edge quality.
- Fast fills (< 1s): We're providing too much edge (spread too tight)
- Slow fills (> 30s): We're too wide (no one wants our prices)
- Optimal: 2-10s average fill time = healthy two-sided market making

This module:
1. Tracks time from quote placement to fill per symbol + side
2. Computes fill-time distribution (mean, median, p90, p99)
3. Adaptive feedback: adjust spread/size based on fill speed
4. Adverse selection detection: very fast fills = we got picked off
5. Fill rate tracking: what % of our quotes get filled vs cancelled

No numpy/pandas — pure Python for lean deployment.
"""
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class FillTimeRecord:
    """Single fill-time observation."""
    symbol: str
    side: str  # "buy" or "sell"
    time_to_fill_s: float
    was_adverse: bool = False  # did price move against us immediately after?
    edge_bps: float = 0.0  # how much edge (mid to our price) at fill time


@dataclass
class FillTimeTracker:
    """Per-symbol fill-time analytics.

    Usage:
    - Call quote_placed() when a new order is submitted
    - Call quote_filled() when the order is filled
    - Call quote_cancelled() when the order is cancelled (unfilled)
    """
    # Configuration
    target_fill_time_s: float = 5.0  # target average fill time
    adverse_threshold_s: float = 0.5  # fills faster than this = likely adverse selection
    stale_threshold_s: float = 30.0   # quotes unfilled for this long = too wide

    # Pending quotes: {quote_id: (symbol, side, timestamp, edge_bps)}
    _pending: dict = field(default_factory=dict)
    # Fill-time history per symbol
    _fill_times: dict = field(default_factory=dict)  # symbol → deque of FillTimeRecord
    # Fill rates
    _quotes_placed: dict = field(default_factory=dict)  # symbol → count
    _quotes_filled: dict = field(default_factory=dict)  # symbol → count
    _quotes_cancelled: dict = field(default_factory=dict)  # symbol → count
    _adverse_fills: dict = field(default_factory=dict)  # symbol → count

    # Global stats
    total_fill_time_ema: float = 5.0  # EMA of fill time across all symbols
    total_fills: int = 0
    total_adverse: int = 0

    def quote_placed(self, quote_id: str, symbol: str, side: str, edge_bps: float = 0.0):
        """Record that a new quote was placed."""
        self._pending[quote_id] = (symbol, side, time.time(), edge_bps)
        self._quotes_placed[symbol] = self._quotes_placed.get(symbol, 0) + 1

    def quote_filled(self, quote_id: str, mid_at_fill: float = 0.0, fill_px: float = 0.0):
        """Record that a pending quote was filled. Returns fill-time record."""
        if quote_id not in self._pending:
            return None

        symbol, side, placed_time, edge_bps = self._pending.pop(quote_id)
        fill_time = time.time() - placed_time

        # Adverse selection: very fast fill = informed trader picked us off
        was_adverse = fill_time < self.adverse_threshold_s

        record = FillTimeRecord(
            symbol=symbol,
            side=side,
            time_to_fill_s=fill_time,
            was_adverse=was_adverse,
            edge_bps=edge_bps,
        )

        # Store in per-symbol history
        if symbol not in self._fill_times:
            self._fill_times[symbol] = deque(maxlen=200)
        self._fill_times[symbol].append(record)

        # Update counters
        self._quotes_filled[symbol] = self._quotes_filled.get(symbol, 0) + 1
        self.total_fills += 1
        if was_adverse:
            self._adverse_fills[symbol] = self._adverse_fills.get(symbol, 0) + 1
            self.total_adverse += 1

        # Update global EMA
        alpha = 0.05
        self.total_fill_time_ema += alpha * (fill_time - self.total_fill_time_ema)

        return record

    def quote_cancelled(self, quote_id: str):
        """Record that a pending quote was cancelled (unfilled)."""
        if quote_id not in self._pending:
            return
        symbol, side, placed_time, edge_bps = self._pending.pop(quote_id)
        self._quotes_cancelled[symbol] = self._quotes_cancelled.get(symbol, 0) + 1

    def cleanup_stale(self):
        """Remove quotes that have been pending too long (stale)."""
        now = time.time()
        stale_ids = [qid for qid, (sym, side, ts, edge) in self._pending.items()
                     if now - ts > self.stale_threshold_s * 2]
        for qid in stale_ids:
            self.quote_cancelled(qid)

    def get_fill_time_stats(self, symbol: str = None) -> dict:
        """Get fill-time statistics for a symbol (or all symbols if None)."""
        if symbol:
            records = list(self._fill_times.get(symbol, []))
        else:
            records = []
            for sym_records in self._fill_times.values():
                records.extend(sym_records)

        if not records:
            return {"mean_s": 0, "median_s": 0, "p90_s": 0, "p99_s": 0, "count": 0}

        times = sorted(r.time_to_fill_s for r in records)
        n = len(times)
        return {
            "mean_s": round(sum(times) / n, 2),
            "median_s": round(times[n // 2], 2),
            "p90_s": round(times[int(n * 0.9)], 2) if n >= 10 else round(times[-1], 2),
            "p99_s": round(times[int(n * 0.99)], 2) if n >= 100 else round(times[-1], 2),
            "min_s": round(times[0], 3),
            "max_s": round(times[-1], 2),
            "count": n,
            "adverse_pct": round(sum(1 for r in records if r.was_adverse) / n * 100, 1),
        }

    def fill_rate(self, symbol: str = None) -> float:
        """Fraction of placed quotes that got filled (0-1)."""
        if symbol:
            placed = self._quotes_placed.get(symbol, 0)
            filled = self._quotes_filled.get(symbol, 0)
        else:
            placed = sum(self._quotes_placed.values())
            filled = sum(self._quotes_filled.values())
        return filled / placed if placed > 0 else 0.0

    def spread_adjustment(self, symbol: str) -> float:
        """Recommend spread adjustment based on fill-time.
        Returns multiplier: < 1.0 = tighten (too slow), > 1.0 = widen (too fast).
        """
        stats = self.get_fill_time_stats(symbol)
        if stats["count"] < 10:
            return 1.0  # not enough data

        mean_fill = stats["mean_s"]
        # Target: 2-10s average fill time
        if mean_fill < 1.0:
            # Filling too fast — widen spread
            return 1.2 + (1.0 - mean_fill) * 0.5  # 1.2-1.7x
        elif mean_fill > 15.0:
            # Filling too slow — tighten spread
            return max(0.7, 1.0 - (mean_fill - 15) * 0.02)  # 0.7-1.0x
        elif mean_fill > self.target_fill_time_s * 2:
            # Moderately slow — slight tighten
            return 0.9
        return 1.0  # in range, no adjustment

    def adverse_selection_rate(self, symbol: str = None) -> float:
        """What fraction of fills were adversely selected (0-1)."""
        if symbol:
            adverse = self._adverse_fills.get(symbol, 0)
            filled = self._quotes_filled.get(symbol, 0)
        else:
            adverse = self.total_adverse
            filled = self.total_fills
        return adverse / filled if filled > 0 else 0.0

    def get_metrics(self) -> dict:
        """Full metrics for /metrics endpoint."""
        per_symbol = {}
        for sym in self._fill_times:
            stats = self.get_fill_time_stats(sym)
            stats["fill_rate"] = round(self.fill_rate(sym), 3)
            stats["adverse_rate"] = round(self.adverse_selection_rate(sym), 3)
            stats["spread_adj"] = round(self.spread_adjustment(sym), 3)
            per_symbol[sym] = stats

        return {
            "global_fill_time_ema_s": round(self.total_fill_time_ema, 2),
            "total_fills": self.total_fills,
            "total_adverse": self.total_adverse,
            "global_adverse_rate": round(self.adverse_selection_rate(), 3),
            "global_fill_rate": round(self.fill_rate(), 3),
            "pending_quotes": len(self._pending),
            "per_symbol": per_symbol,
        }


# Singleton
_tracker = None


def get_fill_tracker() -> FillTimeTracker:
    """Get or create the global fill-time tracker."""
    global _tracker
    if _tracker is None:
        _tracker = FillTimeTracker()
    return _tracker
