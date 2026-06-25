"""
Dynamic capital allocation — allocate more capital to higher-Sharpe strategies.
Rebalances every hour. Uses inverse-volatility weighting with Sharpe tilt.
"""
import os
import time
from collections import deque

CAPITAL = float(os.getenv("CAPITAL", "2000"))
REBALANCE_INTERVAL = 3600  # 1 hour
MIN_ALLOC_PCT = 10  # minimum 10% to any active strategy
MAX_ALLOC_PCT = 50  # cap at 50% per strategy


class StrategyTracker:
    """Track PnL returns for one strategy."""
    def __init__(self, name: str):
        self.name = name
        self.returns: deque = deque(maxlen=200)  # recent trade PnLs
        self.total_pnl: float = 0.0
        self.trade_count: int = 0

    def record(self, pnl: float):
        self.returns.append(pnl)
        self.total_pnl += pnl
        self.trade_count += 1

    @property
    def sharpe(self) -> float:
        """Simple Sharpe: mean(returns) / std(returns). Higher = better."""
        if len(self.returns) < 5:
            return 0.0
        rets = list(self.returns)
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        std = var ** 0.5
        return mean / std if std > 0 else 0.0


class CapitalAllocator:
    """Allocate capital across strategies based on performance."""
    def __init__(self):
        self.strategies: dict[str, StrategyTracker] = {}
        self._allocations: dict[str, float] = {}  # strategy → USD allocation
        self._last_rebalance: float = 0.0

    def register(self, name: str) -> StrategyTracker:
        t = StrategyTracker(name)
        self.strategies[name] = t
        self._equal_alloc()
        return t

    def _equal_alloc(self):
        """Equal allocation as baseline."""
        n = len(self.strategies) or 1
        per = CAPITAL / n
        for name in self.strategies:
            self._allocations[name] = per

    def get_allocation(self, strategy: str) -> float:
        """Get current USD allocation for a strategy. Rebalances if needed."""
        if time.time() - self._last_rebalance > REBALANCE_INTERVAL:
            self._rebalance()
        return self._allocations.get(strategy, CAPITAL / max(1, len(self.strategies)))

    def _rebalance(self):
        """Rebalance based on Sharpe-weighted allocation."""
        self._last_rebalance = time.time()
        if not self.strategies:
            return

        sharpes = {name: max(0.0, t.sharpe) for name, t in self.strategies.items()}
        total_sharpe = sum(sharpes.values())

        if total_sharpe <= 0:
            # No strategy performing well — equal allocation
            self._equal_alloc()
            return

        # Sharpe-proportional allocation with floor/cap
        min_usd = CAPITAL * MIN_ALLOC_PCT / 100
        max_usd = CAPITAL * MAX_ALLOC_PCT / 100
        raw = {name: (s / total_sharpe) * CAPITAL for name, s in sharpes.items()}

        # Apply floor and cap
        for name in raw:
            raw[name] = max(min_usd, min(max_usd, raw[name]))

        # Normalize to total capital
        total_raw = sum(raw.values())
        if total_raw > 0:
            for name in raw:
                self._allocations[name] = raw[name] / total_raw * CAPITAL

    def status(self) -> dict:
        return {
            name: {
                "alloc_usd": round(self._allocations.get(name, 0), 0),
                "alloc_pct": round(self._allocations.get(name, 0) / CAPITAL * 100, 1),
                "sharpe": round(t.sharpe, 2),
                "total_pnl": round(t.total_pnl, 4),
                "trades": t.trade_count,
            }
            for name, t in self.strategies.items()
        }


# Global instance
allocator = CapitalAllocator()
