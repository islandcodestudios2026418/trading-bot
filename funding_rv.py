"""
Cross-Instrument Funding Rate Relative Value.

Key insight: Funding rates diverge between correlated instruments.
When BTC funding is extremely positive (longs paying shorts) but ETH funding is
neutral, there's a mean-reversion opportunity:
- SHORT BTC-SWAP (collect funding)
- LONG ETH-SWAP (low cost)
- The relative spread tends to normalize

This module:
1. Tracks funding rates across multiple instruments
2. Computes z-scores of funding rate spreads (cross-instrument)
3. Detects relative value opportunities (|spread_z| > 2.0)
4. Sizes trades based on historical spread volatility
5. Auto-exits when spread normalizes

Pairs tracked: BTC/ETH, BTC/SOL, ETH/SOL
No numpy/pandas — pure Python for lean deployment.
"""
import os
import time
from collections import deque
from dataclasses import dataclass, field


# Relative value pairs
RV_PAIRS = [
    ("BTC-USDT-SWAP", "ETH-USDT-SWAP"),
    ("BTC-USDT-SWAP", "SOL-USDT-SWAP"),
    ("ETH-USDT-SWAP", "SOL-USDT-SWAP"),
]

# Config
RV_ZSCORE_ENTRY = float(os.getenv("RV_ZSCORE_ENTRY", "2.0"))  # enter when |z| > 2.0
RV_ZSCORE_EXIT = float(os.getenv("RV_ZSCORE_EXIT", "0.5"))   # exit when |z| < 0.5
RV_SIZE_USD = float(os.getenv("RV_SIZE_USD", "50"))            # per-leg size
RV_MAX_POSITIONS = int(os.getenv("RV_MAX_POSITIONS", "3"))     # max concurrent RV trades
RV_ENABLED = os.getenv("RV_ENABLED", "0") == "1"              # disabled by default


@dataclass
class FundingRateSnapshot:
    """Single funding rate observation."""
    instrument: str
    rate: float  # annualized rate (e.g., 0.01 = 1%/8h period)
    timestamp: float


@dataclass
class RVPosition:
    """An active relative value position."""
    long_inst: str  # instrument we're long
    short_inst: str  # instrument we're short
    entry_spread: float  # funding spread at entry
    entry_time: float
    size_usd: float
    entry_zscore: float


@dataclass
class FundingRV:
    """Cross-instrument funding rate relative value detector.

    Tracks funding rate spreads between pairs and signals
    mean-reversion opportunities.
    """
    # Funding rate history per instrument
    _rates: dict = field(default_factory=dict)  # instId → deque of (timestamp, rate)
    _current_rates: dict = field(default_factory=dict)  # instId → latest rate
    # Spread history for z-score
    _spread_history: dict = field(default_factory=dict)  # "INST1-INST2" → deque of spread values
    # Active positions
    positions: list = field(default_factory=list)  # list of RVPosition
    # Stats
    total_pnl: float = 0.0
    trades_count: int = 0
    active_pnl: float = 0.0  # unrealized PnL from current positions
    signals_generated: int = 0

    def __post_init__(self):
        for pair in RV_PAIRS:
            key = f"{pair[0]}|{pair[1]}"
            self._spread_history[key] = deque(maxlen=500)
            self._rates[pair[0]] = deque(maxlen=100)
            self._rates[pair[1]] = deque(maxlen=100)

    def update_rate(self, instrument: str, rate: float):
        """Update funding rate for an instrument.
        Call this whenever a new funding rate is received.
        Rate should be in percentage terms (e.g., 0.01 = 0.01%/8h).
        """
        now = time.time()
        if instrument not in self._rates:
            self._rates[instrument] = deque(maxlen=100)
        self._rates[instrument].append((now, rate))
        self._current_rates[instrument] = rate

        # Update spread history for all pairs involving this instrument
        for pair in RV_PAIRS:
            if instrument in pair:
                other = pair[1] if pair[0] == instrument else pair[0]
                other_rate = self._current_rates.get(other)
                if other_rate is not None:
                    spread = self._current_rates[pair[0]] - self._current_rates[pair[1]]
                    key = f"{pair[0]}|{pair[1]}"
                    self._spread_history[key].append(spread)

    def check_signals(self) -> list:
        """Check for relative value entry/exit signals.
        Returns list of action dicts: {type: "entry"|"exit", ...}
        """
        if not RV_ENABLED:
            return []

        signals = []

        # Check exits first (free up capacity)
        for pos in list(self.positions):
            current_spread = self._get_spread(pos.short_inst, pos.long_inst)
            if current_spread is None:
                continue
            key = f"{pos.short_inst}|{pos.long_inst}"
            z = self._compute_zscore(key, current_spread)
            if z is not None and abs(z) < RV_ZSCORE_EXIT:
                signals.append({
                    "type": "exit",
                    "position": pos,
                    "current_zscore": z,
                    "pnl_est": (pos.entry_spread - current_spread) * pos.size_usd,
                })

        # Check entries (if capacity available)
        if len(self.positions) >= RV_MAX_POSITIONS:
            return signals

        for pair in RV_PAIRS:
            key = f"{pair[0]}|{pair[1]}"
            current_spread = self._get_spread(pair[0], pair[1])
            if current_spread is None:
                continue

            z = self._compute_zscore(key, current_spread)
            if z is None:
                continue

            if abs(z) >= RV_ZSCORE_ENTRY:
                # Significant divergence — generate entry signal
                if z > 0:
                    # Spread positive: pair[0] funding > pair[1]
                    # SHORT pair[0] (expensive funding), LONG pair[1] (cheap)
                    long_inst = pair[1]
                    short_inst = pair[0]
                else:
                    long_inst = pair[0]
                    short_inst = pair[1]

                # Check if we already have this pair
                already_have = any(
                    p.long_inst == long_inst and p.short_inst == short_inst
                    for p in self.positions
                )
                if not already_have:
                    self.signals_generated += 1
                    signals.append({
                        "type": "entry",
                        "long_inst": long_inst,
                        "short_inst": short_inst,
                        "spread": current_spread,
                        "zscore": z,
                        "size_usd": RV_SIZE_USD,
                    })

        return signals

    def execute_entry(self, long_inst: str, short_inst: str, spread: float, zscore: float, size_usd: float):
        """Record entry of a relative value position.
        Actual execution should be done by the caller (via okx_client).
        """
        pos = RVPosition(
            long_inst=long_inst,
            short_inst=short_inst,
            entry_spread=spread,
            entry_time=time.time(),
            size_usd=size_usd,
            entry_zscore=zscore,
        )
        self.positions.append(pos)
        self.trades_count += 1

    def execute_exit(self, position: RVPosition, pnl: float = 0.0):
        """Record exit of a relative value position."""
        if position in self.positions:
            self.positions.remove(position)
        self.total_pnl += pnl
        self.trades_count += 1

    def _get_spread(self, inst1: str, inst2: str) -> float:
        """Get current funding rate spread between two instruments."""
        r1 = self._current_rates.get(inst1)
        r2 = self._current_rates.get(inst2)
        if r1 is None or r2 is None:
            return None
        return r1 - r2

    def _compute_zscore(self, key: str, current_spread: float) -> float:
        """Compute z-score of current spread vs history."""
        history = self._spread_history.get(key, [])
        if len(history) < 20:
            return None  # not enough data

        mean_s = sum(history) / len(history)
        var_s = sum((x - mean_s) ** 2 for x in history) / len(history)
        std_s = var_s ** 0.5
        if std_s == 0:
            return None
        return (current_spread - mean_s) / std_s

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        spreads = {}
        for pair in RV_PAIRS:
            key = f"{pair[0]}|{pair[1]}"
            spread = self._get_spread(pair[0], pair[1])
            z = self._compute_zscore(key, spread) if spread is not None else None
            spreads[key] = {
                "spread": round(spread, 6) if spread is not None else None,
                "zscore": round(z, 3) if z is not None else None,
                "history_len": len(self._spread_history.get(key, [])),
            }
        return {
            "enabled": RV_ENABLED,
            "active_positions": len(self.positions),
            "total_pnl": round(self.total_pnl, 4),
            "trades_count": self.trades_count,
            "signals_generated": self.signals_generated,
            "spreads": spreads,
            "current_rates": {k: round(v, 6) for k, v in self._current_rates.items()},
            "positions": [
                {
                    "long": p.long_inst, "short": p.short_inst,
                    "entry_spread": round(p.entry_spread, 6),
                    "entry_z": round(p.entry_zscore, 2),
                    "age_min": round((time.time() - p.entry_time) / 60, 1),
                }
                for p in self.positions
            ],
        }


# Singleton
_funding_rv = None


def get_funding_rv() -> FundingRV:
    """Get or create the global funding RV instance."""
    global _funding_rv
    if _funding_rv is None:
        _funding_rv = FundingRV()
    return _funding_rv
