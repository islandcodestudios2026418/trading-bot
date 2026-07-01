"""
Volatility Clustering — online GARCH-like vol-of-vol detection.

Key insight: Volatility itself is volatile and clustered.
High vol-of-vol periods precede large moves (breakouts/crashes).
Low vol-of-vol = stable regime (safe to be aggressive).

This module:
1. Tracks realized volatility at multiple timescales (1s, 5s, 30s)
2. Computes vol-of-vol (variance of volatility itself)
3. Detects volatility regime shifts (quiet → explosive transition)
4. Provides multipliers for position sizing and spread width
5. Online exponential GARCH(1,1) approximation (no matrix algebra)

Output signals:
- vol_cluster_state: "quiet", "normal", "elevated", "explosive"
- size_multiplier: 0.3-1.5 (scale position size)
- spread_multiplier: 0.7-2.5 (scale quote spread)

No numpy/pandas — pure Python for lean deployment.
"""
import math
import time
from collections import deque
from dataclasses import dataclass, field


# Vol cluster states
VOL_QUIET = "quiet"        # vol << average, compress size, tighten spreads
VOL_NORMAL = "normal"      # vol near average, baseline
VOL_ELEVATED = "elevated"  # vol > 1.5x average, widen spreads
VOL_EXPLOSIVE = "explosive"  # vol > 3x average OR vol-of-vol spike, reduce size + widen


@dataclass
class VolCluster:
    """Online volatility clustering detector.

    Architecture:
    - Multi-timescale realized vol: EMA of |returns| at 1s/5s/30s
    - Vol-of-vol: EMA of |vol_change| (second derivative of price)
    - GARCH(1,1) approximation: sigma^2_t = omega + alpha*r^2 + beta*sigma^2_{t-1}
    - State machine: quiet/normal/elevated/explosive based on vol percentile
    """
    # GARCH parameters (pre-tuned for crypto, ~100ms ticks)
    omega: float = 0.0001    # long-run variance floor
    alpha: float = 0.08      # innovation weight (how fast vol responds to new info)
    beta: float = 0.90       # persistence (how sticky vol is)

    # Internal state
    _prev_mid: float = 0.0
    _garch_var: float = 0.0001  # conditional variance (GARCH state)
    _vol_1s: float = 0.0     # short-term vol EMA (bps)
    _vol_5s: float = 0.0     # medium-term vol EMA
    _vol_30s: float = 0.0    # long-term vol EMA
    _vol_of_vol: float = 0.0  # vol-of-vol (how volatile is volatility itself)
    _prev_vol: float = 0.0   # previous vol for vol-of-vol computation
    _vol_history: deque = field(default_factory=lambda: deque(maxlen=500))  # for percentile
    _vol_of_vol_history: deque = field(default_factory=lambda: deque(maxlen=200))
    _tick_count: int = 0

    # Public outputs
    state: str = VOL_NORMAL
    realized_vol_bps: float = 0.0  # current realized vol
    vol_of_vol: float = 0.0        # current vol-of-vol
    garch_forecast: float = 0.0    # 1-step-ahead vol forecast
    vol_percentile: float = 0.5    # where current vol sits in history (0-1)
    vol_zscore: float = 0.0        # z-score of current vol vs history

    def update(self, mid: float) -> str:
        """Update with new mid price. Returns vol cluster state."""
        self._tick_count += 1

        if self._prev_mid <= 0 or mid <= 0:
            self._prev_mid = mid
            return self.state

        # Compute return
        ret = (mid - self._prev_mid) / self._prev_mid
        ret_bps = abs(ret) * 10000
        self._prev_mid = mid

        # Multi-timescale vol EMA
        self._vol_1s += 0.07 * (ret_bps - self._vol_1s)   # ~14 tick half-life
        self._vol_5s += 0.014 * (ret_bps - self._vol_5s)  # ~50 tick half-life
        self._vol_30s += 0.002 * (ret_bps - self._vol_30s) # ~350 tick half-life

        # Composite realized vol (weighted average of timescales)
        self.realized_vol_bps = 0.5 * self._vol_1s + 0.3 * self._vol_5s + 0.2 * self._vol_30s

        # GARCH(1,1) update: sigma^2_t = omega + alpha * r^2 + beta * sigma^2_{t-1}
        self._garch_var = self.omega + self.alpha * (ret ** 2) + self.beta * self._garch_var
        self.garch_forecast = math.sqrt(self._garch_var) * 10000  # bps

        # Vol-of-vol: how fast is volatility changing?
        vol_change = abs(self.realized_vol_bps - self._prev_vol)
        self._vol_of_vol += 0.03 * (vol_change - self._vol_of_vol)
        self.vol_of_vol = self._vol_of_vol
        self._prev_vol = self.realized_vol_bps

        # Track history for percentile/z-score
        self._vol_history.append(self.realized_vol_bps)
        self._vol_of_vol_history.append(self._vol_of_vol)

        # Compute vol percentile + z-score
        if len(self._vol_history) >= 50:
            sorted_vol = sorted(self._vol_history)
            rank = sum(1 for v in sorted_vol if v <= self.realized_vol_bps)
            self.vol_percentile = rank / len(sorted_vol)

            mean_vol = sum(self._vol_history) / len(self._vol_history)
            std_vol = math.sqrt(sum((v - mean_vol) ** 2 for v in self._vol_history) / len(self._vol_history))
            self.vol_zscore = (self.realized_vol_bps - mean_vol) / std_vol if std_vol > 0 else 0

        # State machine
        self._classify_state()
        return self.state

    def _classify_state(self):
        """Classify vol regime based on percentile + vol-of-vol."""
        # Vol-of-vol spike = transition to explosive regardless of absolute vol
        if len(self._vol_of_vol_history) >= 30:
            vov_mean = sum(self._vol_of_vol_history) / len(self._vol_of_vol_history)
            if self._vol_of_vol > vov_mean * 3 and self.vol_percentile > 0.6:
                self.state = VOL_EXPLOSIVE
                return

        # Percentile-based classification
        if self.vol_percentile > 0.90:
            self.state = VOL_EXPLOSIVE
        elif self.vol_percentile > 0.70:
            self.state = VOL_ELEVATED
        elif self.vol_percentile < 0.20:
            self.state = VOL_QUIET
        else:
            self.state = VOL_NORMAL

    def size_multiplier(self) -> float:
        """Position size multiplier based on vol regime.
        Quiet = larger size (less risk per tick), Explosive = tiny size.
        """
        mult = {
            VOL_QUIET: 1.3,
            VOL_NORMAL: 1.0,
            VOL_ELEVATED: 0.6,
            VOL_EXPLOSIVE: 0.3,
        }
        return mult.get(self.state, 1.0)

    def spread_multiplier(self) -> float:
        """Spread width multiplier.
        Quiet = tight (more fills), Explosive = wide (protect against adverse selection).
        """
        mult = {
            VOL_QUIET: 0.7,
            VOL_NORMAL: 1.0,
            VOL_ELEVATED: 1.5,
            VOL_EXPLOSIVE: 2.5,
        }
        return mult.get(self.state, 1.0)

    def entry_threshold_mult(self) -> float:
        """Entry threshold multiplier (OFI, etc).
        Quiet = easier entry (0.8x), Explosive = much harder entry (2.0x).
        """
        mult = {
            VOL_QUIET: 0.8,
            VOL_NORMAL: 1.0,
            VOL_ELEVATED: 1.4,
            VOL_EXPLOSIVE: 2.0,
        }
        return mult.get(self.state, 1.0)

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        return {
            "state": self.state,
            "realized_vol_bps": round(self.realized_vol_bps, 2),
            "garch_forecast_bps": round(self.garch_forecast, 2),
            "vol_of_vol": round(self.vol_of_vol, 3),
            "vol_percentile": round(self.vol_percentile, 3),
            "vol_zscore": round(self.vol_zscore, 2),
            "size_multiplier": self.size_multiplier(),
            "spread_multiplier": self.spread_multiplier(),
            "entry_threshold_mult": self.entry_threshold_mult(),
            "tick_count": self._tick_count,
        }
