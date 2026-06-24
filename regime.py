"""
Regime detection — classifies market as trending or mean-reverting.
Uses variance ratio + Hurst exponent (R/S analysis) for robust classification.
No numpy/pandas — pure Python for lean deployment.
"""
from collections import deque
from dataclasses import dataclass, field

# Regime enum
TRENDING = "trending"
RANGING = "ranging"
NEUTRAL = "neutral"


@dataclass
class RegimeDetector:
    """Dual regime detection: Variance Ratio + Hurst Exponent.
    VR > 1.3 or H > 0.6 → trending. VR < 0.7 or H < 0.4 → mean-reverting.
    Consensus required: both must agree for strong signal, else neutral.
    """
    window: int = 120  # ~12s at 100ms ticks
    _returns: deque = field(default_factory=lambda: deque(maxlen=240))
    _prev_mid: float = 0.0
    regime: str = NEUTRAL
    vr: float = 1.0  # variance ratio
    hurst: float = 0.5  # Hurst exponent (0.5 = random walk)
    # Regime stats for analytics
    regime_fills: dict = field(default_factory=lambda: {TRENDING: 0, RANGING: 0, NEUTRAL: 0})
    regime_pnl: dict = field(default_factory=lambda: {TRENDING: 0.0, RANGING: 0.0, NEUTRAL: 0.0})

    def update(self, mid: float) -> str:
        """Update with new mid price, return current regime."""
        if self._prev_mid > 0 and mid > 0:
            ret = (mid - self._prev_mid) / self._prev_mid
            self._returns.append(ret)
        self._prev_mid = mid

        if len(self._returns) >= self.window:
            self.vr = self._calc_vr()
            self.hurst = self._calc_hurst()
            # Consensus: both VR and Hurst must agree
            vr_trend = self.vr > 1.3
            vr_range = self.vr < 0.7
            h_trend = self.hurst > 0.6
            h_range = self.hurst < 0.4
            if vr_trend and h_trend:
                self.regime = TRENDING
            elif vr_range and h_range:
                self.regime = RANGING
            elif vr_trend or h_trend:
                # Weak trend signal — stay neutral but lean trending
                self.regime = NEUTRAL
            elif vr_range or h_range:
                self.regime = NEUTRAL
            else:
                self.regime = NEUTRAL
        return self.regime

    def _calc_vr(self) -> float:
        """Variance ratio: Var(k-period returns) / (k * Var(1-period returns))."""
        rets = list(self._returns)
        n = len(rets)
        k = 10

        mean1 = sum(rets) / n
        var1 = sum((r - mean1) ** 2 for r in rets) / n
        if var1 == 0:
            return 1.0

        k_rets = []
        for i in range(k, n):
            k_rets.append(sum(rets[i - k:i]))
        if not k_rets:
            return 1.0
        mean_k = sum(k_rets) / len(k_rets)
        var_k = sum((r - mean_k) ** 2 for r in k_rets) / len(k_rets)

        return var_k / (k * var1)

    def _calc_hurst(self) -> float:
        """Hurst exponent via simplified R/S analysis.
        H > 0.5 = persistent (trending), H < 0.5 = anti-persistent (mean-reverting).
        Uses multiple sub-window sizes for more robust estimate.
        """
        rets = list(self._returns)
        n = len(rets)
        if n < 20:
            return 0.5

        # R/S at different scales
        log_rs = []
        log_n = []
        for size in (20, 40, 80, 120):
            if size > n:
                break
            rs_vals = []
            for start in range(0, n - size + 1, size):
                chunk = rets[start:start + size]
                mean_c = sum(chunk) / len(chunk)
                # Cumulative deviations
                cum = []
                s = 0.0
                for r in chunk:
                    s += r - mean_c
                    cum.append(s)
                R = max(cum) - min(cum)
                # Standard deviation
                S = (sum((r - mean_c) ** 2 for r in chunk) / len(chunk)) ** 0.5
                if S > 0:
                    rs_vals.append(R / S)
            if rs_vals:
                import math
                avg_rs = sum(rs_vals) / len(rs_vals)
                if avg_rs > 0:
                    log_rs.append(math.log(avg_rs))
                    log_n.append(math.log(size))

        if len(log_rs) < 2:
            return 0.5

        # Linear regression slope = Hurst exponent
        n_pts = len(log_rs)
        sum_x = sum(log_n)
        sum_y = sum(log_rs)
        sum_xy = sum(x * y for x, y in zip(log_n, log_rs))
        sum_x2 = sum(x * x for x in log_n)
        denom = n_pts * sum_x2 - sum_x * sum_x
        if denom == 0:
            return 0.5
        slope = (n_pts * sum_xy - sum_x * sum_y) / denom
        return max(0.0, min(1.0, slope))

    def adapt_thresholds(self, base_buy: float, base_sell: float) -> tuple[float, float]:
        """Adjust entry thresholds based on regime.
        Trending: lower threshold (easier entry), wider exit (let it run).
        Ranging: higher threshold (stricter entry), tight exit (quick scalp).
        """
        if self.regime == TRENDING:
            return base_buy * 0.7, base_sell * 0.7  # easier entry
        elif self.regime == RANGING:
            return base_buy * 1.4, base_sell * 1.4  # stricter entry
        return base_buy, base_sell

    def adapt_exit(self, base_atr_mult: float) -> float:
        """Adjust ATR multiplier for trailing stop.
        Trending: wider stop (2.5x ATR), let winners run.
        Ranging: tighter stop (1.0x ATR), take profits fast.
        """
        if self.regime == TRENDING:
            return base_atr_mult * 1.7  # hold longer
        elif self.regime == RANGING:
            return base_atr_mult * 0.65  # exit fast
        return base_atr_mult

    def record_fill(self, pnl: float):
        """Track PnL per regime for analytics."""
        self.regime_fills[self.regime] = self.regime_fills.get(self.regime, 0) + 1
        self.regime_pnl[self.regime] = self.regime_pnl.get(self.regime, 0.0) + pnl
