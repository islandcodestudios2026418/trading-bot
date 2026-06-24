"""
Regime detection — classifies market as trending or mean-reverting.
Uses variance ratio test (fast Hurst proxy) on recent price returns.
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
    """Variance ratio regime detection.
    VR > 1.3 → trending (momentum), VR < 0.7 → mean-reverting, else neutral.
    Adapts OFI thresholds and exit behavior per regime.
    """
    window: int = 120  # ~12s at 100ms ticks
    _returns: deque = field(default_factory=lambda: deque(maxlen=240))
    _prev_mid: float = 0.0
    regime: str = NEUTRAL
    vr: float = 1.0  # variance ratio
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
            if self.vr > 1.3:
                self.regime = TRENDING
            elif self.vr < 0.7:
                self.regime = RANGING
            else:
                self.regime = NEUTRAL
        return self.regime

    def _calc_vr(self) -> float:
        """Variance ratio: Var(k-period returns) / (k * Var(1-period returns)).
        VR > 1 = positive autocorrelation (trend), VR < 1 = negative (revert)."""
        rets = list(self._returns)
        n = len(rets)
        k = 10  # compare 10-period vs 1-period variance

        # 1-period variance
        mean1 = sum(rets) / n
        var1 = sum((r - mean1) ** 2 for r in rets) / n
        if var1 == 0:
            return 1.0

        # k-period returns
        k_rets = []
        for i in range(k, n):
            k_rets.append(sum(rets[i - k:i]))
        if not k_rets:
            return 1.0
        mean_k = sum(k_rets) / len(k_rets)
        var_k = sum((r - mean_k) ** 2 for r in k_rets) / len(k_rets)

        return var_k / (k * var1)

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
