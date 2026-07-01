"""
Portfolio Risk — multi-symbol correlation matrix + portfolio-level VaR.

Provides:
1. Rolling correlation matrix between all traded pairs
2. Portfolio VaR (Value at Risk) using variance-covariance method
3. Concentration limits (reject new positions that increase correlation exposure)
4. Diversification ratio (actual risk vs sum of individual risks)

No numpy/pandas — pure Python for lean deployment.
"""
import math
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class PortfolioRisk:
    """Real-time portfolio risk calculator.

    Tracks return streams per symbol, computes rolling correlations,
    and estimates portfolio VaR.
    """
    symbols: list = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    window: int = 200  # correlation window (ticks)
    var_confidence: float = 1.645  # 95% VaR (z-score)

    # Per-symbol return streams
    _returns: dict = field(default_factory=dict)  # symbol → deque of returns
    _prev_mids: dict = field(default_factory=dict)  # symbol → last mid price
    _volatilities: dict = field(default_factory=dict)  # symbol → rolling vol (bps)

    # Correlation matrix (upper triangle, stored as dict)
    _corr_matrix: dict = field(default_factory=dict)  # "SYM1-SYM2" → correlation
    _last_corr_update: float = 0.0
    _corr_update_interval: float = 5.0  # seconds

    # Portfolio state
    _positions: dict = field(default_factory=dict)  # symbol → position USD
    portfolio_var_usd: float = 0.0
    diversification_ratio: float = 1.0  # 1.0 = no diversification benefit
    max_correlation: float = 0.0  # highest pairwise correlation

    def __post_init__(self):
        for sym in self.symbols:
            self._returns[sym] = deque(maxlen=self.window)
            self._prev_mids[sym] = 0.0
            self._volatilities[sym] = 0.0

    def update_price(self, symbol: str, mid: float):
        """Update with new price tick for a symbol."""
        if symbol not in self._returns:
            self._returns[symbol] = deque(maxlen=self.window)
            self._prev_mids[symbol] = 0.0
            self._volatilities[symbol] = 0.0

        prev = self._prev_mids[symbol]
        if prev > 0 and mid > 0:
            ret = (mid - prev) / prev
            self._returns[symbol].append(ret)
            # Update rolling volatility (EMA of absolute returns)
            abs_ret_bps = abs(ret) * 10000
            alpha = 0.02
            self._volatilities[symbol] += alpha * (abs_ret_bps - self._volatilities[symbol])
        self._prev_mids[symbol] = mid

        # Periodically recompute correlation matrix
        now = time.time()
        if now - self._last_corr_update > self._corr_update_interval:
            self._recompute_correlations()
            self._recompute_var()
            self._last_corr_update = now

    def update_position(self, symbol: str, position_usd: float):
        """Update position for a symbol."""
        self._positions[symbol] = position_usd

    def _recompute_correlations(self):
        """Recompute the full correlation matrix."""
        symbols_with_data = [s for s in self.symbols if len(self._returns.get(s, [])) >= 30]

        self.max_correlation = 0.0
        for i, sym1 in enumerate(symbols_with_data):
            for sym2 in symbols_with_data[i + 1:]:
                corr = self._pearson(
                    list(self._returns[sym1]),
                    list(self._returns[sym2])
                )
                key = f"{sym1}-{sym2}"
                self._corr_matrix[key] = corr
                if abs(corr) > self.max_correlation:
                    self.max_correlation = abs(corr)

    def _recompute_var(self):
        """Compute portfolio VaR using variance-covariance method.

        VaR = z * sqrt(w' * Σ * w) where:
        - z = confidence z-score (1.645 for 95%)
        - w = position weights vector
        - Σ = covariance matrix
        """
        symbols_with_pos = [s for s in self.symbols
                           if abs(self._positions.get(s, 0)) > 0
                           and len(self._returns.get(s, [])) >= 30]

        if not symbols_with_pos:
            self.portfolio_var_usd = 0.0
            self.diversification_ratio = 1.0
            return

        # Build covariance matrix
        n = len(symbols_with_pos)
        # Compute individual VaRs and portfolio VaR
        individual_vars = []
        for sym in symbols_with_pos:
            pos = abs(self._positions.get(sym, 0))
            vol = self._volatilities.get(sym, 0) / 10000  # convert bps to decimal
            individual_var = self.var_confidence * vol * pos
            individual_vars.append(individual_var)

        # Sum of individual VaRs (undiversified)
        sum_individual = sum(individual_vars)

        # Portfolio variance = sum over all pairs of: pos_i * pos_j * vol_i * vol_j * corr_ij
        portfolio_variance = 0.0
        for i, sym_i in enumerate(symbols_with_pos):
            for j, sym_j in enumerate(symbols_with_pos):
                pos_i = abs(self._positions.get(sym_i, 0))
                pos_j = abs(self._positions.get(sym_j, 0))
                vol_i = self._volatilities.get(sym_i, 0) / 10000
                vol_j = self._volatilities.get(sym_j, 0) / 10000

                if i == j:
                    corr = 1.0
                else:
                    key = f"{sym_i}-{sym_j}"
                    key_rev = f"{sym_j}-{sym_i}"
                    corr = self._corr_matrix.get(key, self._corr_matrix.get(key_rev, 0.5))

                portfolio_variance += pos_i * pos_j * vol_i * vol_j * corr

        # Portfolio VaR
        if portfolio_variance > 0:
            self.portfolio_var_usd = self.var_confidence * math.sqrt(portfolio_variance)
        else:
            self.portfolio_var_usd = 0.0

        # Diversification ratio: undiversified VaR / portfolio VaR
        # > 1.0 means we're getting diversification benefit
        if self.portfolio_var_usd > 0:
            self.diversification_ratio = sum_individual / self.portfolio_var_usd
        else:
            self.diversification_ratio = 1.0

    def should_allow_trade(self, symbol: str, additional_usd: float,
                           max_var_usd: float = 50.0) -> tuple:
        """Check if adding a position would exceed portfolio VaR limits.
        Returns (allowed: bool, reason: str).
        """
        # Simulate adding the position
        current_pos = self._positions.get(symbol, 0)
        test_pos = current_pos + additional_usd

        # Check concentration: no single position > 60% of total portfolio
        total_abs = sum(abs(p) for p in self._positions.values()) + abs(additional_usd)
        if total_abs > 0:
            concentration = abs(test_pos) / total_abs
            if concentration > 0.6:
                return False, f"concentration {concentration:.0%} > 60% limit"

        # Check correlation: reject if highly correlated with existing large positions
        for other_sym, other_pos in self._positions.items():
            if other_sym == symbol or abs(other_pos) < 10:
                continue
            key = f"{symbol}-{other_sym}"
            key_rev = f"{other_sym}-{symbol}"
            corr = self._corr_matrix.get(key, self._corr_matrix.get(key_rev, 0))
            # High correlation + same direction = concentrated risk
            same_direction = (additional_usd > 0) == (other_pos > 0)
            if abs(corr) > 0.8 and same_direction and abs(other_pos) > 30:
                return False, f"high corr ({corr:.2f}) with {other_sym} ({other_pos:.0f}USD)"

        # Check portfolio VaR limit
        if self.portfolio_var_usd > max_var_usd:
            return False, f"portfolio VaR ${self.portfolio_var_usd:.2f} > ${max_var_usd} limit"

        return True, "ok"

    def _pearson(self, x: list, y: list) -> float:
        """Pearson correlation coefficient between two return series."""
        n = min(len(x), len(y))
        if n < 10:
            return 0.0
        x = x[-n:]
        y = y[-n:]
        mx = sum(x) / n
        my = sum(y) / n
        cov = sum((a - mx) * (b - my) for a, b in zip(x, y)) / n
        sx = math.sqrt(sum((a - mx) ** 2 for a in x) / n)
        sy = math.sqrt(sum((b - my) ** 2 for b in y) / n)
        if sx == 0 or sy == 0:
            return 0.0
        return max(-1.0, min(1.0, cov / (sx * sy)))

    def get_correlation_matrix(self) -> dict:
        """Get full correlation matrix as dict."""
        return dict(self._corr_matrix)

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        return {
            "portfolio_var_usd": round(self.portfolio_var_usd, 2),
            "diversification_ratio": round(self.diversification_ratio, 3),
            "max_pairwise_correlation": round(self.max_correlation, 3),
            "correlations": {k: round(v, 3) for k, v in self._corr_matrix.items()},
            "volatilities_bps": {k: round(v, 2) for k, v in self._volatilities.items()},
            "positions_usd": {k: round(v, 2) for k, v in self._positions.items()},
        }


# Singleton
_portfolio_risk = None


def get_portfolio_risk() -> PortfolioRisk:
    """Get or create the global portfolio risk instance."""
    global _portfolio_risk
    if _portfolio_risk is None:
        import os
        symbols = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")]
        _portfolio_risk = PortfolioRisk(symbols=symbols)
    return _portfolio_risk
