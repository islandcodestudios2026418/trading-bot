"""
Adaptive Fee Model — tracks actual maker/taker fill ratios and adjusts PnL calculation.

Problem: Static 5bps entry + 1bps exit assumption is inaccurate.
Reality: Some entries are maker (rebate), some are taker (cost varies by exchange/tier).

This module:
1. Tracks fill type (maker vs taker) from execution metadata
2. Maintains rolling maker_rate per strategy
3. Provides accurate round-trip cost estimates
4. Adapts paper PnL deductions dynamically
5. Estimates actual fee tier based on 30-day volume

Supports OKX fee schedule:
- VIP0: maker -0.01% (rebate), taker 0.05%
- VIP1: maker -0.015%, taker 0.045% (volume > $5M/30d)
- VIP2: maker -0.02%, taker 0.04% (volume > $10M/30d)

For paper trading on Binance:
- Spot maker: 0.01% (with BNB), taker: 0.05%
- Futures maker: 0.01%, taker: 0.04%
"""
import time
from collections import deque
from dataclasses import dataclass, field


# OKX fee tiers (monthly USD volume thresholds)
OKX_TIERS = [
    # (volume_threshold, maker_rate_bps, taker_rate_bps)
    (0,          -0.1, 5.0),     # VIP0: maker rebate -0.01%, taker 0.05%
    (5_000_000,  -1.5, 4.5),     # VIP1
    (10_000_000, -2.0, 4.0),     # VIP2
    (20_000_000, -2.5, 3.5),     # VIP3
    (50_000_000, -3.0, 3.0),     # VIP4
]


@dataclass
class FillRecord:
    """Single fill with fee type tracking."""
    timestamp: float
    size_usd: float
    is_maker: bool
    fee_bps: float  # actual fee paid (negative = rebate)
    strategy: str = "binance_mm"


@dataclass
class AdaptiveFeeModel:
    """Tracks actual fee rates and provides dynamic cost estimation.

    Usage:
    - Call record_fill() after each fill with maker/taker info
    - Call estimate_cost_bps() before trade to get expected round-trip cost
    - Call adjust_pnl() to apply accurate fee to paper PnL
    """
    # Rolling fill history
    _fills: deque = field(default_factory=lambda: deque(maxlen=1000))
    # Per-strategy stats
    _strategy_stats: dict = field(default_factory=dict)
    # Cumulative volume (for tier estimation)
    _monthly_volume: float = 0.0
    _month_start: float = field(default_factory=time.time)
    # Current effective rates (start with VIP0 defaults)
    maker_rate_bps: float = -0.1   # negative = rebate
    taker_rate_bps: float = 5.0
    # Overall maker rate (what fraction of our fills are maker)
    maker_fill_ratio: float = 0.5  # start conservative
    # Fee savings tracking
    total_fees_paid_usd: float = 0.0
    total_rebates_earned_usd: float = 0.0

    def record_fill(self, size_usd: float, is_maker: bool, strategy: str = "binance_mm"):
        """Record a fill with its fee type."""
        now = time.time()

        # Determine actual fee
        if is_maker:
            fee_bps = self.maker_rate_bps
        else:
            fee_bps = self.taker_rate_bps

        fill = FillRecord(
            timestamp=now,
            size_usd=size_usd,
            is_maker=is_maker,
            fee_bps=fee_bps,
            strategy=strategy,
        )
        self._fills.append(fill)

        # Update strategy stats
        if strategy not in self._strategy_stats:
            self._strategy_stats[strategy] = {
                "maker_fills": 0, "taker_fills": 0,
                "total_volume": 0.0, "total_fees_bps": 0.0,
                "fills": 0
            }
        stats = self._strategy_stats[strategy]
        stats["fills"] += 1
        stats["total_volume"] += size_usd
        if is_maker:
            stats["maker_fills"] += 1
        else:
            stats["taker_fills"] += 1

        # Track fees
        fee_usd = size_usd * fee_bps / 10000
        if fee_usd > 0:
            self.total_fees_paid_usd += fee_usd
        else:
            self.total_rebates_earned_usd += abs(fee_usd)

        # Update rolling maker ratio (EMA)
        alpha = 0.02
        self.maker_fill_ratio += alpha * ((1.0 if is_maker else 0.0) - self.maker_fill_ratio)

        # Track monthly volume for tier estimation
        self._monthly_volume += size_usd
        self._maybe_update_tier()

    def _maybe_update_tier(self):
        """Check if monthly volume qualifies for better fee tier."""
        now = time.time()
        # Reset monthly volume every 30 days
        if now - self._month_start > 30 * 86400:
            self._monthly_volume = 0.0
            self._month_start = now
            return

        # Estimate 30-day volume from current rate
        elapsed_days = max(1, (now - self._month_start) / 86400)
        projected_30d = self._monthly_volume / elapsed_days * 30

        # Find applicable tier
        for threshold, maker_r, taker_r in reversed(OKX_TIERS):
            if projected_30d >= threshold:
                self.maker_rate_bps = maker_r
                self.taker_rate_bps = taker_r
                break

    def estimate_cost_bps(self, strategy: str = "binance_mm") -> float:
        """Estimate round-trip cost in bps for a strategy.
        Uses actual maker/taker ratio to weight the fee expectation.
        """
        stats = self._strategy_stats.get(strategy)
        if stats and stats["fills"] >= 20:
            total = stats["maker_fills"] + stats["taker_fills"]
            maker_ratio = stats["maker_fills"] / total if total > 0 else 0.5
        else:
            maker_ratio = self.maker_fill_ratio

        # Entry cost: weighted average of maker/taker
        entry_cost = maker_ratio * self.maker_rate_bps + (1 - maker_ratio) * self.taker_rate_bps
        # Exit cost: typically higher maker rate for MM (we post limits on exit)
        exit_cost = self.maker_rate_bps  # assuming we usually exit as maker

        # Round-trip: entry + exit (both applied to notional)
        return entry_cost + exit_cost

    def adjust_pnl(self, gross_pnl_usd: float, size_usd: float,
                   entry_is_maker: bool = False, exit_is_maker: bool = True) -> float:
        """Apply accurate fee model to a trade's PnL.
        Returns net PnL after fees.
        """
        # Entry fee
        if entry_is_maker:
            entry_fee = size_usd * self.maker_rate_bps / 10000
        else:
            entry_fee = size_usd * self.taker_rate_bps / 10000

        # Exit fee
        if exit_is_maker:
            exit_fee = size_usd * self.maker_rate_bps / 10000
        else:
            exit_fee = size_usd * self.taker_rate_bps / 10000

        # Net PnL = gross - entry_fee - exit_fee
        # Note: maker_rate is negative (rebate), so subtracting it ADDS to PnL
        return gross_pnl_usd - entry_fee - exit_fee

    def effective_round_trip_bps(self) -> float:
        """What we're actually paying per round-trip on average."""
        if not self._fills:
            return self.taker_rate_bps + abs(self.maker_rate_bps)  # worst case

        recent = [f for f in self._fills if time.time() - f.timestamp < 3600]  # last hour
        if not recent:
            recent = list(self._fills)[-100:]

        total_vol = sum(f.size_usd for f in recent)
        if total_vol == 0:
            return 6.0  # fallback

        # Average fee per dollar
        total_fee_bps = sum(f.fee_bps * f.size_usd for f in recent) / total_vol
        # Round-trip = 2x average (entry + exit)
        return total_fee_bps * 2

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        return {
            "maker_fill_ratio": round(self.maker_fill_ratio, 3),
            "effective_maker_bps": round(self.maker_rate_bps, 2),
            "effective_taker_bps": round(self.taker_rate_bps, 2),
            "estimated_tier": self._get_tier_name(),
            "round_trip_bps": round(self.effective_round_trip_bps(), 2),
            "monthly_volume_usd": round(self._monthly_volume, 0),
            "total_fees_paid_usd": round(self.total_fees_paid_usd, 4),
            "total_rebates_earned_usd": round(self.total_rebates_earned_usd, 4),
            "net_fees_usd": round(self.total_fees_paid_usd - self.total_rebates_earned_usd, 4),
            "per_strategy": {
                k: {
                    "maker_rate": round(v["maker_fills"] / max(1, v["fills"]), 3),
                    "fills": v["fills"],
                    "volume_usd": round(v["total_volume"], 0),
                }
                for k, v in self._strategy_stats.items()
            },
        }

    def _get_tier_name(self) -> str:
        """Human-readable tier name."""
        if self._monthly_volume >= 50_000_000:
            return "VIP4"
        elif self._monthly_volume >= 20_000_000:
            return "VIP3"
        elif self._monthly_volume >= 10_000_000:
            return "VIP2"
        elif self._monthly_volume >= 5_000_000:
            return "VIP1"
        return "VIP0"


# Singleton instance
_fee_model = None


def get_fee_model() -> AdaptiveFeeModel:
    """Get or create the global fee model."""
    global _fee_model
    if _fee_model is None:
        _fee_model = AdaptiveFeeModel()
    return _fee_model
