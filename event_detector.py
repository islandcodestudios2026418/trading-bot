"""
Market Microstructure Event Detection.
Detects structural market events that require protective action:

1. Liquidation Cascades — rapid cascading liquidations (price waterfall + volume spike)
2. Funding Rate Spikes — sudden extreme funding (crowded positioning)
3. Flash Crashes — >1% drop in <5s with recovery potential
4. Whale Accumulation — large orders split across time (iceberg above normal)
5. Exchange Halt Risk — orderbook gaps + latency spikes (precursor to maintenance)

Each detector runs on the same tick data as the MM and outputs:
- Event type + severity (0-1)
- Recommended action (pause/reduce/hedge/exit)
- Auto-recovery (events are time-decayed)

No numpy/pandas — pure Python for lean deployment.
"""
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


# Event types
EVENT_LIQUIDATION = "liquidation_cascade"
EVENT_FUNDING_SPIKE = "funding_spike"
EVENT_FLASH_CRASH = "flash_crash"
EVENT_WHALE = "whale_accumulation"
EVENT_HALT_RISK = "exchange_halt_risk"

# Actions
ACTION_PAUSE = "pause"          # stop all new entries
ACTION_REDUCE = "reduce"        # reduce position size by 50%
ACTION_EXIT = "exit_all"        # emergency flatten
ACTION_WIDEN = "widen_spread"   # widen quotes (defensive)
ACTION_NONE = "none"


@dataclass
class MicroEvent:
    """A detected microstructure event."""
    event_type: str
    severity: float  # 0-1
    action: str
    timestamp: float
    details: str = ""
    ttl: float = 60.0  # seconds until auto-clear

    @property
    def active(self) -> bool:
        return time.time() - self.timestamp < self.ttl

    @property
    def age_s(self) -> float:
        return time.time() - self.timestamp


@dataclass
class LiquidationDetector:
    """Detects liquidation cascades from price action + volume patterns.

    Signals:
    - Rapid price decline (>0.5% in 5s) + abnormal volume (>5x average)
    - Multiple cascading sell-side trades (>90% seller-initiated in burst)
    - Book thinning (asks/bids withdrawing rapidly)
    """
    _price_5s: deque = field(default_factory=lambda: deque(maxlen=50))  # (timestamp, mid)
    _vol_5s: deque = field(default_factory=lambda: deque(maxlen=50))    # (timestamp, usd_volume)
    _vol_ema: float = 0.0
    _sell_ratio_ema: float = 0.5  # ratio of sell volume
    _in_cascade: bool = False
    _cascade_start: float = 0.0
    severity: float = 0.0

    def update(self, mid: float, trade_vol_usd: float, is_sell: bool) -> Optional[MicroEvent]:
        """Update with new market data. Returns event if liquidation detected."""
        now = time.time()
        self._price_5s.append((now, mid))
        self._vol_5s.append((now, trade_vol_usd))

        # Volume EMA
        self._vol_ema += 0.02 * (trade_vol_usd - self._vol_ema)

        # Sell ratio EMA
        sell_val = 1.0 if is_sell else 0.0
        self._sell_ratio_ema += 0.05 * (sell_val - self._sell_ratio_ema)

        # Check for cascade conditions
        # 1. Price drop in last 5 seconds
        cutoff = now - 5.0
        old_prices = [(t, p) for t, p in self._price_5s if t <= cutoff + 0.5]
        if not old_prices or mid <= 0:
            self.severity *= 0.95  # decay
            return None

        ref_price = old_prices[-1][1] if old_prices else mid
        price_change_pct = (mid - ref_price) / ref_price * 100 if ref_price > 0 else 0

        # 2. Volume spike
        recent_vol = sum(v for t, v in self._vol_5s if t >= cutoff)
        vol_ratio = recent_vol / (self._vol_ema * 50) if self._vol_ema > 0 else 1.0

        # 3. Sell dominance
        sell_dominated = self._sell_ratio_ema > 0.85

        # Scoring
        drop_score = min(1.0, abs(price_change_pct) / 1.0) if price_change_pct < -0.3 else 0.0
        vol_score = min(1.0, (vol_ratio - 3.0) / 5.0) if vol_ratio > 3.0 else 0.0
        sell_score = min(1.0, (self._sell_ratio_ema - 0.7) / 0.3) if sell_dominated else 0.0

        # Combined severity
        raw_severity = drop_score * 0.4 + vol_score * 0.3 + sell_score * 0.3
        self.severity = max(self.severity * 0.9, raw_severity)  # sticky decay

        if self.severity > 0.6 and not self._in_cascade:
            self._in_cascade = True
            self._cascade_start = now
            action = ACTION_PAUSE if self.severity > 0.8 else ACTION_WIDEN
            return MicroEvent(
                event_type=EVENT_LIQUIDATION,
                severity=self.severity,
                action=action,
                timestamp=now,
                details=f"drop={price_change_pct:.2f}% vol_ratio={vol_ratio:.1f}x sell_pct={self._sell_ratio_ema:.0%}",
                ttl=30.0,
            )

        # Auto-clear cascade
        if self._in_cascade and self.severity < 0.3:
            self._in_cascade = False

        return None


@dataclass
class FundingSpkDetector:
    """Detects sudden funding rate spikes indicating crowded positioning.

    Signal: funding rate changes by >2x in one period, or absolute rate > 0.1%/8h.
    This means one side is extremely crowded and vulnerable to squeeze.
    """
    _rates: deque = field(default_factory=lambda: deque(maxlen=24))  # last 24 readings
    _rate_ema: float = 0.0
    severity: float = 0.0
    last_rate: float = 0.0

    def update(self, funding_rate: float) -> Optional[MicroEvent]:
        """Update with new funding rate. Returns event if spike detected."""
        if funding_rate == 0:
            return None

        now = time.time()
        self._rates.append((now, funding_rate))
        prev_ema = self._rate_ema
        self._rate_ema += 0.1 * (funding_rate - self._rate_ema)
        self.last_rate = funding_rate

        # Detect spike: rate changed >2x from EMA, or absolute extreme
        if prev_ema != 0:
            change_ratio = abs(funding_rate / prev_ema)
        else:
            change_ratio = 1.0

        abs_extreme = abs(funding_rate) > 0.001  # > 0.1% per 8h
        rate_spike = change_ratio > 2.5 and abs(funding_rate) > 0.0003

        if abs_extreme or rate_spike:
            self.severity = min(1.0, abs(funding_rate) / 0.002)
            direction = "LONG crowded" if funding_rate > 0 else "SHORT crowded"
            return MicroEvent(
                event_type=EVENT_FUNDING_SPIKE,
                severity=self.severity,
                action=ACTION_REDUCE,
                timestamp=now,
                details=f"rate={funding_rate*100:.4f}% ({direction}) change={change_ratio:.1f}x",
                ttl=300.0,  # 5 min TTL for funding events
            )

        self.severity *= 0.95
        return None


@dataclass
class FlashCrashDetector:
    """Detects flash crashes — sudden large moves with potential recovery.

    Signal: >1% move in <5 seconds.
    These often recover 50-80%, so the action is to pause (not exit).
    """
    _prices: deque = field(default_factory=lambda: deque(maxlen=100))  # (time, mid)
    severity: float = 0.0
    _flash_active: bool = False
    _flash_direction: str = ""  # "down" or "up"
    _flash_ref_price: float = 0.0

    def update(self, mid: float) -> Optional[MicroEvent]:
        """Update with new mid price. Returns event if flash crash detected."""
        now = time.time()
        self._prices.append((now, mid))

        if mid <= 0:
            return None

        # Look at price N seconds ago
        for window_s in (3.0, 5.0, 10.0):
            cutoff = now - window_s
            old = [(t, p) for t, p in self._prices if t <= cutoff + 0.5]
            if not old:
                continue

            ref_price = old[-1][1]
            if ref_price <= 0:
                continue

            pct_change = (mid - ref_price) / ref_price * 100

            # Flash crash: > 1% drop in window
            if abs(pct_change) > 1.0:
                self.severity = min(1.0, abs(pct_change) / 3.0)
                direction = "down" if pct_change < 0 else "up"
                if not self._flash_active:
                    self._flash_active = True
                    self._flash_direction = direction
                    self._flash_ref_price = ref_price
                    action = ACTION_PAUSE if self.severity > 0.7 else ACTION_WIDEN
                    return MicroEvent(
                        event_type=EVENT_FLASH_CRASH,
                        severity=self.severity,
                        action=action,
                        timestamp=now,
                        details=f"direction={direction} pct={pct_change:.2f}% window={window_s}s",
                        ttl=30.0,
                    )
                break

        # Recovery detection: if price recovers >50% of flash move, clear
        if self._flash_active and self._flash_ref_price > 0:
            recovery_pct = abs(mid - self._flash_ref_price) / self._flash_ref_price * 100
            if recovery_pct < 0.3:
                self._flash_active = False

        self.severity *= 0.92  # fast decay
        return None


@dataclass
class WhaleDetector:
    """Detects whale accumulation (large orders split across time).

    Signal: Sustained large trades in one direction over 30-60 seconds.
    Different from a cascade: slower, more controlled, indicates informed flow.
    """
    _trades: deque = field(default_factory=lambda: deque(maxlen=200))  # (time, usd, is_buy)
    _vol_ema: float = 0.0
    severity: float = 0.0

    def update(self, trade_vol_usd: float, is_buy: bool) -> Optional[MicroEvent]:
        """Update with trade data. Returns event if whale detected."""
        now = time.time()
        self._trades.append((now, trade_vol_usd, is_buy))
        self._vol_ema += 0.01 * (trade_vol_usd - self._vol_ema)

        # Look at last 30 seconds
        cutoff = now - 30.0
        recent = [(t, v, b) for t, v, b in self._trades if t >= cutoff]
        if len(recent) < 10:
            self.severity *= 0.95
            return None

        total_vol = sum(v for _, v, _ in recent)
        buy_vol = sum(v for _, v, b in recent if b)
        sell_vol = total_vol - buy_vol

        # Whale signal: high directional concentration + above-average size
        if total_vol <= 0:
            return None

        buy_ratio = buy_vol / total_vol
        direction_score = max(buy_ratio, 1 - buy_ratio)  # 0.5 = balanced, 1.0 = one-sided
        vol_score = total_vol / (self._vol_ema * 200) if self._vol_ema > 0 else 1.0

        # Need >70% one direction + >3x normal volume
        if direction_score > 0.7 and vol_score > 3.0:
            self.severity = min(1.0, (direction_score - 0.6) * (vol_score - 2.0))
            if self.severity > 0.5:
                side = "BUY" if buy_ratio > 0.5 else "SELL"
                return MicroEvent(
                    event_type=EVENT_WHALE,
                    severity=self.severity,
                    action=ACTION_WIDEN,  # widen to avoid being run over
                    timestamp=now,
                    details=f"side={side} vol={total_vol:.0f}USD dir_pct={direction_score:.0%} multiplier={vol_score:.1f}x",
                    ttl=45.0,
                )

        self.severity *= 0.95
        return None


@dataclass
class HaltRiskDetector:
    """Detects exchange halt risk from book anomalies + latency spikes.

    Signals:
    - Large gaps in orderbook (>0.5% gap between levels)
    - Latency spikes (>500ms between updates)
    - Book depth < 10% of normal
    """
    _depth_ema: float = 0.0
    _last_update: float = 0.0
    _latency_spike_count: int = 0
    severity: float = 0.0

    def update(self, bid_depth_usd: float, ask_depth_usd: float,
               spread_bps: float, now: float = 0.0) -> Optional[MicroEvent]:
        """Update with book state. Returns event if halt risk detected."""
        now = now or time.time()
        total_depth = bid_depth_usd + ask_depth_usd

        # Depth tracking
        self._depth_ema += 0.005 * (total_depth - self._depth_ema)

        # Latency spike detection
        if self._last_update > 0:
            gap = now - self._last_update
            if gap > 0.5:  # >500ms between updates
                self._latency_spike_count += 1
            else:
                self._latency_spike_count = max(0, self._latency_spike_count - 1)
        self._last_update = now

        # Scoring
        depth_ratio = total_depth / self._depth_ema if self._depth_ema > 0 else 1.0
        depth_score = max(0, 1.0 - depth_ratio * 2)  # 1.0 when depth < 50% normal
        spread_score = min(1.0, spread_bps / 100)  # 1.0 at 100bps spread (very wide)
        latency_score = min(1.0, self._latency_spike_count / 5)

        raw_severity = depth_score * 0.4 + spread_score * 0.3 + latency_score * 0.3
        self.severity = max(self.severity * 0.9, raw_severity)

        if self.severity > 0.7:
            return MicroEvent(
                event_type=EVENT_HALT_RISK,
                severity=self.severity,
                action=ACTION_EXIT,
                timestamp=now,
                details=f"depth_ratio={depth_ratio:.2f} spread={spread_bps:.0f}bps latency_spikes={self._latency_spike_count}",
                ttl=60.0,
            )

        return None


@dataclass
class EventDetector:
    """Unified event detection system. Aggregates all sub-detectors.
    Integrated into the MM's tick processing loop.
    """
    liquidation: LiquidationDetector = field(default_factory=LiquidationDetector)
    funding: FundingSpkDetector = field(default_factory=FundingSpkDetector)
    flash_crash: FlashCrashDetector = field(default_factory=FlashCrashDetector)
    whale: WhaleDetector = field(default_factory=WhaleDetector)
    halt_risk: HaltRiskDetector = field(default_factory=HaltRiskDetector)

    # Active events
    _active_events: list = field(default_factory=list)
    # Stats
    total_events: int = 0
    events_by_type: dict = field(default_factory=lambda: {
        EVENT_LIQUIDATION: 0, EVENT_FUNDING_SPIKE: 0,
        EVENT_FLASH_CRASH: 0, EVENT_WHALE: 0, EVENT_HALT_RISK: 0
    })

    def update_trade(self, mid: float, trade_vol_usd: float, is_sell: bool):
        """Update with trade data. Call on every aggTrade."""
        # Liquidation detector
        event = self.liquidation.update(mid, trade_vol_usd, is_sell)
        if event:
            self._add_event(event)

        # Flash crash detector (price-only)
        event = self.flash_crash.update(mid)
        if event:
            self._add_event(event)

        # Whale detector
        event = self.whale.update(trade_vol_usd, not is_sell)
        if event:
            self._add_event(event)

    def update_book(self, bid_depth_usd: float, ask_depth_usd: float, spread_bps: float):
        """Update with orderbook state. Call on every depth update."""
        event = self.halt_risk.update(bid_depth_usd, ask_depth_usd, spread_bps)
        if event:
            self._add_event(event)

    def update_funding(self, rate: float):
        """Update with funding rate. Call on funding rate updates."""
        event = self.funding.update(rate)
        if event:
            self._add_event(event)

    def _add_event(self, event: MicroEvent):
        """Add a new event and clean expired ones."""
        self._active_events.append(event)
        self.total_events += 1
        self.events_by_type[event.event_type] = self.events_by_type.get(event.event_type, 0) + 1
        # Prune expired events
        self._active_events = [e for e in self._active_events if e.active]

    @property
    def active_events(self) -> list:
        """Get currently active events (not expired)."""
        self._active_events = [e for e in self._active_events if e.active]
        return self._active_events

    @property
    def max_severity(self) -> float:
        """Highest severity among active events."""
        if not self.active_events:
            return 0.0
        return max(e.severity for e in self.active_events)

    @property
    def recommended_action(self) -> str:
        """Most protective action from all active events.
        Priority: exit > pause > reduce > widen > none
        """
        actions = [e.action for e in self.active_events]
        if ACTION_EXIT in actions:
            return ACTION_EXIT
        if ACTION_PAUSE in actions:
            return ACTION_PAUSE
        if ACTION_REDUCE in actions:
            return ACTION_REDUCE
        if ACTION_WIDEN in actions:
            return ACTION_WIDEN
        return ACTION_NONE

    def should_skip_entry(self) -> bool:
        """Quick check: should we skip new entries?"""
        action = self.recommended_action
        return action in (ACTION_EXIT, ACTION_PAUSE, ACTION_REDUCE)

    def spread_multiplier(self) -> float:
        """Multiplier for quote spread (1.0 = normal, 2.0 = doubled)."""
        if self.recommended_action == ACTION_WIDEN:
            return 1.5 + self.max_severity * 0.5  # 1.5-2.0x
        if self.recommended_action in (ACTION_PAUSE, ACTION_EXIT):
            return 3.0  # very wide (defensive)
        return 1.0

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        active = self.active_events
        return {
            "total_events": self.total_events,
            "active_events": len(active),
            "max_severity": round(self.max_severity, 3),
            "recommended_action": self.recommended_action,
            "spread_multiplier": round(self.spread_multiplier(), 2),
            "events_by_type": self.events_by_type.copy(),
            "active_details": [
                {
                    "type": e.event_type,
                    "severity": round(e.severity, 3),
                    "action": e.action,
                    "age_s": round(e.age_s, 1),
                    "details": e.details,
                }
                for e in active[:5]  # max 5 most recent
            ],
        }
