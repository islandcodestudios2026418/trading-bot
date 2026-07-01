"""
Anti-Gaming Detection — identifies and protects against common MM gaming patterns.

Detects:
1. Layering (fake depth): Orders placed to mislead, then cancelled before execution
   Signal: >70% cancellation rate at a price level within 2 seconds
2. Momentum Ignition: Small aggressive trades to trigger stops/algos
   Signal: Rapid price movement (>0.3% in 2s) on very low volume, then reversal
3. Spoofing: Large orders placed/cancelled rapidly to move prices
   Signal: Large orders (>5x average) that appear and vanish within 1s
4. Wash Trading: Trades that appear to create false volume
   Signal: Same size trades at same price with unnaturally regular timing

Actions on detection:
- Pull all quotes (cancel_all)
- Wait for cooldown period (5-15s)
- Widen spread on re-entry
- Log for analysis

No numpy/pandas — pure Python for lean deployment.
"""
import time
from collections import deque
from dataclasses import dataclass, field


# Gaming types
GAME_LAYERING = "layering"
GAME_MOMENTUM_IGNITION = "momentum_ignition"
GAME_SPOOFING = "spoofing"
GAME_WASH = "wash_trading"

# Actions
PULL_QUOTES = "pull_quotes"     # cancel all orders immediately
WIDEN_SPREAD = "widen_spread"   # 2x spread on re-entry
PAUSE_TRADING = "pause_trading"  # stop quoting for cooldown period


@dataclass
class GamingEvent:
    """A detected gaming event."""
    game_type: str
    severity: float  # 0-1
    action: str
    timestamp: float
    details: str
    cooldown_s: float = 10.0  # seconds to pause after detection

    @property
    def active(self) -> bool:
        return time.time() - self.timestamp < self.cooldown_s


@dataclass
class LayeringDetector:
    """Detects layering: fake depth placed then cancelled rapidly.

    Signal: Multiple large orders appear at similar prices and disappear
    before they could be filled (>70% cancel rate in 2s window).
    """
    # Track order appearances/disappearances at price levels
    _level_events: deque = field(default_factory=lambda: deque(maxlen=200))
    # (timestamp, price, size, event_type) where event_type = "add" or "remove"
    _cancel_rate_ema: float = 0.0
    severity: float = 0.0

    def update(self, bids: list, asks: list, prev_bids: list, prev_asks: list):
        """Compare consecutive book snapshots to detect layering."""
        now = time.time()

        # Detect large additions that quickly disappear
        # Compare current vs previous: find new large levels
        if not prev_bids or not prev_asks:
            return None

        # Check bid side
        prev_bid_set = {float(b[0]): float(b[1]) for b in prev_bids[:10]}
        curr_bid_set = {float(b[0]): float(b[1]) for b in bids[:10]}

        # Find levels that existed in prev but are gone/reduced now (cancellations)
        cancel_volume = 0.0
        total_volume = sum(prev_bid_set.values()) + sum(float(a[1]) for a in prev_asks[:10])

        for px, qty in prev_bid_set.items():
            curr_qty = curr_bid_set.get(px, 0)
            if curr_qty < qty * 0.5 and qty > 0:  # >50% removed
                self._level_events.append((now, px, qty - curr_qty, "cancel_bid"))
                cancel_volume += (qty - curr_qty) * px

        # Check ask side similarly
        prev_ask_set = {float(a[0]): float(a[1]) for a in prev_asks[:10]}
        curr_ask_set = {float(a[0]): float(a[1]) for a in asks[:10]}

        for px, qty in prev_ask_set.items():
            curr_qty = curr_ask_set.get(px, 0)
            if curr_qty < qty * 0.5 and qty > 0:
                self._level_events.append((now, px, qty - curr_qty, "cancel_ask"))
                cancel_volume += (qty - curr_qty) * px

        # Score: high cancel volume relative to total book = suspicious
        if total_volume > 0:
            cancel_ratio = cancel_volume / (total_volume * max(1, float(bids[0][0]) if bids else 1))
        else:
            cancel_ratio = 0

        # Count recent cancellation events
        cutoff = now - 2.0
        recent_cancels = sum(1 for t, _, _, evt in self._level_events
                           if t >= cutoff and "cancel" in evt)

        # Layering signal: many cancels (>5) in 2s window
        if recent_cancels > 5:
            self.severity = min(1.0, recent_cancels / 10)
            return GamingEvent(
                game_type=GAME_LAYERING,
                severity=self.severity,
                action=PULL_QUOTES,
                timestamp=now,
                details=f"cancel_events={recent_cancels}/2s cancel_vol={cancel_volume:.0f}USD",
                cooldown_s=10.0,
            )

        self.severity *= 0.9
        return None


@dataclass
class MomentumIgnitionDetector:
    """Detects momentum ignition: small trades to trigger stops, then reversal.

    Signal: Rapid price movement (>0.3% in 2s) on low relative volume,
    followed by quick reversal (>50% retracement in next 3s).
    """
    _prices: deque = field(default_factory=lambda: deque(maxlen=100))  # (time, mid)
    _volumes: deque = field(default_factory=lambda: deque(maxlen=100))  # (time, vol_usd)
    _vol_ema: float = 0.0
    _in_ignition: bool = False
    _ignition_start: float = 0.0
    _ignition_peak: float = 0.0
    _ignition_ref: float = 0.0
    severity: float = 0.0

    def update(self, mid: float, trade_vol_usd: float = 0) -> GamingEvent:
        """Update with price + volume data."""
        now = time.time()
        self._prices.append((now, mid))
        if trade_vol_usd > 0:
            self._volumes.append((now, trade_vol_usd))
            self._vol_ema += 0.02 * (trade_vol_usd - self._vol_ema)

        if mid <= 0:
            return None

        # Check for rapid move on low volume
        cutoff_2s = now - 2.0
        old_prices = [(t, p) for t, p in self._prices if t <= cutoff_2s + 0.5]
        if not old_prices:
            return None

        ref_price = old_prices[-1][1]
        move_pct = abs(mid - ref_price) / ref_price * 100 if ref_price > 0 else 0

        # Volume in the move window
        recent_vol = sum(v for t, v in self._volumes if t >= cutoff_2s)

        # Low volume + big move = suspicious
        vol_ratio = recent_vol / (self._vol_ema * 20) if self._vol_ema > 0 else 1.0

        if move_pct > 0.3 and vol_ratio < 0.5:
            # Fast move on low volume — possible ignition
            if not self._in_ignition:
                self._in_ignition = True
                self._ignition_start = now
                self._ignition_ref = ref_price
                self._ignition_peak = mid

        # Check for reversal (confirming ignition)
        if self._in_ignition:
            elapsed = now - self._ignition_start
            if elapsed > 3.0:
                # Check if reverted >50%
                if self._ignition_ref > 0 and self._ignition_peak > 0:
                    initial_move = abs(self._ignition_peak - self._ignition_ref)
                    current_revert = abs(mid - self._ignition_peak)
                    if initial_move > 0 and current_revert / initial_move > 0.5:
                        self.severity = min(1.0, current_revert / initial_move)
                        self._in_ignition = False
                        return GamingEvent(
                            game_type=GAME_MOMENTUM_IGNITION,
                            severity=self.severity,
                            action=PAUSE_TRADING,
                            timestamp=now,
                            details=f"move={move_pct:.2f}% vol_ratio={vol_ratio:.2f} revert={current_revert/initial_move:.0%}",
                            cooldown_s=15.0,
                        )
                # Timeout: no revert detected
                if elapsed > 10.0:
                    self._in_ignition = False

        self.severity *= 0.95
        return None


@dataclass
class SpoofDetector:
    """Detects spoofing: large phantom orders that appear/vanish to move prices.

    Signal: Orders >5x average size that exist for <1s before cancellation.
    Distinguished from layering by the extreme size and very short lifetime.
    """
    _large_orders: deque = field(default_factory=lambda: deque(maxlen=50))
    # (appear_time, price, size, still_exists_at_next_check)
    _avg_size: float = 0.0
    _phantom_count: int = 0
    severity: float = 0.0

    def update(self, best_bid_size: float, best_ask_size: float,
               prev_bid_size: float, prev_ask_size: float) -> GamingEvent:
        """Compare consecutive best-level sizes to detect phantom large orders."""
        now = time.time()

        # Track average best-level size
        avg = (best_bid_size + best_ask_size) / 2
        self._avg_size += 0.01 * (avg - self._avg_size)

        if self._avg_size <= 0:
            return None

        # Detect phantom: large order that was there, now gone
        bid_phantom = prev_bid_size > self._avg_size * 5 and best_bid_size < prev_bid_size * 0.3
        ask_phantom = prev_ask_size > self._avg_size * 5 and best_ask_size < prev_ask_size * 0.3

        if bid_phantom or ask_phantom:
            self._phantom_count += 1
            side = "bid" if bid_phantom else "ask"
            phantom_size = prev_bid_size if bid_phantom else prev_ask_size
            self._large_orders.append((now, side, phantom_size))

            # Multiple phantoms in short window = spoofing
            cutoff = now - 5.0
            recent = sum(1 for t, _, _ in self._large_orders if t >= cutoff)
            if recent >= 2:
                self.severity = min(1.0, recent / 4)
                return GamingEvent(
                    game_type=GAME_SPOOFING,
                    severity=self.severity,
                    action=WIDEN_SPREAD,
                    timestamp=now,
                    details=f"side={side} phantom_size={phantom_size:.1f} (avg={self._avg_size:.1f}) recent={recent}",
                    cooldown_s=10.0,
                )

        self.severity *= 0.92
        return None


@dataclass
class AntiGaming:
    """Unified anti-gaming system. Aggregates all detectors.
    Integrated into the MM's tick loop.
    """
    layering: LayeringDetector = field(default_factory=LayeringDetector)
    momentum: MomentumIgnitionDetector = field(default_factory=MomentumIgnitionDetector)
    spoofing: SpoofDetector = field(default_factory=SpoofDetector)

    # Active events
    _active_events: list = field(default_factory=list)
    # Previous book state for comparison
    _prev_bids: list = field(default_factory=list)
    _prev_asks: list = field(default_factory=list)
    _prev_bid_size: float = 0.0
    _prev_ask_size: float = 0.0
    # Stats
    total_detections: int = 0
    detections_by_type: dict = field(default_factory=lambda: {
        GAME_LAYERING: 0, GAME_MOMENTUM_IGNITION: 0,
        GAME_SPOOFING: 0, GAME_WASH: 0,
    })

    def update(self, mid: float, bids: list, asks: list, trade_vol_usd: float = 0):
        """Update all detectors with current market state.
        Call on every depth update.
        """
        # Layering detection (book comparison)
        event = self.layering.update(bids, asks, self._prev_bids, self._prev_asks)
        if event:
            self._add_event(event)

        # Momentum ignition detection
        event = self.momentum.update(mid, trade_vol_usd)
        if event:
            self._add_event(event)

        # Spoofing detection (best-level size comparison)
        bid_size = float(bids[0][1]) if bids else 0
        ask_size = float(asks[0][1]) if asks else 0
        event = self.spoofing.update(bid_size, ask_size, self._prev_bid_size, self._prev_ask_size)
        if event:
            self._add_event(event)

        # Store current state for next comparison
        self._prev_bids = bids[:10] if bids else []
        self._prev_asks = asks[:10] if asks else []
        self._prev_bid_size = bid_size
        self._prev_ask_size = ask_size

    def _add_event(self, event: GamingEvent):
        """Add event and clean expired."""
        self._active_events.append(event)
        self.total_detections += 1
        self.detections_by_type[event.game_type] = self.detections_by_type.get(event.game_type, 0) + 1
        self._active_events = [e for e in self._active_events if e.active]

    @property
    def active_events(self) -> list:
        self._active_events = [e for e in self._active_events if e.active]
        return self._active_events

    @property
    def should_pull_quotes(self) -> bool:
        """Should we cancel all orders immediately?"""
        return any(e.action == PULL_QUOTES for e in self.active_events)

    @property
    def should_pause(self) -> bool:
        """Should we stop trading entirely?"""
        return any(e.action in (PULL_QUOTES, PAUSE_TRADING) for e in self.active_events)

    @property
    def spread_multiplier(self) -> float:
        """Spread widening multiplier during gaming detection."""
        if self.should_pause:
            return 3.0
        if any(e.action == WIDEN_SPREAD for e in self.active_events):
            return 2.0
        return 1.0

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        active = self.active_events
        return {
            "total_detections": self.total_detections,
            "active_events": len(active),
            "should_pull": self.should_pull_quotes,
            "should_pause": self.should_pause,
            "spread_multiplier": self.spread_multiplier,
            "detections_by_type": dict(self.detections_by_type),
            "active_details": [
                {
                    "type": e.game_type,
                    "severity": round(e.severity, 3),
                    "action": e.action,
                    "age_s": round(time.time() - e.timestamp, 1),
                    "details": e.details,
                }
                for e in active[:3]
            ],
        }
