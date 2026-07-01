"""
Inactivity Detection + Auto-Recovery.

Monitors trading activity per symbol and per strategy.
If no fills occur for an extended period, something may be wrong:
- Our spread is too wide (no one wants our prices)
- Exchange connectivity issue
- Market became illiquid
- All signals are gated (too strict thresholds)

Auto-recovery actions:
1. After 3 min inactivity: tighten spread by 20% (more aggressive quoting)
2. After 5 min inactivity: reduce entry thresholds by 30%
3. After 10 min inactivity: alert via Telegram + force requote
4. After 30 min inactivity: check WS health + attempt reconnect

No numpy/pandas — pure Python for lean deployment.
"""
import time
from collections import deque
from dataclasses import dataclass, field


# Inactivity levels
LEVEL_NORMAL = "normal"           # < 3 min since last fill
LEVEL_MILD = "mild_inactivity"    # 3-5 min
LEVEL_MODERATE = "moderate"       # 5-10 min
LEVEL_SEVERE = "severe"           # 10-30 min
LEVEL_CRITICAL = "critical"       # > 30 min

# Thresholds (seconds)
THRESHOLD_MILD = 180       # 3 minutes
THRESHOLD_MODERATE = 300   # 5 minutes
THRESHOLD_SEVERE = 600     # 10 minutes
THRESHOLD_CRITICAL = 1800  # 30 minutes


@dataclass
class SymbolActivity:
    """Track activity for a single symbol."""
    symbol: str
    last_fill_time: float = 0.0
    last_quote_time: float = 0.0
    last_depth_update: float = 0.0
    fills_last_hour: int = 0
    _fill_times: deque = field(default_factory=lambda: deque(maxlen=100))
    # Auto-recovery state
    recovery_attempts: int = 0
    last_recovery: float = 0.0
    spread_tightening: float = 1.0  # multiplier applied to spread

    @property
    def inactive_seconds(self) -> float:
        """Seconds since last fill."""
        if self.last_fill_time == 0:
            return 0  # never filled, don't trigger yet
        return time.time() - self.last_fill_time

    @property
    def level(self) -> str:
        """Current inactivity level."""
        secs = self.inactive_seconds
        if secs >= THRESHOLD_CRITICAL:
            return LEVEL_CRITICAL
        elif secs >= THRESHOLD_SEVERE:
            return LEVEL_SEVERE
        elif secs >= THRESHOLD_MODERATE:
            return LEVEL_MODERATE
        elif secs >= THRESHOLD_MILD:
            return LEVEL_MILD
        return LEVEL_NORMAL

    @property
    def avg_fill_interval_s(self) -> float:
        """Average time between fills."""
        if len(self._fill_times) < 2:
            return 0
        intervals = [self._fill_times[i] - self._fill_times[i-1]
                     for i in range(1, len(self._fill_times))]
        return sum(intervals) / len(intervals)

    def record_fill(self):
        """Record a new fill."""
        now = time.time()
        self.last_fill_time = now
        self._fill_times.append(now)
        self.fills_last_hour = sum(1 for t in self._fill_times if now - t < 3600)
        # Reset recovery state on fill
        self.recovery_attempts = 0
        self.spread_tightening = 1.0

    def record_quote(self):
        """Record a new quote placement."""
        self.last_quote_time = time.time()

    def record_depth(self):
        """Record a depth update (proves connectivity)."""
        self.last_depth_update = time.time()


@dataclass
class InactivityMonitor:
    """Monitors all symbols for inactivity and triggers auto-recovery."""
    _symbols: dict = field(default_factory=dict)  # symbol → SymbolActivity
    _alerted: dict = field(default_factory=dict)  # symbol → last_alert_time (prevent spam)
    alert_cooldown: float = 300.0  # 5 min between alerts per symbol

    def register_symbol(self, symbol: str):
        """Register a symbol for monitoring."""
        if symbol not in self._symbols:
            self._symbols[symbol] = SymbolActivity(symbol=symbol)

    def record_fill(self, symbol: str):
        """Record a fill for a symbol."""
        self.register_symbol(symbol)
        self._symbols[symbol].record_fill()

    def record_quote(self, symbol: str):
        """Record a quote for a symbol."""
        self.register_symbol(symbol)
        self._symbols[symbol].record_quote()

    def record_depth(self, symbol: str):
        """Record a depth update for a symbol."""
        self.register_symbol(symbol)
        self._symbols[symbol].record_depth()

    def check_all(self) -> list:
        """Check all symbols for inactivity. Returns list of recovery actions."""
        actions = []
        for symbol, activity in self._symbols.items():
            level = activity.level
            if level == LEVEL_NORMAL:
                continue

            action = self._get_recovery_action(activity)
            if action:
                actions.append(action)

        return actions

    def _get_recovery_action(self, activity: SymbolActivity) -> dict:
        """Determine recovery action for an inactive symbol."""
        now = time.time()
        level = activity.level
        symbol = activity.symbol

        # Prevent recovery action spam
        if now - activity.last_recovery < 60:
            return None

        if level == LEVEL_MILD:
            # Tighten spread 20%
            activity.spread_tightening = 0.8
            activity.recovery_attempts += 1
            activity.last_recovery = now
            return {
                "symbol": symbol,
                "action": "tighten_spread",
                "multiplier": 0.8,
                "level": level,
                "inactive_s": round(activity.inactive_seconds, 0),
            }

        elif level == LEVEL_MODERATE:
            # More aggressive: tighten 40%
            activity.spread_tightening = 0.6
            activity.recovery_attempts += 1
            activity.last_recovery = now
            return {
                "symbol": symbol,
                "action": "tighten_spread",
                "multiplier": 0.6,
                "level": level,
                "inactive_s": round(activity.inactive_seconds, 0),
            }

        elif level == LEVEL_SEVERE:
            # Alert + force requote
            activity.recovery_attempts += 1
            activity.last_recovery = now
            should_alert = now - self._alerted.get(symbol, 0) > self.alert_cooldown
            if should_alert:
                self._alerted[symbol] = now
            return {
                "symbol": symbol,
                "action": "alert_and_requote",
                "alert": should_alert,
                "level": level,
                "inactive_s": round(activity.inactive_seconds, 0),
                "recovery_attempts": activity.recovery_attempts,
            }

        elif level == LEVEL_CRITICAL:
            # Critical: check connectivity
            activity.recovery_attempts += 1
            activity.last_recovery = now
            should_alert = now - self._alerted.get(symbol, 0) > self.alert_cooldown
            if should_alert:
                self._alerted[symbol] = now
            return {
                "symbol": symbol,
                "action": "check_connectivity",
                "alert": should_alert,
                "level": level,
                "inactive_s": round(activity.inactive_seconds, 0),
                "last_depth_age_s": round(now - activity.last_depth_update, 0) if activity.last_depth_update else -1,
            }

        return None

    def get_spread_multiplier(self, symbol: str) -> float:
        """Get the spread tightening multiplier for a symbol.
        < 1.0 means tighten (more aggressive), 1.0 = normal.
        """
        activity = self._symbols.get(symbol)
        if not activity:
            return 1.0
        return activity.spread_tightening

    def get_threshold_multiplier(self, symbol: str) -> float:
        """Get entry threshold multiplier for a symbol.
        < 1.0 means easier entry (less strict OFI threshold).
        Applied when inactivity suggests we're too strict.
        """
        activity = self._symbols.get(symbol)
        if not activity:
            return 1.0
        level = activity.level
        if level == LEVEL_MODERATE:
            return 0.7  # 30% easier entry
        elif level in (LEVEL_SEVERE, LEVEL_CRITICAL):
            return 0.5  # 50% easier entry
        elif level == LEVEL_MILD:
            return 0.85  # 15% easier
        return 1.0

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        return {
            "symbols": {
                sym: {
                    "level": act.level,
                    "inactive_s": round(act.inactive_seconds, 0),
                    "fills_last_hour": act.fills_last_hour,
                    "avg_fill_interval_s": round(act.avg_fill_interval_s, 1),
                    "spread_tightening": act.spread_tightening,
                    "recovery_attempts": act.recovery_attempts,
                }
                for sym, act in self._symbols.items()
            },
        }


# Singleton
_monitor = None


def get_inactivity_monitor() -> InactivityMonitor:
    """Get or create the global inactivity monitor."""
    global _monitor
    if _monitor is None:
        _monitor = InactivityMonitor()
    return _monitor
