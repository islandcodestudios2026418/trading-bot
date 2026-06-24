"""
Risk management module — drawdown limits, position sizing, kill switch, circuit breaker.
Must be checked before every trade.
"""
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

TW_TZ = timezone(timedelta(hours=8))


@dataclass
class CircuitBreaker:
    """Auto-pauses trading on rapid adverse conditions.
    Triggers: rapid losses, API errors, or fast equity drops.
    """
    # Config
    max_losses_per_min: int = 3  # 3+ consecutive losses in <60s = flash crash
    max_api_errors_30s: int = 5  # API degradation
    max_equity_drop_pct: float = 3.0  # 3% equity drop in 5 min
    pause_duration: int = 300  # pause for 5 minutes after trigger

    # State
    _loss_times: deque = field(default_factory=lambda: deque(maxlen=10))
    _error_times: deque = field(default_factory=lambda: deque(maxlen=20))
    _equity_history: deque = field(default_factory=lambda: deque(maxlen=60))  # 5min at 5s intervals
    tripped: bool = False
    trip_reason: str = ""
    trip_time: float = 0.0

    def record_loss(self):
        """Record a losing trade."""
        self._loss_times.append(time.time())

    def record_api_error(self):
        """Record an API error."""
        self._error_times.append(time.time())

    def record_equity(self, equity: float):
        """Record current equity for drawdown detection."""
        self._equity_history.append((time.time(), equity))

    def check(self) -> tuple[bool, str]:
        """Check if circuit breaker should trip. Returns (ok, reason)."""
        if self.tripped:
            # Auto-reset after pause duration
            if time.time() - self.trip_time > self.pause_duration:
                self.tripped = False
                self.trip_reason = ""
                return True, "OK (breaker reset)"
            return False, f"CIRCUIT BREAKER: {self.trip_reason} ({int(self.pause_duration - (time.time() - self.trip_time))}s left)"

        now = time.time()

        # Check rapid losses (3+ in 60s)
        recent_losses = sum(1 for t in self._loss_times if now - t < 60)
        if recent_losses >= self.max_losses_per_min:
            self._trip(f"Rapid losses: {recent_losses} in 60s")
            return False, self.trip_reason

        # Check API errors (5+ in 30s)
        recent_errors = sum(1 for t in self._error_times if now - t < 30)
        if recent_errors >= self.max_api_errors_30s:
            self._trip(f"API errors: {recent_errors} in 30s")
            return False, self.trip_reason

        # Check fast equity drop (3% in 5 min)
        if len(self._equity_history) >= 2:
            oldest = self._equity_history[0]
            if now - oldest[0] <= 300 and oldest[1] > 0:  # within 5 min window
                drop_pct = (oldest[1] - self._equity_history[-1][1]) / oldest[1] * 100
                if drop_pct >= self.max_equity_drop_pct:
                    self._trip(f"Equity drop: {drop_pct:.1f}% in {(now - oldest[0]) / 60:.1f}min")
                    return False, self.trip_reason

        return True, "OK"

    def _trip(self, reason: str):
        self.tripped = True
        self.trip_reason = reason
        self.trip_time = time.time()


@dataclass
class RiskManager:
    capital: float = float(os.getenv("CAPITAL", "500"))
    max_drawdown_pct: float = float(os.getenv("MAX_DRAWDOWN_PCT", "5"))  # kill at 5% loss
    max_position_pct: float = float(os.getenv("MAX_POSITION_PCT", "10"))  # max 10% capital per pair
    max_total_exposure_pct: float = float(os.getenv("MAX_EXPOSURE_PCT", "50"))  # max 50% deployed
    max_loss_per_trade_pct: float = float(os.getenv("MAX_LOSS_TRADE_PCT", "1"))  # 1% per trade
    cooldown_after_loss: int = 60  # seconds pause after consecutive losses

    # State
    peak_equity: float = field(init=False)
    current_pnl: float = 0.0
    total_exposure: float = 0.0
    consecutive_losses: int = 0
    last_loss_time: float = 0.0
    killed: bool = False
    kill_reason: str = ""
    circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)

    def __post_init__(self):
        self.peak_equity = self.capital

    @property
    def equity(self) -> float:
        return self.capital + self.current_pnl

    @property
    def drawdown_pct(self) -> float:
        if self.peak_equity <= 0:
            return 0
        return (self.peak_equity - self.equity) / self.peak_equity * 100

    @property
    def max_position_size(self) -> float:
        return self.capital * self.max_position_pct / 100

    def can_trade(self) -> tuple[bool, str]:
        """Check if trading is allowed. Returns (allowed, reason)."""
        if self.killed:
            return False, f"KILLED: {self.kill_reason}"

        # Circuit breaker check
        cb_ok, cb_reason = self.circuit_breaker.check()
        if not cb_ok:
            return False, cb_reason

        # Drawdown kill switch
        if self.drawdown_pct >= self.max_drawdown_pct:
            self.killed = True
            self.kill_reason = f"Max drawdown {self.drawdown_pct:.1f}% >= {self.max_drawdown_pct}%"
            return False, self.kill_reason

        # Total exposure limit
        if self.total_exposure >= self.capital * self.max_total_exposure_pct / 100:
            return False, f"Max exposure reached: ${self.total_exposure:.0f}"

        # Cooldown after consecutive losses
        if self.consecutive_losses >= 3:
            elapsed = time.time() - self.last_loss_time
            if elapsed < self.cooldown_after_loss:
                return False, f"Cooling down ({self.cooldown_after_loss - elapsed:.0f}s left)"

        return True, "OK"

    def size_order(self, spread_bps: float, volatility: float = 1.0) -> float:
        """Calculate order size based on spread and risk budget."""
        base = self.max_position_size
        # Scale down if in drawdown
        dd_factor = max(0.3, 1.0 - self.drawdown_pct / self.max_drawdown_pct)
        # Scale up for wider spreads (more profitable)
        spread_factor = min(2.0, spread_bps / 20)
        # Scale down for high volatility
        vol_factor = max(0.5, 1.0 / volatility)
        return round(base * dd_factor * spread_factor * vol_factor, 2)

    def record_trade(self, pnl: float):
        """Update state after a trade completes."""
        self.current_pnl += pnl
        if pnl < 0:
            self.consecutive_losses += 1
            self.last_loss_time = time.time()
            self.circuit_breaker.record_loss()
        else:
            self.consecutive_losses = 0
        # Update peak
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity
        # Record equity for circuit breaker
        self.circuit_breaker.record_equity(self.equity)

    def add_exposure(self, amount: float):
        self.total_exposure += abs(amount)

    def remove_exposure(self, amount: float):
        self.total_exposure = max(0, self.total_exposure - abs(amount))

    def status(self) -> str:
        return (f"Equity: ${self.equity:.2f} | DD: {self.drawdown_pct:.1f}% "
                f"| Exposure: ${self.total_exposure:.0f}/{self.capital * self.max_total_exposure_pct / 100:.0f} "
                f"| Losses: {self.consecutive_losses} | Kill: {'YES' if self.killed else 'no'}")

    def reset_kill(self):
        """Manual reset after investigation."""
        self.killed = False
        self.kill_reason = ""
        self.consecutive_losses = 0


if __name__ == "__main__":
    rm = RiskManager(capital=500)
    print(f"Initial: {rm.status()}")
    print(f"Max position: ${rm.max_position_size:.0f}")
    print(f"Order size (30bps spread): ${rm.size_order(30):.2f}")
    print(f"Order size (80bps spread): ${rm.size_order(80):.2f}")

    # Simulate losses
    for i in range(4):
        rm.record_trade(-2.0)
        ok, reason = rm.can_trade()
        print(f"After loss #{i+1}: can_trade={ok} ({reason})")
    print(f"Final: {rm.status()}")
