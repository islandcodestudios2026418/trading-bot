"""
Signal attribution — tracks which signals contributed to each trade and their PnL.
Auto-disables signals with persistent negative attribution (protect capital).
"""
from collections import deque
from dataclasses import dataclass, field


@dataclass
class SignalAttribution:
    """Tracks per-signal PnL contribution. Signals with negative attribution get auto-disabled."""
    # Per-signal stats
    _signal_pnl: dict = field(default_factory=lambda: {})  # signal_name → total PnL
    _signal_count: dict = field(default_factory=lambda: {})  # signal_name → trade count
    _signal_recent: dict = field(default_factory=lambda: {})  # signal_name → deque of last 50 PnLs
    # Auto-disable: signals disabled when consistently negative
    disabled_signals: set = field(default_factory=set)
    _eval_interval: int = 50  # evaluate after N trades per signal
    _disable_threshold: float = -0.5  # disable if avg PnL per trade < this (negative)
    _reenable_check: int = 200  # re-check disabled signals every N total trades
    _total_trades: int = 0

    def record_entry(self, active_signals: dict) -> dict:
        """Call at trade entry. Returns snapshot of active signals for later attribution.
        active_signals: {signal_name: strength} e.g. {"ofi": 0.45, "obi": 0.2, "vpin_safe": True}
        """
        return dict(active_signals)

    def record_exit(self, entry_signals: dict, pnl: float):
        """Attribute PnL proportionally to signals that were active at entry."""
        self._total_trades += 1
        if not entry_signals:
            return

        # Attribute PnL equally to all active signals
        n_signals = len(entry_signals)
        per_signal_pnl = pnl / n_signals

        for sig_name in entry_signals:
            if sig_name not in self._signal_pnl:
                self._signal_pnl[sig_name] = 0.0
                self._signal_count[sig_name] = 0
                self._signal_recent[sig_name] = deque(maxlen=50)
            self._signal_pnl[sig_name] += per_signal_pnl
            self._signal_count[sig_name] += 1
            self._signal_recent[sig_name].append(per_signal_pnl)

            # Auto-disable check
            recent = self._signal_recent[sig_name]
            if len(recent) >= self._eval_interval:
                avg = sum(recent) / len(recent)
                if avg < self._disable_threshold and sig_name not in self.disabled_signals:
                    self.disabled_signals.add(sig_name)

        # Periodic re-enable check: give disabled signals another chance
        if self._total_trades % self._reenable_check == 0 and self.disabled_signals:
            to_reenable = set()
            for sig in list(self.disabled_signals):
                recent = self._signal_recent.get(sig)
                if recent and len(recent) >= 20:
                    # Check last 20 attributed trades — if improved, re-enable
                    last20 = list(recent)[-20:]
                    if sum(last20) / len(last20) >= 0:
                        to_reenable.add(sig)
            self.disabled_signals -= to_reenable

    def is_enabled(self, signal_name: str) -> bool:
        """Check if a signal is currently enabled (not auto-disabled)."""
        return signal_name not in self.disabled_signals

    def get_report(self) -> dict:
        """Get attribution report for all signals."""
        report = {}
        for sig in self._signal_pnl:
            count = self._signal_count.get(sig, 0)
            pnl = self._signal_pnl.get(sig, 0)
            recent = self._signal_recent.get(sig, deque())
            recent_avg = sum(recent) / len(recent) if recent else 0
            report[sig] = {
                "total_pnl": round(pnl, 4),
                "count": count,
                "avg_pnl": round(pnl / count, 6) if count > 0 else 0,
                "recent_avg": round(recent_avg, 6),
                "enabled": sig not in self.disabled_signals,
            }
        return report


# Global instance
attrib = SignalAttribution()
