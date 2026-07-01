"""
Trade Journal — structured entry/exit logging to JSONL.

Records comprehensive signal state at every entry + exit for offline analysis:
- All signal values at time of entry (OFI, OBI, VPIN, regime, vol state, etc.)
- All signal values at time of exit
- Outcome (PnL, hold time, max drawdown, max profit)
- Market context (spread, depth, session, micro_state)
- Attribution (which signals contributed to the decision)

File: trade_journal.jsonl (one JSON object per line)
Rotation: daily files (trade_journal_YYYYMMDD.jsonl)

Designed for offline ML training: what signal combinations predict profitable trades?
No numpy/pandas — pure Python for lean deployment.
"""
import json
import os
import time
import gzip
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta

TW_TZ = timezone(timedelta(hours=8))
JOURNAL_DIR = os.getenv("JOURNAL_DIR", "journal")
JOURNAL_ENABLED = os.getenv("JOURNAL_ENABLED", "1") == "1"
COMPRESS_AFTER_DAYS = 1  # gzip files older than this


@dataclass
class EntrySnapshot:
    """Full signal state at time of entry."""
    # Core signals
    ofi: float = 0.0
    obi: float = 0.0
    obi_momentum: float = 0.0
    vpin: float = 0.0
    toxicity: float = 0.0
    arrival_intensity: float = 0.0
    volume_surge: float = 0.0
    institutional_flow: float = 0.0
    depth_pressure: float = 0.0
    spoof_score: float = 0.0
    kyle_lambda: float = 0.0
    # Regime
    regime: str = "neutral"
    regime_confidence: float = 0.0
    vr: float = 1.0
    hurst: float = 0.5
    autocorr: float = 0.0
    # Vol cluster
    vol_state: str = "normal"
    vol_bps: float = 0.0
    vol_percentile: float = 0.5
    garch_forecast: float = 0.0
    # Market context
    spread_bps: float = 0.0
    atr: float = 0.0
    mid_price: float = 0.0
    micro_state: str = "normal"
    session: str = "europe"
    # Position context
    position_before: float = 0.0
    momentum: int = 0
    kelly_fraction: float = 1.0
    # Event state
    event_action: str = "none"
    event_severity: float = 0.0
    # Anti-gaming
    gaming_active: bool = False


@dataclass
class TradeRecord:
    """Complete trade record for journal."""
    # Identity
    symbol: str
    side: str  # "buy" or "sell"
    strategy: str = "binance_mm"
    # Timing
    entry_time: float = 0.0
    exit_time: float = 0.0
    hold_time_s: float = 0.0
    # Prices
    entry_price: float = 0.0
    exit_price: float = 0.0
    size_usd: float = 0.0
    # PnL
    gross_pnl: float = 0.0
    fees: float = 0.0
    net_pnl: float = 0.0
    # Drawdown/runup during trade
    max_unrealized_profit: float = 0.0
    max_unrealized_loss: float = 0.0
    # Entry signals
    entry_signals: dict = field(default_factory=dict)
    # Exit signals (state at exit)
    exit_signals: dict = field(default_factory=dict)
    # Exit reason
    exit_reason: str = ""  # "signal_flip", "trailing_stop", "position_aging", "circuit_breaker"
    # Active signals at entry (for attribution)
    active_signals: list = field(default_factory=list)


class TradeJournal:
    """Manages structured trade logging to JSONL files."""

    def __init__(self):
        self._enabled = JOURNAL_ENABLED
        self._journal_dir = JOURNAL_DIR
        self._current_file = None
        self._current_date = ""
        self._pending_entries: dict = {}  # symbol → EntrySnapshot
        self._records_written = 0
        self._buffer: deque = deque(maxlen=100)  # recent trades for analysis

        # Ensure journal directory exists
        if self._enabled:
            os.makedirs(self._journal_dir, exist_ok=True)

    def _get_file_path(self) -> str:
        """Get current day's journal file path."""
        date_str = datetime.now(TW_TZ).strftime("%Y%m%d")
        if date_str != self._current_date:
            self._current_date = date_str
            self._current_file = os.path.join(self._journal_dir, f"trades_{date_str}.jsonl")
        return self._current_file

    def record_entry(self, symbol: str, side: str, entry_price: float,
                     size_usd: float, signals: dict = None, strategy: str = "binance_mm"):
        """Record a trade entry with full signal snapshot."""
        if not self._enabled:
            return

        snapshot = EntrySnapshot()
        if signals:
            for key, val in signals.items():
                if hasattr(snapshot, key):
                    setattr(snapshot, key, val)

        self._pending_entries[symbol] = {
            "snapshot": snapshot,
            "entry_time": time.time(),
            "entry_price": entry_price,
            "size_usd": size_usd,
            "side": side,
            "strategy": strategy,
            "max_profit": 0.0,
            "max_loss": 0.0,
        }

    def record_exit(self, symbol: str, exit_price: float, net_pnl: float,
                    exit_reason: str = "", exit_signals: dict = None,
                    active_signals: list = None):
        """Record a trade exit and write complete record to journal."""
        if not self._enabled:
            return

        entry_data = self._pending_entries.pop(symbol, None)
        if not entry_data:
            return  # no matching entry

        entry_time = entry_data["entry_time"]
        hold_time = time.time() - entry_time

        record = TradeRecord(
            symbol=symbol,
            side=entry_data["side"],
            strategy=entry_data["strategy"],
            entry_time=entry_time,
            exit_time=time.time(),
            hold_time_s=hold_time,
            entry_price=entry_data["entry_price"],
            exit_price=exit_price,
            size_usd=entry_data["size_usd"],
            gross_pnl=net_pnl,  # approximate (includes fees already)
            net_pnl=net_pnl,
            max_unrealized_profit=entry_data.get("max_profit", 0),
            max_unrealized_loss=entry_data.get("max_loss", 0),
            entry_signals=asdict(entry_data["snapshot"]),
            exit_signals=exit_signals or {},
            exit_reason=exit_reason,
            active_signals=active_signals or [],
        )

        self._write_record(record)
        self._buffer.append(record)

    def update_unrealized(self, symbol: str, unrealized_pnl: float):
        """Update max profit/loss for an open position."""
        if symbol in self._pending_entries:
            entry = self._pending_entries[symbol]
            entry["max_profit"] = max(entry.get("max_profit", 0), unrealized_pnl)
            entry["max_loss"] = min(entry.get("max_loss", 0), unrealized_pnl)

    def _write_record(self, record: TradeRecord):
        """Write a trade record to the JSONL file."""
        try:
            filepath = self._get_file_path()
            data = asdict(record)
            # Convert timestamps to ISO strings for readability
            data["entry_time_iso"] = datetime.fromtimestamp(
                record.entry_time, tz=TW_TZ).isoformat()
            data["exit_time_iso"] = datetime.fromtimestamp(
                record.exit_time, tz=TW_TZ).isoformat()

            with open(filepath, "a") as f:
                f.write(json.dumps(data) + "\n")
            self._records_written += 1
        except Exception:
            pass

    def get_metrics(self) -> dict:
        """Metrics for /metrics endpoint."""
        recent = list(self._buffer)
        if not recent:
            return {
                "enabled": self._enabled,
                "records_written": self._records_written,
                "pending_entries": len(self._pending_entries),
            }

        wins = [r for r in recent if r.net_pnl > 0]
        losses = [r for r in recent if r.net_pnl <= 0]
        return {
            "enabled": self._enabled,
            "records_written": self._records_written,
            "pending_entries": len(self._pending_entries),
            "recent_trades": len(recent),
            "recent_win_rate": round(len(wins) / len(recent), 3) if recent else 0,
            "recent_avg_pnl": round(sum(r.net_pnl for r in recent) / len(recent), 4) if recent else 0,
            "recent_avg_hold_s": round(sum(r.hold_time_s for r in recent) / len(recent), 1) if recent else 0,
            "top_signal_at_wins": self._top_signals(wins),
            "top_signal_at_losses": self._top_signals(losses),
        }

    def _top_signals(self, records: list) -> dict:
        """Find which signals are most active in winning/losing trades."""
        if not records:
            return {}
        # Count average absolute signal values across records
        signal_sums: dict = {}
        signal_counts: dict = {}
        for r in records:
            for key, val in r.entry_signals.items():
                if isinstance(val, (int, float)) and val != 0:
                    signal_sums[key] = signal_sums.get(key, 0) + abs(val)
                    signal_counts[key] = signal_counts.get(key, 0) + 1

        # Top 5 by average absolute value
        avgs = {k: signal_sums[k] / signal_counts[k]
                for k in signal_sums if signal_counts.get(k, 0) > 0}
        top = sorted(avgs.items(), key=lambda x: -x[1])[:5]
        return {k: round(v, 4) for k, v in top}


# Singleton
_journal = None


def get_journal() -> TradeJournal:
    """Get or create the global trade journal."""
    global _journal
    if _journal is None:
        _journal = TradeJournal()
    return _journal
