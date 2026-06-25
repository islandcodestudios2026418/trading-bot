"""
Centralized PnL persistence — saves/loads cumulative PnL for all strategies.
Writes to pnl_history.json on each save. Loaded on startup.
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

TW_TZ = timezone(timedelta(hours=8))
PNL_FILE = os.getenv("PNL_FILE", "pnl_history.json")

_store: dict = {
    "strategies": {},  # strategy_name → {total_pnl, total_fills, last_save}
    "daily": {},       # date → {pnl, fills}
}


def load():
    """Load PnL history from disk."""
    global _store
    try:
        with open(PNL_FILE) as f:
            _store = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass


def save():
    """Persist PnL to disk."""
    _store["last_save"] = datetime.now(TW_TZ).isoformat()
    try:
        with open(PNL_FILE, "w") as f:
            json.dump(_store, f, indent=2)
    except Exception:
        pass


def record(strategy: str, pnl: float):
    """Record a trade PnL for a strategy."""
    if strategy not in _store["strategies"]:
        _store["strategies"][strategy] = {"total_pnl": 0.0, "total_fills": 0, "last_save": ""}
    s = _store["strategies"][strategy]
    s["total_pnl"] += pnl
    s["total_fills"] += 1

    # Daily tracking
    today = datetime.now(TW_TZ).strftime("%Y-%m-%d")
    if today not in _store["daily"]:
        _store["daily"][today] = {"pnl": 0.0, "fills": 0}
    _store["daily"][today]["pnl"] += pnl
    _store["daily"][today]["fills"] += 1

    # Auto-save every 50 fills
    total_fills = sum(st["total_fills"] for st in _store["strategies"].values())
    if total_fills % 50 == 0:
        save()


def get_summary() -> dict:
    """Get current PnL summary for all strategies."""
    today = datetime.now(TW_TZ).strftime("%Y-%m-%d")
    return {
        "strategies": _store.get("strategies", {}),
        "today": _store.get("daily", {}).get(today, {"pnl": 0.0, "fills": 0}),
        "all_time_pnl": sum(s["total_pnl"] for s in _store.get("strategies", {}).values()),
        "all_time_fills": sum(s["total_fills"] for s in _store.get("strategies", {}).values()),
    }


# Auto-load on import
load()
