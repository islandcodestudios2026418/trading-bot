"""Polymarket CLOB execution — places arb trades when Yes+No < $1.00.
Kelly-optimal sizing based on arb profit distribution.
Requires: py-clob-client-v2, POLY_PRIVATE_KEY env var, funded pUSD wallet.
Disabled gracefully if SDK not installed or key not set.
"""
import os
from collections import deque

POLY_KEY = os.getenv("POLY_PRIVATE_KEY", "")
POLY_ENABLED = os.getenv("POLY_EXEC_ENABLED", "0") == "1"
POLY_MAX_SIZE = float(os.getenv("POLY_MAX_SIZE", "20"))  # max USD per arb leg
POLY_KELLY_MULT = float(os.getenv("POLY_KELLY_MULT", "0.5"))  # half-Kelly for safety

_client = None
# Kelly sizing: track arb outcomes
_arb_profits: deque = deque(maxlen=100)  # recent profit_pct values
_arb_wins: int = 0
_arb_total: int = 0


def _kelly_size(profit_pct: float) -> float:
    """Kelly-optimal position size based on historical arb performance.
    f* = p - q/b where p=win_rate, q=1-p, b=win/loss ratio.
    Returns USD size clamped to [5, POLY_MAX_SIZE].
    """
    if _arb_total < 10:
        return POLY_MAX_SIZE * 0.5  # conservative until enough data

    p = _arb_wins / _arb_total  # historical win rate
    q = 1 - p
    # Average win: profit_pct, average loss: assume 1% (fees + failed fills)
    avg_win = sum(x for x in _arb_profits if x > 0) / max(1, sum(1 for x in _arb_profits if x > 0))
    avg_loss = 1.0  # conservative loss estimate
    b = avg_win / avg_loss if avg_loss > 0 else 1.0

    kelly = (p * b - q) / b if b > 0 else 0
    kelly = max(0, kelly * POLY_KELLY_MULT)  # half-Kelly

    # Scale by current opportunity size (bigger arb = more confidence)
    confidence = min(2.0, profit_pct / 1.0)  # 2x max at 2%+ arb
    size = POLY_MAX_SIZE * kelly * confidence
    return max(5.0, min(POLY_MAX_SIZE, size))

_client = None


def _get_client():
    global _client
    if _client:
        return _client
    if not POLY_KEY:
        return None
    try:
        from py_clob_client.client import ClobClient
        c = ClobClient("https://clob.polymarket.com", key=POLY_KEY, chain_id=137)
        c.set_api_creds(c.create_or_derive_api_creds())
        _client = c
        return c
    except Exception:
        return None


def execute_arb(yes_token: str, no_token: str, yes_ask: float, no_ask: float,
                question: str, neg_risk: bool = False) -> dict | None:
    """Buy both Yes and No tokens when their asks sum to < $1.00.
    Uses Kelly-optimal sizing based on arb profit distribution.
    Returns trade result or None if execution disabled/failed.
    """
    global _arb_wins, _arb_total
    if not POLY_ENABLED or not POLY_KEY:
        return None

    client = _get_client()
    if not client:
        return None

    total = yes_ask + no_ask
    if total >= 1.0:
        return None

    profit_pct = (1.0 - total) / total * 100

    # Kelly-optimal sizing: bigger bets on higher-confidence arbs
    kelly_usd = _kelly_size(profit_pct)
    size = kelly_usd / total  # shares to buy

    try:
        from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
        from py_clob_client.order_builder.constants import BUY

        opts = PartialCreateOrderOptions(neg_risk=neg_risk)
        r1 = client.create_and_post_order(
            OrderArgs(token_id=yes_token, price=yes_ask, size=size, side=BUY),
            opts, OrderType.FOK,
        )
        r2 = client.create_and_post_order(
            OrderArgs(token_id=no_token, price=no_ask, size=size, side=BUY),
            opts, OrderType.FOK,
        )
        # Track outcome for Kelly calculation
        _arb_total += 1
        if r1 and r2:  # both filled = profit
            _arb_wins += 1
            _arb_profits.append(profit_pct)
        else:
            _arb_profits.append(-1.0)  # partial fill = loss

        return {
            "yes_order": r1, "no_order": r2,
            "profit_pct": profit_pct, "size": size,
            "kelly_usd": kelly_usd,
            "question": question,
        }
    except Exception as e:
        _arb_total += 1
        _arb_profits.append(-1.0)
        return {"error": str(e)}


def is_ready() -> bool:
    """Check if Polymarket execution is configured and ready."""
    return POLY_ENABLED and bool(POLY_KEY) and _get_client() is not None
