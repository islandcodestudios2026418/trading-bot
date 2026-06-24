"""Polymarket CLOB execution — places arb trades when Yes+No < $1.00.
Requires: py-clob-client-v2, POLY_PRIVATE_KEY env var, funded pUSD wallet.
Disabled gracefully if SDK not installed or key not set.
"""
import os

POLY_KEY = os.getenv("POLY_PRIVATE_KEY", "")
POLY_ENABLED = os.getenv("POLY_EXEC_ENABLED", "0") == "1"
POLY_MAX_SIZE = float(os.getenv("POLY_MAX_SIZE", "20"))  # max USD per arb leg

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
    Returns trade result or None if execution disabled/failed.
    """
    if not POLY_ENABLED or not POLY_KEY:
        return None

    client = _get_client()
    if not client:
        return None

    total = yes_ask + no_ask
    if total >= 1.0:
        return None

    profit_pct = (1.0 - total) / total * 100
    size = min(POLY_MAX_SIZE, POLY_MAX_SIZE / total)  # shares to buy

    try:
        from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
        from py_clob_client.order_builder.constants import BUY

        opts = PartialCreateOrderOptions(neg_risk=neg_risk)
        # Buy YES at ask
        r1 = client.create_and_post_order(
            OrderArgs(token_id=yes_token, price=yes_ask, size=size, side=BUY),
            opts, OrderType.FOK,
        )
        # Buy NO at ask
        r2 = client.create_and_post_order(
            OrderArgs(token_id=no_token, price=no_ask, size=size, side=BUY),
            opts, OrderType.FOK,
        )
        return {
            "yes_order": r1, "no_order": r2,
            "profit_pct": profit_pct, "size": size,
            "question": question,
        }
    except Exception as e:
        return {"error": str(e)}


def is_ready() -> bool:
    """Check if Polymarket execution is configured and ready."""
    return POLY_ENABLED and bool(POLY_KEY) and _get_client() is not None
