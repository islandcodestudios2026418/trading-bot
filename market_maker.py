"""
Polymarket Market Maker Bot

Strategy: Two-sided quoting on selected markets.
- Posts bid at mid - spread, ask at mid + spread
- Cancels and refreshes quotes every REFRESH_INTERVAL seconds
- Inventory-aware: skews quotes when position builds up
"""
import time
import traceback

from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side

import config
from client import get_client


def get_midpoint(client, token_id: str) -> float | None:
    """Get midpoint from orderbook."""
    book = client.get_order_book(token_id)
    if not book or not book.bids or not book.asks:
        return None
    best_bid = float(book.bids[0].price)
    best_ask = float(book.asks[0].price)
    return (best_bid + best_ask) / 2


def compute_quotes(mid: float, spread_bps: int, position: float, max_pos: float):
    """Compute bid/ask prices with inventory skew."""
    half_spread = mid * (spread_bps / 10000)
    # Skew: if long, lower bid more / raise ask less to reduce position
    skew = (position / max_pos) * half_spread if max_pos > 0 else 0
    bid = round(mid - half_spread - skew, 2)
    ask = round(mid + half_spread - skew, 2)
    # Clamp to [0.01, 0.99]
    bid = max(0.01, min(0.99, bid))
    ask = max(0.01, min(0.99, ask))
    return bid, ask


def cancel_all(client):
    """Cancel all open orders."""
    try:
        client.cancel_all()
        print("  Cancelled all orders")
    except Exception as e:
        print(f"  Cancel error: {e}")


def post_quote(client, token_id: str, price: float, side: Side, size: float):
    """Post a limit order."""
    resp = client.create_and_post_order(
        order_args=OrderArgs(
            token_id=token_id,
            price=price,
            side=side,
            size=size,
        ),
        options=PartialCreateOrderOptions(tick_size="0.01"),
        order_type=OrderType.GTC,
    )
    return resp


def run(token_ids: list[str]):
    """Main market making loop."""
    client = get_client()
    print(f"Bot started. Markets: {len(token_ids)}, spread: {config.SPREAD_BPS}bps, size: ${config.ORDER_SIZE_USDC}")

    positions = {tid: 0.0 for tid in token_ids}  # track net position per token
    entry_prices = {tid: 0.0 for tid in token_ids}  # avg entry price

    while True:
        try:
            cancel_all(client)
            time.sleep(1)

            for token_id in token_ids:
                mid = get_midpoint(client, token_id)
                if mid is None:
                    print(f"  [{token_id[:8]}] No orderbook, skipping")
                    continue

                # Stop-loss check: if unrealized loss > MAX_LOSS_USDC, close position
                pos = positions[token_id]
                if pos != 0 and entry_prices[token_id] > 0:
                    unrealized_pnl = pos * (mid - entry_prices[token_id])
                    if unrealized_pnl < -config.MAX_LOSS_USDC:
                        print(f"  [{token_id[:8]}] STOP LOSS triggered. PnL=${unrealized_pnl:.2f}, closing.")
                        side = Side.SELL if pos > 0 else Side.BUY
                        post_quote(client, token_id, mid, side, abs(pos))
                        continue

                bid, ask = compute_quotes(mid, config.SPREAD_BPS, positions[token_id], config.MAX_POSITION)
                size = config.ORDER_SIZE_USDC

                # Skip if position too large
                if abs(positions[token_id]) >= config.MAX_POSITION:
                    print(f"  [{token_id[:8]}] Max position reached, only reducing side")
                    if positions[token_id] > 0:
                        post_quote(client, token_id, ask, Side.SELL, size)
                    else:
                        post_quote(client, token_id, bid, Side.BUY, size)
                    continue

                post_quote(client, token_id, bid, Side.BUY, size)
                post_quote(client, token_id, ask, Side.SELL, size)
                print(f"  [{token_id[:8]}] mid={mid:.3f} bid={bid:.2f} ask={ask:.2f}")

            print(f"Sleeping {config.REFRESH_INTERVAL}s...")
            time.sleep(config.REFRESH_INTERVAL)

        except KeyboardInterrupt:
            print("\nShutting down...")
            cancel_all(client)
            break
        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python market_maker.py <token_id> [token_id2 ...]")
        print("  Use 'python scanner.py' to find good markets first")
        sys.exit(1)
    run(sys.argv[1:])
