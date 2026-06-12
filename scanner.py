"""
Market Scanner — finds Polymarket markets with wide spreads suitable for market making.

Criteria:
- Active market (not resolved)
- Spread > 3% (profitable after our 3% spread)
- Some volume (not dead)
"""
from py_clob_client_v2 import ClobClient
import config
from client import get_client


def scan_markets(client: ClobClient, min_spread_pct: float = 3.0, limit: int = 50):
    """Scan for markets with wide spreads."""
    # Get active markets
    markets = client.get_markets(next_cursor="MA==")  # first page
    opportunities = []

    for market in markets:
        if not market.get("active") or market.get("closed"):
            continue

        for token in market.get("tokens", []):
            token_id = token.get("token_id")
            if not token_id:
                continue

            try:
                book = client.get_order_book(token_id)
                if not book or not book.bids or not book.asks:
                    continue

                best_bid = float(book.bids[0].price)
                best_ask = float(book.asks[0].price)
                spread_pct = ((best_ask - best_bid) / best_bid) * 100 if best_bid > 0 else 0
                mid = (best_bid + best_ask) / 2

                if spread_pct >= min_spread_pct and 0.1 < mid < 0.9:
                    opportunities.append({
                        "question": market.get("question", "")[:60],
                        "token_id": token_id,
                        "outcome": token.get("outcome"),
                        "bid": best_bid,
                        "ask": best_ask,
                        "spread_pct": spread_pct,
                        "mid": mid,
                    })
            except Exception:
                continue

        if len(opportunities) >= limit:
            break

    opportunities.sort(key=lambda x: x["spread_pct"], reverse=True)
    return opportunities


def main():
    client = get_client()
    print("Scanning for market making opportunities...\n")
    opps = scan_markets(client)

    if not opps:
        print("No opportunities found. Try lowering min_spread_pct.")
        return

    print(f"{'Market':<62} {'Outcome':<5} {'Bid':>5} {'Ask':>5} {'Spread%':>7} {'Token ID'}")
    print("-" * 120)
    for o in opps[:20]:
        print(f"{o['question']:<62} {o['outcome']:<5} {o['bid']:>5.2f} {o['ask']:>5.2f} {o['spread_pct']:>6.1f}% {o['token_id']}")


if __name__ == "__main__":
    main()
