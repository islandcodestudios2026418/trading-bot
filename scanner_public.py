"""
Public market scanner — NO wallet needed. Uses Polymarket's public REST API.
Finds markets with wide spreads for paper trading.
"""
import requests

CLOB_HOST = "https://clob.polymarket.com"


def get_markets(limit=50):
    """Fetch active markets from public API."""
    resp = requests.get(f"{CLOB_HOST}/markets", params={"next_cursor": "MA=="})
    resp.raise_for_status()
    return resp.json()


def get_orderbook(token_id: str):
    """Fetch orderbook for a token."""
    resp = requests.get(f"{CLOB_HOST}/book", params={"token_id": token_id})
    resp.raise_for_status()
    return resp.json()


def scan():
    print("Scanning Polymarket for wide-spread markets (no auth needed)...\n")
    markets = get_markets()
    opps = []

    for market in markets:
        if not market.get("active") or market.get("closed"):
            continue
        for token in market.get("tokens", []):
            token_id = token.get("token_id")
            if not token_id:
                continue
            try:
                book = get_orderbook(token_id)
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                if not bids or not asks:
                    continue
                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                if best_bid <= 0:
                    continue
                spread_pct = ((best_ask - best_bid) / best_bid) * 100
                mid = (best_bid + best_ask) / 2
                if spread_pct >= 3.0 and 0.1 < mid < 0.9:
                    opps.append({
                        "question": market.get("question", "")[:55],
                        "outcome": token.get("outcome", ""),
                        "bid": best_bid,
                        "ask": best_ask,
                        "spread": spread_pct,
                        "token_id": token_id,
                    })
            except Exception:
                continue

    opps.sort(key=lambda x: x["spread"], reverse=True)
    print(f"{'Market':<57} {'Out':<4} {'Bid':>5} {'Ask':>5} {'Spr%':>5}  Token ID")
    print("-" * 130)
    for o in opps[:25]:
        print(f"{o['question']:<57} {o['outcome']:<4} {o['bid']:>5.2f} {o['ask']:>5.2f} {o['spread']:>4.1f}%  {o['token_id']}")


if __name__ == "__main__":
    scan()
