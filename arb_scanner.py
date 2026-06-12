"""
Polymarket Arbitrage Scanner — finds markets where Yes_ask + No_ask < $1.00
This is the core HFT arb: buy both sides, guaranteed profit at resolution.
"""
import requests, json, time

GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB = "https://clob.polymarket.com/book"


def scan_arb():
    print("Scanning for Yes+No < $1.00 arbitrage opportunities...\n")
    opps = []

    for offset in range(0, 800, 200):
        r = requests.get(GAMMA, params={
            "closed": "false", "active": "true", "limit": "200", "offset": str(offset)
        }, verify=False)
        markets = r.json()
        if not markets:
            break

        for m in markets:
            tokens_raw = m.get("clobTokenIds", "")
            if not tokens_raw:
                continue
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
            if len(tokens) != 2:
                continue  # only binary markets

            try:
                # Get best ask for both Yes and No
                r1 = requests.get(CLOB, params={"token_id": tokens[0]}, verify=False, timeout=3)
                r2 = requests.get(CLOB, params={"token_id": tokens[1]}, verify=False, timeout=3)
                book1 = r1.json()
                book2 = r2.json()

                asks1 = book1.get("asks", [])
                asks2 = book2.get("asks", [])
                if not asks1 or not asks2:
                    continue

                yes_ask = float(asks1[0]["price"])
                no_ask = float(asks2[0]["price"])
                total = yes_ask + no_ask

                # Also check the reverse (bids sum > 1.0 means sell both sides)
                bids1 = book1.get("bids", [])
                bids2 = book2.get("bids", [])

                if total < 1.0:
                    profit_pct = (1.0 - total) / total * 100
                    size = min(float(asks1[0]["size"]), float(asks2[0]["size"]))
                    opps.append({
                        "type": "BUY BOTH",
                        "q": m["question"][:55],
                        "yes_ask": yes_ask,
                        "no_ask": no_ask,
                        "total": total,
                        "profit_pct": profit_pct,
                        "size": size,
                        "profit_usd": (1.0 - total) * min(size, 1000),
                        "tokens": tokens,
                    })

                if bids1 and bids2:
                    yes_bid = float(bids1[0]["price"])
                    no_bid = float(bids2[0]["price"])
                    bid_total = yes_bid + no_bid
                    if bid_total > 1.0:
                        profit_pct = (bid_total - 1.0) / 1.0 * 100
                        size = min(float(bids1[0]["size"]), float(bids2[0]["size"]))
                        opps.append({
                            "type": "SELL BOTH",
                            "q": m["question"][:55],
                            "yes_ask": yes_bid,
                            "no_ask": no_bid,
                            "total": bid_total,
                            "profit_pct": profit_pct,
                            "size": size,
                            "profit_usd": (bid_total - 1.0) * min(size, 1000),
                            "tokens": tokens,
                        })

            except Exception:
                continue

    opps.sort(key=lambda x: x["profit_pct"], reverse=True)

    if not opps:
        print("No arbitrage found. Market is efficient right now.")
        print("(This scanner should run continuously — arbs appear and disappear in seconds)")
        return

    print(f"Found {len(opps)} opportunities:\n")
    print(f"{'Type':<10} {'Yes':>5} {'No':>5} {'Sum':>5} {'Profit%':>7} {'$Max':>6}  Market")
    print("-" * 100)
    for o in opps[:20]:
        print(f"{o['type']:<10} {o['yes_ask']:>5.3f} {o['no_ask']:>5.3f} {o['total']:>5.3f} {o['profit_pct']:>6.2f}% ${o['profit_usd']:>5.1f}  {o['q']}")


if __name__ == "__main__":
    scan_arb()
