"""
Polymarket 24/7 Arbitrage Monitor — WebSocket based.
Monitors all active binary markets for Yes_ask + No_ask < 1.0 opportunities.
Designed to run on Zeabur (or any always-on server).

Modes:
- ALERT: prints/webhooks when arb found (default)
- EXECUTE: auto-trades when arb found (needs wallet)
"""
import asyncio
import json
import os
import time
import traceback
from datetime import datetime, timezone

import requests
import websockets

GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB = "https://clob.polymarket.com/book"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # Discord/Telegram webhook
MIN_PROFIT_PCT = float(os.getenv("MIN_PROFIT_PCT", "0.3"))  # minimum 0.3% profit to alert


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def send_alert(msg):
    log(f"🚨 {msg}")
    if WEBHOOK_URL:
        try:
            requests.post(WEBHOOK_URL, json={"content": msg}, timeout=5, verify=False)
        except Exception:
            pass


def get_market_tokens() -> dict[str, dict]:
    """Fetch all active binary markets and their token pairs."""
    token_pairs = {}  # token_id -> {pair_token, question}
    for offset in range(0, 1000, 200):
        r = requests.get(GAMMA, params={
            "closed": "false", "active": "true", "limit": "200", "offset": str(offset)
        }, verify=False, timeout=10)
        markets = r.json()
        if not markets:
            break
        for m in markets:
            tokens_raw = m.get("clobTokenIds", "")
            if not tokens_raw:
                continue
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
            if len(tokens) == 2:
                token_pairs[tokens[0]] = {"pair": tokens[1], "q": m["question"][:60]}
                token_pairs[tokens[1]] = {"pair": tokens[0], "q": m["question"][:60]}
    return token_pairs


async def monitor():
    """Main monitoring loop using WebSocket."""
    log("Starting Polymarket Arb Monitor...")

    while True:
        try:
            # Refresh market list every cycle
            log("Fetching active markets...")
            token_pairs = get_market_tokens()
            all_tokens = list(set(token_pairs.keys()))
            log(f"Monitoring {len(all_tokens)//2} binary markets ({len(all_tokens)} tokens)")

            # Track best asks per token
            best_asks: dict[str, float] = {}

            # Subscribe in batches (WS might have limits)
            batch_size = 100
            for i in range(0, len(all_tokens), batch_size):
                batch = all_tokens[i:i+batch_size]

                async with websockets.connect(WS_URL) as ws:
                    await ws.send(json.dumps({
                        "assets_ids": batch,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }))

                    async def heartbeat():
                        while True:
                            await ws.send("PING")
                            await asyncio.sleep(10)

                    hb = asyncio.create_task(heartbeat())
                    deadline = time.time() + 300  # refresh markets every 5 min

                    try:
                        async for raw in ws:
                            if time.time() > deadline:
                                break

                            if raw == "PONG":
                                continue

                            msg = json.loads(raw)
                            evt = msg.get("event_type")

                            if evt == "book":
                                tid = msg.get("asset_id")
                                asks = msg.get("asks", [])
                                if asks and tid in token_pairs:
                                    best_asks[tid] = float(asks[0]["price"])
                                    check_arb(tid, best_asks, token_pairs)

                            elif evt == "price_change":
                                for pc in msg.get("price_changes", []):
                                    tid = pc.get("asset_id")
                                    if tid in token_pairs and pc.get("best_ask"):
                                        best_asks[tid] = float(pc["best_ask"])
                                        check_arb(tid, best_asks, token_pairs)
                    finally:
                        hb.cancel()

        except Exception as e:
            log(f"Error: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)


def check_arb(tid: str, best_asks: dict, token_pairs: dict):
    """Check if Yes+No asks sum to less than 1.0."""
    info = token_pairs.get(tid)
    if not info:
        return
    pair_tid = info["pair"]
    if tid not in best_asks or pair_tid not in best_asks:
        return

    ask_a = best_asks[tid]
    ask_b = best_asks[pair_tid]
    total = ask_a + ask_b

    if total < 1.0:
        profit_pct = (1.0 - total) / total * 100
        if profit_pct >= MIN_PROFIT_PCT:
            send_alert(
                f"ARB FOUND: {info['q']}\n"
                f"  Yes={ask_a:.4f} + No={ask_b:.4f} = {total:.4f}\n"
                f"  Profit: {profit_pct:.2f}% (${(1.0-total)*100:.2f} per $100)"
            )


if __name__ == "__main__":
    asyncio.run(monitor())
