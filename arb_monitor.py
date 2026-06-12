"""
Polymarket 24/7 Arbitrage Monitor — WebSocket based.
Monitors all active binary markets for Yes_ask + No_ask < 1.0 opportunities.
Designed to run on Zeabur (or any always-on server).

Includes a web dashboard at port 8080 showing equity curve.
"""
import asyncio
import json
import os
import time
import traceback
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

import requests
import websockets

# Equity tracking
STARTING_CAPITAL = float(os.getenv("CAPITAL", "500"))
equity_history: list[dict] = [{"ts": datetime.now(timezone.utc).isoformat(), "equity": STARTING_CAPITAL, "poly": 0, "binance": 0}]
_poly_pnl = 0.0
_binance_pnl = 0.0

GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB = "https://clob.polymarket.com/book"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # Discord/Telegram webhook
MIN_PROFIT_PCT = float(os.getenv("MIN_PROFIT_PCT", "0.3"))  # minimum 0.3% profit to alert


DASHBOARD_HTML = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>Trading Bot</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script></head><body>
<h2>Trading Bot - Equity Curve</h2>
<canvas id="c" style="max-width:900px;max-height:400px"></canvas>
<script>
fetch('/data').then(r=>r.json()).then(d=>{
new Chart(document.getElementById('c'),{type:'line',data:{
labels:d.map(p=>p.ts.slice(11,19)),datasets:[
{label:'Total ($)',data:d.map(p=>p.equity),borderColor:'#10b981',fill:false,tension:0.3},
{label:'Binance MM',data:d.map(p=>p.binance),borderColor:'#3b82f6',fill:false,tension:0.3},
{label:'Polymarket Arb',data:d.map(p=>p.poly),borderColor:'#f59e0b',fill:false,tension:0.3}
]},options:{scales:{y:{beginAtZero:false}}}})})
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/data":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(equity_history[-500:]).encode())
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode())

    def log_message(self, *args):
        pass  # suppress request logs


def start_web():
    port = int(os.getenv("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()


def record_trade(profit: float, source: str = "binance"):
    """Record a completed trade."""
    global _poly_pnl, _binance_pnl
    if source == "poly":
        _poly_pnl += profit
    else:
        _binance_pnl += profit
    equity_history.append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "equity": round(STARTING_CAPITAL + _poly_pnl + _binance_pnl, 4),
        "poly": round(_poly_pnl, 4),
        "binance": round(_binance_pnl, 4),
    })


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
            trade_size = min(100, STARTING_CAPITAL * 0.05)  # 5% of capital per trade
            profit_usd = (1.0 - total) * trade_size
            record_trade(profit_usd, source="poly")
            send_alert(
                f"ARB FOUND: {info['q']}\n"
                f"  Yes={ask_a:.4f} + No={ask_b:.4f} = {total:.4f}\n"
                f"  Profit: {profit_pct:.2f}% (${profit_usd:.2f})"
            )


if __name__ == "__main__":
    asyncio.run(monitor())
