"""
Polymarket 24/7 Arbitrage Monitor + Dashboard.
/data — equity curve (backward-compatible)
/stats — per-pair JSON stats for external monitoring
/ — dashboard HTML
"""
import asyncio
import json
import os
import time
import traceback
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

import requests
import websockets

TW_TZ = timezone(timedelta(hours=8))

STARTING_CAPITAL = float(os.getenv("CAPITAL", "2000"))
_start_time = time.time()
equity_history: list[dict] = [{"ts": datetime.now(TW_TZ).isoformat(), "equity": STARTING_CAPITAL, "poly": 0, "binance": 0}]
_poly_pnl = 0.0
_binance_pnl = 0.0

GAMMA = "https://gamma-api.polymarket.com/markets"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
MIN_PROFIT_PCT = float(os.getenv("MIN_PROFIT_PCT", "0.3"))
HTTPS_PROXY = os.getenv("HTTPS_PROXY", "")

# Proxy config for requests
_req_kwargs: dict = {"verify": False, "timeout": 10}
if HTTPS_PROXY:
    _req_kwargs["proxies"] = {"https": HTTPS_PROXY, "http": HTTPS_PROXY}


def log(msg):
    ts = datetime.now(TW_TZ).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def record_trade(profit: float, source: str = "binance"):
    global _poly_pnl, _binance_pnl
    if source == "poly":
        _poly_pnl += profit
    else:
        _binance_pnl += profit
    equity_history.append({
        "ts": datetime.now(TW_TZ).isoformat(),
        "equity": round(STARTING_CAPITAL + _poly_pnl + _binance_pnl, 4),
        "poly": round(_poly_pnl, 4),
        "binance": round(_binance_pnl, 4),
    })


def _get_stats() -> dict:
    """Build stats JSON from all subsystems."""
    try:
        from binance_paper import pair_states, daily_pnl, daily_fills, daily_wins
    except ImportError:
        return {"error": "binance_paper not loaded"}

    pairs = {}
    for sym, ps in pair_states.items():
        pairs[sym] = {
            "position": round(ps.position, 2),
            "entry_price": ps.entry_price,
            "pnl": round(ps.pnl, 4),
            "fills": ps.fills,
            "win_rate": ps.wr,
            "ofi": round(ps.last_ofi, 3),
            "ofi_1s": round(ps.ofi_tracker.ofi_1s, 3),
            "ofi_5s": round(ps.ofi_tracker.ofi_5s, 3),
            "ofi_30s": round(ps.ofi_tracker.ofi_30s, 3),
            "ofi_weights": [round(ps.ofi_tracker.w1, 2), round(ps.ofi_tracker.w5, 2), round(ps.ofi_tracker.w30, 2)],
            "spread_bps": round(ps.last_spread_bps, 1),
            "atr": round(ps.last_atr, 6),
            "vwap_dev": round((ps.mid_prices[-1] - ps.vwap) / ps.vwap * 10000, 1) if ps.vwap and ps.mid_prices else 0,
            "paused": time.time() < ps.paused_until,
        }

    # Funding arb positions
    funding_arb = {}
    try:
        from funding_monitor import _arb_positions
        for inst, pos in _arb_positions.items():
            funding_arb[inst] = {
                "perp_side": pos["perp_side"],
                "size": pos["size"],
                "entry_rate": pos["entry_rate"],
                "hours_held": round((time.time() - pos["opened_at"]) / 3600, 1),
            }
    except (ImportError, Exception):
        pass

    # Cross-arb stats
    cross = {}
    try:
        from cross_arb import _pnl as cross_pnl, _trades as cross_trades, _binance_mids, _okx_mids
        cross = {"pnl": round(cross_pnl, 4), "trades": cross_trades}
        for sym in _binance_mids:
            b, o = _binance_mids.get(sym, 0), _okx_mids.get(sym, 0)
            if b and o:
                cross[sym] = {"div_bps": round((b - o) / o * 10000, 1)}
    except (ImportError, Exception):
        pass

    return {
        "equity": round(STARTING_CAPITAL + _poly_pnl + _binance_pnl, 2),
        "daily_pnl": round(daily_pnl, 4),
        "daily_fills": daily_fills,
        "daily_win_rate": f"{daily_wins/daily_fills*100:.0f}%" if daily_fills else "-",
        "pairs": pairs,
        "funding_arb": funding_arb,
        "cross_arb": cross,
        "uptime_min": round((time.time() - _start_time) / 60),
    }


DASHBOARD_HTML = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>Trading Bot v6.1</title>
<meta http-equiv="refresh" content="30">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>body{font-family:monospace;background:#1a1a2e;color:#e0e0e0;margin:20px}
h2{color:#10b981}h3{color:#3b82f6;margin-top:20px}table{border-collapse:collapse;margin:10px 0}
td,th{padding:4px 12px;border:1px solid #333;text-align:right}
th{background:#2d2d44}.pos{color:#10b981}.neg{color:#ef4444}.dim{color:#888}
canvas{max-width:900px;max-height:350px;margin:20px 0}</style></head><body>
<h2>Trading Bot v6.1 — Multi-Strategy</h2>
<div id="stats"></div>
<canvas id="c"></canvas>
<script>
fetch('/stats').then(r=>r.json()).then(s=>{
  let h='<p>Equity: $'+s.equity+' | Daily PnL: $'+s.daily_pnl+' | Fills: '+s.daily_fills+' | WR: '+s.daily_win_rate+' | Up: '+s.uptime_min+'m</p>';
  h+='<h3>Binance MM (Multi-TF OFI + Mean Reversion)</h3>';
  h+='<table><tr><th>Pair</th><th>Pos</th><th>PnL</th><th>Fills</th><th>WR</th><th>OFI</th><th>1s/5s/30s</th><th>Wts</th><th>VWAP</th><th>Spread</th><th>Status</th></tr>';
  for(let[k,v] of Object.entries(s.pairs||{})){
    let pc=v.pnl>=0?'pos':'neg';
    h+='<tr><td>'+k+'</td><td class="'+(v.position>=0?'pos':'neg')+'">$'+v.position+'</td>';
    h+='<td class="'+pc+'">$'+v.pnl.toFixed(4)+'</td><td>'+v.fills+'</td><td>'+v.win_rate+'</td>';
    h+='<td>'+v.ofi+'</td><td class="dim">'+v.ofi_1s+'/'+v.ofi_5s+'/'+v.ofi_30s+'</td>';
    h+='<td class="dim">'+(v.ofi_weights||[]).join('/')+'</td>';
    h+='<td>'+(v.vwap_dev||0)+'bp</td>';
    h+='<td>'+v.spread_bps.toFixed(1)+'bp</td>';
    h+='<td>'+(v.paused?'⏸':'✅')+'</td></tr>';}
  h+='</table>';
  if(s.cross_arb&&s.cross_arb.trades>0){
    h+='<h3>Cross-Exchange Arb (Binance→OKX)</h3>';
    h+='<p>Trades: '+s.cross_arb.trades+' | PnL: $'+s.cross_arb.pnl+'</p>';
    for(let[k,v] of Object.entries(s.cross_arb)){if(typeof v==='object'&&v.div_bps!==undefined)h+='<span class="dim">'+k+': '+v.div_bps+'bps </span>';}
  }
  if(Object.keys(s.funding_arb||{}).length>0){
    h+='<h3>Funding Rate Arb</h3><table><tr><th>Instrument</th><th>Side</th><th>Entry Rate</th><th>Hours Held</th></tr>';
    for(let[k,v] of Object.entries(s.funding_arb))
      h+='<tr><td>'+k+'</td><td>'+v.perp_side+'</td><td>'+v.entry_rate.toFixed(4)+'%</td><td>'+v.hours_held+'h</td></tr>';
    h+='</table>';
  }
  document.getElementById('stats').innerHTML=h;});
fetch('/data').then(r=>r.json()).then(d=>{
  new Chart(document.getElementById('c'),{type:'line',data:{
    labels:d.map(p=>p.ts.slice(11,19)),datasets:[
    {label:'Total ($)',data:d.map(p=>p.equity),borderColor:'#10b981',fill:false,tension:0.3},
    {label:'Binance MM',data:d.map(p=>p.binance),borderColor:'#3b82f6',fill:false,tension:0.3},
    {label:'Polymarket',data:d.map(p=>p.poly),borderColor:'#f59e0b',fill:false,tension:0.3}
  ]},options:{scales:{y:{beginAtZero:false}},plugins:{legend:{labels:{color:'#ccc'}}}}})});
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/data":
            self._json(equity_history[-500:])
        elif self.path == "/stats":
            self._json(_get_stats())
        elif self.path == "/health":
            uptime_s = time.time() - _start_time
            self._json({"status": "ok", "uptime_s": int(uptime_s), "uptime_h": round(uptime_s / 3600, 1)})
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode())

    def _json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, *args):
        pass


def start_web():
    port = int(os.getenv("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()


def send_alert(msg):
    log(f"🚨 {msg}")
    if WEBHOOK_URL:
        try:
            requests.post(WEBHOOK_URL, json={"content": msg}, timeout=5, verify=False)
        except Exception:
            pass


def get_market_tokens() -> dict[str, dict]:
    token_pairs = {}
    for offset in range(0, 1000, 200):
        try:
            r = requests.get(GAMMA, params={
                "closed": "false", "active": "true", "limit": "200", "offset": str(offset)
            }, **_req_kwargs)
            markets = r.json()
        except (requests.exceptions.JSONDecodeError, ValueError):
            log("Polymarket API returned non-JSON (likely geo-blocked)")
            return token_pairs
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
    log("Starting Polymarket Arb Monitor...")
    while True:
        try:
            log("Fetching active markets...")
            token_pairs = get_market_tokens()
            all_tokens = list(set(token_pairs.keys()))
            log(f"Monitoring {len(all_tokens)//2} binary markets")

            if not all_tokens:
                await asyncio.sleep(60)
                continue

            best_asks: dict[str, float] = {}
            batch_size = 100
            for i in range(0, len(all_tokens), batch_size):
                batch = all_tokens[i:i+batch_size]
                async with websockets.connect(WS_URL) as ws:
                    await ws.send(json.dumps({
                        "assets_ids": batch, "type": "market",
                    }))

                    async def heartbeat():
                        while True:
                            await ws.send("PING")
                            await asyncio.sleep(10)

                    hb = asyncio.create_task(heartbeat())
                    deadline = time.time() + 300

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
                                    _check_arb(tid, best_asks, token_pairs)
                            elif evt == "price_change":
                                for pc in msg.get("price_changes", []):
                                    tid = pc.get("asset_id")
                                    if tid in token_pairs and pc.get("best_ask"):
                                        best_asks[tid] = float(pc["best_ask"])
                                        _check_arb(tid, best_asks, token_pairs)
                    finally:
                        hb.cancel()
        except Exception as e:
            log(f"Polymarket error: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)


def _check_arb(tid: str, best_asks: dict, token_pairs: dict):
    info = token_pairs.get(tid)
    if not info:
        return
    pair_tid = info["pair"]
    if tid not in best_asks or pair_tid not in best_asks:
        return
    total = best_asks[tid] + best_asks[pair_tid]
    if total < 1.0:
        profit_pct = (1.0 - total) / total * 100
        if profit_pct >= MIN_PROFIT_PCT:
            trade_size = min(100, STARTING_CAPITAL * 0.05)
            profit_usd = (1.0 - total) * trade_size

            # Attempt real execution
            try:
                from polymarket_exec import execute_arb, is_ready
                if is_ready():
                    result = execute_arb(tid, pair_tid, best_asks[tid], best_asks[pair_tid], info['q'])
                    if result and "error" not in result:
                        log(f"🎯 ARB EXECUTED: {info['q']} | {total:.4f} | +{profit_pct:.1f}%")
                        record_trade(profit_usd, source="poly")
                    elif result:
                        log(f"⚠️ ARB exec error: {result.get('error')}")
                    return
            except ImportError:
                pass

            # Paper trade fallback
            record_trade(profit_usd, source="poly")
            send_alert(f"ARB: {info['q']} | {total:.4f} | +${profit_usd:.2f}")
            try:
                from telegram_alerts import alert_arb
                alert_arb(info['q'], total, profit_usd)
            except ImportError:
                pass


if __name__ == "__main__":
    asyncio.run(monitor())
