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


def _get_session_info() -> dict:
    """Get current trading session info."""
    try:
        from binance_paper import _get_session, _current_session, SESSION_PARAMS
        session = _current_session
        params = SESSION_PARAMS.get(session, {})
        return {"name": session, "threshold_mult": params.get("threshold_mult", 1.0),
                "size_mult": params.get("size_mult", 1.0), "mom_req": params.get("mom_req", 3)}
    except (ImportError, Exception):
        return {"name": "unknown"}


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
            "regime": ps.regime.regime,
            "variance_ratio": round(ps.regime.vr, 2),
            "institutional_flow": round(ps.ofi_tracker.institutional_flow, 2),
            "toxicity": round(ps.ofi_tracker.toxicity, 2),
            "depth_pressure": round(ps.ofi_tracker.depth_pressure, 3),
            "spoof_score": round(ps.ofi_tracker.spoof_score, 2),
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
        "session": _get_session_info(),
        "pairs": pairs,
        "funding_arb": funding_arb,
        "cross_arb": cross,
        "uptime_min": round((time.time() - _start_time) / 60),
    }


DASHBOARD_HTML = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>Trading Bot v7.2</title>
<meta http-equiv="refresh" content="30">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>body{font-family:monospace;background:#1a1a2e;color:#e0e0e0;margin:20px}
h2{color:#10b981}h3{color:#3b82f6;margin-top:20px}table{border-collapse:collapse;margin:10px 0}
td,th{padding:4px 12px;border:1px solid #333;text-align:right}
th{background:#2d2d44}.pos{color:#10b981}.neg{color:#ef4444}.dim{color:#888}
.trend{color:#f59e0b}.range{color:#8b5cf6}.neutral{color:#6b7280}
canvas{max-width:900px;max-height:350px;margin:20px 0}
.badge{padding:2px 8px;border-radius:4px;font-size:0.85em}
.badge-asian{background:#1e3a5f;color:#60a5fa}.badge-europe{background:#1e3a1e;color:#86efac}
.badge-us{background:#3f1e1e;color:#fca5a5}</style></head><body>
<h2>Trading Bot v7.2 — Multi-Strategy + Regime Detection</h2>
<div id="stats"></div>
<canvas id="c"></canvas>
<script>
fetch('/stats').then(r=>r.json()).then(s=>{
  let sess = s.session||{};
  let sc = sess.name==='asian'?'badge-asian':sess.name==='us'?'badge-us':'badge-europe';
  let h='<p>Equity: $'+s.equity+' | Daily PnL: $'+s.daily_pnl+' | Fills: '+s.daily_fills+' | WR: '+s.daily_win_rate+' | Up: '+s.uptime_min+'m';
  h+=' | Session: <span class="badge '+sc+'">'+sess.name+'</span> (thresh:'+((sess.threshold_mult||1)*100).toFixed(0)+'% size:'+(sess.size_mult||1).toFixed(1)+'x mom:'+sess.mom_req+')</p>';
  h+='<h3>Binance MM (Multi-TF OFI + Regime + Session)</h3>';
  h+='<table><tr><th>Pair</th><th>Pos</th><th>PnL</th><th>Fills</th><th>WR</th><th>OFI</th><th>Regime</th><th>VR</th><th>Inst.Flow</th><th>Tox</th><th>Spread</th><th>Status</th></tr>';
  for(let[k,v] of Object.entries(s.pairs||{})){
    let pc=v.pnl>=0?'pos':'neg';
    let rc=v.regime==='trending'?'trend':v.regime==='ranging'?'range':'neutral';
    h+='<tr><td>'+k+'</td><td class="'+(v.position>=0?'pos':'neg')+'">$'+v.position+'</td>';
    h+='<td class="'+pc+'">$'+v.pnl.toFixed(4)+'</td><td>'+v.fills+'</td><td>'+v.win_rate+'</td>';
    h+='<td>'+v.ofi+'</td>';
    h+='<td class="'+rc+'">'+v.regime+' </td><td class="dim">'+v.variance_ratio+'</td>';
    h+='<td>'+(v.institutional_flow>0.5?'🏦':v.institutional_flow>0.2?'📊':'')+(v.institutional_flow||0).toFixed(1)+'</td>';
    h+='<td class="'+(v.toxicity>0.2?'pos':v.toxicity<-0.2?'neg':'dim')+'">'+(v.toxicity||0).toFixed(2)+'</td>';
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


def _get_analytics() -> dict:
    """Performance analytics: Sharpe, max drawdown, win rate per regime."""
    import math

    # Equity curve metrics
    equities = [e["equity"] for e in equity_history]
    returns = []
    for i in range(1, len(equities)):
        if equities[i - 1] > 0:
            returns.append((equities[i] - equities[i - 1]) / equities[i - 1])

    # Sharpe (annualized, assume ~8640 ticks/day at 10s intervals)
    sharpe = 0.0
    if returns:
        mean_r = sum(returns) / len(returns)
        var_r = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        std_r = math.sqrt(var_r) if var_r > 0 else 1e-10
        sharpe = (mean_r / std_r) * math.sqrt(8640)

    # Max drawdown
    peak = equities[0] if equities else STARTING_CAPITAL
    max_dd = 0.0
    for eq in equities:
        peak = max(peak, eq)
        dd = (peak - eq) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

    # Per-regime stats from Binance MM
    regime_stats = {}
    try:
        from binance_paper import pair_states
        from regime import TRENDING, RANGING, NEUTRAL
        agg = {TRENDING: {"fills": 0, "pnl": 0.0}, RANGING: {"fills": 0, "pnl": 0.0}, NEUTRAL: {"fills": 0, "pnl": 0.0}}
        for ps in pair_states.values():
            for r in [TRENDING, RANGING, NEUTRAL]:
                agg[r]["fills"] += ps.regime.regime_fills.get(r, 0)
                agg[r]["pnl"] += ps.regime.regime_pnl.get(r, 0.0)
        for r, v in agg.items():
            regime_stats[r] = {"fills": v["fills"], "pnl": round(v["pnl"], 4),
                               "avg": round(v["pnl"] / v["fills"], 6) if v["fills"] > 0 else 0}
    except (ImportError, Exception):
        pass

    # Execution quality
    exec_quality = {}
    try:
        from binance_paper import _exec_quality
        for sym, eq in _exec_quality.items():
            avg_slip = eq["slippage_sum"] / eq["fill_count"] if eq["fill_count"] > 0 else 0
            exec_quality[sym] = {
                "avg_slippage_bps": round(avg_slip, 2),
                "fills": eq["fill_count"],
                "adverse_selections": eq["adverse_count"],
            }
    except (ImportError, Exception):
        pass

    return {
        "sharpe": round(sharpe, 2),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "total_returns": len(returns),
        "equity_current": equities[-1] if equities else STARTING_CAPITAL,
        "regime": regime_stats,
        "execution_quality": exec_quality,
        "uptime_h": round((time.time() - _start_time) / 3600, 1),
    }


def _get_metrics() -> dict:
    """Real-time signal metrics: VPIN, OBI, arrival intensity, regime, Hurst per symbol."""
    metrics = {}
    try:
        from binance_paper import pair_states
        for sym, ps in pair_states.items():
            ot = ps.ofi_tracker
            metrics[sym] = {
                "ofi": round(ps.last_ofi, 4),
                "obi": round(ot.obi, 4),
                "vpin": round(ot.vpin, 4),
                "toxicity": round(ot.toxicity, 4),
                "arrival_intensity": round(ot.arrival_intensity, 2),
                "volume_surge": round(ot.volume_surge, 2),
                "institutional_flow": round(ot.institutional_flow, 3),
                "depth_pressure": round(ot.depth_pressure, 4),
                "spoof_score": round(ot.spoof_score, 3),
                "regime": ps.regime.regime,
                "vr": round(ps.regime.vr, 3),
                "hurst": round(ps.regime.hurst, 3),
                "spread_bps": round(ps.last_spread_bps, 1),
                "atr": round(ps.last_atr, 6),
                "position": round(ps.position, 2),
            }
    except (ImportError, Exception):
        pass
    # Signal attribution report
    try:
        from signal_attrib import attrib
        metrics["_attribution"] = attrib.get_report()
        metrics["_disabled_signals"] = list(attrib.disabled_signals)
    except (ImportError, Exception):
        pass
    return metrics


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/data":
            self._json(equity_history[-500:])
        elif self.path == "/stats":
            self._json(_get_stats())
        elif self.path == "/analytics":
            self._json(_get_analytics())
        elif self.path == "/metrics":
            self._json(_get_metrics())
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
