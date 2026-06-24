"""
OKX Spot Market Maker v2 — WebSocket-based, real-time requoting.
Uses public WS for orderbook streams + private WS for instant fill detection.
Replaces 15s REST polling with sub-second reaction to spread changes.
"""
import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timezone, timedelta

import websockets

from okx_client import (
    scan_spreads, place_order, cancel_all, cancel_order, batch_orders, batch_cancel,
    get_balance, _sign, _ts, API_KEY, SECRET, PASSPHRASE, SIMULATED
)
from risk_manager import RiskManager

TW_TZ = timezone(timedelta(hours=8))

# Config
SCAN_INTERVAL = int(os.getenv("OKX_SCAN_MIN", "10"))
MIN_SPREAD_BPS = float(os.getenv("OKX_MIN_SPREAD", "25"))
MAX_PAIRS = int(os.getenv("OKX_MAX_PAIRS", "3"))
EDGE_BPS = float(os.getenv("OKX_EDGE_BPS", "3"))
REQUOTE_BPS = float(os.getenv("OKX_REQUOTE_BPS", "5"))  # requote if mid moves > N bps

OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_PRIVATE = "wss://ws.okx.com:8443/ws/v5/private"

try:
    from arb_monitor import log, record_trade
except ImportError:
    def log(m): print(f"[{datetime.now(TW_TZ).strftime('%H:%M:%S')}] {m}", flush=True)
    def record_trade(p, source="okx"): pass

try:
    from telegram_alerts import send as tg_send
except ImportError:
    def tg_send(m): pass


class OKXWSMarketMaker:
    def __init__(self):
        self.rm = RiskManager()
        self.active_pairs: list[str] = []
        self.last_scan = 0.0
        self.pnl = 0.0
        self.fills_count = 0
        # Per-instrument state
        self._books: dict[str, dict] = {}  # instId → {bid, ask, mid}
        self._last_quote_mid: dict[str, float] = {}  # mid at last quote
        self._last_quote_time: dict[str, float] = {}  # time of last quote
        self._orders: dict[str, dict] = {}  # ordId → {instId, side, px, sz}
        self._inv: dict[str, dict] = {}  # instId → {qty, cost}
        # Fill-rate adaptive spread: track how fast quotes get filled
        self._fill_rate: dict[str, float] = {}  # instId → fill_rate (0-1)
        self._edge_mult: dict[str, float] = {}  # instId → edge multiplier (0.5-3.0)
        # Dynamic spread: volatility tracker per pair
        self._mid_history: dict[str, deque] = {}  # instId → last 100 mids
        self._realized_vol: dict[str, float] = {}  # instId → realized vol (bps/tick)

    def _update_vol(self, inst_id: str, mid: float):
        """Track realized volatility for dynamic spread calculation."""
        if inst_id not in self._mid_history:
            self._mid_history[inst_id] = deque(maxlen=100)
        hist = self._mid_history[inst_id]
        if hist and hist[-1] > 0:
            ret = abs(mid - hist[-1]) / hist[-1] * 10000  # bps
            alpha = 0.05
            prev_vol = self._realized_vol.get(inst_id, ret)
            self._realized_vol[inst_id] = prev_vol + alpha * (ret - prev_vol)
        hist.append(mid)

    def _dynamic_edge(self, inst_id: str) -> float:
        """Calculate dynamic edge (bps) based on volatility + inventory risk.
        Higher vol → wider spread. Inventory skew → asymmetric quotes."""
        base = EDGE_BPS
        # Vol adjustment: scale edge with realized vol (1x at 5bps vol, 2x at 10bps)
        vol = self._realized_vol.get(inst_id, 5.0)
        vol_mult = max(0.5, min(3.0, vol / 5.0))
        # Inventory risk: penalize concentrated positions
        inv = self._inv.get(inst_id, {"qty": 0})
        mid = self._books.get(inst_id, {}).get("mid", 1)
        inv_usd = abs(inv.get("qty", 0)) * mid
        inv_mult = 1.0 + (inv_usd / 100) * 0.5  # +50% per $100 inventory
        # Fill-rate adjustment (existing)
        fill_mult = self._edge_mult.get(inst_id, 1.0)
        return base * vol_mult * min(2.0, inv_mult) * fill_mult

    def _scan_pairs(self):
        """Find widest-spread pairs via REST (periodic, infrequent)."""
        pairs = scan_spreads(min_bps=MIN_SPREAD_BPS)
        self.active_pairs = [p[0] for p in pairs[:MAX_PAIRS]]
        self.last_scan = time.time()
        log(f"[OKX-MM] Scanning... top {MAX_PAIRS}: {self.active_pairs}")
        for inst, bps, vol in pairs[:MAX_PAIRS]:
            log(f"  {inst}: {bps:.0f}bps, ${vol:,.0f}/24h")

    def _should_requote(self, inst_id: str) -> bool:
        """Requote if mid moved more than REQUOTE_BPS from last quote."""
        book = self._books.get(inst_id)
        if not book:
            return False
        mid = book["mid"]
        self._update_vol(inst_id, mid)
        last_mid = self._last_quote_mid.get(inst_id, 0)
        if last_mid == 0:
            return True
        move = abs(mid - last_mid) / last_mid * 10000
        return move >= REQUOTE_BPS

    def _quote_pair(self, inst_id: str) -> bool:
        """Place bid+ask quotes based on current WS book."""
        ok, reason = self.rm.can_trade()
        if not ok:
            return False

        book = self._books.get(inst_id)
        if not book or book["bid"] <= 0:
            return False

        mid = book["mid"]
        spread_bps = (book["ask"] - book["bid"]) / mid * 10000
        if spread_bps < MIN_SPREAD_BPS:
            return False

        # Update volatility tracker
        self._update_vol(inst_id, mid)

        # Dynamic edge: volatility + inventory risk + fill-rate adaptive
        effective_edge = self._dynamic_edge(inst_id)
        edge = mid * effective_edge / 10000

        # Inventory skew (Avellaneda-Stoikov): shift quotes to reduce position
        inv = self._inv.get(inst_id, {"qty": 0, "cost": 0})
        inv_qty = inv.get("qty", 0)
        inv_usd = inv_qty * mid
        # Skew: proportional to inventory / max_pos. Range [-1, 1]
        max_inv = 200  # max expected inventory USD
        skew = max(-1.0, min(1.0, inv_usd / max_inv))
        # Positive skew (long) → tighter ask (encourage sell), wider bid
        skew_amount = edge * skew * 0.5  # max 50% of edge as skew
        our_bid = book["bid"] + edge - skew_amount  # wider when long
        our_ask = book["ask"] - edge - skew_amount  # tighter when long

        size_usd = self.rm.size_order(spread_bps)
        qty = size_usd / mid

        # Precision
        if mid > 100:
            qty_str, bid_px, ask_px = f"{qty:.4f}", f"{our_bid:.2f}", f"{our_ask:.2f}"
        elif mid > 1:
            qty_str, bid_px, ask_px = f"{qty:.2f}", f"{our_bid:.4f}", f"{our_ask:.4f}"
        else:
            qty_str, bid_px, ask_px = f"{qty:.0f}", f"{our_bid:.6f}", f"{our_ask:.6f}"

        cancel_all(inst_id)

        # Batch place bid+ask simultaneously for lower latency
        orders = [
            {"instId": inst_id, "tdMode": "cash", "side": "buy", "ordType": "post_only", "sz": qty_str, "px": bid_px},
            {"instId": inst_id, "tdMode": "cash", "side": "sell", "ordType": "post_only", "sz": qty_str, "px": ask_px},
        ]
        result = batch_orders(orders)
        placed = 0
        if result.get("code") == "0":
            for i, r in enumerate(result.get("data", [])):
                if r.get("sCode") == "0":
                    placed += 1
                    oid = r.get("ordId", "")
                    side = "buy" if i == 0 else "sell"
                    px = our_bid if i == 0 else our_ask
                    if oid:
                        self._orders[oid] = {"instId": inst_id, "side": side, "px": px, "sz": qty}

        if placed > 0:
            self.rm.add_exposure(size_usd)
            self._last_quote_mid[inst_id] = mid
            self._last_quote_time[inst_id] = time.time()
            log(f"[OKX-MM] {inst_id} bid={bid_px} ask={ask_px} sz=${size_usd:.0f} spd={spread_bps:.0f}bps edge={effective_edge:.1f}bps ({placed}/2 placed)")
        return placed > 0

    def _process_fill(self, data: dict):
        """Process fill from private WS orders channel + adapt edge."""
        state = data.get("state", "")
        if state != "filled" and state != "partially_filled":
            return

        inst = data.get("instId", "")
        side = data.get("side", "")
        fill_px = float(data.get("fillPx") or data.get("avgPx") or "0")
        fill_sz = float(data.get("fillSz") or data.get("accFillSz") or "0")
        fee = float(data.get("fee") or "0")

        if not inst or fill_px <= 0:
            return

        # Fill-rate adaptive spread: track time between quote and fill
        quote_time = self._last_quote_time.get(inst, 0)
        if quote_time > 0:
            time_to_fill = time.time() - quote_time
            # Fast fill (<5s) → too tight, widen. Slow fill (>30s) → too wide, tighten.
            if time_to_fill < 5:
                self._edge_mult[inst] = min(3.0, self._edge_mult.get(inst, 1.0) * 1.1)
            elif time_to_fill > 30:
                self._edge_mult[inst] = max(0.5, self._edge_mult.get(inst, 1.0) * 0.9)
            log(f"[OKX-MM] Fill rate: {inst} filled in {time_to_fill:.1f}s → edge_mult={self._edge_mult.get(inst, 1.0):.2f}")

        # Round-trip PnL tracking
        if inst not in self._inv:
            self._inv[inst] = {"qty": 0.0, "cost": 0.0}
        inv = self._inv[inst]

        if side == "buy":
            inv["cost"] += fill_px * fill_sz
            inv["qty"] += fill_sz
            log(f"[OKX-MM] FILL BUY {inst} {fill_sz}@{fill_px}")
        elif side == "sell" and inv["qty"] > 0:
            avg_entry = inv["cost"] / inv["qty"]
            close_qty = min(fill_sz, inv["qty"])
            pnl = (fill_px - avg_entry) * close_qty + fee
            inv["qty"] -= close_qty
            inv["cost"] -= avg_entry * close_qty
            self.pnl += pnl
            self.fills_count += 1
            self.rm.record_trade(pnl)
            record_trade(pnl, source="okx")
            log(f"[OKX-MM] RT {inst}: entry={avg_entry:.4f} exit={fill_px:.4f} pnl=${pnl:.4f} total=${self.pnl:.4f}")
        else:
            inv["cost"] -= fill_px * fill_sz
            inv["qty"] -= fill_sz
            log(f"[OKX-MM] FILL SELL {inst} {fill_sz}@{fill_px}")

    async def _ws_orderbook(self):
        """Subscribe to real-time orderbook for active pairs."""
        while True:
            if not self.active_pairs:
                await asyncio.sleep(5)
                continue
            args = [{"channel": "books5", "instId": inst} for inst in self.active_pairs]
            try:
                async with websockets.connect(OKX_WS_PUBLIC, ssl=True) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    log(f"[OKX-MM] WS orderbook connected: {self.active_pairs}")
                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", [])
                        if not data:
                            continue
                        d = data[0]
                        inst = msg.get("arg", {}).get("instId", "")
                        bids = d.get("bids", [])
                        asks = d.get("asks", [])
                        if bids and asks:
                            bid = float(bids[0][0])
                            ask = float(asks[0][0])
                            self._books[inst] = {"bid": bid, "ask": ask, "mid": (bid + ask) / 2}
            except Exception as e:
                log(f"[OKX-MM] WS book error: {e}, reconnecting...")
                await asyncio.sleep(3)

    async def _ws_private(self):
        """Subscribe to private orders channel for instant fill detection."""
        if not API_KEY:
            return
        while True:
            try:
                async with websockets.connect(OKX_WS_PRIVATE, ssl=True) as ws:
                    # Authenticate
                    ts = str(int(time.time()))
                    sign_str = f"{ts}GET/users/self/verify"
                    import hmac as _hmac, hashlib, base64
                    sig = base64.b64encode(
                        _hmac.new(SECRET.encode(), sign_str.encode(), hashlib.sha256).digest()
                    ).decode()
                    login = {"op": "login", "args": [{
                        "apiKey": API_KEY, "passphrase": PASSPHRASE,
                        "timestamp": ts, "sign": sig
                    }]}
                    await ws.send(json.dumps(login))
                    resp = json.loads(await ws.recv())
                    if resp.get("event") != "login" or resp.get("code") != "0":
                        log(f"[OKX-MM] WS private login failed: {resp}")
                        await asyncio.sleep(30)
                        continue

                    # Subscribe to orders
                    sub = {"op": "subscribe", "args": [{"channel": "orders", "instType": "SPOT"}]}
                    await ws.send(json.dumps(sub))
                    log("[OKX-MM] WS private connected (fills channel)")

                    async for raw in ws:
                        msg = json.loads(raw)
                        for d in msg.get("data", []):
                            self._process_fill(d)
            except Exception as e:
                log(f"[OKX-MM] WS private error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _requote_loop(self):
        """Check if any pair needs requoting based on mid movement."""
        while True:
            await asyncio.sleep(2)  # check every 2s (vs old 15s REST)
            ok, _ = self.rm.can_trade()
            if not ok:
                continue
            for inst in self.active_pairs:
                if self._should_requote(inst):
                    self._quote_pair(inst)
                    await asyncio.sleep(0.5)  # rate limit between pairs

    async def _scan_loop(self):
        """Periodically rescan for best spread pairs."""
        while True:
            self._scan_pairs()
            await asyncio.sleep(SCAN_INTERVAL * 60)


async def run():
    """Main OKX MM — WebSocket-based."""
    mm = OKXWSMarketMaker()
    log("[OKX-MM] Starting WebSocket-based Market Maker v2...")

    if not os.getenv("OKX_API_KEY"):
        log("[OKX-MM] No OKX_API_KEY — scan-only mode")
        while True:
            mm._scan_pairs()
            await asyncio.sleep(600)
        return

    balance = get_balance("USDT")
    log(f"[OKX-MM] USDT balance: ${balance:.2f}")
    mm._scan_pairs()

    await asyncio.gather(
        mm._ws_orderbook(),
        mm._ws_private(),
        mm._requote_loop(),
        mm._scan_loop(),
    )


if __name__ == "__main__":
    asyncio.run(run())
