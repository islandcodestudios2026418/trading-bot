"""
OKX Spot Market Maker — real spread capture with risk management.
Scans widest-spread pairs, posts maker-only quotes, captures bid-ask.
Uses risk_manager for sizing + kill switch, okx_client for execution.
"""
import asyncio
import os
import time
from datetime import datetime, timezone, timedelta

from okx_client import (
    get_orderbook, scan_spreads, place_order, cancel_all,
    get_balance, get_fills, get_open_orders
)
from risk_manager import RiskManager

TW_TZ = timezone(timedelta(hours=8))

# Config
REFRESH_INTERVAL = int(os.getenv("OKX_REFRESH_SEC", "15"))  # requote every N sec
SCAN_INTERVAL = int(os.getenv("OKX_SCAN_MIN", "10"))  # rescan pairs every N min
MIN_SPREAD_BPS = float(os.getenv("OKX_MIN_SPREAD", "25"))  # only trade 25bps+ spreads
MAX_PAIRS = int(os.getenv("OKX_MAX_PAIRS", "3"))  # trade top N pairs
EDGE_BPS = float(os.getenv("OKX_EDGE_BPS", "3"))  # tighten quotes by 3bps from best

try:
    from arb_monitor import log, record_trade
except ImportError:
    def log(m): print(f"[{datetime.now(TW_TZ).strftime('%H:%M:%S')}] {m}", flush=True)
    def record_trade(p, source="okx"): pass


class OKXMarketMaker:
    def __init__(self):
        self.rm = RiskManager()
        self.active_pairs: list[str] = []
        self.last_scan = 0.0
        self.fills_count = 0
        self.pnl = 0.0
        self._inv: dict[str, dict] = {}  # per-instrument inventory for RT tracking

    def _scan_pairs(self):
        """Find widest-spread pairs to market-make."""
        pairs = scan_spreads(min_bps=MIN_SPREAD_BPS)
        self.active_pairs = [p[0] for p in pairs[:MAX_PAIRS]]
        self.last_scan = time.time()
        log(f"[OKX-MM] Scanning... top {MAX_PAIRS} pairs: {self.active_pairs}")
        for inst, bps, vol in pairs[:MAX_PAIRS]:
            log(f"  {inst}: {bps:.0f}bps spread, ${vol:,.0f} vol/24h")

    def _quote_pair(self, inst_id: str) -> bool:
        """Place bid+ask for one pair. Returns True if orders placed."""
        ok, reason = self.rm.can_trade()
        if not ok:
            log(f"[OKX-MM] Risk block: {reason}")
            return False

        book = get_orderbook(inst_id, depth=5)
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return False

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        mid = (best_bid + best_ask) / 2
        spread_bps = (best_ask - best_bid) / mid * 10000

        if spread_bps < MIN_SPREAD_BPS:
            return False

        # Tighten by EDGE_BPS inside the spread
        edge = mid * EDGE_BPS / 10000
        our_bid = best_bid + edge
        our_ask = best_ask - edge

        # Size from risk manager
        size_usd = self.rm.size_order(spread_bps)
        qty = size_usd / mid
        # Round to reasonable precision
        if mid > 100:
            qty_str = f"{qty:.4f}"
            bid_px = f"{our_bid:.2f}"
            ask_px = f"{our_ask:.2f}"
        elif mid > 1:
            qty_str = f"{qty:.2f}"
            bid_px = f"{our_bid:.4f}"
            ask_px = f"{our_ask:.4f}"
        else:
            qty_str = f"{qty:.0f}"
            bid_px = f"{our_bid:.6f}"
            ask_px = f"{our_ask:.6f}"

        # Cancel existing, then place new quotes
        cancel_all(inst_id)

        r_bid = place_order(inst_id, "buy", qty_str, bid_px, "post_only")
        r_ask = place_order(inst_id, "sell", qty_str, ask_px, "post_only")

        bid_ok = r_bid.get("code") == "0"
        ask_ok = r_ask.get("code") == "0"

        if bid_ok or ask_ok:
            self.rm.add_exposure(size_usd)
            log(f"[OKX-MM] {inst_id} quoted: bid={bid_px} ask={ask_px} sz=${size_usd:.0f} spread={spread_bps:.0f}bps")
        else:
            err = r_bid.get("msg", "") or r_ask.get("msg", "")
            if err:
                log(f"[OKX-MM] {inst_id} order error: {err}")

        return bid_ok or ask_ok

    def _check_fills(self):
        """Check recent fills and compute round-trip PnL from matched buy/sell pairs."""
        fills = get_fills(limit=20)
        if not fills:
            return
        new_count = len(fills)
        if new_count <= self.fills_count:
            return
        added = new_count - self.fills_count
        self.fills_count = new_count

        for f in fills[:added]:
            inst = f.get("instId", "")
            side = f.get("side", "")
            px = float(f.get("fillPx", "0"))
            sz = float(f.get("fillSz", "0"))
            fee = float(f.get("fee", "0"))

            if not inst or not px:
                continue

            # Track per-instrument inventory to match round trips
            if inst not in self._inv:
                self._inv[inst] = {"qty": 0.0, "cost": 0.0}
            inv = self._inv[inst]

            if side == "buy":
                inv["cost"] += px * sz
                inv["qty"] += sz
            elif side == "sell" and inv["qty"] > 0:
                # Close: compute PnL from avg entry
                avg_entry = inv["cost"] / inv["qty"] if inv["qty"] > 0 else px
                close_qty = min(sz, inv["qty"])
                pnl = (px - avg_entry) * close_qty + fee
                inv["qty"] -= close_qty
                inv["cost"] -= avg_entry * close_qty
                self.pnl += pnl
                self.rm.record_trade(pnl)
                record_trade(pnl, source="okx")
                log(f"[OKX-MM] RT {inst}: entry={avg_entry:.6f} exit={px:.6f} pnl=${pnl:.4f}")
            else:
                # Sell without inventory = short open, track negative
                inv["cost"] -= px * sz
                inv["qty"] -= sz

        log(f"[OKX-MM] {added} new fills | cumulative PnL: ${self.pnl:.4f}")


async def run():
    """Main OKX MM loop."""
    mm = OKXMarketMaker()
    log("[OKX-MM] Starting OKX Market Maker...")

    # Check if API keys are configured
    if not os.getenv("OKX_API_KEY"):
        log("[OKX-MM] No OKX_API_KEY set — running in scan-only mode")
        while True:
            mm._scan_pairs()
            await asyncio.sleep(600)
        return

    balance = get_balance("USDT")
    log(f"[OKX-MM] USDT balance: ${balance:.2f}")

    while True:
        try:
            # Rescan pairs periodically
            if time.time() - mm.last_scan > SCAN_INTERVAL * 60:
                mm._scan_pairs()

            if not mm.active_pairs:
                await asyncio.sleep(60)
                continue

            # Quote each active pair
            for inst in mm.active_pairs:
                mm._quote_pair(inst)
                await asyncio.sleep(1)  # rate limit

            # Check for fills
            mm._check_fills()

            # Status
            ok, reason = mm.rm.can_trade()
            if not ok:
                log(f"[OKX-MM] {mm.rm.status()}")
                # Cancel all orders if killed
                for inst in mm.active_pairs:
                    cancel_all(inst)
                await asyncio.sleep(300)  # wait 5min before retry
                continue

            await asyncio.sleep(REFRESH_INTERVAL)

        except Exception as e:
            log(f"[OKX-MM] Error: {e}")
            await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(run())
