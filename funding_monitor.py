"""OKX Funding Rate Arb — scans for extreme rates, executes carry trades.
When |rate| > threshold: open perp + hedge spot to capture funding payments.
Auto-closes when rate normalizes below exit threshold.
"""
import asyncio
import os
import time
from datetime import datetime, timezone, timedelta

import requests

TW_TZ = timezone(timedelta(hours=8))
BASE_URL = os.getenv("OKX_BASE_URL", "https://www.okx.com")
SCAN_INTERVAL = int(os.getenv("FUNDING_SCAN_MIN", "30"))
RATE_THRESHOLD = float(os.getenv("FUNDING_THRESHOLD", "0.05"))  # entry: 0.05%/8h (~68% APR)
RATE_EXIT = float(os.getenv("FUNDING_EXIT", "0.02"))  # exit when rate drops below this
ARB_SIZE_USD = float(os.getenv("FUNDING_SIZE_USD", "50"))  # per-leg size
ARB_ENABLED = os.getenv("FUNDING_ARB_ENABLED", "0") == "1"  # manual enable required

try:
    from arb_monitor import log
except ImportError:
    def log(m): print(f"[{datetime.now(TW_TZ).strftime('%H:%M:%S')}] {m}", flush=True)

try:
    from telegram_alerts import send as tg_send
except ImportError:
    def tg_send(m): pass


def get_funding_rates() -> list[dict]:
    """Fetch current funding rates for all SWAP instruments."""
    try:
        r = requests.get(f"{BASE_URL}/api/v5/public/funding-rate-all", timeout=10)
        data = r.json()
        if data.get("data"):
            return data["data"]
    except Exception:
        pass
    # Fallback: top instruments individually
    try:
        r = requests.get(f"{BASE_URL}/api/v5/public/instruments",
                         params={"instType": "SWAP"}, timeout=10)
        instruments = r.json().get("data", [])
        rates = []
        for inst in instruments[:50]:
            rr = requests.get(f"{BASE_URL}/api/v5/public/funding-rate",
                              params={"instId": inst["instId"]}, timeout=5)
            d = rr.json().get("data", [])
            if d:
                rates.append(d[0])
            time.sleep(0.1)
        return rates
    except Exception as e:
        log(f"[FUNDING] Error fetching rates: {e}")
        return []


def scan_extreme_rates() -> list[dict]:
    """Return instruments with |funding rate| > threshold."""
    rates = get_funding_rates()
    extreme = []
    for r in rates:
        inst_id = r.get("instId", "")
        rate = float(r.get("fundingRate", "0"))
        if abs(rate) * 100 >= RATE_THRESHOLD:
            apr = rate * 3 * 365 * 100
            extreme.append({
                "instId": inst_id,
                "rate_pct": rate * 100,
                "apr": apr,
                "direction": "SHORT" if rate > 0 else "LONG",
            })
    return sorted(extreme, key=lambda x: abs(x["rate_pct"]), reverse=True)


# --- Arb Execution ---

# Active arb positions: {instId: {"perp_side", "size", "entry_rate", "opened_at"}}
_arb_positions: dict[str, dict] = {}

# OKX funding settlement times: 00:00, 08:00, 16:00 UTC
_FUNDING_HOURS = [0, 8, 16]
TIMING_ENTRY_BEFORE_MIN = int(os.getenv("FUNDING_ENTRY_BEFORE_MIN", "60"))  # enter 60min before
TIMING_EXIT_AFTER_MIN = int(os.getenv("FUNDING_EXIT_AFTER_MIN", "5"))  # exit 5min after


def _minutes_to_next_settlement() -> int:
    """Minutes until next OKX funding settlement."""
    now = datetime.now(timezone.utc)
    current_min_of_day = now.hour * 60 + now.minute
    settlement_mins = [h * 60 for h in _FUNDING_HOURS]
    # Find next settlement
    for sm in settlement_mins:
        if sm > current_min_of_day:
            return sm - current_min_of_day
    # Wrap to next day
    return (24 * 60 - current_min_of_day) + settlement_mins[0]


def _minutes_since_last_settlement() -> int:
    """Minutes since last funding settlement."""
    now = datetime.now(timezone.utc)
    current_min_of_day = now.hour * 60 + now.minute
    settlement_mins = [h * 60 for h in _FUNDING_HOURS]
    # Find last settlement
    for sm in reversed(settlement_mins):
        if sm <= current_min_of_day:
            return current_min_of_day - sm
    # Wrap from previous day
    return current_min_of_day + (24 * 60 - settlement_mins[-1])


def _is_optimal_entry_window() -> bool:
    """True if we're in the optimal entry window (T-60min to T-5min before settlement)."""
    mins_to = _minutes_to_next_settlement()
    return 5 <= mins_to <= TIMING_ENTRY_BEFORE_MIN


def _is_post_settlement_exit() -> bool:
    """True if we just passed a settlement (within exit window)."""
    mins_since = _minutes_since_last_settlement()
    return mins_since <= TIMING_EXIT_AFTER_MIN


def _execute_arb_entry(inst_id: str, direction: str, rate_pct: float):
    """Open funding arb: perp + spot hedge. Timing-aware: prefer entry T-1h before settlement."""
    from okx_client import place_order, get_orderbook

    # Get spot instrument (e.g. BTC-USDT-SWAP → BTC-USDT)
    spot_inst = inst_id.replace("-SWAP", "")
    book = get_orderbook(spot_inst, depth=1)
    bids, asks = book.get("bids", []), book.get("asks", [])
    if not bids or not asks:
        log(f"[FUNDING-ARB] No book for {spot_inst}, skipping")
        return

    mid = (float(bids[0][0]) + float(asks[0][0])) / 2
    qty = ARB_SIZE_USD / mid

    # Perp: SHORT if rate positive (we receive funding), LONG if negative
    perp_side = "sell" if direction == "SHORT" else "buy"
    spot_side = "buy" if direction == "SHORT" else "sell"

    # Size formatting
    qty_str = f"{qty:.4f}" if mid > 100 else f"{qty:.2f}" if mid > 1 else f"{qty:.0f}"

    # Execute perp leg (market order for speed)
    r1 = place_order(inst_id, perp_side, qty_str, order_type="market", td_mode="cross")
    # Execute spot hedge
    r2 = place_order(spot_inst, spot_side, qty_str, order_type="market", td_mode="cash")

    p_ok = r1.get("code") == "0"
    s_ok = r2.get("code") == "0"

    if p_ok and s_ok:
        _arb_positions[inst_id] = {
            "perp_side": perp_side, "spot_side": spot_side,
            "spot_inst": spot_inst, "size": qty_str,
            "entry_rate": rate_pct, "opened_at": time.time()
        }
        log(f"[FUNDING-ARB] OPENED {inst_id}: perp={perp_side} spot={spot_side} sz={qty_str} rate={rate_pct:+.4f}%")
        tg_send(f"💰 <b>Funding Arb Opened</b>\n{inst_id}: {perp_side} perp + {spot_side} spot\nRate: {rate_pct:+.4f}%/8h | Size: ${ARB_SIZE_USD}")
    else:
        err = r1.get("msg", "") + " | " + r2.get("msg", "")
        log(f"[FUNDING-ARB] Entry failed {inst_id}: {err}")


def _execute_arb_exit(inst_id: str, current_rate: float):
    """Close funding arb position."""
    from okx_client import place_order
    pos = _arb_positions.get(inst_id)
    if not pos:
        return

    # Reverse both legs
    close_perp = "buy" if pos["perp_side"] == "sell" else "sell"
    close_spot = "sell" if pos["spot_side"] == "buy" else "buy"

    r1 = place_order(inst_id, close_perp, pos["size"], order_type="market", td_mode="cross")
    r2 = place_order(pos["spot_inst"], close_spot, pos["size"], order_type="market", td_mode="cash")

    hours_held = (time.time() - pos["opened_at"]) / 3600
    del _arb_positions[inst_id]
    log(f"[FUNDING-ARB] CLOSED {inst_id}: held {hours_held:.1f}h, rate now {current_rate:+.4f}%")
    tg_send(f"📤 <b>Funding Arb Closed</b>\n{inst_id}: held {hours_held:.1f}h\nRate normalized: {current_rate:+.4f}%")


async def run():
    """Funding rate monitor + arb execution loop."""
    log(f"[FUNDING] Starting funding monitor (arb={'ON' if ARB_ENABLED else 'OFF'}, threshold={RATE_THRESHOLD}%)")
    while True:
        try:
            extreme = scan_extreme_rates()

            # Check exits first — close if rate normalized OR post-settlement window
            for inst_id in list(_arb_positions.keys()):
                current = next((e for e in extreme if e["instId"] == inst_id), None)
                current_rate = current["rate_pct"] if current else 0
                # Exit conditions: rate normalized OR just after settlement (captured the payment)
                pos = _arb_positions[inst_id]
                held_hours = (time.time() - pos["opened_at"]) / 3600
                if abs(current_rate) < RATE_EXIT * 100:
                    _execute_arb_exit(inst_id, current_rate)
                elif _is_post_settlement_exit() and held_hours >= 0.5:
                    # Collected funding — exit to free capital
                    _execute_arb_exit(inst_id, current_rate)
                    log(f"[FUNDING-ARB] Timing exit: collected funding after settlement")

            # Alert + optionally open new arbs (timing-aware)
            if extreme:
                top = extreme[:5]
                mins_to = _minutes_to_next_settlement()
                lines = [f"[FUNDING] {len(extreme)} extreme rates (next settlement in {mins_to}min):"]
                for e in top:
                    in_pos = "✅" if e["instId"] in _arb_positions else ""
                    lines.append(f"  {e['instId']}: {e['rate_pct']:+.4f}% ({e['apr']:+.0f}% APR) → {e['direction']} {in_pos}")
                log("\n".join(lines))

                # Execute arb on top signal if enabled and not already in position
                if ARB_ENABLED:
                    best = top[0]
                    if best["instId"] not in _arb_positions and len(_arb_positions) < 3:
                        # Timing: prefer entry during optimal window (T-60min to T-5min)
                        if _is_optimal_entry_window():
                            _execute_arb_entry(best["instId"], best["direction"], best["rate_pct"])
                            log(f"[FUNDING-ARB] Timed entry: {mins_to}min to settlement")
                        elif abs(best["rate_pct"]) >= RATE_THRESHOLD * 3:
                            # 3x threshold = exceptional rate, enter regardless of timing
                            _execute_arb_entry(best["instId"], best["direction"], best["rate_pct"])
                            log(f"[FUNDING-ARB] Exceptional rate entry: {best['rate_pct']:+.4f}%")
                else:
                    best = top[0]
                    tg_send(
                        f"📈 <b>Funding Signal</b>\n"
                        f"{best['instId']}: {best['rate_pct']:+.4f}%/8h ({best['apr']:+.0f}% APR)\n"
                        f"Direction: {best['direction']} perp + hedge spot\n"
                        f"Arb exec: OFF (set FUNDING_ARB_ENABLED=1)"
                    )
            else:
                log("[FUNDING] No extreme rates — market neutral")

            # Status of open positions
            if _arb_positions:
                log(f"[FUNDING-ARB] Open positions: {list(_arb_positions.keys())}")

        except Exception as e:
            log(f"[FUNDING] Error: {e}")

        await asyncio.sleep(SCAN_INTERVAL * 60)


if __name__ == "__main__":
    print("Scanning OKX funding rates...")
    extreme = scan_extreme_rates()
    if extreme:
        print(f"\n{len(extreme)} pairs with |rate| > {RATE_THRESHOLD}%:")
        for e in extreme[:15]:
            print(f"  {e['instId']:20s} rate={e['rate_pct']:+.4f}%  APR={e['apr']:+.0f}%  → {e['direction']}")
    else:
        print("No extreme rates found.")
