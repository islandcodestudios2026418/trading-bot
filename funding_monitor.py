"""OKX Funding Rate Monitor — scans for extreme rates (carry trade signals).
Public API, no auth needed. Alerts via Telegram when |rate| > threshold.
"""
import asyncio
import os
import time
from datetime import datetime, timezone, timedelta

import requests

TW_TZ = timezone(timedelta(hours=8))
BASE_URL = os.getenv("OKX_BASE_URL", "https://www.okx.com")
SCAN_INTERVAL = int(os.getenv("FUNDING_SCAN_MIN", "30"))  # scan every 30min
RATE_THRESHOLD = float(os.getenv("FUNDING_THRESHOLD", "0.05"))  # 0.05% per 8h = ~68% APR

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
        return data.get("data", [])
    except Exception:
        # Fallback: fetch top instruments individually
        try:
            r = requests.get(f"{BASE_URL}/api/v5/public/instruments",
                             params={"instType": "SWAP"}, timeout=10)
            instruments = r.json().get("data", [])
            rates = []
            for inst in instruments[:50]:  # top 50 only
                inst_id = inst["instId"]
                rr = requests.get(f"{BASE_URL}/api/v5/public/funding-rate",
                                  params={"instId": inst_id}, timeout=5)
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
            apr = rate * 3 * 365 * 100  # 3x daily (8h intervals) * 365
            extreme.append({
                "instId": inst_id,
                "rate_pct": rate * 100,
                "apr": apr,
                "direction": "SHORT" if rate > 0 else "LONG",
            })
    return sorted(extreme, key=lambda x: abs(x["rate_pct"]), reverse=True)


async def run():
    """Funding rate monitor loop."""
    log("[FUNDING] Starting funding rate monitor...")
    while True:
        try:
            extreme = scan_extreme_rates()
            if extreme:
                top = extreme[:5]
                lines = [f"[FUNDING] {len(extreme)} extreme rates found:"]
                for e in top:
                    lines.append(f"  {e['instId']}: {e['rate_pct']:+.4f}% ({e['apr']:+.0f}% APR) → {e['direction']}")
                msg = "\n".join(lines)
                log(msg)
                # Telegram alert for top signal
                best = top[0]
                tg_send(
                    f"📈 <b>Funding Signal</b>\n"
                    f"{best['instId']}: {best['rate_pct']:+.4f}%/8h ({best['apr']:+.0f}% APR)\n"
                    f"Direction: {best['direction']} perp + hedge spot\n"
                    f"Total extreme: {len(extreme)} pairs"
                )
            else:
                log("[FUNDING] No extreme rates found — market neutral")
        except Exception as e:
            log(f"[FUNDING] Error: {e}")

        await asyncio.sleep(SCAN_INTERVAL * 60)


if __name__ == "__main__":
    # One-shot scan for testing
    print("Scanning OKX funding rates...")
    extreme = scan_extreme_rates()
    if extreme:
        print(f"\n{len(extreme)} pairs with |rate| > {RATE_THRESHOLD}%:")
        for e in extreme[:15]:
            print(f"  {e['instId']:20s} rate={e['rate_pct']:+.4f}%  APR={e['apr']:+.0f}%  → {e['direction']}")
    else:
        print("No extreme rates found.")
