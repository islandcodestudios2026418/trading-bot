"""Main entrypoint — Polymarket arb + Binance paper MM + OKX real MM.
Includes health monitoring and automatic task restart on failure.
"""
import asyncio
import os
import threading
import time

from arb_monitor import start_web, monitor as polymarket_monitor, log
from binance_paper import run as binance_mm_run
from okx_mm import run as okx_mm_run
from funding_monitor import run as funding_run
from telegram_alerts import daily_summary_loop, send as tg_send

HEALTH_INTERVAL = 300  # log status every 5 min
_start_time = time.time()


async def supervised(name: str, coro_fn):
    """Run a coroutine with automatic restart on crash."""
    while True:
        try:
            await coro_fn()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"⚠️ [{name}] crashed: {e} — restarting in 10s")
            await asyncio.sleep(10)


async def health_monitor():
    """Periodic health check log."""
    while True:
        await asyncio.sleep(HEALTH_INTERVAL)
        uptime = (time.time() - _start_time) / 3600
        try:
            from binance_paper import pair_states, daily_pnl, daily_fills
            pairs_active = sum(1 for ps in pair_states.values() if ps.last_ofi != 0)
            log(f"💓 Health: up {uptime:.1f}h | MM fills={daily_fills} pnl=${daily_pnl:.4f} | pairs_active={pairs_active}")
        except Exception:
            log(f"💓 Health: up {uptime:.1f}h")


async def main():
    symbols = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    log(f"Starting: Polymarket Arb + Binance MM [{symbols}] + OKX MM")
    tg_send(f"🚀 Bot starting: {symbols}")
    threading.Thread(target=start_web, daemon=True).start()
    log(f"Dashboard at http://0.0.0.0:{os.getenv('PORT', '8080')}")
    await asyncio.gather(
        supervised("Polymarket", polymarket_monitor),
        supervised("Binance-MM", binance_mm_run),
        supervised("OKX-MM", okx_mm_run),
        supervised("Funding", funding_run),
        health_monitor(),
        daily_summary_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
