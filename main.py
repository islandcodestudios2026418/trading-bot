"""Main entrypoint — Polymarket arb + Binance paper MM + OKX real MM.
Includes health monitoring and automatic task restart on failure.
"""
import asyncio
import os
import signal
import threading
import time

from arb_monitor import start_web, monitor as polymarket_monitor, log
from binance_paper import run as binance_mm_run
from okx_mm import run as okx_mm_run
from funding_monitor import run as funding_run
from cross_arb import run as cross_arb_run
from tick_recorder import run as tick_recorder_run
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
    """Periodic health check + degraded condition alerting."""
    while True:
        await asyncio.sleep(HEALTH_INTERVAL)
        uptime = (time.time() - _start_time) / 3600
        try:
            from binance_paper import pair_states, daily_pnl, daily_fills
            pairs_active = sum(1 for ps in pair_states.values() if ps.last_ofi != 0)
            log(f"💓 Health: up {uptime:.1f}h | MM fills={daily_fills} pnl=${daily_pnl:.4f} | pairs_active={pairs_active}")
        except Exception:
            log(f"💓 Health: up {uptime:.1f}h")

        # Alert on stale WS connections
        try:
            from ws_manager import any_stale
            stale = any_stale()
            if stale:
                tg_send(f"⚠️ Stale WS: {', '.join(stale)} (>30s no data)")
        except Exception:
            pass

        # Alert on circuit breaker
        try:
            from risk_manager import RiskManager
            rm = RiskManager()
            # Check from OKX MM instance
            from okx_mm import _mm_instance
            if _mm_instance and _mm_instance.rm.circuit_breaker.tripped:
                tg_send(f"🔴 Circuit breaker TRIPPED: {_mm_instance.rm.circuit_breaker.trip_reason}")
        except Exception:
            pass


async def main():
    symbols = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    log(f"Starting: Polymarket Arb + Binance MM [{symbols}] + OKX MM + Cross-Arb")
    tg_send(f"🚀 Bot starting: {symbols}")
    threading.Thread(target=start_web, daemon=True).start()
    log(f"Dashboard at http://0.0.0.0:{os.getenv('PORT', '8080')}")

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_event_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows

    tasks = asyncio.gather(
        supervised("Polymarket", polymarket_monitor),
        supervised("Binance-MM", binance_mm_run),
        supervised("OKX-MM", okx_mm_run),
        supervised("Funding", funding_run),
        supervised("Cross-Arb", cross_arb_run),
        supervised("Tick-Recorder", tick_recorder_run),
        health_monitor(),
        daily_summary_loop(),
    )

    # Wait for stop signal or tasks to complete
    done = asyncio.ensure_future(tasks)
    await asyncio.wait([done, asyncio.ensure_future(stop.wait())], return_when=asyncio.FIRST_COMPLETED)
    if stop.is_set():
        log("🛑 Shutdown signal received — stopping gracefully")
        tg_send("🛑 Bot stopping (signal)")
        # Save stats before exit
        try:
            from binance_paper import _save_stats
            _save_stats()
            log("💾 Stats saved on shutdown")
        except Exception:
            pass
        try:
            from pnl_store import save as pnl_save
            pnl_save()
            log("💾 PnL history saved on shutdown")
        except Exception:
            pass
        done.cancel()
        try:
            await done
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
