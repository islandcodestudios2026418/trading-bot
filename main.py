"""Main entrypoint — Polymarket arb monitor + multi-pair Binance MM."""
import asyncio
import os
import threading

from arb_monitor import start_web, monitor as polymarket_monitor, log
from binance_paper import run as binance_mm_run


async def main():
    symbols = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    log(f"Starting: Polymarket Arb + Binance MM [{symbols}]")
    threading.Thread(target=start_web, daemon=True).start()
    log(f"Dashboard at http://0.0.0.0:{os.getenv('PORT', '8080')}")
    await asyncio.gather(polymarket_monitor(), binance_mm_run())


if __name__ == "__main__":
    asyncio.run(main())
