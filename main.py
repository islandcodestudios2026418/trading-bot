"""Main entrypoint — runs Polymarket arb monitor + Binance paper trader together."""
import asyncio
import threading
import os
import sys

# Start the web dashboard (shared by both)
from arb_monitor import start_web, monitor as polymarket_monitor, log
from binance_paper import run as binance_run

BINANCE_SYMBOL = os.getenv("BINANCE_SYMBOL", "MBOXUSDT")


async def main():
    log("Starting combined bot: Polymarket Arb + Binance Paper")
    threading.Thread(target=start_web, daemon=True).start()
    log(f"Dashboard at http://0.0.0.0:{os.getenv('PORT','8080')}")

    await asyncio.gather(
        polymarket_monitor(),
        binance_run(BINANCE_SYMBOL),
    )


if __name__ == "__main__":
    asyncio.run(main())
