"""
Tick data recorder — captures depth20 + aggTrades for offline replay.
Rolling 24h file in JSONL format. Used for strategy backtesting.
Enable with RECORD_TICKS=1.
"""
import asyncio
import gzip
import json
import os
import time
from datetime import datetime, timezone, timedelta

import websockets

TW_TZ = timezone(timedelta(hours=8))
ENABLED = os.getenv("RECORD_TICKS", "0") == "1"
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")]
DATA_DIR = os.getenv("TICK_DATA_DIR", "tick_data")
ROTATE_HOURS = int(os.getenv("TICK_ROTATE_HOURS", "24"))

_file = None
_file_start = 0


def _get_file():
    """Get current output file, rotating every ROTATE_HOURS."""
    global _file, _file_start
    now = time.time()
    if _file and (now - _file_start) < ROTATE_HOURS * 3600:
        return _file
    # Rotate
    if _file:
        _file.close()
    os.makedirs(DATA_DIR, exist_ok=True)
    ts = datetime.now(TW_TZ).strftime("%Y%m%d_%H%M")
    path = os.path.join(DATA_DIR, f"ticks_{ts}.jsonl.gz")
    _file = gzip.open(path, "at", encoding="utf-8")
    _file_start = now
    return _file


def _write(record: dict):
    """Write one tick record."""
    f = _get_file()
    f.write(json.dumps(record, separators=(",", ":")) + "\n")


async def record_streams():
    """Record depth20 + aggTrades for all symbols."""
    if not ENABLED:
        return

    streams = []
    for s in SYMBOLS:
        streams.append(f"{s.lower()}@depth20@100ms")
        streams.append(f"{s.lower()}@aggTrade")
    url = f"wss://data-stream.binance.vision/stream?streams={'/'.join(streams)}"

    try:
        from arb_monitor import log
    except ImportError:
        def log(m): print(m, flush=True)

    log(f"[RECORDER] Starting tick recorder: {SYMBOLS} → {DATA_DIR}/")

    async for ws in websockets.connect(url, ssl=True):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                stream = msg.get("stream", "")
                data = msg.get("data", {})
                ts_ms = int(time.time() * 1000)

                if "depth" in stream:
                    symbol = stream.split("@")[0].upper()
                    _write({"t": ts_ms, "s": symbol, "type": "depth",
                            "b": [[b[0], b[1]] for b in data.get("bids", [])[:10]],
                            "a": [[a[0], a[1]] for a in data.get("asks", [])[:10]]})
                elif "aggTrade" in stream:
                    d = data
                    _write({"t": ts_ms, "s": d.get("s", ""), "type": "trade",
                            "p": d.get("p"), "q": d.get("q"), "m": d.get("m")})
        except websockets.ConnectionClosed:
            await asyncio.sleep(2)


async def run():
    """Run recorder (only if RECORD_TICKS=1)."""
    if not ENABLED:
        while True:
            await asyncio.sleep(3600)  # sleep forever if disabled
    await record_streams()


if __name__ == "__main__":
    os.environ["RECORD_TICKS"] = "1"
    ENABLED = True
    asyncio.run(record_streams())
