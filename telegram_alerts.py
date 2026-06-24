"""Telegram alerts — trade signals, daily P&L, kill switch notifications."""
import asyncio
import os
import time
from datetime import datetime, timezone, timedelta

import requests

TW_TZ = timezone(timedelta(hours=8))
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
_last_daily: float = 0.0


def send(msg: str):
    """Send a Telegram message. Silent if not configured."""
    if not BOT_TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=5
        )
    except Exception:
        pass


def alert_trade(symbol: str, side: str, profit: float, total_pnl: float):
    """Alert on significant trades."""
    emoji = "🟢" if profit > 0 else "🔴"
    send(f"{emoji} <b>{symbol}</b> {side} ${profit:.4f}\nDaily: ${total_pnl:.4f}")


def alert_kill(reason: str):
    """Alert on kill switch trigger."""
    send(f"⛔ <b>KILL SWITCH</b>\n{reason}")


def alert_arb(question: str, total_cost: float, profit_usd: float):
    """Alert on Polymarket arb opportunity."""
    send(f"💰 <b>ARB FOUND</b>\n{question}\nCost: ${total_cost:.4f} | Profit: ${profit_usd:.2f}")


async def daily_summary_loop():
    """Send daily P&L summary at 23:59 TW time."""
    global _last_daily
    while True:
        now = datetime.now(TW_TZ)
        # Send at 23:59
        if now.hour == 23 and now.minute == 59 and time.time() - _last_daily > 3600:
            _last_daily = time.time()
            try:
                from binance_paper import daily_pnl, daily_fills, daily_wins
                wr = f"{daily_wins/daily_fills*100:.0f}%" if daily_fills else "-"
                send(
                    f"📊 <b>Daily Summary</b> ({now.strftime('%Y-%m-%d')})\n"
                    f"PnL: ${daily_pnl:.4f} | Fills: {daily_fills} | WR: {wr}"
                )
            except Exception:
                pass
        await asyncio.sleep(30)
