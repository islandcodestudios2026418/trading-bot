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


def alert_event(event_type: str, severity: float, action: str, details: str, symbol: str = ""):
    """Alert on market microstructure event (liquidation cascade, flash crash, etc.)."""
    # Only alert on significant events (severity > 0.5)
    if severity < 0.5:
        return
    emoji_map = {
        "liquidation_cascade": "🌊",
        "flash_crash": "⚡",
        "whale_accumulation": "🐋",
        "funding_spike": "📈",
        "exchange_halt_risk": "🚨",
    }
    emoji = emoji_map.get(event_type, "⚠️")
    action_emoji = {"pause": "⏸️", "exit_all": "🛑", "reduce": "📉", "widen_spread": "↔️"}.get(action, "")
    sym_str = f" [{symbol}]" if symbol else ""
    send(
        f"{emoji} <b>EVENT{sym_str}</b>\n"
        f"Type: {event_type.replace('_', ' ').title()}\n"
        f"Severity: {severity:.0%} | Action: {action_emoji} {action}\n"
        f"<code>{details}</code>"
    )


async def daily_summary_loop():
    """Send comprehensive daily P&L report at 23:59 TW time."""
    global _last_daily
    while True:
        now = datetime.now(TW_TZ)
        if now.hour == 23 and now.minute == 59 and time.time() - _last_daily > 3600:
            _last_daily = time.time()
            lines = [f"📊 <b>Daily Report</b> ({now.strftime('%Y-%m-%d')})"]

            # Binance MM summary
            try:
                from binance_paper import daily_pnl, daily_fills, daily_wins, pair_states
                wr = f"{daily_wins/daily_fills*100:.0f}%" if daily_fills else "-"
                lines.append(f"\n<b>Binance MM:</b> ${daily_pnl:.4f} | {daily_fills} fills | WR {wr}")
                for sym, ps in pair_states.items():
                    if ps.fills > 0:
                        swr = f"{ps.wins/ps.fills*100:.0f}%"
                        lines.append(f"  {sym}: ${ps.pnl:.4f} ({ps.fills}f, {swr})")
            except Exception:
                pass

            # OKX MM
            try:
                from okx_mm import OKXWSMarketMaker
                # Note: we can't easily access the running instance, log what we can
            except Exception:
                pass

            # Regime breakdown
            try:
                from binance_paper import pair_states
                from regime import TRENDING, RANGING, NEUTRAL
                regime_agg = {TRENDING: 0.0, RANGING: 0.0, NEUTRAL: 0.0}
                for ps in pair_states.values():
                    for r in (TRENDING, RANGING, NEUTRAL):
                        regime_agg[r] += ps.regime.regime_pnl.get(r, 0.0)
                lines.append(f"\n<b>By Regime:</b>")
                for r, pnl in regime_agg.items():
                    lines.append(f"  {r}: ${pnl:.4f}")
            except Exception:
                pass

            # Signal attribution
            try:
                from signal_attrib import attrib
                report = attrib.get_report()
                if report:
                    lines.append(f"\n<b>Signal Attribution (top):</b>")
                    sorted_sigs = sorted(report.items(), key=lambda x: x[1]["total_pnl"], reverse=True)
                    for sig, data in sorted_sigs[:5]:
                        status = "✅" if data["enabled"] else "❌"
                        lines.append(f"  {status} {sig}: ${data['total_pnl']:.4f} ({data['count']}t)")
            except Exception:
                pass

            send("\n".join(lines))
        await asyncio.sleep(30)
