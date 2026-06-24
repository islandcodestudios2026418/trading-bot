"""
Multi-pair Binance Market Maker (paper) — depth stream, OFI, volatility filter.
"""
import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

import websockets

TW_TZ = timezone(timedelta(hours=8))

try:
    from arb_monitor import record_trade, log
except ImportError:
    def record_trade(p): pass
    def log(m): print(m, flush=True)

try:
    from telegram_alerts import alert_trade, alert_kill
except ImportError:
    def alert_trade(*a): pass
    def alert_kill(*a): pass

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")]
MAX_POS_USD = float(os.getenv("MAX_POS_USD", "100"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "50"))
CAPITAL = float(os.getenv("CAPITAL", "2000"))
CONSEC_LOSS_PAUSE = 5
PAUSE_SECONDS = 300
STATS_FILE = os.getenv("STATS_FILE", "stats.json")


@dataclass
class OFITracker:
    """Multi-timeframe OFI: exponential moving OFI at 1s/5s/30s with adaptive weights."""
    ofi_1s: float = 0.0
    ofi_5s: float = 0.0
    ofi_30s: float = 0.0
    last_bid_vol: float = 0.0
    last_ask_vol: float = 0.0
    _alpha_1s: float = 1 - 0.93
    _alpha_5s: float = 1 - 0.986
    _alpha_30s: float = 1 - 0.998
    # Adaptive weights — start at 0.5/0.3/0.2, adjust based on win rates
    w1: float = 0.5
    w5: float = 0.3
    w30: float = 0.2
    # Win/loss counters per dominant timeframe
    _wins: list = field(default_factory=lambda: [0, 0, 0])  # 1s, 5s, 30s
    _losses: list = field(default_factory=lambda: [0, 0, 0])
    _rebalance_count: int = 0

    def update(self, bid_vol: float, ask_vol: float) -> float:
        """Update with new depth snapshot. Returns weighted composite OFI."""
        total = bid_vol + ask_vol
        raw_ofi = (bid_vol - ask_vol) / total if total > 0 else 0
        self.ofi_1s += self._alpha_1s * (raw_ofi - self.ofi_1s)
        self.ofi_5s += self._alpha_5s * (raw_ofi - self.ofi_5s)
        self.ofi_30s += self._alpha_30s * (raw_ofi - self.ofi_30s)
        return self.w1 * self.ofi_1s + self.w5 * self.ofi_5s + self.w30 * self.ofi_30s

    def dominant_tf(self) -> int:
        """Which timeframe had the strongest signal at this moment? 0=1s, 1=5s, 2=30s."""
        vals = [abs(self.ofi_1s), abs(self.ofi_5s), abs(self.ofi_30s)]
        return vals.index(max(vals))

    def record_outcome(self, win: bool):
        """Record trade outcome for dominant timeframe at entry."""
        tf = self.dominant_tf()
        if win:
            self._wins[tf] += 1
        else:
            self._losses[tf] += 1
        self._rebalance_count += 1
        # Rebalance every 50 trades
        if self._rebalance_count >= 50:
            self._rebalance()

    def _rebalance(self):
        """Shift weights toward timeframes with higher win rates."""
        self._rebalance_count = 0
        rates = []
        for i in range(3):
            total = self._wins[i] + self._losses[i]
            rates.append(self._wins[i] / total if total >= 5 else 0.5)
        # Softmax-style reweight
        s = sum(rates)
        if s > 0:
            self.w1 = rates[0] / s
            self.w5 = rates[1] / s
            self.w30 = rates[2] / s


@dataclass
class PairState:
    symbol: str
    position: float = 0.0
    entry_price: float = 0.0
    pnl: float = 0.0
    fills: int = 0
    wins: int = 0
    paused_until: float = 0.0
    consec_losses: int = 0
    mid_prices: deque = field(default_factory=lambda: deque(maxlen=300))
    last_atr: float = 0.0
    last_ofi: float = 0.0
    last_spread_bps: float = 0.0
    ofi_tracker: OFITracker = field(default_factory=OFITracker)
    # Mean-reversion: VWAP anchor
    vwap_num: float = 0.0
    vwap_den: float = 0.0
    # Kelly sizing: track recent trade sizes
    _recent_wins: deque = field(default_factory=lambda: deque(maxlen=50))
    _recent_losses: deque = field(default_factory=lambda: deque(maxlen=50))
    # Momentum: consecutive price direction counter
    _momentum: int = 0  # +N = N consecutive up ticks, -N = down
    _prev_mid: float = 0.0
    # Trailing stop
    _best_pnl: float = 0.0  # best unrealized PnL since entry

    @property
    def wr(self):
        return f"{self.wins/self.fills*100:.0f}%" if self.fills else "-"

    @property
    def vwap(self):
        return self.vwap_num / self.vwap_den if self.vwap_den > 0 else 0

    @property
    def kelly_fraction(self) -> float:
        """Half-Kelly fraction based on recent 50 trades. Returns 0.25-1.0 multiplier."""
        if len(self._recent_wins) + len(self._recent_losses) < 20:
            return 1.0  # not enough data, use full base size
        total = len(self._recent_wins) + len(self._recent_losses)
        p = len(self._recent_wins) / total
        if not self._recent_wins or not self._recent_losses:
            return 1.0
        avg_w = sum(self._recent_wins) / len(self._recent_wins)
        avg_l = sum(self._recent_losses) / len(self._recent_losses)
        if avg_l == 0:
            return 1.0
        b = avg_w / abs(avg_l)
        kelly = (p * b - (1 - p)) / b if b > 0 else 0
        # Half-Kelly for safety, clamped to [0.25, 1.0]
        return max(0.25, min(1.0, kelly * 0.5 + 0.5))


# Global state shared with dashboard
pair_states: dict[str, PairState] = {}
daily_pnl: float = 0.0
daily_fills: int = 0
daily_wins: int = 0
_halted = False
_last_reset_date: str = ""
_cumulative_pnl: float = 0.0
_cumulative_fills: int = 0


def _save_stats():
    """Persist cumulative stats to JSON."""
    data = {
        "cumulative_pnl": _cumulative_pnl + daily_pnl,
        "cumulative_fills": _cumulative_fills + daily_fills,
        "per_pair": {sym: {"pnl": ps.pnl, "fills": ps.fills, "wins": ps.wins}
                     for sym, ps in pair_states.items()},
    }
    try:
        with open(STATS_FILE, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def _load_stats():
    """Load cumulative stats from JSON on startup."""
    global _cumulative_pnl, _cumulative_fills
    try:
        with open(STATS_FILE) as f:
            data = json.load(f)
        _cumulative_pnl = data.get("cumulative_pnl", 0)
        _cumulative_fills = data.get("cumulative_fills", 0)
        log(f"Loaded stats: cumulative PnL=${_cumulative_pnl:.4f}, fills={_cumulative_fills}")
    except (FileNotFoundError, json.JSONDecodeError):
        pass


async def daily_reset_loop():
    """Reset daily stats at midnight TW time."""
    global daily_pnl, daily_fills, daily_wins, _halted, _last_reset_date, _cumulative_pnl, _cumulative_fills
    while True:
        now = datetime.now(TW_TZ)
        today = now.strftime("%Y-%m-%d")
        if now.hour == 0 and now.minute == 0 and _last_reset_date != today:
            _last_reset_date = today
            log(f"🔄 Daily reset — yesterday: fills={daily_fills} pnl=${daily_pnl:.4f}")
            _cumulative_pnl += daily_pnl
            _cumulative_fills += daily_fills
            _save_stats()
            daily_pnl = 0.0
            daily_fills = 0
            daily_wins = 0
            _halted = False
            for ps in pair_states.values():
                ps.consec_losses = 0
                ps.paused_until = 0.0
        await asyncio.sleep(30)


def _halt_check() -> bool:
    global _halted
    if daily_pnl <= -DAILY_LOSS_LIMIT:
        if not _halted:
            log(f"⛔ DAILY LOSS LIMIT hit: ${daily_pnl:.2f} <= -${DAILY_LOSS_LIMIT}")
            alert_kill(f"Daily loss ${daily_pnl:.2f} <= -${DAILY_LOSS_LIMIT}")
            _halted = True
        return True
    return False


def _calc_atr(prices: deque) -> float:
    """Simple ATR proxy from recent mid-price changes."""
    if len(prices) < 20:
        return 0.0
    recent = list(prices)[-20:]
    changes = [abs(recent[i] - recent[i-1]) for i in range(1, len(recent))]
    return sum(changes) / len(changes)


def _record(ps: PairState, profit: float):
    global daily_pnl, daily_fills, daily_wins
    ps.pnl += profit
    ps.fills += 1
    daily_pnl += profit
    daily_fills += 1
    # Kelly sizing: track win/loss amounts
    if profit > 0:
        ps._recent_wins.append(profit)
    else:
        ps._recent_losses.append(profit)
    # Adaptive OFI weight learning
    ps.ofi_tracker.record_outcome(profit > 0)
    if profit > 0:
        ps.wins += 1
        daily_wins += 1
        ps.consec_losses = 0
    else:
        ps.consec_losses += 1
        if ps.consec_losses >= CONSEC_LOSS_PAUSE:
            ps.paused_until = time.time() + PAUSE_SECONDS
            log(f"{ps.symbol} paused {PAUSE_SECONDS}s after {CONSEC_LOSS_PAUSE} consecutive losses")
            alert_trade(ps.symbol, "PAUSED", profit, daily_pnl)
    record_trade(profit)
    # Alert every 10th fill or on significant loss
    if daily_fills % 10 == 0 or profit < -0.01:
        alert_trade(ps.symbol, "FILL", profit, daily_pnl)


async def run_pair(symbol: str):
    """Run MM for one symbol using depth20 stream."""
    ps = PairState(symbol=symbol)
    pair_states[symbol] = ps
    url = f"wss://data-stream.binance.vision/ws/{symbol.lower()}@depth20@100ms"
    log(f"[{symbol}] Connecting depth20 stream...")

    async for ws in websockets.connect(url, ssl=True):
        try:
            async for raw in ws:
                if _halt_check():
                    await asyncio.sleep(60)
                    continue

                if time.time() < ps.paused_until:
                    continue

                msg = json.loads(raw)
                bids = msg.get("bids", [])
                asks = msg.get("asks", [])
                if not bids or not asks:
                    continue

                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                if best_bid <= 0 or best_ask <= 0:
                    continue

                mid = (best_bid + best_ask) / 2
                ps.mid_prices.append(mid)
                ps.last_spread_bps = (best_ask - best_bid) / mid * 10000

                # Track momentum (consecutive directional ticks)
                if ps._prev_mid > 0:
                    if mid > ps._prev_mid:
                        ps._momentum = max(1, ps._momentum + 1) if ps._momentum > 0 else 1
                    elif mid < ps._prev_mid:
                        ps._momentum = min(-1, ps._momentum - 1) if ps._momentum < 0 else -1
                    # equal = keep current momentum
                ps._prev_mid = mid

                # Multi-timeframe OFI (1s/5s/30s weighted) with depth-weighted volumes
                # Closer levels get exponentially more weight (decay=0.85)
                bid_vol = sum(float(b[1]) * float(b[0]) * (0.85 ** i) for i, b in enumerate(bids))
                ask_vol = sum(float(a[1]) * float(a[0]) * (0.85 ** i) for i, a in enumerate(asks))
                ps.last_ofi = ps.ofi_tracker.update(bid_vol, ask_vol)

                # Update VWAP anchor for mean-reversion
                tick_vol = bid_vol + ask_vol
                ps.vwap_num += mid * tick_vol
                ps.vwap_den += tick_vol

                # ATR volatility filter
                atr = _calc_atr(ps.mid_prices)
                ps.last_atr = atr
                if len(ps.mid_prices) >= 20:
                    # Pause if ATR spikes > 3x its recent average
                    older = list(ps.mid_prices)[-60:-20] if len(ps.mid_prices) > 60 else list(ps.mid_prices)[:20]
                    if older:
                        old_changes = [abs(older[i] - older[i-1]) for i in range(1, len(older))]
                        base_atr = sum(old_changes) / len(old_changes) if old_changes else atr
                        if base_atr > 0 and atr > base_atr * 3:
                            continue  # skip — volatility spike

                # Position sizing: inverse vol × Kelly fraction
                base_size = min(MAX_POS_USD * 0.2, CAPITAL * 0.01)
                if atr > 0 and mid > 0:
                    vol_ratio = (atr / mid) * 10000
                    size = base_size / max(1, vol_ratio / 5)
                    size = max(5, min(size, MAX_POS_USD * 0.3))
                else:
                    size = base_size
                size *= ps.kelly_fraction  # Kelly adjustment

                # OFI-biased entry with inventory skew (Avellaneda-Stoikov)
                # + Mean-reversion overlay: fade extended moves from VWAP
                ofi_threshold = 0.3
                inv_skew = (ps.position / MAX_POS_USD) * 0.2
                buy_thresh = ofi_threshold + inv_skew
                sell_thresh = -ofi_threshold + inv_skew

                # Mean-reversion signal: lower threshold when price extended from VWAP
                mr_signal = 0
                spread = best_ask - best_bid
                if ps.vwap > 0 and spread > 0:
                    dev = (mid - ps.vwap) / spread  # deviation in spread units
                    if dev < -1.5:  # price below VWAP — bullish reversion
                        mr_signal = 1
                        buy_thresh *= 0.6  # easier to enter long
                    elif dev > 1.5:  # price above VWAP — bearish reversion
                        mr_signal = -1
                        sell_thresh *= 0.6  # easier to enter short

                if ps.position == 0:
                    if ps.last_ofi > buy_thresh and ps._momentum >= 3:
                        ps.position = size
                        ps.entry_price = best_ask
                        log(f"[{symbol}] BUY ${size:.0f} @ {best_ask} OFI={ps.last_ofi:.2f} mom={ps._momentum}")
                    elif ps.last_ofi < sell_thresh and ps._momentum <= -3:
                        ps.position = -size
                        ps.entry_price = best_bid
                        log(f"[{symbol}] SELL ${size:.0f} @ {best_bid} OFI={ps.last_ofi:.2f} mom={ps._momentum}")

                elif ps.position > 0:
                    # ATR-based trailing stop + OFI flip
                    spread = best_ask - best_bid
                    pnl_per_unit = (best_bid - ps.entry_price)
                    unrealized = pnl_per_unit / ps.entry_price * ps.position
                    ps._best_pnl = max(ps._best_pnl, unrealized)
                    trail_dist = atr * 1.5 if atr > 0 else spread * 2
                    drawdown_from_peak = ps._best_pnl - unrealized

                    if ps._best_pnl > 0 and drawdown_from_peak > trail_dist / ps.entry_price * ps.position:
                        # Trailing stop hit — lock in profit
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] TRAIL_LONG +${profit:.4f} (peak ${ps._best_pnl:.4f} wr={ps.wr})")
                        ps.position = 0
                        ps._best_pnl = 0
                    elif pnl_per_unit <= -trail_dist:
                        # Hard stop at 1.5*ATR loss
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] STOP_LONG ${profit:.4f} (total ${ps.pnl:.4f})")
                        ps.position = 0
                        ps._best_pnl = 0
                    elif ps.last_ofi < sell_thresh:
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] FLIP_SHORT ${profit:.4f} OFI={ps.last_ofi:.2f}")
                        ps.position = -size
                        ps.entry_price = best_bid
                        ps._best_pnl = 0

                elif ps.position < 0:
                    # ATR-based trailing stop + OFI flip
                    spread = best_ask - best_bid
                    pnl_per_unit = (ps.entry_price - best_ask)
                    unrealized = pnl_per_unit / ps.entry_price * abs(ps.position)
                    ps._best_pnl = max(ps._best_pnl, unrealized)
                    trail_dist = atr * 1.5 if atr > 0 else spread * 2
                    drawdown_from_peak = ps._best_pnl - unrealized

                    if ps._best_pnl > 0 and drawdown_from_peak > trail_dist / ps.entry_price * abs(ps.position):
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] TRAIL_SHORT +${profit:.4f} (peak ${ps._best_pnl:.4f} wr={ps.wr})")
                        ps.position = 0
                        ps._best_pnl = 0
                    elif pnl_per_unit <= -trail_dist:
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] STOP_SHORT ${profit:.4f} (total ${ps.pnl:.4f})")
                        ps.position = 0
                        ps._best_pnl = 0
                    elif ps.last_ofi > buy_thresh:
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] FLIP_LONG ${profit:.4f} OFI={ps.last_ofi:.2f}")
                        ps.position = size
                        ps.entry_price = best_ask
                        ps._best_pnl = 0

        except websockets.ConnectionClosed:
            log(f"[{symbol}] Reconnecting...")
            await asyncio.sleep(2)


async def run():
    _load_stats()
    log(f"Multi-pair MM starting: {SYMBOLS}")
    log(f"  Max pos/pair: ${MAX_POS_USD}, Daily loss limit: -${DAILY_LOSS_LIMIT}, Capital: ${CAPITAL}")
    await asyncio.gather(*[run_pair(s) for s in SYMBOLS], daily_reset_loop())


if __name__ == "__main__":
    asyncio.run(run())
