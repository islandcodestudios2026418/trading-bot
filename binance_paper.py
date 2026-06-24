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

from regime import RegimeDetector

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


def _get_session() -> str:
    """Detect trading session based on UTC hour."""
    h = datetime.now(timezone.utc).hour
    if 0 <= h < 8:
        return "asian"   # ranging, lower vol
    elif 8 <= h < 13:
        return "europe"  # transition, moderate
    else:
        return "us"      # trending, higher vol

SESSION_PARAMS = {
    "asian":  {"threshold_mult": 1.2, "size_mult": 0.7, "mom_req": 4},  # stricter, smaller
    "europe": {"threshold_mult": 1.0, "size_mult": 1.0, "mom_req": 3},  # baseline
    "us":     {"threshold_mult": 0.8, "size_mult": 1.3, "mom_req": 2},  # easier entry, bigger size
}

# Cross-pair correlation: shared OFI state
_pair_ofi: dict[str, float] = {}  # symbol → last OFI
_current_session: str = "europe"


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
    # Adaptive weights
    w1: float = 0.5
    w5: float = 0.3
    w30: float = 0.2
    _wins: list = field(default_factory=lambda: [0, 0, 0])
    _losses: list = field(default_factory=lambda: [0, 0, 0])
    _rebalance_count: int = 0
    # Trade flow toxicity (VPIN-lite)
    _buy_vol: float = 0.0
    _sell_vol: float = 0.0
    _flow_alpha: float = 1 - 0.99  # ~100 trade half-life
    toxicity: float = 0.0  # [-1, +1]: +1 = all buys, -1 = all sells
    # Institutional flow detection (iceberg/order splitting)
    _recent_trades: deque = field(default_factory=lambda: deque(maxlen=50))
    institutional_flow: float = 0.0  # 0-1 score, >0.5 = likely institutional
    # Depth pressure gradient
    depth_pressure: float = 0.0  # >0 = bid wall (support), <0 = ask wall (resistance)
    # Quote stuffing/spoofing detection
    _bid_changes: deque = field(default_factory=lambda: deque(maxlen=30))  # recent bid top changes
    _ask_changes: deque = field(default_factory=lambda: deque(maxlen=30))
    _last_bid_top: float = 0.0
    _last_ask_top: float = 0.0
    spoof_score: float = 0.0  # 0-1, >0.5 = likely spoofing detected

    def update(self, bid_vol: float, ask_vol: float) -> float:
        """Update with new depth snapshot. Returns weighted composite OFI."""
        total = bid_vol + ask_vol
        raw_ofi = (bid_vol - ask_vol) / total if total > 0 else 0
        self.ofi_1s += self._alpha_1s * (raw_ofi - self.ofi_1s)
        self.ofi_5s += self._alpha_5s * (raw_ofi - self.ofi_5s)
        self.ofi_30s += self._alpha_30s * (raw_ofi - self.ofi_30s)
        return self.w1 * self.ofi_1s + self.w5 * self.ofi_5s + self.w30 * self.ofi_30s

    def update_depth_gradient(self, bids: list, asks: list):
        """Compute depth pressure gradient from full orderbook.
        Compares shallow (levels 1-5) vs deep (6-20) liquidity imbalance.
        Positive = bid wall support, Negative = ask wall resistance."""
        if len(bids) < 10 or len(asks) < 10:
            return
        # Shallow: levels 0-4, Deep: levels 5-19
        shallow_bid = sum(float(b[1]) * float(b[0]) for b in bids[:5])
        deep_bid = sum(float(b[1]) * float(b[0]) for b in bids[5:])
        shallow_ask = sum(float(a[1]) * float(a[0]) for a in asks[:5])
        deep_ask = sum(float(a[1]) * float(a[0]) for a in asks[5:])
        # Gradient: deep support vs deep resistance
        total_deep = deep_bid + deep_ask
        if total_deep > 0:
            raw = (deep_bid - deep_ask) / total_deep
            self.depth_pressure += 0.1 * (raw - self.depth_pressure)  # smooth

        # Spoofing detection: rapid top-of-book changes without execution
        import time
        now = time.time()
        bid_top = float(bids[0][0])
        ask_top = float(asks[0][0])
        if self._last_bid_top > 0 and bid_top != self._last_bid_top:
            self._bid_changes.append(now)
        if self._last_ask_top > 0 and ask_top != self._last_ask_top:
            self._ask_changes.append(now)
        self._last_bid_top = bid_top
        self._last_ask_top = ask_top

        # Count changes in last 2 seconds — >10 changes = suspicious
        cutoff = now - 2.0
        bid_chg = sum(1 for t in self._bid_changes if t >= cutoff)
        ask_chg = sum(1 for t in self._ask_changes if t >= cutoff)
        max_chg = max(bid_chg, ask_chg)
        if max_chg > 10:
            self.spoof_score = min(1.0, self.spoof_score + 0.3)
        else:
            self.spoof_score *= 0.9  # decay

    def update_trade(self, qty_usd: float, is_buyer_maker: bool, ts_ms: float = 0):
        """Update trade flow from aggTrade stream + detect institutional splitting."""
        if is_buyer_maker:
            self._sell_vol += self._flow_alpha * (qty_usd - self._sell_vol)
        else:
            self._buy_vol += self._flow_alpha * (qty_usd - self._buy_vol)
        total = self._buy_vol + self._sell_vol
        self.toxicity = (self._buy_vol - self._sell_vol) / total if total > 0 else 0

        # Institutional detection: many similar-sized trades in short window
        import time
        now = ts_ms or time.time() * 1000
        self._recent_trades.append((now, qty_usd, is_buyer_maker))
        # Look at last 500ms window
        cutoff = now - 500
        window = [(t, q, s) for t, q, s in self._recent_trades if t >= cutoff]
        if len(window) >= 5:
            # Check if trades are similar size (CV < 0.3) and same direction
            sizes = [q for _, q, _ in window]
            directions = [s for _, _, s in window]
            mean_sz = sum(sizes) / len(sizes)
            if mean_sz > 0:
                std_sz = (sum((s - mean_sz) ** 2 for s in sizes) / len(sizes)) ** 0.5
                cv = std_sz / mean_sz
                same_dir = sum(1 for d in directions if d == directions[0]) / len(directions)
                # Low CV (similar sizes) + same direction = institutional
                if cv < 0.3 and same_dir > 0.8:
                    self.institutional_flow = min(1.0, self.institutional_flow + 0.2)
                else:
                    self.institutional_flow *= 0.95
            else:
                self.institutional_flow *= 0.95
        else:
            self.institutional_flow *= 0.98

    def dominant_tf(self) -> int:
        vals = [abs(self.ofi_1s), abs(self.ofi_5s), abs(self.ofi_30s)]
        return vals.index(max(vals))

    def record_outcome(self, win: bool):
        tf = self.dominant_tf()
        if win:
            self._wins[tf] += 1
        else:
            self._losses[tf] += 1
        self._rebalance_count += 1
        if self._rebalance_count >= 50:
            self._rebalance()

    def _rebalance(self):
        self._rebalance_count = 0
        rates = []
        for i in range(3):
            total = self._wins[i] + self._losses[i]
            rates.append(self._wins[i] / total if total >= 5 else 0.5)
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
    _scaled_out: bool = False  # True after first partial take-profit
    _entry_time: float = 0.0  # time.time() of position entry
    # Regime detection
    regime: RegimeDetector = field(default_factory=RegimeDetector)

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
    # Regime analytics
    ps.regime.record_fill(profit)
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
                ps.ofi_tracker.update_depth_gradient(bids, asks)

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

                # Regime detection: adapt thresholds
                ps.regime.update(mid)
                buy_thresh, sell_thresh = ps.regime.adapt_thresholds(buy_thresh, sell_thresh)

                # Session adaptation: Asian=ranging(strict), US=trending(easy)
                global _current_session
                _current_session = _get_session()
                sp = SESSION_PARAMS[_current_session]
                buy_thresh *= sp["threshold_mult"]
                sell_thresh *= sp["threshold_mult"]

                # Cross-pair correlation: BTC+ETH agreement boosts signal
                _pair_ofi[symbol] = ps.last_ofi
                btc_ofi = _pair_ofi.get("BTCUSDT", 0)
                eth_ofi = _pair_ofi.get("ETHUSDT", 0)
                if btc_ofi != 0 and eth_ofi != 0 and symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
                    # Both agree on direction = stronger signal
                    if btc_ofi > 0.1 and eth_ofi > 0.1:
                        buy_thresh *= 0.7  # correlated bullish → easier long
                    elif btc_ofi < -0.1 and eth_ofi < -0.1:
                        sell_thresh *= 0.7  # correlated bearish → easier short

                # Spread regime detection: tight (<5bp) → stricter, wide (>15bp) → relax
                if ps.last_spread_bps < 5:
                    buy_thresh *= 1.3   # tight spread = less opportunity, require stronger signal
                    sell_thresh *= 1.3
                elif ps.last_spread_bps > 15:
                    buy_thresh *= 0.7   # wide spread = more edge per trade, relax
                    sell_thresh *= 0.7

                # Mean-reversion signal: lower threshold when price extended from VWAP
                mr_signal = 0
                spread = best_ask - best_bid
                if ps.vwap > 0 and spread > 0:
                    dev = (mid - ps.vwap) / spread
                    if dev < -1.5:
                        mr_signal = 1
                        buy_thresh *= 0.6
                    elif dev > 1.5:
                        mr_signal = -1
                        sell_thresh *= 0.6

                # Trade flow toxicity: confirm with aggressive order flow
                tox = ps.ofi_tracker.toxicity
                if tox > 0.3:  # buyers aggressive — easier to go long
                    buy_thresh *= 0.8
                elif tox < -0.3:  # sellers aggressive — easier to go short
                    sell_thresh *= 0.8

                # Institutional flow: if detected, strongly weight OFI in that direction
                inst_flow = ps.ofi_tracker.institutional_flow
                if inst_flow > 0.5:
                    if tox > 0:
                        buy_thresh *= 0.5  # institutional buying → much easier long entry
                    elif tox < 0:
                        sell_thresh *= 0.5  # institutional selling → much easier short entry

                # Spread-adaptive: no edge if spread is too tight
                if ps.last_spread_bps < 1.0:
                    continue  # sub-1bp spread = no edge, skip tick

                # Depth pressure gradient: bid wall = easier long, ask wall = easier short
                dp = ps.ofi_tracker.depth_pressure
                if dp > 0.2:   # strong bid wall support
                    buy_thresh *= 0.8
                elif dp < -0.2:  # strong ask wall resistance
                    sell_thresh *= 0.8

                # Spoofing detection: if detected, increase threshold (less reliable book)
                if ps.ofi_tracker.spoof_score > 0.5:
                    buy_thresh *= 1.5
                    sell_thresh *= 1.5

                if ps.position == 0:
                    # Momentum confirmation: price ticks + aggTrade direction must align
                    tox_confirms_buy = tox > -0.1  # not actively selling
                    tox_confirms_sell = tox < 0.1  # not actively buying
                    mom_req = sp["mom_req"]  # session-adaptive momentum requirement
                    if ps.last_ofi > buy_thresh and ps._momentum >= mom_req and tox_confirms_buy:
                        # Scale size by spread width (wider spread = more edge = bigger size)
                        spread_mult = min(2.0, max(0.5, ps.last_spread_bps / 5.0))
                        adj_size = size * spread_mult * sp["size_mult"]
                        ps.position = adj_size
                        ps.entry_price = best_ask
                        ps._entry_time = time.time()
                        log(f"[{symbol}] BUY ${adj_size:.0f} @ {best_ask} OFI={ps.last_ofi:.2f} mom={ps._momentum} reg={ps.regime.regime[0]}")
                    elif ps.last_ofi < sell_thresh and ps._momentum <= -mom_req and tox_confirms_sell:
                        spread_mult = min(2.0, max(0.5, ps.last_spread_bps / 5.0))
                        adj_size = size * spread_mult * sp["size_mult"]
                        ps.position = -adj_size
                        ps.entry_price = best_bid
                        ps._entry_time = time.time()
                        log(f"[{symbol}] SELL ${adj_size:.0f} @ {best_bid} OFI={ps.last_ofi:.2f} mom={ps._momentum} reg={ps.regime.regime[0]}")

                elif ps.position > 0:
                    # Position aging: force exit after 60s if not profitable
                    age = time.time() - ps._entry_time if ps._entry_time > 0 else 0
                    pnl_per_unit = (best_bid - ps.entry_price)
                    unrealized = pnl_per_unit / ps.entry_price * ps.position
                    if age > 60 and unrealized <= 0:
                        _record(ps, unrealized)
                        log(f"[{symbol}] AGE_EXIT_LONG ${unrealized:.4f} ({age:.0f}s held)")
                        ps.position = 0
                        ps._best_pnl = 0
                        ps._scaled_out = False
                        ps._entry_time = 0
                        continue
                    # ATR-based trailing stop + OFI flip (regime-adaptive)
                    spread = best_ask - best_bid
                    ps._best_pnl = max(ps._best_pnl, unrealized)
                    atr_mult = ps.regime.adapt_exit(1.5)
                    trail_dist = atr * atr_mult if atr > 0 else spread * 2
                    drawdown_from_peak = ps._best_pnl - unrealized

                    # Partial profit-taking: close 50% at 1x ATR profit
                    if not ps._scaled_out and atr > 0 and pnl_per_unit >= atr:
                        half = ps.position * 0.5
                        partial_profit = pnl_per_unit / ps.entry_price * half
                        _record(ps, partial_profit)
                        ps.position -= half
                        ps._scaled_out = True
                        log(f"[{symbol}] SCALE_LONG 50% +${partial_profit:.4f} (remain ${ps.position:.0f})")
                    elif ps._best_pnl > 0 and drawdown_from_peak > trail_dist / ps.entry_price * ps.position:
                        # Trailing stop hit — lock in profit
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] TRAIL_LONG +${profit:.4f} (peak ${ps._best_pnl:.4f} wr={ps.wr})")
                        ps.position = 0
                        ps._best_pnl = 0
                        ps._scaled_out = False
                    elif pnl_per_unit <= -trail_dist:
                        # Hard stop at 1.5*ATR loss
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] STOP_LONG ${profit:.4f} (total ${ps.pnl:.4f})")
                        ps.position = 0
                        ps._best_pnl = 0
                        ps._scaled_out = False
                    elif ps.last_ofi < sell_thresh:
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] FLIP_SHORT ${profit:.4f} OFI={ps.last_ofi:.2f}")
                        ps.position = -size
                        ps.entry_price = best_bid
                        ps._best_pnl = 0
                        ps._scaled_out = False

                elif ps.position < 0:
                    # Position aging: force exit after 60s if not profitable
                    age = time.time() - ps._entry_time if ps._entry_time > 0 else 0
                    pnl_per_unit = (ps.entry_price - best_ask)
                    unrealized = pnl_per_unit / ps.entry_price * abs(ps.position)
                    if age > 60 and unrealized <= 0:
                        _record(ps, unrealized)
                        log(f"[{symbol}] AGE_EXIT_SHORT ${unrealized:.4f} ({age:.0f}s held)")
                        ps.position = 0
                        ps._best_pnl = 0
                        ps._scaled_out = False
                        ps._entry_time = 0
                        continue
                    # ATR-based trailing stop + OFI flip (regime-adaptive)
                    spread = best_ask - best_bid
                    ps._best_pnl = max(ps._best_pnl, unrealized)
                    atr_mult = ps.regime.adapt_exit(1.5)
                    trail_dist = atr * atr_mult if atr > 0 else spread * 2
                    drawdown_from_peak = ps._best_pnl - unrealized

                    # Partial profit-taking: close 50% at 1x ATR profit
                    if not ps._scaled_out and atr > 0 and pnl_per_unit >= atr:
                        half = abs(ps.position) * 0.5
                        partial_profit = pnl_per_unit / ps.entry_price * half
                        _record(ps, partial_profit)
                        ps.position += half  # reduce short
                        ps._scaled_out = True
                        log(f"[{symbol}] SCALE_SHORT 50% +${partial_profit:.4f} (remain ${ps.position:.0f})")
                    elif ps._best_pnl > 0 and drawdown_from_peak > trail_dist / ps.entry_price * abs(ps.position):
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] TRAIL_SHORT +${profit:.4f} (peak ${ps._best_pnl:.4f} wr={ps.wr})")
                        ps.position = 0
                        ps._best_pnl = 0
                        ps._scaled_out = False
                    elif pnl_per_unit <= -trail_dist:
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] STOP_SHORT ${profit:.4f} (total ${ps.pnl:.4f})")
                        ps.position = 0
                        ps._best_pnl = 0
                        ps._scaled_out = False
                    elif ps.last_ofi > buy_thresh:
                        profit = unrealized
                        _record(ps, profit)
                        log(f"[{symbol}] FLIP_LONG ${profit:.4f} OFI={ps.last_ofi:.2f}")
                        ps.position = size
                        ps.entry_price = best_ask
                        ps._best_pnl = 0
                        ps._scaled_out = False

        except websockets.ConnectionClosed:
            log(f"[{symbol}] Reconnecting...")
            await asyncio.sleep(2)


async def _aggtrade_stream():
    """Stream aggTrades for all symbols to track trade flow toxicity."""
    streams = "/".join(f"{s.lower()}@aggTrade" for s in SYMBOLS)
    url = f"wss://data-stream.binance.vision/stream?streams={streams}"
    async for ws in websockets.connect(url, ssl=True):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                data = msg.get("data", {})
                symbol = data.get("s", "")
                ps = pair_states.get(symbol)
                if not ps:
                    continue
                px = float(data.get("p", 0))
                qty = float(data.get("q", 0))
                is_buyer_maker = data.get("m", False)
                ps.ofi_tracker.update_trade(px * qty, is_buyer_maker, float(data.get("T", 0)))
        except websockets.ConnectionClosed:
            await asyncio.sleep(2)


async def run():
    _load_stats()
    log(f"Multi-pair MM starting: {SYMBOLS}")
    log(f"  Max pos/pair: ${MAX_POS_USD}, Daily loss limit: -${DAILY_LOSS_LIMIT}, Capital: ${CAPITAL}")
    await asyncio.gather(*[run_pair(s) for s in SYMBOLS], _aggtrade_stream(), daily_reset_loop())


if __name__ == "__main__":
    asyncio.run(run())
