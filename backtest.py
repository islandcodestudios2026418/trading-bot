"""
Backtesting engine — replays tick_recorder JSONL files through strategy signals.
Simulates OFI, regime, entries/exits with fee model. Reports PnL + Sharpe + drawdown.

Usage: python backtest.py [tick_data/ticks_20260624_1200.jsonl.gz]
"""
import gzip
import json
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field

from regime import RegimeDetector, TRENDING, RANGING, NEUTRAL

ENTRY_FEE_BPS = float(os.getenv("ENTRY_FEE_BPS", "5"))
EXIT_FEE_BPS = float(os.getenv("EXIT_FEE_BPS", "1"))
MAX_POS_USD = float(os.getenv("MAX_POS_USD", "100"))


@dataclass
class BTState:
    """Per-symbol backtesting state."""
    position: float = 0.0
    entry_price: float = 0.0
    entry_time: float = 0.0
    pnl: float = 0.0
    fills: int = 0
    wins: int = 0
    max_dd: float = 0.0
    peak_pnl: float = 0.0
    # Signals
    ofi_1s: float = 0.0
    ofi_5s: float = 0.0
    ofi_30s: float = 0.0
    last_ofi: float = 0.0
    mid_prices: deque = field(default_factory=lambda: deque(maxlen=300))
    regime: RegimeDetector = field(default_factory=RegimeDetector)
    # OBI (order book imbalance)
    obi_shallow: float = 0.0
    obi_deep: float = 0.0
    # VPIN toxicity
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    toxicity: float = 0.0
    # Chandelier exit
    highest_since_entry: float = 0.0
    lowest_since_entry: float = 999999.0
    # Tracking
    returns: list = field(default_factory=list)
    _ofi_stable: int = 0
    _last_ofi_sign: int = 0


def _calc_atr(prices: deque) -> float:
    if len(prices) < 20:
        return 0.0
    recent = list(prices)[-20:]
    changes = [abs(recent[i] - recent[i - 1]) for i in range(1, len(recent))]
    return sum(changes) / len(changes)


def _apply_fee(trade_value: float) -> float:
    return trade_value * (ENTRY_FEE_BPS + EXIT_FEE_BPS) / 10000


class Backtester:
    def __init__(self):
        self.states: dict[str, BTState] = {}
        self.total_pnl = 0.0
        self.total_fills = 0
        self.equity_curve: list[float] = []
        self._tick_count = 0

    def _get(self, symbol: str) -> BTState:
        if symbol not in self.states:
            self.states[symbol] = BTState()
        return self.states[symbol]

    def process_depth(self, symbol: str, bids: list, asks: list, ts_ms: int):
        """Process depth snapshot — compute OFI, OBI, regime, generate signals."""
        if not bids or not asks:
            return
        st = self._get(symbol)
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        if best_bid <= 0 or best_ask <= 0:
            return
        mid = (best_bid + best_ask) / 2
        st.mid_prices.append(mid)
        spread_bps = (best_ask - best_bid) / mid * 10000

        # Multi-TF OFI
        bid_vol = sum(float(b[1]) * float(b[0]) * (0.85 ** i) for i, b in enumerate(bids))
        ask_vol = sum(float(a[1]) * float(a[0]) * (0.85 ** i) for i, a in enumerate(asks))
        total = bid_vol + ask_vol
        raw_ofi = (bid_vol - ask_vol) / total if total > 0 else 0
        st.ofi_1s += 0.07 * (raw_ofi - st.ofi_1s)
        st.ofi_5s += 0.014 * (raw_ofi - st.ofi_5s)
        st.ofi_30s += 0.002 * (raw_ofi - st.ofi_30s)
        st.last_ofi = 0.5 * st.ofi_1s + 0.3 * st.ofi_5s + 0.2 * st.ofi_30s

        # OBI: shallow vs deep imbalance
        shallow_bid = sum(float(b[1]) * float(b[0]) for b in bids[:5])
        shallow_ask = sum(float(a[1]) * float(a[0]) for a in asks[:5])
        st.obi_shallow = (shallow_bid - shallow_ask) / (shallow_bid + shallow_ask) if (shallow_bid + shallow_ask) > 0 else 0
        if len(bids) > 5 and len(asks) > 5:
            deep_bid = sum(float(b[1]) * float(b[0]) for b in bids[5:])
            deep_ask = sum(float(a[1]) * float(a[0]) for a in asks[5:])
            st.obi_deep = (deep_bid - deep_ask) / (deep_bid + deep_ask) if (deep_bid + deep_ask) > 0 else 0

        # Regime
        st.regime.update(mid)

        # OFI stability
        curr_sign = 1 if st.last_ofi > 0.1 else (-1 if st.last_ofi < -0.1 else 0)
        if curr_sign == st._last_ofi_sign and curr_sign != 0:
            st._ofi_stable += 1
        else:
            st._ofi_stable = 0
        st._last_ofi_sign = curr_sign

        # ATR
        atr = _calc_atr(st.mid_prices)
        if atr == 0:
            return

        # Skip low-spread (no edge)
        if spread_bps < 1.0:
            return

        # Entry thresholds (regime-adaptive)
        buy_thresh, sell_thresh = st.regime.adapt_thresholds(0.3, -0.3)

        # OBI confirmation: require OBI alignment
        if st.obi_shallow > 0.1:
            buy_thresh *= 0.85
        elif st.obi_shallow < -0.1:
            sell_thresh *= 0.85

        # Toxicity confirmation
        if st.toxicity > 0.3:
            buy_thresh *= 0.8
        elif st.toxicity < -0.3:
            sell_thresh *= 0.8

        # Entry logic
        if st.position == 0 and st._ofi_stable >= 2:
            size = min(MAX_POS_USD * 0.2, 20)
            if st.last_ofi > buy_thresh:
                st.position = size
                st.entry_price = best_ask
                st.entry_time = ts_ms
                st.highest_since_entry = mid
                st.lowest_since_entry = mid
            elif st.last_ofi < sell_thresh:
                st.position = -size
                st.entry_price = best_bid
                st.entry_time = ts_ms
                st.highest_since_entry = mid
                st.lowest_since_entry = mid

        # Exit logic: chandelier exit
        elif st.position != 0:
            st.highest_since_entry = max(st.highest_since_entry, mid)
            st.lowest_since_entry = min(st.lowest_since_entry, mid)
            atr_mult = st.regime.adapt_exit(1.5)
            exit_signal = False

            if st.position > 0:
                # Chandelier: trail from highest high
                chandelier_stop = st.highest_since_entry - atr * atr_mult
                unrealized = (best_bid - st.entry_price) / st.entry_price * st.position
                if mid <= chandelier_stop or (ts_ms - st.entry_time > 60000 and unrealized <= 0):
                    exit_signal = True
                    profit = unrealized
            else:
                # Chandelier: trail from lowest low
                chandelier_stop = st.lowest_since_entry + atr * atr_mult
                unrealized = (st.entry_price - best_ask) / st.entry_price * abs(st.position)
                if mid >= chandelier_stop or (ts_ms - st.entry_time > 60000 and unrealized <= 0):
                    exit_signal = True
                    profit = unrealized

            if exit_signal:
                fee = _apply_fee(abs(st.position))
                net = profit - fee
                st.pnl += net
                st.fills += 1
                if net > 0:
                    st.wins += 1
                st.returns.append(net)
                self.total_pnl += net
                self.total_fills += 1
                self.equity_curve.append(self.total_pnl)
                # Drawdown tracking
                st.peak_pnl = max(st.peak_pnl, st.pnl)
                st.max_dd = min(st.max_dd, st.pnl - st.peak_pnl)
                # Reset
                st.position = 0
                st.highest_since_entry = 0
                st.lowest_since_entry = 999999

    def process_trade(self, symbol: str, price: float, qty: float, is_buyer_maker: bool):
        """Process aggTrade — update VPIN toxicity."""
        st = self._get(symbol)
        qty_usd = price * qty
        alpha = 0.01
        if is_buyer_maker:
            st.sell_vol += alpha * (qty_usd - st.sell_vol)
        else:
            st.buy_vol += alpha * (qty_usd - st.buy_vol)
        total = st.buy_vol + st.sell_vol
        st.toxicity = (st.buy_vol - st.sell_vol) / total if total > 0 else 0

    def report(self) -> dict:
        """Generate backtest report."""
        if not self.equity_curve:
            return {"error": "no trades"}
        # Sharpe
        rets = []
        for i in range(1, len(self.equity_curve)):
            rets.append(self.equity_curve[i] - self.equity_curve[i - 1])
        mean_r = sum(rets) / len(rets) if rets else 0
        std_r = (sum((r - mean_r) ** 2 for r in rets) / len(rets)) ** 0.5 if rets else 1
        sharpe = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else 0
        # Max drawdown
        peak = 0.0
        max_dd = 0.0
        for eq in self.equity_curve:
            peak = max(peak, eq)
            max_dd = min(max_dd, eq - peak)
        win_rate = sum(1 for s in self.states.values() for r in s.returns if r > 0) / max(1, self.total_fills)
        return {
            "total_pnl": round(self.total_pnl, 4),
            "total_fills": self.total_fills,
            "win_rate": f"{win_rate * 100:.1f}%",
            "sharpe": round(sharpe, 2),
            "max_drawdown": round(max_dd, 4),
            "per_symbol": {
                sym: {"pnl": round(s.pnl, 4), "fills": s.fills, "wr": f"{s.wins / s.fills * 100:.0f}%" if s.fills else "-"}
                for sym, s in self.states.items()
            },
        }


def run_backtest(filepath: str) -> dict:
    """Run backtest on a tick data file."""
    bt = Backtester()
    opener = gzip.open if filepath.endswith(".gz") else open
    with opener(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            sym = rec.get("s", "")
            ts = rec.get("t", 0)
            if rec["type"] == "depth":
                bt.process_depth(sym, rec.get("b", []), rec.get("a", []), ts)
            elif rec["type"] == "trade":
                bt.process_trade(sym, float(rec.get("p", 0)), float(rec.get("q", 0)), rec.get("m", False))
    return bt.report()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Find most recent tick file
        data_dir = os.getenv("TICK_DATA_DIR", "tick_data")
        if os.path.isdir(data_dir):
            files = sorted(f for f in os.listdir(data_dir) if f.startswith("ticks_"))
            if files:
                filepath = os.path.join(data_dir, files[-1])
            else:
                print("No tick files found. Run with RECORD_TICKS=1 first.")
                sys.exit(1)
        else:
            print(f"Usage: python backtest.py <tick_file.jsonl.gz>")
            sys.exit(1)
    else:
        filepath = sys.argv[1]

    print(f"Running backtest on: {filepath}")
    t0 = time.time()
    result = run_backtest(filepath)
    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"BACKTEST RESULTS ({elapsed:.1f}s)")
    print(f"{'='*50}")
    for k, v in result.items():
        if k == "per_symbol":
            print(f"\nPer-symbol:")
            for sym, stats in v.items():
                print(f"  {sym}: PnL=${stats['pnl']}, fills={stats['fills']}, WR={stats['wr']}")
        else:
            print(f"  {k}: {v}")
