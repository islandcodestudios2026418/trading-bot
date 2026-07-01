"""
ML-based regime classification — online ensemble classifier.
Replaces fixed VR+Hurst thresholds with an adaptive model that learns from market data.

Features: [VR, Hurst, spread_bps, arrival_intensity, OFI_momentum, volatility, autocorrelation]
Labels: self-supervised from forward-looking price behavior.

No numpy/pandas/sklearn — pure Python, production-lean.
"""
import math
import time
from collections import deque
from dataclasses import dataclass, field

# Regime constants (same as regime.py for compatibility)
TRENDING = "trending"
RANGING = "ranging"
NEUTRAL = "neutral"

# Feature indices
F_VR = 0         # variance ratio
F_HURST = 1      # Hurst exponent
F_SPREAD = 2     # spread in bps (EMA)
F_ARRIVAL = 3    # trade arrival intensity
F_OFI_MOM = 4   # OFI momentum (rate of change of composite OFI)
F_VOL = 5        # realized volatility (bps/tick EMA)
F_AUTOCORR = 6   # lag-1 return autocorrelation
N_FEATURES = 7


@dataclass
class OnlineTree:
    """Single decision stump — splits on one feature at a threshold.
    Lightweight online learner that picks the best split greedily.
    """
    feature_idx: int = 0
    threshold: float = 0.0
    left_pred: list = field(default_factory=lambda: [0.0, 0.0, 0.0])   # [trending, ranging, neutral] probabilities
    right_pred: list = field(default_factory=lambda: [0.0, 0.0, 0.0])
    left_count: int = 0
    right_count: int = 0
    weight: float = 1.0  # ensemble weight (boosting)

    def predict(self, features: list) -> list:
        """Return class probabilities [trending, ranging, neutral]."""
        if features[self.feature_idx] <= self.threshold:
            return self.left_pred
        return self.right_pred

    def update(self, features: list, label_idx: int, lr: float = 0.01):
        """Online update: adjust leaf prediction toward true label."""
        target = [0.0, 0.0, 0.0]
        target[label_idx] = 1.0
        if features[self.feature_idx] <= self.threshold:
            self.left_count += 1
            for i in range(3):
                self.left_pred[i] += lr * (target[i] - self.left_pred[i])
        else:
            self.right_count += 1
            for i in range(3):
                self.right_pred[i] += lr * (target[i] - self.right_pred[i])


@dataclass
class RegimeMLDetector:
    """Online ensemble regime detector.

    Architecture:
    - 10 decision stumps (random feature + threshold), online-updated
    - Self-supervised labeling from forward price path (next 50 ticks)
    - Exponential decay on old predictions (concept drift adaptation)
    - Falls back to VR+Hurst during warmup (<200 samples)
    """
    window: int = 120
    n_trees: int = 10
    warmup: int = 200  # samples before ML takes over
    label_horizon: int = 50  # ticks forward for self-labeling

    # Internal state
    _returns: deque = field(default_factory=lambda: deque(maxlen=500))
    _mids: deque = field(default_factory=lambda: deque(maxlen=500))
    _prev_mid: float = 0.0
    _sample_count: int = 0
    _trees: list = field(default_factory=list)
    _pending_labels: deque = field(default_factory=lambda: deque(maxlen=200))
    # ^ Each entry: (tick_count_at_entry, features_at_entry, mid_at_entry)

    # Public outputs (same interface as RegimeDetector)
    regime: str = NEUTRAL
    vr: float = 1.0
    hurst: float = 0.5
    confidence: float = 0.0  # 0-1, how confident the ML model is
    regime_fills: dict = field(default_factory=lambda: {TRENDING: 0, RANGING: 0, NEUTRAL: 0})
    regime_pnl: dict = field(default_factory=lambda: {TRENDING: 0.0, RANGING: 0.0, NEUTRAL: 0.0})

    # Feature tracking
    _spread_ema: float = 0.0
    _vol_ema: float = 0.0
    _ofi_prev: float = 0.0
    _ofi_momentum: float = 0.0
    _arrival_rate: float = 1.0
    _autocorr: float = 0.0
    _tick_count: int = 0

    # Accuracy tracking
    _correct: int = 0
    _total_preds: int = 0
    accuracy: float = 0.0

    def __post_init__(self):
        """Initialize ensemble with diverse random stumps."""
        import random
        random.seed(42)
        self._trees = []
        for _ in range(self.n_trees):
            feat = random.randint(0, N_FEATURES - 1)
            # Initial thresholds: centered at reasonable defaults
            defaults = [1.0, 0.5, 5.0, 1.5, 0.0, 5.0, 0.0]
            noise = random.gauss(0, 0.3)
            tree = OnlineTree(
                feature_idx=feat,
                threshold=defaults[feat] * (1 + noise),
                left_pred=[0.33, 0.34, 0.33],
                right_pred=[0.33, 0.33, 0.34],
                weight=1.0 / self.n_trees
            )
            self._trees.append(tree)

    def update(self, mid: float, spread_bps: float = 0.0,
               arrival_intensity: float = 1.0, ofi_composite: float = 0.0) -> str:
        """Update with new tick. Returns current regime.
        Accepts extra features from the calling context for richer classification.
        """
        self._tick_count += 1

        if self._prev_mid > 0 and mid > 0:
            ret = (mid - self._prev_mid) / self._prev_mid
            self._returns.append(ret)
        self._prev_mid = mid
        self._mids.append(mid)

        # Update feature EMAs
        self._spread_ema += 0.02 * (spread_bps - self._spread_ema)
        self._arrival_rate += 0.05 * (arrival_intensity - self._arrival_rate)
        ofi_delta = ofi_composite - self._ofi_prev
        self._ofi_momentum += 0.1 * (ofi_delta - self._ofi_momentum)
        self._ofi_prev = ofi_composite

        # Realized vol
        if len(self._returns) >= 2:
            last_ret = abs(self._returns[-1]) * 10000  # bps
            self._vol_ema += 0.03 * (last_ret - self._vol_ema)

        # Autocorrelation (lag-1)
        if len(self._returns) >= 20:
            self._autocorr = self._calc_autocorr()

        # Compute VR + Hurst (always, for /metrics and fallback)
        if len(self._returns) >= self.window:
            self.vr = self._calc_vr()
            self.hurst = self._calc_hurst()

        # Build feature vector
        features = [
            self.vr,
            self.hurst,
            self._spread_ema,
            self._arrival_rate,
            self._ofi_momentum,
            self._vol_ema,
            self._autocorr,
        ]

        # Store for self-labeling
        self._pending_labels.append((self._tick_count, features[:], mid))

        # Process resolved labels (ticks that are old enough to label)
        self._resolve_labels()

        # Classification
        if self._sample_count < self.warmup:
            # Fallback: VR + Hurst heuristic during warmup
            self.regime = self._heuristic_regime()
            self.confidence = 0.3
        else:
            # ML ensemble prediction
            probs = [0.0, 0.0, 0.0]  # trending, ranging, neutral
            total_weight = 0.0
            for tree in self._trees:
                pred = tree.predict(features)
                for i in range(3):
                    probs[i] += pred[i] * tree.weight
                total_weight += tree.weight
            if total_weight > 0:
                probs = [p / total_weight for p in probs]

            # Pick class with highest probability
            max_idx = probs.index(max(probs))
            labels = [TRENDING, RANGING, NEUTRAL]
            self.regime = labels[max_idx]
            self.confidence = probs[max_idx]

            # If confidence too low, stay neutral (conservative)
            if self.confidence < 0.45:
                self.regime = NEUTRAL

        return self.regime

    def _resolve_labels(self):
        """Self-supervised labeling: look at price path after N ticks to determine regime."""
        while self._pending_labels:
            tick_at, features, mid_at = self._pending_labels[0]
            elapsed = self._tick_count - tick_at
            if elapsed < self.label_horizon:
                break  # not enough future data yet
            self._pending_labels.popleft()

            # Determine true regime from forward path
            # Find all mids from tick_at to tick_at + label_horizon
            start_idx = len(self._mids) - (self._tick_count - tick_at)
            end_idx = start_idx + self.label_horizon
            if start_idx < 0 or end_idx > len(self._mids):
                continue

            future_mids = list(self._mids)[start_idx:end_idx]
            if len(future_mids) < 20:
                continue

            label = self._label_from_path(mid_at, future_mids)
            label_idx = {TRENDING: 0, RANGING: 1, NEUTRAL: 2}[label]

            # Update all trees
            for tree in self._trees:
                # Boosting: increase weight if tree was wrong
                pred = tree.predict(features)
                pred_idx = pred.index(max(pred))
                if pred_idx == label_idx:
                    tree.weight = min(3.0, tree.weight * 1.01)
                else:
                    tree.weight = max(0.1, tree.weight * 0.99)
                tree.update(features, label_idx, lr=0.02)

            # Check ensemble prediction accuracy (majority vote)
            ensemble_probs = [0.0, 0.0, 0.0]
            for tree in self._trees:
                pred = tree.predict(features)
                for i in range(3):
                    ensemble_probs[i] += pred[i] * tree.weight
            ensemble_pred = ensemble_probs.index(max(ensemble_probs))
            if ensemble_pred == label_idx:
                self._correct += 1

            self._sample_count += 1
            self._total_preds += 1
            if self._total_preds > 0:
                self.accuracy = self._correct / self._total_preds

            # Periodically mutate weakest tree (evolutionary pressure)
            if self._sample_count % 100 == 0:
                self._mutate_weakest()

    def _label_from_path(self, start_mid: float, future_mids: list) -> str:
        """Determine regime from forward price path.
        Trending: price moved significantly in one direction (high directional return).
        Ranging: price oscillated but ended near start (low net, high path variance).
        """
        if not future_mids or start_mid <= 0:
            return NEUTRAL

        # Net return
        end_mid = future_mids[-1]
        net_ret = abs(end_mid - start_mid) / start_mid

        # Path variance: sum of absolute returns along the path
        path_sum = 0.0
        for i in range(1, len(future_mids)):
            if future_mids[i - 1] > 0:
                path_sum += abs(future_mids[i] - future_mids[i - 1]) / future_mids[i - 1]

        # Efficiency ratio: net_move / total_path
        efficiency = net_ret / path_sum if path_sum > 0 else 0.5

        # High efficiency (>0.4) + significant move → trending
        # Low efficiency (<0.2) → ranging
        if efficiency > 0.4 and net_ret > 0.0005:  # > 5bps net move
            return TRENDING
        elif efficiency < 0.2 and path_sum > 0.001:  # lots of movement, no direction
            return RANGING
        return NEUTRAL

    def _heuristic_regime(self) -> str:
        """Fallback: same VR+Hurst consensus logic as original regime.py."""
        vr_trend = self.vr > 1.3
        vr_range = self.vr < 0.7
        h_trend = self.hurst > 0.6
        h_range = self.hurst < 0.4
        if vr_trend and h_trend:
            return TRENDING
        elif vr_range and h_range:
            return RANGING
        return NEUTRAL

    def _mutate_weakest(self):
        """Replace the weakest tree with a new random stump."""
        import random
        if not self._trees:
            return
        weakest = min(self._trees, key=lambda t: t.weight)
        idx = self._trees.index(weakest)
        feat = random.randint(0, N_FEATURES - 1)
        # Use current feature values as threshold center
        defaults = [self.vr, self.hurst, self._spread_ema, self._arrival_rate,
                    self._ofi_momentum, self._vol_ema, self._autocorr]
        noise = random.gauss(0, 0.2)
        val = defaults[feat] if defaults[feat] != 0 else 1.0
        self._trees[idx] = OnlineTree(
            feature_idx=feat,
            threshold=val * (1 + noise),
            left_pred=[0.33, 0.34, 0.33],
            right_pred=[0.33, 0.33, 0.34],
            weight=1.0 / self.n_trees
        )

    def _calc_autocorr(self) -> float:
        """Lag-1 return autocorrelation. Positive = trending, negative = mean-reverting."""
        rets = list(self._returns)
        n = min(len(rets), 60)
        if n < 10:
            return 0.0
        rets = rets[-n:]
        mean = sum(rets) / n
        var = sum((r - mean) ** 2 for r in rets) / n
        if var == 0:
            return 0.0
        cov = sum((rets[i] - mean) * (rets[i - 1] - mean) for i in range(1, n)) / (n - 1)
        return max(-1.0, min(1.0, cov / var))

    def _calc_vr(self) -> float:
        """Variance ratio test (same as regime.py)."""
        rets = list(self._returns)
        n = len(rets)
        k = 10
        mean1 = sum(rets) / n
        var1 = sum((r - mean1) ** 2 for r in rets) / n
        if var1 == 0:
            return 1.0
        k_rets = []
        for i in range(k, n):
            k_rets.append(sum(rets[i - k:i]))
        if not k_rets:
            return 1.0
        mean_k = sum(k_rets) / len(k_rets)
        var_k = sum((r - mean_k) ** 2 for r in k_rets) / len(k_rets)
        return var_k / (k * var1)

    def _calc_hurst(self) -> float:
        """Hurst exponent via R/S analysis (same as regime.py)."""
        rets = list(self._returns)
        n = len(rets)
        if n < 20:
            return 0.5
        log_rs = []
        log_n = []
        for size in (20, 40, 80, 120):
            if size > n:
                break
            rs_vals = []
            for start in range(0, n - size + 1, size):
                chunk = rets[start:start + size]
                mean_c = sum(chunk) / len(chunk)
                cum = []
                s = 0.0
                for r in chunk:
                    s += r - mean_c
                    cum.append(s)
                R = max(cum) - min(cum)
                S = (sum((r - mean_c) ** 2 for r in chunk) / len(chunk)) ** 0.5
                if S > 0:
                    rs_vals.append(R / S)
            if rs_vals:
                avg_rs = sum(rs_vals) / len(rs_vals)
                if avg_rs > 0:
                    log_rs.append(math.log(avg_rs))
                    log_n.append(math.log(size))
        if len(log_rs) < 2:
            return 0.5
        n_pts = len(log_rs)
        sum_x = sum(log_n)
        sum_y = sum(log_rs)
        sum_xy = sum(x * y for x, y in zip(log_n, log_rs))
        sum_x2 = sum(x * x for x in log_n)
        denom = n_pts * sum_x2 - sum_x * sum_x
        if denom == 0:
            return 0.5
        slope = (n_pts * sum_xy - sum_x * sum_y) / denom
        return max(0.0, min(1.0, slope))

    # --- Same interface as RegimeDetector for drop-in replacement ---

    def adapt_thresholds(self, base_buy: float, base_sell: float) -> tuple:
        """Adjust entry thresholds based on regime + confidence.
        ML confidence modulates aggressiveness of adaptation.
        """
        if self.regime == TRENDING:
            # Confidence-scaled: more confident → more aggressive adaptation
            scale = 0.7 - 0.15 * self.confidence  # range: 0.55 to 0.7
            return base_buy * scale, base_sell * scale
        elif self.regime == RANGING:
            scale = 1.4 + 0.3 * self.confidence  # range: 1.4 to 1.7
            return base_buy * scale, base_sell * scale
        return base_buy, base_sell

    def adapt_exit(self, base_atr_mult: float) -> float:
        """Adjust ATR multiplier. Confidence-scaled."""
        if self.regime == TRENDING:
            return base_atr_mult * (1.7 + 0.3 * self.confidence)  # 1.7-2.0x
        elif self.regime == RANGING:
            return base_atr_mult * (0.65 - 0.1 * self.confidence)  # 0.55-0.65x
        return base_atr_mult

    def record_fill(self, pnl: float):
        """Track PnL per regime for analytics."""
        self.regime_fills[self.regime] = self.regime_fills.get(self.regime, 0) + 1
        self.regime_pnl[self.regime] = self.regime_pnl.get(self.regime, 0.0) + pnl

    def get_metrics(self) -> dict:
        """Expose ML model metrics for /metrics endpoint."""
        return {
            "regime": self.regime,
            "confidence": round(self.confidence, 3),
            "vr": round(self.vr, 3),
            "hurst": round(self.hurst, 3),
            "autocorr": round(self._autocorr, 3),
            "vol_ema": round(self._vol_ema, 2),
            "samples_trained": self._sample_count,
            "accuracy": round(self.accuracy, 3),
            "warmup_complete": self._sample_count >= self.warmup,
            "tree_weights": [round(t.weight, 3) for t in self._trees],
        }
