"""
Order flow prediction — 5-tick lookahead direction classifier.
Lightweight online logistic regression using OFI, OBI, depth imbalance, trade flow.
No numpy/pandas — pure Python running average of feature weights.
"""
from collections import deque


class FlowPredictor:
    """Online linear classifier predicting 5-tick price direction.
    Features: ofi, obi, depth_pressure, toxicity, arrival_intensity, momentum.
    Trained online via perceptron update rule on last 500 outcomes.
    """

    FEATURES = ["ofi", "obi", "depth_pressure", "toxicity", "arrival", "momentum"]

    def __init__(self, lookback: int = 5, lr: float = 0.01, min_samples: int = 50):
        self.lookback = lookback
        self.lr = lr
        self.min_samples = min_samples
        # Weights for linear model (initialized to 0)
        self.weights = {f: 0.0 for f in self.FEATURES}
        self.bias = 0.0
        # Pending predictions awaiting outcome
        self._pending: deque = deque(maxlen=100)  # (features, mid_at_entry, tick_count_at_entry)
        self._tick_count = 0
        self._mid_history: deque = deque(maxlen=20)
        # Stats
        self.correct = 0
        self.total = 0

    def _sigmoid(self, x: float) -> float:
        if x > 10:
            return 0.999
        if x < -10:
            return 0.001
        import math
        return 1.0 / (1.0 + math.exp(-x))

    def _score(self, features: dict) -> float:
        """Raw linear score: w·x + b."""
        s = self.bias
        for f in self.FEATURES:
            s += self.weights.get(f, 0) * features.get(f, 0)
        return s

    def predict(self, features: dict) -> tuple[float, float]:
        """Predict direction. Returns (direction, confidence).
        direction: >0 = up, <0 = down
        confidence: 0-1, how certain the model is
        """
        score = self._score(features)
        prob_up = self._sigmoid(score)
        if prob_up > 0.5:
            return 1.0, prob_up
        else:
            return -1.0, 1.0 - prob_up

    def observe(self, features: dict, mid: float):
        """Record current features + mid for later training when outcome is known."""
        self._tick_count += 1
        self._mid_history.append(mid)

        # Check pending predictions: if lookback ticks have passed, train
        while self._pending and (self._tick_count - self._pending[0][2]) >= self.lookback:
            feat, entry_mid, _ = self._pending.popleft()
            if entry_mid > 0 and mid > 0:
                actual = 1.0 if mid > entry_mid else -1.0 if mid < entry_mid else 0.0
                if actual != 0:
                    self._train(feat, actual)

        # Store current observation for future training
        self._pending.append((features.copy(), mid, self._tick_count))

    def _train(self, features: dict, label: float):
        """Online perceptron update."""
        score = self._score(features)
        pred = 1.0 if score > 0 else -1.0

        # Track accuracy
        self.total += 1
        if pred == label:
            self.correct += 1

        # Only update on errors (perceptron rule)
        if pred != label:
            for f in self.FEATURES:
                self.weights[f] += self.lr * label * features.get(f, 0)
            self.bias += self.lr * label

    @property
    def accuracy(self) -> float:
        return self.correct / self.total if self.total > 0 else 0.5

    @property
    def ready(self) -> bool:
        return self.total >= self.min_samples

    def status(self) -> dict:
        return {
            "ready": self.ready,
            "accuracy": round(self.accuracy, 3),
            "samples": self.total,
            "weights": {k: round(v, 4) for k, v in self.weights.items()},
            "bias": round(self.bias, 4),
        }
