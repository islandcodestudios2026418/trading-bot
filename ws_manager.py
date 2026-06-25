"""
WebSocket connection manager — exponential backoff reconnection + state recovery.
Shared infrastructure for all WS connections (Binance, OKX, Polymarket).
"""
import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class WSState:
    """Track WS connection health and reconnection state."""
    name: str
    connected: bool = False
    reconnects: int = 0
    last_msg_time: float = 0.0
    last_connect_time: float = 0.0
    total_messages: int = 0
    _backoff: float = 1.0  # current backoff seconds

    # Latency tracking
    _latencies: list = field(default_factory=list)  # last 100 msg processing times

    def on_connect(self):
        self.connected = True
        self.last_connect_time = time.time()
        self._backoff = 1.0  # reset backoff on successful connect

    def on_disconnect(self):
        self.connected = False

    def on_message(self):
        self.last_msg_time = time.time()
        self.total_messages += 1

    def record_latency(self, latency_ms: float):
        self._latencies.append(latency_ms)
        if len(self._latencies) > 100:
            self._latencies.pop(0)

    @property
    def avg_latency_ms(self) -> float:
        return sum(self._latencies) / len(self._latencies) if self._latencies else 0.0

    @property
    def stale(self) -> bool:
        """No message in 30s = stale."""
        return self.connected and self.last_msg_time > 0 and (time.time() - self.last_msg_time) > 30

    def next_backoff(self) -> float:
        """Exponential backoff: 1s, 2s, 4s, 8s, 16s, max 60s."""
        delay = self._backoff
        self._backoff = min(60.0, self._backoff * 2)
        self.reconnects += 1
        return delay

    def status(self) -> dict:
        return {
            "name": self.name,
            "connected": self.connected,
            "reconnects": self.reconnects,
            "stale": self.stale,
            "msg_count": self.total_messages,
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "uptime_s": round(time.time() - self.last_connect_time, 0) if self.last_connect_time else 0,
        }


# Global registry of all WS connections for monitoring
_connections: dict[str, WSState] = {}


def register(name: str) -> WSState:
    state = WSState(name=name)
    _connections[name] = state
    return state


def all_status() -> list[dict]:
    return [s.status() for s in _connections.values()]


def any_stale() -> list[str]:
    return [name for name, s in _connections.items() if s.stale]
