"""V1 WebSocket lifecycle: connection states, reconnection, heartbeat.

Rust references:
- src/resilience.rs: FailureBackoff, ConnectionHealth
- src/live/private_ws.rs: long-lived worker pattern
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from lightfee.marketdata.resilience import ConnectionHealth, FailureBackoff


# ---------------------------------------------------------------------------
# WS connection state machine
# ---------------------------------------------------------------------------


class WsConnectionState(Enum):
    """V1 WebSocket lifecycle states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DEGRADED = "degraded"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"


@dataclass
class WsConnectionConfig:
    """WebSocket connection configuration matching V1 runtime settings."""
    url: str = ""
    reconnect_initial_ms: int = 1_000
    reconnect_max_ms: int = 30_000
    unhealthy_after_failures: int = 5
    heartbeat_interval_ms: int = 30_000
    heartbeat_timeout_ms: int = 10_000
    max_message_age_ms: int = 3_000
    stale_stream_timeout_ms: int = 60_000


@dataclass
class WsStreamState:
    """Per-stream health tracking (one per venue+symbol or venue+private)."""
    state: WsConnectionState = WsConnectionState.DISCONNECTED
    backoff: FailureBackoff = field(default_factory=lambda: FailureBackoff(1000, 30000, 0x55AA))
    health: ConnectionHealth = field(default_factory=ConnectionHealth)
    last_message_ms: int = 0
    started_at_ms: int = 0
    reconnected_at_ms: int = 0
    reconnect_count: int = 0

    def on_message(self, now_ms: int) -> None:
        self.last_message_ms = now_ms
        if self.state == WsConnectionState.DEGRADED:
            self.state = WsConnectionState.CONNECTED
            self.health.record_success(now_ms)

    def on_connected(self, now_ms: int) -> None:
        if self.state == WsConnectionState.CONNECTING or self.state == WsConnectionState.RECONNECTING:
            self.state = WsConnectionState.CONNECTED
            self.health.record_success(now_ms)
            self.reconnect_count = 0

    def on_disconnected(self, now_ms: int, error: str, unhealthy_after: int = 5) -> int:
        """Record disconnect, return backoff delay_ms for reconnect."""
        self.state = WsConnectionState.RECONNECTING
        self.health.record_failure(now_ms, unhealthy_after, error)
        if self.health.is_unhealthy():
            self.state = WsConnectionState.DEGRADED
        self.reconnect_count += 1
        return self.backoff.on_failure_with_jitter()

    def on_heartbeat_timeout(self, now_ms: int) -> None:
        """Heartbeat missed → degrade connection."""
        self.state = WsConnectionState.DEGRADED
        self.health.record_failure(now_ms, 1, "heartbeat_timeout")

    def is_stale(self, now_ms: int, max_message_age_ms: int) -> bool:
        if self.last_message_ms == 0:
            return False
        return (now_ms - self.last_message_ms) > max_message_age_ms

    def is_healthy(self) -> bool:
        return self.state in (WsConnectionState.CONNECTED, WsConnectionState.CONNECTING)


# ---------------------------------------------------------------------------
# Per-symbol L2 WS stream
# ---------------------------------------------------------------------------


@dataclass
class L2WsStream:
    """WebSocket stream for a single venue+symbol L2 order book."""
    venue: str
    symbol: str
    stream: WsStreamState = field(default_factory=WsStreamState)

    def apply_update(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]], now_ms: int) -> None:
        """Apply incremental L2 update."""
        self.stream.on_message(now_ms)

    def needs_bootstrap(self) -> bool:
        return self.stream.state in (
            WsConnectionState.DISCONNECTED,
            WsConnectionState.RECONNECTING,
            WsConnectionState.DEGRADED,
        )
