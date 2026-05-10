"""Task 7: WebSocket resilience tests — backoff, connection health, stream state.

Rust references:
- src/resilience.rs: FailureBackoff, ConnectionHealth, endpoint_rate_limiter tests
- src/live/private_ws.rs: long_lived_worker reconnect test
"""

from __future__ import annotations

import pytest

from lightfee.marketdata.resilience import (
    ConnectionHealth,
    FailureBackoff,
    is_rate_limited_status,
)
from lightfee.marketdata.ws import (
    WsConnectionState,
    WsStreamState,
)
from lightfee.marketdata.private_ws import (
    PrivateWsClientState,
    PrivateWsEvent,
    PrivateWsEventKind,
)
from lightfee.core.domain import Side, Venue


# ============================================================================
# FailureBackoff
# ============================================================================


class TestFailureBackoff:
    def test_grows_with_cap_and_resets_on_success(self):
        """Rust test: failure_backoff_grows_with_cap_and_resets_on_success."""
        b = FailureBackoff(1000, 8000, jitter_salt=0x55AA)

        assert b.on_failure() == 1000
        assert b.on_failure() == 2000
        assert b.on_failure() == 4000
        assert b.on_failure() == 8000
        assert b.on_failure() == 8000  # capped at max

        b.on_success()
        assert b.on_failure() == 1000  # reset

    def test_jitter_stays_within_20_percent(self):
        """Jitter should produce delay within ±20% of base (first call: base=1000, ±200)."""
        b = FailureBackoff(1000, 8000, jitter_salt=0x55AA)
        delay = b.on_failure_with_jitter()
        assert 800 <= delay <= 1200

    def test_initial_ms_at_least_1(self):
        b = FailureBackoff(0, 8000)
        assert b.initial_ms == 1

    def test_max_ms_at_least_initial(self):
        b = FailureBackoff(10000, 100)
        assert b.max_ms == 10000


# ============================================================================
# ConnectionHealth
# ============================================================================


class TestConnectionHealth:
    def test_turns_unhealthy_after_threshold(self):
        """Rust test: connection_health_turns_unhealthy_after_threshold_and_recovers_on_success."""
        h = ConnectionHealth()

        h.record_failure(1000, 3, "dial")
        h.record_failure(2000, 3, "dial")
        assert not h.is_unhealthy()

        h.record_failure(3000, 3, "dial")
        assert h.is_unhealthy()
        assert h.unhealthy_since_ms == 3000

        h.record_success(4000)
        assert not h.is_unhealthy()
        assert h.consecutive_failures == 0
        assert h.last_success_ms == 4000

    def test_never_unhealthy_when_threshold_zero(self):
        h = ConnectionHealth()
        for _ in range(10):
            h.record_failure(1000, 0, "error")
        assert not h.is_unhealthy()

    def test_error_message_stored(self):
        h = ConnectionHealth()
        h.record_failure(1000, 1, "connection refused")
        assert h.last_error == "connection refused"
        h.record_success(2000)
        assert h.last_error is None


# ============================================================================
# is_rate_limited_status
# ============================================================================


class TestRateLimitStatus:
    def test_429_is_rate_limited(self):
        assert is_rate_limited_status(429)

    def test_418_is_rate_limited(self):
        assert is_rate_limited_status(418)

    def test_400_is_not_rate_limited(self):
        assert not is_rate_limited_status(400)


# ============================================================================
# WsStreamState
# ============================================================================


class TestWsStreamState:
    def test_initial_state_disconnected(self):
        s = WsStreamState()
        assert s.state == WsConnectionState.DISCONNECTED

    def test_connected_transition(self):
        s = WsStreamState(state=WsConnectionState.CONNECTING)
        s.on_connected(5000)
        assert s.state == WsConnectionState.CONNECTED
        assert s.health.consecutive_failures == 0
        assert s.reconnect_count == 0

    def test_message_degraded_to_connected(self):
        s = WsStreamState(state=WsConnectionState.DEGRADED)
        s.on_message(5000)
        assert s.state == WsConnectionState.CONNECTED

    def test_disconnected_with_backoff(self):
        s = WsStreamState(state=WsConnectionState.CONNECTED)
        delay = s.on_disconnected(5000, "dial failed", unhealthy_after=5)
        assert delay >= 800  # 1000ms base ±200 jitter
        assert s.state == WsConnectionState.RECONNECTING
        assert s.reconnect_count == 1

    def test_repeated_failures_cause_degraded(self):
        s = WsStreamState(state=WsConnectionState.CONNECTED)
        for i in range(5):
            s.on_disconnected(5000 + i * 1000, f"fail {i}", unhealthy_after=3)
        assert s.health.is_unhealthy()
        assert s.state == WsConnectionState.DEGRADED

    def test_heartbeat_timeout(self):
        s = WsStreamState(state=WsConnectionState.CONNECTED)
        s.on_heartbeat_timeout(10000)
        assert s.state == WsConnectionState.DEGRADED

    def test_stale_detection(self):
        s = WsStreamState(last_message_ms=10000)
        assert s.is_stale(13001, 3000)  # age=3001 > 3000 → stale
        assert not s.is_stale(13000, 3000)  # age=3000 == max, still fresh

    def test_is_healthy(self):
        s = WsStreamState(state=WsConnectionState.CONNECTED)
        assert s.is_healthy()

        s.state = WsConnectionState.DEGRADED
        assert not s.is_healthy()


# ============================================================================
# PrivateWsClientState
# ============================================================================


class TestPrivateWsClientState:
    def test_position_confirmed(self):
        c = PrivateWsClientState(venue=Venue.BINANCE)
        assert not c.position_confirmed
        c.on_position_confirmed(5000)
        assert c.position_confirmed
        assert c.last_position_update_ms == 5000

    def test_fill_event_buffering(self):
        c = PrivateWsClientState(venue=Venue.BINANCE)
        event = PrivateWsEvent(
            venue=Venue.BINANCE,
            kind=PrivateWsEventKind.ORDER_FILL,
            symbol="BTCUSDT",
            order_id="o1",
            side=Side.SELL,
            quantity=0.01,
            price=50000.0,
            observed_at_ms=5000,
        )
        c.on_fill_event(event)
        assert len(c.pending_reconciliation) == 1

        events = c.drain_reconciliation_events()
        assert len(events) == 1
        assert events[0].order_id == "o1"
        assert len(c.pending_reconciliation) == 0

    def test_fill_to_order_fill(self):
        event = PrivateWsEvent(
            venue=Venue.BINANCE,
            kind=PrivateWsEventKind.ORDER_FILL,
            symbol="BTCUSDT",
            order_id="o1",
            side=Side.BUY,
            quantity=0.01,
            price=50000.0,
            fee_quote=2.5,
            observed_at_ms=5000,
        )
        fill = event.to_order_fill()
        assert fill is not None
        assert fill.venue == Venue.BINANCE
        assert fill.symbol == "BTCUSDT"
        assert fill.quantity == 0.01
        assert fill.price == 50000.0
        assert fill.fee_quote == 2.5

    def test_non_fill_event_returns_none(self):
        event = PrivateWsEvent(
            venue=Venue.BINANCE,
            kind=PrivateWsEventKind.ORDER_ACK,
            quantity=0.0,
        )
        fill = event.to_order_fill()
        assert fill is None

    def test_is_healthy_delegates_to_stream(self):
        c = PrivateWsClientState(venue=Venue.BINANCE)
        c.stream.state = WsConnectionState.CONNECTED
        assert c.is_healthy()

        c.stream.state = WsConnectionState.DEGRADED
        assert not c.is_healthy()
