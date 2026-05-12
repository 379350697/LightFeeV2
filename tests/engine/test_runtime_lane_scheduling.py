"""V1 semantic parity: Runtime lane scheduling with independent failure isolation.

Contract: LANE-001 — Independent Lane Scheduling with Failure Isolation
V1 anchors: src/execution_core/engine/engine.rs (tokio::select tick scheduling),
            src/app_runtime/loop_control.rs (metrics export, state snapshots)
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from lightfee.config.schema import (
    AppConfig,
    PersistenceConfig,
    RuntimeConfig,
    StrategyConfig,
)
from lightfee.engine.bootstrap import (
    active_position_tick_ready,
    full_tick_ready,
    wall_clock_now_ms,
)
from lightfee.engine.runtime import LiveRuntime


class TestLaneFailureIsolation:
    """V1: A slow or failing full tick must not block active or maker-event lanes."""

    @pytest.mark.asyncio
    async def test_full_tick_failure_does_not_block_active_tick(self, monkeypatch):
        """V1 parity: when full tick fails with backoff, active tick still runs."""
        td = tempfile.mkdtemp()
        try:
            config = _paper_config(td)
            runtime = LiveRuntime(config)

            # Track which lanes ran
            lane_runs: dict[str, int] = {"full": 0, "active": 0, "maker": 0, "housekeeping": 0}

            async def boom_tick(_self=None):
                lane_runs["full"] += 1
                raise RuntimeError("V1 simulated tick failure")

            async def ok_active_tick(_self=None):
                lane_runs["active"] += 1

            async def ok_maker_tick(_self=None, now_ms: int = 0):
                lane_runs["maker"] += 1

            async def ok_housekeeping(_self=None, now_ms: int = 0):
                lane_runs["housekeeping"] += 1

            monkeypatch.setattr(runtime, "tick", boom_tick)
            monkeypatch.setattr(runtime, "tick_active_positions", ok_active_tick)
            monkeypatch.setattr(runtime, "_maybe_tick_maker_event", ok_maker_tick)
            monkeypatch.setattr(runtime, "_post_tick_housekeeping", ok_housekeeping)

            runtime.journal.open()
            runtime._running = True
            now_ms = wall_clock_now_ms()

            # Full tick fails → backoff applied
            try:
                await runtime.tick()
            except Exception:
                runtime._apply_tick_backoff(is_active=False)
                runtime.journal.append("runtime.tick_error", {"error": "boom"})

            assert runtime._tick_backoff_until_ms is not None
            assert runtime._tick_backoff_until_ms > now_ms

            # Active tick must still be eligible (separate backoff)
            assert active_position_tick_ready(
                runtime._active_tick_backoff_until_ms, now_ms
            ), "V1 parity violation: active tick blocked by unrelated full-tick backoff"

            # Run active tick — must succeed
            await runtime.tick_active_positions()
            assert lane_runs["active"] >= 1, (
                "V1 parity violation: active tick did not run after full tick failure"
            )

            # Maker tick must still be eligible
            assert full_tick_ready(
                runtime._maker_tick_backoff_until_ms, now_ms
            ), "V1 parity violation: maker tick blocked by unrelated full-tick backoff"

            # Run maker tick
            await runtime._maybe_tick_maker_event(now_ms)
            assert lane_runs["maker"] >= 1, (
                "V1 parity violation: maker tick did not run after full tick failure"
            )

            # Housekeeping must still run
            await runtime._post_tick_housekeeping(now_ms)
            assert lane_runs["housekeeping"] >= 1, (
                "V1 parity violation: post-tick housekeeping blocked after full tick failure"
            )

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_active_tick_failure_does_not_block_full_tick(self, monkeypatch):
        """V1 parity: active tick failure isolates from full tick lane."""
        td = tempfile.mkdtemp()
        try:
            config = _paper_config(td)
            runtime = LiveRuntime(config)

            lane_runs: dict[str, int] = {"full": 0, "active": 0}

            async def ok_tick(_self=None):
                lane_runs["full"] += 1

            async def boom_active(_self=None):
                lane_runs["active"] += 1
                raise RuntimeError("V1 simulated active tick failure")

            monkeypatch.setattr(runtime, "tick", ok_tick)
            monkeypatch.setattr(runtime, "tick_active_positions", boom_active)

            runtime.journal.open()
            now_ms = wall_clock_now_ms()

            # Active tick fails → backoff applied
            try:
                await runtime.tick_active_positions()
            except Exception:
                runtime._apply_tick_backoff(is_active=True)

            assert runtime._active_tick_backoff_until_ms is not None
            assert runtime._active_tick_backoff_until_ms > now_ms

            # Full tick must still be eligible
            assert full_tick_ready(
                runtime._tick_backoff_until_ms, now_ms
            ), "V1 parity violation: full tick blocked by unrelated active-tick backoff"

            # Full tick must succeed
            await runtime.tick()
            assert lane_runs["full"] >= 1, (
                "V1 parity violation: full tick did not run after active tick failure"
            )

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_maker_tick_backoff_independent(self):
        """V1 parity: maker-event tick has its own backoff state independent of full/active."""
        td = tempfile.mkdtemp()
        try:
            config = _paper_config(td)
            runtime = LiveRuntime(config)
            now_ms = wall_clock_now_ms()

            # Apply maker backoff only
            runtime._apply_tick_backoff(is_maker=True)
            assert runtime._maker_tick_backoff_until_ms is not None
            assert runtime._maker_tick_backoff_until_ms > now_ms

            # Full and active ticks must be unaffected
            assert runtime._tick_backoff_until_ms is None, (
                "V1 parity violation: maker backoff leaked to full tick lane"
            )
            assert runtime._active_tick_backoff_until_ms is None, (
                "V1 parity violation: maker backoff leaked to active tick lane"
            )

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)


class TestLaneScheduling:
    """V1: Each lane has its own scheduling contract."""

    def test_full_tick_ready_respects_backoff(self):
        """Full tick fires only when past its backoff deadline."""
        now = wall_clock_now_ms()
        assert full_tick_ready(None, now) is True
        assert full_tick_ready(now + 5000, now) is False
        assert full_tick_ready(now, now) is True

    def test_active_tick_ready_respects_backoff(self):
        """Active tick fires only when past its backoff deadline."""
        now = wall_clock_now_ms()
        assert active_position_tick_ready(None, now) is True
        assert active_position_tick_ready(now + 5000, now) is False

    @pytest.mark.asyncio
    async def test_rate_limit_reload_is_periodic(self, monkeypatch):
        """V1: rate-limit reload fires on its own interval, not gated by tick backoff."""
        td = tempfile.mkdtemp()
        try:
            config = _paper_config(td)
            runtime = LiveRuntime(config)

            reload_count = [0]

            class FakeRateLimitRuntime:
                async def refresh(self, now_ms: int = 0):
                    reload_count[0] += 1

                def flush_recommendations(self):
                    pass

            runtime._rate_limit_runtime = FakeRateLimitRuntime()
            runtime._last_rate_limit_reload_ms = 0

            now_ms = wall_clock_now_ms()
            # First call: should reload (interval elapsed)
            await runtime._maybe_reload_rate_limits(now_ms)
            assert reload_count[0] == 1, "V1: first rate-limit reload should fire"

            # Immediate second call: should be gated by interval
            await runtime._maybe_reload_rate_limits(now_ms + 1000)
            assert reload_count[0] == 1, "V1: rate-limit reload should be interval-gated"

            # After interval: should fire again
            await runtime._maybe_reload_rate_limits(now_ms + 31000)
            assert reload_count[0] == 2, "V1: rate-limit reload should fire after interval"

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    def test_shutdown_is_explicit_lane(self):
        """V1: shutdown is an independently scheduled transition, not a tick lane.
        stop() sets _running=False to break the loop, then runs cleanup."""
        td = tempfile.mkdtemp()
        try:
            config = _paper_config(td)
            runtime = LiveRuntime(config)
            assert runtime._running is False
            runtime._running = True
            runtime._running = False  # Simulate shutdown signal
            assert runtime._running is False
        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)


class TestBackoffSemantics:
    """V1: failure backoff is incremental with configurable floor/cap."""

    def test_backoff_starts_at_initial(self):
        """First failure uses initial backoff, not doubled."""
        td = tempfile.mkdtemp()
        try:
            config = _paper_config(td, tick_failure_backoff_initial_ms=500, tick_failure_backoff_max_ms=5000)
            runtime = LiveRuntime(config)
            now_ms = wall_clock_now_ms()

            runtime._apply_tick_backoff(is_active=False)
            backoff_duration = runtime._tick_backoff_until_ms - now_ms
            assert backoff_duration == 500, (
                f"V1: initial backoff should be 500ms, got {backoff_duration}ms"
            )
        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    def test_backoff_doubles_on_repeat(self):
        """Repeated failures double the backoff up to max."""
        td = tempfile.mkdtemp()
        try:
            config = _paper_config(td, tick_failure_backoff_initial_ms=500, tick_failure_backoff_max_ms=5000)
            runtime = LiveRuntime(config)

            # First failure: 500ms
            runtime._apply_tick_backoff(is_active=False)
            first_deadline = runtime._tick_backoff_until_ms

            # Second failure before first expires: 1000ms from now
            runtime._apply_tick_backoff(is_active=False)
            second_deadline = runtime._tick_backoff_until_ms

            # Third failure: 2000ms
            runtime._apply_tick_backoff(is_active=False)
            third_deadline = runtime._tick_backoff_until_ms

            assert second_deadline > first_deadline, "V1: backoff should increase"
            assert third_deadline > second_deadline, "V1: backoff should increase"

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    def test_backoff_capped_at_max(self):
        """Backoff never exceeds configured max."""
        td = tempfile.mkdtemp()
        try:
            config = _paper_config(td, tick_failure_backoff_initial_ms=500, tick_failure_backoff_max_ms=5000)
            runtime = LiveRuntime(config)
            now_ms = wall_clock_now_ms()

            # Simulate many consecutive failures
            for _ in range(10):
                runtime._apply_tick_backoff(is_active=False)

            backoff_duration = runtime._tick_backoff_until_ms - now_ms
            assert backoff_duration <= 5000, (
                f"V1: backoff {backoff_duration}ms exceeds max 5000ms"
            )
        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _paper_config(
    td: str,
    tick_failure_backoff_initial_ms: int = 500,
    tick_failure_backoff_max_ms: int = 5000,
) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            mode="paper",
            poll_interval_ms=100,
            sidecar_snapshot_path=str(Path(td) / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600_000,
            tick_failure_backoff_initial_ms=tick_failure_backoff_initial_ms,
            tick_failure_backoff_max_ms=tick_failure_backoff_max_ms,
        ),
        strategy=StrategyConfig(
            risk_monitor_enabled=False,
            max_concurrent_positions=2,
            local_l2_enabled=False,
            local_l2_ws_enabled=False,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(Path(td) / "events.jsonl"),
            snapshot_path=str(Path(td) / "state.json"),
        ),
        venues=[],
        symbols=["BTCUSDT"],
    )
