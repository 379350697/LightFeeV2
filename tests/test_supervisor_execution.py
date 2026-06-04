"""Task 6: Supervisor risk execution tests.

Rust references:
- src/engine/risk.rs: manage_open_positions (line 529), manage_open_position (line 1255)
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from lightfee.config.schema import AppConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import OrderFill
from lightfee.core.domain import Side, Venue
from lightfee.engine.risk_actions import (
    AccountRiskSnapshot,
    PositionRiskView,
    RiskExecutionPlan,
    RiskExecutionPlanKind,
    evaluate_position_risk,
)
from lightfee.engine.state import EngineState, OpenPosition, PendingEntry
from lightfee.engine.supervisor import Supervisor
from lightfee.persistence.journal import Journal
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

_STRATEGY_FIELDS = {f.name for f in StrategyConfig.__dataclass_fields__.values()}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> AppConfig:
    strategy = dict(
        warning_health_ratio=3.0,
        delever_health_ratio=1.5,
        death_health_ratio=1.1,
        max_risk_snapshot_age_ms=30_000,
        unsupported_risk_snapshot_behavior="death_line",
        risk_monitor_enabled=True,
        warning_line_enabled=True,
        warning_pause_new_entries_enabled=True,
        delever_line_enabled=True,
        delever_auto_execute_enabled=True,
        death_line_enabled=True,
        death_single_side_protection_enabled=True,
        partial_delever_ratio=0.2,
        partial_delever_cooldown_ms=30_000,
        max_partial_delever_steps=4,
        health_recovery_ratio=2.0,
    )
    strategy.update(overrides)
    filtered = {k: v for k, v in strategy.items() if k in _STRATEGY_FIELDS}
    return AppConfig(
        runtime=RuntimeConfig(),
        strategy=StrategyConfig(**filtered),
    )


def _make_journal() -> Journal:
    j = Journal(Path(tempfile.mkdtemp()) / "test.jsonl")
    j.open()
    return j


def _make_position(**overrides) -> OpenPosition:
    defaults = dict(
        position_id="p001",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        long_quantity=0.01,
        short_quantity=0.01,
        long_entry_price=50000.0,
        short_entry_price=50000.0,
        opened_at_ms=1000000,
        matched_quantity=0.01,
    )
    defaults.update(overrides)
    return OpenPosition(**defaults)


def _snapshot(venue: Venue, equity: float, margin: float, observed_ms: int = 10000) -> AccountRiskSnapshot:
    return AccountRiskSnapshot(
        venue=venue,
        equity_quote=equity,
        maintenance_margin_quote=margin,
        health_ratio=equity / margin if margin > 0 else 0.0,
        observed_at_ms=observed_ms,
        source="test",
    )


# ---------------------------------------------------------------------------
# Global risk mode updates
# ---------------------------------------------------------------------------


class TestGlobalRiskModeUpdate:
    def test_warning_line_sets_entry_paused(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        new_mode = supervisor.update_global_risk_mode({"binance": 2.5, "okx": 5.0})
        assert new_mode == GlobalRiskMode.ENTRY_PAUSED
        assert state.risk_mode == GlobalRiskMode.ENTRY_PAUSED

    def test_delever_line_sets_reduce_only(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        new_mode = supervisor.update_global_risk_mode({"binance": 1.4, "okx": 5.0})
        assert new_mode == GlobalRiskMode.REDUCE_ONLY
        assert state.risk_mode == GlobalRiskMode.REDUCE_ONLY

    def test_death_line_sets_fail_closed(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        new_mode = supervisor.update_global_risk_mode({"binance": 1.0, "okx": 5.0})
        assert new_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.lifecycle == EngineLifecycle.RISK_ONLY  # V1: FailClosed = RISK_ONLY + FAIL_CLOSED risk

    def test_healthy_returns_running(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        new_mode = supervisor.update_global_risk_mode({"binance": 5.0, "okx": 6.0})
        assert new_mode == GlobalRiskMode.RUNNING

    def test_recovery_from_entry_paused_logs_cleared(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        state.risk_mode = GlobalRiskMode.ENTRY_PAUSED
        supervisor.update_global_risk_mode({"binance": 5.0, "okx": 6.0})
        entries = journal.read_all()
        cleared = [e for e in entries if e.get("kind") == "risk.entry_pause_cleared"]
        assert len(cleared) == 1

    def test_disabled_lines_do_not_trigger(self):
        config = _make_config(
            warning_line_enabled=False,
            warning_pause_new_entries_enabled=False,
        )
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        new_mode = supervisor.update_global_risk_mode({"binance": 2.5})
        assert new_mode == GlobalRiskMode.RUNNING

    def test_fail_closed_latch_does_not_clear_with_pending_entry_work(self):
        config = _make_config()
        state = EngineState(lifecycle=EngineLifecycle.RISK_ONLY)
        state.risk_mode = GlobalRiskMode.FAIL_CLOSED
        state.pending_entries["entry-1"] = PendingEntry(
            pending_id="entry-1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
        )
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        new_mode = supervisor.update_global_risk_mode({"binance": 5.0, "okx": 6.0})

        assert new_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert not any(
            event.get("kind") == "risk.fail_closed_auto_resumed"
            for event in journal.read_all()
        )

    def test_supervise_disabled_monitor_clears_clean_fail_closed_latch(self):
        config = _make_config(risk_monitor_enabled=False)
        state = EngineState(lifecycle=EngineLifecycle.RISK_ONLY)
        state.risk_mode = GlobalRiskMode.FAIL_CLOSED
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        supervisor.supervise(5000, {"binance": 5.0, "okx": 6.0})

        assert state.risk_mode == GlobalRiskMode.RUNNING
        assert state.lifecycle == EngineLifecycle.RUNNING
        assert any(
            event.get("kind") == "risk.fail_closed_auto_resumed"
            for event in journal.read_all()
        )


# ---------------------------------------------------------------------------
# Per-position risk supervision
# ---------------------------------------------------------------------------


class TestSupervisePosition:
    def test_death_returns_single_side_protection(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position()
        long_snap = _snapshot(Venue.BINANCE, 105.0, 100.0, 10000)
        short_snap = _snapshot(Venue.OKX, 500.0, 100.0, 10000)

        plan = supervisor.supervise_position(pos, 10000, long_snap, short_snap)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.SINGLE_SIDE_PROTECTION

    def test_delever_returns_delever_plan(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position(matched_quantity=0.01)
        long_snap = _snapshot(Venue.BINANCE, 140.0, 100.0, 10000)
        short_snap = _snapshot(Venue.OKX, 500.0, 100.0, 10000)

        plan = supervisor.supervise_position(pos, 10000, long_snap, short_snap)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.DELEVER

    def test_healthy_returns_none(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position()
        long_snap = _snapshot(Venue.BINANCE, 500.0, 100.0, 10000)
        short_snap = _snapshot(Venue.OKX, 600.0, 100.0, 10000)

        plan = supervisor.supervise_position(pos, 10000, long_snap, short_snap)
        assert plan is None

    def test_delever_recovery_resets_step_count(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position(risk_delever_step_count=2)
        long_snap = _snapshot(Venue.BINANCE, 500.0, 100.0, 10000)
        short_snap = _snapshot(Venue.OKX, 600.0, 100.0, 10000)

        supervisor.supervise_position(pos, 10000, long_snap, short_snap)
        assert pos.risk_delever_step_count == 0
        assert pos.last_risk_action_at_ms == 0

        entries = journal.read_all()
        recovered = [e for e in entries if e.get("kind") == "risk.delever_recovered"]
        assert len(recovered) == 1

    def test_risk_monitor_disabled_returns_none(self):
        config = _make_config(risk_monitor_enabled=False)
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position()
        plan = supervisor.supervise_position(pos, 5000)
        assert plan is None


# ---------------------------------------------------------------------------
# Risk plan execution
# ---------------------------------------------------------------------------


class _FakeCloseExecutor:
    """Fake close executor that records calls for testing supervisor execution."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.single_side_calls: list[dict] = []

    async def execute_close(self, position, reason, now_ms, **kwargs) -> None:
        self.calls.append({
            "position_id": position.position_id,
            "reason": reason,
            "now_ms": now_ms,
            "kwargs": kwargs,
        })

    async def execute_single_side_protection(
        self, position, venue, side, reason, now_ms, **kwargs
    ):
        self.single_side_calls.append({
            "position_id": position.position_id,
            "venue": venue,
            "side": side,
            "reason": reason,
            "now_ms": now_ms,
            "kwargs": kwargs,
        })
        return {
            "outcome": "filled",
            "fill": OrderFill(
                venue=venue,
                symbol=position.symbol,
                side=side,
                quantity=position.matched_quantity,
                price=50000.0,
                order_id="single-side-fill",
                client_order_id="single-side-cid",
                filled_at_ms=now_ms,
            ),
            "client_order_id": "single-side-cid",
            "stage": kwargs.get("stage", ""),
        }


class TestExecuteRiskPlan:
    @pytest.mark.asyncio
    async def test_execute_risk_plan_delever_awaits_close_executor(self):
        """Fix 1: DELEVER plan via execute_risk_plan must call close_executor.execute_close."""
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        fake_close = _FakeCloseExecutor()
        supervisor = Supervisor(config, state, journal, close_executor=fake_close)

        pos = _make_position(matched_quantity=0.01)
        plan = RiskExecutionPlan(
            kind=RiskExecutionPlanKind.DELEVER,
            reason="risk_delever",
            requested_quantity=0.002,
            adjusted_quantity=0.002,
        )
        await supervisor.execute_risk_plan(pos, plan, 5000, long_price_hint=50000.0, short_price_hint=50000.0)

        # The close executor MUST have been called
        assert len(fake_close.calls) == 1, f"expected 1 close executor call, got {len(fake_close.calls)}"
        assert fake_close.calls[0]["position_id"] == "p001"
        assert fake_close.calls[0]["reason"] == "risk_delever"
        # Position state must be updated
        assert pos.risk_delever_step_count == 1
        assert pos.last_risk_action_at_ms == 5000

    @pytest.mark.asyncio
    async def test_execute_delever_increments_step_count(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position(matched_quantity=0.01)
        plan = RiskExecutionPlan(
            kind=RiskExecutionPlanKind.DELEVER,
            reason="risk_delever",
            requested_quantity=0.002,
            adjusted_quantity=0.002,
        )
        await supervisor._execute_delever(pos, plan, 5000, 0.0, 0.0)

        assert pos.risk_delever_step_count == 1
        assert pos.last_risk_action_at_ms == 5000
        assert pos.last_risk_reason == "risk_delever"

    @pytest.mark.asyncio
    async def test_execute_delever_logs_triggered(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position(matched_quantity=0.01)
        plan = RiskExecutionPlan(
            kind=RiskExecutionPlanKind.DELEVER,
            reason="risk_delever",
            requested_quantity=0.002,
            adjusted_quantity=0.002,
        )
        await supervisor._execute_delever(pos, plan, 5000, 0.0, 0.0)

        entries = journal.read_all()
        triggered = [e for e in entries if e.get("kind") == "risk.delever_triggered"]
        assert len(triggered) == 1

    @pytest.mark.asyncio
    async def test_execute_delever_reaches_limit_logs(self):
        config = _make_config(max_partial_delever_steps=2)
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position(matched_quantity=0.01, risk_delever_step_count=1)
        plan = RiskExecutionPlan(
            kind=RiskExecutionPlanKind.DELEVER,
            reason="risk_delever",
            requested_quantity=0.002,
            adjusted_quantity=0.002,
        )
        await supervisor._execute_delever(pos, plan, 5000, 0.0, 0.0)

        entries = journal.read_all()
        limit = [e for e in entries if e.get("kind") == "risk.delever_limit_reached"]
        assert len(limit) == 1

    @pytest.mark.asyncio
    async def test_execute_single_side_protection_enters_fail_closed(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        fake_close = _FakeCloseExecutor()
        supervisor = Supervisor(config, state, journal, close_executor=fake_close)

        pos = _make_position()
        plan = RiskExecutionPlan(
            kind=RiskExecutionPlanKind.SINGLE_SIDE_PROTECTION,
            reason="test_death",
            protection_venue=pos.short_venue,
            protection_side=Side.BUY,
            protection_stage="risk_protection_short",
        )
        await supervisor._execute_single_side_protection(pos, plan, 5000)

        assert fake_close.calls == []
        assert len(fake_close.single_side_calls) == 1
        assert fake_close.single_side_calls[0]["venue"] == pos.short_venue
        assert fake_close.single_side_calls[0]["side"] == Side.BUY
        assert pos.single_side_protection_triggered
        assert pos.last_risk_reason == "test_death"
        assert state.lifecycle == EngineLifecycle.RISK_ONLY  # V1: FailClosed = RISK_ONLY + FAIL_CLOSED risk
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED

    def test_execute_fail_closed_enters_fail_closed(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        pos = _make_position()
        plan = RiskExecutionPlan(
            kind=RiskExecutionPlanKind.FAIL_CLOSED,
            reason="death_line_health_breach",
        )
        supervisor._execute_fail_closed(pos, plan, 5000)

        assert pos.last_risk_reason == "death_line_health_breach"
        assert state.lifecycle == EngineLifecycle.RISK_ONLY  # V1: FailClosed = RISK_ONLY + FAIL_CLOSED risk
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED


# ---------------------------------------------------------------------------
# Full supervision tick
# ---------------------------------------------------------------------------


class TestSuperviseTick:
    def test_tick_updates_global_mode(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        supervisor.supervise(5000, {"binance": 1.4, "okx": 5.0})
        assert state.risk_mode == GlobalRiskMode.REDUCE_ONLY

    def test_tick_disabled_does_nothing(self):
        config = _make_config(risk_monitor_enabled=False)
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        supervisor.supervise(5000, {"binance": 1.0})
        assert state.risk_mode == GlobalRiskMode.RUNNING
        assert state.lifecycle == EngineLifecycle.BOOTING

    def test_tick_logs_trigger_events(self):
        config = _make_config()
        state = EngineState()
        journal = _make_journal()
        supervisor = Supervisor(config, state, journal)

        supervisor.supervise(5000, {"binance": 1.0})
        entries = journal.read_all()
        kinds = {e.get("kind") for e in entries}
        assert "risk.death_line_triggered" in kinds
        assert "risk.global_mode_changed" in kinds
        assert "risk.fail_closed_entered" in kinds
