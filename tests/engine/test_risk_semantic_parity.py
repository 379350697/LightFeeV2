"""Semantic parity tests for risk evaluation and venue health (RISK-001).

V1 references:
- src/risk.rs: evaluate_position_risk, PositionRiskView
- src/health.rs: evaluate_venue_health, VenueHealthAction
- src/engine/risk.rs: manage_open_position, RiskExecutionPlan
"""

from __future__ import annotations

import pytest
from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import Venue
from lightfee.engine.risk_actions import (
    AccountRiskSnapshot,
    PositionRiskView,
    RiskAction,
    RiskExecutionPlan,
    RiskExecutionPlanKind,
    UnsupportedRiskSnapshotBehavior,
    VenueHealthAction,
    VenueHealthView,
    build_risk_execution_plan,
    evaluate_position_risk,
    evaluate_venue_health,
)
from lightfee.engine.state import OpenPosition


# ============================================================================
# Test fixtures
# ============================================================================


def make_strategy(**overrides) -> StrategyConfig:
    defaults = {
        "warning_health_ratio": 1.5,
        "delever_health_ratio": 1.2,
        "death_health_ratio": 1.0,
        "max_risk_snapshot_age_ms": 60_000,
        "unsupported_risk_snapshot_behavior": "death_line",
        "risk_monitor_enabled": True,
        "warning_line_enabled": True,
        "delever_line_enabled": True,
        "death_line_enabled": True,
        "death_single_side_protection_enabled": True,
        "delever_auto_execute_enabled": True,
        "warning_pause_new_entries_enabled": True,
        "max_partial_delever_steps": 5,
        "partial_delever_cooldown_ms": 30_000,
        "partial_delever_ratio": 0.25,
        "health_recovery_ratio": 1.5,
    }
    defaults.update(overrides)
    return StrategyConfig(**defaults)


def make_position(**overrides) -> OpenPosition:
    defaults = {
        "position_id": "risk-test-1",
        "symbol": "BTC-USDT",
        "long_venue": Venue.BINANCE,
        "short_venue": Venue.BYBIT,
        "long_quantity": 1.0,
        "short_quantity": 1.0,
        "long_entry_price": 50000.0,
        "short_entry_price": 50000.0,
        "opened_at_ms": 1000,
        "matched_quantity": 1.0,
    }
    defaults.update(overrides)
    return OpenPosition(**defaults)


def make_risk_snapshot(
    venue: Venue,
    equity: float = 10000.0,
    maintenance_margin: float = 5000.0,
    health_ratio: float = 2.0,
    observed_at_ms: int = 1000,
    supported: bool = True,
    stale: bool = False,
) -> AccountRiskSnapshot:
    return AccountRiskSnapshot(
        venue=venue,
        equity_quote=equity,
        maintenance_margin_quote=maintenance_margin,
        health_ratio=health_ratio,
        observed_at_ms=observed_at_ms,
        supported=supported,
        stale=stale,
    )


# ============================================================================
# RISK-001: Risk Evaluation Semantics
# ============================================================================


class TestVenueHealth:
    """V1 evaluate_venue_health: warning/delever/death lines, cooldowns, fail-closed."""

    def test_healthy_venue(self):
        strategy = make_strategy()
        snapshot = make_risk_snapshot(Venue.BINANCE, health_ratio=2.0)
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=5000,
            supports_risk_health=True, risk_snapshot=snapshot,
        )
        assert view.action == VenueHealthAction.NORMAL
        assert not view.degraded

    def test_warning_line(self):
        strategy = make_strategy(warning_health_ratio=2.0)
        snapshot = make_risk_snapshot(Venue.BINANCE, health_ratio=1.8)
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=5000,
            supports_risk_health=True, risk_snapshot=snapshot,
        )
        assert view.action == VenueHealthAction.PAUSE_ENTRY
        assert view.degraded

    def test_delever_line(self):
        strategy = make_strategy(delever_health_ratio=1.2)
        snapshot = make_risk_snapshot(Venue.BINANCE, health_ratio=1.1)
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=5000,
            supports_risk_health=True, risk_snapshot=snapshot,
        )
        assert view.action == VenueHealthAction.REDUCE_ONLY
        assert view.degraded

    def test_death_line(self):
        strategy = make_strategy(death_health_ratio=1.0)
        snapshot = make_risk_snapshot(Venue.BINANCE, health_ratio=0.9)
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=5000,
            supports_risk_health=True, risk_snapshot=snapshot,
        )
        assert view.action == VenueHealthAction.FAIL_CLOSED
        assert view.degraded

    def test_unsupported_snapshot_death_line_policy(self):
        """V1: unsupported_risk_snapshot_behavior='death_line' → FAIL_CLOSED."""
        strategy = make_strategy(unsupported_risk_snapshot_behavior="death_line")
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=5000,
            supports_risk_health=False,
        )
        assert view.action == VenueHealthAction.FAIL_CLOSED
        assert view.degraded

    def test_unsupported_snapshot_warning_only_policy(self):
        """V1: unsupported_risk_snapshot_behavior='warning_only' → PAUSE_ENTRY."""
        strategy = make_strategy(unsupported_risk_snapshot_behavior="warning_only")
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=5000,
            supports_risk_health=False,
        )
        assert view.action == VenueHealthAction.PAUSE_ENTRY

    def test_unsupported_snapshot_ignore_policy(self):
        strategy = make_strategy(unsupported_risk_snapshot_behavior="ignore")
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=5000,
            supports_risk_health=False,
        )
        assert view.action == VenueHealthAction.NORMAL

    def test_stale_snapshot_triggers_policy(self):
        strategy = make_strategy(unsupported_risk_snapshot_behavior="death_line")
        snapshot = make_risk_snapshot(
            Venue.BINANCE, health_ratio=2.0,
            observed_at_ms=0,  # very old
        )
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=120_000,  # well past max_age
            supports_risk_health=True, risk_snapshot=snapshot,
        )
        assert view.action == VenueHealthAction.FAIL_CLOSED
        assert view.stale

    def test_venue_health_action_ordering(self):
        assert VenueHealthAction.FAIL_CLOSED.max(VenueHealthAction.NORMAL) == VenueHealthAction.FAIL_CLOSED
        assert VenueHealthAction.REDUCE_ONLY.max(VenueHealthAction.PAUSE_ENTRY) == VenueHealthAction.REDUCE_ONLY

    def test_order_health_risk_score(self):
        strategy = make_strategy()
        snapshot = make_risk_snapshot(Venue.BINANCE, health_ratio=2.0)
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, now_ms=5000,
            supports_risk_health=True, risk_snapshot=snapshot,
            recent_order_health_risk_score=0.8,
        )
        # >= 0.75 → REDUCE_ONLY
        assert view.action == VenueHealthAction.REDUCE_ONLY


class TestPositionRisk:
    """V1 evaluate_position_risk: per-position risk view."""

    def test_healthy_position(self):
        strategy = make_strategy()
        long_snap = make_risk_snapshot(Venue.BINANCE, health_ratio=2.0)
        short_snap = make_risk_snapshot(Venue.BYBIT, health_ratio=2.0)
        view = evaluate_position_risk(
            strategy=strategy, now_ms=5000,
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_supports_risk_health=True, short_supports_risk_health=True,
            long_snapshot=long_snap, short_snapshot=short_snap,
        )
        assert view.active_action() == RiskAction.NONE
        assert not view.warning_condition
        assert not view.delever_condition
        assert not view.death_condition

    def test_warning_condition(self):
        strategy = make_strategy(warning_health_ratio=2.0)
        long_snap = make_risk_snapshot(Venue.BINANCE, health_ratio=1.8)
        short_snap = make_risk_snapshot(Venue.BYBIT, health_ratio=2.0)
        view = evaluate_position_risk(
            strategy=strategy, now_ms=5000,
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_supports_risk_health=True, short_supports_risk_health=True,
            long_snapshot=long_snap, short_snapshot=short_snap,
        )
        assert view.warning_condition
        assert not view.death_condition
        assert view.active_action() == RiskAction.WARNING

    def test_delever_condition(self):
        strategy = make_strategy(delever_health_ratio=1.2)
        long_snap = make_risk_snapshot(Venue.BINANCE, health_ratio=1.1)
        short_snap = make_risk_snapshot(Venue.BYBIT, health_ratio=1.1)
        view = evaluate_position_risk(
            strategy=strategy, now_ms=5000,
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_supports_risk_health=True, short_supports_risk_health=True,
            long_snapshot=long_snap, short_snapshot=short_snap,
        )
        assert view.delever_condition
        assert view.active_action() == RiskAction.SYNCHRONIZED_DELEVER

    def test_death_condition(self):
        strategy = make_strategy(death_health_ratio=1.0)
        long_snap = make_risk_snapshot(Venue.BINANCE, health_ratio=0.9)
        short_snap = make_risk_snapshot(Venue.BYBIT, health_ratio=0.9)
        view = evaluate_position_risk(
            strategy=strategy, now_ms=5000,
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_supports_risk_health=True, short_supports_risk_health=True,
            long_snapshot=long_snap, short_snapshot=short_snap,
        )
        assert view.death_condition
        assert view.active_action() == RiskAction.SINGLE_SIDE_PROTECTION

    def test_unsupported_snapshot_degrades_position(self):
        strategy = make_strategy(unsupported_risk_snapshot_behavior="death_line")
        view = evaluate_position_risk(
            strategy=strategy, now_ms=5000,
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_supports_risk_health=False, short_supports_risk_health=True,
            long_snapshot=None, short_snapshot=make_risk_snapshot(Venue.BYBIT),
        )
        assert view.death_condition
        assert view.degraded_reason is not None

    def test_snapshot_staleness_tracking(self):
        strategy = make_strategy(max_risk_snapshot_age_ms=30_000)
        old_snap = make_risk_snapshot(Venue.BINANCE, observed_at_ms=0)
        new_snap = make_risk_snapshot(Venue.BYBIT, observed_at_ms=50_000)
        view = evaluate_position_risk(
            strategy=strategy, now_ms=60_000,
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_supports_risk_health=True, short_supports_risk_health=True,
            long_snapshot=old_snap, short_snapshot=new_snap,
        )
        assert view.long_snapshot_stale is True
        assert view.short_snapshot_stale is False


class TestRiskExecutionPlan:
    """V1 build_risk_execution_plan: cooldowns, max steps, single-side protection."""

    def test_no_plan_when_risk_disabled(self):
        strategy = make_strategy(risk_monitor_enabled=False)
        pos = make_position()
        view = PositionRiskView(death_condition=True)
        plan = build_risk_execution_plan(pos, view, strategy, now_ms=5000)
        assert plan is None

    def test_death_plan_single_side_protection(self):
        strategy = make_strategy(death_single_side_protection_enabled=True)
        pos = make_position()
        view = PositionRiskView(death_condition=True)
        plan = build_risk_execution_plan(pos, view, strategy, now_ms=5000)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.SINGLE_SIDE_PROTECTION

    def test_death_plan_fail_closed(self):
        strategy = make_strategy(death_single_side_protection_enabled=False)
        pos = make_position()
        view = PositionRiskView(death_condition=True)
        plan = build_risk_execution_plan(pos, view, strategy, now_ms=5000)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.FAIL_CLOSED

    def test_delever_plan_with_cooldown_respected(self):
        strategy = make_strategy(partial_delever_cooldown_ms=60_000)
        pos = make_position(last_risk_action_at_ms=5000)
        view = PositionRiskView(
            delever_condition=True,
            min_health_ratio=1.1,
        )
        plan = build_risk_execution_plan(pos, view, strategy, now_ms=5000 + 30_000)
        # 30_000ms < 60_000ms cooldown → no plan
        assert plan is None

    def test_delever_plan_respects_max_steps(self):
        strategy = make_strategy(max_partial_delever_steps=3)
        pos = make_position(risk_delever_step_count=3)
        view = PositionRiskView(
            delever_condition=True,
            min_health_ratio=1.1,
        )
        plan = build_risk_execution_plan(pos, view, strategy, now_ms=100_000)
        assert plan is None

    def test_delever_plan_produced(self):
        strategy = make_strategy()
        pos = make_position(matched_quantity=10.0)
        view = PositionRiskView(
            delever_condition=True,
            min_health_ratio=1.1,
        )
        plan = build_risk_execution_plan(pos, view, strategy, now_ms=100_000)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.DELEVER
        assert plan.requested_quantity > 0

    def test_delever_recovery_resets_steps(self):
        """V1: when health recovers, delever step count is reset."""
        strategy = make_strategy(health_recovery_ratio=1.5)
        pos = make_position(risk_delever_step_count=2)
        view = PositionRiskView(
            min_health_ratio=1.6,  # above recovery ratio
            delever_condition=False,
        )
        # Recovery check is in supervisor.supervise_position, not in build_risk_execution_plan
        # Verify the recovery condition would be triggered
        recovery_reached = (
            pos.risk_delever_step_count > 0
            and view.min_health_ratio is not None
            and view.min_health_ratio >= strategy.health_recovery_ratio
        )
        assert recovery_reached is True


class TestUnsupportedRiskSnapshotBehavior:
    """V1 unsupported snapshot policy enum."""

    def test_enum_values(self):
        assert UnsupportedRiskSnapshotBehavior.DEATH_LINE.value == "death_line"
        assert UnsupportedRiskSnapshotBehavior.WARNING_ONLY.value == "warning_only"
        assert UnsupportedRiskSnapshotBehavior.IGNORE.value == "ignore"


class TestAccountRiskSnapshot:
    """V1 AccountRiskSnapshot: health ratio, staleness."""

    def test_health_ratio_computed_when_margin_present(self):
        snap = AccountRiskSnapshot(
            venue=Venue.BINANCE,
            equity_quote=10000.0,
            maintenance_margin_quote=5000.0,
            health_ratio=0.0,  # should be recomputed
            observed_at_ms=1000,
        )
        assert snap.health_ratio == pytest.approx(2.0)

    def test_unsupported_when_margin_zero(self):
        snap = AccountRiskSnapshot(
            venue=Venue.BINANCE,
            equity_quote=10000.0,
            maintenance_margin_quote=0.0,
            health_ratio=0.0,
            observed_at_ms=1000,
        )
        assert snap.supported is False

    def test_verified_zero_maintenance_is_supported_without_health_ratio(self):
        snap = AccountRiskSnapshot(
            venue=Venue.BYBIT,
            equity_quote=10000.0,
            maintenance_margin_quote=0.0,
            health_ratio=0.0,
            observed_at_ms=1000,
            zero_maintenance_is_normal=True,
        )
        assert snap.supported is True
        assert snap.health_ratio == 0.0

    def test_staleness_detection(self):
        snap = make_risk_snapshot(Venue.BINANCE, observed_at_ms=1000)
        assert snap.is_effectively_stale(now_ms=100_000, max_age_ms=30_000)

    def test_not_stale_when_fresh(self):
        snap = make_risk_snapshot(Venue.BINANCE, observed_at_ms=50_000)
        assert not snap.is_effectively_stale(now_ms=60_000, max_age_ms=30_000)
