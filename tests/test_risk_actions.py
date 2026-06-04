"""Task 6: Risk action contract tests.

Rust references:
- src/risk.rs: evaluate_position_risk (line 114), PositionRiskView, RiskAction
- src/health.rs: evaluate_venue_health (line 60), VenueHealthAction
- src/engine/risk.rs: RiskExecutionPlan (line 42), manage_open_position (line 1255)
"""

from __future__ import annotations

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import Side, Venue
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _strategy(**overrides) -> StrategyConfig:
    defaults = dict(
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
    defaults.update(overrides)
    return StrategyConfig(**defaults)


# ---------------------------------------------------------------------------
# VenueHealthAction
# ---------------------------------------------------------------------------


class TestVenueHealthAction:
    def test_ordering(self):
        assert VenueHealthAction.NORMAL.max(VenueHealthAction.PAUSE_ENTRY) == VenueHealthAction.PAUSE_ENTRY
        assert VenueHealthAction.PAUSE_ENTRY.max(VenueHealthAction.REDUCE_ONLY) == VenueHealthAction.REDUCE_ONLY
        assert VenueHealthAction.REDUCE_ONLY.max(VenueHealthAction.FAIL_CLOSED) == VenueHealthAction.FAIL_CLOSED
        assert VenueHealthAction.FAIL_CLOSED.max(VenueHealthAction.NORMAL) == VenueHealthAction.FAIL_CLOSED


# ---------------------------------------------------------------------------
# evaluate_venue_health
# ---------------------------------------------------------------------------


class TestEvaluateVenueHealth:
    def test_normal_when_healthy(self):
        strategy = _strategy()
        snap = _snapshot(Venue.BINANCE, 500.0, 100.0, 10000)
        view = evaluate_venue_health(strategy, Venue.BINANCE, 10000, True, snap)
        assert view.action == VenueHealthAction.NORMAL
        assert view.health_ratio == 5.0
        assert not view.degraded

    def test_below_warning_triggers_pause_entry(self):
        strategy = _strategy()
        snap = _snapshot(Venue.BINANCE, 250.0, 100.0, 10000)  # ratio = 2.5 <= 3.0
        view = evaluate_venue_health(strategy, Venue.BINANCE, 10000, True, snap)
        assert view.action == VenueHealthAction.PAUSE_ENTRY
        assert view.degraded

    def test_below_delever_triggers_reduce_only(self):
        strategy = _strategy()
        snap = _snapshot(Venue.BYBIT, 150.0, 120.0, 10000)  # ratio = 1.25 <= 1.5
        view = evaluate_venue_health(strategy, Venue.BYBIT, 10000, True, snap)
        assert view.action == VenueHealthAction.REDUCE_ONLY

    def test_below_death_triggers_fail_closed(self):
        strategy = _strategy()
        snap = _snapshot(Venue.OKX, 100.0, 100.0, 10000)  # ratio = 1.0 <= 1.1
        view = evaluate_venue_health(strategy, Venue.OKX, 10000, True, snap)
        assert view.action == VenueHealthAction.FAIL_CLOSED

    def test_snapshot_unavailable_with_death_line_policy(self):
        strategy = _strategy(unsupported_risk_snapshot_behavior="death_line")
        view = evaluate_venue_health(strategy, Venue.BINANCE, 10000, True, None)
        assert view.action == VenueHealthAction.FAIL_CLOSED
        assert view.degraded
        assert "risk_snapshot_unavailable" in view.reasons

    def test_snapshot_unavailable_with_warning_only_policy(self):
        strategy = _strategy(unsupported_risk_snapshot_behavior="warning_only")
        view = evaluate_venue_health(strategy, Venue.BINANCE, 10000, True, None)
        assert view.action == VenueHealthAction.PAUSE_ENTRY
        assert view.degraded

    def test_snapshot_unavailable_with_ignore_policy(self):
        strategy = _strategy(unsupported_risk_snapshot_behavior="ignore")
        view = evaluate_venue_health(strategy, Venue.BINANCE, 10000, True, None)
        assert view.action == VenueHealthAction.NORMAL

    def test_stale_snapshot_triggers_policy(self):
        strategy = _strategy(max_risk_snapshot_age_ms=1000, unsupported_risk_snapshot_behavior="death_line")
        snap = _snapshot(Venue.BINANCE, 500.0, 100.0, 1000)
        view = evaluate_venue_health(strategy, Venue.BINANCE, 5000, True, snap)
        assert view.action == VenueHealthAction.FAIL_CLOSED
        assert view.stale

    def test_unsupported_snapshot_with_warning_only(self):
        strategy = _strategy(unsupported_risk_snapshot_behavior="warning_only")
        snap = AccountRiskSnapshot(
            venue=Venue.OKX, equity_quote=10.0, maintenance_margin_quote=0.0,
            health_ratio=0.0, observed_at_ms=1000, source="test",
            supported=False, stale=False,
        )
        view = evaluate_venue_health(strategy, Venue.OKX, 1000, True, snap)
        assert view.action == VenueHealthAction.PAUSE_ENTRY

    def test_high_order_health_risk_forces_reduce_only(self):
        strategy = _strategy()
        snap = _snapshot(Venue.BINANCE, 500.0, 100.0, 10000)
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, 10000, True, snap,
            recent_order_health_risk_score=0.8,
        )
        assert view.action == VenueHealthAction.REDUCE_ONLY

    def test_elevated_order_health_risk_forces_pause_entry(self):
        strategy = _strategy()
        snap = _snapshot(Venue.BINANCE, 500.0, 100.0, 10000)
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, 10000, True, snap,
            recent_order_health_risk_score=0.5,
        )
        assert view.action == VenueHealthAction.PAUSE_ENTRY

    def test_no_risk_health_capability_applies_policy(self):
        strategy = _strategy(unsupported_risk_snapshot_behavior="death_line")
        view = evaluate_venue_health(strategy, Venue.BINANCE, 10000, False, None)
        assert view.action == VenueHealthAction.FAIL_CLOSED
        assert "risk_snapshot_unavailable" in view.reasons

    def test_order_health_piles_on_top_of_health_ratio(self):
        strategy = _strategy()
        snap = _snapshot(Venue.BINANCE, 250.0, 100.0, 10000)  # 2.5 → warning → PauseEntry
        view = evaluate_venue_health(
            strategy, Venue.BINANCE, 10000, True, snap,
            recent_order_health_risk_score=0.8,  # ReduceOnly
        )
        assert view.action == VenueHealthAction.REDUCE_ONLY  # max(PauseEntry, ReduceOnly)


# ---------------------------------------------------------------------------
# AccountRiskSnapshot
# ---------------------------------------------------------------------------


class TestAccountRiskSnapshot:
    def test_unsupported_when_margin_zero(self):
        snap = _snapshot(Venue.BINANCE, 100.0, 0.0)
        assert not snap.supported
        assert snap.health_ratio == 0.0

    def test_unsupported_when_margin_nan(self):
        snap = _snapshot(Venue.BINANCE, 100.0, float("nan"))
        assert not snap.supported

    def test_age_ms(self):
        snap = _snapshot(Venue.BINANCE, 100.0, 50.0, 10000)
        assert snap.age_ms(15000) == 5000
        assert snap.age_ms(5000) == 0

    def test_effectively_stale(self):
        snap = _snapshot(Venue.BINANCE, 100.0, 50.0, 10000)
        assert snap.is_effectively_stale(50000, 30000)
        assert not snap.is_effectively_stale(20000, 30000)
        snap.stale = True
        assert snap.is_effectively_stale(10001, 30000)


# ---------------------------------------------------------------------------
# evaluate_position_risk
# ---------------------------------------------------------------------------


class TestEvaluatePositionRisk:
    def test_warning_condition_for_low_health(self):
        """Rust test: warning_condition_triggers_for_low_health."""
        strategy = _strategy()
        long_snap = _snapshot(Venue.BINANCE, 300.0, 120.0, 10000)  # 2.5
        short_snap = _snapshot(Venue.OKX, 500.0, 100.0, 10000)  # 5.0
        view = evaluate_position_risk(
            strategy, 10000,
            Venue.BINANCE, Venue.OKX,
            True, True,
            long_snap, short_snap,
        )
        assert view.min_health_ratio == 2.5
        assert view.warning_condition
        assert not view.delever_condition
        assert view.active_action() == RiskAction.WARNING

    def test_delever_condition(self):
        strategy = _strategy()
        long_snap = _snapshot(Venue.BINANCE, 140.0, 100.0, 10000)  # 1.4 <= 1.5
        short_snap = _snapshot(Venue.OKX, 500.0, 100.0, 10000)  # 5.0
        view = evaluate_position_risk(
            strategy, 10000,
            Venue.BINANCE, Venue.OKX,
            True, True,
            long_snap, short_snap,
        )
        assert view.min_health_ratio == 1.4
        assert view.delever_condition
        assert view.active_action() == RiskAction.SYNCHRONIZED_DELEVER

    def test_death_condition(self):
        strategy = _strategy()
        long_snap = _snapshot(Venue.BINANCE, 105.0, 100.0, 10000)  # 1.05 <= 1.1
        short_snap = _snapshot(Venue.OKX, 500.0, 100.0, 10000)  # 5.0
        view = evaluate_position_risk(
            strategy, 10000,
            Venue.BINANCE, Venue.OKX,
            True, True,
            long_snap, short_snap,
        )
        assert view.death_condition
        assert view.active_action() == RiskAction.SINGLE_SIDE_PROTECTION

    def test_stale_snapshot_forces_death_condition(self):
        """Rust test: stale_snapshot_forces_death_condition."""
        strategy = _strategy(max_risk_snapshot_age_ms=1000)
        long_snap = _snapshot(Venue.BINANCE, 300.0, 120.0, 1000)
        short_snap = _snapshot(Venue.OKX, 500.0, 100.0, 1000)
        view = evaluate_position_risk(
            strategy, 5000,
            Venue.BINANCE, Venue.OKX,
            True, True,
            long_snap, short_snap,
        )
        assert view.death_condition
        assert view.degraded_reason == "long_snapshot_stale"
        assert view.active_action() == RiskAction.SINGLE_SIDE_PROTECTION

    def test_stale_snapshot_can_degrade_to_warning_only(self):
        """Rust test: stale_snapshot_can_degrade_to_warning_only."""
        strategy = _strategy(
            max_risk_snapshot_age_ms=1000,
            unsupported_risk_snapshot_behavior="warning_only",
        )
        long_snap = _snapshot(Venue.BINANCE, 300.0, 120.0, 1000)
        short_snap = _snapshot(Venue.OKX, 500.0, 100.0, 1000)
        view = evaluate_position_risk(
            strategy, 5000,
            Venue.BINANCE, Venue.OKX,
            True, True,
            long_snap, short_snap,
        )
        assert view.warning_condition
        assert not view.delever_condition
        assert not view.death_condition
        assert view.active_action() == RiskAction.WARNING

    def test_healthy_position_no_action(self):
        strategy = _strategy()
        long_snap = _snapshot(Venue.BINANCE, 500.0, 100.0, 10000)  # 5.0
        short_snap = _snapshot(Venue.OKX, 600.0, 100.0, 10000)  # 6.0
        view = evaluate_position_risk(
            strategy, 10000,
            Venue.BINANCE, Venue.OKX,
            True, True,
            long_snap, short_snap,
        )
        assert view.active_action() == RiskAction.NONE

    def test_ignore_behavior_passes_through_health(self):
        strategy = _strategy(
            max_risk_snapshot_age_ms=1000,
            unsupported_risk_snapshot_behavior="ignore",
        )
        long_snap = _snapshot(Venue.BINANCE, 300.0, 120.0, 1000)  # 2.5
        short_snap = _snapshot(Venue.OKX, 500.0, 100.0, 1000)  # 5.0
        view = evaluate_position_risk(
            strategy, 5000,
            Venue.BINANCE, Venue.OKX,
            True, True,
            long_snap, short_snap,
        )
        # Stale but ignored → health-based: 2.5 <= 3.0 → warning only
        assert view.warning_condition
        assert not view.delever_condition
        assert not view.death_condition

    def test_no_snapshot_unavailable_degraded(self):
        strategy = _strategy()
        view = evaluate_position_risk(
            strategy, 10000,
            Venue.BINANCE, Venue.OKX,
            True, True,
            None, None,
        )
        assert view.degraded_reason == "long_snapshot_unavailable"
        assert view.death_condition  # death_line behavior

    def test_unsupported_capability_degraded(self):
        strategy = _strategy()
        view = evaluate_position_risk(
            strategy, 10000,
            Venue.BINANCE, Venue.OKX,
            False, True,
            None,
            _snapshot(Venue.OKX, 500.0, 100.0, 10000),
        )
        assert view.degraded_reason == "long_snapshot_unavailable"
        assert view.death_condition


# ---------------------------------------------------------------------------
# build_risk_execution_plan
# ---------------------------------------------------------------------------


class TestBuildRiskExecutionPlan:
    def test_death_line_returns_single_side_protection(self):
        strategy = _strategy()
        pos = _make_position()
        risk_view = PositionRiskView(
            death_condition=True,
            degraded_reason="test_death",
            degraded_venue=pos.long_venue,
        )
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.SINGLE_SIDE_PROTECTION
        assert plan.reason == "test_death"
        assert plan.protection_venue == pos.short_venue
        assert plan.protection_side == Side.BUY
        assert plan.protection_stage == "risk_protection_short"

    def test_death_line_selects_weaker_healthy_leg_when_no_degraded_venue(self):
        strategy = _strategy()
        pos = _make_position()
        risk_view = PositionRiskView(
            death_condition=True,
            long_health_ratio=1.05,
            short_health_ratio=1.4,
            min_health_ratio=1.05,
        )
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.SINGLE_SIDE_PROTECTION
        assert plan.protection_venue == pos.long_venue
        assert plan.protection_side == Side.SELL
        assert plan.protection_stage == "risk_protection_long"

    def test_death_line_without_single_side_protection_returns_fail_closed(self):
        strategy = _strategy(death_single_side_protection_enabled=False)
        pos = _make_position()
        risk_view = PositionRiskView(death_condition=True)
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.FAIL_CLOSED

    def test_death_line_disabled_does_not_trigger(self):
        strategy = _strategy(death_line_enabled=False)
        pos = _make_position()
        risk_view = PositionRiskView(
            death_condition=True,
            delever_condition=True,
        )
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        # Falls through to delever since death is disabled
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.DELEVER

    def test_delever_returns_delever_plan(self):
        strategy = _strategy()
        pos = _make_position(matched_quantity=0.01)
        risk_view = PositionRiskView(
            delever_condition=True,
            min_health_ratio=1.4,
        )
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.DELEVER
        assert plan.reason == "risk_delever"
        assert plan.requested_quantity == pytest.approx(0.002)  # 0.01 * 0.2

    def test_delever_respects_cooldown(self):
        strategy = _strategy()
        pos = _make_position(
            matched_quantity=0.01,
            last_risk_action_at_ms=10000,
        )
        risk_view = PositionRiskView(
            delever_condition=True,
            min_health_ratio=1.4,
        )
        # now_ms = 20000, cooldown = 30000 → still in cooldown
        plan = build_risk_execution_plan(pos, risk_view, strategy, 20000)
        assert plan is None

    def test_delever_respects_max_steps(self):
        strategy = _strategy(max_partial_delever_steps=4)
        pos = _make_position(
            matched_quantity=0.01,
            risk_delever_step_count=4,
        )
        risk_view = PositionRiskView(
            delever_condition=True,
            min_health_ratio=1.4,
        )
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is None

    def test_delever_respects_zero_max_steps(self):
        """max_partial_delever_steps=0 means unlimited."""
        strategy = _strategy(max_partial_delever_steps=0)
        pos = _make_position(
            matched_quantity=0.01,
            risk_delever_step_count=100,
        )
        risk_view = PositionRiskView(
            delever_condition=True,
            min_health_ratio=1.4,
        )
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.DELEVER

    def test_no_action_when_healthy(self):
        strategy = _strategy()
        pos = _make_position()
        risk_view = PositionRiskView()  # all conditions False
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is None

    def test_risk_monitor_disabled_returns_none(self):
        strategy = _strategy(risk_monitor_enabled=False)
        pos = _make_position()
        risk_view = PositionRiskView(death_condition=True)
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is None

    def test_delever_line_disabled_no_delever(self):
        strategy = _strategy(delever_line_enabled=False)
        pos = _make_position(matched_quantity=0.01)
        risk_view = PositionRiskView(delever_condition=True)
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is None

    def test_delever_auto_execute_disabled_no_delever(self):
        strategy = _strategy(delever_auto_execute_enabled=False)
        pos = _make_position(matched_quantity=0.01)
        risk_view = PositionRiskView(delever_condition=True)
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        assert plan is None

    def test_delever_regime_active_after_previous_steps(self):
        """Delever stays active if min_health still below recovery even if not delever_condition."""
        strategy = _strategy()
        pos = _make_position(
            matched_quantity=0.01,
            risk_delever_step_count=1,
        )
        # health 1.8 is below recovery (2.0) but above delever (1.5)
        risk_view = PositionRiskView(
            delever_condition=False,
            min_health_ratio=1.8,
        )
        plan = build_risk_execution_plan(pos, risk_view, strategy, 5000)
        # min_health(1.8) < health_recovery_ratio(2.0) AND risk_delever_step_count > 0
        # → delever_regime_active = True
        assert plan is not None
        assert plan.kind == RiskExecutionPlanKind.DELEVER
