"""V1 risk action planner: evaluate position risk and build execution plans.

Rust references:
- src/risk.rs: evaluate_position_risk (line 114), PositionRiskView, RiskAction, AccountRiskSnapshot
- src/health.rs: evaluate_venue_health (line 60), VenueHealthAction, VenueHealthView
- src/engine/risk.rs: manage_open_position (line 1255), RiskExecutionPlan
- src/runtime_state/config.rs: UnsupportedRiskSnapshotBehavior (line 843)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import Venue
from lightfee.engine.state import OpenPosition


# ---------------------------------------------------------------------------
# Unsupported risk snapshot behavior
# ---------------------------------------------------------------------------


class UnsupportedRiskSnapshotBehavior(Enum):
    """V1 UnsupportedRiskSnapshotBehavior (config.rs line 843)."""
    DEATH_LINE = "death_line"
    WARNING_ONLY = "warning_only"
    IGNORE = "ignore"


# ---------------------------------------------------------------------------
# Venue health action (per-venue severity)
# ---------------------------------------------------------------------------


class VenueHealthAction(Enum):
    """V1 VenueHealthAction (health.rs line 11): per-venue health severity.

    Ordered by severity: Normal < PauseEntry < ReduceOnly < FailClosed.
    """
    NORMAL = "normal"
    PAUSE_ENTRY = "pause_entry"
    REDUCE_ONLY = "reduce_only"
    FAIL_CLOSED = "fail_closed"

    def max(self, other: VenueHealthAction) -> VenueHealthAction:
        order = {
            VenueHealthAction.NORMAL: 0,
            VenueHealthAction.PAUSE_ENTRY: 1,
            VenueHealthAction.REDUCE_ONLY: 2,
            VenueHealthAction.FAIL_CLOSED: 3,
        }
        return self if order[self] >= order[other] else other


# ---------------------------------------------------------------------------
# Venue health view
# ---------------------------------------------------------------------------


@dataclass
class VenueHealthView:
    """V1 VenueHealthView (health.rs line 42)."""
    venue: Venue
    action: VenueHealthAction = VenueHealthAction.NORMAL
    health_ratio: Optional[float] = None
    degraded: bool = False
    stale: bool = False
    supported: bool = False
    order_health_risk_score: float = 0.0
    reasons: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Account risk snapshot
# ---------------------------------------------------------------------------


@dataclass
class AccountRiskSnapshot:
    """V1 AccountRiskSnapshot (risk.rs line 10)."""
    venue: Venue
    equity_quote: float
    maintenance_margin_quote: float
    health_ratio: float
    observed_at_ms: int
    source: str = ""
    available_balance_quote: Optional[float] = None
    supported: bool = True
    stale: bool = False

    def __post_init__(self) -> None:
        if not (self.maintenance_margin_quote > 0 and self.maintenance_margin_quote == self.maintenance_margin_quote):
            self.supported = False
            self.health_ratio = 0.0
        elif self.supported and self.health_ratio == 0.0:
            self.health_ratio = self.equity_quote / self.maintenance_margin_quote

    def age_ms(self, now_ms: int) -> int:
        return max(now_ms - self.observed_at_ms, 0)

    def is_effectively_stale(self, now_ms: int, max_age_ms: int) -> bool:
        return self.stale or (max_age_ms > 0 and self.age_ms(now_ms) > max_age_ms)


# ---------------------------------------------------------------------------
# Position risk view
# ---------------------------------------------------------------------------


class RiskAction(Enum):
    """V1 RiskAction (risk.rs line 63)."""
    NONE = "none"
    WARNING = "warning"
    SYNCHRONIZED_DELEVER = "synchronized_delever"
    SINGLE_SIDE_PROTECTION = "single_side_protection"


class RiskExecutionPlanKind(Enum):
    """V1 RiskExecutionPlan variants (engine/risk.rs line 42)."""
    DELEVER = "delever"
    SINGLE_SIDE_PROTECTION = "single_side_protection"
    FAIL_CLOSED = "fail_closed"


@dataclass
class RiskExecutionPlan:
    """V1 RiskExecutionPlan (engine/risk.rs line 42)."""
    kind: RiskExecutionPlanKind
    reason: str
    # --- Delever fields ---
    requested_quantity: float = 0.0
    adjusted_quantity: float = 0.0
    long_liquidity_source: Optional[str] = None
    short_liquidity_source: Optional[str] = None
    long_slippage_bps: Optional[float] = None
    short_slippage_bps: Optional[float] = None
    capacity_constrained: bool = False


@dataclass
class PositionRiskView:
    """V1 PositionRiskView (risk.rs line 71)."""
    long_health_ratio: Optional[float] = None
    short_health_ratio: Optional[float] = None
    min_health_ratio: Optional[float] = None
    warning_condition: bool = False
    delever_condition: bool = False
    death_condition: bool = False
    long_snapshot_supported: bool = False
    short_snapshot_supported: bool = False
    long_snapshot_stale: bool = False
    short_snapshot_stale: bool = False
    risk_snapshot_age_ms: int = 0
    degraded_reason: Optional[str] = None
    degraded_venue: Optional[Venue] = None

    def active_action(self) -> RiskAction:
        if self.death_condition:
            return RiskAction.SINGLE_SIDE_PROTECTION
        elif self.delever_condition:
            return RiskAction.SYNCHRONIZED_DELEVER
        elif self.warning_condition:
            return RiskAction.WARNING
        return RiskAction.NONE


# ---------------------------------------------------------------------------
# evaluate_venue_health
# ---------------------------------------------------------------------------


def evaluate_venue_health(
    strategy: StrategyConfig,
    venue: Venue,
    now_ms: int,
    supports_risk_health: bool = True,
    risk_snapshot: Optional[AccountRiskSnapshot] = None,
    recent_order_health_risk_score: float = 0.0,
) -> VenueHealthView:
    """V1 evaluate_venue_health (health.rs line 60): per-venue health.

    Applies:
    1. Order health risk score thresholds (>= 0.75 ReduceOnly, >= 0.4 PauseEntry)
    2. Unsupported/missing/stale snapshot policy via unsupported_risk_snapshot_behavior
    3. Health ratio thresholds (death, delever, warning lines)
    """
    view = VenueHealthView(
        venue=venue,
        action=VenueHealthAction.NORMAL,
        order_health_risk_score=recent_order_health_risk_score,
    )

    # Order health risk score
    if recent_order_health_risk_score >= 0.75:
        view.action = view.action.max(VenueHealthAction.REDUCE_ONLY)
        view.degraded = True
        view.reasons.append(f"order_health_risk_high:{recent_order_health_risk_score:.3f}")
    elif recent_order_health_risk_score >= 0.4:
        view.action = view.action.max(VenueHealthAction.PAUSE_ENTRY)
        view.degraded = True
        view.reasons.append(f"order_health_risk_elevated:{recent_order_health_risk_score:.3f}")

    if not supports_risk_health:
        _apply_unsupported_snapshot_policy(strategy, view, "risk_snapshot_capability_unsupported")
        return view

    if risk_snapshot is None:
        _apply_unsupported_snapshot_policy(strategy, view, "risk_snapshot_unavailable")
        return view

    view.supported = risk_snapshot.supported
    stale = risk_snapshot.is_effectively_stale(now_ms, strategy.max_risk_snapshot_age_ms)
    view.stale = stale
    if risk_snapshot.supported and risk_snapshot.health_ratio > 0:
        view.health_ratio = risk_snapshot.health_ratio

    if not risk_snapshot.supported or stale:
        reason = "risk_snapshot_stale" if stale else "risk_snapshot_unsupported"
        _apply_unsupported_snapshot_policy(strategy, view, reason)
        return view

    if view.health_ratio is not None:
        ratio = view.health_ratio
        if ratio <= strategy.death_health_ratio:
            view.action = view.action.max(VenueHealthAction.FAIL_CLOSED)
            view.degraded = True
            view.reasons.append(f"health_below_death_line:{ratio:.4f}")
        elif ratio <= strategy.delever_health_ratio:
            view.action = view.action.max(VenueHealthAction.REDUCE_ONLY)
            view.degraded = True
            view.reasons.append(f"health_below_delever_line:{ratio:.4f}")
        elif ratio <= strategy.warning_health_ratio:
            view.action = view.action.max(VenueHealthAction.PAUSE_ENTRY)
            view.degraded = True
            view.reasons.append(f"health_below_warning_line:{ratio:.4f}")

    return view


def _apply_unsupported_snapshot_policy(
    strategy: StrategyConfig,
    view: VenueHealthView,
    reason: str,
) -> None:
    """V1 apply_unsupported_snapshot_policy (health.rs line 162)."""
    view.degraded = True
    behavior = strategy.unsupported_risk_snapshot_behavior
    if behavior == "death_line":
        view.action = view.action.max(VenueHealthAction.FAIL_CLOSED)
    elif behavior == "warning_only":
        view.action = view.action.max(VenueHealthAction.PAUSE_ENTRY)
    # "ignore" → no additional action
    view.reasons.append(reason)


# ---------------------------------------------------------------------------
# evaluate_position_risk
# ---------------------------------------------------------------------------


def evaluate_position_risk(
    strategy: StrategyConfig,
    now_ms: int,
    long_venue: Venue,
    short_venue: Venue,
    long_supports_risk_health: bool,
    short_supports_risk_health: bool,
    long_snapshot: Optional[AccountRiskSnapshot] = None,
    short_snapshot: Optional[AccountRiskSnapshot] = None,
) -> PositionRiskView:
    """V1 evaluate_position_risk (risk.rs line 114): per-position risk view.

    Evaluates snapshot staleness, support, health ratios, and derives
    warning/delever/death conditions accounting for UnsupportedRiskSnapshotBehavior.
    """
    # Staleness
    long_stale = (
        long_supports_risk_health
        and long_snapshot is not None
        and long_snapshot.is_effectively_stale(now_ms, strategy.max_risk_snapshot_age_ms)
    )
    short_stale = (
        short_supports_risk_health
        and short_snapshot is not None
        and short_snapshot.is_effectively_stale(now_ms, strategy.max_risk_snapshot_age_ms)
    )

    long_supported = (
        long_supports_risk_health
        and long_snapshot is not None
        and long_snapshot.supported
        and not long_stale
    )
    short_supported = (
        short_supports_risk_health
        and short_snapshot is not None
        and short_snapshot.supported
        and not short_stale
    )

    def _health_ratio(snapshot, supports: bool) -> Optional[float]:
        if not supports or snapshot is None:
            return None
        if snapshot.supported and snapshot.health_ratio > 0 and snapshot.health_ratio == snapshot.health_ratio:
            return snapshot.health_ratio
        return None

    long_ratio = _health_ratio(long_snapshot, long_supports_risk_health)
    short_ratio = _health_ratio(short_snapshot, short_supports_risk_health)

    valid_ratios = [r for r in (long_ratio, short_ratio) if r is not None]
    min_ratio = min(valid_ratios) if valid_ratios else None

    risk_snapshot_age_ms = max(
        long_snapshot.age_ms(now_ms) if long_snapshot else 0,
        short_snapshot.age_ms(now_ms) if short_snapshot else 0,
    )

    # Degradation detection
    degraded_reason = None
    degraded_venue = None
    if not long_supports_risk_health:
        degraded_reason = "long_snapshot_capability_unsupported"
        degraded_venue = long_venue
    elif not short_supports_risk_health:
        degraded_reason = "short_snapshot_capability_unsupported"
        degraded_venue = short_venue
    elif long_snapshot is None:
        degraded_reason = "long_snapshot_unavailable"
        degraded_venue = long_venue
    elif short_snapshot is None:
        degraded_reason = "short_snapshot_unavailable"
        degraded_venue = short_venue
    elif not long_supported:
        degraded_reason = "long_snapshot_stale" if long_stale else "long_snapshot_unsupported"
        degraded_venue = long_venue
    elif not short_supported:
        degraded_reason = "short_snapshot_stale" if short_stale else "short_snapshot_unsupported"
        degraded_venue = short_venue

    # Health-based conditions
    warning_from_health = min_ratio is not None and min_ratio <= strategy.warning_health_ratio
    delever_from_health = min_ratio is not None and min_ratio <= strategy.delever_health_ratio
    death_from_health = min_ratio is not None and min_ratio <= strategy.death_health_ratio

    if degraded_reason is not None:
        behavior = strategy.unsupported_risk_snapshot_behavior
        if behavior == "death_line":
            warning_condition = warning_from_health
            delever_condition = delever_from_health
            death_condition = True
        elif behavior == "warning_only":
            warning_condition = True
            delever_condition = False
            death_condition = False
        else:  # ignore
            warning_condition = warning_from_health
            delever_condition = delever_from_health
            death_condition = death_from_health
    else:
        warning_condition = warning_from_health
        delever_condition = delever_from_health
        death_condition = death_from_health

    return PositionRiskView(
        long_health_ratio=long_ratio,
        short_health_ratio=short_ratio,
        min_health_ratio=min_ratio,
        warning_condition=warning_condition,
        delever_condition=delever_condition,
        death_condition=death_condition,
        long_snapshot_supported=long_supported,
        short_snapshot_supported=short_supported,
        long_snapshot_stale=long_stale,
        short_snapshot_stale=short_stale,
        risk_snapshot_age_ms=risk_snapshot_age_ms,
        degraded_reason=degraded_reason,
        degraded_venue=degraded_venue,
    )


# ---------------------------------------------------------------------------
# Risk action planner: convert risk view + position state → execution plan
# ---------------------------------------------------------------------------


def build_risk_execution_plan(
    position: OpenPosition,
    risk_view: PositionRiskView,
    strategy: StrategyConfig,
    now_ms: int,
) -> Optional[RiskExecutionPlan]:
    """V1 manage_open_position risk planning (engine/risk.rs line 1482-1571).

    Returns a RiskExecutionPlan or None if no risk action is needed.
    Applies all V1 gating: risk_monitor_enabled, line enables, cooldowns,
    step limits, recovery thresholds.
    """
    if not strategy.risk_monitor_enabled:
        return None

    # Recovery check: if health recovers above threshold, reset delever state
    delever_recovery_reached = (
        position.risk_delever_step_count > 0
        and risk_view.min_health_ratio is not None
        and risk_view.min_health_ratio >= strategy.health_recovery_ratio
    )
    delever_regime_active = risk_view.delever_condition or (
        position.risk_delever_step_count > 0
        and risk_view.min_health_ratio is not None
        and risk_view.min_health_ratio < strategy.health_recovery_ratio
    )

    # Death line (takes priority over delever)
    if risk_view.death_condition and strategy.death_line_enabled:
        reason = risk_view.degraded_reason or "death_line_health_breach"
        if strategy.death_single_side_protection_enabled:
            return RiskExecutionPlan(
                kind=RiskExecutionPlanKind.SINGLE_SIDE_PROTECTION,
                reason=reason,
            )
        else:
            return RiskExecutionPlan(
                kind=RiskExecutionPlanKind.FAIL_CLOSED,
                reason=reason,
            )

    # Delever line
    if (
        delever_regime_active
        and strategy.delever_line_enabled
        and strategy.delever_auto_execute_enabled
        and position.matched_quantity > 0
        and (
            strategy.max_partial_delever_steps == 0
            or position.risk_delever_step_count < strategy.max_partial_delever_steps
        )
        and not (
            position.last_risk_action_at_ms > 0
            and now_ms < position.last_risk_action_at_ms + strategy.partial_delever_cooldown_ms
        )
    ):
        requested_quantity = (
            position.matched_quantity * strategy.partial_delever_ratio
        )
        if requested_quantity <= 0:
            return None

        return RiskExecutionPlan(
            kind=RiskExecutionPlanKind.DELEVER,
            reason="risk_delever",
            requested_quantity=requested_quantity,
            adjusted_quantity=requested_quantity,
        )

    return None
