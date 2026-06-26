"""Spread-reversion signal and trading-control primitives."""

from lightfee.spread.controller import SpreadTradingController, SpreadTradingState
from lightfee.spread.execution_plan import (
    SpreadExecutionPlan,
    SpreadExecutionPlanError,
    SpreadExecutionPlanner,
)
from lightfee.spread.models import (
    SpreadOrderIntent,
    SpreadPosition,
    SpreadReversionCandidate,
    SpreadSnapshot,
)
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadStatsTracker,
    build_spread_reversion_candidates,
)

__all__ = [
    "SpreadOrderIntent",
    "SpreadPosition",
    "SpreadExecutionPlan",
    "SpreadExecutionPlanError",
    "SpreadExecutionPlanner",
    "SpreadReversionCandidate",
    "SpreadReversionConfig",
    "SpreadSnapshot",
    "SpreadStatsTracker",
    "SpreadTradingController",
    "SpreadTradingState",
    "build_spread_reversion_candidates",
]
