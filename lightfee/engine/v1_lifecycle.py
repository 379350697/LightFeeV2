from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lightfee.engine.funding_lifecycle import FundingLifecycle


@dataclass(frozen=True)
class LifecycleDecision:
    allowed: bool
    reason: str = ""
    evidence: dict = field(default_factory=dict)


class V1TradingLifecycle:
    @classmethod
    def entry_admissibility(
        cls,
        candidate: Any,
        *,
        now_ms: int,
        strategy: Any,
        recovery_ledger: Any = None,
        source: str = "candidate",
    ) -> LifecycleDecision:
        horizon = FundingLifecycle.entry_horizon(
            candidate,
            now_ms,
            strategy,
            source=source,
        )
        if not horizon.allowed:
            return LifecycleDecision(
                allowed=False,
                reason=horizon.reason,
                evidence={
                    "first_funding_timestamp_ms": horizon.first_funding_timestamp_ms,
                    "remaining_to_first_funding_ms": horizon.remaining_to_first_funding_ms,
                    "effective_min_before_ms": horizon.effective_min_before_ms,
                    "source": source,
                },
            )
        return LifecycleDecision(
            allowed=True,
            evidence={
                "first_funding_timestamp_ms": horizon.first_funding_timestamp_ms,
                "remaining_to_first_funding_ms": horizon.remaining_to_first_funding_ms,
                "effective_min_before_ms": horizon.effective_min_before_ms,
                "source": source,
            },
        )

    @classmethod
    def pending_entry_viability(
        cls,
        pending: Any,
        *,
        now_ms: int,
        strategy: Any,
    ) -> LifecycleDecision:
        maker_filled = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
        hedge_filled = float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0)
        has_positive = maker_filled > 0.0 or hedge_filled > 0.0
        horizon = FundingLifecycle.entry_horizon(
            pending,
            now_ms,
            strategy,
            source="pending_entry",
        )
        evidence = {
            "first_funding_timestamp_ms": horizon.first_funding_timestamp_ms,
            "remaining_to_first_funding_ms": horizon.remaining_to_first_funding_ms,
            "effective_min_before_ms": horizon.effective_min_before_ms,
            "source": "pending_entry",
        }
        if not horizon.allowed and has_positive:
            return LifecycleDecision(
                allowed=True,
                reason="pending_entry_terminality_positive_fill_recovery",
                evidence=evidence,
            )
        if not horizon.allowed:
            return LifecycleDecision(
                allowed=False,
                reason="pending_entry_viability_first_funding_too_close",
                evidence=evidence,
            )
        return LifecycleDecision(allowed=True, evidence=evidence)
