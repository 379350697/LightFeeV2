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
    @staticmethod
    def _ledger_blocks(candidate: Any, recovery_ledger: Any) -> bool:
        if recovery_ledger is None:
            return False
        if hasattr(recovery_ledger, "allows_new_entry"):
            allows_new_entry = recovery_ledger.allows_new_entry
            if callable(allows_new_entry):
                return not bool(allows_new_entry(candidate))
            return not bool(allows_new_entry)
        return False

    @staticmethod
    def _ledger_evidence(recovery_ledger: Any, source: str) -> dict:
        evidence = {"source": source}
        if recovery_ledger is None:
            return evidence
        if hasattr(recovery_ledger, "truth_available"):
            evidence["truth_available"] = bool(recovery_ledger.truth_available)

        blocking_work = []
        try:
            work_items = iter(getattr(recovery_ledger, "work_items", ()) or ())
        except TypeError:
            work_items = iter(())
        for item in work_items:
            if len(blocking_work) >= 3:
                break
            if not bool(getattr(item, "blocking", True)):
                continue
            entry = {
                "kind": str(getattr(item, "kind", "") or ""),
                "symbol": str(getattr(item, "symbol", "") or ""),
            }
            decision = getattr(item, "decision", None)
            if decision is not None:
                decision_evidence = {}
                outcome = getattr(decision, "outcome", "")
                reason = getattr(decision, "reason", "")
                if outcome:
                    decision_evidence["outcome"] = str(outcome)
                if reason:
                    decision_evidence["reason"] = str(reason)
                if decision_evidence:
                    entry["decision"] = decision_evidence
            blocking_work.append(entry)
        if blocking_work:
            evidence["blocking_work"] = blocking_work
        return evidence

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
        if cls._ledger_blocks(candidate, recovery_ledger):
            return LifecycleDecision(
                allowed=False,
                reason="entry_blocked_recovery_ledger",
                evidence=cls._ledger_evidence(recovery_ledger, source),
            )
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
