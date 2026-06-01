"""Entry readiness provider boundary for final candidate selection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class EntryReadinessDecision:
    """Provider decision for one entry candidate."""

    allowed: bool
    reason: str = ""
    symbol: str = ""
    pair_id: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def allow(
        cls,
        *,
        symbol: str = "",
        pair_id: str = "",
        evidence: dict[str, Any] | None = None,
    ) -> "EntryReadinessDecision":
        return cls(
            allowed=True,
            symbol=symbol,
            pair_id=pair_id,
            evidence=dict(evidence or {}),
        )

    @classmethod
    def block(
        cls,
        reason: str,
        *,
        symbol: str = "",
        pair_id: str = "",
        evidence: dict[str, Any] | None = None,
    ) -> "EntryReadinessDecision":
        return cls(
            allowed=False,
            reason=str(reason),
            symbol=symbol,
            pair_id=pair_id,
            evidence=dict(evidence or {}),
        )


class EntryReadinessProvider(Protocol):
    """Decides whether a final entry candidate has enough quote evidence."""

    def decide(self, candidate: Any, now_ms: int) -> EntryReadinessDecision:
        ...


class LocalL2EntryReadinessProvider:
    """Default provider preserving the existing local-L2 gate semantics."""

    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime

    def decide(self, candidate: Any, now_ms: int) -> EntryReadinessDecision:
        symbol = str(getattr(candidate, "symbol", ""))
        pair_id = self._runtime._candidate_pair_id(candidate)
        reason = self._runtime._entry_local_l2_selection_blocker(candidate, now_ms)
        if reason:
            return EntryReadinessDecision.block(
                str(reason),
                symbol=symbol,
                pair_id=pair_id,
                evidence={"provider": "local_l2"},
            )
        return EntryReadinessDecision.allow(
            symbol=symbol,
            pair_id=pair_id,
            evidence={"provider": "local_l2"},
        )
