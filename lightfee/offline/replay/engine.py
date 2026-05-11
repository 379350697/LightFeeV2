"""Replay engine: replays journal events through strategy/simulation."""

from __future__ import annotations

from dataclasses import dataclass, field

from lightfee.offline.replay.dataset import ReplayDataset
from lightfee.sidecar.snapshot import CandidateInput


@dataclass
class ReplayResult:
    total_candidates: int = 0
    accepted: int = 0
    rejected: int = 0
    rejected_reasons: dict[str, int] = field(default_factory=dict)
    simulated_positions: int = 0
    estimated_pnl_quote: float = 0.0


def replay_dataset(dataset: ReplayDataset) -> ReplayResult:
    """Replay a dataset through candidate discovery and scoring."""
    result = ReplayResult()
    for record in dataset.records:
        if record.get("kind") == "sidecar.candidate_published":
            result.total_candidates += 1
            payload = record.get("payload", {})
            if payload.get("blocked"):
                result.rejected += 1
                reasons = payload.get("blocked_reasons", ["unknown"])
                if isinstance(reasons, list):
                    for reason in reasons:
                        result.rejected_reasons[reason] = result.rejected_reasons.get(reason, 0) + 1
                else:
                    reason = str(reasons)
                    result.rejected_reasons[reason] = result.rejected_reasons.get(reason, 0) + 1
            else:
                result.accepted += 1
    return result
