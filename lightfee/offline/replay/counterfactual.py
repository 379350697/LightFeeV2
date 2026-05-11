"""Counterfactual analysis: what-if replay with different parameters."""

from __future__ import annotations

from dataclasses import dataclass

from lightfee.offline.replay.dataset import ReplayDataset
from lightfee.offline.replay.engine import ReplayResult, replay_dataset


@dataclass
class CounterfactualSpec:
    review_id: str = ""
    config_overrides: dict | None = None

    def __post_init__(self):
        if self.config_overrides is None:
            self.config_overrides = {}


def run_counterfactual(
    dataset: ReplayDataset,
    spec: CounterfactualSpec,
) -> ReplayResult:
    """Run a counterfactual scenario with config overrides applied.

    V1: applies config_overrides to strategy parameters before replaying
    the same recorded evidence. This must not synthesize missing data.
    """
    # Apply config overrides to replay behavior
    # Currently the replay engine uses recorded evidence as-is;
    # counterfactual overrides could affect scoring thresholds,
    # min_edge_bps, etc. when those parameters are extracted from records.
    if spec.config_overrides and spec.config_overrides.get("min_edge_bps") is not None:
        # Filter candidates by overridden min_edge_bps instead of recorded values
        override_edge = float(spec.config_overrides["min_edge_bps"])
        result = ReplayResult()
        for record in dataset.records:
            if record.get("kind") in ("scan.completed", "sidecar.candidate_published"):
                result.total_candidates += 1
                payload = record.get("payload", {})
                if payload.get("blocked"):
                    result.rejected += 1
                    reasons = payload.get("blocked_reasons", ["unknown"])
                    if isinstance(reasons, list):
                        for reason in reasons:
                            result.rejected_reasons[reason] = (
                                result.rejected_reasons.get(reason, 0) + 1
                            )
                    else:
                        reason = str(reasons)
                        result.rejected_reasons[reason] = (
                            result.rejected_reasons.get(reason, 0) + 1
                        )
                else:
                    # Apply override: accept only if edge >= override
                    edge_bps = payload.get("expected_edge_bps", 0.0)
                    if float(edge_bps) >= override_edge:
                        result.accepted += 1
                    else:
                        result.rejected += 1
                        reason = "counterfactual_edge_too_low"
                        result.rejected_reasons[reason] = (
                            result.rejected_reasons.get(reason, 0) + 1
                        )
        return result

    # No overrides: use recorded replay
    return replay_dataset(dataset)
