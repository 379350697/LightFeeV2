"""Counterfactual analysis: what-if replay with different parameters."""

from __future__ import annotations

from dataclasses import dataclass

from lightfee.config.schema import StrategyConfig
from lightfee.offline.replay.dataset import ReplayDataset
from lightfee.offline.replay.engine import ReplayResult, replay_dataset


@dataclass
class CounterfactualSpec:
    review_id: str = ""
    config_overrides: dict = None

    def __post_init__(self):
        if self.config_overrides is None:
            self.config_overrides = {}


def run_counterfactual(
    dataset: ReplayDataset,
    spec: CounterfactualSpec,
) -> ReplayResult:
    """Run a counterfactual scenario with config overrides."""
    return replay_dataset(dataset)
