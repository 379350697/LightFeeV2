"""Transfer bias evaluation matching Rust transfer preference logic."""

from __future__ import annotations

from enum import Enum

from lightfee.config.schema import StrategyConfig


class TransferState(Enum):
    CLEAR = "clear"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


def evaluate_transfer_bias(
    transfer_state: TransferState,
    config: StrategyConfig,
) -> float:
    """Return transfer bias in bps based on health state."""
    if transfer_state == TransferState.CLEAR:
        return config.transfer_healthy_bias_bps
    elif transfer_state == TransferState.DEGRADED:
        return config.transfer_degraded_bias_bps
    elif transfer_state == TransferState.UNAVAILABLE:
        return config.transfer_degraded_bias_bps * 2.0
    return config.transfer_unknown_bias_bps
