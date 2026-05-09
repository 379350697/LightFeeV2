"""Error types for LightFee runtime."""

from __future__ import annotations

from enum import Enum


class SubmitFailureClass(Enum):
    """Classification of order submission failure outcomes."""

    REJECTED = "rejected"
    UNCERTAIN = "uncertain"


class LightFeeError(Exception):
    """Base error for LightFee domain."""


class OrderSubmitError(LightFeeError):
    """Order submission failure with classified outcome."""

    def __init__(self, class_: SubmitFailureClass, message: str) -> None:
        super().__init__(message)
        self.class_ = class_

    @property
    def is_rejected(self) -> bool:
        return self.class_ == SubmitFailureClass.REJECTED

    @property
    def is_uncertain(self) -> bool:
        return self.class_ == SubmitFailureClass.UNCERTAIN


class ConfigError(LightFeeError):
    """Configuration loading or validation error."""


class SnapshotStaleError(LightFeeError):
    """Sidecar snapshot is too old to use."""


class SnapshotMalformedError(LightFeeError):
    """Sidecar snapshot schema is invalid or malformed."""


class RecoveryError(LightFeeError):
    """Restart recovery failed or produced ambiguous state."""


class RiskViolationError(LightFeeError):
    """Trade blocked by risk gate."""
