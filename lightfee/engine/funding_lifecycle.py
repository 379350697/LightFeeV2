from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lightfee.config.schema import StrategyConfig


@dataclass(frozen=True)
class EntryHorizonDecision:
    allowed: bool
    reason: str = ""
    first_funding_timestamp_ms: int = 0
    remaining_to_first_funding_ms: int = 0
    effective_min_before_ms: int = 0
    source: str = ""


class FundingLifecycle:
    @staticmethod
    def positive_ms(value: Any) -> int:
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError):
            return 0
        return parsed if parsed > 0 else 0

    @staticmethod
    def position_positive_ms(value: Any) -> Any:
        """Preserve OpenPosition direct-compare semantics for close paths."""
        return 0 if value <= 0 else value

    @classmethod
    def first_funding_ms(cls, obj: Any) -> int:
        direct = cls.positive_ms(getattr(obj, "first_funding_timestamp_ms", 0))
        if direct > 0:
            return direct
        fallback = cls.positive_ms(getattr(obj, "funding_timestamp_ms", 0))
        if fallback > 0:
            return fallback
        leg_times = [
            cls.positive_ms(getattr(obj, "long_funding_timestamp_ms", 0)),
            cls.positive_ms(getattr(obj, "short_funding_timestamp_ms", 0)),
        ]
        positives = [ts for ts in leg_times if ts > 0]
        return min(positives) if positives else 0

    @staticmethod
    def effective_entry_min_before_ms(strategy: StrategyConfig) -> int:
        min_scan_ms = int(strategy.min_scan_minutes_before_funding or 0) * 60_000
        return max(min_scan_ms, 0)

    @classmethod
    def entry_horizon(
        cls,
        obj: Any,
        now_ms: int,
        strategy: StrategyConfig,
        *,
        source: str = "candidate",
    ) -> EntryHorizonDecision:
        first_ms = cls.first_funding_ms(obj)
        effective_min = cls.effective_entry_min_before_ms(strategy)
        if first_ms <= 0:
            return EntryHorizonDecision(
                allowed=False,
                reason="entry_blocked_first_funding_missing",
                first_funding_timestamp_ms=0,
                remaining_to_first_funding_ms=0,
                effective_min_before_ms=effective_min,
                source=source,
            )
        remaining = first_ms - max(int(now_ms or 0), 0)
        if remaining < effective_min:
            return EntryHorizonDecision(
                allowed=False,
                reason="entry_blocked_first_funding_too_close",
                first_funding_timestamp_ms=first_ms,
                remaining_to_first_funding_ms=remaining,
                effective_min_before_ms=effective_min,
                source=source,
            )
        return EntryHorizonDecision(
            allowed=True,
            first_funding_timestamp_ms=first_ms,
            remaining_to_first_funding_ms=remaining,
            effective_min_before_ms=effective_min,
            source=source,
        )
