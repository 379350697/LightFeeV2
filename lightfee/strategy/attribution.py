"""Position-level funding attribution backed by allocated exchange statements.

The legacy engine's funding capture is a lifecycle estimate.  This module
intentionally does not turn that estimate into a settlement fact: official
funding is available only after every settlement required by the captured
lifecycle stage has a position-allocated statement record.
"""

from __future__ import annotations

from dataclasses import dataclass

from lightfee.engine.state import FundingSettlementRecord, OpenPosition


@dataclass(frozen=True)
class FundingAttribution:
    lifecycle_forecast_quote: float
    settled_funding_quote: float
    forecast_error_quote: float | None
    evidence_status: str
    official: bool
    required_settlements: tuple[tuple[str, str, int], ...]
    observed_settlements: int

    def to_dict(self) -> dict[str, object]:
        return {
            "lifecycle_forecast_quote": self.lifecycle_forecast_quote,
            "settled_funding_quote": self.settled_funding_quote,
            "forecast_error_quote": self.forecast_error_quote,
            "funding_settlement_evidence_status": self.evidence_status,
            "official_funding": self.official,
            "required_settlement_count": len(self.required_settlements),
            "observed_settlement_count": self.observed_settlements,
        }


class StrategyAttributionService:
    """Owns the boundary between V1 lifecycle estimates and realised funding."""

    @staticmethod
    def required_settlements(position: OpenPosition) -> tuple[tuple[str, str, int], ...]:
        """Return the exact leg/venue/timestamp statement facts now required.

        A stage becomes required only after the existing V1 lifecycle has
        marked it captured.  This preserves V1 close timing while preventing a
        future stage from being counted as realised funding today.
        """
        required: list[tuple[str, str, int]] = []
        long_ts = int(position.long_funding_timestamp_ms or position.funding_timestamp_ms or 0)
        short_ts = int(position.short_funding_timestamp_ms or position.funding_timestamp_ms or 0)
        if position.funding_captured:
            if position.opportunity_type == "staggered":
                first_leg = position.first_funding_leg
                if first_leg == "long" and long_ts > 0:
                    required.append(("long", position.long_venue.value, long_ts))
                elif first_leg == "short" and short_ts > 0:
                    required.append(("short", position.short_venue.value, short_ts))
            else:
                if long_ts > 0:
                    required.append(("long", position.long_venue.value, long_ts))
                if short_ts > 0:
                    required.append(("short", position.short_venue.value, short_ts))
        if position.second_stage_funding_captured:
            second_leg = "short" if position.first_funding_leg == "long" else "long"
            second_ts = int(position.second_funding_timestamp_ms or 0)
            venue = position.short_venue.value if second_leg == "short" else position.long_venue.value
            if second_ts > 0:
                required.append((second_leg, venue, second_ts))
        return tuple(required)

    @staticmethod
    def record_settlements(
        position: OpenPosition,
        records: list[FundingSettlementRecord],
    ) -> FundingAttribution:
        """Store exact allocated statement facts and refresh attribution.

        The caller is responsible for proving the statement's position
        allocation.  Records that do not exactly identify one required leg,
        venue and settlement timestamp are ignored rather than guessed.
        """
        required = set(StrategyAttributionService.required_settlements(position))
        # One leg/venue/settlement timestamp represents exactly one account
        # funding fact for a position.  A second statement reference cannot
        # be safely summed or chosen by arrival order; a correction requires
        # an explicit reconciliation workflow outside this append-only path.
        known_targets = {
            (r.leg, r.venue, r.settlement_timestamp_ms): r
            for r in position.funding_settlement_records
        }
        for record in records:
            target = (record.leg, record.venue, record.settlement_timestamp_ms)
            if target not in required:
                continue
            existing = known_targets.get(target)
            if existing is None:
                position.funding_settlement_records.append(record)
                known_targets[target] = record
            elif existing != record:
                # Preserve conflicting evidence so ``refresh`` can fail
                # closed.  Silently retaining the first arrival would turn an
                # exchange correction or allocation bug into official PnL.
                position.funding_settlement_records.append(record)
        return StrategyAttributionService.refresh(position)

    @staticmethod
    def refresh(position: OpenPosition) -> FundingAttribution:
        required = StrategyAttributionService.required_settlements(position)
        required_set = set(required)
        matched: dict[tuple[str, str, int], FundingSettlementRecord] = {}
        conflicting_targets: set[tuple[str, str, int]] = set()
        for record in position.funding_settlement_records:
            key = (record.leg, record.venue, record.settlement_timestamp_ms)
            if key in required_set:
                existing = matched.get(key)
                if existing is None:
                    matched[key] = record
                elif existing != record:
                    conflicting_targets.add(key)
        settled = sum(record.amount_quote for record in matched.values())
        lifecycle_forecast = (
            float(position.captured_funding_quote) + float(position.second_stage_funding_quote)
        )
        official = (
            bool(required)
            and len(matched) == len(required)
            and not conflicting_targets
        )
        if conflicting_targets:
            status = "conflict"
        elif not required:
            status = "missing"
        elif official:
            status = "complete"
        elif matched:
            status = "partial"
        else:
            status = "missing"
        position.settled_funding_quote = settled
        position.funding_settlement_evidence_status = status
        position.funding_forecast_error_quote = (
            settled - lifecycle_forecast if official else None
        )
        return FundingAttribution(
            lifecycle_forecast_quote=lifecycle_forecast,
            settled_funding_quote=settled,
            forecast_error_quote=position.funding_forecast_error_quote,
            evidence_status=status,
            official=official,
            required_settlements=required,
            observed_settlements=len(matched),
        )
