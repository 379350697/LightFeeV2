"""V1-compatible pending-entry admission decisions.

This module owns the pre-submit contract for passive incremental entries.
Entry dispatch supplies venue evidence; pending-entry hedge-delta owns
post-fill buffering and hedge release decisions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any


_EPSILON = 1e-12


class PendingEntryAdmissionDecisionKind(Enum):
    ALLOW = "allow"
    ALLOW_WITH_ADVISORY = "allow_with_advisory"
    BLOCK = "block"


@dataclass(frozen=True)
class PendingEntryAdmissionRequest:
    symbol: str
    long_venue: str
    short_venue: str
    maker_venue: str
    hedge_venue: str
    entry_type: str
    maker_metadata: dict[str, Any]
    maker_quantity_step: float | None
    hedge_quantity_step: float | None
    min_hedgeable_chunk: float
    full_target_quantity: float
    initial_maker_target_quantity: float
    guard_enabled: bool
    small_fill_buffer_enabled: bool
    ts_ms: int


@dataclass(frozen=True)
class PendingEntryAdmissionDecision:
    kind: PendingEntryAdmissionDecisionKind
    event_kind: str = ""
    payload: dict[str, Any] | None = None

    @property
    def can_submit(self) -> bool:
        return self.kind is not PendingEntryAdmissionDecisionKind.BLOCK


class PendingEntryAdmissionCore:
    @classmethod
    def decide(
        cls,
        request: PendingEntryAdmissionRequest,
    ) -> PendingEntryAdmissionDecision:
        if request.entry_type != "passive_incremental":
            return PendingEntryAdmissionDecision(
                kind=PendingEntryAdmissionDecisionKind.ALLOW,
                payload={},
            )

        maker_evidence = cls.maker_fill_increment_evidence(
            maker_venue=request.maker_venue,
            symbol=request.symbol,
            metadata=request.maker_metadata,
            maker_quantity_step=request.maker_quantity_step,
        )
        hedge_min_chunk = max(
            _positive_float(request.min_hedgeable_chunk),
            _positive_float(request.hedge_quantity_step),
        )
        maker_increment = _positive_float(
            maker_evidence.get("maker_fill_increment_base")
        )
        initial_maker_target = _positive_float(request.initial_maker_target_quantity)
        full_target = _positive_float(request.full_target_quantity)
        planned_clip_hedgeable = (
            hedge_min_chunk > _EPSILON
            and initial_maker_target + _EPSILON >= hedge_min_chunk
            and full_target + _EPSILON >= hedge_min_chunk
        )

        reason = str(maker_evidence.get("reason") or "")
        hard_block = False
        small_fill_buffer_required = False
        if not reason and (maker_increment <= _EPSILON or hedge_min_chunk <= _EPSILON):
            reason = "maker_fill_unit_truth_unavailable"
        if not reason and not planned_clip_hedgeable:
            reason = "planned_maker_clip_below_hedge_min_chunk"
            hard_block = True
        if not reason and maker_increment + _EPSILON < hedge_min_chunk:
            reason = "maker_fill_increment_below_hedge_min_chunk"
            small_fill_buffer_required = True
            hard_block = not request.small_fill_buffer_enabled

        if not reason:
            return PendingEntryAdmissionDecision(
                kind=PendingEntryAdmissionDecisionKind.ALLOW,
                payload={},
            )

        if reason == "maker_fill_unit_truth_unavailable":
            hard_block = True

        payload = {
            **maker_evidence,
            "symbol": request.symbol,
            "long_venue": request.long_venue,
            "short_venue": request.short_venue,
            "hedge_venue": request.hedge_venue,
            "reason": reason,
            "min_hedgeable_chunk": float(hedge_min_chunk or 0.0),
            "hedge_quantity_step": float(request.hedge_quantity_step or 0.0),
            "full_target_quantity": float(request.full_target_quantity or 0.0),
            "initial_maker_target_quantity": float(
                request.initial_maker_target_quantity or 0.0
            ),
            "planned_clip_hedgeable": planned_clip_hedgeable,
            "small_fill_buffer_required": small_fill_buffer_required,
            "small_fill_buffer_enabled": bool(request.small_fill_buffer_enabled),
            "cooldown_scope": "symbol",
            "guard_enabled": bool(request.guard_enabled),
            "guard_disabled": not bool(request.guard_enabled),
            "ts_ms": int(request.ts_ms or 0),
        }
        if hard_block and request.guard_enabled:
            return PendingEntryAdmissionDecision(
                kind=PendingEntryAdmissionDecisionKind.BLOCK,
                event_kind="runtime.entry_blocked_pre_submit_hedgeability",
                payload=payload,
            )
        return PendingEntryAdmissionDecision(
            kind=PendingEntryAdmissionDecisionKind.ALLOW_WITH_ADVISORY,
            event_kind="runtime.entry_pre_submit_hedgeability_advisory",
            payload=payload,
        )

    @staticmethod
    def maker_fill_increment_evidence(
        *,
        maker_venue: str,
        symbol: str,
        metadata: dict[str, Any] | None,
        maker_quantity_step: float | None,
    ) -> dict[str, Any]:
        del symbol
        metadata = metadata or {}
        evidence: dict[str, Any] = {
            "maker_venue": maker_venue,
            "maker_fill_increment_base": 0.0,
            "quantity_units": str(metadata.get("quantity_units") or "base"),
            "maker_quantity_step": float(maker_quantity_step or 0.0),
            "maker_increment_source": "unavailable",
        }
        if maker_venue == "gate":
            contract_multiplier = _metadata_positive_float(
                metadata,
                (
                    "contract_multiplier",
                    "contractMultiplier",
                    "quanto_multiplier",
                    "quantoMultiplier",
                    "contract_size",
                    "contractSize",
                    "ct_val",
                    "ctVal",
                ),
            )
            contract_step = _metadata_positive_float(
                metadata,
                (
                    "contract_step",
                    "order_size_round",
                    "orderSizeRound",
                    "lot_size",
                    "lotSize",
                ),
            )
            evidence.update(
                {
                    "raw_contract_step": float(contract_step or 0.0),
                    "contract_multiplier": float(contract_multiplier or 0.0),
                    "quantity_units": "gate_contracts_to_base",
                }
            )
            if contract_multiplier <= 0.0 or contract_step <= 0.0:
                evidence["reason"] = "maker_fill_unit_truth_unavailable"
                return evidence
            evidence["maker_fill_increment_base"] = float(
                contract_step * contract_multiplier
            )
            evidence["maker_increment_source"] = "gate_contract_step_multiplier"
            return evidence

        increment = _metadata_positive_float(
            metadata,
            ("quantity_step", "step_size", "qtyStep", "base_step", "baseStep"),
        )
        if increment <= 0.0:
            increment = _positive_float(maker_quantity_step)
        if increment <= 0.0:
            evidence["reason"] = "maker_fill_unit_truth_unavailable"
            return evidence
        evidence["maker_fill_increment_base"] = float(increment)
        evidence["maker_increment_source"] = "base_quantity_step"
        return evidence


def _positive_float(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(number) or number <= 0.0:
        return 0.0
    return number


def _metadata_positive_float(
    metadata: dict[str, Any],
    aliases: tuple[str, ...],
) -> float:
    for alias in aliases:
        number = _positive_float(metadata.get(alias))
        if number > 0.0:
            return number
    return 0.0
