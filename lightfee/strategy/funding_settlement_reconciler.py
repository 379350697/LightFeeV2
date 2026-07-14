"""Allocate private funding statements without changing the V1 close lifecycle.

Public/current funding rates answer an alpha question; they are not cash-flow
evidence.  This module is the only bridge from a venue-private funding
statement to either an open position or a closed-position accounting task.
It deliberately fails closed on timestamp, currency, duplicate-statement, or
multi-position ownership ambiguity.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from math import isfinite
from typing import Any, Iterable, Mapping

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import FundingSettlement, Venue
from lightfee.engine.exit import (
    EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS,
    EXECUTION_BENCHMARK_RECEIPT_SCHEMA_VERSION,
    execution_benchmark_receipt_integrity_verified,
    execution_benchmark_receipt_semantically_verified,
    position_execution_benchmark_evidence_complete,
)
from lightfee.engine.state import FundingSettlementRecord, OpenPosition
from lightfee.strategy.attribution import StrategyAttributionService


FUNDING_SETTLEMENT_SCHEMA_VERSION = 1
FUNDING_SETTLEMENT_QUERY_PAD_MS = 5 * 60 * 1000
FUNDING_SETTLEMENT_RETRY_BASE_MS = 30_000
FUNDING_SETTLEMENT_RETRY_MAX_MS = 5 * 60 * 1000
FUNDING_SETTLEMENT_HARD_DEADLINE_MS = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class FundingSettlementReconciliationResult:
    position_id: str
    status: str
    required_count: int
    observed_count: int
    reason: str = ""
    official: bool = False
    settled_funding_quote: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "position_id": self.position_id,
            "status": self.status,
            "reason": self.reason,
            "required_settlement_count": self.required_count,
            "observed_settlement_count": self.observed_count,
            "official_funding": self.official,
            "settled_funding_quote": self.settled_funding_quote,
        }


@dataclass(frozen=True)
class _FundingClaim:
    owner_id: str
    leg: str
    venue: Venue
    symbol: str
    settlement_timestamp_ms: int
    quote_currency: str

    @property
    def key(self) -> tuple[str, str, int]:
        return (self.venue.value, self.symbol, self.settlement_timestamp_ms)

    @property
    def target(self) -> tuple[str, str, int]:
        return (self.leg, self.venue.value, self.settlement_timestamp_ms)


def _quote_currency_for_symbol(symbol: str) -> str:
    normalized = str(symbol or "").upper().replace("-", "").replace("_", "")
    for quote in ("USDT", "USDC", "BUSD", "USD"):
        if normalized.endswith(quote) and len(normalized) > len(quote):
            return quote
    return ""


def _venue_from_value(value: object) -> Venue | None:
    try:
        return value if isinstance(value, Venue) else Venue.from_str(str(value))
    except (TypeError, ValueError):
        return None


def _int_positive(value: object) -> int:
    try:
        converted = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return converted if converted > 0 else 0


def _finite_task_amount(value: object) -> float | None:
    """Parse a persisted accounting amount without converting missing to zero."""
    if value is None or isinstance(value, bool):
        return None
    try:
        amount = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return amount if isfinite(amount) else None


def _execution_benchmark_receipt_complete(position: OpenPosition) -> bool:
    """Return whether persisted fill-quality facts can support promotion.

    Recovery snapshots are a persistence boundary, so a truthy string such as
    ``"false"`` must not revive this permission.  The benchmark flag also
    needs its immutable entry benchmarks and non-negative accumulated
    shortfall values; otherwise a missing measurement could masquerade as a
    zero-cost receipt.
    """
    return position_execution_benchmark_evidence_complete(position)


def _entry_execution_benchmark_receipt_complete(position: OpenPosition) -> bool:
    """Bind persisted entry aggregates to one sealed, side-aware receipt.

    Entry price hints are routing inputs and cannot prove execution quality.
    The closed-position accounting path therefore requires the same immutable
    L2/fill evidence as the exit path, with the entry sides reversed.  Keeping
    this check beside the exit verifier prevents a recovered state snapshot
    from promoting manually edited aggregate fields.
    """
    receipt = position.entry_execution_benchmark_receipt
    if not execution_benchmark_receipt_semantically_verified(
        receipt,
        position_id=position.position_id,
        symbol=position.symbol,
        expected_legs={
            "long": (position.long_venue.value, "buy"),
            "short": (position.short_venue.value, "sell"),
        },
    ):
        return False
    assert isinstance(receipt, dict)
    try:
        receipt_long = float(receipt["long"]["vwap_price"])
        receipt_short = float(receipt["short"]["vwap_price"])
        receipt_shortfall = float(receipt["implementation_shortfall_quote"])
        position_long = float(position.entry_benchmark_long_price)
        position_short = float(position.entry_benchmark_short_price)
        position_shortfall = float(position.entry_implementation_shortfall_quote)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    values = (
        receipt_long,
        receipt_short,
        receipt_shortfall,
        position_long,
        position_short,
        position_shortfall,
    )
    if not all(isfinite(value) for value in values):
        return False
    return (
        abs(receipt_long - position_long)
        <= max(1e-8, max(abs(receipt_long), abs(position_long)) * 1e-8)
        and abs(receipt_short - position_short)
        <= max(1e-8, max(abs(receipt_short), abs(position_short)) * 1e-8)
        and abs(receipt_shortfall - position_shortfall)
        <= max(1e-8, max(abs(receipt_shortfall), abs(position_shortfall)) * 1e-8)
    )


def _exit_execution_benchmark_receipts_complete(position: OpenPosition) -> bool:
    """Require raw, side-aware exit receipts in addition to their aggregate.

    A numerical aggregate alone is not audit evidence: it cannot establish
    whether the close was priced against an executable bid/ask ladder rather
    than a midpoint.  A keyed HMAC authenticates each persisted payload, so
    mutation or a receipt written without the trusted runtime key fails
    closed at the promotion boundary.  The accompanying SHA-256 digest is
    retained only as a corruption diagnostic, not as an authenticity proof.
    """
    receipts = position.exit_execution_benchmark_receipts
    if not isinstance(receipts, list) or not receipts:
        return False
    total_shortfall = 0.0
    for receipt in receipts:
        if not isinstance(receipt, dict):
            return False
        if (
            receipt.get("schema_version")
            != EXECUTION_BENCHMARK_RECEIPT_SCHEMA_VERSION
            or receipt.get("source") != "local_l2_vwap"
            or receipt.get("position_id") != position.position_id
            or receipt.get("symbol") != position.symbol
            or not execution_benchmark_receipt_integrity_verified(receipt)
            or not execution_benchmark_receipt_semantically_verified(
                receipt,
                position_id=position.position_id,
                symbol=position.symbol,
                expected_legs={
                    "long": (position.long_venue.value, "sell"),
                    "short": (position.short_venue.value, "buy"),
                },
            )
        ):
            return False
        try:
            captured_at_ms = int(receipt.get("captured_at_ms", 0))
            requested_quantity = float(receipt.get("requested_base_quantity"))
            max_observation_to_submit_ms = int(
                receipt.get("max_observation_to_submit_ms", 0)
            )
        except (TypeError, ValueError, OverflowError):
            return False
        if (
            captured_at_ms <= 0
            or not isfinite(requested_quantity)
            or requested_quantity <= 0.0
            or max_observation_to_submit_ms
            != EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS
        ):
            return False
        try:
            receipt_shortfall = float(receipt.get("implementation_shortfall_quote"))
        except (TypeError, ValueError, OverflowError):
            return False
        if not isfinite(receipt_shortfall) or receipt_shortfall < 0.0:
            return False
        receipt_recomputed_shortfall = 0.0
        for name, venue, side in (
            ("long", position.long_venue.value, "sell"),
            ("short", position.short_venue.value, "buy"),
        ):
            leg = receipt.get(name)
            if not isinstance(leg, dict):
                return False
            try:
                price = float(leg.get("vwap_price"))
                available = float(leg.get("available_base_quantity"))
                observed_at_ms = int(leg.get("observed_at_ms", 0))
                age_ms = int(leg.get("age_ms", -1))
                filled_quantity = float(leg.get("filled_base_quantity"))
                leg_shortfall = float(leg.get("implementation_shortfall_quote"))
            except (TypeError, ValueError, OverflowError):
                return False
            if (
                leg.get("venue") != venue
                or leg.get("side") != side
                or not isfinite(price)
                or not isfinite(available)
                or price <= 0.0
                or available + max(1e-10, requested_quantity * 1e-8) < requested_quantity
                or observed_at_ms <= 0
                or age_ms < 0
                or observed_at_ms > captured_at_ms
                or captured_at_ms - observed_at_ms != age_ms
                or not isfinite(filled_quantity)
                or not isfinite(leg_shortfall)
                or filled_quantity <= 0.0
                or leg_shortfall < 0.0
            ):
                return False
            quantity_tolerance = max(1e-10, requested_quantity * 1e-8)
            if abs(filled_quantity - requested_quantity) > quantity_tolerance:
                return False
            fills = leg.get("fills")
            if not isinstance(fills, list) or not fills:
                return False
            observed_fill_quantity = 0.0
            observed_leg_shortfall = 0.0
            for fill in fills:
                if not isinstance(fill, dict):
                    return False
                try:
                    fill_quantity = float(fill.get("quantity"))
                    fill_price = float(fill.get("price"))
                    submitted_at_ms = int(fill.get("submitted_at_ms", 0))
                    fill_at_ms = int(fill.get("filled_at_ms", 0))
                except (TypeError, ValueError, OverflowError):
                    return False
                if (
                    not isfinite(fill_quantity)
                    or not isfinite(fill_price)
                    or fill_quantity <= 0.0
                    or fill_price <= 0.0
                    or submitted_at_ms <= 0
                    or fill_at_ms <= 0
                    or captured_at_ms > submitted_at_ms
                    or submitted_at_ms > fill_at_ms
                    or submitted_at_ms - observed_at_ms
                    > max_observation_to_submit_ms
                    or (
                        not str(fill.get("order_id", "") or "")
                        and not str(fill.get("client_order_id", "") or "")
                    )
                ):
                    return False
                observed_fill_quantity += fill_quantity
                adverse_move = price - fill_price if name == "long" else fill_price - price
                observed_leg_shortfall += max(adverse_move, 0.0) * fill_quantity
            shortfall_tolerance = max(
                1e-8,
                max(observed_leg_shortfall, leg_shortfall) * 1e-8,
            )
            if (
                abs(observed_fill_quantity - filled_quantity) > quantity_tolerance
                or abs(observed_leg_shortfall - leg_shortfall) > shortfall_tolerance
            ):
                return False
            receipt_recomputed_shortfall += observed_leg_shortfall
        receipt_tolerance = max(
            1e-8,
            max(receipt_recomputed_shortfall, receipt_shortfall) * 1e-8,
        )
        if abs(receipt_recomputed_shortfall - receipt_shortfall) > receipt_tolerance:
            return False
        total_shortfall += receipt_shortfall
    try:
        persisted_shortfall = float(position.exit_implementation_shortfall_quote)
    except (TypeError, ValueError, OverflowError):
        return False
    total_tolerance = max(1e-8, max(total_shortfall, persisted_shortfall) * 1e-8)
    if abs(total_shortfall - persisted_shortfall) > total_tolerance:
        return False
    return True


class FundingSettlementReconciler:
    """Statement-ledger allocator shared by open and closed position paths."""

    @staticmethod
    def _claims_for_position(position: OpenPosition) -> list[_FundingClaim]:
        quote_currency = _quote_currency_for_symbol(position.symbol)
        if not quote_currency:
            return []
        claims: list[_FundingClaim] = []
        for leg, venue_value, timestamp in StrategyAttributionService.required_settlements(position):
            venue = _venue_from_value(venue_value)
            if venue is None or timestamp <= 0:
                continue
            claims.append(
                _FundingClaim(
                    owner_id=position.position_id,
                    leg=leg,
                    venue=venue,
                    symbol=position.symbol,
                    settlement_timestamp_ms=timestamp,
                    quote_currency=quote_currency,
                )
            )
        return claims

    @staticmethod
    def _claims_for_task(task: Mapping[str, Any]) -> list[_FundingClaim]:
        owner_id = str(task.get("position_id") or "")
        symbol = str(task.get("symbol") or "")
        if not owner_id or not symbol:
            return []
        claims: list[_FundingClaim] = []
        for raw in task.get("required_settlements", []) or []:
            if not isinstance(raw, Mapping):
                continue
            venue = _venue_from_value(raw.get("venue"))
            timestamp = _int_positive(raw.get("settlement_timestamp_ms"))
            leg = str(raw.get("leg") or "")
            quote_currency = str(raw.get("quote_currency") or "").upper()
            if venue is None or timestamp <= 0 or leg not in {"long", "short"} or not quote_currency:
                continue
            claims.append(
                _FundingClaim(
                    owner_id=owner_id,
                    leg=leg,
                    venue=venue,
                    symbol=symbol,
                    settlement_timestamp_ms=timestamp,
                    quote_currency=quote_currency,
                )
            )
        return claims

    @staticmethod
    def _claims_for_statement_claim_ledger(
        rows: Iterable[Mapping[str, Any]],
    ) -> list[_FundingClaim]:
        """Turn durable consumed-statement receipts into ownership claims."""
        claims: list[_FundingClaim] = []
        for row in rows:
            owner_id = str(row.get("owner_id") or "")
            symbol = str(row.get("symbol") or "")
            venue = _venue_from_value(row.get("venue"))
            timestamp = _int_positive(row.get("settlement_timestamp_ms"))
            leg = str(row.get("leg") or "")
            quote_currency = str(row.get("quote_currency") or "").upper()
            if (
                not owner_id
                or not symbol
                or venue is None
                or timestamp <= 0
                or leg not in {"long", "short"}
                or not quote_currency
            ):
                continue
            claims.append(
                _FundingClaim(
                    owner_id=owner_id,
                    leg=leg,
                    venue=venue,
                    symbol=symbol,
                    settlement_timestamp_ms=timestamp,
                    quote_currency=quote_currency,
                )
            )
        return claims

    @staticmethod
    def _pending_task_identity(task: Mapping[str, Any]) -> tuple[str, int]:
        return str(task.get("position_id") or ""), _int_positive(task.get("closed_at_ms"))

    async def _fetch_statement_rows(
        self,
        claims: Iterable[_FundingClaim],
        adapters: Mapping[Venue, VenueAdapter],
    ) -> tuple[dict[tuple[str, str, int], list[FundingSettlement]], dict[tuple[str, str], str]]:
        """Query one adapter once per (venue, symbol), retaining query failures."""
        grouped: dict[tuple[Venue, str], list[_FundingClaim]] = {}
        for claim in claims:
            grouped.setdefault((claim.venue, claim.symbol), []).append(claim)
        rows_by_target: dict[tuple[str, str, int], list[FundingSettlement]] = {}
        errors: dict[tuple[str, str], str] = {}
        for (venue, symbol), scoped_claims in grouped.items():
            adapter = adapters.get(venue)
            if adapter is None:
                errors[(venue.value, symbol)] = "venue_adapter_unavailable"
                continue
            start_time_ms = min(row.settlement_timestamp_ms for row in scoped_claims)
            end_time_ms = max(row.settlement_timestamp_ms for row in scoped_claims)
            try:
                records = await adapter.fetch_funding_settlements(
                    symbol,
                    start_time_ms=max(1, start_time_ms - FUNDING_SETTLEMENT_QUERY_PAD_MS),
                    end_time_ms=end_time_ms + FUNDING_SETTLEMENT_QUERY_PAD_MS,
                )
            except Exception as error:
                errors[(venue.value, symbol)] = f"statement_query_error:{type(error).__name__}"
                continue
            deduped_references: set[str] = set()
            for record in records:
                if not isinstance(record, FundingSettlement):
                    continue
                if record.venue != venue or record.symbol != symbol:
                    continue
                if record.statement_reference in deduped_references:
                    continue
                deduped_references.add(record.statement_reference)
                key = (venue.value, symbol, record.settlement_timestamp_ms)
                rows_by_target.setdefault(key, []).append(record)
        return rows_by_target, errors

    @staticmethod
    def _claim_allocation(
        claims: Iterable[_FundingClaim],
        rows_by_target: Mapping[tuple[str, str, int], list[FundingSettlement]],
        errors: Mapping[tuple[str, str], str],
    ) -> tuple[dict[str, list[FundingSettlementRecord]], dict[str, str]]:
        """Allocate only exactly-one-owner, exactly-one-statement targets."""
        claims_by_key: dict[tuple[str, str, int], list[_FundingClaim]] = {}
        for claim in claims:
            claims_by_key.setdefault(claim.key, []).append(claim)
        allocations: dict[str, list[FundingSettlementRecord]] = {}
        problems: dict[str, str] = {}
        for key, target_claims in claims_by_key.items():
            if len(target_claims) != 1:
                for claim in target_claims:
                    problems[claim.owner_id] = "ambiguous_position_ownership"
                continue
            claim = target_claims[0]
            query_error = errors.get((claim.venue.value, claim.symbol))
            if query_error:
                problems.setdefault(claim.owner_id, query_error)
                continue
            matches = rows_by_target.get(key, [])
            if len(matches) > 1:
                problems.setdefault(claim.owner_id, "multiple_statement_rows_for_target")
                continue
            if not matches:
                problems.setdefault(claim.owner_id, "statement_not_observed")
                continue
            statement = matches[0]
            if statement.quote_currency.upper() != claim.quote_currency:
                problems.setdefault(claim.owner_id, "statement_quote_currency_mismatch")
                continue
            allocations.setdefault(claim.owner_id, []).append(
                FundingSettlementRecord(
                    leg=claim.leg,
                    venue=claim.venue.value,
                    settlement_timestamp_ms=claim.settlement_timestamp_ms,
                    amount_quote=statement.amount_quote,
                    observed_at_ms=statement.observed_at_ms,
                    source=statement.source,
                    statement_reference=statement.statement_reference,
                )
            )
        return allocations, problems

    async def reconcile_open_positions(
        self,
        positions: Iterable[OpenPosition],
        adapters: Mapping[Venue, VenueAdapter],
    ) -> list[FundingSettlementReconciliationResult]:
        """Refresh open positions from private statements, without guessing zero."""
        selected = [position for position in positions if self._claims_for_position(position)]
        claims = [claim for position in selected for claim in self._claims_for_position(position)]
        if not claims:
            return []
        rows_by_target, errors = await self._fetch_statement_rows(claims, adapters)
        allocations, problems = self._claim_allocation(claims, rows_by_target, errors)
        results: list[FundingSettlementReconciliationResult] = []
        for position in selected:
            attribution = StrategyAttributionService.record_settlements(
                position,
                allocations.get(position.position_id, []),
            )
            reason = problems.get(position.position_id, "")
            status = attribution.evidence_status
            if reason:
                status = "unresolved"
            results.append(
                FundingSettlementReconciliationResult(
                    position_id=position.position_id,
                    status=status,
                    reason=reason,
                    required_count=len(attribution.required_settlements),
                    observed_count=attribution.observed_settlements,
                    official=attribution.official,
                    settled_funding_quote=attribution.settled_funding_quote,
                )
            )
        return results

    @staticmethod
    def new_pending_task(
        position: OpenPosition,
        *,
        closed_at_ms: int,
        price_pnl_quote: float,
        entry_fee_quote: float,
        exit_fee_quote: float,
    ) -> dict[str, Any] | None:
        """Persist a post-close accounting task before the position is removed."""
        claims = FundingSettlementReconciler._claims_for_position(position)
        if not claims:
            return None
        existing = {
            (record.leg, record.venue, record.settlement_timestamp_ms): record
            for record in position.funding_settlement_records
        }
        benchmark_complete = _execution_benchmark_receipt_complete(position)
        return {
            "schema_version": FUNDING_SETTLEMENT_SCHEMA_VERSION,
            "kind": "funding_statement_attribution",
            "position_id": position.position_id,
            "symbol": position.symbol,
            "long_venue": position.long_venue.value,
            "short_venue": position.short_venue.value,
            "calculation_version": position.calculation_version,
            "model_epoch": position.model_epoch,
            "economics_observed_at_ms": int(position.economics_observed_at_ms or 0),
            "closed_at_ms": int(closed_at_ms),
            "created_at_ms": int(closed_at_ms),
            "deadline_ms": int(closed_at_ms) + FUNDING_SETTLEMENT_HARD_DEADLINE_MS,
            "attempt_count": 0,
            "next_attempt_ms": int(closed_at_ms),
            "status": "pending_statement_evidence",
            "required_settlements": [
                {
                    "leg": claim.leg,
                    "venue": claim.venue.value,
                    "settlement_timestamp_ms": claim.settlement_timestamp_ms,
                    "quote_currency": claim.quote_currency,
                }
                for claim in claims
            ],
            "funding_settlement_records": [
                existing[claim.target].to_dict()
                for claim in claims
                if claim.target in existing
            ],
            "lifecycle_forecast_funding_quote": float(
                position.captured_funding_quote + position.second_stage_funding_quote
            ),
            "price_pnl_quote": float(price_pnl_quote),
            "entry_fee_quote": float(entry_fee_quote),
            "exit_fee_quote": float(exit_fee_quote),
            # These are immutable fill-quality facts.  ``None`` means the
            # executable benchmark was unavailable and must fail closed for
            # canary promotion; it is never replaced with price PnL.
            "implementation_shortfall_quote": (
                float(
                    position.entry_implementation_shortfall_quote
                    + position.exit_implementation_shortfall_quote
                )
                if benchmark_complete
                else None
            ),
            "execution_benchmark_complete": benchmark_complete,
            "execution_fee_complete": position.execution_fee_complete is True,
            "entry_execution_benchmark_receipt": (
                deepcopy(position.entry_execution_benchmark_receipt)
                if benchmark_complete
                and isinstance(position.entry_execution_benchmark_receipt, dict)
                else None
            ),
            "exit_execution_benchmark_receipts": (
                [deepcopy(receipt) for receipt in position.exit_execution_benchmark_receipts]
                if benchmark_complete
                else []
            ),
        }

    @classmethod
    def register_closed_position_task(
        cls,
        state: Any,
        position: OpenPosition,
        *,
        closed_at_ms: int,
        price_pnl_quote: float,
        exit_fee_quote: float,
    ) -> dict[str, Any] | None:
        """Durably retain statement attribution before removing a position.

        This is deliberately a local state mutation only: close and recovery
        semantics must never wait on a private-account HTTP call.  The runtime
        performs the eventual statement query from the separately persisted
        accounting queue.
        """
        task = cls.new_pending_task(
            position,
            closed_at_ms=closed_at_ms,
            price_pnl_quote=price_pnl_quote,
            entry_fee_quote=(
                position.total_entry_fee_quote
                or position.long_entry_fee_quote + position.short_entry_fee_quote
            ),
            exit_fee_quote=exit_fee_quote,
        )
        if task is not None:
            state.enqueue_pending_funding_settlement_reconciliation(task)
        return task

    async def reconcile_pending_task(
        self,
        task: Mapping[str, Any],
        adapters: Mapping[Venue, VenueAdapter],
        *,
        now_ms: int,
    ) -> tuple[dict[str, Any], FundingSettlementReconciliationResult]:
        """Refresh one durable, accounting-only post-close task.

        Callers that have more than one task due must use
        :meth:`reconcile_pending_tasks`.  A funding statement is an
        account-level cash-flow record, so deciding its owner one task at a
        time can allocate the same statement twice.
        """
        results = await self.reconcile_pending_tasks(
            [task],
            adapters,
            now_ms=now_ms,
        )
        return results[0]

    async def reconcile_pending_tasks(
        self,
        tasks: Iterable[Mapping[str, Any]],
        adapters: Mapping[Venue, VenueAdapter],
        *,
        now_ms: int,
        ownership_positions: Iterable[OpenPosition] = (),
        ownership_tasks: Iterable[Mapping[str, Any]] = (),
        ownership_statement_claims: Iterable[Mapping[str, Any]] = (),
    ) -> list[tuple[dict[str, Any], FundingSettlementReconciliationResult]]:
        """Reconcile a due batch under one global statement-ownership view.

        Closed-position tasks and still-open positions may claim the same
        venue/symbol/settlement timestamp.  They must be considered in the
        same allocation pass: otherwise an account-level statement can become
        official for two independent PnL records.  Open positions supplied
        here are reservations only; their ordinary lifecycle attribution is
        still owned by :meth:`reconcile_open_positions`.
        """
        updated_tasks = [dict(task) for task in tasks]
        task_claims = [self._claims_for_task(task) for task in updated_tasks]
        claim_keys = {
            claim.key
            for claims in task_claims
            for claim in claims
        }

        reservation_claims = [
            claim
            for position in ownership_positions
            for claim in self._claims_for_position(position)
            if claim.key in claim_keys
        ]
        due_identities = {
            self._pending_task_identity(task)
            for task in updated_tasks
        }
        pending_task_reservations = [
            claim
            for task in ownership_tasks
            if self._pending_task_identity(task) not in due_identities
            for claim in self._claims_for_task(task)
            if claim.key in claim_keys
        ]
        ledger_reservations = [
            claim
            for claim in self._claims_for_statement_claim_ledger(
                ownership_statement_claims
            )
            if claim.key in claim_keys
        ]
        all_claims = [
            claim
            for claims in task_claims
            for claim in claims
        ] + reservation_claims + pending_task_reservations + ledger_reservations
        rows_by_target, errors = await self._fetch_statement_rows(all_claims, adapters)
        allocations, problems = self._claim_allocation(
            all_claims,
            rows_by_target,
            errors,
        )

        results: list[tuple[dict[str, Any], FundingSettlementReconciliationResult]] = []
        for updated, claims in zip(updated_tasks, task_claims, strict=True):
            position_id = str(updated.get("position_id") or "")
            if not claims:
                updated["status"] = "invalid_task"
                results.append(
                    (
                        updated,
                        FundingSettlementReconciliationResult(
                            position_id=position_id,
                            status="invalid_task",
                            reason="missing_or_invalid_required_settlements",
                            required_count=0,
                            observed_count=0,
                        ),
                    )
                )
                continue

            results.append(
                self._reconcile_pending_task_from_allocation(
                    updated,
                    claims,
                    allocations.get(position_id, []),
                    problems.get(position_id, ""),
                    now_ms=now_ms,
                )
            )
        return results

    @staticmethod
    def _reconcile_pending_task_from_allocation(
        updated: dict[str, Any],
        claims: list[_FundingClaim],
        allocated_records: Iterable[FundingSettlementRecord],
        problem: str,
        *,
        now_ms: int,
    ) -> tuple[dict[str, Any], FundingSettlementReconciliationResult]:
        """Apply one globally-decided allocation to a durable task."""
        position_id = str(updated.get("position_id") or "")

        existing_records: list[FundingSettlementRecord] = []
        for raw in updated.get("funding_settlement_records", []) or []:
            if not isinstance(raw, Mapping):
                continue
            try:
                existing_records.append(FundingSettlementRecord.from_dict(dict(raw)))
            except (TypeError, ValueError):
                updated["status"] = "conflict"
                return updated, FundingSettlementReconciliationResult(
                    position_id=position_id,
                    status="conflict",
                    reason="invalid_persisted_statement_record",
                    required_count=len(claims),
                    observed_count=0,
                )

        current_by_target = {
            (record.leg, record.venue, record.settlement_timestamp_ms): record
            for record in existing_records
        }
        for record in allocated_records:
            target = (record.leg, record.venue, record.settlement_timestamp_ms)
            previous = current_by_target.get(target)
            if previous is not None and previous != record:
                problem = "conflicting_statement_reference"
                continue
            current_by_target.setdefault(target, record)

        required_targets = {claim.target for claim in claims}
        observed = {target: row for target, row in current_by_target.items() if target in required_targets}
        updated["funding_settlement_records"] = [
            observed[target].to_dict() for target in sorted(observed)
        ]
        updated["last_checked_at_ms"] = int(now_ms)
        settled_funding_quote = sum(record.amount_quote for record in observed.values())
        reason = problem
        complete = len(observed) == len(required_targets) and not reason
        if complete:
            lifecycle_forecast = _finite_task_amount(
                updated.get("lifecycle_forecast_funding_quote")
            )
            price_pnl = _finite_task_amount(updated.get("price_pnl_quote"))
            entry_fee = _finite_task_amount(updated.get("entry_fee_quote"))
            exit_fee = _finite_task_amount(updated.get("exit_fee_quote"))
            net_pnl_complete = bool(
                updated.get("execution_fee_complete") is True
                and lifecycle_forecast is not None
                and price_pnl is not None
                and entry_fee is not None
                and exit_fee is not None
                and isfinite(settled_funding_quote)
            )
            updated.update(
                {
                    "status": "complete",
                    # Statement facts are fully reconciled even if a fill fee
                    # was not observable.  Preserve that distinction instead
                    # of promoting V1's zero fallback into official net PnL.
                    "official_funding_reconciled": True,
                    "official_pnl": net_pnl_complete,
                    "official_funding_quote": settled_funding_quote,
                    "official_net_quote": (
                        price_pnl + settled_funding_quote - entry_fee - exit_fee
                        if net_pnl_complete
                        else None
                    ),
                    "funding_forecast_error_quote": (
                        settled_funding_quote - lifecycle_forecast
                        if lifecycle_forecast is not None
                        else None
                    ),
                    "reconciled_at_ms": int(now_ms),
                }
            )
            result = FundingSettlementReconciliationResult(
                position_id=position_id,
                status="complete",
                required_count=len(required_targets),
                observed_count=len(observed),
                official=True,
                settled_funding_quote=settled_funding_quote,
            )
            return updated, result

        attempt_count = int(updated.get("attempt_count") or 0) + 1
        delay = min(
            FUNDING_SETTLEMENT_RETRY_BASE_MS * (2 ** max(attempt_count - 1, 0)),
            FUNDING_SETTLEMENT_RETRY_MAX_MS,
        )
        updated["attempt_count"] = attempt_count
        updated["next_attempt_ms"] = int(now_ms) + delay
        updated["status"] = "unresolved_statement_evidence"
        updated["last_reason"] = reason or "statement_not_observed"
        return updated, FundingSettlementReconciliationResult(
            position_id=position_id,
            status="unresolved",
            reason=str(updated["last_reason"]),
            required_count=len(required_targets),
            observed_count=len(observed),
            settled_funding_quote=settled_funding_quote,
        )
