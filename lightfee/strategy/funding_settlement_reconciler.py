"""Allocate private funding statements without changing the V1 close lifecycle.

Public/current funding rates answer an alpha question; they are not cash-flow
evidence.  This module is the only bridge from a venue-private funding
statement to either an open position or a closed-position accounting task.
It deliberately fails closed on timestamp, currency, duplicate-statement, or
multi-position ownership ambiguity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import FundingSettlement, Venue
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
        return {
            "schema_version": FUNDING_SETTLEMENT_SCHEMA_VERSION,
            "kind": "funding_statement_attribution",
            "position_id": position.position_id,
            "symbol": position.symbol,
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
            lifecycle_forecast = float(updated.get("lifecycle_forecast_funding_quote") or 0.0)
            price_pnl = float(updated.get("price_pnl_quote") or 0.0)
            entry_fee = float(updated.get("entry_fee_quote") or 0.0)
            exit_fee = float(updated.get("exit_fee_quote") or 0.0)
            updated.update(
                {
                    "status": "complete",
                    "official_pnl": True,
                    "official_funding_quote": settled_funding_quote,
                    "official_net_quote": price_pnl + settled_funding_quote - entry_fee - exit_fee,
                    "funding_forecast_error_quote": settled_funding_quote - lifecycle_forecast,
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
