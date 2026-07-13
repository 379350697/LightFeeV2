"""Entry dispatch runtime delegate.

This module owns behavior mechanically moved from LiveRuntime.
Do not change entry dispatch, order admission, or journal payload semantics while extracting it.
"""

from __future__ import annotations

import asyncio
import math
import re
from contextlib import suppress
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from math import gcd
from typing import Any

from lightfee.core.domain import (
    EntryLeverageEvidence,
    OrderRequest,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.core.errors import OrderSubmitError
from lightfee.engine.entry import EntryContext, EntryType, normalize_opportunity_type
from lightfee.engine.business_contract import classify_entry_quantity_contract
from lightfee.engine.entry_readiness import QuoteLease
from lightfee.engine.execution_planner import (
    ExecutionRoute,
    plan_incremental_entry_execution,
)
from lightfee.engine.pending_entry_admission import (
    PendingEntryAdmissionCore,
    PendingEntryAdmissionRequest,
)
from lightfee.engine.recovery import (
    has_pending_entry_for_symbol,
    is_client_order_id_duplicate,
)
from lightfee.engine.runtime_context import EntryDispatchRuntimeContext
from lightfee.engine.venue_private_health import (
    private_health_status_for_admission_reason,
)
from lightfee.engine.v1_lifecycle import V1TradingLifecycle
from lightfee.strategy.funding_entry_revalidator import FundingEntryRevalidator
from lightfee.strategy.risk_allocator import StrategyRiskAllocator
from lightfee.venues.cid import generate_exchange_cid
from lightfee.venues.transport import ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE


_ASTER_HEADROOM_NUMBER_RE = re.compile(
    r"\b(?P<key>requested_notional|remaining_openable_notional)="
    r"(?P<value>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)"
)


def _entry_leverage_evidence_payload(
    evidence: EntryLeverageEvidence | None,
) -> dict[str, Any]:
    """Serialize only validated leverage evidence for a durable journal."""
    if evidence is None:
        return {
            "present": False,
            "evidence_complete": False,
        }
    return {
        "present": True,
        "venue": evidence.venue.value,
        "symbol": evidence.symbol,
        "requested_leverage": evidence.requested_leverage,
        "effective_leverage": evidence.effective_leverage,
        "account_leverage": evidence.account_leverage,
        "notional_quote": evidence.notional_quote,
        "bracket_verified": evidence.bracket_verified,
        "account_verified": evidence.account_verified,
        "evidence_complete": evidence.evidence_complete,
        "source": evidence.source,
        "observed_at_ms": evidence.observed_at_ms,
    }


def _l2_vwap_and_sweep_limit_for_base_quantity(
    levels: object,
    target_quantity: float,
) -> tuple[float, float, float]:
    """Return ``(VWAP, filled base, IOC sweep limit)`` for one base quantity.

    Local books use base quantities.  The existing quote-notional helpers are
    intentionally not used here: final admission needs the price for the one
    common hedge quantity, not a separately rounded quote amount per venue.

    The sweep limit is the *last displayed level actually needed* for the
    requested quantity.  A buy IOC may therefore consume the asks used in its
    VWAP and no deeper; a sell IOC has the symmetric bid-floor.  It is not a
    second price estimate and must not be replaced with BBO after the final
    economics calculation.
    """
    target = max(float(target_quantity or 0.0), 0.0)
    if target <= 0.0 or not isinstance(levels, list):
        return 0.0, 0.0, 0.0
    filled = 0.0
    notional = 0.0
    sweep_limit = 0.0
    for level in levels:
        price = float(getattr(level, "price", 0.0) or 0.0)
        available = max(float(getattr(level, "quantity", 0.0) or 0.0), 0.0)
        if price <= 0.0 or available <= 0.0:
            continue
        take = min(available, target - filled)
        if take <= 0.0:
            continue
        filled += take
        notional += take * price
        sweep_limit = price
        if filled >= target:
            break
    return (
        (notional / filled, filled, sweep_limit)
        if filled > 0.0
        else (0.0, 0.0, 0.0)
    )


def _l2_vwap_for_base_quantity(levels: object, target_quantity: float) -> tuple[float, float]:
    """Compatibility wrapper for callers that need only VWAP and capacity."""
    vwap, filled, _sweep_limit = _l2_vwap_and_sweep_limit_for_base_quantity(
        levels,
        target_quantity,
    )
    return vwap, filled


def _standard_ioc_price_hints(quote_lease: QuoteLease | None) -> tuple[float, float]:
    """Return bounded buy/sell IOC limits from the final quote lease.

    A complete L2 lease always wins over BBO.  The explicit helper makes it
    impossible for a future route refactor to accidentally calculate depth
    economics and then submit a top-of-book-only IOC.
    """
    long_hint = float(getattr(quote_lease, "long_ask", 0.0) or 0.0)
    short_hint = float(getattr(quote_lease, "short_bid", 0.0) or 0.0)
    if getattr(quote_lease, "l2_vwap_complete", False) is not True:
        return long_hint, short_hint
    long_sweep_limit = float(
        getattr(quote_lease, "long_buy_sweep_limit", 0.0) or 0.0
    )
    short_sweep_limit = float(
        getattr(quote_lease, "short_sell_sweep_limit", 0.0) or 0.0
    )
    if long_sweep_limit > 0.0 and short_sweep_limit > 0.0:
        return long_sweep_limit, short_sweep_limit
    return long_hint, short_hint


def _l2_base_capacity(levels: object) -> float:
    if not isinstance(levels, list):
        return 0.0
    return sum(
        max(float(getattr(level, "quantity", 0.0) or 0.0), 0.0)
        for level in levels
        if float(getattr(level, "price", 0.0) or 0.0) > 0.0
    )


def _common_base_quantity_step(*steps: float | None) -> float:
    """Return the smallest base-quantity grid accepted by every venue.

    The common grid is the decimal LCM, not ``max(step)``.  For example,
    0.003 and 0.002 have a common executable grid of 0.006; choosing 0.003
    would make the 0.002 venue reject an otherwise supposedly paired order.
    """

    decimals: list[Decimal] = []
    for value in steps:
        try:
            decimal = Decimal(str(value or 0.0))
        except (InvalidOperation, TypeError, ValueError):
            return 0.0
        if not decimal.is_finite() or decimal <= 0:
            continue
        decimals.append(decimal.normalize())
    if not decimals:
        return 0.0
    places = max(max(-item.as_tuple().exponent, 0) for item in decimals)
    # Venue quantity metadata is decimal precision.  Refuse pathological
    # precision rather than constructing a huge integer grid at live entry.
    if places > 12:
        return 0.0
    scale = 10**places
    units: list[int] = []
    for decimal in decimals:
        scaled = decimal * scale
        if scaled != scaled.to_integral_value():
            return 0.0
        unit = int(scaled)
        if unit <= 0:
            return 0.0
        units.append(unit)
    common = units[0]
    for unit in units[1:]:
        common = common * unit // gcd(common, unit)
    result = Decimal(common) / Decimal(scale)
    return float(result) if result.is_finite() and result > 0 else 0.0


def _align_base_quantity_down(quantity: float, step: float) -> float:
    """Round down on a proven common decimal grid without float drift."""

    try:
        raw = Decimal(str(quantity))
        grid = Decimal(str(step))
    except (InvalidOperation, TypeError, ValueError):
        return 0.0
    if not raw.is_finite() or not grid.is_finite() or raw <= 0 or grid <= 0:
        return 0.0
    return float((raw / grid).to_integral_value(rounding=ROUND_FLOOR) * grid)


def _aster_headroom_payload_from_error(
    *,
    venue: Venue,
    reason: str,
    error_text: str,
    order_role: str,
) -> dict[str, Any]:
    if venue is not Venue.ASTER or reason not in {
        "max_notional_admission_blocked",
        "aster_headroom_unavailable",
    }:
        return {}

    payload: dict[str, Any] = {
        "order_role": order_role,
        "leverage": ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE,
    }
    parsed: dict[str, float] = {}
    for match in _ASTER_HEADROOM_NUMBER_RE.finditer(str(error_text or "")):
        try:
            parsed[match.group("key")] = float(match.group("value"))
        except (TypeError, ValueError):
            continue

    requested_notional = parsed.get("requested_notional")
    remaining_openable_notional = parsed.get("remaining_openable_notional")
    if requested_notional is not None:
        payload["requested_notional"] = requested_notional
    if remaining_openable_notional is not None:
        payload["remaining_openable_notional"] = remaining_openable_notional
    if requested_notional is not None and remaining_openable_notional is not None:
        payload["notional_gap"] = max(
            requested_notional - remaining_openable_notional,
            0.0,
        )
        payload["headroom_source"] = "exchange_error_text"
        payload["evidence_gap"] = False
        return payload

    payload["evidence_gap"] = True
    payload["headroom_error"] = str(error_text or "")[:500]
    payload["headroom_source"] = (
        "headroom_unavailable"
        if reason == "aster_headroom_unavailable"
        else "exchange_error_without_headroom"
    )
    return payload


def _entry_reject_evidence_matches_venue(
    venue: Venue, reject_evidence: dict[str, Any]
) -> bool:
    evidence_venue = str(reject_evidence.get("venue", "") or "").lower()
    return not evidence_venue or evidence_venue == venue.value


def _entry_reject_evidence_text(reject_evidence: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in (
        "exchange_code",
        "exchange_msg",
        "raw_body",
        "transport_error_type",
        "operation",
    ):
        value = reject_evidence.get(key)
        if value not in (None, ""):
            parts.append(f"{key}={value}")
    extra = reject_evidence.get("extra")
    if isinstance(extra, dict):
        for key in ("code", "retCode", "sCode", "msg", "retMsg", "label"):
            value = extra.get(key)
            if value not in (None, ""):
                parts.append(f"extra.{key}={value}")
    return " ".join(parts)


def _entry_reject_raw_error(
    reject_reason: str, reject_evidence: dict[str, Any]
) -> str:
    evidence_text = _entry_reject_evidence_text(reject_evidence)
    if evidence_text:
        reason = str(reject_reason or "")
        return f"{reason} | exchange_error={evidence_text}" if reason else evidence_text
    return str(reject_reason or "")


def _entry_reject_evidence_payload(
    reject_evidence: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key in (
        "exchange_code",
        "exchange_msg",
        "http_status",
        "operation",
        "transport_error_type",
        "evidence_completeness",
    ):
        value = reject_evidence.get(key)
        if value not in (None, ""):
            payload[key] = value
    request_context = reject_evidence.get("request_context")
    if isinstance(request_context, dict):
        request_key_map = {
            "symbol": "request_symbol",
            "side": "request_side",
            "quantity": "request_quantity",
            "price": "request_price",
            "post_only": "request_post_only",
            "reduce_only": "request_reduce_only",
            "client_order_id": "request_client_order_id",
            "time_in_force": "request_time_in_force",
        }
        for source_key, payload_key in request_key_map.items():
            value = request_context.get(source_key)
            if value not in (None, ""):
                payload[payload_key] = value
    return payload


def _entry_result_admission_source(venue: Venue, reason: str) -> str:
    if venue == Venue.ASTER and reason == "max_notional_admission_blocked":
        return "exchange_5018_fallback"
    return "initial_entry"


class EntryDispatchRuntime:
    def __init__(self, ctx: EntryDispatchRuntimeContext) -> None:
        self.ctx = ctx

    async def decide_after_first_fill(
        self,
        *,
        ctx: EntryContext,
        maker_fill,
        hedge_request: OrderRequest,
        now_ms: int,
    ) -> dict[str, Any]:
        """Use fresh local-L2 evidence to complete or flatten a first fill.

        This is intentionally a narrow decision port: it does not reopen
        candidate selection and it cannot abandon the filled leg.  If either
        executable book is unavailable or stale, the executor receives the
        explicit conservative instruction to complete the hedge.
        """
        default = {
            "action": "complete_hedge",
            "reason": "post_first_fill_market_data_unavailable_complete_hedge",
            "hedge_price": hedge_request.price,
            "market_evidence": {},
        }
        if not self._local_l2_effective_enabled():
            return default

        max_age_ms = max(self.ctx.config.strategy.max_liquidity_snapshot_age_ms, 0)
        if max_age_ms <= 0:
            return default
        long_book = self.ctx.local_l2_runtime.get_book(
            ctx.long_venue.value,
            ctx.symbol,
        )
        short_book = self.ctx.local_l2_runtime.get_book(
            ctx.short_venue.value,
            ctx.symbol,
        )
        if long_book is None or short_book is None:
            return default
        long_observed_at_ms = int(getattr(long_book, "observed_at_ms", 0) or 0)
        short_observed_at_ms = int(getattr(short_book, "observed_at_ms", 0) or 0)
        if (
            long_observed_at_ms <= 0
            or short_observed_at_ms <= 0
            or long_observed_at_ms > now_ms
            or short_observed_at_ms > now_ms
            or now_ms - long_observed_at_ms > max_age_ms
            or now_ms - short_observed_at_ms > max_age_ms
        ):
            return default
        try:
            long_bid = float(long_book.best_bid() or 0.0)
            long_ask = float(long_book.best_ask() or 0.0)
            short_bid = float(short_book.best_bid() or 0.0)
            short_ask = float(short_book.best_ask() or 0.0)
        except Exception:
            return default
        if (
            long_bid <= 0.0
            or long_ask <= long_bid
            or short_bid <= 0.0
            or short_ask <= short_bid
        ):
            return default

        quantity = max(float(getattr(maker_fill, "quantity", 0.0) or 0.0), 0.0)
        maker_price = max(float(getattr(maker_fill, "price", 0.0) or 0.0), 0.0)
        if quantity <= 0.0 or maker_price <= 0.0:
            return default
        if ctx.maker_leg == Side.BUY:
            maker_bid, maker_ask = long_bid, long_ask
            hedge_bid, hedge_ask = short_bid, short_ask
            hedge_venue = ctx.short_venue
            unwind_venue = ctx.long_venue
        else:
            maker_bid, maker_ask = short_bid, short_ask
            hedge_bid, hedge_ask = long_bid, long_ask
            hedge_venue = ctx.long_venue
            unwind_venue = ctx.short_venue

        revalidator = FundingEntryRevalidator()
        hedge_fee_bps = revalidator.taker_fee_bps_for_venue(
            hedge_venue, self.ctx.config.venues
        )
        unwind_fee_bps = revalidator.taker_fee_bps_for_venue(
            unwind_venue, self.ctx.config.venues
        )
        market_decision = revalidator.decide_from_first_fill_market(
            maker_side=ctx.maker_leg,
            maker_fill_price=maker_price,
            quantity=quantity,
            maker_bid=maker_bid,
            maker_ask=maker_ask,
            hedge_bid=hedge_bid,
            hedge_ask=hedge_ask,
            hedge_fee_bps=hedge_fee_bps,
            unwind_fee_bps=unwind_fee_bps,
        )
        if market_decision is None:
            return {
                **default,
                "reason": "post_first_fill_fee_evidence_unavailable_complete_hedge",
                "hedge_price": hedge_bid if ctx.maker_leg == Side.BUY else hedge_ask,
                "market_evidence": {
                    "source": "local_l2_final_bbo",
                    "long_bid": long_bid,
                    "long_ask": long_ask,
                    "short_bid": short_bid,
                    "short_ask": short_ask,
                    "long_observed_at_ms": long_observed_at_ms,
                    "short_observed_at_ms": short_observed_at_ms,
                    "max_age_ms": max_age_ms,
                    "hedge_taker_fee_evidence": hedge_fee_bps is not None,
                    "unwind_taker_fee_evidence": unwind_fee_bps is not None,
                },
            }
        choice = market_decision.decision
        return {
            "action": choice.action,
            "reason": f"post_first_fill_{choice.reason}",
            "hedge_price": market_decision.hedge_price,
            "unwind_price": market_decision.unwind_price,
            "complete_hedge_loss_quote": choice.complete_hedge_loss_quote,
            "unwind_first_leg_loss_quote": choice.unwind_first_leg_loss_quote,
            "complete_hedge_price_loss_quote": choice.complete_hedge_price_loss_quote,
            "unwind_first_leg_price_loss_quote": choice.unwind_first_leg_price_loss_quote,
            "complete_hedge_fee_quote": choice.complete_hedge_fee_quote,
            "unwind_first_leg_fee_quote": choice.unwind_first_leg_fee_quote,
            "market_evidence": {
                "source": "local_l2_final_bbo",
                "long_bid": long_bid,
                "long_ask": long_ask,
                "short_bid": short_bid,
                "short_ask": short_ask,
                "long_observed_at_ms": long_observed_at_ms,
                "short_observed_at_ms": short_observed_at_ms,
                "max_age_ms": max_age_ms,
                "hedge_taker_fee_bps": market_decision.hedge_fee_bps,
                "unwind_taker_fee_bps": market_decision.unwind_fee_bps,
            },
        }

    def get_venue_adapter(self, *args: Any, **kwargs: Any):
        return self.ctx.get_venue_adapter(*args, **kwargs)

    def _safe_positive_float(self, *args: Any, **kwargs: Any):
        return self.ctx._safe_positive_float(*args, **kwargs)

    def _candidate_pair_id(self, *args: Any, **kwargs: Any):
        return self.ctx._candidate_pair_id(*args, **kwargs)

    def _entry_admission_reject_metadata(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_admission_reject_metadata(*args, **kwargs)

    def _record_symbol_admission_block(self, *args: Any, **kwargs: Any):
        return self.ctx._record_symbol_admission_block(*args, **kwargs)

    def _candidate_admission_block(self, *args: Any, **kwargs: Any):
        return self.ctx._candidate_admission_block(*args, **kwargs)

    def _candidate_is_tradeable_for_selection(self, *args: Any, **kwargs: Any):
        return self.ctx._candidate_is_tradeable_for_selection(*args, **kwargs)

    def _gate_reduce_only(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_reduce_only(*args, **kwargs)

    def _gate_pending_close_reconciliation(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_pending_close_reconciliation(*args, **kwargs)

    def _gate_passive_close_pending(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_passive_close_pending(*args, **kwargs)

    def _gate_recovery_ledger(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_recovery_ledger(*args, **kwargs)

    def _gate_pending_entry_dedup(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_pending_entry_dedup(*args, **kwargs)

    def _gate_entry_sizing(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_entry_sizing(*args, **kwargs)

    def _gate_venue_cooldown(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_venue_cooldown(*args, **kwargs)

    def _gate_zero_fill_cooldown(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_zero_fill_cooldown(*args, **kwargs)

    def _entry_quote_lease_max_age_ms(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_quote_lease_max_age_ms(*args, **kwargs)

    def _quote_lease_blocker_family(self, *args: Any, **kwargs: Any):
        return self.ctx._quote_lease_blocker_family(*args, **kwargs)

    def _entry_readiness_provider_name(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_readiness_provider_name(*args, **kwargs)

    def _entry_readiness_provider_uses_quote_lease(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_readiness_provider_uses_quote_lease(*args, **kwargs)

    def _local_l2_effective_enabled(self, *args: Any, **kwargs: Any):
        return self.ctx._local_l2_effective_enabled(*args, **kwargs)

    def _post_only_maker_bbo_guard(self, *args: Any, **kwargs: Any):
        return self.ctx._post_only_maker_bbo_guard(*args, **kwargs)

    def _entry_reject_is_post_only_would_take(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_reject_is_post_only_would_take(*args, **kwargs)

    def _record_post_only_reject_cooldown(self, *args: Any, **kwargs: Any):
        return self.ctx._record_post_only_reject_cooldown(*args, **kwargs)

    def _record_entry_result_admission_blocks(
        self,
        candidate,
        reject_reason: str,
        now_ms: int,
        reject_evidence: dict[str, Any] | None = None,
    ) -> None:
        symbol = str(getattr(candidate, "symbol", "") or "")
        candidate_pair_id = self._candidate_pair_id(candidate)
        reject_evidence = dict(reject_evidence or {})
        evidence_text = _entry_reject_evidence_text(reject_evidence)
        for raw_venue in (
            getattr(candidate, "long_venue", ""),
            getattr(candidate, "short_venue", ""),
        ):
            try:
                venue = Venue.from_str(str(raw_venue))
            except ValueError:
                continue
            if reject_evidence and not _entry_reject_evidence_matches_venue(
                venue, reject_evidence
            ):
                continue
            metadata = self._entry_admission_reject_metadata(venue, reject_reason)
            if metadata is None and evidence_text:
                metadata = self._entry_admission_reject_metadata(venue, evidence_text)
            if metadata:
                reason = str(metadata["reason"])
                raw_error = _entry_reject_raw_error(reject_reason, reject_evidence)
                extra_payload = _aster_headroom_payload_from_error(
                    venue=venue,
                    reason=reason,
                    error_text=raw_error,
                    order_role="entry_result",
                )
                extra_payload.update(_entry_reject_evidence_payload(reject_evidence))
                self._record_symbol_admission_block(
                    venue=venue,
                    symbol=symbol,
                    reason=reason,
                    raw_error=raw_error,
                    now_ms=now_ms,
                    evidence=metadata,
                    source=_entry_result_admission_source(venue, reason),
                    candidate_pair_id=candidate_pair_id,
                    extra_payload=extra_payload,
                )

    async def _prepare_live_entry_leverage_for_candidate(
        self,
        *,
        candidate,
        now_ms: int,
        long_venue: Venue,
        short_venue: Venue,
        notional_quote: float | None = None,
        minimum_evidence_by_venue: dict[Venue, EntryLeverageEvidence] | None = None,
    ) -> tuple[bool, dict[Venue, EntryLeverageEvidence]]:
        """Prepare both venues as one compensable transaction.

        A caller timeout must not abandon a leverage POST after one venue has
        accepted it.  The child task is shielded from caller cancellation,
        publishes a prepared state, and waits for this caller to commit.  A
        cancellation before that commit restores every attempted mutation.
        """
        cancellation_requested = asyncio.Event()
        commit_requested = asyncio.Event()
        finalize_requested = asyncio.Event()
        prepared_result: asyncio.Future[
            tuple[bool, dict[Venue, EntryLeverageEvidence]]
        ] = asyncio.get_running_loop().create_future()
        returnable_result: asyncio.Future[
            tuple[bool, dict[Venue, EntryLeverageEvidence]]
        ] = asyncio.get_running_loop().create_future()

        async def _operation() -> tuple[bool, dict[Venue, EntryLeverageEvidence]]:
            try:
                return await self._prepare_live_entry_leverage_transaction(
                    candidate=candidate,
                    now_ms=now_ms,
                    long_venue=long_venue,
                    short_venue=short_venue,
                    notional_quote=notional_quote,
                    minimum_evidence_by_venue=minimum_evidence_by_venue,
                    cancellation_requested=cancellation_requested,
                    commit_requested=commit_requested,
                    prepared_result=prepared_result,
                    finalize_requested=finalize_requested,
                    returnable_result=returnable_result,
                )
            except BaseException as exc:
                if not prepared_result.done():
                    prepared_result.set_exception(exc)
                elif not returnable_result.done():
                    # Once the caller has observed a prepared state it waits
                    # on this second future.  Do not leave it stranded if a
                    # post-prepare operation (for example the durable ready
                    # receipt) fails unexpectedly.
                    returnable_result.set_exception(exc)
                raise

        operation = asyncio.create_task(
            _operation()
        )

        def _consume_operation_exception(
            task: asyncio.Task[tuple[bool, dict[Venue, EntryLeverageEvidence]]],
        ) -> None:
            with suppress(asyncio.CancelledError):
                task.result()

        operation.add_done_callback(_consume_operation_exception)
        try:
            ready, _prepared_evidence = await asyncio.shield(prepared_result)
            if not ready:
                return await asyncio.shield(operation)
            # The caller has now observed the fully verified prepared state.
            # Only this explicit hand-off permits the child to retain the
            # exchange setting and publish its ready receipt.
            commit_requested.set()
            result = await asyncio.shield(returnable_result)
            # No await follows this hand-off.  A cancellation can therefore
            # only be observed before it (when rollback is still available)
            # or after this method has returned the prepared state.
            finalize_requested.set()
            return result
        except asyncio.CancelledError:
            cancellation_requested.set()
            commit_requested.set()
            finalize_requested.set()
            # ``shield`` leaves the child running.  Complete its verified
            # rollback before propagating the cancellation to the caller.
            # A second cancellation must not detach the compensating task.
            # Consume only those subsequent cancellation requests and keep
            # waiting; the original ``CancelledError`` below remains the
            # caller-visible outcome once every attempted venue is settled.
            while not operation.done():
                try:
                    await asyncio.shield(operation)
                except asyncio.CancelledError:
                    current_task = asyncio.current_task()
                    if current_task is not None:
                        current_task.uncancel()
                    continue
                except BaseException:
                    # The transaction journals its own venue failures; the
                    # caller's cancellation remains authoritative.
                    break
            raise

    async def _prepare_live_entry_leverage_transaction(
        self,
        *,
        candidate,
        now_ms: int,
        long_venue: Venue,
        short_venue: Venue,
        notional_quote: float | None = None,
        minimum_evidence_by_venue: dict[Venue, EntryLeverageEvidence] | None = None,
        cancellation_requested: asyncio.Event | None = None,
        commit_requested: asyncio.Event | None = None,
        prepared_result: asyncio.Future[
            tuple[bool, dict[Venue, EntryLeverageEvidence]]
        ] | None = None,
        finalize_requested: asyncio.Event | None = None,
        returnable_result: asyncio.Future[
            tuple[bool, dict[Venue, EntryLeverageEvidence]]
        ] | None = None,
    ) -> tuple[bool, dict[Venue, EntryLeverageEvidence]]:
        def _finish_without_mutation(
            ready: bool,
        ) -> tuple[bool, dict[Venue, EntryLeverageEvidence]]:
            result = (ready, {})
            if prepared_result is not None and not prepared_result.done():
                prepared_result.set_result(result)
            if returnable_result is not None and not returnable_result.done():
                # Paper mode and adapters without an entry-leverage endpoint
                # have no exchange mutation to commit.  They must still
                # satisfy the outer two-stage protocol rather than leaving
                # its returnable future unresolved.
                returnable_result.set_result(result)
            return result

        def _append_journal_safely(kind: str, payload: dict[str, Any]) -> BaseException | None:
            """Keep rollback progressing even when the audit sink is unavailable."""
            try:
                self.ctx.journal.append(kind, payload)
            except BaseException as exc:
                return exc
            return None

        mode = str(self.ctx.config.runtime.mode or "").lower()
        if mode != "live":
            return _finish_without_mutation(True)
        try:
            target_leverage = int(self.ctx.config.strategy.live_target_leverage or 0)
        except (TypeError, ValueError):
            target_leverage = 0
        if target_leverage <= 0:
            # Live config validation should make this unreachable.  Leaving a
            # silent success here would let a degraded injected config bypass
            # the final account-preparation gate.
            return _finish_without_mutation(False)

        symbol = str(getattr(candidate, "symbol", "") or "")
        if notional_quote is None:
            notional_quote = float(
                getattr(candidate, "entry_notional_quote", 0.0) or 0.0
            )
        else:
            notional_quote = max(float(notional_quote or 0.0), 0.0)
        pair_id = self._candidate_pair_id(candidate)
        tasks: list[tuple[Venue, Any, Any]] = []
        for venue in (long_venue, short_venue):
            if venue not in (Venue.BINANCE, Venue.ASTER):
                continue
            adapter = self.ctx.venue_adapters.get(venue)
            ensure = getattr(adapter, "ensure_entry_leverage", None) if adapter else None
            if callable(ensure):
                inspect = getattr(adapter, "inspect_entry_leverage", None)
                tasks.append((venue, ensure, inspect))
        if not tasks:
            return _finish_without_mutation(True)

        async def _call_operation(
            operation: Any,
            leverage: int,
            operation_notional_quote: float,
        ) -> Any:
            try:
                return await operation(
                    symbol,
                    leverage,
                    notional_quote=operation_notional_quote,
                )
            except TypeError as exc:
                if "notional_quote" not in str(exc):
                    raise
                return await operation(symbol, leverage)

        async def _call(
            venue: Venue,
            ensure: Any,
            inspect: Any,
        ) -> tuple[
            Venue,
            EntryLeverageEvidence | None,
            int,
            EntryLeverageEvidence | None,
            Any,
            bool,
            BaseException | None,
        ]:
            original_leverage = 0
            inspected: EntryLeverageEvidence | None = None
            ensure_attempted = False
            try:
                if callable(inspect):
                    inspected = await _call_operation(
                        inspect,
                        target_leverage,
                        notional_quote,
                    )
                    if not isinstance(inspected, EntryLeverageEvidence):
                        raise ValueError("entry leverage pre-set inspection evidence missing")
                    if inspected.venue != venue or inspected.symbol != symbol:
                        raise ValueError(
                            "entry leverage inspection venue/symbol mismatch"
                        )
                    # Legacy adapters did not expose the exact account
                    # setting.  Their effective value is an acceptable
                    # compatibility fallback only when account truth was
                    # explicitly verified; first-party adapters always
                    # return ``account_leverage``.
                    original_leverage = int(inspected.account_leverage or 0)
                    if original_leverage <= 0 and inspected.account_verified:
                        original_leverage = int(inspected.effective_leverage or 0)
                    if original_leverage <= 0:
                        raise ValueError(
                            "entry leverage original setting unverified"
                        )
                else:
                    raise ValueError("entry leverage pre-set inspection unavailable")
                ensure_attempted = True
                result = await _call_operation(
                    ensure,
                    target_leverage,
                    notional_quote,
                )
                if not isinstance(result, EntryLeverageEvidence):
                    return (
                        venue,
                        None,
                        original_leverage,
                        inspected,
                        ensure,
                        ensure_attempted,
                        None,
                    )
                if result.venue != venue or result.symbol != symbol:
                    return (
                        venue,
                        None,
                        original_leverage,
                        inspected,
                        ensure,
                        ensure_attempted,
                        ValueError("entry leverage evidence venue/symbol mismatch"),
                    )
                return (
                    venue,
                    result,
                    original_leverage,
                    inspected,
                    ensure,
                    ensure_attempted,
                    None,
                )
            except BaseException as exc:
                return (
                    venue,
                    None,
                    original_leverage,
                    inspected,
                    ensure,
                    ensure_attempted,
                    exc,
                )

        results = await asyncio.gather(
            *[_call(venue, ensure, inspect) for venue, ensure, inspect in tasks]
        )
        semantic_failures: list[tuple[Venue, str]] = []
        for venue, evidence, _original, inspected, _ensure, _attempted, exc in results:
            if exc is not None:
                continue
            requirements = [
                item
                for item in (
                    inspected,
                    (minimum_evidence_by_venue or {}).get(venue),
                )
                if isinstance(item, EntryLeverageEvidence) and item.evidence_complete
            ]
            for requirement in requirements:
                if not isinstance(evidence, EntryLeverageEvidence):
                    semantic_failures.append(
                        (venue, "entry leverage final evidence missing")
                    )
                    break
                if not evidence.evidence_complete:
                    semantic_failures.append(
                        (venue, "entry leverage final evidence incomplete")
                    )
                    break
                if evidence.effective_leverage < requirement.effective_leverage:
                    semantic_failures.append(
                        (venue, "entry leverage final evidence weaker than pre-set evidence")
                    )
                    break
        initially_ready = not semantic_failures and all(
            exc is None
            for _venue, _evidence, _original, _inspected, _ensure, _attempted, exc in results
        )
        prepared_evidence = {
            venue: evidence
            for venue, evidence, _original, _inspected, _ensure, _attempted, _exc in results
            if isinstance(evidence, EntryLeverageEvidence)
        }
        if prepared_result is not None:
            prepared_result.set_result(
                (initially_ready, prepared_evidence if initially_ready else {})
            )
        if initially_ready and commit_requested is not None:
            await commit_requested.wait()
        evidence_by_venue: dict[Venue, EntryLeverageEvidence] = {}
        if initially_ready and cancellation_requested is not None and not cancellation_requested.is_set():
            # This receipt is part of successful preparation, not deferred
            # bookkeeping.  The dispatcher must never submit a first leg
            # before recovery can prove the verified leverage state.
            for venue, evidence, _original, _inspected, _ensure, _attempted, _exc in results:
                if evidence is not None:
                    evidence_by_venue[venue] = evidence
                try:
                    self.ctx.journal.append(
                        "execution.entry_leverage_ready",
                        {
                            "venue": venue.value,
                            "symbol": symbol,
                            "target_leverage": target_leverage,
                            "entry_notional_quote": notional_quote,
                            "candidate_pair_id": pair_id,
                            "pair_id": pair_id,
                            "leverage_evidence": _entry_leverage_evidence_payload(evidence),
                            "ts_ms": now_ms,
                        },
                    )
                except BaseException as exc:
                    semantic_failures.append(
                        (venue, f"entry leverage ready receipt failed: {exc}")
                    )
                    break
            initially_ready = not semantic_failures
        if (
            initially_ready
            and cancellation_requested is not None
            and not cancellation_requested.is_set()
            and returnable_result is not None
        ):
            returnable_result.set_result((True, prepared_evidence))
            if finalize_requested is not None:
                await finalize_requested.wait()
        cancellation_failures: list[tuple[Venue, str]] = []
        if cancellation_requested is not None and cancellation_requested.is_set():
            cancellation_failures = [
                (venue, "entry leverage preparation cancelled")
                for venue, _evidence, _original, _inspected, _ensure, attempted, _exc in results
                if attempted
            ]
        ok = not cancellation_failures and not semantic_failures and all(
            exc is None
            for _venue, _evidence, _original, _inspected, _ensure, _attempted, exc in results
        )
        if not ok:
            compensation_failures: list[tuple[Venue, str]] = []
            for (
                venue,
                evidence,
                original_leverage,
                _inspected,
                ensure,
                ensure_attempted,
                _exc,
            ) in results:
                # A transport failure may arrive after the exchange has set
                # leverage.  Any attempted mutation is therefore restored,
                # not merely the calls that returned a success receipt.
                if not ensure_attempted:
                    continue
                if original_leverage <= 0:
                    compensation_failures.append(
                        (venue, "original_leverage_not_verified")
                    )
                    continue
                if (
                    isinstance(evidence, EntryLeverageEvidence)
                    and evidence.account_verified
                    and evidence.account_leverage == original_leverage
                ):
                    continue
                try:
                    restored = await _call_operation(ensure, original_leverage, 0.0)
                    if (
                        not isinstance(restored, EntryLeverageEvidence)
                        or restored.venue != venue
                        or restored.symbol != symbol
                        or not restored.account_verified
                        or restored.account_leverage != original_leverage
                    ):
                        raise ValueError(
                            "entry leverage compensation account setting not verified"
                        )
                except BaseException as restore_error:
                    compensation_failures.append(
                        (venue, f"{type(restore_error).__name__}: {restore_error}"))
                    continue
                receipt_error = _append_journal_safely(
                    "execution.entry_leverage_compensated",
                    {
                        "venue": venue.value,
                        "symbol": symbol,
                        "target_leverage": target_leverage,
                        "restored_leverage": original_leverage,
                        "entry_notional_quote": notional_quote,
                        "candidate_pair_id": pair_id,
                        "pair_id": pair_id,
                        "leverage_evidence": _entry_leverage_evidence_payload(evidence),
                        "restored_leverage_evidence": _entry_leverage_evidence_payload(
                            restored
                        ),
                        "ts_ms": now_ms,
                    },
                )
                if receipt_error is not None:
                    # Exchange restoration succeeded but cannot be proved
                    # durably.  Treat that as a fail-closed admission error,
                    # while continuing to restore every remaining venue.
                    compensation_failures.append(
                        (
                            venue,
                            "entry_leverage_compensation_receipt_failed: "
                            f"{type(receipt_error).__name__}: {receipt_error}",
                        )
                    )
            for venue, restore_error in compensation_failures:
                metadata = {
                    "reason": "entry_leverage_compensation_failed",
                    "official_doc_url": "",
                    "evidence_gap": True,
                }
                # State is armed before its journal writes in the runtime;
                # isolate a broken audit sink so one venue cannot prevent
                # the remaining failures from being fail-closed in memory.
                with suppress(BaseException):
                    self._record_symbol_admission_block(
                        venue=venue,
                        symbol=symbol,
                        reason=metadata["reason"],
                        raw_error=restore_error,
                        now_ms=now_ms,
                        evidence=metadata,
                        source="entry_leverage_compensation",
                        candidate_pair_id=pair_id,
                    )
                _append_journal_safely(
                    "execution.entry_leverage_compensation_failed",
                    {
                        "venue": venue.value,
                        "symbol": symbol,
                        "candidate_pair_id": pair_id,
                        "pair_id": pair_id,
                        "reason": restore_error[:500],
                        "ts_ms": now_ms,
                    },
                )
            failure_by_venue = {
                venue: reason
                for venue, reason in (*semantic_failures, *cancellation_failures)
            }
            for venue, _evidence, _original, _inspected, _ensure, _attempted, exc in results:
                error_text = str(exc) if exc is not None else failure_by_venue.get(venue, "")
                if not error_text:
                    continue
                metadata = self._entry_admission_reject_metadata(venue, error_text)
                if metadata:
                    reason = str(metadata["reason"])
                else:
                    reason = "entry_leverage_unavailable"
                    metadata = {
                        "reason": reason,
                        "official_doc_url": "",
                        "evidence_gap": True,
                    }
                with suppress(BaseException):
                    self._record_symbol_admission_block(
                        venue=venue,
                        symbol=symbol,
                        reason=reason,
                        raw_error=error_text,
                        now_ms=now_ms,
                        evidence=metadata,
                        source="entry_leverage_prepare",
                        candidate_pair_id=pair_id,
                    )
                _append_journal_safely(
                    "execution.entry_leverage_unavailable",
                    {
                        "venue": venue.value,
                        "symbol": symbol,
                        "target_leverage": target_leverage,
                        "entry_notional_quote": notional_quote,
                        "candidate_pair_id": pair_id,
                        "pair_id": pair_id,
                        "reason": reason,
                        "raw_error": error_text[:500],
                        "official_doc_url": metadata.get("official_doc_url", ""),
                        "evidence_gap": bool(metadata.get("evidence_gap", True)),
                        "ts_ms": now_ms,
                    },
                )
            if semantic_failures:
                failed_venue, _failure_reason = semantic_failures[0]
                final = next(
                    (
                        evidence
                        for venue, evidence, _original, _inspected, _ensure, _attempted, _exc in results
                        if venue == failed_venue
                    ),
                    None,
                )
                minimum = (minimum_evidence_by_venue or {}).get(failed_venue)
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason="entry_leverage_weakened_after_sizing",
                    blocked_reasons=["entry_leverage_weakened_after_sizing"],
                    source="entry_leverage_prepare",
                    decision="skip_before_first_leg",
                    extra={
                        "venue": failed_venue.value,
                        "inspected_leverage": _entry_leverage_evidence_payload(minimum),
                        "final_leverage": _entry_leverage_evidence_payload(final),
                        "final_evidence_complete": bool(
                            isinstance(final, EntryLeverageEvidence)
                            and final.evidence_complete
                        ),
                    },
                )
            _append_journal_safely(
                "runtime.entry_blocked_gate",
                {
                    "symbol": symbol,
                    "gate": "entry_leverage_prepare",
                    "reason": "entry_leverage_unavailable",
                    "candidate_pair_id": pair_id,
                    "pair_id": pair_id,
                    "ts_ms": now_ms,
                },
            )
            if returnable_result is not None and not returnable_result.done():
                # The caller has already observed the pre-commit prepared
                # state, so complete its hand-off only after compensation.
                returnable_result.set_result((False, {}))
            return False, {}

        return True, evidence_by_venue

    async def _inspect_live_entry_leverage_for_candidate(
        self,
        *,
        candidate,
        now_ms: int,
        long_venue: Venue,
        short_venue: Venue,
        notional_quote: float | None = None,
    ) -> tuple[bool, dict[Venue, EntryLeverageEvidence]]:
        """Collect GET-only leverage evidence for conservative live sizing.

        This phase is intentionally separate from ``_prepare...``.  A
        candidate can still fail portfolio, duplicate, quote, or post-only
        gates after sizing; none of those rejected candidates may change an
        exchange leverage setting.
        """
        mode = str(self.ctx.config.runtime.mode or "").lower()
        if mode != "live":
            return True, {}
        try:
            target_leverage = int(self.ctx.config.strategy.live_target_leverage or 0)
        except (TypeError, ValueError):
            target_leverage = 0
        if target_leverage <= 0:
            return False, {}

        symbol = str(getattr(candidate, "symbol", "") or "")
        if notional_quote is None:
            notional_quote = float(
                getattr(candidate, "entry_notional_quote", 0.0) or 0.0
            )
        else:
            notional_quote = max(float(notional_quote or 0.0), 0.0)
        pair_id = self._candidate_pair_id(candidate)
        tasks: list[tuple[Venue, Any]] = []
        for venue in (long_venue, short_venue):
            if venue not in (Venue.BINANCE, Venue.ASTER):
                continue
            adapter = self.ctx.venue_adapters.get(venue)
            inspect = getattr(adapter, "inspect_entry_leverage", None) if adapter else None
            if callable(inspect):
                tasks.append((venue, inspect))
        if not tasks:
            return True, {}

        async def _call(
            venue: Venue,
            inspect: Any,
        ) -> tuple[Venue, EntryLeverageEvidence | None, BaseException | None]:
            try:
                try:
                    result = await inspect(
                        symbol,
                        target_leverage,
                        notional_quote=notional_quote,
                    )
                except TypeError as exc:
                    if "notional_quote" not in str(exc):
                        raise
                    result = await inspect(symbol, target_leverage)
                if not isinstance(result, EntryLeverageEvidence):
                    return venue, None, None
                if result.venue != venue or result.symbol != symbol:
                    return venue, None, ValueError(
                        "entry leverage inspection venue/symbol mismatch"
                    )
                return venue, result, None
            except Exception as exc:
                return venue, None, exc

        results = await asyncio.gather(*[_call(venue, inspect) for venue, inspect in tasks])
        ok = True
        evidence_by_venue: dict[Venue, EntryLeverageEvidence] = {}
        for venue, evidence, exc in results:
            if exc is None:
                if evidence is not None:
                    evidence_by_venue[venue] = evidence
                self.ctx.journal.append(
                    "execution.entry_leverage_inspected",
                    {
                        "venue": venue.value,
                        "symbol": symbol,
                        "target_leverage": target_leverage,
                        "entry_notional_quote": notional_quote,
                        "candidate_pair_id": pair_id,
                        "pair_id": pair_id,
                        "leverage_evidence": _entry_leverage_evidence_payload(evidence),
                        "ts_ms": now_ms,
                    },
                )
                continue

            ok = False
            error_text = str(exc)
            metadata = self._entry_admission_reject_metadata(venue, error_text)
            reason = str(metadata["reason"]) if metadata else "entry_leverage_unavailable"
            metadata = metadata or {
                "reason": reason,
                "official_doc_url": "",
                "evidence_gap": True,
            }
            self._record_symbol_admission_block(
                venue=venue,
                symbol=symbol,
                reason=reason,
                raw_error=error_text,
                now_ms=now_ms,
                evidence=metadata,
                source="entry_leverage_inspection",
                candidate_pair_id=pair_id,
            )
            self.ctx.journal.append(
                "execution.entry_leverage_inspection_unavailable",
                {
                    "venue": venue.value,
                    "symbol": symbol,
                    "target_leverage": target_leverage,
                    "entry_notional_quote": notional_quote,
                    "candidate_pair_id": pair_id,
                    "pair_id": pair_id,
                    "reason": reason,
                    "raw_error": error_text[:500],
                    "official_doc_url": metadata.get("official_doc_url", ""),
                    "evidence_gap": bool(metadata.get("evidence_gap", True)),
                    "ts_ms": now_ms,
                },
            )
        if not ok:
            self.ctx.journal.append(
                "runtime.entry_blocked_gate",
                {
                    "symbol": symbol,
                    "gate": "entry_leverage_inspection",
                    "reason": "entry_leverage_unavailable",
                    "candidate_pair_id": pair_id,
                    "pair_id": pair_id,
                    "ts_ms": now_ms,
                },
            )
        return ok, evidence_by_venue

    async def _precheck_entry_admission(
        self,
        *,
        candidate,
        now_ms: int,
        long_venue: Venue,
        short_venue: Venue,
        quantity: float,
        long_order_price_hint: float,
        short_order_price_hint: float,
        maker_venue: Venue,
        entry_type,
        maker_client_order_id: str,
        hedge_client_order_id: str,
    ) -> bool:
        symbol = str(getattr(candidate, "symbol", "") or "")
        pair_id = self._candidate_pair_id(candidate)
        entry_type_value = str(getattr(entry_type, "value", entry_type) or "")

        leg_prechecks = [
            {
                "venue": long_venue,
                "side": Side.BUY,
                "price_hint": long_order_price_hint,
                "order_role": "maker" if maker_venue == long_venue else "hedge",
                "client_order_id": (
                    maker_client_order_id
                    if maker_venue == long_venue
                    else hedge_client_order_id
                ),
            },
            {
                "venue": short_venue,
                "side": Side.SELL,
                "price_hint": short_order_price_hint,
                "order_role": "maker" if maker_venue == short_venue else "hedge",
                "client_order_id": (
                    maker_client_order_id
                    if maker_venue == short_venue
                    else hedge_client_order_id
                ),
            },
        ]

        checked_venues: set[Venue] = set()
        for leg in leg_prechecks:
            venue = leg["venue"]
            if venue in checked_venues:
                continue
            checked_venues.add(venue)
            adapter = self.ctx.venue_adapters.get(venue)
            precheck = getattr(adapter, "precheck_order_admission", None)
            if adapter is None or not callable(precheck):
                continue

            is_maker = leg["order_role"] == "maker"
            passive = is_maker and "passive" in entry_type_value
            price_hint = float(leg["price_hint"] or 0.0)
            request = OrderRequest(
                venue=venue,
                symbol=symbol,
                side=leg["side"],
                quantity=quantity,
                price=price_hint if passive and price_hint > 0 else None,
                reduce_only=False,
                client_order_id=str(leg["client_order_id"] or ""),
                post_only=passive,
                time_in_force=TimeInForce.POST_ONLY if passive else TimeInForce.IOC,
                price_hint=price_hint if price_hint > 0 else None,
                observed_at_ms=now_ms,
            )

            try:
                await precheck(request)
                flush = getattr(self.ctx, "_flush_adapter_order_diagnostics", None)
                if callable(flush):
                    flush(adapter)
                continue
            except OrderSubmitError as exc:
                error_text = str(exc)
                metadata = self._entry_admission_reject_metadata(venue, error_text)
                if metadata:
                    reason = str(metadata["reason"])
                    private_health_status = private_health_status_for_admission_reason(
                        reason
                    )
                    source = f"pre_entry_{venue.value}_precheck"
                    extra_payload: dict[str, Any] = {
                        "order_role": str(leg["order_role"]),
                    }
                    extra_payload.update(
                        _aster_headroom_payload_from_error(
                            venue=venue,
                            reason=reason,
                            error_text=error_text,
                            order_role=str(leg["order_role"]),
                        )
                    )
                    if private_health_status:
                        source = "venue_private_health_precheck"
                        extra_payload.update(
                            {
                                "venue_private_health_status": private_health_status,
                                "cooldown_scope": "venue",
                                "reduce_only": False,
                            }
                        )
                    self._record_symbol_admission_block(
                        venue=venue,
                        symbol=symbol,
                        reason=reason,
                        raw_error=error_text,
                        now_ms=now_ms,
                        evidence=metadata,
                        source=source,
                        candidate_pair_id=pair_id,
                        extra_payload=extra_payload,
                    )
                    self.ctx.journal.append(
                        "runtime.entry_blocked_admission_selection",
                        {
                            "symbol": symbol,
                            "venue": venue.value,
                            "long_venue": long_venue.value,
                            "short_venue": short_venue.value,
                            "pair_id": pair_id,
                            "candidate_pair_id": pair_id,
                            "reason": reason,
                            "stage": "selected_pre_submit",
                            "source": source,
                            "order_role": str(leg["order_role"]),
                            "blocked_count": 1,
                            "ts_ms": now_ms,
                        },
                    )
                    return False
                self.ctx.journal.append(
                    "runtime.entry_admission_precheck_rejected",
                    {
                        "venue": venue.value,
                        "symbol": symbol,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "candidate_pair_id": pair_id,
                        "pair_id": pair_id,
                        "raw_error": error_text[:500],
                        "ts_ms": now_ms,
                    },
                )
                return False
            except Exception as exc:
                self.ctx.journal.append(
                    "runtime.entry_admission_precheck_uncertain",
                    {
                        "venue": venue.value,
                        "symbol": symbol,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "candidate_pair_id": pair_id,
                        "pair_id": pair_id,
                        "raw_error": str(exc)[:500],
                        "ts_ms": now_ms,
                    },
                )
                return False
        return True

    async def _precheck_bybit_entry_admission(self, **kwargs) -> bool:
        return await self._precheck_entry_admission(**kwargs)

    async def _okx_entry_base_quantity_step(
        self, venue: Venue, symbol: str,
    ) -> float | None:
        if venue != Venue.OKX:
            return 0.0
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return None

        explicit_step = self._safe_positive_float(
            getattr(adapter, "okx_base_quantity_step", 0.0)
        )
        if explicit_step > 0:
            return explicit_step

        transport = getattr(adapter, "_transport", None)
        if transport is None:
            return 0.0

        transport_step = self._safe_positive_float(
            getattr(transport, "okx_base_quantity_step", 0.0)
        )
        if transport_step > 0:
            return transport_step

        venue_symbol = symbol
        venue_symbol_fn = getattr(transport, "_venue_symbol", None)
        if callable(venue_symbol_fn):
            try:
                venue_symbol = venue_symbol_fn(symbol)
            except Exception:
                venue_symbol = symbol

        metadata = getattr(transport, "_symbol_metadata", {}) or {}
        for key in (symbol, venue_symbol):
            meta = metadata.get(key) or {}
            if not isinstance(meta, dict):
                continue
            ct_val = self._safe_positive_float(
                meta.get("ct_val") or meta.get("ctVal") or meta.get("contract_size")
            )
            lot_sz = self._safe_positive_float(
                meta.get("lot_sz") or meta.get("lotSz") or meta.get("qty_step")
            )
            if ct_val > 0 and lot_sz > 0:
                return ct_val * lot_sz

        try:
            from lightfee.venues.symbol_rules import get_symbol_rules_cache

            rule = await get_symbol_rules_cache().get(transport, Venue.OKX, venue_symbol)
            ct_val = self._safe_positive_float(getattr(rule, "ct_val", 0.0))
            lot_sz = self._safe_positive_float(getattr(rule, "qty_step", 0.0))
            if ct_val > 0 and lot_sz > 0:
                return ct_val * lot_sz
        except Exception:
            pass

        mode = str(getattr(transport, "mode", "") or "").lower()
        if mode == "live":
            return None
        return 0.0

    async def _okx_aligned_entry_quantity(
        self,
        *,
        long_venue: Venue,
        short_venue: Venue,
        symbol: str,
        quantity: float,
        now_ms: int,
    ) -> tuple[float, float | None]:
        okx_steps: list[float] = []
        missing = False
        for venue in (long_venue, short_venue):
            step = await self._okx_entry_base_quantity_step(venue, symbol)
            if step is None:
                missing = True
            elif step > 0:
                okx_steps.append(step)
        if missing:
            return 0.0, None
        if not okx_steps:
            return quantity, 0.0
        step = max(okx_steps)
        aligned = math.floor((quantity / step) + 1e-12) * step
        if aligned <= 0:
            return 0.0, step
        return aligned, step

    async def _entry_venue_quantity_step(
        self,
        venue: Venue,
        symbol: str,
    ) -> float | None:
        quantity_step, missing_fields = await self._entry_venue_quantity_metadata(
            venue,
            symbol,
        )
        if missing_fields:
            return None
        return quantity_step

    async def _entry_venue_quantity_metadata(
        self,
        venue: Venue,
        symbol: str,
    ) -> tuple[float | None, list[str]]:
        okx_step = await self._okx_entry_base_quantity_step(venue, symbol)
        if okx_step is None:
            return None, ["okx_contract_step"]
        if okx_step > 0:
            return okx_step, []

        adapter = self.get_venue_adapter(venue)
        passive_metadata = getattr(adapter, "passive_metadata", None) if adapter else None
        if callable(passive_metadata):
            try:
                metadata = passive_metadata(symbol) or {}
                if not metadata:
                    return None, ["metadata"]
                quantity_step = self._safe_positive_float(
                    metadata.get("quantity_step")
                    or metadata.get("step_size")
                    or metadata.get("qtyStep")
                )
                missing_fields: list[str] = []
                if quantity_step > 0:
                    for field_name, aliases in {
                        "min_quantity": ("min_quantity", "min_qty", "minOrderQty"),
                        "min_notional": (
                            "min_notional",
                            "min_notional_quote",
                            "minNotionalValue",
                        ),
                    }.items():
                        values = [
                            metadata.get(alias)
                            for alias in aliases
                            if alias in metadata
                        ]
                        if not values:
                            missing_fields.append(field_name)
                            continue
                        if field_name == "min_quantity":
                            min_quantity = self._safe_positive_float(values[0])
                            if min_quantity <= 0:
                                missing_fields.append(field_name)
                        else:
                            try:
                                min_notional = float(values[0] or 0.0)
                            except (TypeError, ValueError):
                                min_notional = -1.0
                            if not math.isfinite(min_notional) or min_notional < 0:
                                missing_fields.append(field_name)
                    return quantity_step, missing_fields
                missing_fields.append("quantity_step")
                return None, missing_fields
            except Exception:
                return None, ["metadata"]
        return None, ["metadata"]

    async def _entry_venue_quantity_metadata_evidence(
        self,
        venue: Venue,
        symbol: str,
        quantity_step: float | None,
        missing_fields: list[str] | None = None,
    ) -> dict:
        evidence = {
            "quantity_step": float(quantity_step or 0.0),
            "min_quantity": 0.0,
            "min_notional": 0.0,
            "missing_fields": list(missing_fields or []),
            "source": "entry_venue_quantity_metadata",
        }
        okx_step = await self._okx_entry_base_quantity_step(venue, symbol)
        if okx_step and okx_step > 0:
            evidence["quantity_step"] = float(okx_step)
            evidence["source"] = "okx_contract_step"
            return evidence
        adapter = self.get_venue_adapter(venue)
        passive_metadata = getattr(adapter, "passive_metadata", None) if adapter else None
        if not callable(passive_metadata):
            if "metadata" not in evidence["missing_fields"]:
                evidence["missing_fields"].append("metadata")
            return evidence
        try:
            metadata = passive_metadata(symbol) or {}
        except Exception:
            if "metadata" not in evidence["missing_fields"]:
                evidence["missing_fields"].append("metadata")
            return evidence
        if not isinstance(metadata, dict) or not metadata:
            if "metadata" not in evidence["missing_fields"]:
                evidence["missing_fields"].append("metadata")
            return evidence
        evidence["quantity_step"] = float(
            self._safe_positive_float(
                metadata.get("quantity_step")
                or metadata.get("step_size")
                or metadata.get("qtyStep")
            )
            or evidence["quantity_step"]
        )
        evidence["min_quantity"] = float(
            self._safe_positive_float(
                metadata.get("min_quantity")
                or metadata.get("min_qty")
                or metadata.get("minOrderQty")
            )
        )
        try:
            min_notional = float(
                metadata.get("min_notional")
                or metadata.get("min_notional_quote")
                or metadata.get("minNotionalValue")
                or 0.0
            )
        except (TypeError, ValueError):
            min_notional = 0.0
        evidence["min_notional"] = min_notional if math.isfinite(min_notional) else 0.0
        return evidence

    def _entry_passive_metadata(self, venue: Venue, symbol: str) -> dict[str, Any]:
        adapter = self.get_venue_adapter(venue)
        passive_metadata = getattr(adapter, "passive_metadata", None) if adapter else None
        if not callable(passive_metadata):
            return {}
        try:
            metadata = passive_metadata(symbol) or {}
        except Exception:
            return {}
        return metadata if isinstance(metadata, dict) else {}

    @staticmethod
    def _entry_quantity_plan_reason(
        *,
        raw_quantity: float,
        common_quantity: float,
        full_target_quantity: float,
        initial_maker_target_quantity: float,
        margin_constrained: bool = False,
    ) -> str:
        if margin_constrained:
            return "margin_health_sizing"
        if abs(common_quantity - raw_quantity) > 1e-9:
            return "exchange_step_rounding"
        if abs(common_quantity - full_target_quantity) > 1e-9:
            return "planner_quantity_adjustment"
        if abs(initial_maker_target_quantity - full_target_quantity) > 1e-9:
            return "passive_initial_slice"
        return "full_target_quantity"

    async def _resolve_live_margin_quantity(
        self,
        *,
        candidate,
        now_ms: int,
        long_venue: Venue,
        short_venue: Venue,
        long_entry_price: float,
        short_entry_price: float,
        current_quantity: float,
        okx_base_step: float | None,
        long_quantity_step: float | None,
        short_quantity_step: float | None,
        leverage_evidence_by_venue: dict[Venue, EntryLeverageEvidence] | None = None,
    ) -> tuple[float, bool] | None:
        """Shrink a paired live entry to verified two-leg collateral.

        The sidecar cannot observe private balances, so it deliberately uses a
        small fallback cap.  Immediately before a live entry we replace that
        provisional cap with per-venue free-collateral evidence, preserve one
        common base quantity, and never increase the shortlisted size.
        """

        evidence_results = await asyncio.gather(
            self.ctx._funding_entry_margin_evidence(long_venue, now_ms),
            self.ctx._funding_entry_margin_evidence(short_venue, now_ms),
        )
        long_evidence, short_evidence = (
            dict(result or {}) for result in evidence_results
        )
        def _margin_value(evidence: dict[str, Any]) -> float | None:
            if evidence.get("evidence_complete") is not True:
                return None
            try:
                value = float(evidence.get("available_margin_quote"))
            except (TypeError, ValueError):
                return None
            return value if math.isfinite(value) and value >= 0.0 else None

        strategy = self.ctx.config.strategy
        try:
            target_leverage = float(strategy.live_target_leverage or 0.0)
        except (TypeError, ValueError):
            target_leverage = 0.0
        common_quantity_step = _common_base_quantity_step(
            okx_base_step,
            long_quantity_step,
            short_quantity_step,
        )
        if common_quantity_step <= 0.0:
            return None
        reference_price = (long_entry_price + short_entry_price) / 2.0
        # A configured leverage is only intent.  A usable sizing value needs
        # exact account+bracket evidence from *both* legs obtained before
        # allocation.  The minimum is the paired constraint.  Any missing,
        # mismatched or incomplete evidence falls back to 1x rather than
        # guessing from a requested leverage or an old account setting.
        evidence_by_venue = leverage_evidence_by_venue or {}
        validated_evidence: dict[Venue, EntryLeverageEvidence] = {}
        for venue in (long_venue, short_venue):
            evidence = evidence_by_venue.get(venue)
            if (
                isinstance(evidence, EntryLeverageEvidence)
                and evidence.venue == venue
                and evidence.symbol == str(candidate.symbol)
                and evidence.evidence_complete is True
            ):
                validated_evidence[venue] = evidence
        leverage_evidence_complete = len(validated_evidence) == 2
        if leverage_evidence_complete:
            sizing_leverage = min(
                float(validated_evidence[long_venue].effective_leverage),
                float(validated_evidence[short_venue].effective_leverage),
            )
            if not math.isfinite(sizing_leverage) or sizing_leverage <= 0.0:
                leverage_evidence_complete = False
                sizing_leverage = 1.0
        else:
            sizing_leverage = 1.0
        allocation = StrategyRiskAllocator().allocate(
            long_entry_price=long_entry_price,
            short_entry_price=short_entry_price,
            long_max_quantity=current_quantity,
            short_max_quantity=current_quantity,
            # The final private check may reduce a public shortlist; it must
            # never use fresh balance evidence to increase its quantity.
            configured_notional_cap_quote=current_quantity * reference_price,
            long_available_margin_quote=_margin_value(long_evidence),
            short_available_margin_quote=_margin_value(short_evidence),
            target_leverage=sizing_leverage,
            health_buffer_ratio=float(
                strategy.funding_risk_health_buffer_ratio or 0.0
            ),
            fallback_notional_quote=float(
                strategy.funding_missing_margin_fallback_notional_quote or 0.0
            ),
        )
        allocated_quantity = min(
            max(float(allocation.base_quantity or 0.0), 0.0),
            current_quantity,
        )
        quantity = _align_base_quantity_down(
            allocated_quantity,
            common_quantity_step,
        )
        pair_id = self._candidate_pair_id(candidate)
        event_payload = {
            "symbol": candidate.symbol,
            "long_venue": long_venue.value,
            "short_venue": short_venue.value,
            "candidate_pair_id": pair_id,
            "pair_id": pair_id,
            "requested_common_quantity": current_quantity,
            "allocated_common_quantity": quantity,
            "common_base_quantity_step": common_quantity_step,
            "long_margin_evidence": long_evidence,
            "short_margin_evidence": short_evidence,
            "margin_evidence_complete": allocation.evidence_complete,
            "constrained_by": list(allocation.constrained_by),
            "configured_target_leverage": target_leverage,
            "sizing_leverage": sizing_leverage,
            "leverage_evidence_complete": leverage_evidence_complete,
            "leverage_evidence_reason": (
                "verified_effective_leverage_min_two_legs"
                if leverage_evidence_complete
                else "unverified_effective_leverage_conservative_1x"
            ),
            "leverage_evidence": {
                venue.value: _entry_leverage_evidence_payload(
                    evidence_by_venue.get(venue)
                )
                for venue in (long_venue, short_venue)
            },
            "health_buffer_ratio": strategy.funding_risk_health_buffer_ratio,
            "fallback_notional_quote": (
                strategy.funding_missing_margin_fallback_notional_quote
            ),
            "ts_ms": now_ms,
        }
        if quantity <= 0.0:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="margin_health_capacity_exhausted",
                blocked_reasons=["margin_health_capacity_exhausted"],
                source="strategy_risk_allocator",
                decision="skip_before_first_leg",
                extra=event_payload,
            )
            self.ctx.journal.append("runtime.entry_margin_sizing_blocked", event_payload)
            return None

        margin_constrained = quantity + 1e-12 < current_quantity
        event_payload["margin_constrained"] = margin_constrained
        self.ctx.journal.append("runtime.entry_margin_sizing", event_payload)
        if margin_constrained:
            # Candidate views are dual-written at admission.  Context,
            # leverage preparation and attribution must all see the actual
            # common quantity, not the public shortlist's provisional cap.
            candidate.entry_target_quantity = quantity
            candidate.entry_notional_quote = quantity * reference_price
        return quantity, margin_constrained

    def _entry_quote_lease_execution_check(
        self,
        candidate,
        now_ms: int,
    ) -> tuple[str, object | None, dict]:
        provider_name = self._entry_readiness_provider_name()
        evidence = {
            "provider": provider_name,
            "source": provider_name,
            "domain": "quote_lease_execution_gate",
            "pair_id": self._candidate_pair_id(candidate),
            "symbol": str(getattr(candidate, "symbol", "")),
            "long_venue": str(getattr(candidate, "long_venue", "")),
            "short_venue": str(getattr(candidate, "short_venue", "")),
            "max_age_ms": self._entry_quote_lease_max_age_ms(),
        }
        def blocked(reason: str, lease: object | None):
            evidence["blocker_family"] = self._quote_lease_blocker_family(reason)
            return reason, lease, evidence

        if (
            self.ctx.config.runtime.mode != "live"
            or not self._entry_readiness_provider_uses_quote_lease()
        ):
            return "", None, evidence

        get_lease = getattr(self.ctx.entry_readiness_provider, "get_lease", None)
        if not callable(get_lease):
            return blocked("missing_quote_lease_provider", None)
        lease = get_lease(evidence["pair_id"])
        if lease is None:
            return blocked("missing_quote_lease", None)

        evidence.update(
            {
                "lease_provider": str(getattr(lease, "provider", "")),
                "created_at_ms": int(getattr(lease, "created_at_ms", 0) or 0),
                "expires_at_ms": int(getattr(lease, "expires_at_ms", 0) or 0),
                "long_observed_at_ms": int(
                    getattr(lease, "long_observed_at_ms", 0) or 0
                ),
                "short_observed_at_ms": int(
                    getattr(lease, "short_observed_at_ms", 0) or 0
                ),
                "long_bid": float(getattr(lease, "long_bid", 0.0) or 0.0),
                "long_ask": float(getattr(lease, "long_ask", 0.0) or 0.0),
                "short_bid": float(getattr(lease, "short_bid", 0.0) or 0.0),
                "short_ask": float(getattr(lease, "short_ask", 0.0) or 0.0),
            }
        )
        if evidence["lease_provider"] != provider_name:
            return blocked("quote_lease_provider_mismatch", lease)
        if str(getattr(lease, "symbol", "")) != evidence["symbol"]:
            return blocked("quote_lease_symbol_mismatch", lease)
        if str(getattr(lease, "long_venue", "")) != evidence["long_venue"]:
            return blocked("quote_lease_long_venue_mismatch", lease)
        if str(getattr(lease, "short_venue", "")) != evidence["short_venue"]:
            return blocked("quote_lease_short_venue_mismatch", lease)

        expires_at_ms = evidence["expires_at_ms"]
        if expires_at_ms <= 0 or now_ms >= expires_at_ms:
            return blocked("expired_quote_lease", lease)

        max_age_ms = int(evidence["max_age_ms"] or 0)
        quote_age_ms: dict[str, int | None] = {}
        for leg in ("long", "short"):
            observed_at_ms = int(evidence[f"{leg}_observed_at_ms"] or 0)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
            evidence[f"{leg}_age_ms"] = age_ms
            quote_age_ms[leg] = age_ms
        evidence["quote_age_ms"] = quote_age_ms
        for leg in ("long", "short"):
            observed_at_ms = int(evidence[f"{leg}_observed_at_ms"] or 0)
            if observed_at_ms > now_ms:
                evidence[f"{leg}_timestamp_after_now"] = True
                return blocked("stale_quote_lease", lease)
            age_ms = evidence[f"{leg}_age_ms"]
            if (
                observed_at_ms <= 0
                or max_age_ms <= 0
                or age_ms is None
                or age_ms > max_age_ms
            ):
                return blocked("stale_quote_lease", lease)

        if (
            evidence["long_bid"] <= 0.0
            or evidence["long_ask"] <= evidence["long_bid"]
            or evidence["short_bid"] <= 0.0
            or evidence["short_ask"] <= evidence["short_bid"]
        ):
            return blocked("invalid_quote_lease", lease)
        return "", lease, evidence

    def _refresh_entry_quote_lease_for_execution(
        self,
        candidate,
        now_ms: int,
        quote_lease_reason: str,
        quote_lease: object | None,
        quote_lease_evidence: dict,
    ) -> tuple[str, object | None, dict]:
        if quote_lease_reason not in {"expired_quote_lease", "stale_quote_lease"}:
            return quote_lease_reason, quote_lease, quote_lease_evidence
        if self._entry_readiness_provider_name() != "ws_bbo_quote_lease":
            return quote_lease_reason, quote_lease, quote_lease_evidence

        decide = getattr(self.ctx.entry_readiness_provider, "decide", None)
        if not callable(decide):
            return quote_lease_reason, quote_lease, quote_lease_evidence

        refresh_evidence = dict(quote_lease_evidence)
        refresh_evidence["execution_refresh_attempted"] = True
        refresh_evidence["execution_refresh_reason"] = quote_lease_reason
        try:
            decision = decide(candidate, now_ms)
        except Exception as exc:
            refresh_evidence["execution_refresh_error"] = (
                f"{type(exc).__name__}: {str(exc)[:300]}"
            )
            return quote_lease_reason, quote_lease, refresh_evidence

        if getattr(decision, "allowed", False) is not True:
            refresh_evidence["execution_refresh_block_reason"] = str(
                getattr(decision, "reason", "")
            )
            refresh_evidence["execution_refresh_evidence"] = dict(
                getattr(decision, "evidence", {}) or {}
            )
            return quote_lease_reason, quote_lease, refresh_evidence

        new_reason, new_lease, new_evidence = self._entry_quote_lease_execution_check(
            candidate,
            now_ms,
        )
        new_evidence = dict(new_evidence)
        new_evidence["execution_refresh_attempted"] = True
        new_evidence["execution_refresh_reason"] = quote_lease_reason
        return new_reason, new_lease, new_evidence

    @staticmethod
    def _quote_lease_reference_price(lease) -> float:
        long_ask = float(getattr(lease, "long_ask", 0.0) or 0.0)
        short_bid = float(getattr(lease, "short_bid", 0.0) or 0.0)
        if long_ask > 0.0 and short_bid > 0.0:
            return (long_ask + short_bid) / 2.0
        return max(long_ask, short_bid, 0.0)

    def _entry_final_gate_skew_blocker(
        self,
        candidate,
        *,
        long_venue,
        short_venue,
        now_ms: int,
    ) -> dict | None:
        if (
            self.ctx.config.runtime.mode != "live"
            or not self._local_l2_effective_enabled()
        ):
            return None
        long_book = self.ctx.local_l2_runtime.get_book(long_venue.value, candidate.symbol)
        short_book = self.ctx.local_l2_runtime.get_book(short_venue.value, candidate.symbol)
        if long_book is None or short_book is None:
            return None
        long_observed_at_ms = int(getattr(long_book, "observed_at_ms", 0) or 0)
        short_observed_at_ms = int(getattr(short_book, "observed_at_ms", 0) or 0)
        if long_observed_at_ms <= 0 or short_observed_at_ms <= 0:
            return None
        if long_observed_at_ms > now_ms or short_observed_at_ms > now_ms:
            return {
                "pair_id": self._candidate_pair_id(candidate),
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "reason": "execution_quote_timestamp_after_now",
                "left_observed_at_ms": long_observed_at_ms,
                "right_observed_at_ms": short_observed_at_ms,
                "ts_ms": now_ms,
            }
        max_skew_ms = max(self.ctx.config.strategy.entry_final_gate_max_skew_ms, 0)
        skew_ms = abs(long_observed_at_ms - short_observed_at_ms)
        if skew_ms <= max_skew_ms:
            return None
        return {
            "pair_id": self._candidate_pair_id(candidate),
            "symbol": candidate.symbol,
            "long_venue": long_venue.value,
            "short_venue": short_venue.value,
            "reason": "execution_skew",
            "skew_ms": skew_ms,
            "max_skew_ms": max_skew_ms,
            "left_venue": long_venue.value,
            "left_observed_at_ms": long_observed_at_ms,
            "right_venue": short_venue.value,
            "right_observed_at_ms": short_observed_at_ms,
            "ts_ms": now_ms,
        }

    def _entry_initial_gate_blocked(self, candidate, now_ms: int) -> bool:
        # Discovery normally enforces these two rules, but dispatch is the
        # final admission boundary.  Keeping the hard gate here prevents a
        # direct caller, stale cache or future orchestration path from
        # bypassing the entry freeze or treating partial economics as live
        # permission.  It intentionally applies only to *new* live entries;
        # pending hedge, residual repair, close and recovery never call this
        # method.
        runtime_mode = str(self.ctx.config.runtime.mode or "paper").lower()
        if runtime_mode == "live":
            strategy = self.ctx.config.strategy
            policy_reason = ""
            # Legacy incident/recovery adapters may still supply a
            # namespace-shaped candidate.  They are fail-closed here; the
            # normal live sidecar path is a typed CandidateInput.
            economics_complete = bool(
                getattr(candidate, "economics_complete", False) is True
                and int(getattr(candidate, "economics_observed_at_ms", 0) or 0) > 0
            )
            if strategy.funding_new_entries_enabled is not True:
                policy_reason = "funding_new_entries_disabled"
            elif not economics_complete:
                policy_reason = "incomplete_economics"
            else:
                # A sidecar candidate is a bounded, versioned assertion, not
                # a transferable permission slip.  In particular, a snapshot
                # produced while `enhanced_shadow` was active must not be
                # admitted after configuration switches to `enhanced_live`:
                # doing so would use quoted funding and bypass the mandatory
                # calibrated forecast gate.  Keep this check at the first
                # live boundary and repeat it in final revalidation below.
                policy_reason = self._live_economics_contract_reason(candidate)
            if policy_reason:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=policy_reason,
                    blocked_reasons=[policy_reason],
                    source="live_entry_policy",
                    decision="skip_dispatch",
                    extra={
                        "funding_new_entries_enabled": strategy.funding_new_entries_enabled,
                        "economics_complete": economics_complete,
                    },
                )
                self.ctx.journal.append(
                    "runtime.entry_blocked_entry_policy",
                    {
                        "symbol": candidate.symbol,
                        "long_venue": candidate.long_venue,
                        "short_venue": candidate.short_venue,
                        "reason": policy_reason,
                        "funding_new_entries_enabled": strategy.funding_new_entries_enabled,
                        "economics_complete": economics_complete,
                        "ts_ms": now_ms,
                    },
                )
                return True

        admission_block = self._candidate_admission_block(candidate, now_ms)
        if admission_block:
            pair_id = self._candidate_pair_id(candidate)
            payload = {
                **admission_block,
                "long_venue": getattr(candidate, "long_venue", ""),
                "short_venue": getattr(candidate, "short_venue", ""),
                "candidate_pair_id": pair_id,
                "pair_id": pair_id,
                "ts_ms": now_ms,
            }
            if payload.get("source"):
                payload["cooldown_source"] = payload["source"]
            payload["source"] = "initial_entry"
            self.ctx.journal.append(
                "runtime.entry_admission_blocked",
                payload,
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=str(payload.get("reason") or "entry_admission_blocked"),
                blocked_reasons=[
                    str(payload.get("reason") or "entry_admission_blocked")
                ],
                source="initial_entry_admission",
                decision="skip_dispatch",
                extra={"candidate_pair_id": pair_id, "pair_id": pair_id},
            )
            return True

        if not self._candidate_is_tradeable_for_selection(candidate):
            blocked_reasons = [
                str(reason)
                for reason in getattr(candidate, "blocked_reasons", []) or []
                if str(reason)
            ]
            if "candidate_not_tradeable_for_selection" not in blocked_reasons:
                blocked_reasons.append("candidate_not_tradeable_for_selection")
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="candidate_not_tradeable_for_selection",
                blocked_reasons=blocked_reasons,
                source="initial_selection_tradeability",
                decision="skip_dispatch",
            )
            self.ctx.journal.append(
                "runtime.entry_blocked_trading_capability",
                {
                    "symbol": getattr(candidate, "symbol", ""),
                    "long_venue": getattr(candidate, "long_venue", ""),
                    "short_venue": getattr(candidate, "short_venue", ""),
                    "reason": "candidate_not_tradeable_for_selection",
                    "ts_ms": now_ms,
                },
            )
            return True

        decision = V1TradingLifecycle.entry_admissibility(
            candidate,
            now_ms=now_ms,
            strategy=self.ctx.config.strategy,
            recovery_ledger=getattr(self, "recovery_ledger", None),
            source="dispatch",
        )
        if not decision.allowed:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=decision.reason,
                blocked_reasons=[decision.reason] if decision.reason else [],
                source="dispatch_lifecycle",
                decision="skip_dispatch",
                extra=dict(getattr(decision, "evidence", {}) or {}),
            )
            self.ctx.journal.append(
                "runtime.entry_blocked_lifecycle",
                {
                    "symbol": getattr(candidate, "symbol", ""),
                    "long_venue": getattr(candidate, "long_venue", ""),
                    "short_venue": getattr(candidate, "short_venue", ""),
                    "reason": decision.reason,
                    **dict(getattr(decision, "evidence", {}) or {}),
                    "ts_ms": now_ms,
                },
            )
            return True

        # V1: apply_runtime_entry_guards — 8+ gate checks before entry
        gates = [
            ("reduce_only", self._gate_reduce_only, ()),
            ("pending_close_reconciliation", self._gate_pending_close_reconciliation, ()),
            ("passive_close_in_flight", self._gate_passive_close_pending, ()),
            ("recovery_ledger", self._gate_recovery_ledger, ()),
            ("pending_entry_duplicate", self._gate_pending_entry_dedup, ()),
            ("entry_sizing", self._gate_entry_sizing, ()),
            ("venue_cooldown", self._gate_venue_cooldown, (now_ms,)),
            ("zero_fill_cooldown", self._gate_zero_fill_cooldown, (now_ms,)),
        ]
        for gate_name, gate_fn, gate_args in gates:
            allowed, reason = gate_fn(candidate, *gate_args)
            if not allowed:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=reason or gate_name,
                    blocked_reasons=[reason or gate_name],
                    source="dispatch_runtime_gate",
                    decision="skip_dispatch",
                    extra={"gate": gate_name},
                )
                self.ctx.journal.append(
                    "runtime.entry_blocked_gate",
                    {"symbol": candidate.symbol, "gate": gate_name, "reason": reason, "ts_ms": now_ms},
                )
                # V1: review.candidate_rejected — per-candidate rejection logging
                self.ctx.journal.append(
                    "review.candidate_rejected",
                    {
                        "symbol": candidate.symbol,
                        "long_venue": candidate.long_venue,
                        "short_venue": candidate.short_venue,
                        "rejected_stage": "runtime_entry_gate",
                        "rejected_reason": f"{gate_name}: {reason}",
                        "ranking_edge_bps": candidate.ranking_edge_bps,
                        "expected_edge_bps": candidate.expected_edge_bps,
                        "funding_edge_bps": candidate.funding_edge_bps,
                        "ts_ms": now_ms,
                    },
                )
                return True
        return False

    def _live_economics_contract_reason(self, candidate) -> str:
        """Return the fail-closed reason when a live candidate is stale by contract.

        Schema v3 makes the economics fields serialisable, but its contents
        can outlive a configuration rollout.  Candidate calculation/model
        versions therefore have to agree with the active configuration.  The
        enhanced-live branch additionally reconstructs the forecast readiness
        predicate from evidence instead of trusting the serialised boolean.
        """
        strategy = self.ctx.config.strategy
        active_version = str(strategy.funding_economics_mode or "v1_exact").lower()
        candidate_version = str(
            getattr(candidate, "calculation_version", "") or ""
        ).lower()
        candidate_epoch = str(getattr(candidate, "model_epoch", "") or "").lower()
        if candidate_version != active_version:
            return "funding_calculation_version_mismatch"
        if candidate_epoch != active_version:
            return "funding_model_epoch_mismatch"
        # Every route has at least one taker leg on entry or exit.  Do not
        # let a hand-built/partially decoded candidate turn an absent fee map
        # into an optimistic zero merely because it selected no passive leg.
        if getattr(candidate, "taker_fee_evidence_complete", False) is not True:
            return "missing_taker_fee_evidence"
        if active_version != "enhanced_live":
            return ""

        try:
            confidence = float(getattr(candidate, "forecast_confidence", 0.0) or 0.0)
            sample_count = int(
                getattr(candidate, "forecast_sample_count", 0) or 0
            )
            shadow_age_ms = int(
                getattr(candidate, "forecast_shadow_age_ms", 0) or 0
            )
            configured_min_samples = int(
                strategy.funding_forecast_min_samples or 0
            )
            configured_shadow_age_ms = max(
                int(strategy.funding_forecast_shadow_min_days or 0), 0
            ) * 24 * 60 * 60 * 1000
        except (TypeError, ValueError, OverflowError):
            return "funding_forecast_evidence_incomplete"
        if (
            getattr(candidate, "forecast_ready", False) is not True
            or not math.isfinite(confidence)
            or confidence <= 0.0
            or configured_min_samples <= 0
            or sample_count < configured_min_samples
            or shadow_age_ms < configured_shadow_age_ms
        ):
            return "funding_forecast_not_ready"
        if getattr(candidate, "forecast_distribution_stable", False) is not True:
            return "funding_forecast_distribution_unstable"
        return ""

    def _emit_entry_dispatch_viability_blocked(
        self,
        candidate,
        now_ms: int,
        *,
        reason: str,
        blocked_reasons: list[str],
        source: str,
        decision: str,
        extra: dict | None = None,
    ) -> None:
        entry_id = str(
            getattr(candidate, "entry_id", "")
            or getattr(candidate, "internal_entry_id", "")
            or getattr(candidate, "pending_owner_id", "")
            or getattr(candidate, "position_id", "")
            or ""
        )
        pair_id = self._candidate_pair_id(candidate)
        payload = {
            "entry_id": entry_id,
            "symbol": getattr(candidate, "symbol", ""),
            "long_venue": getattr(candidate, "long_venue", ""),
            "short_venue": getattr(candidate, "short_venue", ""),
            "candidate_pair_id": pair_id,
            "pair_id": pair_id,
            "reason": reason,
            "blocked_reasons": [item for item in blocked_reasons if item],
            "source": source,
            "decision": decision,
            "ts_ms": now_ms,
        }
        if extra:
            for key, value in extra.items():
                payload.setdefault(key, value)
        self.ctx.journal.append("entry.dispatch_viability_blocked", payload)

    def _entry_price_resolution(
        self,
        candidate,
        now_ms: int,
        price_hint: float,
    ) -> tuple[float, float, float, Any] | None:
        quote_lease = None
        quote_lease_evidence: dict = {}
        quote_lease_reason, quote_lease, quote_lease_evidence = (
            self._entry_quote_lease_execution_check(candidate, now_ms)
        )
        if quote_lease_reason:
            quote_lease_reason, quote_lease, quote_lease_evidence = (
                self._refresh_entry_quote_lease_for_execution(
                    candidate,
                    now_ms,
                    quote_lease_reason,
                    quote_lease,
                    quote_lease_evidence,
                )
            )
        if quote_lease_reason:
            payload = {
                **quote_lease_evidence,
                "reason": quote_lease_reason,
                "ts_ms": now_ms,
            }
            self.ctx.journal.append("runtime.entry_blocked_quote_lease", payload)
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=quote_lease_reason,
                blocked_reasons=[quote_lease_reason],
                source="price_quote_lease",
                decision="skip_dispatch",
                extra=quote_lease_evidence,
            )
            self.ctx.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": candidate.long_venue,
                    "short_venue": candidate.short_venue,
                    "rejected_stage": "quote_lease_execution_gate",
                    "rejected_reason": quote_lease_reason,
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "expected_edge_bps": candidate.expected_edge_bps,
                    "funding_edge_bps": candidate.funding_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return None
        long_order_price_hint = price_hint
        short_order_price_hint = price_hint
        # A readiness provider may have supplied a REST/BBO lease. It is only
        # an eligibility check; live economics must still prefer the freshest
        # local depth snapshot for the requested common base quantity.
        final_l2_quote_lease = self._local_l2_final_quote_lease(candidate, now_ms)
        if final_l2_quote_lease is not None:
            quote_lease = final_l2_quote_lease
        if quote_lease is not None:
            price_hint = self._quote_lease_reference_price(quote_lease)
            long_order_price_hint = float(
                getattr(quote_lease, "long_ask", 0.0) or 0.0
            )
            short_order_price_hint = float(
                getattr(quote_lease, "short_bid", 0.0) or 0.0
            )

        # V1 price gate: require valid quote before constructing entry context
        if price_hint <= 0 or candidate.entry_notional_quote <= 0:
            blocked_reasons = []
            if price_hint <= 0:
                blocked_reasons.append("no_valid_quote")
            if candidate.entry_notional_quote <= 0:
                blocked_reasons.append("no_entry_notional")
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="no_valid_quote",
                blocked_reasons=blocked_reasons,
                source="price_gate",
                decision="skip_dispatch",
                extra={
                    "price_hint": price_hint,
                    "notional": candidate.entry_notional_quote,
                },
            )
            self.ctx.journal.append(
                "runtime.entry_skipped_no_quote",
                {
                    "symbol": candidate.symbol,
                    "price_hint": price_hint,
                    "notional": candidate.entry_notional_quote,
                    "reason": "no valid quote to construct entry — V1 rejects",
                },
            )
            self.ctx.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": candidate.long_venue,
                    "short_venue": candidate.short_venue,
                    "rejected_stage": "price_gate",
                    "rejected_reason": "no valid quote",
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return None
        return price_hint, long_order_price_hint, short_order_price_hint, quote_lease

    def _local_l2_final_quote_lease(
        self,
        candidate,
        now_ms: int,
        *,
        target_quantity: float | None = None,
    ) -> QuoteLease | None:
        """Build final executable BBO evidence from two fresh local L2 books."""
        if (
            self.ctx.config.runtime.mode != "live"
            or not self._local_l2_effective_enabled()
        ):
            return None

        symbol = str(getattr(candidate, "symbol", "") or "").upper()
        long_venue = str(getattr(candidate, "long_venue", "") or "").lower()
        short_venue = str(getattr(candidate, "short_venue", "") or "").lower()
        if not symbol or not long_venue or not short_venue:
            return None
        long_book = self.ctx.local_l2_runtime.get_book(long_venue, symbol)
        short_book = self.ctx.local_l2_runtime.get_book(short_venue, symbol)
        if long_book is None or short_book is None:
            return None

        long_observed_at_ms = int(getattr(long_book, "observed_at_ms", 0) or 0)
        short_observed_at_ms = int(getattr(short_book, "observed_at_ms", 0) or 0)
        max_age_ms = max(self.ctx.config.strategy.max_liquidity_snapshot_age_ms, 0)
        if (
            long_observed_at_ms <= 0
            or short_observed_at_ms <= 0
            or max_age_ms <= 0
            or long_observed_at_ms > now_ms
            or short_observed_at_ms > now_ms
            or now_ms - long_observed_at_ms > max_age_ms
            or now_ms - short_observed_at_ms > max_age_ms
        ):
            return None

        long_bid = float(long_book.best_bid() or 0.0)
        long_ask = float(long_book.best_ask() or 0.0)
        short_bid = float(short_book.best_bid() or 0.0)
        short_ask = float(short_book.best_ask() or 0.0)
        if long_bid <= 0.0 or long_ask <= long_bid:
            return None
        if short_bid <= 0.0 or short_ask <= short_bid:
            return None

        requested_quantity = max(
            float(
                target_quantity
                if target_quantity is not None
                else getattr(candidate, "entry_target_quantity", 0.0)
            )
            or 0.0,
            0.0,
        )
        (
            long_buy_vwap,
            long_vwap_filled,
            long_buy_sweep_limit,
        ) = _l2_vwap_and_sweep_limit_for_base_quantity(
            getattr(long_book, "asks", []), requested_quantity
        )
        (
            short_sell_vwap,
            short_vwap_filled,
            short_sell_sweep_limit,
        ) = _l2_vwap_and_sweep_limit_for_base_quantity(
            getattr(short_book, "bids", []), requested_quantity
        )
        l2_vwap_complete = (
            requested_quantity > 0.0
            and long_vwap_filled + 1e-12 >= requested_quantity
            and short_vwap_filled + 1e-12 >= requested_quantity
        )
        return QuoteLease(
            pair_id=self._candidate_pair_id(candidate),
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            long_bid=long_bid,
            long_ask=long_ask,
            short_bid=short_bid,
            short_ask=short_ask,
            long_observed_at_ms=long_observed_at_ms,
            short_observed_at_ms=short_observed_at_ms,
            created_at_ms=now_ms,
            expires_at_ms=now_ms + max_age_ms,
            provider="local_l2_final_vwap",
            long_buy_vwap=long_buy_vwap,
            short_sell_vwap=short_sell_vwap,
            long_buy_sweep_limit=long_buy_sweep_limit,
            short_sell_sweep_limit=short_sell_sweep_limit,
            long_l2_capacity_quantity=_l2_base_capacity(
                getattr(long_book, "asks", [])
            ),
            short_l2_capacity_quantity=_l2_base_capacity(
                getattr(short_book, "bids", [])
            ),
            l2_vwap_quantity=requested_quantity,
            l2_vwap_complete=l2_vwap_complete,
        )

    def _revalidate_final_entry_economics(
        self,
        *,
        candidate,
        quote_lease: QuoteLease | None,
        required_base_quantity: float,
        now_ms: int,
        source: str,
        execution_is_passive: bool | None = None,
    ) -> bool:
        """Apply the sole live first-leg economics gate from a fresh lease.

        Both the post-shortlist check and the last pre-submit check use this
        method.  Keeping the contract in one place prevents a later execution
        refinement from silently changing the decision formula or its
        fail-closed evidence.
        """
        has_revalidatable_economics = bool(
            getattr(candidate, "economics_complete", False) is True
            and int(getattr(candidate, "economics_observed_at_ms", 0) or 0) > 0
        )
        if self.ctx.config.runtime.mode != "live":
            return True
        if not has_revalidatable_economics:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="incomplete_economics",
                blocked_reasons=["incomplete_economics"],
                source=source,
                decision="skip_before_first_leg",
                extra={
                    "economics_complete": False,
                    "economics_observed_at_ms": int(
                        getattr(candidate, "economics_observed_at_ms", 0) or 0
                    ),
                },
            )
            return False
        contract_reason = self._live_economics_contract_reason(candidate)
        if contract_reason:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=contract_reason,
                blocked_reasons=[contract_reason],
                source=source,
                decision="skip_before_first_leg",
                extra={
                    "calculation_version": str(
                        getattr(candidate, "calculation_version", "") or ""
                    ),
                    "model_epoch": str(
                        getattr(candidate, "model_epoch", "") or ""
                    ),
                    "active_calculation_version": str(
                        self.ctx.config.strategy.funding_economics_mode or "v1_exact"
                    ).lower(),
                    "forecast_ready": bool(
                        getattr(candidate, "forecast_ready", False)
                    ),
                    "forecast_distribution_stable": bool(
                        getattr(candidate, "forecast_distribution_stable", False)
                    ),
                    "forecast_stability_reason": str(
                        getattr(candidate, "forecast_stability_reason", "") or ""
                    ),
                },
            )
            return False

        final_economics = FundingEntryRevalidator().revalidate_before_first_leg(
            candidate,
            long_ask=float(getattr(quote_lease, "long_ask", 0.0) or 0.0),
            short_bid=float(getattr(quote_lease, "short_bid", 0.0) or 0.0),
            long_bid=float(getattr(quote_lease, "long_bid", 0.0) or 0.0),
            short_ask=float(getattr(quote_lease, "short_ask", 0.0) or 0.0),
            now_ms=now_ms,
            config=self.ctx.config.strategy,
            long_buy_vwap=float(getattr(quote_lease, "long_buy_vwap", 0.0) or 0.0),
            short_sell_vwap=float(
                getattr(quote_lease, "short_sell_vwap", 0.0) or 0.0
            ),
            required_base_quantity=max(float(required_base_quantity or 0.0), 0.0),
            l2_vwap_complete=(
                getattr(quote_lease, "l2_vwap_complete", False) is True
            ),
            # Allocator-backed candidates have an executable common quantity
            # and must prove it against L2.  The remaining V1 compatibility
            # adapters retain their historic BBO path for recovery harnesses.
            require_l2_vwap=(
                float(getattr(candidate, "entry_target_quantity", 0.0) or 0.0)
                > 0.0
            ),
            execution_is_passive=execution_is_passive,
        )
        if not final_economics.allowed:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=final_economics.reason,
                blocked_reasons=[final_economics.reason],
                source=source,
                decision="skip_before_first_leg",
                extra={
                    "expected_net_edge_bps": final_economics.edge.expected_net_edge_bps,
                    "worst_case_edge_bps": final_economics.edge.worst_case_edge_bps,
                    "calculation_version": final_economics.edge.calculation_version,
                    "model_epoch": final_economics.edge.model_epoch,
                    "economics_complete": final_economics.edge.economics_complete,
                    "long_ask": final_economics.long_entry_price,
                    "short_bid": final_economics.short_entry_price,
                    "l2_entry_slippage_bps": final_economics.l2_entry_slippage_bps,
                    "l2_vwap_complete": (
                        getattr(quote_lease, "l2_vwap_complete", False) is True
                    ),
                    "l2_vwap_quantity": float(
                        getattr(quote_lease, "l2_vwap_quantity", 0.0) or 0.0
                    ),
                },
            )
            return False

        # Candidate compatibility fields remain dual-written, but only from
        # the immutable EdgeBreakdown produced by FundingEntryRevalidator.
        for field, value in final_economics.edge.candidate_fields().items():
            setattr(candidate, field, value)
        candidate.expected_edge_bps = final_economics.edge.expected_net_edge_bps
        candidate.worst_case_edge_bps = final_economics.edge.worst_case_edge_bps
        candidate.ranking_edge_bps = final_economics.edge.ranking_edge_bps
        # ``fee_bps`` is a legacy attribution field, not an independent
        # economics source.  Keep its dual-write view synchronized when a
        # standard IOC fallback replaces the shortlisted maker fee with four
        # taker fees.
        candidate.fee_bps = (
            final_economics.edge.entry_fee_bps
            + final_economics.edge.exit_fee_bps
        )
        return True

    async def _resolve_entry_quantity_steps(
        self,
        *,
        candidate,
        long_venue: Venue,
        short_venue: Venue,
        price_hint: float,
        now_ms: int,
    ) -> tuple[float, float, float, float | None, float | None] | None:
        # Candidate sizing already chose a common base quantity from the two
        # venues' joint capacity.  Reconstructing it from quote notional here
        # silently changes the hedge ratio whenever the final price moves.
        # Retain the notional conversion only for legacy candidates that have
        # no allocator output.
        allocated_quantity = float(
            getattr(candidate, "entry_target_quantity", 0.0) or 0.0
        )
        raw_quantity = (
            allocated_quantity
            if allocated_quantity > 0.0
            else candidate.entry_notional_quote / price_hint
        )
        # The allocator's common-base capacity is an upper bound, not an
        # advisory.  Re-rounding at dispatch may only reduce a quantity; a
        # stale/direct candidate that exceeds it is fail-closed rather than
        # silently placing more size on one or both venues.
        entry_capacity = float(
            getattr(candidate, "entry_max_executable_quantity", 0.0) or 0.0
        )
        if entry_capacity > 0.0 and raw_quantity - entry_capacity > 1e-9:
            self.ctx.journal.append(
                "runtime.entry_skipped_allocator_capacity_exceeded",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "requested_quantity": raw_quantity,
                    "entry_max_executable_quantity": entry_capacity,
                    "reason": "common_base_capacity_exceeded",
                    "ts_ms": now_ms,
                },
            )
            return None
        quantity = raw_quantity
        quantity, okx_base_step = await self._okx_aligned_entry_quantity(
            long_venue=long_venue,
            short_venue=short_venue,
            symbol=candidate.symbol,
            quantity=quantity,
            now_ms=now_ms,
        )
        if okx_base_step is None:
            self.ctx.journal.append(
                "runtime.entry_skipped_okx_contract_metadata_missing",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "raw_quantity": raw_quantity,
                    "reason": "okx_ct_val_lot_sz_unconfirmed",
                    "ts_ms": now_ms,
                },
            )
            return None
        if quantity <= 0:
            self.ctx.journal.append(
                "runtime.entry_skipped_okx_contract_step",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "okx_base_quantity_step": okx_base_step,
                    "raw_quantity": raw_quantity,
                    "reason": "quantity_below_okx_contract_step",
                    "ts_ms": now_ms,
                },
            )
            return None

        long_quantity_step, long_missing_quantity_fields = await self._entry_venue_quantity_metadata(
            long_venue,
            candidate.symbol,
        )
        short_quantity_step, short_missing_quantity_fields = await self._entry_venue_quantity_metadata(
            short_venue,
            candidate.symbol,
        )
        if (
            long_quantity_step is None
            or short_quantity_step is None
            or long_missing_quantity_fields
            or short_missing_quantity_fields
        ):
            missing_venues = []
            missing_fields = {}
            if long_quantity_step is None:
                missing_venues.append(long_venue.value)
            elif long_missing_quantity_fields:
                missing_venues.append(long_venue.value)
            if short_quantity_step is None:
                missing_venues.append(short_venue.value)
            elif short_missing_quantity_fields:
                missing_venues.append(short_venue.value)
            if long_quantity_step is None or long_missing_quantity_fields:
                missing_fields[long_venue.value] = (
                    long_missing_quantity_fields or ["quantity_step"]
                )
            if short_quantity_step is None or short_missing_quantity_fields:
                missing_fields[short_venue.value] = (
                    short_missing_quantity_fields or ["quantity_step"]
                )
            self.ctx.journal.append(
                "runtime.entry_skipped_quantity_metadata_missing",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "missing_venues": missing_venues,
                    "missing_fields": missing_fields,
                    "raw_quantity": raw_quantity,
                    "common_quantity": quantity,
                    "reason": "quantity_metadata_missing",
                    "ts_ms": now_ms,
                },
            )
            return None
        common_base_quantity_step = _common_base_quantity_step(
            okx_base_step,
            long_quantity_step,
            short_quantity_step,
        )
        enforce_common_grid = (
            str(self.ctx.config.runtime.mode or "").lower() == "live"
            and float(getattr(candidate, "entry_target_quantity", 0.0) or 0.0) > 0.0
        )
        if common_base_quantity_step <= 0.0:
            if not enforce_common_grid:
                return raw_quantity, quantity, okx_base_step, long_quantity_step, short_quantity_step
            self.ctx.journal.append(
                "runtime.entry_skipped_common_quantity_grid_invalid",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "okx_base_quantity_step": okx_base_step,
                    "long_quantity_step": long_quantity_step,
                    "short_quantity_step": short_quantity_step,
                    "reason": "common_base_quantity_grid_invalid",
                    "ts_ms": now_ms,
                },
            )
            return None
        quantity = _align_base_quantity_down(quantity, common_base_quantity_step)
        if quantity <= 0.0:
            if not enforce_common_grid:
                return raw_quantity, raw_quantity, okx_base_step, long_quantity_step, short_quantity_step
            self.ctx.journal.append(
                "runtime.entry_skipped_common_quantity_grid",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "raw_quantity": raw_quantity,
                    "common_base_quantity_step": common_base_quantity_step,
                    "reason": "quantity_below_common_base_quantity_step",
                    "ts_ms": now_ms,
                },
            )
            return None
        return raw_quantity, quantity, okx_base_step, long_quantity_step, short_quantity_step

    def _entry_local_l2_gate_blocked(
        self,
        *,
        candidate,
        long_venue: Venue,
        short_venue: Venue,
        now_ms: int,
    ) -> bool:
        # V1 local-L2 entry readiness gate: block entry when local-L2 enabled
        # but either leg's book is not ready (stale, degraded, cold, etc.)
        if not self._local_l2_effective_enabled():
            return False

        from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2

        long_book = self.ctx.local_l2_runtime.get_book(long_venue.value, candidate.symbol)
        short_book = self.ctx.local_l2_runtime.get_book(short_venue.value, candidate.symbol)

        not_ready_reasons: list[str] = []
        l2_stale_decisions: list[dict] = []
        max_age_ms = self.ctx.config.strategy.max_liquidity_snapshot_age_ms
        if long_book is None:
            not_ready_reasons.append(
                f"long book missing: {long_venue.value}:{candidate.symbol} "
                f"max_age_ms={max_age_ms}"
            )
            l2_stale_decisions.append({
                "venue": long_venue.value,
                "symbol": candidate.symbol,
                "domain": "execution_l2",
                "source": "local_l2",
                "observed_at_ms": 0,
                "age_ms": 0,
                "budget_ms": max_age_ms,
                "decision": "skip_entry",
                "fallback_source": "none",
                "reason": "execution_l2_stale",
                "l2_reason": "missing_book",
                "blocking": True,
            })
        else:
            liq = execution_liquidity_from_local_l2(
                long_book, max_age_ms=max_age_ms,
                now_ms=now_ms, require_ready=True,
            )
            long_age_ms = long_book.age_ms(now_ms)
            if not liq.book_ready:
                not_ready_reasons.append(
                    f"long leg not ready: {long_venue.value}:{candidate.symbol} "
                    f"status={long_book.status.value} pool={long_book.pool.value if hasattr(long_book, 'pool') else 'unknown'} "
                    f"age={long_age_ms}ms max_age_ms={max_age_ms}"
                )
                l2_stale_decisions.append({
                    "venue": long_venue.value,
                    "symbol": candidate.symbol,
                    "domain": "execution_l2",
                    "source": "local_l2",
                    "observed_at_ms": int(getattr(long_book, "observed_at_ms", 0) or 0),
                    "age_ms": int(long_age_ms),
                    "budget_ms": max_age_ms,
                    "decision": "skip_entry",
                    "fallback_source": "none",
                    "reason": "execution_l2_stale",
                    "l2_reason": liq.fallback_reason or "book_not_ready",
                    "book_status": long_book.status.value,
                    "blocking": True,
                })

        if short_book is None:
            not_ready_reasons.append(
                f"short book missing: {short_venue.value}:{candidate.symbol} "
                f"max_age_ms={max_age_ms}"
            )
            l2_stale_decisions.append({
                "venue": short_venue.value,
                "symbol": candidate.symbol,
                "domain": "execution_l2",
                "source": "local_l2",
                "observed_at_ms": 0,
                "age_ms": 0,
                "budget_ms": max_age_ms,
                "decision": "skip_entry",
                "fallback_source": "none",
                "reason": "execution_l2_stale",
                "l2_reason": "missing_book",
                "blocking": True,
            })
        else:
            liq = execution_liquidity_from_local_l2(
                short_book, max_age_ms=max_age_ms,
                now_ms=now_ms, require_ready=True,
            )
            short_age_ms = short_book.age_ms(now_ms)
            if not liq.book_ready:
                not_ready_reasons.append(
                    f"short leg not ready: {short_venue.value}:{candidate.symbol} "
                    f"status={short_book.status.value} pool={short_book.pool.value if hasattr(short_book, 'pool') else 'unknown'} "
                    f"age={short_age_ms}ms max_age_ms={max_age_ms}"
                )
                l2_stale_decisions.append({
                    "venue": short_venue.value,
                    "symbol": candidate.symbol,
                    "domain": "execution_l2",
                    "source": "local_l2",
                    "observed_at_ms": int(getattr(short_book, "observed_at_ms", 0) or 0),
                    "age_ms": int(short_age_ms),
                    "budget_ms": max_age_ms,
                    "decision": "skip_entry",
                    "fallback_source": "none",
                    "reason": "execution_l2_stale",
                    "l2_reason": liq.fallback_reason or "book_not_ready",
                    "book_status": short_book.status.value,
                    "blocking": True,
                })

        if not_ready_reasons:
            pair_id = self._candidate_pair_id(candidate)
            for payload in l2_stale_decisions:
                payload = dict(payload)
                payload["pair_id"] = pair_id
                payload["ts_ms"] = now_ms
                self.ctx.journal.append("runtime.snapshot_freshness_decision", payload)
                self.ctx.journal.append("runtime.execution_l2_stale", payload)
            self.ctx.journal.append(
                "runtime.entry_blocked_local_l2_not_ready",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reasons": not_ready_reasons,
                    "ts_ms": now_ms,
                },
            )
            return True

        skew_blocker = self._entry_final_gate_skew_blocker(
            candidate,
            long_venue=long_venue,
            short_venue=short_venue,
            now_ms=now_ms,
        )
        if skew_blocker is not None:
            self.ctx.journal.append("runtime.entry_blocked_final_gate", skew_blocker)
            self.ctx.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "rejected_stage": "entry_final_gate",
                    "rejected_reason": skew_blocker["reason"],
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "expected_edge_bps": candidate.expected_edge_bps,
                    "funding_edge_bps": candidate.funding_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return True
        return False

    def _build_entry_context(
        self,
        *,
        candidate,
        entry_id: str,
        long_venue: Venue,
        short_venue: Venue,
        effective_quantity: float,
        long_order_price_hint: float,
        short_order_price_hint: float,
        maker_leg: Side,
        entry_type: EntryType,
        route: ExecutionRoute,
        now_ms: int,
        expected_shortfall_bps_entry: float = 0.0,
    ) -> EntryContext:
        def _positive_ms(value) -> int:
            try:
                parsed = int(value or 0)
            except (TypeError, ValueError):
                return 0
            return parsed if parsed > 0 else 0

        def _float_attr(name: str, default: float = 0.0) -> float:
            try:
                return float(getattr(candidate, name, default) or default)
            except (TypeError, ValueError):
                return default

        def _str_attr(name: str, default: str = "") -> str:
            return str(getattr(candidate, name, default) or default)

        opportunity_type = normalize_opportunity_type(
            str(getattr(candidate, "opportunity_type", "aligned") or "aligned")
        )
        long_funding_timestamp_ms = _positive_ms(
            getattr(candidate, "long_funding_timestamp_ms", 0)
        )
        short_funding_timestamp_ms = _positive_ms(
            getattr(candidate, "short_funding_timestamp_ms", 0)
        )
        funding_timestamp_ms = _positive_ms(getattr(candidate, "funding_timestamp_ms", 0))
        first_funding_timestamp_ms = _positive_ms(
            getattr(candidate, "first_funding_timestamp_ms", 0)
        )
        if first_funding_timestamp_ms <= 0 and (long_funding_timestamp_ms > 0 or short_funding_timestamp_ms > 0):
            first_funding_timestamp_ms = min(
                ts for ts in (long_funding_timestamp_ms, short_funding_timestamp_ms)
                if ts > 0
            )
        if funding_timestamp_ms <= 0:
            funding_timestamp_ms = first_funding_timestamp_ms
        second_funding_timestamp_ms = _positive_ms(
            getattr(candidate, "second_funding_timestamp_ms", 0)
        )
        if (
            second_funding_timestamp_ms <= 0
            and opportunity_type == "staggered"
            and long_funding_timestamp_ms > 0
            and short_funding_timestamp_ms > 0
        ):
            later_funding_ms = max(long_funding_timestamp_ms, short_funding_timestamp_ms)
            if later_funding_ms > first_funding_timestamp_ms:
                second_funding_timestamp_ms = later_funding_ms
        funding_edge_bps_entry = float(getattr(candidate, "funding_edge_bps", 0.0) or 0.0)
        total_funding_edge_bps_entry = float(
            getattr(candidate, "total_funding_edge_bps", 0.0) or funding_edge_bps_entry
        )
        expected_edge_bps_entry = float(getattr(candidate, "expected_edge_bps", 0.0) or 0.0)
        worst_case_edge_bps_entry = _float_attr("worst_case_edge_bps")
        enhanced_live_candidate = (
            self.ctx.config.runtime.mode == "live"
            and _float_attr("entry_target_quantity") > 0.0
        )
        if not enhanced_live_candidate:
            expected_shortfall_bps_entry = 0.0
        first_funding_leg = str(getattr(candidate, "first_funding_leg", "") or "")
        entry_maker_leg = (
            "long"
            if entry_type in (EntryType.PASSIVE_INCREMENTAL, EntryType.PASSIVE_FALLBACK)
            and maker_leg == Side.BUY
            else "short"
            if entry_type in (EntryType.PASSIVE_INCREMENTAL, EntryType.PASSIVE_FALLBACK)
            else ""
        )
        entry_liquidity_source_at_entry = (
            getattr(candidate, "entry_liquidity_source_at_entry", None)
            or _str_attr("sizing_liquidity_source")
            or None
        )
        exit_after_first_stage = (
            opportunity_type == "staggered"
            and self.ctx.config.strategy.staggered_exit_mode.lower()
            == "after_first_stage"
        )

        ctx = EntryContext(
            entry_id=entry_id,
            symbol=candidate.symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            long_quantity=effective_quantity,
            short_quantity=effective_quantity,
            long_price_hint=long_order_price_hint,
            short_price_hint=short_order_price_hint,
            maker_leg=maker_leg,
            entry_type=entry_type,
            created_at_ms=now_ms,
            opportunity_type=opportunity_type,
            funding_timestamp_ms=funding_timestamp_ms,
            first_funding_timestamp_ms=first_funding_timestamp_ms,
            long_funding_timestamp_ms=long_funding_timestamp_ms,
            short_funding_timestamp_ms=short_funding_timestamp_ms,
            second_funding_timestamp_ms=second_funding_timestamp_ms,
            first_funding_leg=first_funding_leg,
            funding_edge_bps_entry=funding_edge_bps_entry,
            total_funding_edge_bps_entry=total_funding_edge_bps_entry,
            expected_edge_bps_entry=expected_edge_bps_entry,
            worst_case_edge_bps_entry=worst_case_edge_bps_entry,
            expected_shortfall_bps_entry=expected_shortfall_bps_entry,
            calculation_version=_str_attr("calculation_version", "v1_exact"),
            model_epoch=_str_attr("model_epoch", _str_attr("calculation_version", "v1_exact")),
            economics_observed_at_ms=_positive_ms(
                getattr(candidate, "economics_observed_at_ms", 0)
            ),
            entry_maker_leg=entry_maker_leg,
            exit_maker_leg=_str_attr("exit_maker_leg"),
            entry_cross_bps_entry=_float_attr("entry_cross_bps"),
            fee_bps_entry=_float_attr("fee_bps"),
            entry_slippage_bps_entry=_float_attr("entry_slippage_bps"),
            transfer_bias_bps_entry=_float_attr("transfer_bias_bps"),
            transfer_state_at_entry=getattr(candidate, "transfer_state_at_entry", None),
            entry_liquidity_source_at_entry=entry_liquidity_source_at_entry,
            long_volume_24h_quote_at_entry=_float_attr("long_volume_24h_quote"),
            short_volume_24h_quote_at_entry=_float_attr("short_volume_24h_quote"),
            long_open_interest_quote_at_entry=_float_attr("long_open_interest_quote_at_entry"),
            short_open_interest_quote_at_entry=_float_attr("short_open_interest_quote_at_entry"),
            long_entry_vwap=getattr(candidate, "long_entry_vwap", None),
            short_entry_vwap=getattr(candidate, "short_entry_vwap", None),
            entry_capacity_constrained=bool(
                getattr(candidate, "entry_capacity_constrained", False)
            ),
            entry_target_quantity=_float_attr("entry_target_quantity"),
            long_max_executable_quantity=_float_attr("long_max_executable_quantity"),
            short_max_executable_quantity=_float_attr("short_max_executable_quantity"),
            entry_max_executable_quantity=_float_attr("entry_max_executable_quantity"),
            entry_depth_shortfall_quantity=_float_attr("entry_depth_shortfall_quantity"),
            entry_max_executable_notional_quote=_float_attr(
                "entry_max_executable_notional_quote"
            ),
            entry_depth_capped_at_entry=bool(
                getattr(candidate, "entry_depth_capped_at_entry", False)
            ),
            advisories=list(getattr(candidate, "advisories", []) or []),
            blocked_reasons=list(getattr(candidate, "blocked_reasons", []) or []),
            exit_after_first_stage=exit_after_first_stage,
        )
        candidate_pair_id = self._candidate_pair_id(candidate)

        # V1: review.candidate_shortlisted — candidate passed all gates, entered shortlist
        self.ctx.journal.append(
            "review.candidate_shortlisted",
            {
                "entry_id": entry_id,
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "ranking_edge_bps": candidate.ranking_edge_bps,
                "expected_edge_bps": candidate.expected_edge_bps,
                "funding_edge_bps": candidate.funding_edge_bps,
                "worst_case_edge_bps": candidate.worst_case_edge_bps,
                "opportunity_type": opportunity_type,
                "funding_timestamp_ms": funding_timestamp_ms,
                "first_funding_timestamp_ms": first_funding_timestamp_ms,
                "long_funding_timestamp_ms": long_funding_timestamp_ms,
                "short_funding_timestamp_ms": short_funding_timestamp_ms,
                "second_funding_timestamp_ms": second_funding_timestamp_ms,
                "first_funding_leg": first_funding_leg,
                "exit_after_first_stage": exit_after_first_stage,
                "entry_notional_quote": candidate.entry_notional_quote,
                "route": route.value,
                "maker_leg": maker_leg.value if hasattr(maker_leg, 'value') else str(maker_leg),
                "ts_ms": now_ms,
            },
        )
        return ctx

    def _selected_submit_deadline_ms(self) -> int:
        value = self.ctx.config.strategy.selected_submit_deadline_ms
        return value if value > 0 else 0

    @staticmethod
    def _payload_matches_entry(payload: dict[str, Any], entry_id: str) -> bool:
        for key in ("entry_id", "position_id", "internal_entry_id", "pending_id"):
            if str(payload.get(key) or "") == entry_id:
                return True
        return False

    def _entry_submit_or_terminal_evidence_seen(
        self,
        entry_id: str,
        *,
        after_seq: int,
    ) -> tuple[bool, str]:
        evidence_kinds = {
            "order.submitted",
            "execution.entry_order_submitted",
            "runtime.pending_entry_registered",
            "runtime.entry_dispatched",
            "entry.opened",
            "entry.aborted",
            "entry.passive_unfilled",
            "runtime.position_opened",
        }
        for record in self.ctx.journal.read_all():
            try:
                seq = int(record.get("seq") or 0)
            except (TypeError, ValueError):
                seq = 0
            if seq <= after_seq:
                continue
            kind = str(record.get("kind") or "")
            if kind not in evidence_kinds:
                continue
            payload = record.get("payload", {})
            if not isinstance(payload, dict):
                continue
            if self._payload_matches_entry(payload, entry_id):
                return True, kind
        return False, ""

    async def _execute_entry_with_selected_deadline(
        self,
        *,
        ctx: EntryContext,
        candidate,
        selected_seq: int,
        selected_at_ms: int,
    ):
        deadline_ms = self._selected_submit_deadline_ms()
        if deadline_ms <= 0:
            return await self.ctx.entry_executor.execute(ctx)

        task = asyncio.create_task(self.ctx.entry_executor.execute(ctx))
        done, _pending = await asyncio.wait(
            {task},
            timeout=deadline_ms / 1000.0,
        )
        if done:
            return task.result()

        has_evidence, evidence_kind = self._entry_submit_or_terminal_evidence_seen(
            ctx.entry_id,
            after_seq=selected_seq,
        )
        if has_evidence:
            self.ctx.journal.append(
                "runtime.entry_selected_submit_deadline_waiting_on_order_truth",
                {
                    "entry_id": ctx.entry_id,
                    "symbol": candidate.symbol,
                    "deadline_ms": deadline_ms,
                    "evidence_kind": evidence_kind,
                    "reason": "submit_or_terminal_evidence_seen",
                    "ts_ms": selected_at_ms + deadline_ms,
                },
            )
            return await task

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        candidate_pair_id = self._candidate_pair_id(candidate)
        self.ctx.journal.append(
            "runtime.entry_selected_submit_deadline_exceeded",
            {
                "entry_id": ctx.entry_id,
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "symbol": candidate.symbol,
                "deadline_ms": deadline_ms,
                "reason": "no_submit_or_order_evidence",
                "ts_ms": selected_at_ms + deadline_ms,
            },
        )
        self.ctx.journal.append(
            "review.candidate_rejected",
            {
                "entry_id": ctx.entry_id,
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "symbol": candidate.symbol,
                "long_venue": ctx.long_venue.value,
                "short_venue": ctx.short_venue.value,
                "rejected_stage": "selected_pre_submit_deadline",
                "rejected_reason": "no_submit_or_order_evidence",
                "ts_ms": selected_at_ms + deadline_ms,
            },
        )
        return None

    async def _execute_entry_context(
        self,
        *,
        ctx: EntryContext,
        candidate,
        route: ExecutionRoute,
        effective_quantity: float,
        price_hint: float,
        maker_venue: Venue,
        maker_leg: Side,
        maker_bbo_evidence: dict,
        now_ms: int,
    ) -> bool:
        try:
            # V1: execution.entry_selected — engine decided to open this candidate
            candidate_pair_id = self._candidate_pair_id(candidate)
            selected_seq = self.ctx.journal.append(
                "execution.entry_selected",
                {
                    "symbol": candidate.symbol,
                    "entry_id": ctx.entry_id,
                    "candidate_pair_id": candidate_pair_id,
                    "pair_id": candidate_pair_id,
                    "long_venue": ctx.long_venue.value,
                    "short_venue": ctx.short_venue.value,
                    "quantity": effective_quantity,
                    "route": route.value,
                    "maker_leg": maker_leg.value if hasattr(maker_leg, 'value') else str(maker_leg),
                    "price_hint": price_hint,
                    "long_order_price_hint": ctx.long_price_hint,
                    "short_order_price_hint": ctx.short_price_hint,
                    "opportunity_type": ctx.opportunity_type,
                    "funding_timestamp_ms": ctx.funding_timestamp_ms,
                    "first_funding_timestamp_ms": ctx.first_funding_timestamp_ms,
                    "long_funding_timestamp_ms": ctx.long_funding_timestamp_ms,
                    "short_funding_timestamp_ms": ctx.short_funding_timestamp_ms,
                    "second_funding_timestamp_ms": ctx.second_funding_timestamp_ms,
                    "first_funding_leg": ctx.first_funding_leg,
                    "exit_after_first_stage": ctx.exit_after_first_stage,
                    "funding_edge_bps_entry": ctx.funding_edge_bps_entry,
                    "total_funding_edge_bps_entry": ctx.total_funding_edge_bps_entry,
                    "expected_edge_bps_entry": ctx.expected_edge_bps_entry,
                    "calculation_version": ctx.calculation_version,
                    "model_epoch": ctx.model_epoch,
                    "economics_observed_at_ms": ctx.economics_observed_at_ms,
                    "ts_ms": now_ms,
                },
            )
            result = await self._execute_entry_with_selected_deadline(
                ctx=ctx,
                candidate=candidate,
                selected_seq=selected_seq,
                selected_at_ms=now_ms,
            )
            if result is None:
                return False
            self.ctx.journal.append(
                "runtime.entry_dispatched",
                {
                    "entry_id": ctx.entry_id,
                    "candidate_pair_id": candidate_pair_id,
                    "pair_id": candidate_pair_id,
                    "symbol": candidate.symbol,
                    "route": result.route.value,
                    "state": result.state.value,
                    "has_uncertainty": result.has_uncertainty,
                },
            )
            if (
                result.route == ExecutionRoute.REJECTED
                and self._entry_reject_is_post_only_would_take(
                    getattr(result, "reject_reason", "")
                )
            ):
                self._record_post_only_reject_cooldown(
                    candidate,
                    now_ms,
                    getattr(result, "reject_reason", ""),
                    venue=maker_venue.value,
                    side=maker_leg.value,
                    price=price_hint,
                    bbo=maker_bbo_evidence,
                )
                return True
            if result.open_position is not None:
                self.ctx.state.open_positions[result.open_position.position_id] = result.open_position
                self.ctx.journal.append(
                    "runtime.position_opened",
                    {"position_id": result.open_position.position_id},
                )
            # A terminal post-first-fill unwind cannot be represented as a
            # pending entry: doing so would later resubmit the opposite hedge.
            # Register its remaining one-leg delta directly in the V1 residual
            # queue.  Hedge-rejected flows keep their pending entry and are
            # intentionally not duplicated here.
            if result.residual_task is not None and result.pending_entry is None:
                queue_residual = getattr(self.ctx, "_queue_pending_residual_repair", None)
                if not callable(queue_residual):
                    self.ctx.journal.append(
                        "runtime.entry_residual_queue_unavailable",
                        {
                            "entry_id": ctx.entry_id,
                            "symbol": ctx.symbol,
                            "reason": "terminal_entry_residual_unregistered",
                        },
                    )
                    return False
                queue_residual(
                    result.residual_task,
                    "entry_sync_terminal_residual",
                    {
                        "entry_id": ctx.entry_id,
                        "candidate_pair_id": candidate_pair_id,
                        "source": "post_first_fill_unwind",
                    },
                )
            if result.route == ExecutionRoute.REJECTED and getattr(result, "reject_reason", ""):
                self._record_entry_result_admission_blocks(
                    candidate,
                    str(result.reject_reason),
                    now_ms,
                    reject_evidence=getattr(result, "reject_evidence", None),
                )
            if result.pending_entry is not None:
                if getattr(result.pending_entry, "outcome", "") == "rejected":
                    self.ctx.journal.append(
                        "runtime.rejected_pending_suppressed",
                        {
                            "pending_id": result.pending_entry.pending_id,
                            "symbol": result.pending_entry.symbol,
                            "route": result.route.value,
                            "state": result.state.value,
                            "reason": "maker rejected is terminal in V1",
                        },
                    )
                    return True
                # Track pending entry for reconciliation
                if getattr(result.pending_entry, "created_cycle", 0) == 0:
                    result.pending_entry.created_cycle = int(
                        getattr(self.ctx.state, "tick_count", 0) or 0
                    )
                if getattr(result.pending_entry, "frozen_candidate", None) is None:
                    from dataclasses import asdict, is_dataclass

                    if is_dataclass(candidate):
                        result.pending_entry.frozen_candidate = asdict(candidate)
                    else:
                        result.pending_entry.frozen_candidate = dict(
                            getattr(candidate, "__dict__", {}) or {}
                        )
                self.ctx.state.pending_entries[result.pending_entry.pending_id] = result.pending_entry
                self.ctx._recovery_dedup_index[result.pending_entry.maker_client_order_id] = result.pending_entry.pending_id
                self.ctx._recovery_dedup_index[result.pending_entry.hedge_client_order_id] = result.pending_entry.pending_id
                self.ctx.journal.append(
                    "runtime.pending_entry_registered",
                    {
                        "pending_id": result.pending_entry.pending_id,
                        "symbol": result.pending_entry.symbol,
                        "outcome": result.pending_entry.outcome,
                        "maker_client_order_id": result.pending_entry.maker_client_order_id,
                        "hedge_client_order_id": result.pending_entry.hedge_client_order_id,
                        "opportunity_type": result.pending_entry.opportunity_type,
                        "funding_timestamp_ms": result.pending_entry.funding_timestamp_ms,
                        "first_funding_timestamp_ms": result.pending_entry.first_funding_timestamp_ms,
                        "long_funding_timestamp_ms": result.pending_entry.long_funding_timestamp_ms,
                        "short_funding_timestamp_ms": result.pending_entry.short_funding_timestamp_ms,
                        "second_funding_timestamp_ms": result.pending_entry.second_funding_timestamp_ms,
                        "first_funding_leg": result.pending_entry.first_funding_leg,
                    },
                )
        except Exception as e:
            error_text = str(e)
            if self._entry_reject_is_post_only_would_take(error_text):
                self._record_post_only_reject_cooldown(
                    candidate,
                    now_ms,
                    error_text,
                    venue=maker_venue.value,
                    side=maker_leg.value,
                    price=price_hint,
                    bbo=maker_bbo_evidence,
                )
            else:
                self._record_entry_result_admission_blocks(
                    candidate,
                    error_text,
                    now_ms,
                )
            self.ctx.journal.append(
                "runtime.entry_dispatch_error",
                {"entry_id": ctx.entry_id, "error": error_text},
            )
            return False

        return True

    async def _dispatch_entry(self, candidate, now_ms: int, price_hint: float = 0.0) -> bool:
        """Transform a tradeable candidate into an entry context and execute via entry_executor.

        V1: entry route/maker-leg/price gate from config and execution planner.
        Fix 5: no 1.0 pseudo-price — reject entries without valid quote.
        Fix EN-001: route and maker leg driven by planner, not hardcoded in runtime.
        """
        if self._entry_initial_gate_blocked(candidate, now_ms):
            return False

        # A local-L2 candidate is only eligible for final economics if both
        # books are still executable.  Run the existing V1 guard before price
        # construction so stale books retain their precise lifecycle reason
        # instead of being reported as a generic missing final BBO.
        long_venue = Venue.from_str(candidate.long_venue)
        short_venue = Venue.from_str(candidate.short_venue)
        if self._entry_local_l2_gate_blocked(
            candidate=candidate,
            long_venue=long_venue,
            short_venue=short_venue,
            now_ms=now_ms,
        ):
            return False

        price_resolution = self._entry_price_resolution(candidate, now_ms, price_hint)
        if price_resolution is None:
            return False
        price_hint, long_order_price_hint, short_order_price_hint, quote_lease = price_resolution

        # The sidecar is only a shortlist.  A live first leg is admissible
        # only after repricing the complete economics contract from the fresh
        # executable lease obtained above.  This gate intentionally runs
        # before any order context exists; close/recovery/residual paths do
        # not pass through it.
        # Every sidecar CandidateInput carries the v3 economics fields.  The
        # compatibility method keeps pre-sidecar recovery/harness adapters on
        # their V1 path while complete live candidates fail closed.
        if not self._revalidate_final_entry_economics(
            candidate=candidate,
            quote_lease=quote_lease,
            required_base_quantity=float(
                getattr(quote_lease, "l2_vwap_quantity", 0.0) or 0.0
            ),
            now_ms=now_ms,
            source="final_entry_economics",
        ):
            return False

        quantity_resolution = await self._resolve_entry_quantity_steps(
            candidate=candidate,
            long_venue=long_venue,
            short_venue=short_venue,
            price_hint=price_hint,
            now_ms=now_ms,
        )
        if quantity_resolution is None:
            return False
        raw_quantity, quantity, okx_base_step, long_quantity_step, short_quantity_step = quantity_resolution
        # Portfolio-risk admission is part of the enhanced v3 live contract.
        # Legacy candidates deliberately retain their V1 lifecycle semantics;
        # paper execution is a research surface and must not be silently
        # redefined by a live-only portfolio limit.
        enhanced_live_candidate = (
            self.ctx.config.runtime.mode == "live"
            and float(getattr(candidate, "entry_target_quantity", 0.0) or 0.0) > 0.0
        )
        leverage_evidence_for_sizing: dict[Venue, EntryLeverageEvidence] = {}
        expected_shortfall_bps_entry = 0.0
        if enhanced_live_candidate:
            strategy = self.ctx.config.strategy

            # A new pair may not be admitted against incomplete portfolio
            # state. Pending entries can contain a filled maker leg, so they
            # are neither flat nor safely attributable as a paired position.
            if self.ctx.state.pending_entries:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason="pending_entry_inflight_portfolio_risk",
                    blocked_reasons=["pending_entry_inflight_portfolio_risk"],
                    source="strategy_risk_allocator",
                    decision="skip_before_first_leg",
                    extra={
                        "pending_entry_count": len(self.ctx.state.pending_entries),
                    },
                )
                return False
            risk_long_price = float(
                getattr(quote_lease, "long_buy_vwap", 0.0) or 0.0
            )
            risk_short_price = float(
                getattr(quote_lease, "short_sell_vwap", 0.0) or 0.0
            )
            if risk_long_price <= 0.0:
                risk_long_price = float(
                    getattr(quote_lease, "long_ask", 0.0) or price_hint
                )
            if risk_short_price <= 0.0:
                risk_short_price = float(
                    getattr(quote_lease, "short_bid", 0.0) or price_hint
                )
            basis_es = self.ctx.funding_risk_runtime.estimate_candidate(
                candidate,
                long_venue=long_venue,
                short_venue=short_venue,
                now_ms=now_ms,
            )
            if not basis_es.evidence_complete:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=basis_es.reason,
                    blocked_reasons=[basis_es.reason],
                    source="funding_basis_expected_shortfall",
                    decision="skip_before_first_leg",
                    extra={
                        "expected_shortfall_bps": basis_es.expected_shortfall_bps,
                        "sample_count": basis_es.sample_count,
                        "return_count": basis_es.return_count,
                        "history_ms": basis_es.history_ms,
                        "confidence": basis_es.confidence,
                        "model_version": basis_es.model_version,
                    },
                )
                return False
            # The legacy static field is retained only as a conservative floor
            # during migration.  It can increase the dynamic paired-basis ES,
            # never replace missing historical evidence or reduce the model.
            expected_shortfall_bps_entry = max(
                basis_es.expected_shortfall_bps,
                float(strategy.funding_expected_shortfall_bps or 0.0),
            )
            es_quantity_limit = (
                StrategyRiskAllocator().limit_base_quantity_by_expected_shortfall(
                    long_entry_price=risk_long_price,
                    short_entry_price=risk_short_price,
                    current_base_quantity=quantity,
                    expected_shortfall_bps=expected_shortfall_bps_entry,
                    expected_shortfall_budget_quote=(
                        strategy.funding_expected_shortfall_budget_quote
                    ),
                )
            )
            common_es_step = _common_base_quantity_step(
                okx_base_step,
                long_quantity_step,
                short_quantity_step,
            )
            es_quantity = _align_base_quantity_down(
                es_quantity_limit.base_quantity,
                common_es_step,
            )
            if not es_quantity_limit.evidence_complete or es_quantity <= 0.0:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=(
                        es_quantity_limit.reason
                        or "expected_shortfall_quantity_below_common_grid"
                    ),
                    blocked_reasons=[
                        es_quantity_limit.reason
                        or "expected_shortfall_quantity_below_common_grid"
                    ],
                    source="strategy_risk_allocator",
                    decision="skip_before_first_leg",
                    extra={
                        "expected_shortfall_bps": expected_shortfall_bps_entry,
                        "expected_shortfall_budget_quote": (
                            strategy.funding_expected_shortfall_budget_quote
                        ),
                        "common_base_quantity_step": common_es_step,
                        "maximum_reference_notional_quote": (
                            es_quantity_limit.maximum_reference_notional_quote
                        ),
                    },
                )
                return False
            if es_quantity + 1e-12 < quantity:
                quantity = es_quantity
                candidate.entry_target_quantity = quantity
                candidate.entry_notional_quote = quantity * max(
                    risk_long_price,
                    risk_short_price,
                )
            self.ctx.journal.append(
                "runtime.entry_expected_shortfall_sizing",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "expected_shortfall_bps": expected_shortfall_bps_entry,
                    "dynamic_expected_shortfall_bps": basis_es.expected_shortfall_bps,
                    "legacy_floor_bps": float(
                        strategy.funding_expected_shortfall_bps or 0.0
                    ),
                    "expected_shortfall_budget_quote": (
                        strategy.funding_expected_shortfall_budget_quote
                    ),
                    "maximum_reference_notional_quote": (
                        es_quantity_limit.maximum_reference_notional_quote
                    ),
                    "requested_common_quantity": raw_quantity,
                    "expected_shortfall_common_quantity": quantity,
                    "expected_shortfall_constrained": es_quantity_limit.constrained,
                    "model_version": basis_es.model_version,
                    "sample_count": basis_es.sample_count,
                    "return_count": basis_es.return_count,
                    "history_ms": basis_es.history_ms,
                    "confidence": basis_es.confidence,
                    "ts_ms": now_ms,
                },
            )
            leverage_ready, leverage_evidence_for_sizing = (
                await self._inspect_live_entry_leverage_for_candidate(
                    candidate=candidate,
                    now_ms=now_ms,
                    long_venue=long_venue,
                    short_venue=short_venue,
                    # Each venue sees at most this per-leg notional.  Taking
                    # the more expensive executable side cannot select a
                    # looser bracket than the submitted common quantity.
                    notional_quote=max(risk_long_price, risk_short_price)
                    * quantity,
                )
            )
            if not leverage_ready:
                return False
            margin_resolution = await self._resolve_live_margin_quantity(
                candidate=candidate,
                now_ms=now_ms,
                long_venue=long_venue,
                short_venue=short_venue,
                long_entry_price=risk_long_price,
                short_entry_price=risk_short_price,
                current_quantity=quantity,
                okx_base_step=okx_base_step,
                long_quantity_step=long_quantity_step,
                short_quantity_step=short_quantity_step,
                leverage_evidence_by_venue=leverage_evidence_for_sizing,
            )
            if margin_resolution is None:
                return False
            quantity, margin_constrained = margin_resolution
            risk_admission = StrategyRiskAllocator().assess_portfolio_admission(
                open_positions=self.ctx.state.open_positions.values(),
                symbol=str(candidate.symbol),
                long_venue=long_venue.value,
                short_venue=short_venue.value,
                long_entry_price=risk_long_price,
                short_entry_price=risk_short_price,
                base_quantity=quantity,
                first_funding_timestamp_ms=int(
                    getattr(candidate, "first_funding_timestamp_ms", 0)
                    or getattr(candidate, "funding_timestamp_ms", 0)
                    or 0
                ),
                max_concurrent_positions=strategy.max_concurrent_positions,
                max_single_venue_exposure_quote=(
                    strategy.max_single_venue_exposure_quote
                ),
                max_symbol_exposure_quote=strategy.max_symbol_exposure_quote,
                max_concurrent_venue_pairs=strategy.max_concurrent_venue_pairs,
                max_venue_pair_exposure_quote=(
                    strategy.funding_max_venue_pair_exposure_quote
                ),
                max_global_gross_exposure_quote=(
                    strategy.funding_max_global_gross_exposure_quote
                ),
                max_settlement_bucket_exposure_quote=(
                    strategy.funding_max_settlement_bucket_exposure_quote
                ),
                settlement_crowding_bucket_ms=(
                    strategy.funding_settlement_crowding_bucket_ms
                ),
                max_correlation_group_exposure_quote=(
                    strategy.funding_max_correlation_group_exposure_quote
                ),
                correlation_group_by_symbol=(
                    strategy.funding_correlation_group_by_symbol
                ),
                expected_shortfall_bps=expected_shortfall_bps_entry,
                expected_shortfall_budget_quote=(
                    strategy.funding_expected_shortfall_budget_quote
                ),
            )
            if not risk_admission.allowed:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=risk_admission.reason,
                    blocked_reasons=[risk_admission.reason],
                    source="strategy_risk_allocator",
                    decision="skip_before_first_leg",
                    extra={
                        "reference_notional_quote": risk_admission.reference_notional_quote,
                        "gross_notional_quote": risk_admission.gross_notional_quote,
                        "projected_venue_exposure_quote": (
                            risk_admission.projected_venue_exposure_quote
                        ),
                        "projected_symbol_exposure_quote": (
                            risk_admission.projected_symbol_exposure_quote
                        ),
                        "projected_venue_pair_exposure_quote": (
                            risk_admission.projected_venue_pair_exposure_quote
                        ),
                        "projected_global_gross_exposure_quote": (
                            risk_admission.projected_global_gross_exposure_quote
                        ),
                        "projected_settlement_bucket_exposure_quote": (
                            risk_admission.projected_settlement_bucket_exposure_quote
                        ),
                        "projected_correlation_group_exposure_quote": (
                            risk_admission.projected_correlation_group_exposure_quote
                        ),
                        "projected_expected_shortfall_quote": (
                            risk_admission.projected_expected_shortfall_quote
                        ),
                        "risk_evidence_complete": risk_admission.evidence_complete,
                    },
                )
                return False
        else:
            margin_constrained = False

        # V1 runtime entry guards (apply_runtime_entry_guards)
        gate_checks = [
            ("pending_close_reconciliation", self._gate_pending_close_reconciliation),
            ("passive_close_pending", self._gate_passive_close_pending),
            ("reduce_only", self._gate_reduce_only),
            ("venue_cooldown", self._gate_venue_cooldown),
            ("zero_fill_cooldown", self._gate_zero_fill_cooldown),
        ]
        for gate_name, gate_fn in gate_checks:
            allowed, reason = gate_fn(candidate, now_ms) if gate_name in ("venue_cooldown", "zero_fill_cooldown") else gate_fn(candidate)
            if not allowed:
                self.ctx.journal.append(
                    "runtime.entry_blocked_gate",
                    {"symbol": candidate.symbol, "gate": gate_name, "reason": reason, "ts_ms": now_ms},
                )
                return False

        # V1 entry route planning: derive route and maker leg from execution planner.
        # Strategy config provides min-notional; venue-specific chunk/min-notional
        # are resolved from the adapter or spec when available.
        strategy = self.ctx.config.strategy
        min_notional = strategy.min_entry_leg_notional_quote
        # The economics builder selected the passive leg from current
        # per-leg impact.  Reusing it here is mandatory: otherwise the
        # candidate could receive short-maker fees/spread recovery while the
        # dispatcher rests the long order.  Legacy candidates retain the V1
        # configured preference because they carry no selection evidence.
        candidate_maker_leg = str(
            getattr(candidate, "entry_maker_leg", "") or ""
        ).lower()
        maker_leg = (
            Side.BUY
            if candidate_maker_leg == "long"
            else Side.SELL
            if candidate_maker_leg == "short"
            else Side.BUY
            if strategy.maker_leg_default == "buy"
            else Side.SELL
        )
        if quote_lease is not None:
            if maker_leg == Side.BUY:
                long_order_price_hint = float(
                    getattr(quote_lease, "long_bid", 0.0) or 0.0
                )
                short_order_price_hint = float(
                    getattr(quote_lease, "short_bid", 0.0) or 0.0
                )
            else:
                long_order_price_hint = float(
                    getattr(quote_lease, "long_ask", 0.0) or 0.0
                )
                short_order_price_hint = float(
                    getattr(quote_lease, "short_ask", 0.0) or 0.0
                )
        maker_planner_price = (
            long_order_price_hint if maker_leg == Side.BUY else short_order_price_hint
        )
        hedge_planner_price = (
            short_order_price_hint if maker_leg == Side.BUY else long_order_price_hint
        )

        # V1: min_hedgeable_chunk aligns to venue step and notional floor
        min_hedgeable_chunk = min_notional / price_hint if price_hint > 0 else 0.0
        if okx_base_step and okx_base_step > 0:
            min_hedgeable_chunk = max(min_hedgeable_chunk, okx_base_step)

        route, plan = plan_incremental_entry_execution(
            target_quantity=quantity,
            slice_ratio=strategy.maker_initial_slice_ratio,
            min_hedgeable_chunk=min_hedgeable_chunk,
            maker_min_notional_quote=min_notional,
            maker_price_hint=maker_planner_price if maker_planner_price > 0 else None,
            max_initial_clip_ratio=strategy.entry_max_initial_clip_ratio,
            hedge_min_notional_quote=min_notional,
            hedge_price_hint=hedge_planner_price if hedge_planner_price > 0 else None,
        )

        if route == ExecutionRoute.REJECTED:
            self.ctx.journal.append(
                "runtime.entry_skipped_planner_rejected",
                {
                    "symbol": candidate.symbol,
                    "target_quantity": quantity,
                    "reason": plan.reason or "planner rejected entry",
                },
            )
            return False

        if (
            okx_base_step is not None
            and okx_base_step > 0
            and route == ExecutionRoute.PASSIVE_INCREMENTAL
            and plan.full_target_quantity > 0
        ):
            plan.initial_maker_target_quantity = plan.full_target_quantity

        # Map planner route to EntryType
        if route == ExecutionRoute.PASSIVE_INCREMENTAL:
            entry_type = EntryType.PASSIVE_INCREMENTAL
            effective_quantity = plan.initial_maker_target_quantity
        elif route == ExecutionRoute.FALLBACK_TO_STANDARD:
            entry_type = EntryType.STANDARD_DUAL_TAKER
            effective_quantity = plan.full_target_quantity
        else:
            entry_type = EntryType.STANDARD_DUAL_TAKER
            effective_quantity = plan.full_target_quantity

        # Sizing/rounding and route selection can change the common quantity
        # after the first shortlist revalidation. Refresh L2 once more for
        # the *actual* standard-IOC quantity, recalculate the same immutable
        # economics contract, and derive bounded limits from those exact
        # levels. A passive route deliberately retains V1 post-only pricing.
        if enhanced_live_candidate and entry_type == EntryType.STANDARD_DUAL_TAKER:
            execution_quote_lease = self._local_l2_final_quote_lease(
                candidate,
                now_ms,
                target_quantity=effective_quantity,
            )
            if execution_quote_lease is None:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason="final_l2_execution_quote_unavailable",
                    blocked_reasons=["final_l2_execution_quote_unavailable"],
                    source="final_submit_economics",
                    decision="skip_before_first_leg",
                    extra={"effective_quantity": effective_quantity},
                )
                return False
            if not self._revalidate_final_entry_economics(
                candidate=candidate,
                quote_lease=execution_quote_lease,
                required_base_quantity=effective_quantity,
                now_ms=now_ms,
                source="final_submit_economics",
                execution_is_passive=False,
            ):
                return False
            quote_lease = execution_quote_lease

        if quote_lease is not None and entry_type == EntryType.STANDARD_DUAL_TAKER:
            long_order_price_hint, short_order_price_hint = _standard_ioc_price_hints(
                quote_lease
            )
            if getattr(quote_lease, "l2_vwap_complete", False) is True:
                # ``price_hint`` is audit/UI evidence only here; actual legs
                # use their distinct bounded IOC limits above.
                price_hint = (
                    float(getattr(quote_lease, "long_buy_vwap", 0.0) or 0.0)
                    + float(getattr(quote_lease, "short_sell_vwap", 0.0) or 0.0)
                ) / 2.0

        entry_id = f"entry-{now_ms}-{candidate.symbol}"

        # --- V1 recovery dedup: check for duplicate entries after restart ---
        # Must use the same CID generation as build_entry_orders so the
        # dedup index keys match the actual on-wire clientOrderId.
        maker_venue = long_venue if maker_leg == Side.BUY else short_venue
        hedge_venue = short_venue if maker_leg == Side.BUY else long_venue
        maker_cid = generate_exchange_cid(entry_id, "m", maker_venue)
        hedge_cid = generate_exchange_cid(entry_id, "h", hedge_venue)
        venue_quantity_metadata = {
            long_venue.value: await self._entry_venue_quantity_metadata_evidence(
                long_venue,
                candidate.symbol,
                long_quantity_step,
            ),
            short_venue.value: await self._entry_venue_quantity_metadata_evidence(
                short_venue,
                candidate.symbol,
                short_quantity_step,
            ),
        }
        quantity_plan_reason = self._entry_quantity_plan_reason(
            raw_quantity=raw_quantity,
            common_quantity=quantity,
            full_target_quantity=plan.full_target_quantity,
            initial_maker_target_quantity=plan.initial_maker_target_quantity,
            margin_constrained=margin_constrained,
        )
        quantity_contract = classify_entry_quantity_contract(
            raw_quantity=raw_quantity,
            common_quantity=quantity,
            effective_quantity=effective_quantity,
        )
        self.ctx.journal.append(
            "execution.entry_quantity_plan",
            {
                "entry_id": entry_id,
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "raw_quantity": raw_quantity,
                "common_quantity": quantity,
                "full_target_quantity": plan.full_target_quantity,
                "initial_maker_target_quantity": plan.initial_maker_target_quantity,
                "effective_quantity": effective_quantity,
                "quantity_plan_reason": quantity_plan_reason,
                **quantity_contract,
                "route": route.value,
                "maker_leg": maker_leg.value if hasattr(maker_leg, 'value') else str(maker_leg),
                "min_hedgeable_chunk": min_hedgeable_chunk,
                "okx_base_quantity_step": okx_base_step,
                "common_base_quantity_step": _common_base_quantity_step(
                    okx_base_step,
                    long_quantity_step,
                    short_quantity_step,
                ),
                "margin_constrained": margin_constrained,
                "venue_quantity_steps": {
                    long_venue.value: long_quantity_step or 0.0,
                    short_venue.value: short_quantity_step or 0.0,
                },
                "venue_quantity_metadata": venue_quantity_metadata,
                "ts_ms": now_ms,
            },
        )

        maker_quantity_step = (
            long_quantity_step if maker_leg == Side.BUY else short_quantity_step
        )
        hedge_quantity_step = (
            short_quantity_step if maker_leg == Side.BUY else long_quantity_step
        )
        hedgeability_guard_enabled = bool(
            getattr(
                self.ctx.config.strategy,
                "pending_entry_pre_submit_hedgeable_fill_guard_enabled",
                True,
            )
        )
        small_fill_buffer_enabled = (
            float(
                getattr(
                    self.ctx.config.strategy,
                    "passive_small_fill_buffer_notional_quote",
                    0.0,
                )
                or 0.0
            )
            > 0.0
            and int(
                getattr(
                    self.ctx.config.strategy,
                    "passive_small_fill_buffer_max_wait_ms",
                    0,
                )
                or 0
            )
            > 0
        )
        hedgeability_decision = PendingEntryAdmissionCore.decide(
            PendingEntryAdmissionRequest(
                symbol=candidate.symbol,
                long_venue=long_venue.value,
                short_venue=short_venue.value,
                maker_venue=maker_venue.value,
                hedge_venue=hedge_venue.value,
                entry_type=entry_type.value,
                maker_metadata=self._entry_passive_metadata(
                    maker_venue,
                    candidate.symbol,
                ),
                maker_quantity_step=maker_quantity_step,
                hedge_quantity_step=hedge_quantity_step,
                min_hedgeable_chunk=min_hedgeable_chunk,
                full_target_quantity=plan.full_target_quantity,
                initial_maker_target_quantity=plan.initial_maker_target_quantity,
                guard_enabled=hedgeability_guard_enabled,
                small_fill_buffer_enabled=small_fill_buffer_enabled,
                ts_ms=now_ms,
            )
        )
        if hedgeability_decision.event_kind:
            self.ctx.journal.append(
                hedgeability_decision.event_kind,
                hedgeability_decision.payload or {},
            )
        if not hedgeability_decision.can_submit:
            payload = hedgeability_decision.payload or {}
            self.ctx.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "rejected_stage": "pre_submit_hedgeability_guard",
                    "rejected_reason": payload.get("reason", ""),
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "expected_edge_bps": candidate.expected_edge_bps,
                    "funding_edge_bps": candidate.funding_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return False

        if is_client_order_id_duplicate(maker_cid, self.ctx._recovery_dedup_index):
            self.ctx.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": maker_cid,
                    "reason": "duplicate maker clientOrderId in recovery dedup index",
                },
            )
            return False

        if is_client_order_id_duplicate(hedge_cid, self.ctx._recovery_dedup_index):
            self.ctx.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": hedge_cid,
                    "reason": "duplicate hedge clientOrderId in recovery dedup index",
                },
            )
            return False

        # Check for existing pending entry on same symbol pair
        if has_pending_entry_for_symbol(
            self.ctx.state, candidate.symbol,
            long_venue.value, short_venue.value,
        ):
            self.ctx.journal.append(
                "runtime.entry_skipped_existing_pending",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reason": "pending entry already exists for this symbol pair",
                },
            )
            return False

        if not await self._precheck_entry_admission(
            candidate=candidate,
            now_ms=now_ms,
            long_venue=long_venue,
            short_venue=short_venue,
            quantity=effective_quantity,
            long_order_price_hint=long_order_price_hint,
            short_order_price_hint=short_order_price_hint,
            maker_venue=maker_venue,
            entry_type=entry_type,
            maker_client_order_id=maker_cid,
            hedge_client_order_id=hedge_cid,
        ):
            return False

        maker_bbo_evidence: dict = {}
        if entry_type in (EntryType.PASSIVE_INCREMENTAL, EntryType.PASSIVE_FALLBACK):
            maker_order_price_hint = (
                long_order_price_hint if maker_leg == Side.BUY else short_order_price_hint
            )
            bbo_ok, bbo_reason, maker_bbo_evidence = self._post_only_maker_bbo_guard(
                venue=maker_venue,
                symbol=candidate.symbol,
                side=maker_leg,
                price=maker_order_price_hint,
                now_ms=now_ms,
            )
            if not bbo_ok:
                payload = {
                    **maker_bbo_evidence,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reason": bbo_reason,
                    "ts_ms": now_ms,
                }
                self.ctx.journal.append("runtime.entry_blocked_post_only_bbo", payload)
                self.ctx.journal.append(
                    "review.candidate_rejected",
                    {
                        "symbol": candidate.symbol,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "rejected_stage": "post_only_bbo_gate",
                        "rejected_reason": bbo_reason,
                        "ranking_edge_bps": candidate.ranking_edge_bps,
                        "expected_edge_bps": candidate.expected_edge_bps,
                        "funding_edge_bps": candidate.funding_edge_bps,
                        "ts_ms": now_ms,
                    },
                )
                return False
            repriced_price = float(
                maker_bbo_evidence.get("repriced_price", 0.0) or 0.0
            )
            if repriced_price > 0.0:
                maker_order_price_hint = repriced_price
                if maker_leg == Side.BUY:
                    long_order_price_hint = repriced_price
                else:
                    short_order_price_hint = repriced_price
                self.ctx.journal.append(
                    "runtime.entry_post_only_bbo_repriced",
                    {
                        **maker_bbo_evidence,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "reason": "post_only_would_cross_repriced",
                        "ts_ms": now_ms,
                    },
                )

        # This is the first potentially mutating leverage operation.  Every
        # preceding rejection is venue-side-effect free; a partial two-venue
        # prepare below is compensated back to its just-inspected setting
        # before the candidate is rejected.  The successful post-set evidence
        # must not be weaker than the GET-only evidence used for sizing.
        leverage_ready, final_leverage_evidence = (
            await self._prepare_live_entry_leverage_for_candidate(
                candidate=candidate,
                now_ms=now_ms,
                long_venue=long_venue,
                short_venue=short_venue,
                notional_quote=max(long_order_price_hint, short_order_price_hint)
                * effective_quantity,
                minimum_evidence_by_venue=leverage_evidence_for_sizing,
            )
        )
        if not leverage_ready:
            return False
        for venue, inspected in leverage_evidence_for_sizing.items():
            final = final_leverage_evidence.get(venue)
            if (
                inspected.evidence_complete
                and (
                    not isinstance(final, EntryLeverageEvidence)
                    or not final.evidence_complete
                    or final.effective_leverage < inspected.effective_leverage
                )
            ):
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason="entry_leverage_weakened_after_sizing",
                    blocked_reasons=["entry_leverage_weakened_after_sizing"],
                    source="entry_leverage_prepare",
                    decision="skip_before_first_leg",
                    extra={
                        "venue": venue.value,
                        "inspected_leverage": _entry_leverage_evidence_payload(
                            inspected
                        ),
                        "final_leverage": _entry_leverage_evidence_payload(final),
                        "final_evidence_complete": bool(
                            isinstance(final, EntryLeverageEvidence)
                            and final.evidence_complete
                        ),
                    },
                )
                return False

        ctx = self._build_entry_context(
            candidate=candidate,
            entry_id=entry_id,
            long_venue=long_venue,
            short_venue=short_venue,
            effective_quantity=effective_quantity,
            long_order_price_hint=long_order_price_hint,
            short_order_price_hint=short_order_price_hint,
            maker_leg=maker_leg,
            entry_type=entry_type,
            route=route,
            now_ms=now_ms,
            expected_shortfall_bps_entry=expected_shortfall_bps_entry,
        )

        return await self._execute_entry_context(
            ctx=ctx,
            candidate=candidate,
            route=route,
            effective_quantity=effective_quantity,
            price_hint=price_hint,
            maker_venue=maker_venue,
            maker_leg=maker_leg,
            maker_bbo_evidence=maker_bbo_evidence,
            now_ms=now_ms,
        )
