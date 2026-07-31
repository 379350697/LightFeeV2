"""Entry dispatch runtime delegate.

This module owns behavior mechanically moved from LiveRuntime.
Do not change entry dispatch, order admission, or journal payload semantics while extracting it.
"""

from __future__ import annotations

import asyncio
import math
import re
import time
from contextlib import suppress
from dataclasses import dataclass, field, replace
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
from lightfee.engine.exit import EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS
from lightfee.engine.execution_planner import (
    ExecutionRoute,
    min_hedgeable_chunk_from_notional,
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
from lightfee.engine.task_cancellation import (
    DEFAULT_TASK_CANCEL_DRAIN_S,
    cancel_task_with_bounded_drain,
)
from lightfee.engine.venue_private_health import (
    private_health_status_for_admission_reason,
)
from lightfee.engine.v1_lifecycle import V1TradingLifecycle
from lightfee.marketdata.liquidity import (
    ExecutionLiquiditySnapshot,
    execution_liquidity_from_local_l2,
)
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


@dataclass(frozen=True, slots=True)
class FinalQuoteLeaseResult:
    lease: QuoteLease | None = None
    reason: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)


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
        raw_quantity = getattr(level, "quantity", None)
        if raw_quantity is None:
            raw_quantity = getattr(level, "size", 0.0)
        available = max(float(raw_quantity or 0.0), 0.0)
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


def _ws_bbo_ioc_price_hints(
    quote_lease: QuoteLease | None,
    candidate: Any,
) -> tuple[float, float]:
    """Bind BBO-only IOC limits to the same per-leg slippage budget.

    WS-BBO readiness deliberately has no local depth claim.  It therefore
    submits bounded IOC limits derived from the fresh lease and the immutable
    per-leg shortlist slippage allowance instead of pretending an L2 sweep
    was observed.
    """
    long_ask = float(getattr(quote_lease, "long_ask", 0.0) or 0.0)
    short_bid = float(getattr(quote_lease, "short_bid", 0.0) or 0.0)
    long_slippage_bps = max(
        float(getattr(candidate, "long_entry_slippage_bps", 0.0) or 0.0),
        0.0,
    )
    short_slippage_bps = max(
        float(getattr(candidate, "short_entry_slippage_bps", 0.0) or 0.0),
        0.0,
    )
    return (
        long_ask * (1.0 + long_slippage_bps / 10_000.0),
        short_bid * (1.0 - short_slippage_bps / 10_000.0),
    )


def _candidate_quote_lease_required_base_quantity(
    candidate: Any,
    lease: QuoteLease | None,
) -> float:
    for field_name in ("entry_target_quantity", "entry_max_executable_quantity"):
        try:
            quantity = float(getattr(candidate, field_name, 0.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            quantity = 0.0
        if math.isfinite(quantity) and quantity > 0.0:
            return quantity
    try:
        notional = float(getattr(candidate, "entry_notional_quote", 0.0) or 0.0)
    except (TypeError, ValueError, OverflowError):
        notional = 0.0
    if not math.isfinite(notional) or notional <= 0.0 or lease is None:
        return 0.0
    reference_prices: list[float] = []
    for field_name in ("long_bid", "long_ask", "short_bid", "short_ask"):
        try:
            reference_prices.append(float(getattr(lease, field_name, 0.0) or 0.0))
        except (TypeError, ValueError, OverflowError):
            continue
    reference_price = max(
        (price for price in reference_prices if math.isfinite(price) and price > 0.0),
        default=0.0,
    )
    if reference_price <= 0.0:
        return 0.0
    return notional / reference_price


def _quote_lease_side_capacity_checks(
    candidate: Any,
    lease: QuoteLease | None,
) -> tuple[str, dict[str, Any]]:
    required_quantity = _candidate_quote_lease_required_base_quantity(
        candidate,
        lease,
    )
    evidence: dict[str, Any] = {
        "quote_lease_required_base_quantity": required_quantity,
        "quote_lease_capacity_checks": [],
        "quote_lease_capacity_failed_legs": [],
    }
    if lease is None or required_quantity <= 0.0:
        return "", evidence

    maker_leg = str(
        getattr(candidate, "entry_maker_leg", "")
        or getattr(candidate, "maker_leg", "")
        or ""
    ).strip().lower()
    provider = str(getattr(lease, "provider", "") or "")
    if provider == "local_l2_final_vwap":
        if maker_leg == "long":
            requirements = (
                ("long", "maker", "bid", "long_bid_size"),
                (
                    "short",
                    "hedge",
                    "sell_depth",
                    "short_l2_capacity_quantity",
                ),
            )
        elif maker_leg == "short":
            requirements = (
                ("short", "maker", "ask", "short_ask_size"),
                (
                    "long",
                    "hedge",
                    "buy_depth",
                    "long_l2_capacity_quantity",
                ),
            )
        else:
            requirements = (
                (
                    "long",
                    "taker",
                    "buy_depth",
                    "long_l2_capacity_quantity",
                ),
                (
                    "short",
                    "taker",
                    "sell_depth",
                    "short_l2_capacity_quantity",
                ),
            )
    elif maker_leg == "long":
        requirements = (
            ("long", "maker", "bid", "long_bid_size"),
            ("short", "hedge", "bid", "short_bid_size"),
        )
    elif maker_leg == "short":
        requirements = (
            ("short", "maker", "ask", "short_ask_size"),
            ("long", "hedge", "ask", "long_ask_size"),
        )
    else:
        requirements = (
            ("long", "taker", "ask", "long_ask_size"),
            ("short", "taker", "bid", "short_bid_size"),
        )

    failed: list[dict[str, Any]] = []
    for leg, role, side, size_field in requirements:
        try:
            available_quantity = float(getattr(lease, size_field, 0.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            available_quantity = 0.0
        if not math.isfinite(available_quantity):
            available_quantity = 0.0
        check = {
            "leg": leg,
            "role": role,
            "side": side,
            "size_field": size_field,
            "available_base_quantity": max(available_quantity, 0.0),
            "required_base_quantity": required_quantity,
        }
        evidence["quote_lease_capacity_checks"].append(check)
        if max(available_quantity, 0.0) + 1e-12 < required_quantity:
            failed.append(check)

    if failed:
        evidence["quote_lease_capacity_failed_legs"] = failed
        return "quote_lease_insufficient_bbo_capacity", evidence
    return "", evidence


def _l2_base_capacity(levels: object) -> float:
    if not isinstance(levels, list):
        return 0.0
    capacity = 0.0
    for level in levels:
        raw_quantity = getattr(level, "quantity", None)
        if raw_quantity is None:
            raw_quantity = getattr(level, "size", 0.0)
        price = float(getattr(level, "price", 0.0) or 0.0)
        quantity = max(float(raw_quantity or 0.0), 0.0)
        if price > 0.0:
            capacity += quantity
    return capacity


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
    _SELECTED_SUBMIT_CANCEL_DRAIN_S = DEFAULT_TASK_CANCEL_DRAIN_S

    def __init__(self, ctx: EntryDispatchRuntimeContext) -> None:
        self.ctx = ctx
        self._selected_submit_cancel_cleanup_tasks: set[asyncio.Task] = set()

    async def _cancel_selected_submit_task(self, task: asyncio.Task) -> None:
        """Cancel an unsubmitted entry with a bounded foreground drain."""

        await cancel_task_with_bounded_drain(
            task,
            cleanup_tasks=self._selected_submit_cancel_cleanup_tasks,
            drain_s=self._SELECTED_SUBMIT_CANCEL_DRAIN_S,
        )

    def _local_l2_execution_snapshots(
        self,
        *,
        symbol: str,
        long_venue: str,
        short_venue: str,
        now_ms: int,
        max_age_ms: int,
    ) -> tuple[ExecutionLiquiditySnapshot, ExecutionLiquiditySnapshot] | None:
        """Return detached, execution-ready L2 snapshots for both entry legs.

        This is the sole raw-local-book bridge for entry decisions that can
        produce a price, submit limit, or immutable execution-quality
        evidence.  It deliberately relies on the market-data canonical gate:
        HOT lifecycle, non-future freshness, and whole-book structure must
        all pass before any top-of-book or depth value is consumed.
        """
        if max_age_ms <= 0:
            return None
        long_book = self.ctx.local_l2_runtime.get_book(long_venue, symbol)
        short_book = self.ctx.local_l2_runtime.get_book(short_venue, symbol)
        if long_book is None or short_book is None:
            return None
        long_snapshot = execution_liquidity_from_local_l2(
            long_book,
            max_age_ms=max_age_ms,
            now_ms=now_ms,
            require_ready=True,
        )
        short_snapshot = execution_liquidity_from_local_l2(
            short_book,
            max_age_ms=max_age_ms,
            now_ms=now_ms,
            require_ready=True,
        )
        if not long_snapshot.book_ready or not short_snapshot.book_ready:
            return None
        return long_snapshot, short_snapshot

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
        snapshots = self._local_l2_execution_snapshots(
            symbol=ctx.symbol,
            long_venue=ctx.long_venue.value,
            short_venue=ctx.short_venue.value,
            now_ms=now_ms,
            max_age_ms=max_age_ms,
        )
        if snapshots is None:
            return default
        long_snapshot, short_snapshot = snapshots
        long_observed_at_ms = long_snapshot.observed_at_ms
        short_observed_at_ms = short_snapshot.observed_at_ms
        long_bid = long_snapshot.bids[0].price
        long_ask = long_snapshot.asks[0].price
        short_bid = short_snapshot.bids[0].price
        short_ask = short_snapshot.asks[0].price
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
            hedge_levels = short_snapshot.bids
            unwind_levels = long_snapshot.bids
            hedge_venue = ctx.short_venue
            unwind_venue = ctx.long_venue
        else:
            maker_bid, maker_ask = short_bid, short_ask
            hedge_bid, hedge_ask = long_bid, long_ask
            hedge_levels = long_snapshot.asks
            unwind_levels = short_snapshot.asks
            hedge_venue = ctx.long_venue
            unwind_venue = ctx.short_venue
        hedge_vwap, hedge_filled, hedge_sweep_limit = (
            _l2_vwap_and_sweep_limit_for_base_quantity(hedge_levels, quantity)
        )
        unwind_vwap, unwind_filled, unwind_sweep_limit = (
            _l2_vwap_and_sweep_limit_for_base_quantity(unwind_levels, quantity)
        )
        quantity_tolerance = max(1e-12, quantity * 1e-9)
        hedge_complete = hedge_filled + quantity_tolerance >= quantity
        unwind_complete = unwind_filled + quantity_tolerance >= quantity
        hedge_submit_price = (
            hedge_sweep_limit
            if hedge_complete and hedge_sweep_limit > 0.0
            else hedge_bid if ctx.maker_leg == Side.BUY else hedge_ask
        )
        unwind_submit_price = (
            unwind_sweep_limit
            if unwind_complete and unwind_sweep_limit > 0.0
            else maker_bid if ctx.maker_leg == Side.BUY else maker_ask
        )
        base_market_evidence = {
            "source": "local_l2_multilevel_vwap",
            "long_bid": long_bid,
            "long_ask": long_ask,
            "short_bid": short_bid,
            "short_ask": short_ask,
            "long_observed_at_ms": long_observed_at_ms,
            "short_observed_at_ms": short_observed_at_ms,
            "max_age_ms": max_age_ms,
            "remaining_base_quantity": quantity,
            "hedge_l2_filled_base_quantity": hedge_filled,
            "unwind_l2_filled_base_quantity": unwind_filled,
            "hedge_l2_vwap": hedge_vwap,
            "unwind_l2_vwap": unwind_vwap,
            "hedge_sweep_limit": hedge_sweep_limit,
            "unwind_sweep_limit": unwind_sweep_limit,
            "hedge_l2_complete": hedge_complete,
            "unwind_l2_complete": unwind_complete,
        }
        if not hedge_complete and not unwind_complete:
            return {
                "action": "unwind_first_leg",
                "reason": "post_first_fill_incomplete_l2_depth_residual_repair",
                "hedge_price": hedge_submit_price,
                "unwind_price": unwind_submit_price,
                "market_evidence": base_market_evidence,
            }
        if not hedge_complete and unwind_complete:
            return {
                "action": "unwind_first_leg",
                "reason": "post_first_fill_incomplete_hedge_depth_unwind",
                "hedge_price": hedge_submit_price,
                "unwind_price": unwind_submit_price,
                "market_evidence": base_market_evidence,
            }
        if hedge_complete and not unwind_complete:
            return {
                "action": "complete_hedge",
                "reason": "post_first_fill_incomplete_unwind_depth_complete_hedge",
                "hedge_price": hedge_submit_price,
                "unwind_price": unwind_submit_price,
                "market_evidence": base_market_evidence,
            }

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
            hedge_execution_price=hedge_vwap,
            unwind_execution_price=unwind_vwap,
        )
        if market_decision is None:
            return {
                **default,
                "reason": "post_first_fill_fee_evidence_unavailable_complete_hedge",
                "hedge_price": hedge_submit_price,
                "market_evidence": {
                    **base_market_evidence,
                    "hedge_taker_fee_evidence": hedge_fee_bps is not None,
                    "unwind_taker_fee_evidence": unwind_fee_bps is not None,
                },
            }
        choice = market_decision.decision
        return {
            "action": choice.action,
            "reason": f"post_first_fill_{choice.reason}",
            "hedge_price": hedge_submit_price,
            "unwind_price": unwind_submit_price,
            "complete_hedge_loss_quote": choice.complete_hedge_loss_quote,
            "unwind_first_leg_loss_quote": choice.unwind_first_leg_loss_quote,
            "complete_hedge_price_loss_quote": choice.complete_hedge_price_loss_quote,
            "unwind_first_leg_price_loss_quote": choice.unwind_first_leg_price_loss_quote,
            "complete_hedge_fee_quote": choice.complete_hedge_fee_quote,
            "unwind_first_leg_fee_quote": choice.unwind_first_leg_fee_quote,
            "market_evidence": {
                **base_market_evidence,
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
        resolver = getattr(
            self.ctx,
            "_entry_effective_readiness_provider_name",
            self.ctx._entry_readiness_provider_name,
        )
        return resolver(*args, **kwargs)

    def _entry_readiness_provider_uses_quote_lease(self, *args: Any, **kwargs: Any):
        resolver = getattr(
            self.ctx,
            "_entry_effective_readiness_provider_uses_quote_lease",
            self.ctx._entry_readiness_provider_uses_quote_lease,
        )
        return resolver(*args, **kwargs)

    def _local_l2_effective_enabled(self, *args: Any, **kwargs: Any):
        resolver = getattr(
            self.ctx,
            "_entry_local_l2_effective_enabled",
            self.ctx._local_l2_effective_enabled,
        )
        return resolver(*args, **kwargs)

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
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="entry_leverage_unavailable",
                blocked_reasons=["entry_leverage_unavailable"],
                source="entry_leverage_inspection",
                decision="skip_before_first_leg",
                extra={
                    "configured_target_leverage": target_leverage,
                    "evidence_gap": True,
                },
            )
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
        failed_venues: list[str] = []
        reason_by_venue: dict[str, str] = {}
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
            failed_venues.append(venue.value)
            reason_by_venue[venue.value] = reason
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
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="entry_leverage_unavailable",
                blocked_reasons=["entry_leverage_unavailable"],
                source="entry_leverage_inspection",
                decision="skip_before_first_leg",
                extra={
                    "failed_venues": failed_venues,
                    "reason_by_venue": reason_by_venue,
                    "target_leverage": target_leverage,
                    "entry_notional_quote": notional_quote,
                },
            )
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
                    self._emit_entry_dispatch_viability_blocked(
                        candidate,
                        now_ms,
                        reason=reason,
                        blocked_reasons=[reason],
                        source=source,
                        decision="skip_before_first_leg",
                        extra=extra_payload,
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
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason="entry_admission_precheck_rejected",
                    blocked_reasons=["entry_admission_precheck_rejected"],
                    source="entry_admission_precheck",
                    decision="skip_before_first_leg",
                    extra={
                        "venue": venue.value,
                        "raw_error": error_text[:500],
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
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason="entry_admission_precheck_uncertain",
                    blocked_reasons=["entry_admission_precheck_uncertain"],
                    source="entry_admission_precheck",
                    decision="skip_before_first_leg",
                    extra={
                        "venue": venue.value,
                        "raw_error": str(exc)[:500],
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
        evidence = await self._entry_symbol_order_metadata(venue, symbol)
        quantity_step = self._safe_positive_float(evidence.get("quantity_step"))
        return (
            quantity_step if quantity_step > 0.0 else None,
            list(evidence.get("missing_fields") or []),
        )

    async def _entry_symbol_order_metadata(
        self,
        venue: Venue,
        symbol: str,
    ) -> dict[str, Any]:
        """Resolve one symbol's executable order units in canonical base qty.

        Static ``VenueSpec`` metadata is retained only for adapters without a
        transport (tests/compatibility).  Real transports use ``SymbolRule`` so
        the maker plan and the later hedge share the same per-symbol grid and
        minimums.  Contract venues are converted exactly once to base units.
        """

        adapter = self.get_venue_adapter(venue)
        passive_metadata = (
            getattr(adapter, "passive_metadata", None) if adapter else None
        )
        static_metadata: dict[str, Any] = {}
        if callable(passive_metadata):
            try:
                raw_metadata = passive_metadata(symbol) or {}
                if isinstance(raw_metadata, dict):
                    static_metadata = dict(raw_metadata)
            except Exception:
                static_metadata = {}

        evidence: dict[str, Any] = dict(static_metadata)
        evidence.update(
            {
                "quantity_step": self._safe_positive_float(
                    static_metadata.get("quantity_step")
                    or static_metadata.get("step_size")
                    or static_metadata.get("qtyStep")
                ),
                "min_quantity": self._safe_positive_float(
                    static_metadata.get("min_quantity")
                    or static_metadata.get("min_qty")
                    or static_metadata.get("minOrderQty")
                ),
                "min_notional": 0.0,
                "source": "passive_metadata",
                "rule_source": "",
                "venue_symbol": symbol,
                "missing_fields": [],
            }
        )
        static_min_notional_present = any(
            alias in static_metadata
            for alias in (
                "min_notional",
                "min_notional_quote",
                "minNotionalValue",
            )
        )
        if static_min_notional_present:
            try:
                evidence["min_notional"] = float(
                    static_metadata.get("min_notional")
                    or static_metadata.get("min_notional_quote")
                    or static_metadata.get("minNotionalValue")
                    or 0.0
                )
            except (TypeError, ValueError):
                evidence["min_notional"] = -1.0

        transport = getattr(adapter, "_transport", None) if adapter else None
        if transport is None and not static_metadata:
            evidence["missing_fields"] = ["metadata"]
            return evidence
        if transport is not None:
            venue_symbol = symbol
            venue_symbol_fn = getattr(transport, "_venue_symbol", None)
            if callable(venue_symbol_fn):
                try:
                    venue_symbol = venue_symbol_fn(symbol)
                except Exception:
                    venue_symbol = symbol
            evidence["venue_symbol"] = venue_symbol
            rules_cache = None
            try:
                from lightfee.venues.symbol_rules import get_symbol_rules_cache

                rules_cache = get_symbol_rules_cache()
                rule = await rules_cache.get(
                    transport,
                    venue,
                    venue_symbol,
                )
            except Exception:
                rule = None

            if rule is None:
                if str(getattr(transport, "mode", "") or "").lower() == "live":
                    evidence["missing_fields"] = ["symbol_rule"]
                    evidence["source"] = "symbol_rule_unavailable"
                    return evidence
            else:
                rule_step = self._safe_positive_float(
                    getattr(rule, "qty_step", 0.0)
                )
                rule_min_quantity = self._safe_positive_float(
                    getattr(rule, "min_qty", 0.0)
                )
                try:
                    rule_min_notional = float(
                        getattr(rule, "min_notional", 0.0) or 0.0
                    )
                except (TypeError, ValueError):
                    rule_min_notional = -1.0
                rule_source = str(getattr(rule, "rule_source", "") or "unknown")
                if (
                    rule_source == "spec_fallback"
                    and str(getattr(transport, "mode", "") or "").lower()
                    == "live"
                ):
                    invalidate = getattr(rules_cache, "invalidate", None)
                    if callable(invalidate):
                        invalidate(venue, venue_symbol)
                    evidence.update(
                        {
                            "source": "dynamic_symbol_rule_unavailable",
                            "rule_source": rule_source,
                            "missing_fields": ["dynamic_symbol_rule"],
                        }
                    )
                    return evidence
                evidence.update(
                    {
                        "source": f"symbol_rule:{rule_source}",
                        "rule_source": rule_source,
                        "min_notional": rule_min_notional,
                    }
                )

                if venue == Venue.OKX:
                    contract_multiplier = self._safe_positive_float(
                        getattr(rule, "ct_val", 0.0)
                    )
                    okx_step = await self._okx_entry_base_quantity_step(
                        venue,
                        symbol,
                    )
                    evidence.update(
                        {
                            "contract_step": rule_step,
                            "contract_multiplier": contract_multiplier,
                            "ct_val": contract_multiplier,
                            "quantity_units": "okx_contracts_to_base",
                            "quantity_step": float(okx_step or 0.0),
                            "min_quantity": (
                                rule_min_quantity * contract_multiplier
                                if contract_multiplier > 0.0
                                else 0.0
                            ),
                        }
                    )
                    if okx_step is None:
                        evidence["missing_fields"].append("okx_contract_step")
                    if contract_multiplier <= 0.0:
                        evidence["missing_fields"].append(
                            "okx_contract_multiplier"
                        )
                elif venue == Venue.GATE:
                    contract_multiplier = self._safe_positive_float(
                        getattr(rule, "contract_multiplier", 0.0)
                        or getattr(rule, "ct_val", 0.0)
                    )
                    evidence.update(
                        {
                            "contract_step": rule_step,
                            "contract_multiplier": contract_multiplier,
                            "ct_val": contract_multiplier,
                            "quantity_units": "gate_contracts_to_base",
                            "quantity_step": rule_step * contract_multiplier,
                            "min_quantity": (
                                rule_min_quantity * contract_multiplier
                            ),
                        }
                    )
                    if contract_multiplier <= 0.0:
                        evidence["missing_fields"].append(
                            "contract_multiplier"
                        )
                else:
                    evidence.update(
                        {
                            "quantity_step": rule_step,
                            "min_quantity": rule_min_quantity,
                            "quantity_units": "base",
                        }
                    )
        elif venue == Venue.OKX:
            okx_step = await self._okx_entry_base_quantity_step(venue, symbol)
            if okx_step is None:
                evidence["missing_fields"].append("okx_contract_step")
            elif okx_step > 0.0:
                evidence["quantity_step"] = okx_step
                evidence["source"] = "okx_contract_step"

        if transport is None and not static_min_notional_present:
            evidence["missing_fields"].append("min_notional")
        if self._safe_positive_float(evidence.get("quantity_step")) <= 0.0:
            evidence["missing_fields"].append("quantity_step")
        if self._safe_positive_float(evidence.get("min_quantity")) <= 0.0:
            evidence["missing_fields"].append("min_quantity")
        try:
            min_notional = float(evidence.get("min_notional", -1.0))
        except (TypeError, ValueError):
            min_notional = -1.0
        if not math.isfinite(min_notional) or min_notional < 0.0:
            evidence["missing_fields"].append("min_notional")
        evidence["missing_fields"] = list(
            dict.fromkeys(evidence["missing_fields"])
        )
        return evidence

    async def _entry_venue_quantity_metadata_evidence(
        self,
        venue: Venue,
        symbol: str,
        quantity_step: float | None,
        missing_fields: list[str] | None = None,
    ) -> dict:
        evidence = await self._entry_symbol_order_metadata(venue, symbol)
        if quantity_step and quantity_step > 0.0:
            evidence["quantity_step"] = float(quantity_step)
        evidence["missing_fields"] = list(
            dict.fromkeys(
                [
                    *list(evidence.get("missing_fields") or []),
                    *list(missing_fields or []),
                ]
            )
        )
        return evidence

    def _freeze_symbol_rule_at_entry(
        self,
        *,
        venue: Venue,
        symbol: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Seal executable per-symbol rules in canonical base units."""

        quantity_step = self._safe_positive_float(metadata.get("quantity_step"))
        min_quantity = self._safe_positive_float(metadata.get("min_quantity"))
        try:
            min_notional = float(metadata.get("min_notional", -1.0))
        except (TypeError, ValueError):
            min_notional = -1.0
        missing_fields = list(metadata.get("missing_fields") or [])
        if quantity_step <= 0.0:
            missing_fields.append("quantity_step")
        if min_quantity <= 0.0:
            missing_fields.append("min_quantity")
        if not math.isfinite(min_notional) or min_notional < 0.0:
            missing_fields.append("min_notional")
        missing_fields = list(dict.fromkeys(missing_fields))
        return {
            "venue": venue.value,
            "symbol": symbol,
            "venue_symbol": str(metadata.get("venue_symbol") or symbol),
            "quantity_units": "base",
            "quantity_step_base": quantity_step,
            "min_quantity_base": min_quantity,
            "min_notional_quote": min_notional,
            "source": str(metadata.get("source") or ""),
            "rule_source": str(metadata.get("rule_source") or ""),
            "contract_step": metadata.get("contract_step"),
            "contract_multiplier": metadata.get("contract_multiplier"),
            "missing_fields": missing_fields,
            "evidence_complete": not missing_fields,
        }

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

    @staticmethod
    def _entry_pair_minimum_reason(
        *,
        quantity: float,
        long_price: float,
        short_price: float,
        long_metadata: dict,
        short_metadata: dict,
        strategy_min_notional: float,
    ) -> tuple[str, dict]:
        failures: list[dict[str, float | str]] = []
        for leg, price, metadata in (
            ("long", long_price, long_metadata),
            ("short", short_price, short_metadata),
        ):
            min_quantity = float(metadata.get("min_quantity", 0.0) or 0.0)
            min_notional = max(
                float(metadata.get("min_notional", 0.0) or 0.0),
                max(float(strategy_min_notional or 0.0), 0.0),
            )
            notional = max(float(quantity or 0.0), 0.0) * max(
                float(price or 0.0), 0.0
            )
            if quantity + 1e-12 < min_quantity or notional + 1e-9 < min_notional:
                failures.append(
                    {
                        "leg": leg,
                        "quantity": quantity,
                        "price": price,
                        "notional_quote": notional,
                        "min_quantity": min_quantity,
                        "min_notional_quote": min_notional,
                    }
                )
        if not failures:
            return "", {}
        return "entry_pair_minimum_not_met", {"pair_minimum_failures": failures}

    @staticmethod
    def _entry_normalized_price_consistency_reason(
        *, long_price: float, short_price: float
    ) -> str:
        """Fail closed when the two canonical contract prices cannot be peers."""
        try:
            long_value = float(long_price)
            short_value = float(short_price)
        except (TypeError, ValueError, OverflowError):
            return "entry_normalized_price_invalid"
        if not all(
            math.isfinite(value) and value > 0.0
            for value in (long_value, short_value)
        ):
            return "entry_normalized_price_invalid"
        if max(long_value, short_value) / min(long_value, short_value) > 2.0:
            return "cross_venue_price_normalization_mismatch"
        return ""

    @staticmethod
    def _apply_final_entry_economics(
        candidate,
        *,
        quantity: float,
        long_price: float,
        short_price: float,
    ) -> str:
        """Synchronise all quantity-dependent economics to the final order plan."""
        try:
            final_quantity = float(quantity)
            final_long_price = float(long_price)
            final_short_price = float(short_price)
            expected_edge_bps = float(
                getattr(candidate, "expected_edge_bps", 0.0) or 0.0
            )
            worst_case_edge_bps = float(
                getattr(candidate, "worst_case_edge_bps", 0.0) or 0.0
            )
        except (TypeError, ValueError, OverflowError):
            return "final_entry_economics_invalid"
        if not all(
            math.isfinite(value)
            for value in (
                final_quantity,
                final_long_price,
                final_short_price,
                expected_edge_bps,
                worst_case_edge_bps,
            )
        ) or min(final_quantity, final_long_price, final_short_price) <= 0.0:
            return "final_entry_economics_invalid"
        average_notional = (
            final_quantity * (final_long_price + final_short_price) / 2.0
        )
        candidate.entry_target_quantity = final_quantity
        candidate.entry_notional_quote = average_notional
        candidate.entry_max_leg_notional_quote = final_quantity * max(
            final_long_price, final_short_price
        )
        candidate.expected_profit_quote = (
            average_notional * expected_edge_bps / 10_000.0
        )
        candidate.worst_case_profit_quote = (
            average_notional * worst_case_edge_bps / 10_000.0
        )
        return ""

    @staticmethod
    def _final_entry_economics_binding_reason(
        candidate,
        *,
        quantity: float,
        long_price: float,
        short_price: float,
    ) -> str:
        """Verify that serialized final economics derive from the order plan."""
        raw_values = (
            quantity,
            long_price,
            short_price,
            getattr(candidate, "entry_target_quantity", None),
            getattr(candidate, "entry_notional_quote", None),
            getattr(candidate, "entry_max_leg_notional_quote", None),
            getattr(candidate, "expected_edge_bps", None),
            getattr(candidate, "worst_case_edge_bps", None),
            getattr(candidate, "expected_profit_quote", None),
            getattr(candidate, "worst_case_profit_quote", None),
        )
        if any(isinstance(value, bool) for value in raw_values):
            return "final_entry_economics_binding_invalid"
        try:
            (
                final_quantity,
                final_long_price,
                final_short_price,
                candidate_quantity,
                candidate_notional,
                candidate_max_leg_notional,
                expected_edge_bps,
                worst_case_edge_bps,
                expected_profit,
                worst_case_profit,
            ) = (float(value) for value in raw_values)
        except (TypeError, ValueError, OverflowError):
            return "final_entry_economics_binding_invalid"
        values = (
            final_quantity,
            final_long_price,
            final_short_price,
            candidate_quantity,
            candidate_notional,
            candidate_max_leg_notional,
            expected_edge_bps,
            worst_case_edge_bps,
            expected_profit,
            worst_case_profit,
        )
        if not all(math.isfinite(value) for value in values):
            return "final_entry_economics_binding_invalid"
        if min(final_quantity, final_long_price, final_short_price) <= 0.0:
            return "final_entry_economics_binding_invalid"

        derived_notional = (
            final_quantity * (final_long_price + final_short_price) / 2.0
        )
        derived_max_leg_notional = final_quantity * max(
            final_long_price, final_short_price
        )
        derived_expected_profit = derived_notional * expected_edge_bps / 10_000.0
        derived_worst_case_profit = (
            derived_notional * worst_case_edge_bps / 10_000.0
        )
        expected_values = (
            (candidate_quantity, final_quantity),
            (candidate_notional, derived_notional),
            (candidate_max_leg_notional, derived_max_leg_notional),
            (expected_profit, derived_expected_profit),
            (worst_case_profit, derived_worst_case_profit),
        )
        if any(
            not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
            for actual, expected in expected_values
        ):
            return "final_entry_economics_binding_mismatch"
        return ""

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
            candidate.entry_max_leg_notional_quote = quantity * max(
                long_entry_price, short_entry_price
            )
        return quantity, margin_constrained

    def _quote_lease_current_candidate_revision_id(
        self,
        candidate,
    ) -> str:
        revision_id = str(
            getattr(candidate, "candidate_revision_id", "") or ""
        ).strip()
        if revision_id:
            return revision_id
        binder = getattr(self.ctx, "_bind_entry_candidate_revision_id", None)
        if callable(binder):
            with suppress(Exception):
                return str(binder(candidate) or "").strip()
        return ""

    def _entry_quote_lease_max_skew_ms(self) -> int:
        strategy = self.ctx.config.strategy
        value = getattr(strategy, "entry_quote_lease_max_skew_ms", None)
        if value is None:
            value = getattr(strategy, "entry_final_gate_max_skew_ms", 0)
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError, OverflowError):
            return 0
        return max(parsed, 0)

    def _entry_final_gate_max_skew_ms(self) -> int:
        try:
            parsed = int(
                getattr(self.ctx.config.strategy, "entry_final_gate_max_skew_ms", 0)
                or 0
            )
        except (TypeError, ValueError, OverflowError):
            return 0
        return max(parsed, 0)

    @staticmethod
    def _quote_lease_candidate_revision_mismatch(
        candidate_revision_id: str,
        lease_revision_id: str,
    ) -> bool:
        return bool(
            candidate_revision_id
            and lease_revision_id
            and lease_revision_id != candidate_revision_id
        )

    def _entry_quote_lease_execution_check(
        self,
        candidate,
        now_ms: int,
        *,
        enforce_side_capacity: bool = True,
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
            "candidate_revision_id": str(
                getattr(candidate, "candidate_revision_id", "") or ""
            ),
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
                "lease_candidate_revision_id": str(
                    getattr(lease, "candidate_revision_id", "") or ""
                ),
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
                "long_bid_size": float(
                    getattr(lease, "long_bid_size", 0.0) or 0.0
                ),
                "long_ask_size": float(
                    getattr(lease, "long_ask_size", 0.0) or 0.0
                ),
                "short_bid_size": float(
                    getattr(lease, "short_bid_size", 0.0) or 0.0
                ),
                "short_ask_size": float(
                    getattr(lease, "short_ask_size", 0.0) or 0.0
                ),
            }
        )
        lease_revision_id = str(evidence["lease_candidate_revision_id"] or "").strip()
        candidate_revision_id = self._quote_lease_current_candidate_revision_id(
            candidate
        )
        evidence["candidate_revision_id"] = candidate_revision_id
        evidence["lease_candidate_revision_id"] = lease_revision_id
        expected_lease_provider = provider_name
        if provider_name == "ws_bbo_l2_on_demand":
            expected_lease_provider = "ws_bbo_quote_lease"
        if evidence["lease_provider"] != expected_lease_provider:
            return blocked("quote_lease_provider_mismatch", lease)
        if str(getattr(lease, "symbol", "")) != evidence["symbol"]:
            return blocked("quote_lease_symbol_mismatch", lease)
        if str(getattr(lease, "long_venue", "")) != evidence["long_venue"]:
            return blocked("quote_lease_long_venue_mismatch", lease)
        if str(getattr(lease, "short_venue", "")) != evidence["short_venue"]:
            return blocked("quote_lease_short_venue_mismatch", lease)
        if self._quote_lease_candidate_revision_mismatch(
            candidate_revision_id,
            lease_revision_id,
        ):
            return blocked("quote_lease_candidate_revision_mismatch", lease)
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

        max_skew_ms = self._entry_quote_lease_max_skew_ms()
        quote_skew_ms = abs(
            int(evidence["long_observed_at_ms"] or 0)
            - int(evidence["short_observed_at_ms"] or 0)
        )
        evidence["quote_observation_skew_ms"] = quote_skew_ms
        evidence["quote_observation_max_skew_ms"] = max_skew_ms
        if quote_skew_ms > max_skew_ms and self._local_l2_effective_enabled():
            evidence["quote_lease_skew_deferred"] = True
            evidence["quote_lease_skew_deferred_to"] = "local_l2_final_gate"
        elif quote_skew_ms > max_skew_ms:
            return blocked("quote_lease_skew_exceeded", lease)

        if (
            evidence["long_bid"] <= 0.0
            or evidence["long_ask"] <= evidence["long_bid"]
            or evidence["short_bid"] <= 0.0
            or evidence["short_ask"] <= evidence["short_bid"]
        ):
            return blocked("invalid_quote_lease", lease)
        if enforce_side_capacity:
            capacity_reason, capacity_evidence = _quote_lease_side_capacity_checks(
                candidate,
                lease,
            )
            evidence.update(capacity_evidence)
            if capacity_reason:
                return blocked(capacity_reason, lease)
        else:
            evidence["quote_lease_capacity_deferred"] = True
            evidence["quote_lease_capacity_deferred_to"] = "final_quote_lease"
        return "", lease, evidence

    def _final_quote_lease_reason(
        self,
        candidate,
        lease: QuoteLease | None,
        now_ms: int,
    ) -> str:
        if lease is None:
            return "missing_final_quote_lease"
        if lease.pair_id != self._candidate_pair_id(candidate):
            return "final_quote_lease_pair_mismatch"
        candidate_revision_id = self._quote_lease_current_candidate_revision_id(
            candidate
        )
        lease_revision_id = str(
            getattr(lease, "candidate_revision_id", "") or ""
        ).strip()
        if self._quote_lease_candidate_revision_mismatch(
            candidate_revision_id,
            lease_revision_id,
        ):
            return "final_quote_lease_candidate_revision_mismatch"
        if lease.expires_at_ms <= 0 or now_ms >= lease.expires_at_ms:
            return "expired_final_quote_lease"
        max_age_ms = self._entry_quote_lease_max_age_ms()
        for observed_at_ms in (
            lease.long_observed_at_ms,
            lease.short_observed_at_ms,
        ):
            if (
                observed_at_ms <= 0
                or observed_at_ms > now_ms
                or max_age_ms <= 0
                or now_ms - observed_at_ms > max_age_ms
            ):
                return "stale_final_quote_lease"
        max_skew_ms = self._entry_final_gate_max_skew_ms()
        if abs(lease.long_observed_at_ms - lease.short_observed_at_ms) > max_skew_ms:
            return "final_quote_lease_skew_exceeded"
        if (
            lease.long_bid <= 0.0
            or lease.long_ask <= lease.long_bid
            or lease.short_bid <= 0.0
            or lease.short_ask <= lease.short_bid
        ):
            return "invalid_final_quote_lease"
        capacity_reason, _capacity_evidence = _quote_lease_side_capacity_checks(
            candidate,
            lease,
        )
        if capacity_reason:
            return "final_quote_lease_insufficient_bbo_capacity"
        return ""

    def _refresh_entry_quote_lease_for_execution(
        self,
        candidate,
        now_ms: int,
        quote_lease_reason: str,
        quote_lease: object | None,
        quote_lease_evidence: dict,
        *,
        enforce_side_capacity: bool = True,
    ) -> tuple[str, object | None, dict]:
        if quote_lease_reason not in {"expired_quote_lease", "stale_quote_lease"}:
            return quote_lease_reason, quote_lease, quote_lease_evidence
        if self._entry_readiness_provider_name() != "ws_bbo_l2_on_demand":
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
            enforce_side_capacity=enforce_side_capacity,
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
        max_skew_ms = self._entry_final_gate_max_skew_ms()
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
        raw_reason = str(reason or "")
        normalized_reason = raw_reason or "entry_dispatch_reason_invariant_missing"
        pair_id = self._candidate_pair_id(candidate)
        if not raw_reason:
            self.ctx.journal.append(
                "entry.dispatch_reason_invariant_violation",
                {
                    "reason": normalized_reason,
                    "source": source,
                    "decision": decision,
                    "candidate_pair_id": pair_id,
                    "pair_id": pair_id,
                    "ts_ms": now_ms,
                },
            )
        setattr(self.ctx, "_last_entry_dispatch_block_reason", normalized_reason)
        entry_id = str(
            getattr(candidate, "entry_id", "")
            or getattr(candidate, "internal_entry_id", "")
            or getattr(candidate, "pending_owner_id", "")
            or getattr(candidate, "position_id", "")
            or ""
        )
        normalized_blocked_reasons = [item for item in blocked_reasons if item]
        if not normalized_blocked_reasons:
            normalized_blocked_reasons = [normalized_reason]
        payload = {
            "entry_id": entry_id,
            "symbol": getattr(candidate, "symbol", ""),
            "long_venue": getattr(candidate, "long_venue", ""),
            "short_venue": getattr(candidate, "short_venue", ""),
            "candidate_pair_id": pair_id,
            "pair_id": pair_id,
            "reason": normalized_reason,
            "blocked_reasons": normalized_blocked_reasons,
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
        defer_quote_lease_side_capacity = self._local_l2_effective_enabled()
        quote_lease_reason, quote_lease, quote_lease_evidence = (
            self._entry_quote_lease_execution_check(
                candidate,
                now_ms,
                enforce_side_capacity=not defer_quote_lease_side_capacity,
            )
        )
        if quote_lease_reason:
            quote_lease_reason, quote_lease, quote_lease_evidence = (
                self._refresh_entry_quote_lease_for_execution(
                    candidate,
                    now_ms,
                    quote_lease_reason,
                    quote_lease,
                    quote_lease_evidence,
                    enforce_side_capacity=not defer_quote_lease_side_capacity,
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
        final_l2_quote_result = self._local_l2_final_quote_result(candidate, now_ms)
        if final_l2_quote_result.lease is not None:
            quote_lease = final_l2_quote_result.lease
        elif final_l2_quote_result.reason:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=final_l2_quote_result.reason,
                blocked_reasons=[final_l2_quote_result.reason],
                source="final_quote_lease",
                decision="skip_dispatch",
                extra=final_l2_quote_result.evidence,
            )
            self.ctx.journal.append(
                "runtime.entry_blocked_final_quote_lease",
                {
                    **final_l2_quote_result.evidence,
                    "reason": final_l2_quote_result.reason,
                    "ts_ms": now_ms,
                },
            )
            return None
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

    def _local_l2_final_quote_result(
        self,
        candidate,
        now_ms: int,
        *,
        target_quantity: float | None = None,
    ) -> FinalQuoteLeaseResult:
        """Build final executable BBO/L2 evidence or the exact block reason."""
        pair_id = self._candidate_pair_id(candidate)
        evidence: dict[str, Any] = {
            "pair_id": pair_id,
            "candidate_pair_id": pair_id,
            "symbol": str(getattr(candidate, "symbol", "") or "").upper(),
            "long_venue": str(getattr(candidate, "long_venue", "") or "").lower(),
            "short_venue": str(getattr(candidate, "short_venue", "") or "").lower(),
            "source": "local_l2_final_quote",
            "domain": "final_quote_lease",
            "candidate_revision_id": str(
                getattr(candidate, "candidate_revision_id", "") or ""
            ),
        }

        def fail(reason: str, **extra: Any) -> FinalQuoteLeaseResult:
            payload = dict(evidence)
            payload.update(extra)
            payload["reason"] = reason
            try:
                payload["blocker_family"] = self._quote_lease_blocker_family(reason)
            except AttributeError:
                payload["blocker_family"] = "final_quote"
            return FinalQuoteLeaseResult(reason=reason, evidence=payload)

        if (
            self.ctx.config.runtime.mode != "live"
            or not self._local_l2_effective_enabled()
        ):
            return FinalQuoteLeaseResult(evidence=evidence)

        symbol = evidence["symbol"]
        long_venue = evidence["long_venue"]
        short_venue = evidence["short_venue"]
        if not symbol or not long_venue or not short_venue:
            return fail("final_quote_invalid_identity")
        max_age_ms = max(
            int(
                getattr(
                    self.ctx.config.strategy,
                    "max_liquidity_snapshot_age_ms",
                    0,
                )
                or 0
            ),
            0,
        )
        evidence["max_age_ms"] = max_age_ms
        if max_age_ms <= 0:
            return fail("final_quote_invalid_max_age")
        long_book = self.ctx.local_l2_runtime.get_book(long_venue, symbol)
        short_book = self.ctx.local_l2_runtime.get_book(short_venue, symbol)
        evidence.update(
            {
                "long_book_present": long_book is not None,
                "short_book_present": short_book is not None,
            }
        )
        if long_book is None or short_book is None:
            missing_legs = []
            if long_book is None:
                missing_legs.append("long")
            if short_book is None:
                missing_legs.append("short")
            return fail("final_quote_missing_book", missing_legs=missing_legs)

        def _book_evidence(leg: str, book: Any) -> None:
            observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
            status = getattr(
                getattr(book, "status", ""),
                "value",
                getattr(book, "status", ""),
            )
            evidence.update(
                {
                    f"{leg}_book_status": str(status),
                    f"{leg}_observed_at_ms": observed_at_ms,
                    f"{leg}_age_ms": (
                        max(now_ms - observed_at_ms, 0)
                        if observed_at_ms > 0
                        else None
                    ),
                    f"{leg}_sequence": int(getattr(book, "sequence", 0) or 0),
                    f"{leg}_last_update_id": int(
                        getattr(book, "last_update_id", 0) or 0
                    ),
                }
            )

        _book_evidence("long", long_book)
        _book_evidence("short", short_book)
        not_hot_legs = [
            leg
            for leg in ("long", "short")
            if evidence[f"{leg}_book_status"] != "hot"
        ]
        if not_hot_legs:
            return fail("final_quote_book_not_hot", not_ready_legs=not_hot_legs)
        future_legs = [
            leg
            for leg in ("long", "short")
            if int(evidence[f"{leg}_observed_at_ms"] or 0) > now_ms
        ]
        if future_legs:
            return fail(
                "final_quote_book_timestamp_after_now",
                clock_skew=True,
                future_legs=future_legs,
            )
        stale_legs = [
            leg
            for leg in ("long", "short")
            if (
                int(evidence[f"{leg}_observed_at_ms"] or 0) <= 0
                or evidence[f"{leg}_age_ms"] is None
                or int(evidence[f"{leg}_age_ms"] or 0) > max_age_ms
            )
        ]
        if stale_legs:
            return fail("final_quote_book_stale", stale_legs=stale_legs)

        def raw_top(
            leg: str,
            book: Any,
        ) -> tuple[float, float, float, float] | FinalQuoteLeaseResult | None:
            bids = getattr(book, "bids", None)
            asks = getattr(book, "asks", None)
            if (
                not isinstance(bids, list)
                or not isinstance(asks, list)
                or not bids
                or not asks
            ):
                return None
            bid = bids[0]
            ask = asks[0]
            try:
                bid_price = float(getattr(bid, "price", 0.0) or 0.0)
                ask_price = float(getattr(ask, "price", 0.0) or 0.0)
                bid_quantity = float(getattr(bid, "quantity", 0.0) or 0.0)
                ask_quantity = float(getattr(ask, "quantity", 0.0) or 0.0)
            except (TypeError, ValueError, OverflowError):
                return fail("final_quote_invalid_price", invalid_legs=[leg])
            return bid_price, ask_price, bid_quantity, ask_quantity

        long_top = raw_top("long", long_book)
        short_top = raw_top("short", short_book)
        if isinstance(long_top, FinalQuoteLeaseResult):
            return long_top
        if isinstance(short_top, FinalQuoteLeaseResult):
            return short_top
        if long_top is None or short_top is None:
            missing_depth_legs = []
            if long_top is None:
                missing_depth_legs.append("long")
            if short_top is None:
                missing_depth_legs.append("short")
            return fail(
                "final_quote_invalid_depth",
                missing_depth_legs=missing_depth_legs,
            )
        (
            long_bid_price,
            long_ask_price,
            long_bid_quantity,
            long_ask_quantity,
        ) = long_top
        (
            short_bid_price,
            short_ask_price,
            short_bid_quantity,
            short_ask_quantity,
        ) = short_top
        if not all(
            math.isfinite(value) and value > 0.0
            for value in (
                long_bid_price,
                long_ask_price,
                short_bid_price,
                short_ask_price,
            )
        ):
            return fail("final_quote_invalid_price")
        if long_bid_price >= long_ask_price or short_bid_price >= short_ask_price:
            return fail("final_quote_crossed_book")
        if not all(
            math.isfinite(value) and value > 0.0
            for value in (
                long_bid_quantity,
                long_ask_quantity,
                short_bid_quantity,
                short_ask_quantity,
            )
        ):
            return fail("final_quote_invalid_depth")

        long_snapshot = execution_liquidity_from_local_l2(
            long_book,
            max_age_ms=max_age_ms,
            now_ms=now_ms,
            require_ready=True,
        )
        short_snapshot = execution_liquidity_from_local_l2(
            short_book,
            max_age_ms=max_age_ms,
            now_ms=now_ms,
            require_ready=True,
        )
        evidence.update(
            {
                "long_liquidity_source": long_snapshot.source,
                "short_liquidity_source": short_snapshot.source,
                "long_liquidity_reason": long_snapshot.fallback_reason,
                "short_liquidity_reason": short_snapshot.fallback_reason,
            }
        )
        if not long_snapshot.book_ready or not short_snapshot.book_ready:
            reasons = {
                leg: snapshot.fallback_reason
                for leg, snapshot in (
                    ("long", long_snapshot),
                    ("short", short_snapshot),
                )
                if not snapshot.book_ready
            }
            if any(
                "book_invalid_for_execution_liquidity" in reason
                for reason in reasons.values()
            ):
                return fail("final_quote_invalid_book", invalid_legs=list(reasons))
            return fail("final_quote_book_not_hot", not_ready_legs=list(reasons))

        long_observed_at_ms = long_snapshot.observed_at_ms
        short_observed_at_ms = short_snapshot.observed_at_ms
        if (
            not long_snapshot.bids
            or not long_snapshot.asks
            or not short_snapshot.bids
            or not short_snapshot.asks
        ):
            return fail("final_quote_invalid_depth")
        long_bid = float(long_snapshot.bids[0].price)
        long_ask = float(long_snapshot.asks[0].price)
        short_bid = float(short_snapshot.bids[0].price)
        short_ask = float(short_snapshot.asks[0].price)
        evidence.update(
            {
                "long_bid": long_bid,
                "long_ask": long_ask,
                "short_bid": short_bid,
                "short_ask": short_ask,
            }
        )
        if not all(
            math.isfinite(value) and value > 0.0
            for value in (long_bid, long_ask, short_bid, short_ask)
        ):
            return fail("final_quote_invalid_price")
        if long_bid >= long_ask or short_bid >= short_ask:
            return fail("final_quote_crossed_book")
        final_gate_max_skew_ms = self._entry_final_gate_max_skew_ms()
        final_l2_skew_ms = abs(long_observed_at_ms - short_observed_at_ms)
        evidence["final_l2_observation_skew_ms"] = final_l2_skew_ms
        evidence["final_l2_observation_max_skew_ms"] = final_gate_max_skew_ms
        if final_l2_skew_ms > final_gate_max_skew_ms:
            return fail("execution_skew")

        def top_level_quantity(level: object) -> float:
            raw_quantity = getattr(level, "quantity", None)
            if raw_quantity is None:
                raw_quantity = getattr(level, "size", 0.0)
            try:
                quantity = float(raw_quantity or 0.0)
            except (TypeError, ValueError, OverflowError):
                return 0.0
            if not math.isfinite(quantity):
                return 0.0
            return max(quantity, 0.0)

        try:
            requested_quantity = float(
                target_quantity
                if target_quantity is not None
                else getattr(candidate, "entry_target_quantity", 0.0)
            ) or 0.0
        except (TypeError, ValueError, OverflowError):
            requested_quantity = 0.0
        requested_quantity = max(requested_quantity, 0.0)
        exact_quantity_required = target_quantity is not None
        long_l2_capacity = _l2_base_capacity(long_snapshot.asks)
        short_l2_capacity = _l2_base_capacity(short_snapshot.bids)
        evidence["requested_base_quantity"] = requested_quantity
        evidence["exact_quantity_required"] = exact_quantity_required
        if requested_quantity <= 0.0:
            if exact_quantity_required:
                return fail("final_quote_invalid_quantity")
            evidence.update(
                {
                    "long_buy_vwap": 0.0,
                    "short_sell_vwap": 0.0,
                    "long_buy_sweep_limit": 0.0,
                    "short_sell_sweep_limit": 0.0,
                    "long_l2_filled_base_quantity": 0.0,
                    "short_l2_filled_base_quantity": 0.0,
                    "long_l2_capacity_quantity": long_l2_capacity,
                    "short_l2_capacity_quantity": short_l2_capacity,
                    "l2_vwap_quantity": 0.0,
                    "l2_vwap_complete": False,
                    "route_has_complete_depth": False,
                    "early_eligibility_without_quantity": True,
                }
            )
            lease = QuoteLease(
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
                long_bid_size=top_level_quantity(long_snapshot.bids[0]),
                long_ask_size=top_level_quantity(long_snapshot.asks[0]),
                short_bid_size=top_level_quantity(short_snapshot.bids[0]),
                short_ask_size=top_level_quantity(short_snapshot.asks[0]),
                provider="local_l2_final_vwap",
                candidate_revision_id=str(
                    getattr(candidate, "candidate_revision_id", "") or ""
                ),
                long_buy_vwap=0.0,
                short_sell_vwap=0.0,
                long_buy_sweep_limit=0.0,
                short_sell_sweep_limit=0.0,
                long_l2_capacity_quantity=long_l2_capacity,
                short_l2_capacity_quantity=short_l2_capacity,
                l2_vwap_quantity=0.0,
                l2_vwap_complete=False,
            )
            evidence["lease_provider"] = lease.provider
            return FinalQuoteLeaseResult(lease=lease, evidence=evidence)
        (
            long_buy_vwap,
            long_vwap_filled,
            long_buy_sweep_limit,
        ) = _l2_vwap_and_sweep_limit_for_base_quantity(
            long_snapshot.asks, requested_quantity
        )
        (
            short_sell_vwap,
            short_vwap_filled,
            short_sell_sweep_limit,
        ) = _l2_vwap_and_sweep_limit_for_base_quantity(
            short_snapshot.bids, requested_quantity
        )
        l2_vwap_complete = (
            requested_quantity > 0.0
            and long_vwap_filled + 1e-12 >= requested_quantity
            and short_vwap_filled + 1e-12 >= requested_quantity
        )
        planned_maker_leg = str(
            getattr(candidate, "entry_maker_leg", "") or ""
        ).lower()
        long_maker_route_complete = (
            planned_maker_leg in {"long", "short"}
            and long_bid_quantity + 1e-12 >= requested_quantity
            and short_l2_capacity + 1e-12 >= requested_quantity
            and short_sell_vwap > 0.0
        )
        short_maker_route_complete = (
            planned_maker_leg in {"long", "short"}
            and short_ask_quantity + 1e-12 >= requested_quantity
            and long_l2_capacity + 1e-12 >= requested_quantity
            and long_buy_vwap > 0.0
        )
        route_has_complete_depth = (
            l2_vwap_complete
            or long_maker_route_complete
            or short_maker_route_complete
        )
        evidence.update(
            {
                "long_buy_vwap": long_buy_vwap,
                "short_sell_vwap": short_sell_vwap,
                "long_buy_sweep_limit": long_buy_sweep_limit,
                "short_sell_sweep_limit": short_sell_sweep_limit,
                "long_l2_filled_base_quantity": long_vwap_filled,
                "short_l2_filled_base_quantity": short_vwap_filled,
                "long_l2_capacity_quantity": long_l2_capacity,
                "short_l2_capacity_quantity": short_l2_capacity,
                "l2_vwap_quantity": requested_quantity,
                "l2_vwap_complete": l2_vwap_complete,
                "planned_entry_maker_leg": planned_maker_leg,
                "long_maker_route_complete": long_maker_route_complete,
                "short_maker_route_complete": short_maker_route_complete,
                "route_has_complete_depth": route_has_complete_depth,
            }
        )
        if (
            long_buy_vwap <= 0.0
            or short_sell_vwap <= 0.0
            or long_buy_sweep_limit <= 0.0
            or short_sell_sweep_limit <= 0.0
            or not route_has_complete_depth
        ):
            return fail("final_quote_insufficient_l2_depth")
        lease = QuoteLease(
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
            long_bid_size=top_level_quantity(long_snapshot.bids[0]),
            long_ask_size=top_level_quantity(long_snapshot.asks[0]),
            short_bid_size=top_level_quantity(short_snapshot.bids[0]),
            short_ask_size=top_level_quantity(short_snapshot.asks[0]),
            provider="local_l2_final_vwap",
            candidate_revision_id=str(
                getattr(candidate, "candidate_revision_id", "") or ""
            ),
            long_buy_vwap=long_buy_vwap,
            short_sell_vwap=short_sell_vwap,
            long_buy_sweep_limit=long_buy_sweep_limit,
            short_sell_sweep_limit=short_sell_sweep_limit,
            long_l2_capacity_quantity=long_l2_capacity,
            short_l2_capacity_quantity=short_l2_capacity,
            l2_vwap_quantity=requested_quantity,
            l2_vwap_complete=l2_vwap_complete,
        )
        evidence["lease_provider"] = lease.provider
        return FinalQuoteLeaseResult(lease=lease, evidence=evidence)

    def _local_l2_final_quote_lease(
        self,
        candidate,
        now_ms: int,
        *,
        target_quantity: float | None = None,
    ) -> QuoteLease | None:
        return self._local_l2_final_quote_result(
            candidate,
            now_ms,
            target_quantity=target_quantity,
        ).lease

    def _capture_entry_execution_benchmark_receipt(
        self,
        ctx: EntryContext,
    ) -> dict[str, object] | None:
        """Capture the raw L2 observation that may later qualify entry fills.

        This capture sits immediately before the executor is scheduled, after
        every live admission/revalidation gate.  It deliberately does not
        alter the selected route or order prices.  The executor will discard
        it unless both IOC fills are exact, timely, and side/venue-consistent.
        """
        if (
            self.ctx.config.runtime.mode != "live"
            or ctx.entry_type != EntryType.STANDARD_DUAL_TAKER
            or not self._local_l2_effective_enabled()
        ):
            return None
        requested_quantity = max(float(ctx.long_quantity or 0.0), 0.0)
        if requested_quantity <= 0.0 or abs(ctx.short_quantity - requested_quantity) > 1e-10:
            return None
        captured_at_ms = int(time.time() * 1000)
        configured_max_age_ms = max(
            self.ctx.config.strategy.max_liquidity_snapshot_age_ms,
            0,
        )
        benchmark_max_age_ms = min(
            configured_max_age_ms,
            EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS,
        )
        snapshots = self._local_l2_execution_snapshots(
            symbol=ctx.symbol,
            long_venue=ctx.long_venue.value,
            short_venue=ctx.short_venue.value,
            now_ms=captured_at_ms,
            max_age_ms=benchmark_max_age_ms,
        )
        if snapshots is None:
            return None
        long_snapshot, short_snapshot = snapshots
        long_observed_at_ms = long_snapshot.observed_at_ms
        short_observed_at_ms = short_snapshot.observed_at_ms
        long_vwap, long_filled, _long_limit = _l2_vwap_and_sweep_limit_for_base_quantity(
            long_snapshot.asks, requested_quantity
        )
        short_vwap, short_filled, _short_limit = _l2_vwap_and_sweep_limit_for_base_quantity(
            short_snapshot.bids, requested_quantity
        )
        quantity_tolerance = max(1e-10, requested_quantity * 1e-8)
        long_capacity = _l2_base_capacity(long_snapshot.asks)
        short_capacity = _l2_base_capacity(short_snapshot.bids)
        if (
            long_vwap <= 0.0
            or short_vwap <= 0.0
            or long_filled + quantity_tolerance < requested_quantity
            or short_filled + quantity_tolerance < requested_quantity
            or long_capacity + quantity_tolerance < requested_quantity
            or short_capacity + quantity_tolerance < requested_quantity
        ):
            return None
        return {
            "source": "local_l2_vwap",
            "position_id": ctx.entry_id,
            "symbol": ctx.symbol,
            "captured_at_ms": captured_at_ms,
            "max_observation_to_submit_ms": EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS,
            "requested_base_quantity": requested_quantity,
            "long": {
                "venue": ctx.long_venue.value,
                "side": Side.BUY.value,
                "vwap_price": long_vwap,
                "available_base_quantity": long_capacity,
                "observed_at_ms": long_observed_at_ms,
                "age_ms": captured_at_ms - long_observed_at_ms,
            },
            "short": {
                "venue": ctx.short_venue.value,
                "side": Side.SELL.value,
                "vwap_price": short_vwap,
                "available_base_quantity": short_capacity,
                "observed_at_ms": short_observed_at_ms,
                "age_ms": captured_at_ms - short_observed_at_ms,
            },
        }

    def _final_entry_revalidation_evidence(
        self,
        *,
        candidate,
        quote_lease: QuoteLease | None,
        required_quantity: float,
        final_economics,
        source: str,
    ) -> dict[str, Any]:
        edge = final_economics.edge
        strategy = self.ctx.config.strategy

        def safe_float(value: object) -> float:
            try:
                parsed = float(value or 0.0)
            except (TypeError, ValueError, OverflowError):
                return 0.0
            return parsed if math.isfinite(parsed) else 0.0

        candidate_quantity = max(
            safe_float(getattr(candidate, "entry_target_quantity", 0.0)),
            0.0,
        )
        lease_quantity = max(
            safe_float(getattr(quote_lease, "l2_vwap_quantity", 0.0)),
            0.0,
        )
        final_quantity = max(safe_float(required_quantity), 0.0)
        min_expected_edge_bps = safe_float(
            getattr(strategy, "min_expected_edge_bps", 0.0)
        )
        min_worst_case_edge_bps = safe_float(
            getattr(strategy, "min_worst_case_edge_bps", 0.0)
        )
        worst_case_funding_edge_bps = (
            safe_float(getattr(candidate, "forecast_worst_funding_edge_bps", 0.0))
            if str(getattr(candidate, "calculation_version", "") or "")
            == "enhanced_live"
            else safe_float(edge.funding_edge_bps)
        )
        return {
            "source": source,
            "candidate_revision_id": str(
                getattr(candidate, "candidate_revision_id", "") or ""
            ),
            "calculation_version": edge.calculation_version,
            "model_epoch": edge.model_epoch,
            "economics_observed_at_ms": edge.observed_at_ms,
            "economics_complete": edge.economics_complete,
            "requested_base_quantity": candidate_quantity,
            "aligned_base_quantity": lease_quantity,
            "final_base_quantity": final_quantity,
            "quote_lease_provider": str(
                getattr(quote_lease, "provider", "") or ""
            ),
            "quote_lease_candidate_revision_id": str(
                getattr(quote_lease, "candidate_revision_id", "") or ""
            ),
            "long_bid": safe_float(getattr(quote_lease, "long_bid", 0.0)),
            "long_ask": safe_float(getattr(quote_lease, "long_ask", 0.0)),
            "short_bid": safe_float(getattr(quote_lease, "short_bid", 0.0)),
            "short_ask": safe_float(getattr(quote_lease, "short_ask", 0.0)),
            "long_observed_at_ms": int(
                getattr(quote_lease, "long_observed_at_ms", 0) or 0
            ),
            "short_observed_at_ms": int(
                getattr(quote_lease, "short_observed_at_ms", 0) or 0
            ),
            "long_buy_vwap": safe_float(getattr(quote_lease, "long_buy_vwap", 0.0)),
            "short_sell_vwap": safe_float(
                getattr(quote_lease, "short_sell_vwap", 0.0)
            ),
            "final_long_entry_price": safe_float(final_economics.long_entry_price),
            "final_short_entry_price": safe_float(final_economics.short_entry_price),
            "long_l2_capacity_quantity": safe_float(
                getattr(quote_lease, "long_l2_capacity_quantity", 0.0)
            ),
            "short_l2_capacity_quantity": safe_float(
                getattr(quote_lease, "short_l2_capacity_quantity", 0.0)
            ),
            "l2_vwap_complete": (
                getattr(quote_lease, "l2_vwap_complete", False) is True
            ),
            "l2_entry_slippage_bps": final_economics.l2_entry_slippage_bps,
            "gross_signal_edge_bps": edge.gross_signal_edge_bps,
            "funding_edge_bps": edge.funding_edge_bps,
            "worst_case_funding_edge_bps": worst_case_funding_edge_bps,
            "entry_cross_bps": edge.entry_cross_bps,
            "expected_exit_cross_bps": edge.expected_exit_cross_bps,
            "entry_fee_bps": edge.entry_fee_bps,
            "exit_fee_bps": edge.exit_fee_bps,
            "entry_slippage_bps": edge.entry_slippage_bps,
            "exit_slippage_bps": edge.exit_slippage_bps,
            "adverse_selection_bps": edge.adverse_selection_bps,
            "capital_buffer_bps": edge.capital_buffer_bps,
            "execution_buffer_bps": edge.execution_buffer_bps,
            "venue_risk_haircut_bps": edge.venue_risk_haircut_bps,
            "transfer_or_inventory_bias_bps": edge.transfer_or_inventory_bias_bps,
            "final_expected_net_edge_bps": edge.expected_net_edge_bps,
            "final_worst_case_edge_bps": edge.worst_case_edge_bps,
            "expected_net_edge_bps": edge.expected_net_edge_bps,
            "worst_case_edge_bps": edge.worst_case_edge_bps,
            "min_expected_edge_bps": min_expected_edge_bps,
            "min_worst_case_edge_bps": min_worst_case_edge_bps,
            "expected_edge_floor_delta_bps": (
                edge.expected_net_edge_bps - min_expected_edge_bps
            ),
            "worst_edge_floor_delta_bps": (
                edge.worst_case_edge_bps - min_worst_case_edge_bps
            ),
            "threshold_metadata": {
                "min_expected_edge_bps": min_expected_edge_bps,
                "min_worst_case_edge_bps": min_worst_case_edge_bps,
            },
        }

    def _revalidate_final_entry_economics(
        self,
        *,
        candidate,
        quote_lease: QuoteLease | None,
        required_base_quantity: float,
        now_ms: int,
        source: str,
        execution_is_passive: bool | None = None,
        enforce_canary_notional: bool = True,
        select_passive_maker_orientation: bool = False,
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

        revalidator = FundingEntryRevalidator()
        required_quantity = max(float(required_base_quantity or 0.0), 0.0)
        local_l2_required = (
            self._local_l2_effective_enabled()
            and float(getattr(candidate, "entry_target_quantity", 0.0) or 0.0)
            > 0.0
        )

        def revalidate_orientation(
            maker_leg_override: str | None = None,
        ):
            return revalidator.revalidate_before_first_leg(
                candidate,
                long_ask=float(getattr(quote_lease, "long_ask", 0.0) or 0.0),
                short_bid=float(getattr(quote_lease, "short_bid", 0.0) or 0.0),
                long_bid=float(getattr(quote_lease, "long_bid", 0.0) or 0.0),
                short_ask=float(getattr(quote_lease, "short_ask", 0.0) or 0.0),
                now_ms=now_ms,
                config=self.ctx.config.strategy,
                long_buy_vwap=float(
                    getattr(quote_lease, "long_buy_vwap", 0.0) or 0.0
                ),
                short_sell_vwap=float(
                    getattr(quote_lease, "short_sell_vwap", 0.0) or 0.0
                ),
                required_base_quantity=required_quantity,
                l2_vwap_complete=(
                    getattr(quote_lease, "l2_vwap_complete", False) is True
                ),
                # Require L2 only when the configured readiness data plane owns
                # fresh local books. WS-BBO mode deliberately prices from BBO plus
                # the conservative shortlist slippage; treating every allocator
                # quantity as proof that local L2 exists permanently blocked that
                # valid production mode before first-leg submit.
                require_l2_vwap=local_l2_required,
                execution_is_passive=execution_is_passive,
                passive_maker_leg_override=maker_leg_override,
                long_buy_l2_capacity=float(
                    getattr(quote_lease, "long_l2_capacity_quantity", 0.0) or 0.0
                ),
                short_sell_l2_capacity=float(
                    getattr(quote_lease, "short_l2_capacity_quantity", 0.0) or 0.0
                ),
            )

        def orientation_has_capacity(maker_leg: str) -> bool:
            if quote_lease is None or required_quantity <= 0.0:
                return False
            if maker_leg == "long":
                maker_capacity = float(
                    getattr(quote_lease, "long_bid_size", 0.0) or 0.0
                )
                hedge_capacity = float(
                    getattr(quote_lease, "short_l2_capacity_quantity", 0.0)
                    or 0.0
                )
            else:
                maker_capacity = float(
                    getattr(quote_lease, "short_ask_size", 0.0) or 0.0
                )
                hedge_capacity = float(
                    getattr(quote_lease, "long_l2_capacity_quantity", 0.0)
                    or 0.0
                )
            return (
                maker_capacity + 1e-12 >= required_quantity
                and hedge_capacity + 1e-12 >= required_quantity
            )

        selected_orientation = ""
        original_maker_leg = str(
            getattr(candidate, "entry_maker_leg", "") or ""
        ).lower()
        final_economics = None
        if (
            select_passive_maker_orientation
            and execution_is_passive is not False
            and original_maker_leg in {"long", "short"}
            and str(getattr(quote_lease, "provider", "") or "")
            == "local_l2_final_vwap"
        ):
            orientation_results = []
            for maker_leg_override in ("long", "short"):
                if not orientation_has_capacity(maker_leg_override):
                    continue
                result = revalidate_orientation(maker_leg_override)
                if result.allowed:
                    orientation_results.append((maker_leg_override, result))
            if orientation_results:
                selected_orientation, final_economics = max(
                    orientation_results,
                    key=lambda item: (
                        item[1].edge.worst_case_edge_bps,
                        item[1].edge.expected_net_edge_bps,
                        1 if item[0] == "long" else 0,
                    ),
                )
                if selected_orientation != original_maker_leg:
                    candidate.entry_maker_leg = selected_orientation
                    if hasattr(candidate, "maker_leg"):
                        candidate.maker_leg = selected_orientation
                    self.ctx.journal.append(
                        "runtime.entry_passive_maker_orientation_selected",
                        {
                            "symbol": str(getattr(candidate, "symbol", "") or ""),
                            "candidate_pair_id": self._candidate_pair_id(candidate),
                            "pair_id": self._candidate_pair_id(candidate),
                            "previous_entry_maker_leg": original_maker_leg,
                            "selected_entry_maker_leg": selected_orientation,
                            "expected_net_edge_bps": (
                                final_economics.edge.expected_net_edge_bps
                            ),
                            "worst_case_edge_bps": (
                                final_economics.edge.worst_case_edge_bps
                            ),
                            "required_base_quantity": required_quantity,
                            "source": source,
                            "ts_ms": now_ms,
                        },
                    )
        if final_economics is None:
            final_economics = revalidate_orientation(None)
        if not final_economics.allowed:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=final_economics.reason,
                blocked_reasons=[final_economics.reason],
                source=source,
                decision="skip_before_first_leg",
                extra=self._final_entry_revalidation_evidence(
                    candidate=candidate,
                    quote_lease=quote_lease,
                    required_quantity=required_quantity,
                    final_economics=final_economics,
                    source=source,
                ),
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
        if float(required_base_quantity or 0.0) > 0.0:
            final_economics_reason = self._apply_final_entry_economics(
                candidate,
                quantity=required_base_quantity,
                long_price=final_economics.long_entry_price,
                short_price=final_economics.short_entry_price,
            )
            if final_economics_reason:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=final_economics_reason,
                    blocked_reasons=[final_economics_reason],
                    source=source,
                    decision="skip_before_first_leg",
                )
                return False

        return True

    async def _resolve_entry_quantity_steps(
        self,
        *,
        candidate,
        long_venue: Venue,
        short_venue: Venue,
        price_hint: float,
        now_ms: int,
    ) -> tuple[
        float,
        float,
        float,
        float | None,
        float | None,
        dict,
        dict,
    ] | None:
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
            payload = {
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "requested_quantity": raw_quantity,
                "entry_max_executable_quantity": entry_capacity,
                "reason": "common_base_capacity_exceeded",
                "ts_ms": now_ms,
            }
            self.ctx.journal.append(
                "runtime.entry_skipped_allocator_capacity_exceeded",
                payload,
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="common_base_capacity_exceeded",
                blocked_reasons=["common_base_capacity_exceeded"],
                source="entry_quantity_resolution",
                decision="skip_before_first_leg",
                extra=payload,
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
            payload = {
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "raw_quantity": raw_quantity,
                "reason": "okx_ct_val_lot_sz_unconfirmed",
                "ts_ms": now_ms,
            }
            self.ctx.journal.append(
                "runtime.entry_skipped_okx_contract_metadata_missing",
                payload,
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="okx_ct_val_lot_sz_unconfirmed",
                blocked_reasons=["okx_ct_val_lot_sz_unconfirmed"],
                source="entry_quantity_resolution",
                decision="skip_before_first_leg",
                extra=payload,
            )
            return None
        if quantity <= 0:
            payload = {
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "okx_base_quantity_step": okx_base_step,
                "raw_quantity": raw_quantity,
                "reason": "quantity_below_okx_contract_step",
                "ts_ms": now_ms,
            }
            self.ctx.journal.append(
                "runtime.entry_skipped_okx_contract_step",
                payload,
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="quantity_below_okx_contract_step",
                blocked_reasons=["quantity_below_okx_contract_step"],
                source="entry_quantity_resolution",
                decision="skip_before_first_leg",
                extra=payload,
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
            payload = {
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "missing_venues": missing_venues,
                "missing_fields": missing_fields,
                "raw_quantity": raw_quantity,
                "common_quantity": quantity,
                "reason": "quantity_metadata_missing",
                "ts_ms": now_ms,
            }
            self.ctx.journal.append(
                "runtime.entry_skipped_quantity_metadata_missing",
                payload,
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="quantity_metadata_missing",
                blocked_reasons=["quantity_metadata_missing"],
                source="entry_quantity_resolution",
                decision="skip_before_first_leg",
                extra=payload,
            )
            return None
        long_quantity_metadata = await self._entry_venue_quantity_metadata_evidence(
            long_venue,
            candidate.symbol,
            long_quantity_step,
            long_missing_quantity_fields,
        )
        short_quantity_metadata = await self._entry_venue_quantity_metadata_evidence(
            short_venue,
            candidate.symbol,
            short_quantity_step,
            short_missing_quantity_fields,
        )
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
                return (
                    raw_quantity,
                    quantity,
                    okx_base_step,
                    long_quantity_step,
                    short_quantity_step,
                    long_quantity_metadata,
                    short_quantity_metadata,
                )
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
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="common_base_quantity_grid_invalid",
                blocked_reasons=["common_base_quantity_grid_invalid"],
                source="entry_quantity_resolution",
                decision="skip_before_first_leg",
            )
            return None
        quantity = _align_base_quantity_down(quantity, common_base_quantity_step)
        if quantity <= 0.0:
            if not enforce_common_grid:
                return (
                    raw_quantity,
                    raw_quantity,
                    okx_base_step,
                    long_quantity_step,
                    short_quantity_step,
                    long_quantity_metadata,
                    short_quantity_metadata,
                )
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
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="quantity_below_common_base_quantity_step",
                blocked_reasons=["quantity_below_common_base_quantity_step"],
                source="entry_quantity_resolution",
                decision="skip_before_first_leg",
            )
            return None
        return (
            raw_quantity,
            quantity,
            okx_base_step,
            long_quantity_step,
            short_quantity_step,
            long_quantity_metadata,
            short_quantity_metadata,
        )

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
            first_l2_reason = str(
                (l2_stale_decisions[0] if l2_stale_decisions else {}).get("l2_reason")
                or "execution_l2_stale"
            )
            setattr(self.ctx, "_last_entry_dispatch_block_l2_reason", first_l2_reason)
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=first_l2_reason,
                blocked_reasons=[first_l2_reason],
                source="entry_local_l2_readiness",
                decision="skip_dispatch",
                extra={
                    "l2_stale_decisions": l2_stale_decisions,
                    "not_ready_reasons": not_ready_reasons,
                },
            )
            for payload in l2_stale_decisions:
                payload = dict(payload)
                payload["reason_bucket"] = str(
                    payload.get("l2_reason")
                    or payload.get("reason")
                    or "execution_l2_stale"
                )
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
                    "reason": first_l2_reason,
                    "reason_bucket": first_l2_reason,
                    "l2_reason": first_l2_reason,
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
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=str(skew_blocker.get("reason") or "execution_skew"),
                blocked_reasons=[str(skew_blocker.get("reason") or "execution_skew")],
                source="entry_final_gate",
                decision="skip_dispatch",
                extra=dict(skew_blocker),
            )
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
        long_quantity_metadata: dict[str, Any] | None = None,
        short_quantity_metadata: dict[str, Any] | None = None,
        common_base_quantity_step: float = 0.0,
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
            # Expected-shortfall admission is retired.  The field remains in
            # durable state solely so historical pending entries can recover.
            expected_shortfall_bps_entry=0.0,
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
            candidate_revision_id=_str_attr("candidate_revision_id"),
            entry_max_leg_notional_quote=_float_attr(
                "entry_max_leg_notional_quote"
            ),
            funding_canary_enabled_at_entry=False,
            funding_canary_fee_assurance_tier="",
            funding_canary_hard_max_entry_notional_quote=0.0,
            funding_canary_size_constrained=False,
            long_symbol_rule_at_entry=self._freeze_symbol_rule_at_entry(
                venue=long_venue,
                symbol=candidate.symbol,
                metadata=dict(long_quantity_metadata or {}),
            ),
            short_symbol_rule_at_entry=self._freeze_symbol_rule_at_entry(
                venue=short_venue,
                symbol=candidate.symbol,
                metadata=dict(short_quantity_metadata or {}),
            ),
            common_base_quantity_step_at_entry=float(
                common_base_quantity_step or 0.0
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

    def _selected_pre_submit_deadline_exceeded(
        self,
        candidate,
        *,
        selected_deadline_monotonic: float | None,
        selected_at_ms: int,
        stage: str,
    ) -> bool:
        if selected_deadline_monotonic is None:
            return False
        if asyncio.get_running_loop().time() < selected_deadline_monotonic:
            return False
        deadline_ms = self._selected_submit_deadline_ms()
        candidate_pair_id = self._candidate_pair_id(candidate)
        deadline_ts_ms = int(selected_at_ms) + deadline_ms
        self._emit_entry_dispatch_viability_blocked(
            candidate,
            deadline_ts_ms,
            reason="selected_submit_deadline_exceeded",
            blocked_reasons=["selected_submit_deadline_exceeded"],
            source="selected_submit_deadline",
            decision="skip_before_first_leg",
            extra={
                "deadline_ms": deadline_ms,
                "blocking_stage": stage,
                "blocking_domain": "entry_dispatch",
                "blocking_status": "timeout",
            },
        )
        self.ctx.journal.append(
            "runtime.entry_selected_submit_deadline_exceeded",
            {
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "symbol": str(getattr(candidate, "symbol", "") or ""),
                "deadline_ms": deadline_ms,
                "blocking_stage": stage,
                "blocking_domain": "entry_dispatch",
                "blocking_status": "timeout",
                "blocking_reason": "selected_submit_deadline_exceeded",
                "reason": "selected_submit_deadline_exceeded",
                "ts_ms": deadline_ts_ms,
            },
        )
        self.ctx.journal.append(
            "review.candidate_rejected",
            {
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "symbol": str(getattr(candidate, "symbol", "") or ""),
                "long_venue": str(getattr(candidate, "long_venue", "") or ""),
                "short_venue": str(getattr(candidate, "short_venue", "") or ""),
                "rejected_stage": stage,
                "rejected_reason": "selected_submit_deadline_exceeded",
                "ts_ms": deadline_ts_ms,
            },
        )
        return True

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
        selected_deadline_monotonic: float | None = None,
    ):
        deadline_ms = self._selected_submit_deadline_ms()
        if deadline_ms <= 0:
            return await self.ctx.entry_executor.execute(ctx)

        remaining_s = deadline_ms / 1_000.0
        if selected_deadline_monotonic is not None:
            remaining_s = max(
                selected_deadline_monotonic - asyncio.get_running_loop().time(),
                0.0,
            )
        if remaining_s <= 0.0:
            return None

        task = asyncio.create_task(self.ctx.entry_executor.execute(ctx))
        done, _pending = await asyncio.wait(
            {task},
            timeout=remaining_s,
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

        await self._cancel_selected_submit_task(task)
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
        quote_lease: QuoteLease | None = None,
        requires_final_quote_lease: bool = False,
        selected_deadline_monotonic: float | None = None,
        selected_at_ms: int | None = None,
        leverage_evidence_for_sizing: dict[
            Venue, EntryLeverageEvidence
        ] | None = None,
    ) -> bool:
        try:
            # V1: execution.entry_selected — engine decided to open this candidate
            candidate_pair_id = self._candidate_pair_id(candidate)
            planned_entry_notional_quote = max(
                float(ctx.long_price_hint), float(ctx.short_price_hint)
            ) * float(effective_quantity)
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
                    "runtime_mode": str(
                        self.ctx.config.runtime.mode or ""
                    ).lower(),
                    "entry_notional_quote": float(
                        getattr(candidate, "entry_notional_quote", 0.0) or 0.0
                    ),
                    # The candidate shortlist amount is not a submission
                    # contract.  Persist the final rounded size and IOC price
                    # basis so offline analysis can independently prove the
                    # selected cohort's configured per-leg cap.
                    "planned_entry_notional_quote": planned_entry_notional_quote,
                    "planned_entry_quantity": float(effective_quantity),
                    "planned_long_entry_price": float(ctx.long_price_hint),
                    "planned_short_entry_price": float(ctx.short_price_hint),
                    "expected_net_edge_bps": float(
                        getattr(candidate, "expected_net_edge_bps", 0.0) or 0.0
                    ),
                    "worst_case_edge_bps": float(
                        getattr(candidate, "worst_case_edge_bps", 0.0) or 0.0
                    ),
                    "economics_complete": getattr(candidate, "economics_complete", False)
                    is True,
                    "ts_ms": now_ms,
                },
            )
            raw_entry_benchmark = self._capture_entry_execution_benchmark_receipt(ctx)
            if raw_entry_benchmark is not None:
                ctx = replace(
                    ctx,
                    entry_execution_benchmark_receipt=raw_entry_benchmark,
                )

            # Context construction, journaling and benchmark capture can all
            # consume the short quote lease.  Re-read both the provider lease
            # and the exact lease used for pricing at the last await boundary
            # before the executor can submit the first order.
            submit_clock = getattr(self.ctx, "_entry_wall_clock_now_ms", None)
            executor_submit_now_ms = int(
                submit_clock()
                if callable(submit_clock)
                else time.time() * 1_000
            )
            if requires_final_quote_lease:
                provider_side_capacity_proven_by_final_l2 = (
                    isinstance(quote_lease, QuoteLease)
                    and str(getattr(quote_lease, "provider", "") or "")
                    == "local_l2_final_vwap"
                )
                provider_reason, _provider_lease, provider_evidence = (
                    self._entry_quote_lease_execution_check(
                        candidate,
                        executor_submit_now_ms,
                        enforce_side_capacity=(
                            not provider_side_capacity_proven_by_final_l2
                        ),
                    )
                )
                if provider_reason:
                    self._emit_entry_dispatch_viability_blocked(
                        candidate,
                        executor_submit_now_ms,
                        reason=provider_reason,
                        blocked_reasons=[provider_reason],
                        source="executor_submit_quote_lease",
                        decision="skip_before_first_leg",
                        extra=provider_evidence,
                    )
                    return False
                final_lease_reason = self._final_quote_lease_reason(
                    candidate,
                    quote_lease,
                    executor_submit_now_ms,
                )
                if final_lease_reason:
                    self._emit_entry_dispatch_viability_blocked(
                        candidate,
                        executor_submit_now_ms,
                        reason=final_lease_reason,
                        blocked_reasons=[final_lease_reason],
                        source="executor_submit_quote_lease",
                        decision="skip_before_first_leg",
                        extra={
                            "candidate_revision_id": str(
                                getattr(candidate, "candidate_revision_id", "")
                                or ""
                            ),
                            "lease_candidate_revision_id": str(
                                getattr(
                                    quote_lease,
                                    "candidate_revision_id",
                                    "",
                                )
                                or ""
                            ),
                        },
                    )
                    return False

            # Reserve a small executor handoff window before the first
            # exchange-side mutation.  A leverage preparation that cannot
            # finish inside this budget is cancelled; its transaction waits
            # for verified rollback before propagating cancellation.
            if selected_deadline_monotonic is not None:
                leverage_budget_s = (
                    selected_deadline_monotonic
                    - asyncio.get_running_loop().time()
                    - 0.05
                )
                if leverage_budget_s <= 0.0:
                    self._emit_entry_dispatch_viability_blocked(
                        candidate,
                        executor_submit_now_ms,
                        reason="entry_leverage_prepare_deadline_exceeded",
                        blocked_reasons=[
                            "entry_leverage_prepare_deadline_exceeded"
                        ],
                        source="entry_leverage_prepare",
                        decision="skip_before_first_leg",
                    )
                    return False
            else:
                leverage_budget_s = None
            prepare_awaitable = self._prepare_live_entry_leverage_for_candidate(
                candidate=candidate,
                now_ms=executor_submit_now_ms,
                long_venue=ctx.long_venue,
                short_venue=ctx.short_venue,
                notional_quote=max(ctx.long_price_hint, ctx.short_price_hint)
                * effective_quantity,
                minimum_evidence_by_venue=(
                    leverage_evidence_for_sizing or {}
                ),
            )
            try:
                if leverage_budget_s is None:
                    leverage_ready, _final_leverage_evidence = (
                        await prepare_awaitable
                    )
                else:
                    leverage_ready, _final_leverage_evidence = await asyncio.wait_for(
                        prepare_awaitable,
                        timeout=leverage_budget_s,
                    )
            except asyncio.TimeoutError:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    executor_submit_now_ms,
                    reason="entry_leverage_prepare_deadline_exceeded",
                    blocked_reasons=[
                        "entry_leverage_prepare_deadline_exceeded"
                    ],
                    source="entry_leverage_prepare",
                    decision="skip_before_first_leg",
                )
                return False
            if not leverage_ready:
                if not str(getattr(self.ctx, "_last_entry_dispatch_block_reason", "") or ""):
                    self._emit_entry_dispatch_viability_blocked(
                        candidate,
                        executor_submit_now_ms,
                        reason="entry_leverage_prepare_failed",
                        blocked_reasons=["entry_leverage_prepare_failed"],
                        source="entry_leverage_prepare",
                        decision="skip_before_first_leg",
                    )
                return False
            result = await self._execute_entry_with_selected_deadline(
                ctx=ctx,
                candidate=candidate,
                selected_seq=selected_seq,
                selected_at_ms=int(selected_at_ms or executor_submit_now_ms),
                selected_deadline_monotonic=selected_deadline_monotonic,
            )
            if result is None:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    int(selected_at_ms or executor_submit_now_ms)
                    + self._selected_submit_deadline_ms(),
                    reason="no_submit_or_order_evidence",
                    blocked_reasons=["no_submit_or_order_evidence"],
                    source="selected_submit_deadline",
                    decision="skip_before_first_leg",
                    extra={"entry_id": ctx.entry_id},
                )
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
                    self._emit_entry_dispatch_viability_blocked(
                        candidate,
                        now_ms,
                        reason="terminal_entry_residual_unregistered",
                        blocked_reasons=["terminal_entry_residual_unregistered"],
                        source="entry_residual_queue",
                        decision="skip_after_first_leg",
                        extra={"entry_id": ctx.entry_id},
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
                    result.pending_entry.to_dict(),
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

    async def _dispatch_entry(
        self,
        candidate,
        now_ms: int,
        price_hint: float = 0.0,
        selected_deadline_monotonic: float | None = None,
        selected_at_ms: int | None = None,
    ) -> bool:
        """Transform a tradeable candidate into an entry context and execute via entry_executor.

        V1: entry route/maker-leg/price gate from config and execution planner.
        Fix 5: no 1.0 pseudo-price — reject entries without valid quote.
        Fix EN-001: route and maker leg driven by planner, not hardcoded in runtime.
        """
        setattr(self.ctx, "_last_entry_dispatch_block_reason", "")
        setattr(self.ctx, "_last_entry_dispatch_block_l2_reason", "")
        if self._selected_pre_submit_deadline_exceeded(
            candidate,
            selected_deadline_monotonic=selected_deadline_monotonic,
            selected_at_ms=int(selected_at_ms or now_ms),
            stage="dispatch_start",
        ) or self._entry_initial_gate_blocked(candidate, now_ms):
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
        price_consistency_reason = self._entry_normalized_price_consistency_reason(
            long_price=long_order_price_hint,
            short_price=short_order_price_hint,
        )
        if price_consistency_reason:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=price_consistency_reason,
                blocked_reasons=[price_consistency_reason],
                source="final_entry_price_normalization",
                decision="skip_before_first_leg",
                extra={
                    "long_order_price_hint": long_order_price_hint,
                    "short_order_price_hint": short_order_price_hint,
                },
            )
            return False

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
            enforce_canary_notional=False,
            select_passive_maker_orientation=True,
        ):
            return False

        quantity_resolution = await self._resolve_entry_quantity_steps(
            candidate=candidate,
            long_venue=long_venue,
            short_venue=short_venue,
            price_hint=price_hint,
            now_ms=now_ms,
        )
        if self._selected_pre_submit_deadline_exceeded(
            candidate,
            selected_deadline_monotonic=selected_deadline_monotonic,
            selected_at_ms=int(selected_at_ms or now_ms),
            stage="quantity_step_resolution",
        ):
            return False
        if quantity_resolution is None:
            return False
        (
            raw_quantity,
            quantity,
            okx_base_step,
            long_quantity_step,
            short_quantity_step,
            long_quantity_metadata,
            short_quantity_metadata,
        ) = quantity_resolution
        try:
            original_entry_target_quantity = float(
                getattr(candidate, "entry_target_quantity", 0.0) or 0.0
            )
        except (TypeError, ValueError, OverflowError):
            original_entry_target_quantity = 0.0
        quantity = max(float(quantity or 0.0), 0.0)
        if (
            self.ctx.config.runtime.mode == "live"
            and original_entry_target_quantity <= 0.0
            and quantity > 0.0
        ):
            setattr(candidate, "entry_target_quantity", quantity)
            try:
                previous_max_executable_quantity = float(
                    getattr(candidate, "entry_max_executable_quantity", 0.0)
                    or 0.0
                )
            except (TypeError, ValueError, OverflowError):
                previous_max_executable_quantity = 0.0
            if previous_max_executable_quantity <= 0.0:
                setattr(candidate, "entry_max_executable_quantity", quantity)
            setattr(
                candidate,
                "entry_requested_target_quantity_before_resolution",
                original_entry_target_quantity,
            )
            self.ctx.journal.append(
                "runtime.entry_legacy_target_quantity_resolved",
                {
                    "symbol": str(getattr(candidate, "symbol", "") or ""),
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "candidate_pair_id": self._candidate_pair_id(candidate),
                    "pair_id": self._candidate_pair_id(candidate),
                    "requested_entry_target_quantity": (
                        original_entry_target_quantity
                    ),
                    "raw_quantity": raw_quantity,
                    "aligned_common_quantity": quantity,
                    "previous_entry_max_executable_quantity": (
                        previous_max_executable_quantity
                    ),
                    "entry_notional_quote": float(
                        getattr(candidate, "entry_notional_quote", 0.0) or 0.0
                    ),
                    "ts_ms": now_ms,
                },
            )
        # Private margin and portfolio limits are live-entry checks only.
        # Recovery, passive close and residual repair do not pass through
        # this branch.  The retired expected-shortfall model deliberately has
        # no influence on size or admission here.
        live_candidate = (
            self.ctx.config.runtime.mode == "live"
            and quantity > 0.0
        )
        leverage_evidence_for_sizing: dict[Venue, EntryLeverageEvidence] = {}
        if live_candidate:
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
            if self._selected_pre_submit_deadline_exceeded(
                candidate,
                selected_deadline_monotonic=selected_deadline_monotonic,
                selected_at_ms=int(selected_at_ms or now_ms),
                stage="leverage_inspection",
            ):
                return False
            if not leverage_ready:
                if not str(getattr(self.ctx, "_last_entry_dispatch_block_reason", "") or ""):
                    self._emit_entry_dispatch_viability_blocked(
                        candidate,
                        now_ms,
                        reason="entry_leverage_unavailable",
                        blocked_reasons=["entry_leverage_unavailable"],
                        source="entry_leverage_inspection",
                        decision="skip_before_first_leg",
                    )
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
            if self._selected_pre_submit_deadline_exceeded(
                candidate,
                selected_deadline_monotonic=selected_deadline_monotonic,
                selected_at_ms=int(selected_at_ms or now_ms),
                stage="margin_quantity_resolution",
            ):
                return False
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
                max_concurrent_positions_per_venue=(
                    strategy.max_concurrent_positions_per_venue
                ),
                max_concurrent_positions_per_symbol=(
                    strategy.max_concurrent_positions_per_symbol
                ),
                max_concurrent_positions_per_venue_pair=(
                    strategy.max_concurrent_positions_per_venue_pair
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
                block_reason = reason or gate_name
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=block_reason,
                    blocked_reasons=[block_reason],
                    source="dispatch_runtime_gate",
                    decision="skip_before_first_leg",
                    extra={"gate": gate_name},
                )
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

        common_base_quantity_step = _common_base_quantity_step(
            okx_base_step,
            long_quantity_step,
            short_quantity_step,
        )
        pair_minimum_reason, pair_minimum_evidence = (
            self._entry_pair_minimum_reason(
                quantity=quantity,
                long_price=long_order_price_hint,
                short_price=short_order_price_hint,
                long_metadata=long_quantity_metadata,
                short_metadata=short_quantity_metadata,
                strategy_min_notional=min_notional,
            )
        )
        if pair_minimum_reason:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=pair_minimum_reason,
                blocked_reasons=[pair_minimum_reason],
                source="entry_pair_minimum",
                decision="skip_before_first_leg",
                extra=pair_minimum_evidence,
            )
            return False

        maker_quantity_metadata = (
            long_quantity_metadata
            if maker_leg == Side.BUY
            else short_quantity_metadata
        )
        hedge_quantity_metadata = (
            short_quantity_metadata
            if maker_leg == Side.BUY
            else long_quantity_metadata
        )
        maker_min_notional = max(
            float(min_notional or 0.0),
            float(maker_quantity_metadata.get("min_notional", 0.0) or 0.0),
        )
        hedge_min_notional = max(
            float(min_notional or 0.0),
            float(hedge_quantity_metadata.get("min_notional", 0.0) or 0.0),
        )
        maker_min_quantity = self._safe_positive_float(
            maker_quantity_metadata.get("min_quantity")
        )
        hedge_min_quantity = self._safe_positive_float(
            hedge_quantity_metadata.get("min_quantity")
        )
        # The first maker clip must itself satisfy the symbol's quantity
        # minimum; express that floor in the planner's notional contract.
        if maker_planner_price > 0.0 and maker_min_quantity > 0.0:
            maker_min_notional = max(
                maker_min_notional,
                maker_min_quantity * maker_planner_price,
            )
        try:
            min_hedgeable_chunk = min_hedgeable_chunk_from_notional(
                min_base_quantity=hedge_min_quantity,
                min_notional_quote=hedge_min_notional,
                # Every fill chunk must be executable on both venues, not
                # merely on the hedge venue's smaller native step.
                step_base_quantity=common_base_quantity_step,
                price_hint=(
                    hedge_planner_price if hedge_planner_price > 0.0 else None
                ),
            )
        except ValueError:
            payload = {
                "symbol": candidate.symbol,
                "maker_venue": (
                    long_venue.value
                    if maker_leg == Side.BUY
                    else short_venue.value
                ),
                "hedge_venue": (
                    short_venue.value
                    if maker_leg == Side.BUY
                    else long_venue.value
                ),
                "maker_min_notional_quote": maker_min_notional,
                "hedge_min_notional_quote": hedge_min_notional,
                "hedge_min_quantity": hedge_min_quantity,
                "common_base_quantity_step": common_base_quantity_step,
                "reason": "min_hedgeable_chunk_invalid",
                "ts_ms": now_ms,
            }
            self.ctx.journal.append(
                "runtime.entry_skipped_planner_metadata_invalid",
                payload,
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason="min_hedgeable_chunk_invalid",
                blocked_reasons=["min_hedgeable_chunk_invalid"],
                source="entry_execution_planner",
                decision="skip_before_first_leg",
                extra=payload,
            )
            return False

        route, plan = plan_incremental_entry_execution(
            target_quantity=quantity,
            slice_ratio=strategy.maker_initial_slice_ratio,
            min_hedgeable_chunk=min_hedgeable_chunk,
            maker_min_notional_quote=maker_min_notional,
            maker_price_hint=maker_planner_price if maker_planner_price > 0 else None,
            max_initial_clip_ratio=strategy.entry_max_initial_clip_ratio,
            hedge_min_notional_quote=hedge_min_notional,
            hedge_price_hint=hedge_planner_price if hedge_planner_price > 0 else None,
        )

        if route == ExecutionRoute.REJECTED:
            plan_reason = str(plan.reason or "planner_rejected_entry")
            self.ctx.journal.append(
                "runtime.entry_skipped_planner_rejected",
                {
                    "symbol": candidate.symbol,
                    "target_quantity": quantity,
                    "reason": plan_reason,
                },
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=plan_reason,
                blocked_reasons=[plan_reason],
                source="entry_execution_planner",
                decision="skip_before_first_leg",
                extra={"target_quantity": quantity},
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
        local_l2_execution = self._local_l2_effective_enabled()
        if live_candidate and entry_type == EntryType.STANDARD_DUAL_TAKER:
            final_quote_result = FinalQuoteLeaseResult(quote_lease)
            if local_l2_execution:
                final_quote_result = self._local_l2_final_quote_result(
                    candidate,
                    now_ms,
                    target_quantity=effective_quantity,
                )
            execution_quote_lease = final_quote_result.lease
            if execution_quote_lease is None:
                reason = (
                    final_quote_result.reason
                    or "final_execution_quote_unavailable"
                )
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=reason,
                    blocked_reasons=[reason],
                    source="final_submit_economics",
                    decision="skip_before_first_leg",
                    extra={
                        **final_quote_result.evidence,
                        "effective_quantity": effective_quantity,
                    },
                )
                return False
            if local_l2_execution:
                final_long_price, final_short_price = (
                    _standard_ioc_price_hints(execution_quote_lease)
                )
            else:
                final_long_price, final_short_price = _ws_bbo_ioc_price_hints(
                    execution_quote_lease,
                    candidate,
                )
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
            if local_l2_execution:
                long_order_price_hint, short_order_price_hint = (
                    _standard_ioc_price_hints(quote_lease)
                )
            else:
                long_order_price_hint, short_order_price_hint = (
                    _ws_bbo_ioc_price_hints(quote_lease, candidate)
                )
            if getattr(quote_lease, "l2_vwap_complete", False) is True:
                price_hint = (
                    float(getattr(quote_lease, "long_buy_vwap", 0.0) or 0.0)
                    + float(getattr(quote_lease, "short_sell_vwap", 0.0) or 0.0)
                ) / 2.0

        price_consistency_reason = self._entry_normalized_price_consistency_reason(
            long_price=long_order_price_hint,
            short_price=short_order_price_hint,
        )
        if price_consistency_reason:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=price_consistency_reason,
                blocked_reasons=[price_consistency_reason],
                source="final_submit_price_normalization",
                decision="skip_before_first_leg",
            )
            return False

        final_pair_minimum_reason, final_pair_minimum_evidence = (
            self._entry_pair_minimum_reason(
                quantity=(
                    plan.full_target_quantity
                    if entry_type == EntryType.PASSIVE_INCREMENTAL
                    else effective_quantity
                ),
                long_price=long_order_price_hint,
                short_price=short_order_price_hint,
                long_metadata=long_quantity_metadata,
                short_metadata=short_quantity_metadata,
                strategy_min_notional=min_notional,
            )
        )
        if final_pair_minimum_reason:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=final_pair_minimum_reason,
                blocked_reasons=[final_pair_minimum_reason],
                source="final_entry_pair_minimum",
                decision="skip_before_first_leg",
                extra=final_pair_minimum_evidence,
            )
            return False

        entry_id = f"entry-{now_ms}-{candidate.symbol}"

        # --- V1 recovery dedup: check for duplicate entries after restart ---
        # Must use the same CID generation as build_entry_orders so the
        # dedup index keys match the actual on-wire clientOrderId.
        maker_venue = long_venue if maker_leg == Side.BUY else short_venue
        hedge_venue = short_venue if maker_leg == Side.BUY else long_venue
        maker_cid = generate_exchange_cid(entry_id, "m", maker_venue)
        hedge_cid = generate_exchange_cid(entry_id, "h", hedge_venue)
        venue_quantity_metadata = {
            long_venue.value: long_quantity_metadata,
            short_venue.value: short_quantity_metadata,
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
        maker_admission_metadata = {
            **self._entry_passive_metadata(maker_venue, candidate.symbol),
            **maker_quantity_metadata,
        }
        hedgeability_decision = PendingEntryAdmissionCore.decide(
            PendingEntryAdmissionRequest(
                symbol=candidate.symbol,
                long_venue=long_venue.value,
                short_venue=short_venue.value,
                maker_venue=maker_venue.value,
                hedge_venue=hedge_venue.value,
                entry_type=entry_type.value,
                maker_metadata=maker_admission_metadata,
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
            hedgeability_reason = str(
                payload.get("reason") or "pre_submit_hedgeability_guard_failed"
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=hedgeability_reason,
                blocked_reasons=[hedgeability_reason],
                source="pre_submit_hedgeability_guard",
                decision="skip_before_first_leg",
                extra=dict(payload),
            )
            self.ctx.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "rejected_stage": "pre_submit_hedgeability_guard",
                    "rejected_reason": hedgeability_reason,
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "expected_edge_bps": candidate.expected_edge_bps,
                    "funding_edge_bps": candidate.funding_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return False

        if is_client_order_id_duplicate(maker_cid, self.ctx._recovery_dedup_index):
            reason = "duplicate_maker_client_order_id"
            self.ctx.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": maker_cid,
                    "reason": reason,
                },
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=reason,
                blocked_reasons=[reason],
                source="entry_recovery_dedup",
                decision="skip_before_first_leg",
                extra={"entry_id": entry_id, "client_order_id": maker_cid},
            )
            return False

        if is_client_order_id_duplicate(hedge_cid, self.ctx._recovery_dedup_index):
            reason = "duplicate_hedge_client_order_id"
            self.ctx.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": hedge_cid,
                    "reason": reason,
                },
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=reason,
                blocked_reasons=[reason],
                source="entry_recovery_dedup",
                decision="skip_before_first_leg",
                extra={"entry_id": entry_id, "client_order_id": hedge_cid},
            )
            return False

        # Check for existing pending entry on same symbol pair
        if has_pending_entry_for_symbol(
            self.ctx.state, candidate.symbol,
            long_venue.value, short_venue.value,
        ):
            reason = "existing_pending_entry_for_symbol_pair"
            self.ctx.journal.append(
                "runtime.entry_skipped_existing_pending",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reason": reason,
                },
            )
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=reason,
                blocked_reasons=[reason],
                source="entry_recovery_dedup",
                decision="skip_before_first_leg",
            )
            return False

        admission_ready = await self._precheck_entry_admission(
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
        )
        if self._selected_pre_submit_deadline_exceeded(
            candidate,
            selected_deadline_monotonic=selected_deadline_monotonic,
            selected_at_ms=int(selected_at_ms or now_ms),
            stage="entry_admission_precheck",
        ):
            return False
        if not admission_ready:
            if not str(getattr(self.ctx, "_last_entry_dispatch_block_reason", "") or ""):
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason="entry_admission_precheck_failed",
                    blocked_reasons=["entry_admission_precheck_failed"],
                    source="entry_admission_precheck",
                    decision="skip_before_first_leg",
                )
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
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=bbo_reason,
                    blocked_reasons=[bbo_reason],
                    source="post_only_bbo_gate",
                    decision="skip_before_first_leg",
                    extra=payload,
                )
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
                    if isinstance(quote_lease, QuoteLease):
                        quote_lease = replace(quote_lease, long_bid=repriced_price)
                else:
                    short_order_price_hint = repriced_price
                    if isinstance(quote_lease, QuoteLease):
                        quote_lease = replace(quote_lease, short_ask=repriced_price)
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

            if live_candidate and repriced_price > 0.0:
                if not self._revalidate_final_entry_economics(
                    candidate=candidate,
                    quote_lease=quote_lease,
                    required_base_quantity=plan.full_target_quantity,
                    now_ms=now_ms,
                    source="final_passive_reprice",
                    execution_is_passive=True,
                ):
                    return False

            passive_price_consistency_reason = (
                self._entry_normalized_price_consistency_reason(
                    long_price=long_order_price_hint,
                    short_price=short_order_price_hint,
                )
            )
            if passive_price_consistency_reason:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=passive_price_consistency_reason,
                    blocked_reasons=[passive_price_consistency_reason],
                    source="final_passive_price_normalization",
                    decision="skip_before_first_leg",
                )
                return False

            passive_minimum_reason, passive_minimum_evidence = (
                self._entry_pair_minimum_reason(
                    quantity=plan.full_target_quantity,
                    long_price=long_order_price_hint,
                    short_price=short_order_price_hint,
                    long_metadata=long_quantity_metadata,
                    short_metadata=short_quantity_metadata,
                    strategy_min_notional=min_notional,
                )
            )
            if passive_minimum_reason:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=passive_minimum_reason,
                    blocked_reasons=[passive_minimum_reason],
                    source="final_passive_pair_minimum",
                    decision="skip_before_first_leg",
                    extra=passive_minimum_evidence,
                )
                return False

        final_submission_quantity = (
            plan.full_target_quantity
            if entry_type == EntryType.PASSIVE_INCREMENTAL
            else effective_quantity
        )
        if live_candidate:
            final_economics_reason = self._apply_final_entry_economics(
                candidate,
                quantity=final_submission_quantity,
                long_price=long_order_price_hint,
                short_price=short_order_price_hint,
            )
            if final_economics_reason:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    now_ms,
                    reason=final_economics_reason,
                    blocked_reasons=[final_economics_reason],
                    source="final_candidate_economics_binding",
                    decision="skip_before_first_leg",
                )
                return False
        final_binding_reason = ""
        if live_candidate:
            final_binding_reason = self._final_entry_economics_binding_reason(
                candidate,
                quantity=final_submission_quantity,
                long_price=long_order_price_hint,
                short_price=short_order_price_hint,
            )
        if final_binding_reason:
            self._emit_entry_dispatch_viability_blocked(
                candidate,
                now_ms,
                reason=final_binding_reason,
                blocked_reasons=[final_binding_reason],
                source="final_candidate_economics_binding",
                decision="skip_before_first_leg",
            )
            return False
        submit_clock = getattr(self.ctx, "_entry_wall_clock_now_ms", None)
        submit_now_ms = int(
            submit_clock() if callable(submit_clock) else time.time() * 1_000
        )
        requires_final_quote_lease = (
            self.ctx.config.runtime.mode == "live"
            and (
                live_candidate
                or self._entry_readiness_provider_uses_quote_lease()
                or self._local_l2_effective_enabled()
            )
        )
        if requires_final_quote_lease:
            provider_side_capacity_proven_by_final_l2 = (
                isinstance(quote_lease, QuoteLease)
                and str(getattr(quote_lease, "provider", "") or "")
                == "local_l2_final_vwap"
            )
            provider_reason, _provider_lease, provider_evidence = (
                self._entry_quote_lease_execution_check(
                    candidate,
                    submit_now_ms,
                    enforce_side_capacity=(
                        not provider_side_capacity_proven_by_final_l2
                    ),
                )
            )
            if provider_reason:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    submit_now_ms,
                    reason=provider_reason,
                    blocked_reasons=[provider_reason],
                    source="submit_quote_lease",
                    decision="skip_before_first_leg",
                    extra=provider_evidence,
                )
                return False
            final_lease_reason = self._final_quote_lease_reason(
                candidate,
                quote_lease,
                submit_now_ms,
            )
            if final_lease_reason:
                self._emit_entry_dispatch_viability_blocked(
                    candidate,
                    submit_now_ms,
                    reason=final_lease_reason,
                    blocked_reasons=[final_lease_reason],
                    source="submit_quote_lease",
                    decision="skip_before_first_leg",
                    extra={
                        "candidate_revision_id": str(
                            getattr(candidate, "candidate_revision_id", "") or ""
                        ),
                        "lease_candidate_revision_id": str(
                            getattr(quote_lease, "candidate_revision_id", "") or ""
                        ),
                    },
                )
                return False
        now_ms = submit_now_ms
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
            expected_shortfall_bps_entry=0.0,
            long_quantity_metadata=long_quantity_metadata,
            short_quantity_metadata=short_quantity_metadata,
            common_base_quantity_step=common_base_quantity_step,
        )

        if self._selected_pre_submit_deadline_exceeded(
            candidate,
            selected_deadline_monotonic=selected_deadline_monotonic,
            selected_at_ms=int(selected_at_ms or now_ms),
            stage="executor_submit",
        ):
            return False

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
            quote_lease=quote_lease,
            requires_final_quote_lease=requires_final_quote_lease,
            selected_deadline_monotonic=selected_deadline_monotonic,
            selected_at_ms=int(selected_at_ms or now_ms),
            leverage_evidence_for_sizing=leverage_evidence_for_sizing,
        )
