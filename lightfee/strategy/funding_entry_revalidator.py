"""Final funding-arbitrage economics checks immediately before order submit."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Iterable

from lightfee.config.schema import StrategyConfig
from lightfee.sidecar.snapshot import CandidateInput
from lightfee.strategy.economics import EdgeBreakdown, build_edge_breakdown
from lightfee.strategy.fee_contract import derive_candidate_stage_fee_bps


@dataclass(frozen=True, slots=True)
class FundingEntryRevalidation:
    allowed: bool
    reason: str
    edge: EdgeBreakdown
    long_entry_price: float
    short_entry_price: float
    l2_entry_slippage_bps: float = 0.0


@dataclass(frozen=True, slots=True)
class HedgeOrUnwindDecision:
    action: str
    reason: str
    complete_hedge_loss_quote: float
    unwind_first_leg_loss_quote: float
    complete_hedge_price_loss_quote: float = 0.0
    unwind_first_leg_price_loss_quote: float = 0.0
    complete_hedge_fee_quote: float = 0.0
    unwind_first_leg_fee_quote: float = 0.0


@dataclass(frozen=True, slots=True)
class FirstFillMarketDecision:
    """Unified post-first-fill hedge/unwind economics from executable BBO."""

    decision: HedgeOrUnwindDecision
    hedge_price: float
    unwind_price: float
    hedge_fee_bps: float
    unwind_fee_bps: float


class FundingEntryRevalidator:
    """Reprice the same economics contract from current executable BBO/L2 data."""

    def revalidate_before_first_leg(
        self,
        candidate: CandidateInput,
        *,
        long_ask: float,
        short_bid: float,
        now_ms: int,
        config: StrategyConfig,
        long_bid: float = 0.0,
        short_ask: float = 0.0,
        long_buy_vwap: float = 0.0,
        short_sell_vwap: float = 0.0,
        required_base_quantity: float = 0.0,
        l2_vwap_complete: bool = False,
        require_l2_vwap: bool = False,
        execution_is_passive: bool | None = None,
    ) -> FundingEntryRevalidation:
        best_long_ask = float(long_ask or 0.0)
        best_short_bid = float(short_bid or 0.0)
        best_long_bid = float(long_bid or 0.0)
        best_short_ask = float(short_ask or 0.0)
        # Legacy recovery/harness adapters may still provide the former
        # candidate shape.  This compatibility boundary defaults to the
        # safer all-taker interpretation; the typed v3 runtime always carries
        # this field explicitly.
        planned_maker_leg = str(getattr(candidate, "entry_maker_leg", "") or "").lower()
        # This service is also used directly by recovery and harness adapters,
        # so it cannot rely on its runtime caller to have already parsed a
        # boolean.  A string such as ``"false"`` is truthy in Python and would
        # otherwise switch the economics into the maker path; likewise a
        # truthy L2 marker could make unverified VWAP executable evidence.
        passive_execution = (
            planned_maker_leg in {"long", "short"}
            if execution_is_passive is None
            else execution_is_passive is True
        )
        l2_complete = l2_vwap_complete is True
        if (
            best_long_ask <= 0.0
            or best_short_bid <= 0.0
            or (passive_execution and (best_long_bid <= 0.0 or best_short_ask <= 0.0))
        ):
            return FundingEntryRevalidation(
                False,
                "missing_final_executable_bbo",
                _incomplete_edge(candidate, now_ms),
                best_long_ask,
                best_short_bid,
            )
        long_taker_price = (
            float(long_buy_vwap or 0.0) if l2_complete else best_long_ask
        )
        short_taker_price = (
            float(short_sell_vwap or 0.0) if l2_complete else best_short_bid
        )
        if require_l2_vwap and (
            not l2_complete
            or required_base_quantity <= 0.0
            or long_taker_price <= 0.0
            or short_taker_price <= 0.0
        ):
            return FundingEntryRevalidation(
                False,
                "missing_final_l2_vwap",
                _incomplete_edge(candidate, now_ms),
                long_taker_price,
                short_taker_price,
            )
        if long_taker_price <= 0.0 or short_taker_price <= 0.0:
            return FundingEntryRevalidation(
                False,
                "invalid_final_l2_vwap",
                _incomplete_edge(candidate, now_ms),
                long_taker_price,
                short_taker_price,
            )
        # The final BBO and VWAP are produced from the same immutable local-L2
        # lease.  A taker buy cannot average below that lease's best ask and a
        # taker sell cannot average above its best bid.  Treat a violation as
        # corrupt/mixed-time evidence rather than crediting impossible price
        # improvement to the strategy's edge.  Maker legs do not consume their
        # corresponding taker VWAP, so validate only the legs this route would
        # actually submit as IOC/taker orders.
        long_is_taker = not passive_execution or planned_maker_leg != "long"
        short_is_taker = not passive_execution or planned_maker_leg != "short"
        if l2_complete and (
            (long_is_taker and long_taker_price < best_long_ask)
            or (short_is_taker and short_taker_price > best_short_bid)
        ):
            return FundingEntryRevalidation(
                False,
                "inconsistent_final_l2_vwap",
                _incomplete_edge(candidate, now_ms),
                long_taker_price,
                short_taker_price,
            )
        if passive_execution and planned_maker_leg == "long":
            long_price, short_price = best_long_bid, short_taker_price
            baseline_long_price, baseline_short_price = best_long_bid, best_short_bid
        elif passive_execution and planned_maker_leg == "short":
            long_price, short_price = long_taker_price, best_short_ask
            baseline_long_price, baseline_short_price = best_long_ask, best_short_ask
        else:
            long_price, short_price = long_taker_price, short_taker_price
            baseline_long_price, baseline_short_price = best_long_ask, best_short_bid
        reference = (long_price + short_price) / 2.0
        entry_cross = (short_price - long_price) / reference * 10_000.0
        bbo_reference = (baseline_long_price + baseline_short_price) / 2.0
        l2_entry_slippage_bps = (
            max(long_price - baseline_long_price, 0.0)
            + max(baseline_short_price - short_price, 0.0)
        ) / bbo_reference * 10_000.0
        entry_fee_bps, entry_fee_reason = derive_candidate_stage_fee_bps(
            candidate,
            "entry",
            maker_leg_override=planned_maker_leg if passive_execution else "",
        )
        exit_fee_bps, exit_fee_reason = derive_candidate_stage_fee_bps(
            candidate,
            "exit",
        )
        if (
            entry_fee_reason
            or exit_fee_reason
            or entry_fee_bps is None
            or exit_fee_bps is None
        ):
            return FundingEntryRevalidation(
                False,
                "invalid_final_fee_evidence:"
                + (entry_fee_reason or exit_fee_reason or "missing_derived_fee"),
                _incomplete_edge(candidate, now_ms),
                long_price,
                short_price,
                l2_entry_slippage_bps,
            )
        # When executable VWAP is used, the signed cross already contains the
        # exact book impact.  Subtracting a BBO heuristic again would double
        # charge the same entry slippage.  Without complete L2, retain the
        # conservative shortlist estimate.
        entry_slippage_bps = 0.0 if l2_complete else float(candidate.entry_slippage_bps)
        worst_funding_edge = (
            candidate.forecast_worst_funding_edge_bps
            if candidate.calculation_version == "enhanced_live"
            else candidate.funding_edge_bps
        )
        edge = _edge(
            candidate,
            entry_cross,
            candidate.funding_edge_bps,
            now_ms,
            worst_case_funding_edge_bps=worst_funding_edge,
            entry_fee_bps=entry_fee_bps,
            exit_fee_bps=exit_fee_bps,
            entry_slippage_bps=entry_slippage_bps,
        )
        if not edge.economics_complete:
            return FundingEntryRevalidation(False, "incomplete_economics", edge, long_price, short_price, l2_entry_slippage_bps)
        if edge.expected_net_edge_bps < config.min_expected_edge_bps:
            return FundingEntryRevalidation(False, "final_expected_edge_below_floor", edge, long_price, short_price, l2_entry_slippage_bps)
        if edge.worst_case_edge_bps < config.min_worst_case_edge_bps:
            return FundingEntryRevalidation(False, "final_worst_edge_below_floor", edge, long_price, short_price, l2_entry_slippage_bps)
        return FundingEntryRevalidation(True, "", edge, long_price, short_price, l2_entry_slippage_bps)

    def decide_after_first_leg(
        self,
        *,
        complete_hedge_loss_quote: float,
        unwind_first_leg_loss_quote: float,
        complete_hedge_fee_quote: float = 0.0,
        unwind_first_leg_fee_quote: float = 0.0,
    ) -> HedgeOrUnwindDecision:
        """Never abandon a filled first leg: choose the lower all-in loss path.

        The maker fill and its fee are already sunk in both branches.  What
        differs after that fill is the executable price loss *and* the fee on
        either the hedge order or the reduce-only unwind.  Comparing only the
        price terms can select the economically worse path when venues charge
        different taker rates.  Equal all-in loss resolves to ``complete`` so
        the resulting position is delta-neutral rather than briefly naked.
        """
        hedge_price_loss = _finite_nonnegative(complete_hedge_loss_quote)
        unwind_price_loss = _finite_nonnegative(unwind_first_leg_loss_quote)
        hedge_fee = _finite_nonnegative(complete_hedge_fee_quote)
        unwind_fee = _finite_nonnegative(unwind_first_leg_fee_quote)
        hedge_loss = hedge_price_loss + hedge_fee
        unwind_loss = unwind_price_loss + unwind_fee
        if hedge_loss <= unwind_loss:
            return HedgeOrUnwindDecision(
                "complete_hedge",
                "lower_expected_loss",
                hedge_loss,
                unwind_loss,
                hedge_price_loss,
                unwind_price_loss,
                hedge_fee,
                unwind_fee,
            )
        return HedgeOrUnwindDecision(
            "unwind_first_leg",
            "lower_expected_loss",
            hedge_loss,
            unwind_loss,
            hedge_price_loss,
            unwind_price_loss,
            hedge_fee,
            unwind_fee,
        )

    def decide_from_first_fill_market(
        self,
        *,
        maker_side: object,
        maker_fill_price: float,
        quantity: float,
        maker_bid: float,
        maker_ask: float,
        hedge_bid: float,
        hedge_ask: float,
        hedge_fee_bps: float | None,
        unwind_fee_bps: float | None,
    ) -> FirstFillMarketDecision | None:
        """Price the two mandatory post-fill paths from one BBO contract.

        The immediate executor and the recovered-pending lifecycle differ only
        in how they obtain a fresh book.  Once the first leg is filled, their
        hedge/unwind cash-flow arithmetic must be identical: sell a filled
        long at the hedge/maker bid, or buy back a filled short at the
        hedge/maker ask, then include the prospective taker fee for each
        branch.  Missing or malformed evidence returns ``None`` so callers
        retain the V1 conservative complete-hedge default.
        """
        try:
            maker_price = float(maker_fill_price)
            base_quantity = float(quantity)
            maker_best_bid = float(maker_bid)
            maker_best_ask = float(maker_ask)
            hedge_best_bid = float(hedge_bid)
            hedge_best_ask = float(hedge_ask)
        except (TypeError, ValueError):
            return None
        prices = (
            maker_price,
            base_quantity,
            maker_best_bid,
            maker_best_ask,
            hedge_best_bid,
            hedge_best_ask,
        )
        if not all(isfinite(value) and value > 0.0 for value in prices):
            return None
        side = str(getattr(maker_side, "value", maker_side) or "").lower()
        if side == "buy":
            hedge_price = hedge_best_bid
            unwind_price = maker_best_bid
            hedge_price_loss = max(maker_price - hedge_price, 0.0) * base_quantity
            unwind_price_loss = max(maker_price - unwind_price, 0.0) * base_quantity
        elif side == "sell":
            hedge_price = hedge_best_ask
            unwind_price = maker_best_ask
            hedge_price_loss = max(hedge_price - maker_price, 0.0) * base_quantity
            unwind_price_loss = max(unwind_price - maker_price, 0.0) * base_quantity
        else:
            return None
        if hedge_fee_bps is None or unwind_fee_bps is None:
            return None
        hedge_fee = self.taker_fee_quote(
            price=hedge_price,
            quantity=base_quantity,
            fee_bps=hedge_fee_bps,
        )
        unwind_fee = self.taker_fee_quote(
            price=unwind_price,
            quantity=base_quantity,
            fee_bps=unwind_fee_bps,
        )
        if hedge_fee is None or unwind_fee is None:
            return None
        return FirstFillMarketDecision(
            decision=self.decide_after_first_leg(
                complete_hedge_loss_quote=hedge_price_loss,
                unwind_first_leg_loss_quote=unwind_price_loss,
                complete_hedge_fee_quote=hedge_fee,
                unwind_first_leg_fee_quote=unwind_fee,
            ),
            hedge_price=hedge_price,
            unwind_price=unwind_price,
            hedge_fee_bps=float(hedge_fee_bps),
            unwind_fee_bps=float(unwind_fee_bps),
        )

    @staticmethod
    def taker_fee_bps_for_venue(
        venue: object,
        venue_configs: Iterable[object],
    ) -> float | None:
        """Return a finite configured taker rate, or no evidence.

        This deliberately lives beside the hedge/unwind decision so the
        immediate executor and the recovered-pending path cannot disagree
        about whether a fee is known.  It does not infer a rate from a maker
        fee or use an optimistic zero when the venue is absent.
        """
        target = str(getattr(venue, "value", venue) or "").lower()
        if not target:
            return None
        for config in venue_configs:
            configured = str(getattr(config, "venue", "") or "").lower()
            if configured != target:
                continue
            try:
                fee_bps = float(getattr(config, "taker_fee_bps"))
            except (TypeError, ValueError):
                return None
            return fee_bps if isfinite(fee_bps) and fee_bps >= 0.0 else None
        return None

    @staticmethod
    def taker_fee_quote(*, price: float, quantity: float, fee_bps: float) -> float | None:
        """Compute one prospective taker fee only from finite evidence."""
        try:
            price_value = float(price)
            quantity_value = float(quantity)
            fee_value = float(fee_bps)
        except (TypeError, ValueError):
            return None
        if (
            not all(isfinite(value) for value in (price_value, quantity_value, fee_value))
            or price_value <= 0.0
            or quantity_value <= 0.0
            or fee_value < 0.0
        ):
            return None
        return price_value * quantity_value * fee_value / 10_000.0


def _edge(
    candidate: CandidateInput,
    entry_cross_bps: float,
    funding_edge_bps: float,
    now_ms: int,
    worst_case_funding_edge_bps: float | None = None,
    entry_fee_bps: float | None = None,
    exit_fee_bps: float | None = None,
    entry_slippage_bps: float | None = None,
) -> EdgeBreakdown:
    return build_edge_breakdown(
        gross_signal_edge_bps=float(candidate.gross_signal_edge_bps),
        funding_edge_bps=funding_edge_bps,
        worst_case_funding_edge_bps=worst_case_funding_edge_bps,
        entry_cross_bps=entry_cross_bps,
        expected_exit_cross_bps=float(candidate.expected_exit_cross_bps),
        entry_fee_bps=(
            float(candidate.entry_fee_bps)
            if entry_fee_bps is None
            else float(entry_fee_bps)
        ),
        exit_fee_bps=(
            float(candidate.exit_fee_bps)
            if exit_fee_bps is None
            else float(exit_fee_bps)
        ),
        entry_slippage_bps=(
            float(candidate.entry_slippage_bps)
            if entry_slippage_bps is None
            else float(entry_slippage_bps)
        ),
        exit_slippage_bps=float(candidate.exit_slippage_bps),
        adverse_selection_bps=float(candidate.adverse_selection_bps),
        capital_buffer_bps=float(candidate.capital_buffer_bps),
        execution_buffer_bps=float(candidate.execution_buffer_bps),
        venue_risk_haircut_bps=float(candidate.venue_risk_haircut_bps),
        transfer_or_inventory_bias_bps=float(candidate.transfer_or_inventory_bias_bps),
        calculation_version=str(candidate.calculation_version or "v1_exact"),
        model_epoch=str(candidate.model_epoch or candidate.calculation_version or "v1_exact"),
        observed_at_ms=now_ms,
        economics_complete=candidate.economics_complete is True,
    )


def _incomplete_edge(candidate: CandidateInput, now_ms: int) -> EdgeBreakdown:
    return build_edge_breakdown(
        calculation_version=str(candidate.calculation_version or "v1_exact"),
        model_epoch=str(candidate.model_epoch or candidate.calculation_version or "v1_exact"),
        observed_at_ms=now_ms,
        economics_complete=False,
    )


def _finite_nonnegative(value: object) -> float:
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return numeric if isfinite(numeric) and numeric >= 0.0 else 0.0
