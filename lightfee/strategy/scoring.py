"""Edge scoring after fees, slippage, buffers, and transfer bias (Rust reference behavior)."""

from __future__ import annotations

from dataclasses import dataclass

from lightfee.config.schema import StrategyConfig


def compute_expected_edge_bps(
    funding_edge_bps: float,
    cross_bps: float,
    long_fee_bps: float,
    short_fee_bps: float,
    long_slippage_bps: float,
    short_slippage_bps: float,
    config: StrategyConfig,
) -> float:
    """expected_edge = funding + cross - fees - slippage - reserve - capital_buffer"""
    entry_fee = long_fee_bps + short_fee_bps
    exit_fee = long_fee_bps + short_fee_bps
    entry_slip = long_slippage_bps + short_slippage_bps
    exit_slip = long_slippage_bps + short_slippage_bps

    return (
        funding_edge_bps
        + cross_bps
        - entry_fee
        - entry_slip
        - exit_fee
        - exit_slip
        - config.entry_exit_reserve_bps
        - config.capital_buffer_bps
    )


def compute_worst_case_edge_bps(expected_edge_bps: float, config: StrategyConfig) -> float:
    """worst_case = expected_edge - execution_buffer"""
    return expected_edge_bps - config.execution_buffer_bps


def compute_ranking_edge_bps(worst_case_edge_bps: float, transfer_bias_bps: float) -> float:
    """ranking_edge = worst_case + transfer_bias"""
    return worst_case_edge_bps + transfer_bias_bps


@dataclass(frozen=True)
class FinalL2Cost:
    entry_maker_leg: str
    exit_maker_leg: str
    entry_cross_bps: float
    long_entry_slippage_bps: float
    short_entry_slippage_bps: float
    entry_fee_bps: float
    entry_slippage_bps: float
    long_exit_slippage_bps: float
    short_exit_slippage_bps: float
    exit_fee_bps: float
    exit_slippage_bps: float
    expected_edge_bps: float


def price_final_l2_cost(
    *,
    funding_edge_bps: float,
    long_bid: float,
    long_ask: float,
    short_bid: float,
    short_ask: float,
    long_buy_slippage_bps: float,
    long_sell_slippage_bps: float,
    short_buy_slippage_bps: float,
    short_sell_slippage_bps: float,
    long_maker_fee_bps: float,
    long_taker_fee_bps: float,
    short_maker_fee_bps: float,
    short_taker_fee_bps: float,
    config: StrategyConfig,
    tie_maker_leg: str = "long",
    forced_entry_maker_leg: str | None = None,
) -> FinalL2Cost:
    """Price ready-L2 execution with role-aware fees and one-maker routes."""
    reference_mid = (long_ask + short_bid) / 2.0
    if reference_mid <= 0.0:
        raise ValueError("invalid_l2_reference_mid")
    tie_maker_leg = "short" if str(tie_maker_leg).lower() == "short" else "long"

    def entry_route(maker_leg: str) -> tuple[float, float, float]:
        raw_cross = (short_bid - long_ask) / reference_mid * 10_000.0
        if maker_leg == "long":
            return (
                raw_cross + (long_ask - long_bid) / reference_mid * 10_000.0,
                long_maker_fee_bps + short_taker_fee_bps,
                short_sell_slippage_bps,
            )
        return (
            raw_cross + (short_ask - short_bid) / reference_mid * 10_000.0,
            long_taker_fee_bps + short_maker_fee_bps,
            long_buy_slippage_bps,
        )

    def exit_route(maker_leg: str) -> tuple[float, float, float, float]:
        if maker_leg == "long":
            return (
                long_maker_fee_bps + short_taker_fee_bps,
                short_buy_slippage_bps,
                0.0,
                short_buy_slippage_bps,
            )
        return (
            long_taker_fee_bps + short_maker_fee_bps,
            long_sell_slippage_bps,
            long_sell_slippage_bps,
            0.0,
        )

    long_exit_fee, long_exit_slippage, long_exit_long_slip, long_exit_short_slip = exit_route("long")
    short_exit_fee, short_exit_slippage, short_exit_long_slip, short_exit_short_slip = exit_route("short")
    # V1 selects the passive exit leg from the taker leg's L2 slippage.  The
    # account-specific fee is then applied to that chosen role; it must not
    # silently introduce a different exit-routing policy.
    if long_exit_slippage < short_exit_slippage:
        exit_maker_leg, exit_fee, exit_slippage = "long", long_exit_fee, long_exit_slippage
        long_exit_slip, short_exit_slip = long_exit_long_slip, long_exit_short_slip
    elif short_exit_slippage < long_exit_slippage:
        exit_maker_leg, exit_fee, exit_slippage = "short", short_exit_fee, short_exit_slippage
        long_exit_slip, short_exit_slip = short_exit_long_slip, short_exit_short_slip
    elif tie_maker_leg == "short":
        exit_maker_leg, exit_fee, exit_slippage = "short", short_exit_fee, short_exit_slippage
        long_exit_slip, short_exit_slip = short_exit_long_slip, short_exit_short_slip
    else:
        exit_maker_leg, exit_fee, exit_slippage = "long", long_exit_fee, long_exit_slippage
        long_exit_slip, short_exit_slip = long_exit_long_slip, long_exit_short_slip

    routes = ("long", "short") if forced_entry_maker_leg is None else (str(forced_entry_maker_leg),)
    priced: list[FinalL2Cost] = []
    for maker_leg in routes:
        if maker_leg not in {"long", "short"}:
            raise ValueError("invalid_entry_maker_leg")
        entry_cross, entry_fee, entry_slippage = entry_route(maker_leg)
        long_entry_slip = 0.0 if maker_leg == "long" else long_buy_slippage_bps
        short_entry_slip = short_sell_slippage_bps if maker_leg == "long" else 0.0
        expected = (
            funding_edge_bps + entry_cross - entry_fee - entry_slippage
            - exit_fee - exit_slippage - config.entry_exit_reserve_bps
            - config.capital_buffer_bps
        )
        priced.append(FinalL2Cost(
            entry_maker_leg=maker_leg,
            exit_maker_leg=exit_maker_leg,
            entry_cross_bps=entry_cross,
            long_entry_slippage_bps=long_entry_slip,
            short_entry_slippage_bps=short_entry_slip,
            entry_fee_bps=entry_fee,
            entry_slippage_bps=entry_slippage,
            long_exit_slippage_bps=long_exit_slip,
            short_exit_slippage_bps=short_exit_slip,
            exit_fee_bps=exit_fee,
            exit_slippage_bps=exit_slippage,
            expected_edge_bps=expected,
        ))
    if len(priced) == 1:
        return priced[0]
    long_route, short_route = priced
    if long_route.expected_edge_bps > short_route.expected_edge_bps:
        return long_route
    if short_route.expected_edge_bps > long_route.expected_edge_bps:
        return short_route
    return short_route if tie_maker_leg == "short" else long_route
