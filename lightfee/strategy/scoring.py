"""Edge scoring after fees, slippage, buffers, and transfer bias (Rust reference behavior)."""

from __future__ import annotations

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
