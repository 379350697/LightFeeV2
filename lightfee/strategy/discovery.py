"""Candidate discovery and ranking matching Rust strategy intelligence behavior."""

from __future__ import annotations

from enum import Enum

from lightfee.config.schema import StrategyConfig
from lightfee.sidecar.snapshot import CandidateInput


class BlockReason(Enum):
    STALE_MARKET = "stale_market"
    FUNDING_WINDOW_PASSED = "funding_window_passed"
    NO_NEAR_TERM_SETTLEMENT = "no_near_term_settlement"
    OUTSIDE_SCAN_WINDOW = "outside_scan_window"
    STAGGER_GAP_TOO_WIDE = "stagger_gap_too_wide"
    ZERO_ORDER_SIZE = "zero_order_size"
    FUNDING_EDGE_BELOW_FLOOR = "funding_edge_below_floor"
    EXPECTED_EDGE_BELOW_FLOOR = "expected_edge_below_floor"
    WORST_CASE_EDGE_BELOW_FLOOR = "worst_case_edge_below_floor"
    TRANSFER_UNAVAILABLE = "transfer_unavailable"


def discover_tradeable_candidates(
    candidates: list[CandidateInput],
    config: StrategyConfig,
    now_ms: int,
) -> list[CandidateInput]:
    """Filter and rank candidates through strategy gates. Returns tradeable list."""
    passed: list[tuple[CandidateInput, list[BlockReason]]] = []

    for c in candidates:
        if c.blocked:
            continue

        reasons: list[BlockReason] = []

        # Funding edge floor
        if c.funding_edge_bps < config.min_funding_edge_bps:
            reasons.append(BlockReason.FUNDING_EDGE_BELOW_FLOOR)

        # Expected edge floor
        if c.expected_edge_bps < config.min_expected_edge_bps:
            reasons.append(BlockReason.EXPECTED_EDGE_BELOW_FLOOR)

        # Worst-case edge floor
        if c.worst_case_edge_bps < config.min_worst_case_edge_bps:
            reasons.append(BlockReason.WORST_CASE_EDGE_BELOW_FLOOR)

        # Zero order size
        if c.entry_notional_quote <= 0:
            reasons.append(BlockReason.ZERO_ORDER_SIZE)

        if reasons:
            continue

        passed.append((c, []))

    # Sort by ranking edge (descending)
    passed.sort(key=lambda item: item[0].ranking_edge_bps, reverse=True)

    return [c for c, _ in passed[: config.max_concurrent_positions]]
