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
    MISSING_CANDIDATE_IDENTITY = "missing_candidate_identity_or_funding_timestamp"
    FUNDING_NEW_ENTRIES_DISABLED = "funding_new_entries_disabled"
    INCOMPLETE_ECONOMICS = "incomplete_economics"


def discover_tradeable_candidates(
    candidates: list[CandidateInput],
    config: StrategyConfig,
    now_ms: int,
    *,
    require_complete_economics: bool = False,
) -> list[CandidateInput]:
    """Filter and rank candidates through strategy gates (V1 parity).

    V1: src/strategy_intelligence/discovery.rs — discover_tradeable_candidates()
    Gates applied in V1 order with stable V1 block-reason labels.
    """
    passed: list[tuple[CandidateInput, list[BlockReason]]] = []

    for c in candidates:
        if c.blocked:
            if "missing_candidate_identity_or_funding_timestamp" in c.blocked_reasons:
                continue
            continue

        reasons: list[BlockReason] = []

        # This is deliberately an entry-only gate: it never affects pending
        # hedge, residual repair, recovery or any close lifecycle.
        if config.funding_new_entries_enabled is not True:
            reasons.append(BlockReason.FUNDING_NEW_ENTRIES_DISABLED)
        if require_complete_economics and (
            c.economics_complete is not True
            or c.economics_observed_at_ms <= 0
        ):
            reasons.append(BlockReason.INCOMPLETE_ECONOMICS)

        # Identity
        if c.first_funding_timestamp_ms <= 0:
            reasons.append(BlockReason.MISSING_CANDIDATE_IDENTITY)

        # V1: funding window timing gates (discovery.rs:618-637)
        remaining_ms = c.first_funding_timestamp_ms - now_ms
        if c.first_funding_timestamp_ms > 0 and remaining_ms <= 0:
            reasons.append(BlockReason.FUNDING_WINDOW_PASSED)
        elif not _is_within_funding_scan_window_ms(config, remaining_ms):
            reasons.append(BlockReason.OUTSIDE_SCAN_WINDOW)

        # V1: stagger gap check
        if c.opportunity_type == "staggered" and c.first_funding_timestamp_ms > 0 and c.second_funding_timestamp_ms > 0:
            stagger_gap_ms = c.second_funding_timestamp_ms - c.first_funding_timestamp_ms
            max_gap_ms = config.max_stagger_gap_minutes * 60_000
            if max_gap_ms > 0 and stagger_gap_ms > max_gap_ms:
                reasons.append(BlockReason.STAGGER_GAP_TOO_WIDE)

        # Edge gates
        if c.funding_edge_bps < config.min_funding_edge_bps:
            reasons.append(BlockReason.FUNDING_EDGE_BELOW_FLOOR)
        if c.expected_edge_bps < config.min_expected_edge_bps:
            reasons.append(BlockReason.EXPECTED_EDGE_BELOW_FLOOR)
        if c.worst_case_edge_bps < config.min_worst_case_edge_bps:
            reasons.append(BlockReason.WORST_CASE_EDGE_BELOW_FLOOR)

        # Zero order size
        if c.entry_notional_quote <= 0:
            reasons.append(BlockReason.ZERO_ORDER_SIZE)

        if reasons:
            c.blocked_reasons = list(c.blocked_reasons) + [r.value for r in reasons]
            continue

        passed.append((c, []))

    passed.sort(key=lambda item: item[0].ranking_edge_bps, reverse=True)
    return [c for c, _ in passed]


def _is_within_funding_scan_window_ms(config: StrategyConfig, remaining_ms: int) -> bool:
    """V1 parity: is_within_funding_scan_window_ms (discovery.rs:997-1016).

    Returns True if remaining_ms falls within [min, max] scan window before funding.
    """
    if remaining_ms <= 0:
        return False
    max_before_ms = config.max_scan_minutes_before_funding * 60_000
    min_before_ms = config.min_scan_minutes_before_funding * 60_000
    if max_before_ms > 0:
        return remaining_ms <= max_before_ms and remaining_ms >= min_before_ms
    entry_window_ms = config.entry_window_secs * 1000
    if entry_window_ms > 0:
        return remaining_ms <= entry_window_ms and remaining_ms >= min_before_ms
    return True  # no max or entry window configured — allow all
