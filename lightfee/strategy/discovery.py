"""Candidate discovery and ranking matching Rust strategy intelligence behavior."""

from __future__ import annotations

from collections.abc import MutableMapping
from dataclasses import asdict
from enum import Enum
import hashlib
import json

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


def funding_entry_policy_fingerprint(config: StrategyConfig) -> str:
    """Return a stable fingerprint for the complete static entry policy.

    The full strategy payload is intentional.  Candidate economics and the
    admission floors are both strategy-derived, so a generation built under a
    different strategy must never be mistaken for the same decision universe.
    """

    payload = json.dumps(
        asdict(config),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def funding_entry_static_block_reasons(
    candidate: CandidateInput,
    config: StrategyConfig,
    now_ms: int,
    *,
    require_complete_economics: bool = False,
    include_entry_control: bool = True,
) -> tuple[str, ...]:
    """Evaluate the shared, deterministic funding-entry admission contract.

    This helper is pure: callers may cache candidate economics without a
    transient admission pass mutating the cached object.  Pair construction
    uses the same reasons to bind a complete generation, while live discovery
    repeats the contract at its current decision timestamp.
    """

    reasons: list[str] = []
    if candidate.blocked:
        reasons.extend(
            str(reason)
            for reason in (candidate.blocked_reasons or ["candidate_blocked"])
            if str(reason)
        )

    # This is deliberately an entry-only gate: it never affects pending hedge,
    # residual repair, recovery, or any close lifecycle.
    if include_entry_control and config.funding_new_entries_enabled is not True:
        reasons.append(BlockReason.FUNDING_NEW_ENTRIES_DISABLED.value)
    if require_complete_economics and (
        candidate.economics_complete is not True
        or candidate.economics_observed_at_ms <= 0
    ):
        reasons.append(BlockReason.INCOMPLETE_ECONOMICS.value)

    if candidate.first_funding_timestamp_ms <= 0:
        reasons.append(BlockReason.MISSING_CANDIDATE_IDENTITY.value)

    remaining_ms = candidate.first_funding_timestamp_ms - int(now_ms)
    if candidate.first_funding_timestamp_ms > 0 and remaining_ms <= 0:
        reasons.append(BlockReason.FUNDING_WINDOW_PASSED.value)
    elif not _is_within_funding_scan_window_ms(config, remaining_ms):
        reasons.append(BlockReason.OUTSIDE_SCAN_WINDOW.value)

    if (
        candidate.opportunity_type == "staggered"
        and candidate.first_funding_timestamp_ms > 0
        and candidate.second_funding_timestamp_ms > 0
    ):
        stagger_gap_ms = (
            candidate.second_funding_timestamp_ms
            - candidate.first_funding_timestamp_ms
        )
        max_gap_ms = config.max_stagger_gap_minutes * 60_000
        if max_gap_ms > 0 and stagger_gap_ms > max_gap_ms:
            reasons.append(BlockReason.STAGGER_GAP_TOO_WIDE.value)

    if candidate.funding_edge_bps < config.min_funding_edge_bps:
        reasons.append(BlockReason.FUNDING_EDGE_BELOW_FLOOR.value)
    if candidate.expected_edge_bps < config.min_expected_edge_bps:
        reasons.append(BlockReason.EXPECTED_EDGE_BELOW_FLOOR.value)
    if candidate.worst_case_edge_bps < config.min_worst_case_edge_bps:
        reasons.append(BlockReason.WORST_CASE_EDGE_BELOW_FLOOR.value)
    if candidate.entry_notional_quote <= 0:
        reasons.append(BlockReason.ZERO_ORDER_SIZE.value)

    # Preserve first-seen order while preventing an existing construction
    # blocker from being counted twice by the shared discovery pass.
    return tuple(dict.fromkeys(reasons))


def discover_tradeable_candidates(
    candidates: list[CandidateInput],
    config: StrategyConfig,
    now_ms: int,
    *,
    require_complete_economics: bool = False,
    blocked_reason_counts: MutableMapping[str, int] | None = None,
    blocked_reason_samples: list[dict[str, object]] | None = None,
) -> list[CandidateInput]:
    """Filter and rank candidates through strategy gates (V1 parity).

    V1: src/strategy_intelligence/discovery.rs — discover_tradeable_candidates()
    Gates applied in V1 order with stable V1 block-reason labels.
    """
    passed: list[CandidateInput] = []

    def record_blocker(reason: str, candidate: CandidateInput) -> None:
        if not reason:
            return
        if blocked_reason_counts is not None:
            blocked_reason_counts[reason] = (
                int(blocked_reason_counts.get(reason, 0)) + 1
            )
        if blocked_reason_samples is None:
            return
        long_venue = str(candidate.long_venue or "").strip().lower()
        short_venue = str(candidate.short_venue or "").strip().lower()
        symbol = str(candidate.symbol or "").strip().upper()
        pair_id = str(candidate.pair_id or "").strip()
        if not pair_id:
            pair_id = f"{symbol.lower()}:{long_venue}->{short_venue}"
        evidence = getattr(candidate, "entry_open_interest_evidence", {})
        sample_id = (
            str(evidence.get("sample_id", "") or "")
            if isinstance(evidence, dict)
            else ""
        )
        blocked_reason_samples.append(
            {
                "blocking_reason": reason,
                "long_venue": long_venue,
                "short_venue": short_venue,
                "symbol": symbol,
                "sample_id": sample_id,
                "pair_id": pair_id,
                "candidate_revision_id": str(
                    candidate.candidate_revision_id or ""
                ),
                "opportunity_lease_id": str(
                    candidate.opportunity_lease_id or ""
                ),
            }
        )

    for c in candidates:
        reasons = funding_entry_static_block_reasons(
            c,
            config,
            now_ms,
            require_complete_economics=require_complete_economics,
        )
        if reasons:
            for reason in reasons:
                record_blocker(reason, c)
            continue

        passed.append(c)

    passed.sort(key=lambda candidate: candidate.ranking_edge_bps, reverse=True)
    return passed


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
