"""Fail-closed promotion analysis for the live funding canary cohort.

Promotion is deliberately stricter than ordinary offline attribution.  It
accepts only one immutable, sequenced canary cohort; it never fills in missing
economics, price benchmarks, exchange truth or reconciliation facts from a
later configuration or an inferred PnL value.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import hmac
import json
from math import ceil, isfinite
import os
from typing import Iterable, Mapping

from lightfee.engine.exit import (
    EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS,
    execution_benchmark_receipt_integrity_verified,
    execution_benchmark_receipt_max_fill_at_ms,
    execution_benchmark_receipt_semantically_verified,
)
from lightfee.strategy.funding_canary_policy import canonical_venue_pair


_TERMINAL_KINDS = frozenset(
    {
        "exit.closed",
        "exit.passive_close_resolved",
        "runtime.position_lifecycle_terminal",
    }
)
_RECONCILED_KIND = "funding.settlement_reconciled"
_OPENED_KIND = "entry.opened"
_SELECTED_KIND = "execution.entry_selected"
_FINALIZED_KIND = "pending_entry.pending_entry_finalized"
_RELEVANT_KINDS = frozenset(
    {
        _SELECTED_KIND,
        _FINALIZED_KIND,
        _OPENED_KIND,
        _RECONCILED_KIND,
        *_TERMINAL_KINDS,
    }
)
_LEGACY_POLICY_VERSION = "funding-canary-v1"
_POLICY_VERSION = "funding-canary-v2"
_APPROVED_POLICY_SCHEMA_VERSION = 1
_APPROVED_POLICY_HMAC_ENV = "LIGHTFEE_CANARY_POLICY_HMAC_KEY"
_HARD_MAX_ENTRY_NOTIONAL_QUOTE = 30.0
_HARD_MIN_EXPECTED_NET_EDGE_BPS = 8.0
_HARD_MIN_WORST_CASE_EDGE_BPS = 3.0
_MIN_REQUIRED_CLOSED_LOOPS = 30
_POST_CLOSE_FUNDING_TRUTH_REASON = "post_close_exchange_truth_for_funding_reconciliation"


def _relevant_event_owner_is_complete(kind: str, payload: Mapping[str, object]) -> bool:
    """Require each promotion event to identify the lifecycle object it owns.

    A record without an owner used to be silently ignored after collection,
    which let a damaged ``exit.closed`` or terminal-truth row coexist with a
    seemingly complete loop.  The report is a safety gate, so malformed
    relevant evidence poisons the cohort instead of being treated as an
    unrelated diagnostic event.
    """
    entry_id = _text(payload.get("entry_id"))
    position_id = _text(payload.get("position_id"))
    symbol = _text(payload.get("symbol"))
    long_venue = _text(payload.get("long_venue"))
    short_venue = _text(payload.get("short_venue"))
    if kind == _SELECTED_KIND:
        return bool(entry_id)
    if kind == _FINALIZED_KIND:
        return bool(entry_id and position_id)
    if kind == _OPENED_KIND:
        return bool(position_id)
    if kind == "exit.closed":
        return bool(position_id and symbol and long_venue and short_venue)
    if kind == "runtime.position_lifecycle_terminal":
        return bool(position_id and symbol)
    if kind == "exit.passive_close_resolved":
        return bool(position_id)
    if kind == _RECONCILED_KIND:
        return bool(position_id and symbol)
    return False


@dataclass(frozen=True)
class FundingCanaryLoop:
    entry_id: str
    position_id: str = ""
    planned_entry_notional_quote: float = 0.0
    actual_entry_notional_quote: float = 0.0
    actual_entry_max_leg_notional_quote: float = 0.0
    actual_execution_cost_bps: float | None = None
    budgeted_execution_cost_bps: float | None = None
    execution_reserve_bps: float | None = None
    worst_case_buffer_bps: float = 0.0
    official_funding_reconciled: bool = False
    terminal_truth_flat: bool = False
    contract_valid: bool = False
    contract_reason: str = ""
    status: str = "selected_not_opened"


@dataclass
class FundingCanaryReport:
    required_closed_loops: int = _MIN_REQUIRED_CLOSED_LOOPS
    source_evidence_verified: bool = False
    selected_count: int = 0
    ambiguous_canary_selection_count: int = 0
    ambiguous_event_count: int = 0
    missing_event_sequence_count: int = 0
    cross_run_lifecycle_evidence_count: int = 0
    mixed_cohort_count: int = 0
    cohort_ids: list[str] = field(default_factory=list)
    invalid_canary_contract_count: int = 0
    opened_count: int = 0
    complete_loop_count: int = 0
    official_reconciled_count: int = 0
    missing_official_reconciliation_count: int = 0
    selected_not_opened_count: int = 0
    opened_unclosed_count: int = 0
    missing_actual_cost_count: int = 0
    missing_terminal_truth_count: int = 0
    invalid_lifecycle_evidence_count: int = 0
    malformed_relevant_event_count: int = 0
    over_notional_count: int = 0
    execution_cost_budget_breach_count: int = 0
    # Kept as a diagnostic for reports produced before the contract change.
    # It must never be used as an execution-cost gate.
    buffer_breach_count: int = 0
    p95_actual_execution_cost_bps: float | None = None
    p05_allowed_execution_cost_bps: float | None = None
    p05_worst_case_buffer_bps: float | None = None
    promotion_ready: bool = False
    promotion_blockers: list[str] = field(default_factory=list)
    loops: list[FundingCanaryLoop] = field(default_factory=list)


def analyze_funding_canary_events(
    records: Iterable[Mapping[str, object]],
    *,
    required_closed_loops: int = _MIN_REQUIRED_CLOSED_LOOPS,
    source_evidence_verified: bool = False,
    approved_policy: Mapping[str, object] | None = None,
) -> FundingCanaryReport:
    """Evaluate a canary cohort without filling gaps from assumptions.

    A promotable loop has one sequenced selected/opened/finalized/reconciled
    chain, final planned and actual per-leg caps, immutable 8/3 economics,
    explicit fee/benchmark cost evidence, terminal exchange truth and a
    private-statement funding receipt.  Any replay or conflicting lifecycle
    record blocks the whole promotion rather than relying on input order.
    """

    report = FundingCanaryReport(
        required_closed_loops=max(_integer(required_closed_loops) or 0, _MIN_REQUIRED_CLOSED_LOOPS),
        source_evidence_verified=source_evidence_verified is True,
    )
    selected: dict[tuple[str, str], dict[str, object]] = {}
    finalized: dict[tuple[str, str], dict[str, object]] = {}
    opened: dict[tuple[str, str], dict[str, object]] = {}
    terminal_events: dict[tuple[str, str], list[dict[str, object]]] = {}
    reconciled: dict[tuple[str, str], dict[str, object]] = {}
    entry_to_position: dict[tuple[str, str], str] = {}
    seen_event_ids: set[tuple[str, int]] = set()
    ordered_records: list[tuple[str, int, int, int, str, dict[str, object]]] = []

    for ordinal, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        kind = _text(record.get("kind"))
        if kind not in _RELEVANT_KINDS:
            continue
        payload = record.get("payload")
        run_id = _text(record.get("run_id"))
        seq = _positive_int(record.get("seq"))
        journal_at_ms = _positive_int(record.get("ts_ms"))
        if not run_id or seq is None:
            report.missing_event_sequence_count += 1
        if (
            not isinstance(payload, Mapping)
            or not run_id
            or seq is None
            or journal_at_ms is None
        ):
            # A selected/opened/closed/truth/reconciliation row is promotion
            # evidence.  Dropping a damaged row would splice a false complete
            # lifecycle from its neighbours, so it blocks the whole cohort.
            report.malformed_relevant_event_count += 1
            continue
        data = dict(payload)
        # These events form the only ownership chain accepted for promotion.
        # Do this before filtering paper selections or joining positions: a
        # malformed owner record must not disappear merely because it cannot
        # be joined to a selected canary loop.
        if not _relevant_event_owner_is_complete(kind, data):
            report.malformed_relevant_event_count += 1
            continue
        # Explicit paper selected events are intentionally ignored.  All
        # candidate live evidence must be journal-sequenced and bound to a run.
        is_paper_selected = (
            kind == _SELECTED_KIND
            and data.get("funding_canary") is True
            and _text(data.get("runtime_mode")).lower() == "paper"
        )
        if is_paper_selected:
            continue
        assert seq is not None
        assert journal_at_ms is not None
        event_id = (run_id, seq)
        if event_id in seen_event_ids:
            report.ambiguous_event_count += 1
            continue
        seen_event_ids.add(event_id)
        ordered_records.append((run_id, seq, journal_at_ms, ordinal, kind, data))

    for run_id, seq, journal_at_ms, _, kind, data in sorted(ordered_records):
        # Preserve journal order as evidence.  The input iterable may be
        # reordered by a report/export path, so ordinal is never used to infer
        # lifecycle order.
        data = {
            **data,
            "_canary_journal_kind": kind,
            "_canary_journal_seq": seq,
            "_canary_journal_at_ms": journal_at_ms,
        }
        if kind == _SELECTED_KIND:
            if data.get("funding_canary") is not True:
                continue
            entry_id = _text(data.get("entry_id"))
            runtime_mode = _text(data.get("runtime_mode")).lower()
            if runtime_mode == "live" and entry_id:
                selected_key = (run_id, entry_id)
                if selected_key in selected:
                    report.ambiguous_canary_selection_count += 1
                    report.ambiguous_event_count += 1
                else:
                    selected[selected_key] = data
            elif runtime_mode != "paper":
                report.ambiguous_canary_selection_count += 1
            continue
        if kind == _FINALIZED_KIND:
            entry_id = _text(data.get("entry_id"))
            position_id = _text(data.get("position_id"))
            if entry_id:
                entry_key = (run_id, entry_id)
                if entry_key in finalized:
                    report.ambiguous_event_count += 1
                else:
                    finalized[entry_key] = data
                if position_id:
                    prior = entry_to_position.get(entry_key)
                    if prior is not None:
                        report.ambiguous_event_count += 1
                    else:
                        entry_to_position[entry_key] = position_id
            continue
        position_id = _text(data.get("position_id") or data.get("internal_entry_id"))
        if not position_id:
            continue
        position_key = (run_id, position_id)
        if kind == _OPENED_KIND:
            if position_key in opened:
                report.ambiguous_event_count += 1
            else:
                opened[position_key] = data
        elif kind in _TERMINAL_KINDS:
            terminal_events.setdefault(position_key, []).append(data)
        elif kind == _RECONCILED_KIND:
            if position_key in reconciled:
                report.ambiguous_event_count += 1
            else:
                reconciled[position_key] = data

    cohort_ids = sorted(
        {
            _text(payload.get("funding_canary_cohort_id"))
            for payload in selected.values()
            if _text(payload.get("funding_canary_cohort_id"))
        }
    )
    report.cohort_ids = cohort_ids
    report.mixed_cohort_count = max(len(cohort_ids) - 1, 0)

    for (run_id, entry_id), selected_payload in selected.items():
        position_id = entry_to_position.get((run_id, entry_id), entry_id)
        position_key = (run_id, position_id)
        contract_reason = _selection_contract_reason(
            selected_payload,
            approved_policy=approved_policy,
        )
        contract_valid = not contract_reason
        if not contract_valid:
            report.invalid_canary_contract_count += 1
        opened_payload = opened.get(position_key)
        position_terminal_events = terminal_events.get(position_key, [])
        settlement_payload = reconciled.get(position_key)
        if (
            any(key[1] == position_id and key[0] != run_id for key in opened)
            or any(key[1] == position_id and key[0] != run_id for key in terminal_events)
            or any(key[1] == position_id and key[0] != run_id for key in reconciled)
        ):
            # A position may only be joined to the same journal writer run.
            # Older records without a propagated chain token are diagnostic
            # evidence, not safe promotion evidence after a process restart.
            report.cross_run_lifecycle_evidence_count += 1
        planned_notional = _planned_max_leg_notional(selected_payload)
        worst_buffer = _worst_case_buffer_bps(selected_payload)
        if opened_payload is None:
            report.selected_not_opened_count += 1
            report.loops.append(
                FundingCanaryLoop(
                    entry_id=entry_id,
                    position_id=position_id,
                    planned_entry_notional_quote=planned_notional,
                    worst_case_buffer_bps=worst_buffer,
                    contract_valid=contract_valid,
                    contract_reason=contract_reason,
                )
            )
            continue
        report.opened_count += 1
        actual_notional = _entry_notional(opened_payload)
        selected_notional_cap = _finite_required(
            selected_payload.get("funding_canary_hard_max_entry_notional_quote")
        )
        if (
            actual_notional <= 0.0
            or selected_notional_cap is None
            or actual_notional > selected_notional_cap + 1e-12
        ):
            report.over_notional_count += 1
        if not position_terminal_events:
            report.opened_unclosed_count += 1
            report.loops.append(
                FundingCanaryLoop(
                    entry_id=entry_id,
                    position_id=position_id,
                    planned_entry_notional_quote=planned_notional,
                    actual_entry_notional_quote=actual_notional,
                    actual_entry_max_leg_notional_quote=actual_notional,
                    worst_case_buffer_bps=worst_buffer,
                    contract_valid=contract_valid,
                    contract_reason=contract_reason,
                    status="opened_unclosed",
                )
            )
            continue

        cost, terminal_ambiguous = _actual_execution_cost_from_terminal_events(
            opened_payload, position_terminal_events
        )
        if terminal_ambiguous:
            report.ambiguous_event_count += 1
        truth_flat, lifecycle_evidence_valid = _terminal_truth_flat_from_events(
            position_terminal_events,
            settlement_payload,
            selected=selected_payload,
            opened=opened_payload,
            finalized=finalized.get((run_id, entry_id)),
        )
        official = _official_reconciliation_complete(settlement_payload, position_id=position_id)
        budget, reserve = _selection_execution_cost_budget(selected_payload)
        if cost is None:
            report.missing_actual_cost_count += 1
        if not truth_flat:
            report.missing_terminal_truth_count += 1
        if not lifecycle_evidence_valid:
            report.invalid_lifecycle_evidence_count += 1
        if official:
            report.official_reconciled_count += 1
        else:
            report.missing_official_reconciliation_count += 1
        budget_breached = (
            cost is not None
            and budget is not None
            and reserve is not None
            and cost > budget + reserve + 1e-12
        )
        if budget_breached:
            report.execution_cost_budget_breach_count += 1
        complete = bool(
            contract_valid
            and actual_notional > 0.0
            and selected_notional_cap is not None
            and actual_notional <= selected_notional_cap + 1e-12
            and official
            and truth_flat
            and cost is not None
            and budget is not None
            and reserve is not None
            and not terminal_ambiguous
            and lifecycle_evidence_valid
        )
        if complete:
            report.complete_loop_count += 1
        report.loops.append(
            FundingCanaryLoop(
                entry_id=entry_id,
                position_id=position_id,
                planned_entry_notional_quote=planned_notional,
                actual_entry_notional_quote=actual_notional,
                actual_entry_max_leg_notional_quote=actual_notional,
                actual_execution_cost_bps=cost,
                budgeted_execution_cost_bps=budget,
                execution_reserve_bps=reserve,
                worst_case_buffer_bps=worst_buffer,
                official_funding_reconciled=official,
                terminal_truth_flat=truth_flat,
                contract_valid=contract_valid,
                contract_reason=contract_reason,
                status="complete" if complete else "terminal_incomplete",
            )
        )

    report.selected_count = len(selected)
    complete_costs = [
        loop.actual_execution_cost_bps
        for loop in report.loops
        if loop.status == "complete" and loop.actual_execution_cost_bps is not None
    ]
    complete_allowed_costs = [
        loop.budgeted_execution_cost_bps + loop.execution_reserve_bps
        for loop in report.loops
        if loop.status == "complete"
        and loop.budgeted_execution_cost_bps is not None
        and loop.execution_reserve_bps is not None
    ]
    complete_buffers = [
        loop.worst_case_buffer_bps for loop in report.loops if loop.status == "complete"
    ]
    report.p95_actual_execution_cost_bps = _percentile(complete_costs, 0.95)
    report.p05_allowed_execution_cost_bps = _percentile(complete_allowed_costs, 0.05)
    report.p05_worst_case_buffer_bps = _percentile(complete_buffers, 0.05)
    report.promotion_blockers = _promotion_blockers(report)
    report.promotion_ready = not report.promotion_blockers
    return report


def funding_canary_report_dict(report: FundingCanaryReport) -> dict[str, object]:
    """Return JSON-safe report output, including each immutable loop row."""
    payload = asdict(report)
    payload["loops"] = [asdict(loop) for loop in report.loops]
    return payload


def _promotion_blockers(report: FundingCanaryReport) -> list[str]:
    blockers: list[str] = []
    if report.source_evidence_verified is not True:
        blockers.append("acceptance_source_evidence_unverified")
    if report.complete_loop_count < report.required_closed_loops:
        blockers.append("insufficient_complete_reconciled_truth_flat_loops")
    if report.selected_not_opened_count:
        blockers.append("selected_canaries_without_opened_position")
    if report.opened_unclosed_count:
        blockers.append("opened_canaries_not_terminal")
    if report.missing_actual_cost_count:
        blockers.append("terminal_canaries_missing_actual_cost")
    if report.missing_terminal_truth_count:
        blockers.append("terminal_canaries_missing_exchange_truth_flat")
    if report.invalid_lifecycle_evidence_count:
        blockers.append("canary_lifecycle_evidence_not_ordered_and_scoped")
    if report.missing_official_reconciliation_count:
        blockers.append("terminal_canaries_missing_official_funding_reconciliation")
    if report.invalid_canary_contract_count:
        blockers.append("acceptance_canary_contract_invalid")
    if report.over_notional_count:
        blockers.append("actual_opened_notional_exceeds_canary_cap")
    if report.execution_cost_budget_breach_count:
        blockers.append("actual_execution_cost_exceeds_budget_and_reserve")
    if report.ambiguous_canary_selection_count:
        blockers.append("canary_selection_runtime_mode_not_live_or_missing")
    if report.ambiguous_event_count:
        blockers.append("acceptance_event_ambiguity")
    if report.missing_event_sequence_count:
        blockers.append("acceptance_event_sequence_missing")
    if report.malformed_relevant_event_count:
        blockers.append("acceptance_relevant_event_malformed")
    if report.cross_run_lifecycle_evidence_count:
        blockers.append("cross_run_lifecycle_evidence_not_promotable")
    if report.mixed_cohort_count or len(report.cohort_ids) != 1:
        blockers.append("mixed_or_missing_funding_canary_cohort")
    if (
        report.p95_actual_execution_cost_bps is None
        or report.p05_allowed_execution_cost_bps is None
        or report.p95_actual_execution_cost_bps > report.p05_allowed_execution_cost_bps + 1e-12
    ):
        blockers.append("p95_actual_execution_cost_exceeds_budget_and_reserve")
    return blockers


def sign_funding_canary_approved_policy(
    policy: Mapping[str, object],
) -> dict[str, object]:
    """Seal an operator-approved v2 promotion policy with a separate HMAC key."""

    material = _normalized_approved_v2_policy(policy)
    if material is None:
        raise ValueError("invalid funding canary approved policy")
    key = os.environ.get(_APPROVED_POLICY_HMAC_ENV, "").encode("utf-8")
    if not key:
        raise ValueError(f"{_APPROVED_POLICY_HMAC_ENV} must be non-empty")
    signature = hmac.new(
        key,
        _approved_policy_bytes(material),
        hashlib.sha256,
    ).hexdigest()
    return {**material, "signature": signature}


def _approved_v2_policy_reason(
    selected: Mapping[str, object],
    approved_policy: Mapping[str, object] | None,
) -> str:
    if not isinstance(approved_policy, Mapping):
        return "approved_v2_policy_missing"
    material = _normalized_approved_v2_policy(approved_policy)
    signature = str(approved_policy.get("signature") or "").strip().lower()
    key = os.environ.get(_APPROVED_POLICY_HMAC_ENV, "").encode("utf-8")
    if material is None or not key or len(signature) != 64:
        return "approved_v2_policy_unverified"
    expected_signature = hmac.new(
        key,
        _approved_policy_bytes(material),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        return "approved_v2_policy_unverified"
    if material["cohort_id"] != _text(selected.get("funding_canary_cohort_id")):
        return "approved_v2_policy_cohort_mismatch"
    selected_values = (
        _finite_required(
            selected.get("funding_canary_hard_max_entry_notional_quote")
        ),
        _finite_required(
            selected.get("funding_canary_hard_min_expected_net_edge_bps")
        ),
        _finite_required(
            selected.get("funding_canary_hard_min_worst_case_edge_bps")
        ),
    )
    approved_values = (
        float(material["max_entry_notional_quote"]),
        float(material["min_expected_net_edge_bps"]),
        float(material["min_worst_case_edge_bps"]),
    )
    if any(value is None for value in selected_values) or any(
        abs(float(selected_value) - approved_value) > 1e-12
        for selected_value, approved_value in zip(
            selected_values, approved_values, strict=True
        )
    ):
        return "approved_v2_policy_limits_mismatch"
    pair = canonical_venue_pair(
        selected.get("long_venue"), selected.get("short_venue")
    )
    if pair not in material["allowed_venue_pairs"]:
        return "approved_v2_policy_venue_pair_not_allowed"
    return ""


def _normalized_approved_v2_policy(
    policy: Mapping[str, object],
) -> dict[str, object] | None:
    if (
        _integer(policy.get("schema_version")) != _APPROVED_POLICY_SCHEMA_VERSION
        or _text(policy.get("policy_version")) != _POLICY_VERSION
        or not _text(policy.get("cohort_id"))
    ):
        return None
    max_notional = _finite_required(policy.get("max_entry_notional_quote"))
    min_expected = _finite_required(policy.get("min_expected_net_edge_bps"))
    min_worst = _finite_required(policy.get("min_worst_case_edge_bps"))
    raw_pairs = policy.get("allowed_venue_pairs")
    if (
        max_notional is None
        or max_notional <= 0.0
        or min_expected is None
        or min_expected < 0.0
        or min_worst is None
        or min_worst < 0.0
        or not isinstance(raw_pairs, list)
    ):
        return None
    pairs: set[str] = set()
    for raw_pair in raw_pairs:
        parts = str(raw_pair or "").strip().lower().replace("|", ":").split(":")
        pair = canonical_venue_pair(*parts) if len(parts) == 2 else ""
        if not pair:
            return None
        pairs.add(pair)
    if not pairs:
        return None
    return {
        "schema_version": _APPROVED_POLICY_SCHEMA_VERSION,
        "policy_version": _POLICY_VERSION,
        "cohort_id": _text(policy.get("cohort_id")),
        "max_entry_notional_quote": max_notional,
        "min_expected_net_edge_bps": min_expected,
        "min_worst_case_edge_bps": min_worst,
        "allowed_venue_pairs": sorted(pairs),
    }


def _approved_policy_bytes(material: Mapping[str, object]) -> bytes:
    return json.dumps(
        dict(material),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _selection_contract_reason(
    selected: Mapping[str, object],
    *,
    approved_policy: Mapping[str, object] | None = None,
) -> str:
    """Validate the immutable selected-marker contract, never current config."""

    policy_version = _text(selected.get("funding_canary_policy_version"))
    if policy_version not in {_LEGACY_POLICY_VERSION, _POLICY_VERSION}:
        return "policy_version_missing_or_mismatched"
    if not _text(selected.get("funding_canary_cohort_id")):
        return "cohort_id_missing"
    if selected.get("economics_complete") is not True:
        return "economics_incomplete"
    assurance_tier = _text(
        selected.get("funding_canary_fee_assurance_tier")
    ) or ("account" if policy_version == _LEGACY_POLICY_VERSION else "")
    # Conservative-tier entries are legitimate bounded discovery samples, but
    # cannot graduate a release cohort without account-bound fee evidence.
    if assurance_tier != "account":
        return "account_fee_assurance_required_for_promotion"
    if policy_version == _POLICY_VERSION:
        approval_reason = _approved_v2_policy_reason(selected, approved_policy)
        if approval_reason:
            return approval_reason
    if selected.get("account_fee_evidence_complete") is not True:
        return "account_fee_evidence_incomplete"
    if selected.get("account_fee_evidence_integrity_verified") is not True:
        return "account_fee_evidence_unverified"
    if selected.get("account_fee_evidence_identity_bound") is not True:
        return "account_fee_identity_not_bound"
    hard_cap = _finite_required(selected.get("funding_canary_hard_max_entry_notional_quote"))
    hard_expected = _finite_required(
        selected.get("funding_canary_hard_min_expected_net_edge_bps")
    )
    hard_worst = _finite_required(
        selected.get("funding_canary_hard_min_worst_case_edge_bps")
    )
    expected = _finite_required(selected.get("expected_net_edge_bps"))
    worst = _finite_required(selected.get("worst_case_edge_bps"))
    planned = _planned_max_leg_notional(selected)
    budget, reserve = _selection_execution_cost_budget(selected)
    if (
        hard_cap is None
        or hard_expected is None
        or hard_worst is None
        or expected is None
        or worst is None
        or budget is None
        or reserve is None
    ):
        return "required_numeric_evidence_missing"
    if hard_cap <= 0.0:
        return "hard_notional_cap_invalid"
    if hard_expected < 0.0:
        return "hard_expected_floor_weakened"
    if hard_worst < 0.0:
        return "hard_worst_floor_weakened"
    if policy_version == _LEGACY_POLICY_VERSION and (
        hard_cap > _HARD_MAX_ENTRY_NOTIONAL_QUOTE + 1e-12
        or hard_expected < _HARD_MIN_EXPECTED_NET_EDGE_BPS - 1e-12
        or hard_worst < _HARD_MIN_WORST_CASE_EDGE_BPS - 1e-12
    ):
        return "legacy_hard_release_contract_weakened"
    if expected < hard_expected - 1e-12:
        return "expected_edge_below_hard_floor"
    if worst < hard_worst - 1e-12:
        return "worst_edge_below_hard_floor"
    if planned <= 0.0 or planned > hard_cap + 1e-12:
        return "planned_entry_notional_exceeds_hard_cap"
    return ""


def _selection_execution_cost_budget(
    selected: Mapping[str, object],
) -> tuple[float | None, float | None]:
    budget = _finite_required(selected.get("canary_budgeted_execution_cost_bps"))
    reserve = _finite_required(selected.get("canary_execution_reserve_bps"))
    if budget is None or reserve is None or budget < 0.0 or reserve < 0.0:
        return None, None
    return budget, reserve


def _actual_execution_cost_bps(
    opened: Mapping[str, object], terminal: Mapping[str, object]
) -> float | None:
    # A numeric zero only has cost-evidence meaning when both entry and exit
    # were benchmarked.  Otherwise an unavailable benchmark could be silently
    # serialized as ``0.0`` and falsely improve the canary result.
    if terminal.get("execution_benchmark_complete") is not True:
        return None
    if terminal.get("execution_fee_complete") is not True:
        return None
    if not _terminal_execution_benchmark_receipts_complete(opened, terminal):
        return None
    notional = _entry_notional(opened)
    entry_fee = _finite_required(terminal.get("entry_fee_quote"))
    exit_fee = _finite_required(terminal.get("exit_fee_quote"))
    # Price PnL is holding/basis risk.  The third component must be a measured
    # shortfall versus immutable contemporaneous execution benchmarks; it is
    # not inferred from favourable or adverse PnL after the fact.
    implementation_shortfall = _finite_required(
        terminal.get("implementation_shortfall_quote")
    )
    if (
        notional <= 0.0
        or None in {entry_fee, exit_fee, implementation_shortfall}
        or float(implementation_shortfall) < 0.0
    ):
        return None
    return (
        (float(entry_fee) + float(exit_fee) + float(implementation_shortfall))
        * 10_000.0
        / notional
    )


def _terminal_execution_benchmark_receipts_complete(
    opened: Mapping[str, object],
    terminal: Mapping[str, object],
) -> bool:
    """Verify that a terminal cost aggregate is backed by signed raw receipts.

    The close runtime persists the raw, side-aware L2 receipts alongside the
    terminal aggregate.  Accepting only the aggregate here would turn a
    tampered or synthetic journal row into promotion evidence even though the
    trusted close path deliberately HMAC-seals the underlying receipts.  This
    is a read-only admission check: it neither changes close behaviour nor
    attempts to recreate missing measurements.
    """
    position_id = _text(terminal.get("position_id"))
    symbol = _text(terminal.get("symbol"))
    long_venue = _text(terminal.get("long_venue"))
    short_venue = _text(terminal.get("short_venue"))
    reported_shortfall = _finite_required(
        terminal.get("implementation_shortfall_quote")
    )
    entry_receipt = terminal.get("entry_execution_benchmark_receipt")
    receipts = terminal.get("exit_execution_benchmark_receipts")
    if (
        not position_id
        or not symbol
        or not long_venue
        or not short_venue
        or reported_shortfall is None
        or reported_shortfall < 0.0
        or not isinstance(entry_receipt, dict)
        or not isinstance(receipts, list)
        or not receipts
    ):
        return False

    if not execution_benchmark_receipt_semantically_verified(
        entry_receipt,
        position_id=position_id,
        symbol=symbol,
        expected_legs={
            "long": (long_venue, "buy"),
            "short": (short_venue, "sell"),
        },
        require_fee_observations=True,
    ) or not _execution_benchmark_receipt_is_timely(entry_receipt):
        return False
    entry_quantity = _finite_required(entry_receipt.get("requested_base_quantity"))
    opened_quantity = _opened_matched_base_quantity(opened)
    if (
        entry_quantity is None
        or opened_quantity is None
        or not _execution_benchmark_quantities_match(entry_quantity, opened_quantity)
    ):
        # A signed receipt for a smaller entry is not a harmless diagnostic
        # mismatch: treating it as complete would let the unbenchmarked part
        # of an actually opened pair disappear from the cost denominator.
        return False
    entry_shortfall = _finite_required(
        entry_receipt.get("implementation_shortfall_quote")
    )
    if entry_shortfall is None or entry_shortfall < 0.0:
        return False

    entry_receipt_fee = _execution_benchmark_receipt_fee_quote(entry_receipt)
    if entry_receipt_fee is None:
        return False
    receipt_shortfall = entry_shortfall
    receipt_fee_quote = entry_receipt_fee
    exit_quantity = 0.0
    for receipt in receipts:
        if (
            not isinstance(receipt, dict)
            or receipt.get("source") != "local_l2_vwap"
            or _text(receipt.get("position_id")) != position_id
            or not execution_benchmark_receipt_integrity_verified(receipt)
            or not execution_benchmark_receipt_semantically_verified(
                receipt,
                position_id=position_id,
                symbol=symbol,
                expected_legs={
                    "long": (long_venue, "sell"),
                    "short": (short_venue, "buy"),
                },
                require_fee_observations=True,
            )
            or not _execution_benchmark_receipt_is_timely(receipt)
        ):
            return False
        shortfall = _finite_required(receipt.get("implementation_shortfall_quote"))
        receipt_quantity = _finite_required(receipt.get("requested_base_quantity"))
        if (
            shortfall is None
            or shortfall < 0.0
            or receipt_quantity is None
            or receipt_quantity <= 0.0
        ):
            return False
        receipt_shortfall += shortfall
        exit_quantity += receipt_quantity
        receipt_fee = _execution_benchmark_receipt_fee_quote(receipt)
        if receipt_fee is None:
            return False
        receipt_fee_quote += receipt_fee

    tolerance = max(1e-8, max(receipt_shortfall, reported_shortfall) * 1e-8)
    observed_entry_fee = _finite_required(terminal.get("entry_fee_quote"))
    observed_exit_fee = _finite_required(terminal.get("exit_fee_quote"))
    if observed_entry_fee is None or observed_exit_fee is None:
        return False
    fee_tolerance = max(
        1e-8,
        max(abs(receipt_fee_quote), abs(observed_entry_fee + observed_exit_fee))
        * 1e-8,
    )
    return (
        isfinite(exit_quantity)
        and _execution_benchmark_quantities_match(exit_quantity, opened_quantity)
        and abs(receipt_shortfall - reported_shortfall) <= tolerance
        and abs(receipt_fee_quote - (observed_entry_fee + observed_exit_fee))
        <= fee_tolerance
    )


def _opened_matched_base_quantity(opened: Mapping[str, object]) -> float | None:
    """Read the actual common entry quantity from durable opening evidence.

    ``entry.opened`` has used both ``matched_quantity`` and ``quantity`` in
    historical journals.  The canary accepts either only when it is a finite,
    positive base quantity; it never substitutes a planned size because the
    execution receipts must cover what actually reached the exchange.
    """
    for quantity_field in ("matched_quantity", "quantity"):
        quantity = _finite_required(opened.get(quantity_field))
        if quantity is not None and quantity > 0.0:
            return quantity
    return None


def _execution_benchmark_quantities_match(left: float, right: float) -> bool:
    return abs(left - right) <= max(1e-10, max(abs(left), abs(right)) * 1e-8)


def _execution_benchmark_receipt_fee_quote(
    receipt: Mapping[str, object],
) -> float | None:
    """Sum literal per-fill fees only after the semantic verifier accepts them."""
    total = 0.0
    for leg_name in ("long", "short"):
        leg = receipt.get(leg_name)
        fills = leg.get("fills") if isinstance(leg, Mapping) else None
        if not isinstance(fills, list) or not fills:
            return None
        for fill in fills:
            fee_quote = _finite_required(
                fill.get("fee_quote") if isinstance(fill, Mapping) else None
            )
            if fee_quote is None:
                return None
            total += fee_quote
    return total if isfinite(total) else None


def _execution_benchmark_receipt_is_timely(receipt: Mapping[str, object]) -> bool:
    """Verify the sealed L2 observation remained current when every leg sent."""
    try:
        captured_at_ms = int(receipt.get("captured_at_ms", 0))
        max_delay_ms = int(receipt.get("max_observation_to_submit_ms", 0))
        requested_quantity = float(receipt.get("requested_base_quantity"))
    except (TypeError, ValueError, OverflowError):
        return False
    if (
        captured_at_ms <= 0
        or max_delay_ms != EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS
        or not isfinite(requested_quantity)
        or requested_quantity <= 0.0
    ):
        return False
    for leg_name in ("long", "short"):
        leg = receipt.get(leg_name)
        if not isinstance(leg, Mapping):
            return False
        try:
            observed_at_ms = int(leg.get("observed_at_ms", 0))
            age_ms = int(leg.get("age_ms", -1))
        except (TypeError, ValueError, OverflowError):
            return False
        fills = leg.get("fills")
        if (
            observed_at_ms <= 0
            or age_ms < 0
            or observed_at_ms > captured_at_ms
            or captured_at_ms - observed_at_ms != age_ms
            or not isinstance(fills, list)
            or not fills
        ):
            return False
        for fill in fills:
            if not isinstance(fill, Mapping):
                return False
            try:
                submitted_at_ms = int(fill.get("submitted_at_ms", 0))
                filled_at_ms = int(fill.get("filled_at_ms", 0))
            except (TypeError, ValueError, OverflowError):
                return False
            if (
                submitted_at_ms <= 0
                or filled_at_ms <= 0
                or captured_at_ms > submitted_at_ms
                or submitted_at_ms > filled_at_ms
                or submitted_at_ms - observed_at_ms > max_delay_ms
            ):
                return False
    return True


def _terminal_execution_benchmark_max_fill_at_ms(
    terminal: Mapping[str, object],
) -> int | None:
    """Return the last sealed close-fill timestamp, or ``None`` if absent.

    This is deliberately separate from journal time: the producer may buffer
    a journal append, while the fill boundary is the fact that the later
    exchange-flat probe must follow.
    """
    receipts = terminal.get("exit_execution_benchmark_receipts")
    if not isinstance(receipts, list) or not receipts:
        return None
    latest = 0
    for receipt in receipts:
        filled_at_ms = execution_benchmark_receipt_max_fill_at_ms(receipt)
        if filled_at_ms is None:
            return None
        latest = max(latest, filled_at_ms)
    return latest or None


def _actual_execution_cost_from_terminal_events(
    opened: Mapping[str, object], terminal_events: Iterable[Mapping[str, object]]
) -> tuple[float | None, bool]:
    """Return the unique terminal cost fact; replays/conflicts fail closed."""

    finite_costs = [
        cost
        for event in terminal_events
        if (cost := _actual_execution_cost_bps(opened, event)) is not None
    ]
    if len(finite_costs) != 1:
        return None, bool(finite_costs)
    return finite_costs[0], False


def _terminal_truth_flat_from_events(
    events: Iterable[Mapping[str, object]],
    settlement: Mapping[str, object] | None = None,
    *,
    selected: Mapping[str, object],
    opened: Mapping[str, object],
    finalized: Mapping[str, object] | None,
) -> tuple[bool, bool]:
    """Require one ordered, scoped proof between close and reconciliation.

    A flat account probe attached to a reconciliation row can be captured
    before or after any number of unrelated lifecycle changes.  Promotion
    instead needs one durable post-close proof with an explicit capture time
    and target scope, sequenced strictly after the actual ``exit.closed`` and
    strictly before the funding-accounting receipt.
    """
    terminal_events = list(events)
    closed_events = [
        event
        for event in terminal_events
        if event.get("_canary_journal_kind") == "exit.closed"
    ]
    truth_events = [
        event
        for event in terminal_events
        if event.get("_canary_journal_kind")
        == "runtime.position_lifecycle_terminal"
        and "exchange_truth" in event
    ]
    # A canary loop is a linear state machine, not an event bag.  Extra
    # terminal rows (including passive-close resolution) make it impossible
    # to know which close actually owns the subsequent truth probe, even when
    # only one of them carries a cost field.
    if (
        len(terminal_events) != 2
        or len(closed_events) != 1
        or len(truth_events) != 1
        or not isinstance(settlement, Mapping)
    ):
        return False, False
    close_event = closed_events[0]
    truth_event = truth_events[0]
    close_seq = _positive_int(close_event.get("_canary_journal_seq"))
    truth_seq = _positive_int(truth_event.get("_canary_journal_seq"))
    settlement_seq = _positive_int(settlement.get("_canary_journal_seq"))
    close_journal_at_ms = _positive_int(close_event.get("_canary_journal_at_ms"))
    truth_journal_at_ms = _positive_int(truth_event.get("_canary_journal_at_ms"))
    settlement_journal_at_ms = _positive_int(settlement.get("_canary_journal_at_ms"))
    position_id = _text(close_event.get("position_id"))
    symbol = _text(close_event.get("symbol")).upper()
    long_venue = _text(close_event.get("long_venue")).lower()
    short_venue = _text(close_event.get("short_venue")).lower()
    if (
        close_seq is None
        or truth_seq is None
        or settlement_seq is None
        or close_journal_at_ms is None
        or truth_journal_at_ms is None
        or settlement_journal_at_ms is None
        or not (close_seq < truth_seq < settlement_seq)
        or not (close_journal_at_ms <= truth_journal_at_ms <= settlement_journal_at_ms)
        or not position_id
        or not symbol
        or not long_venue
        or not short_venue
        or long_venue == short_venue
        or _text(settlement.get("position_id")) != position_id
    ):
        return False, False
    selected_seq = _positive_int(selected.get("_canary_journal_seq"))
    opened_seq = _positive_int(opened.get("_canary_journal_seq"))
    finalized_seq = (
        _positive_int(finalized.get("_canary_journal_seq"))
        if isinstance(finalized, Mapping)
        else None
    )
    if (
        selected_seq is None
        or opened_seq is None
        or not (selected_seq < opened_seq < close_seq < truth_seq < settlement_seq)
        or (
            finalized_seq is not None
            and not (opened_seq < finalized_seq < close_seq)
        )
    ):
        return False, False
    scope = truth_event.get("exchange_truth_scope")
    capture_at_ms = _positive_int(truth_event.get("exchange_truth_captured_at_ms"))
    closed_at_ms = _positive_int(close_event.get("closed_at_ms"))
    execution_completed_at_ms = _positive_int(
        close_event.get("execution_completed_at_ms")
    )
    reconciled_at_ms = _positive_int(settlement.get("reconciled_at_ms"))
    max_fill_at_ms = _terminal_execution_benchmark_max_fill_at_ms(close_event)
    venues = scope.get("venues") if isinstance(scope, Mapping) else None
    if (
        capture_at_ms is None
        or closed_at_ms is None
        or execution_completed_at_ms is None
        or reconciled_at_ms is None
        or max_fill_at_ms is None
        or not (
            closed_at_ms
            <= execution_completed_at_ms
            and max_fill_at_ms <= execution_completed_at_ms
            and execution_completed_at_ms <= capture_at_ms <= reconciled_at_ms
        )
        or not isinstance(venues, list)
        or {_text(venue).lower() for venue in venues} != {long_venue, short_venue}
        or len(venues) != 2
        or _text(truth_event.get("position_id")) != position_id
        or _text(truth_event.get("symbol")).upper() != symbol
        or _text(scope.get("position_id")) != position_id
        or _text(scope.get("symbol")).upper() != symbol
    ):
        return False, False
    if _text(truth_event.get("terminal_reason")) != _POST_CLOSE_FUNDING_TRUTH_REASON:
        return False, False
    return _terminal_truth_flat(truth_event), True


def _entry_notional(opened: Mapping[str, object]) -> float:
    """Return maximum actual leg notional, which is the release-cap basis."""

    long_quantity = _finite_required(
        opened.get("long_matched_quantity", opened.get("matched_quantity", opened.get("quantity")))
    )
    short_quantity = _finite_required(
        opened.get("short_matched_quantity", opened.get("matched_quantity", opened.get("quantity")))
    )
    long_price = _finite_required(opened.get("long_entry_price"))
    short_price = _finite_required(opened.get("short_entry_price"))
    if (
        long_quantity is None
        or short_quantity is None
        or long_price is None
        or short_price is None
        or min(long_quantity, short_quantity, long_price, short_price) <= 0.0
    ):
        return 0.0
    return max(long_quantity * long_price, short_quantity * short_price)


def _planned_max_leg_notional(selected: Mapping[str, object]) -> float:
    declared = _finite_required(selected.get("planned_entry_notional_quote"))
    quantity = _finite_required(selected.get("planned_entry_quantity"))
    long_price = _finite_required(selected.get("planned_long_entry_price"))
    short_price = _finite_required(selected.get("planned_short_entry_price"))
    if (
        declared is None
        or quantity is None
        or long_price is None
        or short_price is None
        or min(quantity, long_price, short_price) <= 0.0
    ):
        return 0.0
    calculated = max(long_price, short_price) * quantity
    if abs(calculated - declared) > max(1e-9, abs(calculated) * 1e-9):
        return 0.0
    return calculated


def _worst_case_buffer_bps(selected: Mapping[str, object]) -> float:
    expected = _finite_required(selected.get("expected_net_edge_bps"))
    worst = _finite_required(selected.get("worst_case_edge_bps"))
    if expected is None or worst is None:
        return 0.0
    return max(expected - worst, 0.0)


def _terminal_truth_flat(payload: Mapping[str, object]) -> bool:
    truth = payload.get("exchange_truth")
    if not isinstance(truth, Mapping):
        return False
    return (
        truth.get("truth_available") is True
        and truth.get("positions_flat") is True
        and truth.get("open_orders_flat") is True
    )


def _official_reconciliation_complete(
    payload: Mapping[str, object] | None,
    *,
    position_id: str,
) -> bool:
    """Accept only a complete private-statement reconciliation receipt."""

    if not isinstance(payload, Mapping):
        return False
    if payload.get("official_pnl") is not True:
        return False
    required = _positive_int(payload.get("required_settlement_count"))
    observed = _nonnegative_int(payload.get("observed_settlement_count"))
    claims = payload.get("statement_claims")
    symbol = _text(payload.get("symbol")).upper()
    if (
        required is None
        or observed is None
        or observed < required
        or not isinstance(claims, list)
        or len(claims) != required
        or not symbol
        or not _statement_claims_complete(claims, position_id=position_id, symbol=symbol)
    ):
        return False
    return (
        _finite_required(payload.get("official_funding_quote")) is not None
        and _finite_required(payload.get("official_net_quote")) is not None
    )


def _statement_claims_complete(
    claims: list[object], *, position_id: str, symbol: str
) -> bool:
    seen: set[tuple[str, str, int]] = set()
    for claim in claims:
        if not isinstance(claim, Mapping):
            return False
        venue = _text(claim.get("venue")).lower()
        claim_symbol = _text(claim.get("symbol")).upper()
        timestamp = _positive_int(claim.get("settlement_timestamp_ms"))
        if (
            _text(claim.get("owner_id")) != position_id
            or _text(claim.get("position_id")) != position_id
            or _text(claim.get("leg")) not in {"long", "short"}
            or not venue
            or claim_symbol != symbol
            or timestamp is None
            or not _text(claim.get("quote_currency"))
            or not _text(claim.get("statement_reference"))
            or _positive_int(claim.get("recorded_at_ms")) is None
        ):
            return False
        key = (venue, claim_symbol, timestamp)
        if key in seen:
            return False
        seen.add(key)
    return True


def _positive_int(value: object) -> int | None:
    parsed = _integer(value)
    return parsed if parsed is not None and parsed > 0 else None


def _nonnegative_int(value: object) -> int | None:
    parsed = _integer(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not isfinite(numeric) or not numeric.is_integer():
        return None
    return int(numeric)


def _finite_required(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if isfinite(number) else None


def _percentile(values: list[float | None], quantile: float) -> float | None:
    finite = sorted(float(value) for value in values if value is not None and isfinite(value))
    if not finite:
        return None
    index = max(0, min(len(finite) - 1, ceil(float(quantile) * len(finite)) - 1))
    return finite[index]


def _text(value: object) -> str:
    return str(value or "").strip()
