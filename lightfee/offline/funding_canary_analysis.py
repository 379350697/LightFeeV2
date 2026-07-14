"""Fail-closed promotion analysis for the live funding canary cohort.

Promotion is deliberately stricter than ordinary offline attribution.  It
accepts only one immutable, sequenced canary cohort; it never fills in missing
economics, price benchmarks, exchange truth or reconciliation facts from a
later configuration or an inferred PnL value.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from math import ceil, isfinite
from typing import Iterable, Mapping


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
_POLICY_VERSION = "funding-canary-v1"
_HARD_MAX_ENTRY_NOTIONAL_QUOTE = 30.0
_HARD_MIN_EXPECTED_NET_EDGE_BPS = 8.0
_HARD_MIN_WORST_CASE_EDGE_BPS = 3.0
_MIN_REQUIRED_CLOSED_LOOPS = 30


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
    opened: dict[tuple[str, str], dict[str, object]] = {}
    terminal_events: dict[tuple[str, str], list[dict[str, object]]] = {}
    reconciled: dict[tuple[str, str], dict[str, object]] = {}
    entry_to_position: dict[tuple[str, str], str] = {}
    seen_event_ids: set[tuple[str, int]] = set()
    ordered_records: list[tuple[str, int, int, str, dict[str, object]]] = []

    for ordinal, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        kind = _text(record.get("kind"))
        payload = record.get("payload")
        if kind not in _RELEVANT_KINDS or not isinstance(payload, Mapping):
            continue
        data = dict(payload)
        # Explicit paper selected events are intentionally ignored.  All
        # candidate live evidence must be journal-sequenced and bound to a run.
        is_paper_selected = (
            kind == _SELECTED_KIND
            and data.get("funding_canary") is True
            and _text(data.get("runtime_mode")).lower() == "paper"
        )
        if is_paper_selected:
            continue
        run_id = _text(record.get("run_id"))
        seq = _positive_int(record.get("seq"))
        if not run_id or seq is None:
            report.missing_event_sequence_count += 1
            continue
        event_id = (run_id, seq)
        if event_id in seen_event_ids:
            report.ambiguous_event_count += 1
            continue
        seen_event_ids.add(event_id)
        ordered_records.append((run_id, seq, ordinal, kind, data))

    for run_id, _, _, kind, data in sorted(ordered_records):
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
            if entry_id and position_id:
                entry_key = (run_id, entry_id)
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
        contract_reason = _selection_contract_reason(selected_payload)
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
        if actual_notional <= 0.0 or actual_notional > _HARD_MAX_ENTRY_NOTIONAL_QUOTE + 1e-12:
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
        truth_flat = _terminal_truth_flat_from_events(
            position_terminal_events,
            settlement_payload,
        )
        official = _official_reconciliation_complete(settlement_payload, position_id=position_id)
        budget, reserve = _selection_execution_cost_budget(selected_payload)
        if cost is None:
            report.missing_actual_cost_count += 1
        if not truth_flat:
            report.missing_terminal_truth_count += 1
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
            and actual_notional <= _HARD_MAX_ENTRY_NOTIONAL_QUOTE + 1e-12
            and official
            and truth_flat
            and cost is not None
            and budget is not None
            and reserve is not None
            and not terminal_ambiguous
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


def _selection_contract_reason(selected: Mapping[str, object]) -> str:
    """Validate the immutable selected-marker contract, never current config."""

    if _text(selected.get("funding_canary_policy_version")) != _POLICY_VERSION:
        return "policy_version_missing_or_mismatched"
    if not _text(selected.get("funding_canary_cohort_id")):
        return "cohort_id_missing"
    if selected.get("economics_complete") is not True:
        return "economics_incomplete"
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
    if hard_cap > _HARD_MAX_ENTRY_NOTIONAL_QUOTE + 1e-12 or hard_cap <= 0.0:
        return "hard_notional_cap_invalid"
    if hard_expected < _HARD_MIN_EXPECTED_NET_EDGE_BPS - 1e-12:
        return "hard_expected_floor_weakened"
    if hard_worst < _HARD_MIN_WORST_CASE_EDGE_BPS - 1e-12:
        return "hard_worst_floor_weakened"
    if expected < _HARD_MIN_EXPECTED_NET_EDGE_BPS - 1e-12:
        return "expected_edge_below_hard_floor"
    if worst < _HARD_MIN_WORST_CASE_EDGE_BPS - 1e-12:
        return "worst_edge_below_hard_floor"
    if planned <= 0.0 or planned > min(hard_cap, _HARD_MAX_ENTRY_NOTIONAL_QUOTE) + 1e-12:
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
) -> bool:
    """Require exactly one terminal flat-truth receipt for the same loop.

    A normal close provides execution-cost facts; its post-close accounting
    receipt provides the fresh exchange truth.  Legacy terminal events that
    already carry the receipt remain supported, but duplicated truth claims
    are treated as ambiguous rather than selected by input order.
    """
    truth_events = [event for event in events if "exchange_truth" in event]
    if isinstance(settlement, Mapping) and "exchange_truth" in settlement:
        truth_events.append(settlement)
    if len(truth_events) != 1:
        return False
    return _terminal_truth_flat(truth_events[0])


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
