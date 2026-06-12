"""Pure V1 lifecycle closure projection.

This module is intentionally side-effect free. It projects local runtime state,
exchange truth, recovery decisions, entry market-data scope, and diagnostic
events into one V1-compatible lifecycle table that runtime and ops surfaces can
render without reinterpreting trading semantics.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from lightfee.engine.pending_entry_terminalizer import (
    PendingEntryLiveTruth,
    PendingEntryTerminalizer,
)
from lightfee.engine.recovery_decision_core import (
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.recovery_ledger import RecoveryLedger

EPSILON = 1e-9
VERSION = "v1.lifecycle_closure.v1"
ENTRY_RESIDUAL_DUST_TOLERANCE_RATIO = 0.02


class V1LifecycleClosurePhase(StrEnum):
    ENTRY_QUOTE_LEASE = "ENTRY_QUOTE_LEASE"
    PENDING_ENTRY = "PENDING_ENTRY"
    OPEN_POSITION = "OPEN_POSITION"
    PASSIVE_CLOSE = "PASSIVE_CLOSE"
    RESIDUAL_REPAIR = "RESIDUAL_REPAIR"
    RECOVERY_TRUTH = "RECOVERY_TRUTH"
    RUNTIME_PROGRESS = "RUNTIME_PROGRESS"
    DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"


@dataclass(frozen=True)
class V1LifecycleClosureRow:
    row_key: str
    phase: V1LifecycleClosurePhase | str
    owner_id: str
    evidence_class: str
    terminality: str
    entry_policy: str
    recovery_policy: str
    diagnostic_severity: str
    v1_anchor: str
    details: Mapping[str, Any] = field(default_factory=dict)
    input_hash: str = ""
    closure_decision_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "row_key": self.row_key,
            "phase": _enum_value(self.phase),
            "owner_id": self.owner_id,
            "evidence_class": self.evidence_class,
            "terminality": self.terminality,
            "entry_policy": self.entry_policy,
            "recovery_policy": self.recovery_policy,
            "diagnostic_severity": self.diagnostic_severity,
            "v1_anchor": self.v1_anchor,
            "closure_decision_id": self.closure_decision_id,
            "input_hash": self.input_hash,
            "details": _jsonable(self.details),
        }
        return payload


@dataclass(frozen=True)
class V1LifecycleClosureTable:
    version: str
    generated_at_ms: int
    summary: Mapping[str, Any]
    rows: tuple[V1LifecycleClosureRow, ...]
    unmapped_event_kinds: tuple[str, ...] = ()
    performance_scope: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at_ms": self.generated_at_ms,
            "summary": _jsonable(self.summary),
            "rows": [row.to_dict() for row in self.rows],
            "unmapped_event_kinds": list(self.unmapped_event_kinds),
            "performance_scope": _jsonable(self.performance_scope),
        }


def build_v1_lifecycle_closure_table(
    *,
    local_state: Any,
    exchange_truth: Mapping[str, Any] | None = None,
    generated_at_ms: int | None = None,
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    previous_table: Mapping[str, Any] | None = None,
    recovery_ledger: RecoveryLedger | None = None,
    recovery_decision: Any | None = None,
    owner_index: Any | None = None,
) -> V1LifecycleClosureTable:
    generated = int(generated_at_ms or _now_ms())
    truth = _normalized_exchange_truth(exchange_truth)
    ledger = recovery_ledger or RecoveryLedger.from_local_and_exchange_truth(
        local=local_state,
        exchange_truth=truth or {},
        owner_index=owner_index,
    )
    decision = recovery_decision or V1RecoveryDecisionCore().decide(
        RecoveryEvidenceSnapshot(
            local_open_positions=_state_collection_or_count(
                local_state, "open_positions", "open_position_count"
            ),
            pending_entries=_state_collection_or_count(
                local_state, "pending_entries", "pending_entry_count"
            ),
            residual_repairs=_state_collection_or_count(
                local_state, "pending_residual_repairs", "pending_residual_repair_count"
            ),
            passive_closes=_state_collection_or_count(
                local_state, "pending_passive_closes", "pending_close_count"
            ),
            exchange_truth=truth,
            prior_recovery_block_reason=_get(local_state, "recovery_blocked_reason"),
            recovery_work_items=tuple(ledger.work_items),
            operator_fail_closed=_operator_fail_closed(local_state),
        )
    )

    rows: list[V1LifecycleClosureRow] = []
    rows.append(_recovery_truth_row(decision))
    rows.extend(_entry_quote_lease_rows(local_state))
    rows.extend(_pending_entry_rows(local_state, truth))
    rows.extend(_open_position_rows(local_state, ledger))
    rows.extend(_passive_close_rows(local_state))
    rows.extend(_residual_repair_rows(local_state))
    rows.extend(_runtime_progress_rows(local_state, generated))

    rows = _apply_previous_row_reuse(rows, previous_table)
    unmapped = tuple(sorted(_unmapped_event_kinds(events)))
    performance_scope = _performance_scope(local_state, rows)
    summary = _summary_from_decision(decision, rows, performance_scope)
    return V1LifecycleClosureTable(
        version=VERSION,
        generated_at_ms=generated,
        summary=summary,
        rows=tuple(rows),
        unmapped_event_kinds=unmapped,
        performance_scope=performance_scope,
    )


def map_lifecycle_event_kind(kind: str) -> str | None:
    text = str(kind or "")
    if text in _EVENT_KIND_PHASES:
        return _EVENT_KIND_PHASES[text]
    for prefix, phase in _EVENT_PREFIX_PHASES:
        if text.startswith(prefix):
            return phase
    return None


def entry_gate_from_closure(closure: Mapping[str, Any] | V1LifecycleClosureTable | None) -> tuple[bool, str]:
    """Return the runtime entry decision from the closure summary only."""
    summary = _mapping(_closure_payload(closure).get("summary", {}))
    if summary.get("entry_allowed") is False:
        return False, _first_text(
            summary.get("entry_block_reason"),
            summary.get("recovery_block_reason"),
            summary.get("recovery_decision_kind"),
            "v1_lifecycle_closure_blocked",
        )
    return True, ""


def closure_event_fields(
    closure: Mapping[str, Any] | V1LifecycleClosureTable | None,
    *,
    phase: str,
    owner_id: str = "",
    row_key: str = "",
) -> dict[str, str]:
    """Select the closure row identity that authorized a release event."""
    phase_text = str(phase or "")
    row = _closure_row(
        closure,
        phase=phase_text,
        owner_id=str(owner_id or ""),
        row_key=str(row_key or ""),
    )
    if row is None:
        return {
            "closure_phase": phase_text,
            "closure_row_key": "",
            "closure_decision_id": "",
        }
    return {
        "closure_phase": str(row.get("phase") or phase_text),
        "closure_row_key": str(row.get("row_key") or ""),
        "closure_decision_id": str(row.get("closure_decision_id") or ""),
    }


def _recovery_truth_row(decision: Any) -> V1LifecycleClosureRow:
    block_reason = str(getattr(decision, "block_reason", "") or "")
    clear_reason = str(getattr(decision, "clear_reason", "") or "")
    details = {
        "decision": _enum_value(getattr(decision, "kind", "")),
        "entry_allowed": bool(getattr(decision, "entry_allowed", False)),
        "block_reason": block_reason,
        "clear_reason": clear_reason,
        "management_action": _enum_value(getattr(decision, "management_action", "")),
    }
    recovery_policy = (
        "block"
        if block_reason
        else "clear"
        if clear_reason
        else "observe"
    )
    entry_policy = (
        "allow_new_risk"
        if bool(getattr(decision, "entry_allowed", False))
        else "block_new_risk"
    )
    return _row(
        row_key="recovery_truth:core",
        phase=V1LifecycleClosurePhase.RECOVERY_TRUTH,
        owner_id="core",
        evidence_class=str(getattr(decision, "evidence_quality", "") or ""),
        terminality=_enum_value(getattr(decision, "kind", "")),
        entry_policy=entry_policy,
        recovery_policy=recovery_policy,
        diagnostic_severity=str(getattr(decision, "diagnostic_severity", "info") or "info"),
        v1_anchor="V1RecoveryDecisionCore priority order",
        details=details,
    )


def _entry_quote_lease_rows(local_state: Any) -> list[V1LifecycleClosureRow]:
    config = _mapping(_get(local_state, "runtime_market_data_config", {}))
    last_scan = _mapping(_get(local_state, "last_scan", {}))
    provider = str(config.get("entry_readiness_provider_effective", "") or "")
    ws_bbo_effective = (
        provider == "ws_bbo_quote_lease"
        and config.get("local_l2_effective_enabled") is False
    )
    if not ws_bbo_effective and not any(
        key in last_scan
        for key in (
            "quote_revalidate_candidate_scope",
            "snapshot_freshness_filter_candidate_scope",
            "candidate_freshness_candidate_scope",
            "snapshot_freshness_candidate_scope",
        )
    ):
        return []

    scope = _first_text(
        last_scan.get("quote_revalidate_candidate_scope"),
        last_scan.get("snapshot_freshness_filter_candidate_scope"),
        last_scan.get("candidate_freshness_candidate_scope"),
        last_scan.get("snapshot_freshness_candidate_scope"),
    )
    candidate_count = _int(
        last_scan.get("quote_revalidate_candidate_count"),
        last_scan.get("snapshot_freshness_filter_candidate_count"),
        last_scan.get("candidate_freshness_candidate_count"),
        last_scan.get("snapshot_freshness_candidate_count"),
    )
    all_count = _int(
        last_scan.get("quote_revalidate_all_target_count"),
        last_scan.get("snapshot_freshness_filter_all_candidate_count"),
        last_scan.get("candidate_freshness_all_candidate_count"),
        last_scan.get("snapshot_freshness_all_candidate_count"),
    )
    skipped = _int(
        last_scan.get("quote_revalidate_skipped_untracked_count"),
        last_scan.get("snapshot_freshness_filter_skipped_untracked_count"),
        last_scan.get("candidate_freshness_skipped_untracked_count"),
        last_scan.get("snapshot_freshness_skipped_untracked_count"),
    )
    failed = _int(last_scan.get("quote_revalidate_failed_count"))
    tracked_scope = scope in {"", "v1_primary_shadow"} or (
        scope == "global_snapshot" and candidate_count == 0
    )
    full_universe = bool(
        ws_bbo_effective
        and scope
        and not tracked_scope
        and candidate_count > 0
        and skipped == 0
    )
    missing_or_stale = failed > 0
    severity = "critical" if full_universe or missing_or_stale else "info"
    return [
        _row(
            row_key="entry_quote_lease:ws_bbo",
            phase=V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE,
            owner_id="ws_bbo_quote_lease",
            evidence_class="execution_scope" if not missing_or_stale else "quote_evidence_gap",
            terminality=(
                "full_universe_scope"
                if full_universe
                else "fail_closed_quote_gap"
                if missing_or_stale
                else "tracked_scope"
            ),
            entry_policy=(
                "diagnostic_only"
                if full_universe
                else "fail_closed_tracked_candidates"
                if missing_or_stale
                else "allow_tracked_candidates"
            ),
            recovery_policy="diagnostic_regression" if full_universe else "observe",
            diagnostic_severity=severity,
            v1_anchor="V1 entry data plane tracks primary plus shadow only",
            details={
                "provider": provider,
                "scope": scope,
                "candidate_count": candidate_count,
                "all_count": all_count,
                "skipped_untracked_count": skipped,
                "quote_revalidate_failed_count": failed,
            },
        )
    ]


def _pending_entry_rows(
    local_state: Any,
    exchange_truth: Mapping[str, Any] | None,
) -> list[V1LifecycleClosureRow]:
    rows: list[V1LifecycleClosureRow] = []
    truth_available = _truth_available(exchange_truth)
    for pending in _state_collection(local_state, "pending_entries"):
        symbol = _symbol(pending)
        owner_id = _owner_id(
            pending,
            "pending_id",
            "position_id",
            "entry_id",
            default=symbol or "pending_entry",
        )
        has_live_order = _has_live_open_order(exchange_truth, symbol)
        has_live_position = _has_live_position(exchange_truth, symbol)
        decision = PendingEntryTerminalizer().decide(
            pending,
            live_truth=PendingEntryLiveTruth(
                available=truth_available,
                has_live_open_order=has_live_order,
                has_live_position=has_live_position,
                error="" if truth_available else "exchange_truth_unavailable",
            ),
        )
        if decision.outcome == "deferred_live_open_order":
            terminality = "retain_live_open_order"
        elif decision.outcome == "deferred_live_position":
            terminality = "retain_live_position"
        elif decision.outcome == "deferred_missing_live_truth":
            terminality = "retain_missing_live_truth"
        elif decision.contains_positive_fill_evidence:
            terminality = "terminal_positive_fill_evidence"
        else:
            terminality = decision.outcome
        rows.append(
            _row(
                row_key=f"pending_entry:{owner_id}",
                phase=V1LifecycleClosurePhase.PENDING_ENTRY,
                owner_id=owner_id,
                evidence_class=(
                    "live_artifact"
                    if has_live_order or has_live_position
                    else "positive_fill"
                    if decision.contains_positive_fill_evidence
                    else "pending_owner"
                ),
                terminality=terminality,
                entry_policy=(
                    "allow_after_terminal"
                    if decision.allows_pending_removal
                    else "block_conflicting_new_risk"
                ),
                recovery_policy=(
                    "terminalize_pending_entry"
                    if decision.allows_pending_removal
                    else "manage_pending_entry"
                ),
                diagnostic_severity="info" if decision.healthy else "warning",
                v1_anchor="PendingEntryTerminalizer live-truth authority",
                details={
                    "symbol": symbol,
                    "decision_outcome": decision.outcome,
                    "decision_reason": decision.reason,
                    "matched_quantity": decision.matched_quantity,
                    "residual_quantity": decision.residual_quantity,
                    "has_live_open_order": has_live_order,
                    "has_live_position": has_live_position,
                },
            )
        )
    return rows


def _open_position_rows(local_state: Any, ledger: RecoveryLedger) -> list[V1LifecycleClosureRow]:
    rows: list[V1LifecycleClosureRow] = []
    for position in _state_collection(local_state, "open_positions"):
        owner_id = _owner_id(position, "position_id", default=_symbol(position) or "open_position")
        rows.append(
            _row(
                row_key=f"open_position:{owner_id}",
                phase=V1LifecycleClosurePhase.OPEN_POSITION,
                owner_id=owner_id,
                evidence_class="local_open_position",
                terminality="live_position_managed",
                entry_policy="block_if_capacity_or_overlap_requires",
                recovery_policy="manage_open_position",
                diagnostic_severity="info",
                v1_anchor="Exchange truth plus local owner controls open exposure",
                details={"symbol": _symbol(position), "venues": sorted(_venues(position))},
            )
        )
    for item in ledger.work_items:
        kind = str(_get(item, "kind", "") or "")
        if kind not in {"orphan_maker_order", "unpaired_live_position", "orphan_reduce_only_order"}:
            continue
        owner = _get(item, "owner", None)
        owner_id = _owner_id(
            owner,
            "owner_id",
            default=_owner_id(_first_item(_get(item, "artifacts", ())), "order_id", "client_order_id", default=_symbol(item) or kind),
        )
        rows.append(
            _row(
                row_key=f"open_position:{kind}:{owner_id}",
                phase=V1LifecycleClosurePhase.OPEN_POSITION,
                owner_id=owner_id,
                evidence_class="orphan_live_artifact",
                terminality=kind,
                entry_policy="block_all_new_risk",
                recovery_policy="block_or_flatten_live_artifact",
                diagnostic_severity="critical",
                v1_anchor="Exchange truth outranks local recovered state",
                details={
                    "kind": kind,
                    "symbol": _get(item, "symbol", ""),
                    "venues": sorted(_get(item, "venues", ()) or ()),
                    "decision_reason": _get(_get(item, "decision", None), "reason", ""),
                },
            )
        )
    return rows


def _passive_close_rows(local_state: Any) -> list[V1LifecycleClosureRow]:
    rows: list[V1LifecycleClosureRow] = []
    for key in ("pending_closes", "pending_passive_closes"):
        for close in _state_collection(local_state, key):
            owner_id = _owner_id(close, "position_id", "close_id", default=_symbol(close) or key)
            rows.append(
                _row(
                    row_key=f"passive_close:{owner_id}",
                    phase=V1LifecycleClosurePhase.PASSIVE_CLOSE,
                    owner_id=owner_id,
                    evidence_class="pending_passive_close",
                    terminality="pending_exchange_truth_resolution",
                    entry_policy="block_conflicting_new_risk",
                    recovery_policy="manage_passive_close",
                    diagnostic_severity="warning",
                    v1_anchor="Passive close terminality converges on exchange truth",
                    details={"source": key, "symbol": _symbol(close)},
                )
            )
    return rows


def _residual_repair_rows(local_state: Any) -> list[V1LifecycleClosureRow]:
    rows: list[V1LifecycleClosureRow] = []
    for task in _state_collection(local_state, "pending_residual_repairs"):
        owner_id = _owner_id(task, "repair_id", "task_id", "position_id", default=_symbol(task) or "residual")
        terminal_reason = str(_get(task, "terminal_reason", _get(task, "last_error", "")) or "")
        residual_ratio = _float(_get(task, "residual_ratio", _get(task, "residual_quantity_ratio", 0.0)))
        exchange_min_dust = "exchange_min" in terminal_reason and "dust" in terminal_reason
        entry_origin = str(_get(task, "origin", "") or "") == "entry_open"
        tolerated = (
            entry_origin
            and exchange_min_dust
            and residual_ratio <= ENTRY_RESIDUAL_DUST_TOLERANCE_RATIO + EPSILON
        )
        rows.append(
            _row(
                row_key=f"residual_repair:{owner_id}",
                phase=V1LifecycleClosurePhase.RESIDUAL_REPAIR,
                owner_id=owner_id,
                evidence_class="exchange_min_dust" if exchange_min_dust else "residual_repair",
                terminality="terminal_dust_tolerated" if tolerated else "retain_residual_repair",
                entry_policy="allow_after_terminal" if tolerated else "block_conflicting_new_risk",
                recovery_policy="release_residual_gate" if tolerated else "manage_residual_repair",
                diagnostic_severity="info" if tolerated else "warning",
                v1_anchor="Residual repair releases only on exchange-truth terminal evidence",
                details={
                    "symbol": _symbol(task),
                    "origin": _get(task, "origin", ""),
                    "terminal_reason": terminal_reason,
                    "residual_ratio": residual_ratio,
                    "tolerance_ratio": ENTRY_RESIDUAL_DUST_TOLERANCE_RATIO,
                },
            )
        )
    return rows


def _runtime_progress_rows(local_state: Any, generated_at_ms: int) -> list[V1LifecycleClosureRow]:
    progress = _mapping(_get(local_state, "runtime_progress", {}))
    active_lane = str(progress.get("active_lane", "") or "")
    if not progress and not active_lane:
        return []
    started_ms = _int(progress.get("active_lane_started_ms"))
    budget_ms = _int(progress.get("active_lane_budget_ms"))
    overdue = bool(progress.get("active_lane_overdue"))
    if active_lane and started_ms > 0 and budget_ms > 0:
        overdue = overdue or generated_at_ms - started_ms > budget_ms
    rows = [
        _row(
            row_key="runtime_progress:active_lane",
            phase=V1LifecycleClosurePhase.RUNTIME_PROGRESS,
            owner_id=active_lane or "runtime_loop",
            evidence_class="runtime_lane_progress",
            terminality="active_lane_overdue" if overdue else "bounded_progress",
            entry_policy="diagnostic_only",
            recovery_policy="runtime_progress_stale" if overdue else "observe",
            diagnostic_severity="critical" if overdue else "info",
            v1_anchor="Runtime progress must not be inferred from exporter heartbeat",
            details={
                "active_lane": active_lane,
                "active_lane_started_ms": started_ms,
                "active_lane_budget_ms": budget_ms,
                "active_lane_overdue": overdue,
            },
        )
    ]
    return rows


def _summary_from_decision(
    decision: Any,
    rows: list[V1LifecycleClosureRow],
    performance_scope: Mapping[str, Any],
) -> dict[str, Any]:
    block_reason = str(getattr(decision, "block_reason", "") or "")
    clear_reason = str(getattr(decision, "clear_reason", "") or "")
    decision_kind = _enum_value(getattr(decision, "kind", ""))
    if decision_kind == "RUNNING_WITH_EVIDENCE_GAP":
        recovery_block_policy = "warn_evidence_gap"
    elif block_reason:
        recovery_block_policy = "block"
    elif clear_reason:
        recovery_block_policy = "clear"
    else:
        recovery_block_policy = "none"
    severity = _max_severity([row.diagnostic_severity for row in rows])
    blocking_rows = [
        row.row_key
        for row in rows
        if row.entry_policy.startswith("block") or row.recovery_policy.startswith("block")
    ]
    return {
        "entry_allowed": bool(getattr(decision, "entry_allowed", False)),
        "entry_block_reason": getattr(decision, "entry_block_reason", None),
        "recovery_decision_kind": decision_kind,
        "recovery_block_policy": recovery_block_policy,
        "recovery_block_reason": block_reason or None,
        "recovery_clear_reason": clear_reason or None,
        "diagnostic_severity": severity,
        "row_count": len(rows),
        "blocking_row_count": len(blocking_rows),
        "blocking_rows": blocking_rows,
        "full_universe_hot_path_detected": bool(
            performance_scope.get("full_universe_hot_path_detected")
        ),
    }


def _performance_scope(
    local_state: Any,
    rows: list[V1LifecycleClosureRow],
) -> dict[str, Any]:
    last_scan = _mapping(_get(local_state, "last_scan", {}))
    quote_scope = _first_text(
        last_scan.get("quote_revalidate_candidate_scope"),
        last_scan.get("snapshot_freshness_filter_candidate_scope"),
        last_scan.get("candidate_freshness_candidate_scope"),
        last_scan.get("snapshot_freshness_candidate_scope"),
    )
    full_universe = any(
        row.phase == V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE
        and row.terminality == "full_universe_scope"
        for row in rows
    )
    return {
        "entry_quote_scope": quote_scope,
        "full_universe_hot_path_detected": full_universe,
        "row_count": len(rows),
        "critical_row_count": sum(
            1 for row in rows if row.diagnostic_severity == "critical"
        ),
    }


def _row(
    *,
    row_key: str,
    phase: V1LifecycleClosurePhase,
    owner_id: str,
    evidence_class: str,
    terminality: str,
    entry_policy: str,
    recovery_policy: str,
    diagnostic_severity: str,
    v1_anchor: str,
    details: Mapping[str, Any] | None = None,
) -> V1LifecycleClosureRow:
    raw = {
        "row_key": row_key,
        "phase": _enum_value(phase),
        "owner_id": owner_id,
        "evidence_class": evidence_class,
        "terminality": terminality,
        "entry_policy": entry_policy,
        "recovery_policy": recovery_policy,
        "diagnostic_severity": diagnostic_severity,
        "v1_anchor": v1_anchor,
        "details": _jsonable(details or {}),
    }
    input_hash = _stable_hash(raw)
    decision_id = "v1lc-" + input_hash[:16]
    return V1LifecycleClosureRow(
        row_key=row_key,
        phase=phase,
        owner_id=owner_id,
        evidence_class=evidence_class,
        terminality=terminality,
        entry_policy=entry_policy,
        recovery_policy=recovery_policy,
        diagnostic_severity=diagnostic_severity,
        v1_anchor=v1_anchor,
        details=details or {},
        input_hash=input_hash,
        closure_decision_id=decision_id,
    )


def _apply_previous_row_reuse(
    rows: list[V1LifecycleClosureRow],
    previous_table: Mapping[str, Any] | None,
) -> list[V1LifecycleClosureRow]:
    if not isinstance(previous_table, Mapping):
        return rows
    previous_rows = previous_table.get("rows")
    if not isinstance(previous_rows, list):
        return rows
    previous_by_key = {
        str(row.get("row_key") or ""): row
        for row in previous_rows
        if isinstance(row, Mapping)
    }
    reused: list[V1LifecycleClosureRow] = []
    for row in rows:
        previous = previous_by_key.get(row.row_key)
        if previous and previous.get("input_hash") == row.input_hash:
            reused.append(
                V1LifecycleClosureRow(
                    row_key=row.row_key,
                    phase=row.phase,
                    owner_id=row.owner_id,
                    evidence_class=row.evidence_class,
                    terminality=row.terminality,
                    entry_policy=row.entry_policy,
                    recovery_policy=row.recovery_policy,
                    diagnostic_severity=row.diagnostic_severity,
                    v1_anchor=row.v1_anchor,
                    details=row.details,
                    input_hash=row.input_hash,
                    closure_decision_id=str(
                        previous.get("closure_decision_id")
                        or row.closure_decision_id
                    ),
                )
            )
        else:
            reused.append(row)
    return reused


def _closure_payload(
    closure: Mapping[str, Any] | V1LifecycleClosureTable | None,
) -> dict[str, Any]:
    if isinstance(closure, V1LifecycleClosureTable):
        return closure.to_dict()
    if isinstance(closure, Mapping):
        return dict(closure)
    if hasattr(closure, "to_dict"):
        try:
            payload = closure.to_dict()
        except Exception:
            payload = {}
        return dict(payload) if isinstance(payload, Mapping) else {}
    return {}


def _closure_row(
    closure: Mapping[str, Any] | V1LifecycleClosureTable | None,
    *,
    phase: str,
    owner_id: str,
    row_key: str,
) -> Mapping[str, Any] | None:
    payload = _closure_payload(closure)
    rows = [
        row
        for row in _as_items(payload.get("rows", []))
        if isinstance(row, Mapping)
    ]
    if row_key:
        for row in rows:
            if str(row.get("row_key") or "") == row_key:
                return row
    phase_rows = [
        row
        for row in rows
        if not phase or str(row.get("phase") or "") == phase
    ]
    if owner_id:
        for row in phase_rows:
            if str(row.get("owner_id") or "") == owner_id:
                return row
        for row in phase_rows:
            if str(row.get("row_key") or "").endswith(f":{owner_id}"):
                return row
    if len(phase_rows) == 1:
        return phase_rows[0]
    return None


def _unmapped_event_kinds(events: Any) -> set[str]:
    unmapped: set[str] = set()
    for event in _as_items(events):
        kind = str(_get(event, "kind", "") or "")
        if kind and map_lifecycle_event_kind(kind) is None:
            unmapped.add(kind)
    return unmapped


def _state_collection_or_count(local_state: Any, key: str, count_key: str) -> tuple[Any, ...]:
    items = _state_collection(local_state, key)
    if items:
        return tuple(items)
    count = _int(_get(local_state, count_key, 0))
    if count <= 0:
        return ()
    return tuple({"count_only": True, "collection": key} for _ in range(count))


def _state_collection(local_state: Any, key: str) -> list[Any]:
    return _as_items(_get(local_state, key, []))


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _as_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        return list(value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value)
    return [value]


def _first_item(value: Any) -> Any:
    items = _as_items(value)
    return items[0] if items else {}


def _owner_id(obj: Any, *keys: str, default: str = "") -> str:
    for key in keys:
        value = _get(obj, key, None)
        if value:
            return str(value)
    return str(default or "")


def _symbol(obj: Any) -> str:
    return str(_get(obj, "symbol", "") or "").upper()


def _venue(obj: Any) -> str:
    value = _get(obj, "venue", "")
    if hasattr(value, "value"):
        value = value.value
    return str(value or "").lower()


def _venues(obj: Any) -> set[str]:
    result: set[str] = set()
    for key in ("venue", "long_venue", "short_venue", "maker_venue", "hedge_venue"):
        value = _get(obj, key, "")
        if hasattr(value, "value"):
            value = value.value
        text = str(value or "").lower()
        if text:
            result.add(text)
    return result


def _truth_available(exchange_truth: Mapping[str, Any] | None) -> bool:
    if not isinstance(exchange_truth, Mapping):
        return False
    if "truth_available" in exchange_truth:
        return bool(exchange_truth.get("truth_available"))
    return bool(exchange_truth.get("available"))


def _has_live_open_order(exchange_truth: Mapping[str, Any] | None, symbol: str) -> bool:
    for order in _exchange_open_orders(exchange_truth):
        if symbol and _symbol(order) != symbol:
            continue
        if _quantity(order) > EPSILON:
            return True
    return False


def _has_live_position(exchange_truth: Mapping[str, Any] | None, symbol: str) -> bool:
    for position in _exchange_positions(exchange_truth):
        if symbol and _symbol(position) != symbol:
            continue
        if _quantity(position) > EPSILON:
            return True
    return False


def _exchange_open_orders(exchange_truth: Mapping[str, Any] | None) -> list[Any]:
    return _flatten_exchange_collection(_get(exchange_truth, "open_orders", []))


def _exchange_positions(exchange_truth: Mapping[str, Any] | None) -> list[Any]:
    return _flatten_exchange_collection(_get(exchange_truth, "positions", []))


def _normalized_exchange_truth(
    exchange_truth: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(exchange_truth, Mapping):
        return None
    normalized = dict(exchange_truth)
    normalized["open_orders"] = _exchange_open_orders(exchange_truth)
    normalized["positions"] = _exchange_positions(exchange_truth)
    return normalized


def _flatten_exchange_collection(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        items: list[Any] = []
        for venue, venue_value in value.items():
            if isinstance(venue_value, Mapping):
                for symbol, symbol_value in venue_value.items():
                    if isinstance(symbol_value, Mapping):
                        item = dict(symbol_value)
                        item.setdefault("venue", venue)
                        item.setdefault("symbol", symbol)
                        items.append(item)
                    else:
                        for item in _as_items(symbol_value):
                            if isinstance(item, Mapping):
                                merged = dict(item)
                                merged.setdefault("venue", venue)
                                merged.setdefault("symbol", symbol)
                                items.append(merged)
                            else:
                                items.append(item)
            else:
                items.extend(_as_items(venue_value))
        return items
    return _as_items(value)


def _quantity(obj: Any) -> float:
    for key in ("quantity", "qty", "size"):
        value = _get(obj, key, None)
        if value is not None:
            return _float(value)
    return 0.0


def _float(value: Any) -> float:
    try:
        result = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if result != result:
        return 0.0
    return abs(result)


def _int(*values: Any) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            continue
    return 0


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "")
        if text:
            return text
    return ""


def _operator_fail_closed(local_state: Any) -> bool:
    operator = _get(local_state, "operator", None)
    requested = _get(operator, "requested_mode", None)
    if hasattr(requested, "value"):
        requested = requested.value
    return str(requested or "").lower() == "fail_closed"


def _max_severity(values: list[str]) -> str:
    order = {"info": 0, "warning": 1, "critical": 2}
    best = "info"
    for value in values:
        text = str(value or "info")
        if order.get(text, 0) > order.get(best, 0):
            best = text
    return best


def _enum_value(value: Any) -> str:
    if hasattr(value, "value"):
        return str(value.value)
    return str(value or "")


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _now_ms() -> int:
    return int(time.time() * 1000)


_EVENT_KIND_PHASES = {
    "entry.opened": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "runtime.position_opened": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "runtime.position_lifecycle_terminal": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "pending_entry.force_terminalized": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "exit.passive_close_fallback_terminal_flat": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.passive_close_recovery_probe_flat": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.passive_close_hedge_duplicate_client_order_reconciled": (
        V1LifecycleClosurePhase.PASSIVE_CLOSE.value
    ),
    "exit.passive_close_hedge_reconciled_after_error": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "execution.entry_residual_dust_tolerated": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    "execution.residual_repair_terminal": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    "execution.residual_repair_completed": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    "recovery.flat": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "recovery.residual_repairs_complete": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    "runtime.entry_quote_revalidate_targeted": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.entry_quote_revalidate_failed": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.entry_quote_revalidate_resolved": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.snapshot_fallback_last_good": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.current_state_heartbeat_loop_export_error": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
}

_EVENT_PREFIX_PHASES = (
    ("runtime.entry_ws_bbo_", V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value),
    ("runtime.entry_blocked_ws_bbo", V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value),
    ("runtime.snapshot_freshness", V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value),
    ("exit.passive_close_", V1LifecycleClosurePhase.PASSIVE_CLOSE.value),
    ("execution.residual_", V1LifecycleClosurePhase.RESIDUAL_REPAIR.value),
    ("execution.entry_residual_", V1LifecycleClosurePhase.RESIDUAL_REPAIR.value),
    ("recovery.", V1LifecycleClosurePhase.RECOVERY_TRUTH.value),
    ("runtime.current_state_", V1LifecycleClosurePhase.RUNTIME_PROGRESS.value),
)
