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
    LIVE_ARTIFACT_BLOCK_REASONS,
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
    pending_passive_close_evidence,
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
    state = _local_state_with_open_positions_alias(local_state)
    truth = _normalized_exchange_truth(exchange_truth)
    ledger = recovery_ledger or RecoveryLedger.from_local_and_exchange_truth(
        local=state,
        exchange_truth=truth or {},
        owner_index=owner_index,
    )
    decision = recovery_decision or V1RecoveryDecisionCore().decide(
        RecoveryEvidenceSnapshot(
            local_open_positions=_state_collection_or_count(
                state, "open_positions", "open_position_count"
            ),
            pending_entries=_state_collection_or_count(
                state, "pending_entries", "pending_entry_count"
            ),
            residual_repairs=_state_collection_or_count(
                state, "pending_residual_repairs", "pending_residual_repair_count"
            ),
            passive_closes=pending_passive_close_evidence(state),
            exchange_truth=truth,
            prior_recovery_block_reason=_get(state, "recovery_blocked_reason"),
            recovery_work_items=tuple(ledger.work_items),
            operator_fail_closed=_operator_fail_closed(state),
        )
    )

    rows: list[V1LifecycleClosureRow] = []
    rows.append(_recovery_truth_row(decision))
    rows.extend(_pending_entry_rows(state, truth))
    rows.extend(_open_position_rows(state, ledger))
    rows.extend(_recovery_work_rows(ledger))
    rows.extend(_passive_close_rows(state))
    rows.extend(_residual_repair_rows(state))
    rows.extend(_runtime_progress_rows(state, generated))

    rows = _apply_previous_row_reuse(rows, previous_table)
    unmapped = tuple(sorted(_unmapped_event_kinds(events)))
    performance_scope = _performance_scope(state, rows)
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
        live_long_quantity, live_short_quantity = _pending_entry_live_leg_quantities(
            exchange_truth,
            pending,
            symbol,
        )
        live_balanced_quantity = min(live_long_quantity, live_short_quantity)
        decision = PendingEntryTerminalizer().decide(
            pending,
            live_truth=PendingEntryLiveTruth(
                available=truth_available,
                has_live_open_order=has_live_order,
                has_live_position=has_live_position,
                error="" if truth_available else "exchange_truth_unavailable",
                positive_fill_requires_live_position=True,
                live_long_quantity=live_long_quantity,
                live_short_quantity=live_short_quantity,
                live_balanced_quantity=live_balanced_quantity,
            ),
        )
        if decision.outcome == "deferred_live_open_order":
            terminality = "retain_live_open_order"
        elif decision.outcome == "deferred_live_position":
            terminality = "retain_live_position"
        elif decision.outcome == "deferred_missing_live_truth":
            terminality = "retain_missing_live_truth"
        elif decision.outcome == "positive_fill_live_truth_conflict":
            terminality = decision.outcome
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
                    "venues": sorted(_venues(pending)),
                    "decision_outcome": decision.outcome,
                    "decision_reason": decision.reason,
                    "matched_quantity": decision.matched_quantity,
                    "residual_quantity": decision.residual_quantity,
                    "has_live_open_order": has_live_order,
                    "has_live_position": has_live_position,
                    "live_long_quantity": live_long_quantity,
                    "live_short_quantity": live_short_quantity,
                    "live_balanced_quantity": live_balanced_quantity,
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
        if kind not in LIVE_ARTIFACT_BLOCK_REASONS:
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


def _recovery_work_rows(ledger: RecoveryLedger) -> list[V1LifecycleClosureRow]:
    rows: list[V1LifecycleClosureRow] = []
    for item in ledger.work_items:
        kind = str(_get(item, "kind", "") or "")
        if kind in LIVE_ARTIFACT_BLOCK_REASONS | {
            "ambiguous_exchange_truth",
            "owned_open_position",
        }:
            continue
        owner = _get(item, "owner", None)
        owner_id = _owner_id(
            owner,
            "owner_id",
            default=_owner_id(
                _first_item(_get(item, "artifacts", ())),
                "order_id",
                "client_order_id",
                default=_symbol(item) or kind,
            ),
        )
        phase = _recovery_work_phase(kind)
        blocking = bool(_get(item, "blocking", True))
        rows.append(
            _row(
                row_key=f"recovery_work:{kind}:{owner_id}",
                phase=phase,
                owner_id=owner_id,
                evidence_class=kind,
                terminality=kind,
                entry_policy=(
                    "block_conflicting_new_risk"
                    if blocking
                    else "allow_new_risk_background_work"
                ),
                recovery_policy=(
                    f"manage_{kind}" if blocking else f"background_{kind}"
                ),
                diagnostic_severity="warning",
                v1_anchor=(
                    "RecoveryLedger work scope feeds runtime entry gate"
                    if blocking
                    else "V1 background reconciliation remains visible without "
                    "blocking new execution"
                ),
                details={
                    "kind": kind,
                    "blocking": blocking,
                    "symbol": _get(item, "symbol", ""),
                    "venues": sorted(_get(item, "venues", ()) or ()),
                    "decision_reason": _get(_get(item, "decision", None), "reason", ""),
                },
            )
        )
    return rows


def _recovery_work_phase(kind: str) -> V1LifecycleClosurePhase:
    if kind == "pending_passive_close":
        return V1LifecycleClosurePhase.PASSIVE_CLOSE
    if kind == "pending_residual_repair":
        return V1LifecycleClosurePhase.RESIDUAL_REPAIR
    if kind == "owned_pending_entry":
        return V1LifecycleClosurePhase.PENDING_ENTRY
    return V1LifecycleClosurePhase.RECOVERY_TRUTH


def _passive_close_rows(local_state: Any) -> list[V1LifecycleClosureRow]:
    rows: list[V1LifecycleClosureRow] = []
    for key in ("pending_closes", "pending_passive_closes"):
        for close in _state_collection(local_state, key):
            owner_id = _owner_id(close, "position_id", "close_id", default=_symbol(close) or key)
            venues = _venues(close) | _venues(_get(close, "position_snapshot", {}))
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
                    details={"source": key, "symbol": _symbol(close), "venues": sorted(venues)},
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
    items = _as_items(_get(local_state, key, []))
    if items:
        return items
    if key == "open_positions":
        return _as_items(_get(local_state, "positions", []))
    return items


def _local_state_with_open_positions_alias(local_state: Any) -> Any:
    if not isinstance(local_state, Mapping):
        return local_state
    if _as_items(local_state.get("open_positions", [])):
        return local_state
    positions = _as_items(local_state.get("positions", []))
    if not positions:
        return local_state
    normalized = dict(local_state)
    normalized["open_positions"] = positions
    return normalized


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
    symbol = str(_get(obj, "symbol", "") or "").upper()
    if symbol:
        return symbol
    return str(_get(_get(obj, "position_snapshot", {}), "symbol", "") or "").upper()


def _venue(obj: Any) -> str:
    return _venue_text(_get(obj, "venue", ""))


def _venue_text(value: Any) -> str:
    if hasattr(value, "value"):
        value = value.value
    return str(value or "").lower()


def _side_text(value: Any) -> str:
    if hasattr(value, "value"):
        value = value.value
    text = str(value or "").lower()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    if text.endswith("buy"):
        return "buy"
    if text.endswith("sell"):
        return "sell"
    return text


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


def _pending_entry_live_leg_quantities(
    exchange_truth: Mapping[str, Any] | None,
    pending: Any,
    symbol: str,
) -> tuple[float, float]:
    long_venue = _venue_text(_get(pending, "long_venue", ""))
    short_venue = _venue_text(_get(pending, "short_venue", ""))
    live_long = 0.0
    live_short = 0.0
    for position in _exchange_positions(exchange_truth):
        if symbol and _symbol(position) != symbol:
            continue
        quantity = _quantity(position)
        if quantity <= EPSILON:
            continue
        venue = _venue(position)
        side = _side_text(_get(position, "side", ""))
        if venue == long_venue and side == "buy":
            live_long = max(live_long, quantity)
        elif venue == short_venue and side == "sell":
            live_short = max(live_short, quantity)
    return live_long, live_short


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
    "runtime.booting": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.running": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.started": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.stopped": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.shutdown_stage": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.private_ws_started": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.private_ws_stopped": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.local_l2_phase_start": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.local_l2_phase_complete": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.live_scan_revalidate_required": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.live_scan_recovery_warmup": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "runtime.order_quote_stale_health_summary": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.reconciling": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "runtime.recovery_block_reconcile_attempt": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "runtime.recovery_fail_closed": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "runtime.recovery_awaiting_account_truth": (
        V1LifecycleClosurePhase.RECOVERY_TRUTH.value
    ),
    "runtime.risk_mode_changed": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "runtime.stale_fail_closed_cleared": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    # Risk warnings are observability records for an existing position, not
    # lifecycle transitions.  Keeping them in the open-position projection
    # prevents the auditor from treating a normal risk snapshot as unmapped.
    "risk.warning_triggered": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "risk.warning_cleared": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "scan.no_entry_diagnostics": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "scan.strategy_shortlist_ready": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "scan.shortlist_ready": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "startup.order_path_preflight": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "startup.trading_preflight": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
    "entry.pending_registered": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "entry.opened": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "runtime.entry_admission_venue_degraded": (
        V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value
    ),
    "runtime.entry_admission_venue_recovered": (
        V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value
    ),
    "runtime.entry_owner_claimed": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "runtime.entry_owner_handoff_complete": (
        V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value
    ),
    "runtime.position_opened": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "runtime.position_drift_corrected": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "runtime.position_drift_skipped_passive_close_owner": (
        V1LifecycleClosurePhase.PASSIVE_CLOSE.value
    ),
    "runtime.position_lifecycle_terminal": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "pending_entry.force_terminalized": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "exit.accepted_order_truth_gap_registered": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.accepted_order_truth_gap_resolved": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.accepted_order_truth_gap_superseded": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.pending_close_reconciliation_registered": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.close_order_identity_acknowledged": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.close_order_intent_claimed": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.passive_close_registered": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.retry_wait": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.close_chunk_submitted": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.close_residual_detected": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    # This event is emitted only after passive close creates durable residual
    # repair work.  It must outrank the broad `exit.passive_close_` prefix.
    "exit.passive_close_residual_detected": (
        V1LifecycleClosurePhase.RESIDUAL_REPAIR.value
    ),
    "exit.closed": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.billing_evidence_unavailable": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.billing_evidence_debt_registered": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.billing_evidence_pending": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.reconciliation_abandoned": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.billing_unreconciled": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.compensated": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.reconciled": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.passive_close_fallback_terminal_flat": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.passive_close_recovery_probe_flat": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.passive_close_hedge_duplicate_client_order_reconciled": (
        V1LifecycleClosurePhase.PASSIVE_CLOSE.value
    ),
    "exit.passive_close_hedge_confirmed_after_ack": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.passive_close_hedge_reconciled_after_error": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "exit.passive_close_terminal_zero_qty_reduce_only_evidence": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "execution.entry_residual_dust_tolerated": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    "execution.residual_repair_terminal": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    "execution.residual_repair_completed": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    "recovery.external_pair_flat_observed": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "recovery.external_pair_flat_reclassified": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "recovery.flat": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "recovery.residual_repairs_complete": V1LifecycleClosurePhase.RESIDUAL_REPAIR.value,
    "runtime.entry_quote_revalidate_targeted": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.entry_quote_revalidate_failed": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.entry_quote_revalidate_resolved": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.entry_quote_rewarm_scheduled_after_rest_stale": (
        V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value
    ),
    "runtime.entry_blocked_gate": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "runtime.last_good_revalidated_by_entry_quote_truth": (
        V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value
    ),
    "runtime.order_quote_stale_skipped": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.quote_stale": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.snapshot_fallback_last_good": V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value,
    "runtime.candidate_symbol_skipped": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "runtime.candidates_tradeable": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "runtime.tradeable_candidates_catalog_filtered": (
        V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value
    ),
    "runtime.perp_liquidity_stale_advisory": (
        V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value
    ),
    "runtime.entry_oi_targeted_refresh_started": (
        V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value
    ),
    "runtime.entry_oi_targeted_refresh_resolved": (
        V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value
    ),
    "runtime.entry_oi_targeted_refresh_failed": (
        V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value
    ),
    "execution.entry_liquidity_advisory": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "execution.entry_liquidity_blocked": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "execution.entry_leverage_ready": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "execution.entry_leverage_unavailable": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "execution.direction_drift_blocked": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "execution.entry_quantity_plan": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "execution.entry_selected": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "execution.hedge_deadline_started": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "execution.hedge_deadline_breached": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "runtime.entry_post_only_l2_repriced": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "runtime.entry_blocked_post_only_l2": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "runtime.entry_post_only_reject_cooldown": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "execution.passive_small_fill_buffering": (
        V1LifecycleClosurePhase.PENDING_ENTRY.value
    ),
    "execution.passive_small_fill_buffer_expired": (
        V1LifecycleClosurePhase.PENDING_ENTRY.value
    ),
    "execution.passive_cycle_zero_fill": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "execution.passive_phase_switched": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "entry.abort_maker_cancel_requested": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "entry.cleanup_leg_exposure": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "entry.aborted": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "entry.passive_unfilled": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "reconciliation.entry_abandoned_flat": (
        V1LifecycleClosurePhase.PENDING_ENTRY.value
    ),
    "reconciliation.entry_resolved": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "reconciliation.entry_abandon_retained_unresolved_maker": (
        V1LifecycleClosurePhase.PENDING_ENTRY.value
    ),
    "reconciliation.entry_flat_not_found_terminal_cleared": (
        V1LifecycleClosurePhase.PENDING_ENTRY.value
    ),
    "reconciliation.entry_flat_unresolved_maker_retained": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "entry.opportunity_funnel": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "review.candidate_rejected": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "review.candidate_shortlisted": V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value,
    "runtime.active_position_tick": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "runtime.close_price_evidence_fallback": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "runtime.close_price_evidence_stale": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "runtime.passive_close_deadline_fallback_armed": (
        V1LifecycleClosurePhase.PASSIVE_CLOSE.value
    ),
    "runtime.entry_dispatched": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "runtime.funding_capture_state_updated": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "runtime.normal_close_routing_passive": V1LifecycleClosurePhase.PASSIVE_CLOSE.value,
    "runtime.pending_entry_registered": V1LifecycleClosurePhase.PENDING_ENTRY.value,
    "runtime.position_drift_correction_verified": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "runtime.position_drift_detected": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "runtime.position_drift_flatten_leg": V1LifecycleClosurePhase.OPEN_POSITION.value,
    "runtime.reconciling_complete": V1LifecycleClosurePhase.RECOVERY_TRUTH.value,
    "runtime.current_state_heartbeat_loop_export_error": V1LifecycleClosurePhase.RUNTIME_PROGRESS.value,
}

_EVENT_PREFIX_PHASES = (
    ("runtime.account_fee_snapshot_", V1LifecycleClosurePhase.RUNTIME_PROGRESS.value),
    ("runtime.entry_local_l2_", V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value),
    ("runtime.local_l2_", V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value),
    ("runtime.snapshot_freshness", V1LifecycleClosurePhase.ENTRY_QUOTE_LEASE.value),
    ("order.", V1LifecycleClosurePhase.DIAGNOSTIC_ONLY.value),
    ("passive_maintenance.", V1LifecycleClosurePhase.PENDING_ENTRY.value),
    ("pending_entry.", V1LifecycleClosurePhase.PENDING_ENTRY.value),
    ("exit.passive_close_", V1LifecycleClosurePhase.PASSIVE_CLOSE.value),
    ("execution.residual_", V1LifecycleClosurePhase.RESIDUAL_REPAIR.value),
    ("execution.entry_residual_", V1LifecycleClosurePhase.RESIDUAL_REPAIR.value),
    ("recovery.", V1LifecycleClosurePhase.RECOVERY_TRUTH.value),
    ("runtime.current_state_", V1LifecycleClosurePhase.RUNTIME_PROGRESS.value),
)
