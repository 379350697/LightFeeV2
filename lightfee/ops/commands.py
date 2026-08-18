"""Operator control command implementations.

Rust references:
- src/engine/state.rs:769-868 (apply_operator_command with atomic persistence)
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable

from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.risk.operator import OperatorCommand, apply_operator_command


def _finite_nonnegative_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0.0 else None


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _binance_reduce_only(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.lower() == "true")


def _close_leg_quantity(
    reconciliation: dict[str, Any],
    leg: str,
    *,
    require_identity: bool,
) -> float | None:
    records = reconciliation.get(f"{leg}_legs")
    if not isinstance(records, list):
        return None
    quantities: list[float] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        has_identity = bool(record.get("order_id") or record.get("client_order_id"))
        if has_identity != require_identity:
            continue
        quantity = _finite_nonnegative_float(record.get("quantity"))
        if quantity is not None and quantity > 1e-12:
            quantities.append(quantity)
    return sum(quantities) if quantities else None


def _close_evidence_expected_quantity(
    reconciliation: dict[str, Any],
    snapshot: dict[str, Any],
    leg: str,
) -> tuple[float | None, str]:
    """Find a close quantity without treating a partial snapshot as one."""
    missing_record_quantity = _close_leg_quantity(
        reconciliation,
        leg,
        require_identity=False,
    )
    if missing_record_quantity is not None:
        return missing_record_quantity, "unidentified_close_leg_quantity"

    original_payload = reconciliation.get("original_payload")
    if isinstance(original_payload, dict):
        payload_quantity = _finite_nonnegative_float(
            original_payload.get(f"{leg}_closed_qty")
        )
        if payload_quantity is not None and payload_quantity > 1e-12:
            return payload_quantity, "recorded_close_quantity"

    opposite_leg = "short" if leg == "long" else "long"
    opposite_quantity = _close_leg_quantity(
        reconciliation,
        opposite_leg,
        require_identity=True,
    )
    if opposite_quantity is not None:
        return opposite_quantity, "opposite_identified_close_leg_quantity"

    if str(reconciliation.get("kind") or "final") == "final":
        snapshot_quantity = _finite_nonnegative_float(snapshot.get(f"{leg}_quantity"))
        if snapshot_quantity is not None and snapshot_quantity > 1e-12:
            return snapshot_quantity, "final_position_snapshot_quantity"
    return None, "missing_close_quantity"


def discover_binance_close_evidence_candidates(
    reconciliation: dict[str, Any],
    orders: Iterable[dict[str, Any]],
    *,
    time_window_ms: int = 300_000,
    quantity_relative_tolerance: float = 1e-9,
) -> dict[str, Any]:
    """Rank historical Binance fills for a missing close identity, read-only.

    This is an offline evidence-discovery aid, not a reconciliation or import
    path.  It requires a separately exported Binance ``allOrders`` response
    and never infers an accounting fact from the candidate.  A caller must
    still obtain exact execution evidence and use ``import-billing-evidence``
    for one explicit owner.
    """
    if not isinstance(reconciliation, dict):
        raise ValueError("reconciliation must be an object")
    try:
        parsed_time_window_ms = float(time_window_ms)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("time_window_ms must be a non-negative integer") from exc
    if (
        not math.isfinite(parsed_time_window_ms)
        or parsed_time_window_ms < 0.0
        or not parsed_time_window_ms.is_integer()
    ):
        raise ValueError("time_window_ms must be non-negative")
    time_window_ms = int(parsed_time_window_ms)
    try:
        quantity_relative_tolerance = float(quantity_relative_tolerance)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "quantity_relative_tolerance must be finite and non-negative"
        ) from exc
    if not math.isfinite(quantity_relative_tolerance) or quantity_relative_tolerance < 0.0:
        raise ValueError("quantity_relative_tolerance must be finite and non-negative")

    from lightfee.engine.state import pending_close_reconciliation_missing_legs

    snapshot = reconciliation.get("position_snapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    position_id = str(reconciliation.get("position_id") or snapshot.get("position_id") or "")
    symbol = str(reconciliation.get("symbol") or snapshot.get("symbol") or "")
    closed_at_ms = _positive_int(reconciliation.get("closed_at_ms"))
    if not position_id or not symbol or closed_at_ms is None:
        raise ValueError("reconciliation requires position_id, symbol, and positive closed_at_ms")

    order_rows = [dict(row) for row in orders if isinstance(row, dict)]
    legs: list[dict[str, Any]] = []
    for leg in pending_close_reconciliation_missing_legs(reconciliation):
        venue = str(
            reconciliation.get(f"{leg}_venue") or snapshot.get(f"{leg}_venue") or ""
        ).lower()
        expected_quantity, expected_quantity_source = _close_evidence_expected_quantity(
            reconciliation,
            snapshot,
            leg,
        )
        expected_side = "SELL" if leg == "long" else "BUY"
        item: dict[str, Any] = {
            "leg": leg,
            "venue": venue,
            "expected_side": expected_side,
            "expected_quantity": expected_quantity,
            "expected_quantity_source": expected_quantity_source,
            "time_window_ms": time_window_ms,
            "quantity_relative_tolerance": quantity_relative_tolerance,
            "candidate_count": 0,
            "candidates": [],
        }
        if venue != "binance":
            item["disposition"] = "unsupported_venue"
            legs.append(item)
            continue
        if expected_quantity is None or expected_quantity <= 1e-12:
            item["disposition"] = "missing_expected_quantity"
            legs.append(item)
            continue

        quantity_tolerance = max(expected_quantity * quantity_relative_tolerance, 1e-12)
        candidates: list[dict[str, Any]] = []
        for raw in order_rows:
            order_id = str(raw.get("orderId") or "").strip()
            client_order_id = str(raw.get("clientOrderId") or "").strip()
            executed_quantity = _finite_nonnegative_float(raw.get("executedQty"))
            updated_at_ms = _positive_int(raw.get("updateTime"))
            if (
                str(raw.get("symbol") or "") != symbol
                or str(raw.get("side") or "").upper() != expected_side
                or not _binance_reduce_only(raw.get("reduceOnly"))
                or str(raw.get("status") or "").upper() != "FILLED"
                or executed_quantity is None
                or executed_quantity <= 1e-12
                or updated_at_ms is None
                or not (order_id or client_order_id)
            ):
                continue
            quantity_delta = abs(executed_quantity - expected_quantity)
            time_delta_ms = updated_at_ms - closed_at_ms
            if quantity_delta > quantity_tolerance or abs(time_delta_ms) > time_window_ms:
                continue
            average_price = _finite_nonnegative_float(raw.get("avgPrice"))
            candidates.append(
                {
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                    "system_client_order_id": client_order_id.startswith("lfx"),
                    "executed_quantity": executed_quantity,
                    "average_price": average_price,
                    "updated_at_ms": updated_at_ms,
                    "time_delta_ms": time_delta_ms,
                    "quantity_delta": quantity_delta,
                }
            )
        candidates.sort(
            key=lambda candidate: (
                abs(int(candidate["time_delta_ms"])),
                float(candidate["quantity_delta"]),
                str(candidate["order_id"]),
                str(candidate["client_order_id"]),
            )
        )
        item["candidates"] = candidates
        item["candidate_count"] = len(candidates)
        item["disposition"] = (
            "unique_candidate_requires_operator_evidence"
            if len(candidates) == 1
            else "ambiguous_candidates"
            if candidates
            else "no_candidate"
        )
        legs.append(item)

    return {
        "schema_version": 1,
        "candidate_discovery_only": True,
        "automatically_importable": False,
        "operator_next_action": (
            "verify each candidate against exact exchange execution history and "
            "supply one audited evidence pack; do not import this output directly"
        ),
        "owner": {
            "position_id": position_id,
            "kind": str(reconciliation.get("kind") or "final"),
            "closed_at_ms": closed_at_ms,
        },
        "symbol": symbol,
        "legs": legs,
    }


def load_binance_order_history_export(path: Path) -> list[dict[str, Any]]:
    """Load a read-only Binance ``allOrders`` export for candidate discovery."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read Binance order-history export: {exc}") from exc
    rows = raw.get("orders") if isinstance(raw, dict) else raw
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(
            "Binance order-history export must be a JSON list or {'orders': [...]} object"
        )
    return [dict(row) for row in rows]


def execute_operator_command(
    command: OperatorCommand,
    current_risk: GlobalRiskMode,
    current_lifecycle: EngineLifecycle,
    has_blocking_recovery: bool = False,
    journal: object = None,
    state: object = None,
) -> tuple[GlobalRiskMode, EngineLifecycle, str]:
    """Execute an operator command and return updated state + message.

    V1 parity: when journal and state are provided, the risk/lifecycle
    transition is made durable via append_critical before returning.
    This prevents command loss on crash (V1 state.rs:769-868).
    """
    new_risk, new_lifecycle = apply_operator_command(
        command, current_risk, current_lifecycle, has_blocking_recovery
    )

    # V1: atomic persistence — journal critical event + persist state
    if journal is not None and state is not None:
        from lightfee.engine.bootstrap import wall_clock_now_ms
        journal.append_critical(
            wall_clock_now_ms(),
            "ops.command_applied",
            {
                "command": command.value if hasattr(command, 'value') else str(command),
                "previous_risk": current_risk.value,
                "new_risk": new_risk.value,
                "previous_lifecycle": current_lifecycle.value,
                "new_lifecycle": new_lifecycle.value,
            },
        )
        state.risk_mode = new_risk
        state.lifecycle = new_lifecycle

        # V1: operator-requested mode latch prevents recovery-core auto-release
        # across restarts (state.rs:476-487).
        if command == OperatorCommand.FAIL_CLOSED:
            state.operator.requested_mode = GlobalRiskMode.FAIL_CLOSED
        elif command == OperatorCommand.RESUME_IF_SAFE and new_risk == GlobalRiskMode.RUNNING:
            state.operator.requested_mode = None

    messages = {
        OperatorCommand.PAUSE_ENTRY: "Entries paused",
        OperatorCommand.REDUCE_ONLY: "Entered reduce-only mode",
        OperatorCommand.FAIL_CLOSED: "Entered fail-closed mode",
        OperatorCommand.RECONCILE_NOW: "Reconciliation triggered",
        OperatorCommand.RESUME_IF_SAFE: (
            "Resumed" if new_risk == GlobalRiskMode.RUNNING else "Cannot resume: unsafe or blocking recovery"
        ),
    }

    return new_risk, new_lifecycle, messages.get(command, "Command executed")


def load_billing_evidence_import(path: Path) -> dict[str, Any]:
    """Load one auditable, operator-supplied close-accounting evidence pack."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read billing evidence file: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("billing evidence file must contain one JSON object")
    if raw.get("schema_version") != 1:
        raise ValueError("billing evidence schema_version must be 1")
    if not isinstance(raw.get("reconciliation"), dict):
        raise ValueError("billing evidence reconciliation must be an object")
    if not str(raw.get("evidence_reference") or "").strip():
        raise ValueError("billing evidence evidence_reference is required")
    return raw


def execute_billing_evidence_import(
    evidence: dict[str, Any],
    *,
    journal: object,
    state: object,
    now_ms: int,
) -> str:
    """Durably import one debt replacement for normal exchange reconciliation.

    The journal event is the durable authority.  The caller persists the
    resulting state immediately afterwards; replay applies this same event
    through the state-level import gate if a process stops in between.
    """
    if journal is None or state is None:
        raise ValueError("journal and state are required for evidence import")
    if not isinstance(evidence, dict):
        raise ValueError("billing evidence must be an object")
    reference = str(evidence.get("evidence_reference") or "").strip()
    reconciliation = evidence.get("reconciliation")
    if not isinstance(reconciliation, dict):
        raise ValueError("billing evidence reconciliation must be an object")
    canonical = json.dumps(
        evidence,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()
    imported = state.import_pending_close_reconciliation_evidence(
        reconciliation,
        evidence_reference=reference,
        evidence_sha256=digest,
        imported_at_ms=now_ms,
    )
    journal.append_critical(
        now_ms,
        "exit.billing_evidence_imported",
        {
            "position_id": str(imported.get("position_id") or ""),
            "symbol": str(imported.get("symbol") or ""),
            "kind": str(imported.get("kind") or "final"),
            "closed_at_ms": int(imported.get("closed_at_ms") or 0),
            "evidence_reference": reference,
            "evidence_sha256": digest,
            "reconciliation": imported,
        },
    )
    return "Billing evidence imported; awaiting exact exchange fill reconciliation"
