"""Operator control command implementations.

Rust references:
- src/engine/state.rs:769-868 (apply_operator_command with atomic persistence)
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.risk.operator import OperatorCommand, apply_operator_command


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

        # V1: operator-requested mode latch prevents auto-clear on clean restart.
        # clear_stale_fail_closed_if_recovery_clean() checks this to preserve
        # operator-intended FAIL_CLOSED across restarts (state.rs:476-487).
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
