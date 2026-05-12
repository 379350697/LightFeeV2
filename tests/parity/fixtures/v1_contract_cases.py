"""Shared fixture builders for V1 semantic parity tests.

Later workers (A-F) import these to construct synthetic V1 and V2 journal
records and compare semantic summaries instead of raw line-by-line code.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ── Journal record builders ──────────────────────────────────────────────


def make_v1_journal_record(kind: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a synthetic V1 journal record matching V1's envelope shape."""
    return {
        "seq": 0,  # caller should assign
        "run_id": str(uuid.uuid4()),
        "ts": time.time(),
        "kind": kind,
        "payload": payload or {},
        "source": "v1",
    }


def make_v2_journal_record(kind: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a synthetic V2 journal record matching V2's envelope shape."""
    return {
        "seq": 0,  # caller should assign
        "run_id": str(uuid.uuid4()),
        "ts": time.time(),
        "kind": kind,
        "payload": json.dumps(payload or {}),
        "source": "v2",
    }


# ── Semantic summary ─────────────────────────────────────────────────────


@dataclass
class SemanticSummary:
    """A reduced representation of journal records for cross-version comparison.

    This is what "semantic parity" means: two runs (V1 and V2) with the same
    business inputs should produce semantically equivalent summaries, even if
    the raw record shapes differ.
    """

    event_counts: dict[str, int] = field(default_factory=dict)
    position_ids: set[str] = field(default_factory=set)
    lifecycle_transitions: list[str] = field(default_factory=list)
    risk_mode_changes: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_counts": dict(sorted(self.event_counts.items())),
            "position_ids": sorted(self.position_ids),
            "lifecycle_transitions": list(self.lifecycle_transitions),
            "risk_mode_changes": list(self.risk_mode_changes),
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


def semantic_summary(records: list[dict[str, Any]]) -> SemanticSummary:
    """Reduce a list of journal records (V1 or V2 shape) to a SemanticSummary."""
    summary = SemanticSummary()

    for rec in records:
        kind = rec.get("kind", "unknown")
        summary.event_counts[kind] = summary.event_counts.get(kind, 0) + 1

        payload = rec.get("payload", {})
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                payload = {}

        # Extract position ids
        pos_id = payload.get("position_id") or payload.get("review_id")
        if pos_id:
            summary.position_ids.add(str(pos_id))

        # Track lifecycle transitions
        if kind == "lifecycle_transition":
            summary.lifecycle_transitions.append(
                f"{payload.get('from', '?')}->{payload.get('to', '?')}"
            )

        # Track risk mode changes
        if kind == "risk_mode_changed":
            summary.risk_mode_changes.append(
                f"{payload.get('from', '?')}->{payload.get('to', '?')}"
            )

        # Collect errors and warnings
        if kind.endswith("_error") or kind == "fail_closed":
            summary.errors.append(f"{kind}: {payload.get('reason', 'unknown')}")
        if kind.endswith("_warning") or kind.endswith("_degraded"):
            summary.warnings.append(f"{kind}: {payload.get('reason', 'unknown')}")

    return summary


# ── Semantic equivalence assertion ────────────────────────────────────────


def assert_semantic_equivalence(
    v1_records: list[dict[str, Any]],
    v2_records: list[dict[str, Any]],
    allow_extra_v2_events: bool = True,
) -> SemanticSummary:
    """Assert V1 and V2 record sets are semantically equivalent.

    Equivalence means:
    - Every V1 event kind present in V1 is also present in V2.
    - Every V1 position id is also in V2.
    - Lifecycle and risk-mode transitions match in order.

    Set `allow_extra_v2_events=True` (default) to allow V2 to emit
    additional event kinds that V1 did not have (e.g. v2-native diagnostics).
    """
    v1_summary = semantic_summary(v1_records)
    v2_summary = semantic_summary(v2_records)

    # Check V1 event kinds are all in V2
    for kind in v1_summary.event_counts:
        assert kind in v2_summary.event_counts, (
            f"V1 event kind '{kind}' ({v1_summary.event_counts[kind]} occurrences) "
            f"not found in V2 records"
        )

    # Check position ids
    missing_positions = v1_summary.position_ids - v2_summary.position_ids
    assert not missing_positions, (
        f"V1 position ids not found in V2: {sorted(missing_positions)}"
    )

    # Check lifecycle transitions match
    assert v1_summary.lifecycle_transitions == v2_summary.lifecycle_transitions, (
        f"Lifecycle transition mismatch:\n  V1: {v1_summary.lifecycle_transitions}\n"
        f"  V2: {v2_summary.lifecycle_transitions}"
    )

    # Check risk mode changes match
    assert v1_summary.risk_mode_changes == v2_summary.risk_mode_changes, (
        f"Risk mode mismatch:\n  V1: {v1_summary.risk_mode_changes}\n"
        f"  V2: {v2_summary.risk_mode_changes}"
    )

    return v2_summary
