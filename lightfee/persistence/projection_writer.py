"""Idempotent journal-to-structured-store projection writer.

Consumes journal records in seq order and writes normalized facts into SQLite
fact tables. Uses (seq, kind) as the deduplication anchor so re-processing the
same journal range is safe.

Journal-only event kinds (recovery, lifecycle, state reconciliation) are
intentionally skipped — they stay in the journal for exact replay semantics.
"""

from __future__ import annotations

import json
import time
from typing import Any

from lightfee.persistence.metrics import PersistenceMetrics
from lightfee.persistence.sqlite_store import SqliteStore


# ---------------------------------------------------------------------------
# Event kind classification
# ---------------------------------------------------------------------------

_ORDER_KINDS = frozenset({
    "order.submitted",
    "order.filled",
    "order.rejected",
    "order.uncertain",
})

_ENTRY_EXIT_KINDS = frozenset({
    "entry.opened",
    "exit.closed",
})

_RISK_KINDS = frozenset({
    "risk.warning_triggered",
    "risk.warning_cleared",
    "risk.death_triggered",
    "risk.single_side_protection_triggered",
    "risk.single_side_protection_failed",
    "risk.single_side_protection_unavailable",
})

_LOCAL_L2_HEALTH_KINDS = frozenset({
    "runtime.local_l2_sequence_gap",
    "runtime.local_l2_sync_failed",
})

_DIAGNOSTIC_KINDS = frozenset({
    "scan.no_entry_diagnostics",
    "scan.runtime_gate_blocked",
    "execution.entry_liquidity_blocked",
})

_FAIL_CLOSED_PREFIX = "runtime.fail_closed"

# These stay journal-only — never projected
_JOURNAL_ONLY_KINDS = frozenset({
    "recovery.live_detected",
    "recovery.flat",
    "recovery.blocked",
    "recovery.mismatch_detected",
    "recovery.mismatch_flattened",
    "recovery.resumed",
    "runtime.lifecycle_changed",
    "runtime.risk_mode_changed",
    "runtime.booting",
    "runtime.running",
    "runtime.stopped",
})

PROJECTED_KINDS = (
    _ORDER_KINDS
    | _ENTRY_EXIT_KINDS
    | _RISK_KINDS
    | _LOCAL_L2_HEALTH_KINDS
    | _DIAGNOSTIC_KINDS
)


def _is_fail_closed(kind: str) -> bool:
    return kind.startswith(_FAIL_CLOSED_PREFIX)


def is_projected_kind(kind: str) -> bool:
    """Return True if this journal event kind should be projected into a fact table."""
    return kind in PROJECTED_KINDS or _is_fail_closed(kind)


def is_journal_only_kind(kind: str) -> bool:
    """Return True if this kind must stay in the journal and never be projected."""
    return kind in _JOURNAL_ONLY_KINDS


# ---------------------------------------------------------------------------
# Projection writer
# ---------------------------------------------------------------------------

class ProjectionWriter:
    """Consumes journal records and writes idempotent fact rows into SQLite."""

    def __init__(self, store: SqliteStore, metrics: PersistenceMetrics | None = None) -> None:
        self._store = store
        self._metrics = metrics

    # ------ public API ------

    def project_records(
        self,
        conn: Any,
        records: list[dict[str, Any]],
    ) -> dict[str, int]:
        """Project a batch of journal records into fact tables.

        Returns counts: {appended, skipped, failed}
        """
        appended = 0
        skipped = 0
        failed = 0
        max_seq = 0
        max_ts = 0

        for record in records:
            kind: str = record.get("kind", "")
            seq: int = record.get("seq", 0)
            ts_ms: int = record.get("ts_ms", 0)
            payload: dict = record.get("payload", {})

            if seq > max_seq:
                max_seq = seq
                max_ts = ts_ms

            try:
                if kind in _ORDER_KINDS:
                    ok = self._project_order(conn, seq, ts_ms, kind, payload)
                elif kind in _ENTRY_EXIT_KINDS:
                    ok = self._project_entry_exit(conn, seq, ts_ms, kind, payload)
                elif kind in _RISK_KINDS:
                    ok = self._project_risk(conn, seq, ts_ms, kind, payload)
                elif kind in _LOCAL_L2_HEALTH_KINDS:
                    ok = self._project_local_l2_health(conn, seq, ts_ms, kind, payload)
                elif kind in _DIAGNOSTIC_KINDS or _is_fail_closed(kind):
                    ok = self._project_diagnostic(conn, seq, ts_ms, kind, payload)
                else:
                    # Journal-only kind — intentionally skip
                    continue

                if ok:
                    appended += 1
                    if self._metrics:
                        self._metrics.record_projection_append(seq, ts_ms)
                else:
                    skipped += 1
                    if self._metrics:
                        self._metrics.record_projection_skip()
            except Exception:
                failed += 1
                if self._metrics:
                    self._metrics.record_projection_failure()

        # Update cursor
        if max_seq > 0:
            cursor = self._store.get_projection_cursor(conn)
            self._store.upsert_projection_cursor(
                conn,
                last_projected_seq=max(
                    cursor["last_projected_seq"], max_seq
                ),
                last_projected_at_ms=max(
                    cursor["last_projected_at_ms"], max_ts
                ),
                total_facts_written=cursor["total_facts_written"] + appended,
                total_failures=cursor["total_failures"] + failed,
            )

        return {"appended": appended, "skipped": skipped, "failed": failed}

    def project_all(
        self,
        conn: Any,
        records: list[dict[str, Any]],
    ) -> dict[str, int]:
        """Project all records — convenience alias for project_records."""
        return self.project_records(conn, records)

    def get_cursor(self, conn: Any) -> dict:
        """Return current projection cursor state."""
        return self._store.get_projection_cursor(conn)

    # ------ private projectors ------

    def _project_order(
        self, conn: Any, seq: int, ts_ms: int, kind: str, payload: dict
    ) -> bool:
        filled = kind == "order.filled"
        failed = kind in ("order.rejected", "order.uncertain")
        return self._store.insert_order_fact(
            conn,
            seq=seq,
            ts_ms=ts_ms,
            kind=kind,
            venue=payload.get("venue", "unknown"),
            symbol=payload.get("symbol", ""),
            filled=filled,
            failed=failed,
            latency_ms=payload.get("latency_ms", 0) if filled else 0,
            fee_quote=float(payload.get("fee_quote", 0)) if filled else 0.0,
            payload_json=json.dumps(payload, ensure_ascii=False),
        )

    def _project_entry_exit(
        self, conn: Any, seq: int, ts_ms: int, kind: str, payload: dict
    ) -> bool:
        return self._store.insert_entry_exit_fact(
            conn,
            seq=seq,
            ts_ms=ts_ms,
            kind=kind,
            symbol=payload.get("symbol", ""),
            entry_fee_quote=float(payload.get("entry_fee_quote", 0)),
            exit_fee_quote=float(payload.get("exit_fee_quote", 0)),
            net_quote=float(payload.get("net_quote", 0)),
            payload_json=json.dumps(payload, ensure_ascii=False),
        )

    def _project_risk(
        self, conn: Any, seq: int, ts_ms: int, kind: str, payload: dict
    ) -> bool:
        return self._store.insert_risk_counter_fact(
            conn,
            seq=seq,
            ts_ms=ts_ms,
            kind=kind,
            counter_value=1,
            payload_json=json.dumps(payload, ensure_ascii=False),
        )

    def _project_local_l2_health(
        self, conn: Any, seq: int, ts_ms: int, kind: str, payload: dict
    ) -> bool:
        if kind == "runtime.local_l2_sequence_gap":
            reason = payload.get("continuity_reason", "unspecified")
            category = "sequence_gap"
        else:
            reason = payload.get("failure_category", "unspecified")
            category = "sync_failed"
        return self._store.insert_local_l2_health_fact(
            conn,
            seq=seq,
            ts_ms=ts_ms,
            kind=kind,
            reason=reason,
            category=category,
            venue=payload.get("venue", ""),
            payload_json=json.dumps(payload, ensure_ascii=False),
        )

    def _project_diagnostic(
        self, conn: Any, seq: int, ts_ms: int, kind: str, payload: dict
    ) -> bool:
        reason = payload.get("reason", "unspecified")
        if kind == "execution.entry_liquidity_blocked":
            classification = payload.get("eligibility_class", "")
        elif kind.startswith("runtime.fail_closed"):
            classification = payload.get("reason", "unspecified")
        else:
            classification = ""
        return self._store.insert_diagnostic_fact(
            conn,
            seq=seq,
            ts_ms=ts_ms,
            kind=kind,
            reason=reason,
            classification=classification,
            payload_json=json.dumps(payload, ensure_ascii=False),
        )
