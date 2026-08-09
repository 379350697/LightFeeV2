"""Idempotent journal-to-structured-store projection writer.

Consumes journal records in seq order and writes normalized facts into SQLite
fact tables. Uses (seq, kind) as the deduplication anchor so re-processing the
same journal range is safe.

Journal-first event kinds (recovery, lifecycle, state reconciliation) stay in
the journal for exact replay semantics. Selected recovery/terminal events may
also receive rebuildable lifecycle-ledger rows for attribution.
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
    "exit.billing_evidence_unavailable",
    "exit.billing_evidence_debt_registered",
})

_LEDGER_POSITION_OPEN_KINDS = frozenset({
    "entry.opened",
    "recovery.live_detected",
})

_LEDGER_POSITION_CLOSE_KINDS = frozenset({
    "exit.closed",
    "exit.billing_evidence_unavailable",
    "recovery.flat",
    "runtime.position_lifecycle_terminal",
})

_LEDGER_ORDER_KINDS = frozenset({
    "order.submitted",
    "order.filled",
    "order.rejected",
    "order.uncertain",
})

_LEDGER_COMPENSATION_KINDS = frozenset({
    "entry.compensated",
    "exit.compensated",
    "execution.compensation_failed",
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

# These stay journal-first. Selected recovery kinds still get rebuildable
# lifecycle-ledger rows, but never drive runtime recovery from the projection.
_JOURNAL_ONLY_KINDS = frozenset({
    "pending_entry.viability_blocked",
    "recovery.live_detected",
    "recovery.flat",
    "recovery.blocked",
    "recovery.mismatch_detected",
    "recovery.mismatch_flattened",
    "recovery.resumed",
    "runtime.entry_blocked_lifecycle",
    "runtime.entry_blocked_lifecycle_selection",
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
    | _LEDGER_COMPENSATION_KINDS
    | frozenset({"runtime.position_lifecycle_terminal"})
)


def _is_fail_closed(kind: str) -> bool:
    return kind.startswith(_FAIL_CLOSED_PREFIX)


def is_projected_kind(kind: str) -> bool:
    """Return True if this journal event kind should be projected into a fact table."""
    return kind in PROJECTED_KINDS or _is_fail_closed(kind)


def is_journal_only_kind(kind: str) -> bool:
    """Return True if this kind must stay journal-first for exact replay."""
    return kind in _JOURNAL_ONLY_KINDS


def _ledger_bridge_entity(kind: str, payload: dict, ts_ms: int) -> tuple[str, str] | None:
    if kind in _LEDGER_POSITION_OPEN_KINDS or kind in _LEDGER_POSITION_CLOSE_KINDS:
        position_id = _str_field(payload, "position_id")
        return ("position", position_id) if position_id else None
    if kind in _LEDGER_COMPENSATION_KINDS:
        position_id = _str_field(payload, "position_id")
        if position_id:
            return "position", position_id
        for key in ("review_id", "pair_id"):
            value = _str_field(payload, key)
            if value:
                return "entry_attempt", value
        return None
    if kind in _LEDGER_ORDER_KINDS:
        return "order", _order_key(payload, ts_ms)
    return None


def _truth_level(kind: str, payload: dict) -> str:
    if kind in {"order.filled"}:
        return "venue_fill_confirmed"
    if kind == "exit.closed" and _exit_closed_has_fill_evidence(payload):
        return "venue_fill_confirmed"
    return "runtime_estimated"


def _exit_closed_has_fill_evidence(payload: dict) -> bool:
    for key in ("long_exit_order_id", "short_exit_order_id"):
        if _str_field(payload, key):
            return True
    for key in ("long_exit_order_ids", "short_exit_order_ids", "long_legs", "short_legs"):
        value = payload.get(key)
        if isinstance(value, list) and value:
            return True
    return False


def _str_field(payload: dict, key: str) -> str:
    value = payload.get(key)
    if value is None:
        return ""
    text = str(value)
    return text if text else ""


def _optional_str(payload: dict, key: str) -> str | None:
    return _str_field(payload, key) or None


def _float_field(payload: dict, *keys: str) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _int_field(payload: dict, key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _order_key(payload: dict, ts_ms: int) -> str:
    venue = _str_field(payload, "venue") or "unknown"
    for key in ("client_order_id", "order_id", "exchange_order_id"):
        value = _str_field(payload, key)
        if value:
            return f"{venue}:{value}"
    return f"{venue}:order:{ts_ms}"


def _fill_key(payload: dict, filled_at_ms: int) -> str:
    venue = _str_field(payload, "venue") or "unknown"
    for key in ("trade_id", "exchange_trade_id", "order_id", "client_order_id"):
        value = _str_field(payload, key)
        if value:
            return f"{venue}:fill:{value}"
    return f"{venue}:fill:{filled_at_ms}"


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
                ledger_ok = self._project_lifecycle_ledger(conn, seq, ts_ms, kind, payload)
                if kind in _ORDER_KINDS:
                    ok = self._project_order(conn, seq, ts_ms, kind, payload) or bool(ledger_ok)
                elif kind in _ENTRY_EXIT_KINDS:
                    ok = self._project_entry_exit(conn, seq, ts_ms, kind, payload) or bool(ledger_ok)
                elif kind in _RISK_KINDS:
                    ok = self._project_risk(conn, seq, ts_ms, kind, payload)
                elif kind in _LOCAL_L2_HEALTH_KINDS:
                    ok = self._project_local_l2_health(conn, seq, ts_ms, kind, payload)
                elif kind in _DIAGNOSTIC_KINDS or _is_fail_closed(kind):
                    ok = self._project_diagnostic(conn, seq, ts_ms, kind, payload)
                else:
                    if ledger_ok is None:
                        # Journal-only kind with no ledger bridge — intentionally skip.
                        continue
                    ok = ledger_ok

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

    def _project_lifecycle_ledger(
        self, conn: Any, seq: int, ts_ms: int, kind: str, payload: dict
    ) -> bool | None:
        entity = _ledger_bridge_entity(kind, payload, ts_ms)
        if entity is None:
            return None
        entity_type, entity_id = entity
        payload_json = json.dumps(payload, ensure_ascii=False)
        truth_level = _truth_level(kind, payload)
        wrote = self._store.insert_trade_ledger_event(
            conn,
            event_id=f"{seq}:{ts_ms}:{kind}:{entity_type}:{entity_id}",
            seq=seq,
            ts_ms=ts_ms,
            entity_type=entity_type,
            entity_id=entity_id,
            event_kind=kind,
            truth_level=truth_level,
            created_at_ms=ts_ms,
            run_id=_optional_str(payload, "run_id"),
            instance_id=_optional_str(payload, "instance_id"),
            payload_json=payload_json,
        )

        if kind in _LEDGER_POSITION_OPEN_KINDS:
            self._record_position_open(conn, ts_ms, kind, payload, payload_json)
        elif kind in _LEDGER_POSITION_CLOSE_KINDS:
            self._record_position_close(conn, ts_ms, kind, payload, payload_json, truth_level)
        elif kind in _LEDGER_ORDER_KINDS:
            self._record_order_ledger(conn, ts_ms, kind, payload, payload_json, truth_level)
            if kind == "order.filled":
                self._record_fill_ledger(conn, ts_ms, payload, payload_json)
        return wrote

    def _record_position_open(
        self, conn: Any, ts_ms: int, kind: str, payload: dict, payload_json: str
    ) -> None:
        position_id = _str_field(payload, "position_id")
        if not position_id:
            return
        opened_at_ms = _int_field(payload, "entered_at_ms") or _int_field(payload, "opened_at_ms") or ts_ms
        self._store.upsert_position_ledger(
            conn,
            position_id=position_id,
            candidate_id=_optional_str(payload, "review_id"),
            review_id=_optional_str(payload, "review_id"),
            strategy_id=_optional_str(payload, "strategy_id"),
            run_id=_optional_str(payload, "run_id"),
            instance_id=_optional_str(payload, "instance_id"),
            symbol=_str_field(payload, "symbol"),
            long_venue=_str_field(payload, "long_venue"),
            short_venue=_str_field(payload, "short_venue"),
            state="open",
            opened_at_ms=opened_at_ms,
            entry_qty=_float_field(payload, "quantity", "matched_quantity"),
            entry_notional_quote=_float_field(payload, "entry_notional_quote"),
            owner_instance_id=_optional_str(payload, "owner_instance_id")
            or _optional_str(payload, "instance_id"),
            reconciliation_status="live_recovered" if kind == "recovery.live_detected" else "runtime_open",
            truth_level="runtime_estimated",
            payload_json=payload_json,
            created_at_ms=ts_ms,
            updated_at_ms=ts_ms,
        )

    def _record_position_close(
        self,
        conn: Any,
        ts_ms: int,
        kind: str,
        payload: dict,
        payload_json: str,
        truth_level: str,
    ) -> None:
        position_id = _str_field(payload, "position_id")
        if not position_id:
            return
        closed_at_ms = _int_field(payload, "closed_at_ms") or ts_ms
        opened_at_ms = _int_field(payload, "opened_at_ms") or closed_at_ms
        terminal_reason = None
        if kind != "recovery.flat":
            terminal_reason = (
                _optional_str(payload, "terminal_reason")
                or _optional_str(payload, "reason")
                or _optional_str(payload, "source")
                or kind
            )
        problem = bool(payload.get("problem", False))
        self._store.upsert_position_ledger(
            conn,
            position_id=position_id,
            candidate_id=_optional_str(payload, "review_id"),
            review_id=_optional_str(payload, "review_id"),
            strategy_id=_optional_str(payload, "strategy_id"),
            run_id=_optional_str(payload, "run_id"),
            instance_id=_optional_str(payload, "instance_id"),
            symbol=_str_field(payload, "symbol"),
            long_venue=_str_field(payload, "long_venue"),
            short_venue=_str_field(payload, "short_venue"),
            state="closed",
            opened_at_ms=opened_at_ms,
            closed_at_ms=closed_at_ms,
            exit_qty=_float_field(payload, "exit_quantity", "quantity", "new_quantity"),
            entry_notional_quote=_float_field(payload, "entry_notional_quote"),
            exit_notional_quote=_float_field(payload, "exit_notional_quote"),
            owner_instance_id=_optional_str(payload, "owner_instance_id")
            or _optional_str(payload, "instance_id"),
            terminal_reason=terminal_reason,
            problem=problem,
            problem_reason=_optional_str(payload, "problem_reason")
            or _optional_str(payload, "force_close_reason"),
            reconciliation_status="terminal_problem" if problem else (
                "recovery_flat" if kind == "recovery.flat" else "runtime_closed"
            ),
            truth_level=truth_level,
            payload_json=payload_json,
            created_at_ms=ts_ms,
            updated_at_ms=ts_ms,
        )

        net_quote = _float_field(payload, "net_quote")
        if net_quote is None and kind != "exit.closed":
            return
        self._store.upsert_position_pnl_fact(
            conn,
            pnl_key=f"{position_id}:{truth_level}:{closed_at_ms}",
            position_id=position_id,
            realized_price_pnl_quote=_float_field(payload, "realized_price_pnl_quote"),
            funding_pnl_quote=_float_field(
                payload, "funding_pnl_quote", "captured_funding_quote"
            ),
            entry_fee_quote=_float_field(payload, "total_entry_fee_quote", "entry_fee_quote"),
            exit_fee_quote=_float_field(payload, "total_exit_fee_quote", "exit_fee_quote"),
            slippage_quote=_float_field(payload, "slippage_quote"),
            net_pnl_quote=net_quote,
            exit_reason=_optional_str(payload, "reason") or terminal_reason,
            truth_level=truth_level,
            reconciled_at_ms=closed_at_ms if kind == "exit.reconciled" else None,
            payload_json=payload_json,
            created_at_ms=ts_ms,
        )

    def _record_order_ledger(
        self,
        conn: Any,
        ts_ms: int,
        kind: str,
        payload: dict,
        payload_json: str,
        truth_level: str,
    ) -> None:
        status = {
            "order.submitted": "submitted",
            "order.filled": "filled",
            "order.rejected": "failed",
            "order.uncertain": "uncertain",
        }[kind]
        filled_at_ms = _int_field(payload, "filled_at_ms") or ts_ms
        self._store.upsert_order_ledger(
            conn,
            order_key=_order_key(payload, ts_ms),
            position_id=_optional_str(payload, "position_id"),
            candidate_id=_optional_str(payload, "review_id") or _optional_str(payload, "pair_id"),
            venue=_str_field(payload, "venue"),
            symbol=_str_field(payload, "symbol"),
            side=_str_field(payload, "side"),
            stage=_str_field(payload, "stage"),
            reduce_only=bool(payload.get("reduce_only", False)),
            client_order_id=_optional_str(payload, "client_order_id"),
            exchange_order_id=_optional_str(payload, "order_id")
            or _optional_str(payload, "exchange_order_id"),
            status=status,
            requested_qty=_float_field(payload, "requested_quantity", "quantity"),
            filled_qty=_float_field(payload, "executed_quantity", "filled_quantity"),
            avg_fill_price=_float_field(payload, "average_price", "price"),
            fee_quote=_float_field(payload, "fee_quote"),
            submitted_at_ms=ts_ms if kind == "order.submitted" else None,
            updated_at_ms=filled_at_ms if kind == "order.filled" else ts_ms,
            truth_level=truth_level,
            payload_json=payload_json,
            created_at_ms=ts_ms,
        )

    def _record_fill_ledger(
        self, conn: Any, ts_ms: int, payload: dict, payload_json: str
    ) -> None:
        filled_at_ms = _int_field(payload, "filled_at_ms") or ts_ms
        self._store.upsert_fill_ledger(
            conn,
            fill_key=_fill_key(payload, filled_at_ms),
            order_key=_order_key(payload, ts_ms),
            position_id=_optional_str(payload, "position_id"),
            venue=_str_field(payload, "venue"),
            symbol=_str_field(payload, "symbol"),
            side=_str_field(payload, "side"),
            price=_float_field(payload, "average_price", "price"),
            qty=_float_field(payload, "executed_quantity", "filled_quantity", "quantity"),
            fee_quote=_float_field(payload, "fee_quote"),
            liquidity=_optional_str(payload, "liquidity"),
            exchange_trade_id=_optional_str(payload, "trade_id")
            or _optional_str(payload, "exchange_trade_id")
            or _optional_str(payload, "order_id"),
            filled_at_ms=filled_at_ms,
            truth_level="venue_fill_confirmed",
            payload_json=payload_json,
            created_at_ms=ts_ms,
        )

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
