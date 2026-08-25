"""JSONL journal: append-only event log matching Rust reference behavior.

Rust references:
- src/observability_ops/journal_bridge.rs (JsonlJournal)
- src/observability_ops/replay_bridge.rs (journal replay)
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lightfee.persistence.journal_index import JournalIndex


class Journal:
    """Append-only JSONL journal for event persistence.

    V1 type parity: Python int is arbitrary-precision and covers all V1 integer
    domains (u64 seq, i64 ts_ms, u64/Decimal quantities) without range loss.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        max_bytes: int = 0,
        archive_count: int = 0,
        retention_hours: int = 0,
    ) -> None:
        self.path = Path(path)
        self._seq = 0
        self._run_id = f"lightfee-{int(time.time() * 1000)}-{os.getpid()}"
        self._file = None
        self._max_bytes = max(int(max_bytes or 0), 0)
        self._archive_count = max(int(archive_count or 0), 0)
        self._retention_hours = max(int(retention_hours or 0), 0)

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._prune_expired_archives()
        self._file = open(self.path, "a", encoding="utf-8")

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def append(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        flush: bool = False,
        ts_ms: int | None = None,
    ) -> int:
        if self._file is None:
            raise RuntimeError("journal not open")
        self._seq += 1
        record = {
            "seq": self._seq,
            "run_id": self._run_id,
            "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
            "kind": kind,
            "payload": payload,
        }
        line = json.dumps(record, ensure_ascii=False) + "\n"
        self._rotate_if_needed(len(line.encode("utf-8")))
        self._file.write(line)
        self._file.flush()
        if flush:
            os.fsync(self._file.fileno())
        return self._seq

    def _archive_path(self, index: int) -> Path:
        return self.path.with_name(f"{self.path.name}.{index}")

    def _prune_expired_archives(self) -> None:
        if self._retention_hours <= 0:
            return
        cutoff = time.time() - (self._retention_hours * 3600)
        archive_limit = max(self._archive_count, 1)
        for index in range(1, archive_limit + 1):
            archive = self._archive_path(index)
            try:
                if archive.exists() and archive.stat().st_mtime < cutoff:
                    archive.unlink()
            except OSError:
                continue

    def _rotate_if_needed(self, upcoming_bytes: int) -> None:
        if self._max_bytes <= 0:
            return
        try:
            current_size = self.path.stat().st_size if self.path.exists() else 0
        except OSError:
            current_size = 0
        if current_size <= 0 or current_size + upcoming_bytes <= self._max_bytes:
            return

        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None

        archive_limit = max(self._archive_count, 1)
        oldest = self._archive_path(archive_limit)
        try:
            if oldest.exists():
                oldest.unlink()
        except OSError:
            pass
        for index in range(archive_limit - 1, 0, -1):
            source = self._archive_path(index)
            destination = self._archive_path(index + 1)
            try:
                if source.exists():
                    source.replace(destination)
            except OSError:
                continue
        try:
            if self.path.exists():
                self.path.replace(self._archive_path(1))
        finally:
            self._prune_expired_archives()
            self._file = open(self.path, "a", encoding="utf-8")

    def append_critical(
        self,
        ts_ms: int,
        kind: str,
        payload: dict[str, Any],
    ) -> int:
        """Append a critical event with forced fsync (Rust V1: append_critical).

        Critical events (recovery state changes, risk mode transitions,
        operator commands) must be durable before the caller proceeds.
        """
        return self.append(kind, payload, flush=True, ts_ms=ts_ms)

    def scan_records_matching_kinds(
        self, kinds: list[str]
    ) -> list[dict[str, Any]]:
        """Scan journal records matching given event kinds.

        Rust V1: scan_records_matching_kinds() in journal_bridge.rs.
        Uses stream_records() to avoid materializing the entire journal
        in memory before filtering.
        """
        kind_set = set(kinds)
        return [r for r in self.stream_records() if r.get("kind", "") in kind_set]

    def read_all(self) -> list[dict[str, Any]]:
        """Read all journal records. Returns list of parsed dicts."""
        if not self.path.exists():
            return []
        records: list[dict[str, Any]] = []
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    # ------------------------------------------------------------------
    # Streaming read primitives (V2 projection/backfill)
    # ------------------------------------------------------------------

    def _parse_lines(self, f) -> Iterator[dict[str, Any]]:
        """Yield parsed records from a file object, skipping malformed lines."""
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                pass

    def stream_records(self):
        """Generator: yield every journal record without materializing the file.

        Use this for projection jobs and backfill to avoid read_all()
        memory pressure on large journals.
        """
        if not self.path.exists():
            return
        with open(self.path) as f:
            yield from self._parse_lines(f)

    def stream_from(self, start_seq: int, *, index: JournalIndex | None = None):
        """Generator: yield records with seq >= start_seq.

        When `index` is provided and contains `start_seq`, seeks directly
        to the correct byte offset (sub-linear). Otherwise falls back to
        a linear scan — correct but slower for large journals.
        """
        if not self.path.exists():
            return

        offset: int | None = None
        if index is not None:
            offset = index.offset_for(start_seq)

        with open(self.path) as f:
            if offset is not None:
                f.seek(offset)
            for record in self._parse_lines(f):
                if record.get("seq", 0) >= start_seq:
                    yield record

    @property
    def max_seq(self) -> int:
        """Highest seq in the on-disk journal (0 if file missing/empty).

        Reads only the last line — constant memory.
        """
        if not self.path.exists():
            return 0
        try:
            with open(self.path, "rb") as f:
                # Seek to last ~4 KiB and find the last complete line
                f.seek(0, 2)
                file_size = f.tell()
                if file_size == 0:
                    return 0
                chunk_size = min(4096, file_size)
                f.seek(max(0, file_size - chunk_size))
                tail = f.read(chunk_size).decode("utf-8", errors="replace")
                lines = tail.strip().split("\n")
                last_line = lines[-1].strip() if lines else ""
                if last_line:
                    rec = json.loads(last_line)
                    return int(rec.get("seq", 0))
                return 0
        except (OSError, json.JSONDecodeError, ValueError, KeyError):
            return 0

    @property
    def seq(self) -> int:
        return self._seq

    @property
    def run_id(self) -> str:
        return self._run_id


# ---------------------------------------------------------------------------
# Journal replay (Rust V1: replay_bridge.rs)
# ---------------------------------------------------------------------------

def _normalize_position_snapshot(pdata: dict[str, Any]) -> dict[str, Any]:
    """Normalize a position dict to the fixed schema with all V1-visible fields.

    Includes both ReplayPositionSnapshot fields and OpenPosition fields
    needed for full PnL and state reconstruction.
    """
    return {
        "position_id": pdata.get("position_id", ""),
        "symbol": pdata.get("symbol", ""),
        "long_venue": pdata.get("long_venue", ""),
        "short_venue": pdata.get("short_venue", ""),
        "quantity": float(pdata.get("quantity", 0)),
        "long_quantity": float(pdata.get("long_quantity", 0)),
        "short_quantity": float(pdata.get("short_quantity", 0)),
        "long_entry_price": float(pdata.get("long_entry_price", 0)),
        "short_entry_price": float(pdata.get("short_entry_price", 0)),
        "opened_at_ms": int(pdata.get("opened_at_ms", 0)),
        "matched_quantity": float(pdata.get("matched_quantity", 0)),
        "current_net_quote": float(pdata.get("current_net_quote", 0)),
        "peak_net_quote": float(pdata.get("peak_net_quote", 0)),
        "captured_funding_quote": float(pdata.get("captured_funding_quote", 0)),
        "second_stage_funding_quote": float(pdata.get("second_stage_funding_quote", 0)),
        "long_entry_fee_quote": float(pdata.get("long_entry_fee_quote", 0)),
        "short_entry_fee_quote": float(pdata.get("short_entry_fee_quote", 0)),
        "realized_price_pnl_quote": float(pdata.get("realized_price_pnl_quote", 0)),
        "realized_exit_fee_quote": float(pdata.get("realized_exit_fee_quote", 0)),
        "funding_captured": bool(pdata.get("funding_captured", False)),
        "second_stage_funding_captured": bool(
            pdata.get("second_stage_funding_captured", False)
        ),
        "review_id": pdata.get("review_id", None),
    }


def replay_journal_records(
    records: list[dict[str, Any]],
    seed_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay journal records to reconstruct engine state.

    Rust V1: replay_journal_records() in observability_ops/replay_bridge.rs.
    Tracks open positions, partial closes, lifecycle/risk transitions, pending
    entries/closes, scan statistics, recovery and risk events, and full timeline.

    Returns a dict with open_position_count, open_position_ids,
    pending_entry_count, pending_close_count, pending_close_reconciliation_count,
    pending_close_reconciliation_ids, final_lifecycle, final_risk_mode,
    positions (fixed-schema dicts), scan_stats, recovery_events, risk_events,
    and timeline.
    """
    positions: dict[str, dict[str, Any]] = {}
    lifecycle = "booting"
    risk_mode = "running"
    open_ids: set[str] = set()
    pending_entry_ids: set[str] = set()
    pending_close_ids: set[str] = set()
    pending_close_reconciliation_ids: set[str] = set()
    # Map close_id -> position_id for correct cleanup on close
    _close_to_position: dict[str, str] = {}

    scan_stats: dict[str, Any] | None = None
    recovery_events: list[dict[str, Any]] = []
    risk_events: list[dict[str, Any]] = []
    timeline: list[dict[str, Any]] = []

    _timeline_interesting = frozenset({
        "entry.opened", "entry.pending_registered",
        "exit.closed", "exit.partial_closed", "exit.partial_reconciled", "exit.reconciled",
        "exit.billing_evidence_unavailable",
        "exit.billing_evidence_debt_registered",
        "exit.billing_evidence_imported",
        "exit.billing_evidence_pending",
        "exit.reconciliation_abandoned",
        "exit.pending_close_registered",
        "exit.pending_close_reconciliation_registered",
        "recovery.live_detected", "recovery.external_pair_flat_observed",
        "recovery.external_pair_flat_reclassified", "recovery.flat", "recovery.blocked",
        "recovery.mismatch_detected", "recovery.mismatch_flattened",
        "recovery.resumed",
        "runtime.lifecycle_changed", "runtime.risk_mode_changed",
        "risk.warning_triggered", "risk.death_triggered",
        "risk.single_side_protection_triggered",
        "risk.single_side_protection_failed",
        "risk.single_side_protection_unavailable",
        "scan.completed", "scan.no_entry_diagnostics",
        "scan.runtime_gate_blocked",
        "review.assigned",
    })

    if seed_state:
        lifecycle = seed_state.get("lifecycle", lifecycle)
        risk_mode = seed_state.get("risk_mode", risk_mode)
        seed_positions = seed_state.get("open_positions", {})
        if isinstance(seed_positions, dict):
            for pid, pdata in seed_positions.items():
                if isinstance(pdata, dict):
                    positions[pid] = _normalize_position_snapshot(pdata)
                    open_ids.add(pid)

    for record in records:
        kind = record.get("kind", "")
        payload = record.get("payload", {})

        if kind in _timeline_interesting:
            timeline.append({
                "seq": record.get("seq"),
                "ts_ms": record.get("ts_ms"),
                "kind": kind,
            })

        if kind in ("entry.opened", "recovery.live_detected"):
            pid = payload.get("position_id", "")
            if pid:
                positions[pid] = _normalize_position_snapshot(payload)
                open_ids.add(pid)
                pending_entry_ids.discard(pid)
            # Track recovery.live_detected as a recovery event (V1 parity)
            if kind == "recovery.live_detected":
                recovery_events.append({
                    "kind": kind, "payload": dict(payload),
                    "seq": record.get("seq"),
                })

        elif kind in (
            "exit.closed",
            "exit.reconciled",
            "exit.billing_evidence_unavailable",
            "exit.reconciliation_abandoned",
            "recovery.external_pair_flat_observed",
            "recovery.flat",
        ):
            pid = payload.get("position_id", "")
            if pid and pid in positions:
                del positions[pid]
                open_ids.discard(pid)
            pending_close_ids.discard(pid)
            if not (
                kind in {"recovery.flat", "exit.reconciliation_abandoned"}
                and (
                    (
                        kind == "recovery.flat"
                        and payload.get("billing_reconciliation_pending") is True
                    )
                    or str(pid) in pending_close_reconciliation_ids
                )
            ):
                pending_close_reconciliation_ids.discard(pid)
            # Also remove any pending close registered for this position
            stale_closes = [cid for cid, cpid in _close_to_position.items() if cpid == pid]
            for cid in stale_closes:
                pending_close_ids.discard(cid)
                del _close_to_position[cid]

        elif kind == "exit.partial_closed":
            pid = payload.get("position_id", "")
            if pid and pid in positions:
                pos = positions[pid]
                if "quantity" in payload:
                    pos["quantity"] = float(payload["quantity"])
                if "current_net_quote" in payload:
                    pos["current_net_quote"] = float(payload["current_net_quote"])
                if "peak_net_quote" in payload:
                    pos["peak_net_quote"] = float(payload["peak_net_quote"])
                if "funding_captured" in payload:
                    pos["funding_captured"] = bool(payload["funding_captured"])
                if "second_stage_funding_captured" in payload:
                    pos["second_stage_funding_captured"] = bool(
                        payload["second_stage_funding_captured"])

        elif kind == "exit.partial_reconciled":
            pid = payload.get("position_id", "")
            if pid and pid in positions:
                pos = positions[pid]
                for source_field, target_field in (
                    (
                        "reconciled_realized_price_pnl_quote",
                        "realized_price_pnl_quote",
                    ),
                    (
                        "reconciled_realized_exit_fee_quote",
                        "realized_exit_fee_quote",
                    ),
                ):
                    try:
                        value = float(payload[source_field])
                    except (KeyError, TypeError, ValueError):
                        continue
                    pos[target_field] = value

        elif kind == "entry.pending_registered":
            pid = payload.get("pending_id", "")
            if pid:
                pending_entry_ids.add(pid)

        elif kind == "exit.pending_close_registered":
            cid = payload.get("close_id", "")
            pid = payload.get("position_id", "")
            if cid:
                pending_close_ids.add(cid)
                if pid:
                    _close_to_position[cid] = pid

        elif kind in (
            "exit.pending_close_reconciliation_registered",
            "exit.billing_evidence_debt_registered",
        ):
            reconciliation = payload.get("reconciliation")
            pid = (
                reconciliation.get("position_id")
                if isinstance(reconciliation, dict)
                else payload.get("position_id", "")
            )
            if pid:
                pending_close_reconciliation_ids.add(str(pid))
                # Registration is emitted only after live-flat exchange
                # truth; remove local position/close ownership even for old
                # summary-only registration events.
                positions.pop(str(pid), None)
                open_ids.discard(str(pid))
                pending_close_ids.discard(str(pid))
                stale_closes = [
                    cid
                    for cid, cpid in _close_to_position.items()
                    if cpid == str(pid)
                ]
                for cid in stale_closes:
                    pending_close_ids.discard(cid)
                    del _close_to_position[cid]

        elif kind == "recovery.external_pair_flat_reclassified":
            pid = str(payload.get("position_id") or "")
            if pid:
                pending_close_reconciliation_ids.discard(pid)

        elif kind == "runtime.lifecycle_changed":
            to_val = payload.get("to")
            if to_val:
                lifecycle = str(to_val)

        elif kind == "runtime.risk_mode_changed":
            to_val = payload.get("to")
            if to_val:
                risk_mode = str(to_val)

        elif kind == "scan.completed":
            scan_stats = {
                "candidate_count": int(payload.get("candidate_count", 0)),
                "blocked_count": int(payload.get("blocked_count", 0)),
                "accepted_count": int(payload.get("accepted_count", 0)),
                "blocked_reasons": payload.get("blocked_reasons", {}),
                "no_entry_reason": payload.get("no_entry_reason", ""),
            }

        elif kind.startswith("recovery."):
            recovery_events.append({
                "kind": kind, "payload": dict(payload),
                "seq": record.get("seq"),
            })

        elif kind.startswith("risk."):
            risk_events.append({
                "kind": kind, "payload": dict(payload),
                "seq": record.get("seq"),
            })

    return {
        "open_position_count": len(open_ids),
        "open_position_ids": sorted(open_ids),
        "pending_entry_count": len(pending_entry_ids),
        "pending_close_count": len(pending_close_ids),
        "pending_close_reconciliation_count": len(
            pending_close_reconciliation_ids
        ),
        "pending_close_reconciliation_ids": sorted(
            pending_close_reconciliation_ids
        ),
        "final_lifecycle": lifecycle,
        "final_risk_mode": risk_mode,
        "positions": positions,
        "scan_stats": scan_stats,
        "recovery_events": recovery_events,
        "risk_events": risk_events,
        "timeline": timeline,
    }
