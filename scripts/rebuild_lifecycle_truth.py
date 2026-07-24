#!/usr/bin/env python3
"""Rebuild canonical lifecycle truth from production-visible evidence."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import inspect
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lightfee.lifecycle.exchange_truth_ledger import (  # noqa: E402
    LifecycleClassification,
    build_exchange_truth_lifecycle,
)


DEFAULT_RUNTIME_DIR = Path("runtime")
CORRECTION_DIR = Path("runtime/audits/lifecycle-truth-corrections")
CORRECTION_AUDIT_SCHEMA_VERSION = 2
CORRECTION_AUDIT_KIND = "lightfee.lifecycle_truth_correction_audit"
CORRECTION_EVENT_KIND = "accounting.lifecycle_truth_rebuilt"
CORRECTION_AUDIT_PRODUCER = "rebuild_lifecycle_truth"
CORRECTION_AUDIT_SIGNATURE_ALGORITHM = "hmac-sha256"
CORRECTION_AUDIT_HMAC_KEY_FILENAME = ".audit-hmac-key"
CORRECTION_AUDIT_MIN_HMAC_KEY_BYTES = 32
HISTORICAL_ORDER_QUERY_WINDOW_MS = 6 * 24 * 60 * 60 * 1000
ACCOUNT_HISTORY_QUERY_WINDOW_MS = 6 * 24 * 60 * 60 * 1000
ACCOUNT_HISTORY_IDENTITY_FALLBACK_WINDOW_MS = 30 * 60 * 1000
ACCOUNT_HISTORY_NEXT_ENTRY_BOUNDARY_GRACE_MS = 250
QTY_TOLERANCE = 0.999


class CorrectionAuditError(ValueError):
    """A correction audit is absent, corrupt, or outside the requested scope."""


@dataclass(frozen=True)
class CorrectionAuditInput:
    events: list[dict[str, Any]]
    audits: list[dict[str, Any]]


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _fsync_directory(directory: Path) -> None:
    directory_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _correction_audit_hmac_key(directory: Path, *, create: bool) -> bytes | None:
    """Read, or on the write path create, the private sidecar signing key."""
    key_path = directory / CORRECTION_AUDIT_HMAC_KEY_FILENAME
    try:
        key = key_path.read_bytes()
    except FileNotFoundError:
        if not create:
            return None
        directory.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return _correction_audit_hmac_key(directory, create=False)
        except OSError as exc:
            raise CorrectionAuditError(
                f"cannot create correction audit signing key: {key_path}"
            ) from exc
        key = secrets.token_bytes(CORRECTION_AUDIT_MIN_HMAC_KEY_BYTES)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(key)
                handle.flush()
                os.fsync(handle.fileno())
            _fsync_directory(directory)
        except OSError as exc:
            key_path.unlink(missing_ok=True)
            raise CorrectionAuditError(
                f"cannot persist correction audit signing key: {key_path}"
            ) from exc
    except OSError as exc:
        raise CorrectionAuditError(f"cannot read correction audit signing key: {key_path}") from exc

    if len(key) < CORRECTION_AUDIT_MIN_HMAC_KEY_BYTES:
        raise CorrectionAuditError(f"invalid correction audit signing key: {key_path}")
    try:
        key_mode = key_path.stat().st_mode & 0o777
    except OSError as exc:
        raise CorrectionAuditError(f"cannot stat correction audit signing key: {key_path}") from exc
    if key_mode & 0o077:
        raise CorrectionAuditError(
            f"correction audit signing key is not private (expected 0600): {key_path}"
        )
    return key


def _correction_audit_signed_payload(events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": CORRECTION_AUDIT_SCHEMA_VERSION,
        "kind": CORRECTION_AUDIT_KIND,
        "producer": CORRECTION_AUDIT_PRODUCER,
        "events": events,
        "events_sha256": hashlib.sha256(_canonical_json_bytes(events)).hexdigest(),
    }


def correction_audit_payload(
    events: list[dict[str, Any]],
    *,
    hmac_key: bytes,
) -> dict[str, Any]:
    """Envelope a correction audit with an independently verifiable signature."""
    if len(hmac_key) < CORRECTION_AUDIT_MIN_HMAC_KEY_BYTES:
        raise CorrectionAuditError(
            "correction audit HMAC key is shorter than "
            f"{CORRECTION_AUDIT_MIN_HMAC_KEY_BYTES} bytes"
        )
    signed_payload = _correction_audit_signed_payload(events)
    signature = hmac.new(
        hmac_key,
        _canonical_json_bytes(signed_payload),
        hashlib.sha256,
    ).hexdigest()
    return {
        **signed_payload,
        "signature_algorithm": CORRECTION_AUDIT_SIGNATURE_ALGORITHM,
        "events_hmac_sha256": signature,
    }


def _event_position_id(event: dict[str, Any]) -> str:
    payload = event.get("payload")
    row = payload if isinstance(payload, dict) else event
    return str(row.get("position_id") or row.get("entry_id") or "").strip()


def read_jsonl_events(
    paths: list[Path],
    *,
    position_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    selected_ids = {str(item) for item in position_ids or set() if str(item)}
    events: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    if selected_ids and _event_position_id(event) not in selected_ids:
                        continue
                    events.append(event)
    return events


def read_correction_events(
    paths: list[Path],
    *,
    position_ids: set[str] | None = None,
    hmac_key: bytes | None = None,
    allow_legacy_unsigned: bool = False,
) -> CorrectionAuditInput:
    """Read append-only correction audit files produced by ``--apply``.

    A retained audit is a stricter input than the best-effort runtime journal:
    a missing, truncated, malformed, out-of-scope, or internally inconsistent
    audit must fail the replay rather than create a false-green zero result.
    """
    selected_ids = {str(item) for item in position_ids or set() if str(item)}
    if hmac_key is not None and len(hmac_key) < CORRECTION_AUDIT_MIN_HMAC_KEY_BYTES:
        raise CorrectionAuditError(
            "correction audit HMAC key is shorter than "
            f"{CORRECTION_AUDIT_MIN_HMAC_KEY_BYTES} bytes"
        )
    events: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    seen_snapshot_positions: dict[str, Path] = {}
    for path in paths:
        if not path.is_file():
            raise CorrectionAuditError(f"correction audit does not exist or is not a file: {path}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorrectionAuditError(f"invalid correction audit JSON: {path}") from exc

        integrity = "verified_hmac_sha256"
        if isinstance(payload, list):
            if not allow_legacy_unsigned:
                raise CorrectionAuditError(
                    "legacy unsigned correction audit is refused: "
                    f"{path}; use --allow-legacy-unsigned-correction only for forensic inspection"
                )
            rows = payload
            integrity = "legacy_unsigned"
        elif isinstance(payload, dict):
            schema_version = payload.get("schema_version")
            if schema_version == 1:
                if not allow_legacy_unsigned:
                    raise CorrectionAuditError(
                        "legacy unsigned correction audit is refused: "
                        f"{path}; use --allow-legacy-unsigned-correction only for forensic inspection"
                    )
                integrity = "legacy_unsigned"
            elif schema_version != CORRECTION_AUDIT_SCHEMA_VERSION:
                raise CorrectionAuditError(f"unsupported correction audit schema: {path}")
            if payload.get("kind") != CORRECTION_AUDIT_KIND:
                raise CorrectionAuditError(f"unexpected correction audit kind: {path}")
            if payload.get("producer") != CORRECTION_AUDIT_PRODUCER:
                raise CorrectionAuditError(f"unexpected correction audit producer: {path}")
            rows = payload.get("events")
            expected_digest = payload.get("events_sha256")
            if not isinstance(rows, list) or not isinstance(expected_digest, str):
                raise CorrectionAuditError(f"invalid correction audit envelope: {path}")
            actual_digest = hashlib.sha256(_canonical_json_bytes(rows)).hexdigest()
            if actual_digest != expected_digest:
                raise CorrectionAuditError(f"correction audit checksum mismatch: {path}")
            if schema_version == CORRECTION_AUDIT_SCHEMA_VERSION:
                if payload.get("signature_algorithm") != CORRECTION_AUDIT_SIGNATURE_ALGORITHM:
                    raise CorrectionAuditError(f"unexpected correction audit signature algorithm: {path}")
                supplied_signature = payload.get("events_hmac_sha256")
                if not isinstance(supplied_signature, str):
                    raise CorrectionAuditError(f"invalid correction audit signature: {path}")
                key = hmac_key or _correction_audit_hmac_key(path.parent, create=False)
                if key is None:
                    raise CorrectionAuditError(
                        "correction audit signing key is missing: "
                        f"{path.parent / CORRECTION_AUDIT_HMAC_KEY_FILENAME}"
                    )
                expected_signature = hmac.new(
                    key,
                    _canonical_json_bytes(_correction_audit_signed_payload(rows)),
                    hashlib.sha256,
                ).hexdigest()
                if not hmac.compare_digest(supplied_signature, expected_signature):
                    raise CorrectionAuditError(f"correction audit signature mismatch: {path}")
        else:
            raise CorrectionAuditError(f"invalid correction audit root value: {path}")

        if not isinstance(rows, list):
            raise CorrectionAuditError(f"invalid correction audit events: {path}")
        selected_event_count = 0
        selected_snapshot_count = 0
        for event in rows:
            if not isinstance(event, dict):
                raise CorrectionAuditError(f"non-object correction audit event: {path}")
            snapshot_position_id = _validate_correction_snapshot(event, path=path)
            if snapshot_position_id:
                prior_path = seen_snapshot_positions.get(snapshot_position_id)
                if prior_path is not None:
                    raise CorrectionAuditError(
                        "duplicate canonical snapshot for "
                        f"{snapshot_position_id}: {prior_path} and {path}"
                    )
                seen_snapshot_positions[snapshot_position_id] = path
            if selected_ids and _event_position_id(event) not in selected_ids:
                continue
            events.append(event)
            selected_event_count += 1
            if snapshot_position_id:
                selected_snapshot_count += 1
        audits.append(
            {
                "path": str(path),
                "integrity": integrity,
                "event_count": len(rows),
                "selected_event_count": selected_event_count,
                "selected_snapshot_count": selected_snapshot_count,
            }
        )

    snapshot_ids = {
        _event_position_id(event)
        for event in events
        if str(event.get("kind") or "") == CORRECTION_EVENT_KIND
    }
    if selected_ids:
        missing = sorted(selected_ids - snapshot_ids)
        extra = sorted(snapshot_ids - selected_ids)
        if missing or extra:
            raise CorrectionAuditError(
                "correction audit scope mismatch: "
                f"missing_snapshots={missing}, unexpected_snapshots={extra}"
            )
    if not snapshot_ids:
        raise CorrectionAuditError("correction audit contains no canonical lifecycle snapshots")
    return CorrectionAuditInput(events=events, audits=audits)


def _validate_correction_snapshot(event: dict[str, Any], *, path: Path) -> str | None:
    """Validate one canonical snapshot and return its position id, if any."""
    if str(event.get("kind") or "") != CORRECTION_EVENT_KIND:
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        raise CorrectionAuditError(f"canonical snapshot has non-object payload: {path}")
    position_id = _event_position_id(event)
    truth = payload.get("truth")
    if not position_id or not isinstance(truth, dict):
        raise CorrectionAuditError(f"canonical snapshot missing position_id or truth: {path}")
    if payload.get("source") != CORRECTION_AUDIT_PRODUCER:
        raise CorrectionAuditError(f"canonical snapshot has unexpected source: {path}")
    truth_position_id = str(truth.get("position_id") or "").strip()
    if truth_position_id != position_id:
        raise CorrectionAuditError(
            "canonical snapshot position_id mismatch: "
            f"event={position_id}, truth={truth_position_id}, audit={path}"
        )
    classification = str(truth.get("classification") or "")
    if classification not in {item.value for item in LifecycleClassification}:
        raise CorrectionAuditError(
            f"canonical snapshot has unsupported classification for {position_id}: {path}"
        )
    for field in ("classification", "project_record_status"):
        payload_value = str(payload.get(field) or "")
        truth_value = str(truth.get(field) or "")
        if not payload_value or payload_value != truth_value:
            raise CorrectionAuditError(
                f"canonical snapshot {field} mismatch for {position_id}: {path}"
            )
    return position_id


def apply_correction_replay_snapshots(
    report: dict[str, Any],
    correction_events: Iterable[dict[str, Any]],
) -> int:
    """Overlay canonical truth snapshots from an append-only correction audit.

    A lifecycle correction is the gated result of a previous exchange-truth
    rebuild.  It must retain its canonical classification after the original
    journal has been rotated away; merging its individual fills with stale
    partial rows can otherwise recreate the very gap the correction closed.
    This is deliberately used only by the explicit, no-query replay mode.
    """
    positions = report.get("positions")
    if not isinstance(positions, dict):
        return 0
    applied = 0
    for event in correction_events:
        if str(event.get("kind") or "") != CORRECTION_EVENT_KIND:
            continue
        position_id = _validate_correction_snapshot(event, path=Path("<replay>"))
        assert position_id is not None
        payload = event["payload"]
        truth = payload.get("truth")
        assert isinstance(truth, dict)
        positions[position_id] = dict(truth)
        applied += 1
    if not applied:
        return 0

    classifications = Counter(
        str(truth.get("classification") or "")
        for truth in positions.values()
        if isinstance(truth, dict)
    )
    project_statuses = Counter(
        str(truth.get("project_record_status") or "")
        for truth in positions.values()
        if isinstance(truth, dict) and str(truth.get("project_record_status") or "")
    )
    summary = report.get("summary")
    if not isinstance(summary, dict):
        summary = {}
        report["summary"] = summary
    summary["position_count"] = len(positions)
    for classification in (
        "exchange_lifecycle_complete",
        "exchange_lifecycle_incomplete",
        "evidence_incomplete",
        "phantom_zero_qty_opened",
    ):
        summary[classification] = classifications[classification]
    summary["project_record_status_counts"] = dict(sorted(project_statuses.items()))
    return applied


def discover_event_files(runtime_dir: Path, history: str) -> list[Path]:
    files = sorted(runtime_dir.glob("live-events*.jsonl*"))
    if history == "all":
        files.extend(sorted((runtime_dir / "archive").glob("live-events*.jsonl*")))
    return sorted(set(files), key=lambda path: str(path))


def read_position_ids(path: Path | None) -> list[str] | None:
    if path is None:
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
    if isinstance(payload, list):
        return [str(item) for item in payload if str(item)]
    if isinstance(payload, dict):
        rows = payload.get("positions") or payload.get("position_ids") or payload.get("excluded_positions")
        if isinstance(rows, list):
            out: list[str] = []
            for row in rows:
                if isinstance(row, dict):
                    value = row.get("position_id") or row.get("entry_id")
                else:
                    value = row
                if value:
                    out.append(str(value))
            return out
    raise SystemExit(f"unsupported positions file shape: {path}")


def position_event_windows(
    events: list[dict[str, Any]],
    *,
    position_ids: Iterable[str] | None = None,
) -> dict[str, dict[str, Any]]:
    selected_ids = {str(item) for item in position_ids or [] if str(item)}
    windows: dict[str, dict[str, Any]] = {}
    for event in events:
        _add_position_event_window(windows, event, selected_ids=selected_ids)
    return _finalize_position_event_windows(windows)


def read_position_event_windows(
    paths: Iterable[Path],
    *,
    position_ids: Iterable[str] | None = None,
) -> dict[str, dict[str, Any]]:
    selected_ids = {str(item) for item in position_ids or [] if str(item)}
    windows: dict[str, dict[str, Any]] = {}
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                _add_position_event_window(windows, event, selected_ids=selected_ids)
    return _finalize_position_event_windows(windows)


def _add_position_event_window(
    windows: dict[str, dict[str, Any]],
    event: Any,
    *,
    selected_ids: set[str],
) -> None:
    if not isinstance(event, dict):
        return
    position_id = _event_position_id(event)
    if not position_id or (selected_ids and position_id not in selected_ids):
        return
    ts_ms = _event_ts_ms(event)
    if ts_ms <= 0:
        return
    kind = str(event.get("kind") or "")
    payload = event.get("payload")
    row_payload = payload if isinstance(payload, dict) else event
    symbol = str(row_payload.get("symbol") or "").upper()
    row = windows.setdefault(
        position_id,
        {
            "first_ts_ms": ts_ms,
            "last_ts_ms": ts_ms,
            "entry_ts_ms": 0,
            "close_ts_ms": 0,
            "symbol": "",
        },
    )
    row["first_ts_ms"] = min(int(row.get("first_ts_ms") or ts_ms), ts_ms)
    row["last_ts_ms"] = max(int(row.get("last_ts_ms") or ts_ms), ts_ms)
    if symbol and not row.get("symbol"):
        row["symbol"] = symbol
    if kind in {"entry.opened", "runtime.position_opened"}:
        existing = int(row.get("entry_ts_ms") or 0)
        row["entry_ts_ms"] = ts_ms if existing <= 0 else min(existing, ts_ms)
    if _kind_is_close_or_flatten(kind):
        row["close_ts_ms"] = max(int(row.get("close_ts_ms") or 0), ts_ms)


def _finalize_position_event_windows(
    windows: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    for row in windows.values():
        if int(row.get("entry_ts_ms") or 0) <= 0:
            row["entry_ts_ms"] = int(row.get("first_ts_ms") or 0)
        if int(row.get("close_ts_ms") or 0) <= 0:
            row["close_ts_ms"] = int(row.get("last_ts_ms") or 0)
    return windows


def _event_ts_ms(event: dict[str, Any]) -> int:
    payload = event.get("payload")
    row = payload if isinstance(payload, dict) else event
    for value in (
        event.get("ts_ms"),
        row.get("ts_ms"),
        row.get("timestamp_ms"),
        row.get("opened_at_ms"),
        row.get("entered_at_ms"),
        row.get("submitted_at_ms"),
        row.get("filled_at_ms"),
    ):
        try:
            ts_ms = int(value or 0)
        except (TypeError, ValueError):
            ts_ms = 0
        if ts_ms > 0:
            return ts_ms
    return 0


def _kind_is_close_or_flatten(kind: str) -> bool:
    text = str(kind or "").lower()
    return bool(
        text.startswith("exit.")
        or "close" in text
        or "flatten" in text
        or "reconciled" in text
    )


def _identity_phase(identity: dict[str, Any]) -> str:
    explicit = str(identity.get("phase") or "").lower()
    if explicit in {"open", "close"}:
        return explicit
    source_kind = str(identity.get("source_kind") or "").lower()
    if source_kind in {"order.submitted", "order.passive_submitted"}:
        return "open"
    text = " ".join(
        str(identity.get(key) or "")
        for key in ("source_kind", "source")
    ).lower()
    if any(token in text for token in ("close", "exit", "backfill", "probe", "truth_gap")):
        return "close"
    if "open" in text or "entry" in text:
        return "open"
    return ""


def _candidate_venue(truth: dict[str, Any], identity: dict[str, Any]) -> str:
    venue = str(identity.get("venue") or "").lower()
    if venue:
        return venue
    leg = str(identity.get("leg") or "").lower()
    if leg == "long":
        return str(truth.get("long_venue") or "").lower()
    if leg == "short":
        return str(truth.get("short_venue") or "").lower()
    return ""


def _iter_order_query_candidates(report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    candidates: list[dict[str, Any]] = []
    stats = {"skipped_already_covered": 0, "skipped_order_filled_source": 0}
    seen: set[tuple[str, str, str, str, str, str]] = set()
    positions = report.get("positions")
    if not isinstance(positions, dict):
        return candidates, stats
    for position_id, truth in sorted(positions.items()):
        if not isinstance(truth, dict):
            continue
        symbol = str(truth.get("symbol") or "").upper()
        identities = truth.get("order_identity_history")
        if not symbol or not isinstance(identities, list):
            continue
        for identity in identities:
            if not isinstance(identity, dict):
                continue
            if str(identity.get("source_kind") or "") == "order.filled":
                stats["skipped_order_filled_source"] += 1
                continue
            phase = _identity_phase(identity)
            if phase not in {"open", "close"}:
                continue
            leg = _candidate_leg(truth, identity)
            if leg not in {"long", "short"}:
                continue
            order_id = str(identity.get("order_id") or "")
            client_order_id = str(identity.get("client_order_id") or "")
            if not order_id and not client_order_id:
                continue
            venue = _candidate_venue(truth, identity)
            if not venue:
                continue
            identity_for_check = dict(identity)
            identity_for_check["leg"] = leg
            if _identity_already_covered(truth, identity_for_check, phase=phase):
                stats["skipped_already_covered"] += 1
                continue
            key = (
                str(position_id),
                phase,
                leg,
                venue,
                order_id,
                client_order_id,
            )
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "position_id": str(position_id),
                    "symbol": symbol,
                    "phase": phase,
                    "leg": leg,
                    "venue": venue,
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                    "source_kind": str(identity.get("source_kind") or ""),
                    "source": str(identity.get("source") or ""),
                    "submitted_at_ms": int(identity.get("submitted_at_ms") or 0),
                }
            )
    return candidates, stats


def _iter_close_query_candidates(report: dict[str, Any]) -> list[dict[str, Any]]:
    candidates, _ = _iter_order_query_candidates(report)
    return [candidate for candidate in candidates if candidate.get("phase") == "close"]


def _candidate_leg(truth: dict[str, Any], identity: dict[str, Any]) -> str:
    leg = str(identity.get("leg") or "").lower()
    if leg in {"long", "short"}:
        return leg
    venue = str(identity.get("venue") or "").lower()
    if venue and venue == str(truth.get("long_venue") or "").lower():
        return "long"
    if venue and venue == str(truth.get("short_venue") or "").lower():
        return "short"
    return ""


def _identity_already_covered(
    truth: dict[str, Any],
    identity: dict[str, Any],
    *,
    phase: str,
) -> bool:
    leg = str(identity.get("leg") or "").lower()
    if leg not in {"long", "short"}:
        return False
    coverage = truth.get(f"{phase}_coverage")
    if not isinstance(coverage, dict):
        return False
    row = coverage.get(leg)
    if not isinstance(row, dict) or row.get("covered") is not True:
        return False
    order_id = str(identity.get("order_id") or "")
    client_order_id = str(identity.get("client_order_id") or "")
    order_ids = {str(item) for item in row.get("order_ids") or [] if str(item)}
    client_order_ids = {str(item) for item in row.get("client_order_ids") or [] if str(item)}
    return bool((order_id and order_id in order_ids) or (client_order_id and client_order_id in client_order_ids))


def _json_value(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    return value


def _fill_event_from_reconciliation(
    candidate: dict[str, Any],
    fill: Any,
) -> dict[str, Any]:
    metadata = getattr(fill, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    venue = str(_json_value(getattr(fill, "venue", None)) or candidate["venue"]).lower()
    side = str(_json_value(getattr(fill, "side", None)) or "").lower()
    order_id = str(getattr(fill, "order_id", "") or candidate.get("order_id") or "")
    client_order_id = str(
        getattr(fill, "client_order_id", "") or candidate.get("client_order_id") or ""
    )
    ts_ms = int(getattr(fill, "filled_at_ms", 0) or time.time() * 1000)
    phase = str(candidate.get("phase") or "close")
    return {
        "ts_ms": ts_ms,
        "kind": "order.filled",
        "payload": {
            "position_id": candidate["position_id"],
            "symbol": str(getattr(fill, "symbol", "") or candidate["symbol"]).upper(),
            "phase": phase,
            "leg": candidate.get("leg") or "",
            "venue": venue,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "side": side,
            "tradeSide": str(metadata.get("tradeSide") or metadata.get("trade_side") or phase),
            "quantity": getattr(fill, "quantity", 0.0) or 0.0,
            "average_price": getattr(fill, "average_price", 0.0) or 0.0,
            "fee_quote": getattr(fill, "fee_quote", 0.0) or 0.0,
            "filled_at_ms": ts_ms,
            "source": f"rebuild_lifecycle_truth_exchange_query_{phase}",
        },
    }


def _fill_event_from_account_reconciliation(
    target: dict[str, Any],
    fill: Any,
    *,
    identity_match_mode: str = "identity",
) -> dict[str, Any]:
    metadata = getattr(fill, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    venue = str(_json_value(getattr(fill, "venue", None)) or target["venue"]).lower()
    side = str(_json_value(getattr(fill, "side", None)) or "").lower()
    order_id = str(getattr(fill, "order_id", "") or "")
    client_order_id = str(getattr(fill, "client_order_id", "") or "")
    ts_ms = int(getattr(fill, "filled_at_ms", 0) or target.get("anchor_ts_ms") or time.time() * 1000)
    phase = str(target.get("phase") or "close")
    trade_side = str(
        metadata.get("tradeSide")
        or metadata.get("trade_side")
        or metadata.get("trade_side_raw")
        or phase
    )
    event = {
        "ts_ms": ts_ms,
        "kind": "order.filled",
        "payload": {
            "position_id": target["position_id"],
            "symbol": str(getattr(fill, "symbol", "") or target["symbol"]).upper(),
            "phase": phase,
            "leg": target.get("leg") or "",
            "venue": venue,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "side": side,
            "tradeSide": trade_side,
            "quantity": getattr(fill, "quantity", 0.0) or 0.0,
            "average_price": getattr(fill, "average_price", 0.0) or 0.0,
            "fee_quote": getattr(fill, "fee_quote", 0.0) or 0.0,
            "filled_at_ms": ts_ms,
            "source": f"rebuild_lifecycle_truth_exchange_account_history_{phase}",
            "trade_id": str(metadata.get("trade_id") or metadata.get("tradeId") or ""),
            "exec_id": str(metadata.get("exec_id") or metadata.get("execId") or ""),
        },
    }
    if identity_match_mode == "fallback":
        event["payload"]["identity_match_mode"] = identity_match_mode
    return event


def _iter_account_history_targets(
    report: dict[str, Any],
    *,
    position_windows: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    positions = report.get("positions")
    if not isinstance(positions, dict):
        return targets
    windows = position_windows or {}
    for position_id, truth in sorted(positions.items()):
        if not isinstance(truth, dict):
            continue
        if str(truth.get("classification") or "") == "phantom_zero_qty_opened":
            continue
        symbol = str(truth.get("symbol") or "").upper()
        if not symbol:
            continue
        target_quantity = _to_float(truth.get("target_quantity"))
        if target_quantity <= 0.0:
            continue
        window = windows.get(str(position_id), {})
        for phase, coverage_key in (("open", "open_coverage"), ("close", "close_coverage")):
            coverage = truth.get(coverage_key)
            if not isinstance(coverage, dict):
                continue
            for leg in ("long", "short"):
                row = coverage.get(leg)
                if not isinstance(row, dict):
                    continue
                if row.get("covered") is True:
                    continue
                filled_qty = _to_float(row.get("filled_qty"))
                missing_qty = max(0.0, target_quantity - filled_qty)
                if missing_qty <= 0.0:
                    continue
                venue = str(truth.get(f"{leg}_venue") or "").lower()
                if not venue:
                    continue
                anchor = _account_history_anchor_ts(window, phase)
                start_time_ms, end_time_ms = _account_history_query_window(anchor)
                identities = _target_order_identities(truth, phase=phase, leg=leg, venue=venue)
                lifecycle_start_ms, lifecycle_end_ms = _account_history_lifecycle_bounds(
                    position_id=str(position_id),
                    symbol=symbol,
                    phase=phase,
                    window=window,
                    all_windows=windows,
                )
                targets.append(
                    {
                        "position_id": str(position_id),
                        "symbol": symbol,
                        "phase": phase,
                        "leg": leg,
                        "venue": venue,
                        "target_quantity": target_quantity,
                        "filled_quantity": filled_qty,
                        "missing_quantity": missing_qty,
                        "expected_side": _expected_side_for_target(phase, leg),
                        "anchor_ts_ms": anchor,
                        "start_time_ms": start_time_ms,
                        "end_time_ms": end_time_ms,
                        "identity_order_ids": identities["order_ids"],
                        "identity_client_order_ids": identities["client_order_ids"],
                        "identity_anchor_ts_ms": identities["submitted_at_ms"],
                        "lifecycle_start_ts_ms": lifecycle_start_ms,
                        "lifecycle_end_ts_ms": lifecycle_end_ms,
                    }
                )
    return targets


def _account_history_anchor_ts(window: dict[str, int], phase: str) -> int:
    if phase == "open":
        return int(window.get("entry_ts_ms") or window.get("first_ts_ms") or 0)
    return int(window.get("close_ts_ms") or window.get("last_ts_ms") or window.get("entry_ts_ms") or 0)


def _account_history_query_window(anchor_ts_ms: int) -> tuple[int | None, int | None]:
    if anchor_ts_ms <= 0:
        return None, None
    half_window_ms = ACCOUNT_HISTORY_QUERY_WINDOW_MS // 2
    now_ms = int(time.time() * 1000)
    start_time_ms = max(0, anchor_ts_ms - half_window_ms)
    end_time_ms = min(anchor_ts_ms + half_window_ms, now_ms)
    if end_time_ms <= start_time_ms:
        start_time_ms = max(0, end_time_ms - ACCOUNT_HISTORY_QUERY_WINDOW_MS)
    return start_time_ms, end_time_ms


def _account_history_lifecycle_bounds(
    *,
    position_id: str,
    symbol: str,
    phase: str,
    window: dict[str, Any],
    all_windows: dict[str, dict[str, Any]],
) -> tuple[int, int]:
    entry_ts_ms = int(window.get("entry_ts_ms") or window.get("first_ts_ms") or 0)
    start_ts_ms = int(window.get("first_ts_ms") or entry_ts_ms or 0)
    if phase == "close" and entry_ts_ms > 0:
        start_ts_ms = entry_ts_ms
    if start_ts_ms > 0:
        start_ts_ms = max(0, start_ts_ms - ACCOUNT_HISTORY_IDENTITY_FALLBACK_WINDOW_MS)
    next_entry_ts_ms = _next_symbol_entry_ts_ms(
        position_id=position_id,
        symbol=symbol,
        entry_ts_ms=entry_ts_ms,
        all_windows=all_windows,
    )
    if next_entry_ts_ms > 0:
        return start_ts_ms, max(
            0,
            next_entry_ts_ms + ACCOUNT_HISTORY_NEXT_ENTRY_BOUNDARY_GRACE_MS,
        )
    end_ts_ms = int(window.get("last_ts_ms") or window.get("close_ts_ms") or 0)
    if end_ts_ms > 0:
        end_ts_ms += ACCOUNT_HISTORY_IDENTITY_FALLBACK_WINDOW_MS
    return start_ts_ms, end_ts_ms


def _next_symbol_entry_ts_ms(
    *,
    position_id: str,
    symbol: str,
    entry_ts_ms: int,
    all_windows: dict[str, dict[str, Any]],
) -> int:
    if entry_ts_ms <= 0 or not symbol:
        return 0
    candidates: list[int] = []
    for other_position_id, other_window in all_windows.items():
        if str(other_position_id) == position_id:
            continue
        other_symbol = str(other_window.get("symbol") or "").upper()
        if other_symbol != symbol:
            continue
        other_entry_ts_ms = int(other_window.get("entry_ts_ms") or 0)
        if other_entry_ts_ms > entry_ts_ms:
            candidates.append(other_entry_ts_ms)
    return min(candidates) if candidates else 0


def _target_order_identities(
    truth: dict[str, Any],
    *,
    phase: str,
    leg: str,
    venue: str,
) -> dict[str, set[str] | list[int]]:
    order_ids: set[str] = set()
    client_order_ids: set[str] = set()
    submitted_at_ms: set[int] = set()
    identities = truth.get("order_identity_history")
    if not isinstance(identities, list):
        return {
            "order_ids": order_ids,
            "client_order_ids": client_order_ids,
            "submitted_at_ms": [],
        }
    for identity in identities:
        if not isinstance(identity, dict):
            continue
        if _identity_phase(identity) != phase:
            continue
        if _candidate_leg(truth, identity) != leg:
            continue
        if _candidate_venue(truth, identity) != venue:
            continue
        order_id = str(identity.get("order_id") or "")
        client_order_id = str(identity.get("client_order_id") or "")
        if order_id:
            order_ids.add(order_id)
        if client_order_id:
            client_order_ids.add(client_order_id)
        submitted_at = int(identity.get("submitted_at_ms") or 0)
        if submitted_at > 0:
            submitted_at_ms.add(submitted_at)
    return {
        "order_ids": order_ids,
        "client_order_ids": client_order_ids,
        "submitted_at_ms": sorted(submitted_at_ms),
    }


def _expected_side_for_target(phase: str, leg: str) -> str:
    if phase == "open" and leg == "long":
        return "buy"
    if phase == "open" and leg == "short":
        return "sell"
    if phase == "close" and leg == "long":
        return "sell"
    if phase == "close" and leg == "short":
        return "buy"
    return ""


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _account_history_fill_matches_target(
    fill: Any,
    target: dict[str, Any],
    *,
    require_identity_match: bool = False,
) -> bool:
    if fill is None or _to_float(getattr(fill, "quantity", 0.0)) <= 0.0:
        return False
    fill_venue = str(_json_value(getattr(fill, "venue", None)) or "").lower()
    if fill_venue and fill_venue != str(target.get("venue") or "").lower():
        return False
    fill_symbol = _canonical_account_history_symbol(str(getattr(fill, "symbol", "") or ""))
    target_symbol = _canonical_account_history_symbol(str(target.get("symbol") or ""))
    if fill_symbol and target_symbol and fill_symbol != target_symbol:
        return False
    side = str(_json_value(getattr(fill, "side", None)) or "").lower()
    expected_side = str(target.get("expected_side") or "").lower()
    metadata = getattr(fill, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    if not _account_history_side_matches_target(side, expected_side, metadata, target):
        return False
    if not _account_history_trade_side_matches(metadata, str(target.get("phase") or "")):
        return False
    if not _account_history_position_side_matches(metadata, str(target.get("leg") or "")):
        return False
    if require_identity_match and _target_has_identity(target):
        return _account_history_fill_identity_matches(fill, target)
    return True


def _canonical_account_history_symbol(raw: str) -> str:
    symbol = (
        str(raw or "")
        .upper()
        .replace("-", "")
        .replace("_", "")
        .replace("/", "")
        .replace(" ", "")
    )
    for suffix in ("PERPETUAL", "SWAP", "PERP", "UMCBL", "DMCBL", "CMCBL"):
        if symbol.endswith(suffix):
            symbol = symbol[: -len(suffix)]
            break
    return symbol


def _account_history_side_matches_target(
    side: str,
    expected_side: str,
    metadata: dict[str, Any],
    target: dict[str, Any],
) -> bool:
    if not expected_side or not side or side == expected_side:
        return True
    venue = str(target.get("venue") or "").lower()
    phase = str(target.get("phase") or "").lower()
    if venue == "bitget" and phase == "close":
        trade_side = str(
            metadata.get("tradeSide")
            or metadata.get("trade_side")
            or metadata.get("trade_side_raw")
            or ""
        ).lower()
        if "close" in trade_side or trade_side.startswith("reduce_"):
            return True
    return False


def _target_has_identity(target: dict[str, Any]) -> bool:
    return bool(target.get("identity_order_ids") or target.get("identity_client_order_ids"))


def _account_history_fill_identity_matches(fill: Any, target: dict[str, Any]) -> bool:
    order_ids: set[str] = target.get("identity_order_ids") or set()
    client_order_ids: set[str] = target.get("identity_client_order_ids") or set()
    if order_ids or client_order_ids:
        fill_order_id = str(getattr(fill, "order_id", "") or "")
        fill_client_id = str(getattr(fill, "client_order_id", "") or "")
        return bool(
            (fill_order_id and fill_order_id in order_ids)
            or (fill_client_id and fill_client_id in client_order_ids)
        )
    return True


def _account_history_trade_side_matches(metadata: dict[str, Any], phase: str) -> bool:
    trade_side = str(
        metadata.get("tradeSide")
        or metadata.get("trade_side")
        or metadata.get("trade_side_raw")
        or ""
    ).lower()
    if not trade_side:
        return True
    if phase == "close":
        return "close" in trade_side or trade_side.startswith("reduce_")
    if phase == "open":
        return trade_side == "open" or trade_side.endswith("_single") or "open" in trade_side
    return True


def _account_history_position_side_matches(metadata: dict[str, Any], leg: str) -> bool:
    position_side = str(metadata.get("positionSide") or metadata.get("position_side") or "").lower()
    if not position_side or position_side in {"both", "net"}:
        return True
    return position_side == leg.lower()


def _account_history_fill_sort_key(fill: Any, anchor_ts_ms: int) -> tuple[int, int]:
    filled_at_ms = int(getattr(fill, "filled_at_ms", 0) or 0)
    if anchor_ts_ms > 0 and filled_at_ms > 0:
        return abs(filled_at_ms - anchor_ts_ms), filled_at_ms
    return 0, filled_at_ms


def _account_history_fill_within_identity_fallback_window(
    fill: Any,
    target: dict[str, Any],
) -> bool:
    anchor_values = list(target.get("identity_anchor_ts_ms") or [])
    anchor_values.append(target.get("anchor_ts_ms"))
    anchors: list[int] = []
    for value in anchor_values:
        try:
            anchor_ts_ms = int(value or 0)
        except (TypeError, ValueError):
            anchor_ts_ms = 0
        if anchor_ts_ms > 0:
            anchors.append(anchor_ts_ms)
    filled_at_ms = int(getattr(fill, "filled_at_ms", 0) or 0)
    if not anchors or filled_at_ms <= 0:
        return False
    return any(
        abs(filled_at_ms - anchor_ts_ms) <= ACCOUNT_HISTORY_IDENTITY_FALLBACK_WINDOW_MS
        for anchor_ts_ms in anchors
    )


def _account_history_fill_within_lifecycle_window(
    fill: Any,
    target: dict[str, Any],
) -> bool:
    filled_at_ms = int(getattr(fill, "filled_at_ms", 0) or 0)
    if filled_at_ms <= 0:
        return True
    start_ts_ms = int(target.get("lifecycle_start_ts_ms") or 0)
    end_ts_ms = int(target.get("lifecycle_end_ts_ms") or 0)
    if start_ts_ms > 0 and filled_at_ms < start_ts_ms:
        return False
    if end_ts_ms > 0 and filled_at_ms > end_ts_ms:
        return False
    return True


def _account_history_event_key(event: dict[str, Any]) -> tuple[str, str, str, str, str, str, str] | None:
    key = _fill_event_identity_key(event)
    if key is None:
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return key
    trade_id = str(payload.get("trade_id") or "")
    exec_id = str(payload.get("exec_id") or "")
    if trade_id:
        return key[:4] + ("trade_id", trade_id, "")
    if exec_id:
        return key[:4] + ("exec_id", exec_id, "")
    return key


def _account_history_exchange_event_key(event: dict[str, Any]) -> tuple[str, ...] | None:
    if str(event.get("kind") or "") != "order.filled":
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    venue = str(payload.get("venue") or payload.get("exchange") or "").lower()
    symbol = str(payload.get("symbol") or "").upper()
    trade_id = str(payload.get("trade_id") or "")
    exec_id = str(payload.get("exec_id") or "")
    order_id = str(payload.get("order_id") or payload.get("orderId") or "")
    client_order_id = str(payload.get("client_order_id") or payload.get("clientOrderId") or "")
    side = str(payload.get("side") or "").lower()
    if trade_id:
        return (venue, symbol, "trade_id", trade_id)
    if exec_id:
        return (venue, symbol, "exec_id", exec_id)
    if order_id or client_order_id:
        return (
            venue,
            symbol,
            "order",
            order_id,
            client_order_id,
            side,
            str(payload.get("filled_at_ms") or event.get("ts_ms") or ""),
        )
    return (
        venue,
        symbol,
        "fill",
        side,
        str(payload.get("quantity") or ""),
        str(payload.get("average_price") or payload.get("price") or ""),
        str(payload.get("filled_at_ms") or event.get("ts_ms") or ""),
    )


def _account_history_has_trade_identity(event: dict[str, Any]) -> bool:
    if str(event.get("kind") or "") != "order.filled":
        return False
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return False
    return bool(
        payload.get("trade_id")
        or payload.get("tradeId")
        or payload.get("exec_id")
        or payload.get("execId")
    )


def _account_history_aggregate_fill_fingerprint(event: dict[str, Any]) -> tuple[str, ...] | None:
    if str(event.get("kind") or "") != "order.filled":
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    order_id = str(payload.get("order_id") or payload.get("orderId") or "")
    client_order_id = str(payload.get("client_order_id") or payload.get("clientOrderId") or "")
    if not order_id and not client_order_id:
        return None
    return (
        str(payload.get("position_id") or payload.get("entry_id") or ""),
        str(payload.get("phase") or ""),
        str(payload.get("leg") or ""),
        str(payload.get("venue") or payload.get("exchange") or "").lower(),
        str(payload.get("symbol") or "").upper(),
        order_id,
        client_order_id,
        str(_to_float(payload.get("quantity"))),
        str(_to_float(payload.get("average_price") or payload.get("price"))),
        str(payload.get("filled_at_ms") or event.get("ts_ms") or ""),
    )


def _load_exchange_query_helpers() -> tuple[
    Callable[[str], Any],
    Callable[..., Any],
    Callable[[], Any],
    Callable[[], Any],
    Callable[[Any], None],
]:
    from scripts.diagnose_live import (  # noqa: PLC0415
        _create_readonly_adapter,
        _create_readonly_rate_limiter,
        _install_readonly_exchange_truth_rate_limit_runtime,
        _load_venue_credential,
        _restore_readonly_exchange_truth_rate_limit_runtime,
    )

    return (
        _load_venue_credential,
        _create_readonly_adapter,
        _create_readonly_rate_limiter,
        _install_readonly_exchange_truth_rate_limit_runtime,
        _restore_readonly_exchange_truth_rate_limit_runtime,
    )


def _load_exchange_truth_environment(unit_dir: str = "/etc/systemd/system") -> list[str]:
    try:
        from scripts.diagnose_live import _load_systemd_environment_files  # noqa: PLC0415

        return _load_systemd_environment_files(unit_dir)
    except Exception:
        return []


def _candidate_query_window(candidate: dict[str, Any]) -> tuple[int | None, int | None]:
    submitted_at_ms = int(candidate.get("submitted_at_ms") or 0)
    if submitted_at_ms <= 0:
        return None, None
    half_window_ms = HISTORICAL_ORDER_QUERY_WINDOW_MS // 2
    return max(0, submitted_at_ms - half_window_ms), submitted_at_ms + half_window_ms


def _supports_time_window_kwargs(fetch: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(fetch)
    except (TypeError, ValueError):
        return False
    for parameter in signature.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
    return "start_time_ms" in signature.parameters or "end_time_ms" in signature.parameters


async def _fetch_order_fill_reconciliation_with_window(
    fetch: Callable[..., Any],
    candidate: dict[str, Any],
) -> tuple[Any, str]:
    start_time_ms, end_time_ms = _candidate_query_window(candidate)
    if start_time_ms is not None and end_time_ms is not None:
        if _supports_time_window_kwargs(fetch):
            fill = await fetch(
                candidate["symbol"],
                candidate["order_id"],
                candidate["client_order_id"],
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            return fill, "windowed"
        fill = await fetch(
            candidate["symbol"],
            candidate["order_id"],
            candidate["client_order_id"],
        )
        return fill, "window_unsupported"
    fill = await fetch(
        candidate["symbol"],
        candidate["order_id"],
        candidate["client_order_id"],
    )
    return fill, "unwindowed"


async def query_exchange_fill_events(
    report: dict[str, Any],
    *,
    credential_loader: Callable[[str], Any] | None = None,
    adapter_factory: Callable[..., Any] | None = None,
    rate_limiter_factory: Callable[[], Any] | None = None,
    install_runtime: Callable[[], Any] | None = None,
    restore_runtime: Callable[[Any], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Probe exchange read-only order truth for close identities in a report."""

    if (
        credential_loader is None
        or adapter_factory is None
        or rate_limiter_factory is None
        or install_runtime is None
        or restore_runtime is None
    ):
        (
            credential_loader,
            adapter_factory,
            rate_limiter_factory,
            install_runtime,
            restore_runtime,
        ) = _load_exchange_query_helpers()

    candidates, candidate_stats = _iter_order_query_candidates(report)
    summary: dict[str, Any] = {
        "enabled": True,
        "candidate_count": len(candidates),
        "attempted": 0,
        "filled": 0,
        "not_found": 0,
        "credential_missing": 0,
        "adapter_unavailable": 0,
        "reconciliation_unavailable": 0,
        "windowed_query_count": 0,
        "windowed_query_unsupported_count": 0,
        "unwindowed_query_count": 0,
        **candidate_stats,
        "errors": [],
    }
    fill_events: list[dict[str, Any]] = []
    adapter_cache: dict[str, Any] = {}
    missing_credentials: set[str] = set()
    previous_runtime = install_runtime()
    try:
        for candidate in candidates:
            summary["attempted"] += 1
            venue = candidate["venue"]
            if venue in missing_credentials:
                summary["credential_missing"] += 1
                continue
            adapter = adapter_cache.get(venue)
            if adapter is None:
                credential = credential_loader(venue)
                if credential is None:
                    missing_credentials.add(venue)
                    summary["credential_missing"] += 1
                    continue
                adapter = adapter_factory(
                    venue,
                    credential,
                    rate_limiter=rate_limiter_factory(),
                )
                if adapter is None:
                    summary["adapter_unavailable"] += 1
                    continue
                adapter_cache[venue] = adapter
            fetch = getattr(adapter, "fetch_order_fill_reconciliation", None)
            if not callable(fetch):
                summary["reconciliation_unavailable"] += 1
                continue
            try:
                fill, window_state = await _fetch_order_fill_reconciliation_with_window(
                    fetch,
                    candidate,
                )
                if window_state == "windowed":
                    summary["windowed_query_count"] += 1
                elif window_state == "window_unsupported":
                    summary["windowed_query_unsupported_count"] += 1
                else:
                    summary["unwindowed_query_count"] += 1
            except Exception as exc:  # pragma: no cover - defensive for live adapters.
                if _is_exchange_order_not_found_exception(candidate, exc):
                    summary["not_found"] += 1
                    summary["not_found_from_error_count"] = (
                        int(summary.get("not_found_from_error_count") or 0) + 1
                    )
                    continue
                summary["errors"].append(
                    {
                        "position_id": candidate["position_id"],
                        "venue": venue,
                        "order_id": candidate["order_id"],
                        "client_order_id": candidate["client_order_id"],
                        "error": str(exc),
                    }
                )
                continue
            if fill is None or float(getattr(fill, "quantity", 0.0) or 0.0) <= 0.0:
                summary["not_found"] += 1
                continue
            fill_events.append(_fill_event_from_reconciliation(candidate, fill))
            summary["filled"] += 1
    finally:
        restore_runtime(previous_runtime)
    return fill_events, summary


async def query_exchange_account_history_fill_events(
    report: dict[str, Any],
    *,
    position_windows: dict[str, dict[str, int]] | None = None,
    existing_events: list[dict[str, Any]] | None = None,
    credential_loader: Callable[[str], Any] | None = None,
    adapter_factory: Callable[..., Any] | None = None,
    rate_limiter_factory: Callable[[], Any] | None = None,
    install_runtime: Callable[[], Any] | None = None,
    restore_runtime: Callable[[Any], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Probe exchange account fill history for missing lifecycle legs."""

    if (
        credential_loader is None
        or adapter_factory is None
        or rate_limiter_factory is None
        or install_runtime is None
        or restore_runtime is None
    ):
        (
            credential_loader,
            adapter_factory,
            rate_limiter_factory,
            install_runtime,
            restore_runtime,
        ) = _load_exchange_query_helpers()

    targets = _iter_account_history_targets(report, position_windows=position_windows)
    summary: dict[str, Any] = {
        "enabled": True,
        "target_count": len(targets),
        "attempted": 0,
        "filled": 0,
        "not_found": 0,
        "credential_missing": 0,
        "adapter_unavailable": 0,
        "account_history_unavailable": 0,
        "identity_fallback_targets": 0,
        "identity_fallback_filled": 0,
        "identity_fallback_time_filtered": 0,
        "lifecycle_time_filtered": 0,
        "existing_aggregate_trade_skipped": 0,
        "errors": [],
    }
    fill_events: list[dict[str, Any]] = []
    adapter_cache: dict[str, Any] = {}
    fill_cache: dict[tuple[str, str, int | None, int | None], list[Any]] = {}
    missing_credentials: set[str] = set()
    seed_events = list(existing_events or [])
    seen = {
        _account_history_event_key(event)
        for event in seed_events
        if _account_history_event_key(event)
    }
    seen_exchange = {
        _account_history_exchange_event_key(event)
        for event in seed_events
        if _account_history_exchange_event_key(event)
    }
    existing_aggregate_fingerprints: dict[tuple[str, ...], int] = {}
    for event in seed_events:
        if _account_history_has_trade_identity(event):
            continue
        fingerprint = _account_history_aggregate_fill_fingerprint(event)
        if fingerprint:
            existing_aggregate_fingerprints[fingerprint] = (
                int(existing_aggregate_fingerprints.get(fingerprint) or 0) + 1
            )
    previous_runtime = install_runtime()
    try:
        for target in targets:
            summary["attempted"] += 1
            venue = target["venue"]
            if venue in missing_credentials:
                summary["credential_missing"] += 1
                continue
            adapter = adapter_cache.get(venue)
            if adapter is None:
                credential = credential_loader(venue)
                if credential is None:
                    missing_credentials.add(venue)
                    summary["credential_missing"] += 1
                    continue
                adapter = adapter_factory(
                    venue,
                    credential,
                    rate_limiter=rate_limiter_factory(),
                )
                if adapter is None:
                    summary["adapter_unavailable"] += 1
                    continue
                adapter_cache[venue] = adapter
            fetch = _account_history_fetcher(adapter)
            if not callable(fetch):
                summary["account_history_unavailable"] += 1
                continue
            cache_key = (
                venue,
                target["symbol"],
                target.get("start_time_ms"),
                target.get("end_time_ms"),
            )
            try:
                if cache_key not in fill_cache:
                    fill_cache[cache_key] = await fetch(
                        target["symbol"],
                        start_time_ms=target.get("start_time_ms"),
                        end_time_ms=target.get("end_time_ms"),
                    )
                history_fills = fill_cache[cache_key]
            except Exception as exc:  # pragma: no cover - defensive for live adapters.
                summary["errors"].append(
                    {
                        "position_id": target["position_id"],
                        "venue": venue,
                        "symbol": target["symbol"],
                        "phase": target["phase"],
                        "leg": target["leg"],
                        "error": str(exc),
                    }
                )
                continue
            identity_required = _target_has_identity(target)
            exact_matched = [
                fill
                for fill in history_fills
                if _account_history_fill_matches_target(
                    fill,
                    target,
                    require_identity_match=identity_required,
                )
            ]
            lifecycle_exact_matched = [
                fill
                for fill in exact_matched
                if _account_history_fill_within_lifecycle_window(fill, target)
            ]
            summary["lifecycle_time_filtered"] += max(
                0,
                len(exact_matched) - len(lifecycle_exact_matched),
            )
            matched_rows: list[tuple[Any, str]] = [
                (fill, "identity" if identity_required else "no_identity")
                for fill in lifecycle_exact_matched
            ]
            if identity_required:
                fallback_candidates = [
                    fill
                    for fill in history_fills
                    if _account_history_fill_matches_target(
                        fill,
                        target,
                        require_identity_match=False,
                    )
                ]
                lifecycle_candidates = [
                    fill
                    for fill in fallback_candidates
                    if _account_history_fill_within_lifecycle_window(fill, target)
                ]
                summary["lifecycle_time_filtered"] += max(
                    0,
                    len(fallback_candidates) - len(lifecycle_candidates),
                )
                fallback_matched = [
                    fill
                    for fill in lifecycle_candidates
                    if _account_history_fill_within_identity_fallback_window(fill, target)
                ]
                summary["identity_fallback_time_filtered"] += max(
                    0,
                    len(lifecycle_candidates) - len(fallback_matched),
                )
                if fallback_matched:
                    summary["identity_fallback_targets"] += 1
                matched_rows.extend((fill, "fallback") for fill in fallback_matched)
            matched_rows.sort(
                key=lambda row: _account_history_fill_sort_key(
                    row[0],
                    int(target.get("anchor_ts_ms") or 0),
                )
            )
            accumulated = 0.0
            emitted_for_target = 0
            emitted_fallback_for_target = 0
            needed_qty = float(target.get("missing_quantity") or 0.0)
            for fill, match_mode in matched_rows:
                event = _fill_event_from_account_reconciliation(
                    target,
                    fill,
                    identity_match_mode=match_mode,
                )
                aggregate_fingerprint = _account_history_aggregate_fill_fingerprint(event)
                if aggregate_fingerprint:
                    aggregate_count = int(
                        existing_aggregate_fingerprints.get(aggregate_fingerprint) or 0
                    )
                    if aggregate_count > 0:
                        existing_aggregate_fingerprints[aggregate_fingerprint] = aggregate_count - 1
                        summary["existing_aggregate_trade_skipped"] += 1
                        continue
                key = _account_history_event_key(event)
                exchange_key = _account_history_exchange_event_key(event)
                if exchange_key and exchange_key in seen_exchange:
                    continue
                if key and key in seen:
                    continue
                if exchange_key:
                    seen_exchange.add(exchange_key)
                if key:
                    seen.add(key)
                fill_events.append(event)
                emitted_for_target += 1
                if match_mode == "fallback":
                    emitted_fallback_for_target += 1
                accumulated += _to_float(getattr(fill, "quantity", 0.0))
                if needed_qty > 0.0 and accumulated >= needed_qty * QTY_TOLERANCE:
                    break
            if emitted_for_target:
                summary["filled"] += emitted_for_target
                summary["identity_fallback_filled"] += emitted_fallback_for_target
            else:
                summary["not_found"] += 1
    finally:
        restore_runtime(previous_runtime)
    return fill_events, summary


def _account_history_fetcher(adapter: Any) -> Callable[..., Awaitable[list[Any]]] | None:
    fetch = getattr(adapter, "fetch_account_fill_reconciliations", None)
    if callable(fetch):
        return fetch
    transport = getattr(adapter, "_transport", None)
    fetch = getattr(transport, "fetch_account_fill_reconciliations", None)
    if callable(fetch):
        return fetch
    private = getattr(adapter, "_private", None)
    fetch = getattr(private, "fetch_account_fill_reconciliations", None)
    if callable(fetch):
        return fetch
    return None


def _is_exchange_order_not_found_exception(candidate: dict[str, Any], exc: Exception) -> bool:
    venue = str(candidate.get("venue") or "").lower()
    if venue != "binance":
        return False
    text = str(exc).lower()
    return "-2013" in text or "order does not exist" in text


async def query_exchange_fill_events_until_stable(
    events: list[dict[str, Any]],
    *,
    position_ids: Iterable[str] | None,
    context_events: list[dict[str, Any]] | None = None,
    position_windows: dict[str, dict[str, Any]] | None = None,
    max_passes: int = 3,
    query_func: Callable[[dict[str, Any]], Awaitable[tuple[list[dict[str, Any]], dict[str, Any]]]]
    | None = None,
    account_history_query_func: Callable[
        [dict[str, Any], dict[str, dict[str, Any]]],
        Awaitable[tuple[list[dict[str, Any]], dict[str, Any]]],
    ]
    | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Rebuild lifecycle truth and keep probing newly exposed order identities."""

    query = query_func or query_exchange_fill_events
    scoped_position_ids = set(position_ids or [])
    report = build_exchange_truth_lifecycle(events, position_ids=scoped_position_ids)
    queried_fill_events: list[dict[str, Any]] = []
    pass_summaries: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str, str]] = set()
    for event in events:
        key = _fill_event_identity_key(event)
        if key:
            seen.add(key)
    if position_windows is not None:
        windows = _finalize_position_event_windows(dict(position_windows))
    else:
        context = context_events if context_events is not None else events
        windows = position_event_windows(context)

    for pass_index in range(1, max(1, max_passes) + 1):
        fill_events, summary = await query(report)
        summary = dict(summary)
        summary["pass_index"] = pass_index
        pass_summaries.append(summary)
        new_fill_events: list[dict[str, Any]] = []
        for event in fill_events:
            key = _fill_event_identity_key(event)
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            new_fill_events.append(event)
        if not new_fill_events:
            break
        queried_fill_events.extend(new_fill_events)
        report = build_exchange_truth_lifecycle(
            events + queried_fill_events,
            position_ids=scoped_position_ids,
        )

    if account_history_query_func is not None:
        account_history_events, account_history_summary = await account_history_query_func(report, windows)
    elif query_func is None:
        account_history_events, account_history_summary = await query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            existing_events=events + queried_fill_events,
        )
    else:
        account_history_events, account_history_summary = (
            [],
            {"enabled": False, "reason": "skipped_for_injected_order_query"},
        )
    new_account_history_events: list[dict[str, Any]] = []
    account_history_seen: set[tuple[str, str, str, str, str, str, str]] = set()
    for event in events + queried_fill_events:
        key = _account_history_event_key(event)
        if key:
            account_history_seen.add(key)
    for event in account_history_events:
        key = _account_history_event_key(event) or _fill_event_identity_key(event)
        if key and key in account_history_seen:
            continue
        if key:
            account_history_seen.add(key)
        new_account_history_events.append(event)
    if new_account_history_events:
        queried_fill_events.extend(new_account_history_events)
        report = build_exchange_truth_lifecycle(
            events + queried_fill_events,
            position_ids=scoped_position_ids,
        )

    exchange_query_summary = _merge_exchange_query_summaries(
        pass_summaries,
        synthetic_fill_event_count=len(queried_fill_events),
    )
    exchange_query_summary["account_history"] = account_history_summary
    return report, queried_fill_events, exchange_query_summary


def _fill_event_identity_key(event: dict[str, Any]) -> tuple[str, str, str, str, str, str, str] | None:
    if str(event.get("kind") or "") != "order.filled":
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    return (
        str(payload.get("position_id") or payload.get("entry_id") or ""),
        str(payload.get("phase") or ""),
        str(payload.get("leg") or ""),
        str(payload.get("venue") or payload.get("exchange") or "").lower(),
        str(payload.get("order_id") or payload.get("orderId") or ""),
        str(payload.get("client_order_id") or payload.get("clientOrderId") or ""),
        str(payload.get("filled_at_ms") or event.get("ts_ms") or ""),
    )


def _merge_exchange_query_summaries(
    pass_summaries: list[dict[str, Any]],
    *,
    synthetic_fill_event_count: int,
) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "enabled": True,
        "pass_count": len(pass_summaries),
        "synthetic_fill_event_count": synthetic_fill_event_count,
        "pass_summaries": pass_summaries,
        "errors": [],
    }
    for summary in pass_summaries:
        for key, value in summary.items():
            if key in {"enabled", "errors", "pass_index"}:
                continue
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                merged[key] = int(merged.get(key) or 0) + value
        errors = summary.get("errors")
        if isinstance(errors, list):
            merged["errors"].extend(errors)
    return merged


def correction_event(position_id: str, truth: dict[str, Any]) -> dict[str, Any]:
    return {
        "seq": 0,
        "run_id": "manual-lifecycle-truth-{}".format(int(time.time() * 1000)),
        "ts_ms": int(time.time() * 1000),
        "kind": "accounting.lifecycle_truth_rebuilt",
        "payload": {
            "position_id": position_id,
            "source": "rebuild_lifecycle_truth",
            "classification": truth.get("classification", ""),
            "project_record_status": truth.get("project_record_status", ""),
            "truth": truth,
        },
    }


def write_json(path: Path, payload: Any) -> None:
    _atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
    )


def write_jsonl(path: Path, events: Iterable[dict[str, Any]]) -> None:
    def write_events(handle: Any) -> None:
        for event in events:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")

    _atomic_write(path, write_events)


def _atomic_write_text(path: Path, text: str) -> None:
    """Replace an audit artifact only after its complete content is durable."""
    _atomic_write(path, lambda handle: handle.write(text))


def _atomic_write(path: Path, write: Callable[[Any], None]) -> None:
    """Replace an audit artifact only after its complete content is durable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            write(handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
    finally:
        temporary_path.unlink(missing_ok=True)


def append_events(runtime_dir: Path, events: list[dict[str, Any]]) -> Path:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    path = runtime_dir / "live-events.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
    return path


def _run_gate(cmd: list[str]) -> tuple[bool, str]:
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return proc.returncode == 0, proc.stdout.strip()[-4000:]


def assert_apply_gates() -> None:
    gates = [
        [sys.executable, "scripts/verify_deploy_manifest.py", "--check", "/opt/lightfee-v2"],
        [sys.executable, "scripts/check_process_singleton.py", "--strict"],
        [sys.executable, "scripts/verify_production_services.py", "--json"],
        [
            sys.executable,
            "scripts/diagnose_live.py",
            "--json",
            "--since-deploy",
            "--max-events",
            "200000",
        ],
    ]
    failures: list[dict[str, str]] = []
    for cmd in gates:
        ok, output = _run_gate(cmd)
        if not ok:
            failures.append({"cmd": " ".join(cmd), "output": output})
    if failures:
        raise SystemExit(json.dumps({"apply_allowed": False, "failures": failures}, indent=2))


def apply_report_blockers(
    report: dict[str, Any],
    *,
    position_ids: list[str] | None,
    expected_complete: int | None = None,
    expected_phantom_zero: int | None = None,
    expected_exchange_bad: int | None = None,
) -> list[str]:
    blockers: list[str] = []
    if position_ids is None:
        blockers.append("positions_file_required_for_apply")
    elif not position_ids:
        blockers.append("positions_file_empty")

    exchange_query = report.get("exchange_query")
    if isinstance(exchange_query, dict) and exchange_query.get("enabled", True):
        exchange_checks = [("exchange_query", exchange_query)]
        account_history = exchange_query.get("account_history")
        if isinstance(account_history, dict) and account_history.get("enabled", True):
            exchange_checks.append(("exchange_query_account_history", account_history))
        for prefix, query_row in exchange_checks:
            if query_row.get("errors"):
                blockers.append(f"{prefix}_errors_present")
            for key in (
                "credential_missing",
                "adapter_unavailable",
                "reconciliation_unavailable",
                "account_history_unavailable",
            ):
                try:
                    value = int(query_row.get(key) or 0)
                except (TypeError, ValueError):
                    value = 0
                if value > 0:
                    blockers.append(f"{prefix}_{key}:{value}")

    positions = report.get("positions")
    if isinstance(positions, dict):
        for position_id, truth in sorted(positions.items()):
            if not isinstance(truth, dict):
                continue
            classification = str(truth.get("classification") or "")
            if classification in {
                "exchange_lifecycle_incomplete",
                "evidence_incomplete",
            }:
                blockers.append(f"{classification}:{position_id}")
            pnl = truth.get("pnl") if isinstance(truth.get("pnl"), dict) else {}
            if (
                classification == "exchange_lifecycle_complete"
                and not list(pnl.get("evidence_refs") or [])
            ):
                blockers.append(f"missing_pnl_evidence_refs:{position_id}")

    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    if expected_complete is not None:
        actual = int(summary.get("exchange_lifecycle_complete") or 0)
        if actual != expected_complete:
            blockers.append(f"expected_complete_mismatch:{actual}!={expected_complete}")
    if expected_phantom_zero is not None:
        actual = int(summary.get("phantom_zero_qty_opened") or 0)
        if actual != expected_phantom_zero:
            blockers.append(f"expected_phantom_zero_mismatch:{actual}!={expected_phantom_zero}")
    if expected_exchange_bad is not None:
        actual = int(summary.get("exchange_lifecycle_incomplete") or 0) + int(
            summary.get("evidence_incomplete") or 0
        )
        if actual != expected_exchange_bad:
            blockers.append(f"expected_exchange_bad_mismatch:{actual}!={expected_exchange_bad}")
    return sorted(set(blockers))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_DIR)
    parser.add_argument("--events", type=Path, action="append", default=[])
    parser.add_argument(
        "--correction-events",
        type=Path,
        action="append",
        default=[],
        help=(
            "append-only correction audit JSON from an earlier --apply; "
            "requires a valid HMAC signature; read-only replay input and incompatible with --apply"
        ),
    )
    parser.add_argument(
        "--allow-legacy-unsigned-correction",
        action="store_true",
        help=(
            "allow forensic reading of a pre-HMAC JSON-array or schema-v1 correction audit; "
            "the replay remains deliberately non-passing because its integrity is unverified"
        ),
    )
    parser.add_argument("--positions-file", type=Path)
    parser.add_argument("--history", choices=["current", "all"], default="all")
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--expected-complete", type=int)
    parser.add_argument("--expected-phantom-zero", type=int)
    parser.add_argument("--expected-exchange-bad", type=int)
    parser.add_argument(
        "--query-exchange",
        dest="query_exchange",
        action="store_true",
        default=True,
        help="query read-only exchange order truth for close order identities (default)",
    )
    parser.add_argument(
        "--no-query-exchange",
        dest="query_exchange",
        action="store_false",
        help="rebuild from local event evidence only",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True)
    mode.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.apply and args.correction_events:
        raise SystemExit("--correction-events is read-only and cannot be combined with --apply")
    if args.correction_events and args.query_exchange:
        raise SystemExit("--correction-events requires --no-query-exchange for canonical audit replay")
    if args.correction_events and args.positions_file is None:
        raise SystemExit("--correction-events requires a non-empty --positions-file")
    if args.correction_events:
        missing_expected = [
            option
            for option, value in (
                ("--expected-complete", args.expected_complete),
                ("--expected-phantom-zero", args.expected_phantom_zero),
                ("--expected-exchange-bad", args.expected_exchange_bad),
            )
            if value is None
        ]
        if missing_expected:
            raise SystemExit(
                "--correction-events requires exact expected counts: " + ", ".join(missing_expected)
            )
    event_files = list(args.events)
    if not event_files:
        event_files = discover_event_files(args.runtime_dir, args.history)
    position_ids = read_position_ids(args.positions_file)
    if args.correction_events and not position_ids:
        raise SystemExit("--correction-events requires a non-empty --positions-file")
    event_position_filter = set(position_ids or []) if position_ids is not None else None
    exchange_truth_env_files_loaded: list[str] = []
    events = read_jsonl_events(event_files, position_ids=event_position_filter)
    correction_audit = CorrectionAuditInput(events=[], audits=[])
    if args.correction_events:
        try:
            correction_audit = read_correction_events(
                list(args.correction_events),
                position_ids=event_position_filter,
                allow_legacy_unsigned=bool(args.allow_legacy_unsigned_correction),
            )
        except CorrectionAuditError as exc:
            raise SystemExit(str(exc)) from exc
    correction_events = correction_audit.events
    events.extend(correction_events)
    context_windows: dict[str, dict[str, Any]] | None = None
    if event_position_filter:
        context_windows = read_position_event_windows(event_files)
    queried_fill_events: list[dict[str, Any]] = []
    report = build_exchange_truth_lifecycle(
        events,
        position_ids=set(position_ids or []),
    )
    if args.query_exchange:
        exchange_truth_env_files_loaded = _load_exchange_truth_environment()
        report, queried_fill_events, exchange_query_summary = asyncio.run(
            query_exchange_fill_events_until_stable(
                events,
                position_ids=set(position_ids or []),
                position_windows=context_windows,
            )
        )
        report["exchange_query"] = exchange_query_summary
    else:
        report["exchange_query"] = {"enabled": False}
        report["correction_replay_applied_count"] = apply_correction_replay_snapshots(
            report,
            correction_events,
        )
    report["inputs"] = {
        "runtime_dir": str(args.runtime_dir),
        "event_files": [str(path) for path in event_files],
        "correction_event_files": [str(path) for path in args.correction_events],
        "positions_file": str(args.positions_file) if args.positions_file else "",
        "position_ids": position_ids or [],
        "dry_run": not args.apply,
        "query_exchange": bool(args.query_exchange),
    }
    if correction_audit.audits:
        report["correction_audits"] = correction_audit.audits
    report["exchange_truth_env_files_loaded"] = exchange_truth_env_files_loaded
    report["apply_blockers"] = apply_report_blockers(
        report,
        position_ids=position_ids,
        expected_complete=args.expected_complete,
        expected_phantom_zero=args.expected_phantom_zero,
        expected_exchange_bad=args.expected_exchange_bad,
    )
    legacy_audits = [
        str(audit["path"])
        for audit in correction_audit.audits
        if audit.get("integrity") == "legacy_unsigned"
    ]
    if legacy_audits:
        report["correction_replay_integrity_blockers"] = [
            f"legacy_unsigned_correction_audit:{path}" for path in legacy_audits
        ]
        report["apply_blockers"] = sorted(
            set(report["apply_blockers"] + report["correction_replay_integrity_blockers"])
        )

    if args.apply:
        if report["apply_blockers"]:
            raise SystemExit(
                json.dumps(
                    {
                        "apply_allowed": False,
                        "blockers": report["apply_blockers"],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
            )
        assert_apply_gates()
        correction_events = [
            correction_event(position_id, truth)
            for position_id, truth in sorted(report.get("positions", {}).items())
            if isinstance(truth, dict)
        ]
        append_items = [*queried_fill_events, *correction_events]
        correction_path = ROOT / CORRECTION_DIR / "{}.json".format(int(time.time() * 1000))
        try:
            hmac_key = _correction_audit_hmac_key(correction_path.parent, create=True)
            assert hmac_key is not None
            write_json(correction_path, correction_audit_payload(append_items, hmac_key=hmac_key))
        except CorrectionAuditError as exc:
            raise SystemExit(str(exc)) from exc
        replay_path = correction_path.with_suffix(".replay.jsonl")
        write_jsonl(replay_path, [*events, *append_items])
        journal_path = append_events(args.runtime_dir, append_items)
        report["apply"] = {
            "correction_path": str(correction_path),
            "replay_path": str(replay_path),
            "runtime_journal_path": str(journal_path),
            "event_count": len(append_items),
            "queried_fill_event_count": len(queried_fill_events),
            "correction_event_count": len(correction_events),
        }

    if args.output_json:
        write_json(args.output_json, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    expected_args_present = any(
        value is not None
        for value in (
            args.expected_complete,
            args.expected_phantom_zero,
            args.expected_exchange_bad,
        )
    )
    if not args.apply and expected_args_present and report["apply_blockers"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
