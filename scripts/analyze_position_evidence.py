#!/usr/bin/env python3
"""Build a per-position lifecycle/ledger evidence matrix from JSONL inputs."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from lightfee.offline.position_evidence import (
    build_position_evidence_matrix,
    derive_ledger_rows_from_events,
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict):
                records.append(record)
    return records


def _filter_window(
    records: list[dict[str, Any]],
    *,
    since_ms: int | None,
    until_ms: int | None,
) -> list[dict[str, Any]]:
    if since_ms is None and until_ms is None:
        return records
    filtered: list[dict[str, Any]] = []
    for record in records:
        try:
            ts_ms = int(record.get("ts_ms") or record.get("time_ms") or 0)
        except (TypeError, ValueError):
            ts_ms = 0
        if since_ms is not None and ts_ms < since_ms:
            continue
        if until_ms is not None and ts_ms > until_ms:
            continue
        filtered.append(record)
    return filtered


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )


def _write_tsv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            delimiter="\t",
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _cell(row.get(key)) for key in columns})


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True, action="append", type=Path)
    parser.add_argument("--ledger", action="append", type=Path, default=[])
    parser.add_argument("--since-ms", type=int, default=None)
    parser.add_argument("--until-ms", type=int, default=None)
    parser.add_argument("--quick-flat-ms", type=int, default=120_000)
    parser.add_argument("--derive-ledger-from-events", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()

    events: list[dict[str, Any]] = []
    for path in args.events:
        events.extend(_read_jsonl(path))
    events = _filter_window(events, since_ms=args.since_ms, until_ms=args.until_ms)
    ledger_rows: list[dict[str, Any]] = []
    for path in args.ledger:
        ledger_rows.extend(_read_jsonl(path))
    if args.derive_ledger_from_events:
        ledger_rows.extend(derive_ledger_rows_from_events(events))
    ledger_rows = _filter_window(
        ledger_rows,
        since_ms=args.since_ms,
        until_ms=args.until_ms,
    )

    matrix = build_position_evidence_matrix(
        events=events,
        ledger_rows=ledger_rows,
        quick_flat_threshold_ms=args.quick_flat_ms,
    )
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        _write_json(args.output_dir / "position_evidence_matrix.json", matrix)
        _write_tsv(
            args.output_dir / "unattributed_ledger_rows.tsv",
            matrix["unattributed_ledger_rows"],
            [
                "ts_ms",
                "venue",
                "symbol",
                "income_type",
                "amount",
                "fee",
                "order_id",
                "client_order_id",
                "trade_id",
                "owner_id",
                "candidate_owner_id",
                "match_confidence",
                "unattributed_reason",
                "root_cause",
                "solution",
                "evidence_refs",
            ],
        )
        _write_tsv(
            args.output_dir / "abnormal_positions.tsv",
            matrix["abnormal_positions"],
            [
                "position_id",
                "symbol",
                "venues",
                "classification",
                "open_ts_ms",
                "terminal_ts_ms",
                "duration_ms",
                "terminal_reason",
                "close_source",
                "abnormal_evidence",
                "reasons",
                "order_ids",
                "client_order_ids",
                "financials",
                "ledger",
                "lifecycle_completeness",
                "root_cause",
                "solution",
            ],
        )
        _write_tsv(
            args.output_dir / "normal_lifecycle_ledger_gaps.tsv",
            matrix["normal_lifecycle_ledger_gaps"],
            [
                "position_id",
                "symbol",
                "venues",
                "classification",
                "open_ts_ms",
                "terminal_ts_ms",
                "duration_ms",
                "missing",
                "terminal_reason",
                "close_source",
                "financials",
                "ledger",
                "lifecycle_completeness",
                "root_cause",
                "solution",
            ],
        )
    print(json.dumps(matrix, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
