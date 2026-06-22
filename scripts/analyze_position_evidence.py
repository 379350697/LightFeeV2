#!/usr/bin/env python3
"""Build a per-position lifecycle/ledger evidence matrix from JSONL inputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from lightfee.offline.position_evidence import build_position_evidence_matrix


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True, action="append", type=Path)
    parser.add_argument("--ledger", action="append", type=Path, default=[])
    parser.add_argument("--since-ms", type=int, default=None)
    parser.add_argument("--until-ms", type=int, default=None)
    parser.add_argument("--quick-flat-ms", type=int, default=120_000)
    args = parser.parse_args()

    events: list[dict[str, Any]] = []
    for path in args.events:
        events.extend(_read_jsonl(path))
    ledger_rows: list[dict[str, Any]] = []
    for path in args.ledger:
        ledger_rows.extend(_read_jsonl(path))

    matrix = build_position_evidence_matrix(
        events=_filter_window(events, since_ms=args.since_ms, until_ms=args.until_ms),
        ledger_rows=_filter_window(
            ledger_rows,
            since_ms=args.since_ms,
            until_ms=args.until_ms,
        ),
        quick_flat_threshold_ms=args.quick_flat_ms,
    )
    print(json.dumps(matrix, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
