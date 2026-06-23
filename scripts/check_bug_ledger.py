#!/usr/bin/env python3
"""Validate the lightweight bug ledger routing contract."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


DEFAULT_RECENT_SINCE = "2026-06-19"
DEFAULT_MAX_INDEX_LINES = 220
ARCHIVE_RELATIVE_PATH = (
    "docs/bugs/archive/BUG_INDEX_HISTORY_2026-05-15_to_2026-06-18.md"
)
REQUIRED_ARCHIVE_PHRASES = (
    "Latest Aster 5018 pre-submit headroom",
    "Latest Quote freshness and candidate-scoped OI evidence",
    "Latest stale risk-state alignment and order-error closure",
)


@dataclass(frozen=True)
class RecentRow:
    date: str
    cluster: str
    status: str
    start_here: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        default=".",
        help="Repository root. Default: current directory.",
    )
    parser.add_argument(
        "--recent-since",
        default=DEFAULT_RECENT_SINCE,
        help=f"Earliest date allowed in Recent Closures. Default: {DEFAULT_RECENT_SINCE}.",
    )
    parser.add_argument(
        "--max-index-lines",
        type=int,
        default=DEFAULT_MAX_INDEX_LINES,
        help=f"Maximum allowed BUG_INDEX.md line count. Default: {DEFAULT_MAX_INDEX_LINES}.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON.",
    )
    return parser.parse_args()


def split_markdown_row(line: str) -> list[str]:
    return [part.strip() for part in line.strip().strip("|").split("|")]


def find_recent_rows(index_text: str) -> tuple[list[RecentRow], list[str]]:
    violations: list[str] = []
    lines = index_text.splitlines()
    try:
        start = lines.index("## Recent Closures")
    except ValueError:
        return [], ["missing Recent Closures section"]

    table_lines: list[str] = []
    for line in lines[start + 1 :]:
        if line.startswith("## "):
            break
        if line.startswith("|"):
            table_lines.append(line)
        elif table_lines and not line.strip():
            break

    data_lines = [
        line
        for line in table_lines
        if not re.match(r"\|\s*-+\s*\|", line) and not line.startswith("| Date ")
    ]
    rows: list[RecentRow] = []
    for line in data_lines:
        cells = split_markdown_row(line)
        if len(cells) < 5:
            violations.append(f"malformed Recent Closures row: {line}")
            continue
        date, cluster, _family, status, start_here = cells[:5]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            violations.append(f"Recent Closures row has invalid date: {line}")
            continue
        rows.append(RecentRow(date=date, cluster=cluster, status=status, start_here=start_here))

    if not rows:
        violations.append("Recent Closures table has no dated rows")
    return rows, violations


def validate_recent_rows(rows: list[RecentRow], recent_since: str) -> list[str]:
    violations: list[str] = []
    for row in rows:
        label = f"{row.date} {row.cluster}"
        if row.date < recent_since:
            violations.append(f"{label} is older than recent window {recent_since}")
        if not re.search(r"\bCL-\d+\b", row.cluster):
            violations.append(f"{label} is missing CL-* cluster id")
        if not row.status:
            violations.append(f"{label} has empty status")
        if "daily/" not in row.start_here:
            violations.append(f"{label} is missing a daily link")
        status_lower = row.status.lower()
        if "local green" in status_lower and "deploy pending" not in status_lower:
            violations.append(f"{label} says local green without deploy pending")
        if ("deployed" in status_lower or "cloud verified" in status_lower) and (
            "daily/" not in row.start_here
        ):
            violations.append(f"{label} is deployed/cloud verified without daily link")
    return violations


def count_daily_clusters(root: Path) -> tuple[int, int]:
    daily_files = sorted((root / "docs/bugs/daily").glob("*.md"))
    cluster_count = 0
    for path in daily_files:
        cluster_count += len(re.findall(r"^## Cluster\s+", path.read_text(), re.M))
    return len(daily_files), cluster_count


def validate(root: Path, recent_since: str, max_index_lines: int) -> dict[str, object]:
    index_path = root / "docs/bugs/BUG_INDEX.md"
    archive_path = root / ARCHIVE_RELATIVE_PATH
    violations: list[str] = []

    if not index_path.exists():
        violations.append("missing docs/bugs/BUG_INDEX.md")
        index_text = ""
    else:
        index_text = index_path.read_text()

    index_lines = len(index_text.splitlines())
    if index_lines > max_index_lines:
        violations.append(f"BUG_INDEX.md has {index_lines} lines, max is {max_index_lines}")

    rows, row_parse_violations = find_recent_rows(index_text)
    violations.extend(row_parse_violations)
    violations.extend(validate_recent_rows(rows, recent_since))

    if not archive_path.exists():
        violations.append(f"missing {ARCHIVE_RELATIVE_PATH}")
    else:
        archive_text = archive_path.read_text()
        for phrase in REQUIRED_ARCHIVE_PHRASES:
            if phrase not in archive_text:
                violations.append(f"archive missing historical phrase: {phrase}")

    daily_files, daily_clusters = count_daily_clusters(root)
    return {
        "violations": violations,
        "index_lines": index_lines,
        "daily_files": daily_files,
        "daily_clusters": daily_clusters,
        "recent_rows": len(rows),
        "archive": ARCHIVE_RELATIVE_PATH,
        "recent_since": recent_since,
        "max_index_lines": max_index_lines,
    }


def main() -> int:
    args = parse_args()
    payload = validate(Path(args.root), args.recent_since, args.max_index_lines)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "bug ledger: "
            f"{payload['index_lines']} index lines, "
            f"{payload['daily_files']} daily files, "
            f"{payload['daily_clusters']} daily clusters, "
            f"{payload['recent_rows']} recent rows"
        )
        for violation in payload["violations"]:
            print(f"- {violation}")
    return 1 if payload["violations"] else 0


if __name__ == "__main__":
    sys.exit(main())
