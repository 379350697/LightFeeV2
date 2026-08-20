#!/usr/bin/env python3
"""Check the lightweight bug-status tracker against a deployed Git SHA.

The checker deliberately only validates facts that can stay lightweight and
objective: tracker shape, status vocabulary, the recorded production SHA, and
whether a row claiming deployment has its fixing commit in that SHA.

Usage:
  python scripts/check_bug_ledger.py --deployed-sha <production-.deploy_version>
"""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


TABLE_HEADER = [
    "ID",
    "Status",
    "Fix commit",
    "Regression evidence",
    "Production evidence / next condition",
    "History",
]
TRACKED_STATUSES = {
    "detected",
    "root-cause-confirmed",
    "local-green",
    "deployed-awaiting-verification",
    "closed",
    "superseded",
}
DEPLOYED_STATUSES = {"deployed-awaiting-verification", "closed"}
REGRESSION_STATUSES = DEPLOYED_STATUSES | {"local-green"}
SHA_RE = re.compile(r"`([0-9a-f]{7,40})`")
MARKDOWN_LINK_RE = re.compile(r"^\[[^]\n]+\]\([^)\n]+\)$")
RECORDED_DEPLOY_RE = re.compile(
    r"^- Last production SHA checked:\s+`([0-9a-f]{7,40})`\s*$",
    re.MULTILINE,
)


@dataclass(frozen=True)
class LedgerRow:
    bug_id: str
    status: str
    fix_commit: str
    regression_evidence: str
    production_evidence: str
    history: str


@dataclass(frozen=True)
class Tracker:
    recorded_deploy_sha: str
    rows: list[LedgerRow]


class LedgerFormatError(ValueError):
    """The status tracker is missing a required machine-readable fact."""


def repo_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent.parent,
    )
    return Path(result.stdout.strip())


def _table_cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def read_tracker(path: Path) -> Tracker:
    text = path.read_text(encoding="utf-8")
    deploy_match = RECORDED_DEPLOY_RE.search(text)
    if deploy_match is None:
        raise LedgerFormatError("missing 'Last production SHA checked' record")

    lines = text.splitlines()
    try:
        header_index = lines.index("| " + " | ".join(TABLE_HEADER) + " |")
    except ValueError as exc:
        raise LedgerFormatError("missing Current Batch table with the required header") from exc

    rows: list[LedgerRow] = []
    for line in lines[header_index + 2 :]:
        if not line.startswith("|"):
            break
        cells = _table_cells(line)
        if len(cells) != len(TABLE_HEADER):
            raise LedgerFormatError(f"malformed status row: {line}")
        fix_match = SHA_RE.search(cells[2])
        if fix_match is None:
            raise LedgerFormatError(f"{cells[0]} has no backticked fix commit")
        rows.append(
            LedgerRow(
                bug_id=cells[0],
                status=cells[1],
                fix_commit=fix_match.group(1),
                regression_evidence=cells[3],
                production_evidence=cells[4],
                history=cells[5],
            )
        )

    if not rows:
        raise LedgerFormatError("Current Batch must contain at least one row")
    return Tracker(recorded_deploy_sha=deploy_match.group(1), rows=rows)


def git_commit(root: Path, sha: str) -> str:
    result = subprocess.run(
        ["git", "rev-parse", f"{sha}^{{commit}}"],
        capture_output=True,
        text=True,
        cwd=root,
    )
    if result.returncode != 0:
        raise LedgerFormatError(f"commit is not available locally: {sha}")
    return result.stdout.strip()


def commit_is_ancestor(root: Path, ancestor: str, descendant: str) -> bool:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=root,
    )
    return result.returncode == 0


def validate_tracker(
    tracker: Tracker,
    resolve_commit: Callable[[str], str],
    is_ancestor: Callable[[str, str], bool],
) -> list[str]:
    """Return all tracker consistency errors without performing I/O."""
    errors: list[str] = []
    bug_ids: set[str] = set()
    try:
        recorded_deploy = resolve_commit(tracker.recorded_deploy_sha)
    except LedgerFormatError as exc:
        return [str(exc)]

    for row in tracker.rows:
        if row.bug_id in bug_ids:
            errors.append(f"duplicate bug id: {row.bug_id}")
        bug_ids.add(row.bug_id)
        if row.status not in TRACKED_STATUSES:
            errors.append(f"{row.bug_id}: unknown status '{row.status}'")
        if not MARKDOWN_LINK_RE.fullmatch(row.history.strip()):
            errors.append(f"{row.bug_id}: history must link to the incident evidence")
        if row.status in REGRESSION_STATUSES and not row.regression_evidence.strip():
            errors.append(f"{row.bug_id}: {row.status} requires regression evidence")
        if row.status in DEPLOYED_STATUSES and not row.production_evidence.strip():
            errors.append(f"{row.bug_id}: {row.status} requires production evidence")
        if row.status == "superseded" and "CL-" not in row.production_evidence:
            errors.append(f"{row.bug_id}: superseded rows must name their replacement CL")

        try:
            fix_commit = resolve_commit(row.fix_commit)
        except LedgerFormatError as exc:
            errors.append(f"{row.bug_id}: {exc}")
            continue
        if row.status in DEPLOYED_STATUSES and not is_ancestor(
            fix_commit, recorded_deploy
        ):
            errors.append(
                f"{row.bug_id}: status '{row.status}' claims deployment, but "
                f"{row.fix_commit} is not in recorded deploy {tracker.recorded_deploy_sha}"
            )
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tracker",
        type=Path,
        help="path to the status tracker (defaults to docs/bugs/ACTIVE.md)",
    )
    parser.add_argument(
        "--deployed-sha",
        required=True,
        help="actual production .deploy_version to compare with the recorded SHA",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    tracker_path = args.tracker or root / "docs/bugs/ACTIVE.md"

    try:
        tracker = read_tracker(tracker_path)
        errors = validate_tracker(
            tracker,
            resolve_commit=lambda sha: git_commit(root, sha),
            is_ancestor=lambda older, newer: commit_is_ancestor(root, older, newer),
        )
        recorded_deploy = git_commit(root, tracker.recorded_deploy_sha)
        actual_deploy = git_commit(root, args.deployed_sha)
        if actual_deploy != recorded_deploy:
            errors.append(
                "recorded production SHA differs from supplied production SHA: "
                f"{tracker.recorded_deploy_sha} != {args.deployed_sha}"
            )
    except (LedgerFormatError, OSError, subprocess.CalledProcessError) as exc:
        errors = [str(exc)]

    if errors:
        print("BUG LEDGER CHECK: FAIL")
        for error in errors:
            print(f"- {error}")
        return 1

    counts: dict[str, int] = {}
    for row in tracker.rows:
        counts[row.status] = counts.get(row.status, 0) + 1
    status_summary = ", ".join(
        f"{status}={count}" for status, count in sorted(counts.items())
    )
    print("BUG LEDGER CHECK: PASS")
    print(f"- tracker: {tracker_path}")
    print(f"- recorded production SHA: {recorded_deploy}")
    print("- supplied production SHA matches the recorded deployment")
    print(f"- rows: {len(tracker.rows)} ({status_summary})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
