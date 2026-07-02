from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUG_INDEX = ROOT / "docs/bugs/BUG_INDEX.md"
BUG_INDEX_ARCHIVE = (
    ROOT / "docs/bugs/archive/BUG_INDEX_HISTORY_2026-05-15_to_2026-06-18.md"
)
CHECK_SCRIPT = ROOT / "scripts/check_bug_ledger.py"


def test_cl105_bug_docs_track_latest_cloud_verified_deploy():
    bug_index = BUG_INDEX.read_text()
    daily = (ROOT / "docs/bugs/daily/2026-06-21.md").read_text()

    assert "`5a2fd42` deployed/cloud verified" in bug_index
    assert "`5a2fd42` deployed/cloud verified" in daily
    assert "residual diagnostic closure local green/pending deploy" not in bug_index
    assert "residual diagnostic closure local green/pending deploy" not in daily


def test_bug_index_is_lightweight_recent_router():
    bug_index = BUG_INDEX.read_text()

    assert len(bug_index.splitlines()) <= 220
    assert "Latest Aster 5018 pre-submit headroom" not in bug_index
    assert (
        "archive/BUG_INDEX_HISTORY_2026-05-15_to_2026-06-18.md"
        in bug_index
    )


def test_bug_index_archive_preserves_historical_latest_narrative():
    archive = BUG_INDEX_ARCHIVE.read_text()

    assert "Latest Aster 5018 pre-submit headroom" in archive
    assert "Latest Quote freshness and candidate-scoped OI evidence" in archive
    assert "Latest stale risk-state alignment and order-error closure" in archive


def test_recent_deployed_clusters_do_not_remain_deploy_pending():
    bug_index = BUG_INDEX.read_text()
    daily = (ROOT / "docs/bugs/daily/2026-06-30.md").read_text()

    for cluster in ("CL-140", "CL-141", "CL-142"):
        index_row = next(
            line for line in bug_index.splitlines() if f"| {cluster} |" in line
        )
        assert "deployed/cloud verified" in index_row
        assert "deploy pending" not in index_row

    for cluster in ("CL-140", "CL-141", "CL-142"):
        section_start = daily.index(f"## Cluster {cluster}")
        section_end = daily.find("\n## Cluster ", section_start + 1)
        section = daily[section_start:] if section_end < 0 else daily[section_start:section_end]
        assert "Status: deployed/cloud verified" in section
        assert "Status: local verified; deploy pending" not in section


def test_bug_ledger_checker_json_reports_clean_governance():
    result = subprocess.run(
        [sys.executable, str(CHECK_SCRIPT), "--json"],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["violations"] == []
    assert payload["index_lines"] <= 220
    assert payload["daily_files"] >= 39
    assert payload["daily_clusters"] >= 140
    assert payload["recent_rows"] >= 10
    assert "pending_rows" in payload
    assert "stale_pending_rows" in payload
    assert "status_drift_rows" in payload


def test_bug_ledger_checker_json_reports_pending_clusters_without_status_drift():
    result = subprocess.run(
        [
            sys.executable,
            str(CHECK_SCRIPT),
            "--json",
            "--as-of-date",
            "2026-07-02",
            "--stale-pending-days",
            "7",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    pending_clusters = {row["cluster"] for row in payload["pending_rows"]}
    stale_clusters = {row["cluster"] for row in payload["stale_pending_rows"]}

    assert "CL-139" in pending_clusters
    assert "CL-110" in stale_clusters
    assert payload["status_drift_rows"] == []
