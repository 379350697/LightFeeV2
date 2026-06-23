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
