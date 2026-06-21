from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_cl105_bug_docs_track_latest_cloud_verified_deploy():
    bug_index = (ROOT / "docs/bugs/BUG_INDEX.md").read_text()
    daily = (ROOT / "docs/bugs/daily/2026-06-21.md").read_text()

    assert "`5a2fd42` deployed/cloud verified" in bug_index
    assert "`5a2fd42` deployed/cloud verified" in daily
    assert "residual diagnostic closure local green/pending deploy" not in bug_index
    assert "residual diagnostic closure local green/pending deploy" not in daily
