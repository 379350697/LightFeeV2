from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from lightfee.offline.acceptance_evidence import build_signed_manifest


def test_funding_canary_script_fails_closed_and_writes_json_report(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    output = tmp_path / "report.json"
    manifest = tmp_path / "events.manifest.json"
    events.write_text("", encoding="utf-8")
    manifest.write_text(
        json.dumps(
            build_signed_manifest(
                [events],
                report_kind="funding_canary",
                secret="secret",
            )
        ),
        encoding="utf-8",
    )
    root = Path(__file__).resolve().parents[2]

    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "analyze_funding_canary.py"),
                "--events",
                str(events),
                "--evidence-manifest",
                str(manifest),
                "--output",
                str(output),
            ],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY": "secret"},
    )

    assert result.returncode == 1
    assert json.loads(result.stdout)["promotion_ready"] is False
    persisted = json.loads(output.read_text(encoding="utf-8"))
    assert persisted["complete_loop_count"] == 0
    assert "insufficient_complete_reconciled_truth_flat_loops" in persisted[
        "promotion_blockers"
    ]
    assert persisted["acceptance_evidence"]["verified"] is True
