from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from lightfee.offline.acceptance_evidence import build_signed_manifest


def test_spread_paper_analysis_script_emits_strict_json_report(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    output = tmp_path / "report.json"
    manifest = tmp_path / "events.manifest.json"
    events.write_text(
        json.dumps(
            {
                "kind": "opportunity.paper_closed",
                "payload": {
                    "paper_id": "spread:BTCUSDT:binance->aster:1000",
                    "candidate_id": "spread:BTCUSDT:aster->binance",
                    "registered_at_ms": 1_000,
                    "evaluated_at_ms": 2_000,
                    "symbol": "BTCUSDT",
                    "candidate_opportunity_label": "spread_reversion",
                    "paper_net_quote": 1.0,
                    "paper_gross_quote": 1.02,
                    "paper_fee_quote": 0.01,
                    "paper_slippage_quote": 0.01,
                    "paper_funding_quote": 0.0,
                    "model_epoch": "v2_signed_reversion",
                    "calculation_version": "spread_paper_v3",
                        "journal_schema_version": 8,
                    "official_pnl": True,
                    "paper_unpriced": False,
                    "paper_order_status": "FILLED",
                    "paper_entry_mode": "long_taker:short_taker",
                    "paper_exit_mode": "long_taker:short_taker",
                    "acceptance_eligible": True,
                    "paper_control_group": False,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    manifest.write_text(
        json.dumps(
            build_signed_manifest(
                [events],
                report_kind="spread_paper",
                secret="secret",
            )
        ),
        encoding="utf-8",
    )
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "analyze_spread_paper.py"),
                "--events",
                str(events),
                "--evidence-manifest",
                str(manifest),
                "--model-epoch",
            "v2_signed_reversion",
            "--output",
            str(output),
        ],
        cwd=root,
        check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY": "secret"},
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["closed_count"] == 1
    assert json.loads(output.read_text(encoding="utf-8"))["profit_factor"] is None
    assert json.loads(result.stdout)["acceptance_evidence"]["verified"] is True


def test_spread_paper_analysis_script_requires_epoch_and_v3_out_of_sample_flag(
    tmp_path: Path,
) -> None:
    events = tmp_path / "events.jsonl"
    events.write_text("", encoding="utf-8")
    root = Path(__file__).resolve().parents[2]
    command = [
        sys.executable,
        str(root / "scripts" / "analyze_spread_paper.py"),
        "--events",
        str(events),
    ]

    missing_epoch = subprocess.run(
        command, cwd=root, check=False, capture_output=True, text=True
    )
    v3_without_oos = subprocess.run(
        [*command, "--model-epoch", "v3_cost_normalized_reversion"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert missing_epoch.returncode == 2
    assert "--model-epoch" in missing_epoch.stderr
    assert v3_without_oos.returncode == 2
    assert "--out-of-sample-only" in v3_without_oos.stderr


def test_spread_paper_analysis_script_rejects_nonacceptance_v3_overrides(
    tmp_path: Path,
) -> None:
    events = tmp_path / "events.jsonl"
    events.write_text("", encoding="utf-8")
    root = Path(__file__).resolve().parents[2]
    command = [
        sys.executable,
        str(root / "scripts" / "analyze_spread_paper.py"),
        "--events",
        str(events),
        "--model-epoch",
        "v3_cost_normalized_reversion",
        "--out-of-sample-only",
    ]

    include_nonofficial = subprocess.run(
        [*command, "--include-nonofficial"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    non_taker = subprocess.run(
        [*command, "--allow-non-taker-taker"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert include_nonofficial.returncode == 2
    assert "--include-nonofficial is not valid" in include_nonofficial.stderr
    assert non_taker.returncode == 2
    assert "--allow-non-taker-taker is not valid" in non_taker.stderr
