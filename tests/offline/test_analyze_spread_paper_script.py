from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_spread_paper_analysis_script_emits_strict_json_report(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    output = tmp_path / "report.json"
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
                    "calculation_version": "spread_paper_v2",
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
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "analyze_spread_paper.py"),
            "--events",
            str(events),
            "--output",
            str(output),
        ],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["closed_count"] == 1
    assert json.loads(output.read_text(encoding="utf-8"))["profit_factor"] is None
