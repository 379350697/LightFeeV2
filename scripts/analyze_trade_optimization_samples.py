#!/usr/bin/env python3
"""Read-only historical normal trade sample analysis for optimization review."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lightfee.offline.trade_optimization import (  # noqa: E402
    build_trade_optimization_analysis,
    read_trade_optimization_events,
    render_markdown_report,
    sample_rows_for_csv,
)


CSV_COLUMNS = [
    "position_id",
    "symbol",
    "route",
    "long_venue",
    "short_venue",
    "normality_source",
    "verification_status",
    "open_ts_ms",
    "close_ts_ms",
    "hold_duration_ms",
    "quantity",
    "notional_quote",
    "selected_edge_bps",
    "time_to_funding_ms",
    "funding_capture_ratio",
    "fee_drag_bps",
    "close_markout_bps",
    "funding_pnl_bps",
    "realized_edge_after_cost_bps",
    "price_pnl_quote",
    "funding_pnl_quote",
    "entry_fee_quote",
    "exit_fee_quote",
    "rebate_adjustment_quote",
    "net_pnl_quote",
    "pnl_notional_quote",
    "net_pnl_bps",
    "close_path",
    "entry_spread_bps",
    "entry_spread_bucket",
    "passive_wait_cost_observed",
    "coverage_gaps",
]


def discover_event_files(runtime_dir: Path, history: str) -> list[Path]:
    files = sorted(runtime_dir.glob("live-events*.jsonl*"))
    if history == "all":
        files.extend(sorted((runtime_dir / "archive").glob("live-events*.jsonl*")))
    return sorted(set(files), key=lambda path: str(path))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_cell(row.get(key)) for key in CSV_COLUMNS})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, default=Path("runtime"))
    parser.add_argument("--events", type=Path, action="append", default=[])
    parser.add_argument("--history", choices=["current", "all"], default="current")
    parser.add_argument("--normal-only", action="store_true")
    parser.add_argument("--include-counterfactual", action="store_true")
    parser.add_argument("--market-match-window-ms", type=int, default=300_000)
    parser.add_argument("--json", dest="json_path", type=Path)
    parser.add_argument("--csv", dest="csv_path", type=Path)
    parser.add_argument("--report-md", dest="report_md_path", type=Path)
    parser.add_argument("--stdout", action="store_true", help="print JSON report to stdout")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    event_files = list(args.events)
    if not event_files:
        event_files = discover_event_files(args.runtime_dir, args.history)
    events, event_filter = read_trade_optimization_events(
        event_files,
        include_counterfactual=bool(args.include_counterfactual),
        market_match_window_ms=int(args.market_match_window_ms),
    )
    report = build_trade_optimization_analysis(
        events,
        normal_only=bool(args.normal_only),
        include_counterfactual=bool(args.include_counterfactual),
        market_match_window_ms=int(args.market_match_window_ms),
    )
    report["inputs"] = {
        "runtime_dir": str(args.runtime_dir),
        "history": args.history,
        "event_files": [str(path) for path in event_files],
        "event_filter": event_filter,
    }

    if args.json_path:
        write_json(args.json_path, report)
    if args.csv_path:
        write_csv(args.csv_path, sample_rows_for_csv(report))
    if args.report_md_path:
        write_text(args.report_md_path, render_markdown_report(report))
    if args.stdout or not any([args.json_path, args.csv_path, args.report_md_path]):
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
