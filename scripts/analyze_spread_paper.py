#!/usr/bin/env python3
"""Generate an epoch-safe acceptance report from spread paper JSONL journals."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

# Run against the checked-out source when invoked directly.  Without this,
# Python places ``scripts/`` ahead of the repository root and can import an
# unrelated editable installation of LightFeeV2 instead of this report's model.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lightfee.offline.spread_paper_analysis import (
    DEFAULT_ALLOWED_OPPORTUNITY_LABELS,
    DEFAULT_MODEL_EPOCH,
    analyze_spread_paper_events,
    spread_paper_report_dict,
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(value)
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", action="append", type=Path, required=True)
    parser.add_argument("--excluded-symbol", action="append", default=[])
    parser.add_argument("--allowed-label", action="append", default=[])
    parser.add_argument("--model-epoch", default=DEFAULT_MODEL_EPOCH)
    parser.add_argument("--include-nonofficial", action="store_true")
    parser.add_argument("--allow-non-taker-taker", action="store_true")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    records: list[dict[str, Any]] = []
    for path in args.events:
        records.extend(_read_jsonl(path))
    report = analyze_spread_paper_events(
        records,
        excluded_symbols=args.excluded_symbol,
        allowed_opportunity_labels=(
            args.allowed_label or DEFAULT_ALLOWED_OPPORTUNITY_LABELS
        ),
        model_epoch=args.model_epoch,
        include_nonofficial=args.include_nonofficial,
        require_taker_taker=not args.allow_non_taker_taker,
    )
    payload = spread_paper_report_dict(report)
    rendered = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
