#!/usr/bin/env python3
"""Generate an epoch-safe acceptance report from spread paper JSONL journals."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import json

# Run against the checked-out source when invoked directly.  Without this,
# Python places ``scripts/`` ahead of the repository root and can import an
# unrelated editable installation of LightFeeV2 instead of this report's model.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lightfee.offline.spread_paper_analysis import (  # noqa: E402
    DEFAULT_ALLOWED_OPPORTUNITY_LABELS,
    analyze_spread_paper_events,
    spread_paper_report_dict,
)
from lightfee.offline.acceptance_evidence import (  # noqa: E402
    AcceptanceEvidenceError,
    read_verified_jsonl,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", action="append", type=Path, required=True)
    parser.add_argument(
        "--evidence-manifest",
        type=Path,
        help="signed manifest binding the exact JSONL byte streams to this report",
    )
    parser.add_argument("--excluded-symbol", action="append", default=[])
    parser.add_argument("--allowed-label", action="append", default=[])
    parser.add_argument(
        "--model-epoch",
        required=True,
        help="immutable research epoch to analyze; no legacy default is allowed",
    )
    parser.add_argument("--include-nonofficial", action="store_true")
    parser.add_argument("--allow-non-taker-taker", action="store_true")
    parser.add_argument(
        "--out-of-sample-only",
        action="store_true",
        help="exclude records not explicitly labelled out_of_sample",
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    if str(args.model_epoch).startswith("v3_"):
        if not args.out_of_sample_only:
            parser.error("--out-of-sample-only is required for a v3 model epoch")
        if args.include_nonofficial:
            parser.error("--include-nonofficial is not valid for a v3 model epoch")
        if args.allow_non_taker_taker:
            parser.error("--allow-non-taker-taker is not valid for a v3 model epoch")
    if args.evidence_manifest is None:
        parser.error("--evidence-manifest is required")

    try:
        records, evidence = read_verified_jsonl(
            args.events,
            report_kind="spread_paper",
            manifest_path=args.evidence_manifest,
        )
    except AcceptanceEvidenceError as exc:
        parser.error(str(exc))
    report = analyze_spread_paper_events(
        records,
        excluded_symbols=args.excluded_symbol,
        allowed_opportunity_labels=(
            args.allowed_label or DEFAULT_ALLOWED_OPPORTUNITY_LABELS
        ),
        model_epoch=args.model_epoch,
        include_nonofficial=args.include_nonofficial,
        require_taker_taker=not args.allow_non_taker_taker,
        require_out_of_sample=args.out_of_sample_only,
        source_evidence_verified=True,
    )
    payload = spread_paper_report_dict(report)
    payload["acceptance_evidence"] = evidence.as_dict()
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
