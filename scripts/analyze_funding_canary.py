#!/usr/bin/env python3
"""Generate a fail-closed promotion report from funding canary JSONL events."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lightfee.offline.funding_canary_analysis import (  # noqa: E402
    analyze_funding_canary_events,
    funding_canary_report_dict,
)
from lightfee.offline.acceptance_evidence import (  # noqa: E402
    AcceptanceEvidenceError,
    read_verified_jsonl,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", action="append", required=True, type=Path)
    parser.add_argument(
        "--evidence-manifest",
        type=Path,
        help="signed manifest binding the exact JSONL byte streams to this report",
    )
    parser.add_argument(
        "--approved-policy-manifest",
        type=Path,
        help=(
            "HMAC-signed v2 policy approval binding cohort, caps, floors, and venue pairs"
        ),
    )
    parser.add_argument("--required-closed-loops", type=int, default=30)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.evidence_manifest is None:
        parser.error("--evidence-manifest is required")
    try:
        records, evidence = read_verified_jsonl(
            args.events,
            report_kind="funding_canary",
            manifest_path=args.evidence_manifest,
        )
    except AcceptanceEvidenceError as exc:
        parser.error(str(exc))
    approved_policy = None
    if args.approved_policy_manifest is not None:
        try:
            loaded_policy = json.loads(
                args.approved_policy_manifest.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            parser.error(f"invalid approved policy manifest: {exc}")
        if not isinstance(loaded_policy, dict):
            parser.error("approved policy manifest must contain one JSON object")
        approved_policy = loaded_policy
    report = analyze_funding_canary_events(
        records,
        # The analyzer applies the immutable 30-loop floor as well.  Keep the
        # requested value in the CLI for a future stricter release, but never
        # allow an operator to downgrade this canary by passing ``1``.
        required_closed_loops=max(int(args.required_closed_loops), 30),
        source_evidence_verified=True,
        approved_policy=approved_policy,
    )
    payload = funding_canary_report_dict(report)
    payload["acceptance_evidence"] = evidence.as_dict()
    rendered = json.dumps(payload, sort_keys=True, indent=2, allow_nan=False)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if report.promotion_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
