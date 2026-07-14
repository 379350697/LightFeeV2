#!/usr/bin/env python3
"""Create a signed source manifest for an offline acceptance report."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lightfee.offline.acceptance_evidence import (  # noqa: E402
    AcceptanceEvidenceError,
    build_signed_manifest,
    trusted_acceptance_integrity,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", action="append", required=True, type=Path)
    parser.add_argument(
        "--report-kind",
        required=True,
        choices=("funding_canary", "spread_paper"),
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    key_env, _key_id = trusted_acceptance_integrity(args.report_kind)
    secret = os.environ.get(key_env)
    if not secret:
        parser.error("acceptance evidence integrity key unavailable")
    try:
        manifest = build_signed_manifest(
            args.events,
            report_kind=args.report_kind,
            secret=secret,
        )
    except AcceptanceEvidenceError as exc:
        parser.error(str(exc))
    rendered = json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
