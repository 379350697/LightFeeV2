#!/usr/bin/env python3
"""Validate account-fee evidence without accessing an exchange account.

The strategy deliberately does not scrape public fee pages or invent a fee
tier.  This helper only verifies that a locally supplied private-account API
snapshot/private-fill reconciliation has the strict schema, covers the named
venues, and is still fresh enough for canary/official-paper admission.
"""

from __future__ import annotations

import argparse
import json
import time

from lightfee.strategy.fee_evidence import (
    FEE_EVIDENCE_SCHEMA_VERSION,
    TRUSTED_FEE_EVIDENCE_KEY_ID,
    load_fee_evidence,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="account-fee evidence JSON path")
    parser.add_argument(
        "--max-age-ms",
        type=int,
        default=None,
        help=(
            "maximum evidence age; defaults to 5 days for funding schema-v4 "
            "and 24 hours for strict spread schema-v3"
        ),
    )
    parser.add_argument(
        "--require-integrity",
        action="store_true",
        help="fail unless the schema-v3 fixed-key HMAC verifies",
    )
    parser.add_argument(
        "--require-venue",
        action="append",
        default=[],
        help="venue that must be covered; repeat for every canary/paper venue",
    )
    parser.add_argument(
        "--require-symbol",
        default="",
        help="canonical funding symbol that schema-v4 evidence must cover",
    )
    parser.add_argument(
        "--require-account-identity",
        action="append",
        default=[],
        metavar="VENUE=SHA256",
        help="require a schema-v3 venue/account identity binding; repeat as needed",
    )
    parser.add_argument(
        "--now-ms",
        type=int,
        default=0,
        help="test-only evaluation timestamp; default is current wall clock",
    )
    args = parser.parse_args()
    now_ms = int(args.now_ms or time.time() * 1000)
    max_age_ms = (
        int(args.max_age_ms)
        if args.max_age_ms is not None
        else 5 * 24 * 60 * 60 * 1000
    )
    evidence = load_fee_evidence(
        args.path,
        now_ms=now_ms,
        max_age_ms=max_age_ms,
    )
    if args.max_age_ms is None and evidence.schema_version == FEE_EVIDENCE_SCHEMA_VERSION:
        max_age_ms = 24 * 60 * 60 * 1000
        evidence = load_fee_evidence(
            args.path,
            now_ms=now_ms,
            max_age_ms=max_age_ms,
        )
    venues = [str(venue).strip().lower() for venue in args.require_venue if str(venue).strip()]
    required_symbol = str(args.require_symbol or "").strip().upper()
    expected_identities: dict[str, str] = {}
    for raw in args.require_account_identity:
        venue, separator, identity_hash = str(raw or "").partition("=")
        venue = venue.strip().lower()
        identity_hash = identity_hash.strip().lower()
        if not separator or not venue or venue in expected_identities:
            parser.error("--require-account-identity must be unique VENUE=SHA256")
        expected_identities[venue] = identity_hash
    identity_binding_valid = (
        not expected_identities
        or (
            evidence.schema_version == FEE_EVIDENCE_SCHEMA_VERSION
            and evidence.integrity_key_id == TRUSTED_FEE_EVIDENCE_KEY_ID
            and evidence.identity_matches(expected_identities, *expected_identities)
        )
    )
    symbol_authority_valid = (
        not required_symbol
        or (
            bool(venues)
            and evidence.account_authoritative_for(
                expected_identities,
                *venues,
                symbol=required_symbol,
            )
        )
    )
    valid = (
        evidence.loaded
        and evidence.complete_for(*venues)
        and (not args.require_integrity or evidence.integrity_verified is True)
        and identity_binding_valid
        and symbol_authority_valid
    )
    print(
        json.dumps(
            {
                "valid": valid,
                "reason": evidence.reason,
                "source_path": evidence.source_path,
                "max_age_ms": max_age_ms,
                "required_venues": venues,
                "covered_venues": sorted(evidence.schedules),
                "observed_at_ms": evidence.observed_at_ms_for(*venues)
                if venues
                else 0,
                "source": evidence.source_for(*venues) if venues else "",
                "document_sha256": evidence.document_sha256,
                "integrity_verified": evidence.integrity_verified,
                "integrity_key_id": evidence.integrity_key_id,
                "local_file_verified": evidence.local_file_verified,
                "account_fee_evidence_authoritative": (
                    evidence.account_authoritative_for(
                        expected_identities,
                        *venues,
                        symbol=required_symbol,
                    )
                    if venues
                    else False
                ),
                "required_symbol": required_symbol,
                "symbol_authority_valid": symbol_authority_valid,
                "required_account_identity_hashes": expected_identities,
                "account_identity_binding_valid": identity_binding_valid,
            },
            sort_keys=True,
        )
    )
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
