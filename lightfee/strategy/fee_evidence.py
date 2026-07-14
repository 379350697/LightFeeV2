"""Account-scoped fee schedule evidence used by entry and paper economics.

Configured fee tiers are useful conservative defaults, but they are not proof
of the fee tier of the account that will actually trade.  This module makes
that distinction explicit: an account-fee evidence file is a short-lived,
auditable input produced from a private account fee endpoint or a reconciled
private-fill export.  It is deliberately data-only; it never submits an
exchange request or mutates trading state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
import json
from math import isfinite
import os
from pathlib import Path
from typing import Mapping


FEE_EVIDENCE_SCHEMA_VERSION = 3
_ACCEPTED_SOURCES = frozenset({"account_fee_api", "private_fill"})
_INTEGRITY_ALGORITHM = "hmac-sha256"
# Live admission never trusts a TOML-selected secret.  The release/CI
# environment owns this fixed key reference.
TRUSTED_FEE_EVIDENCE_HMAC_ENV = "LIGHTFEE_FEE_EVIDENCE_HMAC_KEY"
TRUSTED_FEE_EVIDENCE_KEY_ID = "lightfee-fee-evidence-v3"


@dataclass(frozen=True)
class FeeScheduleEvidence:
    """One account-specific maker/taker schedule for a venue."""

    venue: str
    taker_fee_bps: float
    maker_fee_bps: float
    observed_at_ms: int
    source: str
    evidence_ref: str
    account_identity_hash: str = ""


@dataclass(frozen=True)
class FeeEvidenceBook:
    """Validated schedules plus a fail-closed diagnostic reason."""

    schedules: dict[str, FeeScheduleEvidence] = field(default_factory=dict)
    source_path: str = ""
    reason: str = "missing_fee_evidence"
    # The document hash identifies the exact private-account observation that
    # priced an entry.  ``integrity_verified`` is deliberately distinct from
    # structural validity: an unsigned snapshot may remain useful for shadow
    # diagnostics, but it is never proof for a live canary or official cohort.
    document_sha256: str = ""
    integrity_verified: bool = False
    integrity_key_id: str = ""
    schema_version: int = 0

    @property
    def loaded(self) -> bool:
        return bool(self.schedules) and not self.reason

    def schedule_for(self, venue: object) -> FeeScheduleEvidence | None:
        return self.schedules.get(str(venue or "").lower())

    def complete_for(self, *venues: object) -> bool:
        return all(self.schedule_for(venue) is not None for venue in venues)

    def identity_matches(
        self, expected_by_venue: Mapping[str, object], *venues: object
    ) -> bool:
        """Match non-sensitive account hashes against live config binding."""
        if self.schema_version != FEE_EVIDENCE_SCHEMA_VERSION:
            return False
        for venue in venues:
            key = str(venue or "").strip().lower()
            expected = str(expected_by_venue.get(key) or "").strip().lower()
            schedule = self.schedule_for(key)
            if not expected or schedule is None or schedule.account_identity_hash != expected:
                return False
        return True

    def observed_at_ms_for(self, *venues: object) -> int:
        schedules = [self.schedule_for(venue) for venue in venues]
        if not schedules or any(schedule is None for schedule in schedules):
            return 0
        return min(int(schedule.observed_at_ms) for schedule in schedules if schedule)

    def source_for(self, *venues: object) -> str:
        schedules = [self.schedule_for(venue) for venue in venues]
        if not schedules or any(schedule is None for schedule in schedules):
            return ""
        return "+".join(sorted({str(schedule.source) for schedule in schedules if schedule}))

    def provenance_for(self, *venues: object) -> list[dict[str, object]]:
        """Return canonical per-venue provenance for an immutable entry fact."""
        schedules = [self.schedule_for(venue) for venue in venues]
        if not schedules or any(schedule is None for schedule in schedules):
            return []
        return [
            {
                "venue": schedule.venue,
                "taker_fee_bps": schedule.taker_fee_bps,
                "maker_fee_bps": schedule.maker_fee_bps,
                "observed_at_ms": schedule.observed_at_ms,
                "source": schedule.source,
                "evidence_ref": schedule.evidence_ref,
                "account_identity_hash": schedule.account_identity_hash,
                "document_sha256": self.document_sha256,
                "integrity_key_id": self.integrity_key_id,
                "integrity_verified": self.integrity_verified,
            }
            for schedule in sorted(schedules, key=lambda value: value.venue) if schedule
        ]

    def fingerprint_for(self, *venues: object) -> str:
        """Hash the exact pair-scoped evidence that must survive journalling."""
        provenance = self.provenance_for(*venues)
        if not provenance:
            return ""
        return hashlib.sha256(_canonical_json(provenance)).hexdigest()


def load_fee_evidence(
    path: str | Path,
    *,
    now_ms: int,
    max_age_ms: int,
    integrity_key_env: str = "",
) -> FeeEvidenceBook:
    """Load short-lived account fee evidence without trusting partial records.

    A malformed or stale record returns an empty book.  Callers may still use
    their configured fee *floor* for shadow diagnostics, but a canary or an
    official paper cohort must require ``complete_for`` explicitly.
    """

    source_path = str(path or "").strip()
    if not source_path:
        return FeeEvidenceBook(source_path=source_path, reason="missing_fee_evidence_path")
    try:
        raw = json.loads(Path(source_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return FeeEvidenceBook(source_path=source_path, reason="fee_evidence_not_found")
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return FeeEvidenceBook(source_path=source_path, reason="fee_evidence_unreadable")
    if not isinstance(raw, dict):
        return FeeEvidenceBook(source_path=source_path, reason="fee_evidence_invalid_root")
    schema_version = raw.get("schema_version")
    # v1/v2 remain diagnostic-only.  v3 adds fixed-key integrity and the
    # account binding required by live canary admission.
    # ``True == 1`` in Python.  A boolean (or a coercible string) is not a
    # versioned evidence contract and must not select a legacy compatibility
    # path by accident.
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version not in {1, 2, FEE_EVIDENCE_SCHEMA_VERSION}
    ):
        return FeeEvidenceBook(source_path=source_path, reason="fee_evidence_schema_mismatch")
    venues = raw.get("venues")
    if not isinstance(venues, dict) or not venues:
        return FeeEvidenceBook(source_path=source_path, reason="fee_evidence_empty")

    document_sha256 = hashlib.sha256(_canonical_json(_unsigned_payload(raw))).hexdigest()
    integrity_verified, integrity_key_id, integrity_reason = _verify_integrity(
        raw,
        schema_version=int(schema_version),
        integrity_key_env=integrity_key_env,
    )
    if integrity_reason:
        return FeeEvidenceBook(source_path=source_path, reason=integrity_reason)

    schedules: dict[str, FeeScheduleEvidence] = {}
    for raw_venue, raw_schedule in venues.items():
        schedule, reason = _parse_schedule(
            raw_venue,
            raw_schedule,
            now_ms=now_ms,
            max_age_ms=max_age_ms,
            require_account_identity=(
                int(schema_version) == FEE_EVIDENCE_SCHEMA_VERSION
            ),
        )
        if schedule is None:
            return FeeEvidenceBook(source_path=source_path, reason=reason)
        # Venue identifiers are case-insensitive everywhere else in the
        # trading system.  Accepting both ``BINANCE`` and ``binance`` here
        # would make the latter silently overwrite the former and turn an
        # account-fee proof into an ambiguous permission.  This file gates
        # live canary and official-paper admission, so ambiguity must fail
        # closed rather than rely on JSON iteration order.
        if schedule.venue in schedules:
            return FeeEvidenceBook(
                source_path=source_path,
                reason=f"fee_evidence_duplicate_venue:{schedule.venue}",
            )
        schedules[schedule.venue] = schedule
    return FeeEvidenceBook(
        schedules=schedules,
        source_path=source_path,
        reason="",
        document_sha256=document_sha256,
        integrity_verified=integrity_verified,
        integrity_key_id=integrity_key_id,
        schema_version=int(schema_version),
    )


def effective_fee_maps(
    configured_taker_bps: Mapping[str, object],
    configured_maker_bps: Mapping[str, object],
    evidence: FeeEvidenceBook | None,
    *,
    allow_verified_maker_rebates: bool = False,
) -> tuple[dict[str, float], dict[str, float]]:
    """Return conservative maps, never lowering a configured fee floor.

    A verified account schedule replaces neither a deliberately more
    conservative configured assumption nor a missing value with an invented
    zero.  This preserves no-understatement semantics while allowing all
    strategy surfaces to consume the same evidence snapshot.
    """

    taker = _normalise_fee_map(configured_taker_bps)
    # Static TOML is a conservative floor, not account-authoritative rebate
    # evidence.  A negative configured maker fee must therefore fall back to
    # the known taker cost; only the verified schedule branch below may
    # introduce a signed rebate.
    maker = _normalise_fee_map(configured_maker_bps)
    for venue, taker_fee in taker.items():
        maker.setdefault(venue, taker_fee)
    if evidence is None:
        return taker, maker
    for venue, schedule in evidence.schedules.items():
        if venue in taker:
            taker[venue] = max(taker[venue], schedule.taker_fee_bps)
        else:
            taker[venue] = schedule.taker_fee_bps
        configured_maker = maker.get(venue, taker[venue])
        # A maker rebate is real cash flow, not a negative slippage guess.
        # It is only allowed to lower the static floor when the account export
        # has passed an integrity check and the caller explicitly opts in.
        if (
            allow_verified_maker_rebates
            and evidence.integrity_verified
            and schedule.maker_fee_bps < 0.0
        ):
            maker[venue] = schedule.maker_fee_bps
        else:
            maker[venue] = max(configured_maker, schedule.maker_fee_bps)
    return taker, maker


def _parse_schedule(
    raw_venue: object,
    raw_schedule: object,
    *,
    now_ms: int,
    max_age_ms: int,
    require_account_identity: bool,
) -> tuple[FeeScheduleEvidence | None, str]:
    venue = str(raw_venue or "").strip().lower()
    if not venue or not isinstance(raw_schedule, dict):
        return None, "fee_evidence_invalid_schedule"
    try:
        taker = _finite_nonnegative(raw_schedule.get("taker_fee_bps"))
        maker = _finite_number(raw_schedule.get("maker_fee_bps"))
        observed_at_ms = int(raw_schedule.get("observed_at_ms", 0))
    except (TypeError, ValueError, OverflowError):
        return None, f"fee_evidence_invalid_schedule:{venue}"
    source = str(raw_schedule.get("source") or "").strip().lower()
    evidence_ref = str(raw_schedule.get("evidence_ref") or "").strip()
    account_identity_hash = str(
        raw_schedule.get("account_identity_hash") or ""
    ).strip().lower()
    if taker is None or maker is None or observed_at_ms <= 0:
        return None, f"fee_evidence_invalid_schedule:{venue}"
    if source not in _ACCEPTED_SOURCES or not evidence_ref:
        return None, f"fee_evidence_untrusted_source:{venue}"
    if require_account_identity and not _is_sha256_hex(account_identity_hash):
        return None, f"fee_evidence_account_identity_missing_or_invalid:{venue}"
    if now_ms > 0 and observed_at_ms > now_ms:
        return None, f"fee_evidence_from_future:{venue}"
    if max_age_ms >= 0 and now_ms > 0 and now_ms - observed_at_ms > max_age_ms:
        return None, f"fee_evidence_stale:{venue}"
    return (
        FeeScheduleEvidence(
            venue=venue,
            taker_fee_bps=taker,
            maker_fee_bps=maker,
            observed_at_ms=observed_at_ms,
            source=source,
            evidence_ref=evidence_ref,
            account_identity_hash=account_identity_hash,
        ),
        "",
    )


def _finite_nonnegative(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if isfinite(parsed) and parsed >= 0.0 else None


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if isfinite(parsed) else None


def _normalise_fee_map(
    values: Mapping[str, object], *, allow_negative: bool = False
) -> dict[str, float]:
    result: dict[str, float] = {}
    for venue, value in values.items():
        parsed = _finite_number(value) if allow_negative else _finite_nonnegative(value)
        if parsed is not None and str(venue or "").strip():
            result[str(venue).lower()] = parsed
    return result


def sign_fee_evidence_payload(payload: Mapping[str, object], secret: str) -> dict:
    """Return a schema-v3 HMAC envelope for an account-export payload.

    This is intentionally a pure helper: exchange access and secret storage
    stay outside the strategy process.  Callers must keep ``secret`` out of
    the evidence file and configure its environment-variable *name* only.
    """
    signed = dict(payload)
    integrity = signed.get("integrity")
    if not isinstance(integrity, Mapping):
        raise ValueError("fee evidence integrity metadata is required")
    if int(signed.get("schema_version") or 0) == FEE_EVIDENCE_SCHEMA_VERSION:
        if str(integrity.get("key_id") or "").strip() != TRUSTED_FEE_EVIDENCE_KEY_ID:
            raise ValueError("fee evidence trusted key id is required")
    unsigned = _unsigned_payload(signed)
    signature = hmac.new(
        str(secret).encode("utf-8"), _canonical_json(unsigned), hashlib.sha256
    ).hexdigest()
    signed["integrity"] = {
        "algorithm": _INTEGRITY_ALGORITHM,
        "key_id": str(integrity.get("key_id") or "").strip(),
        "signature": signature,
    }
    return signed


def _verify_integrity(
    raw: Mapping[str, object],
    *,
    schema_version: int,
    integrity_key_env: str,
) -> tuple[bool, str, str]:
    if schema_version != FEE_EVIDENCE_SCHEMA_VERSION:
        return False, "", ""
    integrity = raw.get("integrity")
    if not isinstance(integrity, Mapping):
        return False, "", "fee_evidence_integrity_missing"
    algorithm = str(integrity.get("algorithm") or "").strip().lower()
    key_id = str(integrity.get("key_id") or "").strip()
    signature = str(integrity.get("signature") or "").strip().lower()
    if algorithm != _INTEGRITY_ALGORITHM or not key_id or len(signature) != 64:
        return False, "", "fee_evidence_integrity_invalid"
    try:
        int(signature, 16)
    except ValueError:
        return False, "", "fee_evidence_integrity_invalid"
    if schema_version == FEE_EVIDENCE_SCHEMA_VERSION:
        if key_id != TRUSTED_FEE_EVIDENCE_KEY_ID:
            return False, "", "fee_evidence_integrity_key_policy_mismatch"
        requested_key_name = str(integrity_key_env or "").strip()
        if requested_key_name and requested_key_name != TRUSTED_FEE_EVIDENCE_HMAC_ENV:
            return False, "", "fee_evidence_integrity_key_policy_mismatch"
        key_name = TRUSTED_FEE_EVIDENCE_HMAC_ENV
    else:
        # Legacy schemas may be read diagnostically, never as live authority.
        key_name = str(integrity_key_env or "").strip()
        if not key_name:
            return False, key_id, ""
    secret = os.environ.get(key_name)
    if not secret:
        return False, key_id, "fee_evidence_integrity_key_unavailable"
    expected = hmac.new(
        secret.encode("utf-8"), _canonical_json(_unsigned_payload(raw)), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return False, key_id, "fee_evidence_integrity_mismatch"
    return True, key_id, ""


def _is_sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _unsigned_payload(raw: Mapping[str, object]) -> dict:
    """Return the authenticated envelope with only the signature removed.

    ``key_id`` is persisted in each entry's provenance.  Treating it as
    unsigned would let a file editor rewrite the audit identity without
    invalidating the HMAC.  The algorithm and key id are metadata, but they
    are still facts relied on by the audit trail and therefore belong to the
    signed payload.
    """
    unsigned = {str(key): value for key, value in raw.items() if key != "integrity"}
    integrity = raw.get("integrity")
    if isinstance(integrity, Mapping):
        unsigned["integrity"] = {
            "algorithm": str(integrity.get("algorithm") or "").strip().lower(),
            "key_id": str(integrity.get("key_id") or "").strip(),
        }
    return unsigned


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")
