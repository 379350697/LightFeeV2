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
import stat
from typing import Mapping


FEE_EVIDENCE_SCHEMA_VERSION = 3
LOCAL_FEE_EVIDENCE_SCHEMA_VERSION = 4
MAX_FEE_EVIDENCE_BYTES = 1024 * 1024
# The same ceiling is enforced at configuration validation and immediately
# before a live canary entry, so programmatic configuration cannot weaken it.
LIVE_CANARY_FEE_EVIDENCE_MAX_AGE_MS = 5 * 24 * 60 * 60 * 1000
_ACCEPTED_SOURCES = frozenset({"account_fee_api", "private_fill"})
_INTEGRITY_ALGORITHM = "hmac-sha256"
# Live admission never trusts a TOML-selected secret.  The release/CI
# environment owns this fixed key reference.
TRUSTED_FEE_EVIDENCE_HMAC_ENV = "LIGHTFEE_FEE_EVIDENCE_HMAC_KEY"
TRUSTED_FEE_EVIDENCE_KEY_ID = "lightfee-fee-evidence-v3"


@dataclass(frozen=True)
class FeeSymbolEvidence:
    """One contract-scoped fee observation inside a v4 venue envelope."""

    symbol: str
    taker_fee_bps: float
    maker_fee_bps: float
    observed_at_ms: int
    evidence_ref: str


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
    # Schema-v4 rows are venue-level worst-case schedules over this explicit
    # canonical funding-symbol set.  An entry outside the set is never allowed
    # to borrow a rate observed for a different contract.
    covered_symbols: tuple[str, ...] = ()
    symbol_schedules: tuple[FeeSymbolEvidence, ...] = ()


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
    # Schema v4 replaces the redundant same-host HMAC/account-hash ceremony
    # with the operating-system boundary actually protecting this single-user
    # deployment: a regular file owned by the service user and mode 0600.
    local_file_verified: bool = False
    # Stable across refresh timestamps/references; changes only when the
    # account fee contract itself changes.  Funding promotion cohorts use this
    # rather than the per-refresh document digest.
    schedule_sha256: str = ""

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

    def account_authoritative_for(
        self,
        expected_by_venue: Mapping[str, object],
        *venues: object,
        symbol: object = "",
    ) -> bool:
        """Return whether private account rates may gate funding entry.

        Signed v3 snapshots remain backward compatible.  Local v4 snapshots
        are authoritative only when the file ownership/permissions check
        passed; they deliberately do not pretend to have HMAC or identity
        properties that add no security on a single service host.
        """
        if not self.complete_for(*venues):
            return False
        if self.schema_version == LOCAL_FEE_EVIDENCE_SCHEMA_VERSION:
            canonical_symbol = str(symbol or "").strip().upper()
            return bool(
                self.local_file_verified
                and canonical_symbol
                and all(
                    canonical_symbol in schedule.covered_symbols
                    for venue in venues
                    if (schedule := self.schedule_for(venue)) is not None
                )
            )
        return bool(
            self.schema_version == FEE_EVIDENCE_SCHEMA_VERSION
            and self.integrity_verified
            and self.identity_matches(expected_by_venue, *venues)
        )

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

    def provenance_for(
        self, *venues: object, symbol: object = ""
    ) -> list[dict[str, object]]:
        """Return canonical per-venue provenance for an immutable entry fact."""
        schedules = [self.schedule_for(venue) for venue in venues]
        if not schedules or any(schedule is None for schedule in schedules):
            return []
        rows: list[dict[str, object]] = []
        for schedule in sorted(schedules, key=lambda value: value.venue):
            if schedule is None:
                continue
            row: dict[str, object] = {
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
                "local_file_verified": self.local_file_verified,
                "account_fee_evidence_authoritative": self.account_authoritative_for(
                    {}, schedule.venue, symbol=symbol
                ),
                "schedule_sha256": self.schedule_sha256,
            }
            if self.schema_version == LOCAL_FEE_EVIDENCE_SCHEMA_VERSION:
                row["covered_symbols"] = list(schedule.covered_symbols)
            rows.append(row)
        return rows

    def fingerprint_for(self, *venues: object, symbol: object = "") -> str:
        """Hash the exact pair-scoped evidence that must survive journalling."""
        provenance = self.provenance_for(*venues, symbol=symbol)
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
    path_obj = Path(source_path)
    try:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        fd = os.open(path_obj, flags)
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            metadata = os.fstat(handle.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                return FeeEvidenceBook(
                    source_path=source_path,
                    reason="fee_evidence_local_file_not_regular",
                )
            if metadata.st_size > MAX_FEE_EVIDENCE_BYTES:
                return FeeEvidenceBook(
                    source_path=source_path,
                    reason="fee_evidence_file_too_large",
                )
            raw = json.load(handle)
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
        or schema_version
        not in {1, 2, FEE_EVIDENCE_SCHEMA_VERSION, LOCAL_FEE_EVIDENCE_SCHEMA_VERSION}
    ):
        return FeeEvidenceBook(source_path=source_path, reason="fee_evidence_schema_mismatch")
    local_file_verified = False
    if int(schema_version) == LOCAL_FEE_EVIDENCE_SCHEMA_VERSION:
        local_file_verified, local_reason = _verify_local_file(metadata)
        if local_reason:
            return FeeEvidenceBook(source_path=source_path, reason=local_reason)
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
    first_rejected_schedule_reason = ""
    for raw_venue, raw_schedule in venues.items():
        schedule, reason = _parse_schedule(
            raw_venue,
            raw_schedule,
            now_ms=now_ms,
            max_age_ms=max_age_ms,
            require_account_identity=(
                int(schema_version) == FEE_EVIDENCE_SCHEMA_VERSION
            ),
            require_symbol_coverage=(
                int(schema_version) == LOCAL_FEE_EVIDENCE_SCHEMA_VERSION
            ),
        )
        if schedule is None:
            if int(schema_version) == LOCAL_FEE_EVIDENCE_SCHEMA_VERSION:
                first_rejected_schedule_reason = first_rejected_schedule_reason or reason
                continue
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
    if not schedules:
        return FeeEvidenceBook(
            source_path=source_path,
            reason=first_rejected_schedule_reason or "fee_evidence_empty",
        )
    return FeeEvidenceBook(
        schedules=schedules,
        source_path=source_path,
        reason="",
        document_sha256=document_sha256,
        integrity_verified=integrity_verified,
        integrity_key_id=integrity_key_id,
        schema_version=int(schema_version),
        local_file_verified=local_file_verified,
        schedule_sha256=_schedule_sha256(schedules),
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
    require_symbol_coverage: bool,
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
    covered_symbols_raw = raw_schedule.get("covered_symbols", [])
    if not isinstance(covered_symbols_raw, list):
        return None, f"fee_evidence_symbol_coverage_invalid:{venue}"
    covered_symbols = tuple(
        dict.fromkeys(
            str(symbol or "").strip().upper()
            for symbol in covered_symbols_raw
            if str(symbol or "").strip()
        )
    )
    symbol_schedules_raw = raw_schedule.get("symbol_schedules", {})
    if not isinstance(symbol_schedules_raw, dict):
        return None, f"fee_evidence_symbol_schedule_invalid:{venue}"
    all_symbol_schedules: list[FeeSymbolEvidence] = []
    for raw_symbol, raw_symbol_schedule in sorted(symbol_schedules_raw.items()):
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol or not isinstance(raw_symbol_schedule, dict):
            return None, f"fee_evidence_symbol_schedule_invalid:{venue}"
        try:
            symbol_taker = _finite_nonnegative(
                raw_symbol_schedule.get("taker_fee_bps")
            )
            symbol_maker = _finite_number(raw_symbol_schedule.get("maker_fee_bps"))
            symbol_observed_at_ms = int(
                raw_symbol_schedule.get("observed_at_ms", 0)
            )
        except (TypeError, ValueError, OverflowError):
            return None, f"fee_evidence_symbol_schedule_invalid:{venue}:{symbol}"
        symbol_evidence_ref = str(
            raw_symbol_schedule.get("evidence_ref") or ""
        ).strip()
        if (
            symbol_taker is None
            or symbol_maker is None
            or symbol_observed_at_ms <= 0
            or not symbol_evidence_ref
        ):
            return None, f"fee_evidence_symbol_schedule_invalid:{venue}:{symbol}"
        all_symbol_schedules.append(
            FeeSymbolEvidence(
                symbol=symbol,
                taker_fee_bps=symbol_taker,
                maker_fee_bps=symbol_maker,
                observed_at_ms=symbol_observed_at_ms,
                evidence_ref=symbol_evidence_ref,
            )
        )
    if taker is None or maker is None or observed_at_ms <= 0:
        return None, f"fee_evidence_invalid_schedule:{venue}"
    if source not in _ACCEPTED_SOURCES or not evidence_ref:
        return None, f"fee_evidence_untrusted_source:{venue}"
    if require_account_identity and not _is_sha256_hex(account_identity_hash):
        return None, f"fee_evidence_account_identity_missing_or_invalid:{venue}"
    if require_symbol_coverage and (
        not covered_symbols
        or len(covered_symbols) != len(covered_symbols_raw)
        or list(covered_symbols) != sorted(covered_symbols)
        or covered_symbols != tuple(row.symbol for row in all_symbol_schedules)
        or abs(taker - max(row.taker_fee_bps for row in all_symbol_schedules)) > 1e-12
        or abs(maker - max(row.maker_fee_bps for row in all_symbol_schedules)) > 1e-12
        or observed_at_ms != min(row.observed_at_ms for row in all_symbol_schedules)
    ):
        return None, f"fee_evidence_symbol_coverage_invalid:{venue}"
    if require_symbol_coverage:
        symbol_schedules = [
            row
            for row in all_symbol_schedules
            if not (now_ms > 0 and row.observed_at_ms > now_ms)
            and not (
                max_age_ms >= 0
                and now_ms > 0
                and now_ms - row.observed_at_ms > max_age_ms
            )
        ]
        if not symbol_schedules:
            return None, f"fee_evidence_no_fresh_symbol_schedule:{venue}"
        covered_symbols = tuple(row.symbol for row in symbol_schedules)
        taker = max(row.taker_fee_bps for row in symbol_schedules)
        maker = max(row.maker_fee_bps for row in symbol_schedules)
        observed_at_ms = min(row.observed_at_ms for row in symbol_schedules)
    else:
        symbol_schedules = all_symbol_schedules
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
            covered_symbols=covered_symbols,
            symbol_schedules=tuple(symbol_schedules),
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


def _verify_local_file(metadata: os.stat_result) -> tuple[bool, str]:
    """Verify the v4 snapshot is protected by the local service-user boundary."""
    if not stat.S_ISREG(metadata.st_mode):
        return False, "fee_evidence_local_file_not_regular"
    if metadata.st_uid != os.geteuid():
        return False, "fee_evidence_local_file_owner_mismatch"
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        return False, "fee_evidence_local_file_permissions_unsafe"
    return True, ""


def _schedule_sha256(schedules: Mapping[str, FeeScheduleEvidence]) -> str:
    """Hash fee terms without refresh-time fields that do not change economics."""
    material = [
        {
            "venue": schedule.venue,
            "taker_fee_bps": schedule.taker_fee_bps,
            "maker_fee_bps": schedule.maker_fee_bps,
            "source": schedule.source,
            "account_identity_hash": schedule.account_identity_hash,
            "covered_symbols": list(schedule.covered_symbols),
            "symbol_fees": [
                {
                    "symbol": row.symbol,
                    "taker_fee_bps": row.taker_fee_bps,
                    "maker_fee_bps": row.maker_fee_bps,
                }
                for row in schedule.symbol_schedules
            ],
        }
        for schedule in sorted(schedules.values(), key=lambda item: item.venue)
    ]
    return hashlib.sha256(_canonical_json(material)).hexdigest() if material else ""


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
