"""Strict, signed source evidence for promotion/acceptance reports.

Offline reports read append-only JSONL, but a JSONL file is still an untrusted
input at the point an operator turns a report into a release decision.  This
module binds the exact byte streams used by an acceptance run to a small HMAC
manifest.  It deliberately does not alter the runtime journal or V1 recovery
semantics.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import hmac
import json
import os
from pathlib import Path
from typing import Iterable, Mapping


ACCEPTANCE_EVIDENCE_SCHEMA_VERSION = 1
_INTEGRITY_ALGORITHM = "hmac-sha256"


@dataclass(frozen=True)
class _TrustedAcceptanceKey:
    """Release-owned identity of an acceptance-manifest signing key.

    The key id and environment variable are intentionally code-owned, rather
    than command-line inputs.  A caller who can choose both a variable name
    and a key id can manufacture a valid HMAC with an arbitrary local secret,
    which is authentication of nothing for a promotion decision.
    """

    integrity_key_env: str
    integrity_key_id: str


_TRUSTED_ACCEPTANCE_KEYS: dict[str, _TrustedAcceptanceKey] = {
    "funding_canary": _TrustedAcceptanceKey(
        integrity_key_env="LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY",
        integrity_key_id="lightfee-acceptance-evidence-v1",
    ),
    "spread_paper": _TrustedAcceptanceKey(
        integrity_key_env="LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY",
        integrity_key_id="lightfee-acceptance-evidence-v1",
    ),
}


class AcceptanceEvidenceError(ValueError):
    """Raised when a report input cannot be authenticated as acceptance data."""


@dataclass(frozen=True)
class EvidenceSource:
    """Digest and parse cardinality of one exact JSONL source."""

    sha256: str
    byte_count: int
    record_count: int


@dataclass(frozen=True)
class AcceptanceEvidence:
    """Verified source metadata retained beside an offline acceptance result."""

    report_kind: str
    manifest_sha256: str
    integrity_key_id: str
    sources: tuple[EvidenceSource, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "verified": True,
            "report_kind": self.report_kind,
            "manifest_sha256": self.manifest_sha256,
            "integrity_key_id": self.integrity_key_id,
            "sources": [asdict(source) for source in self.sources],
        }


def read_verified_jsonl(
    paths: Iterable[str | Path],
    *,
    report_kind: str,
    manifest_path: str | Path,
    integrity_key_env: str = "",
) -> tuple[list[dict[str, object]], AcceptanceEvidence]:
    """Read JSONL only when the supplied signed manifest matches byte-for-byte.

    Blank lines are harmless formatting and are ignored.  Every nonblank line
    must be a JSON object; malformed or scalar lines are errors rather than
    silently discarded samples.  Duplicate path and duplicate-content inputs
    are rejected so repeating a journal segment cannot inflate a cohort.
    """

    trusted_key = _trusted_key(report_kind)
    requested_key_env = str(integrity_key_env or "").strip()
    if requested_key_env and requested_key_env != trusted_key.integrity_key_env:
        raise AcceptanceEvidenceError(
            "acceptance evidence integrity key policy mismatch"
        )
    source_paths = _normalise_paths(paths)
    records, sources = _read_sources(source_paths)
    manifest_raw, manifest_bytes = _read_manifest(manifest_path)
    _verify_manifest(
        manifest_raw,
        manifest_bytes=manifest_bytes,
        report_kind=report_kind,
        sources=sources,
        trusted_key=trusted_key,
    )
    integrity = manifest_raw["integrity"]
    return records, AcceptanceEvidence(
        report_kind=report_kind,
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        integrity_key_id=str(integrity["key_id"]),
        sources=tuple(sources),
    )


def build_signed_manifest(
    paths: Iterable[str | Path],
    *,
    report_kind: str,
    secret: str,
    integrity_key_id: str = "",
) -> dict[str, object]:
    """Build a manifest for an explicit local signing workflow.

    The caller owns secret retrieval and writes the returned JSON.  This pure
    helper keeps key material out of report files and out of the trading
    process.
    """

    trusted_key = _trusted_key(report_kind)
    requested_key_id = str(integrity_key_id or "").strip()
    if requested_key_id and requested_key_id != trusted_key.integrity_key_id:
        raise AcceptanceEvidenceError(
            "acceptance evidence integrity key policy mismatch"
        )
    if not str(secret or ""):
        raise AcceptanceEvidenceError("acceptance evidence signing secret is required")
    _, sources = _read_sources(_normalise_paths(paths))
    unsigned: dict[str, object] = {
        "schema_version": ACCEPTANCE_EVIDENCE_SCHEMA_VERSION,
        "report_kind": str(report_kind),
        "sources": [asdict(source) for source in sources],
        "integrity": {
            "algorithm": _INTEGRITY_ALGORITHM,
            "key_id": trusted_key.integrity_key_id,
        },
    }
    signature = hmac.new(
        str(secret).encode("utf-8"), _canonical_json(unsigned), hashlib.sha256
    ).hexdigest()
    return {
        **unsigned,
        "integrity": {
            **unsigned["integrity"],
            "signature": signature,
        },
    }


def _normalise_paths(paths: Iterable[str | Path]) -> list[Path]:
    result: list[Path] = []
    seen_paths: set[Path] = set()
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if path in seen_paths:
            raise AcceptanceEvidenceError(f"acceptance evidence duplicate path: {path}")
        seen_paths.add(path)
        result.append(path)
    if not result:
        raise AcceptanceEvidenceError("acceptance evidence requires at least one source")
    return result


def _read_sources(paths: Iterable[Path]) -> tuple[list[dict[str, object]], list[EvidenceSource]]:
    records: list[dict[str, object]] = []
    sources: list[EvidenceSource] = []
    seen_digests: set[str] = set()
    for path in paths:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise AcceptanceEvidenceError(
                f"acceptance evidence source unreadable: {path}"
            ) from exc
        digest = hashlib.sha256(raw).hexdigest()
        if digest in seen_digests:
            raise AcceptanceEvidenceError(
                "acceptance evidence duplicate source content is not allowed"
            )
        seen_digests.add(digest)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AcceptanceEvidenceError(
                f"acceptance evidence source is not UTF-8: {path}"
            ) from exc
        source_records = 0
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise AcceptanceEvidenceError(
                    f"acceptance evidence malformed JSONL at {path}:{line_number}"
                ) from exc
            if not isinstance(value, dict):
                raise AcceptanceEvidenceError(
                    f"acceptance evidence JSONL row is not an object at {path}:{line_number}"
                )
            records.append(value)
            source_records += 1
        sources.append(
            EvidenceSource(
                sha256=digest,
                byte_count=len(raw),
                record_count=source_records,
            )
        )
    return records, sources


def _read_manifest(path: str | Path) -> tuple[dict[str, object], bytes]:
    manifest_path = Path(path).expanduser()
    try:
        raw = manifest_path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcceptanceEvidenceError("acceptance evidence manifest unreadable") from exc
    if not isinstance(value, dict):
        raise AcceptanceEvidenceError("acceptance evidence manifest root must be an object")
    return dict(value), raw


def _verify_manifest(
    manifest: Mapping[str, object],
    *,
    manifest_bytes: bytes,
    report_kind: str,
    sources: list[EvidenceSource],
    trusted_key: _TrustedAcceptanceKey,
) -> None:
    if manifest.get("schema_version") != ACCEPTANCE_EVIDENCE_SCHEMA_VERSION:
        raise AcceptanceEvidenceError("acceptance evidence manifest schema mismatch")
    if manifest.get("report_kind") != report_kind:
        raise AcceptanceEvidenceError("acceptance evidence manifest report kind mismatch")
    expected_sources = manifest.get("sources")
    if not isinstance(expected_sources, list):
        raise AcceptanceEvidenceError("acceptance evidence manifest sources missing")
    actual_sources = [asdict(source) for source in sources]
    if expected_sources != actual_sources:
        raise AcceptanceEvidenceError("acceptance evidence manifest source digest mismatch")
    integrity = manifest.get("integrity")
    if not isinstance(integrity, Mapping):
        raise AcceptanceEvidenceError("acceptance evidence manifest integrity missing")
    algorithm = str(integrity.get("algorithm") or "").strip().lower()
    key_id = str(integrity.get("key_id") or "").strip()
    signature = str(integrity.get("signature") or "").strip().lower()
    if (
        algorithm != _INTEGRITY_ALGORITHM
        or key_id != trusted_key.integrity_key_id
        or not _sha256_hex(signature)
    ):
        raise AcceptanceEvidenceError("acceptance evidence manifest integrity invalid")
    secret = os.environ.get(trusted_key.integrity_key_env)
    if not secret:
        raise AcceptanceEvidenceError("acceptance evidence integrity key unavailable")
    try:
        unsigned = _canonical_json(_unsigned_manifest(manifest))
    except (TypeError, ValueError) as exc:
        raise AcceptanceEvidenceError(
            "acceptance evidence manifest integrity invalid"
        ) from exc
    expected = hmac.new(secret.encode("utf-8"), unsigned, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise AcceptanceEvidenceError("acceptance evidence manifest integrity mismatch")
    # Reading the raw manifest is intentional: it is the exact document whose
    # digest is reported.  The statement quiets linters while documenting that
    # no canonical rewrite is used as the reported fingerprint.
    if not manifest_bytes:
        raise AcceptanceEvidenceError("acceptance evidence manifest is empty")


def trusted_acceptance_integrity(report_kind: str) -> tuple[str, str]:
    """Return the fixed environment variable and key id for a report kind.

    Kept public for the signing command and release automation.  It exposes
    only identifiers, never secret material.
    """

    trusted_key = _trusted_key(report_kind)
    return trusted_key.integrity_key_env, trusted_key.integrity_key_id


def _trusted_key(report_kind: str) -> _TrustedAcceptanceKey:
    key = _TRUSTED_ACCEPTANCE_KEYS.get(str(report_kind or "").strip())
    if key is None:
        raise AcceptanceEvidenceError("acceptance evidence report kind is unsupported")
    return key


def _unsigned_manifest(manifest: Mapping[str, object]) -> dict[str, object]:
    unsigned = {str(key): value for key, value in manifest.items() if key != "integrity"}
    integrity = manifest.get("integrity")
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


def _sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True
