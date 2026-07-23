"""Atomic snapshot publisher for sidecar output."""

from __future__ import annotations

import json
from dataclasses import fields as dataclass_fields, replace
from hashlib import sha256
from math import isclose, isfinite
import os
import tempfile
import time
from pathlib import Path

from lightfee.sidecar.snapshot import (
    LEGACY_SNAPSHOT_SCHEMA_VERSIONS,
    SNAPSHOT_SCHEMA_VERSION,
    QuoteSnapshot,
    SidecarSnapshot,
    entry_targeted_oi_revalidation_required,
    validate_v4_snapshot_contract,
    validate_v5_snapshot_contract,
)
from lightfee.strategy.economics import build_edge_breakdown
from lightfee.strategy.fee_contract import derive_candidate_stage_fee_bps


_V3_ECONOMICS_NUMERIC_FIELDS = (
    "gross_signal_edge_bps",
    "funding_edge_bps",
    "entry_cross_bps",
    "expected_exit_cross_bps",
    "entry_fee_bps",
    "exit_fee_bps",
    "entry_slippage_bps",
    "exit_slippage_bps",
    "adverse_selection_bps",
    "capital_buffer_bps",
    "execution_buffer_bps",
    "venue_risk_haircut_bps",
    "transfer_or_inventory_bias_bps",
    "expected_net_edge_bps",
    "worst_case_edge_bps",
    "ranking_edge_bps",
    "forecast_worst_funding_edge_bps",
    "long_taker_fee_bps",
    "short_taker_fee_bps",
    "fee_bps",
)

_V3_LIFECYCLE_NUMERIC_FIELDS = (
    "first_stage_funding_edge_bps",
    "first_stage_expected_edge_bps",
    "first_stage_worst_case_edge_bps",
    "second_stage_incremental_funding_edge_bps",
    "second_stage_worst_case_funding_edge_bps",
    "stagger_gap_ms",
)


def _v3_economics_contract_reason(
    candidate: dict,
    *,
    quotes_raw: object,
    quote_evidence_index: dict[tuple[str, str], tuple[dict | None, str]] | None = None,
) -> str:
    """Return a fail-closed reason for a claimed-complete V3 candidate.

    JSON defaults are convenient for diagnostics but dangerous at a live
    admission boundary: a truncated V3 record used to inherit zero sizing and
    cost fields from ``CandidateInput``.  Verify both the raw presence and the
    shared formula and the candidate's two contract-normalisation proofs before
    constructing the dataclass, so no live caller can confuse a partially
    decoded or cross-contract candidate with a valid economics assertion.
    """
    required = (
        *_V3_ECONOMICS_NUMERIC_FIELDS,
        *_V3_LIFECYCLE_NUMERIC_FIELDS,
        "entry_notional_quote",
        "entry_target_quantity",
        "entry_max_executable_quantity",
        "economics_observed_at_ms",
        "calculation_version",
        "model_epoch",
        "taker_fee_evidence_complete",
        "forecast_distribution_stable",
        "forecast_stability_reason",
    )
    missing = next((name for name in required if name not in candidate), "")
    if missing:
        return f"missing_v3_economics_field:{missing}"
    if candidate.get("taker_fee_evidence_complete") is not True:
        return "missing_taker_fee_evidence"
    if not isinstance(candidate.get("forecast_distribution_stable"), bool):
        return "invalid_v3_economics_field:forecast_distribution_stable"
    stability_reason = candidate.get("forecast_stability_reason")
    if not isinstance(stability_reason, str) or not stability_reason.strip():
        return "invalid_v3_economics_field:forecast_stability_reason"

    numeric_fields = (
        *_V3_ECONOMICS_NUMERIC_FIELDS,
        *_V3_LIFECYCLE_NUMERIC_FIELDS,
        "entry_notional_quote",
        "entry_target_quantity",
        "entry_max_executable_quantity",
    )
    values: dict[str, float] = {}
    for name in numeric_fields:
        if isinstance(candidate[name], bool):
            return f"invalid_v3_economics_field:{name}"
        try:
            value = float(candidate[name])
        except (TypeError, ValueError, OverflowError):
            return f"invalid_v3_economics_field:{name}"
        if not isfinite(value):
            return f"invalid_v3_economics_field:{name}"
        values[name] = value
    # Transfer/inventory scoring was deliberately removed from the public
    # sidecar: it has neither a balance source nor position-allocation proof.
    # Keeping this field in the schema preserves historical diagnostics, but a
    # snapshot may not reintroduce an unproved alpha by self-reporting a bias.
    # A future private live-admission implementation needs a separate,
    # evidence-carrying contract rather than weakening this parser boundary.
    if not isclose(
        values["transfer_or_inventory_bias_bps"],
        0.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        return "unproved_transfer_or_inventory_bias"
    if any(
        values[name] <= 0.0
        for name in (
            "entry_notional_quote",
            "entry_target_quantity",
            "entry_max_executable_quantity",
        )
    ):
        return "invalid_v3_unified_sizing"
    # Fee fields remain signed: a passive maker leg can have a verified rebate
    # and pairing records it as a real cash flow.  Reserves and execution-risk
    # terms are not cash flows; a negative value there is untrusted alpha.
    for name in (
        "entry_slippage_bps",
        "exit_slippage_bps",
        "adverse_selection_bps",
        "capital_buffer_bps",
        "execution_buffer_bps",
        "venue_risk_haircut_bps",
    ):
        if values[name] < 0.0:
            return f"invalid_v3_economics_cost_sign:{name}"
    for stage in ("entry", "exit"):
        derived_fee_bps, fee_reason = derive_candidate_stage_fee_bps(
            candidate,
            stage,
        )
        if fee_reason or derived_fee_bps is None:
            return f"invalid_v3_fee_contract:{stage}:{fee_reason}"
        if not isclose(
            values[f"{stage}_fee_bps"],
            derived_fee_bps,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            return f"v3_fee_contract_mismatch:{stage}_fee_bps"
    if not isclose(
        values["fee_bps"],
        values["entry_fee_bps"] + values["exit_fee_bps"],
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        return "v3_fee_contract_mismatch:fee_bps"
    if isinstance(candidate["economics_observed_at_ms"], bool):
        return "invalid_v3_economics_observed_at_ms"
    try:
        observed_at_ms = int(candidate["economics_observed_at_ms"])
    except (TypeError, ValueError, OverflowError):
        return "invalid_v3_economics_observed_at_ms"
    if observed_at_ms <= 0:
        return "invalid_v3_economics_observed_at_ms"
    if not str(candidate["calculation_version"] or ""):
        return "missing_v3_calculation_version"
    if not str(candidate["model_epoch"] or ""):
        return "missing_v3_model_epoch"

    edge = build_edge_breakdown(
        gross_signal_edge_bps=values["gross_signal_edge_bps"],
        funding_edge_bps=values["funding_edge_bps"],
        worst_case_funding_edge_bps=values["forecast_worst_funding_edge_bps"],
        entry_cross_bps=values["entry_cross_bps"],
        expected_exit_cross_bps=values["expected_exit_cross_bps"],
        entry_fee_bps=values["entry_fee_bps"],
        exit_fee_bps=values["exit_fee_bps"],
        entry_slippage_bps=values["entry_slippage_bps"],
        exit_slippage_bps=values["exit_slippage_bps"],
        adverse_selection_bps=values["adverse_selection_bps"],
        capital_buffer_bps=values["capital_buffer_bps"],
        execution_buffer_bps=values["execution_buffer_bps"],
        venue_risk_haircut_bps=values["venue_risk_haircut_bps"],
        transfer_or_inventory_bias_bps=values["transfer_or_inventory_bias_bps"],
        calculation_version=str(candidate["calculation_version"]),
        model_epoch=str(candidate["model_epoch"]),
        observed_at_ms=observed_at_ms,
        economics_complete=True,
    )
    if not edge.economics_complete:
        return "invalid_v3_economics_cost_sign"
    for field, computed in (
        ("expected_net_edge_bps", edge.expected_net_edge_bps),
        ("worst_case_edge_bps", edge.worst_case_edge_bps),
        ("ranking_edge_bps", edge.ranking_edge_bps),
    ):
        if not isclose(values[field], computed, rel_tol=0.0, abs_tol=1e-9):
            return f"v3_edge_formula_mismatch:{field}"
    try:
        expected_edge = float(candidate.get("expected_edge_bps", 0.0) or 0.0)
    except (TypeError, ValueError, OverflowError):
        return "invalid_v3_economics_field:expected_edge_bps"
    if not isfinite(expected_edge):
        return "invalid_v3_economics_field:expected_edge_bps"
    if not isclose(
        expected_edge,
        edge.expected_net_edge_bps,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        return "v3_edge_formula_mismatch:expected_edge_bps"
    return _v3_candidate_contract_reason(
        candidate,
        quotes_raw,
        quote_evidence_index=quote_evidence_index,
    )


def _v3_candidate_contract_reason(
    candidate: dict,
    quotes_raw: object,
    *,
    quote_evidence_index: dict[tuple[str, str], tuple[dict | None, str]] | None = None,
) -> str:
    """Verify that a V3 candidate is backed by compatible quote contracts.

    Candidate economics are serialised separately from quote metadata.  A
    formula-correct but replayed/tampered candidate must not be allowed to
    claim a common base quantity if its two corresponding quote records cannot
    still prove the same linear economic unit.  The sidecar pair builder does
    the equivalent check when it creates a candidate; this parser check binds
    that proof to the live snapshot trust boundary.
    """
    if not isinstance(quotes_raw, dict):
        return "missing_v3_contract_evidence:quotes"
    symbol = str(candidate.get("symbol", "") or "").upper()
    long_venue = str(candidate.get("long_venue", "") or "").lower()
    short_venue = str(candidate.get("short_venue", "") or "").lower()
    if not symbol or not long_venue or not short_venue or long_venue == short_venue:
        return "invalid_v3_contract_evidence:candidate_identity"
    if quote_evidence_index is None:
        long_quote, long_quote_reason = _v3_quote_for_contract_evidence(
            quotes_raw,
            long_venue,
            symbol,
        )
        short_quote, short_quote_reason = _v3_quote_for_contract_evidence(
            quotes_raw,
            short_venue,
            symbol,
        )
    else:
        long_quote, long_quote_reason = quote_evidence_index.get(
            (long_venue, symbol),
            (None, "missing"),
        )
        short_quote, short_quote_reason = quote_evidence_index.get(
            (short_venue, symbol),
            (None, "missing"),
        )
    if long_quote is None:
        return f"{long_quote_reason}_v3_contract_evidence:long_quote"
    if short_quote is None:
        return f"{short_quote_reason}_v3_contract_evidence:short_quote"
    long_contract = _v3_normalised_contract_fields(long_quote)
    short_contract = _v3_normalised_contract_fields(short_quote)
    if long_contract is None:
        return "invalid_v3_contract_evidence:long_quote"
    if short_contract is None:
        return "invalid_v3_contract_evidence:short_quote"
    (
        long_underlying,
        long_quote_currency,
        long_multiplier,
    ) = long_contract
    (
        short_underlying,
        short_quote_currency,
        short_multiplier,
    ) = short_contract
    if long_underlying != short_underlying:
        return "v3_contract_evidence:underlying_mismatch"
    if long_quote_currency != short_quote_currency:
        return "v3_contract_evidence:quote_currency_mismatch"
    if not isclose(long_multiplier, short_multiplier, rel_tol=1e-12, abs_tol=0.0):
        return "v3_contract_evidence:multiplier_mismatch"
    return ""


def _v3_quote_for_contract_evidence(
    quotes_raw: dict,
    venue: str,
    symbol: str,
) -> tuple[dict | None, str]:
    """Find a quote by its own identity, not only by a mutable dict key."""
    match: dict | None = None
    for raw in quotes_raw.values():
        if not isinstance(raw, dict):
            continue
        raw_venue = str(raw.get("venue", "") or "").lower()
        raw_symbol = str(raw.get("symbol", "") or "").upper()
        if raw_venue == venue and raw_symbol == symbol:
            # Two differently keyed records that claim the same market make
            # the source ambiguous.  Choosing the first one would let a
            # tampered valid-looking quote vouch for another record's
            # candidate, so reject the entire proof rather than relying on
            # JSON insertion order.
            if match is not None:
                return None, "ambiguous"
            match = raw
    return (match, "") if match is not None else (None, "missing")


def _v3_quote_contract_evidence_index(
    quotes_raw: object,
) -> dict[tuple[str, str], tuple[dict | None, str]]:
    """Index quote identity once while preserving duplicate fail-closed proof."""
    if not isinstance(quotes_raw, dict):
        return {}
    index: dict[tuple[str, str], tuple[dict | None, str]] = {}
    for raw in quotes_raw.values():
        if not isinstance(raw, dict):
            continue
        identity = (
            str(raw.get("venue", "") or "").lower(),
            str(raw.get("symbol", "") or "").upper(),
        )
        if identity in index:
            index[identity] = (None, "ambiguous")
        else:
            index[identity] = (raw, "")
    return index


def _v3_normalised_contract_fields(raw: dict) -> tuple[str, str, float] | None:
    """Return strict common-base contract evidence from an untrusted quote."""
    if raw.get("contract_normalization_complete") is not True:
        return None
    underlying = str(raw.get("underlying", "") or "").strip().upper()
    quote_currency = str(raw.get("quote_currency", "") or "").strip().upper()
    if (
        not underlying
        or not quote_currency
        or str(raw.get("contract_type", "") or "").lower() != "linear"
        or not str(raw.get("mark_index_source", "") or "").strip()
        or str(raw.get("venue_status", "") or "").lower() != "active"
    ):
        return None
    raw_multiplier = raw.get("contract_multiplier")
    if isinstance(raw_multiplier, bool) or not isinstance(raw_multiplier, (int, float)):
        return None
    multiplier = float(raw_multiplier)
    price_precision = _v3_nonnegative_int(raw.get("price_precision"))
    quantity_precision = _v3_nonnegative_int(raw.get("quantity_precision"))
    exact_contract_numbers = tuple(
        _v3_positive_number(raw.get(field))
        for field in (
            "price_tick",
            "quantity_step_base",
            "min_quantity_base",
        )
    )
    min_notional_quote = _v3_nonnegative_number(raw.get("min_notional_quote"))
    funding_interval_ms = _v3_positive_int(raw.get("funding_interval_ms"))
    if (
        not isfinite(multiplier)
        or multiplier <= 0.0
        or price_precision is None
        or quantity_precision is None
        or any(value is None for value in exact_contract_numbers)
        or min_notional_quote is None
        or raw.get("min_notional_evidence_complete") is not True
        or funding_interval_ms is None
    ):
        return None
    return underlying, quote_currency, multiplier


def _v3_positive_int(value: object) -> int | None:
    """Accept only a finite, literal positive integer from raw JSON."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def _v3_positive_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    parsed = float(value)
    return parsed if isfinite(parsed) and parsed > 0.0 else None


def _v3_nonnegative_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    parsed = float(value)
    return parsed if isfinite(parsed) and parsed >= 0.0 else None


def _v3_nonnegative_int(value: object) -> int | None:
    """Accept a literal non-negative integer such as zero-place precision."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION = 7
FUNDING_ENTRY_SNAPSHOT_LEGACY_SCHEMA_VERSION = 6
FUNDING_ENTRY_SNAPSHOT_MAX_BYTES = 1_000_000


def _funding_entry_snapshot_base_path(path: str | Path) -> Path:
    target = Path(path)
    return target.with_name(f"{target.name}.funding-entry-v7.json")


def _legacy_funding_entry_snapshot_base_path(path: str | Path) -> Path:
    target = Path(path)
    return target.with_name(f"{target.name}.funding-entry-v6.json")


def funding_entry_snapshot_manifest_path(path: str | Path) -> Path:
    target = _funding_entry_snapshot_base_path(path)
    return target.with_name(f"{target.name}.manifest.json")


def _legacy_funding_entry_snapshot_manifest_path(path: str | Path) -> Path:
    target = _legacy_funding_entry_snapshot_base_path(path)
    return target.with_name(f"{target.name}.manifest.json")


def _read_json_file_with_stat(
    path: Path,
) -> tuple[dict[str, object], os.stat_result] | None:
    try:
        with open(path, "rb") as source:
            raw = source.read()
            stat = os.fstat(source.fileno())
        value = json.loads(raw)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    return (value, stat) if isinstance(value, dict) else None


def _read_funding_entry_manifest(
    path: str | Path,
) -> tuple[dict[str, object], os.stat_result] | None:
    """Read the authoritative entry manifest without unsafe downgrade.

    Once a V7 manifest path exists it is authoritative even when malformed.
    Falling back to a stale V6 generation in that state would turn corruption
    or a partial operator edit into live-entry permission.  V6 is consulted
    only before the first V7 manifest has ever been installed.
    """
    manifest_path = funding_entry_snapshot_manifest_path(path)
    if manifest_path.exists():
        return _read_json_file_with_stat(manifest_path)
    return _read_json_file_with_stat(_legacy_funding_entry_snapshot_manifest_path(path))


def _funding_entry_payload_path_from_manifest(
    path: str | Path,
    manifest: dict[str, object],
) -> Path | None:
    schema_version = manifest.get("schema_version")
    if schema_version == FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION:
        paths = _funding_entry_page_paths_from_manifest(path, manifest)
        return paths[0] if paths else None
    if schema_version != FUNDING_ENTRY_SNAPSHOT_LEGACY_SCHEMA_VERSION:
        return None
    base = _legacy_funding_entry_snapshot_base_path(path)
    generation_id = str(manifest.get("generation_id", "") or "")
    payload_name = str(manifest.get("payload_path", "") or "")
    if not generation_id or len(generation_id) != 64:
        return None
    if any(ch not in "0123456789abcdef" for ch in generation_id):
        return None
    expected_name = f"{base.name}.{generation_id}.json"
    if payload_name != expected_name or Path(payload_name).name != payload_name:
        return None
    return base.with_name(payload_name)


def _funding_entry_page_paths_from_manifest(
    path: str | Path,
    manifest: dict[str, object],
) -> list[Path] | None:
    if manifest.get("schema_version") != FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION:
        return None
    base = _funding_entry_snapshot_base_path(path)
    generation_id = str(manifest.get("generation_id", "") or "")
    if len(generation_id) != 64 or any(ch not in "0123456789abcdef" for ch in generation_id):
        return None
    raw_pages = manifest.get("pages")
    page_count = manifest.get("page_count")
    if (
        isinstance(page_count, bool)
        or not isinstance(page_count, int)
        or page_count <= 0
        or not isinstance(raw_pages, list)
        or len(raw_pages) != page_count
    ):
        return None
    paths: list[Path] = []
    for page_index, descriptor in enumerate(raw_pages):
        if not isinstance(descriptor, dict) or descriptor.get("page_index") != page_index:
            return None
        payload_name = str(descriptor.get("payload_path", "") or "")
        expected_name = f"{base.name}.{generation_id}.page-{page_index:05d}.json"
        if payload_name != expected_name or Path(payload_name).name != payload_name:
            return None
        paths.append(base.with_name(payload_name))
    return paths


def funding_entry_snapshot_path(path: str | Path) -> Path:
    """Resolve the immutable payload selected by the installed manifest.

    Before the first manifest exists, return the historical base name.  That
    fallback is intentionally non-authoritative and is only useful for
    install-state diagnostics and backwards-compatible path probes.
    """
    installed = _read_funding_entry_manifest(path)
    if installed is not None:
        payload_path = _funding_entry_payload_path_from_manifest(path, installed[0])
        if payload_path is not None:
            return payload_path
    # During a reader-first rollout the old V6 manifest remains readable until
    # V7 is installed.  An invalid existing V7 manifest must not expose V6.
    if not funding_entry_snapshot_manifest_path(path).exists():
        legacy = _legacy_funding_entry_snapshot_base_path(path)
        if _legacy_funding_entry_snapshot_manifest_path(path).exists():
            return legacy
    return _funding_entry_snapshot_base_path(path)


def _json_text(data: object, *, indent: int | None) -> str:
    return json.dumps(
        data,
        ensure_ascii=False,
        indent=indent,
        separators=(",", ":") if indent is None else None,
    )


def _atomic_write_json(
    data: object,
    target: Path,
    *,
    indent: int | None,
    max_bytes: int | None = None,
) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".snapshot-")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        content = _json_text(data, indent=indent)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        size = len(content.encode("utf-8"))
        if max_bytes is not None and size > max(int(max_bytes), 0):
            raise ValueError(f"snapshot payload exceeds hard limit: {size}>{int(max_bytes)}")
        tmp.replace(target)
        # Make the rename durable as well as atomic. The payload/manifest
        # protocol can recover from a crash between files, but it must not
        # claim durability while the directory entry is still only cached.
        directory_fd = os.open(str(target.parent), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return size
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def publish_snapshot(snapshot: SidecarSnapshot, path: str | Path) -> None:
    """Validate and atomically replace the last readable snapshot."""
    data = _snapshot_to_dict(snapshot)
    if data.get("schema_version") == SNAPSHOT_SCHEMA_VERSION:
        contract_errors = validate_v5_snapshot_contract(data)
        if contract_errors:
            raise ValueError(
                "refusing to publish invalid V5 snapshot: " + "; ".join(contract_errors)
            )
    _atomic_write_json(data, Path(path), indent=2)


def _funding_entry_candidates(snapshot: SidecarSnapshot) -> list:
    """Return every executable row without a cardinality limit."""
    return [c for c in snapshot.candidates if not c.blocked and c.economics_complete]


def _is_sha256_hex(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def _funding_entry_policy_fingerprint(
    snapshot: SidecarSnapshot,
    candidates: list,
) -> tuple[str, str]:
    """Bind every page to one deterministic entry-policy contract.

    New producers should provide ``entry_policy_fingerprint`` from the frozen
    strategy configuration.  During reader-first rollout an older producer
    can still create a cryptographically bound V7 generation: its derived
    fingerprint includes the inner schema plus every candidate economics
    model and fee-assurance tier.  A malformed explicit fingerprint is never
    silently replaced with a derived value.
    """
    diagnostics = dict(snapshot.candidate_build_diagnostics or {})
    explicit = diagnostics.get("entry_policy_fingerprint")
    if explicit not in (None, ""):
        if not _is_sha256_hex(explicit):
            raise ValueError("invalid funding entry policy fingerprint")
        return str(explicit), "explicit"
    contract = {
        "contract": "funding_entry_frontier_v7",
        "inner_snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
        "candidate_models": sorted(
            {
                (
                    str(getattr(candidate, "calculation_version", "") or ""),
                    str(getattr(candidate, "model_epoch", "") or ""),
                    str(
                        getattr(
                            candidate,
                            "funding_canary_fee_assurance_tier",
                            "unavailable",
                        )
                        or "unavailable"
                    ),
                    bool(getattr(candidate, "account_fee_evidence_complete", False)),
                    bool(getattr(candidate, "taker_fee_evidence_complete", False)),
                )
                for candidate in candidates
            }
        ),
    }
    encoded = _json_text(contract, indent=None).encode("utf-8")
    return sha256(encoded).hexdigest(), "derived"


def _funding_entry_candidate_identity(candidate: dict[str, object]) -> str:
    pair_id = str(candidate.get("pair_id", "") or "").strip().lower()
    if pair_id:
        return pair_id
    symbol = str(candidate.get("symbol", "") or "").strip().lower()
    long_venue = str(candidate.get("long_venue", "") or "").strip().lower()
    short_venue = str(candidate.get("short_venue", "") or "").strip().lower()
    if not symbol or not long_venue or not short_venue:
        return ""
    return f"{symbol}:{long_venue}->{short_venue}"


def _funding_entry_page_data(
    *,
    generation_id: str,
    policy_fingerprint: str,
    decision_at_ms: int,
    page_index: int,
    page_count: int,
    common: dict[str, object] | None,
    quotes: dict[str, object],
    candidates: list[dict[str, object]],
) -> dict[str, object]:
    page: dict[str, object] = {
        "schema_version": FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION,
        "generation_id": generation_id,
        "policy_fingerprint": policy_fingerprint,
        "decision_at_ms": decision_at_ms,
        "page_index": page_index,
        "page_count": page_count,
        "quotes": quotes,
        "candidates": candidates,
    }
    if common is not None:
        page["snapshot_common"] = common
    return page


def _split_funding_entry_pages(
    data: dict[str, object],
    *,
    generation_id: str,
    policy_fingerprint: str,
    decision_at_ms: int,
    max_bytes: int,
) -> list[dict[str, object]]:
    """Greedily shard one complete V5 payload by its encoded byte size."""
    raw_quotes = data.get("quotes")
    raw_candidates = data.get("candidates")
    if not isinstance(raw_quotes, dict) or not isinstance(raw_candidates, list):
        raise ValueError("funding entry payload collections are invalid")
    common = {key: value for key, value in data.items() if key not in {"quotes", "candidates"}}
    items: list[tuple[str, str | None, object, int]] = []
    for key, value in sorted(raw_quotes.items()):
        text_key = str(key)
        encoded_size = len(_json_text(text_key, indent=None).encode("utf-8"))
        encoded_size += 1 + len(_json_text(value, indent=None).encode("utf-8"))
        items.append(("quote", text_key, value, encoded_size))
    for value in raw_candidates:
        if not isinstance(value, dict):
            raise ValueError("funding entry candidate page item is invalid")
        encoded_size = len(_json_text(value, indent=None).encode("utf-8"))
        items.append(("candidate", None, value, encoded_size))
    # The placeholder is an upper bound on the decimal width of page_count;
    # replacing it with the final count cannot make a page larger.
    page_count_placeholder = max(len(items) + 1, 1)
    pages: list[dict[str, object]] = []
    page_quotes: dict[str, object] = {}
    page_candidates: list[dict[str, object]] = []

    def _candidate_page(
        index: int,
        quotes: dict[str, object],
        candidates: list[dict[str, object]],
    ) -> dict[str, object]:
        return _funding_entry_page_data(
            generation_id=generation_id,
            policy_fingerprint=policy_fingerprint,
            decision_at_ms=decision_at_ms,
            page_index=index,
            page_count=page_count_placeholder,
            common=common if index == 0 else None,
            quotes=quotes,
            candidates=candidates,
        )

    def _empty_page_size(index: int) -> int:
        return len(_json_text(_candidate_page(index, {}, []), indent=None).encode("utf-8"))

    page_size = _empty_page_size(0)
    if page_size > max_bytes:
        raise ValueError("funding entry V7 snapshot metadata exceeds hard limit per page")

    def _item_delta(item_type: str, encoded_size: int) -> int:
        has_same_collection_item = page_quotes if item_type == "quote" else page_candidates
        return encoded_size + (1 if has_same_collection_item else 0)

    def _start_next_page() -> None:
        nonlocal page_quotes, page_candidates, page_size
        pages.append(_candidate_page(len(pages), page_quotes, page_candidates))
        page_quotes = {}
        page_candidates = []
        page_size = _empty_page_size(len(pages))
        if page_size > max_bytes:
            raise ValueError("funding entry V7 page metadata exceeds hard limit per page")

    for item_type, item_key, item_value, encoded_size in items:
        delta = _item_delta(item_type, encoded_size)
        if page_size + delta > max_bytes:
            # Page zero owns the common snapshot envelope.  If that envelope
            # leaves insufficient room for the first collection item, retain a
            # metadata-only first page and continue on a normal data page.
            _start_next_page()
            delta = _item_delta(item_type, encoded_size)
            if page_size + delta > max_bytes:
                raise ValueError("funding entry V7 page item exceeds hard limit per page")
        if item_type == "quote":
            assert item_key is not None
            page_quotes[item_key] = item_value
        else:
            assert isinstance(item_value, dict)
            page_candidates.append(item_value)
        page_size += delta
    pages.append(_candidate_page(len(pages), page_quotes, page_candidates))

    page_count = len(pages)
    final_pages: list[dict[str, object]] = []
    for page_index, page in enumerate(pages):
        final_page = dict(page)
        final_page["page_index"] = page_index
        final_page["page_count"] = page_count
        if len(_json_text(final_page, indent=None).encode("utf-8")) > max_bytes:
            raise ValueError("funding entry V7 page exceeds hard limit per page")
        final_pages.append(final_page)
    return final_pages


def publish_funding_entry_snapshot(
    snapshot: SidecarSnapshot,
    path: str | Path,
) -> dict[str, object]:
    """Publish the complete live-entry frontier before the full audit snapshot.

    The payload deliberately remains a strict V5 contract so all existing
    parsing and evidence validation stays authoritative.  V7 splits that
    payload into byte-bounded immutable pages.  The manifest is the sole
    generation/install contract and is written only after every page is
    durable. Candidate cardinality is never a publication boundary.
    """
    original_diagnostics = dict(snapshot.candidate_build_diagnostics)
    candidates = _funding_entry_candidates(snapshot)

    def _frontier_count(name: str) -> int:
        value = original_diagnostics.get(name, -1)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return -1
        return value

    seed_pair_count = _frontier_count("seed_pair_count")
    pair_decision_count = _frontier_count("pair_decision_count")
    eligible_candidate_count = _frontier_count("eligible_candidate_count")
    omitted_eligible_count = _frontier_count("omitted_eligible_count")
    reported_stop_reason = str(original_diagnostics.get("frontier_stop_reason", "") or "").strip()
    eligible_frontier_complete = bool(
        original_diagnostics.get("eligible_frontier_complete") is True
        and seed_pair_count >= 0
        and pair_decision_count == seed_pair_count
        and eligible_candidate_count == len(candidates)
        and omitted_eligible_count == 0
        and reported_stop_reason in {"", "all_pairs_decided"}
    )
    if eligible_frontier_complete:
        frontier_stop_reason = reported_stop_reason or "all_pairs_decided"
    elif reported_stop_reason and reported_stop_reason != "all_pairs_decided":
        frontier_stop_reason = reported_stop_reason
    else:
        frontier_stop_reason = "pair_decision_incomplete"
    policy_fingerprint, policy_fingerprint_source = _funding_entry_policy_fingerprint(
        snapshot,
        candidates,
    )
    # An incomplete generation may still be published for diagnostics, but it
    # must carry no executable rows.  This prevents a consumer that predates
    # the V7 readiness field from accidentally admitting a partial frontier.
    if not eligible_frontier_complete:
        candidates = []
    targets = {(c.long_venue.lower(), c.symbol.lower()) for c in candidates} | {
        (c.short_venue.lower(), c.symbol.lower()) for c in candidates
    }
    # With no executable candidate, the live data plane is deliberately
    # empty/unavailable.  Rejection counts and source cardinalities remain in
    # the entry diagnostics, while quote/lifecycle evidence stays in the
    # background audit artifact and cannot masquerade as an entry payload.
    quotes = {
        key: quote
        for key, quote in snapshot.quotes.items()
        if (quote.venue.lower(), quote.symbol.lower()) in targets
    }

    def _requires_targeted_oi_revalidation(quote: QuoteSnapshot) -> bool:
        """Identify the explicit handoff from sidecar ranking to live admission.

        Binance-compatible venues intentionally omit broad-universe OI from
        the fast entry snapshot.  The runtime re-fetches it for the selected
        candidate before admitting an order.  This marker is therefore not a
        failed liquidity proof; every other absent or malformed proof remains
        a real compact-snapshot degradation.
        """
        return entry_targeted_oi_revalidation_required(
            evidence_status=quote.open_interest_evidence_status,
            evidence_reason=quote.open_interest_evidence_reason,
            volume_24h_quote=quote.volume_24h_quote,
        )

    def _has_strict_liquidity_evidence(quote: QuoteSnapshot) -> bool:
        return bool(
            isfinite(float(quote.volume_24h_quote or 0.0))
            and float(quote.volume_24h_quote or 0.0) > 0.0
            and quote.open_interest_evidence_status == "observed"
            and quote.open_interest is not None
            and isfinite(float(quote.open_interest))
            and float(quote.open_interest) >= 0.0
            and int(quote.open_interest_observed_at_ms or 0) > 0
            and int(quote.open_interest_received_at_ms or 0)
            >= int(quote.open_interest_observed_at_ms or 0)
            and int(quote.open_interest_event_at_ms or 0)
            <= int(quote.open_interest_received_at_ms or 0) + 5_000
            and bool(str(quote.open_interest_sample_id or "").strip())
            and bool(str(quote.open_interest_venue_symbol or "").strip())
        )

    targeted_oi_revalidation_targets = sorted(
        {
            (quote.venue.lower(), quote.symbol.upper())
            for quote in quotes.values()
            if _requires_targeted_oi_revalidation(quote)
        }
    )
    # The full audit watermark can belong to a quote that is intentionally
    # omitted from this eligible-only payload. Bind the entry snapshot's market
    # watermark to the evidence it actually retains; malformed/future quote
    # clocks still reach the strict V5 validator and fail closed below.
    entry_market_observed_at_ms = (
        max(quote.observed_at_ms for quote in quotes.values())
        if quotes
        else snapshot.market_observed_at_ms
    )
    degraded_symbols = {
        venue: [symbol for symbol in symbols if (venue.lower(), symbol.lower()) in targets]
        for venue, symbols in snapshot.degraded_symbols.items()
    }
    degraded_symbols = {k: v for k, v in degraded_symbols.items() if v}
    quotes_by_venue: dict[str, list[QuoteSnapshot]] = {}
    for quote in quotes.values():
        quotes_by_venue.setdefault(quote.venue, []).append(quote)

    def _lifecycle(rows: list, domain: str) -> list:
        compact = []
        for row in rows:
            venue_quotes = quotes_by_venue.get(row.venue, [])
            if not venue_quotes and targets:
                continue
            has_non_deferred_liquidity_failure = False
            if domain == "funding":
                usable = sum(
                    q.funding_timestamp_ms > 0 and q.funding_interval_ms > 0 for q in venue_quotes
                )
            elif domain == "market":
                usable = sum(q.bid > 0 and q.ask > 0 and q.bid <= q.ask for q in venue_quotes)
            else:
                usable = sum(_has_strict_liquidity_evidence(q) for q in venue_quotes)
                has_non_deferred_liquidity_failure = any(
                    not _has_strict_liquidity_evidence(q)
                    and not _requires_targeted_oi_revalidation(q)
                    for q in venue_quotes
                )
            selected_symbol_degraded = any(
                (row.venue.lower(), str(symbol).lower()) in targets
                for symbol in snapshot.degraded_symbols.get(row.venue, [])
            )
            evidence_unavailable = bool(
                usable < len(venue_quotes)
                and (domain != "liquidity" or has_non_deferred_liquidity_failure)
            )
            selected_reason = (
                row.degraded_reason
                if (not targets or selected_symbol_degraded or evidence_unavailable)
                else ""
            )
            if venue_quotes and evidence_unavailable and not str(selected_reason or "").strip():
                selected_reason = f"entry_snapshot_{domain}_evidence_unavailable"
            compact.append(
                replace(
                    row,
                    symbol_count=len(venue_quotes),
                    coverage_usable=usable,
                    degraded_reason=(
                        selected_reason
                        if venue_quotes or selected_reason
                        else "entry_snapshot_no_candidate"
                    ),
                )
            )
        return compact

    requested_symbols = sorted({symbol.upper() for _, symbol in targets}) or list(
        original_diagnostics.get("requested_symbols", [])
    )
    requested_venues = sorted({venue for venue, _ in targets}) or list(
        original_diagnostics.get("requested_venues", [])
    )
    source_data_ready = bool(snapshot.quotes)
    requested_venue_set = {
        str(venue).strip().lower()
        for venue in requested_venues
        if str(venue).strip()
    }
    source_quote_venues = {
        str(quote.venue or "").strip().lower()
        for quote in snapshot.quotes.values()
        if str(quote.venue or "").strip()
    }
    source_lifecycle_rows = (
        snapshot.funding_lifecycle
        + snapshot.market_lifecycle
        + snapshot.liquidity_lifecycle
    )
    complete_empty_frontier_ready = bool(
        not candidates
        and eligible_frontier_complete
        and source_data_ready
        and snapshot.acquisition_mode != "unavailable"
        and not snapshot.degraded_venues
        and not snapshot.degraded_domains
        and not any(snapshot.degraded_symbols.values())
        and requested_venue_set
        and requested_venue_set <= source_quote_venues
        and all(not str(row.degraded_reason or "").strip() for row in source_lifecycle_rows)
        and all(
            requested_venue_set
            <= {str(row.venue or "").strip().lower() for row in rows}
            for rows in (snapshot.funding_lifecycle, snapshot.market_lifecycle)
        )
    )
    source_rejection_counts = {
        str(reason): int(count)
        for reason, count in sorted(
            dict(
                original_diagnostics.get(
                    "blocked_reason_counts",
                    original_diagnostics.get("rejection_counts", {}),
                )
                or {}
            ).items()
        )
        if int(count or 0) > 0
    }
    # The V5 payload contract models rejection counts as one terminal decision
    # per omitted row.  Full-pair discovery can retain several blocked reasons
    # for one pair, so its reason histogram is not cardinality-conserving.  Keep
    # that source histogram separately and use one transport-level terminal
    # bucket for the V7 eligible-only projection.
    not_published_pair_count = max(seed_pair_count - len(candidates), 0)
    rejection_counts = (
        {"not_eligible_for_entry_frontier": not_published_pair_count}
        if not_published_pair_count > 0
        else {}
    )
    entry_diagnostics = {
        "input_quote_count": len(quotes),
        "requested_symbol_count": len(requested_symbols),
        "requested_symbols": requested_symbols,
        "requested_venues": requested_venues,
        "directional_pair_count": max(seed_pair_count, 0),
        "output_candidate_count": len(candidates),
        "future_input_quote_count": sum(
            quote.observed_at_ms > snapshot.candidate_build_observed_at_ms
            for quote in quotes.values()
        ),
        "rejection_counts": rejection_counts,
        "source_rejection_counts": source_rejection_counts,
        "diagnostics_only": not bool(candidates),
        "source_candidate_count": len(snapshot.candidates),
        "source_quote_count": len(snapshot.quotes),
        "source_data_ready": source_data_ready,
        "pair_decision_count": max(pair_decision_count, 0),
        "eligible_candidate_count": max(eligible_candidate_count, 0),
        "omitted_eligible_count": max(omitted_eligible_count, 0),
        "eligible_frontier_complete": eligible_frontier_complete,
        "entry_frontier_ready": source_data_ready and eligible_frontier_complete,
        "complete_empty_frontier_ready": complete_empty_frontier_ready,
        "entry_policy_fingerprint": policy_fingerprint,
        "entry_policy_fingerprint_source": policy_fingerprint_source,
        "frontier_stop_reason": frontier_stop_reason,
        "seed_pair_count": max(seed_pair_count, 0),
        # This is intentionally separate from degradation. The runtime owns
        # this candidate-scoped network proof and blocks the candidate when it
        # cannot obtain it before the entry deadline.
        "entry_targeted_oi_revalidation_required_count": len(targeted_oi_revalidation_targets),
        "entry_targeted_oi_revalidation_required_venues": sorted(
            {venue for venue, _symbol in targeted_oi_revalidation_targets}
        ),
    }
    funding_lifecycle = _lifecycle(snapshot.funding_lifecycle, "funding")
    market_lifecycle = _lifecycle(snapshot.market_lifecycle, "market")
    liquidity_lifecycle = _lifecycle(snapshot.liquidity_lifecycle, "liquidity")
    reasoned_domains = {
        domain
        for domain, rows in (
            ("funding", funding_lifecycle),
            ("market", market_lifecycle),
            ("liquidity", liquidity_lifecycle),
        )
        if any(str(row.degraded_reason or "").strip() for row in rows)
    }
    reasoned_venues = {
        row.venue
        for rows in (funding_lifecycle, market_lifecycle, liquidity_lifecycle)
        for row in rows
        if str(row.degraded_reason or "").strip()
    }
    selected_degraded_venues = sorted(
        {
            venue
            for venue in requested_venues
            if venue in reasoned_venues or bool(degraded_symbols.get(venue))
        }
    )
    selected_degraded_domains = sorted(reasoned_domains)
    has_selected_degradation = bool(
        selected_degraded_venues or selected_degraded_domains or degraded_symbols
    )
    acquisition_mode = (
        "unavailable"
        if not candidates
        else (
            snapshot.acquisition_mode
            if has_selected_degradation
            and snapshot.acquisition_mode in {"degraded_sidecar", "last_good_sidecar"}
            else "degraded_sidecar"
            if has_selected_degradation
            else "fresh_sidecar"
        )
    )
    entry_snapshot = replace(
        snapshot,
        market_observed_at_ms=entry_market_observed_at_ms,
        funding_lifecycle=funding_lifecycle,
        market_lifecycle=market_lifecycle,
        liquidity_lifecycle=liquidity_lifecycle,
        transfer_lifecycle=[
            row
            for row in snapshot.transfer_lifecycle
            if row.from_venue in quotes_by_venue and row.to_venue in quotes_by_venue
        ],
        degraded_venues=selected_degraded_venues,
        degraded_domains=selected_degraded_domains,
        degraded_symbols=degraded_symbols,
        acquisition_mode=acquisition_mode,
        candidate_build_diagnostics=entry_diagnostics,
        quotes=quotes,
        candidates=candidates,
    )
    data = _snapshot_to_dict(entry_snapshot)
    contract_errors = validate_v5_snapshot_contract(data)
    if contract_errors:
        raise ValueError(
            "refusing to publish invalid funding entry snapshot: " + "; ".join(contract_errors)
        )
    previous_generation = _verified_funding_entry_generation(
        path,
        verify_digest=False,
    )
    previous_payload_paths = (
        _verified_generation_payload_paths(path, previous_generation[0])
        if previous_generation is not None
        else []
    )
    raw_candidates = data.get("candidates", [])
    if not isinstance(raw_candidates, list):
        raise ValueError("funding entry candidates are invalid")
    candidate_identities = [
        _funding_entry_candidate_identity(candidate) if isinstance(candidate, dict) else ""
        for candidate in raw_candidates
    ]
    if any(not identity for identity in candidate_identities):
        raise ValueError("funding entry candidate identity is missing")
    if len(set(candidate_identities)) != len(candidate_identities):
        raise ValueError("funding entry candidate identity is duplicated")
    frontier_digest = sha256(_json_text(raw_candidates, indent=None).encode("utf-8")).hexdigest()
    generation_id = sha256(
        (
            f"{policy_fingerprint}:{snapshot.candidate_build_observed_at_ms}:"
            f"{frontier_digest}:{time.time_ns()}:{os.getpid()}"
        ).encode()
    ).hexdigest()
    pages = _split_funding_entry_pages(
        data,
        generation_id=generation_id,
        policy_fingerprint=policy_fingerprint,
        decision_at_ms=int(snapshot.candidate_build_observed_at_ms),
        max_bytes=FUNDING_ENTRY_SNAPSHOT_MAX_BYTES,
    )
    payload_base = _funding_entry_snapshot_base_path(path)
    installed_page_paths: list[Path] = []
    page_descriptors: list[dict[str, object]] = []
    try:
        for page_index, page in enumerate(pages):
            payload_path = payload_base.with_name(
                f"{payload_base.name}.{generation_id}.page-{page_index:05d}.json"
            )
            payload_content = _json_text(page, indent=None).encode("utf-8")
            payload_digest = sha256(payload_content).hexdigest()
            payload_size = _atomic_write_json(
                page,
                payload_path,
                indent=None,
                max_bytes=FUNDING_ENTRY_SNAPSHOT_MAX_BYTES,
            )
            stat = payload_path.stat()
            if sha256(payload_path.read_bytes()).hexdigest() != payload_digest:
                raise OSError("funding entry page digest changed during installation")
            raw_page_candidates = page.get("candidates", [])
            raw_page_quotes = page.get("quotes", {})
            page_descriptors.append(
                {
                    "page_index": page_index,
                    "payload_path": payload_path.name,
                    "payload_size_bytes": payload_size,
                    "payload_mtime_ns": stat.st_mtime_ns,
                    "payload_sha256": payload_digest,
                    "candidate_count": (
                        len(raw_page_candidates) if isinstance(raw_page_candidates, list) else -1
                    ),
                    "quote_count": (
                        len(raw_page_quotes) if isinstance(raw_page_quotes, dict) else -1
                    ),
                }
            )
            installed_page_paths.append(payload_path)
    except Exception:
        for installed_page_path in installed_page_paths:
            try:
                installed_page_path.unlink()
            except OSError:
                pass
        raise
    page_set_digest = sha256(
        _json_text(
            [
                {
                    "page_index": descriptor["page_index"],
                    "payload_sha256": descriptor["payload_sha256"],
                }
                for descriptor in page_descriptors
            ],
            indent=None,
        ).encode("utf-8")
    ).hexdigest()
    manifest_prepared_at_ms = time.time_ns() // 1_000_000
    first_page = page_descriptors[0]
    manifest = {
        "schema_version": FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION,
        "generation_id": generation_id,
        # A file cannot encode the exact completion time of its own atomic
        # rename.  Consumers therefore use the installed manifest inode clock
        # below; this field is diagnostic preparation time only.
        "manifest_prepared_at_ms": manifest_prepared_at_ms,
        "ready_clock_source": "manifest_install_ctime",
        # Compatibility aliases point at page zero; V7 readers validate the
        # complete ordered ``pages`` array and aggregate byte count.
        "payload_path": first_page["payload_path"],
        "payload_size_bytes": sum(
            int(descriptor["payload_size_bytes"]) for descriptor in page_descriptors
        ),
        "payload_mtime_ns": first_page["payload_mtime_ns"],
        "payload_sha256": first_page["payload_sha256"],
        "page_count": len(page_descriptors),
        "pages": page_descriptors,
        "max_page_size_bytes": max(
            int(descriptor["payload_size_bytes"]) for descriptor in page_descriptors
        ),
        "page_set_sha256": page_set_digest,
        "frontier_sha256": frontier_digest,
        "policy_fingerprint": policy_fingerprint,
        "policy_fingerprint_source": policy_fingerprint_source,
        "decision_at_ms": int(snapshot.candidate_build_observed_at_ms),
        "candidate_count": len(candidates),
        "quote_count": len(quotes),
        "seed_pair_count": max(seed_pair_count, 0),
        "pair_decision_count": max(pair_decision_count, 0),
        "eligible_candidate_count": max(eligible_candidate_count, 0),
        "omitted_eligible_count": max(omitted_eligible_count, 0),
        "eligible_frontier_complete": eligible_frontier_complete,
        "frontier_stop_reason": entry_diagnostics["frontier_stop_reason"],
    }
    manifest_path = funding_entry_snapshot_manifest_path(path)
    try:
        _atomic_write_json(manifest, manifest_path, indent=None)
    except Exception:
        # A pre-install failure leaves the old manifest authoritative.  Only
        # remove this generation when the manifest demonstrably does not point
        # at it; a directory-fsync failure can happen after rename visibility.
        current = _verified_funding_entry_generation(path, verify_digest=False)
        if current is None or current[4][0] != generation_id:
            for installed_page_path in installed_page_paths:
                try:
                    installed_page_path.unlink()
                except OSError:
                    pass
        raise
    installed = _verified_funding_entry_generation(path, verify_digest=True)
    if installed is None or installed[4][0] != generation_id:
        raise OSError("funding entry manifest did not install the prepared generation")
    manifest_stat = installed[3]
    installed_at_ms = (
        max(
            int(manifest_stat.st_ctime_ns),
            int(manifest_stat.st_mtime_ns),
        )
        // 1_000_000
    )
    # The manifest is now the sole authoritative pointer.  Removing the
    # previously selected immutable file cannot make a cold start miss the new
    # generation, and the loader retries if it raced the pointer swap.
    if previous_generation is not None:
        for previous_payload in previous_payload_paths:
            if previous_payload in installed_page_paths:
                continue
            try:
                previous_payload.unlink()
            except OSError:
                pass
    return {**manifest, "ready_at_ms": installed_at_ms}


def _verified_generation_payload_paths(
    path: str | Path,
    manifest: dict[str, object],
) -> list[Path]:
    if manifest.get("schema_version") == FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION:
        return _funding_entry_page_paths_from_manifest(path, manifest) or []
    payload_path = _funding_entry_payload_path_from_manifest(path, manifest)
    return [payload_path] if payload_path is not None else []


def _verified_funding_entry_generation(
    path: str | Path,
    *,
    verify_digest: bool,
) -> (
    tuple[
        dict[str, object],
        Path,
        os.stat_result,
        os.stat_result,
        tuple[str, int, int],
    ]
    | None
):
    installed = _read_funding_entry_manifest(path)
    if installed is None:
        return None
    manifest, manifest_stat = installed
    schema_version = manifest.get("schema_version")
    if isinstance(schema_version, bool) or schema_version not in {
        FUNDING_ENTRY_SNAPSHOT_LEGACY_SCHEMA_VERSION,
        FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION,
    }:
        return None
    generation_id = str(manifest.get("generation_id", "") or "")
    if not _is_sha256_hex(generation_id):
        return None
    if schema_version == FUNDING_ENTRY_SNAPSHOT_LEGACY_SCHEMA_VERSION:
        payload_path = _funding_entry_payload_path_from_manifest(path, manifest)
        if payload_path is None:
            return None
        try:
            payload_stat = payload_path.stat()
            if int(manifest.get("payload_size_bytes", -1)) != payload_stat.st_size:
                return None
            if int(manifest.get("payload_mtime_ns", -1)) != payload_stat.st_mtime_ns:
                return None
            if verify_digest:
                expected_digest = str(manifest.get("payload_sha256", "") or "")
                if not expected_digest or sha256(payload_path.read_bytes()).hexdigest() != (
                    expected_digest
                ):
                    return None
        except (OSError, TypeError, ValueError):
            return None
        identity = (generation_id, payload_stat.st_mtime_ns, payload_stat.st_size)
        return manifest, payload_path, payload_stat, manifest_stat, identity

    policy_fingerprint = manifest.get("policy_fingerprint")
    policy_fingerprint_source = manifest.get("policy_fingerprint_source")
    decision_at_ms = manifest.get("decision_at_ms")
    frontier_stop_reason = manifest.get("frontier_stop_reason")
    if (
        not _is_sha256_hex(policy_fingerprint)
        or policy_fingerprint_source not in {"explicit", "derived"}
        or not isinstance(frontier_stop_reason, str)
        or not frontier_stop_reason.strip()
        or (
            isinstance(decision_at_ms, bool)
            or not isinstance(decision_at_ms, int)
            or decision_at_ms <= 0
        )
    ):
        return None
    count_names = (
        "candidate_count",
        "quote_count",
        "seed_pair_count",
        "pair_decision_count",
        "eligible_candidate_count",
        "omitted_eligible_count",
    )
    counts: dict[str, int] = {}
    for name in count_names:
        value = manifest.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        counts[name] = value
    frontier_complete = manifest.get("eligible_frontier_complete")
    if not isinstance(frontier_complete, bool):
        return None
    if frontier_complete and (
        counts["pair_decision_count"] != counts["seed_pair_count"]
        or counts["eligible_candidate_count"] != counts["candidate_count"]
        or counts["omitted_eligible_count"] != 0
        or frontier_stop_reason != "all_pairs_decided"
    ):
        return None
    if not frontier_complete and (
        counts["candidate_count"] != 0 or frontier_stop_reason == "all_pairs_decided"
    ):
        return None
    page_paths = _funding_entry_page_paths_from_manifest(path, manifest)
    raw_descriptors = manifest.get("pages")
    if not page_paths or not isinstance(raw_descriptors, list):
        return None
    total_size = 0
    descriptor_candidate_count = 0
    descriptor_quote_count = 0
    page_stats: list[os.stat_result] = []
    digest_rows: list[dict[str, object]] = []
    try:
        for page_index, (page_path, descriptor) in enumerate(
            zip(page_paths, raw_descriptors, strict=True)
        ):
            if not isinstance(descriptor, dict):
                return None
            payload_size = descriptor.get("payload_size_bytes")
            payload_mtime = descriptor.get("payload_mtime_ns")
            candidate_count = descriptor.get("candidate_count")
            quote_count = descriptor.get("quote_count")
            expected_digest = descriptor.get("payload_sha256")
            if (
                isinstance(payload_size, bool)
                or not isinstance(payload_size, int)
                or payload_size <= 0
                or payload_size > FUNDING_ENTRY_SNAPSHOT_MAX_BYTES
                or isinstance(payload_mtime, bool)
                or not isinstance(payload_mtime, int)
                or payload_mtime <= 0
                or isinstance(candidate_count, bool)
                or not isinstance(candidate_count, int)
                or candidate_count < 0
                or isinstance(quote_count, bool)
                or not isinstance(quote_count, int)
                or quote_count < 0
                or not _is_sha256_hex(expected_digest)
            ):
                return None
            page_stat = page_path.stat()
            if payload_size != page_stat.st_size or payload_mtime != page_stat.st_mtime_ns:
                return None
            if verify_digest and sha256(page_path.read_bytes()).hexdigest() != expected_digest:
                return None
            page_stats.append(page_stat)
            total_size += payload_size
            descriptor_candidate_count += candidate_count
            descriptor_quote_count += quote_count
            digest_rows.append({"page_index": page_index, "payload_sha256": expected_digest})
    except (OSError, TypeError, ValueError):
        return None
    if (
        total_size != manifest.get("payload_size_bytes")
        or descriptor_candidate_count != counts["candidate_count"]
        or descriptor_quote_count != counts["quote_count"]
        or max(page_stat.st_size for page_stat in page_stats) != manifest.get("max_page_size_bytes")
        or str(manifest.get("payload_path", "") or "")
        != str(raw_descriptors[0].get("payload_path", "") or "")
        or manifest.get("payload_mtime_ns") != raw_descriptors[0].get("payload_mtime_ns")
        or manifest.get("payload_sha256") != raw_descriptors[0].get("payload_sha256")
        or sha256(_json_text(digest_rows, indent=None).encode("utf-8")).hexdigest()
        != manifest.get("page_set_sha256")
        or not _is_sha256_hex(manifest.get("frontier_sha256"))
    ):
        return None
    identity = (generation_id, manifest_stat.st_mtime_ns, total_size)
    return manifest, page_paths[0], page_stats[0], manifest_stat, identity


def funding_entry_snapshot_identity(
    path: str | Path,
    *,
    verify_digest: bool = False,
) -> tuple[str, int, int] | None:
    """Return a verified generation identity; never expose a half-installed payload."""
    installed = _verified_funding_entry_generation(
        path,
        verify_digest=verify_digest,
    )
    return None if installed is None else installed[4]


def _load_v7_funding_entry_generation(
    path: str | Path,
    manifest: dict[str, object],
) -> SidecarSnapshot | None:
    page_paths = _funding_entry_page_paths_from_manifest(path, manifest)
    descriptors = manifest.get("pages")
    if not page_paths or not isinstance(descriptors, list):
        return None
    generation_id = str(manifest.get("generation_id", "") or "")
    policy_fingerprint = str(manifest.get("policy_fingerprint", "") or "")
    decision_at_ms = manifest.get("decision_at_ms")
    page_count = len(page_paths)
    common: dict[str, object] | None = None
    quotes: dict[str, object] = {}
    candidates: list[dict[str, object]] = []
    candidate_identities: set[str] = set()
    allowed_page_keys = {
        "schema_version",
        "generation_id",
        "policy_fingerprint",
        "decision_at_ms",
        "page_index",
        "page_count",
        "snapshot_common",
        "quotes",
        "candidates",
    }
    try:
        for page_index, (page_path, descriptor) in enumerate(
            zip(page_paths, descriptors, strict=True)
        ):
            with open(page_path, "rb") as page_file:
                page = json.loads(page_file.read())
            if not isinstance(page, dict) or set(page) - allowed_page_keys:
                return None
            if (
                page.get("schema_version") != FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION
                or page.get("generation_id") != generation_id
                or page.get("policy_fingerprint") != policy_fingerprint
                or page.get("decision_at_ms") != decision_at_ms
                or page.get("page_index") != page_index
                or page.get("page_count") != page_count
            ):
                return None
            page_common = page.get("snapshot_common")
            if page_index == 0:
                if not isinstance(page_common, dict):
                    return None
                common = page_common
            elif "snapshot_common" in page:
                return None
            page_quotes = page.get("quotes")
            page_candidates = page.get("candidates")
            if not isinstance(page_quotes, dict) or not isinstance(page_candidates, list):
                return None
            if not isinstance(descriptor, dict) or (
                descriptor.get("quote_count") != len(page_quotes)
                or descriptor.get("candidate_count") != len(page_candidates)
            ):
                return None
            for quote_key, quote in page_quotes.items():
                if not isinstance(quote_key, str) or quote_key in quotes:
                    return None
                quotes[quote_key] = quote
            for candidate in page_candidates:
                if not isinstance(candidate, dict):
                    return None
                identity = _funding_entry_candidate_identity(candidate)
                if not identity or identity in candidate_identities:
                    return None
                candidate_identities.add(identity)
                candidates.append(candidate)
    except (OSError, TypeError, ValueError, json.JSONDecodeError, OverflowError):
        return None
    if common is None:
        return None
    data = {**common, "quotes": quotes, "candidates": candidates}
    diagnostics = data.get("candidate_build_diagnostics")
    if not isinstance(diagnostics, dict):
        return None
    manifest_diagnostic_fields = (
        "seed_pair_count",
        "pair_decision_count",
        "eligible_candidate_count",
        "omitted_eligible_count",
        "eligible_frontier_complete",
    )
    if (
        data.get("candidate_build_observed_at_ms") != decision_at_ms
        or diagnostics.get("entry_policy_fingerprint") != policy_fingerprint
        or diagnostics.get("entry_policy_fingerprint_source")
        != manifest.get("policy_fingerprint_source")
        or diagnostics.get("frontier_stop_reason") != manifest.get("frontier_stop_reason")
        or any(diagnostics.get(name) != manifest.get(name) for name in manifest_diagnostic_fields)
        or len(candidates) != manifest.get("candidate_count")
        or len(quotes) != manifest.get("quote_count")
        or sha256(_json_text(candidates, indent=None).encode("utf-8")).hexdigest()
        != manifest.get("frontier_sha256")
    ):
        return None
    if validate_v5_snapshot_contract(data):
        return None
    try:
        return _dict_to_snapshot(data)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def load_funding_entry_snapshot(path: str | Path) -> SidecarSnapshot | None:
    # A reader can capture G1 immediately before G2 installs and prunes G1.
    # Re-reading the manifest once turns that benign race into a G2 cold start
    # instead of a false missing snapshot.
    for _attempt in range(2):
        first = _verified_funding_entry_generation(path, verify_digest=True)
        if first is None:
            continue
        loaded = (
            _load_v7_funding_entry_generation(path, first[0])
            if first[0].get("schema_version") == FUNDING_ENTRY_SNAPSHOT_SCHEMA_VERSION
            else load_snapshot(first[1])
        )
        if loaded is None:
            continue
        second = _verified_funding_entry_generation(path, verify_digest=True)
        if second is None:
            continue
        first_token = (
            first[4],
            first[3].st_ino,
            first[3].st_ctime_ns,
            first[3].st_mtime_ns,
            first[3].st_size,
        )
        second_token = (
            second[4],
            second[3].st_ino,
            second[3].st_ctime_ns,
            second[3].st_mtime_ns,
            second[3].st_size,
        )
        if first_token != second_token:
            continue
        # Atomic rename/install is the publication boundary.  The inode
        # change clock cannot predate that boundary, unlike a timestamp
        # serialized before fsync/rename.
        ready_at_ms = (
            max(
                int(second[3].st_ctime_ns),
                int(second[3].st_mtime_ns),
            )
            // 1_000_000
        )
        if ready_at_ms > 0:
            return replace(loaded, ready_at_ms=ready_at_ms)
    return None


def load_snapshot(path: str | Path) -> SidecarSnapshot | None:
    """Load and validate a sidecar snapshot. Returns None if missing or malformed."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(p) as f:
            data = json.loads(f.read())
    # A syntactically valid snapshot can still carry an invalid scalar from a
    # partial writer, an older publisher, or manual recovery.  Parsing is the
    # compatibility boundary; no caller in the live loop should have to catch
    # a malformed field before deciding whether entry is safe.
    except (json.JSONDecodeError, OSError, TypeError, ValueError, OverflowError):
        return None
    if not isinstance(data, dict):
        return None
    raw_schema_version = data.get("schema_version")
    if not isinstance(raw_schema_version, int) or isinstance(raw_schema_version, bool):
        return None
    schema_version = int(raw_schema_version)
    if schema_version not in {
        *LEGACY_SNAPSHOT_SCHEMA_VERSIONS,
        SNAPSHOT_SCHEMA_VERSION,
    }:
        return None
    if schema_version == 4 and not _v4_proof_contract_valid(data):
        return None
    if schema_version == SNAPSHOT_SCHEMA_VERSION and not _v5_proof_contract_valid(data):
        return None
    try:
        return _dict_to_snapshot(data)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def _v4_proof_contract_valid(data: dict[str, object]) -> bool:
    """Require producer proof before a v4 snapshot enters live consumers."""
    return not validate_v4_snapshot_contract(data)


def _v5_proof_contract_valid(data: dict[str, object]) -> bool:
    """Require independent OI proof before a V5 snapshot is trusted."""
    return not validate_v5_snapshot_contract(data)


def _snapshot_to_dict(s: SidecarSnapshot) -> dict:
    return {
        "schema_version": s.schema_version,
        "published_at_ms": s.published_at_ms,
        "market_observed_at_ms": s.market_observed_at_ms,
        "funding_lifecycle": [
            {
                "venue": fl.venue,
                "observed_at_ms": fl.observed_at_ms,
                "symbol_count": fl.symbol_count,
                "coverage_usable": fl.coverage_usable,
                "degraded_reason": fl.degraded_reason,
            }
            for fl in s.funding_lifecycle
        ],
        "market_lifecycle": [
            {
                "venue": ml.venue,
                "observed_at_ms": ml.observed_at_ms,
                "symbol_count": ml.symbol_count,
                "coverage_usable": ml.coverage_usable,
                "degraded_reason": ml.degraded_reason,
            }
            for ml in s.market_lifecycle
        ],
        "transfer_lifecycle": [
            {
                "from_venue": tl.from_venue,
                "to_venue": tl.to_venue,
                "observed_at_ms": tl.observed_at_ms,
                "coverage_usable": tl.coverage_usable,
                "degraded_reason": tl.degraded_reason,
            }
            for tl in s.transfer_lifecycle
        ],
        "liquidity_lifecycle": [
            {
                "venue": ll.venue,
                "observed_at_ms": ll.observed_at_ms,
                "symbol_count": ll.symbol_count,
                "coverage_usable": ll.coverage_usable,
                "degraded_reason": ll.degraded_reason,
                "domain": ll.domain,
                "source": ll.source,
                "publish_interval_ms": ll.publish_interval_ms,
                "published_at_ms": ll.published_at_ms,
            }
            for ll in s.liquidity_lifecycle
        ],
        "degraded_venues": list(s.degraded_venues),
        "degraded_domains": list(s.degraded_domains),
        "degraded_symbols": {k: list(v) for k, v in s.degraded_symbols.items()},
        "source_mode": s.source_mode,
        "acquisition_mode": s.acquisition_mode,
        "candidate_build_observed_at_ms": s.candidate_build_observed_at_ms,
        "candidate_build_diagnostics": dict(s.candidate_build_diagnostics),
        "quotes": {
            k: {
                "venue": q.venue,
                "symbol": q.symbol,
                "bid": q.bid,
                "ask": q.ask,
                "observed_at_ms": q.observed_at_ms,
                "market_event_at_ms": q.market_event_at_ms,
                "source": q.source,
                "bid_size": q.bid_size,
                "ask_size": q.ask_size,
                "bid_depth": [list(level) for level in q.bid_depth],
                "ask_depth": [list(level) for level in q.ask_depth],
                "funding_rate_bps": q.funding_rate_bps,
                "funding_rate_observed_at_ms": q.funding_rate_observed_at_ms,
                "funding_rate_event_at_ms": q.funding_rate_event_at_ms,
                "funding_rate_received_at_ms": q.funding_rate_received_at_ms,
                "funding_rate_source": q.funding_rate_source,
                "funding_rate_sample_id": q.funding_rate_sample_id,
                "funding_timestamp_ms": q.funding_timestamp_ms,
                "funding_interval_ms": q.funding_interval_ms,
                "predicted_funding_rate_bps": q.predicted_funding_rate_bps,
                "funding_forecast_source": q.funding_forecast_source,
                "funding_forecast_sample_count": q.funding_forecast_sample_count,
                "funding_forecast_uncertainty_bps": q.funding_forecast_uncertainty_bps,
                "funding_forecast_started_at_ms": q.funding_forecast_started_at_ms,
                "funding_forecast_distribution_stable": q.funding_forecast_distribution_stable,
                "funding_forecast_stability_reason": q.funding_forecast_stability_reason,
                "funding_forecast_median_drift_bps": q.funding_forecast_median_drift_bps,
                "funding_forecast_p90_drift_bps": q.funding_forecast_p90_drift_bps,
                "settled_funding_rate_bps": q.settled_funding_rate_bps,
                "mark_price": q.mark_price,
                "index_price": q.index_price,
                "volume_24h_quote": q.volume_24h_quote,
                "open_interest": q.open_interest,
                "open_interest_evidence_status": q.open_interest_evidence_status,
                "open_interest_evidence_reason": q.open_interest_evidence_reason,
                "open_interest_observed_at_ms": q.open_interest_observed_at_ms,
                "open_interest_event_at_ms": q.open_interest_event_at_ms,
                "open_interest_received_at_ms": q.open_interest_received_at_ms,
                "open_interest_source": q.open_interest_source,
                "open_interest_sample_id": q.open_interest_sample_id,
                "open_interest_venue_symbol": q.open_interest_venue_symbol,
                "raw_open_interest": q.raw_open_interest,
                "raw_open_interest_unit": q.raw_open_interest_unit,
                "open_interest_contract_multiplier": q.open_interest_contract_multiplier,
                "open_interest_conversion_mark_price": q.open_interest_conversion_mark_price,
                "oi_candidate_count": q.oi_candidate_count,
                "oi_cache_hit_count": q.oi_cache_hit_count,
                "oi_cache_miss_count": q.oi_cache_miss_count,
                "oi_refresh_attempt_count": q.oi_refresh_attempt_count,
                "oi_refresh_cap": q.oi_refresh_cap,
                "oi_deferred_count": q.oi_deferred_count,
                "oi_timeout_count": q.oi_timeout_count,
                "oi_refresh_elapsed_ms": q.oi_refresh_elapsed_ms,
                "underlying": q.underlying,
                "quote_currency": q.quote_currency,
                "contract_type": q.contract_type,
                "contract_multiplier": q.contract_multiplier,
                "mark_index_source": q.mark_index_source,
                "price_precision": q.price_precision,
                "quantity_precision": q.quantity_precision,
                "price_tick": q.price_tick,
                "quantity_step_base": q.quantity_step_base,
                "min_quantity_base": q.min_quantity_base,
                "min_notional_quote": q.min_notional_quote,
                "min_notional_evidence_complete": q.min_notional_evidence_complete,
                "venue_status": q.venue_status,
                "contract_normalization_complete": q.contract_normalization_complete,
            }
            for k, q in s.quotes.items()
        },
        "candidates": [
            {
                "long_venue": c.long_venue,
                "short_venue": c.short_venue,
                "symbol": c.symbol,
                "funding_diff_bps": c.funding_diff_bps,
                "funding_edge_bps": c.funding_edge_bps,
                "expected_edge_bps": c.expected_edge_bps,
                "worst_case_edge_bps": c.worst_case_edge_bps,
                "ranking_edge_bps": c.ranking_edge_bps,
                "total_funding_edge_bps": c.total_funding_edge_bps,
                "first_stage_funding_edge_bps": c.first_stage_funding_edge_bps,
                "first_stage_expected_edge_bps": c.first_stage_expected_edge_bps,
                "first_stage_worst_case_edge_bps": c.first_stage_worst_case_edge_bps,
                "second_stage_incremental_funding_edge_bps": c.second_stage_incremental_funding_edge_bps,
                "second_stage_worst_case_funding_edge_bps": c.second_stage_worst_case_funding_edge_bps,
                "stagger_gap_ms": c.stagger_gap_ms,
                "transfer_bias_bps": c.transfer_bias_bps,
                "entry_cross_bps": c.entry_cross_bps,
                "fee_bps": c.fee_bps,
                "entry_slippage_bps": c.entry_slippage_bps,
                "long_entry_slippage_bps": c.long_entry_slippage_bps,
                "short_entry_slippage_bps": c.short_entry_slippage_bps,
                "long_exit_slippage_bps": c.long_exit_slippage_bps,
                "short_exit_slippage_bps": c.short_exit_slippage_bps,
                "long_taker_fee_bps": c.long_taker_fee_bps,
                "short_taker_fee_bps": c.short_taker_fee_bps,
                "taker_fee_evidence_complete": c.taker_fee_evidence_complete,
                # Untrusted assertion only.  The live admission boundary
                # reloads the signed account-fee document and requires an
                # exact epoch/fingerprint/provenance match before authorising
                # an order.  Dropping these fields here makes every genuine
                # candidate indistinguishable from a provenance-free one
                # after the required JSON round trip.
                "account_fee_evidence_complete": c.account_fee_evidence_complete,
                "account_fee_evidence_observed_at_ms": (c.account_fee_evidence_observed_at_ms),
                "account_fee_evidence_source": c.account_fee_evidence_source,
                "account_fee_evidence_fingerprint": (c.account_fee_evidence_fingerprint),
                "account_fee_evidence_provenance": list(c.account_fee_evidence_provenance),
                "opportunity_type": c.opportunity_type,
                "blocked": c.blocked,
                "blocked_reasons": c.blocked_reasons,
                "pair_id": c.pair_id,
                "funding_timestamp_ms": c.funding_timestamp_ms,
                "first_funding_timestamp_ms": c.first_funding_timestamp_ms,
                "long_funding_timestamp_ms": c.long_funding_timestamp_ms,
                "short_funding_timestamp_ms": c.short_funding_timestamp_ms,
                "second_funding_timestamp_ms": c.second_funding_timestamp_ms,
                "entry_notional_quote": c.entry_notional_quote,
                "entry_max_leg_notional_quote": c.entry_max_leg_notional_quote,
                "funding_canary_fee_assurance_tier": (c.funding_canary_fee_assurance_tier),
                "funding_canary_hard_max_entry_notional_quote": (
                    c.funding_canary_hard_max_entry_notional_quote
                ),
                "funding_canary_size_constrained": c.funding_canary_size_constrained,
                "funding_canary_requested_quantity": (c.funding_canary_requested_quantity),
                "funding_canary_requested_max_leg_notional_quote": (
                    c.funding_canary_requested_max_leg_notional_quote
                ),
                "contract_price_consistency_ratio": (c.contract_price_consistency_ratio),
                "contract_price_consistency_long_price": (c.contract_price_consistency_long_price),
                "contract_price_consistency_short_price": (
                    c.contract_price_consistency_short_price
                ),
                "candidate_revision_id": c.candidate_revision_id,
                "opportunity_lease_id": c.opportunity_lease_id,
                "candidate_built_at_ms": c.candidate_built_at_ms,
                "first_funding_leg": c.first_funding_leg,
                "entry_maker_leg": c.entry_maker_leg,
                "exit_maker_leg": c.exit_maker_leg,
                "transfer_state_at_entry": c.transfer_state_at_entry,
                "entry_liquidity_source_at_entry": c.entry_liquidity_source_at_entry,
                "long_volume_24h_quote": c.long_volume_24h_quote,
                "short_volume_24h_quote": c.short_volume_24h_quote,
                "long_open_interest_quote_at_entry": c.long_open_interest_quote_at_entry,
                "short_open_interest_quote_at_entry": c.short_open_interest_quote_at_entry,
                "long_entry_vwap": c.long_entry_vwap,
                "short_entry_vwap": c.short_entry_vwap,
                "entry_capacity_constrained": c.entry_capacity_constrained,
                "entry_target_quantity": c.entry_target_quantity,
                "long_max_executable_quantity": c.long_max_executable_quantity,
                "short_max_executable_quantity": c.short_max_executable_quantity,
                "entry_max_executable_quantity": c.entry_max_executable_quantity,
                "entry_depth_shortfall_quantity": c.entry_depth_shortfall_quantity,
                "entry_max_executable_notional_quote": c.entry_max_executable_notional_quote,
                "entry_depth_capped_at_entry": c.entry_depth_capped_at_entry,
                "advisories": c.advisories,
                "direction_consistent": c.direction_consistent,
                "interval_aligned": c.interval_aligned,
                "sizing_liquidity_source": c.sizing_liquidity_source,
                "gross_signal_edge_bps": c.gross_signal_edge_bps,
                "expected_exit_cross_bps": c.expected_exit_cross_bps,
                "entry_fee_bps": c.entry_fee_bps,
                "exit_fee_bps": c.exit_fee_bps,
                "exit_slippage_bps": c.exit_slippage_bps,
                "adverse_selection_bps": c.adverse_selection_bps,
                "capital_buffer_bps": c.capital_buffer_bps,
                "execution_buffer_bps": c.execution_buffer_bps,
                "venue_risk_haircut_bps": c.venue_risk_haircut_bps,
                "transfer_or_inventory_bias_bps": c.transfer_or_inventory_bias_bps,
                "expected_net_edge_bps": c.expected_net_edge_bps,
                "economics_observed_at_ms": c.economics_observed_at_ms,
                "economics_complete": c.economics_complete,
                "economics_incomplete_reason": c.economics_incomplete_reason,
                "calculation_version": c.calculation_version,
                "model_epoch": c.model_epoch,
                "forecast_long_rate_bps": c.forecast_long_rate_bps,
                "forecast_short_rate_bps": c.forecast_short_rate_bps,
                "forecast_worst_funding_edge_bps": c.forecast_worst_funding_edge_bps,
                "forecast_confidence": c.forecast_confidence,
                "forecast_sample_count": c.forecast_sample_count,
                # This is audit evidence for the mandatory shadow period.  Do
                # not let a publish/load round trip turn a mature forecast
                # back into an apparently cold one.
                "forecast_shadow_age_ms": c.forecast_shadow_age_ms,
                "forecast_ready": c.forecast_ready,
                "forecast_distribution_stable": c.forecast_distribution_stable,
                "forecast_stability_reason": c.forecast_stability_reason,
                "forecast_median_drift_bps": c.forecast_median_drift_bps,
                "forecast_p90_drift_bps": c.forecast_p90_drift_bps,
                "forecast_source": c.forecast_source,
            }
            for c in s.candidates
        ],
    }


def _dict_to_snapshot(d: dict) -> SidecarSnapshot:
    schema_version = _safe_int(d.get("schema_version"), default=0)
    # --- V1 compat: convert V1 Rust sidecar format to V2 (see v1_compat.py) ---
    if schema_version == 1:
        from lightfee.sidecar.v1_compat import convert_v1_snapshot_to_v2

        d = convert_v1_snapshot_to_v2(d)
        schema_version = _safe_int(d.get("schema_version"), default=0)
    # --- end V1 compat ---

    from lightfee.sidecar.snapshot import (
        CandidateInput,
        FundingLifecycle,
        LiquidityLifecycle,
        MarketLifecycle,
        TransferLifecycle,
    )

    quotes_raw = d.get("quotes", {})
    quote_evidence_index = _v3_quote_contract_evidence_index(quotes_raw)

    def _enrich_candidate(c: dict) -> CandidateInput:
        """Enrich a candidate dict with V1-required identity + prewarm fields.

        V1: every CandidateOpportunity carries a stable pair_id and a usable
        first_funding_timestamp_ms.  When a schema-2 snapshot omits these
        fields, we derive them from the candidate's own symbol/venues and the
        snapshot quotes so the runtime can apply the prewarm gate correctly.
        If we cannot derive a usable timestamp, the candidate is marked
        blocked — it must not appear tradeable with first_funding_timestamp_ms=0.

        Derivation order (V1 parity):
        1. Raw candidate long_funding_timestamp_ms / short_funding_timestamp_ms
        2. Snapshot quotes for long_venue:symbol and short_venue:symbol
        """
        c = dict(c)
        # v1/v2 snapshots predate the complete economics contract. Keep their
        # displayed legacy fields for V1 recovery/diagnostics, but they can
        # never grant live admission even when a hand-edited old snapshot
        # asserts ``economics_complete=true``.
        raw_economics_complete = c.get("economics_complete", False)
        complete_economics = raw_economics_complete is True
        economics_observed_at_ms = _safe_int(
            c.get("economics_observed_at_ms"),
            default=0,
        )
        contract_reason = (
            _v3_economics_contract_reason(
                c,
                quotes_raw=quotes_raw,
                quote_evidence_index=quote_evidence_index,
            )
            if schema_version >= 3 and complete_economics
            else ""
        )
        if (
            schema_version >= 3
            and raw_economics_complete is not True
            and raw_economics_complete is not False
        ):
            contract_reason = "invalid_v3_economics_field:economics_complete"
        if schema_version < 3 or (
            schema_version >= 3
            and (not complete_economics or economics_observed_at_ms <= 0 or bool(contract_reason))
        ):
            c["economics_complete"] = False
            c.setdefault("calculation_version", "legacy_schema_incomplete")
            c.setdefault("model_epoch", "v1_legacy")
            if schema_version >= 3:
                incomplete_reason = (
                    contract_reason
                    or str(c.get("economics_incomplete_reason", "") or "").strip()
                    or "incomplete_v3_economics"
                )
                c["economics_incomplete_reason"] = incomplete_reason
                c["blocked"] = True
                blocked_reasons = c.get("blocked_reasons")
                normalized_blocked_reasons = (
                    list(blocked_reasons) if isinstance(blocked_reasons, list) else []
                )
                if incomplete_reason not in normalized_blocked_reasons:
                    normalized_blocked_reasons.append(incomplete_reason)
                c["blocked_reasons"] = normalized_blocked_reasons

        pair_id = str(c.get("pair_id", "") or "")
        symbol = str(c.get("symbol", ""))
        long_ven = str(c.get("long_venue", ""))
        short_ven = str(c.get("short_venue", ""))

        if not pair_id and symbol and long_ven and short_ven:
            pair_id = f"{symbol.lower()}:{long_ven}->{short_ven}"

        raw_ff_ts = c.get("first_funding_timestamp_ms")
        explicit_ff_ts_malformed = (
            schema_version >= 3
            and "first_funding_timestamp_ms" in c
            and raw_ff_ts not in (None, 0)
            and (isinstance(raw_ff_ts, bool) or not isinstance(raw_ff_ts, int))
        )
        ff_ts = _safe_int(raw_ff_ts, default=0)
        f_ts = _safe_int(c.get("funding_timestamp_ms"), default=0)
        long_fts = _safe_int(c.get("long_funding_timestamp_ms"), default=0)
        short_fts = _safe_int(c.get("short_funding_timestamp_ms"), default=0)

        # Derive long/short timestamps from quotes if missing in raw candidate
        if long_fts <= 0 or short_fts <= 0:
            for venue, target in [(long_ven, "long"), (short_ven, "short")]:
                qkey = f"{venue}:{symbol}"
                q = quotes_raw.get(qkey, {})
                if isinstance(q, dict):
                    qts = _safe_int(q.get("funding_timestamp_ms"), default=0)
                    if qts > 0:
                        if target == "long" and long_fts <= 0:
                            long_fts = qts
                        elif target == "short" and short_fts <= 0:
                            short_fts = qts

        # Derive first_funding_timestamp_ms from per-leg timestamps (V1: min(long, short))
        if not explicit_ff_ts_malformed and ff_ts <= 0 and long_fts > 0 and short_fts > 0:
            ff_ts = min(long_fts, short_fts)
        elif not explicit_ff_ts_malformed and ff_ts <= 0:
            # Fallback: derive from quotes
            ts_candidates: list[int] = []
            for venue in (long_ven, short_ven):
                qkey = f"{venue}:{symbol}"
                q = quotes_raw.get(qkey, {})
                if isinstance(q, dict):
                    qts = _safe_int(q.get("funding_timestamp_ms"), default=0)
                    if qts > 0:
                        ts_candidates.append(qts)
            if ts_candidates:
                ff_ts = min(ts_candidates)
        if f_ts <= 0 and ff_ts > 0:
            f_ts = ff_ts

        # Compute second_funding_timestamp_ms (V1: max(long, short))
        second_fts = 0
        if long_fts > 0 and short_fts > 0:
            second_fts = max(long_fts, short_fts)

        # Do not let raw JSON scalars bypass the parser boundary.  CandidateInput
        # is a plain dataclass, so an invalid timestamp string would otherwise
        # survive construction and later crash whichever live/readiness path
        # first compares it with an integer.  Rewriting these fields keeps the
        # snapshot readable while the candidate itself remains blocked below.
        c["first_funding_timestamp_ms"] = ff_ts
        c["funding_timestamp_ms"] = f_ts
        c["long_funding_timestamp_ms"] = long_fts
        c["short_funding_timestamp_ms"] = short_fts
        c["second_funding_timestamp_ms"] = second_fts

        invalid_fields = _normalise_candidate_fields(c, CandidateInput)
        if invalid_fields:
            c["blocked"] = True
            blocked_reasons = c.get("blocked_reasons")
            c["blocked_reasons"] = (
                list(blocked_reasons) if isinstance(blocked_reasons, list) else []
            ) + [f"invalid_candidate_field:{name}" for name in invalid_fields]
            c["economics_complete"] = False
            c["economics_incomplete_reason"] = (
                contract_reason
                or str(c.get("economics_incomplete_reason", "") or "")
                or "invalid_candidate_fields:" + ",".join(invalid_fields)
            )

        candidate = CandidateInput(**c)
        if pair_id:
            candidate.pair_id = pair_id
        if ff_ts > 0:
            candidate.first_funding_timestamp_ms = ff_ts
        if f_ts > 0:
            candidate.funding_timestamp_ms = f_ts
        if long_fts > 0:
            candidate.long_funding_timestamp_ms = long_fts
        if short_fts > 0:
            candidate.short_funding_timestamp_ms = short_fts
        if second_fts > 0:
            candidate.second_funding_timestamp_ms = second_fts

        # V1: first_funding_leg — which leg's funding settles first
        # discovery.rs:850-863 — Long if long_ts <= short_ts, else Short
        if long_fts > 0 and short_fts > 0:
            candidate.first_funding_leg = "long" if long_fts <= short_fts else "short"

        # Fail-closed: candidates without usable funding timestamp are not tradeable.
        # V1 never emits a tradeable candidate with first_funding_timestamp_ms=0.
        if candidate.first_funding_timestamp_ms <= 0:
            candidate.blocked = True
            candidate.blocked_reasons = list(candidate.blocked_reasons) + [
                "missing_candidate_identity_or_funding_timestamp"
            ]

        return candidate

    return SidecarSnapshot(
        schema_version=schema_version,
        published_at_ms=_safe_int(d.get("published_at_ms"), default=0),
        market_observed_at_ms=_safe_int(d.get("market_observed_at_ms"), default=0),
        funding_lifecycle=[FundingLifecycle(**fl) for fl in d.get("funding_lifecycle", [])],
        market_lifecycle=[MarketLifecycle(**ml) for ml in d.get("market_lifecycle", [])],
        transfer_lifecycle=[TransferLifecycle(**tl) for tl in d.get("transfer_lifecycle", [])],
        liquidity_lifecycle=[LiquidityLifecycle(**ll) for ll in d.get("liquidity_lifecycle", [])],
        degraded_venues=d.get("degraded_venues", []),
        degraded_domains=d.get("degraded_domains", []),
        degraded_symbols=_parse_degraded_symbols(d.get("degraded_symbols", {})),
        source_mode=d.get("source_mode", ""),
        acquisition_mode=d.get("acquisition_mode", ""),
        candidate_build_observed_at_ms=_safe_int(
            d.get("candidate_build_observed_at_ms"),
            default=0,
        ),
        candidate_build_diagnostics=(
            dict(d.get("candidate_build_diagnostics", {}))
            if isinstance(d.get("candidate_build_diagnostics", {}), dict)
            else {}
        ),
        quotes={
            k: _quote_from_dict(v, schema_version=int(d.get("schema_version", 0) or 0))
            for k, v in quotes_raw.items()
        },
        candidates=[_enrich_candidate(c) for c in d.get("candidates", [])],
    )


def _quote_from_dict(
    raw: object, *, schema_version: int = SNAPSHOT_SCHEMA_VERSION
) -> QuoteSnapshot:
    """Parse optional executable depth at the snapshot compatibility boundary.

    A malformed ladder must never make a historical BBO snapshot unreadable or
    turn arbitrary JSON into executable liquidity.  Drop the malformed ladder
    and retain the independently validated BBO fields; the paper engine will
    then take its conservative top-book-only path.
    """
    if not isinstance(raw, dict):
        raise TypeError("quote must be an object")
    value = dict(raw)
    value["bid_depth"] = _normalise_depth(value.get("bid_depth"))
    value["ask_depth"] = _normalise_depth(value.get("ask_depth"))
    for field in (
        "bid",
        "ask",
        "bid_size",
        "ask_size",
        "funding_rate_bps",
        "funding_forecast_uncertainty_bps",
        "funding_forecast_median_drift_bps",
        "funding_forecast_p90_drift_bps",
        "mark_price",
        "index_price",
        "volume_24h_quote",
        "contract_multiplier",
        "price_tick",
        "quantity_step_base",
        "min_quantity_base",
        "min_notional_quote",
    ):
        if field in value:
            value[field] = _safe_float(value[field], default=0.0)
    for field in ("predicted_funding_rate_bps", "settled_funding_rate_bps"):
        if field in value:
            value[field] = _safe_optional_float(value[field])
    for field in (
        "open_interest",
        "raw_open_interest",
        "open_interest_contract_multiplier",
        "open_interest_conversion_mark_price",
    ):
        if field in value:
            value[field] = _safe_optional_float(value[field])
    for field in (
        "observed_at_ms",
        "market_event_at_ms",
        "funding_timestamp_ms",
        "funding_rate_observed_at_ms",
        "funding_rate_event_at_ms",
        "funding_rate_received_at_ms",
        "funding_interval_ms",
        "funding_forecast_sample_count",
        "funding_forecast_started_at_ms",
        "oi_candidate_count",
        "oi_cache_hit_count",
        "oi_cache_miss_count",
        "oi_refresh_attempt_count",
        "oi_refresh_cap",
        "oi_deferred_count",
        "oi_timeout_count",
        "oi_refresh_elapsed_ms",
        "open_interest_observed_at_ms",
        "open_interest_event_at_ms",
        "open_interest_received_at_ms",
        "price_precision",
        "quantity_precision",
    ):
        if field in value:
            value[field] = _safe_int(value[field], default=0)
    if schema_version < 5:
        value["open_interest"] = None
        value["open_interest_evidence_status"] = "stale"
        value["open_interest_evidence_reason"] = "legacy_snapshot_missing_oi_proof"
        value["open_interest_observed_at_ms"] = 0
        value["open_interest_event_at_ms"] = 0
        value["open_interest_received_at_ms"] = 0
        value["open_interest_source"] = ""
        value["open_interest_sample_id"] = ""
        value["open_interest_venue_symbol"] = ""
        value["raw_open_interest"] = None
        value["raw_open_interest_unit"] = ""
        value["open_interest_contract_multiplier"] = None
        value["open_interest_conversion_mark_price"] = None
    # Snapshot JSON is an untrusted compatibility boundary.  Python treats a
    # non-empty string such as ``"false"`` as truthy, which would otherwise
    # let malformed v3 evidence assert forecast stability or contract
    # normalisation.  Only an actual JSON boolean may grant either permission;
    # absent legacy fields retain their dataclass fail-closed defaults.
    for field in (
        "funding_forecast_distribution_stable",
        "contract_normalization_complete",
        "min_notional_evidence_complete",
    ):
        if field in value and value[field] is not True and value[field] is not False:
            value[field] = False
    return QuoteSnapshot(**value)


def _safe_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if not isfinite(numeric) or numeric <= 0.0 or not numeric.is_integer():
        return default
    return int(numeric)


def _safe_float(value: object, *, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if not isfinite(numeric):
        return default
    return numeric


def _safe_optional_float(value: object) -> float | None:
    if value is None:
        return None
    parsed = _safe_float(value, default=float("nan"))
    return parsed if isfinite(parsed) else None


def _normalise_candidate_fields(
    raw: dict[str, object],
    candidate_type: type,
) -> list[str]:
    """Fail closed and make every present candidate scalar runtime-safe."""
    invalid: list[str] = []
    for candidate_field in dataclass_fields(candidate_type):
        name = candidate_field.name
        if name not in raw:
            continue
        value = raw[name]
        annotation = str(candidate_field.type).strip("'\"")
        normalized = value
        valid = True
        if annotation == "float":
            valid = type(value) in (int, float) and isfinite(float(value))
            normalized = float(value) if valid else 0.0
        elif annotation == "int":
            valid = type(value) is int
            normalized = value if valid else 0
        elif annotation == "bool":
            valid = type(value) is bool
            normalized = value if valid else False
        elif annotation == "str":
            valid = isinstance(value, str)
            normalized = value if valid else ""
        elif annotation == "Optional[float]":
            valid = value is None or (type(value) in (int, float) and isfinite(float(value)))
            normalized = None if value is None or not valid else float(value)
        elif annotation == "Optional[str]":
            valid = value is None or isinstance(value, str)
            normalized = value if valid else None
        elif annotation == "list[str]":
            valid = isinstance(value, list) and all(isinstance(item, str) for item in value)
            normalized = list(value) if valid else []
        elif annotation == "list[dict[str, object]]":
            valid = isinstance(value, list) and all(isinstance(item, dict) for item in value)
            normalized = list(value) if valid else []
        raw[name] = normalized
        if not valid:
            invalid.append(name)
    return invalid


def _normalise_depth(raw: object) -> tuple[tuple[float, float], ...]:
    if raw in (None, ""):
        return ()
    if not isinstance(raw, (list, tuple)):
        return ()
    levels: list[tuple[float, float]] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            return ()
        if isinstance(item[0], bool) or isinstance(item[1], bool):
            return ()
        try:
            price = float(item[0])
            quantity = float(item[1])
        except (TypeError, ValueError, OverflowError):
            return ()
        if not (isfinite(price) and isfinite(quantity) and price > 0.0 and quantity > 0.0):
            return ()
        levels.append((price, quantity))
    return tuple(levels)


def _parse_degraded_symbols(raw: object) -> dict[str, list[str]]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, list[str]] = {}
    for k, v in raw.items():
        if isinstance(v, list):
            result[str(k)] = [str(x) for x in v]
    return result
