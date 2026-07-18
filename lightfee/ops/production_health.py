from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from lightfee.engine.recovery_decision_core import (
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.recovery_owner_index import RecoveryOwnerIndex
from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table
from lightfee.ops.position_side_semantics import side_matches_business_leg
from lightfee.sidecar.snapshot import (
    SNAPSHOT_SCHEMA_VERSION,
    validate_v4_snapshot_contract,
    validate_v5_snapshot_contract,
)


EXPECTED_VENUES = {"aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"}
KNOWN_GOOD_RESOLVERS = {"1.1.1.1", "1.0.0.1", "8.8.8.8", "8.8.4.4", "9.9.9.9"}
FIXTURE_MARKET_OBSERVED_AT_MS = 1710000075000
RUNTIME_TIMESTAMP_MAX_FUTURE_SKEW_MS = 1_000


@dataclass(frozen=True)
class HealthReport:
    name: str
    ok: bool
    severity: str = "info"
    fingerprints: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HealthSummary:
    ok: bool
    critical_count: int
    warning_count: int
    reports: list[HealthReport]


def _execstart_lines(unit_text: str) -> list[str]:
    return [
        line.split("=", 1)[1].strip()
        for line in unit_text.splitlines()
        if line.strip().startswith("ExecStart=")
    ]


def _has_environment_file(unit_text: str) -> bool:
    return any(line.strip().startswith("EnvironmentFile=") for line in unit_text.splitlines())


def _has_limit_nofile(unit_text: str) -> bool:
    for line in unit_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("LimitNOFILE="):
            continue
        value = stripped.split("=", 1)[1].strip()
        try:
            return int(value) >= 65536
        except ValueError:
            return value.lower() in {"infinity", "infinite", "unlimited"}
    return False


def _has_requires_unit(unit_text: str, unit_name: str) -> bool:
    for line in unit_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("Requires="):
            continue
        required_units = stripped.split("=", 1)[1].split()
        if unit_name in required_units:
            return True
    return False


def analyze_systemd_unit(name: str, text: str) -> HealthReport:
    fingerprints: list[str] = []
    details: dict[str, Any] = {"service": name}
    execs = _execstart_lines(text)
    details["execstart"] = execs
    is_sidecar = "sidecar" in name
    is_live = name.endswith("lightfee-live.service") or "live" in name

    if not execs:
        fingerprints.append("missing_execstart")
    command = " ".join(execs)
    if (is_sidecar or is_live) and "--config" not in command:
        fingerprints.append("missing_explicit_config")
    if "config/example.toml" in command or command.endswith("example.toml"):
        fingerprints.append("example_config_in_production")
    if is_sidecar and not _has_environment_file(text):
        fingerprints.append("missing_environment_file")
    if (is_sidecar or is_live) and not _has_limit_nofile(text):
        fingerprints.append("missing_limit_nofile")
    if is_live and _has_requires_unit(text, "lightfee-sidecar.service"):
        fingerprints.append("live_requires_sidecar_service")
    if is_sidecar and "opportunity_input_sidecar" in command and "live.auto.toml" not in command:
        fingerprints.append("rust_sidecar_without_live_auto_config")

    return HealthReport(
        name=f"systemd:{name}",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details=details,
    )


def _quote_venues(snapshot: dict[str, Any]) -> set[str]:
    venues: set[str] = set()
    quotes = snapshot.get("quotes", {})
    if isinstance(quotes, dict):
        for key, raw in quotes.items():
            if isinstance(raw, dict) and raw.get("venue"):
                venues.add(str(raw["venue"]).lower())
            elif isinstance(key, str) and ":" in key:
                venues.add(key.split(":", 1)[0].lower())
    return venues


def _fixture_quote_count(snapshot: dict[str, Any]) -> int:
    count = 0
    quotes = snapshot.get("quotes", {})
    if isinstance(quotes, dict):
        for raw in quotes.values():
            if not isinstance(raw, dict):
                continue
            try:
                if float(raw.get("bid", 0)) == 100.0 and float(raw.get("ask", 0)) == 100.0:
                    count += 1
            except (TypeError, ValueError):
                continue
    return count


def analyze_sidecar_snapshot(
    snapshot: Any,
    *,
    now_ms: int,
    max_age_ms: int,
) -> HealthReport:
    if not isinstance(snapshot, dict):
        return HealthReport(
            name="sidecar_snapshot",
            ok=False,
            severity="critical",
            fingerprints=["sidecar_snapshot_root_invalid"],
            details={"root_type": type(snapshot).__name__},
        )
    fingerprints: list[str] = []
    raw_schema_version = snapshot.get("schema_version")
    if raw_schema_version == SNAPSHOT_SCHEMA_VERSION:
        shared_contract_errors = validate_v5_snapshot_contract(snapshot)
    elif raw_schema_version == 4:
        shared_contract_errors = validate_v4_snapshot_contract(snapshot)
    else:
        shared_contract_errors = ["schema_version_unsupported"]
    required_snapshot_fields = {
        "schema_version",
        "published_at_ms",
        "market_observed_at_ms",
        "candidate_build_observed_at_ms",
        "candidate_build_diagnostics",
        "degraded_venues",
        "degraded_domains",
        "degraded_symbols",
        "quotes",
        "candidates",
    }
    raw_published_at_ms = snapshot.get("published_at_ms")
    raw_market_observed_at_ms = snapshot.get("market_observed_at_ms")
    raw_candidate_build_at_ms = snapshot.get("candidate_build_observed_at_ms")
    strict_timestamp_values = (
        raw_published_at_ms,
        raw_market_observed_at_ms,
        raw_candidate_build_at_ms,
    )
    invalid_timestamp_contract = any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in strict_timestamp_values
    )
    published_at_ms = _safe_int(raw_published_at_ms)
    market_observed_at_ms = _safe_int(raw_market_observed_at_ms)
    candidate_build_observed_at_ms = _safe_int(raw_candidate_build_at_ms)
    observed = market_observed_at_ms or published_at_ms
    age_ms = now_ms - observed if observed else None
    venues = _quote_venues(snapshot)
    raw_quotes = snapshot.get("quotes", {})
    interval_quote_counts: dict[str, int] = {}
    interval_known_counts: dict[str, int] = {}
    interval_missing_quote_keys: list[str] = []
    if isinstance(raw_quotes, dict):
        for quote_key, raw_quote in raw_quotes.items():
            if not isinstance(raw_quote, dict):
                continue
            venue = str(raw_quote.get("venue", "") or "").strip().lower()
            if not venue and isinstance(quote_key, str) and ":" in quote_key:
                venue = quote_key.split(":", 1)[0].strip().lower()
            if not venue:
                continue
            interval_quote_counts[venue] = interval_quote_counts.get(venue, 0) + 1
            interval_known_counts.setdefault(venue, 0)
            interval_ms = raw_quote.get("funding_interval_ms")
            if type(interval_ms) is int and interval_ms > 0:
                interval_known_counts[venue] = interval_known_counts.get(venue, 0) + 1
            else:
                interval_missing_quote_keys.append(str(quote_key))
    missing = sorted(EXPECTED_VENUES - venues)
    unexpected = sorted(venues - EXPECTED_VENUES)
    fixture_quotes = _fixture_quote_count(snapshot)
    candidate_diagnostics = snapshot.get("candidate_build_diagnostics", {})
    if not isinstance(candidate_diagnostics, dict):
        candidate_diagnostics = {}
    requested_venues_raw = candidate_diagnostics.get("requested_venues", [])
    requested_venue_set = {
        venue
        for venue in requested_venues_raw
        if isinstance(venue, str)
    } if isinstance(requested_venues_raw, list) else set()
    rejection_counts = candidate_diagnostics.get("rejection_counts", {})
    if not isinstance(rejection_counts, dict):
        rejection_counts = {}
    required_diagnostic_counts = {
        "input_quote_count",
        "requested_symbol_count",
        "directional_pair_count",
        "output_candidate_count",
        "future_input_quote_count",
    }
    required_diagnostic_fields = required_diagnostic_counts | {
        "requested_symbols",
        "requested_venues",
    }
    missing_contract_fields = sorted(required_snapshot_fields - snapshot.keys())
    missing_diagnostic_fields = sorted(
        required_diagnostic_fields - candidate_diagnostics.keys()
    )
    invalid_diagnostics = (
        not isinstance(snapshot.get("candidate_build_diagnostics"), dict)
        or any(
            not isinstance(candidate_diagnostics.get(field), int)
            or isinstance(candidate_diagnostics.get(field), bool)
            or int(candidate_diagnostics.get(field, -1)) < 0
            for field in required_diagnostic_counts
        )
        or not isinstance(candidate_diagnostics.get("rejection_counts"), dict)
        or not isinstance(candidate_diagnostics.get("requested_symbols"), list)
        or not isinstance(candidate_diagnostics.get("requested_venues"), list)
        or any(
            not isinstance(reason, str)
            or not isinstance(count, int)
            or isinstance(count, bool)
            or count < 0
            for reason, count in rejection_counts.items()
        )
    )
    future_quote_rejections = _safe_int(rejection_counts.get("quote_after_candidate_watermark", 0))
    directional_pair_count = _safe_int(candidate_diagnostics.get("directional_pair_count", 0))
    output_candidate_count = _safe_int(candidate_diagnostics.get("output_candidate_count", 0))
    future_input_quote_count = _safe_int(candidate_diagnostics.get("future_input_quote_count", 0))
    raw_candidates = snapshot.get("candidates", [])
    candidate_count = len(raw_candidates) if isinstance(raw_candidates, list) else 0
    blocked_candidate_count = 0
    declared_blocked_candidate_count = 0
    blocked_reason_counts: dict[str, int] = {}
    if isinstance(raw_candidates, list):
        for candidate in raw_candidates:
            reasons: list[str] = []
            if not isinstance(candidate, dict):
                reasons = ["invalid_candidate_shape"]
            else:
                if candidate.get("blocked") is True:
                    declared_blocked_candidate_count += 1
                raw_reasons = candidate.get("blocked_reasons", [])
                if isinstance(raw_reasons, list):
                    reasons.extend(
                        str(reason)
                        for reason in raw_reasons
                        if isinstance(reason, str) and reason.strip()
                    )
                if candidate.get("economics_complete") is not True:
                    reasons.append(
                        str(candidate.get("economics_incomplete_reason", "") or "").strip()
                        or "incomplete_v3_economics"
                    )
                observed_at_ms = candidate.get("economics_observed_at_ms")
                if (
                    not isinstance(observed_at_ms, int)
                    or isinstance(observed_at_ms, bool)
                    or observed_at_ms <= 0
                ):
                    reasons.append("economics_observation_missing")
                if candidate.get("blocked") is True and not reasons:
                    reasons.append("blocked_without_reason")
            if not reasons:
                continue
            blocked_candidate_count += 1
            for reason in set(reasons):
                blocked_reason_counts[reason] = blocked_reason_counts.get(reason, 0) + 1
    unblocked_candidate_count = max(candidate_count - blocked_candidate_count, 0)
    if (
        raw_schema_version not in {SNAPSHOT_SCHEMA_VERSION, 4}
        or missing_contract_fields
        or missing_diagnostic_fields
    ):
        fingerprints.append("sidecar_diagnostics_contract_missing")
    if (
        invalid_timestamp_contract
        or invalid_diagnostics
        or not isinstance(snapshot.get("degraded_venues"), list)
        or not isinstance(snapshot.get("degraded_domains"), list)
        or not isinstance(snapshot.get("degraded_symbols"), dict)
        or not isinstance(snapshot.get("quotes"), dict)
        or not isinstance(snapshot.get("candidates"), list)
    ):
        fingerprints.append("sidecar_diagnostics_contract_invalid")
    if shared_contract_errors:
        if any(
            error.startswith("missing:") or error == "schema_version_unsupported"
            for error in shared_contract_errors
        ):
            if "sidecar_diagnostics_contract_missing" not in fingerprints:
                fingerprints.append("sidecar_diagnostics_contract_missing")
        if "sidecar_diagnostics_contract_invalid" not in fingerprints:
            fingerprints.append("sidecar_diagnostics_contract_invalid")

    if observed == FIXTURE_MARKET_OBSERVED_AT_MS:
        fingerprints.append("fixture_timestamp")
    if age_ms is None or age_ms < 0 or age_ms > max_age_ms:
        fingerprints.append("snapshot_stale_or_missing_timestamp")
    if (
        published_at_ms <= 0
        or market_observed_at_ms <= 0
        or candidate_build_observed_at_ms <= 0
        or market_observed_at_ms > candidate_build_observed_at_ms
        or candidate_build_observed_at_ms > published_at_ms
        or published_at_ms > now_ms
    ):
        fingerprints.append("sidecar_snapshot_watermark_invalid")
    if missing:
        fingerprints.append("quote_venue_count_lt_7")
    if unexpected:
        fingerprints.append("quote_venue_set_unexpected")
    if requested_venue_set != EXPECTED_VENUES:
        fingerprints.append("requested_venue_set_mismatch")
    if fixture_quotes >= 2:
        fingerprints.append("fixture_100_quotes")
    if future_quote_rejections > 0 or future_input_quote_count > 0:
        fingerprints.append("candidate_build_watermark_rejected_quotes")
    invalid_trade_quote_count = _safe_int(
        rejection_counts.get("invalid_trade_quote", 0)
    )
    if invalid_trade_quote_count > 0 and unblocked_candidate_count <= 0:
        fingerprints.append("sidecar_market_quality_rejected_quotes")
    degraded_venues = snapshot.get("degraded_venues", [])
    degraded_domains = snapshot.get("degraded_domains", [])
    degraded_symbols = snapshot.get("degraded_symbols", {})
    degraded_symbol_items = (
        degraded_symbols.items() if isinstance(degraded_symbols, dict) else ()
    )
    degraded_symbol_keys = {
        f"{str(venue).strip().lower()}:{str(symbol).strip().upper()}"
        for venue, symbols in degraded_symbol_items
        for symbol in (symbols if isinstance(symbols, list) else [])
        if isinstance(venue, str)
        and isinstance(symbol, str)
    }
    unscoped_interval_missing_quote_keys = sorted(
        quote_key
        for quote_key in interval_missing_quote_keys
        if quote_key not in degraded_symbol_keys
    )
    if unscoped_interval_missing_quote_keys:
        # An unknown cadence is not safely substitutable with a venue-wide
        # 8-hour assumption: it changes annualisation, aligned/staggered
        # lifecycle semantics, and the first-settlement gate.  Surface the
        # liveness failure by venue instead of leaving operators with a generic
        # zero-candidate symptom.
        fingerprints.append("funding_interval_evidence_incomplete")
    acquisition_mode = str(snapshot.get("acquisition_mode", "") or "")
    has_degraded_venues = isinstance(degraded_venues, list) and bool(
        degraded_venues
    )
    has_degraded_domains = isinstance(degraded_domains, list) and bool(
        degraded_domains
    )
    symbol_degradation_has_no_usable_candidate = bool(
        degraded_symbol_keys and unblocked_candidate_count <= 0
    )
    has_global_degradation = bool(
        has_degraded_venues
        or has_degraded_domains
        or acquisition_mode in {"last_good_sidecar", "unavailable"}
        or (acquisition_mode == "degraded_sidecar" and not degraded_symbol_keys)
        or symbol_degradation_has_no_usable_candidate
    )
    if has_global_degradation:
        fingerprints.append("sidecar_snapshot_degraded")
    if acquisition_mode == "unavailable":
        fingerprints.append("sidecar_snapshot_unavailable")
    if candidate_diagnostics and candidate_build_observed_at_ms <= 0:
        fingerprints.append("candidate_build_watermark_invalid")
    if (
        candidate_diagnostics
        and directional_pair_count > 0
        and output_candidate_count == 0
        and not rejection_counts
    ):
        fingerprints.append("candidate_builder_unexplained_zero_output")
    if (
        directional_pair_count > 0
        and candidate_count > 0
        and unblocked_candidate_count == 0
    ):
        fingerprints.append("funding_entry_readiness_no_unblocked_candidates")

    return HealthReport(
        name="sidecar_snapshot",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "observed_at_ms": observed,
            "age_ms": age_ms,
            "quote_venues": sorted(venues),
            "missing_venues": missing,
            "unexpected_venues": unexpected,
            "requested_venues": sorted(requested_venue_set),
            "fixture_quote_count": fixture_quotes,
            "funding_interval_quote_counts_by_venue": dict(
                sorted(interval_quote_counts.items())
            ),
            "funding_interval_known_counts_by_venue": dict(
                sorted(interval_known_counts.items())
            ),
            "funding_interval_missing_quote_keys": sorted(
                interval_missing_quote_keys
            ),
            "funding_interval_unscoped_missing_quote_keys": (
                unscoped_interval_missing_quote_keys
            ),
            "degraded_venues": list(degraded_venues)
            if isinstance(degraded_venues, list)
            else [],
            "degraded_domains": list(degraded_domains)
            if isinstance(degraded_domains, list)
            else [],
            "degraded_symbols": degraded_symbols
            if isinstance(degraded_symbols, dict)
            else {},
            "acquisition_mode": acquisition_mode,
            "candidate_build_observed_at_ms": candidate_build_observed_at_ms,
            "candidate_build_diagnostics": candidate_diagnostics,
            "candidate_count": candidate_count,
            "declared_blocked_candidate_count": declared_blocked_candidate_count,
            "blocked_candidate_count": blocked_candidate_count,
            "unblocked_candidate_count": unblocked_candidate_count,
            "blocked_reason_counts": dict(sorted(blocked_reason_counts.items())),
            "funding_entry_ready": (
                unblocked_candidate_count > 0 and not shared_contract_errors
            ),
            "contract_errors": shared_contract_errors,
        },
    )


def analyze_spread_snapshot(
    snapshot: Any,
    *,
    now_ms: int,
    max_age_ms: int,
) -> HealthReport:
    if not isinstance(snapshot, dict):
        return HealthReport(
            name="spread_snapshot",
            ok=False,
            severity="critical",
            fingerprints=["spread_snapshot_root_invalid"],
            details={"root_type": type(snapshot).__name__},
        )
    fingerprints: list[str] = []
    schema_version = _safe_int(snapshot.get("schema_version", 0))
    required_proof_fields = {
        "schema_version",
        "decision_at_ms",
        "published_at_ms",
        "market_observed_at_ms",
        "source_mode",
        "degraded_venues",
        "degraded_symbols",
        "input_quote_count",
        "valid_quote_count",
        "evaluated_pair_count",
        "accepted_pair_count",
        "paper_configured_enabled",
        "paper_admission_enabled",
        "paper_tracked_count",
        "paper_refresh_status",
        "paper_event_count",
        "paper_last_success_at_ms",
        "rejection_counts",
        "paper_admission_rejection_counts",
        "candidates",
    }
    missing_proof_fields = sorted(required_proof_fields - snapshot.keys())
    if schema_version != 4 or missing_proof_fields:
        fingerprints.append("spread_diagnostics_contract_missing")
    decision_at_ms = _safe_int(snapshot.get("decision_at_ms", 0))
    published_at_ms = _safe_int(snapshot.get("published_at_ms", 0))
    market_observed_at_ms = _safe_int(snapshot.get("market_observed_at_ms", 0))
    publish_age_ms = now_ms - published_at_ms if published_at_ms > 0 else None
    market_age_ms = now_ms - market_observed_at_ms if market_observed_at_ms > 0 else None
    source_mode = str(snapshot.get("source_mode", "") or "")
    raw_counts = {
        "schema_version": snapshot.get("schema_version", 0),
        "decision_at_ms": snapshot.get("decision_at_ms", 0),
        "published_at_ms": snapshot.get("published_at_ms", 0),
        "market_observed_at_ms": snapshot.get("market_observed_at_ms", 0),
        "input_quote_count": snapshot.get("input_quote_count", 0),
        "valid_quote_count": snapshot.get("valid_quote_count", 0),
        "evaluated_pair_count": snapshot.get("evaluated_pair_count", 0),
        "accepted_pair_count": snapshot.get("accepted_pair_count", 0),
        "paper_event_count": snapshot.get("paper_event_count", 0),
        "paper_tracked_count": snapshot.get("paper_tracked_count", 0),
        "paper_last_success_at_ms": snapshot.get("paper_last_success_at_ms", 0),
    }
    invalid_count_fields = sorted(
        field_name
        for field_name, value in raw_counts.items()
        if (not isinstance(value, int) or isinstance(value, bool) or value < 0)
    )
    input_quote_count = _safe_int(raw_counts["input_quote_count"])
    valid_quote_count = _safe_int(raw_counts["valid_quote_count"])
    evaluated_pair_count = _safe_int(raw_counts["evaluated_pair_count"])
    accepted_pair_count = _safe_int(raw_counts["accepted_pair_count"])
    paper_event_count = _safe_int(raw_counts["paper_event_count"])
    paper_tracked_count = _safe_int(raw_counts["paper_tracked_count"])
    rejection_counts = snapshot.get("rejection_counts", {})
    if not isinstance(rejection_counts, dict):
        rejection_counts = {}
    paper_admission_rejection_counts = snapshot.get(
        "paper_admission_rejection_counts", {}
    )
    if not isinstance(paper_admission_rejection_counts, dict):
        paper_admission_rejection_counts = {}
    candidates = snapshot.get("candidates", [])
    candidate_count = len(candidates) if isinstance(candidates, list) else 0
    paper_configured_enabled = snapshot.get("paper_configured_enabled") is True
    paper_admission_enabled = snapshot.get("paper_admission_enabled") is True
    paper_refresh_status = str(snapshot.get("paper_refresh_status", "") or "")
    paper_last_success_at_ms = _safe_int(snapshot.get("paper_last_success_at_ms", 0))
    rejection_total = sum(max(_safe_int(value), 0) for value in rejection_counts.values())
    invalid_rejection_counts = any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in rejection_counts.values()
    )
    invalid_paper_rejection_counts = any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in paper_admission_rejection_counts.values()
    )

    if (
        invalid_count_fields
        or invalid_rejection_counts
        or invalid_paper_rejection_counts
        or not isinstance(snapshot.get("candidates", []), list)
        or not isinstance(snapshot.get("degraded_venues", []), list)
        or not isinstance(snapshot.get("degraded_symbols", {}), dict)
        or any(not isinstance(venue, str) for venue in snapshot.get("degraded_venues", []))
        or any(
            not isinstance(venue, str)
            or not isinstance(symbols, list)
            or any(not isinstance(symbol, str) for symbol in symbols)
            for venue, symbols in snapshot.get("degraded_symbols", {}).items()
        )
        or not isinstance(snapshot.get("rejection_counts", {}), dict)
        or not isinstance(snapshot.get("paper_admission_rejection_counts", {}), dict)
        or not isinstance(snapshot.get("source_mode"), str)
        or not str(snapshot.get("source_mode", "") or "")
        or not isinstance(snapshot.get("paper_configured_enabled"), bool)
        or not isinstance(snapshot.get("paper_admission_enabled"), bool)
        or decision_at_ms <= 0
    ):
        fingerprints.append("spread_diagnostics_contract_invalid")
    if (
        valid_quote_count > input_quote_count
        or accepted_pair_count > evaluated_pair_count
        or candidate_count > accepted_pair_count
        or (not paper_configured_enabled and paper_admission_enabled)
    ):
        fingerprints.append("spread_diagnostics_count_invariant_invalid")
    if not paper_configured_enabled and paper_refresh_status != "disabled":
        fingerprints.append("spread_diagnostics_contract_invalid")

    if (
        publish_age_ms is None
        or publish_age_ms < 0
        or publish_age_ms > max_age_ms
        or market_age_ms is None
        or market_age_ms < 0
        or market_age_ms > max_age_ms
    ):
        fingerprints.append("spread_snapshot_stale")
    if (
        decision_at_ms <= 0
        or market_observed_at_ms > decision_at_ms
        or published_at_ms <= 0
        or decision_at_ms > published_at_ms
    ):
        fingerprints.append("spread_publication_watermark_invalid")
    allowed_source_modes = {
        "sidecar_snapshot",
        "sidecar_snapshot_partial",
        "sidecar_snapshot_unavailable",
        "sidecar_snapshot_stale",
        "sidecar_snapshot_quotes_stale",
        "sidecar_snapshot_degraded",
    }
    if source_mode not in allowed_source_modes:
        fingerprints.append("spread_source_mode_unknown")
    if source_mode in {
        "sidecar_snapshot_unavailable",
        "sidecar_snapshot_stale",
        "sidecar_snapshot_quotes_stale",
        "sidecar_snapshot_degraded",
    }:
        fingerprints.append(f"spread_source_{source_mode}")
    degraded_venues = snapshot.get("degraded_venues", [])
    degraded_symbols = snapshot.get("degraded_symbols", {})
    degraded_symbol_venues = (
        {
            str(venue).strip().lower()
            for venue, symbols in degraded_symbols.items()
            if isinstance(symbols, list) and symbols
        }
        if isinstance(degraded_symbols, dict)
        else set()
    )
    scoped_symbol_partial = bool(
        source_mode == "sidecar_snapshot_partial"
        and not degraded_venues
        and degraded_symbol_venues
        and degraded_symbol_venues <= EXPECTED_VENUES
        and valid_quote_count > 0
    )
    if source_mode == "sidecar_snapshot_partial" and not scoped_symbol_partial:
        fingerprints.append("spread_source_sidecar_snapshot_partial")
    if (isinstance(degraded_venues, list) and degraded_venues) or (
        isinstance(degraded_symbols, dict)
        and any(bool(symbols) for symbols in degraded_symbols.values())
        and not scoped_symbol_partial
    ):
        fingerprints.append("spread_degraded_inputs")
    if input_quote_count > 0 and valid_quote_count == 0 and not rejection_counts:
        fingerprints.append("spread_input_pipeline_stalled")
    if valid_quote_count > 1 and evaluated_pair_count == 0 and not rejection_counts:
        fingerprints.append("spread_candidate_pipeline_unexplained_zero")
    if accepted_pair_count + rejection_total != evaluated_pair_count:
        fingerprints.append("spread_pair_attribution_incomplete")
    if paper_configured_enabled and not paper_admission_enabled:
        fingerprints.append("spread_paper_admission_disabled")
    if (
        paper_configured_enabled
        and accepted_pair_count > 0
        and paper_tracked_count == 0
        and not paper_admission_rejection_counts
    ):
        fingerprints.append("spread_paper_admission_unexplained_zero")
    if paper_configured_enabled and (
        paper_refresh_status != "success"
        or paper_last_success_at_ms <= 0
        or paper_last_success_at_ms < decision_at_ms
        or paper_last_success_at_ms > published_at_ms
        or now_ms - paper_last_success_at_ms < 0
        or now_ms - paper_last_success_at_ms > max_age_ms
    ):
        fingerprints.append("spread_paper_refresh_not_proven")
    if not paper_configured_enabled and (
        paper_admission_enabled
        or paper_refresh_status != "disabled"
        or paper_tracked_count != 0
        or paper_event_count != 0
        or paper_last_success_at_ms != 0
    ):
        fingerprints.append("spread_paper_disabled_state_invalid")

    safe_degraded_venues = (
        [str(venue) for venue in degraded_venues] if isinstance(degraded_venues, list) else []
    )
    safe_degraded_symbols = (
        {
            str(venue): [str(symbol) for symbol in symbols]
            for venue, symbols in degraded_symbols.items()
            if isinstance(symbols, list)
        }
        if isinstance(degraded_symbols, dict)
        else {}
    )

    warning_fingerprints = {
        "spread_source_sidecar_snapshot_partial",
        "spread_degraded_inputs",
    }
    severity = (
        "critical"
        if any(fingerprint not in warning_fingerprints for fingerprint in fingerprints)
        else "warning"
    )
    return HealthReport(
        name="spread_snapshot",
        ok=not fingerprints,
        severity=severity if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "schema_version": schema_version,
            "missing_proof_fields": missing_proof_fields,
            "invalid_count_fields": invalid_count_fields,
            "decision_at_ms": decision_at_ms,
            "published_at_ms": published_at_ms,
            "publish_age_ms": publish_age_ms,
            "market_observed_at_ms": market_observed_at_ms,
            "market_age_ms": market_age_ms,
            "source_mode": source_mode,
            "input_quote_count": input_quote_count,
            "valid_quote_count": valid_quote_count,
            "candidate_count": candidate_count,
            "evaluated_pair_count": evaluated_pair_count,
            "accepted_pair_count": accepted_pair_count,
            "rejection_total": rejection_total,
            "paper_configured_enabled": paper_configured_enabled,
            "paper_admission_enabled": paper_admission_enabled,
            "paper_tracked_count": paper_tracked_count,
            "paper_refresh_status": paper_refresh_status,
            "paper_event_count": paper_event_count,
            "paper_last_success_at_ms": paper_last_success_at_ms,
            "rejection_counts": rejection_counts,
            "paper_admission_rejection_counts": paper_admission_rejection_counts,
            "degraded_venues": safe_degraded_venues,
            "degraded_symbols": safe_degraded_symbols,
        },
    )


def analyze_strategy_entry_policy(
    strategy: Any,
    *,
    runtime_mode: str,
    require_entry_enabled: bool = False,
) -> HealthReport:
    funding_entries_enabled = getattr(strategy, "funding_new_entries_enabled", None) is True
    funding_canary_enabled = getattr(strategy, "funding_canary_enabled", None) is True
    spread_reversion_enabled = getattr(strategy, "spread_reversion_enabled", None) is True
    spread_paper_enabled = getattr(strategy, "spread_paper_enabled", None) is True
    spread_live_enabled = getattr(strategy, "spread_live_enabled", None) is True
    funding_live_entry_ready = (
        str(runtime_mode or "").lower() == "live"
        and funding_entries_enabled
        and funding_canary_enabled
    )
    fingerprints: list[str] = []
    if spread_live_enabled:
        fingerprints.append("spread_live_must_remain_disabled")
    if require_entry_enabled and not funding_live_entry_ready:
        fingerprints.append("funding_live_entry_disabled")

    return HealthReport(
        name="strategy_entry_policy",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "runtime_mode": str(runtime_mode or ""),
            "funding_new_entries_enabled": funding_entries_enabled,
            "funding_canary_enabled": funding_canary_enabled,
            "funding_live_entry_ready": funding_live_entry_ready,
            "spread_reversion_enabled": spread_reversion_enabled,
            "spread_paper_enabled": spread_paper_enabled,
            "spread_paper_ready": spread_reversion_enabled and spread_paper_enabled,
            "spread_live_enabled": spread_live_enabled,
            "spread_execution_contract": "paper_only",
        },
    )


def _safe_abs_quantity(value: Any) -> float:
    try:
        return abs(float(value or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _live_position_details(exchange_truth: dict[str, Any]) -> list[dict[str, Any]]:
    live: list[dict[str, Any]] = []
    for venue, positions in (exchange_truth.get("positions") or {}).items():
        if not isinstance(positions, dict):
            continue
        for symbol, pos in positions.items():
            if not isinstance(pos, dict):
                continue
            qty = _safe_abs_quantity(pos.get("quantity"))
            if qty <= 1e-9:
                continue
            live.append(
                {
                    "venue": str(pos.get("venue") or venue).lower(),
                    "symbol": str(pos.get("symbol") or symbol).upper(),
                    "side": str(pos.get("side") or "").lower(),
                    "quantity": qty,
                    "entry_price": pos.get("entry_price"),
                }
            )
    return live


def _live_open_order_details(exchange_truth: dict[str, Any]) -> list[dict[str, Any]]:
    live: list[dict[str, Any]] = []
    for venue, orders_by_symbol in (exchange_truth.get("open_orders") or {}).items():
        if not isinstance(orders_by_symbol, dict):
            continue
        for symbol, rows in orders_by_symbol.items():
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                qty = _safe_abs_quantity(row.get("quantity") or row.get("qty"))
                if qty <= 1e-9:
                    continue
                live.append(
                    {
                        "venue": str(row.get("venue") or venue).lower(),
                        "symbol": str(row.get("symbol") or symbol).upper(),
                        "side": str(row.get("side") or "").lower(),
                        "quantity": qty,
                        "price": row.get("price"),
                        "reduce_only": bool(row.get("reduce_only", False)),
                        "order_id": row.get("order_id"),
                    }
                )
    return live


def _local_expected_legs(state: dict[str, Any]) -> list[dict[str, Any]]:
    legs: list[dict[str, Any]] = []
    for pos in state.get("open_positions", []) or state.get("positions", []) or []:
        if not isinstance(pos, dict):
            continue
        symbol = str(pos.get("symbol") or "").upper()
        qty = _safe_abs_quantity(pos.get("quantity") or pos.get("matched_quantity"))
        if not symbol or qty <= 1e-9:
            continue
        long_venue = str(pos.get("long_venue") or "").lower()
        short_venue = str(pos.get("short_venue") or "").lower()
        if long_venue:
            legs.append(
                {
                    "venue": long_venue,
                    "symbol": symbol,
                    "expected_side": "long",
                    "expected_quantity": qty,
                    "position_id": pos.get("position_id"),
                }
            )
        if short_venue:
            legs.append(
                {
                    "venue": short_venue,
                    "symbol": symbol,
                    "expected_side": "short",
                    "expected_quantity": qty,
                    "position_id": pos.get("position_id"),
                }
            )
    return legs


def _side_matches(actual: str, expected: str) -> bool:
    return side_matches_business_leg(actual, expected)


def _exchange_truth_position_mismatches(
    state: dict[str, Any], exchange_truth: dict[str, Any]
) -> list[dict[str, Any]]:
    live_positions = _live_position_details(exchange_truth)
    live_by_key = {(p["venue"], p["symbol"]): p for p in live_positions}
    expected_legs = _local_expected_legs(state)
    expected_keys = {(leg["venue"], leg["symbol"]) for leg in expected_legs}
    mismatches: list[dict[str, Any]] = []

    for leg in expected_legs:
        key = (leg["venue"], leg["symbol"])
        live = live_by_key.get(key)
        live_qty = float(live.get("quantity", 0.0)) if live else 0.0
        live_side = str(live.get("side", "")) if live else ""
        expected_qty = float(leg["expected_quantity"])
        if abs(live_qty - expected_qty) > 1e-9 or not _side_matches(
            live_side, str(leg["expected_side"])
        ):
            mismatches.append(
                {
                    "check": "local_live_leg_missing_or_quantity_mismatch",
                    **leg,
                    "live_quantity": live_qty,
                    "live_side": live_side,
                }
            )

    if expected_legs:
        for live in live_positions:
            key = (live["venue"], live["symbol"])
            if key not in expected_keys:
                mismatches.append(
                    {
                        "check": "unexpected_live_position",
                        **live,
                    }
                )

    return mismatches


def _pending_entry_live_conflict_summary(
    state: dict[str, Any],
    exchange_truth: dict[str, Any],
) -> dict[str, Any]:
    live_by_key = {(p["venue"], p["symbol"]): p for p in _live_position_details(exchange_truth)}
    open_orders = {(o["venue"], o["symbol"]): [] for o in _live_open_order_details(exchange_truth)}
    for order in _live_open_order_details(exchange_truth):
        open_orders.setdefault((order["venue"], order["symbol"]), []).append(order)

    details: list[dict[str, Any]] = []
    for pending in state.get("pending_entries", []) or []:
        if not isinstance(pending, dict):
            continue
        symbol = str(pending.get("symbol") or "").upper()
        maker_fill = _safe_abs_quantity(pending.get("maker_leg_filled"))
        hedge_fill = _safe_abs_quantity(pending.get("hedge_leg_filled"))
        if not symbol or (maker_fill <= 1e-9 and hedge_fill <= 1e-9):
            continue
        maker_leg = _maker_leg_text(pending.get("maker_leg") or pending.get("maker_side"))
        if maker_leg not in {"long", "short"}:
            continue
        legs = []
        if maker_leg == "long":
            long_qty, short_qty = maker_fill, hedge_fill
        else:
            long_qty, short_qty = hedge_fill, maker_fill
        for venue, expected_side, expected_qty in (
            (str(pending.get("long_venue") or "").lower(), "long", long_qty),
            (str(pending.get("short_venue") or "").lower(), "short", short_qty),
        ):
            if not venue or expected_qty <= 1e-9:
                continue
            live = live_by_key.get((venue, symbol), {})
            live_qty = _safe_abs_quantity(live.get("quantity"))
            live_side = str(live.get("side") or "").lower()
            live_matches = (
                live_qty > 1e-9
                and abs(live_qty - expected_qty) <= 1e-9
                and _side_matches(live_side, expected_side)
            )
            legs.append(
                {
                    "venue": venue,
                    "symbol": symbol,
                    "expected_side": expected_side,
                    "expected_quantity": expected_qty,
                    "live_quantity": live_qty,
                    "live_side": live_side,
                    "live_position_confirmed": live_matches,
                    "open_orders": open_orders.get((venue, symbol), []),
                    "owner": "pending_entry",
                }
            )
        if legs:
            conflict_reasons: list[str] = []
            for leg in legs:
                venue = str(leg.get("venue") or "")
                if _safe_abs_quantity(leg.get("live_quantity")) <= 1e-9:
                    conflict_reasons.append(
                        f"{venue} fill evidence conflicts with {venue} live flat"
                    )
                elif not bool(leg.get("live_position_confirmed")):
                    conflict_reasons.append(
                        f"{venue} fill evidence conflicts with {venue} live mismatch"
                    )
            if any(_safe_abs_quantity(leg.get("live_quantity")) > 1e-9 for leg in legs):
                conflict_reasons.append("live position owned by pending conflict")
            details.append(
                {
                    "pending_id": str(
                        pending.get("pending_id") or pending.get("position_id") or symbol
                    ),
                    "symbol": symbol,
                    "maker_leg": str(pending.get("maker_leg") or ""),
                    "maker_leg_filled": maker_fill,
                    "hedge_leg_filled": hedge_fill,
                    "legs": legs,
                    "conflict_reasons": sorted(set(conflict_reasons)),
                    "next_action": "owned_pending_entry_live_conflict_cleanup",
                }
            )
    return {"count": len(details), "details": details}


def _maker_leg_text(value: Any) -> str:
    if hasattr(value, "value"):
        value = value.value
    text = str(value or "").lower()
    if text == "buy":
        return "long"
    if text == "sell":
        return "short"
    return text


def _runtime_progress_summary(
    state: dict[str, Any],
    *,
    now_ms: int,
    progress_budget_ms: int,
    last_scan_age_ms: int | None,
    current_state_age_ms: int | None,
) -> tuple[str, bool, bool, dict[str, Any]]:
    raw_progress = state.get("runtime_progress")
    runtime_progress = dict(raw_progress) if isinstance(raw_progress, dict) else {}
    last_lane_progress_ms = _safe_int(runtime_progress.get("last_lane_progress_ms"))
    active_lane = str(runtime_progress.get("active_lane") or "")
    active_lane_started_ms = _safe_int(runtime_progress.get("active_lane_started_ms"))
    active_lane_budget_ms = _safe_int(runtime_progress.get("active_lane_budget_ms"))
    loop_iteration_started_ms = _safe_int(runtime_progress.get("loop_iteration_started_ms"))
    loop_iteration_completed_ms = _safe_int(runtime_progress.get("loop_iteration_completed_ms"))

    last_lane_progress_age_ms = (
        now_ms - last_lane_progress_ms if last_lane_progress_ms > 0 else None
    )
    active_lane_age_ms = (
        now_ms - active_lane_started_ms if active_lane and active_lane_started_ms > 0 else None
    )
    active_lane_overdue = bool(runtime_progress.get("active_lane_overdue"))
    if (
        active_lane_age_ms is not None
        and active_lane_age_ms >= 0
        and active_lane_budget_ms > 0
        and active_lane_age_ms > active_lane_budget_ms
    ):
        active_lane_overdue = True

    recent_last_scan = (
        last_scan_age_ms is not None
        and last_scan_age_ms >= 0
        and last_scan_age_ms <= progress_budget_ms
    )
    recent_lane_progress = (
        last_lane_progress_age_ms is not None
        and last_lane_progress_age_ms >= 0
        and last_lane_progress_age_ms <= progress_budget_ms
    )
    active_bounded_lane = (
        bool(active_lane)
        and active_lane_age_ms is not None
        and active_lane_age_ms >= 0
        and active_lane_budget_ms > 0
        and active_lane_age_ms <= active_lane_budget_ms
        and not active_lane_overdue
    )
    exporter_fresh = (
        current_state_age_ms is not None
        and current_state_age_ms >= 0
        and current_state_age_ms <= progress_budget_ms
    )

    if recent_last_scan:
        progress_source = "last_scan"
    elif recent_lane_progress:
        progress_source = "runtime_lane"
    elif active_bounded_lane:
        progress_source = "active_bounded_lane"
    elif exporter_fresh:
        progress_source = "exporter_only"
    else:
        progress_source = "none"

    runtime_progress_details = {
        "loop_iteration_started_ms": loop_iteration_started_ms,
        "loop_iteration_completed_ms": loop_iteration_completed_ms,
        "last_lane_progress_ms": last_lane_progress_ms,
        "last_lane_progress_age_ms": last_lane_progress_age_ms,
        "active_lane": active_lane,
        "active_lane_started_ms": active_lane_started_ms,
        "active_lane_age_ms": active_lane_age_ms,
        "active_lane_budget_ms": active_lane_budget_ms,
        "active_lane_overdue": active_lane_overdue,
    }
    runtime_progress_valid = recent_last_scan or recent_lane_progress or active_bounded_lane
    exporter_only_progress = progress_source == "exporter_only"
    return (
        progress_source,
        exporter_only_progress,
        runtime_progress_valid,
        runtime_progress_details,
    )


def _weak_order_truth_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    weak: list[dict[str, Any]] = []
    for rec in _state_journal_events(state):
        kind = str(rec.get("kind") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        fill_status = str(payload.get("order_truth_fill_status") or "").lower()
        evidence_status = str(payload.get("order_truth_evidence_status") or "").lower()
        if not fill_status and not evidence_status:
            continue
        if (
            fill_status == "confirmed_fill"
            and evidence_status == "available"
            and payload.get("terminal_without_truth") is not True
        ):
            continue
        weak.append(
            {
                "kind": kind,
                "position_id": payload.get("position_id"),
                "symbol": payload.get("symbol"),
                "venue": payload.get("hedge_venue") or payload.get("venue"),
                "order_id": payload.get("order_id") or payload.get("accepted_order_id"),
                "client_order_id": (
                    payload.get("client_order_id") or payload.get("accepted_client_order_id")
                ),
                "order_truth_fill_status": fill_status,
                "order_truth_evidence_status": evidence_status,
                "order_truth_decision": payload.get("order_truth_decision"),
                "order_truth_missing_evidence": (payload.get("order_truth_missing_evidence") or []),
            }
        )
    return weak


def analyze_current_state(
    state: dict[str, Any],
    *,
    now_ms: int,
    max_tick_age_ms: int,
    require_exchange_truth: bool = False,
) -> HealthReport:
    fingerprints: list[str] = []
    last_tick_ms = int(state.get("last_tick_ms") or 0)
    tick_age_ms = now_ms - last_tick_ms if last_tick_ms else None
    generated_at_ms = 0
    try:
        generated_at_ms = int(state.get("generated_at_ms", 0) or 0)
    except (TypeError, ValueError):
        generated_at_ms = 0
    current_state_age_ms = now_ms - generated_at_ms if generated_at_ms > 0 else None
    open_count = int(state.get("open_position_count") or 0)
    pending_entries = int(state.get("pending_entry_count") or 0)
    pending_closes = int(state.get("pending_close_count") or 0)
    pending_residual_repairs = int(state.get("pending_residual_repair_count") or 0)
    pending_close_reconciliations = int(state.get("pending_close_reconciliation_count") or 0)
    pending_close_reconciliation_blocking = int(
        state.get("pending_close_reconciliation_blocking_count") or 0
    )
    pending_close_reconciliation_terminal_flat = int(
        state.get("pending_close_reconciliation_terminal_flat_count") or 0
    )
    clean = (
        open_count == 0
        and pending_entries == 0
        and pending_closes == 0
        and pending_residual_repairs == 0
        and pending_close_reconciliation_blocking == 0
    )
    last_scan = state.get("last_scan")
    last_scan_ts_ms = 0
    if isinstance(last_scan, dict):
        try:
            last_scan_ts_ms = int(last_scan.get("ts_ms", 0) or 0)
        except (TypeError, ValueError):
            last_scan_ts_ms = 0
    last_scan_age_ms = now_ms - last_scan_ts_ms if last_scan_ts_ms > 0 else None

    if state.get("lifecycle") != "running":
        fingerprints.append("live_lifecycle_not_running")
    if (
        state.get("risk_mode") == "fail_closed"
        and clean
        and not state.get("recovery_blocked_reason")
    ):
        fingerprints.append("stale_fail_closed_clean_state")
    if state.get("last_scan") is None:
        fingerprints.append("last_scan_missing")
    if pending_close_reconciliation_blocking:
        fingerprints.append("pending_close_reconciliations_active")
    exchange_truth = state.get("exchange_truth")
    exchange_truth_mismatches: list[dict[str, Any]] = []
    recovery_decision = _recovery_decision_payload(state, exchange_truth)
    v1_lifecycle_closure = _v1_lifecycle_closure_payload(state, exchange_truth, now_ms)
    pending_entry_live_conflicts = (
        _pending_entry_live_conflict_summary(state, exchange_truth)
        if isinstance(exchange_truth, dict)
        else {"count": 0, "details": []}
    )
    exchange_truth_available = isinstance(exchange_truth, dict) and bool(
        exchange_truth.get("available")
    )
    exchange_truth_confidence = (
        str(exchange_truth.get("confidence", "")) if isinstance(exchange_truth, dict) else ""
    )
    exchange_truth_available_flat = (
        exchange_truth_available
        and not bool(exchange_truth.get("has_nonzero_position"))
        and not bool(exchange_truth.get("has_open_order"))
        if isinstance(exchange_truth, dict)
        else False
    )
    progress_budget_ms = max(int(max_tick_age_ms or 0) * 4, 60_000)
    (
        progress_source,
        exporter_only_progress,
        recent_runtime_progress,
        runtime_progress,
    ) = _runtime_progress_summary(
        state,
        now_ms=now_ms,
        progress_budget_ms=progress_budget_ms,
        last_scan_age_ms=last_scan_age_ms,
        current_state_age_ms=current_state_age_ms,
    )
    tick_stale = (
        tick_age_ms is None
        or tick_age_ms < -RUNTIME_TIMESTAMP_MAX_FUTURE_SKEW_MS
        or tick_age_ms > max_tick_age_ms
    )
    tick_stale_suppressed_by_runtime_progress = (
        tick_stale and clean and exchange_truth_available_flat and recent_runtime_progress
    )
    if tick_stale and not tick_stale_suppressed_by_runtime_progress:
        fingerprints.append("live_tick_stale")
        if exporter_only_progress:
            fingerprints.append("exporter_only_progress")
    if require_exchange_truth:
        if not isinstance(exchange_truth, dict):
            fingerprints.append("exchange_truth_missing")
        elif not exchange_truth_available:
            fingerprints.append("exchange_truth_unavailable")
        elif exchange_truth_confidence != "high":
            fingerprints.append("exchange_truth_confidence_not_high")
    if (
        isinstance(exchange_truth, dict)
        and exchange_truth.get("available")
        and exchange_truth.get("confidence") == "high"
    ):
        leg_mismatches = _exchange_truth_position_mismatches(state, exchange_truth)
        if leg_mismatches:
            fingerprints.append("exchange_truth_mismatch")
            fingerprints.append("local_exchange_position_mismatch")
            if any(m.get("check") == "unexpected_live_position" for m in leg_mismatches):
                fingerprints.append("nonzero_live_position")
            exchange_truth_mismatches.extend(leg_mismatches)
        if clean and exchange_truth.get("has_nonzero_position"):
            if "exchange_truth_mismatch" not in fingerprints:
                fingerprints.append("exchange_truth_mismatch")
            if "nonzero_live_position" not in fingerprints:
                fingerprints.append("nonzero_live_position")
            for venue, positions in (exchange_truth.get("positions") or {}).items():
                if not isinstance(positions, dict):
                    continue
                for symbol, pos in positions.items():
                    if not isinstance(pos, dict):
                        continue
                    try:
                        qty = abs(float(pos.get("quantity") or 0.0))
                    except (TypeError, ValueError):
                        qty = 0.0
                    if qty > 1e-9:
                        exchange_truth_mismatches.append(
                            {
                                "check": "unexpected_live_position",
                                "venue": str(pos.get("venue") or venue),
                                "symbol": str(pos.get("symbol") or symbol),
                                "side": pos.get("side"),
                                "quantity": qty,
                                "entry_price": pos.get("entry_price"),
                            }
                        )
        if clean and exchange_truth.get("has_open_order"):
            if "exchange_truth_mismatch" not in fingerprints:
                fingerprints.append("exchange_truth_mismatch")
            if "live_open_order" not in fingerprints:
                fingerprints.append("live_open_order")
            for order in _live_open_order_details(exchange_truth):
                exchange_truth_mismatches.append(
                    {
                        "check": "unexpected_live_open_order",
                        **order,
                    }
                )
    weak_order_truth_events = _weak_order_truth_events(state)
    if weak_order_truth_events:
        fingerprints.append("order_truth_gap_unresolved")
    auto_fail_closed_summary = state.get("auto_fail_closed_summary")
    if not isinstance(auto_fail_closed_summary, dict):
        auto_fail_closed_summary = {}
    stale_risk_state_alignment_summary = state.get("stale_risk_state_alignment_summary")
    if not isinstance(stale_risk_state_alignment_summary, dict):
        stale_risk_state_alignment_summary = {}

    severity = "critical" if any(fp != "last_scan_missing" for fp in fingerprints) else "warning"
    return HealthReport(
        name="current_state",
        ok=not fingerprints,
        severity=severity if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "tick_age_ms": tick_age_ms,
            "risk_mode": state.get("risk_mode"),
            "lifecycle": state.get("lifecycle"),
            "open_position_count": open_count,
            "pending_entry_count": pending_entries,
            "pending_close_count": pending_closes,
            "pending_close_reconciliation_count": pending_close_reconciliations,
            "pending_close_reconciliation_blocking_count": (pending_close_reconciliation_blocking),
            "pending_close_reconciliation_terminal_flat_count": (
                pending_close_reconciliation_terminal_flat
            ),
            "pending_close_reconciliation_symbols": list(
                state.get("pending_close_reconciliation_symbols") or []
            ),
            "pending_residual_repair_count": pending_residual_repairs,
            "last_scan_age_ms": last_scan_age_ms,
            "current_state_age_ms": current_state_age_ms,
            "progress_source": progress_source,
            "exporter_only_progress": exporter_only_progress,
            "runtime_progress": runtime_progress,
            "tick_stale_suppressed_by_runtime_progress": tick_stale_suppressed_by_runtime_progress,
            "exchange_truth_required": require_exchange_truth,
            "exchange_truth_available": exchange_truth_available,
            "exchange_truth_confidence": exchange_truth_confidence,
            "recovery_decision": recovery_decision,
            "v1_lifecycle_closure": v1_lifecycle_closure,
            "exchange_truth_mismatches": exchange_truth_mismatches,
            "pending_entry_live_conflicts": pending_entry_live_conflicts,
            "weak_order_truth_events": weak_order_truth_events,
            "auto_fail_closed_summary": auto_fail_closed_summary,
            "stale_risk_state_alignment_summary": (stale_risk_state_alignment_summary),
        },
    )


def _v1_lifecycle_closure_payload(
    state: dict[str, Any],
    exchange_truth: Any,
    now_ms: int,
) -> dict[str, Any]:
    existing = state.get("v1_lifecycle_closure")
    if (
        isinstance(existing, dict)
        and existing.get("version")
        and not _exchange_truth_high_confidence(exchange_truth)
    ):
        return dict(existing)
    events = _state_journal_events(state)
    owner_index = RecoveryOwnerIndex.from_state_and_journal(state, events)
    return build_v1_lifecycle_closure_table(
        local_state=state,
        exchange_truth=exchange_truth if isinstance(exchange_truth, dict) else None,
        generated_at_ms=now_ms,
        events=events,
        owner_index=owner_index,
    ).to_dict()


def _exchange_truth_high_confidence(exchange_truth: Any) -> bool:
    if not isinstance(exchange_truth, dict):
        return False
    available = bool(exchange_truth.get("available", exchange_truth.get("truth_available")))
    confidence = str(exchange_truth.get("confidence") or "").lower()
    return available and confidence == "high"


def _state_journal_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("journal_events", "events", "recent_events"):
        events = state.get(key)
        if isinstance(events, list):
            return [event for event in events if isinstance(event, dict)]
    return []


def _recovery_decision_payload(
    state: dict[str, Any],
    exchange_truth: Any,
) -> dict[str, Any]:
    decision = V1RecoveryDecisionCore().decide(
        RecoveryEvidenceSnapshot(
            local_open_positions=_state_collection_or_count(
                state, "open_positions", "open_position_count"
            ),
            pending_entries=_state_collection_or_count(
                state, "pending_entries", "pending_entry_count"
            ),
            residual_repairs=_state_collection_or_count(
                state, "pending_residual_repairs", "pending_residual_repair_count"
            ),
            passive_closes=_state_collection_or_count(
                state, "pending_passive_closes", "pending_close_count"
            ),
            exchange_truth=exchange_truth if isinstance(exchange_truth, dict) else None,
            prior_recovery_block_reason=state.get("recovery_blocked_reason"),
        )
    )
    return {
        "kind": decision.kind.value,
        "evidence_quality": decision.evidence_quality,
        "entry_allowed": decision.entry_allowed,
        "block_reason": decision.block_reason,
        "clear_reason": decision.clear_reason,
        "diagnostic_severity": decision.diagnostic_severity,
    }


def _state_collection_or_count(
    state: dict[str, Any],
    collection_key: str,
    count_key: str,
) -> tuple[Any, ...]:
    collection = state.get(collection_key)
    if isinstance(collection, dict):
        return tuple(collection.values())
    if isinstance(collection, (list, tuple, set)):
        return tuple(collection)
    count = int(state.get(count_key) or 0)
    return tuple({"source": count_key} for _ in range(max(count, 0)))


def analyze_resolver_config(text: str) -> HealthReport:
    nameservers: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("nameserver ") and len(line.split()) >= 2:
            nameservers.append(line.split()[1])
        elif line.startswith("servers="):
            nameservers.extend(
                item.strip() for item in line.split("=", 1)[1].split(",") if item.strip()
            )
    fingerprints: list[str] = []
    if not nameservers:
        fingerprints.append("resolver_missing_nameserver")
    elif nameservers[0] not in KNOWN_GOOD_RESOLVERS:
        fingerprints.append("unverified_resolver_first")
    return HealthReport(
        name="resolver_config",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={"nameservers": nameservers},
    )


def summarize_reports(reports: Iterable[HealthReport]) -> HealthSummary:
    items = list(reports)
    critical = sum(1 for r in items if not r.ok and r.severity == "critical")
    warning = sum(1 for r in items if not r.ok and r.severity == "warning")
    return HealthSummary(
        ok=critical == 0 and warning == 0,
        critical_count=critical,
        warning_count=warning,
        reports=items,
    )
