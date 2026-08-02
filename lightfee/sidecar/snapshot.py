"""Sidecar snapshot schema matching Rust reference opportunity input shape."""

from __future__ import annotations

from dataclasses import dataclass, field, fields as dataclass_fields
from enum import Enum
from math import isfinite
from typing import Callable, Optional

from lightfee.engine.execution_planner import ExecutableEntryEnvelope


SNAPSHOT_SCHEMA_VERSION = 5
LEGACY_SNAPSHOT_SCHEMA_VERSIONS = frozenset({1, 2, 3, 4})


def funding_rate_sample_id(
    *,
    venue: str,
    symbol: str,
    observed_at_ms: int,
    rate_bps: float,
    funding_timestamp_ms: int,
) -> str:
    """Canonical identity for one funding-rate observation."""
    return (
        f"funding:{str(venue).strip().lower()}:"
        f"{str(symbol).strip().upper()}:{int(observed_at_ms)}:"
        f"{float(rate_bps):.17g}:{int(funding_timestamp_ms or 0)}"
    )


def funding_rate_evidence_reason(
    *,
    venue: str,
    symbol: str,
    rate_bps: object,
    funding_timestamp_ms: object,
    observed_at_ms: object,
    event_at_ms: object,
    received_at_ms: object,
    source: object,
    sample_id: object,
    decision_at_ms: int,
    future_skew_ms: int = 5_000,
) -> str:
    """Validate funding alpha, clocks and immutable sample identity."""
    if (
        isinstance(rate_bps, bool)
        or not isinstance(rate_bps, (int, float))
        or not isfinite(float(rate_bps))
    ):
        return "funding_rate_invalid"
    for name, value in (
        ("observed", observed_at_ms),
        ("event", event_at_ms),
        ("received", received_at_ms),
    ):
        if isinstance(value, bool) or not isinstance(value, int):
            return f"funding_rate_{name}_at_ms_invalid"
    observed = int(observed_at_ms)
    event = int(event_at_ms)
    received = int(received_at_ms)
    decision = int(decision_at_ms)
    skew = max(int(future_skew_ms), 0)
    if observed <= 0 or observed > decision + skew:
        return "funding_rate_observed_at_ms_invalid"
    if received < observed or received > decision + skew:
        return "funding_rate_received_at_ms_invalid"
    if event < 0 or event > received + skew or event > decision + skew:
        return "funding_rate_event_at_ms_invalid"
    if not str(source or "").strip():
        return "funding_rate_source_invalid"
    actual_sample_id = str(sample_id or "").strip()
    if not actual_sample_id:
        return "funding_rate_sample_id_invalid"
    expected_sample_id = funding_rate_sample_id(
        venue=venue,
        symbol=symbol,
        observed_at_ms=observed,
        rate_bps=float(rate_bps),
        funding_timestamp_ms=int(funding_timestamp_ms or 0),
    )
    if actual_sample_id != expected_sample_id:
        return "funding_rate_sample_id_mismatch"
    return ""
_V3_SOURCE_MODES = {
    "single_process_entry",
    "coarse_sidecar",
    "direct_market",
    "direct_market_enriched",
    "sidecar_scan",
}
_V3_ACQUISITION_MODES = {
    "degraded_sidecar",
    "direct_market_view",
    "fresh_sidecar",
    "last_good_sidecar",
    "unavailable",
}
SLOW_LIQUIDITY_EVIDENCE_PENDING_REASON = "sidecar_slow_liquidity_pending"


def slow_liquidity_evidence_pending(
    *,
    evidence_status: object,
    evidence_reason: object,
    volume_24h_quote: object,
) -> bool:
    """Return whether sidecar-owned slow evidence is still warming.

    This is lifecycle accounting only: it never permits a candidate through
    the sidecar OI gate and it never asks live entry to repair the evidence.
    """
    try:
        volume_valid = bool(
            isfinite(float(volume_24h_quote or 0.0))
            and float(volume_24h_quote or 0.0) > 0.0
        )
    except (TypeError, ValueError):
        volume_valid = False
    return bool(
        volume_valid
        and str(evidence_status or "").strip().lower() == "unavailable"
        and str(evidence_reason or "").strip().lower()
        == SLOW_LIQUIDITY_EVIDENCE_PENDING_REASON
    )


def _validate_snapshot_contract(
    raw: object,
    *,
    expected_schema_version: int,
    require_open_interest_proof: bool,
) -> list[str]:
    """Return fail-closed proof-contract violations for a typed snapshot."""
    if not isinstance(raw, dict):
        return ["root_not_object"]
    required_fields = {
        "schema_version",
        "published_at_ms",
        "market_observed_at_ms",
        "funding_lifecycle",
        "market_lifecycle",
        "transfer_lifecycle",
        "liquidity_lifecycle",
        "degraded_venues",
        "degraded_domains",
        "degraded_symbols",
        "source_mode",
        "acquisition_mode",
        "candidate_build_observed_at_ms",
        "candidate_build_diagnostics",
        "quotes",
        "candidates",
    }
    errors = [
        f"missing:{contract_field}"
        for contract_field in sorted(required_fields - raw.keys())
    ]
    if raw.get("schema_version") != expected_schema_version:
        errors.append("schema_version_unsupported")

    def _nonnegative_int(value: object, *, positive: bool = False) -> bool:
        return (
            isinstance(value, int)
            and not isinstance(value, bool)
            and value >= (1 if positive else 0)
        )

    def _finite_number(value: object, *, positive: bool = False) -> bool:
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and isfinite(float(value))
            and (float(value) > 0 if positive else True)
        )

    def _reason_mentions_symbol(reason: str, symbol: str) -> bool:
        """Match the producer's ``SYMBOL: reason`` entries without substrings."""
        for raw_entry in reason.split(";"):
            entry = raw_entry.strip()
            prefix, separator, detail = entry.partition(":")
            if separator and prefix.strip() == symbol and detail.strip():
                return True
        return False

    pending_slow_liquidity_symbols_by_venue: dict[str, set[str]] = {}
    raw_quotes = raw.get("quotes")
    if isinstance(raw_quotes, dict):
        for quote in raw_quotes.values():
            if not isinstance(quote, dict) or not slow_liquidity_evidence_pending(
                evidence_status=quote.get("open_interest_evidence_status"),
                evidence_reason=quote.get("open_interest_evidence_reason"),
                volume_24h_quote=quote.get("volume_24h_quote"),
            ):
                continue
            venue = str(quote.get("venue", "") or "").strip().lower()
            symbol = str(quote.get("symbol", "") or "").strip().upper()
            if venue and symbol:
                pending_slow_liquidity_symbols_by_venue.setdefault(venue, set()).add(
                    symbol
                )

    published_at_ms = raw.get("published_at_ms")
    market_observed_at_ms = raw.get("market_observed_at_ms")
    candidate_build_at_ms = raw.get("candidate_build_observed_at_ms")
    if not _nonnegative_int(published_at_ms, positive=True):
        errors.append("published_at_ms_invalid")
    if not _nonnegative_int(market_observed_at_ms, positive=True):
        errors.append("market_observed_at_ms_invalid")
    if not _nonnegative_int(candidate_build_at_ms, positive=True):
        errors.append("candidate_build_observed_at_ms_invalid")
    if (
        _nonnegative_int(published_at_ms, positive=True)
        and _nonnegative_int(market_observed_at_ms, positive=True)
        and _nonnegative_int(candidate_build_at_ms, positive=True)
        and not (
            int(market_observed_at_ms)
            <= int(candidate_build_at_ms)
            <= int(published_at_ms)
        )
    ):
        errors.append("snapshot_watermark_order_invalid")

    list_fields = (
        "funding_lifecycle",
        "market_lifecycle",
        "transfer_lifecycle",
        "liquidity_lifecycle",
        "degraded_venues",
        "degraded_domains",
        "candidates",
    )
    for contract_field in list_fields:
        if not isinstance(raw.get(contract_field), list):
            errors.append(f"{contract_field}_invalid")
    degraded_venues_raw = raw.get("degraded_venues")
    degraded_domains_raw = raw.get("degraded_domains")
    degraded_symbols_raw = raw.get("degraded_symbols")
    if isinstance(degraded_venues_raw, list):
        if any(not isinstance(value, str) or not value.strip() for value in degraded_venues_raw):
            errors.append("degraded_venues_member_invalid")
        elif len(set(degraded_venues_raw)) != len(degraded_venues_raw):
            errors.append("degraded_venues_duplicate")
    if isinstance(degraded_domains_raw, list):
        if any(not isinstance(value, str) or not value.strip() for value in degraded_domains_raw):
            errors.append("degraded_domains_member_invalid")
        elif len(set(degraded_domains_raw)) != len(degraded_domains_raw):
            errors.append("degraded_domains_duplicate")
    if not isinstance(degraded_symbols_raw, dict):
        errors.append("degraded_symbols_invalid")
        degraded_symbols_raw = {}
    else:
        for venue, symbols in degraded_symbols_raw.items():
            if (
                not isinstance(venue, str)
                or not venue.strip()
                or venue != venue.strip().lower()
                or not isinstance(symbols, list)
                or not symbols
                or any(
                    not isinstance(symbol, str)
                    or not symbol.strip()
                    or symbol != symbol.strip().upper()
                    for symbol in symbols
                )
                or len(set(symbols)) != len(symbols)
            ):
                errors.append(f"degraded_symbols_member_invalid:{venue}")
    source_mode = raw.get("source_mode")
    acquisition_mode = raw.get("acquisition_mode")
    if source_mode not in _V3_SOURCE_MODES:
        errors.append("source_mode_invalid")
    if acquisition_mode not in _V3_ACQUISITION_MODES:
        errors.append("acquisition_mode_invalid")

    lifecycle_domain_by_field = {
        "funding_lifecycle": "funding",
        "market_lifecycle": "market",
        "transfer_lifecycle": "transfer",
        "liquidity_lifecycle": "liquidity",
    }
    reasoned_venues: set[str] = set()
    reasoned_venue_domains: set[tuple[str, str]] = set()
    reasoned_domains: set[str] = set()
    lifecycle_venues_by_domain: dict[str, set[str]] = {
        "funding": set(),
        "market": set(),
        "liquidity": set(),
    }
    lifecycle_evidence_by_domain: dict[
        str,
        dict[str, tuple[int, int, str]],
    ] = {
        "funding": {},
        "market": {},
        "liquidity": {},
    }
    all_lifecycle_venues: set[str] = set()
    lifecycle_identities_by_field: dict[str, set[tuple[str, ...]]] = {
        field: set() for field in lifecycle_domain_by_field
    }
    for lifecycle_field, domain in lifecycle_domain_by_field.items():
        records = raw.get(lifecycle_field)
        if not isinstance(records, list):
            continue
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                errors.append(f"lifecycle_record_invalid:{lifecycle_field}:{index}")
                continue
            required_identity = (
                ("from_venue", "to_venue")
                if lifecycle_field == "transfer_lifecycle"
                else ("venue",)
            )
            if any(
                not isinstance(record.get(name), str)
                or not str(record.get(name, "")).strip()
                or str(record.get(name)) != str(record.get(name)).strip().lower()
                for name in required_identity
            ):
                errors.append(
                    f"lifecycle_identity_invalid:{lifecycle_field}:{index}"
                )
            else:
                identity = tuple(str(record[name]) for name in required_identity)
                if identity in lifecycle_identities_by_field[lifecycle_field]:
                    errors.append(
                        f"lifecycle_duplicate_identity:{lifecycle_field}:"
                        + ":".join(identity)
                    )
                lifecycle_identities_by_field[lifecycle_field].add(identity)
                all_lifecycle_venues.update(identity)
            if not _nonnegative_int(record.get("observed_at_ms"), positive=True):
                errors.append(
                    f"lifecycle_observed_at_ms_invalid:{lifecycle_field}:{index}"
                )
            elif _nonnegative_int(candidate_build_at_ms, positive=True) and int(
                record["observed_at_ms"]
            ) > int(candidate_build_at_ms):
                errors.append(
                    f"lifecycle_after_candidate_watermark:{lifecycle_field}:{index}"
                )
            if lifecycle_field != "transfer_lifecycle" and not _nonnegative_int(
                record.get("symbol_count")
            ):
                errors.append(
                    f"lifecycle_symbol_count_invalid:{lifecycle_field}:{index}"
                )
            if not _nonnegative_int(record.get("coverage_usable")):
                errors.append(
                    f"lifecycle_coverage_invalid:{lifecycle_field}:{index}"
                )
            if (
                lifecycle_field != "transfer_lifecycle"
                and _nonnegative_int(record.get("symbol_count"))
                and _nonnegative_int(record.get("coverage_usable"))
                and int(record["coverage_usable"]) > int(record["symbol_count"])
            ):
                errors.append(f"lifecycle_coverage_exceeds_total:{lifecycle_field}:{index}")
            degraded_reason = record.get("degraded_reason")
            if not isinstance(degraded_reason, str):
                errors.append(
                    f"lifecycle_degraded_reason_invalid:{lifecycle_field}:{index}"
                )
                continue
            coverage_usable = (
                int(record["coverage_usable"])
                if _nonnegative_int(record.get("coverage_usable"))
                else -1
            )
            symbol_count = (
                int(record["symbol_count"])
                if _nonnegative_int(record.get("symbol_count"))
                else -1
            )
            insufficient_coverage = (
                coverage_usable == 0
                if lifecycle_field == "transfer_lifecycle"
                else symbol_count == 0 or coverage_usable < symbol_count
            )
            if (
                insufficient_coverage
                and not degraded_reason.strip()
                and not (
                    lifecycle_field == "liquidity_lifecycle"
                    and symbol_count > 0
                    and coverage_usable
                    + len(
                        pending_slow_liquidity_symbols_by_venue.get(
                            str(record.get("venue", "") or "").lower(),
                            set(),
                        )
                    )
                    >= symbol_count
                )
            ):
                errors.append(
                    f"lifecycle_insufficient_coverage_unexplained:{lifecycle_field}:{index}"
                )
            if lifecycle_field != "transfer_lifecycle" and isinstance(
                record.get("venue"), str
            ):
                venue = str(record["venue"])
                lifecycle_venues_by_domain[domain].add(venue)
                lifecycle_evidence_by_domain[domain][venue] = (
                    symbol_count,
                    coverage_usable,
                    degraded_reason,
                )
            if degraded_reason.strip():
                reasoned_domains.add(domain)
                if lifecycle_field != "transfer_lifecycle" and isinstance(
                    record.get("venue"), str
                ):
                    venue = str(record["venue"])
                    reasoned_venues.add(venue)
                    reasoned_venue_domains.add((venue, domain))

    degraded_venues = {
        value
        for value in (degraded_venues_raw if isinstance(degraded_venues_raw, list) else [])
        if isinstance(value, str) and value.strip()
    }
    degraded_symbol_venues = {
        value
        for value in degraded_symbols_raw
        if isinstance(value, str) and value.strip()
    }
    if degraded_venues - reasoned_venues:
        errors.append("degraded_venues_without_lifecycle_evidence")
    if degraded_symbol_venues - reasoned_venues:
        errors.append("degraded_symbols_without_lifecycle_evidence")
    normalized_degraded_domains = {
        value
        for value in (degraded_domains_raw if isinstance(degraded_domains_raw, list) else [])
        if isinstance(value, str) and value.strip()
    }
    domain_aliases = {
        "perp_liquidity": "liquidity",
    }
    normalized_degraded_domains = {
        domain_aliases.get(value, value) for value in normalized_degraded_domains
    }
    if normalized_degraded_domains - reasoned_domains:
        errors.append("degraded_domains_without_lifecycle_evidence")
    if any(
        venue not in degraded_venues
        and venue not in degraded_symbol_venues
        and domain not in normalized_degraded_domains
        for venue, domain in reasoned_venue_domains
    ):
        errors.append("lifecycle_degradation_not_attributed")
    has_degradation = bool(
        degraded_venues or degraded_symbol_venues or normalized_degraded_domains
    )
    if acquisition_mode == "fresh_sidecar" and has_degradation:
        errors.append("fresh_acquisition_with_degradation")
    if acquisition_mode in {"degraded_sidecar", "last_good_sidecar"} and not has_degradation:
        errors.append("degraded_acquisition_without_degradation")

    quotes = raw.get("quotes")
    candidates = raw.get("candidates")
    if not isinstance(quotes, dict):
        errors.append("quotes_invalid")
        quotes = {}
    if not isinstance(candidates, list):
        candidates = []
    future_quote_count = 0
    valid_quote_observations: list[int] = []
    quote_venues: set[str] = set()
    quote_symbols_by_venue: dict[str, set[str]] = {}
    funding_usable_symbols_by_venue: dict[str, set[str]] = {}
    crossed_quotes: list[tuple[str, str, str]] = []
    crossed_symbols_by_venue: dict[str, set[str]] = {}
    for key, quote in quotes.items():
        if not isinstance(key, str) or not isinstance(quote, dict):
            errors.append("quote_shape_invalid")
            continue
        errors.extend(
            _quote_field_contract_errors(
                quote,
                key=key,
                require_open_interest_proof=require_open_interest_proof,
            )
        )
        venue = quote.get("venue")
        symbol = quote.get("symbol")
        bid = quote.get("bid")
        ask = quote.get("ask")
        if not isinstance(venue, str) or not venue.strip():
            errors.append(f"quote_venue_invalid:{key}")
        elif venue != venue.strip().lower():
            errors.append(f"quote_venue_not_canonical:{key}")
        else:
            quote_venues.add(venue)
        if not isinstance(symbol, str) or not symbol.strip():
            errors.append(f"quote_symbol_invalid:{key}")
        elif symbol != symbol.strip().upper():
            errors.append(f"quote_symbol_not_canonical:{key}")
        elif isinstance(venue, str) and venue.strip():
            quote_symbols_by_venue.setdefault(venue, set()).add(symbol)
            funding_proof_reason = ""
            funding_proof_present = any(
                quote.get(field_name) not in (None, "", 0)
                for field_name in (
                    "funding_rate_observed_at_ms",
                    "funding_rate_event_at_ms",
                    "funding_rate_received_at_ms",
                    "funding_rate_source",
                    "funding_rate_sample_id",
                )
            )
            if require_open_interest_proof and funding_proof_present:
                funding_proof_reason = funding_rate_evidence_reason(
                    venue=venue,
                    symbol=symbol,
                    rate_bps=quote.get("funding_rate_bps"),
                    funding_timestamp_ms=quote.get("funding_timestamp_ms"),
                    observed_at_ms=quote.get("funding_rate_observed_at_ms"),
                    event_at_ms=quote.get("funding_rate_event_at_ms"),
                    received_at_ms=quote.get("funding_rate_received_at_ms"),
                    source=quote.get("funding_rate_source"),
                    sample_id=quote.get("funding_rate_sample_id"),
                    decision_at_ms=(
                        int(candidate_build_at_ms)
                        if _nonnegative_int(
                            candidate_build_at_ms, positive=True
                        )
                        else 0
                    ),
                )
                if funding_proof_reason:
                    errors.append(
                        f"quote_funding_evidence_invalid:{key}:"
                        f"{funding_proof_reason}"
                    )
            if (
                _finite_number(quote.get("funding_rate_bps"))
                and _nonnegative_int(quote.get("funding_timestamp_ms"), positive=True)
                and _nonnegative_int(quote.get("funding_interval_ms"), positive=True)
                and not funding_proof_reason
            ):
                funding_usable_symbols_by_venue.setdefault(venue, set()).add(symbol)
        if not _finite_number(bid, positive=True):
            errors.append(f"quote_bid_invalid:{key}")
        if not _finite_number(ask, positive=True):
            errors.append(f"quote_ask_invalid:{key}")
        # A crossed BBO is a market-quality failure, not a malformed transport
        # record.  Keep it readable so downstream consumers can reject only the
        # affected venue/symbol and preserve the rest of a multi-venue snapshot.
        if (
            _finite_number(bid, positive=True)
            and _finite_number(ask, positive=True)
            and float(bid) > float(ask)
            and isinstance(venue, str)
            and venue.strip()
            and isinstance(symbol, str)
            and symbol.strip()
        ):
            crossed_quotes.append((key, venue, symbol))
            crossed_symbols_by_venue.setdefault(venue, set()).add(symbol)
        if (
            isinstance(venue, str)
            and venue.strip()
            and isinstance(symbol, str)
            and symbol.strip()
            and key != f"{venue}:{symbol}"
        ):
            errors.append(f"quote_key_identity_mismatch:{key}")
        observed_at_ms = quote.get("observed_at_ms")
        if not _nonnegative_int(observed_at_ms, positive=True):
            errors.append(f"quote_observed_at_ms_invalid:{key}")
            continue
        valid_quote_observations.append(int(observed_at_ms))
        if _nonnegative_int(candidate_build_at_ms, positive=True) and int(
            observed_at_ms
        ) > int(candidate_build_at_ms):
            future_quote_count += 1
            errors.append(f"quote_after_candidate_watermark:{key}")
    if (
        valid_quote_observations
        and _nonnegative_int(market_observed_at_ms, positive=True)
        and int(market_observed_at_ms) > max(valid_quote_observations)
    ):
        errors.append("market_watermark_ahead_of_quotes")
    if isinstance(candidates, list) and any(
        not isinstance(candidate, dict) for candidate in candidates
    ):
        errors.append("candidate_shape_invalid")
    if isinstance(candidates, list):
        candidate_numeric_fields = (
            "funding_diff_bps",
            "funding_edge_bps",
            "expected_edge_bps",
            "worst_case_edge_bps",
            "ranking_edge_bps",
        )
        for index, candidate in enumerate(candidates):
            if not isinstance(candidate, dict):
                continue
            for identity_field in ("long_venue", "short_venue", "symbol"):
                value = candidate.get(identity_field)
                if not isinstance(value, str) or not value.strip():
                    errors.append(
                        f"candidate_identity_invalid:{index}:{identity_field}"
                    )
            for venue_field in ("long_venue", "short_venue"):
                value = candidate.get(venue_field)
                if (
                    isinstance(value, str)
                    and value.strip()
                    and value != value.strip().lower()
                ):
                    errors.append(
                        f"candidate_venue_not_canonical:{index}:{venue_field}"
                    )
            candidate_symbol = candidate.get("symbol")
            if (
                isinstance(candidate_symbol, str)
                and candidate_symbol.strip()
                and candidate_symbol != candidate_symbol.strip().upper()
            ):
                errors.append(f"candidate_symbol_not_canonical:{index}")
            for numeric_field in candidate_numeric_fields:
                if not _finite_number(candidate.get(numeric_field)):
                    errors.append(
                        f"candidate_numeric_invalid:{index}:{numeric_field}"
                    )
    for key, venue, symbol in crossed_quotes:
        degraded_for_venue = degraded_symbols_raw.get(venue, [])
        market_evidence = lifecycle_evidence_by_domain["market"].get(venue)
        market_degradation_proved = (
            market_evidence is not None
            and market_evidence[1] < market_evidence[0]
            and _reason_mentions_symbol(market_evidence[2], symbol)
        )
        if symbol not in degraded_for_venue or not market_degradation_proved:
            errors.append(f"crossed_quote_degradation_unproved:{key}")
    for domain in ("funding", "market", "liquidity"):
        missing_lifecycle_venues = quote_venues - lifecycle_venues_by_domain[domain]
        if missing_lifecycle_venues:
            errors.append(f"quote_venue_missing_{domain}_lifecycle")
        for venue, evidence in lifecycle_evidence_by_domain[domain].items():
            symbols = quote_symbols_by_venue.get(venue, set())
            symbol_count, coverage_usable, degraded_reason = evidence
            if domain == "funding":
                maximum_provable_coverage = len(
                    symbols & funding_usable_symbols_by_venue.get(venue, set())
                )
            elif domain in {"market", "liquidity"}:
                # Market and liquidity proofs are independent venue
                # lifecycles.  A symbol can retain a valid BBO and/or OI
                # observation while malformed funding evidence is quarantined
                # from the strict executable quote payload, so quote-row
                # cardinality is not their upper bound.
                maximum_provable_coverage = symbol_count
            if coverage_usable > maximum_provable_coverage:
                errors.append(
                    f"lifecycle_coverage_exceeds_quote_symbols:{domain}:{venue}"
                )
            if symbol_count < len(symbols):
                errors.append(
                    f"lifecycle_total_below_quote_symbols:{domain}:{venue}"
                )
            if (
                coverage_usable < len(symbols)
                and not degraded_reason.strip()
                and not (
                    domain == "liquidity"
                    and coverage_usable
                    + len(pending_slow_liquidity_symbols_by_venue.get(venue, set()) & symbols)
                    >= len(symbols)
                )
            ):
                errors.append(f"quote_coverage_unproved:{domain}:{venue}")

    diagnostics = raw.get("candidate_build_diagnostics")
    required_counts = {
        "input_quote_count",
        "requested_symbol_count",
        "directional_pair_count",
        "output_candidate_count",
        "future_input_quote_count",
    }
    if not isinstance(diagnostics, dict):
        errors.append("candidate_build_diagnostics_invalid")
        diagnostics = {}
    for diagnostic_field in sorted(required_counts):
        if not _nonnegative_int(diagnostics.get(diagnostic_field)):
            errors.append(f"candidate_diagnostic_invalid:{diagnostic_field}")
    requested_symbols_raw = diagnostics.get("requested_symbols")
    requested_symbols: list[str] = []
    if (
        not isinstance(requested_symbols_raw, list)
        or any(
            not isinstance(symbol, str)
            or not symbol.strip()
            or symbol != symbol.strip().upper()
            for symbol in (
                requested_symbols_raw if isinstance(requested_symbols_raw, list) else []
            )
        )
        or (
            isinstance(requested_symbols_raw, list)
            and requested_symbols_raw != sorted(set(requested_symbols_raw))
        )
    ):
        errors.append("candidate_requested_symbols_invalid")
    else:
        requested_symbols = list(requested_symbols_raw)
    requested_venues_raw = diagnostics.get("requested_venues")
    requested_venues: list[str] = []
    if (
        not isinstance(requested_venues_raw, list)
        or any(
            not isinstance(venue, str)
            or not venue.strip()
            or venue != venue.strip().lower()
            for venue in (
                requested_venues_raw if isinstance(requested_venues_raw, list) else []
            )
        )
        or (
            isinstance(requested_venues_raw, list)
            and requested_venues_raw != sorted(set(requested_venues_raw))
        )
    ):
        errors.append("candidate_requested_venues_invalid")
    else:
        requested_venues = list(requested_venues_raw)
    rejection_counts = diagnostics.get("rejection_counts")
    if not isinstance(rejection_counts, dict) or any(
        not isinstance(reason, str)
        or not reason.strip()
        or not _nonnegative_int(count, positive=True)
        for reason, count in (
            rejection_counts.items() if isinstance(rejection_counts, dict) else []
        )
    ):
        errors.append("candidate_rejection_counts_invalid")
    if _nonnegative_int(diagnostics.get("input_quote_count")) and int(
        diagnostics["input_quote_count"]
    ) != len(quotes):
        errors.append("input_quote_count_mismatch")
    if _nonnegative_int(diagnostics.get("output_candidate_count")) and int(
        diagnostics["output_candidate_count"]
    ) != len(candidates):
        errors.append("output_candidate_count_mismatch")
    if _nonnegative_int(diagnostics.get("future_input_quote_count")) and int(
        diagnostics["future_input_quote_count"]
    ) != future_quote_count:
        errors.append("future_input_quote_count_mismatch")
    if (
        _nonnegative_int(diagnostics.get("directional_pair_count"))
        and _nonnegative_int(diagnostics.get("output_candidate_count"))
        and int(diagnostics["output_candidate_count"])
        > int(diagnostics["directional_pair_count"])
    ):
        errors.append("output_candidate_count_exceeds_evaluated")
    if (
        _nonnegative_int(diagnostics.get("directional_pair_count"))
        and _nonnegative_int(diagnostics.get("output_candidate_count"))
        and isinstance(rejection_counts, dict)
        and all(
            isinstance(reason, str)
            and reason.strip()
            and _nonnegative_int(count, positive=True)
            for reason, count in rejection_counts.items()
        )
        and int(diagnostics["output_candidate_count"])
        + sum(int(count) for count in rejection_counts.values())
        != int(diagnostics["directional_pair_count"])
    ):
        errors.append("candidate_diagnostics_conservation_mismatch")
    requested_symbol_count = diagnostics.get("requested_symbol_count")
    if (
        _nonnegative_int(requested_symbol_count)
        and int(requested_symbol_count) != len(requested_symbols)
    ):
        errors.append("requested_symbol_count_mismatch")
    requested_symbol_set = set(requested_symbols)
    requested_venue_set = set(requested_venues)
    quote_symbol_count = len(
        {
            symbol
            for symbols in quote_symbols_by_venue.values()
            for symbol in symbols
        }
    )
    if (
        quote_symbol_count > 0
        and _nonnegative_int(requested_symbol_count)
        and int(requested_symbol_count) == 0
    ):
        errors.append("requested_symbol_count_missing_for_quotes")
    if (
        requested_symbols
        and any(
            symbol not in requested_symbol_set
            for symbols in quote_symbols_by_venue.values()
            for symbol in symbols
        )
    ):
        errors.append("quote_symbol_not_requested")
    if quote_venues - requested_venue_set:
        errors.append("quote_venue_not_requested")
    if all_lifecycle_venues - requested_venue_set:
        errors.append("lifecycle_venue_not_requested")
    for domain, lifecycle_venues in lifecycle_venues_by_domain.items():
        if requested_venue_set - lifecycle_venues:
            errors.append(f"requested_venue_missing_{domain}_lifecycle")
    if degraded_venues - requested_venue_set or degraded_symbol_venues - requested_venue_set:
        errors.append("degraded_venue_not_requested")
    for venue, symbols in degraded_symbols_raw.items():
        if not isinstance(symbols, list):
            continue
        if any(symbol not in requested_symbol_set for symbol in symbols):
            errors.append(f"degraded_symbol_not_requested:{venue}")
        evidence_rows = [
            evidence_by_venue.get(venue)
            for evidence_by_venue in lifecycle_evidence_by_domain.values()
        ]
        for symbol in symbols:
            if not any(
                evidence is not None
                and evidence[1] < evidence[0]
                and _reason_mentions_symbol(evidence[2], symbol)
                for evidence in evidence_rows
            ):
                errors.append(f"degraded_symbol_evidence_unproved:{venue}:{symbol}")
    crossed_quote_keys = {key for key, _venue, _symbol in crossed_quotes}
    candidate_identities: set[tuple[str, str, str]] = set()
    # Candidate contract evidence is identity-bound and duplicate-sensitive,
    # but scanning every quote for every candidate makes publication O(C*Q).
    # Build the exact same fail-closed identity proof once and reuse it.
    from lightfee.sidecar.publisher import (
        _v3_economics_contract_reason,
        _v3_quote_contract_evidence_index,
    )

    quote_evidence_index = _v3_quote_contract_evidence_index(quotes)
    if isinstance(candidates, list):
        for index, candidate in enumerate(candidates):
            if not isinstance(candidate, dict):
                continue
            errors.extend(_candidate_field_contract_errors(candidate, index=index))
            if candidate.get("economics_complete") is True:
                economics_reason = _v3_economics_contract_reason(
                    candidate,
                    quotes_raw=quotes,
                    quote_evidence_index=quote_evidence_index,
                )
                if economics_reason:
                    errors.append(
                        f"candidate_economics_contract_invalid:{index}:{economics_reason}"
                    )
            else:
                if candidate.get("blocked") is not True:
                    errors.append(f"candidate_incomplete_economics_unblocked:{index}")
                blocked_reasons = candidate.get("blocked_reasons")
                economics_incomplete_reason = candidate.get(
                    "economics_incomplete_reason"
                )
                has_incomplete_reason = (
                    isinstance(blocked_reasons, list)
                    and any(
                        isinstance(reason, str) and reason.strip()
                        for reason in blocked_reasons
                    )
                ) or (
                    isinstance(economics_incomplete_reason, str)
                    and economics_incomplete_reason.strip()
                )
                if not has_incomplete_reason:
                    errors.append(
                        f"candidate_incomplete_economics_reason_missing:{index}"
                    )
            long_venue = candidate.get("long_venue")
            short_venue = candidate.get("short_venue")
            symbol = candidate.get("symbol")
            if all(isinstance(value, str) for value in (long_venue, short_venue, symbol)):
                identity = (str(long_venue), str(short_venue), str(symbol))
                if identity in candidate_identities:
                    errors.append(f"candidate_duplicate_identity:{index}")
                candidate_identities.add(identity)
            if symbol not in requested_symbol_set:
                errors.append(f"candidate_symbol_not_requested:{index}")
            if long_venue not in requested_venue_set or short_venue not in requested_venue_set:
                errors.append(f"candidate_venue_not_requested:{index}")
            if long_venue == short_venue:
                errors.append(f"candidate_venues_not_distinct:{index}")
            for leg, venue in (("long", long_venue), ("short", short_venue)):
                quote_key = f"{venue}:{symbol}"
                if quote_key not in quotes or quote_key in crossed_quote_keys:
                    errors.append(f"candidate_quote_unproved:{index}:{leg}")
                elif symbol not in funding_usable_symbols_by_venue.get(str(venue), set()):
                    errors.append(f"candidate_funding_quote_unproved:{index}:{leg}")
            for field_name in (
                "funding_timestamp_ms",
                "first_funding_timestamp_ms",
                "long_funding_timestamp_ms",
                "short_funding_timestamp_ms",
            ):
                if not _nonnegative_int(candidate.get(field_name), positive=True):
                    errors.append(f"candidate_{field_name}_invalid:{index}")
    if acquisition_mode == "unavailable":
        if not has_degradation:
            errors.append("unavailable_acquisition_without_degradation")
        if quotes or candidates:
            errors.append("unavailable_acquisition_with_payload")
    if _nonnegative_int(requested_symbol_count, positive=True):
        for domain, evidence_by_venue in lifecycle_evidence_by_domain.items():
            for venue, (symbol_count, _coverage, _reason) in evidence_by_venue.items():
                # Candidate construction requests a cross-venue union.  A
                # venue lifecycle total describes only that venue's listed
                # subset, but it may never claim symbols outside the request.
                if symbol_count > int(requested_symbol_count):
                    errors.append(
                        f"lifecycle_requested_count_exceeded:{domain}:{venue}"
                    )
    return errors


def validate_v4_snapshot_contract(raw: object) -> list[str]:
    """Validate the legacy V4 proof contract without granting OI trust."""
    return _validate_snapshot_contract(
        raw,
        expected_schema_version=4,
        require_open_interest_proof=False,
    )


def validate_v5_snapshot_contract(raw: object) -> list[str]:
    """Validate V5, including independent open-interest proof fields."""
    return _validate_snapshot_contract(
        raw,
        expected_schema_version=5,
        require_open_interest_proof=True,
    )


class SnapshotFreshness(Enum):
    """V1 snapshot freshness states (CONTRACT OPP-001).

    V1 anchor: src/opportunity_input/types.rs  OpportunityInputDomainState
    V1 semantics:
      - FRESH: usable directly
      - LAST_GOOD_FALLBACK: current stale/missing but recent valid snapshot exists
      - STALE: publication or broad-market observation exceeded its budget
      - MISSING: no snapshot available at all (blocks trading)
      - DEGRADED: one or more health domains degraded but snapshot is otherwise usable
    """

    FRESH = "fresh"
    LAST_GOOD_FALLBACK = "last_good_fallback"
    STALE = "stale"
    MISSING = "missing"
    DEGRADED = "degraded"


@dataclass
class FundingLifecycle:
    """Funding data freshness metadata — V1 domain-level lifecycle."""

    venue: str
    observed_at_ms: int
    symbol_count: int
    coverage_usable: int = 0
    degraded_reason: str = ""


@dataclass
class MarketLifecycle:
    """Market data freshness metadata — V1 domain-level lifecycle."""

    venue: str
    observed_at_ms: int
    symbol_count: int
    coverage_usable: int = 0
    degraded_reason: str = ""


@dataclass
class TransferLifecycle:
    """Transfer status freshness metadata — V1 domain-level lifecycle."""

    from_venue: str
    to_venue: str
    observed_at_ms: int
    coverage_usable: int = 0
    degraded_reason: str = ""


@dataclass
class LiquidityLifecycle:
    """Liquidity data freshness metadata — V1 domain-level lifecycle."""

    venue: str
    observed_at_ms: int
    symbol_count: int
    coverage_usable: int = 0
    degraded_reason: str = ""
    domain: str = "perp_liquidity"
    source: str = "sidecar_perp_liquidity"
    publish_interval_ms: int = 0
    published_at_ms: int = 0


@dataclass
class QuoteSnapshot:
    venue: str
    symbol: str
    bid: float
    ask: float
    observed_at_ms: int = 0
    market_event_at_ms: int = 0
    source: str = "sidecar_quote"
    bid_size: float = 0.0
    ask_size: float = 0.0
    # Optional executable L2 ladders.  They are complete books in price-time
    # priority (bids descending, asks ascending) and quantities are canonical
    # base units.  An absent ladder deliberately means BBO-only, rather than
    # an inferred depth profile.  Keeping this optional preserves schema-v1/
    # v2/v3 reads and lets the spread paper engine use VWAP only when evidence
    # has actually been supplied by its market-data boundary.
    bid_depth: tuple[tuple[float, float], ...] = ()
    ask_depth: tuple[tuple[float, float], ...] = ()
    funding_rate_bps: float = 0.0
    funding_rate_observed_at_ms: int = 0
    funding_rate_event_at_ms: int = 0
    funding_rate_received_at_ms: int = 0
    funding_rate_source: str = ""
    funding_rate_sample_id: str = ""
    funding_timestamp_ms: int = 0
    funding_interval_ms: int = 0
    predicted_funding_rate_bps: Optional[float] = None
    funding_forecast_source: str = "quoted_rate"
    # Count of settled forecast-vs-realised observations held by the source.
    # A prediction is never high-confidence until this clears the configured
    # calibration threshold.
    funding_forecast_sample_count: int = 0
    funding_forecast_uncertainty_bps: float = 0.0
    # First observed forecast in the current persisted calibration epoch.
    # Enhanced live mode uses this to enforce the required shadow duration,
    # not merely a burst of quickly accumulated samples.
    funding_forecast_started_at_ms: int = 0
    # The calibrator validates that the older and newer halves of the observed
    # error distribution agree before enhanced-live may use a forecast.
    funding_forecast_distribution_stable: bool = False
    funding_forecast_stability_reason: str = "not_calibrated"
    funding_forecast_median_drift_bps: float = 0.0
    funding_forecast_p90_drift_bps: float = 0.0
    settled_funding_rate_bps: Optional[float] = None
    mark_price: float = 0.0
    index_price: float = 0.0
    volume_24h_quote: float = 0.0
    open_interest: float | None = None
    open_interest_evidence_status: str = "unavailable"
    open_interest_evidence_reason: str = ""
    open_interest_observed_at_ms: int = 0
    open_interest_event_at_ms: int = 0
    open_interest_received_at_ms: int = 0
    open_interest_source: str = ""
    open_interest_sample_id: str = ""
    open_interest_venue_symbol: str = ""
    raw_open_interest: float | None = None
    raw_open_interest_unit: str = ""
    open_interest_contract_multiplier: float | None = None
    open_interest_conversion_mark_price: float | None = None
    oi_candidate_count: int = 0
    oi_cache_hit_count: int = 0
    oi_cache_miss_count: int = 0
    oi_refresh_attempt_count: int = 0
    oi_refresh_cap: int = 0
    oi_deferred_count: int = 0
    oi_timeout_count: int = 0
    oi_refresh_elapsed_ms: int = 0
    # Contract normalisation evidence used by spread/reversion admission.
    underlying: str = ""
    quote_currency: str = ""
    contract_type: str = "linear"
    contract_multiplier: float = 1.0
    mark_index_source: str = ""
    price_precision: int = 0
    quantity_precision: int = 0
    # Exact symbol execution increments and current effective minimums.  The
    # decimal-place fields above are display metadata and cannot distinguish
    # steps such as 0.1 from 0.5.
    price_tick: float = 0.0
    quantity_step_base: float = 0.0
    min_quantity_base: float = 0.0
    min_notional_quote: float = 0.0
    min_notional_evidence_complete: bool = False
    venue_status: str = "active"
    # `False` is intentionally fail-closed for spread paper: public BBO alone
    # cannot prove that two venue contracts represent the same economic unit.
    # Funding discovery does not consume this field.
    contract_normalization_complete: bool = False


def _quote_field_contract_errors(
    quote: dict[str, object],
    *,
    key: str,
    require_open_interest_proof: bool = False,
) -> list[str]:
    """Strictly type-check every raw V4 quote field consumed downstream.

    The JSON parser intentionally supports legacy schemas and therefore
    coerces several scalar fields.  V4 must reject those coercions at its raw
    trust boundary so a string size/precision cannot become executable market
    evidence after parsing.
    """
    errors: list[str] = []
    known_fields = {quote_field.name for quote_field in dataclass_fields(QuoteSnapshot)}
    for field_name in quote.keys() - known_fields:
        errors.append(f"quote_unknown_field:{key}:{field_name}")

    finite_fields = {
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
    }
    optional_finite_fields = {
        "predicted_funding_rate_bps",
        "settled_funding_rate_bps",
        "open_interest",
        "raw_open_interest",
        "open_interest_contract_multiplier",
        "open_interest_conversion_mark_price",
    }
    nonnegative_fields = {
        "bid_size",
        "ask_size",
        "funding_forecast_uncertainty_bps",
        "volume_24h_quote",
        "open_interest",
        "price_tick",
        "quantity_step_base",
        "min_quantity_base",
        "min_notional_quote",
    }
    integer_fields = {
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
    }
    boolean_fields = {
        "funding_forecast_distribution_stable",
        "contract_normalization_complete",
        "min_notional_evidence_complete",
    }
    string_fields = {
        "venue",
        "symbol",
        "source",
        "funding_forecast_source",
        "funding_rate_source",
        "funding_rate_sample_id",
        "funding_forecast_stability_reason",
        "open_interest_evidence_status",
        "open_interest_evidence_reason",
        "open_interest_source",
        "open_interest_sample_id",
        "open_interest_venue_symbol",
        "raw_open_interest_unit",
        "underlying",
        "quote_currency",
        "contract_type",
        "mark_index_source",
        "venue_status",
    }

    for field_name in finite_fields:
        if field_name not in quote:
            continue
        value = quote[field_name]
        valid = (
            type(value) in (int, float)
            and isfinite(float(value))
            and (field_name not in nonnegative_fields or float(value) >= 0.0)
        )
        if not valid:
            errors.append(f"quote_field_type_invalid:{key}:{field_name}")
    for field_name in optional_finite_fields:
        if field_name not in quote:
            continue
        value = quote[field_name]
        if value is not None and not (
            type(value) in (int, float) and isfinite(float(value))
        ):
            errors.append(f"quote_field_type_invalid:{key}:{field_name}")
    for field_name in integer_fields:
        if field_name not in quote:
            continue
        value = quote[field_name]
        if type(value) is not int or value < 0:
            errors.append(f"quote_field_type_invalid:{key}:{field_name}")
    for field_name in boolean_fields:
        if field_name in quote and type(quote[field_name]) is not bool:
            errors.append(f"quote_field_type_invalid:{key}:{field_name}")
    for field_name in string_fields:
        if field_name in quote and type(quote[field_name]) is not str:
            errors.append(f"quote_field_type_invalid:{key}:{field_name}")

    if require_open_interest_proof:
        required_oi_fields = {
            "open_interest",
            "open_interest_evidence_status",
            "open_interest_evidence_reason",
            "open_interest_observed_at_ms",
            "open_interest_received_at_ms",
            "open_interest_source",
            "open_interest_sample_id",
            "open_interest_venue_symbol",
            "raw_open_interest",
            "raw_open_interest_unit",
            "open_interest_contract_multiplier",
            "open_interest_conversion_mark_price",
        }
        for field_name in sorted(required_oi_fields - quote.keys()):
            errors.append(f"quote_oi_proof_missing:{key}:{field_name}")
        from lightfee.marketdata.open_interest import (
            normalize_open_interest_status,
            open_interest_sample_id,
        )

        status = normalize_open_interest_status(
            quote.get("open_interest_evidence_status", "unavailable")
        )
        if status == "observed":
            observed_valid = bool(
                type(quote.get("open_interest")) in (int, float)
                and isfinite(float(quote.get("open_interest")))
                and float(quote.get("open_interest")) >= 0.0
                and type(quote.get("raw_open_interest")) in (int, float)
                and isfinite(float(quote.get("raw_open_interest")))
                and float(quote.get("raw_open_interest")) >= 0.0
                and type(quote.get("open_interest_observed_at_ms")) is int
                and int(quote.get("open_interest_observed_at_ms", 0)) > 0
                and type(quote.get("open_interest_event_at_ms", 0)) is int
                and int(quote.get("open_interest_event_at_ms", 0)) >= 0
                and type(quote.get("open_interest_received_at_ms")) is int
                and int(quote.get("open_interest_received_at_ms", 0))
                >= int(quote.get("open_interest_observed_at_ms", 0))
                and int(quote.get("open_interest_event_at_ms", 0))
                <= int(quote.get("open_interest_received_at_ms", 0)) + 5_000
                and isinstance(quote.get("open_interest_source"), str)
                and bool(str(quote.get("open_interest_source", "")).strip())
                and isinstance(quote.get("open_interest_sample_id"), str)
                and bool(str(quote.get("open_interest_sample_id", "")).strip())
                and isinstance(quote.get("open_interest_venue_symbol"), str)
                and bool(str(quote.get("open_interest_venue_symbol", "")).strip())
                and isinstance(quote.get("raw_open_interest_unit"), str)
                and bool(str(quote.get("raw_open_interest_unit", "")).strip())
            )
            if not observed_valid:
                errors.append(f"quote_oi_observed_proof_invalid:{key}")
            else:
                raw_unit = str(quote.get("raw_open_interest_unit") or "")
                raw_value = float(quote.get("raw_open_interest"))
                value_quote = float(quote.get("open_interest"))
                multiplier_raw = quote.get("open_interest_contract_multiplier")
                mark_raw = quote.get("open_interest_conversion_mark_price")
                expected_quote: float | None = None
                if raw_unit == "quote":
                    expected_quote = raw_value
                elif raw_unit == "base":
                    if (
                        type(mark_raw) in (int, float)
                        and isfinite(float(mark_raw))
                        and float(mark_raw) > 0.0
                    ):
                        expected_quote = raw_value * float(mark_raw)
                elif raw_unit == "contracts":
                    if (
                        type(multiplier_raw) in (int, float)
                        and isfinite(float(multiplier_raw))
                        and float(multiplier_raw) > 0.0
                        and type(mark_raw) in (int, float)
                        and isfinite(float(mark_raw))
                        and float(mark_raw) > 0.0
                    ):
                        expected_quote = (
                            raw_value * float(multiplier_raw) * float(mark_raw)
                        )
                if expected_quote is None or abs(value_quote - expected_quote) > max(
                    1e-6,
                    abs(expected_quote or 0.0) * 1e-9,
                ):
                    errors.append(f"quote_oi_economic_identity_invalid:{key}")
                expected_sample_id = open_interest_sample_id(
                    venue=str(quote.get("venue") or ""),
                    canonical_symbol=str(quote.get("symbol") or ""),
                    venue_symbol=str(quote.get("open_interest_venue_symbol") or ""),
                    observed_at_ms=int(
                        quote.get("open_interest_event_at_ms")
                        or quote.get("open_interest_observed_at_ms")
                        or 0
                    ),
                    source=str(quote.get("open_interest_source") or ""),
                    raw_value=raw_value,
                    value_quote=value_quote,
                )
                if str(quote.get("open_interest_sample_id") or "") != expected_sample_id:
                    errors.append(f"quote_oi_sample_id_invalid:{key}")
        elif quote.get("open_interest") is not None:
            errors.append(f"quote_oi_untrusted_value_present:{key}")

    for depth_field in ("bid_depth", "ask_depth"):
        if depth_field not in quote:
            continue
        depth = quote[depth_field]
        valid_depth = isinstance(depth, list)
        if valid_depth:
            for level in depth:
                if (
                    not isinstance(level, list)
                    or len(level) != 2
                    or type(level[0]) not in (int, float)
                    or type(level[1]) not in (int, float)
                    or not isfinite(float(level[0]))
                    or not isfinite(float(level[1]))
                    or float(level[0]) <= 0.0
                    or float(level[1]) <= 0.0
                ):
                    valid_depth = False
                    break
        if not valid_depth:
            errors.append(f"quote_field_type_invalid:{key}:{depth_field}")

    if quote.get("contract_normalization_complete") is True:
        multiplier = quote.get("contract_multiplier")
        complete = bool(
            isinstance(quote.get("underlying"), str)
            and str(quote.get("underlying", "")).strip()
            and isinstance(quote.get("quote_currency"), str)
            and str(quote.get("quote_currency", "")).strip()
            and quote.get("contract_type") == "linear"
            and isinstance(quote.get("mark_index_source"), str)
            and str(quote.get("mark_index_source", "")).strip()
            and quote.get("venue_status") == "active"
            and type(multiplier) in (int, float)
            and isfinite(float(multiplier))
            and float(multiplier) > 0.0
            and type(quote.get("price_precision")) is int
            and int(quote.get("price_precision", -1)) >= 0
            and type(quote.get("quantity_precision")) is int
            and int(quote.get("quantity_precision", -1)) >= 0
            and type(quote.get("price_tick")) in (int, float)
            and isfinite(float(quote.get("price_tick", 0.0)))
            and float(quote.get("price_tick", 0.0)) > 0.0
            and type(quote.get("quantity_step_base")) in (int, float)
            and isfinite(float(quote.get("quantity_step_base", 0.0)))
            and float(quote.get("quantity_step_base", 0.0)) > 0.0
            and type(quote.get("min_quantity_base")) in (int, float)
            and isfinite(float(quote.get("min_quantity_base", 0.0)))
            and float(quote.get("min_quantity_base", 0.0)) > 0.0
            and type(quote.get("min_notional_quote")) in (int, float)
            and isfinite(float(quote.get("min_notional_quote", 0.0)))
            and float(quote.get("min_notional_quote", 0.0)) >= 0.0
            and quote.get("min_notional_evidence_complete") is True
            and type(quote.get("funding_timestamp_ms")) is int
            and int(quote.get("funding_timestamp_ms", 0)) > 0
            and type(quote.get("funding_interval_ms")) is int
            and int(quote.get("funding_interval_ms", 0)) > 0
        )
        if not complete:
            errors.append(f"quote_complete_contract_invalid:{key}")
    return errors


@dataclass
class CandidateInput:
    """One directed pair candidate for the live engine."""

    long_venue: str
    short_venue: str
    symbol: str
    funding_diff_bps: float
    funding_edge_bps: float
    expected_edge_bps: float
    worst_case_edge_bps: float
    ranking_edge_bps: float
    total_funding_edge_bps: float = 0.0
    # A staggered trade is admitted on what can be earned at the first actual
    # settlement, never on carry that requires holding an extra interval.
    # These V1-compatible fields make that lifecycle explicit while preserving
    # the legacy `funding_edge_bps` (which is the first-stage edge).
    first_stage_funding_edge_bps: float = 0.0
    first_stage_expected_edge_bps: float = 0.0
    first_stage_worst_case_edge_bps: float = 0.0
    second_stage_incremental_funding_edge_bps: float = 0.0
    second_stage_worst_case_funding_edge_bps: float = 0.0
    stagger_gap_ms: int = 0
    transfer_bias_bps: float = 0.0
    entry_cross_bps: float = 0.0
    fee_bps: float = 0.0
    entry_slippage_bps: float = 0.0
    # Per-leg heuristic estimates are evidence for the V1 passive-maker
    # selection.  The aggregate fields above and below remain the actual
    # economics inputs; these four fields make that selection auditable.
    long_entry_slippage_bps: float = 0.0
    short_entry_slippage_bps: float = 0.0
    long_exit_slippage_bps: float = 0.0
    short_exit_slippage_bps: float = 0.0
    # The final IOC fallback must charge four taker legs, even when the
    # shortlist assumed V1 passive execution.  These are immutable fee inputs
    # rather than a second edge formula.
    long_taker_fee_bps: float = 0.0
    short_taker_fee_bps: float = 0.0
    opportunity_type: str = "aligned"
    blocked: bool = False
    blocked_reasons: list[str] = field(default_factory=list)
    long_venue_index: int = 0
    short_venue_index: int = 0
    entry_notional_quote: float = 0.0
    # V1 keeps the matched pair notional on the average executable price.
    # The per-leg value remains an execution-quantity safety invariant.
    entry_max_leg_notional_quote: float = 0.0
    contract_price_consistency_ratio: float = 1.0
    contract_price_consistency_long_price: float = 0.0
    contract_price_consistency_short_price: float = 0.0
    candidate_revision_id: str = ""
    opportunity_lease_id: str = ""
    candidate_built_at_ms: int = 0
    # V1 parity fields (CONTRACT OPP-002: candidate identity + prewarm)
    pair_id: str = ""
    funding_timestamp_ms: int = 0
    first_funding_timestamp_ms: int = 0
    long_funding_timestamp_ms: int = 0
    short_funding_timestamp_ms: int = 0
    second_funding_timestamp_ms: int = 0
    # V1: FundingLeg — which side's funding settles first
    first_funding_leg: str = ""  # "long" or "short"
    entry_maker_leg: str = ""
    exit_maker_leg: str = ""
    transfer_state_at_entry: Optional[str] = None
    entry_liquidity_source_at_entry: Optional[str] = None
    long_volume_24h_quote: float = 0.0
    short_volume_24h_quote: float = 0.0
    long_open_interest_quote_at_entry: float = 0.0
    short_open_interest_quote_at_entry: float = 0.0
    long_entry_vwap: Optional[float] = None
    short_entry_vwap: Optional[float] = None
    entry_capacity_constrained: bool = False
    entry_target_quantity: float = 0.0
    long_max_executable_quantity: float = 0.0
    short_max_executable_quantity: float = 0.0
    entry_max_executable_quantity: float = 0.0
    entry_depth_shortfall_quantity: float = 0.0
    entry_max_executable_notional_quote: float = 0.0
    entry_depth_capped_at_entry: bool = False
    advisories: list[str] = field(default_factory=list)
    # V2: direction consistency and interval alignment (V1 fix)
    direction_consistent: bool = False
    interval_aligned: bool = False
    # Optional execution dependency marker. Empty means sizing/execution uses quote
    # and local-L2 gates rather than coarse sidecar perp liquidity.
    sizing_liquidity_source: str = ""
    # Shared economics schema: the legacy flat fields above remain dual-written
    # while these fields make every cost and haircut attributable.
    gross_signal_edge_bps: float = 0.0
    expected_exit_cross_bps: float = 0.0
    entry_fee_bps: float = 0.0
    exit_fee_bps: float = 0.0
    exit_slippage_bps: float = 0.0
    adverse_selection_bps: float = 0.0
    capital_buffer_bps: float = 0.0
    execution_buffer_bps: float = 0.0
    venue_risk_haircut_bps: float = 0.0
    transfer_or_inventory_bias_bps: float = 0.0
    expected_net_edge_bps: float = 0.0
    expected_profit_quote: float = 0.0
    worst_case_profit_quote: float = 0.0
    economics_observed_at_ms: int = 0
    # A hand-built or older-schema candidate must never inherit permission to
    # trade merely because the field was omitted.  Publishers explicitly set
    # this to true only after all v3 economics inputs were constructed.
    economics_complete: bool = False
    # Parser-boundary diagnostic for a V3 record that claimed completeness but
    # omitted/contradicted a required economics or sizing assertion.  It is
    # intentionally descriptive only: recovery and close workflows must never
    # reinterpret it as a reason to mutate an existing live position.
    economics_incomplete_reason: str = ""
    calculation_version: str = "v1_exact"
    model_epoch: str = "v1_exact"
    forecast_long_rate_bps: float = 0.0
    forecast_short_rate_bps: float = 0.0
    forecast_worst_funding_edge_bps: float = 0.0
    forecast_confidence: float = 0.0
    forecast_sample_count: int = 0
    forecast_shadow_age_ms: int = 0
    forecast_ready: bool = False
    forecast_distribution_stable: bool = False
    forecast_stability_reason: str = "not_calibrated"
    forecast_median_drift_bps: float = 0.0
    forecast_p90_drift_bps: float = 0.0
    forecast_source: str = "quoted_rate"

    # Explicit typed handoff for the shared executable entry envelope.  Built
    # by ranked selection before Local-L2/account truth and passed explicitly
    # to dispatch; never serialized in sidecar snapshots and never carried as
    # a dynamic private attribute.
    executable_envelope: ExecutableEntryEnvelope | None = None


def _candidate_field_contract_errors(
    candidate: dict[str, object],
    *,
    index: int,
) -> list[str]:
    """Strictly type-check every candidate field consumed after JSON parsing."""
    errors: list[str] = []
    # Dispatch envelopes are in-memory runtime state, never sidecar schema.
    # Keep this explicit rather than deriving the wire schema directly from
    # CandidateInput, which also carries the typed runtime handoff.
    known_fields = {
        candidate_field.name
        for candidate_field in dataclass_fields(CandidateInput)
        if candidate_field.name != "executable_envelope"
    }
    for field_name in candidate.keys() - known_fields:
        errors.append(f"candidate_unknown_field:{index}:{field_name}")
    for candidate_field in dataclass_fields(CandidateInput):
        field_name = candidate_field.name
        if field_name not in candidate:
            continue
        value = candidate[field_name]
        annotation = str(candidate_field.type).strip("'\"")
        valid = True
        if annotation == "float":
            valid = (
                type(value) in (int, float)
                and isfinite(float(value))
            )
        elif annotation == "int":
            valid = type(value) is int
        elif annotation == "bool":
            valid = type(value) is bool
        elif annotation == "str":
            valid = isinstance(value, str)
        elif annotation == "Optional[float]":
            valid = value is None or (
                type(value) in (int, float)
                and isfinite(float(value))
            )
        elif annotation == "Optional[str]":
            valid = value is None or isinstance(value, str)
        elif annotation == "list[str]":
            valid = isinstance(value, list) and all(
                isinstance(item, str) for item in value
            )
        elif annotation == "list[dict[str, object]]":
            valid = isinstance(value, list) and all(
                isinstance(item, dict) for item in value
            )
        if not valid:
            errors.append(f"candidate_field_type_invalid:{index}:{field_name}")
    return errors


@dataclass
class SidecarSnapshot:
    """The opportunity-input-snapshot published by the sidecar."""

    schema_version: int = SNAPSHOT_SCHEMA_VERSION
    published_at_ms: int = 0
    # Optional producer-ready watermark. Normal snapshots use their atomic
    # publication timestamp when this field is absent.
    ready_at_ms: int = 0
    market_observed_at_ms: int = 0
    funding_lifecycle: list[FundingLifecycle] = field(default_factory=list)
    market_lifecycle: list[MarketLifecycle] = field(default_factory=list)
    transfer_lifecycle: list[TransferLifecycle] = field(default_factory=list)
    liquidity_lifecycle: list[LiquidityLifecycle] = field(default_factory=list)
    degraded_venues: list[str] = field(default_factory=list)
    degraded_domains: list[str] = field(default_factory=list)
    degraded_symbols: dict[str, list[str]] = field(default_factory=dict)  # venue -> [symbol, ...]
    # V1 provider-depth semantics: provenance tracking
    source_mode: str = (
        ""  # "direct_market" | "direct_market_enriched" | "coarse_sidecar" | "sidecar_scan"
    )
    acquisition_mode: str = (
        ""  # "fresh_sidecar" | "last_good_sidecar" | "direct_market_view" | "unavailable"
    )
    # Candidate construction is a decision-time operation.  Its watermark is
    # deliberately distinct from refresh start and quote observation time so
    # a quote received during this refresh cannot be misclassified as future.
    candidate_build_observed_at_ms: int = 0
    candidate_build_diagnostics: dict[str, object] = field(default_factory=dict)
    quotes: dict[str, QuoteSnapshot] = field(default_factory=dict)
    candidates: list[CandidateInput] = field(default_factory=list)


@dataclass(frozen=True)
class SnapshotFreshnessDecision:
    """Freshness state together with the exact payload selected by that state."""

    freshness: SnapshotFreshness
    snapshot: SidecarSnapshot | None


def has_usable_market_payload(snapshot: SidecarSnapshot | None) -> bool:
    """Whether a snapshot has candidate input or an executable non-crossed BBO."""
    if snapshot is None or snapshot.acquisition_mode == "unavailable":
        return False
    # Quote-lease/WS-BBO entry providers can revalidate a sidecar candidate
    # before consulting the snapshot's coarse quote map.  A candidate is
    # therefore usable input payload even when that optional map is empty.
    if snapshot.candidates:
        return True
    for quote in snapshot.quotes.values():
        try:
            bid = float(quote.bid)
            ask = float(quote.ask)
        except (TypeError, ValueError, OverflowError):
            continue
        if isfinite(bid) and isfinite(ask) and bid > 0.0 and ask > 0.0 and bid <= ask:
            return True
    return False


def has_usable_funding_payload(snapshot: SidecarSnapshot | None) -> bool:
    """Whether funding-live has at least one independently usable input.

    A healthy BBO alone is spread evidence, not funding evidence.  Funding-live
    may count a snapshot as successful only when either a quote carries a
    complete funding schedule or an already-built, unblocked candidate carries
    the equivalent directed-pair proof.
    """
    if snapshot is None or snapshot.acquisition_mode == "unavailable":
        return False

    # Funding evidence is useful only for a settlement that is still ahead of
    # the snapshot's own evidence watermark.  Using the snapshot watermark
    # (rather than wall clock) keeps this predicate deterministic while
    # preventing an expired schedule from refreshing funding-live last-good
    # state and creating a misleading "healthy but no candidates" loop.
    evidence_at_ms = max(
        int(snapshot.ready_at_ms or 0),
        int(snapshot.published_at_ms or 0),
        int(snapshot.market_observed_at_ms or 0),
        int(snapshot.candidate_build_observed_at_ms or 0),
    )

    for candidate in snapshot.candidates:
        if candidate.blocked:
            continue
        if not candidate.symbol or not candidate.long_venue or not candidate.short_venue:
            continue
        if candidate.long_venue == candidate.short_venue:
            continue
        timestamps = (
            candidate.funding_timestamp_ms,
            candidate.first_funding_timestamp_ms,
            candidate.long_funding_timestamp_ms,
            candidate.short_funding_timestamp_ms,
        )
        if not all(
            type(value) is int and value > evidence_at_ms for value in timestamps
        ):
            continue
        edges = (
            candidate.funding_diff_bps,
            candidate.funding_edge_bps,
            candidate.expected_edge_bps,
            candidate.worst_case_edge_bps,
        )
        if all(
            type(value) in (int, float) and isfinite(float(value))
            for value in edges
        ):
            return True

    usable_quote_venues_by_symbol: dict[str, set[str]] = {}
    for quote in snapshot.quotes.values():
        values = (quote.bid, quote.ask, quote.funding_rate_bps)
        if not all(
            type(value) in (int, float) and isfinite(float(value))
            for value in values
        ):
            continue
        if float(quote.bid) <= 0.0 or float(quote.ask) <= 0.0 or quote.bid > quote.ask:
            continue
        if (
            type(quote.funding_timestamp_ms) is not int
            or quote.funding_timestamp_ms <= evidence_at_ms
        ):
            continue
        if type(quote.funding_interval_ms) is not int or quote.funding_interval_ms <= 0:
            continue
        venue = str(quote.venue or "").strip().lower()
        symbol = str(quote.symbol or "").strip().upper()
        if venue and symbol:
            usable_quote_venues_by_symbol.setdefault(symbol, set()).add(venue)
    return any(len(venues) >= 2 for venues in usable_quote_venues_by_symbol.values())


def decide_snapshot_freshness(
    snapshot: SidecarSnapshot | None,
    max_age_ms: int,
    now_ms: int,
    last_good: SidecarSnapshot | None = None,
    last_good_max_age_ms: int | None = None,
    market_max_age_ms: int | None = None,
    usable_payload: Callable[[SidecarSnapshot | None], bool] = has_usable_market_payload,
) -> SnapshotFreshnessDecision:
    """Evaluate freshness and atomically select the payload that justified it."""
    last_good_limit_ms = last_good_max_age_ms if last_good_max_age_ms is not None else max_age_ms
    market_limit_ms = market_max_age_ms if market_max_age_ms is not None else max_age_ms

    def _within_last_good_window(candidate: SidecarSnapshot | None) -> bool:
        if not usable_payload(candidate):
            return False
        assert candidate is not None
        publication_at_ms = int(candidate.ready_at_ms or candidate.published_at_ms or 0)
        publish_age_ms = now_ms - publication_at_ms
        if publish_age_ms < 0 or publish_age_ms > last_good_limit_ms:
            return False
        market_age_ms = now_ms - candidate.market_observed_at_ms
        if market_age_ms < 0 or market_age_ms > last_good_limit_ms:
            return False
        if candidate.candidate_build_observed_at_ms > now_ms:
            return False
        return True

    if snapshot is None:
        if _within_last_good_window(last_good):
            return SnapshotFreshnessDecision(SnapshotFreshness.LAST_GOOD_FALLBACK, last_good)
        return SnapshotFreshnessDecision(SnapshotFreshness.MISSING, None)

    if snapshot.acquisition_mode == "unavailable":
        return SnapshotFreshnessDecision(SnapshotFreshness.DEGRADED, snapshot)

    publication_at_ms = int(snapshot.ready_at_ms or snapshot.published_at_ms or 0)
    age_ms = now_ms - publication_at_ms
    if age_ms < 0:
        return SnapshotFreshnessDecision(SnapshotFreshness.STALE, snapshot)

    if age_ms > max_age_ms:
        if _within_last_good_window(snapshot):
            return SnapshotFreshnessDecision(SnapshotFreshness.LAST_GOOD_FALLBACK, snapshot)
        if _within_last_good_window(last_good):
            return SnapshotFreshnessDecision(SnapshotFreshness.LAST_GOOD_FALLBACK, last_good)
        return SnapshotFreshnessDecision(SnapshotFreshness.STALE, snapshot)

    market_age_ms = (
        now_ms - snapshot.market_observed_at_ms if snapshot.market_observed_at_ms > 0 else 0
    )
    if market_age_ms < 0 or snapshot.candidate_build_observed_at_ms > now_ms:
        return SnapshotFreshnessDecision(SnapshotFreshness.STALE, snapshot)
    if market_age_ms > market_limit_ms:
        # A broad-universe collection timestamp is producer-health evidence,
        # not a substitute for a selected pair's quote. Do not exchange a
        # current, publish-fresh snapshot for a long-lived global last-good
        # snapshot here: runtime may retain this snapshot only for targeted
        # candidate revalidation, where both final legs must still prove fresh
        # BBO/L2/OI evidence. Publish stale/missing recovery above retains the
        # V1 last-good behavior.
        return SnapshotFreshnessDecision(SnapshotFreshness.STALE, snapshot)

    if not usable_payload(snapshot):
        return SnapshotFreshnessDecision(SnapshotFreshness.DEGRADED, snapshot)

    if snapshot.degraded_venues or snapshot.degraded_domains or snapshot.degraded_symbols:
        return SnapshotFreshnessDecision(SnapshotFreshness.DEGRADED, snapshot)

    return SnapshotFreshnessDecision(SnapshotFreshness.FRESH, snapshot)


def evaluate_snapshot_freshness(
    snapshot: SidecarSnapshot | None,
    max_age_ms: int,
    now_ms: int,
    last_good: SidecarSnapshot | None = None,
    last_good_max_age_ms: int | None = None,
    market_max_age_ms: int | None = None,
) -> SnapshotFreshness:
    """Evaluate snapshot freshness per V1 OpportunityInputDomainState semantics.

    V1 anchors: src/opportunity_input/types.rs  OpportunityInputDomainState
                 src/opportunity_input/sidecar_snapshot.rs  snapshot freshness evaluation

    Priority order:
    1. MISSING — no snapshot at all
    2. LAST_GOOD_FALLBACK — current publication is stale/missing but a recent
       valid one exists
    3. STALE — current publication or broad-market observation exceeds its
       respective budget
    4. DEGRADED — snapshot exists within max_age but has degraded venues/domains
    5. FRESH — snapshot exists within max_age and has no degradations
    """
    return decide_snapshot_freshness(
        snapshot=snapshot,
        max_age_ms=max_age_ms,
        now_ms=now_ms,
        last_good=last_good,
        last_good_max_age_ms=last_good_max_age_ms,
        market_max_age_ms=market_max_age_ms,
    ).freshness
