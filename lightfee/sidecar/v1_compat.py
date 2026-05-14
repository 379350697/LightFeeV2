"""V1 snapshot → V2 snapshot format conversion.

TEMPORARY compatibility shim — converts the V1 Rust sidecar snapshot format
(schema_version=1) into the V2 Python dataclass format.

When the V2 sidecar ExchangeSource is fully implemented, DELETE THIS FILE and
remove the schema_version==1 branch in publisher._dict_to_snapshot().
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# V1 lifecycle dict → V2 per-venue lifecycle list
# ---------------------------------------------------------------------------

def _v1_lifecycle_to_v2(lifecycle: dict, venues: list[str]) -> list[dict]:
    """V1 lifecycle: {observed_at_ms, age_ms, state, coverage_total, coverage_usable}
    V2 lifecycle: [{venue, observed_at_ms, symbol_count}, ...]

    V2 expects per-venue granularity; V1 has domain-level aggregates.
    Spread the observed_at_ms across all known venues.
    """
    if not isinstance(lifecycle, dict):
        return []
    observed_at_ms = lifecycle.get("observed_at_ms", 0)
    coverage = lifecycle.get("coverage_usable", 0)
    return [
        {"venue": v, "observed_at_ms": observed_at_ms, "symbol_count": coverage}
        for v in venues
    ]


# ---------------------------------------------------------------------------
# V1 quote → V2 QuoteSnapshot
# ---------------------------------------------------------------------------

def _v1_quote_to_v2(venue: str, symbol: str, raw: dict) -> dict:
    """Convert a single V1 quote entry to V2 QuoteSnapshot kwargs.

    V1 keys: best_bid, best_ask, mark_price, funding_rate, funding_timestamp_ms
    V2 keys: bid, ask, mark_price, funding_rate_bps, funding_timestamp_ms,
             bid_size, ask_size, index_price, volume_24h_quote, open_interest,
             venue, symbol
    """
    return {
        "venue": venue,
        "symbol": symbol,
        "bid": raw.get("best_bid", 0.0),
        "ask": raw.get("best_ask", 0.0),
        "bid_size": raw.get("best_bid_size", 0.0),
        "ask_size": raw.get("best_ask_size", 0.0),
        "mark_price": raw.get("mark_price", 0.0),
        "index_price": raw.get("index_price", 0.0),
        "funding_rate_bps": raw.get("funding_rate", 0.0),
        "funding_timestamp_ms": raw.get("funding_timestamp_ms", 0),
        "volume_24h_quote": raw.get("volume_24h_quote", 0.0),
        "open_interest": raw.get("open_interest", 0.0),
    }


def _v1_quotes_to_v2(raw_quotes: dict) -> dict[str, dict]:
    """V1 quotes: {venue: {symbol: {best_bid, best_ask, ...}}}
    V2 quotes: {"venue:symbol": {venue, symbol, bid, ask, ...}}
    """
    result: dict[str, dict] = {}
    if not isinstance(raw_quotes, dict):
        return result
    for venue, symbols in raw_quotes.items():
        if not isinstance(symbols, dict):
            continue
        for symbol, fields in symbols.items():
            if not isinstance(fields, dict):
                continue
            key = f"{venue}:{symbol}"
            result[key] = _v1_quote_to_v2(venue, symbol, fields)
    return result


# ---------------------------------------------------------------------------
# V1 candidate → V2 CandidateInput
# ---------------------------------------------------------------------------

def _v1_candidate_to_v2(raw: dict) -> dict:
    """Convert a single V1 candidate to V2 CandidateInput kwargs.

    V1 keys: symbol, long_venue, short_venue, funding_edge_bps,
             long_funding_timestamp_ms, short_funding_timestamp_ms,
             pair_id, funding_timestamp_ms, first_funding_timestamp_ms,
             direction_consistent, interval_aligned, rank, origin_tags,
             hint_source, quality_notes, quality_penalty_bps, ...
    V2 keys: long_venue, short_venue, symbol, funding_diff_bps,
             funding_edge_bps, expected_edge_bps, worst_case_edge_bps,
             ranking_edge_bps, transfer_bias_bps, opportunity_type,
             blocked, blocked_reasons, pair_id, funding_timestamp_ms,
             first_funding_timestamp_ms, ...
    """
    edge = float(raw.get("funding_edge_bps", 0.0))
    penalty = float(raw.get("quality_penalty_bps", 0.0))
    return {
        "long_venue": raw.get("long_venue", ""),
        "short_venue": raw.get("short_venue", ""),
        "symbol": raw.get("symbol", ""),
        "funding_diff_bps": edge,  # V1 only emits edge, no separate diff field
        "funding_edge_bps": edge,
        "expected_edge_bps": edge - penalty,
        "worst_case_edge_bps": edge - penalty,
        "ranking_edge_bps": edge - penalty,
        "transfer_bias_bps": 0.0,
        "opportunity_type": "aligned",
        "blocked": False,
        "blocked_reasons": [],
        "long_venue_index": 0,
        "short_venue_index": 0,
        "entry_notional_quote": 50.0,  # V1 fixed_live_entry_notional_quote — DELETE with this file
        # V1 parity: preserve candidate identity + prewarm fields
        "pair_id": raw.get("pair_id", ""),
        "funding_timestamp_ms": int(raw.get("funding_timestamp_ms", 0)),
        "first_funding_timestamp_ms": int(raw.get("first_funding_timestamp_ms", 0)),
    }


def _v1_candidates_to_v2(raw_candidates: list) -> list[dict]:
    """Convert V1 candidate list to V2 CandidateInput list."""
    if not isinstance(raw_candidates, list):
        return []
    return [_v1_candidate_to_v2(c) for c in raw_candidates if isinstance(c, dict)]


# ---------------------------------------------------------------------------
# Top-level V1 → V2 conversion
# ---------------------------------------------------------------------------

def convert_v1_snapshot_to_v2(raw: dict) -> dict:
    """Convert a V1 snapshot dict (schema_version=1) to V2-compatible dict.

    The returned dict can be passed directly to SidecarSnapshot(**kw) after
    the caller constructs the dataclass instances from the nested dicts.

    Returns a dict with V2-compatible structure:
      schema_version, published_at_ms, market_observed_at_ms,
      funding_lifecycle (list), market_lifecycle (list),
      transfer_lifecycle (list), liquidity_lifecycle (list),
      degraded_venues, degraded_domains, source_mode, acquisition_mode,
      quotes (dict), candidates (list)
    """
    venues = _collect_venues(raw)

    return {
        "schema_version": 2,  # mark as converted
        "published_at_ms": raw.get("published_at_ms", 0),
        "market_observed_at_ms": raw.get("market_observed_at_ms", 0),
        "funding_lifecycle": _v1_lifecycle_to_v2(
            raw.get("funding_lifecycle", {}), venues
        ),
        "market_lifecycle": _v1_lifecycle_to_v2(
            raw.get("market_lifecycle", {}), venues
        ),
        "transfer_lifecycle": [],
        "liquidity_lifecycle": _v1_lifecycle_to_v2(
            raw.get("perp_liquidity_lifecycle", {}), venues
        ),
        "degraded_venues": raw.get("degraded_venues", []),
        "degraded_domains": [],
        "source_mode": raw.get("source_mode", ""),
        "acquisition_mode": raw.get("source_mode", ""),
        "quotes": _v1_quotes_to_v2(raw.get("quotes", {})),
        "candidates": _v1_candidates_to_v2(raw.get("candidates", [])),
    }


def _collect_venues(raw: dict) -> list[str]:
    """Extract venue list from V1 quotes or candidates."""
    quotes = raw.get("quotes", {})
    if isinstance(quotes, dict) and quotes:
        return list(quotes.keys())
    # Fallback: known venue list
    return ["binance", "okx", "bybit", "hyperliquid", "bitget", "gate", "aster"]
