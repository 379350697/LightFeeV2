"""Atomic snapshot publisher for sidecar output."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from lightfee.sidecar.snapshot import SidecarSnapshot


def publish_snapshot(snapshot: SidecarSnapshot, path: str | Path) -> None:
    """Write snapshot atomically: temp file, flush, replace."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mktemp(dir=str(target.parent), prefix=".snapshot-"))
    try:
        data = _snapshot_to_dict(snapshot)
        content = json.dumps(data, ensure_ascii=False, indent=2)
        with open(tmp, "w") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def load_snapshot(path: str | Path) -> SidecarSnapshot | None:
    """Load and validate a sidecar snapshot. Returns None if missing or malformed."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(p) as f:
            data = json.loads(f.read())
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    if "schema_version" not in data:
        return None
    return _dict_to_snapshot(data)


def _snapshot_to_dict(s: SidecarSnapshot) -> dict:
    return {
        "schema_version": s.schema_version,
        "published_at_ms": s.published_at_ms,
        "market_observed_at_ms": s.market_observed_at_ms,
        "funding_lifecycle": [
            {"venue": fl.venue, "observed_at_ms": fl.observed_at_ms, "symbol_count": fl.symbol_count,
             "coverage_usable": fl.coverage_usable, "degraded_reason": fl.degraded_reason}
            for fl in s.funding_lifecycle
        ],
        "market_lifecycle": [
            {"venue": ml.venue, "observed_at_ms": ml.observed_at_ms, "symbol_count": ml.symbol_count,
             "coverage_usable": ml.coverage_usable, "degraded_reason": ml.degraded_reason}
            for ml in s.market_lifecycle
        ],
        "transfer_lifecycle": [
            {"from_venue": tl.from_venue, "to_venue": tl.to_venue, "observed_at_ms": tl.observed_at_ms,
             "coverage_usable": tl.coverage_usable, "degraded_reason": tl.degraded_reason}
            for tl in s.transfer_lifecycle
        ],
        "liquidity_lifecycle": [
            {"venue": ll.venue, "observed_at_ms": ll.observed_at_ms, "symbol_count": ll.symbol_count,
             "coverage_usable": ll.coverage_usable, "degraded_reason": ll.degraded_reason}
            for ll in s.liquidity_lifecycle
        ],
        "degraded_venues": list(s.degraded_venues),
        "degraded_domains": list(s.degraded_domains),
        "degraded_symbols": {k: list(v) for k, v in s.degraded_symbols.items()},
        "source_mode": s.source_mode,
        "acquisition_mode": s.acquisition_mode,
        "quotes": {
            k: {
                "venue": q.venue,
                "symbol": q.symbol,
                "bid": q.bid,
                "ask": q.ask,
                "bid_size": q.bid_size,
                "ask_size": q.ask_size,
                "funding_rate_bps": q.funding_rate_bps,
                "funding_timestamp_ms": q.funding_timestamp_ms,
                "mark_price": q.mark_price,
                "index_price": q.index_price,
                "volume_24h_quote": q.volume_24h_quote,
                "open_interest": q.open_interest,
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
                "transfer_bias_bps": c.transfer_bias_bps,
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
                "first_funding_leg": c.first_funding_leg,
                "direction_consistent": c.direction_consistent,
                "interval_aligned": c.interval_aligned,
            }
            for c in s.candidates
        ],
    }


def _dict_to_snapshot(d: dict) -> SidecarSnapshot:
    # --- V1 compat: convert V1 Rust sidecar format to V2 (see v1_compat.py) ---
    if d.get("schema_version") == 1:
        from lightfee.sidecar.v1_compat import convert_v1_snapshot_to_v2
        d = convert_v1_snapshot_to_v2(d)
    # --- end V1 compat ---

    from lightfee.sidecar.snapshot import (
        CandidateInput,
        FundingLifecycle,
        LiquidityLifecycle,
        MarketLifecycle,
        QuoteSnapshot,
        TransferLifecycle,
    )

    quotes_raw = d.get("quotes", {})

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
        pair_id = str(c.get("pair_id", "") or "")
        symbol = str(c.get("symbol", ""))
        long_ven = str(c.get("long_venue", ""))
        short_ven = str(c.get("short_venue", ""))

        if not pair_id and symbol and long_ven and short_ven:
            pair_id = f"{symbol.lower()}:{long_ven}->{short_ven}"

        ff_ts = int(c.get("first_funding_timestamp_ms", 0) or 0)
        f_ts = int(c.get("funding_timestamp_ms", 0) or 0)
        long_fts = int(c.get("long_funding_timestamp_ms", 0) or 0)
        short_fts = int(c.get("short_funding_timestamp_ms", 0) or 0)

        # Derive long/short timestamps from quotes if missing in raw candidate
        if long_fts <= 0 or short_fts <= 0:
            for venue, target in [(long_ven, "long"), (short_ven, "short")]:
                qkey = f"{venue}:{symbol}"
                q = quotes_raw.get(qkey, {})
                if isinstance(q, dict):
                    qts = int(q.get("funding_timestamp_ms", 0) or 0)
                    if qts > 0:
                        if target == "long" and long_fts <= 0:
                            long_fts = qts
                        elif target == "short" and short_fts <= 0:
                            short_fts = qts

        # Derive first_funding_timestamp_ms from per-leg timestamps (V1: min(long, short))
        if ff_ts <= 0 and long_fts > 0 and short_fts > 0:
            ff_ts = min(long_fts, short_fts)
        elif ff_ts <= 0:
            # Fallback: derive from quotes
            ts_candidates: list[int] = []
            for venue in (long_ven, short_ven):
                qkey = f"{venue}:{symbol}"
                q = quotes_raw.get(qkey, {})
                if isinstance(q, dict):
                    qts = int(q.get("funding_timestamp_ms", 0) or 0)
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
            candidate.first_funding_leg = (
                "long" if long_fts <= short_fts else "short"
            )

        # Fail-closed: candidates without usable funding timestamp are not tradeable.
        # V1 never emits a tradeable candidate with first_funding_timestamp_ms=0.
        if candidate.first_funding_timestamp_ms <= 0:
            candidate.blocked = True
            candidate.blocked_reasons = list(candidate.blocked_reasons) + [
                "missing_candidate_identity_or_funding_timestamp"
            ]

        return candidate

    return SidecarSnapshot(
        schema_version=d.get("schema_version", 0),
        published_at_ms=d.get("published_at_ms", 0),
        market_observed_at_ms=d.get("market_observed_at_ms", 0),
        funding_lifecycle=[FundingLifecycle(**fl) for fl in d.get("funding_lifecycle", [])],
        market_lifecycle=[MarketLifecycle(**ml) for ml in d.get("market_lifecycle", [])],
        transfer_lifecycle=[TransferLifecycle(**tl) for tl in d.get("transfer_lifecycle", [])],
        liquidity_lifecycle=[LiquidityLifecycle(**ll) for ll in d.get("liquidity_lifecycle", [])],
        degraded_venues=d.get("degraded_venues", []),
        degraded_domains=d.get("degraded_domains", []),
        degraded_symbols=_parse_degraded_symbols(d.get("degraded_symbols", {})),
        source_mode=d.get("source_mode", ""),
        acquisition_mode=d.get("acquisition_mode", ""),
        quotes={k: QuoteSnapshot(**v) for k, v in quotes_raw.items()},
        candidates=[_enrich_candidate(c) for c in d.get("candidates", [])],
    )


def _parse_degraded_symbols(raw: object) -> dict[str, list[str]]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, list[str]] = {}
    for k, v in raw.items():
        if isinstance(v, list):
            result[str(k)] = [str(x) for x in v]
    return result
