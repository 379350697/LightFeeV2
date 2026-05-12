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
            {"venue": fl.venue, "observed_at_ms": fl.observed_at_ms, "symbol_count": fl.symbol_count}
            for fl in s.funding_lifecycle
        ],
        "market_lifecycle": [
            {"venue": ml.venue, "observed_at_ms": ml.observed_at_ms, "symbol_count": ml.symbol_count}
            for ml in s.market_lifecycle
        ],
        "transfer_lifecycle": [
            {"from_venue": tl.from_venue, "to_venue": tl.to_venue, "observed_at_ms": tl.observed_at_ms}
            for tl in s.transfer_lifecycle
        ],
        "liquidity_lifecycle": [
            {"venue": ll.venue, "observed_at_ms": ll.observed_at_ms, "symbol_count": ll.symbol_count}
            for ll in s.liquidity_lifecycle
        ],
        "degraded_venues": list(s.degraded_venues),
        "degraded_domains": list(s.degraded_domains),
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
        source_mode=d.get("source_mode", ""),
        acquisition_mode=d.get("acquisition_mode", ""),
        quotes={k: QuoteSnapshot(**v) for k, v in d.get("quotes", {}).items()},
        candidates=[CandidateInput(**c) for c in d.get("candidates", [])],
    )
