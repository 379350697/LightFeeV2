"""Atomic publisher for spread-reversion snapshots."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from lightfee.spread.models import SpreadReversionCandidate, SpreadSnapshot


def publish_spread_snapshot(snapshot: SpreadSnapshot, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".spread-snapshot-")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        content = json.dumps(_snapshot_to_dict(snapshot), ensure_ascii=False, indent=2)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def load_spread_snapshot(path: str | Path) -> SpreadSnapshot | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    if int(data.get("schema_version", 0) or 0) != 1:
        return None
    candidates = []
    for raw in data.get("candidates", []) or []:
        if isinstance(raw, dict):
            candidates.append(SpreadReversionCandidate(**raw))
    return SpreadSnapshot(
        schema_version=1,
        published_at_ms=int(data.get("published_at_ms", 0) or 0),
        market_observed_at_ms=int(data.get("market_observed_at_ms", 0) or 0),
        snapshot_path=str(data.get("snapshot_path", "") or ""),
        degraded_venues=list(data.get("degraded_venues", []) or []),
        degraded_symbols=dict(data.get("degraded_symbols", {}) or {}),
        candidates=candidates,
    )


def _snapshot_to_dict(snapshot: SpreadSnapshot) -> dict:
    return {
        "schema_version": snapshot.schema_version,
        "published_at_ms": snapshot.published_at_ms,
        "market_observed_at_ms": snapshot.market_observed_at_ms,
        "snapshot_path": snapshot.snapshot_path,
        "degraded_venues": list(snapshot.degraded_venues),
        "degraded_symbols": {
            str(key): list(value)
            for key, value in snapshot.degraded_symbols.items()
        },
        "candidates": [
            {
                "candidate_id": c.candidate_id,
                "symbol": c.symbol,
                "long_venue": c.long_venue,
                "short_venue": c.short_venue,
                "spread_mid_bps": c.spread_mid_bps,
                "executable_spread_bps": c.executable_spread_bps,
                "rolling_mean_bps": c.rolling_mean_bps,
                "rolling_std_bps": c.rolling_std_bps,
                "z_score": c.z_score,
                "net_edge_bps": c.net_edge_bps,
                "sample_count": c.sample_count,
                "signal_ts_ms": c.signal_ts_ms,
                "long_quote_ts_ms": c.long_quote_ts_ms,
                "short_quote_ts_ms": c.short_quote_ts_ms,
                "entry_notional_quote": c.entry_notional_quote,
                "capacity_quote": c.capacity_quote,
                "signal_status": c.signal_status,
                "strategy_bucket": c.strategy_bucket,
                "fee_bps": c.fee_bps,
                "slippage_reserve_bps": c.slippage_reserve_bps,
                "adverse_selection_buffer_bps": c.adverse_selection_buffer_bps,
                "funding_carry_cost_bps": c.funding_carry_cost_bps,
                "quote_skew_ms": c.quote_skew_ms,
                "funding_timestamp_ms": c.funding_timestamp_ms,
                "first_funding_timestamp_ms": c.first_funding_timestamp_ms,
            }
            for c in snapshot.candidates
        ],
    }
