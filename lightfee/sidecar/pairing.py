"""Same-symbol venue pair building matching Rust pairing logic."""

from __future__ import annotations

from lightfee.engine.entry_local_l2 import make_candidate_pair_id
from lightfee.sidecar.snapshot import CandidateInput, QuoteSnapshot


def build_same_symbol_pairs(
    quotes: dict[str, QuoteSnapshot],
    symbols: list[str],
) -> list[CandidateInput]:
    """Build directed (long, short) pairs for each symbol across venues.

    Matches Rust pairing: lower-funding venue becomes long side,
    higher-funding venue becomes short side.
    """
    candidates: list[CandidateInput] = []

    for symbol in symbols:
        venue_quotes: list[QuoteSnapshot] = []
        for q in quotes.values():
            if q.symbol.upper() == symbol.upper():
                venue_quotes.append(q)

        if len(venue_quotes) < 2:
            continue

        for i, long_q in enumerate(venue_quotes):
            for j, short_q in enumerate(venue_quotes):
                if i == j:
                    continue
                if short_q.funding_rate_bps <= long_q.funding_rate_bps:
                    continue

                funding_diff = short_q.funding_rate_bps - long_q.funding_rate_bps
                reference_mid = (short_q.bid + long_q.ask) / 2.0 if reference_mid_valid(long_q, short_q) else 1.0
                raw_cross_bps = ((short_q.bid - long_q.ask) / reference_mid) * 10000.0 if reference_mid > 0 else 0.0

                pair_id = make_candidate_pair_id(symbol, long_q.venue, short_q.venue)
                # First funding timestamp is the earlier of the two venue timestamps
                first_funding_ts = min(
                    long_q.funding_timestamp_ms, short_q.funding_timestamp_ms,
                )

                candidates.append(
                    CandidateInput(
                        long_venue=long_q.venue,
                        short_venue=short_q.venue,
                        symbol=symbol,
                        funding_diff_bps=funding_diff,
                        funding_edge_bps=funding_diff,
                        expected_edge_bps=funding_diff + raw_cross_bps,
                        worst_case_edge_bps=funding_diff + raw_cross_bps,
                        ranking_edge_bps=funding_diff + raw_cross_bps,
                        pair_id=pair_id,
                        funding_timestamp_ms=first_funding_ts,
                        first_funding_timestamp_ms=first_funding_ts,
                    )
                )

    return sorted(candidates, key=lambda c: c.ranking_edge_bps, reverse=True)


def check_stale_snapshot(snapshot_published_at_ms: int, max_age_ms: int, now_ms: int) -> bool:
    """Return True if the snapshot is too old to use."""
    return (now_ms - snapshot_published_at_ms) > max_age_ms


def reference_mid_valid(long_q: QuoteSnapshot, short_q: QuoteSnapshot) -> bool:
    return long_q.ask > 0 and short_q.bid > 0
