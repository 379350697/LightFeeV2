"""Same-symbol venue pair building with V1 parity identity and timing fields."""

from __future__ import annotations

from lightfee.engine.entry_local_l2 import make_candidate_pair_id
from lightfee.sidecar.snapshot import CandidateInput, QuoteSnapshot


# V1: fixed live/paper entry notional — non-zero to avoid ZERO_ORDER_SIZE gate
_DEFAULT_ENTRY_NOTIONAL_QUOTE = 50.0
_INTERVAL_ALIGNED_THRESHOLD_MS = 60_000


def build_same_symbol_pairs(
    quotes: dict[str, QuoteSnapshot],
    symbols: list[str],
) -> list[CandidateInput]:
    """Build directed (long, short) pairs for each symbol across venues.

    V2 fixes:
    - direction_consistent uses long/short mid prices, not ask
    - interval_aligned = abs(long_ts - short_ts) <= 60_000
    - pair_id, first_funding_leg, second_funding_timestamp_ms always populated
    - entry_notional_quote always non-zero
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

                # V2 fix: use mid prices for reference_mid and direction_consistent
                long_mid = (long_q.bid + long_q.ask) / 2.0
                short_mid = (short_q.bid + short_q.ask) / 2.0
                reference_mid = (long_mid + short_mid) / 2.0 if long_mid > 0 and short_mid > 0 else 1.0

                raw_cross_bps = 0.0
                if reference_mid > 0 and long_q.ask > 0 and short_q.bid > 0:
                    raw_cross_bps = ((short_q.bid - long_q.ask) / reference_mid) * 10000.0

                # V2 fix: direction_consistent using mid prices
                direction_consistent = (
                    funding_diff > 0
                    and short_mid >= long_mid
                    and long_mid > 0
                    and short_mid > 0
                )

                long_ts = long_q.funding_timestamp_ms
                short_ts = short_q.funding_timestamp_ms

                # Timing fields
                interval_aligned = abs(long_ts - short_ts) <= _INTERVAL_ALIGNED_THRESHOLD_MS if long_ts > 0 and short_ts > 0 else False
                first_ts = min(long_ts, short_ts)
                second_ts = max(long_ts, short_ts)
                first_leg = "long" if long_ts <= short_ts else "short" if long_ts > 0 and short_ts > 0 else ""
                opportunity_type = "aligned" if interval_aligned else "staggered"

                pair_id = make_candidate_pair_id(symbol, long_q.venue, short_q.venue)

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
                        funding_timestamp_ms=first_ts,
                        first_funding_timestamp_ms=first_ts,
                        long_funding_timestamp_ms=long_ts,
                        short_funding_timestamp_ms=short_ts,
                        second_funding_timestamp_ms=second_ts,
                        first_funding_leg=first_leg,
                        direction_consistent=direction_consistent,
                        interval_aligned=interval_aligned,
                        opportunity_type=opportunity_type,
                        entry_notional_quote=_DEFAULT_ENTRY_NOTIONAL_QUOTE,
                    )
                )

    return sorted(candidates, key=lambda c: c.ranking_edge_bps, reverse=True)


def check_stale_snapshot(snapshot_published_at_ms: int, max_age_ms: int, now_ms: int) -> bool:
    """Return True if the snapshot is too old to use."""
    return (now_ms - snapshot_published_at_ms) > max_age_ms


def reference_mid_valid(long_q: QuoteSnapshot, short_q: QuoteSnapshot) -> bool:
    return long_q.ask > 0 and short_q.bid > 0
