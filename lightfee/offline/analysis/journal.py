"""Journal analysis: venue stats, failure rates, latency, PnL summaries."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class VenueOrderStats:
    venue: str
    order_count: int = 0
    fill_count: int = 0
    failure_count: int = 0
    total_latency_ms: int = 0
    max_latency_ms: int = 0
    min_latency_ms: int = 9223372036854775807  # i64 max


@dataclass
class DailyPnLSummary:
    date: str
    total_pnl_quote: float = 0.0
    total_fee_quote: float = 0.0
    entry_count: int = 0
    exit_count: int = 0
    by_venue: dict[str, float] = field(default_factory=dict)
    by_symbol: dict[str, float] = field(default_factory=dict)


def analyze_journal_records(
    records: list[dict],
) -> tuple[dict[str, VenueOrderStats], DailyPnLSummary]:
    """Analyze journal records for order stats and PnL."""
    venue_stats: dict[str, VenueOrderStats] = {}
    daily = DailyPnLSummary(date="")

    for record in records:
        kind = record.get("kind", "")
        payload = record.get("payload", {})

        if kind == "entry.opened":
            daily.entry_count += 1
            daily.total_fee_quote += payload.get("entry_fee_quote", 0.0)

        elif kind == "exit.closed":
            daily.exit_count += 1
            daily.total_pnl_quote += payload.get("net_quote", 0.0)
            daily.total_fee_quote += payload.get("exit_fee_quote", 0.0)

        elif kind == "order.submitted":
            venue = payload.get("venue", "unknown")
            stats = venue_stats.setdefault(venue, VenueOrderStats(venue=venue))
            stats.order_count += 1

        elif kind == "order.filled":
            venue = payload.get("venue", "unknown")
            stats = venue_stats.setdefault(venue, VenueOrderStats(venue=venue))
            stats.fill_count += 1
            latency = payload.get("latency_ms", 0)
            stats.total_latency_ms += latency
            stats.max_latency_ms = max(stats.max_latency_ms, latency)
            stats.min_latency_ms = min(stats.min_latency_ms, latency)

        elif kind == "order.failed":
            venue = payload.get("venue", "unknown")
            stats = venue_stats.setdefault(venue, VenueOrderStats(venue=venue))
            stats.failure_count += 1

    return venue_stats, daily
