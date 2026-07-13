"""Red test: V1 schema=1 sidecar candidate identity fields.

V1 anchors:
- src/strategy_intelligence/discovery.rs:839-910 build_candidate_from_precomputed_pair
- src/strategy_intelligence/discovery.rs:1330-1400 funding_opportunity_profile_from_inputs
- src/strategy_intelligence/discovery.rs:1611 pair_id format

Validates that when a V1 schema=1 snapshot has candidates WITHOUT
first_funding_timestamp_ms/pair_id but WITH long/short funding timestamps
in either the candidate or the associated quotes, the V2 load_snapshot +
discover_tradeable_candidates pipeline correctly derives the fields.

RED test: previously candidates would have first_funding_timestamp_ms=0
and pair_id="", causing entry-L2 prewarm to permanently block.
"""

import json
import tempfile
from pathlib import Path

import pytest

from lightfee.sidecar.publisher import load_snapshot
from lightfee.config.schema import StrategyConfig
from lightfee.strategy.discovery import discover_tradeable_candidates


# --- V1 schema=1 fixture mimicking Rust sidecar output ---
# Raw candidates have long_funding_timestamp_ms / short_funding_timestamp_ms
# but do NOT have first_funding_timestamp_ms or pair_id (Rust sidecar may omit).
# Quotes carry per-symbol funding_timestamp_ms for the venues.

def _make_v1_schema1_snapshot() -> dict:
    """Schema-1 snapshot where candidates lack pair_id/first_funding_timestamp_ms.

    Long/Short funding timestamps ARE available on the raw candidate,
    simulating the Rust sidecar providing per-leg timestamps.
    """
    return {
        "schema_version": 1,
        "published_at_ms": 1700000000000,
        "market_observed_at_ms": 1700000000000,
        "funding_lifecycle": {
            "observed_at_ms": 1700000000000,
            "age_ms": 500,
            "state": "fresh",
            "coverage_total": 50,
            "coverage_usable": 45,
        },
        "market_lifecycle": {
            "observed_at_ms": 1700000000000,
            "age_ms": 500,
            "state": "fresh",
            "coverage_total": 50,
            "coverage_usable": 45,
        },
        "perp_liquidity_lifecycle": {
            "observed_at_ms": 1700000000000,
            "age_ms": 500,
            "state": "fresh",
            "coverage_total": 50,
            "coverage_usable": 45,
        },
        "degraded_venues": [],
        "source_mode": "coarse_sidecar",
        "quotes": {
            "gate": {
                "SAGAUSDT": {
                    "best_bid": 0.5,
                    "best_ask": 0.51,
                    "best_bid_size": 10000,
                    "best_ask_size": 10000,
                    "mark_price": 0.505,
                    "index_price": 0.505,
                    "funding_rate": 0.0001,
                    "funding_timestamp_ms": 1700000900000,
                    "volume_24h_quote": 5000000,
                    "open_interest": 1000000,
                }
            },
            "bitget": {
                "SAGAUSDT": {
                    "best_bid": 0.5,
                    "best_ask": 0.511,
                    "best_bid_size": 8000,
                    "best_ask_size": 8000,
                    "mark_price": 0.505,
                    "index_price": 0.505,
                    "funding_rate": -0.00005,
                    "funding_timestamp_ms": 1700001000000,
                    "volume_24h_quote": 4000000,
                    "open_interest": 800000,
                }
            },
            "binance": {
                "BTCUSDT": {
                    "best_bid": 50000,
                    "best_ask": 50001,
                    "best_bid_size": 10,
                    "best_ask_size": 10,
                    "mark_price": 50000.5,
                    "index_price": 50000,
                    "funding_rate": 0.0001,
                    "funding_timestamp_ms": 1700000800000,
                    "volume_24h_quote": 100000000,
                    "open_interest": 50000,
                }
            },
            "okx": {
                "BTCUSDT": {
                    "best_bid": 50002,
                    "best_ask": 50003,
                    "best_bid_size": 10,
                    "best_ask_size": 10,
                    "mark_price": 50002.5,
                    "index_price": 50002,
                    "funding_rate": -0.00005,
                    "funding_timestamp_ms": 1700000950000,
                    "volume_24h_quote": 80000000,
                    "open_interest": 40000,
                }
            },
        },
        "candidates": [
            # Candidate with per-leg timestamps but no pre-computed identity
            {
                "symbol": "BTCUSDT",
                "long_venue": "binance",
                "short_venue": "okx",
                "funding_edge_bps": 12.0,
                "long_funding_timestamp_ms": 1700000800000,
                "short_funding_timestamp_ms": 1700000950000,
                # NO pair_id
                # NO first_funding_timestamp_ms
                "direction_consistent": True,
                "interval_aligned": True,
                "rank": 1,
                "origin_tags": ["coarse_scan"],
                "quality_notes": [],
                "quality_penalty_bps": 1.0,
            },
            # SAGA candidate — simulates cloud pending entry scenario
            {
                "symbol": "SAGAUSDT",
                "long_venue": "gate",
                "short_venue": "bitget",
                "funding_edge_bps": 15.0,
                "long_funding_timestamp_ms": 1700000900000,
                "short_funding_timestamp_ms": 1700001000000,
                # NO pair_id
                # NO first_funding_timestamp_ms
                "direction_consistent": True,
                "interval_aligned": True,
                "rank": 2,
                "origin_tags": ["coarse_scan"],
                "quality_notes": [],
                "quality_penalty_bps": 0.5,
            },
        ],
    }


class TestV1Schema1CandidateIdentity:
    """Red test: schema-1 candidates must have identity fields after load_snapshot."""

    def test_schema1_candidates_have_pair_id(self):
        """Every non-blocked candidate must have a non-empty pair_id."""
        raw = _make_v1_schema1_snapshot()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            path.write_text(json.dumps(raw))
            snap = load_snapshot(path)

        assert snap is not None
        assert len(snap.candidates) == 2

        for c in snap.candidates:
            if not c.blocked:
                assert c.pair_id, (
                    f"Non-blocked candidate {c.symbol}:{c.long_venue}->{c.short_venue} "
                    f"has empty pair_id"
                )
                # Verify V1 format: {symbol.lower()}:{long}->{short}
                expected = f"{c.symbol.lower()}:{c.long_venue}->{c.short_venue}"
                assert c.pair_id == expected, (
                    f"pair_id={c.pair_id!r}, expected={expected!r}"
                )

    def test_schema1_candidates_have_first_funding_timestamp(self):
        """Every non-blocked candidate must have first_funding_timestamp_ms > 0."""
        raw = _make_v1_schema1_snapshot()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            path.write_text(json.dumps(raw))
            snap = load_snapshot(path)

        assert snap is not None
        for c in snap.candidates:
            if not c.blocked:
                assert c.first_funding_timestamp_ms > 0, (
                    f"Non-blocked candidate {c.pair_id} has "
                    f"first_funding_timestamp_ms={c.first_funding_timestamp_ms}"
                )

    def test_first_funding_is_min_of_leg_timestamps(self):
        """first_funding_timestamp_ms must be min(long, short)."""
        raw = _make_v1_schema1_snapshot()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            path.write_text(json.dumps(raw))
            snap = load_snapshot(path)

        # BTCUSDT: long=1700000800000, short=1700000950000
        btc = [c for c in snap.candidates if c.symbol == "BTCUSDT"]
        assert len(btc) == 1
        btc_c = btc[0]
        assert btc_c.first_funding_timestamp_ms == 1700000800000, (
            f"BTC first_funding_timestamp_ms={btc_c.first_funding_timestamp_ms}, "
            f"expected min(1700000800000, 1700000950000)=1700000800000"
        )
        assert btc_c.long_funding_timestamp_ms == 1700000800000
        assert btc_c.short_funding_timestamp_ms == 1700000950000

    def test_schema1_identity_is_usable_for_shadow_but_incomplete_economics_cannot_enter_live(self):
        """Identity recovery must not disguise an old snapshot as live-safe economics."""
        raw = _make_v1_schema1_snapshot()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            path.write_text(json.dumps(raw))
            snap = load_snapshot(path)

        # All candidates should now have proper identity
        # Set entry_notional_quote to pass zero-size gate
        for c in snap.candidates:
            c.entry_notional_quote = 100.0

        config = StrategyConfig(
            min_funding_edge_bps=6.0,
            min_expected_edge_bps=1.0,
            min_worst_case_edge_bps=0.0,
            max_concurrent_positions=8,
            funding_new_entries_enabled=True,
        )
        shadow_tradeable = discover_tradeable_candidates(
            snap.candidates, config, 1700000000000
        )
        assert shadow_tradeable, "Recovered identity should remain observable in shadow"
        assert discover_tradeable_candidates(
            snap.candidates,
            config,
            1700000000000,
            require_complete_economics=True,
        ) == []
        assert all("incomplete_economics" in c.blocked_reasons for c in snap.candidates)

    def test_no_tradeable_candidate_has_zero_first_funding(self):
        """No candidate in the tradeable list may have first_funding_timestamp_ms=0."""
        raw = _make_v1_schema1_snapshot()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            path.write_text(json.dumps(raw))
            snap = load_snapshot(path)

        for c in snap.candidates:
            c.entry_notional_quote = 100.0

        config = StrategyConfig(
            max_concurrent_positions=8,
            funding_new_entries_enabled=True,
        )
        tradeable = discover_tradeable_candidates(snap.candidates, config, 1700000000000)

        for c in tradeable:
            assert c.first_funding_timestamp_ms > 0, (
                f"Tradeable candidate {c.pair_id} has first_funding_timestamp_ms=0 — "
                f"this would permanently block entry-L2 prewarm"
            )

    def test_candidates_without_any_timestamp_are_blocked(self):
        """Candidates with no timestamp source at all must be marked blocked."""
        raw = _make_v1_schema1_snapshot()
        # Remove funding timestamps from both candidate and quotes for one entry
        raw["candidates"].append({
            "symbol": "NOFUND",
            "long_venue": "binance",
            "short_venue": "okx",
            "funding_edge_bps": 20.0,
            # No long/short funding timestamps
            "direction_consistent": True,
            "interval_aligned": True,
            "rank": 99,
            "origin_tags": [],
            "quality_notes": [],
            "quality_penalty_bps": 0.0,
        })

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            path.write_text(json.dumps(raw))
            snap = load_snapshot(path)

        nofund = [c for c in snap.candidates if c.symbol == "NOFUND"]
        assert len(nofund) == 1
        assert nofund[0].blocked is True, (
            "Candidate without any funding timestamp source must be blocked"
        )
        assert "missing_candidate_identity_or_funding_timestamp" in nofund[0].blocked_reasons


class TestV1Schema1WithQuotesOnly:
    """Red test: derive fields from quotes when raw candidate has no timestamps."""

    def test_derive_from_quotes_only(self):
        """When candidate has no per-leg timestamps, derive from quotes."""
        raw = {
            "schema_version": 1,
            "published_at_ms": 1700000000000,
            "market_observed_at_ms": 1700000000000,
            "funding_lifecycle": {},
            "market_lifecycle": {},
            "perp_liquidity_lifecycle": {},
            "degraded_venues": [],
            "source_mode": "coarse_sidecar",
            "quotes": {
                "gate": {
                    "ETHUSDT": {
                        "best_bid": 2000, "best_ask": 2001,
                        "best_bid_size": 100, "best_ask_size": 100,
                        "mark_price": 2000.5,
                        "funding_rate": 0.0002,
                        "funding_timestamp_ms": 1700002000000,
                    }
                },
                "bybit": {
                    "ETHUSDT": {
                        "best_bid": 2001, "best_ask": 2002,
                        "best_bid_size": 100, "best_ask_size": 100,
                        "mark_price": 2001.5,
                        "funding_rate": -0.0001,
                        "funding_timestamp_ms": 1700002100000,
                    }
                },
            },
            "candidates": [{
                "symbol": "ETHUSDT",
                "long_venue": "gate",
                "short_venue": "bybit",
                "funding_edge_bps": 30.0,
                # No per-leg timestamps at all — must derive from quotes
                "direction_consistent": True,
                "interval_aligned": True,
                "rank": 1,
                "origin_tags": [],
                "quality_notes": [],
                "quality_penalty_bps": 0.0,
            }],
        }

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            path.write_text(json.dumps(raw))
            snap = load_snapshot(path)

        assert snap is not None
        eth = snap.candidates[0]
        assert not eth.blocked, f"ETH candidate blocked: {eth.blocked_reasons}"
        assert eth.first_funding_timestamp_ms == 1700002000000, (
            f"Expected 1700002000000 (min of quote timestamps), got {eth.first_funding_timestamp_ms}"
        )
        assert eth.pair_id == "ethusdt:gate->bybit"
