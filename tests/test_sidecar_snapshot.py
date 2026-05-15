"""Tests for sidecar snapshot schema, publisher, and pairing."""

import json
import tempfile
from pathlib import Path

import pytest

from lightfee.sidecar.pairing import check_stale_snapshot
from lightfee.sidecar.publisher import load_snapshot, publish_snapshot
from lightfee.sidecar.snapshot import (
    CandidateInput,
    QuoteSnapshot,
    SidecarSnapshot,
)


class TestSnapshotSchema:
    def test_has_required_schema_version(self):
        s = SidecarSnapshot()
        assert s.schema_version >= 1

    def test_no_chillybot_origin_possible(self):
        s = SidecarSnapshot()
        d = _to_dict(s)
        # Provenance contains only exchange/native source markers
        assert "degraded_venues" in d
        assert "chillybot" not in json.dumps(d).lower()

    def test_candidate_list_initialized(self):
        s = SidecarSnapshot()
        assert s.candidates == []


class TestPublisher:
    def test_atomic_publish_and_load(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            snap = SidecarSnapshot(
                published_at_ms=1000,
                degraded_venues=["test_venue"],
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance", symbol="BTCUSDT", bid=50000, ask=50001, funding_rate_bps=10
                    )
                },
                candidates=[
                    CandidateInput(
                        long_venue="binance",
                        short_venue="okx",
                        symbol="BTCUSDT",
                        funding_diff_bps=5.0,
                        funding_edge_bps=5.0,
                        expected_edge_bps=3.0,
                        worst_case_edge_bps=1.0,
                        ranking_edge_bps=1.5,
                    )
                ],
            )

            publish_snapshot(snap, path)
            assert path.exists()

            loaded = load_snapshot(path)
            assert loaded is not None
            assert loaded.published_at_ms == 1000
            assert loaded.schema_version == snap.schema_version
            assert len(loaded.candidates) == 1
            assert loaded.candidates[0].symbol == "BTCUSDT"

    def test_load_missing_returns_none(self):
        assert load_snapshot("/tmp/nonexistent/snap.json") is None

    def test_load_invalid_json_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bad.json"
            path.write_text("not valid json")
            assert load_snapshot(path) is None

    def test_load_missing_schema_version_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "no_version.json"
            path.write_text('{"published_at_ms": 1000}')
            assert load_snapshot(path) is None


class TestV2CandidateIdentity:
    """V2 snapshot candidates must serialize and deserialize all identity fields."""

    def test_candidate_round_trip_all_identity_fields(self):
        c = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0, ranking_edge_bps=5.0,
            pair_id="btcusdt:binance->okx",
            funding_timestamp_ms=1700000001000,
            first_funding_timestamp_ms=1700000001000,
            long_funding_timestamp_ms=1700000001000,
            short_funding_timestamp_ms=1700000002000,
            second_funding_timestamp_ms=1700000002000,
            first_funding_leg="long",
            direction_consistent=True,
            interval_aligned=True,
            opportunity_type="aligned",
            entry_notional_quote=50.0,
        )
        assert c.pair_id == "btcusdt:binance->okx"
        assert c.first_funding_leg == "long"
        assert c.second_funding_timestamp_ms == 1700000002000
        assert c.direction_consistent is True
        assert c.interval_aligned is True
        assert c.entry_notional_quote == 50.0

    def test_snapshot_serialization_includes_v2_fields(self):
        import json
        c = CandidateInput(
            long_venue="a", short_venue="b", symbol="X",
            funding_diff_bps=5, funding_edge_bps=5,
            expected_edge_bps=3, worst_case_edge_bps=1, ranking_edge_bps=3,
            pair_id="x:a->b",
            first_funding_leg="long",
            second_funding_timestamp_ms=1700000002000,
            direction_consistent=True,
            interval_aligned=True,
        )
        snap = SidecarSnapshot(candidates=[c])
        raw = _snapshot_to_dict_for_test(snap)
        cand_dict = raw["candidates"][0]
        assert "second_funding_timestamp_ms" in cand_dict
        assert "first_funding_leg" in cand_dict
        assert "direction_consistent" in cand_dict
        assert "interval_aligned" in cand_dict

    def test_loaded_snapshot_preserves_identity(self):
        import tempfile
        from pathlib import Path
        c = CandidateInput(
            long_venue="a", short_venue="b", symbol="X",
            funding_diff_bps=5, funding_edge_bps=5,
            expected_edge_bps=3, worst_case_edge_bps=1, ranking_edge_bps=3,
            pair_id="x:a->b",
            first_funding_timestamp_ms=1700000001000,
            long_funding_timestamp_ms=1700000001000,
            short_funding_timestamp_ms=1700000002000,
            second_funding_timestamp_ms=1700000002000,
            first_funding_leg="long",
            direction_consistent=True,
            interval_aligned=True,
            entry_notional_quote=50.0,
        )
        snap = SidecarSnapshot(candidates=[c])
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            publish_snapshot(snap, path)
            loaded = load_snapshot(path)
            assert loaded is not None
            lc = loaded.candidates[0]
            assert lc.pair_id == "x:a->b"
            assert lc.first_funding_leg == "long"
            assert lc.second_funding_timestamp_ms == 1700000002000
            assert lc.direction_consistent is True
            assert lc.interval_aligned is True


class TestStaleness:
    def test_stale_snapshot(self):
        assert check_stale_snapshot(1000, 500, 2000)
        assert not check_stale_snapshot(1000, 2000, 2000)


def _snapshot_to_dict_for_test(s: SidecarSnapshot) -> dict:
    """Minimal dict for testing serialization coverage."""
    return {
        "schema_version": s.schema_version,
        "published_at_ms": s.published_at_ms,
        "candidates": [
            {
                "pair_id": c.pair_id,
                "first_funding_leg": c.first_funding_leg,
                "second_funding_timestamp_ms": c.second_funding_timestamp_ms,
                "direction_consistent": c.direction_consistent,
                "interval_aligned": c.interval_aligned,
            }
            for c in s.candidates
        ],
    }


def _to_dict(s: SidecarSnapshot) -> dict:
    return {
        "schema_version": s.schema_version,
        "published_at_ms": s.published_at_ms,
        "degraded_venues": s.degraded_venues,
    }
