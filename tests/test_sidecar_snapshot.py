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


class TestStaleness:
    def test_stale_snapshot(self):
        assert check_stale_snapshot(1000, 500, 2000)
        assert not check_stale_snapshot(1000, 2000, 2000)


def _to_dict(s: SidecarSnapshot) -> dict:
    return {
        "schema_version": s.schema_version,
        "published_at_ms": s.published_at_ms,
        "degraded_venues": s.degraded_venues,
    }
