"""Tests for sidecar snapshot schema, publisher, and pairing."""

import json
import tempfile
from pathlib import Path

from lightfee.sidecar.pairing import check_stale_snapshot
from lightfee.sidecar.publisher import _dict_to_snapshot, load_snapshot, publish_snapshot
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
    @staticmethod
    def _complete_v3_candidate() -> dict:
        """A raw V3 record, not a dataclass with parser-supplied defaults."""
        return {
            "long_venue": "binance",
            "short_venue": "okx",
            "symbol": "BTCUSDT",
            "funding_diff_bps": 10.0,
            "funding_edge_bps": 10.0,
            "expected_edge_bps": 10.0,
            "worst_case_edge_bps": 8.0,
            "ranking_edge_bps": 8.0,
            "first_stage_funding_edge_bps": 10.0,
            "first_stage_expected_edge_bps": 10.0,
            "first_stage_worst_case_edge_bps": 8.0,
            "second_stage_incremental_funding_edge_bps": 0.0,
            "second_stage_worst_case_funding_edge_bps": 0.0,
            "stagger_gap_ms": 0,
            "entry_notional_quote": 100.0,
            "entry_target_quantity": 1.0,
            "entry_max_executable_quantity": 1.0,
            "gross_signal_edge_bps": 0.0,
            "entry_cross_bps": 0.0,
            "expected_exit_cross_bps": 0.0,
            "entry_fee_bps": 0.0,
            "exit_fee_bps": 0.0,
            "entry_slippage_bps": 0.0,
            "exit_slippage_bps": 0.0,
            "adverse_selection_bps": 0.0,
            "capital_buffer_bps": 0.0,
            "execution_buffer_bps": 0.0,
            "venue_risk_haircut_bps": 0.0,
            "transfer_or_inventory_bias_bps": 0.0,
            "expected_net_edge_bps": 10.0,
            "long_taker_fee_bps": 0.0,
            "short_taker_fee_bps": 0.0,
            "taker_fee_evidence_complete": True,
            "forecast_distribution_stable": False,
            "forecast_stability_reason": "not_calibrated",
            "forecast_worst_funding_edge_bps": 8.0,
            "economics_complete": True,
            "economics_observed_at_ms": 9_000,
            "calculation_version": "v1_exact",
            "model_epoch": "v1_exact",
            "first_funding_timestamp_ms": 10_000,
        }

    @staticmethod
    def _complete_v3_contract_quotes() -> dict:
        """Two compatible, fully normalised contracts for a V3 candidate."""
        common = {
            "symbol": "BTCUSDT",
            "bid": 50_000.0,
            "ask": 50_001.0,
            "funding_timestamp_ms": 10_000,
            "funding_interval_ms": 28_800_000,
            "underlying": "BTC",
            "quote_currency": "USDT",
            "contract_type": "linear",
            "contract_multiplier": 1.0,
            "mark_index_source": "venue_index",
            "price_precision": 2,
            "quantity_precision": 3,
            "venue_status": "active",
            "contract_normalization_complete": True,
        }
        return {
            "binance:BTCUSDT": {"venue": "binance", **common},
            "okx:BTCUSDT": {"venue": "okx", **common},
        }

    def test_schema_v3_complete_candidate_requires_raw_sizing_and_formula(self):
        raw = self._complete_v3_candidate()
        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is True
        assert candidate.economics_incomplete_reason == ""
        assert candidate.entry_target_quantity == 1.0

    def test_schema_v3_missing_unified_sizing_is_diagnostic_only(self):
        raw = self._complete_v3_candidate()
        del raw["entry_target_quantity"]

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "missing_v3_economics_field:entry_target_quantity"
        )

    def test_schema_v3_formula_tampering_is_diagnostic_only(self):
        raw = self._complete_v3_candidate()
        raw["expected_net_edge_bps"] = 999.0

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "v3_edge_formula_mismatch:expected_net_edge_bps"
        )

    def test_schema_v3_rejects_unproved_transfer_or_inventory_bias(self):
        raw = self._complete_v3_candidate()
        raw["transfer_or_inventory_bias_bps"] = 1.0
        raw["expected_edge_bps"] = 11.0
        raw["expected_net_edge_bps"] = 11.0
        raw["worst_case_edge_bps"] = 9.0
        raw["ranking_edge_bps"] = 9.0

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == "unproved_transfer_or_inventory_bias"

    def test_schema_v3_rejects_truthy_string_economics_evidence(self):
        raw = self._complete_v3_candidate()
        raw["taker_fee_evidence_complete"] = "false"

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == "missing_taker_fee_evidence"

    def test_schema_v3_rejects_boolean_numeric_economics_fields(self):
        raw = self._complete_v3_candidate()
        raw["funding_edge_bps"] = True

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "invalid_v3_economics_field:funding_edge_bps"
        )

    def test_schema_v3_rejects_boolean_economics_observation_timestamp(self):
        raw = self._complete_v3_candidate()
        raw["economics_observed_at_ms"] = True

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "invalid_v3_economics_observed_at_ms"
        )

    def test_schema_v3_complete_candidate_requires_contract_evidence(self):
        raw = self._complete_v3_candidate()
        snapshot = _dict_to_snapshot({"schema_version": 3, "candidates": [raw]})

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "missing_v3_contract_evidence:long_quote"
        )

    def test_schema_v3_rejects_mismatched_contract_multiplier(self):
        raw = self._complete_v3_candidate()
        quotes = self._complete_v3_contract_quotes()
        quotes["okx:BTCUSDT"]["contract_multiplier"] = 0.01

        snapshot = _dict_to_snapshot(
            {"schema_version": 3, "quotes": quotes, "candidates": [raw]}
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "v3_contract_evidence:multiplier_mismatch"
        )

    def test_schema_v3_rejects_fractional_contract_precision(self):
        raw = self._complete_v3_candidate()
        quotes = self._complete_v3_contract_quotes()
        quotes["binance:BTCUSDT"]["quantity_precision"] = 1.5

        snapshot = _dict_to_snapshot(
            {"schema_version": 3, "quotes": quotes, "candidates": [raw]}
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "invalid_v3_contract_evidence:long_quote"
        )

    def test_schema_v3_rejects_ambiguous_same_market_contract_proof(self):
        raw = self._complete_v3_candidate()
        quotes = self._complete_v3_contract_quotes()
        quotes["duplicate"] = dict(quotes["binance:BTCUSDT"])

        snapshot = _dict_to_snapshot(
            {"schema_version": 3, "quotes": quotes, "candidates": [raw]}
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "ambiguous_v3_contract_evidence:long_quote"
        )

    def test_schema_v1_rejects_truthy_lifecycle_flags(self):
        from lightfee.sidecar.v1_compat import convert_v1_snapshot_to_v2

        converted = convert_v1_snapshot_to_v2(
            {
                "schema_version": 1,
                "candidates": [
                    {
                        "symbol": "BTCUSDT",
                        "long_venue": "binance",
                        "short_venue": "okx",
                        "funding_edge_bps": 10.0,
                        "direction_consistent": "true",
                        "interval_aligned": "true",
                    }
                ],
            }
        )

        candidate = converted["candidates"][0]
        assert candidate["direction_consistent"] is False
        assert candidate["interval_aligned"] is False

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

    def test_quote_freshness_provenance_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            snap = SidecarSnapshot(
                published_at_ms=2000,
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50000,
                        ask=50001,
                        observed_at_ms=1234,
                        source="sidecar_quote",
                    )
                },
            )

            publish_snapshot(snap, path)
            raw = json.loads(path.read_text())
            assert raw["quotes"]["binance:BTCUSDT"]["observed_at_ms"] == 1234
            assert raw["quotes"]["binance:BTCUSDT"]["source"] == "sidecar_quote"

            loaded = load_snapshot(path)
            assert loaded is not None
            quote = loaded.quotes["binance:BTCUSDT"]
            assert quote.observed_at_ms == 1234
            assert quote.source == "sidecar_quote"

    def test_optional_l2_depth_round_trip_and_malformed_ladder_falls_back(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "depth.json"
            snap = SidecarSnapshot(
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50_000,
                        ask=50_001,
                        bid_size=1.0,
                        ask_size=2.0,
                        bid_depth=((50_000.0, 1.0), (49_999.0, 2.0)),
                        ask_depth=((50_001.0, 2.0), (50_002.0, 3.0)),
                    )
                },
            )
            publish_snapshot(snap, path)

            loaded = load_snapshot(path)
            assert loaded is not None
            assert loaded.quotes["binance:BTCUSDT"].ask_depth == (
                (50_001.0, 2.0),
                (50_002.0, 3.0),
            )

            raw = json.loads(path.read_text())
            raw["quotes"]["binance:BTCUSDT"]["ask_depth"] = [["nan", 2.0]]
            path.write_text(json.dumps(raw))
            malformed = load_snapshot(path)
            assert malformed is not None
            assert malformed.quotes["binance:BTCUSDT"].ask_depth == ()

    def test_forecast_shadow_start_round_trip_survives_calibrator_fallback(self):
        """A missing calibration side-file must not reset the seven-day gate."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            snap = SidecarSnapshot(
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50_000,
                        ask=50_001,
                        observed_at_ms=700_000_000,
                        funding_forecast_started_at_ms=100_000_000,
                    )
                },
            )

            publish_snapshot(snap, path)
            loaded = load_snapshot(path)

            assert loaded is not None
            assert (
                loaded.quotes["binance:BTCUSDT"].funding_forecast_started_at_ms
                == 100_000_000
            )

    def test_quote_evidence_rejects_truthy_string_booleans(self):
        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": {
                    "binance:BTCUSDT": {
                        "venue": "binance",
                        "symbol": "BTCUSDT",
                        "bid": 50_000.0,
                        "ask": 50_001.0,
                        "funding_forecast_distribution_stable": "false",
                        "contract_normalization_complete": "true",
                    }
                },
            }
        )

        quote = snapshot.quotes["binance:BTCUSDT"]
        assert quote.funding_forecast_distribution_stable is False
        assert quote.contract_normalization_complete is False

    def test_load_snapshot_rejects_boolean_schema_version(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bool-schema.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": True,
                        "quotes": {
                            "binance:BTCUSDT": {
                                "venue": "binance",
                                "symbol": "BTCUSDT",
                                "bid": 50_000,
                                "ask": 50_001,
                            }
                        },
                    }
                )
            )

            assert load_snapshot(path) is None

    def test_quote_numeric_evidence_rejects_boolean_values(self):
        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": {
                    "binance:BTCUSDT": {
                        "venue": "binance",
                        "symbol": "BTCUSDT",
                        "bid": True,
                        "ask": 50_001.0,
                        "observed_at_ms": True,
                        "funding_timestamp_ms": True,
                        "funding_interval_ms": True,
                        "funding_forecast_sample_count": True,
                        "predicted_funding_rate_bps": True,
                        "contract_multiplier": True,
                        "price_precision": True,
                        "quantity_precision": True,
                    }
                },
            }
        )

        quote = snapshot.quotes["binance:BTCUSDT"]
        assert quote.bid == 0.0
        assert quote.ask == 50_001.0
        assert quote.observed_at_ms == 0
        assert quote.funding_timestamp_ms == 0
        assert quote.funding_interval_ms == 0
        assert quote.funding_forecast_sample_count == 0
        assert quote.predicted_funding_rate_bps is None
        assert quote.contract_multiplier == 0.0
        assert quote.price_precision == 0
        assert quote.quantity_precision == 0

    def test_optional_l2_depth_rejects_boolean_price_or_quantity(self):
        snapshot = _dict_to_snapshot(
            {
                "schema_version": 3,
                "quotes": {
                    "binance:BTCUSDT": {
                        "venue": "binance",
                        "symbol": "BTCUSDT",
                        "bid": 50_000.0,
                        "ask": 50_001.0,
                        "bid_depth": [[True, 1.0]],
                        "ask_depth": [[50_001.0, False]],
                    }
                },
            }
        )

        quote = snapshot.quotes["binance:BTCUSDT"]
        assert quote.bid_depth == ()
        assert quote.ask_depth == ()

    def test_schema_v2_candidate_cannot_claim_complete_economics(self):
        """Schema v2 remains diagnostic-only even when its JSON is edited."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "legacy-v2.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "candidates": [
                            {
                                "long_venue": "binance",
                                "short_venue": "okx",
                                "symbol": "BTCUSDT",
                                "funding_diff_bps": 10.0,
                                "funding_edge_bps": 10.0,
                                "expected_edge_bps": 5.0,
                                "worst_case_edge_bps": 2.0,
                                "ranking_edge_bps": 5.0,
                                "first_funding_timestamp_ms": 10_000,
                                "economics_complete": True,
                                "economics_observed_at_ms": 9_000,
                                "calculation_version": "v1_exact",
                            }
                        ],
                    }
                )
            )

            loaded = load_snapshot(path)

            assert loaded is not None
            candidate = loaded.candidates[0]
            assert candidate.expected_edge_bps == 5.0
            assert candidate.worst_case_edge_bps == 2.0
            assert candidate.economics_complete is False

    def test_load_missing_returns_none(self):
        assert load_snapshot("/tmp/nonexistent/snap.json") is None

    def test_load_invalid_json_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bad.json"
            path.write_text("not valid json")
            assert load_snapshot(path) is None

    def test_load_malformed_candidate_timestamp_blocks_candidate_only(self):
        """A bad candidate scalar must not make the whole snapshot unreadable."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bad-scalar.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 3,
                        "quotes": {
                            "binance:BTCUSDT": {
                                "venue": "binance",
                                "symbol": "BTCUSDT",
                                "bid": 50_000,
                                "ask": 50_001,
                            }
                        },
                        "candidates": [
                            {
                                "long_venue": "binance",
                                "short_venue": "okx",
                                "symbol": "BTCUSDT",
                                "funding_diff_bps": 5.0,
                                "funding_edge_bps": 5.0,
                                "expected_edge_bps": 3.0,
                                "worst_case_edge_bps": 1.0,
                                "ranking_edge_bps": 1.0,
                                "first_funding_timestamp_ms": "not-an-integer",
                            }
                        ],
                    }
                )
            )

            loaded = load_snapshot(path)

            assert loaded is not None
            assert loaded.quotes["binance:BTCUSDT"].bid == 50_000
            candidate = loaded.candidates[0]
            assert candidate.first_funding_timestamp_ms == 0
            assert candidate.blocked is True
            assert "missing_candidate_identity_or_funding_timestamp" in (
                candidate.blocked_reasons
            )

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
        assert check_stale_snapshot(2001, 2000, 2000)


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
