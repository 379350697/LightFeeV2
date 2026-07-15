"""Tests for sidecar snapshot schema, publisher, and pairing."""

import json
import tempfile
from copy import deepcopy
from pathlib import Path

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.ops.production_health import analyze_sidecar_snapshot
from lightfee.sidecar.pairing import check_stale_snapshot
from lightfee.sidecar.publisher import _dict_to_snapshot, load_snapshot, publish_snapshot
from lightfee.sidecar.snapshot import (
    CandidateInput,
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    validate_v4_snapshot_contract,
)
from lightfee.strategy.discovery import discover_tradeable_candidates


def _v3_snapshot_proof(
    published_at_ms: int,
    *,
    market_observed_at_ms: int | None = None,
    input_quote_count: int = 0,
    output_candidate_count: int = 0,
) -> dict:
    market_at_ms = (
        published_at_ms
        if market_observed_at_ms is None
        else market_observed_at_ms
    )
    lifecycle_kwargs = {
        "venue": "binance",
        "observed_at_ms": market_at_ms,
        "symbol_count": 1,
        "coverage_usable": 1,
        "degraded_reason": "",
    }
    return {
        "published_at_ms": published_at_ms,
        "market_observed_at_ms": market_at_ms,
        "candidate_build_observed_at_ms": published_at_ms,
        "candidate_build_diagnostics": {
            "input_quote_count": input_quote_count,
            "requested_symbol_count": 1 if input_quote_count else 0,
            "requested_symbols": ["BTCUSDT"] if input_quote_count else [],
            "requested_venues": ["binance"] if input_quote_count else [],
            "directional_pair_count": output_candidate_count,
            "output_candidate_count": output_candidate_count,
            "future_input_quote_count": 0,
            "rejection_counts": {},
        },
        "source_mode": "direct_market",
        "acquisition_mode": "fresh_sidecar",
        "funding_lifecycle": (
            [FundingLifecycle(**lifecycle_kwargs)] if input_quote_count else []
        ),
        "market_lifecycle": (
            [MarketLifecycle(**lifecycle_kwargs)] if input_quote_count else []
        ),
        "liquidity_lifecycle": (
            [LiquidityLifecycle(**lifecycle_kwargs)] if input_quote_count else []
        ),
        "transfer_lifecycle": [],
    }


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
            "fee_bps": 0.0,
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
            "price_tick": 0.01,
            "quantity_step_base": 0.001,
            "min_quantity_base": 0.001,
            "min_notional_quote": 5.0,
            "min_notional_evidence_complete": True,
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
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is True
        assert candidate.economics_incomplete_reason == ""
        assert candidate.entry_target_quantity == 1.0

    def test_schema_v3_accepts_zero_decimal_precision_for_integer_lots(self):
        raw = self._complete_v3_candidate()
        quotes = self._complete_v3_contract_quotes()
        quotes["binance:BTCUSDT"]["quantity_precision"] = 0
        quotes["okx:BTCUSDT"]["quantity_precision"] = 0

        snapshot = _dict_to_snapshot(
            {"schema_version": 4, "quotes": quotes, "candidates": [raw]}
        )

        assert snapshot.candidates[0].economics_complete is True

    def test_schema_v3_missing_unified_sizing_is_diagnostic_only(self):
        raw = self._complete_v3_candidate()
        del raw["entry_target_quantity"]

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "missing_v3_economics_field:entry_target_quantity"
        )

    def test_schema_v3_formula_tampering_is_blocked_at_parser_boundary(self):
        raw = self._complete_v3_candidate()
        raw["expected_net_edge_bps"] = 999.0

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "v3_edge_formula_mismatch:expected_net_edge_bps"
        )
        assert candidate.blocked is True
        assert "v3_edge_formula_mismatch:expected_net_edge_bps" in (
            candidate.blocked_reasons
        )

    def test_schema_v3_cannot_hide_leg_fees_behind_zero_aggregates(self):
        raw = self._complete_v3_candidate()
        raw["long_taker_fee_bps"] = 5.0
        raw["short_taker_fee_bps"] = 5.0

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.blocked is True
        assert candidate.economics_incomplete_reason == (
            "v3_fee_contract_mismatch:entry_fee_bps"
        )

    def test_schema_v3_never_credits_self_asserted_maker_rebate(self):
        raw = self._complete_v3_candidate()
        raw["entry_maker_leg"] = "long"
        raw["entry_fee_bps"] = -100.0
        raw["fee_bps"] = -100.0
        raw["expected_edge_bps"] = 110.0
        raw["expected_net_edge_bps"] = 110.0
        raw["worst_case_edge_bps"] = 108.0
        raw["ranking_edge_bps"] = 108.0
        raw["account_fee_evidence_complete"] = True
        raw["account_fee_evidence_provenance"] = [
            {"integrity_verified": True, "maker_fee_bps": -100.0}
        ]

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )

        assert snapshot.candidates[0].economics_complete is False
        assert snapshot.candidates[0].economics_incomplete_reason == (
            "v3_fee_contract_mismatch:entry_fee_bps"
        )

    def test_incomplete_v3_economics_cannot_bypass_paper_discovery(self):
        raw = self._complete_v3_candidate()
        raw.update(
            {
                "economics_complete": False,
                "blocked": False,
                "expected_net_edge_bps": 999.0,
                "expected_edge_bps": 999.0,
                "worst_case_edge_bps": 999.0,
                "ranking_edge_bps": 999.0,
            }
        )

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )
        candidate = snapshot.candidates[0]

        assert candidate.blocked is True
        assert "incomplete_v3_economics" in candidate.blocked_reasons
        assert discover_tradeable_candidates(
            [candidate],
            StrategyConfig(
                funding_new_entries_enabled=True,
                max_scan_minutes_before_funding=0,
                min_scan_minutes_before_funding=0,
                min_funding_edge_bps=0.0,
                min_expected_edge_bps=0.0,
                min_worst_case_edge_bps=0.0,
            ),
            now_ms=9_000,
            require_complete_economics=False,
        ) == []

    def test_shared_v3_contract_rejects_formula_tampering_before_paper_load(self, tmp_path):
        raw_candidate = self._complete_v3_candidate()
        raw_candidate.update(
            {
                "funding_timestamp_ms": 10_000,
                "long_funding_timestamp_ms": 10_000,
                "short_funding_timestamp_ms": 10_000,
                "expected_net_edge_bps": 999.0,
                "expected_edge_bps": 999.0,
                "ranking_edge_bps": 999.0,
            }
        )
        quotes = self._complete_v3_contract_quotes()
        for index, quote in enumerate(quotes.values()):
            quote["observed_at_ms"] = 10_000
            quote["funding_rate_bps"] = float(index + 1)
        lifecycle = [
            {
                "venue": venue,
                "observed_at_ms": 10_000,
                "symbol_count": 1,
                "coverage_usable": 1,
                "degraded_reason": "",
            }
            for venue in ("binance", "okx")
        ]
        raw = {
            "schema_version": 4,
            "published_at_ms": 10_000,
            "market_observed_at_ms": 10_000,
            "candidate_build_observed_at_ms": 10_000,
            "candidate_build_diagnostics": {
                "input_quote_count": 2,
                "requested_symbol_count": 1,
                "requested_symbols": ["BTCUSDT"],
                "requested_venues": ["binance", "okx"],
                "directional_pair_count": 1,
                "output_candidate_count": 1,
                "future_input_quote_count": 0,
                "rejection_counts": {},
            },
            "funding_lifecycle": deepcopy(lifecycle),
            "market_lifecycle": deepcopy(lifecycle),
            "liquidity_lifecycle": deepcopy(lifecycle),
            "transfer_lifecycle": [],
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": quotes,
            "candidates": [raw_candidate],
        }

        errors = validate_v4_snapshot_contract(raw)

        assert (
            "candidate_economics_contract_invalid:0:"
            "v3_edge_formula_mismatch:expected_net_edge_bps"
        ) in errors
        path = tmp_path / "tampered-v3.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        assert load_snapshot(path) is None

    def test_schema_v3_rejects_negative_taker_cost_as_untrusted_alpha(self):
        raw = self._complete_v3_candidate()
        raw["entry_slippage_bps"] = -1.0
        raw["expected_net_edge_bps"] += 1.0
        raw["worst_case_edge_bps"] += 1.0
        raw["ranking_edge_bps"] += 1.0

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "invalid_v3_economics_cost_sign:entry_slippage_bps"
        )

    def test_schema_v3_rejects_passive_rebate_without_signed_provenance(self):
        raw = self._complete_v3_candidate()
        raw["entry_maker_leg"] = "long"
        raw["exit_maker_leg"] = "short"
        raw["entry_fee_bps"] = -0.2
        raw["expected_edge_bps"] += 0.2
        raw["expected_net_edge_bps"] += 0.2
        raw["worst_case_edge_bps"] += 0.2
        raw["ranking_edge_bps"] += 0.2

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "v3_fee_contract_mismatch:entry_fee_bps"
        )

    @pytest.mark.parametrize(
        ("field", "value"),
        (
            ("contract_multiplier", "1.0"),
            ("price_precision", "2"),
            ("quantity_precision", "3"),
            ("funding_interval_ms", "28800000"),
        ),
    )
    def test_schema_v3_rejects_stringified_contract_scalars(
        self,
        field: str,
        value: str,
    ):
        raw = self._complete_v3_candidate()
        quotes = self._complete_v3_contract_quotes()
        quotes["binance:BTCUSDT"][field] = value

        snapshot = _dict_to_snapshot(
            {"schema_version": 4, "quotes": quotes, "candidates": [raw]}
        )

        assert snapshot.candidates[0].economics_complete is False
        assert snapshot.candidates[0].economics_incomplete_reason == (
            "invalid_v3_contract_evidence:long_quote"
        )

    @pytest.mark.parametrize(
        ("fee_field", "maker_leg_field", "maker_leg"),
        (
            ("entry_fee_bps", "entry_maker_leg", ""),
            ("entry_fee_bps", "entry_maker_leg", "invalid"),
            ("exit_fee_bps", "exit_maker_leg", ""),
            ("exit_fee_bps", "exit_maker_leg", "invalid"),
        ),
    )
    def test_schema_v3_rejects_signed_fee_without_maker_leg_proof(
        self,
        fee_field: str,
        maker_leg_field: str,
        maker_leg: str,
    ):
        raw = self._complete_v3_candidate()
        raw[fee_field] = -0.2
        raw[maker_leg_field] = maker_leg
        raw["expected_edge_bps"] += 0.2
        raw["expected_net_edge_bps"] += 0.2
        raw["worst_case_edge_bps"] += 0.2
        raw["ranking_edge_bps"] += 0.2

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": self._complete_v3_contract_quotes(),
                "candidates": [raw],
            }
        )

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason.startswith(
            ("invalid_v3_fee_contract:", "v3_fee_contract_mismatch:")
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
                "schema_version": 4,
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
                "schema_version": 4,
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
                "schema_version": 4,
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
                "schema_version": 4,
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
        snapshot = _dict_to_snapshot({"schema_version": 4, "candidates": [raw]})

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
            {"schema_version": 4, "quotes": quotes, "candidates": [raw]}
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
            {"schema_version": 4, "quotes": quotes, "candidates": [raw]}
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
            {"schema_version": 4, "quotes": quotes, "candidates": [raw]}
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
                published_at_ms=1_000,
                market_observed_at_ms=1_000,
                candidate_build_observed_at_ms=1_000,
                candidate_build_diagnostics={
                    "input_quote_count": 2,
                    "requested_symbol_count": 1,
                    "requested_symbols": ["BTCUSDT"],
                    "requested_venues": ["binance", "okx"],
                    "directional_pair_count": 1,
                    "output_candidate_count": 1,
                    "future_input_quote_count": 0,
                    "rejection_counts": {},
                },
                source_mode="direct_market",
                acquisition_mode="fresh_sidecar",
                funding_lifecycle=[
                    FundingLifecycle(
                        venue=venue,
                        observed_at_ms=1_000,
                        symbol_count=1,
                        coverage_usable=1,
                    )
                    for venue in ("binance", "okx")
                ],
                market_lifecycle=[
                    MarketLifecycle(
                        venue=venue,
                        observed_at_ms=1_000,
                        symbol_count=1,
                        coverage_usable=1,
                    )
                    for venue in ("binance", "okx")
                ],
                liquidity_lifecycle=[
                    LiquidityLifecycle(
                        venue=venue,
                        observed_at_ms=1_000,
                        symbol_count=1,
                        coverage_usable=1,
                    )
                    for venue in ("binance", "okx")
                ],
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50000,
                        ask=50001,
                        funding_rate_bps=10,
                        funding_timestamp_ms=2_000,
                        funding_interval_ms=28_800_000,
                        observed_at_ms=1_000,
                    ),
                    "okx:BTCUSDT": QuoteSnapshot(
                        venue="okx",
                        symbol="BTCUSDT",
                        bid=50000,
                        ask=50001,
                        funding_rate_bps=5,
                        funding_timestamp_ms=2_000,
                        funding_interval_ms=28_800_000,
                        observed_at_ms=1_000,
                    ),
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
                        funding_timestamp_ms=2_000,
                        first_funding_timestamp_ms=2_000,
                        long_funding_timestamp_ms=2_000,
                        short_funding_timestamp_ms=2_000,
                        blocked=True,
                        blocked_reasons=["incomplete_v3_economics"],
                        economics_incomplete_reason="incomplete_v3_economics",
                    )
                ],
            )

            publish_snapshot(snap, path)
            assert path.exists()

            loaded = load_snapshot(path)
            assert loaded is not None
            assert loaded.published_at_ms == 1000
            assert loaded.schema_version == snap.schema_version

            malformed = _snapshot_to_dict_for_test(snap)
            malformed["candidates"][0]["entry_notional_quote"] = "evil"
            assert "candidate_field_type_invalid:0:entry_notional_quote" in (
                validate_v4_snapshot_contract(malformed)
            )
            assert len(loaded.candidates) == 1
            assert loaded.candidates[0].symbol == "BTCUSDT"

    def test_quote_freshness_provenance_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            snap = SidecarSnapshot(
                **_v3_snapshot_proof(
                    2_000,
                    market_observed_at_ms=1_234,
                    input_quote_count=1,
                ),
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50000,
                        ask=50001,
                        observed_at_ms=1234,
                        funding_rate_bps=1.0,
                        funding_timestamp_ms=2_000,
                        funding_interval_ms=28_800_000,
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

    def test_invalid_publish_preserves_last_readable_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            valid = SidecarSnapshot(
                **_v3_snapshot_proof(1_000, input_quote_count=1),
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50_000,
                        ask=50_001,
                        observed_at_ms=1_000,
                        funding_rate_bps=1.0,
                        funding_timestamp_ms=2_000,
                        funding_interval_ms=28_800_000,
                    )
                },
            )
            publish_snapshot(valid, path)
            original = path.read_bytes()
            invalid = deepcopy(valid)
            invalid.quotes["binance:BTCUSDT"].bid = 0.0

            with pytest.raises(ValueError, match="quote_bid_invalid"):
                publish_snapshot(invalid, path)

            assert path.read_bytes() == original
            assert load_snapshot(path) is not None

    def test_optional_l2_depth_round_trip_and_malformed_v3_ladder_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "depth.json"
            snap = SidecarSnapshot(
                **_v3_snapshot_proof(1, input_quote_count=1),
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50_000,
                        ask=50_001,
                        bid_size=1.0,
                        ask_size=2.0,
                        observed_at_ms=1,
                        funding_rate_bps=1.0,
                        funding_timestamp_ms=2_000,
                        funding_interval_ms=28_800_000,
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
            assert malformed is None

    @pytest.mark.parametrize(
        ("field_name", "malformed_value"),
        [
            ("bid_size", "1.0"),
            ("contract_multiplier", "1.0"),
            ("price_precision", "2"),
            ("price_tick", "0.01"),
            ("quantity_step_base", "0.001"),
            ("min_quantity_base", "0.001"),
            ("min_notional_quote", "5.0"),
        ],
    )
    def test_v3_raw_quote_scalar_types_are_rejected_without_candidates(
        self,
        field_name,
        malformed_value,
    ):
        """The raw quote boundary is authoritative even with zero candidates."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "raw-quote-types.json"
            snap = SidecarSnapshot(
                **_v3_snapshot_proof(1_000, input_quote_count=1),
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50_000.0,
                        ask=50_001.0,
                        bid_size=1.0,
                        ask_size=2.0,
                        observed_at_ms=1_000,
                        funding_rate_bps=1.0,
                        funding_timestamp_ms=2_000,
                        funding_interval_ms=28_800_000,
                        underlying="BTC",
                        quote_currency="USDT",
                        contract_type="linear",
                        contract_multiplier=1.0,
                        mark_index_source="venue_mark_and_index",
                        price_precision=2,
                        quantity_precision=3,
                        price_tick=0.01,
                        quantity_step_base=0.001,
                        min_quantity_base=0.001,
                        min_notional_quote=5.0,
                        min_notional_evidence_complete=True,
                        venue_status="active",
                        contract_normalization_complete=True,
                    )
                },
            )
            publish_snapshot(snap, path)
            raw = json.loads(path.read_text(encoding="utf-8"))
            raw["quotes"]["binance:BTCUSDT"][field_name] = malformed_value
            path.write_text(json.dumps(raw), encoding="utf-8")

            assert load_snapshot(path) is None

    def test_forecast_shadow_start_round_trip_survives_calibrator_fallback(self):
        """A missing calibration side-file must not reset the seven-day gate."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            snap = SidecarSnapshot(
                **_v3_snapshot_proof(
                    700_000_000,
                    market_observed_at_ms=700_000_000,
                    input_quote_count=1,
                ),
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50_000,
                        ask=50_001,
                        observed_at_ms=700_000_000,
                        funding_rate_bps=1.0,
                        funding_timestamp_ms=800_000_000,
                        funding_interval_ms=28_800_000,
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
                "schema_version": 4,
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
                                "observed_at_ms": 1_000,
                            }
                        },
                    }
                )
            )

            assert load_snapshot(path) is None

    def test_quote_numeric_evidence_rejects_boolean_values(self):
        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
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
                "schema_version": 4,
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

    @pytest.mark.parametrize(
        "mutation",
        ["missing_degradation_proof", "quote_after_candidate", "count_mismatch"],
    )
    def test_v3_loader_rejects_proof_not_bound_to_payload(self, mutation):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "unbound-proof.json"
            snapshot = SidecarSnapshot(
                **_v3_snapshot_proof(1_000, input_quote_count=1),
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=50_000,
                        ask=50_001,
                        observed_at_ms=1_000,
                        funding_rate_bps=1.0,
                        funding_timestamp_ms=2_000,
                        funding_interval_ms=28_800_000,
                    )
                },
            )
            publish_snapshot(snapshot, path)
            raw = json.loads(path.read_text(encoding="utf-8"))
            if mutation == "missing_degradation_proof":
                raw.pop("degraded_symbols")
            elif mutation == "quote_after_candidate":
                raw["quotes"]["binance:BTCUSDT"]["observed_at_ms"] = 1_001
            else:
                raw["candidate_build_diagnostics"]["input_quote_count"] = 2
            path.write_text(json.dumps(raw), encoding="utf-8")

            assert load_snapshot(path) is None

    def test_load_malformed_candidate_timestamp_blocks_candidate_only(self):
        """A bad candidate scalar must not make the whole snapshot unreadable."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bad-scalar.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 4,
                        **_v3_snapshot_proof(1_000),
                        "candidate_build_diagnostics": {
                            "input_quote_count": 2,
                            "requested_symbol_count": 1,
                            "requested_symbols": ["BTCUSDT"],
                            "requested_venues": ["binance", "okx"],
                            "directional_pair_count": 1,
                            "output_candidate_count": 1,
                            "future_input_quote_count": 0,
                            "rejection_counts": {},
                        },
                        "funding_lifecycle": [
                            {
                                "venue": "binance",
                                "observed_at_ms": 1_000,
                                "symbol_count": 1,
                                "coverage_usable": 1,
                                "degraded_reason": "",
                            },
                            {
                                "venue": "okx",
                                "observed_at_ms": 1_000,
                                "symbol_count": 1,
                                "coverage_usable": 1,
                                "degraded_reason": "",
                            },
                        ],
                        "market_lifecycle": [
                            {
                                "venue": "binance",
                                "observed_at_ms": 1_000,
                                "symbol_count": 1,
                                "coverage_usable": 1,
                                "degraded_reason": "",
                            },
                            {
                                "venue": "okx",
                                "observed_at_ms": 1_000,
                                "symbol_count": 1,
                                "coverage_usable": 1,
                                "degraded_reason": "",
                            },
                        ],
                        "transfer_lifecycle": [],
                        "liquidity_lifecycle": [
                            {
                                "venue": "binance",
                                "observed_at_ms": 1_000,
                                "symbol_count": 1,
                                "coverage_usable": 1,
                                "degraded_reason": "",
                            },
                            {
                                "venue": "okx",
                                "observed_at_ms": 1_000,
                                "symbol_count": 1,
                                "coverage_usable": 1,
                                "degraded_reason": "",
                            },
                        ],
                        "degraded_venues": [],
                        "degraded_domains": [],
                        "degraded_symbols": {},
                        "quotes": {
                            "binance:BTCUSDT": {
                                "venue": "binance",
                                "symbol": "BTCUSDT",
                                "bid": 50_000,
                                "ask": 50_001,
                                "observed_at_ms": 1_000,
                                "funding_rate_bps": 1.0,
                                "funding_timestamp_ms": 2_000,
                                "funding_interval_ms": 28_800_000,
                            },
                            "okx:BTCUSDT": {
                                "venue": "okx",
                                "symbol": "BTCUSDT",
                                "bid": 50_000,
                                "ask": 50_001,
                                "observed_at_ms": 1_000,
                                "funding_rate_bps": 1.0,
                                "funding_timestamp_ms": 2_000,
                                "funding_interval_ms": 28_800_000,
                            },
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

            assert loaded is None

    def test_load_missing_schema_version_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "no_version.json"
            path.write_text('{"published_at_ms": 1000}')
            assert load_snapshot(path) is None

    @pytest.mark.parametrize(
        ("mutation", "expected_error"),
        (
            ("empty_modes", "source_mode_invalid"),
            (
                "unexplained_zero_coverage",
                "lifecycle_insufficient_coverage_unexplained:funding_lifecycle:0",
            ),
            ("empty_candidate", "candidate_identity_invalid:0:long_venue"),
            ("incomplete_quote", "quote_venue_invalid:binance:BTCUSDT"),
            (
                "crossed_quote_without_proof",
                "crossed_quote_degradation_unproved:binance:BTCUSDT",
            ),
            (
                "diagnostics_conservation",
                "candidate_diagnostics_conservation_mismatch",
            ),
            (
                "invalid_rejection_entry",
                "candidate_rejection_counts_invalid",
            ),
            (
                "requested_count_mismatch",
                "lifecycle_requested_count_exceeded:funding:binance",
            ),
            (
                "coverage_overclaim",
                "lifecycle_coverage_exceeds_quote_symbols:funding:binance",
            ),
            (
                "funding_coverage_unproved",
                "lifecycle_coverage_exceeds_quote_symbols:funding:binance",
            ),
            ("quote_not_requested", "quote_symbol_not_requested"),
            ("quote_venue_not_requested", "quote_venue_not_requested"),
            ("candidate_not_requested", "candidate_symbol_not_requested:0"),
            ("candidate_same_venue", "candidate_venues_not_distinct:0"),
            (
                "degraded_symbol_not_requested",
                "degraded_symbol_not_requested:binance",
            ),
            ("unavailable_with_payload", "unavailable_acquisition_with_payload"),
            (
                "duplicate_lifecycle",
                "lifecycle_duplicate_identity:funding_lifecycle:binance",
            ),
            ("transfer_not_requested", "lifecycle_venue_not_requested"),
            (
                "requested_venue_missing_lifecycle",
                "requested_venue_missing_funding_lifecycle",
            ),
            ("duplicate_candidate", "candidate_duplicate_identity:1"),
            (
                "degraded_symbol_substring_only",
                "degraded_symbol_evidence_unproved:binance:BTC",
            ),
            (
                "degraded_symbol_bare_reason",
                "degraded_symbol_evidence_unproved:binance:BTCUSDT",
            ),
            (
                "crossed_market_reason_wrong_symbol",
                "crossed_quote_degradation_unproved:binance:BTCUSDT",
            ),
        ),
    )
    def test_v3_shape_and_lifecycle_proof_fail_closed_everywhere(
        self, mutation, expected_error
    ):
        lifecycle = [
            {
                "venue": "binance",
                "observed_at_ms": 1_000,
                "symbol_count": 1,
                "coverage_usable": 1,
                "degraded_reason": "",
            }
        ]
        raw = {
            "schema_version": 4,
            "published_at_ms": 1_000,
            "market_observed_at_ms": 1_000,
            "candidate_build_observed_at_ms": 1_000,
            "candidate_build_diagnostics": {
                "input_quote_count": 1,
                "requested_symbol_count": 1,
                "requested_symbols": ["BTCUSDT"],
                "requested_venues": ["binance"],
                "directional_pair_count": 0,
                "output_candidate_count": 0,
                "future_input_quote_count": 0,
                "rejection_counts": {},
            },
            "funding_lifecycle": deepcopy(lifecycle),
            "market_lifecycle": deepcopy(lifecycle),
            "transfer_lifecycle": [],
            "liquidity_lifecycle": deepcopy(lifecycle),
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                "binance:BTCUSDT": {
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "bid": 50_000,
                    "ask": 50_001,
                    "observed_at_ms": 1_000,
                    "funding_rate_bps": 1.0,
                    "funding_timestamp_ms": 2_000,
                    "funding_interval_ms": 28_800_000,
                }
            },
            "candidates": [],
        }
        if mutation == "empty_modes":
            raw["source_mode"] = ""
            raw["acquisition_mode"] = ""
        elif mutation == "unexplained_zero_coverage":
            raw["funding_lifecycle"][0]["coverage_usable"] = 0
        elif mutation == "empty_candidate":
            raw["candidates"] = [{}]
            raw["candidate_build_diagnostics"]["directional_pair_count"] = 1
            raw["candidate_build_diagnostics"]["output_candidate_count"] = 1
        elif mutation == "incomplete_quote":
            raw["quotes"]["binance:BTCUSDT"] = {"observed_at_ms": 1_000}
        elif mutation == "crossed_quote_without_proof":
            raw["quotes"]["binance:BTCUSDT"]["bid"] = 50_002
        elif mutation == "diagnostics_conservation":
            raw["candidate_build_diagnostics"]["directional_pair_count"] = 2
            raw["candidate_build_diagnostics"]["rejection_counts"] = {
                "invalid_trade_quote": 1
            }
        elif mutation == "invalid_rejection_entry":
            raw["candidate_build_diagnostics"]["rejection_counts"] = {"": 0}
        elif mutation == "requested_count_mismatch":
            raw["funding_lifecycle"][0]["symbol_count"] = 2
        elif mutation == "coverage_overclaim":
            raw["candidate_build_diagnostics"]["requested_symbol_count"] = 2
            raw["candidate_build_diagnostics"]["requested_symbols"] = [
                "BTCUSDT",
                "ETHUSDT",
            ]
            for field in (
                "funding_lifecycle",
                "market_lifecycle",
                "liquidity_lifecycle",
            ):
                raw[field][0]["symbol_count"] = 2
                raw[field][0]["coverage_usable"] = 2
        elif mutation == "funding_coverage_unproved":
            raw["quotes"]["binance:BTCUSDT"].pop("funding_timestamp_ms")
        elif mutation == "quote_not_requested":
            raw["candidate_build_diagnostics"]["requested_symbols"] = ["ETHUSDT"]
        elif mutation == "quote_venue_not_requested":
            raw["candidate_build_diagnostics"]["requested_venues"] = ["okx"]
        elif mutation in {"candidate_not_requested", "candidate_same_venue"}:
            raw["candidate_build_diagnostics"]["directional_pair_count"] = 1
            raw["candidate_build_diagnostics"]["output_candidate_count"] = 1
            raw["candidates"] = [
                {
                    "long_venue": "binance",
                    "short_venue": (
                        "binance" if mutation == "candidate_same_venue" else "okx"
                    ),
                    "symbol": (
                        "BTCUSDT" if mutation == "candidate_same_venue" else "ETHUSDT"
                    ),
                    "funding_diff_bps": 1.0,
                    "funding_edge_bps": 1.0,
                    "expected_edge_bps": 1.0,
                    "worst_case_edge_bps": 1.0,
                    "ranking_edge_bps": 1.0,
                }
            ]
        elif mutation == "degraded_symbol_not_requested":
            raw["acquisition_mode"] = "degraded_sidecar"
            raw["degraded_symbols"] = {"binance": ["ETHUSDT"]}
            raw["funding_lifecycle"][0]["coverage_usable"] = 0
            raw["funding_lifecycle"][0]["degraded_reason"] = (
                "ETHUSDT: funding unavailable"
            )
        elif mutation == "duplicate_lifecycle":
            raw["funding_lifecycle"].append(deepcopy(raw["funding_lifecycle"][0]))
        elif mutation == "transfer_not_requested":
            raw["transfer_lifecycle"] = [
                {
                    "from_venue": "binance",
                    "to_venue": "okx",
                    "observed_at_ms": 1_000,
                    "funding_rate_bps": 1.0,
                    "funding_timestamp_ms": 2_000,
                    "funding_interval_ms": 28_800_000,
                    "coverage_usable": 1,
                    "degraded_reason": "",
                }
            ]
        elif mutation == "requested_venue_missing_lifecycle":
            raw["candidate_build_diagnostics"]["requested_venues"] = [
                "binance",
                "okx",
            ]
        elif mutation == "duplicate_candidate":
            raw["candidate_build_diagnostics"]["directional_pair_count"] = 2
            raw["candidate_build_diagnostics"]["output_candidate_count"] = 2
            candidate = {
                "long_venue": "binance",
                "short_venue": "binance",
                "symbol": "BTCUSDT",
                "funding_diff_bps": 1.0,
                "funding_edge_bps": 1.0,
                "expected_edge_bps": 1.0,
                "worst_case_edge_bps": 1.0,
                "ranking_edge_bps": 1.0,
            }
            raw["candidates"] = [deepcopy(candidate), deepcopy(candidate)]
        elif mutation == "degraded_symbol_substring_only":
            raw["candidate_build_diagnostics"]["requested_symbol_count"] = 2
            raw["candidate_build_diagnostics"]["requested_symbols"] = [
                "BTC",
                "BTCUSDT",
            ]
            for field in (
                "funding_lifecycle",
                "market_lifecycle",
                "liquidity_lifecycle",
            ):
                raw[field][0]["symbol_count"] = 2
            raw["funding_lifecycle"][0]["coverage_usable"] = 1
            raw["funding_lifecycle"][0]["degraded_reason"] = (
                "BTCUSDT: funding unavailable"
            )
            raw["degraded_symbols"] = {"binance": ["BTC"]}
            raw["acquisition_mode"] = "degraded_sidecar"
        elif mutation == "degraded_symbol_bare_reason":
            raw["degraded_symbols"] = {"binance": ["BTCUSDT"]}
            raw["acquisition_mode"] = "degraded_sidecar"
            raw["funding_lifecycle"][0]["coverage_usable"] = 0
            raw["funding_lifecycle"][0]["degraded_reason"] = "BTCUSDT"
        elif mutation == "crossed_market_reason_wrong_symbol":
            raw["quotes"]["binance:BTCUSDT"]["bid"] = 50_002
            raw["degraded_symbols"] = {"binance": ["BTCUSDT"]}
            raw["acquisition_mode"] = "degraded_sidecar"
            raw["market_lifecycle"][0]["coverage_usable"] = 0
            raw["market_lifecycle"][0]["degraded_reason"] = "ETHUSDT: crossed BBO"
            raw["liquidity_lifecycle"][0]["coverage_usable"] = 0
            raw["liquidity_lifecycle"][0]["degraded_reason"] = (
                "BTCUSDT: crossed BBO"
            )
        else:
            raw["acquisition_mode"] = "unavailable"
            raw["degraded_venues"] = ["binance"]
            raw["funding_lifecycle"][0]["coverage_usable"] = 0
            raw["funding_lifecycle"][0]["degraded_reason"] = "venue unavailable"

        errors = validate_v4_snapshot_contract(raw)
        assert expected_error in errors

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "invalid-v3.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            assert load_snapshot(path) is None

        health = analyze_sidecar_snapshot(raw, now_ms=1_000, max_age_ms=10_000)
        assert "sidecar_diagnostics_contract_invalid" in health.fingerprints
        assert expected_error in health.details["contract_errors"]

    def test_crossed_quote_is_readable_only_with_bound_market_degradation(self):
        raw = {
            "schema_version": 4,
            **_v3_snapshot_proof(1_000, input_quote_count=1),
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {"binance": ["BTCUSDT"]},
            "acquisition_mode": "degraded_sidecar",
            "quotes": {
                "binance:BTCUSDT": {
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "bid": 50_002,
                    "ask": 50_001,
                    "observed_at_ms": 1_000,
                    "funding_rate_bps": 1.0,
                    "funding_timestamp_ms": 2_000,
                    "funding_interval_ms": 28_800_000,
                }
            },
            "candidates": [],
        }
        for field in (
            "funding_lifecycle",
            "market_lifecycle",
            "liquidity_lifecycle",
        ):
            raw[field] = [vars(raw[field][0]).copy()]
        raw["market_lifecycle"][0]["coverage_usable"] = 0
        raw["market_lifecycle"][0]["degraded_reason"] = "BTCUSDT: crossed BBO"
        raw["liquidity_lifecycle"][0]["coverage_usable"] = 0
        raw["liquidity_lifecycle"][0]["degraded_reason"] = "BTCUSDT: crossed BBO"

        assert validate_v4_snapshot_contract(raw) == []

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "crossed-but-attributed.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            assert load_snapshot(path) is not None

        health = analyze_sidecar_snapshot(raw, now_ms=1_000, max_age_ms=10_000)
        assert "sidecar_diagnostics_contract_invalid" not in health.fingerprints
        assert "sidecar_snapshot_degraded" in health.fingerprints
        assert not health.ok


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
            funding_timestamp_ms=1700000001000,
            first_funding_leg="long",
            second_funding_timestamp_ms=1700000002000,
            direction_consistent=True,
            interval_aligned=True,
        )
        snap = SidecarSnapshot(
            published_at_ms=1,
            market_observed_at_ms=1,
            candidate_build_observed_at_ms=1,
            candidate_build_diagnostics={
                "input_quote_count": 2,
                "requested_symbol_count": 1,
                "requested_symbols": ["X"],
                "requested_venues": ["a", "b"],
                "directional_pair_count": 1,
                "output_candidate_count": 1,
                "future_input_quote_count": 0,
                "rejection_counts": {},
            },
            source_mode="direct_market",
            acquisition_mode="fresh_sidecar",
            funding_lifecycle=[
                FundingLifecycle(
                    venue=venue,
                    observed_at_ms=1,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("a", "b")
            ],
            market_lifecycle=[
                MarketLifecycle(
                    venue=venue,
                    observed_at_ms=1,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("a", "b")
            ],
            liquidity_lifecycle=[
                LiquidityLifecycle(
                    venue=venue,
                    observed_at_ms=1,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("a", "b")
            ],
            quotes={
                f"{venue}:X": QuoteSnapshot(
                    venue=venue,
                    symbol="X",
                    bid=100,
                    ask=101,
                    observed_at_ms=1,
                    funding_rate_bps=1.0,
                    funding_timestamp_ms=2_000,
                    funding_interval_ms=28_800_000,
                )
                for venue in ("a", "b")
            },
            candidates=[c],
        )
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
            funding_timestamp_ms=1700000001000,
            first_funding_timestamp_ms=1700000001000,
            long_funding_timestamp_ms=1700000001000,
            short_funding_timestamp_ms=1700000002000,
            second_funding_timestamp_ms=1700000002000,
            first_funding_leg="long",
            direction_consistent=True,
            interval_aligned=True,
            entry_notional_quote=50.0,
            blocked=True,
            blocked_reasons=["incomplete_v3_economics"],
            economics_incomplete_reason="incomplete_v3_economics",
        )
        snap = SidecarSnapshot(
            published_at_ms=1,
            market_observed_at_ms=1,
            candidate_build_observed_at_ms=1,
            candidate_build_diagnostics={
                "input_quote_count": 2,
                "requested_symbol_count": 1,
                "requested_symbols": ["X"],
                "requested_venues": ["a", "b"],
                "directional_pair_count": 1,
                "output_candidate_count": 1,
                "future_input_quote_count": 0,
                "rejection_counts": {},
            },
            source_mode="direct_market",
            acquisition_mode="fresh_sidecar",
            funding_lifecycle=[
                FundingLifecycle(
                    venue=venue,
                    observed_at_ms=1,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("a", "b")
            ],
            market_lifecycle=[
                MarketLifecycle(
                    venue=venue,
                    observed_at_ms=1,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("a", "b")
            ],
            liquidity_lifecycle=[
                LiquidityLifecycle(
                    venue=venue,
                    observed_at_ms=1,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("a", "b")
            ],
            quotes={
                f"{venue}:X": QuoteSnapshot(
                    venue=venue,
                    symbol="X",
                    bid=100,
                    ask=101,
                    observed_at_ms=1,
                    funding_rate_bps=1.0,
                    funding_timestamp_ms=2_000,
                    funding_interval_ms=28_800_000,
                )
                for venue in ("a", "b")
            },
            candidates=[c],
        )
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
