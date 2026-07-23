"""Tests for sidecar snapshot schema, publisher, and pairing."""

import json
import tempfile
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.marketdata.open_interest import open_interest_sample_id
from lightfee.ops.production_health import analyze_sidecar_snapshot
from lightfee.sidecar.pairing import check_stale_snapshot
from lightfee.sidecar.publisher import (
    _dict_to_snapshot,
    funding_entry_snapshot_identity,
    funding_entry_snapshot_manifest_path,
    funding_entry_snapshot_path,
    load_funding_entry_snapshot,
    load_snapshot,
    publish_funding_entry_snapshot,
    publish_snapshot,
)
from lightfee.sidecar.snapshot import (
    CandidateInput,
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    SnapshotFreshness,
    decide_snapshot_freshness,
    funding_rate_sample_id,
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
    market_at_ms = published_at_ms if market_observed_at_ms is None else market_observed_at_ms
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
            "seed_pair_count": output_candidate_count,
            "pair_decision_count": output_candidate_count,
            "eligible_candidate_count": output_candidate_count,
            "omitted_eligible_count": 0,
            "eligible_frontier_complete": True,
        },
        "source_mode": "direct_market",
        "acquisition_mode": "fresh_sidecar",
        "funding_lifecycle": ([FundingLifecycle(**lifecycle_kwargs)] if input_quote_count else []),
        "market_lifecycle": ([MarketLifecycle(**lifecycle_kwargs)] if input_quote_count else []),
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

        snapshot = _dict_to_snapshot({"schema_version": 4, "quotes": quotes, "candidates": [raw]})

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
        assert "v3_edge_formula_mismatch:expected_net_edge_bps" in (candidate.blocked_reasons)

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
        assert candidate.economics_incomplete_reason == ("v3_fee_contract_mismatch:entry_fee_bps")

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
        assert (
            discover_tradeable_candidates(
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
            )
            == []
        )

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
            "candidate_economics_contract_invalid:0:v3_edge_formula_mismatch:expected_net_edge_bps"
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
        assert candidate.economics_incomplete_reason == ("v3_fee_contract_mismatch:entry_fee_bps")

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

        snapshot = _dict_to_snapshot({"schema_version": 4, "quotes": quotes, "candidates": [raw]})

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
        assert candidate.economics_incomplete_reason == ("invalid_v3_economics_observed_at_ms")

    def test_schema_v3_complete_candidate_requires_contract_evidence(self):
        raw = self._complete_v3_candidate()
        snapshot = _dict_to_snapshot({"schema_version": 4, "candidates": [raw]})

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == ("missing_v3_contract_evidence:long_quote")

    def test_schema_v3_rejects_mismatched_contract_multiplier(self):
        raw = self._complete_v3_candidate()
        quotes = self._complete_v3_contract_quotes()
        quotes["okx:BTCUSDT"]["contract_multiplier"] = 0.01

        snapshot = _dict_to_snapshot({"schema_version": 4, "quotes": quotes, "candidates": [raw]})

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == ("v3_contract_evidence:multiplier_mismatch")

    def test_schema_v3_rejects_fractional_contract_precision(self):
        raw = self._complete_v3_candidate()
        quotes = self._complete_v3_contract_quotes()
        quotes["binance:BTCUSDT"]["quantity_precision"] = 1.5

        snapshot = _dict_to_snapshot({"schema_version": 4, "quotes": quotes, "candidates": [raw]})

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == ("invalid_v3_contract_evidence:long_quote")

    def test_schema_v3_rejects_ambiguous_same_market_contract_proof(self):
        raw = self._complete_v3_candidate()
        quotes = self._complete_v3_contract_quotes()
        quotes["duplicate"] = dict(quotes["binance:BTCUSDT"])

        snapshot = _dict_to_snapshot({"schema_version": 4, "quotes": quotes, "candidates": [raw]})

        candidate = snapshot.candidates[0]
        assert candidate.economics_complete is False
        assert candidate.economics_incomplete_reason == (
            "ambiguous_v3_contract_evidence:long_quote"
        )

    def test_v4_contract_quote_evidence_scan_is_linear_in_quotes(self):
        class CountingQuotes(dict):
            values_calls = 0

            def values(self):
                self.values_calls += 1
                return super().values()

        quotes = CountingQuotes(self._complete_v3_contract_quotes())
        for index, quote in enumerate(quotes.values()):
            quote["observed_at_ms"] = 10_000
            quote["funding_rate_bps"] = float(index + 1)
        quotes.values_calls = 0
        candidates = [deepcopy(self._complete_v3_candidate()) for _ in range(32)]
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
                "directional_pair_count": len(candidates),
                "output_candidate_count": len(candidates),
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
            "candidates": candidates,
        }

        errors = validate_v4_snapshot_contract(raw)

        assert not any("candidate_economics_contract_invalid" in error for error in errors)
        assert quotes.values_calls <= 4

    def test_v4_loader_quote_evidence_scan_is_linear_in_quotes(self):
        class CountingQuotes(dict):
            values_calls = 0

            def values(self):
                self.values_calls += 1
                return super().values()

        quotes = CountingQuotes(self._complete_v3_contract_quotes())
        candidates = [deepcopy(self._complete_v3_candidate()) for _ in range(32)]

        snapshot = _dict_to_snapshot(
            {
                "schema_version": 4,
                "quotes": quotes,
                "candidates": candidates,
            }
        )

        assert all(candidate.economics_complete for candidate in snapshot.candidates)
        assert quotes.values_calls <= 2

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
            assert loaded.quotes["binance:BTCUSDT"].funding_forecast_started_at_ms == 100_000_000

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
    def test_v3_shape_and_lifecycle_proof_fail_closed_everywhere(self, mutation, expected_error):
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
            raw["candidate_build_diagnostics"]["rejection_counts"] = {"invalid_trade_quote": 1}
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
                    "short_venue": ("binance" if mutation == "candidate_same_venue" else "okx"),
                    "symbol": ("BTCUSDT" if mutation == "candidate_same_venue" else "ETHUSDT"),
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
            raw["funding_lifecycle"][0]["degraded_reason"] = "ETHUSDT: funding unavailable"
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
            raw["funding_lifecycle"][0]["degraded_reason"] = "BTCUSDT: funding unavailable"
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
            raw["liquidity_lifecycle"][0]["degraded_reason"] = "BTCUSDT: crossed BBO"
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
            long_venue="binance",
            short_venue="okx",
            symbol="BTCUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=10.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=5.0,
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
            long_venue="a",
            short_venue="b",
            symbol="X",
            funding_diff_bps=5,
            funding_edge_bps=5,
            expected_edge_bps=3,
            worst_case_edge_bps=1,
            ranking_edge_bps=3,
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
            long_venue="a",
            short_venue="b",
            symbol="X",
            funding_diff_bps=5,
            funding_edge_bps=5,
            expected_edge_bps=3,
            worst_case_edge_bps=1,
            ranking_edge_bps=3,
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


def _complete_funding_entry_snapshot(candidate_count: int = 1) -> SidecarSnapshot:
    now_ms = 10_000
    funding_timestamp_ms = 20_000
    quotes: dict[str, QuoteSnapshot] = {}
    venues = ["long"] + [f"short{index}" for index in range(candidate_count)]
    for venue in venues:
        funding_rate_bps = 5.0 if venue == "long" else -5.0
        raw_quote = TestPublisher._complete_v3_contract_quotes()["binance:BTCUSDT"]
        raw_quote["venue"] = venue
        open_interest = 2_000_000.0
        raw_quote.update(
            observed_at_ms=now_ms,
            funding_rate_bps=funding_rate_bps,
            funding_rate_observed_at_ms=now_ms,
            funding_rate_event_at_ms=now_ms,
            funding_rate_received_at_ms=now_ms,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=funding_rate_sample_id(
                venue=venue,
                symbol="BTCUSDT",
                observed_at_ms=now_ms,
                rate_bps=funding_rate_bps,
                funding_timestamp_ms=funding_timestamp_ms,
            ),
            funding_timestamp_ms=funding_timestamp_ms,
            volume_24h_quote=10_000_000.0,
            open_interest=open_interest,
            open_interest_evidence_status="observed",
            open_interest_evidence_reason="fixture_observed",
            open_interest_observed_at_ms=now_ms,
            open_interest_event_at_ms=now_ms,
            open_interest_received_at_ms=now_ms,
            open_interest_source="test_fixture",
            open_interest_sample_id=open_interest_sample_id(
                venue=venue,
                canonical_symbol="BTCUSDT",
                venue_symbol="BTCUSDT",
                observed_at_ms=now_ms,
                source="test_fixture",
                raw_value=open_interest,
                value_quote=open_interest,
            ),
            open_interest_venue_symbol="BTCUSDT",
            raw_open_interest=open_interest,
            raw_open_interest_unit="quote",
        )
        quotes[f"{venue}:BTCUSDT"] = QuoteSnapshot(**raw_quote)
    candidates: list[CandidateInput] = []
    for index in range(candidate_count):
        raw_candidate = TestPublisher._complete_v3_candidate()
        raw_candidate.update(
            pair_id=f"btcusdt:long->short{index}",
            long_venue="long",
            short_venue=f"short{index}",
            funding_timestamp_ms=funding_timestamp_ms,
            first_funding_timestamp_ms=funding_timestamp_ms,
            long_funding_timestamp_ms=funding_timestamp_ms,
            short_funding_timestamp_ms=funding_timestamp_ms,
        )
        candidates.append(CandidateInput(**raw_candidate))
    diagnostics = {
        "input_quote_count": len(quotes),
        "requested_symbol_count": 1,
        "requested_symbols": ["BTCUSDT"],
        "requested_venues": sorted(venues),
        "directional_pair_count": candidate_count,
        "output_candidate_count": candidate_count,
        "future_input_quote_count": 0,
        "rejection_counts": {},
        "seed_pair_count": candidate_count,
        "pair_decision_count": candidate_count,
        "eligible_candidate_count": candidate_count,
        "omitted_eligible_count": 0,
        "eligible_frontier_complete": True,
        "entry_policy_fingerprint": "a" * 64,
    }
    return SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        candidate_build_observed_at_ms=now_ms,
        candidate_build_diagnostics=diagnostics,
        source_mode="direct_market",
        acquisition_mode="fresh_sidecar",
        funding_lifecycle=[FundingLifecycle(venue, now_ms, 1, 1) for venue in venues],
        market_lifecycle=[MarketLifecycle(venue, now_ms, 1, 1) for venue in venues],
        liquidity_lifecycle=[LiquidityLifecycle(venue, now_ms, 1, 1) for venue in venues],
        quotes=quotes,
        candidates=candidates,
    )


def test_funding_entry_snapshot_manifest_is_installed_after_all_pages(
    tmp_path,
) -> None:
    proof = _v3_snapshot_proof(1_000, input_quote_count=1)
    proof["acquisition_mode"] = "degraded_sidecar"
    snapshot = SidecarSnapshot(
        **proof,
        degraded_venues=["okx"],
        degraded_domains=["liquidity"],
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=1_000,
            )
        },
    )
    path = tmp_path / "audit.json"

    manifest = publish_funding_entry_snapshot(snapshot, path)

    assert manifest["schema_version"] == 7
    assert manifest["page_count"] == len(manifest["pages"])
    assert manifest["eligible_frontier_complete"] is True
    assert manifest["policy_fingerprint_source"] == "derived"
    assert len(manifest["policy_fingerprint"]) == 64
    assert funding_entry_snapshot_identity(path) is not None
    assert funding_entry_snapshot_manifest_path(path).exists()
    assert funding_entry_snapshot_path(path).stat().st_size < 1_000_000
    loaded = load_funding_entry_snapshot(path)
    assert loaded is not None
    assert loaded.ready_at_ms == manifest["ready_at_ms"]
    assert loaded.acquisition_mode == "unavailable"
    assert loaded.quotes == {}
    assert loaded.candidate_build_diagnostics["source_data_ready"] is True
    assert loaded.candidate_build_diagnostics["eligible_frontier_complete"] is True
    assert loaded.candidate_build_diagnostics["entry_frontier_ready"] is True


def test_v7_funding_entry_snapshot_pages_preserve_every_candidate(
    tmp_path,
    monkeypatch,
) -> None:
    import lightfee.sidecar.publisher as publisher

    snapshot = _complete_funding_entry_snapshot(candidate_count=40)
    path = tmp_path / "audit.json"
    monkeypatch.setattr(publisher, "FUNDING_ENTRY_SNAPSHOT_MAX_BYTES", 80_000)

    manifest = publish_funding_entry_snapshot(snapshot, path)
    loaded = load_funding_entry_snapshot(path)

    assert manifest["page_count"] > 1
    assert manifest["candidate_count"] == 40
    assert manifest["eligible_candidate_count"] == 40
    assert manifest["pair_decision_count"] == manifest["seed_pair_count"] == 40
    assert manifest["omitted_eligible_count"] == 0
    assert manifest["eligible_frontier_complete"] is True
    assert manifest["frontier_stop_reason"] == "all_pairs_decided"
    assert all(0 < page["payload_size_bytes"] <= 80_000 for page in manifest["pages"])
    assert loaded is not None
    assert [candidate.pair_id for candidate in loaded.candidates] == [
        f"btcusdt:long->short{index}" for index in range(40)
    ]
    assert len(set(candidate.pair_id for candidate in loaded.candidates)) == 40


def test_v7_marks_only_healthy_complete_empty_frontier_as_ready(tmp_path) -> None:
    snapshot = _complete_funding_entry_snapshot(candidate_count=0)
    path = tmp_path / "audit.json"

    publish_funding_entry_snapshot(snapshot, path)
    loaded = load_funding_entry_snapshot(path)

    assert loaded is not None
    assert loaded.candidates == []
    assert loaded.acquisition_mode == "unavailable"
    assert loaded.candidate_build_diagnostics[
        "complete_empty_frontier_ready"
    ] is True

    degraded = _complete_funding_entry_snapshot(candidate_count=0)
    degraded.acquisition_mode = "degraded_sidecar"
    degraded.degraded_venues = ["long"]
    degraded.funding_lifecycle[0].degraded_reason = "funding evidence degraded"
    degraded_path = tmp_path / "degraded-audit.json"
    publish_funding_entry_snapshot(degraded, degraded_path)
    degraded_loaded = load_funding_entry_snapshot(degraded_path)

    assert degraded_loaded is not None
    assert degraded_loaded.candidate_build_diagnostics[
        "complete_empty_frontier_ready"
    ] is False


def test_v7_funding_entry_snapshot_missing_or_corrupt_page_fails_closed(
    tmp_path,
    monkeypatch,
) -> None:
    import lightfee.sidecar.publisher as publisher

    snapshot = _complete_funding_entry_snapshot(candidate_count=40)
    path = tmp_path / "audit.json"
    monkeypatch.setattr(publisher, "FUNDING_ENTRY_SNAPSHOT_MAX_BYTES", 80_000)
    manifest = publish_funding_entry_snapshot(snapshot, path)
    missing_page = path.parent / manifest["pages"][1]["payload_path"]
    missing_page.unlink()

    assert funding_entry_snapshot_identity(path, verify_digest=True) is None
    assert load_funding_entry_snapshot(path) is None

    manifest = publish_funding_entry_snapshot(snapshot, path)
    corrupt_page = path.parent / manifest["pages"][-1]["payload_path"]
    corrupt_page.write_bytes(corrupt_page.read_bytes() + b"\n")

    assert funding_entry_snapshot_identity(path, verify_digest=True) is None
    assert load_funding_entry_snapshot(path) is None


def test_v7_funding_entry_snapshot_policy_mismatch_fails_closed(tmp_path) -> None:
    snapshot = _complete_funding_entry_snapshot(candidate_count=2)
    path = tmp_path / "audit.json"
    publish_funding_entry_snapshot(snapshot, path)
    manifest_path = funding_entry_snapshot_manifest_path(path)
    manifest = json.loads(manifest_path.read_text())
    manifest["policy_fingerprint"] = "b" * 64
    manifest_path.write_text(json.dumps(manifest, separators=(",", ":")))

    assert load_funding_entry_snapshot(path) is None


def test_v7_publisher_rejects_malformed_explicit_policy_fingerprint(tmp_path) -> None:
    snapshot = _complete_funding_entry_snapshot(candidate_count=1)
    snapshot.candidate_build_diagnostics["entry_policy_fingerprint"] = "not-a-sha256"
    path = tmp_path / "audit.json"

    with pytest.raises(ValueError, match="policy fingerprint"):
        publish_funding_entry_snapshot(snapshot, path)

    assert not funding_entry_snapshot_manifest_path(path).exists()


def test_v7_publisher_rejects_duplicate_candidate_identity(tmp_path) -> None:
    snapshot = _complete_funding_entry_snapshot(candidate_count=2)
    snapshot.candidates[1].pair_id = snapshot.candidates[0].pair_id

    with pytest.raises(ValueError, match="duplicat"):
        publish_funding_entry_snapshot(snapshot, tmp_path / "audit.json")


def test_v7_entry_projection_uses_seed_pair_count_without_double_counting_reasons(
    tmp_path,
) -> None:
    snapshot = _complete_funding_entry_snapshot(candidate_count=2)
    snapshot.candidates[1].blocked = True
    snapshot.candidates[1].blocked_reasons = [
        "outside_scan_window",
        "expected_edge_below_floor",
    ]
    snapshot.candidate_build_diagnostics.update(
        eligible_candidate_count=1,
        rejection_counts={},
        blocked_reason_counts={
            "outside_scan_window": 1,
            "expected_edge_below_floor": 1,
        },
    )

    manifest = publish_funding_entry_snapshot(snapshot, tmp_path / "audit.json")
    loaded = load_funding_entry_snapshot(tmp_path / "audit.json")

    assert loaded is not None
    assert manifest["seed_pair_count"] == 2
    assert manifest["pair_decision_count"] == 2
    assert manifest["candidate_count"] == 1
    assert loaded.candidate_build_diagnostics["directional_pair_count"] == 2
    assert loaded.candidate_build_diagnostics["rejection_counts"] == {
        "not_eligible_for_entry_frontier": 1
    }
    assert loaded.candidate_build_diagnostics["source_rejection_counts"] == {
        "expected_edge_below_floor": 1,
        "outside_scan_window": 1,
    }


def test_v7_incomplete_pair_decision_generation_publishes_no_candidates(
    tmp_path,
) -> None:
    snapshot = _complete_funding_entry_snapshot(candidate_count=2)
    snapshot.candidate_build_diagnostics.update(
        pair_decision_count=1,
        eligible_frontier_complete=False,
        frontier_stop_reason="pair_decision_incomplete",
    )

    manifest = publish_funding_entry_snapshot(snapshot, tmp_path / "audit.json")
    loaded = load_funding_entry_snapshot(tmp_path / "audit.json")

    assert loaded is not None
    assert manifest["eligible_frontier_complete"] is False
    assert manifest["candidate_count"] == 0
    assert manifest["frontier_stop_reason"] == "pair_decision_incomplete"
    assert loaded.candidates == []
    assert loaded.candidate_build_diagnostics["entry_frontier_ready"] is False


def test_v7_reader_accepts_v6_only_until_v7_manifest_exists(tmp_path) -> None:
    import lightfee.sidecar.publisher as publisher

    path = tmp_path / "audit.json"
    snapshot = _complete_funding_entry_snapshot(candidate_count=1)
    data = publisher._snapshot_to_dict(snapshot)
    content = publisher._json_text(data, indent=None)
    generation_id = sha256(content.encode()).hexdigest()
    legacy_base = path.with_name(f"{path.name}.funding-entry-v6.json")
    payload_path = legacy_base.with_name(f"{legacy_base.name}.{generation_id}.json")
    payload_path.write_text(content)
    payload_stat = payload_path.stat()
    legacy_manifest = {
        "schema_version": 6,
        "generation_id": generation_id,
        "payload_path": payload_path.name,
        "payload_size_bytes": payload_stat.st_size,
        "payload_mtime_ns": payload_stat.st_mtime_ns,
        "payload_sha256": sha256(payload_path.read_bytes()).hexdigest(),
        "candidate_count": 1,
        "quote_count": 2,
    }
    legacy_manifest_path = legacy_base.with_name(f"{legacy_base.name}.manifest.json")
    legacy_manifest_path.write_text(json.dumps(legacy_manifest, separators=(",", ":")))

    loaded = load_funding_entry_snapshot(path)
    assert loaded is not None
    assert [candidate.pair_id for candidate in loaded.candidates] == ["btcusdt:long->short0"]

    # Once the V7 path exists it is authoritative. Corruption cannot silently
    # reactivate the older, still-readable V6 generation.
    funding_entry_snapshot_manifest_path(path).write_text("{")
    assert funding_entry_snapshot_identity(path, verify_digest=True) is None
    assert load_funding_entry_snapshot(path) is None


def test_funding_entry_snapshot_rebinds_market_watermark_to_retained_quotes(
    tmp_path,
) -> None:
    selected_observed_at_ms = 9_000
    candidate_build_at_ms = 10_000
    funding_timestamp_ms = 20_000
    quotes: dict[str, QuoteSnapshot] = {}
    for venue, funding_rate_bps in (("binance", 5.0), ("okx", -5.0)):
        raw_quote = TestPublisher._complete_v3_contract_quotes()[f"{venue}:BTCUSDT"]
        open_interest = 2_000_000.0
        raw_quote.update(
            observed_at_ms=selected_observed_at_ms,
            funding_rate_bps=funding_rate_bps,
            funding_rate_observed_at_ms=selected_observed_at_ms,
            funding_rate_event_at_ms=selected_observed_at_ms,
            funding_rate_received_at_ms=selected_observed_at_ms,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=funding_rate_sample_id(
                venue=venue,
                symbol="BTCUSDT",
                observed_at_ms=selected_observed_at_ms,
                rate_bps=funding_rate_bps,
                funding_timestamp_ms=funding_timestamp_ms,
            ),
            funding_timestamp_ms=funding_timestamp_ms,
            volume_24h_quote=10_000_000.0,
            open_interest=open_interest,
            open_interest_evidence_status="observed",
            open_interest_evidence_reason="fixture_observed",
            open_interest_observed_at_ms=selected_observed_at_ms,
            open_interest_event_at_ms=selected_observed_at_ms,
            open_interest_received_at_ms=selected_observed_at_ms,
            open_interest_source="test_fixture",
            open_interest_sample_id=open_interest_sample_id(
                venue=venue,
                canonical_symbol="BTCUSDT",
                venue_symbol="BTCUSDT",
                observed_at_ms=selected_observed_at_ms,
                source="test_fixture",
                raw_value=open_interest,
                value_quote=open_interest,
            ),
            open_interest_venue_symbol="BTCUSDT",
            raw_open_interest=open_interest,
            raw_open_interest_unit="quote",
        )
        quotes[f"{venue}:BTCUSDT"] = QuoteSnapshot(**raw_quote)
    # This unrelated quote owns the full audit snapshot's newest market
    # observation but is intentionally absent from the eligible frontier.
    quotes["bybit:ETHUSDT"] = QuoteSnapshot(
        venue="bybit",
        symbol="ETHUSDT",
        bid=2_000.0,
        ask=2_001.0,
        observed_at_ms=candidate_build_at_ms,
    )
    raw_candidate = TestPublisher._complete_v3_candidate()
    raw_candidate.update(
        funding_timestamp_ms=funding_timestamp_ms,
        first_funding_timestamp_ms=funding_timestamp_ms,
        long_funding_timestamp_ms=funding_timestamp_ms,
        short_funding_timestamp_ms=funding_timestamp_ms,
    )
    snapshot = SidecarSnapshot(
        published_at_ms=candidate_build_at_ms,
        market_observed_at_ms=candidate_build_at_ms,
        candidate_build_observed_at_ms=candidate_build_at_ms,
        candidate_build_diagnostics={
            "input_quote_count": len(quotes),
            "requested_symbol_count": 2,
            "requested_symbols": ["BTCUSDT", "ETHUSDT"],
            "requested_venues": ["binance", "bybit", "okx"],
            "directional_pair_count": 1,
            "output_candidate_count": 1,
            "future_input_quote_count": 0,
            "rejection_counts": {},
            "seed_pair_count": 1,
            "pair_decision_count": 1,
            "eligible_candidate_count": 1,
            "omitted_eligible_count": 0,
            "eligible_frontier_complete": True,
        },
        source_mode="direct_market",
        acquisition_mode="fresh_sidecar",
        funding_lifecycle=[
            FundingLifecycle(
                venue=venue,
                observed_at_ms=candidate_build_at_ms,
                symbol_count=1,
                coverage_usable=1,
            )
            for venue in ("binance", "bybit", "okx")
        ],
        market_lifecycle=[
            MarketLifecycle(
                venue=venue,
                observed_at_ms=candidate_build_at_ms,
                symbol_count=1,
                coverage_usable=1,
            )
            for venue in ("binance", "bybit", "okx")
        ],
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue=venue,
                observed_at_ms=candidate_build_at_ms,
                symbol_count=1,
                coverage_usable=1,
            )
            for venue in ("binance", "bybit", "okx")
        ],
        quotes=quotes,
        candidates=[CandidateInput(**raw_candidate)],
    )
    path = tmp_path / "audit.json"

    publish_funding_entry_snapshot(snapshot, path)
    loaded = load_funding_entry_snapshot(path)

    assert loaded is not None
    assert set(loaded.quotes) == {"binance:BTCUSDT", "okx:BTCUSDT"}
    assert loaded.market_observed_at_ms == selected_observed_at_ms
    assert loaded.candidate_build_observed_at_ms == candidate_build_at_ms


@pytest.mark.parametrize(
    ("oi_status", "oi_reason", "volume_24h_quote", "expect_degraded"),
    [
        (
            "unavailable",
            "entry_targeted_revalidation_required",
            10_000_000.0,
            False,
        ),
        ("timeout", "timeout_waiting_for_oi", 10_000_000.0, True),
        (
            "unavailable",
            "entry_targeted_revalidation_required",
            0.0,
            True,
        ),
    ],
)
def test_funding_entry_snapshot_distinguishes_deferred_and_failed_candidate_oi(
    tmp_path,
    oi_status,
    oi_reason,
    volume_24h_quote,
    expect_degraded,
) -> None:
    """Only the runtime's explicit deferred-OI marker may stay health-neutral.

    Binance-style venue OI is intentionally fetched by the live candidate
    revalidator.  A transport failure is different: it remains visible in the
    compact entry health and must continue to fail closed.
    """
    now_ms = 10_000
    funding_timestamp_ms = 20_000
    quotes: dict[str, QuoteSnapshot] = {}
    for venue, funding_rate_bps in (("binance", 5.0), ("okx", -5.0)):
        raw_quote = TestPublisher._complete_v3_contract_quotes()[f"{venue}:BTCUSDT"]
        raw_quote.update(
            observed_at_ms=now_ms,
            funding_rate_bps=funding_rate_bps,
            funding_rate_observed_at_ms=now_ms,
            funding_rate_event_at_ms=now_ms,
            funding_rate_received_at_ms=now_ms,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=funding_rate_sample_id(
                venue=venue,
                symbol="BTCUSDT",
                observed_at_ms=now_ms,
                rate_bps=funding_rate_bps,
                funding_timestamp_ms=funding_timestamp_ms,
            ),
            funding_timestamp_ms=funding_timestamp_ms,
            volume_24h_quote=(volume_24h_quote if venue == "binance" else 10_000_000.0),
        )
        if venue == "binance":
            raw_quote.update(
                open_interest=None,
                open_interest_evidence_status=oi_status,
                open_interest_evidence_reason=oi_reason,
                open_interest_observed_at_ms=0,
                open_interest_event_at_ms=0,
                open_interest_received_at_ms=now_ms,
                open_interest_source="binance_style_open_interest",
                open_interest_sample_id="",
                open_interest_venue_symbol="BTCUSDT",
            )
        else:
            open_interest = 2_000_000.0
            raw_quote.update(
                open_interest=open_interest,
                open_interest_evidence_status="observed",
                open_interest_evidence_reason="fixture_observed",
                open_interest_observed_at_ms=now_ms,
                open_interest_event_at_ms=now_ms,
                open_interest_received_at_ms=now_ms,
                open_interest_source="test_fixture",
                open_interest_sample_id=open_interest_sample_id(
                    venue=venue,
                    canonical_symbol="BTCUSDT",
                    venue_symbol="BTCUSDT",
                    observed_at_ms=now_ms,
                    source="test_fixture",
                    raw_value=open_interest,
                    value_quote=open_interest,
                ),
                open_interest_venue_symbol="BTCUSDT",
                raw_open_interest=open_interest,
                raw_open_interest_unit="quote",
            )
        quotes[f"{venue}:BTCUSDT"] = QuoteSnapshot(**raw_quote)

    raw_candidate = TestPublisher._complete_v3_candidate()
    raw_candidate.update(
        funding_timestamp_ms=funding_timestamp_ms,
        first_funding_timestamp_ms=funding_timestamp_ms,
        long_funding_timestamp_ms=funding_timestamp_ms,
        short_funding_timestamp_ms=funding_timestamp_ms,
    )
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        candidate_build_observed_at_ms=now_ms,
        candidate_build_diagnostics={
            "input_quote_count": len(quotes),
            "requested_symbol_count": 1,
            "requested_symbols": ["BTCUSDT"],
            "requested_venues": ["binance", "okx"],
            "directional_pair_count": 1,
            "output_candidate_count": 1,
            "future_input_quote_count": 0,
            "rejection_counts": {},
            "seed_pair_count": 1,
            "pair_decision_count": 1,
            "eligible_candidate_count": 1,
            "omitted_eligible_count": 0,
            "eligible_frontier_complete": True,
        },
        source_mode="direct_market",
        acquisition_mode="fresh_sidecar",
        funding_lifecycle=[FundingLifecycle(venue, now_ms, 1, 1) for venue in ("binance", "okx")],
        market_lifecycle=[MarketLifecycle(venue, now_ms, 1, 1) for venue in ("binance", "okx")],
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue,
                now_ms,
                1,
                0 if venue == "binance" else 1,
                "strict_liquidity_proof_missing:1" if venue == "binance" else "",
            )
            for venue in ("binance", "okx")
        ],
        quotes=quotes,
        candidates=[CandidateInput(**raw_candidate)],
    )
    path = tmp_path / "audit.json"

    publish_funding_entry_snapshot(snapshot, path)
    loaded = load_funding_entry_snapshot(path)

    assert loaded is not None
    binance_liquidity = next(row for row in loaded.liquidity_lifecycle if row.venue == "binance")
    assert binance_liquidity.coverage_usable == 0
    assert loaded.candidate_build_diagnostics["entry_targeted_oi_revalidation_required_count"] == (
        0 if expect_degraded else 1
    )
    from scripts import verify_production_services as vps

    health = vps._funding_entry_snapshot_report(
        path,
        now_ms=loaded.ready_at_ms,
        max_age_ms=1_000,
    )
    if expect_degraded:
        assert loaded.acquisition_mode == "degraded_sidecar"
        assert loaded.degraded_venues == ["binance"]
        assert loaded.degraded_domains == ["liquidity"]
        assert binance_liquidity.degraded_reason == "strict_liquidity_proof_missing:1"
        assert health.ok is False
        assert "funding_entry_candidate_evidence_degraded" in health.fingerprints
    else:
        assert loaded.acquisition_mode == "fresh_sidecar"
        assert loaded.degraded_venues == []
        assert loaded.degraded_domains == []
        assert binance_liquidity.degraded_reason == ""
        assert health.ok is True


def test_funding_entry_snapshot_blocked_only_generation_is_unavailable(
    tmp_path,
) -> None:
    proof = _v3_snapshot_proof(
        1_000,
        input_quote_count=2,
        output_candidate_count=1,
    )
    proof["candidate_build_diagnostics"]["requested_venues"] = ["binance", "okx"]
    lifecycle_rows = {
        lifecycle_type.__name__: [
            lifecycle_type(
                venue=venue,
                observed_at_ms=1_000,
                symbol_count=1,
                coverage_usable=1,
            )
            for venue in ("binance", "okx")
        ]
        for lifecycle_type in (FundingLifecycle, MarketLifecycle, LiquidityLifecycle)
    }
    proof["funding_lifecycle"] = lifecycle_rows["FundingLifecycle"]
    proof["market_lifecycle"] = lifecycle_rows["MarketLifecycle"]
    proof["liquidity_lifecycle"] = lifecycle_rows["LiquidityLifecycle"]
    snapshot = SidecarSnapshot(
        **proof,
        quotes={
            f"{venue}:BTCUSDT": QuoteSnapshot(
                venue=venue,
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=1_000,
                funding_rate_bps=1.0,
                funding_rate_observed_at_ms=1_000,
                funding_rate_event_at_ms=1_000,
                funding_rate_received_at_ms=1_000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=f"funding:{venue}:BTCUSDT:1000:1:2000",
                funding_timestamp_ms=2_000,
                funding_interval_ms=28_800_000,
            )
            for venue in ("binance", "okx")
        },
        candidates=[
            CandidateInput(
                long_venue="binance",
                short_venue="okx",
                symbol="BTCUSDT",
                funding_diff_bps=1.0,
                funding_edge_bps=1.0,
                expected_edge_bps=0.0,
                worst_case_edge_bps=-1.0,
                ranking_edge_bps=-1.0,
                funding_timestamp_ms=2_000,
                first_funding_timestamp_ms=2_000,
                long_funding_timestamp_ms=2_000,
                short_funding_timestamp_ms=2_000,
                blocked=True,
                blocked_reasons=["economics_incomplete"],
                economics_incomplete_reason="economics_incomplete",
            )
        ],
    )
    path = tmp_path / "audit.json"

    publish_funding_entry_snapshot(snapshot, path)
    loaded = load_funding_entry_snapshot(path)

    assert loaded is not None
    assert loaded.acquisition_mode == "unavailable"
    assert loaded.candidates == []
    assert loaded.quotes == {}
    assert loaded.candidate_build_diagnostics["diagnostics_only"] is True
    assert loaded.candidate_build_diagnostics["source_candidate_count"] == 1
    assert loaded.candidate_build_diagnostics["source_quote_count"] == 2


def test_entry_snapshot_freshness_age_starts_at_verified_ready_time() -> None:
    snapshot = SidecarSnapshot(
        published_at_ms=1_000,
        ready_at_ms=10_000,
        market_observed_at_ms=10_000,
        candidate_build_observed_at_ms=10_000,
        acquisition_mode="fresh_sidecar",
    )

    decision = decide_snapshot_freshness(
        snapshot,
        max_age_ms=1_000,
        now_ms=10_500,
        market_max_age_ms=1_000,
        usable_payload=lambda _snapshot: True,
    )

    assert decision.freshness == SnapshotFreshness.FRESH


def test_v7_ready_clock_starts_when_manifest_becomes_visible(
    tmp_path,
    monkeypatch,
) -> None:
    import time
    import lightfee.sidecar.publisher as publisher

    snapshot = SidecarSnapshot(
        **_v3_snapshot_proof(1_000, input_quote_count=1),
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=1_000,
            )
        },
    )
    path = tmp_path / "audit.json"
    real_atomic_write = publisher._atomic_write_json
    install_started_at_ms = 0

    def delayed_manifest_install(data, target, **kwargs):
        nonlocal install_started_at_ms
        if Path(target) == funding_entry_snapshot_manifest_path(path):
            time.sleep(0.02)
            install_started_at_ms = time.time_ns() // 1_000_000
        return real_atomic_write(data, target, **kwargs)

    monkeypatch.setattr(publisher, "_atomic_write_json", delayed_manifest_install)

    manifest = publish_funding_entry_snapshot(snapshot, path)
    loaded = load_funding_entry_snapshot(path)

    assert loaded is not None
    assert loaded.ready_at_ms >= install_started_at_ms
    assert manifest["ready_at_ms"] == loaded.ready_at_ms


def test_funding_entry_snapshot_refuses_payload_over_hard_limit(
    tmp_path,
    monkeypatch,
) -> None:
    import lightfee.sidecar.publisher as publisher

    proof = _v3_snapshot_proof(1_000, input_quote_count=1)
    snapshot = SidecarSnapshot(**proof)
    path = tmp_path / "audit.json"
    monkeypatch.setattr(publisher, "FUNDING_ENTRY_SNAPSHOT_MAX_BYTES", 1)

    with pytest.raises(ValueError, match="exceeds hard limit"):
        publish_funding_entry_snapshot(snapshot, path)

    assert not funding_entry_snapshot_path(path).exists()
    assert not funding_entry_snapshot_manifest_path(path).exists()


def test_v7_manifest_failure_keeps_prior_generation_cold_start_readable(
    tmp_path,
    monkeypatch,
) -> None:
    import lightfee.sidecar.publisher as publisher

    path = tmp_path / "audit.json"
    first = SidecarSnapshot(**_v3_snapshot_proof(1_000, input_quote_count=1))
    first_manifest = publish_funding_entry_snapshot(first, path)
    first_payload = path.parent / str(first_manifest["payload_path"])
    real_atomic_write = publisher._atomic_write_json

    def fail_before_manifest_install(data, target, **kwargs):
        if Path(target) == funding_entry_snapshot_manifest_path(path):
            raise RuntimeError("manifest install interrupted")
        return real_atomic_write(data, target, **kwargs)

    monkeypatch.setattr(
        publisher,
        "_atomic_write_json",
        fail_before_manifest_install,
    )

    second = SidecarSnapshot(**_v3_snapshot_proof(2_000, input_quote_count=1))
    with pytest.raises(RuntimeError, match="manifest install interrupted"):
        publish_funding_entry_snapshot(second, path)

    installed_manifest = json.loads(funding_entry_snapshot_manifest_path(path).read_text())
    loaded = load_funding_entry_snapshot(path)
    assert installed_manifest["generation_id"] == first_manifest["generation_id"]
    assert first_payload.exists()
    assert loaded is not None
    assert loaded.published_at_ms == 1_000


def test_v7_successful_manifest_install_prunes_prior_generation(tmp_path) -> None:
    path = tmp_path / "audit.json"
    first = SidecarSnapshot(**_v3_snapshot_proof(1_000, input_quote_count=1))
    first_manifest = publish_funding_entry_snapshot(first, path)
    first_payload = path.parent / str(first_manifest["payload_path"])

    second = SidecarSnapshot(**_v3_snapshot_proof(2_000, input_quote_count=1))
    second_manifest = publish_funding_entry_snapshot(second, path)
    second_payload = path.parent / str(second_manifest["payload_path"])

    assert first_manifest["generation_id"] != second_manifest["generation_id"]
    assert first_payload != second_payload
    assert not first_payload.exists()
    assert second_payload.exists()
    assert funding_entry_snapshot_path(path) == second_payload
    loaded = load_funding_entry_snapshot(path)
    assert loaded is not None
    assert loaded.published_at_ms == 2_000


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
