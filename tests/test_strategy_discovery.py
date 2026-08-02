"""Tests for strategy discovery, scoring, and market view."""

import json
from pathlib import Path

from lightfee.config.schema import StrategyConfig
from lightfee.sidecar.pairing import build_same_symbol_pairs
from lightfee.sidecar.snapshot import (
    CandidateInput,
    QuoteSnapshot,
    funding_rate_sample_id,
)
from lightfee.strategy.discovery import discover_tradeable_candidates
from lightfee.strategy.market_view import (
    compute_raw_cross_bps,
    compute_reference_mid,
    select_maker_leg,
)


FUNDING_TS_MS = 600_000
NOW_IN_SCAN_WINDOW_MS = 0


class TestMarketView:
    def test_reference_mid(self):
        long_q = QuoteSnapshot(venue="a", symbol="BTCUSDT", bid=49900, ask=50000)
        short_q = QuoteSnapshot(venue="b", symbol="BTCUSDT", bid=50100, ask=50200)
        mid = compute_reference_mid(long_q, short_q)
        assert mid == (50000 + 50100) / 2.0

    def test_raw_cross_bps_positive(self):
        long_q = QuoteSnapshot(venue="a", symbol="BTCUSDT", bid=49900, ask=50000)
        short_q = QuoteSnapshot(venue="b", symbol="BTCUSDT", bid=50100, ask=50200)
        cross = compute_raw_cross_bps(long_q, short_q)
        # mid = (50000 + 50100) / 2 = 50050; cross = 100 / 50050 * 10000
        expected = (100.0 / 50050.0) * 10000.0
        assert abs(cross - expected) < 0.01

    def test_select_maker_leg(self):
        long_q = QuoteSnapshot(venue="a", symbol="BTCUSDT", bid=49900, ask=50000)  # spread=100
        short_q = QuoteSnapshot(venue="b", symbol="BTCUSDT", bid=50100, ask=50150)  # spread=50
        assert select_maker_leg(long_q, short_q) == "long"


class TestDiscovery:
    def test_transient_rejection_does_not_mutate_cached_candidate(self):
        candidate = CandidateInput(
            long_venue="binance",
            short_venue="okx",
            symbol="BTCUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=10.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5,
            entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS,
        )
        rejecting = StrategyConfig(
            funding_new_entries_enabled=True,
            min_expected_edge_bps=6.0,
        )
        accepting = StrategyConfig(
            funding_new_entries_enabled=True,
            min_expected_edge_bps=1.0,
        )

        for _ in range(100):
            assert discover_tradeable_candidates([candidate], rejecting, 0) == []
        assert candidate.blocked_reasons == []
        assert discover_tradeable_candidates([candidate], accepting, 0) == [candidate]

    def test_entry_freeze_blocks_only_new_funding_candidates(self):
        config = StrategyConfig(funding_new_entries_enabled=False)
        candidate = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5, entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS,
        )

        assert discover_tradeable_candidates([candidate], config, 0) == []
        assert candidate.blocked_reasons == []

    def test_entry_freeze_treats_non_boolean_config_as_disabled(self):
        config = StrategyConfig(funding_new_entries_enabled="false")  # type: ignore[arg-type]
        candidate = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5, entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS,
        )

        assert discover_tradeable_candidates([candidate], config, 0) == []
        assert candidate.blocked_reasons == []

    def test_live_discovery_requires_complete_economics(self):
        config = StrategyConfig()
        candidate = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5, entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS, economics_complete=False,
        )

        assert discover_tradeable_candidates(
            [candidate], config, 0, require_complete_economics=True
        ) == []
        assert candidate.blocked_reasons == []

    def test_live_discovery_rejects_truthy_non_boolean_economics_flag(self):
        config = StrategyConfig(funding_new_entries_enabled=True)
        candidate = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5, entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS,
            economics_complete="true",  # type: ignore[arg-type]
            economics_observed_at_ms=1,
        )

        assert discover_tradeable_candidates(
            [candidate], config, 0, require_complete_economics=True
        ) == []
        assert candidate.blocked_reasons == []

    def test_filters_below_funding_edge_floor(self):
        config = StrategyConfig(min_funding_edge_bps=6.0, max_concurrent_positions=8)
        candidates = [
            CandidateInput(
                long_venue="binance", short_venue="okx", symbol="BTCUSDT",
                funding_diff_bps=3.0, funding_edge_bps=3.0,
                expected_edge_bps=5.0, worst_case_edge_bps=2.0,
                ranking_edge_bps=2.5,
            )
        ]
        result = discover_tradeable_candidates(candidates, config, 0)
        assert len(result) == 0

    def test_passes_above_all_floors(self):
        config = StrategyConfig(
            min_funding_edge_bps=6.0, min_expected_edge_bps=1.0, min_worst_case_edge_bps=0.0,
            max_concurrent_positions=8, funding_new_entries_enabled=True,
        )
        candidates = [
            CandidateInput(
                long_venue="binance", short_venue="okx", symbol="BTCUSDT",
                funding_diff_bps=10.0, funding_edge_bps=10.0,
                expected_edge_bps=5.0, worst_case_edge_bps=2.0,
                ranking_edge_bps=2.5, entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,  # V1 parity: required for tradeable
            )
        ]
        result = discover_tradeable_candidates(candidates, config, NOW_IN_SCAN_WINDOW_MS)
        assert len(result) == 1

    def test_static_economics_filter_but_notional_is_a_sizing_constraint(self):
        config = StrategyConfig(
            funding_new_entries_enabled=True,
            min_funding_edge_bps=0.0,
            min_expected_edge_bps=8.0,
            min_worst_case_edge_bps=3.0,
        )

        def candidate(symbol: str, *, expected: float, worst: float, notional: float):
            return CandidateInput(
                long_venue="bybit",
                short_venue="okx",
                symbol=symbol,
                funding_diff_bps=20.0,
                funding_edge_bps=20.0,
                expected_edge_bps=expected,
                worst_case_edge_bps=worst,
                ranking_edge_bps=20.0,
                entry_notional_quote=notional,
                first_funding_timestamp_ms=FUNDING_TS_MS,
            )

        low_expected = candidate("LOW_EXPECTED", expected=7.99, worst=4.0, notional=30.0)
        low_worst = candidate("LOW_WORST", expected=9.0, worst=2.99, notional=30.0)
        oversized = candidate("OVERSIZED", expected=9.0, worst=4.0, notional=30.01)
        eligible = candidate("ELIGIBLE", expected=9.0, worst=4.0, notional=30.0)

        result = discover_tradeable_candidates(
            [low_expected, low_worst, oversized, eligible],
            config,
            NOW_IN_SCAN_WINDOW_MS,
        )

        assert result == [oversized, eligible]
        assert low_expected.blocked_reasons == []
        assert low_worst.blocked_reasons == []
        assert oversized.blocked_reasons == []

    def test_ranks_by_ranking_edge_desc(self):
        config = StrategyConfig(max_concurrent_positions=8, funding_new_entries_enabled=True)
        candidates = [
            CandidateInput(
                long_venue="a", short_venue="b", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=2.0,
                entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,  # V1 parity: required for tradeable
            ),
            CandidateInput(
                long_venue="a", short_venue="c", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
                entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,  # V1 parity: required for tradeable
            ),
        ]
        result = discover_tradeable_candidates(candidates, config, NOW_IN_SCAN_WINDOW_MS)
        assert result[0].ranking_edge_bps == 5.0

    def test_preserves_v1_shortlist_pool_beyond_position_capacity(self):
        config = StrategyConfig(max_concurrent_positions=2, funding_new_entries_enabled=True)
        candidates = [
            CandidateInput(
                long_venue="a", short_venue=f"b{i}", symbol=f"S{i}",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2,
                ranking_edge_bps=float(10 - i),
                entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,
            )
            for i in range(5)
        ]

        result = discover_tradeable_candidates(candidates, config, NOW_IN_SCAN_WINDOW_MS)

        assert [c.symbol for c in result] == ["S0", "S1", "S2", "S3", "S4"]

    def test_skips_blocked_candidates(self):
        config = StrategyConfig(max_concurrent_positions=8)
        candidates = [
            CandidateInput(
                long_venue="a", short_venue="b", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
                blocked=True, blocked_reasons=["test"],
            )
        ]
        result = discover_tradeable_candidates(candidates, config, 0)
        assert len(result) == 0

    def test_max_scan_zero_falls_back_to_entry_window(self):
        config = StrategyConfig(
            max_scan_minutes_before_funding=0,
            min_scan_minutes_before_funding=3,
            entry_window_secs=480,
            max_concurrent_positions=8,
            min_funding_edge_bps=0,
        )
        candidate = CandidateInput(
            long_venue="a", short_venue="b", symbol="X",
            funding_diff_bps=10, funding_edge_bps=10,
            expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
            entry_notional_quote=100.0,
            first_funding_timestamp_ms=600_000,
        )

        result = discover_tradeable_candidates([candidate], config, 0)

        assert result == []
        assert candidate.blocked_reasons == []


class TestZeroSizeGate:
    """V2 fix: entry_notional_quote must be non-zero for tradeable candidates."""

    def test_zero_entry_notional_blocked(self):
        config = StrategyConfig(
            max_concurrent_positions=8,
            min_funding_edge_bps=0,
            funding_new_entries_enabled=True,
        )
        candidates = [
            CandidateInput(
                long_venue="a", short_venue="b", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
                entry_notional_quote=0.0,  # V2 fix: zero should be blocked
                first_funding_timestamp_ms=FUNDING_TS_MS,
            )
        ]
        result = discover_tradeable_candidates(candidates, config, 0)
        assert len(result) == 0

    def test_nonzero_entry_notional_passes(self):
        config = StrategyConfig(
            max_concurrent_positions=8,
            min_funding_edge_bps=0,
            funding_new_entries_enabled=True,
        )
        candidates = [
            CandidateInput(
                long_venue="a", short_venue="b", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
                entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,
            )
        ]
        result = discover_tradeable_candidates(candidates, config, NOW_IN_SCAN_WINDOW_MS)
        assert len(result) == 1


class TestV1OracleDifferential:
    """Run the frozen V1 result through the real V2 discovery boundary.

    ``raw_input.json`` is the only source for V2 inputs.  ``raw_output.json``
    is loaded only after production discovery has returned and is used only as
    the exact comparison right-hand side.
    """

    FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "v1_oracle"

    _COMPARABLE_FIELDS = (
        "pair_id",
        "symbol",
        "long_venue",
        "short_venue",
        "funding_edge_bps",
        "expected_edge_bps",
        "worst_case_edge_bps",
        "ranking_edge_bps",
        "quantity",
        "entry_notional_quote",
        "entry_cross_bps",
        "entry_maker_leg",
        "first_funding_leg",
        "first_funding_timestamp_ms",
        "blocked_reasons",
    )

    def _fixture(self, name: str) -> dict:
        with (self.FIXTURE_DIR / name).open() as handle:
            return json.load(handle)

    @staticmethod
    def _v1_enum_label(value: object) -> str:
        """Canonicalize only the enum spelling difference between V1 and V2."""
        return str(value).rsplit(".", 1)[-1].capitalize()

    def _build_v2_input(self, raw_input: dict) -> tuple[dict[str, QuoteSnapshot], StrategyConfig]:
        """Adapt raw V1 records to the V2 production input model.

        The only arithmetic here converts V1's decimal funding-rate unit to
        V2's basis-point unit.  No V1 expected field is read or reconstructed.
        Contract and evidence fields are explicit raw-input records because V2
        correctly refuses to infer them from BBO values.
        """
        paired = raw_input["paired_opportunity"]
        funding_rate_by_venue_bps = {
            paired["long_venue"]: float(paired["long_funding_rate"]) * 10_000.0,
            paired["short_venue"]: float(paired["short_funding_rate"]) * 10_000.0,
        }
        observed_at_ms = int(raw_input["market"]["observed_at_ms"])
        quotes: dict[str, QuoteSnapshot] = {}
        for raw_quote in raw_input["market"]["quotes"]:
            venue = str(raw_quote["venue"])
            symbol = str(raw_quote["symbol"])
            funding_timestamp_ms = int(raw_quote["funding_timestamp_ms"])
            evidence = raw_quote["funding_evidence"]
            contract = raw_quote["contract"]
            rate_bps = funding_rate_by_venue_bps[venue]
            assert evidence["sample_id"] == funding_rate_sample_id(
                venue=venue,
                symbol=symbol,
                observed_at_ms=int(evidence["observed_at_ms"]),
                rate_bps=rate_bps,
                funding_timestamp_ms=funding_timestamp_ms,
            )
            quotes[f"{venue}:{symbol}"] = QuoteSnapshot(
                venue=venue,
                symbol=symbol,
                bid=float(raw_quote["bid"]),
                ask=float(raw_quote["ask"]),
                observed_at_ms=observed_at_ms,
                bid_size=float(raw_quote["bid_size"]),
                ask_size=float(raw_quote["ask_size"]),
                funding_rate_bps=rate_bps,
                funding_rate_observed_at_ms=int(evidence["observed_at_ms"]),
                funding_rate_event_at_ms=int(evidence["event_at_ms"]),
                funding_rate_received_at_ms=int(evidence["received_at_ms"]),
                funding_rate_source=str(evidence["source"]),
                funding_rate_sample_id=str(evidence["sample_id"]),
                funding_timestamp_ms=funding_timestamp_ms,
                funding_interval_ms=int(contract["funding_interval_ms"]),
                mark_price=float(raw_quote["mark_price"]),
                underlying=str(contract["underlying"]),
                quote_currency=str(contract["quote_currency"]),
                contract_type=str(contract["contract_type"]),
                contract_multiplier=float(contract["contract_multiplier"]),
                mark_index_source=str(contract["mark_index_source"]),
                price_precision=int(contract["price_precision"]),
                quantity_precision=int(contract["quantity_precision"]),
                price_tick=float(contract["price_tick"]),
                quantity_step_base=float(contract["quantity_step_base"]),
                min_quantity_base=float(contract["min_quantity_base"]),
                min_notional_quote=float(contract["min_notional_quote"]),
                min_notional_evidence_complete=bool(
                    contract["min_notional_evidence_complete"]
                ),
                venue_status=str(contract["venue_status"]),
                contract_normalization_complete=bool(
                    contract["contract_normalization_complete"]
                ),
            )

        return quotes, StrategyConfig(**raw_input["strategy"])

    def _run_v2_production(self, raw_input: dict) -> list[CandidateInput]:
        quotes, strategy = self._build_v2_input(raw_input)
        venues = {
            str(venue["venue"]): venue
            for venue in raw_input["venues"]
            if bool(venue["enabled"])
        }
        return build_same_symbol_pairs(
            quotes,
            list(raw_input["symbols"]),
            strategy=strategy,
            venue_fee_bps={
                venue: float(values["taker_fee_bps"])
                for venue, values in venues.items()
            },
            venue_maker_fee_bps={
                venue: float(values["maker_fee_bps"])
                for venue, values in venues.items()
            },
            venue_notional_caps={
                venue: float(values["max_notional"])
                for venue, values in venues.items()
            },
            passive_execution_enabled=bool(
                raw_input["runtime"]["passive_execution_enabled"]
            ),
            observed_at_ms=int(raw_input["market"]["observed_at_ms"]),
        )

    @classmethod
    def _canonicalize_actual(cls, candidate: CandidateInput) -> dict[str, object]:
        """Keep only the explicit V1 result contract and normalize spelling."""
        return {
            "pair_id": candidate.pair_id,
            "symbol": candidate.symbol,
            "long_venue": candidate.long_venue,
            "short_venue": candidate.short_venue,
            "funding_edge_bps": candidate.funding_edge_bps,
            "expected_edge_bps": candidate.expected_edge_bps,
            "worst_case_edge_bps": candidate.worst_case_edge_bps,
            "ranking_edge_bps": candidate.ranking_edge_bps,
            "quantity": candidate.entry_target_quantity,
            "entry_notional_quote": candidate.entry_notional_quote,
            "entry_cross_bps": candidate.entry_cross_bps,
            "entry_maker_leg": cls._v1_enum_label(candidate.entry_maker_leg),
            "first_funding_leg": cls._v1_enum_label(candidate.first_funding_leg),
            "first_funding_timestamp_ms": candidate.first_funding_timestamp_ms,
            "blocked_reasons": list(candidate.blocked_reasons),
        }

    @classmethod
    def _canonicalize_expected(cls, row: dict[str, object]) -> dict[str, object]:
        assert set(row) == set(cls._COMPARABLE_FIELDS)
        return {
            **row,
            "entry_maker_leg": cls._v1_enum_label(row["entry_maker_leg"]),
            "first_funding_leg": cls._v1_enum_label(row["first_funding_leg"]),
        }

    def _assert_exact(self, actual: list[CandidateInput], expected_rows: list[dict]) -> None:
        actual_rows = [self._canonicalize_actual(candidate) for candidate in actual]
        expected = [self._canonicalize_expected(row) for row in expected_rows]
        assert actual_rows == expected

    def test_v2_production_result_matches_v1_oracle_exactly(self):
        metadata = self._fixture("oracle_fixture_metadata.json")
        raw_input = self._fixture(metadata["raw_input"])
        assert metadata["v1_commit"] == (
            "ca4b1667ed8e59e05de847934d7182d6fbfaecbc"
        )
        assert metadata["source_hashes"]["discovery.rs"] == (
            "a862e498220f846d2270f592095c75019a1c7689e62a1a04f282164add0a0e65"
        )
        assert metadata["entry_sizing_mode"] == "liquidity_aware"
        assert metadata["canonicalization"]["comparable_fields"] == list(
            self._COMPARABLE_FIELDS
        )

        actual = self._run_v2_production(raw_input)
        expected_rows = self._fixture(metadata["v1_expected"])
        self._assert_exact(actual, expected_rows)
