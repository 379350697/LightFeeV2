"""Entry local-L2 sessions tests — tracked opportunities, readiness, promotion.

Rust V1 reference: src/execution_core/entry_local_l2.rs
                      src/execution_core/entry_local_l2_sessions.rs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from lightfee.engine.entry_local_l2 import (
    EntryLocalL2LegFault,
    EntryLocalL2LegSession,
    EntryLocalL2LegState,
    EntryLocalL2Session,
    EntryLocalL2SessionRuntime,
    EntryLocalL2SessionState,
    SessionArmingReason,
    TrackedOpportunity,
    TrackedOpportunityClass,
    deduplicated_tracked_legs,
    make_candidate_pair_id,
    primary_hold_window_allows_replacement,
    select_tracked_opportunities,
    shadow_promotion_is_eligible,
)
from lightfee.marketdata.open_interest import open_interest_sample_id

if TYPE_CHECKING:
    from lightfee.sidecar.snapshot import CandidateInput


def _install_v7_file_snapshot_fixture(monkeypatch) -> None:
    """Expose legacy JSON fixtures through the live V7 entry boundary."""
    from pathlib import Path

    from lightfee.sidecar.publisher import load_snapshot

    def identity(path):
        try:
            stat = Path(path).stat()
        except OSError:
            return None
        return ("test-v7-file", stat.st_mtime_ns, stat.st_size)

    monkeypatch.setattr(
        "lightfee.engine.runtime.funding_entry_snapshot_identity",
        identity,
    )
    monkeypatch.setattr(
        "lightfee.engine.runtime.load_funding_entry_snapshot",
        load_snapshot,
    )


def _install_v7_object_snapshot_fixture(monkeypatch, snapshot) -> None:
    monkeypatch.setattr(
        "lightfee.engine.runtime.funding_entry_snapshot_identity",
        lambda _path: ("test-v7-object", 1, 1),
    )
    monkeypatch.setattr(
        "lightfee.engine.runtime.load_funding_entry_snapshot",
        lambda _path: snapshot,
    )


def _allow_test_entry_account_truth(monkeypatch, runtime) -> None:
    async def ready() -> bool:
        return True

    monkeypatch.setattr(runtime, "_entry_account_truth_ready_for_tick", ready)


def _complete_v3_economics_fields(
    funding_edge_bps: float,
    observed_at_ms: int,
    *,
    entry_notional_quote: float = 30.0,
) -> dict[str, float | int | bool | str]:
    """Raw sidecar fields for tests that must reach post-economics gates."""
    return {
        "funding_edge_bps": funding_edge_bps,
        "expected_edge_bps": funding_edge_bps,
        "worst_case_edge_bps": funding_edge_bps,
        "ranking_edge_bps": funding_edge_bps,
        "first_stage_funding_edge_bps": funding_edge_bps,
        "first_stage_expected_edge_bps": funding_edge_bps,
        "first_stage_worst_case_edge_bps": funding_edge_bps,
        "second_stage_incremental_funding_edge_bps": 0.0,
        "second_stage_worst_case_funding_edge_bps": 0.0,
        "stagger_gap_ms": 0,
        "entry_notional_quote": entry_notional_quote,
        "entry_target_quantity": entry_notional_quote / 50_000.0,
        "entry_max_executable_quantity": entry_notional_quote / 50_000.0,
        "gross_signal_edge_bps": 0.0,
        "entry_cross_bps": 0.0,
        "expected_exit_cross_bps": 0.0,
        "entry_fee_bps": 0.0,
        "exit_fee_bps": 0.0,
        # Aggregate round-trip fee contract; it must equal entry + exit.
        "fee_bps": 0.0,
        "entry_slippage_bps": 0.0,
        "exit_slippage_bps": 0.0,
        "adverse_selection_bps": 0.0,
        "capital_buffer_bps": 0.0,
        "execution_buffer_bps": 0.0,
        "venue_risk_haircut_bps": 0.0,
        "transfer_or_inventory_bias_bps": 0.0,
        "expected_net_edge_bps": funding_edge_bps,
        "long_taker_fee_bps": 0.0,
        "short_taker_fee_bps": 0.0,
        "taker_fee_evidence_complete": True,
        "forecast_distribution_stable": False,
        "forecast_stability_reason": "not_calibrated",
        "forecast_worst_funding_edge_bps": funding_edge_bps,
        "economics_complete": True,
        "economics_observed_at_ms": observed_at_ms,
        "calculation_version": "v1_exact",
        "model_epoch": "v1_exact",
    }


def _complete_candidate_funding_timestamps(
    timestamp_ms: int,
    *,
    long_timestamp_ms: int | None = None,
    short_timestamp_ms: int | None = None,
) -> dict[str, int]:
    """Directed-pair funding schedule proof required by funding-live."""
    long_timestamp_ms = timestamp_ms if long_timestamp_ms is None else long_timestamp_ms
    short_timestamp_ms = timestamp_ms if short_timestamp_ms is None else short_timestamp_ms
    return {
        "funding_timestamp_ms": min(long_timestamp_ms, short_timestamp_ms),
        "first_funding_timestamp_ms": min(long_timestamp_ms, short_timestamp_ms),
        "long_funding_timestamp_ms": long_timestamp_ms,
        "short_funding_timestamp_ms": short_timestamp_ms,
    }


def _complete_v3_contract_quote(
    venue: str,
    symbol: str,
    **market_fields: float | int | str,
) -> dict[str, float | int | str | bool]:
    """Quote evidence required for a V3 live candidate's common base unit."""
    observed_at_ms = int(market_fields.get("observed_at_ms", 0) or 0)
    open_interest = float(market_fields.get("open_interest", 0.0) or 0.0)
    return {
        "venue": venue,
        "symbol": symbol,
        "funding_interval_ms": 28_800_000,
        "underlying": symbol.removesuffix("USDT"),
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
        "open_interest_evidence_status": "observed",
        "open_interest_evidence_reason": "test_fixture",
        "open_interest_observed_at_ms": observed_at_ms,
        "open_interest_received_at_ms": observed_at_ms,
        "open_interest_source": "test_fixture",
        "open_interest_sample_id": open_interest_sample_id(
            venue=venue,
            canonical_symbol=symbol,
            venue_symbol=symbol,
            observed_at_ms=observed_at_ms,
            source="test_fixture",
            raw_value=open_interest,
            value_quote=open_interest,
        ),
        "open_interest_venue_symbol": symbol,
        "raw_open_interest": open_interest,
        "raw_open_interest_unit": "quote",
        "open_interest_contract_multiplier": 1.0,
        "open_interest_conversion_mark_price": None,
        **market_fields,
    }


def _v3_candidate_build_proof(
    observed_at_ms: int,
    *,
    input_quote_count: int,
    output_candidate_count: int,
    requested_symbols: list[str] | None = None,
) -> dict[str, object]:
    normalized_requested_symbols = (
        requested_symbols
        if requested_symbols is not None
        else (["BTCUSDT"] if output_candidate_count else [])
    )
    symbol_count = len(normalized_requested_symbols)

    def lifecycle_rows() -> list[dict[str, object]]:
        return [
            {
                "venue": venue,
                "observed_at_ms": observed_at_ms,
                "symbol_count": symbol_count,
                "coverage_usable": symbol_count,
                "degraded_reason": "",
            }
            for venue in ("binance", "bybit")
        ]

    return {
        "candidate_build_observed_at_ms": observed_at_ms,
        "candidate_build_diagnostics": {
            "input_quote_count": input_quote_count,
            "requested_symbol_count": len(normalized_requested_symbols),
            "requested_symbols": normalized_requested_symbols,
            "requested_venues": ["binance", "bybit"],
            "directional_pair_count": output_candidate_count,
            "output_candidate_count": output_candidate_count,
            "future_input_quote_count": 0,
            "rejection_counts": {},
        },
        "funding_lifecycle": lifecycle_rows(),
        "market_lifecycle": lifecycle_rows(),
        "transfer_lifecycle": [],
        "liquidity_lifecycle": lifecycle_rows(),
    }


# ---------------------------------------------------------------------------
# TrackedOpportunity
# ---------------------------------------------------------------------------


class TestTrackedOpportunity:
    def test_primary_class(self):
        opp = TrackedOpportunity(
            pair_id="p1", symbol="BTCUSDT",
            long_venue="binance", short_venue="bybit",
            ranking_edge_bps=15.0, class_=TrackedOpportunityClass.PRIMARY,
        )
        assert opp.class_ == TrackedOpportunityClass.PRIMARY

    def test_shadow_is_default(self):
        opp = TrackedOpportunity(
            pair_id="p2", symbol="ETHUSDT",
            long_venue="binance", short_venue="okx",
            ranking_edge_bps=10.0,
        )
        assert opp.class_ == TrackedOpportunityClass.SHADOW


# ---------------------------------------------------------------------------
# EntryLocalL2LegSession
# ---------------------------------------------------------------------------


class TestLegSession:
    def test_new_leg_is_arming(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        assert leg.state == EntryLocalL2LegState.ARMING

    def test_mark_ready(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        leg.mark_ready(seen_at_ms=10000)
        assert leg.state == EntryLocalL2LegState.READY
        assert leg.fault is None

    def test_mark_faulted(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        leg.mark_faulted(EntryLocalL2LegFault.STALE_BOOK, "age=7000ms", seen_at_ms=10000)
        assert leg.state == EntryLocalL2LegState.FAULTED
        assert leg.fault == EntryLocalL2LegFault.STALE_BOOK
        assert leg.fault_detail == "age=7000ms"

    def test_mark_arming_sets_reason(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        leg.mark_arming(SessionArmingReason.FIRST_SESSION)
        assert leg.state == EntryLocalL2LegState.ARMING
        assert leg.arming_reason == SessionArmingReason.FIRST_SESSION

    def test_is_stale_no_data(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        assert leg.is_stale(now_ms=10000, stale_after_ms=5000)

    def test_is_stale_within_window(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT", last_seen_at_ms=10000)
        assert not leg.is_stale(now_ms=12000, stale_after_ms=5000)

    def test_is_stale_exceeds_window(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT", last_seen_at_ms=10000)
        assert leg.is_stale(now_ms=16000, stale_after_ms=5000)

    def test_is_ready(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        leg.mark_ready(seen_at_ms=10000)
        assert leg.is_ready(now_ms=12000, stale_after_ms=5000)

    def test_not_ready_when_faulted(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        leg.mark_faulted(EntryLocalL2LegFault.STALE_BOOK, seen_at_ms=10000)
        assert not leg.is_ready(now_ms=12000, stale_after_ms=5000)

    def test_not_ready_when_stale(self):
        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        leg.mark_ready(seen_at_ms=10000)
        assert not leg.is_ready(now_ms=16000, stale_after_ms=5000)


class TestApplyBookReadinessToLeg:
    """Book readiness must be the only source that promotes entry-L2 legs."""

    @staticmethod
    def _book(
        venue: str = "binance",
        symbol: str = "BTCUSDT",
        *,
        status=None,
        observed_at_ms: int = 10000,
        bid: float = 50000.0,
        ask: float = 50100.0,
        sequence: int = 7,
        fault_reason: str = "",
    ):
        from lightfee.marketdata.l2 import L2BookStatus, LocalL2Book, PriceLevel

        book = LocalL2Book(venue=venue, symbol=symbol)
        book.status = status or L2BookStatus.HOT
        book.bids = [PriceLevel(price=bid, quantity=1.0)]
        book.asks = [PriceLevel(price=ask, quantity=1.0)]
        book.observed_at_ms = observed_at_ms
        book.sequence = sequence
        book.fault_reason = fault_reason
        return book

    def test_missing_book_keeps_leg_arming_with_stable_reason(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        diag = apply_book_readiness_to_leg(leg, None, now_ms=10000, stale_after_ms=5000)

        assert leg.state == EntryLocalL2LegState.ARMING
        assert diag == {
            "venue": "binance",
            "symbol": "BTCUSDT",
            "ready": False,
            "reason": "book_missing",
            "detail": "book not found",
            "book_status": "missing",
            "age_ms": None,
            "observed_at_ms": 0,
            "sequence": 0,
        }

    def test_hot_fresh_non_crossed_book_marks_leg_ready(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        book = self._book(observed_at_ms=9500, bid=50000.0, ask=50100.0)

        diag = apply_book_readiness_to_leg(leg, book, now_ms=10000, stale_after_ms=1000)

        assert leg.state == EntryLocalL2LegState.READY
        assert leg.last_seen_at_ms == 9500
        assert diag["ready"] is True
        assert diag["reason"] == "ready"
        assert diag["detail"] == "local_l2_book_hot_fresh"
        assert diag["book_status"] == "hot"
        assert diag["age_ms"] == 500
        assert diag["observed_at_ms"] == 9500
        assert diag["sequence"] == 7

    def test_stale_crossed_book_reports_stale_before_crossed(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        book = self._book(observed_at_ms=4000, bid=50100.0, ask=50100.0)

        diag = apply_book_readiness_to_leg(leg, book, now_ms=10000, stale_after_ms=5000)

        assert leg.state == EntryLocalL2LegState.FAULTED
        assert leg.fault == EntryLocalL2LegFault.STALE_BOOK
        assert diag["reason"] == "stale_book"
        assert diag["detail"] == "age_ms=6000 stale_after_ms=5000"

    @pytest.mark.parametrize(
        "book_factory,expected_reason,expected_state,expected_detail",
        [
            (
                lambda self: self._book(observed_at_ms=4000),
                "stale_book",
                EntryLocalL2LegState.FAULTED,
                "age_ms=6000 stale_after_ms=5000",
            ),
            (
                lambda self: self._book(bid=50100.0, ask=50100.0),
                "crossed_or_locked_book",
                EntryLocalL2LegState.FAULTED,
                "best_bid=50100.0 best_ask=50100.0",
            ),
            (
                lambda self: self._book(
                    status=pytest.importorskip("lightfee.marketdata.l2").L2BookStatus.DEGRADED,
                    fault_reason="transport_failure",
                ),
                "book_degraded",
                EntryLocalL2LegState.FAULTED,
                "transport_failure",
            ),
            (
                lambda self: self._book(
                    status=pytest.importorskip("lightfee.marketdata.l2").L2BookStatus.BOOTSTRAPPING,
                ),
                "book_bootstrapping",
                EntryLocalL2LegState.ARMING,
                "book_status=bootstrapping",
            ),
        ],
    )
    def test_not_ready_books_keep_stable_reason_and_detail(
        self, book_factory, expected_reason, expected_state, expected_detail,
    ):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
        diag = apply_book_readiness_to_leg(
            leg, book_factory(self), now_ms=10000, stale_after_ms=5000,
        )

        assert leg.state == expected_state
        assert diag["ready"] is False
        assert diag["reason"] == expected_reason
        assert diag["detail"] == expected_detail


# ---------------------------------------------------------------------------
# EntryLocalL2Session
# ---------------------------------------------------------------------------


class TestEntryLocalL2Session:
    def test_new_session_is_arming(self):
        session = EntryLocalL2Session(pair_id="p1")
        assert session.state == EntryLocalL2SessionState.ARMING
        assert len(session.legs) == 0

    def test_both_legs_ready(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=10000)
        assert session.both_legs_ready(now_ms=12000, stale_after_ms=5000)

    def test_both_legs_not_ready_when_one_faulted(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.ensure_leg("bybit", "BTCUSDT").mark_faulted(
            EntryLocalL2LegFault.STALE_BOOK, seen_at_ms=10000,
        )
        assert not session.both_legs_ready(now_ms=12000, stale_after_ms=5000)

    def test_ready_leg_count(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.ensure_leg("bybit", "BTCUSDT").mark_arming(SessionArmingReason.FIRST_SESSION)
        assert session.ready_leg_count(now_ms=12000, stale_after_ms=5000) == 1

    def test_faulted_leg_count(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_faulted(EntryLocalL2LegFault.STALE_BOOK)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=10000)
        assert session.faulted_leg_count() == 1

    def test_stale_leg_count(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=15000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=8000)
        # only bybit leg last seen 8000, now 16000, stale_after=5000 → stale
        # binance leg last seen 15000 → age=1000ms → not stale
        assert session.stale_leg_count(now_ms=16000, stale_after_ms=5000) == 1

    def test_refresh_state_ready_when_both_ready(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.refresh_state(now_ms=12000, stale_after_ms=5000)
        assert session.state == EntryLocalL2SessionState.READY

    def test_refresh_state_arming_when_one_arming(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.ensure_leg("bybit", "BTCUSDT")  # still arming
        session.refresh_state(now_ms=12000, stale_after_ms=5000)
        assert session.state == EntryLocalL2SessionState.ARMING

    def test_refresh_state_faulted_when_any_leg_faulted(self):
        """V1: ANY leg FAULTED → session FAULTED (entry_local_l2_sessions.rs:286-291)."""
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.ensure_leg("bybit", "BTCUSDT").mark_faulted(EntryLocalL2LegFault.STALE_BOOK)
        session.refresh_state(now_ms=12000, stale_after_ms=5000)
        assert session.state == EntryLocalL2SessionState.FAULTED

    def test_refresh_state_faulted_when_all_faulted(self):
        """All legs faulted → session FAULTED (same V1 path)."""
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_faulted(EntryLocalL2LegFault.STALE_BOOK)
        session.ensure_leg("bybit", "BTCUSDT").mark_faulted(EntryLocalL2LegFault.CROSSED_OR_LOCKED_BOOK)
        session.refresh_state(now_ms=12000, stale_after_ms=5000)
        assert session.state == EntryLocalL2SessionState.FAULTED

    def test_refresh_state_stays_closed(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.state = EntryLocalL2SessionState.CLOSED
        session.refresh_state(now_ms=12000, stale_after_ms=5000)
        assert session.state == EntryLocalL2SessionState.CLOSED

    def test_diagnostics_snapshot(self):
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=10000)
        snap = session.diagnostics_snapshot(now_ms=12000, stale_after_ms=5000)
        assert snap["pair_id"] == "p1"
        assert snap["dual_ready"] is True
        assert snap["ready_leg_count"] == 2
        assert snap["leg_count"] == 2


# ---------------------------------------------------------------------------
# EntryLocalL2SessionRuntime
# ---------------------------------------------------------------------------


class TestSessionRuntime:
    def test_get_or_create(self):
        rt = EntryLocalL2SessionRuntime()
        s1 = rt.get_or_create_session("p1")
        s2 = rt.get_or_create_session("p1")
        assert s1 is s2

    def test_track_opportunity_creates_legs(self):
        rt = EntryLocalL2SessionRuntime()
        opp = TrackedOpportunity(
            pair_id="p1", symbol="BTCUSDT",
            long_venue="binance", short_venue="bybit",
            ranking_edge_bps=15.0, class_=TrackedOpportunityClass.PRIMARY,
        )
        session = rt.track_opportunity(opp, now_ms=10000)
        assert len(session.legs) == 2
        assert "binance" in session.legs
        assert "bybit" in session.legs

    def test_track_primary_sets_assigned_at(self):
        rt = EntryLocalL2SessionRuntime()
        opp = TrackedOpportunity(
            pair_id="p1", symbol="BTCUSDT",
            long_venue="binance", short_venue="bybit",
            ranking_edge_bps=15.0, class_=TrackedOpportunityClass.PRIMARY,
        )
        session = rt.track_opportunity(opp, now_ms=10000)
        assert session.primary_assigned_at_ms == 10000

        rt.track_opportunity(opp, now_ms=20000)
        assert session.primary_assigned_at_ms == 10000

    def test_track_opportunity_does_not_timestamp_or_mark_legs_ready(self):
        rt = EntryLocalL2SessionRuntime()
        opp = TrackedOpportunity(
            pair_id="p1", symbol="BTCUSDT",
            long_venue="binance", short_venue="bybit",
            ranking_edge_bps=15.0, class_=TrackedOpportunityClass.PRIMARY,
        )

        session = rt.track_opportunity(opp, now_ms=10000)

        assert session.state == EntryLocalL2SessionState.ARMING
        assert session.legs["binance"].state == EntryLocalL2LegState.ARMING
        assert session.legs["bybit"].state == EntryLocalL2LegState.ARMING
        assert session.legs["binance"].last_seen_at_ms == 0
        assert session.legs["bybit"].last_seen_at_ms == 0

    def test_close_session(self):
        rt = EntryLocalL2SessionRuntime()
        rt.get_or_create_session("p1")
        rt.close_session("p1")
        assert rt.sessions["p1"].state == EntryLocalL2SessionState.CLOSED

    def test_remove_session(self):
        rt = EntryLocalL2SessionRuntime()
        rt.get_or_create_session("p1")
        rt.remove_session("p1")
        assert "p1" not in rt.sessions


# ---------------------------------------------------------------------------
# Promotion / demotion logic
# ---------------------------------------------------------------------------


class TestHoldWindow:
    def test_allows_when_min_hold_zero(self):
        assert primary_hold_window_allows_replacement(
            primary_assigned_at_ms=10000, now_ms=11000, primary_min_hold_ms=0,
        )

    def test_blocks_when_never_assigned(self):
        """V1: primary never assigned (0 sentinel) → no primary to replace."""
        assert not primary_hold_window_allows_replacement(
            primary_assigned_at_ms=0, now_ms=11000, primary_min_hold_ms=90000,
        )

    def test_allows_after_hold_expires(self):
        assert primary_hold_window_allows_replacement(
            primary_assigned_at_ms=10000, now_ms=100000, primary_min_hold_ms=90000,
        )

    def test_blocks_within_hold_window(self):
        assert not primary_hold_window_allows_replacement(
            primary_assigned_at_ms=10000, now_ms=50000, primary_min_hold_ms=90000,
        )


class TestShadowPromotion:
    def test_eligible_when_shadow_outranks(self):
        primary = TrackedOpportunity(
            pair_id="p1", symbol="BTCUSDT",
            long_venue="binance", short_venue="bybit",
            ranking_edge_bps=10.0, class_=TrackedOpportunityClass.PRIMARY,
        )
        shadow = TrackedOpportunity(
            pair_id="p2", symbol="ETHUSDT",
            long_venue="binance", short_venue="okx",
            ranking_edge_bps=15.0, class_=TrackedOpportunityClass.SHADOW,
        )
        assert shadow_promotion_is_eligible(
            primary, shadow,
            primary_assigned_at_ms=10000, now_ms=100000,
            primary_min_hold_ms=0,
            shadow_promotion_score_delta_bps=3.0,
        )

    def test_not_eligible_when_score_delta_insufficient(self):
        primary = TrackedOpportunity(
            pair_id="p1", symbol="BTCUSDT",
            long_venue="binance", short_venue="bybit",
            ranking_edge_bps=15.0, class_=TrackedOpportunityClass.PRIMARY,
        )
        shadow = TrackedOpportunity(
            pair_id="p2", symbol="ETHUSDT",
            long_venue="binance", short_venue="okx",
            ranking_edge_bps=16.0, class_=TrackedOpportunityClass.SHADOW,
        )
        assert not shadow_promotion_is_eligible(
            primary, shadow,
            primary_assigned_at_ms=10000, now_ms=100000,
            primary_min_hold_ms=0,
            shadow_promotion_score_delta_bps=5.0,
        )

    def test_not_eligible_within_hold_window(self):
        primary = TrackedOpportunity(
            pair_id="p1", symbol="BTCUSDT",
            long_venue="binance", short_venue="bybit",
            ranking_edge_bps=10.0, class_=TrackedOpportunityClass.PRIMARY,
        )
        shadow = TrackedOpportunity(
            pair_id="p2", symbol="ETHUSDT",
            long_venue="binance", short_venue="okx",
            ranking_edge_bps=20.0, class_=TrackedOpportunityClass.SHADOW,
        )
        assert not shadow_promotion_is_eligible(
            primary, shadow,
            primary_assigned_at_ms=10000, now_ms=50000,
            primary_min_hold_ms=90000,
            shadow_promotion_score_delta_bps=3.0,
        )


class TestDeduplicatedLegs:
    def test_dedup(self):
        opportunities = [
            TrackedOpportunity(
                pair_id="p1", symbol="BTCUSDT",
                long_venue="binance", short_venue="bybit",
                ranking_edge_bps=10.0,
            ),
            TrackedOpportunity(
                pair_id="p2", symbol="BTCUSDT",
                long_venue="binance", short_venue="okx",
                ranking_edge_bps=15.0,
            ),
        ]
        legs = deduplicated_tracked_legs(opportunities)
        assert len(legs) == 3  # binance:BTCUSDT, bybit:BTCUSDT, okx:BTCUSDT


class TestSelectTrackedOpportunities:
    def test_select_primary_and_shadow(self):
        class MockCandidate:
            def __init__(self, pair_id, symbol, edge):
                self.pair_id = pair_id
                self.symbol = symbol
                self.long_venue = "binance"
                self.short_venue = "bybit"
                self.ranking_edge_bps = edge

        candidates = [
            MockCandidate("p1", "BTC", 20.0),
            MockCandidate("p2", "ETH", 15.0),
            MockCandidate("p3", "SOL", 10.0),
            MockCandidate("p4", "AVAX", 5.0),
        ]
        result = select_tracked_opportunities(candidates, primary_count=2, shadow_count=1)
        assert len(result) == 3
        assert result[0].class_ == TrackedOpportunityClass.PRIMARY
        assert result[1].class_ == TrackedOpportunityClass.PRIMARY
        assert result[2].class_ == TrackedOpportunityClass.SHADOW

    def test_primary_symbols_are_unique_and_alternate_routes_remain_shadowed(self):
        class MockCandidate:
            def __init__(self, pair_id, symbol, short_venue, edge):
                self.pair_id = pair_id
                self.symbol = symbol
                self.long_venue = "binance"
                self.short_venue = short_venue
                self.ranking_edge_bps = edge

        candidates = [
            MockCandidate("btc-binance-bybit", "BTCUSDT", "bybit", 30.0),
            MockCandidate("btc-binance-okx", "BTCUSDT", "okx", 29.0),
            MockCandidate("eth-binance-bybit", "ETHUSDT", "bybit", 28.0),
            MockCandidate("sol-binance-bybit", "SOLUSDT", "bybit", 27.0),
        ]

        tracked = select_tracked_opportunities(
            candidates,
            primary_count=2,
            shadow_count=2,
        )

        assert [row.pair_id for row in tracked] == [
            "btc-binance-bybit",
            "eth-binance-bybit",
            "btc-binance-okx",
            "sol-binance-bybit",
        ]
        assert [row.class_ for row in tracked] == [
            TrackedOpportunityClass.PRIMARY,
            TrackedOpportunityClass.PRIMARY,
            TrackedOpportunityClass.SHADOW,
            TrackedOpportunityClass.SHADOW,
        ]

    def test_primary_window_scans_beyond_thirty_two_surface_routes(self):
        class MockCandidate:
            def __init__(self, index, symbol):
                self.pair_id = f"pair-{index}"
                self.symbol = symbol
                self.long_venue = "binance"
                self.short_venue = "bybit"
                self.ranking_edge_bps = 100.0 - index

        candidates = [
            MockCandidate(index, "BTCUSDT") for index in range(40)
        ] + [
            MockCandidate(40 + index, f"ALT{index}USDT")
            for index in range(5)
        ]

        tracked = select_tracked_opportunities(
            candidates,
            primary_count=6,
            shadow_count=2,
        )

        primaries = [
            row for row in tracked
            if row.class_ == TrackedOpportunityClass.PRIMARY
        ]
        assert [row.pair_id for row in primaries] == [
            "pair-0",
            "pair-40",
            "pair-41",
            "pair-42",
            "pair-43",
            "pair-44",
        ]

    def test_primary_exclusion_backfills_without_removing_route_from_shadow(self):
        class MockCandidate:
            def __init__(self, pair_id, symbol):
                self.pair_id = pair_id
                self.symbol = symbol
                self.long_venue = "binance"
                self.short_venue = "bybit"
                self.ranking_edge_bps = 10.0

        candidates = [
            MockCandidate("p1", "BTCUSDT"),
            MockCandidate("p2", "ETHUSDT"),
            MockCandidate("p3", "SOLUSDT"),
        ]

        tracked = select_tracked_opportunities(
            candidates,
            primary_count=2,
            shadow_count=1,
            primary_excluded_pair_ids={"p1"},
        )

        assert [row.pair_id for row in tracked] == ["p2", "p3", "p1"]
        assert tracked[-1].class_ == TrackedOpportunityClass.SHADOW


class TestMakeCandidatePairId:
    def test_stable_format(self):
        pid = make_candidate_pair_id("BTCUSDT", "binance", "bybit")
        assert pid == "btcusdt:binance->bybit"

    def test_lowercase_symbol(self):
        pid = make_candidate_pair_id("ETHUSDT", "binance", "bybit")
        assert pid == "ethusdt:binance->bybit"

    def test_deterministic_for_same_inputs(self):
        a = make_candidate_pair_id("SOLUSDT", "okx", "gate")
        b = make_candidate_pair_id("SOLUSDT", "okx", "gate")
        assert a == b
        assert a == "solusdt:okx->gate"


# ---------------------------------------------------------------------------
# select_tracked_opportunities with missing pair_id
# ---------------------------------------------------------------------------


class TestSelectTrackedWithMissingPairId:
    def test_fallbacks_to_make_candidate_pair_id(self):
        """CandidateInput lacking pair_id gets stable id from symbol+venues."""

        class CandidateInput:
            symbol = "BTCUSDT"
            long_venue = "binance"
            short_venue = "bybit"
            ranking_edge_bps = 15.0

        result = select_tracked_opportunities([CandidateInput], primary_count=1, shadow_count=0)
        assert len(result) == 1
        assert result[0].pair_id == "btcusdt:binance->bybit"

    def test_respects_existing_pair_id(self):
        """When candidate already has pair_id, it is preserved."""

        class CandidateInput:
            symbol = "BTCUSDT"
            long_venue = "okx"
            short_venue = "gate"
            pair_id = "custom-pair-42"
            ranking_edge_bps = 15.0

        result = select_tracked_opportunities([CandidateInput], primary_count=1, shadow_count=0)
        assert result[0].pair_id == "custom-pair-42"


# ---------------------------------------------------------------------------
# _entry_local_l2_selection_blocker regression tests
# ---------------------------------------------------------------------------


class TestEntryLocalL2SelectionBlockerRealCandidateInput:
    """RED-LIGHT: blocker MUST work with real CandidateInput loaded from snapshot dict.

    Current V2 CandidateInput lacks first_funding_timestamp_ms, funding_timestamp_ms,
    and pair_id. These tests use ONLY the real CandidateInput class (no fakes).
    Tests marked REDLIGHT must FAIL on current V2 code.
    """

    @pytest.fixture
    def runtime_with_l2(self, tmp_path):
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        from lightfee.engine.runtime import LiveRuntime

        config = AppConfig(
            runtime=RuntimeConfig(mode="live", sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                                  sidecar_snapshot_max_age_ms=600_000),
            strategy=StrategyConfig(local_l2_enabled=True, local_l2_ws_enabled=False,
                                    max_concurrent_positions=2,
                                    entry_window_secs=480,
                                    min_scan_minutes_before_funding=0),
            persistence=PersistenceConfig(event_log_path=str(tmp_path / "events.jsonl"),
                                          snapshot_path=str(tmp_path / "state.json")),
        )
        return LiveRuntime(config)

    def test_pending_entry_canonical_pair_id_is_never_reassigned(
        self,
        runtime_with_l2,
        monkeypatch,
    ):
        from types import SimpleNamespace
        from lightfee.core.domain import Venue

        now_ms = 10_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )
        runtime_with_l2.state.pending_entries["entry-1"] = SimpleNamespace(
            pair_id="BTCUSDT:BINANCE->BYBIT",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
        )

        assert runtime_with_l2._tracked_pair_is_executing(
            "btcusdt:binance->bybit"
        )
        assert runtime_with_l2._tracked_pair_is_executing(
            "BTCUSDT:BINANCE->BYBIT"
        )
        runtime_with_l2.config.strategy.entry_local_l2_primary_count = 1
        runtime_with_l2.config.strategy.shadow_entry_opportunity_count = 0
        candidate = self._make_real_candidate(
            pair_id="btcusdt:binance->bybit",
            first_funding_timestamp_ms=20_000,
        )
        assert runtime_with_l2._record_entry_primary_backfill_failure(
            candidate,
            reason="entry_final_revalidation_failed",
            now_ms=now_ms,
        )

        tracked, _ = runtime_with_l2._select_v1_entry_tracked_scope(
            [candidate]
        )

        assert [opportunity.pair_id for opportunity in tracked] == [
            "btcusdt:binance->bybit"
        ]
        assert tracked[0].class_ == TrackedOpportunityClass.PRIMARY

    def test_runtime_ready_primary_evidence_failure_backfills_lower_route(
        self,
        runtime_with_l2,
        monkeypatch,
    ):
        now_ms = 10_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )
        runtime_with_l2.config.strategy.entry_local_l2_primary_count = 2
        runtime_with_l2.config.strategy.shadow_entry_opportunity_count = 1
        candidates = [
            self._make_real_candidate(
                symbol=f"S{index}USDT",
                pair_id=f"s{index}usdt:binance->bybit",
                ranking_edge_bps=100.0 - index,
            )
            for index in range(5)
        ]

        tracked, _tracked_candidates = (
            runtime_with_l2._select_v1_entry_tracked_scope(candidates)
        )
        for opportunity in tracked:
            runtime_with_l2.entry_l2_sessions.track_opportunity(
                opportunity,
                now_ms,
            )
        failed_pair_id = runtime_with_l2._candidate_pair_id(candidates[0])
        failed_session = runtime_with_l2.entry_l2_sessions.sessions[
            failed_pair_id
        ]
        for leg in failed_session.legs.values():
            leg.mark_ready(now_ms)
        failed_session.refresh_state(
            now_ms,
            runtime_with_l2._entry_local_l2_stale_after_ms(),
        )
        runtime_with_l2._tracked_primary_pair_ids = {
            opportunity.pair_id
            for opportunity in tracked
            if opportunity.class_ == TrackedOpportunityClass.PRIMARY
        }

        assert runtime_with_l2._record_entry_primary_backfill_failure(
            candidates[0],
            reason="entry_open_interest_revalidation_failed",
            now_ms=now_ms,
        )
        reselected, _ = runtime_with_l2._select_v1_entry_tracked_scope(
            candidates
        )
        classes = {
            opportunity.pair_id: opportunity.class_
            for opportunity in reselected
        }

        assert classes["s1usdt:binance->bybit"] == (
            TrackedOpportunityClass.PRIMARY
        )
        assert classes["s2usdt:binance->bybit"] == (
            TrackedOpportunityClass.PRIMARY
        )
        assert classes[failed_pair_id] == TrackedOpportunityClass.SHADOW
        assert not runtime_with_l2._record_entry_primary_backfill_failure(
            candidates[1],
            reason="entry_local_l2_waiting_for_primary_tracking",
            now_ms=now_ms,
        )

    def test_runtime_ws_bbo_primary_failure_backfills_lower_route(
        self,
        runtime_with_l2,
        monkeypatch,
    ):
        """WS-BBO failures must release the active slot without losing discovery."""
        now_ms = 10_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )
        runtime_with_l2.config.strategy.entry_readiness_provider = (
            "ws_bbo_quote_lease"
        )
        runtime_with_l2.config.strategy.entry_local_l2_primary_count = 1
        runtime_with_l2.config.strategy.shadow_entry_opportunity_count = 0
        candidates = [
            self._make_real_candidate(
                symbol=f"WS{index}USDT",
                pair_id=f"ws{index}usdt:binance->bybit",
                ranking_edge_bps=100.0 - index,
            )
            for index in range(3)
        ]

        first, _ = runtime_with_l2._select_v1_entry_tracked_scope(candidates)
        assert first[0].pair_id == "ws0usdt:binance->bybit"
        assert first[0].class_ == TrackedOpportunityClass.PRIMARY
        assert runtime_with_l2._record_entry_primary_backfill_failure(
            candidates[0],
            reason="entry_quote_revalidation_failed",
            now_ms=now_ms,
        )

        reselected, _ = runtime_with_l2._select_v1_entry_tracked_scope(
            candidates
        )
        assert reselected[0].pair_id == "ws1usdt:binance->bybit"
        assert reselected[0].class_ == TrackedOpportunityClass.PRIMARY

    @pytest.mark.asyncio
    async def test_ws_bbo_data_plane_serializes_latest_promoted_scope(
        self,
        runtime_with_l2,
        monkeypatch,
    ):
        """A promotion during activation must receive its own BBO scope."""
        import asyncio

        now_ms = 10_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )
        runtime_with_l2.config.strategy.entry_readiness_provider = (
            "ws_bbo_quote_lease"
        )
        runtime_with_l2.config.strategy.entry_local_l2_primary_count = 1
        runtime_with_l2.config.strategy.shadow_entry_opportunity_count = 0
        first = self._make_real_candidate(
            symbol="FIRSTUSDT",
            pair_id="firstusdt:binance->bybit",
            ranking_edge_bps=10.0,
        )
        promoted = self._make_real_candidate(
            symbol="NEXTUSDT",
            pair_id="nextusdt:binance->bybit",
            ranking_edge_bps=9.0,
        )
        first_started = asyncio.Event()
        allow_first_finish = asyncio.Event()
        activated_scopes: list[list[str]] = []

        async def activate(rows, _now_ms):
            activated_scopes.append([candidate.symbol for candidate in rows])
            if len(activated_scopes) == 1:
                first_started.set()
                await allow_first_finish.wait()

        monkeypatch.setattr(
            runtime_with_l2,
            "_ensure_entry_bbo_active_for_candidates",
            activate,
        )

        runtime_with_l2._schedule_entry_data_plane_preparation([first])
        await first_started.wait()
        runtime_with_l2._schedule_entry_data_plane_preparation([promoted])
        allow_first_finish.set()
        task = runtime_with_l2._entry_data_plane_preparation_task
        assert task is not None
        await task

        assert activated_scopes == [["FIRSTUSDT"], ["NEXTUSDT"]]
        assert runtime_with_l2._tracked_primary_pair_ids == {
            "nextusdt:binance->bybit"
        }

    def test_execution_queue_keeps_unseen_routes_ahead_of_expired_failure(
        self,
        runtime_with_l2,
        monkeypatch,
    ):
        """A large frontier cannot restart at rank one when a TTL expires."""
        now_ms = 10_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )
        candidates = [
            self._make_real_candidate(
                symbol=f"QUEUE{index}USDT",
                pair_id=f"queue{index}usdt:binance->bybit",
                ranking_edge_bps=100.0 - index,
            )
            for index in range(3)
        ]
        assert runtime_with_l2._record_entry_primary_backfill_failure(
            candidates[0],
            reason="entry_quote_revalidation_failed",
            now_ms=now_ms,
        )

        # The transient primary exclusion has elapsed, but this fair-queue
        # round still owes the two lower-ranked routes their first evidence
        # attempt before retrying rank one.
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms + 1_000_000,
        )
        ordered = runtime_with_l2._ordered_entry_execution_queue(candidates)

        assert [candidate.symbol for candidate in ordered] == [
            "QUEUE1USDT",
            "QUEUE2USDT",
            "QUEUE0USDT",
        ]

    def test_lower_finalization_ready_route_owns_primary_and_reaches_final_selection(
        self,
        runtime_with_l2,
        monkeypatch,
    ):
        """Higher-ranked early rows cannot monopolize the scarce L2 window."""
        from collections import Counter
        from lightfee.engine.entry_readiness import LocalL2EntryReadinessProvider

        now_ms = 10_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )
        runtime_with_l2.config.strategy.max_concurrent_positions = 6
        runtime_with_l2.config.strategy.entry_local_l2_primary_count = 6
        runtime_with_l2.config.strategy.shadow_entry_opportunity_count = 2
        # This covers the local-L2 selection branch explicitly.  Production
        # defaults use the composed WS-BBO readiness provider.
        runtime_with_l2.entry_readiness_provider = LocalL2EntryReadinessProvider(
            runtime_with_l2
        )
        monkeypatch.setattr(
            runtime_with_l2,
            "_entry_effective_readiness_provider_uses_local_l2",
            lambda: True,
        )
        monkeypatch.setattr(
            runtime_with_l2,
            "_entry_effective_readiness_provider_uses_ws_bbo",
            lambda: False,
        )
        early_candidates = [
            self._make_real_candidate(
                symbol=f"EARLY{index}USDT",
                pair_id=f"early{index}usdt:binance->bybit",
                ranking_edge_bps=100.0 - index,
                first_funding_timestamp_ms=now_ms + 600_000,
                entry_notional_quote=15.0,
            )
            for index in range(7)
        ]
        ready_candidate = self._make_real_candidate(
            symbol="READYUSDT",
            pair_id="readyusdt:binance->bybit",
            ranking_edge_bps=1.0,
            first_funding_timestamp_ms=now_ms + 300_000,
            entry_notional_quote=15.0,
        )
        candidates = [*early_candidates, ready_candidate]

        tracked, _ = runtime_with_l2._select_v1_entry_tracked_scope(candidates)
        primary_pair_ids = {
            opportunity.pair_id
            for opportunity in tracked
            if opportunity.class_ == TrackedOpportunityClass.PRIMARY
        }

        assert primary_pair_ids == {"readyusdt:binance->bybit"}
        for opportunity in tracked:
            runtime_with_l2.entry_l2_sessions.track_opportunity(
                opportunity,
                now_ms,
            )
        ready_session = runtime_with_l2.entry_l2_sessions.sessions[
            "readyusdt:binance->bybit"
        ]
        for leg in ready_session.legs.values():
            leg.mark_ready(now_ms)
        ready_session.refresh_state(
            now_ms,
            runtime_with_l2._entry_local_l2_stale_after_ms(),
        )
        runtime_with_l2._tracked_primary_pair_ids = primary_pair_ids

        assert runtime_with_l2._entry_local_l2_selection_blocker(
            ready_candidate,
            now_ms,
        ) is None
        selection_blockers: Counter = Counter()
        candidate_blockers: dict[str, str] = {}
        selected = runtime_with_l2._select_entry_candidates(
            candidates,
            now_ms=now_ms,
            remaining_slots=1,
            selection_blocker_counts=selection_blockers,
            candidate_blockers=candidate_blockers,
            emit_events=False,
        )

        assert selected == [ready_candidate]
        assert selection_blockers[
            "entry_waiting_for_finalization_window_too_early"
        ] == len(early_candidates)

    @staticmethod
    def _make_real_candidate(**overrides) -> "CandidateInput":
        """Build a real CandidateInput from the actual dataclass — no fake classes."""
        from lightfee.sidecar.snapshot import CandidateInput
        kwargs = dict(
            long_venue="binance", short_venue="bybit", symbol="BTCUSDT",
            funding_diff_bps=15.0, funding_edge_bps=15.0,
            expected_edge_bps=15.0, worst_case_edge_bps=10.0,
            ranking_edge_bps=15.0,
        )
        kwargs.update(overrides)
        return CandidateInput(**kwargs)

    @staticmethod
    def _with_observed_oi(
        candidate,
        *,
        now_ms: int = 10_000,
        status: str = "observed",
        value_quote: float = 2_000_000.0,
    ):
        def venue_name(value) -> str:
            return value.value if hasattr(value, "value") else str(value or "")

        symbol = str(candidate.symbol).upper()
        revision_id = str(
            getattr(candidate, "candidate_revision_id", "")
            or f"test-revision:{symbol}:{candidate.long_venue}:{candidate.short_venue}:{now_ms}"
        )
        candidate.candidate_revision_id = revision_id

        def leg(venue: str) -> dict:
            source = "test_fixture"
            return {
                "venue": venue,
                "canonical_symbol": symbol,
                "venue_symbol": symbol,
                "status": status,
                "observed_at_ms": now_ms,
                "event_at_ms": 0,
                "received_at_ms": now_ms,
                "sample_id": open_interest_sample_id(
                    venue=venue,
                    canonical_symbol=symbol,
                    venue_symbol=symbol,
                    observed_at_ms=now_ms,
                    source=source,
                    raw_value=value_quote,
                    value_quote=value_quote,
                ),
                "value_quote": value_quote,
                "raw_value": value_quote,
                "raw_unit": "quote",
                "source": source,
                "contract_multiplier": 1.0,
                "conversion_mark_price": None,
            }

        candidate.entry_open_interest_evidence = {
            "candidate_revision_id": revision_id,
            "long": leg(venue_name(candidate.long_venue)),
            "short": leg(venue_name(candidate.short_venue)),
        }
        return candidate

    # ------------------------------------------------------------------
    # REDLIGHT 1: real CandidateInput with all conditions met → currently blocked
    # ------------------------------------------------------------------

    def test_redlight_real_candidate_dual_ready_blocked_by_missing_funding_ts(
        self, runtime_with_l2,
    ):
        """WAS RED-LIGHT, NOW GREEN: real CandidateInput with future
        first_funding_timestamp_ms within prewarm window + primary tracking
        + dual-ready session → blocker returns None.
        """
        rt = runtime_with_l2
        # first_funding_timestamp_ms at 15000, now_ms at 10000 → remaining_ms=5000
        # prewarm_window=480s*1000=480000ms → 5000 < 480000 → within window
        c = self._make_real_candidate(first_funding_timestamp_ms=15000)

        pair_id = make_candidate_pair_id(c.symbol, c.long_venue, c.short_venue)

        # Set up primary tracking
        rt._tracked_primary_pair_ids.add(pair_id)

        # Set up dual-ready session
        session = rt.entry_l2_sessions.get_or_create_session(pair_id)
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.refresh_state(now_ms=10000, stale_after_ms=300_000)

        assert session.both_legs_ready(10000, 300_000), "session should be dual-ready"
        assert pair_id in rt._tracked_primary_pair_ids, "pair should be primary tracked"

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason is None, (
            f"dual-ready + primary-tracked candidate with future funding ts "
            f"should NOT be blocked, but blocker returned: {reason}."
        )

    def test_first_funding_ts_outside_prewarm_window_blocked(self, runtime_with_l2):
        """Candidate with first_funding_timestamp_ms too far in future is blocked."""
        rt = runtime_with_l2
        rt.config.strategy.entry_window_secs = 1200
        # remaining_ms = 1000000 - 10000 = 990000 > prewarm_window(480000)
        c = self._make_real_candidate(first_funding_timestamp_ms=1000000)

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason == "entry_local_l2_waiting_for_prewarm_window"

    def test_zero_prewarm_window_disables_only_the_time_window_gate(self, runtime_with_l2):
        rt = runtime_with_l2
        rt.config.strategy.entry_window_secs = 1200
        rt.config.strategy.entry_local_l2_prewarm_window_secs = 0
        c = self._make_real_candidate(first_funding_timestamp_ms=1_000_000)
        pair_id = make_candidate_pair_id(c.symbol, c.long_venue, c.short_venue)
        rt._tracked_primary_pair_ids.add(pair_id)
        session = rt.entry_l2_sessions.get_or_create_session(pair_id)
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.refresh_state(now_ms=10000, stale_after_ms=300_000)

        assert rt._entry_local_l2_selection_blocker(c, now_ms=10000) is None

    def test_first_funding_ts_in_past_blocked(self, runtime_with_l2):
        """Candidate with first_funding_timestamp_ms in the past is blocked."""
        rt = runtime_with_l2
        # remaining_ms = 5000 - 10000 = -5000 <= 0
        c = self._make_real_candidate(first_funding_timestamp_ms=5000)

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason == "entry_finalization_window_expired"

    def test_entry_window_blocks_candidate_before_finalization_window(self, runtime_with_l2):
        """V1 final selection uses entry_window_secs, not only the L2 prewarm window."""
        rt = runtime_with_l2
        rt.config.strategy.entry_window_secs = 300
        rt.config.strategy.entry_local_l2_prewarm_window_secs = 900
        rt.config.strategy.min_scan_minutes_before_funding = 3
        c = self._make_real_candidate(first_funding_timestamp_ms=610000)

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason == "entry_waiting_for_finalization_window_too_early"

    def test_entry_window_blocks_even_when_local_l2_disabled(self, runtime_with_l2):
        """V1 finalization window is applied before the local-L2 feature check."""
        rt = runtime_with_l2
        rt.config.strategy.local_l2_enabled = False
        rt.config.strategy.entry_window_secs = 300
        rt.config.strategy.min_scan_minutes_before_funding = 3
        c = self._make_real_candidate(first_funding_timestamp_ms=610000)

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)

        assert reason == "entry_waiting_for_finalization_window_too_early"

    def test_min_scan_boundary_expires_finalization_window(self, runtime_with_l2):
        """V1 final selection stops entries inside min_scan_minutes_before_funding."""
        rt = runtime_with_l2
        rt.config.strategy.entry_window_secs = 300
        rt.config.strategy.entry_local_l2_prewarm_window_secs = 900
        rt.config.strategy.min_scan_minutes_before_funding = 3
        c = self._make_real_candidate(first_funding_timestamp_ms=130000)

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason == "entry_finalization_window_expired"

    # ------------------------------------------------------------------
    # REDLIGHT 2: snapshot dict load drops pair_id and funding timestamps
    # ------------------------------------------------------------------

    def test_redlight_snapshot_dict_load_drops_pair_id_and_funding_ts(self):
        """RED-LIGHT: _dict_to_snapshot() with V2 data drops pair_id and
        first_funding_timestamp_ms from CandidateInput.

        V1 snapshot candidates carry pair_id, funding_timestamp_ms, and
        first_funding_timestamp_ms. Current V2 CandidateInput silently lacks
        these fields. This test proves they are missing after snapshot load.
        """
        from lightfee.sidecar.publisher import _dict_to_snapshot

        snapshot_dict = {
            "schema_version": 3,
            "published_at_ms": 10000,
            "market_observed_at_ms": 10000,
            "funding_lifecycle": [],
            "market_lifecycle": [],
            "transfer_lifecycle": [],
            "liquidity_lifecycle": [],
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                "binance:BTCUSDT": {
                    "venue": "binance", "symbol": "BTCUSDT",
                    "bid": 50000, "ask": 50010,
                    "funding_rate_bps": 10.0, "funding_timestamp_ms": 15000,
                },
                "bybit:BTCUSDT": {
                    "venue": "bybit", "symbol": "BTCUSDT",
                    "bid": 50005, "ask": 50015,
                    "funding_rate_bps": -5.0, "funding_timestamp_ms": 15000,
                },
            },
            "candidates": [{
                "long_venue": "binance", "short_venue": "bybit",
                "symbol": "BTCUSDT",
                "funding_diff_bps": 15.0, "funding_edge_bps": 15.0,
                "expected_edge_bps": 15.0, "worst_case_edge_bps": 10.0,
                "ranking_edge_bps": 15.0,
            }],
        }
        snapshot = _dict_to_snapshot(snapshot_dict)
        assert len(snapshot.candidates) == 1
        c = snapshot.candidates[0]

        # RED-LIGHT: CandidateInput should eventually carry pair_id and
        # first_funding_timestamp_ms. Currently it does not.
        pair_id_from_field = getattr(c, "pair_id", None)
        ff_ts = getattr(c, "first_funding_timestamp_ms", None)
        f_ts = getattr(c, "funding_timestamp_ms", None)

        # These assertions MUST fail on current V2 — proving the fields are missing
        assert pair_id_from_field is not None, (
            "RED-LIGHT FAIL: CandidateInput should have pair_id field"
        )
        assert ff_ts is not None, (
            "RED-LIGHT FAIL: CandidateInput should have first_funding_timestamp_ms field"
        )
        assert f_ts is not None, (
            "RED-LIGHT FAIL: CandidateInput should have funding_timestamp_ms field"
        )

    # ------------------------------------------------------------------
    # REDLIGHT 3: V1 snapshot → V2 drops pair_id/funding timestamps
    # ------------------------------------------------------------------

    def test_redlight_v1_snapshot_to_v2_drops_pair_id_and_ts(self):
        """RED-LIGHT: convert_v1_snapshot_to_v2 drops pair_id, funding_timestamp_ms,
        and first_funding_timestamp_ms from V1 candidates.
        """
        from lightfee.sidecar.v1_compat import convert_v1_snapshot_to_v2

        v1_snapshot = {
            "schema_version": 1,
            "published_at_ms": 10000,
            "market_observed_at_ms": 10000,
            "quotes": {
                "binance": {
                    "BTCUSDT": {
                        "best_bid": 50000, "best_ask": 50010,
                        "funding_rate": 10.0, "funding_timestamp_ms": 15000,
                    },
                },
                "bybit": {
                    "BTCUSDT": {
                        "best_bid": 50005, "best_ask": 50015,
                        "funding_rate": -5.0, "funding_timestamp_ms": 15000,
                    },
                },
            },
            "candidates": [{
                "symbol": "BTCUSDT",
                "long_venue": "binance", "short_venue": "bybit",
                "pair_id": "btcusdt:binance->bybit",
                "funding_timestamp_ms": 15000,
                "first_funding_timestamp_ms": 15000,
                "funding_edge_bps": 15.0, "quality_penalty_bps": 0.0,
                "rank": 1,
            }],
        }
        v2_dict = convert_v1_snapshot_to_v2(v1_snapshot)
        candidates = v2_dict.get("candidates", [])
        assert len(candidates) == 1
        c = candidates[0]

        # RED-LIGHT: these must fail on current V2 — V1 compat drops the fields
        assert c.get("pair_id") is not None, (
            "RED-LIGHT FAIL: V1→V2 compat should preserve pair_id"
        )
        assert c.get("first_funding_timestamp_ms") is not None, (
            "RED-LIGHT FAIL: V1→V2 compat should preserve first_funding_timestamp_ms"
        )
        assert c.get("funding_timestamp_ms") is not None, (
            "RED-LIGHT FAIL: V1→V2 compat should preserve funding_timestamp_ms"
        )

    # ------------------------------------------------------------------
    # REDLIGHT 4: journal pair_id is empty for real CandidateInput
    # ------------------------------------------------------------------

    def test_journal_blocked_event_has_stable_pair_id(self, tmp_path):
        """Journal blocked event must write stable pair_id, not empty string."""
        import json
        from lightfee.persistence.journal import Journal

        journal_path = tmp_path / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()

        journal.append(
            "runtime.entry_blocked_local_l2_selection",
            {
                "symbol": "BTCUSDT",
                "pair_id": "btcusdt:binance->bybit",
                "reason": "entry_local_l2_waiting_for_prewarm_window",
                "ts_ms": 10000,
            },
        )
        journal.close()

        events = [
            json.loads(line) for line in
            journal_path.read_text().strip().splitlines() if line.strip()
        ]
        blocked_events = [
            e for e in events
            if e.get("kind") == "runtime.entry_blocked_local_l2_selection"
        ]
        assert len(blocked_events) >= 1, "should have at least one blocked event"
        pair_id = blocked_events[0].get("payload", {}).get("pair_id", "")

        assert pair_id != "", "journal pair_id must not be empty"
        assert ":" in pair_id and "->" in pair_id, (
            f"pair_id must follow canonical format, got: {pair_id!r}"
        )
        assert pair_id == "btcusdt:binance->bybit"

    def test_entry_blocked_local_l2_selection_events_are_compacted(
        self, runtime_with_l2,
        monkeypatch,
    ):
        import json
        from collections import Counter

        rt = runtime_with_l2
        from lightfee.engine.entry_readiness import LocalL2EntryReadinessProvider

        c = self._make_real_candidate(
            pair_id="btcusdt:binance->bybit",
            first_funding_timestamp_ms=400_000,
            entry_notional_quote=50.0,
        )
        monkeypatch.setattr(
            rt,
            "_entry_local_l2_selection_blocker",
            lambda _candidate, _now_ms: "entry_local_l2_waiting_for_dual_ready",
        )
        monkeypatch.setattr(
            rt,
            "_entry_ws_bbo_subscription_blocker",
            lambda _candidate: (None, {}),
        )
        rt.entry_readiness_provider = LocalL2EntryReadinessProvider(rt)

        rt.journal.open()
        try:
            for now_ms in (1_000, 2_000, 61_000):
                selected = rt._select_entry_candidates(
                    [c],
                    now_ms=now_ms,
                    remaining_slots=1,
                    selection_blocker_counts=Counter(),
                    candidate_blockers={},
                )
                assert selected == []
        finally:
            rt.journal.close()

        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        payloads = [
            r["payload"] for r in records
            if r["kind"] == "runtime.entry_blocked_local_l2_selection"
        ]
        assert len(payloads) == 2
        assert payloads[0]["pair_id"] == "btcusdt:binance->bybit"
        assert "compact" not in payloads[0]
        assert payloads[1]["compact"] is True
        assert payloads[1]["suppressed_count"] == 1

    @pytest.mark.asyncio
    async def test_candidate_symbol_skipped_events_are_compacted(
        self, runtime_with_l2,
        monkeypatch,
    ):
        import json
        from lightfee.core.domain import Venue

        rt = runtime_with_l2
        c = self._make_real_candidate(
            symbol="SKIPCOMPACTUSDT",
            pair_id="skipcompactusdt:binance->bybit",
        )
        monkeypatch.setattr(rt, "get_venue_adapter", lambda _venue: object())

        async def fake_supported_symbols(
            venue,
            _adapter,
            symbols,
            *,
            skip_event_kind="",
            fail_closed_on_catalog_unavailable=False,
        ):
            if venue == Venue.BYBIT:
                return []
            return list(symbols)

        monkeypatch.setattr(
            rt,
            "_filter_symbols_supported_by_venue",
            fake_supported_symbols,
        )
        times = iter([1_000, 2_000, 61_000])
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: next(times),
        )
        monkeypatch.setattr(
            "lightfee.engine.entry_gate_runtime.wall_clock_now_ms",
            lambda: next(times),
        )

        rt.journal.open()
        try:
            for _ in range(3):
                assert await rt._filter_candidates_supported_by_venue_catalog([c]) == []
        finally:
            rt.journal.close()

        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        payloads = [
            r["payload"] for r in records
            if r["kind"] == "runtime.candidate_symbol_skipped"
        ]
        assert len(payloads) == 2
        assert payloads[0]["pair_id"] == "skipcompactusdt:binance->bybit"
        assert "compact" not in payloads[0]
        assert payloads[1]["compact"] is True
        assert payloads[1]["suppressed_count"] == 1

    # ------------------------------------------------------------------
    # Non-red-light: tests that should already pass (smoke)
    # ------------------------------------------------------------------

    def test_no_session_must_block_with_real_candidate(self, runtime_with_l2):
        """Real CandidateInput with no session → blocked."""
        rt = runtime_with_l2
        c = self._make_real_candidate()
        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason is not None, "entry should be blocked without session"
        assert "prewarm" in reason or "dual_ready" in reason or "primary_tracking" in reason

    def test_prewarm_blocked_when_no_funding_ts(self, runtime_with_l2):
        """Real CandidateInput (no funding_timestamp_ms) → prewarm blocked."""
        rt = runtime_with_l2
        c = self._make_real_candidate()
        # CandidateInput has no funding_timestamp_ms → getattr returns 0 → prewarm blocked
        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason == "entry_local_l2_waiting_for_prewarm_window"

    def test_local_l2_disabled_allows_entry_even_without_funding_ts(
        self, runtime_with_l2,
    ):
        """When local_l2_enabled=False, blocker returns None."""
        runtime_with_l2.config.strategy.local_l2_enabled = False
        c = self._make_real_candidate()
        reason = runtime_with_l2._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason is None

    def test_v1_final_selection_reranks_before_symbol_uniqueness(
        self, runtime_with_l2,
    ):
        """V1 sorts final candidates before keeping only one per symbol."""
        from collections import Counter
        from types import SimpleNamespace

        from lightfee.engine.entry_readiness import EntryReadinessDecision

        rt = runtime_with_l2
        rt.config.strategy.local_l2_enabled = False
        rt.config.strategy.max_concurrent_positions = 3
        rt._entry_ws_bbo_subscription_blocker = lambda _candidate: (None, {})
        rt.entry_readiness_provider = SimpleNamespace(
            decide=lambda candidate, _now_ms, **_kwargs: EntryReadinessDecision.allow(
                symbol=candidate.symbol,
                pair_id=candidate.pair_id,
            )
        )
        candidates = [
            self._make_real_candidate(
                pair_id="btcusdt:bybit->okx", symbol="BTCUSDT",
                long_venue="bybit", short_venue="okx",
                ranking_edge_bps=9.0, worst_case_edge_bps=2.0,
                first_funding_timestamp_ms=250000,
                entry_notional_quote=100.0,
            ),
            self._make_real_candidate(
                pair_id="ethusdt:binance->okx", symbol="ETHUSDT",
                long_venue="binance", short_venue="okx",
                ranking_edge_bps=10.0, worst_case_edge_bps=2.0,
                first_funding_timestamp_ms=250000,
                entry_notional_quote=100.0,
            ),
            self._make_real_candidate(
                pair_id="btcusdt:binance->okx", symbol="BTCUSDT",
                long_venue="binance", short_venue="okx",
                ranking_edge_bps=12.0, worst_case_edge_bps=2.0,
                first_funding_timestamp_ms=250000,
                entry_notional_quote=100.0,
            ),
        ]

        selected = rt._select_entry_candidates(
            candidates,
            now_ms=10000,
            remaining_slots=3,
            selection_blocker_counts=Counter(),
            candidate_blockers={},
        )

        assert [c.pair_id for c in selected] == [
            "btcusdt:binance->okx",
            "ethusdt:binance->okx",
        ]

    def test_v1_final_selection_uses_runtime_depth_risk_before_symbol_uniqueness(
        self, runtime_with_l2,
    ):
        """V1 risk-adjusts ranking edge with leg depth risk before symbol dedupe."""
        from collections import Counter
        from types import SimpleNamespace

        from lightfee.engine.entry_readiness import EntryReadinessDecision
        from lightfee.sidecar.snapshot import QuoteSnapshot

        rt = runtime_with_l2
        rt.config.strategy.local_l2_enabled = False
        rt._entry_ws_bbo_subscription_blocker = lambda _candidate: (None, {})
        rt.entry_readiness_provider = SimpleNamespace(
            decide=lambda candidate, _now_ms, **_kwargs: EntryReadinessDecision.allow(
                symbol=candidate.symbol,
                pair_id=candidate.pair_id,
            )
        )
        candidates = [
            self._make_real_candidate(
                pair_id="btcusdt:binance->bybit", symbol="BTCUSDT",
                long_venue="binance", short_venue="bybit",
                ranking_edge_bps=12.0,
                first_funding_timestamp_ms=250000,
                entry_notional_quote=1000.0,
            ),
            self._make_real_candidate(
                pair_id="btcusdt:okx->gate", symbol="BTCUSDT",
                long_venue="okx", short_venue="gate",
                ranking_edge_bps=10.0,
                first_funding_timestamp_ms=250000,
                entry_notional_quote=1000.0,
            ),
        ]
        quotes = {
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=99.0, ask=101.0, bid_size=1.0, ask_size=1.0,
            ),
            "bybit:BTCUSDT": QuoteSnapshot(
                venue="bybit", symbol="BTCUSDT",
                bid=99.0, ask=101.0, bid_size=1.0, ask_size=1.0,
            ),
            "okx:BTCUSDT": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=99.0, ask=101.0, bid_size=100.0, ask_size=100.0,
            ),
            "gate:BTCUSDT": QuoteSnapshot(
                venue="gate", symbol="BTCUSDT",
                bid=99.0, ask=101.0, bid_size=100.0, ask_size=100.0,
            ),
        }

        selected = rt._select_entry_candidates(
            candidates,
            now_ms=10000,
            remaining_slots=1,
            selection_blocker_counts=Counter(),
            candidate_blockers={},
            market_quotes=quotes,
        )

        assert [c.pair_id for c in selected] == ["btcusdt:okx->gate"]

    def test_v1_final_selection_skips_pending_residual_pair(
        self, runtime_with_l2,
    ):
        """V1 does not select a candidate whose pair has pending residual repair."""
        from collections import Counter
        from types import SimpleNamespace

        from lightfee.engine.entry_readiness import EntryReadinessDecision

        rt = runtime_with_l2
        rt.config.strategy.local_l2_enabled = False
        rt._entry_ws_bbo_subscription_blocker = lambda _candidate: (None, {})
        rt.entry_readiness_provider = SimpleNamespace(
            decide=lambda candidate, _now_ms, **_kwargs: EntryReadinessDecision.allow(
                symbol=candidate.symbol,
                pair_id=candidate.pair_id,
            )
        )
        rt.state.pending_residual_repairs = [
            {"pair_id": "btcusdt:binance->okx"},
        ]
        candidates = [
            self._make_real_candidate(
                pair_id="btcusdt:binance->okx", symbol="BTCUSDT",
                long_venue="binance", short_venue="okx",
                ranking_edge_bps=12.0,
                first_funding_timestamp_ms=250000,
                entry_notional_quote=100.0,
            ),
            self._make_real_candidate(
                pair_id="ethusdt:binance->okx", symbol="ETHUSDT",
                long_venue="binance", short_venue="okx",
                ranking_edge_bps=10.0,
                first_funding_timestamp_ms=250000,
                entry_notional_quote=100.0,
            ),
        ]

        selected = rt._select_entry_candidates(
            candidates,
            now_ms=10000,
            remaining_slots=2,
            selection_blocker_counts=Counter(),
            candidate_blockers={},
        )

        assert [c.pair_id for c in selected] == ["ethusdt:binance->okx"]

    # ------------------------------------------------------------------
    # RED-LIGHT: V2 snapshot ingress enriches missing candidate fields
    # ------------------------------------------------------------------

    def test_redlight_snapshot_v2_ingress_fills_pair_id_and_funding_ts(self):
        """RED-LIGHT: _dict_to_snapshot() must fill pair_id and
        first_funding_timestamp_ms from symbol/venues/quotes when missing.

        V1: CandidateOpportunity always has pair_id + first_funding_timestamp_ms.
        V2 gap: _dict_to_snapshot() passes CandidateInput(**c) which defaults
        pair_id="" and first_funding_timestamp_ms=0 for schema-2 snapshots
        where the sidecar didn't include these fields. This makes the candidate
        permanently prewarm-blocked in runtime.

        This test MUST FAIL on current V2 code — proving the gap exists.
        """
        from lightfee.sidecar.publisher import _dict_to_snapshot

        snapshot_dict = {
            "schema_version": 3,
            "published_at_ms": 10000,
            "market_observed_at_ms": 10000,
            "funding_lifecycle": [],
            "market_lifecycle": [],
            "transfer_lifecycle": [],
            "liquidity_lifecycle": [],
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                "binance:BTCUSDT": {
                    "venue": "binance", "symbol": "BTCUSDT",
                    "bid": 50000, "ask": 50010,
                    "funding_rate_bps": 10.0, "funding_timestamp_ms": 15000,
                },
                "bybit:BTCUSDT": {
                    "venue": "bybit", "symbol": "BTCUSDT",
                    "bid": 50005, "ask": 50015,
                    "funding_rate_bps": -5.0, "funding_timestamp_ms": 15000,
                },
            },
            "candidates": [{
                "long_venue": "binance", "short_venue": "bybit",
                "symbol": "BTCUSDT",
                "funding_diff_bps": 15.0, "funding_edge_bps": 15.0,
                "expected_edge_bps": 15.0, "worst_case_edge_bps": 10.0,
                "ranking_edge_bps": 15.0,
                # NOTE: no pair_id, no first_funding_timestamp_ms, no funding_timestamp_ms
            }],
        }
        snapshot = _dict_to_snapshot(snapshot_dict)
        assert len(snapshot.candidates) == 1
        c = snapshot.candidates[0]

        # RED-LIGHT: pair_id must be derived from symbol + venues
        assert c.pair_id == "btcusdt:binance->bybit", (
            f"RED-LIGHT FAIL: pair_id should be 'btcusdt:binance->bybit', "
            f"got {c.pair_id!r}. _dict_to_snapshot() must derive it from "
            f"symbol/long_venue/short_venue when absent."
        )

        # RED-LIGHT: first_funding_timestamp_ms must be derived from quotes
        assert c.first_funding_timestamp_ms == 15000, (
            f"RED-LIGHT FAIL: first_funding_timestamp_ms should be 15000 "
            f"(min of long/short quote funding_timestamp_ms), "
            f"got {c.first_funding_timestamp_ms}. "
            f"_dict_to_snapshot() must derive it from quotes when absent."
        )

        # RED-LIGHT: funding_timestamp_ms synced to first_funding_timestamp_ms
        assert c.funding_timestamp_ms == 15000, (
            f"RED-LIGHT FAIL: funding_timestamp_ms should be 15000, "
            f"got {c.funding_timestamp_ms}. Must sync with first_funding_timestamp_ms."
        )

    def test_redlight_snapshot_v2_enriched_candidate_passes_prewarm(
        self, runtime_with_l2,
    ):
        """RED-LIGHT: candidate enriched by _dict_to_snapshot() must pass
        the prewarm gate when primary-tracked and dual-ready.

        Proves end-to-end: snapshot load → runtime blocker → None (not blocked).
        """
        from lightfee.sidecar.publisher import _dict_to_snapshot

        snapshot_dict = {
            "schema_version": 3,
            "published_at_ms": 10000,
            "market_observed_at_ms": 10000,
            "funding_lifecycle": [],
            "market_lifecycle": [],
            "transfer_lifecycle": [],
            "liquidity_lifecycle": [],
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                "binance:BTCUSDT": {
                    "venue": "binance", "symbol": "BTCUSDT",
                    "bid": 50000, "ask": 50010,
                    "funding_rate_bps": 10.0, "funding_timestamp_ms": 15000,
                },
                "bybit:BTCUSDT": {
                    "venue": "bybit", "symbol": "BTCUSDT",
                    "bid": 50005, "ask": 50015,
                    "funding_rate_bps": -5.0, "funding_timestamp_ms": 15000,
                },
            },
            "candidates": [{
                "long_venue": "binance", "short_venue": "bybit",
                "symbol": "BTCUSDT",
                "funding_diff_bps": 15.0, "funding_edge_bps": 15.0,
                "expected_edge_bps": 15.0, "worst_case_edge_bps": 10.0,
                "ranking_edge_bps": 15.0,
            }],
        }
        snapshot = _dict_to_snapshot(snapshot_dict)
        c = snapshot.candidates[0]

        # Ensure enrichment happened (if not, this itself is the red-light)
        assert c.pair_id and c.first_funding_timestamp_ms > 0, (
            "prerequisite: candidate must be enriched by _dict_to_snapshot()"
        )

        rt = runtime_with_l2
        pair_id = c.pair_id
        rt._tracked_primary_pair_ids.add(pair_id)
        session = rt.entry_l2_sessions.get_or_create_session(pair_id)
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.refresh_state(now_ms=10000, stale_after_ms=300_000)

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason is None, (
            f"RED-LIGHT FAIL: enriched candidate with future funding ts + "
            f"primary tracking + dual-ready should NOT be blocked. "
            f"Blocker returned: {reason}"
        )

    def test_redlight_snapshot_v2_missing_quotes_fail_closed(self):
        """RED-LIGHT: when candidate AND quotes lack funding_timestamp_ms,
        the candidate must be blocked, NOT silently become tradeable.

        V1: a candidate without usable funding timestamp is not tradeable.
        V2 gap: candidate loads with first_funding_timestamp_ms=0, which
        looks tradeable in discovery but permanently prewarm-blocks in
        runtime. The fix must either:
          - fail closed: candidate.blocked=True with clear reason, OR
          - raise a snapshot schema error.

        This test MUST FAIL on current V2 — where candidate loads as
        non-blocked with first_funding_timestamp_ms=0.
        """
        from lightfee.sidecar.publisher import _dict_to_snapshot

        snapshot_dict = {
            "schema_version": 2,
            "published_at_ms": 10000,
            "market_observed_at_ms": 10000,
            "funding_lifecycle": [],
            "market_lifecycle": [],
            "transfer_lifecycle": [],
            "liquidity_lifecycle": [],
            "degraded_venues": [],
            "degraded_domains": [],
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                # quotes present but funding_timestamp_ms=0
                "binance:BTCUSDT": {
                    "venue": "binance", "symbol": "BTCUSDT",
                    "bid": 50000, "ask": 50010,
                    "funding_rate_bps": 10.0, "funding_timestamp_ms": 0,
                },
                "bybit:BTCUSDT": {
                    "venue": "bybit", "symbol": "BTCUSDT",
                    "bid": 50005, "ask": 50015,
                    "funding_rate_bps": -5.0, "funding_timestamp_ms": 0,
                },
            },
            "candidates": [{
                "long_venue": "binance", "short_venue": "bybit",
                "symbol": "BTCUSDT",
                "funding_diff_bps": 15.0, "funding_edge_bps": 15.0,
                "expected_edge_bps": 15.0, "worst_case_edge_bps": 10.0,
                "ranking_edge_bps": 15.0,
            }],
        }
        snapshot = _dict_to_snapshot(snapshot_dict)
        assert len(snapshot.candidates) == 1
        c = snapshot.candidates[0]

        # RED-LIGHT: candidate with no usable funding timestamp must be
        # blocked or schema must reject it. It must NOT be silently tradeable
        # with first_funding_timestamp_ms=0.
        is_fail_closed = (
            c.blocked
            and any(
                "missing_candidate_identity_or_funding_timestamp" in r
                for r in c.blocked_reasons
            )
        )
        assert is_fail_closed, (
            f"RED-LIGHT FAIL: candidate with zero first_funding_timestamp_ms "
            f"must be blocked or schema-rejected. Got blocked={c.blocked}, "
            f"blocked_reasons={c.blocked_reasons}, "
            f"first_funding_timestamp_ms={c.first_funding_timestamp_ms}. "
            f"Silently generating tradeable-looking candidates with ts=0 "
            f"is a data-contract violation."
        )

    def test_hot_fresh_books_refresh_session_and_unblock_selection(self, runtime_with_l2):
        """HOT/fresh local-L2 books must drive EntryLocalL2Session legs READY."""
        from lightfee.marketdata.l2 import PriceLevel
        from lightfee.engine.entry_local_l2 import (
            TrackedOpportunity,
            TrackedOpportunityClass,
        )

        rt = runtime_with_l2
        c = self._make_real_candidate(first_funding_timestamp_ms=370000)
        pair_id = make_candidate_pair_id(c.symbol, c.long_venue, c.short_venue)
        rt._tracked_primary_pair_ids.add(pair_id)
        rt.entry_l2_sessions.track_opportunity(
            TrackedOpportunity(
                pair_id=pair_id,
                symbol=c.symbol,
                long_venue=c.long_venue,
                short_venue=c.short_venue,
                ranking_edge_bps=c.ranking_edge_bps,
                class_=TrackedOpportunityClass.PRIMARY,
            ),
            now_ms=10000,
        )

        for venue, bid, ask, seq in (
            ("binance", 50000.0, 50100.0, 10),
            ("bybit", 49990.0, 50110.0, 11),
        ):
            book = rt.local_l2_runtime.ensure_book(venue, c.symbol)
            book.transition_to_bootstrapping(now_ms=9000)
            book.apply_snapshot(
                [PriceLevel(price=bid, quantity=1.0)],
                [PriceLevel(price=ask, quantity=1.0)],
                sequence=seq,
                now_ms=9500,
            )
            book.transition_to_hot()

        rt._refresh_entry_l2_session_readiness(now_ms=10000)

        session = rt.entry_l2_sessions.sessions[pair_id]
        assert session.both_legs_ready(now_ms=10000, stale_after_ms=300_000)
        assert rt._entry_local_l2_selection_blocker(c, now_ms=10000) is None

    @pytest.mark.parametrize(
        "mutate_book,expected_reason,expected_detail",
        [
            (None, "book_missing", "book not found"),
            (
                lambda book: setattr(book, "observed_at_ms", 4000),
                "stale_book",
                "age_ms=6000 stale_after_ms=1000",
            ),
            (
                lambda book: book.transition_to_degraded("transport_failure"),
                "book_degraded",
                "transport_failure",
            ),
            (
                lambda book: setattr(book.asks[0], "price", book.bids[0].price),
                "crossed_or_locked_book",
                "best_bid=50000.0 best_ask=50000.0",
            ),
        ],
    )
    def test_bad_book_refresh_still_blocks_selection_with_stable_leg_reason(
        self, runtime_with_l2, mutate_book, expected_reason, expected_detail,
    ):
        from lightfee.marketdata.l2 import PriceLevel
        from lightfee.engine.entry_local_l2 import (
            TrackedOpportunity,
            TrackedOpportunityClass,
        )

        rt = runtime_with_l2
        rt.config.strategy.local_l2_max_age_ms = 1000
        c = self._make_real_candidate(first_funding_timestamp_ms=370000)
        pair_id = make_candidate_pair_id(c.symbol, c.long_venue, c.short_venue)
        rt._tracked_primary_pair_ids.add(pair_id)
        rt.entry_l2_sessions.track_opportunity(
            TrackedOpportunity(
                pair_id=pair_id,
                symbol=c.symbol,
                long_venue=c.long_venue,
                short_venue=c.short_venue,
                ranking_edge_bps=c.ranking_edge_bps,
                class_=TrackedOpportunityClass.PRIMARY,
            ),
            now_ms=10000,
        )

        healthy = rt.local_l2_runtime.ensure_book("bybit", c.symbol)
        healthy.transition_to_bootstrapping(now_ms=9000)
        healthy.apply_snapshot(
            [PriceLevel(price=49990.0, quantity=1.0)],
            [PriceLevel(price=50110.0, quantity=1.0)],
            sequence=11,
            now_ms=9500,
        )
        healthy.transition_to_hot()

        if mutate_book is not None:
            bad = rt.local_l2_runtime.ensure_book("binance", c.symbol)
            bad.transition_to_bootstrapping(now_ms=9000)
            bad.apply_snapshot(
                [PriceLevel(price=50000.0, quantity=1.0)],
                [PriceLevel(price=50100.0, quantity=1.0)],
                sequence=10,
                now_ms=9500,
            )
            bad.transition_to_hot()
            mutate_book(bad)

        rt.journal.open()
        rt._refresh_entry_l2_session_readiness(now_ms=10000)
        rt.journal.close()

        assert (
            rt._entry_local_l2_selection_blocker(c, now_ms=10000)
            == "entry_local_l2_waiting_for_dual_ready"
        )
        diag = rt._entry_l2_last_leg_diagnostics[(pair_id, "binance")]
        assert diag["reason"] == expected_reason
        assert diag["detail"] == expected_detail

    def test_default_entry_l2_stale_window_keeps_quiet_hot_book_ready(self, runtime_with_l2):
        from lightfee.marketdata.l2 import PriceLevel
        from lightfee.engine.entry_local_l2 import (
            TrackedOpportunity,
            TrackedOpportunityClass,
        )

        rt = runtime_with_l2
        c = self._make_real_candidate(first_funding_timestamp_ms=370000)
        pair_id = make_candidate_pair_id(c.symbol, c.long_venue, c.short_venue)
        rt._tracked_primary_pair_ids.add(pair_id)
        rt.entry_l2_sessions.track_opportunity(
            TrackedOpportunity(
                pair_id=pair_id,
                symbol=c.symbol,
                long_venue=c.long_venue,
                short_venue=c.short_venue,
                ranking_edge_bps=c.ranking_edge_bps,
                class_=TrackedOpportunityClass.PRIMARY,
            ),
            now_ms=10000,
        )

        for venue in ("binance", "bybit"):
            book = rt.local_l2_runtime.ensure_book(venue, c.symbol)
            book.transition_to_bootstrapping(now_ms=10000)
            book.apply_snapshot(
                [PriceLevel(price=50000.0, quantity=1.0)],
                [PriceLevel(price=50001.0, quantity=1.0)],
                sequence=10,
                now_ms=10000,
            )
            book.transition_to_hot()

        rt._refresh_entry_l2_session_readiness(now_ms=12000)
        assert rt._entry_local_l2_selection_blocker(c, now_ms=12000) is None

        rt.config.strategy.local_l2_max_age_ms = 1000
        rt._refresh_entry_l2_session_readiness(now_ms=12000)
        assert (
            rt._entry_local_l2_selection_blocker(c, now_ms=12000)
            == "entry_local_l2_waiting_for_dual_ready"
        )

    @pytest.mark.asyncio
    async def test_scan_no_entry_diagnostics_has_local_l2_reason_counts_and_samples(
        self, tmp_path, monkeypatch,
    ):
        import json
        from lightfee.config.schema import (
            AppConfig,
            PersistenceConfig,
            RuntimeConfig,
            StrategyConfig,
        )
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

        now_ms = 10000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms,
        )

        snapshot_path = tmp_path / "sidecar.json"
        event_path = tmp_path / "events.jsonl"
        snapshot_path.write_text(json.dumps({
            "schema_version": 5,
            "published_at_ms": now_ms,
            "market_observed_at_ms": now_ms,
            **_v3_candidate_build_proof(
                now_ms,
                input_quote_count=2,
                output_candidate_count=1,
            ),
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                "binance:BTCUSDT": _complete_v3_contract_quote(
                    "binance",
                    "BTCUSDT",
                    bid=50000,
                    ask=50010,
                    observed_at_ms=now_ms,
                    volume_24h_quote=10_000_000.0,
                    open_interest=2_000_000.0,
                    funding_rate_bps=10.0,
                    funding_timestamp_ms=370000,
                    quantity_precision=4,
                    quantity_step_base=0.0001,
                    min_quantity_base=0.0001,
                ),
                "bybit:BTCUSDT": _complete_v3_contract_quote(
                    "bybit",
                    "BTCUSDT",
                    bid=50005,
                    ask=50015,
                    observed_at_ms=now_ms,
                    volume_24h_quote=10_000_000.0,
                    open_interest=2_000_000.0,
                    funding_rate_bps=-5.0,
                    funding_timestamp_ms=370000,
                    quantity_precision=4,
                    quantity_step_base=0.0001,
                    min_quantity_base=0.0001,
                ),
            },
            "candidates": [{
                "long_venue": "binance",
                "short_venue": "bybit",
                "symbol": "BTCUSDT",
                "funding_diff_bps": 15.0,
                **_complete_candidate_funding_timestamps(370_000),
                **_complete_v3_economics_fields(15.0, now_ms),
            }],
        }))

        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                sidecar_snapshot_path=str(snapshot_path),
                sidecar_snapshot_max_age_ms=600_000,
                live_scan_recovery_success_count=1,
            ),
            strategy=StrategyConfig(
                local_l2_enabled=True,
                local_l2_ws_enabled=False,
                funding_new_entries_enabled=True,
                max_concurrent_positions=2,
                entry_window_secs=480,
                local_l2_max_age_ms=1000,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(event_path),
                snapshot_path=str(tmp_path / "state.json"),
            ),
        )
        rt = LiveRuntime(config)
        from lightfee.engine.entry_readiness import LocalL2EntryReadinessProvider

        rt.entry_readiness_provider = LocalL2EntryReadinessProvider(rt)
        monkeypatch.setattr(
            rt,
            "_entry_effective_readiness_provider_uses_local_l2",
            lambda: True,
        )
        monkeypatch.setattr(
            rt,
            "_entry_effective_readiness_provider_uses_ws_bbo",
            lambda: False,
        )
        async def preserve_catalog(candidates, **_kwargs):
            return list(candidates)
        monkeypatch.setattr(
            rt,
            "_filter_candidates_supported_by_venue_catalog",
            preserve_catalog,
        )

        def preserve_reprice(candidates, **_kwargs):
            for candidate in candidates:
                self._with_observed_oi(candidate, now_ms=now_ms)
            return list(candidates)

        monkeypatch.setattr(
            rt,
            "_reprice_entry_candidates_for_selection",
            preserve_reprice,
        )
        monkeypatch.setattr(
            rt,
            "_filter_candidates_by_snapshot_freshness",
            lambda candidates, **_kwargs: list(candidates),
        )
        from lightfee.marketdata.ws_bbo import TopBookQuote
        for venue, bid, ask in (
            ("binance", 50_000.0, 50_010.0),
            ("bybit", 50_005.0, 50_015.0),
        ):
            rt.ws_bbo_cache.update_quote(TopBookQuote(
                venue=venue,
                symbol="BTCUSDT",
                bid=bid,
                ask=ask,
                bid_size=1.0,
                ask_size=1.0,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                source="test_ws_bbo",
            ))
        rt.state.lifecycle = EngineLifecycle.RUNNING
        rt.state.risk_mode = GlobalRiskMode.RUNNING
        rt.entry_executor = object()
        _install_v7_file_snapshot_fixture(monkeypatch)
        _allow_test_entry_account_truth(monkeypatch, rt)
        rt.journal.open()

        await rt.tick()
        rt.journal.close()

        records = [
            json.loads(line) for line in event_path.read_text().splitlines()
            if line.strip()
        ]
        no_entry = next(
            r["payload"] for r in records
            if r["kind"] == "scan.no_entry_diagnostics"
        )
        expected_fields = {
            "reason",
            "candidate_count",
            "tradeable_count",
            "selected_candidate_count",
            "dispatched_candidate_count",
            "remaining_slots",
            "blocked_reason_counts",
            "tradeable_selection_blocker_counts",
            "entry_local_l2_primary_ready_filter_active",
            "entry_local_l2_primary_not_ready_reason_counts",
            "entry_local_l2_primary_not_ready_reason_totals",
            "entry_local_l2_primary_not_ready_detail_samples",
            "candidates",
        }
        assert expected_fields.issubset(no_entry.keys())
        assert no_entry["reason"] == "tradeable_candidates_waiting_for_entry_local_l2_dual_ready"
        assert no_entry["candidate_count"] == 1
        assert no_entry["tradeable_count"] == 1
        # V1 parity: shortlist/tradeable candidates are not final selected
        # candidates until the immediate selection blocker passes.
        assert no_entry["selected_candidate_count"] == 0
        assert no_entry["dispatched_candidate_count"] == 0
        assert rt.state.last_scan is not None
        assert rt.state.last_scan["selected_candidate_count"] == 0
        assert rt.state.last_scan["dispatched_candidate_count"] == 0
        assert no_entry["remaining_slots"] == 2
        assert no_entry["tradeable_selection_blocker_counts"] == {
            "entry_local_l2_waiting_for_dual_ready": 1
        }
        assert no_entry["entry_local_l2_primary_ready_filter_active"] is True
        assert no_entry["entry_local_l2_primary_not_ready_reason_counts"] == {
            "book_missing": 2
        }
        assert no_entry["entry_local_l2_primary_not_ready_reason_totals"] == {
            "book_missing": 2
        }
        assert len(no_entry["entry_local_l2_primary_not_ready_detail_samples"]) == 2
        assert no_entry["entry_local_l2_primary_not_ready_detail_samples"][0]["reason"] == "book_missing"
        assert no_entry["candidates"][0]["pair_id"] == "btcusdt:binance->bybit"
        assert no_entry["entry_candidate_blocked_counts"] == {}
        assert no_entry["execution_liquidity_blocked_counts"] == {}
        assert no_entry["entry_final_gate_blocked_counts"] == {
            "entry_local_l2_waiting_for_dual_ready": 1
        }
        assert no_entry["candidates"][0]["rank"] == 1
        assert no_entry["candidates"][0]["remaining_ms"] == 360000
        assert no_entry["candidates"][0]["primary_tracked"] is True
        assert no_entry["candidates"][0]["selection_blocker"] == "entry_local_l2_waiting_for_dual_ready"
        assert "blocked_reasons" in no_entry["candidates"][0]

        readiness = next(
            r["payload"] for r in records
            if r["kind"] == "runtime.entry_local_l2_readiness_diagnostics"
        )
        assert readiness["primary_pair_ids"] == ["btcusdt:binance->bybit"]
        assert len(readiness["not_ready"]) == 2
        assert readiness["not_ready"][0]["reason"] == "book_missing"

    @pytest.mark.asyncio
    async def test_scan_no_entry_reason_uses_v1_finalization_window_before_local_l2(
        self, tmp_path, monkeypatch,
    ):
        import json
        from lightfee.config.schema import (
            AppConfig,
            PersistenceConfig,
            RuntimeConfig,
            StrategyConfig,
        )
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

        now_ms = 10000
        funding_ts = now_ms + 900_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms,
        )

        snapshot_path = tmp_path / "sidecar.json"
        event_path = tmp_path / "events.jsonl"
        snapshot_path.write_text(json.dumps({
            "schema_version": 5,
            "published_at_ms": now_ms,
            "market_observed_at_ms": now_ms,
            **_v3_candidate_build_proof(
                now_ms,
                input_quote_count=2,
                output_candidate_count=1,
            ),
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                "binance:BTCUSDT": _complete_v3_contract_quote(
                    "binance",
                    "BTCUSDT",
                    bid=50000,
                    ask=50010,
                    observed_at_ms=now_ms,
                    volume_24h_quote=10_000_000.0,
                    open_interest=2_000_000.0,
                    funding_rate_bps=10.0,
                    funding_timestamp_ms=funding_ts,
                    quantity_precision=4,
                    quantity_step_base=0.0001,
                    min_quantity_base=0.0001,
                ),
                "bybit:BTCUSDT": _complete_v3_contract_quote(
                    "bybit",
                    "BTCUSDT",
                    bid=50005,
                    ask=50015,
                    observed_at_ms=now_ms,
                    volume_24h_quote=10_000_000.0,
                    open_interest=2_000_000.0,
                    funding_rate_bps=-5.0,
                    funding_timestamp_ms=funding_ts,
                    quantity_precision=4,
                    quantity_step_base=0.0001,
                    min_quantity_base=0.0001,
                ),
            },
            "candidates": [{
                "long_venue": "binance",
                "short_venue": "bybit",
                "symbol": "BTCUSDT",
                "funding_diff_bps": 15.0,
                **_complete_candidate_funding_timestamps(funding_ts),
                **_complete_v3_economics_fields(15.0, now_ms),
            }],
        }))

        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                sidecar_snapshot_path=str(snapshot_path),
                sidecar_snapshot_max_age_ms=600_000,
                live_scan_recovery_success_count=1,
            ),
            strategy=StrategyConfig(
                local_l2_enabled=True,
                local_l2_ws_enabled=False,
                funding_new_entries_enabled=True,
                max_concurrent_positions=2,
                entry_window_secs=480,
                local_l2_max_age_ms=1000,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(event_path),
                snapshot_path=str(tmp_path / "state.json"),
            ),
        )
        rt = LiveRuntime(config)
        async def preserve_catalog(candidates, **_kwargs):
            return list(candidates)
        monkeypatch.setattr(
            rt,
            "_filter_candidates_supported_by_venue_catalog",
            preserve_catalog,
        )

        def preserve_reprice(candidates, **_kwargs):
            for candidate in candidates:
                self._with_observed_oi(candidate, now_ms=now_ms)
            return list(candidates)

        monkeypatch.setattr(
            rt,
            "_reprice_entry_candidates_for_selection",
            preserve_reprice,
        )
        monkeypatch.setattr(
            rt,
            "_filter_candidates_by_snapshot_freshness",
            lambda candidates, **_kwargs: list(candidates),
        )
        from lightfee.marketdata.ws_bbo import TopBookQuote
        for venue, bid, ask in (
            ("binance", 50_000.0, 50_010.0),
            ("bybit", 50_005.0, 50_015.0),
        ):
            rt.ws_bbo_cache.update_quote(TopBookQuote(
                venue=venue,
                symbol="BTCUSDT",
                bid=bid,
                ask=ask,
                bid_size=1.0,
                ask_size=1.0,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                source="test_ws_bbo",
            ))
        rt.state.lifecycle = EngineLifecycle.RUNNING
        rt.state.risk_mode = GlobalRiskMode.RUNNING
        rt.entry_executor = object()
        _install_v7_file_snapshot_fixture(monkeypatch)
        _allow_test_entry_account_truth(monkeypatch, rt)
        rt.journal.open()

        await rt.tick()
        rt.journal.close()

        records = [
            json.loads(line) for line in event_path.read_text().splitlines()
            if line.strip()
        ]
        no_entry = next(
            r["payload"] for r in records
            if r["kind"] == "scan.no_entry_diagnostics"
        )
        assert no_entry["tradeable_count"] == 1
        assert no_entry["selected_candidate_count"] == 0
        assert no_entry["reason"] == (
            "tradeable_candidates_waiting_for_entry_finalization_window_too_early"
        )
        assert no_entry["tradeable_selection_blocker_counts"] == {
            "entry_waiting_for_finalization_window_too_early": 1
        }
        assert no_entry["candidates"][0]["pair_id"] == "btcusdt:binance->bybit"
        assert no_entry["entry_candidate_blocked_counts"] == {}
        assert no_entry["execution_liquidity_blocked_counts"] == {}
        assert no_entry["entry_final_gate_blocked_counts"] == {
            "entry_waiting_for_finalization_window_too_early": 1
        }
        assert no_entry["candidates"][0]["rank"] == 1
        assert no_entry["candidates"][0]["remaining_ms"] == 900000
        assert no_entry["candidates"][0]["primary_tracked"] is True
        assert no_entry["candidates"][0]["selection_blocker"] == (
            "entry_waiting_for_finalization_window_too_early"
        )
        assert "blocked_reasons" in no_entry["candidates"][0]
        assert [
            r for r in records
            if r["kind"] == "runtime.entry_blocked_local_l2_selection"
        ] == []

    @pytest.mark.asyncio
    async def test_scan_activates_l2_only_for_v1_primary_and_shadow_scope(
        self, tmp_path, monkeypatch,
    ):
        import json
        from lightfee.config.schema import (
            AppConfig,
            PersistenceConfig,
            RuntimeConfig,
            StrategyConfig,
        )
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

        now_ms = 10000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms,
        )

        snapshot_path = tmp_path / "sidecar.json"
        event_path = tmp_path / "events.jsonl"
        candidates = []
        for i in range(5):
            candidates.append({
                "long_venue": "binance",
                "short_venue": "bybit",
                "symbol": f"S{i}USDT",
                "funding_diff_bps": 20.0 - i,
                **_complete_candidate_funding_timestamps(370_000),
                **_complete_v3_economics_fields(20.0 - i, now_ms),
            })
        snapshot_path.write_text(json.dumps({
            "schema_version": 3,
            "published_at_ms": now_ms,
            "market_observed_at_ms": now_ms,
            **_v3_candidate_build_proof(
                now_ms,
                input_quote_count=10,
                output_candidate_count=5,
                requested_symbols=[f"S{index}USDT" for index in range(5)],
            ),
            "degraded_venues": [],
            "degraded_domains": [],
            "degraded_symbols": {},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                f"{venue}:S{index}USDT": _complete_v3_contract_quote(
                    venue,
                    f"S{index}USDT",
                    bid=50_000.0,
                    ask=50_001.0,
                    open_interest=2_000_000.0,
                    observed_at_ms=now_ms,
                    funding_rate_bps=(10.0 if venue == "binance" else -10.0),
                    funding_timestamp_ms=370_000,
                )
                for index in range(5)
                for venue in ("binance", "bybit")
            },
            "candidates": candidates,
        }))

        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                sidecar_snapshot_path=str(snapshot_path),
                sidecar_snapshot_max_age_ms=600_000,
                live_scan_recovery_success_count=1,
            ),
            strategy=StrategyConfig(
                local_l2_enabled=True,
                local_l2_ws_enabled=False,
                funding_new_entries_enabled=True,
                max_concurrent_positions=2,
                entry_local_l2_primary_count=2,
                shadow_entry_opportunity_count=1,
                entry_window_secs=480,
                min_scan_minutes_before_funding=0,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(event_path),
                snapshot_path=str(tmp_path / "state.json"),
            ),
        )
        rt = LiveRuntime(config)
        async def preserve_catalog(candidates, **_kwargs):
            return list(candidates)
        monkeypatch.setattr(
            rt,
            "_filter_candidates_supported_by_venue_catalog",
            preserve_catalog,
        )
        rt.state.lifecycle = EngineLifecycle.RUNNING
        rt.state.risk_mode = GlobalRiskMode.RUNNING
        rt.entry_executor = object()
        _install_v7_file_snapshot_fixture(monkeypatch)
        _allow_test_entry_account_truth(monkeypatch, rt)
        rt.journal.open()

        activated_symbols = []

        async def record_l2_activation(candidates, now_ms, **kwargs):
            activated_symbols.extend(c.symbol for c in candidates)

        async def no_sync(now_ms, scan_promoted=False):
            return None

        async def no_bbo_activation(candidates, now_ms):
            return None

        monkeypatch.setattr(rt, "_ensure_l2_active_for_candidates", record_l2_activation)
        monkeypatch.setattr(
            rt,
            "_ensure_entry_bbo_active_for_candidates",
            no_bbo_activation,
        )
        monkeypatch.setattr(rt, "_sync_local_l2_data", no_sync)

        from lightfee.sidecar.snapshot import CandidateInput

        runtime_candidates = []
        for candidate_fields in candidates:
            candidate = CandidateInput(**candidate_fields)
            self._with_observed_oi(candidate, now_ms=now_ms)
            runtime_candidates.append(candidate)

        rt._schedule_entry_data_plane_preparation(runtime_candidates)
        if rt._entry_data_plane_preparation_task is not None:
            await rt._entry_data_plane_preparation_task
        rt.journal.close()

        assert activated_symbols == ["S0USDT", "S1USDT", "S2USDT"]

    @pytest.mark.asyncio
    async def test_dynamic_l2_activation_connects_registered_ws_streams(
        self, runtime_with_l2, monkeypatch,
    ):
        from lightfee.core.domain import Venue

        class Adapter:
            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50):
                raise AssertionError("background bootstrap is stubbed in this test")

        rt = runtime_with_l2
        rt.config.strategy.local_l2_ws_enabled = True
        rt.journal.open()
        rt._venue_adapters = {
            Venue.BINANCE: Adapter(),
            Venue.BYBIT: Adapter(),
        }
        candidate = self._with_observed_oi(
            self._make_real_candidate(first_funding_timestamp_ms=20000)
        )
        calls = []

        def start_ws_streams(venue, symbols, adapter=None):
            calls.append(("start", venue, tuple(symbols), adapter is not None))
            return len(symbols)

        async def connect_ws_streams():
            calls.append(("connect",))
            return 2

        def start_background_bootstrap(**kwargs):
            calls.append(("bootstrap", kwargs["venue"], tuple(kwargs["symbols"])))

        monkeypatch.setattr(rt.l2_data_plane, "start_ws_streams", start_ws_streams)
        monkeypatch.setattr(rt.l2_data_plane, "connect_ws_streams", connect_ws_streams)
        monkeypatch.setattr(
            rt.l2_data_plane, "start_background_bootstrap", start_background_bootstrap,
        )

        try:
            await rt._ensure_l2_active_for_candidates([candidate], now_ms=10000)
        finally:
            rt.journal.close()

        assert ("start", "binance", ("BTCUSDT",), True) in calls
        assert ("start", "bybit", ("BTCUSDT",), True) in calls
        assert ("connect",) in calls

        from lightfee.marketdata.l2 import L2PoolAssignment

        assert (
            rt.local_l2_runtime.get_assignment("binance", "BTCUSDT")
            == L2PoolAssignment.HOT_EXEC
        )
        assert (
            rt.local_l2_runtime.get_assignment("bybit", "BTCUSDT")
            == L2PoolAssignment.HOT_EXEC
        )

    @pytest.mark.asyncio
    async def test_dynamic_l2_activation_uses_three_oi_valid_finalists_plus_pending(
        self, runtime_with_l2, monkeypatch,
    ):
        from types import SimpleNamespace

        from lightfee.core.domain import Venue
        from lightfee.marketdata.local_l2_runtime import LocalL2BookKey

        class Adapter:
            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50):
                raise AssertionError("background bootstrap is stubbed in this test")

        rt = runtime_with_l2
        rt.config.strategy.local_l2_hot_exec_per_venue_budget = 8
        rt.journal.open()
        rt._venue_adapters = {
            Venue.BINANCE: Adapter(),
            Venue.BYBIT: Adapter(),
        }
        rt.state.pending_entries["pending-entry"] = SimpleNamespace(
            symbol="PENDUSDT",
            long_venue="binance",
            short_venue="bybit",
        )

        async def preserve_catalog(venue, adapter, symbols, *, skip_event_kind):
            return list(symbols)

        bootstrap_calls: list[tuple[str, tuple[str, ...]]] = []

        def start_background_bootstrap(**kwargs):
            bootstrap_calls.append((kwargs["venue"], tuple(kwargs["symbols"])))

        monkeypatch.setattr(rt, "_filter_symbols_supported_by_venue", preserve_catalog)
        monkeypatch.setattr(
            rt.l2_data_plane,
            "start_background_bootstrap",
            start_background_bootstrap,
        )

        def candidate(symbol: str, status: str):
            item = self._make_real_candidate(
                symbol=symbol,
                first_funding_timestamp_ms=20_000,
            )
            return self._with_observed_oi(item, now_ms=10_000, status=status)

        candidates = [
            candidate("S0USDT", "observed"),
            candidate("S1USDT", "observed"),
            candidate("BADUSDT", "deferred"),
            candidate("S2USDT", "observed"),
            candidate("EXTRAUSDT", "observed"),
        ]

        try:
            await rt._ensure_l2_active_for_candidates(candidates, now_ms=10_000)
        finally:
            rt.journal.close()

        bootstrapped = {
            (venue, symbol)
            for venue, symbols in bootstrap_calls
            for symbol in symbols
        }
        assert bootstrapped == {
            ("binance", "PENDUSDT"),
            ("binance", "S0USDT"),
            ("binance", "S1USDT"),
            ("binance", "S2USDT"),
            ("bybit", "PENDUSDT"),
            ("bybit", "S0USDT"),
            ("bybit", "S1USDT"),
            ("bybit", "S2USDT"),
        }
        assert LocalL2BookKey("binance", "BADUSDT") not in rt.local_l2_runtime.books
        assert LocalL2BookKey("bybit", "BADUSDT") not in rt.local_l2_runtime.books
        assert LocalL2BookKey("binance", "EXTRAUSDT") not in rt.local_l2_runtime.books
        assert LocalL2BookKey("bybit", "EXTRAUSDT") not in rt.local_l2_runtime.books

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "evidence",
        [
            None,
            [],
            {},
            {"long": {"status": "observed"}},
            {"long": {"status": "observed"}, "short": {"status": "observed"}},
            {"long": {"status": "observed"}, "short": {"status": "deferred"}},
        ],
    )
    async def test_dynamic_l2_activation_fail_closes_without_proven_oi_evidence(
        self, runtime_with_l2, monkeypatch, evidence,
    ):
        from lightfee.core.domain import Venue
        from lightfee.marketdata.local_l2_runtime import LocalL2BookKey

        class Adapter:
            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50):
                raise AssertionError("non-OI-qualified candidate must not bootstrap L2")

        rt = runtime_with_l2
        rt.journal.open()
        rt._venue_adapters = {
            Venue.BINANCE: Adapter(),
            Venue.BYBIT: Adapter(),
        }
        bootstrap_calls = []
        monkeypatch.setattr(
            rt.l2_data_plane,
            "start_background_bootstrap",
            lambda **kwargs: bootstrap_calls.append(kwargs),
        )
        candidate = self._make_real_candidate(
            symbol="NOOIUSDT",
            first_funding_timestamp_ms=20_000,
        )
        if evidence is not None:
            candidate.entry_open_interest_evidence = evidence

        try:
            await rt._ensure_l2_active_for_candidates([candidate], now_ms=10_000)
        finally:
            rt.journal.close()

        assert bootstrap_calls == []
        assert LocalL2BookKey("binance", "NOOIUSDT") not in rt.local_l2_runtime.books
        assert LocalL2BookKey("bybit", "NOOIUSDT") not in rt.local_l2_runtime.books

    @pytest.mark.asyncio
    async def test_entry_data_plane_schedule_waits_for_oi_valid_l2_finalists(
        self, runtime_with_l2, monkeypatch,
    ):
        from types import SimpleNamespace

        now_ms = 10_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )
        rt = runtime_with_l2
        rt.config.strategy.max_concurrent_positions = 3
        rt.config.strategy.entry_local_l2_primary_count = 3
        rt.config.strategy.shadow_entry_opportunity_count = 0
        rt.state.pending_entries["pending-entry"] = SimpleNamespace(
            symbol="PENDUSDT",
            long_venue="binance",
            short_venue="bybit",
        )
        calls: list[tuple[list[str], list[str]]] = []
        bbo_calls: list[list[str]] = []

        async def record_l2_activation(candidates, now_ms, *, tracked_opportunities=None):
            calls.append((
                [candidate.symbol for candidate in candidates],
                [
                    opportunity.pair_id
                    for opportunity in list(tracked_opportunities or [])
                ],
            ))

        async def no_sync(now_ms, scan_promoted=False):
            return None

        async def record_bbo_activation(candidates, now_ms):
            bbo_calls.append([candidate.symbol for candidate in candidates])

        monkeypatch.setattr(rt, "_ensure_l2_active_for_candidates", record_l2_activation)
        monkeypatch.setattr(
            rt,
            "_ensure_entry_bbo_active_for_candidates",
            record_bbo_activation,
        )
        monkeypatch.setattr(rt, "_sync_local_l2_data", no_sync)
        monkeypatch.setattr(rt, "_refresh_entry_l2_session_readiness", lambda _now: None)
        monkeypatch.setattr(
            rt,
            "_apply_shadow_promotion_if_eligible",
            lambda _tracked, _now: None,
        )

        def candidate(symbol: str, status: str | None):
            item = self._make_real_candidate(
                symbol=symbol,
                first_funding_timestamp_ms=20_000,
            )
            if status is None:
                return item
            return self._with_observed_oi(item, now_ms=now_ms, status=status)

        candidates = [
            candidate("S0USDT", "observed"),
            candidate("MISSUSDT", None),
            candidate("DEFUSDT", "deferred"),
            candidate("S1USDT", "observed"),
            candidate("S2USDT", "observed"),
            candidate("EXTRAUSDT", "observed"),
        ]

        rt._schedule_entry_data_plane_preparation(candidates)
        task = rt._entry_data_plane_preparation_task
        assert task is not None
        await task

        assert calls == [(
            ["S0USDT", "S1USDT", "S2USDT"],
            [
                "s0usdt:binance->bybit",
                "s1usdt:binance->bybit",
                "s2usdt:binance->bybit",
            ],
        )]
        assert rt._tracked_primary_pair_ids == {
            "s0usdt:binance->bybit",
            "s1usdt:binance->bybit",
            "s2usdt:binance->bybit",
        }
        assert bbo_calls == []

        calls.clear()
        rt._schedule_entry_data_plane_preparation([])
        task = rt._entry_data_plane_preparation_task
        assert task is not None
        await task

        assert calls == [([], [])]
        assert rt._tracked_primary_pair_ids == set()

    @pytest.mark.asyncio
    async def test_entry_data_plane_schedule_skips_status_only_rows_before_l2_limit(
        self, runtime_with_l2, monkeypatch,
    ):
        now_ms = 10_000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )
        rt = runtime_with_l2
        rt.config.strategy.max_concurrent_positions = 3
        rt.config.strategy.entry_local_l2_primary_count = 3
        rt.config.strategy.shadow_entry_opportunity_count = 0
        calls: list[tuple[list[str], list[str]]] = []

        async def record_l2_activation(candidates, now_ms, *, tracked_opportunities=None):
            calls.append((
                [candidate.symbol for candidate in candidates],
                [
                    opportunity.pair_id
                    for opportunity in list(tracked_opportunities or [])
                ],
            ))

        async def no_sync(now_ms, scan_promoted=False):
            return None

        async def no_bbo_activation(candidates, now_ms):
            return None

        monkeypatch.setattr(rt, "_ensure_l2_active_for_candidates", record_l2_activation)
        monkeypatch.setattr(
            rt,
            "_ensure_entry_bbo_active_for_candidates",
            no_bbo_activation,
        )
        monkeypatch.setattr(rt, "_sync_local_l2_data", no_sync)
        monkeypatch.setattr(rt, "_refresh_entry_l2_session_readiness", lambda _now: None)
        monkeypatch.setattr(
            rt,
            "_apply_shadow_promotion_if_eligible",
            lambda _tracked, _now: None,
        )

        def status_only_candidate(symbol: str):
            item = self._make_real_candidate(
                symbol=symbol,
                first_funding_timestamp_ms=20_000,
            )
            item.candidate_revision_id = f"status-only:{symbol}:{now_ms}"
            item.entry_open_interest_evidence = {
                "candidate_revision_id": item.candidate_revision_id,
                "long": {"status": "observed"},
                "short": {"status": "observed"},
            }
            return item

        valid = self._with_observed_oi(
            self._make_real_candidate(
                symbol="VALIDUSDT",
                first_funding_timestamp_ms=20_000,
            ),
            now_ms=now_ms,
        )
        candidates = [
            status_only_candidate("BAD0USDT"),
            status_only_candidate("BAD1USDT"),
            status_only_candidate("BAD2USDT"),
            valid,
        ]

        rt._schedule_entry_data_plane_preparation(candidates)
        task = rt._entry_data_plane_preparation_task
        assert task is not None
        await task

        assert calls == [(["VALIDUSDT"], ["validusdt:binance->bybit"])]
        assert rt._tracked_primary_pair_ids == {"validusdt:binance->bybit"}

    @pytest.mark.asyncio
    async def test_dynamic_l2_activation_registers_ws_for_hot_ws_authoritative_books(
        self, runtime_with_l2, monkeypatch,
    ):
        from lightfee.core.domain import Venue
        from lightfee.marketdata.l2 import PriceLevel

        class Adapter:
            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50):
                raise AssertionError("HOT book must not be re-bootstrapped")

        rt = runtime_with_l2
        rt.config.strategy.local_l2_ws_enabled = True
        rt.journal.open()
        rt._venue_adapters = {
            Venue.BINANCE: Adapter(),
            Venue.BYBIT: Adapter(),
        }

        candidate = self._with_observed_oi(
            self._make_real_candidate(first_funding_timestamp_ms=20000)
        )
        for venue in ("binance", "bybit"):
            book = rt.local_l2_runtime.ensure_book(venue, candidate.symbol)
            book.transition_to_bootstrapping(now_ms=9000)
            book.apply_snapshot(
                [PriceLevel(price=50000.0, quantity=1.0)],
                [PriceLevel(price=50100.0, quantity=1.0)],
                sequence=10,
                now_ms=9500,
            )
            book.transition_to_hot()

        calls = []
        real_start_ws_streams = rt.l2_data_plane.start_ws_streams

        def start_ws_streams(venue, symbols, adapter=None):
            calls.append(("start", venue, tuple(symbols), adapter is not None))
            return real_start_ws_streams(venue, symbols, adapter=adapter)

        async def connect_ws_streams():
            calls.append(("connect",))
            return 1

        def start_background_bootstrap(**kwargs):
            calls.append(("bootstrap", kwargs["venue"], tuple(kwargs["symbols"])))

        monkeypatch.setattr(rt.l2_data_plane, "start_ws_streams", start_ws_streams)
        monkeypatch.setattr(rt.l2_data_plane, "connect_ws_streams", connect_ws_streams)
        monkeypatch.setattr(
            rt.l2_data_plane, "start_background_bootstrap", start_background_bootstrap,
        )

        try:
            await rt._ensure_l2_active_for_candidates([candidate], now_ms=10000)
        finally:
            rt.journal.close()

        assert ("start", "bybit", ("BTCUSDT",), True) in calls
        assert ("start", "binance", ("BTCUSDT",), True) not in calls
        assert ("connect",) in calls
        assert not [call for call in calls if call[0] == "bootstrap"]
        assert rt.l2_data_plane.ws_stream_state("bybit", "BTCUSDT")["registered"] is True

    @pytest.mark.asyncio
    async def test_dynamic_l2_activation_connects_existing_disconnected_ws_streams(
        self, runtime_with_l2, monkeypatch,
    ):
        from lightfee.core.domain import Venue
        from lightfee.marketdata.l2 import PriceLevel

        class Adapter:
            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50):
                raise AssertionError("background bootstrap is stubbed in this test")

        rt = runtime_with_l2
        rt.config.strategy.local_l2_ws_enabled = True
        rt.journal.open()
        rt._venue_adapters = {
            Venue.BINANCE: Adapter(),
            Venue.BYBIT: Adapter(),
        }

        candidate = self._with_observed_oi(
            self._make_real_candidate(first_funding_timestamp_ms=20000)
        )
        binance_book = rt.local_l2_runtime.ensure_book("binance", candidate.symbol)
        binance_book.transition_to_bootstrapping(now_ms=9000)
        binance_book.apply_snapshot(
            [PriceLevel(price=50000.0, quantity=1.0)],
            [PriceLevel(price=50100.0, quantity=1.0)],
            sequence=10,
            now_ms=9500,
        )
        binance_book.transition_to_hot()
        rt.l2_data_plane.start_ws_streams("bybit", [candidate.symbol], adapter=Adapter())

        calls = []
        real_start_ws_streams = rt.l2_data_plane.start_ws_streams

        def start_ws_streams(venue, symbols, adapter=None):
            calls.append(("start", venue, tuple(symbols), adapter is not None))
            return real_start_ws_streams(venue, symbols, adapter=adapter)

        async def connect_ws_streams():
            calls.append(("connect",))
            return 1

        monkeypatch.setattr(rt.l2_data_plane, "start_ws_streams", start_ws_streams)
        monkeypatch.setattr(rt.l2_data_plane, "connect_ws_streams", connect_ws_streams)
        monkeypatch.setattr(
            rt.l2_data_plane, "start_background_bootstrap", lambda **kwargs: None,
        )

        try:
            await rt._ensure_l2_active_for_candidates([candidate], now_ms=10000)
        finally:
            rt.journal.close()

        assert ("start", "bybit", ("BTCUSDT",), True) in calls
        assert ("connect",) in calls

    @pytest.mark.asyncio
    async def test_dynamic_l2_activation_preserves_shadow_warm_assignment(
        self, runtime_with_l2, monkeypatch,
    ):
        from lightfee.core.domain import Venue
        from lightfee.engine.entry_local_l2 import (
            TrackedOpportunity,
            TrackedOpportunityClass,
        )
        from lightfee.marketdata.l2 import L2PoolAssignment

        class Adapter:
            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50):
                raise AssertionError("background bootstrap is stubbed in this test")

        rt = runtime_with_l2
        rt.journal.open()
        rt._venue_adapters = {
            Venue.BINANCE: Adapter(),
            Venue.BYBIT: Adapter(),
        }
        monkeypatch.setattr(
            rt.l2_data_plane,
            "start_background_bootstrap",
            lambda **kwargs: None,
        )

        candidate = self._with_observed_oi(
            self._make_real_candidate(first_funding_timestamp_ms=20000)
        )
        tracked = [
            TrackedOpportunity(
                pair_id="shadow",
                symbol="BTCUSDT",
                long_venue="binance",
                short_venue="bybit",
                ranking_edge_bps=12.0,
                class_=TrackedOpportunityClass.SHADOW,
            )
        ]

        try:
            await rt._ensure_l2_active_for_candidates(
                [candidate],
                now_ms=10000,
                tracked_opportunities=tracked,
            )
        finally:
            rt.journal.close()

        assert (
            rt.local_l2_runtime.get_assignment("binance", "BTCUSDT")
            == L2PoolAssignment.WARM
        )
        assert (
            rt.local_l2_runtime.get_assignment("bybit", "BTCUSDT")
            == L2PoolAssignment.WARM
        )

    @pytest.mark.asyncio
    async def test_snapshot_degraded_and_stale_events_include_root_diagnostics(
        self, tmp_path, monkeypatch,
    ):
        import json
        from lightfee.config.schema import (
            AppConfig,
            PersistenceConfig,
            RuntimeConfig,
            StrategyConfig,
        )
        from lightfee.engine.runtime import LiveRuntime

        now_ms = 10000
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms,
        )

        snapshot_path = tmp_path / "sidecar.json"
        event_path = tmp_path / "events.jsonl"
        base_snapshot = {
            "schema_version": 2,
            "published_at_ms": now_ms,
            "market_observed_at_ms": now_ms - 100,
            "funding_lifecycle": [],
            "market_lifecycle": [
                {
                    "venue": "binance",
                    "observed_at_ms": now_ms - 100,
                    "symbol_count": 1,
                    "coverage_usable": 1,
                    "degraded_reason": "",
                }
            ],
            "transfer_lifecycle": [],
            "liquidity_lifecycle": [
                {
                    "venue": "bybit",
                    "observed_at_ms": now_ms - 500,
                    "symbol_count": 1,
                    "coverage_usable": 0,
                    "degraded_reason": "book_stale",
                }
            ],
            "degraded_venues": ["bybit"],
            "degraded_domains": ["liquidity"],
            "degraded_symbols": {"bybit": ["BTCUSDT"]},
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                "binance:BTCUSDT": {
                    "venue": "binance", "symbol": "BTCUSDT", "bid": 1, "ask": 2,
                    "funding_rate_bps": 1.0,
                    "funding_timestamp_ms": now_ms + 60_000,
                    "funding_interval_ms": 28_800_000,
                },
                "bybit:BTCUSDT": {
                    "venue": "bybit", "symbol": "BTCUSDT", "bid": 1, "ask": 2,
                    "funding_rate_bps": -1.0,
                    "funding_timestamp_ms": now_ms + 60_000,
                    "funding_interval_ms": 28_800_000,
                },
            },
            "candidates": [
                {
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "symbol": "BTCUSDT",
                    "funding_diff_bps": 1.0,
                    "funding_edge_bps": 1.0,
                    "expected_edge_bps": 1.0,
                    "worst_case_edge_bps": 1.0,
                    "ranking_edge_bps": 1.0,
                    **_complete_candidate_funding_timestamps(now_ms + 60_000),
                }
            ],
        }

        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                sidecar_snapshot_path=str(snapshot_path),
                sidecar_snapshot_max_age_ms=5000,
                live_scan_last_good_max_age_ms=5500,
                live_scan_recovery_success_count=1,
            ),
            strategy=StrategyConfig(local_l2_enabled=False),
            persistence=PersistenceConfig(
                event_log_path=str(event_path),
                snapshot_path=str(tmp_path / "state.json"),
            ),
        )
        rt = LiveRuntime(config)
        _install_v7_file_snapshot_fixture(monkeypatch)
        rt.journal.open()

        snapshot_path.write_text(json.dumps(base_snapshot))
        await rt.tick()
        stale_snapshot = dict(base_snapshot)
        stale_snapshot["published_at_ms"] = now_ms - 6000
        stale_snapshot["market_observed_at_ms"] = now_ms - 7000
        stale_snapshot["degraded_venues"] = []
        stale_snapshot["degraded_domains"] = []
        stale_snapshot["degraded_symbols"] = {}
        rt._last_good_snapshot = None
        snapshot_path.write_text(json.dumps(stale_snapshot))
        rt._ensure_sidecar_snapshot_load()
        if rt._sidecar_snapshot_load_task is not None:
            await rt._sidecar_snapshot_load_task
        await rt.tick()
        rt.journal.close()

        records = [json.loads(line) for line in event_path.read_text().splitlines() if line.strip()]
        degraded = next(r["payload"] for r in records if r["kind"] == "runtime.snapshot_degraded")
        stale = next(r["payload"] for r in records if r["kind"] == "runtime.snapshot_stale")

        for payload in (degraded, stale):
            assert payload["snapshot_publish_age_ms"] >= 0
            assert payload["market_observed_age_ms"] >= 0
            assert payload["per_venue_quote_count"]
            assert payload["per_venue_candidate_count"]
            assert payload["stale_degraded_domains"]
            assert payload["source_mode"] == "direct_market"
            assert payload["acquisition_mode"] == "fresh_sidecar"
            assert payload["snapshot_path"] == str(snapshot_path)

        assert degraded["top_degraded_symbols"] == ["BTCUSDT"]
        assert "liquidity" in degraded["stale_degraded_domains"]
        assert "snapshot_publish_stale" in stale["stale_degraded_domains"]


# ===========================================================================
# Primary tracking admission vs local-L2 readiness failure separation
# ===========================================================================


class TestPrimaryTrackingAdmission:
    """V1 parity: not_primary_tracked is an admission bucket, not a readiness
    failure. Only primary-tracked candidates should contribute to dual_ready /
    readiness blocker counts."""

    @staticmethod
    def _make_config(mode="live", local_l2_enabled=True, journal_path=None):
        from pathlib import Path as _Path
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        import tempfile
        td = tempfile.mkdtemp()
        return AppConfig(
            runtime=RuntimeConfig(
                mode=mode,
                poll_interval_ms=100,
                sidecar_snapshot_path=str(_Path(td) / "sidecar.json"),
                sidecar_snapshot_max_age_ms=600_000,
            ),
            strategy=StrategyConfig(
                risk_monitor_enabled=False,
                max_concurrent_positions=2,
                local_l2_enabled=local_l2_enabled,
                local_l2_ws_enabled=False,
            ),
            persistence=PersistenceConfig(
                event_log_path=journal_path or str(_Path(td) / "events.jsonl"),
                snapshot_path=str(_Path(td) / "state.json"),
            ),
            venues=[],
            symbols=["POLYXUSDT", "BANANAUSDT"],
        )

    @staticmethod
    def _make_candidate(symbol, long_venue, short_venue, pair_id,
                        first_funding_timestamp_ms=0):
        from types import SimpleNamespace
        return SimpleNamespace(
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            pair_id=pair_id,
            first_funding_timestamp_ms=first_funding_timestamp_ms,
            ranking_edge_bps=10.0,
            entry_notional_quote=500.0,
        )

    def test_untracked_candidate_gets_primary_tracking_blocker(self, tmp_path):
        """Candidate not in primary set returns admission blocker, not readiness."""
        from lightfee.engine.runtime import LiveRuntime

        journal = tmp_path / "events.jsonl"
        config = self._make_config(mode="live", journal_path=str(journal))
        rt = LiveRuntime(config)
        rt.journal.open()
        rt._tracked_primary_pair_ids = {"polyxusdt:bybit->hyperliquid"}

        now_ms = 1778985600000
        # remaining_ms = 300_000 exactly matches min_before_ms and entry_window_ms
        funding_ts = now_ms + 300_000

        # Untracked candidate
        untracked = self._make_candidate(
            "BANANAUSDT", "bybit", "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=funding_ts,
        )

        blocker = rt._entry_local_l2_selection_blocker(untracked, now_ms=now_ms)
        assert blocker == "entry_local_l2_waiting_for_primary_tracking"

        # Tracked candidate (but no session → dual_ready blocker)
        tracked = self._make_candidate(
            "POLYXUSDT", "bybit", "hyperliquid",
            "polyxusdt:bybit->hyperliquid",
            first_funding_timestamp_ms=funding_ts,
        )

        blocker2 = rt._entry_local_l2_selection_blocker(tracked, now_ms=now_ms)
        assert blocker2 == "entry_local_l2_waiting_for_dual_ready"
        rt.journal.close()

    def test_select_entry_candidates_separates_admission_from_readiness(self, tmp_path):
        """Select candidates counts primary_tracking separately from readiness."""
        from collections import Counter
        from lightfee.engine.entry_readiness import LocalL2EntryReadinessProvider
        from lightfee.engine.runtime import LiveRuntime

        journal = tmp_path / "events.jsonl"
        config = self._make_config(mode="live", journal_path=str(journal))
        rt = LiveRuntime(config)
        rt.entry_readiness_provider = LocalL2EntryReadinessProvider(rt)
        rt._entry_effective_readiness_provider_uses_local_l2 = lambda: True
        rt._entry_effective_readiness_provider_uses_ws_bbo = lambda: False
        rt.journal.open()
        rt._tracked_primary_pair_ids = {"polyxusdt:bybit->hyperliquid"}

        now_ms = 1778985600000
        funding_ts = now_ms + 300_000

        untracked = self._make_candidate(
            "BANANAUSDT", "bybit", "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=funding_ts,
        )
        tracked = self._make_candidate(
            "POLYXUSDT", "bybit", "hyperliquid",
            "polyxusdt:bybit->hyperliquid",
            first_funding_timestamp_ms=funding_ts,
        )

        admission: Counter = Counter()
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        selected = rt._select_entry_candidates(
            [untracked, tracked],
            now_ms=now_ms,
            remaining_slots=2,
            selection_blocker_counts=selection,
            candidate_blockers=blockers,
            admission_blocker_counts=admission,
        )

        assert len(selected) == 0  # neither was ready
        assert admission.get("entry_local_l2_waiting_for_primary_tracking", 0) == 1
        # The tracked candidate is blocked by dual_ready (readiness failure)
        assert selection.get("entry_local_l2_waiting_for_dual_ready", 0) == 1
        rt.journal.close()


class TestEntryReadinessProviderBoundary:
    """Entry selection must depend on an injectable readiness provider boundary."""

    def test_select_entry_candidates_uses_injected_provider(self, tmp_path):
        """A non-local-L2 provider can approve a candidate without primary tracking."""
        from collections import Counter
        from lightfee.engine.entry_readiness import EntryReadinessDecision
        from lightfee.engine.runtime import LiveRuntime

        class AllowProvider:
            def __init__(self):
                self.calls = []

            def decide(
                self,
                candidate,
                now_ms: int,
                *,
                market_quotes=None,
            ) -> EntryReadinessDecision:
                self.calls.append((candidate, now_ms))
                return EntryReadinessDecision.allow(
                    symbol=str(getattr(candidate, "symbol", "")),
                    pair_id=str(getattr(candidate, "pair_id", "")),
                )

        journal = tmp_path / "events.jsonl"
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(journal),
        )
        rt = LiveRuntime(config)
        rt.journal.open()
        provider = AllowProvider()
        rt.entry_readiness_provider = provider
        rt._entry_ws_bbo_subscription_blocker = lambda _candidate: (None, {})

        now_ms = 1778985600000
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )

        admission: Counter = Counter()
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        selected = rt._select_entry_candidates(
            [candidate],
            now_ms=now_ms,
            remaining_slots=1,
            selection_blocker_counts=selection,
            candidate_blockers=blockers,
            admission_blocker_counts=admission,
        )

        assert selected == [candidate]
        assert provider.calls == [(candidate, now_ms)]
        assert admission == Counter()
        assert selection == Counter()
        assert blockers == {}
        rt.journal.close()

    def test_injected_provider_cannot_bypass_finalization_window(self, tmp_path):
        """Strategy timing gates stay outside the replaceable readiness provider."""
        from collections import Counter
        from lightfee.engine.entry_readiness import EntryReadinessDecision
        from lightfee.engine.runtime import LiveRuntime

        class AllowProvider:
            def __init__(self):
                self.calls = []

            def decide(
                self,
                candidate,
                now_ms: int,
                *,
                market_quotes=None,
            ) -> EntryReadinessDecision:
                self.calls.append((candidate, now_ms))
                return EntryReadinessDecision.allow(
                    symbol=str(getattr(candidate, "symbol", "")),
                    pair_id=str(getattr(candidate, "pair_id", "")),
                )

        journal = tmp_path / "events.jsonl"
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(journal),
        )
        rt = LiveRuntime(config)
        rt.journal.open()
        provider = AllowProvider()
        rt.entry_readiness_provider = provider

        now_ms = 1778985600000
        too_early_ms = (
            now_ms + (int(config.strategy.entry_window_secs) * 1000) + 1
        )
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=too_early_ms,
        )
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        selected = rt._select_entry_candidates(
            [candidate],
            now_ms=now_ms,
            remaining_slots=1,
            selection_blocker_counts=selection,
            candidate_blockers=blockers,
        )

        assert selected == []
        assert provider.calls == []
        assert selection == Counter({
            "entry_waiting_for_finalization_window_too_early": 1,
        })
        assert blockers == {
            "bananausdt:bybit->hyperliquid": (
                "entry_waiting_for_finalization_window_too_early"
            ),
        }
        rt.journal.close()

    def test_provider_denial_without_reason_fails_closed(self, tmp_path):
        """Provider mistakes must not silently approve an entry candidate."""
        from collections import Counter
        from lightfee.engine.entry_readiness import EntryReadinessDecision
        from lightfee.engine.runtime import LiveRuntime

        class EmptyReasonDenyProvider:
            def decide(
                self,
                candidate,
                now_ms: int,
                *,
                market_quotes=None,
            ) -> EntryReadinessDecision:
                return EntryReadinessDecision(
                    allowed=False,
                    symbol=str(getattr(candidate, "symbol", "")),
                    pair_id=str(getattr(candidate, "pair_id", "")),
                )

        journal = tmp_path / "events.jsonl"
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(journal),
        )
        rt = LiveRuntime(config)
        rt.journal.open()
        rt.entry_readiness_provider = EmptyReasonDenyProvider()
        rt._entry_ws_bbo_subscription_blocker = lambda _candidate: (None, {})

        now_ms = 1778985600000
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        selected = rt._select_entry_candidates(
            [candidate],
            now_ms=now_ms,
            remaining_slots=1,
            selection_blocker_counts=selection,
            candidate_blockers=blockers,
        )

        assert selected == []
        assert selection == Counter({"entry_readiness_provider_denied": 1})
        assert blockers == {
            "bananausdt:bybit->hyperliquid": "entry_readiness_provider_denied",
        }
        rt.journal.close()


class TestEntryReadinessProviderFactory:
    """Runtime config selects the readiness provider without changing entry flow."""

    def test_default_provider_is_composed_ws_bbo_transport(self, tmp_path):
        from lightfee.engine.entry_readiness import WsBboQuoteLeaseEntryReadinessProvider
        from lightfee.engine.runtime import LiveRuntime

        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        rt = LiveRuntime(config)

        assert config.strategy.entry_readiness_provider == "ws_bbo_l2_on_demand"
        assert rt._entry_effective_readiness_provider_name() == "ws_bbo_l2_on_demand"
        assert isinstance(rt.entry_readiness_provider, WsBboQuoteLeaseEntryReadinessProvider)

    def test_rest_top_book_provider_selects_candidate_from_fresh_quotes(self, tmp_path):
        from collections import Counter
        from lightfee.engine.entry_readiness import WsBboQuoteLeaseEntryReadinessProvider
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote
        from lightfee.sidecar.snapshot import QuoteSnapshot

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "rest_top_book"
        rt = LiveRuntime(config)

        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        quotes = {
            "bybit:BANANAUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="BANANAUSDT",
                bid=99.0,
                ask=100.0,
                observed_at_ms=now_ms - 100,
            ),
            "hyperliquid:BANANAUSDT": QuoteSnapshot(
                venue="hyperliquid",
                symbol="BANANAUSDT",
                bid=101.0,
                ask=102.0,
                observed_at_ms=now_ms - 100,
            ),
        }
        for venue, bid, ask in (("bybit", 99.0, 100.0), ("hyperliquid", 101.0, 102.0)):
            rt.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BANANAUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=now_ms - 100,
                    received_at_ms=now_ms - 100,
                    source=f"{venue}_bbo_ws",
                )
            )

        selected = rt._select_entry_candidates(
            [candidate],
            now_ms=now_ms,
            remaining_slots=1,
            selection_blocker_counts=Counter(),
            candidate_blockers={},
            market_quotes=quotes,
        )

        assert rt._entry_effective_readiness_provider_name() == "ws_bbo_l2_on_demand"
        assert isinstance(rt.entry_readiness_provider, WsBboQuoteLeaseEntryReadinessProvider)
        assert selected == [candidate]

    def test_rest_top_book_provider_blocks_missing_quote(self, tmp_path):
        from collections import Counter
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.sidecar.snapshot import QuoteSnapshot

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "rest_top_book"
        rt = LiveRuntime(config)
        rt.journal.open()
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        quotes = {
            "bybit:BANANAUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="BANANAUSDT",
                bid=99.0,
                ask=100.0,
                observed_at_ms=now_ms - 100,
            ),
        }
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
                market_quotes=quotes,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert selection == Counter({"entry_ws_bbo_quote_lease_waiting_for_subscription": 1})
        assert blockers == {
            "bananausdt:bybit->hyperliquid": "entry_ws_bbo_quote_lease_waiting_for_subscription",
        }

    def test_quote_lease_provider_records_selected_quote_lease(self, tmp_path):
        from collections import Counter
        from lightfee.engine.entry_readiness import WsBboQuoteLeaseEntryReadinessProvider
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote
        from lightfee.sidecar.snapshot import QuoteSnapshot

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        rt = LiveRuntime(config)
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        quotes = {
            "bybit:BANANAUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="BANANAUSDT",
                bid=99.0,
                ask=100.0,
                observed_at_ms=now_ms - 100,
            ),
            "hyperliquid:BANANAUSDT": QuoteSnapshot(
                venue="hyperliquid",
                symbol="BANANAUSDT",
                bid=101.0,
                ask=102.0,
                observed_at_ms=now_ms - 100,
            ),
        }
        for venue, bid, ask in (("bybit", 99.0, 100.0), ("hyperliquid", 101.0, 102.0)):
            rt.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BANANAUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=now_ms - 100,
                    received_at_ms=now_ms - 100,
                    source=f"{venue}_bbo_ws",
                )
            )

        selected = rt._select_entry_candidates(
            [candidate],
            now_ms=now_ms,
            remaining_slots=1,
            selection_blocker_counts=Counter(),
            candidate_blockers={},
            market_quotes=quotes,
        )

        assert isinstance(rt.entry_readiness_provider, WsBboQuoteLeaseEntryReadinessProvider)
        assert selected == [candidate]
        lease = rt.entry_readiness_provider.get_lease("bananausdt:bybit->hyperliquid")
        assert lease is not None
        assert lease.provider == "ws_bbo_quote_lease"
        assert lease.expires_at_ms == now_ms + 1500
        assert lease.long_ask == 100.0
        assert lease.short_bid == 101.0

    def test_ws_top_book_provider_uses_fresh_ws_bbo_without_entry_l2_session(self, tmp_path):
        from collections import Counter
        from lightfee.engine.entry_readiness import WsBboQuoteLeaseEntryReadinessProvider
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.l2 import L2BookStatus, PriceLevel
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_top_book"
        config.strategy.entry_quote_lease_ttl_ms = 1200
        rt = LiveRuntime(config)
        rt.journal.open()
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        for venue, bid, ask in (
            ("bybit", 99.0, 100.0),
            ("hyperliquid", 101.0, 102.0),
        ):
            book = rt.local_l2_runtime.ensure_book(venue, "BANANAUSDT")
            book.status = L2BookStatus.HOT
            book.bids = [PriceLevel(price=bid, quantity=5.0)]
            book.asks = [PriceLevel(price=ask, quantity=5.0)]
            book.observed_at_ms = now_ms - 100
            rt.l2_data_plane.note_ws_delta(
                venue,
                "BANANAUSDT",
                now_ms=now_ms - 100,
                observed_at_ms=now_ms - 100,
            )
            rt.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BANANAUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=now_ms - 100,
                    received_at_ms=now_ms - 100,
                    source=f"{venue}_bbo_ws",
                )
            )

        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=Counter(),
                candidate_blockers={},
            )
        finally:
            rt.journal.close()

        assert isinstance(rt.entry_readiness_provider, WsBboQuoteLeaseEntryReadinessProvider)
        assert selected == [candidate]
        lease = rt.entry_readiness_provider.get_lease("bananausdt:bybit->hyperliquid")
        assert lease is not None
        assert lease.provider == "ws_bbo_quote_lease"
        assert lease.expires_at_ms == now_ms + 1200
        assert lease.long_ask == 100.0
        assert lease.short_bid == 101.0

    def test_ws_top_book_provider_blocks_when_ws_evidence_missing(self, tmp_path):
        from collections import Counter
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.l2 import L2BookStatus, PriceLevel

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_top_book"
        rt = LiveRuntime(config)
        rt.journal.open()
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        for venue, bid, ask in (
            ("bybit", 99.0, 100.0),
            ("hyperliquid", 101.0, 102.0),
        ):
            book = rt.local_l2_runtime.ensure_book(venue, "BANANAUSDT")
            book.status = L2BookStatus.HOT
            book.bids = [PriceLevel(price=bid, quantity=5.0)]
            book.asks = [PriceLevel(price=ask, quantity=5.0)]
            book.observed_at_ms = now_ms - 100
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert selection == Counter({"entry_ws_bbo_quote_lease_waiting_for_subscription": 1})
        assert blockers == {
            "bananausdt:bybit->hyperliquid": "entry_ws_bbo_quote_lease_waiting_for_subscription",
        }

    def test_ws_bbo_quote_lease_provider_uses_independent_cache_without_local_l2_book(
        self,
        tmp_path,
    ):
        from collections import Counter
        from lightfee.engine.entry_readiness import WsBboQuoteLeaseEntryReadinessProvider
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1200
        rt = LiveRuntime(config)
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        for venue, bid, ask in (
            ("bybit", 99.0, 100.0),
            ("hyperliquid", 101.0, 102.0),
        ):
            rt.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BANANAUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=now_ms - 100,
                    received_at_ms=now_ms - 100,
                    source=f"{venue}_bbo_ws",
                )
            )

        selected = rt._select_entry_candidates(
            [candidate],
            now_ms=now_ms,
            remaining_slots=1,
            selection_blocker_counts=Counter(),
            candidate_blockers={},
        )

        assert isinstance(rt.entry_readiness_provider, WsBboQuoteLeaseEntryReadinessProvider)
        assert rt.local_l2_runtime.get_book("bybit", "BANANAUSDT") is None
        assert selected == [candidate]
        lease = rt.entry_readiness_provider.get_lease("bananausdt:bybit->hyperliquid")
        assert lease is not None
        assert lease.provider == "ws_bbo_quote_lease"
        assert lease.long_ask == 100.0
        assert lease.short_bid == 101.0

    def test_ws_bbo_quote_lease_provider_blocks_stale_cache_quote(self, tmp_path):
        from collections import Counter
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.runtime.max_market_age_ms = 3000
        rt = LiveRuntime(config)
        rt.journal.open()
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        for venue, bid, ask, age_ms in (
            ("bybit", 99.0, 100.0, 100),
            ("hyperliquid", 101.0, 102.0, 5000),
        ):
            rt.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BANANAUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=now_ms - age_ms,
                    received_at_ms=now_ms - age_ms,
                    source=f"{venue}_bbo_ws",
                )
            )
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert selection == Counter({"entry_ws_bbo_quote_lease_stale_quote": 1})
        assert blockers == {
            "bananausdt:bybit->hyperliquid": "entry_ws_bbo_quote_lease_stale_quote",
        }
        import json

        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        payload = [
            r["payload"] for r in records
            if r["kind"] == "runtime.entry_blocked_ws_bbo_selection"
        ][-1]
        evidence = payload["readiness_evidence"]
        assert payload["source"] == "ws_bbo_quote_lease"
        assert payload["provider"] == "ws_bbo_quote_lease"
        assert payload["entry_readiness_provider_raw"] == "ws_bbo_quote_lease"
        assert payload["entry_readiness_provider_effective"] == "ws_bbo_l2_on_demand"
        assert payload["entry_readiness_provider_migrated"] is True
        assert payload["domain"] == "ws_bbo_cache"
        assert evidence["blocker_family"] == "stale_quote"
        assert evidence["quote_age_ms"]["long"] == 100
        assert evidence["quote_age_ms"]["short"] == 5000
        assert not any(
            r["kind"] == "runtime.entry_blocked_local_l2_selection"
            and r.get("payload", {}).get("provider") == "ws_bbo_quote_lease"
            for r in records
        )

    def test_ws_bbo_provider_admission_block_uses_admission_event_not_local_l2(
        self,
        tmp_path,
    ):
        from collections import Counter
        import json
        from lightfee.engine.runtime import LiveRuntime

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        rt = LiveRuntime(config)
        rt.state.venue_entry_cooldowns["hyperliquid:*"] = {
            "venue": "hyperliquid",
            "symbol": "*",
            "blocked_symbol": "BANANAUSDT",
            "reason": "insufficient_margin_admission_blocked",
            "source": "pending_hedge",
            "block_scope": "venue",
            "blocked_until_ms": now_ms + 120_000,
            "official_doc_url": (
                "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
            ),
            "evidence_gap": False,
        }
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        selection: Counter = Counter()
        admission: Counter = Counter()
        blockers: dict[str, str] = {}

        rt.journal.open()
        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
                admission_blocker_counts=admission,
            )
            rt._emit_scan_no_entry_diagnostics(
                reason=rt._v1_tradeable_no_entry_reason(selection, admission)
                or "no_entry_dispatched",
                snapshot=type("Snapshot", (), {"candidates": [candidate]})(),
                tradeable=[candidate],
                selected_candidate_count=len(selected),
                dispatched_candidate_count=0,
                remaining_slots=1,
                tradeable_selection_blocker_counts=selection,
                candidate_blockers=blockers,
                now_ms=now_ms,
                admission_blocker_counts=admission,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert selection == Counter()
        assert admission == Counter({"insufficient_margin_admission_blocked": 1})
        assert blockers == {
            "bananausdt:bybit->hyperliquid": "insufficient_margin_admission_blocked",
        }
        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        admission_events = [
            r for r in records
            if r["kind"] == "runtime.entry_blocked_admission_selection"
        ]
        assert admission_events
        payload = admission_events[-1]["payload"]
        assert payload["reason"] == "insufficient_margin_admission_blocked"
        assert payload["provider"] == "ws_bbo_l2_on_demand"
        assert payload["entry_readiness_provider_raw"] == "ws_bbo_quote_lease"
        assert payload["entry_readiness_provider_effective"] == "ws_bbo_l2_on_demand"
        assert payload["entry_readiness_provider_migrated"] is True
        assert payload["source"] == "entry_admission"
        assert payload["domain"] == "entry_admission"
        assert payload["blocker_family"] == "exchange_admission"
        assert not any(
            r["kind"] == "runtime.entry_blocked_local_l2_selection"
            for r in records
        )
        no_entry = [
            r for r in records
            if r["kind"] == "scan.no_entry_diagnostics"
        ][-1]["payload"]
        assert no_entry["reason"] == "tradeable_candidates_blocked_by_entry_admission"
        assert no_entry["entry_admission_blocker_counts"] == {
            "insufficient_margin_admission_blocked": 1,
        }
        assert no_entry["candidate_stage_blocked_counts"]["entry_admission"] == 1

    def test_ws_bbo_quote_lease_refreshes_stale_tracked_quote_from_rest_top_book(
        self,
        tmp_path,
    ):
        from collections import Counter
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1200
        config.runtime.max_market_age_ms = 3000
        rt = LiveRuntime(config)
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "GUNUSDT",
            "aster",
            "binance",
            "gunusdt:aster->binance",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        rt.ws_bbo_data_plane.start_ws_streams("aster", ["GUNUSDT"])
        rt.ws_bbo_data_plane.start_ws_streams("binance", ["GUNUSDT"])
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="aster",
                symbol="GUNUSDT",
                bid=0.00730,
                ask=0.00740,
                observed_at_ms=now_ms - 40_000,
                received_at_ms=now_ms - 40_000,
                source="aster_book_ticker",
            )
        )
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="GUNUSDT",
                bid=0.00750,
                ask=0.00760,
                observed_at_ms=now_ms - 100,
                received_at_ms=now_ms - 100,
                source="binance_book_ticker",
            )
        )

        class FakeRestRefresher:
            def __init__(self):
                self.calls = []

            def refresh_quote(self, venue, symbol, *, now_ms):
                self.calls.append((venue, symbol, now_ms))
                if venue == "aster" and symbol == "GUNUSDT":
                    return TopBookQuote(
                        venue="aster",
                        symbol="GUNUSDT",
                        bid=0.00735,
                        ask=0.00742,
                        observed_at_ms=now_ms - 50,
                        received_at_ms=now_ms - 40,
                        source="aster_rest_top_book",
                    )
                return None

        refresher = FakeRestRefresher()
        rt.ws_bbo_rest_refresher = refresher
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        rt.journal.open()
        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == [candidate]
        assert selection == Counter()
        assert blockers == {}
        assert refresher.calls == [("aster", "GUNUSDT", now_ms)]
        lease = rt.entry_readiness_provider.get_lease("gunusdt:aster->binance")
        assert lease is not None
        assert lease.long_ask == 0.00742
        assert lease.long_observed_at_ms == now_ms - 50

    def test_ws_bbo_quote_lease_refreshes_quote_older_than_lease_ttl(
        self,
        tmp_path,
    ):
        from collections import Counter
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1200
        config.runtime.max_market_age_ms = 30_000
        rt = LiveRuntime(config)
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "GUAUSDT",
            "aster",
            "binance",
            "guausdt:aster->binance",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        rt.ws_bbo_data_plane.start_ws_streams("aster", ["GUAUSDT"])
        rt.ws_bbo_data_plane.start_ws_streams("binance", ["GUAUSDT"])
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="aster",
                symbol="GUAUSDT",
                bid=0.0810,
                ask=0.0820,
                observed_at_ms=now_ms - 2_000,
                received_at_ms=now_ms - 2_000,
                source="aster_book_ticker",
            )
        )
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="GUAUSDT",
                bid=0.0830,
                ask=0.0840,
                observed_at_ms=now_ms - 100,
                received_at_ms=now_ms - 100,
                source="binance_book_ticker",
            )
        )

        class FakeRestRefresher:
            def __init__(self):
                self.calls = []

            def refresh_quote(self, venue, symbol, *, now_ms):
                self.calls.append((venue, symbol, now_ms))
                if venue == "aster" and symbol == "GUAUSDT":
                    return TopBookQuote(
                        venue="aster",
                        symbol="GUAUSDT",
                        bid=0.0815,
                        ask=0.0825,
                        observed_at_ms=now_ms - 50,
                        received_at_ms=now_ms - 40,
                        source="aster_rest_top_book",
                    )
                return None

        refresher = FakeRestRefresher()
        rt.ws_bbo_rest_refresher = refresher
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        rt.journal.open()
        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == [candidate]
        assert selection == Counter()
        assert blockers == {}
        assert refresher.calls == [("aster", "GUAUSDT", now_ms)]
        lease = rt.entry_readiness_provider.get_lease("guausdt:aster->binance")
        assert lease is not None
        assert lease.long_ask == 0.0825
        assert lease.long_observed_at_ms == now_ms - 50

    def test_ws_bbo_quote_lease_keeps_fail_closed_when_rest_top_book_invalid(
        self,
        tmp_path,
    ):
        from collections import Counter
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.runtime.max_market_age_ms = 3000
        rt = LiveRuntime(config)
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "COSUSDT",
            "aster",
            "binance",
            "cosusdt:aster->binance",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        rt.ws_bbo_data_plane.start_ws_streams("aster", ["COSUSDT"])
        rt.ws_bbo_data_plane.start_ws_streams("binance", ["COSUSDT"])
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="aster",
                symbol="COSUSDT",
                bid=0.00110,
                ask=0.00120,
                observed_at_ms=now_ms - 40_000,
                received_at_ms=now_ms - 40_000,
                source="aster_book_ticker",
            )
        )
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="COSUSDT",
                bid=0.00130,
                ask=0.00140,
                observed_at_ms=now_ms - 100,
                received_at_ms=now_ms - 100,
                source="binance_book_ticker",
            )
        )

        class FakeRestRefresher:
            def refresh_quote(self, venue, symbol, *, now_ms):
                return TopBookQuote(
                    venue=venue,
                    symbol=symbol,
                    bid=0.0,
                    ask=0.00115,
                    observed_at_ms=now_ms,
                    received_at_ms=now_ms,
                    source="aster_rest_top_book",
                )

        rt.ws_bbo_rest_refresher = FakeRestRefresher()
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        rt.journal.open()
        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert selection == Counter({"entry_ws_bbo_quote_lease_stale_quote": 1})
        assert blockers == {
            "cosusdt:aster->binance": "entry_ws_bbo_quote_lease_stale_quote",
        }

    def test_ws_bbo_quote_lease_stale_quote_records_rest_refresh_failure_evidence(
        self,
        tmp_path,
    ):
        from collections import Counter
        import json
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1200
        rt = LiveRuntime(config)
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "GUNUSDT",
            "aster",
            "binance",
            "gunusdt:aster->binance",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        rt.ws_bbo_data_plane.start_ws_streams("aster", ["GUNUSDT"])
        rt.ws_bbo_data_plane.start_ws_streams("binance", ["GUNUSDT"])
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="aster",
                symbol="GUNUSDT",
                bid=0.00730,
                ask=0.00740,
                observed_at_ms=now_ms - 20_000,
                received_at_ms=now_ms - 20_000,
                source="aster_book_ticker",
            )
        )
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="GUNUSDT",
                bid=0.00750,
                ask=0.00760,
                observed_at_ms=now_ms - 50,
                received_at_ms=now_ms - 40,
                source="binance_book_ticker",
            )
        )

        class NoQuoteRestRefresher:
            def refresh_quote(self, venue, symbol, *, now_ms):
                return None

        rt.ws_bbo_rest_refresher = NoQuoteRestRefresher()
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        rt.journal.open()
        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert selection == Counter({"entry_ws_bbo_quote_lease_stale_quote": 1})
        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        payload = [
            r["payload"] for r in records
            if r["kind"] == "runtime.entry_blocked_ws_bbo_selection"
        ][-1]
        evidence = payload["readiness_evidence"]
        assert payload["source"] == "ws_bbo_quote_lease"
        assert payload["provider"] == "ws_bbo_quote_lease"
        assert payload["domain"] == "ws_bbo_cache"
        assert evidence["rest_refresh"]["long"]["attempted"] is True
        assert evidence["rest_refresh"]["long"]["outcome"] == "no_quote"

    def test_ws_bbo_quote_lease_missing_tracked_quote_records_rest_refresh_failure_evidence(
        self,
        tmp_path,
    ):
        from collections import Counter
        import json
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1200
        rt = LiveRuntime(config)
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "SIGNUSDT",
            "okx",
            "aster",
            "signusdt:okx->aster",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        rt.ws_bbo_data_plane.start_ws_streams("okx", ["SIGNUSDT"])
        rt.ws_bbo_data_plane.start_ws_streams("aster", ["SIGNUSDT"])
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="SIGNUSDT",
                bid=0.01110,
                ask=0.01120,
                observed_at_ms=now_ms - 50,
                received_at_ms=now_ms - 40,
                source="okx_ticker",
            )
        )

        class NoQuoteRestRefresher:
            def refresh_quote(self, venue, symbol, *, now_ms):
                return None

        rt.ws_bbo_rest_refresher = NoQuoteRestRefresher()
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        rt.journal.open()
        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert selection == Counter({"entry_ws_bbo_quote_lease_missing_quote": 1})
        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        payload = [
            r["payload"] for r in records
            if r["kind"] == "runtime.entry_blocked_ws_bbo_selection"
        ][-1]
        evidence = payload["readiness_evidence"]
        assert payload["source"] == "ws_bbo_quote_lease"
        assert payload["provider"] == "ws_bbo_quote_lease"
        assert payload["domain"] == "ws_bbo_cache"
        assert evidence["rest_refresh"]["short"]["attempted"] is True
        assert evidence["rest_refresh"]["short"]["outcome"] == "no_quote"

    def test_ws_bbo_quote_lease_refreshes_tracked_missing_quote_from_rest_top_book(
        self,
        tmp_path,
    ):
        from collections import Counter
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1200
        config.runtime.max_market_age_ms = 3000
        rt = LiveRuntime(config)
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "ARIAUSDT",
            "aster",
            "binance",
            "ariausdt:aster->binance",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        rt.ws_bbo_data_plane.start_ws_streams("aster", ["ARIAUSDT"])
        rt.ws_bbo_data_plane.start_ws_streams("binance", ["ARIAUSDT"])
        rt.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="ARIAUSDT",
                bid=0.0394,
                ask=0.0396,
                observed_at_ms=now_ms - 100,
                received_at_ms=now_ms - 100,
                source="binance_book_ticker",
            )
        )

        class FakeRestRefresher:
            def refresh_quote(self, venue, symbol, *, now_ms):
                if venue == "aster" and symbol == "ARIAUSDT":
                    return TopBookQuote(
                        venue="aster",
                        symbol="ARIAUSDT",
                        bid=0.0391,
                        ask=0.0393,
                        observed_at_ms=now_ms - 25,
                        received_at_ms=now_ms - 20,
                        source="aster_rest_top_book",
                    )
                return None

        rt.ws_bbo_rest_refresher = FakeRestRefresher()
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        rt.journal.open()
        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == [candidate]
        assert selection == Counter()
        assert blockers == {}
        lease = rt.entry_readiness_provider.get_lease("ariausdt:aster->binance")
        assert lease is not None
        assert lease.long_bid == 0.0391

    def test_ws_bbo_quote_lease_missing_quote_logs_stream_error_evidence(self, tmp_path):
        from collections import Counter
        import json
        from lightfee.engine.runtime import LiveRuntime

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        rt = LiveRuntime(config)
        rt.journal.open()
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        rt.ws_bbo_data_plane.start_ws_streams("bybit", ["BANANAUSDT"])
        client = rt.ws_bbo_data_plane._clients[("bybit", "BANANAUSDT")]
        client._last_error = "ConnectionError: test-stream-down"
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == []
        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        payload = [
            r["payload"] for r in records
            if r["kind"] == "runtime.entry_blocked_ws_bbo_selection"
        ][-1]
        evidence = payload["readiness_evidence"]
        assert payload["source"] == "ws_bbo_quote_lease"
        assert payload["provider"] == "ws_bbo_quote_lease"
        assert payload["domain"] == "ws_bbo_subscription"
        assert evidence["provider"] == "ws_bbo_quote_lease"
        assert evidence["source"] == "ws_bbo_quote_lease"
        assert evidence["domain"] == "ws_bbo_subscription"
        assert evidence["long_stream_state"]["last_error"] == "ConnectionError: test-stream-down"

    def test_ws_bbo_quote_lease_blocks_untracked_candidate_before_provider_missing_quote(
        self,
        tmp_path,
    ):
        from collections import Counter
        import json
        from lightfee.engine.runtime import LiveRuntime

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        rt = LiveRuntime(config)
        rt.journal.open()
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )
        provider_calls = []
        original_decide = rt.entry_readiness_provider.decide

        def capture_decide(*args, **kwargs):
            provider_calls.append(args)
            return original_decide(*args, **kwargs)

        rt.entry_readiness_provider.decide = capture_decide
        selection: Counter = Counter()
        blockers: dict[str, str] = {}

        try:
            selected = rt._select_entry_candidates(
                [candidate],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert provider_calls == []
        assert selection == Counter({
            "entry_ws_bbo_quote_lease_waiting_for_subscription": 1,
        })
        assert blockers == {
            "bananausdt:bybit->hyperliquid": (
                "entry_ws_bbo_quote_lease_waiting_for_subscription"
            ),
        }
        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        payload = [
            r["payload"] for r in records
            if r["kind"] == "runtime.entry_blocked_ws_bbo_selection"
        ][-1]
        assert payload["reason"] == "entry_ws_bbo_quote_lease_waiting_for_subscription"
        assert payload["source"] == "ws_bbo_quote_lease"
        assert payload["provider"] == "ws_bbo_quote_lease"
        assert payload["domain"] == "ws_bbo_subscription"
        evidence = payload["readiness_evidence"]
        assert evidence["provider"] == "ws_bbo_quote_lease"
        assert evidence["source"] == "ws_bbo_quote_lease"
        assert evidence["domain"] == "ws_bbo_subscription"
        assert evidence["blocker_family"] == "subscription"
        assert evidence["missing_long_subscription"] is True
        assert evidence["missing_short_subscription"] is True
        assert evidence["long_stream_state"]["tracked"] is False
        assert evidence["short_stream_state"]["tracked"] is False

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_activation_does_not_create_local_l2_books(
        self,
        tmp_path,
        monkeypatch,
    ):
        from lightfee.engine.runtime import LiveRuntime

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        rt = LiveRuntime(config)
        rt.journal.open()
        candidate = TestPrimaryTrackingAdmission._make_candidate(
            "BANANAUSDT",
            "bybit",
            "hyperliquid",
            "bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
        )

        async def fake_connect_ws_streams():
            return len(rt.ws_bbo_data_plane._clients)

        monkeypatch.setattr(
            rt.ws_bbo_data_plane,
            "connect_ws_streams",
            fake_connect_ws_streams,
        )
        try:
            await rt._ensure_entry_bbo_active_for_candidates([candidate], now_ms)
        finally:
            rt.journal.close()

        assert rt.local_l2_runtime.get_book("bybit", "BANANAUSDT") is None
        assert rt.local_l2_runtime.get_book("hyperliquid", "BANANAUSDT") is None
        assert set(rt.ws_bbo_data_plane._clients) == {
            ("bybit", "BANANAUSDT"),
            ("hyperliquid", "BANANAUSDT"),
        }

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_uses_independent_per_venue_budget(
        self,
        tmp_path,
        monkeypatch,
    ):
        from lightfee.engine.runtime import LiveRuntime

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.local_l2_hot_exec_per_venue_budget = 1
        config.strategy.entry_ws_bbo_per_venue_budget = 2
        rt = LiveRuntime(config)
        rt.journal.open()
        candidates = [
            TestPrimaryTrackingAdmission._make_candidate(
                "BANANAUSDT",
                "bybit",
                "hyperliquid",
                "bananausdt:bybit->hyperliquid",
                first_funding_timestamp_ms=now_ms + 300_000,
            ),
            TestPrimaryTrackingAdmission._make_candidate(
                "MELONUSDT",
                "bybit",
                "hyperliquid",
                "melonusdt:bybit->hyperliquid",
                first_funding_timestamp_ms=now_ms + 300_000,
            ),
        ]

        async def fake_connect_ws_streams():
            return len(rt.ws_bbo_data_plane._clients)

        monkeypatch.setattr(
            rt.ws_bbo_data_plane,
            "connect_ws_streams",
            fake_connect_ws_streams,
        )
        try:
            await rt._ensure_entry_bbo_active_for_candidates(candidates, now_ms)
        finally:
            rt.journal.close()

        assert set(rt.ws_bbo_data_plane._clients) == {
            ("bybit", "BANANAUSDT"),
            ("bybit", "MELONUSDT"),
            ("hyperliquid", "BANANAUSDT"),
            ("hyperliquid", "MELONUSDT"),
        }

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_budget_preserves_candidate_rank_order(
        self,
        tmp_path,
        monkeypatch,
    ):
        from lightfee.engine.runtime import LiveRuntime

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_ws_bbo_per_venue_budget = 1
        rt = LiveRuntime(config)
        rt.journal.open()
        candidates = [
            TestPrimaryTrackingAdmission._make_candidate(
                "MELONUSDT",
                "bybit",
                "hyperliquid",
                "melonusdt:bybit->hyperliquid",
                first_funding_timestamp_ms=now_ms + 300_000,
            ),
            TestPrimaryTrackingAdmission._make_candidate(
                "BANANAUSDT",
                "bybit",
                "hyperliquid",
                "bananausdt:bybit->hyperliquid",
                first_funding_timestamp_ms=now_ms + 300_000,
            ),
        ]

        async def fake_connect_ws_streams():
            return len(rt.ws_bbo_data_plane._clients)

        monkeypatch.setattr(
            rt.ws_bbo_data_plane,
            "connect_ws_streams",
            fake_connect_ws_streams,
        )
        try:
            await rt._ensure_entry_bbo_active_for_candidates(candidates, now_ms)
        finally:
            rt.journal.close()

        assert set(rt.ws_bbo_data_plane._clients) == {
            ("bybit", "MELONUSDT"),
            ("hyperliquid", "MELONUSDT"),
        }

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_marks_budget_excluded_candidates_separately(
        self,
        tmp_path,
        monkeypatch,
    ):
        from collections import Counter
        import json
        from lightfee.engine.runtime import LiveRuntime

        now_ms = 1778985600000
        config = TestPrimaryTrackingAdmission._make_config(
            mode="live",
            journal_path=str(tmp_path / "events.jsonl"),
        )
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_ws_bbo_per_venue_budget = 1
        rt = LiveRuntime(config)
        rt.journal.open()
        candidates = [
            TestPrimaryTrackingAdmission._make_candidate(
                "MELONUSDT",
                "bybit",
                "hyperliquid",
                "melonusdt:bybit->hyperliquid",
                first_funding_timestamp_ms=now_ms + 300_000,
            ),
            TestPrimaryTrackingAdmission._make_candidate(
                "BANANAUSDT",
                "bybit",
                "hyperliquid",
                "bananausdt:bybit->hyperliquid",
                first_funding_timestamp_ms=now_ms + 300_000,
            ),
        ]

        async def fake_connect_ws_streams():
            return len(rt.ws_bbo_data_plane._clients)

        monkeypatch.setattr(
            rt.ws_bbo_data_plane,
            "connect_ws_streams",
            fake_connect_ws_streams,
        )
        try:
            await rt._ensure_entry_bbo_active_for_candidates(candidates, now_ms)
            selection: Counter = Counter()
            blockers: dict[str, str] = {}
            selected = rt._select_entry_candidates(
                [candidates[1]],
                now_ms=now_ms,
                remaining_slots=1,
                selection_blocker_counts=selection,
                candidate_blockers=blockers,
            )
        finally:
            rt.journal.close()

        assert selected == []
        assert selection == Counter({
            "entry_ws_bbo_quote_lease_budget_exhausted": 1,
        })
        assert blockers == {
            "bananausdt:bybit->hyperliquid": (
                "entry_ws_bbo_quote_lease_budget_exhausted"
            ),
        }
        records = [
            json.loads(line)
            for line in rt.journal.path.read_text().splitlines()
            if line.strip()
        ]
        payload = [
            r["payload"] for r in records
            if r["kind"] == "runtime.entry_blocked_ws_bbo_selection"
        ][-1]
        evidence = payload["readiness_evidence"]
        assert payload["source"] == "ws_bbo_quote_lease"
        assert payload["provider"] == "ws_bbo_quote_lease"
        assert payload["domain"] == "ws_bbo_subscription"
        assert evidence["coverage_reason"] == "subscription_budget_exhausted"
        assert evidence["blocker_family"] == "subscription_budget"
        assert evidence["per_venue_budget"] == 1
        assert evidence["long_subscription_budget"]["excluded"] is True
        assert evidence["short_subscription_budget"]["excluded"] is True

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_tick_prewarms_before_snapshot_quote_filter(
        self,
        tmp_path,
        monkeypatch,
    ):
        from lightfee.config.schema import (
            AppConfig,
            PersistenceConfig,
            RuntimeConfig,
            StrategyConfig,
        )
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.engine.lifecycle import transition_to_running
        from lightfee.sidecar.snapshot import CandidateInput, SidecarSnapshot

        now_ms = 1778985600000
        candidate = CandidateInput(
            long_venue="bybit",
            short_venue="hyperliquid",
            symbol="BANANAUSDT",
            funding_diff_bps=15.0,
            funding_edge_bps=15.0,
            expected_edge_bps=15.0,
            worst_case_edge_bps=10.0,
            ranking_edge_bps=15.0,
            entry_notional_quote=500.0,
            pair_id="bananausdt:bybit->hyperliquid",
            first_funding_timestamp_ms=now_ms + 300_000,
            funding_timestamp_ms=now_ms + 300_000,
            long_funding_timestamp_ms=now_ms + 300_000,
            short_funding_timestamp_ms=now_ms + 300_000,
            economics_complete=True,
            economics_observed_at_ms=now_ms,
            calculation_version="v1_exact",
            model_epoch="v1_exact",
        )
        snapshot = SidecarSnapshot(
            published_at_ms=now_ms,
            market_observed_at_ms=now_ms,
            candidates=[candidate],
            quotes={},
        )
        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                sidecar_snapshot_max_age_ms=600_000,
                max_market_age_ms=1_000,
                live_scan_recovery_success_count=1,
            ),
            strategy=StrategyConfig(
                local_l2_enabled=False,
                entry_readiness_provider="ws_bbo_quote_lease",
                funding_new_entries_enabled=True,
                max_concurrent_positions=1,
                min_scan_minutes_before_funding=0,
                max_scan_minutes_before_funding=10,
                entry_window_secs=60,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "state.json"),
            ),
        )
        rt = LiveRuntime(config)
        rt.entry_executor = object()
        transition_to_running(rt.state)
        seen_symbols: list[str] = []

        async def capture_prewarm(candidates, prewarm_now_ms):
            seen_symbols.extend(str(getattr(c, "symbol", "")) for c in candidates)

        async def passthrough_catalog(candidates):
            return list(candidates)

        monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
        _install_v7_object_snapshot_fixture(monkeypatch, snapshot)
        _allow_test_entry_account_truth(monkeypatch, rt)
        monkeypatch.setattr(
            rt,
            "_filter_candidates_supported_by_venue_catalog",
            passthrough_catalog,
        )
        monkeypatch.setattr(rt, "_ensure_entry_bbo_active_for_candidates", capture_prewarm)

        rt.journal.open()
        try:
            await rt.tick()
        finally:
            rt.journal.close()

        assert seen_symbols == ["BANANAUSDT"]


# ===========================================================================
# Dual-ready book state reason taxonomy
# ===========================================================================


class TestBookReadinessReasonTaxonomy:
    """V1 parity: every not-ready reason must be precise; 'book_hot' must
    never appear as a not-ready reason."""

    @staticmethod
    def _make_book(status="hot", bid=1.0, ask=1.1, observed_at_ms=1778985600000,
                   fault_reason="", sequence=1):
        from types import SimpleNamespace

        class BookStatus:
            def __init__(self, value):
                self.value = value

        book = SimpleNamespace(
            venue="binance",
            symbol="CHIPUSDT",
            status=BookStatus(status),
            observed_at_ms=observed_at_ms,
            sequence=sequence,
            fault_reason=fault_reason,
        )
        book.best_bid = lambda: bid
        book.best_ask = lambda: ask
        book.has_crossed_book = lambda: bid > 0 and ask > 0 and bid >= ask
        book.is_stale = lambda max_age_ms, now_ms: (now_ms - observed_at_ms) > max_age_ms
        book.age_ms = lambda now_ms: now_ms - observed_at_ms if observed_at_ms > 0 else 0
        return book

    def _make_leg(self, venue="binance", symbol="CHIPUSDT"):
        return EntryLocalL2LegSession(venue=venue, symbol=symbol)

    def test_bootstrapping_is_not_ready(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="bootstrapping")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["ready"] is False
        assert result["reason"] == "book_bootstrapping"
        assert result["reason"] != "book_hot"

    def test_rebuilding_is_not_ready(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="rebuilding")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["ready"] is False
        assert result["reason"] == "book_rebuilding"
        assert result["reason"] != "book_hot"

    def test_hot_but_stale_is_not_ready(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="hot", observed_at_ms=1778985000000)  # 600s ago
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["ready"] is False
        assert result["reason"] == "stale_book"
        assert result["reason"] != "book_hot"

    def test_hot_but_crossed_is_not_ready(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="hot", bid=1.1, ask=1.0)  # crossed
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["ready"] is False
        assert result["reason"] == "crossed_or_locked_book"
        assert result["reason"] != "book_hot"

    def test_hot_and_fresh_is_ready(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="hot", bid=1.0, ask=1.1, observed_at_ms=1778985600000)
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["ready"] is True
        assert result["reason"] == "ready"
        assert result["detail"] == "local_l2_book_hot_fresh"
        assert result["reason"] != "book_hot"

    @pytest.mark.parametrize(
        ("book_status", "bid", "ask", "observed_at_ms", "expected_reason"),
        [
            ("bootstrapping", 1.0, 1.1, 1778985600000, "book_bootstrapping"),
            ("rebuilding", 1.0, 1.1, 1778985600000, "book_rebuilding"),
            ("hot", 1.0, 1.1, 1778985000000, "stale_book"),
            ("hot", 1.1, 1.0, 1778985600000, "crossed_or_locked_book"),
        ],
    )
    def test_entry_l2_not_ready_reasons_are_specific(
        self, book_status, bid, ask, observed_at_ms, expected_reason,
    ):
        """Plan-specified parametrized test: each book state maps to a precise reason."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(
            status=book_status, bid=bid, ask=ask, observed_at_ms=observed_at_ms,
        )

        result = apply_book_readiness_to_leg(
            leg, book, now_ms=1778985600000, stale_after_ms=300_000,
        )

        assert result["ready"] is False
        assert result["reason"] == expected_reason
        assert result["reason"] != "book_hot"

    def test_hot_empty_bid_is_not_ready(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="hot", bid=0.0, ask=1.1, observed_at_ms=1778985600000)
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["ready"] is False
        assert result["reason"] == "book_empty_side"
        assert result["reason"] != "book_hot"

    def test_hot_missing_timestamp_is_not_ready(self):
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="hot", observed_at_ms=0)
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["ready"] is False
        assert result["reason"] == "book_timestamp_missing"
        assert result["reason"] != "book_hot"

    def test_hot_with_stale_fault_is_not_book_hot(self):
        """Regression CL-002-B: HOT book with leftover stale_hot_book fault_reason
        must report the true readiness state, never 'book_hot'.

        Production recurrence: book transitions stale→rebuilding→bootstrapping→hot
        but fault_reason="stale_hot_book" survives, making apply_book_readiness_to_leg
        report reason="book_hot" even though the book is healthy.  V1 mark_leg_ready
        always clears fault; V2 transition_to_hot must clear fault_reason.
        """
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(
            status="hot",
            bid=1.0,
            ask=1.1,
            observed_at_ms=1778985600000,
            fault_reason="stale_hot_book",
        )
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] != "book_hot", (
            f"book_hot must never be a not-ready reason; got reason={result['reason']} "
            f"detail={result['detail']}"
        )
        assert result["ready"] is True, (
            f"HOT+fresh+non-empty+uncrossed book must be ready; got reason={result['reason']}"
        )
        assert result["reason"] == "ready"
        assert result["detail"] == "local_l2_book_hot_fresh"

    def test_hot_with_stale_fault_but_stale_is_caught(self):
        """HOT book with stale_hot_book fault that IS actually stale must report
        stale_book, not book_hot."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(
            status="hot",
            bid=1.0,
            ask=1.1,
            observed_at_ms=1778985000000,  # 600s old
            fault_reason="stale_hot_book",
        )
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] != "book_hot"
        assert result["reason"] == "stale_book"


# ===========================================================================
# V1 parity: specific arming_reason from book context (DP-1)
# ===========================================================================


class TestArmingReasonFromBookContext:
    """V1 ensure_candidate() maps prior fault → specific arming_reason.
    V2 apply_book_readiness_to_leg must derive reason from book.fault_reason."""

    @staticmethod
    def _make_book(status="bootstrapping", bid=1.0, ask=1.1, observed_at_ms=1778985600000,
                   fault_reason="", sequence=1):
        from types import SimpleNamespace

        class BookStatus:
            def __init__(self, value):
                self.value = value

        book = SimpleNamespace(
            venue="binance",
            symbol="CHIPUSDT",
            status=BookStatus(status),
            observed_at_ms=observed_at_ms,
            sequence=sequence,
            fault_reason=fault_reason,
        )
        book.best_bid = lambda: bid
        book.best_ask = lambda: ask
        book.has_crossed_book = lambda: bid > 0 and ask > 0 and bid >= ask
        book.is_stale = lambda max_age_ms, now_ms: (now_ms - observed_at_ms) > max_age_ms
        book.age_ms = lambda now_ms: now_ms - observed_at_ms if observed_at_ms > 0 else 0
        return book

    def _make_leg(self, venue="binance", symbol="CHIPUSDT"):
        return EntryLocalL2LegSession(venue=venue, symbol=symbol)

    def test_bootstrapping_with_stale_fault_gives_stale_recovery_reason(self):
        """V1: StaleBook fault → StaleBookRecovery arming_reason."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="bootstrapping", fault_reason="stale_hot_book")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] == "book_bootstrapping"
        assert leg.arming_reason == SessionArmingReason.STALE_BOOK_RECOVERY, (
            f"stale fault should derive STALE_BOOK_RECOVERY, got {leg.arming_reason}"
        )

    def test_bootstrapping_with_sequence_gap_fault_gives_sequence_gap_reason(self):
        """V1: GateObuGap/OkxPrevSeqMismatch → SequenceGap arming_reason."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="bootstrapping", fault_reason="sequence_gap_5")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] == "book_bootstrapping"
        assert leg.arming_reason == SessionArmingReason.SEQUENCE_GAP, (
            f"sequence_gap fault should derive SEQUENCE_GAP, got {leg.arming_reason}"
        )

    def test_bootstrapping_with_checksum_fault_gives_sequence_gap_reason(self):
        """V1: OkxChecksumMismatch → SequenceGap arming_reason (checksum is a sequence fault)."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="rebuilding", fault_reason="checksum_mismatch expected=123 actual=456")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] == "book_rebuilding"
        assert leg.arming_reason == SessionArmingReason.SEQUENCE_GAP, (
            f"checksum fault should derive SEQUENCE_GAP, got {leg.arming_reason}"
        )

    def test_bootstrapping_with_transport_fault_gives_transport_recovery_reason(self):
        """V1: HyperliquidDisconnect → TransportFaultRecovery arming_reason."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="bootstrapping", fault_reason="snapshot_bootstrap: connection timeout")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] == "book_bootstrapping"
        assert leg.arming_reason == SessionArmingReason.TRANSPORT_FAULT_RECOVERY, (
            f"transport fault should derive TRANSPORT_FAULT_RECOVERY, got {leg.arming_reason}"
        )

    def test_bootstrapping_without_fault_gives_book_status_transition(self):
        """V1: No prior fault → BookStatusTransition (or FirstSession) arming_reason."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="bootstrapping", fault_reason="")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] == "book_bootstrapping"
        assert leg.arming_reason == SessionArmingReason.BOOK_STATUS_TRANSITION, (
            f"no fault should keep BOOK_STATUS_TRANSITION, got {leg.arming_reason}"
        )

    def test_rebuilding_preserves_fault_reason_in_detail(self):
        """REBUILDING books must carry the fault_reason in diagnostics detail."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="rebuilding", fault_reason="pre_snapshot_buffer_overflow")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] == "book_rebuilding"
        assert "pre_snapshot_buffer_overflow" in result["detail"], (
            f"detail must carry fault_reason, got {result['detail']}"
        )

    def test_cold_book_arming_reason_is_first_session(self):
        """COLD book (never seen) → FIRST_SESSION arming_reason."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        leg = self._make_leg()
        book = self._make_book(status="cold", observed_at_ms=0, fault_reason="")
        result = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000, stale_after_ms=300_000)

        assert result["reason"] == "book_cold"
        assert leg.arming_reason == SessionArmingReason.FIRST_SESSION, (
            f"cold book should derive FIRST_SESSION, got {leg.arming_reason}"
        )


# ===========================================================================
# V1 parity: REBUILDING→BOOTSTRAPPING→readiness 完整闭环 (real path)
# ===========================================================================


class TestRebuildingToBootstrappingReadinessRealPath:
    """V1 ensure_candidate() fault→arming 的完整闭环：REBUILDING+fault 的书
    在 transition_to_bootstrapping() 后，apply_book_readiness_to_leg 必须
    保留 fault_reason 并推导出正确的 arming_reason。

    阻断问题：transition_to_bootstrapping() 曾无条件清空 fault_reason=""，
    导致 sequence_gap/checksum/transport 原因被丢弃，arming_reason 回到
    BOOK_STATUS_TRANSITION。
    """

    @staticmethod
    def _make_book(status="rebuilding", bid=1.0, ask=1.1,
                   observed_at_ms=1778985600000, fault_reason="",
                   sequence=1):
        from lightfee.marketdata.l2 import L2BookStatus, LocalL2Book, PriceLevel

        book = LocalL2Book(venue="binance", symbol="CHIPUSDT")
        book.bids = [PriceLevel(price=bid, quantity=1.0)]
        book.asks = [PriceLevel(price=ask, quantity=1.0)]
        book.observed_at_ms = observed_at_ms
        book.sequence = sequence
        book.fault_reason = fault_reason
        # Set status via internal field (bypass transition for test setup)
        book.status = getattr(L2BookStatus, status.upper()) if isinstance(status, str) else status
        return book

    def _make_leg(self, venue="binance", symbol="CHIPUSDT"):
        return EntryLocalL2LegSession(venue=venue, symbol=symbol)

    def _run_real_path(self, fault_reason: str, expected_arming: SessionArmingReason,
                       now_ms: int = 1778985600000, stale_after_ms: int = 300_000):
        """Simulate the real path: book with fault → transition_to_bootstrapping
        → apply_book_readiness_to_leg. Verify arming_reason is NOT lost."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        book = self._make_book(status="rebuilding", fault_reason=fault_reason)
        # Real path: bootstrap worker calls transition_to_bootstrapping
        book.transition_to_bootstrapping(now_ms=now_ms)
        assert book.status.value == "bootstrapping"

        leg = self._make_leg()
        diag = apply_book_readiness_to_leg(leg, book, now_ms=now_ms,
                                           stale_after_ms=stale_after_ms)

        assert diag["reason"] == "book_bootstrapping", (
            f"reason should be book_bootstrapping, got {diag['reason']}"
        )
        assert leg.arming_reason == expected_arming, (
            f"fault_reason={fault_reason!r} → expected arming={expected_arming.value}, "
            f"got {leg.arming_reason.value if leg.arming_reason else None}. "
            f"diag.detail={diag['detail']!r}"
        )
        # Detail must carry the fault context, NOT just bare "book_status=bootstrapping"
        assert fault_reason in diag["detail"], (
            f"diag.detail must contain fault_reason={fault_reason!r}, "
            f"got {diag['detail']!r}"
        )
        return diag

    def test_sequence_gap_survives_bootstrapping_real_path(self):
        """REBUILDING(sequence_gap) → BOOTSTRAPPING → SEQUENCE_GAP arming."""
        self._run_real_path("sequence_gap: gap=5 prev=100 incoming_prev=105",
                            SessionArmingReason.SEQUENCE_GAP)

    def test_checksum_mismatch_survives_bootstrapping_real_path(self):
        """REBUILDING(checksum) → BOOTSTRAPPING → SEQUENCE_GAP arming."""
        self._run_real_path("checksum_mismatch: expected=123 actual=456",
                            SessionArmingReason.SEQUENCE_GAP)

    def test_transport_failure_survives_bootstrapping_real_path(self):
        """REBUILDING(transport) → BOOTSTRAPPING → TRANSPORT_FAULT_RECOVERY."""
        self._run_real_path("transport_failure: connection reset",
                            SessionArmingReason.TRANSPORT_FAULT_RECOVERY)

    def test_stale_hot_book_survives_bootstrapping_real_path(self):
        """REBUILDING(stale_hot_book) → BOOTSTRAPPING → STALE_BOOK_RECOVERY."""
        self._run_real_path("stale_hot_book",
                            SessionArmingReason.STALE_BOOK_RECOVERY)

    def test_pre_snapshot_buffer_overflow_survives_bootstrapping_real_path(self):
        """REBUILDING(buffer_overflow) → BOOTSTRAPPING → BOOK_STATUS_TRANSITION."""
        self._run_real_path("pre_snapshot_buffer_overflow",
                            SessionArmingReason.BOOK_STATUS_TRANSITION)

    def test_hot_clears_fault_reason_after_successful_bootstrap_real_path(self):
        """Full recovery: REBUILDING → BOOTSTRAPPING → HOT — fault cleared at HOT."""
        book = self._make_book(status="rebuilding",
                               fault_reason="sequence_gap: gap=5")
        book.transition_to_bootstrapping(now_ms=1778985600000)
        assert book.fault_reason == "sequence_gap: gap=5", (
            "fault_reason must survive bootstrapping"
        )
        # Simulate successful snapshot + bootstrap_book completing
        book.transition_to_hot()
        assert book.status.value == "hot"
        assert book.fault_reason == "", (
            "fault_reason must be cleared at HOT — book has recovered"
        )

    def test_cold_no_fault_bootstrapping_no_false_arming_real_path(self):
        """COLD→BOOTSTRAPPING (no prior fault) → FIRST_SESSION or BOOK_STATUS_TRANSITION."""
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        book = self._make_book(status="cold", fault_reason="")
        book.transition_to_bootstrapping(now_ms=1778985600000)
        assert book.fault_reason == "", "no fault to begin with"

        leg = self._make_leg()
        diag = apply_book_readiness_to_leg(leg, book, now_ms=1778985600000,
                                           stale_after_ms=300_000)
        assert diag["reason"] == "book_bootstrapping"
        # COLD→BOOTSTRAPPING with no prior fault: either FIRST_SESSION or
        # BOOK_STATUS_TRANSITION is acceptable (V1: first session has no prior fault)
        assert leg.arming_reason in (SessionArmingReason.FIRST_SESSION,
                                     SessionArmingReason.BOOK_STATUS_TRANSITION), (
            f"got {leg.arming_reason}"
        )
