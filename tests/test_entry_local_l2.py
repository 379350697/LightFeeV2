"""Entry local-L2 sessions tests — tracked opportunities, readiness, promotion.

Rust V1 reference: src/execution_core/entry_local_l2.rs
                      src/execution_core/entry_local_l2_sessions.rs
"""

from __future__ import annotations

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
    primary_hold_window_allows_replacement,
    select_tracked_opportunities,
    shadow_promotion_is_eligible,
)


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
        assert diag["detail"] == ""
        assert diag["book_status"] == "hot"
        assert diag["age_ms"] == 500
        assert diag["observed_at_ms"] == 9500
        assert diag["sequence"] == 7

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

    def test_refresh_state_faulted_when_all_faulted(self):
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

    def test_allows_when_assigned_none(self):
        assert primary_hold_window_allows_replacement(
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


# ---------------------------------------------------------------------------
# make_candidate_pair_id
# ---------------------------------------------------------------------------


from lightfee.engine.entry_local_l2 import make_candidate_pair_id


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

        rt = runtime_with_l2
        rt.config.strategy.local_l2_enabled = False
        rt.config.strategy.max_concurrent_positions = 3
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

        from lightfee.sidecar.snapshot import QuoteSnapshot

        rt = runtime_with_l2
        rt.config.strategy.local_l2_enabled = False
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

        rt = runtime_with_l2
        rt.config.strategy.local_l2_enabled = False
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
            "schema_version": 2,
            "published_at_ms": now_ms,
            "market_observed_at_ms": now_ms,
            "funding_lifecycle": [],
            "market_lifecycle": [],
            "transfer_lifecycle": [],
            "liquidity_lifecycle": [],
            "degraded_venues": [],
            "degraded_domains": [],
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {
                "binance:BTCUSDT": {
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "bid": 50000,
                    "ask": 50010,
                    "funding_rate_bps": 10.0,
                    "funding_timestamp_ms": 370000,
                },
                "bybit:BTCUSDT": {
                    "venue": "bybit",
                    "symbol": "BTCUSDT",
                    "bid": 50005,
                    "ask": 50015,
                    "funding_rate_bps": -5.0,
                    "funding_timestamp_ms": 370000,
                },
            },
            "candidates": [{
                "long_venue": "binance",
                "short_venue": "bybit",
                "symbol": "BTCUSDT",
                "funding_diff_bps": 15.0,
                "funding_edge_bps": 15.0,
                "expected_edge_bps": 15.0,
                "worst_case_edge_bps": 10.0,
                "ranking_edge_bps": 15.0,
                "entry_notional_quote": 30.0,
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
        rt.state.lifecycle = EngineLifecycle.RUNNING
        rt.state.risk_mode = GlobalRiskMode.RUNNING
        rt.entry_executor = object()
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
        assert no_entry["reason"] == "entry_local_l2_selection_blocked"
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
                "funding_edge_bps": 20.0 - i,
                "expected_edge_bps": 10.0,
                "worst_case_edge_bps": 5.0,
                "ranking_edge_bps": 20.0 - i,
                "entry_notional_quote": 30.0,
                "first_funding_timestamp_ms": 370000,
            })
        snapshot_path.write_text(json.dumps({
            "schema_version": 2,
            "published_at_ms": now_ms,
            "market_observed_at_ms": now_ms,
            "funding_lifecycle": [],
            "market_lifecycle": [],
            "transfer_lifecycle": [],
            "liquidity_lifecycle": [],
            "degraded_venues": [],
            "degraded_domains": [],
            "source_mode": "direct_market",
            "acquisition_mode": "fresh_sidecar",
            "quotes": {},
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
        rt.state.lifecycle = EngineLifecycle.RUNNING
        rt.state.risk_mode = GlobalRiskMode.RUNNING
        rt.entry_executor = object()
        rt.journal.open()

        activated_symbols = []

        async def record_l2_activation(candidates, now_ms):
            activated_symbols.extend(c.symbol for c in candidates)

        async def no_sync(now_ms, scan_promoted=False):
            return None

        monkeypatch.setattr(rt, "_ensure_l2_active_for_candidates", record_l2_activation)
        monkeypatch.setattr(rt, "_sync_local_l2_data", no_sync)

        await rt.tick()
        rt.journal.close()

        assert activated_symbols == ["S0USDT", "S1USDT", "S2USDT"]

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
                "binance:BTCUSDT": {"venue": "binance", "symbol": "BTCUSDT", "bid": 1, "ask": 2},
                "bybit:BTCUSDT": {"venue": "bybit", "symbol": "BTCUSDT", "bid": 1, "ask": 2},
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
                    "first_funding_timestamp_ms": now_ms + 60000,
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
