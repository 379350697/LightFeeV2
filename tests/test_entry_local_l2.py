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


class TestEntryLocalL2SelectionBlocker:
    """Regression: blocker must work with real CandidateInput (no pair_id/created_at_ms hacks)."""

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
                                    max_concurrent_positions=2),
            persistence=PersistenceConfig(event_log_path=str(tmp_path / "events.jsonl"),
                                          snapshot_path=str(tmp_path / "state.json")),
        )
        return LiveRuntime(config)

    class RealCandidate:
        """Mimics actual CandidateInput — no pair_id, no created_at_ms."""
        def __init__(self, symbol="BTCUSDT", long_venue="binance", short_venue="bybit",
                     ranking_edge_bps=15.0, funding_timestamp_ms=5000):
            self.symbol = symbol
            self.long_venue = long_venue
            self.short_venue = short_venue
            self.ranking_edge_bps = ranking_edge_bps
            self.funding_timestamp_ms = funding_timestamp_ms
            self.entry_notional_quote = 1000.0
            self.expected_edge_bps = 15.0
            self.worst_case_edge_bps = 10.0
            self.funding_edge_bps = 15.0

    def test_no_session_dual_ready_must_block(self, runtime_with_l2):
        """Real candidate with no session MUST be blocked."""
        rt = runtime_with_l2
        c = self.RealCandidate(funding_timestamp_ms=5000)
        # No session — must block
        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason is not None, "entry should be blocked without session"
        assert "dual_ready" in reason or "primary_tracking" in reason or "prewarm" in reason

    def test_missing_funding_timestamp_blocks_with_prewarm(self, runtime_with_l2):
        """Candidate without funding_timestamp_ms must be blocked for prewarm."""
        rt = runtime_with_l2
        c = self.RealCandidate(funding_timestamp_ms=0)
        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason == "entry_local_l2_waiting_for_prewarm_window"

    def test_dual_ready_session_allows_entry(self, runtime_with_l2):
        """Dual-ready session for a tracked primary allows entry."""
        rt = runtime_with_l2
        c = self.RealCandidate()
        pair_id = make_candidate_pair_id(c.symbol, c.long_venue, c.short_venue)

        # Tracked as primary
        rt._tracked_primary_pair_ids.add(pair_id)

        # Create dual-ready session
        session = rt.entry_l2_sessions.get_or_create_session(pair_id)
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.refresh_state(now_ms=10000, stale_after_ms=300_000)

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason is None, f"dual-ready entry should not be blocked, got: {reason}"

    def test_not_in_primary_set_blocks(self, runtime_with_l2):
        """Session exists but not in tracked primary set → blocked."""
        rt = runtime_with_l2
        c = self.RealCandidate()
        pair_id = make_candidate_pair_id(c.symbol, c.long_venue, c.short_venue)

        # Session exists but NOT in primary set
        session = rt.entry_l2_sessions.get_or_create_session(pair_id)
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=9000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=9000)

        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason == "entry_local_l2_waiting_for_primary_tracking"

    def test_not_blocked_when_local_l2_disabled(self, runtime_with_l2):
        """When local_l2_enabled=False, blocker returns None always."""
        runtime_with_l2.config.strategy.local_l2_enabled = False
        c = self.RealCandidate(funding_timestamp_ms=0)
        reason = runtime_with_l2._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason is None

    def test_no_created_at_ms_field_on_candidate_is_ok(self, runtime_with_l2):
        """The blocker must NOT rely on created_at_ms. V1 uses funding_timestamp_ms for prewarm."""
        rt = runtime_with_l2
        c = self.RealCandidate(funding_timestamp_ms=5000)
        assert not hasattr(c, "created_at_ms"), "candidate should not have created_at_ms"
        # With funding timestamp but no session — should still block correctly
        reason = rt._entry_local_l2_selection_blocker(c, now_ms=10000)
        assert reason is not None
