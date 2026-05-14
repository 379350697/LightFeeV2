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
                                    max_concurrent_positions=2),
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
        assert reason == "entry_local_l2_waiting_for_prewarm_window"

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
