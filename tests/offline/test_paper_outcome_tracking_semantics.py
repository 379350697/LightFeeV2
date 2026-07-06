"""Paper outcome tracking semantics tests.

Validates that paper outcome tracking behavior matches V1 semantics:
- Registration with finalist limit enforcement
- Markout horizon evaluation with realistic market snapshots
- Settlement grace period terminal events
- Real-vs-paper outcome joining
- Disabled/no-op behavior when tracking is off
- Classification labels match V1 exactly
"""

from __future__ import annotations

import pytest
from lightfee.offline.paper_outcome import (
    PaperOutcomeConfig,
    PaperOpportunityRegistration,
    PaperOutcomeTracker,
    classify_paper_outcome,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def enabled_config() -> PaperOutcomeConfig:
    return PaperOutcomeConfig(
        tracking_enabled=True,
        finalist_limit=3,
        markout_secs=[300, 1800],
        settlement_grace_secs=120,
    )


@pytest.fixture
def disabled_config() -> PaperOutcomeConfig:
    return PaperOutcomeConfig(
        tracking_enabled=False,
        finalist_limit=3,
        markout_secs=[300, 1800],
        settlement_grace_secs=0,
    )


@pytest.fixture
def zero_finalist_config() -> PaperOutcomeConfig:
    return PaperOutcomeConfig(
        tracking_enabled=True,
        finalist_limit=0,
        markout_secs=[300, 1800],
        settlement_grace_secs=0,
    )


@pytest.fixture
def sample_registration() -> PaperOpportunityRegistration:
    return PaperOpportunityRegistration(
        paper_id="paper-rvw-1",
        review_id="rvw-1",
        symbol="LABUSDT",
        pair_id="okx:LABUSDT->aster:LABUSDT",
        long_venue="okx",
        short_venue="aster",
        finalist_rank=0,
        selected_real_trade=False,
        not_selected_reason="capacity_full",
        registered_at_ms=1000,
        target_settlement_ts_ms=3_600_000,
        markout_secs=[300],
        entry_notional_quote=50.0,
        fee_quote=0.05,
        expected_funding_quote=0.4,
        entry_slippage_quote=0.02,
    )


@pytest.fixture
def market_snapshots() -> dict:
    return {
        "okx:LABUSDT": {"mid": 1.1},
        "aster:LABUSDT": {"mid": 1.0},
    }


# ---------------------------------------------------------------------------
# classify_paper_outcome
# ---------------------------------------------------------------------------

class TestClassifyPaperOutcome:
    """V1 classify_paper_outcome: label matching."""

    def test_good_trade_missed(self):
        assert classify_paper_outcome(False, 0.33, None) == "good_trade_missed"

    def test_bad_trade_correctly_rejected(self):
        assert classify_paper_outcome(False, -0.10, None) == "bad_trade_correctly_rejected"

    def test_good_trade_executed(self):
        assert classify_paper_outcome(True, -0.10, 0.20) == "good_trade_executed"

    def test_bad_trade_executed(self):
        assert classify_paper_outcome(True, 0.40, -0.20) == "bad_trade_executed"

    def test_unknown_missing_snapshot(self):
        assert classify_paper_outcome(False, None, None) == "unknown_due_to_missing_snapshot"

    def test_unknown_incomplete_lifecycle(self):
        assert classify_paper_outcome(True, None, None) == "unknown_due_to_incomplete_lifecycle"


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

class TestPaperOpportunityRegistration:
    """Registration and finalist limit enforcement."""

    def test_registers_when_enabled_and_within_limit(self, enabled_config):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="p1", review_id=None, symbol="BTC-USDT",
            pair_id="binance:BTC-USDT->okx:BTC-USDT",
            long_venue="binance", short_venue="okx",
            finalist_rank=0, selected_real_trade=False,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
        )
        assert tracker.register(reg) is True
        assert tracker.tracked_count == 1

    def test_idempotent_by_paper_id(self, enabled_config):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="p1", review_id=None, symbol="BTC-USDT",
            pair_id="binance:BTC-USDT->okx:BTC-USDT",
            long_venue="binance", short_venue="okx",
            finalist_rank=0, selected_real_trade=False,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
        )
        assert tracker.register(reg) is True
        assert tracker.register(reg) is False  # duplicate paper_id
        assert tracker.tracked_count == 1

    def test_rejects_beyond_finalist_limit(self, enabled_config):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="p_out_of_range", review_id=None, symbol="BTC-USDT",
            pair_id="binance:BTC-USDT->okx:BTC-USDT",
            long_venue="binance", short_venue="okx",
            finalist_rank=5,  # beyond limit of 3
            selected_real_trade=False,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
        )
        assert tracker.register(reg) is False
        assert tracker.tracked_count == 0

    def test_noop_when_disabled(self, disabled_config):
        tracker = PaperOutcomeTracker(disabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="p1", review_id=None, symbol="BTC-USDT",
            pair_id="binance:BTC-USDT->okx:BTC-USDT",
            long_venue="binance", short_venue="okx",
            finalist_rank=0, selected_real_trade=False,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
        )
        assert tracker.register(reg) is False
        assert tracker.tracked_count == 0

    def test_noop_when_zero_finalist_limit(self, zero_finalist_config):
        tracker = PaperOutcomeTracker(zero_finalist_config)
        reg = PaperOpportunityRegistration(
            paper_id="p1", review_id=None, symbol="BTC-USDT",
            pair_id="binance:BTC-USDT->okx:BTC-USDT",
            long_venue="binance", short_venue="okx",
            finalist_rank=0, selected_real_trade=False,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
        )
        assert tracker.register(reg) is False
        assert tracker.tracked_count == 0

    def test_register_multiple_within_limit(self, enabled_config):
        tracker = PaperOutcomeTracker(enabled_config)
        for i in range(3):
            reg = PaperOpportunityRegistration(
                paper_id=f"p{i}", review_id=None, symbol="BTC-USDT",
                pair_id="binance:BTC-USDT->okx:BTC-USDT",
                long_venue="binance", short_venue="okx",
                finalist_rank=i, selected_real_trade=False,
                not_selected_reason=None, registered_at_ms=1000,
                target_settlement_ts_ms=None, markout_secs=[300],
            )
            assert tracker.register(reg) is True
        assert tracker.tracked_count == 3


# ---------------------------------------------------------------------------
# Markout events
# ---------------------------------------------------------------------------

class TestPaperOutcomeMarkouts:
    """V1 markout horizon evaluation."""

    def test_emits_markout_at_due_time(self, enabled_config, sample_registration, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        tracker.register(sample_registration)

        # Before markout time: no events
        events = tracker.evaluate_due(1001, market_snapshots)
        assert len(events) == 0

        # At 301000ms (300s after registered_at_ms=1000): one markout event
        events = tracker.evaluate_due(301_000, market_snapshots)
        assert len(events) == 1
        assert events[0]["kind"] == "opportunity.paper_markout"
        payload = events[0]["payload"]
        assert payload["horizon_kind"] == "markout_300s"
        assert payload["paper_id"] == "paper-rvw-1"
        assert payload["symbol"] == "LABUSDT"
        assert payload["selected_real_trade"] is False
        assert payload["not_selected_reason"] == "capacity_full"

    def test_markout_is_idempotent(self, enabled_config, sample_registration, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        tracker.register(sample_registration)

        events1 = tracker.evaluate_due(301_000, market_snapshots)
        assert len(events1) == 1

        events2 = tracker.evaluate_due(302_000, market_snapshots)
        assert len(events2) == 0  # already emitted

    def test_unknown_label_for_missing_snapshot(self, enabled_config, sample_registration):
        tracker = PaperOutcomeTracker(enabled_config)
        tracker.register(sample_registration)

        events = tracker.evaluate_due(301_000, {})  # empty market snapshots
        assert len(events) == 1
        assert events[0]["payload"]["opportunity_label"] == "unknown_due_to_missing_snapshot"
        assert events[0]["payload"]["market_snapshot"]["snapshot_available"] is False

    def test_good_trade_missed_label(self, enabled_config):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="p-good", review_id=None, symbol="BTC-USDT",
            pair_id="binance:BTC-USDT->okx:BTC-USDT",
            long_venue="binance", short_venue="okx",
            finalist_rank=0, selected_real_trade=False,
            not_selected_reason="capacity_full",
            registered_at_ms=1000,
            target_settlement_ts_ms=None,
            markout_secs=[300],
            entry_notional_quote=1000.0,
            fee_quote=1.0,
            expected_funding_quote=5.0,
            entry_slippage_quote=0.5,
        )
        tracker.register(reg)
        # long_mid=1.01, short_mid=1.02 => positive spread => positive net quote
        snaps = {"binance:BTC-USDT": {"mid": 1.01}, "okx:BTC-USDT": {"mid": 1.02}}
        events = tracker.evaluate_due(301_000, snaps)
        assert len(events) == 1
        assert events[0]["payload"]["opportunity_label"] == "good_trade_missed"


# ---------------------------------------------------------------------------
# Settlement events
# ---------------------------------------------------------------------------

class TestPaperOutcomeSettlement:
    """V1 settlement grace and terminal events."""

    def test_emits_settlement_at_due_time(self, enabled_config, sample_registration, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        tracker.register(sample_registration)

        # First fire the markout
        tracker.evaluate_due(301_000, market_snapshots)

        # At settlement time: emits paper_closed (terminal)
        events = tracker.evaluate_due(3_600_001, market_snapshots)
        assert len(events) == 1
        assert events[0]["kind"] == "opportunity.paper_closed"
        assert events[0]["payload"]["horizon_kind"] == "settlement"

    def test_settlement_also_idempotent(self, enabled_config, sample_registration, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        tracker.register(sample_registration)

        tracker.evaluate_due(301_000, market_snapshots)
        tracker.evaluate_due(3_600_001, market_snapshots)
        events = tracker.evaluate_due(3_700_000, market_snapshots)
        assert len(events) == 0

    def test_no_settlement_when_no_target_ts(self, enabled_config, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="p-no-settlement", review_id=None, symbol="BTC-USDT",
            pair_id="binance:BTC-USDT->okx:BTC-USDT",
            long_venue="binance", short_venue="okx",
            finalist_rank=0, selected_real_trade=False,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None,  # no settlement target
            markout_secs=[300],
        )
        tracker.register(reg)
        tracker.evaluate_due(301_000, market_snapshots)
        events = tracker.evaluate_due(10_000_000, market_snapshots)
        # Only markout was emitted; no settlement horizon exists
        assert all(e["kind"] != "opportunity.paper_closed" for e in events)


# ---------------------------------------------------------------------------
# Real vs paper join
# ---------------------------------------------------------------------------

class TestRealVsPaperJoin:
    """V1 join_real_outcome: matching paper outcome with realized trade."""

    def test_joins_by_review_id(self, enabled_config, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="paper-real-1", review_id="rvw-real",
            symbol="LABUSDT", pair_id="okx:LABUSDT->aster:LABUSDT",
            long_venue="okx", short_venue="aster",
            finalist_rank=0, selected_real_trade=True,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
            entry_notional_quote=50.0,
            fee_quote=0.05,
            expected_funding_quote=0.4,
            entry_slippage_quote=0.02,
        )
        tracker.register(reg)
        # Fire markout first so latest_outcome_payload is set
        tracker.evaluate_due(301_000, market_snapshots)

        joined = tracker.join_real_outcome("rvw-real", "pos-1", 0.25, 50.0, 370_000)
        assert joined is not None
        assert joined["kind"] == "opportunity.real_vs_paper_joined"
        payload = joined["payload"]
        assert payload["review_id"] == "rvw-real"
        assert payload["position_id"] == "pos-1"
        assert payload["real_net_quote"] == 0.25
        assert payload["real_net_bps"] == 50.0
        assert payload["opportunity_label"] == "good_trade_executed"

    def test_join_idempotent(self, enabled_config, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="paper-real-2", review_id="rvw-real-2",
            symbol="LABUSDT", pair_id="okx:LABUSDT->aster:LABUSDT",
            long_venue="okx", short_venue="aster",
            finalist_rank=0, selected_real_trade=True,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
        )
        tracker.register(reg)
        tracker.evaluate_due(301_000, market_snapshots)

        assert tracker.join_real_outcome("rvw-real-2", "pos-1", 0.25, None, 370_000) is not None
        assert tracker.join_real_outcome("rvw-real-2", "pos-1", 0.25, None, 371_000) is None

    def test_join_requires_review_id_match(self, enabled_config, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="paper-no-match", review_id="rvw-match",
            symbol="LABUSDT", pair_id="okx:LABUSDT->aster:LABUSDT",
            long_venue="okx", short_venue="aster",
            finalist_rank=0, selected_real_trade=True,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
        )
        tracker.register(reg)
        tracker.evaluate_due(301_000, market_snapshots)

        # Wrong review_id
        assert tracker.join_real_outcome("wrong-id", "pos-1", 0.25, None, 370_000) is None

    def test_join_requires_selected_real_trade(self, enabled_config, market_snapshots):
        tracker = PaperOutcomeTracker(enabled_config)
        reg = PaperOpportunityRegistration(
            paper_id="paper-not-selected", review_id="rvw-ns",
            symbol="LABUSDT", pair_id="okx:LABUSDT->aster:LABUSDT",
            long_venue="okx", short_venue="aster",
            finalist_rank=0, selected_real_trade=False,  # not selected
            not_selected_reason="capacity_full",
            registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
        )
        tracker.register(reg)
        tracker.evaluate_due(301_000, market_snapshots)

        assert tracker.join_real_outcome("rvw-ns", "pos-1", 0.25, None, 370_000) is None


# ---------------------------------------------------------------------------
# Disabled behavior
# ---------------------------------------------------------------------------

class TestDisabledBehavior:
    """When tracking is disabled, all operations are no-ops."""

    def test_evaluate_due_returns_empty(self, disabled_config, sample_registration):
        tracker = PaperOutcomeTracker(disabled_config)
        tracker.register(sample_registration)  # no-op
        assert tracker.evaluate_due(999_999, {}) == []

    def test_join_real_outcome_returns_none(self, disabled_config):
        tracker = PaperOutcomeTracker(disabled_config)
        assert tracker.join_real_outcome("any", "any", 1.0) is None

    def test_enabled_property(self, enabled_config, disabled_config, zero_finalist_config):
        assert PaperOutcomeTracker(enabled_config).enabled is True
        assert PaperOutcomeTracker(disabled_config).enabled is False
        assert PaperOutcomeTracker(zero_finalist_config).enabled is False


# ---------------------------------------------------------------------------
# Journal analysis integration
# ---------------------------------------------------------------------------

class TestJournalAnalysisIntegration:
    """Paper outcome events are counted by JournalAnalysisReport."""

    def test_analyze_counts_paper_markout(self):
        from lightfee.offline.analysis.journal import analyze_journal_records
        records = [
            {"kind": "opportunity.paper_markout", "payload": {
                "paper_id": "p1", "opportunity_label": "good_trade_missed",
            }},
            {"kind": "opportunity.paper_markout", "payload": {
                "paper_id": "p2", "opportunity_label": "bad_trade_correctly_rejected",
            }},
        ]
        report = analyze_journal_records(records)
        assert report.paper_outcome_markout_count == 2
        assert report.paper_outcome_by_label["good_trade_missed"] == 1
        assert report.paper_outcome_by_label["bad_trade_correctly_rejected"] == 1

    def test_analyze_counts_paper_closed(self):
        from lightfee.offline.analysis.journal import analyze_journal_records
        records = [
            {"kind": "opportunity.paper_closed", "payload": {
                "paper_id": "p1", "horizon_kind": "settlement",
                "opportunity_label": "good_trade_missed",
            }},
        ]
        report = analyze_journal_records(records)
        assert report.paper_outcome_closed_count == 1

    def test_analyze_counts_paper_hedge_filled_without_pnl_totals(self):
        from lightfee.offline.analysis.journal import analyze_journal_records
        records = [
            {"kind": "opportunity.paper_hedge_filled", "payload": {
                "paper_id": "p1",
                "paper_bot_id": "mt_selected_maker_delay_1000ms",
            }},
        ]
        report = analyze_journal_records(records)
        assert report.paper_outcome_hedge_filled_count == 1
        assert report.paper_outcome_markout_count == 0
        assert report.paper_outcome_closed_count == 0
        assert report.paper_outcome_net_quote_total == 0.0

    def test_analyze_counts_paper_evaluation_skipped_without_pnl_totals(self):
        from lightfee.offline.analysis.journal import analyze_journal_records
        records = [
            {"kind": "opportunity.paper_evaluation_skipped", "payload": {
                "paper_id": "p1",
                "opportunity_label": "unknown_due_to_missing_snapshot",
                "paper_fee_quote": 10.0,
                "paper_slippage_quote": 10.0,
            }},
        ]
        report = analyze_journal_records(records)
        assert report.paper_outcome_evaluation_skipped_count == 1
        assert report.paper_outcome_markout_count == 0
        assert report.paper_outcome_closed_count == 0
        assert report.paper_outcome_by_label["unknown_due_to_missing_snapshot"] == 1
        assert report.paper_outcome_net_quote_total == 0.0
        assert report.paper_outcome_fee_quote_total == 0.0
        assert report.paper_outcome_slippage_quote_total == 0.0

    def test_analyze_counts_real_vs_paper_joined(self):
        from lightfee.offline.analysis.journal import analyze_journal_records
        records = [
            {"kind": "opportunity.real_vs_paper_joined", "payload": {
                "paper_id": "p1", "position_id": "pos-1",
                "opportunity_label": "good_trade_executed",
            }},
        ]
        report = analyze_journal_records(records)
        assert report.paper_outcome_joined_count == 1

    def test_analyze_mixed_events(self):
        from lightfee.offline.analysis.journal import analyze_journal_records
        records = [
            {"kind": "opportunity.paper_markout", "payload": {
                "opportunity_label": "good_trade_missed",
            }},
            {"kind": "opportunity.paper_closed", "payload": {
                "opportunity_label": "bad_trade_correctly_rejected",
            }},
            {"kind": "opportunity.real_vs_paper_joined", "payload": {
                "opportunity_label": "good_trade_executed",
            }},
            {"kind": "entry.opened", "payload": {}},  # unrelated
        ]
        report = analyze_journal_records(records)
        assert report.paper_outcome_markout_count == 1
        assert report.paper_outcome_closed_count == 1
        assert report.paper_outcome_joined_count == 1
        assert len(report.paper_outcome_by_label) == 3
