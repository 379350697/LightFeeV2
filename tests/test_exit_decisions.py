"""Task 4: Exit decision engine contract tests.

Rust references:
- src/engine/exit.rs: standard_close_reason (line 5197)
- src/engine/exit.rs: stale_market_safe_close_reason (line 5290)
- src/engine/exit.rs: position_delta_monitor (line 5245)
- src/engine/exit.rs: remaining_close_delay_active (line 5330)
- src/engine/exit.rs: aligned_settlement_delay_elapsed (line 5348)
- src/engine/risk.rs: update_position_funding_capture_state (line 2070)
"""

from __future__ import annotations

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import Side, Venue
from lightfee.engine.exit import ExitReason
from lightfee.engine.exit_decision import (
    aligned_settlement_delay_elapsed,
    force_close_due,
    is_delever_close_reason,
    is_protection_close_reason,
    normal_close_reason_uses_passive_maker_taker,
    position_delta_monitor,
    remaining_close_delay_active,
    stale_market_safe_close_reason,
    standard_close_reason,
    update_position_funding_capture_state,
)
from lightfee.engine.state import OpenPosition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_position(**overrides) -> OpenPosition:
    defaults = dict(
        position_id="p001",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        long_quantity=0.01,
        short_quantity=0.01,
        long_entry_price=50000.0,
        short_entry_price=50000.0,
        opened_at_ms=1000000,
        current_net_quote=0.0,
        peak_net_quote=0.0,
        funding_timestamp_ms=1000000,
        opportunity_type="aligned",
    )
    defaults.update(overrides)
    return OpenPosition(**defaults)


def _config(**overrides) -> StrategyConfig:
    return StrategyConfig(**overrides)


# ---------------------------------------------------------------------------
# standard_close_reason
# ---------------------------------------------------------------------------


class TestStandardCloseReason:
    def test_hard_stop_when_net_below_threshold(self):
        """V1: current_net_quote <= -net_stop_loss → hard_stop."""
        pos = _make_position(current_net_quote=-25.0)
        cfg = _config(net_stop_loss_quote=20.0)
        assert standard_close_reason(pos, cfg, 2000000) == ExitReason.NET_STOP_LOSS

    def test_hard_stop_exact_threshold(self):
        """V1: <= check triggers at exactly -net_stop_loss."""
        pos = _make_position(current_net_quote=-20.0)
        cfg = _config(net_stop_loss_quote=20.0)
        assert standard_close_reason(pos, cfg, 2000000) == ExitReason.NET_STOP_LOSS

    def test_no_hard_stop_when_net_above_threshold(self):
        """V1: net just above threshold → no hard_stop."""
        pos = _make_position(current_net_quote=-19.99)
        cfg = _config(net_stop_loss_quote=20.0)
        result = standard_close_reason(pos, cfg, 2000000)
        # Should NOT be hard_stop; could be something else or None
        assert result != ExitReason.NET_STOP_LOSS

    def test_delay_active_returns_none(self):
        """V1: remaining_close_delay_active → None (hard_stop fires first though)."""
        # V1 priority: hard_stop is checked BEFORE delay. Delay only blocks
        # trailing/funding checks, not hard_stop. So adjust test to not
        # trigger hard_stop.
        pos = _make_position(
            funding_timestamp_ms=2000000,
            current_net_quote=-5.0,  # not below net_stop_loss (20.0)
        )
        cfg = _config(net_stop_loss_quote=20.0, settlement_remainder_close_delay_secs=300)
        # now_ms is inside delay window → None (no other reason matches)
        result = standard_close_reason(pos, cfg, 2100000)
        assert result is None

    def test_aligned_delay_elapsed_triggers_funding_capture(self):
        """V1: aligned_settlement_delay_elapsed → funding_capture."""
        pos = _make_position(
            funding_timestamp_ms=1000000,
            current_net_quote=5.0,
            matched_quantity=0.01,
        )
        cfg = _config(settlement_remainder_close_delay_secs=300)
        # now_ms is past delay
        result = standard_close_reason(pos, cfg, 2000000)
        assert result == ExitReason.FUNDING_CAPTURE

    def test_zero_funding_timestamp_does_not_trigger_funding_capture(self):
        """Missing funding time is not evidence that settlement already passed."""
        pos = _make_position(
            funding_timestamp_ms=0,
            current_net_quote=5.0,
            matched_quantity=0.01,
        )
        cfg = _config(
            settlement_remainder_close_delay_secs=300,
            profit_take_quote=100.0,
        )
        now_ms = 1780163920476

        assert aligned_settlement_delay_elapsed(pos, now_ms, 300_000) is False
        assert standard_close_reason(pos, cfg, now_ms) is None

    def test_trailing_exit_armed_and_drawn_down(self):
        """V1: peak >= profit_take AND drawdown >= trailing → trailing_exit."""
        # Use staggered to bypass aligned settlement delay check
        pos = _make_position(
            opportunity_type="staggered",
            peak_net_quote=50.0,
            current_net_quote=25.0,
        )
        cfg = _config(profit_take_quote=30.0, trailing_drawdown_quote=20.0)
        result = standard_close_reason(pos, cfg, 2000000)
        assert result == ExitReason.TRAILING_EXIT

    def test_trailing_exit_not_armed_peak_below_profit(self):
        """V1: peak below profit_take → no trailing exit."""
        pos = _make_position(
            opportunity_type="staggered",
            peak_net_quote=25.0,
            current_net_quote=0.0,
        )
        cfg = _config(profit_take_quote=30.0, trailing_drawdown_quote=20.0)
        result = standard_close_reason(pos, cfg, 2000000)
        assert result != ExitReason.TRAILING_EXIT

    def test_trailing_exit_not_triggered_drawdown_too_small(self):
        """V1: peak >= profit but drawdown too small → no trailing exit."""
        pos = _make_position(
            opportunity_type="staggered",
            peak_net_quote=50.0,
            current_net_quote=35.0,  # drawdown = 15, threshold = 20
        )
        cfg = _config(profit_take_quote=30.0, trailing_drawdown_quote=20.0)
        result = standard_close_reason(pos, cfg, 2000000)
        assert result != ExitReason.TRAILING_EXIT

    def test_first_stage_capture_staggered(self):
        """V1: Staggered + funding_captured + exit_after_first_stage → first_stage."""
        pos = _make_position(
            opportunity_type="staggered",
            funding_captured=True,
            exit_after_first_stage=True,
        )
        cfg = _config()
        result = standard_close_reason(pos, cfg, 2000000)
        assert result == ExitReason.FIRST_STAGE_CAPTURE

    def test_second_stage_capture_staggered(self):
        """V1: Staggered + second_stage_enabled + captured → second_stage."""
        pos = _make_position(
            opportunity_type="staggered",
            funding_captured=True,
            second_stage_enabled_at_entry=True,
            second_stage_funding_captured=True,
        )
        cfg = _config()
        result = standard_close_reason(pos, cfg, 2000000)
        assert result == ExitReason.SECOND_STAGE_CAPTURE

    def test_general_funding_capture_net_positive(self):
        """V1: funding_captured + net >= 0 → funding_capture (aligned)."""
        pos = _make_position(
            funding_captured=True,
            current_net_quote=5.0,
            # Must not match earlier checks (not in delay, not trailing, not staggered)
            peak_net_quote=0.0,
        )
        cfg = _config(profit_take_quote=100.0)  # peak won't reach
        result = standard_close_reason(pos, cfg, 2000000)
        assert result == ExitReason.FUNDING_CAPTURE

    def test_general_funding_skipped_when_second_stage_pending(self):
        """V1: Staggered with stage2 enabled but not yet captured → skip general funding."""
        pos = _make_position(
            opportunity_type="staggered",
            funding_captured=True,
            second_stage_enabled_at_entry=True,
            second_stage_funding_captured=False,
            current_net_quote=5.0,
        )
        cfg = _config()
        result = standard_close_reason(pos, cfg, 2000000)
        # Should NOT be general funding_capture because stage2 is pending
        assert result is None or result == ExitReason.FIRST_STAGE_CAPTURE

    def test_no_action_returns_none(self):
        """V1: no reason matched → None (hold position)."""
        pos = _make_position(
            opportunity_type="staggered",
            current_net_quote=5.0,
            peak_net_quote=5.0,
        )
        cfg = _config(profit_take_quote=100.0, net_stop_loss_quote=20.0)
        result = standard_close_reason(pos, cfg, 2000000)
        assert result is None


# ---------------------------------------------------------------------------
# stale_market_safe_close_reason
# ---------------------------------------------------------------------------


class TestStaleMarketSafeCloseReason:
    def test_ignores_hard_stop(self):
        """V1: safe close ignores hard_stop even when net deeply negative."""
        pos = _make_position(current_net_quote=-50.0)
        cfg = _config(net_stop_loss_quote=20.0)
        result = stale_market_safe_close_reason(pos, cfg, 2000000)
        assert result != ExitReason.NET_STOP_LOSS

    def test_ignores_trailing_exit(self):
        """V1: safe close ignores trailing exit signals."""
        pos = _make_position(
            peak_net_quote=50.0,
            current_net_quote=25.0,
        )
        cfg = _config(profit_take_quote=30.0, trailing_drawdown_quote=20.0)
        result = stale_market_safe_close_reason(pos, cfg, 2000000)
        assert result != ExitReason.TRAILING_EXIT

    def test_still_captures_funding(self):
        """V1: safe close still fires funding_capture when appropriate."""
        pos = _make_position(
            funding_captured=True,
            current_net_quote=10.0,
        )
        cfg = _config(profit_take_quote=100.0)
        result = stale_market_safe_close_reason(pos, cfg, 2000000)
        assert result == ExitReason.FUNDING_CAPTURE

    def test_still_captures_first_stage(self):
        """V1: safe close still fires first_stage_capture."""
        pos = _make_position(
            opportunity_type="staggered",
            funding_captured=True,
            exit_after_first_stage=True,
        )
        cfg = _config()
        result = stale_market_safe_close_reason(pos, cfg, 2000000)
        assert result == ExitReason.FIRST_STAGE_CAPTURE

    def test_still_captures_second_stage(self):
        """V1: safe close still fires second_stage_capture."""
        pos = _make_position(
            opportunity_type="staggered",
            funding_captured=True,
            second_stage_enabled_at_entry=True,
            second_stage_funding_captured=True,
        )
        cfg = _config()
        result = stale_market_safe_close_reason(pos, cfg, 2000000)
        assert result == ExitReason.SECOND_STAGE_CAPTURE


# ---------------------------------------------------------------------------
# position_delta_monitor
# ---------------------------------------------------------------------------


class TestPositionDeltaMonitor:
    def test_no_trigger_when_delta_small(self):
        pos = _make_position(matched_quantity=0.01)
        cfg = _config(mark_price_delta_hard_stop_quote=20.0)
        # delta = abs(50100 - 50000) * 0.01 = 1.0 < warning threshold (10.0)
        result = position_delta_monitor(pos, 50000.0, 50100.0, cfg)
        assert result is None

    def test_warning_triggered_at_half_threshold(self):
        pos = _make_position(matched_quantity=0.01)
        cfg = _config(mark_price_delta_hard_stop_quote=20.0)
        # delta = abs(51000 - 50000) * 0.01 = 10.0 = warning threshold (half of 20)
        result = position_delta_monitor(pos, 50000.0, 51000.0, cfg)
        assert result is not None
        assert result["warning_triggered"] is True

    def test_hard_stop_triggered(self):
        pos = _make_position(matched_quantity=0.01)
        cfg = _config(mark_price_delta_hard_stop_quote=20.0)
        # delta = abs(52000 - 50000) * 0.01 = 20.0 >= hard_stop
        result = position_delta_monitor(pos, 50000.0, 52000.0, cfg)
        assert result is not None
        assert result["hard_stop_triggered"] is True
        assert result["warning_triggered"] is True  # hard_stop implies warning too


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


class TestRemainingCloseDelayActive:
    def test_aligned_within_delay(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        delay_ms = 300_000
        assert remaining_close_delay_active(pos, 1100000, delay_ms) is True

    def test_aligned_past_delay(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        delay_ms = 300_000
        assert remaining_close_delay_active(pos, 2000000, delay_ms) is False

    def test_zero_funding_timestamp_not_active(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=0,
        )
        delay_ms = 300_000
        assert remaining_close_delay_active(pos, 1000, delay_ms) is False

    def test_staggered_not_affected_by_aligned_delay(self):
        """V1: delay only applies to aligned positions."""
        pos = _make_position(
            opportunity_type="staggered",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        delay_ms = 300_000
        assert remaining_close_delay_active(pos, 1100000, delay_ms) is False

    def test_post_half_close_delay(self):
        pos = _make_position(
            settlement_half_closed_at_ms=1000000,
            matched_quantity=0.005,  # partial
        )
        delay_ms = 300_000
        assert remaining_close_delay_active(pos, 1100000, delay_ms) is True

    def test_post_half_close_past_delay(self):
        pos = _make_position(
            settlement_half_closed_at_ms=1000000,
        )
        delay_ms = 300_000
        assert remaining_close_delay_active(pos, 2000000, delay_ms) is False


class TestAlignedSettlementDelayElapsed:
    def test_aligned_elapsed(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        delay_ms = 300_000
        assert aligned_settlement_delay_elapsed(pos, 2000000, delay_ms) is True

    def test_aligned_not_elapsed(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        delay_ms = 300_000
        assert aligned_settlement_delay_elapsed(pos, 1100000, delay_ms) is False

    def test_staggered_never_elapsed(self):
        pos = _make_position(
            opportunity_type="staggered",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        delay_ms = 300_000
        assert aligned_settlement_delay_elapsed(pos, 2000000, delay_ms) is False

    def test_zero_quantity_not_elapsed(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.0,
            long_quantity=0.0,
            short_quantity=0.0,
            funding_timestamp_ms=1000000,
        )
        delay_ms = 300_000
        assert aligned_settlement_delay_elapsed(pos, 2000000, delay_ms) is False

    def test_zero_funding_timestamp_not_elapsed(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=0,
        )
        delay_ms = 300_000
        assert aligned_settlement_delay_elapsed(pos, 2000000, delay_ms) is False


class TestForceCloseDue:
    def test_aligned_past_force_deadline(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        cfg = _config(settlement_force_close_delay_secs=1200)
        # now = 1000000 + 1200*1000 + 1 = past deadline
        assert force_close_due(pos, cfg, 2200001) is True

    def test_aligned_before_force_deadline(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        cfg = _config(settlement_force_close_delay_secs=1200)
        assert force_close_due(pos, cfg, 2000000) is False

    def test_staggered_no_force_close(self):
        pos = _make_position(
            opportunity_type="staggered",
            matched_quantity=0.01,
            funding_timestamp_ms=1000000,
        )
        cfg = _config(settlement_force_close_delay_secs=1200)
        assert force_close_due(pos, cfg, 2200001) is False

    def test_zero_funding_timestamp_not_due(self):
        pos = _make_position(
            opportunity_type="aligned",
            matched_quantity=0.01,
            funding_timestamp_ms=0,
        )
        cfg = _config(settlement_force_close_delay_secs=1200)
        assert force_close_due(pos, cfg, 2200001) is False


# ---------------------------------------------------------------------------
# update_position_funding_capture_state
# ---------------------------------------------------------------------------


class TestUpdateFundingCaptureState:
    def test_stage1_capture_after_hold(self):
        pos = _make_position(
            funding_timestamp_ms=1000000,
            funding_captured=False,
            captured_funding_quote=15.0,  # pre-computed by entry
            current_net_quote=15.0,
            peak_net_quote=0.0,
        )
        # hold = 30s, now = 1000000 + 30000 + 1 = past hold deadline
        update_position_funding_capture_state(pos, 1030001, post_funding_hold_ms=30000)
        assert pos.funding_captured is True
        # peak should update to current_net_quote (15.0) since captured_funding > 0
        assert pos.peak_net_quote == max(0.0, pos.current_net_quote)

    def test_stage1_not_captured_before_hold(self):
        pos = _make_position(
            funding_timestamp_ms=1000000,
            funding_captured=False,
        )
        update_position_funding_capture_state(pos, 1001000, post_funding_hold_ms=30000)
        assert pos.funding_captured is False

    def test_stage1_not_captured_without_funding_timestamp(self):
        pos = _make_position(
            funding_timestamp_ms=0,
            funding_captured=False,
        )
        update_position_funding_capture_state(pos, 30001, post_funding_hold_ms=30000)
        assert pos.funding_captured is False

    def test_stage1_already_captured_noop(self):
        pos = _make_position(
            funding_timestamp_ms=1000000,
            funding_captured=True,
            captured_funding_quote=15.0,
            second_stage_enabled_at_entry=False,
            second_stage_funding_captured=False,
        )
        update_position_funding_capture_state(pos, 2000000, post_funding_hold_ms=30000)
        assert pos.second_stage_funding_captured is False  # stage2 not enabled

    def test_stage2_capture_after_second_hold(self):
        pos = _make_position(
            funding_timestamp_ms=1000000,
            funding_captured=True,
            second_stage_enabled_at_entry=True,
            second_stage_funding_captured=False,
            second_funding_timestamp_ms=2000000,
            second_stage_funding_quote=8.0,
            current_net_quote=25.0,
            peak_net_quote=15.0,
        )
        update_position_funding_capture_state(pos, 2030000, post_funding_hold_ms=30000)
        assert pos.second_stage_funding_captured is True
        assert pos.peak_net_quote == max(15.0, 25.0)

    def test_stage2_not_captured_stage1_not_done(self):
        pos = _make_position(
            funding_timestamp_ms=1000000,
            funding_captured=False,
            second_stage_enabled_at_entry=True,
            second_funding_timestamp_ms=2000000,
        )
        # now is before stage1 hold (1030000), so stage1 is NOT captured
        update_position_funding_capture_state(pos, 1010000, post_funding_hold_ms=30000)
        assert pos.funding_captured is False
        assert pos.second_stage_funding_captured is False  # stage1 not captured yet

    def test_stage2_rejected_before_second_timestamp(self):
        pos = _make_position(
            funding_timestamp_ms=1000000,
            funding_captured=True,
            second_stage_enabled_at_entry=True,
            second_funding_timestamp_ms=2000000,
            second_stage_funding_captured=False,
        )
        # now is past stage1 hold but before second timestamp
        update_position_funding_capture_state(pos, 1500000, post_funding_hold_ms=30000)
        assert pos.second_stage_funding_captured is False


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------


class TestReasonClassification:
    def test_is_delever_close_reason(self):
        assert is_delever_close_reason("risk_delever_0.2_partial") is True
        assert is_delever_close_reason("risk_delever") is True
        assert is_delever_close_reason("trailing_exit") is False
        assert is_delever_close_reason("hard_stop") is False

    def test_is_protection_close_reason(self):
        assert is_protection_close_reason("protection_full") is True
        assert is_protection_close_reason("risk_protection_death") is True
        assert is_protection_close_reason("single_side_protection") is True
        assert is_protection_close_reason("risk_delever") is False

    def test_normal_close_uses_passive_maker_taker(self):
        assert normal_close_reason_uses_passive_maker_taker("trailing_exit") is True
        assert normal_close_reason_uses_passive_maker_taker("first_stage_capture") is True
        assert normal_close_reason_uses_passive_maker_taker("second_stage_capture") is True
        assert normal_close_reason_uses_passive_maker_taker("funding_capture") is True
        assert normal_close_reason_uses_passive_maker_taker("settlement_half_close") is True
        assert normal_close_reason_uses_passive_maker_taker("settlement_force_close") is True
        assert normal_close_reason_uses_passive_maker_taker("hard_stop") is False
        assert normal_close_reason_uses_passive_maker_taker("risk_delever_0.2") is False
        assert normal_close_reason_uses_passive_maker_taker(None) is False
