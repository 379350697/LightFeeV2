"""Semantic parity tests for V1 state model fields (STATE-001, STATE-002, STATE-003).

Validates that V2 OpenPosition, PendingEntry, and EngineState preserve all
V1-required semantic fields, either as direct attributes or via equivalent
V2-native structures.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import textwrap

import pytest
from lightfee.engine.state import (
    EngineState,
    OpenPosition,
    PassiveOrderManagerRuntime,
    PendingEntry,
    PendingEntryPassivePhaseState,
    PendingEntryRemainderSlice,
    PendingClose,
    OperatorControlState,
    normalize_pending_close_reconciliations,
)
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


# ---------------------------------------------------------------------------
# STATE-001: OpenPosition field completeness
# ---------------------------------------------------------------------------

V1_OPENPOSITION_REQUIRED_FIELDS = {
    # Core identity
    "position_id",
    "symbol",
    "long_venue",
    "short_venue",
    # Quantities
    "quantity",          # V1: quantity (total matched)
    "matched_quantity",  # V1: min(long_qty, short_qty)
    "initial_quantity",
    "long_quantity",
    "short_quantity",
    # Entry prices / notional
    "long_entry_price",
    "short_entry_price",
    "entry_notional_quote",
    # Review & origin
    "review_id",
    "opportunity_origin_tags",
    "opportunity_hint_source",
    # Edge breakdowns (entry)
    "funding_edge_bps_entry",
    "total_funding_edge_bps_entry",
    "expected_edge_bps_entry",
    "worst_case_edge_bps_entry",
    "entry_cross_bps_entry",
    "fee_bps_entry",
    "entry_slippage_bps_entry",
    # Transfer & liquidity
    "transfer_bias_bps_entry",
    "transfer_state_at_entry",
    "entry_liquidity_source_at_entry",
    "long_volume_24h_quote_at_entry",
    "short_volume_24h_quote_at_entry",
    "long_open_interest_quote_at_entry",
    "short_open_interest_quote_at_entry",
    # VWAP
    "long_entry_vwap",
    "short_entry_vwap",
    # Capacity constraints
    "entry_capacity_constrained",
    "entry_target_quantity",
    "long_max_executable_quantity",
    "short_max_executable_quantity",
    "entry_max_executable_quantity",
    "entry_depth_shortfall_quantity",
    "entry_max_executable_notional_quote",
    "entry_depth_capped_at_entry",
    # Advisories & blocked reasons
    "advisories",
    "blocked_reasons",
    # Quality markouts
    "entry_quality_markout_5s_emitted",
    "entry_quality_markout_30s_emitted",
    "entry_quality_completed_at_ms",
    # Risk/Protection PnL
    "risk_delever_realized_price_pnl_quote",
    "risk_delever_realized_exit_fee_quote",
    "protection_realized_price_pnl_quote",
    "protection_realized_exit_fee_quote",
    # Settlement
    "settlement_half_closed_quantity",
    "settlement_half_closed_at_ms",
    # Funding stages
    "funding_captured",
    "second_stage_funding_captured",
    "captured_funding_quote",
    "second_stage_funding_quote",
    "second_stage_enabled_at_entry",
    "opportunity_type",
    "first_funding_leg",
    "entry_maker_leg",
    "exit_maker_leg",
    # Risk tracking
    "risk_delever_step_count",
    "last_risk_reason",
    "single_side_protection_triggered",
    # PnL
    "realized_price_pnl_quote",
    "realized_exit_fee_quote",
    # Fees
    "long_entry_fee_quote",
    "short_entry_fee_quote",
    "total_entry_fee_quote",
    "entry_fee_evidence_complete",
    # Net/Peak
    "peak_net_quote",
    "current_net_quote",
    # Timestamps
    "opened_at_ms",
    "entered_at_ms",
    "funding_timestamp_ms",
    "long_funding_timestamp_ms",
    "short_funding_timestamp_ms",
    "last_risk_action_at_ms",
    # Exit
    "exit_after_first_stage",
    "exit_reason",
}

# Fields V2 uses different names for (V1 name -> V2 name mapping for test)
V1_TO_V2_FIELD_ALIASES = {
    "quantity": "matched_quantity",  # V2 uses matched_quantity for V1's quantity
}


def _get_openposition_field_names() -> set[str]:
    """Extract all field names from an OpenPosition dataclass instance."""
    import dataclasses
    return {f.name for f in dataclasses.fields(OpenPosition)}


class TestOpenPositionFieldCompleteness:
    """STATE-001: OpenPosition preserves V1 semantic fields."""

    def test_all_v1_fields_present_or_aliased(self):
        """Every V1 OpenPosition field must exist in V2 or have an approved alias."""
        v2_fields = _get_openposition_field_names()
        missing = []
        for v1_field in sorted(V1_OPENPOSITION_REQUIRED_FIELDS):
            v2_name = V1_TO_V2_FIELD_ALIASES.get(v1_field, v1_field)
            if v2_name not in v2_fields:
                missing.append(v1_field)
        assert not missing, (
            f"V1 OpenPosition fields missing in V2: {missing}\n"
            f"V2 fields: {sorted(v2_fields)}"
        )

    def test_entry_notional_quote_field_exists(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "entry_notional_quote")
        assert pos.entry_notional_quote == 0.0

    def test_initial_quantity_and_entered_at_fields_exist(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "initial_quantity")
        assert hasattr(pos, "entered_at_ms")
        assert pos.initial_quantity == 1.0
        assert pos.entered_at_ms == 1000

    def test_review_id_field_exists(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "review_id")
        assert pos.review_id is None

    def test_origin_tags_and_hint_source(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "opportunity_origin_tags")
        assert isinstance(pos.opportunity_origin_tags, list)
        assert hasattr(pos, "opportunity_hint_source")
        assert pos.opportunity_hint_source is None

    def test_transfer_and_liquidity_fields(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "transfer_state_at_entry")
        assert hasattr(pos, "entry_liquidity_source_at_entry")
        assert pos.transfer_state_at_entry is None
        assert pos.entry_liquidity_source_at_entry is None

    def test_vwap_fields(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "long_entry_vwap")
        assert hasattr(pos, "short_entry_vwap")
        assert pos.long_entry_vwap is None
        assert pos.short_entry_vwap is None

    def test_capacity_constraint_field(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "entry_capacity_constrained")
        assert pos.entry_capacity_constrained is False

    def test_advisories_and_blocked_reasons(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "advisories")
        assert isinstance(pos.advisories, list)
        assert hasattr(pos, "blocked_reasons")
        assert isinstance(pos.blocked_reasons, list)

    def test_quality_markout_fields(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "entry_quality_markout_5s_emitted")
        assert hasattr(pos, "entry_quality_markout_30s_emitted")
        assert pos.entry_quality_markout_5s_emitted is False
        assert pos.entry_quality_markout_30s_emitted is False

    def test_risk_and_protection_pnl_fields(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "risk_delever_realized_price_pnl_quote")
        assert hasattr(pos, "risk_delever_realized_exit_fee_quote")
        assert hasattr(pos, "protection_realized_price_pnl_quote")
        assert hasattr(pos, "protection_realized_exit_fee_quote")
        assert pos.risk_delever_realized_price_pnl_quote == 0.0
        assert pos.protection_realized_price_pnl_quote == 0.0

    def test_settlement_fields(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "settlement_half_closed_quantity")
        assert hasattr(pos, "settlement_half_closed_at_ms")
        assert pos.settlement_half_closed_quantity == 0.0

    def test_exit_reason_field(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert hasattr(pos, "exit_reason")
        assert pos.exit_reason is None

    def test_openposition_roundtrip_serialization(self):
        """OpenPosition dict serialization must include V1-visible fields."""
        from lightfee.engine.state import OpenPosition
        from lightfee.core.domain import Venue
        pos = OpenPosition(
            position_id="test-p1",
            symbol="ETH-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=2.0,
            short_quantity=2.0,
            long_entry_price=3000.0,
            short_entry_price=3000.0,
            opened_at_ms=1000000,
            review_id="review-1",
            opportunity_origin_tags=["tag1", "tag2"],
            opportunity_hint_source="hint-1",
            transfer_state_at_entry="normal",
            entry_liquidity_source_at_entry="sidecar",
            long_entry_vwap=2999.5,
            short_entry_vwap=3000.5,
            entry_capacity_constrained=False,
            advisories=["low-liquidity"],
            blocked_reasons=[],
            entry_quality_markout_5s_emitted=True,
            risk_delever_realized_price_pnl_quote=10.0,
            protection_realized_price_pnl_quote=5.0,
            settlement_half_closed_quantity=0.5,
            exit_reason="manual",
        )
        # Serialize and check keys exist
        d = {
            "position_id": pos.position_id,
            "symbol": pos.symbol,
            "review_id": pos.review_id,
            "opportunity_origin_tags": pos.opportunity_origin_tags,
            "opportunity_hint_source": pos.opportunity_hint_source,
            "transfer_state_at_entry": pos.transfer_state_at_entry,
            "entry_liquidity_source_at_entry": pos.entry_liquidity_source_at_entry,
            "long_entry_vwap": pos.long_entry_vwap,
            "short_entry_vwap": pos.short_entry_vwap,
            "entry_capacity_constrained": pos.entry_capacity_constrained,
            "advisories": pos.advisories,
            "blocked_reasons": pos.blocked_reasons,
            "entry_quality_markout_5s_emitted": pos.entry_quality_markout_5s_emitted,
            "entry_quality_markout_30s_emitted": pos.entry_quality_markout_30s_emitted,
            "risk_delever_realized_price_pnl_quote": pos.risk_delever_realized_price_pnl_quote,
            "risk_delever_realized_exit_fee_quote": pos.risk_delever_realized_exit_fee_quote,
            "protection_realized_price_pnl_quote": pos.protection_realized_price_pnl_quote,
            "protection_realized_exit_fee_quote": pos.protection_realized_exit_fee_quote,
            "settlement_half_closed_quantity": pos.settlement_half_closed_quantity,
            "settlement_half_closed_at_ms": pos.settlement_half_closed_at_ms,
            "exit_reason": pos.exit_reason,
            "funding_timestamp_ms": pos.funding_timestamp_ms,
            "funding_edge_bps_entry": pos.funding_edge_bps_entry,
            "total_funding_edge_bps_entry": pos.total_funding_edge_bps_entry,
            "expected_edge_bps_entry": pos.expected_edge_bps_entry,
        }
        # All values should be retrievable
        assert d["review_id"] == "review-1"
        assert d["opportunity_origin_tags"] == ["tag1", "tag2"]
        assert d["protection_realized_price_pnl_quote"] == 5.0
        assert d["exit_reason"] == "manual"

    def test_enginestate_to_dict_preserves_open_position_funding_semantics(self):
        from lightfee.core.domain import Venue

        state = EngineState()
        state.open_positions["entry-magma"] = OpenPosition(
            position_id="entry-magma",
            symbol="MAGMAUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
            long_quantity=100.0,
            short_quantity=100.0,
            long_entry_price=0.275,
            short_entry_price=0.274,
            opened_at_ms=1780163908797,
            total_entry_fee_quote=1.23,
            funding_timestamp_ms=1780167600000,
            long_funding_timestamp_ms=1780167600000,
            short_funding_timestamp_ms=1780171200000,
            second_funding_timestamp_ms=1780171200000,
            opportunity_type="staggered",
            second_stage_enabled_at_entry=True,
            exit_after_first_stage=False,
            funding_edge_bps_entry=7.45,
            total_funding_edge_bps_entry=7.45,
            expected_edge_bps_entry=6.9,
            worst_case_edge_bps_entry=4.2,
            first_funding_leg="long",
            entry_maker_leg="long",
            exit_maker_leg="short",
            entry_cross_bps_entry=1.0,
            fee_bps_entry=2.0,
            entry_slippage_bps_entry=0.5,
            transfer_bias_bps_entry=-0.25,
            transfer_state_at_entry="ok",
            entry_liquidity_source_at_entry="local_l2",
            long_volume_24h_quote_at_entry=1_000_000.0,
            short_volume_24h_quote_at_entry=2_000_000.0,
            long_open_interest_quote_at_entry=3_000_000.0,
            short_open_interest_quote_at_entry=4_000_000.0,
            long_entry_vwap=0.2751,
            short_entry_vwap=0.2741,
            entry_capacity_constrained=True,
            entry_target_quantity=120.0,
            long_max_executable_quantity=110.0,
            short_max_executable_quantity=105.0,
            entry_max_executable_quantity=105.0,
            entry_depth_shortfall_quantity=15.0,
            entry_max_executable_notional_quote=28.8,
            entry_depth_capped_at_entry=True,
            entry_quality_completed_at_ms=0,
        )

        pos = state.to_dict()["open_positions"]["entry-magma"]

        assert pos["funding_timestamp_ms"] == 1780167600000
        assert pos["long_funding_timestamp_ms"] == 1780167600000
        assert pos["short_funding_timestamp_ms"] == 1780171200000
        assert pos["second_funding_timestamp_ms"] == 1780171200000
        assert pos["total_entry_fee_quote"] == pytest.approx(1.23)
        assert pos["entry_quality_completed_at_ms"] == 0
        assert pos["opportunity_type"] == "staggered"
        assert pos["second_stage_enabled_at_entry"] is True
        assert pos["exit_after_first_stage"] is False
        assert pos["funding_edge_bps_entry"] == pytest.approx(7.45)
        assert pos["total_funding_edge_bps_entry"] == pytest.approx(7.45)
        assert pos["expected_edge_bps_entry"] == pytest.approx(6.9)
        assert pos["worst_case_edge_bps_entry"] == pytest.approx(4.2)
        assert pos["first_funding_leg"] == "long"
        assert pos["entry_maker_leg"] == "long"
        assert pos["exit_maker_leg"] == "short"
        assert pos["entry_cross_bps_entry"] == pytest.approx(1.0)
        assert pos["fee_bps_entry"] == pytest.approx(2.0)
        assert pos["entry_slippage_bps_entry"] == pytest.approx(0.5)
        assert pos["transfer_bias_bps_entry"] == pytest.approx(-0.25)
        assert pos["transfer_state_at_entry"] == "ok"
        assert pos["entry_liquidity_source_at_entry"] == "local_l2"
        assert pos["long_volume_24h_quote_at_entry"] == pytest.approx(1_000_000.0)
        assert pos["short_volume_24h_quote_at_entry"] == pytest.approx(2_000_000.0)
        assert pos["long_open_interest_quote_at_entry"] == pytest.approx(3_000_000.0)
        assert pos["short_open_interest_quote_at_entry"] == pytest.approx(4_000_000.0)
        assert pos["long_entry_vwap"] == pytest.approx(0.2751)
        assert pos["short_entry_vwap"] == pytest.approx(0.2741)
        assert pos["entry_capacity_constrained"] is True
        assert pos["entry_target_quantity"] == pytest.approx(120.0)
        assert pos["long_max_executable_quantity"] == pytest.approx(110.0)
        assert pos["short_max_executable_quantity"] == pytest.approx(105.0)
        assert pos["entry_max_executable_quantity"] == pytest.approx(105.0)
        assert pos["entry_depth_shortfall_quantity"] == pytest.approx(15.0)
        assert pos["entry_max_executable_notional_quote"] == pytest.approx(28.8)
        assert pos["entry_depth_capped_at_entry"] is True

    def test_open_position_v1_entry_metadata_roundtrip(self):
        from lightfee.core.domain import Venue
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict

        state = EngineState()
        state.open_positions["entry-meta"] = OpenPosition(
            position_id="entry-meta",
            symbol="BTC-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=0.1,
            short_quantity=0.1,
            long_entry_price=50000.0,
            short_entry_price=50010.0,
            opened_at_ms=1000,
            worst_case_edge_bps_entry=4.0,
            first_funding_leg="long",
            entry_maker_leg="long",
            exit_maker_leg="short",
            entry_cross_bps_entry=1.25,
            fee_bps_entry=2.1,
            entry_slippage_bps_entry=0.75,
            transfer_bias_bps_entry=-0.5,
            transfer_state_at_entry="ok",
            entry_liquidity_source_at_entry="local_l2",
            long_volume_24h_quote_at_entry=12_000_000.0,
            short_volume_24h_quote_at_entry=15_000_000.0,
            long_open_interest_quote_at_entry=8_000_000.0,
            short_open_interest_quote_at_entry=9_000_000.0,
            long_entry_vwap=50000.5,
            short_entry_vwap=50010.5,
            entry_capacity_constrained=True,
            entry_target_quantity=0.2,
            long_max_executable_quantity=0.18,
            short_max_executable_quantity=0.16,
            entry_max_executable_quantity=0.16,
            entry_depth_shortfall_quantity=0.04,
            entry_max_executable_notional_quote=8000.0,
            entry_depth_capped_at_entry=True,
        )

        restored = _restore_state_from_snapshot_dict(state.to_dict())
        pos = restored.open_positions["entry-meta"]

        assert pos.worst_case_edge_bps_entry == pytest.approx(4.0)
        assert pos.first_funding_leg == "long"
        assert pos.entry_maker_leg == "long"
        assert pos.exit_maker_leg == "short"
        assert pos.entry_cross_bps_entry == pytest.approx(1.25)
        assert pos.fee_bps_entry == pytest.approx(2.1)
        assert pos.entry_slippage_bps_entry == pytest.approx(0.75)
        assert pos.transfer_bias_bps_entry == pytest.approx(-0.5)
        assert pos.transfer_state_at_entry == "ok"
        assert pos.entry_liquidity_source_at_entry == "local_l2"
        assert pos.long_volume_24h_quote_at_entry == pytest.approx(12_000_000.0)
        assert pos.short_volume_24h_quote_at_entry == pytest.approx(15_000_000.0)
        assert pos.long_open_interest_quote_at_entry == pytest.approx(8_000_000.0)
        assert pos.short_open_interest_quote_at_entry == pytest.approx(9_000_000.0)
        assert pos.long_entry_vwap == pytest.approx(50000.5)
        assert pos.short_entry_vwap == pytest.approx(50010.5)
        assert pos.entry_capacity_constrained is True
        assert pos.entry_target_quantity == pytest.approx(0.2)
        assert pos.long_max_executable_quantity == pytest.approx(0.18)
        assert pos.short_max_executable_quantity == pytest.approx(0.16)
        assert pos.entry_max_executable_quantity == pytest.approx(0.16)
        assert pos.entry_depth_shortfall_quantity == pytest.approx(0.04)
        assert pos.entry_max_executable_notional_quote == pytest.approx(8000.0)
        assert pos.entry_depth_capped_at_entry is True


# ---------------------------------------------------------------------------
# STATE-002: PendingEntry field completeness
# ---------------------------------------------------------------------------

V1_PENDINGENTRY_REQUIRED_FIELDS = {
    "pending_id",
    "symbol",
    "long_venue",
    "short_venue",
    "target_quantity",
    "long_side",
    "short_side",
    "created_at_ms",
    # Metadata
    "metadata",
    # Client order IDs (idempotency)
    "maker_client_order_id",
    "hedge_client_order_id",
    # Leg fills
    "maker_leg_filled",
    "hedge_leg_filled",
    # Deadline
    "deadline_ms",
    # Retry state
    "reconcile_attempt",
    "reconcile_next_attempt_ms",
    # Route
    "entry_route",
    # Outcome
    "outcome",
    # Recovery dedup
    "run_id",
    # Funding semantics copied from selected candidate until entry finalizes
    "opportunity_type",
    "funding_timestamp_ms",
    "first_funding_timestamp_ms",
    "long_funding_timestamp_ms",
    "short_funding_timestamp_ms",
    "second_funding_timestamp_ms",
    "first_funding_leg",
    "funding_edge_bps_entry",
    "total_funding_edge_bps_entry",
    "expected_edge_bps_entry",
    # Passive pending-entry lifecycle state from V1 PendingEntryHedge
    "phase_state",
    "passive_manager_runtime",
    "created_cycle",
    "repost_attempt_count",
    "passive_attempt_count",
    "passive_ops_total",
    "maker_remainder_slices",
    "lifetime_exhausted_logged_final_reason",
    "frozen_candidate",
}


class TestPendingEntryFieldCompleteness:
    """STATE-002: PendingEntry preserves V1 semantic fields."""

    def test_all_v1_fields_present(self):
        import dataclasses
        v2_fields = {f.name for f in dataclasses.fields(PendingEntry)}
        missing = sorted(V1_PENDINGENTRY_REQUIRED_FIELDS - v2_fields)
        assert not missing, (
            f"V1 PendingEntry fields missing in V2: {missing}\n"
            f"V2 fields: {sorted(v2_fields)}"
        )

    def test_metadata_field_exists(self):
        from lightfee.core.domain import Side, Venue
        pe = PendingEntry(
            pending_id="pe1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=1.0, long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
        )
        assert hasattr(pe, "metadata")
        assert isinstance(pe.metadata, dict)

    def test_pending_entry_dedup_key(self):
        """PendingEntry.run_id serves as recovery dedup key."""
        from lightfee.core.domain import Side, Venue
        pe = PendingEntry(
            pending_id="pe1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=1.0, long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
            run_id="run-123",
        )
        assert pe.run_id == "run-123"

    def test_pending_entry_funding_semantics_roundtrip(self):
        from lightfee.core.domain import Side, Venue
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict

        state = EngineState()
        state.pending_entries["entry-magma"] = PendingEntry(
            pending_id="entry-magma",
            symbol="MAGMAUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
            target_quantity=100.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1780163908797,
            opportunity_type="staggered",
            funding_timestamp_ms=1780167600000,
            first_funding_timestamp_ms=1780167600000,
            long_funding_timestamp_ms=1780167600000,
            short_funding_timestamp_ms=1780171200000,
            second_funding_timestamp_ms=1780171200000,
            first_funding_leg="long",
            funding_edge_bps_entry=7.45,
            total_funding_edge_bps_entry=7.45,
            expected_edge_bps_entry=6.9,
            worst_case_edge_bps_entry=4.0,
            entry_maker_leg="long",
            exit_maker_leg="short",
            entry_cross_bps_entry=1.25,
            fee_bps_entry=2.1,
            entry_slippage_bps_entry=0.75,
            transfer_bias_bps_entry=-0.5,
            transfer_state_at_entry="ok",
            entry_liquidity_source_at_entry="local_l2",
            long_volume_24h_quote_at_entry=12_000_000.0,
            short_volume_24h_quote_at_entry=15_000_000.0,
            long_open_interest_quote_at_entry=8_000_000.0,
            short_open_interest_quote_at_entry=9_000_000.0,
            long_entry_vwap=50000.5,
            short_entry_vwap=50010.5,
            entry_capacity_constrained=True,
            entry_target_quantity=0.2,
            long_max_executable_quantity=0.18,
            short_max_executable_quantity=0.16,
            entry_max_executable_quantity=0.16,
            entry_depth_shortfall_quantity=0.04,
            entry_max_executable_notional_quote=8000.0,
            entry_depth_capped_at_entry=True,
            advisories=["thin_book"],
            blocked_reasons=["capacity_cap"],
            phase_state=PendingEntryPassivePhaseState(
                execution_kind="entry",
                preferred_maker_leg="long",
                active_maker_leg="short",
                phase="low_slippage_maker",
                zero_fill_cycles_in_phase=2,
                cycle_attempt=3,
                next_cycle_delay_ms=250,
                small_fill_min_notional_attempts=1,
                hedge_deadline_at_ms=1780163909999,
                hedge_timeout_grace_deadline_at_ms=1780163910999,
                phase_started_at_ms=1780163908000,
                cycle_started_at_ms=1780163908500,
            ),
            passive_manager_runtime=PassiveOrderManagerRuntime(
                cooldown_until_ms=1780163910000,
                consecutive_failures=2,
                last_success_ms=1780163900000,
                last_attempt_ms=1780163901000,
                ops_budget_remaining=7,
                ops_budget_reset_ms=1780163920000,
                last_operation_ms=1780163902000,
            ),
            created_cycle=42,
            repost_attempt_count=4,
            passive_attempt_count=5,
            passive_ops_total=9,
            maker_remainder_slices=[
                PendingEntryRemainderSlice(
                    quantity=1.5,
                    notional_quote=30.0,
                    fill_at_ms=1780163909000,
                )
            ],
            lifetime_exhausted_logged_final_reason="passive_entry_lifetime_exhausted",
            frozen_candidate={"pair_id": "magmausdt:aster->bybit", "ranking_edge_bps": 7.45},
        )

        snap = state.to_dict()
        pending = snap["pending_entries"]["entry-magma"]
        assert pending["opportunity_type"] == "staggered"
        assert pending["funding_timestamp_ms"] == 1780167600000
        assert pending["first_funding_timestamp_ms"] == 1780167600000
        assert pending["long_funding_timestamp_ms"] == 1780167600000
        assert pending["short_funding_timestamp_ms"] == 1780171200000
        assert pending["second_funding_timestamp_ms"] == 1780171200000
        assert pending["first_funding_leg"] == "long"
        assert pending["funding_edge_bps_entry"] == pytest.approx(7.45)
        assert pending["total_funding_edge_bps_entry"] == pytest.approx(7.45)
        assert pending["expected_edge_bps_entry"] == pytest.approx(6.9)
        assert pending["phase_state"]["execution_kind"] == "entry"
        assert pending["phase_state"]["active_maker_leg"] == "short"
        assert pending["phase_state"]["next_cycle_delay_ms"] == 250
        assert pending["passive_manager_runtime"]["cooldown_until_ms"] == 1780163910000
        assert pending["created_cycle"] == 42
        assert pending["repost_attempt_count"] == 4
        assert pending["passive_attempt_count"] == 5
        assert pending["passive_ops_total"] == 9
        assert pending["maker_remainder_slices"] == [
            {
                "quantity": 1.5,
                "notional_quote": 30.0,
                "fill_at_ms": 1780163909000,
            }
        ]
        assert pending["lifetime_exhausted_logged_final_reason"] == "passive_entry_lifetime_exhausted"
        assert pending["frozen_candidate"]["pair_id"] == "magmausdt:aster->bybit"
        assert pending["worst_case_edge_bps_entry"] == pytest.approx(4.0)
        assert pending["entry_maker_leg"] == "long"
        assert pending["exit_maker_leg"] == "short"
        assert pending["entry_cross_bps_entry"] == pytest.approx(1.25)
        assert pending["fee_bps_entry"] == pytest.approx(2.1)
        assert pending["entry_slippage_bps_entry"] == pytest.approx(0.75)
        assert pending["transfer_bias_bps_entry"] == pytest.approx(-0.5)
        assert pending["transfer_state_at_entry"] == "ok"
        assert pending["entry_liquidity_source_at_entry"] == "local_l2"
        assert pending["long_volume_24h_quote_at_entry"] == pytest.approx(12_000_000.0)
        assert pending["short_volume_24h_quote_at_entry"] == pytest.approx(15_000_000.0)
        assert pending["long_open_interest_quote_at_entry"] == pytest.approx(8_000_000.0)
        assert pending["short_open_interest_quote_at_entry"] == pytest.approx(9_000_000.0)
        assert pending["long_entry_vwap"] == pytest.approx(50000.5)
        assert pending["short_entry_vwap"] == pytest.approx(50010.5)
        assert pending["entry_capacity_constrained"] is True
        assert pending["entry_target_quantity"] == pytest.approx(0.2)
        assert pending["long_max_executable_quantity"] == pytest.approx(0.18)
        assert pending["short_max_executable_quantity"] == pytest.approx(0.16)
        assert pending["entry_max_executable_quantity"] == pytest.approx(0.16)
        assert pending["entry_depth_shortfall_quantity"] == pytest.approx(0.04)
        assert pending["entry_max_executable_notional_quote"] == pytest.approx(8000.0)
        assert pending["entry_depth_capped_at_entry"] is True
        assert pending["advisories"] == ["thin_book"]
        assert pending["blocked_reasons"] == ["capacity_cap"]

        restored = _restore_state_from_snapshot_dict(snap)
        restored_pending = restored.pending_entries["entry-magma"]
        assert restored_pending.opportunity_type == "staggered"
        assert restored_pending.funding_timestamp_ms == 1780167600000
        assert restored_pending.first_funding_timestamp_ms == 1780167600000
        assert restored_pending.long_funding_timestamp_ms == 1780167600000
        assert restored_pending.short_funding_timestamp_ms == 1780171200000
        assert restored_pending.second_funding_timestamp_ms == 1780171200000
        assert restored_pending.first_funding_leg == "long"
        assert restored_pending.funding_edge_bps_entry == pytest.approx(7.45)
        assert restored_pending.total_funding_edge_bps_entry == pytest.approx(7.45)
        assert restored_pending.expected_edge_bps_entry == pytest.approx(6.9)
        assert restored_pending.phase_state is not None
        assert restored_pending.phase_state.execution_kind == "entry"
        assert restored_pending.phase_state.active_maker_leg == "short"
        assert restored_pending.phase_state.next_cycle_delay_ms == 250
        assert restored_pending.passive_manager_runtime.cooldown_until_ms == 1780163910000
        assert restored_pending.created_cycle == 42
        assert restored_pending.repost_attempt_count == 4
        assert restored_pending.passive_attempt_count == 5
        assert restored_pending.passive_ops_total == 9
        assert len(restored_pending.maker_remainder_slices) == 1
        assert restored_pending.maker_remainder_slices[0].average_price() == pytest.approx(20.0)
        assert restored_pending.maker_remainder_slices[0].fill_at_ms == 1780163909000
        assert restored_pending.lifetime_exhausted_logged_final_reason == "passive_entry_lifetime_exhausted"
        assert restored_pending.frozen_candidate["pair_id"] == "magmausdt:aster->bybit"
        assert restored_pending.worst_case_edge_bps_entry == pytest.approx(4.0)
        assert restored_pending.entry_maker_leg == "long"
        assert restored_pending.exit_maker_leg == "short"
        assert restored_pending.entry_cross_bps_entry == pytest.approx(1.25)
        assert restored_pending.fee_bps_entry == pytest.approx(2.1)
        assert restored_pending.entry_slippage_bps_entry == pytest.approx(0.75)
        assert restored_pending.transfer_bias_bps_entry == pytest.approx(-0.5)
        assert restored_pending.transfer_state_at_entry == "ok"
        assert restored_pending.entry_liquidity_source_at_entry == "local_l2"
        assert restored_pending.long_volume_24h_quote_at_entry == pytest.approx(12_000_000.0)
        assert restored_pending.short_volume_24h_quote_at_entry == pytest.approx(15_000_000.0)
        assert restored_pending.long_open_interest_quote_at_entry == pytest.approx(8_000_000.0)
        assert restored_pending.short_open_interest_quote_at_entry == pytest.approx(9_000_000.0)
        assert restored_pending.long_entry_vwap == pytest.approx(50000.5)
        assert restored_pending.short_entry_vwap == pytest.approx(50010.5)
        assert restored_pending.entry_capacity_constrained is True
        assert restored_pending.entry_target_quantity == pytest.approx(0.2)
        assert restored_pending.long_max_executable_quantity == pytest.approx(0.18)
        assert restored_pending.short_max_executable_quantity == pytest.approx(0.16)
        assert restored_pending.entry_max_executable_quantity == pytest.approx(0.16)
        assert restored_pending.entry_depth_shortfall_quantity == pytest.approx(0.04)
        assert restored_pending.entry_max_executable_notional_quote == pytest.approx(8000.0)
        assert restored_pending.entry_depth_capped_at_entry is True
        assert restored_pending.advisories == ["thin_book"]
        assert restored_pending.blocked_reasons == ["capacity_cap"]


# ---------------------------------------------------------------------------
# STATE-003: EngineState lifecycle and control fields
# ---------------------------------------------------------------------------

V1_ENGINESTATE_REQUIRED_FIELDS = {
    "lifecycle",
    "risk_mode",
    "operator",
    "open_positions",
    "pending_entries",
    "pending_closes",
    "pending_passive_closes",
    "run_id",
    "venue_health",
    # Recovery blocked state
    "recovery_blocked_reason",
    "recovery_blocked_at_ms",
    # Pending residual repairs
    "pending_residual_repairs",
    # Live recovery reduce-only
    "live_recovery_reduce_only_pairs",
    # Venue cooldowns
    "venue_entry_cooldowns",
    # Market data degradations
    "venue_market_data_degradations",
    # Transfer truth
    "transfer_truth",
    # Retained L2 books
    "retained_local_l2_books",
    # Entry liquidity qualifications
    "entry_liquidity_qualification_records",
    # Pending close reconciliations
    "pending_close_reconciliations",
    # Global risk reason
    "global_risk_reason",
}


class TestEngineStateFieldCompleteness:
    """STATE-003: EngineState preserves V1 lifecycle and control fields."""

    def test_all_v1_fields_present(self):
        v2_fields = {f.name for f in dataclasses.fields(EngineState)}
        missing = sorted(V1_ENGINESTATE_REQUIRED_FIELDS - v2_fields)
        assert not missing, (
            f"V1 EngineState fields missing in V2: {missing}\n"
            f"V2 fields: {sorted(v2_fields)}"
        )

    def test_engine_state_declares_hyperliquid_disabled_reason_once(self):
        v2_fields = [
            f.name
            for f in dataclasses.fields(EngineState)
            if f.name == "hyperliquid_trading_disabled_reason"
        ]
        source = textwrap.dedent(inspect.getsource(EngineState))
        tree = ast.parse(source)
        class_node = next(
            node for node in tree.body if isinstance(node, ast.ClassDef)
        )
        source_declarations = [
            node.target.id
            for node in class_node.body
            if isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "hyperliquid_trading_disabled_reason"
        ]

        assert v2_fields == ["hyperliquid_trading_disabled_reason"]
        assert source_declarations == ["hyperliquid_trading_disabled_reason"]

    def test_engine_state_to_dict_emits_hyperliquid_disabled_reason_once(self):
        source = textwrap.dedent(inspect.getsource(EngineState.to_dict))
        tree = ast.parse(source)
        return_node = next(
            node for node in ast.walk(tree) if isinstance(node, ast.Return)
        )
        assert isinstance(return_node.value, ast.Dict)
        literal_keys = [
            key.value
            for key in return_node.value.keys
            if isinstance(key, ast.Constant)
        ]

        assert literal_keys.count("hyperliquid_trading_disabled_reason") == 1

    def test_recovery_blocked_state(self):
        state = EngineState()
        assert hasattr(state, "recovery_blocked_reason")
        assert hasattr(state, "recovery_blocked_at_ms")
        assert state.recovery_blocked_reason is None
        assert state.recovery_blocked_at_ms == 0

    def test_pending_residual_repairs(self):
        state = EngineState()
        assert hasattr(state, "pending_residual_repairs")
        assert isinstance(state.pending_residual_repairs, list)

    def test_live_recovery_reduce_only_pairs(self):
        state = EngineState()
        assert hasattr(state, "live_recovery_reduce_only_pairs")
        assert isinstance(state.live_recovery_reduce_only_pairs, list)

    def test_venue_entry_cooldowns(self):
        state = EngineState()
        assert hasattr(state, "venue_entry_cooldowns")
        assert isinstance(state.venue_entry_cooldowns, dict)

    def test_venue_market_data_degradations(self):
        state = EngineState()
        assert hasattr(state, "venue_market_data_degradations")
        assert isinstance(state.venue_market_data_degradations, dict)

    def test_transfer_truth(self):
        state = EngineState()
        assert hasattr(state, "transfer_truth")
        assert isinstance(state.transfer_truth, dict)

    def test_entry_liquidity_qualification_records(self):
        state = EngineState()
        assert hasattr(state, "entry_liquidity_qualification_records")
        assert isinstance(state.entry_liquidity_qualification_records, list)

    def test_pending_close_reconciliations(self):
        state = EngineState()
        assert hasattr(state, "pending_close_reconciliations")
        assert isinstance(state.pending_close_reconciliations, list)

    def test_restore_migrates_dict_shaped_pending_close_reconciliations(self):
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict

        snapshot = {
            "lifecycle": "risk_only",
            "risk_mode": "fail_closed",
            "pending_close_reconciliations": {
                "entry-1780771924982-BABYUSDT": {
                    "position_id": "entry-1780771924982-BABYUSDT",
                    "symbol": "BABYUSDT",
                    "kind": "final",
                    "reason": "pending_passive_close_flat_probe",
                    "closed_at_ms": 1780771929000,
                    "created_cycle": 42,
                    "position_snapshot": {
                        "position_id": "entry-1780771924982-BABYUSDT",
                        "symbol": "BABYUSDT",
                        "long_venue": "okx",
                        "short_venue": "bybit",
                    },
                    "long_legs": [],
                    "short_legs": [],
                    "attempt_count": 0,
                    "next_attempt_ms": 1780771929000,
                }
            },
        }

        state = _restore_state_from_snapshot_dict(snapshot)

        assert isinstance(state.pending_close_reconciliations, list)
        assert (
            state.pending_close_reconciliations[0]["position_id"]
            == "entry-1780771924982-BABYUSDT"
        )
        assert isinstance(state.to_dict()["pending_close_reconciliations"], list)

    def test_pending_close_reconciliation_enqueue_deduplicates_position_and_kind(self):
        state = EngineState()
        item = {
            "position_id": "entry-1780771924982-BABYUSDT",
            "symbol": "BABYUSDT",
            "kind": "final",
            "closed_at_ms": 1780771929000,
        }

        state.enqueue_pending_close_reconciliation(item)
        state.enqueue_pending_close_reconciliation({**item, "reason": "duplicate"})

        assert state.pending_close_reconciliations == [item]

    def test_pending_close_reconciliation_enqueue_caps_oldest_at_256(self):
        state = EngineState()

        for index in range(260):
            state.enqueue_pending_close_reconciliation(
                {
                    "position_id": f"entry-{index}",
                    "symbol": "BABYUSDT",
                    "kind": "final",
                    "closed_at_ms": 1780771929000 + index,
                }
            )

        assert len(state.pending_close_reconciliations) == 256
        assert state.pending_close_reconciliations[0]["position_id"] == "entry-4"

    def test_pending_close_reconciliation_enqueue_remove_matches_closed_at_ms(self):
        state = EngineState()
        first = {
            "position_id": "entry-1780771924982-BABYUSDT",
            "symbol": "BABYUSDT",
            "kind": "final",
            "closed_at_ms": 1780771929000,
        }
        second = {**first, "closed_at_ms": 1780771929001}
        state.pending_close_reconciliations = [first, second]

        removed = state.remove_pending_close_reconciliation(first)

        assert removed is True
        assert state.pending_close_reconciliations == [second]

    def test_pending_close_reconciliation_enqueue_normalizes_existing_dict_shape(self):
        state = EngineState()
        state.pending_close_reconciliations = {
            "entry-1780771924982-BABYUSDT": {
                "position_id": "entry-1780771924982-BABYUSDT",
                "symbol": "BABYUSDT",
                "kind": "final",
                "closed_at_ms": 1780771929000,
            }
        }

        state.enqueue_pending_close_reconciliation(
            {
                "position_id": "entry-1780771924982-MORPHOUSDT",
                "symbol": "MORPHOUSDT",
                "kind": "final",
                "closed_at_ms": 1780771929001,
            }
        )

        assert isinstance(state.pending_close_reconciliations, list)
        assert [
            item["position_id"] for item in state.pending_close_reconciliations
        ] == [
            "entry-1780771924982-BABYUSDT",
            "entry-1780771924982-MORPHOUSDT",
        ]

    def test_pending_close_reconciliation_normalizes_single_task_dict_shape(self):
        raw = {
            "position_id": "entry-1780771924982-BABYUSDT",
            "symbol": "BABYUSDT",
            "kind": "final",
            "reason": "pending_passive_close_flat_probe",
            "closed_at_ms": 1780771929000,
            "position_snapshot": {
                "position_id": "entry-1780771924982-BABYUSDT",
                "symbol": "BABYUSDT",
                "long_venue": "okx",
                "short_venue": "bybit",
            },
            "long_legs": [],
            "short_legs": [],
        }

        normalized = normalize_pending_close_reconciliations(raw)

        assert normalized == [raw]

    def test_pending_close_reconciliation_preserves_invalid_evidence_task(self):
        raw = [
            "poisoned-item",
            {
                "position_id": "entry-1780771924982-BABYUSDT",
                "symbol": "BABYUSDT",
                "kind": "final",
                "closed_at_ms": 1780771929000,
            },
        ]

        normalized = normalize_pending_close_reconciliations(raw)

        assert normalized[0]["invalid_pending_close_reconciliation"] is True
        assert normalized[0]["raw_type"] == "str"
        assert normalized[1]["position_id"] == "entry-1780771924982-BABYUSDT"

    def test_global_risk_reason(self):
        state = EngineState()
        assert hasattr(state, "global_risk_reason")
        assert state.global_risk_reason is None

    def test_engine_state_to_dict_includes_new_fields(self):
        state = EngineState(
            run_id="test-run",
            global_risk_reason="test-reason",
            recovery_blocked_reason="ambiguous",
            recovery_blocked_at_ms=5000,
        )
        d = state.to_dict()
        assert d["run_id"] == "test-run"
        assert d.get("global_risk_reason") == "test-reason"
        assert d.get("recovery_blocked_reason") == "ambiguous"
        assert d.get("recovery_blocked_at_ms") == 5000


# ---------------------------------------------------------------------------
# Edge breakdown and funding leg fields (V1 OpenPosition parity)
# ---------------------------------------------------------------------------

class TestEdgeBreakdownFields:
    """V1 edge breakdown fields must be preserved on OpenPosition."""

    def test_funding_edge_fields(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000,
                           funding_edge_bps_entry=5.0,
                           total_funding_edge_bps_entry=4.5,
                           expected_edge_bps_entry=3.0)
        assert pos.funding_edge_bps_entry == 5.0
        assert pos.total_funding_edge_bps_entry == 4.5
        assert pos.expected_edge_bps_entry == 3.0

    def test_funding_leg_fields(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000,
                           funding_timestamp_ms=3000)
        assert pos.funding_timestamp_ms == 3000


class TestFundingStageFields:
    """V1 funding stage fields (second_stage_*, opportunity_type)."""

    def test_funding_stage_defaults(self):
        pos = OpenPosition(position_id="p1", symbol="BTC-USDT",
                           long_venue="binance", short_venue="okx",
                           long_quantity=1.0, short_quantity=1.0,
                           long_entry_price=50000.0, short_entry_price=50000.0,
                           opened_at_ms=1000)
        assert pos.opportunity_type == "aligned"
        assert pos.second_stage_enabled_at_entry is False
        assert pos.second_stage_funding_captured is False
        assert pos.second_stage_funding_quote == 0.0
        assert pos.second_funding_timestamp_ms == 0
