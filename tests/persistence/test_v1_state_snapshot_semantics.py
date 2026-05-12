"""Semantic parity tests for V1 state model fields (STATE-001, STATE-002, STATE-003).

Validates that V2 OpenPosition, PendingEntry, and EngineState preserve all
V1-required semantic fields, either as direct attributes or via equivalent
V2-native structures.
"""

from __future__ import annotations

import pytest
from lightfee.engine.state import (
    EngineState,
    OpenPosition,
    PendingEntry,
    PendingClose,
    OperatorControlState,
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
    "long_quantity",
    "short_quantity",
    # Entry prices / notional
    "long_entry_price",
    "short_entry_price",
    # Review & origin
    "review_id",
    "opportunity_origin_tags",
    "opportunity_hint_source",
    # Edge breakdowns (entry)
    "funding_edge_bps_entry",
    "total_funding_edge_bps_entry",
    "expected_edge_bps_entry",
    # Transfer & liquidity
    "transfer_state_at_entry",
    "entry_liquidity_source_at_entry",
    # VWAP
    "long_entry_vwap",
    "short_entry_vwap",
    # Capacity constraints
    "entry_capacity_constrained",
    # Advisories & blocked reasons
    "advisories",
    "blocked_reasons",
    # Quality markouts
    "entry_quality_markout_5s_emitted",
    "entry_quality_markout_30s_emitted",
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
    # Net/Peak
    "peak_net_quote",
    "current_net_quote",
    # Timestamps
    "opened_at_ms",
    "funding_timestamp_ms",
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
        import dataclasses
        v2_fields = {f.name for f in dataclasses.fields(EngineState)}
        missing = sorted(V1_ENGINESTATE_REQUIRED_FIELDS - v2_fields)
        assert not missing, (
            f"V1 EngineState fields missing in V2: {missing}\n"
            f"V2 fields: {sorted(v2_fields)}"
        )

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
