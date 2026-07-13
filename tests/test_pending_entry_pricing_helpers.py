from __future__ import annotations

from types import SimpleNamespace

from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.engine.pending_entry_runtime import (
    pending_entry_edge_headroom_bps,
    pending_entry_maker_inventory_bias_threshold_quote,
    pending_entry_signed_inventory_notional_quote,
)


def test_pending_entry_inventory_helpers_preserve_paired_and_legacy_position_signs():
    strategy = SimpleNamespace(
        live_entry_notional_cap_quote=40.0,
        entry_notional_cap_quote=50.0,
        min_entry_leg_notional_quote=10.0,
    )
    paired_long = SimpleNamespace(
        symbol="BTCUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        quantity=2.0,
    )
    paired_short = SimpleNamespace(
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        quantity=1.0,
    )
    legacy_long = PositionSnapshot(
        venue=Venue.OKX,
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=3.0,
        entry_price=100.0,
        observed_at_ms=1,
    )

    assert pending_entry_maker_inventory_bias_threshold_quote(strategy) == 50.0
    assert pending_entry_signed_inventory_notional_quote(
        {"a": paired_long, "b": paired_short, "c": legacy_long},
        Venue.OKX,
        "BTCUSDT",
        100.0,
    ) == 400.0


def test_pending_entry_edge_headroom_uses_frozen_evidence_and_rejects_bad_values():
    strategy = SimpleNamespace(
        min_expected_edge_bps=10.0,
        min_worst_case_edge_bps=2.0,
    )
    pending = SimpleNamespace(
        frozen_candidate={
            "entry_notional_quote": 50.0,
            "expected_edge_bps": 14.0,
            "worst_case_edge_bps": 5.0,
        },
    )

    assert pending_entry_edge_headroom_bps(pending, strategy) == 3.0
    pending.frozen_candidate["expected_edge_bps"] = "not-a-number"
    assert pending_entry_edge_headroom_bps(pending, strategy) is None
