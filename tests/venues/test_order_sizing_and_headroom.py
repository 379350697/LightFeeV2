"""V1 semantic parity: Order sizing specification and open-notional headroom tests.

Validates that every venue adapter provides correct:
- Order sizing spec (quantity_step, min_quantity, min_notional, price_tick)
- Quantity normalization floors to step
- Min notional enforcement
- Open notional headroom calculation
"""

from __future__ import annotations

import pytest

from lightfee.core.domain import Venue
from lightfee.venues.capabilities import (
    OrderSizingSpec,
    OrderSizingMode,
    get_capability_flags,
)
from lightfee.venues.specs import get_spec


# ---------------------------------------------------------------------------
# Order sizing specification
# ---------------------------------------------------------------------------


class TestOrderSizingSpec:
    """OrderSizingSpec must provide V1-compatible sizing constraints."""

    def test_binance_sizing_spec(self):
        spec = get_spec(Venue.BINANCE)
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            quantity_step=spec.quantity_step,
            min_quantity=spec.min_quantity,
            min_notional=spec.min_notional,
            contract_size=spec.contract_size,
            price_tick=spec.price_tick,
        )
        assert sizing.quantity_step == 0.001
        assert sizing.min_notional == 5.0

    def test_okx_sizing_spec(self):
        spec = get_spec(Venue.OKX)
        sizing = OrderSizingSpec(
            venue=Venue.OKX,
            symbol="BTCUSDT",
            quantity_step=spec.quantity_step,
            min_quantity=spec.min_quantity,
            min_notional=spec.min_notional,
        )
        assert sizing.quantity_step == 0.01
        assert sizing.min_notional == 1.0

    def test_hyperliquid_sizing_spec(self):
        spec = get_spec(Venue.HYPERLIQUID)
        sizing = OrderSizingSpec(
            venue=Venue.HYPERLIQUID,
            symbol="BTC",
            quantity_step=spec.quantity_step,
            min_quantity=spec.min_quantity,
            min_notional=spec.min_notional,
        )
        assert sizing.quantity_step == 1.0  # HL uses integer lots
        assert sizing.min_notional == 10.0

    @pytest.mark.parametrize("venue", list(Venue))
    def test_every_venue_has_valid_spec(self, venue):
        """Every venue spec must have positive quantity_step and min_notional."""
        spec = get_spec(venue)
        assert spec.quantity_step > 0, f"{venue}: quantity_step must be > 0"
        assert spec.min_notional > 0, f"{venue}: min_notional must be > 0"


# ---------------------------------------------------------------------------
# Quantity normalization (floors to step)
# ---------------------------------------------------------------------------


class TestQuantityNormalization:
    """Quantity normalization must floor to the venue's step size."""

    def test_normalize_floors_to_step_binance(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        assert sizing.normalize_quantity(0.0015) == 0.001  # floor
        assert sizing.normalize_quantity(0.003) == 0.003     # exact
        assert sizing.normalize_quantity(0.0) == 0.0

    def test_normalize_floors_to_step_okx(self):
        sizing = OrderSizingSpec(
            venue=Venue.OKX, symbol="BTCUSDT",
            quantity_step=0.01, min_quantity=0.01, min_notional=1.0,
        )
        assert sizing.normalize_quantity(0.015) == 0.01
        assert sizing.normalize_quantity(0.02) == 0.02
        assert sizing.normalize_quantity(0.009) == 0.0  # below step → 0

    def test_normalize_floors_to_step_hyperliquid(self):
        sizing = OrderSizingSpec(
            venue=Venue.HYPERLIQUID, symbol="BTC",
            quantity_step=1.0, min_quantity=1.0, min_notional=10.0,
        )
        assert sizing.normalize_quantity(1.7) == 1.0
        assert sizing.normalize_quantity(3.0) == 3.0
        assert sizing.normalize_quantity(0.5) == 0.0

    def test_normalize_with_default_step(self):
        sizing = OrderSizingSpec(
            venue=Venue.GATE, symbol="BTCUSDT",
            quantity_step=1.0, min_quantity=1.0, min_notional=1.0,
        )
        assert sizing.normalize_quantity(1.5) == 1.0


# ---------------------------------------------------------------------------
# Min notional enforcement
# ---------------------------------------------------------------------------


class TestMinNotionalEnforcement:
    """Min notional check must gate orders below venue minimum."""

    def test_above_min_notional_passes(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        # 0.001 BTC at $50,000 = $50 notional → passes
        assert sizing.check_min_notional(0.001, 50000.0) is True

    def test_below_min_notional_fails(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        # 0.001 BTC at $4,000 = $4 notional → fails
        assert sizing.check_min_notional(0.001, 4000.0) is False

    def test_exactly_at_min_notional_passes(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        # 0.001 BTC at $5,000 = $5 notional → passes
        assert sizing.check_min_notional(0.001, 5000.0) is True

    def test_zero_quantity_fails(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        assert sizing.check_min_notional(0.0, 50000.0) is False


# ---------------------------------------------------------------------------
# Venue adapter order sizing spec
# ---------------------------------------------------------------------------


class TestVenueAdapterOrderSizing:
    """Every venue adapter must expose order_sizing_spec()."""

    def test_binance_adapter_sizing_spec(self):
        from lightfee.venues.binance import BinanceAdapter
        adapter = BinanceAdapter()
        spec = adapter.order_sizing_spec("BTCUSDT")
        assert spec["quantity_step"] == 0.001
        assert spec["min_notional"] == 5.0
        assert spec["price_tick"] == 0.01

    def test_okx_adapter_sizing_spec(self):
        from lightfee.venues.okx import OkxAdapter
        adapter = OkxAdapter()
        spec = adapter.order_sizing_spec("BTCUSDT")
        assert spec["quantity_step"] == 0.01
        assert spec["min_notional"] == 1.0

    def test_bybit_adapter_sizing_spec(self):
        from lightfee.venues.bybit import BybitAdapter
        adapter = BybitAdapter()
        spec = adapter.order_sizing_spec("BTCUSDT")
        assert spec["quantity_step"] == 0.001
        assert spec["min_notional"] == 1.0

    def test_bitget_adapter_sizing_spec(self):
        from lightfee.venues.bitget import BitgetAdapter
        adapter = BitgetAdapter()
        spec = adapter.order_sizing_spec("BTCUSDT")
        assert spec["quantity_step"] == 0.001
        assert spec["min_notional"] == 5.0

    def test_gate_adapter_sizing_spec(self):
        from lightfee.venues.gate import GateAdapter
        adapter = GateAdapter()
        spec = adapter.order_sizing_spec("BTCUSDT")
        assert spec["quantity_step"] == 1.0
        assert spec["min_notional"] == 1.0

    def test_aster_adapter_sizing_spec(self):
        from lightfee.venues.aster import AsterAdapter
        adapter = AsterAdapter()
        spec = adapter.order_sizing_spec("BTCUSDT")
        assert spec["quantity_step"] == 0.001
        assert spec["min_notional"] == 5.0

    def test_hyperliquid_adapter_sizing_spec(self):
        from lightfee.venues.hyperliquid import HyperliquidAdapter
        adapter = HyperliquidAdapter()
        spec = adapter.order_sizing_spec("BTC")
        assert spec["quantity_step"] == 1.0
        assert spec["min_notional"] == 10.0


# ---------------------------------------------------------------------------
# Entry open-notional headroom
# ---------------------------------------------------------------------------


class TestEntryOpenNotionalHeadroom:
    """Entry open-notional headroom must match V1 venue constraints."""

    def test_headroom_unbounded_when_no_max(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        assert sizing.available_headroom(1.0) == float("inf")
        assert sizing.available_headroom(100.0) == float("inf")

    def test_headroom_capped_at_max(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        assert sizing.available_headroom(3.0, max_position=5.0) == 2.0

    def test_headroom_zero_when_full(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        assert sizing.available_headroom(5.0, max_position=5.0) == 0.0

    def test_headroom_zero_when_over_max(self):
        sizing = OrderSizingSpec(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            quantity_step=0.001, min_quantity=0.001, min_notional=5.0,
        )
        assert sizing.available_headroom(6.0, max_position=5.0) == 0.0

    def test_hyperliquid_no_entry_headroom(self):
        """Hyperliquid does not support entry open-notional headroom queries."""
        flags = get_capability_flags(Venue.HYPERLIQUID)
        assert flags.entry_open_notional_headroom is False


# ---------------------------------------------------------------------------
# Capability-based sizing mode
# ---------------------------------------------------------------------------


class TestOrderSizingModePerVenue:
    """Each venue must declare its order sizing mode explicitly."""

    def test_binance_contract_size_step(self):
        flags = get_capability_flags(Venue.BINANCE)
        assert flags.order_sizing_mode == OrderSizingMode.CONTRACT_SIZE_STEP

    def test_hyperliquid_native_round_lot(self):
        flags = get_capability_flags(Venue.HYPERLIQUID)
        assert flags.order_sizing_mode == OrderSizingMode.NATIVE_ROUND_LOT

    def test_all_venues_declare_sizing_mode(self):
        matrix = get_capability_flags  # function, not dict
        from lightfee.venues.capabilities import capability_matrix
        matrix_dict = capability_matrix()
        for venue in Venue:
            flags = matrix_dict[venue]
            assert flags.order_sizing_mode is not None, (
                f"{venue}: order_sizing_mode must not be None"
            )
