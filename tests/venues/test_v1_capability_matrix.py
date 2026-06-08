"""V1 semantic parity: Venue capability matrix tests.

Validates that V2 venue capability declarations match V1 business semantics.
Key drift fix: Bitget and Gate risk_health marked UNSUPPORTED per V1.
"""

from __future__ import annotations

import pytest

from lightfee.core.domain import Venue
from lightfee.venues.base import (
    CapabilitySupport,
    ExecutionLiquidityCapability,
    ReconcileQuality,
    TestnetSupport,
    VenueCapabilities,
    VenuePrivateApiContract,
)
from lightfee.venues.capabilities import (
    OrderSizingMode,
    PassiveProgressMode,
    TransferCapability,
    VenueCapabilityFlags,
    capability_matrix,
    get_capability_flags,
)


# ---------------------------------------------------------------------------
# Capability matrix must match V1
# ---------------------------------------------------------------------------


class TestCapabilityMatrixMatchesV1:
    """Every venue's capability declaration must match V1 business semantics."""

    def test_all_seven_venues_in_matrix(self):
        matrix = capability_matrix()
        for venue in Venue:
            assert venue in matrix, f"{venue} missing from capability matrix"

    def test_binance_risk_health_supported(self):
        caps = get_capability_flags(Venue.BINANCE)
        assert caps.risk_health == CapabilitySupport.SUPPORTED

    def test_okx_risk_health_supported(self):
        caps = get_capability_flags(Venue.OKX)
        assert caps.risk_health == CapabilitySupport.SUPPORTED

    def test_bybit_risk_health_supported(self):
        caps = get_capability_flags(Venue.BYBIT)
        assert caps.risk_health == CapabilitySupport.SUPPORTED

    def test_aster_risk_health_supported(self):
        caps = get_capability_flags(Venue.ASTER)
        assert caps.risk_health == CapabilitySupport.SUPPORTED

    def test_aster_v3_private_health_is_not_legacy_private_ws(self):
        caps = get_capability_flags(Venue.ASTER)
        assert caps.private_health == CapabilitySupport.UNSUPPORTED
        assert caps.cached_private_health is False
        assert caps.passive_progress_mode == PassiveProgressMode.PERIODIC_POLL
        assert caps.passive_wakeups is False
        assert caps.private_api_contract == VenuePrivateApiContract.ASTER_PRO_API_V3

    def test_bitget_risk_health_unsupported_v1_parity(self):
        """DEV-002 fix: Bitget risk_health must be UNSUPPORTED per V1."""
        caps = get_capability_flags(Venue.BITGET)
        assert caps.risk_health == CapabilitySupport.UNSUPPORTED, (
            "V1 marks Bitget risk_health as unsupported — "
            "V2 must match unless an approved deviation exists"
        )

    def test_gate_risk_health_unsupported_v1_parity(self):
        """DEV-002 fix: Gate risk_health must be UNSUPPORTED per V1."""
        caps = get_capability_flags(Venue.GATE)
        assert caps.risk_health == CapabilitySupport.UNSUPPORTED, (
            "V1 marks Gate risk_health as unsupported — "
            "V2 must match unless an approved deviation exists"
        )

    def test_hyperliquid_risk_health_unsupported(self):
        caps = get_capability_flags(Venue.HYPERLIQUID)
        assert caps.risk_health == CapabilitySupport.UNSUPPORTED

    def test_binance_execution_liquidity_true_l2(self):
        caps = get_capability_flags(Venue.BINANCE)
        assert caps.execution_liquidity == ExecutionLiquidityCapability.TRUE_L2

    def test_bitget_reconcile_quality_unsupported(self):
        caps = get_capability_flags(Venue.BITGET)
        assert caps.reconcile_quality == ReconcileQuality.UNSUPPORTED

    def test_gate_reconcile_quality_unsupported(self):
        caps = get_capability_flags(Venue.GATE)
        assert caps.reconcile_quality == ReconcileQuality.UNSUPPORTED

    def test_no_venue_is_silently_missing(self):
        """Every venue must have both VenueCapabilities and VenueCapabilityFlags."""
        matrix = capability_matrix()
        for venue in Venue:
            legacy = VenueCapabilities.for_venue(venue)
            flags = matrix[venue]
            assert legacy.venue == flags.venue
            # risk_health must agree between legacy and new system
            assert legacy.risk_health == flags.risk_health, (
                f"{venue}: legacy caps risk_health={legacy.risk_health} "
                f"!= flags risk_health={flags.risk_health}"
            )


# ---------------------------------------------------------------------------
# Legacy VenueCapabilities consistency
# ---------------------------------------------------------------------------


class TestLegacyVenueCapabilityConsistency:
    """Legacy VenueCapabilities must match the v1 parity matrix."""

    def test_bitget_risk_health_unsupported_in_legacy(self):
        """After fix: Bitget legacy caps must also show UNSUPPORTED."""
        caps = VenueCapabilities.for_venue(Venue.BITGET)
        assert caps.risk_health == CapabilitySupport.UNSUPPORTED, (
            "Legacy VenueCapabilities.for_venue(BITGET) must have risk_health=UNSUPPORTED"
        )

    def test_gate_risk_health_unsupported_in_legacy(self):
        """After fix: Gate legacy caps must also show UNSUPPORTED."""
        caps = VenueCapabilities.for_venue(Venue.GATE)
        assert caps.risk_health == CapabilitySupport.UNSUPPORTED, (
            "Legacy VenueCapabilities.for_venue(GATE) must have risk_health=UNSUPPORTED"
        )

    def test_bitget_gate_risk_health_deviation(self):
        """DEV-002 coverage: Bitget and Gate risk_health drift is now fixed.

        If this test passes, the deviation can be marked as resolved.
        """
        for venue in (Venue.BITGET, Venue.GATE):
            caps = VenueCapabilities.for_venue(venue)
            assert caps.risk_health == CapabilitySupport.UNSUPPORTED, (
                f"DEV-002: {venue} risk_health should be UNSUPPORTED (fix applied)"
            )


# ---------------------------------------------------------------------------
# VenueAdapter contract completeness
# ---------------------------------------------------------------------------


class TestAdapterContractCompleteness:
    """VenueAdapter must expose the full V1 contract."""

    @pytest.fixture
    def all_adapters(self):
        from lightfee.venues.binance import BinanceAdapter
        from lightfee.venues.okx import OkxAdapter
        from lightfee.venues.bybit import BybitAdapter
        from lightfee.venues.bitget import BitgetAdapter
        from lightfee.venues.gate import GateAdapter
        from lightfee.venues.aster import AsterAdapter
        from lightfee.venues.hyperliquid import HyperliquidAdapter
        return [
            (Venue.BINANCE, BinanceAdapter),
            (Venue.OKX, OkxAdapter),
            (Venue.BYBIT, BybitAdapter),
            (Venue.BITGET, BitgetAdapter),
            (Venue.GATE, GateAdapter),
            (Venue.ASTER, AsterAdapter),
            (Venue.HYPERLIQUID, HyperliquidAdapter),
        ]

    def test_all_adapters_are_venue_adapter(self, all_adapters):
        from lightfee.core.contracts import VenueAdapter
        for venue, adapter_cls in all_adapters:
            adapter = adapter_cls()
            assert isinstance(adapter, VenueAdapter), (
                f"{adapter_cls.__name__} must be a VenueAdapter"
            )

    def test_all_adapters_have_venue_property(self, all_adapters):
        for venue, adapter_cls in all_adapters:
            adapter = adapter_cls()
            assert adapter.venue == venue

    def test_all_adapters_respond_to_shutdown(self, all_adapters):
        import asyncio
        for _venue, adapter_cls in all_adapters:
            adapter = adapter_cls()
            asyncio.run(adapter.shutdown())

    def test_all_adapters_have_private_health(self, all_adapters):
        for _venue, adapter_cls in all_adapters:
            adapter = adapter_cls()
            assert hasattr(adapter, 'private_health'), (
                f"{adapter_cls.__name__} missing private_health"
            )

    def test_all_adapters_have_prewarm(self, all_adapters):
        for _venue, adapter_cls in all_adapters:
            adapter = adapter_cls()
            assert hasattr(adapter, 'prewarm'), (
                f"{adapter_cls.__name__} missing prewarm"
            )

    def test_all_adapters_have_activate_for_live(self, all_adapters):
        for _venue, adapter_cls in all_adapters:
            adapter = adapter_cls()
            assert hasattr(adapter, 'activate_for_live'), (
                f"{adapter_cls.__name__} missing activate_for_live"
            )

    def test_all_adapters_have_supported_symbols(self, all_adapters):
        for _venue, adapter_cls in all_adapters:
            adapter = adapter_cls()
            symbols = adapter.supported_symbols()
            assert isinstance(symbols, list), (
                f"{adapter_cls.__name__}: supported_symbols must return list"
            )

    def test_all_adapters_have_order_sizing_spec(self, all_adapters):
        for _venue, adapter_cls in all_adapters:
            adapter = adapter_cls()
            spec = adapter.order_sizing_spec("BTCUSDT")
            assert isinstance(spec, dict)
            assert "quantity_step" in spec
            assert "min_quantity" in spec or "min_notional" in spec


# ---------------------------------------------------------------------------
# Capability flags sanity
# ---------------------------------------------------------------------------


class TestCapabilityFlagsSanity:
    """Capability flags must be internally consistent."""

    def test_risk_health_supported_implies_cached_private_health(self):
        """Venues with risk_health should also support cached_private_health."""
        matrix = capability_matrix()
        for venue, flags in matrix.items():
            if venue == Venue.ASTER:
                # Aster Pro API V3 exposes REST account risk, but no Binance-style
                # private WS/listen-key health contract.
                assert flags.risk_health == CapabilitySupport.SUPPORTED
                assert flags.private_health == CapabilitySupport.UNSUPPORTED
                continue
            if flags.risk_health == CapabilitySupport.SUPPORTED:
                assert flags.private_health == CapabilitySupport.SUPPORTED, (
                    f"{venue}: risk_health requires private_health"
                )

    def test_true_l2_implies_local_l2_supported(self):
        matrix = capability_matrix()
        for venue, flags in matrix.items():
            if flags.execution_liquidity == ExecutionLiquidityCapability.TRUE_L2:
                assert flags.local_l2_supported, (
                    f"{venue}: TRUE_L2 requires local_l2_supported"
                )

    def test_passive_progress_not_unsupported_for_order_fill_reconcile(self):
        """Venues with ORDER_FILL reconcile should have passive progress."""
        matrix = capability_matrix()
        for venue, flags in matrix.items():
            if flags.reconcile_quality == ReconcileQuality.ORDER_FILL:
                assert flags.passive_progress_mode != PassiveProgressMode.UNSUPPORTED, (
                    f"{venue}: ORDER_FILL reconcile requires passive progress support"
                )
