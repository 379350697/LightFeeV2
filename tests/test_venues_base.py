"""Tests for venue adapter port, capabilities, and sizing."""

import pytest

from lightfee.core.domain import Venue
from lightfee.venues.base import (
    CapabilitySupport,
    ExecutionLiquidityCapability,
    ReconcileQuality,
    VenueCapabilities,
)
from lightfee.venues.common import venue_reduce_only_close_exempts_min_notional


class TestVenueCapabilities:
    def test_all_seven_venues_have_true_l2(self):
        for v in Venue:
            caps = VenueCapabilities.for_venue(v)
            assert caps.execution_liquidity == ExecutionLiquidityCapability.TRUE_L2

    def test_binance_has_full_support(self):
        caps = VenueCapabilities.for_venue(Venue.BINANCE)
        assert caps.risk_health == CapabilitySupport.SUPPORTED
        assert caps.private_health == CapabilitySupport.SUPPORTED
        assert caps.reconcile_quality == ReconcileQuality.ORDER_FILL
        assert caps.supports_risk_health()
        assert caps.supports_private_health()

    def test_hyperliquid_risk_unsupported(self):
        """Only Hyperliquid has no account risk endpoint."""
        caps = VenueCapabilities.for_venue(Venue.HYPERLIQUID)
        assert caps.risk_health == CapabilitySupport.UNSUPPORTED
        assert not caps.supports_risk_health()

    def test_bitget_gate_risk_now_supported(self):
        """Bitget and Gate risk health is now implemented (live REST endpoints)."""
        for v in (Venue.BITGET, Venue.GATE):
            caps = VenueCapabilities.for_venue(v)
            assert caps.risk_health == CapabilitySupport.SUPPORTED
            assert caps.supports_risk_health()

    def test_okx_bybit_have_unified_account(self):
        for v in (Venue.OKX, Venue.BYBIT):
            caps = VenueCapabilities.for_venue(v)
            assert "unified" in caps.account_contract.value

    def test_distinct_api_contracts(self):
        binance = VenueCapabilities.for_venue(Venue.BINANCE)
        aster = VenueCapabilities.for_venue(Venue.ASTER)
        bitget = VenueCapabilities.for_venue(Venue.BITGET)
        assert binance.market_api_contract != aster.market_api_contract
        assert aster.private_api_contract != binance.private_api_contract
        assert bitget.market_api_contract.value == "bitget_market_v3"


class TestReduceOnlyExemptions:
    def test_aster_binance_are_exempt(self):
        assert venue_reduce_only_close_exempts_min_notional(Venue.ASTER)
        assert venue_reduce_only_close_exempts_min_notional(Venue.BINANCE)

    def test_gate_is_exempt(self):
        """Gate is exempt per V1 live/gate.rs: reduce-only empty position is terminal success."""
        assert venue_reduce_only_close_exempts_min_notional(Venue.GATE)

    def test_others_not_exempt(self):
        for v in (Venue.OKX, Venue.BYBIT, Venue.BITGET, Venue.HYPERLIQUID):
            assert not venue_reduce_only_close_exempts_min_notional(v)
