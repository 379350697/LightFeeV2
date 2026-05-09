"""Venue adapter base classes and capability types."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from lightfee.core.domain import (
    AccountBalanceSnapshot,
    ExecutionLiquiditySnapshot,
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PerpLiquiditySnapshot,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)


class CapabilitySupport(Enum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"

    def is_supported(self) -> bool:
        return self == CapabilitySupport.SUPPORTED


class ExecutionLiquidityCapability(Enum):
    TRUE_L2 = "true_l2"
    FALLBACK_ONLY = "fallback_only"
    UNSUPPORTED = "unsupported"


class ReconcileQuality(Enum):
    ORDER_FILL = "order_fill"
    UNSUPPORTED = "unsupported"


class VenueMarketApiContract(Enum):
    BINANCE_USDM_REST = "binance_usdm_rest"
    ASTER_PERPETUALS_FAPI = "aster_perpetuals_fapi"
    OKX_V5 = "okx_v5"
    BYBIT_V5 = "bybit_v5"
    BITGET_MARKET_V3 = "bitget_market_v3"
    GATE_FUTURES_V4 = "gate_futures_v4"
    HYPERLIQUID_INFO_API = "hyperliquid_info_api"


class VenuePrivateApiContract(Enum):
    BINANCE_USDM_PRIVATE_V3 = "binance_usdm_private_v3"
    ASTER_BALANCE_V2 = "aster_balance_v2_account_v4_position_v2"
    OKX_V5 = "okx_v5"
    BYBIT_V5 = "bybit_v5"
    BITGET_MIX_PRIVATE_V2 = "bitget_mix_private_v2"
    GATE_FUTURES_V4 = "gate_futures_v4"
    HYPERLIQUID_EXCHANGE_API = "hyperliquid_exchange_api"


class VenueAccountContract(Enum):
    SINGLE_OR_MULTI_ASSET = "single_or_multi_asset_modes"
    UNIFIED_ACCOUNT = "unified_account"
    DETECT_CLASSIC_VS_UTA = "detect_classic_vs_uta"
    NATIVE_PERP_ACCOUNT = "native_perp_account"
    DUAL_POSITION_MODE = "dual_position_mode_account"


class TestnetSupport(Enum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class VenueCapabilities:
    venue: Venue
    risk_health: CapabilitySupport
    private_health: CapabilitySupport
    execution_liquidity: ExecutionLiquidityCapability
    reconcile_quality: ReconcileQuality
    testnet_support: TestnetSupport
    market_api_contract: VenueMarketApiContract
    private_api_contract: VenuePrivateApiContract
    account_contract: VenueAccountContract

    def supports_risk_health(self) -> bool:
        return self.risk_health.is_supported()

    def supports_private_health(self) -> bool:
        return self.private_health.is_supported()

    @classmethod
    def for_venue(cls, venue: Venue) -> VenueCapabilities:
        mapping = {
            Venue.BINANCE: cls(
                venue=Venue.BINANCE,
                risk_health=CapabilitySupport.SUPPORTED,
                private_health=CapabilitySupport.SUPPORTED,
                execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
                reconcile_quality=ReconcileQuality.ORDER_FILL,
                testnet_support=TestnetSupport.SUPPORTED,
                market_api_contract=VenueMarketApiContract.BINANCE_USDM_REST,
                private_api_contract=VenuePrivateApiContract.BINANCE_USDM_PRIVATE_V3,
                account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            ),
            Venue.ASTER: cls(
                venue=Venue.ASTER,
                risk_health=CapabilitySupport.SUPPORTED,
                private_health=CapabilitySupport.SUPPORTED,
                execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
                reconcile_quality=ReconcileQuality.ORDER_FILL,
                testnet_support=TestnetSupport.UNKNOWN,
                market_api_contract=VenueMarketApiContract.ASTER_PERPETUALS_FAPI,
                private_api_contract=VenuePrivateApiContract.ASTER_BALANCE_V2,
                account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            ),
            Venue.OKX: cls(
                venue=Venue.OKX,
                risk_health=CapabilitySupport.SUPPORTED,
                private_health=CapabilitySupport.SUPPORTED,
                execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
                reconcile_quality=ReconcileQuality.ORDER_FILL,
                testnet_support=TestnetSupport.SUPPORTED,
                market_api_contract=VenueMarketApiContract.OKX_V5,
                private_api_contract=VenuePrivateApiContract.OKX_V5,
                account_contract=VenueAccountContract.UNIFIED_ACCOUNT,
            ),
            Venue.BYBIT: cls(
                venue=Venue.BYBIT,
                risk_health=CapabilitySupport.SUPPORTED,
                private_health=CapabilitySupport.SUPPORTED,
                execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
                reconcile_quality=ReconcileQuality.ORDER_FILL,
                testnet_support=TestnetSupport.SUPPORTED,
                market_api_contract=VenueMarketApiContract.BYBIT_V5,
                private_api_contract=VenuePrivateApiContract.BYBIT_V5,
                account_contract=VenueAccountContract.UNIFIED_ACCOUNT,
            ),
            Venue.HYPERLIQUID: cls(
                venue=Venue.HYPERLIQUID,
                risk_health=CapabilitySupport.UNSUPPORTED,
                private_health=CapabilitySupport.SUPPORTED,
                execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
                reconcile_quality=ReconcileQuality.UNSUPPORTED,
                testnet_support=TestnetSupport.UNKNOWN,
                market_api_contract=VenueMarketApiContract.HYPERLIQUID_INFO_API,
                private_api_contract=VenuePrivateApiContract.HYPERLIQUID_EXCHANGE_API,
                account_contract=VenueAccountContract.NATIVE_PERP_ACCOUNT,
            ),
            Venue.BITGET: cls(
                venue=Venue.BITGET,
                risk_health=CapabilitySupport.UNSUPPORTED,
                private_health=CapabilitySupport.SUPPORTED,
                execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
                reconcile_quality=ReconcileQuality.UNSUPPORTED,
                testnet_support=TestnetSupport.UNKNOWN,
                market_api_contract=VenueMarketApiContract.BITGET_MARKET_V3,
                private_api_contract=VenuePrivateApiContract.BITGET_MIX_PRIVATE_V2,
                account_contract=VenueAccountContract.DETECT_CLASSIC_VS_UTA,
            ),
            Venue.GATE: cls(
                venue=Venue.GATE,
                risk_health=CapabilitySupport.UNSUPPORTED,
                private_health=CapabilitySupport.SUPPORTED,
                execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
                reconcile_quality=ReconcileQuality.UNSUPPORTED,
                testnet_support=TestnetSupport.UNKNOWN,
                market_api_contract=VenueMarketApiContract.GATE_FUTURES_V4,
                private_api_contract=VenuePrivateApiContract.GATE_FUTURES_V4,
                account_contract=VenueAccountContract.DUAL_POSITION_MODE,
            ),
        }
        return mapping[venue]
