"""Venue specification objects for the shared transport layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional

from lightfee.core.domain import Venue
from lightfee.venues.base import VenueAccountContract


class AuthScheme(Enum):
    HMAC_SHA256_HEX = "hmac_sha256_hex"
    HMAC_SHA256_BASE64 = "hmac_sha256_base64"
    HMAC_SHA512_HEX = "hmac_sha512_hex"
    EIP712 = "eip712"


@dataclass(frozen=True)
class VenueSpec:
    venue_id: Venue
    public_base_url: str
    private_base_url: str
    auth_scheme: AuthScheme
    account_contract: VenueAccountContract
    quantity_step: float = 0.001
    contract_size: float = 1.0
    min_quantity: float = 0.001
    min_notional: float = 5.0
    # V1: canonical price tick for passive repricing (NOT quantity step)
    price_tick: float = 0.0

    # Endpoint path builders
    market_snapshot_path: str = ""
    position_path: str = ""
    order_path: str = ""
    account_risk_path: str = ""
    l2_snapshot_path: str = ""  # REST order book depth endpoint for local-L2 bootstrap

    # Whether the venue requires a passphrase for auth
    requires_passphrase: bool = False
    # Whether the venue requires a wallet private key (EIP-712)
    requires_wallet_key: bool = False
    # Paper mode can simulate order placement
    paper_order_supported: bool = True
    # Live mode can submit orders (False = unsupported despite valid spec)
    live_order_supported: bool = True

    # Headers / query param naming
    api_key_header: str = ""
    signature_header: str = ""
    signature_param: str = ""
    timestamp_header: str = ""
    timestamp_param: str = ""
    passphrase_header: str = ""
    recv_window_header: str = ""

    # Timestamp format: False = epoch millis, True = ISO 8601 (OKX requires this)
    use_iso8601_timestamp: bool = False

    # Symbol normalization: function to convert LightFee symbol to venue symbol
    symbol_to_venue: Optional[Callable[[str], str]] = None
    symbol_from_venue: Optional[Callable[[str], str]] = None


def binance_spec() -> VenueSpec:
    return VenueSpec(
        venue_id=Venue.BINANCE,
        public_base_url="https://fapi.binance.com",
        private_base_url="https://fapi.binance.com",
        auth_scheme=AuthScheme.HMAC_SHA256_HEX,
        account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
        quantity_step=0.001,
        contract_size=1.0,
        min_quantity=0.001,
        min_notional=5.0,
        price_tick=0.01,
        market_snapshot_path="/fapi/v1/ticker/bookTicker",
        position_path="/fapi/v2/positionRisk",
        order_path="/fapi/v1/order",
        account_risk_path="/fapi/v3/account",
        l2_snapshot_path="/fapi/v1/depth",
        api_key_header="X-MBX-APIKEY",
        signature_param="signature",
        timestamp_param="timestamp",
        symbol_to_venue=lambda s: s,  # Binance USDM wire format matches canonical
        symbol_from_venue=lambda s: s,
    )


def okx_spec() -> VenueSpec:
    return VenueSpec(
        venue_id=Venue.OKX,
        public_base_url="https://www.okx.com",
        private_base_url="https://www.okx.com",
        auth_scheme=AuthScheme.HMAC_SHA256_BASE64,
        account_contract=VenueAccountContract.UNIFIED_ACCOUNT,
        quantity_step=0.01,
        contract_size=1.0,
        min_quantity=0.01,
        min_notional=1.0,
        price_tick=0.01,
        market_snapshot_path="/api/v5/market/tickers",
        position_path="/api/v5/account/positions",
        order_path="/api/v5/trade/order",
        account_risk_path="/api/v5/account/balance",
        l2_snapshot_path="/api/v5/market/books",
        requires_passphrase=True,
        api_key_header="OK-ACCESS-KEY",
        signature_header="OK-ACCESS-SIGN",
        timestamp_header="OK-ACCESS-TIMESTAMP",
        passphrase_header="OK-ACCESS-PASSPHRASE",
        use_iso8601_timestamp=True,
        symbol_to_venue=lambda s: s.replace("USDT", "-USDT-SWAP"),
        symbol_from_venue=lambda s: s.replace("-USDT-SWAP", "USDT").replace("-SWAP", ""),
    )


def bybit_spec() -> VenueSpec:
    return VenueSpec(
        venue_id=Venue.BYBIT,
        public_base_url="https://api.bybit.com",
        private_base_url="https://api.bybit.com",
        auth_scheme=AuthScheme.HMAC_SHA256_HEX,
        account_contract=VenueAccountContract.UNIFIED_ACCOUNT,
        quantity_step=0.001,
        contract_size=1.0,
        min_quantity=0.001,
        min_notional=1.0,
        price_tick=0.01,
        market_snapshot_path="/v5/market/tickers",
        position_path="/v5/position/list",
        order_path="/v5/order/create",
        account_risk_path="/v5/account/wallet-balance",
        l2_snapshot_path="/v5/market/orderbook",
        api_key_header="X-BAPI-API-KEY",
        signature_header="X-BAPI-SIGN",
        timestamp_header="X-BAPI-TIMESTAMP",
        recv_window_header="X-BAPI-RECV-WINDOW",
        symbol_to_venue=lambda s: s,  # Bybit V5 linear wire format matches canonical
        symbol_from_venue=lambda s: s,
    )


def bitget_spec() -> VenueSpec:
    return VenueSpec(
        venue_id=Venue.BITGET,
        public_base_url="https://api.bitget.com",
        private_base_url="https://api.bitget.com",
        auth_scheme=AuthScheme.HMAC_SHA256_BASE64,
        account_contract=VenueAccountContract.DETECT_CLASSIC_VS_UTA,
        quantity_step=0.001,
        contract_size=1.0,
        min_quantity=0.001,
        min_notional=5.0,
        price_tick=0.01,
        # V2 API paths (Bitget V1 /api/mix/v1/ decommissioned 2026-05)
        market_snapshot_path="/api/v2/mix/market/tickers",
        position_path="/api/v2/mix/position/all-position",
        order_path="/api/v2/mix/order/place-order",
        account_risk_path="/api/v2/mix/account/account",
        l2_snapshot_path="/api/v2/mix/market/orderbook",
        requires_passphrase=True,
        api_key_header="ACCESS-KEY",
        signature_header="ACCESS-SIGN",
        timestamp_header="ACCESS-TIMESTAMP",
        passphrase_header="ACCESS-PASSPHRASE",
        symbol_to_venue=lambda s: s,  # Bitget USDT-FUTURES wire format matches canonical
        symbol_from_venue=lambda s: s,
    )


def gate_spec() -> VenueSpec:
    return VenueSpec(
        venue_id=Venue.GATE,
        public_base_url="https://api.gateio.ws",
        private_base_url="https://api.gateio.ws",
        auth_scheme=AuthScheme.HMAC_SHA512_HEX,
        account_contract=VenueAccountContract.DUAL_POSITION_MODE,
        quantity_step=1.0,
        contract_size=1.0,
        min_quantity=1.0,
        min_notional=1.0,
        price_tick=0.01,
        market_snapshot_path="/api/v4/futures/usdt/tickers",
        position_path="/api/v4/futures/usdt/positions",
        order_path="/api/v4/futures/usdt/orders",
        account_risk_path="/api/v4/futures/usdt/accounts",
        l2_snapshot_path="/api/v4/futures/usdt/order_book",
        api_key_header="KEY",
        signature_header="SIGN",
        timestamp_header="Timestamp",
        symbol_to_venue=lambda s: s.replace("USDT", "_USDT") if "_" not in s else s,
        symbol_from_venue=lambda s: s.replace("_USDT", "USDT").replace("_", ""),
    )


def aster_spec() -> VenueSpec:
    return VenueSpec(
        venue_id=Venue.ASTER,
        public_base_url="https://fapi.aster.exchange",
        private_base_url="https://fapi.aster.exchange",
        auth_scheme=AuthScheme.HMAC_SHA256_HEX,
        account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
        quantity_step=0.001,
        contract_size=1.0,
        min_quantity=0.001,
        min_notional=5.0,
        price_tick=0.01,
        market_snapshot_path="/fapi/v1/ticker/bookTicker",
        position_path="/fapi/v1/positionRisk",
        order_path="/fapi/v1/order",
        account_risk_path="/fapi/v4/account",
        l2_snapshot_path="/fapi/v1/depth",
        api_key_header="X-MBX-APIKEY",
        signature_param="signature",
        timestamp_param="timestamp",
        symbol_to_venue=lambda s: s,  # Aster (Binance-compatible) wire format matches canonical
        symbol_from_venue=lambda s: s,
    )


def hyperliquid_spec() -> VenueSpec:
    return VenueSpec(
        venue_id=Venue.HYPERLIQUID,
        public_base_url="https://api.hyperliquid.xyz",
        private_base_url="https://api.hyperliquid.xyz",
        auth_scheme=AuthScheme.EIP712,
        account_contract=VenueAccountContract.NATIVE_PERP_ACCOUNT,
        quantity_step=1.0,
        contract_size=1.0,
        min_quantity=1.0,
        min_notional=10.0,
        price_tick=0.01,
        market_snapshot_path="/info",
        position_path="/info",
        order_path="/exchange",
        l2_snapshot_path="/info",  # POST {"type": "l2Book", "coin": "BTC"}
        requires_wallet_key=True,
        live_order_supported=True,
        symbol_to_venue=lambda s: s.replace("USDT", "").replace("usdt", ""),
        symbol_from_venue=lambda s: s + "USDT" if "USDT" not in s.upper() else s,
    )


SPEC_REGISTRY: dict[Venue, Callable[[], VenueSpec]] = {
    Venue.BINANCE: binance_spec,
    Venue.OKX: okx_spec,
    Venue.BYBIT: bybit_spec,
    Venue.BITGET: bitget_spec,
    Venue.GATE: gate_spec,
    Venue.ASTER: aster_spec,
    Venue.HYPERLIQUID: hyperliquid_spec,
}


def get_spec(venue: Venue) -> VenueSpec:
    return SPEC_REGISTRY[venue]()
