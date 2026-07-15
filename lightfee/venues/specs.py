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


class VenueOperation(Enum):
    CREATE_ORDER = "create_order"
    AMEND_ORDER = "amend_order"
    CANCEL_ORDER = "cancel_order"
    ORDER_STATUS = "order_status"
    ORDER_HISTORY = "order_history"
    EXECUTION_HISTORY = "execution_history"
    OPEN_ORDERS = "open_orders"
    POSITION = "position"
    ALL_POSITIONS = "all_positions"
    ACCOUNT_RISK = "account_risk"
    L2_BOOK = "l2_book"
    INFO = "info"
    USER_ABSTRACTION = "user_abstraction"
    SPOT_CLEARINGHOUSE_STATE = "spot_clearinghouse_state"


class BitgetContractFamily(Enum):
    CLASSIC_MIX_V2 = "classic_mix_v2"
    UTA_V3 = "uta_v3"


@dataclass(frozen=True)
class VenueOperationContract:
    method: str
    path: str
    private: bool = True
    payload: str = "body"
    supported: bool = True
    required_params: tuple[str, ...] = ()
    symbol_shape: str = "canonical"
    official_doc_url: str = ""


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
    operation_contracts: dict[VenueOperation, VenueOperationContract] = field(default_factory=dict)
    family_operation_contracts: dict[Enum, dict[VenueOperation, VenueOperationContract]] = field(default_factory=dict)

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

    # V1 transport metadata (Task 4)
    server_time_path: str = ""
    server_time_safety_margin_ms: int = 0
    recv_window_ms: int | None = None
    venue_scope: str = ""
    rest_group_scope: str = "group:rest"
    endpoint_scope_map: dict[str, str] = field(default_factory=dict)
    endpoint_weights: dict[str, int] = field(default_factory=dict)
    endpoint_min_interval_ms: dict[str, int] = field(default_factory=dict)

    # V2 sidecar public endpoint paths
    funding_ticker_path: str = ""
    funding_rate_path: str = ""
    funding_contracts_path: str = ""
    premium_index_path: str = ""
    volume_24h_path: str = ""
    open_interest_path: str = ""
    transfer_status_path: str = ""
    ticker_includes_volume_oi: bool = False


def _contract(
    method: str,
    path: str,
    *,
    private: bool = True,
    payload: str = "body",
    supported: bool = True,
    required_params: tuple[str, ...] = (),
    symbol_shape: str = "canonical",
    official_doc_url: str = "",
) -> VenueOperationContract:
    return VenueOperationContract(
        method=method.upper(),
        path=path,
        private=private,
        payload=payload,
        supported=supported,
        required_params=required_params,
        symbol_shape=symbol_shape,
        official_doc_url=official_doc_url,
    )


def _unsupported_contract(reason: str = "unsupported") -> VenueOperationContract:
    return _contract("", "", supported=False, official_doc_url=reason)


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
        # V1 transport metadata
        server_time_path="/fapi/v1/time",
        server_time_safety_margin_ms=1000,
        recv_window_ms=10000,
        venue_scope="venue:binance",
        # V2 sidecar public endpoints
        funding_ticker_path="/fapi/v1/ticker/bookTicker",
        funding_contracts_path="/fapi/v1/exchangeInfo",
        premium_index_path="/fapi/v1/premiumIndex",
        volume_24h_path="/fapi/v1/ticker/24hr",
        open_interest_path="/fapi/v1/openInterest",
        operation_contracts={
            VenueOperation.CREATE_ORDER: _contract("POST", "/fapi/v1/order", payload="params"),
            VenueOperation.AMEND_ORDER: _contract("PUT", "/fapi/v1/order", payload="params"),
            VenueOperation.CANCEL_ORDER: _contract("DELETE", "/fapi/v1/order", payload="params"),
            VenueOperation.ORDER_STATUS: _contract("GET", "/fapi/v1/order", payload="params"),
            VenueOperation.OPEN_ORDERS: _contract("GET", "/fapi/v1/openOrders", payload="params"),
            VenueOperation.POSITION: _contract("GET", "/fapi/v2/positionRisk", payload="params"),
        },
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
        # V1 transport metadata
        server_time_path="/api/v5/public/time",
        venue_scope="venue:okx",
        # V2 sidecar public endpoints
        funding_ticker_path="/api/v5/market/tickers",
        funding_rate_path="/api/v5/public/funding-rate",
        open_interest_path="/api/v5/public/open-interest",
        operation_contracts={
            VenueOperation.CREATE_ORDER: _contract("POST", "/api/v5/trade/order"),
            VenueOperation.AMEND_ORDER: _contract(
                "POST",
                "/api/v5/trade/amend-order",
                official_doc_url="https://www.okx.com/docs-v5/en/#order-book-trading-trade-amend-order",
            ),
            VenueOperation.CANCEL_ORDER: _contract("POST", "/api/v5/trade/cancel-order"),
            VenueOperation.ORDER_STATUS: _contract("GET", "/api/v5/trade/order", payload="params"),
            VenueOperation.ORDER_HISTORY: _contract("GET", "/api/v5/trade/orders-history", payload="params"),
            VenueOperation.EXECUTION_HISTORY: _contract("GET", "/api/v5/trade/fills-history", payload="params"),
            VenueOperation.OPEN_ORDERS: _contract("GET", "/api/v5/trade/orders-pending", payload="params"),
            VenueOperation.POSITION: _contract("GET", "/api/v5/account/positions", payload="params"),
        },
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
        # V1 transport metadata
        server_time_path="/v5/market/time",
        recv_window_ms=5000,
        venue_scope="venue:bybit",
        # V2 sidecar public endpoints
        funding_ticker_path="/v5/market/tickers",
        funding_contracts_path="/v5/market/instruments-info",
        ticker_includes_volume_oi=True,
        operation_contracts={
            VenueOperation.CREATE_ORDER: _contract("POST", "/v5/order/create"),
            VenueOperation.AMEND_ORDER: _contract(
                "POST",
                "/v5/order/amend",
                official_doc_url="https://bybit-exchange.github.io/docs/v5/order/amend-order",
            ),
            VenueOperation.CANCEL_ORDER: _contract("POST", "/v5/order/cancel"),
            VenueOperation.ORDER_STATUS: _contract("GET", "/v5/order/realtime", payload="params"),
            VenueOperation.ORDER_HISTORY: _contract("GET", "/v5/order/history", payload="params"),
            VenueOperation.EXECUTION_HISTORY: _contract("GET", "/v5/execution/list", payload="params"),
            VenueOperation.OPEN_ORDERS: _contract("GET", "/v5/order/realtime", payload="params"),
            VenueOperation.POSITION: _contract("GET", "/v5/position/list", payload="params"),
        },
    )


def bitget_spec() -> VenueSpec:
    classic_mix_v2_contracts = {
        VenueOperation.CREATE_ORDER: _contract("POST", "/api/v2/mix/order/place-order"),
        VenueOperation.AMEND_ORDER: _unsupported_contract("bitget_amend_cancel_replace_required"),
        VenueOperation.CANCEL_ORDER: _contract(
            "POST",
            "/api/v2/mix/order/cancel-order",
            required_params=("productType=USDT-FUTURES", "marginCoin=USDT"),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.ORDER_STATUS: _contract(
            "GET",
            "/api/v2/mix/order/detail",
            payload="params",
            required_params=("productType=USDT-FUTURES", "marginCoin=USDT"),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.OPEN_ORDERS: _contract(
            "GET",
            "/api/v2/mix/order/orders-pending",
            payload="params",
            required_params=("productType=USDT-FUTURES", "marginCoin=USDT"),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.POSITION: _contract(
            "GET",
            "/api/v2/mix/position/single-position",
            payload="params",
            required_params=("productType=USDT-FUTURES", "marginCoin=USDT"),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.ALL_POSITIONS: _contract(
            "GET",
            "/api/v2/mix/position/all-position",
            payload="params",
            required_params=("productType=USDT-FUTURES", "marginCoin=USDT"),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.ACCOUNT_RISK: _contract(
            "GET",
            "/api/v2/mix/account/accounts",
            payload="params",
            required_params=("productType=USDT-FUTURES",),
        ),
    }
    uta_v3_contracts = {
        VenueOperation.CREATE_ORDER: _contract("POST", "/api/v3/trade/place-order"),
        VenueOperation.AMEND_ORDER: _unsupported_contract("bitget_uta_amend_cancel_replace_required"),
        VenueOperation.CANCEL_ORDER: _contract(
            "POST",
            "/api/v3/trade/cancel-order",
            required_params=("category=USDT-FUTURES",),
            symbol_shape="",
        ),
        VenueOperation.ORDER_STATUS: _contract(
            "GET",
            "/api/v3/trade/order-info",
            payload="params",
            required_params=("category=USDT-FUTURES",),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.OPEN_ORDERS: _contract(
            "GET",
            "/api/v3/trade/unfilled-orders",
            payload="params",
            required_params=("category=USDT-FUTURES",),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.POSITION: _contract(
            "GET",
            "/api/v3/position/current-position",
            payload="params",
            required_params=("category=USDT-FUTURES",),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.ALL_POSITIONS: _contract(
            "GET",
            "/api/v3/position/current-position",
            payload="params",
            required_params=("category=USDT-FUTURES",),
            symbol_shape="BTCUSDT",
        ),
        VenueOperation.ACCOUNT_RISK: _contract(
            "GET",
            "/api/v3/account/assets",
            payload="params",
        ),
    }
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
        # Public market remains v2 mix; private truth is selected by resolved account family.
        market_snapshot_path="/api/v2/mix/market/tickers",
        position_path="/api/v2/mix/position/all-position",
        order_path="/api/v2/mix/order/place-order",
        account_risk_path="/api/v2/mix/account/accounts",
        l2_snapshot_path="/api/v3/market/orderbook",
        requires_passphrase=True,
        api_key_header="ACCESS-KEY",
        signature_header="ACCESS-SIGN",
        timestamp_header="ACCESS-TIMESTAMP",
        passphrase_header="ACCESS-PASSPHRASE",
        symbol_to_venue=lambda s: s,  # Bitget USDT-FUTURES wire format matches canonical
        symbol_from_venue=lambda s: s,
        # V1 transport metadata
        venue_scope="venue:bitget",
        # V2 sidecar public endpoints
        funding_ticker_path="/api/v2/mix/market/tickers",
        funding_rate_path="/api/v2/mix/market/current-fund-rate",
        ticker_includes_volume_oi=True,
        operation_contracts=classic_mix_v2_contracts,
        family_operation_contracts={
            BitgetContractFamily.CLASSIC_MIX_V2: classic_mix_v2_contracts,
            BitgetContractFamily.UTA_V3: uta_v3_contracts,
        },
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
        # V1 transport metadata
        venue_scope="venue:gate",
        # V2 sidecar public endpoints
        funding_ticker_path="/api/v4/futures/usdt/tickers",
        funding_contracts_path="/api/v4/futures/usdt/contracts",
        ticker_includes_volume_oi=True,
        operation_contracts={
            VenueOperation.CREATE_ORDER: _contract("POST", "/api/v4/futures/usdt/orders"),
            VenueOperation.AMEND_ORDER: _unsupported_contract("gate_amend_cancel_replace_required"),
            VenueOperation.CANCEL_ORDER: _contract(
                "DELETE",
                "/api/v4/futures/usdt/orders/{order_id}",
                payload="params",
            ),
            VenueOperation.ORDER_STATUS: _contract("GET", "/api/v4/futures/usdt/orders/{order_id}", payload="params"),
            VenueOperation.OPEN_ORDERS: _contract("GET", "/api/v4/futures/usdt/orders", payload="params"),
            VenueOperation.POSITION: _contract("GET", "/api/v4/futures/usdt/positions", payload="params"),
        },
    )


def aster_spec() -> VenueSpec:
    return VenueSpec(
        venue_id=Venue.ASTER,
        public_base_url="https://fapi.asterdex.com",
        private_base_url="https://fapi.asterdex.com",
        auth_scheme=AuthScheme.EIP712,
        account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
        quantity_step=0.001,
        contract_size=1.0,
        min_quantity=0.001,
        min_notional=5.0,
        price_tick=0.01,
        market_snapshot_path="/fapi/v1/ticker/bookTicker",
        position_path="/fapi/v3/positionRisk",
        order_path="/fapi/v3/order",
        account_risk_path="/fapi/v3/accountWithJoinMargin",
        l2_snapshot_path="/fapi/v1/depth",
        requires_wallet_key=True,
        symbol_to_venue=lambda s: s,  # Aster (Binance-compatible) wire format matches canonical
        symbol_from_venue=lambda s: s,
        # V1 transport metadata
        server_time_path="/fapi/v1/time",
        server_time_safety_margin_ms=0,
        venue_scope="venue:aster",
        # V2 sidecar public endpoints (Binance-compatible)
        funding_ticker_path="/fapi/v1/ticker/bookTicker",
        funding_contracts_path="/fapi/v1/exchangeInfo",
        premium_index_path="/fapi/v1/premiumIndex",
        volume_24h_path="/fapi/v1/ticker/24hr",
        open_interest_path="/fapi/v1/openInterest",
        operation_contracts={
            VenueOperation.CREATE_ORDER: _contract("POST", "/fapi/v3/order", payload="params"),
            VenueOperation.AMEND_ORDER: _unsupported_contract("aster_v3_amend_cancel_replace_required"),
            VenueOperation.CANCEL_ORDER: _contract("DELETE", "/fapi/v3/order", payload="params"),
            VenueOperation.ORDER_STATUS: _contract("GET", "/fapi/v3/order", payload="params"),
            VenueOperation.OPEN_ORDERS: _contract("GET", "/fapi/v3/openOrders", payload="params"),
            VenueOperation.POSITION: _contract("GET", "/fapi/v3/positionRisk", payload="params"),
            VenueOperation.ACCOUNT_RISK: _contract("GET", "/fapi/v3/accountWithJoinMargin", payload="params"),
        },
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
        # V1 transport metadata
        venue_scope="venue:hyperliquid",
        # V2 sidecar public endpoints
        funding_ticker_path="/info",
        ticker_includes_volume_oi=True,
        operation_contracts={
            VenueOperation.CREATE_ORDER: _contract("POST", "/exchange"),
            VenueOperation.AMEND_ORDER: _unsupported_contract("hyperliquid_amend_cancel_replace_required"),
            VenueOperation.CANCEL_ORDER: _contract("POST", "/exchange"),
            VenueOperation.ORDER_STATUS: _contract(
                "POST",
                "/info",
                private=False,
                required_params=("type=orderStatus", "user=configured_account_address"),
                symbol_shape="coin",
            ),
            VenueOperation.OPEN_ORDERS: _contract(
                "POST",
                "/info",
                private=False,
                required_params=("type=openOrders", "user=configured_account_address"),
                symbol_shape="coin",
            ),
            VenueOperation.POSITION: _contract(
                "POST",
                "/info",
                private=False,
                required_params=("type=clearinghouseState", "user=configured_account_address"),
                symbol_shape="coin",
            ),
            VenueOperation.USER_ABSTRACTION: _contract(
                "POST",
                "/info",
                private=False,
                required_params=("type=userAbstraction", "user=configured_account_address"),
                symbol_shape="coin",
            ),
            VenueOperation.SPOT_CLEARINGHOUSE_STATE: _contract(
                "POST",
                "/info",
                private=False,
                required_params=(
                    "type=spotClearinghouseState",
                    "user=configured_account_address",
                ),
                symbol_shape="coin",
            ),
            VenueOperation.L2_BOOK: _contract(
                "POST",
                "/info",
                private=False,
                required_params=("type=l2Book", "coin=coin"),
                symbol_shape="coin",
            ),
        },
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


def get_operation_contract(
    spec: VenueSpec,
    operation: VenueOperation,
    *,
    resolved_account_family: Enum | str | None = None,
) -> VenueOperationContract:
    if resolved_account_family is not None:
        family_key: Enum | str = resolved_account_family
        if isinstance(resolved_account_family, str) and spec.venue_id == Venue.BITGET:
            family_key = BitgetContractFamily(resolved_account_family)
        family_contracts = spec.family_operation_contracts.get(family_key)
        if family_contracts is None:
            return _unsupported_contract(
                f"{spec.venue_id.value}:{resolved_account_family}:{operation.value}:unsupported"
            )
        contract = family_contracts.get(operation)
        if contract is not None:
            return contract
        return _unsupported_contract(
            f"{spec.venue_id.value}:{resolved_account_family}:{operation.value}:unsupported"
        )

    contract = spec.operation_contracts.get(operation)
    if contract is not None:
        return contract
    return _unsupported_contract(f"{spec.venue_id.value}:{operation.value}:unsupported")
