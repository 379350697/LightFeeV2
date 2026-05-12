"""Venue capability system — explicit capability objects for V1 semantic parity.

Replaces hasattr() checks with structured CapabilityFlags that drive
behavior in engine code. Each venue declares its capabilities via
a CapabilityMatrix that must match V1 unless an approved deviation exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from lightfee.core.domain import Venue
from lightfee.venues.base import (
    CapabilitySupport,
    ExecutionLiquidityCapability,
    ReconcileQuality,
    TestnetSupport,
    VenueAccountContract,
    VenueCapabilities,
    VenueMarketApiContract,
    VenuePrivateApiContract,
)


class PassiveProgressMode(Enum):
    """How a venue reports passive order progress (fill updates)."""
    ORDER_FILL_STREAM = "order_fill_stream"       # Private WS fill events
    PERIODIC_POLL = "periodic_poll"               # REST polling
    UNSUPPORTED = "unsupported"


class TransferCapability(Enum):
    """Whether a venue supports asset transfer queries."""
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"


class OrderSizingMode(Enum):
    """How the venue defines order sizing constraints."""
    CONTRACT_SIZE_STEP = "contract_size_step"     # qty_step from spec
    NOTIONAL_MIN = "notional_min"                  # min notional only
    NATIVE_ROUND_LOT = "native_round_lot"          # Venue-specific round lot


@dataclass(frozen=True)
class VenueCapabilityFlags:
    """Explicit capability flags for a venue — drives engine behavior.

    Used by engine code instead of hasattr() checks on adapters.
    Every flag maps to a V1 venue capability declaration.
    """

    venue: Venue

    # --- Health ---
    risk_health: CapabilitySupport = CapabilitySupport.UNSUPPORTED
    private_health: CapabilitySupport = CapabilitySupport.UNSUPPORTED
    cached_private_health: bool = False  # V1: caches last successful private health for fail-closed

    # --- Execution ---
    execution_liquidity: ExecutionLiquidityCapability = ExecutionLiquidityCapability.FALLBACK_ONLY
    reconcile_quality: ReconcileQuality = ReconcileQuality.UNSUPPORTED
    live_order_supported: bool = True

    # --- Passive orders ---
    passive_order_supported: bool = True
    passive_progress_mode: PassiveProgressMode = PassiveProgressMode.UNSUPPORTED
    passive_wakeups: bool = False  # V1: WS events can wake the maker-event lane
    passive_metadata: bool = True  # V1: venue provides metadata (min notional, tick size)

    # --- Order sizing ---
    order_sizing_mode: OrderSizingMode = OrderSizingMode.CONTRACT_SIZE_STEP
    entry_open_notional_headroom: bool = True  # V1: can query open notional before entry

    # --- Transfer ---
    transfer_status: TransferCapability = TransferCapability.UNSUPPORTED

    # --- Market data ---
    market_data_activity_control: bool = True   # V1: adapter can pause/resume market data
    local_l2_supported: bool = True              # V1: adapter supports local-L2 book
    local_l2_reconcile_targets: bool = True      # V1: provides reconcile targets for L2

    # --- Live startup ---
    live_startup_activation: bool = True          # V1: adapter can activate on live startup
    prewarm_supported: bool = True                # V1: adapter supports prewarm before trading
    shutdown_supported: bool = True               # V1: adapter has graceful shutdown

    # --- Worker management ---
    worker_status_supported: bool = True          # V1: adapter reports worker category status
    worker_categories: tuple[str, ...] = ()       # V1: WsWorkerCategory entries

    # --- Connectivity ---
    testnet_support: TestnetSupport = TestnetSupport.UNKNOWN

    # --- API contracts (for diagnostics) ---
    market_api_contract: Optional[VenueMarketApiContract] = None
    private_api_contract: Optional[VenuePrivateApiContract] = None
    account_contract: Optional[VenueAccountContract] = None

    # --- Symbol support ---
    supported_symbols_max: int = 0  # 0 = no explicit limit

    def supports(self, flag_name: str) -> bool:
        """Check a boolean capability by name."""
        val = getattr(self, flag_name, None)
        if isinstance(val, bool):
            return val
        if isinstance(val, CapabilitySupport):
            return val.is_supported()
        return False


# ---------------------------------------------------------------------------
# Full capability matrix — must match V1 unless an approved deviation exists
# ---------------------------------------------------------------------------


def capability_matrix() -> dict[Venue, VenueCapabilityFlags]:
    """Return the full capability matrix for all 7 venues.

    Each entry corresponds to V1 declarations in src/market_gateway/capability_ports.rs.
    """
    return {
        Venue.BINANCE: VenueCapabilityFlags(
            venue=Venue.BINANCE,
            risk_health=CapabilitySupport.SUPPORTED,
            private_health=CapabilitySupport.SUPPORTED,
            cached_private_health=True,
            execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
            reconcile_quality=ReconcileQuality.ORDER_FILL,
            passive_progress_mode=PassiveProgressMode.ORDER_FILL_STREAM,
            passive_wakeups=True,
            order_sizing_mode=OrderSizingMode.CONTRACT_SIZE_STEP,
            transfer_status=TransferCapability.UNSUPPORTED,
            testnet_support=TestnetSupport.SUPPORTED,
            market_api_contract=VenueMarketApiContract.BINANCE_USDM_REST,
            private_api_contract=VenuePrivateApiContract.BINANCE_USDM_PRIVATE_V3,
            account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            worker_categories=("market_local_l2",),
        ),
        Venue.OKX: VenueCapabilityFlags(
            venue=Venue.OKX,
            risk_health=CapabilitySupport.SUPPORTED,
            private_health=CapabilitySupport.SUPPORTED,
            cached_private_health=True,
            execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
            reconcile_quality=ReconcileQuality.ORDER_FILL,
            passive_progress_mode=PassiveProgressMode.ORDER_FILL_STREAM,
            passive_wakeups=True,
            order_sizing_mode=OrderSizingMode.CONTRACT_SIZE_STEP,
            transfer_status=TransferCapability.UNSUPPORTED,
            testnet_support=TestnetSupport.SUPPORTED,
            market_api_contract=VenueMarketApiContract.OKX_V5,
            private_api_contract=VenuePrivateApiContract.OKX_V5,
            account_contract=VenueAccountContract.UNIFIED_ACCOUNT,
            worker_categories=("market_local_l2",),
        ),
        Venue.BYBIT: VenueCapabilityFlags(
            venue=Venue.BYBIT,
            risk_health=CapabilitySupport.SUPPORTED,
            private_health=CapabilitySupport.SUPPORTED,
            cached_private_health=True,
            execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
            reconcile_quality=ReconcileQuality.ORDER_FILL,
            passive_progress_mode=PassiveProgressMode.ORDER_FILL_STREAM,
            passive_wakeups=True,
            order_sizing_mode=OrderSizingMode.CONTRACT_SIZE_STEP,
            transfer_status=TransferCapability.UNSUPPORTED,
            testnet_support=TestnetSupport.SUPPORTED,
            market_api_contract=VenueMarketApiContract.BYBIT_V5,
            private_api_contract=VenuePrivateApiContract.BYBIT_V5,
            account_contract=VenueAccountContract.UNIFIED_ACCOUNT,
            worker_categories=("market_local_l2",),
        ),
        Venue.BITGET: VenueCapabilityFlags(
            venue=Venue.BITGET,
            risk_health=CapabilitySupport.UNSUPPORTED,  # V1 parity: Bitget risk_health unsupported
            private_health=CapabilitySupport.SUPPORTED,
            execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
            reconcile_quality=ReconcileQuality.UNSUPPORTED,
            passive_progress_mode=PassiveProgressMode.PERIODIC_POLL,
            order_sizing_mode=OrderSizingMode.CONTRACT_SIZE_STEP,
            transfer_status=TransferCapability.UNSUPPORTED,
            testnet_support=TestnetSupport.UNKNOWN,
            market_api_contract=VenueMarketApiContract.BITGET_MARKET_V3,
            private_api_contract=VenuePrivateApiContract.BITGET_MIX_PRIVATE_V2,
            account_contract=VenueAccountContract.DETECT_CLASSIC_VS_UTA,
        ),
        Venue.GATE: VenueCapabilityFlags(
            venue=Venue.GATE,
            risk_health=CapabilitySupport.UNSUPPORTED,  # V1 parity: Gate risk_health unsupported
            private_health=CapabilitySupport.SUPPORTED,
            execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
            reconcile_quality=ReconcileQuality.UNSUPPORTED,
            passive_progress_mode=PassiveProgressMode.PERIODIC_POLL,
            order_sizing_mode=OrderSizingMode.CONTRACT_SIZE_STEP,
            transfer_status=TransferCapability.UNSUPPORTED,
            testnet_support=TestnetSupport.UNKNOWN,
            market_api_contract=VenueMarketApiContract.GATE_FUTURES_V4,
            private_api_contract=VenuePrivateApiContract.GATE_FUTURES_V4,
            account_contract=VenueAccountContract.DUAL_POSITION_MODE,
        ),
        Venue.ASTER: VenueCapabilityFlags(
            venue=Venue.ASTER,
            risk_health=CapabilitySupport.SUPPORTED,
            private_health=CapabilitySupport.SUPPORTED,
            cached_private_health=True,
            execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
            reconcile_quality=ReconcileQuality.ORDER_FILL,
            passive_progress_mode=PassiveProgressMode.ORDER_FILL_STREAM,
            passive_wakeups=True,
            order_sizing_mode=OrderSizingMode.CONTRACT_SIZE_STEP,
            transfer_status=TransferCapability.UNSUPPORTED,
            testnet_support=TestnetSupport.UNKNOWN,
            market_api_contract=VenueMarketApiContract.ASTER_PERPETUALS_FAPI,
            private_api_contract=VenuePrivateApiContract.ASTER_BALANCE_V2,
            account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            worker_categories=("market_local_l2",),
        ),
        Venue.HYPERLIQUID: VenueCapabilityFlags(
            venue=Venue.HYPERLIQUID,
            risk_health=CapabilitySupport.UNSUPPORTED,
            private_health=CapabilitySupport.SUPPORTED,
            execution_liquidity=ExecutionLiquidityCapability.TRUE_L2,
            reconcile_quality=ReconcileQuality.UNSUPPORTED,
            passive_progress_mode=PassiveProgressMode.PERIODIC_POLL,
            order_sizing_mode=OrderSizingMode.NATIVE_ROUND_LOT,
            entry_open_notional_headroom=False,
            transfer_status=TransferCapability.UNSUPPORTED,
            live_order_supported=True,
            testnet_support=TestnetSupport.UNKNOWN,
            market_api_contract=VenueMarketApiContract.HYPERLIQUID_INFO_API,
            private_api_contract=VenuePrivateApiContract.HYPERLIQUID_EXCHANGE_API,
            account_contract=VenueAccountContract.NATIVE_PERP_ACCOUNT,
        ),
    }


def get_capability_flags(venue: Venue) -> VenueCapabilityFlags:
    """Get the capability flags for a venue."""
    return capability_matrix()[venue]


# ---------------------------------------------------------------------------
# Facade: Order sizing contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderSizingSpec:
    """V1-compatible order sizing specification for a venue/symbol pair."""

    venue: Venue
    symbol: str
    quantity_step: float
    min_quantity: float
    min_notional: float
    contract_size: float = 1.0
    price_tick: float = 0.0
    max_quantity: float = 0.0  # 0 = no explicit limit

    def normalize_quantity(self, quantity: float) -> float:
        """Floor quantity to the venue's quantity step."""
        if self.quantity_step <= 0:
            return quantity
        steps = int(quantity / self.quantity_step)
        return max(0.0, steps * self.quantity_step)

    def check_min_notional(self, quantity: float, price: float) -> bool:
        """Check if the order meets the venue's minimum notional."""
        return (quantity * price) >= self.min_notional

    def available_headroom(
        self,
        position_qty: float,
        max_position: float = 0.0,
    ) -> float:
        """Calculate remaining notional headroom for a position."""
        if max_position <= 0:
            return float("inf")
        return max(0.0, max_position - abs(position_qty))
