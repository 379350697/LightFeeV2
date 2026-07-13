"""Core domain types for LightFee - funding-rate arbitrage execution engine."""

from __future__ import annotations

import math

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class Venue(Enum):
    """Trading venue identifier."""

    BINANCE = "binance"
    OKX = "okx"
    BYBIT = "bybit"
    BITGET = "bitget"
    GATE = "gate"
    ASTER = "aster"
    HYPERLIQUID = "hyperliquid"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_str(cls, value: str) -> Venue:
        v = value.strip().lower()
        if v in ("gateio", "gate_io"):
            v = "gate"
        try:
            return cls(v)
        except ValueError:
            raise ValueError(f"unsupported venue: {value}")


@dataclass(frozen=True)
class EntryLeverageEvidence:
    """Exact, pre-entry leverage evidence returned by a private venue adapter.

    A configured target is merely an intent: venue brackets can lower it and
    an accepted set-leverage response is not proof unless the effective value
    is explicit.  Consumers must use ``evidence_complete`` before treating
    ``effective_leverage`` as a sizing input.
    """

    venue: Venue
    symbol: str
    requested_leverage: int
    effective_leverage: int
    notional_quote: float
    bracket_verified: bool
    account_verified: bool
    source: str
    observed_at_ms: int
    # Exact account setting read before an entry-preparation mutation.  This
    # is distinct from ``effective_leverage``: sizing may cap the latter at
    # the target/bracket, while compensation needs the original setting.
    account_leverage: int = 0

    @property
    def evidence_complete(self) -> bool:
        return (
            self.requested_leverage > 0
            and self.effective_leverage > 0
            and self.effective_leverage <= self.requested_leverage
            and math.isfinite(self.notional_quote)
            and self.notional_quote >= 0.0
            and self.bracket_verified
            and self.account_verified
        )


class Symbol:
    """Normalized trading symbol (always uppercase)."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        v = value.strip()
        if not v:
            raise ValueError("symbol must not be blank")
        self._value = v.upper()

    def __str__(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return f"Symbol({self._value!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Symbol):
            return self._value == other._value
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._value)

    def __lt__(self, other: Symbol) -> bool:
        return self._value < other._value


class Side(Enum):
    """Order/trade side."""

    BUY = "buy"
    SELL = "sell"

    def opposite(self) -> Side:
        return Side.SELL if self == Side.BUY else Side.BUY

    def signed_qty(self, quantity: float) -> float:
        return quantity if self == Side.BUY else -quantity


class TimeInForce(Enum):
    """Order time-in-force. V1 parity: GTC for maker, IOC for hedge/close."""

    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    POST_ONLY = "post_only"  # GTX on some venues


class FundingOpportunityType(Enum):
    """Classification of funding-rate arbitrage opportunity timing."""

    ALIGNED = "aligned"
    STAGGERED = "staggered"


class FundingLeg(Enum):
    """Which side of a funding-rate pair a leg represents."""

    LONG = "long"
    SHORT = "short"


@dataclass(frozen=True, slots=True)
class FundingSnapshot:
    venue: Venue
    symbol: str
    funding_rate_bps: float
    funding_timestamp_ms: int
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    venue: Venue
    symbol: str
    bid: float
    ask: float
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    venue: Venue
    symbol: str
    side: Side
    quantity: float
    entry_price: float
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class OrderRequest:
    venue: Venue
    symbol: str
    side: Side
    quantity: float
    price: Optional[float] = None
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    post_only: bool = False
    time_in_force: Optional[TimeInForce] = None
    # --- Rust V1 live-path fields for execution quality and timing ---
    price_hint: Optional[float] = None
    mark_price_hint: Optional[float] = None
    observed_at_ms: Optional[int] = None
    order_id: str = ""  # exchange-assigned order ID for amend/cancel


@dataclass(frozen=True, slots=True)
class OrderFill:
    venue: Venue
    symbol: str
    side: Side
    quantity: float
    price: float
    order_id: str = ""
    client_order_id: Optional[str] = None
    fee_quote: Optional[float] = None
    filled_at_ms: int = 0


@dataclass(frozen=True, slots=True)
class OrderFillReconciliation:
    venue: Venue
    symbol: str
    side: Side
    quantity: float
    average_price: float
    order_id: str = ""
    client_order_id: Optional[str] = None
    fee_quote: Optional[float] = None
    filled_at_ms: int = 0
    metadata: Optional[dict] = field(default=None)


@dataclass(frozen=True, slots=True)
class FundingSettlement:
    """One immutable funding cash-flow fact from a private exchange statement.

    The amount is expressed in the strategy's quote currency and preserves the
    exchange sign: positive means the account received funding, negative means
    it paid funding.  This is intentionally an account-level fact, not an
    internal-position attribution.  The reconciliation layer allocates it only
    when one position can be proven to own the exact venue/symbol/settlement
    target.
    """

    venue: Venue
    symbol: str
    settlement_timestamp_ms: int
    amount_quote: float
    quote_currency: str
    observed_at_ms: int
    source: str
    statement_reference: str
    account_reference: str = ""
    metadata: Optional[dict] = field(default=None)

    def __post_init__(self) -> None:
        if not str(self.symbol).strip():
            raise ValueError("funding settlement symbol is required")
        if int(self.settlement_timestamp_ms or 0) <= 0:
            raise ValueError("funding settlement timestamp must be positive")
        if not math.isfinite(float(self.amount_quote)):
            raise ValueError("funding settlement amount must be finite")
        if not str(self.quote_currency).strip():
            raise ValueError("funding settlement quote currency is required")
        if int(self.observed_at_ms or 0) <= 0:
            raise ValueError("funding settlement observation timestamp must be positive")
        if not str(self.source).strip():
            raise ValueError("funding settlement source is required")
        if not str(self.statement_reference).strip():
            raise ValueError("funding settlement statement reference is required")


class OrderFillProbeStatus(Enum):
    """Explicit exchange-truth status for order fill probes."""

    CONFIRMED_FILL = "confirmed_fill"
    CONFIRMED_NO_FILL = "confirmed_no_fill"
    UNAVAILABLE = "unavailable"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class OrderFillProbeResult:
    status: OrderFillProbeStatus
    venue: Venue
    symbol: str
    order_id: str = ""
    client_order_id: str = ""
    reconciliation: Optional[OrderFillReconciliation] = None
    metadata: Optional[dict] = field(default=None)
    error: str = ""


@dataclass(frozen=True, slots=True)
class VenueMarketQuote:
    symbol: str
    bid: float
    ask: float
    bid_size: float = 0.0
    ask_size: float = 0.0


@dataclass(frozen=True, slots=True)
class VenueMarketSnapshot:
    venue: Venue
    observed_at_ms: int
    quotes: tuple[VenueMarketQuote, ...] = ()


@dataclass(frozen=True, slots=True)
class AccountBalanceSnapshot:
    venue: Venue
    asset: str
    free: float
    locked: float
    observed_at_ms: int
    balance_classification: str = ""
    user_abstraction: str = ""
    spot_usdc_available: float | None = None


@dataclass(frozen=True, slots=True)
class AccountRiskSnapshot:
    venue: Venue
    total_equity_usd: float
    total_margin_usd: float
    total_health_ratio: float
    observed_at_ms: int


@dataclass(frozen=True, slots=True)
class PerpLiquiditySnapshot:
    venue: Venue
    symbol: str
    bid_depth: list[tuple[float, float]] = field(default_factory=list)
    ask_depth: list[tuple[float, float]] = field(default_factory=list)
    observed_at_ms: int = 0


@dataclass(frozen=True, slots=True)
class ExecutionLiquiditySnapshot:
    venue: Venue
    symbol: str
    observed_at_ms: int
    best_bid: float = 0.0
    bid_size: float = 0.0
    best_ask: float = 0.0
    ask_size: float = 0.0


@dataclass(frozen=True, slots=True)
class AssetTransferStatus:
    asset: str
    from_venue: Venue
    to_venue: Venue
    available: float
    observed_at_ms: int


# ---------------------------------------------------------------------------
# Passive order domain types (V1 parity: resting-order lifecycle)
# ---------------------------------------------------------------------------


class PassiveOrderState(Enum):
    """V1 PassiveOrderState: lifecycle of a resting passive order."""
    UNKNOWN = "unknown"            # V1: state not yet determined (fresh ack, uncertain)
    CREATED = "created"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"

    def is_terminal(self) -> bool:
        """True if the order has reached a terminal state."""
        return self in (
            PassiveOrderState.FILLED,
            PassiveOrderState.CANCELED,
            PassiveOrderState.REJECTED,
            PassiveOrderState.EXPIRED,
        )

    def is_active(self) -> bool:
        """True if the order is actively resting on the book."""
        return self in (
            PassiveOrderState.UNKNOWN,
            PassiveOrderState.CREATED,
            PassiveOrderState.OPEN,
            PassiveOrderState.PARTIALLY_FILLED,
        )


@dataclass(frozen=True, slots=True)
class PassiveOrderAck:
    """V1 acknowledgement of a passive order submission (not a terminal fill).

    client_order_id may be absent (empty string) when the venue does not echo it.
    progress fields are optional for venues that ack without fill data.
    """
    venue: Venue
    symbol: str
    side: Side
    order_id: str
    client_order_id: str = ""  # V1: optional — venue may not echo client_order_id
    price: float = 0.0
    quantity: float = 0.0       # resting quantity (may differ from requested)
    accepted_at_ms: int = 0
    state: PassiveOrderState = PassiveOrderState.UNKNOWN

    @property
    def resting_quantity(self) -> float:
        """V1: quantity currently resting on the book (may be less than requested)."""
        return self.quantity


@dataclass(frozen=True, slots=True)
class PassiveOrderProgress:
    """V1 cumulative progress for a resting passive order.

    client_order_id is optional — venues may not echo it in progress updates.
    cumulative_quantity tracks total filled quantity since submission.
    """
    venue: Venue
    symbol: str
    side: Side
    order_id: str
    client_order_id: str = ""  # V1: optional — venue may not include in progress updates
    cumulative_quantity: float = 0.0
    average_price: float = 0.0
    fee_quote: float = 0.0
    last_fill_time_ms: int = 0
    state: PassiveOrderState = PassiveOrderState.UNKNOWN
    observed_at_ms: int = 0
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def remaining_quantity(self) -> float:
        """V1: estimated remaining quantity on the book."""
        return 0.0  # Subclasses or callers override with actual resting qty


@dataclass(frozen=True, slots=True)
class PassiveOrderAmendRequest:
    """V1 request to amend a resting passive order."""
    symbol: str
    side: Side
    order_id: str
    client_order_id: Optional[str] = None
    new_client_order_id: Optional[str] = None
    new_price_hint: Optional[float] = None
    new_quantity: Optional[float] = None
