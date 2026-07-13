"""Fake venue adapters for testing entry sync, residual, and execution flows.

Provides controllable adapters that can simulate:
- Full fills, partial fills, rejected submits, uncertain submits
- Delayed reconciliation responses
- Position queries
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    EntryLeverageEvidence,
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass

if TYPE_CHECKING:
    from lightfee.marketdata.l2 import LocalL2Update


@dataclass
class FakeVenueAdapter(VenueAdapter):
    """Programmable fake adapter for testing.

    Configure outcomes via the *_outcome attributes before calling methods.
    Each outcome is consumed once (FIFO queue), then falls back to defaults.
    """

    _venue: Venue
    _min_notional_quote: float = 0.0

    # --- Programmable outcomes (consumed FIFO) ---
    place_order_outcomes: list[OrderFill | OrderSubmitError] = field(default_factory=list)
    submit_passive_order_outcomes: list = field(default_factory=list)
    position_snapshots: list[PositionSnapshot] = field(default_factory=list)

    # --- Default outcomes (used when queue is empty) ---
    default_fill_price: float = 0.0
    default_position_side: Side = Side.BUY
    default_position_qty: float = 0.0
    entry_account_leverage: int = 4
    # A venue bracket can cap executable leverage below the account setting.
    # None means the effective value follows the account setting.
    entry_effective_leverage: int | None = None

    # --- Spy fields ---
    last_request: Optional[OrderRequest] = None
    place_order_call_count: int = 0
    submit_passive_order_call_count: int = 0
    fetch_position_call_count: int = 0

    @property
    def venue(self) -> Venue:
        return self._venue

    async def place_order(self, request: OrderRequest) -> OrderFill:
        self.place_order_call_count += 1
        self.last_request = request

        if self.place_order_outcomes:
            outcome = self.place_order_outcomes.pop(0)
            if isinstance(outcome, OrderSubmitError):
                raise outcome
            return outcome

        # Default: instant fill at default_fill_price or request price
        price = self.default_fill_price if self.default_fill_price > 0 else request.price or 1.0
        return OrderFill(
            venue=self._venue,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=price,
            order_id=f"fake-{self._venue.value}-{self.place_order_call_count}",
            filled_at_ms=1000,
        )

    async def amend_order(self, request: OrderRequest) -> OrderFill:
        """Amend existing order — same as place_order for fake testing."""
        self.last_request = request
        price = self.default_fill_price if self.default_fill_price > 0 else request.price or 1.0
        return OrderFill(
            venue=self._venue,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=price,
            order_id=request.order_id or f"fake-amend-{self._venue.value}",
            filled_at_ms=1000,
        )

    async def cancel_order(self, request: OrderRequest) -> None:
        """Cancel existing order — no-op for fake testing."""
        self.last_request = request
        return None

    async def submit_passive_order(self, request: OrderRequest):
        """Submit a GTC post-only maker order. Returns ack, not fill."""
        from lightfee.core.domain import PassiveOrderAck
        self.submit_passive_order_call_count += 1
        self.last_request = request
        if self.submit_passive_order_outcomes:
            outcome = self.submit_passive_order_outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return PassiveOrderAck(
            venue=self._venue,
            symbol=request.symbol,
            side=request.side,
            order_id=f"passive-{self._venue.value}-{self.submit_passive_order_call_count}",
            client_order_id=request.client_order_id or "",
            price=request.price or 0.0,
            quantity=request.quantity,
            accepted_at_ms=1234,
        )

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        self.fetch_position_call_count += 1
        if self.position_snapshots:
            return self.position_snapshots.pop(0)
        return PositionSnapshot(
            venue=self._venue,
            symbol=symbol,
            side=self.default_position_side,
            quantity=self.default_position_qty,
            entry_price=0.0,
            observed_at_ms=1000,
        )

    async def fetch_order_fill_reconciliation(
        self, symbol: str, order_id: str, client_order_id: Optional[str] = None
    ) -> Optional[OrderFill]:
        # Default: order was filled at the request quantity
        return None  # signal "unknown" - caller should handle

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity

    async def inspect_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> EntryLeverageEvidence:
        """Return verified account truth for live-entry contract tests."""
        return EntryLeverageEvidence(
            venue=self._venue,
            symbol=symbol,
            requested_leverage=int(leverage),
            effective_leverage=self._effective_entry_leverage(),
            notional_quote=float(notional_quote or 0.0),
            bracket_verified=True,
            account_verified=True,
            source="fake_account_leverage",
            observed_at_ms=1000,
            account_leverage=int(self.entry_account_leverage),
        )

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> EntryLeverageEvidence:
        """Model a verified exchange-side leverage update for test adapters."""
        self.entry_account_leverage = int(leverage)
        return EntryLeverageEvidence(
            venue=self._venue,
            symbol=symbol,
            requested_leverage=int(leverage),
            effective_leverage=self._effective_entry_leverage(),
            notional_quote=float(notional_quote or 0.0),
            bracket_verified=True,
            account_verified=True,
            source="fake_ensure_leverage",
            observed_at_ms=1000,
            account_leverage=int(self.entry_account_leverage),
        )

    def _effective_entry_leverage(self) -> int:
        if self.entry_effective_leverage is not None:
            return int(self.entry_effective_leverage)
        return int(self.entry_account_leverage)

    async def fetch_l2_snapshot(self, symbol: str, depth: int = 50) -> "LocalL2Update":
        """Return a fake L2 snapshot for testing."""
        from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind, PriceLevel

        now_ms = 1000
        return LocalL2Update(
            venue=self._venue.value,
            symbol=symbol,
            bids=[PriceLevel(price=49900.0, quantity=1.0), PriceLevel(price=49800.0, quantity=2.0)],
            asks=[PriceLevel(price=50100.0, quantity=1.0), PriceLevel(price=50200.0, quantity=2.0)],
            sequence=1,
            event_time_ms=now_ms,
            received_at_ms=now_ms,
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )


def make_rejected_error(reason: str = "order rejected") -> OrderSubmitError:
    return OrderSubmitError(SubmitFailureClass.REJECTED, reason)


def make_uncertain_error(reason: str = "order timeout") -> OrderSubmitError:
    return OrderSubmitError(SubmitFailureClass.UNCERTAIN, reason)


def make_fake_fill(
    venue: Venue,
    symbol: str,
    side: Side,
    quantity: float,
    price: float = 50000.0,
    order_id: str = "fill-001",
    fee_quote: float = 2.5,
    filled_at_ms: int = 1000,
) -> OrderFill:
    return OrderFill(
        venue=venue,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        order_id=order_id,
        fee_quote=fee_quote,
        filled_at_ms=filled_at_ms,
    )
