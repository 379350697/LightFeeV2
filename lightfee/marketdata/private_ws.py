"""V1 private WebSocket: order/fill event stream for reconciliation.

Rust references:
- src/live/private_ws.rs: long-lived worker, connection_health, record_success/failure
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from lightfee.core.domain import OrderFill, Side, Venue
from lightfee.marketdata.ws import WsConnectionState, WsStreamState


# ---------------------------------------------------------------------------
# Private WS events
# ---------------------------------------------------------------------------


class PrivateWsEventKind(Enum):
    ORDER_ACK = "order_ack"
    ORDER_FILL = "order_fill"
    ORDER_CANCEL = "order_cancel"
    ORDER_REJECT = "order_reject"
    POSITION_UPDATE = "position_update"
    ACCOUNT_SNAPSHOT = "account_snapshot"


@dataclass
class PrivateWsEvent:
    """V1 private WS event: order fill, ack, cancel, position update."""
    venue: Venue
    kind: PrivateWsEventKind
    symbol: str = ""
    order_id: str = ""
    client_order_id: str = ""
    side: Side = Side.BUY
    quantity: float = 0.0
    price: float = 0.0
    fee_quote: float = 0.0
    filled_quantity: float = 0.0
    average_price: float = 0.0
    order_status: str = ""
    position_quantity: float = 0.0
    equity_quote: float = 0.0
    maintenance_margin_quote: float = 0.0
    observed_at_ms: int = 0
    raw_payload: str = ""

    def to_order_fill(self) -> Optional[OrderFill]:
        """Convert fill event to OrderFill for reconciliation."""
        if self.kind != PrivateWsEventKind.ORDER_FILL or self.quantity <= 0:
            return None
        return OrderFill(
            venue=self.venue,
            symbol=self.symbol,
            side=self.side,
            quantity=self.quantity,
            price=self.price,
            order_id=self.order_id,
            fee_quote=self.fee_quote,
            filled_at_ms=self.observed_at_ms,
        )


# ---------------------------------------------------------------------------
# Private WS client state
# ---------------------------------------------------------------------------


@dataclass
class PrivateWsClientState:
    """V1 private WS client: tracks connection + recent events for reconciliation."""
    venue: Venue
    stream: WsStreamState = field(default_factory=WsStreamState)
    last_position_update_ms: int = 0
    position_confirmed: bool = False
    pending_reconciliation: list[PrivateWsEvent] = field(default_factory=list)

    def on_position_confirmed(self, now_ms: int) -> None:
        self.position_confirmed = True
        self.last_position_update_ms = now_ms

    def on_fill_event(self, event: PrivateWsEvent) -> None:
        """Buffer fill event for reconciliation with pending entries/closes."""
        self.pending_reconciliation.append(event)

    def drain_reconciliation_events(self) -> list[PrivateWsEvent]:
        events = list(self.pending_reconciliation)
        self.pending_reconciliation.clear()
        return events

    def is_healthy(self) -> bool:
        return self.stream.is_healthy()
