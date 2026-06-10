"""Protocol contracts (ABCs) matching Rust VenueAdapter trait behavior.

V1 semantic parity: VenueAdapter must expose every V1 adapter capability
so engine code never reaches into private transport internals.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from lightfee.core.domain import (
    AccountBalanceSnapshot,
    AccountRiskSnapshot,
    AssetTransferStatus,
    ExecutionLiquiditySnapshot,
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PassiveOrderAck,
    PassiveOrderAmendRequest,
    PassiveOrderProgress,
    PerpLiquiditySnapshot,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.core.errors import OrderSubmitError


class VenueAdapter(ABC):
    """Abstract venue adapter matching Rust VenueAdapter trait."""

    @property
    @abstractmethod
    def venue(self) -> Venue:
        ...

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        raise NotImplementedError

    async def refresh_market_snapshot(self, symbol: str) -> VenueMarketSnapshot:
        return await self.fetch_market_snapshot([symbol])

    @abstractmethod
    async def place_order(self, request: OrderRequest) -> OrderFill:
        ...

    async def amend_order(self, request: OrderRequest) -> OrderFill:
        """Amend an existing order (modify price/quantity). Default: not implemented."""
        raise NotImplementedError(f"amend_order not implemented for {self.venue.value}")

    async def cancel_order(self, request: OrderRequest) -> None:
        """Cancel an existing order. Default: not implemented."""
        raise NotImplementedError(f"cancel_order not implemented for {self.venue.value}")

    @abstractmethod
    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        ...

    async def fetch_l2_snapshot(
        self, symbol: str, depth: int = 50,
    ) -> "LocalL2Update":
        """Fetch a full order book depth snapshot for local-L2 bootstrap.

        Returns a canonical LocalL2Update that can be fed into
        LocalL2Runtime.record_update(). Default delegates to transport if available.
        Raises NotImplementedError if the adapter has no L2 snapshot capability.
        """
        transport = getattr(self, '_transport', None)
        if transport is not None:
            return await transport.fetch_l2_snapshot(symbol=symbol, depth=depth)
        raise NotImplementedError(
            f"fetch_l2_snapshot not implemented for {self.venue.value}"
        )

    async def fetch_all_positions(self) -> Optional[list[PositionSnapshot]]:
        transport = getattr(self, '_transport', None)
        if transport is not None and hasattr(transport, "fetch_all_positions"):
            return await transport.fetch_all_positions()
        return None

    async def fetch_account_balance_snapshot(self) -> Optional[AccountBalanceSnapshot]:
        return None

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> Optional[OrderFillReconciliation]:
        return None

    async def fetch_order_status(
        self,
        symbol: str,
        *,
        order_id: str = "",
        client_order_id: str = "",
    ) -> Optional[OrderFillReconciliation]:
        """Task 11: Query order status by exchange ID or client ID.

        Bybit uses orderLinkId; Bitget uses clientOid.
        Returns OrderFillReconciliation if found, or None.
        """
        transport = getattr(self, "_transport", None)
        if transport is not None and hasattr(transport, "fetch_order_status"):
            return await transport.fetch_order_status(
                symbol, order_id=order_id, client_order_id=client_order_id,
            )
        return None

    async def fetch_perp_liquidity_snapshot(
        self, symbol: str
    ) -> Optional[PerpLiquiditySnapshot]:
        return None

    async def fetch_execution_liquidity_snapshot(
        self, symbol: str
    ) -> Optional[ExecutionLiquiditySnapshot]:
        snapshot = await self.refresh_market_snapshot(symbol)
        for quote in snapshot.quotes:
            if quote.symbol == symbol:
                return ExecutionLiquiditySnapshot(
                    venue=snapshot.venue,
                    symbol=symbol,
                    observed_at_ms=snapshot.observed_at_ms,
                    best_bid=quote.bid,
                    bid_size=quote.bid_size,
                    best_ask=quote.ask,
                    ask_size=quote.ask_size,
                )
        return None

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity

    async def ensure_entry_leverage(self, symbol: str, leverage: int) -> None:
        pass

    @property
    def supports_risk_health(self) -> bool:
        """Whether this venue adapter can provide account risk health snapshots.

        V1: Risk health requires equity/margin data to compute health ratios.
        Default is False — overridden by adapters that support it.
        """
        return False

    @property
    def supports_private_health(self) -> bool:
        """Whether this venue adapter can provide private-stream health data.

        V1: Private health requires a working private WebSocket connection
        and position caching. Default is False — overridden by adapters
        that support it.
        """
        return False

    async def fetch_account_risk_snapshot(self) -> Optional[AccountRiskSnapshot]:
        """Fetch an account risk snapshot for risk health evaluation.

        Returns None if not supported. V1 equivalent: account risk polling
        that feeds evaluate_position_risk().
        """
        return None

    def ws_worker_categories(self) -> list[dict]:
        """Return worker category diagnostics (V1: WsWorkerCategoryStatus).

        Each entry: {category, active_count, expected_max, risk_relevant}.
        Default empty — adapters that own workers override this.
        """
        return []

    async def shutdown(self) -> None:
        pass

    # --- V1 contract completeness: Private health ---

    @property
    def private_health(self) -> Optional[dict]:
        """V1: last known private health snapshot (cached).

        Returns a dict with keys matching V1 PrivateHealth: equity, margin,
        health_ratio, observed_at_ms. None if never fetched.
        """
        return None

    async def fetch_private_health(self) -> Optional[dict]:
        """V1: fetch and cache the latest private health snapshot.

        Returns the same shape as private_health property.
        """
        return None

    @property
    def cached_private_health(self) -> Optional[dict]:
        """V1: explicitly cached copy for fail-closed scenarios."""
        return self.private_health

    def cached_private_connection_health(self):
        """V1: cached private WebSocket connection health.

        Returns a ConnectionHealth object or None if not available.
        Used by supervisor to determine if private stream is healthy.

        Delegates to transport if available, otherwise returns None.
        """
        transport = getattr(self, '_transport', None)
        if transport is not None and hasattr(transport, 'cached_private_connection_health'):
            return transport.cached_private_connection_health()
        return None

    def cached_position(self, symbol: str):
        """V1: cached position data from private stream for a symbol.

        Returns a PositionSnapshot or None if not cached.
        Used by supervisor to verify private position confirmation.

        Delegates to transport if available, otherwise returns None.
        """
        transport = getattr(self, '_transport', None)
        if transport is not None and hasattr(transport, 'cached_position'):
            return transport.cached_position(symbol)
        return None

    # --- V1 contract completeness: Passive progress ---

    @property
    def private_passive_progress(self) -> bool:
        """V1: whether this venue supports private passive order progress queries."""
        return hasattr(self, '_transport') and hasattr(getattr(self, '_transport', None), 'query_passive_order_progress')

    # --- V1 contract completeness: Passive metadata ---

    def passive_metadata(self, symbol: str) -> dict:
        """V1: metadata for passive orders on this venue.

        Returns: {min_notional, min_quantity, price_tick, quantity_step, max_quantity}.
        """
        spec = self._get_spec()
        return {
            "min_notional": spec.min_notional if spec else 5.0,
            "min_quantity": spec.min_quantity if spec else 0.001,
            "price_tick": spec.price_tick if spec else 0.01,
            "quantity_step": spec.quantity_step if spec else 0.001,
            "max_quantity": 0.0,
        }

    def _get_spec(self):
        try:
            from lightfee.venues.specs import get_spec
            return get_spec(self.venue)
        except Exception:
            return None

    # --- V1 contract completeness: Order sizing ---

    def order_sizing_spec(self, symbol: str) -> dict:
        """V1: order sizing specification for a symbol on this venue.

        Returns: {quantity_step, min_quantity, min_notional, price_tick, contract_size}.
        """
        return self.passive_metadata(symbol)

    # --- V1 contract completeness: Entry headroom ---

    async def entry_open_notional_headroom(self, symbol: str) -> Optional[float]:
        """V1: remaining notional capacity before hitting position limits.

        Returns None if the venue does not support this query.
        """
        return None

    # --- V1 contract completeness: Transfer status ---

    async def fetch_transfer_status(
        self, asset: str, from_venue: Venue, to_venue: Venue
    ) -> Optional[AssetTransferStatus]:
        """V1: query asset transfer status between venues.

        Returns None if transfers are unsupported.
        """
        return None

    # --- V1 contract completeness: Supported symbols ---

    def supported_symbols(self) -> list[str]:
        """V1: list of symbols this venue supports for trading.

        Default empty — adapters that filter symbols override this.
        """
        return []

    # --- V1 contract completeness: Market data activity control ---

    @property
    def market_data_active(self) -> bool:
        """V1: whether market data ingestion is currently active."""
        return True

    async def pause_market_data(self) -> None:
        """V1: pause market data ingestion for this venue."""
        pass

    async def resume_market_data(self) -> None:
        """V1: resume market data ingestion for this venue."""
        pass

    # --- V1 contract completeness: Live startup activation ---

    async def activate_for_live(self) -> bool:
        """V1: activate the adapter for live trading.

        Called during startup phase (after prewarm). Returns True if activated.
        """
        return True

    # --- V1 contract completeness: Local-L2 reconcile targets ---

    def local_l2_reconcile_targets(self) -> list[str]:
        """V1: symbols that should have active local-L2 books.

        Returns a list of canonical symbols. Default empty — adapters
        with L2 support override this.
        """
        return []

    # --- V1 contract completeness: Worker status ---

    def worker_status(self) -> dict:
        """V1: per-worker status diagnostics.

        Returns: {worker_count, categories: [{category, active, healthy}]}.
        """
        return {"worker_count": 0, "categories": self.ws_worker_categories()}

    # --- V1 contract completeness: Prewarm ---

    async def prewarm(self) -> bool:
        """V1: prewarm the adapter before live trading starts.

        Typically fetches initial snapshots, validates connectivity,
        and warms rate-limit tokens. Returns True on success.
        """
        return True

    # --- Passive order contract (V1 resting-order semantics) ---

    async def submit_passive_order(self, request: OrderRequest) -> PassiveOrderAck:
        """Submit a reduce-only GTC post-only maker order. Returns ack, not fill."""
        transport = getattr(self, '_transport', None)
        if transport is not None and hasattr(transport, 'submit_passive_order'):
            return await transport.submit_passive_order(request)
        raise NotImplementedError(
            f"submit_passive_order not implemented for {self.venue.value}"
        )

    async def query_passive_order_progress(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
        side: "Side | None" = None,
    ) -> Optional[PassiveOrderProgress]:
        """Query cumulative progress for a resting passive order."""
        transport = getattr(self, '_transport', None)
        if transport is not None and hasattr(transport, 'query_passive_order_progress'):
            return await transport.query_passive_order_progress(
                symbol=symbol, order_id=order_id, client_order_id=client_order_id,
                side=side,
            )
        raise NotImplementedError(
            f"query_passive_order_progress not implemented for {self.venue.value}"
        )

    async def amend_passive_order(
        self, request: PassiveOrderAmendRequest
    ) -> PassiveOrderAck:
        """Amend a resting passive order (price/quantity)."""
        transport = getattr(self, '_transport', None)
        if transport is not None and hasattr(transport, 'amend_passive_order'):
            return await transport.amend_passive_order(request)
        raise NotImplementedError(
            f"amend_passive_order not implemented for {self.venue.value}"
        )

    async def cancel_passive_order(
        self, symbol: str, order_id: str, client_order_id: Optional[str] = None
    ) -> PassiveOrderAck:
        """Cancel a resting passive order."""
        transport = getattr(self, '_transport', None)
        if transport is not None and hasattr(transport, 'cancel_passive_order'):
            return await transport.cancel_passive_order(
                symbol=symbol, order_id=order_id, client_order_id=client_order_id,
            )
        raise NotImplementedError(
            f"cancel_passive_order not implemented for {self.venue.value}"
        )

    def price_tick_size(self, symbol: str) -> Optional[float]:
        """Return the canonical price tick size for a symbol on this venue.

        V1: passive_order_tick_size() in entry.rs line 2957.
        Must be the venue's price tick, NOT the quantity step.

        Default: reads from VenueSpec.price_tick via get_spec().
        """
        try:
            from lightfee.venues.specs import get_spec
            spec = get_spec(self.venue)
            if spec.price_tick > 0:
                return spec.price_tick
        except Exception:
            pass
        return None

    def min_entry_notional_quote_hint(
        self, symbol: str, price_hint: Optional[float] = None
    ) -> Optional[float]:
        """Return the venue's min notional quote for a symbol."""
        return None
