"""Protocol contracts (ABCs) matching Rust VenueAdapter trait behavior."""

from __future__ import annotations

from abc import ABC, abstractmethod
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
