"""Exchange-native liquidity/depth data source backed by MarketDataClient."""

from __future__ import annotations

from typing import Optional

from lightfee.core.domain import ExecutionLiquiditySnapshot, PerpLiquiditySnapshot, Venue
from lightfee.venues.market_data import MarketDataClient, PerpLiquidity
from lightfee.venues.specs import VenueSpec, get_spec


class LiquiditySource:
    """Fetches perp liquidity and execution depth snapshots from public endpoints."""

    def __init__(
        self,
        spec: VenueSpec,
        rate_limiter: Optional[object] = None,
        http_max_connections: int | None = None,
    ) -> None:
        self._client = MarketDataClient(
            spec,
            rate_limiter=rate_limiter,
            http_max_connections=http_max_connections,
        )
        self.venue = spec.venue_id.value

    @classmethod
    def for_venue(
        cls,
        venue: Venue,
        rate_limiter: Optional[object] = None,
        http_max_connections: int | None = None,
    ) -> LiquiditySource:
        return cls(
            get_spec(venue),
            rate_limiter=rate_limiter,
            http_max_connections=http_max_connections,
        )

    async def close(self) -> None:
        await self._client.close()

    async def fetch_perp_liquidity(self, symbols: list[str]) -> dict[str, PerpLiquiditySnapshot]:
        """Fetch perp liquidity (volume + OI) for symbols."""
        perp_map = await self._client.fetch_perp_liquidity(symbols)
        result: dict[str, PerpLiquiditySnapshot] = {}
        for key, pl in perp_map.items():
            result[key] = PerpLiquiditySnapshot(
                venue=Venue.from_str(pl.venue),
                symbol=pl.symbol,
                observed_at_ms=pl.observed_at_ms,
            )
        return result

    async def fetch_execution_depth(self, symbol: str) -> Optional[ExecutionLiquiditySnapshot]:
        """Fetch execution depth (L2 snapshot) for a single symbol."""
        try:
            raw = await self._client.fetch_l2_snapshot(symbol)
        except Exception:
            return None
        bids = raw.get("bids", [])
        asks = raw.get("asks", [])
        best_bid = float(bids[0][0]) if bids else 0.0
        bid_size = float(bids[0][1]) if bids else 0.0
        best_ask = float(asks[0][0]) if asks else 0.0
        ask_size = float(asks[0][1]) if asks else 0.0
        return ExecutionLiquiditySnapshot(
            venue=Venue.from_str(raw.get("venue", self.venue)),
            symbol=raw.get("symbol", symbol),
            observed_at_ms=raw.get("received_at_ms", 0),
            best_bid=best_bid,
            bid_size=bid_size,
            best_ask=best_ask,
            ask_size=ask_size,
        )
