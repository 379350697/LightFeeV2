"""Exchange-native liquidity/depth data source."""

from __future__ import annotations

from lightfee.core.domain import ExecutionLiquiditySnapshot, PerpLiquiditySnapshot, Venue


class LiquiditySource:
    """Fetches perp liquidity and execution depth snapshots."""

    def __init__(self, venue: Venue) -> None:
        self.venue = venue

    async def fetch_perp_liquidity(self, symbols: list[str]) -> dict[str, PerpLiquiditySnapshot]:
        return {}

    async def fetch_execution_depth(self, symbol: str) -> Optional[ExecutionLiquiditySnapshot]:
        return None
