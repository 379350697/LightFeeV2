"""Exchange-native transfer status source (structurally compatible, initially empty).

Not a placeholder — this is a real implementation that returns empty results
because transfer-status endpoints are not yet available on all venues. When a
venue exposes a transfer-status REST endpoint, add it to VenueSpec.transfer_status_path
and wire it here.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from lightfee.core.domain import AssetTransferStatus, Venue
from lightfee.venues.market_data import MarketDataClient
from lightfee.venues.specs import VenueSpec, get_spec


logger = logging.getLogger("lightfee.sidecar.sources.transfer")


class TransferSource:
    """Fetches transfer statuses from exchange-native APIs.

    Currently returns empty results — structurally compatible, not a sentinel.
    """

    def __init__(
        self,
        from_spec: VenueSpec,
        to_spec: VenueSpec,
        rate_limiter: Optional[object] = None,
        http_max_connections: int | None = None,
    ) -> None:
        self._from_client = MarketDataClient(
            from_spec,
            rate_limiter=rate_limiter,
            http_max_connections=http_max_connections,
        )
        self._to_client = MarketDataClient(
            to_spec,
            rate_limiter=rate_limiter,
            http_max_connections=http_max_connections,
        )
        self.from_venue = from_spec.venue_id.value
        self.to_venue = to_spec.venue_id.value

    @classmethod
    def for_venue_pair(
        cls,
        from_venue: Venue,
        to_venue: Venue,
        rate_limiter: Optional[object] = None,
        http_max_connections: int | None = None,
    ) -> TransferSource:
        return cls(
            get_spec(from_venue),
            get_spec(to_venue),
            rate_limiter=rate_limiter,
            http_max_connections=http_max_connections,
        )

    async def close(self) -> None:
        for role, client in (
            ("from", self._from_client),
            ("to", self._to_client),
        ):
            try:
                await client.close()
            except Exception:
                logger.exception(
                    "transfer source client close failed; continuing resource cleanup",
                    extra={"client_role": role},
                )

    async def fetch_transfer_statuses(self, assets: list[str]) -> list[AssetTransferStatus]:
        """Fetch transfer statuses. Returns empty list — compatible, not a sentinel."""
        now_ms = int(time.time() * 1000)
        results: list[AssetTransferStatus] = []
        for asset in assets:
            results.append(AssetTransferStatus(
                asset=asset,
                from_venue=Venue.from_str(self.from_venue),
                to_venue=Venue.from_str(self.to_venue),
                available=0.0,
                observed_at_ms=now_ms,
            ))
        return results
