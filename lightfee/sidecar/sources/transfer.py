"""Exchange-native transfer status source (no Chillybot transfer source)."""

from __future__ import annotations

from lightfee.core.domain import AssetTransferStatus, Venue


class TransferSource:
    """Fetches transfer statuses from exchange-native APIs."""

    def __init__(self, from_venue: Venue, to_venue: Venue) -> None:
        self.from_venue = from_venue
        self.to_venue = to_venue

    async def fetch_transfer_statuses(self, assets: list[str]) -> list[AssetTransferStatus]:
        return []
