"""Guarded live smoke tests — opt-in only, never runs in CI.

Set LIGHTFEE_LIVE_SMOKE=1 to enable, and provide per-venue env vars
(e.g. LIGHTFEE_BINANCE_API_KEY, LIGHTFEE_BINANCE_API_SECRET).
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LIGHTFEE_LIVE_SMOKE") != "1",
    reason="LIGHTFEE_LIVE_SMOKE=1 required for live exchange smoke tests",
)

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import Venue
from lightfee.venues.registry import build_adapter_map
from lightfee.config.loader import load_config


@pytest.fixture(scope="module")
def live_config():
    path = os.environ.get("LIGHTFEE_LIVE_CONFIG", "config/live.example.toml")
    return load_config(path)


@pytest.fixture(scope="module")
def live_adapters(live_config):
    return build_adapter_map(live_config)


class TestLiveAdapterConstruction:
    def test_all_configured_venues_build(self, live_adapters):
        assert len(live_adapters) > 0
        for venue, adapter in live_adapters.items():
            assert isinstance(adapter, VenueAdapter)
            assert adapter.venue == venue

    @pytest.mark.asyncio
    async def test_fetch_market_snapshot_live(self, live_adapters):
        for venue, adapter in live_adapters.items():
            snap = await adapter.fetch_market_snapshot(["BTCUSDT"])
            assert snap.venue == venue
