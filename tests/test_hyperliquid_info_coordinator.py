from __future__ import annotations

import json
import multiprocessing
import os
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

import lightfee.config.paths as config_paths
from lightfee.config.loader import load_config
from lightfee.core.domain import Venue
from lightfee.venues.hyperliquid_info_coordinator import (
    HYPERLIQUID_INFO_CACHE_TTL_MS_ENV,
    HYPERLIQUID_INFO_COORDINATOR_DIR_ENV,
    HYPERLIQUID_INFO_MIN_INTERVAL_MS_ENV,
    HyperliquidInfoCoordinator,
    hyperliquid_info_coordinator,
    is_metadata_cacheable_hyperliquid_info_body,
    should_coordinate_hyperliquid_info_request,
    should_coordinate_hyperliquid_info_url,
)
from lightfee.marketdata.open_interest import open_interest_timestamps_are_fresh
from lightfee.venues.market_data import MarketDataClient
from lightfee.venues.specs import hyperliquid_spec
from lightfee.venues.transport import VenueTransport


def _multiprocess_wait_worker(
    directory: str,
    min_interval_ms: int,
    queue: Any,
) -> None:
    coordinator = HyperliquidInfoCoordinator(
        directory=directory,
        min_interval_ms=min_interval_ms,
        cooldown_ms=0,
        cache_ttl_ms=0,
    )
    coordinator.wait_until_ready({"type": "l2Book", "coin": str(os.getpid())})
    queue.put(time.monotonic_ns())


def test_multiprocess_hyperliquid_info_requests_share_pacing(tmp_path: Path) -> None:
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    min_interval_ms = 60
    processes = [
        ctx.Process(
            target=_multiprocess_wait_worker,
            args=(str(tmp_path), min_interval_ms, queue),
        )
        for _ in range(3)
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=5)

    for process in processes:
        assert process.exitcode == 0

    reservations = sorted(queue.get(timeout=1) for _ in processes)
    gaps_ms = [
        (right - left) / 1_000_000
        for left, right in zip(reservations, reservations[1:])
    ]
    assert all(gap >= 45 for gap in gaps_ms)


def test_rate_limit_response_creates_shared_cooldown(tmp_path: Path) -> None:
    coordinator = HyperliquidInfoCoordinator(
        directory=tmp_path,
        min_interval_ms=0,
        cooldown_ms=60,
        cache_ttl_ms=0,
    )
    coordinator.wait_until_ready({"type": "l2Book", "coin": "BTC"})
    assert coordinator.record_http_response(
        429,
        {},
        body={"type": "l2Book", "coin": "BTC"},
    ) == 60

    started = time.monotonic()
    coordinator.wait_until_ready({"type": "l2Book", "coin": "ETH"})
    elapsed_ms = (time.monotonic() - started) * 1000

    assert elapsed_ms >= 45


def test_repeated_rate_limits_share_exponential_backoff_and_success_resets_it(
    tmp_path: Path,
) -> None:
    coordinator = HyperliquidInfoCoordinator(
        directory=tmp_path,
        min_interval_ms=0,
        cooldown_ms=20,
        max_cooldown_ms=80,
        cache_ttl_ms=0,
    )
    body = {"type": "l2Book", "coin": "BTC"}

    assert coordinator.record_http_response(429, {}, body=body) == 20
    assert coordinator.record_http_response(429, {}, body=body) == 40
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["consecutive_rate_limit_count"] == 2
    assert state["last_rate_limit_scope"] == "POST /info"

    # A separate process observes the increased shared cooldown rather than
    # starting a fresh fixed-delay retry sequence.
    other = HyperliquidInfoCoordinator(
        directory=tmp_path,
        min_interval_ms=0,
        cooldown_ms=20,
        max_cooldown_ms=80,
        cache_ttl_ms=0,
    )
    state = other._read_state()
    assert state["consecutive_rate_limit_count"] == 2

    assert coordinator.record_http_response(200, {}, body=body) == 0
    assert coordinator.record_http_response(429, {}, body=body) == 20


def test_stale_lock_is_replaced_without_bypassing_pacing(tmp_path: Path) -> None:
    coordinator = HyperliquidInfoCoordinator(
        directory=tmp_path,
        min_interval_ms=0,
        cooldown_ms=0,
        cache_ttl_ms=0,
        lock_stale_ms=1,
    )
    tmp_path.mkdir(parents=True, exist_ok=True)
    lock_path = tmp_path / "state.lock"
    lock_path.write_text("stale", encoding="utf-8")
    old = time.time() - 1
    os.utime(lock_path, (old, old))

    reservation = coordinator.wait_until_ready({"type": "l2Book", "coin": "BTC"})

    assert reservation.waited_ms == 0
    assert not lock_path.exists()


def test_metadata_cache_preserves_original_receipt_and_never_caches_private_truth(
    tmp_path: Path,
) -> None:
    coordinator = HyperliquidInfoCoordinator(
        directory=tmp_path,
        min_interval_ms=0,
        cooldown_ms=0,
        cache_ttl_ms=2_000,
    )
    receipt_ms = int(time.time() * 1000) - 350
    assert coordinator.store_metadata_response(
        {"type": "meta"},
        {"universe": [{"name": "BTC"}]},
        received_at_ms=receipt_ms,
    )

    hit = coordinator.lookup_metadata_response({"type": "meta"})
    assert hit is not None
    assert hit.received_at_ms == receipt_ms
    assert hit.age_ms >= 300
    hit.payload["universe"][0]["name"] = "MUTATED"
    assert coordinator.lookup_metadata_response({"type": "meta"}).payload == {
        "universe": [{"name": "BTC"}]
    }

    private_body = {"type": "clearinghouseState", "user": "0xabc"}
    assert should_coordinate_hyperliquid_info_request(
        Venue.HYPERLIQUID,
        "POST",
        "/info",
        private_body,
    )
    assert not is_metadata_cacheable_hyperliquid_info_body(private_body)
    assert not coordinator.store_metadata_response(
        private_body,
        {"assetPositions": []},
        received_at_ms=receipt_ms,
    )
    assert coordinator.lookup_metadata_response(private_body) is None


def test_strict_readonly_scope_excludes_exchange_actions() -> None:
    assert should_coordinate_hyperliquid_info_url(
        "POST",
        "https://api.hyperliquid.xyz/info",
        {"type": "openOrders", "user": "0xabc"},
    )
    assert not should_coordinate_hyperliquid_info_url(
        "POST",
        "https://api.hyperliquid.xyz/exchange",
        {"action": {"type": "order"}},
    )
    assert not should_coordinate_hyperliquid_info_request(
        Venue.HYPERLIQUID,
        "POST",
        "/info",
        {"type": "unknownWriteLikeShape"},
    )


def test_generic_factory_uses_loaded_config_runtime_directory_from_non_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(HYPERLIQUID_INFO_COORDINATOR_DIR_ENV, raising=False)
    monkeypatch.setattr(
        config_paths,
        "_CONFIGURED_HYPERLIQUID_INFO_COORDINATOR_DIR",
        None,
    )
    project = tmp_path / "project"
    config_dir = project / "config"
    config_dir.mkdir(parents=True)
    path = config_dir / "live.toml"
    path.write_text(
        """
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"
""",
        encoding="utf-8",
    )
    other_cwd = tmp_path / "other"
    other_cwd.mkdir()
    monkeypatch.chdir(other_cwd)

    config = load_config(path)

    expected = (project / "runtime" / "hyperliquid-info-coordinator").resolve()
    assert (
        config.runtime.hyperliquid_info_coordinator_dir
        == "runtime/hyperliquid-info-coordinator"
    )
    assert config_paths.configured_hyperliquid_info_coordinator_dir() == expected
    assert hyperliquid_info_coordinator().directory == expected

    override = tmp_path / "env-override" / "hyperliquid-info"
    monkeypatch.setenv(HYPERLIQUID_INFO_COORDINATOR_DIR_ENV, str(override))

    assert hyperliquid_info_coordinator().directory == override.resolve()


@pytest.mark.asyncio
async def test_transport_metadata_cache_uses_single_public_info_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(HYPERLIQUID_INFO_COORDINATOR_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(HYPERLIQUID_INFO_MIN_INTERVAL_MS_ENV, "0")
    monkeypatch.setenv(HYPERLIQUID_INFO_CACHE_TTL_MS_ENV, "2000")
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        calls.append(body)
        return httpx.Response(200, json={"universe": [{"name": "BTC"}]})

    transport = VenueTransport(spec=hyperliquid_spec(), mode="paper")
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        first = await transport._request(
            "POST",
            "/info",
            body={"type": "meta"},
            private=False,
        )
        second = await transport._request(
            "POST",
            "/info",
            body={"type": "meta"},
            private=False,
        )
    finally:
        await transport.close()

    assert first == second == {"universe": [{"name": "BTC"}]}
    assert calls == [{"type": "meta"}]


@pytest.mark.asyncio
async def test_market_data_metadata_cache_preserves_received_at(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(HYPERLIQUID_INFO_COORDINATOR_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(HYPERLIQUID_INFO_MIN_INTERVAL_MS_ENV, "0")
    monkeypatch.setenv(HYPERLIQUID_INFO_CACHE_TTL_MS_ENV, "2000")
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        calls.append(body)
        return httpx.Response(
            200,
            json=[
                {"universe": [{"name": "BTC"}]},
                [{"markPx": "65000"}],
            ],
        )

    client = MarketDataClient(hyperliquid_spec())
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        first, first_received_at = await client._public_post_with_received_at(
            "/info",
            body={"type": "metaAndAssetCtxs"},
        )
        time.sleep(0.01)
        second, second_received_at = await client._public_post_with_received_at(
            "/info",
            body={"type": "metaAndAssetCtxs"},
        )
    finally:
        await client.close()

    assert first == second
    assert first_received_at == second_received_at
    assert calls == [{"type": "metaAndAssetCtxs"}]


@pytest.mark.asyncio
async def test_hyperliquid_entry_open_interest_cache_hit_preserves_original_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(HYPERLIQUID_INFO_COORDINATOR_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(HYPERLIQUID_INFO_MIN_INTERVAL_MS_ENV, "0")
    monkeypatch.setenv(HYPERLIQUID_INFO_CACHE_TTL_MS_ENV, "120000")
    receipt_ms = int(time.time() * 1000) - 60_000
    payload = [
        {"universe": [{"name": "BTC"}]},
        [{"openInterest": "12.5", "markPx": "65000"}],
    ]
    coordinator = HyperliquidInfoCoordinator(
        directory=tmp_path,
        min_interval_ms=0,
        cooldown_ms=0,
        cache_ttl_ms=120_000,
    )
    assert coordinator.store_metadata_response(
        {"type": "metaAndAssetCtxs"},
        payload,
        received_at_ms=receipt_ms,
    )
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content.decode("utf-8")))
        return httpx.Response(200, json=payload)

    client = MarketDataClient(hyperliquid_spec())
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        tickers = await client.fetch_entry_open_interest_evidence(["BTCUSDT"])
    finally:
        await client.close()

    ticker = tickers["hyperliquid:BTCUSDT"]
    assert calls == []
    assert ticker.open_interest_evidence_status == "observed"
    assert ticker.open_interest_observed_at_ms == receipt_ms
    assert ticker.open_interest_received_at_ms == receipt_ms
    assert not open_interest_timestamps_are_fresh(
        observed_at_ms=ticker.open_interest_observed_at_ms,
        received_at_ms=ticker.open_interest_received_at_ms,
        now_ms=receipt_ms + 60_000,
        max_age_ms=30_000,
    )
