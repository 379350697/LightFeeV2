from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig
from lightfee.core.domain import Side, Venue
from lightfee.engine.business_contract import (
    close_reconciliation_exchange_truth,
    close_reconciliation_exchange_truth_clean,
)
from lightfee.engine.runtime import LiveRuntime
from lightfee.venues.market_data import MarketDataClient
from lightfee.venues.specs import bybit_spec, okx_spec
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    VenueTransport,
)


class _FakeOkxClient(MarketDataClient):
    def __init__(self) -> None:
        super().__init__(okx_spec())
        self.requests: list[tuple[str, dict[str, Any]]] = []

    async def _public_get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        del timeout
        params = dict(params or {})
        self.requests.append((path, params))
        if path == "/api/v5/market/tickers":
            return {
                "data": [
                    {
                        "instId": "BONK-USDT-SWAP",
                        "bidPx": "0.1000",
                        "askPx": "0.1001",
                        "bidSz": "100",
                        "askSz": "100",
                        "last": "0.1000",
                        "markPx": "0.1000",
                        "volCcy24h": "100000",
                    }
                ]
            }
        if path == "/api/v5/public/funding-rate":
            inst_id = params.get("instId")
            if inst_id == "BONK-USDT-SWAP":
                return {
                    "data": [
                        {
                            "instId": inst_id,
                            "fundingRate": "0.0001",
                            "nextFundingTime": "2000000000000",
                            "markPrice": "0.1000",
                            "indexPrice": "0.1000",
                        }
                    ]
                }
            return {"data": []}
        if path == "/api/v5/public/open-interest":
            return {"data": [{"instId": "BONK-USDT-SWAP", "oiUsd": "1000"}]}
        if path == "/api/v5/public/instruments":
            return {
                "data": [
                    {
                        "instId": "BONK-USDT-SWAP",
                        "instType": "SWAP",
                        "state": "live",
                    }
                ]
            }
        if path == "/api/v5/public/mark-price":
            return {
                "data": [
                    {
                        "instId": "BONK-USDT-SWAP",
                        "markPx": "0.1000",
                    }
                ]
            }
        if path == "/api/v5/market/index-tickers":
            return {
                "data": [
                    {
                        "instId": "BONK-USDT",
                        "idxPx": "0.1000",
                    }
                ]
            }
        raise AssertionError(f"unexpected OKX path: {path}")


@pytest.mark.asyncio
async def test_okx_prefixed_alias_does_not_reuse_unprefixed_instrument() -> None:
    client = _FakeOkxClient()

    tickers = await client._fetch_okx_style(["1000BONKUSDT"])

    assert tickers == {}
    assert ("/api/v5/public/funding-rate", {"instId": "1000BONK-USDT-SWAP"}) in client.requests
    assert ("/api/v5/public/funding-rate", {"instId": "BONK-USDT-SWAP"}) not in client.requests


def _bybit_page(rows: list[dict[str, Any]], cursor: str = "") -> dict[str, Any]:
    return {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": rows,
            "nextPageCursor": cursor,
        },
    }


class _FakeBybitTransport(VenueTransport):
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        super().__init__(
            bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: Any = None,
        private: bool = False,
    ) -> Any:
        del body
        assert method == "GET"
        assert path == "/v5/position/list"
        assert private is True
        self.requests.append(dict(params or {}))
        if not self.responses:
            raise AssertionError("unexpected extra Bybit request")
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_bybit_all_positions_uses_limit_cursor_and_nets_hedge_rows() -> None:
    transport = _FakeBybitTransport(
        [
            _bybit_page(
                [
                    {
                        "symbol": "BTCUSDT",
                        "side": "Buy",
                        "size": "1.0",
                        "avgPrice": "50000",
                    }
                ],
                cursor="page-2",
            ),
            _bybit_page(
                [
                    {
                        "symbol": "BTCUSDT",
                        "side": "Sell",
                        "size": "0.25",
                        "avgPrice": "50100",
                    }
                ]
            ),
        ]
    )

    positions = await transport.fetch_all_positions()

    assert transport.requests == [
        {"category": "linear", "settleCoin": "USDT", "limit": 200},
        {
            "category": "linear",
            "settleCoin": "USDT",
            "limit": 200,
            "cursor": "page-2",
        },
    ]
    assert len(positions) == 1
    assert positions[0].venue is Venue.BYBIT
    assert positions[0].symbol == "BTCUSDT"
    assert positions[0].side is Side.BUY
    assert positions[0].quantity == pytest.approx(0.75)


@pytest.mark.asyncio
async def test_bybit_all_positions_fails_closed_on_later_page_failure() -> None:
    transport = _FakeBybitTransport(
        [
            _bybit_page(
                [
                    {
                        "symbol": "ETHUSDT",
                        "side": "Buy",
                        "size": "1.0",
                        "avgPrice": "3000",
                    }
                ],
                cursor="page-2",
            ),
            {"retCode": 10001, "retMsg": "bad cursor", "result": {}},
        ]
    )

    with pytest.raises(TransportError) as exc_info:
        await transport.fetch_all_positions()

    assert exc_info.value.category is TransportErrorCategory.TRANSPORT_FAILURE
    assert "cursor" in str(exc_info.value).lower() or "retcode" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_bybit_all_positions_fails_closed_on_cursor_loop() -> None:
    transport = _FakeBybitTransport(
        [
            _bybit_page(
                [
                    {
                        "symbol": "ETHUSDT",
                        "side": "Buy",
                        "size": "1.0",
                        "avgPrice": "3000",
                    }
                ],
                cursor="repeat",
            ),
            _bybit_page([], cursor="repeat"),
        ]
    )

    with pytest.raises(TransportError) as exc_info:
        await transport.fetch_all_positions()

    assert exc_info.value.category is TransportErrorCategory.TRANSPORT_FAILURE
    assert "cursor" in str(exc_info.value).lower()


class _FakePrivateWsTransport:
    def __init__(self) -> None:
        self.starts: list[list[str]] = []
        self.stops = 0
        self.updates: list[list[str]] = []

    def start_private_ws(self, symbols: list[str]) -> None:
        self.starts.append(sorted(symbols))

    def stop_private_ws(self) -> None:
        self.stops += 1

    def update_private_ws_symbols(self, symbols: list[str]) -> None:
        self.updates.append(sorted(symbols))


def _live_runtime_with_private_transport(
    tmp_path,
    transport: _FakePrivateWsTransport,
) -> LiveRuntime:
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    adapter = SimpleNamespace(supports_private_health=True, _transport=transport)
    runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: adapter})
    runtime.journal.open()
    return runtime


def test_private_ws_empty_refresh_keeps_started_worker(tmp_path) -> None:
    transport = _FakePrivateWsTransport()
    runtime = _live_runtime_with_private_transport(tmp_path, transport)
    try:
        runtime._current_tracked_private_symbols = lambda: {
            Venue.BINANCE: {"BTCUSDT"}
        }
        runtime._ensure_private_ws_started(1000)

        runtime._current_tracked_private_symbols = lambda: {}
        runtime._ensure_private_ws_started(2000)

        assert transport.starts == [["BTCUSDT"]]
        assert transport.stops == 0
        assert Venue.BINANCE in runtime._private_ws_started
        assert runtime._private_ws_symbols[Venue.BINANCE] == {"BTCUSDT"}
    finally:
        runtime.journal.close()


def test_private_ws_symbol_addition_updates_without_restart(tmp_path) -> None:
    transport = _FakePrivateWsTransport()
    runtime = _live_runtime_with_private_transport(tmp_path, transport)
    try:
        runtime._current_tracked_private_symbols = lambda: {
            Venue.BINANCE: {"BTCUSDT"}
        }
        runtime._ensure_private_ws_started(1000)

        runtime._current_tracked_private_symbols = lambda: {
            Venue.BINANCE: {"BTCUSDT", "ETHUSDT"}
        }
        runtime._ensure_private_ws_started(2000)

        assert transport.starts == [["BTCUSDT"]]
        assert transport.updates == [["ETHUSDT"]]
        assert transport.stops == 0
        assert runtime._private_ws_symbols[Venue.BINANCE] == {
            "BTCUSDT",
            "ETHUSDT",
        }
    finally:
        runtime.journal.close()


def _pair_reconciliation() -> dict[str, Any]:
    return {
        "symbol": "LABUSDT",
        "position_snapshot": {
            "symbol": "LABUSDT",
            "long_venue": "binance",
            "short_venue": "okx",
        },
    }


def _scoped_flat_truth(extra_evidence: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "available": True,
        "truth_supported": True,
        "positions": [
            {"venue": "binance", "symbol": "LABUSDT", "quantity": 0.0},
            {"venue": "okx", "symbol": "LABUSDT", "quantity": 0.0},
        ],
        "open_orders": [],
        "probe_evidence": [
            {
                "venue": "binance",
                "symbol": "LABUSDT",
                "endpoint": "fetch_all_positions",
                "classification": "position_probe_unfiltered_succeeded",
            },
            {
                "venue": "okx",
                "symbol": "LABUSDT",
                "endpoint": "fetch_all_positions",
                "classification": "position_probe_unfiltered_succeeded",
            },
            {
                "venue": "binance",
                "symbol": "LABUSDT",
                "endpoint": "fetch_open_orders",
                "classification": "open_order_probe_unfiltered_succeeded",
            },
            {
                "venue": "okx",
                "symbol": "LABUSDT",
                "endpoint": "fetch_open_orders",
                "classification": "open_order_probe_unfiltered_succeeded",
            },
            *(extra_evidence or []),
        ],
    }


def _terminal_pair_flat_truth(
    *,
    positions: list[dict[str, Any]] | None = None,
    open_order_truth: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "truth_available": True,
        "positions_flat": True,
        "open_orders_flat": True,
        "positions": (
            positions
            if positions is not None
            else [
                {"venue": "binance", "symbol": "LABUSDT", "quantity": 0.0},
                {"venue": "okx", "symbol": "LABUSDT", "quantity": 0.0},
            ]
        ),
        "open_order_truth": (
            open_order_truth
            if open_order_truth is not None
            else [
                {
                    "venue": "binance",
                    "symbol": "LABUSDT",
                    "open_orders_empty": True,
                },
                {
                    "venue": "okx",
                    "symbol": "LABUSDT",
                    "open_orders_empty": True,
                },
            ]
        ),
    }


def test_current_exchange_truth_overrides_stale_embedded_truth() -> None:
    reconciliation = {
        **_pair_reconciliation(),
        "exchange_truth": _terminal_pair_flat_truth(),
    }
    current_truth = _terminal_pair_flat_truth(
        positions=[
            {"venue": "binance", "symbol": "LABUSDT", "quantity": 0.0},
            {"venue": "okx", "symbol": "LABUSDT", "quantity": 0.25},
        ],
    )

    assert (
        close_reconciliation_exchange_truth(
            reconciliation,
            current_exchange_truth=current_truth,
        )
        is None
    )


def test_terminal_truth_empty_lists_fail_closed_without_probe_evidence() -> None:
    reconciliation = _pair_reconciliation()
    truth = _terminal_pair_flat_truth(positions=[], open_order_truth=[])

    assert not close_reconciliation_exchange_truth_clean(
        reconciliation,
        current_exchange_truth=truth,
    )


def test_venue_less_terminal_truth_rows_require_both_leg_probe_evidence() -> None:
    reconciliation = _pair_reconciliation()
    venue_less_truth = _terminal_pair_flat_truth(
        positions=[{"symbol": "LABUSDT", "quantity": 0.0}],
        open_order_truth=[{"symbol": "LABUSDT", "open_orders_empty": True}],
    )

    assert not close_reconciliation_exchange_truth_clean(
        reconciliation,
        current_exchange_truth=venue_less_truth,
    )

    venue_less_truth["probe_evidence"] = [
        {
            "venue": venue,
            "symbol": "LABUSDT",
            "endpoint": endpoint,
            "classification": classification,
        }
        for venue in ("binance", "okx")
        for endpoint, classification in (
            ("fetch_all_positions", "position_probe_unfiltered_succeeded"),
            ("fetch_open_orders", "open_order_probe_unfiltered_succeeded"),
        )
    ]

    assert close_reconciliation_exchange_truth_clean(
        reconciliation,
        current_exchange_truth=venue_less_truth,
    )


def test_dirty_current_truth_source_prevents_embedded_reconciliation_fallback() -> None:
    from lightfee.engine.close_runtime import CloseRuntime

    runtime = CloseRuntime(
        ctx=SimpleNamespace(
            _last_recovery_exchange_truth=_terminal_pair_flat_truth(
                positions=[
                    {"venue": "binance", "symbol": "LABUSDT", "quantity": 0.0},
                    {"venue": "okx", "symbol": "LABUSDT", "quantity": 0.5},
                ],
            )
        )
    )
    runtime._close_reconciliation_exchange_truth = None
    reconciliation = {
        **_pair_reconciliation(),
        "exchange_truth": _terminal_pair_flat_truth(),
    }

    assert runtime._current_exchange_truth_for_close_reconciliation(reconciliation) is None


def test_pair_terminal_truth_ignores_unrelated_venue_probe_error() -> None:
    truth = _scoped_flat_truth(
        [
            {
                "venue": "gate",
                "symbol": "LABUSDT",
                "endpoint": "fetch_all_positions",
                "classification": "position_probe_failed",
                "error": "timeout",
            }
        ]
    )

    assert close_reconciliation_exchange_truth_clean(
        _pair_reconciliation(),
        current_exchange_truth=truth,
    )


def test_pair_terminal_truth_blocks_relevant_venue_probe_error() -> None:
    truth = _scoped_flat_truth(
        [
            {
                "venue": "okx",
                "symbol": "LABUSDT",
                "endpoint": "fetch_all_positions",
                "classification": "position_probe_failed",
                "error": "timeout",
            }
        ]
    )

    assert not close_reconciliation_exchange_truth_clean(
        _pair_reconciliation(),
        current_exchange_truth=truth,
    )


def test_unscoped_recovered_flat_flags_do_not_archive_pair_close() -> None:
    stale_recovered_payload = {
        "truth_available": True,
        "positions_flat": True,
        "open_orders_flat": True,
    }

    assert not close_reconciliation_exchange_truth_clean(
        _pair_reconciliation(),
        current_exchange_truth=stale_recovered_payload,
    )


def test_app_logging_suppresses_noisy_transport_libraries(monkeypatch) -> None:
    from lightfee.apps.logging_config import configure_app_logging

    noisy_loggers = ("httpx", "httpcore", "websockets", "websockets.client")
    original_levels = {
        name: logging.getLogger(name).level
        for name in noisy_loggers
    }
    try:
        for name in noisy_loggers:
            logging.getLogger(name).setLevel(logging.NOTSET)

        configure_app_logging(level=logging.INFO)

        for name in noisy_loggers:
            assert logging.getLogger(name).getEffectiveLevel() >= logging.WARNING
    finally:
        for name, level in original_levels.items():
            logging.getLogger(name).setLevel(level)
