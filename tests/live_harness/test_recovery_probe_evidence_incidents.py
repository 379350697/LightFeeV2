from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.engine.runtime import LiveRuntime
from lightfee.venues.transport import TransportError, TransportErrorCategory
from tests.fake_adapters import FakeVenueAdapter


pytestmark = pytest.mark.live_harness


def _config(temp_dir: str) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            live_recovery_rest_probe_timeout_ms=50,
            sidecar_snapshot_path=str(Path(temp_dir) / "sidecar.json"),
        ),
        strategy=StrategyConfig(risk_monitor_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(Path(temp_dir) / "events.jsonl"),
            snapshot_path=str(Path(temp_dir) / "state.json"),
        ),
        venues=[],
        symbols=["BTCUSDT"],
    )


def _records(config: AppConfig) -> list[dict]:
    return [
        json.loads(line)
        for line in Path(config.persistence.event_log_path).read_text().splitlines()
        if line.strip()
    ]


class _CatalogAdapter(FakeVenueAdapter):
    def __init__(self, venue: Venue = Venue.BITGET):
        super().__init__(venue)
        self.fetch_position_symbols: list[str] = []
        self._transport = SimpleNamespace(
            _spec=SimpleNamespace(position_path="/private/position"),
            _venue_symbol=lambda symbol: f"{symbol}_UMCBL",
        )

    def supported_symbols(self) -> list[str]:
        return ["BTCUSDT"]

    async def fetch_all_positions(self):
        return None

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        self.fetch_position_symbols.append(symbol)
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1700000000000,
        )


@pytest.mark.asyncio
async def test_unsupported_symbols_are_aggregate_evidence_not_probe_errors():
    with tempfile.TemporaryDirectory() as td:
        config = _config(td)
        adapter = _CatalogAdapter()
        runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: adapter})
        runtime.journal.open()
        try:
            await runtime._fetch_startup_live_position_snapshots(
                ["BTCUSDT", "DELISTEDUSDT", "OLDUSDT"]
            )
        finally:
            runtime.journal.close()

        records = _records(config)

    assert adapter.fetch_position_symbols == ["BTCUSDT"]
    unsupported = [
        record
        for record in records
        if record["kind"] == "recovery.live_position_probe_unsupported_symbols"
    ]
    assert len(unsupported) == 1
    assert unsupported[0]["payload"]["venue"] == "bitget"
    assert unsupported[0]["payload"]["unsupported_count"] == 2
    assert unsupported[0]["payload"]["sample_symbols"] == [
        "DELISTEDUSDT",
        "OLDUSDT",
    ]
    assert not any(
        record["kind"] == "recovery.live_position_probe_error"
        and record["payload"].get("reason") == "unsupported_symbol"
        for record in records
    )


@pytest.mark.asyncio
async def test_static_config_probe_skip_is_bounded_summary_evidence():
    with tempfile.TemporaryDirectory() as td:
        config = _config(td)
        config.symbols = [f"SYM{i}USDT" for i in range(100)]
        adapter = _CatalogAdapter()
        runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: adapter})
        runtime.journal.open()
        try:
            await runtime._fetch_startup_live_position_snapshots([])
        finally:
            runtime.journal.close()

        payload = next(
            record["payload"]
            for record in _records(config)
            if record["kind"] == "recovery.live_position_static_config_probe_skipped"
        )

    assert payload["event_scope"] == "bounded_summary"
    assert payload["static_symbol_count"] == 100
    assert payload["max_static_symbol_count"] == runtime._MAX_STATIC_RECOVERY_PROBE_SYMBOLS
    assert payload["sample_symbols"] == [f"SYM{i}USDT" for i in range(10)]
    assert payload["omitted_symbol_count"] == 90
    assert payload["decision"] == "skip_per_symbol_fallback"
    assert payload["suppressed_count"] == 0


@pytest.mark.asyncio
async def test_static_config_probe_skip_suppresses_repeated_same_venue_evidence():
    with tempfile.TemporaryDirectory() as td:
        config = _config(td)
        config.symbols = [f"SYM{i}USDT" for i in range(100)]
        adapter = _CatalogAdapter()
        runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: adapter})
        runtime.journal.open()
        try:
            await runtime._fetch_startup_live_position_snapshots([])
            await runtime._fetch_startup_live_position_snapshots([])
        finally:
            runtime.journal.close()

        events = [
            record
            for record in _records(config)
            if record["kind"] == "recovery.live_position_static_config_probe_skipped"
        ]

    assert len(events) == 1
    assert events[0]["payload"]["event_scope"] == "bounded_summary"
    assert events[0]["payload"]["suppressed_count"] == 0


@pytest.mark.asyncio
async def test_timeout_probe_error_is_structured_and_non_empty():
    class TimeoutAdapter(_CatalogAdapter):
        async def fetch_position(self, symbol: str) -> PositionSnapshot:
            raise asyncio.TimeoutError()

    with tempfile.TemporaryDirectory() as td:
        config = _config(td)
        adapter = TimeoutAdapter()
        runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: adapter})
        runtime.journal.open()
        try:
            await runtime._fetch_startup_live_position_snapshots(["BTCUSDT"])
        finally:
            runtime.journal.close()

        payload = next(
            record["payload"]
            for record in _records(config)
            if record["kind"] == "recovery.live_position_probe_error"
        )

    assert payload["venue"] == "bitget"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["venue_symbol"] == "BTCUSDT_UMCBL"
    assert payload["endpoint"] == "/private/position"
    assert payload["classification"] == "timeout"
    assert payload["exception_class"] == "TimeoutError"
    assert payload["error"]


@pytest.mark.asyncio
async def test_rate_limit_probe_error_is_structured_and_non_empty():
    class RateLimitedAdapter(_CatalogAdapter):
        async def fetch_position(self, symbol: str) -> PositionSnapshot:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                "too many requests",
                status_code=429,
                body='{"code":"50011","msg":"Rate limit reached"}',
            )

    with tempfile.TemporaryDirectory() as td:
        config = _config(td)
        adapter = RateLimitedAdapter()
        runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: adapter})
        runtime.journal.open()
        try:
            await runtime._fetch_startup_live_position_snapshots(["BTCUSDT"])
        finally:
            runtime.journal.close()

        payload = next(
            record["payload"]
            for record in _records(config)
            if record["kind"] == "recovery.live_position_probe_error"
        )

    assert payload["venue"] == "bitget"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["endpoint"] == "/private/position"
    assert payload["classification"] == "rate_limited"
    assert payload["exception_class"] == "TransportError"
    assert payload["retCode"] == "50011"
    assert payload["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exc", "expected_kind", "expected_classification"),
    [
        (
            asyncio.TimeoutError(),
            "recovery.live_position_bulk_probe_error",
            "timeout",
        ),
        (
            TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                "too many requests",
                status_code=429,
                body='{"code":"50011","msg":"Rate limit reached"}',
            ),
            "recovery.live_position_probe_venue_cooldown",
            "rate_limited",
        ),
    ],
)
async def test_bulk_probe_errors_keep_aggregate_symbol_evidence(
    exc: Exception,
    expected_kind: str,
    expected_classification: str,
):
    class BulkFailingAdapter(_CatalogAdapter):
        def supported_symbols(self) -> list[str]:
            return ["BTCUSDT", "ETHUSDT"]

        async def fetch_all_positions(self):
            raise exc

    with tempfile.TemporaryDirectory() as td:
        config = _config(td)
        adapter = BulkFailingAdapter()
        runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: adapter})
        runtime.journal.open()
        try:
            await runtime._fetch_startup_live_position_snapshots(["BTCUSDT", "ETHUSDT"])
        finally:
            runtime.journal.close()

        payload = next(
            record["payload"]
            for record in _records(config)
            if record["kind"] == expected_kind
        )

    assert payload["venue"] == "bitget"
    assert payload["symbol"] == "*"
    assert payload["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert payload["endpoint"] == "/private/position"
    assert payload["classification"] == expected_classification
    assert payload["exception_class"]
    assert payload["error"]


@pytest.mark.asyncio
async def test_instrument_missing_probe_error_is_structured_and_non_empty():
    class InstrumentMissingAdapter(_CatalogAdapter):
        def __init__(self):
            super().__init__(Venue.OKX)
            self._transport = SimpleNamespace(
                _spec=SimpleNamespace(position_path="/api/v5/account/positions"),
                _venue_symbol=lambda symbol: str(symbol).replace("USDT", "-USDT-SWAP"),
            )

        def supported_symbols(self) -> list[str]:
            return ["CHIP-USDT-SWAP"]

        async def fetch_position(self, symbol: str) -> PositionSnapshot:
            raise TransportError(
                TransportErrorCategory.NORMALIZATION_FAILURE,
                (
                    "okx_contract_metadata_missing_ct_val "
                    "classification=instrument_missing "
                    "instId=CHIP-USDT-SWAP"
                ),
            )

    with tempfile.TemporaryDirectory() as td:
        config = _config(td)
        adapter = InstrumentMissingAdapter()
        runtime = LiveRuntime(config, venue_adapters={Venue.OKX: adapter})
        runtime.journal.open()
        try:
            await runtime._fetch_startup_live_position_snapshots(["CHIPUSDT"])
        finally:
            runtime.journal.close()

        payload = next(
            record["payload"]
            for record in _records(config)
            if record["kind"] == "recovery.live_position_probe_metadata_missing"
        )

    assert payload["venue"] == "okx"
    assert payload["symbol"] == "CHIPUSDT"
    assert payload["venue_symbol"] == "CHIP-USDT-SWAP"
    assert payload["endpoint"] == "/api/v5/account/positions"
    assert payload["classification"] == "instrument_missing"
    assert payload["exception_class"] == "TransportError"
    assert payload["error"]
