from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import PendingEntry
from lightfee.ops.production_health import analyze_current_state
from lightfee.venues.okx import OkxAdapter
from lightfee.venues.transport import TransportError, TransportErrorCategory


pytestmark = pytest.mark.live_harness

FIXTURE = (
    Path(__file__).parents[1]
    / "fixtures/live_incidents/2026-05-27/okx_recovery_probe_noise.jsonl"
)


def _load_case() -> dict:
    line = next(item for item in FIXTURE.read_text().splitlines() if item.strip())
    return json.loads(line)["payload"]


def _config(temp_dir: str, symbols: list[str]) -> AppConfig:
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
        symbols=symbols,
    )


def _records(config: AppConfig) -> list[dict]:
    path = Path(config.persistence.event_log_path)
    return [
        json.loads(line)
        for line in path.read_text().splitlines()
        if line.strip()
    ]


def _okx_adapter_with_catalog(case: dict) -> OkxAdapter:
    adapter = OkxAdapter(mode="paper")
    metadata: dict[str, dict] = {}
    for row in case["okx_catalog"]:
        inst_id = str(row["instId"])
        canonical = inst_id.replace("-USDT-SWAP", "USDT").replace("-SWAP", "")
        metadata[inst_id] = dict(row)
        metadata[canonical] = dict(row)
    adapter._transport.set_symbol_metadata(metadata)
    return adapter


def test_startup_probe_symbols_do_not_expand_clean_state_to_config_universe():
    case = _load_case()
    with tempfile.TemporaryDirectory() as td:
        config = _config(td, case["probe_symbols"])
        runtime = LiveRuntime(config)

        assert runtime._startup_position_probe_symbols(
            {"resolved_symbols": case["probe_symbols"]}
        ) == []

        runtime.state.pending_entries["pending-eth"] = PendingEntry(
            pending_id="pending-eth",
            symbol="ETHUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BINANCE,
            target_quantity=0.1,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1779804000000,
        )

        assert runtime._startup_position_probe_symbols(
            {"resolved_symbols": case["probe_symbols"]}
        ) == ["ETHUSDT"]


def test_startup_recovery_ledger_symbols_do_not_expand_clean_state_to_config_universe():
    case = _load_case()
    with tempfile.TemporaryDirectory() as td:
        config = _config(td, case["probe_symbols"])
        runtime = LiveRuntime(config)

        assert runtime._startup_recovery_ledger_symbols(
            {"resolved_symbols": case["probe_symbols"]}
        ) == []

        runtime.state.pending_entries["pending-eth"] = PendingEntry(
            pending_id="pending-eth",
            symbol="ETHUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BINANCE,
            target_quantity=0.1,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1779804000000,
        )

        assert runtime._startup_recovery_ledger_symbols(
            {"resolved_symbols": case["probe_symbols"]}
        ) == ["ETHUSDT"]


@pytest.mark.asyncio
async def test_okx_recovery_probe_prefers_single_bulk_positions_request(monkeypatch):
    case = _load_case()
    adapter = _okx_adapter_with_catalog(case)
    bulk_calls = 0

    async def fetch_all_positions():
        nonlocal bulk_calls
        bulk_calls += 1
        return [
            PositionSnapshot(
                venue=Venue.OKX,
                symbol="BTCUSDT",
                side=Side.BUY,
                quantity=0.25,
                entry_price=101000.0,
                observed_at_ms=1779804000000,
            )
        ]

    async def fetch_position(_symbol: str):
        raise AssertionError("OKX recovery must not fan out when bulk positions works")

    monkeypatch.setattr(adapter, "fetch_all_positions", fetch_all_positions)
    monkeypatch.setattr(adapter, "fetch_position", fetch_position)

    with tempfile.TemporaryDirectory() as td:
        config = _config(td, case["probe_symbols"])
        runtime = LiveRuntime(config, venue_adapters={Venue.OKX: adapter})
        runtime.journal.open()
        try:
            snapshots = await runtime._fetch_startup_live_position_snapshots(
                case["probe_symbols"]
            )
        finally:
            runtime.journal.close()

    assert bulk_calls == 1
    assert [(symbol, pos.symbol, pos.quantity) for symbol, pos in snapshots] == [
        ("BTCUSDT", "BTCUSDT", 0.25)
    ]


@pytest.mark.asyncio
async def test_okx_recovery_bulk_rate_limit_is_catalog_gated_and_classified(monkeypatch):
    case = _load_case()
    adapter = _okx_adapter_with_catalog(case)
    requested_position_symbols: list[str] = []

    async def fetch_all_positions():
        err = case["rate_limit_error"]
        raise TransportError(
            TransportErrorCategory.TRANSPORT_FAILURE,
            "too many requests",
            status_code=int(err["status_code"]),
            body=str(err["body"]),
            headers=dict(err["headers"]),
        )

    async def fetch_position(symbol: str):
        requested_position_symbols.append(symbol)
        return PositionSnapshot(
            venue=Venue.OKX,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1779804000000,
        )

    monkeypatch.setattr(adapter, "fetch_all_positions", fetch_all_positions)
    monkeypatch.setattr(adapter, "fetch_position", fetch_position)

    with tempfile.TemporaryDirectory() as td:
        config = _config(td, case["probe_symbols"])
        runtime = LiveRuntime(config, venue_adapters={Venue.OKX: adapter})
        runtime.journal.open()
        try:
            snapshots = await runtime._fetch_startup_live_position_snapshots(
                case["probe_symbols"]
            )
        finally:
            runtime.journal.close()

            records = _records(config)

    assert snapshots == []
    assert requested_position_symbols == []

    rate_limited = next(
        record["payload"]
        for record in records
        if record["kind"] == "recovery.live_position_probe_venue_cooldown"
        and record["payload"].get("classification") == "rate_limited"
    )
    assert rate_limited["retryable"] is True
    assert rate_limited["cooldown_scope"] == "venue:okx:private_positions"
    assert rate_limited["endpoint"] == "/api/v5/account/positions"
    assert rate_limited["retry_after_ms"] == 2000
    assert rate_limited["rate_limit_budget"] == {
        "requests": 10,
        "window_ms": 2000,
        "scope": "User ID",
    }

    skipped = next(
        record["payload"]
        for record in records
        if record["kind"] == "recovery.live_position_probe_unsupported_symbols"
    )
    assert skipped["venue"] == "okx"
    assert skipped["requested_symbols"] == case["probe_symbols"]
    assert skipped["skipped_by_catalog"] == ["CHIPUSDT", "DELISTEDUSDT", "OLDUSDT"]
    assert skipped["symbol_count"] == 5


@pytest.mark.asyncio
async def test_okx_recovery_bulk_timeout_does_not_fan_out_positions(monkeypatch):
    case = _load_case()
    adapter = _okx_adapter_with_catalog(case)
    requested_position_symbols: list[str] = []

    async def fetch_all_positions():
        raise asyncio.TimeoutError()

    async def fetch_position(symbol: str):
        requested_position_symbols.append(symbol)
        return PositionSnapshot(
            venue=Venue.OKX,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1779804000000,
        )

    monkeypatch.setattr(adapter, "fetch_all_positions", fetch_all_positions)
    monkeypatch.setattr(adapter, "fetch_position", fetch_position)

    with tempfile.TemporaryDirectory() as td:
        config = _config(td, case["probe_symbols"])
        runtime = LiveRuntime(config, venue_adapters={Venue.OKX: adapter})
        runtime.journal.open()
        try:
            snapshots = await runtime._fetch_startup_live_position_snapshots(
                case["probe_symbols"]
            )
        finally:
            runtime.journal.close()

        records = _records(config)

    assert snapshots == []
    assert requested_position_symbols == []
    timeout = next(
        record["payload"]
        for record in records
        if record["kind"] == "recovery.live_position_bulk_diagnostic_error"
    )
    assert timeout["classification"] == "timeout"
    assert timeout["endpoint"] == "/api/v5/account/positions"
    assert timeout["symbol_count"] == 2
    assert timeout["requested_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert timeout["truth_required_by"] == []
    assert timeout["diagnostic_scope"] == "best_effort_bulk_positions"
    assert timeout["blocking"] is False
    assert timeout["decision"] == "running_with_nonblocking_health_diagnostic"
    assert not any(
        record["kind"] == "recovery.required_position_truth_unavailable"
        for record in records
    )


@pytest.mark.asyncio
async def test_okx_recovery_bulk_timeout_with_truth_required_work_uses_bounded_fallback(
    monkeypatch,
):
    case = _load_case()
    adapter = _okx_adapter_with_catalog(case)
    requested_position_symbols: list[str] = []

    async def fetch_all_positions():
        raise asyncio.TimeoutError()

    async def fetch_position(symbol: str):
        requested_position_symbols.append(symbol)
        return PositionSnapshot(
            venue=Venue.OKX,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1779804000000,
        )

    monkeypatch.setattr(adapter, "fetch_all_positions", fetch_all_positions)
    monkeypatch.setattr(adapter, "fetch_position", fetch_position)

    with tempfile.TemporaryDirectory() as td:
        config = _config(td, case["probe_symbols"])
        runtime = LiveRuntime(config, venue_adapters={Venue.OKX: adapter})
        runtime.state.pending_residual_repairs.append(
            {
                "position_id": "entry-okx-timeout-repair",
                "pair_id": "ethusdt:binance->okx",
                "symbol": "ETHUSDT",
                "origin": "entry_open",
                "repair_venue": "okx",
                "repair_side": "buy",
                "repair_quantity": 1.0,
            }
        )
        runtime.journal.open()
        try:
            snapshots = await runtime._fetch_startup_live_position_snapshots([])
        finally:
            runtime.journal.close()

        records = _records(config)

    assert snapshots == []
    assert requested_position_symbols == ["ETHUSDT"]
    timeout = next(
        record["payload"]
        for record in records
        if record["kind"] == "recovery.required_position_bulk_fallback_planned"
    )
    assert timeout["classification"] == "timeout"
    assert timeout["truth_required_by"] == ["pending_residual_repair"]
    assert timeout["fallback_symbol_count"] == 1
    assert timeout["fallback_symbols_sample"] == ["ETHUSDT"]
    assert timeout["fallback_planned"] is True
    assert timeout["blocking"] is False
    assert timeout["decision"] == "bounded_symbol_fallback_required"
    assert not any(
        record["kind"] == "recovery.required_position_truth_unavailable"
        for record in records
    )


def test_okx_instrument_missing_is_non_retryable_metadata_skip_not_health_critical():
    case = _load_case()
    adapter = _okx_adapter_with_catalog(case)
    with tempfile.TemporaryDirectory() as td:
        config = _config(td, case["probe_symbols"])
        runtime = LiveRuntime(config, venue_adapters={Venue.OKX: adapter})
        payload = runtime._position_probe_exception_payload(
            Venue.OKX,
            adapter,
            TransportError(
                TransportErrorCategory.NORMALIZATION_FAILURE,
                case["instrument_missing_error"],
            ),
            symbol="CHIPUSDT",
        )

    assert payload["classification"] == "instrument_missing"
    assert payload["retryable"] is False
    assert payload["skip_reason"] == "catalog_or_metadata_missing"

    report = analyze_current_state(
        case["current_state"],
        now_ms=1779804001000,
        max_tick_age_ms=10_000,
    )
    assert report.name == "current_state"
    assert report.severity != "critical"
