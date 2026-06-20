from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from lightfee.config.schema import AppConfig, StrategyConfig, VenueConfig
from lightfee.core.domain import OrderFill, PositionSnapshot, Side, TimeInForce, Venue
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.recovery_startup_runtime import RecoveryStartupRuntime
from lightfee.engine.state import EngineState
from lightfee.engine.unpaired_live_position_recovery import (
    UnpairedLivePositionRecoveryRuntime,
)
from lightfee.persistence.journal import Journal
from tests.fake_adapters import FakeVenueAdapter


class OpenOrderTruthAdapter(FakeVenueAdapter):
    def __init__(
        self,
        venue: Venue,
        *,
        positions: list[PositionSnapshot] | None = None,
        open_orders: list[Any] | None = None,
        open_order_error: Exception | None = None,
    ) -> None:
        super().__init__(venue)
        self.position_snapshots = positions or []
        self.open_orders = list(open_orders or [])
        self.open_order_error = open_order_error
        self.fetch_open_orders_call_count = 0

    async def fetch_open_orders(self, symbol: str) -> list[Any]:
        self.fetch_open_orders_call_count += 1
        if self.open_order_error is not None:
            raise self.open_order_error
        return list(self.open_orders)


def _ctx(
    tmp_path: Path,
    *,
    auto_enabled: bool = False,
    symbol: str = "ESPORTSUSDT",
    venue: Venue = Venue.BINANCE,
    symbols: list[str] | None = None,
    venues: list[Venue] | None = None,
    adapter: OpenOrderTruthAdapter | None = None,
    cap: float = 30.0,
) -> SimpleNamespace:
    journal = Journal(tmp_path / "events.jsonl")
    journal.open()
    configured_symbols = [symbol] if symbols is None else list(symbols)
    configured_venues = [venue] if venues is None else list(venues)
    config = AppConfig(
        symbols=configured_symbols,
        venues=[VenueConfig(venue=item.value) for item in configured_venues],
        strategy=StrategyConfig(
            live_entry_notional_cap_quote=cap,
            unpaired_live_position_auto_recovery_enabled=auto_enabled,
        ),
    )
    adapters = {venue: adapter or OpenOrderTruthAdapter(venue)}
    return SimpleNamespace(
        state=EngineState(),
        config=config,
        journal=journal,
        get_venue_adapter=lambda v: adapters.get(v),
    )


def _ledger_for_position(
    *,
    symbol: str = "ESPORTSUSDT",
    venue: Venue = Venue.BINANCE,
    side: str = "sell",
    quantity: float = 592.0,
    entry_price: float = 0.02,
    local: dict[str, Any] | None = None,
) -> RecoveryLedger:
    return RecoveryLedger.from_local_and_exchange_truth(
        local=local or {},
        exchange_truth={
            "positions": [
                {
                    "venue": venue.value,
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "entry_price": entry_price,
                }
            ],
            "open_orders": [],
        },
    )


def _events(ctx: SimpleNamespace) -> list[dict[str, Any]]:
    ctx.journal.close()
    path = ctx.journal.path
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _kinds(ctx: SimpleNamespace) -> list[str]:
    return [event["kind"] for event in _events(ctx)]


def test_registers_unpaired_work_without_creating_owner_state(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)

    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    assert ctx.state.unpaired_live_position_recoveries == [
        {
            "venue": "binance",
            "symbol": "ESPORTSUSDT",
            "side": "sell",
            "quantity": 592.0,
            "notional_quote": pytest.approx(11.84),
            "first_seen_ms": 1_000,
            "attempt_count": 0,
            "next_attempt_ms": 1_000,
            "last_error": "",
            "terminal_status": "",
            "owner_excluded": True,
            "open_order_truth_available": False,
            "cap_quote": 30.0,
            "cap_ok": True,
        }
    ]
    assert ctx.state.open_positions == {}
    assert ctx.state.pending_entries == {}
    assert ctx.state.pending_residual_repairs == []
    assert "recovery.unpaired_live_position_detected" in _kinds(ctx)
    assert "recovery.unpaired_live_position_owner_excluded" in _kinds(ctx)


def test_owned_local_position_does_not_enter_unpaired_route(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)

    ledger = _ledger_for_position(local={"open_positions": [{"symbol": "ESPORTSUSDT"}]})
    runtime.register_from_ledger(ledger, now_ms=1_000)

    assert ctx.state.unpaired_live_position_recoveries == []


@pytest.mark.asyncio
async def test_auto_disabled_keeps_work_diagnostic_only(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100)
        ],
    )
    ctx = _ctx(tmp_path, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    assert ctx.state.unpaired_live_position_recoveries[0]["terminal_status"] == ""
    events = _events(ctx)
    skipped = [
        event["payload"]
        for event in events
        if event["kind"] == "recovery.unpaired_live_position_cleanup_skipped"
    ]
    assert skipped
    assert skipped[-1]["current_risk_exposure"] is True
    assert skipped[-1]["business_terminal"] is False
    assert skipped[-1]["diagnostic_severity"] == "critical"
    assert skipped[-1]["next_action"] == "operator_or_config_enable_required"


@pytest.mark.asyncio
async def test_auto_enabled_submits_reduce_only_and_marks_terminal_flat(
    tmp_path: Path,
) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100),
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 0.0, 0.0, 1_200),
        ],
    )
    adapter.place_order_outcomes = [
        OrderFill(
            venue=Venue.BINANCE,
            symbol="ESPORTSUSDT",
            side=Side.BUY,
            quantity=592.0,
            price=0.02,
            order_id="reduce-1",
            filled_at_ms=1_150,
        )
    ]
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 1
    assert adapter.last_request is not None
    assert adapter.last_request.reduce_only is True
    assert adapter.last_request.side is Side.BUY
    assert adapter.last_request.time_in_force is TimeInForce.IOC
    assert ctx.state.unpaired_live_position_recoveries[0]["terminal_status"] == "flat"
    assert ctx.state.open_positions == {}
    assert ctx.state.pending_entries == {}
    assert ctx.state.pending_residual_repairs == []
    kinds = _kinds(ctx)
    assert "recovery.unpaired_live_position_cleanup_attempt" in kinds
    assert "recovery.unpaired_live_position_cleanup_submitted" in kinds
    assert "recovery.unpaired_live_position_cleanup_succeeded" in kinds
    assert "recovery.unpaired_live_position_terminal_flat" in kinds


@pytest.mark.asyncio
async def test_empty_symbol_allowlist_does_not_auto_cleanup(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100)
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter, symbols=[])
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    assert ctx.state.unpaired_live_position_recoveries[0]["last_error"] == (
        "symbol_not_configured"
    )


@pytest.mark.asyncio
async def test_empty_venue_allowlist_does_not_auto_cleanup(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100)
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter, venues=[])
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    assert ctx.state.unpaired_live_position_recoveries[0]["last_error"] == (
        "venue_not_configured"
    )


@pytest.mark.asyncio
async def test_stale_position_truth_skips_cleanup(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_000)
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=20_000)

    assert adapter.place_order_call_count == 0
    record = ctx.state.unpaired_live_position_recoveries[0]
    assert record["last_error"] == "position_truth_stale"
    assert record["position_truth_age_ms"] == 19_000


@pytest.mark.asyncio
async def test_reduce_only_open_order_after_submit_prevents_terminal_flat(
    tmp_path: Path,
) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100),
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 0.0, 0.0, 1_200),
        ],
        open_orders=[
            {
                "symbol": "ESPORTSUSDT",
                "quantity": 592.0,
                "reduce_only": True,
            }
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 1
    record = ctx.state.unpaired_live_position_recoveries[0]
    assert record["terminal_status"] == ""
    assert record["last_error"] == "open_orders_still_present"
    assert "recovery.unpaired_live_position_cleanup_failed" in _kinds(ctx)


@pytest.mark.asyncio
async def test_open_order_truth_unavailable_skips_cleanup(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100)
        ],
        open_order_error=RuntimeError("open order truth unavailable"),
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    record = ctx.state.unpaired_live_position_recoveries[0]
    assert record["terminal_status"] == ""
    assert record["last_error"] == "open_order_truth_unavailable"
    assert "recovery.unpaired_live_position_cleanup_skipped" in _kinds(ctx)


@pytest.mark.asyncio
async def test_non_reduce_open_order_conflict_skips_cleanup(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100)
        ],
        open_orders=[
            {
                "symbol": "ESPORTSUSDT",
                "quantity": 10.0,
                "reduce_only": False,
            }
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    assert ctx.state.unpaired_live_position_recoveries[0]["last_error"] == (
        "non_reduce_open_order_conflict"
    )


@pytest.mark.asyncio
async def test_open_order_envelope_conflict_skips_cleanup(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100)
        ],
        open_orders=[
            {
                "orders": [
                    {
                        "symbol": "ESPORTSUSDT",
                        "qty": 10.0,
                        "reduce_only": False,
                    }
                ]
            }
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    assert ctx.state.unpaired_live_position_recoveries[0]["last_error"] == (
        "non_reduce_open_order_conflict"
    )


@pytest.mark.asyncio
async def test_reduce_only_false_string_and_orig_qty_conflict_skips_cleanup(
    tmp_path: Path,
) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100)
        ],
        open_orders=[
            {
                "symbol": "ESPORTSUSDT",
                "origQty": "10",
                "reduceOnly": "false",
            }
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    assert ctx.state.unpaired_live_position_recoveries[0]["last_error"] == (
        "non_reduce_open_order_conflict"
    )


@pytest.mark.asyncio
async def test_notional_cap_exceeded_skips_cleanup(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 2.0, 100.0, 1_100)
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter, cap=30.0)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(
        _ledger_for_position(quantity=2.0, entry_price=100.0),
        now_ms=1_000,
    )

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    record = ctx.state.unpaired_live_position_recoveries[0]
    assert record["cap_ok"] is False
    assert record["last_error"] == "cap_exceeded"


@pytest.mark.asyncio
async def test_unknown_notional_skips_cleanup(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.0, 1_100)
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter, cap=30.0)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(
        _ledger_for_position(entry_price=0.0),
        now_ms=1_000,
    )

    await runtime.drive(now_ms=1_100)

    assert adapter.place_order_call_count == 0
    record = ctx.state.unpaired_live_position_recoveries[0]
    assert record["cap_ok"] is False
    assert record["last_error"] == "notional_unknown"


@pytest.mark.asyncio
async def test_submit_then_nonzero_position_backs_off(tmp_path: Path) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100),
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 100.0, 0.02, 1_200),
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    await runtime.drive(now_ms=1_100)

    record = ctx.state.unpaired_live_position_recoveries[0]
    assert adapter.place_order_call_count == 1
    assert record["attempt_count"] == 1
    assert record["terminal_status"] == ""
    assert record["last_error"] == "position_still_nonzero"
    assert record["next_attempt_ms"] > 1_100


def test_terminal_flat_record_does_not_count_as_recovery_work(tmp_path: Path) -> None:
    from lightfee.engine.recovery import (
        build_recovery_snapshot,
        classify_startup_recovery_state,
        needs_reconciliation,
    )

    ctx = _ctx(tmp_path)
    ctx.state.unpaired_live_position_recoveries.append(
        {
            "venue": "binance",
            "symbol": "ESPORTSUSDT",
            "side": "sell",
            "quantity": 0.0,
            "notional_quote": 0.0,
            "first_seen_ms": 1_000,
            "attempt_count": 1,
            "next_attempt_ms": 1_100,
            "last_error": "",
            "terminal_status": "flat",
        }
    )

    snapshot = build_recovery_snapshot(ctx.state)

    assert snapshot.has_unpaired_live_position_recoveries is False
    assert needs_reconciliation(ctx.state) is False
    assert classify_startup_recovery_state(ctx.state) == "clean"


def test_ledger_clean_terminalizes_stale_unpaired_recovery(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)
    runtime.register_from_ledger(_ledger_for_position(), now_ms=1_000)

    runtime.register_from_ledger(
        RecoveryLedger.from_local_and_exchange_truth(
            local={},
            exchange_truth={"positions": [], "open_orders": []},
        ),
        now_ms=2_000,
    )

    record = ctx.state.unpaired_live_position_recoveries[0]
    assert record["terminal_status"] == "flat"
    assert record["last_error"] == ""
    assert "recovery.unpaired_live_position_terminal_flat" in _kinds(ctx)


def test_has_local_recovery_work_tracks_only_active_unpaired_work(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    startup = RecoveryStartupRuntime(ctx)
    ctx.state.unpaired_live_position_recoveries.append(
        {
            "venue": "binance",
            "symbol": "ESPORTSUSDT",
            "terminal_status": "",
        }
    )

    assert startup._has_local_recovery_work() is True

    ctx.state.unpaired_live_position_recoveries[0]["terminal_status"] = "flat"
    assert startup._has_local_recovery_work() is False


@pytest.mark.asyncio
async def test_max_attempts_enters_manual_required_without_repeating(
    tmp_path: Path,
) -> None:
    adapter = OpenOrderTruthAdapter(
        Venue.BINANCE,
        positions=[
            PositionSnapshot(Venue.BINANCE, "ESPORTSUSDT", Side.SELL, 592.0, 0.02, 1_100)
        ],
    )
    ctx = _ctx(tmp_path, auto_enabled=True, adapter=adapter)
    ctx.state.unpaired_live_position_recoveries.append(
        {
            "venue": "binance",
            "symbol": "ESPORTSUSDT",
            "side": "sell",
            "quantity": 592.0,
            "notional_quote": 11.84,
            "first_seen_ms": 1_000,
            "attempt_count": 3,
            "next_attempt_ms": 1_100,
            "last_error": "position_still_nonzero",
            "terminal_status": "",
        }
    )
    runtime = UnpairedLivePositionRecoveryRuntime(ctx)

    await runtime.drive(now_ms=1_100)
    await runtime.drive(now_ms=31_100)

    record = ctx.state.unpaired_live_position_recoveries[0]
    assert adapter.place_order_call_count == 0
    assert record["terminal_status"] == "manual_required"
    assert record["last_error"] == "max_attempts_exceeded"
    assert _kinds(ctx).count("recovery.unpaired_live_position_cleanup_failed") == 1
