from __future__ import annotations

import json
from pathlib import Path

import pytest

from lightfee.core.domain import (
    OrderFill,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.engine.passive_close import PassiveCloseExecutor
from lightfee.engine.state import (
    ActiveMakerLeg,
    EngineState,
    OpenPosition,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingPassiveClose,
    PendingPassiveLegFill,
)
from lightfee.persistence.journal import Journal


pytestmark = pytest.mark.live_harness
FIXTURE_ROOT = Path("tests/fixtures/live_incidents")


class _HistoricalFlatAdapter:
    def __init__(
        self,
        venue: Venue,
        symbol: str,
        side: Side,
        *,
        live_quantity: float = 0.0,
        progress: PassiveOrderProgress | None = None,
        normalized_quantity: float | None = None,
    ) -> None:
        self.venue = venue
        self.symbol = symbol
        self.side = side
        self.live_quantity = live_quantity
        self.progress = progress
        self.normalized_quantity = normalized_quantity
        self.fetch_position_calls: list[str] = []
        self.place_order_calls: list[object] = []
        self.submit_passive_order_calls: list[object] = []
        self.query_passive_order_progress_calls: list[tuple[str, str, str | None]] = []

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        self.fetch_position_calls.append(symbol)
        return PositionSnapshot(
            venue=self.venue,
            symbol=self.symbol,
            side=self.side,
            quantity=self.live_quantity,
            entry_price=0.01 if self.live_quantity else 0.0,
            observed_at_ms=1779698773650,
        )

    async def fetch_all_positions(self) -> list[PositionSnapshot]:
        return []

    async def query_passive_order_progress(
        self,
        symbol: str,
        order_id: str,
        client_order_id: str | None = None,
        **_kwargs,
    ) -> PassiveOrderProgress | None:
        self.query_passive_order_progress_calls.append((symbol, order_id, client_order_id))
        return self.progress

    async def place_order(self, request) -> OrderFill:
        self.place_order_calls.append(request)
        raise AssertionError("historical live-flat harness must not place orders")

    async def submit_passive_order(self, request) -> OrderFill:
        self.submit_passive_order_calls.append(request)
        raise AssertionError("historical live-flat harness must not submit passive orders")

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        if self.normalized_quantity is not None:
            return self.normalized_quantity
        return quantity

    def price_tick_size(self, symbol: str | None = None) -> float:
        return 0.0001


def _journal(tmp_path, name: str) -> Journal:
    journal = Journal(tmp_path / name)
    journal.open()
    return journal


def _load_incident_fixture(date: str, filename: str) -> dict:
    return json.loads((FIXTURE_ROOT / date / filename).read_text())


def _venue(value: str) -> Venue:
    return Venue(value)


def _active_leg(value: str) -> ActiveMakerLeg:
    return ActiveMakerLeg(value)


def _side_for_live_position(quantity: float) -> Side:
    if quantity < 0:
        return Side.SELL
    return Side.BUY


def _open_position(
    *,
    position_id: str,
    symbol: str,
    long_venue: Venue,
    short_venue: Venue,
    quantity: float,
) -> OpenPosition:
    return OpenPosition(
        position_id=position_id,
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        long_quantity=quantity,
        short_quantity=quantity,
        long_entry_price=0.01,
        short_entry_price=0.01,
        opened_at_ms=1779698770000,
        matched_quantity=quantity,
    )


def _pending_close(
    position: OpenPosition,
    *,
    include_snapshot: bool,
    active_leg: ActiveMakerLeg,
) -> PendingPassiveClose:
    return PendingPassiveClose(
        position_id=position.position_id,
        reason="funding_capture",
        position_snapshot=position if include_snapshot else None,
        target_quantity=position.matched_quantity,
        chunk_quantities=[position.matched_quantity],
        active_chunk_index=0,
        phase_state=PassivePhaseState(
            phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
            active_maker_leg=active_leg,
        ),
        maker_fill=PendingPassiveLegFill(quantity=0.0),
        hedge_fill=PendingPassiveLegFill(quantity=0.0),
        next_retry_at_ms=0,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_id",
    [
        "gmtusdt_missing_pending_snapshot_live_flat",
        "lynusdt_missing_pending_snapshot_live_flat",
    ],
)
async def test_20260523_terminal_flat_fixture_uses_recovered_open_position(
    tmp_path,
    case_id: str,
):
    """GMT/LYN terminal-flat incident: recovered pending may lack position_snapshot."""

    fixture = _load_incident_fixture("2026-05-23", "passive_close_terminal_flat.json")
    case = next(item for item in fixture["incidents"] if item["case_id"] == case_id)
    long_venue = _venue(case["long_venue"])
    short_venue = _venue(case["short_venue"])
    symbol = case["symbol"]
    live_positions = case["live_positions"]
    journal = _journal(tmp_path, f"{case_id}.jsonl")
    long_adapter = _HistoricalFlatAdapter(
        long_venue,
        symbol,
        _side_for_live_position(float(live_positions[long_venue.value])),
        live_quantity=float(live_positions[long_venue.value]),
    )
    short_adapter = _HistoricalFlatAdapter(
        short_venue,
        symbol,
        _side_for_live_position(float(live_positions[short_venue.value])),
        live_quantity=float(live_positions[short_venue.value]),
    )
    executor = PassiveCloseExecutor(
        {long_venue: long_adapter, short_venue: short_adapter},
        journal,
    )
    state = EngineState()
    position = _open_position(
        position_id=case["position_id"],
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        quantity=float(case["matched_quantity"]),
    )
    pending = _pending_close(
        position,
        include_snapshot=bool(case["pending_position_snapshot_present"]),
        active_leg=_active_leg(case["active_maker_leg"]),
    )
    state.open_positions[position.position_id] = position
    state.pending_passive_closes[position.position_id] = pending

    remaining = await executor.process_pending_passive_closes(
        state,
        now_ms=1779698773650,
    )

    assert remaining == set()
    assert state.open_positions == {}
    assert state.pending_passive_closes == {}
    assert long_adapter.fetch_position_calls == [symbol, symbol]
    assert short_adapter.fetch_position_calls == [symbol, symbol]
    assert long_adapter.place_order_calls == []
    assert short_adapter.place_order_calls == []
    kinds = [record["kind"] for record in journal.read_all()]
    for expected in case["expected_events"]:
        assert expected in kinds


@pytest.mark.asyncio
async def test_20260525_xcnusdt_recovered_live_flat_clears_with_drift_payload(tmp_path):
    """XCNUSDT incident: recovered local state is subordinate to live-flat truth."""

    fixture = _load_incident_fixture("2026-05-25", "passive_close_recovered_flat.json")
    case = next(
        item for item in fixture["incidents"]
        if item["case_id"] == "xcnusdt_recovered_bybit_aster_live_flat"
    )
    long_venue = _venue(case["long_venue"])
    short_venue = _venue(case["short_venue"])
    symbol = case["symbol"]
    live_positions = case["live_positions"]
    journal = _journal(tmp_path, "xcn-events.jsonl")
    long_adapter = _HistoricalFlatAdapter(
        long_venue,
        symbol,
        _side_for_live_position(float(live_positions[long_venue.value])),
        live_quantity=float(live_positions[long_venue.value]),
    )
    short_adapter = _HistoricalFlatAdapter(
        short_venue,
        symbol,
        _side_for_live_position(float(live_positions[short_venue.value])),
        live_quantity=float(live_positions[short_venue.value]),
    )
    executor = PassiveCloseExecutor(
        {long_venue: long_adapter, short_venue: short_adapter},
        journal,
    )
    state = EngineState()
    state.recovery_blocked_reason = case["recovery_blocked_reason"]
    state.recovery_blocked_at_ms = 1779698770000
    position = _open_position(
        position_id=case["position_id"],
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        quantity=float(case["matched_quantity"]),
    )
    pending = _pending_close(
        position,
        include_snapshot=bool(case["pending_position_snapshot_present"]),
        active_leg=_active_leg(case["active_maker_leg"]),
    )
    state.open_positions[position.position_id] = position
    state.pending_passive_closes[position.position_id] = pending

    remaining = await executor.process_pending_passive_closes(
        state,
        now_ms=1779698773650,
    )

    assert remaining == set()
    assert state.open_positions == {}
    assert state.pending_passive_closes == {}
    assert state.recovery_blocked_reason is None
    assert state.recovery_blocked_at_ms == 0
    events = journal.read_all()
    drift = next(
        record["payload"]
        for record in events
        if record["kind"] == "runtime.position_drift_detected"
    )
    assert drift["position_id"] == position.position_id
    assert drift["symbol"] == symbol
    assert drift["expected_size"] == float(case["matched_quantity"])
    assert drift["actual_long_size"] == 0.0
    assert drift["actual_short_size"] == 0.0
    assert drift["source"] == "pending_passive_close_flat_probe"
    assert all(
        record["kind"] != "order.submit_attempt"
        for record in events
    )


@pytest.mark.asyncio
async def test_20260525_ubusdt_terminal_maker_under_min_uses_live_flat_probe(
    tmp_path,
):
    """UBUSDT incident: under-min hedge branch probes flat and avoids HTTP submit."""

    fixture = _load_incident_fixture("2026-05-25", "passive_close_recovered_flat.json")
    case = next(
        item for item in fixture["incidents"]
        if item["case_id"] == "ubusdt_terminal_maker_filled_bybit_under_min"
    )
    long_venue = _venue(case["long_venue"])
    short_venue = _venue(case["short_venue"])
    symbol = case["symbol"]
    live_positions = case["live_positions"]
    journal = _journal(tmp_path, "ubusdt-events.jsonl")
    maker_progress = PassiveOrderProgress(
        venue=long_venue,
        symbol=symbol,
        side=Side.SELL,
        order_id=case["maker_order_id"],
        client_order_id=case["maker_client_order_id"],
        cumulative_quantity=float(case["maker_cumulative_quantity"]),
        average_price=float(case["maker_average_price"]),
        state=PassiveOrderState.FILLED,
        observed_at_ms=1779700000000,
    )
    long_adapter = _HistoricalFlatAdapter(
        long_venue,
        symbol,
        _side_for_live_position(float(live_positions[long_venue.value])),
        live_quantity=float(live_positions[long_venue.value]),
        progress=maker_progress,
    )
    short_adapter = _HistoricalFlatAdapter(
        short_venue,
        symbol,
        _side_for_live_position(float(live_positions[short_venue.value])),
        live_quantity=float(live_positions[short_venue.value]),
        normalized_quantity=float(case["hedge_normalized_quantity"]),
    )
    executor = PassiveCloseExecutor(
        {long_venue: long_adapter, short_venue: short_adapter},
        journal,
    )
    executor.set_l2_mid_resolver(lambda venue, symbol: 0.01)
    state = EngineState()
    position = _open_position(
        position_id=f"fixture:{case['case_id']}",
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        quantity=float(case["matched_quantity"]),
    )
    pending = _pending_close(
        position,
        include_snapshot=bool(case["pending_position_snapshot_present"]),
        active_leg=_active_leg(case["active_maker_leg"]),
    )
    pending.phase_state.maker_order_id = case["maker_order_id"]
    pending.phase_state.maker_client_order_id = case["maker_client_order_id"]
    pending.phase_state.maker_resting_limit_price = float(case["maker_average_price"])
    state.open_positions[position.position_id] = position
    state.pending_passive_closes[position.position_id] = pending

    result = await executor.drive_pending_passive_close(
        state,
        position.position_id,
        wait_until_terminal=False,
    )

    assert result is True
    assert state.open_positions == {}
    assert state.pending_passive_closes == {}
    assert long_adapter.query_passive_order_progress_calls == [
        (symbol, case["maker_order_id"], case["maker_client_order_id"])
    ]
    assert short_adapter.place_order_calls == []
    events = journal.read_all()
    kinds = [record["kind"] for record in events]
    for expected in case["expected_events"]:
        assert expected in kinds
    assert all(record["kind"] != "order.submit_attempt" for record in events)
