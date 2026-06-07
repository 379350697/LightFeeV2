from __future__ import annotations

import pytest

from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.recovery_decision_core import (
    RecoveryDecision,
    RecoveryDecisionKind,
    RecoveryEvidenceClass,
)
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.risk.modes import EngineLifecycle
from tests.test_live_entry_hedge_root_fix import _FakeVenueAdapter, _make_open_runtime


class IncidentVenueAdapter(_FakeVenueAdapter):
    def __init__(self, venue: Venue):
        super().__init__(venue)
        self.open_orders: list[dict] = []
        self.fetch_position_raises: Exception | None = None
        self.fetch_open_orders_raises: Exception | None = None
        self._fetch_open_orders_calls: list[str] = []

    async def fetch_position(self, symbol: str) -> PositionSnapshot | None:
        if self.fetch_position_raises is not None:
            raise self.fetch_position_raises
        return await super().fetch_position(symbol)

    async def fetch_open_orders(self, symbol: str) -> list[dict]:
        self._fetch_open_orders_calls.append(symbol)
        if self.fetch_open_orders_raises is not None:
            raise self.fetch_open_orders_raises
        return list(self.open_orders)


class TransportOnlyOpenOrdersAdapter(_FakeVenueAdapter):
    def __init__(self, venue: Venue, transport):
        super().__init__(venue)
        self._transport = transport
        self.fetch_open_orders = None


class RecordingOpenOrdersTransport:
    def __init__(self, venue: Venue, response: dict | list):
        self.venue = venue
        self.response = response
        self.calls: list[tuple[str, str, dict | None, bool]] = []

    def _venue_symbol(self, symbol: str) -> str:
        if self.venue == Venue.OKX:
            return symbol.replace("USDT", "-USDT-SWAP")
        return symbol

    async def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        body: dict | None = None,
        private: bool = False,
    ):
        self.calls.append((method, path, params, private))
        return self.response


def _flat_position(venue: Venue, symbol: str, now_ms: int) -> PositionSnapshot:
    return PositionSnapshot(
        venue=venue,
        symbol=symbol,
        side=Side.BUY,
        quantity=0.0,
        entry_price=0.0,
        observed_at_ms=now_ms,
    )


@pytest.mark.asyncio
async def test_exhausted_residual_repair_already_flat_clears_lyn_opg(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1779803978233
    runtime.state.live_recovery_reduce_only_pairs.extend([
        {"pair_id": "lynusdt:aster->bybit", "symbol": "LYNUSDT"},
        {"pair_id": "opgusdt:binance->okx", "symbol": "OPGUSDT"},
    ])
    runtime.state.pending_residual_repairs.extend([
        {
            "position_id": "entry-1779569524920-LYNUSDT",
            "pair_id": "lynusdt:aster->bybit",
            "symbol": "LYNUSDT",
            "origin": "close_residual",
            "repair_venue": "aster",
            "repair_side": "sell",
            "repair_quantity": 532.0,
            "local_entry_paused": True,
            "last_error": "residual_repair_deadline_or_attempts_exhausted",
            "deadline_ms": now_ms - 1,
            "retry_count": 3,
            "next_attempt_ms": 0,
        },
        {
            "position_id": "entry-1779594732734-OPGUSDT",
            "pair_id": "opgusdt:binance->okx",
            "symbol": "OPGUSDT",
            "origin": "entry_open",
            "repair_venue": "okx",
            "repair_side": "buy",
            "repair_quantity": 9.0,
            "local_entry_paused": True,
            "last_error": "residual_repair_deadline_or_attempts_exhausted",
            "deadline_ms": now_ms - 1,
            "retry_count": 3,
            "next_attempt_ms": 0,
        },
    ])

    adapters = {
        Venue.ASTER: IncidentVenueAdapter(Venue.ASTER),
        Venue.BYBIT: IncidentVenueAdapter(Venue.BYBIT),
        Venue.BINANCE: IncidentVenueAdapter(Venue.BINANCE),
        Venue.OKX: IncidentVenueAdapter(Venue.OKX),
    }
    for venue, adapter in adapters.items():
        symbol = "LYNUSDT" if venue in {Venue.ASTER, Venue.BYBIT} else "OPGUSDT"
        adapter.position = _flat_position(venue, symbol, now_ms)
    runtime._venue_adapters = adapters

    await runtime._recover_residual_repairs(now_ms)

    assert runtime.state.pending_residual_repairs == []
    assert runtime.state.live_recovery_reduce_only_pairs == []
    events = runtime.journal.read_all()
    completed = [
        event for event in events
        if event["kind"] == "execution.residual_repair_completed"
    ]
    assert {event["payload"]["symbol"] for event in completed} == {"LYNUSDT", "OPGUSDT"}
    assert all(event["payload"]["result"] == "already_flat" for event in completed)
    assert all(adapter._fetch_open_orders_calls for adapter in adapters.values())


@pytest.mark.asyncio
async def test_residual_repair_completed_records_live_truth_evidence(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1779803978233
    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1779594732734-OPGUSDT",
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "buy",
        "repair_quantity": 9.0,
        "local_entry_paused": True,
        "last_error": "residual_repair_deadline_or_attempts_exhausted",
        "deadline_ms": now_ms - 1,
        "retry_count": 3,
        "next_attempt_ms": 0,
    })

    binance = IncidentVenueAdapter(Venue.BINANCE)
    binance.position = _flat_position(Venue.BINANCE, "OPGUSDT", now_ms)
    okx = IncidentVenueAdapter(Venue.OKX)
    okx.position = _flat_position(Venue.OKX, "OPGUSDT", now_ms)
    runtime._venue_adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

    await runtime._recover_residual_repairs(now_ms)

    events = runtime.journal.read_all()
    completed = [
        event["payload"] for event in events
        if event["kind"] == "execution.residual_repair_completed"
    ][-1]
    assert completed["result"] == "already_flat"
    assert completed["open_order_count"] == 0
    assert completed["open_order_counts_by_venue"] == {"okx": 0, "binance": 0}
    assert completed["live_truth_venues"] == ["okx", "binance"]
    assert completed["live_positions"]["okx"]["quantity"] == pytest.approx(0.0)
    assert completed["live_positions"]["binance"]["quantity"] == pytest.approx(0.0)
    assert completed["baseline_quantity"] == pytest.approx(0.0)
    assert completed["live_excess_quantity"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_residual_repair_ack_only_submit_preserves_order_truth_gap_evidence(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1779803978233
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-residual-ack-only",
        "pair_id": "edenusdt:binance->bybit",
        "symbol": "EDENUSDT",
        "origin": "entry_open",
        "repair_venue": "bybit",
        "repair_side": "buy",
        "repair_quantity": 10.0,
        "created_at_ms": now_ms,
        "deadline_ms": now_ms + 30_000,
        "retry_count": 0,
        "last_attempt_at_ms": 0,
    })

    bybit = IncidentVenueAdapter(Venue.BYBIT)
    bybit.position = PositionSnapshot(
        venue=Venue.BYBIT,
        symbol="EDENUSDT",
        side=Side.SELL,
        quantity=10.0,
        entry_price=1.0,
        observed_at_ms=now_ms,
    )
    ack_error = OrderSubmitError(
        SubmitFailureClass.UNCERTAIN,
        "order accepted (id=repair-ack-oid) but fill not confirmed",
    )
    ack_error.order_ack_only = True
    ack_error.accepted_order_id = "repair-ack-oid"
    ack_error.accepted_client_order_id = "repair-ack-cid"
    ack_error.fill_confirmation_missing_fields = ["executedQty", "cumQty"]
    ack_error.exchange_response_body = (
        '{"retCode":0,"result":{"orderId":"repair-ack-oid","orderLinkId":"repair-ack-cid"}}'
    )
    bybit.place_order_raises = ack_error
    runtime._venue_adapters = {Venue.BYBIT: bybit}

    await runtime._recover_residual_repairs(now_ms)

    assert runtime.state.pending_residual_repairs
    failed = [
        event["payload"] for event in runtime.journal.read_all()
        if event["kind"] == "recovery.residual_repair_failed"
    ][-1]
    assert failed["order_ack_only"] is True
    assert failed["accepted_order_id"] == "repair-ack-oid"
    assert failed["accepted_client_order_id"] == "repair-ack-cid"
    assert failed["fill_confirmation_missing_fields"] == ["executedQty", "cumQty"]
    assert "fill_confirmation" in failed["missing_evidence"]
    assert failed["order_truth_probe_paths"]["rest_order_status"] == "GET /v5/order/realtime"
    assert failed["next_action"] == "reconcile_accepted_order_or_probe_live_position"


@pytest.mark.asyncio
async def test_residual_repair_completion_refreshes_stale_core_block(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1779803978233
    runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
    runtime.state.recovery_blocked_reason = "truth_unavailable_for_required_recovery"
    runtime.state.recovery_blocked_at_ms = now_ms - 1000
    runtime.recovery_decision = RecoveryDecision(
        kind=RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH,
        evidence_class=RecoveryEvidenceClass.TRUTH_UNAVAILABLE_FOR_REQUIRED_RECOVERY,
        entry_allowed=False,
        block_reason="truth_unavailable_for_required_recovery",
    )
    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1779594732734-OPGUSDT",
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "buy",
        "repair_quantity": 9.0,
        "local_entry_paused": True,
        "last_error": "residual_repair_deadline_or_attempts_exhausted",
        "deadline_ms": now_ms - 1,
        "retry_count": 3,
        "next_attempt_ms": 0,
    })

    binance = IncidentVenueAdapter(Venue.BINANCE)
    binance.position = _flat_position(Venue.BINANCE, "OPGUSDT", now_ms)
    okx = IncidentVenueAdapter(Venue.OKX)
    okx.position = _flat_position(Venue.OKX, "OPGUSDT", now_ms)
    runtime._venue_adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

    await runtime._recover_residual_repairs(now_ms)

    assert runtime.state.pending_residual_repairs == []
    assert runtime.state.recovery_blocked_reason is None
    assert runtime.state.recovery_blocked_at_ms == 0
    assert runtime.state.lifecycle == EngineLifecycle.RUNNING
    assert runtime.recovery_decision.entry_allowed is True
    assert runtime._gate_recovery_ledger(object()) == (True, "")


@pytest.mark.asyncio
async def test_residual_repair_completion_core_allow_ignores_stale_ledger_veto(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1779803978233
    runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
    runtime.state.recovery_blocked_reason = "truth_unavailable_for_required_recovery"
    runtime.state.recovery_blocked_at_ms = now_ms - 1000
    runtime.recovery_decision = RecoveryDecision(
        kind=RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH,
        evidence_class=RecoveryEvidenceClass.TRUTH_UNAVAILABLE_FOR_REQUIRED_RECOVERY,
        entry_allowed=False,
        block_reason="truth_unavailable_for_required_recovery",
    )
    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1779594732734-OPGUSDT",
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "buy",
        "repair_quantity": 9.0,
        "local_entry_paused": True,
        "last_error": "residual_repair_deadline_or_attempts_exhausted",
        "deadline_ms": now_ms - 1,
        "retry_count": 3,
        "next_attempt_ms": 0,
    })
    runtime.recovery_ledger = RecoveryLedger.from_local_and_exchange_truth(
        local=runtime.state,
        exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
    )

    binance = IncidentVenueAdapter(Venue.BINANCE)
    binance.position = _flat_position(Venue.BINANCE, "OPGUSDT", now_ms)
    okx = IncidentVenueAdapter(Venue.OKX)
    okx.position = _flat_position(Venue.OKX, "OPGUSDT", now_ms)
    runtime._venue_adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

    await runtime._recover_residual_repairs(now_ms)

    assert runtime.state.pending_residual_repairs == []
    assert runtime.recovery_decision.entry_allowed is True
    assert runtime._gate_recovery_ledger(
        type("Candidate", (), {
            "symbol": "OPGUSDT",
            "long_venue": "binance",
            "short_venue": "okx",
        })()
    ) == (True, "")


@pytest.mark.asyncio
async def test_exhausted_residual_repair_live_nonzero_repairs_but_untrusted_stays_fail_closed(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1779803978233
    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1779594732734-OPGUSDT",
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "buy",
        "repair_quantity": 9.0,
        "local_entry_paused": True,
        "last_error": "residual_repair_deadline_or_attempts_exhausted",
        "deadline_ms": now_ms - 1,
        "retry_count": 3,
        "next_attempt_ms": 0,
    })

    binance = IncidentVenueAdapter(Venue.BINANCE)
    binance.position = _flat_position(Venue.BINANCE, "OPGUSDT", now_ms)
    okx = IncidentVenueAdapter(Venue.OKX)
    okx.position = PositionSnapshot(
        venue=Venue.OKX,
        symbol="OPGUSDT",
        side=Side.SELL,
        quantity=9.0,
        entry_price=0.2,
        observed_at_ms=now_ms,
    )
    okx.place_order_fill = OrderFill(
        venue=Venue.OKX,
        symbol="OPGUSDT",
        side=Side.BUY,
        quantity=9.0,
        price=0.2,
        order_id="opg-residual-repair",
        filled_at_ms=now_ms,
    )
    runtime._venue_adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

    await runtime._recover_residual_repairs(now_ms)

    assert runtime.state.pending_residual_repairs == []
    assert runtime.state.live_recovery_reduce_only_pairs == []
    events = runtime.journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "execution.residual_repair_resumed" in kinds
    assert "execution.residual_repair_completed" in kinds
    assert "execution.residual_repair_paused" not in kinds
    assert len(okx._place_order_calls) == 1
    assert okx._place_order_calls[0].side == Side.BUY
    completed_before_untrusted = len([
        event for event in events
        if event["kind"] == "execution.residual_repair_completed"
    ])

    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1779594732734-OPGUSDT",
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "buy",
        "repair_quantity": 9.0,
        "local_entry_paused": True,
        "last_error": "residual_repair_deadline_or_attempts_exhausted",
        "deadline_ms": now_ms - 1,
        "retry_count": 3,
        "next_attempt_ms": 0,
    })
    okx.position = _flat_position(Venue.OKX, "OPGUSDT", now_ms)
    okx.fetch_open_orders_raises = RuntimeError("open order truth unavailable")

    await runtime._recover_residual_repairs(now_ms + 1)

    assert len(runtime.state.pending_residual_repairs) == 1
    assert runtime.state.pending_residual_repairs[0]["last_error"].startswith(
        "residual_repair_live_truth_untrusted:"
    )
    paused_after_untrusted = [
        event for event in runtime.journal.read_all()
        if event["kind"] == "execution.residual_repair_paused"
    ]
    assert paused_after_untrusted[-1]["payload"]["last_error"].startswith(
        "residual_repair_live_truth_untrusted:"
    )
    completed_after_untrusted = [
        event for event in runtime.journal.read_all()
        if event["kind"] == "execution.residual_repair_completed"
    ]
    assert len(completed_after_untrusted) == completed_before_untrusted


@pytest.mark.asyncio
async def test_exhausted_residual_repair_uses_transport_open_order_truth(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1779803978233
    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1779594732734-OPGUSDT",
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "buy",
        "repair_quantity": 9.0,
        "local_entry_paused": True,
        "last_error": "residual_repair_deadline_or_attempts_exhausted",
        "deadline_ms": now_ms - 1,
        "retry_count": 3,
        "next_attempt_ms": 0,
    })

    binance_transport = RecordingOpenOrdersTransport(Venue.BINANCE, [])
    okx_transport = RecordingOpenOrdersTransport(Venue.OKX, {"data": []})
    binance = TransportOnlyOpenOrdersAdapter(Venue.BINANCE, binance_transport)
    okx = TransportOnlyOpenOrdersAdapter(Venue.OKX, okx_transport)
    binance.position = _flat_position(Venue.BINANCE, "OPGUSDT", now_ms)
    okx.position = _flat_position(Venue.OKX, "OPGUSDT", now_ms)
    runtime._venue_adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

    await runtime._recover_residual_repairs(now_ms)

    assert runtime.state.pending_residual_repairs == []
    assert runtime.state.live_recovery_reduce_only_pairs == []
    assert binance_transport.calls == [
        ("GET", "/fapi/v1/openOrders", {"symbol": "OPGUSDT"}, True)
    ]
    assert okx_transport.calls == [
        ("GET", "/api/v5/trade/orders-pending", {"instId": "OPG-USDT-SWAP"}, True)
    ]


@pytest.mark.asyncio
async def test_exhausted_residual_repair_transport_open_order_truth_failure_keeps_task(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1779803978233
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1779594732734-OPGUSDT",
        "pair_id": "opgusdt:binance->okx",
        "symbol": "OPGUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "buy",
        "repair_quantity": 9.0,
        "local_entry_paused": True,
        "last_error": "residual_repair_deadline_or_attempts_exhausted",
        "deadline_ms": now_ms - 1,
        "retry_count": 3,
        "next_attempt_ms": 0,
    })

    class FailingTransport(RecordingOpenOrdersTransport):
        async def _request(self, *args, **kwargs):
            await super()._request(*args, **kwargs)
            raise RuntimeError("open orders unavailable")

    okx_transport = FailingTransport(Venue.OKX, {})
    okx = TransportOnlyOpenOrdersAdapter(Venue.OKX, okx_transport)
    okx.position = _flat_position(Venue.OKX, "OPGUSDT", now_ms)
    runtime._venue_adapters = {Venue.OKX: okx}

    await runtime._recover_residual_repairs(now_ms)

    assert len(runtime.state.pending_residual_repairs) == 1
    task = runtime.state.pending_residual_repairs[0]
    assert task["last_error"].startswith("residual_repair_live_truth_untrusted:")
    assert "execution.residual_repair_completed" not in [
        event["kind"] for event in runtime.journal.read_all()
    ]


@pytest.mark.asyncio
async def test_hmstr_open_orders_present_pause_records_truth_evidence(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1780084367773
    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "hmstrusdt:bybit->okx",
        "symbol": "HMSTRUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1780084367773-HMSTRUSDT",
        "pair_id": "hmstrusdt:bybit->okx",
        "symbol": "HMSTRUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "buy",
        "repair_quantity": 142090.0,
        "local_entry_paused": True,
        "last_error": "residual_repair_live_open_orders_present",
        "deadline_ms": now_ms - 1,
        "retry_count": 3,
        "next_attempt_ms": 0,
    })

    bybit = IncidentVenueAdapter(Venue.BYBIT)
    bybit.position = _flat_position(Venue.BYBIT, "HMSTRUSDT", now_ms)
    bybit.open_orders = [{"orderId": "hmstr-live-open-order"}]
    okx = IncidentVenueAdapter(Venue.OKX)
    okx.position = _flat_position(Venue.OKX, "HMSTRUSDT", now_ms)
    runtime._venue_adapters = {Venue.BYBIT: bybit, Venue.OKX: okx}

    await runtime._recover_residual_repairs(now_ms)

    assert len(runtime.state.pending_residual_repairs) == 1
    paused = [
        event["payload"]
        for event in runtime.journal.read_all()
        if event["kind"] == "execution.residual_repair_paused"
    ]
    assert paused
    assert paused[-1]["last_error"] == "residual_repair_live_open_orders_present"
    assert paused[-1]["open_order_count"] == 1
    assert paused[-1]["open_order_counts_by_venue"] == {"okx": 0, "bybit": 1}
    assert paused[-1]["live_truth_venues"] == ["okx", "bybit"]
    assert paused[-1]["live_excess_quantity"] == pytest.approx(0.0)
