"""Production HTTP paths for the SAGA/ONE signed-contract incident (offline)."""
import json
import asyncio

import httpx
import pytest

from lightfee.core.domain import OrderRequest, PassiveOrderState, Side, TimeInForce
from lightfee.core.domain import Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.gate import GateAdapter
from lightfee.venues.symbol_rules import get_symbol_rules_cache
from lightfee.venues.transport import LiveCredential, TransportError
from lightfee.marketdata.private_ws import PrivateWsState
from lightfee.venues.gate_private_ws import handle_gate_private_message


def contract(symbol="ONE_USDT", multiplier="10", tick="0.000001", **extra):
    return {"name": symbol, "quanto_multiplier": multiplier,
            "order_price_round": tick, "order_size_min": "1",
            "order_size_max": "1000000", "enable_decimal": False, **extra}


@pytest.fixture(autouse=True)
def isolated_rules():
    get_symbol_rules_cache().clear()
    yield
    get_symbol_rules_cache().clear()


@pytest.fixture
async def gate_wire():
    state = {"metadata": contract(), "posts": [], "positions": [],
             "order": None, "response_side": None}

    def handler(request):
        path = request.url.path
        if "/contracts" in path:
            return httpx.Response(200, json=state["metadata"])
        if path.endswith("/positions"):
            return httpx.Response(200, json=state["positions"])
        if path.endswith("/my_trades"):
            row = state["order"]
            return httpx.Response(200, json=[{
                "order_id": row["id"], "size": row["size"],
                "price": row["fill_price"], "fee": "0.01",
                "contract": row["contract"], "create_time": row.get("finish_time", "1790207523"),
            }])
        if request.method == "POST":
            body = json.loads(request.content)
            state["posts"].append(body)
            size = float(body["size"])
            if state["response_side"] is not None:
                size = abs(size) * state["response_side"]
            row = {"id": "160159262129906499", "contract": body["contract"],
                   "size": str(size), "left": "0", "status": "finished",
                   "finish_as": "filled", "price": body["price"],
                   "fill_price": "0.002345", "text": body.get("text", ""),
                   "finish_time": "1790207523"}
            state["order"] = state["order"] or row
            return httpx.Response(200, json=state["order"])
        return httpx.Response(200, json=state["order"])

    adapter = GateAdapter(mode="live", credential=LiveCredential(api_key="test", api_secret="test"))
    adapter._transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        yield adapter, state
    finally:
        await adapter._transport.close()


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
@pytest.mark.parametrize("reduce_only", [False, True])
@pytest.mark.parametrize("symbol,multiplier,tick,quantity,price,contracts", [
    ("SAGAUSDT", "1", "0.00001", 501, .04814, 501),
    ("ONEUSDT", "10", "0.000001", 11400, .002345, 1140),
    ("BTCUSDT", "0.0001", "0.1", .001, 60000, 10),
])
async def test_gate_passive_wire_contract(gate_wire, side, reduce_only, symbol,
                                         multiplier, tick, quantity, price, contracts):
    adapter, state = gate_wire
    venue_symbol = adapter._transport._venue_symbol(symbol)
    state["metadata"] = contract(venue_symbol, multiplier, tick)
    request = OrderRequest(venue=Venue.GATE, symbol=symbol, side=side,
                           quantity=quantity, price=price, reduce_only=reduce_only,
                           post_only=True, client_order_id="audit-gate-01")
    ack = await adapter.submit_passive_order(request)
    body = state["posts"][0]
    assert float(body["size"]) == pytest.approx(contracts * (1 if side == Side.BUY else -1))
    assert float(body["price"]) == pytest.approx(price)
    assert body["tif"] == "poc"
    assert "post_only" not in body
    assert bool(body.get("reduce_only")) == reduce_only
    assert body["text"] == "t-audit-gate-01"
    assert ack.side == side
    assert ack.quantity == pytest.approx(quantity)


@pytest.mark.parametrize("left,finish,expected_qty,expected_state", [
    (left, reason, quantity, PassiveOrderState.CANCELED)
    for reason in ("cancelled", "liquidated", "ioc", "auto_deleveraged",
                   "reduce_only", "position_closed", "reduce_out", "stp")
    for left, quantity in ((11, 0), (3, 80))
] + [
    (11, "unknown_reason", 0, PassiveOrderState.UNKNOWN),
    (3, "unknown_reason", 80, PassiveOrderState.UNKNOWN),
    (3, "", 80, PassiveOrderState.PARTIALLY_FILLED),
    (0, "filled", 110, PassiveOrderState.FILLED),
])
async def test_gate_private_wire_uses_contract_execution(left, finish, expected_qty, expected_state):
    private = PrivateWsState()
    raw = json.dumps({"channel": "futures.orders", "event": "update", "result": [{
        "contract": "ONE_USDT", "id": "gate-ws", "text": "t-audit-ws",
        "size": "-11", "left": str(left), "status": "finished" if finish else "open",
        "finish_as": finish, "fill_total": "99999", "fill_price": ".002345",
    }]})
    handle_gate_private_message(private, {"ONE_USDT": "ONEUSDT"}, raw, {"ONE_USDT": 10})
    await asyncio.sleep(0)
    update = private.order_by_order_id("gate-ws")
    assert update.filled_quantity == expected_qty
    assert update.state == expected_state
    assert update.client_order_id == "audit-ws"


async def test_gate_private_position_base_units():
    private = PrivateWsState()
    raw = json.dumps({"channel": "futures.positions", "result": [{
        "contract": "ONE_USDT", "size": "-11", "mode": "single",
    }]})
    handle_gate_private_message(private, {"ONE_USDT": "ONEUSDT"}, raw, {"ONE_USDT": 10})
    await asyncio.sleep(0)
    assert private.position("ONEUSDT").size == -110


@pytest.mark.parametrize("price", ["NaN", "Infinity", "-Infinity", "invalid", "-1"])
@pytest.mark.parametrize("path", ["submit", "rest", "ws"])
async def test_gate_invalid_fill_price_cannot_be_confirmed(gate_wire, price, path):
    adapter, wire = gate_wire
    row = {"id": "price-evidence", "contract": "ONE_USDT", "text": "t-price-evidence",
           "size": "-11", "left": "0", "status": "finished", "finish_as": "filled",
           "fill_price": price}
    wire["order"] = row
    if path == "submit":
        with pytest.raises(OrderSubmitError) as caught:
            await adapter.place_order(OrderRequest(venue=Venue.GATE, symbol="ONEUSDT",
                side=Side.SELL, quantity=110, client_order_id="price-evidence"))
        assert caught.value.class_ == SubmitFailureClass.UNCERTAIN
        assert caught.value.accepted_order_id == "price-evidence"
    elif path == "rest":
        assert await adapter.query_passive_order_progress("ONEUSDT", order_id="price-evidence") is None
    else:
        private = PrivateWsState()
        with pytest.raises(ValueError):
            handle_gate_private_message(private, {"ONE_USDT": "ONEUSDT"},
                json.dumps({"channel": "futures.orders", "result": [row]}), {"ONE_USDT": 10})
        await asyncio.sleep(0)
        assert private.order_by_order_id("price-evidence") is None


async def test_gate_decimal_ws_worker_negotiates_and_preserves_size(gate_wire):
    import websockets
    from lightfee.venues.gate_private_ws import _gate_private_ws_loop

    adapter, wire = gate_wire
    wire["metadata"] = contract(order_size_min="0.1", enable_decimal=True)
    subscriptions = []
    headers = {}

    async def server(connection):
        headers.update(connection.request.headers)
        for _ in range(2):
            subscriptions.append(json.loads(await connection.recv()))
        await connection.send(json.dumps({
            "channel": "futures.orders", "event": "update", "result": [{
                "id": "decimal-ws", "contract": "ONE_USDT", "text": "t-decimal",
                "size": "-1.1", "left": "0.1", "status": "open", "fill_price": ".002345",
            }],
        }))
        await connection.wait_closed()

    async with websockets.serve(server, "127.0.0.1", 0) as listener:
        port = listener.sockets[0].getsockname()[1]
        private = PrivateWsState()
        task = asyncio.create_task(_gate_private_ws_loop(
            adapter._transport, "test", "test", f"ws://127.0.0.1:{port}",
            {"ONE_USDT": "ONEUSDT"}, private, 3, 100, 1000,
        ))
        try:
            async with asyncio.timeout(5):
                while private.order_by_order_id("decimal-ws") is None:
                    await asyncio.sleep(.001)
            assert headers["x-gate-size-decimal"] == "1"
            assert {s["channel"] for s in subscriptions} == {"futures.orders", "futures.positions"}
            assert all(s["payload"] == ["ONE_USDT"] for s in subscriptions)
            assert private.order_by_order_id("decimal-ws").filled_quantity == pytest.approx(10)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


async def test_gate_authoritative_zero_minimum_reaches_hedge_and_close(gate_wire, tmp_path):
    from lightfee.config.schema import AppConfig
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.engine.state import PendingEntry
    from lightfee.engine.passive_close import PassiveCloseExecutor
    from lightfee.persistence.journal import Journal
    adapter, _ = gate_wire
    await adapter.normalize_quantity("ONEUSDT", 110)
    runtime = LiveRuntime(AppConfig(), venue_adapters={Venue.GATE: adapter})
    pending = PendingEntry(pending_id="one-minimum", symbol="ONEUSDT", long_venue=Venue.BINANCE,
                           short_venue=Venue.GATE, target_quantity=110, long_side=Side.BUY,
                           short_side=Side.SELL, created_at_ms=1000, maker_leg="long")
    plan = runtime._pending_entry_hedgeability_plan(pending, Venue.GATE, 110, .002345)
    quantity, violation, _ = await runtime._normalize_pending_entry_hedge_quantity(
        pending=pending, hedge_venue=Venue.GATE, adapter=adapter, missing=110,
        hedge_price=.002345, hedgeability_plan=plan)
    assert quantity == 110
    assert violation is None
    assert plan.diagnostics["exchange_min_notional_quote"] == 0
    journal = Journal(str(tmp_path / "close-minimum.jsonl"))
    journal.open()
    try:
        close = PassiveCloseExecutor({Venue.GATE: adapter}, journal)
        minimum, source = await close._resolve_hedge_min_notional_quote(Venue.GATE, "ONEUSDT")
        assert minimum == 0
        assert source == "gate_contract"
    finally:
        journal.close()


async def test_gate_passive_close_delta_submits_legal_small_reduce_order(gate_wire, tmp_path):
    from lightfee.engine.state import (
        ActiveMakerLeg, EngineState, OpenPosition, PendingPassiveClose,
        PendingPassiveLegFill, PassivePhaseState, PassiveExecutionPhase,
    )
    from lightfee.engine.passive_close import PassiveCloseExecutor
    from lightfee.persistence.journal import Journal
    adapter, wire = gate_wire
    position = OpenPosition(position_id="close-one", symbol="ONEUSDT", long_venue=Venue.BINANCE,
                            short_venue=Venue.GATE, long_quantity=110, short_quantity=110,
                            matched_quantity=110, long_entry_price=.002345,
                            short_entry_price=.002345, opened_at_ms=1000)
    pending = PendingPassiveClose(position_id=position.position_id, reason="funding_capture",
        position_snapshot=position, target_quantity=110, chunk_quantities=[110],
        phase_state=PassivePhaseState(phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                                      active_maker_leg=ActiveMakerLeg.LONG),
        maker_fill=PendingPassiveLegFill(quantity=110, average_price=.002345),
        hedge_fill=PendingPassiveLegFill(quantity=0))
    journal = Journal(str(tmp_path / "small-close.jsonl"))
    journal.open()
    try:
        close = PassiveCloseExecutor({Venue.GATE: adapter}, journal)
        close.set_l2_mid_resolver(lambda venue, symbol: .002345)
        result = await close._submit_hedge_for_delta(EngineState(), pending, position, 110, maker_terminal=True)
        assert result.success
        assert result.filled == 110
        assert len(wire["posts"]) == 1
        assert wire["posts"][0]["reduce_only"]
        assert float(wire["posts"][0]["size"]) == 11
        assert wire["posts"][0]["tif"] == "ioc"
    finally:
        journal.close()


@pytest.mark.parametrize("live", [True, False])
def test_running_health_respects_critical_recovery_decision(live):
    from lightfee.ops.production_health import analyze_current_state
    state = {"lifecycle": "running", "risk_mode": "running", "last_tick_ms": 1000,
             "last_scan": {"ts_ms": 1000}, "pending_entry_count": 1 if live else 0,
             "exchange_truth": {"available": True, "confidence": "high",
                "has_nonzero_position": live, "has_open_order": False,
                "positions": {"gate": {"SAGAUSDT": {"venue": "gate", "symbol": "SAGAUSDT",
                                                       "side": "buy", "quantity": 501}}} if live else {},
                "open_orders": {}}}
    report = analyze_current_state(state, now_ms=1001, max_tick_age_ms=10000)
    if live:
        assert report.details["recovery_decision"]["diagnostic_severity"] == "critical"
        assert report.severity == "critical"
        assert "recovery_decision_critical" in report.fingerprints
    else:
        assert report.ok


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
async def test_gate_taker_fill_status_and_position_share_base_units(gate_wire, side):
    adapter, state = gate_wire
    sign = 1 if side == Side.BUY else -1
    request = OrderRequest(venue=Venue.GATE, symbol="ONEUSDT", side=side,
                           quantity=110, reduce_only=True, time_in_force=TimeInForce.IOC)
    fill = await adapter.place_order(request)
    assert float(state["posts"][0]["size"]) == 11 * sign
    assert fill.quantity == pytest.approx(110)
    assert fill.price == pytest.approx(.002345)
    rec = await adapter.fetch_order_fill_reconciliation("ONEUSDT", fill.order_id, "")
    assert rec.quantity == pytest.approx(110)
    assert rec.side == side
    progress = await adapter.query_passive_order_progress("ONEUSDT", fill.order_id, side=side)
    assert progress.cumulative_quantity == pytest.approx(110)
    assert progress.side == side
    state["positions"] = [{"contract": "ONE_USDT", "size": str(11 * sign),
                           "entry_price": ".002345"}]
    pos = await adapter.fetch_position("ONEUSDT")
    assert pos.quantity == pytest.approx(110)
    assert pos.side == side
    all_positions = await adapter.fetch_all_positions()
    assert all_positions[0].quantity == pytest.approx(110)
    assert await adapter.normalize_quantity("ONEUSDT", 119) == pytest.approx(110)


@pytest.mark.parametrize("left,finish_as,expected_qty,expected_state", [
    (left, reason, quantity, PassiveOrderState.CANCELED)
    for reason in ("cancelled", "liquidated", "ioc", "auto_deleveraged",
                   "reduce_only", "position_closed", "reduce_out", "stp")
    for left, quantity in (("11", 0), ("3", 80))
] + [
    ("11", "unknown_reason", 0, PassiveOrderState.UNKNOWN),
    ("3", "unknown_reason", 80, PassiveOrderState.UNKNOWN),
    ("11", "", 0, PassiveOrderState.UNKNOWN),
    ("0", "filled", 110, PassiveOrderState.FILLED),
])
async def test_gate_finished_does_not_invent_fills(gate_wire, left, finish_as,
                                                 expected_qty, expected_state):
    adapter, state = gate_wire
    state["order"] = {"id": "123", "contract": "ONE_USDT", "size": "-11",
                      "left": left, "status": "finished", "finish_as": finish_as,
                      "price": ".002345", "fill_price": ".002345"}
    progress = await adapter.query_passive_order_progress("ONEUSDT", "123", side=Side.SELL)
    assert progress.state == expected_state
    assert progress.cumulative_quantity == pytest.approx(expected_qty)
    assert progress.side == Side.SELL


@pytest.mark.parametrize("metadata", [
    {}, contract(quanto_multiplier="0"), contract(quanto_multiplier="NaN"),
    contract(order_price_round="0"), contract(order_size_min="bad"),
    contract(symbol="OTHER_USDT"), contract(quanto_multiplier="1e999"),
    contract(quanto_multiplier="1e-999"),
])
async def test_gate_bad_metadata_blocks_before_order(gate_wire, metadata):
    adapter, state = gate_wire
    state["metadata"] = metadata
    with pytest.raises((OrderSubmitError, TransportError)):
        await adapter.submit_passive_order(OrderRequest(
            venue=Venue.GATE, symbol="ONEUSDT", side=Side.SELL,
            quantity=110, price=.002345, post_only=True))
    assert state["posts"] == []


async def test_gate_wrong_side_ack_preserves_uncertain_order_identity(gate_wire):
    adapter, state = gate_wire
    state["response_side"] = 1
    with pytest.raises(OrderSubmitError) as caught:
        await adapter.submit_passive_order(OrderRequest(
            venue=Venue.GATE, symbol="ONEUSDT", side=Side.SELL,
            quantity=110, price=.002345, post_only=True))
    assert caught.value.class_ == SubmitFailureClass.UNCERTAIN
    assert caught.value.accepted_order_id == "160159262129906499"


@pytest.mark.parametrize("passive", [False, True])
@pytest.mark.parametrize("invalid_ack", ["price", "side"])
async def test_gate_uncertain_ack_identity_reaches_pending_owner(gate_wire, tmp_path, passive, invalid_ack):
    from lightfee.engine.entry import EntryContext, EntryType
    from lightfee.engine.entry_sync import EntrySyncExecutor
    from lightfee.persistence.journal import Journal

    adapter, state = gate_wire
    state["order"] = {
        "id": "gate-accepted-123", "contract": "ONE_USDT",
        "size": "11" if invalid_ack == "side" else "-11",
        "left": "0", "status": "finished", "finish_as": "filled",
        "price": ".002345", "fill_price": "NaN" if invalid_ack == "price" else ".002345",
    }
    journal = Journal(tmp_path / "uncertain-ack.jsonl")
    journal.open()
    try:
        result = await EntrySyncExecutor(adapters={Venue.GATE: adapter}, journal=journal).execute(
            EntryContext(entry_id="accepted-ack", symbol="ONEUSDT",
                long_venue=Venue.BINANCE, short_venue=Venue.GATE,
                long_quantity=110, short_quantity=110,
                long_price_hint=.002345, short_price_hint=.002345,
                maker_leg=Side.SELL, created_at_ms=1000,
                entry_type=EntryType.PASSIVE_INCREMENTAL if passive else EntryType.STANDARD_DUAL_TAKER))
        assert len(state["posts"]) == 1
        assert result.has_uncertainty
        assert result.open_position is None
        assert result.pending_entry.maker_order_id == "gate-accepted-123"
        assert state["posts"][0]["text"] == "t-" + result.pending_entry.maker_client_order_id
        assert bool(state["posts"][0]["tif"] == "poc") == passive
        uncertain = [r for r in journal.read_all() if r["kind"] == "order.uncertain"]
        assert uncertain[-1]["payload"]["order_id"] == "gate-accepted-123"
    finally:
        journal.close()


async def test_gate_decimal_contract_and_generated_identity(gate_wire):
    from lightfee.venues.cid import generate_exchange_cid
    adapter, state = gate_wire
    state["metadata"] = contract(multiplier="10", order_size_min=".1", enable_decimal=True)
    cid = generate_exchange_cid("incident-long-entry-id", "short", Venue.GATE)
    assert len(cid) <= 28
    ack = await adapter.submit_passive_order(OrderRequest(
        venue=Venue.GATE, symbol="ONEUSDT", side=Side.SELL, quantity=11,
        price=.002345, post_only=True, client_order_id=cid))
    assert float(state["posts"][0]["size"]) == pytest.approx(-1.1)
    assert state["posts"][0]["text"] == "t-" + cid
    rec = await adapter.fetch_order_fill_reconciliation("ONEUSDT", "", cid)
    assert rec.quantity == 11
    assert rec.order_id == ack.order_id
    progress = await adapter.query_passive_order_progress("ONEUSDT", "", cid)
    assert progress.cumulative_quantity == 11


@pytest.mark.parametrize("quantity,price,cid", [
    (float("nan"), .002, ""), (float("inf"), .002, ""), (-1, .002, ""),
    (11, float("nan"), ""), (11, .002, "x" * 29), (11, .002, "bad/cid"),
])
async def test_gate_invalid_order_blocks_before_wire(gate_wire, quantity, price, cid):
    adapter, state = gate_wire
    with pytest.raises(OrderSubmitError):
        await adapter.submit_passive_order(OrderRequest(
            venue=Venue.GATE, symbol="ONEUSDT", side=Side.SELL, quantity=quantity,
            price=price, post_only=True, client_order_id=cid))
    assert not state["posts"]


@pytest.mark.parametrize("positions", [None, {}, [None], [{"size": 0}],
                                       [{"contract": "ONE_USDT", "size": "NaN"}]])
async def test_gate_missing_position_evidence_is_not_flat(gate_wire, positions):
    adapter, state = gate_wire
    state["positions"] = positions
    with pytest.raises((TransportError, ValueError)):
        await adapter.fetch_all_positions()
    with pytest.raises((TransportError, ValueError)):
        await adapter.fetch_position("ONEUSDT")


async def test_gate_history_and_cancel_share_contract_units(gate_wire):
    adapter, state = gate_wire
    state["order"] = {"id": "123", "contract": "ONE_USDT", "size": "-11", "left": "0",
                      "status": "finished", "finish_as": "filled", "fill_price": ".002345",
                      "is_reduce_only": True, "finish_time": "1790207523"}
    result = await adapter.discover_historical_close_fill_reconciliation(
        symbol="ONEUSDT", side=Side.SELL, position_side="long", quantity=110,
        closed_at_ms=1790207523000)
    assert result.classification == "unique_candidate_exact_recheck"
    assert result.reconciliation.quantity == 110
    ack = await adapter.cancel_passive_order("ONEUSDT", "123")
    assert ack.quantity == 110
    assert ack.side == Side.SELL


@pytest.mark.parametrize("valid", [True, False])
async def test_live_entry_metadata_and_both_leg_admission_use_gate_rules(gate_wire, valid, tmp_path):
    from types import SimpleNamespace
    from lightfee.config.schema import AppConfig
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.persistence.journal import Journal
    adapter, state = gate_wire
    if not valid:
        state["metadata"] = {}
    config = AppConfig()
    config.runtime.mode = "live"
    runtime = LiveRuntime(config, venue_adapters={Venue.GATE: adapter})
    journal = Journal(str(tmp_path / "events.jsonl"))
    journal.open()
    runtime.journal = journal
    try:
        step, missing = await runtime._entry_venue_quantity_metadata(Venue.GATE, "ONEUSDT")
        assert step == (10 if valid else None)
        assert bool(missing) is (not valid)
        result = await runtime.entry_dispatch_runtime._precheck_live_entry_admission(
            candidate=SimpleNamespace(symbol="ONEUSDT", long_venue=Venue.BINANCE, short_venue=Venue.GATE),
            now_ms=1000, long_venue=Venue.BINANCE, short_venue=Venue.GATE, quantity=110,
            long_order_price_hint=.002344, short_order_price_hint=.002345,
            maker_venue=Venue.BINANCE, entry_type="passive", maker_client_order_id="maker",
            hedge_client_order_id="hedge")
        assert result is valid
        assert state["posts"] == []
    finally:
        journal.close()


async def test_gate_dual_live_sides_are_not_net_flat(gate_wire):
    adapter, state = gate_wire
    state["positions"] = [
        {"contract": "ONE_USDT", "mode": "dual_long", "size": "11", "entry_price": ".002"},
        {"contract": "ONE_USDT", "mode": "dual_short", "size": "-11", "entry_price": ".003"},
    ]
    rows = await adapter.fetch_all_positions()
    assert len(rows) == 2
    assert sum(p.quantity for p in rows) == pytest.approx(220)
    with pytest.raises(TransportError, match="multiple_live_position_sides"):
        await adapter.fetch_position("ONEUSDT")


@pytest.mark.parametrize("case", ["normal", "opposite", "excess", "local_zero", "duplicate", "split_owners", "missing_long", "missing_short", "flat", "local_one_leg", "local_imbalance"])
@pytest.mark.parametrize("reverse", [False, True])
async def test_account_recovery_requires_complete_directional_ownership(tmp_path, case, reverse):
    from dataclasses import replace
    from lightfee.core.domain import PositionSnapshot
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.engine.state import OpenPosition
    from lightfee.engine.recovery_decision_core import RecoveryEvidenceSnapshot, V1RecoveryDecisionCore
    from tests.test_live_startup_preflight import make_test_config

    rows = [{"contract": "SAGA_USDT", "size": "501", "mode": "dual_long", "entry_price": ".05"}]
    if case == "opposite":
        rows.append({"contract": "SAGA_USDT", "size": "-20", "mode": "dual_short", "entry_price": ".05"})
    elif case == "excess":
        rows[0]["size"] = "521"
    elif case == "duplicate":
        rows.append(dict(rows[0]))
    if case in {"missing_long", "flat", "local_one_leg"}:
        rows.clear()
    if reverse:
        rows.reverse()

    def wire(request):
        assert request.method == "GET"
        if "/contracts" in request.url.path:
            return httpx.Response(200, json=contract("SAGA_USDT", "1", ".00001"))
        if request.url.path.endswith("/positions"):
            return httpx.Response(200, json=rows)
        if request.url.path.endswith("/orders"):
            return httpx.Response(200, json=[])
        raise AssertionError(request.url)

    class Binance:
        venue = Venue.BINANCE
        async def fetch_all_positions(self):
            if case in {"missing_short", "flat"}:
                return []
            return [PositionSnapshot(venue=self.venue, symbol="SAGAUSDT", side=Side.SELL,
                                     quantity=300 if case == "local_imbalance" else 501, entry_price=.05, observed_at_ms=1)]
        async def fetch_open_orders(self, symbol=None):
            return []

    gate = GateAdapter(mode="live", credential=LiveCredential(api_key="test", api_secret="test"))
    gate._transport._client = httpx.AsyncClient(transport=httpx.MockTransport(wire))
    runtime = LiveRuntime(make_test_config(str(tmp_path)), venue_adapters={Venue.GATE: gate, Venue.BINANCE: Binance()})
    runtime.journal.open()
    position = OpenPosition(position_id="owned", symbol="SAGAUSDT", long_venue=Venue.GATE,
                            short_venue=Venue.BINANCE, long_quantity=501, short_quantity=501,
                            long_entry_price=.05, short_entry_price=.05, opened_at_ms=1)
    if case in {"local_zero", "local_one_leg"}:
        position.long_quantity = 0
    if case == "local_imbalance":
        position.short_quantity = 300
    if case == "split_owners":
        position.long_quantity = position.short_quantity = 300
        runtime.state.open_positions["second"] = replace(position, position_id="second", long_quantity=201, short_quantity=201)
    runtime.state.open_positions[position.position_id] = position
    try:
        truth = await runtime.recovery_startup_runtime._collect_recovery_ledger_account_truth(1000)
        assert truth["truth_available"]
        ledger = runtime._refresh_recovery_ledger_from_exchange_truth(truth, now_ms=1000)
        allowed = case in {"normal", "split_owners", "flat"}
        assert runtime.recovery_decision.entry_allowed is allowed
        # No evidence row may vanish when the same owner has multiple legs.
        assert sum(a.kind == "position" for item in ledger.work_items for a in item.artifacts) == len(truth["positions"])
        if case in {"missing_long", "missing_short"}:
            missing = [a for item in ledger.work_items for a in item.artifacts if a.kind == "missing_position"]
            assert len(missing) == 1
            assert missing[0].quantity == 0
            assert missing[0].raw["expected_quantity"] == 501
        direct = V1RecoveryDecisionCore().decide(RecoveryEvidenceSnapshot(
            local_open_positions=tuple(runtime.state.open_positions.values()), exchange_truth=truth))
        assert direct.entry_allowed is allowed
        if case == "opposite":
            # Incomplete evidence cannot release the observed artifact latch.
            gap = dict(truth, truth_available=False, positions=[], errors=["probe failed"])
            runtime._refresh_recovery_ledger_from_exchange_truth(gap, now_ms=1001)
            assert not runtime.recovery_decision.entry_allowed
            rows[:] = [row for row in rows if row["mode"] == "dual_long"]
            clean = await runtime.recovery_startup_runtime._collect_recovery_ledger_account_truth(1002)
            runtime._refresh_recovery_ledger_from_exchange_truth(clean, now_ms=1002)
            assert runtime.recovery_decision.entry_allowed
            assert runtime.recovery_decision.clear_previous_block
    finally:
        runtime.journal.close()
        await gate._transport.close()


@pytest.mark.parametrize("result", [None, {"status": "fail"}])
async def test_gate_worker_rejection_reconnects_without_false_success(result):
    import contextlib
    import websockets
    from lightfee.venues.gate_private_ws import _gate_private_ws_loop

    adapter = GateAdapter(mode="live", credential=LiveCredential(api_key="test", api_secret="test"))
    transport = adapter._transport
    connections = 0
    async def server(connection):
        nonlocal connections
        connections += 1
        await connection.recv()
        await connection.recv()
        await connection.send(json.dumps({"channel": "futures.orders", "event": "subscribe",
            "error": None, "result": {"status": "success"}}))
        await connection.send(json.dumps({"channel": "futures.positions", "event": "subscribe",
            "error": {"code": 2, "message": "invalid argument"}, "result": result}))
        await connection.wait_closed()

    async with websockets.serve(server, "127.0.0.1", 0) as listener:
        task = asyncio.create_task(_gate_private_ws_loop(transport, "test", "test",
            f"ws://127.0.0.1:{listener.sockets[0].getsockname()[1]}", {"ONE_USDT": "ONEUSDT"},
            transport._private_ws_state, 3, 1, 5))
        try:
            for _ in range(100):
                if transport.cached_private_connection_health().is_unhealthy():
                    break
                await asyncio.sleep(.01)
            health = transport.cached_private_connection_health()
            assert health.is_unhealthy()
            assert health.last_success_ms is None
            assert connections >= 3
            assert "futures.positions" in health.last_error
            assert "2" in health.last_error and "invalid argument" in health.last_error
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await transport.close()


@pytest.mark.parametrize("first_attempt", ["none", "one", "duplicate", "unknown", "rejected", "malformed"])
async def test_gate_worker_requires_both_acks_and_recovers_after_reconnect(monkeypatch, first_attempt):
    import contextlib
    import websockets
    from lightfee.venues import gate_private_ws as worker

    monkeypatch.setattr(worker, "GATE_PRIVATE_SUBSCRIBE_TIMEOUT_SECS", .03)
    monkeypatch.setattr(worker, "GATE_PRIVATE_PING_INTERVAL_SECS", .005)
    adapter = GateAdapter(mode="live", credential=LiveCredential(api_key="test", api_secret="test"))
    transport = adapter._transport
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=contract("ONE_USDT", "1", ".00001"))))
    seen = []
    failures_before_recovery = []
    release = asyncio.Event()
    pending_connection = asyncio.Event()
    success_ack = {"event": "subscribe", "error": None, "result": {"status": "success"}}

    async def server(connection):
        await connection.recv()
        await connection.recv()
        seen.append(connection)
        if len(seen) == 1:
            if first_attempt in {"one", "duplicate", "rejected"}:
                await connection.send(json.dumps({**success_ack, "channel": "futures.orders"}))
            if first_attempt == "duplicate":
                await connection.send(json.dumps({**success_ack, "channel": "futures.orders"}))
            if first_attempt == "unknown":
                await connection.send(json.dumps({**success_ack, "channel": "futures.unknown"}))
            if first_attempt == "rejected":
                await connection.send(json.dumps({"event": "subscribe", "channel": "futures.positions",
                                                 "result": {"status": "fail"}}))
            if first_attempt == "malformed":
                await connection.send("[]")
            with contextlib.suppress(websockets.ConnectionClosed):
                while True:
                    ping = json.loads(await connection.recv())
                    assert ping["channel"] == "futures.ping"
                    await connection.send(json.dumps({"channel": "futures.pong", "result": None}))
        else:
            health = transport.cached_private_connection_health()
            failures_before_recovery.append((health.consecutive_failures, health.last_success_ms, health.is_unhealthy()))
            # Even useful data and one success ack do not prove the second subscription.
            await connection.send(json.dumps({"event": "update", "channel": "futures.orders", "result": [{
                "contract": "ONE_USDT", "id": 101, "size": "1", "left": "1", "status": "open",
            }]}))
            await connection.send(json.dumps({**success_ack, "channel": "futures.orders"}))
            pending_connection.set()
            await release.wait()
            await connection.send(json.dumps({**success_ack, "channel": "futures.positions"}))
            await connection.wait_closed()

    async with websockets.serve(server, "127.0.0.1", 0) as listener:
        task = asyncio.create_task(worker._gate_private_ws_loop(transport, "test", "test",
            f"ws://127.0.0.1:{listener.sockets[0].getsockname()[1]}", {"ONE_USDT": "ONEUSDT"},
            transport._private_ws_state, 1, 1, 5))
        try:
            await asyncio.wait_for(pending_connection.wait(), 2)
            assert failures_before_recovery == [(1, None, True)]
            assert transport.cached_private_connection_health().last_success_ms is None
            release.set()
            for _ in range(100):
                if transport.cached_private_connection_health().last_success_ms is not None:
                    break
                await asyncio.sleep(.001)
            health = transport.cached_private_connection_health()
            assert health.last_success_ms is not None and not health.is_unhealthy()
            assert health.consecutive_failures == 0 and health.last_error is None
        finally:
            release.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await transport.close()


@pytest.mark.parametrize("bad_result,failed_channels", [
    (None, ("futures.orders",)), ("bad", ("futures.orders",)), ([1], ("futures.orders",)),
    (None, ("futures.orders", "futures.positions")),
])
async def test_gate_worker_data_failure_survives_ack_reconnect_until_matching_update(bad_result, failed_channels):
    import contextlib
    import websockets
    from lightfee.venues.gate_private_ws import _gate_private_ws_loop

    adapter = GateAdapter(mode="live", credential=LiveCredential(api_key="test", api_secret="test"))
    transport = adapter._transport
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=contract("ONE_USDT", "1", ".00001"))))
    connections = 0
    recovery_waiting = asyncio.Event()
    allow_recovery = asyncio.Event()
    async def server(connection):
        nonlocal connections
        connections += 1
        number = connections
        await connection.recv()
        await connection.recv()
        for channel in ("futures.orders", "futures.positions"):
            await connection.send(json.dumps({"channel": channel, "event": "subscribe", "result": {"status": "success"}}))
        if number <= 3:
            failed = failed_channels[(number - 1) % len(failed_channels)]
            await connection.send(json.dumps({"channel": failed, "event": "update", "result": bad_result}))
        else:
            # One valid stream cannot clear a different stream's failure.
            await connection.send(json.dumps({"channel": "futures.positions", "event": "update", "result": [{
                "contract": "ONE_USDT", "size": "0", "mode": "single",
            }]}))
            await connection.send(json.dumps({"channel": "futures.orders", "event": "update", "result": [{"contract": "UNKNOWN_USDT"}]}))
            await connection.send(json.dumps({"channel": "futures.pong", "result": None}))
            await connection.send(json.dumps({"channel": "futures.orders", "event": "subscribe", "result": {"status": "success"}}))
            recovery_waiting.set()
            await allow_recovery.wait()
            await connection.send(json.dumps({"channel": "futures.orders", "event": "update", "result": [{
                "contract": "ONE_USDT", "id": 101, "size": "1", "left": "1", "status": "open",
            }]}))
        await connection.wait_closed()

    async with websockets.serve(server, "127.0.0.1", 0) as listener:
        task = asyncio.create_task(_gate_private_ws_loop(transport, "test", "test",
            f"ws://127.0.0.1:{listener.sockets[0].getsockname()[1]}", {"ONE_USDT": "ONEUSDT"},
            transport._private_ws_state, 3, 1, 5))
        try:
            await asyncio.wait_for(recovery_waiting.wait(), 2)
            await asyncio.sleep(.01)
            health = transport.cached_private_connection_health()
            assert health.is_unhealthy() and health.consecutive_failures == 3
            allow_recovery.set()
            for _ in range(100):
                if not transport.cached_private_connection_health().is_unhealthy():
                    break
                await asyncio.sleep(.001)
            health = transport.cached_private_connection_health()
            assert not health.is_unhealthy() and health.consecutive_failures == 0
            assert health.last_error is None
        finally:
            allow_recovery.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await transport.close()


@pytest.fixture
async def recovery_wire(tmp_path, monkeypatch):
    from lightfee.core.domain import PositionSnapshot
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.engine.state import OpenPosition
    from tests.test_live_startup_preflight import make_test_config

    wire = {"positions": [{"contract": "SAGA_USDT", "size": "501", "mode": "dual_long", "entry_price": ".05"}],
            "orders": [], "fail": False}
    http_client = httpx.AsyncClient
    async def handler(request):
        if wire.get("slow_metadata") and request.url.path.endswith("/contracts/OTHER_USDT"):
            await asyncio.sleep(3)
        assert request.method == "GET"
        if wire["fail"]:
            return httpx.Response(503, json={"label": "UNAVAILABLE"})
        if "/contracts" in request.url.path:
            return httpx.Response(200, json=contract(request.url.path.rsplit("/", 1)[-1], "1", ".00001"))
        return httpx.Response(200, json=wire["positions" if request.url.path.endswith("/positions") else "orders"])

    # Cancellation retires the client in the real transport. Its replacement
    # must use the same offline wire while exercising that production path.
    monkeypatch.setattr(httpx, "AsyncClient", lambda **options: http_client(
        **{**options, "transport": httpx.MockTransport(handler)}))

    class Binance:
        venue = Venue.BINANCE
        async def fetch_position(self, symbol):
            return PositionSnapshot(venue=self.venue, symbol=symbol, side=Side.SELL,
                                    quantity=501, entry_price=.05, observed_at_ms=1)
        async def fetch_all_positions(self):
            return [await self.fetch_position("SAGAUSDT")]
        async def fetch_open_orders(self, symbol=None):
            return []

    gate = GateAdapter(mode="live", credential=LiveCredential(api_key="test", api_secret="test"))
    gate._transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    runtime = LiveRuntime(make_test_config(str(tmp_path)), venue_adapters={Venue.GATE: gate, Venue.BINANCE: Binance()})
    runtime.state.open_positions["owned"] = OpenPosition(position_id="owned", symbol="SAGAUSDT",
        long_venue=Venue.GATE, short_venue=Venue.BINANCE, long_quantity=501, short_quantity=501,
        long_entry_price=.05, short_entry_price=.05, opened_at_ms=1)
    runtime.journal.open()
    try:
        yield runtime, gate, wire
    finally:
        runtime.journal.close()
        await gate._transport.close()


@pytest.mark.parametrize("size", [501, -501, 0, "missing", None, "NaN", "Infinity", "bad"])
@pytest.mark.parametrize("order_kind", ["maker", "reduce_only", "is_reduce_only", "owned"])
@pytest.mark.parametrize("scope", ["account", "symbols"])
async def test_recovery_open_order_presence_survives_numeric_fields(recovery_wire, size, order_kind, scope):
    from lightfee.engine.recovery_decision_core import RecoveryEvidenceSnapshot, V1RecoveryDecisionCore
    from lightfee.engine.state import PendingEntry
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    runtime, _, wire = recovery_wire
    bad_price = size == 501 and order_kind == "maker"
    wire["orders"] = [{"id": "live-order", "text": "t-live-order", "contract": "SAGA_USDT", "price": "bad" if bad_price else ".05",
        **({} if size == "missing" else {"size": size}),
        **({order_kind: True} if order_kind in {"reduce_only", "is_reduce_only"} else {})}]
    if order_kind == "owned":
        runtime.state.pending_entries["pending"] = PendingEntry(pending_id="pending", symbol="SAGAUSDT",
            long_venue=Venue.GATE, short_venue=Venue.BINANCE, target_quantity=501, long_side=Side.BUY,
            short_side=Side.SELL, created_at_ms=1, maker_order_id="live-order", maker_leg="long")
    collector = runtime.recovery_startup_runtime
    truth = (await collector._collect_recovery_ledger_account_truth(1000) if scope == "account" else
             await collector._collect_recovery_ledger_exchange_truth(["SAGAUSDT"], 1000))
    ledger = runtime._refresh_recovery_ledger_from_exchange_truth(truth, now_ms=1000)
    orders = [a for item in ledger.work_items for a in item.artifacts if a.kind == "open_order"]
    assert len(orders) == 1
    assert orders[0].raw["raw"] == wire["orders"][0]
    assert ("price" in orders[0].raw.get("normalization_errors", [])) is bad_price
    assert not runtime.recovery_decision.entry_allowed
    expected_kind = ("owned_pending_entry" if order_kind == "owned" else
                     "orphan_reduce_only_order" if order_kind != "maker" else "orphan_maker_order")
    assert any(item.kind == expected_kind for item in ledger.work_items)
    direct = V1RecoveryDecisionCore().decide(RecoveryEvidenceSnapshot(
        local_open_positions=tuple(runtime.state.open_positions.values()),
        pending_entries=tuple(runtime.state.pending_entries.values()), exchange_truth=truth))
    assert not direct.entry_allowed
    table = build_v1_lifecycle_closure_table(local_state=runtime.state, exchange_truth=truth)
    assert not table.summary["entry_allowed"]
    if order_kind == "owned":
        # Isolate the live-order retention branch from the higher-priority
        # live-position branch already exercised by the full snapshot above.
        table = build_v1_lifecycle_closure_table(local_state=runtime.state, exchange_truth=dict(truth, positions=[]))
        pending_row = next(row for row in table.to_dict()["rows"] if row["phase"] == "PENDING_ENTRY")
        assert pending_row["terminality"] == "retain_live_open_order"
    else:
        assert direct.block_reason == expected_kind
        wire["fail"] = True
        gap = await collector._collect_recovery_ledger_account_truth(1001)
        runtime._refresh_recovery_ledger_from_exchange_truth(gap, now_ms=1001)
        assert not runtime.recovery_decision.entry_allowed
        wire["fail"] = False
        wire["orders"].clear()
        clean = await collector._collect_recovery_ledger_account_truth(1002)
        runtime._refresh_recovery_ledger_from_exchange_truth(clean, now_ms=1002)
        assert runtime.recovery_decision.entry_allowed and runtime.recovery_decision.clear_previous_block


@pytest.mark.parametrize("collection,bad_row", [
    ("orders", None), ("orders", {"size": "bad"}),
    ("positions", None), ("positions", {"contract": "OTHER_USDT", "size": "NaN"}),
    ("positions", {"contract": "OTHER_USDT", "size": "bad"}),
    ("positions", {"contract": "OTHER_USDT"}),
])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("scope", ["account", "symbols"])
async def test_recovery_mixed_rows_preserve_stronger_truth(recovery_wire, collection, bad_row, reverse, scope):
    from lightfee.engine.recovery_decision_core import RecoveryEvidenceSnapshot, V1RecoveryDecisionCore
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table
    from lightfee.engine.exchange_truth import require_open_orders_response, probe_venue_open_orders_flat

    runtime, gate, wire = recovery_wire
    if collection == "positions":
        wire[collection].append({"contract": "SAGA_USDT", "size": "-20", "mode": "dual_short", "entry_price": ".05"})
    else:
        wire[collection].append({"contract": "SAGA_USDT", "id": "live-order", "size": "-501", "reduce_only": True})
    wire[collection].append(bad_row)
    if reverse:
        wire[collection].reverse()
    if collection == "positions":
        with pytest.raises(TransportError):
            await gate._transport.fetch_all_positions()
    else:
        with pytest.raises(TransportError):
            require_open_orders_response(wire[collection], venue=Venue.GATE)
        flat, _ = await probe_venue_open_orders_flat(gate, Venue.GATE, "SAGAUSDT")
        assert flat is None
    collector = runtime.recovery_startup_runtime
    truth = (await collector._collect_recovery_ledger_account_truth(1000) if scope == "account" else
             await collector._collect_recovery_ledger_exchange_truth(["SAGAUSDT"], 1000))
    assert not truth["truth_available"] and truth["errors"]
    result = truth["open_orders" if collection == "orders" else "positions"]
    gate_rows = [row for row in result if row["venue"] == "gate"]
    assert len(gate_rows) == len(wire[collection])
    assert any(row.get("raw") == bad_row and row.get("normalization_error") for row in gate_rows)
    if collection == "positions":
        assert sorted(row["quantity"] for row in gate_rows if row["quantity"] is not None) == [20, 501]
    ledger = runtime._refresh_recovery_ledger_from_exchange_truth(truth, now_ms=1000)
    assert not runtime.recovery_decision.entry_allowed
    assert any(item.blocks_all_new_entries for item in ledger.work_items)
    direct = V1RecoveryDecisionCore().decide(RecoveryEvidenceSnapshot(
        local_open_positions=tuple(runtime.state.open_positions.values()), exchange_truth=truth))
    assert not direct.entry_allowed
    assert not build_v1_lifecycle_closure_table(local_state=runtime.state, exchange_truth=truth).summary["entry_allowed"]


async def test_recovery_metadata_timeout_retains_already_observed_positions(recovery_wire):
    runtime, _, wire = recovery_wire
    wire["positions"].extend([
        {"contract": "SAGA_USDT", "size": "-20", "mode": "dual_short", "entry_price": ".05"},
        {"contract": "OTHER_USDT", "size": "1"},
    ])
    wire["slow_metadata"] = True
    probe_budget = runtime.config.runtime.live_recovery_rest_probe_timeout_ms
    runtime.config.runtime.live_recovery_rest_probe_timeout_ms = 40
    collector = runtime.recovery_startup_runtime
    truth = await collector._collect_recovery_ledger_account_truth(1000)
    assert not truth["truth_available"] and any("timeout" in error for error in truth["errors"])
    gate_rows = [row for row in truth["positions"] if row["venue"] == "gate"]
    assert len(gate_rows) == 3
    assert sorted(row["quantity"] for row in gate_rows if row["quantity"] is not None) == [20, 501]
    assert next(row for row in gate_rows if row["quantity"] is None)["raw"]["contract"] == "OTHER_USDT"
    runtime._refresh_recovery_ledger_from_exchange_truth(truth, now_ms=1000)
    assert not runtime.recovery_decision.entry_allowed
    wire["positions"] = wire["positions"][:1]
    wire["slow_metadata"] = False
    runtime.config.runtime.live_recovery_rest_probe_timeout_ms = probe_budget
    clean = await collector._collect_recovery_ledger_account_truth(1001)
    assert clean["truth_available"]
    runtime._refresh_recovery_ledger_from_exchange_truth(clean, now_ms=1001)
    assert runtime.recovery_decision.entry_allowed and runtime.recovery_decision.clear_previous_block


@pytest.mark.parametrize("channel,patches,valid", [
    pytest.param("orders", [], False, id="empty-orders"),
    pytest.param("positions", [], False, id="empty-positions"),
    *[pytest.param("orders", [identity], False, id=f"invalid-identity-{name}")
      for name, identity in [
          ("missing", {}), ("null", {"id": None, "text": None}),
          ("blank", {"id": "  ", "text": ""}),
          ("bool", {"id": True}), ("zero", {"id": 0}), ("negative", {"id": -1}),
          ("float", {"id": 1.5}), ("object", {"id": {"value": 101}}), ("array", {"id": [101]}),
          ("text-object", {"text": {"value": "t-test"}}), ("text-bool", {"text": False}),
          ("text-number", {"text": 101}), ("bare-prefix", {"text": "t-"}),
          ("blank-suffix", {"text": "t-  "}), ("source-only", {"text": "web"}),
      ]],
    pytest.param("orders", [{"id": 101}], True, id="integer-id-only"),
    pytest.param("orders", [{"id": "opaque-order", "text": None}], True, id="opaque-id-null-text"),
    pytest.param("orders", [{"id": None, "text": "t-recovery"}], True, id="cid-only"),
    *[pytest.param("orders", [{"id": 101, "text": label}, {"id": 102, "text": label}], True,
                   id=f"distinct-orders-with-{label}") for label in ("web", "api", "app")],
    pytest.param("orders", [{"id": 101, "status": "finished", "finish_as": "cancelled"}], True,
                 id="zero-fill-terminal"),
    *[pytest.param("orders", rows, False, id=name) for name, rows in [
        ("bad-before-good", [{}, {"id": 101}]),
        ("good-before-bad", [{"id": 101}, {}]),
        ("unknown-before-good", [{"contract": "UNKNOWN_USDT"}, {"id": 101}]),
        ("good-before-unknown", [{"id": 101}, {"contract": "UNKNOWN_USDT"}]),
    ]],
    pytest.param("positions", [{"mode": "dual_long", "size": "NaN"}], False, id="dual-nan"),
    pytest.param("positions", [{"mode": "dual_short", "size": "bad"}], False, id="dual-invalid"),
    pytest.param("positions", [{"mode": "dual_long"}], True, id="dual-valid"),
    pytest.param("positions", [{"mode": "dual_long"}, {"mode": "dual_short", "size": "-1"}],
                 True, id="dual-pair-valid"),
    pytest.param("positions", [{}], True, id="single-valid"),
    pytest.param("positions", [{"size": "0"}], True, id="single-zero"),
    pytest.param("positions", [{}, {"contract": "UNKNOWN_USDT"}], False, id="single-unknown"),
])
async def test_gate_worker_health_requires_validated_update(tmp_path, channel, patches, valid):
    import contextlib
    import websockets
    from lightfee.engine.state import EngineState
    from lightfee.engine.supervisor import Supervisor
    from lightfee.engine.risk_actions import VenueHealthAction
    from lightfee.persistence.journal import Journal
    from lightfee.venues.gate_private_ws import _gate_private_ws_loop
    from tests.test_supervisor_execution import _make_config

    adapter = GateAdapter(mode="live", credential=LiveCredential(api_key="test", api_secret="test"))
    transport = adapter._transport
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=contract("ONE_USDT", "1", ".00001"))))
    base = {"contract": "ONE_USDT", "size": "1"}
    base.update({"left": "1", "status": "open"} if channel == "orders" else {"mode": "single"})
    rows = [{**base, **patch} for patch in patches]
    journal = Journal(tmp_path / "health.jsonl")
    journal.open()
    state = EngineState()
    state.pending_close_reconciliations = [{"long_venue": "gate", "short_venue": "binance"}]
    # Gate has no risk snapshot support; isolate its private-stream risk action.
    supervisor = Supervisor(_make_config(unsupported_risk_snapshot_behavior="ignore"), state, journal)
    connections = 0
    ready, release, sent = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def server(ws):
        nonlocal connections
        connections += 1
        number = connections
        await ws.recv()
        await ws.recv()
        for subscribed in ("futures.orders", "futures.positions"):
            await ws.send(json.dumps({"channel": subscribed, "event": "subscribe", "result": {"status": "success"}}))
        if number == 1:
            await ws.send(json.dumps({"channel": f"futures.{channel}", "event": "update", "result": None}))
        elif number == 2:
            ready.set()
            await release.wait()
            await ws.send(json.dumps({"channel": f"futures.{channel}", "event": "update", "result": rows}))
            sent.set()
        await ws.wait_closed()
    async with websockets.serve(server, "127.0.0.1", 0) as listener:
        task = asyncio.create_task(_gate_private_ws_loop(transport, "test", "test",
            f"ws://127.0.0.1:{listener.sockets[0].getsockname()[1]}", {"ONE_USDT": "ONEUSDT"},
            transport._private_ws_state, 1, 1, 5))
        try:
            await asyncio.wait_for(ready.wait(), 2)
            await asyncio.sleep(.01)
            assert transport.cached_private_connection_health().is_unhealthy()
            before = supervisor._collect_venue_health_views(1000, {Venue.GATE: adapter})[Venue.GATE]
            assert before.action == VenueHealthAction.FAIL_CLOSED
            assert "private_stream_unhealthy" in before.reasons
            release.set()
            await asyncio.wait_for(sent.wait(), 2)
            await asyncio.sleep(.03)
            health = transport.cached_private_connection_health()
            assert health.is_unhealthy() is (not valid)
            assert (health.consecutive_failures == 0) is valid
            after = supervisor._collect_venue_health_views(1000, {Venue.GATE: adapter})[Venue.GATE]
            assert ("private_stream_unhealthy" in after.reasons) is (not valid)
            assert after.action == (VenueHealthAction.NORMAL if valid else VenueHealthAction.FAIL_CLOSED)
            if channel == "positions" and any(row.get("mode", "").startswith("dual") for row in rows):
                assert transport._private_ws_state.position("ONEUSDT") is None
            if channel == "orders" and valid:
                for row in rows:
                    if row.get("id"):
                        update = transport._private_ws_state.order_by_order_id(str(row["id"]))
                        assert update.order_id == str(row["id"])
                    else:
                        update = transport._private_ws_state.order_by_client_id("recovery")
                        assert update is not None and update.client_order_id == "recovery"
                        assert update.order_id == ""
                    assert update.filled_quantity == 0
                    if row.get("text") is None:
                        assert update.client_order_id is None
                    if row.get("text") in {"web", "api", "app"}:
                        assert update.client_order_id is None
                        assert transport._private_ws_state.order_by_client_id(row["text"]) is None
                    if row.get("finish_as") == "cancelled":
                        assert update.state == PassiveOrderState.CANCELED
        finally:
            release.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await transport.close()
            journal.close()
