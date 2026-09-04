"""Production-path regressions for automatic historical close evidence.

Candidate discovery is deliberately weaker than billing evidence.  The live
runtime may only settle an evidence debt after trusted terminal-flat truth, a
unique history match, and the adapter's existing exact order/execution recheck.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from lightfee.core.domain import (
    HistoricalCloseEvidenceDiscovery,
    OrderFillReconciliation,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.engine.close_runtime import CloseRuntime
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.venues.binance import (
    BinanceAdapter,
    find_binance_historical_close_order_candidates,
)
from lightfee.venues.bybit import (
    BybitAdapter,
    find_bybit_historical_close_order_candidates,
)
from lightfee.venues.aster import AsterAdapter
from lightfee.venues.bitget import (
    BitgetAccountProfile,
    BitgetAdapter,
    find_bitget_historical_close_order_candidates,
)
from lightfee.venues.transport import LiveCredential


NOW_MS = 1_787_400_000_000


def _snapshot(
    *,
    position_id: str,
    symbol: str,
    long_venue: Venue,
    short_venue: Venue,
    long_quantity: float,
    short_quantity: float,
) -> dict:
    return {
        "position_id": position_id,
        "symbol": symbol,
        "long_venue": long_venue.value,
        "short_venue": short_venue.value,
        "long_quantity": long_quantity,
        "short_quantity": short_quantity,
        "matched_quantity": max(long_quantity, short_quantity),
        "long_entry_price": 0.01,
        "short_entry_price": 0.011,
        "total_entry_fee_quote": 0.02,
        "entry_fee_evidence_complete": True,
        "captured_funding_quote": 0.0,
    }


def _debt(
    *,
    snapshot: dict,
    long_legs: list[dict],
    short_legs: list[dict],
    reason: str = "missing_close_order_identity",
) -> dict:
    return {
        "position_id": snapshot["position_id"],
        "symbol": snapshot["symbol"],
        "kind": "final",
        "reason": "funding_capture",
        "closed_at_ms": NOW_MS - 60_000,
        "created_cycle": 0,
        "attempt_count": 0,
        "next_attempt_ms": 0,
        "position_snapshot": snapshot,
        "long_legs": long_legs,
        "short_legs": short_legs,
        "reconciliation_status": "evidence_debt",
        "evidence_debt_reason": reason,
        "billing_reconciliation_required": True,
    }


def _fill(
    *,
    venue: Venue,
    symbol: str,
    side: Side,
    quantity: float,
    price: float,
    order_id: str,
    client_order_id: str = "",
    fee_quote: float | None = 0.01,
    provenance: str = "exact_exchange_execution",
) -> OrderFillReconciliation:
    return OrderFillReconciliation(
        venue=venue,
        symbol=symbol,
        side=side,
        quantity=quantity,
        average_price=price,
        order_id=order_id,
        client_order_id=client_order_id or None,
        fee_quote=fee_quote,
        filled_at_ms=NOW_MS - 59_000,
        metadata={
            "fee_evidence_complete": fee_quote is not None,
            "historical_evidence_provenance": provenance,
        },
    )


class _Adapter:
    def __init__(
        self,
        venue: Venue,
        *,
        exact: dict[str, OrderFillReconciliation] | None = None,
        discovery: HistoricalCloseEvidenceDiscovery | None = None,
        position_quantity: float = 0.0,
        open_orders: list[dict] | None = None,
    ) -> None:
        self.venue = venue
        self._exact = exact or {}
        self._discovery = discovery
        self._position_quantity = position_quantity
        self._open_orders = [] if open_orders is None else open_orders
        self.fetch_order_fill_reconciliation = AsyncMock(side_effect=self._fetch_exact)
        self.discover_historical_close_fill_reconciliation = AsyncMock(
            side_effect=self._discover
        )

    async def _fetch_exact(self, _symbol: str, order_id: str, _client_id: str):
        return self._exact.get(order_id)

    async def _discover(self, **_kwargs):
        return self._discovery

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=self._position_quantity,
            entry_price=0.0,
            observed_at_ms=NOW_MS,
        )

    async def fetch_open_orders(self, _symbol: str) -> list[dict]:
        return list(self._open_orders)


def _ctx(task: dict, adapters: dict[Venue, object]) -> MagicMock:
    ctx = MagicMock()
    ctx.state.pending_close_reconciliations = [task]
    ctx.state.open_positions = {}
    ctx.state.pending_entries = {}
    ctx.state.pending_passive_closes = {}
    ctx.state.pending_residual_repairs = []
    ctx.state.tick_count = 10
    ctx.state.lifecycle = EngineLifecycle.RUNNING
    ctx.state.risk_mode = GlobalRiskMode.RUNNING
    ctx.state.operator.requested_mode = None
    ctx.state.last_error = None
    ctx.config.runtime.mode = "live"
    ctx.venue_adapters = adapters
    ctx._flush_adapter_order_diagnostics = lambda _adapter: None
    for name in (
        "_fetch_close_leg_reconciliations",
        "_fetch_pending_close_terminal_live_sizes",
        "_try_register_terminal_fill_evidence_debt",
        "_venue_private_position_confirmed",
        "_open_positions_private_confirmation_ready",
        "_resolve_local_l2_mid",
    ):
        setattr(ctx, name, None)
    return ctx


def _critical_payload(ctx: MagicMock, kind: str) -> dict | None:
    for call in ctx.journal.append_critical.call_args_list:
        if len(call.args) >= 3 and call.args[1] == kind:
            return call.args[2]
    return None


@pytest.mark.asyncio
async def test_coti_debt_unique_binance_history_exactly_rechecks_and_reconciles():
    snapshot = _snapshot(
        position_id="entry-coti-history",
        symbol="COTIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        long_quantity=0.0,
        short_quantity=2400.0,
    )
    snapshot.update(
        {
            "long_entry_price": 0.010005,
            "short_entry_price": 0.010001,
            "total_entry_fee_quote": 0.01800708,
            "captured_funding_quote": 0.06767389608,
            "realized_exit_fee_quote": 0.01335444,
            "realized_price_pnl_quote": 0.2688,
        }
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    binance = BinanceAdapter(
        mode="live",
        credential=LiveCredential(api_key="key", api_secret="secret"),
    )
    binance.fetch_position = AsyncMock(
        return_value=PositionSnapshot(
            venue=Venue.BINANCE,
            symbol="COTIUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=NOW_MS,
        )
    )
    binance.fetch_open_orders = AsyncMock(return_value=[])
    seen: list[str] = []

    async def request(_method: str, path: str, **kwargs):
        seen.append(path)
        if path == "/fapi/v1/allOrders":
            assert kwargs["params"]["limit"] == 1000
            return [
                {
                    "symbol": "COTIUSDT",
                    "side": "BUY",
                    "positionSide": "LONG",
                    "reduceOnly": True,
                    "status": "FILLED",
                    "executedQty": "2400",
                    "avgPrice": "0.010000",
                    "orderId": "wrong-hedge-leg",
                    "clientOrderId": "",
                    "updateTime": NOW_MS - 59_500,
                },
                {
                    "symbol": "COTIUSDT",
                    "side": "BUY",
                    "positionSide": "SHORT",
                    # Binance Hedge Mode identifies the close by positionSide;
                    # reduceOnly cannot be required by the matching contract.
                    "reduceOnly": False,
                    "status": "FILLED",
                    "executedQty": "2400",
                    "avgPrice": "0.010112",
                    "orderId": "7918051356",
                    "clientOrderId": "lfxsf1b3c859d1f7e023",
                    "updateTime": NOW_MS - 59_000,
                },
            ]
        if path == "/fapi/v1/order":
            assert kwargs["params"]["orderId"] == "7918051356"
            return {
                "symbol": "COTIUSDT",
                "side": "BUY",
                "positionSide": "SHORT",
                "status": "FILLED",
                "executedQty": "2400",
                "avgPrice": "0.010112",
                "orderId": "7918051356",
                "clientOrderId": "lfxsf1b3c859d1f7e023",
                "updateTime": NOW_MS - 59_000,
            }
        if path == "/fapi/v1/userTrades":
            assert kwargs["params"]["orderId"] == "7918051356"
            return [
                {"orderId": "7918051356", "commission": "0.004"},
                {"orderId": "7918051356", "commission": "0.00813439"},
            ]
        raise AssertionError(path)

    binance._transport._request = AsyncMock(side_effect=request)
    bybit = _Adapter(Venue.BYBIT)
    ctx = _ctx(task, {Venue.BINANCE: binance, Venue.BYBIT: bybit})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    assert ctx.state.pending_close_reconciliations == []
    assert seen == ["/fapi/v1/allOrders", "/fapi/v1/order", "/fapi/v1/userTrades"]
    payload = _critical_payload(ctx, "exit.reconciled")
    assert payload is not None
    assert payload["short_order_id"] == "7918051356"
    assert payload["segment_exit_fee_quote"] == pytest.approx(0.01213439)
    assert payload["exit_fee_quote"] == pytest.approx(0.02548883)
    assert payload["price_pnl"] == pytest.approx(0.0024)
    assert payload["historical_evidence_resolution"]["short"]["provenance"] == (
        "system_client_id_execution"
    )
    assert payload["venue_statement_reconciled"] is True


def test_bitget_history_candidates_use_executed_quantity_and_close_ownership():
    """History discovery must not promote submitted size or an opening row."""
    rows = [
        {
            "symbol": "COTIUSDT",
            "side": "buy",
            "posSide": "short",
            "tradeSide": "close",
            "status": "filled",
            "baseVolume": "2400",
            "size": "9999",
            "orderId": "executed-quantity",
            "clientOid": "lf-close-1",
            "uTime": str(NOW_MS - 60_000),
        },
        {
            "symbol": "COTIUSDT",
            "side": "buy",
            "posSide": "net",
            "reduceOnly": "NO",
            "tradeSide": "open",
            "status": "filled",
            "cumExecQty": "2400",
            "size": "2400",
            "orderId": "opening-row",
            "uTime": str(NOW_MS - 60_000),
        },
        {
            "symbol": "COTIUSDT",
            "side": "buy",
            "posSide": "short",
            "tradeSide": "close",
            "status": "filled",
            "baseVolume": "0",
            "size": "2400",
            "orderId": "zero-execution",
            "uTime": str(NOW_MS - 60_000),
        },
    ]

    candidates = find_bitget_historical_close_order_candidates(
        rows,
        symbol="COTIUSDT",
        side=Side.BUY,
        position_side="SHORT",
        quantity=2400.0,
        closed_at_ms=NOW_MS - 60_000,
    )

    assert [candidate["order_id"] for candidate in candidates] == [
        "executed-quantity"
    ]
    assert candidates[0]["executed_quantity"] == pytest.approx(2400.0)


def test_bitget_history_close_ownership_covers_official_trade_side_variants():
    """Every documented close marker is accepted; missing UTA marker is not."""
    close_markers = (
        "offset_close_short",
        "burst_close_short",
        "delivery_close_short",
        "dte_sys_adl_close_short",
        "reduce_buy_single",
        "burst_buy_single",
        "delivery_buy_single",
        "dte_sys_adl_buy_in_single_side_mode",
    )
    rows = [
        {
            "symbol": "COTIUSDT",
            "side": "buy",
            "posSide": "short",
            "tradeSide": marker,
            "status": "filled",
            "baseVolume": "1",
            "orderId": f"close-{index}",
            "uTime": str(NOW_MS - 60_000 + index),
        }
        for index, marker in enumerate(close_markers)
    ]
    rows.append(
        {
            "symbol": "COTIUSDT",
            "side": "buy",
            "posSide": "short",
            "tradeSide": "open",
            "status": "filled",
            "cumExecQty": "1",
            "orderId": "uta-hedge-opening-row",
            "uTime": str(NOW_MS - 60_000),
        }
    )

    candidates = find_bitget_historical_close_order_candidates(
        rows,
        symbol="COTIUSDT",
        side=Side.BUY,
        position_side="SHORT",
        quantity=1.0,
        closed_at_ms=NOW_MS - 60_000,
    )

    assert [candidate["order_id"] for candidate in candidates] == [
        f"close-{index}" for index in range(len(close_markers))
    ]

    uta_hedge_close = find_bitget_historical_close_order_candidates(
        [
            {
                "symbol": "COTIUSDT",
                "side": "buy",
                "posSide": "short",
                "status": "filled",
                "cumExecQty": "1",
                "orderId": "uta-hedge-close-without-reduce-marker",
                "uTime": str(NOW_MS - 60_000),
            }
        ],
        symbol="COTIUSDT",
        side=Side.BUY,
        position_side="SHORT",
        quantity=1.0,
        closed_at_ms=NOW_MS - 60_000,
    )
    assert [candidate["order_id"] for candidate in uta_hedge_close] == [
        "uta-hedge-close-without-reduce-marker"
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("profile", "history_path", "detail_path", "history_row", "detail_row"),
    [
        (
            BitgetAccountProfile.CLASSIC,
            "/api/v2/mix/order/orders-history",
            "/api/v2/mix/order/detail",
            {
                "symbol": "COTIUSDT",
                "side": "buy",
                "posSide": "short",
                "tradeSide": "close",
                "status": "filled",
                "baseVolume": "2400",
                "size": "9999",
                "orderId": "classic-history-order",
                "clientOid": "lf-classic-close",
                "uTime": str(NOW_MS - 60_000),
            },
            {
                "symbol": "COTIUSDT",
                "side": "buy",
                "status": "filled",
                "baseVolume": "2400",
                "priceAvg": "0.010112",
                "fee": "0",
                "orderId": "classic-history-order",
                "clientOid": "lf-classic-close",
                "uTime": str(NOW_MS - 59_000),
            },
        ),
        (
            BitgetAccountProfile.UTA,
            "/api/v3/trade/history-orders",
            "/api/v3/trade/order-info",
            {
                "symbol": "COTIUSDT",
                "side": "buy",
                "posSide": "net",
                "reduceOnly": "YES",
                "orderStatus": "filled",
                "cumExecQty": "2400",
                "orderId": "uta-history-order",
                "clientOid": "lf-uta-close",
                "updatedTime": str(NOW_MS - 60_000),
            },
            {
                "symbol": "COTIUSDT",
                "side": "buy",
                "orderStatus": "filled",
                "cumExecQty": "2400",
                "avgPrice": "0.010112",
                "feeDetail": [{"fee": "0", "feeCoin": "USDT"}],
                "orderId": "uta-history-order",
                "clientOid": "lf-uta-close",
                "updatedTime": str(NOW_MS - 59_000),
            },
        ),
    ],
)
async def test_bitget_history_discovery_uses_family_contract_and_exact_recheck(
    profile,
    history_path,
    detail_path,
    history_row,
    detail_row,
):
    adapter = BitgetAdapter(
        mode="live",
        credential=LiveCredential(api_key="key", api_secret="secret", api_passphrase="pass"),
    )
    adapter._profile = profile
    calls: list[tuple[str, str, dict]] = []

    async def request(method, path, *, params=None, body=None, private=False):
        del body, private
        calls.append((method, path, dict(params or {})))
        if path == history_path:
            return {"code": "00000", "data": {"entrustedList" if profile is BitgetAccountProfile.CLASSIC else "list": [history_row], "endId" if profile is BitgetAccountProfile.CLASSIC else "cursor": ""}}
        if path == detail_path:
            return {"code": "00000", "data": detail_row}
        raise AssertionError(path)

    adapter._transport._request = request
    try:
        result = await adapter.discover_historical_close_fill_reconciliation(
            symbol="COTIUSDT",
            side=Side.BUY,
            position_side="SHORT",
            quantity=2400.0,
            closed_at_ms=NOW_MS - 60_000,
        )
    finally:
        await adapter.shutdown()

    assert result.classification == "unique_candidate_exact_recheck"
    assert result.candidate_count == 1
    assert result.reconciliation is not None
    assert result.reconciliation.fee_quote == pytest.approx(0.0)
    assert result.reconciliation.metadata["fee_evidence_complete"] is True
    assert result.reconciliation.metadata["historical_candidate_endpoint"] == history_path
    assert calls[0][0:2] == ("GET", history_path)
    assert calls[0][2]["symbol"] == "COTIUSDT"
    assert calls[0][2]["limit"] == 100
    required_family_param = "productType" if profile is BitgetAccountProfile.CLASSIC else "category"
    assert calls[0][2][required_family_param] == "USDT-FUTURES"
    assert calls[1][0:2] == ("GET", detail_path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("profile", "collection_key"),
    [
        (BitgetAccountProfile.CLASSIC, "entrustedList"),
        (BitgetAccountProfile.UTA, "list"),
    ],
)
async def test_bitget_history_discovery_treats_null_collection_as_empty_page(
    profile,
    collection_key,
):
    """Empty Bitget history windows may encode their collection as JSON null."""
    adapter = BitgetAdapter(
        mode="live",
        credential=LiveCredential(api_key="key", api_secret="secret", api_passphrase="pass"),
    )
    adapter._profile = profile

    async def request(method, path, *, params=None, body=None, private=False):
        del method, params, body, private
        assert path in {"/api/v2/mix/order/orders-history", "/api/v3/trade/history-orders"}
        return {"code": "00000", "data": {collection_key: None, "endId": "", "cursor": ""}}

    adapter._transport._request = request
    try:
        result = await adapter.discover_historical_close_fill_reconciliation(
            symbol="COTIUSDT",
            side=Side.BUY,
            position_side="SHORT",
            quantity=2400.0,
            closed_at_ms=NOW_MS - 60_000,
        )
    finally:
        await adapter.shutdown()

    assert result.classification == "no_candidate"
    assert result.candidate_count == 0


@pytest.mark.asyncio
async def test_bitget_history_discovery_follows_uta_cursor_before_matching():
    adapter = BitgetAdapter(
        mode="live",
        credential=LiveCredential(api_key="key", api_secret="secret", api_passphrase="pass"),
    )
    adapter._profile = BitgetAccountProfile.UTA
    history_calls: list[dict] = []

    async def request(method, path, *, params=None, body=None, private=False):
        del method, body, private
        if path == "/api/v3/trade/history-orders":
            history_calls.append(dict(params or {}))
            if len(history_calls) == 1:
                return {
                    "code": "00000",
                    "data": {
                        "list": [],
                        "cursor": "next-page",
                    },
                }
            return {
                "code": "00000",
                "data": {
                    "list": [{
                        "symbol": "COTIUSDT",
                        "side": "buy",
                        "posSide": "net",
                        "reduceOnly": "YES",
                        "orderStatus": "filled",
                        "cumExecQty": "2400",
                        "orderId": "cursor-history-order",
                        "clientOid": "lf-cursor-close",
                        "updatedTime": str(NOW_MS - 60_000),
                    }],
                    "cursor": "",
                },
            }
        if path == "/api/v3/trade/order-info":
            return {
                "code": "00000",
                "data": {
                    "symbol": "COTIUSDT",
                    "side": "buy",
                    "orderStatus": "filled",
                    "cumExecQty": "2400",
                    "avgPrice": "0.010112",
                    "feeDetail": [{"fee": "0"}],
                    "orderId": "cursor-history-order",
                    "clientOid": "lf-cursor-close",
                    "updatedTime": str(NOW_MS - 59_000),
                },
            }
        raise AssertionError(path)

    adapter._transport._request = request
    try:
        result = await adapter.discover_historical_close_fill_reconciliation(
            symbol="COTIUSDT",
            side=Side.BUY,
            position_side="SHORT",
            quantity=2400.0,
            closed_at_ms=NOW_MS - 60_000,
        )
    finally:
        await adapter.shutdown()

    assert result.classification == "unique_candidate_exact_recheck"
    assert len(history_calls) == 2
    assert history_calls[1]["cursor"] == "next-page"


@pytest.mark.asyncio
async def test_bitget_history_discovery_is_wired_through_close_runtime():
    """The live close path must settle a Bitget evidence debt via the contract."""
    snapshot = _snapshot(
        position_id="entry-bitget-history",
        symbol="COTIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.BITGET,
        long_quantity=0.0,
        short_quantity=2400.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    bitget = BitgetAdapter(
        mode="live",
        credential=LiveCredential(api_key="key", api_secret="secret", api_passphrase="pass"),
    )
    bitget._profile = BitgetAccountProfile.CLASSIC
    bitget.fetch_position = AsyncMock(
        return_value=PositionSnapshot(
            venue=Venue.BITGET,
            symbol="COTIUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=NOW_MS,
        )
    )
    bitget.fetch_open_orders = AsyncMock(return_value=[])

    async def request(method, path, *, params=None, body=None, private=False):
        del method, body, private
        if path == "/api/v2/mix/order/orders-history":
            return {
                "code": "00000",
                "data": {
                    "entrustedList": [{
                        "symbol": "COTIUSDT",
                        "side": "buy",
                        "posSide": "short",
                        "tradeSide": "close",
                        "status": "filled",
                        "baseVolume": "2400",
                        "orderId": "runtime-history-order",
                        "clientOid": "lf-runtime-close",
                        "uTime": str(NOW_MS - 60_000),
                    }],
                    "endId": "",
                },
            }
        if path == "/api/v2/mix/order/detail":
            return {
                "code": "00000",
                "data": {
                    "symbol": "COTIUSDT",
                    "side": "buy",
                    "status": "filled",
                    "baseVolume": "2400",
                    "priceAvg": "0.010112",
                    "fee": "0",
                    "orderId": "runtime-history-order",
                    "clientOid": "lf-runtime-close",
                    "uTime": str(NOW_MS - 59_000),
                },
            }
        raise AssertionError(path)

    bitget._transport._request = request
    bybit = _Adapter(Venue.BYBIT)
    ctx = _ctx(task, {Venue.BYBIT: bybit, Venue.BITGET: bitget})

    try:
        await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)
    finally:
        await bitget.shutdown()

    assert ctx.state.pending_close_reconciliations == []
    payload = _critical_payload(ctx, "exit.reconciled")
    assert payload is not None
    assert payload["historical_evidence_resolution"]["short"]["order_id"] == (
        "runtime-history-order"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("order_status", "expected_reconciled"),
    [
        ("FILLED", True),
        ("CLOSED", False),
        ("FINISHED", False),
        ("COMPLETED", False),
        ("DONE", False),
    ],
)
async def test_aster_debt_requires_documented_filled_order_for_exact_recheck(
    order_status: str,
    expected_reconciled: bool,
):
    """Only Aster's documented final order status can settle live audit debt."""
    snapshot = _snapshot(
        position_id="entry-aster-history",
        symbol="COTIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.ASTER,
        long_quantity=0.0,
        short_quantity=2400.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    aster = AsterAdapter()
    aster._private = MagicMock()
    aster.fetch_position = AsyncMock(
        return_value=PositionSnapshot(
            venue=Venue.ASTER,
            symbol="COTIUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=NOW_MS,
        )
    )
    aster.fetch_open_orders = AsyncMock(return_value=[])
    seen: list[str] = []

    async def request(_method: str, path: str, **kwargs):
        seen.append(path)
        if path == "/fapi/v3/userTrades":
            assert kwargs["params"]["symbol"] == "COTIUSDT"
            assert kwargs["params"]["limit"] == 1000
            return [
                {
                    "symbol": "COTIUSDT",
                    "side": "BUY",
                    "positionSide": "SHORT",
                    "orderId": "aster-close-1",
                    "clientOrderId": "lfxs-aster-close-1",
                    "qty": "1200",
                    "price": "0.010112",
                    "commission": "-0.004",
                    "commissionAsset": "USDT",
                    "time": NOW_MS - 59_100,
                },
                {
                    "symbol": "COTIUSDT",
                    "side": "BUY",
                    "positionSide": "SHORT",
                    "orderId": "aster-close-1",
                    "clientOrderId": "lfxs-aster-close-1",
                    "qty": "1200",
                    "price": "0.010112",
                    "commission": "-0.00813439",
                    "commissionAsset": "USDT",
                    "time": NOW_MS - 59_000,
                },
            ]
        if path == "/fapi/v3/order":
            assert kwargs["params"] == {
                "symbol": "COTIUSDT",
                "orderId": "aster-close-1",
            }
            return {
                "symbol": "COTIUSDT",
                "side": "BUY",
                "positionSide": "SHORT",
                "status": order_status,
                "executedQty": "2400",
                "avgPrice": "0.010112",
                "orderId": "aster-close-1",
                "clientOrderId": "lfxs-aster-close-1",
                "updateTime": NOW_MS - 59_000,
            }
        raise AssertionError(path)

    aster._private._request = AsyncMock(side_effect=request)
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.ASTER: aster})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    assert seen == ["/fapi/v3/userTrades", "/fapi/v3/order"]
    payload = _critical_payload(ctx, "exit.reconciled")
    if expected_reconciled:
        assert ctx.state.pending_close_reconciliations == []
        assert payload is not None
        assert payload["short_order_id"] == "aster-close-1"
        assert payload["segment_exit_fee_quote"] == pytest.approx(0.01213439)
        assert payload["historical_evidence_resolution"]["short"]["provenance"] == (
            "aster_v3_user_trades_exact_order"
        )
    else:
        assert ctx.state.pending_close_reconciliations == [task]
        assert payload is None
        assert task["automatic_history_terminal_reason"] == (
            "exact_recheck_identity_mismatch"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("order_reduce_only", [True, False])
async def test_aster_user_trade_without_reduce_only_is_candidate_only(
    order_reduce_only: bool,
):
    """Aster V3 omits this field on trades; only its exact order may prove it."""
    snapshot = _snapshot(
        position_id="entry-aster-one-way-history",
        symbol="ONGUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.ASTER,
        long_quantity=0.0,
        short_quantity=149.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    aster = AsterAdapter()
    aster._private = MagicMock()
    aster.fetch_position = AsyncMock(
        return_value=PositionSnapshot(
            venue=Venue.ASTER,
            symbol="ONGUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=NOW_MS,
        )
    )
    aster.fetch_open_orders = AsyncMock(return_value=[])

    async def request(_method: str, path: str, **_kwargs):
        if path == "/fapi/v3/userTrades":
            # Captured production V3 shape: the execution is in one-way mode
            # (BOTH) and does not include reduceOnly.
            return [{
                "symbol": "ONGUSDT",
                "side": "BUY",
                "positionSide": "BOTH",
                "orderId": "19932399",
                "clientOrderId": "lfex5432933e299cf15e",
                "qty": "149",
                "price": "0.1569400",
                "commission": "0",
                "commissionAsset": "USDT",
                "time": NOW_MS - 59_000,
            }]
        if path == "/fapi/v3/order":
            return {
                "symbol": "ONGUSDT",
                "side": "BUY",
                "positionSide": "BOTH",
                "reduceOnly": order_reduce_only,
                "status": "FILLED",
                "executedQty": "149",
                "avgPrice": "0.1569400",
                "orderId": "19932399",
                "clientOrderId": "lfex5432933e299cf15e",
                "updateTime": NOW_MS - 59_000,
            }
        raise AssertionError(path)

    aster._private._request = AsyncMock(side_effect=request)
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.ASTER: aster})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    if order_reduce_only:
        assert ctx.state.pending_close_reconciliations == []
        assert _critical_payload(ctx, "exit.reconciled") is not None
    else:
        assert ctx.state.pending_close_reconciliations == [task]
        assert _critical_payload(ctx, "exit.reconciled") is None
        assert task["automatic_history_terminal_reason"] == (
            "exact_recheck_identity_mismatch"
        )


@pytest.mark.asyncio
async def test_history_discovery_unsupported_debt_reopens_once_when_adapter_capability_arrives():
    """Only the old capability-missing terminal result may be retried."""
    snapshot = _snapshot(
        position_id="entry-history-capability-upgrade",
        symbol="COTIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.ASTER,
        long_quantity=0.0,
        short_quantity=2400.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    task.update(
        {
            "automatic_history_terminal_status": "irrecoverable_audit_debt",
            "automatic_history_terminal_reason": "history_discovery_unsupported",
            "automatic_history_terminalized_at_ms": NOW_MS - 120_000,
        }
    )
    aster = _Adapter(
        Venue.ASTER,
        discovery=HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=_fill(
                venue=Venue.ASTER,
                symbol="COTIUSDT",
                side=Side.BUY,
                quantity=2400.0,
                price=0.010112,
                order_id="aster-reopened-history",
            ),
        ),
    )
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.ASTER: aster})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    assert ctx.state.pending_close_reconciliations == []
    aster.discover_historical_close_fill_reconciliation.assert_awaited_once()
    assert _critical_payload(ctx, "exit.reconciled") is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted_reason", [True, False])
async def test_legacy_aster_no_candidate_debt_retries_the_fixed_parser_once(
    persisted_reason: bool,
):
    """Only legacy Aster no-candidate debt may see the corrected parser."""
    snapshot = _snapshot(
        position_id="entry-aster-legacy-no-candidate",
        symbol="ONGUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.ASTER,
        long_quantity=0.0,
        short_quantity=149.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    task.update(
        {
            "automatic_history_terminal_status": "irrecoverable_audit_debt",
            "automatic_history_terminal_reason": "no_candidate",
            "automatic_history_terminalized_at_ms": NOW_MS - 120_000,
        }
    )
    if not persisted_reason:
        task.pop("evidence_debt_reason")
    aster = _Adapter(
        Venue.ASTER,
        discovery=HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=_fill(
                venue=Venue.ASTER,
                symbol="ONGUSDT",
                side=Side.BUY,
                quantity=149.0,
                price=0.15694,
                order_id="19932399",
                fee_quote=0.0,
                provenance="aster_v3_user_trades_exact_order",
            ),
        ),
    )
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.ASTER: aster})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    assert ctx.state.pending_close_reconciliations == []
    aster.discover_historical_close_fill_reconciliation.assert_awaited_once()
    assert _critical_payload(ctx, "exit.reconciled") is not None


@pytest.mark.asyncio
async def test_legacy_aster_no_candidate_debt_stays_terminal_after_one_fixed_miss():
    """A legacy re-scan which still finds nothing must not become a loop."""
    snapshot = _snapshot(
        position_id="entry-aster-legacy-fixed-miss",
        symbol="ONGUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.ASTER,
        long_quantity=0.0,
        short_quantity=149.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    task.update(
        {
            "automatic_history_terminal_status": "irrecoverable_audit_debt",
            "automatic_history_terminal_reason": "no_candidate",
            "automatic_history_terminalized_at_ms": NOW_MS - 120_000,
        }
    )
    aster = _Adapter(
        Venue.ASTER,
        discovery=HistoricalCloseEvidenceDiscovery(
            classification="aster_v3_no_candidate",
            candidate_count=0,
        ),
    )
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.ASTER: aster})
    runtime = CloseRuntime(ctx)

    await runtime._process_pending_close_reconciliations(NOW_MS)
    await runtime._process_pending_close_reconciliations(NOW_MS + 60_000)

    assert ctx.state.pending_close_reconciliations == [task]
    aster.discover_historical_close_fill_reconciliation.assert_awaited_once()
    assert task["automatic_history_terminal_reason"] == "aster_v3_no_candidate"


@pytest.mark.asyncio
async def test_fixed_aster_no_candidate_debt_never_reopens():
    """A current strict Aster miss remains terminal after the parser repair."""
    snapshot = _snapshot(
        position_id="entry-aster-current-no-candidate",
        symbol="ONGUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.ASTER,
        long_quantity=0.0,
        short_quantity=149.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    task.update(
        {
            "automatic_history_terminal_status": "irrecoverable_audit_debt",
            "automatic_history_terminal_reason": "aster_v3_no_candidate",
            "automatic_history_terminalized_at_ms": NOW_MS - 120_000,
        }
    )
    aster = _Adapter(Venue.ASTER)
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.ASTER: aster})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    assert ctx.state.pending_close_reconciliations == [task]
    aster.discover_historical_close_fill_reconciliation.assert_not_awaited()
    assert task["automatic_history_terminal_reason"] == "aster_v3_no_candidate"


@pytest.mark.asyncio
async def test_strict_history_terminal_debt_never_reopens_for_new_adapter_capability():
    """Capability upgrades cannot turn an ambiguous history into evidence."""
    snapshot = _snapshot(
        position_id="entry-history-ambiguous",
        symbol="COTIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.ASTER,
        long_quantity=0.0,
        short_quantity=2400.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    task.update(
        {
            "automatic_history_terminal_status": "irrecoverable_audit_debt",
            "automatic_history_terminal_reason": "ambiguous_candidates",
            "automatic_history_terminalized_at_ms": NOW_MS - 120_000,
        }
    )
    aster = _Adapter(Venue.ASTER)
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.ASTER: aster})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    assert ctx.state.pending_close_reconciliations == [task]
    aster.discover_historical_close_fill_reconciliation.assert_not_awaited()
    assert task["automatic_history_terminal_reason"] == "ambiguous_candidates"


@pytest.mark.asyncio
async def test_ong_bybit_takeover_is_reconciled_without_claiming_v2_submission():
    snapshot = _snapshot(
        position_id="entry-ong-history",
        symbol="ONGUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        long_quantity=270.0,
        short_quantity=270.0,
    )
    task = _debt(
        snapshot=snapshot,
        long_legs=[
            {
                "venue": "binance",
                "order_id": "",
                "client_order_id": "lfexe2ab679cf8440975",
                "quantity": 0.0,
            },
            {
                "venue": "binance",
                "order_id": "",
                "client_order_id": "lfex07006fc64ee6ed2c",
                "quantity": 0.0,
            },
        ],
        short_legs=[],
    )
    binance = BinanceAdapter(
        mode="live",
        credential=LiveCredential(api_key="key", api_secret="secret"),
    )
    binance.fetch_position = AsyncMock(
        return_value=PositionSnapshot(
            venue=Venue.BINANCE,
            symbol="ONGUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=NOW_MS,
        )
    )
    binance.fetch_open_orders = AsyncMock(return_value=[])
    binance_seen: list[str] = []

    async def binance_request(_method: str, path: str, **kwargs):
        binance_seen.append(path)
        params = kwargs.get("params", {})
        if path == "/fapi/v1/order" and params.get("origClientOrderId") == (
            "lfexe2ab679cf8440975"
        ):
            return {"code": -2013, "msg": "Order does not exist"}
        if path == "/fapi/v1/allOrders":
            return [
                {
                    "symbol": "ONGUSDT",
                    "side": "SELL",
                    "positionSide": "LONG",
                    "reduceOnly": False,
                    "status": "FILLED",
                    "executedQty": "270",
                    "avgPrice": "0.08768",
                    "orderId": "2926675711",
                    "clientOrderId": "lfex07006fc64ee6ed2c",
                    "updateTime": NOW_MS - 59_000,
                }
            ]
        if path == "/fapi/v1/order" and params.get("orderId") == "2926675711":
            return {
                "symbol": "ONGUSDT",
                "side": "SELL",
                "positionSide": "LONG",
                "status": "FILLED",
                "executedQty": "270",
                "avgPrice": "0.08768",
                "orderId": "2926675711",
                "clientOrderId": "lfex07006fc64ee6ed2c",
                "updateTime": NOW_MS - 59_000,
            }
        if path == "/fapi/v1/userTrades":
            return [
                {"orderId": "2926675711", "commission": "0.00473472"}
            ]
        raise AssertionError((path, params))

    binance._transport._request = AsyncMock(side_effect=binance_request)
    bybit = BybitAdapter(
        mode="live",
        credential=LiveCredential(api_key="key", api_secret="secret"),
    )
    bybit.fetch_position = AsyncMock(
        return_value=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="ONGUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=NOW_MS,
        )
    )
    bybit.fetch_open_orders = AsyncMock(return_value=[])
    seen: list[tuple[str, str]] = []

    async def request(_method: str, path: str, **kwargs):
        cursor = str(kwargs.get("params", {}).get("cursor") or "")
        seen.append((path, cursor))
        if path == "/v5/order/history" and not cursor:
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "symbol": "ONGUSDT",
                            "side": "Buy",
                            "positionIdx": 2,
                            "reduceOnly": True,
                            "orderStatus": "Filled",
                            "cumExecQty": "270",
                            "orderId": "9cec8c96-dc21-4c31-baf9-ec43f6184195",
                            "orderLinkId": "",
                            "updatedTime": NOW_MS - 59_000,
                        }
                    ],
                    "nextPageCursor": "page-2",
                },
            }
        if path == "/v5/order/history" and cursor == "page-2":
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "symbol": "ONGUSDT",
                            "side": "Buy",
                            "positionIdx": 1,
                            "reduceOnly": True,
                            "orderStatus": "Filled",
                            "cumExecQty": "270",
                            "orderId": "wrong-long-position",
                            "orderLinkId": "",
                            "updatedTime": NOW_MS - 58_000,
                        }
                    ],
                    "nextPageCursor": "",
                },
            }
        if path == "/v5/execution/list":
            assert kwargs["params"]["orderId"] == (
                "9cec8c96-dc21-4c31-baf9-ec43f6184195"
            )
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "orderId": "different-order",
                            "execId": "unrelated-funding",
                            "execType": "Funding",
                            "side": "Buy",
                            "execQty": "9999",
                            "execPrice": "9",
                            "execFee": "999",
                            "execTime": str(NOW_MS - 59_500),
                        },
                        {
                            "orderId": "",
                            "execId": "unscoped-funding",
                            "execType": "Funding",
                            "side": "Buy",
                            "execQty": "9999",
                            "execPrice": "9",
                            "execFee": "999",
                            "execTime": str(NOW_MS - 59_500),
                        },
                        {
                            "orderId": "9cec8c96-dc21-4c31-baf9-ec43f6184195",
                            "execId": "ong-bust-exec",
                            "execType": "BustTrade",
                            "side": "Buy",
                            "execQty": "270",
                            "execPrice": "0.09747",
                            "execFee": "0.0144743",
                            "execTime": str(NOW_MS - 59_000),
                        },
                    ]
                },
            }
        raise AssertionError((path, cursor))

    bybit._transport._request = AsyncMock(side_effect=request)
    ctx = _ctx(task, {Venue.BINANCE: binance, Venue.BYBIT: bybit})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    payload = _critical_payload(ctx, "exit.reconciled")
    assert payload is not None
    assert seen == [
        ("/v5/order/history", ""),
        ("/v5/order/history", "page-2"),
        ("/v5/execution/list", ""),
    ]
    assert payload["short_order_id"] == "9cec8c96-dc21-4c31-baf9-ec43f6184195"
    assert payload["exit_fee_quote"] == pytest.approx(0.01920902)
    assert payload["historical_evidence_resolution"]["short"]["provenance"] == (
        "exchange_takeover_execution"
    )
    assert payload["historical_evidence_resolution"]["long"] == {
        "classification": "unique_candidate_exact_recheck",
        "candidate_count": 1,
        "provenance": "system_client_id_execution",
        "order_id": "2926675711",
        "client_order_id": "lfex07006fc64ee6ed2c",
        "replaced_incomplete_known_identity": True,
    }
    assert binance_seen == [
        "/fapi/v1/order",
        "/fapi/v1/allOrders",
        "/fapi/v1/order",
        "/fapi/v1/userTrades",
    ]
    assert "v2_submitted" not in str(payload).lower()
    assert ctx.state.pending_close_reconciliations == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("classification", "candidate_count", "fee_quote"),
    [
        ("no_candidate", 0, 0.01),
        ("ambiguous_candidates", 2, 0.01),
        ("history_incomplete", 0, 0.01),
        ("unique_candidate_exact_recheck", 1, None),
    ],
)
async def test_completed_automatic_history_failures_terminalize_audit_debt_once(
    classification: str,
    candidate_count: int,
    fee_quote: float | None,
):
    snapshot = _snapshot(
        position_id=f"entry-retain-{classification}",
        symbol="COTIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        long_quantity=0.0,
        short_quantity=2400.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    candidate = None
    if candidate_count == 1:
        candidate = _fill(
            venue=Venue.BINANCE,
            symbol="COTIUSDT",
            side=Side.BUY,
            quantity=2400.0,
            price=0.010112,
            order_id="candidate",
            fee_quote=fee_quote,
        )
    result = HistoricalCloseEvidenceDiscovery(
        classification=classification,
        candidate_count=candidate_count,
        reconciliation=candidate,
    )
    ctx = _ctx(
        task,
        {
            Venue.BYBIT: _Adapter(Venue.BYBIT),
            Venue.BINANCE: _Adapter(Venue.BINANCE, discovery=result),
        },
    )

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    terminal_classification = (
        "unique_candidate_exact_recheck_incomplete"
        if candidate_count == 1
        else classification
    )
    assert ctx.state.pending_close_reconciliations == [task]
    assert task["automatic_history_terminal_status"] == "irrecoverable_audit_debt"
    assert task["automatic_history_terminal_reason"] == terminal_classification
    assert task["next_attempt_ms"] == 0
    terminal_payload = _critical_payload(
        ctx, "exit.billing_evidence_debt_irrecoverable"
    )
    assert terminal_payload is not None
    assert terminal_payload["classification"] == terminal_classification
    assert terminal_payload["reconciliation"] == task
    assert _critical_payload(ctx, "exit.reconciled") is None

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS + 600_000)

    assert ctx.state.pending_close_reconciliations == [task]
    assert _critical_payload(ctx, "exit.reconciled") is None
    assert (
        ctx.venue_adapters[Venue.BINANCE]
        .discover_historical_close_fill_reconciliation.await_count
        == 1
    )


@pytest.mark.asyncio
async def test_transient_history_query_failure_retains_retryable_debt():
    snapshot = _snapshot(
        position_id="entry-history-query-transient",
        symbol="COTIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        long_quantity=0.0,
        short_quantity=2400.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    binance = _Adapter(Venue.BINANCE)
    binance.discover_historical_close_fill_reconciliation = AsyncMock(
        side_effect=RuntimeError("temporary exchange history outage")
    )
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.BINANCE: binance})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    assert ctx.state.pending_close_reconciliations == [task]
    assert "automatic_history_terminal_status" not in task
    assert task["automatic_history_last_classification"] == "history_query_error"
    assert task["next_attempt_ms"] > NOW_MS
    assert _critical_payload(ctx, "exit.billing_evidence_debt_irrecoverable") is None


@pytest.mark.asyncio
async def test_known_exact_debt_settles_when_exact_records_arrive_without_history():
    """Known IDs stay on the exact path when delayed exchange data arrives."""
    snapshot = _snapshot(
        position_id="entry-known-exact-arrives",
        symbol="COTIUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        long_quantity=20.0,
        short_quantity=20.0,
    )
    task = _debt(
        snapshot=snapshot,
        reason="known_close_fill_temporarily_unavailable",
        long_legs=[{
            "venue": Venue.BINANCE.value,
            "order_id": "known-long",
            "client_order_id": "known-long-cid",
        }],
        short_legs=[{
            "venue": Venue.BYBIT.value,
            "order_id": "known-short",
            "client_order_id": "known-short-cid",
        }],
    )
    binance = _Adapter(
        Venue.BINANCE,
        exact={
            "known-long": _fill(
                venue=Venue.BINANCE,
                symbol="COTIUSDT",
                side=Side.SELL,
                quantity=20.0,
                price=0.0101,
                order_id="known-long",
                client_order_id="known-long-cid",
            )
        },
    )
    bybit = _Adapter(
        Venue.BYBIT,
        exact={
            "known-short": _fill(
                venue=Venue.BYBIT,
                symbol="COTIUSDT",
                side=Side.BUY,
                quantity=20.0,
                price=0.0100,
                order_id="known-short",
                client_order_id="known-short-cid",
            )
        },
    )
    ctx = _ctx(task, {Venue.BINANCE: binance, Venue.BYBIT: bybit})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    assert ctx.state.pending_close_reconciliations == []
    assert _critical_payload(ctx, "exit.reconciled") is not None
    binance.discover_historical_close_fill_reconciliation.assert_not_awaited()
    bybit.discover_historical_close_fill_reconciliation.assert_not_awaited()


@pytest.mark.asyncio
async def test_known_exact_debt_uses_unique_history_only_after_exact_recheck_is_incomplete():
    """A stale known ID may resolve only through the existing strict fallback."""
    snapshot = _snapshot(
        position_id="entry-known-exact-history-fallback",
        symbol="COTIUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        long_quantity=20.0,
        short_quantity=20.0,
    )
    task = _debt(
        snapshot=snapshot,
        reason="known_close_fill_temporarily_unavailable",
        long_legs=[{
            "venue": Venue.BINANCE.value,
            "order_id": "known-long",
            "client_order_id": "known-long-cid",
        }],
        short_legs=[{
            "venue": Venue.BYBIT.value,
            "order_id": "stale-short",
            "client_order_id": "stale-short-cid",
        }],
    )
    binance = _Adapter(
        Venue.BINANCE,
        exact={
            "known-long": _fill(
                venue=Venue.BINANCE,
                symbol="COTIUSDT",
                side=Side.SELL,
                quantity=20.0,
                price=0.0101,
                order_id="known-long",
                client_order_id="known-long-cid",
            )
        },
    )
    bybit = _Adapter(
        Venue.BYBIT,
        discovery=HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=_fill(
                venue=Venue.BYBIT,
                symbol="COTIUSDT",
                side=Side.BUY,
                quantity=20.0,
                price=0.0100,
                order_id="recovered-short",
                client_order_id="recovered-short-cid",
            ),
        ),
    )
    ctx = _ctx(task, {Venue.BINANCE: binance, Venue.BYBIT: bybit})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    payload = _critical_payload(ctx, "exit.reconciled")
    assert ctx.state.pending_close_reconciliations == []
    assert payload is not None
    assert payload["historical_evidence_resolution"] == {
        "short": {
            "classification": "unique_candidate_exact_recheck",
            "candidate_count": 1,
            "provenance": "exact_exchange_execution",
            "order_id": "recovered-short",
            "client_order_id": "recovered-short-cid",
            "replaced_incomplete_known_identity": True,
        }
    }
    binance.discover_historical_close_fill_reconciliation.assert_not_awaited()
    bybit.discover_historical_close_fill_reconciliation.assert_awaited_once()


@pytest.mark.asyncio
async def test_unique_history_candidate_rejects_mismatched_exact_order_identity():
    adapter = BinanceAdapter(
        mode="live",
        credential=LiveCredential(api_key="key", api_secret="secret"),
    )
    adapter._transport._request = AsyncMock(
        return_value=[
            {
                "symbol": "COTIUSDT",
                "side": "BUY",
                "positionSide": "SHORT",
                "reduceOnly": False,
                "status": "FILLED",
                "executedQty": "2400",
                "orderId": "history-order",
                "clientOrderId": "lfx-history-order",
                "updateTime": NOW_MS - 60_000,
            }
        ]
    )
    adapter.fetch_order_fill_reconciliation = AsyncMock(
        return_value=_fill(
            venue=Venue.BINANCE,
            symbol="COTIUSDT",
            side=Side.BUY,
            quantity=2400.0,
            price=0.010112,
            order_id="different-exact-order",
        )
    )

    result = await adapter.discover_historical_close_fill_reconciliation(
        symbol="COTIUSDT",
        side=Side.BUY,
        position_side="SHORT",
        quantity=2400.0,
        closed_at_ms=NOW_MS - 60_000,
    )

    assert result.classification == "exact_recheck_identity_mismatch"
    assert result.candidate_count == 1
    assert result.reconciliation is None


def test_binance_history_close_semantics_cover_hedge_and_one_way_modes():
    base = {
        "symbol": "COTIUSDT",
        "side": "BUY",
        "status": "FILLED",
        "executedQty": "2400",
        "orderId": "close-order",
        "updateTime": NOW_MS - 60_000,
    }
    rows = [
        {**base, "positionSide": "SHORT", "reduceOnly": False},
        {**base, "orderId": "one-way-close", "positionSide": "BOTH", "reduceOnly": True},
        {**base, "orderId": "one-way-open", "positionSide": "BOTH", "reduceOnly": False},
        {**base, "orderId": "wrong-hedge", "positionSide": "LONG", "reduceOnly": True},
    ]

    candidates = find_binance_historical_close_order_candidates(
        rows,
        symbol="COTIUSDT",
        side=Side.BUY,
        position_side="SHORT",
        quantity=2400.0,
        closed_at_ms=NOW_MS - 60_000,
    )

    assert {candidate["order_id"] for candidate in candidates} == {
        "close-order",
        "one-way-close",
    }


def test_bybit_one_way_history_requires_reduce_only():
    base = {
        "symbol": "ONGUSDT",
        "side": "Buy",
        "positionIdx": 0,
        "orderStatus": "Filled",
        "cumExecQty": "270",
        "updatedTime": NOW_MS - 60_000,
    }

    candidates = find_bybit_historical_close_order_candidates(
        [
            {**base, "orderId": "one-way-close", "reduceOnly": True},
            {**base, "orderId": "one-way-open", "reduceOnly": False},
        ],
        symbol="ONGUSDT",
        side=Side.BUY,
        position_side="SHORT",
        quantity=270.0,
        closed_at_ms=NOW_MS - 60_000,
    )

    assert [candidate["order_id"] for candidate in candidates] == ["one-way-close"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("position_quantity", "open_orders"),
    [(1.0, []), (0.0, [{"orderId": "still-open"}])],
)
async def test_nonflat_or_open_order_truth_blocks_history_discovery(
    position_quantity: float,
    open_orders: list[dict],
):
    snapshot = _snapshot(
        position_id="entry-live-truth-block",
        symbol="COTIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        long_quantity=0.0,
        short_quantity=2400.0,
    )
    task = _debt(snapshot=snapshot, long_legs=[], short_legs=[])
    binance = _Adapter(
        Venue.BINANCE,
        discovery=HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=_fill(
                venue=Venue.BINANCE,
                symbol="COTIUSDT",
                side=Side.BUY,
                quantity=2400.0,
                price=0.010112,
                order_id="candidate",
            ),
        ),
        position_quantity=position_quantity,
        open_orders=open_orders,
    )
    ctx = _ctx(task, {Venue.BYBIT: _Adapter(Venue.BYBIT), Venue.BINANCE: binance})

    await CloseRuntime(ctx)._process_pending_close_reconciliations(NOW_MS)

    binance.discover_historical_close_fill_reconciliation.assert_not_awaited()
    assert ctx.state.pending_close_reconciliations == [task]
    assert task["next_attempt_ms"] > NOW_MS
