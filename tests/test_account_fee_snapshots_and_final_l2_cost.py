"""Regression coverage for slow account fees and post-L2 final costing."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from lightfee.config.schema import (
    AppConfig,
    PersistenceConfig,
    RuntimeConfig,
    StrategyConfig,
    VenueConfig,
)
from lightfee.core.domain import AccountFeeSnapshot, Side, Venue
from lightfee.engine.entry_readiness import EntryReadinessDecision
from lightfee.engine.runtime import LiveRuntime
from lightfee.marketdata.l2 import L2BookStatus, PriceLevel
from lightfee.sidecar.snapshot import CandidateInput, QuoteSnapshot, SidecarSnapshot
from lightfee.strategy.scoring import price_final_l2_cost
from lightfee.venues.aster import AsterAdapter
from lightfee.venues.binance import BinanceAdapter
from lightfee.venues.bitget import BitgetAdapter
from lightfee.venues.bybit import BybitAdapter
from lightfee.venues.gate import GateAdapter
from lightfee.venues.hyperliquid import HyperliquidAdapter
from lightfee.venues.okx import OkxAdapter
from lightfee.venues.transport import TransportError, TransportErrorCategory
from tests.fake_adapters import FakeVenueAdapter


class _FeeTransport:
    def __init__(self, responses: list[dict]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, str, dict | None, dict | None, bool]] = []
        self.mode = "paper"
        self._symbol_metadata: dict = {}

    def _venue_symbol(self, symbol: str) -> str:
        return "BTC-USDT-SWAP" if symbol == "BTCUSDT" else symbol

    async def _request(self, method, path, *, params=None, body=None, private=False):
        self.calls.append((method, path, params, body, private))
        return self._responses.pop(0)

    async def close(self) -> None:
        return None


def _adapter_for_fee_response(venue: Venue, response: dict):
    if venue == Venue.BINANCE:
        adapter = BinanceAdapter(mode="paper")
    elif venue == Venue.OKX:
        adapter = OkxAdapter(mode="paper")
    elif venue == Venue.BYBIT:
        adapter = BybitAdapter(mode="paper")
    elif venue == Venue.BITGET:
        adapter = BitgetAdapter(mode="paper")
    elif venue == Venue.GATE:
        adapter = GateAdapter(mode="paper")
    elif venue == Venue.ASTER:
        adapter = AsterAdapter(mode="paper")
    elif venue == Venue.HYPERLIQUID:
        adapter = HyperliquidAdapter(mode="paper")
        adapter._credential = SimpleNamespace(account_address="0xfee")
    else:  # pragma: no cover - keeps the parameter matrix exhaustive.
        raise AssertionError(f"unexpected venue: {venue}")
    transport = _FeeTransport([response])
    adapter._transport = transport
    if venue == Venue.ASTER:
        # Aster's V2 private contract is API-wallet V3, not legacy FAPI HMAC.
        adapter._private = transport
    return adapter, transport


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "venue",
        "response",
        "path",
        "private",
        "expected_maker_bps",
        "expected_taker_bps",
    ),
    [
        (
            Venue.BINANCE,
            {"makerCommissionRate": "-0.00002", "takerCommissionRate": "0.0004"},
            "/fapi/v1/commissionRate",
            True,
            -0.2,
            4.0,
        ),
        (
            Venue.OKX,
            {"code": "0", "data": [{"maker": "-0.00002", "taker": "-0.0005"}]},
            "/api/v5/account/trade-fee",
            True,
            0.2,
            5.0,
        ),
        (
            Venue.BYBIT,
            {
                "retCode": 0,
                "result": {"list": [{"makerFeeRate": "-0.00002", "takerFeeRate": "0.0006"}]},
            },
            "/v5/account/fee-rate",
            True,
            -0.2,
            6.0,
        ),
        (
            Venue.BITGET,
            {"code": "00000", "data": {"makerFeeRate": "-0.00002", "takerFeeRate": "0.0006"}},
            "/api/v3/account/fee-rate",
            True,
            -0.2,
            6.0,
        ),
        (
            Venue.GATE,
            {"futures_maker_fee": "-0.00002", "futures_taker_fee": "0.0006"},
            "/api/v4/wallet/fee",
            True,
            -0.2,
            6.0,
        ),
        (
            Venue.ASTER,
            {"makerCommissionRate": "-0.00002", "takerCommissionRate": "0.0004"},
            "/fapi/v3/commissionRate",
            False,
            -0.2,
            4.0,
        ),
        (
            Venue.HYPERLIQUID,
            {"userAddRate": "-0.00002", "userCrossRate": "0.0004"},
            "/info",
            False,
            -0.2,
            4.0,
        ),
    ],
)
async def test_each_venue_parses_its_account_maker_taker_fee_schedule(
    venue, response, path, private, expected_maker_bps, expected_taker_bps
):
    adapter, transport = _adapter_for_fee_response(venue, response)

    snapshot = await adapter.fetch_account_fee_snapshot("BTCUSDT")

    assert snapshot is not None
    assert snapshot.venue == venue
    assert snapshot.maker_fee_bps == pytest.approx(expected_maker_bps)
    assert snapshot.taker_fee_bps == pytest.approx(expected_taker_bps)
    assert transport.calls[0][1] == path
    assert transport.calls[0][4] is private


@pytest.mark.asyncio
async def test_okx_positive_account_rates_are_rebates_in_final_cost_convention():
    adapter, _ = _adapter_for_fee_response(
        Venue.OKX,
        {"code": "0", "data": [{"maker": "0.00002", "taker": "0.0005"}]},
    )

    snapshot = await adapter.fetch_account_fee_snapshot("BTCUSDT")

    assert snapshot is not None
    assert snapshot.maker_fee_bps == pytest.approx(-0.2)
    assert snapshot.taker_fee_bps == pytest.approx(-5.0)


@pytest.mark.asyncio
async def test_bitget_classic_account_error_retries_the_classic_fee_endpoint():
    adapter, transport = _adapter_for_fee_response(
        Venue.BITGET,
        {"code": "40084", "msg": "classic account is not supported by this endpoint"},
    )
    transport._responses.append(
        {"code": "00000", "data": {"makerFeeRate": "-0.00001", "takerFeeRate": "0.0005"}}
    )

    snapshot = await adapter.fetch_account_fee_snapshot("BTCUSDT")

    assert snapshot is not None
    assert snapshot.maker_fee_bps == pytest.approx(-0.1)
    assert snapshot.taker_fee_bps == pytest.approx(5.0)
    assert [call[1] for call in transport.calls] == [
        "/api/v3/account/fee-rate",
        "/api/v2/common/trade-rate",
    ]


@pytest.mark.asyncio
async def test_bitget_classic_transport_error_retries_the_classic_fee_endpoint():
    adapter, transport = _adapter_for_fee_response(
        Venue.BITGET,
        {"code": "00000", "data": {"makerFeeRate": "-0.00001", "takerFeeRate": "0.0005"}},
    )
    original_request = transport._request

    async def classic_uta_rejection(method, path, *, params=None, body=None, private=False):
        if path == "/api/v3/account/fee-rate":
            transport.calls.append((method, path, params, body, private))
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "classic account is not supported by this endpoint",
                status_code=400,
                body='{"code":"40084","msg":"classic account is not supported by this endpoint"}',
            )
        return await original_request(method, path, params=params, body=body, private=private)

    transport._request = classic_uta_rejection

    snapshot = await adapter.fetch_account_fee_snapshot("BTCUSDT")

    assert snapshot is not None
    assert snapshot.maker_fee_bps == pytest.approx(-0.1)
    assert snapshot.taker_fee_bps == pytest.approx(5.0)
    assert [call[1] for call in transport.calls] == [
        "/api/v3/account/fee-rate",
        "/api/v2/common/trade-rate",
    ]


class _AccountFeeAdapter(FakeVenueAdapter):
    def __init__(
        self,
        venue: Venue,
        result: AccountFeeSnapshot | Exception,
        *,
        supported: list[str] | None = None,
    ) -> None:
        super().__init__(venue)
        self._result = result
        self._supported = supported if supported is not None else ["BTCUSDT"]
        self.reference_symbols: list[str] = []

    def supported_symbols(self) -> list[str]:
        return list(self._supported)

    async def fetch_account_fee_snapshot(self, reference_symbol: str = ""):
        self.reference_symbols.append(reference_symbol)
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


def _live_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            local_l2_enabled=True,
            local_l2_ws_enabled=True,
            min_expected_edge_bps=1.0,
            min_worst_case_edge_bps=-100.0,
            entry_exit_reserve_bps=0.0,
            capital_buffer_bps=0.0,
            execution_buffer_bps=0.0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
        venues=[
            VenueConfig(venue="binance", maker_fee_bps=-0.1, taker_fee_bps=4.0),
            VenueConfig(venue="okx", maker_fee_bps=-0.1, taker_fee_bps=5.0),
        ],
    )


class _LiveOkxFeeAdapter(OkxAdapter):
    """Real OKX fee parser with flat live-recovery responses for startup coverage."""

    async def ensure_supported_symbols_loaded(self) -> None:
        return None

    async def fetch_position(self, symbol: str):
        return SimpleNamespace(
            venue=Venue.OKX,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1000,
        )


@pytest.mark.asyncio
async def test_fee_refresh_persists_success_and_keeps_cache_then_config_on_failure(tmp_path):
    config = _live_config(tmp_path)
    seed = LiveRuntime(config, venue_adapters={})
    seed.state.account_fee_snapshots[Venue.OKX.value] = {
        "venue": "okx",
        "maker_fee_bps": -0.4,
        "taker_fee_bps": 4.4,
        "observed_at_ms": 1111,
        "source": "persisted",
    }
    seed.snapshot_store.write(seed.state.to_dict())
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BINANCE: _AccountFeeAdapter(
                Venue.BINANCE,
                AccountFeeSnapshot(Venue.BINANCE, -0.3, 3.2, 1234, "account"),
            ),
            Venue.OKX: _AccountFeeAdapter(Venue.OKX, RuntimeError("private endpoint down")),
        },
    )
    await runtime.start()

    assert runtime._account_fee_snapshot_for_venue(Venue.BINANCE).source == "account"
    assert runtime._account_fee_snapshot_for_venue(Venue.OKX).source == "persisted"
    persisted = runtime.snapshot_store.read()
    assert persisted is not None
    assert persisted["account_fee_snapshots"]["binance"]["taker_fee_bps"] == 3.2
    runtime.state.account_fee_snapshots.clear()
    fallback = runtime._account_fee_snapshot_for_venue(Venue.OKX)
    assert fallback.source == "config_fee_fallback"
    assert fallback.maker_fee_bps == pytest.approx(-0.1)
    assert fallback.taker_fee_bps == pytest.approx(5.0)
    assert any(
        record["kind"] == "runtime.account_fee_snapshot_refresh_unavailable"
        and record["payload"]["venue"] == "okx"
        for record in runtime.journal.read_all()
    )
    assert runtime.state.lifecycle.value == "running"
    await runtime.stop()


@pytest.mark.asyncio
async def test_live_startup_persists_okx_commission_as_positive_final_l2_cost(tmp_path):
    adapter = _LiveOkxFeeAdapter(mode="paper")
    adapter._transport = _FeeTransport(
        [{"code": "0", "data": [{"maker": "-0.0002", "taker": "-0.0005"}]}]
    )
    adapter._transport._symbol_metadata = {
        "BTC-USDT-SWAP": {"ctType": "linear", "ctVal": "1"}
    }
    runtime = LiveRuntime(_live_config(tmp_path), venue_adapters={Venue.OKX: adapter})

    await runtime.start()
    try:
        persisted = runtime.snapshot_store.read()
        assert persisted is not None
        assert persisted["account_fee_snapshots"]["okx"]["maker_fee_bps"] == 2.0
        assert persisted["account_fee_snapshots"]["okx"]["taker_fee_bps"] == 5.0

        now_ms = 10_000
        runtime.state.last_scan = {}
        _install_hot_book(runtime, "binance", "BTCUSDT", 100.0, 100.0, now_ms)
        _install_hot_book(runtime, "okx", "BTCUSDT", 100.0, 100.0, now_ms)
        candidate = _candidate("BTCUSDT", funding_edge_bps=5.0)

        assert runtime._reprice_final_l2_candidates([candidate], now_ms) == []
        assert "final_l2_expected_edge_below_floor" in candidate.blocked_reasons

        dual_taker_candidate = _candidate("BTCUSDT", funding_edge_bps=5.0)
        assert (
            runtime._reprice_final_l2_candidate(
                dual_taker_candidate,
                now_ms,
                dual_taker=True,
            )
            is False
        )
        assert "final_l2_expected_edge_below_floor" in dual_taker_candidate.blocked_reasons
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_fee_refresh_failure_without_cache_uses_config_and_does_not_block_startup(tmp_path):
    config = _live_config(tmp_path)
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BINANCE: _AccountFeeAdapter(
                Venue.BINANCE,
                RuntimeError("private endpoint down"),
            ),
        },
    )

    await runtime.start()

    fallback = runtime._account_fee_snapshot_for_venue(Venue.BINANCE)
    assert fallback.source == "config_fee_fallback"
    assert fallback.maker_fee_bps == pytest.approx(-0.1)
    assert fallback.taker_fee_bps == pytest.approx(4.0)
    assert runtime.state.lifecycle.value == "running"
    await runtime.stop()


@pytest.mark.asyncio
async def test_fee_refresh_uses_a_manual_symbol_supported_by_each_venue(tmp_path):
    config = _live_config(tmp_path)
    config.symbols = ["BTCUSDT", "ETHUSDT"]
    binance = _AccountFeeAdapter(
        Venue.BINANCE,
        AccountFeeSnapshot(Venue.BINANCE, -0.3, 3.2, 1234, "account"),
        supported=["BTCUSDT"],
    )
    okx = _AccountFeeAdapter(
        Venue.OKX,
        AccountFeeSnapshot(Venue.OKX, -0.2, 4.2, 1234, "account"),
        supported=["ETHUSDT"],
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
    )

    runtime.journal.open()
    try:
        await runtime._refresh_account_fee_snapshots()
    finally:
        runtime.journal.close()

    assert binance.reference_symbols == ["BTCUSDT"]
    assert okx.reference_symbols == ["ETHUSDT"]


@pytest.mark.asyncio
async def test_fee_refresh_skips_private_query_when_known_catalog_has_no_manual_symbol(tmp_path):
    config = _live_config(tmp_path)
    adapter = _AccountFeeAdapter(
        Venue.BINANCE,
        AccountFeeSnapshot(Venue.BINANCE, -0.3, 3.2, 1234, "account"),
        supported=["ETHUSDT"],
    )
    runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: adapter})

    runtime.journal.open()
    try:
        await runtime._refresh_account_fee_snapshots()
    finally:
        runtime.journal.close()

    assert adapter.reference_symbols == []
    assert runtime._account_fee_snapshot_for_venue(Venue.BINANCE).source == "config_fee_fallback"


def test_config_fee_fallback_uses_taker_rate_when_maker_rate_is_omitted(tmp_path):
    config = _live_config(tmp_path)
    config.venues[0].maker_fee_bps = None
    runtime = LiveRuntime(config, venue_adapters={})

    fallback = runtime._account_fee_snapshot_for_venue(Venue.BINANCE)

    assert fallback.source == "config_fee_fallback"
    assert fallback.maker_fee_bps == pytest.approx(fallback.taker_fee_bps)


def test_contract_l2_quantity_scales_use_cached_instrument_metadata():
    okx = OkxAdapter(mode="paper")
    okx._transport._symbol_metadata = {
        "BTC-USDT-SWAP": {"ctType": "linear", "ctVal": "0.01"}
    }
    gate = GateAdapter(mode="paper")
    gate._transport._symbol_metadata = {
        "BTC_USDT": {"quanto_multiplier": "0.001"}
    }

    assert okx.l2_book_quantity_to_base_scale("BTCUSDT") == pytest.approx(0.01)
    assert gate.l2_book_quantity_to_base_scale("BTCUSDT") == pytest.approx(0.001)


def test_final_l2_cost_compares_both_entry_maker_routes_with_role_aware_fees():
    config = StrategyConfig(
        entry_exit_reserve_bps=0.0,
        capital_buffer_bps=0.0,
    )

    priced = price_final_l2_cost(
        funding_edge_bps=5.0,
        long_bid=99.0,
        long_ask=100.0,
        short_bid=101.0,
        short_ask=102.0,
        long_buy_slippage_bps=1.0,
        long_sell_slippage_bps=2.0,
        short_buy_slippage_bps=1.0,
        short_sell_slippage_bps=1.0,
        long_maker_fee_bps=-0.5,
        long_taker_fee_bps=5.0,
        short_maker_fee_bps=4.0,
        short_taker_fee_bps=8.0,
        config=config,
        tie_maker_leg="short",
    )

    assert priced.entry_maker_leg == "long"
    assert priced.entry_fee_bps == pytest.approx(7.5)
    assert priced.exit_maker_leg == "long"
    assert priced.exit_fee_bps == pytest.approx(7.5)
    assert priced.long_entry_slippage_bps == 0.0
    assert priced.short_entry_slippage_bps == pytest.approx(1.0)


def test_final_l2_cost_keeps_v1_exit_route_selection_independent_of_fee_rebates():
    priced = price_final_l2_cost(
        funding_edge_bps=20.0,
        long_bid=99.0,
        long_ask=100.0,
        short_bid=101.0,
        short_ask=102.0,
        long_buy_slippage_bps=1.0,
        long_sell_slippage_bps=3.0,
        short_buy_slippage_bps=1.0,
        short_sell_slippage_bps=1.0,
        long_maker_fee_bps=100.0,
        long_taker_fee_bps=10.0,
        short_maker_fee_bps=-5.0,
        short_taker_fee_bps=10.0,
        config=StrategyConfig(entry_exit_reserve_bps=0.0, capital_buffer_bps=0.0),
        tie_maker_leg="short",
    )

    # The short maker route leaves the long sell as taker at 3 bps, while the
    # long maker route leaves the short buy at 1 bps.  V1 keeps long maker on
    # exit although the short-maker fee is much cheaper: fee is deliberately
    # irrelevant to this route-selection rule.
    assert priced.exit_maker_leg == "long"
    assert priced.exit_slippage_bps == pytest.approx(1.0)


def test_final_l2_cost_uses_configured_maker_leg_only_for_exact_entry_ties():
    priced = price_final_l2_cost(
        funding_edge_bps=20.0,
        long_bid=99.0,
        long_ask=100.0,
        short_bid=101.0,
        short_ask=102.0,
        long_buy_slippage_bps=1.0,
        long_sell_slippage_bps=1.0,
        short_buy_slippage_bps=1.0,
        short_sell_slippage_bps=1.0,
        long_maker_fee_bps=1.0,
        long_taker_fee_bps=2.0,
        short_maker_fee_bps=1.0,
        short_taker_fee_bps=2.0,
        config=StrategyConfig(entry_exit_reserve_bps=0.0, capital_buffer_bps=0.0),
        tie_maker_leg="short",
    )

    assert priced.entry_maker_leg == "short"


def test_final_l2_reprice_uses_equal_base_quantity_vwap_not_quote_weighted_price(tmp_path):
    runtime = LiveRuntime(_live_config(tmp_path), venue_adapters={})
    book = runtime.local_l2_runtime.ensure_book("binance", "BTCUSDT")
    book.asks = [
        PriceLevel(price=100.0, quantity=1.0),
        PriceLevel(price=200.0, quantity=1.0),
    ]

    slippage_bps, vwap = runtime._final_l2_taker_slippage_bps(book, Side.BUY, 2.0)

    assert vwap == pytest.approx(150.0)
    assert slippage_bps == pytest.approx(5_000.0)


def _install_hot_book(runtime, venue: str, symbol: str, bid: float, ask: float, now_ms: int):
    book = runtime.local_l2_runtime.ensure_book(venue, symbol)
    book.status = L2BookStatus.HOT
    book.bids = [PriceLevel(price=bid, quantity=100.0)]
    book.asks = [PriceLevel(price=ask, quantity=100.0)]
    book.observed_at_ms = now_ms


class _L2QuantityScaleAdapter(FakeVenueAdapter):
    def __init__(self, venue: Venue, scale: float | None) -> None:
        super().__init__(venue)
        self._scale = scale

    def l2_book_quantity_to_base_scale(self, symbol: str) -> float | None:
        del symbol
        return self._scale


def _candidate(symbol: str, funding_edge_bps: float) -> CandidateInput:
    return CandidateInput(
        long_venue="binance",
        short_venue="okx",
        symbol=symbol,
        funding_diff_bps=funding_edge_bps,
        funding_edge_bps=funding_edge_bps,
        expected_edge_bps=99.0,
        worst_case_edge_bps=99.0,
        ranking_edge_bps=99.0,
        entry_notional_quote=50.0,
    )


def test_final_l2_reprice_filters_cost_failures_and_reranks_live_candidates(tmp_path):
    runtime = LiveRuntime(_live_config(tmp_path), venue_adapters={})
    runtime.state.last_scan = {}
    now_ms = 10_000
    for symbol in ("GOODUSDT", "BADUSDT"):
        _install_hot_book(runtime, "binance", symbol, 99.9, 100.0, now_ms)
        _install_hot_book(runtime, "okx", symbol, 100.1, 100.2, now_ms)
    good = _candidate("GOODUSDT", 5.0)
    lower_initial_rank_but_better_final = _candidate("SECONDUSDT", 6.0)
    good.ranking_edge_bps = 20.0
    lower_initial_rank_but_better_final.ranking_edge_bps = 10.0
    bad = _candidate("BADUSDT", -100.0)

    _install_hot_book(runtime, "binance", "SECONDUSDT", 99.9, 100.0, now_ms)
    _install_hot_book(runtime, "okx", "SECONDUSDT", 100.1, 100.2, now_ms)

    repriced = runtime._reprice_final_l2_candidates(
        [good, lower_initial_rank_but_better_final, bad], now_ms
    )

    assert repriced == [lower_initial_rank_but_better_final, good]
    assert good.entry_liquidity_source_at_entry == "true_l2"
    assert good.entry_maker_leg in {"long", "short"}
    assert "final_l2_expected_edge_below_floor" in bad.blocked_reasons


def test_final_l2_reprice_is_applied_in_the_final_selection_path(tmp_path):
    class ReadyCandidate:
        def decide(self, candidate, now_ms, *, market_quotes=None):
            return EntryReadinessDecision.allow()

    runtime = LiveRuntime(_live_config(tmp_path), venue_adapters={})
    runtime.entry_readiness_provider = ReadyCandidate()
    runtime.journal.open()
    try:
        runtime.state.last_scan = {}
        now_ms = 10_000
        for symbol in ("GOODUSDT", "SECONDUSDT", "BADUSDT"):
            _install_hot_book(runtime, "binance", symbol, 99.9, 100.0, now_ms)
            _install_hot_book(runtime, "okx", symbol, 100.1, 100.2, now_ms)
        good = _candidate("GOODUSDT", 5.0)
        second = _candidate("SECONDUSDT", 6.0)
        bad = _candidate("BADUSDT", -100.0)
        for candidate in (good, second, bad):
            candidate.first_funding_timestamp_ms = now_ms + 300_000
            candidate.funding_timestamp_ms = now_ms + 300_000
        # Lightweight ranking prefers GOOD before L2 is available; final L2
        # cost must instead rank SECOND first and remove BAD.
        good.ranking_edge_bps = 20.0
        second.ranking_edge_bps = 10.0

        selected = runtime._select_entry_candidates(
            [good, second, bad],
            now_ms=now_ms,
            remaining_slots=1,
            selection_blocker_counts=Counter(),
            candidate_blockers={},
            final_cost_reprice=runtime._reprice_final_l2_candidates,
        )
    finally:
        runtime.journal.close()

    assert selected == [second, good]
    assert runtime.state.last_scan["final_l2_repriced_candidate_count"] == 2
    assert runtime.state.last_scan["final_l2_filtered_candidate_count"] == 1
    assert "final_l2_expected_edge_below_floor" in bad.blocked_reasons


@pytest.mark.asyncio
async def test_ws_bbo_tick_activates_candidate_l2_then_reprices_before_dispatch(
    tmp_path, monkeypatch,
):
    """BBO readiness does not suppress the V1 candidate-L2 cost path."""
    from lightfee.engine.entry import EntryState
    from lightfee.engine.entry_sync import EntryExecutionResult
    from lightfee.engine.execution_planner import ExecutionRoute
    from lightfee.marketdata.ws_bbo import TopBookQuote
    from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

    class CapturingExecutor:
        def __init__(self) -> None:
            self.contexts = []

        async def execute(self, ctx):
            self.contexts.append(ctx)
            return EntryExecutionResult(
                route=ExecutionRoute.PASSIVE_INCREMENTAL,
                state=EntryState.COMPLETED,
            )

    now_ms = 10_000
    config = _live_config(tmp_path)
    config.runtime.live_scan_recovery_success_count = 1
    config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
    config.strategy.local_l2_ws_enabled = False
    config.strategy.entry_window_secs = 480
    config.strategy.min_scan_minutes_before_funding = 0
    adapters = {
        Venue.BINANCE: _AccountFeeAdapter(
            Venue.BINANCE,
            AccountFeeSnapshot(Venue.BINANCE, -0.1, 4.0, now_ms, "test"),
        ),
        Venue.OKX: _AccountFeeAdapter(
            Venue.OKX,
            AccountFeeSnapshot(Venue.OKX, -0.1, 5.0, now_ms, "test"),
        ),
    }
    runtime = LiveRuntime(config, venue_adapters=adapters)
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingExecutor()
    runtime.entry_executor = executor
    candidate = _candidate("BTCUSDT", 15.0)
    candidate.first_funding_timestamp_ms = now_ms + 300_000
    candidate.funding_timestamp_ms = now_ms + 300_000
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT", bid=99.9, ask=100.0,
                bid_size=100.0, ask_size=100.0, observed_at_ms=now_ms,
                volume_24h_quote=10_000_000.0, open_interest=2_000_000.0,
            ),
            "okx:BTCUSDT": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT", bid=100.1, ask=100.2,
                bid_size=100.0, ask_size=100.0, observed_at_ms=now_ms,
                volume_24h_quote=10_000_000.0, open_interest=2_000_000.0,
            ),
        },
        candidates=[candidate],
    )
    for venue, bid, ask in (
        ("binance", 99.9, 100.0),
        ("okx", 100.1, 100.2),
    ):
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=venue, symbol="BTCUSDT", bid=bid, ask=ask,
                bid_size=100.0, ask_size=100.0, observed_at_ms=now_ms,
                received_at_ms=now_ms, source=f"{venue}_bbo_ws",
            )
        )

    async def keep_existing_bbo_quotes(candidates, prewarm_now_ms):
        del candidates, prewarm_now_ms
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("binance", "BTCUSDT"), ("okx", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()
        runtime._entry_bbo_subscription_per_venue_budget = 1

    bootstrap_calls = []

    def bootstrap_hot_l2(*, venue, symbols, **_kwargs):
        bootstrap_calls.append((venue, tuple(symbols)))
        for symbol in symbols:
            _install_hot_book(
                runtime,
                venue,
                symbol,
                99.9 if venue == "binance" else 100.1,
                100.0 if venue == "binance" else 100.2,
                now_ms,
            )

    async def skip_external_l2_sync(**_kwargs):
        return 0

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(
        runtime,
        "_ensure_entry_bbo_active_for_candidates",
        keep_existing_bbo_quotes,
    )
    monkeypatch.setattr(
        runtime.l2_data_plane,
        "start_background_bootstrap",
        bootstrap_hot_l2,
    )
    monkeypatch.setattr(runtime.l2_data_plane, "sync_snapshots", skip_external_l2_sync)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert runtime._local_l2_effective_enabled() is False
    assert bootstrap_calls == [
        ("binance", ("BTCUSDT",)),
        ("okx", ("BTCUSDT",)),
    ]
    assert runtime.state.last_scan["final_l2_repriced_candidate_count"] == 1
    assert runtime.state.last_scan["final_l2_filtered_candidate_count"] == 0
    assert candidate.entry_liquidity_source_at_entry == "true_l2"
    assert len(executor.contexts) == 1


@pytest.mark.parametrize(
    ("okx_scale", "expected_reason"),
    [
        (0.01, "final_l2_insufficient_depth"),
        (None, "final_l2_quantity_scale_unavailable"),
    ],
)
def test_final_l2_reprice_uses_venue_base_quantity_scale_and_fails_closed(
    tmp_path, okx_scale, expected_reason
):
    runtime = LiveRuntime(
        _live_config(tmp_path),
        venue_adapters={
            Venue.BINANCE: _L2QuantityScaleAdapter(Venue.BINANCE, 1.0),
            Venue.OKX: _L2QuantityScaleAdapter(Venue.OKX, okx_scale),
        },
    )
    runtime.state.last_scan = {}
    now_ms = 10_000
    _install_hot_book(runtime, "binance", "BTCUSDT", 99.9, 100.0, now_ms)
    # One raw OKX contract is only 0.01 BTC, while the $50 candidate needs
    # roughly 0.5 BTC at this price.
    _install_hot_book(runtime, "okx", "BTCUSDT", 100.1, 100.2, now_ms)
    for side in ("bids", "asks"):
        setattr(
            runtime.local_l2_runtime.get_book("okx", "BTCUSDT"),
            side,
            [PriceLevel(price=100.1 if side == "bids" else 100.2, quantity=1.0)],
        )
    candidate = _candidate("BTCUSDT", 10.0)

    repriced = runtime._reprice_final_l2_candidates([candidate], now_ms)

    assert repriced == []
    assert expected_reason in candidate.blocked_reasons


@pytest.mark.asyncio
async def test_live_start_keeps_v1_recovery_available_without_local_l2(tmp_path):
    config = _live_config(tmp_path)
    config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
    runtime = LiveRuntime(config, venue_adapters={})

    await runtime.start()
    assert runtime.state.lifecycle.value == "running"
    await runtime.stop()
