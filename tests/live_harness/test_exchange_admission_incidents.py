from __future__ import annotations

import tempfile
from types import SimpleNamespace

import pytest

from lightfee.engine.entry import EntryState
from lightfee.engine.entry_sync import EntryExecutionResult
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import PendingEntry
from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from tests.test_live_startup_preflight import make_test_config


pytestmark = pytest.mark.live_harness


def _candidate(symbol: str, long_venue: str, short_venue: str) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        entry_notional_quote=50.0,
        ranking_edge_bps=10.0,
        expected_edge_bps=10.0,
        funding_edge_bps=0.0,
        worst_case_edge_bps=8.0,
        blocked=False,
        blocked_reasons=[],
    )


class RejectingExecutor:
    def __init__(self, reject_reason: str):
        self.reject_reason = reject_reason
        self.calls = 0

    async def execute(self, ctx):
        self.calls += 1
        return EntryExecutionResult(
            route=ExecutionRoute.REJECTED,
            state=EntryState.FAILED,
            reject_reason=self.reject_reason,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "venue",
        "symbol",
        "raw_error",
        "expected_reason",
        "official_doc_url",
        "evidence_gap",
    ),
    [
        (
            "bybit",
            "BALUSDT",
            "bybit retCode=110007 retMsg=Available balance is insufficient",
            "insufficient_balance_admission_blocked",
            "https://bybit-exchange.github.io/docs/v5/error",
            False,
        ),
        (
            "bybit",
            "LITEUSDT",
            "bybit retCode=110126 retMsg=must sign required agreement",
            "bybit_trading_terms_required",
            "https://bybit-exchange.github.io/docs/v5/error",
            False,
        ),
        (
            "binance",
            "MARGINUSDT",
            'HTTP 400: {"code":-2019,"msg":"Margin is insufficient."}',
            "insufficient_margin_admission_blocked",
            "https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code",
            False,
        ),
        (
            "binance",
            "GTXUSDT",
            (
                'HTTP 400: {"code":-5022,"msg":"Due to the order could not be '
                'executed as maker, the Post Only order will be rejected."}'
            ),
            "post_only_would_take",
            "https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code",
            False,
        ),
        (
            "aster",
            "ESPORTSUSDT",
            'HTTP 400: {"code":-2027,"msg":"Exceeded the maximum allowable position at current leverage."}',
            "leverage_admission_blocked",
            "https://docs.asterdex.com/product/aster-perpetuals/api/api-documentation",
            False,
        ),
        (
            "aster",
            "MAXUSDT",
            'HTTP 400: {"code":-5018,"msg":"maximum notional value limit"}',
            "max_notional_admission_blocked",
            "https://asterdex.github.io/aster-api-website/futures/account%26trades/#remaining-openable-notional-value-user_data",
            False,
        ),
    ],
)
async def test_exchange_rule_rejects_create_admission_blocks_with_evidence_payload(
    venue: str,
    symbol: str,
    raw_error: str,
    expected_reason: str,
    official_doc_url: str,
    evidence_gap: bool,
):
    with tempfile.TemporaryDirectory() as td:
        runtime = LiveRuntime(make_test_config(td))
        runtime.journal.open()
        executor = RejectingExecutor(raw_error)
        runtime.entry_executor = executor

        other_venue = "bybit" if venue != "bybit" else "binance"
        candidate = _candidate(symbol, venue, other_venue)

        first = await runtime._dispatch_entry(candidate, 1778787000000, price_hint=1.0)
        second = await runtime._dispatch_entry(candidate, 1778787001000, price_hint=1.0)

        assert first is True
        assert second is False
        assert executor.calls == 1

        records = runtime.journal.read_all()
        if expected_reason == "post_only_would_take":
            assert f"{venue}:{symbol}" not in runtime.state.venue_entry_cooldowns
            event_payload = [
                record["payload"]
                for record in records
                if record["kind"] == "runtime.entry_post_only_reject_cooldown"
                and record["payload"].get("venue") == venue
                and record["payload"].get("symbol") == symbol
            ][-1]
        else:
            state_key = f"{venue}:{symbol}"
            state_payload = runtime.state.venue_entry_cooldowns[state_key]
            assert state_payload["venue"] == venue
            assert state_payload["symbol"] == symbol
            assert state_payload["reason"] == expected_reason
            assert state_payload["raw_error"] == raw_error[:500]
            assert state_payload["blocked_until_ms"] > 1778787000000
            assert state_payload["official_doc_url"] == official_doc_url
            assert state_payload["evidence_gap"] is evidence_gap
            event_payload = [
                record["payload"]
                for record in records
                if record["kind"] == "runtime.entry_admission_blocked"
                and record["payload"].get("venue") == venue
                and record["payload"].get("symbol") == symbol
            ][-1]
            assert event_payload["blocked_until_ms"] == state_payload["blocked_until_ms"]

        assert event_payload["reason"] == expected_reason
        assert event_payload["raw_error"] == raw_error[:500]
        assert event_payload["blocked_until_ms"] > 1778787000000
        assert event_payload["official_doc_url"] == official_doc_url
        assert event_payload["evidence_gap"] is evidence_gap

        runtime.journal.close()


class FlatAdapter:
    async def fetch_position(self, symbol: str):
        return None

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity


class RejectingHedgeAdapter(FlatAdapter):
    def __init__(self, message: str):
        self.message = message
        self.place_order_calls = 0

    async def place_order(self, request):
        self.place_order_calls += 1
        raise OrderSubmitError(SubmitFailureClass.REJECTED, self.message)

    async def fetch_order_fill_reconciliation(self, symbol: str, order_id: str, client_order_id: str):
        return PositionSnapshot(
            venue=Venue.BYBIT,
            symbol=symbol,
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=0,
        )


def _pending_for_hedge_reject(
    *,
    entry_id: str,
    symbol: str,
    long_venue: Venue,
    short_venue: Venue,
    maker_leg: str,
) -> PendingEntry:
    return PendingEntry(
        pending_id=entry_id,
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        target_quantity=4.0,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=1778787000000,
        maker_leg=maker_leg,
        maker_leg_filled=4.0,
        hedge_leg_filled=0.0,
        maker_price=1.0,
        maker_fill_price=1.0,
    )


@pytest.mark.asyncio
async def test_pending_hedge_bybit_trading_terms_reject_aborts_without_retry():
    with tempfile.TemporaryDirectory() as td:
        bybit = RejectingHedgeAdapter(
            "bybit retCode=110126 retMsg=must sign required agreement"
        )
        runtime = LiveRuntime(
            make_test_config(td),
            venue_adapters={Venue.ASTER: FlatAdapter(), Venue.BYBIT: bybit},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-bz",
            symbol="BZUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
            maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._drive_missing_hedge_live(
            pending, pending.pending_id, 1778787001000
        )

        assert driven is False
        assert bybit.place_order_calls == 1
        assert pending.pending_id not in runtime.state.pending_entries
        assert pending.repair_state == "hedge_admission_blocked:bybit_trading_terms_required"
        assert runtime.state.venue_entry_cooldowns["bybit:BZUSDT"]["reason"] == (
            "bybit_trading_terms_required"
        )
        records = runtime.journal.read_all()
        assert [
            record for record in records
            if record["kind"] == "pending_entry.hedge_admission_blocked"
        ][-1]["payload"]["reason"] == "bybit_trading_terms_required"
        assert [
            record for record in records
            if record["kind"] == "entry.aborted"
        ][-1]["payload"]["reason"] == "hedge_admission_blocked:bybit_trading_terms_required"
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_aster_max_notional_reject_arms_v1_venue_cooldown():
    with tempfile.TemporaryDirectory() as td:
        aster = RejectingHedgeAdapter(
            'HTTP 400: {"code":-5018,"msg":"maximum notional value limit"}'
        )
        runtime = LiveRuntime(
            make_test_config(td),
            venue_adapters={Venue.BINANCE: FlatAdapter(), Venue.ASTER: aster},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-lab",
            symbol="LABUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.ASTER,
            maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._drive_missing_hedge_live(
            pending, pending.pending_id, 1778787001000
        )

        assert driven is False
        assert aster.place_order_calls == 1
        assert pending.pending_id not in runtime.state.pending_entries
        assert runtime.state.venue_entry_cooldowns["aster:LABUSDT"]["block_scope"] == "symbol"
        assert runtime.state.venue_entry_cooldowns["aster:*"]["block_scope"] == "venue"
        assert runtime._candidate_admission_block(
            _candidate("OTHERUSDT", "aster", "binance"),
            1778787002000,
        )["reason"] == "max_notional_admission_blocked"
        records = runtime.journal.read_all()
        assert [
            record for record in records
            if record["kind"] == "runtime.venue_cooldown_started"
        ][-1]["payload"]["reason"] == "aster_max_notional_limit"
        runtime.journal.close()


@pytest.mark.asyncio
async def test_binance_5022_exception_path_creates_cooldown_without_admission_block():
    raw_error = (
        'HTTP 400: {"code":-5022,"msg":"Due to the order could not be '
        'executed as maker, the Post Only order will be rejected."}'
    )

    with tempfile.TemporaryDirectory() as td:
        runtime = LiveRuntime(make_test_config(td))
        runtime.journal.open()

        class RaisingExecutor:
            calls = 0

            async def execute(self, ctx):
                self.calls += 1
                raise OrderSubmitError(SubmitFailureClass.REJECTED, raw_error)

        executor = RaisingExecutor()
        runtime.entry_executor = executor
        candidate = _candidate("GTXUSDT", "binance", "bybit")

        first = await runtime._dispatch_entry(candidate, 1778787000000, price_hint=1.0)
        second = await runtime._dispatch_entry(candidate, 1778787001000, price_hint=1.0)

        assert first is False
        assert second is False
        assert executor.calls == 1
        assert "binance:GTXUSDT" not in runtime.state.venue_entry_cooldowns
        payload = [
            record["payload"]
            for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_post_only_reject_cooldown"
        ][-1]
        assert payload["venue"] == "binance"
        assert payload["symbol"] == "GTXUSDT"
        assert payload["reason"] == "post_only_would_take"
        assert payload["raw_error"] == raw_error[:500]
        assert payload["official_doc_url"] == (
            "https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code"
        )
        assert payload["evidence_gap"] is False
        assert payload["blocked_until_ms"] > 1778787000000

        runtime.journal.close()


@pytest.mark.asyncio
async def test_recovered_admission_block_prevents_dispatch_until_ttl_expires():
    with tempfile.TemporaryDirectory() as td:
        runtime = LiveRuntime(make_test_config(td))
        runtime.journal.open()
        runtime.state.venue_entry_cooldowns["bybit:LITEUSDT"] = {
            "venue": "bybit",
            "symbol": "LITEUSDT",
            "reason": "bybit_trading_terms_required",
            "raw_error": "bybit retCode=110126 retMsg=must sign required agreement",
            "blocked_until_ms": 1778787600000,
            "ttl_ms": 21_600_000,
            "official_doc_url": "",
            "evidence_gap": True,
        }

        class CountingExecutor:
            calls = 0

            async def execute(self, ctx):
                self.calls += 1
                return EntryExecutionResult(
                    route=ExecutionRoute.REJECTED,
                    state=EntryState.FAILED,
                    reject_reason="should not dispatch while admission block is live",
                )

        executor = CountingExecutor()
        runtime.entry_executor = executor
        candidate = _candidate("LITEUSDT", "bybit", "binance")

        blocked = await runtime._dispatch_entry(candidate, 1778787000000, price_hint=1.0)

        assert blocked is False
        assert executor.calls == 0
        assert runtime._symbol_admission_blocked_until_ms[("bybit", "LITEUSDT")] == 1778787600000
        payload = [
            record["payload"]
            for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_admission_blocked"
        ][-1]
        assert payload["venue"] == "bybit"
        assert payload["symbol"] == "LITEUSDT"
        assert payload["reason"] == "bybit_trading_terms_required"
        assert payload["raw_error"] == "bybit retCode=110126 retMsg=must sign required agreement"
        assert payload["blocked_until_ms"] == 1778787600000
        assert payload["official_doc_url"] == ""
        assert payload["evidence_gap"] is True

        runtime.journal.close()
