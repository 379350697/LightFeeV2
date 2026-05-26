from __future__ import annotations

import tempfile
from types import SimpleNamespace

import pytest

from lightfee.engine.entry import EntryState
from lightfee.engine.entry_sync import EntryExecutionResult
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.runtime import LiveRuntime
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
            "",
            True,
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
            "",
            True,
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
