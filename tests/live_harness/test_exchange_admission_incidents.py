from __future__ import annotations

import tempfile
from types import SimpleNamespace

import pytest

from lightfee.engine.entry import EntryState
from lightfee.engine.business_contract import entry_route_key
from lightfee.engine.entry_sync import EntryExecutionResult
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import PendingEntry
from lightfee.core.domain import AccountBalanceSnapshot, PositionSnapshot, Side, TimeInForce, Venue
from lightfee.core.domain import PassiveOrderProgress, PassiveOrderState
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.risk.modes import EngineLifecycle
from lightfee.venues.aster import AsterAdapter
from lightfee.venues.symbol_rules import SymbolRule
from lightfee.venues.transport import ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE
from tests.test_live_startup_preflight import make_test_config


pytestmark = pytest.mark.live_harness

_ADMISSIBLE_FIRST_FUNDING_MS = 1778787600000


class TrustedVenueAdapter:
    trading_capability_trusted = True
    okx_base_quantity_step = 0.001

    def passive_metadata(self, symbol: str):
        return {
            "quantity_step": 0.001,
            "min_quantity": 0.001,
            "min_notional": 0.0,
        }


class StrictReadonlyDisabledAdapter(TrustedVenueAdapter):
    trading_capability_trusted = False
    trading_preflight_status = {
        "venue": "hyperliquid",
        "authorization_mode": "api_wallet",
        "reason": "api_wallet_authorization_not_verified_strict_readonly",
        "clearinghouse_state_readable": True,
        "signer_matches_account": False,
        "trading_capability_trusted": False,
    }


def make_harness_config(tmp_path: str):
    config = make_test_config(tmp_path)
    config.strategy.pending_entry_pre_submit_hedgeable_fill_guard_enabled = False
    return config


def _runtime_with_metadata(tmp_path: str) -> LiveRuntime:
    adapter = TrustedVenueAdapter()
    return LiveRuntime(
        make_harness_config(tmp_path),
        venue_adapters={
            Venue.ASTER: adapter,
            Venue.BINANCE: adapter,
            Venue.BYBIT: adapter,
            Venue.HYPERLIQUID: adapter,
            Venue.OKX: adapter,
        },
    )


def _candidate(symbol: str, long_venue: str, short_venue: str) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        first_funding_timestamp_ms=_ADMISSIBLE_FIRST_FUNDING_MS,
        funding_timestamp_ms=_ADMISSIBLE_FIRST_FUNDING_MS,
        long_funding_timestamp_ms=_ADMISSIBLE_FIRST_FUNDING_MS,
        short_funding_timestamp_ms=_ADMISSIBLE_FIRST_FUNDING_MS,
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


class StructuredRejectingExecutor:
    def __init__(self, reject_reason: str, reject_evidence: dict):
        self.reject_reason = reject_reason
        self.reject_evidence = reject_evidence
        self.calls = 0

    async def execute(self, ctx):
        self.calls += 1
        result = EntryExecutionResult(
            route=ExecutionRoute.REJECTED,
            state=EntryState.FAILED,
            reject_reason=self.reject_reason,
        )
        result.reject_evidence = dict(self.reject_evidence)
        return result


class RejectingBybitPrecheckAdapter(TrustedVenueAdapter):
    def __init__(self, reject_reason: str):
        self.reject_reason = reject_reason
        self.precheck_calls = 0

    async def precheck_order_admission(self, request):
        self.precheck_calls += 1
        raise OrderSubmitError(SubmitFailureClass.REJECTED, self.reject_reason)


class RejectingAsterPrecheckAdapter(TrustedVenueAdapter):
    def __init__(self, reject_reason: str):
        self.reject_reason = reject_reason
        self.precheck_calls = 0

    async def precheck_order_admission(self, request):
        self.precheck_calls += 1
        raise OrderSubmitError(SubmitFailureClass.REJECTED, self.reject_reason)


class FakeAsterPrivateHeadroom:
    def __init__(
        self,
        remaining: float | None,
        *,
        available_balance_quote: float = 60.0,
        position_qty: float = 0.0,
        open_orders: list[dict] | None = None,
    ):
        self.remaining = remaining
        self.available_balance_quote = available_balance_quote
        self.position_qty = position_qty
        self.open_orders = list(open_orders or [])
        self.calls: list[tuple[str, int]] = []
        self.account_calls = 0
        self.position_calls: list[str] = []
        self.open_order_calls: list[str] = []

    async def fetch_remaining_openable_notional(self, symbol: str, leverage: int):
        self.calls.append((symbol, leverage))
        return self.remaining

    async def fetch_account_risk_snapshot(self):
        self.account_calls += 1
        return SimpleNamespace(
            available_balance_quote=self.available_balance_quote,
            equity_quote=self.available_balance_quote,
        )

    async def fetch_position(self, symbol: str):
        self.position_calls.append(symbol)
        return PositionSnapshot(
            venue=Venue.ASTER,
            symbol=symbol,
            side=Side.BUY,
            quantity=self.position_qty,
            entry_price=0.0,
            observed_at_ms=1778787000000,
        )

    async def fetch_open_orders(self, symbol: str | None = None):
        self.open_order_calls.append(str(symbol or ""))
        return list(self.open_orders)

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> None:
        return None


class FakeAsterRulesCache:
    async def get(self, transport, venue, venue_symbol):
        assert venue == Venue.ASTER
        return SymbolRule(
            tick_size=0.0001,
            qty_step=0.001,
            min_qty=0.001,
            min_notional=0.0,
            rule_source="exchangeInfo",
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
            "hyperliquid",
            "SEIUSDT",
            "Insufficient margin to place order. asset=40",
            "insufficient_margin_admission_blocked",
            "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses",
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
            "binance",
            "HMSTRUSDT",
            'HTTP 400: {"code":-2027,"msg":"Exceeded the maximum allowable position at current leverage."}',
            "leverage_admission_blocked",
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
            True,
        ),
        (
            "aster",
            "HUSDT",
            (
                "max_notional_admission_blocked: requested_notional=23.90043 "
                "remaining_openable_notional=0.0"
            ),
            "max_notional_admission_blocked",
            "https://asterdex.github.io/aster-api-website/futures/account%26trades/#remaining-openable-notional-value-user_data",
            False,
        ),
        (
            "aster",
            "LABUSDT",
            "aster_headroom_unavailable",
            "aster_headroom_unavailable",
            "https://asterdex.github.io/aster-api-website/futures/account%26trades/#remaining-openable-notional-value-user_data",
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
        runtime = _runtime_with_metadata(td)
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
            if expected_reason == "insufficient_balance_admission_blocked":
                cooldown_payload = [
                    record["payload"]
                    for record in records
                    if record["kind"] == "runtime.entry_admission_symbol_cooldown_armed"
                    and record["payload"].get("venue") == venue
                    and record["payload"].get("symbol") == symbol
                ][-1]
                assert cooldown_payload["reason"] == expected_reason
                assert cooldown_payload["reduce_only"] is False
                assert cooldown_payload["block_scope"] == "symbol"
                assert cooldown_payload["blocked_until_ms"] == state_payload["blocked_until_ms"]

        assert event_payload["reason"] == expected_reason
        assert event_payload["raw_error"] == raw_error[:500]
        assert event_payload["blocked_until_ms"] > 1778787000000
        assert event_payload["official_doc_url"] == official_doc_url
        assert event_payload["evidence_gap"] is evidence_gap

        runtime.journal.close()


@pytest.mark.asyncio
async def test_aster_submit_reject_evidence_creates_symbol_admission_block():
    with tempfile.TemporaryDirectory() as td:
        runtime = _runtime_with_metadata(td)
        runtime.journal.open()
        executor = StructuredRejectingExecutor(
            "aster_v3 passive order rejected: aster_v3 POST /fapi/v3/order rejected status=400",
            {
                "venue": "aster",
                "operation": "submit_passive_order",
                "http_status": 400,
                "raw_body": (
                    '{"code":-5018,"msg":"Youve reached the maximum notional '
                    'value limit for this symbol. You can still reduce or close '
                    'your position to manage your risk."}'
                ),
                "exchange_code": "-5018",
                "exchange_msg": (
                    "Youve reached the maximum notional value limit for this symbol. "
                    "You can still reduce or close your position to manage your risk."
                ),
                "request_context": {
                    "symbol": "ESPORTSUSDT",
                    "side": "buy",
                    "quantity": 12.0,
                    "price": 0.08,
                    "post_only": True,
                    "reduce_only": False,
                    "client_order_id": "entry-aster-reject-m",
                },
                "evidence_completeness": "complete",
            },
        )
        runtime.entry_executor = executor
        candidate = _candidate("ESPORTSUSDT", "aster", "binance")

        first = await runtime._dispatch_entry(candidate, 1778787000000, price_hint=0.08)
        second = await runtime._dispatch_entry(candidate, 1778787001000, price_hint=0.08)

        assert first is True
        assert second is False
        assert executor.calls == 1

        state_payload = runtime.state.venue_entry_cooldowns["aster:ESPORTSUSDT"]
        assert state_payload["reason"] == "max_notional_admission_blocked"
        assert state_payload["source"] == "exchange_5018_fallback"
        assert state_payload["exchange_code"] == "-5018"
        assert state_payload["http_status"] == 400
        assert state_payload["operation"] == "submit_passive_order"
        assert state_payload["order_role"] == "entry_result"
        assert state_payload["request_symbol"] == "ESPORTSUSDT"
        assert "maximum notional value limit" in state_payload["raw_error"]
        assert "aster:*" not in runtime.state.venue_entry_cooldowns

        blocked_payload = [
            record["payload"]
            for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_admission_blocked"
            and record["payload"].get("venue") == "aster"
            and record["payload"].get("symbol") == "ESPORTSUSDT"
        ][-1]
        assert blocked_payload["reason"] == "max_notional_admission_blocked"
        assert blocked_payload["exchange_code"] == "-5018"
        assert blocked_payload["block_scope"] == "symbol"
        runtime.journal.close()


@pytest.mark.asyncio
async def test_bybit_expired_key_blocks_paired_entry_before_maker_submit_venue_wide():
    with tempfile.TemporaryDirectory() as td:
        bybit = RejectingBybitPrecheckAdapter(
            "bybit order precheck failed: bybit retCode=33004 retMsg=Your api key has expired"
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={
                Venue.BINANCE: TrustedVenueAdapter(),
                Venue.BYBIT: bybit,
            },
        )
        runtime.journal.open()

        class CountingExecutor:
            calls = 0

            async def execute(self, ctx):
                self.calls += 1
                return EntryExecutionResult(
                    route=ExecutionRoute.REJECTED,
                    state=EntryState.FAILED,
                    reject_reason="executor must not receive Bybit-auth-invalid candidate",
                )

        executor = CountingExecutor()
        runtime.entry_executor = executor
        bybit_candidate = _candidate("AUTHUSDT", "binance", "bybit")
        clean_candidate = _candidate("CLEANUSDT", "binance", "aster")

        first = await runtime._dispatch_entry(
            bybit_candidate,
            1778787000000,
            price_hint=1.0,
        )
        filtered = runtime._filter_candidates_by_entry_admission(
            [bybit_candidate, clean_candidate],
            now_ms=1778787001000,
            stage="shortlist",
        )

        assert first is False
        assert bybit.precheck_calls == 1
        assert executor.calls == 0
        assert [candidate.symbol for candidate in filtered] == ["CLEANUSDT"]
        venue_cooldown = runtime.state.venue_entry_cooldowns["bybit:*"]
        assert venue_cooldown["reason"] == "venue_auth_invalid"
        assert venue_cooldown["source"] == "venue_private_health_precheck"
        assert venue_cooldown["block_scope"] == "venue"
        assert venue_cooldown["cooldown_scope"] == "venue"
        assert venue_cooldown["reduce_only"] is False
        assert venue_cooldown["venue_private_health_status"] == "auth_invalid"
        payload = [
            record["payload"]
            for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_admission_blocked"
            and record["payload"].get("venue") == "bybit"
        ][-1]
        assert payload["reason"] == "venue_auth_invalid"
        assert payload["source"] == "venue_private_health_precheck"
        assert payload["cooldown_scope"] == "venue"
        assert payload["reduce_only"] is False
        assert payload["raw_error"] == bybit.reject_reason[:500]
        runtime.journal.close()


@pytest.mark.asyncio
async def test_aster_zero_headroom_blocks_hedge_side_before_maker_submit():
    with tempfile.TemporaryDirectory() as td:
        aster = RejectingAsterPrecheckAdapter(
            "max_notional_admission_blocked: requested_notional=23.90043 "
            "remaining_openable_notional=0.0"
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={
                Venue.BINANCE: TrustedVenueAdapter(),
                Venue.ASTER: aster,
            },
        )
        runtime.journal.open()

        class CountingExecutor:
            calls = 0

            async def execute(self, ctx):
                self.calls += 1
                return EntryExecutionResult(route=ExecutionRoute.FALLBACK_TO_STANDARD)

        executor = CountingExecutor()
        runtime.entry_executor = executor
        candidate = _candidate("HUSDT", "binance", "aster")

        dispatched = await runtime._dispatch_entry(
            candidate,
            1778787000000,
            price_hint=1.0,
        )

        assert dispatched is False
        assert executor.calls == 0
        assert aster.precheck_calls == 1
        assert runtime.state.venue_entry_cooldowns["aster:HUSDT"]["reason"] == (
            "max_notional_admission_blocked"
        )
        symbol_cooldown = runtime.state.venue_entry_cooldowns["aster:HUSDT"]
        assert symbol_cooldown["requested_notional"] == pytest.approx(23.90043)
        assert symbol_cooldown["remaining_openable_notional"] == pytest.approx(0.0)
        assert symbol_cooldown["notional_gap"] == pytest.approx(23.90043)
        assert symbol_cooldown["leverage"] == ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE
        assert symbol_cooldown["headroom_source"] == "exchange_error_text"
        assert symbol_cooldown["order_role"] == "hedge"
        assert "aster:*" not in runtime.state.venue_entry_cooldowns

        clean_candidate = _candidate("CLEANUSDT", "binance", "bybit")
        filtered = runtime._filter_candidates_by_entry_admission(
            [candidate, clean_candidate],
            now_ms=1778787001000,
            stage="shortlist",
        )
        assert [item.symbol for item in filtered] == ["CLEANUSDT"]
        degraded_payload = [
            record["payload"]
            for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_admission_venue_degraded"
        ][-1]
        assert degraded_payload["symbol"] == "HUSDT"
        assert degraded_payload["blocked_symbol"] == "HUSDT"
        assert degraded_payload["requested_notional"] == pytest.approx(23.90043)
        assert degraded_payload["remaining_openable_notional"] == pytest.approx(0.0)
        assert degraded_payload["notional_gap"] == pytest.approx(23.90043)
        assert degraded_payload["leverage"] == ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE
        assert degraded_payload["headroom_source"] == "exchange_error_text"
        assert degraded_payload["samples"][0]["requested_notional"] == pytest.approx(
            23.90043
        )
        assert degraded_payload["samples"][0]["remaining_openable_notional"] == (
            pytest.approx(0.0)
        )
        assert degraded_payload["samples"][0]["notional_gap"] == pytest.approx(
            23.90043
        )
        records = runtime.journal.read_all()
        assert not [
            record for record in records
            if record["kind"] == "order.passive_submitted"
        ]
        assert [
            record
            for record in records
            if record["kind"] == "runtime.entry_blocked_admission_selection"
        ][-1]["payload"]["reason"] == "max_notional_admission_blocked"
        runtime.journal.close()


@pytest.mark.asyncio
async def test_real_aster_adapter_zero_headroom_allows_submit_when_account_truth_sufficient(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        import lightfee.venues.aster as aster_mod

        aster = AsterAdapter(mode="paper")
        private = FakeAsterPrivateHeadroom(remaining=0.0)
        aster._private = private
        monkeypatch.setattr(
            aster_mod.transport_mod,
            "get_symbol_rules_cache",
            lambda: FakeAsterRulesCache(),
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={
                Venue.BINANCE: TrustedVenueAdapter(),
                Venue.ASTER: aster,
            },
        )
        runtime.journal.open()

        class CountingExecutor:
            calls = 0

            async def execute(self, ctx):
                self.calls += 1
                return EntryExecutionResult(route=ExecutionRoute.FALLBACK_TO_STANDARD)

        executor = CountingExecutor()
        runtime.entry_executor = executor

        dispatched = await runtime._dispatch_entry(
            _candidate("HUSDT", "binance", "aster"),
            1778787000000,
            price_hint=1.0,
        )

        assert dispatched is True
        assert executor.calls == 1
        assert private.calls == [
            ("HUSDT", ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE)
        ]
        assert private.account_calls == 1
        assert private.position_calls == ["HUSDT"]
        assert private.open_order_calls == ["HUSDT"]
        assert "aster:HUSDT" not in runtime.state.venue_entry_cooldowns
        assert "aster:*" not in runtime.state.venue_entry_cooldowns
        records = runtime.journal.read_all()
        assert [
            record
            for record in records
            if record["kind"] == "runtime.entry_admission_headroom_advisory"
        ][-1]["payload"]["reason"] == "aster_headroom_advisory_zero"
        assert not [
            record for record in records
            if record["kind"] == "runtime.entry_blocked_admission_selection"
        ]
        runtime.journal.close()


@pytest.mark.parametrize(
    "reject_status",
    ["perpMarginRejected", "insufficientSpotBalanceRejected"],
)
def test_hyperliquid_official_margin_reject_statuses_classify_as_admission_blocks(
    reject_status: str,
):
    metadata = LiveRuntime._entry_admission_reject_metadata(
        Venue.HYPERLIQUID,
        reject_status,
    )

    assert metadata is not None
    assert metadata["reason"] == "insufficient_margin_admission_blocked"
    assert metadata["official_doc_url"] == (
        "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
    )
    assert metadata["evidence_gap"] is False


class FlatAdapter:
    async def fetch_position(self, symbol: str):
        return None

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity


class BalanceAdapter(FlatAdapter):
    def __init__(self, balance: AccountBalanceSnapshot | None = None, error: Exception | None = None):
        self.balance = balance
        self.error = error
        self.balance_calls = 0

    async def fetch_account_balance_snapshot(self):
        self.balance_calls += 1
        if self.error is not None:
            raise self.error
        return self.balance


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


class AuthInvalidHedgeAdapter(FlatAdapter):
    def __init__(self, message: str, venue: Venue):
        self.message = message
        self.venue = venue
        self.place_order_calls = 0

    async def place_order(self, request):
        self.place_order_calls += 1
        raise OrderSubmitError(SubmitFailureClass.REJECTED, self.message)

    async def fetch_order_fill_reconciliation(self, symbol: str, order_id: str, client_order_id: str):
        return None

    async def fetch_position(self, symbol: str):
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1778787001000,
        )

    async def fetch_open_orders(self, symbol: str):
        return []


class LiveSingleLegCleanupAdapter(FlatAdapter):
    def __init__(self, venue: Venue, side: Side, quantity: float):
        self.venue = venue
        self.side = side
        self.quantity = quantity
        self.fetch_position_calls = 0
        self.place_order_calls = 0
        self.last_request = None
        self.cancel_calls: list[dict] = []
        self.progress_calls: list[dict] = []

    async def fetch_position(self, symbol: str):
        self.fetch_position_calls += 1
        qty = self.quantity if self.fetch_position_calls <= 2 else 0.0
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=self.side,
            quantity=qty,
            entry_price=1.0 if qty > 0 else 0.0,
            observed_at_ms=1778787001000,
        )

    async def fetch_open_orders(self, symbol: str):
        return []

    async def cancel_passive_order(self, symbol, order_id="", client_order_id=None):
        self.cancel_calls.append({
            "symbol": symbol,
            "order_id": order_id,
            "client_order_id": client_order_id,
        })

    async def query_passive_order_progress(
        self,
        symbol,
        order_id="",
        client_order_id=None,
        side=None,
    ):
        self.progress_calls.append({
            "symbol": symbol,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "side": side,
        })
        return PassiveOrderProgress(
            venue=self.venue,
            symbol=symbol,
            side=side or self.side,
            order_id=order_id,
            client_order_id=client_order_id or "",
            cumulative_quantity=0.0,
            state=PassiveOrderState.CANCELED,
            observed_at_ms=1778787001000,
        )

    async def place_order(self, request):
        self.place_order_calls += 1
        self.last_request = request
        return SimpleNamespace(
            order_id=f"{self.venue.value}-cleanup-order",
            quantity=float(request.quantity or 0.0),
            price=1.0,
        )


class TruthUnavailableSingleLegCleanupAdapter(LiveSingleLegCleanupAdapter):
    async def fetch_position(self, symbol: str):
        self.fetch_position_calls += 1
        raise RuntimeError("position truth unavailable")


class OpenOrderSingleLegCleanupAdapter(LiveSingleLegCleanupAdapter):
    async def fetch_open_orders(self, symbol: str):
        return [
            {
                "symbol": symbol,
                "orderId": "maker-order-open",
                "clientOrderId": "maker-cid-open",
            }
        ]


class FailingSingleLegCleanupAdapter(LiveSingleLegCleanupAdapter):
    async def fetch_position(self, symbol: str):
        self.fetch_position_calls += 1
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=self.side,
            quantity=self.quantity,
            entry_price=1.0,
            observed_at_ms=1778787001000,
        )

    async def place_order(self, request):
        self.place_order_calls += 1
        self.last_request = request
        raise OrderSubmitError(
            SubmitFailureClass.REJECTED,
            "binance reduce-only cleanup rejected",
        )


def _pending_for_hedge_reject(
    *,
    entry_id: str,
    symbol: str,
    long_venue: Venue,
    short_venue: Venue,
    maker_leg: str,
    maker_order_id: str = "",
    maker_client_order_id: str = "",
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
        maker_order_id=maker_order_id,
        maker_client_order_id=maker_client_order_id,
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
            make_harness_config(td),
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
async def test_pending_hedge_unclassified_reject_after_maker_exposure_aborts_without_retry():
    with tempfile.TemporaryDirectory() as td:
        aster = RejectingHedgeAdapter(
            "aster_v3 order rejected: aster_v3 POST /fapi/v3/order rejected status=400"
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BINANCE: FlatAdapter(), Venue.ASTER: aster},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-lab-generic-reject",
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
        assert pending.repair_state == (
            "hedge_admission_blocked:hedge_rejected_after_maker_exposure"
        )
        cooldown = runtime.state.venue_entry_cooldowns["aster:LABUSDT"]
        assert cooldown["reason"] == "hedge_rejected_after_maker_exposure"
        assert cooldown["source"] == "pending_hedge"
        assert cooldown["block_scope"] == "symbol"
        assert cooldown["evidence_gap"] is True
        records = runtime.journal.read_all()
        admission = [
            record for record in records
            if record["kind"] == "pending_entry.hedge_admission_blocked"
        ][-1]["payload"]
        assert admission["reason"] == "hedge_rejected_after_maker_exposure"
        assert admission["evidence_gap"] is True
        assert not [
            record
            for record in records
            if record["kind"] == "pending_entry.hedge_submit_result"
            and record["payload"].get("outcome") == "error"
        ]
        runtime.journal.close()


@pytest.mark.asyncio
async def test_recovery_pending_hedge_unclassified_reject_after_maker_exposure_aborts_without_truth_gap_retry():
    with tempfile.TemporaryDirectory() as td:
        aster = RejectingHedgeAdapter(
            "aster_v3 order rejected: aster_v3 POST /fapi/v3/order rejected status=400"
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BINANCE: FlatAdapter(), Venue.ASTER: aster},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-lab-recovery-generic-reject",
            symbol="LABUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.ASTER,
            maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._recover_drive_missing_hedge(
            pending, "startup_recovery"
        )

        assert driven is False
        assert aster.place_order_calls == 1
        assert pending.pending_id not in runtime.state.pending_entries
        assert pending.repair_state == (
            "hedge_admission_blocked:hedge_rejected_after_maker_exposure"
        )
        records = runtime.journal.read_all()
        assert [
            record for record in records
            if record["kind"] == "pending_entry.hedge_admission_blocked"
        ][-1]["payload"]["reason"] == "hedge_rejected_after_maker_exposure"
        assert not [
            record for record in records
            if record["kind"] == "pending_entry.accepted_order_truth_gap_registered"
        ]
        assert not [
            record for record in records
            if record["kind"] == "recovery.hedge_submit_error"
        ]
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_bybit_auth_invalid_recovers_owned_single_leg_on_maker_venue():
    with tempfile.TemporaryDirectory() as td:
        binance = LiveSingleLegCleanupAdapter(Venue.BINANCE, Side.BUY, 4.0)
        bybit = AuthInvalidHedgeAdapter(
            "bybit order failed: bybit retCode=33004 retMsg=Your api key has expired",
            Venue.BYBIT,
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-auth-single-leg",
            symbol="AUTHUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            maker_leg="long",
            maker_order_id="maker-auth-single-leg",
            maker_client_order_id="maker-auth-single-leg-cid",
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._drive_missing_hedge_live(
            pending,
            pending.pending_id,
            1778787001000,
        )

        assert driven is False
        assert bybit.place_order_calls == 1
        assert binance.place_order_calls == 1
        assert binance.last_request is not None
        assert binance.last_request.reduce_only is True
        assert binance.last_request.post_only is False
        assert binance.last_request.time_in_force == TimeInForce.IOC
        assert binance.last_request.side == Side.SELL
        assert pending.pending_id not in runtime.state.pending_entries
        assert pending.repair_state == "single_leg_exposure_recovery:venue_auth_invalid"
        records = runtime.journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "entry.aborted" not in kinds
        assert "pending_entry.single_leg_flatten_submitted" in kinds
        assert "pending_entry.single_leg_flatten_succeeded" in kinds
        assert "pending_entry.terminalized_after_single_leg_recovery" in kinds
        started = [
            record["payload"]
            for record in records
            if record["kind"] == "pending_entry.single_leg_exposure_recovery_started"
        ][-1]
        assert started["entry_id"] == pending.pending_id
        assert started["failed_hedge_venue"] == "bybit"
        assert started["cleanup_venue"] == "binance"
        assert started["reason"] == "venue_auth_invalid"
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_bybit_auth_invalid_keeps_risk_only_when_truth_unavailable():
    with tempfile.TemporaryDirectory() as td:
        binance = TruthUnavailableSingleLegCleanupAdapter(Venue.BINANCE, Side.BUY, 4.0)
        bybit = AuthInvalidHedgeAdapter(
            "bybit order failed: bybit retCode=33004 retMsg=Your api key has expired",
            Venue.BYBIT,
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-auth-truth-unavailable",
            symbol="AUTHUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._drive_missing_hedge_live(
            pending,
            pending.pending_id,
            1778787001000,
        )

        assert driven is False
        assert pending.pending_id in runtime.state.pending_entries
        assert pending.repair_state == "single_leg_exposure_recovery:venue_auth_invalid"
        assert pending.reconcile_next_attempt_ms > 1778787001000
        assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
        assert runtime.state.recovery_blocked_reason == "single_leg_exposure_recovery"
        records = runtime.journal.read_all()
        failures = [
            record["payload"]
            for record in records
            if record["kind"] == "pending_entry.single_leg_flatten_failed"
        ]
        assert failures[-1]["reason"] == "single_leg_truth_unavailable"
        assert "pending_entry.terminalized_after_single_leg_recovery" not in [
            record["kind"] for record in records
        ]
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_bybit_auth_invalid_keeps_risk_only_when_maker_order_open():
    with tempfile.TemporaryDirectory() as td:
        binance = OpenOrderSingleLegCleanupAdapter(Venue.BINANCE, Side.BUY, 4.0)
        bybit = AuthInvalidHedgeAdapter(
            "bybit order failed: bybit retCode=33004 retMsg=Your api key has expired",
            Venue.BYBIT,
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-auth-open-order",
            symbol="AUTHUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            maker_leg="long",
        )
        pending.maker_order_id = "maker-order-open"
        pending.maker_client_order_id = "maker-cid-open"
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._drive_missing_hedge_live(
            pending,
            pending.pending_id,
            1778787001000,
        )

        assert driven is False
        assert pending.pending_id in runtime.state.pending_entries
        assert pending.repair_state == "single_leg_exposure_recovery:venue_auth_invalid"
        assert pending.reconcile_next_attempt_ms > 1778787001000
        assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
        records = runtime.journal.read_all()
        failures = [
            record["payload"]
            for record in records
            if record["kind"] == "pending_entry.single_leg_flatten_failed"
        ]
        assert failures[-1]["reason"] == "single_leg_open_order_truth_present"
        assert binance.place_order_calls == 0
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_bybit_auth_invalid_keeps_risk_only_when_reduce_only_fails():
    with tempfile.TemporaryDirectory() as td:
        binance = FailingSingleLegCleanupAdapter(Venue.BINANCE, Side.BUY, 4.0)
        bybit = AuthInvalidHedgeAdapter(
            "bybit order failed: bybit retCode=33004 retMsg=Your api key has expired",
            Venue.BYBIT,
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-auth-cleanup-fails",
            symbol="AUTHUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._drive_missing_hedge_live(
            pending,
            pending.pending_id,
            1778787001000,
        )

        assert driven is False
        assert pending.pending_id in runtime.state.pending_entries
        assert pending.repair_state == "single_leg_exposure_recovery:venue_auth_invalid"
        assert pending.reconcile_next_attempt_ms > 1778787001000
        assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
        assert binance.place_order_calls >= 1
        records = runtime.journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "pending_entry.single_leg_flatten_failed" in kinds
        assert "pending_entry.terminalized_after_single_leg_recovery" not in kinds
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_binance_leverage_reject_aborts_without_retry():
    with tempfile.TemporaryDirectory() as td:
        binance = RejectingHedgeAdapter(
            'HTTP 400: {"code":-2027,"msg":"Exceeded the maximum allowable position at current leverage."}'
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BYBIT: FlatAdapter(), Venue.BINANCE: binance},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-hmstr",
            symbol="HMSTRUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.BINANCE,
            maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._drive_missing_hedge_live(
            pending, pending.pending_id, 1778787001000
        )

        assert driven is False
        assert binance.place_order_calls == 1
        assert pending.pending_id not in runtime.state.pending_entries
        assert pending.repair_state == "hedge_admission_blocked:leverage_admission_blocked"
        cooldown = runtime.state.venue_entry_cooldowns["binance:HMSTRUSDT"]
        assert cooldown["reason"] == "leverage_admission_blocked"
        assert cooldown["official_doc_url"] == (
            "https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code"
        )
        records = runtime.journal.read_all()
        assert [
            record for record in records
            if record["kind"] == "pending_entry.hedge_admission_blocked"
        ][-1]["payload"]["reason"] == "leverage_admission_blocked"
        assert [
            record for record in records
            if record["kind"] == "entry.aborted"
        ][-1]["payload"]["reason"] == "hedge_admission_blocked:leverage_admission_blocked"
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_hyperliquid_insufficient_margin_reject_aborts_without_retry():
    with tempfile.TemporaryDirectory() as td:
        hyperliquid = RejectingHedgeAdapter(
            "Insufficient margin to place order. asset=40"
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={
                Venue.BYBIT: FlatAdapter(),
                Venue.HYPERLIQUID: hyperliquid,
            },
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-sei",
            symbol="SEIUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.HYPERLIQUID,
            maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        driven = await runtime._drive_missing_hedge_live(
            pending, pending.pending_id, 1778787001000
        )

        assert driven is False
        assert hyperliquid.place_order_calls == 1
        assert pending.pending_id not in runtime.state.pending_entries
        assert pending.repair_state == (
            "hedge_admission_blocked:insufficient_margin_admission_blocked"
        )
        cooldown = runtime.state.venue_entry_cooldowns["hyperliquid:SEIUSDT"]
        expected_pair_id = "seiusdt:bybit->hyperliquid"
        assert cooldown["reason"] == "insufficient_margin_admission_blocked"
        assert cooldown["source"] == "pending_hedge"
        assert cooldown["block_scope"] == "symbol"
        expected_blocked_until_ms = 1778787001000 + runtime._SYMBOL_ADMISSION_BLOCK_TTL_MS
        assert cooldown["blocked_until_ms"] == expected_blocked_until_ms
        assert cooldown["candidate_pair_id"] == expected_pair_id
        assert cooldown["official_doc_url"] == (
            "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
        )
        assert cooldown["evidence_gap"] is False
        venue_cooldown = runtime.state.venue_entry_cooldowns["hyperliquid:*"]
        assert venue_cooldown["reason"] == "insufficient_margin_admission_blocked"
        assert venue_cooldown["block_scope"] == "venue"
        assert venue_cooldown["source"] == "pending_hedge"
        assert venue_cooldown["blocked_symbol"] == "SEIUSDT"
        assert venue_cooldown["candidate_pair_id"] == expected_pair_id
        assert runtime._candidate_admission_block(
            _candidate("WLDUSDT", "bybit", "hyperliquid"),
            1778787002000,
        )["reason"] == "insufficient_margin_admission_blocked"
        records = runtime.journal.read_all()
        pending_payload = [
            record for record in records
            if record["kind"] == "pending_entry.hedge_admission_blocked"
        ][-1]["payload"]
        assert pending_payload["reason"] == "insufficient_margin_admission_blocked"
        assert pending_payload["source"] == "pending_hedge"
        assert pending_payload["block_scope"] == "venue"
        assert pending_payload["blocked_until_ms"] == expected_blocked_until_ms
        assert pending_payload["candidate_pair_id"] == expected_pair_id
        assert pending_payload["official_doc_url"] == (
            "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
        )
        assert pending_payload["evidence_gap"] is False
        venue_cooldown_payload = [
            record for record in records
            if record["kind"] == "runtime.venue_cooldown_started"
        ][-1]["payload"]
        assert venue_cooldown_payload["reason"] == "insufficient_margin_admission_blocked"
        assert venue_cooldown_payload["source"] == "pending_hedge"
        assert venue_cooldown_payload["official_doc_url"] == (
            "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
        )
        assert venue_cooldown_payload["evidence_gap"] is False
        assert [
            record for record in records
            if record["kind"] == "entry.aborted"
        ][-1]["payload"]["reason"] == (
            "hedge_admission_blocked:insufficient_margin_admission_blocked"
        )
        runtime.journal.close()


def test_hyperliquid_venue_cooldown_prunes_new_entry_candidates_before_shortlist():
    with tempfile.TemporaryDirectory() as td:
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BYBIT: FlatAdapter(), Venue.HYPERLIQUID: FlatAdapter()},
        )
        runtime.journal.open()
        now_ms = 1778787002000
        blocked_until_ms = now_ms + runtime._SYMBOL_ADMISSION_BLOCK_TTL_MS
        runtime.state.venue_entry_cooldowns["hyperliquid:*"] = {
            "venue": "hyperliquid",
            "symbol": "*",
            "blocked_symbol": "SEIUSDT",
            "reason": "insufficient_margin_admission_blocked",
            "source": "pending_hedge",
            "block_scope": "venue",
            "blocked_until_ms": blocked_until_ms,
            "candidate_pair_id": "seiusdt:bybit->hyperliquid",
            "pair_id": "seiusdt:bybit->hyperliquid",
            "official_doc_url": (
                "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
            ),
            "evidence_gap": False,
        }

        filtered = runtime._filter_candidates_by_entry_admission(
            [
                _candidate("WLDUSDT", "bybit", "hyperliquid"),
                _candidate("BTCUSDT", "bybit", "binance"),
            ],
            now_ms=now_ms,
            stage="shortlist",
        )

        assert [candidate.symbol for candidate in filtered] == ["BTCUSDT"]
        assert runtime._last_entry_admission_filter_blockers == {
            "insufficient_margin_admission_blocked": 1
        }
        sample = runtime._last_entry_admission_filter_samples[0]
        assert sample["candidate_pair_id"] == "wldusdt:bybit->hyperliquid"
        assert sample["venue"] == "hyperliquid"
        assert sample["symbol"] == "WLDUSDT"
        assert sample["blocked_symbol"] == "SEIUSDT"
        assert sample["block_scope"] == "venue"
        assert sample["blocked_until_ms"] == blocked_until_ms
        assert sample["official_doc_url"] == (
            "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
        )
        assert sample["evidence_gap"] is False
        payload = [
            record["payload"] for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_admission_venue_degraded"
        ][-1]
        assert payload["venue"] == "hyperliquid"
        assert payload["reason"] == "insufficient_margin_admission_blocked"
        assert payload["block_scope"] == "venue"
        assert payload["aggregation_key"] == (
            "shortlist:hyperliquid:WLDUSDT:insufficient_margin_admission_blocked:venue"
        )
        assert payload["source"] == "pending_hedge"
        assert payload["candidate_count"] == 2
        assert payload["blocked_count"] == 1
        assert payload["allowed_count"] == 1
        assert payload["suppressed_count"] == 0
        assert payload["samples"][0]["candidate_pair_id"] == "wldusdt:bybit->hyperliquid"
        runtime.journal.close()


def test_hyperliquid_strict_readonly_prunes_new_entry_candidates_before_shortlist():
    with tempfile.TemporaryDirectory() as td:
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={
                Venue.BYBIT: TrustedVenueAdapter(),
                Venue.BINANCE: TrustedVenueAdapter(),
                Venue.HYPERLIQUID: StrictReadonlyDisabledAdapter(),
            },
        )
        runtime.journal.open()

        filtered = runtime._filter_candidates_by_entry_admission(
            [
                _candidate("WLDUSDT", "bybit", "hyperliquid"),
                _candidate("BTCUSDT", "bybit", "binance"),
            ],
            now_ms=1778787002000,
            stage="shortlist",
        )

        assert [candidate.symbol for candidate in filtered] == ["BTCUSDT"]
        assert runtime._last_entry_admission_filter_blockers == {
            "api_wallet_authorization_not_verified_strict_readonly": 1
        }
        sample = runtime._last_entry_admission_filter_samples[0]
        assert sample["candidate_pair_id"] == "wldusdt:bybit->hyperliquid"
        assert sample["venue"] == "hyperliquid"
        assert sample["block_scope"] == "venue"
        assert sample["source"] == "trading_capability_preflight"
        assert sample["policy_block"] is True
        assert sample["account_state_readable"] is True
        assert sample["signer_matches_account"] is False
        assert sample["trading_authorization_trusted"] is False

        payload = [
            record["payload"] for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_admission_venue_degraded"
        ][-1]
        assert payload["venue"] == "hyperliquid"
        assert payload["reason"] == "api_wallet_authorization_not_verified_strict_readonly"
        assert payload["source"] == "trading_capability_preflight"
        assert payload["policy_block"] is True
        assert payload["blocked_count"] == 1
        assert payload["allowed_count"] == 1
        runtime.journal.close()


@pytest.mark.asyncio
async def test_hyperliquid_scan_start_balance_prefilter_arms_venue_cooldown():
    with tempfile.TemporaryDirectory() as td:
        now_ms = 1778787002000
        hyperliquid = BalanceAdapter(
            AccountBalanceSnapshot(
                venue=Venue.HYPERLIQUID,
                asset="USDC",
                free=5.0,
                locked=0.0,
                observed_at_ms=now_ms,
                balance_classification="unified_collateral_available",
                user_abstraction="unifiedAccount",
                spot_usdc_available=145.863168,
            )
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.HYPERLIQUID: hyperliquid},
        )
        runtime.journal.open()

        allowed = await runtime._refresh_hyperliquid_entry_balance_admission(now_ms)

        assert allowed is False
        assert hyperliquid.balance_calls == 1
        cooldown = runtime.state.venue_entry_cooldowns["hyperliquid:*"]
        assert cooldown["reason"] == "insufficient_margin_admission_prefiltered"
        assert cooldown["source"] == "scan_start_balance_prefilter"
        assert cooldown["block_scope"] == "venue"
        assert cooldown["available_balance_quote"] == pytest.approx(5.0)
        assert cooldown["required_initial_margin_quote"] > 5.0
        assert cooldown["live_target_leverage"] == runtime.config.strategy.live_target_leverage
        assert cooldown["balance_classification"] == "unified_collateral_available"
        assert cooldown["user_abstraction"] == "unifiedAccount"
        assert cooldown["spot_usdc_available"] == pytest.approx(145.863168)
        records = runtime.journal.read_all()
        event = [
            record["payload"] for record in records
            if record["kind"] == "runtime.entry_admission_blocked"
        ][-1]
        assert event["reason"] == "insufficient_margin_admission_prefiltered"
        assert event["source"] == "scan_start_balance_prefilter"
        assert event["evidence_gap"] is False
        assert event["balance_classification"] == "unified_collateral_available"
        assert event["user_abstraction"] == "unifiedAccount"
        assert event["spot_usdc_available"] == pytest.approx(145.863168)
        runtime.journal.close()


@pytest.mark.asyncio
async def test_hyperliquid_candidate_balance_prefilter_prunes_only_underfunded_candidates():
    with tempfile.TemporaryDirectory() as td:
        now_ms = 1778787002000
        hyperliquid = BalanceAdapter(
            AccountBalanceSnapshot(
                venue=Venue.HYPERLIQUID,
                asset="USDC",
                free=20.0,
                locked=0.0,
                observed_at_ms=now_ms,
            )
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BYBIT: FlatAdapter(), Venue.HYPERLIQUID: hyperliquid},
        )
        runtime.journal.open()
        small = _candidate("SMALLUSDT", "bybit", "hyperliquid")
        small.entry_notional_quote = 50.0
        large = _candidate("LARGEUSDT", "bybit", "hyperliquid")
        large.entry_notional_quote = 100.0
        bybit_only = _candidate("BTCUSDT", "bybit", "binance")
        bybit_only.entry_notional_quote = 100.0

        filtered = await runtime._filter_candidates_by_entry_balance_admission(
            [small, large, bybit_only],
            now_ms=now_ms,
            stage="shortlist",
        )

        assert [candidate.symbol for candidate in filtered] == ["SMALLUSDT", "BTCUSDT"]
        assert runtime._last_entry_admission_filter_blockers == {
            "insufficient_margin_admission_prefiltered": 1
        }
        sample = runtime._last_entry_admission_filter_samples[0]
        assert sample["candidate_pair_id"] == "largeusdt:bybit->hyperliquid"
        assert sample["available_balance_quote"] == pytest.approx(20.0)
        assert sample["entry_notional_quote"] == pytest.approx(100.0)
        assert sample["required_initial_margin_quote"] > 20.0
        assert "balance_classification" not in sample
        assert "user_abstraction" not in sample
        assert "spot_usdc_available" not in sample
        payload = [
            record["payload"] for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_admission_venue_degraded"
        ][-1]
        assert payload["reason"] == "insufficient_margin_admission_prefiltered"
        assert payload["source"] == "candidate_balance_prefilter"
        assert payload["blocked_count"] == 1
        assert payload["allowed_count"] == 2
        runtime.journal.close()


@pytest.mark.asyncio
async def test_hyperliquid_balance_unavailable_blocks_entry_with_evidence_gap():
    with tempfile.TemporaryDirectory() as td:
        now_ms = 1778787002000
        hyperliquid = BalanceAdapter(error=RuntimeError("clearinghouse unavailable"))
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BYBIT: FlatAdapter(), Venue.HYPERLIQUID: hyperliquid},
        )
        runtime.journal.open()
        candidate = _candidate("MOVEUSDT", "bybit", "hyperliquid")

        filtered = await runtime._filter_candidates_by_entry_balance_admission(
            [candidate],
            now_ms=now_ms,
            stage="shortlist",
        )

        assert filtered == []
        assert runtime._last_entry_admission_filter_blockers == {
            "hyperliquid_account_balance_unavailable": 1
        }
        sample = runtime._last_entry_admission_filter_samples[0]
        assert sample["reason"] == "hyperliquid_account_balance_unavailable"
        assert sample["evidence_gap"] is True
        payload = [
            record["payload"] for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_admission_venue_degraded"
        ][-1]
        assert payload["reason"] == "hyperliquid_account_balance_unavailable"
        assert payload["evidence_gap"] is True
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_aster_max_notional_reject_arms_v1_venue_cooldown():
    with tempfile.TemporaryDirectory() as td:
        aster = RejectingHedgeAdapter(
            'HTTP 400: {"code":-5018,"msg":"maximum notional value limit"}'
        )
        runtime = LiveRuntime(
            make_harness_config(td),
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
        assert "aster:*" not in runtime.state.venue_entry_cooldowns
        assert runtime._candidate_admission_block(
            _candidate("OTHERUSDT", "aster", "binance"),
            1778787002000,
        ) is None
        assert runtime._candidate_admission_block(
            _candidate("LABUSDT", "aster", "binance"),
            1778787002000,
        )["reason"] == "max_notional_admission_blocked"
        records = runtime.journal.read_all()
        assert not [
            record for record in records
            if record["kind"] == "runtime.venue_cooldown_started"
        ]
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_aster_max_notional_error_text_aborts_without_retry_and_hard_cools_down():
    with tempfile.TemporaryDirectory() as td:
        aster = RejectingHedgeAdapter(
            "max_notional_admission_blocked: requested_notional=23.90043 "
            "remaining_openable_notional=0.0"
        )
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BINANCE: FlatAdapter(), Venue.ASTER: aster},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-husdt",
            symbol="HUSDT",
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
        assert (
            pending.repair_state
            == "hedge_admission_blocked:max_notional_admission_blocked"
        )
        assert runtime.state.venue_entry_cooldowns["aster:HUSDT"]["reason"] == (
            "max_notional_admission_blocked"
        )
        assert runtime.state.venue_entry_cooldowns["aster:HUSDT"]["block_scope"] == (
            "symbol"
        )
        assert "aster:*" not in runtime.state.venue_entry_cooldowns
        assert runtime._candidate_admission_block(
            _candidate("HUSDT", "binance", "aster"),
            1778787002000,
        )["reason"] == "max_notional_admission_blocked"
        assert runtime._candidate_admission_block(
            _candidate("OTHERUSDT", "binance", "aster"),
            1778787002000,
        ) is None

        records = runtime.journal.read_all()
        assert not [
            record
            for record in records
            if record["kind"] == "pending_entry.hedge_submit_result"
            and record["payload"].get("outcome") == "error"
        ]
        assert [
            record
            for record in records
            if record["kind"] == "pending_entry.hedge_admission_blocked"
        ][-1]["payload"]["reason"] == "max_notional_admission_blocked"
        assert not [
            record
            for record in records
            if record["kind"] == "runtime.venue_cooldown_started"
        ]
        runtime.journal.close()


@pytest.mark.asyncio
async def test_pending_hedge_aster_headroom_unavailable_arms_symbol_cooldown():
    with tempfile.TemporaryDirectory() as td:
        aster = RejectingHedgeAdapter("aster_headroom_unavailable")
        runtime = LiveRuntime(
            make_harness_config(td),
            venue_adapters={Venue.BINANCE: FlatAdapter(), Venue.ASTER: aster},
        )
        runtime.journal.open()
        pending = _pending_for_hedge_reject(
            entry_id="entry-lab-headroom-gap",
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
        assert runtime.state.venue_entry_cooldowns["aster:LABUSDT"]["block_scope"] == "symbol"
        assert "aster:*" not in runtime.state.venue_entry_cooldowns
        assert runtime._candidate_admission_block(
            _candidate("OTHERUSDT", "aster", "binance"),
            1778787002000,
        ) is None
        assert runtime._candidate_admission_block(
            _candidate("LABUSDT", "aster", "binance"),
            1778787002000,
        )["reason"] == "aster_headroom_unavailable"
        records = runtime.journal.read_all()
        assert not [
            record for record in records
            if record["kind"] == "runtime.venue_cooldown_started"
        ]
        runtime.journal.close()


@pytest.mark.asyncio
async def test_binance_5022_exception_path_creates_cooldown_without_admission_block():
    raw_error = (
        'HTTP 400: {"code":-5022,"msg":"Due to the order could not be '
        'executed as maker, the Post Only order will be rejected."}'
    )

    with tempfile.TemporaryDirectory() as td:
        runtime = _runtime_with_metadata(td)
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
        runtime = _runtime_with_metadata(td)
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
        assert payload["source"] == "initial_entry"
        assert payload["block_scope"] == "symbol"
        assert payload["candidate_pair_id"] == "liteusdt:bybit->binance"
        assert payload["raw_error"] == "bybit retCode=110126 retMsg=must sign required agreement"
        assert payload["blocked_until_ms"] == 1778787600000
        assert payload["official_doc_url"] == ""
        assert payload["evidence_gap"] is True

        runtime.journal.close()


def test_symbol_scope_admission_block_filters_candidate_before_selection():
    with tempfile.TemporaryDirectory() as td:
        runtime = _runtime_with_metadata(td)
        runtime.journal.open()
        try:
            runtime.state.venue_entry_cooldowns["bybit:BALUSDT"] = {
                "venue": "bybit",
                "symbol": "BALUSDT",
                "reason": "insufficient_balance_admission_blocked",
                "raw_error": "bybit retCode=110007 retMsg=Available balance is insufficient",
                "blocked_until_ms": 1778787600000,
                "ttl_ms": 21_600_000,
                "official_doc_url": "https://bybit-exchange.github.io/docs/v5/error",
                "evidence_gap": False,
                "block_scope": "symbol",
            }
            blocked = _candidate("BALUSDT", "binance", "bybit")
            clean = _candidate("CLEANUSDT", "binance", "bybit")

            filtered = runtime._filter_candidates_by_entry_admission(
                [blocked, clean],
                now_ms=1778787000000,
                stage="candidate_prefilter",
            )

            assert [candidate.symbol for candidate in filtered] == ["CLEANUSDT"]
            assert runtime._last_entry_admission_filter_blockers == {
                "insufficient_balance_admission_blocked": 1
            }
            samples = runtime._last_entry_admission_filter_samples
            assert samples == [
                {
                    "candidate_pair_id": "balusdt:binance->bybit",
                    "pair_id": "balusdt:binance->bybit",
                    "symbol": "BALUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "venue": "bybit",
                    "reason": "insufficient_balance_admission_blocked",
                    "block_scope": "symbol",
                    "blocked_until_ms": 1778787600000,
                    "blocked_symbol": "",
                    "source": "entry_admission_cooldown",
                    "official_doc_url": "https://bybit-exchange.github.io/docs/v5/error",
                    "evidence_gap": False,
                    "stage": "candidate_prefilter",
                }
            ]
        finally:
            runtime.journal.close()


def test_route_abnormal_cooldown_filters_only_same_directed_route():
    with tempfile.TemporaryDirectory() as td:
        runtime = _runtime_with_metadata(td)
        runtime.journal.open()
        try:
            route_key = entry_route_key("HOMEUSDT", "binance", "bybit")
            runtime.state.venue_entry_cooldowns[route_key] = {
                "venue": "route",
                "symbol": "HOMEUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "reason": "route_abnormal_terminal_cooldown",
                "raw_error": "fallback_live_balanced",
                "blocked_until_ms": 1778787600000,
                "ttl_ms": 1_800_000,
                "official_doc_url": "",
                "evidence_gap": False,
                "block_scope": "route",
                "route_key": route_key,
                "source": "passive_close_abnormal_terminal",
            }
            blocked = _candidate("HOMEUSDT", "binance", "bybit")
            reversed_route = _candidate("HOMEUSDT", "bybit", "binance")
            clean = _candidate("CLEANUSDT", "binance", "bybit")

            filtered = runtime._filter_candidates_by_entry_admission(
                [blocked, reversed_route, clean],
                now_ms=1778787000000,
                stage="candidate_prefilter",
            )

            assert [candidate.symbol for candidate in filtered] == [
                "HOMEUSDT",
                "CLEANUSDT",
            ]
            assert filtered[0].long_venue == "bybit"
            assert runtime._last_entry_admission_filter_blockers == {
                "route_abnormal_terminal_cooldown": 1
            }
            sample = runtime._last_entry_admission_filter_samples[0]
            assert sample["block_scope"] == "route"
            assert sample["source"] == "passive_close_abnormal_terminal"
            payload = [
                record["payload"]
                for record in runtime.journal.read_all()
                if record["kind"] == "runtime.entry_admission_venue_degraded"
            ][-1]
            assert payload["reason"] == "route_abnormal_terminal_cooldown"
            assert payload["block_scope"] == "route"
        finally:
            runtime.journal.close()


def test_aster_legacy_venue_scope_headroom_cooldown_does_not_filter_other_aster_routes():
    with tempfile.TemporaryDirectory() as td:
        runtime = _runtime_with_metadata(td)
        runtime.journal.open()
        try:
            runtime.state.venue_entry_cooldowns["aster:*"] = {
                "venue": "aster",
                "symbol": "*",
                "blocked_symbol": "ESPORTSUSDT",
                "reason": "max_notional_admission_blocked",
                "raw_error": (
                    "max_notional_admission_blocked: requested_notional=23.9706 "
                    "remaining_openable_notional=0.0"
                ),
                "blocked_until_ms": 1778787600000,
                "ttl_ms": 21_600_000,
                "official_doc_url": "https://www.asterdex.com/",
                "evidence_gap": False,
                "block_scope": "venue",
                "source": "pre_entry_aster_precheck",
                "requested_notional": 23.9706,
                "remaining_openable_notional": 0.0,
                "notional_gap": 23.9706,
                "leverage": ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE,
                "headroom_source": "exchange_error_text",
                "order_role": "hedge",
            }
            blocked = _candidate("LABUSDT", "binance", "aster")
            clean = _candidate("CLEANUSDT", "binance", "bybit")

            filtered = runtime._filter_candidates_by_entry_admission(
                [blocked, clean],
                now_ms=1778787000000,
                stage="candidate_prefilter",
            )

            assert [candidate.symbol for candidate in filtered] == [
                "LABUSDT",
                "CLEANUSDT",
            ]
            assert runtime._last_entry_admission_filter_blockers == {}
            assert runtime._last_entry_admission_filter_samples == []
            assert not [
                record
                for record in runtime.journal.read_all()
                if record["kind"] == "runtime.entry_admission_venue_degraded"
            ]
        finally:
            runtime.journal.close()


def test_legacy_aster_venue_cooldown_without_headroom_is_nonblocking():
    with tempfile.TemporaryDirectory() as td:
        runtime = _runtime_with_metadata(td)
        runtime.journal.open()
        try:
            runtime.state.venue_entry_cooldowns["aster:*"] = {
                "venue": "aster",
                "symbol": "*",
                "blocked_symbol": "ESPORTSUSDT",
                "reason": "max_notional_admission_blocked",
                "raw_error": "max_notional_admission_blocked",
                "blocked_until_ms": 1778787600000,
                "ttl_ms": 21_600_000,
                "official_doc_url": "https://www.asterdex.com/",
                "evidence_gap": False,
                "block_scope": "venue",
                "source": "pre_entry_aster_precheck",
            }
            blocked = _candidate("LABUSDT", "binance", "aster")

            filtered = runtime._filter_candidates_by_entry_admission(
                [blocked],
                now_ms=1778787000000,
                stage="candidate_prefilter",
            )

            assert [candidate.symbol for candidate in filtered] == ["LABUSDT"]
            assert runtime._last_entry_admission_filter_blockers == {}
            assert runtime._last_entry_admission_filter_samples == []
            assert not [
                record
                for record in runtime.journal.read_all()
                if record["kind"] == "runtime.entry_admission_venue_degraded"
            ]
        finally:
            runtime.journal.close()
