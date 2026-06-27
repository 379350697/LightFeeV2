"""Live startup preflight tests matching Rust V1 bootstrap validation.

Rust references:
- src/main.rs (main startup sequence)
- src/app_runtime/bootstrap.rs (symbol resolution, credential validation)
- src/app_runtime/services.rs (adapter construction)
"""

import asyncio
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.bootstrap import (
    active_position_poll_enabled,
    active_position_poll_interval_ms,
    full_tick_ready,
    prepare_runtime_symbols,
    startup_market_warmup_ms,
    wall_clock_now_ms,
)
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.recovery_decision_core import RecoveryDecisionKind
from lightfee.engine.state import (
    ActiveMakerLeg,
    OpenPosition,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingEntry,
    PendingPassiveClose,
    PersistedCloseExecutionLeg,
)
from lightfee.engine.entry import EntryState
from lightfee.engine.entry_sync import EntryExecutionResult
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from tests.fake_adapters import FakeVenueAdapter, make_fake_fill, make_uncertain_error


def make_test_config(temp_dir: str) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            poll_interval_ms=200,
            sidecar_snapshot_path=str(Path(temp_dir) / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600_000,
            tick_failure_backoff_initial_ms=100,
            tick_failure_backoff_max_ms=1000,
        ),
        strategy=StrategyConfig(
            risk_monitor_enabled=False,
            max_concurrent_positions=2,
            local_l2_enabled=False,
            local_l2_ws_enabled=False,
            pending_entry_pre_submit_hedgeable_fill_guard_enabled=False,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(Path(temp_dir) / "events.jsonl"),
            snapshot_path=str(Path(temp_dir) / "state.json"),
        ),
        venues=[],
        symbols=["BTCUSDT"],
    )


_ADMISSIBLE_FIRST_FUNDING_MS = 1778787600000


def _admissible_dispatch_candidate(
    *,
    symbol: str,
    long_venue: str,
    short_venue: str,
) -> SimpleNamespace:
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


def _fake_adapters_for_venues(
    *venues: str,
    overrides: dict[Venue, FakeVenueAdapter] | None = None,
) -> dict[Venue, FakeVenueAdapter]:
    adapters = dict(overrides or {})
    for venue in venues:
        ven = Venue.from_str(venue)
        adapters.setdefault(ven, FakeVenueAdapter(ven))
    return adapters


class TestBootstrapHelpers:
    """Test bootstrap utility functions."""

    def test_wall_clock_now_ms_is_recent(self):
        now = wall_clock_now_ms()
        import time
        expected = int(time.time() * 1000)
        assert abs(now - expected) < 5000

    def test_full_tick_ready_no_backoff(self):
        assert full_tick_ready(None, 1000)

    def test_full_tick_ready_past_deadline(self):
        assert full_tick_ready(500, 1000)

    def test_full_tick_ready_before_deadline(self):
        assert not full_tick_ready(1500, 1000)

    def test_active_position_poll_interval_with_positions(self):
        interval = active_position_poll_interval_ms(EngineLifecycle.RUNNING, 3000, 1)
        assert interval <= 250

    def test_active_position_poll_interval_without_positions(self):
        interval = active_position_poll_interval_ms(EngineLifecycle.RUNNING, 3000, 0)
        assert interval == 3000

    def test_active_position_poll_enabled(self):
        # Fast poll enabled when lifecycle RUNNING + positions > 0 + fast < poll
        # poll_interval must be > 250 for fast poll to be faster
        assert active_position_poll_enabled(EngineLifecycle.RUNNING, 3000, 1)
        assert not active_position_poll_enabled(EngineLifecycle.RUNNING, 3000, 0)
        assert not active_position_poll_enabled(EngineLifecycle.BOOTING, 3000, 1)

    def test_startup_market_warmup_without_positions(self):
        warmup = startup_market_warmup_ms(
            EngineLifecycle.RUNNING, True, 0, 3000
        )
        assert warmup is not None
        assert 3000 <= warmup <= 10000

    def test_startup_market_warmup_skipped_with_positions(self):
        warmup = startup_market_warmup_ms(
            EngineLifecycle.RUNNING, True, 1, 3000
        )
        assert warmup is None

    @pytest.mark.asyncio
    async def test_prepare_runtime_symbols_returns_passthrough(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            result = await prepare_runtime_symbols(config)
            assert result is not None
            assert result["resolved_symbol_count"] == 1
            assert "BTCUSDT" in result["resolved_symbols"]


class TestRuntimePreflight:
    """Preflight checks before live trading starts."""

    @pytest.mark.asyncio
    async def test_startup_with_valid_config(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING

    @pytest.mark.asyncio
    async def test_startup_journals_run_id(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()

            # Run ID is a non-empty timestamp string
            assert runtime.state.run_id
            assert len(runtime.state.run_id) > 0
            assert runtime.state.started_at_ms > 0

    @pytest.mark.asyncio
    async def test_shutdown_graceful(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()
            await runtime.stop()

            # Journal is closed after stop
            assert runtime.journal._file is None

    @pytest.mark.asyncio
    async def test_bybit_trading_terms_reject_blocks_symbol_admission(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(
                config,
                venue_adapters=_fake_adapters_for_venues("bybit", "binance"),
            )
            runtime.journal.open()

            class RejectingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    return EntryExecutionResult(
                        route=ExecutionRoute.REJECTED,
                        state=EntryState.FAILED,
                        reject_reason=(
                            "bybit order failed: bybit retCode=110126 "
                            "retMsg=must sign required agreement"
                        ),
                    )

            executor = RejectingExecutor()
            runtime.entry_executor = executor
            candidate = _admissible_dispatch_candidate(
                symbol="LITEUSDT",
                long_venue="bybit",
                short_venue="binance",
            )

            first = await runtime._dispatch_entry(candidate, 1778787000000, price_hint=1.0)
            second = await runtime._dispatch_entry(candidate, 1778787001000, price_hint=1.0)

            assert first is True
            assert second is False
            assert executor.calls == 1
            key = "bybit:LITEUSDT"
            assert runtime.state.venue_entry_cooldowns[key]["reason"] == "bybit_trading_terms_required"
            kinds = [e["kind"] for e in runtime.journal.read_all()]
            assert "runtime.entry_admission_blocked" in kinds
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_bybit_precheck_terms_reject_blocks_before_maker_dispatch(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class PrecheckRejectingBybit(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BYBIT)
                    self.precheck_requests = []

                async def precheck_order_admission(self, request):
                    self.precheck_requests.append(request)
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "bybit order precheck failed: bybit retCode=110125 "
                        "retMsg=You must agree to the Crude Oil Trading Terms "
                        "before trading this contract.",
                    )

            class RecordingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    raise AssertionError("maker dispatch must be blocked by Bybit precheck")

            bybit = PrecheckRejectingBybit()
            runtime = LiveRuntime(
                config,
                venue_adapters=_fake_adapters_for_venues(
                    "binance",
                    "bybit",
                    overrides={Venue.BYBIT: bybit},
                ),
            )
            executor = RecordingExecutor()
            runtime.entry_executor = executor
            runtime.journal.open()
            candidate = _admissible_dispatch_candidate(
                symbol="CLUSDT",
                long_venue="binance",
                short_venue="bybit",
            )

            dispatched = await runtime._dispatch_entry(
                candidate,
                1778787000000,
                price_hint=100.0,
            )

            assert dispatched is False
            assert executor.calls == 0
            assert len(bybit.precheck_requests) == 1
            precheck_req = bybit.precheck_requests[0]
            assert precheck_req.venue == Venue.BYBIT
            assert precheck_req.symbol == "CLUSDT"
            assert precheck_req.side == Side.SELL
            assert precheck_req.reduce_only is False
            key = "bybit:CLUSDT"
            cooldown = runtime.state.venue_entry_cooldowns[key]
            assert cooldown["reason"] == "bybit_trading_terms_required"
            assert cooldown["source"] == "pre_entry_bybit_precheck"
            assert cooldown["block_scope"] == "symbol"
            assert cooldown["evidence_gap"] is False
            events = runtime.journal.read_all()
            blocked = [
                event["payload"]
                for event in events
                if event["kind"] == "runtime.entry_admission_blocked"
            ]
            assert blocked
            assert blocked[-1]["source"] == "pre_entry_bybit_precheck"
            assert blocked[-1]["candidate_pair_id"] == "clusdt:binance->bybit"
            assert not any(
                event["kind"] == "execution.entry_selected"
                for event in events
            )
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_bybit_precheck_ok_allows_entry_dispatch(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class PrecheckOkBybit(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BYBIT)
                    self.precheck_requests = []

                async def precheck_order_admission(self, request):
                    self.precheck_requests.append(request)
                    return {"status": "ok"}

            class RecordingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    return EntryExecutionResult(
                        route=ExecutionRoute.REJECTED,
                        state=EntryState.FAILED,
                        reject_reason="planner test terminal",
                    )

            bybit = PrecheckOkBybit()
            runtime = LiveRuntime(
                config,
                venue_adapters=_fake_adapters_for_venues(
                    "binance",
                    "bybit",
                    overrides={Venue.BYBIT: bybit},
                ),
            )
            executor = RecordingExecutor()
            runtime.entry_executor = executor
            runtime.journal.open()
            candidate = _admissible_dispatch_candidate(
                symbol="CLUSDT",
                long_venue="binance",
                short_venue="bybit",
            )

            dispatched = await runtime._dispatch_entry(
                candidate,
                1778787000000,
                price_hint=100.0,
            )

            assert dispatched is True
            assert executor.calls == 1
            assert len(bybit.precheck_requests) == 1
            assert bybit.precheck_requests[0].side == Side.SELL
            assert "bybit:CLUSDT" not in runtime.state.venue_entry_cooldowns
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_live_entry_prepares_leverage_before_dispatch(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class LeverageAwareAdapter(FakeVenueAdapter):
                def __init__(self, venue: Venue):
                    super().__init__(venue)
                    self.leverage_requests = []

                async def ensure_entry_leverage(self, symbol: str, leverage: int) -> None:
                    self.leverage_requests.append((symbol, leverage))

            binance = LeverageAwareAdapter(Venue.BINANCE)
            aster = LeverageAwareAdapter(Venue.ASTER)

            class RecordingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    return EntryExecutionResult(
                        route=ExecutionRoute.REJECTED,
                        state=EntryState.FAILED,
                        reject_reason="planner test terminal",
                    )

            runtime = LiveRuntime(
                config,
                venue_adapters=_fake_adapters_for_venues(
                    "binance",
                    "aster",
                    overrides={Venue.BINANCE: binance, Venue.ASTER: aster},
                ),
            )
            executor = RecordingExecutor()
            runtime.entry_executor = executor
            runtime.journal.open()
            candidate = _admissible_dispatch_candidate(
                symbol="HUSDT",
                long_venue="binance",
                short_venue="aster",
            )

            dispatched = await runtime._dispatch_entry(
                candidate,
                1778787000000,
                price_hint=0.57329,
            )

            assert dispatched is True
            assert executor.calls == 1
            assert binance.leverage_requests == [("HUSDT", config.strategy.live_target_leverage)]
            assert aster.leverage_requests == [("HUSDT", config.strategy.live_target_leverage)]
            events = runtime.journal.read_all()
            ready = [
                event["payload"]
                for event in events
                if event["kind"] == "execution.entry_leverage_ready"
            ]
            assert {event["venue"] for event in ready} == {"binance", "aster"}
            assert all(event["symbol"] == "HUSDT" for event in ready)
            assert not any(event["kind"] == "runtime.entry_dispatch_error" for event in events)
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_live_entry_blocks_when_leverage_prepare_fails(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class FailingLeverageAdapter(FakeVenueAdapter):
                async def ensure_entry_leverage(self, symbol: str, leverage: int) -> None:
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "HTTP 400: {\"code\":-2027,\"msg\":\"Exceeded the maximum allowable position at current leverage.\"}",
                    )

            class RecordingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    raise AssertionError("entry must be blocked before order dispatch")

            runtime = LiveRuntime(
                config,
                venue_adapters=_fake_adapters_for_venues(
                    "binance",
                    "aster",
                    overrides={Venue.BINANCE: FailingLeverageAdapter(Venue.BINANCE)},
                ),
            )
            runtime.entry_executor = RecordingExecutor()
            runtime.journal.open()
            candidate = _admissible_dispatch_candidate(
                symbol="HUSDT",
                long_venue="binance",
                short_venue="aster",
            )

            dispatched = await runtime._dispatch_entry(
                candidate,
                1778787000000,
                price_hint=0.57329,
            )

            assert dispatched is False
            assert runtime.entry_executor.calls == 0
            key = "binance:HUSDT"
            assert runtime.state.venue_entry_cooldowns[key]["reason"] == "leverage_admission_blocked"
            events = runtime.journal.read_all()
            assert any(
                event["kind"] == "execution.entry_leverage_unavailable"
                and event["payload"]["venue"] == "binance"
                and event["payload"]["reason"] == "leverage_admission_blocked"
                for event in events
            )
            assert any(
                event["kind"] == "runtime.entry_blocked_gate"
                and event["payload"]["gate"] == "entry_leverage_prepare"
                for event in events
            )
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_aster_max_leverage_reject_blocks_symbol_admission(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(
                config,
                venue_adapters=_fake_adapters_for_venues("aster", "bybit"),
            )
            runtime.journal.open()

            class RejectingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    return EntryExecutionResult(
                        route=ExecutionRoute.REJECTED,
                        state=EntryState.FAILED,
                        reject_reason=(
                            "HTTP 400: {\"code\":-2027,\"msg\":\"Exceeded the maximum allowable "
                            "position at current leverage.\"}"
                        ),
                    )

            executor = RejectingExecutor()
            runtime.entry_executor = executor
            candidate = _admissible_dispatch_candidate(
                symbol="ESPORTSUSDT",
                long_venue="aster",
                short_venue="bybit",
            )

            first = await runtime._dispatch_entry(candidate, 1778787000000, price_hint=1.0)
            second = await runtime._dispatch_entry(candidate, 1778787001000, price_hint=1.0)

            assert first is True
            assert second is False
            assert executor.calls == 1
            key = "aster:ESPORTSUSDT"
            assert runtime.state.venue_entry_cooldowns[key]["reason"] == "leverage_admission_blocked"
            runtime.journal.close()

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
                "MAXUSDT",
                'HTTP 400: {"code":-5018,"msg":"maximum notional value limit"}',
                "max_notional_admission_blocked",
                "https://asterdex.github.io/aster-api-website/futures/account%26trades/#remaining-openable-notional-value-user_data",
                True,
            ),
        ],
    )
    async def test_exchange_rule_reject_payload_records_evidence(
        self,
        venue,
        symbol,
        raw_error,
        expected_reason,
        official_doc_url,
        evidence_gap,
    ):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            short_venue = "bybit" if venue != "bybit" else "binance"
            runtime = LiveRuntime(
                config,
                venue_adapters=_fake_adapters_for_venues(venue, short_venue),
            )
            runtime.journal.open()

            class RejectingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    return EntryExecutionResult(
                        route=ExecutionRoute.REJECTED,
                        state=EntryState.FAILED,
                        reject_reason=raw_error,
                    )

            executor = RejectingExecutor()
            runtime.entry_executor = executor
            candidate = _admissible_dispatch_candidate(
                symbol=symbol,
                long_venue=venue,
                short_venue=short_venue,
            )

            assert await runtime._dispatch_entry(candidate, 1778787000000, price_hint=1.0) is True
            assert await runtime._dispatch_entry(candidate, 1778787001000, price_hint=1.0) is False
            assert executor.calls == 1

            if expected_reason == "post_only_would_take":
                assert f"{venue}:{symbol}" not in runtime.state.venue_entry_cooldowns
                payload = [
                    record["payload"]
                    for record in runtime.journal.read_all()
                    if record["kind"] == "runtime.entry_post_only_reject_cooldown"
                ][-1]
            else:
                payload = runtime.state.venue_entry_cooldowns[f"{venue}:{symbol}"]
            assert payload["reason"] == expected_reason
            assert payload["raw_error"] == raw_error[:500]
            assert payload["official_doc_url"] == official_doc_url
            assert payload["evidence_gap"] is evidence_gap
            assert payload["blocked_until_ms"] > 1778787000000
            runtime.journal.close()

    def test_live_main_wires_production_executors_for_real_runtime(self):
        """The live entrypoint wiring must remain active for real LiveRuntime."""
        from lightfee.apps.live import _wire_production_executors
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.entry_sync import EntrySyncExecutor
        from lightfee.engine.reconciliation import OrderReconciler

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config, venue_adapters={})

            wired = _wire_production_executors(runtime, {})

            assert wired is True
            assert isinstance(runtime.entry_executor, EntrySyncExecutor)
            assert isinstance(runtime.close_executor, CloseExecutor)
            assert isinstance(runtime.reconciler, OrderReconciler)
            assert runtime.supervisor.close_executor is runtime.close_executor

    @pytest.mark.asyncio
    async def test_shutdown_calls_per_adapter_shutdown(self):
        """V1 parity: LiveRuntime.stop() calls shutdown() on each venue adapter."""
        shutdown_calls: list[str] = []

        class ShutdownTrackingAdapter(FakeVenueAdapter):
            async def shutdown(self) -> None:
                shutdown_calls.append(self._venue.value)

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            adapters = {
                Venue.BINANCE: ShutdownTrackingAdapter(Venue.BINANCE),
                Venue.OKX: ShutdownTrackingAdapter(Venue.OKX),
                Venue.HYPERLIQUID: ShutdownTrackingAdapter(Venue.HYPERLIQUID),
            }
            runtime = LiveRuntime(config, venue_adapters=adapters)
            await runtime.start()
            await runtime.stop()

            assert sorted(shutdown_calls) == ["binance", "hyperliquid", "okx"]

    @pytest.mark.asyncio
    async def test_shutdown_adapter_error_does_not_block(self):
        """V1 parity: adapter shutdown errors are journaled, not re-raised."""
        class FailingShutdownAdapter(FakeVenueAdapter):
            async def shutdown(self) -> None:
                raise RuntimeError("adapter shutdown failure")

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            adapters = {Venue.BINANCE: FailingShutdownAdapter(Venue.BINANCE)}
            runtime = LiveRuntime(config, venue_adapters=adapters)
            await runtime.start()
            # Must not raise — error is journaled
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_shutdown_timeout_logs_blocked_adapter_task(self, caplog):
        """A stuck adapter shutdown is bounded and the blocked task is named."""
        class HangingShutdownAdapter(FakeVenueAdapter):
            async def shutdown(self) -> None:
                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.shutdown_grace_period_ms = 50
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: HangingShutdownAdapter(Venue.BINANCE)},
            )
            await runtime.start()

            with caplog.at_level("ERROR"):
                await asyncio.wait_for(runtime.stop(), timeout=0.3)

            messages = "\n".join(record.getMessage() for record in caplog.records)
            assert "shutdown stage=close_network" in messages
            assert "task=adapter.shutdown:binance" in messages
            assert "status=timeout" in messages

    @pytest.mark.asyncio
    async def test_shutdown_timeout_does_not_wait_for_cancel_hostile_adapter(self, caplog):
        """Timeout must bound a shutdown task even if it delays after cancellation."""
        release_cancelled_shutdown = asyncio.Event()
        saw_cancel = asyncio.Event()

        class CancelHostileShutdownAdapter(FakeVenueAdapter):
            async def shutdown(self) -> None:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    saw_cancel.set()
                    await release_cancelled_shutdown.wait()

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.shutdown_grace_period_ms = 50
            runtime = LiveRuntime(
                config,
                venue_adapters={
                    Venue.BINANCE: CancelHostileShutdownAdapter(Venue.BINANCE)
                },
            )
            await runtime.start()

            with caplog.at_level("ERROR"):
                await asyncio.wait_for(runtime.stop(), timeout=0.3)

            await asyncio.sleep(0)
            assert saw_cancel.is_set()
            assert runtime.journal._file is None
            messages = "\n".join(record.getMessage() for record in caplog.records)
            assert "task=adapter.shutdown:binance" in messages
            assert "status=timeout" in messages
            release_cancelled_shutdown.set()
            await asyncio.sleep(0)

    @pytest.mark.asyncio
    async def test_shutdown_stage_logs_follow_runtime_order(self, caplog):
        """Runtime shutdown phases must be logged in the order they execute."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()

            with caplog.at_level("INFO"):
                await runtime.stop()

            stages = [
                record.getMessage().split("shutdown stage=", 1)[1].split()[0]
                for record in caplog.records
                if "shutdown stage=" in record.getMessage()
            ]
            assert stages.index("close_network") < stages.index("flush_state")
            assert stages.index("flush_state") < stages.index("exit_complete")

    @pytest.mark.asyncio
    async def test_startup_emits_order_path_preflight_without_secrets(self):
        class PreflightTransport:
            def startup_preflight(self):
                return {
                    "venue": "hyperliquid",
                    "status": "failed",
                    "missing_dependencies": ["eth-account"],
                    "endpoint": "/exchange",
                    "product_type": "perp",
                    "secret": "must-not-leak",
                }

        class PreflightAdapter(FakeVenueAdapter):
            def __init__(self):
                super().__init__(Venue.HYPERLIQUID)
                self._transport = PreflightTransport()

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config, venue_adapters={Venue.HYPERLIQUID: PreflightAdapter()})
            await runtime.start()
            await runtime.stop()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            preflight = next(
                r["payload"] for r in records
                if r["kind"] == "startup.order_path_preflight"
            )
            assert preflight["venue"] == "hyperliquid"
            assert preflight["status"] == "failed"
            assert preflight["missing_dependencies"] == ["eth-account"]
            assert "secret" not in json.dumps(preflight)

            assert runtime.journal._file is None

    @pytest.mark.asyncio
    async def test_trading_preflight_logs_authorization_mode_and_exports_hl_disabled_reason(self):
        class TradingPreflightTransport:
            async def verify_live_trading_preflight(self):
                return {
                    "venue": "hyperliquid",
                    "status": "failed",
                    "trading_capability_trusted": False,
                    "reason": "api_wallet_authorization_unverified",
                    "authorization_mode": "api_wallet",
                    "configured_account_address": "0x000000000000000000000000000000000000beef",
                    "signer_address": "0x1111111111111111111111111111111111111111",
                    "api_wallet_authorization_verified": False,
                    "authorization_error": "L1 error: signer mismatch",
                    "auth_payload": "must-not-leak",
                    "auth_headers": {"Authorization": "must-not-leak"},
                    "authorization_header": "must-not-leak",
                    "signature": "must-not-leak",
                    "private_key": "must-not-leak",
                }

        class TradingPreflightAdapter(FakeVenueAdapter):
            def __init__(self):
                super().__init__(Venue.HYPERLIQUID)
                self._transport = TradingPreflightTransport()

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.HYPERLIQUID: TradingPreflightAdapter()},
            )
            await runtime.start()
            await runtime.stop()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            preflight = next(
                r["payload"] for r in records
                if r["kind"] == "startup.trading_preflight"
            )
            assert preflight["authorization_mode"] == "api_wallet"
            assert preflight["configured_account_address"].lower().endswith("beef")
            assert preflight["signer_address"].startswith("0x1111")
            assert preflight["authorization_error"] == "L1 error: signer mismatch"
            assert "auth_payload" not in preflight
            assert "auth_headers" not in preflight
            assert "authorization_header" not in preflight
            assert "signature" not in json.dumps(preflight)
            assert "private_key" not in json.dumps(preflight)
            assert runtime.state.hyperliquid_trading_disabled_reason == (
                "api_wallet_authorization_unverified"
            )

            from lightfee.engine.loop_control import current_state_export_path

            exported = json.loads(Path(current_state_export_path(config)).read_text())
            assert exported["hyperliquid_trading_disabled_reason"] == (
                "api_wallet_authorization_unverified"
            )

    @pytest.mark.asyncio
    async def test_startup_recovers_balanced_live_exchange_positions(self):
        """Startup must not report zero positions when exchanges already hold a pair."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            okx = FakeVenueAdapter(Venue.OKX)
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.02,
                    entry_price=65000.0,
                    observed_at_ms=1700000000000,
                )
            ]
            okx.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTC-USDT-SWAP",
                    side=Side.SELL,
                    quantity=0.02,
                    entry_price=65010.0,
                    observed_at_ms=1700000000000,
                )
            ]

            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )

            await runtime.start()
            await runtime.stop()

            assert len(runtime.state.open_positions) == 1
            pos = next(iter(runtime.state.open_positions.values()))
            assert pos.symbol == "BTCUSDT"
            assert pos.long_venue == Venue.BINANCE
            assert pos.short_venue == Venue.OKX
            assert pos.matched_quantity == pytest.approx(0.02)

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            assert any(r["kind"] == "recovery.live_detected" for r in records)

    @pytest.mark.asyncio
    async def test_startup_recovers_balanced_positions_from_bulk_private_scan(self):
        """V1 first scans all private positions instead of relying only on symbol probes."""

        class BulkOnlyAdapter(FakeVenueAdapter):
            def __init__(self, venue: Venue, positions: list[PositionSnapshot]):
                super().__init__(venue)
                self.positions = positions
                self.fetch_all_positions_call_count = 0

            async def fetch_all_positions(self) -> list[PositionSnapshot]:
                self.fetch_all_positions_call_count += 1
                return list(self.positions)

            async def fetch_position(self, symbol: str) -> PositionSnapshot:
                raise AssertionError("runtime should prefer fetch_all_positions")

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = BulkOnlyAdapter(
                Venue.BINANCE,
                [
                    PositionSnapshot(
                        venue=Venue.BINANCE,
                        symbol="BTCUSDT",
                        side=Side.BUY,
                        quantity=0.02,
                        entry_price=65000.0,
                        observed_at_ms=1700000000000,
                    )
                ],
            )
            okx = BulkOnlyAdapter(
                Venue.OKX,
                [
                    PositionSnapshot(
                        venue=Venue.OKX,
                        symbol="BTCUSDT",
                        side=Side.SELL,
                        quantity=0.02,
                        entry_price=65010.0,
                        observed_at_ms=1700000000000,
                    )
                ],
            )

            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )

            await runtime.start()
            await runtime.stop()

            assert binance.fetch_all_positions_call_count == 1
            assert okx.fetch_all_positions_call_count == 1
            assert len(runtime.state.open_positions) == 1

    @pytest.mark.asyncio
    async def test_startup_probe_symbols_include_static_config_when_resolved_subset(self):
        """Per-symbol fallback must not expand clean startup to the config universe."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.symbols = ["BTCUSDT", "ETHUSDT"]
            runtime = LiveRuntime(config)

            symbols = runtime._startup_position_probe_symbols(
                {"resolved_symbols": ["BTCUSDT"]}
            )

            assert symbols == []

    @pytest.mark.asyncio
    async def test_clean_live_position_probe_empty_symbols_does_not_probe_static_config(self):
        """Clean startup must not scan configured symbols when no recovery symbol exists."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.symbols = ["BTCUSDT", "ETHUSDT"]

            class NoBulkBinanceAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BINANCE)
                    self.fetch_position_symbols: list[str] = []

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT", "ETHUSDT"]

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=Venue.BINANCE,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            binance = NoBulkBinanceAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})

            runtime.journal.open()
            try:
                snapshots = await runtime._fetch_startup_live_position_snapshots([])
            finally:
                runtime.journal.close()

            assert snapshots == []
            assert binance.fetch_position_symbols == []
            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            skipped = [
                r["payload"] for r in records
                if r["kind"] == "recovery.live_position_static_config_probe_skipped"
            ]
            assert skipped[-1]["venue"] == "binance"
            assert skipped[-1]["static_symbol_count"] == 2
            assert skipped[-1]["max_static_symbol_count"] == 1
            assert skipped[-1]["decision"] == "skip_per_symbol_fallback"

    @pytest.mark.asyncio
    async def test_live_position_probe_skips_when_local_recovery_work_exists(self):
        """Pending recovery work owns its live legs; flat live discovery must not race it."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                )
            ]
            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})
            runtime.state.pending_entries["pending-1"] = PendingEntry(
                pending_id="pending-1",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                target_quantity=0.05,
                long_side=Side.BUY,
                short_side=Side.SELL,
                created_at_ms=1700000000000,
            )

            await runtime._recover_startup_live_positions(["BTCUSDT"], 1700000010000)

            assert binance.fetch_position_call_count == 0
            assert binance.place_order_call_count == 0
            assert len(runtime.state.open_positions) == 0

    @pytest.mark.asyncio
    async def test_live_position_fallback_probe_filters_unsupported_venue_symbols(self):
        """Fallback single-position probes must not ask a venue for delisted symbols."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)
                    self.fetch_position_symbols: list[str] = []

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"]

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=Venue.BITGET,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            bitget = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})

            snapshots = await runtime._fetch_startup_live_position_snapshots(
                ["BTCUSDT", "DELISTEDUSDT"]
            )

            assert snapshots == []
            assert bitget.fetch_position_symbols == ["BTCUSDT"]

    @pytest.mark.asyncio
    async def test_live_position_fallback_probe_loads_symbol_catalog_before_filtering(self):
        """A lazy venue catalog must be loaded before fallback per-symbol probes."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class LazySupportedAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)
                    self.loaded = False
                    self.fetch_position_symbols: list[str] = []

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"] if self.loaded else []

                async def ensure_supported_symbols_loaded(self) -> None:
                    self.loaded = True

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=Venue.BITGET,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            bitget = LazySupportedAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})

            await runtime._fetch_startup_live_position_snapshots(
                ["BTCUSDT", "DELISTEDUSDT"]
            )

            assert bitget.loaded is True
            assert bitget.fetch_position_symbols == ["BTCUSDT"]

    @pytest.mark.asyncio
    async def test_live_position_fallback_probe_aggregates_unsupported_symbols(self):
        """Unsupported catalog misses are a single diagnostic, not per-symbol errors."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)
                    self.fetch_position_symbols: list[str] = []

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"]

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=Venue.BITGET,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            bitget = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(
                    ["BTCUSDT", "DELISTEDUSDT", "OLDUSDT"]
                )
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            unsupported = [
                r for r in records
                if r["kind"] == "recovery.live_position_probe_unsupported_symbols"
            ]
            assert bitget.fetch_position_symbols == ["BTCUSDT"]
            assert len(unsupported) == 1
            assert unsupported[0]["payload"]["venue"] == "bitget"
            assert unsupported[0]["payload"]["unsupported_count"] == 2
            assert unsupported[0]["payload"]["sample_symbols"] == [
                "DELISTEDUSDT",
                "OLDUSDT",
            ]
            assert unsupported[0]["payload"]["endpoint"] == "fetch_position"
            assert unsupported[0]["payload"]["catalog_source"] == "adapter.supported_symbols"
            assert unsupported[0]["payload"]["catalog_supported_count"] == 1
            assert unsupported[0]["payload"]["sample_supported_symbols"] == ["BTCUSDT"]
            assert unsupported[0]["payload"]["symbol_mapping_samples"] == [
                {"symbol": "DELISTEDUSDT", "venue_symbol": "DELISTEDUSDT"},
                {"symbol": "OLDUSDT", "venue_symbol": "OLDUSDT"},
            ]
            assert unsupported[0]["payload"]["diagnostic_rate_limit_ms"] > 0
            assert not any(
                r["kind"] == "recovery.live_position_probe_symbol_skipped"
                for r in records
            )
            assert not any(
                r["kind"] == "recovery.live_position_probe_error"
                and r["payload"].get("reason") == "unsupported_symbol"
                for r in records
            )

    @pytest.mark.asyncio
    async def test_live_position_fallback_probe_rate_limits_unsupported_diagnostics(self):
        """Repeated unsupported catalog misses must not spam recovery diagnostics."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)
                    self.fetch_position_symbols: list[str] = []

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"]

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=Venue.BITGET,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            bitget = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})
            runtime.journal.open()
            try:
                for _ in range(2):
                    await runtime._fetch_startup_live_position_snapshots(
                        ["BTCUSDT", "DELISTEDUSDT", "OLDUSDT"]
                    )
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            unsupported = [
                r for r in records
                if r["kind"] == "recovery.live_position_probe_unsupported_symbols"
            ]
            assert bitget.fetch_position_symbols == ["BTCUSDT", "BTCUSDT"]
            assert len(unsupported) == 1
            assert unsupported[0]["payload"]["unsupported_count"] == 2

    @pytest.mark.asyncio
    async def test_live_position_fallback_probe_dedupes_unsupported_symbols_across_venues(self):
        """Catalog diagnostics are symbol-scope, not one blocker per venue fanout."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self, venue: Venue):
                    super().__init__(venue)
                    self.fetch_position_symbols: list[str] = []

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"]

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=self.venue,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            bitget = SupportedOnlyAdapter(Venue.BITGET)
            bybit = SupportedOnlyAdapter(Venue.BYBIT)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BITGET: bitget, Venue.BYBIT: bybit},
            )
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(
                    ["BTCUSDT", "DELISTEDUSDT", "OLDUSDT"]
                )
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            unsupported = [
                r for r in records
                if r["kind"] == "recovery.live_position_probe_unsupported_symbols"
            ]
            assert bitget.fetch_position_symbols == ["BTCUSDT"]
            assert bybit.fetch_position_symbols == ["BTCUSDT"]
            assert len(unsupported) == 1
            assert unsupported[0]["payload"]["classification"] == "catalog_diagnostic"
            assert unsupported[0]["payload"]["skipped_by_catalog"] == [
                "DELISTEDUSDT",
                "OLDUSDT",
            ]
            assert not any(
                r["kind"] == "recovery.live_position_probe_error"
                and r["payload"].get("decision")
                == "truth_unavailable_for_required_recovery"
                for r in records
            )

    @pytest.mark.asyncio
    async def test_live_position_fallback_probe_aggregates_different_unsupported_sets_across_venues(self):
        """Recovery catalog diagnostics are emitted once per recovery scope."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self, venue: Venue, supported: list[str]):
                    super().__init__(venue)
                    self._supported = supported
                    self.fetch_position_symbols: list[str] = []

                def supported_symbols(self) -> list[str]:
                    return list(self._supported)

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=self.venue,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            bitget = SupportedOnlyAdapter(Venue.BITGET, ["BTCUSDT"])
            bybit = SupportedOnlyAdapter(Venue.BYBIT, ["BTCUSDT", "OLDUSDT"])
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BITGET: bitget, Venue.BYBIT: bybit},
            )
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(
                    ["BTCUSDT", "DELISTEDUSDT", "OLDUSDT"]
                )
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            unsupported = [
                r for r in records
                if r["kind"] == "recovery.live_position_probe_unsupported_symbols"
            ]
            assert bitget.fetch_position_symbols == ["BTCUSDT"]
            assert bybit.fetch_position_symbols == ["BTCUSDT", "OLDUSDT"]
            assert len(unsupported) == 1
            payload = unsupported[0]["payload"]
            assert payload["classification"] == "catalog_diagnostic"
            assert payload["venues"] == ["bitget", "bybit"]
            assert payload["skipped_by_catalog"] == ["DELISTEDUSDT", "OLDUSDT"]
            assert payload["unsupported_by_venue"] == {
                "bitget": ["DELISTEDUSDT", "OLDUSDT"],
                "bybit": ["DELISTEDUSDT"],
            }
            assert not any(
                r["kind"] == "recovery.required_position_truth_unavailable"
                for r in records
            )

    @pytest.mark.asyncio
    async def test_live_position_fallback_probe_dedupes_same_unsupported_set_after_rate_window(
        self,
        monkeypatch,
    ):
        """Same unsupported symbol set should not re-emit after time-only changes."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)
                    self.fetch_position_symbols: list[str] = []

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"]

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=Venue.BITGET,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            now_ms = 1_780_000_000_000
            monkeypatch.setattr(
                "lightfee.engine.runtime.wall_clock_now_ms",
                lambda: now_ms,
            )
            bitget = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(
                    ["BTCUSDT", "DELISTEDUSDT", "OLDUSDT"]
                )
                now_ms += runtime._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS + 1
                await runtime._fetch_startup_live_position_snapshots(
                    ["BTCUSDT", "DELISTEDUSDT", "OLDUSDT"]
                )
                now_ms += runtime._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS + 1
                await runtime._fetch_startup_live_position_snapshots(
                    ["BTCUSDT", "DELISTEDUSDT", "NEWUSDT", "OLDUSDT"]
                )
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            unsupported = [
                r for r in records
                if r["kind"] == "recovery.live_position_probe_unsupported_symbols"
            ]
            assert bitget.fetch_position_symbols == ["BTCUSDT", "BTCUSDT", "BTCUSDT"]
            assert len(unsupported) == 2
            assert unsupported[0]["payload"]["skipped_by_catalog"] == [
                "DELISTEDUSDT",
                "OLDUSDT",
            ]
            assert unsupported[1]["payload"]["skipped_by_catalog"] == [
                "DELISTEDUSDT",
                "NEWUSDT",
                "OLDUSDT",
            ]

    @pytest.mark.asyncio
    async def test_live_position_probe_catalog_unavailable_is_journaled(self):
        """If the catalog cannot load, keep fallback behavior but record why."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class CatalogUnavailableAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)
                    self.fetch_position_symbols: list[str] = []

                async def ensure_supported_symbols_loaded(self) -> None:
                    raise RuntimeError("catalog load timed out")

                def supported_symbols(self) -> list[str]:
                    return []

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    self.fetch_position_symbols.append(symbol)
                    return PositionSnapshot(
                        venue=Venue.BITGET,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.0,
                        entry_price=0.0,
                        observed_at_ms=1700000010000,
                    )

            bitget = CatalogUnavailableAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(["BTCUSDT", "ETHUSDT"])
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            payload = next(
                r["payload"] for r in records
                if r["kind"] == "recovery.live_position_probe_catalog_unavailable"
            )
            assert bitget.fetch_position_symbols == ["BTCUSDT", "ETHUSDT"]
            assert payload["venue"] == "bitget"
            assert payload["catalog_source"] == "adapter.supported_symbols"
            assert payload["catalog_available"] is False
            assert payload["catalog_unavailable_reason"] == "ensure_supported_symbols_loaded_failed"
            assert payload["ensure_supported_symbols_available"] is True
            assert payload["supported_symbols_available"] is True
            assert payload["catalog_supported_count"] == 0
            assert payload["requested_symbols"] == ["BTCUSDT", "ETHUSDT"]
            assert payload["symbol_count"] == 2
            assert payload["decision"] == "probe_unfiltered"
            assert "catalog load timed out" in payload["catalog_error"]

    @pytest.mark.asyncio
    async def test_live_position_probe_error_records_catalog_unavailable_reason(self):
        """Probe errors must say whether missing catalog evidence is itself the gap."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class CatalogErrorAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)

                def supported_symbols(self) -> list[str]:
                    raise RuntimeError("catalog cache failed")

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    raise asyncio.TimeoutError()

            bitget = CatalogErrorAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(["BTCUSDT"])
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            payload = next(
                r["payload"] for r in records
                if r["kind"] == "recovery.live_position_probe_error"
            )
            assert payload["classification"] == "timeout"
            assert payload["catalog_available"] is False
            assert payload["catalog_source"] == "adapter.supported_symbols"
            assert payload["catalog_unavailable_reason"] == "supported_symbols_failed"
            assert payload["supported_symbols_available"] is True
            assert payload["ensure_supported_symbols_available"] is False
            assert payload["catalog_supported_count"] == 0
            assert "catalog cache failed" in payload["catalog_error"]

    @pytest.mark.asyncio
    async def test_live_position_probe_error_keeps_context_for_blank_exception(self):
        """Blank exception strings must still journal class, endpoint, and symbols."""
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class BlankProbeError(Exception):
                pass

            class FailingAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)
                    self._transport = SimpleNamespace(
                        _spec=SimpleNamespace(position_path="/api/v2/mix/position/single-position"),
                        _venue_symbol=lambda symbol: f"{symbol}_UMCBL",
                    )

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"]

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    raise BlankProbeError()

            bitget = FailingAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(["BTCUSDT"])
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            payload = next(
                r["payload"] for r in records
                if r["kind"] == "recovery.live_position_probe_error"
            )
            assert payload["exception_class"] == "BlankProbeError"
            assert payload["endpoint"] == "/api/v2/mix/position/single-position"
            assert payload["normalized_symbol"] == "BTCUSDT"
            assert payload["venue_symbol"] == "BTCUSDT_UMCBL"
            assert payload["error"]

    @pytest.mark.asyncio
    async def test_live_position_probe_timeout_has_explicit_classification(self):
        """Timeout probe errors must not depend on vague exception text."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class TimeoutAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BITGET)
                    self._transport = SimpleNamespace(
                        _spec=SimpleNamespace(position_path="/api/v2/mix/position/single-position"),
                        _venue_symbol=lambda symbol: f"{symbol}_UMCBL",
                    )

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"]

                async def fetch_all_positions(self):
                    return None

                async def fetch_position(self, symbol: str) -> PositionSnapshot:
                    raise asyncio.TimeoutError()

            bitget = TimeoutAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BITGET: bitget})
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(["BTCUSDT"])
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            payload = next(
                r["payload"] for r in records
                if r["kind"] == "recovery.live_position_probe_error"
            )
            assert payload["venue"] == "bitget"
            assert payload["normalized_symbol"] == "BTCUSDT"
            assert payload["venue_symbol"] == "BTCUSDT_UMCBL"
            assert payload["endpoint"] == "/api/v2/mix/position/single-position"
            assert payload["classification"] == "timeout"
            assert payload["exception_class"] == "TimeoutError"
            assert payload["probe_category"] == "private_positions"
            assert payload["catalog_source"] == "adapter.supported_symbols"
            assert payload["catalog_supported"] is True
            assert payload["catalog_supported_count"] == 1
            assert payload["cooldown_scope"] == "symbol:bitget:BTCUSDT:private_positions"
            assert payload["cooldown_ms"] == 0
            assert payload["error"]

    @pytest.mark.asyncio
    async def test_live_position_bulk_timeout_records_probe_budget_and_batch_evidence(self):
        """Bulk timeout evidence must identify endpoint, budget, timing, and batch scope."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.live_recovery_rest_probe_timeout_ms = 10

            class SlowBulkAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.OKX)
                    self._transport = SimpleNamespace(
                        _spec=SimpleNamespace(position_path="/api/v5/account/positions"),
                        _venue_symbol=lambda symbol: symbol,
                    )

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT", "ETHUSDT"]

                async def fetch_all_positions(self):
                    await asyncio.sleep(1)
                    return []

            okx = SlowBulkAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx})
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(
                    ["BTCUSDT", "ETHUSDT"]
                )
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            payload = next(
                r["payload"] for r in records
                if r["kind"] == "recovery.live_position_bulk_diagnostic_error"
            )
            assert payload["venue"] == "okx"
            assert payload["endpoint"] == "/api/v5/account/positions"
            assert payload["classification"] == "timeout"
            assert payload["truth_required_by"] == []
            assert payload["diagnostic_scope"] == "best_effort_bulk_positions"
            assert payload["blocking"] is False
            assert payload["decision"] == "running_with_nonblocking_health_diagnostic"
            assert payload["timeout_budget_ms"] == 10
            assert payload["timeout_budget_source"] == (
                "runtime.live_recovery_rest_probe_timeout_ms"
            )
            assert payload["timeout_trigger"] == "per_venue_wait_for"
            assert payload["global_timeout_triggered"] is False
            assert payload["global_timeout_budget_ms"] == 0
            assert payload["concurrency_limit"] == 8
            assert payload["probe_batch_index"] == 1
            assert payload["probe_batch_count"] == 1
            assert payload["probe_batch_symbol_count"] == 2
            assert payload["requested_symbols"] == ["BTCUSDT", "ETHUSDT"]
            assert payload["probe_started_at_ms"] > 0
            assert payload["probe_finished_at_ms"] >= payload["probe_started_at_ms"]
            assert payload["probe_elapsed_ms"] >= 0
            assert payload["global_probe_started_at_ms"] <= payload["probe_started_at_ms"]
            assert payload["global_probe_elapsed_ms"] >= payload["probe_elapsed_ms"]

    @pytest.mark.asyncio
    async def test_live_position_bulk_metadata_missing_keeps_inst_id_context(self):
        """Bulk OKX metadata failures must not lose the failing instId context."""
        from lightfee.venues.specs import okx_spec
        from lightfee.venues.transport import TransportError, TransportErrorCategory

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            class BulkMetadataMissingAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.OKX)
                    self._transport = type(
                        "Transport",
                        (),
                        {
                            "_spec": okx_spec(),
                            "_venue_symbol": lambda _self, symbol: (
                                str(symbol).replace("USDT", "-USDT-SWAP")
                            ),
                        },
                    )()

                def supported_symbols(self) -> list[str]:
                    return ["CHIP-USDT-SWAP"]

                async def fetch_all_positions(self):
                    raise TransportError(
                        TransportErrorCategory.NORMALIZATION_FAILURE,
                        (
                            "okx_contract_metadata_missing_ct_val "
                            "classification=metadata_missing "
                            "instId=CHIP-USDT-SWAP"
                        ),
                    )

            okx = BulkMetadataMissingAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx})
            runtime.journal.open()
            try:
                await runtime._fetch_startup_live_position_snapshots(["CHIPUSDT"])
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            payload = next(
                r["payload"] for r in records
                if r["kind"] == "recovery.live_position_bulk_probe_metadata_missing"
            )
            assert payload["classification"] == "metadata_missing"
            assert payload["venue"] == "okx"
            assert payload["endpoint"] == "/api/v5/account/positions"
            assert payload["exception_class"] == "TransportError"
            assert payload["normalized_symbol"] == "CHIPUSDT"
            assert payload["symbol"] == "CHIPUSDT"
            assert payload["venue_symbol"] == "CHIP-USDT-SWAP"
            assert payload["error"]

    @pytest.mark.asyncio
    async def test_local_l2_candidate_activation_filters_unsupported_venue_symbols(self):
        """Local-L2 bootstrap must not request books for non-trading contracts."""
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.strategy.local_l2_enabled = True
            config.strategy.local_l2_ws_enabled = False

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BINANCE)
                    self.loaded = False

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"] if self.loaded else []

                async def ensure_supported_symbols_loaded(self) -> None:
                    self.loaded = True

            binance = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})

            await runtime._ensure_l2_active_for_candidates(
                [
                    SimpleNamespace(
                        symbol="SYSUSDT",
                        long_venue="binance",
                        short_venue="binance",
                    )
                ],
                now_ms=1700000010000,
            )

            assert binance.loaded is True
            assert runtime.local_l2_runtime.get_book("binance", "SYSUSDT") is None

    @pytest.mark.asyncio
    async def test_tradeable_candidate_filter_removes_unsupported_pair_before_tracking(self):
        """Unsupported venue symbols must not reach shortlist/L2 tracking."""
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.strategy.local_l2_enabled = True

            class CatalogAdapter(FakeVenueAdapter):
                def __init__(self, venue: Venue, supported: list[str]):
                    super().__init__(venue)
                    self.loaded = False
                    self._supported = supported

                def supported_symbols(self) -> list[str]:
                    return self._supported if self.loaded else []

                async def ensure_supported_symbols_loaded(self) -> None:
                    self.loaded = True

            aster = CatalogAdapter(Venue.ASTER, ["BTCUSDT"])
            okx = CatalogAdapter(Venue.OKX, ["BTCUSDT", "RLSUSDT"])
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.ASTER: aster, Venue.OKX: okx},
            )
            runtime.journal.open()
            try:
                candidates = [
                    SimpleNamespace(
                        symbol="RLSUSDT",
                        long_venue="aster",
                        short_venue="okx",
                        pair_id="rlsusdt:aster->okx",
                    ),
                    SimpleNamespace(
                        symbol="BTCUSDT",
                        long_venue="aster",
                        short_venue="okx",
                        pair_id="btcusdt:aster->okx",
                    ),
                ]

                filtered = await runtime._filter_candidates_supported_by_venue_catalog(
                    candidates,
                )
            finally:
                runtime.journal.close()

            assert aster.loaded is True
            assert okx.loaded is True
            assert [candidate.symbol for candidate in filtered] == ["BTCUSDT"]

    @pytest.mark.asyncio
    async def test_local_l2_startup_bootstrap_runs_when_ws_disabled(self):
        """REST bootstrap must still use filtered target pairs when WS is disabled."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.strategy.local_l2_enabled = True
            config.strategy.entry_readiness_provider = "local_l2"
            config.strategy.local_l2_ws_enabled = False

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BINANCE)
                    self.loaded = False

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"] if self.loaded else []

                async def ensure_supported_symbols_loaded(self) -> None:
                    self.loaded = True

            binance = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})
            runtime.state.retained_local_l2_books = [
                {"venue": "binance", "symbol": "BTCUSDT"},
                {"venue": "binance", "symbol": "SYSUSDT"},
            ]

            started: list[dict] = []

            def capture_bootstrap(**kwargs):
                started.append(kwargs)

            runtime.l2_data_plane.start_background_bootstrap = capture_bootstrap

            runtime.journal.open()
            try:
                await runtime._activate_local_l2_phase(now_ms=1700000010000)
            finally:
                runtime.journal.close()

            assert binance.loaded is True
            assert started[0]["venue"] == "binance"
            assert started[0]["symbols"] == ["BTCUSDT"]
            assert runtime.local_l2_runtime.get_book("binance", "BTCUSDT") is not None
            assert runtime.local_l2_runtime.get_book("binance", "SYSUSDT") is None

    @pytest.mark.asyncio
    async def test_ws_bbo_effective_mode_skips_local_l2_startup_despite_legacy_flag(
        self,
    ):
        """WS BBO provider must suppress Local-L2 data-plane startup noise."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
            config.strategy.local_l2_enabled = True
            config.strategy.local_l2_ws_enabled = True

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BINANCE)
                    self.loaded = False

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"] if self.loaded else []

                async def ensure_supported_symbols_loaded(self) -> None:
                    self.loaded = True

            binance = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})
            runtime.state.retained_local_l2_books = [
                {"venue": "binance", "symbol": "BTCUSDT"},
            ]
            started: list[dict] = []
            runtime.l2_data_plane.start_background_bootstrap = (
                lambda **kwargs: started.append(kwargs)
            )

            runtime.journal.open()
            try:
                await runtime._activate_local_l2_phase(now_ms=1700000010000)
            finally:
                runtime.journal.close()

            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            assert started == []
            assert binance.loaded is False
            assert runtime.local_l2_runtime.get_book("binance", "BTCUSDT") is None
            assert all(
                not str(record["kind"]).startswith("runtime.local_l2_")
                for record in records
            )
            state = runtime.state.to_dict()
            effective = state["runtime_market_data_config"]
            assert effective["entry_readiness_provider_effective"] == "ws_bbo_quote_lease"
            assert effective["local_l2_configured_enabled"] is True
            assert effective["local_l2_effective_enabled"] is False

    @pytest.mark.asyncio
    async def test_ws_bbo_effective_mode_drops_local_l2_snapshot_restore_and_persistence(
        self,
    ):
        """WS BBO provider must not resurrect or persist Local-L2 snapshots."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
            config.strategy.local_l2_enabled = True
            config.strategy.local_l2_ws_enabled = True

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BINANCE)
                    self.loaded = False

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"] if self.loaded else []

                async def ensure_supported_symbols_loaded(self) -> None:
                    self.loaded = True

            binance = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})
            runtime.state.retained_local_l2_books = [
                {"venue": "binance", "symbol": "BTCUSDT"},
            ]
            runtime.state.local_l2_books_snapshot = [
                {
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "status": "hot",
                    "pool": "dropped",
                    "sequence": 10,
                    "last_update_id": 10,
                    "bids": [{"price": 50000.0, "quantity": 1.0}],
                    "asks": [{"price": 50100.0, "quantity": 1.0}],
                },
            ]
            runtime.state.local_l2_session_snapshot = [
                {"venue": "binance", "symbol": "BTCUSDT"},
            ]

            runtime.journal.open()
            try:
                await runtime._restore_local_l2_state()
            finally:
                runtime.journal.close()

            assert binance.loaded is False
            assert runtime.local_l2_runtime.get_book("binance", "BTCUSDT") is None
            assert runtime.state.retained_local_l2_books == []
            assert runtime.state.local_l2_books_snapshot == []
            assert runtime.state.local_l2_session_snapshot == []

            runtime.local_l2_runtime.ensure_book("binance", "BTCUSDT")
            runtime.state.local_l2_session_snapshot = [
                {"venue": "binance", "symbol": "BTCUSDT"},
            ]

            runtime._snapshot_local_l2_state()

            assert runtime.state.retained_local_l2_books == []
            assert runtime.state.local_l2_books_snapshot == []
            assert runtime.state.local_l2_session_snapshot == []

    @pytest.mark.asyncio
    async def test_local_l2_snapshot_restore_filters_unsupported_venue_symbols(self):
        """Persisted full-book snapshots must not resurrect non-trading contracts."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.strategy.local_l2_enabled = True
            config.strategy.entry_readiness_provider = "local_l2"

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BINANCE)
                    self.loaded = False

                def supported_symbols(self) -> list[str]:
                    return ["BTCUSDT"] if self.loaded else []

                async def ensure_supported_symbols_loaded(self) -> None:
                    self.loaded = True

            binance = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})
            runtime.state.local_l2_books_snapshot = [
                {
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "status": "hot",
                    "pool": "dropped",
                    "sequence": 10,
                    "last_update_id": 10,
                    "bids": [{"price": 50000.0, "quantity": 1.0}],
                    "asks": [{"price": 50100.0, "quantity": 1.0}],
                },
                {
                    "venue": "binance",
                    "symbol": "SYSUSDT",
                    "status": "rebuilding",
                    "pool": "dropped",
                    "sequence": 0,
                    "last_update_id": 0,
                    "bids": [],
                    "asks": [],
                },
            ]

            runtime.journal.open()
            try:
                await runtime._restore_local_l2_state()
            finally:
                runtime.journal.close()

            assert binance.loaded is True
            assert runtime.local_l2_runtime.get_book("binance", "BTCUSDT") is not None
            assert runtime.local_l2_runtime.get_book("binance", "SYSUSDT") is None

    @pytest.mark.asyncio
    async def test_local_l2_snapshot_restore_skips_unowned_transient_hot_exec_books(self):
        """Entry HOT books without retained/live owners must not survive restart."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.strategy.local_l2_enabled = True

            class SupportedOnlyAdapter(FakeVenueAdapter):
                def __init__(self):
                    super().__init__(Venue.BYBIT)
                    self.loaded = False

                def supported_symbols(self) -> list[str]:
                    return ["HUSDT"] if self.loaded else []

                async def ensure_supported_symbols_loaded(self) -> None:
                    self.loaded = True

            bybit = SupportedOnlyAdapter()
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.state.local_l2_books_snapshot = [
                {
                    "venue": "bybit",
                    "symbol": "HUSDT",
                    "status": "hot",
                    "pool": "hot_exec",
                    "sequence": 2978207,
                    "last_update_id": 2978207,
                    "bids": [{"price": 1.0, "quantity": 1.0}],
                    "asks": [{"price": 1.1, "quantity": 1.0}],
                },
            ]

            runtime.journal.open()
            try:
                await runtime._restore_local_l2_state()
            finally:
                runtime.journal.close()

            assert bybit.loaded is True
            assert runtime.local_l2_runtime.get_book("bybit", "HUSDT") is None

    @pytest.mark.asyncio
    async def test_housekeeping_recovers_balanced_live_positions_after_start(self):
        """A running clean state must not stay false-clean after live positions appear."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            okx = FakeVenueAdapter(Venue.OKX)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )

            await runtime.start()
            assert len(runtime.state.open_positions) == 0

            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.03,
                    entry_price=65000.0,
                    observed_at_ms=1700000005000,
                )
            ]
            okx.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTC-USDT-SWAP",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65015.0,
                    observed_at_ms=1700000005000,
                )
            ]

            await runtime._post_tick_housekeeping(1700000005000)
            await runtime.stop()

            assert len(runtime.state.open_positions) == 1
            pos = next(iter(runtime.state.open_positions.values()))
            assert pos.symbol == "BTCUSDT"
            assert pos.matched_quantity == pytest.approx(0.03)

    @pytest.mark.asyncio
    async def test_runtime_live_recovery_clears_stale_pending_without_open_block(self):
        """Balanced live recovery must release the startup pending-without-open block."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            bybit = FakeVenueAdapter(Venue.BYBIT)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
            )

            await runtime.start()
            assert len(runtime.state.open_positions) == 0

            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.RUNNING
            runtime.state.recovery_blocked_reason = (
                "startup_recovery_pending_work_without_open_positions"
            )
            runtime.state.recovery_blocked_at_ms = 1234
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.03,
                    entry_price=65000.0,
                    observed_at_ms=1700000005000,
                )
            ]
            bybit.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65015.0,
                    observed_at_ms=1700000005000,
                )
            ]

            await runtime._maybe_recover_clean_live_positions(1700000005000)
            await runtime.stop()

            assert len(runtime.state.open_positions) == 1
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            assert any(r["kind"] == "recovery.live_detected" for r in records)
            assert any(
                r["kind"] == "runtime.running"
                and r["payload"].get("reason")
                == "startup_recovery_completed_with_positions"
                for r in records
            )

    @pytest.mark.asyncio
    async def test_runtime_position_flat_truth_clears_unpaired_live_position_block(self):
        """A later flat position probe terminalizes a prior unpaired-position block."""

        class FlatBulkPositionAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str):
                return []

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = FlatBulkPositionAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = "unpaired_live_position"
            runtime.state.recovery_blocked_at_ms = 1234
            runtime.state.last_scan = {"recent_touched_symbols": ["BTCUSDT"]}

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.recovery_decision.clear_reason == "core_running_clean"
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_flat_truth_clears_owned_pending_entry_live_conflict_block(self):
        """A later account-flat probe releases an owned pending-entry live conflict."""

        class FlatBulkPositionAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str):
                return []

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = FlatBulkPositionAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = "owned_pending_entry_live_conflict"
            runtime.state.recovery_blocked_at_ms = 1234
            runtime.state.last_scan = {"recent_touched_symbols": ["HOMEUSDT"]}

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_unpaired_live_position_uses_account_truth_not_symbol_sweep(self):
        """Old live-artifact blockers clear from unfiltered account truth, not dirty symbols."""

        class AccountFlatTruthAdapter(FakeVenueAdapter):
            def __init__(self, venue: Venue):
                super().__init__(venue)
                self.account_open_order_calls = 0
                self.symbol_open_order_calls: list[str] = []

            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None):
                if symbol is None:
                    self.account_open_order_calls += 1
                    return []
                self.symbol_open_order_calls.append(symbol)
                raise RuntimeError(f"unsupported symbol: {symbol}")

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = AccountFlatTruthAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = "unpaired_live_position"
            runtime.state.recovery_blocked_at_ms = 1234
            runtime.state.last_scan = {
                "recent_touched_symbols": ["CL-USDT-SWAP", "CLUSDT"]
            }

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert bybit.account_open_order_calls == 1
            assert bybit.symbol_open_order_calls == []
            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_flat_account_truth_terminalizes_active_unpaired_records_before_clear(self):
        """Current production shape: stale active records must not block clean truth."""

        class AccountFlatTruthAdapter(FakeVenueAdapter):
            def __init__(self, venue: Venue):
                super().__init__(venue)
                self.account_open_order_calls = 0

            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None):
                if symbol is None:
                    self.account_open_order_calls += 1
                    return []
                raise AssertionError("stale risk alignment must use account order truth")

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = AccountFlatTruthAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.RUNNING
            runtime.state.recovery_blocked_reason = "unpaired_live_position"
            runtime.state.recovery_blocked_at_ms = 1234
            runtime.state.last_scan = {"recent_touched_symbols": ["HOMEUSDT"]}
            runtime.state.unpaired_live_position_recoveries = [
                {
                    "venue": "bybit",
                    "symbol": "HOMEUSDT",
                    "side": "buy",
                    "quantity": 5780.0,
                    "notional_quote": 23.99,
                    "first_seen_ms": 1700000000000,
                    "attempt_count": 0,
                    "next_attempt_ms": 1700000000000,
                    "last_error": "auto_disabled",
                    "terminal_status": "",
                    "owner_excluded": True,
                    "open_order_truth_available": False,
                    "cap_quote": 50.0,
                    "cap_ok": True,
                }
            ]

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert bybit.account_open_order_calls == 1
            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            assert (
                runtime.state.unpaired_live_position_recoveries[0]["terminal_status"]
                == "flat"
            )
            events = runtime.journal.read_all()
            assert any(
                event["kind"] == "runtime.stale_risk_state_alignment_started"
                for event in events
            )
            assert any(
                event["kind"] == "runtime.stale_risk_state_aligned"
                for event in events
            )
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_account_truth_prefers_aster_v3_adapter_open_orders(self):
        """Aster private truth is V3 adapter-owned, not the public FAPI transport."""

        class PublicTransport:
            async def _request(self, *args, **kwargs):
                raise AssertionError("runtime must not use Aster public transport")

        class AsterAccountTruthAdapter(FakeVenueAdapter):
            def __init__(self):
                super().__init__(Venue.ASTER)
                self._transport = PublicTransport()
                self.open_order_symbol = "not-called"

            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                self.open_order_symbol = symbol
                return []

        with tempfile.TemporaryDirectory() as td:
            runtime = LiveRuntime(
                make_test_config(td),
                venue_adapters={Venue.ASTER: AsterAccountTruthAdapter()},
            )

            truth = await runtime._collect_recovery_ledger_account_truth(1700000005000)

            assert truth["truth_available"] is True
            assert truth["open_orders"] == []
            adapter = runtime._venue_adapters[Venue.ASTER]
            assert adapter.open_order_symbol is None

    @pytest.mark.asyncio
    async def test_runtime_unpaired_live_position_flat_truth_requires_open_order_truth(self):
        """The unpaired-position release must not synthesize empty open orders."""

        class OpenOrderAdapter(FakeVenueAdapter):
            def supported_symbols(self) -> list[str]:
                return ["BTCUSDT"]

            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str):
                return [
                    {
                        "venue": self.venue.value,
                        "symbol": "BTCUSDT",
                        "side": "sell",
                        "quantity": 1.0,
                        "order_id": "still-live-order",
                    }
                ]

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = OpenOrderAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = "unpaired_live_position"
            runtime.state.recovery_blocked_at_ms = 1234
            runtime.state.last_scan = {"recent_touched_symbols": ["BTCUSDT"]}

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason
            assert runtime.state.recovery_blocked_reason != "unpaired_live_position"
            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.entry_allowed is False
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_unpaired_live_position_flat_truth_requires_available_order_truth(self):
        """Flat positions cannot clear an unpaired block when order truth errors."""

        class OpenOrderUnavailableAdapter(FakeVenueAdapter):
            def supported_symbols(self) -> list[str]:
                return ["BTCUSDT"]

            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str):
                raise RuntimeError("open order truth unavailable")

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = OpenOrderUnavailableAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = "unpaired_live_position"
            runtime.state.recovery_blocked_at_ms = 1234
            runtime.state.last_scan = {"recent_touched_symbols": ["BTCUSDT"]}

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason == "unpaired_live_position"
            assert runtime.state.recovery_blocked_at_ms == 1234
            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_live_mismatch_flat_truth_releases_fail_closed_latch(self):
        """A later account-truth pass closes a startup mismatch-flatten latch."""

        class FlatTruthAdapter(FakeVenueAdapter):
            def supported_symbols(self) -> list[str]:
                return ["BTCUSDT"]

            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str):
                return []

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = FlatTruthAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = (
                "live_position_mismatch_flatten_failed"
            )
            runtime.state.recovery_blocked_at_ms = 1234
            runtime.state.last_scan = {"recent_touched_symbols": ["BTCUSDT"]}

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            events = runtime.journal.read_all()
            assert any(
                event["kind"] == "recovery.ledger_clear"
                and event["payload"].get("reason") == "core_running_clean"
                for event in events
            )
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_flat_truth_clears_stale_risk_only_lifecycle_without_block_reason(self):
        """Production stale latch: core is clean but lifecycle stayed risk_only."""

        class FlatTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str):
                return []

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = FlatTruthAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            await runtime.start()

            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.RUNNING
            runtime.state.recovery_blocked_reason = None
            runtime.state.recovery_blocked_at_ms = 0
            runtime.state.last_scan = {"recent_touched_symbols": ["BTCUSDT"]}
            runtime._last_private_position_probe_ms = 0

            await runtime._post_tick_housekeeping(1700000005000)

            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            events = runtime.journal.read_all()
            assert any(
                event["kind"] == "recovery.lifecycle_clear"
                and event["payload"].get("reason")
                == "runtime_flat_truth_current_state_clean"
                for event in events
            )
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_runtime_flat_truth_clears_stale_fail_closed_lifecycle_without_block_reason(self):
        """Production stale latch: core clean but lifecycle and fail_closed stayed latched."""

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()

            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = None
            runtime.state.recovery_blocked_at_ms = 0

            runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": True,
                    "positions": [],
                    "open_orders": [],
                    "probe_evidence": [],
                    "errors": [],
                },
                now_ms=1700000005000,
                lifecycle_clear_reason="runtime_flat_truth_current_state_clean",
            )

            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            events = runtime.journal.read_all()
            assert any(
                event["kind"] == "recovery.lifecycle_clear"
                and event["payload"].get("reason")
                == "runtime_flat_truth_current_state_clean"
                for event in events
            )
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_stale_lifecycle_requires_open_order_truth_before_clear(self):
        """Flat positions alone are not enough when live open-order truth exists."""

        class OpenOrderTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str):
                return [
                    {
                        "venue": self.venue.value,
                        "symbol": symbol,
                        "side": "buy",
                        "quantity": 1.0,
                        "order_id": "live-open-order",
                    }
                ]

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = OpenOrderTruthAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            await runtime.start()

            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.RUNNING
            runtime.state.recovery_blocked_reason = None
            runtime.state.last_scan = {"recent_touched_symbols": ["BTCUSDT"]}
            runtime._last_private_position_probe_ms = 0

            await runtime._post_tick_housekeeping(1700000005000)

            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.entry_allowed is False
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_runtime_stale_lifecycle_requires_exchange_truth_before_clear(self):
        """The stale lifecycle release cannot run on unavailable truth."""

        class TruthUnavailableAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str):
                raise RuntimeError("open order truth unavailable")

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = TruthUnavailableAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            await runtime.start()

            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.RUNNING
            runtime.state.recovery_blocked_reason = None
            runtime.state.last_scan = {"recent_touched_symbols": ["BTCUSDT"]}
            runtime._last_private_position_probe_ms = 0

            await runtime._post_tick_housekeeping(1700000005000)

            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_runtime_position_flat_truth_does_not_clear_orphan_order_block(self):
        """Position-flat truth alone cannot clear an order-artifact blocker."""

        class FlatBulkPositionAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = FlatBulkPositionAdapter(Venue.BYBIT)
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = "orphan_maker_order"
            runtime.state.recovery_blocked_at_ms = 1234

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason == "orphan_maker_order"
            assert runtime.state.recovery_blocked_at_ms == 1234
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_live_mismatch_flatten_closes_core_ledger_block(self):
        """A successful runtime flatten must return to the core to clear its block."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = FakeVenueAdapter(Venue.BYBIT)
            bybit.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=53.6,
                    entry_price=0.4469,
                    observed_at_ms=1700000005000,
                ),
                PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=53.6,
                    entry_price=0.4469,
                    observed_at_ms=1700000005000,
                ),
            ]
            bybit.default_position_side = Side.BUY
            bybit.default_position_qty = 0.0
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.RUNNING
            runtime.state.recovery_blocked_reason = (
                "exchange_truth_recovery_ledger_blocked"
            )
            runtime.state.recovery_blocked_at_ms = 1234

            await runtime._maybe_recover_clean_live_positions(1700000005000)

            assert bybit.place_order_call_count == 1
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            events = runtime.journal.read_all()
            flattened_events = [
                event for event in events
                if event["kind"] == "recovery.live_mismatch_flattened"
            ]
            assert flattened_events
            flattened_payload = flattened_events[-1]["payload"]
            assert flattened_payload["owner_resolution"] == "unowned_live_artifact"
            assert flattened_payload["truth_required_by"] == "v1_recovery_decision_core"
            assert flattened_payload["probe_family"] == "runtime_live_position_probe"
            assert flattened_payload["post_cleanup_truth"]["truth_available"] is True
            assert flattened_payload["post_cleanup_truth"]["positions"] == []
            assert flattened_payload["positions"][0]["cleanup_intent_id"].startswith(
                "live-recovery:runtime_live_position_probe:BTCUSDT:bybit:"
            )
            assert flattened_payload["positions"][0]["post_cleanup_truth"] == {
                "truth_available": True,
                "position_qty": 0.0,
                "side": "",
            }
            clears = [
                event for event in events
                if event["kind"] == "recovery.ledger_clear"
            ]
            assert clears[-1]["payload"]["decision"] in {
                "RUNNING_CLEAN",
                "RUNNING_WITH_EVIDENCE_GAP",
            }
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_live_mismatch_flatten_failure_stays_blocked_after_housekeeping_cleaner(self):
        """A failed runtime live flatten is a live-artifact block, not stale state."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = FakeVenueAdapter(Venue.BYBIT)
            live_position = PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="BTCUSDT",
                side=Side.BUY,
                quantity=53.6,
                entry_price=0.4469,
                observed_at_ms=1700000005000,
            )
            bybit.position_snapshots = [
                live_position,
                live_position,
                live_position,
                live_position,
                live_position,
                live_position,
                live_position,
            ]
            bybit.default_position_side = Side.BUY
            bybit.default_position_qty = 53.6
            bybit.place_order_outcomes = [
                make_uncertain_error("cleanup timeout 1"),
                make_uncertain_error("cleanup timeout 2"),
                make_uncertain_error("cleanup timeout 3"),
            ]
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()

            await runtime._maybe_recover_clean_live_positions(1700000005000)
            from lightfee.engine.recovery import (
                clear_legacy_recovery_block_via_core,
            )
            from lightfee.engine.recovery_decision_core import (
                RecoveryDecision,
                RecoveryDecisionKind,
                RecoveryEvidenceClass,
            )
            core_decision = RecoveryDecision(
                kind=RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP,
                evidence_class=RecoveryEvidenceClass.PARTIAL_EVIDENCE_GAP,
                entry_allowed=True,
                clear_previous_block=True,
            )
            cleared = clear_legacy_recovery_block_via_core(
                runtime.state,
                core_decision,
                runtime.journal,
            )

            assert bybit.place_order_call_count == 3
            assert cleared is False
            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason == (
                "live_position_mismatch_flatten_failed"
            )
            events = runtime.journal.read_all()
            failed_events = [
                event for event in events
                if event["kind"] == "recovery.live_mismatch_flatten_failed"
            ]
            assert failed_events
            failed_payload = failed_events[-1]["payload"]
            assert failed_payload["owner_resolution"] == "unowned_live_artifact"
            assert failed_payload["truth_required_by"] == "v1_recovery_decision_core"
            assert failed_payload["probe_family"] == "runtime_live_position_probe"
            assert failed_payload["post_cleanup_truth"]["truth_available"] is True
            assert failed_payload["post_cleanup_truth"]["positions"] == [
                {
                    "venue": "bybit",
                    "symbol": "BTCUSDT",
                    "position_qty": 53.6,
                    "side": "buy",
                }
            ]
            assert failed_payload["failed_positions"][0]["cleanup_intent_id"].startswith(
                "live-recovery:runtime_live_position_probe:BTCUSDT:bybit:"
            )
            assert failed_payload["failed_positions"][0]["post_cleanup_truth"] == {
                "truth_available": True,
                "position_qty": 53.6,
                "side": "buy",
            }
            assert not any(
                event["kind"] == "recovery.legacy_block_cleared"
                for event in events
            )
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_runtime_live_mismatch_cleanup_uses_fresh_cids_across_probe_cycles(self):
        """Repeated recovery probes must not resubmit already-used Bybit cleanup CIDs."""

        class DuplicateRecordingAdapter(FakeVenueAdapter):
            def __init__(self):
                super().__init__(Venue.BYBIT)
                self.cleanup_client_order_ids: list[str] = []

            async def place_order(self, request):
                self.cleanup_client_order_ids.append(request.client_order_id)
                return await super().place_order(request)

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            bybit = DuplicateRecordingAdapter()
            live_position = PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="BTCUSDT",
                side=Side.BUY,
                quantity=53.6,
                entry_price=0.4469,
                observed_at_ms=1700000005000,
            )
            bybit.position_snapshots = [
                live_position,
                live_position,
                live_position,
                live_position,
            ]
            bybit.default_position_side = Side.BUY
            bybit.default_position_qty = 53.6
            bybit.place_order_outcomes = [
                make_uncertain_error(
                    "bybit order failed: bybit retCode=110072 "
                    "retMsg=OrderLinkedID is duplicate"
                ),
                make_uncertain_error(
                    "bybit order failed: bybit retCode=110072 "
                    "retMsg=OrderLinkedID is duplicate"
                ),
            ]
            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            runtime.journal.open()

            await runtime._flatten_startup_live_position_mismatches(
                [("BTCUSDT", live_position)],
                1700000005000,
                source="runtime_live_position_probe",
            )
            await runtime._flatten_startup_live_position_mismatches(
                [("BTCUSDT", live_position)],
                1700000020000,
                source="runtime_live_position_probe",
            )

            assert len(bybit.cleanup_client_order_ids) == 2
            assert (
                bybit.cleanup_client_order_ids[0]
                != bybit.cleanup_client_order_ids[1]
            )
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_startup_flattens_unpaired_live_exchange_position(self):
        """Unpaired live exposure should be reduce-only flattened, not shown as clean."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                ),
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                )
            ]
            binance.default_position_side = Side.BUY
            binance.default_position_qty = 0.0

            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})

            await runtime.start()
            await runtime.stop()

            assert len(runtime.state.open_positions) == 0
            assert binance.place_order_call_count == 1
            assert binance.last_request is not None
            assert binance.last_request.reduce_only is True
            assert binance.last_request.side == Side.SELL
            assert binance.last_request.quantity == pytest.approx(0.05)
            assert binance.last_request.price is None
            assert binance.last_request.client_order_id
            assert runtime.state.risk_mode != GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason is None

    def test_startup_recovery_ledger_blocks_local_flat_with_live_open_order(self):
        """Local flat is not accepted while exchange truth has a live maker order."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()

            ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": True,
                    "positions": [],
                    "open_orders": [
                        {
                            "venue": "bybit",
                            "symbol": "TRXUSDT",
                            "side": "buy",
                            "quantity": 72.0,
                            "price": 0.33044,
                            "reduce_only": False,
                            "order_id": "a84df707-efb3-4e40-bab1-641a4eb0f3d4",
                        }
                    ],
                },
                now_ms=1778787000000,
            )

            assert any(item.blocking for item in ledger.work_items)
            assert ledger.work_items[0].kind == "orphan_maker_order"
            assert runtime.state.recovery_blocked_reason == "orphan_maker_order"
            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            events = [
                event for event in runtime.journal.read_all()
                if event["kind"] == "recovery.ledger_blocked"
            ]
            assert events[-1]["payload"]["work_items"][0]["kind"] == "orphan_maker_order"
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_startup_builds_recovery_ledger_before_running_with_live_open_order(self):
        """Startup should probe owner-evidence symbols before entering RUNNING."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.symbols = ["TRXUSDT"]

            class OpenOrderAdapter(FakeVenueAdapter):
                async def fetch_open_orders(self, symbol: str):
                    assert symbol == "TRXUSDT"
                    return [
                        {
                            "venue": "bybit",
                            "symbol": "TRXUSDT",
                            "side": "buy",
                            "quantity": 72.0,
                            "price": 0.33044,
                            "reduce_only": False,
                            "order_id": "a84df707-efb3-4e40-bab1-641a4eb0f3d4",
                        }
                    ]

            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BYBIT: OpenOrderAdapter(Venue.BYBIT)},
            )
            runtime.journal.open()
            runtime.journal.append(
                "entry.maker_submitted",
                {
                    "entry_id": "entry-trx",
                    "symbol": "TRXUSDT",
                    "order_id": "a84df707-efb3-4e40-bab1-641a4eb0f3d4",
                    "client_order_id": "entry-1780595698673-TRXUSDT",
                },
            )
            runtime.journal.close()

            await runtime.start()

            assert runtime.recovery_ledger is not None
            item = runtime.recovery_ledger.work_items[0]
            assert item.kind == "owned_pending_entry"
            assert item.owner.owner_id == "entry-trx"
            assert item.owner.confidence == "probable"
            assert runtime.state.recovery_blocked_reason == (
                "owned_recovery_work"
            )
            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert [
                event["kind"] for event in runtime.journal.read_all()
                if event["kind"] == "recovery.ledger_blocked"
            ]

            await runtime.stop()

    def test_startup_recovery_ledger_symbols_include_journal_owner_evidence(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.symbols = ["BTCUSDT", "ETHUSDT", "TRXUSDT"]
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.journal.append(
                "entry.maker_submitted",
                {
                    "entry_id": "entry-trx",
                    "symbol": "TRXUSDT",
                    "order_id": "maker-order",
                    "client_order_id": "maker-client",
                },
            )

            assert runtime._startup_recovery_ledger_symbols(
                {"resolved_symbols": config.symbols}
            ) == ["TRXUSDT"]
            runtime.journal.close()

    def test_startup_recovery_ledger_symbols_include_terminal_order_owner_facts(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.symbols = ["SEIUSDT", "TRXUSDT", "WLDUSDT"]
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.journal.append(
                "entry.maker_submitted",
                {
                    "entry_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "order_id": "old-maker-order",
                    "client_order_id": "old-maker-client",
                },
            )
            runtime.journal.append(
                "pending_entry.pending_entry_finalized",
                {
                    "entry_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "position_id": None,
                },
            )
            runtime.journal.append(
                "entry.maker_submitted",
                {
                    "entry_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "order_id": "live-maker-order",
                    "client_order_id": "live-maker-client",
                },
            )

            assert runtime._startup_recovery_ledger_symbols(
                {"resolved_symbols": config.symbols}
            ) == ["SEIUSDT", "WLDUSDT"]
            runtime.journal.close()

    def test_startup_recovery_ledger_symbols_include_positive_fill_conflict_owner(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.symbols = ["BTCUSDT", "HOMEUSDT"]
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.journal.append(
                "pending_entry.positive_fill_live_truth_conflict",
                {
                    "entry_id": "entry-home",
                    "symbol": "HOMEUSDT",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                    "live_long_quantity": 0.0,
                    "live_short_quantity": 1600.0,
                    "live_balanced_quantity": 0.0,
                },
            )

            assert runtime._startup_recovery_ledger_symbols(
                {"resolved_symbols": config.symbols}
            ) == ["HOMEUSDT"]
            runtime.journal.close()

    def test_recovery_ledger_uses_journal_owner_evidence_for_local_flat_order(self):
        """Journal order evidence should reconstruct ownership after local state is flat."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.journal.append(
                "entry.maker_submitted",
                {
                    "entry_id": "entry-trx",
                    "symbol": "TRXUSDT",
                    "order_id": "maker-order",
                    "client_order_id": "maker-client",
                },
            )

            ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": True,
                    "positions": [],
                    "open_orders": [
                        {
                            "venue": "bybit",
                            "symbol": "TRXUSDT",
                            "side": "buy",
                            "quantity": 72.0,
                            "reduce_only": False,
                            "order_id": "maker-order",
                        }
                    ],
                },
                now_ms=1778787000000,
            )

            item = ledger.work_items[0]
            assert item.kind == "owned_pending_entry"
            assert item.owner.confidence == "probable"
            assert item.owner.owner_id == "entry-trx"
            runtime.journal.close()

    def test_recovery_ledger_keeps_terminal_journal_order_owned_if_exchange_order_is_live(self):
        """V1 order facts survive terminal local events until exchange truth is flat."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.journal.append(
                "order.passive_submitted",
                {
                    "entry_id": "entry-stable",
                    "symbol": "STABLEUSDT",
                    "venue": "bybit",
                    "order_id": "5b9dd1ec-c1be-4f9b-bf35-b34087be810a",
                    "client_order_id": "stable-maker-client",
                },
            )
            runtime.journal.append(
                "entry.aborted",
                {
                    "entry_id": "entry-stable",
                    "symbol": "STABLEUSDT",
                    "reason": "exchange_rejected",
                },
            )

            ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": True,
                    "positions": [],
                    "open_orders": [
                        {
                            "venue": "bybit",
                            "symbol": "STABLEUSDT",
                            "side": "buy",
                            "quantity": 680.0,
                            "price": 0.035049,
                            "reduce_only": False,
                            "order_id": "5b9dd1ec-c1be-4f9b-bf35-b34087be810a",
                        }
                    ],
                },
                now_ms=1780665150176,
            )

            item = ledger.work_items[0]
            assert item.kind == "owned_pending_entry"
            assert item.owner.owner_id == "entry-stable"
            assert item.owner.confidence == "probable"
            assert item.decision.reason == "live_order_has_runtime_owner"
            assert runtime.state.recovery_blocked_reason == (
                "owned_recovery_work"
            )
            runtime.journal.close()

    def test_clean_recovery_ledger_clears_previous_ledger_blocker(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.state.recovery_blocked_reason = "exchange_truth_recovery_ledger_blocked"
            runtime.state.recovery_blocked_at_ms = 123
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY

            ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": True,
                    "positions": [],
                    "open_orders": [],
                },
                now_ms=1778787000000,
            )

            assert not any(item.blocking for item in ledger.work_items)
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            runtime.journal.close()

    def test_required_truth_timeout_blocker_clears_after_clean_truth(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.state.pending_entries["entry-timeout"] = {
                "pending_id": "entry-timeout",
                "symbol": "TRXUSDT",
                "long_venue": "bybit",
                "short_venue": "okx",
            }

            blocked_ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": False,
                    "positions": [],
                    "open_orders": [],
                    "probe_evidence": [
                        {
                            "venue": "okx",
                            "symbol": "TRXUSDT",
                            "endpoint": "fetch_position",
                            "classification": "timeout",
                            "error": "position truth timed out",
                        }
                    ],
                },
                now_ms=1778787000000,
            )

            assert any(item.blocking for item in blocked_ledger.work_items)
            assert runtime.recovery_decision is not None
            assert (
                runtime.recovery_decision.kind
                == RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH
            )
            assert runtime.state.recovery_blocked_reason == (
                "truth_unavailable_for_required_recovery"
            )
            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY

            runtime.state.pending_entries.clear()
            clean_ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": True,
                    "positions": [],
                    "open_orders": [],
                },
                now_ms=1778787001000,
            )

            assert not any(item.blocking for item in clean_ledger.work_items)
            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            events = runtime.journal.read_all()
            assert any(
                event["kind"] == "recovery.ledger_blocked"
                and event["payload"]["reason"]
                == "truth_unavailable_for_required_recovery"
                for event in events
            )
            clears = [
                event for event in events
                if event["kind"] == "recovery.ledger_clear"
            ]
            assert clears[-1]["payload"]["reason"] == "core_running_clean"
            runtime.journal.close()

    def test_complete_flat_truth_clears_previous_live_artifact_blocker(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.state.recovery_blocked_reason = "unpaired_live_position"
            runtime.state.recovery_blocked_at_ms = 123
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY

            ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": True,
                    "positions": [],
                    "open_orders": [],
                },
                now_ms=1778787000000,
            )

            assert not any(item.blocking for item in ledger.work_items)
            assert runtime.recovery_decision is not None
            assert runtime.recovery_decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
            assert runtime.recovery_decision.clear_reason == "core_running_clean"
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            runtime.journal.close()

    def test_evidence_gap_clears_previous_ledger_blocker_through_core(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()
            runtime.state.recovery_blocked_reason = "exchange_truth_recovery_ledger_blocked"
            runtime.state.recovery_blocked_at_ms = 123
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY

            ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
                {
                    "truth_available": False,
                    "positions": [],
                    "open_orders": [],
                    "probe_evidence": [
                        {
                            "venue": "bybit",
                            "symbol": "TRXUSDT",
                            "endpoint": "fetch_position",
                            "error": "timeout",
                        }
                    ],
                },
                now_ms=1778787000000,
            )

            assert not any(item.blocking for item in ledger.work_items)
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            events = runtime.journal.read_all()
            assert [
                event for event in events
                if event["kind"] == "recovery.ledger_blocked"
            ] == []
            clears = [
                event for event in events
                if event["kind"] == "recovery.ledger_clear"
            ]
            assert clears[-1]["payload"]["reason"] == "core_evidence_gap_no_local_work"
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_startup_preserves_live_mismatch_blocked_reason_when_snapshot_is_clean(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            SnapshotStore(config.persistence.snapshot_path).write({
                "lifecycle": "running",
                "risk_mode": "running",
                "recovery_blocked_reason": "live_position_mismatch_flatten_failed",
                "recovery_blocked_at_ms": 1234,
                "open_positions": [],
                "pending_entries": [],
                "pending_closes": [],
                "pending_passive_closes": [],
            })

            runtime = LiveRuntime(config, venue_adapters={})
            await runtime.start()
            await runtime.stop()

            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason == (
                "live_position_mismatch_flatten_failed"
            )
            assert runtime.state.recovery_blocked_at_ms == 1234

    @pytest.mark.asyncio
    async def test_startup_live_mismatch_flatten_closes_core_ledger_block(self):
        """Startup mismatch flatten must not skip the core-owned clear decision."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            SnapshotStore(config.persistence.snapshot_path).write({
                "lifecycle": "risk_only",
                "risk_mode": "running",
                "recovery_blocked_reason": "exchange_truth_recovery_ledger_blocked",
                "recovery_blocked_at_ms": 1234,
                "open_positions": [],
                "pending_entries": [],
                "pending_closes": [],
                "pending_passive_closes": [],
            })
            bybit = FakeVenueAdapter(Venue.BYBIT)
            bybit.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=53.6,
                    entry_price=0.4469,
                    observed_at_ms=1700000010000,
                ),
                PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=53.6,
                    entry_price=0.4469,
                    observed_at_ms=1700000010000,
                ),
            ]
            bybit.default_position_side = Side.BUY
            bybit.default_position_qty = 0.0

            runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
            await runtime.start()
            await runtime.stop()

            assert bybit.place_order_call_count == 1
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            records = [
                json.loads(line)
                for line in Path(config.persistence.event_log_path).read_text().splitlines()
                if line.strip()
            ]
            assert any(
                event["kind"] == "recovery.live_mismatch_flattened"
                for event in records
            )
            flattened = [
                event["payload"] for event in records
                if event["kind"] == "recovery.live_mismatch_flattened"
            ][-1]
            assert flattened["owner_resolution"] == "unowned_live_artifact"
            assert flattened["truth_required_by"] == "v1_recovery_decision_core"
            assert flattened["probe_family"] == "startup_live_position_probe"
            assert flattened["post_cleanup_truth"]["truth_available"] is True
            assert flattened["post_cleanup_truth"]["positions"] == []
            assert flattened["positions"][0]["cleanup_intent_id"].startswith(
                "live-recovery:startup_live_position_probe:BTCUSDT:bybit:"
            )
            assert flattened["positions"][0]["post_cleanup_truth"] == {
                "truth_available": True,
                "position_qty": 0.0,
                "side": "",
            }
            assert any(
                event["kind"] == "recovery.ledger_clear"
                for event in records
            )

    @pytest.mark.asyncio
    async def test_startup_flattens_size_mismatched_live_exchange_positions(self):
        """A long/short pair with unequal size is mismatch exposure, not recovery."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            okx = FakeVenueAdapter(Venue.OKX)
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                ),
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                )
            ]
            okx.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTC-USDT-SWAP",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65010.0,
                    observed_at_ms=1700000010000,
                ),
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTC-USDT-SWAP",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65010.0,
                    observed_at_ms=1700000010000,
                )
            ]
            binance.default_position_side = Side.BUY
            binance.default_position_qty = 0.0
            okx.default_position_side = Side.SELL
            okx.default_position_qty = 0.0

            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )

            await runtime.start()
            await runtime.stop()

            assert len(runtime.state.open_positions) == 0
            assert binance.place_order_call_count == 1
            assert okx.place_order_call_count == 1
            assert binance.last_request is not None
            assert okx.last_request is not None
            assert binance.last_request.reduce_only is True
            assert okx.last_request.reduce_only is True
            assert binance.last_request.side == Side.SELL
            assert okx.last_request.side == Side.BUY
            assert binance.last_request.price is None
            assert okx.last_request.price is None
            assert binance.last_request.client_order_id
            assert okx.last_request.client_order_id
            assert runtime.state.risk_mode != GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason is None

    @pytest.mark.asyncio
    async def test_startup_blocks_unpaired_live_exchange_position_when_flatten_fails(self):
        """If all V1 mismatch-flatten attempts fail, runtime fail-closes visibly."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                )
            ]
            binance.default_position_side = Side.BUY
            binance.default_position_qty = 0.05
            binance.place_order_outcomes = [
                make_uncertain_error("cleanup timeout 1"),
                make_uncertain_error("cleanup timeout 2"),
                make_uncertain_error("cleanup timeout 3"),
            ]

            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})

            await runtime.start()
            await runtime.stop()

            assert len(runtime.state.open_positions) == 0
            assert binance.place_order_call_count == 3
            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason == "live_position_mismatch_flatten_failed"

    @pytest.mark.asyncio
    async def test_startup_live_mismatch_retries_uncertain_cleanup_like_v1(self):
        """V1 recovery flatten retries uncertain cleanup before fail-closing."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            live_position = PositionSnapshot(
                venue=Venue.BINANCE,
                symbol="BTCUSDT",
                side=Side.BUY,
                quantity=0.05,
                entry_price=65000.0,
                observed_at_ms=1700000010000,
            )
            binance.position_snapshots = [
                live_position,  # startup probe
                live_position,  # cleanup attempt 1 prefetch
                live_position,  # attempt 1 post-error verification still not flat
                live_position,  # cleanup attempt 2 prefetch
            ]
            binance.place_order_outcomes = [
                make_uncertain_error("okx-style ack accepted before fill visibility"),
                # Retry succeeds with a normal taker fill.
            ]

            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})

            await runtime.start()
            await runtime.stop()

            assert len(runtime.state.open_positions) == 0
            assert binance.place_order_call_count == 2
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None

    @pytest.mark.asyncio
    async def test_active_position_drift_flattens_excess_leg_and_updates_quantity(self):
        """Active positions must be reconciled against live leg size drift."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            okx = FakeVenueAdapter(Venue.OKX)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )
            await runtime.start()
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                )
            ]
            okx.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65010.0,
                    observed_at_ms=1700000010000,
                )
            ]
            runtime.state.open_positions["pos-1"] = OpenPosition(
                position_id="pos-1",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=0.05,
                short_quantity=0.05,
                long_entry_price=65000.0,
                short_entry_price=65010.0,
                opened_at_ms=1700000000000,
                matched_quantity=0.05,
            )

            await runtime.tick_active_positions()
            await runtime.stop()

            pos = runtime.state.open_positions["pos-1"]
            assert pos.matched_quantity == pytest.approx(0.03)
            assert pos.long_quantity == pytest.approx(0.03)
            assert pos.short_quantity == pytest.approx(0.03)
            assert binance.place_order_call_count == 1
            assert binance.last_request is not None
            assert binance.last_request.reduce_only is True
            assert binance.last_request.side == Side.SELL
            assert binance.last_request.quantity == pytest.approx(0.02)
            assert binance.last_request.price is None
            assert binance.last_request.client_order_id
            assert okx.place_order_call_count == 0

    @pytest.mark.asyncio
    async def test_active_position_drift_verifies_live_truth_after_false_negative_flatten(self):
        """A reduce-only cleanup can be live-effective before fill evidence is complete."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            okx = FakeVenueAdapter(Venue.OKX)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )
            await runtime.start()
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                ),
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.03,
                    entry_price=65000.0,
                    observed_at_ms=1700000010500,
                ),
            ]
            okx.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65010.0,
                    observed_at_ms=1700000010000,
                ),
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65010.0,
                    observed_at_ms=1700000010500,
                ),
            ]
            binance.place_order_outcomes = [
                make_fake_fill(
                    Venue.BINANCE,
                    "BTCUSDT",
                    Side.SELL,
                    quantity=0.0,
                    price=65000.0,
                )
            ]
            runtime.state.open_positions["pos-1"] = OpenPosition(
                position_id="pos-1",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=0.05,
                short_quantity=0.05,
                long_entry_price=65000.0,
                short_entry_price=65010.0,
                opened_at_ms=1700000000000,
                matched_quantity=0.05,
            )

            await runtime.tick_active_positions()
            await runtime.stop()

            pos = runtime.state.open_positions["pos-1"]
            assert pos.matched_quantity == pytest.approx(0.03)
            assert pos.long_quantity == pytest.approx(0.03)
            assert pos.short_quantity == pytest.approx(0.03)
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            kinds = [event["kind"] for event in runtime.journal.read_all()]
            assert "runtime.position_drift_correction_verified" in kinds
            assert "runtime.position_drift_correction_failed" not in kinds

    @pytest.mark.asyncio
    async def test_active_position_drift_skips_passive_close_live_action_settling(self):
        """Passive close owns a position while its live close action is settling."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            okx = FakeVenueAdapter(Venue.OKX)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )
            await runtime.start()
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                )
            ]
            okx.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65010.0,
                    observed_at_ms=1700000010000,
                )
            ]
            position = OpenPosition(
                position_id="pos-passive-settling",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=0.05,
                short_quantity=0.05,
                long_entry_price=65000.0,
                short_entry_price=65010.0,
                opened_at_ms=1700000000000,
                matched_quantity=0.05,
            )
            runtime.state.open_positions[position.position_id] = position
            runtime.state.pending_passive_closes[position.position_id] = PendingPassiveClose(
                position_id=position.position_id,
                reason="funding_capture",
                position_snapshot=position,
                target_quantity=0.05,
                chunk_quantities=[0.05],
                phase_state=PassivePhaseState(
                    phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                    active_maker_leg=ActiveMakerLeg.SHORT,
                ),
                long_legs=[
                    PersistedCloseExecutionLeg(
                        fill=make_fake_fill(
                            Venue.BINANCE,
                            "BTCUSDT",
                            Side.SELL,
                            quantity=0.02,
                            price=65000.0,
                        ),
                        client_order_id="lfex-live-flatten",
                        submit_started_at_ms=1700000010000,
                    )
                ],
                next_retry_at_ms=wall_clock_now_ms() + 5_000,
            )

            await runtime.tick_active_positions()
            await runtime.stop()

            assert binance.place_order_call_count == 0
            assert okx.place_order_call_count == 0
            assert position.position_id in runtime.state.pending_passive_closes
            assert runtime.state.open_positions[position.position_id].matched_quantity == pytest.approx(0.05)

    @pytest.mark.asyncio
    async def test_active_position_drift_skips_passive_close_owner_without_maker_order(self):
        """Passive close ownership blocks drift even after maker id is cleared."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            okx = FakeVenueAdapter(Venue.OKX)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )
            await runtime.start()
            binance.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.05,
                    entry_price=65000.0,
                    observed_at_ms=1700000010000,
                )
            ]
            okx.position_snapshots = [
                PositionSnapshot(
                    venue=Venue.OKX,
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    quantity=0.03,
                    entry_price=65010.0,
                    observed_at_ms=1700000010000,
                )
            ]
            position = OpenPosition(
                position_id="pos-passive-owner-no-maker",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=0.05,
                short_quantity=0.05,
                long_entry_price=65000.0,
                short_entry_price=65010.0,
                opened_at_ms=1700000000000,
                matched_quantity=0.05,
            )
            runtime.state.open_positions[position.position_id] = position
            runtime.state.pending_passive_closes[position.position_id] = PendingPassiveClose(
                position_id=position.position_id,
                reason="funding_capture",
                position_snapshot=position,
                target_quantity=0.05,
                chunk_quantities=[0.05],
                phase_state=PassivePhaseState(
                    phase=PassiveExecutionPhase.DUAL_TAKER,
                    active_maker_leg=ActiveMakerLeg.SHORT,
                    maker_order_id="",
                    maker_client_order_id="",
                ),
                next_retry_at_ms=wall_clock_now_ms() + 5_000,
            )

            await runtime.tick_active_positions()
            await runtime.stop()

            assert binance.place_order_call_count == 0
            assert okx.place_order_call_count == 0
            assert position.position_id in runtime.state.pending_passive_closes
            assert runtime.state.open_positions[position.position_id].matched_quantity == pytest.approx(0.05)
            kinds = [event["kind"] for event in runtime.journal.read_all()]
            assert "runtime.position_drift_skipped_passive_close_owner" in kinds
            assert "runtime.position_drift_correction_failed" not in kinds

    @pytest.mark.asyncio
    async def test_active_position_drift_removes_position_when_exchange_flat(self):
        """If both live legs are flat, V2 must not keep showing an open position."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            binance = FakeVenueAdapter(Venue.BINANCE)
            okx = FakeVenueAdapter(Venue.OKX)
            runtime = LiveRuntime(
                config,
                venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            )
            await runtime.start()
            runtime.state.open_positions["pos-1"] = OpenPosition(
                position_id="pos-1",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=0.05,
                short_quantity=0.05,
                long_entry_price=65000.0,
                short_entry_price=65010.0,
                opened_at_ms=1700000000000,
                matched_quantity=0.05,
            )

            await runtime.tick_active_positions()
            await runtime.stop()

            assert "pos-1" not in runtime.state.open_positions
            assert binance.place_order_call_count == 0
            assert okx.place_order_call_count == 0

    @pytest.mark.asyncio
    async def test_venue_adapters_accessible(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            adapters = {
                Venue.BINANCE: FakeVenueAdapter(Venue.BINANCE),
            }
            runtime = LiveRuntime(config, venue_adapters=adapters)
            await runtime.start()

            adapter = runtime.get_venue_adapter(Venue.BINANCE)
            assert adapter is not None

    @pytest.mark.asyncio
    async def test_missing_venue_adapter_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()

            adapter = runtime.get_venue_adapter(Venue.HYPERLIQUID)
            assert adapter is None


class TestRateLimitConfigManagerStartup:
    """Verify live startup constructs RateLimitConfigManager with correct parameter name."""

    def test_rate_limit_config_manager_accepts_config_path_param(self):
        from lightfee.rate_limit.config import RateLimitConfigManager
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            rl_path = os.path.join(td, "rate_limits.toml")
            with open(rl_path, "w") as f:
                f.write("[global]\ndefault_margin = 0.95\n")

            mgr = RateLimitConfigManager(config_path=rl_path)
            assert mgr.config.default_margin == 0.95
            outcome = mgr.refresh()
            assert outcome in ("reloaded", "unchanged")

    def test_rate_limit_config_manager_path_is_stored(self):
        from lightfee.rate_limit.config import RateLimitConfigManager

        mgr = RateLimitConfigManager(config_path="/tmp/test_limits.toml")
        assert mgr.path == "/tmp/test_limits.toml"

    def test_rate_limit_config_manager_no_path_uses_defaults(self):
        from lightfee.rate_limit.config import RateLimitConfigManager

        mgr = RateLimitConfigManager()
        assert mgr.path is None
        assert mgr.config.default_margin == 0.95
        # Refresh should be a no-op without path
        outcome = mgr.refresh()
        assert outcome == "unchanged"


class TestLiveMainStartupShutdownOrder:
    """V1 parity: startup always calls start before stop, stop always fires on exit."""

    @pytest.mark.asyncio
    async def test_live_main_calls_start_then_stop(self, monkeypatch):
        """V1: async_main calls LiveRuntime.start() then LiveRuntime.stop() in order."""
        calls: list[str] = []

        async def fake_start(self) -> None:
            calls.append("start")

        async def fake_stop(self) -> None:
            calls.append("stop")

        async def fake_run_loop(self) -> None:
            calls.append("run_loop")
            self._running = False

        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.start", fake_start
        )
        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.stop", fake_stop
        )
        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.run_loop", fake_run_loop
        )

        import tempfile
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            # Patch load_config to return the in-memory config directly
            monkeypatch.setattr(
                "lightfee.apps.live.load_config", lambda _path: config
            )
            from lightfee.apps.live import async_main
            await async_main("test.toml")

        assert calls == ["start", "run_loop", "stop"], (
            f"V1 parity violation: expected start→run_loop→stop, got {calls}"
        )

    @pytest.mark.asyncio
    async def test_live_main_stop_always_called_on_keyboard_interrupt(self, monkeypatch):
        """V1: async_main calls stop() even when KeyboardInterrupt fires during run_loop."""
        calls: list[str] = []

        async def fake_start(self) -> None:
            calls.append("start")

        async def fake_stop(self) -> None:
            calls.append("stop")

        async def fake_run_loop(self) -> None:
            calls.append("run_loop")
            raise KeyboardInterrupt()

        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.start", fake_start
        )
        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.stop", fake_stop
        )
        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.run_loop", fake_run_loop
        )

        import tempfile
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            monkeypatch.setattr(
                "lightfee.apps.live.load_config", lambda _path: config
            )
            from lightfee.apps.live import async_main
            await async_main("test.toml")

        assert "start" in calls, f"start was never called: {calls}"
        assert "stop" in calls, (
            f"V1 parity violation: stop() must be called even after KeyboardInterrupt, got {calls}"
        )

    @pytest.mark.asyncio
    async def test_sigterm_shutdown_cancels_background_tasks_and_flushes(
        self, monkeypatch, caplog, tmp_path
    ):
        """SIGTERM path must cancel runtime-owned background tasks and flush state."""
        calls: list[str] = []
        task_cancelled = asyncio.Event()
        shutdown_event = asyncio.Event()
        flush_path = tmp_path / "flushed.txt"

        class MockRuntime:

            def __init__(self, config, venue_adapters):
                self.config = config
                self._running = True

            async def start(self) -> None:
                calls.append("start")

                async def _mock_background_task() -> None:
                    try:
                        await asyncio.Event().wait()
                    finally:
                        task_cancelled.set()

                asyncio.create_task(
                    _mock_background_task(),
                    name="mock:ws-listen-key-sidecar-recovery-scan",
                )

            async def run_loop(self) -> None:
                calls.append("run_loop")

                await shutdown_event.wait()

            async def stop(self) -> None:
                calls.append("stop")
                logger = __import__("logging").getLogger("lightfee.engine.runtime")
                logger.info("shutdown stage=close_network")
                logger.info("shutdown stage=flush_state")
                flush_path.write_text("flushed")
                logger.info("shutdown stage=exit_complete")

        config = make_test_config(str(tmp_path))
        config.runtime.shutdown_grace_period_ms = 200

        monkeypatch.setattr("lightfee.apps.live.load_config", lambda _path: config)
        monkeypatch.setattr("lightfee.apps.live.build_adapter_map", lambda _config: {})
        monkeypatch.setattr("lightfee.apps.live.LiveRuntime", MockRuntime)

        from lightfee.apps.live import async_main

        async def trigger_shutdown() -> None:
            await asyncio.sleep(0)
            shutdown_event.set()

        with caplog.at_level("INFO"):
            trigger = asyncio.create_task(trigger_shutdown())
            await asyncio.wait_for(
                async_main(
                    "test.toml",
                    shutdown_event=shutdown_event,
                    shutdown_signal_name=lambda: "SIGTERM",
                ),
                timeout=0.5,
            )

            await trigger

        assert calls == ["start", "run_loop", "stop"]
        assert flush_path.read_text() == "flushed"
        assert task_cancelled.is_set()
        messages = "\n".join(record.getMessage() for record in caplog.records)
        for stage in (
            "signal_received",
            "cancel_tasks",
            "flush_state",
            "close_network",
            "exit_complete",
        ):
            assert f"shutdown stage={stage}" in messages
        assert messages.index("shutdown stage=close_network") < messages.index(
            "shutdown stage=flush_state"
        )

    @pytest.mark.asyncio
    async def test_sigterm_during_startup_enters_shutdown_path(
        self, monkeypatch, caplog, tmp_path
    ):
        """SIGTERM during runtime.start() must not wait for startup to finish."""
        calls: list[str] = []
        shutdown_event = asyncio.Event()
        start_cancelled = asyncio.Event()

        flush_path = tmp_path / "flushed.txt"

        class MockRuntime:
            def __init__(self, config, venue_adapters):
                self.config = config
                self._running = True

            async def start(self) -> None:
                calls.append("start")
                try:
                    await asyncio.Event().wait()
                finally:
                    start_cancelled.set()

            async def run_loop(self) -> None:
                calls.append("run_loop")

            async def stop(self) -> None:
                calls.append("stop")
                flush_path.write_text("flushed")

        config = make_test_config(str(tmp_path))
        config.runtime.shutdown_grace_period_ms = 100

        monkeypatch.setattr("lightfee.apps.live.load_config", lambda _path: config)
        monkeypatch.setattr("lightfee.apps.live.build_adapter_map", lambda _config: {})
        monkeypatch.setattr("lightfee.apps.live.LiveRuntime", MockRuntime)

        from lightfee.apps.live import async_main

        async def trigger_shutdown() -> None:
            await asyncio.sleep(0)
            shutdown_event.set()

        with caplog.at_level("INFO"):
            trigger = asyncio.create_task(trigger_shutdown())
            await asyncio.wait_for(
                async_main(
                    "test.toml",
                    shutdown_event=shutdown_event,
                    shutdown_signal_name=lambda: "SIGTERM",
                ),
                timeout=0.5,
            )

            await trigger

        assert calls == ["start", "stop"]
        assert start_cancelled.is_set()
        assert flush_path.read_text() == "flushed"
        messages = "\n".join(record.getMessage() for record in caplog.records)
        assert "shutdown stage=signal_received signal=SIGTERM" in messages

    def test_main_final_cleanup_does_not_wait_a_second_time_for_stuck_tasks(
        self, monkeypatch, caplog, tmp_path
    ):
        """Production main must not add an extra hardcoded wait after async_main."""
        import sys
        import time

        calls: list[float] = []

        async def fake_async_main(*args, **kwargs) -> None:
            async def _cancel_hostile_task() -> None:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    await asyncio.Event().wait()

            asyncio.create_task(_cancel_hostile_task(), name="stuck-after-async-main")

        async def fail_if_waited_again(tasks, *, timeout_s, stage):
            calls.append(timeout_s)
            raise AssertionError("main must not wait for pending tasks after async_main")

        monkeypatch.setattr("lightfee.apps.live.async_main", fake_async_main)
        monkeypatch.setattr(
            "lightfee.apps.live._cancel_tasks_with_timeout",
            fail_if_waited_again,
        )
        monkeypatch.setattr(sys, "argv", ["lightfee-live", "--config", str(tmp_path / "live.toml")])

        from lightfee.apps.live import main

        started = time.monotonic()
        with caplog.at_level("WARNING"):
            main()
        elapsed = time.monotonic() - started

        assert calls == []
        assert elapsed < 0.5
        messages = "\n".join(record.getMessage() for record in caplog.records)
        assert "shutdown stage=cancel_tasks" in messages
        assert "stuck-after-async-main" in messages
