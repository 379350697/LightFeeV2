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

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.engine.bootstrap import (
    active_position_poll_enabled,
    active_position_poll_interval_ms,
    full_tick_ready,
    prepare_runtime_symbols,
    startup_market_warmup_ms,
    wall_clock_now_ms,
)
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import OpenPosition, PendingEntry
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from tests.fake_adapters import FakeVenueAdapter, make_uncertain_error


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
        ),
        persistence=PersistenceConfig(
            event_log_path=str(Path(temp_dir) / "events.jsonl"),
            snapshot_path=str(Path(temp_dir) / "state.json"),
        ),
        venues=[],
        symbols=["BTCUSDT"],
    )


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
        """Recovery must still probe configured symbols that drop out of a daily universe."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.symbols = ["BTCUSDT", "ETHUSDT"]
            runtime = LiveRuntime(config)

            symbols = runtime._startup_position_probe_symbols(
                {"resolved_symbols": ["BTCUSDT"]}
            )

            assert symbols == ["BTCUSDT", "ETHUSDT"]

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
    async def test_local_l2_startup_bootstrap_runs_when_ws_disabled(self):
        """REST bootstrap must still use filtered target pairs when WS is disabled."""
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
    async def test_local_l2_snapshot_restore_filters_unsupported_venue_symbols(self):
        """Persisted full-book snapshots must not resurrect non-trading contracts."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.strategy.local_l2_enabled = True

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
                )
            ]
            binance.default_position_side = Side.BUY
            binance.default_position_qty = 0.05

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

    @pytest.mark.asyncio
    async def test_startup_clears_stale_blocked_reason_when_snapshot_is_clean(self):
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

            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0

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
                )
            ]
            binance.default_position_side = Side.BUY
            binance.default_position_qty = 0.05
            okx.default_position_side = Side.SELL
            okx.default_position_qty = 0.03

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

    @pytest.mark.asyncio
    async def test_startup_blocks_unpaired_live_exchange_position_when_flatten_fails(self):
        """If mismatch flattening fails, V2 must fail closed with visible reason."""
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
            binance.place_order_outcomes = [make_uncertain_error("cleanup timeout")]

            runtime = LiveRuntime(config, venue_adapters={Venue.BINANCE: binance})

            await runtime.start()
            await runtime.stop()

            assert len(runtime.state.open_positions) == 0
            assert binance.place_order_call_count == 1
            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason == "live_position_mismatch_flatten_failed"

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
        import tempfile, os

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
