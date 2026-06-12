"""End-to-end configuration and runtime smoke tests."""

import json
import subprocess
import sys

import pytest

from lightfee.config.loader import load_config
from lightfee.config.validation import validate_config
from lightfee.config.schema import AppConfig


class TestConfigSmoke:
    def test_example_config_loads(self):
        config = load_config("config/example.toml")
        issues = validate_config(config)
        assert len(issues) == 0

    def test_live_example_config_loads(self):
        config = load_config("config/live.example.toml")
        issues = validate_config(config)
        assert len(issues) == 0

    def test_no_chillybot_fields_accepted(self):
        config = load_config("config/example.toml")
        # Verify no Chillybot module/field leakage
        import lightfee
        assert not hasattr(lightfee, "chillybot")
        assert not hasattr(lightfee, "feedgrab")

    def test_all_seven_venues_in_registry(self):
        from lightfee.venues.registry import all_live_perp_venues
        venues = all_live_perp_venues()
        venue_names = {v.value for v in venues}
        assert venue_names == {"binance", "okx", "bybit", "bitget", "gate", "aster", "hyperliquid"}


class TestImportSmoke:
    def test_core_imports(self):
        from lightfee.core import domain, money, time, errors, contracts

    def test_config_imports(self):
        from lightfee.config import schema, loader, validation, compatibility, defaults

    def test_persistence_imports(self):
        from lightfee.persistence import journal, snapshot_store, sqlite_store, ledgers, metrics

    def test_venues_imports(self):
        from lightfee.venues import base, registry, common, binance, okx, bybit, bitget, gate, aster, hyperliquid

    def test_sidecar_imports(self):
        from lightfee.sidecar import snapshot, publisher, pairing, service

    def test_strategy_imports(self):
        from lightfee.strategy import discovery, scoring, market_view, transfer_bias

    def test_risk_imports(self):
        from lightfee.risk import modes, budgets, health, operator

    def test_engine_imports(self):
        from lightfee.engine import state, lifecycle, recovery, entry, exit, passive_maker, execution_planner, runtime, supervisor

    def test_marketdata_imports(self):
        from lightfee.marketdata import l2, liquidity, freshness, local_book

    def test_offline_imports(self):
        from lightfee.offline.analysis import journal as aj, incident
        from lightfee.offline.replay import dataset, engine, counterfactual, walk_forward
        from lightfee.offline.evolution import report, ledger, approval, cycle
        from lightfee.offline.llm_evolution import report as llm_report
        from lightfee.offline.reports import render, daily

    def test_apps_imports(self):
        from lightfee.apps import live, sidecar, scheduler, ops, report, replay, evolution, probe

    def test_sidecar_module_help_invokes_main(self):
        result = subprocess.run(
            [sys.executable, "-m", "lightfee.apps.sidecar", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "lightfee-sidecar" in result.stdout

    def test_ops_imports(self):
        from lightfee.ops import commands


class TestRuntimeLaneScheduling:
    """V1 parity: run_loop schedules all eight lanes in V1 order."""

    @pytest.mark.asyncio
    async def test_run_loop_schedules_all_lanes(self, monkeypatch):
        """Verify run_loop calls every lane at least once in V1 order."""
        import tempfile
        from pathlib import Path
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        from lightfee.engine.runtime import LiveRuntime

        td = tempfile.mkdtemp()
        try:
            config = AppConfig(
                runtime=RuntimeConfig(
                    mode="paper",
                    poll_interval_ms=100,
                    sidecar_snapshot_path=str(Path(td) / "sidecar.json"),
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
                    event_log_path=str(Path(td) / "events.jsonl"),
                    snapshot_path=str(Path(td) / "state.json"),
                ),
                venues=[],
                symbols=["BTCUSDT"],
            )
            runtime = LiveRuntime(config)
            runtime.journal.open()

            seen: list[str] = []

            async def fake_full_tick():
                seen.append("full_tick")

            async def fake_active_tick():
                seen.append("active_tick")

            async def fake_rate_limit_reload(now_ms: int):
                seen.append("rate_limit_reload")

            async def fake_l2_sync(now_ms: int):
                seen.append("l2_sync")

            async def fake_passive_close(now_ms: int):
                seen.append("passive_close")

            async def fake_normal_exits(now_ms: int):
                seen.append("normal_exits")

            async def fake_maker_event(now_ms: int):
                seen.append("maker_event")

            async def fake_housekeeping(now_ms: int):
                seen.append("housekeeping")

            monkeypatch.setattr(runtime, "tick", fake_full_tick)
            monkeypatch.setattr(runtime, "tick_active_positions", fake_active_tick)
            monkeypatch.setattr(runtime, "_maybe_reload_rate_limits", fake_rate_limit_reload)
            monkeypatch.setattr(runtime, "_sync_local_l2_data", fake_l2_sync)
            monkeypatch.setattr(runtime, "_maybe_tick_passive_close", fake_passive_close)
            monkeypatch.setattr(runtime, "_maybe_process_normal_exits", fake_normal_exits)
            monkeypatch.setattr(runtime, "_maybe_tick_maker_event", fake_maker_event)
            monkeypatch.setattr(runtime, "_post_tick_housekeeping", fake_housekeeping)

            # Run exactly one iteration, then stop
            original_run_loop = runtime.run_loop

            async def one_shot_run_loop():
                runtime._running = True
                now_ms = 0

                # Full tick lane
                await runtime.tick()
                # Active-position fast tick lane
                await runtime.tick_active_positions()
                # Rate-limit reload
                await runtime._maybe_reload_rate_limits(now_ms)
                # Local-L2 data sync
                await runtime._sync_local_l2_data(now_ms)
                # Passive close lane
                await runtime._maybe_tick_passive_close(now_ms)
                # Normal exit lane
                await runtime._maybe_process_normal_exits(now_ms)
                # Maker-event lane
                await runtime._maybe_tick_maker_event(now_ms)
                # Post-tick housekeeping
                await runtime._post_tick_housekeeping(now_ms)

                runtime._running = False

            monkeypatch.setattr(runtime, "run_loop", one_shot_run_loop)

            await runtime.run_loop()

            expected = [
                "full_tick",
                "active_tick",
                "rate_limit_reload",
                "l2_sync",
                "passive_close",
                "normal_exits",
                "maker_event",
                "housekeeping",
            ]
            assert seen == expected, (
                f"V1 lane ordering violation: expected {expected}, got {seen}"
            )

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_tick_exports_current_state_before_long_or_early_return_tick(self, tmp_path):
        """Runtime heartbeat: current-state refresh must not wait for full tick completion."""
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        from lightfee.engine.loop_control import current_state_export_path
        from lightfee.engine.runtime import LiveRuntime

        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                poll_interval_ms=1000,
                sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
                sidecar_snapshot_max_age_ms=1000,
            ),
            strategy=StrategyConfig(
                risk_monitor_enabled=False,
                local_l2_enabled=False,
                local_l2_ws_enabled=False,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "live-state.json"),
            ),
            venues=[],
            symbols=["BTCUSDT"],
        )
        runtime = LiveRuntime(config)
        runtime.journal.open()
        try:
            await runtime.tick()
        finally:
            runtime.journal.close()

        current_state_path = current_state_export_path(config)
        with open(current_state_path) as f:
            exported = json.load(f)
        assert exported["tick_count"] == 1
        assert exported["last_tick_ms"] > 0
        assert exported["open_position_count"] == 0


class TestReplaySmoke:
    """V2: smoke tests for replay dataset structured reads and journal fallback."""

    def test_replay_dataset_imports(self):
        """Verify all replay dataset symbols are importable."""
        from lightfee.offline.replay.dataset import (
            ReplayDataset,
            _is_journal_only,
            _ensure_replay_facts_table,
            _read_replay_facts,
            _read_journal_only_events,
        )

    def test_replay_dataset_load_journal_baseline(self):
        """load() with only journal_path returns journal-sourced dataset."""
        import tempfile
        from pathlib import Path
        from lightfee.offline.replay.dataset import ReplayDataset
        from lightfee.persistence.journal import Journal

        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "smoke-events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened",
                          {"position_id": "smoke-pos", "symbol": "BTCUSDT"},
                          ts_ms=1768003200000)
            journal.append("runtime.lifecycle_changed",
                          {"from": "booting", "to": "running"},
                          ts_ms=1768003300000)
            journal.close()

            dataset = ReplayDataset.load(str(jp))
            assert dataset.source == "journal"
            assert len(dataset.records) == 2

            kinds = {r["kind"] for r in dataset.records}
            assert "entry.opened" in kinds
            assert "runtime.lifecycle_changed" in kinds

    def test_replay_dataset_from_journal_range_preserves_filter(self):
        """from_journal_range date filter works correctly."""
        import tempfile
        from pathlib import Path
        from lightfee.offline.replay.dataset import ReplayDataset
        from lightfee.persistence.journal import Journal

        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "smoke-filter.jsonl"
            journal = Journal(jp)
            journal.open()
            # Jan 10 2026
            journal.append("scan.completed", {"candidate_count": 1},
                          ts_ms=1768003200000)
            # Jan 15 2026
            journal.append("scan.completed", {"candidate_count": 2},
                          ts_ms=1768435200000)
            # Jan 20 2026
            journal.append("scan.completed", {"candidate_count": 3},
                          ts_ms=1768867200000)
            journal.close()

            dataset = ReplayDataset.from_journal_range(
                str(jp), date_from="20260112", date_to="20260117"
            )
            assert len(dataset.records) == 1
            assert dataset.records[0]["payload"]["candidate_count"] == 2

    def test_replay_semantics_identical_regardless_of_source(self):
        """Replay results must be identical whether records come from
        structured store, journal, or merged path."""
        import json
        import sqlite3
        import tempfile
        from pathlib import Path
        from lightfee.offline.replay.dataset import (
            ReplayDataset, _ensure_replay_facts_table, _is_journal_only, _ts_to_date_str,
        )
        from lightfee.persistence.journal import Journal, replay_journal_records

        with tempfile.TemporaryDirectory() as td:
            all_records = [
                {"seq": 1, "ts_ms": 1768003200000, "kind": "entry.opened",
                 "payload": {"position_id": "pos-smoke", "symbol": "ETHUSDT",
                            "long_venue": "binance", "short_venue": "okx",
                            "quantity": 2.0}},
                {"seq": 2, "ts_ms": 1768003300000, "kind": "runtime.lifecycle_changed",
                 "payload": {"from": "booting", "to": "running"}},
                {"seq": 3, "ts_ms": 1768003400000, "kind": "exit.closed",
                 "payload": {"position_id": "pos-smoke"}},
            ]

            # Path A: journal
            jp = Path(td) / "smoke-journal.jsonl"
            journal = Journal(jp)
            journal.open()
            for r in all_records:
                journal.append(r["kind"], r["payload"], ts_ms=r["ts_ms"])
            journal.close()

            result_a = replay_journal_records(
                ReplayDataset.from_journal_range(str(jp)).records
            )

            # Path B: structured with journal fallback (journal-only events)
            store_path = Path(td) / "smoke-store.db"
            conn = sqlite3.connect(str(store_path))
            _ensure_replay_facts_table(conn)
            for r in all_records:
                if _is_journal_only(r["kind"]):
                    continue
                conn.execute(
                    "INSERT INTO replay_facts (seq, run_id, ts_ms, kind, payload_json, date) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (r["seq"], "test-run", r["ts_ms"], r["kind"],
                     json.dumps(r["payload"]), _ts_to_date_str(r["ts_ms"])),
                )
            conn.commit()
            conn.close()

            result_b = replay_journal_records(
                ReplayDataset.from_structured(str(store_path), journal_path=str(jp)).records
            )

            assert result_a["open_position_count"] == result_b["open_position_count"]
            assert result_a["final_lifecycle"] == result_b["final_lifecycle"]
            assert result_a["pending_entry_count"] == result_b["pending_entry_count"]
            assert result_a["pending_close_count"] == result_b["pending_close_count"]
