"""End-to-end configuration and runtime smoke tests."""

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

    def test_ops_imports(self):
        from lightfee.ops import commands
