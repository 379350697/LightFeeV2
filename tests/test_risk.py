"""Tests for risk modes, budgets, health, and operator controls."""

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.risk.budgets import RiskBudgets
from lightfee.risk.health import evaluate_risk_health
from lightfee.risk.modes import (
    EngineLifecycle,
    EngineMode,
    GlobalRiskMode,
    derive_engine_mode,
)
from lightfee.risk.operator import OperatorCommand, apply_operator_command


class TestGlobalRiskMode:
    def test_ordering(self):
        assert GlobalRiskMode.FAIL_CLOSED.at_least(GlobalRiskMode.REDUCE_ONLY)
        assert GlobalRiskMode.REDUCE_ONLY.at_least(GlobalRiskMode.ENTRY_PAUSED)
        assert not GlobalRiskMode.RUNNING.at_least(GlobalRiskMode.ENTRY_PAUSED)

    def test_max(self):
        assert GlobalRiskMode.RUNNING.max(GlobalRiskMode.REDUCE_ONLY) == GlobalRiskMode.REDUCE_ONLY


class TestEngineMode:
    def test_derive_running(self):
        assert derive_engine_mode(EngineLifecycle.RUNNING, GlobalRiskMode.RUNNING) == EngineMode.RUNNING

    def test_derive_fail_closed_from_risk_only_lifecycle(self):
        """V1: FailClosed = RISK_ONLY lifecycle + FAIL_CLOSED risk mode."""
        assert derive_engine_mode(EngineLifecycle.RISK_ONLY, GlobalRiskMode.FAIL_CLOSED) == EngineMode.FAIL_CLOSED

    def test_derive_fail_closed_from_risk(self):
        assert derive_engine_mode(EngineLifecycle.RUNNING, GlobalRiskMode.FAIL_CLOSED) == EngineMode.FAIL_CLOSED

    def test_derive_recovering_from_booting(self):
        assert derive_engine_mode(EngineLifecycle.BOOTING, GlobalRiskMode.RUNNING) == EngineMode.RECOVERING


class TestRiskBudgets:
    def test_blocks_exceeding_max_positions(self):
        b = RiskBudgets(max_concurrent_positions=2, current_position_count=2)
        ok, reason = b.check_entry("binance", "BTCUSDT", 100)
        assert not ok
        assert "max_concurrent_positions" in reason

    def test_blocks_exceeding_venue_exposure(self):
        b = RiskBudgets(
            max_single_venue_exposure_quote=200,
            current_single_venue_exposures={"binance": 150},
        )
        ok, reason = b.check_entry("binance", "BTCUSDT", 100)
        assert not ok

    def test_allows_entry_within_budget(self):
        b = RiskBudgets(max_concurrent_positions=8)
        ok, reason = b.check_entry("binance", "BTCUSDT", 100)
        assert ok

    def test_records_entry_and_exit(self):
        b = RiskBudgets()
        b.record_entry("binance", "BTCUSDT", 50)
        assert b.current_position_count == 1
        b.record_exit("binance", "BTCUSDT", 50)
        assert b.current_position_count == 0


class TestRiskHealth:
    def test_no_health_triggers_when_ratios_high(self):
        config = StrategyConfig()
        health = evaluate_risk_health({"binance": 5.0, "okx": 4.0}, config)
        assert not health.death_condition
        assert not health.delever_condition

    def test_death_condition_when_below_death_ratio(self):
        config = StrategyConfig(death_line_enabled=True, death_health_ratio=1.1)
        health = evaluate_risk_health({"binance": 1.0}, config)
        assert health.death_condition


class TestOperatorCommands:
    def test_pause_entry_raises_risk(self):
        risk, lifecycle = apply_operator_command(
            OperatorCommand.PAUSE_ENTRY, GlobalRiskMode.RUNNING, EngineLifecycle.RUNNING
        )
        assert risk == GlobalRiskMode.ENTRY_PAUSED

    def test_fail_closed(self):
        risk, lifecycle = apply_operator_command(
            OperatorCommand.FAIL_CLOSED, GlobalRiskMode.RUNNING, EngineLifecycle.RUNNING
        )
        assert risk == GlobalRiskMode.FAIL_CLOSED
        assert lifecycle == EngineLifecycle.RISK_ONLY

    def test_resume_if_safe_blocked_by_recovery(self):
        risk, lifecycle = apply_operator_command(
            OperatorCommand.RESUME_IF_SAFE,
            GlobalRiskMode.ENTRY_PAUSED,
            EngineLifecycle.RUNNING,
            has_blocking_recovery=True,
        )
        assert risk == GlobalRiskMode.ENTRY_PAUSED  # unchanged
