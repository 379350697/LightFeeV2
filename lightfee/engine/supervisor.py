"""Supervisor: monitors open positions, triggers exits, manages risk."""

from __future__ import annotations

from lightfee.config.schema import AppConfig
from lightfee.engine.state import EngineState
from lightfee.persistence.journal import Journal
from lightfee.risk.health import evaluate_risk_health
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode, derive_engine_mode


class Supervisor:
    """Monitors open positions and applies risk rules."""

    def __init__(self, config: AppConfig, state: EngineState, journal: Journal) -> None:
        self.config = config
        self.state = state
        self.journal = journal

    def supervise(self, now_ms: int, venue_health_ratios: dict[str, float]) -> None:
        """Run supervision tick: evaluate risk lines, trigger actions."""
        strategy = self.config.strategy

        if not strategy.risk_monitor_enabled:
            return

        health = evaluate_risk_health(venue_health_ratios, strategy)

        if health.death_condition:
            self.journal.append(
                "risk.death_line_triggered",
                {"min_health_ratio": health.min_health_ratio, "ts_ms": now_ms},
            )
        elif health.delever_condition:
            self.journal.append(
                "risk.delever_line_triggered",
                {"min_health_ratio": health.min_health_ratio, "ts_ms": now_ms},
            )
        elif health.warning_condition and strategy.warning_pause_new_entries_enabled:
            self.journal.append(
                "risk.warning_line_triggered",
                {"min_health_ratio": health.min_health_ratio, "ts_ms": now_ms},
            )
