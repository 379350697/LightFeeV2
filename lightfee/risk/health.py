"""Risk health monitoring: warning, delever, death lines."""

from __future__ import annotations

from dataclasses import dataclass

from lightfee.config.schema import StrategyConfig


@dataclass
class RiskHealthView:
    min_health_ratio: float
    death_condition: bool
    delever_condition: bool
    warning_condition: bool


def evaluate_risk_health(
    health_ratios: dict[str, float],
    config: StrategyConfig,
) -> RiskHealthView:
    """Evaluate risk lines from venue health ratios."""
    ratios = list(health_ratios.values())
    if not ratios:
        return RiskHealthView(
            min_health_ratio=float("inf"),
            death_condition=False,
            delever_condition=False,
            warning_condition=False,
        )

    min_ratio = min(ratios)
    death = config.death_line_enabled and min_ratio <= config.death_health_ratio
    delever = config.delever_line_enabled and min_ratio <= config.delever_health_ratio
    warning = config.warning_line_enabled and min_ratio <= config.warning_health_ratio

    return RiskHealthView(
        min_health_ratio=min_ratio,
        death_condition=death,
        delever_condition=delever,
        warning_condition=warning,
    )
