from __future__ import annotations

from lightfee.config.schema import StrategyConfig
from lightfee.spread.controller import (
    SpreadTradingController,
    SpreadTradingState,
)
from lightfee.spread.models import SpreadReversionCandidate, SpreadPosition
from lightfee.spread.modules import DegradationState


def _candidate(**overrides) -> SpreadReversionCandidate:
    data = {
        "candidate_id": "spread:BTCUSDT:cheap->rich",
        "symbol": "BTCUSDT",
        "long_venue": "cheap",
        "short_venue": "rich",
        "spread_mid_bps": 20.0,
        "executable_spread_bps": 18.0,
        "rolling_mean_bps": 8.0,
        "rolling_std_bps": 4.0,
        "z_score": 3.0,
        "net_edge_bps": 12.0,
        "sample_count": 20,
        "signal_ts_ms": 1_000,
        "long_quote_ts_ms": 1_000,
        "short_quote_ts_ms": 1_000,
        "entry_notional_quote": 20.0,
        "capacity_quote": 100.0,
        "signal_status": "entry_ready",
    }
    data.update(overrides)
    return SpreadReversionCandidate(**data)


def test_spread_controller_defaults_to_disabled() -> None:
    controller = SpreadTradingController(StrategyConfig())

    decision = controller.evaluate_entry(
        _candidate(),
        state=SpreadTradingState(),
        now_ms=1_000,
    )

    assert decision.allowed is False
    assert decision.reason == "spread_reversion_disabled"


def test_spread_controller_allows_only_small_live_canary_when_enabled() -> None:
    cfg = StrategyConfig(
        spread_reversion_enabled=True,
        spread_live_notional_quote=20.0,
        spread_max_gross_quote=50.0,
        spread_max_concurrent_positions=1,
        spread_entry_z=2.0,
        spread_min_net_edge_bps=5.0,
        spread_signal_ttl_ms=1_000,
    )
    controller = SpreadTradingController(cfg)

    decision = controller.evaluate_entry(
        _candidate(entry_notional_quote=200.0, net_edge_bps=8.0),
        state=SpreadTradingState(),
        now_ms=1_500,
    )

    assert decision.allowed is True
    assert decision.intent is not None
    assert decision.intent.strategy_bucket == "spread_reversion"
    assert decision.intent.entry_notional_quote == 20.0


def test_spread_controller_blocks_concurrent_spread_positions() -> None:
    cfg = StrategyConfig(
        spread_reversion_enabled=True,
        spread_max_concurrent_positions=1,
    )
    controller = SpreadTradingController(cfg)
    state = SpreadTradingState(
        open_positions=[
            SpreadPosition(
                position_id="spread-1",
                symbol="ETHUSDT",
                long_venue="cheap",
                short_venue="rich",
                entry_spread_bps=12.0,
                entry_z_score=2.2,
                entry_notional_quote=20.0,
                opened_at_ms=100,
            )
        ]
    )

    decision = controller.evaluate_entry(_candidate(), state=state, now_ms=1_000)

    assert decision.allowed is False
    assert decision.reason == "spread_max_concurrent_positions_reached"


def test_spread_controller_exits_on_convergence_or_time_stop() -> None:
    cfg = StrategyConfig(
        spread_reversion_enabled=True,
        spread_exit_z=0.5,
        spread_stop_z=3.5,
        spread_max_hold_ms=60_000,
    )
    controller = SpreadTradingController(cfg)
    position = SpreadPosition(
        position_id="spread-1",
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        entry_spread_bps=20.0,
        entry_z_score=2.5,
        entry_notional_quote=20.0,
        opened_at_ms=1_000,
    )

    convergence = controller.evaluate_exit(
        position,
        _candidate(z_score=0.2, net_edge_bps=1.0),
        now_ms=10_000,
    )
    time_stop = controller.evaluate_exit(
        position,
        _candidate(z_score=1.0, net_edge_bps=1.0),
        now_ms=70_001,
    )

    assert convergence.allowed is True
    assert convergence.reason == "spread_converged"
    assert time_stop.allowed is True
    assert time_stop.reason == "spread_max_hold_elapsed"


def test_spread_controller_uses_degradation_state_before_forced_exit() -> None:
    cfg = StrategyConfig(spread_reversion_enabled=True)
    controller = SpreadTradingController(cfg)
    position = SpreadPosition(
        position_id="spread-1",
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        entry_spread_bps=20.0,
        entry_z_score=2.5,
        entry_notional_quote=20.0,
        opened_at_ms=1_000,
    )

    missing = controller.evaluate_exit(position, None, now_ms=10_000)
    observe = controller.evaluate_exit(
        position,
        _candidate(degradation_state=DegradationState.OBSERVE_DEGRADED.value),
        now_ms=10_000,
    )
    protective = controller.evaluate_exit(
        position,
        _candidate(degradation_state=DegradationState.PROTECTIVE_EXIT_READY.value),
        now_ms=10_000,
    )
    recovery = controller.evaluate_exit(
        position,
        _candidate(degradation_state=DegradationState.RECOVERY_REQUIRED.value),
        now_ms=10_000,
    )

    assert missing.allowed is False
    assert missing.reason == "spread_exit_observe_degraded"
    assert observe.allowed is False
    assert observe.reason == "spread_exit_observe_degraded"
    assert protective.allowed is True
    assert protective.reason == "spread_protective_exit_ready"
    assert recovery.allowed is False
    assert recovery.reason == "spread_recovery_required"
