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
        "economics_complete": True,
        "fee_evidence_complete": True,
        "contract_normalization_status": "complete",
    }
    data.update(overrides)
    data.setdefault("expected_net_edge_bps", data["net_edge_bps"])
    data.setdefault("worst_case_edge_bps", data["net_edge_bps"])
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


def test_spread_controller_treats_truthy_live_gate_as_disabled() -> None:
    controller = SpreadTradingController(
        StrategyConfig(
            spread_reversion_enabled=True,
            spread_live_enabled="false",  # type: ignore[arg-type]
        )
    )

    decision = controller.evaluate_entry(
        _candidate(), state=SpreadTradingState(), now_ms=1_000
    )

    assert decision.allowed is False
    assert decision.reason == "spread_live_disabled"


def test_spread_controller_fails_closed_for_candidate_without_complete_economics() -> None:
    controller = SpreadTradingController(
        StrategyConfig(spread_reversion_enabled=True, spread_live_enabled=True)
    )

    decision = controller.evaluate_entry(
        _candidate(economics_complete=False),
        state=SpreadTradingState(),
        now_ms=1_000,
    )

    assert decision.allowed is False
    assert decision.reason == "spread_candidate_economics_incomplete"


def test_spread_controller_requires_explicit_fee_evidence() -> None:
    controller = SpreadTradingController(
        StrategyConfig(spread_reversion_enabled=True, spread_live_enabled=True)
    )

    decision = controller.evaluate_entry(
        _candidate(fee_evidence_complete=False),
        state=SpreadTradingState(),
        now_ms=1_000,
    )

    assert decision.allowed is False
    assert decision.reason == "spread_candidate_fee_evidence_incomplete"


def test_spread_controller_rejects_truthy_non_boolean_evidence() -> None:
    controller = SpreadTradingController(
        StrategyConfig(spread_reversion_enabled=True, spread_live_enabled=True)
    )

    economics = controller.evaluate_entry(
        _candidate(economics_complete="true"),
        state=SpreadTradingState(),
        now_ms=1_000,
    )
    fee = controller.evaluate_entry(
        _candidate(fee_evidence_complete="true"),
        state=SpreadTradingState(),
        now_ms=1_000,
    )

    assert economics.reason == "spread_candidate_economics_incomplete"
    assert fee.reason == "spread_candidate_fee_evidence_incomplete"


def test_spread_controller_requires_contract_proof_and_current_calculation() -> None:
    controller = SpreadTradingController(
        StrategyConfig(spread_reversion_enabled=True, spread_live_enabled=True)
    )

    unnormalized = controller.evaluate_entry(
        _candidate(contract_normalization_status="unknown"),
        state=SpreadTradingState(),
        now_ms=1_000,
    )
    legacy_calculation = controller.evaluate_entry(
        _candidate(calculation_version="spread_v1_legacy"),
        state=SpreadTradingState(),
        now_ms=1_000,
    )

    assert unnormalized.reason == "spread_contract_normalization_incomplete"
    assert legacy_calculation.reason == "spread_candidate_calculation_version_mismatch"


def test_spread_controller_suppresses_live_entry_intent_but_reports_hypothetical_canary() -> None:
    cfg = StrategyConfig(
        spread_reversion_enabled=True,
        spread_live_enabled=True,
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

    assert decision.allowed is False
    assert decision.reason == "spread_live_not_supported"
    assert decision.intent is None
    assert decision.evidence["entry_intent_suppressed"] is True
    assert decision.evidence["hypothetical_entry_notional_quote"] == 20.0


def test_spread_controller_accepts_current_v3_diagnostics_but_still_suppresses_intent() -> None:
    controller = SpreadTradingController(
        StrategyConfig(
            spread_reversion_enabled=True,
            spread_live_enabled=True,
            spread_dynamic_net_edge_enabled=True,
            spread_model_epoch="v3_cost_normalized_reversion",
            spread_signal_ttl_ms=1_000,
        )
    )

    decision = controller.evaluate_entry(
        _candidate(
            calculation_version="spread_v3_cost_normalized_reversion",
            model_epoch="v3_cost_normalized_reversion",
        ),
        state=SpreadTradingState(),
        now_ms=1_000,
    )

    assert decision.allowed is False
    assert decision.reason == "spread_live_not_supported"
    assert decision.intent is None


def test_spread_controller_treats_gross_limit_as_both_legs_not_one_leg() -> None:
    controller = SpreadTradingController(
        StrategyConfig(
            spread_reversion_enabled=True,
            spread_live_enabled=True,
            spread_live_notional_quote=100.0,
            spread_max_gross_quote=50.0,
            spread_signal_ttl_ms=1_000,
        )
    )

    empty = controller.evaluate_entry(
        _candidate(entry_notional_quote=100.0),
        state=SpreadTradingState(),
        now_ms=1_000,
    )
    residual = controller.evaluate_entry(
        _candidate(entry_notional_quote=100.0),
        state=SpreadTradingState(global_gross_quote=40.0),
        now_ms=1_000,
    )

    assert empty.allowed is False
    assert empty.reason == "spread_live_not_supported"
    assert empty.intent is None
    assert empty.evidence["hypothetical_entry_notional_quote"] == 25.0
    assert residual.allowed is False
    assert residual.reason == "spread_live_not_supported"
    assert residual.intent is None
    assert residual.evidence["hypothetical_entry_notional_quote"] == 5.0


def test_spread_controller_blocks_single_venue_dislocation_from_live_entry() -> None:
    cfg = StrategyConfig(spread_reversion_enabled=True, spread_live_enabled=True)
    controller = SpreadTradingController(cfg)

    decision = controller.evaluate_entry(
        _candidate(
            opportunity_label="single_venue_dislocation",
            screening_reasons=["fair_outlier_override"],
        ),
        state=SpreadTradingState(),
        now_ms=1_000,
    )

    assert decision.allowed is False
    assert decision.reason == "spread_single_venue_dislocation_paper_only"


def test_spread_controller_blocks_concurrent_spread_positions() -> None:
    cfg = StrategyConfig(
        spread_reversion_enabled=True,
        spread_live_enabled=True,
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


def test_signed_negative_entry_and_stop_use_absolute_z_score() -> None:
    controller = SpreadTradingController(
        StrategyConfig(
            spread_reversion_enabled=True,
            spread_live_enabled=True,
            spread_entry_z=2.0,
            spread_stop_z=3.5,
        )
    )
    entry = controller.evaluate_entry(
        _candidate(z_score=-3.0), state=SpreadTradingState(), now_ms=1_000
    )
    position = SpreadPosition(
        position_id="spread-negative",
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        entry_spread_bps=-20.0,
        entry_z_score=-2.5,
        entry_notional_quote=20.0,
        opened_at_ms=1_000,
    )
    stop = controller.evaluate_exit(
        position, _candidate(z_score=-4.0), now_ms=2_000
    )

    assert entry.allowed is False
    assert entry.reason == "spread_live_not_supported"
    assert entry.intent is None
    assert stop.allowed is True
    assert stop.reason == "spread_stop_z_reached"


def test_spread_controller_requires_explicit_live_gate_and_complete_economics() -> None:
    disabled = SpreadTradingController(StrategyConfig(spread_reversion_enabled=True))
    assert disabled.evaluate_entry(
        _candidate(), state=SpreadTradingState(), now_ms=1_000
    ).reason == "spread_live_disabled"

    controller = SpreadTradingController(
        StrategyConfig(spread_reversion_enabled=True, spread_live_enabled=True)
    )
    assert controller.evaluate_entry(
        _candidate(economics_complete=False), state=SpreadTradingState(), now_ms=1_000
    ).reason == "spread_candidate_economics_incomplete"


def test_spread_controller_requires_worst_case_edge_not_just_expected_edge() -> None:
    controller = SpreadTradingController(
        StrategyConfig(
            spread_reversion_enabled=True,
            spread_live_enabled=True,
            spread_min_net_edge_bps=5.0,
        )
    )

    decision = controller.evaluate_entry(
        _candidate(expected_net_edge_bps=12.0, worst_case_edge_bps=2.0),
        state=SpreadTradingState(),
        now_ms=1_000,
    )

    assert decision.allowed is False
    assert decision.reason == "spread_worst_case_edge_below_threshold"


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
