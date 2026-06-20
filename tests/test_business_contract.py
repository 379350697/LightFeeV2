from __future__ import annotations

import pytest

from lightfee.engine.business_contract import (
    classify_business_event_kind,
    classify_entry_quantity_contract,
    classify_noise_visibility,
    close_reconciliation_evidence_contract,
    close_order_error_resolution_contract,
    entry_market_evidence_contract,
    passive_close_final_truth_contract,
    quote_rewarm_handoff_contract,
)


def test_entry_quantity_contract_residual_uses_common_quantity_not_initial_slice():
    result = classify_entry_quantity_contract(
        raw_quantity=100.0,
        common_quantity=100.0,
        effective_quantity=50.0,
    )

    assert result["quantity_contract_status"] == "hedgeable"
    assert result["unhedgeable_residual_quantity"] == pytest.approx(0.0)


def test_entry_quantity_contract_marks_exchange_step_adjustment():
    result = classify_entry_quantity_contract(
        raw_quantity=1856.0,
        common_quantity=1800.0,
        effective_quantity=1800.0,
    )

    assert result["quantity_contract_status"] == "hedgeable_adjusted"
    assert result["unhedgeable_residual_quantity"] == pytest.approx(56.0)


def test_entry_quantity_contract_blocks_unhedgeable_quantity():
    result = classify_entry_quantity_contract(
        raw_quantity=10.0,
        common_quantity=0.0,
        effective_quantity=0.0,
    )

    assert result["quantity_contract_status"] == "blocked_unhedgeable_quantity"
    assert result["unhedgeable_residual_quantity"] == pytest.approx(10.0)


def test_quote_rewarm_handoff_contract_terminalizes_hard_timeout():
    result = quote_rewarm_handoff_contract(
        phase="quote_rewarm",
        status="hard_over_budget",
        configured_action="skip_candidate_after_hard_rewarm",
        terminal_kind="",
    )

    assert result == {
        "action_taken": "skip_candidate_after_hard_rewarm",
        "action_evidence_kind": "business_contract.quote_rewarm_hard_timeout",
        "diagnostic_severity": "production_issue",
    }


def test_entry_market_evidence_contract_blocks_stale_quote():
    result = entry_market_evidence_contract(
        "runtime.entry_quote_revalidate_failed",
        {
            "venue": "aster",
            "symbol": "ALICEUSDT",
            "reason_bucket": "rest_resolved_but_stale",
            "reason_family": "rest_invalid_quote",
        },
    )

    assert result["phase"] == "ENTRY_MARKET_EVIDENCE"
    assert result["evidence_class"] == "quote"
    assert result["action"] == "block_stale_quote"
    assert result["blocks_entry"] is True
    assert result["diagnostic_severity"] == "production_issue"


def test_entry_market_evidence_contract_maps_oi_resolution_and_failure():
    resolved = entry_market_evidence_contract(
        "runtime.entry_oi_targeted_refresh_resolved",
        {
            "venue": "binance",
            "symbol": "HOMEUSDT",
            "previous_open_interest_evidence_status": "deferred_by_cap",
            "open_interest_evidence_status": "available",
        },
    )
    failed = entry_market_evidence_contract(
        "runtime.entry_oi_targeted_refresh_failed",
        {
            "venue": "aster",
            "symbol": "BSBUSDT",
            "open_interest_evidence_status": "timeout",
        },
    )

    assert resolved["action"] == "allow_entry_evidence"
    assert resolved["blocks_entry"] is False
    assert resolved["terminality"] == "terminal_evidence_resolved"
    assert failed["action"] == "block_oi_unavailable"
    assert failed["blocks_entry"] is True
    assert failed["terminality"] == "terminal_candidate_block"


def test_entry_market_evidence_contract_terminalizes_quote_rewarm():
    result = entry_market_evidence_contract(
        "runtime.entry_quote_rewarm_terminal_stale",
        {
            "venue": "aster",
            "symbol": "TRUSTUSDT",
            "action_taken": "skip_candidate_after_hard_rewarm",
        },
    )

    assert result["action"] == "terminal_candidate_rewarm"
    assert result["action_taken"] == "skip_candidate_after_hard_rewarm"
    assert result["blocks_entry"] is True
    assert result["terminality"] == "terminal_candidate_block"


def test_close_reconciliation_evidence_contract_keeps_flat_gap_non_blocking():
    result = close_reconciliation_evidence_contract(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "evidence_gap": True,
            "evidence_gap_reason": "missing_short_close_trade_statement",
            "statement_probe_status": "partial",
        },
        current_exchange_truth_clean=True,
    )

    assert result["phase"] == "CLOSE_RECONCILIATION"
    assert result["action"] == "terminal_flat_accounting_gap"
    assert result["terminality"] == "terminal_flat_accounting_gap"
    assert result["blocks_business_terminal"] is False
    assert result["diagnostic_severity"] == "info"


def test_close_reconciliation_evidence_contract_blocks_unclean_gap():
    result = close_reconciliation_evidence_contract(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "evidence_gap": True,
            "evidence_gap_reason": "missing_short_close_trade_statement",
        },
        current_exchange_truth_clean=False,
    )

    assert result["action"] == "unresolved_close_accounting_gap"
    assert result["terminality"] == "unresolved_close_accounting_gap"
    assert result["blocks_business_terminal"] is True
    assert result["diagnostic_severity"] == "critical"


def test_noise_visibility_classifies_resolved_close_artifact_as_historical():
    payload = {
        "position_id": "entry-h",
        "symbol": "HUSDT",
        "venue": "binance",
        "exchange_code": "-2022",
        "reason": "HTTP 400: ReduceOnly Order is rejected.",
        "request_context": {"reduce_only": True},
    }

    clean = classify_noise_visibility(
        "order.rejected",
        payload,
        current_exchange_truth_clean=True,
    )
    dirty = classify_noise_visibility(
        "order.rejected",
        payload,
        current_exchange_truth_clean=False,
    )

    assert clean["visibility"] == "historical_terminal_evidence"
    assert clean["blocks_gate"] is False
    assert clean["requires_operator_action"] is False
    assert clean["reason"] == "resolved_close_artifact_after_terminal_truth"
    assert dirty["visibility"] == "current_blocker"
    assert dirty["blocks_gate"] is True


def test_noise_visibility_keeps_entry_market_blocks_as_admission_evidence():
    result = classify_noise_visibility(
        "runtime.entry_quote_revalidate_failed",
        {"venue": "binance", "symbol": "STABLEUSDT", "reason": "quote_stale"},
        current_exchange_truth_clean=True,
    )

    assert result["visibility"] == "current_admission_blocker"
    assert result["blocks_gate"] is False
    assert result["requires_operator_action"] is False
    assert result["reason"] == "entry_market_evidence_block"


def test_noise_visibility_never_hides_current_single_leg_exposure():
    result = classify_noise_visibility(
        "recovery.unpaired_live_position_cleanup_skipped",
        {
            "position_id": "entry-risk",
            "symbol": "ESPORTSUSDT",
            "current_risk_exposure": True,
        },
        current_exchange_truth_clean=False,
    )

    assert result["visibility"] == "current_blocker"
    assert result["blocks_gate"] is True
    assert result["requires_operator_action"] is True
    assert result["reason"] == "current_single_leg_or_risk_only_exposure"


def test_noise_visibility_terminal_flat_downgrades_historical_unpaired_cleanup():
    result = classify_noise_visibility(
        "recovery.unpaired_live_position_cleanup_skipped",
        {
            "position_id": "entry-risk",
            "symbol": "ESPORTSUSDT",
            "current_risk_exposure": True,
        },
        current_exchange_truth_clean=True,
    )

    assert result["visibility"] == "historical_terminal_evidence"
    assert result["blocks_gate"] is False
    assert result["requires_operator_action"] is False
    assert result["reason"] == "terminal_flat_recovered_unpaired_cleanup"


def test_business_event_kind_maps_dual_taker_to_pending_entry():
    result = classify_business_event_kind(
        "execution.dual_taker_armed",
        {"entry_id": "entry-1", "symbol": "SIRENUSDT"},
    )

    assert result["phase"] == "PENDING_ENTRY"
    assert result["terminality"] == "terminal_fallback_armed"
    assert result["diagnostic_severity"] == "info"


def test_business_event_kind_maps_exit_dual_taker_to_passive_close():
    result = classify_business_event_kind(
        "execution.dual_taker_armed",
        {
            "position_id": "p001",
            "symbol": "BTCUSDT",
            "execution_kind": "exit",
        },
    )

    assert result["phase"] == "PASSIVE_CLOSE"
    assert result["terminality"] == "terminal_fallback_armed"
    assert result["action_taken"] == "execute_terminal_taker_fallback"
    assert result["diagnostic_severity"] == "info"


def test_business_event_kind_maps_passive_close_recovery_result():
    result = classify_business_event_kind(
        "runtime.passive_close_recovery_result",
        {"position_id": "entry-1", "decision": "RUNNING_CLEAN"},
    )

    assert result["phase"] == "PASSIVE_CLOSE"
    assert result["terminality"] == "terminal_truth_recorded"
    assert result["action_taken"] == "record_passive_close_recovery_result"
    assert result["diagnostic_severity"] == "info"


def test_business_event_kind_maps_compensation_already_flat_to_passive_close():
    result = classify_business_event_kind(
        "exit.compensation_already_flat",
        {
            "position_id": "entry-1",
            "symbol": "ALICEUSDT",
            "venue": "bybit",
            "reason": "funding_capture",
        },
    )

    assert result["phase"] == "PASSIVE_CLOSE"
    assert result["terminality"] == "terminal_flat_already_proven"
    assert result["action_taken"] == "record_compensation_terminal_flat"
    assert result["diagnostic_severity"] == "info"


def test_close_order_error_resolution_requires_clean_exchange_truth():
    payload = {
        "position_id": "entry-1",
        "symbol": "SAHARAUSDT",
        "venue": "binance",
        "exchange_code": "-2022",
        "reason": "HTTP 400: ReduceOnly Order is rejected.",
        "request_context": {"reduce_only": True},
    }

    clean = close_order_error_resolution_contract(
        kind="order.rejected",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=False,
        has_order_identity=False,
    )
    dirty = close_order_error_resolution_contract(
        kind="order.rejected",
        payload=payload,
        current_exchange_truth_clean=False,
        position_terminal_match=True,
        order_terminal_match=False,
        has_order_identity=False,
    )

    assert clean["resolved"] is True
    assert clean["resolution_bucket"] == "reduce_only_terminal_flat"
    assert dirty["resolved"] is False


def test_close_order_error_resolution_reads_nested_exchange_error_payload():
    payload = {
        "position_id": "entry-1",
        "symbol": "SAHARAUSDT",
        "venue": "binance",
        "exchange_error": {
            "exchange_code": "-2022",
            "exchange_msg": "ReduceOnly Order is rejected.",
            "raw_body": '{"code":-2022,"msg":"ReduceOnly Order is rejected."}',
            "request_context": {"reduce_only": True},
        },
    }

    result = close_order_error_resolution_contract(
        kind="order.rejected",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=False,
        has_order_identity=False,
    )

    assert result["resolved"] is True
    assert result["resolution_bucket"] == "reduce_only_terminal_flat"


def test_close_order_error_resolution_requires_reduce_only_context():
    payload = {
        "position_id": "entry-1",
        "symbol": "SAHARAUSDT",
        "venue": "binance",
        "exchange_error": {
            "exchange_code": "-2022",
            "exchange_msg": "ReduceOnly Order is rejected.",
            "request_context": {"reduce_only": False},
        },
    }

    result = close_order_error_resolution_contract(
        kind="order.rejected",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=False,
        has_order_identity=False,
    )

    assert result["resolved"] is False


def test_passive_close_final_truth_contract_clears_flat_truth():
    result = passive_close_final_truth_contract(
        {
            "truth_available": True,
            "positions_flat": True,
            "open_orders_flat": True,
            "positions": [
                {"venue": "bybit", "symbol": "ESPORTSUSDT", "quantity": 0.0},
                {"venue": "binance", "symbol": "ESPORTSUSDT", "quantity": 0.0},
            ],
            "open_order_truth": [
                {"venue": "bybit", "symbol": "ESPORTSUSDT", "open_orders_empty": True},
                {"venue": "binance", "symbol": "ESPORTSUSDT", "open_orders_empty": True},
            ],
        },
        long_venue="bybit",
        short_venue="binance",
    )

    assert result["action"] == "clear_flat"
    assert result["terminal"] is True


def test_passive_close_final_truth_contract_routes_trusted_single_leg_to_flatten():
    result = passive_close_final_truth_contract(
        {
            "truth_available": True,
            "positions_flat": False,
            "open_orders_flat": True,
            "positions": [
                {"venue": "bybit", "symbol": "ESPORTSUSDT", "quantity": 0.0},
                {"venue": "binance", "symbol": "ESPORTSUSDT", "quantity": 470.0},
            ],
            "open_order_truth": [
                {"venue": "bybit", "symbol": "ESPORTSUSDT", "open_orders_empty": True},
                {"venue": "binance", "symbol": "ESPORTSUSDT", "open_orders_empty": True},
            ],
        },
        long_venue="bybit",
        short_venue="binance",
    )

    assert result["action"] == "flatten_remaining_live_leg"
    assert result["terminal"] is False
    assert result["leg_label"] == "short"
    assert result["venue"] == "binance"
    assert result["quantity"] == pytest.approx(470.0)
    assert result["next_action"] == "flatten_remaining_live_leg"


def test_passive_close_final_truth_contract_retains_untrusted_truth():
    result = passive_close_final_truth_contract(
        {
            "truth_available": False,
            "positions_flat": None,
            "open_orders_flat": None,
            "positions": [],
            "open_order_truth": [],
            "missing_evidence": ["position_snapshot"],
        },
        long_venue="bybit",
        short_venue="binance",
    )

    assert result["action"] == "retain_untrusted_truth"
    assert result["next_action"] == "retain_untrusted_truth"
    assert result["terminal"] is False


def test_passive_close_final_truth_contract_blocks_ownerless_open_order():
    result = passive_close_final_truth_contract(
        {
            "truth_available": True,
            "positions_flat": False,
            "open_orders_flat": False,
            "positions": [
                {"venue": "bybit", "symbol": "ESPORTSUSDT", "quantity": 0.0},
                {"venue": "binance", "symbol": "ESPORTSUSDT", "quantity": 470.0},
            ],
            "open_order_truth": [
                {"venue": "bybit", "symbol": "ESPORTSUSDT", "open_orders_empty": True},
                {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "open_orders_empty": False,
                    "evidence": "open_orders_count=1",
                },
            ],
        },
        long_venue="bybit",
        short_venue="binance",
    )

    assert result["action"] == "adopt_or_block_existing_close_order"
    assert result["next_action"] == "adopt_existing_reduce_only_close_order"
    assert result["terminal"] is False
