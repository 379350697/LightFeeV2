from __future__ import annotations

import pytest

from lightfee.engine.business_contract import (
    classify_business_event_kind,
    classify_entry_quantity_contract,
    close_order_error_resolution_contract,
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


def test_business_event_kind_maps_dual_taker_to_pending_entry():
    result = classify_business_event_kind(
        "execution.dual_taker_armed",
        {"entry_id": "entry-1", "symbol": "SIRENUSDT"},
    )

    assert result["phase"] == "PENDING_ENTRY"
    assert result["terminality"] == "terminal_fallback_armed"
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
