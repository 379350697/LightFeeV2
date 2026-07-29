from __future__ import annotations

import pytest

from lightfee.engine.business_contract import (
    classify_business_event_kind,
    classify_close_reconciliation_state,
    classify_entry_quantity_contract,
    classify_noise_visibility,
    close_reconciliation_exchange_truth,
    close_reconciliation_exchange_truth_clean,
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


def test_entry_market_evidence_contract_keeps_prewarm_quote_failures_non_blocking():
    result = entry_market_evidence_contract(
        "runtime.entry_quote_revalidate_failed",
        {
            "venue": "bitget",
            "symbol": "GATEUSDT",
            "reason_bucket": "rest_resolved_but_stale",
            "reason_family": "rest_invalid_quote",
            "evidence_role": "prewarm_only",
            "candidate_scope": "prewarm_extra",
        },
    )

    assert result["phase"] == "ENTRY_MARKET_EVIDENCE"
    assert result["evidence_class"] == "quote"
    assert result["action"] == "refresh_evidence"
    assert result["blocks_entry"] is False
    assert result["terminality"] == "active"
    assert result["diagnostic_severity"] == "info"


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


def test_entry_market_evidence_contract_keeps_prewarm_oi_failures_non_blocking():
    result = entry_market_evidence_contract(
        "runtime.entry_oi_targeted_refresh_failed",
        {
            "venue": "gate",
            "symbol": "ALICEUSDT",
            "previous_open_interest_evidence_status": "deferred_by_cap",
            "open_interest_evidence_status": "timeout",
            "open_interest_evidence_reason": "timeout_waiting_for_oi",
            "evidence_role": "prewarm_only",
            "candidate_scope": "prewarm_extra",
        },
    )

    assert result["evidence_class"] == "oi"
    assert result["action"] == "refresh_evidence"
    assert result["blocks_entry"] is False
    assert result["terminality"] == "active"


def test_entry_market_evidence_contract_distinguishes_low_oi_from_unavailable():
    structural = entry_market_evidence_contract(
        "execution.entry_liquidity_blocked",
        {
            "venue": "aster",
            "symbol": "HUSDT",
            "reason": "perp_open_interest_structural",
            "open_interest_evidence_status": "available",
            "observed_open_interest_quote": 432_987.0,
            "min_open_interest_quote": 1_000_000.0,
        },
    )
    below_floor = entry_market_evidence_contract(
        "execution.entry_liquidity_blocked",
        {
            "venue": "hyperliquid",
            "symbol": "DYMUSDT",
            "reason": "perp_open_interest_below_floor",
            "open_interest_evidence_status": "available",
            "observed_open_interest_quote": 209_347.0,
            "min_open_interest_quote": 1_000_000.0,
        },
    )
    unavailable = entry_market_evidence_contract(
        "execution.entry_liquidity_blocked",
        {
            "venue": "binance",
            "symbol": "BSBUSDT",
            "reason": "oi_evidence_unavailable",
            "open_interest_evidence_status": "timeout",
        },
    )

    assert structural["action"] == "block_oi_structural"
    assert structural["blocks_entry"] is True
    assert below_floor["action"] == "block_oi_below_floor"
    assert below_floor["blocks_entry"] is True
    assert unavailable["action"] == "block_oi_unavailable"
    assert unavailable["blocks_entry"] is True


def test_entry_market_evidence_contract_treats_structural_suppression_as_backoff():
    result = entry_market_evidence_contract(
        "execution.entry_liquidity_blocked",
        {
            "venue": "aster",
            "symbol": "ESPORTSUSDT",
            "reason": "perp_open_interest_structural",
            "open_interest_evidence_status": "available",
            "structural_suppressed": True,
            "next_structural_recheck_ms": 1779817850000,
        },
    )

    assert result["action"] == "suppress_oi_structural"
    assert result["blocks_entry"] is False
    assert result["terminality"] == "structural_backoff_suppressed"
    assert result["diagnostic_severity"] == "info"


def test_entry_market_evidence_contract_infers_structural_backoff_from_ttl():
    payload = {
        "venue": "hyperliquid",
        "symbol": "0GUSDT",
        "reason": "perp_open_interest_structural",
        "open_interest_evidence_status": "available",
        "consecutive_failures": 310,
        "suppress_until_ms": 1779817850000,
    }

    result = entry_market_evidence_contract(
        "execution.entry_liquidity_blocked",
        payload,
    )
    visibility = classify_noise_visibility(
        "execution.entry_liquidity_blocked",
        payload,
        current_exchange_truth_clean=True,
    )

    assert result["action"] == "suppress_oi_structural"
    assert result["blocks_entry"] is False
    assert result["terminality"] == "structural_backoff_suppressed"
    assert visibility["visibility"] == "aggregated_diagnostic"


def test_entry_market_evidence_contract_treats_expired_structural_backoff_as_blocker():
    result = entry_market_evidence_contract(
        "execution.entry_liquidity_blocked",
        {
            "venue": "hyperliquid",
            "symbol": "0GUSDT",
            "reason": "perp_open_interest_structural",
            "open_interest_evidence_status": "available",
            "consecutive_failures": 310,
            "suppress_until_ms": 1779816049000,
            "last_structural_probe_at_ms": 1779816050000,
        },
    )

    assert result["action"] == "block_oi_structural"
    assert result["blocks_entry"] is True
    assert result["terminality"] == "terminal_candidate_block"
    assert result["diagnostic_severity"] == "production_issue"


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


def test_close_reconciliation_evidence_contract_does_not_trust_terminal_marker_without_truth():
    result = close_reconciliation_evidence_contract(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "evidence_gap": True,
            "evidence_gap_reason": "missing_short_close_trade_statement",
            "close_reconciliation_state": "terminal_flat_accounting_gap",
        },
        current_exchange_truth_clean=False,
    )

    assert result["action"] == "unresolved_close_accounting_gap"
    assert result["terminality"] == "unresolved_close_accounting_gap"
    assert result["blocks_business_terminal"] is True
    assert result["diagnostic_severity"] == "critical"


def test_close_reconciliation_evidence_contract_rejects_unscoped_frozen_exchange_truth_when_current_probe_dirty():
    result = close_reconciliation_evidence_contract(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "evidence_gap": True,
            "evidence_gap_reason": "missing_short_close_trade_statement",
            "close_reconciliation_state": "terminal_flat_accounting_gap",
            "exchange_truth": {
                "truth_available": True,
                "positions_flat": True,
                "open_orders_flat": True,
                "source": "passive_close_final_exchange_truth_gate",
            },
        },
        current_exchange_truth_clean=False,
    )

    assert result["action"] == "unresolved_close_accounting_gap"
    assert result["terminality"] == "unresolved_close_accounting_gap"
    assert result["blocks_business_terminal"] is True
    assert result["diagnostic_severity"] == "critical"


def test_close_reconciliation_state_releases_terminal_flat_accounting_gap():
    result = classify_close_reconciliation_state(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "long_venue": "binance",
            "short_venue": "bybit",
            "pending_backfill": True,
            "last_evidence_gap_reason": "missing_short_close_trade_statement",
        },
        current_exchange_truth_clean=True,
    )

    assert result["state"] == "terminal_flat_accounting_gap"
    assert result["blocks_entry"] is False
    assert result["archive_reconciliation"] is True
    assert result["reason"] == "missing_short_close_trade_statement"


def test_close_reconciliation_state_releases_accepted_order_truth_gap_when_flat():
    result = classify_close_reconciliation_state(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "long_venue": "aster",
            "short_venue": "bybit",
            "kind": "accepted_order_truth_gap",
            "accepted_order_truth_gap": True,
            "truth_required_by": "accepted_order_truth_gap",
            "reason": "first_stage_capture",
        },
        current_exchange_truth_clean=True,
    )

    assert result["state"] == "terminal_flat_accounting_gap"
    assert result["blocks_entry"] is False
    assert result["archive_reconciliation"] is True
    assert result["reason"] == "accepted_order_truth_gap"


def test_close_reconciliation_state_blocks_accepted_order_truth_gap_without_truth():
    result = classify_close_reconciliation_state(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "long_venue": "aster",
            "short_venue": "bybit",
            "kind": "accepted_order_truth_gap",
            "accepted_order_truth_gap": True,
            "truth_required_by": "accepted_order_truth_gap",
        },
        current_exchange_truth_clean=False,
    )

    assert result["state"] == "truth_unavailable"
    assert result["blocks_entry"] is True
    assert result["archive_reconciliation"] is False
    assert result["reason"] == "accepted_order_truth_gap"


def test_close_reconciliation_state_blocks_when_truth_unavailable():
    result = classify_close_reconciliation_state(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "long_venue": "binance",
            "short_venue": "bybit",
            "pending_backfill": True,
            "last_evidence_gap_reason": "missing_short_close_trade_statement",
        },
        current_exchange_truth_clean=False,
    )

    assert result["state"] == "truth_unavailable"
    assert result["blocks_entry"] is True
    assert result["archive_reconciliation"] is False


def test_close_reconciliation_state_does_not_trust_terminal_marker_without_truth():
    result = classify_close_reconciliation_state(
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "pending_backfill": True,
            "close_reconciliation_state": "terminal_flat_accounting_gap",
            "last_evidence_gap_reason": "missing_short_close_trade_statement",
        },
        current_exchange_truth_clean=False,
    )

    assert result["state"] == "truth_unavailable"
    assert result["blocks_entry"] is True
    assert result["archive_reconciliation"] is False


def test_close_reconciliation_state_catalog_diagnostic_never_becomes_truth_blocker():
    result = classify_close_reconciliation_state(
        {
            "symbol": "HOMEUSDT",
            "evidence_gap": True,
            "close_reconciliation_state": "catalog_diagnostic",
            "reason": "unsupported_symbol_probe_filtered",
        },
        current_exchange_truth_clean=False,
    )

    assert result["state"] == "catalog_diagnostic"
    assert result["blocks_entry"] is False
    assert result["archive_reconciliation"] is True
    assert result["reason"] == "unsupported_symbol_probe_filtered"


def _account_level_flat_truth(*venues: str) -> dict:
    return {
        "truth_available": True,
        "available": True,
        "confidence": "high",
        "has_nonzero_position": False,
        "has_open_order": False,
        "positions": {venue: {} for venue in venues},
        "open_orders": {venue: {"*": []} for venue in venues},
        "position_probe_evidence": {
            venue: {
                "*": {
                    "classification": "position_probe_unfiltered_succeeded",
                    "position_count": 0,
                },
            }
            for venue in venues
        },
        "open_order_probe_evidence": {
            venue: {
                "*": {
                    "classification": "open_order_probe_unfiltered_succeeded",
                    "order_count": 0,
                },
            }
            for venue in venues
        },
        "fetch_status": {
            venue: {
                "status": "ok",
                "positions_succeeded": ["*"],
                "positions_failed": [],
                "orders_succeeded": ["*"],
                "orders_failed": [],
                "positions_unsupported_symbols": [],
                "orders_unsupported_symbols": [],
            }
            for venue in venues
        },
    }


def _recovery_ledger_flat_truth(symbol: str, *venues: str) -> dict:
    return {
        "truth_supported": True,
        "truth_available": True,
        "positions": [
            {
                "venue": venue,
                "symbol": symbol,
                "side": "buy",
                "quantity": 0.0,
                "entry_price": 0.0,
            }
            for venue in venues
        ],
        "open_orders": [],
        "probe_evidence": [
            {
                "venue": venue,
                "symbol": symbol,
                "endpoint": "fetch_position",
                "method": "fetch_position",
                "classification": "position_truth",
            }
            for venue in venues
        ]
        + [
            {
                "venue": venue,
                "symbol": symbol,
                "endpoint": "fetch_open_orders",
                "method": "fetch_open_orders",
                "classification": "open_order_truth",
            }
            for venue in venues
        ],
    }


def _recovery_ledger_account_flat_truth(*venues: str) -> dict:
    return {
        "truth_supported": True,
        "truth_available": False,
        "positions": [],
        "open_orders": [],
        "probe_evidence": [
            {
                "venue": venue,
                "symbol": "*",
                "endpoint": "fetch_all_positions",
                "method": "fetch_all_positions",
                "classification": "position_probe_unfiltered_succeeded",
            }
            for venue in venues
        ]
        + [
            {
                "venue": venue,
                "symbol": "*",
                "endpoint": "fetch_open_orders(None)",
                "method": "fetch_open_orders",
                "classification": "open_order_probe_unfiltered_succeeded",
            }
            for venue in venues
        ]
        + [
            {
                "venue": "hyperliquid",
                "symbol": "*",
                "endpoint": "fetch_all_positions",
                "method": "fetch_all_positions",
                "classification": "position_probe_unfiltered_failed",
                "error": "account_wallet_signer_mismatch",
            }
        ],
        "errors": ["hyperliquid:*:positions:account_wallet_signer_mismatch"],
    }


def test_close_reconciliation_exchange_truth_accepts_account_level_flat_probe():
    reconciliation = {
        "position_id": "entry-h",
        "symbol": "HUSDT",
        "long_venue": "bybit",
        "short_venue": "okx",
        "pending_backfill": True,
        "last_evidence_gap_reason": "missing_long_close_trade_statement",
    }
    truth = _account_level_flat_truth("bybit", "okx")

    assert (
        close_reconciliation_exchange_truth(
            reconciliation,
            current_exchange_truth=truth,
        )
        is truth
    )
    assert close_reconciliation_exchange_truth_clean(
        reconciliation,
        current_exchange_truth=truth,
    ) is True


def test_close_reconciliation_exchange_truth_accepts_recovery_ledger_flat_probe():
    reconciliation = {
        "position_id": "entry-h",
        "symbol": "HUSDT",
        "long_venue": "bybit",
        "short_venue": "okx",
        "pending_backfill": True,
        "last_evidence_gap_reason": "missing_long_close_trade_statement",
    }
    truth = _recovery_ledger_flat_truth("HUSDT", "bybit", "okx")

    assert (
        close_reconciliation_exchange_truth(
            reconciliation,
            current_exchange_truth=truth,
        )
        is truth
    )
    assert close_reconciliation_exchange_truth_clean(
        reconciliation,
        current_exchange_truth=truth,
    ) is True


def test_close_reconciliation_exchange_truth_accepts_recovery_account_flat_probe():
    reconciliation = {
        "position_id": "entry-h",
        "symbol": "HUSDT",
        "long_venue": "bybit",
        "short_venue": "okx",
        "pending_backfill": True,
        "last_evidence_gap_reason": "missing_long_close_trade_statement",
    }
    truth = _recovery_ledger_account_flat_truth("bybit", "okx")

    assert (
        close_reconciliation_exchange_truth(
            reconciliation,
            current_exchange_truth=truth,
        )
        is truth
    )
    assert close_reconciliation_exchange_truth_clean(
        reconciliation,
        current_exchange_truth=truth,
    ) is True


def test_close_reconciliation_exchange_truth_rejects_target_account_truth_error():
    reconciliation = {
        "position_id": "entry-h",
        "symbol": "HUSDT",
        "long_venue": "bybit",
        "short_venue": "okx",
        "pending_backfill": True,
    }
    truth = _recovery_ledger_account_flat_truth("bybit", "okx")
    truth["errors"] = ["okx:*:open_orders:timeout"]
    truth["probe_evidence"].append({
        "venue": "okx",
        "symbol": "*",
        "endpoint": "fetch_open_orders(None)",
        "method": "fetch_open_orders",
        "classification": "open_order_probe_unfiltered_failed",
        "error": "timeout",
    })

    assert close_reconciliation_exchange_truth(
        reconciliation,
        current_exchange_truth=truth,
    ) is None


def test_close_reconciliation_exchange_truth_accepts_unfiltered_unrelated_position():
    reconciliation = {
        "position_id": "entry-h",
        "symbol": "HUSDT",
        "long_venue": "bybit",
        "short_venue": "okx",
        "pending_backfill": True,
    }
    truth = _recovery_ledger_account_flat_truth("bybit", "okx")
    truth["positions"] = [{
        "venue": "bybit",
        "symbol": "DEXEUSDT",
        "quantity": 1.0,
    }]

    assert (
        close_reconciliation_exchange_truth(
            reconciliation,
            current_exchange_truth=truth,
        )
        is truth
    )


def test_close_reconciliation_exchange_truth_rejects_unfiltered_target_symbol_order():
    reconciliation = {
        "position_id": "entry-h",
        "symbol": "HUSDT",
        "long_venue": "bybit",
        "short_venue": "okx",
        "pending_backfill": True,
    }
    truth = _recovery_ledger_account_flat_truth("bybit", "okx")
    truth["open_orders"] = [{
        "venue": "bybit",
        "symbol": "HUSDT",
        "quantity": 1.0,
    }]

    assert close_reconciliation_exchange_truth(
        reconciliation,
        current_exchange_truth=truth,
    ) is None


def test_close_reconciliation_exchange_truth_rejects_incomplete_account_level_probe():
    reconciliation = {
        "position_id": "entry-h",
        "symbol": "HUSDT",
        "long_venue": "bybit",
        "short_venue": "okx",
        "pending_backfill": True,
    }
    truth = _account_level_flat_truth("bybit")

    assert close_reconciliation_exchange_truth(
        reconciliation,
        current_exchange_truth=truth,
    ) is None
    assert close_reconciliation_exchange_truth_clean(
        reconciliation,
        current_exchange_truth=truth,
    ) is False


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
    for kind in (
        "runtime.entry_quote_revalidate_failed",
        "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
        "runtime.entry_quote_rewarm_terminal_stale",
    ):
        result = classify_noise_visibility(
            kind,
            {"venue": "binance", "symbol": "STABLEUSDT", "reason": "quote_stale"},
            current_exchange_truth_clean=True,
        )

        assert result["visibility"] == "current_admission_blocker"
        assert result["blocks_gate"] is False
        assert result["requires_operator_action"] is False
        assert result["reason"] == "entry_market_evidence_block"
        assert result["scope"] == "entry_candidate_admission"


@pytest.mark.parametrize(
    "payload",
    [
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "venue": "bybit",
            "exchange_code": "110072",
            "exchange_msg": "OrderLinkedID is duplicate",
        },
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "venue": "binance",
            "exchange_code": "-5022",
            "exchange_msg": "Post Only order will be rejected.",
            "request_context": {"post_only": True, "reduce_only": True},
        },
        {
            "position_id": "entry-h",
            "symbol": "HUSDT",
            "venue": "binance",
            "reason": "zero fill order uncertain",
        },
    ],
)
def test_noise_visibility_classifies_known_close_artifacts_by_truth(payload):
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
    assert dirty["requires_operator_action"] is True
    assert dirty["reason"] == "unresolved_close_artifact"


def test_noise_visibility_classifies_unsupported_symbol_probe_as_catalog_diagnostic():
    result = classify_noise_visibility(
        "recovery.live_position_probe_unsupported_symbols",
        {
            "venue": "okx",
            "unsupported_symbols": ["HOMEUSDT", "GUNUSDT"],
            "classification": "unsupported_symbol_flat",
        },
        current_exchange_truth_clean=True,
    )

    assert result["visibility"] == "catalog_diagnostic"
    assert result["blocks_gate"] is False
    assert result["requires_operator_action"] is False
    assert result["reason"] == "unsupported_symbol_probe_filtered"
    assert result["scope"] == "exchange_truth_catalog_filter"


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


def test_noise_visibility_classifies_route_cooldown_as_admission_blocker():
    event = classify_business_event_kind(
        "runtime.route_abnormal_cooldown_armed",
        {
            "symbol": "HOMEUSDT",
            "route_key": "route:HOMEUSDT:binance->bybit",
            "reason": "route_abnormal_terminal_cooldown",
        },
    )
    visibility = classify_noise_visibility(
        "runtime.route_abnormal_cooldown_armed",
        {
            "symbol": "HOMEUSDT",
            "route_key": "route:HOMEUSDT:binance->bybit",
            "reason": "route_abnormal_terminal_cooldown",
        },
        current_exchange_truth_clean=True,
    )

    assert event["phase"] == "ENTRY_MARKET_EVIDENCE"
    assert event["blocks_entry"] is True
    assert event["evidence_class"] == "route"
    assert visibility["visibility"] == "current_admission_blocker"
    assert visibility["blocks_gate"] is False
    assert visibility["requires_operator_action"] is False
    assert visibility["scope"] == "entry_candidate_admission"


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

    assert clean["resolved"] is False
    assert dirty["resolved"] is False


def test_close_order_error_resolution_requires_order_identity_match_for_reduce_only():
    payload = {
        "position_id": "entry-1",
        "symbol": "SAHARAUSDT",
        "venue": "binance",
        "client_order_id": "close-leg-1",
        "exchange_code": "-2022",
        "reason": "HTTP 400: ReduceOnly Order is rejected.",
        "request_context": {"reduce_only": True},
    }

    unmatched = close_order_error_resolution_contract(
        kind="order.rejected",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=False,
        has_order_identity=True,
    )
    matched = close_order_error_resolution_contract(
        kind="order.rejected",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=True,
        has_order_identity=True,
    )

    assert unmatched["resolved"] is False
    assert matched == {
        "resolved": True,
        "resolution_bucket": "reduce_only_terminal_flat",
    }


def test_close_order_error_resolution_accepts_aster_reduce_only_reject_with_position_terminal_no_order():
    payload = {
        "position_id": "entry-1782816837656-LABUSDT",
        "symbol": "LABUSDT",
        "venue": "aster",
        "exchange_code": "-2022",
        "exchange_msg": "ReduceOnly Order is rejected.",
        "exchange_error": {
            "venue": "aster",
            "operation": "place_order",
            "http_status": 400,
            "raw_body": '{"code":-2022,"msg":"ReduceOnly Order is rejected."}',
            "exchange_code": "-2022",
            "exchange_msg": "ReduceOnly Order is rejected.",
            "request_context": {
                "symbol": "LABUSDT",
                "side": "sell",
                "time_in_force": "IOC",
                "quantity": 3.0,
                "price": 0.0,
                "reduce_only": True,
                "client_order_id": "lfxlfafdaee235c94d4e",
            },
        },
        "request_context": {
            "symbol": "LABUSDT",
            "side": "sell",
            "time_in_force": "IOC",
            "quantity": 3.0,
            "price": 0.0,
            "reduce_only": True,
            "client_order_id": "lfxlfafdaee235c94d4e",
        },
    }

    result = close_order_error_resolution_contract(
        kind="order.rejected",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=False,
        has_order_identity=True,
    )

    assert result == {
        "resolved": True,
        "resolution_bucket": "reduce_only_terminal_flat",
    }


def test_close_order_error_resolution_accepts_bybit_zero_position_reject_with_position_terminal():
    payload = {
        "position_id": "entry-1782748326583-POWRUSDT",
        "symbol": "POWRUSDT",
        "venue": "bybit",
        "exchange_code": "110017",
        "reason": "current position is zero, cannot fix reduce-only order qty",
        "request_context": {
            "reduce_only": True,
            "client_order_id": "lfex25de598b3bf652c6",
            "symbol": "POWRUSDT",
        },
        "exchange_error": {
            "venue": "bybit",
            "exchange_code": "110017",
            "exchange_msg": "current position is zero, cannot fix reduce-only order qty",
            "request_context": {
                "reduce_only": True,
                "client_order_id": "lfex25de598b3bf652c6",
            },
        },
    }

    result = close_order_error_resolution_contract(
        kind="exit.passive_close_hedge_error",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=False,
        has_order_identity=True,
    )
    dirty = close_order_error_resolution_contract(
        kind="exit.passive_close_hedge_error",
        payload=payload,
        current_exchange_truth_clean=False,
        position_terminal_match=True,
        order_terminal_match=False,
        has_order_identity=True,
    )

    assert result == {
        "resolved": True,
        "resolution_bucket": "reduce_only_terminal_flat",
    }
    assert dirty["resolved"] is False


def test_close_order_error_resolution_treats_gate_empty_position_as_terminal_flat():
    payload = {
        "position_id": "entry-1",
        "symbol": "HUSDT",
        "venue": "gate",
        "client_order_id": "close-leg-1",
        "exchange_code": "REDUCE_EXCEEDED",
        "exchange_msg": "empty position",
        "exchange_error": {
            "exchange_code": "REDUCE_EXCEEDED",
            "exchange_msg": "empty position",
            "raw_body": '{"label":"REDUCE_EXCEEDED","message":"empty position"}',
            "request_context": {
                "reduce_only": True,
                "client_order_id": "close-leg-1",
            },
            "extra": {
                "label": "REDUCE_EXCEEDED",
                "message": "empty position",
            },
        },
        "request_context": {
            "reduce_only": True,
            "client_order_id": "close-leg-1",
        },
    }

    result = close_order_error_resolution_contract(
        kind="exit.passive_close_maker_submit_error",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=True,
        has_order_identity=True,
    )

    assert result == {
        "resolved": True,
        "resolution_bucket": "reduce_only_terminal_flat",
    }


def test_close_order_error_resolution_reads_gate_empty_position_from_raw_body_text():
    payload = {
        "position_id": "entry-1",
        "symbol": "HUSDT",
        "venue": "gate",
        "client_order_id": "close-leg-1",
        "exchange_error": {
            "raw_body": '{"label":"REDUCE_EXCEEDED","message":"empty position"}',
            "request_context": {
                "reduce_only": True,
                "client_order_id": "close-leg-1",
            },
        },
        "request_context": {
            "reduce_only": True,
            "client_order_id": "close-leg-1",
        },
    }

    result = close_order_error_resolution_contract(
        kind="exit.passive_close_maker_submit_error",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=True,
        has_order_identity=True,
    )

    assert result == {
        "resolved": True,
        "resolution_bucket": "reduce_only_terminal_flat",
    }


def test_close_order_error_resolution_reads_gate_empty_position_from_nested_raw_body_with_generic_error():
    payload = {
        "position_id": "entry-1",
        "symbol": "HUSDT",
        "venue": "gate",
        "client_order_id": "close-leg-1",
        "error": "HTTP 400",
        "exchange_error": {
            "raw_body": '{"label":"REDUCE_EXCEEDED","message":"empty position"}',
            "request_context": {
                "reduce_only": True,
                "client_order_id": "close-leg-1",
            },
        },
        "request_context": {
            "reduce_only": True,
            "client_order_id": "close-leg-1",
        },
    }

    result = close_order_error_resolution_contract(
        kind="exit.passive_close_maker_submit_error",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=True,
        has_order_identity=True,
    )

    assert result == {
        "resolved": True,
        "resolution_bucket": "reduce_only_terminal_flat",
    }


def test_close_order_error_resolution_does_not_treat_gate_empty_position_without_reduce_only_as_terminal_flat():
    payload = {
        "position_id": "entry-1",
        "symbol": "HUSDT",
        "venue": "gate",
        "client_order_id": "close-leg-1",
        "exchange_code": "REDUCE_EXCEEDED",
        "exchange_msg": "empty position",
        "request_context": {
            "reduce_only": False,
            "client_order_id": "close-leg-1",
        },
    }

    result = close_order_error_resolution_contract(
        kind="exit.passive_close_maker_submit_error",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=True,
        has_order_identity=True,
    )

    assert result["resolved"] is False


def test_close_order_error_resolution_does_not_treat_gate_pending_conflict_as_terminal_flat():
    payload = {
        "position_id": "entry-1",
        "symbol": "HUSDT",
        "venue": "gate",
        "client_order_id": "close-leg-1",
        "exchange_code": "REDUCE_EXCEEDED",
        "exchange_msg": "pending order blocks reduce order",
        "exchange_error": {
            "exchange_code": "REDUCE_EXCEEDED",
            "exchange_msg": "pending order blocks reduce order",
            "raw_body": '{"label":"REDUCE_EXCEEDED","message":"pending order blocks reduce order"}',
            "request_context": {
                "reduce_only": True,
                "client_order_id": "close-leg-1",
            },
            "extra": {
                "label": "REDUCE_EXCEEDED",
                "message": "pending order blocks reduce order",
            },
        },
        "request_context": {
            "reduce_only": True,
            "client_order_id": "close-leg-1",
        },
    }

    result = close_order_error_resolution_contract(
        kind="exit.passive_close_maker_submit_error",
        payload=payload,
        current_exchange_truth_clean=True,
        position_terminal_match=True,
        order_terminal_match=True,
        has_order_identity=True,
    )

    assert result["resolved"] is False


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

    assert result["resolved"] is False


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
