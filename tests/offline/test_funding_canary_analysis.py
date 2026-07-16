from __future__ import annotations

import pytest

from lightfee.engine.exit import seal_execution_benchmark_receipt
from lightfee.offline.funding_canary_analysis import (
    analyze_funding_canary_events,
    sign_funding_canary_approved_policy,
)


@pytest.fixture(autouse=True)
def _execution_benchmark_integrity_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LIGHTFEE_EXECUTION_BENCHMARK_HMAC_KEY", "canary-test-secret")
    monkeypatch.setenv("LIGHTFEE_CANARY_POLICY_HMAC_KEY", "policy-test-secret")


def _event(entry_id: str, seq: int, kind: str, payload: dict) -> dict:
    return {
        "run_id": f"run-{entry_id}",
        "seq": seq,
        "ts_ms": seq * 1_000,
        "kind": kind,
        "payload": payload,
    }


def _selected_payload(entry_id: str) -> dict:
    return {
        "entry_id": entry_id,
        "funding_canary": True,
        "runtime_mode": "live",
        "funding_canary_policy_version": "funding-canary-v1",
        "funding_canary_cohort_id": "fixture-cohort-v1",
        "funding_canary_hard_max_entry_notional_quote": 30.0,
        "funding_canary_hard_min_expected_net_edge_bps": 8.0,
        "funding_canary_hard_min_worst_case_edge_bps": 3.0,
        "expected_net_edge_bps": 10.0,
        "worst_case_edge_bps": 5.0,
        "planned_entry_notional_quote": 20.0,
        "planned_entry_quantity": 1.0,
        "planned_long_entry_price": 20.0,
        "planned_short_entry_price": 20.0,
        "economics_complete": True,
        "account_fee_evidence_complete": True,
        "account_fee_evidence_integrity_verified": True,
        "account_fee_evidence_identity_bound": True,
        "canary_budgeted_execution_cost_bps": 3.0,
        "canary_execution_reserve_bps": 2.0,
    }


def _signed_exit_receipt(
    entry_id: str,
    *,
    shortfall_quote: float = 0.0,
    quantity: float = 1.0,
    fee_quote: float = 0.0,
) -> dict:
    sealed = seal_execution_benchmark_receipt(
        {
            "source": "local_l2_vwap",
            "position_id": entry_id,
            "symbol": "BTCUSDT",
            "captured_at_ms": 1_000,
            "max_observation_to_submit_ms": 1_000,
            "requested_base_quantity": quantity,
            "long": {
                "venue": "cheap",
                "side": "sell",
                "vwap_price": 20.0,
                "available_base_quantity": quantity,
                "observed_at_ms": 995,
                "age_ms": 5,
                "filled_base_quantity": quantity,
                "implementation_shortfall_quote": 0.0,
                "fills": [
                    {
                        "order_id": "long-close",
                        "client_order_id": "long-close-client",
                        "submitted_at_ms": 1_000,
                        "filled_at_ms": 1_001,
                        "quantity": quantity,
                        "price": 20.0,
                        "fee_quote": fee_quote,
                    }
                ],
            },
            "short": {
                "venue": "rich",
                "side": "buy",
                "vwap_price": 20.0,
                "available_base_quantity": quantity,
                "observed_at_ms": 995,
                "age_ms": 5,
                "filled_base_quantity": quantity,
                "implementation_shortfall_quote": 0.0,
                "fills": [
                    {
                        "order_id": "short-close",
                        "client_order_id": "short-close-client",
                        "submitted_at_ms": 1_000,
                        "filled_at_ms": 1_001,
                        "quantity": quantity,
                        "price": 20.0,
                        "fee_quote": fee_quote,
                    }
                ],
            },
            "implementation_shortfall_quote": shortfall_quote,
        }
    )
    assert sealed is not None
    return sealed


def _signed_entry_receipt(
    entry_id: str,
    *,
    shortfall_quote: float = 0.0,
    fee_quote: float = 0.0,
) -> dict:
    sealed = seal_execution_benchmark_receipt(
        {
            "source": "local_l2_vwap",
            "position_id": entry_id,
            "symbol": "BTCUSDT",
            "captured_at_ms": 1_000,
            "max_observation_to_submit_ms": 1_000,
            "requested_base_quantity": 1.0,
            "long": {
                "venue": "cheap",
                "side": "buy",
                "vwap_price": 20.0,
                "available_base_quantity": 1.0,
                "observed_at_ms": 995,
                "age_ms": 5,
                "filled_base_quantity": 1.0,
                "implementation_shortfall_quote": 0.0,
                "fills": [
                    {
                        "order_id": "long-entry",
                        "client_order_id": "long-entry-client",
                        "submitted_at_ms": 1_000,
                        "filled_at_ms": 1_001,
                        "quantity": 1.0,
                        "price": 20.0,
                        "fee_quote": fee_quote,
                    }
                ],
            },
            "short": {
                "venue": "rich",
                "side": "sell",
                "vwap_price": 20.0,
                "available_base_quantity": 1.0,
                "observed_at_ms": 995,
                "age_ms": 5,
                "filled_base_quantity": 1.0,
                "implementation_shortfall_quote": 0.0,
                "fills": [
                    {
                        "order_id": "short-entry",
                        "client_order_id": "short-entry-client",
                        "submitted_at_ms": 1_000,
                        "filled_at_ms": 1_001,
                        "quantity": 1.0,
                        "price": 20.0,
                        "fee_quote": fee_quote,
                    }
                ],
            },
            "implementation_shortfall_quote": shortfall_quote,
        }
    )
    assert sealed is not None
    return sealed


def _complete_loop(entry_id: str, *, cost_bps: float = 3.0, truth: bool = True) -> list[dict]:
    # The maximum per-leg notional is 20 quote, so a fee quote of cost/500
    # produces exactly ``cost_bps`` after bps conversion.
    fee_quote = cost_bps / 500.0
    return [
        _event(entry_id, 1, "execution.entry_selected", _selected_payload(entry_id)),
        _event(
            entry_id,
            2,
            "entry.opened",
            {
                "position_id": entry_id,
                "matched_quantity": 1.0,
                "long_entry_price": 20.0,
                "short_entry_price": 20.0,
            },
        ),
        _event(
            entry_id,
            3,
            "exit.closed",
            {
                "position_id": entry_id,
                "symbol": "BTCUSDT",
                "long_venue": "cheap",
                "short_venue": "rich",
                "closed_at_ms": 3_000,
                "execution_completed_at_ms": 3_000,
                "entry_fee_quote": fee_quote,
                "exit_fee_quote": 0.0,
                # Explicit independent shortfall benchmark result; price PnL
                # below must not be repurposed as an execution-cost proxy.
                "implementation_shortfall_quote": 0.0,
                "execution_benchmark_complete": True,
                "execution_fee_complete": True,
                "entry_execution_benchmark_receipt": _signed_entry_receipt(
                    entry_id,
                    fee_quote=fee_quote / 2.0,
                ),
                "exit_execution_benchmark_receipts": [_signed_exit_receipt(entry_id)],
                "price_pnl_quote": 0.0,
            },
        ),
        _event(
            entry_id,
            4,
            "runtime.position_lifecycle_terminal",
            {
                "position_id": entry_id,
                "symbol": "BTCUSDT",
                "long_venue": "cheap",
                "short_venue": "rich",
                "exchange_truth_captured_at_ms": 4_000,
                "exchange_truth_scope": {
                    "position_id": entry_id,
                    "symbol": "BTCUSDT",
                    "venues": ["cheap", "rich"],
                },
                "exchange_truth": {
                    "truth_available": truth,
                    "positions_flat": truth,
                    "open_orders_flat": truth,
                },
                "terminal_reason": "post_close_exchange_truth_for_funding_reconciliation",
            },
        ),
        _event(
            entry_id,
            5,
            "funding.settlement_reconciled",
            {
                "position_id": entry_id,
                "symbol": "BTCUSDT",
                "reconciled_at_ms": 5_000,
                "required_settlement_count": 1,
                "observed_settlement_count": 1,
                "official_pnl": True,
                "statement_claims": [
                    {
                        "owner_id": entry_id,
                        "position_id": entry_id,
                        "leg": "long",
                        "venue": "cheap",
                        "symbol": "BTCUSDT",
                        "settlement_timestamp_ms": 1_000,
                        "quote_currency": "USDT",
                        "statement_reference": f"private-{entry_id}",
                        "recorded_at_ms": 1_100,
                    }
                ],
                "official_funding_quote": 0.0,
                "official_net_quote": 0.0,
            },
        ),
    ]


def test_canary_promotion_requires_30_reconciled_truth_flat_loops() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is True
    assert report.complete_loop_count == 30
    assert report.p95_actual_execution_cost_bps == 3.0
    assert report.p05_allowed_execution_cost_bps == 5.0


def test_canary_request_cannot_lower_required_closed_loops() -> None:
    report = analyze_funding_canary_events(
        _complete_loop("only-one"),
        required_closed_loops=1,
        source_evidence_verified=True,
    )

    assert report.required_closed_loops == 30
    assert report.promotion_ready is False
    assert "insufficient_complete_reconciled_truth_flat_loops" in report.promotion_blockers


def test_canary_promotion_requires_verified_acceptance_source() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]

    report = analyze_funding_canary_events(records)

    assert report.promotion_ready is False
    assert "acceptance_source_evidence_unverified" in report.promotion_blockers


def test_canary_cost_excludes_favourable_or_adverse_price_pnl() -> None:
    records = _complete_loop("markout", cost_bps=3.0)
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    terminal["payload"]["price_pnl_quote"] = -999.0

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.loops[0].actual_execution_cost_bps == 3.0


def test_canary_requires_independent_implementation_shortfall() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    next(event for event in records if event["kind"] == "exit.closed")["payload"].pop(
        "implementation_shortfall_quote"
    )

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_zero_shortfall_without_complete_execution_benchmark() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    next(event for event in records if event["kind"] == "exit.closed")["payload"][
        "execution_benchmark_complete"
    ] = False

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_tampered_or_missing_signed_execution_receipt() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    terminal["payload"]["exit_execution_benchmark_receipts"][0][
        "implementation_shortfall_quote"
    ] = 1.0

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_signed_entry_receipt_with_false_shortfall_aggregate() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    receipt = terminal["payload"]["entry_execution_benchmark_receipt"]
    receipt["long"]["fills"][0]["price"] = 21.0
    resealed = seal_execution_benchmark_receipt(receipt)
    assert resealed is not None
    terminal["payload"]["entry_execution_benchmark_receipt"] = resealed

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.complete_loop_count == 29
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_signed_receipt_with_stale_l2_observation() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    receipt = terminal["payload"]["exit_execution_benchmark_receipts"][0]
    receipt["long"]["fills"][0]["submitted_at_ms"] = 2_000
    receipt["long"]["fills"][0]["filled_at_ms"] = 2_001
    resealed = seal_execution_benchmark_receipt(receipt)
    assert resealed is not None
    terminal["payload"]["exit_execution_benchmark_receipts"] = [resealed]

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_signed_receipt_set_that_does_not_cover_opened_quantity() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    entry_id = terminal["payload"]["position_id"]
    # The receipt is authentic and internally self-consistent, but only covers
    # half the base quantity actually opened.  It must not turn the uncovered
    # close into a zero-cost canary observation.
    terminal["payload"]["exit_execution_benchmark_receipts"] = [
        _signed_exit_receipt(entry_id, quantity=0.5)
    ]

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.complete_loop_count == 29
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_entry_receipt_that_does_not_match_opened_quantity() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    opened = next(event for event in records if event["kind"] == "entry.opened")
    opened["payload"]["matched_quantity"] = 0.5

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.complete_loop_count == 29
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_fee_total_not_backed_by_signed_fill_observations() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    terminal["payload"]["entry_fee_quote"] = 0.25

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.complete_loop_count == 29
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_signed_benchmark_without_per_fill_fee_observation() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    receipt = terminal["payload"]["exit_execution_benchmark_receipts"][0]
    del receipt["long"]["fills"][0]["fee_quote"]
    resealed = seal_execution_benchmark_receipt(receipt)
    assert resealed is not None
    terminal["payload"]["exit_execution_benchmark_receipts"] = [resealed]

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.complete_loop_count == 29
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_unknown_execution_fee_as_zero_cost() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    terminal["payload"]["execution_fee_complete"] = False

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.missing_actual_cost_count == 1


def test_canary_rejects_truth_captured_before_exit_or_after_reconciliation() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    exit_closed = next(
        event
        for event in records
        if event["kind"] == "exit.closed"
        and event["payload"]["position_id"] == "entry-0"
    )
    early_truth = next(
        event
        for event in records
        if event["kind"] == "runtime.position_lifecycle_terminal"
        and event["payload"]["position_id"] == "entry-0"
    )
    settlement = next(
        event
        for event in records
        if event["kind"] == "funding.settlement_reconciled"
        and event["payload"]["position_id"] == "entry-0"
    )
    exit_closed["seq"] = 5
    early_truth["seq"] = 4
    settlement["seq"] = 6

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.missing_terminal_truth_count == 1


def test_canary_rejects_duplicate_or_unscoped_terminal_truth_receipt() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    duplicate = next(
        event
        for event in records
        if event["kind"] == "runtime.position_lifecycle_terminal"
        and event["payload"]["position_id"] == "entry-0"
    )
    duplicate = {
        **duplicate,
        "seq": 6,
        "payload": dict(duplicate["payload"]),
    }
    records.append(duplicate)

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.missing_terminal_truth_count == 1


def test_canary_requires_account_identity_binding_in_selected_evidence() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    next(
        event for event in records if event["kind"] == "execution.entry_selected"
    )["payload"]["account_fee_evidence_identity_bound"] = False

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.invalid_canary_contract_count == 1
    assert "acceptance_canary_contract_invalid" in report.promotion_blockers


def test_local_account_fee_authority_replaces_same_host_hmac_identity_ceremony() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    for event in records:
        if event["kind"] != "execution.entry_selected":
            continue
        event["payload"].update(
            {
                "account_fee_evidence_authoritative": True,
                "account_fee_evidence_integrity_verified": False,
                "account_fee_evidence_identity_bound": False,
            }
        )

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.invalid_canary_contract_count == 0
    assert report.promotion_ready is True


def test_v2_conservative_fee_tier_can_collect_samples_but_cannot_promote() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    for event in records:
        if event["kind"] != "execution.entry_selected":
            continue
        event["payload"].update(
            {
                "funding_canary_policy_version": "funding-canary-v2",
                "funding_canary_cohort_id": "conservative-cohort-v2",
                "funding_canary_fee_assurance_tier": "conservative",
                "account_fee_evidence_complete": False,
                "account_fee_evidence_integrity_verified": False,
                "account_fee_evidence_identity_bound": False,
                "funding_canary_hard_max_entry_notional_quote": 15.0,
                "funding_canary_hard_min_expected_net_edge_bps": 3.0,
                "funding_canary_hard_min_worst_case_edge_bps": 0.0,
            }
        )

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.invalid_canary_contract_count == 30
    assert "acceptance_canary_contract_invalid" in report.promotion_blockers


def test_v2_account_tier_requires_matching_signed_operator_policy() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    for event in records:
        if event["kind"] != "execution.entry_selected":
            continue
        event["payload"].update(
            {
                "funding_canary_policy_version": "funding-canary-v2",
                "funding_canary_cohort_id": "approved-cohort-v2",
                "funding_canary_fee_assurance_tier": "account",
                "funding_canary_hard_max_entry_notional_quote": 50.0,
                "funding_canary_hard_min_expected_net_edge_bps": 3.0,
                "funding_canary_hard_min_worst_case_edge_bps": 0.0,
                "long_venue": "cheap",
                "short_venue": "rich",
            }
        )
    missing = analyze_funding_canary_events(
        records,
        source_evidence_verified=True,
    )
    approved_policy = sign_funding_canary_approved_policy(
        {
            "schema_version": 1,
            "policy_version": "funding-canary-v2",
            "cohort_id": "approved-cohort-v2",
            "max_entry_notional_quote": 50.0,
            "min_expected_net_edge_bps": 3.0,
            "min_worst_case_edge_bps": 0.0,
            "allowed_venue_pairs": ["rich|cheap"],
        }
    )
    approved = analyze_funding_canary_events(
        records,
        source_evidence_verified=True,
        approved_policy=approved_policy,
    )

    assert missing.promotion_ready is False
    assert missing.invalid_canary_contract_count == 30
    assert approved.promotion_ready is True
    assert approved.complete_loop_count == 30


def test_v2_operator_policy_rejects_signature_or_limit_tampering() -> None:
    records = _complete_loop("entry-v2")
    selected = records[0]["payload"]
    selected.update(
        {
            "funding_canary_policy_version": "funding-canary-v2",
            "funding_canary_cohort_id": "approved-cohort-v2",
            "funding_canary_fee_assurance_tier": "account",
            "funding_canary_hard_max_entry_notional_quote": 50.0,
            "funding_canary_hard_min_expected_net_edge_bps": 3.0,
            "funding_canary_hard_min_worst_case_edge_bps": 0.0,
            "long_venue": "cheap",
            "short_venue": "rich",
        }
    )
    approved_policy = sign_funding_canary_approved_policy(
        {
            "schema_version": 1,
            "policy_version": "funding-canary-v2",
            "cohort_id": "approved-cohort-v2",
            "max_entry_notional_quote": 50.0,
            "min_expected_net_edge_bps": 3.0,
            "min_worst_case_edge_bps": 0.0,
            "allowed_venue_pairs": ["cheap:rich"],
        }
    )
    approved_policy["max_entry_notional_quote"] = 500.0

    report = analyze_funding_canary_events(
        records,
        source_evidence_verified=True,
        approved_policy=approved_policy,
    )

    assert report.invalid_canary_contract_count == 1
    assert report.loops[0].contract_reason == "approved_v2_policy_unverified"


def test_canary_rejects_final_edge_and_per_leg_cap_breaches() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    selected = next(event for event in records if event["kind"] == "execution.entry_selected")
    selected["payload"].update(
        {
            "expected_net_edge_bps": 2.0,
            "worst_case_edge_bps": 1.0,
        }
    )
    opened = next(event for event in records if event["kind"] == "entry.opened")
    opened["payload"].update({"long_entry_price": 31.0, "short_entry_price": 10.0})

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.invalid_canary_contract_count == 1
    assert report.over_notional_count == 1
    assert "acceptance_canary_contract_invalid" in report.promotion_blockers
    assert "actual_opened_notional_exceeds_canary_cap" in report.promotion_blockers


def test_canary_rejects_cost_above_immutable_budget_and_reserve() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    terminal = next(event for event in records if event["kind"] == "exit.closed")
    terminal["payload"]["entry_fee_quote"] = 6.0 / 500.0
    receipt = terminal["payload"]["entry_execution_benchmark_receipt"]
    for leg_name in ("long", "short"):
        receipt[leg_name]["fills"][0]["fee_quote"] = 3.0 / 500.0
    resealed = seal_execution_benchmark_receipt(receipt)
    assert resealed is not None
    terminal["payload"]["entry_execution_benchmark_receipt"] = resealed

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.execution_cost_budget_breach_count == 1
    assert "actual_execution_cost_exceeds_budget_and_reserve" in report.promotion_blockers


def test_canary_rejects_duplicate_or_unsequenced_lifecycle_evidence() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    duplicate = dict(next(event for event in records if event["kind"] == "entry.opened"))
    duplicate["payload"] = dict(duplicate["payload"])
    records.append(duplicate)
    records.append(
        {
            "kind": "exit.closed",
            "payload": {"position_id": "unrelated"},
        }
    )

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.ambiguous_event_count == 1
    assert report.missing_event_sequence_count == 1
    assert "acceptance_event_ambiguity" in report.promotion_blockers
    assert "acceptance_event_sequence_missing" in report.promotion_blockers


@pytest.mark.parametrize(
    ("event_kind", "sequence"),
    [
        ("execution.entry_selected", 6),
        ("entry.opened", 6),
    ],
)
def test_canary_rejects_lifecycle_event_after_close(
    event_kind: str,
    sequence: int,
) -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    event = next(item for item in records if item["kind"] == event_kind)
    event["seq"] = sequence

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.complete_loop_count == 29
    assert report.invalid_lifecycle_evidence_count == 1
    assert "canary_lifecycle_evidence_not_ordered_and_scoped" in report.promotion_blockers


def test_canary_requires_dedicated_post_close_truth_reason() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    truth = next(
        item
        for item in records
        if item["kind"] == "runtime.position_lifecycle_terminal"
    )
    truth["payload"]["terminal_reason"] = "recovery_flat"

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.complete_loop_count == 29
    assert report.invalid_lifecycle_evidence_count == 1
    assert "canary_lifecycle_evidence_not_ordered_and_scoped" in report.promotion_blockers


def test_canary_requires_truth_capture_between_close_and_reconciliation() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    truth = next(
        item
        for item in records
        if item["kind"] == "runtime.position_lifecycle_terminal"
    )
    truth["payload"]["exchange_truth_captured_at_ms"] = 1

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.complete_loop_count == 29
    assert report.invalid_lifecycle_evidence_count == 1


def test_canary_rejects_malformed_relevant_event_instead_of_skipping_it() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    records.append(
        {
            "run_id": "run-entry-0",
            "seq": 6,
            "ts_ms": 6_000,
            "kind": "runtime.position_lifecycle_terminal",
            "payload": "corrupted",
        }
    )

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.complete_loop_count == 30
    assert report.malformed_relevant_event_count == 1
    assert "acceptance_relevant_event_malformed" in report.promotion_blockers


def test_canary_binds_optional_finalized_marker_between_open_and_close() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    first_loop = [item for item in records if item["run_id"] == "run-entry-0"]
    for event in first_loop:
        if event["kind"] == "exit.closed":
            event["seq"] = 4
        elif event["kind"] == "runtime.position_lifecycle_terminal":
            event["seq"] = 5
        elif event["kind"] == "funding.settlement_reconciled":
            event["seq"] = 6
    records.append(
        _event(
            "entry-0",
            3,
            "pending_entry.pending_entry_finalized",
            {"entry_id": "entry-0", "position_id": "entry-0"},
        )
    )

    valid = analyze_funding_canary_events(records, source_evidence_verified=True)
    assert valid.promotion_ready is True

    finalized = next(
        item
        for item in records
        if item["kind"] == "pending_entry.pending_entry_finalized"
    )
    finalized["seq"] = 7
    invalid = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert invalid.promotion_ready is False
    assert invalid.complete_loop_count == 29
    assert invalid.invalid_lifecycle_evidence_count == 1


def test_canary_rejects_mixed_cohort_and_order_independent_replay() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    selected = next(
        event for event in records if event["kind"] == "execution.entry_selected" and event["payload"]["entry_id"] == "entry-1"
    )
    selected["payload"]["funding_canary_cohort_id"] = "other-policy"

    forward = analyze_funding_canary_events(records, source_evidence_verified=True)
    backward = analyze_funding_canary_events(list(reversed(records)), source_evidence_verified=True)

    assert forward.promotion_ready is False
    assert forward.cohort_ids == backward.cohort_ids
    assert forward.mixed_cohort_count == 1
    assert "mixed_or_missing_funding_canary_cohort" in forward.promotion_blockers


def test_canary_refuses_cross_run_lifecycle_splicing() -> None:
    records: list[dict] = []
    for index in range(30):
        entry_id = f"entry-{index}"
        loop = _complete_loop(entry_id)
        loop[0]["run_id"] = "selected-run"
        loop[0]["seq"] = index + 1
        for offset, event in enumerate(loop[1:], start=1):
            event["run_id"] = "terminal-run"
            event["seq"] = index * 4 + offset
        records.extend(loop)

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.complete_loop_count == 0
    assert report.cross_run_lifecycle_evidence_count == 30
    assert "cross_run_lifecycle_evidence_not_promotable" in report.promotion_blockers


def test_canary_selected_without_opened_position_blocks_promotion() -> None:
    report = analyze_funding_canary_events(
        [_event("entry-unopened", 1, "execution.entry_selected", _selected_payload("entry-unopened"))]
    )

    assert report.promotion_ready is False
    assert report.selected_not_opened_count == 1
    assert "selected_canaries_without_opened_position" in report.promotion_blockers
