from __future__ import annotations

from lightfee.offline.funding_canary_analysis import analyze_funding_canary_events


def _event(entry_id: str, seq: int, kind: str, payload: dict) -> dict:
    return {
        "run_id": f"run-{entry_id}",
        "seq": seq,
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
                "entry_fee_quote": fee_quote,
                "exit_fee_quote": 0.0,
                # Explicit independent shortfall benchmark result; price PnL
                # below must not be repurposed as an execution-cost proxy.
                "implementation_shortfall_quote": 0.0,
                "execution_benchmark_complete": True,
                "price_pnl_quote": 0.0,
            },
        ),
        _event(
            entry_id,
            4,
            "runtime.position_lifecycle_terminal",
            {
                "position_id": entry_id,
                "exchange_truth": {
                    "truth_available": truth,
                    "positions_flat": truth,
                    "open_orders_flat": truth,
                },
            },
        ),
        _event(
            entry_id,
            5,
            "funding.settlement_reconciled",
            {
                "position_id": entry_id,
                "symbol": "BTCUSDT",
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


def test_canary_requires_account_identity_binding_in_selected_evidence() -> None:
    records = [event for index in range(30) for event in _complete_loop(f"entry-{index}")]
    next(
        event for event in records if event["kind"] == "execution.entry_selected"
    )["payload"]["account_fee_evidence_identity_bound"] = False

    report = analyze_funding_canary_events(records, source_evidence_verified=True)

    assert report.promotion_ready is False
    assert report.invalid_canary_contract_count == 1
    assert "acceptance_canary_contract_invalid" in report.promotion_blockers


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
