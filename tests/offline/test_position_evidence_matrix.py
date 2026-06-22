from __future__ import annotations

from lightfee.offline.position_evidence import (
    build_position_evidence_matrix,
    derive_ledger_rows_from_events,
)


def test_entry_aborted_without_open_is_not_normal_lifecycle():
    matrix = build_position_evidence_matrix(
        events=[
            {
                "ts_ms": 1782100000000,
                "kind": "entry.aborted",
                "payload": {
                    "entry_id": "entry-1782100325096-LABUSDT",
                    "symbol": "LABUSDT",
                    "reason": "Post Only order will be rejected: -5022",
                },
            },
        ],
        ledger_rows=[],
        quick_flat_threshold_ms=120_000,
    )

    row = matrix["positions"]["entry-1782100325096-LABUSDT"]

    assert row["classification"] == "admission_aborted_no_open"
    assert row["has_entry_opened"] is False
    assert matrix["summary"]["normal_count"] == 0
    assert matrix["summary"]["admission_aborted_no_open_count"] == 1


def test_entry_aborted_with_ledger_is_business_gap():
    matrix = build_position_evidence_matrix(
        events=[
            {
                "ts_ms": 1782100000000,
                "kind": "entry.aborted",
                "payload": {
                    "entry_id": "entry-1782100325096-GAPUSDT",
                    "symbol": "GAPUSDT",
                    "maker_venue": "bybit",
                    "reason": "pending_entry_max_lifetime_exhausted",
                },
            },
        ],
        ledger_rows=[
            {
                "ts_ms": 1782100005000,
                "venue": "bybit",
                "symbol": "GAPUSDT",
                "amount": "-0.10",
                "fee": "-0.01",
            },
        ],
        quick_flat_threshold_ms=120_000,
    )

    row = matrix["positions"]["entry-1782100325096-GAPUSDT"]

    assert row["classification"] == "aborted_with_ledger"
    assert row["ledger"]["row_count"] == 1
    assert row["ledger"]["net_amount"] == "-0.11"


def test_live_recovered_id_parses_venues_and_attributes_ledger_rows():
    matrix = build_position_evidence_matrix(
        events=[
            {
                "ts_ms": 1782100000000,
                "kind": "exit.closed",
                "payload": {
                    "position_id": "live-recovered:SAHARAUSDT:binance->bybit",
                    "symbol": "SAHARAUSDT",
                    "reason": "settlement_force_close",
                },
            },
            {
                "ts_ms": 1782100010000,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "live-recovered:SAHARAUSDT:binance->bybit",
                    "symbol": "SAHARAUSDT",
                    "reason": "fallback_live_balanced_matched_close_flat_probe",
                },
            },
        ],
        ledger_rows=[
            {
                "ts_ms": 1782100005000,
                "venue": "binance",
                "symbol": "SAHARAUSDT",
                "amount": "-0.40",
                "fee": "-0.01",
                "income_type": "REALIZED_PNL",
            },
            {
                "ts_ms": 1782100007000,
                "venue": "bybit",
                "symbol": "SAHARAUSDT",
                "amount": "-0.30",
                "fee": "-0.02",
                "income_type": "COMMISSION",
            },
        ],
        quick_flat_threshold_ms=120_000,
    )

    row = matrix["positions"]["live-recovered:SAHARAUSDT:binance->bybit"]

    assert row["venues"] == ["binance", "bybit"]
    assert row["classification"] == "abnormal_recovered"
    assert row["ledger"]["row_count"] == 2
    assert row["ledger"]["net_amount"] == "-0.73"
    assert matrix["summary"]["unattributed_ledger_row_count"] == 0


def test_clean_seconds_and_abnormal_quick_terminal_are_separate_classes():
    matrix = build_position_evidence_matrix(
        events=[
            {
                "ts_ms": 1782028800000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "entry-1782028374700-OPNUSDT",
                    "symbol": "OPNUSDT",
                    "long_venue": "bybit",
                    "short_venue": "okx",
                },
            },
            {
                "ts_ms": 1782028825000,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "entry-1782028374700-OPNUSDT",
                    "symbol": "OPNUSDT",
                    "reason": "passive_close_final_exchange_flat",
                },
            },
            {
                "ts_ms": 1782028900000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "entry-quick-abnormal-HUSDT",
                    "symbol": "HUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                },
            },
            {
                "ts_ms": 1782028950000,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "entry-quick-abnormal-HUSDT",
                    "symbol": "HUSDT",
                    "terminal_reason": "fallback_live_balanced_matched_close_flat_probe",
                    "client_order_ids": ["cid-close-1"],
                    "order_ids": ["oid-close-1"],
                    "exchange_truth": {
                        "exchange_truth_flat": True,
                        "exchange_truth_no_open_orders": True,
                    },
                },
            },
        ],
        ledger_rows=[],
        quick_flat_threshold_ms=120_000,
    )

    assert (
        matrix["positions"]["entry-1782028374700-OPNUSDT"]["classification"]
        == "seconds_open_close"
    )
    assert (
        matrix["positions"]["entry-quick-abnormal-HUSDT"]["classification"]
        == "abnormal_quick_terminal"
    )
    abnormal_row = matrix["positions"]["entry-quick-abnormal-HUSDT"]
    assert abnormal_row["terminal_reason"] == "fallback_live_balanced_matched_close_flat_probe"
    assert abnormal_row["client_order_ids"] == ["cid-close-1"]
    assert abnormal_row["order_ids"] == ["oid-close-1"]
    assert abnormal_row["exchange_truth_flat"] is True
    assert abnormal_row["exchange_truth_no_open_orders"] is True
    assert matrix["summary"]["seconds_open_close_count"] == 1
    assert matrix["summary"]["abnormal_quick_terminal_count"] == 1


def test_unattributed_ledger_rows_are_all_preserved_with_diagnostics():
    ledger_rows = [
        {
            "ts_ms": 1782100000000 + idx,
            "venue": "binance",
            "symbol": "HUSDT",
            "amount": "-0.01",
            "income_type": "COMMISSION",
        }
        for idx in range(45)
    ]

    matrix = build_position_evidence_matrix(
        events=[
            {
                "ts_ms": 1782100000000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "entry-1782100000000-HUSDT",
                    "symbol": "HUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                },
            }
        ],
        ledger_rows=ledger_rows,
    )

    assert matrix["summary"]["unattributed_ledger_row_count"] == 45
    assert len(matrix["unattributed_ledger_rows"]) == 45
    for row in matrix["unattributed_ledger_rows"]:
        assert row["owner_id"] == ""
        assert row["match_confidence"] == "none"
        assert row["unattributed_reason"] == "audit_missing_durable_anchor"
        assert row["root_cause"] == "audit_matcher_gap"
        assert row["evidence_refs"]


def test_order_client_and_trade_id_matching_reports_confidence():
    matrix = build_position_evidence_matrix(
        events=[
            {
                "ts_ms": 1782100000000,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "entry-1782100000000-IDUSDT",
                    "symbol": "IDUSDT",
                    "long_venue": "bybit",
                    "short_venue": "binance",
                    "order_ids": ["order-a"],
                    "client_order_ids": ["client-a"],
                    "terminal_reason": "passive_close_final_exchange_flat",
                },
            }
        ],
        ledger_rows=[
            {
                "ts_ms": 1782100000001,
                "venue": "binance",
                "symbol": "IDUSDT",
                "order_id": "order-a",
                "trade_id": "trade-a",
                "amount": "0.10",
            }
        ],
    )

    ledger = matrix["positions"]["entry-1782100000000-IDUSDT"]["ledger"]

    assert ledger["row_count"] == 1
    assert ledger["rows"][0]["owner_id"] == "entry-1782100000000-IDUSDT"
    assert ledger["rows"][0]["match_confidence"] == "order_id"
    assert ledger["rows"][0]["evidence_refs"] == ["order_id:order-a"]


def test_abnormal_positions_list_keeps_all_fallback_compensated_samples():
    events = []
    for idx in range(28):
        position_id = f"entry-1782100000{idx:03d}-HOMEUSDT"
        events.extend(
            [
                {
                    "ts_ms": 1782100000000 + idx,
                    "kind": "entry.opened",
                    "payload": {
                        "position_id": position_id,
                        "symbol": "HOMEUSDT",
                        "long_venue": "binance",
                        "short_venue": "bybit",
                    },
                },
                {
                    "ts_ms": 1782100060000 + idx,
                    "kind": "runtime.position_lifecycle_terminal",
                    "payload": {
                        "position_id": position_id,
                        "symbol": "HOMEUSDT",
                        "terminal_reason": "fallback_live_balanced_matched_close_flat_probe",
                    },
                },
            ]
        )

    matrix = build_position_evidence_matrix(events=events, ledger_rows=[])

    assert matrix["summary"]["abnormal_position_count"] == 28
    assert len(matrix["abnormal_positions"]) == 28
    assert {row["classification"] for row in matrix["abnormal_positions"]} == {
        "abnormal_quick_terminal"
    }
    assert all(
        row["abnormal_evidence"] == ["fallback_live_balanced_matched_close_flat_probe"]
        for row in matrix["abnormal_positions"]
    )
    assert matrix["summary"]["normal_count"] == 0


def test_normal_lifecycle_ledger_gaps_explain_missing_reconcile_and_ledger():
    matrix = build_position_evidence_matrix(
        events=[
            {
                "ts_ms": 1782100000000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "entry-1782100000000-FIDAUSDT",
                    "symbol": "FIDAUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                },
            },
            {
                "ts_ms": 1782100600000,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "entry-1782100000000-FIDAUSDT",
                    "symbol": "FIDAUSDT",
                    "terminal_reason": "passive_close_final_exchange_flat",
                },
            },
            {
                "ts_ms": 1782100600001,
                "kind": "recovery.flat",
                "payload": {
                    "position_id": "entry-1782100000000-FIDAUSDT",
                    "symbol": "FIDAUSDT",
                    "source": "passive_close_final_exchange_flat",
                },
            },
        ],
        ledger_rows=[],
    )

    gap = matrix["normal_lifecycle_ledger_gaps"][0]

    assert gap["position_id"] == "entry-1782100000000-FIDAUSDT"
    assert gap["missing"] == ["exit.reconciled", "ledger_rows", "order.filled"]
    assert gap["root_cause"] == "business_event_missing_anchor"
    assert matrix["positions"][gap["position_id"]]["lifecycle_completeness"] == {
        "entry_opened": True,
        "terminal": True,
        "recovery_flat": True,
        "exit_reconciled": False,
        "ledger_rows": False,
        "funding_capture": False,
        "order_filled": False,
    }


def test_derive_ledger_rows_from_exit_reconciled_preserves_statement_gaps():
    rows = derive_ledger_rows_from_events(
        [
            {
                "ts_ms": 1782100600000,
                "kind": "exit.reconciled",
                "payload": {
                    "position_id": "entry-1782100000000-LABUSDT",
                    "symbol": "LABUSDT",
                    "source": "passive_close_live_one_sided_flattened",
                    "net_quote": "-0.07821048",
                    "entry_fee_quote": "0",
                    "exit_fee_quote": "0",
                    "funding_pnl_quote": "0.01778952",
                    "price_pnl": "-0.096",
                    "evidence_gap": True,
                    "evidence_gap_reason": "missing_short_close_trade_statement",
                    "statement_probe_status": "partial",
                    "trade_probe_status": {"long": "found", "short": "missing"},
                    "long_legs": [
                        {
                            "venue": "binance",
                            "order_id": "4401575767",
                            "client_order_id": "lfex7ff61741a1674828",
                            "fee_quote": 0.0,
                            "quantity": 1.0,
                        }
                    ],
                    "short_legs": [],
                    "short_closed_qty": 0.0,
                    "short_order_id": "",
                    "short_client_order_id": "",
                },
            }
        ]
    )

    assert [row["income_type"] for row in rows] == [
        "REALIZED_PNL",
        "FUNDING_FEE",
        "COMMISSION",
        "MISSING_TRADE_STATEMENT",
    ]
    assert rows[-1]["unattributed_reason"] == "exchange_statement_leg_missing"
    assert rows[-1]["root_cause"] == "exchange_ledger_field_gap"
