from __future__ import annotations

from lightfee.offline.position_evidence import build_position_evidence_matrix


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
