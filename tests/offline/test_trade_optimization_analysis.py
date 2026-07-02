from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from lightfee.offline.trade_optimization import build_trade_optimization_analysis


def _event(ts_ms: int, kind: str, payload: dict) -> dict:
    return {"ts_ms": ts_ms, "kind": kind, "payload": payload}


def test_current_reconciled_complete_sample_gets_real_accounting_label():
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": "entry-1-LABUSDT",
                "symbol": "LABUSDT",
                "quantity": 10,
                "long_venue": "bitget",
                "short_venue": "okx",
                "long_entry_price": 2.0,
                "short_entry_price": 2.02,
                "selected_edge_bps": 80.0,
                "selected_total_funding_edge_bps": 50.0,
                "funding_ts": 61_000,
                "entry_fee_quote": "0.10",
            },
        ),
        _event(
            1_200,
            "runtime.snapshot_freshness_decision",
            {
                "symbol": "LABUSDT",
                "venue": "bitget",
                "best_bid": 1.99,
                "best_ask": 2.01,
                "bid_size": 100,
                "ask_size": 90,
                "open_interest": 1_000_000,
                "volume_24h": 4_000_000,
                "freshness_status": "current_ok",
            },
        ),
        _event(
            1_500,
            "order.filled",
            {
                "position_id": "entry-1-LABUSDT",
                "symbol": "LABUSDT",
                "phase": "open",
                "venue": "bitget",
                "leg": "long",
                "order_id": "long-open-1",
                "quantity": 10,
                "average_price": 2.0,
                "fee_quote": "0.04",
            },
        ),
        _event(
            1_600,
            "order.filled",
            {
                "position_id": "entry-1-LABUSDT",
                "symbol": "LABUSDT",
                "phase": "open",
                "venue": "okx",
                "leg": "short",
                "order_id": "short-open-1",
                "quantity": 10,
                "average_price": 2.02,
                "fee_quote": "0.06",
            },
        ),
        _event(
            20_000,
            "exit.reconciled",
            {
                "position_id": "entry-1-LABUSDT",
                "symbol": "LABUSDT",
                "accounting_status": "complete",
                "evidence_gap": False,
                "pending_backfill": False,
                "price_pnl": "-1.20",
                "funding_pnl_quote": "0.80",
                "net_quote": "-0.62",
                "reason": "funding_capture",
                "long_legs": [
                    {
                        "venue": "bitget",
                        "order_id": "long-close-1",
                        "quantity": 10,
                        "average_price": 1.96,
                        "fee_quote": "0.05",
                    }
                ],
                "short_legs": [
                    {
                        "venue": "okx",
                        "order_id": "short-close-1",
                        "quantity": 10,
                        "average_price": 2.10,
                        "fee_quote": "0.07",
                    }
                ],
            },
        ),
    ]

    report = build_trade_optimization_analysis(events, normal_only=True)

    assert report["summary"]["entry_opened_positions"] == 1
    assert report["summary"]["normal_sample_count"] == 1
    sample = report["samples"][0]
    assert sample["position_id"] == "entry-1-LABUSDT"
    assert sample["normality_source"] == "exit.reconciled"
    assert sample["verification_status"] == "verified_exchange_lifecycle"
    assert sample["pnl"]["price_pnl_quote"] == "-1.2"
    assert sample["pnl"]["funding_pnl_quote"] == "0.8"
    assert sample["pnl"]["entry_fee_quote"] == "-0.1"
    assert sample["pnl"]["exit_fee_quote"] == "-0.12"
    assert sample["pnl"]["net_pnl_quote"] == "-0.62"
    assert sample["market"]["entry_snapshot"]["spread_bps"] == "100"
    assert sample["features"]["time_to_funding_ms"] == 60_000
    assert report["aggregates"]["by_symbol"]["LABUSDT"]["net_pnl_quote"] == "-0.62"


def test_legacy_exit_closed_requires_full_qty_and_close_fill_evidence():
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": "entry-legacy-TAIKO",
                "symbol": "TAIKOUSDT",
                "quantity": 4,
                "long_venue": "bybit",
                "short_venue": "binance",
                "long_entry_price": 1.00,
                "short_entry_price": 1.01,
                "entry_fee_quote": "0.01",
            },
        ),
        _event(
            4_900,
            "funding.settled",
            {
                "position_id": "entry-legacy-TAIKO",
                "symbol": "TAIKOUSDT",
                "funding_pnl_quote": "0.20",
            },
        ),
        _event(
            4_950,
            "order.filled",
            {
                "position_id": "entry-legacy-TAIKO",
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "venue": "bybit",
                "leg": "long",
                "client_order_id": "open-long",
                "quantity": 4,
                "price": 1.00,
                "fee_quote": "0.004",
            },
        ),
        _event(
            4_960,
            "order.filled",
            {
                "position_id": "entry-legacy-TAIKO",
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "venue": "binance",
                "leg": "short",
                "client_order_id": "open-short",
                "quantity": 4,
                "price": 1.01,
                "fee_quote": "0.006",
            },
        ),
        _event(
            5_000,
            "order.filled",
            {
                "position_id": "entry-legacy-TAIKO",
                "symbol": "TAIKOUSDT",
                "venue": "bybit",
                "leg": "long",
                "source": "close",
                "client_order_id": "close-long",
                "quantity": 4,
                "price": 1.02,
                "fee_quote": "0.02",
            },
        ),
        _event(
            5_100,
            "order.filled",
            {
                "position_id": "entry-legacy-TAIKO",
                "symbol": "TAIKOUSDT",
                "venue": "binance",
                "leg": "short",
                "source": "close",
                "client_order_id": "close-short",
                "quantity": 4,
                "price": 1.00,
                "fee_quote": "0.03",
            },
        ),
        _event(
            5_200,
            "exit.closed",
            {
                "position_id": "entry-legacy-TAIKO",
                "close_id": "close-entry-legacy",
                "reason": "funding_capture",
                "long_client_order_id": "close-long",
                "short_client_order_id": "close-short",
                "long_closed_qty": 4,
                "short_closed_qty": 4,
                "long_uncertain": False,
                "short_uncertain": False,
                "price_pnl": "0.40",
                "funding_pnl_quote": "0.20",
                "entry_fee_quote": "0.01",
                "exit_fee_quote": "0.05",
                "net_quote": "0.54",
            },
        ),
    ]

    report = build_trade_optimization_analysis(events, normal_only=True)

    assert report["summary"]["normal_sample_count"] == 1
    sample = report["samples"][0]
    assert sample["normality_source"] == "exchange.truth.legacy_project_gap"
    assert sample["verification_status"] == "verified_exchange_lifecycle"
    assert sample["pnl"]["net_pnl_quote"] == "0.26"
    assert sample["execution"]["close_fill_evidence_count"] == 2


def test_pending_backfill_and_legacy_missing_fill_are_excluded_with_reasons():
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": "entry-gap-LAB",
                "symbol": "LABUSDT",
                "quantity": 3,
                "long_venue": "bitget",
                "short_venue": "okx",
            },
        ),
        _event(
            2_000,
            "exit.reconciled",
            {
                "position_id": "entry-gap-LAB",
                "symbol": "LABUSDT",
                "accounting_status": "pending_backfill",
                "evidence_gap": True,
                "pending_backfill": True,
            },
        ),
        _event(
            3_000,
            "entry.opened",
            {
                "position_id": "entry-legacy-gap",
                "symbol": "TAIKOUSDT",
                "quantity": 2,
                "long_venue": "binance",
                "short_venue": "bitget",
            },
        ),
        _event(
            4_000,
            "exit.closed",
            {
                "position_id": "entry-legacy-gap",
                "long_client_order_id": "missing-long",
                "short_client_order_id": "missing-short",
                "long_closed_qty": 2,
                "short_closed_qty": 2,
                "long_uncertain": False,
                "short_uncertain": False,
            },
        ),
    ]

    report = build_trade_optimization_analysis(events, normal_only=True)

    assert report["summary"]["normal_sample_count"] == 0
    assert report["summary"]["excluded_position_count"] == 2
    reasons = {
        row["position_id"]: row["reason"]
        for row in report["excluded_positions"]
    }
    assert reasons["entry-gap-LAB"] == "exit_reconciled_pending_backfill_or_evidence_gap"
    assert reasons["entry-legacy-gap"] == "legacy_exit_closed_missing_exchange_fill_evidence"


def test_counterfactual_selected_entries_are_kept_out_of_main_samples():
    events = [
        _event(
            1_000,
            "execution.entry_selected",
            {
                "symbol": "GUAUSDT",
                "long_venue": "aster",
                "short_venue": "binance",
                "selected_edge_bps": 120,
            },
        ),
    ]

    report = build_trade_optimization_analysis(
        events,
        normal_only=True,
        include_counterfactual=True,
    )

    assert report["summary"]["normal_sample_count"] == 0
    assert report["counterfactual"]["selected_count"] == 1
    assert report["counterfactual"]["by_symbol"]["GUAUSDT"]["count"] == 1


def test_sidecar_liquidity_snapshot_fields_are_preserved():
    events = [
        _event(
            10_000,
            "entry.opened",
            {
                "position_id": "entry-liquidity-LAB",
                "symbol": "LABUSDT",
                "quantity": 2,
                "long_venue": "bitget",
                "short_venue": "okx",
                "long_entry_price": 12,
                "short_entry_price": 12,
            },
        ),
        _event(
            10_100,
            "order.filled",
            {
                "position_id": "entry-liquidity-LAB",
                "symbol": "LABUSDT",
                "phase": "open",
                "venue": "bitget",
                "leg": "long",
                "order_id": "long-liquidity-open",
                "quantity": 2,
                "average_price": 12,
            },
        ),
        _event(
            10_200,
            "order.filled",
            {
                "position_id": "entry-liquidity-LAB",
                "symbol": "LABUSDT",
                "phase": "open",
                "venue": "okx",
                "leg": "short",
                "order_id": "short-liquidity-open",
                "quantity": 2,
                "average_price": 12,
            },
        ),
        _event(
            9_900,
            "runtime.snapshot_freshness_decision",
            {
                "symbol": "LABUSDT",
                "venue": "bitget",
                "quote_bid": 11.99,
                "quote_ask": 12.01,
                "quote_bid_size": 500,
                "quote_ask_size": 450,
                "observed_open_interest_quote": 1_250_000,
                "observed_volume_24h_quote": 3_400_000,
                "open_interest_evidence_status": "available",
                "decision": "allow_entry",
            },
        ),
        _event(
            20_000,
            "exit.reconciled",
            {
                "position_id": "entry-liquidity-LAB",
                "symbol": "LABUSDT",
                "accounting_status": "complete",
                "evidence_gap": False,
                "pending_backfill": False,
                "net_quote": "0.10",
                "long_legs": [
                    {
                        "venue": "bitget",
                        "order_id": "long-liquidity-close",
                        "quantity": 2,
                        "average_price": 12.01,
                    }
                ],
                "short_legs": [
                    {
                        "venue": "okx",
                        "order_id": "short-liquidity-close",
                        "quantity": 2,
                        "average_price": 11.99,
                    }
                ],
            },
        ),
    ]

    report = build_trade_optimization_analysis(events, normal_only=True)

    snapshot = report["samples"][0]["market"]["entry_snapshot"]
    assert snapshot["bid_price"] == "11.99"
    assert snapshot["ask_price"] == "12.01"
    assert snapshot["bid_size"] == "500"
    assert snapshot["ask_size"] == "450"
    assert snapshot["open_interest"] == "1250000"
    assert snapshot["volume_24h"] == "3400000"
    assert snapshot["open_interest_evidence_status"] == "available"


def test_cli_writes_json_csv_and_markdown_outputs(tmp_path: Path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    events_path = runtime_dir / "live-events.jsonl"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": "entry-cli-LAB",
                "symbol": "LABUSDT",
                "quantity": 1,
                "long_venue": "bitget",
                "short_venue": "okx",
                "long_entry_price": 10,
                "short_entry_price": 10,
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": "entry-cli-LAB",
                "symbol": "LABUSDT",
                "phase": "open",
                "venue": "bitget",
                "leg": "long",
                "order_id": "long-cli-open",
                "quantity": 1,
                "average_price": 10,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": "entry-cli-LAB",
                "symbol": "LABUSDT",
                "phase": "open",
                "venue": "okx",
                "leg": "short",
                "order_id": "short-cli-open",
                "quantity": 1,
                "average_price": 10,
            },
        ),
        _event(
            2_000,
            "exit.reconciled",
            {
                "position_id": "entry-cli-LAB",
                "symbol": "LABUSDT",
                "accounting_status": "complete",
                "evidence_gap": False,
                "pending_backfill": False,
                "net_quote": "1.25",
                "long_legs": [
                    {
                        "venue": "bitget",
                        "order_id": "long-cli-close",
                        "quantity": 1,
                        "average_price": 10.75,
                    }
                ],
                "short_legs": [
                    {
                        "venue": "okx",
                        "order_id": "short-cli-close",
                        "quantity": 1,
                        "average_price": 10.25,
                    }
                ],
            },
        ),
    ]
    events_path.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")

    json_path = tmp_path / "latest.json"
    csv_path = tmp_path / "samples.csv"
    report_path = tmp_path / "report.md"

    subprocess.run(
        [
            sys.executable,
            "scripts/analyze_trade_optimization_samples.py",
            "--runtime-dir",
            str(runtime_dir),
            "--history",
            "all",
            "--normal-only",
            "--include-counterfactual",
            "--json",
            str(json_path),
            "--csv",
            str(csv_path),
            "--report-md",
            str(report_path),
        ],
        check=True,
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["summary"]["normal_sample_count"] == 1
    assert "entry-cli-LAB" in csv_path.read_text(encoding="utf-8")
    assert "Trade Optimization Sample Report" in report_path.read_text(encoding="utf-8")
