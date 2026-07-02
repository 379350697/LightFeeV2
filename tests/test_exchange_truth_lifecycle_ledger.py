from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

from lightfee.core.domain import OrderFillReconciliation, Side, Venue
from lightfee.lifecycle.exchange_truth_ledger import (
    LifecycleClassification,
    build_exchange_truth_lifecycle,
)


def _event(ts_ms: int, kind: str, payload: dict) -> dict:
    return {"ts_ms": ts_ms, "kind": kind, "payload": payload}


def _truth(events: list[dict], position_id: str) -> dict:
    report = build_exchange_truth_lifecycle(events)
    return report["positions"][position_id]


def test_zero_quantity_entry_opened_is_phantom_not_normal_lifecycle():
    position_id = "entry-1779421945495-XCNUSDT"
    truth = _truth(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "XCNUSDT",
                    "matched_quantity": 0,
                    "long_venue": "binance",
                    "short_venue": "bitget",
                },
            )
        ],
        position_id,
    )

    assert truth["classification"] == LifecycleClassification.PHANTOM_ZERO_QTY_OPENED.value
    assert truth["project_record_status"] == "phantom_zero_qty_project_opened_no_real_trade"
    assert truth["close_coverage"]["long"]["covered"] is False
    assert truth["close_coverage"]["short"]["covered"] is False


def test_sparse_runtime_position_opened_does_not_overwrite_entry_opened_truth():
    position_id = "entry-1779382428040-PROVEUSDT"
    truth = _truth(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "PROVEUSDT",
                    "quantity": 74,
                    "matched_quantity": 74,
                    "long_venue": "aster",
                    "short_venue": "bybit",
                    "long_entry_price": 0.3209,
                    "short_entry_price": 0.3211,
                },
            ),
            _event(
                1_100,
                "runtime.position_opened",
                {
                    "position_id": position_id,
                    "symbol": "PROVEUSDT",
                },
            ),
            _event(
                1_200,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "PROVEUSDT",
                    "phase": "open",
                    "venue": "aster",
                    "leg": "long",
                    "order_id": "open-long",
                    "quantity": 74,
                    "price": 0.3209,
                },
            ),
            _event(
                1_300,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "PROVEUSDT",
                    "phase": "open",
                    "venue": "bybit",
                    "leg": "short",
                    "order_id": "open-short",
                    "quantity": 74,
                    "price": 0.3211,
                },
            ),
            _event(
                2_000,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "PROVEUSDT",
                    "phase": "close",
                    "venue": "aster",
                    "leg": "long",
                    "order_id": "close-long",
                    "quantity": 74,
                    "price": 0.3210,
                },
            ),
            _event(
                2_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "PROVEUSDT",
                    "phase": "close",
                    "venue": "bybit",
                    "leg": "short",
                    "order_id": "close-short",
                    "quantity": 74,
                    "price": 0.3208,
                },
            ),
        ],
        position_id,
    )

    assert truth["target_quantity"] == "74"
    assert truth["long_venue"] == "aster"
    assert truth["short_venue"] == "bybit"
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value


def test_entry_opened_exact_fill_timestamps_supply_open_leg_truth():
    position_id = "entry-1782982448575-LABUSDT"
    truth = _truth(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "quantity": 5,
                    "matched_quantity": 5,
                    "long_venue": "bitget",
                    "short_venue": "okx",
                    "long_quantity": 5,
                    "short_quantity": 5,
                    "long_entry_price": 0.100,
                    "short_entry_price": 0.101,
                    "long_entry_fee_quote": 0.10,
                    "short_entry_fee_quote": 0.20,
                    "maker_fill_timestamp_quality": "exchange_fill_exact",
                    "hedge_fill_timestamp_quality": "exchange_fill_exact",
                    "entry_timestamp_quality": "exchange_fill_exact",
                    "maker_filled_at_ms": 990,
                    "hedge_filled_at_ms": 995,
                    "opened_at_ms": 1_000,
                },
            ),
            _event(
                2_000,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "close",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "1456505976935575553",
                    "quantity": 5,
                    "price": 0.102,
                    "fee_quote": 0.10,
                },
            ),
            _event(
                2_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "close",
                    "venue": "okx",
                    "leg": "short",
                    "order_id": "3707107089734017024",
                    "quantity": 5,
                    "price": 0.100,
                    "fee_quote": 0.10,
                },
            ),
        ],
        position_id,
    )

    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["open_coverage"]["long"]["covered"] is True
    assert truth["open_coverage"]["short"]["covered"] is True
    assert truth["open_legs"][0]["source"] == "entry.opened"
    assert truth["pnl"]["entry_fee_quote"] == "-0.3"


def test_lifecycle_completion_uses_target_coverage_not_zero_leg_only():
    position_id = "entry-1779789403921-BEATUSDT"
    truth = _truth(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "quantity": 24,
                    "long_venue": "bitget",
                    "short_venue": "bybit",
                    "long_entry_price": 0.10,
                    "short_entry_price": 0.11,
                },
            ),
            _event(
                1_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "phase": "open",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "open-long",
                    "quantity": 24,
                    "price": 0.10,
                    "fee_quote": 0.01,
                },
            ),
            _event(
                1_200,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "phase": "open",
                    "venue": "bybit",
                    "leg": "short",
                    "order_id": "open-short",
                    "quantity": 24,
                    "price": 0.11,
                    "fee_quote": 0.01,
                },
            ),
            _event(
                2_000,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "source": "close",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "close-long",
                    "quantity": 24,
                    "price": 0.105,
                    "fee_quote": 0.01,
                },
            ),
            _event(
                2_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "source": "close",
                    "venue": "bybit",
                    "leg": "short",
                    "order_id": "close-short-a",
                    "quantity": 15,
                    "price": 0.104,
                    "fee_quote": 0.01,
                },
            ),
            _event(
                2_200,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "source": "close",
                    "venue": "bybit",
                    "leg": "short",
                    "order_id": "close-short-b",
                    "quantity": 9,
                    "price": 0.103,
                    "fee_quote": 0.01,
                },
            ),
        ],
        position_id,
    )

    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["long"]["filled_qty"] == "24"
    assert truth["close_coverage"]["short"]["filled_qty"] == "24"
    assert truth["project_record_status"] == "missing_exit_reconciliation"


def test_bitget_trade_side_close_is_mapped_by_venue_not_raw_side():
    position_id = "entry-1782874583508-TAIKOUSDT"
    truth = _truth(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "TAIKOUSDT",
                    "quantity": 295,
                    "long_venue": "bybit",
                    "short_venue": "bitget",
                },
            ),
            _event(
                1_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "TAIKOUSDT",
                    "phase": "open",
                    "venue": "bybit",
                    "leg": "long",
                    "order_id": "open-long",
                    "quantity": 295,
                    "price": 0.811,
                    "fee_quote": 0.04,
                },
            ),
            _event(
                1_200,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "TAIKOUSDT",
                    "phase": "open",
                    "venue": "bitget",
                    "leg": "short",
                    "order_id": "open-short",
                    "quantity": 295,
                    "price": 0.814,
                    "fee_quote": 0.05,
                },
            ),
            _event(
                2_000,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "TAIKOUSDT",
                    "source": "exchange_trade_history",
                    "venue": "bitget",
                    "order_id": "1456049108408758276",
                    "side": "sell",
                    "tradeSide": "close",
                    "quantity": 295,
                    "price": 0.812,
                    "fee_quote": 0.05,
                },
            ),
            _event(
                2_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "TAIKOUSDT",
                    "source": "exchange_trade_history",
                    "venue": "bybit",
                    "order_id": "1a95-close-long",
                    "side": "sell",
                    "tradeSide": "close",
                    "quantity": 295,
                    "price": 0.813,
                    "fee_quote": 0.04,
                },
            ),
        ],
        position_id,
    )

    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["short"]["order_ids"] == ["1456049108408758276"]
    assert truth["close_coverage"]["long"]["order_ids"] == ["1a95-close-long"]


def test_legacy_exit_closed_without_exchange_fill_is_not_complete():
    position_id = "entry-legacy-gap"
    truth = _truth(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "TAIKOUSDT",
                    "quantity": 2,
                    "long_venue": "binance",
                    "short_venue": "bitget",
                },
            ),
            _event(
                2_000,
                "exit.closed",
                {
                    "position_id": position_id,
                    "long_closed_qty": 2,
                    "short_closed_qty": 2,
                    "long_uncertain": False,
                    "short_uncertain": False,
                    "net_quote": "20.0",
                },
            ),
        ],
        position_id,
    )

    assert truth["classification"] == LifecycleClassification.EVIDENCE_INCOMPLETE.value
    assert truth["project_record_status"] == "legacy_exit_closed_missing_exchange_fill_evidence"
    assert truth["pnl"]["net_pnl_quote"] == "0"


def test_rebuild_lifecycle_truth_cli_dry_run(tmp_path: Path):
    position_id = "entry-1782874583508-TAIKOUSDT"
    events_path = tmp_path / "live-events.jsonl"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "quantity": 1,
                "long_venue": "bybit",
                "short_venue": "bitget",
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "venue": "bybit",
                "leg": "long",
                "order_id": "open-long",
                "quantity": 1,
                "price": 1,
            },
        ),
        _event(
            2_050,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "venue": "bitget",
                "leg": "short",
                "order_id": "open-short",
                "quantity": 1,
                "price": 1,
            },
        ),
        _event(
            2_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "source": "exchange_trade_history",
                "venue": "bitget",
                "leg": "short",
                "tradeSide": "close",
                "order_id": "close-short",
                "quantity": 1,
                "price": 1,
            },
        ),
        _event(
            2_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "source": "exchange_trade_history",
                "venue": "bybit",
                "leg": "long",
                "tradeSide": "close",
                "order_id": "close-long",
                "quantity": 1,
                "price": 1,
            },
        ),
    ]
    events_path.write_text("\n".join(json.dumps(event) for event in events), encoding="utf-8")
    positions_path = tmp_path / "positions.txt"
    positions_path.write_text(position_id + "\n", encoding="utf-8")

    proc = subprocess.run(
        [
            sys.executable,
            "scripts/rebuild_lifecycle_truth.py",
            "--events",
            str(events_path),
            "--positions-file",
            str(positions_path),
            "--dry-run",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )

    payload = json.loads(proc.stdout)
    assert payload["summary"]["exchange_lifecycle_complete"] == 1
    assert payload["positions"][position_id]["classification"] == (
        LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    )


def test_rebuild_lifecycle_truth_reads_only_selected_position_events(tmp_path: Path):
    from scripts import rebuild_lifecycle_truth

    keep_id = "entry-keep-LABUSDT"
    drop_id = "entry-drop-LABUSDT"
    events_path = tmp_path / "live-events.jsonl"
    rows = [
        _event(1_000, "entry.opened", {"position_id": keep_id, "symbol": "LABUSDT"}),
        _event(1_001, "entry.opened", {"position_id": drop_id, "symbol": "LABUSDT"}),
        _event(1_002, "runtime.position_opened", {"entry_id": keep_id, "symbol": "LABUSDT"}),
        {"kind": "runtime.heartbeat", "payload": {"ok": True}, "ts_ms": 1_003},
    ]
    events_path.write_text(
        "\n".join([*(json.dumps(row) for row in rows), "{not-json"]),
        encoding="utf-8",
    )

    events = rebuild_lifecycle_truth.read_jsonl_events(
        [events_path],
        position_ids={keep_id},
    )

    assert [event["kind"] for event in events] == [
        "entry.opened",
        "runtime.position_opened",
    ]
    assert all(
        (event.get("payload") or {}).get("position_id", (event.get("payload") or {}).get("entry_id"))
        == keep_id
        for event in events
    )


def test_rebuild_lifecycle_truth_queries_close_identity_into_fill_event():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-1779789403921-BEATUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "BEATUSDT",
                "quantity": 9,
                "long_venue": "bitget",
                "short_venue": "bybit",
            },
        ),
            _event(
                2_000,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "phase": "open",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "long-open",
                    "quantity": 9,
                    "price": 0.101,
                },
            ),
            _event(
                2_050,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "phase": "open",
                    "venue": "bybit",
                    "leg": "short",
                    "order_id": "short-open",
                    "quantity": 9,
                    "price": 0.104,
                },
            ),
            _event(
                2_075,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "BEATUSDT",
                    "source": "close",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "long-close",
                    "quantity": 9,
                    "price": 0.105,
                },
            ),
        _event(
            2_100,
            "exit.pending_close_reconciliation_registered",
            {
                "position_id": position_id,
                "statement_probe_candidates": [
                    {
                        "leg": "short",
                        "venue": "bybit",
                        "order_id": "short-close-probe",
                        "source": "maker_submitted_statement_probe",
                    }
                ],
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)

    class FakeAdapter:
        async def fetch_order_fill_reconciliation(
            self,
            symbol: str,
            order_id: str,
            client_order_id: str = "",
        ) -> OrderFillReconciliation:
            assert symbol == "BEATUSDT"
            assert order_id == "short-close-probe"
            assert client_order_id == ""
            return OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.BUY,
                quantity=9,
                average_price=0.103,
                order_id=order_id,
                fee_quote=0.01,
                filled_at_ms=2_200,
                metadata={"tradeSide": "close"},
            )

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["attempted"] == 1
    assert summary["filled"] == 1
    assert fill_events == [
        {
            "ts_ms": 2_200,
            "kind": "order.filled",
            "payload": {
                "position_id": position_id,
                "symbol": "BEATUSDT",
                "phase": "close",
                "leg": "short",
                "venue": "bybit",
                "order_id": "short-close-probe",
                "client_order_id": "",
                "side": "buy",
                "tradeSide": "close",
                "quantity": 9,
                "average_price": 0.103,
                "fee_quote": 0.01,
                "filled_at_ms": 2_200,
                "source": "rebuild_lifecycle_truth_exchange_query_close",
            },
        }
    ]

    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["short"]["covered"] is True


def test_rebuild_lifecycle_truth_queries_passive_close_maker_identity():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-1782867317803-INUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "INUSDT",
                "quantity": 360,
                "matched_quantity": 360,
                "long_venue": "binance",
                "short_venue": "bybit",
                "long_quantity": 360,
                "short_quantity": 360,
                "long_entry_price": 0.050,
                "short_entry_price": 0.051,
                "maker_fill_timestamp_quality": "exchange_fill_exact",
                "hedge_fill_timestamp_quality": "exchange_fill_exact",
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "INUSDT",
                "phase": "close",
                "venue": "binance",
                "leg": "long",
                "order_id": "1094652589-close",
                "quantity": 360,
                "price": 0.052,
            },
        ),
        _event(
            2_100,
            "exit.passive_close_maker_submitted",
            {
                "position_id": position_id,
                "symbol": "INUSDT",
                "maker_venue": "bybit",
                "maker_leg": "short",
                "order_id": "ba8d6524-3bac-4fa3-a3d8-91ff799bff6f",
                "client_order_id": "",
                "quantity": 360,
                "phase": "exit_short",
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)

    class FakeAdapter:
        async def fetch_order_fill_reconciliation(
            self,
            symbol: str,
            order_id: str,
            client_order_id: str = "",
        ) -> OrderFillReconciliation:
            assert symbol == "INUSDT"
            assert order_id == "ba8d6524-3bac-4fa3-a3d8-91ff799bff6f"
            assert client_order_id == ""
            return OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.BUY,
                quantity=360,
                average_price=0.050,
                order_id=order_id,
                fee_quote=0.02,
                filled_at_ms=2_200,
                metadata={"tradeSide": "close"},
            )

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["attempted"] == 1
    assert summary["filled"] == 1
    assert fill_events[0]["payload"]["phase"] == "close"
    assert fill_events[0]["payload"]["leg"] == "short"
    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value


def test_rebuild_lifecycle_truth_queries_open_submit_identities():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-1782856332836-TAIKOUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "quantity": 295,
                "matched_quantity": 295,
                "long_venue": "bybit",
                "short_venue": "bitget",
            },
        ),
        _event(
            1_020,
            "order.passive_submitted",
            {
                "entry_id": position_id,
                "symbol": "TAIKOUSDT",
                "venue": "bybit",
                "order_id": "open-long-bybit",
                "quantity": 295,
            },
        ),
        _event(
            1_030,
            "order.submitted",
            {
                "entry_id": position_id,
                "symbol": "TAIKOUSDT",
                "venue": "bitget",
                "order_id": "open-short-bitget",
                "quantity": 295,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "close",
                "venue": "bybit",
                "leg": "long",
                "order_id": "close-long-bybit",
                "quantity": 295,
                "price": 0.813,
            },
        ),
        _event(
            2_010,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "close",
                "venue": "bitget",
                "leg": "short",
                "order_id": "close-short-bitget",
                "quantity": 295,
                "price": 0.812,
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)

    class FakeAdapter:
        async def fetch_order_fill_reconciliation(
            self,
            symbol: str,
            order_id: str,
            client_order_id: str = "",
        ) -> OrderFillReconciliation:
            if order_id == "open-long-bybit":
                return OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=295,
                    average_price=0.811,
                    order_id=order_id,
                    fee_quote=0.04,
                    filled_at_ms=1_100,
                    metadata={"tradeSide": "open"},
                )
            if order_id == "open-short-bitget":
                return OrderFillReconciliation(
                    venue=Venue.BITGET,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=295,
                    average_price=0.814,
                    order_id=order_id,
                    fee_quote=0.05,
                    filled_at_ms=1_200,
                    metadata={"tradeSide": "open"},
                )
            raise AssertionError(order_id)

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["attempted"] == 2
    assert summary["filled"] == 2
    assert {event["payload"]["phase"] for event in fill_events} == {"open"}
    assert {event["payload"]["leg"] for event in fill_events} == {"long", "short"}
    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value


def test_entry_opened_infers_unlabeled_hedge_open_identity_from_submitted_maker():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-1779479522323-ALTUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "quantity": 19,
                "matched_quantity": 19,
                "long_venue": "binance",
                "short_venue": "bybit",
                "maker_order_id": "4492933796",
                "maker_client_order_id": "maker-open-client",
                "hedge_order_id": "d7df6fe1-1057-448f-9c79-16a91eb4087b",
                "hedge_client_order_id": "hedge-open-client",
            },
        ),
        _event(
            1_020,
            "order.passive_submitted",
            {
                "entry_id": position_id,
                "symbol": "ALTUSDT",
                "venue": "binance",
                "order_id": "4492933796",
                "client_order_id": "maker-open-client",
                "quantity": 19,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "phase": "close",
                "venue": "binance",
                "leg": "long",
                "order_id": "close-long-binance",
                "quantity": 19,
                "price": 0.041,
            },
        ),
        _event(
            2_010,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "phase": "close",
                "venue": "bybit",
                "leg": "short",
                "order_id": "close-short-bybit",
                "quantity": 19,
                "price": 0.0408,
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)

    class FakeAdapter:
        async def fetch_order_fill_reconciliation(
            self,
            symbol: str,
            order_id: str,
            client_order_id: str = "",
        ) -> OrderFillReconciliation:
            if order_id == "4492933796":
                return OrderFillReconciliation(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=19,
                    average_price=0.0407,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    filled_at_ms=1_100,
                    metadata={"tradeSide": "open"},
                )
            if order_id == "d7df6fe1-1057-448f-9c79-16a91eb4087b":
                return OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=19,
                    average_price=0.0409,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    filled_at_ms=1_120,
                    metadata={"tradeSide": "open"},
                )
            raise AssertionError(order_id)

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["attempted"] == 2
    assert {
        (event["payload"]["leg"], event["payload"]["venue"], event["payload"]["order_id"])
        for event in fill_events
    } == {
        ("long", "binance", "4492933796"),
        ("short", "bybit", "d7df6fe1-1057-448f-9c79-16a91eb4087b"),
    }
    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value


def test_entry_opened_infers_hedge_when_submitted_maker_uses_noncanonical_leg():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-1779479522323-ALTUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "quantity": 19,
                "matched_quantity": 19,
                "long_venue": "binance",
                "short_venue": "bybit",
                "maker_order_id": "4492933796",
                "maker_client_order_id": "maker-open-client",
                "hedge_order_id": "d7df6fe1-1057-448f-9c79-16a91eb4087b",
                "hedge_client_order_id": "hedge-open-client",
            },
        ),
        _event(
            1_020,
            "order.passive_submitted",
            {
                "entry_id": position_id,
                "symbol": "ALTUSDT",
                "venue": "binance",
                "leg": "maker",
                "order_id": "4492933796",
                "client_order_id": "maker-open-client",
                "quantity": 19,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "phase": "close",
                "venue": "binance",
                "leg": "long",
                "order_id": "close-long-binance",
                "quantity": 19,
                "price": 0.041,
            },
        ),
        _event(
            2_010,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "phase": "close",
                "venue": "bybit",
                "leg": "short",
                "order_id": "close-short-bybit",
                "quantity": 19,
                "price": 0.0408,
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)

    class FakeAdapter:
        async def fetch_order_fill_reconciliation(
            self,
            symbol: str,
            order_id: str,
            client_order_id: str = "",
        ) -> OrderFillReconciliation:
            if order_id == "4492933796":
                return OrderFillReconciliation(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=19,
                    average_price=0.0407,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    filled_at_ms=1_100,
                    metadata={"tradeSide": "open"},
                )
            if order_id == "d7df6fe1-1057-448f-9c79-16a91eb4087b":
                return OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=19,
                    average_price=0.0409,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    filled_at_ms=1_120,
                    metadata={"tradeSide": "open"},
                )
            raise AssertionError(order_id)

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["attempted"] == 2
    assert {
        (event["payload"]["leg"], event["payload"]["venue"], event["payload"]["order_id"])
        for event in fill_events
    } == {
        ("long", "binance", "4492933796"),
        ("short", "bybit", "d7df6fe1-1057-448f-9c79-16a91eb4087b"),
    }


def test_rebuild_lifecycle_truth_treats_binance_order_not_exist_as_not_found():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-binance-not-found"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "quantity": 19,
                "matched_quantity": 19,
                "long_venue": "binance",
                "short_venue": "bybit",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "phase": "open",
                "venue": "binance",
                "leg": "long",
                "order_id": "open-long",
                "quantity": 19,
                "price": 0.0407,
            },
        ),
        _event(
            1_120,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "phase": "open",
                "venue": "bybit",
                "leg": "short",
                "order_id": "open-short",
                "quantity": 19,
                "price": 0.0409,
            },
        ),
        _event(
            2_000,
            "exit.pending_close_reconciliation_registered",
            {
                "position_id": position_id,
                "statement_probe_candidates": [
                    {
                        "leg": "long",
                        "venue": "binance",
                        "order_id": "missing-binance-order",
                    }
                ],
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)

    class FakeAdapter:
        async def fetch_order_fill_reconciliation(
            self,
            symbol: str,
            order_id: str,
            client_order_id: str = "",
        ) -> None:
            raise RuntimeError('HTTP 400: {"code":-2013,"msg":"Order does not exist."}')

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert fill_events == []
    assert summary["attempted"] == 1
    assert summary["not_found"] == 1
    assert summary["errors"] == []


def test_rebuild_lifecycle_truth_queries_exchange_until_no_new_fill_events():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-iterative-query"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "quantity": 19,
                "matched_quantity": 19,
                "long_venue": "binance",
                "short_venue": "bybit",
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "phase": "close",
                "venue": "binance",
                "leg": "long",
                "order_id": "close-long-binance",
                "quantity": 19,
                "price": 0.041,
            },
        ),
        _event(
            2_010,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "ALTUSDT",
                "phase": "close",
                "venue": "bybit",
                "leg": "short",
                "order_id": "close-short-bybit",
                "quantity": 19,
                "price": 0.0408,
            },
        ),
    ]
    first_fill = _event(
        1_100,
        "order.filled",
        {
            "position_id": position_id,
            "symbol": "ALTUSDT",
            "phase": "open",
            "leg": "long",
            "venue": "binance",
            "order_id": "open-long-binance",
            "quantity": 19,
            "price": 0.0407,
        },
    )
    second_fill = _event(
        1_120,
        "order.filled",
        {
            "position_id": position_id,
            "symbol": "ALTUSDT",
            "phase": "open",
            "leg": "short",
            "venue": "bybit",
            "order_id": "open-short-bybit",
            "quantity": 19,
            "price": 0.0409,
        },
    )
    calls = 0

    async def fake_query(report: dict) -> tuple[list[dict], dict]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return [first_fill], {"enabled": True, "attempted": 1, "filled": 1, "errors": []}
        if calls == 2:
            return [second_fill], {"enabled": True, "attempted": 1, "filled": 1, "errors": []}
        return [], {"enabled": True, "attempted": 0, "filled": 0, "errors": []}

    report, fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events_until_stable(
            events,
            position_ids={position_id},
            query_func=fake_query,
            max_passes=3,
        )
    )

    assert calls == 3
    assert fill_events == [first_fill, second_fill]
    assert summary["pass_count"] == 3
    assert summary["synthetic_fill_event_count"] == 2
    assert report["positions"][position_id]["classification"] == (
        LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    )


def test_hedge_duplicate_close_identity_infers_opposite_leg_and_venue():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-1779551578130-LYNUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "LYNUSDT",
                "quantity": 100,
                "matched_quantity": 100,
                "long_venue": "binance",
                "short_venue": "bybit",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LYNUSDT",
                "phase": "open",
                "venue": "binance",
                "leg": "long",
                "order_id": "open-long-binance",
                "quantity": 100,
                "price": 0.10,
            },
        ),
        _event(
            1_120,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LYNUSDT",
                "phase": "open",
                "venue": "bybit",
                "leg": "short",
                "order_id": "open-short-bybit",
                "quantity": 100,
                "price": 0.101,
            },
        ),
        _event(
            2_000,
            "exit.passive_close_maker_submitted",
            {
                "position_id": position_id,
                "symbol": "LYNUSDT",
                "maker_venue": "binance",
                "maker_leg": "long",
                "order_id": "close-long-binance",
                "quantity": 100,
            },
        ),
        _event(
            2_020,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LYNUSDT",
                "phase": "close",
                "venue": "binance",
                "leg": "long",
                "order_id": "close-long-binance",
                "quantity": 100,
                "price": 0.102,
            },
        ),
        _event(
            2_050,
            "exit.passive_close_hedge_duplicate_client_order_reconciled",
            {
                "position_id": position_id,
                "symbol": "LYNUSDT",
                "order_id": "95dbe960-6b01-4259-958b-02ef11bb6dbc",
                "client_order_id": "lfexfff-hedge-close",
                "quantity": 100,
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)

    class FakeAdapter:
        async def fetch_order_fill_reconciliation(
            self,
            symbol: str,
            order_id: str,
            client_order_id: str = "",
        ) -> OrderFillReconciliation:
            assert order_id == "95dbe960-6b01-4259-958b-02ef11bb6dbc"
            return OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.BUY,
                quantity=100,
                average_price=0.099,
                order_id=order_id,
                client_order_id=client_order_id,
                filled_at_ms=2_100,
                metadata={"tradeSide": "close"},
            )

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["attempted"] == 1
    assert fill_events[0]["payload"]["phase"] == "close"
    assert fill_events[0]["payload"]["leg"] == "short"
    assert fill_events[0]["payload"]["venue"] == "bybit"
    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value


def test_rebuild_lifecycle_truth_queries_historical_identity_with_time_window():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-1783000000000-OLDUSDT"
    submitted_at_ms = 1_783_000_000_000
    events = [
        _event(
            submitted_at_ms,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "OLDUSDT",
                "quantity": 12,
                "matched_quantity": 12,
                "long_venue": "binance",
                "short_venue": "bybit",
                "maker_venue": "bybit",
                "maker_leg": "short",
                "maker_order_id": "bybit-old-open",
                "maker_client_order_id": "cid-old-open",
                "maker_quantity": 12,
                "submitted_at_ms": submitted_at_ms,
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)
    identity = report["positions"][position_id]["order_identity_history"][0]
    assert identity["submitted_at_ms"] == submitted_at_ms

    seen_windows: list[tuple[int | None, int | None]] = []

    class FakeAdapter:
        async def fetch_order_fill_reconciliation(
            self,
            symbol: str,
            order_id: str,
            client_order_id: str = "",
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> OrderFillReconciliation:
            assert symbol == "OLDUSDT"
            assert order_id == "bybit-old-open"
            assert client_order_id == "cid-old-open"
            seen_windows.append((start_time_ms, end_time_ms))
            assert start_time_ms is not None
            assert end_time_ms is not None
            assert start_time_ms < submitted_at_ms < end_time_ms
            return OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.SELL,
                quantity=12,
                average_price=0.25,
                order_id=order_id,
                client_order_id=client_order_id,
                fee_quote=0.01,
                filled_at_ms=submitted_at_ms + 100,
                metadata={"tradeSide": "open"},
            )

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert seen_windows
    assert summary["windowed_query_count"] == 1
    assert summary["filled"] == 1
    assert fill_events[0]["payload"]["phase"] == "open"


def test_rebuild_lifecycle_truth_uses_account_history_when_close_identity_is_missing():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-account-history-TAIKOUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "quantity": 2,
                "matched_quantity": 2,
                "long_venue": "bybit",
                "short_venue": "bitget",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "bybit",
                "order_id": "open-long",
                "quantity": 2,
                "price": 0.081,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "bitget",
                "order_id": "open-short",
                "quantity": 2,
                "price": 0.082,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "close",
                "leg": "long",
                "venue": "bybit",
                "order_id": "close-long",
                "quantity": 2,
                "price": 0.083,
            },
        ),
        _event(
            2_100,
            "runtime.position_drift_flatten_leg",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "venue": "bitget",
                "leg": "short",
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)
    windows = rebuild_lifecycle_truth.position_event_windows(events, position_ids=[position_id])

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            assert symbol == "TAIKOUSDT"
            assert start_time_ms is not None and start_time_ms < 2_100
            assert end_time_ms is not None and end_time_ms > 2_100
            return [
                OrderFillReconciliation(
                    venue=Venue.BITGET,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=2,
                    average_price=0.0805,
                    order_id="close-short-from-history",
                    fee_quote=0.01,
                    filled_at_ms=2_105,
                    metadata={
                        "tradeSide": "close",
                        "trade_id": "bitget-trade-1",
                    },
                )
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["target_count"] == 1
    assert summary["attempted"] == 1
    assert summary["filled"] == 1
    assert fill_events == [
        {
            "ts_ms": 2_105,
            "kind": "order.filled",
            "payload": {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "close",
                "leg": "short",
                "venue": "bitget",
                "order_id": "close-short-from-history",
                "client_order_id": "",
                "side": "buy",
                "tradeSide": "close",
                "quantity": 2,
                "average_price": 0.0805,
                "fee_quote": 0.01,
                "filled_at_ms": 2_105,
                "source": "rebuild_lifecycle_truth_exchange_account_history_close",
                "trade_id": "bitget-trade-1",
                "exec_id": "",
            },
        }
    ]

    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["short"]["covered"] is True


def test_rebuild_lifecycle_truth_account_history_falls_back_when_statement_omits_local_identity():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-account-history-fallback-LABUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "quantity": 2,
                "matched_quantity": 2,
                "long_venue": "bitget",
                "short_venue": "bybit",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "bitget",
                "order_id": "open-long",
                "quantity": 2,
                "price": 10.0,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "bybit",
                "order_id": "open-short",
                "quantity": 2,
                "price": 10.1,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "phase": "close",
                "leg": "short",
                "venue": "bybit",
                "order_id": "close-short",
                "quantity": 2,
                "price": 10.2,
            },
        ),
        _event(
            2_050,
            "exit.accepted_order_truth_gap_registered",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "phase": "close",
                "leg": "long",
                "venue": "bitget",
                "client_order_id": "local-close-client-id",
                "quantity_hint": 2,
            },
        ),
        _event(
            3_700_000,
            "exit.passive_close_recovery_probe_diagnostic",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "venue": "bitget",
                "leg": "long",
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)
    windows = rebuild_lifecycle_truth.position_event_windows(events, position_ids=[position_id])

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            assert symbol == "LABUSDT"
            return [
                OrderFillReconciliation(
                    venue=Venue.BITGET,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=2,
                    average_price=10.15,
                    order_id="exchange-history-order-id",
                    client_order_id="",
                    fee_quote=0.01,
                    filled_at_ms=2_060,
                    metadata={
                        "tradeSide": "close",
                        "trade_id": "bitget-fallback-trade",
                    },
                )
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["filled"] == 1
    assert summary["identity_fallback_filled"] == 1
    assert fill_events[0]["payload"]["identity_match_mode"] == "fallback"

    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["long"]["covered"] is True


def test_rebuild_lifecycle_truth_account_history_accepts_okx_net_position_side():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-account-history-okx-net-UBUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "UBUSDT",
                "quantity": 100,
                "matched_quantity": 100,
                "long_venue": "binance",
                "short_venue": "okx",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "UBUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "binance",
                "order_id": "open-long",
                "quantity": 100,
                "price": 0.1595,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "UBUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "okx",
                "order_id": "open-short",
                "quantity": 100,
                "price": 0.1602,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "UBUSDT",
                "phase": "close",
                "leg": "long",
                "venue": "binance",
                "order_id": "close-long",
                "quantity": 100,
                "price": 0.1601,
            },
        ),
        _event(
            2_050,
            "runtime.position_drift_flatten_leg",
            {
                "position_id": position_id,
                "symbol": "UBUSDT",
                "venue": "okx",
                "leg": "short",
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)
    windows = rebuild_lifecycle_truth.position_event_windows(events, position_ids=[position_id])

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            return [
                OrderFillReconciliation(
                    venue=Venue.OKX,
                    symbol="UB-USDT-SWAP",
                    side=Side.BUY,
                    quantity=100,
                    average_price=0.1606,
                    order_id="okx-close-short",
                    fee_quote=0.01,
                    filled_at_ms=2_060,
                    metadata={"positionSide": "net", "trade_id": "okx-trade-1"},
                )
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["filled"] == 1
    assert fill_events[0]["payload"]["venue"] == "okx"
    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["short"]["covered"] is True


def test_rebuild_lifecycle_truth_account_history_continues_after_existing_partial_identity():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-account-history-partial-BEATUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "BEATUSDT",
                "quantity": 24,
                "matched_quantity": 24,
                "long_venue": "aster",
                "short_venue": "bybit",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "BEATUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "aster",
                "order_id": "open-long",
                "quantity": 24,
                "price": 0.9618,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "BEATUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "bybit",
                "order_id": "open-short",
                "quantity": 24,
                "price": 0.9592,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "BEATUSDT",
                "phase": "close",
                "leg": "long",
                "venue": "aster",
                "order_id": "close-long",
                "quantity": 24,
                "price": 0.9601,
            },
        ),
        _event(
            2_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "BEATUSDT",
                "phase": "close",
                "leg": "short",
                "venue": "bybit",
                "order_id": "partial-short-close",
                "client_order_id": "partial-short-client",
                "quantity": 15,
                "average_price": 0.9623,
                "filled_at_ms": 2_100,
                "exec_id": "partial-exec",
            },
        ),
        _event(
            10_000,
            "exit.passive_close_recovery_probe_flat",
            {
                "position_id": position_id,
                "symbol": "BEATUSDT",
                "venue": "bybit",
                "leg": "short",
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)
    windows = rebuild_lifecycle_truth.position_event_windows(events, position_ids=[position_id])

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            return [
                OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=15,
                    average_price=0.9623,
                    order_id="partial-short-close",
                    client_order_id="partial-short-client",
                    fee_quote=0.01,
                    filled_at_ms=2_100,
                    metadata={"exec_id": "partial-exec"},
                ),
                OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=3,
                    average_price=0.9981,
                    order_id="late-short-close",
                    fee_quote=0.01,
                    filled_at_ms=10_000,
                    metadata={"exec_id": "late-exec-a"},
                ),
                OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=6,
                    average_price=0.9979,
                    order_id="late-short-close",
                    fee_quote=0.01,
                    filled_at_ms=10_000,
                    metadata={"exec_id": "late-exec-b"},
                ),
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            existing_events=events,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["filled"] == 2
    assert summary["identity_fallback_filled"] == 2
    assert [event["payload"]["exec_id"] for event in fill_events] == [
        "late-exec-a",
        "late-exec-b",
    ]
    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["short"]["filled_qty"] == "24"


def test_rebuild_lifecycle_truth_account_history_respects_next_symbol_entry_boundary():
    from scripts import rebuild_lifecycle_truth

    first_id = "entry-account-history-boundary-a-TAIKOUSDT"
    second_id = "entry-account-history-boundary-b-TAIKOUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "quantity": 2,
                "matched_quantity": 2,
                "long_venue": "bybit",
                "short_venue": "bitget",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "bybit",
                "order_id": "first-open-long",
                "quantity": 2,
                "price": 0.081,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "bitget",
                "order_id": "first-open-short",
                "quantity": 2,
                "price": 0.082,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "phase": "close",
                "leg": "short",
                "venue": "bitget",
                "order_id": "first-close-short",
                "quantity": 2,
                "price": 0.0805,
            },
        ),
        _event(
            100_000,
            "exit.passive_close_recovery_probe_flat",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "venue": "bybit",
                "leg": "long",
            },
        ),
        _event(
            5_000,
            "entry.opened",
            {
                "position_id": second_id,
                "symbol": "TAIKOUSDT",
                "quantity": 2,
                "matched_quantity": 2,
                "long_venue": "bybit",
                "short_venue": "bitget",
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events, position_ids={first_id})
    windows = rebuild_lifecycle_truth.position_event_windows(events)

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            return [
                OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=2,
                    average_price=0.083,
                    order_id="wrong-next-position-close",
                    fee_quote=0.01,
                    filled_at_ms=6_000,
                    metadata={"exec_id": "wrong-exec"},
                ),
                OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=2,
                    average_price=0.0828,
                    order_id="first-position-close",
                    fee_quote=0.01,
                    filled_at_ms=2_500,
                    metadata={"exec_id": "first-exec"},
                ),
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["filled"] == 1
    assert summary["lifecycle_time_filtered"] >= 1
    assert fill_events[0]["payload"]["order_id"] == "first-position-close"
    rebuilt = build_exchange_truth_lifecycle(events + fill_events, position_ids={first_id})
    truth = rebuilt["positions"][first_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["long"]["order_ids"] == ["first-position-close"]


def test_rebuild_lifecycle_truth_account_history_allows_next_entry_settlement_grace():
    from scripts import rebuild_lifecycle_truth

    first_id = "entry-account-history-boundary-grace-a-BSBUSDT"
    second_id = "entry-account-history-boundary-grace-b-BSBUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": first_id,
                "symbol": "BSBUSDT",
                "quantity": 30,
                "matched_quantity": 30,
                "long_venue": "aster",
                "short_venue": "okx",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "BSBUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "aster",
                "order_id": "first-open-long",
                "quantity": 30,
                "price": 0.8277,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "BSBUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "okx",
                "order_id": "first-open-short",
                "quantity": 30,
                "price": 0.8278,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "BSBUSDT",
                "phase": "close",
                "leg": "long",
                "venue": "aster",
                "order_id": "first-close-long",
                "quantity": 30,
                "price": 0.8263,
            },
        ),
        _event(
            5_000,
            "entry.opened",
            {
                "position_id": second_id,
                "symbol": "BSBUSDT",
                "quantity": 30,
                "matched_quantity": 30,
                "long_venue": "aster",
                "short_venue": "okx",
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events, position_ids={first_id})
    windows = rebuild_lifecycle_truth.position_event_windows(events)

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            return [
                OrderFillReconciliation(
                    venue=Venue.OKX,
                    symbol="BSB-USDT-SWAP",
                    side=Side.BUY,
                    quantity=30,
                    average_price=0.8018,
                    order_id="previous-close-inside-grace",
                    fee_quote=0.01,
                    filled_at_ms=5_050,
                    metadata={"positionSide": "net", "trade_id": "inside-grace"},
                ),
                OrderFillReconciliation(
                    venue=Venue.OKX,
                    symbol="BSB-USDT-SWAP",
                    side=Side.BUY,
                    quantity=30,
                    average_price=0.4563,
                    order_id="next-position-fill-outside-grace",
                    fee_quote=0.01,
                    filled_at_ms=6_000,
                    metadata={"positionSide": "net", "trade_id": "outside-grace"},
                ),
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["filled"] == 1
    assert summary["lifecycle_time_filtered"] >= 1
    assert fill_events[0]["payload"]["order_id"] == "previous-close-inside-grace"
    rebuilt = build_exchange_truth_lifecycle(events + fill_events, position_ids={first_id})
    truth = rebuilt["positions"][first_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value


def test_rebuild_lifecycle_truth_streams_context_windows_for_filtered_positions(tmp_path: Path):
    from scripts import rebuild_lifecycle_truth

    first_id = "entry-account-history-stream-window-a-TAIKOUSDT"
    second_id = "entry-account-history-stream-window-b-TAIKOUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "quantity": 2,
                "matched_quantity": 2,
                "long_venue": "bybit",
                "short_venue": "bitget",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "bybit",
                "order_id": "first-open-long",
                "quantity": 2,
                "price": 0.081,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "bitget",
                "order_id": "first-open-short",
                "quantity": 2,
                "price": 0.082,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "phase": "close",
                "leg": "short",
                "venue": "bitget",
                "order_id": "first-close-short",
                "quantity": 2,
                "price": 0.0805,
            },
        ),
        _event(
            100_000,
            "exit.passive_close_recovery_probe_flat",
            {
                "position_id": first_id,
                "symbol": "TAIKOUSDT",
                "venue": "bybit",
                "leg": "long",
            },
        ),
        _event(
            5_000,
            "entry.opened",
            {
                "position_id": second_id,
                "symbol": "TAIKOUSDT",
                "quantity": 2,
                "matched_quantity": 2,
                "long_venue": "bybit",
                "short_venue": "bitget",
            },
        ),
    ]
    path = tmp_path / "live-events.jsonl"
    path.write_text(
        "\n".join(json.dumps(event, sort_keys=True) for event in events) + "\n",
        encoding="utf-8",
    )
    selected_events = rebuild_lifecycle_truth.read_jsonl_events(
        [path],
        position_ids={first_id},
    )
    streamed_windows = rebuild_lifecycle_truth.read_position_event_windows([path])

    assert all(event["payload"]["position_id"] == first_id for event in selected_events)
    assert second_id in streamed_windows

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            return [
                OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=2,
                    average_price=0.083,
                    order_id="wrong-next-position-close",
                    fee_quote=0.01,
                    filled_at_ms=6_000,
                    metadata={"exec_id": "wrong-exec"},
                ),
                OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=2,
                    average_price=0.0828,
                    order_id="first-position-close",
                    fee_quote=0.01,
                    filled_at_ms=2_500,
                    metadata={"exec_id": "first-exec"},
                ),
            ]

    report = build_exchange_truth_lifecycle(selected_events, position_ids={first_id})
    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=streamed_windows,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["filled"] == 1
    assert summary["lifecycle_time_filtered"] >= 1
    assert fill_events[0]["payload"]["order_id"] == "first-position-close"


def test_rebuild_lifecycle_truth_account_history_bitget_close_short_uses_trade_side():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-account-history-bitget-short-TAIKOUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "quantity": 295,
                "matched_quantity": 295,
                "long_venue": "bybit",
                "short_venue": "bitget",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "bybit",
                "order_id": "open-long",
                "quantity": 295,
                "price": 0.081,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "bitget",
                "order_id": "open-short",
                "quantity": 295,
                "price": 0.082,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "phase": "close",
                "leg": "long",
                "venue": "bybit",
                "order_id": "close-long",
                "quantity": 295,
                "price": 0.083,
            },
        ),
        _event(
            2_050,
            "runtime.position_drift_flatten_leg",
            {
                "position_id": position_id,
                "symbol": "TAIKOUSDT",
                "venue": "bitget",
                "leg": "short",
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)
    windows = rebuild_lifecycle_truth.position_event_windows(events, position_ids=[position_id])

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            assert symbol == "TAIKOUSDT"
            return [
                OrderFillReconciliation(
                    venue=Venue.BITGET,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=295,
                    average_price=0.0817,
                    order_id="1456049108408758276",
                    fee_quote=0.01,
                    filled_at_ms=2_060,
                    metadata={
                        "tradeSide": "close",
                        "trade_id": "1456049108604944384",
                    },
                )
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["filled"] == 1
    assert fill_events[0]["payload"]["leg"] == "short"
    assert fill_events[0]["payload"]["side"] == "sell"

    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["short"]["covered"] is True


def test_rebuild_lifecycle_truth_account_history_keeps_same_order_time_trade_ids():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-account-history-binance-multifill-EPICUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "quantity": 50.9,
                "matched_quantity": 50.9,
                "long_venue": "binance",
                "short_venue": "bitget",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "binance",
                "order_id": "open-long",
                "quantity": 50.9,
                "price": 0.47,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "bitget",
                "order_id": "open-short",
                "quantity": 50.9,
                "price": 0.472,
            },
        ),
        _event(
            2_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "phase": "close",
                "leg": "short",
                "venue": "bitget",
                "order_id": "close-short",
                "quantity": 50.9,
                "price": 0.471,
            },
        ),
    ]

    async def fake_account_history_query(report: dict, windows: dict):
        return (
            [
                _event(
                    2_200,
                    "order.filled",
                    {
                        "position_id": position_id,
                        "symbol": "EPICUSDT",
                        "phase": "close",
                        "leg": "long",
                        "venue": "binance",
                        "order_id": "1094652589",
                        "client_order_id": "",
                        "side": "sell",
                        "tradeSide": "close",
                        "quantity": quantity,
                        "average_price": 0.4714,
                        "fee_quote": 0.01,
                        "filled_at_ms": 2_200,
                        "source": "rebuild_lifecycle_truth_exchange_account_history_close",
                        "trade_id": trade_id,
                        "exec_id": "",
                    },
                )
                for quantity, trade_id in (
                    (10.9, "112559178"),
                    (11.8, "112559179"),
                    (28.2, "112559180"),
                )
            ],
            {"enabled": True, "attempted": 1, "filled": 3, "errors": []},
        )

    report, queried_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events_until_stable(
            events,
            position_ids=[position_id],
            query_func=lambda report: asyncio.sleep(
                0,
                result=(
                    [],
                    {"enabled": True, "attempted": 0, "filled": 0, "errors": []},
                ),
            ),
            account_history_query_func=fake_account_history_query,
        )
    )

    assert len(queried_events) == 3
    assert summary["synthetic_fill_event_count"] == 3
    truth = report["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["long"]["filled_qty"] == "50.9"


def test_rebuild_lifecycle_truth_account_history_consumes_existing_aggregate_once():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-account-history-existing-aggregate-EPICUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "quantity": 3,
                "matched_quantity": 3,
                "long_venue": "binance",
                "short_venue": "bitget",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "phase": "open",
                "leg": "long",
                "venue": "binance",
                "order_id": "open-long",
                "quantity": 3,
                "price": 0.47,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "phase": "open",
                "leg": "short",
                "venue": "bitget",
                "order_id": "open-short",
                "quantity": 3,
                "price": 0.472,
            },
        ),
        _event(
            2_000,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "phase": "close",
                "leg": "long",
                "venue": "binance",
                "order_id": "close-long-binance",
                "client_order_id": "",
                "side": "sell",
                "quantity": 1,
                "price": 0.4714,
                "average_price": 0.4714,
                "filled_at_ms": 2_000,
            },
        ),
        _event(
            2_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "EPICUSDT",
                "phase": "close",
                "leg": "short",
                "venue": "bitget",
                "order_id": "close-short",
                "quantity": 3,
                "price": 0.471,
            },
        ),
    ]
    report = build_exchange_truth_lifecycle(events)
    windows = rebuild_lifecycle_truth.position_event_windows(events, position_ids=[position_id])

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            return [
                OrderFillReconciliation(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=1,
                    average_price=0.4714,
                    order_id="close-long-binance",
                    fee_quote=0.01,
                    filled_at_ms=2_000,
                    metadata={"tradeSide": "close", "trade_id": "112559178"},
                ),
                OrderFillReconciliation(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=1,
                    average_price=0.4714,
                    order_id="close-long-binance",
                    fee_quote=0.01,
                    filled_at_ms=2_000,
                    metadata={"tradeSide": "close", "trade_id": "112559179"},
                ),
                OrderFillReconciliation(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=1,
                    average_price=0.4714,
                    order_id="close-long-binance",
                    fee_quote=0.01,
                    filled_at_ms=2_000,
                    metadata={"tradeSide": "close", "trade_id": "112559180"},
                ),
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            existing_events=events,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["filled"] == 2
    assert [event["payload"]["trade_id"] for event in fill_events] == [
        "112559179",
        "112559180",
    ]

    rebuilt = build_exchange_truth_lifecycle(events + fill_events)
    truth = rebuilt["positions"][position_id]
    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["close_coverage"]["long"]["filled_qty"] == "3"


def test_rebuild_lifecycle_truth_account_history_window_caps_future_end(monkeypatch):
    from scripts import rebuild_lifecycle_truth

    now_ms = 20_000
    monkeypatch.setattr(rebuild_lifecycle_truth.time, "time", lambda: now_ms / 1000)

    start_time_ms, end_time_ms = rebuild_lifecycle_truth._account_history_query_window(18_000)

    assert end_time_ms == now_ms
    assert start_time_ms < end_time_ms
    assert end_time_ms - start_time_ms <= rebuild_lifecycle_truth.ACCOUNT_HISTORY_QUERY_WINDOW_MS


def test_rebuild_lifecycle_truth_account_history_fill_is_not_double_assigned():
    from scripts import rebuild_lifecycle_truth

    position_ids = [
        "entry-account-history-dup-a-TAIKOUSDT",
        "entry-account-history-dup-b-TAIKOUSDT",
    ]
    events: list[dict] = []
    for index, position_id in enumerate(position_ids):
        offset = index * 100
        events.extend(
            [
                _event(
                    1_000 + offset,
                    "entry.opened",
                    {
                        "position_id": position_id,
                        "symbol": "TAIKOUSDT",
                        "quantity": 2,
                        "matched_quantity": 2,
                        "long_venue": "bybit",
                        "short_venue": "bitget",
                    },
                ),
                _event(
                    1_010 + offset,
                    "order.filled",
                    {
                        "position_id": position_id,
                        "symbol": "TAIKOUSDT",
                        "phase": "open",
                        "leg": "long",
                        "venue": "bybit",
                        "order_id": f"{position_id}-open-long",
                        "quantity": 2,
                        "price": 0.081,
                    },
                ),
                _event(
                    1_020 + offset,
                    "order.filled",
                    {
                        "position_id": position_id,
                        "symbol": "TAIKOUSDT",
                        "phase": "open",
                        "leg": "short",
                        "venue": "bitget",
                        "order_id": f"{position_id}-open-short",
                        "quantity": 2,
                        "price": 0.082,
                    },
                ),
                _event(
                    2_000 + offset,
                    "order.filled",
                    {
                        "position_id": position_id,
                        "symbol": "TAIKOUSDT",
                        "phase": "close",
                        "leg": "long",
                        "venue": "bybit",
                        "order_id": f"{position_id}-close-long",
                        "quantity": 2,
                        "price": 0.083,
                    },
                ),
                _event(
                    2_100 + offset,
                    "runtime.position_drift_flatten_leg",
                    {
                        "position_id": position_id,
                        "symbol": "TAIKOUSDT",
                        "venue": "bitget",
                        "leg": "short",
                    },
                ),
            ]
        )

    report = build_exchange_truth_lifecycle(events)
    windows = rebuild_lifecycle_truth.position_event_windows(events, position_ids=position_ids)

    class FakeAdapter:
        async def fetch_account_fill_reconciliations(
            self,
            symbol: str,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ) -> list[OrderFillReconciliation]:
            return [
                OrderFillReconciliation(
                    venue=Venue.BITGET,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=2,
                    average_price=0.0805,
                    order_id="same-exchange-order",
                    fee_quote=0.01,
                    filled_at_ms=2_105,
                    metadata={
                        "tradeSide": "close",
                        "trade_id": "same-bitget-trade",
                    },
                )
            ]

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_account_history_fill_events(
            report,
            position_windows=windows,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: FakeAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert summary["target_count"] == 2
    assert len(fill_events) == 1
    assert summary["filled"] == 1


def test_rebuild_lifecycle_truth_dry_run_returns_nonzero_when_expected_counts_mismatch(tmp_path: Path):
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-mismatch"
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(
        json.dumps(
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "matched_quantity": 0,
                    "long_venue": "bitget",
                    "short_venue": "bybit",
                },
            )
        )
        + "\n",
        encoding="utf-8",
    )
    positions_path = tmp_path / "positions.txt"
    positions_path.write_text(position_id + "\n", encoding="utf-8")

    rc = rebuild_lifecycle_truth.main(
        [
            "--events",
            str(events_path),
            "--positions-file",
            str(positions_path),
            "--dry-run",
            "--no-query-exchange",
            "--expected-complete",
            "1",
            "--expected-phantom-zero",
            "0",
            "--expected-exchange-bad",
            "0",
        ]
    )

    assert rc == 2


def test_rebuild_lifecycle_truth_loads_exchange_env_before_query(monkeypatch, tmp_path: Path):
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-env-load"
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(
        json.dumps(
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "quantity": 1,
                    "long_venue": "bitget",
                    "short_venue": "bybit",
                },
            )
        )
        + "\n",
        encoding="utf-8",
    )
    positions_path = tmp_path / "positions.txt"
    positions_path.write_text(position_id + "\n", encoding="utf-8")
    monkeypatch.delenv("LIGHTFEE_BYBIT_API_KEY", raising=False)
    observed: list[str] = []

    def fake_load_env() -> list[str]:
        os.environ["LIGHTFEE_BYBIT_API_KEY"] = "loaded-from-systemd-env"
        return ["/etc/lightfee/lightfee.env"]

    async def fake_query(report: dict) -> tuple[list[dict], dict]:
        observed.append(os.environ.get("LIGHTFEE_BYBIT_API_KEY", ""))
        return [], {"enabled": True, "candidate_count": 0}

    monkeypatch.setattr(
        rebuild_lifecycle_truth,
        "_load_exchange_truth_environment",
        fake_load_env,
    )
    monkeypatch.setattr(rebuild_lifecycle_truth, "query_exchange_fill_events", fake_query)

    rc = rebuild_lifecycle_truth.main(
        [
            "--events",
            str(events_path),
            "--positions-file",
            str(positions_path),
            "--dry-run",
        ]
    )

    assert rc == 0
    assert observed == ["loaded-from-systemd-env"]


def test_rebuild_lifecycle_truth_skips_query_for_already_complete_identity():
    from scripts import rebuild_lifecycle_truth

    position_id = "entry-complete-LABUSDT"
    report = build_exchange_truth_lifecycle(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "quantity": 1,
                    "long_venue": "bitget",
                    "short_venue": "okx",
                    "long_order_id": "open-long",
                    "short_order_id": "open-short",
                },
            ),
            _event(
                1_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "open",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "open-long",
                    "quantity": 1,
                    "price": 10,
                },
            ),
            _event(
                1_200,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "open",
                    "venue": "okx",
                    "leg": "short",
                    "order_id": "open-short",
                    "quantity": 1,
                    "price": 12,
                },
            ),
            _event(
                2_000,
                "exit.reconciled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "accounting_status": "complete",
                    "evidence_gap": False,
                    "pending_backfill": False,
                    "long_legs": [
                        {
                            "venue": "bitget",
                            "order_id": "close-long",
                            "quantity": 1,
                            "average_price": 11,
                        }
                    ],
                    "short_legs": [
                        {
                            "venue": "okx",
                            "order_id": "close-short",
                            "quantity": 1,
                            "average_price": 11.5,
                        }
                    ],
                },
            ),
        ]
    )

    class ExplodingAdapter:
        async def fetch_order_fill_reconciliation(self, *args, **kwargs):
            raise AssertionError("complete order identity must not be queried again")

    fill_events, summary = asyncio.run(
        rebuild_lifecycle_truth.query_exchange_fill_events(
            report,
            credential_loader=lambda venue: object(),
            adapter_factory=lambda venue, credential, rate_limiter=None: ExplodingAdapter(),
            rate_limiter_factory=lambda: None,
            install_runtime=lambda: None,
            restore_runtime=lambda previous: None,
        )
    )

    assert fill_events == []
    assert summary["candidate_count"] == 0
    assert summary["skipped_already_covered"] >= 2


def test_apply_validation_rejects_incomplete_or_query_error():
    from scripts import rebuild_lifecycle_truth

    report = {
        "summary": {
            "position_count": 2,
            LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value: 1,
            LifecycleClassification.PHANTOM_ZERO_QTY_OPENED.value: 0,
            LifecycleClassification.EXCHANGE_LIFECYCLE_INCOMPLETE.value: 1,
            LifecycleClassification.EVIDENCE_INCOMPLETE.value: 0,
        },
        "positions": {
            "entry-ok": {
                "classification": LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value,
                "source_coverage": {"gaps": []},
            },
            "entry-gap": {
                "classification": LifecycleClassification.EXCHANGE_LIFECYCLE_INCOMPLETE.value,
                "source_coverage": {"gaps": ["missing_short_close_exchange_fill_coverage"]},
            },
        },
        "exchange_query": {
            "errors": [{"position_id": "entry-gap", "error": "timeout"}],
            "credential_missing": 0,
            "adapter_unavailable": 0,
            "reconciliation_unavailable": 0,
        },
    }

    blockers = rebuild_lifecycle_truth.apply_report_blockers(
        report,
        position_ids=["entry-ok", "entry-gap"],
    )

    assert "exchange_query_errors_present" in blockers
    assert "exchange_lifecycle_incomplete:entry-gap" in blockers


def test_apply_validation_rejects_account_history_query_error():
    from scripts import rebuild_lifecycle_truth

    report = {
        "summary": {
            "position_count": 0,
            LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value: 0,
            LifecycleClassification.PHANTOM_ZERO_QTY_OPENED.value: 0,
            LifecycleClassification.EXCHANGE_LIFECYCLE_INCOMPLETE.value: 0,
            LifecycleClassification.EVIDENCE_INCOMPLETE.value: 0,
        },
        "positions": {},
        "exchange_query": {
            "errors": [],
            "account_history": {
                "enabled": True,
                "errors": [{"position_id": "entry-gap", "error": "Invalid time interval"}],
                "account_history_unavailable": 0,
            },
        },
    }

    blockers = rebuild_lifecycle_truth.apply_report_blockers(
        report,
        position_ids=["entry-gap"],
    )

    assert "exchange_query_account_history_errors_present" in blockers


def test_lifecycle_truth_rebuilt_correction_event_is_replayable():
    position_id = "entry-1782874583508-TAIKOUSDT"
    correction_truth = {
        "position_id": position_id,
        "symbol": "TAIKOUSDT",
        "long_venue": "bybit",
        "short_venue": "bitget",
        "target_quantity": "2",
        "open_legs": [
            {
                "phase": "open",
                "leg": "long",
                "venue": "bybit",
                "order_id": "long-open",
                "quantity": "2",
                "price": "0.811",
                "fee_quote": "0.01",
                "source": "rebuild_lifecycle_truth_exchange_query_open",
            },
            {
                "phase": "open",
                "leg": "short",
                "venue": "bitget",
                "order_id": "short-open",
                "quantity": "2",
                "price": "0.814",
                "fee_quote": "0.01",
                "source": "rebuild_lifecycle_truth_exchange_query_open",
            },
        ],
        "close_legs": [
            {
                "phase": "close",
                "leg": "long",
                "venue": "bybit",
                "order_id": "long-close",
                "quantity": "2",
                "price": "0.813",
                "fee_quote": "0.01",
                "source": "rebuild_lifecycle_truth_exchange_query_close",
            },
            {
                "phase": "close",
                "leg": "short",
                "venue": "bitget",
                "order_id": "short-close",
                "quantity": "2",
                "price": "0.812",
                "fee_quote": "0.01",
                "trade_side": "close",
                "source": "rebuild_lifecycle_truth_exchange_query_close",
            },
        ],
    }

    truth = _truth(
        [
            _event(
                1_000,
                "accounting.lifecycle_truth_rebuilt",
                {
                    "position_id": position_id,
                    "source": "rebuild_lifecycle_truth",
                    "classification": LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value,
                    "truth": correction_truth,
                },
            )
        ],
        position_id,
    )

    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["open_coverage"]["long"]["covered"] is True
    assert truth["open_coverage"]["short"]["covered"] is True
    assert truth["close_coverage"]["long"]["covered"] is True
    assert truth["close_coverage"]["short"]["covered"] is True


def test_duplicate_exchange_query_fill_does_not_double_count_close_or_pnl():
    position_id = "entry-dup-LABUSDT"
    events = [
        _event(
            1_000,
            "entry.opened",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "quantity": 1,
                "long_venue": "bitget",
                "short_venue": "okx",
            },
        ),
        _event(
            1_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "phase": "open",
                "venue": "bitget",
                "leg": "long",
                "order_id": "open-long",
                "quantity": 1,
                "price": 10,
                "fee_quote": 0.1,
            },
        ),
        _event(
            1_200,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "phase": "open",
                "venue": "okx",
                "leg": "short",
                "order_id": "open-short",
                "quantity": 1,
                "price": 12,
                "fee_quote": 0.1,
            },
        ),
        _event(
            2_000,
            "exit.reconciled",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "accounting_status": "complete",
                "evidence_gap": False,
                "pending_backfill": False,
                "long_legs": [
                    {
                        "venue": "bitget",
                        "order_id": "close-long",
                        "quantity": 1,
                        "average_price": 11,
                        "fee_quote": 0.05,
                    }
                ],
                "short_legs": [
                    {
                        "venue": "okx",
                        "order_id": "close-short",
                        "quantity": 1,
                        "average_price": 11.5,
                        "fee_quote": 0.05,
                    }
                ],
            },
        ),
        _event(
            2_100,
            "order.filled",
            {
                "position_id": position_id,
                "symbol": "LABUSDT",
                "phase": "close",
                "venue": "bitget",
                "leg": "long",
                "order_id": "close-long",
                "quantity": 1,
                "average_price": 11,
                "fee_quote": 0.05,
                "source": "rebuild_lifecycle_truth_exchange_query_close",
            },
        ),
    ]

    truth = _truth(events, position_id)

    assert truth["close_coverage"]["long"]["filled_qty"] == "1"
    assert truth["close_coverage"]["short"]["filled_qty"] == "1"
    assert truth["pnl"]["price_pnl_quote"] == "1.5"
    assert truth["pnl"]["exit_fee_quote"] == "-0.1"
    assert truth["pnl"]["net_pnl_quote"] == "1.2"


def test_pnl_uses_exchange_open_fills_not_local_entry_prices():
    position_id = "entry-open-truth-LABUSDT"
    truth = _truth(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "quantity": 2,
                    "long_venue": "bitget",
                    "short_venue": "okx",
                    "long_entry_price": 100,
                    "short_entry_price": 1,
                    "entry_fee_quote": "999",
                },
            ),
            _event(
                1_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "open",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "open-long",
                    "quantity": 2,
                    "average_price": 10,
                    "fee_quote": 0.1,
                },
            ),
            _event(
                1_200,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "open",
                    "venue": "okx",
                    "leg": "short",
                    "order_id": "open-short",
                    "quantity": 2,
                    "average_price": 12,
                    "fee_quote": -0.2,
                },
            ),
            _event(
                1_300,
                "funding.settled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "statement_id": "funding-1",
                    "funding_pnl_quote": 0.4,
                },
            ),
            _event(
                2_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "close",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "close-long",
                    "quantity": 2,
                    "average_price": 11,
                    "fee_quote": 0.05,
                },
            ),
            _event(
                2_200,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "close",
                    "venue": "okx",
                    "leg": "short",
                    "order_id": "close-short",
                    "quantity": 2,
                    "average_price": 11.5,
                    "fee_quote": 0.05,
                },
            ),
        ],
        position_id,
    )

    assert truth["classification"] == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    assert truth["open_coverage"]["long"]["average_price"] == "10"
    assert truth["open_coverage"]["short"]["average_price"] == "12"
    assert truth["pnl"]["price_pnl_quote"] == "3"
    assert truth["pnl"]["funding_pnl_quote"] == "0.4"
    assert truth["pnl"]["entry_fee_quote"] == "-0.3"
    assert truth["pnl"]["exit_fee_quote"] == "-0.1"
    assert truth["pnl"]["net_pnl_quote"] == "3"
    assert truth["pnl"]["notional_quote"] == "22"
    assert truth["pnl"]["net_pnl_bps"] == "1363.636363636363636363636364"


def test_funding_statement_and_exit_summary_are_not_double_counted():
    position_id = "entry-funding-dedupe-LABUSDT"
    truth = _truth(
        [
            _event(
                1_000,
                "entry.opened",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "quantity": 1,
                    "long_venue": "bitget",
                    "short_venue": "okx",
                    "captured_funding_quote": "0.4",
                },
            ),
            _event(
                1_100,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "open",
                    "venue": "bitget",
                    "leg": "long",
                    "order_id": "open-long",
                    "quantity": 1,
                    "price": 10,
                },
            ),
            _event(
                1_200,
                "order.filled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "phase": "open",
                    "venue": "okx",
                    "leg": "short",
                    "order_id": "open-short",
                    "quantity": 1,
                    "price": 12,
                },
            ),
            _event(
                1_500,
                "funding.settled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "venue": "bitget",
                    "statement_id": "funding-1",
                    "funding_pnl_quote": "0.4",
                },
            ),
            _event(
                2_000,
                "exit.reconciled",
                {
                    "position_id": position_id,
                    "symbol": "LABUSDT",
                    "accounting_status": "complete",
                    "evidence_gap": False,
                    "pending_backfill": False,
                    "funding_pnl_quote": "0.4",
                    "long_legs": [
                        {
                            "venue": "bitget",
                            "order_id": "close-long",
                            "quantity": 1,
                            "average_price": 11,
                        }
                    ],
                    "short_legs": [
                        {
                            "venue": "okx",
                            "order_id": "close-short",
                            "quantity": 1,
                            "average_price": 11.5,
                        }
                    ],
                },
            ),
        ],
        position_id,
    )

    assert truth["pnl"]["funding_pnl_quote"] == "0.4"
    assert "funding:bitget:statement_id:funding-1" in truth["pnl"]["evidence_refs"]
