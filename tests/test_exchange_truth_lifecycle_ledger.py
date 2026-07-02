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
