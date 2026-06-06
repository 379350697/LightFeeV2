from __future__ import annotations

from types import SimpleNamespace

from lightfee.engine.recovery_ledger import RecoveryLedger, RecoveryWorkItem
from lightfee.engine.recovery_owner_index import RecoveryOwnerIndex


def _has_blocking_work(ledger: RecoveryLedger) -> bool:
    return any(item.blocking for item in ledger.work_items)


def test_local_flat_plus_live_non_reduce_open_order_blocks_as_orphan_maker_order():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "open_positions": [],
            "pending_entries": [],
            "pending_residual_repairs": [],
        },
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "bybit",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "reduce_only": False,
                    "order_id": "live-order",
                }
            ],
        },
    )

    assert _has_blocking_work(ledger)
    assert [item.kind for item in ledger.work_items] == ["orphan_maker_order"]
    assert ledger.is_proven_flat("TRXUSDT") is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is False


def test_local_flat_plus_live_position_blocks_as_unpaired_live_position():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [
                {
                    "venue": "bybit",
                    "symbol": "SEIUSDT",
                    "side": "buy",
                    "quantity": 455.0,
                    "entry_price": 0.1887,
                }
            ],
            "open_orders": [],
        },
    )

    assert _has_blocking_work(ledger)
    assert [item.kind for item in ledger.work_items] == ["unpaired_live_position"]
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is False


def test_pending_entry_positive_maker_fill_blocks_as_owned_pending_entry():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "open_positions": [],
            "pending_entries": [
                {
                    "pending_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "maker_leg_filled": 455.0,
                    "hedge_leg_filled": 0.0,
                    "maker_order_id": "maker-order",
                }
            ],
            "pending_residual_repairs": [],
        },
        exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
    )

    assert _has_blocking_work(ledger)
    assert ledger.work_items[0].kind == "owned_pending_entry"
    assert ledger.contains_positive_fill_evidence("SEIUSDT")
    assert ledger.is_proven_flat("SEIUSDT") is False


def test_residual_repair_with_live_flat_records_repair_work_and_flat_decision():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "open_positions": [],
            "pending_entries": [],
            "pending_residual_repairs": [
                {
                    "repair_id": "repair-sei",
                    "symbol": "SEIUSDT",
                    "venue": "bybit",
                    "quantity": 455.0,
                }
            ],
        },
        exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
    )

    assert _has_blocking_work(ledger)
    assert ledger.work_items[0].kind == "pending_residual_repair"
    assert ledger.work_items[0].decision.outcome == "proven_flat"
    assert ledger.is_proven_flat("SEIUSDT") is False


def test_unavailable_exchange_truth_records_nonblocking_ambiguous_evidence_gap():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": False,
            "positions": [],
            "open_orders": [],
            "evidence": {"bybit": {"error": "timeout"}},
        },
    )

    assert _has_blocking_work(ledger) is False
    assert ledger.work_items[0].kind == "ambiguous_exchange_truth"
    assert ledger.work_items[0].blocking is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is True


def test_ambiguous_exchange_truth_kind_is_never_global_blocking():
    item = RecoveryWorkItem(kind="ambiguous_exchange_truth", blocking=True)

    assert item.blocks_all_new_entries is False


def test_recovery_ledger_collects_evidence_without_calling_decision_core():
    import lightfee.engine.recovery_ledger as recovery_ledger

    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": False,
            "positions": [],
            "open_orders": [],
            "probe_evidence": [{"venue": "bybit", "error": "timeout"}],
        },
    )

    assert not hasattr(recovery_ledger, "V1RecoveryDecisionCore")
    assert [item.kind for item in ledger.work_items] == ["ambiguous_exchange_truth"]
    assert ledger.work_items[0].blocking is False


def test_flat_no_local_work_unavailable_truth_is_nonblocking_evidence_gap():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "open_positions": [],
            "pending_entries": [],
            "pending_residual_repairs": [],
            "pending_passive_closes": [],
        },
        exchange_truth={
            "truth_available": False,
            "positions": [],
            "open_orders": [],
            "probe_evidence": [{"venue": "bybit", "error": "timeout"}],
        },
    )

    assert _has_blocking_work(ledger) is False
    assert [item.kind for item in ledger.work_items] == ["ambiguous_exchange_truth"]
    assert ledger.work_items[0].blocking is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is True


def test_local_work_unavailable_truth_remains_blocking():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "open_positions": [],
            "pending_entries": [{"pending_id": "entry-sei", "symbol": "SEIUSDT"}],
            "pending_residual_repairs": [],
        },
        exchange_truth={"truth_available": False, "positions": [], "open_orders": []},
    )

    assert _has_blocking_work(ledger)
    assert any(item.kind == "ambiguous_exchange_truth" for item in ledger.work_items)
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is True


def test_no_live_artifacts_and_no_local_work_is_proven_flat():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "open_positions": [],
            "pending_entries": [],
            "pending_residual_repairs": [],
        },
        exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
    )

    assert _has_blocking_work(ledger) is False
    assert ledger.is_proven_flat("SEIUSDT") is True
    assert ledger.allows_new_entry(SimpleNamespace(symbol="SEIUSDT")) is True


def test_unresolved_work_blocks_same_symbol_or_venue_overlap():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "pending_entries": [
                {
                    "pending_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "long_venue": "bybit",
                    "short_venue": "hyperliquid",
                }
            ]
        },
        exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
    )

    assert ledger.allows_new_entry(SimpleNamespace(symbol="SEIUSDT")) is False
    assert ledger.allows_new_entry(
        SimpleNamespace(symbol="BTCUSDT", long_venue="bybit", short_venue="okx")
    ) is True
    assert ledger.allows_new_entry(
        SimpleNamespace(symbol="BTCUSDT", long_venue="binance", short_venue="okx")
    ) is True


def test_unresolved_work_blocks_same_symbol_with_venue_overlap():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "pending_entries": [
                {
                    "pending_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "long_venue": "bybit",
                    "short_venue": "hyperliquid",
                }
            ]
        },
        exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
    )

    assert ledger.allows_new_entry(
        SimpleNamespace(symbol="SEIUSDT", long_venue="bybit", short_venue="okx")
    ) is False


def test_legacy_unavailable_exchange_truth_records_nonblocking_gap():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={"available": False, "positions": [], "open_orders": []},
    )

    assert _has_blocking_work(ledger) is False
    assert ledger.work_items[0].kind == "ambiguous_exchange_truth"
    assert ledger.work_items[0].blocking is False


def test_owned_non_reduce_maker_order_returns_owned_cancel_decision():
    owner_index = RecoveryOwnerIndex.from_state(
        {
            "pending_entries": [
                {
                    "pending_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "maker_order_id": "maker-order",
                    "maker_client_order_id": "maker-client",
                }
            ]
        }
    )
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "bybit",
                    "symbol": "SEIUSDT",
                    "side": "buy",
                    "quantity": 455.0,
                    "reduce_only": False,
                    "order_id": "maker-order",
                }
            ],
        },
        owner_index=owner_index,
    )

    item = ledger.work_items[0]
    assert item.kind == "owned_pending_entry"
    assert item.owner.confidence == "proven"
    assert item.decision.outcome == "owned_order_cancel_requested"


def test_orphan_maker_order_returns_fail_closed_operator_block():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "bybit",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "reduce_only": False,
                    "order_id": "orphan-maker",
                }
            ],
        },
    )

    item = ledger.work_items[0]
    assert item.kind == "orphan_maker_order"
    assert item.owner.confidence == "orphan"
    assert item.decision.outcome == "fail_closed_operator_block"


def test_orphan_reduce_only_order_is_cleanup_work_not_ignored():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "bybit",
                    "symbol": "TRXUSDT",
                    "side": "sell",
                    "quantity": 72.0,
                    "reduce_only": True,
                    "order_id": "orphan-reduce-only",
                }
            ],
        },
    )

    item = ledger.work_items[0]
    assert item.kind == "orphan_reduce_only_order"
    assert item.owner.confidence == "orphan"
    assert item.decision.outcome == "reduce_only_cleanup_submitted"
