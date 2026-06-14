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


def test_local_baby_state_does_not_own_same_window_unpaired_bybit_positions():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "open_positions": [
                {
                    "position_id": "entry-1780771924982-BABYUSDT",
                    "symbol": "BABYUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                }
            ],
            "pending_entries": [],
        },
        exchange_truth={
            "truth_available": True,
            "positions": [
                {
                    "venue": "bybit",
                    "symbol": "MORPHOUSDT",
                    "side": "buy",
                    "quantity": 14.0,
                },
                {
                    "venue": "bybit",
                    "symbol": "MONUSDT",
                    "side": "buy",
                    "quantity": 1150.0,
                },
                {
                    "venue": "bybit",
                    "symbol": "SEIUSDT",
                    "side": "buy",
                    "quantity": 341.0,
                },
            ],
            "open_orders": [],
        },
    )

    items_by_symbol = {item.symbol: item for item in ledger.work_items}
    assert set(items_by_symbol) == {"MORPHOUSDT", "MONUSDT", "SEIUSDT"}
    assert {item.kind for item in items_by_symbol.values()} == {
        "unpaired_live_position"
    }
    assert all(item.blocking for item in items_by_symbol.values())
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


def test_pending_entry_owned_live_position_blocks_as_live_conflict_not_orphan():
    owner_index = RecoveryOwnerIndex.from_state(
        {
            "pending_entries": [
                {
                    "pending_id": "entry-home",
                    "symbol": "HOMEUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                    "maker_leg": "long",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                }
            ],
            "open_positions": [],
        }
    )
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "open_positions": [],
            "pending_entries": [
                {
                    "pending_id": "entry-home",
                    "symbol": "HOMEUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                    "maker_leg": "long",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                }
            ],
        },
        exchange_truth={
            "truth_available": True,
            "positions": [
                {
                    "venue": "bybit",
                    "symbol": "HOMEUSDT",
                    "side": "sell",
                    "quantity": 1600.0,
                }
            ],
            "open_orders": [],
        },
        owner_index=owner_index,
    )

    bybit_items = [
        item
        for item in ledger.work_items
        if item.symbol == "HOMEUSDT" and item.venues == frozenset({"bybit"})
    ]

    assert [item.kind for item in bybit_items] == ["owned_pending_entry_live_conflict"]
    assert bybit_items[0].owner.owner_type == "pending_entry"
    assert bybit_items[0].blocking is True
    assert bybit_items[0].blocks_all_new_entries is True
    assert bybit_items[0].decision.outcome == "pending_entry_live_conflict_requires_cleanup"
    assert not any(item.kind == "unpaired_live_position" for item in ledger.work_items)


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


def test_multi_symbol_owned_work_blocks_same_symbol_without_global_block():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={
            "pending_entries": [
                {
                    "pending_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "long_venue": "bybit",
                    "short_venue": "hyperliquid",
                },
                {
                    "pending_id": "entry-trx",
                    "symbol": "TRXUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                },
            ]
        },
        exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
    )

    items_by_symbol = {item.symbol: item for item in ledger.work_items}
    assert set(items_by_symbol) == {"SEIUSDT", "TRXUSDT"}
    assert all(item.kind == "owned_pending_entry" for item in ledger.work_items)
    assert all(item.owner.confidence == "proven" for item in ledger.work_items)
    assert all(item.blocks_all_new_entries is False for item in ledger.work_items)
    assert ledger.allows_new_entry(SimpleNamespace(symbol="SEIUSDT")) is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="TRXUSDT")) is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is True


def test_multi_symbol_orphan_maker_order_globally_blocks_while_owned_work_remains_visible():
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
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "okx",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "reduce_only": False,
                    "order_id": "orphan-trx-maker",
                }
            ],
        },
    )

    kinds = {item.kind for item in ledger.work_items}
    orphan = next(item for item in ledger.work_items if item.kind == "orphan_maker_order")
    owned = next(item for item in ledger.work_items if item.kind == "owned_pending_entry")
    assert kinds == {"owned_pending_entry", "orphan_maker_order"}
    assert orphan.owner.confidence == "orphan"
    assert orphan.blocks_all_new_entries is True
    assert owned.owner.confidence == "proven"
    assert owned.symbol == "SEIUSDT"
    assert ledger.allows_new_entry(SimpleNamespace(symbol="SEIUSDT")) is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is False


def test_partial_truth_with_local_pending_entry_records_owned_work_and_truth_gap():
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
        exchange_truth={
            "truth_available": False,
            "available": True,
            "positions": [],
            "open_orders": [],
            "probe_evidence": [
                {
                    "venue": "bybit",
                    "symbol": "SEIUSDT",
                    "classification": "position_probe_succeeded",
                },
                {
                    "venue": "okx",
                    "symbol": "SEIUSDT",
                    "classification": "open_order_probe_timeout",
                    "error": "exchange truth probe timed out after 2s",
                },
            ],
            "missing_evidence": ["okx:SEIUSDT:open_order_probe_timeout"],
        },
    )

    owned = next(item for item in ledger.work_items if item.kind == "owned_pending_entry")
    gap = next(item for item in ledger.work_items if item.kind == "ambiguous_exchange_truth")
    assert owned.symbol == "SEIUSDT"
    assert owned.owner.confidence == "proven"
    assert owned.blocking is True
    assert gap.blocking is False
    assert gap.decision.outcome == "evidence_gap_observed"
    assert ledger.is_proven_flat("SEIUSDT") is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="SEIUSDT")) is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is True


def test_journal_owned_order_is_owned_pending_entry_when_local_pending_is_absent():
    owner_index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [
            {
                "kind": "order.passive_submitted",
                "payload": {
                    "entry_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "venue": "bybit",
                    "order_id": "journal-maker-order",
                    "client_order_id": "journal-maker-client",
                },
            }
        ],
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
                    "order_id": "journal-maker-order",
                }
            ],
        },
        owner_index=owner_index,
    )

    item = ledger.work_items[0]
    assert item.kind == "owned_pending_entry"
    assert item.symbol == "SEIUSDT"
    assert item.owner.owner_type == "journal_pending_entry"
    assert item.owner.owner_id == "entry-sei"
    assert item.owner.confidence == "probable"
    assert item.blocks_all_new_entries is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="SEIUSDT")) is False
    assert ledger.allows_new_entry(SimpleNamespace(symbol="BTCUSDT")) is True


def test_journal_positive_fill_conflict_owns_live_position_after_pending_removed():
    owner_index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [
            {
                "kind": "pending_entry.positive_fill_live_truth_conflict",
                "payload": {
                    "entry_id": "entry-home",
                    "symbol": "HOMEUSDT",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                    "matched_quantity": 1600.0,
                    "live_long_quantity": 0.0,
                    "live_short_quantity": 1600.0,
                    "live_balanced_quantity": 0.0,
                },
            }
        ],
    )
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [
                {
                    "venue": "bybit",
                    "symbol": "HOMEUSDT",
                    "side": "sell",
                    "quantity": 1600.0,
                    "entry_price": 0.0285,
                }
            ],
            "open_orders": [],
        },
        owner_index=owner_index,
    )

    item = ledger.work_items[0]
    assert item.kind == "owned_pending_entry_live_conflict"
    assert item.owner.owner_type == "journal_pending_entry"
    assert item.owner.owner_id == "entry-home"
    assert item.owner.confidence == "probable"
    assert item.decision.outcome == "pending_entry_live_conflict_requires_cleanup"
    assert not any(item.kind == "unpaired_live_position" for item in ledger.work_items)
