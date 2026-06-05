from __future__ import annotations

from lightfee.engine.recovery_ledger import ExchangeArtifact
from lightfee.engine.recovery_owner_index import RecoveryOwnerIndex


def test_order_id_matches_pending_entry_owner():
    index = RecoveryOwnerIndex.from_state(
        {
            "pending_entries": [
                {
                    "pending_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "long_venue": "bybit",
                    "short_venue": "hyperliquid",
                    "maker_order_id": "maker-order",
                    "maker_client_order_id": "maker-client",
                }
            ]
        }
    )

    owner = index.owner_for_order(
        ExchangeArtifact(
            kind="open_order",
            venue="bybit",
            symbol="SEIUSDT",
            order_id="maker-order",
        )
    )

    assert owner.owner_type == "pending_entry"
    assert owner.owner_id == "entry-sei"
    assert owner.confidence == "proven"


def test_client_order_id_matches_pending_entry_owner():
    index = RecoveryOwnerIndex.from_state(
        {
            "pending_entries": [
                {
                    "pending_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "maker_order_id": "",
                    "maker_client_order_id": "maker-client",
                }
            ]
        }
    )

    owner = index.owner_for_order(
        ExchangeArtifact(
            kind="open_order",
            venue="bybit",
            symbol="SEIUSDT",
            client_order_id="maker-client",
        )
    )

    assert owner.owner_type == "pending_entry"
    assert owner.owner_id == "entry-sei"
    assert owner.confidence == "proven"


def test_live_position_matches_open_position_owner():
    index = RecoveryOwnerIndex.from_state(
        {
            "open_positions": [
                {
                    "position_id": "pos-sei",
                    "symbol": "SEIUSDT",
                    "long_venue": "bybit",
                    "short_venue": "hyperliquid",
                }
            ]
        }
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="SEIUSDT",
            quantity=455.0,
        )
    )

    assert owner.owner_type == "open_position"
    assert owner.owner_id == "pos-sei"
    assert owner.confidence == "proven"


def test_residual_repair_matches_symbol_and_repair_venue():
    index = RecoveryOwnerIndex.from_state(
        {
            "pending_residual_repairs": [
                {
                    "repair_id": "repair-sei",
                    "symbol": "SEIUSDT",
                    "venue": "bybit",
                }
            ]
        }
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="SEIUSDT",
            quantity=455.0,
        )
    )

    assert owner.owner_type == "residual_repair"
    assert owner.owner_id == "repair-sei"
    assert owner.confidence == "probable"


def test_journal_event_reconstructs_missing_pending_owner():
    index = RecoveryOwnerIndex.from_state_and_journal(
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

    owner = index.owner_for_order(
        ExchangeArtifact(
            kind="open_order",
            venue="bybit",
            symbol="SEIUSDT",
            order_id="journal-maker-order",
        )
    )

    assert owner.owner_type == "journal_pending_entry"
    assert owner.owner_id == "entry-sei"
    assert owner.confidence == "probable"


def test_terminal_journal_event_keeps_submitted_order_owner_fact():
    index = RecoveryOwnerIndex.from_state_and_journal(
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
            },
            {
                "kind": "pending_entry.pending_entry_finalized",
                "payload": {
                    "entry_id": "entry-sei",
                    "symbol": "SEIUSDT",
                    "position_id": None,
                },
            },
        ],
    )

    owner = index.owner_for_order(
        ExchangeArtifact(
            kind="open_order",
            venue="bybit",
            symbol="SEIUSDT",
            order_id="journal-maker-order",
        )
    )

    assert owner.owner_type == "journal_pending_entry"
    assert owner.owner_id == "entry-sei"
    assert owner.confidence == "probable"


def test_trxusdt_order_without_owner_remains_orphan():
    index = RecoveryOwnerIndex.from_state({"pending_entries": [], "open_positions": []})

    owner = index.owner_for_order(
        ExchangeArtifact(
            kind="open_order",
            venue="bybit",
            symbol="TRXUSDT",
            order_id="a84df707-efb3-4e40-bab1-641a4eb0f3d4",
        )
    )

    assert owner.owner_type == "exchange_order"
    assert owner.owner_id == "a84df707-efb3-4e40-bab1-641a4eb0f3d4"
    assert owner.confidence == "orphan"
