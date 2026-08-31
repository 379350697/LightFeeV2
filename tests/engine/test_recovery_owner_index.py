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


def test_okx_wire_symbol_matches_canonical_local_open_position_owner():
    """Recovery ownership compares exchange wire symbols in canonical space."""
    index = RecoveryOwnerIndex.from_state(
        {
            "open_positions": [
                {
                    "position_id": "pos-home",
                    "symbol": "HOMEUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                }
            ]
        }
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="okx",
            symbol="HOME-USDT-SWAP",
            quantity=1600.0,
        )
    )

    assert owner.owner_type == "open_position"
    assert owner.owner_id == "pos-home"
    assert owner.confidence == "proven"


def test_positive_fill_pending_entry_owns_expected_live_position():
    index = RecoveryOwnerIndex.from_state(
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

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="HOMEUSDT",
            side="sell",
            quantity=1600.0,
        )
    )

    assert owner.owner_type == "pending_entry"
    assert owner.owner_id == "entry-home"
    assert owner.confidence == "proven"
    assert owner.evidence["source"] == "local_pending_entry"
    assert owner.evidence["position_scope"] == "positive_fill_pending_entry"


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


def test_v1_terminal_pending_removal_retires_journal_order_owner():
    events = [
        {
            "kind": "order.passive_submitted",
            "payload": {
                "entry_id": "entry-cleared",
                "symbol": "SEIUSDT",
                "venue": "bybit",
                "order_id": "historical-order",
                "client_order_id": "historical-client",
            },
        },
        {
            "kind": "pending_entry.removed_by_v1_lifecycle_closure",
            "payload": {
                "entry_id": "entry-cleared",
                "owner_id": "entry-cleared",
            },
        },
    ]

    assert RecoveryOwnerIndex.active_journal_owner_events(events) == []
    owner = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []}, events
    ).owner_for_order(
        ExchangeArtifact(
            kind="open_order",
            venue="bybit",
            symbol="SEIUSDT",
            order_id="historical-order",
        )
    )

    assert owner.confidence == "orphan"


def test_terminal_close_keeps_order_owner_for_accounting_reconciliation():
    """A flat position can still need its exact order/fee evidence."""
    events = [
        {
            "kind": "order.passive_submitted",
            "payload": {
                "entry_id": "position-accounting",
                "position_id": "position-accounting",
                "symbol": "SEIUSDT",
                "venue": "bybit",
                "order_id": "historical-close-order",
            },
        },
        {
            "kind": "exit.billing_evidence_unavailable",
            "payload": {"position_id": "position-accounting"},
        },
    ]

    owner = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []}, events
    ).owner_for_order(
        ExchangeArtifact(
            kind="open_order",
            venue="bybit",
            symbol="SEIUSDT",
            order_id="historical-close-order",
        )
    )

    assert owner.owner_type == "journal_pending_entry"
    assert owner.owner_id == "position-accounting"
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


def test_positive_fill_journal_without_per_leg_venue_does_not_claim_live_position():
    index = RecoveryOwnerIndex.from_state_and_journal(
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

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="HOMEUSDT",
            side="sell",
            quantity=1600.0,
        )
    )

    assert owner.owner_type == "exchange_position"
    assert owner.confidence == "orphan"


def test_unhanded_entry_submission_claim_owns_matching_live_position():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [{
            "kind": "runtime.entry_owner_claimed",
            "payload": {
                "entry_id": "entry-handoff",
                "symbol": "HOMEUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "long_side": "buy",
                "short_side": "sell",
                "long_quantity": 1600.0,
                "short_quantity": 1600.0,
            },
        }],
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="HOMEUSDT",
            side="sell",
            quantity=1600.0,
        )
    )

    assert owner.owner_type == "journal_entry_submission"
    assert owner.owner_id == "entry-handoff"
    assert owner.confidence == "probable"
    assert owner.evidence["position_scope"] == "journal_entry_submission"


def test_unhanded_entry_submission_claim_allows_one_percent_quantity_step_delta():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [{
            "kind": "runtime.entry_owner_claimed",
            "payload": {
                "entry_id": "entry-2z",
                "symbol": "2ZUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "long_side": "buy",
                "short_side": "sell",
                "long_quantity": 462.696,
                "short_quantity": 462.696,
            },
        }],
    )

    normalized_owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="2ZUSDT",
            side="sell",
            quantity=462.0,
        )
    )
    boundary_owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="2ZUSDT",
            side="sell",
            quantity=462.696 * 0.99,
        )
    )
    outside_owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="2ZUSDT",
            side="sell",
            quantity=462.696 * 0.9899,
        )
    )

    assert normalized_owner.owner_type == "journal_entry_submission"
    assert boundary_owner.owner_type == "journal_entry_submission"
    assert outside_owner.confidence == "orphan"


def test_short_maker_claim_uses_canonical_long_buy_short_sell_positions():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [{
            "kind": "runtime.entry_owner_claimed",
            "payload": {
                "entry_id": "entry-short-maker",
                "symbol": "HOMEUSDT",
                "long_venue": "okx",
                "short_venue": "bybit",
                "long_side": "buy",
                "short_side": "sell",
                "long_quantity": 1600.0,
                "short_quantity": 1600.0,
            },
        }],
    )

    long_owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="okx",
            symbol="HOMEUSDT",
            side="buy",
            quantity=1600.0,
        )
    )
    short_owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="HOMEUSDT",
            side="sell",
            quantity=1600.0,
        )
    )

    assert long_owner.owner_type == "journal_entry_submission"
    assert short_owner.owner_type == "journal_entry_submission"


def test_pending_handoff_without_durable_successor_keeps_pre_submit_claim():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [
            {
                "kind": "runtime.entry_owner_claimed",
                "payload": {
                    "entry_id": "entry-crash-window",
                    "symbol": "HOMEUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "long_side": "buy",
                    "short_side": "sell",
                    "long_quantity": 1600.0,
                    "short_quantity": 1600.0,
                },
            },
            {
                "kind": "runtime.entry_owner_handoff_complete",
                "payload": {
                    "entry_id": "entry-crash-window",
                    "owner_destination": "pending_entry",
                },
            },
        ],
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="HOMEUSDT",
            side="sell",
            quantity=1600.0,
        )
    )

    assert owner.owner_type == "journal_entry_submission"


def test_pending_handoff_with_durable_successor_retires_pre_submit_claim():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [
            {
                "kind": "runtime.entry_owner_claimed",
                "payload": {
                    "entry_id": "entry-complete-pending",
                    "symbol": "HOMEUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "long_side": "buy",
                    "short_side": "sell",
                    "long_quantity": 1600.0,
                    "short_quantity": 1600.0,
                },
            },
            {
                "kind": "entry.pending_registered",
                "payload": {
                    "entry_id": "entry-complete-pending",
                    "pending_id": "entry-complete-pending",
                    "symbol": "HOMEUSDT",
                },
            },
            {
                "kind": "runtime.entry_owner_handoff_complete",
                "payload": {
                    "entry_id": "entry-complete-pending",
                    "owner_destination": "pending_entry",
                },
            },
        ],
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="HOMEUSDT",
            side="sell",
            quantity=1600.0,
        )
    )

    assert owner.confidence == "orphan"


def test_unhanded_entry_submission_claim_owns_both_deterministic_leg_cids():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [{
            "kind": "runtime.entry_owner_claimed",
            "payload": {
                "entry_id": "entry-cids",
                "symbol": "HOMEUSDT",
                "maker_client_order_id": "maker-cid",
                "hedge_client_order_id": "hedge-cid",
            },
        }],
    )

    for client_order_id in ("maker-cid", "hedge-cid"):
        owner = index.owner_for_order(
            ExchangeArtifact(kind="order", client_order_id=client_order_id)
        )
        assert owner.owner_type == "journal_entry_submission"
        assert owner.owner_id == "entry-cids"


def test_entry_submission_claim_does_not_own_same_symbol_position_on_other_venue():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [{
            "kind": "runtime.entry_owner_claimed",
            "payload": {
                "entry_id": "entry-handoff",
                "symbol": "HOMEUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "long_side": "buy",
                "short_side": "sell",
                "long_quantity": 1600.0,
                "short_quantity": 1600.0,
            },
        }],
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="okx",
            symbol="HOMEUSDT",
            side="sell",
            quantity=1600.0,
        )
    )

    assert owner.owner_type == "exchange_position"
    assert owner.confidence == "orphan"


def test_completed_entry_submission_claim_does_not_mask_an_orphan_position():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [
            {
                "kind": "runtime.entry_owner_claimed",
                "payload": {
                    "entry_id": "entry-complete",
                    "symbol": "HOMEUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "long_side": "buy",
                    "short_side": "sell",
                    "long_quantity": 1600.0,
                    "short_quantity": 1600.0,
                },
            },
            {
                "kind": "runtime.entry_owner_handoff_complete",
                "payload": {"entry_id": "entry-complete"},
            },
        ],
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="HOMEUSDT",
            side="sell",
            quantity=1600.0,
        )
    )

    assert owner.owner_type == "exchange_position"
    assert owner.confidence == "orphan"


def test_positive_fill_live_conflict_journal_does_not_own_mismatched_position():
    index = RecoveryOwnerIndex.from_state_and_journal(
        {"pending_entries": [], "open_positions": []},
        [
            {
                "kind": "pending_entry.positive_fill_live_truth_conflict",
                "payload": {
                    "entry_id": "entry-home",
                    "symbol": "HOMEUSDT",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                    "live_short_quantity": 1600.0,
                },
            }
        ],
    )

    owner = index.owner_for_position(
        ExchangeArtifact(
            kind="position",
            venue="bybit",
            symbol="HOMEUSDT",
            side="buy",
            quantity=1600.0,
        )
    )

    assert owner.owner_type == "exchange_position"
    assert owner.confidence == "orphan"


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
