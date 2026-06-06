from __future__ import annotations

from types import SimpleNamespace

from lightfee.engine.exchange_truth import (
    ExchangeTruthOpenOrder,
    ExchangeTruthPosition,
    ExchangeTruthSnapshot,
)
from lightfee.engine.recovery_decision_core import (
    RecoveryManagementAction,
    RecoveryDecisionKind,
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)


def test_flat_no_local_work_truth_gap_runs_with_evidence_gap_and_allows_entry():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(
            available=False,
            confidence="low",
            missing_evidence=("unsupported_symbol",),
        ),
        prior_recovery_block_reason=None,
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
    assert decision.entry_allowed is True
    assert decision.block_reason is None
    assert decision.clear_previous_block is True
    assert decision.evidence_quality == "partial_evidence_gap"


def test_local_recovery_work_plus_unavailable_truth_blocks_new_entry():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(),
        pending_entries=({"pending_id": "entry-sei", "symbol": "SEIUSDT"},),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(available=False, confidence="low"),
        prior_recovery_block_reason=None,
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH
    assert decision.entry_allowed is False
    assert decision.block_reason == "truth_unavailable_for_required_recovery"
    assert decision.entry_block_reason == "truth_unavailable_for_required_recovery"
    assert decision.management_action == RecoveryManagementAction.WAIT_FOR_TRUTH


def test_orphan_live_open_order_blocks_as_live_artifact():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(
            available=True,
            confidence="high",
            open_orders=(
                ExchangeTruthOpenOrder(
                    venue="bybit",
                    symbol="TRXUSDT",
                    side="buy",
                    quantity=72.0,
                    reduce_only=False,
                    order_id="live-maker",
                ),
            ),
        ),
        prior_recovery_block_reason=None,
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.BLOCK_OR_FLATTEN_LIVE_ARTIFACT
    assert decision.entry_allowed is False
    assert decision.block_reason == "orphan_maker_order"
    assert decision.management_action == RecoveryManagementAction.FLATTEN_OR_BLOCK_LIVE_ARTIFACT


def test_unpaired_live_position_blocks_as_live_artifact():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(
            available=True,
            confidence="high",
            positions=(
                ExchangeTruthPosition(
                    venue="bybit",
                    symbol="SEIUSDT",
                    side="buy",
                    quantity=455.0,
                    entry_price=0.1887,
                ),
            ),
        ),
        prior_recovery_block_reason=None,
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.BLOCK_OR_FLATTEN_LIVE_ARTIFACT
    assert decision.entry_allowed is False
    assert decision.block_reason == "unpaired_live_position"
    assert decision.management_action == RecoveryManagementAction.FLATTEN_OR_BLOCK_LIVE_ARTIFACT


def test_previous_ambiguous_block_clears_only_through_core():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(available=True, confidence="high"),
        prior_recovery_block_reason="exchange_truth_recovery_ledger_blocked",
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
    assert decision.clear_previous_block is True
    assert decision.clear_reason == "core_running_clean"
    assert decision.block_reason is None


def test_evidence_gap_does_not_clear_prior_live_artifact_block():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(available=False, confidence="low"),
        prior_recovery_block_reason="unpaired_live_position",
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
    assert decision.entry_allowed is True
    assert decision.block_reason is None
    assert decision.clear_previous_block is False


def test_nonblocking_ambiguous_evidence_item_does_not_require_truth():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        recovery_work_items=(
            SimpleNamespace(kind="ambiguous_exchange_truth", blocking=False),
        ),
        exchange_truth=ExchangeTruthSnapshot(available=False, confidence="low"),
        prior_recovery_block_reason="exchange_truth_recovery_ledger_blocked",
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
    assert decision.entry_allowed is True
    assert decision.clear_previous_block is True
    assert decision.block_reason is None


def test_managed_local_open_position_is_not_recovery_work_by_itself():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(
            SimpleNamespace(
                position_id="entry-aria",
                symbol="ARIAUSDT",
                long_venue="bybit",
                short_venue="binance",
            ),
        ),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(available=False, confidence="low"),
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
    assert decision.entry_allowed is True
    assert decision.block_reason is None


def test_local_open_position_marked_recovery_required_requires_truth():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(
            SimpleNamespace(
                position_id="entry-drift",
                symbol="DRIFTUSDT",
                long_venue="bybit",
                short_venue="binance",
                recovery_required=True,
            ),
        ),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(available=False, confidence="low"),
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH
    assert decision.entry_allowed is False
    assert decision.block_reason == "truth_unavailable_for_required_recovery"
    assert decision.clear_previous_block is False


def test_truth_required_owned_open_position_blocks_only_when_truth_unavailable():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(
            SimpleNamespace(
                position_id="entry-reconcile",
                symbol="RECONUSDT",
                long_venue="bybit",
                short_venue="binance",
            ),
        ),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        recovery_work_items=(
            SimpleNamespace(
                kind="owned_open_position",
                symbol="RECONUSDT",
                blocking=False,
                requires_truth=True,
            ),
        ),
        exchange_truth=ExchangeTruthSnapshot(available=False, confidence="low"),
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH
    assert decision.entry_allowed is False
    assert decision.block_reason == "truth_unavailable_for_required_recovery"


def test_managed_local_open_position_owns_matching_live_position():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(
            SimpleNamespace(
                position_id="entry-aria",
                symbol="ARIAUSDT",
                long_venue="bybit",
                short_venue="binance",
            ),
        ),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(
            available=True,
            confidence="high",
            positions=(
                ExchangeTruthPosition(
                    venue="bybit",
                    symbol="ARIAUSDT",
                    side="buy",
                    quantity=619.0,
                ),
            ),
        ),
        prior_recovery_block_reason="position_drift_correction_failed",
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RUNNING_CLEAN
    assert decision.block_reason is None


def test_same_symbol_live_position_with_unmatched_owner_evidence_still_blocks():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=(
            SimpleNamespace(
                position_id="entry-foo",
                symbol="FOOUSDT",
                long_venue="okx",
                long_size=1.0,
            ),
        ),
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        exchange_truth=ExchangeTruthSnapshot(
            available=True,
            confidence="high",
            positions=(
                ExchangeTruthPosition(
                    venue="bybit",
                    symbol="FOOUSDT",
                    side="buy",
                    quantity=99.0,
                ),
            ),
        ),
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.BLOCK_OR_FLATTEN_LIVE_ARTIFACT
    assert decision.block_reason == "unpaired_live_position"


def test_same_symbol_orphan_maker_order_is_not_owned_by_unrelated_work():
    snapshot = RecoveryEvidenceSnapshot(
        pending_entries=(),
        residual_repairs=(),
        passive_closes=(),
        recovery_work_items=(
            SimpleNamespace(kind="owned_pending_entry", symbol="FOOUSDT", blocking=True),
        ),
        exchange_truth=ExchangeTruthSnapshot(
            available=True,
            confidence="high",
            open_orders=(
                ExchangeTruthOpenOrder(
                    venue="bybit",
                    symbol="FOOUSDT",
                    side="buy",
                    quantity=1.0,
                    reduce_only=False,
                    order_id="orphan-maker",
                ),
            ),
        ),
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.BLOCK_OR_FLATTEN_LIVE_ARTIFACT
    assert decision.block_reason == "orphan_maker_order"
