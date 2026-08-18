"""Regression coverage for audited historical close-accounting evidence imports."""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from lightfee.core.domain import Venue
from lightfee.engine.close_runtime import CloseRuntime
from lightfee.engine.recovery import (
    _apply_journal_replay_to_state,
    _restore_state_from_snapshot_dict,
    build_persistent_state_view,
)
from lightfee.engine.state import (
    BillingEvidenceImportError,
    EngineState,
    pending_close_reconciliation_import_reason,
)
from lightfee.ops.commands import (
    discover_binance_close_evidence_candidates,
    execute_billing_evidence_import,
)
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.persistence.writer_lease import (
    PersistenceWriterLease,
    PersistenceWriterLeaseError,
)


POSITION_ID = "historical-close-owner"
SYMBOL = "HOMEUSDT"
CLOSED_AT_MS = 1_700_000_000_000


def _snapshot() -> dict[str, object]:
    return {
        "position_id": POSITION_ID,
        "symbol": SYMBOL,
        "long_venue": "bybit",
        "short_venue": "okx",
        "long_quantity": 2.0,
        "short_quantity": 2.0,
        "matched_quantity": 2.0,
        "long_entry_price": 10.0,
        "short_entry_price": 11.0,
        "long_entry_fee_quote": 0.01,
        "short_entry_fee_quote": 0.02,
        "total_entry_fee_quote": 0.03,
        "entry_fee_evidence_complete": True,
        "captured_funding_quote": 0.0,
        "opened_at_ms": CLOSED_AT_MS - 10_000,
    }


def _legs() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    return (
        [{
            "venue": "bybit",
            "order_id": "long-close-order",
            "client_order_id": "long-close-client",
            "quantity": 2.0,
            "average_price": 12.0,
            "fee_quote": 0.02,
        }],
        [{
            "venue": "okx",
            "order_id": "short-close-order",
            "client_order_id": "short-close-client",
            "quantity": 2.0,
            "average_price": 9.0,
            "fee_quote": 0.02,
        }],
    )


def _debt(*, snapshot: dict[str, object] | None, with_legs: bool) -> dict[str, object]:
    long_legs, short_legs = _legs() if with_legs else ([], [])
    return {
        "position_id": POSITION_ID,
        "symbol": SYMBOL,
        "kind": "final",
        "source": "historical_live_flat_cleanup",
        "closed_at_ms": CLOSED_AT_MS,
        "position_snapshot": snapshot or {},
        "long_legs": long_legs,
        "short_legs": short_legs,
        "reconciliation_mode": "venue_execution_history_required",
        "reconciliation_status": "evidence_debt",
        "evidence_debt_reason": (
            "missing_close_order_identity" if snapshot is not None else "missing_position_snapshot"
        ),
        "billing_reconciliation_required": True,
    }


def _evidence(
    *,
    snapshot: dict[str, object] | None = None,
    include_legs: bool = False,
) -> dict[str, object]:
    reconciliation: dict[str, object] = {
        "position_id": POSITION_ID,
        "kind": "final",
        "closed_at_ms": CLOSED_AT_MS,
    }
    if snapshot is not None:
        reconciliation["position_snapshot"] = snapshot
    if include_legs:
        long_legs, short_legs = _legs()
        reconciliation["long_legs"] = long_legs
        reconciliation["short_legs"] = short_legs
    return {
        "schema_version": 1,
        "evidence_reference": "exchange-export:case-20260809-01",
        "reconciliation": reconciliation,
    }


def _binance_close_debt() -> dict[str, object]:
    snapshot = _snapshot()
    snapshot.update(
        {
            "symbol": "COWUSDT",
            "long_venue": "binance",
            "short_venue": "bybit",
            "long_quantity": 100.0,
            "short_quantity": 100.0,
            "matched_quantity": 100.0,
        }
    )
    return {
        "position_id": "missing-binance-close-owner",
        "symbol": "COWUSDT",
        "kind": "partial",
        "closed_at_ms": CLOSED_AT_MS,
        "position_snapshot": snapshot,
        "long_legs": [],
        "short_legs": [
            {
                "venue": "bybit",
                "order_id": "known-bybit-close",
                "client_order_id": "known-bybit-client",
                "quantity": 100.0,
                "average_price": 1.0,
                "fee_quote": 0.01,
            }
        ],
        "reconciliation_status": "evidence_debt",
        "evidence_debt_reason": "missing_close_order_identity",
    }


def _binance_order(**overrides: object) -> dict[str, object]:
    order: dict[str, object] = {
        "symbol": "COWUSDT",
        "side": "SELL",
        "reduceOnly": True,
        "status": "FILLED",
        "executedQty": "100",
        "origQty": "100",
        "avgPrice": "1.25",
        "orderId": "123456",
        "clientOrderId": "lfx-historical-cow-close",
        "updateTime": CLOSED_AT_MS + 1_000,
    }
    order.update(overrides)
    return order


def test_binance_close_evidence_discovery_returns_read_only_unique_candidate():
    result = discover_binance_close_evidence_candidates(
        _binance_close_debt(),
        [_binance_order()],
        time_window_ms=5_000,
    )

    assert result["candidate_discovery_only"] is True
    assert result["automatically_importable"] is False
    assert result["owner"] == {
        "position_id": "missing-binance-close-owner",
        "kind": "partial",
        "closed_at_ms": CLOSED_AT_MS,
    }
    leg = result["legs"][0]
    assert leg["leg"] == "long"
    assert leg["expected_quantity_source"] == "opposite_identified_close_leg_quantity"
    assert leg["disposition"] == "unique_candidate_requires_operator_evidence"
    assert leg["candidate_count"] == 1
    assert leg["candidates"][0]["order_id"] == "123456"
    assert leg["candidates"][0]["system_client_order_id"] is True


@pytest.mark.parametrize(
    "overrides",
    [
        {"symbol": "OTHERUSDT"},
        {"side": "BUY"},
        {"reduceOnly": False},
        {"executedQty": "0"},
        {"executedQty": "99"},
        {"updateTime": CLOSED_AT_MS + 5_001},
        {"orderId": "", "clientOrderId": ""},
    ],
)
def test_binance_close_evidence_discovery_rejects_non_proof_candidates(overrides):
    result = discover_binance_close_evidence_candidates(
        _binance_close_debt(),
        [_binance_order(**overrides)],
        time_window_ms=5_000,
    )

    leg = result["legs"][0]
    assert leg["candidate_count"] == 0
    assert leg["disposition"] == "no_candidate"
    assert result["automatically_importable"] is False


def test_binance_close_evidence_discovery_keeps_multiple_matches_ambiguous():
    result = discover_binance_close_evidence_candidates(
        _binance_close_debt(),
        [_binance_order(orderId="1"), _binance_order(orderId="2")],
        time_window_ms=5_000,
    )

    leg = result["legs"][0]
    assert leg["candidate_count"] == 2
    assert leg["disposition"] == "ambiguous_candidates"
    assert result["automatically_importable"] is False


def test_binance_close_evidence_discovery_does_not_use_partial_open_quantity():
    debt = _binance_close_debt()
    debt["short_legs"][0]["quantity"] = 0.0

    result = discover_binance_close_evidence_candidates(
        debt,
        [_binance_order()],
        time_window_ms=5_000,
    )

    leg = result["legs"][0]
    assert leg["expected_quantity"] is None
    assert leg["expected_quantity_source"] == "missing_close_quantity"
    assert leg["disposition"] == "missing_expected_quantity"


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"time_window_ms": float("nan")}, "time_window_ms"),
        ({"time_window_ms": 1.5}, "time_window_ms"),
        ({"quantity_relative_tolerance": float("nan")}, "quantity_relative_tolerance"),
    ],
)
def test_binance_close_evidence_discovery_rejects_invalid_matching_bounds(kwargs, message):
    with pytest.raises(ValueError, match=message):
        discover_binance_close_evidence_candidates(
            _binance_close_debt(),
            [_binance_order()],
            **kwargs,
        )


def test_import_replaces_missing_snapshot_debt_and_replays_durably(tmp_path):
    state = EngineState()
    state.pending_close_reconciliations = [_debt(snapshot=None, with_legs=True)]
    evidence = _evidence(snapshot=_snapshot())

    journal = Journal(tmp_path / "journal.jsonl")
    journal.open()
    try:
        message = execute_billing_evidence_import(
            evidence,
            journal=journal,
            state=state,
            now_ms=CLOSED_AT_MS + 1,
        )
        records = journal.read_all()
    finally:
        journal.close()

    assert "awaiting exact exchange fill reconciliation" in message
    imported = state.pending_close_reconciliations[0]
    assert imported["reconciliation_status"] == "operator_evidence_imported"
    assert pending_close_reconciliation_import_reason(imported) is None
    assert imported["operator_evidence"]["reference"] == evidence["evidence_reference"]
    assert records[-1]["kind"] == "exit.billing_evidence_imported"
    assert len(records[-1]["payload"]["evidence_sha256"]) == 64

    replayed = EngineState()
    replayed.pending_close_reconciliations = [_debt(snapshot=None, with_legs=True)]
    _apply_journal_replay_to_state(replayed, records)
    assert replayed.pending_close_reconciliations == [imported]

    # Replaying the durable event twice is idempotent and cannot duplicate work.
    _apply_journal_replay_to_state(replayed, records)
    assert replayed.pending_close_reconciliations == [imported]


def test_import_replaces_missing_order_identity_without_rewriting_snapshot():
    state = EngineState()
    original_snapshot = _snapshot()
    state.pending_close_reconciliations = [_debt(snapshot=original_snapshot, with_legs=False)]
    evidence = _evidence(include_legs=True)

    message = execute_billing_evidence_import(
        evidence,
        journal=_JournalRecorder(),
        state=state,
        now_ms=CLOSED_AT_MS + 1,
    )

    imported = state.pending_close_reconciliations[0]
    assert "awaiting" in message
    assert imported["position_snapshot"] == original_snapshot
    assert imported["reconciliation_status"] == "operator_evidence_imported"
    assert pending_close_reconciliation_import_reason(imported) is None


def test_import_rejects_wrong_owner_and_keeps_debt_unchanged():
    state = EngineState()
    debt = _debt(snapshot=None, with_legs=True)
    state.pending_close_reconciliations = [debt]
    evidence = _evidence(snapshot=_snapshot())
    evidence["reconciliation"]["closed_at_ms"] = CLOSED_AT_MS + 1

    with pytest.raises(BillingEvidenceImportError, match="exactly one"):
        execute_billing_evidence_import(
            evidence,
            journal=_JournalRecorder(),
            state=state,
            now_ms=CLOSED_AT_MS + 1,
        )

    assert state.pending_close_reconciliations == [debt]


def test_import_rejects_provisional_entry_fee_evidence_and_keeps_debt():
    state = EngineState()
    debt = _debt(snapshot=None, with_legs=True)
    state.pending_close_reconciliations = [debt]
    snapshot = _snapshot()
    snapshot["entry_fee_evidence_complete"] = False

    with pytest.raises(BillingEvidenceImportError, match="entry_fee_evidence_incomplete"):
        execute_billing_evidence_import(
            _evidence(snapshot=snapshot),
            journal=_JournalRecorder(),
            state=state,
            now_ms=CLOSED_AT_MS + 1,
        )

    assert state.pending_close_reconciliations == [debt]


def test_ops_cli_requires_apply_and_persists_audited_import(tmp_path, monkeypatch):
    from lightfee.apps import ops

    state = EngineState()
    state.pending_close_reconciliations = [_debt(snapshot=None, with_legs=True)]
    event_log_path = tmp_path / "live-events.jsonl"
    snapshot_path = tmp_path / "live-state.json"
    store = SnapshotStore(snapshot_path)
    store.write(build_persistent_state_view(state))
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(_evidence(snapshot=_snapshot())), encoding="utf-8")
    monkeypatch.setenv("LIGHTFEE_DATA_DIR", str(tmp_path))

    monkeypatch.setattr(sys, "argv", ["lightfee-ops", "import-billing-evidence", "--file", str(evidence_path)])
    with pytest.raises(SystemExit) as rejected:
        ops.main()
    assert rejected.value.code == 2
    assert _restore_state_from_snapshot_dict(store.read() or {}).pending_close_reconciliations == [
        _debt(snapshot=None, with_legs=True)
    ]

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lightfee-ops",
            "import-billing-evidence",
            "--file",
            str(evidence_path),
            "--apply",
            "--event-log-path",
            str(event_log_path),
            "--snapshot-path",
            str(snapshot_path),
        ],
    )
    with pytest.raises(SystemExit) as applied:
        ops.main()
    assert applied.value.code == 0
    persisted = _restore_state_from_snapshot_dict(store.read() or {})
    assert persisted.pending_close_reconciliations[0]["reconciliation_status"] == "operator_evidence_imported"
    assert Journal(event_log_path).read_all()[-1]["kind"] == "exit.billing_evidence_imported"


def test_ops_paths_require_one_explicit_persistence_pair(tmp_path):
    from lightfee.apps.ops import _resolve_paths

    with pytest.raises(ValueError, match="must be supplied together"):
        _resolve_paths(event_log_path=tmp_path / "events.jsonl")


def test_ops_cli_discovers_binance_candidate_without_writer_or_persistence_mutation(
    tmp_path,
    monkeypatch,
    capsys,
):
    from lightfee.apps import ops

    state = EngineState()
    state.pending_close_reconciliations = [_binance_close_debt()]
    snapshot_path = tmp_path / "live-state.json"
    SnapshotStore(snapshot_path).write(build_persistent_state_view(state))
    snapshot_before = snapshot_path.read_bytes()
    orders_path = tmp_path / "binance-all-orders.json"
    orders_path.write_text(json.dumps([_binance_order()]), encoding="utf-8")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lightfee-ops",
            "discover-binance-close-evidence",
            "--snapshot-path",
            str(snapshot_path),
            "--orders-file",
            str(orders_path),
            "--position-id",
            "other-owner",
            "--kind",
            "partial",
            "--closed-at-ms",
            str(CLOSED_AT_MS),
        ],
    )
    with pytest.raises(SystemExit) as rejected:
        ops.main()
    assert rejected.value.code == 2
    assert snapshot_path.read_bytes() == snapshot_before

    event_log_path = tmp_path / "live-events.jsonl"
    live_writer = PersistenceWriterLease(event_log_path)
    live_writer.acquire()
    try:
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "lightfee-ops",
                "discover-binance-close-evidence",
                "--snapshot-path",
                str(snapshot_path),
                "--orders-file",
                str(orders_path),
                "--position-id",
                "missing-binance-close-owner",
                "--kind",
                "partial",
                "--closed-at-ms",
                str(CLOSED_AT_MS),
            ],
        )
        with pytest.raises(SystemExit) as completed:
            ops.main()
    finally:
        live_writer.release()

    assert completed.value.code == 0
    result = json.loads(capsys.readouterr().out)
    assert result["candidate_discovery_only"] is True
    assert result["automatically_importable"] is False
    assert result["legs"][0]["disposition"] == "unique_candidate_requires_operator_evidence"
    assert snapshot_path.read_bytes() == snapshot_before
    assert not event_log_path.exists()


def test_ops_cli_refuses_import_while_live_writer_lease_is_held(tmp_path, monkeypatch):
    from lightfee.apps import ops

    state = EngineState()
    debt = _debt(snapshot=None, with_legs=True)
    state.pending_close_reconciliations = [debt]
    event_log_path = tmp_path / "live-events.jsonl"
    snapshot_path = tmp_path / "live-state.json"
    SnapshotStore(snapshot_path).write(build_persistent_state_view(state))
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(_evidence(snapshot=_snapshot())), encoding="utf-8")

    live_writer = PersistenceWriterLease(event_log_path)
    live_writer.acquire()
    try:
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "lightfee-ops",
                "import-billing-evidence",
                "--file",
                str(evidence_path),
                "--apply",
                "--event-log-path",
                str(event_log_path),
                "--snapshot-path",
                str(snapshot_path),
            ],
        )
        with pytest.raises(SystemExit) as rejected:
            ops.main()
    finally:
        live_writer.release()

    assert rejected.value.code == 2
    restored = _restore_state_from_snapshot_dict(SnapshotStore(snapshot_path).read() or {})
    assert restored.pending_close_reconciliations == [debt]
    assert not event_log_path.exists()


@pytest.mark.parametrize(
    "command",
    (
        "pause-entry",
        "reduce-only",
        "fail-closed",
        "reconcile-now",
        "resume-if-safe",
    ),
)
def test_ops_cli_refuses_every_control_mutation_while_live_writer_lease_is_held(
    tmp_path,
    monkeypatch,
    command,
):
    from lightfee.apps import ops

    event_log_path = tmp_path / "journal.jsonl"
    snapshot_path = tmp_path / "snapshot.json"
    persisted_view = build_persistent_state_view(EngineState())
    SnapshotStore(snapshot_path).write(persisted_view)
    monkeypatch.setenv("LIGHTFEE_DATA_DIR", str(tmp_path))

    live_writer = PersistenceWriterLease(event_log_path)
    live_writer.acquire()
    try:
        monkeypatch.setattr(sys, "argv", ["lightfee-ops", command])
        with pytest.raises(SystemExit) as rejected:
            ops.main()
    finally:
        live_writer.release()

    assert rejected.value.code == 2
    assert SnapshotStore(snapshot_path).read() == persisted_view
    assert not event_log_path.exists()


@pytest.mark.asyncio
async def test_live_entrypoint_refuses_same_persistence_pair_when_import_owns_lease(
    tmp_path,
    monkeypatch,
):
    from lightfee.apps import live

    event_log_path = tmp_path / "live-events.jsonl"
    config = SimpleNamespace(
        persistence=SimpleNamespace(event_log_path=str(event_log_path))
    )
    monkeypatch.setattr(live, "load_config", lambda _path: config)

    import_writer = PersistenceWriterLease(event_log_path)
    import_writer.acquire()
    try:
        with pytest.raises(PersistenceWriterLeaseError, match="writer is active"):
            await live.async_main("ignored-config.toml")
    finally:
        import_writer.release()


@pytest.mark.asyncio
async def test_live_entrypoint_releases_writer_lease_after_shutdown(tmp_path, monkeypatch):
    from lightfee.apps import live

    event_log_path = tmp_path / "live-events.jsonl"
    config = SimpleNamespace(
        persistence=SimpleNamespace(event_log_path=str(event_log_path))
    )
    monkeypatch.setattr(live, "load_config", lambda _path: config)

    async def completed_live_run(*_args, **_kwargs):
        return None

    monkeypatch.setattr(live, "_async_main_with_writer_lease", completed_live_run)
    await live.async_main("ignored-config.toml")

    next_writer = PersistenceWriterLease(event_log_path)
    next_writer.acquire()
    next_writer.release()


@pytest.mark.asyncio
async def test_imported_evidence_uses_exact_order_ids_before_terminal_reconciliation():
    state = EngineState()
    state.tick_count = 1
    state.pending_close_reconciliations = [_debt(snapshot=None, with_legs=True)]
    execute_billing_evidence_import(
        _evidence(snapshot=_snapshot()),
        journal=_JournalRecorder(),
        state=state,
        now_ms=CLOSED_AT_MS + 1,
    )

    bybit = MagicMock()
    bybit.fetch_order_fill_reconciliation = AsyncMock(
        return_value=SimpleNamespace(
            quantity=2.0,
            average_price=12.0,
            fee_quote=0.02,
            order_id="long-close-order",
            client_order_id="long-close-client",
            venue="bybit",
            filled_at_ms=CLOSED_AT_MS,
        )
    )
    okx = MagicMock()
    okx.fetch_order_fill_reconciliation = AsyncMock(
        return_value=SimpleNamespace(
            quantity=2.0,
            average_price=9.0,
            fee_quote=0.02,
            order_id="short-close-order",
            client_order_id="short-close-client",
            venue="okx",
            filled_at_ms=CLOSED_AT_MS,
        )
    )
    ctx = MagicMock()
    ctx.state = state
    ctx.config.runtime.mode = "live"
    ctx.venue_adapters = {Venue.BYBIT: bybit, Venue.OKX: okx}
    ctx._flush_adapter_order_diagnostics = lambda adapter: None
    for attribute in (
        "_fetch_close_leg_reconciliations",
        "_fetch_pending_close_terminal_live_sizes",
        "_try_abandon_stale_pending_close_reconciliation",
        "_venue_private_position_confirmed",
        "_open_positions_private_confirmation_ready",
    ):
        setattr(ctx, attribute, None)

    await CloseRuntime(ctx)._process_pending_close_reconciliations(CLOSED_AT_MS + 2)

    bybit.fetch_order_fill_reconciliation.assert_awaited_once_with(
        SYMBOL, "long-close-order", "long-close-client"
    )
    okx.fetch_order_fill_reconciliation.assert_awaited_once_with(
        SYMBOL, "short-close-order", "short-close-client"
    )
    assert state.pending_close_reconciliations == []
    assert any(
        call.args[1] == "exit.reconciled"
        for call in ctx.journal.append_critical.call_args_list
    )


class _JournalRecorder:
    def __init__(self) -> None:
        self.records: list[tuple[int, str, dict[str, object]]] = []

    def append_critical(self, now_ms: int, kind: str, payload: dict[str, object]) -> None:
        self.records.append((now_ms, kind, payload))
