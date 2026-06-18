from __future__ import annotations

import json
import sys

import pytest

from lightfee.engine.state import EngineState
from lightfee.ops.auto_fail_closed_repair import (
    SAFE_TO_ALIGN_STALE_RISK_STATE,
    classify_auto_fail_closed_latch,
    classify_stale_risk_state_alignment,
    repair_auto_fail_closed_latch,
    repair_stale_risk_state_alignment,
)
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


def _flat_exchange_truth() -> dict:
    return {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": False,
        "has_open_order": False,
        "positions": {},
        "open_orders": {},
    }


def _state_with_stale_auto_latch() -> EngineState:
    state = EngineState(
        lifecycle=EngineLifecycle.RISK_ONLY,
        risk_mode=GlobalRiskMode.FAIL_CLOSED,
    )
    state.operator.requested_mode = GlobalRiskMode.FAIL_CLOSED
    state.recovery_blocked_reason = "startup_recovery_pending_work_without_open_positions"
    state.unpaired_live_position_recoveries = [
        {
            "venue": "bybit",
            "symbol": "HOMEUSDT",
            "terminal_status": "flat",
            "quantity": 0.0,
        }
    ]
    return state


def _state_with_stale_auto_fail_closed_without_operator() -> EngineState:
    state = EngineState(
        lifecycle=EngineLifecycle.RISK_ONLY,
        risk_mode=GlobalRiskMode.FAIL_CLOSED,
    )
    state.operator.requested_mode = None
    state.recovery_blocked_reason = "unpaired_live_position"
    return state


def _state_with_stale_active_unpaired_record() -> EngineState:
    state = EngineState(
        lifecycle=EngineLifecycle.RISK_ONLY,
        risk_mode=GlobalRiskMode.RUNNING,
    )
    state.operator.requested_mode = None
    state.recovery_blocked_reason = "unpaired_live_position"
    state.recovery_blocked_at_ms = 1234
    state.unpaired_live_position_recoveries = [
        {
            "venue": "okx",
            "symbol": "HOME-USDT-SWAP",
            "side": "sell",
            "quantity": 1300.0,
            "notional_quote": 44.733,
            "last_error": "auto_disabled",
            "terminal_status": "",
            "owner_excluded": True,
            "open_order_truth_available": False,
        }
    ]
    return state


def test_classifies_auto_latch_as_safe_when_truth_flat_and_no_ops_fail_closed():
    state = _state_with_stale_auto_latch()

    result = classify_auto_fail_closed_latch(
        state,
        journal_events=[
            {
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {"source": "auto_pending_entry_abort"},
            }
        ],
        exchange_truth=_flat_exchange_truth(),
    )

    assert result["classification"] == "safe_to_repair_auto_latch"
    assert result["apply_allowed"] is True
    assert result["has_operator_fail_closed_evidence"] is False


def test_classifies_current_auto_fail_closed_safe_despite_historical_ops_fail_closed():
    state = _state_with_stale_auto_fail_closed_without_operator()

    result = classify_auto_fail_closed_latch(
        state,
        journal_events=[
            {
                "kind": "ops.command_applied",
                "payload": {
                    "command": "fail_closed",
                    "new_risk": "fail_closed",
                    "new_lifecycle": "risk_only",
                },
            },
            {
                "kind": "ops.command_applied",
                "payload": {
                    "command": "resume_if_safe",
                    "new_risk": "running",
                    "new_lifecycle": "running",
                },
            },
        ],
        exchange_truth=_flat_exchange_truth(),
    )

    assert result["classification"] == "safe_to_repair_auto_latch"
    assert result["apply_allowed"] is True
    assert result["operator_requested_mode"] is None
    assert result["has_operator_fail_closed_evidence"] is True


def test_preserves_latch_when_journal_has_real_operator_fail_closed():
    state = _state_with_stale_auto_latch()

    result = classify_auto_fail_closed_latch(
        state,
        journal_events=[
            {
                "kind": "ops.command_applied",
                "payload": {"command": "fail_closed"},
            }
        ],
        exchange_truth=_flat_exchange_truth(),
    )

    assert result["classification"] == "operator_latch_must_preserve"
    assert result["apply_allowed"] is False
    assert "operator_fail_closed_evidence" in result["reasons"]


def test_apply_repairs_auto_fail_closed_without_operator_latch():
    state = _state_with_stale_auto_fail_closed_without_operator()
    journal = _CaptureJournal()

    result = repair_auto_fail_closed_latch(
        state,
        journal_events=[],
        exchange_truth=_flat_exchange_truth(),
        apply=True,
        journal=journal,
        ts_ms=1234,
    )

    assert result["classification"] == "safe_to_repair_auto_latch"
    assert result["applied"] is True
    assert state.operator.requested_mode is None
    assert state.recovery_blocked_reason is None
    assert state.risk_mode == GlobalRiskMode.RUNNING
    assert state.lifecycle == EngineLifecycle.RUNNING
    assert journal.critical_records[-1]["kind"] == "runtime.auto_fail_closed_recovered"


def test_refuses_repair_when_exchange_truth_is_not_high_confidence_flat():
    state = _state_with_stale_auto_latch()

    result = classify_auto_fail_closed_latch(
        state,
        journal_events=[],
        exchange_truth={
            "available": True,
            "confidence": "low",
            "has_nonzero_position": False,
            "has_open_order": False,
        },
    )

    assert result["classification"] == "unsafe_truth_or_cleanup_required"
    assert result["apply_allowed"] is False
    assert "exchange_truth_not_high_confidence_flat" in result["reasons"]


def test_classifies_current_stale_risk_state_as_safe_to_align():
    state = _state_with_stale_active_unpaired_record()

    result = classify_stale_risk_state_alignment(
        state,
        journal_events=[],
        exchange_truth=_flat_exchange_truth(),
    )

    assert result["classification"] == SAFE_TO_ALIGN_STALE_RISK_STATE
    assert result["apply_allowed"] is True
    assert result["active_unpaired_recovery_count"] == 1


def test_stale_risk_alignment_preserves_real_operator_fail_closed():
    state = _state_with_stale_active_unpaired_record()
    state.operator.requested_mode = GlobalRiskMode.FAIL_CLOSED

    result = classify_stale_risk_state_alignment(
        state,
        journal_events=[
            {"kind": "ops.command_applied", "payload": {"command": "fail_closed"}}
        ],
        exchange_truth=_flat_exchange_truth(),
    )

    assert result["classification"] == "operator_latch_must_preserve"
    assert result["apply_allowed"] is False
    assert "operator_fail_closed_evidence" in result["reasons"]


def test_historical_fail_closed_journal_without_current_latch_does_not_block_alignment():
    state = _state_with_stale_active_unpaired_record()
    state.operator.requested_mode = None

    result = classify_stale_risk_state_alignment(
        state,
        journal_events=[
            {"kind": "ops.command_applied", "payload": {"command": "fail_closed"}}
        ],
        exchange_truth=_flat_exchange_truth(),
    )

    assert result["classification"] == SAFE_TO_ALIGN_STALE_RISK_STATE
    assert result["apply_allowed"] is True


def test_apply_stale_risk_alignment_terminalizes_records_and_recomputes_running():
    state = _state_with_stale_active_unpaired_record()
    journal = _CaptureJournal()

    result = repair_stale_risk_state_alignment(
        state,
        journal_events=[],
        exchange_truth=_flat_exchange_truth(),
        apply=True,
        journal=journal,
        ts_ms=1234,
    )

    assert result["classification"] == SAFE_TO_ALIGN_STALE_RISK_STATE
    assert result["applied"] is True
    assert state.risk_mode == GlobalRiskMode.RUNNING
    assert state.lifecycle == EngineLifecycle.RUNNING
    assert state.recovery_blocked_reason is None
    assert state.unpaired_live_position_recoveries[0]["terminal_status"] == "flat"
    assert journal.critical_records[-1]["kind"] == "runtime.stale_risk_state_aligned"


class _CaptureJournal:
    def __init__(self) -> None:
        self.critical_records: list[dict] = []

    def append_critical(self, ts_ms, kind, payload):
        self.critical_records.append({
            "ts_ms": ts_ms,
            "kind": kind,
            "payload": payload,
        })
        return len(self.critical_records)


def test_apply_clears_pseudo_latch_and_recomputes_running_via_recovery_core():
    state = _state_with_stale_auto_latch()
    journal = _CaptureJournal()

    result = repair_auto_fail_closed_latch(
        state,
        journal_events=[],
        exchange_truth=_flat_exchange_truth(),
        apply=True,
        journal=journal,
        ts_ms=1234,
    )

    assert result["classification"] == "safe_to_repair_auto_latch"
    assert result["applied"] is True
    assert state.operator.requested_mode is None
    assert state.recovery_blocked_reason is None
    assert state.risk_mode == GlobalRiskMode.RUNNING
    assert state.lifecycle == EngineLifecycle.RUNNING
    assert result["previous_risk_mode"] == "fail_closed"
    assert result["new_risk_mode"] == "running"
    assert journal.critical_records[-1]["kind"] == "runtime.auto_fail_closed_recovered"
    assert journal.critical_records[-1]["payload"]["new_risk_mode"] == "running"


def test_ops_repair_apply_persists_running_and_critical_journal(tmp_path, monkeypatch, capsys):
    from lightfee.apps import ops
    from lightfee.engine.recovery import (
        _restore_state_from_snapshot_dict,
        build_persistent_state_view,
    )
    from lightfee.persistence.journal import Journal
    from lightfee.persistence.snapshot_store import SnapshotStore

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state = _state_with_stale_auto_latch()
    SnapshotStore(data_dir / "snapshot.json").write(build_persistent_state_view(state))
    truth_path = tmp_path / "exchange_truth.json"
    truth_path.write_text(json.dumps(_flat_exchange_truth()), encoding="utf-8")

    monkeypatch.setenv("LIGHTFEE_DATA_DIR", str(data_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lightfee-ops",
            "repair-auto-fail-closed-latch",
            "--exchange-truth",
            str(truth_path),
            "--apply",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        ops.main()

    assert exc.value.code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["applied"] is True
    assert output["new_risk_mode"] == "running"

    repaired = _restore_state_from_snapshot_dict(
        SnapshotStore(data_dir / "snapshot.json").read()
    )
    assert repaired.operator.requested_mode is None
    assert repaired.risk_mode == GlobalRiskMode.RUNNING
    assert repaired.lifecycle == EngineLifecycle.RUNNING

    records = Journal(data_dir / "journal.jsonl").read_all()
    recovered = [
        record for record in records
        if record["kind"] == "runtime.auto_fail_closed_recovered"
    ]
    assert recovered
    assert recovered[-1]["payload"]["source"] == "repair_auto_fail_closed_latch"


def test_ops_stale_risk_apply_persists_aligned_state_and_journal(tmp_path, monkeypatch, capsys):
    from lightfee.apps import ops
    from lightfee.engine.recovery import (
        _restore_state_from_snapshot_dict,
        build_persistent_state_view,
    )
    from lightfee.persistence.journal import Journal
    from lightfee.persistence.snapshot_store import SnapshotStore

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state = _state_with_stale_active_unpaired_record()
    SnapshotStore(data_dir / "snapshot.json").write(build_persistent_state_view(state))
    truth_path = tmp_path / "exchange_truth.json"
    truth_path.write_text(json.dumps(_flat_exchange_truth()), encoding="utf-8")

    monkeypatch.setenv("LIGHTFEE_DATA_DIR", str(data_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lightfee-ops",
            "repair-stale-risk-state",
            "--exchange-truth",
            str(truth_path),
            "--apply",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        ops.main()

    assert exc.value.code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["classification"] == SAFE_TO_ALIGN_STALE_RISK_STATE
    assert output["applied"] is True
    assert output["new_lifecycle"] == "running"

    repaired = _restore_state_from_snapshot_dict(
        SnapshotStore(data_dir / "snapshot.json").read()
    )
    assert repaired.risk_mode == GlobalRiskMode.RUNNING
    assert repaired.lifecycle == EngineLifecycle.RUNNING
    assert repaired.recovery_blocked_reason is None
    assert repaired.unpaired_live_position_recoveries[0]["terminal_status"] == "flat"

    records = Journal(data_dir / "journal.jsonl").read_all()
    aligned = [
        record for record in records
        if record["kind"] == "runtime.stale_risk_state_aligned"
    ]
    assert aligned
    assert aligned[-1]["payload"]["source"] == "repair_stale_risk_state"


def test_ops_repair_apply_accepts_explicit_production_paths(tmp_path, monkeypatch, capsys):
    from lightfee.apps import ops
    from lightfee.engine.recovery import (
        _restore_state_from_snapshot_dict,
        build_persistent_state_view,
    )
    from lightfee.persistence.journal import Journal
    from lightfee.persistence.snapshot_store import SnapshotStore

    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    snapshot_path = runtime_dir / "live-state.json"
    journal_path = runtime_dir / "live-events.jsonl"
    state = _state_with_stale_auto_fail_closed_without_operator()
    SnapshotStore(snapshot_path).write(build_persistent_state_view(state))
    truth_path = tmp_path / "exchange_truth.json"
    truth_path.write_text(json.dumps(_flat_exchange_truth()), encoding="utf-8")

    monkeypatch.delenv("LIGHTFEE_DATA_DIR", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lightfee-ops",
            "repair-auto-fail-closed-latch",
            "--snapshot-path",
            str(snapshot_path),
            "--journal-path",
            str(journal_path),
            "--exchange-truth",
            str(truth_path),
            "--apply",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        ops.main()

    assert exc.value.code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["applied"] is True
    repaired = _restore_state_from_snapshot_dict(SnapshotStore(snapshot_path).read())
    assert repaired.risk_mode == GlobalRiskMode.RUNNING
    assert repaired.lifecycle == EngineLifecycle.RUNNING

    records = Journal(journal_path).read_all()
    assert records[-1]["kind"] == "runtime.auto_fail_closed_recovered"
