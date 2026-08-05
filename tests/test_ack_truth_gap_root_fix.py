"""RED→GREEN contract tests for ACK truth-gap / billing / diagnose root fix.

Covers:
- ACK truth-gap NEVER enters billing_unreconciled loop
- Terminal-flat supersede, confirmed-execution-fill resolve, weak/zero/unavailable retain
- Mixed-evidence fail-closed (one fill + one unavailable → retain)
- Ledger-based weak-evidence discrimination (ack_only, order_detail, new, zero-qty)
- billing_unreconciled temporal ordering in diagnose
- pending_close_count=0 but reconciliation queue non-empty blocks diagnose gate
- State export and lifecycle-closure contract integrity
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from lightfee.core.domain import Venue
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ack_task(**kw: Any) -> dict[str, Any]:
    """Default task matches passive_close._register_accepted_order_truth_gap:
    only the ACK-triggering long leg is recorded; short_legs is empty."""
    pid = kw.get("position_id", "entry-1-BTCUSDT")
    sym = kw.get("symbol", "BTCUSDT")
    long_v = kw.get("long_venue", "bybit")
    short_v = kw.get("short_venue", "okx")
    return {
        "position_id": pid, "symbol": sym,
        "kind": "accepted_order_truth_gap", "reason": "passive_close",
        "closed_at_ms": kw.get("closed_at_ms", 1000000),
        "created_cycle": kw.get("created_cycle", 0),
        "attempt_count": kw.get("attempt_count", 0),
        "next_attempt_ms": kw.get("next_attempt_ms", 0),
        "requested_quantity": kw.get("requested_quantity", 100.0),
        "position_snapshot": {
            "position_id": pid, "symbol": sym,
            "long_venue": long_v, "short_venue": short_v,
        },
        "long_legs": kw.get("long_legs", [
            {"venue": long_v, "order_id": "abc-123", "client_order_id": "cl-456"},
        ]),
        "short_legs": kw.get("short_legs", []),
    }


class _FakeFill:
    """Simulates OrderFillReconciliation from an adapter."""

    def __init__(self, quantity: float = 0.0, **kw: Any):
        self.quantity = quantity
        self.fee_quote = kw.pop("fee_quote", 0.0)
        self.price = kw.pop("price", 0.0)
        self.order_id = kw.pop("order_id", "")
        self.client_order_id = kw.pop("client_order_id", "")
        self.venue = kw.pop("venue", "")
        self.filled_at_ms = kw.pop("filled_at_ms", 0)
        self.metadata = kw.pop("metadata", None) or {}
        for k, v in kw.items():
            setattr(self, k, v)


def _bybit_confirmed_fill(**kw: Any) -> _FakeFill:
    """A fill with Bybit execution-list metadata → CONFIRMED_FILL by ledger."""
    return _FakeFill(
        quantity=kw.pop("quantity", 100.0),
        fee_quote=0.5, price=50000.0,
        metadata={
            "queried_endpoints": ["/v5/order/realtime", "/v5/execution/list"],
            "response_classification": "filled",
            "evidence_source": "bybit_execution_list",
        },
        **kw,
    )


def _weak_ack_fill(**kw: Any) -> _FakeFill:
    """ACK-only / order-detail response — NOT a confirmed fill."""
    return _FakeFill(
        quantity=kw.pop("quantity", 100.0),
        fee_quote=0.5, price=50000.0,
        metadata={
            "queried_endpoints": ["/v5/order/realtime"],
            "response_classification": "new",
            "evidence_source": "bybit_order_realtime",
        },
        **kw,
    )


def _ctx() -> MagicMock:
    """CloseRuntime-compatible MagicMock context."""
    ctx = MagicMock()
    ctx.state.pending_close_reconciliations = []
    ctx.state.open_positions = {}
    ctx.state.tick_count = 10
    ctx.state.lifecycle = EngineLifecycle.RUNNING
    ctx.state.risk_mode = GlobalRiskMode.RUNNING
    ctx.state.operator.requested_mode = None
    ctx.state.last_error = None
    ctx.config.runtime.mode = "live"
    for attr in (
        "_apply_pending_close_reconciliation_backoff",
        "_fetch_close_leg_reconciliations",
        "_fetch_pending_close_terminal_live_sizes",
        "_try_abandon_stale_pending_close_reconciliation",
        "_venue_private_position_confirmed",
        "_open_positions_private_confirmation_ready",
        "_resolve_ws_bbo_close_mid",
        "_resolve_local_l2_mid",
    ):
        setattr(ctx, attr, None)
    return ctx


def _no_billing(calls: list) -> None:
    for c in calls:
        k = str(c[0][0]) if c[0] else str(c[0][1]) if len(c[0]) > 1 else ""
        assert "billing" not in k and "exit.reconciled" not in k


# ---------------------------------------------------------------------------
# 1. ACK truth-gap → NEVER enters billing
# ---------------------------------------------------------------------------


class TestAckTruthGapRouting:
    """accepted_order_truth_gap routes exclusively through order-truth
    or terminal-flat resolution.  No billing events emitted."""

    @pytest.mark.asyncio
    async def test_terminal_flat_supersedes_no_billing(self):
        """Both venues flat + no open orders → superseded.  No billing."""
        from lightfee.engine.close_runtime import CloseRuntime

        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [_ack_task()]
        cr = CloseRuntime(ctx)

        async def tf(*args, **kw):
            return (0.0, 0.0), None
        cr._fetch_pending_close_terminal_live_flat_truth = AsyncMock(side_effect=tf)

        await cr._process_pending_close_reconciliations(2000000)
        assert len(ctx.state.pending_close_reconciliations) == 0
        _no_billing(list(ctx.journal.append.call_args_list)
                     + list(ctx.journal.append_critical.call_args_list))
        assert any(
            c[0] and str(c[0][0]) == "exit.accepted_order_truth_gap_superseded"
            for c in ctx.journal.append.call_args_list
        )

    @pytest.mark.asyncio
    async def test_confirmed_execution_fill_resolves_no_billing(self):
        """Adapter returns fill from confirmed execution source →
        ledger CONFIRMED_FILL for the single persisted leg → resolved.
        Empty opposite side is normal (one-leg contract).  No billing."""
        from lightfee.engine.close_runtime import CloseRuntime

        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [_ack_task()]
        cr = CloseRuntime(ctx)

        async def tf(*args, **kw):
            return None, None  # unavailable → fall through to order truth
        cr._fetch_pending_close_terminal_live_flat_truth = AsyncMock(side_effect=tf)

        mock = MagicMock()
        mock.fetch_order_fill_reconciliation = AsyncMock(
            return_value=_bybit_confirmed_fill(quantity=100.0)
        )
        ctx.venue_adapters = {Venue.BYBIT: mock}

        await cr._process_pending_close_reconciliations(2000000)
        assert len(ctx.state.pending_close_reconciliations) == 0
        assert any(
            c[0] and str(c[0][0]) == "exit.accepted_order_truth_gap_resolved"
            for c in ctx.journal.append.call_args_list
        )
        _no_billing(list(ctx.journal.append.call_args_list)
                     + list(ctx.journal.append_critical.call_args_list))

    @pytest.mark.asyncio
    async def test_one_leg_empty_opposite_resolves(self):
        """One leg confirmed fill + empty opposite side (normal one-leg
        contract) → resolved.  Empty short_legs is NOT a retain condition."""
        from lightfee.engine.close_runtime import CloseRuntime

        task = _ack_task(
            long_legs=[{"venue": "bybit", "order_id": "long-1", "client_order_id": "cl-1"}],
            short_legs=[],  # Normal — only ACK-triggering leg recorded
        )
        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [task]
        cr = CloseRuntime(ctx)

        async def tf(*args, **kw):
            return None, None
        cr._fetch_pending_close_terminal_live_flat_truth = AsyncMock(side_effect=tf)

        mock = MagicMock()
        mock.fetch_order_fill_reconciliation = AsyncMock(
            return_value=_bybit_confirmed_fill(quantity=100.0, order_id="long-1")
        )
        ctx.venue_adapters = {Venue.BYBIT: mock}

        await cr._process_pending_close_reconciliations(2000000)
        assert len(ctx.state.pending_close_reconciliations) == 0
        _no_billing(list(ctx.journal.append.call_args_list)
                     + list(ctx.journal.append_critical.call_args_list))

    @pytest.mark.asyncio
    async def test_one_leg_confirmed_other_malformed_retains(self):
        """One leg confirmed fill + other side has non-dict element →
        MUST retain.  Malformed persisted leg is unresolved."""
        from lightfee.engine.close_runtime import CloseRuntime

        task = _ack_task(
            long_legs=[{"venue": "bybit", "order_id": "long-1", "client_order_id": "cl-1"}],
            short_legs=["not-a-dict"],  # Malformed
        )
        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [task]
        cr = CloseRuntime(ctx)

        async def tf(*args, **kw):
            return None, None
        cr._fetch_pending_close_terminal_live_flat_truth = AsyncMock(side_effect=tf)

        mock = MagicMock()
        mock.fetch_order_fill_reconciliation = AsyncMock(
            return_value=_bybit_confirmed_fill(quantity=100.0, order_id="long-1")
        )
        ctx.venue_adapters = {Venue.BYBIT: mock}

        await cr._process_pending_close_reconciliations(2000000)
        assert len(ctx.state.pending_close_reconciliations) == 1
        _no_billing(list(ctx.journal.append.call_args_list)
                     + list(ctx.journal.append_critical.call_args_list))

    @pytest.mark.asyncio
    async def test_weak_evidence_retains_no_billing(self):
        """Adapter returns fill from weak source (ACK/order_detail/new) →
        ledger classify as TRUTH_GAP → retained.  No billing."""
        from lightfee.engine.close_runtime import CloseRuntime

        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [_ack_task()]
        cr = CloseRuntime(ctx)

        async def tf(*args, **kw):
            return (0.0, 100.0), None  # not terminal-flat
        cr._fetch_pending_close_terminal_live_flat_truth = AsyncMock(side_effect=tf)

        mock = MagicMock()
        mock.fetch_order_fill_reconciliation = AsyncMock(
            return_value=_weak_ack_fill(quantity=100.0)
        )
        ctx.venue_adapters = {Venue.BYBIT: mock}

        await cr._process_pending_close_reconciliations(2000000)
        assert len(ctx.state.pending_close_reconciliations) == 1
        r = ctx.state.pending_close_reconciliations[0]
        assert int(r.get("attempt_count") or 0) >= 1
        _no_billing(list(ctx.journal.append.call_args_list)
                     + list(ctx.journal.append_critical.call_args_list))

    @pytest.mark.asyncio
    async def test_zero_qty_retains_no_billing(self):
        """Adapter returns None (zero qty / no fill found) → retained.
        Zero-qty order detail is NOT confirmed no-fill.  No billing."""
        from lightfee.engine.close_runtime import CloseRuntime

        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [_ack_task()]
        cr = CloseRuntime(ctx)

        async def tf(*args, **kw):
            return (0.0, 100.0), None  # not terminal-flat
        cr._fetch_pending_close_terminal_live_flat_truth = AsyncMock(side_effect=tf)

        mock = MagicMock()
        mock.fetch_order_fill_reconciliation = AsyncMock(return_value=None)
        ctx.venue_adapters = {Venue.BYBIT: mock}

        await cr._process_pending_close_reconciliations(2000000)
        assert len(ctx.state.pending_close_reconciliations) == 1
        r = ctx.state.pending_close_reconciliations[0]
        assert int(r.get("attempt_count") or 0) >= 1
        _no_billing(list(ctx.journal.append.call_args_list)
                     + list(ctx.journal.append_critical.call_args_list))

    @pytest.mark.asyncio
    async def test_mixed_evidence_retains_fail_closed(self):
        """Long leg has confirmed fill, short leg adapter returns None →
        MUST retain.  A fill on one identity does not excuse uncertainty
        on another.  No billing."""
        from lightfee.engine.close_runtime import CloseRuntime

        task = _ack_task(
            long_legs=[{"venue": "bybit", "order_id": "long-1", "client_order_id": "cl-1"}],
            short_legs=[{"venue": "okx", "order_id": "short-1", "client_order_id": "cl-2"}],
        )
        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [task]
        cr = CloseRuntime(ctx)

        async def tf(*args, **kw):
            return (0.0, 100.0), None  # Not terminal-flat
        cr._fetch_pending_close_terminal_live_flat_truth = AsyncMock(side_effect=tf)

        class _BybitOkTwoFace:
            async def fetch_order_fill_reconciliation(self, symbol, oid, coid):
                if "long" in oid:
                    return _bybit_confirmed_fill(quantity=100.0, order_id=oid)
                return None  # short leg unavailable

        ctx.venue_adapters = {Venue.BYBIT: _BybitOkTwoFace(), Venue.OKX: _BybitOkTwoFace()}

        await cr._process_pending_close_reconciliations(2000000)
        assert len(ctx.state.pending_close_reconciliations) == 1
        r = ctx.state.pending_close_reconciliations[0]
        assert int(r.get("attempt_count") or 0) >= 1
        _no_billing(list(ctx.journal.append.call_args_list)
                     + list(ctx.journal.append_critical.call_args_list))


# ---------------------------------------------------------------------------
# 2. Diagnose false-green fixes
# ---------------------------------------------------------------------------


def _dg_state(**kw: Any) -> dict[str, Any]:
    return {
        "lifecycle": "running", "risk_mode": "running",
        "open_position_count": 0, "pending_entry_count": 0,
        "pending_close_count": 0, "pending_residual_repair_count": 0,
        **kw,
    }


class TestDiagnoseTemporalOrdering:
    """billing_unreconciled resolved ONLY by subsequent terminal event."""

    def test_after_terminal_remains_unresolved(self):
        """reconciled@300, billing_unreconciled@500 → terminal BEFORE gap → unresolved."""
        from scripts import diagnose_live as dl

        events = [
            {"kind": "exit.reconciled", "ts_ms": 300, "payload": {"position_id": "p1"}},
            {"kind": "exit.billing_unreconciled", "ts_ms": 500, "payload": {"position_id": "p1"}},
        ]
        r = dl._build_production_acceptance_gate(events, _dg_state(), {"available": True, "venues": {}})
        assert r["unresolved_billing_unreconciled_count"] == 1
        assert "billing_unreconciled_unresolved" in r["blocking_reasons"]

    def test_before_terminal_is_resolved(self):
        """billing_unreconciled@300, reconciled@500 → terminal AFTER gap → resolved."""
        from scripts import diagnose_live as dl

        events = [
            {"kind": "exit.billing_unreconciled", "ts_ms": 300, "payload": {"position_id": "p1"}},
            {"kind": "exit.reconciled", "ts_ms": 500, "payload": {"position_id": "p1"}},
        ]
        r = dl._build_production_acceptance_gate(events, _dg_state(), {"available": True, "venues": {}})
        assert r["unresolved_billing_unreconciled_count"] == 0


class TestDiagnoseReconciliationGate:
    """Diagnose reads authoritative reconciliation summary, not just pending_close_count."""

    def test_queue_nonempty_when_closes_zero(self):
        from scripts import diagnose_live as dl

        s = _dg_state(pending_close_reconciliation_summary={
            "total_count": 2, "by_kind": {"accepted_order_truth_gap": 2},
            "backed_off_count": 1, "unknown_status_count": 0,
        })
        r = dl._build_production_acceptance_gate([], s, {"available": True, "venues": {}})
        assert "pending_close_reconciliations_not_empty" in r["blocking_reasons"]
        assert r["pending_close_reconciliation_total"] == 2
        assert r["pending_close_reconciliation_ack_truth_gap"] == 2

    def test_queue_empty_passes(self):
        from scripts import diagnose_live as dl

        s = _dg_state(pending_close_reconciliation_summary={
            "total_count": 0, "by_kind": {}, "backed_off_count": 0, "unknown_status_count": 0,
        })
        r = dl._build_production_acceptance_gate([], s, {"available": True, "venues": {}})
        assert "pending_close_reconciliations_not_empty" not in r["blocking_reasons"]


# ---------------------------------------------------------------------------
# 3. State export + lifecycle closure contract
# ---------------------------------------------------------------------------


class TestStateAndClosureContract:
    """EngineState.to_dict() exports reconciliation summary.  Event-kinds are mapped."""

    def test_reconciliation_summary_export(self):
        from lightfee.engine.state import EngineState

        state = EngineState()
        state.enqueue_pending_close_reconciliation(_ack_task(position_id="e-1"))
        state.enqueue_pending_close_reconciliation(_ack_task(position_id="e-2"))

        s = state.to_dict()["pending_close_reconciliation_summary"]
        assert s["total_count"] == 2
        assert s["by_kind"]["accepted_order_truth_gap"] == 2

    def test_dedup_preserves_unique_tasks(self):
        from lightfee.engine.state import EngineState

        state = EngineState()
        state.enqueue_pending_close_reconciliation(_ack_task(position_id="e-1"))
        state.enqueue_pending_close_reconciliation(_ack_task(position_id="e-1"))
        assert state.to_dict()["pending_close_reconciliation_summary"]["total_count"] == 1

    def test_event_kind_mappings(self):
        from lightfee.engine.v1_lifecycle_closure import (
            _EVENT_KIND_PHASES,
            V1LifecycleClosurePhase,
        )

        p = V1LifecycleClosurePhase.PASSIVE_CLOSE.value
        for key in (
            "exit.accepted_order_truth_gap_registered",
            "exit.accepted_order_truth_gap_resolved",
            "exit.accepted_order_truth_gap_superseded",
            "exit.passive_close_terminal_zero_qty_reduce_only_evidence",
            "exit.billing_evidence_unavailable",
            "exit.billing_unreconciled",
        ):
            assert key in _EVENT_KIND_PHASES, f"{key} missing"
            assert _EVENT_KIND_PHASES[key] == p, f"{key} phase mismatch"
