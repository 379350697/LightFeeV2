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

from lightfee.core.domain import OrderFill, Side, Venue
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
        "_try_register_terminal_fill_evidence_debt",
        "_venue_private_position_confirmed",
        "_open_positions_private_confirmation_ready",
        "_resolve_local_l2_mid",
    ):
        setattr(ctx, attr, None)
    return ctx


def _no_billing(calls: list) -> None:
    for c in calls:
        k = str(c[0][0]) if c[0] else str(c[0][1]) if len(c[0]) > 1 else ""
        assert "billing" not in k and "exit.reconciled" not in k


@pytest.mark.parametrize(
    ("long_closed", "short_closed", "expected_kind", "expected_owner"),
    (
        (1.0, 0.0, "partial", {"long": 1.0, "short": 0.0}),
        (1.0, 1.0, "final", {"long": 1.0, "short": 1.0}),
    ),
)
def test_close_registration_persists_immutable_segment_owner(
    tmp_path,
    long_closed,
    short_closed,
    expected_kind,
    expected_owner,
):
    """All active/risk close callers share this durable accounting boundary."""
    from lightfee.engine.close_executor import (
        CloseExecutionLeg,
        register_close_accounting_reconciliation,
    )
    from lightfee.engine.close_runtime import CloseRuntime
    from lightfee.engine.state import EngineState, OpenPosition
    from lightfee.persistence.journal import Journal

    state = EngineState()
    position = OpenPosition(
        position_id="entry-owner",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        long_quantity=1.0,
        short_quantity=1.0,
        long_entry_price=100.0,
        short_entry_price=100.0,
        opened_at_ms=1_000,
    )
    journal = Journal(tmp_path / "owner.jsonl")
    journal.open()
    try:
        register_close_accounting_reconciliation(
            state,
            journal,
            position,
            long_legs=[
                CloseExecutionLeg(
                    OrderFill(Venue.BINANCE, "BTCUSDT", Side.SELL, long_closed, 101.0, "long-1")
                )
            ] if long_closed else [],
            short_legs=[
                CloseExecutionLeg(
                    OrderFill(Venue.BYBIT, "BTCUSDT", Side.BUY, short_closed, 99.0, "short-1")
                )
            ] if short_closed else [],
            now_ms=2_000,
            reason="funding_capture",
            source="active_close",
            evidence_gaps=(),
        )
    finally:
        journal.close()

    reconciliation = state.pending_close_reconciliations[0]
    assert reconciliation["kind"] == expected_kind
    assert reconciliation["owned_close_quantities"] == expected_owner
    assert CloseRuntime._close_reconciliation_expected_quantities(
        reconciliation, reconciliation["position_snapshot"]
    ) == (expected_owner["long"], expected_owner["short"])


def test_malformed_immutable_close_owner_stays_fail_closed():
    """A bad new owner must not silently fall back to a mutable legacy shape."""
    from lightfee.engine.state import pending_close_reconciliation_missing_legs

    reconciliation = {
        "kind": "final",
        "position_snapshot": {
            "long_quantity": 1.0,
            "short_quantity": 1.0,
        },
        "owned_close_quantities": {"long": "not-a-quantity", "short": 1.0},
        "long_legs": [{"order_id": "long-1"}],
        "short_legs": [{"order_id": "short-1"}],
    }

    assert pending_close_reconciliation_missing_legs(reconciliation) == (
        "long",
        "short",
    )


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


class TestBillingEvidenceIdentityGap:
    """A close without durable identity becomes a persistent evidence debt."""

    @pytest.mark.asyncio
    async def test_staged_partial_then_final_reconciles_once_with_combined_pnl(self):
        """V1 partial accounting is non-terminal and flows into its later final.

        A historical COTI-style close first confirms only the long leg.  The
        second close is a final task whose snapshot has no remaining long
        quantity.  Both operator-evidence imports must be accepted, the
        partial must emit ``exit.partial_reconciled``, and the final bill must
        include each segment exactly once.
        """
        from lightfee.engine.close_runtime import CloseRuntime
        from lightfee.engine.state import EngineState

        position_id = "entry-staged-COTIUSDT"
        snapshot = {
            "position_id": position_id,
            "symbol": "COTIUSDT",
            "long_venue": "bybit",
            "short_venue": "binance",
            "long_quantity": 100.0,
            "short_quantity": 100.0,
            "matched_quantity": 100.0,
            "long_entry_price": 10.0,
            "short_entry_price": 12.0,
            "total_entry_fee_quote": 0.4,
            "entry_fee_evidence_complete": True,
            "captured_funding_quote": 0.0,
        }
        partial = {
            "position_id": position_id,
            "symbol": "COTIUSDT",
            "kind": "partial",
            "reason": "funding_capture",
            "closed_at_ms": 1000,
            "position_snapshot": snapshot,
            "original_payload": {
                "long_closed_qty": 100.0,
                "short_closed_qty": 0.0,
            },
            "long_legs": [{"venue": "bybit", "order_id": "long-partial"}],
            "short_legs": [],
            "reconciliation_status": "evidence_debt",
        }
        final = {
            "position_id": position_id,
            "symbol": "COTIUSDT",
            "kind": "final",
            "reason": "funding_capture",
            "closed_at_ms": 2000,
            "position_snapshot": {**snapshot, "long_quantity": 0.0},
            "original_payload": {"long_closed_qty": 0.0, "short_closed_qty": 100.0},
            "long_legs": [],
            "short_legs": [{"venue": "binance", "order_id": "short-final"}],
            "reconciliation_status": "evidence_debt",
        }
        stale_final_snapshot = dict(final["position_snapshot"])

        state = EngineState(lifecycle=EngineLifecycle.RUNNING)
        state.pending_close_reconciliations = [partial, final]
        evidence_digest = "a" * 64
        state.import_pending_close_reconciliation_evidence(
            {
                "position_id": position_id,
                "kind": "partial",
                "closed_at_ms": 1000,
                "position_snapshot": snapshot,
                "long_legs": partial["long_legs"],
                "short_legs": [],
            },
            evidence_reference="fixture:partial",
            evidence_sha256=evidence_digest,
            imported_at_ms=3000,
        )
        class Adapter:
            async def fetch_order_fill_reconciliation(self, _symbol, order_id, _cid):
                return {
                    "long-partial": _FakeFill(
                        quantity=100.0,
                        price=11.0,
                        fee_quote=0.2,
                        order_id=order_id,
                        venue="bybit",
                        filled_at_ms=1100,
                    ),
                    "short-final": _FakeFill(
                        quantity=100.0,
                        price=10.0,
                        fee_quote=0.3,
                        order_id=order_id,
                        venue="binance",
                        filled_at_ms=2100,
                    ),
                }[order_id]

        ctx = _ctx()
        ctx.state = state
        ctx.venue_adapters = {Venue.BYBIT: Adapter(), Venue.BINANCE: Adapter()}
        runtime = CloseRuntime(ctx)
        await runtime._process_pending_close_reconciliations(4000)

        # The final evidence arrives after the partial is reconciled.  Its
        # source snapshot is necessarily older and must not erase the partial
        # cumulative accounting that V1 carries forward.
        state.import_pending_close_reconciliation_evidence(
            {
                "position_id": position_id,
                "kind": "final",
                "closed_at_ms": 2000,
                "position_snapshot": stale_final_snapshot,
                "long_legs": [],
                "short_legs": final["short_legs"],
            },
            evidence_reference="fixture:final",
            evidence_sha256="b" * 64,
            imported_at_ms=4000,
        )
        imported_final = state.pending_close_reconciliations[0]["position_snapshot"]
        assert imported_final["realized_price_pnl_quote"] == pytest.approx(100.0)
        assert imported_final["realized_exit_fee_quote"] == pytest.approx(0.2)

        await runtime._process_pending_close_reconciliations(4001)

        assert state.pending_close_reconciliations == []
        critical = list(ctx.journal.append_critical.call_args_list)
        partial_events = [call for call in critical if call.args[1] == "exit.partial_reconciled"]
        final_events = [call for call in critical if call.args[1] == "exit.reconciled"]
        assert len(partial_events) == 1
        assert len(final_events) == 1
        assert partial_events[0].args[2]["price_pnl"] == pytest.approx(100.0)
        assert final_events[0].args[2]["price_pnl"] == pytest.approx(300.0)
        assert final_events[0].args[2]["exit_fee_quote"] == pytest.approx(0.5)
        assert final_events[0].args[2]["net_quote"] == pytest.approx(299.1)

    def test_partial_reconciled_replay_propagates_later_final_accounting(self):
        """A restart after partial evidence must not lose the later final sum."""
        from lightfee.engine.recovery import _apply_journal_replay_to_state
        from lightfee.engine.state import EngineState
        from lightfee.persistence.journal import replay_journal_records

        position_id = "entry-partial-replay"
        final = {
            "position_id": position_id,
            "kind": "final",
            "closed_at_ms": 2000,
            "position_snapshot": {
                "position_id": position_id,
                "realized_price_pnl_quote": 0.0,
                "realized_exit_fee_quote": 0.0,
            },
        }
        partial_event = {
            "kind": "exit.partial_reconciled",
            "ts_ms": 1500,
            "payload": {
                "position_id": position_id,
                "reconciled_realized_price_pnl_quote": 100.0,
                "reconciled_realized_exit_fee_quote": 0.2,
            },
        }

        restored = EngineState()
        restored.pending_close_reconciliations = [final]
        _apply_journal_replay_to_state(restored, [partial_event])
        replayed = replay_journal_records([
            {
                "kind": "entry.opened",
                "ts_ms": 1000,
                "payload": {"position_id": position_id},
            },
            partial_event,
        ])

        final_snapshot = restored.pending_close_reconciliations[0]["position_snapshot"]
        assert final_snapshot["realized_price_pnl_quote"] == pytest.approx(100.0)
        assert final_snapshot["realized_exit_fee_quote"] == pytest.approx(0.2)
        assert replayed["positions"][position_id]["realized_price_pnl_quote"] == pytest.approx(100.0)
        assert replayed["positions"][position_id]["realized_exit_fee_quote"] == pytest.approx(0.2)

    @pytest.mark.asyncio
    async def test_orphan_partial_is_not_cleared_as_a_terminal_bill(self):
        """Exact partial evidence without a remaining owner stays fail-closed."""
        from lightfee.engine.close_runtime import CloseRuntime
        from lightfee.engine.state import EngineState

        task = {
            "position_id": "entry-orphan-partial",
            "symbol": "COWUSDT",
            "kind": "partial",
            "closed_at_ms": 1000,
            "position_snapshot": {
                "position_id": "entry-orphan-partial",
                "symbol": "COWUSDT",
                "long_venue": "bybit",
                "short_venue": "binance",
                "long_quantity": 100.0,
                "short_quantity": 100.0,
                "long_entry_price": 10.0,
                "short_entry_price": 12.0,
                "total_entry_fee_quote": 0.4,
                "entry_fee_evidence_complete": True,
                "captured_funding_quote": 0.0,
            },
            "original_payload": {"long_closed_qty": 100.0, "short_closed_qty": 0.0},
            "long_legs": [{"venue": "bybit", "order_id": "partial-only"}],
            "short_legs": [],
        }

        class Adapter:
            async def fetch_order_fill_reconciliation(self, _symbol, order_id, _cid):
                return _FakeFill(
                    quantity=100.0,
                    price=11.0,
                    fee_quote=0.2,
                    order_id=order_id,
                    venue="bybit",
                )

        state = EngineState(lifecycle=EngineLifecycle.RUNNING)
        state.pending_close_reconciliations = [task]
        ctx = _ctx()
        ctx.state = state
        ctx.venue_adapters = {Venue.BYBIT: Adapter(), Venue.BINANCE: Adapter()}
        await CloseRuntime(ctx)._process_pending_close_reconciliations(2000)

        assert len(state.pending_close_reconciliations) == 1
        retained = state.pending_close_reconciliations[0]
        assert retained["position_id"] == task["position_id"]
        assert retained["attempt_count"] == 1
        assert not any(
            call.args and call.args[1] == "exit.partial_reconciled"
            for call in ctx.journal.append_critical.call_args_list
        )

    @pytest.mark.asyncio
    async def test_missing_identity_is_classified_once_and_retained_fail_closed(self):
        from lightfee.engine.close_runtime import CloseRuntime

        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [{
            "position_id": "entry-1-BTCUSDT",
            "symbol": "BTCUSDT",
            "kind": "final",
            "reason": "funding_capture",
            "closed_at_ms": 1000000,
            "created_cycle": 0,
            "attempt_count": 0,
            "next_attempt_ms": 0,
            "reconciliation_mode": "venue_execution_history_required",
            "missing_close_order_identity": True,
            "billing_reconciliation_required": True,
            "position_snapshot": {
                "position_id": "entry-1-BTCUSDT",
                "symbol": "BTCUSDT",
                "long_venue": "bybit",
                "short_venue": "okx",
                "matched_quantity": 100.0,
            },
            "long_legs": [],
            "short_legs": [],
        }]
        cr = CloseRuntime(ctx)

        await cr._process_pending_close_reconciliations(2000000)

        assert len(ctx.state.pending_close_reconciliations) == 1
        retained = ctx.state.pending_close_reconciliations[0]
        assert retained["reconciliation_status"] == "evidence_debt"
        assert retained["evidence_debt_reason"] == "missing_close_order_identity"
        assert retained["next_attempt_ms"] == 0
        debt_events = [
            call for call in ctx.journal.append_critical.call_args_list
            if call.args and call.args[1] == "exit.billing_evidence_debt_registered"
        ]
        assert len(debt_events) == 1
        assert debt_events[0].args[2]["operator_action"] == (
            "none_automatic_exact_then_unique_history_recheck"
        )
        assert debt_events[0].args[2]["identity_evidence"] == {
            "missing_identity_legs": ["long", "short"],
            "long": {
                "leg_count": 0,
                "exchange_order_id_count": 0,
                "client_order_id_only_count": 0,
                "recovery_placeholder_count": 0,
                "missing_identity_count": 0,
            },
            "short": {
                "leg_count": 0,
                "exchange_order_id_count": 0,
                "client_order_id_only_count": 0,
                "recovery_placeholder_count": 0,
                "missing_identity_count": 0,
            },
        }

        await cr._process_pending_close_reconciliations(2000001)
        assert len([
            call for call in ctx.journal.append_critical.call_args_list
            if call.args and call.args[1] == "exit.billing_evidence_debt_registered"
        ]) == 1

    @pytest.mark.asyncio
    async def test_malformed_reconciliation_timestamps_are_retained_fail_closed(self):
        from lightfee.engine.close_runtime import CloseRuntime

        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [{
            "position_id": "entry-malformed-timestamps",
            "symbol": "BTCUSDT",
            "kind": "final",
            "closed_at_ms": "not-a-timestamp",
            "created_cycle": "not-a-cycle",
            "next_attempt_ms": float("inf"),
            "position_snapshot": {
                "long_venue": "bybit",
                "short_venue": "okx",
                "matched_quantity": 1.0,
            },
            "long_legs": [],
            "short_legs": [],
        }]
        cr = CloseRuntime(ctx)

        await cr._process_pending_close_reconciliations(2_000_000)

        assert len(ctx.state.pending_close_reconciliations) == 1
        assert any(
            call.args and call.args[1] == "exit.billing_evidence_debt_registered"
            for call in ctx.journal.append_critical.call_args_list
        )

    @pytest.mark.asyncio
    async def test_partial_leg_identity_is_migrated_and_retained_fail_closed(self):
        """One venue identity must not allow provisional billing terminalization."""
        from lightfee.engine.close_runtime import CloseRuntime

        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [{
            "position_id": "entry-zil-partial-identity",
            "symbol": "ZILUSDT",
            "kind": "final",
            "closed_at_ms": 1000000,
            "position_snapshot": {
                "position_id": "entry-zil-partial-identity",
                "symbol": "ZILUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "matched_quantity": 100.0,
            },
            "long_legs": [{
                "venue": "binance",
                "order_id": "binance-close-1",
                "client_order_id": "binance-cid-1",
            }],
            "short_legs": [],
        }]
        cr = CloseRuntime(ctx)

        await cr._process_pending_close_reconciliations(2000000)

        retained = ctx.state.pending_close_reconciliations[0]
        assert retained["reconciliation_status"] == "evidence_debt"
        assert retained["evidence_debt_reason"] == "missing_close_order_identity"
        assert retained["missing_close_order_identity"] is True
        assert retained["billing_reconciliation_required"] is True
        assert any(
            call.args
            and call.args[1] == "exit.billing_evidence_debt_registered"
            and call.args[2]["terminal_reason"] == "missing_close_order_identity"
            and call.args[2]["short_leg_count"] == 0
            for call in ctx.journal.append_critical.call_args_list
        )
        assert not any(
            call.args and call.args[0] == "exit.billing_evidence_unavailable"
            for call in ctx.journal.append.call_args_list
        )

    @pytest.mark.asyncio
    async def test_missing_venue_is_evidence_debt_without_exchange_retry(self):
        from lightfee.engine.close_runtime import CloseRuntime

        ctx = _ctx()
        ctx.state.pending_close_reconciliations = [{
            "position_id": "entry-missing-venue",
            "symbol": "BTCUSDT",
            "kind": "final",
            "closed_at_ms": 1000000,
            "position_snapshot": {
                "position_id": "entry-missing-venue",
                "symbol": "BTCUSDT",
                "long_venue": "bybit",
                "matched_quantity": 1.0,
            },
            "long_legs": [],
            "short_legs": [],
        }]
        cr = CloseRuntime(ctx)

        await cr._process_pending_close_reconciliations(2_000_000)

        retained = ctx.state.pending_close_reconciliations[0]
        assert retained["reconciliation_status"] == "evidence_debt"
        assert retained["evidence_debt_reason"] == "missing_position_snapshot_venues"
        assert ctx._fetch_close_leg_reconciliations is None

    def test_pending_close_reconciliation_registration_replays_full_queue(self):
        from lightfee.engine.recovery import _apply_journal_replay_to_state
        from lightfee.persistence.journal import replay_journal_records
        from lightfee.engine.state import EngineState

        reconciliation = {
            "position_id": "entry-replay-billing",
            "symbol": "ZILUSDT",
            "kind": "final",
            "closed_at_ms": 1000,
            "position_snapshot": {
                "position_id": "entry-replay-billing",
                "symbol": "ZILUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "matched_quantity": 1.0,
            },
            "long_legs": [],
            "short_legs": [],
            "reconciliation_mode": "venue_execution_history_required",
            "missing_close_order_identity": True,
            "billing_reconciliation_required": True,
        }
        records = [{
            "kind": "entry.opened",
            "ts_ms": 900,
            "payload": {
                "position_id": reconciliation["position_id"],
                "symbol": reconciliation["symbol"],
                "long_venue": "binance",
                "short_venue": "bybit",
            },
        }, {
            "kind": "exit.pending_close_reconciliation_registered",
            "ts_ms": 1000,
            "payload": {
                "position_id": reconciliation["position_id"],
                "symbol": reconciliation["symbol"],
                "live_flat_terminal": True,
                "reconciliation": reconciliation,
            },
        }]

        restored = EngineState()
        restored.open_positions[reconciliation["position_id"]] = object()
        _apply_journal_replay_to_state(restored, records)
        assert restored.pending_close_reconciliations == [reconciliation]
        assert reconciliation["position_id"] not in restored.open_positions

        replayed = replay_journal_records(records)
        assert reconciliation["position_id"] not in replayed["open_position_ids"]
        assert replayed["pending_close_reconciliation_count"] == 1
        assert replayed["pending_close_reconciliation_ids"] == [
            reconciliation["position_id"]
        ]

        legacy_records = [
            {
                "kind": "entry.opened",
                "ts_ms": 900,
                "payload": {
                    "position_id": reconciliation["position_id"],
                    "symbol": reconciliation["symbol"],
                },
            },
            {
                "kind": "exit.pending_close_reconciliation_registered",
                "ts_ms": 1000,
                "payload": {
                    "position_id": reconciliation["position_id"],
                    "symbol": reconciliation["symbol"],
                },
            },
            {
                "kind": "recovery.flat",
                "ts_ms": 1001,
                "payload": {"position_id": reconciliation["position_id"]},
            },
        ]
        legacy_replayed = replay_journal_records(legacy_records)
        assert legacy_replayed["open_position_ids"] == []
        assert legacy_replayed["pending_close_reconciliation_ids"] == [
            reconciliation["position_id"]
        ]

    def test_evidence_debt_replay_overrides_old_registration_and_accepts_stronger_task(self):
        from lightfee.engine.recovery import _apply_journal_replay_to_state
        from lightfee.engine.state import EngineState

        raw = {
            "position_id": "entry-evidence-debt-replay",
            "symbol": "BTCUSDT",
            "long_venue": "bybit",
            "short_venue": "okx",
            "kind": "final",
            "long_legs": [{"order_id": "long-close"}],
            "short_legs": [{"order_id": "short-close"}],
        }
        debt = {
            **raw,
            "reconciliation_status": "evidence_debt",
            "evidence_debt_reason": "missing_position_snapshot",
            "evidence_debt_at_ms": 1000,
        }
        records = [
            {
                "kind": "exit.pending_close_reconciliation_registered",
                "ts_ms": 900,
                "payload": {"reconciliation": raw},
            },
            {
                "kind": "exit.billing_evidence_debt_registered",
                "ts_ms": 1000,
                "payload": {"reconciliation": debt},
            },
        ]
        restored = EngineState()
        _apply_journal_replay_to_state(restored, records)
        assert restored.pending_close_reconciliations == [debt]

        restored.enqueue_pending_close_reconciliation({
            **raw,
            "position_snapshot": {
                "position_id": raw["position_id"],
                "symbol": raw["symbol"],
                "long_venue": raw["long_venue"],
                "short_venue": raw["short_venue"],
                "matched_quantity": 1.0,
            },
        })
        assert restored.pending_close_reconciliations[0].get(
            "reconciliation_status"
        ) is None

    def test_irrecoverable_history_debt_replays_without_reenabling_retry(self):
        """The one-shot history conclusion must survive a crash before snapshot."""
        from lightfee.engine.recovery import _apply_journal_replay_to_state
        from lightfee.engine.state import EngineState

        debt = {
            "position_id": "entry-history-terminal-replay",
            "symbol": "COTIUSDT",
            "kind": "final",
            "closed_at_ms": 1_780_000_000_000,
            "position_snapshot": {
                "position_id": "entry-history-terminal-replay",
                "symbol": "COTIUSDT",
                "long_venue": "bybit",
                "short_venue": "binance",
                "matched_quantity": 2400.0,
            },
            "long_legs": [],
            "short_legs": [],
            "reconciliation_status": "evidence_debt",
            "evidence_debt_reason": "missing_close_order_identity",
        }
        terminalized = {
            **debt,
            "automatic_history_terminal_status": "irrecoverable_audit_debt",
            "automatic_history_terminal_reason": "ambiguous_candidates",
            "automatic_history_terminalized_at_ms": 1_780_000_060_000,
            "next_attempt_ms": 0,
        }

        restored = EngineState()
        _apply_journal_replay_to_state(
            restored,
            [
                {
                    "kind": "exit.billing_evidence_debt_registered",
                    "ts_ms": 1_780_000_000_000,
                    "payload": {"reconciliation": debt},
                },
                {
                    "kind": "exit.billing_evidence_debt_irrecoverable",
                    "ts_ms": 1_780_000_060_000,
                    "payload": {"reconciliation": terminalized},
                },
            ],
        )

        assert restored.pending_close_reconciliations == [terminalized]

    def test_terminal_history_debt_cannot_be_replaced_by_ordinary_close_registration(self):
        """A repeated close observation must not reopen a terminal accounting owner."""
        from lightfee.engine.state import EngineState

        terminal = {
            "position_id": "entry-history-terminal-owner",
            "symbol": "ONGUSDT",
            "kind": "final",
            "closed_at_ms": 1_780_000_000_000,
            "position_snapshot": {
                "position_id": "entry-history-terminal-owner",
                "symbol": "ONGUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "matched_quantity": 149.0,
            },
            "long_legs": [],
            "short_legs": [],
            "reconciliation_status": "evidence_debt",
            "evidence_debt_reason": "missing_close_order_identity",
            "automatic_history_terminal_status": "irrecoverable_audit_debt",
            "automatic_history_terminal_reason": "ambiguous_candidates",
            "next_attempt_ms": 0,
        }
        repeated_observation = {
            **terminal,
            "reconciliation_status": None,
            "long_legs": [
                {"venue": "binance", "order_id": "late-observation"}
            ],
            "reconciliation_mode": "order_identity",
        }

        state = EngineState()
        state.enqueue_pending_close_reconciliation(terminal)
        state.enqueue_pending_close_reconciliation(repeated_observation)

        assert state.pending_close_reconciliations == [terminal]

    def test_external_recovery_reclassification_replays_as_debt_removal(self):
        """A crash after reclassification cannot resurrect external debt."""
        from lightfee.engine.recovery import _apply_journal_replay_to_state
        from lightfee.engine.state import EngineState
        from lightfee.persistence.journal import replay_journal_records

        debt = {
            "position_id": "live-recovered:CLUSDT:okx->bitget",
            "symbol": "CLUSDT",
            "kind": "final",
            "closed_at_ms": 1786542792764,
            "position_snapshot": {
                "position_id": "live-recovered:CLUSDT:okx->bitget",
                "symbol": "CLUSDT",
                "long_venue": "okx",
                "short_venue": "bitget",
                "entry_fee_evidence_complete": False,
            },
            "original_payload": {"client_order_ids": [], "order_ids": []},
            "long_legs": [],
            "short_legs": [],
            "reconciliation_status": "evidence_debt",
        }
        records = [
            {
                "kind": "exit.billing_evidence_debt_registered",
                "ts_ms": 1000,
                "payload": {"reconciliation": debt},
            },
            {
                "kind": "recovery.external_pair_flat_reclassified",
                "ts_ms": 2000,
                "payload": {
                    "position_id": debt["position_id"],
                    "kind": debt["kind"],
                    "closed_at_ms": debt["closed_at_ms"],
                    "accounting_owner": "external_unattributed",
                },
            },
        ]

        restored = EngineState()
        _apply_journal_replay_to_state(restored, records)
        assert restored.pending_close_reconciliations == []
        replayed = replay_journal_records(records)
        assert replayed["pending_close_reconciliation_count"] == 0
        assert "recovery.external_pair_flat_reclassified" in {
            record["kind"] for record in replayed["timeline"]
        }

    def test_reclassification_replay_preserves_identified_v2_debt(self):
        """An audit event alone cannot erase a durable V2 order identity."""
        from lightfee.engine.recovery import _apply_journal_replay_to_state
        from lightfee.engine.state import EngineState

        debt = {
            "position_id": "live-recovered:CLUSDT:okx->bitget",
            "symbol": "CLUSDT",
            "kind": "final",
            "closed_at_ms": 1786542792764,
            "position_snapshot": {
                "position_id": "live-recovered:CLUSDT:okx->bitget",
                "symbol": "CLUSDT",
                "long_venue": "okx",
                "short_venue": "bitget",
                "entry_fee_evidence_complete": False,
            },
            "original_payload": {"client_order_ids": [], "order_ids": []},
            "long_legs": [{"client_order_id": "v2-close-cid"}],
            "short_legs": [],
            "reconciliation_status": "evidence_debt",
        }
        records = [
            {
                "kind": "exit.billing_evidence_debt_registered",
                "ts_ms": 1000,
                "payload": {"reconciliation": debt},
            },
            {
                "kind": "recovery.external_pair_flat_reclassified",
                "ts_ms": 2000,
                "payload": {
                    "position_id": debt["position_id"],
                    "kind": debt["kind"],
                    "closed_at_ms": debt["closed_at_ms"],
                    "accounting_owner": "external_unattributed",
                },
            },
        ]

        restored = EngineState()
        _apply_journal_replay_to_state(restored, records)
        assert restored.pending_close_reconciliations == [debt]

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

    def test_compact_reconciliation_count_blocks_when_summary_is_absent(self):
        from scripts import diagnose_live as dl

        s = _dg_state(pending_close_reconciliation_count=1)
        r = dl._build_production_acceptance_gate([], s, {"available": True, "venues": {}})
        assert "local_pending_entries_or_closes_present" in r["blocking_reasons"]
        assert "pending_close_reconciliations_not_empty" in r["blocking_reasons"]
        assert r["pending_close_reconciliation_total"] == 1

    def test_mixed_snapshot_sources_fail_closed(self):
        """An empty queue must not erase a nonzero durable summary."""
        from lightfee.engine.recovery_decision_core import pending_close_owner_counts

        counts = pending_close_owner_counts({
            "pending_close_reconciliations": [],
            "pending_close_reconciliation_count": 0,
            "pending_close_reconciliation_summary": {"total_count": 4},
        })

        assert counts.pending_close_reconciliation_count == 4
        assert counts.pending_close_owner_count == 4

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

    def test_external_recovery_audit_kind_mappings(self):
        from lightfee.engine.v1_lifecycle_closure import (
            _EVENT_KIND_PHASES,
            V1LifecycleClosurePhase,
        )

        phase = V1LifecycleClosurePhase.RECOVERY_TRUTH.value
        for key in (
            "recovery.external_pair_flat_observed",
            "recovery.external_pair_flat_reclassified",
        ):
            assert _EVENT_KIND_PHASES[key] == phase
