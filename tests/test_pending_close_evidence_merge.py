"""Production-path regression: executor close legs must reach the billing owner.

Production shape (LSKUSDT 2026-09-16, entry-1789498326920): the passive-close
deadline fallback terminalized the position through taker legs while the
passive-phase reconciliation record held only zero-fill maker identities.  The
CloseExecutor's uncertain-leg PendingClose — the only durable record of the
executed order identities — was orphaned on the next reconciliation pass, so
the billing owner retried `close_order_lookup_returned_no_fill` forever.
"""

from __future__ import annotations

from typing import Any

import pytest

from lightfee.core.domain import Venue
from lightfee.engine.close_runtime import CloseRuntime
from lightfee.engine.pending_entry_runtime import PendingEntryRuntime
from lightfee.engine.state import CloseLegRecord, EngineState, PendingClose
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


POSITION_ID = "entry-1-LSKUSDT"


class _FakeFill:
    """OrderFillReconciliation-shaped adapter result."""

    def __init__(self, **kw: Any):
        self.quantity = kw.pop("quantity")
        self.fee_quote = kw.pop("fee_quote")
        self.price = kw.pop("price")
        self.order_id = kw.pop("order_id")
        self.client_order_id = kw.pop("client_order_id")
        self.venue = kw.pop("venue", "")
        self.filled_at_ms = kw.pop("filled_at_ms", 0)
        self.metadata = kw.pop("metadata", {}) or {}


class _Adapter:
    """Venue adapter stub resolving exact close identities to fills."""

    def __init__(self, results: dict[tuple[str, str], _FakeFill]):
        self.results = results
        self.calls: list[tuple[str, str]] = []

    async def fetch_order_fill_reconciliation(
        self, symbol: str, order_id: str, client_order_id: str
    ):
        self.calls.append((order_id, client_order_id))
        return self.results.get((order_id, client_order_id))


class _Journal:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def append(self, kind: str, payload: dict[str, Any]) -> None:
        self.events.append((kind, payload))

    def append_critical(self, _ts_ms: int, kind: str, payload: dict[str, Any]) -> None:
        self.events.append((kind, payload))

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.events]


class _Reconciler:
    def __init__(self) -> None:
        self.calls = 0

    async def reconcile_position(self, **kw: Any):
        self.calls += 1

        class _Result:
            is_flat = True

        return _Result()


class _FallbackCtx(CloseRuntime):
    """Minimal engine-shaped context: CloseRuntime bound to itself."""

    def __init__(
        self,
        state: EngineState,
        journal: _Journal,
        adapters: dict[Venue, _Adapter],
        terminal_sizes: tuple[float, float] | None,
    ):
        super().__init__(self)
        self.state = state
        self.journal = journal
        self._venue_adapters = adapters
        self.reconciler = _Reconciler()

        class _Config:
            class runtime:
                mode = "live"

        self.config = _Config()
        self._terminal_sizes = terminal_sizes

    @property
    def venue_adapters(self) -> dict[Venue, _Adapter]:
        return self._venue_adapters

    def _flush_adapter_order_diagnostics(self, adapter) -> None:
        return None

    async def _fetch_pending_close_terminal_live_sizes(self, **kw):
        return self._terminal_sizes


def _reconciliation() -> dict[str, Any]:
    """The deployed 03:05 record shape: passive zero-fill identities only."""
    return {
        "position_id": POSITION_ID,
        "symbol": "LSKUSDT",
        "kind": "final",
        "reason": "funding_capture",
        "source": "fallback_live_balanced_matched_close_flat_probe",
        "closed_at_ms": 1_000_000,
        "created_cycle": 10,
        # Production owned segment: the fallback registration snapshotted the
        # position mid-writeback, so the already-executed short reads as 0.
        "owned_close_quantities": {"long": 62.0, "short": 0.0},
        "position_snapshot": {
            "position_id": POSITION_ID,
            "symbol": "LSKUSDT",
            "long_venue": "binance",
            "short_venue": "bybit",
            "long_quantity": 62.0,
            "short_quantity": 62.0,
            "matched_quantity": 62.0,
            "long_entry_price": 0.39,
            "short_entry_price": 0.395,
            "long_entry_fee_quote": 0.004,
            "short_entry_fee_quote": 0.004,
            "total_entry_fee_quote": 0.008,
            "entry_fee_evidence_complete": True,
            "captured_funding_quote": 0.01,
            "second_stage_funding_quote": 0.0,
            "opened_at_ms": 900_000,
        },
        "long_legs": [
            {
                "venue": "binance",
                "order_id": "",
                "client_order_id": "lfex-no-ack-one",
                "quantity": 0.0,
                "average_price": 0.0,
                "fee_quote": None,
            },
            {
                "venue": "binance",
                "order_id": "",
                "client_order_id": "lfex-no-ack-two",
                "quantity": 0.0,
                "average_price": 0.0,
                "fee_quote": None,
            },
            {
                "venue": "binance",
                "order_id": "2678659923",
                "client_order_id": "lfex-maker-cancelled",
                "quantity": 0.0,
                "average_price": 0.0,
                "fee_quote": None,
            },
        ],
        "short_legs": [],
        "attempt_count": 0,
        "next_attempt_ms": 0,
    }


def _executor_pending() -> PendingClose:
    return PendingClose(
        close_id="close-entry-1",
        position_id=POSITION_ID,
        reason="funding_capture",
        created_at_ms=1_000_050,
        long_uncertain=True,
        short_uncertain=True,
        long_closed=62.0,
        short_closed=62.0,
        long_legs=[
            CloseLegRecord(
                venue="binance",
                order_id="2679230467",
                client_order_id="lfex-taker-long",
                quantity=62.0,
                average_price=0.397,
                fee_quote=0.003,
            )
        ],
        short_legs=[
            CloseLegRecord(
                venue="bybit",
                order_id="cfd5d547-2e1b",
                client_order_id="lfex-taker-short",
                quantity=62.0,
                average_price=0.39705,
                fee_quote=0.004,
            )
        ],
    )


def _harness(
    *,
    reconciliation: dict[str, Any] | None,
    adapter_results: dict[tuple[str, str], _FakeFill] | None = None,
    terminal_sizes: tuple[float, float] | None = None,
) -> tuple[_FallbackCtx, EngineState, _Journal]:
    state = EngineState()
    state.lifecycle = EngineLifecycle.RUNNING
    state.risk_mode = GlobalRiskMode.RUNNING
    state.tick_count = 11
    if reconciliation is not None:
        state.pending_close_reconciliations = [reconciliation]
    state.pending_closes = {"close-entry-1": _executor_pending()}
    journal = _Journal()
    adapter = _Adapter(adapter_results or {})
    ctx = _FallbackCtx(
        state,
        journal,
        {Venue.BINANCE: adapter, Venue.BYBIT: adapter},
        terminal_sizes,
    )
    return ctx, state, journal


@pytest.mark.asyncio
async def test_orphaned_executor_close_hands_legs_to_billing_owner_and_settles():
    """Merge at the orphan boundary, then the next billing cycle reconciles."""
    ctx, state, journal = _harness(reconciliation=_reconciliation())
    runtime = PendingEntryRuntime(ctx)

    # Tick 1: position already terminal -> the executor PendingClose is
    # orphaned, and its executed legs must land in the reconciliation owner.
    await runtime._reconcile_pending_state(1_000_100)

    assert "reconciliation.pending_close_orphaned" in journal.kinds()
    owner = state.pending_close_reconciliations[0]
    long_identities = {
        (leg["order_id"], leg["client_order_id"]) for leg in owner["long_legs"]
    }
    short_identities = {
        (leg["order_id"], leg["client_order_id"]) for leg in owner["short_legs"]
    }
    assert ("2679230467", "lfex-taker-long") in long_identities
    assert ("cfd5d547-2e1b", "lfex-taker-short") in short_identities
    # The executor's proven close totals upgrade the mid-writeback segment.
    assert owner["owned_close_quantities"] == {"long": 62.0, "short": 62.0}
    merged = [
        payload
        for kind, payload in journal.events
        if kind == "reconciliation.pending_close_evidence_merged"
    ]
    # Two executed legs plus the short-owned-quantity upgrade.
    assert merged and merged[0]["merged_leg_count"] == 3
    invalid_after_tick1 = sum(
        1
        for kind, _ in journal.events
        if kind == "reconciliation.pending_close_reconciliation_invalid"
    )

    # Tick 2: the billing recheck now finds both executed fills.
    binance_fill = _FakeFill(
        quantity=62.0,
        fee_quote=0.003,
        price=0.397,
        order_id="2679230467",
        client_order_id="lfex-taker-long",
    )
    bybit_fill = _FakeFill(
        quantity=62.0,
        fee_quote=0.004,
        price=0.39705,
        order_id="cfd5d547-2e1b",
        client_order_id="lfex-taker-short",
    )
    shared = ctx._venue_adapters[Venue.BINANCE]
    shared.results = {
        ("2679230467", "lfex-taker-long"): binance_fill,
        ("cfd5d547-2e1b", "lfex-taker-short"): bybit_fill,
    }

    await runtime._reconcile_pending_state(1_060_100)

    reconciled = [
        payload
        for kind, payload in journal.events
        if kind == "exit.reconciled"
    ]
    assert reconciled and reconciled[0]["venue_statement_reconciled"] is True
    assert reconciled[0]["long_closed_qty"] == 62.0
    assert reconciled[0]["short_closed_qty"] == 62.0
    assert state.pending_close_reconciliations == []
    # The invalid-retry loop must not fire again for the settled owner.
    invalid_total = sum(
        1
        for kind, _ in journal.events
        if kind == "reconciliation.pending_close_reconciliation_invalid"
    )
    assert invalid_total == invalid_after_tick1


@pytest.mark.asyncio
async def test_orphan_without_billing_owner_stays_a_pure_no_op():
    """No reconciliation owner for the position -> merge adds nothing."""
    ctx, state, journal = _harness(reconciliation=None)
    runtime = PendingEntryRuntime(ctx)

    await runtime._reconcile_pending_state(1_000_100)

    assert "reconciliation.pending_close_orphaned" in journal.kinds()
    assert "reconciliation.pending_close_evidence_merged" not in journal.kinds()
    assert state.pending_close_reconciliations == []


@pytest.mark.asyncio
async def test_unavailable_fill_lookup_after_merge_stays_fail_closed():
    """Merged identities the venue cannot prove must retain the owner, not settle."""
    ctx, state, journal = _harness(
        reconciliation=_reconciliation(),
        terminal_sizes=(0.0, 0.0),
    )
    runtime = PendingEntryRuntime(ctx)

    await runtime._reconcile_pending_state(1_000_100)
    await runtime._reconcile_pending_state(1_060_100)

    assert "exit.reconciled" not in journal.kinds()
    owner = state.pending_close_reconciliations[0]
    assert owner.get("reconciliation_status") == "evidence_debt"
    assert owner.get("evidence_debt_reason") == (
        "known_close_fill_temporarily_unavailable"
    )
