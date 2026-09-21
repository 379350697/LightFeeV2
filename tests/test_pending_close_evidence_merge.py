"""CL-154 v2 production-path regressions: executor close evidence ownership.

Circuit-breaker attribution rules (2026-09-16 review):
- executor evidence merges into the single ``final`` owner only; a coexisting
  partial owner keeps its own immutable segment and legs byte-identical;
- the registration boundary absorbs same-position executor evidence so the
  final owner is born truthful (never from a mid-writeback position read);
- the orphan catch-all merge stays idempotent behind the registration
  absorption, and unavailable lookups keep the owner fail-closed.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from lightfee.core.domain import OrderFill, Side, Venue
from lightfee.engine.close_runtime import CloseRuntime
from lightfee.engine.passive_close import PassiveCloseExecutor
from lightfee.engine.pending_entry_runtime import PendingEntryRuntime
from lightfee.engine.state import (
    CloseLegRecord,
    EngineState,
    OpenPosition,
    PendingClose,
    PendingPassiveClose,
    PersistedCloseExecutionLeg,
)
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


POSITION_ID = "entry-1-LSKUSDT"


class _FakeFill:
    def __init__(self, **kw: Any):
        self.quantity = kw.pop("quantity")
        self.fee_quote = kw.pop("fee_quote")
        self.price = kw.pop("price")
        self.order_id = kw.pop("order_id")
        self.client_order_id = kw.pop("client_order_id")
        self.venue = kw.pop("venue", "")
        self.side = kw.pop("side", None)
        self.symbol = kw.pop("symbol", "LSK_USDT")
        self.filled_at_ms = kw.pop("filled_at_ms", 0)
        self.metadata = kw.pop("metadata", {}) or {}


class _Adapter:
    def __init__(self, results: dict[tuple[str, str], _FakeFill]):
        self.results = results
        self.calls: list[tuple[str, str]] = []

    async def fetch_order_fill_reconciliation(
        self, symbol: str, order_id: str, client_order_id: str
    ):
        self.calls.append((order_id, client_order_id))
        return self.results.get((order_id, client_order_id))

    async def discover_historical_close_fill_reconciliation(
        self, *, symbol, side, position_side, quantity, closed_at_ms
    ):
        from lightfee.core.domain import HistoricalCloseEvidenceDiscovery

        fill = self.results.get(("discovered", side.value.upper()))
        if fill is None:
            return HistoricalCloseEvidenceDiscovery(
                classification="gate_my_trades_no_candidate", candidate_count=0
            )
        return HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=fill,
        )


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


class _Ctx(CloseRuntime):
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
    def venue_adapters(self):
        return self._venue_adapters

    def _flush_adapter_order_diagnostics(self, adapter) -> None:
        return None

    async def _fetch_pending_close_terminal_live_sizes(self, **kw):
        return self._terminal_sizes

    async def _fetch_pending_close_terminal_live_flat_truth(self, **kw):
        if self._terminal_sizes is None:
            return None, "terminal truth unavailable"
        return self._terminal_sizes, None


def _final_owner() -> dict[str, Any]:
    """The deployed 03:05 shape: passive zero-fill identities only, and the
    mid-writeback owned segment missing the already-executed short."""
    return {
        "position_id": POSITION_ID,
        "symbol": "LSKUSDT",
        "kind": "final",
        "reason": "funding_capture",
        "source": "fallback_live_balanced_matched_close_flat_probe",
        "closed_at_ms": 1_000_000,
        "created_cycle": 10,
        "owned_close_quantities": {"long": 62.0, "short": 0.0},
        "position_snapshot": {
            "position_id": POSITION_ID,
            "symbol": "LSKUSDT",
            "long_venue": "binance",
            "short_venue": "bybit",
            "long_quantity": 62.0,
            "short_quantity": 0.0,
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
                "order_id": "2678659923",
                "client_order_id": "lfex-maker-cancelled",
                "quantity": 0.0,
                "average_price": 0.0,
                "fee_quote": None,
            }
        ],
        "short_legs": [],
        "attempt_count": 0,
        "next_attempt_ms": 0,
    }


def _partial_owner() -> dict[str, Any]:
    """A lawful coexisting partial owner with its own settled segment."""
    return {
        "position_id": POSITION_ID,
        "symbol": "LSKUSDT",
        "kind": "partial",
        "reason": "funding_capture",
        "source": "active_close",
        "closed_at_ms": 900_000,
        "created_cycle": 9,
        "owned_close_quantities": {"long": 20.0, "short": 20.0},
        "position_snapshot": {
            "position_id": POSITION_ID,
            "symbol": "LSKUSDT",
            "long_venue": "binance",
            "short_venue": "bybit",
        },
        "long_legs": [
            {
                "venue": "binance",
                "order_id": "partial-long-1",
                "client_order_id": "lfex-partial-long",
                "quantity": 20.0,
                "average_price": 0.39,
                "fee_quote": 0.002,
            }
        ],
        "short_legs": [
            {
                "venue": "bybit",
                "order_id": "partial-short-1",
                "client_order_id": "lfex-partial-short",
                "quantity": 20.0,
                "average_price": 0.395,
                "fee_quote": 0.002,
            }
        ],
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
    owners: list[dict[str, Any]],
    *,
    adapter_results: dict[tuple[str, str], _FakeFill] | None = None,
    terminal_sizes: tuple[float, float] | None = None,
    with_pending_close: bool = True,
):
    state = EngineState()
    state.lifecycle = EngineLifecycle.RUNNING
    state.risk_mode = GlobalRiskMode.RUNNING
    state.tick_count = 11
    state.pending_close_reconciliations = owners
    if with_pending_close:
        state.pending_closes = {"close-entry-1": _executor_pending()}
    journal = _Journal()
    adapter = _Adapter(adapter_results or {})
    ctx = _Ctx(state, journal, {Venue.BINANCE: adapter, Venue.BYBIT: adapter}, terminal_sizes)
    return ctx, state, journal, adapter


@pytest.mark.asyncio
async def test_merge_never_touches_a_coexisting_partial_owner():
    """Reviewer repro: executor evidence must land on the final owner only;
    the partial's immutable segment and legs stay byte-identical."""
    partial = _partial_owner()
    final = _final_owner()
    partial_before = json.dumps(partial, sort_keys=True)
    ctx, state, journal, _ = _harness(owners=[partial, final])
    runtime = PendingEntryRuntime(ctx)

    await runtime._reconcile_pending_state(1_000_100)

    assert "reconciliation.pending_close_orphaned" in journal.kinds()
    assert "reconciliation.pending_close_evidence_merged" in journal.kinds()
    assert json.dumps(partial, sort_keys=True) == partial_before
    final_long = {leg["order_id"] for leg in final["long_legs"]}
    assert "2679230467" in final_long
    partial_long = {leg["order_id"] for leg in partial["long_legs"]}
    assert partial_long == {"partial-long-1"}
    assert final["owned_close_quantities"] == {"long": 62.0, "short": 62.0}
    assert partial["owned_close_quantities"] == {"long": 20.0, "short": 20.0}


@pytest.mark.asyncio
async def test_registered_final_and_next_billing_cycle_settle():
    """Full path: merged identities let the next billing cycle settle
    `exit.reconciled` with venue-complete evidence."""
    final = _final_owner()
    ctx, state, journal, adapter = _harness(owners=[final])
    runtime = PendingEntryRuntime(ctx)
    await runtime._reconcile_pending_state(1_000_100)

    adapter.results = {
        ("2679230467", "lfex-taker-long"): _FakeFill(
            quantity=62.0, fee_quote=0.003, price=0.397,
            order_id="2679230467", client_order_id="lfex-taker-long",
        ),
        ("cfd5d547-2e1b", "lfex-taker-short"): _FakeFill(
            quantity=62.0, fee_quote=0.004, price=0.39705,
            order_id="cfd5d547-2e1b", client_order_id="lfex-taker-short",
        ),
    }
    await runtime._reconcile_pending_state(1_060_100)

    reconciled = [p for k, p in journal.events if k == "exit.reconciled"]
    assert reconciled and reconciled[0]["venue_statement_reconciled"] is True
    assert state.pending_close_reconciliations == []


@pytest.mark.asyncio
async def test_unavailable_lookup_after_merge_stays_fail_closed():
    final = _final_owner()
    ctx, state, journal, _ = _harness(
        owners=[final], terminal_sizes=(0.0, 0.0)
    )
    runtime = PendingEntryRuntime(ctx)
    await runtime._reconcile_pending_state(1_000_100)
    await runtime._reconcile_pending_state(1_060_100)

    assert "exit.reconciled" not in journal.kinds()
    owner = state.pending_close_reconciliations[0]
    assert owner.get("reconciliation_status") == "evidence_debt"


def _registration_fixture():
    """Real passive-close registration inputs: the mid-writeback position
    (short already deducted) plus the same-position executor PendingClose."""
    state = EngineState()
    state.lifecycle = EngineLifecycle.RUNNING
    state.risk_mode = GlobalRiskMode.RUNNING
    state.tick_count = 11
    journal = _Journal()
    adapters = {Venue.BINANCE: _Adapter({}), Venue.BYBIT: _Adapter({})}
    executor = PassiveCloseExecutor(adapters=adapters, journal=journal)
    pending = PendingPassiveClose(
        position_id=POSITION_ID,
        reason="funding_capture",
        target_quantity=62.0,
        chunk_quantities=[62.0],
        long_legs=[
            PersistedCloseExecutionLeg(
                fill=None, client_order_id="lfex-maker-cancelled"
            )
        ],
    )
    position = OpenPosition(
        position_id=POSITION_ID,
        symbol="LSKUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        long_quantity=62.0,
        short_quantity=0.0,
        long_entry_price=0.39,
        short_entry_price=0.395,
        matched_quantity=62.0,
        opened_at_ms=900_000,
    )
    state.pending_closes = {"close-entry-1": _executor_pending()}
    return state, journal, executor, pending, position


def test_registration_boundary_absorbs_same_position_executor_evidence():
    """v2a: `_register_close_reconciliation_after_live_flat` must absorb the
    same-position executor PendingClose so the final owner is born truthful
    (owned short 0 -> 62 with the executed legs) instead of mid-writeback."""
    state, journal, executor, pending, position = _registration_fixture()

    registered = executor._register_close_reconciliation_after_live_flat(
        state,
        pending,
        position,
        source="fallback_live_balanced_matched_close_flat_probe",
        payload={},
        extra=None,
    )

    assert registered is True
    owner = state.pending_close_reconciliations[-1]
    assert owner["position_id"] == POSITION_ID
    assert owner["owned_close_quantities"] == {"long": 62.0, "short": 62.0}
    long_ids = {leg["order_id"] for leg in owner["long_legs"]}
    assert "2679230467" in long_ids
    short_ids = {leg["order_id"] for leg in owner["short_legs"]}
    assert "cfd5d547-2e1b" in short_ids
    assert "exit.pending_close_reconciliation_registered" in journal.kinds()


class _DiscoveringAdapter(_Adapter):
    """Adapter with fill reconciliation plus unique-window discovery."""

    async def fetch_position(self, symbol: str):
        class _Pos:
            quantity = 0.0

        return _Pos()

    async def discover_historical_close_fill_reconciliation(
        self, *, symbol, side, position_side, quantity, closed_at_ms
    ):
        from lightfee.core.domain import HistoricalCloseEvidenceDiscovery

        return HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=self.results.get(("discovered", side.value)),
        )


def _phantom_debt() -> dict[str, Any]:
    owner = _final_owner()
    owner["attempt_count"] = 0
    return owner


def _grant_ctx(terminal_sizes=(0.0, 0.0), adapter_results=None):
    """Pre-merge phantom shape: passive identities only, no executor merge,
    two clean all-no-fill cycles already served."""
    final = _final_owner()
    final["attempt_count"] = 2
    ctx, state, journal, adapter = _harness(
        owners=[final],
        adapter_results=adapter_results,
        terminal_sizes=terminal_sizes,
        with_pending_close=False,
    )
    return ctx, state, journal, adapter, final


@pytest.mark.asyncio
async def test_all_no_fill_debt_arms_one_time_discovery_grant():
    """v2c: with two clean all-no-fill cycles and provably flat terminal
    truth, the dead exact-retry loop reclassifies once into the bounded
    unique-history discovery path; the mid-writeback segment corrects from
    the snapshot's matched quantity."""
    ctx, state, journal, adapter, final = _grant_ctx()
    runtime = PendingEntryRuntime(ctx)

    await runtime._reconcile_pending_state(1_000_100)

    assert "reconciliation.discovery_grant_armed" in journal.kinds()
    owner = state.pending_close_reconciliations[0]
    assert owner["all_no_fill_discovery_grant"] is True
    assert owner["owned_close_quantities"] == {"long": 62.0, "short": 62.0}
    assert owner["evidence_debt_reason"] == "missing_close_order_identity"
    assert owner["reconciliation_status"] == "evidence_debt"
    # The corrected segment demands both legs: the invalid loop is over.
    assert "reconciliation.pending_close_reconciliation_invalid" in journal.kinds()


@pytest.mark.asyncio
async def test_granted_debt_settles_via_unique_history_discovery():
    """After the grant, the machinery's unique-window discovery finds both
    executions and the fee-complete exact rechecks settle the debt."""
    ctx, state, journal, adapter, final = _grant_ctx(
        adapter_results={
            # Unique-window discoveries: the long leg's stored identity is a
            # cancelled maker (no fill), so both legs are discovered.
            ("discovered", "SELL"): _FakeFill(
                quantity=62.0, fee_quote=0.003, price=0.397,
                order_id="2679230467", client_order_id="lfex-taker-long",
                venue=Venue.BINANCE, side=Side.SELL,
                metadata={
                    "fee_evidence_complete": True,
                    "historical_evidence_provenance": "exchange_execution_unattributed",
                },
            ),
            ("discovered", "BUY"): _FakeFill(
                quantity=62.0, fee_quote=0.004, price=0.39705,
                order_id="cfd5d547-2e1b", client_order_id="lfex-taker-short",
                venue=Venue.BYBIT, side=Side.BUY,
                metadata={
                    "fee_evidence_complete": True,
                    "historical_evidence_provenance": "exchange_execution_unattributed",
                },
            ),
        }
    )
    runtime = PendingEntryRuntime(ctx)

    await runtime._reconcile_pending_state(1_000_100)
    await runtime._reconcile_pending_state(1_000_200)

    reconciled = [p for k, p in journal.events if k == "exit.reconciled"]
    assert reconciled and reconciled[0]["venue_statement_reconciled"] is True
    assert state.pending_close_reconciliations == []


@pytest.mark.asyncio
async def test_grant_requires_flat_terminal_truth():
    ctx, state, journal, _ = _harness(
        owners=[_final_owner()], terminal_sizes=None
    )
    runtime = PendingEntryRuntime(ctx)
    for now in (1_000_100, 1_030_200, 1_060_300):
        await runtime._reconcile_pending_state(now)
    assert "reconciliation.discovery_grant_armed" not in journal.kinds()
    owner = state.pending_close_reconciliations[0]
    assert owner.get("all_no_fill_discovery_grant") is None
    assert "exit.reconciled" not in journal.kinds()
