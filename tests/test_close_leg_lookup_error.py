"""Production-path regression: a venue reconciliation lookup failure must
degrade to evidence-unavailable, never kill the runtime.

Production incident 2026-09-19 00:25 CST: the Bybit API key expired
(retCode=33004) while two LSKUSDT final debts with positive-quantity bybit
legs were pending. `fetch_order_fill_reconciliation` raised TransportError,
`_fetch_close_leg_reconciliations` let it propagate, and every
`_reconcile_pending_state` pass killed the process — a 2,370-restart crash
loop that also froze the missing-hedge drive for a stranded gate long.
"""

from __future__ import annotations

from typing import Any

import pytest

from lightfee.core.domain import Venue
from lightfee.engine.close_runtime import CloseRuntime
from lightfee.engine.state import EngineState
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.venues.transport import TransportError, TransportErrorCategory


POSITION_ID = "entry-1-LSKUSDT"


class _ExpiredKeyAdapter:
    """Bybit adapter whose every reconciliation lookup raises retCode=33004."""

    def __init__(self) -> None:
        self.calls = 0

    async def fetch_order_fill_reconciliation(
        self, symbol: str, order_id: str, client_order_id: str
    ):
        self.calls += 1
        raise TransportError(
            TransportErrorCategory.REQUEST_REJECTED,
            "bybit execution reconciliation: bybit retCode=33004 "
            "retMsg=Your api key has expired.",
        )

    async def fetch_position(self, symbol: str):
        class _Pos:
            quantity = 0.0

        return _Pos()


class _Journal:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def append(self, kind: str, payload: dict[str, Any]) -> None:
        self.events.append((kind, payload))

    def append_critical(self, _ts_ms: int, kind: str, payload: dict[str, Any]) -> None:
        self.events.append((kind, payload))


class _Ctx(CloseRuntime):
    """Minimal engine-shaped context binding CloseRuntime to itself."""

    def __init__(self, state: EngineState, journal: _Journal, adapter) -> None:
        super().__init__(self)
        self.state = state
        self.journal = journal
        self._venue_adapters = {Venue.BYBIT: adapter}
        self._terminal_sizes = (0.0, 0.0)

        class _Config:
            class runtime:
                mode = "live"

        self.config = _Config()

    @property
    def venue_adapters(self):
        return self._venue_adapters

    def _flush_adapter_order_diagnostics(self, adapter) -> None:
        return None

    async def _fetch_pending_close_terminal_live_sizes(self, **kw):
        return self._terminal_sizes


def _owner_with_positive_bybit_leg() -> dict[str, Any]:
    return {
        "position_id": POSITION_ID,
        "symbol": "LSKUSDT",
        "kind": "final",
        "reason": "funding_capture",
        "source": "aggressive_close_execution",
        "closed_at_ms": 1_000_000,
        "created_cycle": 10,
        "owned_close_quantities": {"long": 22.0, "short": 22.0},
        "position_snapshot": {
            "position_id": POSITION_ID,
            "symbol": "LSKUSDT",
            "long_venue": "bybit",
            "short_venue": "bybit",
            "long_quantity": 22.0,
            "short_quantity": 22.0,
            "matched_quantity": 22.0,
            "long_entry_price": 0.79,
            "short_entry_price": 0.82,
            "total_entry_fee_quote": 0.01,
            "entry_fee_evidence_complete": True,
            "captured_funding_quote": 0.0,
            "second_stage_funding_quote": 0.0,
            "opened_at_ms": 900_000,
        },
        "long_legs": [
            {
                "venue": "bybit",
                "order_id": "fda2d7a3-0b01",
                "client_order_id": "lfxs-long",
                "quantity": 22.0,
                "average_price": 0.82,
                "fee_quote": None,
            }
        ],
        "short_legs": [
            {
                "venue": "bybit",
                "order_id": "fda2d7a3-0b02",
                "client_order_id": "lfxs-short",
                "quantity": 22.0,
                "average_price": 0.82,
                "fee_quote": None,
            }
        ],
        "attempt_count": 0,
        "next_attempt_ms": 0,
    }


def _harness():
    state = EngineState()
    state.lifecycle = EngineLifecycle.RUNNING
    state.risk_mode = GlobalRiskMode.RUNNING
    state.tick_count = 11
    state.pending_close_reconciliations = [_owner_with_positive_bybit_leg()]
    journal = _Journal()
    adapter = _ExpiredKeyAdapter()
    ctx = _Ctx(state, journal, adapter)
    return ctx, state, journal, adapter


@pytest.mark.asyncio
async def test_expired_key_lookup_error_degrades_instead_of_crashing():
    """The billing pass must survive a venue lookup failure: journal the
    typed error, keep the owner fail-closed (evidence debt), and never
    propagate the exception into the runtime loop."""
    ctx, state, journal, adapter = _harness()
    await ctx._process_pending_close_reconciliations(1_000_100)

    errors = [
        payload
        for kind, payload in journal.events
        if kind == "reconciliation.close_leg_lookup_error"
    ]
    assert len(errors) == 2
    assert "33004" in errors[0]["error"]
    assert adapter.calls >= 2
    owner = state.pending_close_reconciliations[0]
    assert owner.get("reconciliation_status") == "evidence_debt"
    assert not any(
        kind == "exit.reconciled" for kind, _ in journal.events
    )
