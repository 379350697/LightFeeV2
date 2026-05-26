from __future__ import annotations

import json
from pathlib import Path

import pytest

from lightfee.core.domain import (
    PassiveOrderAck,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import PendingEntry, PendingPassiveOrder
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from tests.test_live_full_closure import make_test_config


pytestmark = pytest.mark.live_harness

FIXTURE = Path("tests/fixtures/live_incidents/2026-05-27/passive_maker_zero_fill.jsonl")


class _ZeroFillMakerAdapter:
    def __init__(self, venue: Venue, progress_state: PassiveOrderState) -> None:
        self.venue = venue
        self.progress_state = progress_state
        self.cancel_calls: list[dict[str, object]] = []
        self.submit_passive_order_calls: list[object] = []
        self.query_calls: list[dict[str, object]] = []

    async def query_passive_order_progress(
        self,
        symbol: str,
        order_id: str,
        client_order_id: str | None = None,
        **_kwargs,
    ) -> PassiveOrderProgress:
        self.query_calls.append(
            {
                "symbol": symbol,
                "order_id": order_id,
                "client_order_id": client_order_id,
                "state": self.progress_state.value,
            }
        )
        return PassiveOrderProgress(
            venue=self.venue,
            symbol=symbol,
            side=_kwargs.get("side", Side.BUY),
            order_id=order_id,
            client_order_id=client_order_id or "",
            state=self.progress_state,
            cumulative_quantity=0.0,
            average_price=0.0,
            observed_at_ms=1779816047600,
        )

    async def cancel_passive_order(
        self,
        symbol: str,
        order_id: str,
        client_order_id: str | None = None,
    ) -> None:
        self.cancel_calls.append(
            {
                "symbol": symbol,
                "order_id": order_id,
                "client_order_id": client_order_id,
            }
        )
        self.progress_state = PassiveOrderState.CANCELED

    async def submit_passive_order(self, request) -> PassiveOrderAck:
        self.submit_passive_order_calls.append(request)
        self.progress_state = PassiveOrderState.OPEN
        return PassiveOrderAck(
            venue=self.venue,
            symbol=request.symbol,
            side=request.side,
            order_id=f"repost-{len(self.submit_passive_order_calls)}",
            client_order_id=request.client_order_id or f"repost-cid-{len(self.submit_passive_order_calls)}",
            state=PassiveOrderState.OPEN,
            quantity=request.quantity,
            price=request.price or 0.0101,
            accepted_at_ms=1779816048100,
        )

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1779816047600,
        )

    async def fetch_open_orders(self, symbol: str | None = None) -> list[object]:
        return []

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: str | None = None,
    ):
        return None


def _events() -> list[dict]:
    return [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]


def _payload(kind: str) -> dict:
    return next(event["payload"] for event in _events() if event["kind"] == kind)


def _pending_from_fixture() -> PendingEntry:
    selected = _payload("entry.selected")
    submitted = _payload("order.passive_submitted")
    return PendingEntry(
        pending_id=selected["entry_id"],
        symbol=selected["symbol"],
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        target_quantity=float(selected["target_quantity"]),
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=int(_events()[0]["ts_ms"]),
        maker_order_id=submitted["order_id"],
        maker_client_order_id=submitted["client_order_id"],
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        uncertain_outcome=True,
        entry_type="passive_incremental",
        maker_leg="long",
        maker_price=float(submitted["price"]),
        long_quantity=float(submitted["quantity"]),
        short_quantity=float(submitted["quantity"]),
        repost_count=1,
        zero_fill_since_ms=int(submitted["accepted_at_ms"]),
        passive_order=PendingPassiveOrder(
            order_id=submitted["order_id"],
            client_order_id=submitted["client_order_id"],
            limit_price=float(submitted["price"]),
            target_quantity=float(submitted["quantity"]),
            accepted_at_ms=int(submitted["accepted_at_ms"]),
            timeout_at_ms=int(submitted["accepted_at_ms"]) + 6000,
            last_progress_state=PassiveOrderState.OPEN,
        ),
    )


@pytest.mark.asyncio
async def test_zero_fill_canceled_maker_reposts_without_fail_closed(tmp_path):
    """Incident 2026-05-27: zero-fill maker cancel is a V1 retry cycle, not health."""

    cancel_sample = _payload("passive_maintenance.cancel_try_window")
    truth_sample = _payload("exchange.truth")
    abort_sample = _payload("entry.aborted")

    config = make_test_config(str(tmp_path))
    config.strategy.maker_try_window_ms = int(cancel_sample["try_window_ms"])
    config.strategy.maker_min_fill_ratio = float(cancel_sample["min_fill_ratio"])
    config.strategy.maker_entry_progress_poll_ms = 100
    config.strategy.maker_venue_budget_window_ms = 100
    config.strategy.maker_entry_rest_timeout_ms = 6000
    config.strategy.maker_entry_max_reposts = 2
    config.strategy.maker_cycle_retry_delays_ms = [500, 1000, 1000]
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(Venue.OKX, PassiveOrderState.OPEN)
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.CANCELED)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    await runtime.start()
    pending = _pending_from_fixture()
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.accepted_at_ms + config.strategy.maker_try_window_ms
    )
    assert okx.cancel_calls, "zero-fill try window must cancel the stale maker order"

    cancel_requested_at = pending.passive_order.cancel_requested_at_ms
    await runtime._maintain_pending_entry_passive_orders(
        cancel_requested_at + config.strategy.maker_venue_budget_window_ms
    )

    retry_at_ms = pending.next_progress_poll_ms
    await runtime._maintain_pending_entry_passive_orders(retry_at_ms)

    kinds = [record["kind"] for record in runtime.journal.read_all()]
    evidence = {
        "fill_rate": cancel_sample["fill_ratio"],
        "cancel_delay_ms": cancel_sample["elapsed_ms"],
        "exchange_flat": truth_sample["maker_position"] == 0.0
        and truth_sample["hedge_position"] == 0.0
        and truth_sample["open_orders"] == 0,
        "reprice_or_repost_occurred": bool(okx.submit_passive_order_calls),
        "fail_closed": runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        or runtime.state.lifecycle == EngineLifecycle.RISK_ONLY,
        "v1_expected": "zero-fill canceled maker records a retry cycle and reposts before terminal abort",
        "incident_bad_terminal": abort_sample["reason"],
    }

    assert evidence["fill_rate"] == 0.0
    assert evidence["cancel_delay_ms"] == config.strategy.maker_try_window_ms
    assert evidence["exchange_flat"] is True
    assert evidence["reprice_or_repost_occurred"] is True, evidence
    assert evidence["fail_closed"] is False, evidence
    assert "entry.aborted" not in kinds
    assert "passive_maintenance.zero_fill_cycle" in kinds
    assert "passive_maintenance.passive_entry_reposted" in kinds


@pytest.mark.asyncio
async def test_recovered_terminal_zero_fill_pending_reposts_without_stalling(tmp_path):
    """Recovered canceled/zero-fill pending must not be skipped by terminal guard."""

    cancel_sample = _payload("passive_maintenance.cancel_try_window")

    config = make_test_config(str(tmp_path))
    config.strategy.maker_try_window_ms = int(cancel_sample["try_window_ms"])
    config.strategy.maker_min_fill_ratio = float(cancel_sample["min_fill_ratio"])
    config.strategy.maker_entry_progress_poll_ms = 100
    config.strategy.maker_venue_budget_window_ms = 100
    config.strategy.maker_entry_rest_timeout_ms = 6000
    config.strategy.maker_entry_max_reposts = 2
    config.strategy.maker_cycle_retry_delays_ms = [500, 1000, 1000]
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(Venue.OKX, PassiveOrderState.CANCELED)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx})
    await runtime.start()
    pending = _pending_from_fixture()
    pending.passive_order.cancel_requested_at_ms = pending.passive_order.accepted_at_ms + 1500
    pending.passive_order.last_progress_state = PassiveOrderState.CANCELED
    pending.next_progress_poll_ms = 0
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.cancel_requested_at_ms + config.strategy.maker_venue_budget_window_ms
    )
    retry_at_ms = pending.next_progress_poll_ms
    await runtime._maintain_pending_entry_passive_orders(retry_at_ms)

    kinds = [record["kind"] for record in runtime.journal.read_all()]
    assert okx.submit_passive_order_calls, "recovered terminal zero-fill must repost"
    assert "passive_maintenance.zero_fill_cycle" in kinds
    assert "passive_maintenance.passive_entry_reposted" in kinds
    assert "entry.aborted" not in kinds
    assert runtime.state.risk_mode != GlobalRiskMode.FAIL_CLOSED


@pytest.mark.asyncio
async def test_second_zero_fill_cycle_reposts_until_repost_budget_is_reached(tmp_path):
    """A second zero-fill cycle below max reposts must retry, not terminalize early."""

    cancel_sample = _payload("passive_maintenance.cancel_try_window")

    config = make_test_config(str(tmp_path))
    config.strategy.maker_try_window_ms = int(cancel_sample["try_window_ms"])
    config.strategy.maker_min_fill_ratio = float(cancel_sample["min_fill_ratio"])
    config.strategy.maker_entry_progress_poll_ms = 100
    config.strategy.maker_venue_budget_window_ms = 100
    config.strategy.maker_entry_rest_timeout_ms = 6000
    config.strategy.maker_entry_max_reposts = 3
    config.strategy.maker_cycle_retry_delays_ms = [500, 1000, 1000]
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(Venue.OKX, PassiveOrderState.OPEN)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx})
    await runtime.start()
    pending = _pending_from_fixture()
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.accepted_at_ms + config.strategy.maker_try_window_ms
    )
    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.cancel_requested_at_ms + config.strategy.maker_venue_budget_window_ms
    )
    await runtime._maintain_pending_entry_passive_orders(pending.next_progress_poll_ms)
    assert len(okx.submit_passive_order_calls) == 1
    assert pending.repost_count == 2

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.accepted_at_ms + config.strategy.maker_try_window_ms
    )
    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.cancel_requested_at_ms + config.strategy.maker_venue_budget_window_ms
    )
    await runtime._maintain_pending_entry_passive_orders(pending.next_progress_poll_ms)

    records = runtime.journal.read_all()
    cycle_attempts = [
        record["payload"]["cycle_attempt"]
        for record in records
        if record["kind"] == "passive_maintenance.zero_fill_cycle"
    ]
    kinds = [record["kind"] for record in records]
    assert cycle_attempts == [1, 2]
    assert len(okx.submit_passive_order_calls) == 2
    assert pending.repost_count == 3
    assert "passive_maintenance.zero_fill_repost_exhausted" not in kinds
    assert "entry.aborted" not in kinds
    assert runtime.state.risk_mode != GlobalRiskMode.FAIL_CLOSED


@pytest.mark.asyncio
async def test_zero_fill_repost_limit_exhaustion_finalizes_unfilled_without_abort(tmp_path):
    """When V1 repost budget is exhausted, zero-fill terminalizes cleanly."""

    config = make_test_config(str(tmp_path))
    config.strategy.maker_entry_progress_poll_ms = 100
    config.strategy.maker_venue_budget_window_ms = 100
    config.strategy.maker_entry_rest_timeout_ms = 6000
    config.strategy.maker_entry_max_reposts = 2
    config.strategy.maker_cycle_retry_delays_ms = [500, 1000, 1000]
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(Venue.OKX, PassiveOrderState.CANCELED)
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.CANCELED)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    await runtime.start()
    pending = _pending_from_fixture()
    pending.repost_count = config.strategy.maker_entry_max_reposts
    pending.passive_order.cancel_requested_at_ms = pending.passive_order.accepted_at_ms + 1500
    pending.passive_order.last_progress_state = PassiveOrderState.CANCELED
    pending.next_progress_poll_ms = 0
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.cancel_requested_at_ms + config.strategy.maker_venue_budget_window_ms
    )

    kinds = [record["kind"] for record in runtime.journal.read_all()]
    assert "passive_maintenance.zero_fill_repost_exhausted" in kinds
    assert "entry.passive_unfilled" in kinds
    assert "pending_entry.pending_entry_finalized" in kinds
    assert "entry.aborted" not in kinds
    assert pending.pending_id not in runtime.state.pending_entries
    assert runtime.state.risk_mode != GlobalRiskMode.FAIL_CLOSED
