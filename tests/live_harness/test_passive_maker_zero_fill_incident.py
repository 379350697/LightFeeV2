from __future__ import annotations

import json
from pathlib import Path

import pytest

from lightfee.core.domain import (
    OrderFill,
    PassiveOrderAck,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.engine.entry_sync import EntrySyncExecutor
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import (
    PendingEntry,
    PendingEntryPassivePhaseState,
    PendingEntryRemainderSlice,
    PendingPassiveOrder,
)
from lightfee.marketdata.l2 import L2BookStatus, PriceLevel
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from tests.test_live_full_closure import make_test_config


pytestmark = pytest.mark.live_harness

FIXTURE = Path("tests/fixtures/live_incidents/2026-05-27/passive_maker_zero_fill.jsonl")
_VIABLE_FIRST_FUNDING_MS = 1779816647600


class _ZeroFillMakerAdapter:
    def __init__(
        self,
        venue: Venue,
        progress_state: PassiveOrderState,
        *,
        cumulative_quantity: float = 0.0,
        average_price: float = 0.0,
        observed_at_ms: int = 1779816047600,
        post_only_failures: int = 0,
        ack_price: float | None = None,
        post_only_error: str = "post_only_would_take",
    ) -> None:
        self.venue = venue
        self.progress_state = progress_state
        self.cumulative_quantity = cumulative_quantity
        self.average_price = average_price
        self.observed_at_ms = observed_at_ms
        self.post_only_failures = post_only_failures
        self.ack_price = ack_price
        self.post_only_error = post_only_error
        self.cancel_calls: list[dict[str, object]] = []
        self.submit_passive_order_calls: list[object] = []
        self.place_order_calls: list[object] = []
        self.query_calls: list[dict[str, object]] = []
        self.refresh_market_snapshot_calls: list[str] = []

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
            cumulative_quantity=self.cumulative_quantity,
            average_price=self.average_price,
            observed_at_ms=self.observed_at_ms,
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
        if self.post_only_failures > 0:
            self.post_only_failures -= 1
            raise RuntimeError(self.post_only_error)
        self.progress_state = PassiveOrderState.OPEN
        return PassiveOrderAck(
            venue=self.venue,
            symbol=request.symbol,
            side=request.side,
            order_id=f"repost-{len(self.submit_passive_order_calls)}",
            client_order_id=request.client_order_id or f"repost-cid-{len(self.submit_passive_order_calls)}",
            state=PassiveOrderState.OPEN,
            quantity=request.quantity,
            price=self.ack_price if self.ack_price is not None else request.price or 0.0101,
            accepted_at_ms=1779816048100,
        )

    async def refresh_market_snapshot(self, symbol: str):
        self.refresh_market_snapshot_calls.append(symbol)
        return None

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

    async def place_order(self, request) -> OrderFill:
        self.place_order_calls.append(request)
        return OrderFill(
            venue=self.venue,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=request.price or self.average_price or 0.0,
            order_id=f"hedge-{len(self.place_order_calls)}",
            filled_at_ms=self.observed_at_ms,
        )

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return float(quantity or 0.0)


def _events() -> list[dict]:
    return [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]


def _payload(kind: str) -> dict:
    return next(event["payload"] for event in _events() if event["kind"] == kind)


def _frozen_base_symbol_rule(venue: Venue, symbol: str) -> dict[str, object]:
    """Executable entry-time rule evidence for the synthetic RIVER incident."""

    return {
        "venue": venue.value,
        "symbol": symbol,
        "venue_symbol": symbol,
        "quantity_units": "base",
        "quantity_step_base": 0.001,
        "min_quantity_base": 0.001,
        "min_notional_quote": 0.0,
        "source": "live_harness_fixture",
        "rule_source": "synthetic_identity_adapter",
        "missing_fields": [],
        "evidence_complete": True,
    }


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
        first_funding_timestamp_ms=_VIABLE_FIRST_FUNDING_MS,
        funding_timestamp_ms=_VIABLE_FIRST_FUNDING_MS,
        long_funding_timestamp_ms=_VIABLE_FIRST_FUNDING_MS,
        short_funding_timestamp_ms=_VIABLE_FIRST_FUNDING_MS,
        long_symbol_rule_at_entry=_frozen_base_symbol_rule(
            Venue.OKX,
            selected["symbol"],
        ),
        short_symbol_rule_at_entry=_frozen_base_symbol_rule(
            Venue.BYBIT,
            selected["symbol"],
        ),
        common_base_quantity_step_at_entry=0.001,
        repost_count=1,
        zero_fill_since_ms=int(submitted["accepted_at_ms"]),
        phase_state=PendingEntryPassivePhaseState(
            execution_kind="entry",
            preferred_maker_leg="long",
            active_maker_leg="long",
            phase="high_slippage_maker",
            cycle_attempt=1,
            phase_started_at_ms=int(submitted["accepted_at_ms"]),
            cycle_started_at_ms=int(submitted["accepted_at_ms"]),
        ),
        passive_attempt_count=1,
        passive_order=PendingPassiveOrder(
            order_id=submitted["order_id"],
            client_order_id=submitted["client_order_id"],
            limit_price=float(submitted["price"]),
            target_quantity=float(submitted["quantity"]),
            accepted_at_ms=int(submitted["accepted_at_ms"]),
            timeout_at_ms=int(submitted["accepted_at_ms"]) + 6000,
            last_progress_state=PassiveOrderState.OPEN,
        ),
        frozen_candidate={
            "symbol": selected["symbol"],
            "long_venue": "okx",
            "short_venue": "bybit",
            "blocked": False,
            "blocked_reasons": [],
            "ranking_edge_bps": 12.0,
            "expected_edge_bps": 10.0,
            "funding_edge_bps": 8.0,
            "entry_notional_quote": 50.0,
            "opportunity_type": "aligned",
        },
    )


def _tradeable_frozen_candidate(*, entry_notional_quote: float = 50.0) -> dict:
    return {
        "symbol": "RIVERUSDT",
        "long_venue": "okx",
        "short_venue": "bybit",
        "blocked": False,
        "blocked_reasons": [],
        "ranking_edge_bps": 12.0,
        "expected_edge_bps": 10.0,
        "funding_edge_bps": 8.0,
        "entry_notional_quote": entry_notional_quote,
        "long_price_hint": 7.0,
        "short_price_hint": 8.0,
        "opportunity_type": "aligned",
    }


def _seed_passive_repost_quote(
    runtime: LiveRuntime,
    *,
    observed_at_ms: int = 1_779_816_045_100,
) -> None:
    for venue in ("okx", "bybit"):
        book = runtime.local_l2_runtime.ensure_book(venue, "RIVERUSDT")
        book.status = L2BookStatus.HOT
        book.bids = [PriceLevel(price=0.0101, quantity=1_000_000.0)]
        book.asks = [PriceLevel(price=0.0119, quantity=1_000_000.0)]
        book.observed_at_ms = observed_at_ms


def _install_passive_repost_quote(runtime: LiveRuntime) -> None:
    quote_state = {"bid": 0.0101, "ask": 0.0119}
    runtime._resolve_local_l2_quote = lambda venue, symbol: (
        quote_state["bid"],
        quote_state["ask"],
    )
    runtime.config.strategy.entry_local_l2_book_stale_after_ms = 300_000
    _seed_passive_repost_quote(runtime)

    original_start = runtime.start

    async def start_with_passive_repost_quote(*args, **kwargs):
        result = await original_start(*args, **kwargs)
        _seed_passive_repost_quote(runtime)
        return result

    runtime.start = start_with_passive_repost_quote

    original_quote_gate = runtime._pending_entry_passive_repost_quote_gate

    def passive_repost_quote_gate_with_fresh_fixture(pending, *, now_ms: int):
        _seed_passive_repost_quote(runtime, observed_at_ms=now_ms)
        return original_quote_gate(pending, now_ms=now_ms)

    runtime._pending_entry_passive_repost_quote_gate = (
        passive_repost_quote_gate_with_fresh_fixture
    )

    original_refresh = runtime._refresh_pending_entry_passive_market_snapshot

    async def refresh_with_new_passive_repost_quote(pending, adapter):
        result = await original_refresh(pending, adapter)
        quote_state["bid"] = 0.0100
        quote_state["ask"] = 0.0120
        return result

    runtime._refresh_pending_entry_passive_market_snapshot = (
        refresh_with_new_passive_repost_quote
    )

    async def fake_retry_sleep(_wait_ms: int) -> None:
        return None

    runtime._pending_entry_post_only_retry_sleep = fake_retry_sleep


class _RecordingEntryExecutor:
    def __init__(self) -> None:
        self.contexts: list[object] = []

    async def execute(self, ctx):
        self.contexts.append(ctx)
        return type("EntryResult", (), {"open_position": None})()


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
    _install_passive_repost_quote(runtime)
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
    assert pending.passive_ops_total == 2
    assert pending.repost_attempt_count == 1
    assert pending.passive_attempt_count == 1
    assert pending.phase_state is not None
    assert pending.phase_state.zero_fill_cycles_in_phase == 1
    assert pending.phase_state.cycle_attempt == 2
    assert pending.phase_state.next_cycle_delay_ms is None
    assert pending.phase_state.cycle_started_at_ms == pending.passive_order.accepted_at_ms


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
    _install_passive_repost_quote(runtime)
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
    assert pending.repost_attempt_count == 1
    assert pending.passive_attempt_count == 1
    assert pending.phase_state is not None
    assert pending.phase_state.zero_fill_cycles_in_phase == 1


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
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.OPEN)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    _install_passive_repost_quote(runtime)
    runtime.entry_executor = EntrySyncExecutor(
        adapters={Venue.OKX: okx, Venue.BYBIT: bybit},
        journal=runtime.journal,
    )
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
    assert len(okx.submit_passive_order_calls) == 1
    assert len(bybit.submit_passive_order_calls) == 1
    assert pending.repost_count == 3
    assert pending.maker_leg == "short"
    assert pending.repost_attempt_count == 0
    assert pending.passive_attempt_count == 1
    assert pending.passive_ops_total == 4
    assert pending.phase_state is not None
    assert pending.phase_state.phase == "low_slippage_maker"
    assert pending.phase_state.active_maker_leg == "short"
    assert pending.phase_state.zero_fill_cycles_in_phase == 0
    assert pending.phase_state.cycle_attempt == 1
    assert "passive_maintenance.zero_fill_repost_exhausted" not in kinds
    assert "entry.aborted" not in kinds
    assert runtime.state.risk_mode != GlobalRiskMode.FAIL_CLOSED


@pytest.mark.asyncio
async def test_low_slippage_zero_fill_exhaustion_arms_dual_taker_terminal(tmp_path):
    """V1: high phase exhausts into low maker; low phase exhaustion arms dual-taker."""

    cancel_sample = _payload("passive_maintenance.cancel_try_window")

    config = make_test_config(str(tmp_path))
    config.strategy.maker_try_window_ms = int(cancel_sample["try_window_ms"])
    config.strategy.maker_min_fill_ratio = float(cancel_sample["min_fill_ratio"])
    config.strategy.maker_entry_progress_poll_ms = 100
    config.strategy.maker_venue_budget_window_ms = 100
    config.strategy.maker_entry_rest_timeout_ms = 6000
    config.strategy.maker_entry_max_reposts = 10
    config.strategy.maker_cycle_retry_delays_ms = [500, 1000, 1000]
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(Venue.OKX, PassiveOrderState.OPEN)
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.OPEN)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    _install_passive_repost_quote(runtime)
    runtime.entry_executor = EntrySyncExecutor(
        adapters={Venue.OKX: okx, Venue.BYBIT: bybit},
        journal=runtime.journal,
    )
    await runtime.start()
    pending = _pending_from_fixture()
    runtime.state.pending_entries[pending.pending_id] = pending

    async def zero_fill_one_cycle() -> None:
        await runtime._maintain_pending_entry_passive_orders(
            pending.passive_order.accepted_at_ms + config.strategy.maker_try_window_ms
        )
        await runtime._maintain_pending_entry_passive_orders(
            pending.passive_order.cancel_requested_at_ms + config.strategy.maker_venue_budget_window_ms
        )
        await runtime._maintain_pending_entry_passive_orders(pending.next_progress_poll_ms)

    await zero_fill_one_cycle()
    await zero_fill_one_cycle()
    assert pending.phase_state is not None
    assert pending.phase_state.phase == "low_slippage_maker"

    await zero_fill_one_cycle()
    await zero_fill_one_cycle()

    records = runtime.journal.read_all()
    kinds = [record["kind"] for record in records]
    assert "execution.dual_taker_armed" in kinds
    assert "execution.entry_fallback_to_taker" in kinds
    assert "entry.opened" in kinds
    assert "runtime.position_opened" in kinds
    assert "entry.passive_unfilled" not in kinds
    assert pending.phase_state.phase == "dual_taker"
    assert pending.pending_id not in runtime.state.pending_entries
    assert pending.pending_id in runtime.state.open_positions
    assert len(bybit.submit_passive_order_calls) == 2
    assert okx.place_order_calls
    assert bybit.place_order_calls
    assert "entry.aborted" not in kinds
    assert runtime.state.risk_mode != GlobalRiskMode.FAIL_CLOSED


@pytest.mark.asyncio
async def test_terminal_taker_fallback_rechecks_runtime_guards_before_force_standard(tmp_path):
    """V1: terminal taker fallback must reuse runtime entry guards before taker open."""

    config = make_test_config(str(tmp_path))
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(Venue.OKX, PassiveOrderState.CANCELED)
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.CANCELED)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    recorder = _RecordingEntryExecutor()
    runtime.entry_executor = recorder
    await runtime.start()

    pending = _pending_from_fixture()
    pending.frozen_candidate = _tradeable_frozen_candidate()
    runtime._zero_fill_cooldown_until_ms[("RIVERUSDT", "okx", "bybit")] = (
        pending.passive_order.accepted_at_ms + 60_000
    )

    executed = await runtime._execute_pending_entry_terminal_taker_fallback(
        pending,
        pending.pending_id,
        pending.passive_order.accepted_at_ms + 1_000,
        "maker_entry_dual_taker_after_phase_exhaustion",
    )

    events = runtime.journal.read_all()
    skipped = [
        record
        for record in events
        if record["kind"] == "execution.entry_fallback_to_taker_skipped"
    ]
    assert executed is False
    assert recorder.contexts == []
    assert skipped
    assert skipped[-1]["payload"]["reason"] == (
        "candidate_not_tradeable_after_terminal_reprice"
    )
    assert "zero_fill_cooldown" in skipped[-1]["payload"]["blocked_reasons"]


@pytest.mark.asyncio
async def test_force_standard_terminal_fallback_uses_rechecked_candidate_sizing(tmp_path):
    """V1: ForceStandard fallback builds the open attempt from the rechecked candidate."""

    config = make_test_config(str(tmp_path))
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(Venue.OKX, PassiveOrderState.CANCELED)
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.CANCELED)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    recorder = _RecordingEntryExecutor()
    runtime.entry_executor = recorder
    await runtime.start()

    pending = _pending_from_fixture()
    pending.target_quantity = 3.0
    pending.long_quantity = 3.0
    pending.short_quantity = 3.0
    pending.passive_order.limit_price = 5.0
    pending.maker_price = 5.0
    pending.frozen_candidate = _tradeable_frozen_candidate(entry_notional_quote=25.0)

    executed = await runtime._execute_pending_entry_terminal_taker_fallback(
        pending,
        pending.pending_id,
        pending.passive_order.accepted_at_ms + 1_000,
        "maker_entry_dual_taker_after_phase_exhaustion",
    )

    assert executed is True
    assert len(recorder.contexts) == 1
    ctx = recorder.contexts[0]
    assert ctx.long_quantity == pytest.approx(3.571)
    assert ctx.short_quantity == pytest.approx(3.571)
    assert ctx.long_price_hint == pytest.approx(7.0)
    assert ctx.short_price_hint == pytest.approx(8.0)
    assert ctx.blocked_reasons == []
    assert pending.next_progress_poll_ms > pending.passive_order.accepted_at_ms
    assert any(
        record["kind"] == "execution.entry_fallback_to_taker_deferred"
        for record in runtime.journal.read_all()
    )


@pytest.mark.asyncio
async def test_zero_fill_global_repost_count_does_not_skip_v1_phase_switch(tmp_path):
    """V1: phase switch happens before legacy global repost_count exhaustion."""

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
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.CANCELED)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    _install_passive_repost_quote(runtime)
    await runtime.start()
    pending = _pending_from_fixture()
    pending.frozen_candidate = _tradeable_frozen_candidate()
    pending.repost_count = config.strategy.maker_entry_max_reposts
    pending.passive_order.cancel_requested_at_ms = pending.passive_order.accepted_at_ms + 1500
    pending.passive_order.last_progress_state = PassiveOrderState.CANCELED
    pending.metadata["passive_zero_fill_retry_pending"] = True
    pending.metadata["passive_zero_fill_retry_at_ms"] = (
        pending.passive_order.cancel_requested_at_ms
        + config.strategy.maker_venue_budget_window_ms
    )
    pending.phase_state.zero_fill_cycles_in_phase = (
        config.strategy.pending_entry_phase_zero_fill_budget
    )
    pending.phase_state.cycle_attempt = pending.phase_state.zero_fill_cycles_in_phase
    runtime.state.pending_entries[pending.pending_id] = pending

    retained = await runtime._handle_pending_passive_zero_fill_completion(
        pending,
        pending.pending_id,
        pending.passive_order,
        okx,
        pending.passive_order.cancel_requested_at_ms
        + config.strategy.maker_venue_budget_window_ms,
    )

    kinds = [record["kind"] for record in runtime.journal.read_all()]
    assert retained is True
    assert pending.pending_id in runtime.state.pending_entries
    assert pending.phase_state.phase == "low_slippage_maker"
    assert pending.maker_leg == "short"
    assert "execution.passive_phase_switched" in kinds
    assert "passive_maintenance.zero_fill_repost_exhausted" not in kinds
    assert "entry.passive_unfilled" not in kinds


@pytest.mark.asyncio
async def test_terminal_balanced_partial_remainder_reposts_before_finalize(tmp_path):
    """V1: terminal passive order with hedged partial fill reposts the remainder."""

    config = make_test_config(str(tmp_path))
    config.strategy.maker_entry_progress_poll_ms = 100
    config.strategy.maker_venue_budget_window_ms = 0
    config.strategy.maker_entry_rest_timeout_ms = 6000
    config.strategy.maker_entry_max_reposts = 2
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(
        Venue.OKX,
        PassiveOrderState.CANCELED,
        post_only_failures=1,
        ack_price=0.0,
    )
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.CANCELED)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    _install_passive_repost_quote(runtime)

    async def fake_retry_sleep(_wait_ms: int) -> None:
        return None

    runtime._pending_entry_post_only_retry_sleep = fake_retry_sleep
    await runtime.start()
    pending = _pending_from_fixture()
    pending.target_quantity = 10.0
    pending.maker_leg_filled = 4.0
    pending.hedge_leg_filled = 4.0
    pending.repost_attempt_count = 0
    pending.passive_attempt_count = 0
    pending.next_progress_poll_ms = 0
    pending.passive_order.cancel_requested_at_ms = pending.passive_order.accepted_at_ms + 1500
    pending.passive_order.last_progress_state = PassiveOrderState.CANCELED
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.cancel_requested_at_ms + 1
    )

    kinds = [record["kind"] for record in runtime.journal.read_all()]
    assert len(okx.submit_passive_order_calls) == 2
    assert okx.submit_passive_order_calls[0].price != okx.submit_passive_order_calls[1].price
    assert pending.pending_id in runtime.state.pending_entries
    assert pending.passive_order.limit_price == pytest.approx(okx.submit_passive_order_calls[1].price)
    assert pending.passive_order.target_quantity == pytest.approx(6.0)
    assert pending.repost_attempt_count == 1
    assert "execution.passive_entry_reposted" in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds


@pytest.mark.asyncio
async def test_maker_progress_records_v1_remainder_slice(tmp_path):
    """V1: maker progress delta creates a PendingEntryRemainderSlice."""

    config = make_test_config(str(tmp_path))
    config.strategy.maker_try_window_ms = 1500
    config.strategy.maker_min_fill_ratio = 0.25
    config.strategy.maker_entry_progress_poll_ms = 100
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(
        Venue.OKX,
        PassiveOrderState.PARTIALLY_FILLED,
        cumulative_quantity=3.0,
        average_price=0.0102,
        observed_at_ms=1779816047900,
    )
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx})
    await runtime.start()
    pending = _pending_from_fixture()
    pending.repost_count = 0
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.accepted_at_ms + config.strategy.maker_entry_progress_poll_ms
    )

    assert pending.maker_leg_filled == pytest.approx(3.0)
    assert pending.maker_fill_price == pytest.approx(0.0102)
    assert len(pending.maker_remainder_slices) == 1
    remainder = pending.maker_remainder_slices[0]
    assert remainder.quantity == pytest.approx(3.0)
    assert remainder.notional_quote == pytest.approx(0.0306)
    assert remainder.fill_at_ms == 1779816047900


@pytest.mark.asyncio
async def test_maker_progress_drives_missing_hedge_in_same_tick(tmp_path):
    """V1: maker progress wake immediately drives the missing hedge delta."""

    config = make_test_config(str(tmp_path))
    config.strategy.maker_try_window_ms = 1500
    config.strategy.maker_min_fill_ratio = 0.25
    config.strategy.maker_entry_progress_poll_ms = 100
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(
        Venue.OKX,
        PassiveOrderState.PARTIALLY_FILLED,
        cumulative_quantity=1300.0,
        average_price=0.0301,
        observed_at_ms=1779816047900,
    )
    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.OPEN)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit})
    await runtime.start()
    pending = _pending_from_fixture()
    pending.target_quantity = 1300.0
    pending.long_quantity = 1300.0
    pending.short_quantity = 1300.0
    pending.passive_order.target_quantity = 1300.0
    pending.passive_order.limit_price = 0.02969
    pending.repost_count = 0
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.accepted_at_ms + config.strategy.maker_entry_progress_poll_ms
    )

    assert bybit.place_order_calls, "maker progress should immediately submit the hedge"
    request = bybit.place_order_calls[-1]
    assert request.quantity == pytest.approx(1300.0)
    assert pending.hedge_leg_filled == pytest.approx(1300.0)
    assert pending.missing_hedge_quantity() == pytest.approx(0.0)
    kinds = [record["kind"] for record in runtime.journal.read_all()]
    assert kinds.index("passive_maintenance.maker_progress") < kinds.index(
        "pending_entry.hedge_submit_attempt"
    )


@pytest.mark.asyncio
async def test_reposted_maker_progress_uses_v1_fill_checkpoint(tmp_path):
    """V1: progress after repost is fill_checkpoint plus order cumulative fill."""

    config = make_test_config(str(tmp_path))
    config.strategy.maker_try_window_ms = 1500
    config.strategy.maker_min_fill_ratio = 0.25
    config.strategy.maker_entry_progress_poll_ms = 100
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(
        Venue.OKX,
        PassiveOrderState.PARTIALLY_FILLED,
        cumulative_quantity=2.0,
        average_price=0.0105,
        observed_at_ms=1779816049000,
    )
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx})
    await runtime.start()
    pending = _pending_from_fixture()
    pending.target_quantity = 10.0
    pending.maker_leg_filled = 4.0
    pending.maker_fill_price = 0.0100
    pending.passive_order.fill_checkpoint_quantity = 4.0
    pending.passive_order.fill_checkpoint_notional_quote = 0.0400
    pending.passive_order.target_quantity = 6.0
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.accepted_at_ms + config.strategy.maker_entry_progress_poll_ms
    )

    assert pending.maker_leg_filled == pytest.approx(6.0)
    assert len(pending.maker_remainder_slices) == 1
    remainder = pending.maker_remainder_slices[0]
    assert remainder.quantity == pytest.approx(2.0)
    assert remainder.notional_quote == pytest.approx(0.021)
    assert remainder.fill_at_ms == 1779816049000


@pytest.mark.asyncio
async def test_live_hedge_drive_consumes_maker_remainder_fifo(tmp_path):
    """V1: hedge_pending_entry_delta consumes maker remainder slices FIFO."""

    config = make_test_config(str(tmp_path))
    config.symbols = ["RIVERUSDT"]

    bybit = _ZeroFillMakerAdapter(Venue.BYBIT, PassiveOrderState.OPEN)
    runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})
    await runtime.start()
    pending = _pending_from_fixture()
    pending.maker_leg_filled = 3.0
    pending.hedge_leg_filled = 0.0
    pending.maker_fill_price = 20.0
    pending.maker_remainder_slices = [
        PendingEntryRemainderSlice(quantity=1.0, notional_quote=10.0, fill_at_ms=1001),
        PendingEntryRemainderSlice(quantity=2.0, notional_quote=40.0, fill_at_ms=1002),
    ]

    driven = await runtime._drive_missing_hedge_live(
        pending,
        pending.pending_id,
        1779816048000,
    )

    assert driven is True
    assert bybit.place_order_calls
    request = bybit.place_order_calls[-1]
    assert request.quantity == pytest.approx(3.0)
    assert request.price == pytest.approx(50.0 / 3.0)
    assert pending.hedge_leg_filled == pytest.approx(3.0)
    assert pending.missing_hedge_quantity() == pytest.approx(0.0)
    assert pending.maker_remainder_slices == []
    assert pending.outcome == "filled"


@pytest.mark.asyncio
async def test_zero_fill_same_phase_ignores_legacy_repost_count_limit(tmp_path):
    """V1: same-phase zero-fill repost is not cut off by legacy repost_count."""

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
    _install_passive_repost_quote(runtime)
    await runtime.start()
    pending = _pending_from_fixture()
    pending.repost_count = config.strategy.maker_entry_max_reposts
    pending.repost_attempt_count = 1
    pending.passive_order.cancel_requested_at_ms = pending.passive_order.accepted_at_ms + 1500
    pending.passive_order.last_progress_state = PassiveOrderState.CANCELED
    pending.next_progress_poll_ms = 0
    pending.phase_state.zero_fill_cycles_in_phase = 1
    pending.phase_state.cycle_attempt = 1
    pending.metadata["passive_zero_fill_retry_pending"] = True
    pending.metadata["passive_zero_fill_retry_at_ms"] = (
        pending.passive_order.cancel_requested_at_ms
        + config.strategy.maker_venue_budget_window_ms
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._maintain_pending_entry_passive_orders(
        pending.passive_order.cancel_requested_at_ms + config.strategy.maker_venue_budget_window_ms
    )

    kinds = [record["kind"] for record in runtime.journal.read_all()]
    assert okx.submit_passive_order_calls
    assert pending.pending_id in runtime.state.pending_entries
    assert pending.phase_state is not None
    assert pending.phase_state.phase == "high_slippage_maker"
    assert pending.phase_state.zero_fill_cycles_in_phase == 1
    assert pending.phase_state.cycle_attempt == 2
    assert pending.repost_attempt_count == 1
    assert pending.passive_attempt_count == 1
    assert "passive_maintenance.passive_entry_reposted" in kinds
    assert "passive_maintenance.zero_fill_repost_exhausted" not in kinds
    assert "entry.passive_unfilled" not in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds
    assert "entry.aborted" not in kinds
    assert runtime.state.risk_mode != GlobalRiskMode.FAIL_CLOSED


@pytest.mark.asyncio
async def test_zero_fill_repost_retries_post_only_reprice_before_backoff(tmp_path):
    """V1: submit_pending_entry_passive_cycle retries post-only reprice errors."""

    config = make_test_config(str(tmp_path))
    config.strategy.maker_entry_progress_poll_ms = 100
    config.strategy.maker_venue_budget_window_ms = 0
    config.strategy.maker_entry_rest_timeout_ms = 6000
    config.strategy.maker_entry_max_reposts = 2
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(
        Venue.OKX,
        PassiveOrderState.CANCELED,
        post_only_failures=1,
        ack_price=0.0,
        post_only_error="status=429 too many requests retry_after_ms=700 post_only_would_take",
    )
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx})
    _install_passive_repost_quote(runtime)
    sleep_calls: list[int] = []

    async def fake_retry_sleep(wait_ms: int) -> None:
        sleep_calls.append(wait_ms)

    runtime._pending_entry_post_only_retry_sleep = fake_retry_sleep
    await runtime.start()
    pending = _pending_from_fixture()
    pending.passive_order.cancel_requested_at_ms = pending.passive_order.accepted_at_ms + 1500
    pending.passive_order.last_progress_state = PassiveOrderState.CANCELED
    pending.metadata["passive_zero_fill_retry_pending"] = True
    pending.metadata["passive_zero_fill_retry_at_ms"] = pending.passive_order.cancel_requested_at_ms
    runtime.state.pending_entries[pending.pending_id] = pending

    retained = await runtime._handle_pending_passive_zero_fill_completion(
        pending,
        pending.pending_id,
        pending.passive_order,
        okx,
        pending.passive_order.cancel_requested_at_ms,
    )

    assert retained is True
    assert len(okx.submit_passive_order_calls) == 2
    assert okx.submit_passive_order_calls[0].price != okx.submit_passive_order_calls[1].price
    assert pending.passive_order is not None
    assert pending.passive_order.order_id == "repost-2"
    assert pending.passive_order.limit_price == pytest.approx(okx.submit_passive_order_calls[1].price)
    assert pending.passive_attempt_count == 2
    assert sleep_calls == [700]
    assert okx.refresh_market_snapshot_calls == ["RIVERUSDT"]
    assert runtime._maker_venue_request_budget_frozen_until_ms[Venue.OKX.value] > 0
    assert any(
        record["kind"] == "execution.passive_entry_requote_retry"
        for record in runtime.journal.read_all()
    )


@pytest.mark.asyncio
async def test_zero_fill_repost_missing_price_hint_finalizes_without_stale_submit(tmp_path):
    """V1: missing post-only price hint finalizes instead of reusing stale maker price."""

    config = make_test_config(str(tmp_path))
    config.strategy.maker_entry_progress_poll_ms = 100
    config.strategy.maker_venue_budget_window_ms = 0
    config.strategy.maker_entry_rest_timeout_ms = 6000
    config.strategy.maker_entry_max_reposts = 2
    config.symbols = ["RIVERUSDT"]

    okx = _ZeroFillMakerAdapter(Venue.OKX, PassiveOrderState.CANCELED)
    runtime = LiveRuntime(config, venue_adapters={Venue.OKX: okx})
    runtime._resolve_local_l2_quote = lambda venue, symbol: None
    await runtime.start()
    pending = _pending_from_fixture()
    pending.passive_order.cancel_requested_at_ms = pending.passive_order.accepted_at_ms + 1500
    pending.passive_order.last_progress_state = PassiveOrderState.CANCELED
    pending.metadata["passive_zero_fill_retry_pending"] = True
    pending.metadata["passive_zero_fill_retry_at_ms"] = pending.passive_order.cancel_requested_at_ms
    runtime.state.pending_entries[pending.pending_id] = pending

    retained = await runtime._handle_pending_passive_zero_fill_completion(
        pending,
        pending.pending_id,
        pending.passive_order,
        okx,
        pending.passive_order.cancel_requested_at_ms,
    )

    kinds = [record["kind"] for record in runtime.journal.read_all()]
    assert retained is True
    assert okx.submit_passive_order_calls == []
    assert "passive_maintenance.zero_fill_repost_exhausted" in kinds
    assert "pending_entry.pending_entry_finalized" in kinds
    assert pending.pending_id not in runtime.state.pending_entries


def test_pending_entry_post_only_price_uses_same_side_bbo(tmp_path):
    """Pending passive reposts rest at the current same-side BBO."""

    config = make_test_config(str(tmp_path))
    config.strategy.maker_inventory_bias_bps_per_unit = 500.0
    config.strategy.maker_inventory_bias_max_bps = 500.0
    runtime = LiveRuntime(config, venue_adapters={})
    runtime._resolve_local_l2_quote = lambda venue, symbol: (0.0101, 0.0119)
    pending = _pending_from_fixture()
    pending.frozen_candidate = None

    neutral = runtime._pending_entry_post_only_price_hint_at_attempt(
        pending,
        1,
        fallback_price=None,
    )
    pending.frozen_candidate = {
        "entry_notional_quote": 50.0,
        "expected_edge_bps": 50.0,
        "worst_case_edge_bps": 50.0,
    }
    edge_aware = runtime._pending_entry_post_only_price_hint_at_attempt(
        pending,
        1,
        fallback_price=None,
    )
    pending.frozen_candidate = None
    runtime.state.open_positions["inventory-okx"] = PositionSnapshot(
        venue=Venue.OKX,
        symbol=pending.symbol,
        side=Side.BUY,
        quantity=100000.0,
        entry_price=0.011,
        observed_at_ms=1779816047600,
    )
    inventory_biased = runtime._pending_entry_post_only_price_hint_at_attempt(
        pending,
        1,
        fallback_price=None,
    )

    assert neutral == pytest.approx(0.0101)
    assert edge_aware == pytest.approx(0.0101)
    assert inventory_biased == pytest.approx(0.0101)
