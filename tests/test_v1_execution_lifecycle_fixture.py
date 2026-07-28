"""Fixture-driven execution and restart parity for the V1 lifecycle contract.

This deliberately executes V2's public entry and recovery boundaries.  It
does not duplicate their state machines in test helpers: a mismatch in first
leg ordering, hedge sizing, residual construction, or restart blocking fails
against the V1 fixture directly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lightfee.core.domain import OrderFill, Side, Venue
from lightfee.engine.entry import EntryContext, EntryType
from lightfee.engine.entry_sync import EntrySyncExecutor
from lightfee.engine.recovery import recover_from_snapshot
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from tests.fake_adapters import (
    FakeVenueAdapter,
    make_rejected_error,
    make_uncertain_error,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "v1_execution_lifecycle_parity.json"


def _load_fixture() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _context(raw: dict[str, object]) -> EntryContext:
    return EntryContext(
        entry_id=str(raw["entry_id"]),
        symbol=str(raw["symbol"]),
        long_venue=Venue.from_str(str(raw["long_venue"])),
        short_venue=Venue.from_str(str(raw["short_venue"])),
        long_quantity=float(raw["long_quantity"]),
        short_quantity=float(raw["short_quantity"]),
        long_price_hint=float(raw["long_price_hint"]),
        short_price_hint=float(raw["short_price_hint"]),
        maker_leg=Side(str(raw["maker_leg"])),
        entry_type=EntryType(str(raw["entry_type"])),
        created_at_ms=1_700_000_000_000,
    )


def _outcome(
    raw: dict[str, object],
    *,
    venue: Venue,
    symbol: str,
    side: Side,
    price: float,
    order_id: str,
) -> OrderFill | Exception:
    kind = str(raw["outcome"])
    if kind == "rejected":
        return make_rejected_error("fixture rejected")
    if kind == "uncertain":
        return make_uncertain_error("fixture uncertain")
    if kind != "fill":
        raise AssertionError(f"unsupported fixture outcome: {kind}")
    return OrderFill(
        venue=venue,
        symbol=symbol,
        side=side,
        quantity=float(raw["quantity"]),
        price=price,
        order_id=order_id,
        filled_at_ms=1_700_000_000_100,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_name",
    [
        "symmetric_fill_opens_position",
        "maker_zero_fill_rejected_never_submits_hedge",
        "maker_uncertain_is_pending_and_never_submits_hedge",
        "partial_maker_full_hedge_stays_pending_below_match_ratio",
        "partial_hedge_creates_exact_unhedged_residual",
        "second_leg_rejection_creates_exact_first_leg_residual",
    ],
)
async def test_v1_execution_fixture_runs_actual_maker_hedge_lifecycle(
    tmp_path: Path,
    case_name: str,
) -> None:
    fixture = _load_fixture()
    v1_source = fixture["v1_source"]
    assert isinstance(v1_source, dict)
    assert v1_source["commit"] == "ca4b166"
    raw_context = fixture["entry_context"]
    cases = fixture["entry_cases"]
    assert isinstance(raw_context, dict)
    assert isinstance(cases, list)
    case = next(item for item in cases if item["name"] == case_name)
    assert isinstance(case, dict)
    expected = case["expected"]
    assert isinstance(expected, dict)

    context = _context(raw_context)
    maker = FakeVenueAdapter(Venue.BINANCE)
    hedge = FakeVenueAdapter(Venue.OKX)
    raw_maker = case["maker"]
    assert isinstance(raw_maker, dict)
    maker.place_order_outcomes = [
        _outcome(
            raw_maker,
            venue=Venue.BINANCE,
            symbol=context.symbol,
            side=Side.BUY,
            price=context.long_price_hint,
            order_id="fixture-maker-001",
        )
    ]
    raw_hedge = case.get("hedge")
    if isinstance(raw_hedge, dict):
        hedge.place_order_outcomes = [
            _outcome(
                raw_hedge,
                venue=Venue.OKX,
                symbol=context.symbol,
                side=Side.SELL,
                price=context.short_price_hint,
                order_id="fixture-hedge-001",
            )
        ]

    journal = Journal(tmp_path / f"{case_name}.jsonl")
    journal.open()
    try:
        overrides = case.get("executor", {})
        assert isinstance(overrides, dict)
        result = await EntrySyncExecutor(
            adapters={Venue.BINANCE: maker, Venue.OKX: hedge},
            journal=journal,
            config_overrides=overrides,
        ).execute(context)
    finally:
        journal.close()

    assert result.state.value == expected["state"]
    assert result.route.value == expected["route"]
    assert maker.place_order_call_count == expected["maker_order_calls"]
    assert hedge.place_order_call_count == expected["hedge_order_calls"]
    assert result.has_uncertainty is expected["has_uncertainty"]
    assert (result.open_position is not None) is expected["has_open_position"]
    assert (result.pending_entry is not None) is expected["has_pending_entry"]
    assert (result.residual_task is not None) is expected["has_residual"]
    if result.residual_task is not None:
        assert result.residual_task.exposure_quantity == pytest.approx(
            float(expected["residual_quantity"])
        )
        assert result.residual_task.exposure_venue.value == expected["residual_venue"]


def test_v1_execution_fixture_replays_pending_first_leg_on_restart(
    tmp_path: Path,
) -> None:
    fixture = _load_fixture()
    restart = fixture["restart_pending"]
    assert isinstance(restart, dict)
    pending = restart["snapshot"]
    expected = restart["expected"]
    assert isinstance(pending, dict)
    assert isinstance(expected, dict)

    snapshot = SnapshotStore(tmp_path / "state.json")
    snapshot.write(
        {
            "lifecycle": "running",
            "run_id": "v1-execution-fixture",
            "pending_entries": {str(pending["pending_id"]): pending},
        }
    )
    restored = recover_from_snapshot(
        snapshot,
        Journal(tmp_path / "journal.jsonl"),
    )

    restored_pending = restored.pending_entries[str(pending["pending_id"])]
    assert restored.lifecycle.value == expected["lifecycle"]
    assert bool(restored.pending_entries) is expected["has_pending_entry"]
    assert restored_pending.startup_recovery_ready() is expected["startup_recovery_ready"]
    assert restored_pending.missing_hedge_quantity() == pytest.approx(
        float(expected["missing_hedge_quantity"])
    )
