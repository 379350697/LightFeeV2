from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

from lightfee.core.domain import PassiveOrderState
from lightfee.engine.state import PendingPassiveOrder
from lightfee.engine.pending_entry_terminalizer import (
    PendingEntryLiveTruth,
    PendingEntryTerminalizer,
)


def _pending(**overrides):
    data = {
        "pending_id": "entry-sei",
        "symbol": "SEIUSDT",
        "maker_leg_filled": 0.0,
        "hedge_leg_filled": 0.0,
        "maker_fill_price": 0.0,
        "hedge_fill_price": 0.0,
        "maker_order_id": "maker-order",
        "maker_client_order_id": "maker-client",
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def _resting_passive_order(**overrides):
    data = {
        "order_id": "maker-order",
        "client_order_id": "maker-client",
        "last_progress_state": PassiveOrderState.OPEN,
        "cancel_requested_at_ms": 0,
    }
    data.update(overrides)
    return PendingPassiveOrder(**data)


def test_terminal_zero_fill_with_no_live_order_or_position_is_passive_unfilled():
    decision = PendingEntryTerminalizer().decide(
        _pending(),
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_open_order=False,
            has_live_position=False,
        ),
    )

    assert decision.terminal is True
    assert decision.outcome == "passive_unfilled"
    assert decision.allows_pending_removal is True
    assert decision.healthy is True


def test_terminal_zero_fill_with_live_open_order_is_deferred():
    decision = PendingEntryTerminalizer().decide(
        _pending(),
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_open_order=True,
            has_live_position=False,
        ),
    )

    assert decision.terminal is False
    assert decision.outcome == "deferred_live_open_order"
    assert decision.allows_pending_removal is False
    assert decision.healthy is False


def test_maker_positive_fill_plus_hedge_partial_returns_matched_open_and_residual():
    decision = PendingEntryTerminalizer().decide(
        _pending(maker_leg_filled=455.0, hedge_leg_filled=400.0),
        live_truth=PendingEntryLiveTruth(available=True),
    )

    assert decision.terminal is True
    assert decision.outcome == "open_position_with_residual"
    assert decision.matched_quantity == 400.0
    assert decision.residual_quantity == 55.0
    assert decision.contains_positive_fill_evidence is True
    assert decision.allows_pending_removal is True


def test_positive_fill_requires_direction_correct_live_balanced_quantity():
    decision = PendingEntryTerminalizer().decide(
        _pending(maker_leg_filled=1600.0, hedge_leg_filled=1600.0),
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_position=True,
            positive_fill_requires_live_position=True,
            live_long_quantity=1600.0,
            live_short_quantity=1600.0,
            live_balanced_quantity=1600.0,
        ),
    )

    assert decision.terminal is True
    assert decision.outcome == "open_position"
    assert decision.matched_quantity == 1600.0
    assert decision.allows_pending_removal is True
    assert decision.live_long_quantity == 1600.0
    assert decision.live_short_quantity == 1600.0
    assert decision.live_balanced_quantity == 1600.0


def test_positive_fill_requiring_live_truth_rejects_single_leg_position():
    decision = PendingEntryTerminalizer().decide(
        _pending(maker_leg_filled=1600.0, hedge_leg_filled=1600.0),
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_position=True,
            positive_fill_requires_live_position=True,
            live_long_quantity=0.0,
            live_short_quantity=1600.0,
            live_balanced_quantity=0.0,
        ),
    )

    assert decision.terminal is False
    assert decision.outcome == "positive_fill_live_truth_conflict"
    assert decision.reason == "positive_fill_conflicts_with_live_unmatched_truth"
    assert decision.allows_pending_removal is False
    assert decision.operator_block_required is True
    assert decision.live_long_quantity == 0.0
    assert decision.live_short_quantity == 1600.0
    assert decision.live_balanced_quantity == 0.0


def test_maker_positive_fill_plus_no_hedge_returns_unmatched_residual_cleanup():
    decision = PendingEntryTerminalizer().decide(
        _pending(maker_leg_filled=455.0, hedge_leg_filled=0.0),
        live_truth=PendingEntryLiveTruth(available=True),
    )

    assert decision.terminal is True
    assert decision.outcome == "unmatched_residual_cleanup"
    assert decision.matched_quantity == 0.0
    assert decision.residual_quantity == 455.0
    assert decision.contains_positive_fill_evidence is True
    assert decision.allows_pending_removal is True


def test_missing_live_truth_retains_pending_and_is_not_healthy():
    decision = PendingEntryTerminalizer().decide(
        _pending(),
        live_truth=PendingEntryLiveTruth(available=False, error="timeout"),
    )

    assert decision.terminal is False
    assert decision.outcome == "deferred_missing_live_truth"
    assert decision.allows_pending_removal is False
    assert decision.healthy is False
    assert decision.operator_block_required is True


def test_supervision_stale_clear_allows_zero_fill_resting_when_progress_absent():
    pending = _pending(passive_order=_resting_passive_order())

    decision = PendingEntryTerminalizer().decide_supervision_stale_clear(
        pending,
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_open_order=False,
            has_live_position=False,
        ),
        passive_progress_found=False,
    )

    assert decision.terminal is True
    assert decision.outcome == "supervision_passive_unfilled"
    assert decision.allows_pending_removal is True
    assert decision.healthy is True


def test_supervision_stale_clear_retains_when_open_order_truth_unavailable():
    pending = _pending(passive_order=_resting_passive_order())

    decision = PendingEntryTerminalizer().decide_supervision_stale_clear(
        pending,
        live_truth=PendingEntryLiveTruth(available=False, error="bybit timeout"),
        passive_progress_found=False,
    )

    assert decision.terminal is False
    assert decision.outcome == "deferred_missing_live_truth"
    assert decision.allows_pending_removal is False
    assert decision.operator_block_required is True


def test_supervision_stale_clear_retains_matching_live_open_order():
    pending = _pending(passive_order=_resting_passive_order())

    decision = PendingEntryTerminalizer().decide_supervision_stale_clear(
        pending,
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_open_order=True,
            has_live_position=False,
        ),
        passive_progress_found=False,
    )

    assert decision.terminal is False
    assert decision.outcome == "deferred_live_open_order"
    assert decision.allows_pending_removal is False

    live_position_decision = PendingEntryTerminalizer().decide_supervision_stale_clear(
        pending,
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_open_order=False,
            has_live_position=True,
        ),
        passive_progress_found=False,
    )

    assert live_position_decision.terminal is False
    assert live_position_decision.outcome == "deferred_live_position"
    assert live_position_decision.allows_pending_removal is False


def test_supervision_stale_clear_retains_any_fill_inflight_cancel_or_progress():
    terminalizer = PendingEntryTerminalizer()

    cases = [
        _pending(maker_leg_filled=1.0, passive_order=_resting_passive_order()),
        _pending(hedge_leg_filled=1.0, passive_order=_resting_passive_order()),
        _pending(hedge_inflight=object(), passive_order=_resting_passive_order()),
        _pending(
            passive_order=_resting_passive_order(cancel_requested_at_ms=123),
        ),
        _pending(
            passive_order=_resting_passive_order(
                last_progress_state=PassiveOrderState.PARTIALLY_FILLED,
            ),
        ),
    ]

    for pending in cases:
        decision = terminalizer.decide_supervision_stale_clear(
            pending,
            live_truth=PendingEntryLiveTruth(
                available=True,
                has_live_open_order=False,
                has_live_position=False,
            ),
            passive_progress_found=False,
        )

        assert decision.terminal is False
        assert decision.allows_pending_removal is False

    progress_decision = terminalizer.decide_supervision_stale_clear(
        _pending(passive_order=_resting_passive_order()),
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_open_order=False,
            has_live_position=False,
        ),
        passive_progress_found=True,
    )

    assert progress_decision.terminal is False
    assert progress_decision.allows_pending_removal is False


def test_caller_cannot_pop_pending_when_terminalizer_returns_deferred():
    state = {"pending_entries": {"entry-sei": _pending()}}
    decision = PendingEntryTerminalizer().decide(
        state["pending_entries"]["entry-sei"],
        live_truth=PendingEntryLiveTruth(
            available=True,
            has_live_open_order=True,
            has_live_position=False,
        ),
    )

    removed = PendingEntryTerminalizer.remove_if_allowed(
        state["pending_entries"],
        "entry-sei",
        decision,
    )

    assert removed is False
    assert "entry-sei" in state["pending_entries"]


def test_direct_pending_entry_removal_is_limited_to_terminality_allowlist():
    root = Path("lightfee/engine")
    pattern = re.compile(r"pending_entries\.pop|del .*pending_entries")
    allowed = {
        "runtime.py": "_remove_pending_entry_after_terminal_decision",
        "pending_entry_runtime.py": "_remove_pending_entry_after_terminal_decision",
        "pending_entry_terminalizer.py": "remove_if_allowed",
    }
    violations: list[str] = []

    for path in sorted(root.glob("*.py")):
        lines = path.read_text().splitlines()
        for idx, line in enumerate(lines, start=1):
            if not pattern.search(line):
                continue
            token = allowed.get(path.name)
            window = "\n".join(lines[max(0, idx - 20):idx + 3])
            if token is None or token not in window:
                violations.append(f"{path}:{idx}:{line.strip()}")

    assert violations == []
