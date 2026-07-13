"""Pending passive maintenance must follow the execution truth, not config drift."""

from __future__ import annotations

from types import SimpleNamespace

from lightfee.core.domain import Side
from lightfee.engine.passive_maker_runtime import PassiveMakerRuntime


def test_pending_maker_leg_survives_a_default_config_change() -> None:
    runtime = object.__new__(PassiveMakerRuntime)
    runtime.ctx = SimpleNamespace(
        config=SimpleNamespace(strategy=SimpleNamespace(maker_leg_default="buy"))
    )

    assert runtime._pending_maker_side(
        SimpleNamespace(maker_leg="short", entry_maker_leg="short")
    ) == Side.SELL
    assert runtime._pending_maker_side(
        SimpleNamespace(maker_leg="long", entry_maker_leg="long")
    ) == Side.BUY
    # An old persisted working maker order still has durable evidence even if
    # it predates the explicit entry_maker_leg field.
    assert runtime._pending_maker_side(
        SimpleNamespace(maker_leg="short", maker_order_id="old-order")
    ) == Side.SELL
    # Recovery of an old pending record without maker-leg evidence retains the
    # legacy config fallback, but new persisted selections never do.
    assert runtime._pending_maker_side(SimpleNamespace(maker_leg="")) == Side.BUY
