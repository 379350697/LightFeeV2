from __future__ import annotations

import pytest

from lightfee.engine.business_contract import classify_entry_quantity_contract


def test_entry_quantity_contract_residual_uses_common_quantity_not_initial_slice():
    result = classify_entry_quantity_contract(
        raw_quantity=100.0,
        common_quantity=100.0,
        effective_quantity=50.0,
    )

    assert result["quantity_contract_status"] == "hedgeable"
    assert result["unhedgeable_residual_quantity"] == pytest.approx(0.0)


def test_entry_quantity_contract_marks_exchange_step_adjustment():
    result = classify_entry_quantity_contract(
        raw_quantity=1856.0,
        common_quantity=1800.0,
        effective_quantity=1800.0,
    )

    assert result["quantity_contract_status"] == "hedgeable_adjusted"
    assert result["unhedgeable_residual_quantity"] == pytest.approx(56.0)
