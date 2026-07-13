from __future__ import annotations

import pytest

from lightfee.offline.funding_attribution import analyze_funding_attribution_events


def _record(kind: str, payload: dict[str, object]) -> dict[str, object]:
    return {"kind": kind, "payload": payload}


def _terminal(position_id: str = "p-1") -> dict[str, object]:
    return {
        "position_id": position_id,
        "symbol": "BTCUSDT",
        "long_venue": "binance",
        "short_venue": "okx",
        "reason": "funding_capture",
        "lifecycle_forecast_funding_quote": 0.4,
        "price_pnl_quote": 1.0,
        "entry_fee_quote": 0.2,
        "exit_fee_quote": 0.3,
        "calculation_version": "v1_exact",
        "model_epoch": "v1_exact",
    }


def _settled(position_id: str = "p-1") -> dict[str, object]:
    return {
        "position_id": position_id,
        "official_funding_quote": 0.5,
        "official_net_quote": 1.0,
        "funding_forecast_error_quote": 0.1,
        "calculation_version": "v1_exact",
        "model_epoch": "v1_exact",
    }


def test_report_counts_only_statement_proven_net_pnl_as_official() -> None:
    report = analyze_funding_attribution_events(
        [
            _record("exit.closed", _terminal()),
            _record("funding.settlement_reconciled", _settled()),
            _record("exit.closed", _terminal("p-2")),
            _record("funding.settlement_reconciliation_expired", {"position_id": "p-2"}),
        ]
    )

    assert report.position_count == 2
    assert report.official_position_count == 1
    assert report.expired_statement_count == 1
    assert report.awaiting_statement_count == 0
    assert report.official_net_quote == pytest.approx(1.0)
    assert report.settled_funding_quote == pytest.approx(0.5)
    assert report.funding_forecast_error_quote == pytest.approx(0.1)
    assert report.by_symbol["BTCUSDT"].official_position_count == 1


def test_report_deduplicates_terminal_events_and_detects_epoch_mismatch() -> None:
    stale_terminal = _terminal()
    latest_terminal = _terminal()
    latest_terminal["price_pnl_quote"] = 2.0
    bad_epoch = _settled()
    bad_epoch["model_epoch"] = "wrong"

    report = analyze_funding_attribution_events(
        [
            _record("exit.closed", stale_terminal),
            _record("exit.passive_close_resolved", latest_terminal),
            _record("funding.settlement_reconciled", bad_epoch),
        ]
    )

    assert report.position_count == 1
    assert report.duplicate_terminal_event_count == 1
    assert report.price_pnl_quote == pytest.approx(2.0)
    assert report.model_epoch_mismatch_count == 1
