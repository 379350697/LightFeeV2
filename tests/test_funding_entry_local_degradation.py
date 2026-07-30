"""End-to-end sidecar proof that a failed venue only removes its own pairs."""

from __future__ import annotations

import time

import pytest

from lightfee.config.schema import AppConfig, RuntimeConfig, StrategyConfig, VenueConfig
from lightfee.marketdata.open_interest import open_interest_sample_id
from lightfee.sidecar.service import SidecarService
from lightfee.sidecar.snapshot import (
    QuoteSnapshot,
    funding_rate_sample_id,
)


def _quote(*, venue: str, funding_rate_bps: float, now_ms: int) -> QuoteSnapshot:
    funding_timestamp_ms = now_ms + 10 * 60 * 1_000
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=100.00,
        ask=100.01,
        bid_size=10.0,
        ask_size=10.0,
        observed_at_ms=now_ms,
        market_event_at_ms=now_ms,
        funding_rate_bps=funding_rate_bps,
        funding_rate_observed_at_ms=now_ms,
        funding_rate_event_at_ms=now_ms,
        funding_rate_received_at_ms=now_ms,
        funding_rate_source=f"{venue}_funding",
        funding_rate_sample_id=funding_rate_sample_id(
            venue=venue,
            symbol="BTCUSDT",
            observed_at_ms=now_ms,
            rate_bps=funding_rate_bps,
            funding_timestamp_ms=funding_timestamp_ms,
        ),
        funding_timestamp_ms=funding_timestamp_ms,
        funding_interval_ms=28_800_000,
        volume_24h_quote=10_000_000.0,
        open_interest=2_000_000.0,
        open_interest_observed_at_ms=now_ms,
        open_interest_event_at_ms=now_ms,
        open_interest_received_at_ms=now_ms,
        open_interest_source="fixture",
        open_interest_sample_id=open_interest_sample_id(
            venue=venue,
            canonical_symbol="BTCUSDT",
            venue_symbol="BTCUSDT",
            observed_at_ms=now_ms,
            source="fixture",
            raw_value=2_000_000.0,
            value_quote=2_000_000.0,
        ),
        open_interest_venue_symbol="BTCUSDT",
        open_interest_evidence_status="observed",
        raw_open_interest=2_000_000.0,
        raw_open_interest_unit="quote",
        open_interest_contract_multiplier=1.0,
        open_interest_conversion_mark_price=1.0,
        underlying="BTC",
        quote_currency="USDT",
        contract_type="linear",
        contract_multiplier=1.0,
        mark_index_source="fixture",
        price_precision=2,
        quantity_precision=3,
        price_tick=0.01,
        quantity_step_base=0.001,
        min_quantity_base=0.001,
        min_notional_quote=1.0,
        min_notional_evidence_complete=True,
        venue_status="active",
        contract_normalization_complete=True,
    )


@pytest.mark.asyncio
async def test_failed_venue_keeps_healthy_pair_in_live_entry_generation(
    tmp_path, monkeypatch
) -> None:
    """No global frontier/oracle may erase a clean pair after another venue fails."""
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "funding-entry.json"),
            local_l2_depth_bridge_enabled=False,
        ),
        strategy=StrategyConfig(
            min_funding_edge_bps=1.0,
            min_expected_edge_bps=0.0,
            live_entry_notional_cap_quote=50.0,
            entry_notional_cap_quote=50.0,
        ),
        venues=[
            VenueConfig(venue="binance", taker_fee_bps=1.0, maker_fee_bps=0.5),
            VenueConfig(venue="okx", taker_fee_bps=1.0, maker_fee_bps=0.5),
            VenueConfig(venue="bybit", taker_fee_bps=1.0, maker_fee_bps=0.5),
        ],
    )
    service = SidecarService(config)
    now_ms = int(time.time() * 1_000)

    async def funding_results(*_args, **_kwargs):
        return [
            (
                "binance",
                {"binance:BTCUSDT": _quote(venue="binance", funding_rate_bps=1.0, now_ms=now_ms)},
                None,
                set(),
            ),
            (
                "okx",
                {"okx:BTCUSDT": _quote(venue="okx", funding_rate_bps=15.0, now_ms=now_ms)},
                None,
                set(),
            ),
            ("bybit", None, TimeoutError("fixture venue timeout"), {"BTCUSDT"}),
        ]

    monkeypatch.setattr(service, "_fetch_all_venues", funding_results)
    try:
        snapshot = await service.refresh_once()
    finally:
        await service.close()

    assert snapshot.degraded_venues == ["bybit"]
    assert snapshot.degraded_symbols == {"bybit": ["BTCUSDT"]}
    assert "btcusdt:binance->okx" in {
        candidate.pair_id for candidate in snapshot.candidates if not candidate.blocked
    }
    assert all("bybit" not in candidate.pair_id for candidate in snapshot.candidates)
