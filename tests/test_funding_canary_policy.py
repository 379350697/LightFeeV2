from types import SimpleNamespace

from lightfee.config.schema import StrategyConfig
from lightfee.strategy.funding_canary_policy import (
    ALL_LIVE_FUNDING_VENUES,
    canary_edge_floors,
    canary_fee_assurance_tier,
    canary_notional_cap,
    canonical_venue_pair,
)


def test_policy_covers_all_seven_live_funding_venues() -> None:
    assert ALL_LIVE_FUNDING_VENUES == {
        "aster",
        "binance",
        "bitget",
        "bybit",
        "gate",
        "hyperliquid",
        "okx",
    }


def test_pair_floors_are_direction_independent_and_override_global_defaults() -> None:
    strategy = StrategyConfig(
        funding_canary_min_expected_net_edge_bps=3.0,
        funding_canary_min_worst_case_edge_bps=0.0,
        funding_canary_min_expected_net_edge_bps_by_venue_pair={"OKX|Bybit": 5.0},
        funding_canary_min_worst_case_edge_bps_by_venue_pair={"bybit:okx": 1.0},
    )

    assert canonical_venue_pair("OKX", "bybit") == "bybit:okx"
    assert canary_edge_floors(strategy, "okx", "BYBIT") == (5.0, 1.0)
    assert canary_edge_floors(strategy, "gate", "aster") == (3.0, 0.0)


def test_conservative_fee_assurance_is_explicit_and_size_bounded() -> None:
    strategy = StrategyConfig(
        funding_canary_max_entry_notional_quote=50.0,
        funding_canary_conservative_fee_max_entry_notional_quote=15.0,
        funding_canary_require_account_fee_evidence=False,
    )
    conservative = SimpleNamespace(
        account_fee_evidence_complete=False,
        taker_fee_evidence_complete=True,
    )
    account = SimpleNamespace(account_fee_evidence_complete=True)
    unavailable = SimpleNamespace(
        account_fee_evidence_complete=False,
        taker_fee_evidence_complete=False,
    )

    assert canary_fee_assurance_tier(conservative, strategy) == "conservative"
    assert canary_notional_cap(conservative, strategy) == 15.0
    assert canary_fee_assurance_tier(account, strategy) == "account"
    assert canary_notional_cap(account, strategy) == 50.0
    assert canary_fee_assurance_tier(unavailable, strategy) == "unavailable"
