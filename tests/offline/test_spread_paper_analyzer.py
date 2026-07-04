from __future__ import annotations

from lightfee.offline.spread_paper_analysis import analyze_spread_paper_events


def _closed(
    symbol: str,
    net_quote: float,
    *,
    label: str = "spread_reversion",
    bot_id: str = "tt_conservative",
    cohort: str = "baseline_current",
) -> dict:
    return {
        "kind": "opportunity.paper_closed",
        "payload": {
            "paper_id": f"spread:{symbol}:gate->binance:1000",
            "symbol": symbol,
            "candidate_opportunity_label": label,
            "paper_bot_id": bot_id,
            "paper_cohort": cohort,
            "paper_net_quote": net_quote,
            "paper_gross_quote": net_quote + 0.02,
            "paper_fee_quote": 0.01,
            "paper_slippage_quote": 0.01,
            "paper_funding_quote": 0.0,
            "paper_net_bps": net_quote / 20.0 * 10_000.0,
        },
    }


def test_spread_paper_analyzer_excludes_bb_and_qnt_by_default() -> None:
    report = analyze_spread_paper_events(
        [
            _closed("BBUSDT", -50.0),
            _closed("QNTUSDT", -20.0),
            _closed("LABUSDT", -3.0, label="single_venue_dislocation"),
            _closed("POWERUSDT", -1.0),
            _closed("BREVUSDT", 2.0),
        ]
    )

    assert report.closed_count == 2
    assert report.excluded_symbols == ["BBUSDT", "QNTUSDT"]
    assert report.allowed_opportunity_labels == ["spread_reversion"]
    assert report.net_quote_total == 1.0
    assert report.win_rate == 0.5
    assert report.by_symbol["POWERUSDT"].net_quote_total == -1.0
    assert report.by_symbol["BREVUSDT"].net_quote_total == 2.0
    assert "BBUSDT" not in report.by_symbol
    assert "QNTUSDT" not in report.by_symbol


def test_spread_paper_analyzer_can_filter_allowed_labels() -> None:
    report = analyze_spread_paper_events(
        [
            _closed("POWERUSDT", -1.0, label="single_venue_dislocation"),
            _closed("BREVUSDT", 2.0, label="spread_reversion"),
        ],
        allowed_opportunity_labels={"spread_reversion"},
    )

    assert report.closed_count == 1
    assert report.net_quote_total == 2.0
    assert report.by_label["spread_reversion"].closed_count == 1
    assert "single_venue_dislocation" not in report.by_label


def test_spread_paper_analyzer_can_include_single_venue_when_requested() -> None:
    report = analyze_spread_paper_events(
        [
            _closed("POWERUSDT", -1.0, label="single_venue_dislocation"),
            _closed("BREVUSDT", 2.0, label="spread_reversion"),
        ],
        allowed_opportunity_labels={"spread_reversion", "single_venue_dislocation"},
    )

    assert report.closed_count == 2
    assert report.net_quote_total == 1.0
    assert report.by_label["single_venue_dislocation"].closed_count == 1


def test_spread_paper_analyzer_groups_by_bot_and_cohort() -> None:
    report = analyze_spread_paper_events(
        [
            _closed("LABUSDT", 1.0, bot_id="tt_conservative", cohort="baseline_current"),
            _closed("LABUSDT", 2.0, bot_id="core_v1_bot", cohort="core_v1"),
            _closed("DOGSUSDT", -0.5, bot_id="core_v1_bot", cohort="core_v1"),
            _closed("INITUSDT", -1.0, bot_id="bad_pair_control_bot", cohort="bad_pair_control"),
        ]
    )

    assert report.by_bot["tt_conservative"].net_quote_total == 1.0
    assert report.by_bot["core_v1_bot"].closed_count == 2
    assert report.by_bot["core_v1_bot"].net_quote_total == 1.5
    assert report.by_cohort["core_v1"].closed_count == 2
    assert report.by_cohort["bad_pair_control"].win_rate == 0.0
