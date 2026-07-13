from __future__ import annotations

import json

from lightfee.offline.spread_paper_analysis import (
    analyze_spread_paper_events,
    spread_paper_report_dict,
)


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
            "candidate_id": f"spread:{symbol}:gate->binance",
            "registered_at_ms": 1_000,
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
            "model_epoch": "v2_signed_reversion",
            "calculation_version": "spread_paper_v2",
            "official_pnl": True,
            "paper_unpriced": False,
            "paper_order_status": "FILLED",
            "paper_entry_mode": "long_taker:short_taker",
            "paper_exit_mode": "long_taker:short_taker",
            "acceptance_eligible": True,
            "paper_control_group": False,
        },
    }


def test_spread_paper_analyzer_uses_only_explicit_symbol_exclusions() -> None:
    report = analyze_spread_paper_events(
        [
            _closed("BBUSDT", -50.0),
            _closed("QNTUSDT", -20.0),
            _closed("LABUSDT", -3.0, label="single_venue_dislocation"),
            _closed("POWERUSDT", -1.0),
            _closed("BREVUSDT", 2.0),
        ],
        excluded_symbols={"BBUSDT", "QNTUSDT"},
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


def test_spread_paper_analyzer_does_not_mix_legacy_or_nonofficial_results() -> None:
    legacy = _closed("LEGACYUSDT", 100.0)
    legacy["payload"]["model_epoch"] = "v1_legacy"
    control = _closed("CONTROLUSDT", 100.0)
    control["payload"]["official_pnl"] = False
    report = analyze_spread_paper_events([legacy, control, _closed("BASEUSDT", 2.0)])

    assert report.closed_count == 1
    assert report.independent_episode_count == 1
    assert report.excluded_legacy_count == 1
    assert report.excluded_nonofficial_count == 1


def test_spread_paper_analyzer_rejects_truthy_journal_gate_values() -> None:
    malformed_official = _closed("OFFICIALUSDT", 100.0)
    malformed_official["payload"]["official_pnl"] = "false"
    malformed_eligibility = _closed("ELIGIBLEUSDT", 100.0)
    malformed_eligibility["payload"]["acceptance_eligible"] = "false"
    malformed_unpriced = _closed("PRICEDUSDT", 100.0)
    malformed_unpriced["payload"]["paper_unpriced"] = "false"

    report = analyze_spread_paper_events(
        [malformed_official, malformed_eligibility, malformed_unpriced]
    )

    assert report.closed_count == 0
    assert report.excluded_nonofficial_count == 1
    assert report.excluded_execution_cohort_count == 1
    assert report.stale_or_unpriced_count == 1


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
    second_lab_episode = _closed("LABUSDT", 2.0, bot_id="core_v1_bot", cohort="core_v1")
    second_lab_episode["payload"]["registered_at_ms"] = 2_000
    report = analyze_spread_paper_events(
        [
            _closed("LABUSDT", 1.0, bot_id="tt_conservative", cohort="baseline_current"),
            second_lab_episode,
            _closed("DOGSUSDT", -0.5, bot_id="core_v1_bot", cohort="core_v1"),
            _closed("INITUSDT", -1.0, bot_id="bad_pair_control_bot", cohort="bad_pair_control"),
        ]
    )

    assert report.by_bot["tt_conservative"].net_quote_total == 1.0
    assert report.by_bot["core_v1_bot"].closed_count == 2
    assert report.by_bot["core_v1_bot"].net_quote_total == 1.5
    assert report.by_cohort["core_v1"].closed_count == 2
    assert report.by_cohort["bad_pair_control"].win_rate == 0.0


def test_spread_paper_analyzer_deduplicates_episodes_before_statistics() -> None:
    first = _closed("BTCUSDT", 2.0)
    duplicate = _closed("BTCUSDT", 20.0)

    report = analyze_spread_paper_events([first, duplicate])

    assert report.independent_episode_count == 1
    assert report.duplicate_episode_count == 1
    assert report.closed_count == 1
    assert report.net_quote_total == 2.0


def test_spread_paper_analyzer_rejects_false_official_nonbaseline_execution() -> None:
    maker = _closed("BTCUSDT", 2.0)
    maker["payload"].update(
        {
            "paper_entry_mode": "long_maker:short_taker",
            "paper_exit_mode": "long_taker:short_taker",
        }
    )

    report = analyze_spread_paper_events([maker])

    assert report.closed_count == 0
    assert report.excluded_execution_cohort_count == 1


def test_spread_paper_analyzer_reports_stress_and_sample_split() -> None:
    event = _closed("BTCUSDT", 2.0)
    event["payload"].update(
        {
            "paper_adverse_selection_quote": 0.02,
            "research_sample_split": "out_of_sample",
        }
    )

    report = analyze_spread_paper_events([event])

    assert report.by_sample_split["out_of_sample"].closed_count == 1
    assert report.stress_net_quote_mean_by_multiplier["1.5x"] < report.mean_net_quote
    assert report.stress_net_quote_mean_by_multiplier["2x"] < report.stress_net_quote_mean_by_multiplier["1.5x"]


def test_spread_paper_analyzer_rejects_nonfinite_economics_and_reports_it() -> None:
    event = _closed("BTCUSDT", float("nan"))

    report = analyze_spread_paper_events([event])

    assert report.closed_count == 0
    assert report.invalid_economics_count == 1


def test_spread_paper_analyzer_orders_metrics_by_evaluation_time_not_jsonl_order() -> None:
    first = _closed("FIRSTUSDT", 10.0)
    second = _closed("SECONDUSDT", -6.0)
    third = _closed("THIRDUSDT", -6.0)
    first["payload"]["evaluated_at_ms"] = 1_000
    second["payload"]["evaluated_at_ms"] = 2_000
    third["payload"]["evaluated_at_ms"] = 3_000

    report = analyze_spread_paper_events([second, first, third])

    assert report.closed_count == 3
    assert report.max_drawdown_quote == 12.0
    payload = spread_paper_report_dict(report)
    assert payload["bootstrap_net_quote_ci95"]
    assert "_net_quotes" not in payload
    assert payload["by_symbol"]["FIRSTUSDT"]["closed_count"] == 1


def test_spread_paper_report_dict_is_strict_json_when_profit_factor_is_unbounded() -> None:
    report = analyze_spread_paper_events([_closed("BTCUSDT", 1.0)])
    payload = spread_paper_report_dict(report)

    assert payload["profit_factor"] is None
    json.dumps(payload, allow_nan=False)
