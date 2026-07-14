from __future__ import annotations

import hashlib
import json

import pytest

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
            "paper_hedge_delay_quote": 0.0,
            "paper_residual_quote": 0.0,
            "paper_adverse_selection_assumption_quote": 0.0,
            "paper_net_bps": net_quote / 20.0 * 10_000.0,
            "model_epoch": "v2_signed_reversion",
            "calculation_version": "spread_paper_v3",
            "journal_schema_version": 6,
            "official_pnl": True,
            "paper_unpriced": False,
            "paper_order_status": "FILLED",
            "paper_entry_mode": "long_taker:short_taker",
            "paper_exit_mode": "long_taker:short_taker",
            "acceptance_eligible": True,
            "paper_control_group": False,
        },
    }


def _analyze(records: list[dict], **kwargs: object):
    return analyze_spread_paper_events(
        records,
        model_epoch="v2_signed_reversion",
        **kwargs,
    )


def _v3_closed(symbol: str = "V3USDT") -> dict:
    record = _closed(symbol, 2.0)
    payload = record["payload"]
    payload.update(
        {
            "model_epoch": "v3_cost_normalized_reversion",
            "research_manifest_digest": "d" * 64,
            "research_sample_split": "out_of_sample",
            "long_venue": "gate",
            "short_venue": "binance",
            "paper_fill_capacity_source": "l2_vwap",
            "paper_exit_capacity_source": "l2_vwap",
            "funding_settlement_evidence_complete": True,
            "account_fee_evidence_complete": True,
            "account_fee_evidence_observed_at_ms": 900,
            "account_fee_evidence_source": "account_fee_api",
        }
    )
    provenance = [
        {
            "venue": "binance",
            "taker_fee_bps": 1.0,
            "maker_fee_bps": 0.2,
            "observed_at_ms": 900,
            "source": "account_fee_api",
            "evidence_ref": "private-fee-export-binance",
            "document_sha256": "a" * 64,
            "account_identity_hash": "b" * 64,
            "integrity_key_id": "lightfee-fee-evidence-v3",
            "integrity_verified": True,
        },
        {
            "venue": "gate",
            "taker_fee_bps": 1.1,
            "maker_fee_bps": 0.2,
            "observed_at_ms": 950,
            "source": "account_fee_api",
            "evidence_ref": "private-fee-export-gate",
            "document_sha256": "a" * 64,
            "account_identity_hash": "c" * 64,
            "integrity_key_id": "lightfee-fee-evidence-v3",
            "integrity_verified": True,
        },
    ]
    fingerprint = hashlib.sha256(
        json.dumps(
            sorted(provenance, key=lambda row: row["venue"]),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    payload.update(
        {
            "account_fee_evidence_fingerprint": fingerprint,
            "account_fee_evidence_provenance": provenance,
            "candidate_snapshot": {
                "account_fee_evidence_complete": True,
                "account_fee_evidence_observed_at_ms": 900,
                "account_fee_evidence_source": "account_fee_api",
                "account_fee_evidence_fingerprint": fingerprint,
                "account_fee_evidence_provenance": provenance,
            },
            "long_leg": {
                "entry_execution_source": "l2_vwap",
                "exit_execution_source": "l2_vwap",
            },
            "short_leg": {
                "entry_execution_source": "l2_vwap",
                "exit_execution_source": "l2_vwap",
            },
        }
    )
    return record


def test_v3_analysis_requires_oos_official_taker_taker_contract() -> None:
    with pytest.raises(ValueError, match="out_of_sample"):
        analyze_spread_paper_events([], model_epoch="v3_cost_normalized_reversion")
    with pytest.raises(ValueError, match="official_pnl"):
        analyze_spread_paper_events(
            [],
            model_epoch="v3_cost_normalized_reversion",
            require_out_of_sample=True,
            include_nonofficial=True,
        )
    with pytest.raises(ValueError, match="taker_taker"):
        analyze_spread_paper_events(
            [],
            model_epoch="v3_cost_normalized_reversion",
            require_out_of_sample=True,
            require_taker_taker=False,
        )
    with pytest.raises(ValueError, match="journal schema v6"):
        analyze_spread_paper_events(
            [],
            model_epoch="v3_cost_normalized_reversion",
            require_out_of_sample=True,
            required_journal_schema_version=None,
        )
    with pytest.raises(ValueError, match="spread_reversion label"):
        analyze_spread_paper_events(
            [],
            model_epoch="v3_cost_normalized_reversion",
            require_out_of_sample=True,
            allowed_opportunity_labels=None,
        )


def test_v3_analysis_rechecks_frozen_l2_funding_and_account_fee_receipts() -> None:
    accepted = _v3_closed("GOODV3USDT")
    missing_fee_receipt = _v3_closed("BADFEEV3USDT")
    missing_fee_receipt["payload"]["account_fee_evidence_complete"] = False
    bbo_exit = _v3_closed("BADL2V3USDT")
    bbo_exit["payload"]["short_leg"]["exit_execution_source"] = "top_book_only"
    forged_fingerprint = _v3_closed("BADHASHV3USDT")
    forged_fingerprint["payload"]["account_fee_evidence_fingerprint"] = "0" * 64
    forged_fingerprint["payload"]["candidate_snapshot"][
        "account_fee_evidence_fingerprint"
    ] = "0" * 64

    report = analyze_spread_paper_events(
        [accepted, missing_fee_receipt, bbo_exit, forged_fingerprint],
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )

    assert report.closed_count == 1
    assert report.excluded_evidence_count == 3


def test_v3_analysis_rejects_missing_cost_component_and_forged_net_identity() -> None:
    missing_component = _v3_closed("MISSINGCOSTV3USDT")
    del missing_component["payload"]["paper_fee_quote"]
    forged_identity = _v3_closed("FORGEDNETV3USDT")
    forged_identity["payload"]["paper_net_quote"] = 99.0

    report = analyze_spread_paper_events(
        [missing_component, forged_identity],
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )

    assert report.closed_count == 0
    assert report.invalid_economics_count == 2
    assert report.acceptance_ready is False


def test_v3_analysis_rejects_coercible_non_numeric_economics() -> None:
    boolean_cost = _v3_closed("BOOLCOSTV3USDT")
    boolean_cost["payload"]["paper_fee_quote"] = True
    text_cost = _v3_closed("TEXTCOSTV3USDT")
    text_cost["payload"]["paper_slippage_quote"] = "0.01"

    report = analyze_spread_paper_events(
        [boolean_cost, text_cost],
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )

    assert report.closed_count == 0
    assert report.invalid_economics_count == 2
    assert report.acceptance_ready is False


def test_v3_analysis_blocks_duplicate_or_mixed_manifest_acceptance() -> None:
    first = _v3_closed("DUPV3USDT")
    duplicate = _v3_closed("DUPV3USDT")
    mixed_manifest = _v3_closed("OTHERV3USDT")
    mixed_manifest["payload"]["research_manifest_digest"] = "e" * 64

    report = analyze_spread_paper_events(
        [first, duplicate, mixed_manifest],
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )

    assert report.closed_count == 1
    assert report.duplicate_episode_count == 1
    assert report.manifest_digest_mismatch_count == 1
    assert report.research_manifest_digest == ""
    assert report.acceptance_ready is False


def test_spread_paper_analyzer_uses_only_explicit_symbol_exclusions() -> None:
    report = _analyze(
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
    report = _analyze([legacy, control, _closed("BASEUSDT", 2.0)])

    assert report.closed_count == 1
    assert report.independent_episode_count == 1
    assert report.excluded_legacy_count == 1
    assert report.excluded_nonofficial_count == 1


def test_spread_paper_analyzer_excludes_pre_fee_snapshot_journal_schema() -> None:
    legacy_cost_model = _closed("LEGACYCOSTUSDT", 100.0)
    legacy_cost_model["payload"]["journal_schema_version"] = 3

    report = _analyze(
        [legacy_cost_model, _closed("CURRENTUSDT", 2.0)]
    )

    assert report.closed_count == 1
    assert report.net_quote_total == 2.0
    assert report.journal_schema_mismatch_count == 1


def test_spread_paper_analyzer_rejects_truthy_journal_gate_values() -> None:
    malformed_official = _closed("OFFICIALUSDT", 100.0)
    malformed_official["payload"]["official_pnl"] = "false"
    malformed_eligibility = _closed("ELIGIBLEUSDT", 100.0)
    malformed_eligibility["payload"]["acceptance_eligible"] = "false"
    malformed_unpriced = _closed("PRICEDUSDT", 100.0)
    malformed_unpriced["payload"]["paper_unpriced"] = "false"

    report = _analyze(
        [malformed_official, malformed_eligibility, malformed_unpriced]
    )

    assert report.closed_count == 0
    assert report.excluded_nonofficial_count == 1
    assert report.excluded_execution_cohort_count == 1
    assert report.stale_or_unpriced_count == 1


def test_spread_paper_analyzer_can_filter_allowed_labels() -> None:
    report = _analyze(
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
    report = _analyze(
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
    report = _analyze(
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

    report = _analyze([first, duplicate])

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

    report = _analyze([maker])

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

    report = _analyze([event])

    assert report.by_sample_split["out_of_sample"].closed_count == 1
    assert report.stress_net_quote_mean_by_multiplier["1.5x"] < report.mean_net_quote
    assert report.stress_net_quote_mean_by_multiplier["2x"] < report.stress_net_quote_mean_by_multiplier["1.5x"]


def test_spread_paper_analyzer_can_require_out_of_sample_acceptance() -> None:
    in_sample = _closed("INSAMPLEUSDT", 10.0)
    in_sample["payload"]["research_sample_split"] = "in_sample"
    out_of_sample = _closed("OOSUSDT", 2.0)
    out_of_sample["payload"]["research_sample_split"] = "out_of_sample"

    report = _analyze(
        [in_sample, out_of_sample],
        require_out_of_sample=True,
    )

    assert report.closed_count == 1
    assert report.net_quote_total == 2.0
    assert report.excluded_in_sample_count == 1


def test_spread_paper_analyzer_rejects_nonfinite_economics_and_reports_it() -> None:
    event = _closed("BTCUSDT", float("nan"))

    report = _analyze([event])

    assert report.closed_count == 0
    assert report.invalid_economics_count == 1


def test_spread_paper_analyzer_orders_metrics_by_evaluation_time_not_jsonl_order() -> None:
    first = _closed("FIRSTUSDT", 10.0)
    second = _closed("SECONDUSDT", -6.0)
    third = _closed("THIRDUSDT", -6.0)
    first["payload"]["evaluated_at_ms"] = 1_000
    second["payload"]["evaluated_at_ms"] = 2_000
    third["payload"]["evaluated_at_ms"] = 3_000

    report = _analyze([second, first, third])

    assert report.closed_count == 3
    assert report.max_drawdown_quote == 12.0
    payload = spread_paper_report_dict(report)
    assert payload["bootstrap_net_quote_ci95"]
    assert "_net_quotes" not in payload
    assert payload["by_symbol"]["FIRSTUSDT"]["closed_count"] == 1


def test_spread_paper_report_dict_is_strict_json_when_profit_factor_is_unbounded() -> None:
    report = _analyze([_closed("BTCUSDT", 1.0)])
    payload = spread_paper_report_dict(report)

    assert payload["profit_factor"] is None
    json.dumps(payload, allow_nan=False)
