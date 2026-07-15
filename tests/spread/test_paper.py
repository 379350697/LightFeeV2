from __future__ import annotations

from dataclasses import replace
from math import isfinite

import pytest

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.offline.spread_paper_analysis import analyze_spread_paper_events
from lightfee.spread.models import SpreadReversionCandidate
from lightfee.spread.paper import (
    SPREAD_PAPER_JOURNAL_SCHEMA_VERSION,
    FundingSettlement,
    PaperOrderState,
    SpreadPaperConfig,
    SpreadPaperBotSpec,
    SpreadPaperTracker,
    _execution_contract_from_payload,
    _paper_bot_execution_supported,
)
from lightfee.spread.research_manifest import DEFAULT_SPREAD_RESEARCH_MANIFEST
from lightfee.strategy.fee_evidence import FeeEvidenceBook, FeeScheduleEvidence


def _candidate(
    *,
    signal_ts_ms: int = 1_000,
    long_venue: str = "cheap",
    short_venue: str = "rich",
    entry_notional_quote: float = 20.0,
) -> SpreadReversionCandidate:
    return SpreadReversionCandidate(
        candidate_id=f"spread:BTCUSDT:{long_venue}->{short_venue}",
        symbol="BTCUSDT",
        long_venue=long_venue,
        short_venue=short_venue,
        spread_mid_bps=-50.0,
        executable_spread_bps=-60.0,
        rolling_mean_bps=0.0,
        rolling_std_bps=10.0,
        z_score=-5.0,
        net_edge_bps=25.0,
        sample_count=120,
        signal_ts_ms=signal_ts_ms,
        long_quote_ts_ms=signal_ts_ms,
        short_quote_ts_ms=signal_ts_ms,
        entry_notional_quote=entry_notional_quote,
        capacity_quote=100.0,
        signal_status="entry_ready",
        canonical_venue_a=min(long_venue, short_venue),
        canonical_venue_b=max(long_venue, short_venue),
        current_signed_mid_spread_bps=-50.0,
        current_executable_entry_spread_bps=-60.0,
        equilibrium_spread_bps=0.0,
        target_exit_spread_bps=-5.0,
        gross_reversion_edge_bps=55.0,
        expected_net_edge_bps=25.0,
        worst_case_edge_bps=20.0,
        economics_complete=True,
        fee_evidence_complete=True,
        contract_normalization_status="complete",
    )


def test_paper_bot_gate_rejects_truthy_non_boolean_cohort_evidence() -> None:
    assert _paper_bot_execution_supported(
        SpreadPaperBotSpec("bad-acceptance", "test", acceptance_eligible="false")
    ) is False
    assert _paper_bot_execution_supported(
        SpreadPaperBotSpec("int-acceptance", "test", acceptance_eligible=1)  # type: ignore[arg-type]
    ) is False
    assert _paper_bot_execution_supported(
        SpreadPaperBotSpec(
            "bad-control",
            "test",
            entry_long_role="maker",
            maker_leg="long",
            control_group="false",
        )
    ) is False


def _quote(
    venue: str,
    *,
    bid: float,
    ask: float,
    observed_at_ms: int,
    bid_size: float = 10.0,
    ask_size: float = 10.0,
    funding_rate_bps: float = 0.0,
    funding_timestamp_ms: int = 0,
    funding_interval_ms: int = 0,
    settled_funding_rate_bps: float | None = None,
    mark_price: float = 0.0,
    bid_depth: tuple[tuple[float, float], ...] = (),
    ask_depth: tuple[tuple[float, float], ...] = (),
    price_tick: float = 0.1,
    quantity_step_base: float = 0.001,
    min_quantity_base: float = 0.001,
    min_notional_quote: float = 0.1,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        observed_at_ms=observed_at_ms,
        funding_rate_bps=funding_rate_bps,
        funding_timestamp_ms=funding_timestamp_ms,
        funding_interval_ms=funding_interval_ms,
        settled_funding_rate_bps=settled_funding_rate_bps,
        mark_price=mark_price,
        bid_depth=bid_depth,
        ask_depth=ask_depth,
        price_tick=price_tick,
        quantity_step_base=quantity_step_base,
        min_quantity_base=min_quantity_base,
        min_notional_quote=min_notional_quote,
        min_notional_evidence_complete=True,
    )


def _quotes(
    *,
    now_ms: int,
    long_bid: float = 99.9,
    long_ask: float = 100.0,
    short_bid: float = 101.0,
    short_ask: float = 101.1,
    bid_size: float = 10.0,
    ask_size: float = 10.0,
    long_funding_bps: float = 8.0,
    short_funding_bps: float = 4.0,
    long_funding_ts_ms: int = 0,
    short_funding_ts_ms: int = 0,
    funding_interval_ms: int = 0,
    long_settled_funding_bps: float | None = None,
    short_settled_funding_bps: float | None = None,
    long_mark_price: float = 0.0,
    short_mark_price: float = 0.0,
) -> dict[str, QuoteSnapshot]:
    return {
        "cheap:BTCUSDT": _quote(
            "cheap",
            bid=long_bid,
            ask=long_ask,
            bid_size=bid_size,
            ask_size=ask_size,
            observed_at_ms=now_ms,
            funding_rate_bps=long_funding_bps,
            funding_timestamp_ms=long_funding_ts_ms,
            funding_interval_ms=funding_interval_ms,
            settled_funding_rate_bps=long_settled_funding_bps,
            mark_price=long_mark_price,
        ),
        "rich:BTCUSDT": _quote(
            "rich",
            bid=short_bid,
            ask=short_ask,
            bid_size=bid_size,
            ask_size=ask_size,
            observed_at_ms=now_ms,
            funding_rate_bps=short_funding_bps,
            funding_timestamp_ms=short_funding_ts_ms,
            funding_interval_ms=funding_interval_ms,
            settled_funding_rate_bps=short_settled_funding_bps,
            mark_price=short_mark_price,
        ),
    }


def _tracker(**overrides: object) -> SpreadPaperTracker:
    values: dict[str, object] = {
        "enabled": True,
        "finalist_limit": 1,
        # Tests that exercise later lifecycle stages choose the smallest
        # positive delay explicitly; production defaults remain 250ms.
        "min_decision_latency_ms": 1,
        "markout_secs": [1],
        "terminal_secs": 2,
        "quote_ttl_ms": 1_000,
        "taker_fee_bps_by_venue": {"cheap": 0.0, "rich": 0.0},
    }
    values.update(overrides)
    return SpreadPaperTracker(SpreadPaperConfig(**values))


def _fill_taker(
    tracker: SpreadPaperTracker,
    *,
    now_ms: int,
    quotes: dict[str, QuoteSnapshot],
) -> dict:
    events = tracker.evaluate_due(now_ms, quotes)
    assert [item["kind"] for item in events] == [
        "opportunity.paper_taker_pair_filled"
    ]
    return events[0]["payload"]


def _quotes_at(
    quotes: dict[str, QuoteSnapshot], *, now_ms: int
) -> dict[str, QuoteSnapshot]:
    """Advance an entry snapshot to a distinct, contemporaneous observation."""
    return {
        key: replace(quote, observed_at_ms=now_ms)
        for key, quote in quotes.items()
    }


def test_registration_aligns_quantity_to_both_exact_exchange_steps() -> None:
    tracker = _tracker()
    quotes = _quotes(now_ms=1_000)
    quotes["cheap:BTCUSDT"] = replace(
        quotes["cheap:BTCUSDT"],
        quantity_step_base=0.5,
        min_quantity_base=0.5,
    )
    quotes["rich:BTCUSDT"] = replace(
        quotes["rich:BTCUSDT"],
        quantity_step_base=0.1,
        min_quantity_base=0.1,
    )

    event = tracker.register(
        _candidate(entry_notional_quote=100.0),
        quotes,
        finalist_rank=0,
    )

    assert event is not None
    assert event["payload"]["requested_base_qty"] == pytest.approx(0.5)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [("min_quantity_base", 0.5), ("min_notional_quote", 50.0)],
)
def test_registration_rejects_below_symbol_execution_minimum(
    field_name: str,
    value: float,
) -> None:
    tracker = _tracker()
    quotes = {
        key: replace(quote, **{field_name: value})
        for key, quote in _quotes(now_ms=1_000).items()
    }

    assert (
        tracker.register(
            _candidate(entry_notional_quote=20.0),
            quotes,
            finalist_rank=0,
        )
        is None
    )


def test_min_quantity_only_contract_does_not_invent_ask_based_notional_floor() -> None:
    tracker = _tracker()
    quotes = _quotes(
        now_ms=1_000,
        long_bid=19.0,
        long_ask=20.0,
        short_bid=22.0,
        short_ask=23.0,
    )
    quotes = {
        key: replace(
            quote,
            quantity_step_base=1.0,
            min_quantity_base=1.0,
            min_notional_quote=0.0,
        )
        for key, quote in quotes.items()
    }

    event = tracker.register(
        _candidate(entry_notional_quote=22.0),
        quotes,
        finalist_rank=0,
    )

    assert event is not None
    assert event["payload"]["requested_base_qty"] == pytest.approx(1.0)


def test_protocol_notional_floor_is_not_raised_to_step_times_ask() -> None:
    tracker = _tracker()
    quotes = _quotes(
        now_ms=1_000,
        long_bid=19.0,
        long_ask=20.0,
        short_bid=22.0,
        short_ask=23.0,
    )
    quotes = {
        key: replace(
            quote,
            quantity_step_base=1.0,
            min_quantity_base=1.0,
            min_notional_quote=10.0,
        )
        for key, quote in quotes.items()
    }

    event = tracker.register(
        _candidate(entry_notional_quote=22.0),
        quotes,
        finalist_rank=0,
    )

    assert event is not None
    assert event["payload"]["requested_base_qty"] == pytest.approx(1.0)


def _account_fee_evidence(*, schema_v3: bool = False) -> FeeEvidenceBook:
    cheap_identity = "a" * 64 if schema_v3 else ""
    rich_identity = "b" * 64 if schema_v3 else ""
    return FeeEvidenceBook(
        schedules={
            "cheap": FeeScheduleEvidence(
                venue="cheap",
                taker_fee_bps=1.0,
                maker_fee_bps=0.2,
                observed_at_ms=1_000,
                source="account_fee_api",
                evidence_ref="fee-proof-1",
                account_identity_hash=cheap_identity,
            ),
            "rich": FeeScheduleEvidence(
                venue="rich",
                taker_fee_bps=2.0,
                maker_fee_bps=0.2,
                observed_at_ms=1_000,
                source="account_fee_api",
                evidence_ref="fee-proof-1",
                account_identity_hash=rich_identity,
            ),
        },
        reason="",
        document_sha256="c" * 64 if schema_v3 else "unit-test-account-fee-document",
        integrity_verified=True,
        integrity_key_id=(
            "lightfee-fee-evidence-v3" if schema_v3 else "unit-test-key-v1"
        ),
        schema_version=3 if schema_v3 else 0,
    )


def test_v3_paper_admission_requires_configured_account_identity_binding() -> None:
    evidence = _account_fee_evidence(schema_v3=True)
    candidate = replace(
        _candidate(),
        calculation_version="spread_v3_cost_normalized_reversion",
        model_epoch="v3_cost_normalized_reversion",
        account_fee_evidence_complete=True,
        account_fee_evidence_observed_at_ms=1_000,
        account_fee_evidence_source="account_fee_api",
        account_fee_evidence_fingerprint=evidence.fingerprint_for("cheap", "rich"),
        account_fee_evidence_provenance=evidence.provenance_for("cheap", "rich"),
    )
    v3_manifest = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST,
        model_epoch="v3_cost_normalized_reversion",
    )
    strict = {
        "require_account_fee_evidence": True,
        "account_fee_evidence": evidence,
        "fee_evidence_account_identity_hashes": {
            "cheap": "a" * 64,
            "rich": "b" * 64,
        },
        "model_epoch": "v3_cost_normalized_reversion",
        "research_manifest": v3_manifest,
    }

    assert _tracker(**strict).register(
        candidate, _quotes(now_ms=1_000), finalist_rank=0
    )
    assert not _tracker(
        **{
            **strict,
            "fee_evidence_account_identity_hashes": {
                "cheap": "a" * 64,
                "rich": "d" * 64,
            },
        }
    ).register(candidate, _quotes(now_ms=1_000), finalist_rank=0)


def test_paper_maker_rebate_requires_exact_verified_account_schedule() -> None:
    from lightfee.spread.paper import _execution_fee_evidence_complete, _fee_bps

    evidence = FeeEvidenceBook(
        schedules={
            "cheap": FeeScheduleEvidence(
                venue="cheap",
                taker_fee_bps=1.0,
                maker_fee_bps=-0.2,
                observed_at_ms=1_000,
                source="account_fee_api",
                evidence_ref="rebate-proof",
            )
        },
        reason="",
        document_sha256="test",
        integrity_verified=True,
        integrity_key_id="unit-test-key-v1",
    )
    config = SpreadPaperConfig(
        taker_fee_bps_by_venue={"cheap": 1.0},
        maker_fee_bps_by_venue={"cheap": -0.2},
        account_fee_evidence=evidence,
    )

    assert _fee_bps(config, "cheap", "maker") == 1.0
    assert not _execution_fee_evidence_complete(
        config,
        long_venue="cheap",
        short_venue="cheap",
        entry_long_role="maker",
        entry_short_role="taker",
        exit_long_role="taker",
        exit_short_role="taker",
    )
    verified = replace(config, allow_verified_maker_rebates=True)
    assert _fee_bps(
        verified, "cheap", "maker"
    ) == -0.2
    assert _execution_fee_evidence_complete(
        verified,
        long_venue="cheap",
        short_venue="cheap",
        entry_long_role="maker",
        entry_short_role="taker",
        exit_long_role="taker",
        exit_short_role="taker",
    )


def _with_l2(quotes: dict[str, QuoteSnapshot]) -> dict[str, QuoteSnapshot]:
    return {
        key: replace(
            quote,
            bid_depth=((quote.bid, quote.bid_size),),
            ask_depth=((quote.ask, quote.ask_size),),
        )
        for key, quote in quotes.items()
    }


def test_paper_is_disabled_by_default() -> None:
    tracker = SpreadPaperTracker(SpreadPaperConfig())

    assert not tracker.enabled
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0) is None


def test_strict_paper_requires_same_epoch_account_fees_and_l2_then_freezes_exit_fee() -> None:
    evidence = _account_fee_evidence()
    tracker = _tracker(
        require_l2_vwap=True,
        require_account_fee_evidence=True,
        account_fee_evidence=evidence,
        taker_fee_bps_by_venue={"cheap": 1.0, "rich": 2.0},
        markout_secs=[1],
        terminal_secs=3,
    )
    candidate = replace(
        _candidate(),
        account_fee_evidence_complete=True,
        account_fee_evidence_observed_at_ms=1_000,
        account_fee_evidence_source="account_fee_api",
        account_fee_evidence_fingerprint=evidence.fingerprint_for("cheap", "rich"),
        account_fee_evidence_provenance=evidence.provenance_for("cheap", "rich"),
    )
    quotes = _quotes(now_ms=1_000)

    registered = tracker.register(candidate, quotes, finalist_rank=0)
    assert registered is not None
    # A later BBO-only quote is not an executable official fill.
    assert tracker.evaluate_due(1_001, _quotes_at(quotes, now_ms=1_001)) == []

    fill_quotes = _with_l2(_quotes_at(quotes, now_ms=1_002))
    filled = _fill_taker(tracker, now_ms=1_002, quotes=fill_quotes)
    assert filled["official_pnl"] is True
    # A later config/evidence refresh cannot rewrite a registered position's
    # four-leg fee schedule.
    tracker.config = replace(
        tracker.config,
        taker_fee_bps_by_venue={"cheap": 99.0, "rich": 99.0},
    )
    events = tracker.evaluate_due(2_002, _with_l2(_quotes_at(quotes, now_ms=2_002)))
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["paper_unpriced"] is False
    assert payload["long_leg"]["exit_fee_bps"] == pytest.approx(1.0)
    assert payload["short_leg"]["exit_fee_bps"] == pytest.approx(2.0)
    assert payload["paper_latency_buffer_quote"] == pytest.approx(0.0)


def test_diagnostic_paper_never_labels_bbo_or_unsigned_fees_as_official() -> None:
    tracker = _tracker(
        require_l2_vwap=False,
        require_account_fee_evidence=False,
    )
    quotes = _quotes(now_ms=1_000)

    assert tracker.register(_candidate(), quotes, finalist_rank=0) is not None
    filled = _fill_taker(tracker, now_ms=1_001, quotes=_quotes_at(quotes, now_ms=1_001))

    assert filled["paper_fill_capacity_source"] == "top_book_only"
    assert filled["official_pnl"] is False


def test_open_position_keeps_its_strict_l2_contract_after_config_reload() -> None:
    evidence = _account_fee_evidence()
    tracker = _tracker(
        require_l2_vwap=True,
        require_account_fee_evidence=True,
        account_fee_evidence=evidence,
        markout_secs=[1],
        terminal_secs=3,
    )
    candidate = replace(
        _candidate(),
        account_fee_evidence_complete=True,
        account_fee_evidence_observed_at_ms=1_000,
        account_fee_evidence_source="account_fee_api",
        account_fee_evidence_fingerprint=evidence.fingerprint_for("cheap", "rich"),
        account_fee_evidence_provenance=evidence.provenance_for("cheap", "rich"),
    )
    quotes = _quotes(now_ms=1_000)

    assert tracker.register(candidate, quotes, finalist_rank=0) is not None
    assert _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_with_l2(_quotes_at(quotes, now_ms=1_001)),
    )["official_pnl"] is True

    # A later relaxed admission config must not re-price this observation:
    # its frozen contract still rejects BBO-only exits.
    tracker.config = replace(
        tracker.config,
        require_l2_vwap=False,
        require_account_fee_evidence=False,
    )
    events = tracker.evaluate_due(2_001, _quotes_at(quotes, now_ms=2_001))

    assert len(events) == 1
    assert events[0]["payload"]["paper_unpriced"] is True
    assert events[0]["payload"]["official_pnl"] is False


def test_strict_paper_rejects_fee_provenance_mismatch_and_enforces_oos_cutoff() -> None:
    evidence = _account_fee_evidence()
    strict = _tracker(
        require_account_fee_evidence=True,
        account_fee_evidence=evidence,
        oos_start_ms=2_000,
        require_out_of_sample=True,
    )
    matching = replace(
        _candidate(signal_ts_ms=1_000),
        account_fee_evidence_complete=True,
        account_fee_evidence_observed_at_ms=1_000,
        account_fee_evidence_source="account_fee_api",
        account_fee_evidence_fingerprint=evidence.fingerprint_for("cheap", "rich"),
        account_fee_evidence_provenance=evidence.provenance_for("cheap", "rich"),
    )
    assert strict.register(matching, _quotes(now_ms=1_000), finalist_rank=0) is None

    mismatched = replace(
        matching,
        signal_ts_ms=2_000,
        account_fee_evidence_observed_at_ms=999,
    )
    assert strict.register(mismatched, _quotes(now_ms=2_000), finalist_rank=0) is None

    out_of_sample = replace(
        matching,
        signal_ts_ms=2_000,
        candidate_id="spread:BTCUSDT:cheap->rich:oos",
    )
    event = strict.register(out_of_sample, _quotes(now_ms=2_000), finalist_rank=0)
    assert event is not None
    assert event["payload"]["research_sample_split"] == "out_of_sample"


def test_restore_rejects_position_fee_receipt_mismatched_to_candidate_snapshot() -> None:
    evidence = _account_fee_evidence()
    config = {
        "require_account_fee_evidence": True,
        "account_fee_evidence": evidence,
        "markout_secs": [99],
        "terminal_secs": 10,
    }
    tracker = _tracker(**config)
    candidate = replace(
        _candidate(),
        account_fee_evidence_complete=True,
        account_fee_evidence_observed_at_ms=1_000,
        account_fee_evidence_source="account_fee_api",
        account_fee_evidence_fingerprint=evidence.fingerprint_for("cheap", "rich"),
        account_fee_evidence_provenance=evidence.provenance_for("cheap", "rich"),
    )
    registered = tracker.register(candidate, _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    forged = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "account_fee_evidence_fingerprint": "mismatched",
        },
    }

    restored = _tracker(**config)
    restored.restore_from_records([forged])

    assert restored.tracked_count == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("enabled", "false"),
        ("require_taker_taker", "false"),
        ("require_taker_taker", "true"),
        ("require_taker_taker", 1),
    ],
)
def test_paper_tracker_rejects_truthy_non_boolean_enablement(
    field: str,
    value: object,
) -> None:
    tracker = _tracker(**{field: value})

    assert not tracker.enabled
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0) is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"primary_fill_model": "maker_taker"},
        {"require_taker_taker": False},
        {
            "model_epoch": "v3_other_model",
        },
    ],
)
def test_paper_acceptance_fails_closed_when_baseline_contract_is_not_taker_taker(
    overrides: dict[str, object],
) -> None:
    tracker = _tracker(**overrides)

    assert not tracker.enabled
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0) is None


@pytest.mark.parametrize(
    "candidate",
    [
        replace(_candidate(), contract_normalization_status="unknown"),
        replace(_candidate(), calculation_version="spread_v1_legacy"),
    ],
)
def test_paper_admission_requires_normalized_v2_candidate(
    candidate: SpreadReversionCandidate,
) -> None:
    tracker = _tracker()

    assert tracker.register(candidate, _quotes(now_ms=1_000), finalist_rank=0) is None


def test_paper_admission_requires_candidate_and_execution_fee_evidence() -> None:
    tracker = _tracker()

    assert tracker.register(
        replace(_candidate(), fee_evidence_complete=False),
        _quotes(now_ms=1_000),
        finalist_rank=0,
    ) is None
    assert _tracker(taker_fee_bps_by_venue={}).register(
        _candidate(),
        _quotes(now_ms=1_000),
        finalist_rank=0,
    ) is None


@pytest.mark.parametrize(
    "field",
    ["economics_complete", "fee_evidence_complete"],
)
def test_paper_admission_rejects_truthy_non_boolean_evidence(field: str) -> None:
    tracker = _tracker()

    candidate = replace(_candidate(), **{field: "true"})

    assert tracker.register(candidate, _quotes(now_ms=1_000), finalist_rank=0) is None


def test_taker_registration_requires_later_quote_before_official_fill() -> None:
    tracker = _tracker()
    event = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)

    assert event is not None
    payload = event["payload"]
    assert payload["journal_schema_version"] == SPREAD_PAPER_JOURNAL_SCHEMA_VERSION
    assert payload["model_epoch"] == "v2_signed_reversion"
    assert payload["paper_order_status"] == PaperOrderState.WORKING.value
    assert payload["official_pnl"] is False
    # The decision snapshot cannot fill itself, even after wall-clock time.
    assert tracker.evaluate_due(1_001, _quotes(now_ms=1_000)) == []

    payload = _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))
    assert payload["paper_order_status"] == PaperOrderState.FILLED.value
    assert payload["paper_fill_assumption"] == "taker_l2_vwap_or_top_book_size_only"
    assert payload["paper_fill_capacity_source"] == "top_book_only"
    assert payload["official_pnl"] is False
    assert payload["long_leg"]["qty"] == pytest.approx(payload["short_leg"]["qty"])
    assert payload["long_leg"]["entry_filled_at_ms"] == 1_001
    assert payload["short_leg"]["entry_filled_at_ms"] == 1_001
    assert payload["residual_base_qty"] == 0.0


def test_taker_uses_coherent_l2_vwap_and_can_close_beyond_top_book_size() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    candidate = _candidate(entry_notional_quote=1_000.0)
    quotes = {
        "cheap:BTCUSDT": _quote(
            "cheap",
            bid=99.9,
            ask=100.0,
            bid_size=1.0,
            ask_size=1.0,
            ask_depth=((100.0, 5.0), (101.0, 5.0)),
            bid_depth=((99.9, 5.0), (99.0, 5.0)),
            observed_at_ms=1_000,
        ),
        "rich:BTCUSDT": _quote(
            "rich",
            bid=101.0,
            ask=101.1,
            bid_size=1.0,
            ask_size=1.0,
            bid_depth=((101.0, 5.0), (100.0, 5.0)),
            ask_depth=((101.1, 5.0), (102.0, 5.0)),
            observed_at_ms=1_000,
        ),
    }

    registered = tracker.register(candidate, quotes, finalist_rank=0)

    assert registered is not None
    payload = _fill_taker(
        tracker,
        now_ms=1_001,
        quotes={key: replace(value, observed_at_ms=1_001) for key, value in quotes.items()},
    )
    assert payload["paper_fill_capacity_source"] == "l2_vwap"
    assert payload["paper_order_status"] == PaperOrderState.FILLED.value
    assert payload["long_leg"]["entry_execution_source"] == "l2_vwap"
    assert payload["short_leg"]["entry_execution_source"] == "l2_vwap"
    # Buying about 9.9 units consumes both ask levels, so the entry is not
    # incorrectly priced at the one-unit best ask.
    assert payload["long_leg"]["entry_raw_price"] > 100.4

    exit_quotes = {
        "cheap:BTCUSDT": _quote(
            "cheap",
            bid=100.0,
            ask=100.1,
            bid_size=1.0,
            ask_size=1.0,
            bid_depth=((100.0, 5.0), (99.5, 5.0)),
            ask_depth=((100.1, 10.0),),
            observed_at_ms=2_001,
        ),
        "rich:BTCUSDT": _quote(
            "rich",
            bid=100.4,
            ask=100.5,
            bid_size=1.0,
            ask_size=1.0,
            bid_depth=((100.4, 10.0),),
            ask_depth=((100.5, 5.0), (101.0, 5.0)),
            observed_at_ms=2_001,
        ),
    }
    closed = tracker.evaluate_due(2_001, exit_quotes)

    assert closed[0]["payload"]["paper_unpriced"] is False
    assert closed[0]["payload"]["paper_exit_capacity_source"] == "l2_vwap"


def test_incoherent_l2_ladder_falls_back_to_conservative_bbo_capacity() -> None:
    tracker = _tracker()
    quotes = _quotes(now_ms=1_000, bid_size=0.1, ask_size=0.1)
    quotes["cheap:BTCUSDT"] = replace(
        quotes["cheap:BTCUSDT"],
        ask_depth=((99.0, 100.0),),  # Cannot be the same book as ask=100.
    )

    event = tracker.register(_candidate(), quotes, finalist_rank=0)

    assert event is not None
    payload = _fill_taker(
        tracker,
        now_ms=1_001,
        quotes={key: replace(value, observed_at_ms=1_001) for key, value in quotes.items()},
    )
    assert payload["paper_fill_capacity_source"] == "top_book_only"
    assert payload["filled_base_qty"] == pytest.approx(0.1)


def test_taker_capacity_creates_matched_partial_fill_and_residual() -> None:
    tracker = _tracker()
    # Requested quantity is about 0.2 BTC; top-book capacity allows 0.1.
    event = tracker.register(
        _candidate(), _quotes(now_ms=1_000, bid_size=0.1, ask_size=0.1), finalist_rank=0
    )

    assert event is not None
    payload = _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(now_ms=1_001, bid_size=0.1, ask_size=0.1),
    )
    assert payload["paper_order_status"] == PaperOrderState.PARTIAL.value
    assert payload["filled_base_qty"] == pytest.approx(0.1)
    assert payload["residual_base_qty"] > 0.0
    assert payload["long_leg"]["qty"] == pytest.approx(payload["short_leg"]["qty"])
    assert payload["official_pnl"] is False


def test_maker_touch_is_not_a_fill_but_strict_cross_fills_with_later_hedge_quote() -> None:
    control_manifest = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST,
        cohorts=tuple(
            replace(cohort, enabled=True)
            if cohort.bot_id == "mt_selected_maker_delay_1000ms"
            else cohort
            for cohort in DEFAULT_SPREAD_RESEARCH_MANIFEST.cohorts
        ),
    )
    tracker = _tracker(
        paper_bot_ids=["mt_selected_maker_delay_1000ms"],
        terminal_secs=5,
        research_manifest=control_manifest,
    )
    event = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)

    assert event is not None
    assert event["payload"]["paper_order_status"] == PaperOrderState.WORKING.value
    # Original maker bid is 99.9; a touch at 99.9 is explicitly non-fill.
    assert tracker.evaluate_due(2_000, _quotes(now_ms=2_000, long_bid=99.9, long_ask=99.9)) == []

    observed = tracker.evaluate_due(
        2_001,
        _quotes(now_ms=2_001, long_bid=99.7, long_ask=99.8, short_bid=100.5, short_ask=100.6),
    )
    assert [item["kind"] for item in observed] == ["opportunity.paper_maker_fill_observed"]
    assert observed[0]["payload"]["paper_order_status"] == PaperOrderState.UNKNOWN.value
    # Markout timing starts from the actual maker fill, rather than the old
    # entry eligibility boundary.
    delayed = tracker.evaluate_due(3_000, _quotes(now_ms=3_000))
    assert delayed == []

    # Replaying the quote observed before the delay boundary must not turn it
    # into a hedge merely because the scheduler has reached that boundary.
    # Its fresh delta markout remains explicitly non-official.
    unhedged = tracker.evaluate_due(3_001, _quotes(now_ms=3_000))
    assert [item["kind"] for item in unhedged] == ["opportunity.paper_delta_markout"]
    assert unhedged[0]["payload"]["official_pnl"] is False

    filled = tracker.evaluate_due(
        3_002,
        _quotes(now_ms=3_002, long_bid=99.7, long_ask=99.8, short_bid=100.5, short_ask=100.6),
    )
    assert [item["kind"] for item in filled] == ["opportunity.paper_hedge_filled"]
    payload = filled[0]["payload"]
    assert payload["paper_order_status"] == PaperOrderState.UNKNOWN.value
    assert payload["official_pnl"] is False
    assert payload["short_leg"]["entry_raw_price"] == pytest.approx(100.5)
    assert payload["short_leg"]["entry_observed_at_ms"] == 3_002
    assert payload["long_leg"]["entry_filled_at_ms"] == 2_001
    assert payload["short_leg"]["entry_filled_at_ms"] == 3_002
    # The hedge-fill event is intentionally a lifecycle record, not a PnL
    # markout.  At the next fully-hedged markout the delay metric must start from the
    # hedge BBO visible when the maker fill occurred (100.5), rather than the
    # earlier decision-time bid (101.0).
    markout = tracker.evaluate_due(4_002, _quotes(now_ms=4_002))
    assert [item["kind"] for item in markout] == ["opportunity.paper_markout"]
    assert markout[0]["payload"]["paper_hedge_delay_quote"] == pytest.approx(0.0)


def test_short_maker_fill_uses_quote_timestamp_for_delayed_hedge() -> None:
    short_maker = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST.cohorts[1],
        enabled=True,
        entry_long_role="taker",
        entry_short_role="maker",
        maker_leg="short",
    )
    manifest = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST,
        cohorts=(DEFAULT_SPREAD_RESEARCH_MANIFEST.cohorts[0], short_maker),
    )
    tracker = _tracker(
        paper_bot_ids=[short_maker.bot_id],
        markout_secs=[99],
        terminal_secs=5,
        research_manifest=manifest,
    )
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)

    maker_quotes = _quotes(now_ms=2_000, short_bid=101.2, short_ask=101.3)
    maker_quotes["cheap:BTCUSDT"] = replace(
        maker_quotes["cheap:BTCUSDT"], observed_at_ms=1_950
    )
    maker_quotes["rich:BTCUSDT"] = replace(
        maker_quotes["rich:BTCUSDT"], observed_at_ms=1_800
    )
    observed = tracker.evaluate_due(2_000, maker_quotes)
    assert [item["kind"] for item in observed] == [
        "opportunity.paper_maker_fill_observed"
    ]
    assert observed[0]["payload"]["maker_fill_observed_at_ms"] == 1_800
    assert observed[0]["payload"]["short_leg"]["entry_filled_at_ms"] == 1_800

    assert tracker.evaluate_due(2_799, _quotes(now_ms=2_799)) == []
    hedged = tracker.evaluate_due(2_800, _quotes(now_ms=2_800))
    assert [item["kind"] for item in hedged] == [
        "opportunity.paper_hedge_filled"
    ]
    assert hedged[0]["payload"]["long_leg"]["entry_filled_at_ms"] == 2_800


def test_taker_pair_preserves_each_quote_timestamp_and_uses_later_pair_completion() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=5)
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    quotes = _quotes(now_ms=1_200)
    quotes["cheap:BTCUSDT"] = replace(
        quotes["cheap:BTCUSDT"], observed_at_ms=1_100
    )
    payload = _fill_taker(tracker, now_ms=1_200, quotes=quotes)

    assert payload["long_leg"]["entry_filled_at_ms"] == 1_100
    assert payload["short_leg"]["entry_filled_at_ms"] == 1_200
    assert payload["evaluated_at_ms"] == 1_200


def test_active_max_hold_starts_when_the_taker_pair_is_complete() -> None:
    tracker = _tracker(
        markout_secs=[99],
        terminal_secs=30,
        active_exit_enabled=True,
        exit_z=0.0,
        stop_z=100.0,
        max_hold_ms=500,
    )
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    quotes = _quotes(now_ms=1_200)
    quotes["cheap:BTCUSDT"] = replace(
        quotes["cheap:BTCUSDT"], observed_at_ms=1_100
    )
    _fill_taker(tracker, now_ms=1_200, quotes=quotes)

    assert tracker.evaluate_due(1_699, _quotes(now_ms=1_699)) == []
    closed = tracker.evaluate_due(1_700, _quotes(now_ms=1_700))

    assert [item["kind"] for item in closed] == ["opportunity.paper_closed"]
    assert closed[0]["payload"]["paper_close_reason"] == "spread_max_hold_elapsed"


def test_funding_before_simulated_entry_cannot_be_allocated_to_paper_position() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=2, quote_ttl_ms=10)
    candidate = _candidate(signal_ts_ms=1_001)
    # The quote is still fresh but was published before the known settlement.
    # A pending paper entry must not fabricate a fill at admission, so this
    # funding debit cannot belong to the candidate either.
    quotes = _quotes(
        now_ms=999,
        long_funding_ts_ms=1_000,
        short_funding_ts_ms=1_000,
        funding_interval_ms=10_000,
    )
    registered = tracker.register(candidate, quotes, finalist_rank=0)

    assert registered is not None
    paper_id = registered["payload"]["paper_id"]
    assert registered["payload"]["long_leg"]["entry_observed_at_ms"] == 999
    assert registered["payload"]["long_leg"]["entry_filled_at_ms"] == 0
    assert tracker.record_funding_settlements([
        FundingSettlement(
            paper_id=paper_id,
            leg_side="long",
            settlement_timestamp_ms=1_000,
            amount_quote=-0.02,
            observed_at_ms=1_002,
            source="exchange_funding_ledger",
        ),
    ]) == []


def test_future_quote_timestamp_is_not_fresh_for_official_paper_execution() -> None:
    tracker = _tracker()

    assert tracker.register(_candidate(), _quotes(now_ms=1_001), finalist_rank=0) is None


def test_taker_fill_requires_quote_observed_after_decision_latency() -> None:
    tracker = _tracker(min_decision_latency_ms=250)
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)

    # Processing a stale-but-still-TTL-valid snapshot at the eligibility time
    # must not retroactively turn it into an executable fill.
    assert tracker.evaluate_due(1_250, _quotes(now_ms=1_100)) == []

    filled = _fill_taker(tracker, now_ms=1_250, quotes=_quotes(now_ms=1_250))
    assert filled["evaluated_at_ms"] == 1_250
    assert filled["long_leg"]["entry_filled_at_ms"] == 1_250
    assert filled["short_leg"]["entry_filled_at_ms"] == 1_250


def test_taker_quote_arriving_after_pending_entry_terminal_never_opens_position() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)

    expired = tracker.evaluate_due(2_002, _quotes(now_ms=2_002))

    assert [item["kind"] for item in expired] == ["opportunity.paper_expired"]
    assert expired[0]["payload"]["paper_skip_reason"] == "entry_not_filled"
    assert tracker.tracked_count == 0


def test_maker_quote_arriving_after_pending_entry_terminal_never_opens_position() -> None:
    manifest = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST,
        cohorts=tuple(
            replace(cohort, enabled=True)
            if cohort.bot_id == "mt_selected_maker_delay_1000ms"
            else cohort
            for cohort in DEFAULT_SPREAD_RESEARCH_MANIFEST.cohorts
        ),
    )
    tracker = _tracker(
        paper_bot_ids=["mt_selected_maker_delay_1000ms"],
        markout_secs=[99],
        terminal_secs=1,
        research_manifest=manifest,
    )
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)

    expired = tracker.evaluate_due(
        2_002,
        _quotes(now_ms=2_002, long_bid=99.7, long_ask=99.8),
    )

    assert [item["kind"] for item in expired] == ["opportunity.paper_expired"]
    assert expired[0]["payload"]["paper_skip_reason"] == "entry_not_filled"
    assert tracker.tracked_count == 0


def test_unhedged_maker_expiry_records_fresh_residual_markout_as_nonofficial() -> None:
    control_manifest = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST,
        cohorts=tuple(
            replace(cohort, enabled=True, hedge_delay_ms=10_000)
            if cohort.bot_id == "mt_selected_maker_delay_1000ms"
            else cohort
            for cohort in DEFAULT_SPREAD_RESEARCH_MANIFEST.cohorts
        ),
    )
    tracker = _tracker(
        paper_bot_ids=["mt_selected_maker_delay_1000ms"],
        terminal_secs=2,
        research_manifest=control_manifest,
    )
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert tracker.evaluate_due(
        1_500,
        _quotes(now_ms=1_500, long_bid=99.7, long_ask=99.8),
    )[0]["kind"] == "opportunity.paper_maker_fill_observed"

    expired = tracker.evaluate_due(
        3_501,
        _quotes(now_ms=3_501, long_bid=99.0, long_ask=99.1),
    )

    assert [item["kind"] for item in expired] == ["opportunity.paper_expired"]
    payload = expired[0]["payload"]
    assert payload["paper_unpriced"] is False
    assert payload["official_pnl"] is False
    assert payload["delta_exposure_base_qty"] > 0.0
    assert payload["paper_residual_quote"] != 0.0


def test_non_finite_exit_bbo_is_unpriced_and_never_official() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))
    quotes = _quotes(now_ms=2_001)
    quotes["rich:BTCUSDT"] = replace(
        quotes["rich:BTCUSDT"], ask=float("nan")
    )

    payload = tracker.evaluate_due(2_001, quotes)[0]["payload"]

    assert payload["paper_unpriced"] is True
    assert payload["paper_net_quote"] is None
    assert payload["official_pnl"] is False


def test_pending_maker_emits_nonofficial_delta_markout_before_delayed_hedge() -> None:
    control_manifest = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST,
        cohorts=tuple(
            replace(cohort, enabled=True, hedge_delay_ms=10_000)
            if cohort.bot_id == "mt_selected_maker_delay_1000ms"
            else cohort
            for cohort in DEFAULT_SPREAD_RESEARCH_MANIFEST.cohorts
        ),
    )
    tracker = _tracker(
        paper_bot_ids=["mt_selected_maker_delay_1000ms"],
        terminal_secs=5,
        research_manifest=control_manifest,
    )
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert tracker.evaluate_due(
        1_500,
        _quotes(now_ms=1_500, long_bid=99.7, long_ask=99.8),
    )[0]["kind"] == "opportunity.paper_maker_fill_observed"

    events = tracker.evaluate_due(
        2_500,
        _quotes(now_ms=2_500, long_bid=99.0, long_ask=99.1),
    )

    assert [item["kind"] for item in events] == ["opportunity.paper_delta_markout"]
    payload = events[0]["payload"]
    assert payload["paper_unpriced"] is False
    assert payload["official_pnl"] is False
    assert payload["delta_exposure_base_qty"] > 0.0
    assert payload["paper_delta_markout_quote"] != 0.0
    assert payload["paper_net_quote"] is None


def test_official_pnl_requires_actual_allocated_settlement_records() -> None:
    evidence = _account_fee_evidence()
    strict = {
        "require_l2_vwap": True,
        "require_account_fee_evidence": True,
        "account_fee_evidence": evidence,
        "markout_secs": [1],
        "terminal_secs": 2,
        "taker_fee_bps_by_venue": {"cheap": 1.0, "rich": 2.0},
        "slippage_buffer_bps": 5.0,
    }
    candidate = replace(
        _candidate(),
        account_fee_evidence_complete=True,
        account_fee_evidence_observed_at_ms=1_000,
        account_fee_evidence_source="account_fee_api",
        account_fee_evidence_fingerprint=evidence.fingerprint_for("cheap", "rich"),
        account_fee_evidence_provenance=evidence.provenance_for("cheap", "rich"),
    )
    tracker = _tracker(**strict)
    registered = tracker.register(
        candidate,
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=5_000,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_with_l2(
            _quotes(
                now_ms=1_001,
                long_funding_ts_ms=1_500,
                short_funding_ts_ms=5_000,
                funding_interval_ms=10_000,
            )
        ),
    )

    # A quoted rate is an estimate, not a settlement debit. Without an actual
    # position-allocated ledger entry the markout remains diagnostic-only.
    first = tracker.evaluate_due(
        3_001,
        _with_l2(
            _quotes(
                now_ms=3_001,
                long_funding_ts_ms=1_500,
                short_funding_ts_ms=5_000,
                funding_interval_ms=10_000,
            )
        ),
    )
    payload = first[0]["payload"]
    assert payload["paper_funding_quote"] == 0.0
    assert payload["funding_settlement_evidence_complete"] is False
    assert payload["official_pnl"] is False

    # Re-open the case and attach the exchange-account fact to the exact paper
    # position/leg. The known short settlement has not happened yet.
    tracker = _tracker(**strict)
    registered = tracker.register(
        candidate,
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=5_000,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_with_l2(
            _quotes(
                now_ms=1_001,
                long_funding_ts_ms=1_500,
                short_funding_ts_ms=5_000,
                funding_interval_ms=10_000,
            )
        ),
    )
    observed = tracker.record_funding_settlements([
        FundingSettlement(
            paper_id=registered["payload"]["paper_id"],
            leg_side="long",
            settlement_timestamp_ms=1_500,
            amount_quote=-0.02,
            observed_at_ms=1_600,
            source="exchange_funding_ledger",
        )
    ])
    assert [item["kind"] for item in observed] == ["opportunity.paper_funding_settlement_observed"]

    events = tracker.evaluate_due(
        3_001,
        _with_l2(
            _quotes(
                now_ms=3_001,
                long_funding_ts_ms=11_500,
                short_funding_ts_ms=5_000,
                funding_interval_ms=10_000,
            )
        ),
    )
    assert [item["kind"] for item in events] == [
        "opportunity.paper_markout",
        "opportunity.paper_closed",
    ]
    payload = events[0]["payload"]
    assert payload["paper_funding_quote"] == pytest.approx(-0.02)
    assert payload["settlement_realized_funding_quote"] == payload["paper_funding_quote"]
    assert payload["paper_funding_quote"] != payload["accrued_funding_estimate_quote"]
    assert payload["funding_settlement_evidence_complete"] is True
    assert payload["settlement_funding_rate_evidence"] == "actual_position_allocated_funding_ledger"
    assert payload["official_pnl"] is True
    assert payload["paper_net_quote"] == pytest.approx(
        payload["paper_gross_quote"]
        + payload["paper_funding_quote"]
        - payload["paper_fee_quote"]
        - payload["paper_slippage_quote"]
        - payload["paper_adverse_selection_assumption_quote"]
    )
    assert tracker.tracked_count == 0


def test_paper_net_includes_frozen_adverse_selection_assumption() -> None:
    tracker = _tracker(markout_secs=[1], terminal_secs=2)
    candidate = replace(_candidate(), adverse_selection_bps=100.0)
    registered = tracker.register(candidate, _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))

    events = tracker.evaluate_due(3_001, _quotes(now_ms=3_001))
    closed = next(event["payload"] for event in events if event["kind"] == "opportunity.paper_closed")

    assert closed["paper_adverse_selection_assumption_quote"] > 0.0
    assert closed["paper_net_quote"] == pytest.approx(
        closed["paper_gross_quote"]
        + closed["paper_funding_quote"]
        - closed["paper_fee_quote"]
        - closed["paper_slippage_quote"]
        - closed["paper_adverse_selection_assumption_quote"]
    )


def test_later_paper_entry_requires_only_post_entry_funding_settlements() -> None:
    tracker = _tracker(markout_secs=[1], terminal_secs=3, quote_ttl_ms=1_000)
    registered = tracker.register(
        _candidate(signal_ts_ms=1_001),
        _quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_000,
            short_funding_ts_ms=1_000,
            funding_interval_ms=1_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    paper_id = registered["payload"]["paper_id"]
    _fill_taker(
        tracker,
        now_ms=1_002,
        quotes=_quotes(
            now_ms=1_002,
            long_funding_ts_ms=1_000,
            short_funding_ts_ms=1_000,
            funding_interval_ms=1_000,
        ),
    )

    observed = tracker.record_funding_settlements([
        FundingSettlement(
            paper_id=paper_id,
            leg_side="long",
            settlement_timestamp_ms=2_000,
            amount_quote=-0.02,
            observed_at_ms=2_001,
            source="exchange_funding_ledger",
        ),
        FundingSettlement(
            paper_id=paper_id,
            leg_side="short",
            settlement_timestamp_ms=2_000,
            amount_quote=0.01,
            observed_at_ms=2_001,
            source="exchange_funding_ledger",
        ),
    ])
    assert len(observed) == 2

    events = tracker.evaluate_due(
        2_002,
        _quotes(
            now_ms=2_002,
            long_funding_ts_ms=3_000,
            short_funding_ts_ms=3_000,
            funding_interval_ms=1_000,
        ),
    )

    payload = events[0]["payload"]
    assert payload["funding_settlement_evidence_complete"] is True
    assert payload["paper_funding_quote"] == pytest.approx(-0.01)
    assert payload["official_pnl"] is False


def test_public_settled_rate_near_settlement_is_allocated_to_paper_legs() -> None:
    tracker = _tracker(markout_secs=[1], terminal_secs=3, quote_ttl_ms=1_000)
    registered = tracker.register(
        _candidate(signal_ts_ms=1_000),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    filled = _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
    )

    observed = tracker.record_observed_public_funding_settlements(
        1_501,
        _quotes(
            now_ms=1_501,
            long_funding_ts_ms=11_500,
            short_funding_ts_ms=11_500,
            funding_interval_ms=10_000,
            long_settled_funding_bps=4.0,
            short_settled_funding_bps=2.0,
            long_mark_price=100.0,
            short_mark_price=101.0,
        ),
    )
    assert [item["kind"] for item in observed] == [
        "opportunity.paper_funding_settlement_observed",
        "opportunity.paper_funding_settlement_observed",
    ]
    assert tracker.record_observed_public_funding_settlements(
        1_501,
        _quotes(
            now_ms=1_501,
            long_funding_ts_ms=11_500,
            short_funding_ts_ms=11_500,
            funding_interval_ms=10_000,
            long_settled_funding_bps=4.0,
            short_settled_funding_bps=2.0,
            long_mark_price=100.0,
            short_mark_price=101.0,
        ),
    ) == []

    events = tracker.evaluate_due(
        2_001,
        _quotes(
            now_ms=2_001,
            long_funding_ts_ms=11_500,
            short_funding_ts_ms=11_500,
            funding_interval_ms=10_000,
        ),
    )
    payload = events[0]["payload"]
    assert payload["funding_settlement_evidence_complete"] is True
    assert payload["official_pnl"] is False
    shared_base_qty = float(filled["long_leg"]["qty"])
    assert shared_base_qty == pytest.approx(
        float(filled["short_leg"]["qty"])
    )
    expected_funding = (
        -4.0 * shared_base_qty * 100.0 / 10_000.0
        + 2.0 * shared_base_qty * 101.0 / 10_000.0
    )
    assert payload["paper_funding_quote"] == pytest.approx(expected_funding)


def test_late_public_settled_rate_never_backfills_official_paper_funding() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=3, quote_ttl_ms=1_000)
    assert tracker.register(
        _candidate(signal_ts_ms=1_000),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    ) is not None
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
    )

    assert tracker.record_observed_public_funding_settlements(
        3_000,
        _quotes(
            now_ms=3_000,
            long_funding_ts_ms=11_500,
            short_funding_ts_ms=11_500,
            funding_interval_ms=10_000,
            long_settled_funding_bps=4.0,
            short_settled_funding_bps=2.0,
            long_mark_price=100.0,
            short_mark_price=101.0,
        ),
    ) == []


def test_funding_interval_change_fails_closed_before_legacy_schedule() -> None:
    """A changed venue cadence can hide an earlier settlement from old plans."""
    tracker = _tracker(markout_secs=[4], terminal_secs=10, quote_ttl_ms=1_000)
    registered = tracker.register(
        _candidate(signal_ts_ms=1_000),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=9_000,
            short_funding_ts_ms=9_000,
            funding_interval_ms=8_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=9_000,
            short_funding_ts_ms=9_000,
            funding_interval_ms=8_000,
        ),
    )

    events = tracker.evaluate_due(
        5_001,
        _quotes(
            now_ms=5_001,
            long_funding_ts_ms=5_000,
            short_funding_ts_ms=5_000,
            funding_interval_ms=4_000,
        ),
    )

    payload = events[0]["payload"]
    assert payload["funding_settlement_evidence_complete"] is False
    assert payload["settlement_funding_rate_evidence"] == "funding_schedule_changed"
    assert payload["paper_funding_quote"] == 0.0
    assert payload["official_pnl"] is False


def test_funding_timestamp_shift_fails_closed_with_unchanged_interval() -> None:
    tracker = _tracker(markout_secs=[4], terminal_secs=10, quote_ttl_ms=1_000)
    registered = tracker.register(
        _candidate(signal_ts_ms=1_000),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=9_000,
            short_funding_ts_ms=9_000,
            funding_interval_ms=8_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=9_000,
            short_funding_ts_ms=9_000,
            funding_interval_ms=8_000,
        ),
    )

    # The venue moves the next settlement time but retains the same interval.
    # There may have been an unobserved cash event, so official PnL must stop.
    events = tracker.evaluate_due(
        5_001,
        _quotes(
            now_ms=5_001,
            long_funding_ts_ms=10_000,
            short_funding_ts_ms=10_000,
            funding_interval_ms=8_000,
        ),
    )

    payload = events[0]["payload"]
    assert payload["funding_settlement_evidence_complete"] is False
    assert payload["settlement_funding_rate_evidence"] == "funding_schedule_changed"
    assert payload["official_pnl"] is False


def test_truthy_non_boolean_in_memory_official_flag_never_enters_acceptance_pnl() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))
    paper_id = registered["payload"]["paper_id"]
    tracker._positions[paper_id] = replace(
        tracker._positions[paper_id],
        official_pnl="false",
    )

    payload = tracker.evaluate_due(2_001, _quotes(now_ms=2_001))[0]["payload"]

    assert payload["official_pnl"] is False


def test_unknown_later_funding_settlement_never_becomes_official_pnl() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=3)
    assert tracker.register(
        _candidate(),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=1_000,
        ),
        finalist_rank=0,
    )
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=1_000,
        ),
    )

    registration = next(iter(tracker._positions.values()))
    tracker.record_funding_settlements([
        FundingSettlement(
            paper_id=registration.paper_id,
            leg_side="long",
            settlement_timestamp_ms=1_500,
            amount_quote=-0.01,
            observed_at_ms=1_600,
            source="exchange_funding_ledger",
        ),
        FundingSettlement(
            paper_id=registration.paper_id,
            leg_side="short",
            settlement_timestamp_ms=1_500,
            amount_quote=0.01,
            observed_at_ms=1_600,
            source="exchange_funding_ledger",
        ),
    ])

    events = tracker.evaluate_due(
        4_001,
        _quotes(
            now_ms=4_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=1_000,
        ),
    )

    payload = events[0]["payload"]
    assert payload["paper_funding_quote"] == pytest.approx(0.0)
    assert payload["funding_settlement_evidence_complete"] is False
    assert payload["official_pnl"] is False


def test_funding_after_terminal_close_boundary_is_not_allocated() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=2)
    registered = tracker.register(
        _candidate(),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=1_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    paper_id = registered["payload"]["paper_id"]
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=1_000,
        ),
    )

    # The terminal horizon is 3,000ms.  A delayed refresh must not let the
    # 3,500ms cash event enter PnL before it notices that the paper position
    # already reached that simulated close boundary.
    assert tracker.record_funding_settlements(
        [
            FundingSettlement(
                paper_id, "long", 3_500, -0.02, 3_600, "exchange_funding_ledger"
            ),
            FundingSettlement(
                paper_id, "short", 3_500, 0.01, 3_600, "exchange_funding_ledger"
            ),
        ]
    ) == []
    position = tracker._positions[paper_id]
    assert position.long_leg.funding_settlements == ()
    assert position.short_leg.funding_settlements == ()


def test_future_observed_funding_ledger_cannot_time_travel_into_paper_pnl() -> None:
    tracker = _tracker(markout_secs=[1, 3], terminal_secs=10)
    registered = tracker.register(
        _candidate(),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    paper_id = registered["payload"]["paper_id"]
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
    )
    tracker.record_funding_settlements([
        FundingSettlement(paper_id, "long", 1_500, -0.02, 4_000, "exchange_funding_ledger"),
        FundingSettlement(paper_id, "short", 1_500, 0.01, 4_000, "exchange_funding_ledger"),
    ])

    before_observation = tracker.evaluate_due(
        3_000,
        _quotes(
            now_ms=3_000,
            long_funding_ts_ms=11_500,
            short_funding_ts_ms=11_500,
            funding_interval_ms=10_000,
        ),
    )[0]["payload"]
    assert before_observation["paper_funding_quote"] == 0.0
    assert before_observation["settlement_funding_rate_evidence"] == "missing_actual_funding_ledger"
    assert before_observation["official_pnl"] is False

    after_observation = tracker.evaluate_due(
        4_001,
        _quotes(
            now_ms=4_001,
            long_funding_ts_ms=11_500,
            short_funding_ts_ms=11_500,
            funding_interval_ms=10_000,
        ),
    )[0]["payload"]
    assert after_observation["paper_funding_quote"] == pytest.approx(-0.01)
    assert after_observation["settlement_funding_rate_evidence"] == "actual_position_allocated_funding_ledger"
    assert after_observation["official_pnl"] is False


def test_conflicting_funding_ledger_amount_fails_closed_without_overwriting_first_fact() -> None:
    tracker = _tracker(markout_secs=[1], terminal_secs=10)
    registered = tracker.register(
        _candidate(),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    paper_id = registered["payload"]["paper_id"]
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
    )
    tracker.record_funding_settlements([
        FundingSettlement(paper_id, "long", 1_500, -0.02, 1_600, "exchange_funding_ledger"),
        FundingSettlement(paper_id, "short", 1_500, 0.01, 1_600, "exchange_funding_ledger"),
    ])
    tracker.record_funding_settlements([
        FundingSettlement(paper_id, "long", 1_500, -9.99, 1_700, "exchange_funding_ledger"),
    ])

    payload = tracker.evaluate_due(
        2_001,
        _quotes(now_ms=2_001, long_funding_ts_ms=1_500, short_funding_ts_ms=1_500, funding_interval_ms=10_000),
    )[0]["payload"]
    assert payload["paper_funding_quote"] == pytest.approx(-0.01)
    assert payload["long_leg"]["funding_settlement_conflict"] is True
    assert payload["settlement_funding_rate_evidence"] == "conflicting_actual_funding_ledger"
    assert payload["funding_settlement_evidence_complete"] is False
    assert payload["official_pnl"] is False


def test_nonfinite_funding_ledger_amount_is_rejected_before_it_can_poison_pnl() -> None:
    tracker = _tracker(markout_secs=[1], terminal_secs=10)
    registered = tracker.register(
        _candidate(),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    paper_id = registered["payload"]["paper_id"]
    _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
    )

    assert tracker.record_funding_settlements([
        FundingSettlement(paper_id, "long", 1_500, float("nan"), 1_600, "exchange_funding_ledger"),
        FundingSettlement(paper_id, "short", 1_500, float("inf"), 1_600, "exchange_funding_ledger"),
    ]) == []

    payload = tracker.evaluate_due(
        2_001,
        _quotes(
            now_ms=2_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
    )[0]["payload"]
    assert payload["paper_funding_quote"] == 0.0
    assert payload["official_pnl"] is False
    assert isfinite(float(payload["paper_net_quote"]))


def test_restore_skips_nonfinite_persisted_leg_quantity() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=10)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    record = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "long_leg": {
                **registered["payload"]["long_leg"],
                "qty": float("nan"),
            },
        },
    }

    restored = _tracker(markout_secs=[99], terminal_secs=10)
    restored.restore_from_records([record])
    assert restored.tracked_count == 0


def test_restore_rejects_missing_or_negative_frozen_exit_cost() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=10)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None

    missing_exit_fee = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "long_leg": {
                key: value
                for key, value in registered["payload"]["long_leg"].items()
                if key != "exit_fee_bps"
            },
        },
    }
    negative_exit_fee = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "short_leg": {
                **registered["payload"]["short_leg"],
                "exit_fee_bps": -0.1,
            },
        },
    }

    restored = _tracker(markout_secs=[99], terminal_secs=10)
    restored.restore_from_records([missing_exit_fee, negative_exit_fee])

    assert restored.tracked_count == 0


def test_restore_keeps_frozen_fee_schedule_after_active_config_changes() -> None:
    tracker = _tracker(
        markout_secs=[99],
        terminal_secs=10,
        taker_fee_bps_by_venue={"cheap": 1.0, "rich": 2.0},
    )
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    filled = _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))

    restored = _tracker(markout_secs=[99], terminal_secs=10)
    restored.config = replace(
        restored.config,
        taker_fee_bps_by_venue={},
        maker_fee_bps_by_venue={},
    )
    restored.restore_from_records([
        registered,
        {"kind": "opportunity.paper_taker_pair_filled", "payload": filled},
    ])

    # A refresh changes only future admissions.  A v8 journal row carries its
    # complete contract, so recovery continues it under the original costs.
    assert restored.tracked_count == 1


def _v3_official_replay_records() -> tuple[list[dict], dict[str, object]]:
    """Build one official v3 fill plus the exact active cohort contract."""
    evidence = _account_fee_evidence(schema_v3=True)
    manifest = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST,
        model_epoch="v3_cost_normalized_reversion",
    )
    strict: dict[str, object] = {
        "require_l2_vwap": True,
        "require_account_fee_evidence": True,
        "account_fee_evidence": evidence,
        "fee_evidence_account_identity_hashes": {
            "cheap": "a" * 64,
            "rich": "b" * 64,
        },
        "model_epoch": "v3_cost_normalized_reversion",
        "research_manifest": manifest,
        "markout_secs": [1],
        "terminal_secs": 3,
        "oos_start_ms": 1_000,
        "require_out_of_sample": True,
        "taker_fee_bps_by_venue": {"cheap": 1.0, "rich": 2.0},
    }
    candidate = replace(
        _candidate(),
        calculation_version="spread_v3_cost_normalized_reversion",
        model_epoch="v3_cost_normalized_reversion",
        account_fee_evidence_complete=True,
        account_fee_evidence_observed_at_ms=1_000,
        account_fee_evidence_source="account_fee_api",
        account_fee_evidence_fingerprint=evidence.fingerprint_for("cheap", "rich"),
        account_fee_evidence_provenance=evidence.provenance_for("cheap", "rich"),
    )
    tracker = _tracker(**strict)
    registered = tracker.register(candidate, _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    filled = _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_with_l2(_quotes(now_ms=1_001)),
    )
    assert filled["official_pnl"] is True
    return [registered, {"kind": "opportunity.paper_taker_pair_filled", "payload": filled}], strict


@pytest.mark.parametrize("cohort_change", ["manifest", "account_identity"])
def test_restore_downgrades_v3_official_position_after_active_cohort_change(
    cohort_change: str,
) -> None:
    records, strict = _v3_official_replay_records()
    if cohort_change == "manifest":
        strict["research_manifest"] = replace(
            strict["research_manifest"],  # type: ignore[arg-type]
            hypothesis="changed hypothesis with the same manifest version",
        )
    else:
        strict["fee_evidence_account_identity_hashes"] = {
            "cheap": "a" * 64,
            "rich": "d" * 64,
        }

    restored = _tracker(**strict)
    restored.restore_from_records(records)

    # Keep reproducible frozen economics for diagnostics, but a restart under
    # another research contract or account must never emit an official sample.
    assert restored.tracked_count == 1
    restored_position = next(iter(restored._positions.values()))
    assert restored_position.official_pnl is False


def test_restore_keeps_v3_official_position_only_for_exact_active_cohort() -> None:
    records, strict = _v3_official_replay_records()
    restored = _tracker(**strict)
    restored.restore_from_records(records)

    restored_position = next(iter(restored._positions.values()))
    assert restored_position.official_pnl is True


def test_restore_keeps_nonterminal_evaluation_skip_and_its_horizon() -> None:
    records, strict = _v3_official_replay_records()
    tracker = _tracker(**strict)
    tracker.restore_from_records(records)
    quotes = _with_l2(_quotes(now_ms=2_001))
    quotes["rich:BTCUSDT"] = replace(
        quotes["rich:BTCUSDT"],
        observed_at_ms=1_000,
    )

    skipped = tracker.evaluate_due(2_001, quotes)

    assert [event["kind"] for event in skipped] == [
        "opportunity.paper_evaluation_skipped"
    ]
    restored = _tracker(**strict)
    restored.restore_from_records(records + skipped)

    # A normal, nonterminal inability to price must survive restart without
    # disabling new admission or re-emitting the same scheduled horizon.
    assert restored.enabled is True
    assert restored.tracked_count == 1
    assert restored.evaluate_due(2_001, _with_l2(_quotes(now_ms=2_001))) == []


def test_restore_keeps_maker_leg_funding_before_delayed_hedge() -> None:
    control_manifest = replace(
        DEFAULT_SPREAD_RESEARCH_MANIFEST,
        cohorts=tuple(
            replace(cohort, enabled=True)
            if cohort.bot_id == "mt_selected_maker_delay_1000ms"
            else cohort
            for cohort in DEFAULT_SPREAD_RESEARCH_MANIFEST.cohorts
        ),
    )
    tracker = _tracker(
        paper_bot_ids=["mt_selected_maker_delay_1000ms"],
        terminal_secs=5,
        research_manifest=control_manifest,
    )
    registered = tracker.register(
        _candidate(),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=2_500,
            short_funding_ts_ms=2_500,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    maker_fill = tracker.evaluate_due(
        2_001,
        _quotes(
            now_ms=2_001,
            long_bid=99.7,
            long_ask=99.8,
            short_bid=100.5,
            short_ask=100.6,
            long_funding_ts_ms=2_500,
            short_funding_ts_ms=2_500,
            funding_interval_ms=10_000,
        ),
    )
    assert [event["kind"] for event in maker_fill] == [
        "opportunity.paper_maker_fill_observed"
    ]
    funding = tracker.record_funding_settlements(
        [
            FundingSettlement(
                paper_id=registered["payload"]["paper_id"],
                leg_side="long",
                settlement_timestamp_ms=2_500,
                amount_quote=-0.01,
                observed_at_ms=2_501,
                source="exchange_funding_ledger",
            )
        ]
    )
    assert [event["kind"] for event in funding] == [
        "opportunity.paper_funding_settlement_observed"
    ]

    restored = _tracker(
        paper_bot_ids=["mt_selected_maker_delay_1000ms"],
        terminal_secs=5,
        research_manifest=control_manifest,
    )
    restored.restore_from_records([registered, *maker_fill, *funding])

    assert restored.enabled is True
    assert restored.tracked_count == 1


def test_restore_rejects_active_max_hold_before_frozen_holding_boundary() -> None:
    tracker = _tracker(
        markout_secs=[99],
        terminal_secs=5,
        active_exit_enabled=True,
        max_hold_ms=500,
    )
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    filled = _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))
    closed = tracker.evaluate_due(1_501, _quotes(now_ms=1_501))
    assert [event["kind"] for event in closed] == ["opportunity.paper_closed"]
    assert closed[0]["payload"]["horizon_kind"] == "active_exit:max_hold"

    restored = _tracker(
        markout_secs=[99],
        terminal_secs=5,
        active_exit_enabled=True,
        max_hold_ms=500,
    )
    restored.restore_from_records(
        [
            registered,
            {"kind": "opportunity.paper_taker_pair_filled", "payload": filled},
            closed[0],
        ]
    )
    assert restored.enabled is True

    early_close = {
        **closed[0],
        "payload": {**closed[0]["payload"], "evaluated_at_ms": 1_002},
    }
    restored = _tracker(
        markout_secs=[99],
        terminal_secs=5,
        active_exit_enabled=True,
        max_hold_ms=500,
    )
    restored.restore_from_records(
        [
            registered,
            {"kind": "opportunity.paper_taker_pair_filled", "payload": filled},
            early_close,
        ]
    )
    assert restored.enabled is False


def test_v3_offline_acceptance_replays_real_frozen_lifecycle() -> None:
    records, strict = _v3_official_replay_records()
    tracker = _tracker(**strict)
    tracker.restore_from_records(records)
    closed = tracker.evaluate_due(4_001, _with_l2(_quotes(now_ms=4_001)))
    assert [event["kind"] for event in closed] == [
        "opportunity.paper_markout",
        "opportunity.paper_closed",
    ]

    journal: list[dict] = []
    for sequence, event in enumerate(
        [records[0], records[1], *closed],
        start=1,
    ):
        journal.append(
            {
                **event,
                "run_id": "v3-offline-replay",
                "seq": sequence,
                "ts_ms": 1_000 + sequence,
            }
        )
    report = analyze_spread_paper_events(
        journal,
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )

    assert report.journal_replay_integrity_count == 0
    assert report.excluded_nonofficial_count == 1

    spliced_terminal = {
        **journal[-1],
        "payload": {
            **journal[-1]["payload"],
            "candidate_id": "spread:OTHERUSDT:cheap->rich",
        },
    }
    spliced = analyze_spread_paper_events(
        [*journal[:-1], spliced_terminal],
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )
    assert spliced.journal_replay_integrity_count >= 1
    replay = _tracker(**strict)
    replay.restore_from_records([records[0], records[1], closed[0], spliced_terminal])
    assert replay.enabled is False

    malformed_markout = {
        **journal[2],
        # A fill snapshot is a valid position record but not a valid horizon
        # observation: it has no due-horizon owner to mark as emitted.
        "payload": dict(journal[1]["payload"]),
    }
    malformed_horizon = analyze_spread_paper_events(
        [journal[0], journal[1], malformed_markout, journal[-1]],
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )
    assert malformed_horizon.journal_replay_integrity_count >= 1
    replay = _tracker(**strict)
    replay.restore_from_records([records[0], records[1], malformed_markout])
    assert replay.enabled is False

    early_markout = {
        **journal[2],
        "payload": {**journal[2]["payload"], "evaluated_at_ms": 2_000},
    }
    early_horizon = analyze_spread_paper_events(
        [journal[0], journal[1], early_markout, journal[-1]],
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )
    assert early_horizon.journal_replay_integrity_count >= 1

    malformed_fill = {
        **journal[1],
        "payload": {
            **journal[1]["payload"],
            "execution_contract": {
                **journal[1]["payload"]["execution_contract"],
                # JSON bool must not coerce to the strict integer contract.
                "terminal_secs": True,
            },
        },
    }
    malformed = analyze_spread_paper_events(
        [journal[0], malformed_fill, *journal[2:]],
        model_epoch="v3_cost_normalized_reversion",
        require_out_of_sample=True,
        source_evidence_verified=True,
    )

    assert malformed.journal_replay_integrity_count >= 1
    assert malformed.acceptance_ready is False


@pytest.mark.parametrize("cohort_change", ["manifest", "account_identity"])
def test_restored_pending_v3_position_cannot_repromote_after_active_cohort_change(
    cohort_change: str,
) -> None:
    records, strict = _v3_official_replay_records()
    if cohort_change == "manifest":
        strict["research_manifest"] = replace(
            strict["research_manifest"],  # type: ignore[arg-type]
            hypothesis="changed hypothesis with the same manifest version",
        )
    else:
        strict["fee_evidence_account_identity_hashes"] = {
            "cheap": "a" * 64,
            "rich": "d" * 64,
        }

    restored = _tracker(**strict)
    # Replay only registration: the old cohort is still pending at restart.
    restored.restore_from_records(records[:1])
    events = restored.evaluate_due(
        1_001,
        _with_l2(_quotes(now_ms=1_001)),
    )

    assert len(events) == 1
    assert events[0]["kind"] == "opportunity.paper_taker_pair_filled"
    assert events[0]["payload"]["official_pnl"] is False
    assert next(iter(restored._positions.values())).official_pnl is False


@pytest.mark.parametrize("cohort_change", ["manifest", "account_identity"])
def test_active_v3_position_is_downgraded_at_same_cohort_boundary_as_restart(
    cohort_change: str,
) -> None:
    records, strict = _v3_official_replay_records()
    tracker = _tracker(**strict)
    tracker.restore_from_records(records)
    assert next(iter(tracker._positions.values())).official_pnl is True
    if cohort_change == "manifest":
        tracker.config = replace(
            tracker.config,
            research_manifest=replace(
                tracker.config.research_manifest,
                hypothesis="changed hypothesis with the same manifest version",
            ),
        )
    else:
        tracker.config = replace(
            tracker.config,
            fee_evidence_account_identity_hashes={
                "cheap": "a" * 64,
                "rich": "d" * 64,
            },
        )

    events = tracker.evaluate_due(
        2_001,
        _with_l2(_quotes(now_ms=2_001)),
    )

    assert len(events) == 1
    assert events[0]["payload"]["official_pnl"] is False
    assert next(iter(tracker._positions.values())).official_pnl is False


def test_restore_requires_current_journal_and_candidate_economics_proof() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None

    legacy_schema = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "journal_schema_version": 1,
        },
    }
    missing_fee_proof = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "candidate_snapshot": {
                **registered["payload"]["candidate_snapshot"],
                "fee_evidence_complete": False,
            },
        },
    }

    restored = _tracker(markout_secs=[99], terminal_secs=1)
    restored.restore_from_records([legacy_schema, missing_fee_proof])

    assert restored.tracked_count == 0
    assert restored.evaluate_due(2_000, _quotes(now_ms=2_000)) == []


def test_restore_rejects_truthy_string_booleans_at_journal_boundary() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None

    forged = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "official_pnl": "false",
            "candidate_snapshot": {
                **registered["payload"]["candidate_snapshot"],
                "fee_evidence_complete": "false",
            },
        },
    }

    restored = _tracker(markout_secs=[99], terminal_secs=1)
    restored.restore_from_records([forged])

    assert restored.tracked_count == 0
    assert restored.evaluate_due(2_000, _quotes(now_ms=2_000)) == []


def test_restore_rejects_coercible_execution_contract_values() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    contract = registered["payload"]["execution_contract"]
    assert _execution_contract_from_payload(contract) is not None

    bool_as_int = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "execution_contract": {**contract, "terminal_secs": True},
        },
    }
    int_as_float = {
        "kind": registered["kind"],
        "payload": {
            **registered["payload"],
            "execution_contract": {
                **contract,
                "taker_fee_bps_by_venue": {
                    **contract["taker_fee_bps_by_venue"],
                    "cheap": 1,
                },
            },
        },
    }

    assert _execution_contract_from_payload(
        bool_as_int["payload"]["execution_contract"]
    ) is None
    assert _execution_contract_from_payload(
        int_as_float["payload"]["execution_contract"]
    ) is None
    restored = _tracker(markout_secs=[99], terminal_secs=1)
    restored.restore_from_records([bool_as_int, int_as_float])

    assert restored.tracked_count == 0
    assert restored.evaluate_due(2_000, _quotes(now_ms=2_000)) == []


def test_restore_poisoned_later_state_record_cannot_revive_prior_pending_position() -> None:
    """A damaged replay update must remove, not retain, its old registration."""
    tracker = _tracker(markout_secs=[99], terminal_secs=10)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    filled = _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))
    corrupted_filled = {
        "kind": "opportunity.paper_taker_pair_filled",
        "payload": {
            **filled,
            "execution_contract": {
                **filled["execution_contract"],
                "terminal_secs": True,
            },
        },
    }

    restored = _tracker(markout_secs=[99], terminal_secs=10)
    restored.restore_from_records([registered, corrupted_filled])

    assert restored.tracked_count == 0
    assert restored.evaluate_due(2_001, _quotes(now_ms=2_001)) == []


@pytest.mark.parametrize("payload", ["truncated", {}])
def test_restore_rejects_unscoped_corrupt_state_event(
    payload: object,
) -> None:
    """A malformed state transition cannot safely be assigned to one paper id."""
    tracker = _tracker(markout_secs=[99], terminal_secs=10)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    corrupt = {
        "kind": "opportunity.paper_taker_pair_filled",
        "payload": payload,
    }

    restored = _tracker(markout_secs=[99], terminal_secs=10)
    restored.restore_from_records([registered, corrupt])

    assert restored.tracked_count == 0
    assert restored.evaluate_due(2_000, _quotes(now_ms=2_000)) == []


def test_restore_rejects_unknown_paper_namespace_event_and_disables_admission() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=10)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    unknown = {
        "kind": "opportunity.paper_closed_corrupt",
        "payload": {"paper_id": registered["payload"]["paper_id"]},
    }

    restored = _tracker(markout_secs=[99], terminal_secs=10)
    restored.restore_from_records([registered, unknown])

    assert restored.enabled is False
    assert restored.tracked_count == 0
    assert restored.register(_candidate(), _quotes(now_ms=2_000), finalist_rank=0) is None


def test_strict_restore_rejects_reversed_journal_sequence_before_state_replay() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=10)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    paper_id = registered["payload"]["paper_id"]
    records = [
        {
            "run_id": "paper-run",
            "seq": 2,
            "ts_ms": 1_002,
            "kind": "opportunity.paper_closed",
            "payload": {"paper_id": paper_id, "horizon_kind": "terminal_10s"},
        },
        {
            "run_id": "paper-run",
            "seq": 1,
            "ts_ms": 1_001,
            "kind": registered["kind"],
            "payload": registered["payload"],
        },
    ]

    restored = _tracker(markout_secs=[99], terminal_secs=10)
    restored.restore_from_records(records, require_journal_envelope=True)

    assert restored.enabled is False
    assert restored.tracked_count == 0


def test_restore_rejects_terminal_then_reregistered_paper_id() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=10)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    paper_id = registered["payload"]["paper_id"]
    records = [
        registered,
        {
            "kind": "opportunity.paper_closed",
            "payload": {"paper_id": paper_id, "horizon_kind": "terminal_10s"},
        },
        registered,
    ]

    restored = _tracker(markout_secs=[99], terminal_secs=10)
    restored.restore_from_records(records)

    assert restored.enabled is False
    assert restored.tracked_count == 0


def test_restore_deduplicates_settlement_replay_and_marks_amount_conflict() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    registered = tracker.register(
        _candidate(),
        _quotes(
            now_ms=1_000,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
        finalist_rank=0,
    )
    assert registered is not None
    filled = _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
    )
    payload = filled
    paper_id = payload["paper_id"]
    long_settlement = {
        "paper_id": paper_id,
        "leg_side": "long",
        "settlement_timestamp_ms": 1_500,
        "amount_quote": 1.0,
        "observed_at_ms": 1_600,
        "source": "exchange_funding_ledger",
    }
    forged = {
        "kind": "opportunity.paper_taker_pair_filled",
        "payload": {
            **payload,
            "long_leg": {
                **payload["long_leg"],
                "funding_settlements": [
                    long_settlement,
                    {**long_settlement, "amount_quote": 9.0},
                ],
            },
            "short_leg": {
                **payload["short_leg"],
                "funding_settlements": [
                    {
                        **long_settlement,
                        "leg_side": "short",
                        "amount_quote": 0.0,
                    }
                ],
            },
        },
    }

    restored = _tracker(markout_secs=[99], terminal_secs=1)
    restored.restore_from_records([forged])
    events = restored.evaluate_due(
        2_001,
        _quotes(
            now_ms=2_001,
            long_funding_ts_ms=1_500,
            short_funding_ts_ms=1_500,
            funding_interval_ms=10_000,
        ),
    )

    assert len(events) == 1
    assert events[0]["payload"]["paper_funding_quote"] == pytest.approx(1.0)
    assert events[0]["payload"]["official_pnl"] is False


def test_restore_diagnostic_position_never_upgrades_to_official_status() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    entry_quotes = _quotes(
        now_ms=1_000,
        long_funding_ts_ms=10_000,
        short_funding_ts_ms=10_000,
        funding_interval_ms=10_000,
    )
    registered = tracker.register(_candidate(), entry_quotes, finalist_rank=0)
    assert registered is not None
    filled = _fill_taker(
        tracker,
        now_ms=1_001,
        quotes=_quotes(
            now_ms=1_001,
            long_funding_ts_ms=10_000,
            short_funding_ts_ms=10_000,
            funding_interval_ms=10_000,
        ),
    )

    restored = _tracker(markout_secs=[99], terminal_secs=1)
    restored.restore_from_records([
        registered,
        {"kind": "opportunity.paper_taker_pair_filled", "payload": filled},
    ])
    events = restored.evaluate_due(
        2_001,
        _quotes(
            now_ms=2_001,
            long_funding_ts_ms=10_000,
            short_funding_ts_ms=10_000,
            funding_interval_ms=10_000,
        ),
    )

    assert restored.tracked_count == 0
    assert len(events) == 1
    assert events[0]["payload"]["official_pnl"] is False


def test_restore_maker_leg_cannot_forge_taker_baseline_mode() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None

    payload = _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))
    forged = {
        "kind": "opportunity.paper_taker_pair_filled",
        "payload": {
            **payload,
            # The reporting mode is intentionally left untouched.  The
            # restore boundary must inspect leg-level execution evidence.
            "long_leg": {
                **payload["long_leg"],
                "entry_liquidity_role": "maker",
                "entry_execution_source": "maker_bbo_unknown",
            },
        },
    }

    restored = _tracker(markout_secs=[99], terminal_secs=1)
    restored.restore_from_records([forged])
    events = restored.evaluate_due(2_001, _quotes(now_ms=2_001))

    assert len(events) == 1
    assert events[0]["payload"]["official_pnl"] is False


def test_exit_fee_uses_actual_slipped_exit_cash_price() -> None:
    tracker = _tracker(
        markout_secs=[1],
        terminal_secs=10,
        taker_fee_bps_by_venue={"cheap": 1.0, "rich": 2.0},
        slippage_buffer_bps=5.0,
    )
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))

    payload = tracker.evaluate_due(2_001, _quotes(now_ms=2_001))[0]["payload"]
    long_leg = payload["long_leg"]
    short_leg = payload["short_leg"]
    expected = (
        long_leg["qty"] * long_leg["exit_price"] * 1.0 / 10_000.0
        + short_leg["qty"] * short_leg["exit_price"] * 2.0 / 10_000.0
    )
    assert payload["paper_exit_fee_quote"] == pytest.approx(expected)


def test_stale_or_missing_exit_quote_is_unpriced_and_never_official() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))

    events = tracker.evaluate_due(2_001, {})
    assert [item["kind"] for item in events] == ["opportunity.paper_closed"]
    payload = events[0]["payload"]
    assert payload["paper_unpriced"] is True
    assert payload["paper_net_quote"] is None
    assert payload["official_pnl"] is False
    assert tracker.tracked_count == 0


def test_exit_top_book_capacity_cannot_price_an_entire_paper_close() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1)
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))

    # Entry filled roughly 0.2 base units.  A fresh exit BBO with only 0.1
    # visible on either consuming side must not pretend it filled the whole
    # pair at that price.
    payload = tracker.evaluate_due(
        2_001,
        _quotes(now_ms=2_001, bid_size=0.1, ask_size=0.1),
    )[0]["payload"]

    assert payload["paper_unpriced"] is True
    assert payload["paper_skip_reason"] == "exit_top_book_capacity_insufficient"
    assert payload["paper_net_quote"] is None
    assert payload["official_pnl"] is False


def test_exit_quotes_within_ttl_but_outside_cross_venue_skew_are_unpriced() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1, quote_ttl_ms=1_000, quote_skew_ms=250)
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))
    quotes = _quotes(now_ms=2_001)
    quotes["rich:BTCUSDT"] = replace(
        quotes["rich:BTCUSDT"], observed_at_ms=1_501
    )

    payload = tracker.evaluate_due(2_001, quotes)[0]["payload"]
    assert payload["paper_unpriced"] is True
    assert payload["official_pnl"] is False


def test_active_exit_uses_fresh_quotes_and_closes_before_terminal() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=30, active_exit_enabled=True, exit_z=0.5)
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))

    events = tracker.evaluate_due(
        2_000,
        _quotes(now_ms=2_000, long_bid=100.0, long_ask=100.1, short_bid=100.0, short_ask=100.1),
    )
    assert [item["kind"] for item in events] == ["opportunity.paper_closed"]
    assert events[0]["payload"]["paper_close_reason"] == "spread_converged"
    assert events[0]["payload"]["paper_unpriced"] is False


def test_unsupported_legacy_bot_ids_do_not_create_hidden_cohorts() -> None:
    tracker = _tracker(paper_bot_ids=["core_v1_bot", "bad_pair_control_bot"])
    rejection_counts: dict[str, int] = {}

    assert tracker.register_many(
        _candidate(),
        _quotes(now_ms=1_000),
        finalist_rank=0,
        rejection_counts=rejection_counts,
    ) == []
    assert rejection_counts == {"paper_bot_configuration_empty": 1}


def test_paper_admission_attributes_exact_minimum_rejection() -> None:
    tracker = _tracker()
    quotes = _quotes(now_ms=1_000)
    quotes["cheap:BTCUSDT"] = replace(
        quotes["cheap:BTCUSDT"], min_notional_quote=1_000.0
    )
    rejection_counts: dict[str, int] = {}

    assert tracker.register_many(
        _candidate(),
        quotes,
        finalist_rank=0,
        rejection_counts=rejection_counts,
    ) == []
    assert rejection_counts == {"paper_min_notional_not_met": 1}


def test_episode_is_deduplicated_and_restores_from_journal() -> None:
    tracker = _tracker(markout_secs=[99], terminal_secs=1, episode_cooldown_ms=10_000)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    filled = _fill_taker(tracker, now_ms=1_001, quotes=_quotes(now_ms=1_001))
    closed = tracker.evaluate_due(2_001, _quotes(now_ms=2_001))

    restored = _tracker(markout_secs=[99], terminal_secs=1, episode_cooldown_ms=10_000)
    restored.restore_from_records([
        registered,
        {"kind": "opportunity.paper_taker_pair_filled", "payload": filled},
        *closed,
    ])
    assert restored.tracked_count == 0
    assert restored.register(_candidate(signal_ts_ms=5_000), _quotes(now_ms=5_000), finalist_rank=0) is None
    assert restored.register(_candidate(signal_ts_ms=11_001), _quotes(now_ms=11_001), finalist_rank=0) is not None
