from __future__ import annotations

import math
import zlib

import pytest

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadStatsTracker,
    build_spread_reversion_candidates,
)


_MEAN_REVERTING_HISTORY = [
    8, 6, 4, 3, 2, 1, 0, -1, -2, -1,
    0, 1, 2, 1, 0, -1, -2, -1, 0, 1,
    2, 3, 2, 1, 0, -1, -2, -1, 0, 1,
]


def _quote(
    venue: str,
    *,
    bid: float,
    ask: float,
    observed_at_ms: int,
    symbol: str = "BTCUSDT",
    bid_size: float = 10.0,
    ask_size: float = 10.0,
    **contract: object,
) -> QuoteSnapshot:
    contract.setdefault("underlying", "BTC")
    contract.setdefault("quote_currency", "USDT")
    contract.setdefault("contract_type", "linear")
    contract.setdefault("contract_multiplier", 1.0)
    contract.setdefault("mark_index_source", "venue_mark")
    contract.setdefault("price_precision", 2)
    contract.setdefault("quantity_precision", 3)
    contract.setdefault("price_tick", 0.01)
    contract.setdefault("quantity_step_base", 0.001)
    contract.setdefault("min_quantity_base", 0.001)
    contract.setdefault("min_notional_quote", 0.1)
    contract.setdefault("min_notional_evidence_complete", True)
    contract.setdefault("contract_normalization_complete", True)
    contract.setdefault("funding_timestamp_ms", observed_at_ms + 3_600_000)
    contract.setdefault("funding_interval_ms", 28_800_000)
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
        bid=bid,
        ask=ask,
        observed_at_ms=observed_at_ms,
        bid_size=bid_size,
        ask_size=ask_size,
        **contract,
    )


def _quotes_for_signed_basis(
    basis_bps: float,
    *,
    now_ms: int,
    symbol: str = "BTCUSDT",
    half_spread: float = 0.01,
    **contract: object,
) -> dict[str, QuoteSnapshot]:
    # If a=100 and d=(a-b)/((a+b)/2)*10_000, solve b exactly.
    a_mid = 100.0
    b_mid = a_mid * (1.0 - basis_bps / 20_000.0) / (1.0 + basis_bps / 20_000.0)
    return {
        f"cheap:{symbol}": _quote(
            "cheap", bid=a_mid - half_spread, ask=a_mid + half_spread,
            observed_at_ms=now_ms, symbol=symbol, **contract,
        ),
        f"rich:{symbol}": _quote(
            "rich", bid=b_mid - half_spread, ask=b_mid + half_spread,
            observed_at_ms=now_ms, symbol=symbol, **contract,
        ),
    }


def _config(**overrides: object) -> SpreadReversionConfig:
    values: dict[str, object] = {
        "min_samples": len(_MEAN_REVERTING_HISTORY),
        "min_history_ms": 0,
        "min_fair_price_confidence": 0.0,
        "min_liquidity_capacity_ratio": 1.0,
        "entry_z": 2.0,
        "exit_z": 0.5,
        "min_net_edge_bps": 0.0,
        "live_notional_quote": 20.0,
        "max_gross_quote": 20.0,
        "slippage_reserve_bps": 0.0,
        "adverse_selection_buffer_bps": 0.0,
        "taker_fee_bps_by_venue": {"cheap": 0.0, "rich": 0.0},
        "signal_ttl_ms": 5_000,
        "quote_skew_ms": 1_000,
        # Focused signal tests advance independent market evidence once per
        # second. Preserve that intended cadence while production's six-hour /
        # 7,200-sample contract derives a three-second interval.
        "stats_window_ms": 7_200_000,
        "stats_max_samples": 7_200,
    }
    values.update(overrides)
    return SpreadReversionConfig(**values)


def _prewarm(tracker: SpreadStatsTracker, *, center: float = 0.0) -> None:
    for index, value in enumerate(_MEAN_REVERTING_HISTORY, start=1):
        tracker.update(
            "BTCUSDT",
            "cheap",
            "rich",
            center + value,
            observed_at_ms=index * 1_000,
            exit_half_spread_bps=2.0,
        )


def test_signed_basis_pair_identity_does_not_flip_at_zero_crossing() -> None:
    tracker = SpreadStatsTracker()
    for index, value in enumerate([-4.0, -1.0, 1.0, 4.0], start=1):
        tracker.update("BTCUSDT", "rich", "cheap", value, observed_at_ms=index * 1_000)

    state = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=4_000)
    assert state is not None
    assert state.sample_count == 4
    assert state.first_observed_ms == 1_000


def test_stats_snapshot_is_computed_once_per_accepted_observation(monkeypatch) -> None:
    import lightfee.spread.reversion as reversion

    calls = 0
    original = reversion._robust_location_scale

    def counted(values):
        nonlocal calls
        calls += 1
        return original(values)

    monkeypatch.setattr(reversion, "_robust_location_scale", counted)
    tracker = SpreadStatsTracker(window_ms=1_000)

    first = tracker.update(
        "BTCUSDT",
        "cheap",
        "rich",
        1.0,
        observed_at_ms=1_000,
    )
    assert calls == 1
    assert tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=1_000) == first
    assert calls == 1

    second = tracker.update(
        "BTCUSDT",
        "cheap",
        "rich",
        2.0,
        observed_at_ms=2_000,
    )
    assert calls == 2
    assert tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=2_000) == second
    assert calls == 2

    expired = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=3_001)
    assert expired is not None
    assert expired.sample_count == 0


def test_robust_model_recompute_is_phase_staggered_across_pairs(monkeypatch) -> None:
    import lightfee.spread.reversion as reversion

    calls = 0
    original = reversion._robust_location_scale

    def counted(values):
        nonlocal calls
        calls += 1
        return original(values)

    monkeypatch.setattr(reversion, "_robust_location_scale", counted)
    tracker = SpreadStatsTracker()
    for index in range(254):
        for sample_index in range(3):
            tracker.update(
                f"S{index:03d}",
                "alpha",
                "beta",
                float(sample_index),
                observed_at_ms=90_000 + sample_index * 3_000,
            )
    tracker.configure(
        SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
        )
    )
    started_at_ms = 100_000
    for index in range(254):
        tracker.snapshot(
            f"S{index:03d}",
            "alpha",
            "beta",
            now_ms=started_at_ms,
        )
    calls = 0

    calls_per_refresh: list[int] = []
    for offset_ms in range(250, 15_001, 250):
        before = calls
        for index in range(254):
            tracker.snapshot(
                f"S{index:03d}",
                "alpha",
                "beta",
                now_ms=started_at_ms + offset_ms,
            )
        calls_per_refresh.append(calls - before)

    assert sum(calls_per_refresh) == 254
    assert max(calls_per_refresh) <= 12


def test_configured_stats_sampling_represents_full_window_and_staggers_pairs() -> None:
    tracker = SpreadStatsTracker()
    tracker.configure(
        SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
        )
    )
    assert tracker.sample_interval_ms == 3_000

    pair_count = 254
    started_at_ms = 100_000
    for index in range(pair_count):
        update = tracker.observe(
            f"S{index:03d}",
            "alpha",
            "beta",
            float(index),
            observed_at_ms=started_at_ms,
        )
        assert update.accepted is True

    accepted_per_refresh: list[int] = []
    for offset_ms in range(250, 3_001, 250):
        accepted = 0
        for index in range(pair_count):
            update = tracker.observe(
                f"S{index:03d}",
                "alpha",
                "beta",
                float(index),
                observed_at_ms=started_at_ms + offset_ms,
            )
            accepted += int(update.accepted)
        accepted_per_refresh.append(accepted)

    assert sum(accepted_per_refresh) == pair_count
    assert max(accepted_per_refresh) <= 32


def test_robust_model_recompute_is_bounded_without_dropping_samples(monkeypatch) -> None:
    import lightfee.spread.reversion as reversion

    calls = 0
    input_sizes: list[int] = []
    original = reversion._robust_location_scale

    def counted(values):
        nonlocal calls
        calls += 1
        input_sizes.append(len(values))
        return original(values)

    monkeypatch.setattr(reversion, "_robust_location_scale", counted)
    tracker = SpreadStatsTracker()
    tracker.configure(
        SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
        )
    )
    assert tracker.sample_interval_ms == 3_000
    assert tracker.stats_recompute_interval_ms == 15_000

    for index in range(10):
        tracker.update(
            "BTCUSDT",
            "cheap",
            "rich",
            float(index),
            observed_at_ms=100_000 + index * 3_000,
        )

    snapshot = tracker.snapshot(
        "BTCUSDT",
        "cheap",
        "rich",
        now_ms=127_000,
    )
    assert snapshot is not None
    # The public statistics snapshot is one coherent model generation. It
    # never advertises newer evidence bounds beside older robust metrics.
    assert snapshot.sample_count == input_sizes[-1]
    assert snapshot.sample_count < 10
    assert snapshot.last_observed_ms == 100_000 + (snapshot.sample_count - 1) * 3_000
    assert snapshot.computed_at_ms > 0
    assert snapshot.evidence_sample_count == 10
    assert snapshot.evidence_last_observed_ms == 127_000
    assert snapshot.conservative_sample_count == snapshot.sample_count
    assert len(tracker.checkpoint(now_ms=127_000)["BTCUSDT|cheap|rich"]["samples"]) == 10
    assert calls == 2

    rejection_counts: dict[str, int] = {}
    assert build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=127_500),
        ["BTCUSDT"],
        tracker=tracker,
        config=SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
            min_samples=10,
            min_history_ms=0,
            fair_price_min_venues=2,
        ),
        now_ms=127_500,
        rejection_counts=rejection_counts,
    ) == []
    assert rejection_counts == {"insufficient_history_samples": 1}


def test_cached_model_cannot_fail_open_after_current_evidence_eviction() -> None:
    tracker = SpreadStatsTracker()
    tracker.configure(
        SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
        )
    )
    key = ("BTCUSDT", "cheap", "rich")
    phase_ms = tracker._stats_recompute_phase(key)
    computed_at_ms = phase_ms + 20 * tracker.stats_recompute_interval_ms + 5_000
    oldest_ms = computed_at_ms - tracker.window_ms
    for index in range(12):
        tracker.update(
            *key,
            float(index),
            observed_at_ms=oldest_ms + index * tracker.sample_interval_ms,
        )

    state = tracker._states[key]
    with state.lock:
        state.cached_snapshot = None
        state.last_stats_computed_ms = 0
    model = tracker.snapshot(*key, now_ms=computed_at_ms)
    assert model is not None
    assert model.sample_count == 12
    assert model.evidence_sample_count == 12

    # The next millisecond expires the oldest observation but remains inside
    # the same robust-model bucket. Metrics stay one coherent as-of generation;
    # exact evidence bounds move independently and entry gates take the minimum.
    cached = tracker.snapshot(*key, now_ms=computed_at_ms + 1)
    assert cached is not None
    assert cached.sample_count == 12
    assert cached.first_observed_ms == oldest_ms
    assert cached.evidence_sample_count == 11
    assert cached.evidence_first_observed_ms == oldest_ms + tracker.sample_interval_ms
    assert cached.conservative_sample_count == 11
    assert cached.conservative_history_age_ms < cached.history_age_ms

    rejection_counts: dict[str, int] = {}
    assert build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=computed_at_ms + 500),
        ["BTCUSDT"],
        tracker=tracker,
        config=SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
            min_samples=12,
            min_history_ms=0,
            fair_price_min_venues=2,
        ),
        now_ms=computed_at_ms + 500,
        rejection_counts=rejection_counts,
    ) == []
    assert rejection_counts == {"insufficient_history_samples": 1}


def test_configure_resamples_oversampled_history_without_lookahead() -> None:
    tracker = SpreadStatsTracker()
    for observed_at_ms in range(100_000, 103_000, 250):
        tracker.update(
            "BTCUSDT",
            "alpha",
            "beta",
            float(observed_at_ms),
            observed_at_ms=observed_at_ms,
        )
    assert tracker.snapshot("BTCUSDT", "alpha", "beta", now_ms=103_000).sample_count == 12

    tracker.configure(
        SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
        )
    )
    state = tracker.snapshot("BTCUSDT", "alpha", "beta", now_ms=103_000)
    assert state is not None
    assert 1 <= state.sample_count <= 2
    assert state.last_observed_ms <= 102_750


def test_same_bucket_shocks_still_trigger_causal_structural_break() -> None:
    tracker = SpreadStatsTracker()
    tracker.configure(
        SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
            structural_break_sigma=3.0,
            structural_break_consecutive=5,
            structural_break_cooldown_ms=30_000,
        )
    )
    for index, value in enumerate((-1.0, 0.0, 1.0, -1.0, 0.0, 1.0)):
        tracker.update(
            "BTCUSDT",
            "alpha",
            "beta",
            value,
            observed_at_ms=100_000 + index * 3_000,
        )

    key = ("BTCUSDT", "alpha", "beta")
    last_observed_ms = 115_000
    next_bucket = tracker._sample_bucket(key, last_observed_ms) + 1
    phase_ms = zlib.crc32("|".join(key).encode("utf-8")) % tracker.sample_interval_ms
    bucket_started_at_ms = phase_ms + next_bucket * tracker.sample_interval_ms
    normal = tracker.observe(
        "BTCUSDT",
        "alpha",
        "beta",
        0.0,
        observed_at_ms=bucket_started_at_ms,
    )
    assert normal.accepted is True

    first_shock = tracker.observe(
        "BTCUSDT",
        "alpha",
        "beta",
        100.0,
        observed_at_ms=bucket_started_at_ms + 100,
    )
    assert first_shock.accepted is False
    assert first_shock.structural_break is False
    for _ in range(5):
        repeated = tracker.observe(
            "BTCUSDT",
            "alpha",
            "beta",
            100.0,
            observed_at_ms=bucket_started_at_ms + 100,
        )
        assert repeated.structural_break is False
        assert repeated.duplicate_evidence is True
    late = tracker.observe(
        "BTCUSDT",
        "alpha",
        "beta",
        100.0,
        observed_at_ms=bucket_started_at_ms + 99,
    )
    assert late.structural_break is False
    assert late.out_of_order_evidence is True

    for offset_ms in (200, 300, 400):
        control = tracker.observe(
            "BTCUSDT",
            "alpha",
            "beta",
            100.0,
            observed_at_ms=bucket_started_at_ms + offset_ms,
        )
        assert control.accepted is False
        assert control.structural_break is False

    broken = tracker.observe(
        "BTCUSDT",
        "alpha",
        "beta",
        100.0,
        observed_at_ms=bucket_started_at_ms + 500,
    )
    assert broken.accepted is False
    assert broken.structural_break is True
    state = tracker.snapshot(
        "BTCUSDT",
        "alpha",
        "beta",
        now_ms=bucket_started_at_ms + 500,
    )
    assert state is not None
    assert state.sample_count == 0
    assert state.cooldown_until_ms == bucket_started_at_ms + 30_500


def test_resampling_resets_incomparable_structural_break_counters() -> None:
    legacy = SpreadStatsTracker()
    for index in range(12):
        legacy.update(
            "BTCUSDT",
            "alpha",
            "beta",
            float(index % 3 - 1),
            observed_at_ms=100_000 + index * 250,
        )
    checkpoint = legacy.checkpoint(now_ms=103_000)
    row = checkpoint["BTCUSDT|alpha|beta"]
    row["break_consecutive"] = 4
    row["shock_consecutive"] = 4

    restored = SpreadStatsTracker()
    restored.configure(
        SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
        )
    )
    assert restored.restore(checkpoint, now_ms=103_000)
    migrated = restored.checkpoint(now_ms=103_000)["BTCUSDT|alpha|beta"]
    assert migrated["break_consecutive"] == 0
    assert migrated["shock_consecutive"] == 0


def test_out_of_order_observation_cannot_contaminate_rolling_state_or_signal() -> None:
    tracker = SpreadStatsTracker()
    tracker.update("BTCUSDT", "cheap", "rich", -1.0, observed_at_ms=1_000)
    tracker.update("BTCUSDT", "cheap", "rich", 1.0, observed_at_ms=3_000)

    late = tracker.update("BTCUSDT", "cheap", "rich", 500.0, observed_at_ms=2_000)

    assert late.sample_count == 2
    assert late.last_observed_ms == 3_000
    assert build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=2_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(min_samples=1, min_history_ms=0),
        now_ms=2_000,
    ) == []


def test_control_only_stats_observation_preserves_full_update_state() -> None:
    kwargs = {
        "window_ms": 15_000,
        "max_samples": 3,
        "short_window_ms": 100,
        "structural_break_sigma": 2.0,
        "structural_break_consecutive": 1,
        "structural_break_cooldown_ms": 1_000,
    }
    full = SpreadStatsTracker(**kwargs)
    control_only = SpreadStatsTracker(**kwargs)

    for observed_at_ms, value in (
        (1_000, -1.0),
        (2_000, 0.0),
        (3_000, 1.0),
        (10_000, 100.0),
    ):
        full_snapshot = full.update(
            "BTCUSDT",
            "cheap",
            "rich",
            value,
            observed_at_ms=observed_at_ms,
            exit_half_spread_bps=2.0,
        )
        control = control_only.observe(
            "BTCUSDT",
            "cheap",
            "rich",
            value,
            observed_at_ms=observed_at_ms,
            exit_half_spread_bps=2.0,
        )

        assert control.accepted is not control.structural_break
        assert control.structural_break is full_snapshot.structural_break
        assert control.cooldown_until_ms == full_snapshot.cooldown_until_ms
        assert control_only.checkpoint(now_ms=observed_at_ms) == full.checkpoint(
            now_ms=observed_at_ms
        )
        assert control_only.revision == full.revision


def test_current_observation_is_not_in_its_own_zscore_and_direction_is_correct() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    before = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=30_000)
    assert before is not None

    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.sample_count == before.sample_count
    assert candidate.rolling_mean_bps == before.median_bps
    assert candidate.z_score > 0.0
    assert candidate.long_venue == "rich"
    assert candidate.short_venue == "cheap"
    after = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=31_000)
    assert after.sample_count == before.sample_count
    assert after.first_observed_ms == before.first_observed_ms
    assert after.last_observed_ms == before.last_observed_ms
    assert after.median_bps == before.median_bps
    assert after.robust_scale_bps == before.robust_scale_bps
    assert after.evidence_sample_count == 31
    assert after.evidence_last_observed_ms == 31_000
    assert len(tracker.checkpoint(now_ms=31_000)["BTCUSDT|cheap|rich"]["samples"]) == 31

    duplicate_rejections: dict[str, int] = {}
    assert build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
        rejection_counts=duplicate_rejections,
    ) == []
    assert duplicate_rejections == {"duplicate_stats_evidence": 1}


def test_repeated_scheduler_reads_do_not_manufacture_history_samples() -> None:
    tracker = SpreadStatsTracker()
    quotes = _quotes_for_signed_basis(1.0, now_ms=10_000)
    config = _config(min_samples=100, min_history_ms=0)

    assert build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=config,
        now_ms=10_100,
    ) == []
    first = tracker.checkpoint(now_ms=10_000)["BTCUSDT|cheap|rich"]
    assert len(first["samples"]) == 1

    assert build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=config,
        now_ms=12_000,
    ) == []
    repeated = tracker.checkpoint(now_ms=12_000)["BTCUSDT|cheap|rich"]
    assert len(repeated["samples"]) == 1
    assert repeated["samples"][0][0] == 10_000


def test_negative_z_reverses_trade_direction_without_creating_a_second_series() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)

    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(-20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.z_score < 0.0
    assert candidate.long_venue == "cheap"
    assert candidate.short_venue == "rich"
    assert candidate.candidate_id == "spread:BTCUSDT:cheap->rich"


def test_gross_edge_is_reversion_space_not_the_full_observed_cross_venue_spread() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker, center=80.0)

    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(100.0, now_ms=31_000, half_spread=0.0001),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(min_net_edge_bps=-100.0),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert abs(candidate.current_signed_mid_spread_bps) == pytest.approx(100.0)
    assert 10.0 < candidate.gross_reversion_edge_bps < 30.0
    assert candidate.gross_reversion_edge_bps < abs(candidate.current_signed_mid_spread_bps)
    assert candidate.expected_exit_cross_bps == pytest.approx(-2.0)


def test_v3_dynamic_threshold_uses_cost_headroom_and_exposes_capital_efficiency() -> None:
    blocked_tracker = SpreadStatsTracker()
    _prewarm(blocked_tracker)
    rejections: dict[str, int] = {}

    blocked = build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=blocked_tracker,
        config=_config(
            dynamic_net_edge_enabled=True,
            model_epoch="v3_cost_normalized_reversion",
            min_profit_buffer_bps=100.0,
            min_net_edge_bps=-100.0,
        ),
        now_ms=31_000,
        rejection_counts=rejections,
    )
    assert blocked == []
    assert rejections == {"dynamic_min_gross_edge": 1}

    ready_tracker = SpreadStatsTracker()
    _prewarm(ready_tracker)
    ready = build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=ready_tracker,
        config=_config(
            dynamic_net_edge_enabled=True,
            model_epoch="v3_cost_normalized_reversion",
            min_profit_buffer_bps=0.0,
            min_net_edge_bps=-100.0,
            rank_by_capital_efficiency=True,
        ),
        now_ms=31_000,
    )

    assert len(ready) == 1
    candidate = ready[0]
    assert candidate.calculation_version == "spread_v3_cost_normalized_reversion"
    assert candidate.dynamic_min_gross_edge_bps >= 0.0
    assert candidate.risk_adjusted_edge_per_capital_hour_bps == pytest.approx(
        candidate.score
    )
    assert candidate.score < candidate.net_edge_per_capital_hour_bps


def test_spread_candidate_notional_is_per_leg_and_cannot_exceed_total_gross_cap() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)

    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(live_notional_quote=100.0, max_gross_quote=50.0),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    assert candidates[0].entry_notional_quote == pytest.approx(25.0)


def test_missing_taker_fee_evidence_blocks_spread_candidate_but_explicit_zero_is_valid() -> None:
    missing_tracker = SpreadStatsTracker()
    _prewarm(missing_tracker)
    rejections: dict[str, int] = {}

    assert build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=missing_tracker,
        config=_config(taker_fee_bps_by_venue={}),
        now_ms=31_000,
        rejection_counts=rejections,
    ) == []
    assert rejections == {"missing_taker_fee_evidence": 1}

    valid_tracker = SpreadStatsTracker()
    _prewarm(valid_tracker)
    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=valid_tracker,
        config=_config(),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    assert candidates[0].fee_evidence_complete is True


def test_funding_changes_ranking_once_via_the_unified_edge_contract() -> None:
    def candidate_with_rates(long_rate: float, short_rate: float):
        tracker = SpreadStatsTracker()
        _prewarm(tracker)
        quotes = _quotes_for_signed_basis(20.0, now_ms=31_000)
        # A positive z-score goes long "rich" and short "cheap".
        quotes["rich:BTCUSDT"] = _quote(
            "rich",
            bid=quotes["rich:BTCUSDT"].bid,
            ask=quotes["rich:BTCUSDT"].ask,
            observed_at_ms=31_000,
            funding_rate_bps=long_rate,
            funding_timestamp_ms=32_000,
        )
        quotes["cheap:BTCUSDT"] = _quote(
            "cheap",
            bid=quotes["cheap:BTCUSDT"].bid,
            ask=quotes["cheap:BTCUSDT"].ask,
            observed_at_ms=31_000,
            funding_rate_bps=short_rate,
            funding_timestamp_ms=32_000,
        )
        result = build_spread_reversion_candidates(
            quotes,
            ["BTCUSDT"],
            tracker=tracker,
            config=_config(),
            now_ms=31_000,
        )
        assert len(result) == 1
        return result[0]

    neutral = candidate_with_rates(0.0, 0.0)
    tailwind = candidate_with_rates(-4.0, 8.0)

    assert tailwind.ranking_edge_bps - neutral.ranking_edge_bps == pytest.approx(12.0)
    assert tailwind.score - neutral.score == pytest.approx(12.0)


def test_ar1_requires_mean_reversion_and_uses_the_true_half_life_formula() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    state = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=30_000)
    assert state is not None
    assert state.ar1_phi is not None and 0.0 < state.ar1_phi < 1.0
    expected_half_life = -math.log(2.0) / math.log(state.ar1_phi) * 1_000.0
    assert state.half_life_ms == pytest.approx(expected_half_life, abs=1.0)

    non_reverting = SpreadStatsTracker()
    for index, value in enumerate([-2.0, 2.0] * 16, start=1):
        non_reverting.update("BTCUSDT", "cheap", "rich", value, observed_at_ms=index * 1_000)
    rejected = non_reverting.snapshot("BTCUSDT", "cheap", "rich", now_ms=32_000)
    assert rejected is not None
    assert rejected.ar1_phi is not None and rejected.ar1_phi <= 0.0


def test_ar1_half_life_fails_closed_after_an_irregular_quote_gap() -> None:
    tracker = SpreadStatsTracker()
    for observed_at_ms, value in (
        (1_000, 3.0),
        (2_000, 2.0),
        (3_000, 1.0),
        # A large data outage cannot be hidden by averaging it into the AR(1)
        # sampling interval.
        (30_000, 0.5),
    ):
        tracker.update("BTCUSDT", "cheap", "rich", value, observed_at_ms=observed_at_ms)

    state = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=30_000)

    assert state is not None
    assert state.ar1_phi is not None and 0.0 < state.ar1_phi < 1.0
    assert state.half_life_ms == 0


def test_rolling_eviction_checkpoint_restore_and_structural_break_cooldown() -> None:
    tracker = SpreadStatsTracker(
        window_ms=15_000,
        max_samples=3,
        short_window_ms=100,
        structural_break_sigma=2.0,
        structural_break_consecutive=1,
        structural_break_cooldown_ms=1_000,
    )
    for index, value in enumerate([-1.0, 0.0, 1.0], start=1):
        tracker.update("BTCUSDT", "cheap", "rich", value, observed_at_ms=index * 1_000)
    tracker.update("BTCUSDT", "cheap", "rich", 100.0, observed_at_ms=10_000)
    broken = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=10_000)
    assert broken is not None
    assert broken.sample_count == 0
    assert broken.cooldown_until_ms == 11_000

    checkpoint = tracker.checkpoint(now_ms=10_000)
    restored = SpreadStatsTracker(window_ms=15_000, max_samples=3)
    restored.restore(checkpoint, now_ms=10_000)
    state = restored.snapshot("BTCUSDT", "cheap", "rich", now_ms=10_000)
    assert state is not None and state.sample_count == 0


def test_contract_normalization_and_timestamp_freshness_fail_closed() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    incompatible = _quotes_for_signed_basis(
        20.0,
        now_ms=31_000,
        contract_type="linear",
        quote_currency="USDT",
    )
    incompatible["rich:BTCUSDT"] = _quote(
        "rich", bid=99.79, ask=99.81, observed_at_ms=31_000,
        contract_type="inverse", quote_currency="USD",
    )
    assert build_spread_reversion_candidates(
        incompatible, ["BTCUSDT"], tracker=tracker, config=_config(), now_ms=31_000
    ) == []

    inverse = _quotes_for_signed_basis(
        20.0,
        now_ms=31_000,
        contract_type="inverse",
        quote_currency="USD",
        contract_multiplier=1.0,
    )
    inverse_rejections: dict[str, int] = {}
    assert build_spread_reversion_candidates(
        inverse,
        ["BTCUSDT"],
        tracker=SpreadStatsTracker(),
        config=_config(),
        now_ms=31_000,
        rejection_counts=inverse_rejections,
    ) == []
    assert inverse_rejections == {"unsupported_contract_type_for_base_quantity_pnl": 1}

    stale = _quotes_for_signed_basis(20.0, now_ms=1_000)
    assert build_spread_reversion_candidates(
        stale, ["BTCUSDT"], tracker=SpreadStatsTracker(), config=_config(), now_ms=10_000
    ) == []


def test_non_finite_bbo_or_funding_cannot_create_or_rank_a_spread_candidate() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    malformed_price = _quotes_for_signed_basis(20.0, now_ms=31_000)
    malformed_price["rich:BTCUSDT"] = _quote(
        "rich",
        bid=float("nan"),
        ask=99.81,
        observed_at_ms=31_000,
    )
    assert build_spread_reversion_candidates(
        malformed_price, ["BTCUSDT"], tracker=tracker, config=_config(), now_ms=31_000
    ) == []

    malformed_funding = _quotes_for_signed_basis(20.0, now_ms=32_000)
    malformed_funding["rich:BTCUSDT"] = _quote(
        "rich",
        bid=malformed_funding["rich:BTCUSDT"].bid,
        ask=malformed_funding["rich:BTCUSDT"].ask,
        observed_at_ms=32_000,
        funding_rate_bps=float("nan"),
        funding_timestamp_ms=33_000,
    )
    assert build_spread_reversion_candidates(
        malformed_funding, ["BTCUSDT"], tracker=tracker, config=_config(), now_ms=32_000
    ) == []


def test_contract_evidence_and_funding_schedule_are_required_with_stable_reasons() -> None:
    tracker = SpreadStatsTracker()
    rejection_counts: dict[str, int] = {}
    incomplete = _quotes_for_signed_basis(
        20.0,
        now_ms=31_000,
        contract_normalization_complete=False,
    )

    assert build_spread_reversion_candidates(
        incomplete,
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
        rejection_counts=rejection_counts,
    ) == []
    assert rejection_counts == {"contract_normalization_incomplete": 1}

    _prewarm(tracker)
    schedule_unknown = _quotes_for_signed_basis(20.0, now_ms=31_000, funding_interval_ms=0)
    assert build_spread_reversion_candidates(
        schedule_unknown,
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
    ) == []


@pytest.mark.parametrize("field", ("price_precision", "quantity_precision"))
def test_malformed_contract_precision_is_rejected(field: str) -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    rejection_counts: dict[str, int] = {}
    quotes = _quotes_for_signed_basis(20.0, now_ms=31_000, **{field: -1})

    assert build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
        rejection_counts=rejection_counts,
    ) == []
    assert rejection_counts == {f"missing_{field}": 1}


def test_zero_decimal_quantity_precision_is_valid_for_integer_lots() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    rejection_counts: dict[str, int] = {}
    quotes = _quotes_for_signed_basis(
        20.0,
        now_ms=31_000,
        quantity_precision=0,
    )

    candidates = build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
        rejection_counts=rejection_counts,
    )

    assert candidates
    assert "missing_quantity_precision" not in rejection_counts
