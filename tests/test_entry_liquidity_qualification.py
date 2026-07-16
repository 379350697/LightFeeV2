from lightfee.engine.entry_liquidity_qualification import (
    ENTRY_LIQUIDITY_STRUCTURAL_FAILURE_THRESHOLD,
    ENTRY_LIQUIDITY_STRUCTURAL_PROBE_INTERVAL_MS,
    ENTRY_LIQUIDITY_STRUCTURAL_SUPPRESS_MS,
    EntryLiquidityEligibilityClass,
    EntryLiquidityQualificationState,
)


def test_v1_entry_liquidity_memory_structuralizes_after_three_low_oi_failures():
    state = EntryLiquidityQualificationState()

    assert state.record_result(
        "aster",
        "ZETAUSDT",
        EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
        now_ms=1_000,
        sample_id="oi-sample-1",
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR
    assert state.record_result(
        "aster",
        "ZETAUSDT",
        EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
        now_ms=2_000,
        sample_id="oi-sample-2",
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR
    assert state.current_class(
        "aster",
        "ZETAUSDT",
        now_ms=2_000,
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR

    assert state.record_result(
        "aster",
        "ZETAUSDT",
        EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
        now_ms=3_000,
        sample_id="oi-sample-3",
    ) == EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY

    assert ENTRY_LIQUIDITY_STRUCTURAL_FAILURE_THRESHOLD == 3
    assert ENTRY_LIQUIDITY_STRUCTURAL_SUPPRESS_MS == 30 * 60 * 1_000
    assert state.current_class(
        "aster",
        "ZETAUSDT",
        now_ms=3_100,
    ) == EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY


def test_v1_entry_liquidity_structural_probe_rate_limits_and_persists_records():
    state = EntryLiquidityQualificationState.from_records([
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": 3,
            "last_failure_at_ms": 10_000,
            "suppress_until_ms": 1_810_000,
            "last_class": "structural_ineligibility",
            "last_observed_open_interest_quote": 900_000,
            "last_observed_open_interest_at_ms": 10_000,
            "counted_low_sample_ids": ["sample-1", "sample-2", "sample-3"],
            "last_observed_sample_id": None,
            "last_structural_probe_at_ms": 70_000,
        }
    ])

    assert ENTRY_LIQUIDITY_STRUCTURAL_PROBE_INTERVAL_MS == 60 * 1_000
    assert state.should_probe_structural("okx", "BTCUSDT", now_ms=100_000) is False
    assert state.should_probe_structural("okx", "BTCUSDT", now_ms=130_000) is True

    state.note_open_interest_observation("okx", "BTCUSDT", 1_250_000.4, observed_at_ms=130_000)
    records = state.to_records()

    assert records == [
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": 3,
            "last_failure_at_ms": 10_000,
            "suppress_until_ms": 1_810_000,
            "last_class": "structural_ineligibility",
            "last_observed_open_interest_quote": 1_250_000,
            "last_observed_open_interest_at_ms": 130_000,
            "last_observed_sample_id": None,
            "counted_low_sample_ids": ["sample-1", "sample-2", "sample-3"],
            "last_counted_low_sample_id": "sample-3",
            "last_structural_probe_at_ms": 130_000,
        }
    ]


def test_v1_entry_liquidity_memory_resets_on_eligible_result():
    state = EntryLiquidityQualificationState.from_records([
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": 3,
            "last_failure_at_ms": 10_000,
            "suppress_until_ms": 1_810_000,
            "last_class": "structural_ineligibility",
            "counted_low_sample_ids": ["sample-1", "sample-2", "sample-3"],
        }
    ])

    assert state.record_result(
        "okx",
        "BTCUSDT",
        EntryLiquidityEligibilityClass.ELIGIBLE,
        now_ms=130_000,
    ) == EntryLiquidityEligibilityClass.ELIGIBLE

    assert state.current_class(
        "okx",
        "BTCUSDT",
        now_ms=130_000,
    ) == EntryLiquidityEligibilityClass.ELIGIBLE
    assert state.to_records()[0]["consecutive_failures"] == 0
    assert state.to_records()[0]["suppress_until_ms"] is None


def test_legacy_structural_state_without_three_proven_samples_is_downgraded():
    state = EntryLiquidityQualificationState.from_records([
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": 99,
            "last_failure_at_ms": 10_000,
            "suppress_until_ms": 1_810_000,
            "last_class": "structural_ineligibility",
            "last_observed_sample_id": "only-proven-sample",
        }
    ])

    record = state.to_records()[0]
    assert record["consecutive_failures"] == 1
    assert record["last_class"] == "temporary_below_floor"
    assert record["suppress_until_ms"] is None
    assert state.current_class(
        "okx", "BTCUSDT", now_ms=20_000
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR


def test_malformed_persisted_failure_count_fails_closed_without_crashing():
    state = EntryLiquidityQualificationState.from_records([
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": "not-an-integer",
            "suppress_until_ms": 1_810_000,
            "last_class": "structural_ineligibility",
        }
    ])

    record = state.to_records()[0]
    assert record["consecutive_failures"] == 0
    assert record["last_class"] == "data_unavailable"
    assert record["suppress_until_ms"] is None


def test_explicit_structural_result_cannot_bypass_three_sample_proof():
    state = EntryLiquidityQualificationState()

    assert state.record_result(
        "okx",
        "BTCUSDT",
        EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY,
        now_ms=1_000,
        sample_id="sample-1",
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR
    record = state.to_records()[0]
    assert record["consecutive_failures"] == 1
    assert record["suppress_until_ms"] is None


def test_non_adjacent_replayed_low_oi_sample_does_not_advance_structural_count():
    state = EntryLiquidityQualificationState()

    for now_ms, sample_id in ((1_000, "sample-a"), (2_000, "sample-b")):
        assert state.record_result(
            "okx",
            "BTCUSDT",
            EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
            now_ms=now_ms,
            sample_id=sample_id,
        ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR

    restored = EntryLiquidityQualificationState.from_records(state.to_records())
    assert restored.record_result(
        "okx",
        "BTCUSDT",
        EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
        now_ms=3_000,
        sample_id="sample-a",
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR

    record = restored.to_records()[0]
    assert record["consecutive_failures"] == 2
    assert record["counted_low_sample_ids"] == ["sample-a", "sample-b"]
    assert record["last_counted_low_sample_id"] == "sample-b"


def test_legacy_non_structural_count_is_clamped_to_provable_unique_samples():
    state = EntryLiquidityQualificationState.from_records([
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": 2,
            "last_class": "temporary_below_floor",
            "last_observed_sample_id": "same-sample",
        }
    ])

    migrated = state.to_records()[0]
    assert migrated["consecutive_failures"] == 1
    assert migrated["counted_low_sample_ids"] == ["same-sample"]

    assert state.record_result(
        "okx",
        "BTCUSDT",
        EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
        now_ms=3_000,
        sample_id="same-sample",
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR
    assert state.to_records()[0]["consecutive_failures"] == 1

    assert state.record_result(
        "okx",
        "BTCUSDT",
        EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
        now_ms=4_000,
        sample_id="new-sample",
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR
    assert state.to_records()[0]["consecutive_failures"] == 2


def test_legacy_last_observed_and_last_counted_samples_are_both_deduplicated():
    state = EntryLiquidityQualificationState.from_records([
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": 9,
            "last_class": "temporary_below_floor",
            "last_counted_low_sample_id": "sample-a",
            "last_observed_sample_id": "sample-b",
        }
    ])

    migrated = state.to_records()[0]
    assert migrated["consecutive_failures"] == 2
    assert migrated["counted_low_sample_ids"] == ["sample-a", "sample-b"]

    for now_ms, sample_id in ((3_000, "sample-a"), (4_000, "sample-b")):
        assert state.record_result(
            "okx",
            "BTCUSDT",
            EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
            now_ms=now_ms,
            sample_id=sample_id,
        ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR
    assert state.to_records()[0]["consecutive_failures"] == 2


def test_counted_low_oi_sample_history_is_persisted_with_a_finite_bound():
    state = EntryLiquidityQualificationState()
    for index in range(12):
        state.record_result(
            "okx",
            "BTCUSDT",
            EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
            now_ms=1_000 + index,
            sample_id=f"sample-{index}",
            threshold_failures=100,
        )

    record = state.to_records()[0]
    assert record["counted_low_sample_ids"] == [
        f"sample-{index}" for index in range(4, 12)
    ]
    restored = EntryLiquidityQualificationState.from_records([record])
    assert restored.to_records()[0]["counted_low_sample_ids"] == (
        record["counted_low_sample_ids"]
    )
