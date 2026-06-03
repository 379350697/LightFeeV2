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
    ) == EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR
    assert state.record_result(
        "aster",
        "ZETAUSDT",
        EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
        now_ms=2_000,
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
