from __future__ import annotations

import json
from types import SimpleNamespace

import lightfee.spread.service as service_module
from lightfee.spread.reversion import SpreadStatsTracker
from lightfee.spread.service import SpreadSidecarService
from lightfee.spread.stats_checkpoint import (
    SPREAD_STATS_CHECKPOINT_SCHEMA_VERSION,
    publish_spread_stats_checkpoint,
    restore_spread_stats_checkpoint,
)


def test_stats_checkpoint_restores_only_the_matching_epoch(tmp_path) -> None:
    path = tmp_path / "spread-stats.json"
    original = SpreadStatsTracker(window_ms=60_000, max_samples=10)
    for index, value in enumerate((-2.0, 0.0, 2.0), start=1):
        original.update("BTCUSDT", "alpha", "beta", value, observed_at_ms=index * 1_000)

    publish_spread_stats_checkpoint(
        original,
        path,
        model_epoch="v2_signed_reversion",
        now_ms=3_000,
    )

    restored = SpreadStatsTracker(window_ms=60_000, max_samples=10)
    assert restore_spread_stats_checkpoint(
        restored,
        path,
        model_epoch="v2_signed_reversion",
        now_ms=3_000,
    )
    state = restored.snapshot("BTCUSDT", "alpha", "beta", now_ms=3_000)
    assert state is not None
    assert state.sample_count == 3

    wrong_epoch = SpreadStatsTracker(window_ms=60_000, max_samples=10)
    assert not restore_spread_stats_checkpoint(
        wrong_epoch,
        path,
        model_epoch="v3_other_model",
        now_ms=3_000,
    )
    assert wrong_epoch.snapshot("BTCUSDT", "alpha", "beta", now_ms=3_000) is None


def test_stats_checkpoint_restores_only_the_frozen_sampling_universe(tmp_path) -> None:
    path = tmp_path / "spread-stats.json"
    original = SpreadStatsTracker(window_ms=60_000, max_samples=10)
    original.update("BTCUSDT", "alpha", "beta", 1.0, observed_at_ms=1_000)
    original.update("ETHUSDT", "alpha", "beta", 2.0, observed_at_ms=1_000)
    publish_spread_stats_checkpoint(
        original,
        path,
        model_epoch="v2_signed_reversion",
        now_ms=1_000,
    )

    restored = SpreadStatsTracker(window_ms=60_000, max_samples=10)
    assert restore_spread_stats_checkpoint(
        restored,
        path,
        model_epoch="v2_signed_reversion",
        now_ms=1_000,
        allowed_symbols={"BTCUSDT"},
    )
    assert restored.snapshot("BTCUSDT", "alpha", "beta", now_ms=1_000) is not None
    assert restored.snapshot("ETHUSDT", "alpha", "beta", now_ms=1_000) is None


def test_stats_checkpoint_corruption_cold_starts(tmp_path) -> None:
    path = tmp_path / "spread-stats.json"
    path.write_text("not-json", encoding="utf-8")
    tracker = SpreadStatsTracker()

    assert not restore_spread_stats_checkpoint(
        tracker,
        path,
        model_epoch="v2_signed_reversion",
        now_ms=1_000,
    )


def test_legacy_scheduler_time_checkpoint_cold_starts(tmp_path) -> None:
    path = tmp_path / "spread-stats.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "model_epoch": "v2_signed_reversion",
                "saved_at_ms": 1_000,
                "states": {
                    "BTCUSDT|alpha|beta": {
                        "samples": [[1_000, 1.0, 0.1]],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    tracker = SpreadStatsTracker(window_ms=60_000)

    assert not restore_spread_stats_checkpoint(
        tracker,
        path,
        model_epoch="v2_signed_reversion",
        now_ms=1_000,
    )
    assert tracker.snapshot("BTCUSDT", "alpha", "beta", now_ms=1_000) is None


def test_stats_checkpoint_rejects_nonfinite_rows_and_expired_state(tmp_path) -> None:
    path = tmp_path / "spread-stats.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": SPREAD_STATS_CHECKPOINT_SCHEMA_VERSION,
                "model_epoch": "v2_signed_reversion",
                "saved_at_ms": 1_000,
                "states": {
                    "BTCUSDT|alpha|beta": {
                        "samples": [[1_000, float("nan"), 0.1]]
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    tracker = SpreadStatsTracker(window_ms=60_000)

    assert not restore_spread_stats_checkpoint(
        tracker,
        path,
        model_epoch="v2_signed_reversion",
        now_ms=1_000,
    )
    assert tracker.snapshot("BTCUSDT", "alpha", "beta", now_ms=1_000) is None

    path.write_text(
        json.dumps(
            {
                "schema_version": SPREAD_STATS_CHECKPOINT_SCHEMA_VERSION,
                "model_epoch": "v2_signed_reversion",
                "saved_at_ms": 1_000,
                "states": {},
            }
        ),
        encoding="utf-8",
    )
    assert not restore_spread_stats_checkpoint(
        tracker,
        path,
        model_epoch="v2_signed_reversion",
        now_ms=62_000,
    )


def test_stats_tracker_rejects_nonfinite_online_observation() -> None:
    tracker = SpreadStatsTracker()
    tracker.update("BTCUSDT", "alpha", "beta", float("nan"), observed_at_ms=1_000)

    state = tracker.snapshot("BTCUSDT", "alpha", "beta", now_ms=1_000)
    assert state is not None
    assert state.sample_count == 0
    assert tracker.revision == 0


def test_stats_tracker_revision_only_advances_for_new_market_evidence() -> None:
    tracker = SpreadStatsTracker()

    tracker.update("BTCUSDT", "alpha", "beta", 1.0, observed_at_ms=1_000)
    assert tracker.revision == 1

    tracker.update("BTCUSDT", "alpha", "beta", 2.0, observed_at_ms=1_000)
    assert tracker.revision == 1

    checkpoint = tracker.checkpoint(now_ms=1_000)
    restored = SpreadStatsTracker()
    assert restored.restore(checkpoint, now_ms=1_000)
    assert restored.revision == 0


def test_service_checkpoints_only_dirty_state_at_bounded_intervals(
    tmp_path,
    monkeypatch,
) -> None:
    service = object.__new__(SpreadSidecarService)
    service.stats = SpreadStatsTracker()
    service.stats_checkpoint_path = tmp_path / "spread-stats.json"
    service.signal_config = SimpleNamespace(model_epoch="test-epoch")
    service._stats_checkpoint_persisted_revision = service.stats.revision
    service._stats_checkpoint_last_attempt_ms = 1_000
    calls: list[tuple[int, int]] = []

    def record_checkpoint(tracker, _path, *, model_epoch, now_ms) -> None:
        assert model_epoch == "test-epoch"
        calls.append((tracker.revision, now_ms))

    monkeypatch.setattr(
        service_module,
        "publish_spread_stats_checkpoint",
        record_checkpoint,
    )

    assert not service._checkpoint_stats_if_due(31_000)
    service.stats.update(
        "BTCUSDT",
        "alpha",
        "beta",
        1.0,
        observed_at_ms=31_001,
    )
    assert service._checkpoint_stats_if_due(31_001)
    assert calls == [(1, 31_001)]

    service.stats.update(
        "BTCUSDT",
        "alpha",
        "beta",
        2.0,
        observed_at_ms=31_002,
    )
    assert not service._checkpoint_stats_if_due(31_002)
    assert service._checkpoint_stats_if_due(31_002, force=True)
    assert calls == [(1, 31_001), (2, 31_002)]
