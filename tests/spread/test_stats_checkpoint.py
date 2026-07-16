from __future__ import annotations

import asyncio
import json
import threading
from types import SimpleNamespace

import pytest

import lightfee.spread.service as service_module
from lightfee.spread.reversion import SpreadStatsTracker
from lightfee.spread.reversion import SpreadReversionConfig
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


def test_checkpoint_cannot_mix_pre_break_samples_with_post_break_control_state() -> None:
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
    tracker.snapshot("BTCUSDT", "alpha", "beta", now_ms=115_000)
    state = tracker._states[("BTCUSDT", "alpha", "beta")]
    started = threading.Event()
    result: dict[str, dict] = {}

    def capture() -> None:
        started.set()
        result.update(tracker.checkpoint(now_ms=115_500))

    with state.lock:
        worker = threading.Thread(target=capture)
        worker.start()
        assert started.wait(timeout=1.0)
        for offset_ms in (100, 200, 300, 400):
            control = tracker.observe(
                "BTCUSDT",
                "alpha",
                "beta",
                100.0,
                observed_at_ms=115_000 + offset_ms,
            )
            assert control.structural_break is False
        broken = tracker.observe(
            "BTCUSDT",
            "alpha",
            "beta",
            100.0,
            observed_at_ms=115_500,
        )
        assert broken.structural_break is True
    worker.join(timeout=2.0)
    assert not worker.is_alive()

    row = result["BTCUSDT|alpha|beta"]
    assert row["samples"] == []
    assert row["cooldown_until_ms"] == 145_500
    restored = SpreadStatsTracker()
    restored.configure(
        SpreadReversionConfig(
            stats_window_ms=36_000,
            stats_max_samples=12,
        )
    )
    assert restored.restore(result, now_ms=145_501)
    snapshot = restored.snapshot("BTCUSDT", "alpha", "beta", now_ms=145_501)
    assert snapshot is None or snapshot.sample_count == 0


@pytest.mark.asyncio
async def test_service_checkpoints_only_dirty_state_at_bounded_intervals(
    tmp_path,
    monkeypatch,
) -> None:
    service = object.__new__(SpreadSidecarService)
    service.stats = SpreadStatsTracker()
    service.stats_checkpoint_path = tmp_path / "spread-stats.json"
    service.signal_config = SimpleNamespace(model_epoch="test-epoch")
    service._stats_checkpoint_persisted_revision = service.stats.revision
    service._stats_checkpoint_last_attempt_ms = 1_000
    service._stats_checkpoint_task = None
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
    assert service._stats_checkpoint_task is not None
    await service._stats_checkpoint_task
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
    assert service._stats_checkpoint_task is not None
    await service._stats_checkpoint_task
    assert calls == [(1, 31_001), (2, 31_002)]


@pytest.mark.asyncio
async def test_service_checkpoint_io_does_not_block_signal_event_loop(
    tmp_path,
    monkeypatch,
) -> None:
    service = object.__new__(SpreadSidecarService)
    service.stats = SpreadStatsTracker()
    service.stats.update(
        "BTCUSDT",
        "alpha",
        "beta",
        1.0,
        observed_at_ms=31_001,
    )
    service.stats_checkpoint_path = tmp_path / "spread-stats.json"
    service.signal_config = SimpleNamespace(model_epoch="test-epoch")
    service._stats_checkpoint_persisted_revision = 0
    service._stats_checkpoint_last_attempt_ms = 0
    service._stats_checkpoint_task = None
    started = threading.Event()
    release = threading.Event()

    def blocked_checkpoint(*_args, **_kwargs) -> None:
        started.set()
        assert release.wait(timeout=2.0)

    monkeypatch.setattr(
        service_module,
        "publish_spread_stats_checkpoint",
        blocked_checkpoint,
    )

    assert service._checkpoint_stats_if_due(31_001)
    for _ in range(20):
        if started.is_set():
            break
        await asyncio.sleep(0.01)
    assert started.is_set()
    assert service._stats_checkpoint_task is not None
    assert not service._stats_checkpoint_task.done()

    release.set()
    await service._stats_checkpoint_task
    assert service._stats_checkpoint_persisted_revision == 1
