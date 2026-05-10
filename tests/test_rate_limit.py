"""Tests for token-bucket rate-limit engine, config, and recommendations."""

import time

import pytest

from lightfee.rate_limit.config import (
    RateLimitConfig,
    RateLimitConfigManager,
    RateLimitHostConfig,
    RateLimitVenueConfig,
    built_in_defaults,
)
from lightfee.rate_limit.engine import (
    RateLimitEngine,
    RateLimitError,
    RateLimitErrorReason,
    RateLimitRuntime,
    install_global_rate_limit_runtime,
    global_rate_limit_runtime,
)
from lightfee.rate_limit.recommendations import (
    RateLimitLimitHit,
    RateLimitRecommendation,
    RateLimitRequestObserved,
    RecommendationEngine,
)


class TestRateLimitEngine:
    def test_register_and_consume(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("test", capacity=10.0, refill_per_sec=2.0)
        eng.try_consume("test", ["read"], now_ms=0)
        snap = eng.bucket_snapshot("test")
        assert snap is not None
        assert snap["tokens"] == 9.0

    def test_budget_exceeded(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("test", capacity=1.0, refill_per_sec=0.0)
        eng.try_consume("test", ["read"], now_ms=0)  # consume the only token
        with pytest.raises(RateLimitError) as exc:
            eng.try_consume("test", ["read"], now_ms=1)
        assert exc.value.reason == RateLimitErrorReason.BUDGET_EXCEEDED

    def test_weight_multiplier(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("test", capacity=5.0, refill_per_sec=0.0)
        eng.register_weight("test", "write", 3.0)
        eng.try_consume("test", ["write"], now_ms=0)
        snap = eng.bucket_snapshot("test")
        assert snap["tokens"] == 2.0  # 5 - 3 = 2

    def test_cooldown_blocks(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("test", capacity=10.0, refill_per_sec=2.0)
        eng.apply_cooldown("test", 5000, now_ms=0)
        with pytest.raises(RateLimitError) as exc:
            eng.try_consume("test", ["read"], now_ms=1000)
        assert exc.value.reason == RateLimitErrorReason.COOLDOWN
        # Should pass after cooldown
        eng.try_consume("test", ["read"], now_ms=6000)

    def test_backoff_blocks(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("test", capacity=10.0, refill_per_sec=2.0)
        eng.apply_backoff("test", 3000, now_ms=0)
        with pytest.raises(RateLimitError) as exc:
            eng.try_consume("test", ["read"], now_ms=500)
        assert exc.value.reason == RateLimitErrorReason.COOLDOWN

    def test_min_interval_blocks(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("test", capacity=10.0, refill_per_sec=10.0)
        eng.register_min_interval("test", "read", 1000)
        eng.try_consume("test", ["read"], now_ms=0)
        with pytest.raises(RateLimitError) as exc:
            eng.try_consume("test", ["read"], now_ms=500)
        assert exc.value.reason == RateLimitErrorReason.MIN_INTERVAL
        # OK after interval
        eng.try_consume("test", ["read"], now_ms=1100)

    def test_refill_over_time(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("test", capacity=10.0, refill_per_sec=10.0)
        eng.try_consume("test", ["read"], now_ms=0)
        eng.try_consume("test", ["read"], now_ms=0)
        snap = eng.bucket_snapshot("test")
        assert snap["tokens"] == 8.0
        # Fast-forward 500ms → refill 5 tokens, capped at capacity
        eng.try_consume("test", ["read"], now_ms=500)
        snap = eng.bucket_snapshot("test")
        assert pytest.approx(snap["tokens"], abs=0.1) == 9.0  # 8 - 1 + (500/1000*10) = 12 → cap 10 → 9 after consume

    def test_unregistered_bucket_always_allows(self):
        eng = RateLimitEngine()
        eng.try_consume("nonexistent", ["read"], now_ms=0)  # no error

    def test_try_consume_scopes(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("a", capacity=5.0, refill_per_sec=0.0)
        eng.register_bucket("b", capacity=5.0, refill_per_sec=0.0)
        eng.try_consume_scopes(["read"], now_ms=0)
        assert eng.bucket_snapshot("a")["tokens"] == 4.0
        assert eng.bucket_snapshot("b")["tokens"] == 4.0

    def test_default_margin(self):
        eng = RateLimitEngine(default_margin=0.5)
        eng.register_bucket("test", capacity=10.0, refill_per_sec=1.0)
        snap = eng.bucket_snapshot("test")
        assert snap["capacity"] == 5.0  # 10 * 0.5


class TestRateLimitConfig:
    def test_built_in_defaults_has_all_venues(self):
        cfg = built_in_defaults()
        expected = {"binance", "okx", "bybit", "bitget", "gate", "aster", "hyperliquid"}
        assert set(cfg.venues.keys()) == expected

    def test_built_in_defaults_has_hosts(self):
        cfg = built_in_defaults()
        assert "binance.com" in cfg.hosts
        assert "okx.com" in cfg.hosts

    def test_config_manager_starts_with_builtins(self):
        mgr = RateLimitConfigManager()
        assert "binance" in mgr.config.venues

    def test_config_manager_unchanged_on_none_path(self):
        mgr = RateLimitConfigManager(config_path=None)
        outcome = mgr.refresh()
        assert outcome == "unchanged"


class TestRecommendationEngine:
    def test_empty_flush_returns_empty(self):
        rec = RecommendationEngine(window_ms=60000)
        results = rec.flush(now_ms=120000)
        assert results == []

    def test_records_and_flushes(self):
        rec = RecommendationEngine(window_ms=60000)
        # Seed the window start to a known synthetic time
        rec._window_start_ms = 0
        # Record data using the public API
        rec.record_request("binance", "/api/order", weight=2.0)
        rec.record_request("binance", "/api/order")
        rec.record_limit_hit("binance", "/api/order", retry_after_ms=5000)
        # records were stamped with real wall-clock time; flush at T+120s
        now_ms = rec._window_start_ms + 120_000
        results = rec.flush(now_ms=now_ms)
        assert len(results) >= 1
        r = results[0]
        assert r.venue == "binance"
        assert r.observed_rate_per_min > 0
        assert r.limit_hit_rate > 0

    def test_endpoint_count(self):
        rec = RecommendationEngine()
        rec.record_request("a", "x")
        rec.record_request("b", "y")
        assert rec.endpoint_count == 2


class TestRateLimitRuntime:
    def test_global_singleton(self):
        rt = RateLimitRuntime()
        install_global_rate_limit_runtime(rt)
        assert global_rate_limit_runtime() is rt

    def test_refresh_interval(self):
        rt = RateLimitRuntime()
        assert rt.refresh_interval_secs() == 30
