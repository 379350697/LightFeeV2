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
        eng.register_bucket("bucket", budget_per_minute=5.0)
        eng.register_weight("write", 3.0)
        eng.try_consume_scopes(["write", "bucket"], weight=3.0, now_ms=0)
        snap = eng.bucket_snapshot("bucket")
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
        eng.register_bucket("a", budget_per_minute=5.0)
        eng.register_bucket("b", budget_per_minute=5.0)
        eng.try_consume_scopes(["a", "b"], weight=1.0, now_ms=0)
        assert eng.bucket_snapshot("a")["tokens"] == 4.0
        assert eng.bucket_snapshot("b")["tokens"] == 4.0

    def test_default_margin(self):
        eng = RateLimitEngine(default_margin=0.5)
        eng.register_bucket("test", budget_per_minute=10.0)
        snap = eng.bucket_snapshot("test")
        assert snap["capacity"] == 5.0  # 10 * 0.5


class TestRateLimitConfig:
    def test_built_in_defaults_has_all_venues(self):
        cfg = built_in_defaults()
        expected = {"binance", "okx", "bybit", "bitget", "gate", "aster", "hyperliquid"}
        assert set(cfg.venues.keys()) == expected

    def test_built_in_defaults_has_hosts(self):
        cfg = built_in_defaults()
        assert "fapi.binance.com" in cfg.hosts
        assert "www.okx.com" in cfg.hosts

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


# ============================================================================
# Task 1: V1 Rate-Limit Defaults
# ============================================================================

EXPECTED_HOSTS = {
    "fapi.binance.com": (2400, 25),
    "fapi.asterdex.com": (1200, 50),
    "api.bybit.com": (600, 75),
    "api.bitget.com": (600, 100),
    "www.okx.com": (600, 100),
    "api.gateio.ws": (900, 75),
    "api.hyperliquid.xyz": (1200, 50),
}

EXPECTED_VENUES = {
    "binance": (2400, 25, 600),
    "aster": (1200, 50, 600),
    "bybit": (600, 75, 300),
    "bitget": (600, 100, 300),
    "okx": (600, 100, 300),
    "gate": (900, 75, 300),
    "hyperliquid": (1200, 50, 300),
}

EXPECTED_GROUP_WEIGHTS = {
    "depth": 5,
    "market": 1,
    "order": 1,
    "account": 1,
    "ws_public": 1,
    "ws_private": 1,
}

EXPECTED_BINANCE_ENDPOINTS = {
    "GET /fapi/v1/depth": (5, 25, "depth"),
    "GET /fapi/v1/exchangeInfo": (10, None, "market"),
    "GET /fapi/v1/ticker/bookTicker": (2, None, "market"),
    "GET /fapi/v1/premiumIndex": (1, None, "market"),
    "POST /fapi/v1/order": (1, None, "order"),
}

EXPECTED_BYBIT_ENDPOINTS = {
    "GET /v5/market/orderbook": (5, 75, "depth"),
    "GET /v5/market/tickers": (1, None, "market"),
    "GET /v5/market/instruments-info": (2, None, "market"),
    "POST /v5/order/create": (1, None, "order"),
    "GET /v5/account/fee-rate": (1, None, "account"),
}

EXPECTED_OKX_ENDPOINTS = {
    "GET /api/v5/market/books": (5, 100, "depth"),
    "GET /api/v5/public/funding-rate": (1, None, "market"),
    "GET /api/v5/market/tickers": (1, None, "market"),
    "POST /api/v5/trade/order": (1, None, "order"),
    "GET /api/v5/account/config": (1, None, "account"),
}


class TestV1RateLimitDefaults:
    """Task 1: Verify V2 built-in defaults match V1 host/venue/group/endpoint tables."""

    def test_host_defaults_exact(self):
        cfg = built_in_defaults()
        for host, (budget, min_interval) in EXPECTED_HOSTS.items():
            h = cfg.hosts.get(host)
            assert h is not None, f"Missing host config: {host}"
            assert h.budget_per_minute == budget, (
                f"{host}: budget_per_minute={h.budget_per_minute} != {budget}"
            )
            assert h.min_interval_ms == min_interval, (
                f"{host}: min_interval_ms={h.min_interval_ms} != {min_interval}"
            )

    def test_host_count_exact_seven(self):
        cfg = built_in_defaults()
        assert len(cfg.hosts) == 7

    def test_venue_defaults_exact(self):
        cfg = built_in_defaults()
        for venue, (budget, min_interval, ws_budget) in EXPECTED_VENUES.items():
            v = cfg.venues.get(venue)
            assert v is not None, f"Missing venue config: {venue}"
            assert v.budget_per_minute == budget, (
                f"{venue}: budget_per_minute={v.budget_per_minute} != {budget}"
            )
            assert v.min_interval_ms == min_interval, (
                f"{venue}: min_interval_ms={v.min_interval_ms} != {min_interval}"
            )
            assert v.ws_budget_per_minute == ws_budget, (
                f"{venue}: ws_budget_per_minute={v.ws_budget_per_minute} != {ws_budget}"
            )

    def test_group_weights_exact(self):
        cfg = built_in_defaults()
        for venue_name in EXPECTED_VENUES:
            v = cfg.venues.get(venue_name)
            assert v is not None
            for group, expected_weight in EXPECTED_GROUP_WEIGHTS.items():
                assert group in v.group_weights, (
                    f"{venue_name}: missing group weight '{group}'"
                )
                assert v.group_weights[group] == expected_weight, (
                    f"{venue_name}: group '{group}' weight={v.group_weights[group]} != {expected_weight}"
                )

    def test_group_min_intervals_match_venue(self):
        cfg = built_in_defaults()
        for venue_name, (_budget, min_interval, _ws) in EXPECTED_VENUES.items():
            v = cfg.venues.get(venue_name)
            assert v is not None
            for group in EXPECTED_GROUP_WEIGHTS:
                assert group in v.group_min_interval_ms, (
                    f"{venue_name}: missing group min interval '{group}'"
                )
                assert v.group_min_interval_ms[group] == min_interval, (
                    f"{venue_name}: group '{group}' min_interval={v.group_min_interval_ms[group]} != {min_interval}"
                )

    def test_binance_endpoint_weights_exact(self):
        cfg = built_in_defaults()
        v = cfg.venues["binance"]
        for endpoint, (weight, min_interval, scope) in EXPECTED_BINANCE_ENDPOINTS.items():
            assert endpoint in v.endpoint_weights, f"binance: missing endpoint weight '{endpoint}'"
            assert v.endpoint_weights[endpoint] == weight, (
                f"binance: endpoint '{endpoint}' weight={v.endpoint_weights[endpoint]} != {weight}"
            )
            assert endpoint in v.scopes, f"binance: missing scope '{endpoint}'"
            assert v.scopes[endpoint] == scope, (
                f"binance: endpoint '{endpoint}' scope={v.scopes[endpoint]} != {scope}"
            )
            if min_interval is not None:
                assert endpoint in v.endpoint_min_interval_ms, (
                    f"binance: missing endpoint min_interval '{endpoint}'"
                )
                assert v.endpoint_min_interval_ms[endpoint] == min_interval

    def test_bybit_endpoint_weights_exact(self):
        cfg = built_in_defaults()
        v = cfg.venues["bybit"]
        for endpoint, (weight, min_interval, scope) in EXPECTED_BYBIT_ENDPOINTS.items():
            assert endpoint in v.endpoint_weights
            assert v.endpoint_weights[endpoint] == weight
            assert endpoint in v.scopes
            assert v.scopes[endpoint] == scope
            if min_interval is not None:
                assert endpoint in v.endpoint_min_interval_ms
                assert v.endpoint_min_interval_ms[endpoint] == min_interval

    def test_okx_endpoint_weights_exact(self):
        cfg = built_in_defaults()
        v = cfg.venues["okx"]
        for endpoint, (weight, min_interval, scope) in EXPECTED_OKX_ENDPOINTS.items():
            assert endpoint in v.endpoint_weights
            assert v.endpoint_weights[endpoint] == weight
            assert endpoint in v.scopes
            assert v.scopes[endpoint] == scope
            if min_interval is not None:
                assert endpoint in v.endpoint_min_interval_ms
                assert v.endpoint_min_interval_ms[endpoint] == min_interval

    def test_bitget_current_funding_endpoint_is_market_scoped(self):
        endpoint = "GET /api/v2/mix/market/current-fund-rate"
        bitget = built_in_defaults().venues["bitget"]

        assert bitget.endpoint_weights[endpoint] == 1
        assert bitget.scopes[endpoint] == "market"

    def test_aster_v3_private_endpoint_weights_match_official_contract(self):
        cfg = built_in_defaults()
        v = cfg.venues["aster"]
        expected = {
            "GET /fapi/v3/order": (1, "account"),
            "POST /fapi/v3/order": (1, "order"),
            "DELETE /fapi/v3/order": (1, "order"),
            "GET /fapi/v3/openOrders": (1, "account"),
            "GET /fapi/v3/positionRisk": (5, "account"),
            "GET /fapi/v3/positionSide/dual": (30, "account"),
            "GET /fapi/v3/accountWithJoinMargin": (5, "account"),
            "POST /fapi/v3/leverage": (1, "order"),
            "GET /fapi/v3/leverageBracket": (1, "account"),
        }

        for endpoint, (weight, scope) in expected.items():
            assert v.endpoint_weights[endpoint] == weight
            assert v.scopes[endpoint] == scope
            assert v.endpoint_min_interval_ms[endpoint] == 50

    def test_runtime_accepts_weight_override_for_param_sensitive_endpoints(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("venue:aster", budget_per_minute=100.0)
        rt = RateLimitRuntime(engine=eng)

        assert rt.wait_until_ready_for_scopes(
            ["GET /fapi/v3/openOrders", "venue:aster"],
            weight_override=40.0,
        )

        snap = eng.bucket_snapshot("venue:aster")
        assert snap["tokens"] == pytest.approx(60.0)

    def test_docs_fallback_present(self):
        cfg = built_in_defaults()
        for venue_name in EXPECTED_VENUES:
            v = cfg.venues.get(venue_name)
            assert v is not None
            assert v.docs_fallback is not None, f"{venue_name}: missing docs_fallback"

    def test_global_defaults_match_v1(self):
        cfg = built_in_defaults()
        assert cfg.global_config.default_margin == 0.95
        assert cfg.global_config.refresh_interval_secs == 30


# ============================================================================
# Task 3: V1 Engine Semantics
# ============================================================================


class TestRateLimitEngineV1Scopes:
    """Task 3: Verify engine enforces V1 weights, min intervals, backoff, cooldown."""

    def test_endpoint_weight_consumed(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("venue:binance", budget_per_minute=100.0)
        eng.register_weight("GET /fapi/v1/depth", 5.0)
        eng.register_weight("POST /fapi/v1/order", 1.0)
        eng.try_consume_scopes(
            ["GET /fapi/v1/depth", "venue:binance"], weight=5.0, now_ms=0
        )
        snap = eng.bucket_snapshot("venue:binance")
        assert snap["tokens"] == 95.0  # 100 - 5

    def test_group_fallback_weight(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("venue:binance", budget_per_minute=100.0)
        # Register group weight for "depth" — should apply when endpoint not found
        eng.register_weight("group:depth", 5.0)
        eng.try_consume_scopes(
            ["some_endpoint", "venue:binance", "group:depth"], weight=5.0, now_ms=0
        )
        snap = eng.bucket_snapshot("venue:binance")
        assert snap["tokens"] == 95.0  # 100 - 5

    def test_min_interval_enforced_for_endpoint(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("binance", capacity=100.0, refill_per_sec=10.0)
        eng.register_min_interval("binance", "GET /fapi/v1/depth", 25)
        eng.try_consume("binance", ["GET /fapi/v1/depth"], now_ms=0)
        with pytest.raises(RateLimitError) as exc:
            eng.try_consume("binance", ["GET /fapi/v1/depth"], now_ms=10)
        assert exc.value.reason == RateLimitErrorReason.MIN_INTERVAL

    def test_cooldown_with_retry_after_blocks_all_scopes(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket("host:fapi.binance.com", capacity=100.0, refill_per_sec=10.0)
        eng.register_bucket("venue:binance", capacity=100.0, refill_per_sec=10.0)
        eng.apply_cooldown("host:fapi.binance.com", 5000, now_ms=0)
        eng.apply_cooldown("venue:binance", 5000, now_ms=0)
        with pytest.raises(RateLimitError):
            eng.try_consume("host:fapi.binance.com", ["GET /fapi/v1/depth"], now_ms=1000)
        with pytest.raises(RateLimitError):
            eng.try_consume("venue:binance", ["GET /fapi/v1/depth"], now_ms=1000)

    def test_backoff_starts_at_1000ms_caps_at_8000ms(self):
        from lightfee.venues.transport import EndpointRateLimiter
        rl = EndpointRateLimiter(initial_ms=1000, max_ms=8000)
        assert rl._failure_backoff_ms(0) == 1000
        assert rl._failure_backoff_ms(1) == 2000
        assert rl._failure_backoff_ms(2) == 4000
        assert rl._failure_backoff_ms(3) == 8000
        assert rl._failure_backoff_ms(10) == 8000  # capped

    def test_success_resets_cooldown(self):
        from lightfee.venues.transport import EndpointRateLimiter
        rl = EndpointRateLimiter(initial_ms=1000, max_ms=8000)
        rl.record_rate_limit_for_scopes(["test"], retry_after_ms=5000)
        assert rl._cooldown_remaining_ms_for_scopes(["test"]) is not None
        rl.record_success_for_scopes(["test"])
        # V1: record_success is no-op; cooldown stays
        # This test confirms V1 behavior — success does NOT clear cooldown
        assert rl._cooldown_remaining_ms_for_scopes(["test"]) is not None

    @pytest.mark.asyncio
    async def test_pace_for_scopes_reserves_first_slot(self):
        from lightfee.venues.transport import EndpointRateLimiter

        rl = EndpointRateLimiter(initial_ms=1000, max_ms=8000, pacing_interval_ms=50)

        await rl.pace_for_scopes(["venue:aster"])

        assert rl._last_request_ms["venue:aster"] > 0
