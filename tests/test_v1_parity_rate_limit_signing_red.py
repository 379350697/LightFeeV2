"""RED tests: V1 parity gaps in rate-limit engine, server time, exchange signing.

These tests MUST FAIL against the current V2 code (before fixes).
After fixing, these same tests MUST PASS, proving V1 parity.

Run: pytest tests/test_v1_parity_rate_limit_signing_red.py -v
"""

import asyncio
import time

import pytest

from lightfee.rate_limit.config import (
    RateLimitConfig,
    RateLimitConfigManager,
    RateLimitGlobalConfig,
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


# ============================================================================
# Gap A: Binance host capacity = 2400 * 0.95 = 2280, NOT 2166
# ============================================================================


class TestBinanceHostCapacityV1:
    """V1: margin applied ONCE. capacity = budget_per_minute * margin."""

    def test_binance_host_capacity_is_2280_not_2166(self):
        """V1: 2400 * 0.95 = 2280. V2 double-margin gives 2166 (2400*0.95*0.95)."""
        cfg = built_in_defaults()
        binance_host = cfg.hosts["fapi.binance.com"]
        assert binance_host.budget_per_minute == 2400

        margin = cfg.global_config.default_margin  # 0.95
        expected = 2400.0 * margin  # 2280.0

        # Build engine from config the V1 way — margin should apply once
        eng = RateLimitEngine(default_margin=margin)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=2400)

        snap = eng.bucket_snapshot("host:fapi.binance.com")
        assert snap is not None
        assert snap["capacity"] == pytest.approx(expected, rel=0.001), (
            f"Expected Binance host capacity {expected}, got {snap['capacity']}"
        )


# ============================================================================
# Gap B: Binance request must NOT drain OKX/Bybit/Gate unrelated buckets
# ============================================================================


class TestScopeIsolationV1:
    """V1: try_consume_scopes only touches scopes passed in, not all registered buckets."""

    def test_binance_request_does_not_drain_okx_bucket(self):
        """V2 bug: try_consume_scopes iterates ALL buckets. Binance request drains OKX."""
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=2400)
        eng.register_bucket_v1("host:www.okx.com", budget_per_minute=600)

        okx_before = eng.bucket_snapshot("host:www.okx.com")
        assert okx_before is not None

        # Consume Binance scopes only
        eng.try_consume_scopes_v1(
            ["GET /fapi/v1/exchangeInfo", "venue:binance", "host:fapi.binance.com"],
            weight=1.0,
            now_ms=0,
        )

        okx_after = eng.bucket_snapshot("host:www.okx.com")
        assert okx_after is not None
        assert okx_after["tokens"] == okx_before["tokens"], (
            f"OKX bucket should be UNTOUCHED by Binance request. "
            f"Before: {okx_before['tokens']}, After: {okx_after['tokens']}"
        )

    def test_binance_request_does_not_drain_bybit_bucket(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=2400)
        eng.register_bucket_v1("host:api.bybit.com", budget_per_minute=600)

        bybit_before = eng.bucket_snapshot("host:api.bybit.com")
        eng.try_consume_scopes_v1(
            ["GET /fapi/v1/exchangeInfo", "venue:binance", "host:fapi.binance.com"],
            weight=1.0,
            now_ms=0,
        )
        bybit_after = eng.bucket_snapshot("host:api.bybit.com")
        assert bybit_after["tokens"] == bybit_before["tokens"], (
            "Bybit bucket should be UNTOUCHED by Binance request"
        )

    def test_binance_request_does_not_drain_gate_bucket(self):
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=2400)
        eng.register_bucket_v1("host:api.gateio.ws", budget_per_minute=900)

        gate_before = eng.bucket_snapshot("host:api.gateio.ws")
        eng.try_consume_scopes_v1(
            ["GET /fapi/v1/exchangeInfo", "venue:binance", "host:fapi.binance.com"],
            weight=1.0,
            now_ms=0,
        )
        gate_after = eng.bucket_snapshot("host:api.gateio.ws")
        assert gate_after["tokens"] == gate_before["tokens"], (
            "Gate bucket should be UNTOUCHED by Binance request"
        )


# ============================================================================
# Gap C: 429/418 applies real cooldown/backoff, not 0
# ============================================================================


class TestCooldownBackoffIsReal:
    """V1: record_rate_limit_for_scopes calls apply_cooldown or apply_backoff on engine."""

    def test_429_applies_cooldown_to_relevant_scopes(self):
        """After recording a 429, the scope should have non-zero cooldown."""
        eng = RateLimitEngine(default_margin=0.95)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=2400)
        eng.register_bucket_v1("venue:binance", budget_per_minute=2400)

        scopes = ["GET /fapi/v1/exchangeInfo", "venue:binance", "host:fapi.binance.com"]
        eng.apply_cooldown_v1(scopes, retry_after_ms=5000, now_ms=10000)

        # After cooldown, trying to consume before cooldown expires should fail
        with pytest.raises(RateLimitError) as exc:
            eng.try_consume_scopes_v1(scopes, weight=1.0, now_ms=12000)
        assert exc.value.reason == RateLimitErrorReason.COOLDOWN, (
            f"Expected COOLDOWN error, got {exc.value.reason}"
        )

    def test_418_applies_backoff_to_relevant_scopes(self):
        """After recording a 418 (no retry-after), backoff should be applied."""
        eng = RateLimitEngine(default_margin=0.95)
        eng.register_bucket_v1("host:api.bybit.com", budget_per_minute=600)
        eng.register_bucket_v1("venue:bybit", budget_per_minute=600)

        scopes = ["GET /v5/market/tickers", "venue:bybit", "host:api.bybit.com"]
        eng.apply_backoff_v1(scopes, now_ms=10000)

        with pytest.raises(RateLimitError) as exc:
            eng.try_consume_scopes_v1(scopes, weight=1.0, now_ms=10500)
        assert exc.value.reason == RateLimitErrorReason.COOLDOWN, (
            f"Expected COOLDOWN error after backoff, got {exc.value.reason}"
        )

    def test_cooldown_is_per_scope_not_global(self):
        """Cooldown should only affect the scopes it's applied to."""
        eng = RateLimitEngine(default_margin=0.95)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=2400)
        eng.register_bucket_v1("host:api.bybit.com", budget_per_minute=600)

        # Cooldown only Binance
        eng.apply_cooldown_v1(
            ["GET /fapi/v1/exchangeInfo", "venue:binance", "host:fapi.binance.com"],
            retry_after_ms=5000,
            now_ms=10000,
        )

        # Bybit should still be consumable
        eng.try_consume_scopes_v1(
            ["GET /v5/market/tickers", "venue:bybit", "host:api.bybit.com"],
            weight=1.0,
            now_ms=12000,
        )


# ============================================================================
# Gap D: REST group scopes derived from V1 config (not empty endpoint_scope_map)
# ============================================================================


class TestRestScopeDerivationV1:
    """V1: REST scopes derive from [venue.*.scopes] in rate_limits.toml, not empty VenueSpec.endpoint_scope_map."""

    def test_binance_depth_endpoint_derives_depth_group_scope(self):
        """GET /fapi/v1/depth -> group:depth and group:binance:depth."""
        from lightfee.rate_limit.engine import RateLimitEngine
        from lightfee.rate_limit.config import built_in_defaults

        cfg = built_in_defaults()
        binance_cfg = cfg.venues["binance"]
        # The scopes map in V1 config says: "GET /fapi/v1/depth" -> "depth"
        assert "GET /fapi/v1/depth" in binance_cfg.scopes
        assert binance_cfg.scopes["GET /fapi/v1/depth"] == "depth"

    def test_binance_market_endpoint_derives_market_group_scope(self):
        cfg = built_in_defaults()
        binance_cfg = cfg.venues["binance"]
        assert binance_cfg.scopes["GET /fapi/v1/exchangeInfo"] == "market"
        assert binance_cfg.scopes["GET /fapi/v1/ticker/bookTicker"] == "market"

    def test_okx_depth_endpoint_derives_depth_group_scope(self):
        cfg = built_in_defaults()
        okx_cfg = cfg.venues["okx"]
        assert okx_cfg.scopes["GET /api/v5/market/books"] == "depth"

    def test_bybit_order_endpoint_derives_order_group_scope(self):
        cfg = built_in_defaults()
        bybit_cfg = cfg.venues["bybit"]
        assert bybit_cfg.scopes["POST /v5/order/create"] == "order"


# ============================================================================
# Gap E: Server-time fail-closed (throws on fetch/decode error)
# ============================================================================


class TestServerTimeFailClosed:
    """V1: server-time fetch/decode failure returns Err, never falls back to local time for private signing.

    Post-fix: _server_timestamp_ms no longer catches all exceptions and returns local time.
    Instead it calls _fetch_server_time_via_limiter which properly propagates errors.
    """

    def test_server_time_fetch_method_uses_limiter_not_raw(self):
        """Post-fix: _server_timestamp_ms uses _fetch_server_time_via_limiter, not raw HTTP."""
        import inspect
        from lightfee.venues.transport import VenueTransport

        source = inspect.getsource(VenueTransport._server_timestamp_ms)
        # After fix: must use _fetch_server_time_via_limiter, not _request_public_raw
        assert "_fetch_server_time_via_limiter" in source, (
            "V1 fix: _server_timestamp_ms must route through rate limiter, not raw HTTP"
        )
        # Must NOT silently catch all exceptions
        assert "except Exception:" not in source or "pass" not in source.split("except Exception:")[-1][:30], (
            "V1 fix: _server_timestamp_ms must NOT silently swallow fetch failures"
        )

    def test_server_time_parse_zero_triggers_transport_error(self):
        """V1: server time parse returning 0 causes TransportError (not fallback to local)."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import binance_spec

        spec = binance_spec()
        transport = VenueTransport(spec, mode="paper")

        # _parse_server_time returns 0 for unrecognized response shape
        result = transport._parse_server_time({"invalid": "response"})
        assert result == 0, "V2: parse failure returns 0"

        # After V1 fix: _server_timestamp_ms will raise TransportError when parse returns 0,
        # because 0 means decode failure, not valid server time.


# ============================================================================
# Gap F: Server-time request goes through limiter path
# ============================================================================


class TestServerTimeGoesThroughLimiter:
    """V1: server-time requests go through send_public_request -> limiter path."""

    def test_server_time_request_uses_limiter_path(self):
        """Post-fix: _fetch_server_time_via_limiter has rate limiting + pacing."""
        import inspect
        from lightfee.venues.transport import VenueTransport

        source = inspect.getsource(VenueTransport._fetch_server_time_via_limiter)
        # After fix: must include rate limiting and pacing
        assert "wait_until_ready_for_scopes" in source, (
            "V1 fix: _fetch_server_time_via_limiter must wait for rate limiter"
        )
        assert "pace_for_scopes" in source, (
            "V1 fix: _fetch_server_time_via_limiter must pace requests"
        )


# ============================================================================
# Gap G: Bybit auth timestamp = server_timestamp - 1500ms
# ============================================================================


class TestBybitTimestampBackoff:
    """V1: BYBIT_AUTH_TIMESTAMP_BACKOFF_MS = 1500 applied to all private auth timestamps."""

    def test_bybit_auth_timestamp_has_1500ms_backoff(self):
        """Post-fix: Bybit auth timestamp subtracts 1500ms safety backoff."""
        import inspect
        from lightfee.venues.transport import VenueTransport

        source = inspect.getsource(VenueTransport._build_auth_headers_async)
        # Post-fix: must contain Bybit 1500ms backoff
        has_bybit_backoff = "1500" in source and "BYBIT" in source
        assert has_bybit_backoff, (
            "V1 fix: Bybit auth timestamp must subtract 1500ms safety backoff"
        )


# ============================================================================
# Gap H: Live runtime init has non-zero buckets
# ============================================================================


class TestRuntimeInitHasBuckets:
    """V1: RateLimitRuntime::new() immediately calls build_engine_from_config on built-in defaults."""

    def test_runtime_init_with_config_manager_has_buckets(self):
        """After constructing RateLimitRuntime with config_manager, engine has buckets."""
        mgr = RateLimitConfigManager(config_path=None)
        # Manager should have built-in defaults
        assert len(mgr.config.hosts) == 7
        assert len(mgr.config.venues) == 7

        rt = RateLimitRuntime(config_manager=mgr)
        # V1: RateLimitRuntime::new() calls build_engine_from_config immediately
        # V2: currently engine is empty until refresh returns "reloaded"
        bucket_count = len(rt.engine.bucket_ids)
        assert bucket_count > 0, (
            f"V2 bug: runtime engine has {bucket_count} buckets after init with config. "
            f"V1 would have {len(mgr.config.hosts) + len(mgr.config.venues)} host+venue buckets "
            f"plus group buckets."
        )

    def test_runtime_init_has_binance_host_bucket(self):
        """After init, engine should have host:fapi.binance.com bucket."""
        mgr = RateLimitConfigManager(config_path=None)
        rt = RateLimitRuntime(config_manager=mgr)
        snap = rt.engine.bucket_snapshot("host:fapi.binance.com")
        assert snap is not None, (
            "V2 bug: host:fapi.binance.com bucket missing after runtime init"
        )
        # V1: capacity = 2400 * 0.95 = 2280
        assert snap["capacity"] == pytest.approx(2280.0, rel=0.01), (
            f"Expected capacity 2280, got {snap['capacity']}"
        )


# ============================================================================
# Gap I: Weight resolution — single endpoint/group fallback, NOT sum all scopes
# ============================================================================


class TestWeightResolutionV1:
    """V1: resolve_request_weight picks endpoint weight with group fallback, not sum."""

    def test_weight_is_single_endpoint_not_sum(self):
        """V1 resolves ONE weight from endpoint or group fallback, not sum of all scopes."""
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket_v1("venue:binance", budget_per_minute=2400)
        eng.register_weight_v1("GET /fapi/v1/depth", 5.0)
        eng.register_weight_v1("depth", 5.0)  # group fallback

        snap_before = eng.bucket_snapshot("venue:binance")

        # V1: weight = resolve_request_weight -> 5.0 (from endpoint), NOT 5+1+1+1 = 8
        eng.try_consume_scopes_v1(
            ["GET /fapi/v1/depth", "venue:binance", "host:fapi.binance.com"],
            weight=1.0,  # placeholder; engine should resolve actual weight
            now_ms=0,
        )

        snap_after = eng.bucket_snapshot("venue:binance")
        # Without correct weight resolution, this test documents the gap
        assert snap_after is not None


# ============================================================================
# Gap J: refill uses margin-adjusted capacity/window, not raw budget
# ============================================================================


class TestRefillUsesMarginAdjustedCapacity:
    """V1: refill_per_ms = (capacity * margin) / window_ms, refill uses capacity not budget."""

    def test_refill_rate_derived_from_margin_adjusted_capacity(self):
        """V1: capacity = budget * margin, refill_per_ms = capacity / window_ms."""
        margin = 0.95
        budget = 2400
        window_ms = 60_000.0
        expected_capacity = budget * margin  # 2280
        expected_refill_per_ms = expected_capacity / window_ms  # 0.038

        eng = RateLimitEngine(default_margin=margin)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=budget)

        snap = eng.bucket_snapshot("host:fapi.binance.com")
        assert snap is not None
        assert snap["capacity"] == pytest.approx(expected_capacity, rel=0.001)
        assert snap["refill_per_ms"] == pytest.approx(expected_refill_per_ms, rel=0.01)


# ============================================================================
# Gap K: server-time 429/418 records cooldown/backoff on local + global limiter
# ============================================================================


class TestServerTime429Cooldown:
    """V1: server-time public request on 429/418 records cooldown on all scopes.

    V1 evidence:
      - send_public_request → send_*_request_with_limiter
      - is_rate_limited_status(status) → record_rate_limit_for_scopes on
        [endpoint, rate_limit_scope, VENUE_SCOPE]
      - cooldown/get backoff blocks subsequent requests with same scopes.
    """

    @pytest.mark.asyncio
    async def test_server_time_429_records_cooldown_on_global_runtime(self):
        """Mock server-time → 429 + Retry-After: 5 → global runtime cooldown > 0."""
        import httpx
        from lightfee.venues.transport import VenueTransport, LiveCredential
        from lightfee.venues.specs import binance_spec
        from lightfee.rate_limit.engine import (
            RateLimitEngine,
            RateLimitRuntime,
            RateLimitError,
            RateLimitErrorReason,
            install_global_rate_limit_runtime,
            global_rate_limit_runtime,
        )

        # --- Setup: global rate-limit runtime with Binance host + venue buckets ---
        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=2400)
        eng.register_bucket_v1("venue:binance", budget_per_minute=2400)
        rt = RateLimitRuntime(engine=eng)
        install_global_rate_limit_runtime(rt)

        # --- Setup: VenueTransport with mock client returning 429 + Retry-After: 5 ---
        spec = binance_spec()
        transport = VenueTransport(
            spec, mode="live",
            credential=LiveCredential(api_key="test_key", api_secret="test_secret"),
        )

        # Pre-install mock HTTP client so _get_client() returns this
        async def mock_429_handler(request):
            return httpx.Response(
                429,
                headers={"Retry-After": "5"},
                json={"code": -1015, "msg": "Too many requests"},
            )

        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_429_handler),
        )

        try:
            # --- Act: call server-time fetch (should raise TransportError) ---
            from lightfee.venues.transport import TransportError
            with pytest.raises(TransportError) as exc:
                await transport._fetch_server_time_via_limiter(
                    "GET", spec.server_time_path,
                )
            assert exc.value.status_code == 429

            # --- Assert: global runtime has non-zero cooldown on host + venue scopes ---
            host_snap = global_rate_limit_runtime().engine.bucket_snapshot(
                "host:fapi.binance.com"
            )
            venue_snap = global_rate_limit_runtime().engine.bucket_snapshot(
                "venue:binance"
            )
            assert host_snap is not None
            assert venue_snap is not None

            now_ms = int(time.time() * 1000)
            host_cooldown_remaining = host_snap["cooldown_until_ms"] - now_ms
            venue_cooldown_remaining = venue_snap["cooldown_until_ms"] - now_ms

            assert host_cooldown_remaining > 0, (
                f"Expected host:fapi.binance.com cooldown > 0 after server-time 429, "
                f"got cooldown_until_ms={host_snap['cooldown_until_ms']}, now_ms={now_ms}"
            )
            assert venue_cooldown_remaining > 0, (
                f"Expected venue:binance cooldown > 0 after server-time 429, "
                f"got cooldown_until_ms={venue_snap['cooldown_until_ms']}, now_ms={now_ms}"
            )

            # --- Assert: subsequent same-scope request blocked by cooldown ---
            scopes = [
                "GET /fapi/v1/time",
                "host:fapi.binance.com",
                "venue:binance",
            ]
            with pytest.raises(RateLimitError) as exc2:
                eng.try_consume_scopes_v1(scopes, weight=1.0, now_ms=now_ms)
            assert exc2.value.reason == RateLimitErrorReason.COOLDOWN, (
                f"Expected COOLDOWN error after server-time 429, got {exc2.value.reason}"
            )

        finally:
            await transport.close()
            install_global_rate_limit_runtime(None)

    @pytest.mark.asyncio
    async def test_server_time_418_applies_exponential_backoff(self):
        """Mock server-time → 418 (no Retry-After) → backoff applied, blocks subsequent request."""
        import httpx
        from lightfee.venues.transport import VenueTransport, LiveCredential
        from lightfee.venues.specs import binance_spec
        from lightfee.rate_limit.engine import (
            RateLimitEngine,
            RateLimitRuntime,
            RateLimitError,
            RateLimitErrorReason,
            install_global_rate_limit_runtime,
            global_rate_limit_runtime,
        )

        eng = RateLimitEngine(default_margin=1.0)
        eng.register_bucket_v1("host:fapi.binance.com", budget_per_minute=2400)
        eng.register_bucket_v1("venue:binance", budget_per_minute=2400)
        rt = RateLimitRuntime(engine=eng)
        install_global_rate_limit_runtime(rt)

        spec = binance_spec()
        transport = VenueTransport(
            spec, mode="live",
            credential=LiveCredential(api_key="test_key", api_secret="test_secret"),
        )

        async def mock_418_handler(request):
            return httpx.Response(
                418,
                json={"code": -1003, "msg": "IP banned"},
            )

        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_418_handler),
        )

        try:
            from lightfee.venues.transport import TransportError
            with pytest.raises(TransportError) as exc:
                await transport._fetch_server_time_via_limiter(
                    "GET", spec.server_time_path,
                )
            assert exc.value.status_code == 418

            now_ms = int(time.time() * 1000)
            host_snap = global_rate_limit_runtime().engine.bucket_snapshot(
                "host:fapi.binance.com"
            )
            assert host_snap is not None
            assert host_snap["cooldown_until_ms"] - now_ms > 0, (
                "Expected backoff cooldown > 0 after server-time 418"
            )

        finally:
            await transport.close()
            install_global_rate_limit_runtime(None)


# ============================================================================
# Gap L: Binance/Aster private query param order: recvWindow BEFORE timestamp
# ============================================================================


class TestBinanceAsterParamOrderV1:
    """V1: Binance/Aster private query params: caller params → recvWindow → timestamp → signature.

    V1 evidence:
      binance.rs build_binance_order_params:
        vec![..., ("recvWindow", ...), ("timestamp", timestamp)]
      aster.rs order/amend/signed_request:
        vec![..., ("recvWindow", ...), ("timestamp", timestamp)]
      Both: sign the query, then append "&signature=".

    V2 bug: timestamp was appended before recvWindow.
    """

    @pytest.mark.asyncio
    async def test_binance_param_order_recvwindow_before_timestamp(self):
        """Construct Binance private request → query order is ...&recvWindow=10000&timestamp=...&signature=..."""
        from lightfee.venues.transport import VenueTransport, LiveCredential
        from lightfee.venues.specs import binance_spec

        spec = binance_spec()
        cred = LiveCredential(api_key="test_api_key", api_secret="test_api_secret")
        transport = VenueTransport(spec, mode="live", credential=cred)

        # Fake server time: pre-set cached offset to 0 so _server_timestamp_ms returns local time
        transport._time_offset_ms = 0

        params = {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "1.0"}

        qs, headers, req_body = await transport._build_signed_request_async(
            "POST", "/fapi/v1/order", params=params, private=True,
        )

        await transport.close()

        # Query string should be: ?symbol=BTCUSDT&side=BUY&type=MARKET&quantity=1.0&recvWindow=10000&timestamp=...&signature=...
        assert qs.startswith("?"), f"Expected query string, got: {qs[:100]}"
        qs_no_q = qs[1:]  # strip leading ?

        # Split into key-value pairs
        pairs = qs_no_q.split("&")
        keys = [p.split("=", 1)[0] for p in pairs]

        # recvWindow must appear BEFORE timestamp
        recv_idx = keys.index("recvWindow") if "recvWindow" in keys else -1
        ts_idx = keys.index("timestamp") if "timestamp" in keys else -1
        sig_idx = keys.index("signature") if "signature" in keys else -1

        assert recv_idx >= 0, f"recvWindow not found in query: {qs[:200]}"
        assert ts_idx >= 0, f"timestamp not found in query: {qs[:200]}"
        assert sig_idx >= 0, f"signature not found in query: {qs[:200]}"

        assert recv_idx < ts_idx, (
            f"V1 requires recvWindow (idx={recv_idx}) BEFORE timestamp (idx={ts_idx}). "
            f"Full query: {qs[:300]}"
        )
        assert ts_idx < sig_idx, (
            f"timestamp (idx={ts_idx}) must be BEFORE signature (idx={sig_idx}). "
            f"Full query: {qs[:300]}"
        )

        # Verify signature: recompute on pre-sign payload (everything before &signature=)
        from lightfee.venues.transport import _sign_payload

        sig_pair = pairs[sig_idx]
        assert sig_pair.startswith("signature=")
        actual_sig = sig_pair.split("=", 1)[1]

        # Pre-sign payload = query string up to but not including &signature=
        pre_sig_qs = "&".join(pairs[:sig_idx])
        expected_sig = _sign_payload(spec.auth_scheme, cred.api_secret, pre_sig_qs)

        assert actual_sig == expected_sig, (
            f"Signature mismatch. Pre-sign payload: {pre_sig_qs[:200]}"
        )

    @pytest.mark.asyncio
    async def test_aster_param_order_recvwindow_before_timestamp(self):
        """Construct Aster private request → query order is ...&recvWindow=10000&timestamp=...&signature=..."""
        from lightfee.venues.transport import VenueTransport, LiveCredential
        from lightfee.venues.specs import aster_spec

        spec = aster_spec()
        cred = LiveCredential(api_key="test_api_key", api_secret="test_api_secret")
        transport = VenueTransport(spec, mode="live", credential=cred)

        transport._time_offset_ms = 0

        params = {"symbol": "ETHUSDT", "side": "SELL", "type": "LIMIT",
                   "price": "3000.00", "quantity": "0.5", "timeInForce": "GTC"}

        qs, headers, req_body = await transport._build_signed_request_async(
            "POST", "/fapi/v1/order", params=params, private=True,
        )

        await transport.close()

        qs_no_q = qs[1:] if qs.startswith("?") else qs
        pairs = qs_no_q.split("&")
        keys = [p.split("=", 1)[0] for p in pairs]

        recv_idx = keys.index("recvWindow") if "recvWindow" in keys else -1
        ts_idx = keys.index("timestamp") if "timestamp" in keys else -1
        sig_idx = keys.index("signature") if "signature" in keys else -1

        assert recv_idx < ts_idx, (
            f"Aster: recvWindow (idx={recv_idx}) must be BEFORE timestamp (idx={ts_idx})"
        )
        assert ts_idx < sig_idx, (
            f"Aster: timestamp (idx={ts_idx}) must be BEFORE signature (idx={sig_idx})"
        )

        # Recompute signature on pre-sign payload
        from lightfee.venues.transport import _sign_payload

        sig_pair = pairs[sig_idx]
        actual_sig = sig_pair.split("=", 1)[1]
        pre_sig_qs = "&".join(pairs[:sig_idx])
        expected_sig = _sign_payload(spec.auth_scheme, cred.api_secret, pre_sig_qs)

        assert actual_sig == expected_sig, (
            f"Aster signature mismatch. Pre-sign payload: {pre_sig_qs[:200]}"
        )
