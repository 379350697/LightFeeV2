# BUG-20260514-exchange-signing-time-rate-limit-v1-parity

Status: fixed; verified 2026-05-14
Severity: critical
Component: `venue-transport`, `exchange-signing`, `server-time-sync`, `rate-limit-runtime`, `v1-parity`
Fingerprint: `v2.exchange-v1-parity.tests-green.runtime-semantics-red`
First Seen: 2026-05-14 +08:00 during post-implementation acceptance review
First Seen Commit: `94a1d5e`
Related Refactor: `V1 exchange REST signing, server-time, recvWindow, and rate-limit parity`
Fixed In: V1 parity engine rewrite + transport fixes (2026-05-14)
Verified In: 2119 tests passing + 4 behavioral tests added (server-time 429 cooldown probe, server-time 418 backoff probe, Binance param order/signature probe, Aster param order/signature probe) on 2026-05-14

## Summary

Commit `94a1d5e` claimed V1 parity for exchange REST signing, server-time sync, recvWindow, and rate-limit scopes. Targeted tests passed, but acceptance review found that the live runtime behavior is not code-level V1 parity.

The main pattern repeats an earlier V2 failure mode: V2 copied visible V1 concepts and parameter tables, while the production control flow still differs from V1 in failure semantics, bucket consumption, cooldown application, and scope derivation.

## Symptoms

- Binance external errors still include `429 Too Many Requests`, `-1022 Signature for this request is not valid`, and `-1021 Timestamp outside recvWindow`.
- Tests pass while real exchange-facing behavior remains divergent from V1.
- Rate-limit config values exist, but global runtime can start with zero buckets and does not apply cooldown on 429/418.
- Server-time fetch can silently fall back to local wall-clock time for private signing.

## Root Causes

### Root Cause A: Global rate-limit runtime is not initialized with effective buckets

`lightfee/apps/live.py` creates and installs `RateLimitRuntime`, but does not apply the built-in or disk-backed config at startup. `RateLimitRuntime.refresh()` only calls `_apply_config()` when the refresh outcome is `reloaded`, so a missing or unchanged config path can leave the global engine empty.

### Root Cause B: RateLimitEngine semantics differ from V1

V2 applies the margin twice: once in `_apply_config()` and again in `register_bucket()`. It also refills from raw budget instead of margin-adjusted capacity.

`try_consume_scopes()` iterates every registered bucket for every request. A Binance request can consume OKX, Bybit, Gate, and unrelated buckets. Weight calculation sums default weights for every scope, while V1 resolves one request weight from endpoint scope with group fallback.

### Root Cause C: 429/418 does not create global cooldown/backoff

`RateLimitRuntime.record_rate_limit_for_scopes()` only records recommendation events. It does not call `apply_cooldown()` or `apply_backoff()` on affected scopes, so the global runtime does not actually throttle after exchange rate-limit responses.

### Root Cause D: REST group scopes are not derived from V1 config

`VenueTransport._rest_rate_limit_scopes()` depends on `VenueSpec.endpoint_scope_map`, but every venue spec has an empty map. V1 derives group scopes from `[venue.*.scopes]` in `rate_limits.toml`, including venue-specific group scopes.

### Root Cause E: Server-time sync is fail-open and bypasses V1 limiter path

`VenueTransport._server_timestamp_ms()` catches all server-time fetch/decode failures and signs with local wall-clock time. V1 server timestamp helpers return errors when server-time fetch or decode fails. V2 `_request_public_raw()` also bypasses the same request wrapper/rate limiter that V1 uses for server-time public requests.

### Root Cause F: Bybit timestamp safety backoff is missing

V1 applies `BYBIT_AUTH_TIMESTAMP_BACKOFF_MS = 1500` to private auth timestamps. V2 does not apply this safety backoff.

## Evidence

| Date | Environment | Command / Evidence | Result |
|---|---|---|---|
| 2026-05-14 | local | `pytest tests/test_venues_transport.py tests/test_rate_limit.py tests/engine/test_runtime_lane_scheduling.py -q` | `248 passed` but semantic acceptance failed |
| 2026-05-14 | local | GitNexus compare `HEAD~1` | risk `medium`; changed files: rate-limit config/engine, venue specs/transport, tests |
| 2026-05-14 | local | runtime probe: Binance host bucket after `_apply_config(built_in_defaults())` | capacity `2166.0`, expected V1 `2280.0` |
| 2026-05-14 | local | runtime probe: Binance request against V2 engine | unrelated `venue:okx` tokens changed `541.5 -> 538.5` |
| 2026-05-14 | local | runtime probe: missing config path then `refresh()` | bucket count stayed `0` even though manager config had 7 hosts |
| 2026-05-14 | local | runtime probe: `record_rate_limit_for_scopes(['venue:binance'], 5000)` | cooldown/backoff remained `0/0` |
| 2026-05-14 | local | specs probe | all venue `endpoint_scope_map` lengths were `0` |

## Fix Status

**Fixed.** All eight root causes addressed with V1-code-level parity. Two additional gaps discovered and fixed on 2026-05-14 (see Root Causes G and H below).

## Fix Summary

### Root Cause A (fixed): Global rate-limit runtime not initialized with effective buckets
- `RateLimitRuntime.__init__` now immediately builds engine from config (V1: `RateLimitRuntime::new()` calls `build_engine_from_config`).
- `live.py` also calls `await rate_limit_rt.refresh()` for immediate disk config load.

### Root Cause B (fixed): RateLimitEngine semantics differ from V1
- Complete engine rewrite to V1 scope architecture: `ScopeState` (bucket, weight, min_interval, last_request_at_ms).
- `register_bucket(scope, budget_per_minute)`: capacity = budget * margin (ONCE), refill_per_ms = capacity / window_ms.
- `try_consume_scopes(scopes, weight, now_ms)`: only touches the given scopes, never all registered buckets.
- `resolve_weight(endpoint_scope, group_scope)`: picks endpoint weight with group fallback, or 1.0. Does NOT sum.
- `resolve_min_interval(scopes)`: max of min intervals across given scopes.

### Root Cause C (fixed): 429/418 does not create global cooldown/backoff
- `RateLimitRuntime.record_rate_limit_for_scopes` now calls `engine.apply_cooldown` (with retry_after) or `engine.apply_backoff` (exponential backoff without retry_after).
- Backoff curve: 1000ms initial, 2000/4000/8000ms cap (V1: `DEFAULT_BACKOFF_INITIAL_MS` / `DEFAULT_BACKOFF_MAX_MS`).

### Root Cause D (fixed): REST group scopes not derived from V1 config
- `_rest_rate_limit_scopes` now reads `[venue.*.scopes]` from global rate-limit config (`built_in_defaults()` or runtime config manager).
- Falls back to `built_in_defaults()` when global runtime is not installed.
- Creates `group:<venue>:<group>` and `group:<group>` scopes from endpoint→group mappings.

### Root Cause E (fixed): Server-time sync is fail-open and bypasses V1 limiter path
- `_server_timestamp_ms` no longer catches all exceptions and falls back to local time.
- `_parse_server_time` returning 0 triggers `TransportError` (V1: decode failure → `context("failed to decode")` → Err).
- Server-time requests now go through `_fetch_server_time_via_limiter` which applies rate limiter wait + pacing + global runtime check (V1: `send_public_request` → `send_*_request_with_limiter`).

### Root Cause F (fixed): Bybit timestamp safety backoff missing
- `_build_auth_headers_async` now applies `server_ms - 1500` for Bybit (V1: `BYBIT_AUTH_TIMESTAMP_BACKOFF_MS = 1500`).

### Root Cause G (fixed): Server-time 429/418 did not record cooldown on local + global limiter
- `_fetch_server_time_via_limiter` error path previously only raised `TransportError` without rate-limit recording.
- Now: on 429/418, parses `Retry-After`, calls `self._rate_limiter.record_rate_limit_for_scopes(scopes, retry_after_ms)` and `global_rt.record_rate_limit_for_scopes(scopes, retry_after_ms)` — V1 parity with `send_*_request_with_limiter`.
- On success: calls `self._rate_limiter.record_success_for_scopes(scopes)` — V1 parity.

### Root Cause H (fixed): Binance/Aster private query param order differed from V1
- V2 previously appended `timestamp` before `recvWindow` (`...&timestamp=...&recvWindow=...`).
- V1 order: caller params → `recvWindow` → `timestamp` → sign → `signature`.
- Fixed in `_build_signed_request_async` and `_build_signed_request` (sync fallback).

## Verification Evidence

| Date | Acceptance Criterion | Evidence | Status |
|---|---|---|---|
| 2026-05-14 | Server-time 429 records cooldown on local + global limiter | Behavioral test: mocked 429 + Retry-After: 5 → global runtime `host:fapi.binance.com` cooldown > 0 and `venue:binance` cooldown > 0; subsequent try_consume_scopes raises COOLDOWN | PASS |
| 2026-05-14 | Server-time 418 applies exponential backoff | Behavioral test: mocked 418 (no Retry-After) → `host:fapi.binance.com` cooldown > 0 | PASS |
| 2026-05-14 | Binance private query order = recvWindow BEFORE timestamp | Behavioral test: `_build_signed_request_async` → `...&recvWindow=10000&timestamp=...&signature=...`; signature verified by re-computing HMAC on pre-sign payload | PASS |
| 2026-05-14 | Aster private query order = recvWindow BEFORE timestamp | Behavioral test: `_build_signed_request_async` (aster_spec) → same order; signature recompute verified | PASS |
| 2026-05-14 | Binance host capacity = 2280 (not 2166) | Runtime probe: `capacity=2280.0` | PASS |
| 2026-05-14 | Binance request does not drain OKX/Bybit/Gate buckets | Runtime probe: OKX 600→600, Bybit 600→600, Gate 900→900 | PASS |
| 2026-05-14 | 429 cooldown blocks subsequent requests | Runtime probe: `RateLimitError(cooldown, retry_in_ms=3000)` at T+2000 with 5000ms cooldown | PASS |
| 2026-05-14 | Runtime init has non-zero buckets | Runtime probe: 28 buckets after init, includes `host:fapi.binance.com` | PASS |
| 2026-05-14 | REST scopes derive from V1 config | Code: `_rest_rate_limit_scopes` reads `config.venues[venue_id].scopes` | PASS |
| 2026-05-14 | Server-time fetch failure raises error | Code: `_server_timestamp_ms` has no `except Exception: pass`, uses `_fetch_server_time_via_limiter` | PASS |
| 2026-05-14 | Server-time request goes through limiter | Code: `_fetch_server_time_via_limiter` calls `wait_until_ready_for_scopes` + `pace_for_scopes` | PASS |
| 2026-05-14 | Bybit auth timestamp = server_time - 1500ms | Code: `max(0, server_ms - 1500)` in `_build_auth_headers_async` | PASS |
| 2026-05-14 | Refill uses margin-adjusted capacity/window | Runtime probe: `refill_per_ms=0.038` (= 2280/60000) | PASS |
| 2026-05-14 | Full test suite | 2115 tests passing | PASS |

## Regression Watch

- `429 Too Many Requests` — now triggers real cooldown/backoff on all related scopes
- Binance/Aster `code=-1021`, `recvWindow`, `timestamp` — already handled by time error retry
- Binance/Aster `code=-1022`, `Signature for this request is not valid` — time offset cleared on error, retried with fresh server time
- `v2.exchange-v1-parity.tests-green.runtime-semantics-red` — resolved
- Empty global rate-limit bucket set in live runtime — fixed
- Server-time fetch failure followed by private request signed with local wall-clock time — fixed (fail-closed)
- Binance request draining any non-Binance bucket — fixed (scope-only consumption)
