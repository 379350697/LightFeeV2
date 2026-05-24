# V2 V1 Exchange Signing, Time, and Rate-Limit Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align LightFeeV2 live exchange REST signing, server-time usage, `recvWindow`, retry-on-time-error behavior, and all rate-limit parameters/scopes with Rust LightFee V1.

**Architecture:** Keep the public adapter interface stable and fix parity in shared transport/rate-limit layers. Add V1-shaped venue metadata and rate-limit config/runtime support, then make venue request builders consume that metadata so each request signs and throttles exactly like V1.

**Tech Stack:** Python 3.11+, `httpx`, `pytest`, existing LightFeeV2 venue adapters, Rust V1 as reference.

---

## Required References

- Spec: `docs/superpowers/specs/2026-05-14-v2-v1-exchange-signing-time-rate-limit-parity-design.md`
- V1: `/media/wl/新加卷/codex/LightFee/src/live/common.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/live/binance.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/live/aster.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/live/okx.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/live/gate.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/live/hyperliquid.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/rate_limit/config.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/rate_limit/mod.rs`
- V1: `/media/wl/新加卷/codex/LightFee/src/resilience.rs`

## File Map

- Modify: `lightfee/venues/specs.py`
  - Add V1 transport metadata: server time path, recv window, safety margin, venue scope, REST group scope, endpoint group mapping.
- Modify: `lightfee/venues/transport.py`
  - Implement V1 URL encoding, server-time offset cache, Binance-compatible query-only private signing, V1 request scopes, success reset, retry header parsing, and timestamp-error retry hooks.
- Modify: `lightfee/rate_limit/config.py`
  - Replace coarse V2 defaults with V1-shaped host/venue config, endpoint weights, group weights, min intervals, scopes, docs fallback.
- Modify: `lightfee/rate_limit/engine.py`
  - Align token bucket, min interval, backoff, cooldown, global runtime, and recommendation behavior with V1.
- Modify: `lightfee/apps/live.py`
  - Ensure live startup refreshes/applies built-in or file-backed rate-limit config before requests use the global runtime.
- Modify: `tests/test_venues_transport.py`
  - Add/adjust signing, server-time, retry, and request-scope tests.
- Modify: `tests/test_rate_limit.py`
  - Add V1 parameter/config/runtime tests.
- Optional create: `tests/fixtures/venues/rate_limit_v1_defaults.json`
  - Only if the implementer wants a fixture table rather than inline expectations.

## Task 0: Safety and Impact

**Files:**
- No code changes.

- [ ] **Step 1: Refresh GitNexus if needed**

Run:

```bash
npx gitnexus analyze
```

Expected: repository indexed successfully. Scope-extraction warnings for empty `__init__.py` files are acceptable if indexing completes.

- [ ] **Step 2: Run upstream impact before editing symbols**

Run GitNexus MCP impact for each target before code edits:

```text
impact(target="VenueTransport", file_path="lightfee/venues/transport.py", kind="Class", direction="upstream")
impact(target="EndpointRateLimiter", file_path="lightfee/venues/transport.py", kind="Class", direction="upstream")
impact(target="RateLimitEngine", file_path="lightfee/rate_limit/engine.py", kind="Class", direction="upstream")
impact(target="RateLimitConfigManager", file_path="lightfee/rate_limit/config.py", kind="Class", direction="upstream")
```

Expected: report blast radius. If any result is HIGH or CRITICAL, stop and report before editing.

## Task 1: Lock V1 Rate-Limit Parameters in Tests

**Files:**
- Modify: `tests/test_rate_limit.py`

- [ ] **Step 1: Add failing tests for V1 built-in defaults**

Add a test class named `TestV1RateLimitDefaults` with assertions for:

```python
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
```

Also assert endpoint weights and endpoint min intervals from the spec exactly.

- [ ] **Step 2: Run the new rate-limit default tests**

Run:

```bash
pytest tests/test_rate_limit.py::TestV1RateLimitDefaults -q
```

Expected before implementation: FAIL because V2 config does not expose V1-shaped host/venue endpoint/group defaults.

## Task 2: Implement V1-Shaped Rate-Limit Config

**Files:**
- Modify: `lightfee/rate_limit/config.py`
- Test: `tests/test_rate_limit.py`

- [ ] **Step 1: Replace coarse config dataclasses**

Implement dataclasses equivalent to V1:

```python
@dataclass
class RateLimitGlobalConfig:
    default_margin: float = 0.95
    refresh_interval_secs: int = 30

@dataclass
class RateLimitHostConfig:
    budget_per_minute: int | None = None
    min_interval_ms: int | None = None

@dataclass
class VenueDocsFallback:
    budget_per_minute: int | None = None
    min_interval_ms: int | None = None

@dataclass
class RateLimitVenueConfig:
    budget_per_minute: int | None = None
    min_interval_ms: int | None = None
    ws_budget_per_minute: int | None = None
    endpoint_weights: dict[str, int] = field(default_factory=dict)
    group_weights: dict[str, int] = field(default_factory=dict)
    endpoint_min_interval_ms: dict[str, int] = field(default_factory=dict)
    group_min_interval_ms: dict[str, int] = field(default_factory=dict)
    scopes: dict[str, str] = field(default_factory=dict)
    docs_fallback: VenueDocsFallback = field(default_factory=VenueDocsFallback)
```

Keep `RateLimitConfig.default_margin` and `refresh_interval_secs` properties or update tests/callers to read `config.global_config`.

- [ ] **Step 2: Port V1 built-in defaults exactly**

Populate `built_in_defaults()` from the spec's tables. Host keys must be exact hostnames, not broad suffixes like `binance.com`.

- [ ] **Step 3: Parse V1 TOML shape**

Support V1 table names:

```toml
[global]
default_margin = 0.95
refresh_interval_secs = 30

[host."fapi.binance.com"]
budget_per_minute = 2400
min_interval_ms = 25

[venue.binance]
budget_per_minute = 2400
min_interval_ms = 25
ws_budget_per_minute = 600
```

When a file omits sections, merge overrides into built-ins, matching V1 behavior.

- [ ] **Step 4: Run defaults tests**

Run:

```bash
pytest tests/test_rate_limit.py::TestV1RateLimitDefaults -q
```

Expected: PASS.

## Task 3: Align RateLimitEngine and Runtime with V1

**Files:**
- Modify: `lightfee/rate_limit/engine.py`
- Test: `tests/test_rate_limit.py`

- [ ] **Step 1: Add failing tests for weighted scopes**

Add tests that prove:

- `GET /fapi/v1/depth` consumes weight `5`.
- `POST /fapi/v1/order` consumes weight `1`.
- group fallback `depth` consumes weight `5` when endpoint has no explicit weight.
- min interval is enforced for endpoint and group scopes.
- cooldown with `Retry-After` blocks all scopes passed to `record_rate_limit_for_scopes`.
- backoff without `Retry-After` starts at `1000ms` and caps at `8000ms`.

Run:

```bash
pytest tests/test_rate_limit.py::TestRateLimitEngineV1Scopes -q
```

Expected before implementation: FAIL.

- [ ] **Step 2: Port V1 engine semantics**

Implement or adapt:

```python
DEFAULT_BACKOFF_INITIAL_MS = 1000
DEFAULT_BACKOFF_MAX_MS = 8000
DEFAULT_BUDGET_RETRY_MS = 50

def normalize_scopes(scopes: list[str]) -> list[str]:
    return [s for s in scopes if s]

def resolve_request_weight(engine: RateLimitEngine, scopes: list[str]) -> float:
    endpoint_scope = next((s for s in scopes if s.startswith(("GET ", "POST ", "PUT ", "DELETE "))), "")
    group_scope = next((s for s in scopes if s.startswith("group:")), None)
    return engine.resolve_weight(endpoint_scope, group_scope)
```

Make the bucket model match V1:

- register budget as `budget_per_minute * default_margin`
- refill over 60 seconds
- per-scope min intervals
- apply cooldown/backoff to every passed scope with a bucket
- success resets failure state where V1 resets it

- [ ] **Step 3: Make runtime usable from async transport**

V2 transport is async. Do not block the event loop with `time.sleep`.

Provide one of these:

```python
async def wait_until_ready_for_scopes(self, scopes: list[str]) -> None:
    ...
```

or:

```python
async def async_wait_until_ready_for_scopes(self, scopes: list[str]) -> None:
    ...
```

Then update transport to call the async path.

- [ ] **Step 4: Apply config to engine exactly like V1**

When runtime refreshes config:

- register `host:<host>` buckets
- register `venue:<venue>` buckets
- register `group:<venue>:ws_public` and `group:<venue>:ws_private` buckets when `ws_budget_per_minute` exists
- register endpoint weights
- register group weights as both `group:<group>` and `group:<venue>:<group>` if needed by scope augmentation
- register endpoint and group min intervals

- [ ] **Step 5: Run rate-limit tests**

Run:

```bash
pytest tests/test_rate_limit.py -q
```

Expected: PASS.

## Task 4: Add Venue Transport Metadata

**Files:**
- Modify: `lightfee/venues/specs.py`
- Test: `tests/test_venues_transport.py`

- [ ] **Step 1: Extend VenueSpec**

Add fields with conservative defaults:

```python
server_time_path: str = ""
server_time_safety_margin_ms: int = 0
recv_window_ms: int | None = None
venue_scope: str = ""
rest_group_scope: str = "group:rest"
endpoint_scope_map: dict[str, str] = field(default_factory=dict)
endpoint_weights: dict[str, int] = field(default_factory=dict)
endpoint_min_interval_ms: dict[str, int] = field(default_factory=dict)
```

- [ ] **Step 2: Populate per-venue metadata**

Examples:

```python
# Binance
server_time_path="/fapi/v1/time"
server_time_safety_margin_ms=1000
recv_window_ms=10000
venue_scope="venue:binance"

# Aster
server_time_path="/fapi/v1/time"
server_time_safety_margin_ms=0
recv_window_ms=10000
venue_scope="venue:aster"

# Bybit
server_time_path="/v5/market/time"
recv_window_ms=5000
venue_scope="venue:bybit"

# OKX
server_time_path="/api/v5/public/time"
venue_scope="venue:okx"
```

For Bitget, Gate, and Hyperliquid, add venue scopes and endpoint scope mappings but do not add server-time offset unless V1 has it.

- [ ] **Step 3: Add metadata tests**

Run:

```bash
pytest tests/test_venues_transport.py::TestVenueSpecV1TransportMetadata -q
```

Expected: PASS after implementation.

## Task 5: Implement Server-Time Offset Cache

**Files:**
- Modify: `lightfee/venues/transport.py`
- Test: `tests/test_venues_transport.py`

- [ ] **Step 1: Add failing server-time tests**

Add tests that use a fake `httpx.AsyncClient` or monkeypatched `_request_public_raw` helper to assert:

- Binance adjusted timestamp = server time minus `1000ms` safety margin.
- Aster adjusted timestamp = server time.
- OKX adjusted timestamp becomes ISO8601.
- Bybit adjusted timestamp uses `/v5/market/time`.
- Cached offset avoids a second time endpoint call.
- Clearing offset forces a new time endpoint call.

Run:

```bash
pytest tests/test_venues_transport.py::TestVenueServerTimeOffset -q
```

Expected before implementation: FAIL.

- [ ] **Step 2: Implement transport cache**

Add to `VenueTransport.__init__`:

```python
self._time_offset_ms: int | None = None
```

Add methods:

```python
async def _server_timestamp_ms(self) -> int:
    ...

def _clear_server_time_offset(self) -> None:
    self._time_offset_ms = None
```

Parsing rules:

- Binance/Aster: JSON key `serverTime`.
- OKX: JSON `data[0].ts`.
- Bybit: response `time` or `result.timeNano // 1_000_000`.

- [ ] **Step 3: Use adjusted timestamps in signing**

Make `_build_signed_request` async or split it:

```python
async def _build_signed_request_async(...)
```

Update `_request()` to await the async builder. Keep the existing sync `_build_signed_request` only for tests if needed, but tests should move to async where server time matters.

- [ ] **Step 4: Run server-time tests**

Run:

```bash
pytest tests/test_venues_transport.py::TestVenueServerTimeOffset -q
```

Expected: PASS.

## Task 6: Fix Binance/Aster Query-Only Private Signing

**Files:**
- Modify: `lightfee/venues/transport.py`
- Test: `tests/test_venues_transport.py`

- [ ] **Step 1: Add failing Binance/Aster order signature tests**

Assert for Binance and Aster:

- `POST /fapi/v1/order` returns an empty request body or `None`.
- query contains `symbol`, `side`, `quantity`, `type`, `recvWindow`, `timestamp`, `signature`.
- signature is HMAC-SHA256 hex over the URL-encoded query before `signature`.
- parameter order matches V1 call order.

Run:

```bash
pytest tests/test_venues_transport.py::TestBinanceAsterV1QueryOnlySigning -q
```

Expected before implementation: FAIL because current V2 puts order fields in JSON body.

- [ ] **Step 2: Add URL encoding helper**

Add V1-compatible helper:

```python
from urllib.parse import urlencode

def _build_query_v1(params: list[tuple[str, object]]) -> str:
    return urlencode([(k, str(v)) for k, v in params])
```

Use list-of-tuples for Binance/Aster private params so order is preserved.

- [ ] **Step 3: Update Binance/Aster order path**

In `place_order()`:

- Build `params` for Binance/Aster instead of `body`.
- Call `_request("POST", spec.order_path, params=params, private=True)`.
- Let the Binance-compatible signer append `recvWindow`, adjusted `timestamp`, and `signature`.

- [ ] **Step 4: Update Binance/Aster private GETs**

For `fetch_position`, `fetch_account_risk_snapshot`, order lookup/reconciliation, and amend/cancel helpers if present:

- Pass private params as ordered query params.
- Do not sign public requests.
- Do not add timestamp to public depth/ticker requests.

- [ ] **Step 5: Run signing tests**

Run:

```bash
pytest tests/test_venues_transport.py::TestBinanceAsterV1QueryOnlySigning -q
pytest tests/test_venues_transport.py::TestBinanceAsterPostSigning -q
```

Expected: PASS. Update older tests that expected sorted query params so they assert V1 URL-encoded call order instead.

## Task 7: Align OKX, Bybit, Bitget, Gate Signing

**Files:**
- Modify: `lightfee/venues/transport.py`
- Test: `tests/test_venues_transport.py`

- [ ] **Step 1: Add tests for each signing payload**

Extend existing signing tests to assert:

- OKX GET signs `timestamp + METHOD + path + ?query`.
- OKX POST signs `timestamp + METHOD + path + body`.
- Bybit GET signs `timestamp + api_key + recv_window + query_without_question_mark`.
- Bybit POST signs `timestamp + api_key + recv_window + body`.
- Bitget signs `timestamp + METHOD + path_with_query + body`.
- Gate signs newline payload with SHA512 body hash.
- Hyperliquid EIP-712 path does not call HMAC helpers.

Run:

```bash
pytest tests/test_venues_transport.py::TestOkxGetSignature tests/test_venues_transport.py::TestBybitGetSignature -q
```

Expected: existing tests may pass locally but should fail once server-time expectations are added.

- [ ] **Step 2: Implement payload parity**

Use the V1 payload rules from the spec. Use adjusted server time only for OKX and Bybit.

- [ ] **Step 3: Run full transport signing block**

Run:

```bash
pytest tests/test_venues_transport.py -k "Signature or Signing or Headers" -q
```

Expected: PASS.

## Task 8: Implement V1 Request Scope Derivation in Transport

**Files:**
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/rate_limit/engine.py` if runtime scope augmentation lives there
- Test: `tests/test_venues_transport.py`
- Test: `tests/test_rate_limit.py`

- [ ] **Step 1: Add failing scope tests**

Add tests for `_rest_rate_limit_scopes(method, path, base_url)`:

```python
assert scopes for Binance GET /fapi/v1/depth include:
["GET /fapi/v1/depth", "host:fapi.binance.com", "venue:binance"]

assert scopes for Bybit GET /v5/market/orderbook include:
endpoint "GET /v5/market/orderbook"
group "group:rest" or venue-qualified V1 equivalent used by runtime
venue "venue:bybit"
host "host:api.bybit.com"
```

Also assert Bitget/Gate/Hyperliquid group behavior and host normalization.

- [ ] **Step 2: Implement host normalization**

Add:

```python
from urllib.parse import urlparse

def _normalize_host_scope(base_url: str) -> str:
    parsed = urlparse(base_url.rstrip("/"))
    host = parsed.netloc or parsed.path.split("/")[0]
    return f"host:{host}"
```

- [ ] **Step 3: Implement endpoint normalization**

Add:

```python
def _normalize_rest_endpoint_key(method: str, path: str) -> str:
    clean_path = path.split("?", 1)[0]
    return f"{method.upper()} {clean_path}".strip()
```

- [ ] **Step 4: Use same scopes for wait, pace, success, and rate-limit hit**

In `_request()`:

- Derive scopes once.
- Before send: await rate limiter/global runtime readiness and pacing using those scopes.
- On success: record success for those scopes.
- On 429/418: parse retry header and record cooldown for those scopes.

- [ ] **Step 5: Run scope tests**

Run:

```bash
pytest tests/test_venues_transport.py::TestV1RestRateLimitScopes tests/test_rate_limit.py::TestRateLimitEngineV1Scopes -q
```

Expected: PASS.

## Task 9: Parse Retry/Reset Headers Like V1

**Files:**
- Modify: `lightfee/venues/transport.py`
- Test: `tests/test_venues_transport.py`

- [ ] **Step 1: Add tests**

Add tests for:

- `Retry-After: 5` -> `5000ms`
- HTTP-date `Retry-After` -> computed ms
- Bybit `X-Bapi-Limit-Reset-Timestamp`
- Gate `X-RateLimit-Reset`
- Gate `X-Gate-RateLimit-Reset`

- [ ] **Step 2: Implement parser helpers**

Implement helpers equivalent to V1:

```python
def _parse_retry_after_ms(headers: Mapping[str, str], now: datetime | None = None) -> int | None:
    ...

def _parse_reset_header_ms(headers: Mapping[str, str], header_name: str, now_ms: int) -> int | None:
    ...

def _parse_venue_retry_after_ms(venue: Venue, headers: Mapping[str, str], now_ms: int) -> int | None:
    ...
```

- [ ] **Step 3: Run header parser tests**

Run:

```bash
pytest tests/test_venues_transport.py::TestV1RetryAfterParsing -q
```

Expected: PASS.

## Task 10: Retry Once on Binance/Aster Time Errors

**Files:**
- Modify: `lightfee/venues/transport.py`
- Test: `tests/test_venues_transport.py`

- [ ] **Step 1: Add failing tests**

Simulate a first private request returning:

```json
{"code": -1021, "msg": "Timestamp for this request is outside of the recvWindow."}
```

Assert:

- V2 clears `_time_offset_ms`.
- V2 sleeps approximately `100ms` or uses a test-injected sleeper.
- V2 retries once.
- V2 does not retry indefinitely.

Also test signature error retry only if V1 retry predicate includes that exact error class. Do not broaden retries beyond V1.

- [ ] **Step 2: Implement retry hook**

Add:

```python
def _is_time_offset_retryable(self, error: TransportError) -> bool:
    ...
```

In `_request()`, when private Binance/Aster request receives retryable time error:

- clear time offset
- retry once

- [ ] **Step 3: Run retry tests**

Run:

```bash
pytest tests/test_venues_transport.py::TestBinanceAsterTimeErrorRetry -q
```

Expected: PASS.

## Task 11: Wire Global Runtime into Live Transport

**Files:**
- Modify: `lightfee/apps/live.py`
- Modify: `lightfee/venues/registry.py` if needed
- Modify: `lightfee/venues/transport.py`
- Test: `tests/engine/test_runtime_lane_scheduling.py`
- Test: `tests/test_rate_limit.py`

- [ ] **Step 1: Add tests for startup config application**

Assert live startup:

- creates `RateLimitConfigManager`
- installs global runtime
- refreshes/applies config before request flow
- runtime refresh interval remains `30s`

- [ ] **Step 2: Apply built-ins immediately**

Ensure `RateLimitRuntime` has V1 built-in defaults applied even if `rate_limits.toml` is absent.

- [ ] **Step 3: Preserve SIGHUP and periodic reload behavior**

Do not remove existing SIGHUP reload or runtime periodic reload.

- [ ] **Step 4: Run live scheduling tests**

Run:

```bash
pytest tests/engine/test_runtime_lane_scheduling.py tests/test_rate_limit.py -q
```

Expected: PASS.

## Task 12: Full Verification and GitNexus Change Detection

**Files:**
- No new code changes unless failures require fixes.

- [ ] **Step 1: Run targeted test suites**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_rate_limit.py tests/engine/test_runtime_lane_scheduling.py -q
```

Expected: PASS.

- [ ] **Step 2: Run broader venue-adjacent tests if time allows**

Run:

```bash
pytest tests/venues tests/test_close_execution.py tests/test_entry_sync.py -q
```

Expected: PASS or unrelated pre-existing failures documented with exact failure names.

- [ ] **Step 3: Run GitNexus detect changes before commit**

Run:

```text
detect_changes(scope="all", repo="LightFeeV2")
```

Expected: affected symbols and flows are limited to venue transport, rate-limit config/runtime, live startup wiring, and tests.

- [ ] **Step 4: Commit**

Only commit after tests and GitNexus detect-changes are reviewed:

```bash
git add lightfee/venues/transport.py lightfee/venues/specs.py lightfee/rate_limit/config.py lightfee/rate_limit/engine.py lightfee/apps/live.py tests/test_venues_transport.py tests/test_rate_limit.py tests/engine/test_runtime_lane_scheduling.py
git commit -m "fix: align exchange signing and rate limits with v1"
```

Expected: commit succeeds.

## Handoff Notes

- Do not hand-roll exchange-specific behavior from external docs unless V1 lacks the behavior. V1 is the source of truth for this task.
- Do not broaden retries beyond V1's retry predicates.
- Do not change strategy, risk, persistence, or local-L2 behavior except for REST bootstrap rate-limit scopes already covered by V1.
- Keep Hyperliquid EIP-712 isolated from HMAC changes.
- If implementation requires splitting `lightfee/venues/transport.py`, keep the first split focused: signing/time helpers and rate-limit scope helpers only.
