# V2 Exchange Signing, Time, and Rate-Limit V1 Parity Design

## Goal

Make LightFeeV2 exchange REST behavior match the Rust LightFee V1 implementation for all live venues, with special focus on the Binance production errors observed after deployment:

- `429 Too Many Requests`
- Binance `-1022 Signature for this request is not valid`
- Binance `-1021 Timestamp outside recvWindow`

This is not a deployment failure. The live process may continue running while external venue requests fail. The fix is to align V2's exchange signing, server-time handling, `recvWindow`, retry-on-time-error behavior, and rate-limit parameters/scopes with V1.

## Scope

In scope:

- Binance
- Aster
- OKX
- Bybit
- Bitget
- Gate
- Hyperliquid
- Shared V2 transport, rate-limit config/runtime, and transport tests

Out of scope:

- Strategy behavior
- Live process lifecycle
- Order sizing semantics not directly required by signing/rate-limit parity
- WebSocket local-L2 protocol changes, except where REST bootstrap rate-limit scopes are shared

## V1 Source References

Use the Rust V1 repository at `/media/wl/新加卷/codex/LightFee` as the source of truth.

Primary references:

- `src/live/binance.rs`
- `src/live/aster.rs`
- `src/live/okx.rs`
- `src/live/bybit.rs`
- `src/live/bitget.rs`
- `src/live/gate.rs`
- `src/live/hyperliquid.rs`
- `src/live/common.rs`
- `src/resilience.rs`
- `src/rate_limit/config.rs`
- `src/rate_limit/mod.rs`

Current V2 targets:

- `lightfee/venues/transport.py`
- `lightfee/venues/specs.py`
- `lightfee/venues/binance.py`
- `lightfee/venues/aster.py`
- `lightfee/venues/okx.py`
- `lightfee/venues/bybit.py`
- `lightfee/venues/bitget.py`
- `lightfee/venues/gate.py`
- `lightfee/venues/hyperliquid.py`
- `lightfee/rate_limit/config.py`
- `lightfee/rate_limit/engine.py`
- `lightfee/apps/live.py`
- `tests/test_venues_transport.py`
- `tests/test_rate_limit.py`

## Current V2 Gaps

### Binance/Aster Signature Shape

V2 currently builds Binance/Aster private signatures from query params only, while order fields are sent as JSON body for `POST /fapi/v1/order`. That leaves the signed payload missing `symbol`, `side`, `quantity`, `type`, `price`, `timeInForce`, and `reduceOnly`.

V1 builds the Binance-compatible query with all signed params, signs exactly that URL-encoded query, then appends `signature` to the URL. The request may have a body only when a V1 call explicitly passes one, but Binance/Aster order params are query params.

Required V2 behavior:

- Binance/Aster private order placement must pass order fields as query params, not JSON body.
- The signature payload must be exactly the URL query string before `signature`.
- The query string must be URL-encoded with the same semantics as Rust `url::form_urlencoded::Serializer`.
- Do not sort params unless V1 sorted them for that exact call. For Binance/Aster V1, preserve the caller-provided param order.

### Binance/Aster `recvWindow` and Server Time

V2 currently signs with `time.time() * 1000` and does not add Binance/Aster `recvWindow`.

Required V2 behavior:

- Binance: `recvWindow=10000`.
- Aster: `recvWindow=10000`.
- Binance timestamp comes from cached server-time offset:
  - Request `GET /fapi/v1/time`.
  - Offset = `server_time - now_ms - 1000`.
  - Timestamp = `now_ms + offset`.
- Aster timestamp comes from cached server-time offset:
  - Request `GET /fapi/v1/time`.
  - Offset = `server_time - now_ms`.
  - Timestamp = `now_ms + offset`.
- On Binance/Aster retryable timestamp/signature/order-mode errors that V1 retries, clear cached server-time offset and retry once after the same short delay V1 uses.

### OKX/Bybit Server Time

V2 currently signs OKX/Bybit with local wall-clock time.

Required V2 behavior:

- OKX:
  - Fetch server timestamp through `GET /api/v5/public/time`.
  - Cache offset.
  - Convert adjusted server time to the ISO8601 string used in V1.
  - Sign `timestamp + METHOD + request_path_with_query + body`.
- Bybit:
  - Fetch server timestamp through `GET /v5/market/time`.
  - Cache offset.
  - Use adjusted timestamp in `X-BAPI-TIMESTAMP`.
  - Use `X-BAPI-RECV-WINDOW=5000`.
  - Sign `timestamp + api_key + recv_window + query_or_body`.

### Bitget/Gate/Hyperliquid Time and Signing

Required V2 behavior:

- Bitget keeps local millisecond timestamp, V1 header names, locale header, and payload:
  - `timestamp + METHOD + request_path_with_query + body`
  - HMAC-SHA256 base64
- Gate keeps local seconds timestamp and payload:
  - `METHOD + "\n" + path + "\n" + query + "\n" + sha512(body) + "\n" + timestamp`
  - HMAC-SHA512 hex
  - `X-Gate-Size-Decimal: 1`
- Hyperliquid keeps the EIP-712 signing path isolated from HMAC exchange signing changes.

## Rate-Limit Parity Requirements

V2 must use V1's rate-limit parameters and scope model, not the current coarse V2 `capacity/refill_per_sec` approximation.

### Core Runtime Constants

Match V1:

- Default margin: `0.95`
- Refresh interval: `30s`
- Backoff initial: `1000ms`
- Backoff max: `8000ms`
- Endpoint fallback pacing for `EndpointRateLimiter::new_with_pacing`: `25ms` unless overridden by rate-limit config
- Rate-limited HTTP statuses: `429` and `418`
- Rate-limit hit with `Retry-After`: apply explicit cooldown
- Rate-limit hit without `Retry-After`: apply exponential backoff
- Success resets endpoint/scope failure state and cooldown, matching V1 `record_success_for_scopes`

### Built-In Host Defaults

The V2 built-in config must use these V1 values:

| Host | budget_per_minute | min_interval_ms |
|---|---:|---:|
| `fapi.binance.com` | 2400 | 25 |
| `fapi.asterdex.com` | 1200 | 50 |
| `api.bybit.com` | 600 | 75 |
| `api.bitget.com` | 600 | 100 |
| `www.okx.com` | 600 | 100 |
| `api.gateio.ws` | 900 | 75 |
| `api.hyperliquid.xyz` | 1200 | 50 |

### Built-In Venue Defaults

| Venue | budget_per_minute | min_interval_ms | ws_budget_per_minute |
|---|---:|---:|---:|
| Binance | 2400 | 25 | 600 |
| Aster | 1200 | 50 | 600 |
| Bybit | 600 | 75 | 300 |
| Bitget | 600 | 100 | 300 |
| OKX | 600 | 100 | 300 |
| Gate | 900 | 75 | 300 |
| Hyperliquid | 1200 | 50 | 300 |

### Common Group Weights

For every venue:

| Group | Weight |
|---|---:|
| `depth` | 5 |
| `market` | 1 |
| `order` | 1 |
| `account` | 1 |
| `ws_public` | 1 |
| `ws_private` | 1 |

Group min intervals are the venue's default `min_interval_ms` for every group above.

### Endpoint Weights and Min Intervals

Binance:

- `GET /fapi/v1/depth`: weight `5`, min interval `25`, scope `depth`
- `GET /fapi/v1/exchangeInfo`: weight `10`, scope `market`
- `GET /fapi/v1/ticker/bookTicker`: weight `2`, scope `market`
- `GET /fapi/v1/premiumIndex`: weight `1`, scope `market`
- `POST /fapi/v1/order`: weight `1`, scope `order`
- docs fallback: budget `1200`, min interval `50`

Aster:

- `GET /fapi/v1/depth`: weight `5`, min interval `50`, scope `depth`
- `GET /fapi/v1/exchangeInfo`: weight `10`, scope `market`
- `GET /fapi/v1/ticker/bookTicker`: weight `2`, scope `market`
- `GET /fapi/v1/premiumIndex`: weight `1`, scope `market`
- `POST /fapi/v1/order`: weight `1`, scope `order`
- docs fallback: budget `1200`, min interval `50`

Bybit:

- `GET /v5/market/orderbook`: weight `5`, min interval `75`, scope `depth`
- `GET /v5/market/tickers`: weight `1`, scope `market`
- `GET /v5/market/instruments-info`: weight `2`, scope `market`
- `POST /v5/order/create`: weight `1`, scope `order`
- `GET /v5/account/fee-rate`: weight `1`, scope `account`
- docs fallback: budget `600`, min interval `100`

Bitget:

- `GET /api/v3/market/orderbook`: weight `1`, min interval `100`, scope `depth`
- `GET /api/v2/mix/market/merge-depth`: weight `5`, min interval `100`, scope `depth`
- `GET /api/v2/mix/market/ticker`: weight `1`, scope `market`
- `GET /api/v2/mix/market/contracts`: weight `2`, scope `market`
- `POST /api/v2/mix/order/place-order`: weight `1`, scope `order`
- docs fallback: budget `600`, min interval `100`

OKX:

- `GET /api/v5/market/books`: weight `5`, min interval `100`, scope `depth`
- `GET /api/v5/public/funding-rate`: weight `1`, scope `market`
- `GET /api/v5/market/tickers`: weight `1`, scope `market`
- `POST /api/v5/trade/order`: weight `1`, scope `order`
- `GET /api/v5/account/config`: weight `1`, scope `account`
- docs fallback: budget `600`, min interval `100`

Gate:

- `GET /api/v4/futures/usdt/order_book`: weight `5`, min interval `75`, scope `depth`
- `GET /api/v4/futures/usdt/tickers`: weight `1`, scope `market`
- `GET /api/v4/futures/usdt/contracts`: weight `2`, scope `market`
- `POST /api/v4/futures/usdt/orders`: weight `1`, scope `order`
- docs fallback: budget `900`, min interval `75`

Hyperliquid:

- `POST /info`: weight `2`, min interval `50`, scope `market`
- `POST /exchange`: weight `1`, scope `order`
- docs fallback: budget `1200`, min interval `50`

### Scope Model

Each REST request must derive:

- normalized endpoint key, e.g. `GET /fapi/v1/depth`
- venue scope, e.g. `venue:binance`
- host scope, normalized to hostname, e.g. `host:fapi.binance.com`
- group scope when applicable, e.g. `group:rest` or V1's venue-qualified group bucket in the global runtime
- legacy host scope compatibility where V1 calls `legacy_scopes()`

For Binance/Aster/OKX, V1 direct limiter calls use:

- `[endpoint, rate_limit_scope, venue_scope]`

For Bybit/Bitget/Gate/Hyperliquid, V1 builds `RestRateLimitScopes` and uses `legacy_scopes()` for local fallback plus `engine_scopes()` when the global rate-limit runtime is installed.

V2 must preserve this two-layer behavior:

- local `EndpointRateLimiter` fallback must pace/cool down the same legacy scopes V1 did
- global `RateLimitRuntime` must consume weighted/min-interval buckets using engine scopes

### Reset Header Parsing

V2 must parse V1's retry headers:

- Common: `Retry-After`
- Bybit: `Retry-After` or `X-Bapi-Limit-Reset-Timestamp`
- Gate: `Retry-After` or `X-RateLimit-Reset` or `X-Gate-RateLimit-Reset`

## Acceptance Criteria

- Binance/Aster private order tests prove order fields are in the query, `recvWindow=10000` is present, and the signature matches the exact URL-encoded pre-signature query.
- Binance/Aster/OKX/Bybit server time tests prove local wall-clock skew is corrected through cached offsets.
- `-1021` style Binance/Aster errors clear cached offset and retry once.
- Rate-limit tests prove all V1 host, venue, endpoint, group, weight, min-interval, fallback, and reset-header parameters exist in V2.
- Transport request tests prove 429/418 records cooldown on every V1 scope for the request.
- Success tests prove `record_success_for_scopes` clears failures/cooldowns.
- Existing venue contract/signature tests continue passing.

## Verification Commands

Run at minimum:

```bash
pytest tests/test_venues_transport.py tests/test_rate_limit.py -q
pytest tests/engine/test_runtime_lane_scheduling.py -q
```

Before committing implementation changes, run:

```bash
npx gitnexus detect-changes
```

If GitNexus reports stale index before impact/detect, run:

```bash
npx gitnexus analyze
```
