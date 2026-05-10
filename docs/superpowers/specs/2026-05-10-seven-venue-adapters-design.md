# Seven Venue Adapters Design

**Goal:** Turn the seven V2 venue stubs into one shared live adapter system that can fetch market snapshots, normalize order sizes, fetch positions, and submit orders for Binance, OKX, Bybit, Bitget, Gate, Aster, and Hyperliquid in a single implementation batch.

## Why this spec exists

The current V2 codebase already has venue names, capabilities, config slots, registry wiring, and live runtime entrypoints. What it does not have is a real venue transport layer. Today the per-venue modules are mostly placeholder shells, so the system can name venues but cannot trade through them.

This spec defines the adapter layer that finishes all seven venues together instead of repeating the same work seven times. The Rust repository is the reference for endpoint behavior, request shaping, and venue-specific quirks, but the Python design keeps a shared runtime and small venue-specific codec modules.

## Scope

In scope:

- shared REST transport and signing helpers
- venue credential resolution from config
- normalized adapter contract
- market snapshot fetch
- position fetch
- order submission
- quantity normalization
- explicit venue capability flags
- startup preflight and smoke checks
- shared contract tests for all seven venues

Out of scope for this spec:

- entry and exit orchestration
- sidecar source expansion
- local L2 maintenance
- private websocket parity
- risk-triggered execution
- offline replay, evolution, and reporting

## Current State

The code already has the right anchors:

- `lightfee/venues/base.py` defines capability metadata.
- `lightfee/venues/registry.py` enumerates the seven venues.
- `lightfee/config/schema.py` already carries live credential fields.
- `lightfee/apps/live.py` and `lightfee/engine/runtime.py` already expect a live runtime boundary.
- `tests/test_runtime_smoke.py` already asserts all seven venues exist in the registry.

The missing part is the transport and codec layer that makes those adapters actually useful.

## Design Overview

### 1. One shared transport core

Introduce a shared venue transport layer that owns:

- HTTP client lifecycle
- timeouts and retries
- auth signing
- header construction
- response decoding
- transport error classification

This layer must not contain strategy logic, venue selection logic, or order policy. Its job is to move bytes safely and consistently.

### 2. One adapter contract

Keep the public adapter surface small and stable:

- `fetch_market_snapshot(symbols)`
- `place_order(request)`
- `fetch_position(symbol)`
- `normalize_quantity(symbol, quantity)`

Optional capabilities remain explicit in `VenueCapabilities`, not hidden behind `None` or silent fallback behavior.

### 3. Seven thin venue specs

Each venue module becomes a thin wrapper around a shared adapter runtime plus a venue-specific spec object that defines:

- REST base URLs
- account mode
- auth scheme
- symbol and instrument mapping
- quantity and contract sizing rules
- request and response codecs
- venue-specific rejection translation

The point is not to make all venues look identical. The point is to make their differences declarative and keep them out of the rest of the system.

## Proposed File Layout

This spec assumes the following files will be created or revised:

- `lightfee/venues/base.py`
- `lightfee/venues/common.py`
- `lightfee/venues/registry.py`
- `lightfee/venues/transport.py`
- `lightfee/venues/specs.py`
- `lightfee/venues/binance.py`
- `lightfee/venues/okx.py`
- `lightfee/venues/bybit.py`
- `lightfee/venues/bitget.py`
- `lightfee/venues/gate.py`
- `lightfee/venues/aster.py`
- `lightfee/venues/hyperliquid.py`
- `tests/test_venues_base.py`
- `tests/test_venues_contract.py`
- `tests/fixtures/venues/*`

## Venue Matrix

| Venue | Auth / Account Shape | Special Rules |
|---|---|---|
| Binance | USDM REST, HMAC, single or multi-asset modes | Reduce-only closes are exempt from min notional in the shared sizing helper |
| OKX | V5 REST, unified account, passphrase required | Instrument and account mapping must be explicit |
| Bybit | V5 REST, unified account | Category and symbol mapping must be normalized in the adapter |
| Bitget | Mix V2 REST, classic vs UTA detection | Adapter must detect the account profile and choose the matching path |
| Gate | Futures V4 REST, dual position mode | Decimal contract sizes must be preserved during normalization |
| Aster | Perpetuals FAPI, separate balance and position surfaces | Position precheck failures must fail closed, not silently degrade |
| Hyperliquid | Info API + Exchange API, native perp account | Risk-health support remains unsupported and explicit |

## Adapter Behavior

### Market snapshot

`fetch_market_snapshot()` returns the normalized top-of-book snapshot for the requested symbols. The adapter may use the best public venue endpoint available for that exchange, but the return shape to the rest of the system stays uniform.

### Position fetch

`fetch_position()` returns a single normalized `PositionSnapshot` for the requested symbol. If a venue exposes separate long and short records, the adapter resolves them into the shared domain shape before returning.

### Order submission

`place_order()` accepts a business-order intent, not a venue-native payload. The adapter is responsible for:

- normalizing quantity
- selecting the venue-native order type
- applying reduce-only and post-only semantics
- translating venue rejects into normalized exceptions or rejection results
- reconciling the final order outcome before returning

The caller should never need to know whether the venue used market, IOC limit, native close, or a fallback path.

### Quantity normalization

`normalize_quantity()` floors to the venue step size and respects contract size and minimum quantity rules. The shared helper should also preserve existing reduce-only exceptions, including the current Binance and Aster rule.

## Error Model

Normalize venue errors into a small set of categories:

- transport failure
- authentication failure
- authorization failure
- unsupported capability
- request rejected by venue
- order state uncertain
- normalization failure

This keeps engine code from having to parse exchange-specific text. It also makes live startup decisions deterministic: authentication or unsupported-capability failures should fail closed early, while transport failures should retry under the existing runtime backoff rules.

## Runtime Wiring

The adapter layer plugs into the existing runtime in three places:

1. `lightfee.apps.live` constructs adapters from config and refuses to start if a configured live venue cannot be built.
2. `lightfee.engine.runtime.LiveRuntime` uses the adapters for position checks, order submission, and recovery actions.
3. `lightfee.sidecar` can later reuse the same venue primitives for snapshot collection, but that is a follow-on spec, not part of this one.

The runtime should not contain venue-specific branching beyond capability checks.

## Testing Strategy

Use one parameterized contract suite for all seven venues instead of one hand-written test file per exchange.

Test layers:

- shared sizing and normalization helpers
- shared transport signing and error mapping
- per-venue codec mapping using recorded fixtures
- adapter construction from `config/example.toml` and `config/live.example.toml`
- startup smoke that verifies all seven venues can be instantiated

The default CI suite should use fixtures and fakes only. Live exchange calls belong in manual smoke checks or a dedicated opt-in harness.

## Acceptance Criteria

This spec is complete when all of the following are true:

- all seven venue modules construct a live-capable adapter object
- no required live path uses `NotImplementedError`
- all required public adapter methods return normalized domain objects
- venue-specific rules are hidden inside the adapter layer, not scattered across engine or strategy code
- the same contract test suite runs against all seven venues
- live startup can build every configured venue from the existing config schema

## Completion / Closure

**Status:** 完整闭环 — 7/7 venues live-capable, all 8 mandatory deviations fixed, 432 tests passing.

### Per-Venue Capability Matrix

| Venue | L2 Data | Risk Health | Live Order | Reconcile | Testnet | Account Contract | Auth |
|---|---|---|---|---|---|---|---|
| Binance | true_l2 | supported | supported | order_fill | supported | single_or_multi_asset | HMAC-SHA256 |
| OKX | true_l2 | supported | supported | order_fill | supported | unified_account | HMAC-SHA256 + passphrase |
| Bybit | true_l2 | supported | supported | order_fill | supported | unified_account | HMAC-SHA256 |
| Bitget | true_l2 | **unsupported** | supported | **unsupported** | unknown | detect_classic_vs_uta | HMAC-SHA256 + passphrase |
| Gate | true_l2 | **unsupported** | supported | **unsupported** | unknown | dual_position_mode | HMAC-SHA512 |
| Aster | true_l2 | supported | supported | order_fill | unknown | single_or_multi_asset | HMAC-SHA256 |
| Hyperliquid | true_l2 | **unsupported** | **supported** | **unsupported** | unknown | native_perp_account | EIP-712 (secp256k1) |

### Fixed Deviation List (8/8)

| # | Deviation | Fix | Tests |
|---|---|---|---|
| 1 | OKX private GET signature missing `?query_string` | Sign `path + query_string` for GET; `path + body` for POST | `TestOkxGetSignature` (3) |
| 2 | Bybit V5 private GET signed with empty body | Sign `query_string.lstrip("?")` for GET; JSON body for POST | `TestBybitGetSignature` (3) |
| 3 | Private GET routed to `public_base_url` | Added `private: bool` parameter; position/order/probe use private base | `TestPrivateBaseUrl` (3) |
| 4 | Bitget profile detection silently fell back to CLASSIC on 401/429/network | Only fallback on explicit classic-mode error codes; propagate all other errors | `TestBitgetProfileDetectionFullFlow` (5) |
| 5 | Position side parsing only recognized `SHORT`; `Sell`/negative qty parsed as BUY | Handle SHORT/SELL/SHORT_SIDE/short/sell + negative quantity → SELL | `TestPositionSideParsing` (7) |
| 6 | Ack-only order responses returned fake fill with `request.quantity` | Ack-only → raise `UNCERTAIN`; only filled responses return `OrderFill` | `TestOrderAckNotFill` (4) + `TestAckOnlyOrderIntegration` (2) |
| 7 | Hyperliquid live order `not yet implemented` | Full EIP-712 signing (Option A): Agent struct, keccak256 connection_id, secp256k1 sign, asset index resolution. Verified against Rust test vectors. | `TestHyperliquidLiveOrderNowSupported` (2) + `TestHyperliquidCapabilityConsistency` (4) |
| 8 | Only happy-path fixture tests | Added failure-mode coverage: OKX/Bybit signature recomputation, Bitget error propagation, ack→UNCERTAIN, short parsing, Hyperliquid signing | 28+ new test cases across transport + contract suites |

### Hyperliquid Live Order — Explicit Status

**Implemented (Option A):** Real EIP-712 exchange action signing via `lightfee/venues/hyperliquid_signing.py`.

- Agent struct typed data with `source` + `connectionId`
- `connection_id = keccak256(msgpack(action) + nonce_be_bytes + vault_flag_byte + vault_address_bytes)`
- secp256k1 signing via `eth_account.Account.sign_message` with recovery id + 27
- Exchange payload: `{action, signature: {r, s, v}, nonce, vaultAddress?}`
- Asset index resolved via `POST /info {"type": "meta"}`
- Two Rust test vectors verified (mainnet sign match + cancel action hash match)

### Still Unsupported — Capability Boundaries

| Capability | Venues | Reason |
|---|---|---|
| `risk_health` | Bitget, Gate, Hyperliquid | No risk/health-check API contract defined; requires per-venue margin/leverage endpoint work |
| `reconcile_quality` | Bitget, Gate, Hyperliquid | Reconcile needs order-status polling or WebSocket stream confirmation; current codec returns UNCERTAIN for ack-only |
| `testnet_support` | Bitget, Gate, Aster, Hyperliquid | Testnet availability not verified; declared as UNKNOWN pending registration + sandbox key testing |

### Final Acceptance

```bash
$ pytest tests/test_venues_transport.py tests/test_venues_contract.py \
        tests/test_venues_base.py tests/test_runtime_smoke.py -v
432 passed

$ python3 -m compileall lightfee
Listing 'lightfee'...  # all modules compile cleanly

$ python3 -c "
from lightfee.venues.base import VenueCapabilities
from lightfee.core.domain import Venue
for v in Venue:
    caps = VenueCapabilities.for_venue(v)
    print(f'{v.value}: live_order={\"supported\" if caps.live_order_supported else \"unsupported\"}')
"

binance: live_order=supported
okx: live_order=supported
bybit: live_order=supported
bitget: live_order=supported
gate: live_order=supported
aster: live_order=supported
hyperliquid: live_order=supported
```

### Follow-on Specs

After this batch lands, the next specs should cover:

1. market data and sidecar source completion
2. live entry and exit orchestration
3. risk-triggered execution and recovery behavior
4. offline replay, evolution, and reporting

Those should remain separate docs so each one can produce a working, testable slice on its own.
