# Seven Venue Adapters Implementation Plan

> **This is a historical plan.** The implementation is complete. The **Completion / Closure** section at the end of this document is the authoritative record of what was actually delivered, including the per-venue capability matrix, the 8 fixed deviations, final acceptance results (236 passed), and explicit unsupported capability boundaries. The task breakdown below is preserved as the original execution roadmap; if any detail conflicts, the Closure section takes precedence.

**Goal:** Replace the seven V2 venue stubs with real, shared, live-capable adapters that can fetch market snapshots, normalize quantities, fetch positions, and submit orders for Binance, OKX, Bybit, Bitget, Gate, Aster, and Hyperliquid.

**Architecture:** Build one shared venue transport layer, one per-venue spec layer, and seven thin adapter modules. Keep `lightfee/core/contracts.py` as the public adapter contract, keep `lightfee/venues/base.py` as capability metadata only, and keep venue-specific quirks inside the venue layer. Support both paper and live startup paths: paper mode should stay credential-light and deterministic, while live mode must fail fast on missing credentials or unsupported capability combinations.

**Tech Stack:** Python 3.12, asyncio, `httpx`, `pytest`, `pytest-asyncio`, dataclasses, `tomllib`, existing LightFee domain models.

---

## Reference Documents

- Design: `docs/superpowers/specs/2026-05-10-seven-venue-adapters-design.md`
- Rust reference for adapter behavior:
  - `src/live/binance.rs`
  - `src/live/okx.rs`
  - `src/live/bybit.rs`
  - `src/live/bitget.rs`
  - `src/live/gate.rs`
  - `src/live/aster.rs`
  - `src/live/hyperliquid.rs`
  - `src/market_gateway/ports.rs`
  - `src/market_gateway/capability_ports.rs`
- Current Python anchors:
  - `lightfee/core/contracts.py`
  - `lightfee/venues/base.py`
  - `lightfee/venues/common.py`
  - `lightfee/venues/registry.py`
  - `lightfee/apps/live.py`
  - `lightfee/apps/probe.py`
  - `lightfee/engine/runtime.py`
  - `tests/test_venues_base.py`
  - `tests/test_runtime_smoke.py`

## Shared Rules

- Do not spread exchange-specific logic into `engine/`, `strategy/`, or `risk/`.
- Do not add one-off transport code inside each venue module.
- Do not let required adapter methods keep `NotImplementedError`.
- Do not make CI depend on live exchange access.
- Keep adapter construction deterministic in paper mode.
- Keep all venue-specific signing, endpoint selection, and payload shaping behind one shared transport API.

## Execution Order

1. Task 1 must land first: shared transport, venue specs, and common sizing helpers.
2. Task 2 and Task 3 can run in parallel after Task 1:
   - Task 2 owns Binance, OKX, Bybit.
   - Task 3 owns Bitget, Gate, Aster, Hyperliquid.
3. Task 4 wires the adapter factory into runtime and probe entrypoints.
4. Task 5 adds the parameterized contract suite and fixture corpus.

## Ownership Split

- Worker A: `lightfee/venues/transport.py`, `lightfee/venues/specs.py`, `lightfee/venues/common.py`, `tests/test_venues_transport.py`
- Worker B: `lightfee/venues/binance.py`, `lightfee/venues/okx.py`, `lightfee/venues/bybit.py`, corresponding fixtures and contract cases
- Worker C: `lightfee/venues/bitget.py`, `lightfee/venues/gate.py`, `lightfee/venues/aster.py`, `lightfee/venues/hyperliquid.py`, corresponding fixtures and contract cases
- Worker D: `lightfee/venues/registry.py`, `lightfee/apps/live.py`, `lightfee/apps/probe.py`, `lightfee/engine/runtime.py`, smoke and integration tests

---

### Task 1: Shared Transport, Venue Specs, and Normalization

**Files:**
- Create: `lightfee/venues/transport.py`
- Create: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/common.py`
- Test: `tests/test_venues_transport.py`

**Goal of this task:** create one reusable venue transport core that handles paper/live mode selection, request signing, response decoding, error classification, and quantity normalization for every exchange.

**Step 1: Write the failing tests**

Write tests for these behaviors:

- paper mode can build transport objects without live credentials
- live mode fails fast when required credentials are missing
- signing differs correctly for Binance/OKX/Bybit/Bitget/Gate/Aster/Hyperliquid
- quantity normalization floors to the venue step and respects contract size
- reduce-only close sizing still honors the Binance and Aster min-notional exemption in `lightfee/venues/common.py`
- transport errors are translated into the shared LightFee error classes instead of leaking raw exchange exceptions

Use fixture-driven tests with `httpx.MockTransport` or equivalent local stubs. Do not reach a real exchange in this test file.

**Step 2: Implement the venue spec layer**

Create one `VenueSpec`-style definition per venue with the fields needed by the shared transport:

- venue id
- public market base URL
- private REST base URL
- auth scheme
- account mode
- symbol mapping rules
- quantity step / contract size / min quantity
- request path builders
- response parsers
- whether paper mode can simulate order placement locally

Keep the per-venue constants close to the transport layer so the adapter modules stay thin.

**Step 3: Implement the transport core**

Implement a shared async transport that owns:

- `fetch_market_snapshot`
- `fetch_position`
- `place_order`
- `normalize_quantity`
- `close`

The transport should:

- use `httpx.AsyncClient`
- support paper mode with deterministic dry-run behavior
- support live mode with proper auth headers and signing
- translate venue rejects into `OrderSubmitError` with `SubmitFailureClass.REJECTED`
- translate uncertain outcomes into `OrderSubmitError` with `SubmitFailureClass.UNCERTAIN`
- keep response parsing out of the venue modules

**Step 4: Extend the shared sizing helper**

Update `lightfee/venues/common.py` so the shared sizing math is explicit and reusable. Keep `venue_reduce_only_close_exempts_min_notional()` there, and add a helper for flooring quantity to step size before venue-specific contract conversion.

**Step 5: Run the transport tests**

Run:

```bash
pytest tests/test_venues_transport.py -v
python -m compileall lightfee
```

Expected:

- all transport tests pass
- the package compiles cleanly

**Step 6: Commit**

```bash
git add lightfee/venues/common.py lightfee/venues/specs.py lightfee/venues/transport.py tests/test_venues_transport.py
git commit -m "feat: add shared venue transport"
```

---

### Task 2: Binance, OKX, and Bybit Adapters

**Files:**
- Modify: `lightfee/venues/binance.py`
- Modify: `lightfee/venues/okx.py`
- Modify: `lightfee/venues/bybit.py`
- Modify: `tests/test_venues_base.py` if capability assertions need tightening
- Create: `tests/fixtures/venues/binance/*`
- Create: `tests/fixtures/venues/okx/*`
- Create: `tests/fixtures/venues/bybit/*`

**Goal of this task:** turn the three closest REST-style venues into real adapters on top of the shared transport, with the existing class names preserved: `BinanceAdapter`, `OkxAdapter`, `BybitAdapter`.

**Step 1: Write the failing contract tests**

Add parameterized cases that verify:

- `fetch_market_snapshot()` returns normalized quote data
- `fetch_position()` returns normalized long/short position data
- `place_order()` returns an `OrderFill` in paper mode and a live-shaped order result in live mode
- `normalize_quantity()` respects each venue's lot size and contract rules
- the adapter class imports still work with the current module names

Use one shared test matrix instead of three separate bespoke suites.

**Step 2: Implement Binance**

Binance must cover:

- USDM HMAC signing
- single or multi-asset mode detection
- reduce-only close exemption handling
- order and position request shaping

Keep the public surface the same, but route everything through the shared transport.

**Step 3: Implement OKX**

OKX must cover:

- passphrase-aware authentication
- unified account request shaping
- explicit instrument mapping
- live/private endpoint selection

**Step 4: Implement Bybit**

Bybit must cover:

- V5 signing
- category or account-mode mapping
- position normalization
- order request shaping

**Step 5: Verify the three-venue batch**

Run:

```bash
pytest tests/test_venues_base.py tests/test_venues_transport.py -v
pytest tests/test_venues_contract.py -v -k "binance or okx or bybit"
```

Expected:

- all three adapters pass the shared contract suite
- no required method in these modules uses `NotImplementedError`

**Step 6: Commit**

```bash
git add lightfee/venues/binance.py lightfee/venues/okx.py lightfee/venues/bybit.py tests/fixtures/venues/binance tests/fixtures/venues/okx tests/fixtures/venues/bybit tests/test_venues_contract.py
git commit -m "feat: implement binance okx bybit venue adapters"
```

---

### Task 3: Bitget, Gate, Aster, and Hyperliquid Adapters

**Files:**
- Modify: `lightfee/venues/bitget.py`
- Modify: `lightfee/venues/gate.py`
- Modify: `lightfee/venues/aster.py`
- Modify: `lightfee/venues/hyperliquid.py`
- Create: `tests/fixtures/venues/bitget/*`
- Create: `tests/fixtures/venues/gate/*`
- Create: `tests/fixtures/venues/aster/*`
- Create: `tests/fixtures/venues/hyperliquid/*`

**Goal of this task:** finish the four remaining venue adapters in one batch, with venue-specific quirks isolated in their own spec or codec branches.

**Step 1: Write the failing contract tests**

Add parameterized cases that verify:

- `fetch_market_snapshot()` returns normalized quotes
- `fetch_position()` returns normalized positions
- `place_order()` returns a normalized fill or rejection
- `normalize_quantity()` respects contract and step rules
- unsupported optional capabilities remain explicit in the capability registry

**Step 2: Implement Bitget**

Bitget must:

- detect classic vs UTA profile on first use
- cache the profile for later requests
- choose the correct private endpoint based on that profile
- preserve the existing venue naming and class export

**Step 3: Implement Gate**

Gate must:

- preserve decimal contract sizes
- handle dual-position mode correctly
- normalize sizes before submission

**Step 4: Implement Aster**

Aster must:

- use the separate balance and position surfaces explicitly
- fail closed if the required precheck data is missing
- keep the position normalization path deterministic

**Step 5: Implement Hyperliquid**

Hyperliquid must:

- split info and exchange API usage cleanly
- keep unsupported risk-health support explicit
- normalize order and position responses into the shared domain shapes

**Step 6: Verify the four-venue batch**

Run:

```bash
pytest tests/test_venues_contract.py -v -k "bitget or gate or aster or hyperliquid"
python -m compileall lightfee
```

Expected:

- all four adapters pass the shared contract suite
- package compilation still succeeds

**Step 7: Commit**

```bash
git add lightfee/venues/bitget.py lightfee/venues/gate.py lightfee/venues/aster.py lightfee/venues/hyperliquid.py tests/fixtures/venues/bitget tests/fixtures/venues/gate tests/fixtures/venues/aster tests/fixtures/venues/hyperliquid
git commit -m "feat: implement remaining venue adapters"
```

---

### Task 4: Registry, Live Runtime, and Probe Wiring

**Files:**
- Modify: `lightfee/venues/registry.py`
- Modify: `lightfee/apps/live.py`
- Modify: `lightfee/apps/probe.py`
- Modify: `lightfee/engine/runtime.py`

**Goal of this task:** make the new adapters buildable from config and visible to the live entrypoints, without pushing exchange-specific code into the engine.

**Step 1: Write the failing integration tests**

Add tests for:

- building all configured venue adapters from `config/example.toml`
- building all configured venue adapters from `config/live.example.toml`
- `lightfee-probe --list-capabilities` still works
- `lightfee-probe` can instantiate adapters in dry-run mode without live secrets
- `LiveRuntime` receives the adapter map at startup

**Step 2: Extend the registry**

Add a factory in `lightfee/venues/registry.py` that can:

- build one adapter from one `VenueConfig`
- build the full adapter map from `AppConfig`
- preserve the existing venue list and capability lookups

Keep the registry as the top-level entry for venue discovery, but do not move transport logic into it.

**Step 3: Wire the live process**

Update `lightfee/apps/live.py` so startup:

- loads config
- builds the adapter map
- fails fast if any configured venue cannot be constructed
- passes the adapter map into `LiveRuntime`

Update `lightfee/engine/runtime.py` so it stores the adapter map and exposes a simple lookup path for later entry/exit work. Do not add entry or exit orchestration here yet.

**Step 4: Wire the probe**

Update `lightfee/apps/probe.py` so:

- `--list-capabilities` still uses the registry capability data
- the default path can instantiate all configured adapters in dry-run mode
- `--execute` is reserved for later live smoke behavior, not for random ad hoc logic

**Step 5: Verify the wiring**

Run:

```bash
pytest tests/test_runtime_smoke.py -v
pytest tests/test_venues_contract.py tests/test_venues_transport.py -v
lightfee-probe --config config/example.toml --list-capabilities
```

Expected:

- runtime smoke still passes
- adapter construction is visible through the live and probe entrypoints

**Step 6: Commit**

```bash
git add lightfee/venues/registry.py lightfee/apps/live.py lightfee/apps/probe.py lightfee/engine/runtime.py tests/test_runtime_smoke.py
git commit -m "feat: wire venue adapters into runtime"
```

---

### Task 5: Fixture Corpus and Final Contract Sweep

**Files:**
- Create or expand: `tests/fixtures/venues/*`
- Create: `tests/test_venues_contract.py`
- Create: `tests/test_venues_live_smoke.py` if a guarded live-only harness is useful

**Goal of this task:** make the new adapter layer easy for other workers to trust by giving them one contract suite, one fixture layout, and one final validation pass.

**Step 1: Standardize the fixture layout**

Use the same shape for every venue directory:

- `market_snapshot.json`
- `position_snapshot.json`
- `place_order_success.json`
- `place_order_reject.json`
- `instrument_meta.json`

Keep payloads as small as possible, but keep enough detail to verify endpoint shape, symbol mapping, and normalization.

**Step 2: Finish the parameterized contract suite**

The contract suite must assert, for every venue:

- adapter class imports still resolve
- `VenueAdapter` methods exist on the public class
- market snapshot, position, and order paths return normalized domain objects
- paper mode stays deterministic
- live mode rejects missing credentials early
- no required method raises `NotImplementedError`

**Step 3: Add a guarded live smoke file if needed**

If a live-only smoke harness is useful, make it opt-in through an explicit environment variable so CI never depends on live exchanges.

**Step 4: Run the final sweep**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_venues_base.py tests/test_runtime_smoke.py -v
python -m compileall lightfee
```

Expected:

- all tests pass
- the package compiles cleanly
- the adapter layer is ready for the next plan that wires market data, entry, exit, and risk execution

**Step 5: Commit**

```bash
git add tests/fixtures/venues tests/test_venues_contract.py tests/test_venues_live_smoke.py
git commit -m "test: add venue adapter contract coverage"
```

---

## Done Criteria

This plan is complete when:

- all seven venue modules are real adapters, not stubs
- shared transport and sizing code exist once, not seven times
- paper mode can build adapters without secrets
- live mode fails fast on missing or incompatible credentials
- runtime and probe can build the adapter map from config
- one parameterized test suite covers all seven venues
- later work can focus on market data, entry, exit, and risk without revisiting the adapter plumbing

---

## Completion / Closure

**Status:** 完整闭环 — All 7 venues have live-capable adapters; all 8 closure deviations fixed; 236 tests passing.

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

| # | Deviation | Fix | Test Coverage |
|---|---|---|---|
| 1 | OKX private GET signature missing `?query_string` | Sign `path + query_string` for GET/DELETE; `path + body` for POST | `TestOkxGetSignature` (3 tests: query, no-query, POST) |
| 2 | Bybit V5 private GET signed with empty body | Sign `query_string.lstrip("?")` for GET; JSON body for POST | `TestBybitGetSignature` (3 tests: query, no-query, POST) |
| 3 | Private GET routed to `public_base_url` | Added `private: bool` parameter to `_request()`; position/order/Bitget probe use private base | `TestPrivateBaseUrl` (3 tests) |
| 4 | Bitget profile detection silently fell back to CLASSIC on 401/429/network errors | Fallback only on explicit classic-mode error codes; 401/429/network propagate immediately via `TransportError` | `TestBitgetProfileDetectionFullFlow` (5 tests: classic fallback, 401, 429, network error, caching) |
| 5 | Position side parsing: only `SHORT` recognized; `Sell`/negative quantity → BUY | Handle SHORT/SELL/SHORT_SIDE/short/sell + negative quantity → SELL with absolute value | `TestPositionSideParsing` (7 tests across all venues) |
| 6 | Ack-only order responses returned fake fill with `request.quantity` | Ack-only → raise `OrderSubmitError(UNCERTAIN)`; only filled payloads return `OrderFill` | `TestOrderAckNotFill` (4 tests) + `TestAckOnlyOrderIntegration` (2 tests) |
| 7 | Hyperliquid live order: `not yet implemented` | Full EIP-712 signing via `hyperliquid_signing.py`: Agent struct, keccak256 connection_id, secp256k1 sign, asset index resolution. Verified against 2 Rust test vectors. | `TestHyperliquidLiveOrderNowSupported` (2) + `TestHyperliquidCapabilityConsistency` (4) |
| 8 | Only happy-path fixture tests existed | Added failure-mode coverage: signature recomputation, error propagation, ack→UNCERTAIN, short parsing, Hyperliquid signing integrity | 28+ new cases across transport + contract suites |

### Hyperliquid Live Order — Explicit Status

**Implemented (Option A).** Module: `lightfee/venues/hyperliquid_signing.py`.

- EIP-712 typed data: `Agent` struct with `source` (string `"a"`) + `connectionId` (bytes32)
- `connection_id = keccak256(msgpack(action_bytes) + nonce_be_bytes + vault_flag_byte + vault_address_bytes)`
- Signing: `eth_account.Account.sign_message(typed_data)` → r, s, v (recovery_id + 27)
- Exchange payload shape: `{action: {...}, signature: {r, s, v}, nonce: <int>, vaultAddress: <hex>?}`
- Asset index: resolved via `POST /info {"type": "meta"}` → `universe[]` position; cached per symbol
- Rust cross-validation: both mainnet sign and cancel-action hash test vectors pass

### Still Unsupported — Capability Boundaries

| Capability | Venues | Rationale |
|---|---|---|
| `risk_health` | Bitget, Gate, Hyperliquid | No risk/health-check API contract per venue; requires margin/leverage endpoint design |
| `reconcile_quality` | Bitget, Gate, Hyperliquid | Reconcile requires order-status polling or WebSocket stream; current transport returns UNCERTAIN for ack-only |
| `testnet_support` | Bitget, Gate, Aster, Hyperliquid | Registration + sandbox key provisioning not done; declared UNKNOWN pending verification |

### Final Acceptance Commands & Results

```bash
# Full test suite
$ pytest tests/test_venues_transport.py tests/test_venues_contract.py \
      tests/test_venues_base.py tests/test_runtime_smoke.py -v
236 passed

# Targeted deviation tests
$ pytest tests/test_venues_transport.py -k "okx or bybit or private or short or ack" -v
# All targeted cases pass (OKX signature, Bybit signature, private base URL,
# position side parsing, ack-not-fill)

$ pytest tests/test_venues_contract.py -k "order or profile or hyperliquid" -v
# All targeted cases pass (parameterized order success/reject, Bitget profile
# detection full flow, Hyperliquid capability consistency + live order signing)

# Compile check
$ python3 -m compileall lightfee
Listing 'lightfee'...  # clean compile, zero errors

# Capability probe
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
