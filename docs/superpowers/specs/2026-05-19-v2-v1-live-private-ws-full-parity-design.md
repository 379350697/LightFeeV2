# LightFee V2 / V1 Live Private WS Full Parity Design

Date: 2026-05-19

## Purpose

This spec defines the required V2 implementation shape for fully restoring V1 live private
WebSocket semantics inside the existing V2 architecture. It covers live private WS connection
health, realtime fill/order progress pushes, passive order progress, position pushes, runtime
startup/shutdown, and tests.

This is not a claim that the gap is already closed. The current V2 code has private health
interfaces and cache hooks, but the real live private WS production path is not yet wired into
`VenueTransport.record_private_ws_success()` / `record_private_ws_failure()` for every venue.

## Required Reading

Read these before changing code:

- V2 audit: `docs/parity/2026-05-18-v1-v2-full-semantic-parity-audit.md`
- V1 private WS state: `../LightFee/src/live/private_ws.rs`
- V1 venue private WS implementations:
  - `../LightFee/src/live/binance.rs`
  - `../LightFee/src/live/aster.rs`
  - `../LightFee/src/live/okx.rs`
  - `../LightFee/src/live/bybit.rs`
  - `../LightFee/src/live/bitget.rs`
  - `../LightFee/src/live/gate.rs`
  - `../LightFee/src/live/hyperliquid.rs`
- V2 surfaces to extend:
  - `lightfee/marketdata/private_ws.py`
  - `lightfee/marketdata/ws.py`
  - `lightfee/venues/transport.py`
  - `lightfee/core/contracts.py`
  - `lightfee/engine/runtime.py`
  - `lightfee/engine/supervisor.py`
  - `lightfee/engine/entry_sync.py`
  - `lightfee/engine/passive_close.py`

## Current Gap

V2 already exposes:

- `VenueTransport.cached_private_connection_health()`
- `VenueTransport.record_private_ws_success()`
- `VenueTransport.record_private_ws_failure()`
- `VenueTransport.cached_position()`
- adapter-level `supports_private_health`

But the production live private WS path is not complete because:

- `lightfee/marketdata/private_ws.py` is only a small event/client-state skeleton.
- Real per-venue private WS workers are not started from live runtime.
- Real WS connect/login/subscribe/message/ping/pong/close/error paths do not call
  `VenueTransport.record_private_ws_success()` or `record_private_ws_failure()`.
- Private position pushes do not authoritatively populate the cache used by supervisor private
  confirmation.
- Passive order progress still depends on REST query paths rather than private-push-first V1
  semantics.

Therefore C-R2 must remain open until a real production worker path proves these calls and caches
are updated by live private WS events.

## Non-Negotiable Parity Rules

1. Follow V2 architecture, but do not let architecture change V1 behavior.
2. Do not force venue-specific private WS protocols into one generic implementation.
3. Shared code is allowed only for invariant state and plumbing:
   `PrivateWsState`, order/position caches, health counters, worker bookkeeping, backoff helpers,
   event queue/waker primitives, and common tests.
4. Each venue must keep its own private WS worker, auth/login/subscribe logic, heartbeat behavior,
   message parser, reconnect decisions, rate-limit pacing key, and venue-specific edge cases.
5. No mock-only closure. Tests may use fake websocket transports, but they must exercise the same
   production worker methods that live mode uses.
6. Do not mark the audit gap closed until production code, not tests, calls
   `record_private_ws_success()` / `record_private_ws_failure()` from real private WS lifecycle
   paths.

## V1 Semantics To Replicate

### Shared Private State

V1 `WsPrivateState` provides:

- order cache keyed by both exchange order id and client order id
- newest-update-wins ordering by `updated_at_ms`
- bounded order cache trimming
- `order_by_client_id()`
- `order_by_order_id()`
- `order_progress_if_fresh()`
- `lookup_or_wait_private_order()`
- `lookup_or_wait_private_order_progress()`
- `lookup_or_wait_private_order_progress_after()`
- position cache with `position_if_fresh()` and `positions_if_fresh()`
- connection health via `record_connection_success()`, `record_connection_failure()`,
  `connection_health()`
- worker lifecycle via `push_worker()`, `abort_workers()`, `worker_count()`
- fill enrichment via `enrich_fill_from_private()`

V2 should implement the equivalent in `lightfee/marketdata/private_ws.py`, using Python async
primitives where V1 uses Rust watch channels.

### Private Connection Health

For every live venue, success/failure must come from real private WS lifecycle events:

- success on connected/authenticated/subscribed as applicable
- success on valid private message, ping, pong, or other V1-equivalent liveness signal
- failure on listenKey start/keepalive/close failure where applicable
- failure on auth/sign/login/subscribe send failure
- failure on receive error, close frame, stream end, explicit private WS error, idle watchdog
- failure on decode/process errors that V1 counts as connection-relevant

These must flow through `VenueTransport.record_private_ws_success(now_ms)` and
`VenueTransport.record_private_ws_failure(now_ms, error, unhealthy_after)` so supervisor observes
the actual stream state.

### Realtime Order Fill And Passive Progress

Private order updates must populate the shared private state with:

- venue
- symbol
- side when known
- exchange order id
- client order id
- cumulative filled quantity
- average price
- fee quote
- passive order state
- update timestamp
- raw/sanitized source metadata if useful for diagnostics

Consumers must use private state before REST where V1 does:

- order submission reconciliation should enrich fill information from private order updates
- pending entry hedge recovery must be able to observe late private fills
- passive close maker progress must see private updates without waiting for a REST polling cycle
- `query_passive_order_progress()` must be private-push-first and REST fallback second
- "after observed timestamp" progress waits must be supported for amend/cancel/replace flows

### Position Pushes

Private position updates must update the same cache used by:

- `VenueTransport.cached_position(symbol)`
- `VenueAdapter.cached_position(symbol)`
- `RiskSupervisor._venue_private_position_confirmed()`
- active-position drift checks where fresh private cache is acceptable under V1 semantics

The TTL must respect `runtime.private_position_max_age_ms` for supervisor confirmation, not only a
hard-coded transport TTL. REST `fetch_position()` can still refresh cache, but private WS pushes
must be first-class position evidence.

## Venue-Specific Requirements

The following are intentionally venue-specific. Do not replace them with one common auth or
subscribe template.

### Binance

Replicate V1 Binance private WS:

- REST start listenKey before websocket connect
- keep listenKey alive on the V1 cadence
- close listenKey on shutdown/reconnect cleanup where V1 does
- private URL is listenKey-based
- rate-limit pacing uses Binance private WS connect action/group
- message parser handles Binance user-data events and records order/position updates
- ping requires pong response; close/receive/connect/listenKey failures record failure

### Aster

Replicate V1 Aster private WS separately from Binance even if parts resemble Binance:

- Aster listenKey start/keepalive/close endpoints and URL mapping
- Aster private message parser and field names
- Aster rate-limit pacing keys
- Aster-specific order/position conversion rules

### OKX

Replicate V1 OKX private WS:

- fetch server timestamp for signing
- login payload with API key, passphrase, timestamp, signature
- subscribe only after login ack behavior matches V1
- order and position channels are built per V1 `build_okx_private_subscribe_messages()`
- ping text heartbeat and idle watchdog are preserved
- server-time/sign/login/subscribe/idle/receive/close failures record failure
- message/ping/pong liveness records success as in V1
- OKX ctVal/contract conversion is preserved when converting position/order sizes

### Bybit

Replicate V1 Bybit private WS:

- private URL resolution from V1
- auth signature uses the V1 expires payload
- post-auth subscribe flow is not merged with OKX/Bitget
- topic set and position-mode handling follow V1
- JSON ping payload behavior is preserved
- Bybit parser preserves order, execution, and position update semantics

### Bitget

Replicate V1 Bitget private WS:

- private URL and login payload follow V1
- timestamp/sign/passphrase behavior follows V1
- subscription is triggered by V1 Bitget login ack handling
- text ping behavior is preserved
- Bitget order/position parser handles Bitget status and field names exactly as V1

### Gate

Replicate V1 Gate private WS:

- futures order and futures position subscriptions are separate signed messages
- each subscription uses the V1 channel/event/auth payload
- contract multiplier mapping is applied in position/order conversion
- text ping behavior is preserved
- Gate parser handles `futures.orders` and `futures.positions` independently

### Hyperliquid

Replicate V1 Hyperliquid private WS:

- hydrate private position state from the info API before subscribing
- subscribe to both user events and order updates
- use Hyperliquid account address semantics
- private ping/pong/no-data/error handling follows V1
- `NoData` and explicit WS errors feed failure/rebuild behavior
- fill cache and Hyperliquid-specific order update conversion follow V1

## V2 Design

### Private State Module

Extend `lightfee/marketdata/private_ws.py` with V1-equivalent state objects:

- `PrivateOrderUpdate`
- `PrivatePositionUpdate`
- `PrivateWsState`
- `PrivateWsWorkerHandle` or equivalent task tracking if useful
- conversion helpers to `PassiveOrderProgress` and `OrderFill`
- async wait helpers for order updates

This module is shared because it is state plumbing, not venue protocol logic.

### Transport Ownership

`VenueTransport` should own one private state instance per adapter/venue. Existing methods should
delegate to that state:

- `cached_private_connection_health()`
- `record_private_ws_success()`
- `record_private_ws_failure()`
- `cached_position()`
- `query_passive_order_progress()`
- private fill/progress lookup helpers
- shutdown worker cleanup

Add explicit transport methods for:

- `start_private_ws(symbols: list[str])`
- `stop_private_ws()`
- `private_ws_worker_count()`
- venue-specific worker methods such as `_start_binance_private_ws(...)`,
  `_start_okx_private_ws(...)`, etc.

The dispatch method may select by venue, but each selected implementation must stay venue-specific.

### Runtime Integration

Live runtime should start private WS after credentials/adapters/symbols are known and before
supervisor relies on private health as a live signal. Startup should journal success/failure
visibility without leaking secrets.

Shutdown must abort private WS workers before or during adapter shutdown, matching the current V2
adapter shutdown path and V1 worker abort semantics.

Tracked symbol updates must be handled. If the runtime changes active/private-tracked symbols, the
venue worker must be reconfigured using V1-equivalent worker replacement semantics rather than
leaving stale subscriptions.

### Execution Integration

Order/private events should reach execution through the private state and, where needed, an async
event queue/waker:

- Entry paths use private updates to enrich fills and resolve uncertain maker outcomes.
- Pending entry recovery can wait briefly for private fills before REST fallback.
- Passive close maker progress consults private progress first.
- Position pushes update supervisor-visible cache.
- Journal emits private WS connection/order/position diagnostics using sanitized payloads.

Do not let a generic event bus hide venue-specific parser decisions. The parser remains per venue;
the normalized result may enter shared V2 state.

## Tests And Acceptance Criteria

The implementation is accepted only when these pass:

1. `PrivateWsState` unit tests:
   - newest update wins
   - stale updates are ignored
   - order id and client id indexes cohere
   - mismatched order id/client id returns no progress
   - bounded cache evicts oldest entries and indexes
   - `lookup_or_wait_*` captures late updates
   - position freshness honors configured TTL
   - worker count prunes completed tasks and aborts active tasks
2. Per-venue parser fixture tests:
   - order ack/open/resting
   - partial fill
   - full fill
   - cancel/expired/reject
   - position update
   - unknown/benign message ignored without false failure
3. Per-venue worker lifecycle tests with fake websocket/session:
   - successful connect/login/subscribe/message calls `record_private_ws_success()`
   - connect/auth/sign/login/subscribe/send/receive/close/idle failures call
     `record_private_ws_failure()`
   - reconnect backoff is scheduled after counted failures
   - shutdown aborts worker and closes listenKey where applicable
   - venue-specific rate-limit pacing keys are called
4. Supervisor integration tests:
   - live adapter with private health support fails closed/reduce-only when real worker records
     repeated private WS failures
   - no cached position with healthy private connection blocks private position confirmation
   - fresh position push makes confirmation true
   - stale position push does not confirm
5. Passive progress tests:
   - private update is returned before REST query
   - stale private update falls back to REST
   - `after_updated_at_ms` waits for a newer private update
   - private/REST conflict is resolved conservatively and journaled
6. Entry/close integration tests:
   - realtime private maker fill advances pending entry or passive close without waiting for REST
   - late private fill resolves uncertain order before residual repair escalates
   - private event records include client order id and exchange order id
7. Full suite:
   - run the relevant focused tests first
   - run `pytest`
   - run GitNexus `detect_changes` before any commit
   - update the parity audit only after the production path and tests prove closure

## Implementation Order

1. Build V1-equivalent private state in `lightfee/marketdata/private_ws.py`.
2. Bridge `VenueTransport` to own that state and expose private lookup/start/stop methods.
3. Implement venue-specific parsers and fixture tests one venue at a time.
4. Implement venue-specific workers one venue at a time. Keep auth/subscribe/heartbeat/reconnect
   behavior close to the V1 source.
5. Wire live runtime startup, symbol reconfiguration, and shutdown.
6. Wire execution consumers: fill enrichment, pending entry recovery, passive progress, position
   confirmation.
7. Add integration tests and update the parity audit status only after real production calls exist.

## Explicit Anti-Drift Checks

Before claiming closure, prove all of the following:

- `rg "record_private_ws_success|record_private_ws_failure" lightfee` shows production worker
  callers, not only tests or method definitions.
- `rg "start_private_ws|private_ws_worker" lightfee` shows live runtime/adapter startup and
  shutdown paths.
- Each venue has a venue-named parser and worker path.
- Tests can fail if any venue worker stops calling transport health on real connect/error paths.
- The audit report does not say C-R2 is closed unless the above evidence exists.

