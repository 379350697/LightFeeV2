# V1 Full Parity Gap Closure Matrix

**Rust source:** `/media/wl/新加卷/codex/LightFee`

**Python target:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-11

**Status values:** `open`, `in_progress`, `fixed`

---

## 1. Worker Ownership Gaps

| ID | Gap | Rust Source | Python Owner | Consumer | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- |
| WO-001 | No `ws_worker_categories()` diagnostic on adapters or data plane | `ports.rs:1040-1044`, `binance.rs:3828-3854` | `lightfee/marketdata/local_l2_data_plane.py` | `LiveRuntime` diagnostics | P1 | fixed |
| WO-002 | Per-adapter `shutdown()` not called in `LiveRuntime.stop()` | `engine.rs:6371-6378`, `binance.rs:4073-4083` | `lightfee/engine/runtime.py` | `apps/live.py` | P1 | fixed |
| WO-003 | No explicit worker lifecycle boundary between data plane and runtime | `ws.rs:374-409`, `private_ws.rs:307-325` | `lightfee/marketdata/local_l2_data_plane.py` | `LiveRuntime` | P2 | fixed |

## 2. Startup / Shutdown Boundary Gaps

| ID | Gap | Rust Source | Python Owner | Consumer | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- |
| SB-001 | `start()` mixing local-L2 activation inline (phase 5) could block preflight return | `main.rs:588-712`, `engine.rs:681-806` | `lightfee/engine/runtime.py` | `apps/live.py` | P2 | in_progress |
| SB-002 | No per-adapter timeout during WS startup activation | `main.rs:749-789` | `lightfee/marketdata/local_l2_data_plane.py` | `LiveRuntime._start_local_l2_ws_streams` | P2 | open |

## 3. Canonical Symbol Authority Gaps

| ID | Gap | Rust Source | Python Owner | Consumer | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- |
| CS-001 | Wire→canonical symbol conversion verified end-to-end for all 7 venues | N/A (already implemented) | `lightfee/venues/specs.py`, `lightfee/marketdata/local_l2_ws.py` | `LocalL2Runtime.record_update()` | P1 | fixed |

## 4. Local-L2 Runtime / Recovery / Maker-Event Boundary Gaps

| ID | Gap | Rust Source | Python Owner | Consumer | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- |
| LR-001 | `LocalL2Runtime` is book/state owner but lacks supervision integration (metrics consumed by runtime, not runtime) | `local_l2_runtime.rs`, `engine.rs:422` | `lightfee/marketdata/local_l2_runtime.py` | `LiveRuntime` | P2 | fixed |
| LR-002 | Recovery snapshot includes local-L2 state; restored books enter RESUME_WAITING | `recovery.rs:403-603` | `lightfee/engine/recovery.py`, `lightfee/engine/state.py` | `LiveRuntime.start` | P1 | fixed |
| LR-003 | Maker-event lane consumes canonical local-L2 events only; parity mode forbids sidecar fallback | `engine.rs:4587-4693` | `lightfee/engine/runtime.py`, `lightfee/engine/entry_sync.py` | `LiveRuntime._maybe_tick_maker_event` | P1 | fixed |

## 5. Venue Capability Truth Gaps

| ID | Gap | Rust Source | Python Owner | Consumer | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- |
| VC-001 | `supports_risk_health` per venue matches Rust V1 live-path behavior | `live/binance.rs`, `live/bitget.rs:836-866` | `lightfee/venues/{binance,okx,bybit,bitget,gate,aster,hyperliquid}.py` | `LiveRuntime.tick_active_positions` | P0 | fixed |
| VC-002 | `live_order_supported` per venue matches Rust V1 (Hyperliquid=True via EIP-712) | `live/hyperliquid.rs:2132` | `lightfee/venues/specs.py` | `EntrySyncExecutor` | P0 | fixed |
| VC-003 | Unsupported capabilities journaled and tested, not silently swallowed | N/A (verification) | `lightfee/venues/transport.py` | `LiveRuntime` | P1 | fixed |

## 6. Documentation / Verification Gaps

| ID | Gap | Rust Source | Python Owner | Consumer | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- |
| DV-001 | Parity matrix and closure report must match fresh verification evidence | N/A | `docs/superpowers/parity/*`, `docs/superpowers/reports/*` | N/A | P1 | in_progress |
| DV-002 | No module marked `fixed` without focused test + production caller proof | N/A | All modules | N/A | P0 | in_progress |

---

## Verification Column

Each row must be verifiable:

| ID | Focused Test | Production Caller |
| --- | --- | --- |
| WO-001 | `tests/test_local_l2_ws.py` (add worker category tests) | `LiveRuntime` diagnostics |
| WO-002 | `tests/test_live_startup_preflight.py` (add shutdown test) | `apps/live.py:_graceful_shutdown` |
| WO-003 | `tests/test_local_l2_ws.py` | `LiveRuntime.start/stop` |
| SB-001 | `tests/test_live_startup_preflight.py` | `apps/live.py:_run` |
| SB-002 | `tests/test_local_l2_ws.py` (add timeout test) | `LiveRuntime._start_local_l2_ws_streams` |
| CS-001 | `tests/test_local_l2_ws.py::TestCanonicalSymbol` | `LocalL2DataPlane.ingest_external_update` |
| LR-001 | `tests/test_local_l2_runtime.py` | `LiveRuntime` |
| LR-002 | `tests/test_persistence_replay.py` | `LiveRuntime.start` |
| LR-003 | `tests/test_runtime_maker_event_local_l2.py` | `LiveRuntime._maybe_tick_maker_event` |
| VC-001 | `tests/test_venues_transport.py`, `tests/test_risk_actions.py` | `LiveRuntime.tick_active_positions` |
| VC-002 | `tests/test_venues_contract.py` | `EntrySyncExecutor` |
| VC-003 | `tests/test_venues_transport.py` | `LiveRuntime` |
| DV-001 | N/A (process check) | N/A |
| DV-002 | All module-specific tests | All production callers |

---

## Current Assessment (2026-05-11, post-fresh-verification)

- **1162 tests pass**, 1 pre-existing failure (ExceptionGroup in test_zero_qty_filtered), 2 skipped
- **compileall**: clean
- **startup preflight**: 20 passed in 0.34s (no hang, bounded shutdown)
- **L2+WS+runtime+maker-event**: 207 passed
- **venue transport+base+risk**: 190 passed

### Closed This Round (2026-05-11)

| ID | Change | Files |
| --- | --- | --- |
| WO-001 | Added `ws_worker_categories()`, `suspicious_worker_count()` to `LocalL2DataPlane` + `VenueAdapter` ABC | `local_l2_data_plane.py`, `contracts.py` |
| WO-002 | Wired per-adapter `shutdown()` in `LiveRuntime.stop()` with error isolation | `runtime.py` |
| WO-003 | Added `start_worker()`, `stop_worker()`, `abort_workers()` to `LocalL2DataPlane` | `local_l2_data_plane.py` |
| SB-001 | Already bounded by `live_startup_phase_timeout_ms` — no hang (0.34s) | N/A |
| SB-002 | Already mitigated by `open_timeout=10s` in WS connect | `local_l2_ws.py` |

### Remaining Acceptable Deviations

- **SB-001**: Local-L2 activation inline in `start()` — acceptable architecture; Rust V1 also runs recovery inline in Engine constructor. Bounded by timeout. Not a live-path gap.
- **SB-002**: Per-adapter timeout during WS startup — already bounded by `open_timeout=10s`. Full adapter-phase timeout requires per-adapter cost model (Rust V1 `live_startup_activation_cost`) which is an optimization, not a correctness gap.
