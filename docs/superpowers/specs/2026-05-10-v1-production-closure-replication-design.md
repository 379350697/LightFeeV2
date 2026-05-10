# V1 Production Closure Replication Design

**Goal:** Make LightFeeV2 a production-capable Python port of the verified Rust LightFee V1 live trading system, preserving business behavior while using the cleaner V2 module boundaries.

**Primary source of truth:** `/media/wl/新加卷/codex/LightFee`

**Target repository:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-10

---

## Executive Decision

LightFeeV2 is not a paper-trading prototype and not a strategy redesign. The migration target is a direct production full-closure port of the useful V1 live system:

```text
sidecar/live market data
  -> opportunity filtering
  -> entry planning
  -> synchronized two-leg execution
  -> open-position supervision
  -> exit and risk close execution
  -> reconciliation, recovery, persistence, observability
```

The Rust V1 implementation remains the only business-logic authority. Python code may improve structure, testability, typed boundaries, and async ergonomics, but it must not change thresholds, state transitions, retry classes, precision rules, funding-window logic, stale-data behavior, or risk-line semantics unless the deviation is explicitly documented and approved.

## Migration Rule

For each migrated function, class, or state transition, the implementation protocol is:

1. Identify the exact Rust source slice and live-path caller.
2. Write an alignment note covering core flow and high-risk details.
3. Run GitNexus impact analysis before editing Python symbols.
4. Add or update Python tests that lock the Rust-equivalent behavior.
5. Fill the Python skeleton using V2 architecture, keeping business behavior equivalent.
6. Run focused tests, then `gitnexus_detect_changes()` before any commit.

If the Python skeleton cannot represent required Rust state, the skeleton must be expanded. Hardcoded shortcuts are not allowed.

## Scope

In scope:

- live entry execution, including candidate selection, budgets, passive and taker routes, pending-entry lifecycle, fallback, partial fill handling, synchronized hedge execution, and residual protection
- live exit execution, including funding capture stages, trailing exit, hard stop, settlement close, risk death close, risk delever close, reduce-only order construction, close chunking, dust checks, PnL attribution, and close reconciliation
- risk execution closure, including warning pause, delever action, death-line protection, stale risk snapshot behavior, unsupported-risk fallback behavior, and journaled operator-visible transitions
- market-data closure, including WS L2, local book state, freshness gates, execution liquidity snapshots, REST fallback rules, local-L2 protect/suspend behavior, and cached true-L2 priority
- venue production details for the seven live venues: Binance, OKX, Bybit, Bitget, Gate, Aster, Hyperliquid
- private websocket/order update support where V1 uses it for certainty, reconciliation, or latency
- restart recovery, snapshot replay, journal replay, unknown order recovery, partial close recovery, residual exposure cleanup, and fail-closed behavior
- production observability: journal event parity, Prometheus/current-state exports, operator commands, and enough structured metadata to diagnose live incidents

Out of scope:

- LLM evolution loops, proposal approval systems, and strategy mutation workflows
- offline reports and research-only harnesses that are not required by the live trading path
- historical compatibility wrappers that V1 no longer uses in live execution
- broad V1 architectural cleanup inside the Rust repository
- changing the funding-arbitrage strategy, threshold philosophy, or risk appetite

Paper-mode or fake transport can remain as a test utility, but it is not a milestone or acceptance target for this effort.

## Current V2 Starting Point

V2 already has useful anchors:

- `lightfee/core/domain.py` defines venue, side, order, fill, market, position, balance, and risk snapshot domain types.
- `lightfee/core/contracts.py` defines the adapter contract expected by the runtime.
- `lightfee/venues/transport.py` and per-venue modules provide a shared REST adapter architecture.
- `lightfee/rate_limit/` has a reusable token-bucket and cooldown/backoff runtime.
- `lightfee/engine/runtime.py` has lifecycle, snapshot consumption, tick lanes, supervisor hook, backoff, and export scaffolding.
- `lightfee/engine/entry.py` has entry enums and basic order construction.
- `lightfee/engine/exit.py` has exit enums and basic reduce-only order construction.
- `lightfee/risk/` has risk-line detection but not execution closure.
- `lightfee/marketdata/` has L2/local-book data structures but not V1-grade live feed management.
- `lightfee/persistence/` has journal/snapshot/sqlite anchors.

The important gap is not naming or structure. The gap is that live execution is not yet closed.

## Production Architecture

### Runtime Orchestrator

`lightfee/engine/runtime.py` becomes the live trading orchestrator. It should remain small enough to understand, but it must call real execution services:

```text
LiveRuntime.start()
  -> prepare symbols
  -> recover snapshot and journal
  -> reconcile live venue state
  -> enter RUNNING only if state is safe

LiveRuntime.tick()
  -> load sidecar snapshot
  -> reject stale or unsupported snapshot according to V1 policy
  -> discover candidates
  -> apply risk budgets and runtime gates
  -> schedule entry execution
  -> persist resulting pending/open/failed state

LiveRuntime.tick_active_positions()
  -> refresh execution liquidity and risk views
  -> evaluate exit reasons
  -> execute close/delever actions
  -> reconcile fills and residuals

LiveRuntime._post_tick_housekeeping()
  -> risk supervisor action closure
  -> exports
  -> snapshot persistence
```

The runtime must not contain exchange-specific payloads or strategy math that belongs in dedicated services.

### Entry Closure

Entry must be ported from V1 live behavior, primarily:

- `src/engine/entry.rs`
- `src/engine/entry_sync.rs`
- `src/engine/market_data.rs`
- `src/engine/reliability_contract.rs`
- `src/engine/helpers.rs`
- `src/execution_core/` entry and residual helpers if used by the live path

Python target modules:

- `lightfee/engine/entry.py`
- `lightfee/engine/execution_planner.py`
- `lightfee/engine/entry_sync.py`
- `lightfee/engine/residual.py`
- `lightfee/engine/state.py`
- `lightfee/engine/runtime.py`
- `tests/test_entry_execution.py`
- `tests/test_entry_sync.py`

Required behavior:

- candidate-to-entry context construction keeps V1 symbol, venue, long/short, maker/hedge, and funding timing semantics
- entry budgets and max concurrent position gates are applied before order submission
- passive incremental route follows V1 clip/min-notional/hedge-chunk rules
- standard dual-taker route uses V1 ordering, retry, and uncertainty policy
- pending entry state records maker fill, hedge fill, client order ids, deadlines, fallback route, and residual exposure
- partial fills are never silently upgraded to complete positions
- hedge failure after maker fill triggers V1 residual protection, not a generic exception
- completed entry creates an `OpenPosition` with the same matched-quantity and price attribution semantics as V1

### Exit Closure

Exit must be ported from V1 live behavior, primarily:

- `src/engine/exit.rs`
- `src/engine/market_data.rs`
- `src/engine/risk.rs`
- `src/engine/state.rs`
- local-L2 and close-chunking tests referenced by GitNexus processes

Python target modules:

- `lightfee/engine/exit.py`
- `lightfee/engine/close_executor.py`
- `lightfee/engine/state.py`
- `lightfee/engine/runtime.py`
- `tests/test_exit_decisions.py`
- `tests/test_close_execution.py`

Required behavior:

- all existing `ExitReason` values remain live-actionable, not just enum labels
- funding capture stages match V1 timing and edge logic
- trailing drawdown uses V1 peak/net edge accounting
- mark-price hard stop uses V1 mark/price fallback policy
- settlement force close follows V1 deadline behavior
- death/delever exits are triggered by risk supervision, not manual close only
- reduce-only close orders are chunked, normalized, and dust-checked with V1 semantics
- close PnL includes price PnL, fees, and funding PnL attribution
- uncertain close outcomes enter pending-close reconciliation instead of being dropped

### Risk Closure

Risk execution must be ported from V1 live behavior, primarily:

- `src/risk.rs`
- `src/engine/risk.rs`
- `src/health.rs`
- `src/engine/supervision.rs`

Python target modules:

- `lightfee/risk/health.py`
- `lightfee/risk/budgets.py`
- `lightfee/risk/operator.py`
- `lightfee/engine/supervisor.py`
- `lightfee/engine/risk_actions.py`
- `tests/test_risk_actions.py`

Required behavior:

- warning line pauses new entries when enabled
- delever line triggers synchronized partial delever according to V1 sizing rules
- death line triggers protective close with V1 single-side or dual-side protection semantics
- unsupported or stale risk snapshots follow V1 configured behavior: fail closed, protect only, or suspend as applicable
- risk actions produce journal events before and after order execution
- risk state changes are reflected in `EngineState.risk_mode`

### Market Data And Local L2

Market data must be ported from V1 live behavior, primarily:

- `src/engine/market_data.rs`
- `src/ws.rs`
- `src/private_ws.rs`
- `src/resilience.rs`
- relevant `src/live/*.rs` websocket code

Python target modules:

- `lightfee/marketdata/l2.py`
- `lightfee/marketdata/local_book.py`
- `lightfee/marketdata/freshness.py`
- `lightfee/marketdata/liquidity.py`
- `lightfee/marketdata/ws.py`
- `lightfee/marketdata/private_ws.py`
- `lightfee/marketdata/resilience.py`
- `tests/test_marketdata_l2.py`
- `tests/test_ws_resilience.py`

Required behavior:

- true L2 data is preferred when V1 prefers it
- cached hot local-L2 data is consumed before REST snapshot fallback where V1 does so
- protect/suspend modes for missing local-L2 match V1
- stale books degrade, rebuild, suspend, or fall back according to V1 state machine
- private fills update pending entry/close reconciliation when V1 depends on private order stream certainty

### Venue Closure

The V2 shared transport is a good structural direction, but venue-specific production behavior must not be flattened away.

Rust references:

- `src/live/binance.rs`
- `src/live/okx.rs`
- `src/live/bybit.rs`
- `src/live/bitget.rs`
- `src/live/gate.rs`
- `src/live/aster.rs`
- `src/live/hyperliquid.rs`
- `src/market_gateway/`

Python target modules:

- `lightfee/venues/transport.py`
- `lightfee/venues/specs.py`
- `lightfee/venues/common.py`
- `lightfee/venues/binance.py`
- `lightfee/venues/okx.py`
- `lightfee/venues/bybit.py`
- `lightfee/venues/bitget.py`
- `lightfee/venues/gate.py`
- `lightfee/venues/aster.py`
- `lightfee/venues/hyperliquid.py`
- `tests/test_venues_transport.py`
- `tests/test_venues_contract.py`

Required behavior:

- per-venue signing and endpoint selection match V1
- leverage setup and position mode setup are explicit live preflight steps
- lot size, tick size, min quantity, min notional, contract size, and reduce-only exceptions match V1
- exchange-specific rejected-vs-uncertain error classification is preserved
- order id and client order id parsing are venue-specific where needed
- cancel/amend and reconciliation support are added where V1 uses them
- private websocket support is added for venues where V1 uses it for live certainty

### Persistence, Recovery, And Reconciliation

Rust references:

- `src/runtime_state/`
- `src/engine/recovery.rs`
- `src/engine/state.rs`
- `src/observability_ops/`

Python target modules:

- `lightfee/engine/recovery.py`
- `lightfee/engine/reconciliation.py`
- `lightfee/persistence/journal.py`
- `lightfee/persistence/snapshot_store.py`
- `lightfee/persistence/sqlite_store.py`
- `lightfee/persistence/ledgers.py`
- `tests/test_recovery_reconciliation.py`

Required behavior:

- startup never enters RUNNING with ambiguous open orders or residual exposure unless V1 would do so
- journal replay and snapshot recovery rebuild pending entries, pending closes, open positions, and risk mode
- unknown order outcomes are reconciled through venue adapters
- residual exposures are detected, journaled, and closed/protected according to V1
- persistence writes are atomic where V1 relies on atomicity

## Precision And Error Semantics

All price, quantity, notional, fee, and funding arithmetic that affects order size or decision thresholds must use `Decimal` or an equivalent exact quantization wrapper. Python `float` may remain for purely observational fields only after the value is already normalized and cannot alter a trading decision.

Mandatory precision rules:

- quantity is floored to step size, never rounded up into over-exposure
- price is quantized to tick size using the same side-aware behavior as V1
- min-notional checks use the same reference price and reduce-only exemptions as V1
- matched entry and close quantity uses V1 `min` semantics
- dust prevention follows V1 close-chunk rules
- funding PnL and fee attribution are not collapsed into price PnL

Mandatory error rules:

- `REJECTED` means no order-side uncertainty remains and the engine can fail the action deterministically
- `UNCERTAIN` means reconciliation is required before retrying or mutating state
- auth/config/capability failures fail closed before live trading
- transport timeouts during live order submission are uncertain unless V1 marks the specific case rejected
- partial fills produce pending state, not silent success

## Testing And Acceptance

Every production migration task must include tests at three levels:

- unit tests for pure math, state transition, precision, and classification behavior
- fake-adapter async tests for entry, exit, risk, and recovery flows
- fixture-based venue contract tests for request/response shaping and error mapping

Production full-closure acceptance requires:

- `pytest` passes for the full V2 suite
- `python -m compileall lightfee tests` passes
- no live-path required method raises `NotImplementedError`
- entry can open a fully reconciled two-leg position through fake live adapters
- exit can close both legs reduce-only and record full PnL attribution
- death/delever risk actions execute close/delever orders, not just logs
- restart recovery rebuilds pending/open state and reconciles unknown orders
- venue adapters expose explicit unsupported capabilities instead of silent fallback
- GitNexus change detection reports only expected symbols and flows before commit

## Non-Negotiable Review Checklist

Before accepting any migrated module:

- The Rust source file and function names are listed in the implementation note.
- All Rust thresholds and constants are either copied from config or deliberately mapped to an existing V2 config field.
- Every state transition has a test.
- Every `except` block classifies rejected vs uncertain outcomes.
- Every order quantity and close chunk is normalized with venue rules.
- Every live action writes a journal event with enough context to replay the decision.
- No module adds a strategy behavior that cannot be traced to V1 live code.

## Implementation Strategy

Use V2 architecture, but do not let architecture override verified behavior:

- Keep `venues/` as shared transport plus venue-specific profiles.
- Keep `engine/` as orchestration and execution services.
- Keep `risk/` as pure evaluation plus action decision helpers.
- Keep `marketdata/` as local book, freshness, and feed management.
- Keep `persistence/` as the only durable state boundary.

When V1 code is useful but structurally messy, port the behavior into smaller Python units. When a V1 detail is too subtle to safely optimize, copy the behavior first and optimize only after tests prove equivalence.
