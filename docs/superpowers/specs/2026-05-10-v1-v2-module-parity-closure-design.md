# V1/V2 Module Parity Closure Design

**Goal:** Close remaining Rust V1 to Python V2 business-logic drift by assigning every gap, including small default-value and edge-case drift, to the correct LightFeeV2 module boundary.

**Primary source of truth:** `/media/wl/新加卷/codex/LightFee`

**Target repository:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-10

---

## Decision

LightFeeV2 must keep its cleaner Python architecture, but every live-path behavior must trace back to Rust V1. Remaining gaps must not be tracked as a loose global list. Each gap belongs to exactly one owning module, with downstream integration points listed separately.

Rust V1 has accumulated many production patches across local L2, entry, exit, risk, venue adapters, recovery, rate limits, and observability. Those patches are not noise if they sit on the live path. In Python, the implementation should be cleaner than Rust where possible, but the production semantics behind those patches must be preserved.

The migration rule for every module is:

```text
Rust live behavior: 1:1 semantic parity
Python implementation: optimized structure, clearer ownership, smaller helpers
Strategy/risk/precision/retry semantics: no unapproved changes
```

This spec extends the production-closure spec:

- `docs/superpowers/specs/2026-05-10-v1-production-closure-replication-design.md`
- `docs/superpowers/prompts/2026-05-10-v1-production-closure-replication-execution-prompt.md`

When documents conflict, the stricter interpretation closer to Rust V1 live behavior wins.

## Scope

In scope:

- Full live-path parity review for runtime, entry, exit, risk, market data, venues, recovery, persistence, state, rate limiting, and observability.
- Small drift items: constants, defaults, enum strings, reason strings, journal event names, JSON field names, retry/backoff timing, rounding direction, epsilon comparisons, missing awaits, and test-only compatibility paths.
- Module ownership for each drift point, so fixes land in the correct V2 boundary instead of accumulating in `runtime.py`.
- Code quality and structure improvements in Python, provided every Rust live-path patch behavior remains represented by a module-owned test.

Out of scope:

- Rust non-live research/report/evolution paths.
- Rewriting V2 architecture to mimic Rust file layout.
- Mechanical line-by-line Rust reproduction when a smaller Python design can express the same live behavior more clearly.
- Changing strategy thresholds, risk appetite, funding timing, or venue-specific policy without Rust evidence and explicit approval.

## Semantic Parity, Structural Optimization

Python should not cargo-cult Rust's accumulated implementation shape. Large Rust functions, `Arc`/`Mutex` plumbing, channel choreography, duplicated match branches, and historical layering can be replaced with Pythonic services, pure helpers, dataclasses, async tasks, and clear module boundaries.

However, a Python simplification is valid only when it preserves the Rust live behavior under test. For every Rust patch or special case found on the live path, the worker must do one of the following:

- Port the behavior into the owning Python module and add a focused test.
- Replace several Rust branches with a cleaner Python abstraction and add tests that cover each original Rust branch.
- Mark the row `approved_deviation` with explicit approval and a reason.
- Mark the row `deferred_non_live` only when the Rust code is proven outside live trading.

The following are forbidden:

- Removing a Rust defensive branch because it looks like a historical patch.
- Collapsing `REJECTED`, `UNCERTAIN`, retryable, and terminal-success outcomes into one generic error path.
- Replacing venue-specific behavior with a generic helper unless fixture tests prove equivalent behavior per venue.
- Moving business formulas into `runtime.py` for convenience.
- Introducing broad `except Exception: pass` paths that hide live uncertainty, retry, or fail-closed semantics.
- Keeping fake prices, fake order IDs, placeholder asset IDs, or fake fills in production paths.

Code quality expectations for Python:

- Prefer pure decision functions for strategy, risk, exit, and planner logic.
- Keep venue-specific API quirks in `lightfee/venues/`.
- Keep runtime as orchestration and dependency wiring.
- Use explicit result objects or typed exceptions for uncertain/rejected/retryable outcomes.
- Keep tests close to the module that owns the behavior, plus one integration test for the production caller.
- Add comments only where they preserve Rust business provenance or explain a non-obvious edge case.

## Architecture Rule

The Python boundary owns the behavior closest to its responsibility:

| V2 Module | Owns | Must Not Own |
| --- | --- | --- |
| `lightfee/core/` | domain types, adapter protocol, errors, money/precision primitives | strategy decisions, exchange payloads |
| `lightfee/config/` | loading, validation, compatibility defaults | live runtime decisions |
| `lightfee/strategy/` | candidate filtering, maker-leg selection inputs, opportunity scoring | order submission, venue API quirks |
| `lightfee/engine/runtime.py` | orchestration, tick lanes, dependency wiring, lifecycle transitions | exchange-specific payloads, entry/exit formulas |
| `lightfee/engine/entry.py` | entry context, entry orders, state-machine labels | runtime candidate discovery |
| `lightfee/engine/execution_planner.py` | V1 entry sizing, clip, min-notional, fallback/reject planning | actual venue order submission |
| `lightfee/engine/entry_sync.py` | synchronized maker/hedge execution, pending entry, residual entry protection | candidate ranking |
| `lightfee/engine/exit_decision.py` | pure exit intent decisions | order submission |
| `lightfee/engine/close_executor.py` | reduce-only close execution, chunks, PnL attribution, pending close | risk-line threshold calculation |
| `lightfee/engine/risk_actions.py` | risk view to action plan | venue HTTP details |
| `lightfee/engine/supervisor.py` | per-position supervision and execution of risk plans | entry discovery, venue adapters directly except injected executors |
| `lightfee/engine/recovery.py` | snapshot/journal replay and startup recovery classification | live venue queries |
| `lightfee/engine/reconciliation.py` | querying adapters to resolve unknown orders/positions | risk thresholds |
| `lightfee/marketdata/` | WS/L2 state, freshness, execution liquidity, private event buffer | strategy filtering |
| `lightfee/venues/` | per-exchange signing, payloads, precision, capability truth, special errors | engine strategy decisions |
| `lightfee/rate_limit/` | token buckets, cooldown, reload, recommendations | business order sizing |
| `lightfee/persistence/` | journal, snapshot, sqlite ledgers, replayable event shape | runtime business branching |
| `lightfee/ops/` | operator commands and current-state control | hidden state mutation outside engine lifecycle |

If a fix needs two modules, one owns the business rule and the other only consumes it. For example, `execution_planner.py` owns passive route sizing; `runtime.py` only calls it.

## Required Parity Matrix

Create and maintain:

`docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`

Every row must include:

```text
ID
Owner module
Rust source path and function
Python source path and function
Live-path caller
Behavior category
Observed drift
Required parity behavior
Test file
Priority
Status
```

Allowed status values:

- `open`
- `in_progress`
- `fixed`
- `deferred_non_live`
- `approved_deviation`

No drift item can be marked fixed unless a focused test proves it and the production caller path is connected.

## Known Module-Owned Gaps

### Runtime Orchestration

Owner: `lightfee/engine/runtime.py`, with support from `lightfee/apps/live.py`.

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/main.rs`
- `/media/wl/新加卷/codex/LightFee/src/app_runtime/loop_control.rs`
- `/media/wl/新加卷/codex/LightFee/src/app_runtime/bootstrap.rs`

Known gaps:

- Rust has full tick, active-position tick, maker-event lane, rate-limit reload interval, SIGHUP reload, jittered backoff, and evidence-store recording. V2 currently has a simpler loop.
- Rust live startup runs prewarm and phased activation for private streams, market streams, and local L2. V2 live wiring creates executors but does not yet match phase activation semantics.
- Async housekeeping must be awaited in all production and test callers.

Required behavior:

- Runtime remains the orchestrator only.
- Lane timing, backoff, reload, and evidence events must match V1 where live behavior depends on them.
- Any missing lane gets its own module-owned service instead of being embedded as ad hoc runtime logic.

### Entry And Planner

Owners:

- `lightfee/engine/execution_planner.py`
- `lightfee/engine/entry.py`
- `lightfee/engine/entry_sync.py`
- `lightfee/strategy/market_view.py`

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_execution_planner.rs`
- `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/entry.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/helpers.rs`

Known gaps:

- V2 planner helpers are close to V1, but `LiveRuntime._dispatch_entry()` still does not drive route, maker leg, and initial quantity from the planner.
- `maker_leg` and `EntryType` must not be fixed in runtime when V1 would choose passive, fallback, rejected, or standard route from inputs.
- Budget and max-position gates must block before order construction with V1 reason semantics.
- Pending entry deadlines, fallback state, client order IDs, and uncertain outcomes must survive restart and reconciliation.

Required behavior:

- Strategy/market view determines candidate-side facts.
- `execution_planner.py` determines route and quantities.
- `entry_sync.py` executes the route and records pending/open/residual state.
- Runtime only assembles inputs and dispatches.

### Exit And Close Execution

Owners:

- `lightfee/engine/exit_decision.py`
- `lightfee/engine/exit.py`
- `lightfee/engine/close_executor.py`
- `lightfee/engine/residual.py`

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/engine/exit.rs`
- `/media/wl/新加卷/codex/LightFee/src/execution_core/exit.rs`
- `/media/wl/新加卷/codex/LightFee/src/execution_core/residual.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/risk.rs`

Known gaps:

- All `ExitReason` values must be live-actionable, not only representable.
- Funding capture stages, trailing drawdown, mark hard stop, settlement force close, risk close, and dust terminalization need per-reason parity tests.
- Reduce-only close chunking must use venue precision/min-notional exemptions exactly where V1 does.
- PnL attribution must keep price PnL, entry fees, exit fees, and funding captured separate enough for replay.

Required behavior:

- Pure exit decisions produce close intents.
- `close_executor.py` submits reduce-only orders, tracks uncertain outcomes, updates state, and creates pending close/reconciliation tasks when needed.
- Dust/residual handling belongs in `residual.py` and is consumed by close/entry executors.

### Risk

Owners:

- `lightfee/engine/risk_actions.py`
- `lightfee/engine/supervisor.py`
- `lightfee/risk/health.py`
- `lightfee/risk/budgets.py`
- `lightfee/risk/operator.py`

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/risk.rs`
- `/media/wl/新加卷/codex/LightFee/src/health.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/risk.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/supervision.rs`

Known gaps:

- V2 no longer hardcodes unsupported risk snapshots in runtime, but venue adapters do not yet provide real `fetch_account_risk_snapshot()` implementations.
- Capability truth is split: `venues/base.py` says some venues support risk health, while adapter protocol defaults return unsupported/no snapshot.
- Death-line single-side protection currently records state transition but must be checked against V1's live protective order behavior.
- Warning/delever/death journal events and mode transitions must match V1 replay needs.

Required behavior:

- Venue adapters expose capability truth and real snapshots where V1 supports them.
- `risk_actions.py` owns stale/unsupported snapshot policy.
- `supervisor.py` executes plans through injected close/protection services.

### Market Data, Local L2, And Private WS

Owners:

- `lightfee/marketdata/l2.py`
- `lightfee/marketdata/local_book.py`
- `lightfee/marketdata/ws.py`
- `lightfee/marketdata/private_ws.py`
- `lightfee/marketdata/freshness.py`
- `lightfee/marketdata/liquidity.py`

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/engine/market_data.rs`
- `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/ws.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/private_ws.rs`
- `/media/wl/新加卷/codex/LightFee/src/resilience.rs`

Known gaps:

- Rust startup activates private streams, market streams, and local L2 in phases with budgets and timeouts.
- Rust has maker-event lane and local-L2 primary/shadow readiness semantics.
- Private fills update pending entry/close certainty where V1 relies on private streams.
- Execution liquidity prefers cached true L2 and falls back to REST under V1 conditions.

Required behavior:

- Market data modules own feed state and freshness.
- Runtime asks for readiness/liquidity snapshots and reacts to structured results.
- Private WS events are buffered and consumed by reconciliation/entry/close services.

### Venues

Owners:

- `lightfee/venues/transport.py`
- `lightfee/venues/specs.py`
- per-venue files under `lightfee/venues/`
- `lightfee/core/contracts.py`

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/live/binance.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/okx.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/gate.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/aster.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/hyperliquid.rs`
- `/media/wl/新加卷/codex/LightFee/src/market_gateway/venue_rules.rs`

Known gaps:

- Risk-health capability and implementation are not aligned.
- Reduce-only special cases must be verified per venue, including empty-position terminal success and pending-conflict cancellation/retry.
- Signing payloads, timestamp formats, account modes, leverage setup, position side, quantity units, and precision must be checked line by line.
- Hyperliquid asset index handling must not use placeholders in live paths.
- Venue-specific error codes must classify `REJECTED` vs `UNCERTAIN` exactly as V1.

Required behavior:

- Venue modules own exchange-specific quirks.
- Engine receives normalized domain objects and classified errors only.

### Recovery And Reconciliation

Owners:

- `lightfee/engine/recovery.py`
- `lightfee/engine/reconciliation.py`
- `lightfee/persistence/journal.py`
- `lightfee/persistence/snapshot_store.py`

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs`
- `/media/wl/新加卷/codex/LightFee/src/runtime_state/persisted_engine.rs`
- `/media/wl/新加卷/codex/LightFee/src/runtime_state/snapshot_store.rs`
- `/media/wl/新加卷/codex/LightFee/src/observability_ops/replay_bridge.rs`

Known gaps:

- Reconciliation service is now called, but retry schedule, deadline handling, residual exposure repair, and private WS fill integration need V1 parity.
- Old constructor compatibility in `OrderReconciler` can hide tests that do not actually query both legs.
- Journal replay must reconstruct enough open/pending/risk state for safe restart, not just counts.

Required behavior:

- `recovery.py` owns offline reconstruction and startup classification.
- `reconciliation.py` owns live venue queries and retry outcomes.
- Persistence modules own atomicity and replayable event shape.

### Persistence, Journal, Metrics, And Evidence

Owners:

- `lightfee/persistence/`
- `lightfee/engine/loop_control.py`
- `lightfee/apps/live.py`

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/main.rs`
- `/media/wl/新加卷/codex/LightFee/src/observability_ops/journal_bridge.rs`
- `/media/wl/新加卷/codex/LightFee/src/observability_ops/replay_bridge.rs`
- `/media/wl/新加卷/codex/LightFee/src/runtime_state/persist_worker.rs`

Known gaps:

- Rust records runtime error evidence for failed ticks, active ticks, maker-event ticks, and rate-limit reload failures.
- V2 journal events exist, but event names and payloads need parity review.
- Async journal semantics, critical append behavior, dropped async counts, and metrics names are not fully mirrored.

Required behavior:

- Every live action and failure path writes enough structured data to replay the decision.
- Metrics and current-state exports must not become the source of truth; journal/snapshot do.

### Rate Limit And Operator Control

Owners:

- `lightfee/rate_limit/`
- `lightfee/ops/`
- `lightfee/risk/operator.py`
- `lightfee/engine/lifecycle.py`

Rust references:

- `/media/wl/新加卷/codex/LightFee/src/rate_limit/`
- `/media/wl/新加卷/codex/LightFee/src/main.rs`
- `/media/wl/新加卷/codex/LightFee/src/runtime_state/config.rs`

Known gaps:

- Rust reloads rate-limit config periodically and on SIGHUP; V2 SIGHUP is still a placeholder.
- Operator resume/fail-closed behavior must respect blocking recovery state.
- Rate-limit recommendation events must be flushed with evidence/journal semantics.

Required behavior:

- Rate-limit runtime owns reload and recommendation generation.
- Runtime only schedules reload and records outcome.
- Operator control must never bypass recovery blockers.

## Small-Drift Checklist

Every module review must check:

- Numeric constants: `1e-9`, `1e-12`, default `0.8`, retry bases, retry caps, stale windows.
- Rounding direction: floor vs ceil vs round; contract multiplier ordering.
- Optional/None semantics: missing risk snapshot, missing quote, unsupported venue capability.
- Async correctness: every coroutine production path and test path is awaited.
- Error class: rejected vs uncertain vs terminal success vs retryable failure.
- Journal event kind and payload fields.
- State serialization keys and enum string values.
- Test fixtures that return `1.0` or default fills only as fake-adapter behavior, never live-path behavior.

## Acceptance

The parity closure is accepted only when:

- The parity matrix has rows for all known live-path modules.
- Every open P0/P1 gap is fixed or explicitly marked `approved_deviation`.
- Focused tests cover pure helper behavior and production caller integration.
- `pytest -q` passes.
- `python3 -m compileall lightfee tests` passes.
- `pytest -q -W error` has no unawaited coroutine warnings in touched tests.
- No live-path required method raises `NotImplementedError`.
- No production path uses fake price, fake quantity, fake order id, or placeholder venue asset index.
