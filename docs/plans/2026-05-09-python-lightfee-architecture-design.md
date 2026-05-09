# Python LightFee Architecture Design

## Goal

Build a Python version of LightFee with the same business behavior as the current
working Rust implementation, while reducing runtime coupling and long-term
maintenance cost.

The Rust project is the reference implementation. The Python version should
preserve the trading semantics, runtime safety posture, persistence model,
daily review workflow, and live venue coverage. It should not preserve Rust
compatibility shims, historical public surfaces, or crate-split scaffolding.

## Non-Goals

- Do not redesign the trading strategy from scratch.
- Do not reduce existing non-Chillybot configuration coverage.
- Do not ship a paper-only first version; the Python version must support live
  trading.
- Do not introduce a separate Chillybot feed service.
- Do not migrate Chillybot hint/feed behavior. The Python version uses
  exchange-native data sources only.
- Do not make offline review or LLM/evolution jobs part of the live trading hot
  path.

## Process Model

The system uses two required long-running processes and one isolated offline
job boundary.

### `lightfee-live`

`lightfee-live` is the live trading process.

Responsibilities:

- startup validation
- live adapter construction
- restart recovery
- lifecycle and global risk mode management
- sidecar snapshot consumption
- targeted exchange revalidation before entry
- entry execution
- exit execution
- open-position supervision
- fail-closed and reduce-only handling
- journal and state snapshot writes

`lightfee-live` must not perform broad opportunity scans, daily reports, offline
replay, LLM work, or parameter evolution.

### `lightfee-sidecar`

`lightfee-sidecar` is the opportunity input data plane.

Responsibilities:

- fetch exchange-native funding data
- fetch exchange-native quotes and market snapshots
- fetch exchange-native depth or executable liquidity
- fetch exchange-native transfer/status data where supported
- build same-symbol directed pair inputs
- rank and publish candidate shortlists
- write `runtime/opportunity-input-snapshot.json`
- persist scan facts and source health diagnostics

`lightfee-sidecar` must not place orders, mutate open positions, or apply
operator controls.

The sidecar has no Chillybot source and no feed server dependency. External
hint provenance is removed from the Python design.

### `lightfee-scheduler`

`lightfee-scheduler` is the offline job boundary. It may be implemented as a
long-running scheduler or as systemd timers/cron invoking individual commands.

Responsibilities:

- daily account and PnL snapshot
- journal analysis
- incident/runtime posture reports
- offline replay and counterfactual analysis
- walk-forward review
- deterministic evolution reports
- optional LLM-assisted evolution reports
- approval and outcome ledgers

Scheduler failure must not affect live position management or sidecar snapshot
publication.

### Manual CLIs

The following commands remain manual or timer-invoked entrypoints, not live
trading dependencies:

- `lightfee-ops`: pause-entry, reduce-only, fail-closed, reconcile-now,
  resume-if-safe
- `lightfee-report`: journal, incident, runtime posture reports
- `lightfee-replay`: offline replay, backfill, walk-forward
- `lightfee-evolution`: proposals, approvals, outcomes, LLM-assisted reports
- `lightfee-probe`: live smoke, local L2 probe, venue capabilities

## Package Structure

```text
lightfee_py/
  pyproject.toml
  config/
    example.toml
    live.example.toml
  runtime/
    events.jsonl
    state.json
    opportunity-input-snapshot.json
    lightfee.sqlite
  lightfee/
    core/
      domain.py
      money.py
      time.py
      errors.py
      contracts.py
    config/
      loader.py
      schema.py
      defaults.py
      validation.py
      compatibility.py
    venues/
      base.py
      registry.py
      common.py
      binance.py
      okx.py
      bybit.py
      bitget.py
      gate.py
      aster.py
      hyperliquid.py
    sidecar/
      service.py
      snapshot.py
      publisher.py
      pairing.py
      sources/
        exchange.py
        liquidity.py
        transfer.py
    engine/
      runtime.py
      lifecycle.py
      state.py
      supervisor.py
      entry.py
      exit.py
      recovery.py
      execution_planner.py
      passive_maker.py
    strategy/
      discovery.py
      scoring.py
      market_view.py
      transfer_bias.py
    risk/
      budgets.py
      health.py
      modes.py
      operator.py
    marketdata/
      l2.py
      liquidity.py
      freshness.py
      local_book.py
    persistence/
      journal.py
      snapshot_store.py
      sqlite_store.py
      ledgers.py
      metrics.py
    offline/
      analysis/
      replay/
      evolution/
      llm_evolution/
      reports/
    ops/
      commands.py
    apps/
      live.py
      sidecar.py
      scheduler.py
      ops.py
      report.py
      replay.py
      evolution.py
      probe.py
```

## Dependency Rules

```text
core          -> no internal dependencies
config        -> core
persistence   -> core, config
venues        -> core, config, persistence.metrics
marketdata    -> core, config, venues
sidecar       -> core, config, venues, marketdata, persistence
strategy      -> core, config, marketdata
risk          -> core, config, persistence
engine        -> core, config, strategy, risk, marketdata, venues, persistence
offline       -> core, config, persistence, strategy/risk/engine data contracts
ops           -> core, config, persistence, risk.operator
apps          -> composition roots; may depend on all modules
```

Hard rules:

- `sidecar` cannot submit orders.
- `venues` cannot perform strategy selection.
- `strategy` cannot submit orders or own venue credentials.
- `offline` cannot mutate live runtime state.
- `evolution` and `llm_evolution` cannot auto-apply live parameters.
- `live` cannot depend on scheduler success.
- `live` consumes snapshot freshness and schema validity, not sidecar process
  liveness.
- `scheduler` failures delay reports only.

## Venue Coverage

The Python live version must support all seven venues from the Rust reference:

- Binance
- OKX
- Bybit
- Bitget
- Gate
- Aster
- Hyperliquid

Each venue adapter implements a shared port for:

- market snapshot fetch
- funding fetch
- position fetch
- order submission
- reduce-only close
- passive order support where available
- order fill reconciliation where available
- account balance snapshot
- account risk snapshot where supported
- true L2 or exchange depth where supported
- order sizing and quantity normalization
- startup/shutdown hooks

Unsupported venue-specific capabilities are explicit capability flags, not
implicit `None` behavior hidden in strategy or execution code.

## Data Source Policy

The Python version uses exchange-native sources only.

Removed from the Python migration:

- Chillybot opportunity hints
- Chillybot transfer source
- Feedgrab source
- Chillybot feed HTTP service
- `opportunity_source = "chillybot_first"`
- `opportunity_source = "chillybot_via_feedgrab"`
- `sidecar_chillybot_mode`
- `chillybot_api_base`
- `chillybot_timeout_ms`
- Chillybot origin tags

Retained source flow:

```text
exchange REST/WS
  -> lightfee-sidecar
  -> opportunity-input-snapshot.json
  -> lightfee-live targeted revalidation
  -> risk gates
  -> execution
```

If exchange-native transfer truth is unavailable, the system uses the retained
configuration-driven transfer degradation model. It must not call external
hint services.

## Persistence Model

The Python version keeps the Rust reference persistence split:

- JSONL journal as append-only event truth
- JSON state snapshot for restart recovery
- sidecar snapshot JSON for opportunity input
- SQLite for structured facts, daily snapshots, diagnostics, and ledgers
- Markdown/JSON report artifacts for review
- approval and outcome ledgers for evolution proposals

`lightfee-live` writes critical journal and state records synchronously where
needed for recovery safety. Non-critical telemetry can be queued and flushed
asynchronously with bounded queues.

`lightfee-sidecar` writes opportunity snapshots atomically. The live process
must reject malformed, stale, or schema-incompatible snapshots.

`lightfee-scheduler` reads journal, state, and SQLite data. It writes reports,
facts, rollups, proposal catalogs, approval queues, and experiment ledgers.

## Configuration Policy

All non-Chillybot runtime, strategy, venue, persistence, risk, execution,
passive maker, local L2, replay, analysis, and evolution parameters remain in
scope.

The Python config loader should support the existing TOML shape where practical
and should provide a compatibility layer for renamed or regrouped fields.

Removed config fields are limited to Chillybot/feed-specific settings. Removed
fields should fail validation with a clear migration error instead of being
silently ignored.

## Runtime Safety

The live process keeps the reference fail-closed posture:

- ambiguous restart state -> fail closed
- unbalanced live exposure -> fail closed or protected recovery path
- stale sidecar snapshot -> skip new entry
- malformed sidecar snapshot -> skip new entry and record diagnostic event
- uncertain order outcome -> cooldown and reconciliation
- private position truth unavailable during recovery -> risk-only
- scheduler failure -> no impact on open position management
- sidecar failure -> no new entries after freshness grace; existing positions
  continue to be managed

## Business Behavior To Preserve

The Python version preserves:

- long lower-funding venue and short higher-funding venue
- aligned and staggered funding opportunity handling
- near-settlement scan windows
- edge scoring after fees, slippage, buffers, and transfer bias
- max concurrent positions
- same-symbol venue pair selection
- maker-first entry on higher-slippage leg
- taker hedge on incremental maker fills
- fallback execution paths
- passive maker timeout, amend/cancel-replace, zero-fill, and fallback rules
- funding capture hold and exit rules
- trailing giveback, stop loss, hard stop, and recovery failure exits
- lifecycle states: booting, reconciling, risk_only, running
- global risk modes: running, entry_paused, reduce_only, fail_closed
- operator overrides
- restart recovery from snapshot, journal, and live position truth
- journal replay and daily/offline review outputs

## Stability Rationale

This design keeps the live trading hot path small while preserving daily review
and evolution workflows.

The failure domains are explicit:

```text
live failure      -> trading process issue
sidecar failure   -> no fresh entries; existing positions still managed
scheduler failure -> delayed review/reporting only
CLI failure       -> single manual command failure only
```

This is more stable than a two-process system where daily review and evolution
must live inside either the trading process or sidecar. It is also more stable
than many long-running services because only live and sidecar are required
constant processes.

## Migration Method

Migration should proceed by reference-behavior mapping, not by literal file
translation.

For each Rust business area:

1. identify the stable input and output contracts
2. port the data model to Python
3. port the deterministic decision logic
4. port the live I/O adapter only at the boundary
5. preserve journal and snapshot semantics
6. add focused equivalence checks for the ported behavior

The objective is to remove historical Rust compatibility debt while keeping the
business behavior and persisted evidence model intact.

