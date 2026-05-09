# Python LightFee Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the Python LightFee implementation with exchange-native data sources, live trading support for all seven venues, and separated live/sidecar/scheduler runtime boundaries.

**Architecture:** The Rust LightFee project is the reference implementation. The Python version ports behavior by stable contracts and deterministic decisions, not by copying Rust compatibility scaffolding. The runtime has two required long-running processes (`lightfee-live`, `lightfee-sidecar`) plus an offline scheduler/timer boundary.

**Tech Stack:** Python 3.12, asyncio, pydantic or dataclasses for schema contracts, tomli/tomllib for TOML, aiohttp/httpx for REST, websockets for WS, sqlite3/aiosqlite for persistence, pytest for focused equivalence checks.

---

## Reference Documents

- Design: `docs/plans/2026-05-09-python-lightfee-architecture-design.md`
- Rust reference root: `D:\codex2\LightFee`
- Primary Rust reference areas:
  - `src/main.rs`
  - `src/bin/opportunity_input_sidecar.rs`
  - `src/app_runtime/`
  - `src/runtime_state/`
  - `src/market_gateway/ports.rs`
  - `src/execution_core/`
  - `src/engine/`
  - `src/strategy_intelligence/`
  - `src/opportunity_input/`
  - `src/live/`
  - `src/analysis.rs`
  - `src/offline_replay/`
  - `src/evolution/`
  - `src/llm_evolution/`

## Global Guardrails

- Do not add Chillybot or feed service code.
- Do not drop non-Chillybot config parameters.
- Do not let `sidecar` place orders.
- Do not let `offline/evolution` mutate live runtime state.
- Keep live process independent from scheduler success.
- Keep all live entry decisions exchange-native and sidecar-snapshot backed.
- Keep every persisted schema versioned.
- Prefer small deterministic functions around business decisions.

---

### Task 1: Project Skeleton And Tooling

**Files:**
- Create: `pyproject.toml`
- Create: `lightfee/__init__.py`
- Create: `lightfee/apps/__init__.py`
- Create: `lightfee/core/__init__.py`
- Create: `lightfee/config/__init__.py`
- Create: `lightfee/venues/__init__.py`
- Create: `lightfee/sidecar/__init__.py`
- Create: `lightfee/engine/__init__.py`
- Create: `lightfee/strategy/__init__.py`
- Create: `lightfee/risk/__init__.py`
- Create: `lightfee/marketdata/__init__.py`
- Create: `lightfee/persistence/__init__.py`
- Create: `lightfee/offline/__init__.py`
- Create: `lightfee/ops/__init__.py`
- Create: `tests/__init__.py`

**Step 1: Write the package metadata**

Add `pyproject.toml` with package name `lightfee-py`, Python `>=3.12`, and console scripts:

```toml
[project.scripts]
lightfee-live = "lightfee.apps.live:main"
lightfee-sidecar = "lightfee.apps.sidecar:main"
lightfee-scheduler = "lightfee.apps.scheduler:main"
lightfee-ops = "lightfee.apps.ops:main"
lightfee-report = "lightfee.apps.report:main"
lightfee-replay = "lightfee.apps.replay:main"
lightfee-evolution = "lightfee.apps.evolution:main"
lightfee-probe = "lightfee.apps.probe:main"
```

**Step 2: Add empty package files**

Create the package directories listed above with empty `__init__.py` files.

**Step 3: Run import smoke**

Run: `python -m compileall lightfee`

Expected: command exits `0`.

**Step 4: Commit**

```bash
git add pyproject.toml lightfee tests
git commit -m "chore: scaffold python lightfee package"
```

---

### Task 2: Core Domain Contracts

**Files:**
- Create: `lightfee/core/domain.py`
- Create: `lightfee/core/money.py`
- Create: `lightfee/core/time.py`
- Create: `lightfee/core/errors.py`
- Test: `tests/test_domain.py`

**Step 1: Write domain tests**

Cover:

- `Venue` supports `binance`, `okx`, `bybit`, `bitget`, `gate`, `aster`, `hyperliquid`
- `Symbol` normalizes to uppercase and rejects blanks
- `Side.opposite()` and signed quantity behavior match Rust
- `FundingOpportunityType` supports `aligned` and `staggered`

**Step 2: Implement minimal domain models**

Use enums and frozen dataclasses for:

- `Venue`
- `Symbol`
- `Side`
- `FundingLeg`
- `FundingOpportunityType`
- `FundingSnapshot`
- `MarketSnapshot`
- `PositionSnapshot`
- `OrderRequest`
- `OrderFill`
- `VenueMarketSnapshot`

**Step 3: Run tests**

Run: `pytest tests/test_domain.py -v`

Expected: all tests pass.

**Step 4: Commit**

```bash
git add lightfee/core tests/test_domain.py
git commit -m "feat: add core domain contracts"
```

---

### Task 3: Configuration Schema And Chillybot Removal

**Files:**
- Create: `lightfee/config/schema.py`
- Create: `lightfee/config/loader.py`
- Create: `lightfee/config/defaults.py`
- Create: `lightfee/config/validation.py`
- Create: `lightfee/config/compatibility.py`
- Create: `config/example.toml`
- Test: `tests/test_config.py`

**Step 1: Write config tests**

Cover:

- Existing TOML sections load: `runtime`, `strategy`, `persistence`, `venues`
- All seven venues parse
- Chillybot fields raise explicit migration errors
- `opportunity_input_mode = "sidecar_backed"` is accepted
- `opportunity_source = "chillybot_first"` is rejected
- non-Chillybot strategy knobs are retained

**Step 2: Implement schema**

Create dataclasses or pydantic models for:

- `RuntimeConfig`
- `StrategyConfig`
- `PersistenceConfig`
- `VenueConfig`
- `AppConfig`

Include compatibility handling for legacy names that are not Chillybot-specific.

**Step 3: Implement loader**

Use `tomllib` for Python 3.12.

Validation must reject removed Chillybot/feed fields with an error like:

```text
removed Chillybot config field: runtime.chillybot_api_base
```

**Step 4: Run tests**

Run: `pytest tests/test_config.py -v`

Expected: all tests pass.

**Step 5: Commit**

```bash
git add lightfee/config config tests/test_config.py
git commit -m "feat: add config loader and chillybot migration validation"
```

---

### Task 4: Persistence Contracts

**Files:**
- Create: `lightfee/persistence/journal.py`
- Create: `lightfee/persistence/snapshot_store.py`
- Create: `lightfee/persistence/sqlite_store.py`
- Create: `lightfee/persistence/ledgers.py`
- Create: `lightfee/persistence/metrics.py`
- Test: `tests/test_persistence.py`

**Step 1: Write persistence tests**

Cover:

- JSONL journal appends records with `seq`, `run_id`, `ts_ms`, `kind`, `payload`
- critical append can force flush
- snapshot write is atomic
- invalid snapshot load returns a controlled error
- SQLite schema creates tables for daily reports, facts, proposal catalog, approval queue, experiment ledger

**Step 2: Implement journal**

Implement bounded async queue later; first implement synchronous append and read helpers.

**Step 3: Implement atomic snapshot store**

Write to temp file in same directory, flush, then replace.

**Step 4: Implement SQLite schema bootstrap**

Keep schema creation idempotent.

**Step 5: Run tests**

Run: `pytest tests/test_persistence.py -v`

Expected: all tests pass.

**Step 6: Commit**

```bash
git add lightfee/persistence tests/test_persistence.py
git commit -m "feat: add persistence contracts"
```

---

### Task 5: Venue Adapter Port And Capability Registry

**Files:**
- Create: `lightfee/venues/base.py`
- Create: `lightfee/venues/registry.py`
- Create: `lightfee/venues/common.py`
- Test: `tests/test_venues_base.py`

**Step 1: Write adapter port tests**

Cover:

- all seven venue capabilities exist
- true L2 capability is explicit per venue
- risk health unsupported venues are explicit
- quantity normalization floors to step
- reduce-only min-notional exemptions match Rust reference

**Step 2: Implement base adapter protocol**

Define async methods for:

- `fetch_market_snapshot`
- `fetch_funding`
- `fetch_position`
- `place_order`
- `fetch_order_fill_reconciliation`
- `fetch_passive_order_progress`
- `fetch_account_balance_snapshot`
- `fetch_account_risk_snapshot`
- `fetch_execution_liquidity_snapshot`
- `order_sizing_spec`
- `normalize_quantity`
- `ensure_entry_leverage`
- `shutdown`

**Step 3: Implement capability registry**

Mirror Rust capability intent from `src/market_gateway/ports.rs`.

**Step 4: Run tests**

Run: `pytest tests/test_venues_base.py -v`

Expected: all tests pass.

**Step 5: Commit**

```bash
git add lightfee/venues tests/test_venues_base.py
git commit -m "feat: define venue adapter port and capabilities"
```

---

### Task 6: Sidecar Snapshot Schema And Publisher

**Files:**
- Create: `lightfee/sidecar/snapshot.py`
- Create: `lightfee/sidecar/publisher.py`
- Create: `lightfee/sidecar/pairing.py`
- Test: `tests/test_sidecar_snapshot.py`

**Step 1: Write sidecar tests**

Cover:

- snapshot schema version is required
- snapshot candidate list validates
- stale snapshot is rejected by freshness helper
- atomic publisher writes valid JSON
- provenance contains exchange/native source only
- Chillybot origin tags are impossible

**Step 2: Implement snapshot schema**

Include:

- `schema_version`
- `published_at_ms`
- `market_observed_at_ms`
- `funding_lifecycle`
- `market_lifecycle`
- `transfer_lifecycle`
- `liquidity_lifecycle`
- `degraded_venues`
- `quotes`
- `candidates`

**Step 3: Implement atomic publisher**

Use persistence atomic write helper.

**Step 4: Run tests**

Run: `pytest tests/test_sidecar_snapshot.py -v`

Expected: all tests pass.

**Step 5: Commit**

```bash
git add lightfee/sidecar tests/test_sidecar_snapshot.py
git commit -m "feat: add sidecar snapshot contract"
```

---

### Task 7: Strategy Discovery And Scoring

**Files:**
- Create: `lightfee/strategy/market_view.py`
- Create: `lightfee/strategy/transfer_bias.py`
- Create: `lightfee/strategy/scoring.py`
- Create: `lightfee/strategy/discovery.py`
- Test: `tests/test_strategy_discovery.py`

**Step 1: Write strategy tests**

Cover:

- lower funding venue becomes long side
- higher funding venue becomes short side
- fees, slippage, buffers, and transfer bias affect expected edge
- aligned and staggered opportunities are classified
- blocked candidates preserve blocked reasons
- max scan timing window is applied

**Step 2: Implement deterministic scoring**

Port behavior from Rust strategy/opportunity input modules by inputs and outputs.

**Step 3: Run tests**

Run: `pytest tests/test_strategy_discovery.py -v`

Expected: all tests pass.

**Step 4: Commit**

```bash
git add lightfee/strategy tests/test_strategy_discovery.py
git commit -m "feat: port strategy discovery and scoring"
```

---

### Task 8: Risk Modes And Budgets

**Files:**
- Create: `lightfee/risk/modes.py`
- Create: `lightfee/risk/budgets.py`
- Create: `lightfee/risk/health.py`
- Create: `lightfee/risk/operator.py`
- Test: `tests/test_risk.py`

**Step 1: Write risk tests**

Cover:

- lifecycle maps to global risk mode
- operator pause/reduce/fail-closed/reconcile/resume behavior
- resume-if-safe rejects blocking recovery work
- exposure budget blocks entries
- unsupported risk snapshot behavior is explicit

**Step 2: Implement risk contracts**

Port lifecycle and operator transition behavior from Rust engine contract.

**Step 3: Run tests**

Run: `pytest tests/test_risk.py -v`

Expected: all tests pass.

**Step 4: Commit**

```bash
git add lightfee/risk tests/test_risk.py
git commit -m "feat: add risk mode and operator transitions"
```

---

### Task 9: Engine State And Recovery Contracts

**Files:**
- Create: `lightfee/engine/state.py`
- Create: `lightfee/engine/lifecycle.py`
- Create: `lightfee/engine/recovery.py`
- Test: `tests/test_engine_recovery.py`

**Step 1: Write recovery tests**

Cover:

- empty state starts booting/reconciling safely
- open positions force recovery posture
- ambiguous live position truth fails closed
- snapshot load plus journal replay rebuilds open positions
- missing private confirmation keeps risk-only

**Step 2: Implement state models**

Include:

- `EngineState`
- `OpenPosition`
- `PendingEntry`
- `PendingClose`
- `OperatorControlState`
- recovery work snapshot

**Step 3: Implement recovery projection**

Keep deterministic first. Venue I/O is injected.

**Step 4: Run tests**

Run: `pytest tests/test_engine_recovery.py -v`

Expected: all tests pass.

**Step 5: Commit**

```bash
git add lightfee/engine tests/test_engine_recovery.py
git commit -m "feat: add engine state and recovery contracts"
```

---

### Task 10: Entry Execution Planner

**Files:**
- Create: `lightfee/engine/execution_planner.py`
- Test: `tests/test_execution_planner.py`

**Step 1: Write planner tests**

Cover the Rust-documented planner behavior:

- raise too-small passive clip to maker minimum when valid
- fall back to standard when minimum clip is too close to full target
- reject when hedgeable aligned quantity is zero
- never produce maker clip below effective minimum notional

**Step 2: Implement planner**

Keep it pure. Inputs are target quantity, price, min notional, hedge chunk, and
threshold config. Output is one of:

- `passive_incremental`
- `fallback_to_standard`
- `rejected`

**Step 3: Run tests**

Run: `pytest tests/test_execution_planner.py -v`

Expected: all tests pass.

**Step 4: Commit**

```bash
git add lightfee/engine/execution_planner.py tests/test_execution_planner.py
git commit -m "feat: add entry execution planner"
```

---

### Task 11: Entry And Exit State Machines

**Files:**
- Create: `lightfee/engine/entry.py`
- Create: `lightfee/engine/exit.py`
- Create: `lightfee/engine/passive_maker.py`
- Test: `tests/test_engine_entry_exit.py`

**Step 1: Write state-machine tests**

Cover:

- maker-first entry submits maker leg before hedge
- maker fill triggers taker hedge for fill quantity
- partial fill persists pending state
- uncertain order starts cooldown
- exit closes both legs reduce-only
- close failure enters protected recovery

**Step 2: Implement state-machine shell**

Use injected venue adapter ports. Do not implement concrete exchange logic here.

**Step 3: Run tests**

Run: `pytest tests/test_engine_entry_exit.py -v`

Expected: all tests pass.

**Step 4: Commit**

```bash
git add lightfee/engine tests/test_engine_entry_exit.py
git commit -m "feat: add entry and exit state machines"
```

---

### Task 12: Sidecar Exchange Sources

**Files:**
- Create: `lightfee/sidecar/sources/exchange.py`
- Create: `lightfee/sidecar/sources/liquidity.py`
- Create: `lightfee/sidecar/sources/transfer.py`
- Create: `lightfee/sidecar/service.py`
- Test: `tests/test_sidecar_service.py`

**Step 1: Write sidecar service tests**

Use fake venue adapters. Cover:

- sidecar gathers exchange-native funding and quotes
- sidecar builds same-symbol pairs
- sidecar publishes ranked candidates
- source failure marks domain degraded
- no Chillybot source is constructed

**Step 2: Implement service**

Keep refresh loop separate from `refresh_once`.

**Step 3: Run tests**

Run: `pytest tests/test_sidecar_service.py -v`

Expected: all tests pass.

**Step 4: Commit**

```bash
git add lightfee/sidecar tests/test_sidecar_service.py
git commit -m "feat: add exchange-native sidecar service"
```

---

### Task 13: Live Runtime Composition

**Files:**
- Create: `lightfee/engine/runtime.py`
- Create: `lightfee/engine/supervisor.py`
- Create: `lightfee/apps/live.py`
- Test: `tests/test_live_runtime.py`

**Step 1: Write runtime tests**

Use fake adapters and sidecar snapshots. Cover:

- stale snapshot skips new entry
- malformed snapshot records diagnostic and skips new entry
- fresh tradeable snapshot enters through risk and execution
- open positions are managed even when sidecar is stale
- scheduler absence does not affect tick

**Step 2: Implement runtime tick**

Separate:

- full scan tick
- active positions tick
- maker event lane
- housekeeping

**Step 3: Implement app entrypoint**

Parse config path, load config, construct adapters, initialize engine, run loop.

**Step 4: Run tests**

Run: `pytest tests/test_live_runtime.py -v`

Expected: all tests pass.

**Step 5: Commit**

```bash
git add lightfee/engine lightfee/apps/live.py tests/test_live_runtime.py
git commit -m "feat: add live runtime composition"
```

---

### Task 14: Concrete Venue Adapters

**Files:**
- Create: `lightfee/venues/binance.py`
- Create: `lightfee/venues/okx.py`
- Create: `lightfee/venues/bybit.py`
- Create: `lightfee/venues/bitget.py`
- Create: `lightfee/venues/gate.py`
- Create: `lightfee/venues/aster.py`
- Create: `lightfee/venues/hyperliquid.py`
- Modify: `lightfee/venues/registry.py`
- Test: `tests/test_venue_contracts.py`

**Step 1: Write adapter contract tests**

Use mocked HTTP/WS clients. Cover each venue:

- signing path builds expected headers/query shape
- symbol normalization
- quantity normalization
- market snapshot parse
- position parse
- order response parse
- error classification as rejected vs uncertain

**Step 2: Implement adapters one venue at a time**

Order:

1. Binance
2. OKX
3. Bybit
4. Bitget
5. Gate
6. Aster
7. Hyperliquid

Commit after each venue.

**Step 3: Run per-venue tests**

Run: `pytest tests/test_venue_contracts.py -v`

Expected: all tests pass.

**Step 4: Commit after each adapter**

Example:

```bash
git add lightfee/venues/binance.py tests/test_venue_contracts.py
git commit -m "feat: add binance live adapter"
```

---

### Task 15: Offline Analysis And Reports

**Files:**
- Create: `lightfee/offline/analysis/__init__.py`
- Create: `lightfee/offline/analysis/journal.py`
- Create: `lightfee/offline/analysis/incident.py`
- Create: `lightfee/offline/reports/render.py`
- Create: `lightfee/apps/report.py`
- Test: `tests/test_offline_analysis.py`

**Step 1: Write analysis tests**

Cover:

- venue order stats
- failure rates
- latency percentiles
- daily PnL summary
- pending entry quality summary
- incident report reads state plus journal

**Step 2: Implement journal analysis**

Port data aggregation behavior from Rust `analysis.rs`.

**Step 3: Implement report CLI**

Support JSON and text modes.

**Step 4: Run tests**

Run: `pytest tests/test_offline_analysis.py -v`

Expected: all tests pass.

**Step 5: Commit**

```bash
git add lightfee/offline/analysis lightfee/offline/reports lightfee/apps/report.py tests/test_offline_analysis.py
git commit -m "feat: add offline journal analysis"
```

---

### Task 16: Offline Replay

**Files:**
- Create: `lightfee/offline/replay/__init__.py`
- Create: `lightfee/offline/replay/dataset.py`
- Create: `lightfee/offline/replay/engine.py`
- Create: `lightfee/offline/replay/counterfactual.py`
- Create: `lightfee/offline/replay/walk_forward.py`
- Create: `lightfee/apps/replay.py`
- Test: `tests/test_offline_replay.py`

**Step 1: Write replay tests**

Cover:

- replay dataset loads journal range
- rejected candidate attribution is preserved
- counterfactual spec can force select review id
- walk-forward windows are generated deterministically

**Step 2: Implement replay core**

Port the Rust behavior by records and outputs.

**Step 3: Run tests**

Run: `pytest tests/test_offline_replay.py -v`

Expected: all tests pass.

**Step 4: Commit**

```bash
git add lightfee/offline/replay lightfee/apps/replay.py tests/test_offline_replay.py
git commit -m "feat: add offline replay"
```

---

### Task 17: Evolution And LLM Evolution

**Files:**
- Create: `lightfee/offline/evolution/__init__.py`
- Create: `lightfee/offline/evolution/report.py`
- Create: `lightfee/offline/evolution/ledger.py`
- Create: `lightfee/offline/evolution/approval.py`
- Create: `lightfee/offline/evolution/cycle.py`
- Create: `lightfee/offline/llm_evolution/__init__.py`
- Create: `lightfee/offline/llm_evolution/report.py`
- Create: `lightfee/apps/evolution.py`
- Test: `tests/test_evolution.py`

**Step 1: Write evolution tests**

Cover:

- report reads evidence and writes markdown/json
- proposal catalog persists
- approval queue persists
- outcome ledger persists
- LLM stage disabled mode works without network
- no function auto-applies live config

**Step 2: Implement deterministic evolution**

Start with disabled/mock synthesizer modes.

**Step 3: Implement optional HTTP LLM mode**

Require explicit env prefix. Do not read credentials from live venue config.

**Step 4: Run tests**

Run: `pytest tests/test_evolution.py -v`

Expected: all tests pass.

**Step 5: Commit**

```bash
git add lightfee/offline/evolution lightfee/offline/llm_evolution lightfee/apps/evolution.py tests/test_evolution.py
git commit -m "feat: add offline evolution workflows"
```

---

### Task 18: Scheduler And Daily Snapshot

**Files:**
- Create: `lightfee/apps/scheduler.py`
- Create: `lightfee/offline/reports/daily.py`
- Test: `tests/test_scheduler.py`

**Step 1: Write scheduler tests**

Cover:

- daily snapshot job writes SQLite report
- journal analysis job writes report artifacts
- replay/evolution job failures do not stop later jobs
- scheduler never writes live runtime state

**Step 2: Implement job runner**

Support one-shot mode and optional loop mode.

**Step 3: Run tests**

Run: `pytest tests/test_scheduler.py -v`

Expected: all tests pass.

**Step 4: Commit**

```bash
git add lightfee/apps/scheduler.py lightfee/offline/reports/daily.py tests/test_scheduler.py
git commit -m "feat: add scheduler jobs"
```

---

### Task 19: Ops And Probe CLIs

**Files:**
- Create: `lightfee/ops/commands.py`
- Create: `lightfee/apps/ops.py`
- Create: `lightfee/apps/probe.py`
- Test: `tests/test_ops_probe.py`

**Step 1: Write CLI tests**

Cover:

- ops writes operator control command through persistence
- resume-if-safe rejects blocking recovery work
- venue capabilities lists all seven venues
- probe dry-run does not place orders unless explicit execute flag is set

**Step 2: Implement ops CLI**

Mirror Rust command names:

- `pause-entry`
- `reduce-only`
- `fail-closed`
- `reconcile-now`
- `resume-if-safe`

**Step 3: Implement probe CLI shell**

Keep live smoke and L2 probe behind explicit flags.

**Step 4: Run tests**

Run: `pytest tests/test_ops_probe.py -v`

Expected: all tests pass.

**Step 5: Commit**

```bash
git add lightfee/ops lightfee/apps/ops.py lightfee/apps/probe.py tests/test_ops_probe.py
git commit -m "feat: add ops and probe entrypoints"
```

---

### Task 20: End-To-End Configuration And Runtime Smoke

**Files:**
- Modify: `config/example.toml`
- Create: `config/live.example.toml`
- Create: `tests/test_runtime_smoke.py`

**Step 1: Write smoke tests**

Cover:

- example config loads
- live example config validates without Chillybot fields
- sidecar refresh with fake adapters publishes snapshot
- live tick consumes fake snapshot and records expected journal events
- scheduler daily job writes SQLite facts

**Step 2: Run full test suite**

Run: `pytest -v`

Expected: all tests pass.

**Step 3: Run compile check**

Run: `python -m compileall lightfee`

Expected: command exits `0`.

**Step 4: Commit**

```bash
git add config tests/test_runtime_smoke.py
git commit -m "test: add end-to-end runtime smoke coverage"
```

---

## Final Verification

Run:

```bash
pytest -v
python -m compileall lightfee
```

Expected:

- pytest exits `0`
- compileall exits `0`

Then manually verify:

- no Chillybot module exists
- no Chillybot config field is accepted
- only `lightfee-live` and `lightfee-sidecar` are required long-running services
- scheduler/timer jobs are offline-only
- all seven venues are present in the registry

