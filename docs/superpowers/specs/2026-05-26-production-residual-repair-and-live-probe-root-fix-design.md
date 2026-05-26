# Production Residual Repair and Live-Probe Root Fix Design

日期: 2026-05-26

状态: 设计稿，覆盖 `2026-05-26 13:10 CST` 之后生产暴露的问题。目标是把仍卡在历史 residual / recovery work 的生产状态根修，而不是手动清状态或扩大阈值掩盖。

## 背景

部署后服务是单例且运行中，但生产仍不健康:

- `lifecycle=risk_only`
- `risk_mode=fail_closed`
- `open_position_count=0`
- `pending_entry_count=0`
- `pending_close_count=0`
- `pending_passive_close_count=0`
- `pending_residual_repair_count=2`

当前阻塞项:

| Symbol | Pair | Origin | Repair venue | Side | Quantity | Last error |
|---|---|---|---|---|---:|---|
| `LYNUSDT` | `lynusdt:aster->bybit` | `close_residual` | `aster` | `sell` | `532.0` | `residual_repair_deadline_or_attempts_exhausted` |
| `OPGUSDT` | `opgusdt:binance->okx` | `entry_open` | `okx` | `buy` | `9.0` | `residual_repair_deadline_or_attempts_exhausted` |

这不是可接受的“历史问题残留”。如果生产仍因为旧 residual 进入 `fail_closed/risk_only`，说明修复还没有实现 V1 风格的恢复工作终结语义，也没有用真实 harness/probe 闭环验证。

## 非发散原则

1. **V2/V1 不一致导致的问题，复刻 V1。**
   - 不做近似语义。
   - 不靠更宽松阈值。
   - 不用手动 state edit 当作根修。
   - 必须有 V1 行为、现有 bug ledger、生产事件或 fixture 三者之一作为证据。

2. **V2/V1 共有或 V1 未覆盖的问题，查交易所官方文档。**
   - Binance / Bybit / Aster / OKX 等交易所错误码、orderbook、leverage bracket、contract metadata 必须引用官方文档或官方文档镜像。
   - 文档未直接证明的错误码只能标为 evidence gap，不能宣称根修。

3. **通过真实复现才算根修。**
   - 单元测试只是第一层。
   - 每个 P0 必须有生产事件 fixture replay 或 fake-adapter harness。
   - 涉及真实交易所状态的分支必须有只读 probe。
   - 修复后云端验收必须看到 current state 退出对应卡死状态。

4. **Harness/probe 独立于主链路。**
   - 默认 `pytest` 和 `validate_change --profile full` 不访问真实交易所。
   - 真实 probe 默认跳过，必须显式设置环境变量才运行。
   - 所有下单拒绝类 case 用 fixture/fake adapter，不用真实下单复现。

## 13:10 之后的问题分类

| Priority | Problem | Old/New | Root class | Evidence status | Root-fix route |
|---|---|---|---|---|---|
| P0 | `LYNUSDT` / `OPGUSDT` exhausted residual repairs keep production in `fail_closed` | 老问题族，新暴露 symbols | V2/V1 residual terminality drift | 当前 state 直接证明；CL-003/CL-005 记录同类 | 复刻 V1: normal housekeeping 驱动 residual repair；deadline/attempt exhausted 前先 live-flat/dust probe；flat/dust terminalize 并释放 gate |
| P0 | `BEATUSDT` recovered passive close先诊断/驱动，后证明 flat | 老问题复现 | V2/V1 recovered-state ordering drift | `exit.passive_close_recovery_probe_diagnostic=3088`，随后 `recovery.flat`/`position_drift_corrected` | 复刻 V1: recovered state 必须先 live-flat probe，flat 则清 open/pending/recovery block，不进入 close chunks |
| P0 | `BIOUSDT` Bybit duplicate CID cleanup pressure | 老问题复现/验收样本 | V2/V1 idempotency drift + exchange-documented duplicate code | `order.submit_result=185`，Bybit docs `110072 OrderLinkedID is duplicate` | 复刻 V1: duplicate 只代表 idempotency event；必须 query original cid + live position；old filled order 不能在 live nonzero 时清成功 |
| P0 | Recovery probe error volume still high | 老问题复现 | V2/V1 recovery symbol-catalog drift + exchange/rate-limit evidence gap | `recovery.live_position_probe_error=1127`，OKX timeout/blank/metadata 类仍多 | 复刻 V1 catalog filter；用官方 OKX instrument metadata/rate-limit semantics 分类；空 error 禁止作为最终证据 |
| P1 | Bybit `110126` LITE agreement reject | 老问题 | V2/V1 共有 exchange rule | 日志明确；公开 Bybit error table未直接命中 `110126`，但同族 trading terms codes 存在 | 作为 evidence gap 的 admission block；补 Bybit raw body/official page 后才能关闭 |
| P1 | Aster `-2027` leverage/max position | 老问题 | V2/V1 共有 exchange rule | Aster docs: `-2027 MAX_LEVERAGE_RATIO` 和 `GET /fapi/v1/leverageBracket` | admission/preflight block；不要重试刷单 |
| P1 | Aster `-5018` max notional | 老问题 | V2/V1 共有 exchange rule or venue headroom drift | 日志明确，公开 docs 未直接命中 `-5018` | 先保守分类为 max-notional headroom；补官方接口/返回体证据后关闭 |
| P1 | Binance `-2019` margin insufficient / Bybit `110007` insufficient balance | 老问题 | V2/V1 共有 exchange rule | Binance / Bybit 官方错误码可证明 | account/margin admission；不作为系统 bug 重试 |
| P1 | Binance `-5022` post-only would take | 老问题 | V2/V1 共有 exchange rule | Binance docs: `-5022 GTX_ORDER_REJECT` | maker BBO guard + post-only cooldown；不作为 outage |
| P1 | Local-L2 sequence/rebuild/snapshot/no-entry counts | 老问题复现 | 混合: V1 parity + exchange orderbook docs | 当前计数高，但需要 per-venue payload fixture | 只在 fixture/probe 证明具体 venue drift 后改代码 |

## 官方文档依据

- Bybit V5 error codes: `110072 OrderLinkedID is duplicate`, `110007 Available balance is insufficient`, trading-term adjacent codes `110123/110125`: https://bybit-exchange.github.io/docs/v5/error
- Bybit order/execution lookup by `orderLinkId`: https://bybit-exchange.github.io/docs/v5/order/execution
- Binance USD-M futures error codes: `-2019 Margin is insufficient`, `-5022 GTX_ORDER_REJECT`: https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code
- Aster API documentation: `-2027 MAX_LEVERAGE_RATIO`, `GET /fapi/v1/leverageBracket`, `POST /fapi/v1/leverage`: https://docs.asterdex.com/product/aster-trade-pro/api/api-document
- OKX instruments metadata for `ctVal` / `ctType`: https://www.okx.com/docs-v5/en/#rest-api-public-data-get-instruments

## Architecture

### 1. Incident Fixture Layer

Create sanitized production incident fixtures under:

- `tests/fixtures/live_incidents/2026-05-26/state/`
- `tests/fixtures/live_incidents/2026-05-26/events/`
- `tests/fixtures/live_incidents/2026-05-26/exchange_truth/`

Fixtures must include only non-secret state, event payloads, venue/symbol, order ids/client ids when needed for idempotency, normalized quantities, and raw exchange error bodies. No API keys, account IDs, signatures, or full unrelated journal excerpts.

### 2. Offline Harness Suite

Create `tests/live_harness/` for deterministic fake-adapter tests. These tests simulate:

- exhausted residual repairs with live-flat truth;
- exhausted residual repairs with live nonzero truth;
- BEATUSDT recovered passive close where exchange truth is flat;
- BIOUSDT duplicate CID where old order fill and live position disagree;
- OKX metadata/rate-limit/unsupported-symbol probe classification;
- exchange reject classification for documented admission blocks.

This suite is offline and safe. It may run in CI when selected by profile, but does not run as part of ordinary focused profiles unless explicitly requested.

### 3. Read-Only Probe Suite

Create `tests/probes/` for real exchange read-only probes. These tests are skipped unless explicitly enabled:

- `LIGHTFEE_RUN_LIVE_PROBES=1`
- credential env vars are present
- test is read-only by construction

Allowed probe operations:

- fetch positions;
- fetch open orders;
- fetch instrument metadata;
- fetch account/risk/leverage bracket where needed;
- fetch orderbook public data.

Forbidden probe operations:

- submit order;
- cancel order;
- change leverage/margin mode;
- mutate account state.

### 4. Residual Repair Terminality

Residual repair processing must be a normal runtime housekeeping responsibility, not only a startup cleanup. For each pending residual task:

1. Fetch trusted live position for repair venue and counter venue if known.
2. Fetch open orders for the symbol where supported.
3. If both venues are flat and no open orders exist, remove the residual task, release local entry pause/pair gate, and emit `execution.residual_repair_completed(result=already_flat)`.
4. If live residual exists but quantity is below venue minimum/contract dust, terminalize as `exchange_min_quantity_dust` only when official instrument metadata proves the threshold.
5. If live residual exists and is tradeable, keep or reschedule repair with structured evidence.
6. If live truth cannot be fetched, keep fail-closed with non-empty structured reason.

### 5. Recovered Passive-Close Live-Flat Priority

Recovered local state must not drive order submission before live-flat proof. V1 recovery semantics are authoritative:

- local recovered open/pending state is subordinate to exchange truth;
- if both legs are live-flat, clear local open/pending/recovery block;
- emit `exit.passive_close_fallback_terminal_flat`, `recovery.flat`, `runtime.position_drift_corrected`, and stale block cleanup events;
- do not emit hedge dust/min-notional abort before live-flat proof.

### 6. Duplicate CID Idempotency

Bybit duplicate CID is not a normal reject. It is an idempotency signal. Root semantics:

- query original `orderLinkId` in realtime/history/execution endpoints;
- classify full, partial, no evidence, uncertain;
- live position truth dominates old order-fill evidence;
- old filled order with live nonzero position must retry remaining/live residual with fresh CID or fail closed with evidence;
- no branch may clear state solely because a historical order looked filled.

### 7. Exchange Admission

Exchange-rule rejects must be admission blocks, not repeated strategy/order loops:

- documented: Bybit `110007`, Binance `-2019`, Binance `-5022`, Aster `-2027`;
- evidence-gap but operationally blockable: Bybit `110126`, Aster `-5018`;
- each block must record `venue`, `symbol`, `raw_error`, `classification`, `official_doc_url` or `evidence_gap=true`, `blocked_until_ms`;
- blocked symbols must not enter new entry dispatch until TTL expires or preflight proves resolved.

## Acceptance Criteria

P0 acceptance requires all of the following:

1. `tests/live_harness/` reproduces LYN/OPG/BEAT/BIO incident branches and passes after the fix.
2. Focused legacy suites pass:
   - `python3 scripts/validate_change.py --profile close --keep-going`
   - `python3 scripts/validate_change.py --profile venue-okx --keep-going`
   - `python3 scripts/validate_change.py --profile venue-bybit --keep-going`
3. New independent profile passes:
   - `python3 scripts/validate_change.py --profile live-harness --keep-going`
4. Read-only production probe confirms:
   - no live nonzero positions for repaired symbols unless repair remains intentionally open;
   - no open orders for flat-terminalized symbols;
   - current state no longer has stale `pending_residual_repairs` for flat/dust tasks.
5. Production health after deploy is not green unless recovery work is truly done. A false green is a failed acceptance.
6. Bug docs are updated with:
   - old vs new classification;
   - V1 parity evidence or official-doc evidence;
   - harness/probe command and result;
   - remaining evidence gaps.

## Parallel Workstreams

The work can be split after the fixture contract is frozen:

| Workstream | Scope | Depends on | Does not touch |
|---|---|---|---|
| A | Incident fixture + independent harness/probe profiles | None | Runtime semantics |
| B | Residual repair terminality for LYN/OPG | A fixture contract | Local-L2 data plane |
| C | BEAT/BIO live-flat and duplicate-CID replay acceptance | A fixture contract | OKX residual sizing |
| D | Recovery probe evidence quality | A fixture contract | Entry/close state transitions |
| E | Exchange admission blocks | A fixture contract + official docs | Residual repair state machine |
| F | Local-L2/snapshot replay evidence gate | A fixture contract | Residual/passive close logic |

No workstream may change another workstream's behavior without adding an explicit fixture that proves the cross-boundary effect.
