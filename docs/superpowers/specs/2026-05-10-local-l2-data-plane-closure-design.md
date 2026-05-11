# Local-L2 实盘数据平面闭环专项 Spec

**Date:** 2026-05-10

**Rust source of truth:** `/media/wl/新加卷/codex/LightFee`

**Python target:** `/media/wl/新加卷/codex/LightFeeV2`

**Related parity docs:**

- `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
- `docs/superpowers/specs/2026-05-10-v1-v2-module-parity-closure-design.md`

---

## 结论

是。若目标是 Rust V1 live-path 的严格闭环，Python V2 现在必须先把整套实盘 local-L2 数据平面做好。

当前 Python 的 maker-event lane 已经不是空实现，但它依赖 sidecar snapshot mid-price 做被动 maker repricing。这只能证明“有一个简化 repricing lane”，不能证明与 Rust V1 的 local-L2 depth event-driven lane 等价。Rust V1 的 maker-event lane 依赖真实 local-L2 数据面：本地订单簿、事件队列、运行时 assignment/lease、entry-local-L2 readiness、resume/rebuild、fallback、metrics，以及对 pending entry hedge 的原位驱动。

因此 RT-001/RT-002/P2-L2 的真实闭环标准不是“再补几个 if”，而是：

```text
实盘 WS/REST L2 feed -> 本地 L2 book -> local-L2 runtime 状态机
-> entry-local-L2 session/readiness -> execution liquidity / maker-event lane
-> pending entry hedge 原位 reprice/cancel-replace -> persistence / recovery / metrics
```

## Source Of Truth

Rust V1 必读路径：

| Area | Rust Reference |
| --- | --- |
| Maker-event lane | `/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs:4587-4693` |
| Pending entry hedge driver | `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs:5459+` |
| Entry local-L2 sessions | `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2.rs` |
| Session readiness/state | `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2_sessions.rs` |
| Runtime sync/fallback/events | `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime.rs` |
| Runtime decisions/log throttling | `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime_decision.rs` |
| Assignment and targeting | `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_targeting.rs` |
| Promotion and tracking | `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_promotion.rs`, `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_tracking.rs` |
| Health and status | `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_health.rs` |
| Gateway L2 state machine | `/media/wl/新加卷/codex/LightFee/src/market_gateway/local_l2.rs`, `/media/wl/新加卷/codex/LightFee/src/market_gateway/local_l2_state_machine.rs` |
| Startup note and config visibility | `/media/wl/新加卷/codex/LightFee/src/app_runtime/bootstrap.rs:185-249` |
| Metrics export | `/media/wl/新加卷/codex/LightFee/src/main.rs:897-1235`, `/media/wl/新加卷/codex/LightFee/src/app_runtime/loop_control.rs:822-1030` |

Python V2 当前相关路径：

| Area | Python Reference | Current State |
| --- | --- | --- |
| Basic L2 model | `lightfee/marketdata/l2.py` | 只有轻量状态机、levels、pool；缺 sequence/checksum/update/event/runtime 语义 |
| Book selectors | `lightfee/marketdata/local_book.py` | 只有过滤和计数 helper |
| Runtime maker-event | `lightfee/engine/runtime.py:_maybe_tick_maker_event` | sidecar-mid repricing，非 Rust local-L2 event-driven |
| Lifecycle phase enum | `lightfee/engine/lifecycle.py` | 有 `LOCAL_L2` phase enum，但未承载 V1 启动激活语义 |
| Config fields | `lightfee/config/schema.py` | 已有一批 local-L2 字段，字段多于行为 |
| Existing tests | `tests/test_marketdata_l2.py`, `tests/test_live_full_closure.py` | 覆盖轻量模型和 sidecar-mid lane；未覆盖 V1 local-L2 数据平面 |

## Non-Goals

- 不做 Paper Trading / Shadow Trading 级闭环替代，不把“观测模式”当作实盘闭环。
- 不用 sidecar snapshot 替代 local-L2 execution data plane。
- 不改策略阈值、资金费率套利逻辑、风险线、下单精度、retry/backoff 数值，除非 Rust V1 证据支持。
- 不机械复制 Rust 的 `Arc`、`Mutex`、channel 结构；Python 可以用更清晰的 dataclass、async task、service 边界。
- 不把 venue-specific L2 patch 堆进 `runtime.py`。

## Closure Definition

本专项完成后，以下能力必须全部成立：

1. **Local L2 Book Core**
   - 每个 venue/symbol 有本地订单簿，支持 snapshot、delta update、best bid/ask、depth slice、age、staleness、readiness。
   - 记录 update id、sequence、checksum、last event time、last good snapshot time。
   - 能区分 `HOT`、`BOOTSTRAPPING`、`DEGRADED`、`REBUILDING`、`SUSPENDED`、resume-waiting。

2. **Venue L2 Normalization**
   - Binance、OKX、Bybit、Bitget、Gate、Aster、Hyperliquid 的 L2 消息进入统一 update model。
   - venue-specific sequence gap、checksum mismatch、depth 限制、symbol format、rebuild/resubscribe 规则由 `lightfee/venues/` 或 `lightfee/marketdata/local_l2_venues.py` 持有。

3. **Local-L2 Runtime**
   - 负责 assignment、lease、budget、runtime suspension、rate-limited、transport failure、fallback、resume expiry、rebuild 计数。
   - 负责 draining local-L2 events，并把相关事件推给 maker-event lane。
   - 负责日志节流与诊断字段，事件名与 Rust live semantics 对齐。

4. **Entry Local-L2 Sessions**
   - 跟踪 primary/shadow opportunities、tracked legs、arming/ready/not-ready reason。
   - 实现 prewarm window、quiet book grace、per-leg freshness、readiness downgrade、shadow promotion、primary demotion、assignment lease preserve/expire。
   - entry final gate 使用 session readiness，而不是只看 sidecar 或 top-book。

5. **Execution Liquidity Source**
   - entry/delever/close 的可成交深度需要能从 local-L2 生成 `ExecutionLiquiditySnapshot`。
   - Rust V1 不允许 fallback 的路径，Python 也不得用 top-book 或 cached value 悄悄放行。
   - fallback 必须产生明确 reason、journal、metric。

6. **Maker-Event Lane**
   - `_maybe_tick_maker_event()` 不再以 sidecar mid-price 为核心触发源。
   - 它必须像 Rust V1 一样：刷新 execution local-L2 assignments，sync runtime，finalize entry-local-L2 readiness，drain pending local-L2 events，过滤与 pending entry 相关的事件，调用 pending entry hedge driver 原位推进。
   - 不得通过新建一个独立 `EntryContext(entry_id="reprice-...")` 来模拟 V1 的 pending hedge reprice。

7. **Live Startup Phasing**
   - live startup 必须有 private streams -> market streams -> local-L2 的 phased activation。
   - local-L2 phase 有 timeout、background startup、resume retained books、readiness/fail-closed 或 degraded policy。

8. **Persistence And Recovery**
   - retained books、assignments、session readiness、resume waiting、metrics counters、pending events 的必要状态能进入 snapshot/journal。
   - recovery 后不会把未知 local-L2 状态当作 ready。

9. **Observability**
   - 暴露 Rust V1 对应的 local-L2/entry-local-L2 metrics。
   - journal 至少覆盖：status transition、venue mode change、sequence gap、checksum mismatch、transport fault、quote age triggered、idle timeout、resume expired、budget enforced、assignment empty/preserved/expired、maker-event wake。

## Module Ownership

| Module | Owns | Must Not Own |
| --- | --- | --- |
| `lightfee/config/schema.py`, `lightfee/config/validation.py` | local-L2 config fields, defaults, validation, legacy compatibility | runtime state transitions |
| `lightfee/marketdata/l2.py` | book/status/update/event dataclasses and pure book operations | venue HTTP/WS payload quirks |
| `lightfee/marketdata/local_book.py` | query/filter helpers over books | mutation-heavy runtime orchestration |
| `lightfee/marketdata/local_l2_runtime.py` | assignments, leases, budgets, events queue, metrics, runtime sync decisions | entry strategy formulas or order submission |
| `lightfee/marketdata/local_l2_venues.py` | venue L2 normalization rules and checksum/sequence policy | adapter signing or private order APIs |
| `lightfee/venues/*.py`, `lightfee/venues/transport.py` | actual REST snapshot, WS subscription, venue-specific event parsing surfaces | strategy readiness decisions |
| `lightfee/engine/entry_local_l2.py` | tracked opportunities, sessions, readiness, diagnostics | raw WS parsing |
| `lightfee/engine/entry_sync.py` | pending entry hedge reprice/cancel-replace/fallback execution | local-L2 assignment selection |
| `lightfee/engine/runtime.py` | lane scheduling and service orchestration | venue-specific patch logic, book mutation internals |
| `lightfee/apps/live.py` | startup phase wiring and operator-visible preflight | business formulas |
| `lightfee/engine/recovery.py`, `lightfee/persistence/*` | replay and snapshot restoration of local-L2 state | live WS subscription logic |

## Drift Rules

- Sidecar-mid repricing may remain as a non-parity fallback only behind an explicit config name and journal reason. It cannot be the default V1 parity implementation.
- If Python compresses several Rust patch branches into one cleaner helper, tests must cover each original live branch.
- If a Rust venue lacks reliable local-L2 support, Python must mark that venue/symbol as unsupported/degraded with explicit reason; it must not emit fake ready state.
- A local-L2 update with invalid sequence/checksum/stale age must never be silently accepted.
- A pending entry reprice must mutate the pending-entry hedge flow, not create a second independent entry flow.
- `runtime.py` may call local-L2 services; it must not become the home of sequence gap, checksum, venue depth, or session readiness rules.

## Acceptance Tests

Minimum test set:

| Test File | Required Coverage |
| --- | --- |
| `tests/test_marketdata_l2.py` | snapshot/delta application, sorted depth, zero-size delete, sequence gap, checksum mismatch, stale age, status transitions |
| `tests/test_local_l2_venue_rules.py` | venue-specific sequence/checksum/depth normalization fixtures for supported venues |
| `tests/test_local_l2_runtime.py` | assignment lease preserve/expire, budget suspension, runtime fallback, resume expiry, event draining, metrics counters |
| `tests/test_entry_local_l2.py` | primary/shadow tracking, per-leg readiness, quiet book grace, readiness downgrade, shadow promotion/demotion |
| `tests/test_runtime_maker_event_local_l2.py` | maker-event lane drains local-L2 events and drives original pending entry hedge; no sidecar-only reprice path in parity mode |
| `tests/test_live_startup_preflight.py` | private -> market -> local-L2 phased startup and timeout/degraded behavior |
| `tests/test_recovery_reconciliation.py` or new recovery test | local-L2 retained state restores without false-ready |
| `tests/test_live_full_closure.py` | integration smoke proving runtime wiring consumes the new services |

Whole-repo verification:

```bash
rtk pytest -q
rtk pytest -q -W error
rtk python3 -m compileall lightfee tests
```

## Why This Was Not Closed By The Previous Patch

Previous fixes closed P0 risk health, planner config drift, reconciliation backoff, SIGHUP/rate-limit reload, and several venue/config defects. The remaining local-L2 gap is different in kind:

- It requires a data plane, not a scalar price read.
- It requires event-driven pending hedge progression, not a fresh entry execution.
- It requires venue-specific sequence/checksum/rebuild behavior, not a generic mid-price comparison.
- It reaches startup, runtime, marketdata, entry sessions, venues, recovery, persistence, and metrics at once.

So “短期无法闭合”的准确含义是：不能通过修 `_maybe_tick_maker_event()` 这一处快速闭合。作为一个独立专项，按本 spec 分阶段实现后可以闭合。
