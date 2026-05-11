# Local-L2 实盘数据平面闭环执行提示词

你是接手 LightFeeV2 的执行智能体。你的任务不是继续做 sidecar-mid repricing，也不是做 paper/shadow trading，也不是重写策略。你的任务是把 Rust V1 已验证过的 local-L2 实盘数据平面语义，复刻到 Python V2 的清晰模块架构里，最终关闭 RT-001/RT-002/P2-L2 drift。

## 一句话任务

以 Rust V1 为唯一业务真源，在 Python V2 中完成：

```text
实盘 WS/REST L2 feed
  -> 本地 L2 order book
  -> local-L2 runtime assignment/lease/event queue
  -> entry-local-L2 session/readiness
  -> execution liquidity
  -> maker-event lane
  -> pending entry hedge 原位 reprice/cancel-replace
  -> persistence/recovery/metrics
```

只有这条链路完成，才算 local-L2 实盘数据平面闭合。

## 必须先读

按顺序读，不要跳：

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-10-local-l2-data-plane-closure-design.md`
4. `docs/superpowers/plans/2026-05-10-local-l2-data-plane-closure-implementation-plan.md`
5. `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
6. `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
7. 当前任务涉及的 Rust V1 源文件
8. 当前任务涉及的 Python V2 目标文件

Rust V1 项目路径：

```text
/media/wl/新加卷/codex/LightFee
```

Python V2 项目路径：

```text
/media/wl/新加卷/codex/LightFeeV2
```

## Rust 真源入口

优先从这些 Rust 文件读：

```text
/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs:4587-4693
/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs:5459+
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime_decision.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_targeting.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_promotion.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_tracking.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_health.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2_sessions.rs
/media/wl/新加卷/codex/LightFee/src/market_gateway/local_l2.rs
/media/wl/新加卷/codex/LightFee/src/market_gateway/local_l2_state_machine.rs
```

## 当前事实

- Python V2 当前 maker-event lane 是 sidecar snapshot mid-price repricing。
- 这不是 Rust V1 local-L2 event-driven 等价实现。
- 不能继续把 sidecar-mid 当作闭环；它最多只能作为明确标注的 non-parity fallback。
- 当前已有轻量 `lightfee/marketdata/l2.py` 和 `lightfee/marketdata/local_book.py`，但缺 sequence/checksum/update/event/runtime/session/recovery 语义。
- 当前已有一批 local-L2 config 字段，但字段多于行为。

## 绝对禁止

- 禁止把 sidecar snapshot 当作 V1 local-L2 execution data plane。
- 禁止用 fake L2 readiness、fake order id、fake price、fake fill 通过测试。
- 禁止把新 reprice 做成新 entry flow，例如 `entry_id="reprice-..."`。
- 禁止把 venue-specific sequence/checksum/depth 规则堆进 `runtime.py`。
- 禁止改策略阈值、风控线、retry/backoff、精度、下单语义。
- 禁止吞异常后静默降级。
- 禁止只改测试来适配错误实现。
- 禁止未做 GitNexus impact analysis 就改生产函数、类或方法。

## 模块归属

按这个边界落代码：

```text
lightfee/marketdata/l2.py
  本地 L2 book、update、event、status、纯 book 操作

lightfee/marketdata/local_book.py
  book 查询、过滤、计数 helper

lightfee/marketdata/local_l2_venues.py
  venue L2 normalization、sequence/checksum/depth 规则

lightfee/marketdata/local_l2_runtime.py
  assignments、leases、budget、events queue、runtime fault、metrics

lightfee/venues/*.py / lightfee/venues/transport.py
  真实 venue REST/WS 接入和 payload 解析表面

lightfee/engine/entry_local_l2.py
  tracked opportunities、primary/shadow sessions、readiness、diagnostics

lightfee/engine/entry_sync.py
  pending entry hedge 原位 reprice/cancel-replace/fallback

lightfee/engine/runtime.py
  只做 lane scheduling 和 orchestration

lightfee/apps/live.py
  private -> market -> local-L2 phased startup wiring

lightfee/engine/recovery.py / persistence
  retained books、sessions、leases、resume waiting、metrics 的恢复
```

## 执行顺序

严格按 plan 做：

1. Task 0: Baseline And Drift Lock
2. Task 1: Upgrade Local L2 Book Core
3. Task 2: Add Venue L2 Normalization Layer
4. Task 3: Build Local-L2 Runtime Service
5. Task 4: Implement Entry Local-L2 Sessions
6. Task 5: Connect Execution Liquidity To Local L2
7. Task 6: Rewrite Maker-Event Lane To Consume Local-L2 Events
8. Task 7: Add Live Startup Local-L2 Phase
9. Task 8: Persist And Recover Local-L2 State
10. Task 9: Metrics, Docs, And Final Closure

不要提前做 Task 6。maker-event lane 必须等 book core、runtime、entry-local-L2 session、execution liquidity 基础稳定后再接。

## 每个任务固定流程

1. 读 spec/plan 对应任务。
2. 读对应 Rust V1 真源。
3. 用 GitNexus 查 Python 影响面。
4. 先写能暴露 drift 的 focused test。
5. 实现 Python，结构可以优化，业务语义必须等价。
6. 跑 touched tests。
7. 更新 parity matrix 和 closure report。
8. 最后跑全量验证。

修改生产 symbol 前必须执行类似：

```text
gitnexus_impact({target: "LocalL2Book", direction: "upstream", repo: "LightFeeV2"})
```

收尾前必须执行：

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

## 必须证明的验收点

- local-L2 update 能处理 snapshot、delta、zero-size delete、sequence gap、checksum mismatch、stale age。
- venue L2 normalization 有每家交易所 fixture。
- local-L2 runtime 有 assignment lease preserve/expire、budget suspension、runtime fallback、resume expiry、event draining、metrics。
- entry-local-L2 有 primary/shadow、per-leg readiness、quiet book grace、readiness downgrade、promotion/demotion。
- maker-event lane 不读 sidecar 也能被 local-L2 event 唤醒。
- maker-event lane 只推进原 pending entry hedge，不创建新 entry flow。
- live startup 有 private -> market -> local-L2 phased activation。
- recovery 不会把未知或过期 local-L2 状态恢复成 ready。
- docs 如实标记 fixed/partial/open，不能假闭环。

## 验证命令

每个任务至少跑 touched tests。最终必须跑：

```bash
rtk pytest -q
rtk pytest -q -W error
rtk python3 -m compileall lightfee tests
```

如果改了 async runtime、entry_sync、venue transport，还要跑相关 focused tests，例如：

```bash
rtk pytest tests/test_runtime_maker_event_local_l2.py -q -W error
rtk pytest tests/test_local_l2_runtime.py tests/test_entry_local_l2.py -q -W error
rtk pytest tests/test_live_startup_preflight.py -q -W error
```

## 并行分工建议

可以并行：

- Worker A: Task 1，book core。
- Worker B: Task 2，venue normalization。
- Worker C: Task 3，local-L2 runtime。
- Worker D: Task 4，entry-local-L2 sessions。

必须串行整合：

```text
Task 1 -> Task 3 -> Task 5 -> Task 6
Task 4 -> Task 5 -> Task 6
Task 7/8 在 Task 3 API 稳定后做
Task 9 最后做
```

不要让多个智能体同时改 `lightfee/engine/runtime.py` 和 `lightfee/engine/entry_sync.py`。

## 最终报告格式

按这个格式回复：

```text
结论：local-L2 实盘数据平面已闭合 / 未闭合

已完成：
- ...

仍未完成：
- ...

Rust 对齐证据：
- Rust: ...
- Python: ...

验证：
- rtk pytest -q: ...
- rtk pytest -q -W error: ...
- rtk python3 -m compileall lightfee tests: ...

GitNexus:
- impact: ...
- detect_changes: ...

风险：
- ...
```
