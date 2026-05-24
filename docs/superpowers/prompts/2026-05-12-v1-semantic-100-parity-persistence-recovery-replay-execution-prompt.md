# V1 Semantic 100% Parity 持久化恢复 Replay 执行提示词

你是接手 LightFeeV2 persistence / recovery / replay 语义复刻的执行智能体。你的任务只覆盖 journal、metrics、snapshot、EngineState recovery、reconciliation、offline replay，不要修改控制面、行情/venue、执行/风控、offline analysis/evolution。

## 工作目录

```bash
cd /media/wl/新加卷/codex/LightFeeV2
```

## 必读

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
4. `docs/superpowers/plans/2026-05-12-v1-semantic-100-parity-persistence-recovery-replay-implementation-plan.md`
5. 旧 record-layer design：`docs/superpowers/specs/2026-05-11-v1-record-layer-full-parity-and-semantic-alignment-design.md`
6. V1 源码：
   - `/media/wl/新加卷/codex/LightFee/src/observability_ops/journal_bridge.rs`
   - `/media/wl/新加卷/codex/LightFee/src/observability_ops/replay_bridge.rs`
   - `/media/wl/新加卷/codex/LightFee/src/runtime_state/*`
   - `/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs`

## 你的目标

让 V2 持久化、恢复和 replay 语义对齐 V1：

- journal envelope、run_id、seq、critical append、Unicode round-trip 对齐
- metrics counters 覆盖 V1 runtime health 和 typed event counters
- snapshot/recovery 能恢复 open positions、pending entries/closes/passive closes、local-L2 retained state
- ambiguous recovery 不压缩成泛型 error，必须保留 diagnostic evidence
- replay 输出固定 schema、timeline、pending counts、risk/recovery/scan evidence
- counterfactual 只基于 recorded evidence，不合成数据
- walk-forward 使用真实日期窗口

## 允许修改

- `lightfee/persistence/journal.py`
- `lightfee/persistence/metrics.py`
- `lightfee/persistence/snapshot_store.py`
- `lightfee/persistence/sqlite_store.py`
- `lightfee/engine/state.py`
- `lightfee/engine/recovery.py`
- `lightfee/engine/reconciliation.py`
- `lightfee/offline/replay/dataset.py`
- `lightfee/offline/replay/engine.py`
- `lightfee/offline/replay/counterfactual.py`
- `lightfee/offline/replay/walk_forward.py`
- `tests/test_persistence.py`
- `tests/test_persistence_replay.py`
- `tests/test_recovery_reconciliation.py`
- `tests/test_engine_recovery.py`

不要改 journal event names，除非同步更新所有 producer/consumer 并在报告中说明。

## 严格约束

- 修改任何生产函数/类/方法前，按 `AGENTS.md` 先跑 GitNexus upstream impact。
- 先写失败测试，再写实现。
- 所有 shell 命令前缀使用 `rtk`。
- 不要丢字段、压缩 evidence、合成 replay 数据。
- 不要提交，除非调度者明确要求。

## 建议执行顺序

1. 确认索引新鲜度，必要时运行 `rtk npx gitnexus analyze`。
2. 按 plan Task 1-4 执行。
3. 先锁 journal envelope 和 metrics。
4. 再锁 snapshot/recovery。
5. 再锁 replay/counterfactual/walk-forward。
6. 跑 persistence focused suite。
7. 跑 GitNexus detect_changes。

## 必跑验证

```bash
rtk pytest tests/test_persistence.py -q -W error
rtk pytest tests/test_engine_recovery.py tests/test_recovery_reconciliation.py -q -W error
rtk pytest tests/test_persistence_replay.py -q -W error
rtk python3 -m compileall lightfee tests
```

最后运行：

```text
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

## 最终报告

用中文返回：

- 改了哪些文件
- 对齐了哪些 V1 journal/recovery/replay 语义
- 是否改动 journal event names 或 shared state model
- 每条验证命令 exit code
- GitNexus detect_changes 是否只落在 persistence/recovery/replay 范围
- 剩余缺口，必须具体到文件/行为

