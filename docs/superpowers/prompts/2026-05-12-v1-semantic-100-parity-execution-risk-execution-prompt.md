# V1 Semantic 100% Parity 执行与风控执行提示词

你是接手 LightFeeV2 entry / exit / passive close / residual / risk 语义复刻的执行智能体。你的任务只覆盖交易执行和风险动作，不要修改控制面、行情/venue、persistence/replay、offline/evolution。

## 工作目录

```bash
cd /media/wl/新加卷/codex/LightFeeV2
```

## 必读

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
4. `docs/superpowers/plans/2026-05-12-v1-semantic-100-parity-execution-risk-implementation-plan.md`
5. 可参考旧 passive close plan：`docs/superpowers/plans/2026-05-11-v1-passive-close-gap-closure-implementation-plan.md`
6. V1 源码：
   - `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs`
   - `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_execution_planner.rs`
   - `/media/wl/新加卷/codex/LightFee/src/engine/entry.rs`
   - `/media/wl/新加卷/codex/LightFee/src/engine/exit.rs`
   - `/media/wl/新加卷/codex/LightFee/src/engine/risk.rs`
   - `/media/wl/新加卷/codex/LightFee/src/engine/supervision.rs`

## 你的目标

让 V2 执行与风控语义对齐 V1：

- entry planner 决定 route、slice、maker leg，不由 runtime 硬编码
- maker leg post-only GTC，hedge leg IOC
- partial fill / hedge reject / uncertain outcome 进入正确 pending/recovery 语义
- close reason 正确路由 passive maker+taker 或 aggressive IOC
- passive close 独立于 PendingClose，保留 chunk、delta hedge、repricing、fallback、recovery
- risk warning/death/delever/single-side protection 产生真实动作和完整 journal payload
- 不因测试方便改变阈值、精度、风控条件

## 允许修改

- `lightfee/engine/entry.py`
- `lightfee/engine/entry_sync.py`
- `lightfee/engine/execution_planner.py`
- `lightfee/engine/entry_local_l2.py`
- `lightfee/engine/exit.py`
- `lightfee/engine/exit_decision.py`
- `lightfee/engine/close_executor.py`
- `lightfee/engine/passive_close.py`
- `lightfee/engine/reconciliation.py`
- `lightfee/engine/residual.py`
- `lightfee/engine/risk_actions.py`
- `lightfee/engine/supervisor.py`
- `tests/test_entry_sync.py`
- `tests/test_runtime_entry_flow.py`
- `tests/test_entry_planner.py`
- `tests/test_close_execution.py`
- `tests/test_passive_close.py`
- `tests/test_exit_decisions.py`
- `tests/test_risk_actions.py`
- `tests/test_supervisor_execution.py`

若必须改 `lightfee/core/domain.py` 或 `lightfee/engine/state.py`，最终报告里必须标为 shared contract change。

## 严格约束

- 修改任何生产函数/类/方法前，按 `AGENTS.md` 先跑 GitNexus upstream impact。
- 先写失败测试，再写实现。
- 所有 shell 命令前缀使用 `rtk`。
- 不要用 fake/paper/shadow path 冒充 live 执行闭环。
- 不要把 passive close 降级成 aggressive close 的 reason 字符串。
- 不要提交，除非调度者明确要求。

## 建议执行顺序

1. 确认索引新鲜度，必要时运行 `rtk npx gitnexus analyze`。
2. 按 plan Task 1-4 执行。
3. 先补 entry planner / entry_sync。
4. 再补 close / passive close。
5. 再补 risk / supervisor。
6. 跑 execution focused suite。
7. 跑 GitNexus detect_changes。

## 必跑验证

```bash
rtk pytest tests/test_entry_planner.py tests/test_entry_sync.py tests/test_runtime_entry_flow.py -q -W error
rtk pytest tests/test_close_execution.py tests/test_passive_close.py tests/test_exit_decisions.py -q -W error
rtk pytest tests/test_risk_actions.py tests/test_supervisor_execution.py -q -W error
rtk python3 -m compileall lightfee tests
```

最后运行：

```text
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

## 最终报告

用中文返回：

- 改了哪些文件
- 对齐了哪些 V1 entry/close/risk 语义
- 是否改动 shared contract
- 每条验证命令 exit code
- GitNexus detect_changes 是否只落在 execution/risk 范围
- 剩余缺口，必须具体到文件/行为

