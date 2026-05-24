# V1 Semantic 100% Parity 控制面执行提示词

你是接手 LightFeeV2 控制面语义复刻的执行智能体。你的任务只覆盖 startup / shutdown / runtime loop / backoff / export / signal handling，不要修改行情、venue、执行、风控、持久化、offline/evolution 业务模块。

## 工作目录

```bash
cd /media/wl/新加卷/codex/LightFeeV2
```

## 必读

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
4. `docs/superpowers/plans/2026-05-12-v1-semantic-100-parity-control-plane-implementation-plan.md`
5. V1 源码：
   - `/media/wl/新加卷/codex/LightFee/src/main.rs`
   - `/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs`

## 你的目标

让 V2 控制面语义对齐 V1：

- startup 阶段顺序和边界清楚
- shutdown 总是停止 worker、flush 状态、关闭 journal
- runtime loop 调度 V1 对应 lane
- full tick / active tick / maker-event / rate-limit reload 都有独立 backoff 或门控语义
- post-tick housekeeping、metrics export、current-state export 语义完整
- 信号处理不吞异常、不漏 stop

## 允许修改

- `lightfee/apps/live.py`
- `lightfee/engine/runtime.py`
- `lightfee/engine/bootstrap.py`
- `lightfee/engine/loop_control.py`
- `tests/test_live_startup_preflight.py`
- `tests/test_runtime_smoke.py`
- `tests/test_control_plane.py`

不要修改其他 workstream 文件，除非你先在最终报告里明确说明原因。

## 严格约束

- 修改任何生产函数/类/方法前，按 `AGENTS.md` 先跑 GitNexus upstream impact。
- 先写失败测试，再写实现。
- 所有 shell 命令前缀使用 `rtk`。
- 不要用 fake/paper/shadow 语义替代 live 控制面。
- 不要把业务规则塞进 `runtime.py`；runtime 只做 orchestration。
- 不要提交，除非调度者明确要求。

## 建议执行顺序

1. 确认索引新鲜度，必要时运行 `rtk npx gitnexus analyze`。
2. 阅读 plan 中 Task 1-4。
3. 为 startup/shutdown 顺序写测试。
4. 为 lane 调度和 backoff 写测试。
5. 为 tick error、export、final stop 写测试。
6. 实现最小控制面修复。
7. 跑 focused tests。
8. 跑 GitNexus detect_changes。

## 必跑验证

```bash
rtk pytest tests/test_live_startup_preflight.py -q -W error
rtk pytest tests/test_runtime_smoke.py -q -W error
rtk pytest tests/test_control_plane.py -q -W error
rtk python3 -m compileall lightfee tests
```

最后运行：

```text
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

## 最终报告

用中文返回：

- 改了哪些文件
- 对齐了哪些 V1 控制面语义
- 哪些测试新增/修改
- 每条验证命令 exit code
- GitNexus detect_changes 是否只落在控制面范围
- 剩余缺口，必须具体到文件/行为

