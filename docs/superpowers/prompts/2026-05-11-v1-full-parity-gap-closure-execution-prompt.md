# V1 Full Parity Gap Closure 执行提示词

你是接手 LightFeeV2 的执行智能体。你的任务不是继续打单点补丁，而是按 spec/plan 把 Rust V1 剩余 live-path 缺口按模块闭环。

## 必读

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-11-v1-full-parity-gap-closure-design.md`
4. `docs/superpowers/plans/2026-05-11-v1-full-parity-gap-closure-implementation-plan.md`
5. Rust 真源：`/media/wl/新加卷/codex/LightFee`

## 核心目标

- 补齐 worker ownership
- 补齐 startup / recovery / shutdown 边界
- 补齐 canonical symbol authority
- 补齐 local-L2 runtime / data-plane / maker-event 边界
- 补齐 venue capability truth
- 只在 fresh verification 通过后更新 parity matrix / closure report

## 严格约束

- 修改任何生产符号前，先跑 GitNexus impact，并把 blast radius 报给用户
- 提交前跑 `gitnexus_detect_changes()`
- 先写失败测试，再写实现
- 不得用 sidecar / shadow / mock / fake path 假装闭环
- 不得改阈值、重试、精度、风控语义，除非 Rust V1 明确证明
- `runtime.py` 只做 orchestration，不要继续堆业务规则
- 任何 `fixed` 只能来自本次 fresh 验证

## 执行顺序

1. 建/更新 gap registry
2. 修 worker ownership / lifecycle
3. 修 canonical symbol 边界
4. 修 runtime / recovery / maker-event 边界
5. 修 venue capability truth
6. 只在代码与测试通过后更新文档
7. 跑完整验证命令并输出 exit code

## 验收命令

必须贴出结果：

```bash
rtk timeout 25s pytest tests/test_live_startup_preflight.py -q -W error
rtk pytest tests/test_local_l2_ws.py tests/test_local_l2_runtime.py tests/test_live_startup_preflight.py tests/test_runtime_maker_event_local_l2.py tests/test_marketdata_l2.py -q -W error
rtk pytest tests/test_venues_transport.py tests/test_venues_base.py tests/test_risk_actions.py -q -W error
rtk python3 -m compileall lightfee tests
rtk pytest -q -W error
```

## 最终报告

必须用中文，明确写：

- 已完成哪些模块
- 仍有哪些缺口
- 每个验证命令的退出码
- 若未闭环，具体到文件与行为，不要泛泛而谈
