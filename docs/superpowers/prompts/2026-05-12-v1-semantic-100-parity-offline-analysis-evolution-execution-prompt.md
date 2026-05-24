# V1 Semantic 100% Parity 离线分析与 Evolution 执行提示词

你是接手 LightFeeV2 offline analysis / reports / evolution / LLM evolution 语义复刻的执行智能体。你的任务只覆盖离线分析、报告和演化系统，不要修改 live runtime、venue、执行、风控、persistence/replay。

## 工作目录

```bash
cd /media/wl/新加卷/codex/LightFeeV2
```

## 必读

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
4. `docs/superpowers/plans/2026-05-12-v1-semantic-100-parity-offline-analysis-evolution-implementation-plan.md`
5. record-layer parity matrix：`docs/superpowers/parity/2026-05-11-v1-record-layer-parity-matrix.md`
6. V1 源码：
   - `/media/wl/新加卷/codex/LightFee/src/analysis.rs`
   - `/media/wl/新加卷/codex/LightFee/src/analysis/review_samples.rs`
   - `/media/wl/新加卷/codex/LightFee/src/evolution/*`
   - `/media/wl/新加卷/codex/LightFee/src/llm_evolution/*`
   - `/media/wl/新加卷/codex/LightFee/src/offline_replay/*`

## 你的目标

让 V2 离线分析和 evolution 语义对齐 V1：

- `analyze_journal_records()` 不只统计 entry/exit/order，也要消费 recovery、risk、scan、local-L2、execution diagnostics
- daily/report render 输出稳定、可测试、覆盖 V1 关键摘要
- evolution cycle 不再是只把 status 设为 completed 的壳
- parameter registry、cycle observation、previous action evaluation、cycle run persistence、approval overlay 对齐 V1
- LLM evolution 默认 disabled，显式启用，disabled 状态不做网络调用

## 允许修改

- `lightfee/offline/analysis/journal.py`
- `lightfee/offline/analysis/incident.py`
- `lightfee/offline/reports/daily.py`
- `lightfee/offline/reports/render.py`
- `lightfee/offline/evolution/approval.py`
- `lightfee/offline/evolution/cycle.py`
- `lightfee/offline/evolution/ledger.py`
- `lightfee/offline/evolution/report.py`
- `lightfee/offline/llm_evolution/report.py`
- `lightfee/apps/report.py`
- `lightfee/apps/evolution.py`
- `tests/test_offline_analysis.py`
- `tests/test_evolution.py`

不要改 live execution 或 persistence/replay 模块。

## 严格约束

- 修改任何生产函数/类/方法前，按 `AGENTS.md` 先跑 GitNexus upstream impact。
- 先写失败测试，再写实现。
- 所有 shell 命令前缀使用 `rtk`。
- 不要把 V1 offline/evolution 语义降级成“输出一段 markdown”。
- 不要引入默认网络调用。
- 不要提交，除非调度者明确要求。

## 建议执行顺序

1. 确认索引新鲜度，必要时运行 `rtk npx gitnexus analyze`。
2. 按 plan Task 1-5 执行。
3. 先扩展 journal analysis consumer。
4. 再补 daily/report render。
5. 再补 deterministic evolution cycle。
6. 最后补 LLM evolution disabled/enabled contract。
7. 跑 focused tests。
8. 跑 GitNexus detect_changes。

## 必跑验证

```bash
rtk pytest tests/test_offline_analysis.py -q -W error
rtk pytest tests/test_evolution.py -q -W error
rtk pytest tests/test_evolution.py tests/test_offline_analysis.py -q -W error
rtk python3 -m compileall lightfee tests
```

最后运行：

```text
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

## 最终报告

用中文返回：

- 改了哪些文件
- 对齐了哪些 V1 offline/evolution 语义
- LLM evolution 默认/启用行为是否清楚
- 每条验证命令 exit code
- GitNexus detect_changes 是否只落在 offline/evolution 范围
- 剩余缺口，必须具体到文件/行为

