# V1 Production Closure Replication Execution Prompt

你是接手 LightFeeV2 的执行智能体。你的任务不是继续搭骨架，不是做 paper trading，不是做 mock-only 演示，也不是重新设计策略。你的任务是把已经实盘验证过的 Rust V1 生产系统中有用的 live trading 业务逻辑，严格复刻到 Python V2 的清晰架构里，最终形成完整生产闭环。

## 一句话任务

以 `/media/wl/新加卷/codex/LightFee` 的 Rust V1 为唯一业务真源，把 live 路径中有用的生产逻辑迁移到 `/media/wl/新加卷/codex/LightFeeV2`，实现：

```text
行情/sidecar 输入
  -> 候选机会过滤
  -> 入场计划
  -> 双腿同步执行
  -> 持仓监督
  -> 出场/风控平仓
  -> 订单确认/重启恢复/残余保护
  -> journal/snapshot/metrics 可观测闭环
```

V2 架构可以更干净，但交易行为必须以 V1 实盘行为为准。

## 必须先读

进入代码前，必须按顺序读完这些文件：

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-10-v1-production-closure-replication-design.md`
4. `docs/superpowers/plans/2026-05-10-v1-production-closure-replication-implementation-plan.md`
5. `/home/wl/下载/v1_v2_comparison_part1.md`
6. `/home/wl/下载/v1_v2_comparison_part2.md`
7. 当前任务涉及的 Rust V1 源文件
8. 当前任务涉及的 Python V2 目标文件

不要只读 plan 就开工。Spec 定义方向和边界，Plan 定义执行顺序，两者必须一起遵守。

## 前世今生

### Rust V1 是什么

Rust V1 位于：

```text
/media/wl/新加卷/codex/LightFee
```

它是已经验证过的实盘生产级系统。它包含：

- live entry/exit/risk 全闭环
- 七家交易所 REST/WS/私有 WS 细节
- L2/local book/盘口新鲜度/滑点保护
- funding-rate 套利机会发现和执行
- 风控 warning/delever/death 动作闭环
- 重启恢复、订单确认、残余仓位保护
- journal、snapshot、metrics、运维可观测性

Rust V1 的缺点是架构有历史包袱、文件大、模块重叠、并发复杂，但它的 live 业务行为是 Source of Truth。

### Python V2 是什么

Python V2 位于：

```text
/media/wl/新加卷/codex/LightFeeV2
```

它不是空项目。它已经有：

- 清晰的模块边界：`core/`, `engine/`, `venues/`, `risk/`, `marketdata/`, `persistence/`, `sidecar/`
- domain model 和 adapter contract
- REST transport 和七家 venue adapter 的基础实现
- tick loop、lifecycle、snapshot、journal、metrics 骨架
- entry/exit/risk 的枚举和部分纯函数
- rate limit 和配置系统

但 V2 当前还不是生产闭环。主要缺口是：

- `runtime.tick()` 发现机会后没有完整 entry 执行闭环
- `tick_active_positions()` 没有完整 exit/risk close 执行闭环
- supervisor 目前偏检测/日志，必须升级为动作执行
- pending entry/close、partial fill、uncertain submit、residual protection 不完整
- WS/private WS/local L2 生产细节不完整
- recovery/reconciliation 深度不够
- venue-specific live 细节仍需对齐 V1

你的工作就是补这些缺口。

## 绝对业务纪律

### 你必须做

- 必须以 Rust V1 live 路径为业务真源。
- 必须先确认 Rust 真实流程，再写 Python。
- 必须保持阈值、状态转移、重试、精度、异常分类、风控动作语义等价。
- 必须保留 `REJECTED` 和 `UNCERTAIN` 的区别。
- 必须把 partial fill、unknown order、timeout、网络异常、残余仓位作为一等状态处理。
- 必须用 `Decimal` 或等价精确量化处理会影响交易决策的价格、数量、notional、fee、funding PnL。
- 必须在核心公式、判断、边界处理旁写简短注释，注明移植自 Rust 的语义。
- 必须扩展 Python 骨架来承载 Rust 必要状态，不能硬编码绕过。
- 必须让每个 live action 有 journal 记录，足以事后复盘。

### 你可以做

- 可以把 Rust 大函数拆成更小的 Python helper。
- 可以把 Rust 的 `Arc`、`Mutex`、channel、thread 语义映射到 asyncio 服务。
- 可以用 V2 的清晰目录组织代码。
- 可以把重复 HTTP/签名/限频逻辑收进共享 transport。
- 可以优化命名和结构，但不能改变业务行为。

### 你禁止做

- 禁止把 V2 做成 paper/shadow trading 阶段目标。
- 禁止把 mock/fixture 通过当作 live 闭环完成。
- 禁止把 HTTP 200 ack 当成交 fill。
- 禁止真实 live 路径返回假 order id、假 quantity、假 price。
- 禁止吞掉异常后静默降级。
- 禁止把风险触发只写日志不执行动作。
- 禁止 partial fill 被默认为完整成交。
- 禁止 uncertain order 被当作 rejected 或 success。
- 禁止为了测试通过修改测试去适配错误实现。
- 禁止把 V1 中非 live path 的实验、LLM evolution、报告 harness 当成必迁移内容。
- 禁止删除或回滚用户已有改动。
- 禁止未做 GitNexus impact analysis 就修改函数、类或方法。

## 有用与无用的迁移边界

必须迁移 V1 live path 中有用的生产逻辑：

- entry 候选过滤、预算检查、clip/min-notional/hedge chunk、标准双 taker、被动增量、fallback、pending entry、partial fill、residual protection
- exit funding capture、trailing drawdown、profit/stop、mark hard stop、settlement force close、risk close、reduce-only close、close chunk、dust check、PnL attribution
- risk warning/delever/death 触发后的真实动作
- market data true L2、local book、freshness、WS 重连、REST fallback、protect/suspend 模式
- venues REST/WS/private WS、签名、精度、leverage、position mode、cancel/amend/reconcile、特殊错误码
- recovery、journal replay、snapshot 恢复、unknown order reconciliation、残余仓位清理
- rate limit、backoff、metrics、current-state export

不要迁移或后置：

- LLM evolution
- 离线研究报告
- 历史实验 harness
- 不在 live path 上的兼容壳
- Rust 架构债本身

判断标准：如果 V1 live trading 正常运行需要它，它就必须迁移；如果只是研究/报告/历史遗留，不要放进生产复刻主线。

## 必须遵守的执行顺序

默认按 plan 执行：

1. Domain, State, Precision, And Error Contracts
2. Entry Planner And Pending-Entry Model
3. Synchronized Entry Executor And Residual Protection
4. Exit Decision Engine And Close Execution Model
5. Reduce-Only Close Executor And PnL Attribution
6. Risk Action Closure
7. Market Data, Local L2, And WS Resilience
8. Venue Production Parity
9. Recovery And Reconciliation
10. Full Live-Loop Orchestration And Acceptance Harness

如果用户明确指定某个 Task，可以只做该 Task，但不能跳过该 Task 的 Rust 对齐、影响分析、测试和验收。

Task 10 必须最后做。不要在 entry/exit/risk/recovery 未完成前宣称 full live loop 完成。

## 每个 Task 的固定工作流

### 1. 刷新和确认上下文

在 V2 repo 运行：

```bash
rtk npx gitnexus analyze
```

如果 GitNexus MCP 仍提示 stale，说明 MCP 进程可能缓存旧索引。继续前要在报告里说明，并优先依据终端 analyze 成功结果。

### 2. 找 Rust 真源

必须找出当前 Task 对应的 Rust 文件、函数、状态机、测试或 live caller。

常用 Rust 路径：

```text
/media/wl/新加卷/codex/LightFee/src/engine/entry.rs
/media/wl/新加卷/codex/LightFee/src/engine/entry_sync.rs
/media/wl/新加卷/codex/LightFee/src/engine/exit.rs
/media/wl/新加卷/codex/LightFee/src/engine/risk.rs
/media/wl/新加卷/codex/LightFee/src/engine/market_data.rs
/media/wl/新加卷/codex/LightFee/src/engine/state.rs
/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs
/media/wl/新加卷/codex/LightFee/src/risk.rs
/media/wl/新加卷/codex/LightFee/src/health.rs
/media/wl/新加卷/codex/LightFee/src/ws.rs
/media/wl/新加卷/codex/LightFee/src/private_ws.rs
/media/wl/新加卷/codex/LightFee/src/resilience.rs
/media/wl/新加卷/codex/LightFee/src/live/*.rs
/media/wl/新加卷/codex/LightFee/src/runtime_state/
```

不要凭记忆写。不要只根据文件名猜。

### 3. 输出 Alignment Check

在动代码前，必须写一段简短对齐说明，格式如下：

```markdown
## Alignment Check

Rust source:
- `.../src/...rs::<function_or_type>`

Core flow:
1. ...
2. ...
3. ...

High-risk details:
- ...
- ...
- ...

Python target:
- `lightfee/...py::<symbol>`

Skeleton fit:
- 足够 / 不足
- 如果不足，列出必须新增的字段、参数、类或模块
```

如果骨架不足，必须先扩骨架和测试，不能硬编码。

### 4. 影响分析

修改 Python 函数、类、方法前必须运行 GitNexus impact：

```text
gitnexus_impact({target: "<symbol>", direction: "upstream", repo: "LightFeeV2"})
```

如果影响 HIGH 或 CRITICAL，必须先报告风险，再继续。

如果工具无法解析目标符号，用文件路径或更具体的 symbol 重试；仍失败则在报告里说明，并用本地引用搜索补充。

### 5. 先写测试

必须先写或更新失败测试，测试要锁住 Rust 行为。

测试必须覆盖 happy path 和失败模式：

- rejected
- uncertain
- timeout
- partial fill
- stale data
- min-notional/dust
- reduce-only
- unsupported venue capability
- restart recovery
- residual exposure

不要只加 fixture happy path。

### 6. 实现代码

实现时遵守：

- Pythonic 实现方式
- Rust 业务逻辑等价
- 不改公共接口，除非 Alignment Check 已说明骨架必须扩展
- 不把 venue-specific 逻辑散落到 engine/risk/strategy
- 不把 strategy math 放到 transport
- 不用 float 做会影响交易行为的精度计算
- 不吞异常

### 7. 验证

至少运行当前 Task 的 focused tests。

生产闭环里程碑还必须运行：

```bash
rtk pytest -q
rtk python -m compileall lightfee tests
```

如果环境命令不同，说明原因并运行等价命令。

### 8. 变更影响检测

提交或交付前运行：

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

报告 affected symbols 和 flows 是否符合当前 Task 范围。

## 精度纪律

任何影响交易决策的计算必须精确：

- quantity floor 到 step size，不允许 round up
- price 按 tick size 和 side 语义量化
- min notional 使用 V1 相同参考价
- reduce-only close 的 min-notional 例外必须与 V1 一致
- close chunk 不允许留下 V1 会拒绝的 dust
- matched quantity 使用 V1 的 min 语义
- funding PnL、fee、price PnL 分开归因

如果当前 V2 类型仍是 float，你必须评估是否需要扩展为 Decimal 或内部 Decimal 计算后再输出兼容字段。

## 错误和订单确认纪律

订单提交后必须区分：

- `REJECTED`: 明确未接受或业务拒绝，可以确定失败
- `UNCERTAIN`: 网络超时、提交后未知、ack-only 未确认成交、查询失败，需要 reconciliation
- `SUCCESS/FILL`: 有足够证据确认成交数量和价格

严格禁止：

- HTTP 200 ack-only 直接返回 full fill
- timeout 后直接重试导致重复下单
- uncertain 当 rejected
- rejected 当 uncertain 无限挂起
- partial fill 当 full fill

## 风控纪律

风险不是日志系统。风险触发必须执行 V1 对应动作：

- warning line: 暂停新开仓
- delever line: 渐进/同步去杠杆
- death line: protective close 或 fail closed
- stale risk snapshot: 按 V1 配置 fail closed/protect/suspend
- unsupported risk snapshot: 按 V1 配置处理

每个动作必须 journal：

- trigger
- selected action
- order intent
- submit result
- reconciliation result
- final state

## Venue 纪律

V2 可以保留共享 transport，但不能抹平 V1 的交易所差异。

每家 venue 必须明确：

- public/private base URL
- signing payload
- timestamp format
- query string ordering
- leverage setup
- position mode
- lot size
- tick size
- contract size
- min quantity
- min notional
- reduce-only exception
- post-only semantics
- cancel/amend support
- fill reconciliation
- private websocket support
- rejected/uncertain 特殊错误码

不支持的能力必须显式声明 unsupported，不能伪装成功。

## Recovery 纪律

重启恢复必须保守：

- clean open position 可以恢复为 open
- pending entry 必须查询 maker/hedge 状态
- pending close 必须查询 close order 状态
- unknown order 必须 reconcile
- residual exposure 必须 protect/close
- ambiguous state 必须 reconciling 或 fail closed

禁止 ambiguous state 直接进入 RUNNING。

## 最终验收定义

只有满足以下条件，才能宣称当前范围完成：

- 当前 Task 的 Rust 真源已列出
- Alignment Check 已完成
- Python 骨架不足之处已明确扩展
- 相关 GitNexus impact 已执行
- 行为测试已覆盖 happy path 和失败模式
- focused tests 通过
- 涉及生产闭环时 full tests/compileall 通过
- GitNexus detect_changes 已执行
- 没有 live-path `NotImplementedError`
- 没有 paper/mock-only 冒充 live 完成
- 没有未知订单、partial fill、residual exposure 被吞掉

## 最终回复格式

每次完成一个执行切片后，按这个格式回复：

```markdown
## 完成范围

- Task: ...
- Rust 真源: ...
- Python 文件: ...

## 业务等价说明

- ...

## 骨架变更

- ...

## 验证

- `...`: pass/fail
- `...`: pass/fail

## GitNexus

- impact: ...
- detect_changes: ...

## 风险和后续

- ...
```

如果有测试没跑或命令失败，必须明确写出来，不允许用“应该可以”替代。

## 当前总控文档

本提示词必须配合以下两份文档执行：

- `docs/superpowers/specs/2026-05-10-v1-production-closure-replication-design.md`
- `docs/superpowers/plans/2026-05-10-v1-production-closure-replication-implementation-plan.md`

如果本提示词和上述文档冲突，以更严格、更接近 Rust V1 live 行为的一方为准。
