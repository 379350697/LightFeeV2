# Production Entry Local-L2 Pending/Reconcile Root Fix Design

日期: 2026-05-17

状态: 设计稿；先更新规格、计划和 bug ledger，不改代码。

## 背景

生产云机当前运行 `/opt/lightfee-v2`，代码为 `7792db5`。证据只取当前绝对路径:

- 事件日志: `/opt/lightfee-v2/runtime/live-events.jsonl`
- 当前状态: `/opt/lightfee-v2/runtime/live-state.json`
- sidecar 快照: `/opt/lightfee-v2/runtime/opportunity-input-snapshot.json`

`/var/log/lightfee-v2/live.log` 是旧日志，不作为本轮当前证据。

本轮观测时，`lightfee-sidecar.service` 和 `lightfee-live.service` 均 active，sidecar 快照覆盖 7 个 venue，当前状态 `lifecycle=running`、`risk_mode=running`。问题不是 sidecar 断连，而是 entry local-L2 选择层和 pending/reconcile 执行层共同阻碍开仓。当前 `open_position_count=0`，`pending_entry_count=2`，事件日志内没有 `entry.opened` / `runtime.position_opened`。

## 生产频次证据

### 最近 2 小时

| 事件 / 原因 | 次数 | 结论 |
|---|---:|---|
| `runtime.entry_blocked_local_l2_selection` | 44,524 | 没有进入 dispatch 的主因 |
| `entry_waiting_for_finalization_window_too_early` | 37,108 | 策略时间窗门控，需降噪但不是策略无关 bug |
| `entry_local_l2_waiting_for_primary_tracking` | 6,426 | 候选选择与 primary tracked set 语义不一致 |
| `entry_local_l2_waiting_for_dual_ready` | 990 | primary tracked session 双腿仍未 ready |
| `scan.no_entry_diagnostics: entry_local_l2_selection_blocked` | 472 | no-entry 诊断已能聚合到 L2 selection blocker |
| `pending_entry.missing_hedge_detected` | 48 | maker fill 后 hedge 未闭环仍在发生 |
| `order.reconcile_result: hyperliquid uncertain` | 48 | Hyperliquid hedge 侧回查不确定 |
| `order.reconcile_result: okx uncertain` | 24 | OKX maker 侧部分回查不确定 |
| `pending_entry.hedge_submit_result: min_notional_rejected` | 24 | Hyperliquid hedge 残量低于最小名义金额 |

### 当前日志窗口

| 事件 / 原因 | 次数 | 结论 |
|---|---:|---|
| `runtime.entry_blocked_local_l2_selection` | 709,615 | 长期主阻碍 |
| `entry_waiting_for_finalization_window_too_early` | 479,907 | 需要从 bug 频次中单独剥离 |
| `entry_local_l2_waiting_for_prewarm_window` | 135,187 | 旧窗口/预热等待噪音 |
| `entry_local_l2_waiting_for_primary_tracking` | 70,589 | 本轮最重要的新高频 L2 语义问题 |
| `entry_local_l2_waiting_for_dual_ready` | 23,932 | 5/16 CL-001 的老问题族仍在 |
| `book_bootstrapping` | 159,396 | dual-ready 未达成的最大 per-leg 原因 |
| `stale_book` | 35,310 | 本地盘口 freshness / rebuild 问题 |
| `crossed_or_locked_book` | 14,606 | 本地盘口状态质量问题 |
| `runtime.local_l2_hot_stale_rebuild` | 25,097 | HOT book 反复 stale/rebuild，需要根修 |
| `runtime.entry_blocked_gate:pending_entry_duplicate` | 14,840 | 历史老问题，最近 24 小时不再是主因 |

## 问题归类

### Issue A: `entry_local_l2_waiting_for_primary_tracking`

Fingerprint: `production.entry-local-l2.primary-tracking-selection-mismatch`

现象: 当前有 tradeable candidate，但 `_entry_local_l2_selection_blocker()` 会对所有 tradeable candidate 检查 `pair_id in _tracked_primary_pair_ids`。实际只有按 `entry_local_l2_primary_count` 选出的 primary set 会被跟踪。因此大量 tradeable candidate 被记录为 `entry_local_l2_waiting_for_primary_tracking`。

归类:

- 不是交易所问题。
- 不是收益策略本身判断失败。
- 是 V2 selection/blocker 诊断口径和 V1 primary tracked set 语义没有完全对齐。

能否复刻 V1:

- 可以。V1 语义应把 primary tracking 当作 shortlist admission / tracked-set 状态，而不是把所有未入 primary set 的 tradeable candidate 都算成 local-L2 readiness failure。
- 根修不能放宽 L2 安全门槛，只能调整 selection 顺序、诊断 bucket、primary/shadow tracking 保留策略。

验收:

- `scan.no_entry_diagnostics` 能区分 `not_primary_tracked`、`primary_tracked_not_ready`、`primary_tracked_ready_but_final_gate_blocked`。
- `entry_local_l2_waiting_for_primary_tracking` 不再淹没真正的 `dual_ready` 根因。

### Issue B: `entry_local_l2_waiting_for_dual_ready`

Fingerprint: `production.entry-local-l2.dual-ready-book-state-not-hot`

现象: 5/16 CL-001 已经记录过 V2 缺少 entry local-L2 readiness bridge。当前生产 `7792db5` 已有 readiness diagnostics，但仍有 dual-ready 阻塞。现在能看到原因主要是 `book_bootstrapping`、`book_rebuilding`、`stale_book`、`crossed_or_locked_book`，以及大量 `local_l2_hot_stale_rebuild`。

归类:

- 老问题复现，但不是 5/16 那种完全看不见 per-leg 原因的状态。
- 当前是数据面/session 生命周期实际没有稳定进入双腿 HOT/fresh。

能否复刻 V1:

- 大部分可以。V1 的做法是让 local-L2 book lifecycle、entry local-L2 session readiness、no-entry diagnostics 三者闭环。
- 如果 V1/V2 代码语义已经一致，剩余部分必须按交易所 local order book 官方文档核对 sequence、snapshot/delta 衔接、stale/rebuild 规则。

验收:

- 对 primary tracked pair，双腿 fresh HOT book 必须让 blocker 返回 `None`。
- missing/bootstrap/rebuilding/stale/crossed book 必须有稳定 reason、venue、symbol、age、sequence 样本。
- `book_hot` 不应作为 not-ready reason 出现；若出现，必须拆出真实原因，例如 stale、crossed、empty side、age missing。

### Issue C: pending/reconcile 仍卡住，当前没有成功开仓

Fingerprint: `production.pending-entry.reconcile-uncertain-and-hedge-residual`

当前 pending:

| Entry | 状态 | 当前阻碍 |
|---|---|---|
| `POLYXUSDT bybit -> hyperliquid` | Bybit maker 已有 fill evidence，Hyperliquid hedge 仍 `uncertain`，`hedge_inflight` 存在 | 不能重复 hedge；必须先用 Hyperliquid order/position evidence 证明有没有 hedge |
| `STABLEUSDT okx -> hyperliquid` | OKX maker 部分成交，Hyperliquid hedge 补单反复失败 | residual notional 约 `3.12`，低于 Hyperliquid `MinTradeNtl` `$10` |

归类:

- 5/17 CL-001 已修的是“正常 tick 要驱动 missing hedge、uncertain hedge 要稳定 CID、恢复要保留字段”。
- 当前不是完全相同的旧 bug，而是旧 pending 闭环进入交易所约束后的残留问题。
- V1 可以提供状态机和 idempotency 语义，但 Hyperliquid/OKX/Bybit 的最终判断必须按交易所文档查询 order、fills、position。

能否复刻 V1:

- 可以复刻 V1 的 pending hedge delta / idempotency / finalization 状态机。
- 不能仅复刻 V1 来解决最小名义金额、cloid 查询、order history 延迟、fills/position 优先级。这些必须按交易所文档根修 adapter/reconciler。

验收:

- `hedge_inflight` 不能永久阻止修复；在 order/fill/position 都证明无 hedge 时，允许安全清理 stale inflight 并补 hedge。
- 对 min-notional residual，系统不能每 5 分钟无限重试同一个必然失败的 hedge。
- 当 maker residual 低于 hedge venue 最小 notional 时，必须进入明确状态: aggregate、cancel/flatten maker residual、或 mark dust/manual_repair。

### Issue D: 历史噪音和当前主因要分离

Fingerprint: `production.blocker-regression-classification.current-vs-historical`

历史问题:

- `runtime.entry_blocked_gate:pending_entry_duplicate`: 日志窗口共 14,840，但最近 24 小时不是主因。
- API key value 包含 `\r`、signature invalid、recvWindow、invalid symbol、FundingLifecycle 类型错误: 属于历史运行残留，不能混入当前不开仓主因。
- `runtime.snapshot_degraded`: 总量 27,712，但最近 24 小时只有少量残留，当前 sidecar 快照是 fresh。

验收:

- analyzer 和 bug ledger 必须同时报告 `last_2h`、`last_24h`、`run_window`，避免把历史老错误当当前阻碍。
- bug ledger 每个 issue 都标明 old recurrence / new residual / historical only。

## 官方文档约束

需要按官方文档核对以下契约。每个交易所给精确链接，避免搜错版本：

- **Hyperliquid Exchange Endpoint**
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
  order action 支持 optional `cloid`，place order 可能返回 resting、filled 或 error；cancel by cloid 也有独立 action。
- **Hyperliquid Error Responses**
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses
  `MinTradeNtl` 表示 perp order 最小价值 `$10`；这是当前 STABLE residual hedge 的直接约束。
- **Hyperliquid Tick and Lot Size**
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size
  price/size precision 必须按 `szDecimals` 和 significant figures 处理。
- **Bybit V5 Place Order**
  https://bybit-exchange.github.io/docs/v5/order/create-order
  place order ACK 只是异步接受，必须用 websocket 或查询确认最终 order status。
- **Bybit V5 Order History**
  https://bybit-exchange.github.io/docs/v5/order/order-history
  history 数据有延迟，实时信息应优先 open/closed order endpoint 或 websocket。
- **OKX API v5 Trade**
  https://www.okx.com/docs-v5/en/#order-book-trading-trade
  `clOrdId` 可用于查询/撤改单，但历史唯一性不保证；order details、pending orders、history、fills 都要纳入 reconcile fallback。
- **Binance USD-M Local Order Book**
  https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
  snapshot/diff stream 必须按 update id 衔接，`pu` 不连续时要重新 snapshot。

## 部署边界

本轮默认只做到：**本地实现、测试、GitNexus detect_changes、文档更新和生产只读验证**。不得部署、重启服务或下单，除非用户明确授权。生产只读验证使用绝对路径和 `--no-secrets`，不重启、不下单。

## 非目标

- 不禁用 local-L2 安全门槛。
- 不为了开仓而绕过 pending/reconcile 风险。
- 不在仓库里存 raw cloud logs、账号标识、secret、API key 或 SSH 凭据。
- 不把 5/14 到 5/16 的历史错误重新当作当前主要 blocker。

## 根修目标

1. primary tracking 语义和 V1 对齐，避免未入 primary set 的 tradeable candidate 被误报为 local-L2 readiness failure。
2. dual-ready 只保留真实 per-leg book reason，消除 `book_hot` 等含混 not-ready reason。
3. local-L2 book HOT 后不应频繁 stale/rebuild；对 sequence gap、snapshot age、empty side、crossed book 做 venue/symbol 归因。
4. pending/reconcile 有可终止状态机，解决 stale inflight、uncertain order、min-notional residual。
5. analyzer 和 bug ledger 能一眼区分当前阻碍、老问题复现、新问题、历史噪音。

## 验收标准

- 本地测试证明 primary tracking bucket 不再吞掉真实 L2 readiness reason。
- 本地测试证明 fresh HOT 双腿能通过 `_entry_local_l2_selection_blocker()`，stale/bootstrap/rebuild/crossed 仍 fail-closed。
- 本地测试证明 Hyperliquid residual below `$10` 不再反复提交，进入明确 residual policy。
- 本地测试证明 stale `hedge_inflight` 在 order/fill/position 三方确认无 hedge 后可以安全清理并补 hedge。
- 生产 read-only 验证在不重启、不下单的情况下给出:
  - sidecar 7 venue fresh；
  - current state running；
  - `last_2h` / `last_24h` / `run_window` blocker table；
  - current pending entry decision tree；
  - no raw secrets in output。

## 参考文件

- `docs/bugs/daily/2026-05-16.md`
- `docs/bugs/daily/2026-05-17.md`
- `docs/bugs/BUG-20260514-v2-v1-parity-root-fix-loop.md`
- `lightfee/engine/runtime.py`
- `lightfee/engine/entry_local_l2.py`
- `lightfee/engine/reconciliation.py`
- `lightfee/venues/hyperliquid.py`
- `lightfee/venues/transport.py`

