# Production Entry Local-L2 Root Fix Design

日期: 2026-05-16

状态: 设计稿，供执行智能体实现；本文件记录 2026-05-15 之后生产不开仓的永久根修规格。

## 背景

2026-05-15 之后，生产云机 `lightfee-live` 和 `lightfee-sidecar` 进程均为 active，sidecar 当时可发布 7 venue 快照，live 当前状态为 `lifecycle=running`、`risk_mode=running`，但没有开仓。

按 `>= 2026-05-15 00:00:00 CST` 过滤 journal 后，主要证据是:

| 事件 / 原因 | 次数 | 结论 |
|---|---:|---|
| `runtime.entry_blocked_local_l2_selection` | 104,920 | 当前不开仓主因在 entry local-L2 选择门槛 |
| `entry_local_l2_waiting_for_prewarm_window` | 97,291 | 多数候选仍在资金费预热窗口外，属于正常等待或候选选择/诊断不足 |
| `entry_local_l2_waiting_for_dual_ready` | 7,629 | 核心异常：进入预热窗口后仍无法证明双腿 local-L2 ready |
| `runtime.snapshot_degraded` | 2,299 | 数据面仍有退化，需要细化 venue/symbol/domain 归因 |
| `runtime.snapshot_stale` | 18 | 次要但需要纳入 sidecar freshness 诊断 |
| `order.submitted` / `entry.*` 下单事件 | 0 | 5.15 后没有走到下单层；历史下单错误是下一层风险，不是本轮不开仓主因 |

本轮根修要先解决“为什么 tradeable candidate 永远过不了 local-L2 gate”，再确保一旦过 gate 后，下单层按交易所文档处理 ACK、fill、reconcile、precision、signing。

## 根因拆分

### 可以复刻 V1 的 V2 残差问题

1. **entry local-L2 session 没有从 local-L2 book 状态获得 ready 驱动。**
   - V2 `LiveRuntime._entry_local_l2_selection_blocker()` 要求 session `both_legs_ready()`。
   - V2 `EntryLocalL2LegSession.mark_ready()` 存在，但运行时路径基本没有调用；测试手动标 ready，生产不会自动标 ready。
   - `LocalL2DataPlane.bootstrap_book()` 能把 book 写入 runtime 并转 HOT，但没有同步到 `entry_l2_sessions`。
   - V1 有 `EntryLocalL2SessionRuntime`、per-leg arming/ready/fault、primary not-ready 诊断和 `scan.no_entry_diagnostics`。

2. **no-entry 诊断没有 V1 级别的根因字段。**
   - 当前生产只能看到 `entry_local_l2_waiting_for_dual_ready`，看不到哪条腿、哪个 book 状态、哪个 sequence/fault/age 导致不 ready。
   - V1 `scan.no_entry_diagnostics` 有 `entry_local_l2_primary_not_ready_reason_counts`、`entry_local_l2_primary_not_ready_detail_samples`、selection blocker counts、candidate checklist。

3. **primary tracking / prewarm 选择语义需要 V1 诊断闭环。**
   - 生产 `max_concurrent_positions=8`，V2 discovery 只返回 top N 候选；如果 top N 大多在 prewarm window 外，系统会反复等待而不是解释“窗口外候选占据 shortlist”。
   - 根修不应放宽 local-L2 安全门槛，而应让 V1 级别诊断证明候选处于哪个门槛。

4. **snapshot degraded/stale 需要 V1 风格细分。**
   - 当前知道有 `snapshot_degraded`，但不知道是 sidecar publish stale、venue funding stale、quote stale、local-L2 snapshot failed、WS delta gap、还是 symbol unsupported。

### V1/V2 共有或交易所约定问题

这些不是单纯搬 V1 就完事，必须按交易所文档建立统一状态机:

1. **REST 下单回执不是最终成交。**
   - Bybit 文档说明 place order 是异步请求，需要 websocket 确认订单状态。
   - Bitget best practice 说明 REST 成功后仍要等订单 channel 推送。
   - Aster futures 文档建议在极端波动下从 WebSocket user data stream 获取订单/仓位状态。
   - Binance USD-M `newOrderRespType` 默认 `ACK`，只有特定 `RESULT` 模式会返回最终状态。
   - OKX place order 返回 `ordId/clOrdId`，最终状态通过 order details / pending / history / fills 获取。

2. **signing、dependency、host/path/productType 是启动前契约。**
   - 历史日志出现过 Binance signature invalid、Hyperliquid/Aster signing dependency 缺失。
   - 这些必须在 startup preflight 中失败可见，不能等到真实下单后才暴露。

3. **price/qty precision 和合约单位必须由 adapter metadata 量化。**
   - Gate futures size 是整数合约方向量，Bybit 要参考 `priceFilter.tickSize`，OKX/Bitget/Aster/Binance 也各有 tick/step/min/max。
   - 历史 `price digit is greater than 12` 必须由统一 quantizer 根修。

4. **余额/保证金不足要在 candidate admission 前暴露。**
   - `INSUFFICIENT_AVAILABLE` 是资金/杠杆/冻结/双腿保证金问题。
   - 需要 account/margin/leverage precheck 和清晰的 admission blocker。

## 目标

- 让 V2 entry local-L2 session readiness 从真实 local-L2 book 状态自动驱动，生产进入预热窗口后可以从 `dual_ready=false` 走到 `dual_ready=true`，或给出可根修的 per-leg 原因。
- 完整移植 V1 no-entry 诊断能力，下一次日志能一次定位“没开仓原因”。
- 保持安全语义: 不绕过 prewarm、不绕过 primary tracking、不在 local-L2 未 ready 时下单。
- 为下单层建立交易所文档一致的 ACK / fill / reconcile / precision / signing 后续根修任务。

## 非目标

- 不降低 `local_l2_enabled` 的开仓安全门槛。
- 不在单元测试或 CI 里发送真实 live order。
- 不把生产 secret、API key、账号标识、原始大日志写进仓库。
- 不把所有 exchange adapter 重写进本轮第一阶段；第一阶段先解决 5.15 后不开仓主因。

## 生产契约

### Entry Local-L2 Readiness 契约

对每个 primary tracked opportunity:

- `track_opportunity()` 只负责创建/维持 session 和 legs，不能把 leg 视为 ready。
- 每次 `_sync_local_l2_data()` 之后，必须把 `LocalL2Runtime` book 状态同步到 `EntryLocalL2SessionRuntime`:
  - book 不存在: leg `ARMING`, reason `book_missing`
  - book `BOOTSTRAPPING`: leg `ARMING`, reason `book_bootstrapping`
  - book `HOT` 且 fresh 且未 crossed/locked: leg `READY`
  - book stale: leg `FAULTED` 或 `ARMING` with `session_arming_stale_recovery`，并记录 `age_ms`
  - book `REBUILDING` / sequence gap / checksum mismatch: leg `FAULTED` 或 `ARMING` with V1 reason
  - book `DEGRADED` / `SUSPENDED` / runtime suspended: leg `FAULTED`
  - crossed or locked book: leg `FAULTED` with `crossed_or_locked_book`
- `both_legs_ready()` 只在两条腿都 ready 且未 stale 时为 true。
- session 状态变化必须可观测，且日志要包含 pair、venue、symbol、book_status、reason、detail、age、sequence。

### No-Entry Diagnostics 契约

当 scan 没有 dispatch entry 时，必须周期性/指纹去重发出 `scan.no_entry_diagnostics`:

- `reason`
- `candidate_count`
- `tradeable_count`
- `selected_candidate_count`
- `remaining_slots`
- `blocked_reason_counts`
- `entry_candidate_blocked_counts`
- `execution_liquidity_blocked_counts`
- `entry_final_gate_blocked_counts`
- `tradeable_selection_blocker_counts`
- `entry_local_l2_primary_ready_filter_active`
- `entry_local_l2_primary_not_ready_reason_counts`
- `entry_local_l2_primary_not_ready_reason_totals`
- `entry_local_l2_primary_not_ready_detail_samples`
- bounded candidate checklist with pair id, rank, venues, prewarm remaining, primary/shadow/tracked state, selection blocker

### Sidecar / Snapshot Diagnostics 契约

`runtime.snapshot_degraded` 和 `runtime.snapshot_stale` 不能只给总量，至少需要:

- snapshot publish age
- market observed age
- per-venue quote count
- per-venue candidate count
- stale/degraded domains
- top degraded symbols
- sidecar snapshot source and config hash when available

### Order Path 契约

下单层后续根修必须遵循:

- submit response with order id is `ACK/accepted/resting`，不是 fill。
- fill 只能来自 immediate filled response、private order stream、order details/query、execution/fills endpoint 或 position reconcile。
- uncertain submit 不能随意换 client order id 重试。
- 所有 exchange request 必须记录 sanitized request metadata: venue、endpoint、product type、client id、order id、raw/quantized price qty、tick/step、response classification；不得记录 secret/signature。

## 官方交易所文档依据

- Binance USD-M Futures New Order: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order
- Binance local order book management: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- Bybit V5 Place Order: https://bybit-exchange.github.io/docs/v5/order/create-order
- Bybit V5 Order History: https://bybit-exchange.github.io/docs/v5/order/order-list
- OKX API v5 Trade / Order details: https://www.okx.com/docs-v5/en/
- Bitget Best Practices: https://www.bitget.com/api-doc/classic/best-practices
- Bitget UTA Order Channel: https://www.bitget.com/api-doc/uta/websocket/private/Order-Channel
- Gate API v4: https://www.gate.com/docs/apiv4/index.html
- Hyperliquid Exchange Endpoint: https://hyperliquid.gitbook.io/Hyperliquid-docs/for-developers/api/exchange-endpoint
- Aster Futures v3 Account & Trading: https://asterdex.github.io/aster-api-website/futures-v3/account%26trades/

## 验收标准

- 单元测试证明: tracked session 不会因 track 本身 ready；只有 fresh HOT book 会让两腿 ready。
- 单元测试证明: stale、missing、bootstrapping、degraded、crossed book 会产生稳定 not-ready reason 和 detail samples。
- `scan.no_entry_diagnostics` 测试证明包含 V1 关键字段和 local-L2 per-leg not-ready counts。
- 本地 replay/probe 能从 5.15 后 journal 生成 blocker frequency、top pairs、top symbols、primary not-ready reasons。
- 生产 dry-run 验证能在不下单的情况下证明:
  - sidecar 7 venue connected and fresh
  - live running
  - entry local-L2 primary sessions either dual-ready or有 per-leg root reason
  - order path preflight imports/signing dependencies pass

