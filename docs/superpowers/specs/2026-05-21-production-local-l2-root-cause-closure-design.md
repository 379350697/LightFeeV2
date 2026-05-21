# Production Local-L2 Root-Cause Closure Design

日期: 2026-05-21

状态: 设计稿，供执行智能体按证据闭环实现；本文件只覆盖 2026-05-12 至 2026-05-21 生产 Local-L2 / Entry-L2 / readiness / bootstrap / rebuild / entry blocker 问题。

## 背景

本轮问题不能用最近几小时的样本判断。云端 `/opt/lightfee-v2/runtime/live-events.jsonl` 从 `2026-05-12T22:55:03+08:00` 到 `2026-05-21T02:41:45+08:00` 共 `2,544,221` 行，覆盖 149 个 run。

多天证据显示:

| 事件 / 原因 | 次数 | 结论 |
|---|---:|---|
| `runtime.entry_blocked_local_l2_selection` | 1,312,235 | 大量 entry blocker 是 local-L2 admission/readiness 层，不等于全部数据面 bug |
| `runtime.local_l2_buffer_overflow_rebuild` | 129,616 | 真实多日主线，不能因当前短窗口下降而判定根除 |
| `runtime.local_l2_hot_stale_rebuild` | 122,416 | 真实多日主线，当前 run 仍复现 |
| `runtime.local_l2_snapshot_error` | 34,801 | 大部分是历史 parser/catalog 问题，最新 run 已大幅下降 |
| `runtime.entry_local_l2_readiness_diagnostics` | 18,629 | 可定位 per-leg not-ready，必须继续保留并增强 |
| `runtime.local_l2_snapshot_stale` | 144 | 最新 run 主要集中在 Bybit `IRYSUSDT` |

最新 run `lightfee-1779300740095-1232768` 的关键事实:

- `risk_mode=running`，`lifecycle=running`，没有 open/pending work。
- Unsupported catalog 类问题已转为 `runtime.local_l2_symbol_skipped=46`。
- 当前仍有 `runtime.local_l2_hot_stale_rebuild=645`、`runtime.local_l2_snapshot_stale=94`、`runtime.local_l2_snapshot_error=1`。
- 当前 entry-critical blocker 集中在 `irysusdt:binance->bybit`，Bybit 腿持续 `book_bootstrapping`。
- 当前 Bybit `IRYSUSDT` stale 样本连续出现 `book_seq=13700598`，REST `snapshot_seq=7103xxx`，说明 V2 正在比较两个不一定同域的 Bybit orderbook 序列。

## 非发散原则

1. **V1/V2 不一致导致的问题，直接完整复刻 V1。**
   - 不做“近似 V1”。
   - 不只补测试。
   - 不用更宽松 readiness、top-book fallback、sidecar-mid 替代 true local-L2。

2. **V1/V2 共有或 V1 未覆盖的问题，必须用生产日志和交易所官方文档确定根因。**
   - 不按猜测修改。
   - 不把真实 sequence gap、交易所维护、断流伪装成 HOT。
   - 不把交易所文档不支持的跨 depth / 跨 channel sequence compare 当成连续性证明。

3. **必须完整闭环，不接受最小闭环。**
   - 本地 unit/integration 测试只是第一层。
   - 必须有 live public probe 或 replay harness。
   - 必须用曾经失败的 venue/symbol 样本验证。
   - 必须有云端当前 run 级别验收。
   - 必须更新 bug ledger，记录哪些 closed、哪些 residual。

## 已关闭范围

### Unsupported Catalog / Symbol 过滤

已根修并部署的 catalog 问题:

- Binance `SYSUSDT status=SETTLING` 不再进入 active local-L2 book。
- Aster `RLSUSDT` 不在 exchangeInfo，不再进入 active local-L2 book。
- Hyperliquid `MAV isDelisted=true` 不再进入 active local-L2 book。

根修不是表面删除 symbol，而是将 filtered venue catalog 接入以下路径:

- startup local-L2 activation
- candidate-triggered local-L2 activation
- retained local-L2 restore
- persisted full local-L2 snapshot restore
- fallback position probes

验收继续要求:

- 当前 run 不得再出现这些 symbol 的 `runtime.local_l2_snapshot_error`。
- 当前 `local_l2_session_snapshot` 不得再包含 unsupported venue pair。
- 这些 symbol 可以作为 `runtime.local_l2_symbol_skipped` 观测项出现。

### REST Snapshot Parser / Snapshot Buffering

已根修并部署的 parser/data-plane 问题:

- Binance REST `/fapi/v1/depth` 解析为 snapshot。
- OKX REST `/api/v5/market/books` 无 `action` 时解析为 snapshot。
- Bybit REST `/v5/market/orderbook` `result` wrapper 解析为 snapshot。
- Bitget 使用 `seq/pseq` 而不是 `checksum` 做连续性字段。
- Authoritative snapshot 不再被普通 delta buffer 卡住。

这些修复只能关闭“parser/schema drift”，不能自动关闭 buffer overflow、hot stale、Bybit depth sequence mismatch、真实 sequence gap。

## 未关闭范围

### P0: Bybit REST/WS Depth Sequence Domain Drift

证据:

- 当前 run `IRYSUSDT` 多次 `runtime.local_l2_snapshot_stale`。
- `book_seq=13700598`，REST `snapshot_seq=7103xxx`，持续被判 stale。
- 同一 pair `irysusdt:binance->bybit` 的 Bybit 腿持续 `book_bootstrapping`，阻碍 dual-ready。

官方文档依据:

- Bybit WS orderbook topic 是 `orderbook.{depth}.{symbol}`，订阅成功后先推 `snapshot`，收到新 `snapshot` 必须 reset local orderbook。
- Bybit REST `/v5/market/orderbook` 响应是 snapshot format。
- Bybit REST `u` 对合约场景对应 `1000-level` WS orderbook 的 `u`，而 V2 当前 WS 订阅是 `orderbook.50`。

根因方向:

- V2 不应把 Bybit REST snapshot `u` 与 WS `orderbook.50` book sequence 做 stale/replay 连续性比较。
- Bybit 的安全根修应转成 V1 风格: WS snapshot 是 authoritative bootstrap/recovery anchor；delta before initial snapshot ignored；如 `pu` 存在且 mismatch，清空 book/sequence 并 resubscribe/rebuild。
- REST snapshot 只能作为无 WS fallback 或 probe evidence，不能与 active WS depth-50 delta buffer 混合 replay。

验收:

- Bybit `IRYSUSDT` harness 必须复现旧逻辑的 false stale。
- 修复后同一 harness 中 Bybit `IRYSUSDT` 不得卡在 `book_bootstrapping`，且不得把跨 domain sequence 当连续。
- 云端新 run 中 Bybit `IRYSUSDT` 不得重复出现相同 `book_seq` 对大量更小 REST `snapshot_seq` 的 stale loop。

### P0: Pre-Snapshot Buffer Overflow / Replay Loop

证据:

- 多天 `runtime.local_l2_buffer_overflow_rebuild=129,616`。
- Top symbols 包括 Bybit `IRYSUSDT`、`RONINUSDT`、`CHIPUSDT`，Binance `CHIPUSDT`，OKX `CHIPUSDT`。
- V2 `_PRE_SNAPSHOT_BUFFER_CAP = 512`。
- V1 Binance `BINANCE_LOCAL_L2_PRE_SNAPSHOT_BUFFER_CAP = 4096`，并明确说明 tiny cap 会导致 active symbols snapshot-boundary rebuild loop。

V1 parity:

- Binance/Aster 类 diff-depth bootstrap 必须按 V1 模型完整复刻:
  - open WS stream
  - buffer events
  - fetch REST depth snapshot
  - drop events already covered by snapshot
  - first replay event must bridge snapshot boundary
  - subsequent `pu` must equal previous `u`
  - gap forces rebuild, not HOT
  - pre-snapshot buffer cap must match V1 enough覆盖 active symbol bridge，不能使用 512 generic cap

非 V1 范围:

- Bybit、OKX、Bitget、Gate 不应无脑套 Binance REST snapshot + delta replay。
- 这些 venue 必须按各自 WS snapshot/reset/seq/pseq/full 语义做 venue-specific policy。

验收:

- Harness 通过旧失败 symbols 在人工延迟 REST snapshot 时复现 512 overflow。
- 修复后 Binance/Aster 类桥接在 4096 cap 下不再触发同样 overflow loop。
- 如果仍 overflow，日志必须包含 venue、symbol、buffered_count、snapshot_seq、first/last buffered seq、reason class，能证明是实 gap 而不是 cap 太小。

### P0: OKX Keepalive / Reset / Checksum Replay Drift

证据:

- 历史 readiness 里有 checksum mismatch、crossed/locked、buffered replay boundary。
- V1 OKX replay 明确区分 `Normal`、`Keepalive`、`Reset`、`Obsolete`、`Invalid`。
- V2 generic `_replay_buffered_updates()` 没有完整复刻这套分类。

官方文档依据:

- OKX incremental orderbook 使用 `seqId/prevSeqId`。
- keepalive 可能出现 `seqId == prevSeqId` 且 bids/asks 为空。
- sequence reset 可能出现 `seqId < prevSeqId`，后续消息重新按规则连续。
- snapshot `prevSeqId` 可为 `-1`。

根因方向:

- OKX replay 必须复刻 V1 分类，不得把 keepalive/reset 误判成 previous-link mismatch。
- OKX checksum 验证必须使用 raw contract quantity string / ctVal 语义，不得只用 float normalize 后的 book 推断。

验收:

- V1 OKX tests 中的 keepalive/reset/obsolete/invalid fixtures 必须迁移到 V2。
- Harness 对 OKX `INJUSDT`/`CHIPUSDT` 捕获到 keepalive/reset 时，不得触发错误 rebuild。

### P1: Hot Stale Rebuild

证据:

- 多天 `runtime.local_l2_hot_stale_rebuild=122,416`。
- Top symbols 包括 OKX `INJUSDT`、Binance `INJUSDT`、Binance `STABLEUSDT`、OKX `TRUTHUSDT`、OKX `LABUSDT`。
- 当前 run 仍有 `hot_stale_rebuild=645`。

分类:

- V1/V2 共有: 真实断流、交易所不推送、冷门 symbol 无更新、网络抖动。
- V2 drift: timestamp source、clock normalization、WS worker lifecycle、subscription payload 与官方 docs 不一致、snapshot refresh policy 错误。

根因方向:

- 如果 V1 有 venue-specific worker/session/timestamp 语义，完整复刻。
- 如果 V1 未覆盖，用官方 docs 和 live probe 判断:
  - Binance event time / update cadence
  - Bybit `ts`/`cts`
  - OKX `ts`
  - Bitget UTA `ts`
  - Gate `time_ms`/`t`
  - Hyperliquid poll interval

验收:

- hot stale 的日志必须能区分 worker disconnected、no updates but keepalive healthy、timestamp missing、clock skew、subscription rejected、REST fallback stale。
- 修复后不得仅通过扩大 stale threshold 掩盖问题。

### P1: Bitget UTA Depth Channel Contract

证据方向:

- V2 当前 Bitget WS subscribe/parser 使用 `channel`，但 UTA docs 使用 `topic`。
- 旧 CL-003 已修 `seq/pseq` 解析，但还需要真实 WS probe 证明订阅字段与当前 production endpoint 完全匹配。

官方文档依据:

- Bitget UTA depth channel `books` 首推 `snapshot`，后续 `update`。
- `seq` 是 serial number，`pseq` 是 previous push serial。
- `pseq=0` 表示 reset/system release 类重建边界。
- snapshot sequence must fall inside first update `[pseq, seq]` bridge。

根因方向:

- 如果 live probe 证明 current endpoint 使用 `topic`，V2 必须改为 docs-compatible 或兼容 `topic/channel`。
- `pseq=0` 必须触发 reset/rebuild anchor，不得当普通 delta 连续。

### P2: Gate Local Book Channel

证据方向:

- V2 当前使用 legacy `futures.order_book`，官方不推荐用它维护更及时 local orderbook。
- 官方 `futures.order_book_update` 有 `full`、`U`、`u` 的本地 book 维护语义。

根因方向:

- 只有当 Gate 成为 entry-critical blocker 或 probe 证明 legacy channel 造成 stale/rebuild，才切换到 `futures.order_book_update`。
- 如果切换，必须按官方 `full=true` reset、`U == local_depth_id + 1` 连续规则实现，不能套 generic timestamp sequence。

## Entry Blocker 分类

以下是 V1/V2 共有 gate，不作为 local-L2 data-plane bug:

- `entry_waiting_for_finalization_window_too_early`
- `entry_finalization_window_expired`
- `entry_local_l2_waiting_for_prewarm_window`
- `entry_local_l2_waiting_for_primary_tracking`

以下才进入本轮 data-plane / readiness 根修:

- `entry_local_l2_waiting_for_dual_ready`
- readiness reason: `book_bootstrapping`
- readiness reason: `book_rebuilding`
- readiness reason: `stale_book`
- readiness reason: `book_hot` with `stale_hot_book`
- readiness reason: `book_empty_side`
- readiness reason: `book_timestamp_missing`
- readiness reason: `crossed_or_locked_book`
- snapshot/replay categories: `pre_snapshot_buffer_overflow`、`buffered_replay_snapshot_boundary`、`buffered_replay_previous_link_mismatch`

## Harness / Probe 契约

必须新增 live public probe 和 deterministic replay harness。

Probe 必须支持:

- Binance: WS diff-depth + REST `/fapi/v1/depth` bridge validation。
- Bybit: WS `orderbook.50` snapshot/delta + REST `/v5/market/orderbook` sequence-domain comparison。
- OKX: WS books snapshot/update + keepalive/reset/checksum capture。
- Bitget: UTA depth subscribe request/response + `seq/pseq` capture。
- Gate: legacy `futures.order_book` 与 `futures.order_book_update` schema capture。
- Hyperliquid: `/info {"type":"l2Book"}` poller freshness and empty-side classification。

Replay harness 必须支持:

- 将 probe 输出保存为 sanitized fixture。
- 在无网络 CI 中重放 fixture。
- 对曾经失败的 symbols 建立固定 regression cases:
  - `bybit IRYSUSDT`
  - `bybit CHIPUSDT`
  - `bybit FOGOUSDT`
  - `binance JTOUSDT`
  - `binance CHIPUSDT`
  - `okx INJUSDT`
  - `okx CHIPUSDT`

## 验收标准

### 本地验收

- 新增 tests 必须先 red 后 green。
- `pytest tests/test_local_l2_runtime.py tests/test_local_l2_ws.py tests/test_local_l2_venue_rules.py tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py -q`
- `python3 -m compileall -q lightfee scripts`
- `git diff --check`
- full `pytest -q`
- GitNexus `detect_changes(scope="all")`

### Harness 验收

- Live public probe 在无 secret 情况下可运行。
- Probe JSON 明确输出每个 venue/symbol 的:
  - source
  - depth
  - action/kind
  - sequence fields
  - bridge decision
  - readiness effect
  - old behavior result
  - new behavior result
- Replay fixtures 能在 CI 中复现旧失败并证明修复。

### 云端验收

部署后必须用当前 run_id 过滤，不得混旧 run:

- 当前 run 无 Binance `SYSUSDT`、Aster `RLSUSDT`、Hyperliquid `MAV` snapshot errors。
- Bybit `IRYSUSDT` 不再出现同一个 `book_seq` 对大量更小 REST `snapshot_seq` 的 stale loop。
- `runtime.local_l2_buffer_overflow_rebuild` 对修复目标 symbols 显著消失；如果出现，必须有 structured evidence 证明真实 gap。
- `runtime.local_l2_hot_stale_rebuild` 对修复目标 venue/symbol 降到可解释范围，且 detail 能分类 worker/subscription/timestamp/keepalive。
- Entry-critical primary pairs 要么 `dual_ready=true`，要么有 per-leg root reason。
- 不得用 `local_l2_enabled=false` 或扩大 stale threshold 作为验收方式。

## 官方文档依据

- Binance local order book: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- Bybit WS orderbook: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- Bybit REST orderbook: https://bybit-exchange.github.io/docs/v5/market/orderbook
- OKX API v5 order book: https://www.okx.com/docs-v5/en/
- Bitget UTA depth channel: https://www.bitget.com/api-doc/uta/websocket/public/Order-Book-Channel
- Gate futures order book update: https://www.gate.com/docs/developers/futures/ws/en/#order-book-update
- Hyperliquid info endpoint: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
