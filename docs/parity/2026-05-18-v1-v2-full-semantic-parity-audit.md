# LightFee V2 vs V1 全量语义审计报告

- **原始日期**: 2026-05-18
- **最后更新**: 2026-05-20 (M-R8 根修完成: ImportError + success code + 字段解析 + state 漂移 + fee/timestamp + 10 runtime tests; M-R12/M-R14 完整闭环)
- **仓库**: V2 `/media/wl/新加卷/codex/LightFeeV2` | V1 `/media/wl/新加卷/codex/LightFee`
- **修复后 V2 HEAD**: 见当前 git log
- **本次范围**: M-R8/M-R12/M-R14 完整闭环 + C-R2 live private WS 生产路径 + 文档修正

---

## 最终结论

14 个开放项中，11 个已完成闭环（含本轮 M-R8/M-R12/M-R14），2 个需批准差异：

| 状态 | 项 | 说明 |
|------|----|------|
| **已闭环** | C-R2, C-R4, H-R5/M-R2, H-R6, M-R3, M-R4, M-R8, M-R12, M-R14, M-R19, L-R6 | 生产路径已实现，测试通过 |
| **需批准差异** | H-R7, M-R16 | 表示方式差异，有 contract/fixture 测试证明外部等价 |

### 仍需批准的差异 (2 项)

- **H-R7**: V1 signed size vs V2 abs qty + side — contract test 已补
- **M-R16**: V1 strong fault enum vs V2 string vocabulary — contract test 已补

### 本轮完整闭环 (3 项, 2026-05-19/20)

| ID | 状态 | 修复说明 |
|----|------|----------|
| M-R8 | **已闭环** (两轮根修 2026-05-20) | (1) Parser: V1 全部字段兼容 + normalize_contract_symbol; (2) Subscribe: V1 格式 positions+orders 无 instId; (3) REST detail: `_fetch_bitget_order_detail()` → UTA `/api/v3/trade/order-info` + classic `/api/v2/mix/order/detail` fallback + success code 00000/0 验证; (4) Reconciliation: 复用 `_fetch_bitget_order_detail`; (5) Merge: timestamp max() 语义 (V1 ports.rs:242-260) + state 来源 `bitget_passive_order_state` (V1 bitget.rs:2560-2590), 不来自 merged.state; (6) 根修 ImportError + 字段解析 + fee abs + s→ms 转换; (7) 14 个真实 runtime 测试 (含 timestamp max() + state source order 回归) |
| M-R12 | **已闭环** | (1) Gate label-based empty_position/pending_conflict/order_not_found 结构化检测; (2) pending conflict 正确进入 retry loop (continue 而非 sleep+return); (3) OKX code 51000/51108 + Bybit 110001/20001 + Binance code -2010/-2011 结构化 error code 映射; (4) 所有 venue 的 terminal reduce-only 都经过 `_is_terminal_reduce_only` + exchange flatness 验证 |
| M-R14 | **已闭环** | (1) small-fill buffer: `_small_fill_buffer_decision()` V1 exit.rs:6212 精确复刻; (2) 结构化 min-notional check (同时检查 zero_fill AND notional < buffer_threshold); (3) accumulation attempts + maker terminal escalation; (4) **pre-submit**: `_check_hedge_min_notional()` 在 hedge 提交前执行 normalize_quantity + close_leg_exchange_min_notional_violation, 低于 threshold 时跳过提交并 tracked accumulation; (5) cross-chunk reset |

---

## C-R2: Private-stream health → production global risk ✓ 已闭环（本轮根修）

### 修复 1: 风险快照缓存时序（根因）

**问题**: `_post_tick_housekeeping()` 先调用 `supervisor.supervise()`，再赋值 `self.supervisor._risk_snapshot_cache = self._risk_snapshot_cache`。`supervise()` 内部的 `_collect_venue_health_views()` 调用 `_fetch_risk_snapshot_for_venue()` 读取 supervisor 自身缓存时，看到的是上一 tick 的过期数据或空缓存，导致误判 `risk_snapshot_unavailable` 并进入 fail-closed。

**修复**: `supervise()` 接受显式 `risk_snapshot_cache` 参数，调用方在 `_post_tick_housekeeping()` 中注入后再调用 `supervise()`。supervisor 内部在 `_collect_venue_health_views()` 之前同步缓存。

修改文件:
- `lightfee/engine/supervisor.py`: `supervise()` 签名增加 `risk_snapshot_cache: Optional[dict[Venue, dict]] = None`，方法开头即同步到 `self._risk_snapshot_cache`
- `lightfee/engine/runtime.py`: `_post_tick_housekeeping()` 将 `risk_snapshot_cache=self._risk_snapshot_cache` 作为显式参数传入，删除旧的 post-call 赋值

**验收标准**: runtime 中已有健康 `AccountRiskSnapshot` 时，`_post_tick_housekeeping()` 后不应误入 `fail_closed risk_snapshot_unavailable`。

### 修复 2: 真实 adapter 接入 private WS health（生产路径）

**问题**: 前轮在 ABC 层添加了 `supports_private_health` / `cached_private_connection_health()` / `cached_position()` 默认实现，但真实 venue adapter 全部继承默认值（supports_private_health=False, 两个 cached_* 返回 None），导致私有 WS 不健康时不会注入 global risk。ABC 默认 ≠ 生产闭环。

**修复**: 全生产路径接入 — 从 adapter 到 transport 到 supervisor：

1. **VenueAdapter 基础合约** (`lightfee/core/contracts.py`):
   - `cached_private_connection_health()` 通过 `getattr(self, '_transport', None)` 委托到 transport
   - `cached_position()` 同上委托

2. **VenueTransport** (`lightfee/venues/transport.py`):
   - 新增 `_private_ws_health: ConnectionHealth` （初始健康）
   - 新增 `_position_cache: dict[str, tuple[PositionSnapshot, int]]`（symbol → 快照+时间戳）
   - 新增 `cached_private_connection_health()` → 返回 ConnectionHealth
   - 新增 `cached_position(symbol)` → 返回缓存的 PositionSnapshot（30s TTL）
   - 新增 `record_private_ws_success(now_ms)` / `record_private_ws_failure(now_ms, error)` → 供外部 WS 管理更新健康状态
   - `fetch_position()` 成功后写入 `_position_cache`
   - `fetch_all_positions()` 成功后批量写入 `_position_cache`

3. **真实 adapter** (Binance/Bybit/OKX/Aster/Bitget/Gate/Hyperliquid):
   - 全部覆盖 `supports_private_health` → `return self._transport.mode == "live"`
   - `cached_private_connection_health()` / `cached_position()` 通过基础合约自动委托到 transport

**生产路径**: private WS 连接管理 → `transport.record_private_ws_failure()` → `adapter.cached_private_connection_health()` 返回 unhealthy ConnectionHealth → `supervisor._collect_venue_health_views()` 检测 private_stream_unhealthy → `VenueHealthAction.FAIL_CLOSED` → `GlobalRiskMode.FAIL_CLOSED`

同时: `fetch_position()` → `transport._position_cache` → `adapter.cached_position(symbol)` → `supervisor._venue_private_position_confirmed()` → private connection healthy 但 position 未确认时 → `REDUCE_ONLY`

**验收标准**:
- 真实 adapter 或真实 adapter 包装路径能暴露 private WS unhealthy
- supervisor 聚合后进入 `GlobalRiskMode.FAIL_CLOSED`
- private connection healthy 但 position 未确认时 → reduce-only
- 不依赖 mock/test/ABC 默认实现

### 本轮修改文件

- `lightfee/engine/supervisor.py`: `supervise()` 显式 `risk_snapshot_cache` 参数
- `lightfee/engine/runtime.py`: `_post_tick_housekeeping()` 注入缓存时序修正
- `lightfee/core/contracts.py`: `cached_private_connection_health()` / `cached_position()` 委托 transport
- `lightfee/venues/transport.py`: 导入 ConnectionHealth，新增 private WS health + position cache + 管理方法
- `lightfee/venues/binance.py`: `supports_private_health` → live mode
- `lightfee/venues/bybit.py`: `supports_private_health` → live mode
- `lightfee/venues/okx.py`: `supports_private_health` → live mode
- `lightfee/venues/aster.py`: `supports_private_health` → live mode
- `lightfee/venues/bitget.py`: `supports_private_health` → live mode
- `lightfee/venues/gate.py`: `supports_private_health` → live mode
- `lightfee/venues/hyperliquid.py`: `supports_private_health` → live mode

---

## C-R4: Primary hold and shadow promotion → selector-owned V1 flow（CL-165 本地根修已验证）

The earlier description was stale: `_apply_shadow_promotion_if_eligible()` was
not on the ranked-selector production path and required a shadow Local-L2
session, which violates V1's shadow ownership model. It could not preserve
primary ownership through rank churn.

**修正后的实现**:
- `lightfee/engine/runtime.py`: `_select_v1_entry_tracked_scope()` is the sole
  ranked-frontier decision. It keeps an in-scope, non-transiently-failed
  primary through V1's hold window, fills empty primary slots, and then
  evaluates the best shadow against the worst primary using V1 execution,
  hold, and score-delta rules.
- `lightfee/engine/entry_local_l2.py`:
  `local_l2_tracking_book_ready()` applies the same HOT/WARM, fresh,
  uncrossed book contract directly to shadows without creating a session;
  `primary_hold_window_allows_replacement()` follows V1 by allowing the
  unassigned (`0`) initial-assignment case.
- `lightfee/config/schema.py` and validation: two shadows, a 15-second hold,
  and a 3.0-bps delta are explicit validated V1 defaults, rather than implicit
  `getattr` fallbacks or a disabled shadow frontier.

**生产路径**: ranked candidate flow preserves the previous primary set →
`_select_v1_entry_tracked_scope()` makes the complete ownership decision →
primaries receive Local-L2 session handoff and shadows receive only bounded
warm-pool coverage → composed Local-L2 + WS-BBO readiness revalidates the
selected candidate. This keeps all strict quote, Local-L2, OI, funding,
account-truth, and final-window gates unchanged. Deployment and controlled
live proof remain pending.

---

## H-R5/M-R2: Runtime snapshot serializer unified ✓ 已闭环

**修改点**:
- `lightfee/engine/runtime.py`: 3 处 `self.snapshot_store.write(self.state.to_dict())` 全部替换为 `self.snapshot_store.write(build_persistent_state_view(self.state))`
- `lightfee/engine/runtime.py`: import 增加 `build_persistent_state_view`

**生产路径**: 所有 snapshot 写路径统一经过 `build_persistent_state_view()` → `_serialize_open_position()` (含完整 53 字段)

---

## H-R6: Close chunk re-fetch ✓ 已验证对齐

**验证结论**: V1 的 `execute_aggressive_close_orders()` (exit.rs:3335-3527) 和 `close_execution_chunks()` (risk.rs:826-1003) 同样只预计算一次 chunk list，不在正常 chunk loop 中 re-fetch 仓位。V1 唯一 re-fetch 发生在 `compensate_failed_full_close()` (exit.rs:1482-1601) 的补偿路径。V2 已有对等实现。**V2 已对齐 V1，无需修改。**

---

## M-R3: Recovery live_detected for journal replay ✓ 已闭环

**修改点**:
- `lightfee/engine/recovery.py`: `recover_from_snapshot()` — 对所有 `state.open_positions` 发射 `recovery.live_detected`，`source` 字段区分 snapshot/journal_replay

---

## M-R4: scan_records_matching_kinds → stream-based ✓ 已闭环

**修改点**:
- `lightfee/persistence/journal.py`: `scan_records_matching_kinds()` 改用 `self.stream_records()` 而非 `self.read_all()`

---

## M-R19: Flush adapter diagnostics in cancel_replace ✓ 已闭环

**修改点**:
- `lightfee/engine/entry_sync.py`: 新增 `_flush_adapter_diagnostics(adapter, journal)` helper
- `lightfee/engine/entry_sync.py`: `drive_pending_entry_hedge()` 的 reprice/cancel_replace 路径每次 adapter 操作后 flush

---

## L-R6: Ops token/cooldown rate limiting for passive close maintainer ✓ 已闭环

**修改点**:
- `lightfee/engine/passive_close.py`: `_maintain_maker_order()` — 在 amend/cancel-replace 前检查 `_ops_token_available()`，token 在操作前消耗
- `lightfee/engine/passive_close.py`: `ops_token_available()` — 提取为模块级函数，固定窗口计数器，仅在完整窗口到期时重置（无 cooldown 子窗口放大）
- `lightfee/engine/state.py`: `PendingPassiveClose` 新增 `ops_count_this_window` / `ops_window_started_at_ms`

**Token bucket 语义**: 每个 `ops_budget_window_ms` 窗口最多 `ops_budget_per_window` 次操作（默认 10/60000ms）。窗口到期后完整重置。超限后等待窗口到期（通过 `next_retry_at_ms` + `cooldown_ms` 调度下次尝试）。token 在操作执行前消耗，保证失败也计数。

**生产路径测试** (`tests/engine/test_passive_close_semantic_parity.py::TestLazyOpsTokenBucket`, 14 tests):
- `test_rate_limit_reached_emits_correct_journal_kind` — 预算耗尽
- `test_rate_limit_sets_next_retry_with_cooldown` — cooldown 调度
- `test_window_not_complete_does_not_reset_counter` — 半窗口不重置
- `test_window_expires_resets_counter` — 完整窗口到期重置
- `test_window_exactly_at_boundary` / `test_window_just_past_boundary` — 边界条件
- `test_zero_window_start_grants_token` — 首次调用
- `test_token_consumed_before_operation` / `test_multiple_failures_consume_tokens` — 失败也计数
- `test_cooldown_is_not_sub_window_reset` — cooldown 非子窗口
- `test_cooldown_scheduling_without_rate_amplification` — 操作频率不放大
- `test_single_token_budget` / `test_large_budget_not_exhausted_early` / `test_very_short_window` — 边界

---

## 需批准差异 (2 项，均有 contract tests)

### H-R7: abs qty + side vs signed size ✓ Contract test 已补

**差异**: V1 signed size 推导 flatten side。V2 `PositionSnapshot.quantity` (abs) + `PositionSnapshot.side` 表达方向。

**Contract test**: `tests/test_parity_contract_h7_m16.py::TestH7SideNormalizationEquivalence` (10 tests)
- V1 signed ↔ V2 (side, abs) 双向转换可逆
- 所有 6 个 venue 边界等价
- Flatten side 推导等价
- 零量边界正确

### M-R16: string fault vocabulary vs strong enum ✓ Contract test 已补

**差异**: V2 `_derive_arming_reason_from_book()` 使用字符串关键词而非 V1 强类型 `EntryLocalL2LegFault` enum。

**Contract test**: `tests/test_parity_contract_h7_m16.py::TestM16ArmingReasonVocabulary` (8 tests)
- 全部 7 个 V1 fault 类型 → 确定的 arming reason
- 20 种已知 fault 子串全覆盖
- 大小写不敏感
- 未知 fault → 安全回退 `BOOK_STATUS_TRANSITION`

---

## 前轮已闭环项 (本次未改)

### CRITICAL: C-R1, C-R3, C-R5, C-R6, C-R7, C-R8
### HIGH: H-R1, H-R2, H-R3, H-R4, H-R8, H-R9, H-R10, H-R11, H-R12, H-R13
### MEDIUM: M-R1, M-R5, M-R6, M-R7, M-R9, M-R10, M-R11, M-R13, M-R15, M-R17, M-R18
### LOW: L-R1, L-R2, L-R3, L-R4, L-R5, L-R7, L-R8, L-R9

---

## C-R2 补充根修: Live Private WS 生产路径 5 项修复 (2026-05-19)

C-R2 在上轮已标记闭环（缓存时序修复 + private health 生产路径接入），但严格验收发现 5 项遗留问题。本轮逐项修复：

### 修复 1: OKX _build_okx_ct_val_map() 规范符号查找

**问题**: V2 用 vendor symbol (symbol_map key) 查 `_symbol_metadata`，V1 `okx_ct_val_map_from_cached_metadata()` (okx.rs:5791-5805) 用 canonical symbol (symbol_map value) 查 metadata。V2 在 metadata 按 canonical symbol 索引时拿不到 ctVal，且缺失时不回退到 1.0。

**修复**: `_build_okx_ct_val_map()` 优先按 canonical symbol 查找，vendor key 作为 fallback；每个 inst_id 缺失时默认 ct_val=1.0。

**测试**: `tests/test_v1_private_ws_parity.py::TestOkxCtValMap` (5 tests):
- `test_canonical_symbol_lookup` — metadata={"ETHUSDT": {ct_val: 0.1}}, symbol_map={"ETH-USDT-SWAP": "ETHUSDT"} → {"ETH-USDT-SWAP": 0.1}
- `test_vendor_key_fallback` — metadata keyed by vendor symbol → fallback 命中
- `test_missing_metadata_defaults_to_one` — 空 metadata → 所有 inst_id=1.0
- `test_mixed_metadata` — 部分有 metadata、部分缺失 → 混合正确
- `test_zero_ct_val_ignored_defaults_to_one` — ct_val=0 被忽略 → 回退 1.0

### 修复 2: Runtime _current_tracked_private_symbols() 收集 pending_passive_closes

**问题**: 代码遍历 `self.state.pending_closes` 而非 `self.state.pending_passive_closes`，注释写 pending passive closes 但实际漏掉了 passive close 的 symbol 收集。`position_snapshot` 为 None 时也没有 fallback。

**修复**:
- 改为遍历 `self.state.pending_passive_closes`
- `position_snapshot` 为 None 时通过 `position_id` 回退到 `self.state.open_positions` 查找

**测试**: `tests/test_v1_private_ws_parity.py::TestRuntimePrivateSymbols` (6 tests):
- `test_pending_passive_closes_produces_symbols` — 含 position_snapshot → correct long/short venue symbols
- `test_pending_passive_closes_without_snapshot_falls_back_to_open_positions` — position_snapshot=None → 回退 open_positions
- `test_tracked_pair_ids_parsed_correctly` — canonical pair_id 解析
- `test_open_positions_produces_symbols` — open_positions → venue symbols
- `test_empty_state_returns_empty_dict` — 空状态 → 空 dict
- `test_pending_entries_produces_symbols` — pending_entries → venue symbols

### 修复 3: Worker lifecycle 测试补齐 (Aster/Bybit/Bitget/Gate/Hyperliquid)

**问题**: 前轮只有 Binance/OKX 有 fake websocket lifecycle 测试，其他 5 个 venue 没有。

**修复**: 在 `tests/test_v1_private_ws_parity.py` 新增 5 个 venue 的 worker lifecycle 测试类：
- `TestAsterWorkerLifecycle` (4 tests): listenKey success, listenKey failure, connect failure, close failure
- `TestBybitWorkerLifecycle` (3 tests): auth+subscribe success, connect failure, auth send failure
- `TestBitgetWorkerLifecycle` (3 tests): login+subscribe success, connect failure, login send failure
- `TestGateWorkerLifecycle` (4 tests): signed subscribe+message success, connect failure, subscribe send failure, futures.positions event
- `TestHyperliquidWorkerLifecycle` (4 tests): hydrate+subscribe success, connect failure, NoData error triggers failure, user fill event

所有测试走生产 worker 方法 (`start_*_private_ws()`)，验证 `record_private_ws_success()`/`record_private_ws_failure()` 在真实 connect/auth/subscribe/message/error 路径上被调用。

### 修复 4: Passive progress / side / terminal state 审查

**审查结论**: 无需修改。证据：
- `query_passive_order_progress()` (transport.py:3809-3854): private WS 结果对 CANCELED/REJECTED/EXPIRED/OPEN 状态 + 0 fill 全部返回，不走 REST 覆盖 ✓
- `_poll_maker_progress()` (passive_close.py:404-407): 调用时传 `side=maker_side` (SELL for long, BUY for short) ✓
- `_probe_order_dead()` (passive_close.py:1213-1216): 调用时传 `side=maker_side` ✓
- runtime `query_passive_order_progress` (runtime.py:2929-2936): 从 `pending.maker_side()` 取 side ✓

### 修复 5: 验证结果 (修复 1-4)

```
python3 -m compileall -q lightfee                                    # 通过
pytest tests/test_ws_resilience.py tests/test_private_ws_state.py \
       tests/test_venue_private_ws_parsers.py \
       tests/test_v1_private_ws_parity.py -q                         # 118 passed
pytest -q                                                             # 2719 passed, 2 skipped
rg "record_private_ws_success|record_private_ws_failure" lightfee     # 7 venue 生产 worker 全有调用
rg "start_private_ws|private_ws_worker" lightfee                      # runtime → transport → 7 venue workers
gitnexus_detect_changes(scope="all")                                  # low risk, 0 affected processes
```

### 修复 6: Tracked pair id lowercase → canonical private WS symbol (根因修复, 2026-05-19)

**问题**: `make_candidate_pair_id("ETHUSDT","binance","bybit")` 产生 `"ethusdt:binance->bybit"`（小写 symbol 是稳定身份格式，符合设计）。`_current_tracked_private_symbols()` 解析 pair_id 后直接把 `"ethusdt"`（小写）放进 private WS symbols 集合。随后：

1. `start_private_ws(["ethusdt"])` → `_venue_symbol("ethusdt")` → 对 Binance/Bybit/Aster 等 identity venue 返回 `"ethusdt"`（小写）
2. `symbol_map = {"ethusdt": "ethusdt"}` — 所有 key 都是小写
3. Binance 推送 `"s":"ETHUSDT"`（大写），parser 做 `symbol_map.get("ETHUSDT")` → `None` → **live private WS fill/position 消息被静默丢弃**
4. OKX/Gate/Hyperliquid 等需要 venue symbol 转换的 venue，`_venue_symbol("ethusdt")` 因输入不是 canonical uppercase 而无法正确映射

**根因**: `make_candidate_pair_id()` 小写 symbol 是其稳定身份语义（不修改），但 runtime 消费 pair_id 时未还原 canonical symbol。
数据流: `"ETHUSDT" → make_candidate_pair_id() → "ethusdt" → _current_tracked_private_symbols() → "ethusdt" → symbol_map → parser miss`

**修复**: `_current_tracked_private_symbols()` 解析 pair_id 后对 symbol 调用 `.upper()` 还原 V2 内部 canonical 形式（例如 `"ETHUSDT"`）。同时修复 pipe-delimited fallback 路径。

修改文件:
- `lightfee/engine/runtime.py`: `_current_tracked_private_symbols()` — 两处 `.upper()` canonical 还原（canonical `->` 格式 + pipe-delimited fallback）

**测试**: `tests/test_v1_private_ws_parity.py` 新增/修改:
- `TestRuntimePrivateSymbols.test_tracked_pair_ids_lowercase_from_make_candidate_pair_id` — 调用 `make_candidate_pair_id("ETHUSDT","binance","bybit")` 确认输出小写 pair_id，断言 runtime 输出 `"ETHUSDT"` 而非 `"ethusdt"`
- `TestRuntimePrivateSymbols.test_pipe_delimited_pair_id_also_canonicalizes` — pipe 格式也 canonical 化
- `TestRuntimePrivateSymbols.test_okx_pair_id_produces_canonical_symbol_for_venue_conversion` — OKX pair_id → canonical `"ETHUSDT"` → 交给 venue worker 各自转换
- `TestRuntimePrivateSymbols.test_gate_and_hyperliquid_pair_ids_also_canonicalize` — Gate/Hyperliquid pair_id → canonical
- `TestRuntimePrivateSymbols.test_multiple_pair_ids_mixed_case_produce_unique_canonical` — 多 pair_id 混合大小写 → 去重 canonical
- `TestLowercasePairIdSymbolMapRegression` (6 tests): Binance/Aster/Bybit parser symbol_map 回归 + 旧 bug 复现 + 端到端路径
  - `test_binance_trade_lite_canonical_symbol_map_match` — canonical map → `"s":"ETHUSDT"` → match
  - `test_lowercase_symbol_map_causes_binance_parser_miss` — 小写 map → `"s":"ETHUSDT"` → miss（旧 bug 复现）
  - `test_aster_trade_lite_canonical_symbol_map_match` — Aster 同
  - `test_bybit_execution_canonical_symbol_map_match` — Bybit execution topic match
  - `test_lowercase_symbol_map_causes_bybit_parser_miss` — Bybit 旧 bug 复现
  - `test_end_to_end_make_candidate_pair_id_to_parser_resolution` — 全链路: make_candidate_pair_id → runtime canonicalize → symbol_map → parser resolve

### 修复 6 验证结果 (2026-05-19)

```
python3 -m compileall -q lightfee                                    # 通过
pytest tests/test_ws_resilience.py tests/test_private_ws_state.py \
       tests/test_venue_private_ws_parsers.py \
       tests/test_v1_private_ws_parity.py -q                         # 129 passed (+11)
pytest -q                                                             # 2730 passed
python3 -c "make_candidate_pair_id('ETHUSDT','binance','bybit') +
             _current_tracked_private_symbols() → ETHUSDT"           # 通过, 无小写泄漏
python3 -c "Binance TRADE_LITE s='ETHUSDT' → symbol_map canonical    # 通过, parser 解析正确
             resolution; old bug lowercase mismatch reproduced"       # 旧 bug 复现确认
rg "class Test.*WorkerLifecycle" tests/test_v1_private_ws_parity.py  # 7 venue 全存在
rg "class TestOkxCtValMap" tests/test_v1_private_ws_parity.py        # 存在
rg "class TestRuntimePrivateSymbols" tests/test_v1_private_ws_parity.py  # 存在
rg "make_candidate_pair_id" tests/test_v1_private_ws_parity.py       # 7 处, 覆盖所有新测试
```

### C-R2 当前状态: 已闭环（补充根修完成 + lowercase symbol 根因修复）

C-R2 的生产路径证据链完整：
1. runtime._post_tick_housekeeping → _ensure_private_ws_started → transport.start_private_ws → venue-specific start_*_private_ws → worker loop
2. worker loop 所有 connect/auth/login/subscribe/message/ping/pong/close/error 路径 → record_private_ws_success/failure
3. 7 个 venue (Binance/Aster/OKX/Bybit/Bitget/Gate/Hyperliquid) 全部有生产 worker 和 lifecycle 测试
4. _current_tracked_private_symbols 正确收集 open_positions + tracked_pair_ids + pending_entries + pending_passive_closes
5. OKX ctVal 按 canonical symbol 查找，默认 1.0
6. Passive progress private-first 语义正确，maker side 传递正确
7. **新增**: tracked pair id lowercase → canonical private WS symbol 还原 — make_candidate_pair_id() 小写 symbol 在 runtime 消费端还原为 canonical uppercase，Binance/Bybit/Aster parser exact-match 不再丢消息，OKX/Gate/Hyperliquid venue symbol 转换正确输入

变更文件: `lightfee/engine/runtime.py`, `tests/test_v1_private_ws_parity.py`

---

---

## M-R8/M-R12/M-R14 部分修复 (2026-05-19/20)

### M-R8: Bitget passive order progress — 生产路径根修 (2026-05-19 初修 + 2026-05-20 根修)

#### 初修 (2026-05-19): Parser 字段兼容 + normalize_contract_symbol fallback

**问题**: V2 Bitget parser 只吃 orderId/accBaseVolume/avgPrice 三个字段，symbol_map miss 直接丢弃消息，无法消费 V1 fixture `instId=BTCUSDT` 类 V1 常见 payload。

**修复**:
- `lightfee/venues/bitget_private_ws.py`: `_handle_bitget_order_data()` 重写为 V1 字段兼容
- `_handle_bitget_position_data()` 同 V1
- 新增 `_normalize_contract_symbol()` / `_json_string()` / `_json_f64()` / `_json_i64()`

**测试** (8 tests, `TestBitgetV1PrivateOrderParser`): 已覆盖 V1 fixture 字段变体、symbol_map fallback、position 链路

#### 根修 (2026-05-20): ImportError + 字段缺失 + success code + state 漂移 + fee/timestamp

**硬阻塞 (ImportError)**: `lightfee/venues/transport.py` `_fetch_bitget_order_detail()` 第 4010 行:
```python
from lightfee.core.errors import TransportErrorCategory
```
`TransportErrorCategory` 实则在 `transport.py` 本模块第 119 行定义。`lightfee.core.errors` 无此类 → 每次调用均抛出 `ImportError`，被上层 `except (TransportError, Exception): pass` 吞掉 → REST detail 和 reconciliation 均不发出请求 → `query_passive_order_progress()` 返回 None。

**连带缺陷 (逐行 V1 对照后发现)**:

1. **缺失 success code 验证**: `_fetch_bitget_order_detail` 只检查 absent order 码 (40109/43001)，不验证 success code (00000/0)。V1 的 `bitget_data()` 强制要求 code=00000/0，非成功非缺席响应返回 error。V2 会把错误响应当合法数据传给上层 → 修复：UTA 和 classic 路径均增加 `if code not in ("00000", "0"): return None`

2. **`_query_passive_order_progress_bitget` 委托通用 `_parse_passive_order_progress`** (V2 架构便捷性): 通用 parser 缺少 Bitget 专属字段：
   - cum_qty 缺 `filled_amount`
   - fee 缺 `totalFee`/`filledFee`/`feeDetail.totalFee`（且不 abs()）
   - timestamp 缺 `update_time_ms`、无秒→毫秒转换
   - state 使用通用状态映射而非 V1 `bitget_passive_order_state`（需比较 original_quantity 判断 FILLED）
   → 修复：完全重写 REST detail 解析路径，在 `_query_passive_order_progress_bitget` 内按 V1 风格直接提取 Bitget 专属字段（_bf/_bf_f64/_bf_i64/_bf_fee 辅助函数），含 V1 `bitget_passive_order_state` 逻辑

3. **`_parse_order_status_bitget` order_id 缺 `ordId` fallback**: V1 用 `["orderId", "ordId"]`，V2 只用 `"orderId"` → 修复：增加 `ordId` fallback

4. **Bitget private WS state 语义漂移**: V1 `handle_bitget_private_message` orders 路径显式设置 `state: None` (bitget.rs:4915)，state 仅在 merge 时由 REST detail 决定。V2 `_handle_bitget_order_data` 从 WS message 解析 `status` 并推导 PassiveOrderState → 修复：`state=None` 对齐 V1

5. **V2 `_parse_passive_order_progress` 补充 Bitget 字段** (作为安全网): cum_qty 增 `baseVolume`/`fillSz`/`size`；avg_price 增 `priceAvg`/`fillPriceAvg`/`averagePrice`；last_fill_time 增 `uTime`

**修复文件**:
- `lightfee/venues/transport.py`: 删除错误 import；`_fetch_bitget_order_detail` 增 success code 验证；`_query_passive_order_progress_bitget` 完整重写为 V1 风格直接字段解析 (含 fee abs/timestamp 转换/state 检测)；`_parse_order_status_bitget` 增 `ordId` fallback；`_parse_passive_order_progress` 补 Bitget 字段
- `lightfee/venues/bitget_private_ws.py`: `_handle_bitget_order_data` 的 `state=None` 对齐 V1
- `tests/test_v1_private_ws_parity.py`: `test_zero_fill_terminal_order_still_recorded` 的 state 断言对齐 V1

**新增/更新 runtime 测试** (10 tests, `TestBitgetPassiveProgressEndpoint`):
1. `test_fetch_bitget_order_detail_hits_uta_not_place_order` — fake `_request` 断言调用 `/api/v3/trade/order-info`，不含 place-order
2. `test_fetch_bitget_order_detail_no_import_error` — 直接调用无 ImportError
3. `test_query_passive_order_progress_bitget_uta_happy_path` — 端到端 UTA → parse → merge → PassiveOrderProgress (cum/avg/fee/order_id/client_order_id 全量验证)
4. `test_uta_request_rejected_falls_back_to_classic` — UTA REQUEST_REJECTED → classic `/api/v2/mix/order/detail` (productType=USDT-FUTURES)
5. `test_reconciliation_participates_in_merge_with_highest_qty` — 三源不同量，证明 reconciliation 确实被调用 (>=2 次 _request)
6. `test_reconciliation_wins_when_higher_qty_than_detail_and_private` — reconciliation 0.030 > private 0.012 > detail 0.005 → reconciliation 字段获胜
7. `test_absent_order_code_40109_returns_none` — 40109 → None
8. `test_merge_has_three_sources_recon_detail_private` — reconciliation qty=0.015 最高 → 获胜 (source="reconciliation")
9. `test_equal_quantity_reconciliation_wins_over_detail` — 等量时 reconciliation 优先
10. `test_identity_fallback_from_detail_when_recon_missing` — reconciliation 有 fill 但无 order_id → fallback detail 的 identity

**生产路径逐段验证证据** (`_request` monkeypatch 记录真实 HTTP 调用):

| 路径段 | V2 调用 | V1 对应 |
|--------|---------|---------|
| UTA endpoint | `GET /api/v3/trade/order-info` (params: orderId/clientOid) | bitget.rs:3685 |
| Absent order | code=40109 → return None | bitget_payload_indicates_absent_order |
| Success code check | code∉{00000,0} → return None | bitget_data() |
| Classic fallback | `GET /api/v2/mix/order/detail` (productType=USDT-FUTURES) | bitget.rs:3677 |
| REST detail parse | V1 multi-key fallback: baseVolume/priceAvg/uTime + fee abs + s→ms | bitget.rs:2496-2533 |
| Reconciliation | 复用 `_fetch_bitget_order_detail` → `_parse_order_status_bitget` | bitget.rs:2912-2949 |
| Merge | `merge_passive_progress_sources(detail, reconciliation, private)` | passive_progress.rs:6 |
| State | V1 `bitget_passive_order_state` 含 original_qty FILLED 检测 | bitget.rs:3755-3795 |

#### 补充根修 (2026-05-20 第二轮): timestamp max() + state source order 漂移

上轮验收发现全量 pytest 通过，但逐行 V1 对照仍发现两个语义漂移。

**漂移 1: `resolve_cumulative_order_progress` timestamp 选择逻辑**

- **V1 语义** (ports.rs:242-260): `updated_at_ms` 和 `last_fill_at_ms` 在 highest-quantity sources 内取 `max()`，没有才 fallback 到所有 sources 的 `max()`
- **V2 旧行为**: `last_fill_at_ms` 取 highest 内第一个非空值 (`break`)，`updated_at_ms` 取所有 sources 的 `max()` (跳过了 highest-only 阶段)
- **漂移影响**: reconciliation 和 detail 等量时，reconciliation 的 timestamp 总是赢(first-in-list)，即使 detail 的 timestamp 更新。V1 会取 max
- **修复** (`lightfee/marketdata/private_ws.py:569-599`):
  - `last_fill_at_ms`: highest sources 内 `max()` → fallback 到 all sources `max()`
  - `updated_at_ms`: highest sources 内 `max()` → fallback 到 all sources `max()` (原直接取 all sources)
  - 同时补 `state` 的 fallback 到 all sources (V1 有，V2 原缺)
- **回归测试** (3 tests, `TestBitgetPassiveProgressEndpoint`):
  - `test_timestamp_last_fill_max_within_highest_sources` — 等量时 reconciliation=100 vs detail=200 → max() 选 200
  - `test_timestamp_updated_at_max_within_highest_fallback_to_all` — highest 无 updated_at_ms → fallback 到 all sources max=300
  - `test_timestamp_last_fill_fallback_to_all_sources` — highest 无 last_fill → fallback 到 all sources max=500
- **V1 对应**: ports.rs:242-260 (`filter_map(|s| s.last_fill_at_ms).max().or_else(|| sources.iter()...max())`)

**漂移 2: Bitget final state 来​源顺序**

- **V1 语义** (bitget.rs:2560-2590): state 由 `bitget_passive_order_state(status, merged_cumulative_quantity, original_quantity)` 决定，参数来自 REST detail。只有 `detail.is_none() && private_progress.is_some() && cumulative_quantity <= 0.0` 时返回 Resting。`merged.state` 完全不被用于 Bitget state
- **V2 旧行为**: 先用 `merged.state` (resolve_cumulative_order_progress 输出的最高 qty 源的 state)，再 fallback 到 `parsed_state` (REST detail 的 bitget_passive_order_state)。私有 WS 若带 state 会覆盖 REST detail 推导的状态。且 `parsed_state` 使用 pre-merge `detail_progress.cumulative_quantity`，非 `merged.cumulative_quantity`
- **漂移影响**: 
  - 场景: REST detail status=open, baseVolume=0.5, size=1.0; private WS quantity=0.5 但 state=FILLED
  - 旧 V2: `merged.state` → private WS 的 FILLED 胜出 → **错误返回 FILLED**
  - V1: `bitget_passive_order_state("open", 0.5, Some(1.0))` → cum_qty>0 → **PARTIALLY_FILLED** ✓
  - 另一个场景: REST detail cum_qty=0 status=open; private WS cum_qty=0.5
  - 旧 V2: `parsed_state` 用 detail cum_qty=0 → **open 状态**
  - V1: merged cum_qty=0.5 > 0 → **PARTIALLY_FILLED** ✓
- **修复** (`lightfee/venues/transport.py:4083-4110`):
  - 移除 `merged.state` 和预计算 `parsed_state`
  - 提取 `_btg_status_str`、`_btg_original_qty` 在 REST detail 解析段保存
  - Post-merge 用 `merged.cumulative_quantity` + REST detail 的 status/original_qty 重新计算 `bitget_passive_order_state`
  - 保留 V1 fallback: `detail_data is None and private_progress and merged.cumulative_quantity <= 0 → OPEN`
- **回归测试** (1 test):
  - `test_bitget_state_comes_from_rest_detail_not_private_ws` — REST detail: status=open, baseVolume=0.5, size=1.0; private WS: state=FILLED → final state=PARTIALLY_FILLED (cum_qty>0 with status=open), NOT FILLED
- **V1 对应**: bitget.rs:2560-2590 (state = bitget_passive_order_state(status, cumulative_quantity, original_quantity)); bitget.rs:3755-3795 (bitget_passive_order_state 函数); passive_progress.rs:24-34 (build_passive_order_progress 接收独立 state 参数)

### M-R12: Gate terminal reduce-only 结构化 error classifier

**问题**: V2 close_executor 仅用 `contains("reduce_only")`, `contains("empty position")` 等字符串 pattern 判断 terminal reduce-only，无法区分 Gate pending conflict (可重试) 和 empty position (terminal)。

**修复**:
- `lightfee/engine/close_executor.py`: 新增模块级函数:
  - `_extract_gate_error_fields()`: 从 Gate 错误串提取 `label=` / `msg=`
  - `_classify_close_leg_error()`: 结构化分类 → dict with `empty_position`, `order_not_found`, `pending_conflict`, `terminal_reduce_only`
  - `_is_terminal_reduce_only()`: 综合判断 — empty_position → terminal; pending_conflict → NOT terminal; 通用 fallback
  - `_string_contains_any()`: 通用字符串匹配辅助
- `_submit_close_leg_with_retry()` 更新为: 先结构化分类, pending_conflict → journal + 可重试, empty_position → 验证 exchange flat → terminal success

**测试** (10 tests, `TestM12CloseLegErrorClassification`):
- Gate `label=reduce_exceeded msg=empty position` → terminal
- 大小写不敏感
- `label=reduce_only_fail msg=pending order conflicts with reduce order` → NOT terminal
- `label=reduce_exceeded` with pending order → conflict, not terminal
- `label=ORDER_NOT_FOUND` → terminal
- Generic `reduce_only` text → terminal fallback
- Non-terminal reduce_only text → 结构化分类正确不误判
- Unrelated error → not terminal
- `_string_contains_any()` 辅助验证

### M-R14: small-fill buffer 完整接入 passive close drive loop

**问题**: V2 `PendingPassiveClose` 有字段 `small_fill_min_notional_attempts` / `last_small_fill_missing_quantity` / `small_fill_buffer_started_at_ms` 和 `PassiveCloseExecutor` 中 chunk advance 时的 reset 逻辑，但 drive loop 从未使用这些字段。maker 小额成交导致 hedge delta 低于 min-notional 时，不会 buffer，直接提交无效 hedge。

**修复**:
- `lightfee/engine/passive_close.py`: 
  - `_small_fill_buffer_decision()`: V1 `passive_close_small_fill_buffer_decision()` 精确复刻 (exit.rs:6212-6242)
  - `PassiveCloseConfig` 新增: `small_fill_buffer_notional_quote` (default 10.0), `small_fill_buffer_max_wait_ms` (default 5000), `maker_min_notional_accumulation_attempts` (default 5)
  - `drive_pending_passive_close()`: delta hedge 段增加:
    1. 计算 `buffered_notional = unhedged_gap * hedge_price_hint`
    2. 判断 `can_accumulate_small_fill` (maker 非 terminal state)
    3. `_small_fill_buffer_decision()` → should_buffer: set `small_fill_buffer_started_at_ms`, journal buffering, short retry
    4. wait_expired: journal buffer expired, fall through to hedge
    5. hedge 失败 → 检查 `last_small_fill_missing_quantity` 增量 → `small_fill_min_notional_attempts += 1`
    6. attempt 耗尽或 maker terminal → escalate to DUAL_TAKER
  - Chunk advance 中原有 reset 逻辑已存在（`advance_chunk` → `small_fill_min_notional_attempts = 0`, `last_small_fill_missing_quantity = 0.0`, `small_fill_buffer_started_at_ms = None`）

**测试** (14 tests):
- `TestM14SmallFillBufferDecision` (9 tests): below threshold buffer, above threshold no buffer, cannot accumulate, zero config disabled, zero notional, buffer expired, buffer active, remaining_wait ≥ 1, config defaults
- `TestM14SmallFillBufferStateTransitions` (5 tests): attempt increment, field reset on chunk advance, cross-chunk non-contamination, accumulation exhausted threshold, maker terminal escalation

---

## 残余风险

1. **H-R7/M-R16**: 需批准差异 — 有 contract tests，未被批准则需改内部模型
2. **新 venue 上线**: 需同步更新 adapter 的 `supports_private_health` 覆盖、private WS health 接入、error code 映射
3. **fault_reason 词汇表扩散**: 需同步更新 contract test 的 coverage
4. **M-R8 已验证但需注意**: 本轮的 reconciliation 在 `_query_passive_order_progress_bitget` 内调用了两次 `_fetch_bitget_order_detail` (一次 REST detail，一次 reconciliation)。V1 同样两次调用 `fetch_bitget_order_detail` (bitget.rs:2490 + 2919)。这是 V1 行为，不是 V2 冗余。未来若 reconciliation 独立为通用路径，需确保 Bitget reconciliation 保留 UTA→classic fallback。


---

## 测试结果

```
全量: 2799 passed, 0 failed, 2 skipped, 1 warning (2026-05-20, M-R8 第二轮 timestamp+state 根修后)
Private WS parity: 75 passed (含 TestV1PrivateWsResolver)
Bitget parser: 15 passed
Merge tests: 9 passed (TestMergePassiveProgressSources)
M-R8 Bitget passive progress runtime: 14 passed (TestBitgetPassiveProgressEndpoint: 10 base + 3 timestamp max() + 1 state source order)
M-R8 parse/subscribe: 8 passed (TestBitgetV1PrivateOrderParser)
M-R12 classifier: 10 passed (TestM12CloseLegErrorClassification) + 9 passed (TestM12VenueErrorCodes)
M-R14 buffer+pre-submit: 14 passed (buffer decision/state transitions) + 5 passed (TestM14PreSubmitMinNotional)
Passive close/close: 138 passed
L-R6: 14 passed (TestLazyOpsTokenBucket)
H-R7/M-R16: 18 passed
```

---

## 口径

- **已闭环**: 生产路径有实现，有 journal/observability 链路证据，测试通过。不允许 ABC 默认实现或 mock-only 算闭环
- **需批准差异**: V2 内部表达不同于 V1，有 contract/fixture tests 证明外部交易语义等价
- **仍未闭环**: 生产路径缺口或实现未完成，需要在后续迭代中补全
- **不允许**: helper-only/test-only/文档解释为修复；不允许语义漂移；不允许将开放风险降级为条件豁免
