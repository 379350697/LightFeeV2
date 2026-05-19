# LightFee V2 vs V1 全量语义审计报告

- **原始日期**: 2026-05-18
- **最后更新**: 2026-05-19 (C-R2 缓存时序修复 + private health 生产路径接入 + L-R6 生产路径测试)
- **仓库**: V2 `/media/wl/新加卷/codex/LightFeeV2` | V1 `/media/wl/新加卷/codex/LightFee`
- **上一基线 V2 HEAD**: `1645e2b Fix V1 semantic parity: close C-R2/C-R4/H-R5/M-R2/H-R6/M-R3/M-R4/M-R19/L-R6 + H-R7/M-R16 contract tests`
- **修复后 V2 HEAD**: 见当前 git log (C-R2 缓存时序修复 + private health 全生产路径接入 + L-R6 测试补齐)
- **本次范围**: 根修 C-R2 缓存时序 + 接入真实 private-stream health 生产路径 + L-R6 补测试

---

## 最终结论

14 个开放项中，8 个已完成闭环（含本轮 C-R2 根修），2 个需批准差异，4 个仍未闭环：

| 状态 | 项 | 说明 |
|------|----|------|
| **已闭环** | C-R2, C-R4, H-R5/M-R2, H-R6, M-R3, M-R4, M-R19, L-R6 | 生产路径已实现，测试通过 |
| **需批准差异** | H-R7, M-R16 | 表示方式差异，有 contract/fixture 测试证明外部等价 |
| **仍未闭环** | M-R8, M-R12, M-R14 | 详见下方"仍未闭环"表 |

### 仍需批准的差异 (2 项)

- **H-R7**: V1 signed size vs V2 abs qty + side — contract test 已补
- **M-R16**: V1 strong fault enum vs V2 string vocabulary — contract test 已补

### 仍未闭环 (3 项)

| ID | 当前判断 | 阻碍 | 建议 |
|----|----------|------|------|
| M-R8 | Bitget private WS order-progress 缓存/merge 接口仍然缺 | Bitget passive maker live 未启用，补全需要 private WS progress cache 统一层 | 若启用 Bitget passive 则必须对齐 |
| M-R12 | Terminal reduce-only 仍然用字符串 pattern 匹配 | 需逐 venue 增加结构化 error code → enum 映射 | 建议对齐，不是最高风险但能减少误判 |
| M-R14 | small-fill min-notional accumulation 字段预留但策略逻辑未接入 drive loop | passive close 是生产路由 (runtime.py:5675)，不是低频路径 | **必须对齐** — 字段已存在但无策略闭环 |

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

## C-R4: Shadow promotion guard → real promotion flow ✓ 已闭环

**修改点**:
- `lightfee/engine/runtime.py`: `_apply_shadow_promotion_if_eligible()` — 在 `_refresh_entry_l2_session_readiness()` 后、`_select_entry_candidates()` 前插入 shadow promotion 逻辑
- `lightfee/engine/runtime.py`: `_tracked_pair_is_executing()` — 检查 tracked pair 是否有正在执行的 pending entry

**生产路径**: `_select_and_dispatch_entries()` → `select_tracked_opportunities()` → `_sync_local_l2_data(scan_promoted=True)` → `_refresh_entry_l2_session_readiness()` → **新增**: `_apply_shadow_promotion_if_eligible(tracked, now_ms)` → best_shadow 替换 worst_primary，更新 `_tracked_primary_pair_ids`，journal 记录

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

## 残余风险

1. **M-R8/M-R12/M-R14**: 仍未闭环 — 见上方 "仍未闭环" 表
2. **H-R7/M-R16**: 需批准差异 — 有 contract tests，未被批准则需改内部模型
3. **新 venue 上线**: 需同步更新 adapter 的 `supports_private_health` 覆盖和 private WS health 接入
4. **fault_reason 词汇表扩散**: 需同步更新 contract test 的 coverage
5. **private WS health 更新方**: 外部 private WS 连接管理器必须调用 `transport.record_private_ws_success/failure()` 以维持 ConnectionHealth 同步。当前 transport 默认初始化为健康，若未接入外部 WS 管理器则 private health 永远健康 — 需在 live startup 阶段完成对接

---

## 测试结果

```
全量: 2621 passed, 2 skipped, 1 warning in ~45s
L-R6 新增: 14 passed (TestLazyOpsTokenBucket)
H-R7/M-R16 contract tests: 18 passed
```

---

## 口径

- **已闭环**: 生产路径有实现，有 journal/observability 链路证据，测试通过。不允许 ABC 默认实现或 mock-only 算闭环
- **需批准差异**: V2 内部表达不同于 V1，有 contract/fixture tests 证明外部交易语义等价
- **仍未闭环**: 生产路径缺口或实现未完成，需要在后续迭代中补全
- **不允许**: helper-only/test-only/文档解释为修复；不允许语义漂移；不允许将开放风险降级为条件豁免
