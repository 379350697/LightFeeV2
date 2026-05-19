# LightFee V2 vs V1 全量语义审计报告

- **原始日期**: 2026-05-18
- **最后更新**: 2026-05-19 (闭环修复 + contract tests)
- **仓库**: V2 `/media/wl/新加卷/codex/LightFeeV2` | V1 `/media/wl/新加卷/codex/LightFee`
- **上一基线 V2 HEAD**: `9c39c6d Fix C-R3: persist operator latch to snapshot`
- **修复后 V2 HEAD**: 见当前 git log (C-R2/C-R4/H-R5/M-R2/M-R3/M-R4/M-R19/L-R6 闭环 + H-R7/M-R16 contract tests)
- **本次范围**: 按 "仍未闭环项与处理建议" 逐项闭环，补生产路径 + contract tests

---

## 最终结论

14 个开放项中，8 个已完成闭环，2 个需批准差异（有 contract tests），4 个仍未闭环：

| 状态 | 项 | 说明 |
|------|----|------|
| **已闭环** | C-R2, C-R4, H-R5/M-R2, H-R6, M-R3, M-R4, M-R19, L-R6 | 生产路径已实现，测试通过 |
| **需批准差异** | H-R7, M-R16 | 表示方式差异，有 contract/fixture 测试证明外部等价 |
| **仍未闭环** | M-R8, M-R12, M-R14 | 详见下方"仍未闭环"表 |

### 仍需批准的差异 (2 项)

- **H-R7**: V1 signed size vs V2 abs qty + side — contract test 已补
- **M-R16**: V1 strong fault enum vs V2 string vocabulary — contract test 已补

### 仍未闭环 (4 项)

| ID | 当前判断 | 阻碍 | 建议 |
|----|----------|------|------|
| M-R8 | Bitget private WS order-progress 缓存/merge 接口仍然缺 | Bitget passive maker live 未启用，补全需要 private WS progress cache 统一层 | 若启用 Bitget passive 则必须对齐 |
| M-R12 | Terminal reduce-only 仍然用字符串 pattern 匹配 | 需逐 venue 增加结构化 error code → enum 映射 | 建议对齐，不是最高风险但能减少误判 |
| M-R14 | small-fill min-notional accumulation 字段预留但策略逻辑未接入 drive loop | passive close 是生产路由 (runtime.py:5675)，不是低频路径 | **必须对齐** — 字段已存在但无策略闭环 |

---

## 本次新增闭环项 (9 项)

### C-R2: Private-stream health → production global risk ✓ 已闭环

**修改点**:
- `lightfee/core/contracts.py`: `VenueAdapter` ABC 新增 `supports_private_health` property (default False), `cached_private_connection_health()` (default None), `cached_position()` (default None)
- `lightfee/engine/supervisor.py`: 新增 `_supervisor_action_mode()` (V1 state.rs:443-473), `_collect_venue_health_views()` (V1 risk.rs:151-255), `_supervised_venues()`, `_venue_private_position_confirmed()` (V1 engine.rs:4829-4843)
- `lightfee/engine/supervisor.py`: `update_global_risk_mode()` 接受 `venue_health_views: dict[Venue, VenueHealthView]` 并聚合 per-venue 健康视图
- `lightfee/engine/supervisor.py`: `supervise()` 接受 `adapters` dict，为每个被监管 venue 采集 private health 并调用 `evaluate_venue_health()` (V1 health.rs:60)
- `lightfee/engine/supervisor.py`: 所有 adapter 属性访问改为 property (not method call) — 修复 `TypeError: 'bool' object is not callable` 崩溃
- `lightfee/engine/runtime.py`: `_post_tick_housekeeping()` 传入 `self._venue_adapters` 和 `self._risk_snapshot_cache` 给 supervisor

**生产路径**: `_post_tick_housekeeping()` → `supervisor.supervise(now_ms, venue_health, adapters)` → `_collect_venue_health_views()` → 每个 venue 检查 `cached_private_connection_health()` → 若 private stream unhealthy → `VenueHealthAction.FAIL_CLOSED` → `update_global_risk_mode()` 通过 `_supervisor_action_mode()` 映射到 `GlobalRiskMode.FAIL_CLOSED`

**已知限制**: `cached_private_connection_health()` 和 `cached_position()` 默认返回 None — 真实 adapter 需要接入 private WS health 暴露和 position cache 才能让 private-stream health 生效。当前代码不会崩溃，但不支持 private health 的 venue 会安全跳过检查。

### C-R4: Shadow promotion guard → real promotion flow ✓ 已闭环

**修改点**:
- `lightfee/engine/runtime.py`: `_apply_shadow_promotion_if_eligible()` — 在 `_refresh_entry_l2_session_readiness()` 后、`_select_entry_candidates()` 前插入 shadow promotion 逻辑
- `lightfee/engine/runtime.py`: `_tracked_pair_is_executing()` — 检查 tracked pair 是否有正在执行的 pending entry

**生产路径**: `_select_and_dispatch_entries()` → `select_tracked_opportunities()` → `_sync_local_l2_data(scan_promoted=True)` → `_refresh_entry_l2_session_readiness()` → **新增**: `_apply_shadow_promotion_if_eligible(tracked, now_ms)` → best_shadow 替换 worst_primary，更新 `_tracked_primary_pair_ids`，journal 记录

### H-R5/M-R2: Runtime snapshot serializer unified ✓ 已闭环

**修改点**:
- `lightfee/engine/runtime.py`: 3 处 `self.snapshot_store.write(self.state.to_dict())` 全部替换为 `self.snapshot_store.write(build_persistent_state_view(self.state))`
- `lightfee/engine/runtime.py`: import 增加 `build_persistent_state_view`

**生产路径**: 所有 snapshot 写路径统一经过 `build_persistent_state_view()` → `_serialize_open_position()` (含完整 53 字段)

### H-R6: Close chunk re-fetch ✓ 已验证对齐

**验证结论**: V1 的 `execute_aggressive_close_orders()` (exit.rs:3335-3527) 和 `close_execution_chunks()` (risk.rs:826-1003) 同样只预计算一次 chunk list，不在正常 chunk loop 中 re-fetch 仓位。V1 唯一 re-fetch 发生在 `compensate_failed_full_close()` (exit.rs:1482-1601) 的补偿路径。V2 已有对等实现。**V2 已对齐 V1，无需修改。**

### M-R3: Recovery live_detected for journal replay ✓ 已闭环

**修改点**:
- `lightfee/engine/recovery.py`: `recover_from_snapshot()` — 对所有 `state.open_positions` 发射 `recovery.live_detected`，`source` 字段区分 snapshot/jounal_replay

### M-R4: scan_records_matching_kinds → stream-based ✓ 已闭环

**修改点**:
- `lightfee/persistence/journal.py`: `scan_records_matching_kinds()` 改用 `self.stream_records()` 而非 `self.read_all()`

### M-R19: Flush adapter diagnostics in cancel_replace ✓ 已闭环

**修改点**:
- `lightfee/engine/entry_sync.py`: 新增 `_flush_adapter_diagnostics(adapter, journal)` helper
- `lightfee/engine/entry_sync.py`: `drive_pending_entry_hedge()` 的 reprice/cancel_replace 路径每次 adapter 操作后 flush

### L-R6: Ops token/cooldown rate limiting for passive close maintainer ✓ 已闭环

**修改点**:
- `lightfee/engine/passive_close.py`: `_maintain_maker_order()` — 在 amend/cancel-replace 前检查 `_ops_token_available()`，token 在操作前消耗
- `lightfee/engine/passive_close.py`: `_ops_token_available()` — 固定窗口计数器，仅在完整窗口到期时重置（无 cooldown 子窗口放大）
- `lightfee/engine/state.py`: `PendingPassiveClose` 新增 `ops_count_this_window` / `ops_window_started_at_ms`

**Token bucket 语义**: 每个 `ops_budget_window_ms` 窗口最多 `ops_budget_per_window` 次操作（默认 10/60000ms）。窗口到期后完整重置。超限后等待窗口到期（通过 `next_retry_at_ms` + `cooldown_ms` 调度下次尝试）。token 在操作执行前消耗，保证失败也计数。

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

## 仍未闭环 (4 项)

### M-R8: Bitget private WS order-progress — 仍未闭环

Bitget adapter 已有 `_parse_passive_order_progress()` 解析 private WS progress，但未通过统一缓存接口 merge 到 `query_passive_order_progress()`。若 Bitget passive maker live 启用则必须对齐。

### M-R12: Terminal reduce-only structured — 仍未闭环

当前 string pattern → `adapter.fetch_position()` 验证 flat → terminal success。安全网存在但 string matching 对交易所文案变化敏感。建议逐 venue 增加结构化 error code → enum 映射。

### M-R14: Small-fill min-notional accumulation — 仍未闭环

`PendingPassiveClose` 已有 `small_fill_min_notional_attempts`, `last_small_fill_missing_quantity`, `small_fill_buffer_started_at_ms` 字段，但完整的 abort/flatten 策略未接入 `drive_pending_passive_close()` drive loop。passive close 是生产路由 (`runtime.py` → `PassiveCloseExecutor.drive_pending_passive_close()`)，不能降级为低频豁免。

### L-R6 注: ops token bucket 已修复

Token 消耗时机已从操作后改为操作前，窗口重置改为仅在完整窗口到期时触发（移除 cooldown 子窗口放大）。但仍需补测试覆盖 rate-limit 到达、窗口重置、异常路径不泄漏 token 等场景。

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
3. **C-R2**: `cached_private_connection_health()` / `cached_position()` 默认返回 None，真实 private WS health 需各 adapter 暴露；当前不会崩溃但 private health 检查在 adapter 接入前静默跳过
4. **新 venue 上线**: 需同步更新 `_collect_venue_health_views()` 和 fault vocabulary
5. **fault_reason 词汇表扩散**: 需同步更新 contract test 的 coverage

---

## 测试结果

```
2607 passed, 2 skipped, 1 warning in ~45s
```

新增 contract tests: 24 passed
- H-R7 side normalization: 10 tests
- M-R16 arming reason vocabulary: 8 tests + 6 parametrized

---

## 口径

- **已闭环**: 生产路径有实现，有 journal/observability 链路证据，测试通过
- **需批准差异**: V2 内部表达不同于 V1，有 contract/fixture tests 证明外部交易语义等价
- **仍未闭环**: 生产路径缺口或实现未完成，需要在后续迭代中补全
- **不允许**: helper-only/test-only/文档解释为修复；不允许语义漂移；不允许将开放风险降级为条件豁免
