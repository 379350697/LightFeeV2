# Workstream 2: Entry Gate Pipeline

## 目标
修复 `runtime.py` 和 `discovery.py` 中 entry 门控与 L2 激活差距。

## 项目路径
- V2: `/media/wl/新加卷/codex/LightFeeV2/`
- V1 参考: `/media/wl/新加卷/codex/LightFee/src/execution_core/market_data.rs` (entry gates), `engine.rs` (tick flow)

---

## 任务 1: B15 每 tick 多个 entry

**文件**: `lightfee/engine/runtime.py`

**位置**: `tick()` 方法中 `_dispatch_entry` 调用处（约 600 行）

**当前**:
```python
mid_price = price_hints.get(tradeable[0].symbol, 0.0)
await self._dispatch_entry(tradeable[0], now_ms, price_hint=mid_price)
```

**改为**: 遍历 tradeable 列表，直到 `len(open_positions) >= max_concurrent_positions` 或无更多候选。V1 遍历整个 selected shortlist。

```python
max_slots = self.config.strategy.max_concurrent_positions
for candidate in tradeable:
    if len(self.state.open_positions) >= max_slots:
        break
    mid_price = price_hints.get(candidate.symbol, 0.0)
    await self._dispatch_entry(candidate, now_ms, price_hint=mid_price)
```

**V1 参考**: `tick_with_scan_mode` 中 `for candidate in selected_candidates { try_open_position_with_mode(...) }`

---

## 任务 2: B12 运行时 entry guards

**文件**: `lightfee/engine/runtime.py`

**改动**: 在 `_dispatch_entry()` 开头添加 gate 函数序列。每个 gate 返回 `(allowed: bool, blocked_reason: str)`。

新增 gate 方法（添加到 LiveRuntime 类）：

1. **`_gate_pending_entry_dedup(candidate)`**: 
   - 检查 `self.state.pending_entries` 中是否有同 `(long_venue, short_venue, symbol)` 的待处理 entry → 拒绝

2. **`_gate_pending_close_reconciliation(candidate)`**:
   - 检查 `self.state.pending_closes` 中是否有同 symbol+pairs 的待对账 close → 拒绝

3. **`_gate_passive_close_pending(candidate)`**:
   - 检查是否有同 pair 的进行中被动平仓 → 拒绝

4. **`_gate_reduce_only(candidate)`**:
   - 检查 `self.state.lifecycle == RISK_ONLY or risk_mode >= ReduceOnly` → 新开仓只允许 reduce-only，因候选不是 reduce-only 所以拒绝

5. **`_gate_entry_sizing(candidate)`**:
   - `candidate.entry_notional_quote <= 0` → 拒绝

6. **`_gate_venue_cooldown(candidate, now_ms)`**:
   - 检查 per-venue cooldown 状态 → 在冷却中则拒绝

7. **`_gate_zero_fill_cooldown(candidate, now_ms)`**:
   - 检查同 pair 是否有零成交终端事件在冷却中 → 拒绝

**V1 参考**: `apply_runtime_entry_guards()` 中 8+ 子检查

---

## 任务 3: A4 全局 hot 预算

**文件**: `lightfee/engine/runtime.py`

**位置**: `_activate_local_l2_phase()` (约 260 行)

**改动**: 在对 hot positions 迭代 bootstrap 前，加上全局上限：
```python
hot_global_budget = max(
    getattr(self.config.strategy, 'local_l2_hot_exec_global_budget', 0), 0
)
hot_count = 0
for pos in ...:
    if hot_global_budget > 0 and hot_count >= hot_global_budget:
        break
    ...
    hot_count += 1
```

**V1 参考**: `hot_local_l2_symbols()` 使用 `local_l2_hot_exec_global_budget`

---

## 任务 4: A5 Supervisor retained 直接 bootstrap

**文件**: `lightfee/engine/runtime.py`

**位置**: `_restore_local_l2_state()` (约 500 行)

**改动**: 恢复 `pool == RETAINED` 的书时，直接设 BOOTSTRAPPING（而非 RESUME_WAITING），加入 `target_pairs` 纳入 bootstrap。

当前只设 `book.pool = L2PoolAssignment(pool_str)`，然后对 COLD 书设 RESUME_WAITING。改为：
```python
if book.pool == L2PoolAssignment.RETAINED:
    if book.status in (L2BookStatus.COLD, L2BookStatus.RESUME_WAITING):
        book.transition_to_bootstrapping(now_ms)
```

**V1 参考**: `startup_local_l2_symbols()` 包含 `retained_local_l2_books`

---

## 任务 5: B2 Live scan 恢复

**文件**: `lightfee/engine/runtime.py`

**位置**: `tick()` 方法，snapshot 加载后

**改动**:
1. 添加 `self._live_scan_success_streak: int = 0` 和 `self._last_good_snapshot = None` 到 `__init__`
2. 在 `tick()` 中：
   - Snapshot 有效 → `success_streak += 1`
   - Snapshot 过期 → 使用 `last_good_snapshot`（如果可用），success_streak 清零
   - Snapshot 缺失 → 阻塞 entry
3. `can_enter_new_positions` 中添加 `success_streak >= live_scan_recovery_success_count`（默认 3）

**V1 参考**: `advance_live_scan_recovery_state()` 和 LAST_GOOD_FALLBACK 逻辑

---

## 验证
```bash
cd /media/wl/新加卷/codex/LightFeeV2
python3 -m compileall lightfee/engine/runtime.py lightfee/strategy/discovery.py -q
```
