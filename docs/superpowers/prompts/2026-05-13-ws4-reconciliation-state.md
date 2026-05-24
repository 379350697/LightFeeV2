# Workstream 4: Reconciliation 与状态持久化

## 目标
修复 reconciliation 循环、启动恢复处理和 L2 book 数据持久化。

## 项目路径
- V2: `/media/wl/新加卷/codex/LightFeeV2/`
- V1 参考: `/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs`, `state.rs`

---

## 任务 1: C2 CloseLegRecord — per-leg fill 数据

**文件**: `lightfee/engine/state.py`

**改动**: `PendingClose` dataclass 增加 per-leg 成交数据字段：

```python
@dataclass
class CloseLegRecord:
    venue: str
    order_id: str = ""
    client_order_id: str = ""
    quantity: float = 0.0
    average_price: float = 0.0
    fee_quote: float = 0.0

@dataclass
class PendingClose:
    # ... 现有字段 ...
    long_legs: list[CloseLegRecord] = field(default_factory=list)
    short_legs: list[CloseLegRecord] = field(default_factory=list)
```

在 close executor 创建 `PendingClose` 时填充这些字段。

**V1 参考**: `PendingCloseReconciliation` 中的 `long_legs: Vec<CloseLegRecord>`, `short_legs: Vec<CloseLegRecord>`

---

## 任务 2: C3 启动恢复按序处理

**文件**: `lightfee/engine/runtime.py`

**位置**: `start()` 方法中 RECONCILING 分支

**改动**: 扩展 RECONCILING 状态下的启动处理，不只设生命周期：

```python
if recovery_state == "recovery_needed":
    # V1: finalize_startup_position_recovery
    transition_to_reconciling(self.state)
    
    # 按 V1 顺序处理：
    # 1. reconcile_open_positions (force_reconcile)
    await self._reconcile_pending_entries_force(now_ms)
    
    # 2. process pending_entry_hedges
    # 3. process pending_passive_closes
    # 4. process pending_close_reconciliations
    # 5. residual repairs
    # 6. manage_open_positions
    
    # 如果还有活跃 positions > max_concurrent_positions → RISK_ONLY
    if len(self.state.open_positions) > self.config.strategy.max_concurrent_positions:
        enter_fail_closed(self.state)
```

**V1 参考**: `finalize_startup_position_recovery()` 的完整处理流程

---

## 任务 3: C4 状态归一化增强

**文件**: `lightfee/engine/recovery.py` (或 `state.py`)

**位置**: `normalize_engine_state()` 函数

**改动**: 当前只做 2 项（移除零数量 + matched_quantity 默认值）。添加：

1. **Dust migration**: 将 `exit_reason` 含 `exchange_min_notional_dust` 的 position 从 `open_positions` 移到 `dust_residuals`
2. **Timestamp repair**: 修复 `funding_timestamp_ms == 0` 或 `total_funding_edge_bps_entry == 0`
3. **Dedup**: 对 `open_positions`, `live_recovery_reduce_only_pairs` 去重
4. **Passive phase state 填充**: 为空 `PassivePhaseState` 的 entry 填默认值

**V1 参考**: `normalize_engine_state_positions()` 的 12+ 修复操作

---

## 任务 4: C6 Book 数据持久化

**文件**: `lightfee/engine/runtime.py`

**位置**: `_snapshot_local_l2_state()` 和 `_restore_local_l2_state()`

**改动**:

**持久化时**（`_snapshot_local_l2_state`）:
```python
retained_local_l2_books = [{
    "venue": b.venue,
    "symbol": b.symbol,
    "status": b.status.value,
    "pool": b.pool.value,
    "sequence": b.sequence,
    "last_snapshot_ms": b.last_snapshot_ms,
    "last_delta_ms": b.last_delta_ms,
    # NEW: actual book data
    "bids": [{"price": l.price, "quantity": l.quantity} for l in b.bids],
    "asks": [{"price": l.price, "quantity": l.quantity} for l in b.asks],
    "last_update_id": b.last_update_id,
    "generation": getattr(b, 'generation', 1),
} for b in books if b.pool == L2PoolAssignment.RETAINED]
```

**恢复时**（`_restore_local_l2_state`）:
```python
book.last_update_id = entry.get("last_update_id", 0)
# Restore book data if available
if entry.get("bids"):
    book.bids = [PriceLevel(price=l["price"], quantity=l["quantity"]) for l in entry["bids"]]
if entry.get("asks"):
    book.asks = [PriceLevel(price=l["price"], quantity=l["quantity"]) for l in entry["asks"]]
```

**V1 参考**: `PersistedRetainedLocalL2Book` 中的 `bids: Vec<BookLevel>`, `asks: Vec<BookLevel>`

---

## 验证
```bash
cd /media/wl/新加卷/codex/LightFeeV2
python3 -m compileall lightfee/engine/runtime.py lightfee/engine/state.py lightfee/engine/recovery.py -q
```
