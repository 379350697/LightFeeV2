# Workstream 3: Snapshot 新鲜度状态机 + 双阶段 L2 Sync

## 目标
激活已有 `evaluate_snapshot_freshness` 并接入 tick 流程，添加预热检查，实现双阶段 L2 sync。

## 项目路径
- V2: `/media/wl/新加卷/codex/LightFeeV2/`
- 已有代码: `lightfee/sidecar/snapshot.py` (evaluate_snapshot_freshness 已实现但未使用)
- V1 参考: `/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs` (tick_with_scan_mode)

---

## 任务 1: B1 Snapshot 新鲜度状态机接入

**文件**: `lightfee/engine/runtime.py`

**改动**: 在 `tick()` 中替换简单的 `check_stale_snapshot` → `evaluate_snapshot_freshness`

**当前**:
```python
if check_stale_snapshot(snapshot.published_at_ms, max_age, now_ms):
    ...
    return
```

**改为**:
```python
from lightfee.sidecar.snapshot import evaluate_snapshot_freshness, SnapshotFreshness, FRESH_SIDECAR_SNAPSHOT_MAX_AGE_MS

freshness = evaluate_snapshot_freshness(
    snapshot=snapshot,
    now_ms=now_ms,
    max_age_ms=max_age,
    last_good_snapshot=self._last_good_snapshot,
    degraded_venues=snapshot.degraded_venues,
)
if freshness == SnapshotFreshness.MISSING:
    self.journal.append("runtime.snapshot_missing", {"ts_ms": now_ms})
    return
if freshness == SnapshotFreshness.STALE:
    # Use last_good_snapshot if available
    if self._last_good_snapshot is not None:
        snapshot = self._last_good_snapshot
        self.journal.append("runtime.snapshot_fallback_last_good", {"ts_ms": now_ms})
    else:
        self.journal.append("runtime.snapshot_stale", {"ts_ms": now_ms})
        return
if freshness == SnapshotFreshness.DEGRADED:
    # Some venues degraded but can still trade on healthy ones
    self.journal.append("runtime.snapshot_degraded",
        {"venues": snapshot.degraded_venues, "ts_ms": now_ms})
if freshness in (SnapshotFreshness.FRESH, SnapshotFreshness.DEGRADED):
    self._last_good_snapshot = snapshot  # Update fallback
```

**注意**: 需要在 `__init__` 添加 `self._last_good_snapshot = None`

---

## 任务 2: B3 Market data warmup

**文件**: `lightfee/engine/runtime.py`

**改动**: 在 `tick()` 中 candidate 发现前，添加 funding 覆盖率检查。

V1 逻辑：市场数据预热期间，funding rate 覆盖率不足时跳过 entry dispatch。

```python
# Check market data warmup — funding coverage must meet threshold
if hasattr(snapshot, 'funding_lifecycle'):
    funding_coverage_ratio = 0.0
    for fl in (snapshot.funding_lifecycle or []):
        # fl has coverage/total fields
        pass
    # If coverage < threshold and no positions yet, skip entry
    # This prevents entries during initial warmup
```

**V1 参考**: `is_within_funding_scan_window_ms()` 和 warmup gating

---

## 任务 3: B17 双阶段 L2 sync

**文件**: `lightfee/engine/runtime.py`

**改动**: 在 `tick()` 中添加两次 L2 sync：

1. **Pre-scan sync**（tick 开始处，snapshot 加载后立即）:
```python
# V1: sync_local_l2_runtime(now_ms, false) — execution-owned books only
await self._sync_local_l2_data(now_ms, scan_promoted=False)
```

2. **Post-shortlist sync**（candidate 筛选后，entry dispatch 前）:
```python
# V1: sync_local_l2_runtime(now_ms, true) — allows scan-promoted books
await self._sync_local_l2_data(now_ms, scan_promoted=True)
```

**需要**: 修改 `_sync_local_l2_data` 方法签名，接受 `scan_promoted` 参数并传递给底层 `sync()` 调用。

**V1 参考**: `tick_with_scan_mode` 中 pre-scan `sync_local_l2_runtime(false)` 和 post-shortlist `sync_local_l2_runtime(true)`

---

## 验证
```bash
cd /media/wl/新加卷/codex/LightFeeV2
python3 -m compileall lightfee/engine/runtime.py lightfee/sidecar/snapshot.py -q
```
