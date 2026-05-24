# Workstream 1: L2 Bootstrap 鲁棒性

## 目标
修复 `local_l2_data_plane.py` 和 `transport.py` 中与 V1 的 6 个语义差距。

## 项目路径
- V2: `/media/wl/新加卷/codex/LightFeeV2/`
- V1 参考: `/media/wl/新加卷/codex/LightFee/src/live/binance.rs`

---

## 任务 1: A1 无限重试

**文件**: `lightfee/marketdata/local_l2_data_plane.py`

**位置**: `_bootstrap_one()` 函数（约 533-559 行）

**改动**: 将 `while attempt < 5:` 改为 `while True:`，移除 `attempt` 变量。V1 的 `loop {}` 无限循环直到成功的语义。

**V1 参考**: `bootstrap_binance_local_l2_symbol()` 的 `loop {}` 块

---

## 任务 2: A2 速率限制头

**文件**: `lightfee/venues/transport.py`

**改动**: `fetch_l2_snapshot()` 的 retry 循环中，`except TransportError` 分支：
1. 检查 `e.status_code == 429` 或 418
2. 如果是，从 `Retry-After` 响应头提取 `retry_after_ms`（需要改动 `_request` 也返回 headers）
3. 实际的 `delay_ms = max(backoff_delay_ms, retry_after_ms)` — 取大值

**注意**: 需要在 `TransportError` 上添加 `headers` 字段，或在 `_request` 的 429 处理后传递。

**V1 参考**: `send_binance_bootstrap_request_with_limiter()` 中 `parse_retry_after_ms(response.headers(), ...)` 和 `record_rate_limit_for_scopes()` 的逻辑。

---

## 任务 3: A3 快照失败清理

**文件**: `lightfee/marketdata/local_l2_data_plane.py`

**位置**: `bootstrap_book()` 的 `except TransportError` 分支（约 192 行）

**改动**: 在记录 snapshot_error 前，添加：
```python
# V1: clear_binance_local_l2_depth_updates_for_instance()
buf_key = f"{venue}:{symbol}"
self._pre_snapshot_buffers.pop(buf_key, None)
```

这样失败的 snapshot 不会带着之前积累的脏 WS 增量一起重放。

**V1 参考**: `bootstrap_binance_local_l2_symbol()` 中 `apply_bootstrap_snapshot` 失败 → `clear_binance_local_l2_depth_updates_for_instance(instance_id, local_l2_state)` 的逻辑

---

## 任务 4: A6 resume_without_bootstrap 轮询

**文件**: `lightfee/marketdata/local_l2_data_plane.py`

**位置**: `_bootstrap_one()` 入口，在状态检查之前

**改动**: 
```python
# V1: check resume_without_bootstrap_remaining_ms() inline polling
book = self._runtime.get_book(venue, symbol)
if book is not None:
    remaining = book.resume_waiting_remaining_ms(now_ms)
    if remaining > 0:
        await asyncio.sleep(min(remaining, 250) / 1000.0)
        continue  # 回到重试循环顶部重新检查
```

**V1 参考**: `bootstrap_binance_local_l2_symbol()` 中 `resume_without_bootstrap_remaining_ms(&symbol, now_ms())` 的 250ms 轮询

---

## 任务 5: A7 过期快照检测

**文件**: `lightfee/marketdata/local_l2_data_plane.py`

**位置**: `bootstrap_book()` 中 `adapter.fetch_l2_snapshot()` 返回后，`record_update` 之前

**改动**:
1. 从 `update` 中提取 snapshot 的 `lastUpdateId`（即 `update.sequence`）
2. 获取当前 `book.last_update_id`
3. 如果 `update.sequence > 0 and book.last_update_id > 0 and update.sequence <= book.last_update_id`：
   - 该 snapshot 已过期（WS 已收到更新的增量）
   - 返回 False，触发重试
   - 增加一个小延迟（min 250ms）避免紧循环

**V1 参考**: `binance_local_l2_snapshot_is_stale()` 函数

---

## 任务 6: A8 初始延迟

**文件**: `lightfee/marketdata/local_l2_data_plane.py`

**位置**: `_bootstrap_one()` 入口处延迟计算（约 519 行）

**当前**:
```python
delay_ms = (index % batch_size) * jitter_ms + random.randint(0, jitter_ms)
```

**改为**:
```python
delay_ms = jitter_ms * (index % batch_size) // batch_size
```

V1 用除法产生小的 slot 延迟（0, 62, 125, 187 ms），不叠加随机 jitter。

**V1 参考**: `binance_local_l2_bootstrap_initial_delay_ms()` 逻辑

---

## 验证
```bash
cd /media/wl/新加卷/codex/LightFeeV2
python3 -m compileall lightfee/marketdata/local_l2_data_plane.py lightfee/venues/transport.py -q
```
