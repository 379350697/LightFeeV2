# Local-L2 硬漂移修复执行提示词

你是接手 LightFeeV2 的执行智能体。当前状态是：测试全绿，但 local-L2 实盘数据平面仍存在硬漂移，文档把未闭合内容标成了 fixed。你的任务是把这些硬漂移全部修到 Rust V1 live-path 语义等价，而不是继续补测试壳。

## 一句话任务

把 Python V2 当前“local-L2 模型层 + runtime 雏形”修成 Rust V1 等价的实盘数据平面：

```text
真实 venue L2 payload/stream
  -> LocalL2Update
  -> LocalL2Book 正确 sequence/checksum/rebuild
  -> LocalL2Runtime assignment/event
  -> maker-event lane
  -> 原 pending entry hedge amend/cancel-replace
  -> startup/recovery/docs 如实闭环
```

## 必须先读

按顺序读：

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-10-local-l2-data-plane-closure-design.md`
4. `docs/superpowers/plans/2026-05-10-local-l2-data-plane-closure-implementation-plan.md`
5. `docs/superpowers/prompts/2026-05-10-local-l2-data-plane-closure-execution-prompt.md`
6. `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
7. `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`

Rust V1 真源：

```text
/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs:4587-4693
/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs:5459+
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime_decision.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_targeting.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2.rs
/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2_sessions.rs
/media/wl/新加卷/codex/LightFee/src/market_gateway/local_l2.rs
/media/wl/新加卷/codex/LightFee/src/market_gateway/local_l2_state_machine.rs
```

Python V2 重点文件：

```text
lightfee/engine/runtime.py
lightfee/engine/entry_sync.py
lightfee/engine/entry.py
lightfee/engine/state.py
lightfee/marketdata/l2.py
lightfee/marketdata/local_l2_runtime.py
lightfee/marketdata/local_l2_venues.py
lightfee/marketdata/liquidity.py
lightfee/venues/base.py
lightfee/venues/transport.py
lightfee/venues/*.py
tests/test_runtime_maker_event_local_l2.py
tests/test_local_l2_venue_rules.py
tests/test_local_l2_runtime.py
tests/test_marketdata_l2.py
docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md
docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md
```

## 当前硬漂移

必须全部修复：

1. **文档假闭环**
   - closure report 把 P2-L2-001 到 P2-L2-008 全标 fixed。
   - parity matrix 把 RT-001/RT-002/P2-L2 全标 fixed。
   - 但代码还没有完全闭合。

2. **maker-event lane 不是 Rust V1 原位 pending hedge 驱动**
   - 当前 `_reprice_passive_maker()` 构造 `EntryContext` 并调用 `entry_executor.execute(ctx)`。
   - 这会走完整 entry maker->hedge flow，不等价于 Rust `drive_pending_entry_hedge()`。

3. **local-L2 parity mode 会自动 fallback 到 sidecar**
   - `local_l2_enabled=True` 但无 matching events 时，当前会调用 `_maybe_tick_maker_event_sidecar()`。
   - Rust V1 parity lane 不允许用 sidecar-mid 假装 local-L2 event-driven。

4. **startup local-L2 phase 是伪激活**
   - 当前 `_activate_local_l2_phase()` 遍历 `self.config.runtime.mode` 字符串，内部还有 `pass`。
   - 当前只给 `"binance"` 建 book，没有按 configured venues/symbols 激活。
   - 没有真实 subscribe/bootstrap/timeout/degraded/fail-closed 语义。

5. **venue L2 normalization 只有规则表，没有 payload parser / transport 接口**
   - `local_l2_venues.py` 只有规则字段。
   - 缺少每家交易所 raw L2 payload -> `LocalL2Update` 的解析。
   - `venues/transport.py` 和各 adapter 缺少 local-L2 snapshot/update 接口。

6. **checksum 非 Rust/venue 等价**
   - 当前 `compute_checksum()` 用 Python `hash()`，进程间随机且不是 OKX CRC32。
   - 文档却声称 OKX CRC32 supported。

## 修复顺序

严格按这个顺序做。

### Task 1: 先修文档为 honest partial

修改：

```text
docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md
docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md
```

要求：

- 把 RT-001、RT-002、P2-L2-002、P2-L2-006、P2-L2-007 标成 `partial/open`。
- P2-L2-001 可以拆成：
  - book snapshot/delta/sort/readiness: fixed
  - venue checksum/sequence parity: open
- 明确写：当前 tests green 但不代表 local-L2 实盘数据平面闭合。
- 文档必须在最后修完后再改回 fixed。

### Task 2: 实现真正的 pending hedge 原位驱动

修改：

```text
lightfee/engine/entry_sync.py
lightfee/engine/runtime.py
lightfee/engine/state.py
tests/test_runtime_maker_event_local_l2.py
```

要求：

- 在 `EntrySyncExecutor` 或独立 module 中实现 Python 版 `drive_pending_entry_hedge(entry_id, market/local_l2_view, allow_fallback)`。
- 它必须读取并更新 `EngineState.pending_entries[entry_id]` 或等价 pending hedge state。
- 它只能 amend/cancel-replace 原 maker order 或推进原 pending hedge，不得新建完整 entry flow。
- 不得调用 `EntrySyncExecutor.execute()` 作为 maker-event reprice 实现。
- `_reprice_passive_maker()` 要删除或改为 thin wrapper 调用 `drive_pending_entry_hedge()`。
- 新测试必须失败于旧实现：
  - monkeypatch `entry_executor.execute` 为直接 fail，maker-event local-L2 reprice 仍应通过新的 hedge driver。
  - 断言不会产生 `entry.maker_submitted` / `entry.hedge_submitted` 这类完整 entry flow journal。
  - 断言 pending entry 原状态被更新，entry id 不变。

### Task 3: 禁止 parity mode 自动 sidecar fallback

修改：

```text
lightfee/engine/runtime.py
tests/test_runtime_maker_event_local_l2.py
tests/test_live_full_closure.py
```

要求：

- 当 `strategy.local_l2_enabled=True` 时，`_maybe_tick_maker_event_local_l2()` 无 matching local-L2 events 必须直接 return，并 journal `runtime.maker_event_no_local_l2_events` 或等价 reason。
- 不得调用 `_maybe_tick_maker_event_sidecar()`。
- Sidecar fallback 只能在显式配置下启用，例如：

```text
strategy.local_l2_enabled=False
```

或新增清晰配置：

```text
runtime.maker_event_sidecar_fallback_enabled=True
```

默认必须不污染 parity mode。

测试：

- local-L2 enabled + no events + valid sidecar snapshot -> 不 reprice，不 wake sidecar。
- local-L2 disabled + valid sidecar snapshot -> sidecar fallback 才可运行，并 journal `source="sidecar_mid"`。

### Task 4: 补真实 venue L2 payload normalization

修改/新增：

```text
lightfee/marketdata/local_l2_venues.py
lightfee/venues/base.py
lightfee/venues/transport.py
lightfee/venues/*.py
tests/test_local_l2_venue_rules.py
tests/fixtures/venues/*/local_l2_*.json
```

要求：

- 为 7 家 venue 增加 raw payload -> `LocalL2Update` parser：
  - Binance
  - Aster
  - OKX
  - Bybit
  - Bitget
  - Gate
  - Hyperliquid
- parser 必须处理：
  - snapshot vs delta
  - bids/asks price/qty
  - sequence / previous_sequence / timestamp
  - checksum 字段
  - symbol normalization
  - malformed payload 的 deterministic fault
- `VenueAdapter` 或 `VenueTransport` 要暴露清晰接口，例如：

```python
async def fetch_l2_snapshot(self, symbol: str, depth: int) -> LocalL2Update
def parse_l2_update(self, payload: dict) -> LocalL2Update
```

- 如果某 venue 暂无真实 WS transport，也必须把 parser 和 REST snapshot 接口接好，并把 live WS gap 标到 docs，不得声称 full live supported。

测试：

- 每家 venue 至少 1 个 snapshot fixture、1 个 delta fixture、1 个 malformed fixture。
- 测试解析出来的 `LocalL2Update` 字段，而不是只断言 rules 表。

### Task 5: 修 checksum/sequence 为 venue 等价

修改：

```text
lightfee/marketdata/l2.py
lightfee/marketdata/local_l2_venues.py
tests/test_marketdata_l2.py
tests/test_local_l2_venue_rules.py
```

要求：

- 删除或停止使用 Python `hash()` 作为 live checksum。
- 实现 deterministic checksum：
  - OKX: 按 OKX book checksum 语义实现 CRC32。
  - 其他 venue: 若 Rust V1 不校验 checksum，明确 `ChecksumMode.NONE`，不得假装 supported。
- sequence gap 必须按 venue rules 驱动：
  - rule.max_sequence_gap
  - rule.sequence_mode
  - rule.reconnect_rebuild_on_gap
- `LocalL2Runtime.record_update()` 必须应用 venue rule 的 checksum/sequence policy，而不是只调用 book 默认逻辑。

测试：

- OKX fixture checksum 正确通过。
- OKX checksum mismatch -> `CHECKSUM_MISMATCH` event + rebuild/fault。
- sequence gap 超阈值 -> rebuild_required，不应用 delta。
- sequence gap 未超阈值 -> 可按 Rust 语义处理。

### Task 6: 修 startup local-L2 phase 为真实 configured venues/symbols 激活

修改：

```text
lightfee/engine/runtime.py
lightfee/apps/live.py
tests/test_live_startup_preflight.py
```

要求：

- `_activate_local_l2_phase()` 不得遍历 `runtime.mode` 字符串。
- 必须按 `config.venues` + `config.symbols` 建立目标集合。
- 对每个 venue/symbol：
  - 使用 venue rules 确定 depth/bootstrap mode。
  - 尝试 REST snapshot bootstrap 或 adapter local-L2 bootstrap method。
  - 成功后进入 BOOTSTRAPPING/HOT，失败进入 DEGRADED/RESUME_WAITING/SUSPENDED，按 Rust 语义 journal。
- Respect `runtime.live_startup_phase_timeout_ms`。
- 如果 local-L2 required 但启动失败，按配置进入 degraded/fail-closed；不得安静成功。

测试：

- 配 2 个 venue x 2 symbols，应该创建 4 个 books，不是只有 binance。
- startup timeout 产生明确 journal。
- retained book 恢复进入 RESUME_WAITING，不自动 HOT。
- bootstrap failure 不得被 counted as success。

### Task 7: 接入 execution liquidity 与 entry readiness 的真实调用路径

修改：

```text
lightfee/marketdata/liquidity.py
lightfee/engine/runtime.py
lightfee/engine/entry_local_l2.py
tests/test_runtime_entry_flow.py
tests/test_entry_local_l2.py
```

要求：

- entry final gate / dispatch 必须真的查询 entry-local-L2 session readiness。
- execution liquidity 必须从对应 venue/symbol book 生成；book not ready 时阻塞 parity path。
- 不得只提供 helper 而没有 production caller。

测试：

- local-L2 enabled + one leg stale -> entry blocked with reason。
- both legs ready -> entry can dispatch。
- fallback reason 进入 journal/diagnostics。

### Task 8: 最后修文档 closure

只有所有 Task 2-7 完成并验证后，才允许把 docs 改回 fixed。

更新：

```text
docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md
docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md
```

要求：

- 每个 fixed 都要有：
  - Rust source
  - Python source
  - test file
  - production caller
  - remaining risk
- 如果仍无真实 WS stream，只能写 REST snapshot/bootstrap partial，不得写 full live local-L2 supported。

## GitNexus 和影响分析

修改任何生产函数/类/方法前必须跑：

```text
gitnexus_impact({target: "<symbol>", direction: "upstream", repo: "LightFeeV2"})
```

至少需要对这些 symbol 做 impact：

```text
LiveRuntime
_maybe_tick_maker_event
_maybe_tick_maker_event_local_l2
_activate_local_l2_phase
EntrySyncExecutor
LocalL2Book
LocalL2Runtime
VenueTransport
```

收尾前必须跑：

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

如果 GitNexus 风险是 HIGH/CRITICAL，最终报告必须写清楚受影响 execution flows。

## 验证命令

每个任务跑 focused tests。最终必须跑：

```bash
rtk pytest tests/test_runtime_maker_event_local_l2.py -q -W error
rtk pytest tests/test_local_l2_runtime.py tests/test_entry_local_l2.py tests/test_local_l2_venue_rules.py tests/test_marketdata_l2.py -q -W error
rtk pytest tests/test_live_startup_preflight.py tests/test_runtime_entry_flow.py -q -W error
rtk pytest -q -W error
rtk python3 -m compileall lightfee tests
```

## 通过标准

全部满足才算完成：

- `local_l2_enabled=True` 时 maker-event lane 不会自动 sidecar fallback。
- maker-event reprice 不调用 `EntrySyncExecutor.execute()`。
- pending entry hedge 是原位 amend/cancel-replace，entry id 不变。
- startup local-L2 phase 按 configured venues/symbols 激活，不是 hardcoded binance。
- 7 家 venue 都有真实 fixture payload parser。
- OKX checksum 是 deterministic CRC32，不是 Python hash。
- sequence/checksum failure 会触发 rebuild/fault，不会静默应用。
- entry production path 会消费 local-L2 readiness/liquidity。
- docs 不再假闭环；fixed 只对应真实 production caller + tests。

## 最终报告格式

按这个格式回复：

```text
结论：local-L2 硬漂移已全部修复 / 仍未全部修复

已修复：
- HD-001 文档假闭环 ...
- HD-002 pending hedge 原位驱动 ...
- HD-003 parity mode 禁止 sidecar fallback ...
- HD-004 startup local-L2 configured activation ...
- HD-005 venue payload parser ...
- HD-006 checksum/sequence ...
- HD-007 execution liquidity/readiness production caller ...

仍未修复：
- ...

Rust 对齐证据：
- Rust: ...
- Python: ...

测试：
- ...

GitNexus：
- impact: ...
- detect_changes: ...

剩余风险：
- ...
```
