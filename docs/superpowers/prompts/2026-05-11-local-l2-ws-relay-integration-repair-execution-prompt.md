# Local-L2 WS/Relay 接入修复执行提示词

你是接手 LightFeeV2 的执行智能体。先读完这些文件，再开始做事：

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
4. `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
5. `docs/superpowers/prompts/2026-05-11-local-l2-semantic-drift-and-integration-risk-execution-prompt.md`
6. Rust 真源：`/media/wl/新加卷/codex/LightFee`

## 任务

把 `P2-L2-009` 从 `partial` 修到 `fixed`。

目标不是再做 REST snapshot bootstrap，而是把真实 venue 数据接入闭环：

```text
真实 WS / relay / bootstrap payload
  -> canonical LocalL2Update
  -> LocalL2Runtime.record_update()
  -> live startup / maker-event lane / recovery 可消费
```

## 必须做到

- 真实 payload 必须进入 `LocalL2Runtime.record_update()`
- `local_l2_data_plane.py` 不能只停在 REST periodic refresh
- `runtime.py`、`local_l2_data_plane.py`、`transport.py`、`core/contracts.py` 的接口边界要收干净
- 生产路径不得直接访问 `adapter._transport`
- 如果 Rust V1 走 WS 或 relay，Python 也要接同等语义路径
- 不得用 sidecar、fixture、mock、测试桩假装闭环

## 禁止

- 不改策略阈值
- 不把 REST 周期刷新当成 WS 闭环
- 不做 paper/shadow trading
- 不 hardcode venue
- 不改语义只改文档

## 重点文件

- `lightfee/marketdata/local_l2_data_plane.py`
- `lightfee/marketdata/local_l2_runtime.py`
- `lightfee/venues/transport.py`
- `lightfee/core/contracts.py`
- `lightfee/engine/runtime.py`
- `tests/test_local_l2_runtime.py`
- `tests/test_live_startup_preflight.py`
- `tests/test_venues_transport.py`

## 验收

- live payload 能进入 `LocalL2Runtime.record_update()`
- startup/bootstrap 能通过真实 venue 接口跑通
- maker-event lane 仍只消费真实 local-L2 事件，不回退 sidecar
- parity matrix 和 closure report 只在真实接入完成后才把 `P2-L2-009` 写成 `fixed`
