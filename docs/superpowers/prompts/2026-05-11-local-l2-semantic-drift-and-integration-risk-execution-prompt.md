# Local-L2 语义漂移 / 接入闭环执行提示词

你是接手 LightFeeV2 的执行智能体。先读完这些文件，再开始做事：

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/prompts/2026-05-11-local-l2-hard-drift-repair-execution-prompt.md`
4. `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
5. `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
6. Rust 真源：`/media/wl/新加卷/codex/LightFee`

## 只做两件事

### 1. 语义漂移
把 Python V2 的 local-L2 / maker-event 行为修到和 Rust V1 一致。

重点检查并修复：
- `PendingEntry` 在 local-L2 maker reprice 成功后的持久化回写
- parity mode 下禁止 sidecar fallback 污染
- checksum / sequence / rebuild 语义
- startup local-L2 按 configured venues × symbols 激活
- 文档里不能把未闭环内容写成 fixed

### 2. 接入闭环
把真实 WS / relay / bootstrap 数据接到 `LocalL2Runtime.record_update()` 和 live startup path，做到 Python V2 的 local-L2 数据面与 Rust V1 对齐。

重点是“真实接入”，不是“文档标注”：
- 不能只停在 parser、fixture、REST bootstrap、mock harness。
- 必须让 live payload 进入 canonical `LocalL2Update`，再进入 `LocalL2Runtime.record_update()`。
- venue 如果是 WS 直连就直连；如果 Rust 走 relay/中继路径，Python 也要接同等语义路径。
- 不得用 sidecar snapshot、测试桩、假数据流替代真实接入。

## 硬约束

- 不改策略阈值，不做 paper/shadow trading。
- 不 hardcode venue，不绕过 Rust 语义。
- Python 可以比 Rust 更清晰，但不能改业务含义。
- 代码、测试、parity matrix、closure report 必须同口径。
- 接入闭环完成前，不得把 WS L2 / local-L2 真实接入写成已 closed。

## 完成标准

- 语义漂移修完并有测试证明。
- 真实 WS/relay 已接入，live path 的 local-L2 更新能进入 `LocalL2Runtime.record_update()`，并与 Rust V1 语义对齐。
