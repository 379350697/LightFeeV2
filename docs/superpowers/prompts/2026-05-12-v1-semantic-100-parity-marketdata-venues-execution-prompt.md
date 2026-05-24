# V1 Semantic 100% Parity 行情与 Venue 执行提示词

你是接手 LightFeeV2 marketdata / venues / Local-L2 语义复刻的执行智能体。你的任务只覆盖 venue contract、canonical symbol、local-L2 book/runtime/data-plane/WS、venue transport truth，不要修改 runtime 控制面、entry/close/risk、persistence/replay、offline/evolution。

## 工作目录

```bash
cd /media/wl/新加卷/codex/LightFeeV2
```

## 必读

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
4. `docs/superpowers/plans/2026-05-12-v1-semantic-100-parity-marketdata-venues-implementation-plan.md`
5. V1 源码：
   - `/media/wl/新加卷/codex/LightFee/src/live/*.rs`
   - `/media/wl/新加卷/codex/LightFee/src/market_gateway/*`
   - `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_*.rs`

## 你的目标

让 V2 行情与 venue 语义对齐 V1：

- 内部只用 canonical symbol
- wire symbol 只在 request / subscribe / parse 边界出现
- 所有 7 个 venue 的能力真值明确
- unsupported / partial support 必须显式 journal 或返回结构化结果
- Local-L2 snapshot/delta/checksum/sequence-gap/readiness 对齐 V1
- WS worker lifecycle 可 start/stop/abort，且连接启动有超时
- Hyperliquid poller、Binance/Aster auto-subscribe 等特殊路径不能被泛化抹掉

## 允许修改

- `lightfee/core/contracts.py`
- `lightfee/venues/specs.py`
- `lightfee/venues/transport.py`
- `lightfee/venues/*.py`
- `lightfee/marketdata/l2.py`
- `lightfee/marketdata/liquidity.py`
- `lightfee/marketdata/local_l2_data_plane.py`
- `lightfee/marketdata/local_l2_runtime.py`
- `lightfee/marketdata/local_l2_venues.py`
- `lightfee/marketdata/local_l2_ws.py`
- `tests/test_local_l2_runtime.py`
- `tests/test_local_l2_venue_rules.py`
- `tests/test_local_l2_ws.py`
- `tests/test_venues_contract.py`
- `tests/test_venues_transport.py`
- `tests/test_marketdata_l2.py`

不要修改 runtime 或 execution/risk 模块。

## 严格约束

- 修改任何生产函数/类/方法前，按 `AGENTS.md` 先跑 GitNexus upstream impact。
- 先写失败测试，再写实现。
- 所有 shell 命令前缀使用 `rtk`。
- 不要用 fake adapter 行为证明 live venue 语义。
- 不要把 symbol conversion 复制到多个层里制造二义性。
- 不要提交，除非调度者明确要求。

## 建议执行顺序

1. 确认索引新鲜度，必要时运行 `rtk npx gitnexus analyze`。
2. 阅读 plan 中 Task 1-4。
3. 先锁 canonical symbol round-trip 和 unsupported capability 测试。
4. 再补 worker lifecycle / open_timeout / error visibility。
5. 再补 Local-L2 book state、checksum、sequence gap、readiness。
6. 最后跑 venue + marketdata focused suite。
7. 跑 GitNexus detect_changes。

## 必跑验证

```bash
rtk pytest tests/test_venues_contract.py -q -W error
rtk pytest tests/test_venues_transport.py -q -W error
rtk pytest tests/test_local_l2_ws.py -q -W error
rtk pytest tests/test_marketdata_l2.py tests/test_local_l2_runtime.py tests/test_local_l2_venue_rules.py -q -W error
rtk python3 -m compileall lightfee tests
```

最后运行：

```text
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

## 最终报告

用中文返回：

- 改了哪些文件
- 对齐了哪些 V1 venue/local-L2 语义
- 哪些 venue 有特殊处理
- 每条验证命令 exit code
- GitNexus detect_changes 是否只落在 marketdata/venue 范围
- 剩余缺口，必须具体到 venue、文件、行为

