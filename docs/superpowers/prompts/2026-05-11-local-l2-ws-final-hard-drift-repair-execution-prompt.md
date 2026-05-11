# Local-L2 WS 最终硬漂移修复执行提示词

你是接手 LightFeeV2 的执行智能体。你的任务不是“再看看”，而是把当前 Python Local-L2 / WS 数据平面与 Rust V1 的实盘语义严格对齐到可闭环状态。

## 必读上下文

开始前必须读完：

1. `AGENTS.md`
2. `/home/wl/.codex/RTK.md`
3. `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
4. `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
5. `docs/superpowers/prompts/2026-05-11-local-l2-ws-relay-integration-repair-execution-prompt.md`
6. Rust 真源项目：`/media/wl/新加卷/codex/LightFee`

重点 Rust 参考：

- `/media/wl/新加卷/codex/LightFee/src/market_gateway/local_l2.rs`
- `/media/wl/新加卷/codex/LightFee/src/market_gateway/local_l2_state_machine.rs`
- `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime.rs`
- `/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/binance.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/okx.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/gate.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/aster.rs`

## 绝对约束

- 修改任何生产函数、类、方法前，必须按 `AGENTS.md` 跑 GitNexus impact，报告风险；HIGH / CRITICAL 必须先说明再改。
- 不能改交易策略语义：不改阈值、不改下单条件、不改 maker-event 触发条件、不改精度处理。
- 不能靠关闭 `local_l2_ws_enabled`、跳过 live WS、sidecar fallback、mock payload、只改测试或只改文档来宣布闭环。
- Python 可以优化 Rust 里历史补丁造成的臃肿结构，但业务语义必须等价。
- 生产路径不得直接访问 `adapter._transport` 或 venue 私有属性；通过 adapter / transport 明确接口完成。
- 文档只能在代码与测试真实通过后更新；不得提前把 `P2-L2-009` 写成 `fixed`。

## 当前结论

当前实现仍不是完整闭环。已存在 WS client / relay 代码，但有硬漂移和生产接入风险。必须全部修复后，才能把 Local-L2 WS 数据平面标记为 `fixed`。

## 必修问题 1：Live startup 不能被真实 WS 挂死

现象：

- `LiveRuntime.start()` 在 local-L2 bootstrap 后会启动真实 WS。
- 当前无网络或测试环境下，`tests/test_live_startup_preflight.py` 会挂住，`timeout 25s` 退出码为 `124`。
- 这说明 startup lifecycle 与 Rust 实盘语义不对齐：实盘启动可以激活数据平面，但不能因为一个 WS connect/read 无限等待而阻塞整个 runtime startup。

必须修复：

- `LiveRuntime.start()` / `_activate_local_l2_phase()` / `_start_local_l2_ws_streams()` 必须是有界、可返回、可停止的。
- `LocalL2DataPlane.connect_ws_streams()` 只能注册并启动后台任务，不能在 startup 路径无限等待真实网络。
- WS 连接失败必须 journal / diagnostics / error_count / reconnect state 可见，不能裸 `except: pass`。
- `LiveRuntime.stop()` 必须取消所有 WS/poller task，不留下 pending task。
- live 默认仍然可以启用 WS；不能用“生产默认关闭 WS”绕过。

必须新增或修复测试：

- 无网络/测试 transport 下，startup preflight 在 25 秒内返回。
- stop 后没有悬挂任务。
- WS 连接异常会被记录，不会阻塞 startup。

最低验收命令：

```bash
rtk timeout 25s pytest tests/test_live_startup_preflight.py -q -W error
```

必须退出码 `0`。

## 必修问题 2：内部 symbol key 必须统一，不能 OKX/Gate 分裂成两本书

现象：

- startup 用 config 里的 canonical symbol 创建 book，例如 `okx:BTCUSDT`。
- `VenueTransport.fetch_l2_snapshot()` 会把 symbol 转成 venue wire symbol，例如 OKX `BTC-USDT-SWAP`。
- parse 后的 `LocalL2Update.symbol` 可能是 venue wire symbol，导致 `record_update()` 写入另一本书，例如 `okx:BTC-USDT-SWAP`。
- 后续 HOT book、WS subscribe、maker-event lookup 可能读 `BTCUSDT`，真实 L2 更新却写到 `BTC-USDT-SWAP`，这是硬漂移。

必须修复：

- 明确并执行单一内部 key 策略：LightFeeV2 内部 book key / pending entry lookup / maker-event matching 必须使用 canonical symbol，例如 `BTCUSDT`。
- venue wire symbol 只允许出现在 REST/WS 请求和 exchange payload 层。
- REST snapshot parse、WS delta parse、Hyperliquid poll snapshot 最终进入 `LocalL2Runtime.record_update()` 前，`LocalL2Update.symbol` 必须恢复为 canonical symbol。
- OKX 订阅可以使用 `BTC-USDT-SWAP`，但 runtime book 必须是 `okx:BTCUSDT`。
- Gate 订阅可以使用 `BTC_USDT`，但 runtime book 必须是 `gate:BTCUSDT`。
- Hyperliquid wire coin 可以是 `BTC`，但内部 book 必须保持策略 symbol，例如 `BTCUSDT`。

允许实现方式：

- 在 venue spec / adapter 层增加明确的 `to_venue_symbol()` / `from_venue_symbol()` 或等价方法。
- WS client 持有 `canonical_symbol` 与 `venue_symbol` 两个字段。
- parse 函数可以接收 canonical symbol override，禁止让 venue wire symbol 泄漏到 runtime book key。

必须新增或修复测试：

- OKX bootstrap：输入 config symbol `BTCUSDT`，snapshot payload 来自 `BTC-USDT-SWAP`，最终只有一个 book key：`okx:BTCUSDT`。
- OKX WS：订阅参数使用 `BTC-USDT-SWAP`，record_update 使用 `BTCUSDT`。
- Gate WS：订阅参数使用 `BTC_USDT`，record_update 使用 `BTCUSDT`。
- Hyperliquid poll：请求 coin 使用 `BTC`，record_update 使用 `BTCUSDT`。
- maker-event repricing 用 pending entry 的 canonical symbol 能读到真实 L2 book。

禁止：

- 不允许在 runtime 里到处手写 `replace("-USDT-SWAP", "USDT")`。
- 不允许同时保留 `okx:BTCUSDT` 和 `okx:BTC-USDT-SWAP` 两本活跃书。

## 必修问题 3：Hyperliquid poller 必须注入真实 adapter

现象：

- `HyperliquidL2Poller` 依赖 adapter 调 `fetch_l2_snapshot()`。
- 当前 data plane 创建 poller 时没有传 adapter，也没有调用 `set_adapter()`。
- 结果是 Hyperliquid 生产路径看似启动，实际不会 ingest 真实数据。

必须修复：

- `LocalL2DataPlane.start_ws_streams()` 或 factory 必须能够拿到对应 venue adapter。
- Hyperliquid poller 创建后必须注入 adapter。
- poller tick 必须调用 adapter 的 `fetch_l2_snapshot()`，并把 snapshot 转成 canonical `LocalL2Update` 后进入 `record_update()`。
- adapter 缺失必须显式报错或 journal degraded，不能静默空跑。

必须新增或修复测试：

- fake Hyperliquid adapter 被调用一次以上。
- poller ingest 后 runtime 有 `hyperliquid:BTCUSDT` 的非空 book。
- adapter 缺失路径有可观测错误，不会被当成 fixed。

## 必修问题 4：Binance / Aster auto-subscribe WS 不能发送空 JSON

现象：

- Binance/Aster 使用 per-symbol stream URL 时，连接本身已经订阅。
- 当前 base `_connect_and_read()` 无条件发送 `build_subscribe_message()`。
- Binance/Aster 的 `build_subscribe_message()` 返回 `{}`，实际可能向交易所发送无意义 `{}`，造成 error/disconnect。

必须修复：

- `build_subscribe_message()` 可返回 `None`。
- base WS 只在 subscribe message 非空时发送。
- Binance/Aster per-symbol stream 返回 `None`，不得发送 `{}`。
- OKX/Bybit/Bitget/Gate 这类需要 subscribe frame 的 venue 仍必须发送正确订阅消息。

必须新增或修复测试：

- Binance client connect 后没有发送 `{}`。
- Aster client connect 后没有发送 `{}`。
- OKX/Bybit/Bitget/Gate 仍发送 subscribe frame。

## 必修问题 5：文档必须诚实闭环，不能互相矛盾

现象：

- parity matrix 里 `P2-L2-009` 已写 `fixed`，但实现仍有上述硬漂移。
- closure report 写了“7 家 WS 全部实现”，但 startup hang / symbol split / Hyperliquid adapter 缺失说明不成立。
- 有些条目仍写 “WS stream integration deferred”，与 `fixed` 结论冲突。

必须修复：

- 修代码前可以先把文档改回 `partial/open`，或者在同一 PR 内先修代码再改文档。
- 最终文档只能在所有验收命令通过后写 `fixed`。
- parity matrix 与 closure report 口径必须一致：
  - 如果全部修复：写明真实 WS/poller 数据已进入 `LocalL2Runtime.record_update()`，startup 有界，symbol canonical，无 split book。
  - 如果仍有任何 venue 未真实接入：不能写 `fixed`，必须列为 remaining risk。
- 文档中的测试数量、命令输出只能写本次新跑过的结果，不得沿用历史数字。

## 建议修改文件

预计需要改：

- `lightfee/engine/runtime.py`
- `lightfee/marketdata/local_l2_data_plane.py`
- `lightfee/marketdata/local_l2_ws.py`
- `lightfee/venues/transport.py`
- `lightfee/venues/specs.py`
- `lightfee/core/contracts.py` 或当前定义 `LocalL2Update` 的文件
- `tests/test_local_l2_ws.py`
- `tests/test_local_l2_runtime.py`
- `tests/test_live_startup_preflight.py`
- `tests/test_runtime_maker_event_local_l2.py`
- `tests/test_marketdata_l2.py`
- `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`

不要机械照改文件列表；以当前代码实际位置为准。

## 执行顺序

1. 跑 GitNexus query/context 定位 Local-L2 startup、WS client、record_update、maker-event flow。
2. 对即将编辑的生产 symbol 跑 GitNexus impact，并把风险写入执行报告。
3. 先写失败测试，覆盖上面 5 类问题。
4. 修 startup lifecycle，保证 WS 后台化、有界启动、stop 可取消。
5. 修 canonical symbol 边界，确保所有 venue wire symbol 不进入 runtime book key。
6. 修 Hyperliquid adapter 注入与 poller ingest。
7. 修 auto-subscribe 的空消息发送。
8. 跑 focused tests。
9. 跑全量 tests 与 compileall。
10. 只有全部验证通过后，更新 parity matrix / closure report。
11. 跑 `gitnexus_detect_changes(scope="unstaged")`，确认影响范围符合预期。

## 最低验收命令

必须全部通过，并在最终报告贴出退出码和关键输出：

```bash
rtk timeout 25s pytest tests/test_live_startup_preflight.py -q -W error
rtk pytest tests/test_local_l2_ws.py tests/test_local_l2_runtime.py tests/test_live_startup_preflight.py tests/test_runtime_maker_event_local_l2.py tests/test_marketdata_l2.py -q -W error
rtk python3 -m compileall lightfee tests
rtk pytest -q -W error
```

还必须做静态检查：

```bash
rtk rg -n "adapter\\._transport|_transport" lightfee/engine lightfee/marketdata
```

生产 data-plane / engine 不得新增 direct `_transport` 访问。若输出只来自测试或 venue adapter 内部，必须在报告中说明。

## 最终报告格式

最终报告必须用中文，按下面格式：

```text
结论：P2-L2-009 已修复 / 未修复

修复内容：
1. Startup lifecycle：...
2. Canonical symbol：...
3. Hyperliquid poller：...
4. Auto-subscribe：...
5. Docs closure：...

Rust 对齐说明：
- Rust 参考文件：...
- Python 对应文件：...
- 语义等价点：...

验证：
- rtk timeout 25s pytest tests/test_live_startup_preflight.py -q -W error -> exit 0, ...
- rtk pytest ... -> exit 0, ...
- rtk python3 -m compileall lightfee tests -> exit 0, ...
- rtk pytest -q -W error -> exit 0, ...

仍余风险：
- 如果没有，写“无已知硬漂移”
- 如果有，必须具体到 venue / 文件 / 行为，不能写泛泛而谈
```

## 成功定义

只有同时满足以下条件，才允许宣布“完整闭环”：

- live startup 不会因 WS 网络阻塞挂死。
- 真实 WS/poller payload 会进入 `LocalL2Runtime.record_update()`。
- runtime book key 使用 canonical symbol，没有 OKX/Gate/Hyperliquid split book。
- Hyperliquid poller 生产路径注入真实 adapter 并能 ingest。
- Binance/Aster auto-subscribe 不发送空 `{}`。
- maker-event lane 只消费真实 local-L2，不回退 sidecar mid。
- parity matrix 与 closure report 口径一致，且有本次新跑的验证证据。

达不到任一条，就不能写 `fixed`，只能写 remaining drift。
