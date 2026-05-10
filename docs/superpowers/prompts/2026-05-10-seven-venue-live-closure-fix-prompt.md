# Seven Venue Live Closure Fix Prompt

你是接手 LightFeeV2 的执行体。你的任务不是继续搭骨架，也不是只让 fixture/mock 测试通过；你的任务是把 8 个交易所 adapter 从“看起来有接口”推进到“真实 live 路径不会在签名、路由、解析、下单确认上断掉”的闭环。

## 必须先读

- `docs/superpowers/specs/2026-05-10-seven-venue-adapters-design.md`
- `docs/superpowers/plans/2026-05-10-seven-venue-adapters-implementation-plan.md`
- `lightfee/venues/transport.py`
- `lightfee/venues/bitget.py`
- `lightfee/venues/specs.py`
- `tests/test_venues_transport.py`
- `tests/test_venues_contract.py`

## 当前状态判断

当前代码已经能通过 mock/fixture 层面的 7 家行情、仓位、下单解析测试，但不能宣称“7 家真实 live 闭环完成”。

已验证通过的命令：

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_venues_base.py tests/test_runtime_smoke.py -v
python3 -m compileall lightfee
python3 -c "import sys; sys.argv=['lightfee-probe','--config','config/example.toml','--list-capabilities']; from lightfee.apps.probe import main; main()"
```

这只能证明当前测试覆盖通过，不代表真实交易所 live 已闭环。

## 严格禁止

- 禁止把 paper mode 结果当 live 结果。
- 禁止把 HTTP 200 ack 当成交 fill。
- 禁止在真实 live 路径里返回假 order id、假 quantity、假 price。
- 禁止只加 fixture happy path 测试。
- 禁止用“Hyperliquid live order unsupported”冒充 7 家全打通。
- 禁止吞掉鉴权、限流、网络异常后静默降级到另一个 account profile。
- 禁止改测试来迎合错误实现。

## 必修缺口

### 1. 修复 OKX 私有 GET 签名

文件：`lightfee/venues/transport.py`

当前问题：OKX header 签名 payload 只用了：

```python
ts + method.upper() + path + body
```

但私有 GET 带 query string 时，请求路径必须包含 query，例如：

```text
/api/v5/account/positions?instId=BTC-USDT-SWAP
```

要求：

- `_build_auth_headers()` 对 OKX GET/DELETE 有 query string 时必须签 `path + query_string`。
- POST 仍签 `path + body`。
- 增加测试：OKX `fetch_position("BTCUSDT")` 的签名 payload 必须包含 `?instId=BTC-USDT-SWAP`。
- 测试不能只检查 header 存在，必须复算签名并断言相等。

### 2. 修复 Bybit V5 私有 GET 签名

文件：`lightfee/venues/transport.py`

当前问题：Bybit header 签名 payload 对 GET 使用空 body：

```python
ts + api_key + recv_window + body
```

Bybit V5 GET 私有接口应签 query string：

```text
timestamp + api_key + recv_window + query_string_without_question_mark
```

要求：

- GET/DELETE 使用 `query_string.lstrip("?")` 参与签名。
- POST 使用 JSON body 参与签名。
- 增加测试：Bybit `fetch_position("BTCUSDT")` 请求带 `category=linear&symbol=BTCUSDT`，并复算 header signature。
- 注意 query 参数排序必须和 URL 中最终 query 一致。

### 3. 修复私有 GET base_url 选择

文件：`lightfee/venues/transport.py`

当前问题：

```python
base_url = private_base_url if method.upper() == "POST" else public_base_url
```

仓位查询是私有 GET，不应该固定走 public base。

要求：

- 给 `_request()` 增加明确参数，例如 `private: bool = False`，或在 spec 中标注私有 endpoint。
- `fetch_position()`、`place_order()`、Bitget profile probe、Bitget profile-aware position/order 都必须走 private base。
- `fetch_market_snapshot()` 走 public base。
- 增加测试：用 public/private base 不同的临时 spec，断言 position/order URL 走 private，market URL 走 public。
- 不要靠当前交易所 public/private 域名恰好一样来过关。

### 4. 修复 Bitget account profile 检测

文件：`lightfee/venues/bitget.py`

当前问题：`detect_profile()` 捕获任意异常都设为 `CLASSIC`。这会把鉴权失败、限流、网络错误误判成 classic account。

要求：

- 只有明确 classic/UTA mismatch 才 fallback 到 `BitgetAccountProfile.CLASSIC`。
- 鉴权错误、权限错误、限流、网络错误、未知 5xx 必须原样抛出或包装为明确错误，不能切 classic。
- `TransportError` 需要携带 status code 和 response body，或者提供足够信息让 Bitget 判断 `_is_classic_mode_error(status_code, body)`。
- 增加测试：
  - UTA endpoint 返回 classic account code/message 时，fallback 到 classic endpoint，并完成 `fetch_position()`。
  - 401/403 不 fallback，必须抛错。
  - 429 不 fallback，必须抛错。
  - network/timeout 不 fallback，必须抛错。
  - profile 一旦成功检测要缓存，第二次不再 probe。

### 5. 修复仓位方向解析

文件：`lightfee/venues/transport.py`

当前问题：只判断 `"SHORT"`，导致 `"Sell"` 被解析成 BUY。

要求：

- 明确支持以下 SELL/short 表示：
  - `SHORT`
  - `SELL`
  - `SHORT_SIDE`
  - `short`
  - `sell`
  - 负数 `positionAmt` / `pos` / `size`
- 支持 BUY/long 表示：
  - `LONG`
  - `BUY`
  - `long`
  - `buy`
- 如果数量字段是负数，即使 side 字段缺失，也必须判 SELL，quantity 返回绝对值。
- 增加每家至少一个 short/sell fixture 或 inline raw 测试，覆盖 Binance/Aster/OKX/Bybit/Bitget/Gate/Hyperliquid。

### 6. 修复下单确认：ack 不能直接当 fill

文件：`lightfee/venues/transport.py`

当前问题：`_parse_order_fill()` 如果响应没有成交量，会回退到 `request.quantity`。真实 OKX/Bybit/Bitget/Gate 常常只返回 order id，这不代表成交。

要求：

- 对只返回 order id 的响应，不允许返回“已成交 request.quantity”。
- 选择一种清晰策略：
  - 实现 order status 查询并确认成交后返回 `OrderFill`；或者
  - 返回 `OrderSubmitError(SubmitFailureClass.UNCERTAIN, ...)`，提示 order accepted but fill not confirmed。
- Binance/Aster 如果响应确有 `executedQty`/`avgPrice` 可以直接返回 fill。
- OKX/Bybit/Bitget/Gate 若 place order 响应只有 order id，必须查询订单详情或报 UNCERTAIN。
- 增加测试：
  - ack-only 响应不能返回 request quantity。
  - filled 响应才返回 quantity/price。
  - reject 响应仍映射为 `REJECTED`。

### 7. Hyperliquid live 下单必须二选一：真实现或能力标记不支持

文件：

- `lightfee/venues/transport.py`
- `lightfee/venues/specs.py`
- `tests/test_venues_contract.py`
- `tests/test_venues_base.py`

当前问题：live 下单明确 `not yet implemented`，但用户目标是 7 家一次接全。

要求二选一，优先 A：

A. 真实现 Hyperliquid live order：

- 实现 Hyperliquid exchange action signing。
- 不要伪造 EIP-712。
- 构造 action、nonce、signature、vault/address 字段必须符合 Hyperliquid 官方 API。
- 增加签名向量测试或至少固定 nonce/body 的 deterministic signing 测试。
- live mock 下单必须能走到 HTTP request，不能提前 unsupported。

B. 如果当前轮无法可靠实现：

- 明确把 Hyperliquid 的 `place_order` live 能力标记为 unsupported。
- contract 测试和 probe 输出必须表达“行情/仓位支持，live order 不支持”。
- 最终报告不能写“7 家 live 下单闭环”，只能写“6 家 live order + Hyperliquid data path”。

不要保留“代码里 unsupported、文档里说 7 家全打通”的矛盾。

### 8. 扩展测试，不要只加 happy path

必须新增或强化以下测试：

- OKX GET 签名包含 query string，并复算签名。
- Bybit GET 签名包含 query string，并复算签名。
- 私有 GET 使用 private base URL。
- Bitget classic fallback 是通过真实 `fetch_position()` 完成，不是只测 `_is_classic_mode_error()`。
- Bitget 401/429/network 不 fallback。
- short/sell position parsing。
- ack-only order response 不返回假 fill。
- Hyperliquid 能力声明和实际行为一致。

## 验收命令

执行体完成后必须运行：

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_venues_base.py tests/test_runtime_smoke.py -v
python3 -m compileall lightfee
python3 -c "import sys; sys.argv=['lightfee-probe','--config','config/example.toml','--list-capabilities']; from lightfee.apps.probe import main; main()"
```

如果改了签名、profile 或下单确认，还必须单独运行新增测试，例如：

```bash
pytest tests/test_venues_transport.py -k "okx or bybit or bitget or private or short or ack" -v
pytest tests/test_venues_contract.py -k "order or profile or hyperliquid" -v
```

## 完成定义

只有满足以下条件，才能说“闭环”：

- 7 家 adapter 的 live 行情路径没有 paper fallback。
- 7 家 adapter 的 live 仓位路径能构造正确私有请求、签名、base URL，并解析真实 shape。
- Binance/Aster/OKX/Bybit/Bitget/Gate live 下单要么确认成交后返回 fill，要么对未确认成交返回 UNCERTAIN。
- Hyperliquid live 下单要么真实实现，要么能力声明明确 unsupported，最终报告不得说 7 家 live order 全通。
- 所有新增测试覆盖失败模式，而不仅是 fixture happy path。
- 上方验收命令全部通过。

## 最终回复格式

完成后按这个格式回复：

```text
结论：完整闭环 / 未完整闭环

已修复：
- ...

仍未闭环：
- 如果有，必须写清楚，不能省略

验证：
- pytest ...: 通过/失败
- compileall: 通过/失败
- probe: 通过/失败

真实能力声明：
- Binance:
- Aster:
- OKX:
- Bybit:
- Bitget:
- Gate:
- Hyperliquid:
```

如果 Hyperliquid 仍是 unsupported，结论必须是“未完整闭环”，不要用措辞掩盖。
