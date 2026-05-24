# V2 Sidecar 公开行情层大搬家实施方案

日期: 2026-05-15

状态: 文档定稿，供执行智能体实现；本轮不直接改生产代码。

## 硬性结论

这次不做过渡方案。V2 sidecar 要一次性补齐 V1 生产 sidecar 的核心能力，并修掉 V1 已知问题。

1. 不把 sidecar 新能力继续塞进 3000 行的 `VenueTransport`。
2. 新增公开数据层 `lightfee/venues/market_data.py`，实现 `MarketDataClient`。
3. `VenueTransport` 继承或复用 `MarketDataClient` 的公开行情能力，但保留下单、仓位、账户风控、签名等交易职责。
4. `SidecarService` 只依赖 `ExchangeSource`、`LiquiditySource`、`TransferSource`，source 内部持有 `MarketDataClient`。
5. 保留并实现 `lightfee/sidecar/sources/exchange.py`、`liquidity.py`、`transfer.py`，不要删除这三个文件。
6. sidecar 禁止依赖下单、仓位、账户风控接口；公开行情客户端不能要求 `LiveCredential`。
7. 不再引入 Chillybot 耦合，`hint_lifecycle`/`transfer_lifecycle` 为空值兼容即可。

## 已确认现状

V2 sidecar 的直接 blocker 是三个 source 仍是空壳:

| 文件 | 当前行为 |
| --- | --- |
| `lightfee/sidecar/sources/exchange.py` | `fetch_funding_rates`、`fetch_market_quotes`、`fetch_all` 全部返回 `{}` |
| `lightfee/sidecar/sources/liquidity.py` | `fetch_perp_liquidity` 返回 `{}`，`fetch_execution_depth` 返回 `None` |
| `lightfee/sidecar/sources/transfer.py` | `fetch_transfer_statuses` 返回 `[]` |

`lightfee/sidecar/service.py` 已经会构造 source 并调用 `source.fetch_all(symbols)`，但因为 source 没有连接任何真实交易所，`quotes` 和 `candidates` 永远为空。

`VenueTransport` 当前约 3077 行，混合了公开行情、L2 深度、仓位、账户风控、下单、被动单、订单状态、HTTP、签名、限速、时间校准、错误分类。它的 `__init__` 在 `mode == "live"` 时会触发 `_validate_live_credentials`，这对 sidecar 的公开行情访问是不合理依赖。

`runtime.py`、`discovery.py`、`publisher.load_snapshot()` 的消费路径不是本次核心问题，不要为了 sidecar 取数去重写 runtime。

## 目标架构

```text
lightfee/venues/market_data.py
  MarketDataClient
    - public HTTP helpers
    - public rate-limit scope usage
    - symbol conversion
    - fetch_market_snapshot(symbols)
    - fetch_funding_tickers(symbols)
    - fetch_perp_liquidity(symbols)
    - fetch_l2_snapshot(symbol, depth)
    - fetch_transfer_statuses(assets), initially compatible empty when unavailable

lightfee/venues/transport.py
  VenueTransport(MarketDataClient)
    - private credential validation
    - private signed requests
    - fetch_position
    - fetch_account_risk_snapshot
    - place_order
    - submit/amend/cancel passive order
    - fetch_order_status

lightfee/sidecar/sources/*.py
  ExchangeSource(MarketDataClient)
  LiquiditySource(MarketDataClient)
  TransferSource(MarketDataClient)

lightfee/sidecar/service.py
  SidecarService
    - one MarketDataClient per venue
    - concurrent per-venue refresh with independent timeout
    - degraded_venues/degraded_domains on partial failure
    - publish V2 snapshot with V1-compatible candidate identity fields
```

`MarketDataClient` 必须保持公开数据专用。如果实现过程中超过可维护范围，可拆解析 helper，但不要再造一个新的大杂烩。

## 需要创建和修改的文件

| 路径 | 要求 |
| --- | --- |
| `lightfee/venues/market_data.py` | 新增 `MarketDataClient`、`FundingTicker`、`PerpLiquidity`，迁入公开行情和 L2 能力 |
| `lightfee/venues/transport.py` | 改为复用或继承公开数据层，保留交易私有方法 |
| `lightfee/venues/specs.py` | 增加 sidecar 公开端点字段 |
| `lightfee/rate_limit/config.py` | 补齐新增公开端点权重、scope、min interval |
| `lightfee/sidecar/sources/exchange.py` | 改成真实 wrapper，输出 `QuoteSnapshot` |
| `lightfee/sidecar/sources/liquidity.py` | 改成真实 wrapper，输出流动性数据 |
| `lightfee/sidecar/sources/transfer.py` | 保留兼容实现，可先空结果但不能是未接入占位语义 |
| `lightfee/sidecar/service.py` | 重写为并发拉取、超时隔离、降级不阻断 |
| `lightfee/sidecar/pairing.py` | 补齐 V1 candidate 语义和 V1 问题修复 |
| `lightfee/sidecar/snapshot.py` | 补齐 candidate 字段 |
| `lightfee/sidecar/publisher.py` | 补齐序列化、反序列化、旧快照兼容 |
| `lightfee/sidecar/v1_compat.py` | 保持 V1 快照字段兼容，不丢身份字段 |
| `tests/...` | 先写失败测试，再实现 |

## VenueSpec 字段

新增字段建议:

```python
funding_ticker_path: str = ""
funding_rate_path: str = ""
premium_index_path: str = ""
volume_24h_path: str = ""
open_interest_path: str = ""
transfer_status_path: str = ""
ticker_includes_volume_oi: bool = False
```

`funding_rate_path` 是 OKX 必需字段，不能只靠 `market/tickers`。

## 端点矩阵和解析要求

| Venue | 公开行情/funding | 24h volume | OI | 备注 |
| --- | --- | --- | --- | --- |
| Binance | `GET /fapi/v1/ticker/bookTicker` + `GET /fapi/v1/premiumIndex` | `GET /fapi/v1/ticker/24hr` | `GET /fapi/v1/openInterest` | Aster 同 Binance 兼容 |
| Aster | `GET /fapi/v1/ticker/bookTicker` + `GET /fapi/v1/premiumIndex` | `GET /fapi/v1/ticker/24hr` | `GET /fapi/v1/openInterest` | 与 Binance parser 共享 |
| Bybit | `GET /v5/market/tickers?category=linear` | 同 tickers | 同 tickers | `turnover24h`、`openInterestValue` 可直接作为 quote 口径 |
| Bitget | `GET /api/v2/mix/market/tickers?productType=USDT-FUTURES` | 同 tickers | 同 tickers | 注意 `USDT-FUTURES` 大小写 |
| Gate | `GET /api/v4/futures/usdt/tickers` | 同 tickers | 同 tickers 或合约字段换算 | 用 `quanto_multiplier * mark_price` 换算 OI 时要有测试 |
| OKX | `GET /api/v5/market/tickers?instType=SWAP` + per-symbol `GET /api/v5/public/funding-rate?instId=...` | `volCcy24h * last` 或已有 quote 口径 | `GET /api/v5/public/open-interest?instType=SWAP` | `market/tickers` 不提供可靠 funding/mark 全量，不要误写 |
| Hyperliquid | `POST /info {"type":"metaAndAssetCtxs"}` | 同响应 `dayNtlVlm` | 同响应 `openInterest` | bulk sidecar 不用 `allMids + meta` 作为完整实现 |

统一输出:

```python
@dataclass(frozen=True)
class FundingTicker:
    venue: str
    symbol: str
    bid: float
    ask: float
    bid_size: float = 0.0
    ask_size: float = 0.0
    mark_price: float = 0.0
    index_price: float = 0.0
    funding_rate_bps: float = 0.0
    funding_timestamp_ms: int = 0
    volume_24h_quote: float = 0.0
    open_interest_quote: float = 0.0

@dataclass(frozen=True)
class PerpLiquidity:
    venue: str
    symbol: str
    volume_24h_quote: float
    open_interest_quote: float
    observed_at_ms: int
```

`QuoteSnapshot.open_interest` 现有字段可继续承载 quote-denominated OI，但实现和测试必须明确这是 `open_interest_quote` 口径，避免 base/contract/quote 单位混用。

## V1 问题修复必须一起完成

| 问题 | V2 修复要求 |
| --- | --- |
| V1 `direction_consistent` 用 ask 判断 | V2 用 long/short 各自 mid price: `(bid + ask) / 2` |
| `CandidateInput` 缺 `direction_consistent`、`interval_aligned` | 在 schema 中新增并序列化 |
| 快照序列化字段不全 | `_snapshot_to_dict` 必须写出 `second_funding_timestamp_ms`、`first_funding_leg`、`direction_consistent`、`interval_aligned` |
| 旧 V1 snapshot 可能无 `pair_id` | loader 可兜底补，但 V2 原生快照必须直接写出 `pair_id` |
| `entry_notional_quote` 默认为 0 导致 `ZERO_ORDER_SIZE` | pairing 必须填非零策略入口名义，优先沿用配置中的 fixed live/paper cap 语义 |
| Chillybot lifecycle 耦合 | 完全移除，生命周期字段为空兼容 |
| quote row 字段误判 | V1 row 有 `mark_price`，无 `index_price`；V2 `QuoteSnapshot` 已有二者，应正确填充 |

Pairing 规则:

```text
long side: lower funding venue
short side: higher funding venue
funding_diff_bps = short.funding_rate_bps - long.funding_rate_bps
long_mid = (long.bid + long.ask) / 2
short_mid = (short.bid + short.ask) / 2
direction_consistent = funding_diff_bps > 0 and short_mid >= long_mid
interval_aligned = abs(long_ts - short_ts) <= 60_000
opportunity_type = "aligned" if interval_aligned else "staggered"
first_funding_timestamp_ms = min(long_ts, short_ts)
second_funding_timestamp_ms = max(long_ts, short_ts)
first_funding_leg = "long" if long_ts <= short_ts else "short"
pair_id = f"{symbol.lower()}:{long_venue}->{short_venue}"
```

## SidecarService 要求

1. 每个 venue 一个 `MarketDataClient`。
2. 所有 venue 并发拉取，使用 `asyncio.gather(..., return_exceptions=True)` 或等价结构。
3. 每个 venue 有独立 timeout。单个 venue 失败只进入 `degraded_venues`，不得影响其他 venue。
4. funding/market 每次刷新；liquidity 可先每次刷新，但代码结构要允许后续降低频率；transfer 可先返回空兼容。
5. `source_mode`、`acquisition_mode` 按当前 sidecar 语义填可诊断值。
6. 增加 close/cleanup，退出时关闭底层 HTTP client；如需要，同步更新 `lightfee/apps/sidecar.py`。

## 测试先行要求

执行智能体必须先写失败测试，再改生产代码。最低测试覆盖:

| 测试文件 | 覆盖点 |
| --- | --- |
| `tests/venues/test_market_data_client.py` | public-only 构造不需要 credential；Binance/Aster、Bybit、OKX、Bitget、Gate、Hyperliquid parser；新增端点限速 scope |
| `tests/sidecar/test_sources.py` | `ExchangeSource` 输出 `QuoteSnapshot`；`LiquiditySource` 填 volume/OI；`TransferSource` 兼容空结果 |
| `tests/sidecar/test_sidecar_service.py` | 多 venue 并发；一个 venue 失败不阻断；`degraded_venues` 正确；无 Chillybot 字符串 |
| `tests/test_sidecar_snapshot.py` | candidate 身份和 funding 字段完整序列化/反序列化 |
| `tests/test_strategy_discovery.py` | `entry_notional_quote` 非零候选能通过 zero-size gate |
| `tests/test_venues_transport.py` | `VenueTransport` 继承公开行情后，既保留执行路径也保留原 market snapshot/L2 行为 |

建议验证命令:

```bash
rtk pytest tests/venues/test_market_data_client.py -q
rtk pytest tests/sidecar tests/test_sidecar_snapshot.py tests/test_strategy_discovery.py -q
rtk pytest tests/test_venues_transport.py tests/test_rate_limit.py -q
rtk pytest tests/test_venues_contract.py tests/venues -q
```

最后必须运行 GitNexus 变更检测:

```text
gitnexus_detect_changes(scope="all")
```

## 验收标准

1. `lightfee/sidecar/sources/*.py` 不再是空壳。
2. sidecar 能在无交易私钥场景构造公开行情客户端。
3. `VenueTransport` 不再是 sidecar 唯一路径，sidecar 不依赖下单/仓位接口。
4. 七个 venue 的 funding、bid/ask、mark/index、24h volume、quote OI 至少有 fixture/parser 级覆盖。
5. OKX 使用 `funding-rate` 和 `open-interest` 独立端点；Hyperliquid 使用 `metaAndAssetCtxs`。
6. partial venue failure 只降级，不清空全局候选。
7. 快照 candidate 原生包含 `pair_id`、long/short/first/second funding timestamps、`first_funding_leg`、`direction_consistent`、`interval_aligned`、非零 `entry_notional_quote`。
8. 所有新增端点有 rate-limit scope/weight 测试。
9. 不出现新的 Chillybot 运行时依赖。
10. 执行智能体提交总结中必须列出修改文件、测试命令和 GitNexus 影响报告。
