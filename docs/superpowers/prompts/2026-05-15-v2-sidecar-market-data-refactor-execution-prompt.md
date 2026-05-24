# V2 Sidecar 公开行情层大搬家执行提示词

你是执行智能体。严格执行 `docs/superpowers/plans/2026-05-15-v2-sidecar-full-implementation-plan.md`，不要做过渡方案。

硬性规则:

1. 先读 `AGENTS.md` 和方案文档；编辑任何 symbol 前按项目要求跑 GitNexus impact。
2. 先写失败测试，再改生产代码；不要跳过 RED 阶段。
3. 新增 `lightfee/venues/market_data.py`，实现 credential-free `MarketDataClient`、`FundingTicker`、`PerpLiquidity`。
4. `VenueTransport` 只能继承或复用公开数据层，保留下单/仓位/账户风控职责；不要把 sidecar 新 fetch 方法继续塞进 `VenueTransport`。
5. 保留并实现 `lightfee/sidecar/sources/exchange.py`、`liquidity.py`、`transfer.py`；不要删除这三个 source 文件。
6. `SidecarService` 只通过 source/`MarketDataClient` 拉公开数据；并发按 venue 拉取，单 venue 失败只标记 `degraded_venues`。
7. 端点必须按方案纠偏: OKX 用 `funding-rate` + `open-interest`，Hyperliquid 用 `metaAndAssetCtxs`，Bitget 用 `USDT-FUTURES`，Binance/Aster 用 bookTicker + premiumIndex + 24hr + openInterest。
8. 补齐 rate-limit scope/weight/min interval 和测试。
9. candidate 必须原生写出 `pair_id`、long/short/first/second funding timestamps、`first_funding_leg`、`direction_consistent`、`interval_aligned`、非零 `entry_notional_quote`。
10. 不引入 Chillybot 运行时依赖；transfer 可先空结果兼容，但不能阻断 sidecar。

最低验证:

```bash
rtk pytest tests/venues/test_market_data_client.py -q
rtk pytest tests/sidecar tests/test_sidecar_snapshot.py tests/test_strategy_discovery.py -q
rtk pytest tests/test_venues_transport.py tests/test_rate_limit.py -q
rtk pytest tests/test_venues_contract.py tests/venues -q
```

完成前必须运行 `gitnexus_detect_changes(scope="all")`。最终回复只给: 修改文件、测试结果、GitNexus 影响摘要、未解决风险。没有这些视为未完成。
