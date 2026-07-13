# 双策略经济学整改运行手册

状态：**本地实现与完整 pytest 回归已通过；未部署、未完成 24 小时 soak，未 cloud-verified。**

本次变更把资金费与价差策略的候选收益统一到不可变的
`EdgeBreakdown`。`expected_net_edge_bps`、`worst_case_edge_bps` 和
`ranking_edge_bps` 只能由同一个纯函数给出；旧的平铺字段仍会双写，供既有
runtime、journal 与诊断工具读取。

## 资金费策略

- `funding_new_entries_enabled=false` 只冻结新开仓。平仓、恢复、残腿修复和
  exchange truth 同步不受影响。生产配置必须先保持这个值为 `false`。
- sidecar schema v3 承载完整经济学和 funding forecast。解析时同时将 candidate
  的 venue/symbol 回查到两条原始 quote，要求二者均为已标准化的 linear 合约、
  underlying/quote/multiplier 一致且精度、interval、index 与 venue 状态完整；
  因此被篡改或跨合约回放的 candidate 不能凭正确的收益公式取得 live 权限。v1/v2
  快照仍可读，但缺少完整经济学、观测时间或这项合约证明的候选在 live 模式
  fail-closed。
- 候选以同一 base quantity sizing，受共同 L2 容量、名义上限、保证金和
  fallback notional 约束。候选层不再隐含固定 50 quote。交易所有效杠杆只有在
  当次 private position 与 leverage bracket 都完整验证、且 venue/symbol/target
  一致时才能放大 sizing；任一证据缺失、陈旧或不一致时严格退回 1x。
- 动态 Expected Shortfall 使用跨所 paired-basis 历史尾部损失，而不是标的单边波动。
  只有 `FRESH` 主 sidecar snapshot 可以更新该模型；last-good、degraded、观测异常、
  缺失/陈旧 checkpoint 或单条 quote 时间戳超龄都会让 live 新开仓 fail-closed。
  当前 snapshot batch 永远不进入自身 ES 估计，静态
  `funding_expected_shortfall_bps` 只可作为保守 floor，不能替代缺失的动态证据。
  生产若要打开 `funding_new_entries_enabled=true`，必须同时保持动态 ES 启用并设置
  正数 `funding_expected_shortfall_budget_quote`。
- 第一腿前使用当前 quote-lease 或本地 L2 双腿顶档再次计算收益。首腿成交
  后，`FundingEntryRevalidator.decide_after_first_leg` 只在完成 hedge 和
  回补首腿之间选择较小损失的受控路径；不得丢弃已成交裸腿。
- WS BBO、local-L2 与 sidecar 源行情的时间戳不得晚于当前刷新/决策时钟；未来
  时间戳一律不是可执行证据。它不能进入首腿后的 hedge/unwind 选择、候选排序或
  funding forecast 校准持久化。
- local-L2 路径的顶档会转成最终 quote lease，避免把可用 L2 误判为
  `missing_final_executable_bbo`。陈旧或无效 L2 继续发出原有精确阻断事件。
- live 进程的 HOT 本地深度会以原子、带时效的只读 bridge 提供给 sidecar；只在
  深度顶档与 sidecar BBO 一致、且两者观测时间落在 `spread_quote_skew_ms` 内时附加
  L2。否则退化为 BBO 容量，永不覆盖 BBO，也不增加 public REST 请求。

`funding_economics_mode` 的发布顺序固定为 `v1_exact`，然后
`enhanced_shadow`。forecast 仅能先 shadow 记录至少 7 天；样本不足时使用
配置的 uncertainty haircut，不能将当前费率伪装成高置信预测。

资金费归因现在通过独立于 V1 平仓/恢复状态机的 durable accounting queue 完成：
关闭前先保存所需 settlement 与已有事实，后台只在 live 模式查询 private
statement，成功后才发出 `funding.settlement_reconciled` 并给出 official PnL。
Binance、Bybit、OKX 与 Gate 已有私有账单解析；Bitget、Aster、Hyperliquid 暂无
可验证的账单适配，会保持 unresolved/expired，绝不把预测值写成 official PnL。
同一 `(venue, symbol, settlement timestamp)` 同时对应多个内部仓位、时间戳/币种
不精确匹配或账单冲突时一律 fail-closed。该队列不影响 exchange truth、残腿修复
或新开仓 gate。

终态 `exit.closed` 与 `exit.passive_close_resolved` 使用仓位累计的价格 PnL、四腿
费用和生命周期 funding，避免分块平仓把 entry fee/funding 重复计入多个 closed
事件。它仍然不等于交易所官方 PnL，直到上述 statement reconciliation 完整成功。

## 价差 paper 策略

- 每个 `(symbol, venue_a, venue_b)` 用固定规范顺序维护 signed basis；穿越
  零轴不会创建新的统计序列。
- 信号在写入当前样本前用已有窗口计算，中心为 median、尺度为
  `1.4826 × MAD`（退化时才用标准差），窗口有界且支持 checkpoint。
- 仅在 AR(1) `0 < phi < 1`、半衰期合格且未发生 structural break 时允许
  entry。普通跨所均值回归与单所 dislocation 标签严格分开。
- 毛收益是当前可执行价差到目标退出价差的可回归部分，不是完整跨所价差；
  随后统一扣除四腿费用、滑点、funding、adverse selection 与 buffer。
- paper journal schema v2 记录状态机、共同 base quantity、残余敞口与 epoch。
  maker 只有穿价才成交；没有 queue/trade-tape 证据的 maker 结果不进入主
  验收组。official funding 仅在真实跨越 settlement 时间后、且注入已分配的
  settlement record 时计入。当前/预测 funding rate 或估算值不能计入；只有在
  settlement 后一个 quote TTL 内观察到的公开 `settled_funding_rate_bps`，配合
  同时的 mark 与实际共同 base quantity，才可生成可追溯的 paper 分配记录；
  迟到、缺失或换期的观察仍 fail-closed。
  `official_pnl` 还是 literal boolean permission，任何字符串或其他真值对象均
  fail-closed，不能污染验收 cohort。
- `config/research/spread_v2_signed_reversion.json` 是 cohort 的唯一研究清单；
  它记录假设、启用状态、control 属性和 acceptance eligibility。变更任一
  研究参数都必须新建 manifest/version 与 model epoch，不能回写历史。
- 离线分析默认只接纳该 epoch 的 official taker/taker 闭环，legacy、control、
  stale/unpriced 与 expired 都单独计数；报告提供独立 episode、fill 状态、
  各成本分解、venue pair/regime 分组以及 deterministic block-bootstrap 95% CI。
- spread sidecar 只能读取主 sidecar snapshot（并可附加上述本地 L2 bridge）；任何
  `spread_sidecar_direct_fetch_enabled` 或非 `sidecar_snapshot` source mode 都会在
  服务构造时 fail-closed，防止 paper 路径暗中新增市场数据请求。
  主 snapshot 的 `published_at_ms`、`market_observed_at_ms` 和每条 quote 的
  `observed_at_ms` 都必须在同一 freshness budget 内，且 quote 价格/size 必须有限、
  正常、非 crossed；外层 snapshot 新鲜但单条 quote 陈旧或非法时，只能降级/剔除，
  不能进入 signed-basis 统计、paper 成交或 official PnL。
- `SpreadTradingController.evaluate_entry` 会完整运行 entry admission 诊断并输出
  hypothetical notional 证据，但在本 epoch 永远不返回 `SpreadOrderIntent`；因此
  即使程序化构造 `StrategyConfig(spread_live_enabled=True)`，spread 也不会从控制器
  产生活体 entry intent。真实执行接口只保留为未来方案的类型边界。

## 运行时配置合同

关键交易路径直接消费 `StrategyConfig` / `RuntimeConfig` 的已声明字段，不再以
`getattr(..., default)` 静默放宽配置。兼容性仅保留在 schema/parser 边界。maker
对冲软、硬期限均为显式配置（默认 800ms），且校验要求正数并满足 soft ≤ hard；这
保证启动时即可发现配置遗漏，而不会在已有 maker fill 后才回退到隐式默认值。
`runtime.funding_basis_risk_checkpoint_path` 必须非空；该 checkpoint 是 live 新开仓
风险证据的一部分，不能由临时内存状态或旧 sidecar snapshot 替代。

## 发布与验收（人工受控，不由代码自动打开）

1. 部署安全开关，生产保持 `funding_new_entries_enabled=false`。
2. 确认云端 HEAD 与本地待发布提交一致，且 exchange truth flat、无 open
   orders、无 pending entry/close/residual。存在仓位时仅走既有安全关闭路径。
3. 资金费 `v1_exact` 与 `enhanced_shadow` 并行至少 7 天或 500 候选，保存
   candidate/block/ranking/sizing 及 forecast error 对比。
4. 仅在 shadow 无语义差异后，才可单仓 30 quote canary；至少完成 30 个完整
   闭环、所有成本归因完整、无未收口订单或残腿、每次关闭后 truth flat，且
   p95 实际成本不突破 worst-case buffer，才可另行审批最多两仓。
5. spread 永远保持 paper-only。本轮不允许自动开启 `spread_live_enabled`。
   `v2_signed_reversion` 至少运行 30 天、200 个去重 taker/taker 闭环、覆盖
   3 个主要 venue pair 与 2 类波动 regime，并通过 95% block-bootstrap 和
   2×成本压力门槛后，才有资格另行提出小仓 live 方案。

每次生产发布须保存以下结构化证据；本地 pytest 不能替代这些证明：

```text
verify_deploy_manifest.py --check /opt/lightfee-v2
check_process_singleton.py --strict
verify_production_services.py --json
diagnose_live.py --json --since-deploy
```

## 本地验证

```text
/Users/wl/projects/LightFeeV2/.venv/bin/python -m pytest -q
/Users/wl/projects/LightFeeV2/.venv/bin/python -m compileall -q lightfee tests
/Users/wl/projects/LightFeeV2/.venv/bin/ruff check lightfee/engine/funding_risk_runtime.py tests/test_funding_basis_risk.py lightfee/config/validation.py tests/test_config.py
git diff --check
```

历史事故回放、24 小时 soak 以及云端证明链仍是发布前的必经门槛；不能把本地
完整回归写成 cloud-verified。
