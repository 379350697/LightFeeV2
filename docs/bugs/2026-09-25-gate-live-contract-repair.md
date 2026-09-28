# Gate live contract incident — 2026-09-25

## 当前结论（北京时间 2026-09-27）

**implemented but not closed：本轮 P2 本地代码收口，完整生产事故仍未闭环。**
当前冻结版本全量为 **5163 passed / 9 skipped / 1 warning**，full profile **10/10 通过**。
9 月 27 日最终审核发现的 P2 已在两个既有 WS 解析器修复：空集合不再解除故障，
无效身份不再生成伪缓存键，web/api/app 来源标签不再把不同订单合并。
两个把空列表当作恢复证据的旧测试已按契约修正。当前 45 项定向、500 项相邻回归通过；
源码/测试/依赖共 55 文件冻结；独立复审新增 17 个状态检查通过，未发现新的
可证实 P0/P1/P2，没有本轮新增无关代码或必须撤销的过度设计。
历史 4932、4996、5038、5132 等测试结果均为当时版本；最新状态以本节和文末为准。
生产仍为旧代码，异常多仓未处理；未执行订单、撤单或启用交易。

**21:35:08（北京时间）只读核验**：Gate `SAGA_USDT dual_long=501`、`dual_short=0`，
Binance `SAGAUSDT quantity=0`，双方该合约未成交订单均为 0。Gate 多仓入场价
`0.048207784431`，标记价 `0.02606`，未实现盈亏 `-11.096039999931 USDT`，
`pnl_fee=-0.01207605`、`pnl_fund=-0.24101360007`。这些字段不能与策略报表直接
相加当作完整账户收益。服务器仍部署 `339064218db45f68c268e11131e0a49620c9c05d`。
异常仓仍在，需人工处置；[最新交易所证据](/tmp/lightfee-root-repair-20260925/ws-closure-production.json)。

### 逐项问题、根因和处置

| 问题 | 已有证据及根因 | 本地处置／剩余条件 |
|---|---|---|
| **P0：计划开空，Gate 实际开多** | 原单 `160159262129906499` 的原始 `size=+501`，本地意图 SELL；Gate 以 size 正负决定方向。被动订单构造遗漏符号，ACK/成交读取又用请求方向覆盖实际方向 | 统一 Gate 带符号合约数量构造和实际方向解析；响应冲突保留订单身份并报结果不明；买卖、开平和 reduce-only 反例覆盖。线上旧代码仍未替换 |
| **P1：挂单语义与价格错误** | 原单 `tif=gtc`，并非 Gate 的 post-only `poc`；静态 tick `.01` 把 `.04814` 归一成 `.05`，SAGA 实际 tick `.00001` | 使用动态合约元数据和 Gate `poc`；元数据失败阻断，不再静默用静态规则。对照 V1 `submit_passive_entry_order`、`parse_gate_contract_meta` |
| **P1：ONE 对冲无法进入下单且反复暴露单腿** | 最新部署后 198 次零名义价值拒绝、33 次 maker 已成交后耗尽生命周期；ONE 合约乘数 10、tick `.000001`。Gate 缺动态数量/价格规则，同时合法 min-notional=0 被消费者误判为未知 | 同一份规则贯穿准入、数量、下单、查询、持仓、私有 WS；引擎对零门槛保留“已知”语义。REST/WS 合约单位矩阵覆盖 |
| **P1：成交终态与成交量不可信** | 旧解析把 FINISHED 直接当全成交，未统一使用 `abs(size)-abs(left)` 与合约乘数；取消、部分成交和无成交可能混淆。审查另复现 P2：明确的 `reduce_out` 被当 UNKNOWN | REST 查询、ACK、被动进度、私有 WS 复用 Gate 执行字段解析；不合法/非有限价格不能成为已证实成交；所有官方取消原因覆盖零/部分成交，未知原因仍保留 UNKNOWN |
| **P0/P1：反向异常仓一直清不掉** | Binance 的 501 正向腿已清；Gate 错向多仓仍在。约 470 次清理跳过把“预期空头数量=0”解释成无仓，丢掉 actual long | pending truth 保留实际 venue/side/quantity；仅在唯一归属、双边订单证据完整时按实际方向形成 reduce-only 清理；清理后重新验证双边仓位与订单，未知时保留任务 |
| **P1：零成交／过期／强制恢复仍能错误删任务** | 独立审查复现 maker-only 查询、known-zero 快捷路径、缺 adapter 当平仓等旁路；正成交 500/500 对 live 501/501 可提前写 `entry.opened`。另复现已有持仓 500 时，后续 501 发布在重放中被跳过，pending 却被删除 | 统一删除与发布前的承接校验；完整 open position + residual 必须覆盖实际仓位。快照后同 entry 的新发布按顺序替换旧持仓，再删除 pending；验证每个日志前缀、连续重启及后续平仓。回退下单前持久化 CID，失败/部分成交进入待对账任务 |
| **P1：残仓处理后重启恢复错误** | 新的实际路径矩阵复现剩余数量、修正方向、旧 ACK 标记和 pair gate 在重放中不一致 | 每个残仓 completed/terminal 事件记录该任务范围的实际剩余任务及 pair gate；重放恢复完整状态，未知旧事件保留任务等待交易所证据 |
| **P1：健康检查可淡化严重阻塞** | 恢复决策已判 critical，但 lifecycle 为 RUNNING 时 severity 分支未采用它 | severity 消费现有 critical 决策；增加 RUNNING+严重恢复产物反例。具体哪个 writer 把 lifecycle 恢复 RUNNING，历史日志不足，仅补埋点 |
| **P1：Gate ACK 丢失后的 CID 恢复缺口** | 旧 36 字符 CID 超出 Gate text 前缀后的 28 字节限制；旧 bug 文档错误称 CID-only 无查询通道；open-order 原始 text/contract 未归一给归属判断。另复现 P2：异常 ACK 中已取得的数字订单号被执行器丢弃，但 CID 尚在 | 新生成 ID 限 28；历史 ID 原样保留；查单接受精确 text 并验证返回身份；taker/passive 的 uncertain 结果同时保留订单号至日志与 pending。文档 CL-157 已更正，新增私有 WS 小数数量协商头 |
| **P1：健康风险快照被误判缺失** | 已有 runtime 写入 `(True, snapshot)`，supervisor 只识别裸 snapshot；两边健康度 10 仍进入 fail-closed。仅剩 pending close 时也没有采集入口 | 沿用 runtime 统一缓存与解包，housekeeping 覆盖全部受监督交易所，supervisor 仅消费领域快照；删除重复解码。35 项真实链路矩阵和独立反例通过，Gate capability 策略未放宽 |
| **P1：完整账户归属误放行** | 只比 venue/symbol、按 owner 去重丢行、只遍历实际有仓的腿，导致反向仓、超量、首次缺腿被误认正常；本地 0/501 或 501/300 无恢复任务也能 RUNNING_CLEAN。V1 要求正常等量双腿或明确的残量承接 | OwnerIndex 统一真实方向、全量数量、阶段去重和恢复承接；完整 account 才推导缺腿，缺腿记录实际 0 与预期数量；双边皆零保持 V1 退休；旧锁未取得完整证明不释放。cleanup 单独保护部分/未知 claim |
| **P1：活跃订单被当作不存在** | ledger 按正数量筛单，core 跳过 reduce-only；负数卖单、零量自动平仓单和坏数值均可被丢弃。table 对 Gate 合约别名的重复解释还会错误清除 pending | 三个消费者按活单存在性处理；数量不可解析保留原始值和错误，不能成为无单证明；table 复用现有 canonical symbol。实际 HTTP → collector → 三消费者覆盖 64 个数量/类型/范围组合 |
| **P1：坏行或元数据超时抹除同批异常仓** | adapter/collector 全批失败后清空已取得行，正常双腿之外的 Gate 反向 20 被另一坏行掩盖；仓位响应成功后元数据请求超时也会发生 | 复用 TransportError 保存成功行、未知行及错误；交易 adapter 仍严格失败，account/symbol 诊断保留更强证据；取消继续抛原 CancelledError，wait_for 超时链携带已观察行。真实 HTTP 反例覆盖混合、顺序、首次阻断和完整证明释放 |
| **P2：真实快照和诊断的证据契约漂移** | current-state 只导出 matched quantity；三个 collection 解析器把 nested row/error envelope 解释不同；缺挂单集合又被补成空集合 | 原 producer 导出每腿数量；旧数据不反造数量；ledger/core/table 共用纯归一化与证据函数；错误保留证据缺口，真实异常行不丢弃，missing/None 不合成空集合 |
| **P2：入场事件重放改变费用和方向** | 两处 entry.opened 手写字段子集丢失费用证据/名义金额；SELL 反序列化调用不存在的 Side.from_str，异常后退成 BUY | 两处复用现有完整 OpenPosition 序列化；使用实际 Side 枚举构造；真实入场 → critical journal → restart → close accounting 验证金额及费用证据等价，legacy 缺失证据仍不当成已对账 |
| **P2：Gate WS 伪健康及订单缓存串单** | 建连/任意 JSON 被当成功；错误订阅和坏 update 未正确计数；重连 ACK 又清零连续归一化失败；dual 分支早退、空集合或无有效订单身份仍被当成功；普通 text 来源标签被错误当作 CID，web 同源的不同订单被合并 | 两频道明确订阅成功、10 秒 ACK 窗口、错误重连；坏消息保持失败频道，ACK/PONG/其他频道/未知合约/空集合不能代替该频道有效 update。订单在转换前验证 ID 类型，只有非空 t- 自定义信息进入 CID 索引；合法零成交、零持仓和 dual 仍可恢复，dual 不写单向缓存；复用现有解析器和 ConnectionHealth |
| **依赖声明不能保证 Gate WS 协商生效** | websockets 13.1 不支持当前 additional_headers 调用；原 >=12.0 声明允许安装不兼容版本 | 最低版本提高至 14.0，同步 lock；真实 localhost 握手验证 14.0/16.0。部署生成器在同步前检查远端调用能力；独立假 SSH 验证旧版本先退出，服务器当前为 16.0 |
| **全量门禁的 25 项基线失败** | 7 项测试 adapter 缺杠杆准备能力，17 项旧 fixture/断言不符合完整平仓数量及身份契约，1 项 Bitget settings 调用绕过 operation registry | 修复 fixture 和明确有权威依据的断言，不放宽生产保护；Bitget 在现有 registry 增加并使用 ACCOUNT_SETTINGS。全量清零，不将 25 项测试失败误报为 25 起线上事故 |
| **证据不足：Gate 风险健康能力** | 当前 `supports_risk_health=False` 与 V1 一致；旧 margin 计算不是当前运行时实际路径。缺足够账户模式/保证金原始证据 | 记录能力、缓存、是否调用和来源；不凭猜测开启风险计算。下一次需补账户模式及风险字段原始只读快照 |
| **证据不足：生命周期为何显示 RUNNING** | 严重恢复阻塞与 RUNNING 同时存在；怀疑 close reconciliation 写入路径，但没有当时 writer 证据 | 新日志记录写入源、前后 lifecycle、risk/operator 模式、阻塞原因和各类待处理数量；不把怀疑写成已确认根因 |
| **收益／账单未闭环** | 13 对已关闭仓报 modeled funding `.610057`、net `1.173245`；不包含异常 SAGA 的全部成本。V1 也以入场 edge 估算 funding；这不等于交易所现金账单已核对 | 增加 `funding_pnl_source=entry_edge_estimate`、`funding_statement_reconciled=false`。保留账单缺口，不能据此宣布实盘盈利或平仓核算正常 |
| **旧修复“已部署”不等于同族闭环** | 云端部署标记 `33906421` 与本地 HEAD `48e57afd` 的已知差异为文档；目前无证据说明事故仅因二进制没更新。已有补丁覆盖数字查单等局部分支，未覆盖上述 contract 与生命周期旁路 | 保留既有历史结论并记录更正；本次不把 Git marker 差异当根因。必须完成审查、部署和实盘证据后才能讨论生效 |

### 对未确定问题补的日志

- 生命周期各写入者扩充既有事件；`runtime.lifecycle_decision_observed` 只作诊断，不参与状态重放。记录写入源、前后状态、风险/operator 模式、恢复 core 来源与决策、恢复阻塞、各种 owner 数量和回滚前后状态。
- `runtime.risk_snapshot_capability`：能力与调用来源；同能力状态只记录一次，避免刷屏。
- `venue.health_changed`：能力、是否提供本次结果、快照是否存在、来源、年龄、过期标记，以及 unsupported policy；runtime fetch 错误/无快照另有原因事件。
- `runtime.funding_capture_state_updated`：两阶段金额、前后增量、名义金额及其来源、价格/数量、入场和总 edge；最终/部分对账事件明确是估算、未完成资金费现金账单核验。
- `pending_entry.terminal_removal_evidence`：双边实际仓位、所有订单、拟承接数量、是否允许删除及原因。
- 沿用订单 submit/progress/reconciliation 诊断，保留 Gate 原始方向、数量、精度、归一化及结果不明的订单身份。

这些日志同样只在本地代码中；未部署前，不能承诺下一次线上复现会自动产出新增字段。

Status: implemented but not closed. No production code deployment or trading-state
mutation has been performed by this repair. Manual SAGA close was requested;
it has not been confirmed.

## Frozen evidence

- Baseline HEAD: `48e57afd17a535bc040b92cb61160bdf59ae173d`; branch `main`.
- Pre-existing changes: `.deploy_manifest.json`, four egg-info files,
  `.dev-flow/`, and the untracked `" in text or b-` file. Preserve these.
- 2026-09-25 15:54:14 UTC: Gate SAGA_USDT dual_long 501, dual_short 0,
  no open SAGA orders. The planned Binance long was already flattened.
- Gate original order 160159262129906499: size +501, tif gtc, filled;
  local intent SELL, normalized price .05 from .04814 using static tick .01.
- ONE: 198 zero-notional hedge rejections and 33 maker-filled lifetime aborts
  after the 2026-09-22 deployment. Gate ONE multiplier is 10, tick .000001.
- Pending-entry truth discarded opposite-side quantities; cleanup repeatedly
  reported no live single leg while Gate still held the long.

## Authoritative contracts

- Gate API: https://www.gate.com/docs/developers/apiv4/en/futures/
  signed size controls direction; poc is post-only; size/left are contracts;
  order_price_round and quanto_multiplier are symbol-specific.
- Rust V1 `src/live/gate.rs`: submit_passive_entry_order, parse_gate_contract_meta,
  gate_contract_step, position/fill conversion use the same contract multiplier.
- V1 live exchange truth, ownership and terminal flatness outrank local planned
  directions. Missing evidence must retain the owner and block release.

## Root mechanisms and shared boundaries

1. Gate passive builder omitted sign and used gtc; generic response parsers
   relabelled actual side and treated FINISHED as full execution. Repair Gate
   wire/parse normalization, retaining exact accepted identity on uncertainty.
2. Gate had no dynamic SymbolRule producer. Static .01/1 metadata corrupted
   price and base/contract units. One exchange-backed rule must feed admission,
   passive/taker orders, position/fill/progress reads, quantity planning and WS.
3. PendingEntryLiveTruth retained only expected-side quantities. Preserve actual
   venue/side/quantity evidence through cleanup selection; never net two opposing
   positions to flat or remove a pending owner on incomplete post-cleanup truth.
4. Health computes a critical recovery decision but ignores it when assigning
   severity. Consume that authoritative decision regardless of lifecycle label.

Former tests asserted post_only=True (an ineffective Gate body field), used
unit contract sizes and tested cleanup only in the expected direction. They
missed raw signed exchange payloads and the real HTTP-to-runtime path.

## Impact and path map

GitNexus index baseline matches HEAD. Upstream impact: preflight and symbol-rule
cache CRITICAL; prepare_order_request and position parser HIGH; Gate order status
and passive ack CRITICAL. Direct callers: prepare/place/submit/precheck,
fetch_order_status, fetch_position/fetch_all_positions, passive query/cancel.
Pending cleanup callers: positive-fill finalize, zero-fill finalize, reconciliation.
Dynamic runtime delegation is also traced with repository search because graph
edges do not fully resolve RuntimeContext dispatch. Raw impact output is under
`/tmp/lightfee-root-repair-20260925/`.

Path: public metadata -> normalized base-unit rule -> both-leg admission ->
signed contract wire order -> actual exchange execution/position -> base-unit
domain evidence -> pending terminalizer/cleanup -> owner retain/release -> health.

Identity extension in the same wire contract: Gate `text` supports a `t-` prefix
and at most 28 ASCII bytes after it; exact order APIs accept that text. The former
36-character generator and CID-only refusal break recovery after a lost ACK.
Generator impact HIGH (entry dispatch and passive close); preserve persisted IDs,
generate legal new IDs, validate returned numeric ID/text, and never infer zero
fill from a text lookup miss. Local L2 already converts through
`GateAdapter.l2_book_quantity_to_base_scale`; do not multiply it again.

## Counterexample matrix established before implementation

| Boundary | Cases |
|---|---|
| Direction | buy/sell, open/reduce-only, wrong-side acknowledgement |
| Units | multiplier 1, 10, fractional multiplier; decimal contract step |
| Metadata | complete, absent, malformed, nonfinite, symbol mismatch, retry |
| Order state | resting, partial, filled, canceled without fill, canceled partial |
| Truth | matching, opposite direction, both sides, missing/error, post-close residual |
| Ownership | owned single leg, multiple live legs, competing owner, unknown owner |
| Release | complete fresh flat proof releases; live/open-order/error retains |
| Health | RUNNING with critical recovery artifact, ordinary pending, clean flat |

## Evidence gaps: instrumentation instead of speculative behavior changes

- Lifecycle release writers: log source, before/after lifecycle, risk/operator
  mode, pending owner counts and recovery block at the suspect close-retry path.
- Gate risk health: keep V1 unsupported capability; log capability/cache/fetch
  source so unsupported is distinguishable from endpoint failure. Current raw
  account-mode/margin semantics are insufficient to enable risk evaluation.
- Funding: distinguish modeled funding estimate from exchange-statement evidence;
  V1 also estimates. Do not describe modeled pair PnL as reconciled cash income.

## Validation and closure

### Repair-loop reset after first independent review

Status remains not closed. The first review reproduced four P1 mechanisms:
maker order-query failure was discarded by position aggregation; Gate raw
`text`/`contract` did not reach canonical order identity; cleanup ignored hedge
and unowned same-symbol orders; engine minimum-notional consumers discarded
the authoritative Gate zero. It also found missing decimal negotiation on WS.
Targeted tests had missed alternate order-evidence and min-notional consumers.

Renewed invariant: a destructive cleanup requires one unambiguous owned live
leg, complete order-list evidence for BOTH venues, no same-symbol open orders,
and fresh complete flat evidence before owner release. Position-only truth
does not imply order completeness. Partial evidence retains known live positions.
Gate raw order identity is normalized once in `exchange_truth` for pending,
startup recovery and residual-repair consumers; business matchers keep consuming
canonical identity. Gate exchange metadata, including an explicit zero notional
minimum, governs entry, hedge, residual and passive-close consumers.

Additional matrix before renewed implementation: maker/hedge order timeout;
CID-only maker; unrelated same-symbol order; malformed/missing identity row;
error/live order appearing AFTER cleanup; complete empty lists before and after;
ONE below static $1 but above contract minimum; missing/invalid metadata;
decimal WS handshake and actual worker-to-cache execution. Existing fixture
failures are incomplete mocks (missing Gate public metadata/execution fields,
or `None` instead of a proved flat position); expected outcomes stay unchanged.

Impact: `parse_open_orders_response` LOW, direct callers
`require_open_orders_response` and `probe_venue_open_orders_flat`, including
recovery, residual and passive-close consumers; residual fetch LOW. Dynamic
pending runtime dispatch is graph-UNKNOWN and traced with local searches.
`_resolve_hedge_min_notional_quote` HIGH: `_submit_hedge_for_delta`,
`drive_pending_passive_close`, `_handle_live_imbalanced_positions`,
`_flatten_live_one_sided_position`. `_venue_min_notional` remains absent from
the graph after a fresh rebuild (large runtime module); local call tracing
covers hedgeability, dust terminalization and residual repair. No new trading
state mutation or deployment is authorized by these offline tests.

Pending: real-path RED/GREEN, contract matrix, adjacent/profile/full suite,
independent read-only review, production read-only proof after user deployment.
Record exact commands and outcomes below; never mark this repair closed using
only helper tests, scoped green tests, or a refreshed source index.

### Second repair-loop reset — terminal removal boundary

The second independent review verified the positive-fill repairs but reproduced
three old release bypasses: zero-fill finalization used maker-only truth; known
zero-fill reconciliation skipped truth; stale abandonment treated a missing
adapter as flat and trusted local maker completion. All reach
`_complete_pending_entry_terminal_removal`, which previously trusted its callers.
The root is duplicated release authority, not another venue-specific condition.

Renewed contract: every live terminal removal must either prove BOTH venues'
positions and symbol-scoped orders clear, or retain exposure in an existing
same-entry OpenPosition/residual successor with clear order evidence. A caller's
reason, submit certainty, timeout, or local zero is not terminal proof. Unknown
or live evidence retains the original owner. Zero finalization and abandonment
use the existing pair-truth producer; the shared async removal boundary rejects
all other legacy bypasses. Successful owner transfers must remain possible.

Path inventory: normal and forced startup reconciliation; rejection; passive
maintenance; stale/budget/startup abandonment; abort cleanup; zero/positive
finalizer; fallback-to-taker and residual transfer. All runtime pops use the one
removal authority. Journal replay consumes its durable closure event, or a
durable `entry.opened` successor; it must not emit closure on blocked removal.

Pre-implementation matrix: each zero/rejected/forced/stale/direct-removal route
crossed with clean flat, maker orphan order, hedge order, hedge position,
missing adapter, order error and position error; complementary open/residual
transfer; fresh post-cleanup evidence. Gate shared REST/WS execution parser must
also reject NaN/infinite/invalid/negative supplied fill prices while retaining
accepted order identity as uncertain. No exchange mutation in these tests.

### Third repair-loop reset — durable publication precedes removal

The third review reproduced local fills 500/500 versus live positions 501/501:
force reconciliation wrote `entry.opened(500)` before the removal gate rejected
insufficient successor coverage. Replay consumed that premature event and
deleted the pending owner. In-memory retention alone is not durable retention.

Renewed invariant: validate the complete proposed successor set against both
venues' actual positions and open orders BEFORE publishing any terminal owner
event. Every crash prefix must retain the pending owner or restore successors
covering the full exposure. Reuse the pending runtime's coverage authority;
do not duplicate a quantity check in the finalizer. The scan also includes
EntrySyncExecutor's taker-fallback publication and residual replay, not only
the final in-memory pop. The change remains not closed during this reset.

Matrix before implementation: balanced, partially matched and unmatched fills;
exact versus undersized successor; open order or unavailable truth; repeated
finalization; replay from a prior snapshot and at each durable-event boundary.
No test may claim success by examining only the final in-memory dictionary.

The same handoff scan found terminal taker fallback ignored non-success
`EntryExecutionResult.pending_entry` and retained the passive zero-fill retry
flag. Its changed CIDs therefore also need a durable pending registration
BEFORE submission, and all uncertain/partial results must update that same
owner. The existing deterministic order builder and pending snapshot schema
are reused. Matrix adds maker uncertainty, hedge rejection/uncertainty, partial
fills, and replay at each submit. Runtime delegation is traced with `rg`:
maintenance/zero-fill completion -> terminal fallback -> EntrySyncExecutor ->
pending reconciliation/replay. This is high-risk ownership code; graph impact
does not resolve the dynamic runtime edges.

Residual completion must persist its actual remaining owner set, including
direction changes and cleared accepted-order evidence. Reconstructing a task
from only `remaining_quantity` duplicates the live transition and can retain
an obsolete direction/CID after replay. The existing residual runtime records
the scoped remaining tasks and pair gates in every completed/terminal event;
replay restores this canonical state without reimplementing cleanup policy.
Counterexamples: live flat, full/partial fill, accepted-order partial fill,
direction rebuild, dust terminal, unavailable/malformed event state, repeated
replay, unrelated residual owner. This is persistence-only; order decisions
remain owned by the existing residual runtime.

### Fourth repair-loop reset — ordered replacement of an existing successor

Independent review reproduced a prior OpenPosition of 500/500 plus a pending
entry and verified live exposure of 501/501. Finalization replaces the live
position with 501/501, but recovery skipped `entry.opened` when that ID already
existed, then removed the pending entry. The residual 1/1 had no owner after
restart. The former matrix started with only pending entries and missed the
same-entry existing-position branch. Status remains not closed.

Renewed contract: a complete, ordered `entry.opened` after the snapshot's byte
checkpoint replaces that entry's previous position BEFORE consuming pending.
Rust V1 `src/engine/recovery.rs:2936–2970` uses ordered map insertion for this
transition. V2's partial `recovery.live_detected` audit events only supplement
missing positions and must not erase an existing complete position. Subsequent
close events still remove the position; old events before the checkpoint must
never resurrect it. The existing replay owner is the shared broken boundary;
no quantity-max merge or new persistence layer is needed.

Additional matrix: no prior successor, smaller prior quantity, larger prior
quantity, partial match with residual; full/order-gap/undersized/error evidence;
every journal prefix, actual SnapshotStore recovery, repeated startup and a
later terminal close. Replay impact is LOW in the graph (direct caller
`recover_from_snapshot`), but operational risk is HIGH for restart ownership.

The same review found two P2 evidence losses: official Gate `reduce_out` was
missing from the common REST/WS terminal parser, and EntrySyncExecutor discarded
`OrderSubmitError.accepted_order_id` on both taker and passive submissions.
The CID survives the latter, so this is not evidence of a completely lost owner.
Repair their existing parse/submit boundaries; cover every official cancel
reason with zero/partial fills, unknown reasons retaining UNKNOWN, and real
HTTP-to-executor uncertain ACKs on both submission paths. Gate parse impact is
HIGH, with `_handle_gate_order_data` and `_gate_order_progress` direct callers.

## 历史阶段验证与交付状态（2026-09-26 早期；已被文末验收更新）

### 修复前后的可复现证据

以下均为离线真实生产调用路径，交易所边界使用 HTTP MockTransport / fake
adapter，没有替换被修方法，也没有向交易所发出交易请求。

| 验证 | 命令／范围 | 结果与日志 |
|---|---|---|
| 同 entry 持仓交接 RED | `pytest -q tests/test_pending_entry_v1_semantic_drift.py -k successor_publication --tb=short` | 3 failed / 21 passed，exit 1；[RED](/tmp/lightfee-root-repair-20260925/ordered-handoff-red.log) |
| 同一矩阵 GREEN | 同上，实际 finalizer → 日志前缀 → SnapshotStore/checkpoint → 多次 recover → 后续 close | 24 passed / 173 deselected，exit 0；[GREEN](/tmp/lightfee-root-repair-20260925/ordered-handoff-green.log) |
| Gate 终态及异常 ACK RED | `pytest -q tests/test_gate_live_contract.py -k 'private_wire_uses or finished_does or uncertain_ack_identity' --tb=short` | 8 failed / 36 passed / 60 deselected，exit 1；[RED](/tmp/lightfee-root-repair-20260925/ordered-gate-red.log) |
| Gate 与开仓执行器 GREEN | `pytest -q tests/test_gate_live_contract.py tests/test_entry_sync.py --tb=short` | 121 passed，exit 0；[GREEN](/tmp/lightfee-root-repair-20260925/ordered-gate-green-final.log) |
| 相邻恢复／持久化／残仓路径 | 下方六文件命令 | 272 passed，exit 0；[完整日志](/tmp/lightfee-root-repair-20260925/ordered-adjacent.log) |
| 项目完整验证配置 | `.venv/bin/python scripts/validate_change.py --profile full --keep-going` | 10/10 项通过，exit 0；含 compileall、diff check、被动平仓、transport、诊断、close semantics、venue contract、开仓流程、L2；[日志](/tmp/lightfee-root-repair-20260925/validation-profile-ordered.log) |
| 全量测试 | `.venv/bin/python -m pytest -q --tb=short` | **4863 passed / 25 failed / 9 skipped / 1 warning**，63.55 秒，exit 1；[全量日志](/tmp/lightfee-root-repair-20260925/pytest-ordered-final.log) |

表中 `pytest` 命令均通过仓库 `.venv/bin/python -m pytest` 执行。
相邻验证的完整命令为：

```bash
.venv/bin/python -m pytest -q \
  tests/test_gate_live_contract.py \
  tests/test_entry_sync.py \
  tests/live_harness/test_passive_maker_zero_fill_incident.py \
  tests/live_harness/test_residual_repair_incident_replay.py \
  tests/test_recovery_reconciliation.py \
  tests/test_persistence_replay.py --tb=short
```

此前 Gate 方向/单位、双边 truth、终态删除、fallback 和残仓的 RED/GREEN
证据仍保留在 `/tmp/lightfee-root-repair-20260925/`。中间未通过的实验日志也保留；
不能把日志文件名带 `green` 理解为通过，以上表格列出的最终 exit status 为准。
本轮首次修后 Gate 测试因编辑位置错误产生 2 个 `UnboundLocalError`，已修正并
通过最终 121 例、相邻 272 例及全量验证；未将该中间运行包装成通过。

### 当时的 25 项原有失败（后续已逐项处理）

按 pytest FAILED 节点逐一比较：当前 25 项均已在原始 HEAD 基线失败，
与上一轮 25 项集合相同；[机器比较结果](/tmp/lightfee-root-repair-20260925/ordered-validation-comparison.json)。
原始归档基线最初 27 项中，1 项来自归档缺少 Git 元数据，另 1 项是控制面
deadline 时序测试；补齐归档 Git 上下文／重跑后确认 25 项稳定失败。
这些都没有通过修改断言被掩盖。

| 类别 | 数量 | 当前判断 |
|---|---:|---|
| pending close / billing reconciliation 语义 | 14 | 原有契约或 fixture 冲突；本次未完成独立根因审计 |
| snapshot fallback degraded live harness | 2 | 原有失败，保留 |
| 账单 evidence import | 1 | 原有失败，保留 |
| V1 state persistence 的数量／身份证据 | 2 | 原有失败，保留 |
| runtime snapshot freshness | 5 | 原有失败，保留 |
| runtime split architecture | 1 | 原有失败，保留 |

失败测试不是已经证实的线上事故清单；也不能因它们是基线失败而忽略发布门槛。
完整节点名及断言见全量日志。项目的 full profile 通过与整个 pytest 套件失败是
两个不同结论。

### 独立只读审查

先完成全部工作树的独立审查，发现 1 项 P1 与 2 项 P2，全部进入上述修复。
实现冻结后再次独立复核该增量及相邻路径，**未发现新的可证实 P0/P1/P2**。
除本地矩阵外，审查者额外验证：

- Gate 合法零成交 ACK 的 maker/taker、maker/passive、hedge/taker 三条路径；
  订单号保留，查询显示 CANCELED、成交零。
- 真实 finalizer：旧持仓 500 → 新发布 501 → 冲突 recovery 审计 → 部分平仓 1
  → 重复启动 → 新 checkpoint → 最终平仓；无重复扣减、价格覆盖或 owner 复活。
- 变更复用现有解析、执行、truth、持久化边界，没有新增并行状态机。

审查者核对了全量日志，没有另外重跑整个全量套件；25 项基线失败不在本轮
独立根因审计范围。此处通过指代码范围的审查，**不代表生产整改闭环**。

### 逐项对照入口

- Gate 原始方向、数量单位、价格和挂单语义：
  [V1 下单实现](/Users/wl/projects/LightFee/src/live/gate.rs:1790)、
  [V1 合约元数据](/Users/wl/projects/LightFee/src/live/gate.rs:3361)、
  [Gate 官方 REST 文档](https://www.gate.com/docs/developers/apiv4/en/futures/)、
  [V2 合约规则](/Users/wl/projects/LightFeeV2/lightfee/venues/symbol_rules.py:118)。
- 成交终态、ACK 和 WS：
  [V1 状态解释](/Users/wl/projects/LightFee/src/live/gate.rs:3245)、
  [V2 共享执行解析](/Users/wl/projects/LightFeeV2/lightfee/venues/transport.py:1210)、
  [Gate 官方 WS 文档](https://www.gate.com/docs/developers/futures/ws/en/)、
  [真实 HTTP/WS 回归](/Users/wl/projects/LightFeeV2/tests/test_gate_live_contract.py)。
- 清理、删除与持仓交接：
  [统一 bug 契约](/Users/wl/projects/LightFeeV2/docs/bugs/contracts/pending-entry-live-truth-contract.md)、
  [双边实际 truth](/Users/wl/projects/LightFeeV2/lightfee/engine/pending_entry_runtime.py:1523)、
  [交接校验](/Users/wl/projects/LightFeeV2/lightfee/engine/pending_entry_runtime.py:2427)。
- 重启后持仓／残仓：
  [V1 有序重放](/Users/wl/projects/LightFee/src/engine/recovery.rs:2936)、
  [V2 重放](/Users/wl/projects/LightFeeV2/lightfee/engine/recovery.py:932)、
  [残仓实际结果记录](/Users/wl/projects/LightFeeV2/lightfee/engine/residual_repair_runtime.py:52)。
- 日志缺口：
  [生命周期写入埋点](/Users/wl/projects/LightFeeV2/lightfee/engine/close_runtime.py:2708)、
  [风险能力埋点](/Users/wl/projects/LightFeeV2/lightfee/engine/runtime.py:4573)、
  [资金费估算标识](/Users/wl/projects/LightFeeV2/lightfee/engine/close_runtime.py:2799)。

### 交付边界

**Status: implemented but not closed。** 根因、生产路径、修复位置、反例和
审查证据见上表。已完成本地源码和测试修改；原有无关工作树改动保留。
没有 commit、push、部署、撤单、下单、平仓或修改线上状态。

最后只读交易所证据时间为 `2026-09-25T17:46:03.555375Z`，即北京时间
`2026-09-26 01:46:03`：SAGA 多仓仍为 501，空仓为 0，挂单为空。
异常多仓需要用户在交易所手动处理；此报告不能作为已平仓证明。

尚缺：人工平仓后的双边交易所核验；全量失败门槛的处置；用户自行部署后对新版本、
新埋点、实际开平及恢复的生产验证。风险健康、生命周期历史写入者和交易所现金账单
仍有证据缺口，只补日志不推断行为；新增日志未部署，不能保证下一次线上事件已被记录。

## 2026-09-26 全部收口：重新冻结契约

基线仍为 `48e57afd17a535bc040b92cb61160bdf59ae173d`；保留上述无关工作树改动。
本节开启新一轮分析，上面的阶段性结论保留为历史证据。

| 家族 | 已复现机制与契约 | 修复边界 / 反例矩阵 |
|---|---|---|
| Gate WS 依赖 | 13.1 的顶层 connect 不接受 additional_headers；官方自 14.0 切换实现，原 >=12.0 声明不成立 | 提高最低版本、同步锁；真实 localhost 握手与十进制推送，验证 14.0 与锁定 16.0；拒绝 13.1 作为支持环境 |
| 快照准入 7 项 | 日志实际为 entry_leverage_prepare_unsupported；三个旧测试 adapter 缺少 V1 杠杆准备能力，尚未进入快照测试目标路径 | 补测试能力，保留原放行/阻断断言；生产杠杆保护不变，配套真实 adapter 杠杆失败/读回测试 |
| 规则冗余 | get 已提前处理 Gate，_fetch 的 Gate 分支不可达 | 删除该分支；get → metadata → 下单/查询原有矩阵覆盖 |
| 架构门禁 | Bitget ensure_entry_leverage 直接复制三处 settings endpoint，绕过已存在的 operation registry | 在现有 registry 增加 ACCOUNT_SETTINGS 并复用；UTA 读前/写后读回、classic、失败路径及门禁通过 |
| 平仓账务基线 | 数量、订单身份、审计债务和交易阻断对照 immutable owned_close_quantities 合同；独立审查确认 17 项 fixture/断言修正有据 | fixture 补完整双边关闭数量/手续费证据；负数保持整对数量未知，身份缺失先报身份缺口。未知/负数/部分成交的 fail-closed 保留，293 项相邻验证通过 |

GitNexus 刷新工作树后，SymbolRulesCache._fetch 上游直接为 get，影响 14 个流程，
图风险 CRITICAL；实际增量仅删不可达分支。三个 fixture class 与 WS 测试图无上游，
文本搜索确认仅由相应测试使用。原始输出在 `/tmp/lightfee-root-repair-20260925/closure-*`。
只读服务器确认时间 `2026-09-26T07:10:08Z`：部署版本仍为 `339064218db45f68c268e11131e0a49620c9c05d`，
Python 3.12.3、websockets 16.0；进程从 9 月 22 日启动，此结果不证明修复已经上线。

### 本轮诊断契约与路径冻结

当多个生命周期写入者恢复 RUNNING、回滚或保留阻断时，现有日志缺少统一的
写入来源、前后状态和恢复 core 上下文，无法从日志确定 RUNNING 与 critical
并存的具体机制。此前只覆盖平仓对账单个写入者，覆盖不足。本轮只修诊断契约：
在现有 lifecycle 模块共享字段构造，扩充业务分支已有事件；快照恢复在完成
重放与归一化后记录一次摘要。诊断事件不得使用会驱动重放的 runtime.lifecycle_changed。

路径覆盖：启动、快照恢复、启动恢复完成、pending 对账完成、平仓对账、残仓
core 清理、交易所 truth ledger 清理、stale lifecycle 清理、旧阻断清理、
supervisor 风险恢复，以及被动平仓失败后的状态回滚。core 来自当前计算还是
缓存必须明确；没有 core 的写入者明确记录不可用。反例：清理/阻断、前后不变
但仍有阻断、operator fail-closed、回滚恢复、重复重放不得受新增诊断影响。

监督器 venue.health_changed 补齐能力、来源/年龄/缺失和策略取值，覆盖仅有
pending close 的交易所；该阶段的诊断不增加网络请求或修改 unsupported 策略。
后续风险缓存修复补齐采集路径，见下节。诊断矩阵包含
不支持、支持但缺失、陈旧、正常快照，以及 death_line / warning_only / ignore。

资金费状态事件复用既有 paired notional 计算，记录两阶段公式所需输入与增量；
最终/部分对账事件明确资金费仍为估算，不能因订单成交与手续费证据齐全而误认为
现金账单已对齐。矩阵覆盖存储名义金额/数量价格回推、第一/第二阶段、前后边界。

GitNexus upstream：clear_legacy_recovery_block_via_core 的直接调用者为
PassiveCloseExecutor._clear_live_flat_state；后者直接来自 live-flat 检查及其测试，
影响被动开关/推进和失衡处理 3 个流程，风险 HIGH。supervise 调用 health views
与 global risk 更新，图风险 LOW；LiveRuntime 的启动/tick/delegate 动态边界多为
UNKNOWN（部分大文件符号未索引），已通过文本搜索补齐调用链，不把 UNKNOWN 当 LOW。
完整原始输出见 closure-impact-diag-*；新增字段不改变业务分支或恢复决策。

### 风险缓存家族：复审 P1 后重新冻结（未闭环）

独立复审发现 HEAD 已有的生产者/消费者契约断裂：runtime 缓存写入
`(True, AccountRiskSnapshot)`，supervisor 只接受裸 snapshot。两边真实采集健康度
均为 10，仍被解释为缺失并进入 FAIL_CLOSED。此前诊断测试手工构造裸 snapshot，
绕开生产者，属于覆盖缺口。复现脚本及 RED 输出在
`/tmp/lightfee-root-repair-20260925/review-risk-cache-boundary.py` 和
`closure-risk-cache-reproduction-red.json`。4909 项通过不能关闭此 P1。

V1 `src/engine/risk.rs:85-188` 明确由同一个 runtime cache owner 负责 TTL、
Ok(snapshot)/Ok(None)/Err 的解释，supervisor 对所有 supervised venues 通过该
owner 取快照。V2 还存在同族断点：只有 active position tick 发起采集，仅剩
pending close 的交易所虽被监督，却没有采集入口。完整路径为 adapter → runtime
fetch/cache → housekeeping → supervisor health → 全局 risk/lifecycle → journal。

共享边界选择：沿用 `_fetch_venue_risk_snapshot` 作为唯一缓存解释者；housekeeping
为全部 `_supervised_venues` 获取 `AccountRiskSnapshot | None`，再将这个领域映射
传给 supervisor。删除 supervisor 的原始缓存解码路径，不新增缓存框架或第二份
tuple 解包规则。已有 TTL、错误缓存、capability 和 unsupported policy 保持原义；
监督输入每 tick 替换，缺失数据不得沿用上次健康结果。禁用风险监控不发起新请求。

反例矩阵：active / pending-close-only；fresh / stale / None / exception /
capability unsupported；death_line / warning_only / ignore；同 tick 与 TTL 内
复用、TTL 到期重新采集；两边健康 / 一边失败；失败后恢复 / operator 阻断保留；
monitor disabled 无请求。测试必须通过真实 housekeeping → runtime fetch →
supervisor 链路，允许隔离其后的对账、残仓和 WS 激活副作用，不能替换风险链路。

GitNexus 中 fetch/collect 上游为 collect/supervise，图风险 LOW；supervise 的
动态调用和大型 runtime 的 housekeeping 未完整解析（UNKNOWN），文本检索确认
唯一生产调用是 run_forever 的 housekeeping lane，另有直接监督单测。该修改按
实盘风控高风险处理，原始 impact 输出为 `closure-risk-impact-*`。

反例期首次 GREEN 为 32 passed / 1 failed，失败是新测试错误预期“健康恢复后，
仍有 open position 也自动释放 fail-closed”。V1
`crates/lightfee-engine/src/lib.rs:311-318,684` 明确 open position 属于 resume
blocking work。按该权威契约修正新测试：健康恢复但持仓仍在必须保留；持仓清空
且无其他恢复工作才可释放；operator fail-closed 必须继续保留。生产释放逻辑未改。

## 上轮交付与验收记录（已被后续反例推翻，非当前结论）

### Status

当时结论为“源码与离线验收通过；生产整改 implemented but not closed”。该源码
收口判断已被文末的归属、重放与 WS 反例推翻，以下保留历史证据，不作为当前验收。

### 根因、路径与不变量

- Gate 原始订单、REST/WS 执行字段 → 同一合约单位与方向解释 → pending/live truth
  → 持仓或残仓承接 → 原子日志及顺序重放：每一步必须保留实际方向、数量和已接受
  订单身份。未知证据不转为零仓，完整交易所证据与后继 owner 覆盖是删除前提。
- 风险 adapter → 原 runtime cache owner → 全部受监督 venue → supervisor →
  risk/lifecycle：缓存包装不能跨层被重新误解；缺失/错误/过期/能力撤销保持原策略；
  健康结果不会凭缓存格式被误判缺失。没有当前输入不得沿用上次健康结果。
- 多处 lifecycle 写入、失败回滚和 funding 捕获 → 既有事件字段：诊断必须能区分
  writer、前后状态、core 来源与估算来源。新增诊断不得成为新的状态重放指令。

权威依据是文首逐项表及 V1 入口；WebSocket 依赖边界另见
[websockets 官方升级说明](https://websockets.readthedocs.io/en/stable/howto/upgrade.html)。
原来的测试缺口包括：只验 post_only 布尔值、单位乘数固定为 1、只观察预期持仓
方向、只测辅助函数、手工拼造裸风险快照。最终回归使用真实 wire/parser/runtime/
finalizer/replay 路径；交易所边界替身只用于隔离实盘交易。

### 原有 25 项失败如何处理

| 数量 | 确认根因 | 实际改动与未放宽的契约 |
|---:|---|---|
| 7 | 三种测试 adapter 未实现已有杠杆准备能力，测试在快照场景之前被阻断 | fixture 补 supports_entry_leverage_preparation 与对应无副作用方法；原快照放行/阻断断言保留，生产杠杆保护不改 |
| 17 | 旧平仓/账务 fixture 缺完整双边数量或费用证据；两项断言与整对 immutable 数量、身份优先验证契约不符 | 补全测试已有成功场景的真实输入；负数仍使整对数量未知；未提供身份仍拒绝。V1 typed OpenPosition、现有 owned_close_quantities 与 bug 文档支撑变更 |
| 1 | Bitget settings 端点绕过已有 operation registry，违反架构门禁 | 在现有 registry 加 ACCOUNT_SETTINGS；读前/写后读回使用同一 operation。门禁不删不放宽，测试验证两次 GET 的实际路径 |

这 25 项不是 25 起已证实线上事故。此次处置完成了全量发布门槛，未把失败留作
“已知问题”而宣称全量通过，也未批量将安全断言改成接受。

### 最小性和范围复核

- Gate、truth、pending、残仓和重放均沿用已有 owner；没有并行执行器或状态机。
- 删除 SymbolRulesCache._fetch 中已被 get 提前处理的 Gate 不可达分支；风险修复
  删除 supervisor 的重复缓存解码，缓存仍归 runtime 一个 owner。
- lifecycle 两个字段构造函数被多个现有写入者使用，只有诊断责任，不承载决策。
- websockets 最低版本与部署前检查直接服务 Gate 小数数量协商；生成器是持久入口，
  本地忽略的生成脚本已同步。Bitget 小改动用于关闭实际架构门禁失败。
- 保留预存 `.deploy_manifest.json`、四个 egg-info 文件、`.dev-flow/` 和异常文件名
  的无关改动，未把它们作为本轮修复。没有修改 Codex 权限、审批或沙箱配置。
- 全部源码/测试/依赖的 46 文件冻结清单在
  [closure-risk-source-checksums.json](/tmp/lightfee-root-repair-20260925/closure-risk-source-checksums.json)。
  最后一轮复审前后 SHA256 完全一致，复审期间没有修改代码或文档；复审完成后仅
  整理此报告。

### 上轮验证证据（2026-09-26 最终复查发现遗漏，不能作为当前闭环结论）

| 层级 | 实际命令与范围 | 最终结果 |
|---|---|---|
| 风险真实链路 RED | `.venv/bin/python -m pytest -q tests/test_supervisor_execution.py -k 'diagnoses_runtime_risk or reuses_runtime_cache or disabled_does_not_fetch'` | 30 failed / 3 passed / 29 deselected，exit 1；[RED](/tmp/lightfee-root-repair-20260925/closure-risk-red.log) |
| 风险 GREEN 与相邻路径 | `.venv/bin/python -m pytest -q tests/test_supervisor_execution.py tests/test_venues_transport.py tests/test_parity_contract_h7_m16.py tests/test_runtime_entry_flow.py` | 657 passed，exit 0；[日志](/tmp/lightfee-root-repair-20260925/closure-risk-green-adjacent.log) |
| 生命周期/funding/恢复相邻验证 | supervisor、runtime entry、pending、passive、startup、engine recovery、recovery 目录及 residual harness | 719 passed，exit 0；[日志](/tmp/lightfee-root-repair-20260925/closure-diagnostics-adjacent-final.log)。发生在最后风险增量之前，随后由最终全量覆盖 |
| 最终全量 | `.venv/bin/python -m pytest -q` | **4932 passed / 9 skipped / 1 warning，64.44 秒，exit 0**；[日志](/tmp/lightfee-root-repair-20260925/closure-full-risk-final.log) |
| 完整项目验证配置 | `.venv/bin/python scripts/validate_change.py --profile full --keep-going` | **10/10 passed，exit 0**；[日志](/tmp/lightfee-root-repair-20260925/closure-profile-risk-final.log)，逐步命令和耗时保留在日志内 |
| 静态与依赖 | `git diff --check`、compileall、`uv lock --check --offline`、`bash -n scripts/deploy.sh` | 全部 exit 0；最终 full profile 同时覆盖完整 lightfee/tests/scripts 编译及 diff check |
| WS 最低依赖 | 真实 websockets 13.1 / 14.0 / 锁定 16.0 的 localhost 协商测试 | 13.1 复现不兼容；14.0 和 16.0 通过；[13.1 RED](/tmp/lightfee-root-repair-20260925/closure-ws13-red.log)、[14.0 GREEN](/tmp/lightfee-root-repair-20260925/closure-ws14-green.log) |
| 部署前拒绝不兼容依赖 | 独立复审通过假 SSH 执行实际生成脚本 | 13.1 在任何同步前退出；16.0 能进入下一阶段；后续步骤由替身主动截停，没有部署。[证据](/tmp/lightfee-root-repair-20260925/review-deploy-failstop.json) |
| GitNexus | index-only/drop-embeddings → status → detect_changes（scope all） | 447 个覆盖文件内容一致、HEAD 一致。diff/search 补充动态委托和未索引大文件；总风险 critical，图的流程计数不作为精准业务影响计数。[status](/tmp/lightfee-root-repair-20260925/closure-index-risk-final-status.log)、[changes](/tmp/lightfee-root-repair-20260925/closure-detect-changes-risk-final.log) |

9 项跳过来自 opt-in 线上 probes/smoke（`LIGHTFEE_RUN_LIVE_PROBES` 与
`LIGHTFEE_LIVE_SMOKE` 未启用），不是 9 项交易所验证通过。1 项 warning 是既有
`TestnetSupport` Enum 被 pytest 当作候选测试类的收集提示。本地验证环境 Python
3.14.5；只读生产环境 Python 3.12.3、websockets 16.0。没有将生产版本和本地环境
当作完全相同的验证环境。

### 上轮独立只读复审（结论已被后续三项反例推翻）

完整工作树审查及随后风险增量复审均已完成。首次风险增量前发现的既存 P1 已按
新契约重新修复；最后一轮结论：**没有未解决的 P0/P1/P2，未发现阻塞交付的过度
设计或无关源码变更。** 审查者核对 46 个文件冻结值与全量/profile 日志，并自行
运行真实 producer → housekeeping → supervisor 反例：

- 健康缓存不再误触发 fail-closed；
- Aster 30 秒 TTL 包含边界且到期重新采集；
- capability 撤销后不沿用仍然健康的旧缓存；
- 字典格式 pending-close-only 正常采集；
- snapshot.supported=False 和 stale=True 仍阻断。

[独立脚本](/tmp/lightfee-root-repair-20260925/review-risk-final-counterexamples.py)
与[结果](/tmp/lightfee-root-repair-20260925/review-risk-final-counterexamples.json)。
最后复审没有修改仓库，也没有访问生产；生产证据由主任务另行只读取得。

### 上轮生产验证和剩余条件

当时只读证据为 `2026-09-26T07:53:44.389777Z`（北京时间 15:53:44）。请求在独立
探针进程内限制为 GET，只查询 Gate/Binance 的 SAGA 仓位和挂单；没有调用交易
接口、重启服务或改写运行状态。Gate 多仓 501、Binance 0、双方挂单 0，部署仍为
旧版本，详见文首和[JSON 证据](/tmp/lightfee-root-repair-20260925/closure-production-risk-final.json)。

| 剩余条件 | 当前状态与验收标准 |
|---|---|
| 异常多仓 | 仍有 Gate 501，尚未完成处置。需用户在交易所处理，再取双边仓位和挂单证据；不能仅凭本地任务删除认定已平 |
| 代码部署与启用 | 未 commit/push/部署/重启。实盘平仓及启用自动交易由用户执行；部署后需核验实际版本、依赖、进程与新日志字段 |
| 实际开仓、平仓、恢复 | 离线真实路径已通过，修后版本尚无生产运行证据；需要新版本的订单意图/原始方向单位/成交、双边仓位、残仓承接及重启恢复证据 |
| Gate 风险能力 | supports_risk_health=False 与 V1 一致；不推测账户保证金公式。仍需账户模式及原始风险字段证明可支持的算法，埋点已完成但未上线 |
| 历史 lifecycle 写入者 | 当时日志不足，不能倒推为某个分支已被证实。现在补齐各 writer/core/回滚证据，下次事件才能判定实际机制 |
| 资金费现金账单 | 仍为入场 edge 模型估算；新日志标识公式和未对账状态。未取得交易所账单核对前，不认定收益及 funding 准确 |

因此，这次完成的是已证实代码问题的根修、全量测试门槛、诊断契约与独立审查。
异常仓处理、部署生效和实盘结果仍是未完成项，不宣称“实盘全部闭环”。

## 2026-09-26 最终复查后的根因重置与修复契约

状态：**未闭环，重新修复中**。上轮本地闭环结论撤回。新增 P1 触发 repair-loop
circuit breaker；冻结并保留原改动和失败复现，先完成以下契约和反例，再实现。

| 机制与权威契约 | 完整生产路径与唯一归属边界 | 必须覆盖的反例 |
|---|---|---|
| P1：同 venue/symbol 被直接视为 owned，账本按 owner 去重又丢失第二条持仓，core 还有两条弱匹配；违反 V1 recovery.rs 的真实方向、数量与本地承接契约 | Gate account REST → normalized positions → RecoveryOwnerIndex → RecoveryLedger → core → lifecycle/entry gate/diagnostics。OwnerIndex 统一方向和数量覆盖；账本保留每条证据；core 不再以同币种或 owned 前缀替代覆盖证明。cleanup 的竞争 owner 检查必须保护部分持仓，不能把“不足以证明整仓归属”等同“无人拥有任何部分” | 正常双腿放行；额外反向仓/同向超量/本地零量/未知方向阻断；多 owner 数量聚合；open+pending/residual；重复与顺序；账户与单币种分支；完整证据释放、缺失证据保留；部分竞争 owner 不得被清理 |
| P2：entry.opened 两个 producer 手写字段子集，丢失 fee evidence/notional 等，重启后的同笔平仓账务改变 | ordinary/terminal fallback EntrySyncExecutor 与 pending finalize → critical entry.opened → checkpoint/journal replay → OpenPosition → close accounting。复用现有 recovery._serialize_open_position，保留事件身份和原子 residual 事实；legacy 缺失 evidence 仍保守 false | 有费用/已证明零费用/未知费用；普通与 fallback/pending producer；事件前快照、事件后崩溃；完整持仓与账务等价；legacy 不升级为已对账 |
| P2：Gate WS 把 connect/任意 JSON（包括 error ack）当作健康，订阅拒绝未累计失败 | real websocket → two subscription responses → existing ConnectionHealth → supervisor。仅两频道明确 success 后清失败计数；拒绝带 channel/code/message，关闭连接并退避；无 ack 有界超时；PONG/重连/单频道 ack 不清失败 | result=null/error；status=fail；一个成功另一个拒绝；全成功；无/半 ack 超时；PONG/重复 ack 不替代第二频道；连续重连达到 unhealthy；后续完整确认恢复；正常订单/仓位推送继续归一化 |

Gate WS 协议权威：[官方 response 文档](https://www.gate.com/docs/developers/futures/ws/en/#response)
要求检查 error 与 result.status；不能用 TCP/WS 建连代表认证订阅成功。

已有三个独立实际路径反例保留于 `/tmp/lightfee-root-repair-20260925/` 的
`final-review-dual-position-owner-confirmed.json`、
`final-review-fallback-fee-replay-confirmed.json`、`final-review-gate-ws-error-ack.json`。
现有测试遗漏跨 account API 的第二条持仓、完整 journal 重放到 close accounting、
以及 worker 收到拒绝帧后的健康状态；后续必须补真实路径 RED/GREEN。

实现前/矩阵展开时新增的同边界证据：完整重放比较证明 `_deserialize_order_fill`
调用不存在的 Side.from_str，所有 sell 都恢复为 buy；该函数直接由持仓、被动平仓
成交与 pending-close 恢复消费，需使用现有 Side 枚举解析并覆盖双方向。异常仓已
建立 latch 后，下一次缺失/子集 truth 使 core.entry_allowed=True 但 clear=False；
统一 gate 输出必须保持阻断，直到既有完整 account release predicate 成立。
GitNexus：decide 与 ledger 为 CRITICAL，fill 恢复图风险 LOW（持久化事实影响仍需回归），按真实
上下游补测。旧测试若允许存在 account latch 的 symbol-only proof 放行，属于弱
契约保存，按完整 account 证明更新预期。

ownership 的阶段去重规则：同 entry 已有 open successor 时 pending 的成交不再
相加；entry_open 残量在 balanced open 之外；close/live_recovery 残量若有同一 open
owner 则已包含于其剩余腿；所有 residual side 都是平仓方向，覆盖时反转。关联但
未知数量的 claim 仍保护 cleanup；完整归属与部分竞争不能混用同一个 confidence。
WS 的 keepalive 同时按官方 futures.ping JSON 协议发送，避免新的 error 检查把旧
纯文本 ping 的协议错误当作不可解释的健康失败；不增加配置或连接框架。

## 2026-09-26 独立复核后的第二次契约重置

状态：未闭环。只读审查完成后统一修复，禁止把全量两个失败简单改断言消除。

- 根因：归属只遍历 observed，漏掉同 owner 仍应存续的另一腿；未知 venue/side/maker leg
  被早退或分 key 掩盖。完整 account 才能推导缺行=0，OwnerIndex 统一输出缺腿证据，
  ledger/core 共用；同 owner 两腿皆零保持 V1 recovery.rs:1167 的 flat 退休。
- 根因：运行时 current-state producer 仅导出 matched_quantity，丢掉每腿事实；必须在
  原 producer 补 long_quantity/short_quantity，不允许 consumer 用 matched 反造双腿。
- 根因：core/ledger/lifecycle table 有三份 exchange collection flatten，真实 row、
  nested venue/symbol、error envelope 被不同解释。统一使用 core 的现有解析边界，
  错误 envelope 保留 evidence-gap，不合成仓位；真实 malformed row 仍阻断；混合证据
  保留成功 venue 的异常仓/订单。
- 根因：WS ACK 与有效数据消费共同清零同一个失败序列；即使坏 update 抛错，每次
  重连双 ACK 仍能掩盖连续归一化失败。worker 局部保留待恢复的失败频道，订阅失败由
  双 ACK 解除，数据失败只由对应频道有效 update 解除，不增加健康框架。

完整路径：实际 account REST → collector → 唯一归属/集合解释 → ledger/core →
entry/lifecycle；EngineState → current-state export → diagnose/health/table 走同一契约；
Gate 双订阅 → ack/update 验证 → parser → ConnectionHealth → supervisor。

新增矩阵：首次缺长/缺短与旧锁、双边皆平、局部/错误证据；混合未知 scope；真实导出
再读取两个 alias；legacy quantity-only；裸错误/嵌套错误/真实坏 row/混合成功错误；
WS null/string/非 mapping row，连续失败达到 unhealthy，错误频道未恢复时 ACK、PONG、
另一个频道、未知合约均不得清计数，正确频道有效 update 后恢复。

GitNexus：account flat predicate、ledger、table build 为 CRITICAL；collection 解析、
local claim 索引为 HIGH；export/worker 图风险 LOW。实际动态调用以文本检索补足。
审查证据：review-reset-missing-leg、review-reset-ws-malformed-update、
review-reset-ws-repeated-normalization 的脚本/JSON；全量失败日志 closure-reset-full.log。

### 矩阵展开后的同边界补充

- V1 `src/engine/recovery.rs:1167/1185/1229`：双边皆平退休、等量反向双腿正常、
  已有 residual 可承接例外、其余不等量进入 mismatch。真实 Gate REST → runtime
  account collector 证明本地 0/501、501/300 且无恢复任务仍误放行（4 个 RED）。
  OwnerIndex 现在同时校验本地 pair；pending/residual/passive-close 明确承接的
  不等量仓继续归恢复工作管理，未知/部分声明仍保护 cleanup，不成为整仓安全证明。
- table 把缺少 open_orders 的证据变成 `[]`，违反完整性判定；归一化现在保留
  missing/None。此项由新增 coverage 矩阵真实调用 table 复现，没有更改错误断言。
- ledger 直接依赖 DecisionCore 被已有架构门禁拒绝；将共用 availability/gap/account
  判定放到现有模块的纯函数，保留原 core API，ledger 不调用决策类；没有新模块或框架。

## 2026-09-26 17:12 后的冻结验收

Status：**implemented but not closed**。源码已完成本轮修复和全部本地测试；独立
只读复审待结论。生产仍为旧版、有异常仓，新日志尚未上线。

### 当前契约、完整路径和最小性

- 账户仓位必须保留每条 venue/symbol/side 的真实数量；只能由完整且不重复的
  owner 数量及当前阶段证明归属。原账本丢行、core 两个弱匹配分支均已删除。
- 正常双腿、受管理的不等量仓、未承接的单腿、双方已平是不同终态；缺失接口证据
  不能被解释为平仓或释放旧阻断。ledger 是证据收集器，决策仍归 core。
- producer → journal → replay → close accounting 保留完整字段，不再复制持仓
  序列化字段清单；本轮同时修复原 SELL 恢复错误。
- WS 建连 → 双 ACK → update 解析 → ConnectionHealth → supervisor 同一健康路径，
  一个 worker 局部集合记录待恢复的失败频道，没有增加全局健康状态机或配置。
- 新 helper 均服务现有消费者：missing claims 给 ledger/core；集合解析给
  core/ledger/table；证据判定给同三处。没有平行交易执行器。
- 保留文首记录的预存无关文件改动。本轮 Bitget 的 ACCOUNT_SETTINGS 调用只用于
  修复已复现的 registry 架构门禁，未扩展其交易语义。

### 当前反例矩阵和验证命令

| 门槛 | 命令或真实路径 | 结果及证据 |
|---|---|---|
| 原三个缺口 RED | Gate account recovery、完整 entry.opened replay、真实 WS worker | `closure-three-red.log`：17 failed；对应 `closure-three-green.log`：17 passed |
| 首次缺腿/真实导出 RED | actual Gate REST + runtime collector；EngineState 实际 export 再 diagnose | `closure-reset2-red.log`，随后 `closure-reset3-green.log`：408 passed |
| 本地无承接的不等量 RED/GREEN | 同一 actual Gate/runtime 测试 local_one_leg/local_imbalance，正逆输入顺序 | `closure-reset3-pair-red.log`：4 failed、33 passed；`closure-reset3-pair-green.log`：308 passed |
| 扩展反例 | `.venv/bin/python -m pytest -q tests/engine/test_v1_lifecycle_closure_table.py tests/test_gate_live_contract.py tests/engine/test_recovery_owner_index.py` | 242 passed、exit 0；`closure-reset3-matrix-green.log` |
| 全量 | `.venv/bin/python -m pytest -q` | **5038 passed、9 skipped、1 warning，exit 0，66.32 秒**；`closure-reset3-full.log` |
| 仓库 profile | `.venv/bin/python scripts/validate_change.py --profile full --keep-going` | **10/10 passed、exit 0**；`closure-reset3-profile.log`，包含 compileall 与 diff check |
| 依赖/脚本 | `uv lock --check --offline`；`bash -n scripts/deploy.sh`；`git diff --check` | exit 0 |
| 图与范围 | 重新 index-only/drop-embeddings 后 status/list，再 `npx gitnexus detect_changes --repo LightFeeV2 --scope all --limit 1000` | HEAD `48e57af`，447 covered files 匹配；62 tracked changed files / 172 symbols / 527 flows，CRITICAL；另外纳入未跟踪测试。图存在动态边/截断限制，结合全量 diff 审查 |
| 生产只读 | 独立 Python 进程的请求包装器拒绝非 GET；Gate/Binance 仓位及挂单 | 17:11:49 北京时间，Gate long501/short0，Binance0，双方挂单0；`closure-reset3-production.json` |

所有日志位于 `/tmp/lightfee-root-repair-20260925/`。9 skipped 是未授权启用的实盘
probe，不算验证通过；warning 是既有 TestnetSupport 枚举的 pytest 收集提示。
没有中断/超时的用例计入当前全量结果。冻结源码/测试/依赖为 55 个文件，清单与
SHA256：`closure-reset3-source-freeze.json`。

新增矩阵涵盖：正常/额外反向/超量/重复/多个 owner/未知数量和 scope，双方皆平与
首次缺长缺短，旧锁保持与完整证明释放，open/pending/residual 阶段去重，实际
current-state 导出及旧 quantity-only，裸/venue/symbol/list error 与成功异常行混合，
缺集合不合成空集合，以及 WS null/string/坏行连续重连、正确频道恢复。

### 独立审查与生产剩余条件

只读审查正在检查上述冻结树，待写入结论。最终源码结论必须同时满足复审与哈希
一致；测试通过本身不代表生产生效。

生产异常仓、实际部署和新版本实盘开平仓/重启证据均未完成。用户需在交易所处置
异常仓；实盘订单和启用自动交易不由本次工具执行。当前没有提交、推送、部署或
重启服务。证据不足的历史 lifecycle writer、Gate 风险算法、资金费现金账单仍按
前文保留诊断和验收条件，不把新增埋点视作这些问题已被证实根修。

## 2026-09-26 第三次契约重置：集合存在性与有效消费

状态：未闭环，冻结版本独立复核已发现以下反例，按修复循环熔断重新分析同族。

- P1：Gate 活跃卖单数量为负，账本的 quantity > 0 过滤将它丢弃；零、缺失、NaN
  也会丢弃。core 的 reduce-only 早退使其不能兜底。Gate 官方 FuturesOrder 规定
  size 正买负卖、close/auto_size 可以为 0；存在性不依赖是否可执行或数量为正。
- P1：collector 中任一订单数值转换失败会丢掉整批；Gate 仓位批校验也会让一条
  坏行抹除同批已观察到的反向异常仓。纯 HTTP/超时缺证与已收到但不能安全归一的
  行不能等价：后者必须保留原始行、已解析成功行和错误，禁止恢复为 RUNNING 放行。
- P2：WS dual 仓位分支提前忽略消息，worker 却因已加载 metadata 把它当有效
  消费并清除此前 positions 失败。忽略的数据不能证明坏数据流已经恢复。

权威来源：[Gate FuturesOrder 官方文档](https://www.gate.com/docs/developers/apiv4/en/futures/)，
V1 `src/live/gate.rs` 的原始 open-orders 与带符号 size；已有
`probe_venue_open_orders_flat` 对非空集合的存在性契约、REST 权威的双向仓位处理。

完整路径：Gate HTTP → transport/shared open-order parser → account/symbol collector
→ ledger/core/table → 阻断与释放；WebSocket ACK/update → parser 的实际消费结果
→ ConnectionHealth。保留 adapter 失败语义，不把坏行作为成功 PositionSnapshot
提供给订单消费者；复用既有异常/证据路径保存部分结果，不建立第二套交易执行器。

实施前反例矩阵：

| 维度 | 必须验证 |
|---|---|
| 活单数量 | 正/负/0/missing/None/NaN/Infinity/非法字符串；price 非法不得抹除订单 |
| 订单类型 | maker/reduce_only/is_reduce_only；有 owner、无 owner、同 ID 异 venue/symbol |
| 集合 | 成功行+坏数值/缺身份/非 mapping；调换行顺序；坏仓位+已知反向仓 |
| 消费路径 | 真实 account/symbol REST collector；ledger、direct core、lifecycle table |
| 阻断生命周期 | 初次异常就阻断；旧锁在部分/失败证据下保持；完整空单/有效平衡仓释放 |
| 请求结果 | 空集合正常；请求错误/超时仍为缺证；格式错误不得伪造空集合 |
| WS 恢复 | dual 通过完整校验后才可恢复；unknown/混合忽略不清失败；坏数据继续失败；有效对应频道 update 恢复 |

审查复现：`review-reset3-gate-live-order-matrix.py/.json`、
`review-reset3-mixed-position-evidence.py/.json`、`review-reset3-ws-dual-healing.py/.json`，
均位于 `/tmp/lightfee-root-repair-20260925/`，只使用本地 MockTransport 或 localhost WS。

## 2026-09-26 第三次重置后的实现与冻结验证

Status：**implemented but not closed**。代码、验证和冻结源码的独立只读复核均已
完成；生产仍有异常仓、未部署新代码。

### 根因、共享边界及最小性

当返回集合中有有效活跃事实，同时存在不可解析的行、字段或后续元数据失败时，
旧逻辑将整个集合转换为缺证甚至空事实，导致恢复决策丢失更强的异常证据。数量
正负和 reduce-only 又在三个消费者分别充当错误的“是否存在”判断。此前测试
主要覆盖全部有效或整个请求失败，遗漏混合结果以及拿到仓位后的元数据超时。

- open-order 原解析主体抽为私有证据解析，strict public parser 仍返回 None/error；
  require API 对部分坏行抛既有 TransportError 并携带 truth_evidence。无已观察行的
  响应错误继续使用原 RuntimeError，保留 Bitget recovery/residual 兼容契约。
- Gate account/symbol 仓位共享 fetch_all_positions 的严格归一化路径。每条原始行
  先保留，成功时替换为归一化证据，任何失败都不返回成功 PositionSnapshot。
  symbol 请求遇双方向仍严格拒绝。生产订单消费者不能把部分证据当可交易仓位。
- 元数据超时的 RED 为 `closure-reset4-timeout-red.log`：已观察 Gate 501 和反向 20，
  但收集结果没有任何 Gate 行。取消异常的标准 cause 链现在保留该证据，account
  collector 仍记录 timeout；不屏蔽取消，也不把超时升级为成功。
- account/symbol collector 保留部分行与错误；ledger/core/table 对活单统一采用
  存在性。table 删除其重复 symbol helper，复用账本的合约名归一化。
- WS 原 handler 返回真实验证结果，worker 据此解除失败频道。合法 dual 只证明
  数据流恢复，REST 仍是双向持仓真相；不因双向模式永久报告不健康。
- 没有新增策略、执行框架、状态机、运行时配置或模块。新增异常字段由 transport
  和共享 parser 生产，由既有 collector 消费；取消链 helper 服务 timeout/exception
  两个现有分支。

### 全量失败的调查和修正

首轮 `closure-reset4-full.log` 为 11 failed / 5121 passed / 9 skipped，未算通过：

1. 8 项混合订单测试只在全量失败：`tests/test_passive_close.py` 在模块导入时直接
   给 VenueAdapter 写入默认空挂单方法，使其他模块绕过真实 HTTP。现使用该模块
   的 autouse monkeypatch fixture，测试结束恢复，消除全量污染。
2. 1 项元数据超时回归的后续释放失败：真实取消路径会淘汰 HTTP client，测试只
   给首个 client 配了 MockTransport。测试现在固定 HTTP client factory 的传输层，
   新 client 继续使用同一离线响应；未替换待修复方法或放宽释放断言。
3. Gate 仓位 fixture 原为单对象；实际调用的是
   [官方账户 positions 列表接口](https://www.gate.com/docs/developers/apiv4/en/futures/#get-user-position-list)，
   按其 array schema 修正样本，生产严格集合校验保留。
4. Bitget 无行错误恢复原 RuntimeError 类型，不将新的部分证据机制扩展为无关
   异常契约变更。已有 recovery/residual 双路径用例保持原断言。

另外，旧 preflight 测试中的具体坏行不再等价于“没有收到事实”：有原始行时应为
BLOCK_OR_FLATTEN_LIVE_ARTIFACT；只有未收到可识别集合时才保持 RISK_ONLY_WAIT_FOR_TRUTH。
这三个断言按更强证据契约调整，验证原始行仍在，而非删除用例。

### 当前验证证据

所有日志位于 `/tmp/lightfee-root-repair-20260925/`。当前源码/测试/依赖共 55 文件，
SHA256 清单 `closure-reset4-final-source-freeze.json`；相对上次冻结 11 文件发生变化。

| 门槛 | 命令或范围 | 结果 |
|---|---|---|
| 第三次重置 RED | 真实 HTTP/WS 新矩阵；`closure-reset4-red.log` | 91 failed / 2 passed；未实现契约的失败证据，另有独立复现脚本 |
| 扩展矩阵 GREEN | `pytest -q tests/test_gate_live_contract.py -k 'recovery_open_order_presence or recovery_mixed_rows or recovery_metadata_timeout or worker_health_requires_validated'` | 94 passed / 137 deselected，exit 0；`closure-reset4-final-matrix.log` |
| 相邻链路 | Gate、passive close、runtime architecture、venue contract、startup、recovery core/ledger/owner/table、WS 共 11 文件 | 1043 passed，exit 0，20.91 秒；`closure-reset4-adjacent-complete.log` |
| 全量 | `.venv/bin/python -m pytest -q` | **5132 passed / 9 skipped / 1 warning，exit 0，64.82 秒**；`closure-reset4-full-complete.log` |
| 仓库 profile | `.venv/bin/python scripts/validate_change.py --profile full --keep-going` | **10/10 passed，exit 0**；`closure-reset4-profile-complete.log`，含 compileall、diff check |
| 依赖与 shell | `uv lock --check --offline`、`bash -n scripts/deploy.sh` | exit 0 |
| Python 3.12 异步边界 | `/tmp/lightfee-root-repair-20260925/python312/bin/python -m pytest -q tests/test_gate_live_contract.py -k 'recovery_metadata_timeout or worker_health_requires_validated'` | 6 passed / 225 deselected，exit 0；`closure-reset4-python312-async.log`。3.12.13 隔离环境、锁定依赖；专门验证生产 3.12 系列与本地 3.14 的取消/WS 路径差异，非生产执行 |
| GitNexus | index-only/drop-embeddings 重建；status/list；detect_changes scope all | HEAD 48e57af，447 covered files 匹配；62 tracked files / 282 symbols / 136 flows，CRITICAL；结合 diff 与未跟踪测试审查 |
| 生产只读 | 独立 GET-only 进程查询 Gate/Binance SAGA 仓位及挂单 | 17:46:41 北京时间：Gate long501/short0、Binance0、挂单0；`closure-reset4-production.json` |

9 skipped 为未启用的实盘探针；warning 为既有 TestnetSupport 收集提示。不把跳过、
历史失败或未经部署的新增埋点计为生产验证成功。生产开平仓、异常仓清除、新日志
在线产出仍没有完成证明。

### 最终独立只读审查

结论：**本轮代码审查通过，未发现新的可证实 P0/P1/P2；上轮 2 项 P1 和 1 项 P2
在本地实现与反例验证中关闭。** 审查结合上轮完整树复核，逐一检查本轮 11 个变化
文件及相邻消费者；审查过程未修改实现，55 文件哈希与送审清单一致。

- 独立新增 16 个真实 HTTP/collector/取消/释放反例，全部通过：
  `review-reset4-independent.py/.json`。包含零 auto-close、坏身份/非字典行在前、
  嵌套坏数值、数值溢出、首次 metadata 即超时、有无旧锁、跨 scope 同 ID、直接取消。
  account/symbol → runtime/direct core/table 均正确阻断，完整新证据可释放；
  CancelledError 仍向上传播并携带已观察行。
- 独立新增 7 个 localhost WS worker 反例，全部通过：
  `review-reset4-ws-independent.py/.json`。坏数量、坏时间戳、未知合约混合及重复
  single 行不得解除此前错误；合法 dual 可恢复健康且不写单向缓存。
- 实证确认导入 `tests.test_passive_close` 不再修改全局 VenueAdapter。没有发现
  为通过测试而不当放宽断言；部分 truth_evidence 未成为成功交易结果。
- 跨 scope 同 ID 未复现安全放行或错误清理，不以静态猜测扩大重构。现有异常和
  解析边界均有当前消费者，未发现无关源码修改或必须撤销的过度设计。
- 已核对全量/profile 日志；独立审查没有自行重跑全量，没有用已有测试替代新反例。

### 完成门槛与交接

- [x] 原故障及新增同族反例有 RED、交易所文档和 V1 契约依据。
- [x] 共享根因、完整生产调用路径、调用者/影响范围与重复规则已分析。
- [x] 保持/释放、成功/失败、全部/部分证据、重启/终态矩阵及实际调用链已验证。
- [x] 相邻测试、full profile、compile/import、diff hygiene、完整 pytest 已通过。
- [x] GitNexus 重建、scope 检查及冻结文件完整性校验完成。
- [x] 独立只读复审没有未解决的已证实 P0/P1/P2；未发现新增无关源码或过度设计。
- [x] 生产 GET-only 取证及旧版本、异常仓仍存在的限制已记录。
- [ ] 异常 SAGA 多仓已由用户在交易所处置，并取得成交/仓位/挂单证明。
- [ ] 新代码和新增诊断已上线，并取得生产版本、启动恢复及实际开平仓证据。
- [ ] 资金费现金账单已完成核验；证据不足的历史 lifecycle writer/Gate 风险字段
      已在下次事件中取得足够原始证据。当前仅完成埋点与未对账标记。

交接状态：**implemented but not closed（本地验收通过，生产未闭环）**。
本轮未提交、推送、部署、重启或发出实盘订单。原有无关工作树改动保持原样。
实盘交易及启用自动交易不由本次工具执行；未完成项没有被写成“修复已线上生效”。

## 2026-09-27：WS 恢复证据与订单身份契约重置（本地代码收口，生产未闭环）

最终只读审核再次发现同族释放旁路，触发 repair-loop circuit breaker。
此前本地“无遗留 P2”的结论撤回；冻结当前树并重建契约后再实施。

根因：两个 Gate WS 解析器把“遍历没有抛错”当作恢复证据，空集合默认成功；
订单身份先 str() 导致 null/布尔/复合值伪造缓存键，普通 text 来源标签也被
当作 CID。后者已复现两个 id 不同、text=web 的订单合并进同一缓存。
旧测试甚至用空列表证明故障恢复，属于保留漏洞的错误预期，必须以有效订单替换。

契约与最小改动边界：

- 非空、所有行可识别且完整校验的数据，才可解除对应频道已有归一化故障。
- 空集合/未知合约不构成恢复证明；坏身份或坏数值进入既有异常/退避路径。
- 订单 ID 保持现有 domain 的 opaque str/正整数契约，先检查类型再转换；
  bool/复合类型不能成为 ID，空 ID 不构成身份。
- text 仅 t- 开头且后缀非空白才可成为 CID；web/api/app 等来源标签不是 CID。
  有效 ID 可独立成立，有效自定义 CID 也可独立成立；无身份不能恢复健康。
- 合法零成交终态、零持仓、完整 dual 行继续可以恢复；dual 不写单向缓存。
- 保留两频道 ACK 门槛、跨重连故障、同频道恢复与 REST 交易真相权威。

完整路径：socket → Gate worker → 两个现有解析器 → PrivateWsState →
awaiting_valid_updates → ConnectionHealth → supervisor → venue risk view。
只修改既有 Gate 解析器、已有回归文件和本记录，不新增运行时框架或第二套实现。

实施前反例矩阵：空 orders/positions；missing/null/blank/invalid identity；
CID-only/ID-only；合法 ID + 同来源标签多订单；有效/坏身份或未知行的前后混合；
非对应频道/重复 ACK；合法零成交/零持仓/single/dual；无效之后有效证据恢复。
真实 worker 回归必须到 supervisor 的 private_stream_unhealthy 风险原因。

GitNexus 已重建并核对 HEAD 48e57af，447 covered files 一致。两解析器各有
直接调用者 handle_gate_private_message，上游是 _gate_private_ws_loop /
start_gate_private_ws / VenueTransport._start_gate_private_ws；图上 LOW，
实际为交易健康保护路径，使用完整回归与独立审查。冻结/impact/旧 RED 证据在
`/tmp/lightfee-root-repair-20260925/ws-closure-*` 与 `final-audit-ws-boundaries.*`。

参考：[Gate Orders notification](https://www.gate.com/docs/developers/futures/ws/en/#orders-notification)
及 V1 Gate parser/PrivateWsState。V1 身份与缓存仍保持兼容；不恢复其“任意消息即健康”的旧缺陷。

### 本轮分层验证（当前冻结源码）

证据目录 `/tmp/lightfee-root-repair-20260925/`；送审冻结清单
`ws-closure-review-freeze.json` 包含当前 55 个源码/测试/依赖文件。
本轮相对 `ws-closure-before.json` 的 89 个既有脏/未跟踪路径，只修改本记录、
`lightfee/venues/gate_private_ws.py`、`tests/test_gate_live_contract.py`。
生产实现只改两个解析器，净增 10 行；没有新增 helper、配置或运行时文件。

| 门槛 | 命令或实际路径 | 结果与日志 |
|---|---|---|
| RED | `.venv/bin/python -m pytest -q tests/test_gate_live_contract.py -k 'worker_health_requires_validated_update or worker_data_failure_survives or worker_requires_both_acks' --tb=short`，旧生产实现配新契约测试 | **24 failed / 20 passed / 217 deselected，exit 1**；`ws-closure-red-final.log`。此前初次 RED 混入 unsupported risk snapshot 的测试配置干扰，已隔离；不使用该次结果作为根因证据 |
| GREEN | 同一命令和 44 项测试，新生产实现 | **44 passed / 217 deselected，exit 0**；`ws-closure-green.log` |
| 当前相邻回归 | `.venv/bin/python -m pytest -q tests/test_gate_live_contract.py tests/test_venue_private_ws_parsers.py tests/test_v1_private_ws_parity.py tests/test_supervisor_execution.py tests/test_runtime_snapshot_freshness.py --tb=short` | **500 passed，exit 0，9.80 秒**；`ws-closure-adjacent.log`。包含随后补充的“双频道均故障，修复一个频道仍不能释放”反例 |
| Python 3.12 定向 | `PYTHONPATH=/Users/wl/projects/LightFeeV2 /tmp/lightfee-root-repair-20260925/python312/bin/python -m pytest -q tests/test_gate_live_contract.py -k 'worker_health_requires_validated_update or worker_data_failure_survives or worker_requires_both_acks' --tb=short` | **45 passed / 217 deselected，exit 0，2.46 秒**；`ws-closure-python312.log`，Python 3.12.13 隔离环境；生产为 3.12.3，此结果不是线上执行 |
| 全量 | `.venv/bin/python -m pytest -q` | **5163 passed / 9 skipped / 1 warning，exit 0，69.72 秒**；`ws-closure-full.log` |
| full profile | `.venv/bin/python scripts/validate_change.py --profile full --keep-going` | **10/10 passed，exit 0**；`ws-closure-profile.log`，包含 compileall 和 git diff --check |
| 生产只读 | SSH 内独立 GET-only 进程查询 Gate/Binance 仓位与挂单 | **2026-09-27 21:35:08 北京时间**；旧部署 3390642，Gate long501/short0、Binance0、挂单0；`ws-closure-production.json` |

9 skipped 是未启用的实盘探针；warning 为既有 TestnetSupport 收集提示。
真实 localhost WS、实际解析器/PrivateWsState/ConnectionHealth 一直连到 Supervisor
的 private_stream_unhealthy 原因与 FAIL_CLOSED/NORMAL 结果；仅元数据 HTTP 用
MockTransport，不替换被修复函数。新反例也断言 CID-only 缓存身份、正常来源标签
多订单独立索引、混合行两种顺序、零成交终态与合法 dual 不写单向缓存。

### 本轮独立只读复审

**未发现新增可证实 P0/P1/P2；本轮 WS 误恢复与来源标签缓存串单可在本地收口。**
复审承接此前完整树审查，检查本轮增量与 PrivateWsState、ConnectionHealth、
Supervisor 相邻路径；不是重新宣称所有生产问题均已解决。

独立脚本 `review-ws-final-independent.py` 与结果 `review-ws-final-independent.json`
使用真实 localhost WS → Gate parser → PrivateWsState → Supervisor.supervise，
两个连续场景、17 个状态检查均通过：

- positions→orders 及反向故障顺序、多次重连、重复 ACK、空集合与混合未知行
  不提前释放；合法 CID 搭配非法 order ID 仍阻断。
- CID-only→ID+CID→ID-only 保持单一订单缓存；4 个同 web/app 来源标签的订单
  独立保存，后续更新不覆盖其他订单。
- 好坏混合消息可保留合法行，但不据此恢复健康；合法 dual 可恢复对应频道，
  不写单向仓位缓存。
- WS 恢复后，实际 OpenPosition 仍使全局 FAIL_CLOSED 保留；阻断工作清空后
  才释放，验证健康恢复与整体风险释放的区别。

复审 55 文件哈希与送审冻结清单完全一致；没有修改实现或测试，没有重新运行
全量、没有访问生产。独立临时脚本首轮误将纯 pending-close 对账债务当成阻断
owner，按 V1 改为实际 OpenPosition 后重跑通过；不将该次脚本断言误报为代码缺陷。

### 本轮完成门槛与交接

- [x] 已保存修前基线、真实 RED、官方字段依据、V1 兼容边界及测试漏检根因。
- [x] 共享解析边界、真实消费者、全部释放入口和同族矩阵已重新分析。
- [x] GREEN、当前矩阵、相邻回归、Python 3.12、完整 pytest 与 full profile 通过。
- [x] 冻结树独立只读复审通过；本轮只有解析器、已有测试和本记录 3 文件增量。
- [x] 生产 GET-only 核验完成；旧版本、异常仓及未部署日志的限制明确记录。
- [ ] 生产代码上线、实盘异常仓消除、实际开平仓和现金账单核验完成。

Status: **implemented but not closed**（WS P2 本地代码已收口，生产事故未闭环）。

Symptom and reproduction: 故障后的空 update/无身份订单误恢复，普通来源标签把
不同订单合并；修前实际 worker 矩阵 24 失败，独立来源标签串单复现。

Authoritative contract: Gate Orders notification 的 id/text 语义；现有领域支持
非空 opaque ID 或自定义 CID；V1 实际持仓与恢复承接优先于健康标签。

Root cause: 空遍历默认 true、类型校验前 str()、未区分来源 text 与 t- 自定义
信息；旧测试以空集合证明恢复，掩盖契约缺口。

Broken production path / invariant / changed boundary: 修复既有两个 Gate WS
解析器，使 worker 只有取得对应频道非空、完整有效的证据后才解除数据故障；
订单只用有效身份进入原缓存，不改变 supervisor、REST 或交易策略。

Counterexample matrix / validation / independent review: 见本节上述矩阵、命令、
退出码及独立 17 项状态检查。最终哈希、GitNexus scope、日志索引记录于
`/tmp/lightfee-root-repair-20260925/ws-closure-verification.json`；索引结合本地
增量核对，不能把整个既有工作树的累计 diff 误当成本轮新增范围。

Production verification: 2026-09-27 21:35:08 北京时间，旧部署 3390642，Gate SAGA
long501/short0、Binance0、双方挂单0；本轮未提交、推送、部署、重启或执行实盘订单。

Remaining uncertainty/non-goals: 生产开平仓、异常仓消除、新日志线上产出仍无闭环
证明；生命周期历史 writer、Gate 风险能力与资金费现金账单仍按上文保留证据缺口。
