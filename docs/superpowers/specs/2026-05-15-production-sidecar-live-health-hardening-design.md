# Production Sidecar/Live Health Hardening Design

日期: 2026-05-15

状态: 设计稿，供执行智能体实现；本文件记录生产事故后的永久防复发规格。

## 背景

2026-05-15 生产云机出现“进程正常但业务不健康”的复合问题:

1. `lightfee-sidecar.service` 处于 active，但启动命令没有传 `--config`，实际跑默认 `config/example.toml`。
2. sidecar 快照只覆盖 4 个 venue，行情存在 fixture 特征：`BTCUSDT` bid/ask 为 `100.0`，`market_observed_at_ms=1710000075000`。
3. 修正为 live sidecar 配置后，系统 resolver 对 `www.okx.com` 解析失败，导致 sidecar 重启循环。
4. DNS 修复后 sidecar 恢复 7 venue 快照，live 仍因为历史持久化 `risk_mode=fail_closed` 卡住；open/pending 全为 0 时手工安全恢复后 live 才进入 `running`。
5. 当前生产热修已恢复业务健康，但 repo 还缺少防止复发的代码、部署资产、自动验收和恢复机制。

这个问题不是单一代码 bug，而是生产服务契约、部署漂移检测、DNS 持久化和恢复状态语义共同缺口。

## 目标

把“systemd 绿灯”升级为“业务健康可验证”:

- sidecar 必须证明自己使用 live 配置、连接真实 7 venue、写出新鲜非 fixture 快照。
- live 必须证明自己在安全状态下能从历史 `fail_closed` 恢复，且当前状态导出能说明扫描是否真的运行。
- 部署必须提供可版本化 systemd/network 配置模板，并有脚本验证生产机是否符合契约。
- DNS 解析必须变成持久化配置，不能依赖一次性编辑 `/etc/resolv.conf`。

## 非目标

- 不在本规格里替换 Rust V1 sidecar。当前生产允许继续使用 Rust sidecar 作为 V2 live 的 opportunity-input data plane，直到 V2 sidecar full refactor 完成。
- 不放宽 fail-closed 的安全语义。只有“无 open position、无 pending entry、无 pending close、无 operator-requested fail_closed”的干净状态可以自动清除陈旧 fail_closed。
- 不把真实生产密码、API key、账号标识写进仓库。

## 生产契约

### Sidecar 服务契约

生产 sidecar 必须满足:

- systemd `ExecStart` 显式包含 `--config`。
- config path 不能是 `config/example.toml`。
- unit 必须加载 `/etc/lightfee/lightfee.env` 或等价 secret env 文件。
- 同一时间最多一个 sidecar writer 写 `runtime/opportunity-input-snapshot.json`。
- 快照必须新鲜，且至少覆盖 7 个 venue:
  - `aster`
  - `binance`
  - `bitget`
  - `bybit`
  - `gate`
  - `hyperliquid`
  - `okx`
- 快照不能带已知 fixture 特征:
  - `market_observed_at_ms == 1710000075000`
  - 多数主流 symbol bid/ask 同时等于 `100.0`
  - `quote_venue_count < 7`

### Live 服务契约

生产 live 必须满足:

- systemd `ExecStart` 显式包含 live config，例如 `/opt/lightfee-v2/config/live.toml`。
- current-state 导出中:
  - `lifecycle == running`
  - `risk_mode == running`，除非存在明确 open/pending/recovery 阻塞原因
  - `last_tick_ms` 新鲜
  - `last_scan` 能反映最近一次 sidecar scan / candidate discovery 结果
- 如果恢复出历史 `risk_mode=fail_closed`，但状态已经干净，启动流程可以清除这个陈旧 latch，并记录 journal 事件。

### DNS 契约

生产 DNS 必须满足:

- `www.okx.com`、`fapi.binance.com`、`api.bybit.com`、`api.bitget.com`、`api.gateio.ws`、`api.hyperliquid.xyz` 可由系统 resolver 解析。
- resolver 顺序必须优先使用公共 resolver 或已验证的稳定 resolver。
- DNS 配置必须通过 NetworkManager/systemd-resolved/drop-in 等持久方式落地，不能只改 `/etc/resolv.conf`。

## 需要实现的能力

### 1. 生产健康分析库

新增 `lightfee/ops/production_health.py`，提供纯函数:

- `analyze_systemd_unit(name, text)`
- `analyze_resolver_config(text)`
- `analyze_sidecar_snapshot(data, now_ms, max_age_ms)`
- `analyze_current_state(data, now_ms, max_tick_age_ms)`
- `summarize_reports(reports)`

这些函数不直接 SSH、不读 secret、不依赖真实系统，方便单元测试。

### 2. 生产验证 CLI

新增 `scripts/verify_production_services.py`，封装:

- 本地/远程文件路径输入。
- systemd unit 内容检查。
- snapshot JSON 检查。
- current-state JSON 检查。
- resolver 配置检查。
- `--json` 输出，便于 CI/agent 解析。
- 任一 critical 失败时退出码为 1。

### 3. 版本化部署资产

新增:

- `deploy/systemd/lightfee-live.service`
- `deploy/systemd/lightfee-sidecar-rust-v1.service`
- `deploy/network/NetworkManager-lightfee-dns.conf`
- `docs/ops/production-health-runbook.md`

部署资产要表达当前生产事实: V2 live 消费 sidecar snapshot；sidecar 可暂时是 Rust V1 binary，但必须使用 live config。

### 4. 安全恢复陈旧 fail-closed

修改恢复语义:

- 如果启动恢复后 `needs_reconciliation(state) == False`，且:
  - `risk_mode == FAIL_CLOSED`
  - `operator.requested_mode != FAIL_CLOSED`
  - `open_positions/pending_entries/pending_closes/pending_passive_closes` 全为空
  - `recovery_blocked_reason` 为空或被本次 clean recovery 清除
- 则恢复到 `risk_mode=RUNNING`、`lifecycle=RUNNING`，并记录:
  - `runtime.risk_mode_changed`
  - `runtime.stale_fail_closed_cleared`

必须保留:

- operator 主动设置的 fail-closed。
- 有 open/pending/recovery work 的 fail-closed。
- recovery 仍然 blocked 的 fail-closed。

### 5. `last_scan` 可观测性

当前 current-state 有 `last_scan` 字段，但生产里观察到它为 `null`，同时 journal 已经出现 `runtime.candidates_tradeable`。这会误导运维判断。

要求:

- `EngineState` 显式拥有 `last_scan: dict | None`。
- live tick 每次 sidecar snapshot 被成功评估后写入 `last_scan`。
- `last_scan` 至少包含:
  - `ts_ms`
  - `snapshot_freshness`
  - `candidate_count`
  - `tradeable_count`
  - `degraded_venues`
  - `no_entry_reason`
- current-state 导出必须保留这个结构。

## 验收标准

1. 单元测试能证明 example-config sidecar unit 被判为 critical failure。
2. 单元测试能证明 4 venue / fixture timestamp / `100.0` 假行情快照被判为 critical failure。
3. 单元测试能证明 7 venue 新鲜真实快照通过。
4. 单元测试能证明 stale fail-closed clean state 会自动恢复 running。
5. 单元测试能证明 operator-requested fail-closed 和 pending-work fail-closed 不会被自动清除。
6. current-state 导出包含非空 `last_scan`，且能反映 tradeable count / blocked reason。
7. `scripts/verify_production_services.py` 在 fixture 输入上能输出 JSON 报告并正确设置退出码。
8. 部署 manifest 把新增 production health 脚本和部署资产列为 critical。
9. runbook 给出生产验证命令，不包含任何 secret。

