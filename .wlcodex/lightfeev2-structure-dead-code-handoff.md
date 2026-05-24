# LightFeeV2 结构臃肿与死代码候选交接包

## 结论

当前代码不是全局失控式臃肿，而是热点集中式臃肿。主要风险集中在 `lightfee/engine/runtime.py`、`lightfee/venues/transport.py`、`lightfee/engine/passive_close.py` 等少数文件：它们同时承担启动恢复、运行循环、入场/出场协调、交易所适配、签名、限流、私有 WS、诊断等职责。

死代码方面，已经发现可清理候选，但不能直接按静态结果删除业务函数。强信号主要是未用导入、未用局部变量和若干 `undefined-name`；弱信号是 GitNexus 调用图中“无人调用”的入口函数、协议方法、测试专用兼容方法，误报较多。

## 证据摘要

- GitNexus 索引：349 个文件、16281 个符号、224 条执行流。
- `lightfee` 业务代码约 45911 行；`tests` 约 56092 行。
- 最大业务文件：
  - `lightfee/engine/runtime.py`：6964 行，单个 `LiveRuntime` 类从第 53 行持续到第 6963 行。
  - `lightfee/venues/transport.py`：5098 行，`VenueTransport` 类从第 1034 行持续到第 4834 行。
  - `lightfee/engine/passive_close.py`：2241 行。
  - `lightfee/engine/close_executor.py`：1535 行。
  - `lightfee/engine/recovery.py`：1423 行。
- 模块行数集中：
  - `lightfee/engine`：17995 行。
  - `lightfee/venues`：11829 行。
  - `lightfee/marketdata`：5298 行。
- `uvx ruff check lightfee scripts --select F401,F841,F821 --statistics` 结果：
  - `F401 unused-import`：100 个。
  - `F841 unused-variable`：16 个。
  - `F821 undefined-name`：32 个。
- `uvx vulture lightfee scripts --min-confidence 80` 强信号样例：
  - `lightfee/apps/evolution.py:22` 未用 `apply_approval_overlay`、`reject_proposal`。
  - `lightfee/config/universe.py:51` 未用参数 `global_symbols`。
  - `lightfee/core/contracts.py:139` 未用参数 `leverage`，更像协议占位，禁止直接删。
  - `lightfee/engine/passive_close.py:2204` 未用参数 `l2_mid`，测试中存在兼容签名用法，禁止直接删。
  - `lightfee/engine/supervisor.py:31` 未用导入 `derive_engine_mode`。
  - `lightfee/marketdata/local_l2_ws.py:28` 未用导入 `ws_exc`。
  - `lightfee/venues/transport.py:43` 未用导入 `enrich_fill_from_private`。

## 根因

1. V1 语义对齐和生产修复持续叠加，新增逻辑优先塞进既有大文件，导致 `runtime` 和 `transport` 变成协调器加功能库的混合体。
2. 交易所差异、签名、限流、私有 WS、订单诊断仍集中在 `VenueTransport`，缺少按能力或协议分层的边界。
3. 入场、被动 maker、恢复、残差修复、启动恢复在 `LiveRuntime` 中交叉调度，方法私有化但没有形成可独立测试的小服务。
4. 兼容层、V1 parity 和临时修复留下了未用导入、未用变量、字符串类型注解未导入等清理债。

## Claude 需要触碰的文件

第一阶段只做低风险清理：

- `lightfee/apps/evolution.py`
- `lightfee/apps/sidecar.py`
- `lightfee/config/loader.py`
- `lightfee/config/validation.py`
- `lightfee/engine/close_executor.py`
- `lightfee/engine/entry_sync.py`
- `lightfee/engine/passive_close.py`
- `lightfee/engine/recovery.py`
- `lightfee/engine/runtime.py`
- `lightfee/marketdata/local_l2_ws.py`
- `lightfee/venues/transport.py`
- `lightfee/venues/market_data.py`
- `scripts/check_process_singleton.py`

第二阶段才考虑拆分热点：

- `lightfee/engine/runtime.py`
- `lightfee/venues/transport.py`
- `lightfee/engine/passive_close.py`
- `lightfee/engine/recovery.py`
- 新增内部模块可放在 `lightfee/engine/`、`lightfee/venues/` 下，但必须保持公开导入面兼容。

## 实施步骤

1. 先跑 `uvx ruff check lightfee scripts --select F401,F841,F821 --statistics`，保存当前基线。
2. 仅清理 `F401` 未用导入和明显无副作用的 `F841` 未用局部变量。
3. 对 `F821` 分类：
   - 真实运行时缺失，例如 `lightfee/engine/recovery.py` 中使用 `time.time()` 但没有导入 `time`，应补导入。
   - 仅类型注解缺失，优先在 `TYPE_CHECKING` 下导入或使用已有本地导入策略。
4. 不处理协议占位参数，例如 `ensure_entry_leverage(..., leverage)`。
5. 不删除 GitNexus “无调用”但属于 CLI 入口、协议方法、测试覆盖对象、兼容层的方法。
6. 第一阶段通过后，再为拆分写设计，不要直接大规模搬迁。
7. 拆分热点时按职责分批：
   - `LiveRuntime` 启动恢复、pending entry 恢复、local L2 激活、主 tick 循环分离。
   - `VenueTransport` 签名/请求、限流、私有 WS、订单解析、被动单操作分离。
   - `PassiveCloseExecutor` 成本估算、maker 维护、fallback close、live flatness probe 分离。

## 验收标准

- `uvx ruff check lightfee scripts --select F401,F841,F821 --statistics` 中 `F401` 和明显 `F841` 清零；`F821` 只允许有明确注释或 `TYPE_CHECKING` 解决后的零误报。
- 不改变 `pyproject.toml`、`uv.lock`、部署文件和配置。
- 公开 CLI 入口仍保持 `pyproject.toml [project.scripts]` 中的名称和目标不变。
- 现有测试不因为导入路径变化失效。
- 若进入第二阶段拆分，原模块对外导出的类和函数必须保持兼容，先加薄转发再迁移内部实现。

## 验证计划

1. `uvx ruff check lightfee scripts --select F401,F841,F821`
2. `uvx vulture lightfee scripts --min-confidence 80`
3. `uv run pytest tests/test_evolution.py tests/test_risk.py tests/test_private_ws_state.py tests/test_passive_close.py tests/test_venues_transport.py tests/test_recovery_reconciliation.py`
4. `uv run pytest tests`
5. 修改前后分别运行 GitNexus：
   - 修改每个函数/类前按项目要求跑 `gitnexus_impact`。
   - 提交前跑 `gitnexus_detect_changes()`。

## 禁止事项

- 禁止按静态“无人调用”结果批量删除函数、类、协议方法或 CLI 入口。
- 禁止改业务逻辑、测试语义、依赖锁、配置或 systemd 部署文件来顺手通过检查。
- 禁止重命名公开符号，除非使用 GitNexus rename 并完成影响分析。
- 禁止在同一补丁里同时做清理和大规模架构拆分。
- 禁止删除 V1 parity、compat、recovery 相关代码，除非有测试和运行入口证明已经废弃。
