# LightFee V2 只读运营看板

这是一个零依赖的静态前端，用于观察：

- 当前持仓和待处理开平仓
- 从 journal 推导的历史仓生命周期
- 被拒绝、阻断或恢复中的问题开仓
- 已由诊断采集的交易所余额视图
- 需要关注的运行日志

页面本身不包含任何交易、撤单、风控模式切换或运行时写入逻辑。它只会读取用户在浏览器中选择的文件，或同目录下的静态快照文件。

## 使用方式

在生产机或拥有只读运行数据的环境中，先生成诊断快照。`diagnose_live.py` 使用现有的只读诊断路径查询状态和交易所真相，不能替代交易风控流程。

```bash
cd /opt/lightfee-v2
mkdir -p dashboard/data
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/diagnose_live.py --json > dashboard/data/latest.json
cp runtime/events.jsonl dashboard/data/events.jsonl
python3 -m http.server 8080 --directory dashboard
```

然后打开 `http://127.0.0.1:8080`，点击“读取本地快照”。也可以直接点击“导入诊断”选择 `diagnose_live.py --json` 的输出，并按需点击“导入日志”选择 journal JSONL 文件。

`latest.json` 和 `events.jsonl` 会包含持仓、余额与运行信息。请只在受控的内网或本机环境中托管，不要作为公共静态站点发布。

## 数据范围

- `latest.json`：兼容 `scripts/diagnose_live.py --json` 的输出。当前持仓来自 `local_state`，余额来自 `exchange_truth.balance_views`。
- `events.jsonl`：兼容 LightFee journal。页面只在浏览器内以 `entry.opened`、结束事件和问题关键词归纳历史仓与日志。

“未采集”或“未提供”不代表余额、仓位或交易所真相为零。生产处置仍应以完整诊断证据和交易所真相为准。
