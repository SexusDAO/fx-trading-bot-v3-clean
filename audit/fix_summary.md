# 测试基线存档（分支 fix/system-overhaul）

## 本次只改测试环境，未动业务代码

| 改动 | 文件 | 说明 |
|---|---|---|
| 指定测试目录 | `fx/pytest.ini` | `testpaths = fx/tests`；移除不生效的 `ignore` 配置项 |
| 排除脚本型文件 | `fx/tests/conftest.py` | `collect_ignore_glob = ["**/test_trade.py"]`、`collect_ignore = ["../test_trade.py"]`（`fx/test_trade.py` 收集时会直接 `sys.exit`） |
| 跳过缺 fixture | `fx/tests/test_analyze_one.py` | 模块级 `pytest.skip('fixture file missing', allow_module_level=True)`（`tests/fixtures/phase2_golden.json` 不在快照内） |
| 跳过依赖线上分支 | `fx/tests/test_shutdown.py`、`fx/tests/test_task_exception.py` | 模块级 `pytest.skip('requires feature/passive-trigger branch', ...)`（二者从 `main.py` 抽取 `_shutdown_components` / `_handle_task_exception`，当前 main 分支没有） |
| 修抽取逻辑 | `fx/tests/test_patrol_hub.py` | `func_body()` 的终止符正则改为只匹配第 0 列 `def`/`class`，函数内嵌套 `def` 不再截断函数体 |

## 最终基线数字

```
cd fx && python3 -m pytest tests/ -q
57 failed, 969 passed, 19 skipped, 6 warnings, 74 subtests passed in 66.71s
```

**基线失败数：57（历史遗留，与本次修复无关）**

后续每次改完业务代码跑同一条命令，失败数不得超过 57。

## 失败分类对照

| 测试文件 | 失败数 | 分类 | 原因 |
|---|---|---|---|
| `test_cancel_state_batch1.py` | 16 | 历史遗留 | 改动前的逐文件基线即 16 failed |
| `test_exit_signal_and_direction.py` | 8 | **顺序污染（非回归）** | 单文件运行 12 passed / 0 failed；见下节 |
| `test_opt_v387_step5.py` | 7 | 历史遗留 | 改动前逐文件即 7 failed |
| `test_ensure_deps.py` | 6 | 历史遗留 | 改动前逐文件即 6 failed |
| `test_notifier_flush_timing.py` | 5 | 历史遗留 | 改动前逐文件即 5 failed |
| `test_input_validation.py` | 4 | 历史遗留 | 改动前逐文件即 4 failed |
| `test_trigger_identity.py` | 3 | 历史遗留 | 改动前逐文件即 3 failed |
| `test_reversal_entry_range.py` | 3 | 历史遗留 | 改动前逐文件即 3 failed |
| `test_scanner_batch_markprice.py` | 2 | 历史遗留 | 改动前逐文件即 2 failed |
| `test_price_event_freshness.py` | 2 | 历史遗留 | 改动前逐文件即 2 failed |
| `test_config_fallback_signal_watcher.py` | 1 | 历史遗留 | 改动前逐文件即 1 failed |
| `test_patrol_hub.py` | 0（已修） | **本次引入 → 已修** | `func_body()` 被 `_补挂保护单` 内的嵌套 `def _repair_sl_only` 截断，取不到 `get_open_orders` 那行；已修正抽取正则 |
| 合计 | 57 | | |

## 关于 `test_exit_signal_and_direction.py` 的 8 条

这 8 条**不是本次修复引入的回归**，是测试套件固有的顺序污染：

- 单文件运行：`12 passed`（改动前、改动后都一样）。
- 全量运行：8 failed。
- 成因：在其之前被收集的 `test_notifier_flush_timing.py`、`test_opt_v387_step5.py` 会替换 `sys.modules` 里的 `shared.*` 桩模块且在用例结束后不恢复，导致本文件拿到的是别的文件留下的桩。该文件自身的注释里已记录过这个问题。

属测试基础设施的历史缺陷，需单独排期修（例如在 `conftest.py` 里加 autouse fixture 保存/恢复 `sys.modules`），本次不处理。

## 已知限制

- 本基线是「全量运行」口径，所含 8 条顺序污染使该数字略高于「逐文件运行」的真实遗留数（真实遗留约 49 条）。
- 整体测试仍无法覆盖 `fx/test_trade.py`、`test_analyze_one.py`、`test_shutdown.py`、`test_task_exception.py` 四个文件，原因见上表。
- 沙箱内 `test_online_fixes.py` 曾出现卡住（挂起）现象，本轮全量运行未复现。


## 第一~四批修复提交（fix/system-overhaul）

| Commit | 说明 |
|---|---|
| `8052b4c957` | fix(#risk): RISK_PER_TRADE 从 5% 降到 1% |
| `48453e0dc0` | fix(#signal): breakout做多RSI上限从65改到75，避免系统性过滤强势突破 |
| `f14d901c05` | fix(#signal): reversal SHORT chg24门槛提高到10% |
| `e5afd2ccb2` | fix(#risk): 恢复日内亏损熔断，改为权益8%触发 |
| `ffaae5e6d3` | fix(#strategy): TIME_STOP_PROFIT_THRESHOLD接入时间止损逻辑 |
| `1690d57fab` | fix(#strategy): TP1成交后剩余仓位止损移到入场价保本 |
| `c7c6dffab0` | fix(#46): get_order 查询失败抛 ApiQueryError，删除 24h 清除死代码 |
| `41c486a8a5` | fix(#48): 清理代码未引用的失效常量，日重置拆分熔断重置与日志备份 |
| `d751ce8c06` | fix(#39): watchdog 令牌走环境变量，必填项缺失启动报错，listenKey 日志脱敏 |
| `5fefacb24d` | fix(#50): DEPLOY 克隆地址去令牌，LLM 中转地址移到 .env，日志轮转统一由 main.py 负责 |
| `9b9deb525d` | fix(#35b): LLM 调用加 60 秒总超时，超时跳过评分用默认分值并记 WARNING |
| `fcae54097a` | fix(#16): 仅确认非对冲模式时拒绝启动，无法确认时告警后继续 |
| `e9703c4159` | fix(#signal): reversal SHORT RSI1H补充通道chg24门槛提高到10%，与主通道一致 |
| `19e4d861cc` | test(#signal): reversal SHORT 用例 chg24 8%→12%，匹配提高后的10%门槛 |
| `796464269d` | feat(#perf): 开启触发器驱动与市场数据hub，兜底扫描改为15分钟 |
| `c2add5236e` | test(#perf): 触发器单测隔离开关默认值，适配生产默认开启 |
| `dce13ac6cb` | perf(4-B): liq_cache 预热改为固定5线程池，去掉每标的一线程 |
| `ba5a97e96c` | chore(4-C): 删除 tracemalloc 内存快照与 kline_cache DEBUG 临时诊断 |

**最终测试**：57 failed / 969 passed / 19 skipped（与基线一致，无新增失败）。
