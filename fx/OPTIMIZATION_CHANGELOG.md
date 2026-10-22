# OPTIMIZATION_CHANGELOG — v3.83 / v3.84 改动清单（供评审）

> 原则：每项改动都有开关或可单独回滚；代码里以 `[v3.8x ...]` 注释标出原因、依据和评估方法。
> 以下"预期效果"是基于金融逻辑的假设，**没有经过回测或实盘验证**，请用模拟盘/回测确认后再决定是否合并。

## 分支与内容

| 分支 | 内容 | 性质 |
|---|---|---|
| `fix/scanner-obvious-bugs` | 预筛方向判断反了；熊市预筛过宽；EMA 趋势空头分支不可达 | bug 修复，会增加信号数量 |
| `fix/logic-conflicts-pm` | `_entry` 未定义、`open_ts` 字段错、`signal_watcher` 缺 `api` 导入；价格循环异常隔离 | bug 修复 |
| `opt/first-pass-v384`（本分支） | 下表 OPT-1 ~ OPT-3 + 保本阶梯缓冲 + 工具与单测 | 优化，均有开关 |

## 本分支的优化项

| 编号 | 位置 | 原逻辑 | 新逻辑 | 开关（config.py） | 预期效果（假设） | 怎么评估 |
|---|---|---|---|---|---|---|
| OPT-1 | `position_manager._check_time_stop` | 持仓 6H(反转 8H) 且浮亏>固定 2% 就市价平仓，比止损(≥2.5%/3%/5%)更紧，是"隐形止损" | 阈值=max(2%, 0.6×止损距离) | `OPT_STAGNATION_SCALE_BY_SL` | 宽止损标的少被提前砍，实现亏损次数下降 | 对比 `close_reason=early_stagnation` 占比及其后续是否触及止损 |
| OPT-2 | `position_manager._on_price_update` | TP1 后价格跌穿入场价，却把止损放到入场价另一侧(错误一侧)，会被拒单或立即成交 | 此时直接平掉剩余仓位；价格仍在有利侧才移动止损到保本 | `OPT_TP1_PROTECT_CLOSE` | 避免拒单/不可预期成交，锁定 TP1 利润 | 看 `close_reason=tp1_protect_exit` 的笔数与平均盈亏；观察日志有无 -2021 |
| OPT-3 | `calc_sl_tp._entry_pullback` | 无清算区降级：多 0.3ATR、空 0.8ATR，不对称 | 两侧统一 0.5ATR(最小0.3，上限2.0) | `OPT_PULLBACK_ENTRY_SYMMETRIC` | 多头入场价更优；空头更易成交，开仓次数上升 | 对比回踩单成交率、成交后 MAE |
| OPT-4 | `config.TRAIL_STEPS_TREND` | 浮盈 0.8ATR 就把止损挪到恰好入场价 | 1.0ATR 才挪，且放在入场价+0.1ATR(覆盖手续费滑点)，其余档后移 | `OPT_TRAIL_BE_BUFFER` | 减少"赚一点又被扫回 0"的零收益止损 | 把止损拆成"保本/移动止损"与"真亏损止损"分别统计 |

## 辅助

- `shared/opt_v384.py`：上面三项的纯函数实现，便于单测与评审。
- `tests/test_opt_v384.py`：12 个单测，`cd fx && python3 -m unittest tests.test_opt_v384 -v`。
- `tools/reject_funnel.py`：只读，统计各路径通过率与拒绝原因 Top N，用于评估放宽过滤后的效果。

## 刻意没有改的内容

- 熔断、杠杆、保证金预检、日亏损上限等风控项（按你的要求留到最后）。
- Stop Hunt 已是 `STOP_HUNT_ENABLED=False`，入场价随机化已是 `ORDER_ENTRY_RANDOMIZE=False`，无需处理。
- `strategy-config.json` 未动。

## 合并前请重点观察

1. OPT-4 会让趋势单的止损更晚保本，**单笔最大回撤可能变大**，请与"零收益止损减少"一起权衡。
2. 前两个分支的 bug 修复会让"TP1 后保本"首次真正运行，请先看 OPT-2 的日志。
3. 本轮改动都没有在实盘或回测环境验证过，仅做了语法、静态检查和纯函数单测。

---

# 第二轮（v3.85）— 分支 `opt/second-pass-v385`（基于 `opt/first-pass-v384`）

> 验证：静态检查无未定义变量；新增 4 个单测通过（两轮合计 16 个）；原有测试结果与改动前一致（`test_judge.py` 改动前后均为 8 失败 / 13 通过，均为既有问题）。未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关（config.py） |
|---|---|---|---|---|
| OPT-5 | `shared/judges.py` `_judge_breakout_impl` | 判断突破前，若最后一根 1H K 线未收盘（`k[6]` > 当前时间）就去掉它 | 原代码把 `k1h[-1]` 当作已闭合突破 K 线，但 scanner 拉到的最后一根通常未收盘：盘中触发后会被重绘成假突破，量比也只算了部分成交量 | `OPT_BREAKOUT_CLOSED_BAR` |
| OPT-6 | `scanner.py` 新增 `_in_symbol_funding_window`；`analyze_symbol`；`run_scan` | 资金费禁开仓窗口改为按该标的自己的 `nextFundingTime` 和结算周期（1/4/8H）判断；启用后不再因 BTC 费率整轮停扫；拿不到结算时间时回退旧逻辑 | 原逻辑只看 UTC 0/8/16 整点 + BTC 一个费率：1H/4H 结算的合约在其他结算时刻没有保护，而所有标的又会被一起停扫，白白丢失扫描轮次 | `OPT_FUNDING_WINDOW_PER_SYMBOL` |
| FIX-1 | `shared/judges.py` `_judge_breakout_impl`；`config.py` `BREAKOUT_RSI_*` | 突破的 RSI 门槛改为读取配置；配置值同步为原代码实际生效值（多 65、空 35） | 原写法 `65 if tier=="high" else 65` 两个分支相同，分层是假的；配置里 `BREAKOUT_RSI_MAX_LONG=75` 等从未被读取。**本项行为不变**，只是让配置真正生效 | 无（行为不变） |
| FIX-2 | `position_manager.py` `_maybe_pyramid` | 加仓所需 RSI 拿不到时，本轮不加仓 | 原注释写"保守允许加仓"，但数据缺失时继续加仓恰恰不保守 | `OPT_PYRAMID_FAIL_CLOSED` |
| DOC-1 | `position_manager.py` `_check_time_stop` | 更正硬上限注释：实际所有类型统一 16H；并注明 TP1 已成交且盈利的持仓实际上限是 72H | 原注释写"reversal 16H、其他 12H"，与实现不符，容易误导调参 | 无（仅注释） |
| DOC-2 | `config.py` | 给 93 个代码从未引用的常量加上注释 `[v3.85 DOC-2] 代码未引用此常量，修改无效` | 这些常量（候选池、反转、扫描间隔等）改了不会生效，调参时会误以为改了 | 无（仅注释） |

辅助文件：`shared/opt_v385.py`（逐标的结算窗口的纯函数）、`tests/test_opt_v385.py`（4 个单测）。

## 本轮发现但刻意未改（需要你决定）

- **反转做空没有分层门槛**（`judges.py` `_judge_reversal_impl`）：`_rev_chg24_short`、`rsi_short_thresh`、`fr_short_thresh` 算了但没用，做空实际只用写死的 `chg24>=5`、`RSI4H>65`。接上会改变信号数量，留给你定。
- **TP1 可达性检查只完成一半**（`signal_watcher.py`）：读取了 TP1 价格但从未使用，补全需要定义"不可达"的标准。
- **保证金预检写死 10x**（`executor._account_final_check`）：属于风控，按你的要求暂不动。
- **有 51 个既有测试报错、`test_judge.py` 有 8 个失败**：改动前就存在，测试本身已过时，暂未修。

## 本轮合并后请观察

1. OPT-5：突破信号会晚出现约一根 K 线内的时间，但假突破应减少；对比突破单的 TP1 触发率。
2. OPT-6：资金费窗口内仍会扫描并对非结算标的开仓，扫描次数会增加，注意内存和接口限频；`get_funding_interval` 每 6 小时全量刷新一次缓存。


---

# 第三轮（v3.86）— 分支 `opt/third-pass-v386`（基于 `opt/second-pass-v385`）

> 验证：全量测试由改动前的「51 个报错 + 2 个文件无法导入」变为「151 通过 / 9 失败」；剩余 9 个失败在 main 上单独运行同样失败（`test_judge.py` 8 个、`test_risk.py` 1 个），属于测试本身过时，非本轮引入。未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关（config.py） |
|---|---|---|---|---|
| FIX-3 | `shared/signal_watcher.py` BTC 剧烈波动后的「止损踩空」检查 | `from shared.notify` 改为 `from shared.notifier` | 原模块名不存在，ImportError 被 except 吞掉，止损踩空告警从未发出过 | 无（bug 修复） |
| OPT-7 | `shared/judges.py` `_judge_reversal_impl` 做空条件二 | 做空 RSI4H 门槛取 `max(65, rsi_short_thresh)`，即高波动层 70、其余 65 | 原代码已按波动层算出 `rsi_short_thresh` 却从未使用；高波动币 RSI 更容易冲高，65 不算超买，易在加速段过早做空 | `OPT_REVERSAL_SHORT_TIER_RSI` |
| TEST-1 | `tests/test_exit_signal_and_direction.py` | 桩模块只在本文件测试运行期间临时装入（fixture），收集结束即恢复真实模块 | 原实现永久覆盖 `sys.modules` 中的 `shared.*`，导致其他测试文件导入到空桩，出现「无法导入」和 51 个 AttributeError，测试失去保护作用 | 无（仅测试） |

## 本轮评估后决定不改

- `fr_short_thresh`（反转做空资金费门槛 0.25%/0.35%）：作为硬门槛会大幅减少做空信号，与 v3.6 有意放宽冲突，仅在代码中注释说明。
- TP1 可达性检查（`signal_watcher.py` 读取了 `_tp12` 未使用）：需要先定义"不可达"的标准（例如 TP1 距现价超过 N 倍 ATR），留待你定。
- 剩余 9 个过时测试：需要按当前判定逻辑重写测试数据，工作量较大，单独一轮处理更清楚。

## 本轮合并后请观察

- OPT-7：高波动币的反转做空信号会减少，对比这类单的胜率。
- FIX-3：BTC 剧烈波动时会开始收到「止损踩空」告警，若过于频繁可再调节流。

---

# 第四轮（v3.87）开仓场景重构·第一批：分支 `opt/entry-redesign-v387`（基于 `opt/third-pass-v386`）

> 统一原则（四要素）：**触发 → 确认 → 在确认处入场 → 止损放在"开仓理由失效位"外侧**。目标是让每一次止损都能归因：要么市场整体波动，要么结构真的失效，而不是入场位置本身有问题。
> 验证：静态检查无未定义变量；新增 7 个单测通过（四轮合计 23 个）；全量测试 158 通过 / 9 失败，9 个失败与改动前相同（均为既有过时测试）。中途发现本批改动使 3 个旧测试失败，已修正实现（见 OPT-9 说明），并更新 1 个测试断言。未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关（config.py） |
|---|---|---|---|---|
| OPT-8 | `shared/calc_sl_tp.py` `_entry_pullback` | 回踩限价挂在 `mark∓0.1ATR`，即确认发生处；入场模式记为 `at_confirm` | 回踩判定已要求"价格在回踩区 + 止跌/止涨信号"，原逻辑却在确认后再挂到更深的位置：只有价格继续走坏才成交，而这说明信号已失效，成交天然偏向坏单（逆向选择）。**覆盖第一轮 OPT-3 的降级参数**（OPT-8 开启时 OPT-3 不再生效） | `OPT_PULLBACK_ENTRY_AT_CONFIRM` |
| OPT-9 | `shared/calc_sl_tp.py` `calc_sl_tp` 止损段 | 回踩 / EMA 趋势 / 反转：止损放在最近 8 根已闭合 1H 结构低点（高点）外 0.3ATR，距入场 1.0~3.0ATR；同时不小于原波动层最小距离（2.5% / 3% / 5%）；结构太远则保留原止损。`sl_degraded` 保持原值，新增 `signal["_sl_source"]="invalidation"` 供日志统计 | 原止损由清算图（非币安官方数据，常缺失）或固定 ATR/百分比决定，与开仓理由无关。改为只用币安官方 K 线的结构失效位。**注意**：止损距离变化会通过 `RISK_PER_TRADE` 改变名义仓位（仍受单仓上限约束） | `OPT_INVALIDATION_SL` |
| OPT-10 | `shared/judges.py` `_judge_oi_divergence_impl` | 新增衰竭确认：① 最后一根已闭合 1H 未创近 3 根新低（做空：新高）② 主动买入占比高于（做空：低于）近 20 根均值 | 原逻辑"价格在 24H 低点附近 + OI 1H 增 10% → 做多"，同样是空头顺势加仓的典型形态，更常见的是继续下跌。只有卖压衰竭、买盘回升，才是"空头被困" | `OPT_OI_DIV_EXHAUSTION_CONFIRM` |
| OPT-11 | `shared/judges.py` `_judge_ema_trend_impl` | 要求 1H 或 15M 出现止跌（做多）/止涨（做空）形态；形态名记入 `trigger_bar` | 原逻辑价格回到 4H EMA20 附近就以现价立即入场，等于"到了区域就买"，回踩可能还在进行中 | `OPT_EMA_TREND_TRIGGER_BAR` |

辅助文件：`shared/opt_v387.py`（失效位止损、衰竭确认两个纯函数）、`tests/test_opt_v387.py`（7 个单测）；`tests/test_calc_sl_tp.py` 的入场模式断言加入 `at_confirm`。

## 场景评估结论（本轮的设计依据）

| 场景 | 结论 | 本批是否改动 |
|---|---|---|
| 回踩 | 保留结构；入场与止损改为四要素 | 是（OPT-8、OPT-9） |
| OI 背离 | 方向假设不可靠，需衰竭确认 | 是（OPT-10） |
| EMA 趋势 | 保留；缺触发 K 线 | 是（OPT-11、OPT-9） |
| 反转 | 条件严但清晰；只换止损锚点 | 仅 OPT-9 |
| 突破 | 保留；执行方式需改（真突破不回踩会错过） | 下一批 |
| 区间反弹 | 保留；可补对称的做空版本 | 下一批 |
| 资金费套利 | 保留 | 否 |
| Stop Hunt | 维持关闭（延续率约 50%，无优势） | 否 |

## 下一批计划

1. 突破：拆成"突破位回踩"与"15M 延续确认"两种入场，止损放在突破 K 线另一端外侧。
2. 区间反弹：补"暴跌后横盘上沿做空"。
3. 止盈锚点：从清算图逐步换成官方 K 线可推导的结构位与成交量分布（价值区高低点）。

## 本批合并后请观察

1. 回踩成交率会上升（不再等更深的价），关注成交后 1 小时内的最大不利偏移（MAE）是否下降。
2. 按 `_sl_source=invalidation` 统计止损单：被打掉时价格是否真的越过了结构位。
3. OI 背离与 EMA 趋势的信号数会减少，这是预期内的。
4. 止损距离变化会改变仓位大小，首批成交请核对名义仓位。

---

# 第五轮（v3.87 第二批）突破场景：同分支 `opt/entry-redesign-v387`

> 验证：静态检查无未定义变量；全量测试 158 通过 / 9 失败，9 个失败与改动前相同（既有过时测试）。这两项没有新增单测（逻辑嵌在判定与监视流程里），只做了静态检查，未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关（config.py） |
|---|---|---|---|---|
| OPT-12 | `shared/judges.py` `_judge_breakout_impl` RSI 过滤处 | 量比达到强量阈值（low 2.5 / 其它 3.0，与同函数 `vol_strong` 一致）时，做多 RSI1H 上限放宽到 75，做空下限放宽到 25；量能不够强仍用原来的 65 / 35 | 真正放量的突破 RSI1H 常在 65~75，固定 65 会把最强的一类突破全部挡掉；只对强量放宽，避免普通量能的追高 | `OPT_BREAKOUT_RSI_STRONG_RELAX`、`BREAKOUT_RSI_MAX_LONG_STRONG`、`BREAKOUT_RSI_MIN_SHORT_STRONG` |
| OPT-13 | `shared/signal_watcher.py` `compute_entry_range` 突破分支 | 价格距突破位 ≤ 0.5ATR 时在现价入场（延续入场）；更远仍等回踩突破位 | 原逻辑价格在突破位上方时一律只等回踩（有效期 30 分钟），真突破不回踩就整单错过；信号已有放量 + 15M 确认，紧贴突破位时回踩空间很小 | `OPT_BREAKOUT_MOMENTUM_ENTRY` |

## 本轮明确没改的

- 突破止损：仍用原逻辑。1H 结构位常离突破位较远，直接套 OPT-9 会过宽，需要专门的"突破位失效"锚点，待有成交数据再定。
- 区间反弹做空版本：目前区间反弹只在"大涨后横盘"这一种形态下成立，镜像做空需要新增路由与止盈锚点，没有数据支撑，暂不加。
- 反转、资金费套利：条件已清晰，不动。Stop Hunt：保持关闭。

## 观察点

1. 突破单的成交数是否上升；延续入场（日志含"延续入场"）的单被止损的比例，与回踩入场的单对比。
2. RSI 放宽后进来的单（RSI1H > 65）胜率是否明显更差，若更差则把 `OPT_BREAKOUT_RSI_STRONG_RELAX` 关掉。

---

# 合并决策（2026-10-19）：`opt/entry-redesign-v387` 内容并入 main，开关按评审结论设置

| 类别 | 编号 | 处理 |
|---|---|---|
| Bug 修复 | BUG-1/2/3、FIX-1/2/3、Scanner 三处 | 生效（`OPT_PYRAMID_FAIL_CLOSED` 保持 True） |
| 文档/测试 | DOC-1/2、TEST-1 | 生效，无逻辑影响 |
| 优化（先关闭） | OPT-1/2/4/5/6 | 代码合入，开关默认 False，行为与合并前一致 |
| 暂缓（待数据） | OPT-3/7/10/12/13，另 OPT-11 | 代码合入，开关默认 False |
| 舍弃 | OPT-8/9 | 会绕过清算区入场价与止损价，代码保留但开关永久 False，不建议开启 |

说明：所有 `OPT_*` 开关在 `config.py` 中已加注释 `[v3.87 合并决策]`。开启某项只需把对应开关改为 True。OPT-11（EMA 趋势触发K线）评审清单未列出，我按保守原则默认关闭。

---

# 第六轮（v3.87 逐模块审查·第 1~2 步）：分支 `opt/step-2-scanner`（基于 main c865905e）

> 验证：静态检查无未定义变量；全量测试 158 通过 / 9 失败（9 个为既有过时测试，与改动前相同）。未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP1-DOC | `config.py` | 7 个确实未引用的常量补 DOC-2 标注；`STOP_HUNT_MIN_SCORE_WEEKEND` 撤销误标并注明仅回测使用；`FUNDING_ARB_ENTRY_WINDOW_MAX_H` 注明占位值 | 避免误导调参 | 仅注释 |
| STEP2-1 | `scanner._load_pool` | 候选池缓存超过 45 分钟则重新刷新；刷新失败保留旧池兜底，并发 Telegram 告警（1 小时内不重复） | 原逻辑不检查时间，刷新线程挂掉后会静默使用陈旧候选池 | 无（`_POOL_CACHE_MAX_AGE_SEC`） |
| STEP2-2 | `scanner.run_scan` 信号注册 `_do_register` | 触发快照（RSI1H / ATR / EMA20 / 突破基准）改为按标的保存并读取；mode 取信号自身的 sig_type | 原写法 `'k1h' in dir()` 读到的是预算循环里最后一个标的的残留变量（已实测确认），所有信号的快照都是别的币的数据；breakout 基准还依赖残留 mode | 无（bug 修复） |
| STEP2-3 | `scanner.analyze_symbol` / `run_funding_arb_scan` | 删除未使用变量 `_chg`、`ok`（LLM 妖币分支）、`hours_to_settle`、`now_ts`、`_pending_score`；删除结果从未使用的 `get_open_interest_hist` 调用 | 清理死代码；资金费套利扫描每个候选少一次接口请求。`ok` 经核对并非信号丢失：结果经 sig_type/direction/reason 传出 | 无 |
| STEP2-DOC | `scanner.refresh_pool` 文档字符串 | 更正为与实际配置一致 | 原文档数字与配置矛盾 | 仅注释 |

## 审查发现的问题与处理

- **已处理（STEP2-4）：** `config.LLM_JUDGE_EXPLOSIVE_COIN` 改为 False（回滚：改回 True）。原因见下。
- 妖币 LLM 分支（`LLM_JUDGE_EXPLOSIVE_COIN=True`）对任何已通过的信号都会检测，命中后把信号类型强制改为 `range_bounce` 且方向强制为 LONG，可能覆盖原本的 SHORT 信号；同时依赖外部大模型输出，不属于币安官方数据。

---

# 第七轮（v3.87 逐模块审查·第 3 步 judges.py）：分支 `opt/step-3-judges`（基于 main 44da0b8f）

> 验证：静态检查无未定义变量；全量测试 158 通过 / 9 失败（既有过时测试，与改动前相同）。本轮只改注释与死变量/重复导入，不改任何判定逻辑。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP3-1 | `_judge_reversal_impl` `_rev_chg24_short` | 补注释，保留变量 | v3.64 起做空主通道有意写死 chg24 ≥ +5，该变量遗留未用 | 仅注释 |
| STEP3-2 | `_judge_reversal_impl` `fr_short_thresh` | 补注释，保留变量 | 做空资金费率检查有意不接入分层门槛（避免拦死做空） | 仅注释 |
| STEP3-3 | `_judge_pullback_impl` `sl_mult`；突破确认函数 `lo`/`v`；`judge_funding_arb` `price`；止损猎杀 `bar_close` | 删除未使用变量 | 死代码，不影响运行 | 无 |
| STEP3-4 | 反转函数 `ATR_TIER_MID_MIN`；资金费套利 `FUNDING_ARB_STRONG_RATE_*` | 删除重复导入（值与保留的导入相同） | 重复导入，后者覆盖前者，无行为差异 | 无 |

## 审查结论（未改，按设计保留）

- 回踩 LONG 专有的两段保护（60 日高点回撤 > 40% 禁止做多、60 日价格区间低 30% 分位禁止做多）：目的是避免在结构性下跌里做"反弹"。做空没有镜像，是因为对称的场景（涨到 60 日高位禁止做空）会直接与"超买后做空"的反转思路冲突。**是有意的非对称设计，未补镜像。**
- 回踩做空额外的涨幅下限与量比下限：有意设计。
- 突破：LONG / SHORT 的突破幅度、4H EMA 偏离 ±2%、量比、资金费率上限、RSI 限制均已镜像，未发现不对称。`rsi_healthy` 区间（做多 50~65，做空 35~50）是对称的。
- EMA 趋势：方向由 4H EMA20/EMA50 差决定，做空 RSI1H 上限固定 70、做多用配置值，是不对称的轻微细节，未改。
- 资金费套利：方向由费率正负决定，RSI4H 门槛按费率强度分层，LONG / SHORT 镜像完整。

---

# 第八轮（v3.87 逐模块审查·第 4 步 calc_sl_tp.py）：分支 `opt/step-4-sltp`（基于 main 2b6a6f8d）

> 验证：静态检查无未定义变量；新增 5 个单测通过；全量测试 163 通过 / 9 失败（9 个为既有过时测试，与改动前相同）。未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP4-1 | `calc_sl_tp._calc_sl` 结构止损保护 | 删除无效赋值 `sl_degraded = True`，补注释 | 函数最终返回的是 `degraded`，该赋值是遗留的遮蔽变量，从未生效。摆动点保护有意不触发降级（降级会让执行器减半仓位）。**行为不变** | 无 |
| STEP4-2 | `calc_sl_tp` TP1 截断后收紧止损（新增 `_tighten_sl_after_cap`） | 收紧后的最小止损距离由写死 2.5% 改为按波动层（低 2.5% / 中 3% / 高 5%），达不到则拒绝信号；仍要求不比原止损更宽 | 原逻辑会把中高波动币的止损收紧到噪声区间内（例如高波动层 2.5%~5%），而该步骤发生在最小距离保护之后，无人再纠正。**信号数量会轻微下降（预期）** | 无（逻辑修复；回滚需回退提交） |
| STEP4-3 | `calc_sl_tp` VP TP1 增强 | RR 门槛由写死 1.2 改为全局 `RR_MIN_TP1`（2.0，高波动层读 `RR_MIN_TP1_HIGH`=1.5） | 核对配置后确认 1.2 低于全局门槛，VP 路径会放过本该被拒绝的低 RR 单 | 无 |
| STEP4-5 | `calc_sl_tp` 文档字符串 | 列出 `entry_mode` 实际出现的全部取值及执行器处理方式 | 原文档只写 immediate / pending_order | 仅注释 |
| STEP4-6 | `executor._place_open_order` 参数校验失败出口 | `return None` 改为 `return None, 原因` | 调用方按二元组解包 `order_id, err = ...`，原写法在参数校验失败时会抛 TypeError。与其它失败出口一致 | 无（bug 修复） |

新增 `tests/test_opt_v387_step4.py`（5 个用例）：中波动层收紧后止损距离 ≥ 3%、2.7% 在中波动层被拒绝而低波动层通过、高波动层要求 ≥ 5%、不允许比原止损更宽、做空对称。

## 审查结论（未改）

- OPT-8 / OPT-9：两个开关均为 False，代码以 `try: import … except: False` 读取，读不到也是关闭；按决定不加硬锁，由 config 控制。
- 所有出口的返回字典字段一致；`entry_mode` 除 `pending_order` 外，执行器统一按 GTC 限价单提交（breakout 为 GTD，reversal 的 pending_order 为 GTX），处理正确。
- 最终出口不加统一 RR 检查：改动面覆盖所有路径，风险高于收益。

---

# 第九轮（v3.87 逐模块审查·第 5 步 signal_watcher.py）：分支 `opt/step-5-watcher`（基于 main eaf167ef）

> 验证：静态检查无未定义变量；全量测试 163 通过 / 9 失败（9 个为既有过时测试，与改动前相同）。未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP5-1 | `judges._judge_range_bounce_impl` 输出 | 新增 `c_low`（区间下沿）字段，只加字段不改判定 | `_survival_check` 的 range_bounce 做多撤单条件读 `reason['c_low']`，此前从未被输出，值恒为 0，「跌破区间下沿则撤单」从未生效 | 无 |
| STEP5-6 | `signal_watcher._survival_check` ema_trend 分支 | 优先使用实时 EMA20，触发快照仅兜底 | 与 pullback 分支对齐；快照 EMA20 在价格移动后会偏离真实支撑/压力位 | 无 |
| STEP5-7 | `signal_watcher._recheck_one` | 删除未使用的 `_entry_clamped` | 死变量 | 无 |

## 审查结论（未改）

- `triggered=True` 是在价格进入触发区间、开始下限价单流程之前就置位的，因此 BTC 守护与 `_recheck_one` 都不处理已触发条目；已向交易所挂出的限价单不在守护撤销范围内。属有意设计，记录在案。
- BTC 守护对所有 LONG 挂单一刀切，不套用 `_recheck_one` 的三条豁免：按决定保持。
- 低分撤单实际只作用于 weak / 未知强度信号：按决定保持。
- `_tp12` 取而不用（TP1 可达性未接入）：按决定保留，待实盘数据积累后再定义「不可达」。
- funding_arb 不进入挂单监控（市价立即入场），BTC 守护不会撤它。
- OPT-13 关闭时 `compute_entry_range` 突破分支与原逻辑一致（读代码确认，无单测）。

## 追加修复（STEP5-8，🔴）

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP5-8 | `signal_watcher._recheck_one` B1 段（价格位移重挂）、`add_signal`、新增 `_reorder_replace` | ① 重挂调用补齐 `on_trigger` 和全部触发快照字段，去掉不存在的 `loop` 参数；② 改为先注册新单（`add_signal` 新增 `replace=True`，覆盖同标的旧条目），成功后才撤旧单的交易所限价单；③ 失败日志由 debug 改为 warning「重挂失败，保留原挂单」 | 原写法先撤旧单再调用参数错误的 `add_signal`（多 `loop`、缺必需的 `on_trigger`），必抛 TypeError 并被吞掉，旧单已撤，信号静默丢失，日志却写「保留原挂单」。**价格位移重挂自上线以来从未真正工作过** | 无（bug 修复；行为会真正生效，需观察） |

新增 `tests/test_opt_v387_step5.py`（3 个用例）：撤旧单发生时队列里已是新条目、注册失败时旧单保留且不撤单、不带 replace 时重复注册仍被跳过。

**合并后请观察：** 日志中「价格位移」「重算入场参数并重挂」出现的频率，以及「价格位移重挂失败，保留原挂单」的 warning 是否出现。只对 reversal / breakout 生效，价格偏离原入场价超过 1 ATR 才触发。

---

# 第十轮（v3.87 逐模块审查·第 6 步 executor.py）：分支 `opt/step-6-executor`（基于 main 405d78bd）

> 验证：静态检查无未定义变量；新增 6 个单测通过；全量测试 172 通过 / 9 失败（9 个为既有过时测试，与改动前相同）。二次验证平仓失败出口没有单测（需要大量模拟下单环境）。未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP6-1 | `executor._entry_recheck` | 删除 v3.78「逆势拉盘扫入就平仓」规则及其死变量 `_fill_candle_dir`、`rsi_1h` | 规则依赖快照里的 `vol_ratio` / `prev_close` / `entry_price_snap`，调用方构造的快照从未提供这三个字段，条件恒不成立；无数据支撑，按评审决定删除。现在该函数只在资金费率极端（多头 >0.5% / 空头 <-0.5%）时判定平仓 | 无 |
| STEP6-2 | `executor._execute_async` 二次验证 CLOSE 平仓失败出口 | 平仓抛异常时：发送告警（symbol、方向、成交价、数量、复核结论、错误原因，注明需人工确认），然后 `return False`；不写持仓记录、不挂保护单 | 原写法只发通知，随后用回退价格 0 计算盈亏并 `return True`，调用方以为流程已完成，而仓位无保护、也不在系统记录里。**注意：失败后仓位不被机器人跟踪，必须人工处理** | 无 |
| STEP6-3 | `executor._place_guards_async` 止损数量；新增 `_round_sum_qty` | `sl_qty = tp1_qty + tp2_qty` 相加后按步长精度四舍五入 | `binance_api.place_sl` 直接用 `f"{quantity}"` 拼接数量，两个浮点数相加会产生 0.30000000000000004 之类的噪声，可能被交易所以精度错误拒绝，止损单提交失败 | 无 |
| STEP6-4 | `executor` 死变量 | 删除 `LIQUIDITY_GATE_MIN_U`（`_account_final_check` 内）、`exec_type`（`_wait_fill_ws`）；保留 `_max_lev_for_notional`（涉及杠杆，不碰） | 死代码 | 无 |

## 审查结论（未改）

- 其余返回元组的函数（`_precheck`、`_check_correlation`、`_check_liquidity_gate`、`_validate_order_params`、`_account_final_check`、`_place_open_order`、`_entry_recheck`）用语法树核对过，所有出口的元组长度与调用方解包一致，没有其他解包错误。
- 保护单提交失败只告警、不重试：依赖持仓管理 5 分钟巡检兜底，按决定保持。
- `_precheck` 顺序：结果正确，按决定不动。余额快速预检（20U）已在第 1-9 项，「余额检查太晚」已不存在。

## 追加修复（STEP6-5）

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP6-5 | `executor._round_qty` | `floor(qty/step)*step` 改为 `floor(round(qty/step, 10))*step` | 浮点除法会多丢一个步长：0.3/0.1=2.9999999999999996，floor 得 2，数量变成 0.2。先取整消除噪声再向下取整，仍保证不超过实际数量。**影响开仓数量和止盈数量**：边界值上的下单数量会多一个步长（回到本应有的值） | 无（回滚需回退提交） |

新增 5 个 `_round_qty` 单测：0.3/0.1 边界、0.7、1.1、0.29/0.01 等浮点边界，仍然向下取整，结果不超过输入，步长为 0 的回退。

`_round_price` 使用同样的 `floor(price/tick)*tick`，可能有同类问题（价格差一个 tick），本轮未改，仅记录。

---

# 第十一轮（v3.87 逐模块审查·第 7 步 position_manager.py）：分支 `opt/step-7-pm`（基于 main 72d3b65e）

> 验证：静态检查无未定义变量；新增 1 个单测（持仓管理取整与 executor 一致）；全量测试 178 通过 / 9 失败（9 个为既有过时测试，与改动前相同）。未回测、未跑模拟盘。**本轮只做了无风险项，涉及平仓 / 止损 / Algo 单的发现全部留待决策。**

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP7-1 | `position_manager._round_qty` | `floor(qty/step)*step` 改为 `floor(round(qty/step, 10))*step`，与 executor 对齐 | 同样的浮点误差会多丢一个步长。本模块用它算平仓数量、止损/止盈补挂数量、加仓数量、挂单调整数量 | 无 |
| STEP7-5 | `_execute_trail_sl`、日志后台任务、MFE 初始化 | 删除未使用的 `direction` 赋值和异常变量 `_e` | 死变量，无行为影响 | 无 |

## 审查结论

- **funding_arb 路由隔离：成立。** `_check_time_stop` 与 `_check_exit_signal` 内部开头就跳过 funding_arb，健康检查在 `_patrol_loop` 里显式跳过，`patrol_once` 对加仓、TP2 收紧也显式跳过，专用巡检 `_patrol_funding_arb` 只在 `patrol_once` 里调用。
- **加仓后 Algo 止损同步：仍然有效。** `_maybe_pyramid` 加仓成功后立即把止损设到原入场价（保本线），并调用 `_execute_trail_sl` 同步交易所 Algo 单。
- **`_adjust_sl_tp_once` 止损单向约束：仍然有效。** LONG 只允许上移、SHORT 只允许下移，反方向分支明确 `pass`；上移还需超过 0.3×ATR 才执行。（函数文档字符串写的是「SL 双向调整」，与实现不符，是过时描述。）
- **OPT-1 / OPT-2 关闭时行为与原来一致：** OPT-1 关闭时阈值回退固定 2%；OPT-2 关闭时 `tp1_protect_action` 对「止损仍在入场价亏损侧」返回 `move_sl`（保持原有保本移动），对「止损已在有利侧」返回 `none`（原代码此时也不动作）。读代码 + 已有单测确认。

## 追加修复（按决定实施）

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| STEP7-2 | `position_manager._force_close_static`（新增 `_pos_still_open`） | 在 `_closing_set` 之后、任何平仓操作之前，重新从 state 读取该仓位：读不到或 status 不是 open 就直接返回（debug 日志「持仓已不存在或已非 open，跳过重复平仓」）；读状态本身失败时按「仍然 open」处理，不放弃平仓 | `PositionManager._patrol_loop` 与 main 里的 `patrol_once` 每 5 分钟都跑时间止损 / 突破失败 / 反向信号检查；`_closing_set` 只防同时执行，不防先后各执行一次，第一个平仓完成后第二个拿旧快照仍可能再平一次。`_force_close` 方法是 `_force_close_static` 的包装，同一入口 | 无 |
| STEP7-3 | `_adjust_sl_tp_once` 文档字符串 | 「SL 双向调整」更正为「只允许向有利方向移动，幅度需 > 0.3×ATR」 | v3.6 起已禁止外扩，文档过时 | 仅注释 |
| STEP7-4 | `_patrol_loop` 开头 | 加注释说明巡检职责分布在 `_patrol_loop` 与 `patrol_once` 两处 | 避免以后只保留其中一个循环时丢功能 | 仅注释 |

新增 `tests/test_opt_v387_step7.py`（5 个用例）：open 仓位正常平仓；仓位已删除则跳过；status 非 open 则跳过；两个巡检先后调用时第二次不再平仓；读状态失败时不阻断平仓。

## 审查结论（未改）

- `all_mp`、`k15`（及 `k15_raw`）未使用但带副作用的接口调用：删除会改变异常路径，且前者在平仓决策路径上，按决定不动。
- 全部 7 步的审查结果与分支对应关系见本文件各轮记录。

---

# 第十二轮（v3.88 OPT-14 清算区相对强度）：分支 `opt/liq-relative-v388`（基于 main 7cc82d95）

> 验证：静态检查无未定义变量；新增 7 个单测通过（含「开关关闭时入场价与 TP1 与原公式完全一致」）；全量测试 190 通过 / 9 失败（9 个为既有过时测试，与改动前相同）。**开关默认关闭**，未回测、未跑模拟盘。

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| OPT-14a | `calc_sl_tp._entry_pullback`（LONG / SHORT） | 清算区锚点权重 = 2 × 相对强度 / 10%，截断到 [0.5, 3.0]；分母同步改为 `权重 + 2`，保证仍是加权平均 | 相对强度 = 清算区金额 / 24H 成交额。原权重固定为 2，不区分清算区对该币的实际影响力；同样 500 万 U 对 BTC 是噪声、对小币能推动整个市场 | `OPT_LIQ_RELATIVE_STRENGTH` |
| OPT-14b | `calc_sl_tp._calc_tp` TP1 | TP1 距清算区前沿缓冲按相对强度分档：≥20% 用 0.2ATR、5%~20% 用 0.35ATR、<5% 用 0.5ATR | 原缓冲固定 0.3ATR | 同上 |
| OPT-14c | `calc_sl_tp.calc_sl_tp` | 从 `signal["ind"]["vol24_usd"]`（scanner 已注入的 ticker `quoteVolume`）传入成交额 | 无需新增接口 | — |

回退规则：成交额或清算金额缺失时，权重回到 2、TP1 缓冲回到 0.3ATR，与原公式完全一致。TP2 缓冲（0.5ATR）未改。新增 `shared/opt_v388.py`（3 个纯函数）、`tests/test_opt_v388.py`。

## 与需求描述的出入（已按代码实际情况处理）

- 原入场权重是固定的 2，代码里没有「金额 / 200 万 U」的公式。
- 原 TP1 缓冲是 0.3ATR，0.5ATR 是 TP2 的缓冲。因此「弱」档的 0.5ATR 比原来更保守。

## 全系统固定金额门槛排查（未改，待决策）

| 位置 | 固定金额 | 用途 |
|---|---|---|
| `judges._fill_reversal_liq` | ≥300 万 / ≥100 万 U | reversal 仓位上限 0.75 / 0.5 |
| `judges` pullback 高波动层（约 537、540 行） | 200 万 U | 清算区挂单门槛 |
| `judges` breakout（约 1014、1087 行） | 500 万 U | 上方清算压力：评分「清算通道畅通」、强度降为 medium |
| `judges` reversal 强度（约 1541 行） | 300 万 U | 强信号判定 |
| `judges` OI 背离清算墙（约 3186 行） | 50 万 U | 拒绝入场 |
| `calc_sl_tp` OI 背离止损（约 972 行） | 30 万 U | 止损外移（**涉及止损**） |
| `scanner._calc_route_quality`（约 140 行） | ≥300 万 / ≥100 万 U | 路线质量评分 |
| `signal_watcher._calc_pending_score`（约 320 行） | ≥300 万 / ≥100 万 U | 挂单竞争评分 |
| `coinglass_api`（约 183 行） | 5000 万 U | CVD 净卖出扣分 |
| 流动性下限：`judges`（128、1114 行）100 万 U、`scanner`（774 行）50 万 U、`config.UNIVERSE_MIN_VOLUME_24H` 300 万 U | — | 准入门槛，本身就是以成交额衡量，**建议保持绝对值** |
| `executor` 流动性深度 `LIQUIDITY_GATE_MIN_U` 1 万 U | — | 盘口深度下限，与名义仓位挂钩，**建议保持** |
| `risk_manager._SHOCK_LIQ_THRESH` 5 亿 U | — | 全市场强平量，非单币指标，**建议保持** |

## 第 13 轮（v3.88 续：相对强度分母对比 + TP1 入场锁定，分支 opt/liq-relative-v388）

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关 |
|---|---|---|---|---|
| OPT-14d | `shared/opt_v388.liq_metrics`、`calc_sl_tp._collect_liq_metrics`、`executor`（持仓记录 + `trade_log.jsonl`） | 每次 `calc_sl_tp` 计算时，记录入场侧、止盈侧最近清算区的：绝对金额、`rel_vol24`（÷24H 成交额）、`rel_oi`（÷持仓名义价值 USDT）、`concentration`（÷同方向清算图总量）、`zone_count`、`concentration_valid` | **为什么三种分母都记**：三者语义不同（流动性背景 / 杠杆脆弱度 / 清算图内部拥挤度），现有数据无法判断哪个与胜率相关，先同时记录，积累 2~4 周后用自己的实盘结果对比再定分母和阈值，避免拍脑袋。三者独立计算，任一缺数据只让对应字段为 None | 只记录，不参与决策；无独立开关 |
| OPT-14d-说明 | 同上 | `oi_now` 是合约张数，记录前乘以标记价换算成 USDT；单区时 `concentration` 恒为 1.0，已用 `concentration_valid=False` 显式标记，分析时应剔除 | 避免量级错误和无区分度样本污染结论 | — |
| OPT-14d-频率 | 同上 | 写入频率：每次 `calc_sl_tp` 调用产生一次（含开仓前计算与 30 分钟重算），但只有开仓时的值随持仓和成交日志落盘（字段 `liq_metrics`、`tp1_liq_buffer_atr`）；重算结果不覆盖 | 一笔交易一条记录，便于与结果（止盈/止损/超时）对应 | — |
| OPT-14e | `calc_sl_tp._calc_tp`、`position_manager._adjust_sl_tp_once`、`executor` | 开仓时把实际采用的 TP1 清算区缓冲（ATR 倍数）存入持仓；30 分钟重算时直接沿用，不按最新清算图重算 | **为什么 TP1 锁定**：TP1 是开仓时风险收益评估的一部分，中途跟随清算图变化等于改变入场依据，行为前后不一致且难以归因；同时补上上一版“重算缺少成交额、缓冲退回 0.3ATR”的不一致。旧仓位无该字段时回到原逻辑 | `OPT_LIQ_RELATIVE_STRENGTH`（默认 False）；分母选择 `LIQ_REL_DENOM`（默认 vol24） |

验证：pyflakes 无新增问题；`tests/test_opt_v388.py` 16 项通过；全量 pytest 仅剩已知 9 个过期旧测试失败。未回测、未实盘。
合并时机：等拿到 2~4 周 `liq_metrics` 数据、确定分母与阈值之后再合并 main。

## 第 14 轮（v3.88 续：分歧度门控 + 数据降级回退 + OI 背离全阳拒绝，分支 opt/liq-relative-v388）

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关状态 |
|---|---|---|---|---|
| OPT-14f | `shared/opt_v388.liq_weight_gated`；`calc_sl_tp._entry_pullback`（LONG / SHORT 两处） | 动态权重叠加分歧度门控：清算区锚点（LONG 取区间上沿，SHORT 取下沿）与 EMA20 的偏离 > `OPT_DIVERGENCE_GATE_ATR`（1.5）× ATR 时，权重上限从 3.0 压到 `OPT_DIVERGENCE_GATE_WEIGHT_CAP`（2.0）；未触发时仍是原 clamp(0.5, 3.0)。门控只会压低权重，不会抬高；ATR 或 EMA20 缺失时不触发 | 外部多模型意见（A2）：高集中度在极端行情里可能是陷阱而不是锚点，清算区与趋势/成交密集区严重分歧时，不应让清算区主导入场价 | 受 `OPT_LIQ_RELATIVE_STRENGTH` 控制（默认 False），不新增总开关 |
| OPT-14g | `shared/liq_cache`（新增 `src_level` 标记与只读函数 `get_liq_src_level`）；`calc_sl_tp.calc_sl_tp`；`_entry_pullback` | 清算图来源为二级（ByKaranteli）或三级（Coinglass liq_orders）降级时，动态权重强制回到固定值 2。**说明：原代码并没有"降级级别"标记**（只有三级全失败时的 `sl_degraded`），所以在缓存字典里新增了 `src_level`（1=主路径，2、3=降级），多个 range 取最差级别；缓存无记录时按 0（未知）处理，不触发回退。新增字段不改变任何已有读取方（他们只按方向取区） | 外部意见（A2）：权重表达的是信心，等于信号强度 × 数据可信度；降级数据不应还能把权重拉到 3.0 | 受 `OPT_LIQ_RELATIVE_STRENGTH` 控制（默认 False） |
| OPT-15 | `calc_sl_tp.calc_sl_tp` oi_divergence 入场价分支 | SHORT：近 5 根无收阴 K 时直接返回 `{"error": ...}` 拒绝信号，不再用 `mark×0.992` 兜底；LONG：近 5 根无收阳 K 时同理拒绝，不再用 `mark×1.008` | 原兜底得到的入场价低于市价，空单限价单会立刻成交（等于市价追空），且与清算图锚点无关；多个外部模型一致否定该兜底 | `OPT_OID_REJECT_ALL_BULL`，**默认 True（唯一默认开启、改变现有行为的项）**；设为 False 恢复原兜底 |

本轮明确不改（写入记录，避免重复讨论）：
- TP1 缓冲分档（0.2/0.35/0.5）：继续只记录，开关保持关闭。
- 分母选择（vol24 / OI / concentration）：三种继续都记录，不选边。
- ⑧ OI 背离止损外移的固定金额门槛（30 万 U）：永久保留固定值，不改。
- 其余固定金额门槛：等影子运行数据，不改。

验证：pyflakes 无新增问题；`tests/test_opt_v388.py` 25 项通过（本轮新增 9 项）；全量 pytest 仅剩已知 9 个过期旧测试失败。未回测、未实盘。

## 第 15 轮（v3.88 续：开启两个开关 + 价格/数量取整浮点修复，分支 opt/liq-relative-v388）

| 编号 | 在哪里 | 改了什么 | 为什么改 | 开关状态 |
|---|---|---|---|---|
| OPT-5 开启 | `config.OPT_BREAKOUT_CLOSED_BAR`；`judges`（突破判断） | 默认值由 False 改为 **True**：突破只用已闭合 1H K 线 | 未闭合 K 线可被重绘成假突破；代价是突破信号晚约一根 K 线 | **默认开启**（用户决定）；设 False 恢复 |
| OPT-6 开启 | `config.OPT_FUNDING_WINDOW_PER_SYMBOL`；`scanner`（资金费窗口） | 默认值由 False 改为 **True**：资金费窗口按标的自身结算时间计算，不再整轮停扫 | 原逻辑整轮停扫，1H/4H 结算的合约没有保护，还白白损失扫描轮次；代价是扫描频率增加，需观察接口限速 | **默认开启**（用户决定）；设 False 恢复 |
| OPT-16 | `executor._round_price`、`position_manager._round_price`、`shared/binance_api.round_price`、`round_quantity` | 取整前先 `round(x/步长, 10)` 消除浮点误差再向下取整（与 `_round_qty` 的修法一致） | 原写法 `floor(price/tick)`：0.3/0.1=2.9999999999999996，floor 得 2，价格少一个 tick；数量同理少一个步长。正常价格结果不变，只修边界 | 无开关（纯精度修复） |

注意：OPT-16 影响所有下单和挂单价格的取整（含入场、止损、止盈的挂单价）。只修浮点边界，不改变任何价格的计算逻辑。

验证：pyflakes 无新增问题；`tests/test_opt_v388.py` 27 项通过（本轮新增 2 项）；全量 pytest 仅剩已知 9 个过期旧测试失败。未回测、未实盘。

---

## 设计决策记录（2026-10-20，ai_panel 多模型评审结论）

### OPT-14b RR 兜底方向（已拍板）

**背景：** ai_panel 5模型对「X缩小时RR方向」存在分歧，经用户逐公式判断后定案。

**公式确认：**
```
TP1 = 清算区前沿 - X × ATR
```
- X 越小 → TP1 越靠近清算区前沿 → TP1 离入场越远 → **RR 上升**
- X 越大（弱区 X=0.5）→ TP1 越靠近入场价 → **RR 可能 < 1.5**

**结论：** claude-opus-5 推导正确，其余 3 个模型（glm-5.2、claude-sonnet-4-6、o4-mini）将 X 方向理解反了（gpt-6.1-sol 结论正确但表述模糊）。

**OPT-14b 实现规则（待实现，开关开启时生效）：**
1. 按相对强度分档计算 X（强≥20% → 0.2ATR、中 5~20% → 0.35ATR、弱<5% → 0.5ATR）
2. **若 X = 0.5（弱区），校验 RR ≥ 1.5**
3. 若 RR < 1.5，**往下调 X**（不是往上），直到 RR 刚好满足约束
   - 往下调 X = TP1 更远 = RR 上升，方向正确
   - 禁止往上调 X 来「凑 RR」，那等于用更近的止盈换账面合规，是隐性亏损
4. X 下调后若仍无法满足 RR（如止损太宽），直接拒绝信号

### 其他 ai_panel 结论摘要

| 题目 | 结论 | 行动 |
|------|------|------|
| B2 全阳拒绝 | 方案B 4/5票，直接拒绝信号 | 维持 OPT_OID_REJECT_ALL_BULL=True |
| C1 权重联动 | X/Y 数学等价，Y 更直观 | 实现时用方案Y（独立变化），无需纠结 |
| D2 多空非对称 | 空单 0.8→0.5 风险更大 | OPT-3 若开启，空单系数须单独验证 |
| A1 分母选择 | 等 liq_metrics 积累 2~4 周 | 暂不选边，三种分母继续同时记录 |
| A2 权重分档 | 阈值无法收敛，等实盘定 | 同上 |
| D1 数据降级 | 相对强度比固定门槛更危险 | 降级时强制回退默认值（已由 OPT-14g 实现）|
