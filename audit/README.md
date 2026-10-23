# fx 合约机器人代码审查：总览与待处理清单

被审查对象：`fx/`（Binance 合约机器人）的精简快照，基于原仓库 main 分支现状。

| 步骤 | 报告 | 主题 |
|---|---|---|
| 01 | [01_framework.md](01_framework.md) | 框架设计 |
| 02 | [02_startup.md](02_startup.md) | 启动与恢复 |
| 03 | [03_execution.md](03_execution.md) | 下单执行 |
| 04 | [04_risk.md](04_risk.md) | 风控 |
| 05 | [05_signal.md](05_signal.md) | 信号逻辑 |
| 06 | [06_stability.md](06_stability.md) | 稳定性与安全 |
| 07 | [07_strategy.md](07_strategy.md) | 交易策略闭环 |

## 阅读须知

- 所有结论来自读代码，**没有运行验证**。标"推断"的条目需要在测试环境或测试网复现后再定论。
- 阅读覆盖度并不均匀。`executor.py`、`position_manager.py`、`scanner.py`、`judges.py`、`config.py` 等大文件是分段读取加关键字检索，没有逐行通读；各报告开头的"阅读说明"写明了每一步实际读到的范围。
- **尚未审查的部分：** 各策略 judge 的具体入场条件、三个入场价函数（`_entry_pullback`、`_entry_breakout`、`_entry_reversal`）、`position_manager._adjust_sl_tp_once`、运行期保护单存活检查、`funding_arb` 的完整闭环。
- 严重程度含义：🔴 可能直接造成资金损失、仓位失控或凭据泄露；🟡 在特定条件下会出问题，或削弱保护；🔵 可维护性与改进建议。

## 待处理清单（按严重程度排序）

### 🔴 严重

| # | 事项 | 涉及文件 | 来源 |
|---|---|---|---|
| 1 | 硬编码的第三方密钥与网关令牌：CoinGlass key、OpenClaw 网关 Bearer 令牌、推送目标标识；需轮换并清理 git 历史 | `shared/clawby_adapter.py:70`、`shared/notifier.py:31、35`、`.env.example` | 06 |
| 2 | 接口失败返回 `[]`，被当成"交易所无持仓"，启动与 WS 重连时会清空本地持仓记录 | `main.py:476-514`、`shared/binance_api.py:598-606、622-630` | 02 |
| 3 | 孤儿仓位（交易所有、本地无）只补保护单不建档，方向取本地默认 `long`，补挂后 `update_position` 对不存在的 `pid` 失败 | `main.py:517-618`、`shared/state.py:178-190` | 02 |
| 4 | 启动对账补挂会重复挂 SL，只缺 SL 时还会重挂 TP，新 SL 的 id 不回写，每次重启或重连叠加 | `main.py:531-618` | 02 |
| 5 | `pos_state` 损坏或为空文件后以空持仓重启，没有从交易所重建记录的路径 | `shared/state.py:58-88、702-731` | 02 |
| 6 | 两套守护互不识别，`main.py` 无单实例保护，可能同时运行多个实例 | `main.py:119-123`、`daemon_start.py:316-402`、`watchdog.sh`、`DEPLOY.md` | 01 |
| 7 | 重复开仓防护在下单之后才拦截（只防重复建档），`executor` 无按标的互斥 | `executor.py:174-323、1198`、`shared/state.py:155-170` | 03 |
| 8 | funding_arb 线程与主扫描不共享锁，先检查后执行存在秒级窗口，可能重复入场或短暂超仓 | `scanner.py:4196`、`executor.py:1432-1445` | 03 |
| 9 | `_retry` 吞掉所有异常、错误码丢失，`-4024`/`-2019` 处理与保证金暂停成为死代码 | `shared/binance_api.py:154-204`、`executor.py:753-798` | 03 |
| 10 | 保护单只部分成功时仍标记 `open`，没有重试或紧急平仓，调用方不检查 `protection_status` | `executor.py:1003-1038`、`position_manager.py:3042-3053`、`main.py:607-616` | 04 |
| 11 | 熔断被配置为 9999，实际关闭；默认值在三处互相矛盾 | `config.py:258`、`shared/state.py`（`record_pnl`）、`shared/risk_manager.py:213` | 04 |
| 12 | 日亏损只累计亏损、不与盈利抵消，且不含手续费和资金费；记账路径分散，可能漏记或重复 | `shared/state.py`（`record_pnl`）、`executor.py:1155-1158`、`position_manager.py:1073、2091` | 04 |

### 🟡 中等

| # | 事项 | 涉及文件 | 来源 |
|---|---|---|---|
| 13 | 两份补挂逻辑不一致（锁、已有 TP 检查、数量校验、止损倍数），并发时可能重复补挂 | `main.py:517-618`、`position_manager.py:2883-3110` | 02 |
| 14 | 部分成交后被撤单或过期，本地记录被直接删除，残留无保护小仓位 | `position_manager.py:1780-1783、1821-1838` | 02 |
| 15 | 空文件被当作合法；备份失败仍覆盖原文件；其余四个状态文件（含熔断器）损坏时无备份直接重置 | `shared/state.py:77、84`、`_ensure_files` | 02 |
| 16 | 对账假设对冲模式且按 `symbol` 建索引，缺少启动时持仓模式校验；单向模式下会清空记录 | `main.py:479-499`、`shared/binance_api.py:391-395` | 02、03 |
| 17 | `systemd` 配置与双 fork 守护不兼容（推断）；两套守护的环境、重启上限不一致；`DEPLOY.md` 混有无关项目内容 | `DEPLOY.md`、`daemon_start.py`、`watchdog.sh` | 01 |
| 18 | 状态文件分散在多个模块，部分非原子写，路径常量失效；线程锁不跨进程；原子写无 `fsync` | `shared/state.py`、`shared/risk_manager.py`、`shared/trigger_detector.py:278`、`scanner.py:2009`、`executor.py:781` | 01 |
| 19 | `main.py` 含大段业务逻辑，模块间循环依赖、导入私有函数，`signal_watcher` 直接下止损单 | `main.py:435-660`、`position_manager.py`、`shared/signal_watcher.py:930、1422`、`scanner.py:3142、4194` | 01 |
| 20 | 配置不集中：URL 重复、魔法数字、默认值分散、多处常量标注"代码未引用" | `config.py`、`shared/binance_api.py:56`、`shared/coinglass_api.py:229`、`shared/vol_regime.py:38-39` | 01、04 |
| 21 | `async` 函数内同步 HTTP 与磁盘 I/O，WS 重连回调也在其中 | `main.py:476-492、582、728-731`、`position_manager.py:1769、1774、1823` | 01、02 |
| 22 | 常开 `tracemalloc`、启动时运行时 `pip install` | `main.py:52、81-102` | 01 |
| 23 | 交易所挂单兜底检查在接口失败时静默通过 | `executor.py:192-207` | 03 |
| 24 | 对业务拒绝也重试；没有限频处理；无 `clientOrderId` 幂等；MARKET 单写超时对账找不到 | `shared/binance_api.py:154-204、575-589` | 03 |
| 25 | 精度：默认值静默兜底；未处理最小金额与市价数量限制；小数字符串可能出现科学计数法（推断） | `executor.py:113-116、695-696`、`shared/binance_api.py:922-923` | 03 |
| 26 | 对冲模式下 `reduceOnly` 不发送；`_validate_sl_order` 不在止损实际路径上 | `shared/binance_api.py:395、557-561` | 03 |
| 27 | 应急补止损（撤单结果未知场景）未取整、成功判断失真、不补 TP、不建档 | `shared/signal_watcher.py:1400-1427` | 03 |
| 28 | 回撤保护与净 Delta 异常时放行；高水位更新策略矛盾；净 Delta 未计入待开仓名义 | `executor.py:270-305、584-596`、`shared/risk_manager.py:503-505` | 04 |
| 29 | 多处"出错即放行"与"出错即拒绝"口径不统一 | `shared/risk_manager.py:692-694、1186-1190`、`executor.py:649-651` | 04 |
| 30 | 杠杆一律设为最大值，查询失败回退 10 并覆盖原设置；终检用 10 倍估算；总名义上限 5 倍与全仓强平风险需重估 | `executor.py:145-157、566、657-675`、`config.py:225` | 04 |
| 31 | 多个 judge 把未闭合 K 线当已收盘使用，`vol_ratio` 用未闭合量 | `shared/judges.py:511、1600-1602、1929、1953、2011、2028-2029`、`scanner.py:561-562、1274-1292` | 05 |
| 32 | 挂单竞争替换直接操作 `signal_watcher._watched` 并 `cancel_all_orders`；撤单失败仍移除条目 | `scanner.py:3032-3065` | 05 |
| 33 | `maintain_pending_order` 读取不存在的 `open_ts` 字段，超时撤单是死代码；出错即保留；挂单维护分散在四处 | `scanner.py:1728、1832-1834`、`position_manager.py:1812-1838` | 05、07 |
| 34 | 过期时间多处独立计算，`immediate` 模式的 GTC 入场单无过期 | `executor.py:741-747、1769-1783`、`scanner.py:2919、3108`、`config.py:318-319` | 05 |
| 35 | 评分：taker 重复计分；分数被常量覆盖；周末门槛写死；两套评分并存；LLM 参与打分且同步阻塞、不可复现 | `scanner.py:2660-2672、2730、2736、2762-2771、3716-3764`、`config.py:188、194` | 05 |
| 36 | 断线期间的 REST 补偿是死代码（`on_disconnect` 从未注册）；重连回调 fire-and-forget；退避无抖动 | `shared/ws_listener.py:207-208、355-362、377-378`、`main.py:731` | 06 |
| 37 | 内部后台任务不在 `_watchdog` 监控内；没有 `threading.excepthook`；守护重启无指数退避 | `main.py:780-857`、`position_manager.py:386-389`、`shared/signal_watcher.py:1484-1597`、`daemon_start.py:324-399` | 06 |
| 38 | `os.umask(0)` 使日志与状态文件权限过宽 | `daemon_start.py:422` | 06 |
| 39 | `watchdog.sh` 把令牌与消息拼进 `python -c`；`.env.example` 与实现不符（缺必填项只警告）；日志含 listenKey 前缀与签名异常文本；订单 ID 与余额进入日志和推送 | `watchdog.sh`、`.env.example`、`config.py:21-26`、`shared/ws_listener.py:342`、`shared/binance_api.py:199` | 06 |
| 40 | 策略与市场状态匹配只扣分不禁止，`pullback` 总是兜底；judge 异常处理需核实 | `scanner.py:312-338、1229-1255`、`shared/judges.py:149-170` | 07 |
| 41 | TP2 最小盈亏比 2.5 与配置 3.0 冲突；最小止损距离表与 `_FA_SL_MULT` 写死 | `shared/calc_sl_tp.py:88、707、1315`、`config.py:278` | 07 |
| 42 | `TP_SL_ADJUST_SL_WIDEN_ATR` 允许止损外扩，可能突破开仓时的风险上限（需核实）；`TP_MIN_RR_LIVE` 主动平仓需评估 | `config.py:391-394、411`、`position_manager.py:3235、3586-3598` | 07 |
| 43 | `tp2_quantity` / `tp2_qty` 字段名漂移；保护单撤销失败只通知 | `position_manager.py:671、698-705、763-768、2971` | 07 |
| 44 | 复盘数据：盈亏口径不含手续费与资金费；信号与成交缺少关联字段；JSONL 多写者无 schema；备份清单漏掉复盘数据 | `position_manager.py:1025、1030、2080-2085`、`trade_log.jsonl`、`DEPLOY.md` 第 10 节 | 07 |

### 🔵 建议改进

| # | 事项 | 涉及文件 | 来源 |
|---|---|---|---|
| 45 | 超大文件按职责拆分 | `position_manager.py`、`scanner.py`、`executor.py` | 01 |
| 46 | `get_order` 失败返回 `{}`，"查询失败超 24 小时清除"分支是死代码；`CANCELED/EXPIRED` 无 WS 实时处理 | `position_manager.py:1803-1808`、`ws_listener.py` | 02 |
| 47 | `PreOrderError` 的重试与 5 分钟方向冷却冲突（推断）；开仓"不重试"的文档与实现不一致 | `shared/signal_watcher.py:1123-1135、1233-1243`、`executor.py:689` | 03 |
| 48 | 残留的失效风控常量需清理或接入；日重置职责过多 | `config.py:315、478-482`、`shared/risk_manager.py:362-368` | 04 |
| 49 | 路径质量分与 `final_score` 两套评分并存，分值表缺少数据依据；hub 路径 K 线是否含未闭合需核实 | `scanner.py:295-372、1597-1600、3695-3712` | 05 |
| 50 | `DEPLOY.md` 克隆地址不应带令牌；LLM 中转站地址写在启动脚本；两套日志轮转并存 | `DEPLOY.md`、`daemon_start.py:335、232-246`、`watchdog.sh`、`main.py:140-145` | 06 |
| 51 | TP1 后剩余仓位的止损位置（原始 vs 保本）属于策略选择，建议用历史数据评估；时间止损等检查在两处重复 | `position_manager.py:722-733、779-781`、`config.py:398-401` | 07 |

## 建议的处理顺序

1. **先处理凭据：** 第 1 项。轮换三个已泄露的值，清理历史。
2. **再处理会直接影响实盘持仓安全的缺陷：** 第 2、3、4、5 项（启动与恢复）。这几项互相叠加，网络抖动一次就可能让持仓失去管理。
3. **然后处理下单侧的去重和错误处理：** 第 6、7、8、9 项。
4. **随后补风控：** 第 10、11、12 项，并重新评估总名义上限与杠杆。
5. 其余按严重程度逐项处理。每一项修改前，建议先写一个能复现该问题的测试。


## 修复进度（分支 fix/system-overhaul）

本分支基于本清单逐项修复。状态标记：✅ 已修 / ⏭️ 按用户决定跳过 / 🔄 待确认（附疑问，见下节）。

### ✅ 已修

| 编号 | 修复提交 |
|---|---|
| #2 | fix(#2)：get_position/get_open_orders 失败抛 ApiQueryError，启动对齐失败中止不清理 |
| #3、#4、#13 | fix(#3,#4,#13)：孤儿仓位建档；启动补挂统一走 position_manager._补挂保护单；SL 不重复挂并回写 id |
| #5、#15 | fix(#5,#15)：空文件视为损坏、损坏标志持久化、不自动重置；备份失败不覆盖 |
| #6 | fix(#6)：main.py 单实例保护 |
| #7、#8 | fix(#7,#8)：开仓入口按 symbol 加跨 event loop 互斥锁 |
| #9、#24 | fix(#9,#24)：业务错误码不重试并抛带 code 的异常；限频退避；clientOrderId 幂等 |
| #10 | fix(#10)：保护单部分成功改 pending_guard 并记录缺失项，patrol 重试 |
| #12 | fix(#12)：record_pnl 净盈亏口径，手续费/资金费单独字段 |
| #14 | fix(#14)：挂单撤销/过期时部分成交按孤儿仓位处理 |
| #16 | fix(#16)：启动校验对冲持仓模式 |
| #23、#28、#29 | fix(#23,#28)/fix(#29)：检查异常一律拒绝开仓 |
| #25 | fix(#25)：统一定点数字格式；精度获取失败拒绝下单 |
| #26 | fix(#26)：对冲模式平仓单强制校验 positionSide |
| #27 | fix(#27)：应急补止损按 executor 取整并校验 algoId |
| #32 | fix(#32)：竞争替换改走 cancel_signal 公开接口 |
| #33 | fix(#33)：超时撤单改读 open_time |
| #34 | fix(#34)：immediate 入场单改 GTD |
| #35（a） | fix(#35)：删除重复 taker 加分 |
| #36 | fix(#36)：注册 ws on_disconnect 补偿 |
| #37 | fix(#37)：内部后台 task 纳入 watchdog |
| #38 | fix(#38)：umask 改 0o027 |
| #41 | fix(#41)：TP2 最小盈亏比读 config |
| #42 | fix(#42)：SL 调整加初始风险上限检查 |
| #43 | fix(#43)：tp2_qty 统一为 tp2_quantity |
| #44 | fix(#44)：trade_log 增加 signal_id/fees_usdt/pnl_net_usdt，记账用实际手续费 |
| #46 | fix(#46)：get_order 失败抛 ApiQueryError，删除 24h 清除死代码 |
| #48 | fix(#48)：清理未引用失效常量，日重置拆分熔断重置与日志备份 |
| #39 | fix(#39)：watchdog 令牌走环境变量；必填项缺失启动报错；listenKey 脱敏 |
| #50 | fix(#50)：DEPLOY 去令牌；LLM 中转地址移到 .env；日志轮转统一 |
| #35（b） | fix(#35b)：LLM 调用 60s 总超时，超时用默认分值并记 WARNING |
| #16（补充） | fix(#16)：仅确认非对冲时拒绝启动，无法确认时告警继续 |
| #11 | fix(#risk)：熔断恢复为权益 8% 触发（第一批 B） |
| #51 | 已被第二批 F（TP1 后剩余仓位止损移到保本）覆盖 |
| #3、#29、#41、#42 | 用户确认现有实现，不再修改 |
| #47 | fix(#47,#49)：PreOrderError 不重试直接移除条目，5 分钟方向冷却保留 |
| #49 | fix(#47,#49)：废弃 route_quality 决策，统一按 final_score 口径择优（字段保留供日志） |

### ⏭️ 跳过（按用户指示）

- #1 密钥硬编码：单独处理
- #21、#22：线上分支已修，本仓库未同步（无该分支访问）
- #30 杠杆：审查结论判定为对币安杠杆机制的理解偏差，不改
- #31 未闭合K线、#40 策略市场状态：按用户指示不改

### ⏳ 未处理（需重构或策略决策，待用户定）

- #17 守护进程配置、#18 状态文件统一、#19 模块耦合、#20 配置集中、#45 大文件拆分

### 🔄 待确认

- 已全部由用户确认（#42 保留、#35b 加超时、#16 放宽、#3/#29/#41 接受）。
