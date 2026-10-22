# 审查 07：交易策略完整闭环

范围：`fx/shared/calc_sl_tp.py`（止损止盈与入场价）、`fx/shared/signal_watcher.py`（挂单监控与重挂）、`fx/position_manager.py`（成交后维护、移动止损、TP1 后处理）、`fx/scanner.py` 与 `fx/shared/judges.py`（信号捕捉与确认）、统计与日志相关代码。

阅读说明：本步以前面几份报告为基础，补读了 `calc_sl_tp.py` 的止损止盈计算（624-742、813-938 行）、`position_manager.py` 的移动止损与成交处理（170-314、664-778 行）、`signal_watcher.py` 的价格位移重挂（2080-2160 行）。**各策略 judge（`judges.py` 共约 4000 行）的具体入场条件没有逐条审查**，入场价的三个计算函数（`_entry_pullback`、`_entry_breakout`、`_entry_reversal`，`calc_sl_tp.py:297-590`）以及 `position_manager.py` 的 `_adjust_sl_tp_once` 也没有通读。这些部分的结论只能算抽查，需要后续逐条走读。标"推断"的结论依据代码路径，没有运行验证。

图例：🔴 严重问题 / 🟡 中等问题 / 🟢 没有问题 / 🔵 建议改进

## 1. 信号捕捉：触发条件是否合理？有没有在错误的市场状态下触发？

🟡 有较多的市场状态过滤，但策略与市场状态的匹配只体现在"扣分"上，不是"禁止"，且兜底路径总是开着。

- 候选路径的收集（`scanner.py:1229-1255`）：`stop_hunt`、`oi_divergence`、`ema_trend`、`range_bounce`、`reversal`、`breakout` 按粗筛条件加入，**`pullback` 作为兜底总是加入**（"pullback 作为最低优先级兜底，始终加入"）。也就是说，即使其他策略都不适用，每个标的仍至少会走一遍 `pullback` 的 judge，是否放行完全取决于该 judge 的条件。
- 策略与市场状态的适配表（`_calc_route_quality` 里的 `_TYPE_MKT`，`scanner.py:312-338`）：例如 `breakout` 在震荡市只给 5 分，`reversal` 在牛市只给 5 分，`ema_trend` 在熊市只给 5 分。但这只是影响"同一标的多条路径择优"时的相对分数，并不阻止这些策略在不适配的市场状态下触发；最终是否过关取决于 `final_score` 门槛（工作日 50，周末 60/70，见报告 05）。
- 已有的状态过滤（🟢）：BTC 暴跌拒绝（`judges.py:202` 的 `_btc_crash_reject`）、市场冲击与系统性风险检查（`scanner.py:3281-3303`）、股票代币休市跳过（`scanner.py:1225-1244`）、周末降频与周末 `breakout` 只放行 `strong`（`scanner.py:2762-2790`）、同标的资金费率结算窗口过滤（`scanner.py:908-966`）。
- 判断周期偏短但触发链路较长：粗筛 → judge → 评分 → 过滤 → 挂单监控 → 价格触发 → 预检 → 下单。链路上多个环节各自读取行情，数据时点不一致（K 线、标记价、hub 缓存），同一信号在不同环节看到的价格可能相差数个 tick 到数个 ATR。`signal_watcher` 在触发前有滑点合理性检查（偏离超过 0.5 ATR 或跳过入场区超过 0.3 ATR 就放弃，`signal_watcher.py:1175-1198`），这是对该风险的有效缓解（🟢）。
- 已知问题沿用：未闭合 K 线被当作已收盘使用，可能让信号在盘中触发后重绘（报告 05 第 1 节）。

## 2. 信号确认：judge 层的条件是否完整？有没有逻辑矛盾的判断？

🟡 judge 层本身没有逐条走读，已确认的问题集中在约定不一致和异常处理上。

- K 线收盘约定不统一：`judge_breakout` 按收盘时间剔除，`judge_range_bounce` 自己切片，其余 judge 假设调用方已剔除（报告 05 第 1 节）。这是跨 judge 的"条件矛盾"，同样的一根 K 线在不同 judge 里被当成"已收盘"或"未收盘"。
- 配置读取失败的静默降级：`judges.py:32-90` 有 `_cfg_fallback` 机制，配置读取失败时回退到代码里的默认值并记录。这避免了崩溃，但也意味着改配置不一定生效，需要确认日志里是否出现过回退记录。
- judge 异常：`_judge_error_result`（`judges.py:149-170`）把异常转成结果返回，具体是拒绝还是放行，本步没有展开核实（需要确认异常时是 `False`）。
- LLM 参与确认：边界信号（45 到 55 分）由 LLM 判断是否开仓，`final_score` 被改写为常量（`scanner.py:2736`），不可复现（报告 05 第 4 节）。
- 评分的逻辑漏洞见报告 05：taker 重复计分、周末门槛写死、两套评分并存。
- 需要后续逐条走读的 judge：`judge_pullback`（`judges.py:228-724`，约 500 行）、`judge_reversal`（1224-1729）、`judge_stop_hunt`（2648-3155）、`judge_oi_divergence`（3168-3726）、`judge_ema_trend`（3739 起）。

## 3. 挂单定位：入场价、止损价、止盈价的计算逻辑是否正确？

🟡 止损和止盈的计算思路清晰且有多重保护，但参数来源分散、有硬编码和失效配置，入场价的三个函数本步没有读。

**止损（`calc_sl_tp._calc_sl`，`calc_sl_tp.py:624-742`）**

- 优先锚定清算区外侧：多单取最近多头清算区下沿减 0.2 ATR（`_SL_LIQ_BUFFER_ATR`，587 行），超过各档位的上限（`config.py:264-266`，12%/15%/20%）则降级为 `entry ± sl_mult × ATR`，并标记 `degraded`（638-676 行）。
- 至少离入场 0.5 ATR（656、676 行），再叠加结构止损（摆动高低点，678-703 行；只会比原值更宽，不会更窄），最后做最小距离保护（`low 2.5% / mid 3% / high 5%`，705-740 行）：不足时拉宽，若落进清算区内部再外推到清算区外侧。
- 结构止损和最小距离保护都会把止损拉宽，拉宽发生在上限检查之后；但后续仍有 `_check_sl_distance`（216 行，在 1244 和 1305 行被调用）重新校验，这一点处理得合理（🟢）。
- 参数来源：`_SL_MULT` 和 `_SL_MAX_MAP` 来自配置（82-84 行），最小距离表 `_sl_min_map` 写死在函数里（707 行），`funding_arb` 另有写死的 `_FA_SL_MULT = 1.5`（1315 行）。
- 降级标记的副作用：止损降级为 ATR 公式时（`degraded=True`），执行器把仓位减半，且 `score < 55` 时直接拒绝（`executor.py:307-321`）；而结构止损保护"有意不触发降级"（687-689 行注释）。结果是：被结构止损拉宽后的止损，会享受完整仓位，风险由 `RISK_PER_TRADE ÷ 止损距离` 的仓位公式吸收，这一点在逻辑上是自洽的。

**止盈（`_calc_tp`，813-938 行）**

- 有清算图：TP1 取最近的、盈亏比不低于 `_TP1_MIN_RR` 的清算区前沿（缓冲 0.2 到 0.5 ATR，按清算区相对强度分档），找不到则按盈亏比兜底并标记降级；TP2 仅在至少有两处清算区时才设，取"最远且盈亏比达标"的区（843-915 行）。
- 无清算图：订单簿找密集区作 TP1，仍不行则用 ATR 兜底的单 TP（917-936 行）。
- 参数冲突：TP2 的最小盈亏比在代码里写死 `_TP2_MIN_RR = 2.5`（`calc_sl_tp.py:88`），而 `config.py:278` 的 `RR_MIN_TP2 = 3.0` 注释写着"代码未引用，修改无效"，两处数值不一致且配置不生效。
- TP1 的最小盈亏比来自配置 `RR_MIN_TP1`（87 行），当前为 2.0（高波动档 1.5）。
- 未读部分：三个入场价函数 `_entry_pullback`、`_entry_breakout`、`_entry_reversal`（297-590 行）、订单簿 TP 逻辑（752-812 行）。

## 4. 挂单维护：价格偏离后如何重挂？重挂时旧单是否一定先撤？

🟢 `signal_watcher` 的重挂流程设计得比较稳妥；🟡 但 `scanner` 一侧的维护路径有问题，且存在多条互相独立的维护路径。

- `signal_watcher._reorder_replace`（`signal_watcher.py` 约 2112-2150 行）的顺序是"先撤旧单并确认终态，再注册新单"：旧条目先置 `cancelling`，确认已撤才清空旧订单号并注册新条目；若旧单已（部分）成交或撤单结果未知，则不注册新单，未知情况登记到 `_orphan_orders` 继续对账。注释里记录这是 v3.89 修复的问题（原先是先注册后撤，会同时存在两张挂单）。这一部分是整个仓库里处理得最规范的地方（🟢）。
- 重挂保留原始有效期和创建时间（`score_original_expire`、`score_created_at`，`signal_watcher.py:112-115`），重挂单不参与竞争替换（637 行），避免无限续命；重挂次数有计数。
- 问题一：`scanner.maintain_pending_order`（`scanner.py:1720-1834`）读取 `open_ts`，而挂单记录里写的是 `open_time`，超时撤单永远不触发（报告 05 第 3 节）；任何异常都返回 `keep`；返回 `reorder` 的分支只在降级路径里出现。
- 问题二：挂单维护分散在四个地方：`scanner.maintain_pending_order`、`signal_watcher` 的复核与存活检查、`position_manager._check_pending_expire_static`、`scanner.py:3032-3065` 的竞争替换（它会直接操作 `signal_watcher._watched`，报告 05 第 2 节）。四者的过期、撤单判定标准各自独立。
- 问题三：竞争替换使用 `cancel_all_orders(symbol)`（`scanner.py:3056`），会撤销该标的全部普通挂单，不区分入场单与保护单。

## 5. 开仓维护：TP1 成交后剩余仓位如何管理？止损是否移动？

🟡 TP1 后会为剩余仓位补挂止损，但补挂的价格是原始止损价，不是保本价；之后的保护依赖若干条独立规则。

- TP1 成交（`position_manager._handle_order_filled`，`position_manager.py:708-756`）：
  - 单 TP 模式：撤 SL、记 `tp1_filled`、平仓后处理（708-714 行）。
  - 双 TP 模式：记 `tp1_filled=True`、`quantity_remaining=tp2_qty`、`trailing_stop_activated=True`，并**用原始 `sl_price`** 为剩余 40% 仓位新挂止损，成功后再撤旧的全仓位止损（722-748 行，顺序合理）；挂不上则通知"剩余仓位无止损保护"（750-753 行）。这里正确检查了 `place_sl` 返回 `{}` 的情况，是少数处理得当的位置（🟢）。
- 止损是否上移：
  - 阶梯移动止损 `_calc_trail_sl`（170-236 行）按浮盈相对 ATR 的倍数逐级上移，止损只向有利方向移动。执行时先挂新止损再撤旧的（242-314 行，持 `_guard_locks`，被占用时本轮跳过），这个顺序避免了裸仓窗口（🟢）。新单失败则标 `sl_sync_status=failed`，不会回退。
  - "R ≥ 1.5 则止损移到 0.8R 附近"的 RR 保护只在 TP1 未成交时生效（206 行）；TP1 成交后改由"TP1 后保本保护"规则处理（`position_manager.py:501-570`，依赖 `opt_v384.tp1_protect_action`）。
  - 所以 TP1 刚成交时，剩余仓位的止损在原始位置，只有满足阶梯条件或保本保护条件才会上移，不是自动保本。从盈亏上看，剩余 40% 仓位若在原始止损处被打掉，亏损约 0.4R，而 TP1 已实现盈利不少于 `RR_MIN_TP1 × 0.6` 个 R，整体不会亏。这是策略取舍，不算错误，但与"保护已实现利润"的直觉不同（🔵）。
- 其他会改变持仓风险的规则：
  - 时间止损（`config.py:398-401`：8/12/16 小时；`TIME_STOP_PROFIT_THRESHOLD` 标注"代码未引用"）。
  - 实时盈亏比低于 `TP_MIN_RR_LIVE = 1.0` 就主动平仓（`config.py:411`；`position_manager.py:3586-3598` 的 `rr_below_threshold`）。
  - 定时重算 TP/SL（每 30 分钟，`config.py:391-394`），其中**允许止损外扩**（`TP_SL_ADJUST_SL_WIDEN_ATR`，新止损比旧的宽超过 0.3 ATR 才更新）。止损外扩意味着实际亏损可能超过开仓时按 `RISK_PER_TRADE` 算出的上限（推断，`_adjust_sl_tp_once` 本步没有读，需核实实际是否会对已成交仓位外扩止损）。
  - 巡检里的时间止损、突破失败、反向信号检查同时存在两处（`position_manager.py:779-781` 的注释承认重复，靠 `_force_close_static` 的状态重读去重）。
- 字段名漂移风险：`tp2_quantity` 在 `_handle_order_filled` 里读取（671 行），在 `_补挂保护单` 里写成 `tp2_quantity or tp2_qty`（2971 行），说明历史上字段名不统一；写入端是否一致需要核实。

## 6. 止盈止损：计算方式、挂单类型、reduceOnly 是否全部正确？

🟡 计算和挂单类型基本合理，但 reduceOnly 在对冲模式下实际不生效，保护单提交和校验存在前面几份报告提到的缺口。

- 挂单类型：止损为 `STOP_MARKET` 条件单，走 `/fapi/v1/algoOrder`，`workingType=MARK_PRICE`（`binance_api.py:909-926`）；止盈为 `LIMIT` 的 GTC 限价单。条件单用标记价触发，可避免插针，但触发后是市价成交，会有滑点。
- reduceOnly：对冲模式下 `place_order` 不发送该参数，`reduce_only=True` 是空操作（`binance_api.py:557-561`）；约束全靠 `positionSide`（报告 03 第 4 节）。
- 数量：TP1 与 TP2 取整后可能小于成交量，差额记为 `unallocated_qty` 但无后续处理（`executor.py:1039`）；止损数量使用 `tp1 + tp2` 之和（936 行）；TP1 成交后为剩余仓位重新挂止损时按 `tp2_qty` 取整（`position_manager.py:725`）。
- 保护单齐全性：部分成功时仍标 `open`（报告 04 第 1 节）；`_validate_sl_order` 不在 `place_sl` 的路径上（报告 03 第 4 节）；数量与价格字符串化可能出现科学计数法（报告 03 第 2 节）。
- 止盈止损被触发后的清理：止损触发会撤 TP1/TP2（`position_manager.py:698-705`），TP2 触发会撤止损（`:763-768`），撤单失败只通知，不重试到成功。

## 7. 统计与复盘：信号记录、拒绝记录、成交记录是否完整？

🟡 记录的维度相当丰富，但存在口径、关联和持久化上的缺口，复盘时难以完全还原一笔交易的来龙去脉。

- 现有记录：
  - 信号分析：`signal_analytics.jsonl`（`scanner.py:373-470、2966`，`config.py:872`）。
  - 扫描汇总：`scanner.log`（`scanner.py:3871-3911`）。
  - judge 拒绝与预检拒绝的影子记录：`shadow_trace.record_judge_rejection`（`scanner.py:1452`）、`record_precheck_rejection`（`executor.py:1460`），另有 `tools/reject_funnel.py`、`tools/track_rejected.py`、`tools/track_outcomes.py` 做事后追踪。
  - 成交：`trade_log.jsonl`（`position_manager.py:1065、2209`、`config.py:483`），字段包括模式、波动档、信号强度、入出场价、数量、杠杆、盈亏、持仓时长、平仓原因、TP1/TP2 是否成交、止损是否降级、清算图是否可用、入场量比、相关性、judge 详情（`position_manager.py:1040-1062`）。
  - 异常与对账：`unpersisted_positions.jsonl`、`cancel_reconcile.jsonl`、`tp_sl_adjust_log.jsonl`。
- 缺口：
  - **盈亏口径：** `trade_log` 里的 `pnl_usdt` 来自成交记录的 `realizedPnl`（`position_manager.py:1025`）或 `(出场价 - 入场价) × 数量` 的估算（1030 行、2080-2085 行），不含手续费和资金费。资金费另存于 `pos_state` 的 `funding_cost_total`（2644-2704 行），在 `trade_log` 中是否合并没有看到。
  - **关联性：** 信号记录与成交记录的关联靠 `_sig_id` 和 `trade_id`（`{时间}_{标的}`），本步没有看到 `trade_log` 里写入 `_sig_id`；若没有，则信号分析与成交记录之间需要靠时间和标的模糊匹配（推断）。
  - **分数覆盖导致失真：** `final_score` 被常量覆盖后，复盘无法区分原始分数（报告 05 第 4 节）。
  - **滑点与时延：** 没有看到"信号价、触发价、实际成交价"的并列记录；时延也没有统一字段。
  - **部分成交与异常路径：** 部分成交后被撤单、孤儿仓位、未建档的仓位，成交记录可能缺失（报告 02、03）。
  - **多写者追加写入：** 多个模块各自 `open(..., "a")` 追加 JSONL，没有统一的写入函数、没有 schema 版本；崩溃时可能留下半行（推断）。
  - **持久化与备份：** 这些文件都在 `logs/` 下，被 `.gitignore` 排除；`DEPLOY.md` 第 10 节的备份清单只列了 `pos_state.json`、`candidate_pool.json`、`cooldown_state.json` 和几个规则文件，**没有列 `trade_log.jsonl`、`signal_analytics.jsonl`**，主机故障会丢失全部复盘数据。
  - 文件权限过宽（报告 06 第 3 节）。

## 待处理清单

- [ ] 🟡 `scanner.py:1229-1255`、`scanner.py:312-338`：策略与市场状态的匹配只扣分不禁止，且 `pullback` 总是兜底；需要明确哪些组合应该直接禁止。
- [ ] 🟡 `judges.py:511、1600-1602、1929、1953、2011、2028-2029`：K 线收盘约定不统一（同报告 05）。
- [ ] 🟡 `judges.py:149-170`：核实 judge 异常时是否一律拒绝。
- [ ] 🟡 逐条审查 `judge_pullback`、`judge_reversal`、`judge_stop_hunt`、`judge_oi_divergence`、`judge_ema_trend` 的入场条件（本步未覆盖）。
- [ ] 🟡 `calc_sl_tp.py:88` 与 `config.py:278`：`_TP2_MIN_RR=2.5` 与 `RR_MIN_TP2=3.0` 冲突，配置不生效。
- [ ] 🟡 `calc_sl_tp.py:707、1315`：最小止损距离表和 `_FA_SL_MULT` 写死，应并入配置。
- [ ] 🟡 `scanner.py:1728`、`scanner.py:3056`、`scanner.py:1832-1834`：挂单维护的字段名错误、`cancel_all_orders` 过度撤单、失败即保留（同报告 05）。
- [ ] 🟡 挂单维护分散在四处（`scanner.maintain_pending_order`、`signal_watcher` 复核、`position_manager._check_pending_expire_static`、`scanner.py:3032-3065`），需要合并为单一入口。
- [ ] 🟡 `config.py:391-394` 与 `position_manager.py:3235` 起的 `_adjust_sl_tp_once`：确认是否会对已成交仓位外扩止损，若会，则与 `RISK_PER_TRADE` 的风险上限矛盾。
- [ ] 🟡 `position_manager.py:3586-3598`、`config.py:411`：`TP_MIN_RR_LIVE=1.0` 的主动平仓规则需要用历史数据评估是否过早离场。
- [ ] 🟡 `position_manager.py:671、2971`：`tp2_quantity` / `tp2_qty` 字段名漂移，需统一。
- [ ] 🟡 `position_manager.py:698-705、763-768`：保护单撤销失败只通知，需要重试直到确认。
- [ ] 🟡 `position_manager.py:1025、1030、2080-2085`、`trade_log.jsonl`：盈亏口径不含手续费与资金费，需要拆分为毛利、手续费、资金费、净利。
- [ ] 🟡 `trade_log.jsonl`：补充 `_sig_id`、信号价、触发价、实际成交价、原始分数与覆盖后分数，以便复盘。
- [ ] 🟡 各模块的 JSONL 追加写入：统一写入函数，带 schema 版本和原子追加。
- [ ] 🟡 `DEPLOY.md` 第 10 节：备份清单补上 `trade_log.jsonl`、`signal_analytics.jsonl` 等复盘数据。
- [ ] 🔵 `position_manager.py:722-733`：TP1 后剩余仓位的止损位置（原始止损 vs 保本）属于策略选择，建议用历史数据对比。
- [ ] 🔵 `config.py:398-401`：`TIME_STOP_PROFIT_THRESHOLD` 等多个常量标注"代码未引用"，建议清理或接入。
- [ ] 🔵 `position_manager.py:779-781`：时间止损等检查在两处重复，建议合并。

> 注：async 内同步 I/O 问题在 feature/passive-trigger 分支已有部分修复
> （state.py OI缓存、main.py WS重连、trigger_detector.py 候选池缓存），
> 本仓库当前代码未包含这些修复，审查结论基于 main 分支现状。
