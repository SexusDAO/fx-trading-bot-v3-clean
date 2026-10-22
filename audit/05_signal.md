# 审查 05：信号逻辑

范围：`fx/scanner.py`（指标、路由、评分、挂单维护、注册信号）、`fx/shared/judges.py`（各策略的 judge）、`fx/shared/signal_watcher.py`（信号过期、存活检查）。

阅读说明：`scanner.py` 4218 行、`judges.py` 3972 行、`signal_watcher.py` 2324 行，本步没有逐行通读。我读了指标计算（`scanner.py:491-595`）、挂单维护（1720-1834）、路由质量与评分（295-372、2660-2790、3762-3813）、注册信号与挂单竞争（3022-3066）、`signal_watcher` 的过期与存活检查，以及各 judge 对 K 线的取用方式（用关键字检索定位后抽读）。各策略 judge 的具体阈值与条件细节放在第 07 步。标"推断"的结论依据代码路径，没有运行验证。

图例：🔴 严重问题 / 🟡 中等问题 / 🟢 没有问题 / 🔵 建议改进

## 1. 有没有用到未来数据（用了未收盘的 K 线做判断）？

🟡 指标层处理得当，但各 judge 对"最后一根是否已收盘"的约定不一致，部分 judge 把正在形成的 K 线当成已收盘 K 线用。

- 做得对的地方（🟢）：`calc_indicators`（`scanner.py:520-528`）显式剔除最后一根未闭合 K 线再算 EMA、RSI、ATR、布林带、近 20 根高低点，当前价另取最新价（534 行）。`vol_ratio_closed`（491-500、608 行）也只用已收盘 K 线，`judges.py:347` 优先使用它。
- 未闭合量比仍在流通：`vol_ratio`（`scanner.py:561-562`）用的是正在形成的那根 K 线的成交量除以均量，代码注释写"只用于量比参考"。整点刚过时这个值天然偏低，临近收盘时偏高，用它做阈值的 judge 会随时间漂移。
- 传给 judge 的是未剔除的原始 K 线：`analyze_one` 把 `k1h`、`k15` 原样传入各 judge（`scanner.py:1274-1292`），中间没有剔除最后一根（`scanner.py:1133-1230` 范围内没有切片处理）。
- judge 内部的处理不统一：
  - `judge_breakout` 明确按收盘时间 `k[6]` 判断，未闭合则去掉最后一根，由开关 `OPT_BREAKOUT_CLOSED_BAR` 控制，当前为开（`judges.py:905-922`、`config.py:956`）。
  - `judge_range_bounce` 自己用 `k1h[:-1]`（`judges.py:2525`），`k15[:-1]` 也有局部处理（2801 行）。`_is_stop_hunt` 等粗筛函数同样自行切片（`scanner.py:670、718、835`）。
  - 其余 judge 假设"调用者已去除未闭合根"：`judges.py:1600` 的注释写明"已收盘的最新K线（调用者已去除未闭合根，-1即最新闭合）"，`judges.py:511`（`k_prev, k_last = k1h[-2], k1h[-1]`）、1929、1953、2011、2028-2029 也按这个约定取 `-1`、`-2`。但调用方并没有剔除，所以这些位置的 `k1h[-1]` 实际是正在形成的 K 线（推断，需要用真实 K 线数据打日志核实）。
- 后果（推断）：盘中某一时刻满足的"反转、减速、加速"形态，K 线收盘后可能消失（重绘），表现为实盘触发的信号在回测里不存在。`judges.py:911-913` 的注释自己就记录了突破策略出过这个问题，其他策略有同样的风险。
- 关于 `_update_btc_1h_drop`（`scanner.py:506-517`）：用最新一根的收盘（实时价）对比上一根收盘，语义是"BTC 近 1 小时跌幅"，属于有意使用实时价，不算问题。
- 取 K 线的来源还有 WS 中心数据（`hub`）路径（`scanner.py:222-236、1597-1600`），hub 返回的最后一根是否带未闭合 K 线，本步没有核实。

## 2. 同一标的重复开仓的防护在哪里？是否完整？

🟡 前置过滤较多，但都是"先检查后执行"，且存在直接操作对方内部状态的地方；详细的下单侧分析见 `03_execution.md` 第 1 节。

防护位置一览：

| 层级 | 位置 | 作用 |
|---|---|---|
| 扫描 | `scanner.py:1367` | 方向冲突：同标的已有同向持仓则放弃 |
| 扫描 | `scanner.py:2223-2247` | 相关性过滤：同方向同策略持仓数量上限 |
| 扫描（funding_arb） | `scanner.py:4032-4044` | 同标的已有主策略仓位则跳过 |
| 监控 | `signal_watcher.py:520、565-609、1240` | 按标的加锁，同标的已有未触发条目则不重复登记或替换 |
| 触发 | `signal_watcher.py:1123-1135` | 同方向同策略 5 分钟冷却 |
| 下单前 | `executor.py:174-323` | `_precheck`：本地记录加交易所挂单兜底 |
| 落库 | `state.py:155-170` | `add_position` 对同标的拒绝 |

- 竞争替换会越过 `signal_watcher` 的内部：每个方向的挂单槽位满时，`scanner.py:3032-3065` 直接读取 `signal_watcher._watched`，对分数最低的已挂标的调用 `api.cancel_all_orders`，再直接 `_watched.pop`。这绕过了 `signal_watcher._cancel_and_remove`（有终态确认与孤儿对账），撤单失败只记 warning（3058-3059 行），之后照样 `pop`，会留下"交易所挂单还在、本地已无条目"的情况（推断）。同时被替换标的在 `pos_state` 中的 `pending` 记录是否同步清除，本步没有看到。
- `cancel_all_orders(symbol)` 会撤掉该标的所有普通挂单；对只有挂单、尚无仓位的标的是安全的，但如果该标的此刻已成交并带有 TP 挂单，会一并误撤（推断，取决于时序）。
- 其余缺口沿用报告 03：并发路径下的检查窗口、交易所挂单兜底失败时静默通过、`add_position` 拒绝时订单已发出。

## 3. 信号过期逻辑是否正确？过期后挂单如何处理？

🟡 `signal_watcher` 一侧的过期与撤单处理较完整，但存在多套互相独立的过期时钟，且 `scanner` 侧的挂单超时判断读取了不存在的字段。

- 过期的主路径（🟢）：`WatchEntry.expire_ts`（`signal_watcher.py:96`）、`is_expired`（132-133）、后台 GC 发现过期后调用 `_handle_expired`（1012-1013、1282-1292），统一走 `_cancel_and_remove`；撤单结果未知时条目保留并登记到 `_orphan_orders`，由 `_reconcile_orphans` 继续对账（1385-1400 行及其后）。重挂时保留原始有效期（`score_original_expire`，113、389、617 行），避免无限续命。
- 多套过期时钟，来源各自计算：
  - 监控层：`scanner.py:2919`（`expire_sec`）与 3108 行（`exp`）。
  - 交易所层：`executor.py:1769-1783` 为 `breakout`、`oi_divergence` 和 `pending_order` 计算 GTD 到期时间，其中 `pending_order` 的 4 小时是写死的 `4 * 3600`，而配置 `ORDER_EXPIRE_PULLBACK_MIN = 240`（`config.py:318-319`）只是恰好相同。
  - 其他入场（`entry_mode` 为 `immediate` 的限价单）`expire_ts = None`，下的是 GTC 单（`executor.py:741-747`），没有交易所侧过期；一旦进程停了，这类挂单会一直挂着，直到重启后的对账处理（推断）。
  - 持仓层：`position_manager._check_pending_expire_static` 再按 `ORDER_EXPIRE_*_MIN` 判断（`position_manager.py:1812-1838`）。
- **`scanner.maintain_pending_order` 的超时判断读错字段：** `scanner.py:1728` 读取 `pos.get("open_ts", 0)`，而 `executor` 写入挂单记录用的是 `open_time`（`executor.py:1266、1412`），全仓库没有写入 `open_ts` 的地方（检索无结果）。于是 `elapsed_min` 恒为 0，该函数里的"超时撤单"（1731-1758 行）永远不会触发（推断）。当前靠 `position_manager` 和 `signal_watcher` 的过期机制兜底，但这个函数里依赖 `elapsed_min` 的分支是死代码。
- `maintain_pending_order` 的失败策略是"保留"：任何异常都返回 `keep`（`scanner.py:1832-1834`），行情或接口异常时失效的挂单会继续挂着。它的 `reorder` 分支只在 `_survival_check` 导入失败的降级路径里出现（1822-1827 行），正常路径不会重挂。
- 过期时的部分成交：`signal_watcher` 一侧会按已成交量判断（1327-1330 行）；`position_manager` 一侧对撤单/过期直接删本地记录，没有核对已成交数量（见报告 02 第 3 节）。
- 存活检查（`_survival_check`）按模式判断，pullback 用 1H 收盘价而不是瞬时价，是为了防插针（做法合理）；breakout/reversal 则用实时价加 ATR 缓冲，抗插针能力弱于前者。

## 4. 信号评分的计算是否有逻辑漏洞？

🟡 评分链条很长，存在重复计分、被常量覆盖和配置失效的地方。

- 评分链条：基础分（strong 70、medium 50 等，见 `config.py:170` 的注释）+ BTC 调整 + 清算图加分 + 策略调整 + 冲击后调整 + ATR 档位调整 + pulse 惩罚 + taker 调整，截断到 0 到 100（`scanner.py` 约 2675 行）；再叠加三个"hub 加分"（`_apply_score_bonuses`，3762-3813 行），最后可能被 `-10` 惩罚（2408、2706 行）和 LLM 边界提升覆盖。
- **taker 因素重复计分：** 评分链条里已有 `taker_adj`（买卖比 > 0.55 或 < 0.45 时 ±3，`scanner.py:2660-2672`），随后 `_apply_score_bonuses` 又有 `SCORE_TAKER_BONUS`（顺向 +10、逆向 -5，`config.py:194`；`_calc_taker_bonus`，3716 行）。两者都反映同一指标（主动买入占比），且 `config.py:194-196` 的三个加分开关当前全部为开，实际是重复计分（推断）。
- **分数被常量覆盖：** 边界信号（45 到 55 分）经 LLM 判断为"开仓"后，`final_score` 被直接改写为 `MIN_SIGNAL_SCORE_WEEKDAY + 1`（`scanner.py:2736`）；周末分数超过 80 被直接改写为 80（2771 行）。覆盖之后，原始分数信息丢失，后续按分数排序（2815-2817 行）、挂单竞争（3041-3046）、仓位分档（`executor` 里按 `score` 取 `cap`）都用被覆盖后的值，复盘时无法还原信号原本的质量。
- **配置失效：** 周末门槛在 `config.py:188` 是 `MIN_SIGNAL_SCORE_WEEKEND = 70`，代码里被导入（2713 行）但没有使用，实际使用写死的 `60 if strong else 70`（`scanner.py:2762-2764`）。改配置对周末门槛不生效。
- **两套评分并存：** `_calc_route_quality`（295-372 行，满分 100，由强度 40、与市场模式适配 25、与 BTC 趋势一致 15、清算图质量 20 组成）只用于"同一标的多条路径都通过时择优"；`final_score` 是另一套。两套的口径不同，同一信号在两套里的排序可能不一致。路径与市场模式的适配表 `_TYPE_MKT`（312-338 行）是手工设定的分值，缺失项默认 15，没有数据依据。
- **LLM 参与打分且同步阻塞：** 边界信号调用 `judge_borderline_signal_llm`（`scanner.py:2730`）在扫描线程里同步执行；输出不确定，导致同样的行情在不同时间可能得到不同结论，回测无法复现。`LLM_JUDGE_ENABLED`、`LLM_JUDGE_BORDERLINE` 当前均为开（`config.py:664-666`）。边界区间 45 到 55 与工作日门槛 50 重叠，意味着低于门槛 5 分以内的信号可以被 LLM"救回"，而高于门槛 5 分以内的信号不会被 LLM 复核。
- 缺值默认：`final_score` 缺失按 0 处理（直接被过滤），`signal_strength` 缺失按 `medium`（`scanner.py:2763`）；这类静默默认值容易掩盖上游的取数失败。
- 单项加分里的未闭合 K 线：`_bonus_hub_klines(symbol, "1h", ...)`（3695 行起）取的 K 线是否包含未闭合那根，本步没有核实。

## 待处理清单

- [ ] 🟡 `judges.py:511、1600-1602、1929、1953、2011、2028-2029`：多个 judge 假设调用者已剔除未闭合 K 线，但 `scanner.py:1274-1292` 并未剔除；需统一约定（在入口统一切片，或各 judge 自行按收盘时间判断）。
- [ ] 🟡 `scanner.py:561-562`：`vol_ratio` 使用未闭合 K 线的成交量，应只保留 `vol_ratio_closed` 或明确标注哪些阈值使用哪一个。
- [ ] 🟡 `scanner.py:3032-3065`：挂单竞争替换直接操作 `signal_watcher._watched` 并 `cancel_all_orders`，应改为调用 `signal_watcher` 提供的撤单接口，并同步清理 `pos_state` 的 pending 记录；撤单失败时不应继续 `pop`。
- [ ] 🟡 `scanner.py:1728`：`open_ts` 与 `executor` 写入的 `open_time` 不一致，`maintain_pending_order` 的超时撤单是死代码；需统一字段名。
- [ ] 🟡 `scanner.py:1832-1834`：`maintain_pending_order` 出错即 `keep`，应明确失败策略。
- [ ] 🟡 `executor.py:1769-1783`、`scanner.py:2919、3108`、`config.py:318-319`：过期时间在多处独立计算，`4 * 3600` 写死；需要单一来源。
- [ ] 🟡 `executor.py:741-747`：`immediate` 模式的入场单为 GTC 且无过期，进程停止时无法自动失效。
- [ ] 🟡 `scanner.py:2660-2672` 与 `scanner.py:3716-3764`、`config.py:194`：taker 因素重复计分，需合并为一项。
- [ ] 🟡 `scanner.py:2736、2771`：分数被常量覆盖，应保留原始分数字段，覆盖值另存。
- [ ] 🟡 `scanner.py:2762-2764` 与 `config.py:188`：周末门槛写死，配置不生效。
- [ ] 🟡 `scanner.py:2730`：LLM 边界判断在扫描线程内同步调用，结果不可复现；需要记录 LLM 的输入输出，并考虑是否保留这条救回机制。
- [ ] 🔵 `scanner.py:295-372`：路径质量分与 `final_score` 两套评分并存，分值表缺少数据依据，建议基于历史成交统计校准。
- [ ] 🔵 `scanner.py:1597-1600`、`3695-3712`：hub 路径下 K 线是否含未闭合那根需要核实并在文档中明确。

> 注：async 内同步 I/O 问题在 feature/passive-trigger 分支已有部分修复
> （state.py OI缓存、main.py WS重连、trigger_detector.py 候选池缓存），
> 本仓库当前代码未包含这些修复，审查结论基于 main 分支现状。
