# 审查 02：启动与恢复

范围：`fx/main.py` 的 `_sync_positions_on_startup`（435-660 行）、`fx/position_manager.py` 的 `_补挂保护单`（2883-3110 行）与 `_check_pending_expire_static`（1753-1838 行）、`fx/shared/state.py` 的 `_read_json`/`_write_json`/`_ensure_files`、`fx/executor.py` 的 `_place_guards_async`（907-1051 行）。
补充读取：`shared/binance_api.py` 的 `_retry`、`get_position`、`get_open_orders`、`get_order`，用来确认接口失败时的返回值。

阅读说明：以下结论来自读代码，没有运行验证。标"推断"的是依据代码路径做的判断，需要线上或测试环境复现。`position_manager.py` 其余部分（如 `_adjust_sl_tp` 一类的运行期维护）本步没有通读。

图例：🔴 严重问题 / 🟡 中等问题 / 🟢 没有问题 / 🔵 建议改进

## 0. 先说最关键的一条：接口失败会被当成"交易所没有持仓" 🔴

- `binance_api.get_position()` 的文档字符串写明"失败返回 []"（598-606 行）：`_retry` 在重试耗尽后返回 `None`，再被 `or []` 转成空列表。`get_open_orders()` 同理，`get_order()` 失败返回 `{}`。
- `_sync_positions_on_startup` 对 `_bapi.get_position()`（476 行）的结果不区分"成功返回空"与"请求失败"。一旦代理故障、超时或限频，`real` 就是空字典。
- 随后 503-514 行的"清僵尸记录"循环会把 `pos_state` 里所有 `status == "open"` 的记录当作"交易所已无持仓"逐条 `remove_position`。
- 这个函数在两个时机运行：进程启动，以及每次 WS 重连（728-731 行）。WS 重连恰恰最可能伴随网络或代理异常。
- 结果（推断）：网络抖动一次，可能清空全部本地持仓记录，而交易所里的真实仓位仍在。后续巡检、移动止损、时间止损、槽位计数都不再认得这些仓位（见问题 4）。
- 同一个问题还会放大补挂：`get_open_orders()`、`get_algo_open_orders()` 失败时得到空集合，`live_ids`、`live_algo_ids` 为空，所有保护单都会被判为"不在交易所"，触发对每个仓位的重复补挂（见问题 2）。
- 这条是本步最需要优先处理的问题，建议在继续看其他项之前先确认线上是否发生过。

## 1. 重启后持仓对齐是否可靠？🔴

- 流程：先清孤儿 `patching`（459 行）→ 拉持仓、挂单、algo 止损单 → 清僵尸记录 → 逐仓位检查保护单并补挂 → 汇总通知。
- 记录有、交易所没有：只处理 `status == "open"` 的记录，且仅当 `(symbol, direction)` 不在 `real` 中时才删除（504-514 行）。逻辑本身合理，但前提是 `real` 可信，见第 0 节。`pending`、`patching` 记录不在这里对账，交给巡检（`_check_pending_expire_static`，见问题 3）。
- 交易所有、记录没有（孤儿仓位）：
  - `ps = ps_map.get(sym, {})` 为空，`pid` 退化为 `sym`（520 行）。
  - `single_tp_mode`、`tp1_alive` 等判断都落到"需要补挂"，进入 582-618 行。
  - 方向取自 `ps.get("direction", "long")`（594 行），不是交易所返回的 `positionSide`。孤儿空单会被当成多单计算止损止盈价并以 `LONG` 下单，推断会被交易所拒绝或挂在错误方向，最终进入 `errors`。孤儿多单则能挂出保护单。
  - 补挂后调用 `update_position(pid, {...})`（607 行），而 `pid == sym` 在 `pos_state` 里并不存在，`update_position` 只记一条 warning 并返回 `False`（`state.py:189`）。也就是说，孤儿仓位拿到了保护单，但没有被建档。
  - 下一次启动或 WS 重连，它仍然是孤儿，保护单再被补挂一轮，保护单会不断累积。
- `ps_map = {p["symbol"]: p ...}`（499 行）按 `symbol` 建索引，而 `real` 用 `(symbol, positionSide)`。同标的多空双持（对冲模式）时后者会覆盖前者，对账不对称。
- 没有持仓模式校验：对账假设是对冲模式（479 行注释），并且只统计 `positionSide in ("LONG", "SHORT")`。如果账户是单向模式，接口返回 `BOTH`，`real` 恒为空，会把全部记录当僵尸删除。本步读过的部分没有看到对持仓模式的启动校验，需要确认。
- `_sync_positions_on_startup` 在 `pm.start()` 之前运行（725 行 vs 737 行），首次启动是串行的；但作为 WS 重连回调时（728-731 行）会与已在运行的巡检、`_adjust_sl_tp` 等任务并发，且没有任何互斥。
- 对账没有数量校验：只比较"有没有"，不比较 `positionAmt` 与本地 `quantity`（部分平仓、手动加减仓后数量漂移不会被发现）。
- 做得对的地方：启动时先清理孤儿 `patching`（避免该标的永远无法再开仓），以及通知只在补挂失败时推送，减少噪音。

## 2. 止损止盈挂单能否在重启后补回？两份逻辑是否一致？🔴

两份实现：`main.py:517-618`（启动对账）与 `position_manager._补挂保护单`（2883-3110 行）。两者都复用 `executor._place_guards_async`，但外围逻辑不一致。

| 项目 | `main.py` 启动对账 | `position_manager._补挂保护单` |
|---|---|---|
| 并发互斥 | 无 | 每个 `pos_id` 一把 `asyncio.Lock`（2900 行，`_guard_locks`） |
| 查交易所已有 TP | 仅按本地记录的订单 id 对照 | 直接查交易所 LIMIT 平仓单，已有则只同步状态（2935-3002 行） |
| 校验实际持仓量 | 用 `positionRisk` 的数量 | 重新查询并以交易所数量为准，为 0 则标记已平仓（2912-2933 行） |
| ATR 降级倍数 | 取 `config.SL_MULT_*`（591 行） | 写死 `{low:1.2, mid:1.5, high:2.0}`（3018 行） |
| 写回 `sl_order_id` | 只在单独补 SL 的分支写（562 行）；TP 补挂分支不写（607-616 行） | 写（3047 行） |
| 异常处理 | 记入 `errors`，仅失败时推送 | 回滚 `pending`、推送并抛出 |

主要问题：

- **重复补挂 SL：** 531-535 行在 SL 缺失时先单独补一次 SL；之后 `sl_alive` 仍是旧值 `False`，571 行的跳过条件不成立，流程继续进入 582-618 行，再调用一次 `_place_guards_async`，其中又包含 SL。同一仓位在同一轮可能挂出两张 SL，而第二张的 id 没有保存（607-616 行没有 `sl_order_id`）。
- **只缺 SL 也会重挂 TP：** 当 TP 都在、只有 SL 缺失时，因为 `sl_alive == False`，仍会走完整补挂，把 TP1/TP2 也重挂一遍。`main.py` 路径里没有"查交易所是否已有 TP"的保护（只有 `_补挂保护单` 有）。
- **SL 补挂不会清除旧 id：** 记录里陈旧的 `sl_order_id` 如果没被覆盖，下一次对账仍会判定"SL 不在交易所"，每次重启或 WS 重连都再补一次。
- **dead code：** 544-547 行是 `... if False else None` 形式的无效代码。
- **半保护被记成 open：** `_place_guards_async` 失败时只通知并把对应 id 设为 `None`，返回 `protection_status: "partial"`（1038 行）。`main.py:607` 和 `position_manager.py:3042` 都不检查该字段，照样把 `status` 写成 `open`。自愈依赖上面这些"顺带重挂"的路径，而不是明确的重试机制。
- **与并发路径的竞态：** WS 重连时的启动对账与巡检、`_check_pending_expire_static`、WS 成交事件触发的 `_补挂保护单` 可能同时处理同一仓位。`main.py` 不持有 `_guard_locks`，互斥不成立。
- 重复保护单的实际后果取决于下单参数：TP 带 `reduce_only=True`（`executor.py:1018`），`place_sl` 的参数本步没有展开，留到下单审查确认。

## 3. 挂单超时或被交易所撤销时，本地状态如何更新？🟡

- 轮询机制：有。`PositionManager` 的巡检循环（`position_manager.py:830`）和 `main.py` 的 `patrol_once`（`position_manager.py:3150`）会对 `pending` 记录调用 `_check_pending_expire_static`，周期约 5 分钟。
- 处理逻辑（1766-1810 行）：订单不在 `get_open_orders` 里时，用 `get_order` 查终态；`CANCELED/EXPIRED/REJECTED` 删本地记录，`FILLED` 走补挂；查不到则等 24 小时。到期后主动撤单并删记录（1821-1838 行），`-2011/-2013` 视为已不存在，处理合理。
- 实时感知只覆盖成交：WS 路径（约 603-650 行）只处理 `FILLED`，`CANCELED/EXPIRED` 没有实时处理，最长要等一个巡检周期。期间这条 `pending` 仍占用挂单槽位，属于低风险。
- **部分成交后被撤/过期：** 1780-1783 行对 `CANCELED/EXPIRED` 一律直接删除本地记录，没有检查 `executedQty`。如果入场单部分成交后被撤销或 GTD 到期，交易所里会留下一个没有保护单、也没有本地记录的小仓位；超时主动撤单路径（1821-1838 行）同样先撤单后无条件删记录。这个孤儿仓位之后会掉进问题 1 的孤儿路径（推断）。
- **dead code：** `get_order` 失败返回 `{}` 而不是 `None`，所以 1803-1808 行的"查询失败且超过 24 小时清除"分支永远不会进入；`except` 也因为 `_retry` 吞掉异常而基本不触发。
- **同步阻塞：** 1769、1774、1823 行在 `async` 函数里直接调用同步 `api.*`，没有走 `_run_sync`；第一步已提到，这里属于同类问题。
- **已有保护单是否仍存活：** 本步读过的部分里，没有看到对已有 SL/TP 存活状态的周期性检查，只有启动和 WS 重连时的对账。运行期是否由 `_adjust_sl_tp` 等路径覆盖，需要后续通读 `position_manager` 确认。

## 4. `pos_state.json` 损坏时的恢复路径是什么？🔴

- 路径：`_read_json` 解析失败时调用 `_backup_corrupted_pos`（`state.py:58-67`）：改名为 `pos_state.json.corrupted_<毫秒时间戳>`，把内存标志 `_POS_STATE_CORRUPTED` 置为真，并返回默认值 `[]`。启动时的 `_ensure_files` 也走同样的备份路径，然后写入新的空 `[]`。
- 标志只在本进程有效：该标志拦截本进程内所有持仓写入（`add_position`、`update_position`、`remove_*`）。进程一重启，新文件是合法的空数组，标志归零，系统以空持仓正常启动，并且不再有任何告警。
- 没有从交易所重建的路径：真实仓位在交易所仍在，`_sync_positions_on_startup` 会把它们当孤儿处理，只补保护单，不建档（见问题 1）。`position_manager` 的巡检只管 `pos_state` 里的记录，这些仓位不会再有时间止损、移动止损等维护，也不计入槽位上限。`executor` 的 `unpersisted_positions.jsonl`（`executor.py:1192-1223`）只是追加写的人工核查日志，不会被重新加载。
- 空文件被静默接受：`_read_json` 和 `_ensure_files` 都对"空内容"直接返回默认值，不视为损坏（`state.py:77`、`84`，以及 `_ensure_files` 的 `if raw:`）。也就是说，因断电造成的零字节文件（见 `_write_json` 没有 `fsync`）不会触发备份，不会置标志，也不会告警。
- 备份失败仍会覆盖原文件：`_backup_corrupted_pos` 在改名失败时日志写"原文件保留"，但 `_ensure_files` 随后无条件执行 `_write_json(path, default)`，仍然会覆盖掉这份损坏的原文件。
- 备份文件会无限累积，没有保留数量限制，也没有告警机制之外的提示（只在日志中输出）。

## 5. `_ensure_files` 在启动时做了哪些检查？校验是否足够？🟡

- 做了什么（`state.py:702-731`）：创建 `logs/` 目录；对五个状态文件，不存在则写默认值；存在则尝试 `json.loads`，失败则重置为默认值。
- 触发时机是模块导入（731 行），任何导入 `state` 的进程都会执行，包括测试、回测、`run_once.py`、分析工具。如果有第二个 `main.py` 实例，也会同时执行并可能互相覆盖。
- 校验不足：
  - 只校验"能否解析成 JSON"，不校验结构。`pos_state` 应为列表，若内容是合法的 JSON 对象，校验也通过，随后访问时才出错。
  - 不校验记录字段（`pos_id`、`symbol`、`status` 等）。
  - 空文件按合法处理（见问题 4）。
- 只有 `pos_state` 有备份：其他四个文件（熔断、冷却、待确认、OI 缓存）损坏时直接重置，不备份。最重要的是 `circuit_breaker.json`：被重置后 `is_open` 回到 `False`，已触发的熔断会在没有任何提示的情况下解除（推断，依据 `_CB_DEFAULT`）。冷却和待确认状态丢失同理。
- 范围不全：`_ensure_files` 只覆盖这五个文件。`funding_arb_state.json` 在同一个文件后面单独定义（`_FA_FILE`），`risk_manager` 的回撤、冲击状态，以及 `trigger_detector` 的状态文件都不在它的范围内。
- `_read_json` 用文件名里是否含 `"pos_state"` 来区分是否备份（72 行），较脆弱。

## 总结

| 问题 | 严重程度 | 影响范围 |
|---|---|---|
| 接口失败返回空列表，被当成"交易所无持仓"，启动与 WS 重连时会清空全部本地持仓记录 | 🔴 | 持仓记录、止损止盈维护、槽位计数 |
| 孤儿仓位（交易所有、本地无）只补保护单不建档，且方向取自本地默认值 `long` | 🔴 | 空单孤儿补挂失败，多单孤儿无人管理 |
| 启动对账补挂会重复挂 SL，只缺 SL 时还会重挂 TP，新 SL 的 id 不回写 | 🔴 | 保护单堆积，每次重启或重连再叠加 |
| `main.py` 与 `position_manager` 两份补挂逻辑不一致（锁、查已有 TP、校验数量、倍数） | 🟡 | 行为差异、并发补挂竞态 |
| 保护单只部分成功时仍写 `status = open`，调用方不检查 `protection_status` | 🟡 | 半保护仓位无明确重试 |
| 部分成交后撤单或过期，本地记录直接删除，残留无保护小仓位 | 🟡 | 孤儿仓位来源 |
| `pos_state` 损坏后以空持仓重启，标志不跨重启，无从交易所重建的路径 | 🔴 | 真实仓位失去管理 |
| 空文件被静默当作合法（断电后零字节） | 🟡 | 持仓记录静默丢失 |
| 备份失败仍会覆盖原损坏文件；备份无保留策略 | 🟡 | 取证与人工恢复 |
| 其余四个状态文件损坏时无备份直接重置，包括熔断器 | 🟡 | 已触发的熔断被无声解除 |
| `_ensure_files` 在导入时执行，只校验 JSON 语法，不校验结构，覆盖文件不全 | 🟡 | 多进程、测试脚本误改、脏数据 |
| 对账假设对冲模式，缺少启动时校验；`ps_map` 按 symbol 建索引 | 🟡 | 单向模式下可能全部记录被清除 |
| `CANCELED/EXPIRED` 无 WS 实时处理，`get_order` 失败分支为 dead code | 🔵 | 最多一个巡检周期的延迟 |
| `_check_pending_expire_static` 在 `async` 内直接调用同步 HTTP | 🟡 | 事件循环阻塞 |

> 注：async 内同步 I/O 问题在 feature/passive-trigger 分支已有部分修复
> （state.py OI缓存、main.py WS重连、trigger_detector.py 候选池缓存），
> 本仓库当前代码未包含这些修复，审查结论基于 main 分支现状。
