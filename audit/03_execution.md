# 审查 03：下单执行

范围：`fx/executor.py`、`fx/shared/binance_api.py`（`place_order`、`place_sl`、`_retry`、`_signed_post`、`get_position` 等）、`fx/position_manager.py` 的 `_补挂保护单`、`fx/shared/signal_watcher.py` 的触发与应急补止损路径、`fx/scanner.py` 的 funding_arb 开仓路径。

阅读说明：`executor.py`（2041 行）我逐段读了预检（60-326 行）、参数校验（394-445 行）、开仓下单与等待成交（657-856 行）、保护单（907-1051 行）、`_execute_async` 的入口与下单段（1446-1475、1760-1860 行）、二次验证平仓（1880-1905 行），其余部分（风控计算、通知、成交后写仓位、`_on_position_closed` 等）只做了关键字检索，没有逐行通读。标"推断"的结论依据代码路径，尚未在测试网复现。

图例：🔴 严重问题 / 🟡 中等问题 / 🟢 没有问题 / 🔵 建议改进

## 1. 重复开仓防护是否可靠？🔴

- 防护有三层，但都不是"下单前原子占位"：
  - `_precheck` 读 `pos_state`，同标的存在 `open/pending/patching` 就拒绝（`executor.py:178-190`）。
  - 查交易所同方向入场挂单兜底（192-207 行），只查挂单，不查交易所已有持仓。
  - `state.add_position` 对同标的拒绝（`state.py:155-170`），但它在订单已经发出之后才被调用：挂单路径在 `_write_pending`（`executor.py:1849`），成交路径在 `_write_position`（1940 行），最终都经 `_persist_position_record`（1198 行）。
- 因此 `add_position` 防的是"重复建档"，不是"重复下单"。第二个并发者的订单已经在交易所，`add_position` 返回 `False` 后，只是写一条 `unpersisted_positions.jsonl` 并告警（1190-1223 行）。交易所里的订单继续存在，没有任何代码去撤它（本步读过的部分没有）。
- 兜底检查的漏洞：1-2b 的交易所检查在 `get_open_orders` 失败时得到 `[]`（该函数失败返回空列表，见 `binance_api.py:622-630`），检查直接通过；外层还有 `except Exception: pass`（206-207 行）。
- 没有进程内的按标的互斥：`executor` 里没有 `Lock`。`signal_watcher` 有按标的的 `asyncio.Lock`（`signal_watcher.py:1240`），但只覆盖它自己的触发路径。
- **主扫描与 funding_arb 同时触发同一标的：**
  - 主扫描经 `signal_watcher` 触发，在主事件循环里 `await execute_signal`（`scanner.py:3066-3109`）。另有扫描线程里的直接调用（`scanner.py:3146` 的 `execute_signal_sync`）。
  - funding_arb 在线程里调用 `execute_signal_sync`（`scanner.py:4196`），后者新建独立事件循环（`executor.py:1432-1445`）。两条路径不共享任何锁。
  - 从 `_precheck` 到 `add_position` 之间要经过拉行情、多次查账户、设置杠杆、提交订单、REST 确认，耗时数秒。两边都在此窗口内通过 `_precheck` 时，会各发一张入场单。后到者的 `_precheck` 若恰好在先到者下单之后，才会被 1-2b 的挂单检查拦住。
  - 持仓数量上限也是先检查后执行（`check_position_limits` → 稍后才建档），并发时可能短暂超限。
- 另外，funding_arb 的信号 `entry_mode` 为 `immediate`，入场价取标记价（`scanner.py` 约 4160 行），与主扫描同标的时，方向可以相反；`_precheck` 对"同标的"一律拒绝，但并发窗口内同样无法拦住。
- 小结：单进程、串行触发时防护基本有效；一旦存在并发路径（funding_arb 线程、第二个 `main.py` 实例，见报告 01），会漏。

## 2. 精度处理是否正确？🟡

- 做精度截断的位置：
  - `_round_price`、`_round_qty`、`_round_sum_qty`（`executor.py:119-142`）：先 `round(x/step, 10)` 消除浮点误差再向下取整，处理得比较细致。
  - 开仓单在 `_place_open_order`（695-696 行）截断；保护单在 `_place_guards_async`（916-939 行）截断；二次验证平仓直接用 `fill_qty`（1887-1892 行）。
- 精度来源：`exchangeInfo` 里的 `PRICE_FILTER.tickSize` 和 `LOT_SIZE.stepSize`，缓存 10 分钟（84-110 行）。
- 可能被交易所拒绝的情形：
  - **查不到精度时静默使用默认值**：`_get_precision` 在缓存里没有该标的时返回 `{"tick": 0.01, "step": 0.001}`（113-116 行）。对于最小变动价位更小的低价币，会被截成 0 或错位。
  - **没有处理最小下单金额/数量**：只解析了 `PRICE_FILTER` 和 `LOT_SIZE`（100-104 行），没有 `MIN_NOTIONAL`、`MARKET_LOT_SIZE`（市价单的数量上下限）、`PERCENT_PRICE`。`_validate_order_params` 也只检查上限（394-440 行），不检查下限。
  - **科学计数法（推断）**：开仓价格和数量用 `str(float)`（695-696 行），保护单止损触发价和数量用 `f"{x}"`（`binance_api.py:922-923`）。Python 对小于 1e-4 的浮点数会输出 `5.1e-05` 这类形式。对价格低于 0.0001 的标的，这类字符串有可能被交易所判为非法参数。需要在测试网用这类标的验证。
  - **TP 数量取整后的残量：** `tp1 + tp2` 向下取整后可能小于成交量，差额会记入 `unallocated_qty`（1039 行），但没有后续处理。
- **错误信息丢失导致无法区分精度类拒绝：** 见第 5 节。

## 3. 持仓模式假设是否一致？🟡

- 代码整体假设对冲模式（LONG/SHORT 分开），但这个假设是硬编码的：`_HEDGE_MODE: bool = True`（`binance_api.py:391-395`），注释写"直接确定，不再动态查询"。全仓库没有查询 `dualSidePosition` 的代码（检索无结果）。
- 混入 `BOTH` 的位置：
  - `signal_watcher.py:1412-1420`：把 `positionSide == "BOTH"` 当作可匹配，并在 `BOTH` 时改用信号方向。
  - `position_manager.py:301`：`ao.get("positionSide") in (pos_side, "BOTH")`。
  - 而 `main.py` 的启动对账只接受 `LONG/SHORT`（479 行），单向模式下会把全部记录当僵尸清掉（见报告 02）。
- `place_order` 和 `place_sl` 都保留了单向模式分支（`reduceOnly`），默认不可达，等于没有测试过的代码。
- 参数命名不统一：有的调用传 `position_side=`，有的传 `positionSide=`（如 `executor.py:1890`，通过 `**extra` 进入，最后由 `params.update(extra)` 覆盖，结果是对的，但很脆弱）。
- 账户实际模式与标志不一致时：标志为真但账户是单向，所有带 `positionSide` 的订单会被拒；反过来标志为假但账户是对冲，则缺少 `positionSide` 同样被拒。两种都是失败而非反向开仓，但没有任何启动时的校验或提示。

## 4. reduceOnly 是否正确使用？🟡

- **对冲模式下 `reduceOnly` 根本没有发送：** `place_order` 在对冲模式不带该参数（`binance_api.py:557-561`，注释"Hedge Mode 不支持 reduceOnly"）。因此 `executor.py:1018、1029` 传的 `reduce_only=True` 是空操作。
- 各类订单的实际约束：
  - 开仓单（`executor.py:721-747`）：不带 `position_side`、不带 `reduce_only`，由 `place_order` 按 `BUY→LONG、SELL→SHORT` 推断，正确。
  - 止盈单：`LIMIT` + `positionSide` + 平仓方向，靠 `positionSide` 保证只能减仓。
  - 止损单（`place_sl`，`binance_api.py:909-982`）：`STOP_MARKET` 条件单，对冲模式只带 `positionSide`，不带 `reduceOnly`。
  - 二次验证平仓（`executor.py:1887-1892`）：`MARKET` + `positionSide`，没有 `reduce_only`。
- 能不能反向开仓：在对冲模式下，`SELL + positionSide=LONG` 只会减多，不会开空，所以反向开仓的风险很低。风险转为"数量超过持仓被拒"（推断为被交易所拒绝）或"孤立的 TP/SL 挂单残留"，后者要看撤单逻辑（`position_manager` 的平仓路径，本步未通读）。
- `_validate_sl_order` 这道"最后一道防线"（`binance_api.py:395` 起）只在 `place_order` 的 `STOP*` 分支里生效，但止损实际都走 `place_sl`（`executor.py:986`、`position_manager.py` 多处、`signal_watcher.py:1422`、`main.py:551`），**不经过它**。也就是说，止损的方向和距离校验在实际路径上没有被执行。

## 5. 重试和限频是否合理？🔴

- 重试次数与间隔：
  - `_retry` 总尝试 = `1 + API_REQUEST_MAX_RETRY`（`config.py:335`，当前为 1），即 2 次。
  - 间隔：连接类错误重建会话后等 0.5 秒；其他错误固定等 1 秒（`binance_api.py:195、199`）。
  - 对任何异常一视同仁，**包括业务拒绝**（如 `-2019` 保证金不足、`-4024` 价格不合法、精度类错误）：这些重试毫无意义，只会多打一次请求。
- **错误信息被吞掉，调用方的错误分支成了死代码：**
  - `_retry` 耗尽后返回 `None`，不再抛出（`binance_api.py:200-204`）；`place_order` 再转成 `{}`。
  - `_place_open_order` 的 `except` 分支里处理 `-4024`、`-2019` 的逻辑（`executor.py:753-798`），包括写入 15 分钟保证金暂停标志，在正常路径上永远不会被触发，因为异常根本传不到那里。
  - 调用方最终只看到"接口返回无 orderId: {}"，原因丢失，无法区分精度错误、保证金不足、限频或网络问题。
- 重试是否会重复下单：
  - 对写请求有保护：遇到"结果未知"的超时/断连（`_UNKNOWN_RESULT_MARKERS`，`binance_api.py:115-117`）时不再重试，抛出 `WriteTimeoutError`，`place_order` 去查挂单对账，找到唯一匹配的订单就当作已提交（575-589 行），做得对。
  - 缺口一：对账只看 `get_open_orders`，`MARKET` 单或已立即成交的限价单不会在挂单里，函数返回 `{}`，调用方认为未提交，但订单可能已成交（代码注释自己也承认，587 行）。这会产生没有记录的仓位。
  - 缺口二：没有使用 `newClientOrderId` 做幂等（全仓库检索无结果）。
  - 缺口三：HTTP 5xx 或 Binance 的"状态未知"类错误码（如 `-1007`）不在超时特征串里，可能被当作普通错误重试一次（推断，需核实 `-1007` 的返回形式）。
- **限频：** 在 `binance_api.py`、`ws_listener.py`、`executor.py` 里检索 `429`、`418`、`-1003`、`Retry-After`、已用权重头，均无结果；没有客户端限速器或信号量。扫描、巡检、补挂、对账都通过默认线程池并发调用同步 REST。出现限频或封禁时，只会表现为通用失败，不会降速。
- 触发重试的设计与冷却冲突（推断）：`signal_watcher` 在调用 `on_trigger` 之前就写入方向冷却时间（`signal_watcher.py:1131-1132`，冷却 300 秒，439 行），`PreOrderError`（下单前的暂时性失败）后虽然保留了条目等待下次触发（1233-1243 行），但下次触发若在 5 分钟内，会命中冷却，直接 `pop` 掉条目（1126-1128 行）。"可安全重试"的设计在冷却窗口内实际变成丢弃信号。
- 开仓单不重试（`_place_open_order` 文档串写"失败直接返回 None，不重试"），但底层 `_retry` 仍会重试一次，文档与实现不一致。

## 6. signal_watcher 绕过 executor 的路径 🟡

对第 1 步结论的修正：`signal_watcher` 开仓**没有**绕过 `executor`。触发后调用 `entry.on_trigger`（`signal_watcher.py:1245`），由 `scanner.py:3066-3109` 注入，其中 3070-3075 行 `await _executor_mod.execute_signal`。`on_trigger` 的内部我没有逐行验证，但没有发现直接下入场单的代码。

`signal_watcher` 里直接调用交易所接口的只有这些：

| 位置 | 操作 | 评价 |
|---|---|---|
| `signal_watcher.py:539、543` | 查持仓、查挂单 | 只读 |
| `signal_watcher.py:1310、1318` | 撤单、查单 | 失败返回 `{}`，调用方有处理 |
| `signal_watcher.py:1405-1422` | `_guard_unknown_fill` 应急补止损 | 见下 |

`_guard_unknown_fill`（撤单结果未知时，查到有持仓就补止损）与 `executor._place_guards_async` 的差异：

- **价格和数量没有取整：** 止损价直接取 `pre_params.sl_price`，数量是 `abs(amt)`，没有经过 `_round_price/_round_qty`；`place_sl` 又用 `f"{x}"` 拼字符串（`binance_api.py:922-923`）。`executor.py:934-935` 的注释正好说明了这种浮点噪声会导致止损提交被拒。
- **成功判断是错的：** `place_sl` 失败时返回 `{}` 而不抛异常，但 1421-1427 行只用 `try/except` 包住，只要不抛异常就记"已补止损"并返回 `True`。止损实际没挂上时，日志和返回值仍显示成功。
- **没有止盈、没有建档：** 只补 SL，不补 TP，不调用 `add_position`，这个仓位不在 `pos_state` 里，后续巡检、时间止损、移动止损都不会管它（与报告 02 的孤儿仓位问题叠加）。
- 方向取自信号，`BOTH` 被当成可匹配项（1412-1420 行）。
- 没有任何风控检查，但这是保护动作，不挂入场风控本身合理；问题在于缺少"补完后记录并交给 `PositionManager`"这一步。

## 总结

| 问题 | 严重程度 | 影响范围 |
|---|---|---|
| `add_position` 在下单之后才拦截，只防重复建档不防重复下单；`executor` 无按标的互斥 | 🔴 | 并发路径（funding_arb、第二个实例）下重复入场单 |
| 主扫描与 funding_arb 线程共用标的时，先检查后执行存在秒级窗口，仓位上限同样可能被短暂突破 | 🔴 | 超仓、重复开仓 |
| 兜底的交易所挂单检查在接口失败时静默通过 | 🟡 | 去重在网络不稳时失效 |
| `_retry` 吞掉所有异常，错误码丢失；`executor` 里 `-4024`/`-2019` 分支和保证金暂停成为死代码 | 🔴 | 无法区分失败原因，保护机制不生效 |
| 对业务拒绝也重试，且重试间隔固定 | 🟡 | 无意义的请求，增加限频压力 |
| 没有任何限频处理（429/418/-1003），没有客户端限速 | 🟡 | 触发限频或封禁时无降速 |
| 无 `clientOrderId` 幂等；MARKET 单和已成交限价单的写超时对账找不到 | 🟡 | 订单已成交但无记录 |
| 精度：默认 tick/step 静默兜底；未处理 `MIN_NOTIONAL`、`MARKET_LOT_SIZE`、`PERCENT_PRICE`；小数字符串可能出现科学计数法（推断） | 🟡 | 低价币、小额订单被拒 |
| 持仓模式硬编码为对冲，无启动校验，且混入 `BOTH` 判断 | 🟡 | 账户模式不符时全部订单失败或对账清空记录 |
| 对冲模式下 `reduceOnly` 不发送，`reduce_only=True` 是空操作；反向开仓风险低，但超量单靠交易所拒绝 | 🟡 | 平仓类订单的约束全靠 `positionSide` |
| `_validate_sl_order` 在止损的实际路径（`place_sl`）上不生效 | 🟡 | 止损方向和距离无最后校验 |
| `signal_watcher` 应急补止损：未取整、成功判断失真、不补 TP、不建档 | 🟡 | 未知成交场景下仓位无人管理且告警误导 |
| `PreOrderError` 的重试与 5 分钟方向冷却冲突，会丢弃信号（推断） | 🔵 | 暂时性故障后丢失本可入场的信号 |
| 文档与实现不一致（"开仓不重试"实际会重试一次） | 🔵 | 可维护性 |

> 注：async 内同步 I/O 问题在 feature/passive-trigger 分支已有部分修复
> （state.py OI缓存、main.py WS重连、trigger_detector.py 候选池缓存），
> 本仓库当前代码未包含这些修复，审查结论基于 main 分支现状。
