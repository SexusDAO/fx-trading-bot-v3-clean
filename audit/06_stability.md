# 审查 06：稳定性与安全

范围：`fx/shared/ws_listener.py`、`fx/main.py` 的异常处理与退出流程、`fx/daemon_start.py`、`fx/watchdog.sh`、`fx/.env.example`，以及全仓库对密钥和敏感字段的检索（`config.py`、`shared/notifier.py`、`shared/clawby_adapter.py`、日志语句）。

阅读说明：`ws_listener.py`（735 行）通读了 userData 重连、补偿、续期（203-257、320-380、641-690 行），markPrice 与强平流的重连循环只看了退避部分（437-465、542-562 行）。日志脱敏部分是用关键字检索全仓库后抽读，不能保证没有遗漏。**本报告里出现的密钥一律只保留前 6 位**，完整值请直接在源文件里核对。标"推断"的结论依据代码路径，没有运行验证。

图例：🔴 严重问题 / 🟡 中等问题 / 🟢 没有问题 / 🔵 建议改进

## 1. WS 断线重连是否有退避策略？重连期间保护单如何维护？

🟡 重连有指数退避，但断线期间的 REST 补偿实际没有启用，重连后的对账又有已知缺陷。

- 退避策略（🟢）：userData 流在断线后等待 `delay` 秒再重连，`delay` 从 `WS_RECONNECT_BASE`（3 秒）起每次翻倍，封顶 `WS_RECONNECT_MAX`（60 秒），连接成功后重置（`ws_listener.py:70-71、325、351、377-378`）。markPrice 流和强平流也有同样的退避循环（437-465、542-562 行）。拿不到 listenKey 时固定等 10 秒重试（335-339 行）。
- 退避没有随机抖动：多个流同时断线时会在同一时刻同时重连。
- 续期与过期处理（🟢）：listenKey 每 20 分钟续期（`KEEPALIVE_INTERVAL`，58 行；`_keepalive_loop`，656-686 行），续期失败或收到 `listenKeyExpired` 事件时主动关闭连接触发重建（388-392、641-654 行）。续期和申请 listenKey 都放进有界线程池，避免阻塞事件循环（328-334、668 行），这一处处理比其他模块好。
- **断线期间的 REST 补偿是死代码（推断）：** `_mark_user_stream_down`（203-212 行）在 `self._disconnect_callbacks` 为空时直接返回，不会启动 `_compensation_loop`（214-243 行）。全仓库只有 `main.py:731` 注册了 `on_reconnect`，没有任何地方调用 `ws.on_disconnect(...)`（检索只在 `market_data_hub.py` 里有同名私有方法）。所以断线期间不会做主动的订单对齐，补偿任务从不运行。
- **重连后的对账是"发出去就不管"：** 重连成功后用 `ensure_future(_cb())` 触发回调（`ws_listener.py:355-362`），回调就是 `_sync_positions_on_startup`（`main.py:728-731`）。这个函数的缺陷见报告 02：接口失败被当作无持仓，会清空本地记录；重复补挂保护单；不加锁，与巡检并发。
- 保护单在断线期间的状态：止损（`STOP_MARKET` 条件单）和止盈（限价单）都在交易所侧，断线不影响其触发。受影响的是本地的软件逻辑：移动止损、时间止损、挂单过期、成交事件的实时感知（`PositionManager` 靠 WS 感知成交，`position_manager.py:603-650`）。漏掉的成交事件只能等 5 分钟一轮的巡检靠 REST 发现（代码注释自己标注为"已知 KI-001"）。
- 重连回调没有串行化：连续快速断线重连时，多个 `_sync_positions_on_startup` 可能同时跑，没有"已在运行则跳过"的保护（推断）。

## 2. 未捕获异常会不会让整个进程崩溃？

🟡 主流程防护较多，崩溃后由守护拉起；但内部后台任务没有被统一监控，守护本身也有风险。

- 防护做得对的地方（🟢）：
  - `main.py` 里的循环任务都在 `while True` 内部包了 `try/except`，不会因为单次异常退出（如 `_loop_patrol`、`_loop_refresh_pool`、`_loop_funding_arb`）。
  - 入口兜底：`asyncio.run(main())` 外层捕获 `Exception`，记 `critical` 后 `exit(1)`（`main.py` 末尾）。
  - 事件循环异常处理器会记录未捕获的协程异常（`main.py:838-842`）。
  - 扫描线程池的异常通过 `add_done_callback` 取走，避免 `exception was never retrieved`（205 行）。
  - `_watchdog` 任务发现 `tasks` 里有任务意外结束，就触发整体退出，交给守护重启（846-857 行）。
- 缺口：
  - 受监控的只有 `main.py` 里创建的那一批任务（780-802 行）。`PositionManager` 内部的四个任务（`position_manager.py:386-389`，用 `ensure_future` 创建）、`signal_watcher` 的 GC、轮询、复核、BTC 护卫任务，以及 `ws_listener` 的五个任务，都不在 `_watchdog` 的监控范围内。这些任务如果因未处理的异常结束，进程仍然存活，但对应功能（如成交感知、挂单复核）会悄悄停止（推断，取决于各循环内部是否有兜底）。
  - 没有设置 `threading.excepthook` / `sys.excepthook`：后台线程（`execute_signal_sync` 的子线程、`liq_cache`、`hub` 线程）的未捕获异常只会打印到 stderr。
  - 事件循环被同步调用卡住（报告 01 第 5 节）时，看门狗线程只会 dump 堆栈，不会触发重启。
- 守护层：
  - 重启间隔固定 15 秒，上限 `daemon_start.py` 500 次、`watchdog.sh` 200 次（超过后停止并要求人工介入），没有指数退避，也没有"短时间内连续崩溃"的熔断判断，崩溃循环会一直重复"启动 → 对账 → 崩溃"。
  - 对账（报告 02 的第 0 节）发生在每次重启，崩溃循环会把其中的缺陷放大。
  - 内存守护：`daemon_start.py:357-372` 超过 3000MB 时主动发 `SIGTERM` 重启，这是合理的保护；但同时 `main.py:52` 常开 `tracemalloc`，会让内存占用本身上升。
  - 进程收到 `SIGTERM` 后：先 `cancel` 任务，再停 `pm`、`signal_watcher`、`ws`（`main.py:867-890`）。停止过程中没有"撤销未成交入场单/确认保护单齐全"的步骤，交给下次启动的对账处理。

## 3. 密钥是否只通过环境变量注入？代码里有没有敏感信息残留？

🔴 Binance 密钥确实只走环境变量，但代码里仍有三处硬编码的第三方密钥/令牌和账号标识，并已随快照推送到了 GitHub 仓库。

**做得对的部分（🟢）**

- Binance API Key/Secret 只从环境变量读取（`config.py:18-19`、`shared/binance_api.py` 开头），`.env` 在 `.gitignore` 里，`watchdog.sh` 也用 `${VAR:?...}` 强制要求提前设置，不在脚本里硬编码。
- 全仓库按常见格式检索（OpenAI `sk-`、GitHub `ghp_`、Telegram bot token、64 位十六进制私钥）没有发现命中。

**硬编码的敏感信息（🔴）**

| 位置 | 内容 | 说明 |
|---|---|---|
| `shared/clawby_adapter.py:70` | `CG_API_KEY` 的默认值为 `cg_998dad…`（完整值在源文件里） | CoinGlass 数据源密钥，作为 `os.environ.get` 的默认值直接写在代码里。`.env.example` 注明该项"有内置默认值，可不填"，等于鼓励使用这把写死的 key |
| `shared/notifier.py:35` | `_GW_TOKEN = "560e77…"` | 本地 OpenClaw 网关的 Bearer 令牌，请求头里带着它调用 `/tools/invoke`（36-37 行）。能调用该接口的人可以向用户会话推送消息、执行网关工具 |
| `shared/notifier.py:31` | `_JARVIS_TARGET`（一个用户 ID 加线程 ID） | 推送目标标识，属于个人账号标识 |

- 这三处现在都在本审查仓库的快照里（已推送到私有仓库 `fx-trading-bot-v3-clean`），原仓库 `fx-trading-bot-v3` 里同样存在。建议把这几个值都视为已泄露：到各自的服务端轮换，然后改为只从环境变量读取。仓库历史中的旧值需要单独清理或重建仓库，仅改当前代码不够。
- 其他和凭据相关的风险：
  - 代理地址 `socks5://user:pass@host:port` 带用户名密码，通过 `BINANCE_PROXY`/`PROXY_URL` 环境变量传递并注入子进程环境（`daemon_start.py:333-340`、`watchdog.sh`），同机其他有权限的用户可通过进程环境读取。
  - `watchdog.sh` 的 `send_telegram`（约 40-60 行）把 Telegram token 和消息内容拼进 `python3 -c` 的源码字符串，令牌会出现在进程命令行里，可被 `ps` 看到；消息中含引号时还会破坏脚本。
  - `DEPLOY.md` 第 2 步用 `git clone https://<token>@github.com/...`，令牌会写进 `.git/config`。
  - `.env.example` 声称"带 [必填] 的项缺失时程序会直接报错退出"，但 `config.py:21-26` 和 `binance_api.py:55` 只在缺少时打一条 warning，说明与实现不符，容易让人以为缺失会被拦住。
  - 把 `OPENAI_API_KEY` 和行情上下文发往第三方中转站（`daemon_start.py:335`、`watchdog.sh` 的 `https://novaapi.ai`），以及 `bykaranteli.com`、`openclawby.com` 等数据中转，属于信任边界，需要有意识地接受。
- 文件权限：`daemon_start.py:422` 在守护化时执行 `os.umask(0)`，之后由 `main.py` 创建的日志、`pos_state.json`、`card_state.json` 等文件默认权限为 0666（推断：Python `open()` 默认 0666 减去 umask），同机其他用户可读写。持仓状态文件被篡改会直接影响对账和风控。

## 4. 日志里会不会打印订单 ID、账户余额等敏感字段？

🟡 订单 ID 在日志里大量出现，余额偶有出现，listenKey 前缀会被记录；没有看到 API 密钥被直接记录。

- 订单 ID：开仓、撤单、保护单提交、对账都会把 `orderId`/`algoId` 写入日志（如 `executor.py:751、843、999、1844、2017`，`position_manager.py:967、977、1151、1782、1906、1943、2987`，`signal_watcher.py:1346、1391`，`binance_api.py:583`）。订单 ID 本身不是凭据，但配合时间、标的和价格会暴露交易行为，日志文件需要按敏感数据对待。
- 余额：保证金不足等拒绝路径会记录可用余额数值（如 `executor.py` 中 "可用余额不足（杠杆…）"一类语句）；`risk_manager` 与定时心跳通知里也会带权益、保证金率等（`main.py:262-267` 的心跳消息只含信号计数，未含余额）。这些进入日志和 Jarvis/Telegram 推送，需要确认推送渠道可信。
- listenKey：`ws_listener.py:342` 打印 `url[:60]`，而 `USER_DATA_URL_TPL` 为 `config.BINANCE_WS_URL + "/ws/{listen_key}"`（76 行），URL 前缀约 30 个字符，截取 60 个字符会带出 listenKey 的前一部分。listenKey 可以订阅该账户的 userData 流，部分泄露也建议避免（推断，具体长度取决于配置的 WS 地址）。
- API Key/Secret：本步检索的日志语句里没有直接输出（`ai_advisor.py:227`、`binance_api.py:55` 只写"未设置"）。`_signed_post` 抛出的异常信息只含错误码和消息，不含签名参数（`binance_api.py:236-241`）。请求失败时 `requests` 的异常文本可能包含完整 URL 和查询串（带 `timestamp` 与 `signature`），`_retry` 在 `logger.warning(f"[api] _call 第{attempt+1}次失败: {e}")`（`binance_api.py:199`）里原样输出，需要确认不会带出签名（推断，未验证具体异常文本）。
- 交易日志：`trade_log.jsonl` 等把仓位、盈亏、价格完整落盘（`position_manager.py:1065-1066、2209-2210`），位于被 `.gitignore` 排除的 `logs/` 下，但随 `umask(0)` 权限过宽。
- 日志轮转：`system.log` 10MB × 5 份（`main.py:140-145`），`daemon.py` 另有 50MB 阈值的手工归档（`daemon_start.py:232-246`），两套轮转并存，归档目录没有大小上限。

## 待处理清单

- [ ] 🔴 `shared/clawby_adapter.py:70`：移除硬编码的 CoinGlass key 默认值，轮换该 key，并清理 `.env.example` 里"可不填"的说明。
- [ ] 🔴 `shared/notifier.py:35`：移除硬编码的网关 Bearer 令牌，轮换，改为环境变量；`notifier.py:31` 的推送目标标识也改为配置。
- [ ] 🔴 原仓库与本审查仓库的 git 历史里已有上述值：需要重建仓库或清理历史，并确认私有仓库的访问人员。
- [ ] 🟡 `shared/ws_listener.py:207-208`、`main.py:731`：`on_disconnect` 从未被注册，断线期间的 REST 补偿任务是死代码；需要注册并验证。
- [ ] 🟡 `shared/ws_listener.py:355-362`：重连回调 fire-and-forget，需要串行化并避免与巡检并发；回调内容见报告 02 的缺陷。
- [ ] 🟡 `shared/ws_listener.py:377-378`、`437-465`、`542-562`：重连退避加随机抖动。
- [ ] 🟡 `main.py:780-857`、`position_manager.py:386-389`、`signal_watcher.py:1484-1597`、`ws_listener.py:269-272`：内部后台任务不在 `_watchdog` 监控内，需要统一监控任务存活。
- [ ] 🟡 `main.py`（入口）：增加 `threading.excepthook` 与 `sys.excepthook`，记录后台线程的未捕获异常。
- [ ] 🟡 `daemon_start.py:324-399`、`watchdog.sh`：重启增加指数退避和"短时间连续崩溃"的熔断，并保证只有一个守护存在（见报告 01）。
- [ ] 🟡 `daemon_start.py:422`：`os.umask(0)` 导致文件权限过宽，应收紧并显式设置状态/日志文件权限。
- [ ] 🟡 `watchdog.sh`（`send_telegram`）：令牌出现在进程命令行；消息拼接进脚本字符串，需要改为经环境变量或标准输入传递。
- [ ] 🟡 `.env.example` 与 `config.py:21-26`、`binance_api.py:55`：说明"缺失会报错退出"与实现不符，缺少必填项时应直接拒绝启动。
- [ ] 🟡 `shared/ws_listener.py:342`：不要记录含 listenKey 的 URL。
- [ ] 🟡 `shared/binance_api.py:199`：失败日志里的异常文本可能包含签名参数，需要脱敏。
- [ ] 🟡 订单 ID 与余额日志（见第 4 节列出的位置）：明确日志的访问权限和保留周期；推送渠道（Jarvis/Telegram）中避免带余额。
- [ ] 🔵 `DEPLOY.md`：改为不把令牌写进克隆地址（使用凭据助手或只读部署密钥），并删除无关项目的内容。
- [ ] 🔵 `daemon_start.py:335`、`watchdog.sh`：第三方 LLM 中转站的地址和模型写死在启动脚本里，建议统一到配置；评估上传到第三方的数据范围。
- [ ] 🔵 `daemon_start.py:232-246` 与 `main.py:140-145`：两套日志轮转并存，归档目录无上限。

> 注：async 内同步 I/O 问题在 feature/passive-trigger 分支已有部分修复
> （state.py OI缓存、main.py WS重连、trigger_detector.py 候选池缓存），
> 本仓库当前代码未包含这些修复，审查结论基于 main 分支现状。
