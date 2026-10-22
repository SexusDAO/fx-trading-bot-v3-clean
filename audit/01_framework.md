# 审查 01：框架设计

范围：`fx/main.py`、`fx/config.py`、`fx/daemon_start.py`、`fx/watchdog.sh`、`fx/shared/state.py`、`DEPLOY.md`。
只看架构，不涉及交易策略逻辑和下单细节。

阅读说明：`main.py`、`daemon_start.py`、`watchdog.sh` 读得较完整；`config.py`（1056 行）、`state.py` 后半部分、`main.py` 876 行以后、`daemon_start.py` 428 行以后只读了开头并做了关键字检索。标"推断"的结论未经线上验证。

图例：🔴 严重问题 / 🟡 中等问题 / 🟢 没有问题 / 🔵 建议改进

## 1. 运行入口是否唯一？🔴

- 三者关系：`main.py` 是实际工作的 asyncio 进程；`daemon_start.py` 与 `watchdog.sh` 是两套互相独立的守护，都会拉起 `main.py`，崩溃后等 15 秒重启。
- 互不知情：`daemon_start.py` 用 `daemon.pid`、`main.pid` 做身份校验和杀进程（`_stop_all_existing`，253-309 行）；`watchdog.sh` 只用 `watchdog.lock`。两边的锁互不识别，各自会再启动一个 `main.py`。
- `main.py` 无单实例保护：120-123 行启动时直接覆盖写 `main.pid`，不检查是否已有实例。
- 启动环境不一致：`watchdog.sh` 强制要求 `BINANCE_PROXY` 并写死 `OPENAI_MODEL`；`daemon_start.py` 会读 `.env`、注入 `OPENAI_BASE_URL`，不设模型。最大重启次数一个 200、一个 500。
- 文档与实现不符：`DEPLOY.md` 第 8 节 systemd 配置为 `Type=simple` + `daemon_start.py start`，而该命令会双 fork 后父进程退出（`_daemonize`，409-419 行）。推断 systemd 会判定服务已退出并按 `Restart=always` 反复重启，需线上确认。`DEPLOY.md` 第 7 节还混有 `robinhood_sniper` 的启动命令。
- 重复下单风险：若两个 `main.py` 同时运行则可能发生。唯一防线 `state.add_position` 对同标的拒绝重复（`state.py:155-170`）用的是进程内 `threading.Lock`，`state.py` 第 12 行自己也承认不跨进程。`executor.py:193` 注释提到会对照交易所持仓去重，留待下单审查确认。
- 是否真会发生，取决于线上是否同时启动过两套守护。

## 2. 状态管理是否有单一真实来源？🟡

- 五个状态文件（`pos_state`、`circuit_breaker`、`cooldown_state`、`pending_confirm_state`、`oi_cache`）集中在 `state.py`，有 11 个模块通过其接口读写。
- 但状态不只在 `state.py`：
  - `risk_manager.py` 自管回撤状态和冲击状态，`_save_shock_state`（约 993 行）是普通 `write_text`，非原子写。
  - `trigger_detector.py:278` 用固定 `.tmp` 后缀写文件。
  - `scanner.py:2009` 用 `write_text` 写 `candidate_pool.json`，`main.py:769` 再直接读取。
  - `executor.py:781`、`main.py:189` 也各自直接写文件。
- `config.py:479-481` 定义了状态文件路径，但注释写明"代码未引用，修改无效"；真实路径在 `state.py:32-36` 写死。
- `pos_state` 的写入者有多个（均走 `state` 接口）：`executor` 新增，`position_manager` 更新删除，`main.py:512` 启动对齐时也会删。生命周期归属不明确。
- 原子写入：仅被 SIGKILL 时，"写临时文件再替换"（`state.py:96-99`）可避免损坏；但没有 `fsync`，掉电或宿主机崩溃可能留下空文件。临时文件只在异常时清理，被杀时会残留。
- 损坏保护只在内存：`_POS_STATE_CORRUPTED`（55 行）重启后归零，损坏文件被改名备份后系统以空持仓启动。
- 其他：`state.py:72` 靠文件名含 `"pos_state"` 区分处理方式，较脆弱；状态文件与日志同放 `logs/`。

## 3. 模块边界是否清晰？🟡

- `main.py` 含业务逻辑：`_sync_positions_on_startup`（435-660 行，约 225 行）含持仓对齐、清僵尸记录、拉 K 线、补挂止盈止损，与 `position_manager.py:2886-3039` 的补挂逻辑重复；`_loop_scan` 内有周末降频和触发器模式切换（233-245 行）。
- 模块间靠私有属性和模块全局变量接线：`executor._LISTENER = ws`（743 行）、`signal_watcher._main_loop` / `_ws_listener`（751-752 行）、`_trigger_det._watcher`（801 行）。
- 依赖方向混乱：
  - `position_manager` 多处导入 `executor` 私有函数（2353、2558、2894、4242、4293 行）。
  - `signal_watcher` 反向导入 `position_manager`（930 行），并直接调用 `api.place_sl`（1422 行）下单，绕过 `executor`。
  - `scanner` 直接 `import executor`（3142、4194 行）。
  - 循环依赖靠函数内延迟导入绕开。
- 🔵 模块过大：`position_manager.py` 约 4400 行，`scanner.py` 约 4200 行，`executor.py` 约 2000 行。

## 4. 配置是否集中？🟡

- `config.py` 第 3 行声明"禁止硬编码数字"，约 490 个常量，主体集中，但未做到：
  - 多处重复 URL：`binance_api.py:56`、`coinglass_api.py:229`、`vol_regime.py:38-39` 与 `config.py:32` 重复；`ai_advisor.py:248`、`llm_judge.py:134`、`daemon_start.py:335` 另有 LLM 地址；`config.py:680` 的 `LLM_API_BASE_URL` 同样注释为"代码未引用"。
  - 环境变量读取分散：除 `config.py`、`daemon_start.py`、`main.py` 外还有 29 处。
  - `main.py` 魔法数字：300/900/1800 秒、6 小时心跳、内存阈值 1500MB（175、236、340、381 行）；`daemon_start.py` 另有 `MEM_RESTART_MB=3000` 等。
  - 默认值写在调用处（`main.py:239、680、703`），可能与 `config.py` 不一致。
  - 缺 API Key 只警告不拒绝启动（`config.py:21-26`）。

## 5. asyncio 架构是否合理？🟡

- `main.py` 固定任务 10 个（`heartbeat`、`adjust_sl_tp`、`patrol`、`scan`、`funding_arb`、`refresh_pool`、`reset_circuit`、`loop_latency`、`track_outcomes`、`mem_snapshot`，780-798 行），条件任务 `trigger_detector`，另有 846 行的 `_watchdog`（任一任务意外结束即触发整体退出）。`ws`、`position_manager`、`signal_watcher` 内部另有任务；另有扫描线程池、`liq_cache`、`hub` 后台线程和事件循环看门狗线程。
- 做得好的地方：扫描超时用 `shield` 加跳过重叠轮次（194-209 行）；有信号处理和优雅退出；有事件循环延迟监控。
- async 内同步阻塞：
  - `_sync_positions_on_startup` 在 async 函数中直接调用同步 `_bapi.get_position()`、`get_open_orders()`、`get_algo_open_orders()`、`get_klines()`（476、484、492、582 行），且被注册为 WS 重连回调（728-731 行）。
  - `_loop_scan` 同步 `write_text` 写心跳（186-191 行）；`_loop_mem_snapshot` 的 `tracemalloc.take_snapshot()` 很重。
  - 状态接口每次读写整份 JSON 并持有线程锁，推断 async 调用点会触发磁盘 I/O。
  - `position_manager` 仅 4 处用 `_run_sync` 封装，`executor`、`signal_watcher` 基本未用。
- "临时诊断"代码留在生产：`tracemalloc.start(10)`（52 行）常开；看门狗线程和延迟监控同样标注"定位后删除"。
- 启动时自动 `pip install`（81-102 行，带 `--break-system-packages`）：运行时联网装包，不可复现且有供应链风险。
- 开仓路径：主扫描走专用线程池，`funding_arb`（`dry_run=False`）走默认线程池，两条线程路径都可能下单；"先检查槽位再开仓"是否有竞态尚未验证。

## 总结

| 问题 | 严重程度 | 影响范围 |
|---|---|---|
| 两套守护 + 单实例保护缺失，可能起多个 `main.py` | 🔴 | 重复开仓、状态竞争 |
| `DEPLOY.md` 的 systemd 方案与双 fork 守护不兼容（推断），文档含无关项目内容 | 🟡 | 部署稳定性 |
| 两套守护的启动环境、重启上限不一致 | 🟡 | 行为不可预测 |
| 状态文件分散在多个模块，部分非原子写，路径常量失效 | 🟡 | 崩溃恢复、数据一致性 |
| 线程锁不跨进程，原子写无 `fsync`，损坏标志不持久 | 🟡 | 掉电或多实例时丢更新 |
| `main.py` 含大段业务逻辑（对齐、补挂） | 🟡 | 可维护性，与 `position_manager` 重复 |
| 模块循环依赖、导入私有函数、`signal_watcher` 绕过 `executor` 下单 | 🟡 | 改动风险、职责不清 |
| 三个大文件（约 2000 到 4400 行） | 🔵 | 审查和测试难度 |
| 配置未集中：URL 重复、魔法数字、死常量、默认值分散 | 🟡 | 改参数可能不生效 |
| `async` 函数内同步 HTTP 和磁盘 I/O，WS 重连回调也在其中 | 🟡（偏高） | 保护单维护被延迟 |
| 常开 `tracemalloc` 等"临时诊断"代码、运行时 `pip install` | 🟡 | 性能、可复现性 |
| `funding_arb` 与主扫描双线程路径的开仓竞态 | 待验证 | 超仓风险 |

> 注：async 内同步 I/O 问题在 feature/passive-trigger 分支已有部分修复
> （state.py OI缓存、main.py WS重连、trigger_detector.py 候选池缓存），
> 本仓库当前代码未包含这些修复，审查结论基于 main 分支现状。
