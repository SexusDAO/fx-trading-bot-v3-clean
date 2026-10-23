# FX Trading Bot v3

币安合约自动化交易系统，支持多策略信号扫描、风控管理、持仓跟踪和实时执行。

## 系统架构

- **main.py** — 主进程，事件循环和任务调度
- **scanner.py** — 信号扫描（reversal/breakout/pullback 三种策略）
- **executor.py** — 订单执行和仓位管理
- **position_manager.py** — 持仓状态跟踪和对齐
- **market_data_hub.py** — WebSocket 行情中心（K线、标记价格、资金费率）
- **signal_watcher.py** — 信号监控和价格触发检测
- **daemon_start.py** — 进程管理和守护

## 部署要求

- Python 3.11+
- pip 23+
- Git
- Ubuntu 22.04+ / Debian 11+

```bash
# 安装依赖
pip install -r fx/requirements.txt
```

## 环境变量配置

在 `fx/.env` 中配置（参考 `fx/.env.example`）：

- **BINANCE_API_KEY** — 币安 API Key（需要合约交易权限）
- **BINANCE_API_SECRET** — 币安 API Secret
- **BINANCE_TESTNET** — 是否使用测试网（true/false）
- **PROXY_URL** — 代理地址（可选，格式 socks5://user:pass@host:port）
- **OPENAI_API_KEY** — OpenAI API Key（用于 LLM 信号过滤）
- **OPENAI_BASE_URL** — OpenAI API 地址（可选，用于自定义 endpoint）

## 启动方式

```bash
cd /root/.openclaw/workspace/fx

# 启动
python3 fx/daemon_start.py start

# 停止
python3 fx/daemon_start.py stop

# 重启
python3 fx/daemon_start.py restart

# 查看状态
python3 fx/daemon_start.py status

# 清理日志
python3 fx/daemon_start.py clean
```

## 主要 config.py 参数

### 风控类
- **MAX_POSITIONS_TOTAL_RANGE** — 最大总持仓数（默认 4）
- **MAX_POSITIONS_SHORT_RANGE** — 最大空单持仓数（默认 2）
- **MAX_SINGLE_NOMINAL** — 单仓名义上限 USDT（默认 1000）
- **SL_MAX_PCT** — 止损百分比（默认 12%）
- **NET_DELTA_MAX_PCT** — 净多/空头 Delta 上限（默认 30%）

### 周末模式
- **WEEKEND_MIN_SCORE_MEDIUM_PULLBACK** — 周末 pullback 信号最低分（默认 60）
- **WEEKEND_SIGNAL_EXPIRE_CUTOFF_HOUR** — 周日信号到期截止时间 UTC（默认 17）

### Hub 相关
- **MARKET_DATA_HUB_ENABLED** — 启用行情 Hub（默认 True）
- **HUB_LOAD_MAX_WORKERS** — 历史 K线加载并发数（默认 8）
- **KLINE_LOAD_BATCH_SIZE** — 每轮 K线加载任务数（默认 200）
- **HUB_NO_MSG_TIMEOUT** — WebSocket 无消息超时秒数（默认 180）

## 日志文件位置

所有日志在 `fx/logs/` 目录：

- **system.log** — 主系统日志（含所有模块的 INFO/WARNING/ERROR）
- **scanner.log** — 扫描器专用日志（信号详情、过滤原因）
- **daemon.log** — 守护进程日志（启停记录）
- **watchdog_stack.txt** — 事件循环阻塞时的线程堆栈 dump
- **trigger_log.jsonl** — 触发器执行日志
- **scan_signals.jsonl** — 扫描信号记录

日志自动轮转，保留最近 5 个文件（system.log.1-5）。

## 常见问题

### kline就绪数为 0 或很低

```bash
# 查看当前 kline 就绪数
grep "kline就绪" fx/logs/system.log | tail -1

# 查看历史 K线加载日志
grep "历史K线就绪" fx/logs/system.log | tail -20

# 查看断线情况
grep "组合流.*断线" fx/logs/system.log | tail -10
```

频繁断线会导致 kline 数据被清空。检查网络稳定性和代理配置。

### 代理配置

在 `fx/.env` 中设置：
```bash
PROXY_URL=socks5://user:pass@host:port
```

Hub 可以单独配置代理（在 config.py）：
```python
HUB_WS_PROXY = "socks5://user:pass@host:port"  # 空字符串=直连
```

### API 白名单

1. 登录币安 → API 管理 → 编辑 API Key
2. 添加服务器 IP 到白名单
3. 确保开启"启用合约"权限
4. 如果 IP 经常变动，建议使用代理固定出口 IP

### 事件循环阻塞

```bash
# 查看阻塞日志
grep "事件循环阻塞" fx/logs/system.log | tail -20

# 查看线程堆栈
cat fx/logs/watchdog_stack.txt
```

正常范围 2-4 秒，超过 10 秒需要排查（通常是 GIL 争抢或网络超时）。
