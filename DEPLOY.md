# DEPLOY.md — 新服务器完整重建手册

> 目标：在一台全新的 Linux 服务器上从零把所有工作流跑起来。  
> 预计耗时：30 分钟（依赖网速）。

---

## 0. 前置条件

| 项目 | 要求 |
|------|------|
| OS | Ubuntu 22.04+ / Debian 11+（root 或 sudo） |
| Python | 3.11（`python3 --version`） |
| pip | 23+（`pip --version`） |
| Git | 任意版本 |
| 网络 | 需要能访问 Binance API — 若 IP 受限，必须先配好代理 |

---

## 1. 安装系统依赖

```bash
apt-get update && apt-get install -y \
  python3 python3-pip python3-venv \
  git curl wget screen \
  libssl-dev libffi-dev build-essential
```

---

## 2. 拉取代码

```bash
# 用 GitHub Personal Access Token（PAT）克隆私有仓库
git clone https://<YOUR_GITHUB_TOKEN>@github.com/SexusDAO/fx-trading-bot-v3.git \
  /root/.openclaw/workspace
cd /root/.openclaw/workspace
```

---

## 3. 安装 Python 依赖

```bash
# 安装 fx 工作流全部依赖（50 个包，固定版本）
pip install -r fx/requirements.txt

# arc_sniper 额外依赖
pip install -r arc_sniper/requirements.txt
```

---

## 4. 配置环境变量

```bash
# 从模板复制
cp fx/.env.example fx/.env

# 编辑，填入真实值（至少填必填项）
nano fx/.env
```

**必填项清单**（缺任意一项工作流启动时就报错）：

| 变量 | 说明 |
|------|------|
| `BINANCE_API_KEY` | Binance API Key（期货权限） |
| `BINANCE_API_SECRET` | Binance API Secret |
| `BINANCE_PROXY` | SOCKS5 代理，格式 `socks5://user:pass@host:port`（无代理填空） |
| `PROXY_URL` | 同上，与 BINANCE_PROXY 保持一致 |
| `OPENAI_API_KEY` | LLM API Key（NovaAPI 兼容 OpenAI 格式） |

填完后加载到当前 shell：

```bash
set -a && source fx/.env && set +a
# 验证
echo $BINANCE_API_KEY  # 应显示 key（不是空）
```

---

## 5. 启动 fx 主交易工作流

```bash
cd /root/.openclaw/workspace/fx

# 启动（daemon 模式，自动崩溃重启）
python3 daemon_start.py start

# 查看状态
python3 daemon_start.py status

# 查看实时日志
tail -f logs/scanner.log | grep "\[scan\]"
```

**正常启动的标志**：

```
[scan] YYYY-MM-DDTHH:MM:SS 扫描NNN reversal=X breakout=X pullback=X ...
```

约 2 分钟后出现第一条 `[scan]` 日志即为正常。

---

## 6. 验证工作流健康

```bash
cd /root/.openclaw/workspace/fx

# 检查进程
pgrep -a python | grep -E "main\.py|daemon"

# 检查候选池
python3 -c "
import json, datetime
d = json.load(open('logs/candidate_pool.json'))
ts = d.get('ts', 0)
age = (datetime.datetime.now(datetime.timezone.utc).timestamp() - ts) / 60
print(f'候选池: {len(d.get(\"pool\",[]))} 个标的，更新于 {age:.0f} 分钟前')
"

# 连通性测试
python3 -c "
import sys; sys.path.insert(0,'.')
from shared.binance_api import get_mark_price
print('Binance OK, BTC:', get_mark_price('BTCUSDT').get('markPrice'))
"
```

---

## 7. 启动其他子系统（按需）

### Arc Sniper（Arc 链 Meme 打新）

```bash
cd /root/.openclaw/workspace/arc_sniper
export ARC_PRIVATE_KEY="0x你的Arc链钱包私钥"
python3 main.py
```

> ⚠️  ARC_PRIVATE_KEY 必须设置，未设置会直接报错退出。不要写在代码里。

### Robinhood Sniper（链上巨鲸跟单）

```bash
cd /root/.openclaw/workspace/robinhood_sniper
export BINANCE_PROXY="socks5://user:pass@host:port"
python3 main.py
```

---

## 8. 持久化运行（screen / systemd）

### 方案 A：screen（简单，推荐）

```bash
screen -S fx-trading
cd /root/.openclaw/workspace/fx
set -a && source .env && set +a
python3 daemon_start.py start
# Ctrl+A D 退出 screen，进程保持运行

# 重新接入
screen -r fx-trading
```

### 方案 B：开机自启（systemd）

```ini
# /etc/systemd/system/fx-trading.service
[Unit]
Description=FX Trading Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/.openclaw/workspace/fx
EnvironmentFile=/root/.openclaw/workspace/fx/.env
ExecStart=/usr/bin/python3 daemon_start.py start
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable fx-trading
systemctl start fx-trading
systemctl status fx-trading
```

---

## 9. 常见问题

| 症状 | 原因 | 解决 |
|------|------|------|
| `EnvironmentError: BINANCE_API_KEY 未设置` | 环境变量没 export | `set -a && source fx/.env && set +a` |
| `EnvironmentError: ARC_PK 未设置` | arc_sniper 私钥未设置 | `export ARC_PK=0x你的私钥` |
| 扫描日志停了 10 分钟以上 | 进程崩溃或 WebSocket 断线 | `python3 daemon_start.py restart` |
| `ConnectionError` / `ProxyError` | 代理失效 | 检查 `BINANCE_PROXY` 变量，验证代理连通性 |
| 候选池 0 个标的 | API 连通问题 | 重启 daemon，等一轮扫描（约 2 分钟） |
| `ModuleNotFoundError` | 依赖包未安装 | `pip install -r fx/requirements.txt` |

---

## 10. 备份策略（防止状态丢失）

以下文件是运行时生成的，**不在 git 里**，需要定期手动备份：

```
fx/logs/pos_state.json        # 当前持仓
fx/logs/candidate_pool.json   # 候选池（可重建）
fx/logs/cooldown_state.json   # 冷却状态
strategy-config.json           # 策略参数（如有手动改过）
position_rules.json            # 评分规则
TRADING_RULES.md               # 交易规则文档
```

备份命令：

```bash
tar czf ~/fx-state-backup-$(date +%Y%m%d).tar.gz \
  /root/.openclaw/workspace/fx/logs/pos_state.json \
  /root/.openclaw/workspace/strategy-config.json \
  /root/.openclaw/workspace/position_rules.json \
  /root/.openclaw/workspace/TRADING_RULES.md
```

---

## 11. 版本信息

- 仓库：`SexusDAO/fx-trading-bot-v3`（私有）
- 对照备份：`SexusDAO/fx-trading-bot-v2`（私有，含旧历史）
- Python：3.11
- 最后更新：2026-10-20
