# 快速启动指南

## 5 分钟部署新架构

### Step 1: 验证模块 (1 分钟)

```bash
cd /root/.openclaw/workspace
python3 test_architecture.py
```

预期输出：所有 6 项测试通过 ✅

---

### Step 2: 注册 Cron 任务 (1 分钟)

```bash
python3 scripts/register_crons.py
```

验证：
```bash
openclaw cron list
```

应看到 9 个任务：
- patrol-sl-check (3m)
- patrol-tp-check (3m)
- scanner-main (30m)
- health-check (30m)
- market-regime (30m)
- circuit-daily-reset (24h)
- daily-report (24h)
- activity-check (24h)
- process-guardian (24h)

---

### Step 3: 启动进程守护 (1 分钟)

```bash
mkdir -p logs
nohup bash guards/process_guardian.sh > logs/guardian.log 2>&1 &
```

验证：
```bash
ps aux | grep process_guardian
tail -f logs/guardian.log
```

---

### Step 4: 同步现有持仓 (2 分钟)

如果有现有持仓，需要同步到 pos_state.json：

```bash
python3 -c "
from modules.position_state import PositionState
ps = PositionState()

# 示例：添加 BTCUSDT 持仓
ps.add_position(
    symbol='BTCUSDT',
    side='LONG',
    entry_price=95000,  # 修改为实际入场价
    sl_price=92000,     # 修改为实际止损价
    tp_price=102000,    # 修改为实际止盈价
    position_usdt=2000
)

print('仓位已添加')
print(ps.get_all_positions())
"
```

---

### Step 5: 测试 Patrol 脚本 (1 分钟)

```bash
python3 guards/patrol_sl_check.py
python3 guards/patrol_tp_check.py
```

检查日志：
```bash
tail -f logs/patrol_sl.log
tail -f logs/patrol_tp.log
```

---

## 监控检查

### 实时日志

```bash
# 进程守护日志
tail -f logs/guardian.log

# Patrol 止损日志
tail -f logs/patrol_sl.log

# Patrol 止盈日志
tail -f logs/patrol_tp.log

# Scanner 日志
tail -f logs/scanner.log
```

### Cron 运行记录

```bash
# 查看所有 Cron 任务状态
openclaw cron list

# 查看特定任务运行记录
openclaw cron runs --id <jobId>
```

### 配置文件状态

```bash
# 市场环境
cat config/market_regime.json

# 熔断器
cat config/circuit_breaker.json

# 仓位状态
cat config/pos_state.json

# Scanner 结果
cat config/scanner_results.json
```

---

## 常见问题

### Q: Cron 任务不执行？
```bash
# 检查 Cron 状态
openclaw cron status

# 手动触发测试
python3 guards/patrol_sl_check.py
```

### Q: 进程守护不工作？
```bash
# 检查脚本权限
chmod +x guards/process_guardian.sh

# 手动运行测试
bash guards/process_guardian.sh &
```

### Q: 如何查看市场模式？
```bash
python3 -c "
from config.market_rules import MarketFilter
mf = MarketFilter()
regime = mf.check_btc_filter()
print(f'市场模式：{regime}')
print(f'持仓上限：{mf.get_position_limits()}')
"
```

### Q: 如何手动重置熔断器？
```bash
python3 -c "
from modules.circuit_breaker import CircuitBreaker
cb = CircuitBreaker()
cb.reset()
print('熔断器已重置')
"
```

---

## 停止/回滚

### 停止新架构

```bash
# 停止进程守护
pkill -f process_guardian.sh

# 删除所有 Cron 任务
for id in $(openclaw cron list | grep -oP '(?<=id:)\S+'); do
    openclaw cron rm $id
done
```

### 回滚旧架构

```bash
# 恢复备份
cp backup/<timestamp>/*.json config/

# 重启旧进程
python3 ws_monitor.py &
python3 scanner.py &
```

---

## 下一步优化

1. **集成 LLM API**: 替换 7 个 LLM 任务中的模拟逻辑
2. **接入 Coinglass**: 获取清算数据用于双网决策
3. **接入 Binance Pro**: 获取热度榜、聪明钱信号
4. **优化通知**: 配置 Jarvis 推送规则

---

## 文档索引

- 📘 **ARCHITECTURE.md** - 完整架构设计
- 📗 **MIGRATION_GUIDE.md** - 详细迁移步骤
- 📙 **REFACTORING_SUMMARY.md** - 重构总结
- 📕 **QUICKSTART.md** - 本文件（快速启动）

---

_最后更新：2026-08-02 15:00 UTC_
