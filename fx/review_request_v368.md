# Binance Futures Trading Bot v3.68 代码评审请求

## 评审范围

信号扫描 → 挂单 → 挂单维护 → 开仓 → 开仓维护 完整链路

涉及文件：
- `scanner.py` (2685 行) - 信号扫描主逻辑
- `shared/signal_watcher.py` (1230 行) - 挂单监控和维护
- `executor.py` (1641 行) - 开仓执行
- `position_manager.py` (3551 行) - 开仓后维护（止损止盈、健康检查、平仓等）

## 系统设计理念

### 1. 多策略信号扫描
- **策略类型**：reversal（极端反转）、pullback（EMA20 回踩）、breakout（放量突破）、range_bounce（区间震荡）、stop_hunt（止损猎杀）、funding_arb（资金费率套利）、oi_divergence（OI 背离）
- **分类层粗筛**：`_is_reversal` / `_is_breakout` 等函数用简单条件快速分类，避免所有标的都跑完整 judge
- **精筛层**：`judge_reversal` / `judge_pullback` 等多维度验证，确保信号质量
- **评分系统**：`final_score` 综合 btc_trend、coin_glance、strategy_fit、shock、tier、pulse、taker 等维度

### 2. 开仓条件和存活条件分离（v3.67）
- **开仓条件**：严格多维度 judge，确保入场时机正确
- **存活条件**：`_survival_check` 只检查「核心理由是否被证伪」，不重新跑完整 judge
- **避免误撤**：vol_ratio、15M 形态、OI 等瞬时指标波动不再导致挂单被撤

### 3. 入场价距离保护（v3.66）
- **问题**：reversal 模式下，清算区可能在很远位置，直接挂单会导致成交率极低
- **解决**：`_entry_reversal` 检查 entry 距 mark 是否 > 1.5 ATR，超距则降级到 mark ± 0.5 ATR
- **复盘字段**：记录 `liq_entry_raw`（原始清算区价格）、`liq_entry_dist_atr`（原始距离），用于后续评估是否恢复 pending_order 模式

### 4. 动态止损止盈调整（v3.68）
- **RR 保护**：TP1 调整后检查 RR，< 1.0 则主动平仓（`rr_below_threshold`）
- **健康检查时间分层**：早期（<2H）放宽门槛×0.5，中期（2-6H）标准，晚期（>6H）收紧×1.5

## 核心流程

### 信号扫描流程
```
1. refresh_pool: 筛选候选池（成交量、振幅、资金费率等）
2. analyze_symbol: 对每个标的拉数据 + 分类 + judge + 评分
3. 过滤：active_syms（已有仓位）、pending_syms（已有挂单）、cooldown、blacklist
4. 排序：按 final_score 降序
5. 注册：调用 signal_watcher.add_signal 写入触发快照字段
```

### 挂单维护流程
```
1. _recheck_loop: 每 5 分钟跑一次
2. _recheck_one: 对每个挂单
   - Section A: _survival_check（核心理由是否被证伪）
   - Section B: 清算区位移检查（仅 reversal SHORT）
   - BTC 大盘结构检查（BTC 4H 空头时撤 LONG 挂单）
   - 新闻否决检查
3. 清算区位移重挂：撤旧单 → 重算入场价 → 挂新单（v3.68 加 dist 保护）
```

### 开仓执行流程
```
1. execute_signal: 接收 signal dict
2. 预检：检查是否已有挂单、仓位是否已满
3. calc_sl_tp: 计算 SL/TP（reversal 模式用清算区锚点）
4. 下单：市价/限价（根据 entry_mode）
5. 挂保护单：SL（algo 单）、TP1/TP2（限价单）
6. _write_position: 写入 pos_state.json
```

### 开仓维护流程
```
1. patrol_once: 每 5 分钟跑一次
2. _check_tp_filled_rest: 检查 TP 是否成交，撤未成交的保护单
3. _adjust_sl_tp_once: 重新拉数据，调整 SL/TP（v3.68 加 RR 检查）
4. _check_health: 健康评分检查（v3.68 按持仓时间分层）
5. 各种时间止损：early_stagnation、time_stop_no_progress、time_stop_hard
6. trail_sl: 移动止损（浮盈达到一定比例后激活）
7. _force_close: 市价平仓
```

## 关键改动（v3.66 - v3.68）

### v3.66: 入场价距离保护
- `_entry_reversal` 返回三元组 `(entry, mode, degrade_info)`
- 超距降级时记录 `liq_entry_raw`、`liq_entry_dist_atr`、`entry_degraded_reason`
- 复盘字段透传到 pos_state 和 archiver

### v3.67: 开仓条件和存活条件分离
- `WatchEntry` 新增触发快照字段：`trigger_price`、`trigger_rsi_1h`、`breakout_ref`、`ema20_at_trigger`、`atr_at_trigger`
- `_survival_check` 按 mode 分别检查核心理由：
  - pullback: 价格仍在 EMA20 支撑/压力区间（±1.5 ATR）
  - breakout: 价格仍在突破位上方/下方
  - reversal: 价格未走反 + RSI 极端未消退
  - range_bounce: 区间结构未被破坏
  - stop_hunt: 二探未过深

### v3.68: 三个防御性改进
1. **重挂单 dist 保护**：清算区位移重挂时，检查新入场价是否 > 1.5 ATR，超距则降级
2. **RR < 1.0 主动平仓**：TP1 调整后检查 RR，< 1.0 则主动平仓
3. **健康检查时间分层**：早期放宽、晚期收紧

## 评审要求

请从以下维度评审：

### 1. 架构设计（20 分）
- 模块划分是否合理
- 职责是否清晰
- 耦合度是否过高
- 是否易于测试和维护

### 2. 代码质量（20 分）
- 代码风格是否一致
- 命名是否清晰
- 注释是否充分
- 是否有明显的 bug 或逻辑错误

### 3. 健壮性（20 分）
- 异常处理是否充分
- 边界条件是否考虑
- 并发安全是否保证
- 资源泄漏是否避免

### 4. 性能（20 分）
- 是否有不必要的重复计算
- 是否有可以优化的 IO 操作
- 内存使用是否合理
- 是否有性能瓶颈

### 5. 可扩展性（20 分）
- 是否易于添加新策略
- 是否易于调整参数
- 是否易于集成新指标
- 是否易于回测

## 输出格式

```
## 评审结果

### 总分：XX / 100

### 各维度得分
- 架构设计：XX / 20
- 代码质量：XX / 20
- 健壮性：XX / 20
- 性能：XX / 20
- 可扩展性：XX / 20

### 优点
1. ...
2. ...
3. ...

### 不足
1. [严重] ...
2. [中等] ...
3. [轻微] ...

### 优化建议
1. ...
2. ...
3. ...

### 具体代码问题（如有）
- 文件：xxx.py，行号：xxx
  问题：...
  建议：...
```

## 附录：关键代码片段

### scanner.py: analyze_symbol 核心逻辑
```python
def analyze_symbol(symbol: str, ticker: dict,
                   active_syms: set, pending_syms: set,
                   allowed_directions: set | None = None,
                   loss_blacklist: set | None = None,
                   btc_trend: str = "") -> dict | None:
    # 零成本预筛
    if loss_blacklist and symbol in loss_blacklist:
        return None
    
    # ticker 快速预筛（5ms）
    _chg = float(ticker.get("priceChangePercent", 0))
    _vol = float(ticker.get("quoteVolume", 0))
    if _vol < 500_000:
        return None
    
    # 拉数据 + 分类 + judge + 评分
    # ...
    
    return signal_dict
```

### signal_watcher.py: _survival_check
```python
@staticmethod
def _survival_check(entry: 'WatchEntry', price: float, rsi_1h: float, atr: float) -> tuple[bool, str]:
    """挂单存活检查——只检查「核心理由是否被证伪」"""
    mode = entry.mode
    direction = entry.signal.get('direction', 'LONG').upper()
    
    if mode == 'pullback':
        ema20 = entry.ema20_at_trigger
        if direction == 'LONG':
            if price < ema20 - atr * 1.5:
                return True, f"pullback LONG 价格跌破支撑位"
        else:
            if price > ema20 + atr * 1.5:
                return True, f"pullback SHORT 价格涨破压力位"
    
    elif mode == 'breakout':
        ref = entry.breakout_ref
        if direction == 'LONG':
            if price < ref - atr * 0.3:
                return True, f"breakout LONG 假突破确认"
    
    elif mode == 'reversal':
        if direction == 'SHORT':
            if price < entry.trigger_price - atr * 0.5:
                return True, f"reversal SHORT 价格反向下跌"
            if rsi_1h < 60:
                return True, f"reversal SHORT RSI 回落至 60 以下"
    
    return False, ""
```

### position_manager.py: _adjust_sl_tp_once RR 检查
```python
async def _adjust_sl_tp_once(pos: dict, force: bool = False) -> dict | None:
    # ... 重算 SL/TP ...
    
    # v3.68: RR < 1.0 主动平仓
    current_mark = await asyncio.get_event_loop().run_in_executor(
        None, lambda: float(api.get_mark_price(symbol).get('markPrice', 0))
    )
    if current_mark > 0:
        sl_dist = abs(current_mark - new_sl)
        tp1_dist = abs(new_tp1 - current_mark)
        if sl_dist > 0:
            rr_now = tp1_dist / sl_dist
            if rr_now < TP_MIN_RR_LIVE:
                logger.warning(f"[adjust_sltp] {symbol} RR < {TP_MIN_RR_LIVE}，主动平仓")
                await _force_close_static(pos, "rr_below_threshold")
                return {"closed": True, "reason": "rr_below_threshold", "rr": rr_now}
```

### position_manager.py: _check_health 时间分层
```python
async def _check_health(self, pos: dict):
    # ... 计算健康评分 ...
    
    # v3.68: 按持仓时间动态调整强平门槛
    holding_hours = hold_min / 60
    if holding_hours < PM_HEALTH_EARLY_HOURS:
        threshold_mult = PM_HEALTH_EARLY_MULT  # 0.5
    elif holding_hours < PM_HEALTH_LATE_HOURS:
        threshold_mult = PM_HEALTH_MID_MULT    # 1.0
    else:
        threshold_mult = PM_HEALTH_LATE_MULT   # 1.5
    
    effective_loss_threshold = PM_HEALTH_FORCE_CLOSE_LOSS_PCT * threshold_mult
    
    if score < PM_HEALTH_FORCE_CLOSE_SCORE and hold_min >= PM_MIN_HOLD_BEFORE_HEALTH:
        if pnl_pct <= effective_loss_threshold:
            await self._force_close(pos, "health_force_close")
```

---

**评审时间**：2026-10-19  
**版本**：v3.68  
**评审人**：Claude  
**提交人**：Binance AI Pro
