"""
fx/position_manager.py - 开仓后全生命周期管理

两个并行机制:
  A. WebSocket 实时监听(markPrice + userData)
     - 动态止损跟随(ATR阶梯)
     - 止损/TP 联动撤单
  B. 定时巡检任务(每5分钟)
     - 健康评分检查(每30分钟)
     - 时间止损检查
     - 挂单有效期检查
     - 突破失败主动止损(breakout专属)
     - 日报生成(北京时间10:00 = UTC 02:00)
"""

from __future__ import annotations
from shared.indicators import atr as _calc_atr_from_klines, ema as _calc_ema, rsi as _calc_rsi
import asyncio
import json
import logging
import math
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent))

from config import (
    TRAIL_STEPS_TREND, TRAIL_STEPS_REVERSAL,
    PM_MAX_HOLD_HOURS, PM_HARD_MAX_HOLD_HOURS,
    TIME_STOP_REVERSAL_HOURS,
    EMA_TREND_MAX_HOLD_HOURS,
    PM_HEALTH_CHECK_INTERVAL, PM_HEALTH_FORCE_CLOSE_SCORE,
    PM_HEALTH_FORCE_CLOSE_LOSS_PCT,
    PM_HEALTH_WARN_SCORE, PM_MIN_HOLD_BEFORE_HEALTH,
    PM_BREAKOUT_FAIL_WINDOW, PM_COOLDOWN_SL_HOURS,
    PM_COOLDOWN_CRASH_HOURS, DAILY_REPORT_UTC_HOUR,
    PRICE_DEBOUNCE_SEC,
    ORDER_EXPIRE_BREAKOUT_MIN, ORDER_EXPIRE_PULLBACK_MIN,
    TP_SL_ADJUST_INTERVAL_MIN, TP_SL_ADJUST_MIN_HOLD_MIN,
    TP_SL_ADJUST_TP_SHRINK_ATR, TP_SL_ADJUST_SL_WIDEN_ATR,
    TP_SL_ADJUST_LOG,
    EXIT_SIGNAL_ENABLED, EXIT_SIGNAL_RSI_LONG_THRESH, EXIT_SIGNAL_RSI_SHORT_THRESH,
    EXIT_SIGNAL_MIN_PROFIT_RATIO, EXIT_SIGNAL_MIN_HOLD_MIN,
    # v3.68/v3.69 新增参数
    TP_MIN_RR_LIVE,
    PM_HEALTH_EARLY_MULT, PM_HEALTH_MID_MULT, PM_HEALTH_LATE_MULT,
    PM_HEALTH_EARLY_HOURS, PM_HEALTH_LATE_HOURS,
)
from shared import binance_api as api
from shared.async_utils import _run_sync   # [v3.89 #61] 同步调用移出事件循环的统一封装
from shared.risk_manager import record_pnl as _record_pnl
from shared.state import (
    get_all_positions, update_position, remove_position,
    set_cooldown, is_in_cooldown,
)
from shared.notifier import (
    notify, card_append, card_append_async, card_remove,
    _hhmm, _CLOSE_REASON_CN, fmt_hold_time,
    fmt_node_tp1, fmt_node_close, fmt_node_sl_lost,
)

logger = logging.getLogger("fx.position_manager")


def _remove_position_checked(pos_id: str, symbol: str = "") -> bool:
    """
    [v3.89 remove-pos 检查] remove_position 的调用包装：返回 False 说明 pos_state 结构性失败
    （文件损坏 / 写入原子替换均失败），持仓记录很可能已丢失或残留，必须人工核查。
    只做日志，不改变任何业务行为。
    """
    try:
        ok = remove_position(pos_id)
    except Exception as e:
        logger.error(f"[pm] _remove_position_checked({symbol or pos_id}) 抛异常，持仓文件可能损坏，请人工核查: {e}")
        return False
    if ok is False:
        logger.warning(
            f"[pm] _remove_position_checked({symbol or pos_id}) 返回 False，pos_state 未写入成功，"
            f"该持仓记录可能残留或丢失，请人工核查"
        )
    return ok is not False

# ── 系统主动撤单锁 ────────────────────────────────────────────────────────────
# 当系统自己在更新保护单(撤旧挂新)时,暂时豁免「手动平仓」误判
# 避免:系统撤了旧TP → _check_manual_close 误判为用户手动平仓 → 错误市价平仓
_SYSTEM_ORDER_UPDATE: dict = {}   # symbol → expire_ts
_SYSTEM_ORDER_TTL    = 60         # 豁免窗口 60 秒(足够撤单+重挂完成)

def mark_system_order_update(symbol: str) -> None:
    """系统准备更新某标的保护单前调用,60s 内禁止手动平仓误判。"""
    _SYSTEM_ORDER_UPDATE[symbol] = time.time() + _SYSTEM_ORDER_TTL
    logger.debug(f"[pm] {symbol} 系统撤单锁已加(60s)")

def is_system_order_update(symbol: str) -> bool:
    """True = 系统正在更新保护单,_check_manual_close 应跳过。"""
    expire = _SYSTEM_ORDER_UPDATE.get(symbol, 0)
    if time.time() < expire:
        return True
    _SYSTEM_ORDER_UPDATE.pop(symbol, None)
    return False

# ─────────────────────────────────────────────────────────────────────────────
# 内存状态(浮盈水位线、告警节流)
# ─────────────────────────────────────────────────────────────────────────────
_peak: dict[str, float]    = {}   # pos_id → 持仓期间最高浮盈价格(做多高,做空低)
_mfe:  dict[str, float]    = {}   # pos_id → 持仓期间最大有利偏移(MFE,始终≥0)
_mae:  dict[str, float]    = {}   # pos_id → 持仓期间最大不利偏移(MAE,始终≥0)
_last_price: dict[str, float] = {} # symbol → 最新标记价格
_last_price_ts: dict[str, float] = {} # symbol → 最后更新时间(防抖)
_health_warned: dict[str, float] = {} # pos_id → 最后告警时间戳
_sl_check_ts: dict[str, float]   = {} # pos_id → 上次 Algo SL 辅助确认时间戳
_health_check_counter: int = 0         # 巡检计数器(每6次触发一次健康检查)
_last_daily_report_date: str = ""      # 上次日报日期(UTC date string)

# 补挂互斥锁:key = pos_id,防止 executor 和 patrol 同时对同一仓位补挂 TP/SL
_guard_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_pg_retry_ts: dict = {}   # [fix #10] pending_guard 重试节流

# v3.56 平仓幂等保护:防止 WS路径 和 patrol路径 并发对同一仓位发出两次市价平仓
_closing_set: set[str] = set()   # 正在平仓的 pos_id 集合

TRADE_LOG_PATH = ROOT / "logs" / "trade_log.jsonl"


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────
def _round_price(price: float, tick: float) -> float:
    if tick <= 0: return round(price, 6)
    prec = max(0, -int(math.floor(math.log10(tick))))
    # [v3.88 OPT-16] 浮点修复:原写法 floor(price/tick) 受浮点误差影响(0.3/0.1=2.9999999999999996,floor 得 2,价格少一个 tick)。
    #   先 round(price/tick, 10) 消除误差再向下取整,与 _round_qty 的修法一致;正常价格结果不变。
    return round(math.floor(round(price / tick, 10)) * tick, prec)


def _round_qty(qty: float, step: float) -> float:
    if step <= 0: return round(qty, 3)
    prec = max(0, -int(math.floor(math.log10(step))))
    # [v3.87 STEP7-1] 与 executor._round_qty 对齐:原写法 floor(qty/step)*step 受浮点误差影响会多丢一个步长
    #   (0.3/0.1=2.9999999999999996,floor 得 2,数量变成 0.2)。先 round(qty/step, 10) 消除噪声再 floor。
    #   影响本模块的平仓、止损/止盈补挂、加仓、挂单调整数量。
    return round(math.floor(round(qty / step, 10)) * step, prec)


def _get_precision(symbol: str) -> dict:
    """从 binance_api 带缓存的 get_precision 读精度(1h TTL,不重复拉 exchangeInfo)"""
    p = api.get_precision(symbol)
    return {"tick": p["tick_size"], "step": p["step_size"]}


async def _notify_async(text: str) -> None:
    await asyncio.get_running_loop().run_in_executor(None, lambda: notify(text))


# ─────────────────────────────────────────────────────────────────────────────
# 动态止损阶梯计算
# ─────────────────────────────────────────────────────────────────────────────
def _trail_steps(mode: str) -> list:
    if mode == "reversal":
        return TRAIL_STEPS_REVERSAL
    return TRAIL_STEPS_TREND


def _calc_trail_sl(pos: dict, current_price: float) -> float | None:
    """
    冲突四修复(v3.3):统一止损跟随逻辑。
    每次计算所有可能触发的止损目标位(阶梯跟随 + RR保护),
    取对持仓最有利的那个(做多取最高,做空取最低),只执行一次移动。
    RR保护不再单独在 _check_time_stop 里触发,统一归入此处。
    """
    entry     = float(pos["entry_price"])
    direction = pos["direction"].upper()
    mode      = pos.get("mode", "pullback")
    atr       = float(pos.get("atr_at_entry", 0))
    old_sl    = float(pos["sl_price"])
    tp1_filled = pos.get("tp1_filled", False)

    if atr <= 0:
        return None

    if direction == "LONG":
        profit = current_price - entry
    else:
        profit = entry - current_price
    profit_in_atr = profit / atr

    candidates = []   # 所有候选止损目标位

    # ── 候选一:阶梯跟随止损 ──────────────────────────────────────────────
    steps = _trail_steps(mode)
    for trigger_atr, offset_atr in reversed(steps):
        if profit_in_atr >= trigger_atr:
            if direction == "LONG":
                candidates.append(entry + atr * offset_atr)
            else:
                candidates.append(entry - atr * offset_atr)
            break

    # ── 候选二:RR保护(冲突四合并,R>1.5且TP1未成交,仅触发一次)──────
    if not pos.get("rr_protection_triggered") and not tp1_filled and profit > 0:
        sl_dist = abs(entry - old_sl)
        if sl_dist > 0:
            r_value = profit / sl_dist
            if r_value >= 1.5:
                rr_sl = (entry + sl_dist * 0.8) if direction == "LONG" else (entry - sl_dist * 0.8)
                candidates.append(rr_sl)

    if not candidates:
        return None

    # ── 取最有利值(做多取最高,做空取最低),且止损只能向有利方向移动 ──
    if direction == "LONG":
        best_sl = max(candidates)
        if best_sl <= old_sl + 1e-9:
            return None
        # 标记 RR 保护已触发(如果最终选择的是 RR 候选)
        if len(candidates) >= 2:
            rr_candidate = candidates[-1]
            if abs(best_sl - rr_candidate) < 1e-8:
                pos["rr_protection_triggered"] = True
    else:
        best_sl = min(candidates)
        if best_sl >= old_sl - 1e-9:
            return None
        if len(candidates) >= 2:
            rr_candidate = candidates[-1]
            if abs(best_sl - rr_candidate) < 1e-8:
                pos["rr_protection_triggered"] = True

    return best_sl


# ─────────────────────────────────────────────────────────────────────────────
# 移动止损执行(先挂新单再撤旧单,最小化裸仓窗口)
# ─────────────────────────────────────────────────────────────────────────────
async def _execute_trail_sl(pos: dict, new_sl: float) -> bool:
    symbol    = pos["symbol"]
    # [v3.87 STEP7-5] 删除未使用的 direction 赋值
    pos_id    = pos.get("pos_id", pos.get("symbol","unknown"))
    old_sl_id = pos.get("sl_order_id")
    qty_remaining = float(pos.get("quantity_remaining", pos.get("quantity", 0)))

    # v3.56: 持 _guard_locks 防止与 _adjust_sl_tp_once 并发修改同一仓位的止损单
    lock = _guard_locks[pos_id]
    if lock.locked():
        logger.debug(f"[pm] {symbol} _execute_trail_sl 跳过:_guard_lock 被占用")
        return False
    async with lock:
        return await _execute_trail_sl_inner(pos, new_sl, pos_id, old_sl_id, qty_remaining)

async def _execute_trail_sl_inner(pos: dict, new_sl: float,
                                   pos_id: str, old_sl_id, qty_remaining: float) -> bool:
    symbol    = pos["symbol"]
    direction = pos["direction"].upper()

    precision = (await asyncio.to_thread(_get_precision, symbol))
    tick = precision["tick"]
    step = precision["step"]
    close_side = "SELL" if direction == "LONG" else "BUY"

    pos_side = "LONG" if direction == "LONG" else "SHORT"

    # v3.56: 先挂新SL再撤旧SL,消除先撤后挂的裸仓窗口(约200ms)
    # 1. 先挂新 Algo STOP_MARKET 止损单
    try:
        sl_r = _round_price(new_sl, tick)
        new_resp = await asyncio.get_running_loop().run_in_executor(
            None, lambda: api.place_sl(
                symbol=symbol,
                side=close_side,
                position_side=pos_side,
                trigger_price=sl_r,
                quantity=_round_qty(qty_remaining, step),
            )
        )
        new_sl_id = int(new_resp.get("algoId") or new_resp.get("orderId") or 0)
        if not new_sl_id:
            raise RuntimeError(f"algoId=0, resp={new_resp}")
        logger.info(f"[pm] {symbol} 新止损单 algoId={new_sl_id} stopPrice={sl_r:.6g}")
    except Exception as e:
        logger.error(f"[pm] {symbol} 新止损单提交失败: {e}")
        (await asyncio.to_thread(update_position, pos_id, {"sl_target_price": new_sl, "sl_sync_status": "failed"}))
        return False

    # 2. 新单确认成功后,撤旧SL(确保新保护已就位再撤旧的,零裸仓窗口)
    try:
        all_algo = await asyncio.get_running_loop().run_in_executor(
            None, lambda: api.get_algo_open_orders(symbol)
        )
        for ao in (all_algo or []):
            aid = int(ao.get("algoId", 0))
            owned = (str(aid) == str(old_sl_id) and aid != new_sl_id
                     and ao.get("symbol") == symbol
                     and ao.get("side") == close_side
                     and ao.get("positionSide") in (pos_side, "BOTH")
                     and ao.get("orderType", ao.get("type")) in ("STOP", "STOP_MARKET"))
            if str(aid) == str(old_sl_id) and not owned:
                logger.warning(f"[pm] {symbol} 旧止损归属或类型无法确认，保留 id={aid}")
            if owned:
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None, lambda _aid=aid: api.cancel_algo_order(symbol, _aid)
                    )
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"[pm] {symbol} 撤旧SL失败(新单已保护,不影响安全): {e}")

    # 3. 更新状态(问题三修复:RR保护标志同步持久化)
    _update_fields = {
        "sl_price":    sl_r,
        "sl_target_price": new_sl,
        "sl_sync_status": "confirmed",
        "sl_order_id": new_sl_id,
    }
    # 如果 _calc_trail_sl 内将 rr_protection_triggered 设为 True,同步写入
    if pos.get("rr_protection_triggered"):
        _update_fields["rr_protection_triggered"] = True
    if (await asyncio.to_thread(update_position, pos_id, _update_fields)) is False:
        logger.error(f"[pm] {symbol} 新止损已受理但持仓状态写入失败")
        return False
    logger.info(f"[pm] {symbol} 移动止损完成 {pos.get('sl_price'):.6g} → {new_sl:.6g}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# A. WebSocket 实时监听任务
# ─────────────────────────────────────────────────────────────────────────────
def _pm_hub_enabled() -> bool:
    try:
        import config as _c
        return bool(getattr(_c, "MARKET_DATA_HUB_ENABLED", False))
    except Exception:
        return False


def _pm_mark_price(symbol: str) -> dict:
    """巡检用标记价：hub 开启、就绪且数据足够新(<=5s)时读 hub，否则回落 REST（开关关闭时与原来完全一致）。"""
    if _pm_hub_enabled():
        try:
            from shared import market_data_hub as _hub
            if _hub.is_ready():
                _d = _hub.get_mark_price(symbol)
                if _d:
                    logger.debug(f"[hub] {symbol} price={_d.get('markPrice')} age={_d.get('age_sec')}s")
                    return _d
        except Exception as _e:
            logger.debug(f"[hub] 标记价读取失败，回落 REST: {_e}")
    return api.get_mark_price(symbol)


def _pm_klines(symbol: str, interval: str, limit: int) -> list:
    """巡检用 K 线（格式与 REST 完全一致）：hub 就绪时读 hub，None 则回落 api.get_klines。"""
    if _pm_hub_enabled():
        try:
            from shared import market_data_hub as _hub
            _d = _hub.get_klines(symbol, interval, limit)
            if _d:
                logger.debug(f"[hub] {symbol} {interval} klines n={len(_d)}（来自 WS 缓存）")
                return _d
        except Exception as _e:
            logger.debug(f"[hub] K线读取失败，回落 REST: {_e}")
    return api.get_klines(symbol, interval, limit)


class PositionManager:
    """
    持仓管理器主类。
    由 main.py 实例化后调用 start() 启动两个并行任务。
    ws_listener: WsOrderListener 实例(由 main 传入)。
    """

    def __init__(self, ws_listener: Any = None):
        self._ws_listener = ws_listener
        self._running     = False
        self._tasks: list[asyncio.Task] = []

    async def start(self):
        self._running = True
        t1 = asyncio.ensure_future(self._mark_price_loop())
        t2 = asyncio.ensure_future(self._order_event_loop())
        t3 = asyncio.ensure_future(self._patrol_loop())
        t4 = asyncio.ensure_future(self._daily_report_loop())
        self._tasks = [t1, t2, t3, t4]
        logger.info("[pm] PositionManager 已启动")

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()

    # ── A1. 标记价格实时监听(防抖 1 秒)────────────────────────────────────
    async def _mark_price_loop(self):
        while self._running:
            try:
                positions = (await asyncio.to_thread(get_all_positions))
                open_pos  = [p for p in positions if p.get("status") == "open"]
                if not open_pos:
                    await asyncio.sleep(2)
                    continue

                for pos in open_pos:
                    symbol = pos["symbol"]
                    # 初始化峰值水位线
                    pid = pos.get("pos_id", pos.get("symbol","unknown"))
                    if pid not in _peak:
                        # [v3.87 STEP7-5] 删除未使用的 direction 赋值
                        _peak[pid] = float(pos["entry_price"])
                        _mfe[pid]  = 0.0
                        _mae[pid]  = 0.0

                # 拉标记价格
                all_prices = await asyncio.get_running_loop().run_in_executor(
                    None, api.get_all_mark_prices
                )
                price_map = {p["symbol"]: float(p["markPrice"]) for p in all_prices}

                now = time.time()
                for pos in open_pos:
                    symbol = pos["symbol"]
                    price  = price_map.get(symbol)
                    if price is None:
                        continue

                    # 防抖:同一标的 1 秒内只处理一次
                    last_ts = _last_price_ts.get(symbol, 0)
                    if now - last_ts < PRICE_DEBOUNCE_SEC:
                        continue
                    _last_price[symbol]    = price
                    _last_price_ts[symbol] = now

                    # [v3.83 健壮性] 单个持仓处理失败不应中断整轮价格循环,
                    #   否则排在后面的持仓会被连带跳过(BUG-1 就是这样放大影响的)。
                    try:
                        await self._on_price_update(pos, price)
                    except asyncio.CancelledError:
                        raise
                    except Exception as _pu_e:
                        logger.warning(f"[pm] {symbol} _on_price_update 异常(已隔离,不影响其他持仓): {_pu_e}")

                await asyncio.sleep(1)

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(f"[pm] mark_price_loop 异常: {e}")
                await asyncio.sleep(5)

    async def _on_price_update(self, pos: dict, price: float):
        """价格更新 → 检查动态止损"""
        pid       = pos.get("pos_id", pos.get("symbol","unknown"))
        direction = pos["direction"].upper()

        # 更新峰值水位线 + MFE/MAE
        entry_p = float(pos.get("entry_price", price))
        if direction == "LONG":
            _peak[pid] = max(_peak.get(pid, price), price)
            fav = (price - entry_p) / entry_p
            adv = (entry_p - price) / entry_p
        else:
            _peak[pid] = min(_peak.get(pid, price), price)
            fav = (entry_p - price) / entry_p
            adv = (price - entry_p) / entry_p
        _mfe[pid] = max(_mfe.get(pid, 0.0), fav)   # 最大有利偏移
        _mae[pid] = max(_mae.get(pid, 0.0), adv)   # 最大不利偏移

        # ── MFE 高水位回撤保护(v3.24)──────────────────────────────────────
        # 条件:1 曾盈利 > 0.5%(排除噪声)2 当前仍盈利 3 从峰值回撤超过 50%
        # 注意:_mfe 存小数(0.03 = 3%),门槛 0.005 = 0.5%
        # 目的:防止「从 +3% 滑回 +0%」白送利润
        _mfe_val = _mfe[pid]
        if _mfe_val > 0.005 and fav > 0:
            _drawdown_from_peak = (_mfe_val - fav) / _mfe_val
            if _drawdown_from_peak >= 0.5:
                # v3.39: 加绝对浮盈门槛--浮盈须≥TP1距离×30%才值得锁利
                # 防止「仓位才涨0.1%就触发MFE回撤」的误平(绝对收益太小,不值当)
                _entry_p  = float(pos.get("entry_price", 0))
                _tp1_p    = float(pos.get("tp1_price", 0))
                _tp1_dist = abs(_tp1_p - _entry_p) / (_entry_p + 1e-9) if _entry_p > 0 else 0
                _min_lock = _tp1_dist * 0.3   # TP1距离×30% 才算「值得锁利」
                _sym = pos.get("symbol", pid)
                if fav >= _min_lock:
                    logger.info(
                        f"[pm] {_sym} MFE高水位回撤保护触发: "
                        f"峰值盈利={_mfe_val*100:.2f}% 当前={fav*100:.2f}% "
                        f"回撤={_drawdown_from_peak*100:.0f}%≥50% 绝对浮盈={fav*100:.2f}%≥{_min_lock*100:.2f}%,平仓锁利"
                    )
                    await _force_close_static(pos, "mfe_drawback_50pct")  # v3.56 fix: 原 _close_position 未定义
                    return
                else:
                    logger.debug(
                        f"[pm] {_sym} MFE回撤≥50%但绝对浮盈={fav*100:.2f}%<{_min_lock*100:.2f}%,跳过锁利"
                    )

        # ── v3.45 TP1 后超时保本止损(OR 双条件)────────────────────────────────
        # 条件A:浮盈已接近 0(< 1%),无论持仓多久,直接保本--防止「盈利单变亏损」
        # 条件B:持仓 > 48H + 历史峰值浮盈 > 5% + 当前已从峰值回撤超 80%--长持仓深回撤才保本
        # 两个条件 OR,任一满足即移到保本止损位
        _tp1_filled = pos.get("tp1_filled", False)
        # [v3.83 修复 BUG-1] 原代码使用未定义的变量 `_entry`:只要某持仓 tp1_filled=True,
        #   这里就会抛 NameError。该异常没有被就地捕获,会冒泡到 _mark_price_loop 的外层 except,
        #   导致1「TP1 后保本」从未生效 2同一轮循环里排在它后面的持仓价格更新被整体跳过(移动止损/MFE 保护延迟 5 秒且每轮重复)。
        #   修复:在此处用本函数已有的 entry_p 显式定义 _entry。
        _entry = entry_p
        if _tp1_filled and _entry > 0:
            # [v3.83 修复 BUG-2] 原代码读取 pos["open_ts"],但持仓记录里开仓时间字段是 "open_time"(毫秒,见 executor/_check_time_stop),
            #   "open_ts" 不存在 → 默认 0 → _hold_h≈49 万小时,使条件B中的「持仓>72H」恒为真,
            #   只要峰值浮盈>5% 且当前浮亏>0.5% 就会提前触发保本止损。改读 open_time,缺失时回退为当前时间(持仓时长=0,不误触发)。
            _open_ms = float(pos.get("open_time") or 0)
            _hold_h  = ((time.time() * 1000 - _open_ms) / 3_600_000) if _open_ms > 0 else 0.0
            _sl_now  = float(pos.get("sl_price", 0))
            _dir     = pos.get("direction", "long").upper()
            _mfe_val = _mfe.get(pid, 0.0)   # 历史峰值浮盈(小数,0.05=5%)
            # fav 已在上方计算完毕(当前浮盈比例)
            _cond_a = fav < 0.0                           # 条件A:跌穿入场价(浮盈变负)
            _cond_b = (_hold_h > 72                       # 条件B:72H+大峰值+彻底转负
                       and _mfe_val > 0.05
                       and fav < -0.005)                  # 已亏损 0.5%
            _should_protect = _cond_a or _cond_b
            # [v3.84 优化 OPT-2] 逻辑冲突修复:触发条件要求「浮盈<0(价格已跌穿入场价)」,
            #   但原动作是把止损移到入场价上方/下方 0.1%,该价位在当前价的错误一侧,交易所会拒单(-2021)或立即市价成交。
            #   新:此时直接市价平掉剩余仓位(TP1 利润已兑现,剩余仓位回到亏损侧即离场);
            #   价格仍在有利一侧时保持原来的「移动止损到保本」。判定逻辑见 shared/opt_v384.tp1_protect_action(含单测)。
            if _should_protect:
                # 先生成原因文本(原代码在后面的分支里才定义,新逻辑需要提前使用)
                _reason = (f"浮盈{fav*100:.2f}%<0跌穿入场价" if _cond_a
                           else f"持仓{_hold_h:.0f}H+峰值{_mfe_val*100:.1f}%+当前{fav*100:.2f}%<-0.5%")
                try:
                    from config import OPT_TP1_PROTECT_CLOSE as _OPT_TP1
                except Exception:
                    _OPT_TP1 = False
                from shared.opt_v384 import tp1_protect_action as _tp1_action
                _act = _tp1_action(_dir, _sl_now, _entry, fav, _OPT_TP1)
                if _act == "close":
                    logger.info(
                        f"[PM] {pos.get('symbol')} TP1后保本保护:{_reason},止损仍在亏损侧,"
                        f"改为市价平掉剩余仓位(OPT-2)"
                    )
                    await _force_close_static(pos, "tp1_protect_exit")
                    return
                if _act == "none":
                    _should_protect = False
            if _should_protect:
                _reason = (f"浮盈{fav*100:.2f}%<0跌穿入场价" if _cond_a
                           else f"持仓{_hold_h:.0f}H+峰值{_mfe_val*100:.1f}%+当前{fav*100:.2f}%<-0.5%")
                if _dir == "LONG" and _sl_now < _entry:
                    _new_sl_be = round(_entry * 1.001, 8)
                    logger.info(
                        f"[PM] {pos.get('symbol')} TP1保本触发({_reason}),"
                        f"止损上移 {_sl_now:.6g}→{_new_sl_be:.6g}"
                    )
                    await _execute_trail_sl(pos, _new_sl_be)
                    return
                elif _dir == "SHORT" and _sl_now > _entry:
                    _new_sl_be = round(_entry * 0.999, 8)
                    logger.info(
                        f"[PM] {pos.get('symbol')} TP1保本触发({_reason}),"
                        f"止损下移 {_sl_now:.6g}→{_new_sl_be:.6g}"
                    )
                    await _execute_trail_sl(pos, _new_sl_be)
                    return

        # 仅当 trailing_stop_activated=True 时才执行(TP1已触发或已开仓超过一定时间)
        # 按规范:TP1触发后激活;但也可以在开仓后立即激活(此处立即激活)
        new_sl = _calc_trail_sl(pos, price)
        if new_sl is not None:
            await _execute_trail_sl(pos, new_sl)

    # ── A2. 订单状态监听(复用 ws_listener)─────────────────────────────────
    async def _order_event_loop(self):
        if self._ws_listener is None:
            logger.info("[pm] 无 ws_listener,跳过订单事件监听(降级模式)")
            return

        while self._running:
            try:
                positions = (await asyncio.to_thread(get_all_positions))
                open_pos    = [p for p in positions if p.get("status") == "open"]
                pending_pos = [p for p in positions if p.get("status") == "pending"
                               and p.get("open_order_id")]

                # ── open 持仓:监听 SL/TP 成交 ───────────────────────────────
                for pos in open_pos:
                    sl_id  = pos.get("sl_order_id")
                    tp1_id = pos.get("tp1_order_id")
                    tp2_id = pos.get("tp2_order_id")
                    ids = [i for i in [sl_id, tp1_id, tp2_id] if i]
                    if not ids:
                        continue

                    event = await self._ws_listener.wait_any_order_event(ids, timeout=0.1)
                    if event is None:
                        continue

                    oid    = int(event.get("i", 0))
                    status = event.get("X", "")
                    if status != "FILLED":
                        continue

                    await self._handle_order_filled(pos, oid, event)

                # ── v3.55 pending 入场单:WS 实时感知成交,不再等 patrol 5分钟 ──
                # 原来 pending 成交后最长等 5 分钟 patrol 才发现并补挂 SL/TP
                # 现在通过 WS ORDER_TRADE_UPDATE 实时感知,收到立即补挂
                for pos in pending_pos:
                    if pos.get("status") == "patching":
                        continue
                    _oid = pos.get("open_order_id")
                    if not _oid:
                        continue
                    _event = await self._ws_listener.wait_any_order_event(
                        [int(_oid)], timeout=0.05
                    )
                    if _event is None:
                        continue
                    _status = _event.get("X", "")
                    if _status != "FILLED":
                        continue
                    # [v3.89 #2] 成交均价必须用累计均价 ap（与累计成交量 z 配套）；L 是最后一笔成交价，
                    #   分批成交时与均价不同，用它补挂止损会算错。ap 缺失或为 0 时不回退到 L：
                    #   记 warning 并跳过，由 patrol 的 REST 确认路径补挂（最长延迟 5 分钟，已知 KI-001）。
                    _symbol = pos["symbol"]
                    try:
                        _fill_price = float(_event.get("ap") or 0)
                    except (TypeError, ValueError):
                        _fill_price = 0.0
                    _fill_qty   = float(_event.get("z", 0))
                    if _fill_price <= 0:
                        logger.warning(f"[pm] {_symbol} pending入场单 {_oid} 成交事件缺少有效均价 ap="
                                       f"{_event.get('ap')!r}，不回退到最后成交价 L，交给 patrol 补挂")
                        continue
                    if _fill_qty <= 0:
                        continue
                    _pid    = pos.get("pos_id", pos.get("symbol", "unknown"))
                    logger.info(
                        f"[pm] ⚡ {_symbol} pending入场单 {_oid} WS实时感知成交 "
                        f"price={_fill_price:.6g} qty={_fill_qty},立即补挂SL/TP"
                    )
                    (await asyncio.to_thread(update_position, _pid, {"status": "patching"}))
                    mark_system_order_update(_symbol)
                    try:
                        await _补挂保护单(pos, _fill_price, _fill_qty, _pid, _symbol)
                    except Exception as _ge:
                        (await asyncio.to_thread(update_position, _pid, {"status": "pending"}))
                        logger.error(f"[pm] {_symbol} WS感知后补挂失败: {_ge}")
                        await _notify_async(
                            f"⚠️ {_symbol} 入场单成交但补挂止损失败,请人工介入!\n错误:{_ge}"
                        )

                await asyncio.sleep(0.2)

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(f"[pm] order_event_loop 异常: {e}")
                await asyncio.sleep(2)

    async def _handle_order_filled(self, pos: dict, filled_oid: int, event: dict):
        pid       = pos.get("pos_id", pos.get("symbol","unknown"))
        symbol    = pos["symbol"]
        direction = pos["direction"].upper()
        sl_id     = pos.get("sl_order_id")
        tp1_id    = pos.get("tp1_order_id")
        tp2_id    = pos.get("tp2_order_id")
        tp2_qty   = float(pos.get("tp2_quantity", 0))
        sl_price  = float(pos.get("sl_price", 0))
        tp1_filled = pos.get("tp1_filled", False)

        precision = (await asyncio.to_thread(_get_precision, symbol))

        async def _cancel(oid, is_algo: bool = False):
            """撤单:SL 是 Algo 单用 cancel_algo_order,TP 是普通限价单用 cancel_order"""
            if oid is None: return
            for attempt in range(2):
                try:
                    if is_algo:
                        await asyncio.get_running_loop().run_in_executor(
                            None, lambda: api.cancel_algo_order(symbol, oid)
                        )
                    else:
                        await asyncio.get_running_loop().run_in_executor(
                            None, lambda: api.cancel_order(symbol, oid)
                        )
                    return
                except Exception as e:
                    if attempt == 0:
                        await asyncio.sleep(0.3)
                    else:
                        await _notify_async(f"⚠️ {symbol} 撤单 {oid} 失败,请人工处理。\n错误:{e}")

        # ── 止损触发 ──────────────────────────────────────────────────────────
        if filled_oid == sl_id:
            logger.info(f"[pm] {symbol} 止损触发,撤 TP1/TP2")
            await asyncio.gather(
                _cancel(tp1_id) if not tp1_filled else asyncio.sleep(0),
                _cancel(tp2_id),
            )
            await self._close_position_post(pid, symbol, "sl", event)
            return

        # ── TP1 触发 ──────────────────────────────────────────────────────────
        if filled_oid == tp1_id and not tp1_filled:
            single_tp = pos.get("tp_mode") == "single" or (pos.get("tp_mode") is None and pos.get("tp2_price") is None and not tp2_id)
            if single_tp:
                await _cancel(sl_id, is_algo=True)
                (await asyncio.to_thread(update_position, pid, {"tp1_filled": True, "quantity_remaining": 0.0}))
                await self._close_position_post(pid, symbol, "tp1", event)
                return
            logger.info(f"[pm] {symbol} TP1触发,更新剩余数量,补挂剩余40%仓位的SL单")
            (await asyncio.to_thread(update_position, pid, {
                "tp1_filled":        True,
                "quantity_remaining": tp2_qty,
                "trailing_stop_activated": True,
            }))
            # v3.55: TP1触发后为剩余40%仓位补挂真实SL单(原来只靠patrol软止损,5分钟盲区)
            if tp2_qty > 0 and sl_price > 0:
                try:
                    precision = (await asyncio.to_thread(_get_precision, symbol))
                    _sl_qty   = _round_qty(tp2_qty, precision["step"])
                    # [fix F] TP1 成交后剩余仓位止损移到入场价保本：
                    # LONG 取 max(原止损, 入场成交价)，SHORT 取 min(原止损, 入场成交价)，
                    # 保证止损至少不劣于原止损、且不劣于保本价。
                    _fill_ref = float(pos.get("fill_price") or pos.get("entry_price") or 0.0)
                    if _fill_ref > 0:
                        _sl_be = (max(sl_price, _fill_ref) if direction == "LONG"
                                  else min(sl_price, _fill_ref))
                    else:
                        _sl_be = sl_price
                    _sl_p     = _round_price(_sl_be, precision["tick"])
                    if abs(_sl_p - sl_price) > 0:
                        logger.info(
                            f"[pm] {symbol} TP1后剩余仓位止损移向保本: "
                            f"{sl_price} → {_sl_p} (入场参考 {_fill_ref})"
                        )
                    _close_side = "SELL" if direction == "LONG" else "BUY"
                    _pos_side   = "LONG" if direction == "LONG" else "SHORT"
                    _resp = await asyncio.get_running_loop().run_in_executor(
                        None, lambda: api.place_sl(
                            symbol=symbol,
                            side=_close_side,
                            position_side=_pos_side,
                            trigger_price=_sl_p,
                            quantity=_sl_qty,
                        )
                    )
                    _new_sl_id = _resp.get("orderId") or _resp.get("algoId")
                    if _new_sl_id:
                        # 撤旧SL(如果有),更新pos_state里的sl_order_id
                        if sl_id:
                            try:
                                await asyncio.get_running_loop().run_in_executor(
                                    None, lambda: api.cancel_algo_order(symbol, sl_id)
                                )
                            except Exception:
                                pass
                        (await asyncio.to_thread(update_position, pid, {"sl_order_id": int(_new_sl_id), "sl_price": _sl_p}))
                        logger.info(
                            f"[pm] {symbol} TP1后补挂SL单成功: "
                            f"qty={_sl_qty} price={_sl_p} id={_new_sl_id}"
                        )
                    else:
                        logger.warning(f"[pm] {symbol} TP1后补挂SL单失败: {_resp}")
                        await _notify_async(f"🚨 {symbol} TP1触发后补挂SL失败,剩余40%仓位无止损保护!")
                        # 同步卡片
                        _pos_tp1f = (await asyncio.to_thread(get_all_positions))
                        _tp1_sid = next((p.get("_sig_id","") for p in _pos_tp1f if p.get("symbol")==symbol), "")
                        if _tp1_sid: await card_append_async(_tp1_sid, fmt_node_sl_lost())
                except Exception as _tp1_sl_e:
                    logger.error(f"[pm] {symbol} TP1后补挂SL异常: {_tp1_sl_e}")
                    await _notify_async(f"🚨 {symbol} TP1触发后补挂SL异常,请检查仓位!\n错误:{_tp1_sl_e}")
                    _pos_tp1f2 = (await asyncio.to_thread(get_all_positions))
                    _tp1_sid2 = next((p.get("_sig_id","") for p in _pos_tp1f2 if p.get("symbol")==symbol), "")
                    if _tp1_sid2: await card_append_async(_tp1_sid2, fmt_node_sl_lost())
            return

        # ── TP2 触发 ──────────────────────────────────────────────────────────
        if filled_oid == tp2_id:
            logger.info(f"[pm] {symbol} TP2触发,撤止损单")
            await _cancel(sl_id, is_algo=True)  # SL 是 Algo 单
            (await asyncio.to_thread(update_position, pid, {"tp2_filled": True}))   # 标记 TP2 已成交,供 trade_log 和平仓后分析区分关闭原因
            await self._close_position_post(pid, symbol, "tp2", event)
            return

    # ── B. 定时巡检任务(每5分钟)──────────────────────────────────────────
    async def _patrol_loop(self):
        # [v3.87 STEP7-4] 说明:巡检职责分布在两处,修改任一处前请注意另一处。
        #   本循环(PositionManager.start 启动):孤单检测、TP REST 确认、时间止损 / 突破失败 / 反向信号、健康检查(含计数器)、挂单有效期。
        #   patrol_once(main.py 的 _loop_patrol 调用):除上述同类检查外,还负责 funding_arb 专用巡检、TP2 动态收紧、加仓。
        #   两处的 B1 检查同时存在,重复平仓由 _force_close_static 开头的状态重读兜底(见 STEP7-2)。
        global _health_check_counter
        # 错开 scanner 的30秒
        await asyncio.sleep(30)

        while self._running:
            try:
                _health_check_counter += 1
                do_health = (_health_check_counter % PM_HEALTH_CHECK_INTERVAL == 0)

                positions = (await asyncio.to_thread(get_all_positions))
                open_pos  = [p for p in positions if p.get("status") == "open"]
                # patching = 正在补挂中,跳过巡检防止重入
                pending   = [p for p in positions if p.get("status") == "pending"]

                now_ms = int(time.time() * 1000)

                # B-1. 孤单检测:pos_state 有记录但交易所持仓已为零 → 手动平仓,清理挂单
                for pos in open_pos:
                    await self._check_manual_close(pos)
                # 重新加载(_check_manual_close 可能删除了仓位)
                positions = (await asyncio.to_thread(get_all_positions))
                open_pos  = [p for p in positions if p.get("status") == "open"]

                # B0. REST 轮询确认 TP1/TP2 成交(WS不可用时的保底,每轮都跑)
                for pos in open_pos:
                    await self._check_tp_filled_rest(pos)

                # B1. 时间止损 + 突破失败 + 反向信号主动平仓
                for pos in open_pos:
                    await self._check_time_stop(pos, now_ms)
                    if pos.get("mode") == "breakout":
                        await self._check_breakout_fail(pos)
                    await self._check_exit_signal(pos)

                # v3.56: B1 可能平仓,刷新 open_pos 避免 B2/B3 对已平仓仓位跑 API
                positions = (await asyncio.to_thread(get_all_positions))
                open_pos  = [p for p in positions if p.get("status") == "open"]

                # B2. 健康检查(每30分钟)
                # v3.6: funding_arb 由专用 _patrol_funding_arb 处理,EMA 评分对其无效,跳过
                if do_health:
                    for pos in open_pos:
                        if pos.get("sig_type") == "funding_arb":
                            continue
                        await self._check_health(pos)

                # B3. 挂单有效期
                for pos in pending:
                    await self._check_pending_expire(pos, now_ms)

                # B4. 挂单健康检查(每30分钟)
                if do_health and pending:
                    for pos in pending:
                        await _check_pending_health_static(pos)

                # B5. 日报(UTC 02:00)
                await self._maybe_daily_report()

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[pm] patrol_loop 异常: {e}")

            await asyncio.sleep(300)   # 5分钟

    # ── B-1. 孤单检测(手动平仓后清理挂单)────────────────────────────────────
    async def _check_manual_close(self, pos: dict):
        """
        检查 pos_state 里 open 仓位在交易所是否真的还有持仓。
        若持仓量为零(手动市价止盈/止损),则:
          1. 撤销所有对应的 Algo SL 和限价 TP 挂单
          2. 将 pos_state 标记为 closed 并删除
        """
        symbol    = pos["symbol"]
        pid       = pos.get("pos_id", symbol)
        direction = pos["direction"].upper()
        pos_side  = "LONG" if direction == "LONG" else "SHORT"
        qty       = float(pos.get("quantity_remaining", pos.get("quantity", 0)))

        try:
            live = await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.get_position(symbol)
            )
            # [fix] 第一道保护：API 返回 None 或空列表时，跳过判定，避免网络不通误判为已平仓
            if live is None or live == []:
                logger.warning(
                    f"[pm] {symbol} get_position 返回空，跳过平仓判定，避免误清仓"
                )
                return
            live_qty = sum(
                abs(float(p.get("positionAmt", 0)))
                for p in live
                if p.get("symbol") == symbol
                and p.get("positionSide", "").upper() == pos_side
            )
        except Exception as e:
            logger.debug(f"[pm] {symbol} 孤单检测查持仓失败: {e}")
            return

        if live_qty > qty * 0.05:   # 持仓量 > 5% → 仓位仍存在,跳过
            return

        # [fix] 第二道保护：live_qty 为零时，3 秒后二次确认，防止瞬时 API 抗平误判
        try:
            await asyncio.sleep(3)
            live2 = await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.get_position(symbol)
            )
            if live2 is None or live2 == []:
                logger.warning(
                    f"[pm] {symbol} 二次确认 get_position 返回空，跳过平仓判定"
                )
                return
            live_qty2 = sum(
                abs(float(p.get("positionAmt", 0)))
                for p in live2
                if p.get("symbol") == symbol
                and p.get("positionSide", "").upper() == pos_side
            )
            if live_qty2 > qty * 0.05:
                logger.info(
                    f"[pm] {symbol} 二次确认持仓存在(二次={live_qty2:.4f})，取消平仓判定"
                )
                return
            # 两次都为零，才继续清理流程
            logger.info(
                f"[pm] {symbol} 二次确认持仓仍为零(一次={live_qty:.4f} 二次={live_qty2:.4f})，继续清理"
            )
        except Exception as e2:
            logger.debug(f"[pm] {symbol} 二次确认查持仓失败，跳过平仓判定: {e2}")
            return

        # ── 系统撤单锁:系统正在更新保护单时,跳过误判 ──────────────────────
        if is_system_order_update(symbol):
            logger.debug(f"[pm] {symbol} 系统撤单锁生效,跳过手动平仓判定(持仓={live_qty:.4f})")
            return

        # ── [BUG-FIX] 区分「SL 触发清零」vs「用户手动平仓」──────────────────
        # 判断逻辑:若 SL Algo 单已不在交易所且 SL 价格存在 → SL 已触发,
        # 写 close_reason="sl_triggered";否则仍视为手动平仓。
        _inferred_close_reason = "manual_close"
        _sl_id   = pos.get("sl_order_id")
        _sl_price = float(pos.get("sl_price", 0) or 0)
        if _sl_id and _sl_price > 0:
            try:
                _algo_orders = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.get_algo_open_orders(symbol)
                )
                _algo_ids = {int(o.get("algoId", 0)) for o in (_algo_orders or [])}
                if int(_sl_id) not in _algo_ids:
                    # SL 单已不存在 → 大概率是 SL 触发了
                    _inferred_close_reason = "sl_triggered"
                    logger.info(
                        f"[pm] {symbol} SL algoId={_sl_id} 已不在交易所挂单中,"
                        f"判定为 SL 触发平仓(非手动)"
                    )
            except Exception as _sl_check_e:
                logger.debug(f"[pm] {symbol} 孤单检测查 SL 挂单失败,回退为 manual_close: {_sl_check_e}")

        # 持仓量为零或极小 → 清理所有挂单
        logger.info(
            f"[pm] {symbol} 检测到仓位清零(交易所持仓={live_qty:.4f},本地记录={qty:.4f}),"
            f"判定: {_inferred_close_reason},清理挂单"
        )

        sl_id  = pos.get("sl_order_id")
        tp1_id = pos.get("tp1_order_id")
        tp2_id = pos.get("tp2_order_id")

        # 撤 SL(Algo)
        if sl_id:
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.cancel_algo_order(symbol, int(sl_id))
                )
                logger.info(f"[pm] {symbol} 已撤 SL algoId={sl_id}")
            except Exception as e:
                logger.debug(f"[pm] {symbol} 撤SL失败(可能已触发): {e}")

        # 撤 TP1
        if tp1_id and not pos.get("tp1_filled"):
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.cancel_order(symbol, int(tp1_id))
                )
                logger.info(f"[pm] {symbol} 已撤 TP1 orderId={tp1_id}")
            except Exception as e:
                logger.debug(f"[pm] {symbol} 撤TP1失败(可能已成交): {e}")

        # 撤 TP2
        if tp2_id and not pos.get("tp2_filled"):
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.cancel_order(symbol, int(tp2_id))
                )
                logger.info(f"[pm] {symbol} 已撤 TP2 orderId={tp2_id}")
            except Exception as e:
                logger.debug(f"[pm] {symbol} 撤TP2失败(可能已成交): {e}")

        # 额外保险:撤清该标的所有残余 Algo 单
        try:
            all_algo = await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.get_algo_open_orders(symbol)
            )
            for ao in (all_algo or []):
                aid = int(ao.get("algoId", 0))
                if aid:
                    try:
                        await asyncio.get_running_loop().run_in_executor(
                            None, lambda _id=aid: api.cancel_algo_order(symbol, _id)
                        )
                    except Exception:
                        pass
        except Exception:
            pass

        # 更新 pos_state
        (await asyncio.to_thread(update_position, pid, {"status": "closed", "close_reason": _inferred_close_reason}))
        (await asyncio.to_thread(_remove_position_checked, pid))
        logger.info(f"[pm] {symbol} 仓位清理完成 close_reason={_inferred_close_reason}, pos_state 已删除")

        # v3.78 fix: 手动平仓补写 trade_log
        # 原因:_check_manual_close 没有调用 _close_position_post_static,手动平仓全部丢失
        # 修复:平仓后查询交易所实际成交(get_user_trades),取真实出场价 + realizedPnl
        try:
            from shared.binance_api import get_user_trades as _get_user_trades
            _entry   = float(pos.get("entry_price", 0))
            _qty     = float(pos.get("quantity", qty))
            _dir     = pos.get("direction", "LONG").upper()
            _open_ms = int(pos.get("open_time", 0))
            # 仅查开仓时间之后的成交,防止拓占旧记录
            _start_ts = max(_open_ms, int(time.time() * 1000) - 3600_000)  # 最多查 1 小时内
            _trades  = (await asyncio.to_thread(_get_user_trades, symbol, start_time=_start_ts, limit=20))
            # 取平仓方向的成交:SHORT 对应 BUY,LONG 对应 SELL
            _close_side = "BUY" if _dir == "SHORT" else "SELL"
            _close_trades = [
                t for t in _trades
                if t.get("side", "").upper() == _close_side
                and t.get("positionSide", "").upper() == _dir
            ] or [t for t in _trades if t.get("side", "").upper() == _close_side]
            if _close_trades:
                _exit_price = sum(float(t["price"]) * float(t["qty"]) for t in _close_trades) / \
                              sum(float(t["qty"]) for t in _close_trades)
                _real_pnl   = sum(float(t.get("realizedPnl", 0)) for t in _close_trades)
            else:
                # 拿不到成交记录,用当前 mark price 估算
                _mp = (await asyncio.to_thread(api.get_mark_price, symbol))
                _exit_price = float(_mp.get("markPrice", _entry))
                _real_pnl   = (_entry - _exit_price) * _qty if _dir == "SHORT" else (_exit_price - _entry) * _qty
                logger.warning(f"[pm] {symbol} 手动平仓未找到成交记录,用 mark price 估算 pnl")

            _close_ms  = int(time.time() * 1000)
            _nominal   = _entry * _qty
            _pnl_pct   = _real_pnl / (_nominal + 1e-9) * 100
            _hold_min  = (_close_ms - _open_ms) / 60000
            _record = {
                "trade_id":          f"{_close_ms}_{symbol}",
                "symbol":            symbol,
                "direction":         _dir.lower(),
                "mode":              pos.get("mode", "unknown"),
                "volatility_tier":   pos.get("volatility_tier", ""),
                "signal_strength":   pos.get("signal_strength", ""),
                "entry_price":       _entry,
                "exit_price":        round(_exit_price, 8),
                "quantity":          _qty,
                "leverage":          int(pos.get("leverage", 1)),
                "nominal_value":     round(_nominal, 2),
                "pnl_usdt":          round(_real_pnl, 4),
                "pnl_pct":           round(_pnl_pct, 4),
                "holding_minutes":   round(_hold_min, 1),
                "open_time":         _open_ms,
                "close_time":        _close_ms,
                "close_reason":      _inferred_close_reason,
                "tp1_filled":        pos.get("tp1_filled", False),
                "tp2_filled":        pos.get("tp2_filled", False),
                "sl_degraded":       pos.get("sl_degraded", False),
                "liq_data_available": pos.get("liq_data_available", False),
                "vol_ratio_at_entry": round(float(
                    (pos.get("judge_detail") or {}).get("vol_ratio", 0) or 0), 4),
                "judge_detail":      pos.get("judge_detail", {}),
                "max_correlation":   round(float(pos.get("max_correlation", 0.0)), 4),
                "signal_id":         pos.get("_sig_id", ""),          # [fix #44]
                "fees_usdt":         round(_manual_fees, 6),          # [fix #44]
                "pnl_net_usdt":      round(_real_pnl - _manual_fees, 4),
            }
            TRADE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            from shared.trade_log_io import append_jsonl as _append_jsonl   # v3.93
            await asyncio.to_thread(_append_jsonl, TRADE_LOG_PATH, _record)
            logger.info(
                f"[pm] {symbol} {_inferred_close_reason} trade_log 已补写: "
                f"exit={_exit_price:.6g} pnl={_real_pnl:+.4f}U close_reason={_inferred_close_reason}"
            )
            # 补写熔断器盘口（[fix #44] 带实际手续费）
            _manual_fees = _fetch_actual_fees(symbol, int(pos.get("open_time", 0) or 0),
                                              float(pos.get("quantity", 0) or 0) * float(_exit_price or 0))
            try:
                _record_pnl(_real_pnl, symbol=symbol, fee=_manual_fees)
            except Exception:
                pass
        except Exception as _te:
            logger.error(f"[pm] {symbol} {_inferred_close_reason} trade_log 补写失败: {_te}")

        # 手动平仓/SL触发 → 不单独推送（_close_position_post_static 的平仓行已覆盖）
        logger.info(f"[pm] {symbol} {_inferred_close_reason} 挂单已清理")

    # ── B1. 时间止损 ─────────────────────────────────────────────────────────
    async def _check_time_stop(self, pos: dict, now_ms: int):
        # v3.56: funding_arb 有自己的最大持仓逻辑(_patrol_funding_arb),跳过通用时间止损
        if pos.get("sig_type") == "funding_arb":
            return
        pid       = pos.get("pos_id", pos.get("symbol","unknown"))
        symbol    = pos["symbol"]
        open_ms   = int(pos.get("open_time", 0))
        hold_min  = (now_ms - open_ms) / 60000
        entry     = float(pos["entry_price"])
        direction = pos["direction"].upper()

        price = _last_price.get(symbol)
        if price is None:
            try:
                md = (await asyncio.to_thread(_pm_mark_price, symbol))
                price = float(md.get("markPrice", entry))
            except Exception:
                price = entry

        pnl_pct = ((price - entry) / entry) if direction == "LONG" else ((entry - price) / entry)

        # ── v3.47 条件性时间止损:TP1 已触发 + 仍盈利 → 豁免全部时间止损 ─────────
        # 回测验证:AVA +55U 提升(最大回撤 0.00U),BTW +14U 提升
        # TP1 已触发说明系统入场时机准确,让 MFE 高水位保护(_patrol_positions)接管出场
        # 72H 硬上限兜底,防止永久持仓
        _hold_h_v347 = hold_min / 60
        if pos.get("tp1_filled") and pnl_pct > 0:
            if _hold_h_v347 < 72:
                logger.debug(
                    f"[pm] {symbol} v3.47 TP1已触发且仍盈利({pnl_pct*100:.1f}%),"
                    f"豁免时间止损(持仓{_hold_h_v347:.1f}H),由MFE高水位接管"
                )
                return  # 豁免本次时间止损检查
            else:
                # 72H 硬上限,即使 TP1 盈利也必须出场
                logger.info(f"[pm] {symbol} v3.47 TP1后持仓满72H硬上限,强制平仓(浮盈{pnl_pct*100:.1f}%)")
                await self._force_close(pos, "time_stop_hard_72h")
                return

        # ── [fix E] 接入 TIME_STOP_PROFIT_THRESHOLD：浮盈达到 ATR × 阈值后不再触发时间止损 ──
        # 原常量在 config 里标注「代码未引用」，实际未生效；此处接入。
        try:
            from config import TIME_STOP_PROFIT_THRESHOLD as _TSPT
            _atr_ref = float(pos.get("atr_at_entry", 0.0) or 0.0)
            if _atr_ref > 0:
                _profit_abs = (price - entry) if direction == "LONG" else (entry - price)
                if _profit_abs >= _atr_ref * float(_TSPT):
                    logger.info(
                        f"[pm] {symbol} 浮盈 {_profit_abs:.6g} ≥ ATR({_atr_ref:.6g})×{_TSPT}"
                        f"={_atr_ref * float(_TSPT):.6g}，跳过时间止损"
                    )
                    return
        except Exception as _ts_e:
            logger.debug(f"[pm] {symbol} 时间止损浮盈豁免检查异常，按原逻辑继续: {_ts_e}")

        # SL 辅助确认:每5分钟检查 Algo SL 是否仍在交易所,不在则重挂
        # 注意:本账号 SL 走 /fapi/v1/algoOrder,必须查 openAlgoOrders 而非 openOrders
        sl_order_id = pos.get("sl_order_id")
        sl_price    = float(pos.get("sl_price", 0))
        if sl_order_id and sl_price > 0:
            check_interval = 5 * 60
            last_check = _sl_check_ts.get(pid, 0)
            if time.time() - last_check >= check_interval:
                _sl_check_ts[pid] = time.time()
                try:
                    # 查 Algo 挂单(SL 走 Algo 接口)
                    algo_orders = await asyncio.get_running_loop().run_in_executor(
                        None, lambda: api.get_algo_open_orders(symbol)
                    )
                    algo_ids = {int(o.get("algoId", 0)) for o in (algo_orders or [])}
                    sl_exists = int(sl_order_id) in algo_ids
                    if not sl_exists:
                        # 先检查实际持仓,再决定是重挂还是标记已平仓
                        live_pos = await asyncio.get_running_loop().run_in_executor(
                            None, lambda: api.get_position(symbol)
                        )
                        live_qty = sum(abs(float(p.get("positionAmt", 0)))
                                      for p in (live_pos or [])
                                      if p.get("symbol") == symbol)
                        if live_qty == 0:
                            logger.info(f"[pm] {symbol} SL 已触发成交(实际持仓为零),标记已平仓")
                            await self._force_close(pos, "sl_triggered")
                            return
                        # 持仓仍存在 → 重挂新止损单
                        logger.warning(f"[pm] {symbol} Algo SL algoId={sl_order_id} 不在交易所,持仓仍存在,重挂")
                        close_side = "SELL" if direction == "LONG" else "BUY"
                        qty_remaining = float(pos.get("quantity_remaining", pos.get("quantity", 0)))
                        new_resp = await asyncio.get_running_loop().run_in_executor(
                            None, lambda: api.place_sl(
                                symbol=symbol, side=close_side,
                                position_side=direction,
                                trigger_price=sl_price,
                                quantity=qty_remaining,
                            )
                        )
                        new_id = int(new_resp.get("algoId") or new_resp.get("orderId") or 0)
                        if new_id:
                            (await asyncio.to_thread(update_position, pid, {"sl_order_id": new_id}))
                            logger.info(f"[pm] {symbol} 重挂 SL 成功: algoId={new_id} stopPrice={sl_price:.6g}")
                            # 仅在真正失效后重挂成功时通知,不打扰正常状态
                        else:
                            logger.error(f"[pm] {symbol} 重挂 SL 失败: resp={new_resp}")
                            # 追加到卡片 + 独立告警
                            try:
                                _sl_pos = next((p for p in (await asyncio.to_thread(get_all_positions)) if p.get("symbol")==symbol), {})
                                _sl_sid = _sl_pos.get("_sig_id", "")
                                _sl_warn = fmt_node_sl_lost()
                                if _sl_sid:
                                    await card_append_async(_sl_sid, _sl_warn)
                                else:
                                    await _notify_async(_sl_warn)
                            except Exception:
                                pass
                except Exception as e:
                    logger.error(f"[pm] {symbol} SL 检查失败: {e}")

        # 备用软止损(仅当 Algo SL 不存在时兜底,防止进程崩溃后无保护)
        if sl_price > 0 and not sl_order_id:
            sl_triggered = (
                (direction == "LONG"  and price <= sl_price) or
                (direction == "SHORT" and price >= sl_price)
            )
            if sl_triggered:
                logger.warning(f"[pm] {symbol} 备用软止损触发: mark={price:.6g} sl={sl_price:.6g}(Algo SL 丢失)")
                await self._force_close(pos, "soft_sl_fallback")
                return

        # 硬上限:所有类型统一 PM_HARD_MAX_HOLD_HOURS(当前 16H)。
        # [v3.85 DOC-1] 原注释写「reversal 16H、其他 12H」,与实现不符,已更正。
        # 注意:上方 v3.47「TP1 已成交且仍盈利」分支会先 return,这类持仓实际上限是 72H,不受此处 16H 约束。
        _hard_limit = PM_HARD_MAX_HOLD_HOURS * 60
        if hold_min >= _hard_limit:
            logger.info(f"[pm] {symbol} 时间止损(硬上限{_hard_limit//60:.0f}H)")
            await self._force_close(pos, "time_stop_hard")
            return

        # 功能三(盈亏比保护)已移除:
        # _check_time_stop 里只能更新本地 sl_price,无法移动交易所 Algo 止损单
        # RR保护由 _calc_trail_sl 的 RR候选二 统一处理(会真正执行交易所止损移动)

        # ── 早期横盘止损:reversal=8H未反弹超过1%,pullback/breakout=6H)────────────────
        # 豁免条件(两个同时满足才豁免):
        #   1. 槽位未满:当前持仓数 < RISK_MAX_POSITIONS
        #   2. 趋势结构仍在:4H EMA 方向与持仓方向一致且差距 > 0.5%
        _mode = pos.get("mode", "pullback")
        _stag_limit = 480 if _mode == "reversal" else 360  # reversal=8H, 其他=6H
        # [v3.84 优化 OPT-1] 原「浮亏>固定2%直接平仓」比止损(最小2.5%/3%/5%)更紧,等于隐形止损,抬高实现亏损次数。
        #   新:阈值 = max(2%, 0.6×止损距离);止损距离取不到时回退 2%(与原行为一致)。公式见 shared/opt_v384.stagnation_loss_threshold。
        try:
            from config import (OPT_STAGNATION_SCALE_BY_SL as _OPT_ST,
                                EARLY_STAGNATION_LOSS_FRAC as _ST_FRAC,
                                EARLY_STAGNATION_LOSS_MIN as _ST_MIN)
        except Exception:
            _OPT_ST, _ST_FRAC, _ST_MIN = False, 0.6, 0.02
        from shared.opt_v384 import stagnation_loss_threshold as _stag_thr
        _sl_dist_pct = abs(entry - sl_price) / entry if (entry > 0 and sl_price > 0) else 0.0
        _stag_loss_thr = _stag_thr(_sl_dist_pct, _OPT_ST, _ST_FRAC, _ST_MIN)
        _stag_upper = _stag_limit + 120  # 检查窗口=stag_limit ~ stag_limit+2H
        if hold_min >= _stag_limit and hold_min < _stag_upper:
            if not pos.get("early_stagnation_checked"):
                if pnl_pct >= 0.01:  # 浮盈已超1%,持仓有效,不触发
                    (await asyncio.to_thread(update_position, pid, {"early_stagnation_checked": True}))
                elif pnl_pct < -_stag_loss_thr:  # 浮亏超过动态阈值(OPT-1):已在持续下跌,直接平仓,不给豁免
                    logger.info(f"[pm] {symbol} 早期滞涨:浮亏={pnl_pct*100:.2f}%>{_stag_loss_thr*100:.1f}%(止损距离{_sl_dist_pct*100:.1f}%),直接平仓")
                    await self._force_close(pos, "early_stagnation")
                    (await asyncio.to_thread(update_position, pid, {"early_stagnation_checked": True}))
                    return
                else:  # -2% <= pnl_pct < 1%:横盘/小亏,看 EMA 结构
                    # ── 豁免检查 ──────────────────────────────────────────
                    _slot_free = False
                    _structure_ok = False
                    try:
                        from config import RISK_MAX_POSITIONS
                        _open_cnt = len([p for p in (await asyncio.to_thread(get_all_positions)) if p.get("status") == "open"])
                        _slot_free = _open_cnt < RISK_MAX_POSITIONS
                    except Exception as _e:
                        logger.warning(f"[pm] {symbol} 早期横盘豁免:槽位检查失败 {_e}")
                    try:
                        _klines_4h = _pm_klines(symbol, "4h", 60)
                        # 只用已闭合的 4H bar:排除最后一根(当前未完成bar)
                        # 未闭合bar的实时价格会随1H波动压低EMA20,导致gap虚小误杀持仓
                        _now_ms = int(time.time() * 1000)
                        _klines_4h_closed = [k for k in _klines_4h if k[6] < _now_ms]
                        _closes_4h = [float(k[4]) for k in _klines_4h_closed]
                        _ema20_4h = _calc_ema(_closes_4h, 20)
                        _ema50_4h = _calc_ema(_closes_4h, 50)
                        _ema_gap = (_ema20_4h - _ema50_4h) / _ema50_4h if _ema50_4h > 0 else 0
                        if direction == "LONG":
                            _structure_ok = _ema_gap > 0.005   # EMA20 > EMA50 且差距>0.5%
                        else:
                            _structure_ok = _ema_gap < -0.005  # EMA20 < EMA50 且差距>0.5%
                    except Exception as _e:
                        logger.warning(f"[pm] {symbol} 早期横盘豁免:EMA检查失败 {_e}")
                    if _slot_free and _structure_ok:
                        logger.info(
                            f"[pm] {symbol} 早期横盘止损豁免:槽位未满({_open_cnt}/{RISK_MAX_POSITIONS})"
                            f" 且4H趋势结构仍在(EMA_gap={_ema_gap*100:.2f}%),继续持有"
                        )
                        (await asyncio.to_thread(update_position, pid, {"early_stagnation_checked": True}))
                    else:
                        _reason = []
                        if not _slot_free:
                            _reason.append(f"槽位已满({_open_cnt}/{RISK_MAX_POSITIONS})")
                        if not _structure_ok:
                            _reason.append(f"4H结构失效(EMA_gap={_ema_gap*100:.2f}%)")
                        logger.info(f"[pm] {symbol} 早期横盘止损({_stag_limit//60}H内反弹不足1%,当前{pnl_pct*100:.2f}%)原因: {', '.join(_reason)}")
                        await self._force_close(pos, "early_stagnation")
                        (await asyncio.to_thread(update_position, pid, {"early_stagnation_checked": True}))
                        return

        # 软上限:reversal 12H、ema_trend 16H(4H趋势追踪需要更长窗口)、其余 8H
        mode = pos.get("mode", "pullback")   # _check_time_stop 内局部取,不依赖外部
        if mode == "reversal":
            _soft_limit = TIME_STOP_REVERSAL_HOURS * 60
        elif mode == "ema_trend":
            _soft_limit = EMA_TREND_MAX_HOLD_HOURS * 60  # 4H 趋势追踪,宽松上限
        else:
            _soft_limit = PM_MAX_HOLD_HOURS * 60
        if hold_min >= _soft_limit:
            # v3.47 无进展时间止损:TP1未触发 + 超时 + 浮盈<0.5% → 释放槽位
            if not pos.get("tp1_filled") and pnl_pct < 0.005:
                logger.info(
                    f"[pm] {symbol} v3.47 持仓{hold_min/60:.1f}H无进展"
                    f"(浮盈{pnl_pct*100:.2f}%<0.5%),时间止损释放槽位"
                )
                await self._force_close(pos, "time_stop_no_progress")
                return
            if pnl_pct <= 0:
                # ── MAE 感知时间止损(v3.22)─────────────────────────────────
                # MAE < 1%:价格没有大幅不利移动,只是横盘,给额外 2H 缓冲等待
                # MAE >= 1% 或 亏损 > 2%:真实下跌,直接止损
                pid_ts = pos.get("pos_id", symbol)
                mae_now = _mae.get(pid_ts, 0.0)  # 当前 MAE(持仓期间最大不利偏移 %)
                pnl_abs = abs(pnl_pct)
                _extra_buf_min = 120  # 最多额外等 2H
                if mae_now < 0.01 and pnl_abs < 0.02 and hold_min < _soft_limit + _extra_buf_min:
                    # 横盘磨底:MAE 小且亏损小,给额外 2H
                    logger.info(
                        f"[pm] {symbol} 时间止损缓冲:MAE={mae_now*100:.2f}%<1% 亏损={pnl_abs*100:.2f}%<2%,"
                        f"给额外{_extra_buf_min}min缓冲({hold_min:.0f}/{_soft_limit+_extra_buf_min}min)"
                    )
                    return  # 本轮跳过,下次巡检再判断
                logger.info(f"[pm] {symbol} 时间止损(亏损超时 MAE={mae_now*100:.2f}%)")
                await self._force_close(pos, "time_stop_loss")
                return
            # ── 功能四:时间止损改为部分平仓(v3.1)──────────────────────────
            # 8H + 浮盈 0 ~ ATR×2.0 → 平40%,止损收紧到入场价(保本)
            # 8H + 浮盈 > ATR×2.0 → 不触发,继续持有
            # 已触发过部分止损 → 不重复触发
            atr = float(pos.get("atr_at_entry", 0))
            if atr <= 0:
                logger.warning(
                    f"[pm] {symbol} _check_time_stop: atr_at_entry=0,"
                    f"时间止损部分平仓逻辑静默跳过,请确认 _write_position 已正确写入 atr"
                )
                return  # atr=0 时无法计算 profit_atr,主动退出避免依赖偶然正确的 else 0
            atr_ratio = atr / entry if entry > 0 else 0
            profit_atr = pnl_pct / atr_ratio if atr_ratio > 0 else 0

            if not pos.get("partial_time_stop_triggered"):
                if 0 < profit_atr < 2.0:
                    total_qty   = float(pos.get("quantity", 0))
                    partial_qty = round(total_qty * 0.4, 8)
                    if partial_qty > 0:
                        close_side = "SELL" if direction == "LONG" else "BUY"
                        try:
                            await asyncio.get_running_loop().run_in_executor(
                                None, lambda: api.place_order(
                                    symbol=symbol, side=close_side,
                                    order_type="MARKET",
                                    quantity=partial_qty,
                                    position_side=direction,
                                )
                            )
                            remaining_qty = round(total_qty - partial_qty, 8)
                            partial_pnl   = round(pnl_pct * float(pos.get("pos_usdt", 0)) * 0.4, 2)
                            (await asyncio.to_thread(update_position, pid, {
                                "quantity":                    remaining_qty,
                                "pos_usdt":                    round(float(pos.get("pos_usdt", 0)) * 0.6, 2),
                                "sl_price":                    entry,
                                "partial_time_stop_triggered": True,
                                "quantity_remaining":          remaining_qty,
                            }))
                            logger.info(
                                f"[pm] {symbol} 时间止损部分平仓 40%({partial_qty})"
                                f" profit_atr={profit_atr:.2f} 剩余{remaining_qty}"
                            )
                            # 同步移动交易所 Algo SL 到入场价(保本),避免本地与交易所止损价不一致
                            try:
                                _pos_upd = dict(pos)
                                _pos_upd["sl_price"] = entry
                                _pos_upd["quantity"] = remaining_qty
                                await _execute_trail_sl(_pos_upd, entry)
                                logger.info(f"[pm] {symbol} 时间止损: Algo SL 已移至入场价 {entry:.6g}")
                            except Exception as _trail_e:
                                logger.warning(f"[pm] {symbol} 时间止损: 移动 Algo SL 失败: {_trail_e}")
                            await _notify_async(
                                f"\u23f0 {symbol} 时间止损部分平仓\n"
                                f"平仓 40%({partial_qty}) PnL~{partial_pnl:+.2f}U\n"
                                f"止损收紧到入场价 {entry:.6g}(保本)\n"
                                f"剩余60%仓位继续持有"
                            )
                        except Exception as _pe:
                            logger.error(f"[pm] {symbol} 部分平仓失败: {_pe}")
                    return
                elif profit_atr >= 2.0:
                    return  # 浮盈足够,不触发时间止损
                # else: profit_atr <= 0 且 pnl_pct > 0 只在 atr=0 时发生,已被上方拦截,死代码已删除
            # 已触发过部分平仓:不重复触发
        # ───────────────────────────────────────────────────────────────────────

    # ── B0.5. REST 轮询确认 TP1/TP2 成交(WS不透传时保底)─────────────────────
    async def _check_tp_filled_rest(self, pos: dict):
        """
        每5分钟巡检时,通过 REST 查询 TP1/TP2 挂单是否已成交。
        解决 fstream WS 代理不透传导致 _order_event_loop 无法收到成交通知的问题。
        """
        pid       = pos.get("pos_id", pos.get("symbol", "unknown"))
        symbol    = pos["symbol"]
        tp1_id    = pos.get("tp1_order_id")
        tp2_id    = pos.get("tp2_order_id")
        tp1_filled = pos.get("tp1_filled", False)
        loop      = asyncio.get_running_loop()

        # ── 检查 TP1 ──────────────────────────────────────────────────────────
        if tp1_id and not tp1_filled:
            try:
                od = await loop.run_in_executor(
                    None, lambda: api.get_order(symbol, int(tp1_id))
                )
                if od and od.get("status") == "FILLED":
                    # 问题四修复:用 tp2_quantity(开仓时已计算)而非总量×0.4
                    tp2_qty = float(
                        pos.get("tp2_quantity") or
                        pos.get("quantity_remaining", pos.get("quantity", 0)) * 0.4
                    )
                    (await asyncio.to_thread(update_position, pid, {
                        "tp1_filled":              True,
                        "quantity_remaining":       tp2_qty,
                        "trailing_stop_activated": True,
                    }))
                    fill_price = float(od.get("avgPrice") or od.get("price", 0))
                    logger.info(f"[pm] {symbol} REST确认 TP1成交 price={fill_price:.6g},补挂SL+启动移动止损")
                    # v3.56: REST路径同步补挂剩余40%仓位的SL(与WS路径_handle_order_filled对称)
                    _tp2_qty_r = float(
                        pos.get("tp2_quantity") or
                        pos.get("quantity_remaining", pos.get("quantity", 0)) * 0.4
                    )
                    _sl_price_r = float(pos.get("sl_price", 0))
                    if _tp2_qty_r > 0 and _sl_price_r > 0:
                        try:
                            _prec_r   = (await asyncio.to_thread(_get_precision, symbol))
                            _sl_qty_r = _round_qty(_tp2_qty_r, _prec_r["step"])
                            _sl_p_r   = _round_price(_sl_price_r, _prec_r["tick"])
                            _dir_r    = pos.get("direction", "LONG").upper()
                            _resp_r   = await loop.run_in_executor(
                                None, lambda: api.place_sl(
                                    symbol=symbol,
                                    side="SELL" if _dir_r == "LONG" else "BUY",
                                    position_side=_dir_r,
                                    trigger_price=_sl_p_r,
                                    quantity=_sl_qty_r,
                                )
                            )
                            _new_sl_id_r = _resp_r.get("orderId") or _resp_r.get("algoId")
                            if _new_sl_id_r:
                                _old_sl_id_r = pos.get("sl_order_id")
                                if _old_sl_id_r:
                                    try:
                                        await loop.run_in_executor(
                                            None, lambda: api.cancel_algo_order(symbol, _old_sl_id_r)
                                        )
                                    except Exception:
                                        pass
                                (await asyncio.to_thread(update_position, pid, {"sl_order_id": int(_new_sl_id_r)}))
                                logger.info(f"[pm] {symbol} REST路径TP1后补挂SL成功 id={_new_sl_id_r}")
                        except Exception as _rsl_e:
                            logger.warning(f"[pm] {symbol} REST路径TP1后补挂SL失败: {_rsl_e}")
                    # TP1 成交：追加到卡片
                    _pos_tp1r  = next((p for p in (await asyncio.to_thread(get_all_positions)) if p.get("symbol")==symbol), {})
                    _sl_new_p  = float(_pos_tp1r.get("sl_price", 0))
                    _tp1_sid_r = _pos_tp1r.get("_sig_id", "")
                    _tp1_line  = fmt_node_tp1(fill_price, _sl_new_p)
                    if _tp1_sid_r:
                        await card_append_async(_tp1_sid_r, _tp1_line)
                    else:
                        await _notify_async(_tp1_line)
                    tp1_filled = True
            except Exception as e:
                logger.debug(f"[pm] {symbol} TP1 REST查询失败: {e}")

        # ── 检查 TP2 ──────────────────────────────────────────────────────────
        if tp2_id and tp1_filled:
            try:
                od = await loop.run_in_executor(
                    None, lambda: api.get_order(symbol, int(tp2_id))
                )
                if od and od.get("status") == "FILLED":
                    fill_price = float(od.get("avgPrice") or od.get("price", 0))
                    fill_qty   = float(od.get("executedQty", 0))
                    logger.info(f"[pm] {symbol} REST确认 TP2成交 price={fill_price:.6g}")
                    await _close_position_post_static(
                        pid, symbol, "tp2_rest", od,
                        fill_price=fill_price, fill_qty=fill_qty
                    )
            except Exception as e:
                logger.debug(f"[pm] {symbol} TP2 REST查询失败: {e}")

    # ── B2. 突破失败主动止损 ──────────────────────────────────────────────────
    async def _check_breakout_fail(self, pos: dict):
        symbol    = pos["symbol"]
        direction = pos["direction"].upper()
        # v3.6: 改用 fill_time(实际成交时间)而非 open_time(挂单时间)
        # GTX/挂单模式下 open_time 是挂单时间,实际成交可能晚几分钟甚至几小时,
        # 用 open_time 会导致成交后检查窗口过短
        fill_ts   = int(pos.get("fill_time", pos.get("open_time", 0)))
        hold_min  = (time.time() * 1000 - fill_ts) / 60000

        if hold_min > PM_BREAKOUT_FAIL_WINDOW:
            return  # 超过30分钟,停止检查

        ref = float(pos.get("breakout_ref_price") or 0)
        if ref <= 0:
            return

        price = _last_price.get(symbol)
        if price is None:
            return

        # v3.64: breakout_fail 改为 ATR 自适应(原固定回到 ref 就算失败)
        # 低波动币 small pullback 就触发导致误杀,高波动币正常回踩也会被误判
        # 新:回撤超过 1.5×ATR 才判失败;无 ATR 时降级到原逻辑
        _atr_entry = float(pos.get("atr_at_entry", 0))
        if _atr_entry > 0:
            _fail_dist = 1.5 * _atr_entry
            fail = (direction == "LONG" and price < ref - _fail_dist) or \
                   (direction == "SHORT" and price > ref + _fail_dist)
            _fail_label = f"ref±{_fail_dist:.4g}(1.5×ATR)"
        else:
            fail = (direction == "LONG" and price < ref) or \
                   (direction == "SHORT" and price > ref)
            _fail_label = "ref(无ATR降级)"
        if fail:
            logger.info(f"[pm] {symbol} 突破失败(price={price:.6g} ref={ref:.6g} {_fail_label})")
            await self._force_close(pos, "breakout_fail")

    # ── B2.5 反向信号主动平仓(exit_signal)─────────────────────────────────────
    async def _check_exit_signal(self, pos: dict):
        """持仓中反向信号主动平仓。

        触发条件(全部满足):
          做空持仓: RSI1H < EXIT_SIGNAL_RSI_LONG_THRESH + 15M 止跌形态
          做多持仓: RSI1H > EXIT_SIGNAL_RSI_SHORT_THRESH + 15M 止涨形态
          浮盈 >= TP1 距离 * EXIT_SIGNAL_MIN_PROFIT_RATIO
          持仓时间 >= EXIT_SIGNAL_MIN_HOLD_MIN 分钟
        平仓后:不触发冷却期,让 scanner 下一轮自然扫描反向信号。
        """
        if not EXIT_SIGNAL_ENABLED:
            return
        if pos.get("sig_type") == "funding_arb":
            return

        symbol    = pos["symbol"]
        direction = pos["direction"].upper()
        open_ms   = int(pos.get("fill_time", pos.get("open_time", 0)))
        hold_min  = (time.time() * 1000 - open_ms) / 60000

        # 持仓时间门槛
        if hold_min < EXIT_SIGNAL_MIN_HOLD_MIN:
            return

        try:
            # 拉取最新指标
            now_ms = time.time() * 1000
            k1h = _pm_klines(symbol, "1h", 30)
            k1h = [k for k in k1h if float(k[6]) < now_ms]
            k15 = _pm_klines(symbol, "15m", 10)
            k15 = [k for k in k15 if float(k[6]) < now_ms]

            if len(k1h) < 15 or len(k15) < 3:
                return

            closes_1h = [float(k[4]) for k in k1h]
            rsi_1h    = _calc_rsi(closes_1h)
        except Exception as e:
            logger.debug(f"[pm] {symbol} _check_exit_signal 数据拉取失败: {e}")
            return

        # 判断 RSI 门槛 + 15M 形态
        from shared.judges import _reversal_signal_15m
        if direction == "SHORT":
            # 做空持仓,检查多头反转信号
            if rsi_1h >= EXIT_SIGNAL_RSI_LONG_THRESH:
                return
            rev_sig = _reversal_signal_15m(k15, "LONG")
            if not rev_sig:
                return
        else:
            # 做多持仓,检查空头反转信号
            if rsi_1h <= EXIT_SIGNAL_RSI_SHORT_THRESH:
                return
            rev_sig = _reversal_signal_15m(k15, "SHORT")
            if not rev_sig:
                return

        # 检查浮盈和 TP1 距离
        entry   = float(pos.get("entry_price", 0))
        tp1     = float(pos.get("tp1_price", 0))
        price   = _last_price.get(symbol)
        if not entry or not tp1 or not price:
            return

        tp1_dist = abs(tp1 - entry)
        if tp1_dist < 1e-9:
            return

        profit = (entry - price) if direction == "SHORT" else (price - entry)

        if profit < 0:
            return  # 不允许亏损时主动平仓

        if profit < tp1_dist * EXIT_SIGNAL_MIN_PROFIT_RATIO:
            return

        logger.info(
            f"[pm] {symbol} EXIT_SIGNAL 触发 dir={direction} "
            f"rsi1h={rsi_1h:.1f} rev={rev_sig} "
            f"profit={profit/entry*100:.2f}% tp1_ratio={profit/tp1_dist:.2f}"
        )
        await self._force_close(pos, "exit_signal_reversal")

    # ── B3. 健康检查(每30分钟) ──────────────────────────────────────────────
    async def _check_health(self, pos: dict):
        symbol    = pos["symbol"]
        pid       = pos.get("pos_id", pos.get("symbol","unknown"))
        direction = pos["direction"].upper()
        open_ms   = int(pos.get("open_time", 0))
        hold_min  = (time.time() * 1000 - open_ms) / 60000

        try:
            # 1H K线
            k1h = _pm_klines(symbol, "1h", 55)
            now_ms = time.time() * 1000
            k1h = [k for k in k1h if float(k[6]) < now_ms]
            closes_1h = [float(k[4]) for k in k1h]

            # 4H K线
            k4h = _pm_klines(symbol, "4h", 55)
            k4h = [k for k in k4h if float(k[6]) < now_ms]
            closes_4h = [float(k[4]) for k in k4h]

            ema20_1h = _calc_ema(closes_1h, 20)
            ema50_1h = _calc_ema(closes_1h, 50)
            ema20_4h = _calc_ema(closes_4h, 20)
            ema50_4h = _calc_ema(closes_4h, 50)

            # 资金费率
            md      = (await asyncio.to_thread(_pm_mark_price, symbol))
            funding = float(md.get("lastFundingRate", 0))
            price   = float(md.get("markPrice", pos["entry_price"]))

            # RSI 1H
            rsi_1h = _calc_rsi(closes_1h)

        except Exception as e:
            logger.warning(f"[pm] {symbol} 健康检查数据拉取失败: {e}")
            return

        # 评分(reversal 模式跳过 EMA 尺度:reversal 的前提就是 EMA 逆势,评分相应调整)
        mode      = pos.get("mode", "pullback")
        score = _health_score(direction, ema20_1h, ema50_1h, ema20_4h, ema50_4h, rsi_1h, funding,
                              mode=mode)
        entry = float(pos["entry_price"])
        pnl_pct = ((price - entry) / entry * 100) if direction == "LONG" \
                  else ((entry - price) / entry * 100)

        if score < PM_HEALTH_FORCE_CLOSE_SCORE and hold_min >= PM_MIN_HOLD_BEFORE_HEALTH:
            # ── v3.68 改动三:健康检查按持仓时间动态调整强平门槛 ──────────────────
            # 早期(<2H):放宽一倍,容忍短期波动
            # 中期(2-6H):标准门槛
            # 晚期(>6H):收紧 50%,更敏感地响应健康下降
            # PM_HEALTH_*:已在模块顶部导入
            holding_hours = hold_min / 60
            if holding_hours < PM_HEALTH_EARLY_HOURS:
                threshold_mult = PM_HEALTH_EARLY_MULT
            elif holding_hours < PM_HEALTH_LATE_HOURS:
                threshold_mult = PM_HEALTH_MID_MULT
            else:
                threshold_mult = PM_HEALTH_LATE_MULT

            effective_loss_threshold = PM_HEALTH_FORCE_CLOSE_LOSS_PCT * threshold_mult

            # 双条件:评分偏低 且 浮亏超过动态阈值,才强制平仓;单独评分低只告警
            if pnl_pct <= effective_loss_threshold:
                logger.info(
                    f"[pm] {symbol} 健康评分 {score}/100 且浮亏{pnl_pct:.2f}% "
                    f"(持仓{holding_hours:.1f}H, 门槛×{threshold_mult}, "
                    f"有效阈值={effective_loss_threshold:.2f}%),触发强制平仓"
                )
                await self._force_close(pos, "health_force_close")
                return
            else:
                logger.info(f"[pm] {symbol} 健康评分 {score}/100 低但浮盈{pnl_pct:.2f}%,仅告警不平仓")

        if score < PM_HEALTH_WARN_SCORE:
            # 告警节流:内存 + 持久化到 pos_state(重启不丢失),每30分钟最多一次
            last_warn = max(
                _health_warned.get(pid, 0),
                float(pos.get("health_last_warn_ts", 0))
            )
            if time.time() - last_warn >= 1800:
                _health_warned[pid] = time.time()
                (await asyncio.to_thread(update_position, pid, {"health_last_warn_ts": time.time()}))
                dim = _health_score_detail(direction, ema20_1h, ema50_1h,
                                            ema20_4h, ema50_4h, rsi_1h, funding,
                                            mode=mode)
                await _notify_async(
                    f"⚠️ {symbol} 健康评分 {score}/100"
                    + (f"(reversal模式,EMA逆势正常)" if mode == "reversal" else "")
                    + f"\n方向: {direction}  浮盈: {pnl_pct:+.2f}%\n"
                    f"EMA: {dim['ema']}分  RSI: {dim['rsi']}分  资金费率: {dim['fr']}分"
                )


def _health_score(direction: str,
                   ema20_1h: float, ema50_1h: float,
                   ema20_4h: float, ema50_4h: float,
                   rsi_1h: float, funding: float,
                   mode: str = "pullback") -> int:
    d = _health_score_detail(direction, ema20_1h, ema50_1h, ema20_4h, ema50_4h, rsi_1h, funding,
                             mode=mode)
    return d["ema"] + d["rsi"] + d["fr"]


def _health_score_detail(direction: str,
                          ema20_1h: float, ema50_1h: float,
                          ema20_4h: float, ema50_4h: float,
                          rsi_1h: float, funding: float,
                          mode: str = "pullback") -> dict:
    # reversal 模式的前提就是 EMA 逆势反弹,EMA 项直接满分,不用判断结构
    if mode == "reversal":
        ema_score = 40
    else:
        ema_score = 0
        if direction == "LONG":
            if ema20_1h > ema50_1h: ema_score += 20
            if ema20_4h > ema50_4h: ema_score += 20
        else:
            if ema20_1h < ema50_1h: ema_score += 20
            if ema20_4h < ema50_4h: ema_score += 20

    if direction == "LONG":
        # RSI:超卖(<30)和正常(30~70)对多头均满分
        if rsi_1h <= 70:
            rsi_score = 30
        elif rsi_1h <= 80:
            rsi_score = 15
        else:
            rsi_score = 0
        # 资金费率
        if -0.001 <= funding <= 0.001:
            fr_score = 30
        elif funding <= 0.002:
            fr_score = 15
        else:
            fr_score = 0
    else:  # SHORT
        # v3.6: 做空时 RSI>70(超买)对空头有利;30~70 是中性区间,不该给满分
        # 改为:>70 满分,>50 中性给半分,<=50 价格已走弱对空头不利给 0
        if rsi_1h > 70:
            rsi_score = 30
        elif rsi_1h > 50:
            rsi_score = 15
        else:
            rsi_score = 0
        if -0.001 <= funding <= 0.001:
            fr_score = 30
        elif funding >= -0.002:
            fr_score = 15
        else:
            fr_score = 0

    return {"ema": ema_score, "rsi": rsi_score, "fr": fr_score}


    # ── B4. 挂单有效期检查 ────────────────────────────────────────────────────
async def _recover_partial_fill(pos: dict, od: dict, pid: str, symbol: str) -> bool:
    """[fix #14] 挂单被撤/过期时若已有部分成交（executedQty>0），不能直接删本地记录：
    交易所里已经有真实持仓，按"孤儿仓位"处理——用实际成交量/均价补挂保护单并保留记录。
    返回 True 表示已按部分成交接管（调用方不得再删记录）。"""
    try:
        exec_qty = float((od or {}).get("executedQty", 0) or 0)
    except (TypeError, ValueError):
        exec_qty = 0.0
    if exec_qty <= 0:
        return False
    try:
        fill_price = float((od or {}).get("avgPrice") or (od or {}).get("price") or 0)
    except (TypeError, ValueError):
        fill_price = 0.0
    if fill_price <= 0:
        fill_price = float(pos.get("entry_price") or 0)
    logger.warning(f"[pm] {symbol} 挂单已{(od or {}).get('status')}但部分成交 executedQty={exec_qty} "
                   f"avg={fill_price}，按孤儿仓位处理：保留记录并补挂保护单")
    (await asyncio.to_thread(update_position, pid, {"status": "patching", "quantity": exec_qty, "quantity_remaining": exec_qty,
                          "fill_price": fill_price, "entry_price": fill_price or pos.get("entry_price"),
                          "partial_fill": True}))
    mark_system_order_update(symbol)
    try:
        await _补挂保护单(pos, fill_price, exec_qty, pid, symbol)
    except Exception as ge:
        (await asyncio.to_thread(update_position, pid, {"status": "pending_guard", "missing_guards": ["sl", "tp1"]}))
        logger.error(f"[pm] {symbol} 部分成交补挂保护单失败，置 pending_guard: {ge}")
        await _notify_async(f"⚠️ {symbol} 挂单部分成交({exec_qty})后补挂保护单失败，已标记 pending_guard，patrol 将重试\n错误:{ge}")
    return True


async def _check_pending_expire_static(pos: dict, now_ms: int):
    pid      = pos.get("pos_id", pos.get("symbol","unknown"))
    symbol   = pos["symbol"]
    mode     = pos.get("mode", "pullback")
    open_ms  = int(pos.get("open_time", 0))
    hold_min = (now_ms - open_ms) / 60000
    order_id = pos.get("open_order_id")

    # 已在补挂中,跳过--防止重复触发
    if pos.get("status") == "patching":
        logger.debug(f"[pm] {symbol} status=patching,跳过 pending 检查")
        return

    # ── 实时核验:检查订单是否已在交易所消失(取消/过期)─────────────────────
    if order_id:
        try:
            live_orders = (await asyncio.to_thread(api.get_open_orders, symbol))
            live_ids = {o["orderId"] for o in (live_orders or [])}
            if int(order_id) not in live_ids:
                # 订单不在交易所 → 用已封装的 api.get_order 查询订单状态(不重复造轮)
                try:
                    od = (await asyncio.to_thread(api.get_order, symbol, int(order_id)))
                except Exception as _oe:
                    od = None
                    logger.warning(f"[pm] {symbol} 查询订单 {order_id} 失败: {_oe}")
                if od is not None:
                    status = od.get("status","")
                    if status in ("CANCELED","EXPIRED","REJECTED"):
                        if await _recover_partial_fill(pos, od, pid, symbol):   # [fix #14]
                            return
                        (await asyncio.to_thread(_remove_position_checked, pid))
                        logger.info(f"[pm] {symbol} pending订单 {order_id} 已{status},清除本地记录")
                        return
                    elif status == "FILLED":
                        # 已成交但 pos_state 仍是 pending → 补挂止损止盈并升级状态
                        fill_price = float(od.get("avgPrice") or od.get("price", 0))
                        fill_qty   = float(od.get("executedQty", 0))
                        logger.warning(
                            f"[pm] {symbol} 订单 {order_id} 已FILLED"
                            f"(price={fill_price} qty={fill_qty})但状态未更新,开始补挂止损止盈"
                        )
                        # 先把状态标记为 patching,防止下一轮 patrol 重复进入
                        (await asyncio.to_thread(update_position, pid, {"status": "patching"}))
                        mark_system_order_update(symbol)  # 补挂期间禁止手动平仓误判
                        try:
                            await _补挂保护单(pos, fill_price, fill_qty, pid, symbol)
                        except Exception as ge:
                            # 补挂失败→状态回滚到 pending,下轮再试
                            (await asyncio.to_thread(update_position, pid, {"status": "pending"}))
                            logger.error(f"[pm] {symbol} 补挂保护单失败: {ge}")
                            await _notify_async(f"⚠️ {symbol} 已成交但补挂止损失败,请人工介入!\n错误:{ge}")
                        return
                # [fix #46] 查询失败时 od 为 None：保留本地记录，下轮 patrol 重试（删除原 24h 清除死代码）
        except Exception as e:
            logger.warning(f"[pm] {symbol} 核验pending订单失败: {e}")

    expire_min = ORDER_EXPIRE_BREAKOUT_MIN if mode == "breakout" else ORDER_EXPIRE_PULLBACK_MIN

    if hold_min < expire_min:
        return

    if not order_id:
        (await asyncio.to_thread(_remove_position_checked, pid))
        return

    for attempt in range(2):
        try:
            (await asyncio.to_thread(api.cancel_order, symbol, int(order_id)))
            # [fix #14] 撤单后复查成交量：撤单前可能已部分成交，不能直接删记录
            try:
                _od_after = (await asyncio.to_thread(api.get_order, symbol, int(order_id)))
            except Exception as _qe:
                _od_after = None
                logger.warning(f"[pm] {symbol} 撤单后查询订单失败: {_qe}")
            if _od_after and await _recover_partial_fill(pos, _od_after, pid, symbol):
                return
            break
        except Exception as e:
            err_str = str(e).lower()
            # GTD 单可能已被交易所自动撤销(订单不存在),静默处理即可
            if any(kw in err_str for kw in ("unknown order", "order does not exist", "-2011", "-2013")):
                try:
                    _od_gone = (await asyncio.to_thread(api.get_order, symbol, int(order_id)))
                except Exception:
                    _od_gone = None
                if _od_gone and await _recover_partial_fill(pos, _od_gone, pid, symbol):   # [fix #14]
                    return
                logger.info(f"[pm] {symbol} 挂单 {order_id} 已不存在(可能 GTD 已自动到期),直接清除本地记录")
                break
            if attempt == 0:
                await asyncio.sleep(0.5)
            else:
                await _notify_async(f"⚠️ {symbol} 挂单 {order_id} 超时撤单失败,请人工处理。\n错误:{e}")
                return

    (await asyncio.to_thread(_remove_position_checked, pid))
    logger.info(f"[pm] {symbol} 挂单超时({hold_min:.0f}min)已撤销")


    # ── 平仓执行(健康/时间止损/突破失败)────────────────────────────────────
def _pos_still_open(pos: dict) -> bool:
    """[v3.87 STEP7-2] 按 pos_id(没有则按 symbol)从 state 取最新记录,仅当 status == 'open' 返回 True。"""
    try:
        pid = pos.get("pos_id")
        sym = pos.get("symbol")
        for p in get_all_positions():
            if (pid and p.get("pos_id") == pid) or (not pid and not p.get("pos_id") and p.get("symbol") == sym):
                return p.get("status") == "open"
        return False
    except Exception as e:
        # 读状态失败时不能因此放弃平仓(宁可重复,也不能漏平),按「仍然 open」处理
        logger.warning(f"[pm] 强平前重读持仓状态失败,按仍然 open 处理: {e}")
        return True


async def _force_close_static(pos: dict, close_reason: str):
    # v3.56: 幂等保护--同一 pos_id 同时只允许一条路径执行平仓
    # 场景:WS SL触发 和 patrol时间止损 极端情况下并发调用同一仓位
    _pid_fc = pos.get("pos_id", pos.get("symbol", "unknown"))
    if _pid_fc in _closing_set:
        logger.info(f"[pm] {pos.get('symbol')} _force_close 已在执行中,跳过重复调用({close_reason})")
        return
    _closing_set.add(_pid_fc)
    try:
        # [v3.87 STEP7-2] 第二道防线:重新从 state 读取该仓位的最新状态。
        #   _closing_set 只防「同时执行」,防不住「先后各执行一次」:PositionManager._patrol_loop 与 main 里的 patrol_once
        #   都每 5 分钟跑时间止损 / 突破失败 / 反向信号检查,第一个平仓完成后,第二个拿着旧快照仍可能再发一次平仓。
        #   读不到(已被删除)或 status 不是 open → 直接返回,不做任何平仓操作。
        if not _pos_still_open(pos):
            logger.debug(f"[pm] {pos.get('symbol')} 持仓已不存在或已非 open,跳过重复平仓({close_reason})")
            return
        await _force_close_static_inner(pos, close_reason)
    finally:
        _closing_set.discard(_pid_fc)

async def _force_close_static_inner(pos: dict, close_reason: str):
    symbol    = pos["symbol"]
    direction = pos["direction"].upper()
    qty       = float(pos.get("quantity_remaining", pos.get("quantity", 0)))
    sl_id     = pos.get("sl_order_id")
    tp1_id    = pos.get("tp1_order_id")
    tp2_id    = pos.get("tp2_order_id")
    tp1_filled = pos.get("tp1_filled", False)
    tp2_filled = pos.get("tp2_filled", False)
    pid       = pos.get("pos_id", pos.get("symbol","unknown"))

    # 步骤1:撤所有未成交保护单
    # SL 是 algo 单,用 cancel_algo_order;TP 是普通限价单,用 cancel_order
    async def _try_cancel_sl(oid):
        if not oid: return
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.cancel_algo_order(symbol, int(oid))
            )
            logger.info(f"[pm] {symbol} 已撤 SL algoId={oid}")
        except Exception as e:
            logger.warning(f"[pm] {symbol} 撤 SL 失败: {e}")

    async def _try_cancel_tp(oid):
        if not oid: return
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.cancel_order(symbol, int(oid))
            )
            logger.info(f"[pm] {symbol} 已撤 TP orderId={oid}")
        except Exception as e:
            logger.warning(f"[pm] {symbol} 撤 TP 失败: {e}")

    await asyncio.gather(
        _try_cancel_sl(sl_id),
        _try_cancel_tp(tp1_id) if not tp1_filled else asyncio.sleep(0),
        _try_cancel_tp(tp2_id) if not tp2_filled else asyncio.sleep(0),
    )

    # 步骤1.5:清理该标的所有残余挂单(防止漏撤)
    try:
        # 清理残余 algo 单
        algo_orders = await asyncio.get_running_loop().run_in_executor(
            None, lambda: api.get_algo_open_orders(symbol)
        )
        for ao in (algo_orders or []):
            aid = int(ao.get("algoId", 0))
            if aid:
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None, lambda _aid=aid: api.cancel_algo_order(symbol, _aid)
                    )
                    logger.info(f"[pm] {symbol} 清理残余 algo 单 algoId={aid}")
                except Exception:
                    pass
        # 清理残余普通挂单（[fix #2] 查询失败时跳过清理，不当作"无挂单"）
        try:
            open_orders = await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.get_open_orders(symbol)
            )
        except Exception as _qe:
            logger.warning(f"[pm] {symbol} 查询残余挂单失败，跳过清理: {_qe}")
            open_orders = []
        for oo in (open_orders or []):
            oid = int(oo.get("orderId", 0))
            if oid:
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None, lambda _oid=oid: api.cancel_order(symbol, _oid)
                    )
                    logger.info(f"[pm] {symbol} 清理残余挂单 orderId={oid}")
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"[pm] {symbol} 清理残余挂单失败: {e}")

    # 步骤2:市价平仓
    close_side = "SELL" if direction == "LONG" else "BUY"
    fill_price = float(pos["entry_price"])  # fallback
    try:
        resp = (await asyncio.to_thread(api.place_order, symbol=symbol, side=close_side,
            order_type="MARKET",
            quantity=qty,
            # [fix #26] 对冲模式平仓单必须显式带 positionSide（原先已带，这里加校验）
            position_side=("SHORT" if direction.upper() == "SHORT" else "LONG"),))
        order_id = resp.get("orderId") or resp.get("algoId")
        # 尝试从订单历史得到实际成交均价
        fill_ok = False
        if order_id:
            try:
                await asyncio.sleep(1.0)  # 等实际成交
                order_info = (await asyncio.to_thread(api.get_order, symbol, order_id))
                avg_price  = float(order_info.get("avgPrice") or 0)
                if avg_price > 0:
                    fill_price = avg_price
                    fill_ok = True
            except Exception:
                pass
        # fallback: 愉试从 mark_price 获得近似出场价
        if not fill_ok:
            try:
                mp = (await asyncio.to_thread(api.get_mark_price, symbol))
                mp_price = float(mp.get("markPrice") or 0)
                if mp_price > 0:
                    fill_price = mp_price
            except Exception:
                pass
    except Exception as e:
        logger.error(f"[pm] {symbol} 市价平仓失败: {e}")
        await _notify_async(f"⚠️ {symbol} 强制平仓失败!请人工处理。\n原因: {close_reason}\n错误: {e}")
        return

    # 步骤3:平仓后处理
    # [Fix 2] SL 穿透检测(v3.48):time_stop 出场时如果市价已穿透 SL 价格,记录穿透量供分析
    # 实盘中 Algo SL 会先一步出场,此处主要是给回测加监控点
    _sl_price_ref = float(pos.get("sl_price", 0))
    if _sl_price_ref > 0 and close_reason in (
        "time_stop_no_progress", "time_stop_loss",
        "time_stop_hard", "time_stop_hard_72h", "early_stagnation",
    ):
        if direction == "LONG" and fill_price < _sl_price_ref:
            _breach = round(_sl_price_ref - fill_price, 8)
            logger.info(
                f"[pm] {symbol} [Fix2] SL穿透: time_stop出场价{fill_price:.6g}"
                f" < SL{_sl_price_ref:.6g},穿透={_breach:.6g}"
            )
            pos["_sl_breach_detected"] = True
            pos["_sl_breach_gap"]      = _breach
        elif direction == "SHORT" and fill_price > _sl_price_ref:
            _breach = round(fill_price - _sl_price_ref, 8)
            logger.info(
                f"[pm] {symbol} [Fix2] SL穿透: time_stop出场价{fill_price:.6g}"
                f" > SL{_sl_price_ref:.6g},穿透={_breach:.6g}"
            )
            pos["_sl_breach_detected"] = True
            pos["_sl_breach_gap"]      = _breach

    fake_event = {"rp": 0, "L": fill_price}
    await _close_position_post_static(pid, symbol, close_reason, fake_event,
                                       fill_price=fill_price, fill_qty=qty)


# ─────────────────────────────────────────────────────────────────────────────
# 平仓后处理(七步)
# ─────────────────────────────────────────────────────────────────────────────
def _count_recent_sl(symbol: str, days: int = 7) -> tuple[int, list]:
    """功能二:统计 symbol 在过去 days 天内的止损次数"""
    import time as _t
    cutoff_ms = (_t.time() - days * 86400) * 1000
    records = []
    try:
        if not TRADE_LOG_PATH.exists():
            return 0, []
        with open(TRADE_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if (rec.get("symbol") == symbol
                        and rec.get("close_reason") == "sl"
                        and float(rec.get("close_time", 0)) >= cutoff_ms):
                    try:
                        import datetime as _dt
                        ct = int(rec.get("close_time", 0))
                        rec["close_time_str"] = _dt.datetime.utcfromtimestamp(ct/1000).strftime("%m-%d %H:%M")
                    except Exception:
                        rec["close_time_str"] = "?"
                    records.append(rec)
    except Exception:
        pass
    return len(records), records


def _fetch_actual_fees(symbol: str, since_ms: int = 0, notional: float = 0.0) -> float:
    """[fix #44] 从交易所成交回报（userTrades.commission）取本仓位实际手续费合计（USDT）。
    取不到时按名义价值 × 双边费率估算。"""
    try:
        from shared.binance_api import get_user_trades as _gut
        _t = _gut(symbol, start_time=int(since_ms) if since_ms else None, limit=100) or []
        _tot = 0.0
        for _x in _t:
            try:
                _tot += abs(float(_x.get("commission", 0) or 0))
            except (TypeError, ValueError):
                continue
        if _tot > 0:
            return round(_tot, 6)
    except Exception as _e:
        logger.warning(f"[pm] {symbol} 查询实际手续费失败，改用估算: {_e}")
    try:
        from config import TAKER_FEE_RATE as _tfr
    except Exception:
        _tfr = 0.0005
    return round(abs(float(notional or 0.0)) * float(_tfr) * 2, 6)


async def _close_position_post_static(
    pos_id: str, symbol: str, close_reason: str, event: dict,
    fill_price: float = 0, fill_qty: float = 0,
) -> None:
    pos = None
    for p in (await asyncio.to_thread(get_all_positions)):
        if p.get("pos_id") == pos_id:
            pos = p; break
    if pos is None:
        return

    entry      = float(pos.get("entry_price", fill_price))
    direction  = pos.get("direction", "long").upper()
    quantity   = float(pos.get("quantity", fill_qty))
    leverage   = int(pos.get("leverage", 1))
    open_ms    = int(pos.get("open_time", 0))
    close_ms   = int(time.time() * 1000)
    mode       = pos.get("mode", "pullback")

    # [v3.89 #2] 平仓成交价同样优先用累计均价 ap；仅在 ap 缺失时才退回最后成交价 L（只用于盈亏记录，不用于下单）
    try:
        _ap = float(event.get("ap") or 0)
    except (TypeError, ValueError):
        _ap = 0.0
    exit_price = fill_price or _ap or float(event.get("L", entry))
    if fill_qty <= 0:
        fill_qty = quantity

    nominal = entry * quantity

    # 步骤1:计算盈亏
    if direction == "LONG":
        pnl_usdt = (exit_price - entry) * fill_qty
    else:
        pnl_usdt = (entry - exit_price) * fill_qty
    pnl_pct      = pnl_usdt / (nominal + 1e-9) * 100
    hold_minutes = (close_ms - open_ms) / 60000

    # 步骤2:更新熔断器(v3.16 修复:record_pnl 正确区分盈亏,盈利不再计入 daily_loss)
    # [fix #44] 用交易所成交回报里的实际手续费，并计入总亏损
    _fees = _fetch_actual_fees(symbol, open_ms, nominal)
    try:
        _record_pnl(pnl_usdt, symbol=symbol, fee=_fees)
    except Exception as e:
        logger.error(f"[pm] record_pnl 失败: {e}")

    # 步骤3:设置冷却期(清除持仓前先设置,防止窗口期重新开仓)
    try:
        if close_reason == "sl":
            set_cooldown(symbol, PM_COOLDOWN_SL_HOURS, "sl_trigger")
        elif close_reason == "crash_guard":
            set_cooldown(symbol, PM_COOLDOWN_CRASH_HOURS, "crash_guard")
        # 止盈/时间止损/健康检查/突破失败不设冷却
    except Exception as e:
        logger.error(f"[pm] set_cooldown 失败: {e}")

    # 步骤3b:累计亏损黑名单检查
    # 止损触发后检查过去7天同标的止损次数,≥3次加入7天冷却黑名单
    if close_reason == "sl":
        try:
            _sl_count, _sl_history = _count_recent_sl(symbol, days=7)
            if _sl_count >= 3:
                _blacklist_days = 7
                set_cooldown(symbol, _blacklist_days * 86400, "repeated_stop_loss")
                _hist_str = "  ".join(
                    f"{r.get('close_time_str','?')} {r.get('pnl_usdt',0):+.1f}U"
                    for r in _sl_history[-3:]
                )
                asyncio.ensure_future(_notify_async(
                    f"⛔ 标的加入临时黑名单\n"
                    f"标的:{symbol}\n"
                    f"原因:7天内连续止损{_sl_count}次\n"
                    f"黑名单期限:{_blacklist_days}天\n"
                    f"历史{len(_sl_history)}次止损:{_hist_str}"
                ))
                logger.warning(f"[blacklist] {symbol} 7天止损{_sl_count}次,加入{_blacklist_days}天黑名单")
        except Exception as _be:
            logger.error(f"[pm] 黑名单检查失败: {_be}")


    # 步骤4:清除持仓状态
    try:
        (await asyncio.to_thread(_remove_position_checked, pos_id))
        _peak.pop(pos_id, None)
        _mfe.pop(pos_id, None)
        _mae.pop(pos_id, None)
    except Exception as e:
        logger.error(f"[pm] remove_position 失败: {e}")


    # 步骤5:写入 trade_log.jsonl(持仓清除后再写,保证状态一致)
    try:
        TRADE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "trade_id":          f"{close_ms}_{symbol}",
            "symbol":            symbol,
            "direction":         direction.lower(),
            "mode":              mode,
            "volatility_tier":   pos.get("volatility_tier", "mid"),
            "signal_strength":   pos.get("signal_strength", "medium"),
            "entry_price":       entry,
            "exit_price":        exit_price,
            "quantity":          quantity,
            "leverage":          leverage,
            "nominal_value":     round(nominal, 2),
            "pnl_usdt":          round(pnl_usdt, 4),
            "pnl_pct":           round(pnl_pct, 4),
            "fees_usdt":         round(_fees, 6),              # [fix #44] 实际手续费
            "pnl_net_usdt":      round(pnl_usdt - _fees, 4),   # [fix #44] 净盈亏
            "signal_id":         pos.get("_sig_id", ""),       # [fix #44] 信号↔成交关联
            "holding_minutes":   round(hold_minutes, 1),
            "open_time":         open_ms,
            "close_time":        close_ms,
            "close_reason":      close_reason,
            "tp1_filled":        pos.get("tp1_filled", False),
            "tp2_filled":        pos.get("tp2_filled", False),
            "sl_degraded":       pos.get("sl_degraded", False),
            "liq_data_available": pos.get("liq_data_available", False),
            # 量比记录:用于后续统计低量比 vs 高量比费率差异
            "vol_ratio_at_entry": round(
                float(pos.get("judge_detail", {}).get("vol_ratio", 0) or
                      pos.get("ind", {}).get("vol_ratio", 0) or 0), 4
            ),
            # 事后标注:最大有利/不利偏移(MFE/MAE),用于后续策略质量分析
            "mfe_pct":           round(_mfe.get(pos_id, 0.0) * 100, 4),
            "mae_pct":           round(_mae.get(pos_id, 0.0) * 100, 4),
            # 开仓时市场环境快照
            "btc_trend":         pos.get("market_env", {}).get("btc_trend", ""),
            "market_mode":       pos.get("market_env", {}).get("market_mode", ""),
            "market_volatility": pos.get("market_env", {}).get("market_volatility", 0.0),
            "funding_market":    pos.get("market_env", {}).get("funding_market", 0.0),
            "judge_detail":      pos.get("judge_detail", {}),
            "max_correlation":   round(float(pos.get("max_correlation", 0.0)), 4),
            # [Fix 2] SL 穿透检测字段(v3.48)
            "sl_breach_detected": pos.get("_sl_breach_detected", False),
            "sl_breach_gap":      pos.get("_sl_breach_gap", 0.0),
            # ── 滑点记录(v3.71)──────────────────────────────────────────────
            "entry_slippage_pct": pos.get("entry_slippage_pct"),
            "entry_slippage_u":   pos.get("entry_slippage_u"),
        }
        # 计算出场滑点并写入
        _expected_sl  = float(pos.get("sl_price", 0) or 0)
        _expected_tp1 = float(pos.get("tp1_price", 0) or 0)
        _expected_tp2 = float(pos.get("tp2_price", 0) or 0)
        if close_reason in ("sl", "sl_triggered") and _expected_sl > 0:
            _sl_slip = (exit_price - _expected_sl) / (_expected_sl + 1e-9) * 100
            record["sl_slippage_pct"] = round(_sl_slip, 4)
            # 滑点告警
            try:
                from config import SLIPPAGE_SL_WARN_PCT
                if abs(_sl_slip) > SLIPPAGE_SL_WARN_PCT:
                    asyncio.ensure_future(_notify_async(
                        f"⚠️ {symbol} SL 滑点 {_sl_slip:+.2f}%"
                        f"(预期 {_expected_sl:.6g},实际 {exit_price:.6g})"
                    ))
            except Exception:
                pass
        elif close_reason in ("tp1", "tp1_hit") and _expected_tp1 > 0:
            _tp1_slip = (exit_price - _expected_tp1) / (_expected_tp1 + 1e-9) * 100
            record["tp_slippage_pct"] = round(_tp1_slip, 4)
        elif close_reason in ("tp2", "tp2_hit") and _expected_tp2 > 0:
            _tp2_slip = (exit_price - _expected_tp2) / (_expected_tp2 + 1e-9) * 100
            record["tp_slippage_pct"] = round(_tp2_slip, 4)
        from shared.trade_log_io import append_jsonl as _append_jsonl   # v3.93
        await asyncio.to_thread(_append_jsonl, TRADE_LOG_PATH, record)
    except Exception as e:
        logger.error(f"[pm] trade_log 写入失败: {e}")


    # 步骤5b:sl_quality 止损质量后台标注
    # 止损出场后 2H 内价格是否反弹超过止损幅度 → noise / valid
    if close_reason in ("sl", "sl_triggered"):
        import threading as _thr
        def _sl_quality_check(sym, sl_px, entry_px, direction, log_path, trade_id):
            import time as _t, json as _j
            _t.sleep(2 * 3600)  # 等 2H
            try:
                from shared import binance_api as _bapi
                _klines = _bapi.get_klines(sym, "5m", 24) or []
                if direction.upper() == "LONG":
                    # 做多止损:止损后价格如果涨回 entry,说明是噪音
                    _max_h = max((float(k[2]) for k in _klines), default=sl_px)
                    _sl_q  = "noise" if _max_h >= entry_px else "valid"
                else:
                    # 做空止损:止损后价格如果跌回 entry,说明是噪音
                    _min_l = min((float(k[3]) for k in _klines), default=sl_px)
                    _sl_q  = "noise" if _min_l <= entry_px else "valid"
                # 回写到 trade_log
                # v3.93: 锁内读-改-写 + .tmp/os.replace 原子替换，防止覆盖并发追加的记录
                from shared.trade_log_io import update_jsonl_field as _update_jsonl_field
                _update_jsonl_field(log_path, "trade_id", trade_id, "sl_quality", _sl_q)
            except Exception:   # [v3.87 STEP7-5] 去掉未使用的异常变量 _e
                pass  # 后台任务失败不影响主流程
        _thr.Thread(
            target=_sl_quality_check,
            args=(symbol, float(pos.get("sl_price", exit_price)),
                  entry, direction, str(TRADE_LOG_PATH),
                  f"{close_ms}_{symbol}"),
            daemon=True
        ).start()

    # 步骤6:发送 Jarvis 通知 → 追加到信号卡片
    # fix: 步骤4 已 remove_position，不能再用 get_all_positions 找 pos；
    #      直接用函数顶部读入的 pos 变量，确保 _sig_id / tp1_filled / tp2_filled 正确。
    try:
        _sig_id_cls = pos.get("_sig_id", "")
        _close_line = fmt_node_close(
            close_reason = close_reason,
            exit_price   = exit_price,
            hold_minutes = hold_minutes,
            pnl_usdt     = pnl_usdt,
            pnl_pct      = pnl_pct,
            tp1_filled   = pos.get("tp1_filled", False),
            tp2_filled   = pos.get("tp2_filled", False),
        )
        if _sig_id_cls:
            await card_append_async(_sig_id_cls, _close_line)
            (await asyncio.to_thread(card_remove, _sig_id_cls))
        else:
            await _notify_async(_close_line)
    except Exception as e:
        logger.error(f"[pm] 平仓通知发送失败: {e}")

    logger.info(f"[pm] {symbol} 平仓后处理完成 pnl={pnl_usdt:+.2f}U reason={close_reason}")


# ─────────────────────────────────────────────────────────────────────────────
# 日报生成(UTC 02:00 = 北京时间 10:00)
# ─────────────────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
# TP2 动态收紧(持仓时间自适应,v3.72)
# ═══════════════════════════════════════════════════════════════════════════════
async def _maybe_tighten_tp2(pos: dict) -> None:
    """
    根据持仓时间动态收紧 TP2 目标,防止利润被时间止损带走。

    规则:
      持仓 < 4H  → 不收紧(等大行情)
      持仓 4~8H  → TP2 = 当前浮盈 × 1.5 倍位置
      持仓 > 8H  → TP2 = 当前浮盈 × 1.2 倍位置(快速锁定)

    只在 TP1 未触达 + TP2 未成交 + 浮盈 > 0 时生效。
    try/except 全包,失败不影响主策略。
    """
    try:
        from config import (
            TP2_TIGHTEN_4H_MULT, TP2_TIGHTEN_8H_MULT,
            TP2_TIGHTEN_CHECK_INTERVAL_MIN,
        )
        pid       = pos.get("pos_id", "")
        symbol    = pos.get("symbol", "")
        direction = pos.get("direction", "long").upper()
        entry     = float(pos.get("entry_price", 0) or 0)
        old_tp2   = float(pos.get("tp2_price", 0) or 0)
        open_ms   = int(pos.get("open_time", 0) or 0)
        tp1_filled = pos.get("tp1_filled", False)
        tp2_filled = pos.get("tp2_filled", False)

        if not pid or not symbol or entry <= 0 or old_tp2 <= 0:
            return
        if tp1_filled or tp2_filled:
            return  # TP1/TP2 已触达,无需收紧

        # 检查频率限制(避免每次 patrol 都修改挂单)
        _last_tighten_ts = pos.get("tp2_tighten_ts", 0)
        if time.time() - _last_tighten_ts < TP2_TIGHTEN_CHECK_INTERVAL_MIN * 60:
            return

        hold_h = (time.time() * 1000 - open_ms) / 3600000
        if hold_h < 4.0:
            return  # 持仓不足4H,不收紧

        # 当前价格和浮盈
        price = _last_price.get(symbol)
        if price is None:
            return
        pnl_pct = (price - entry) / entry if direction == "LONG" else (entry - price) / entry
        if pnl_pct <= 0:
            return  # 亏损时不收紧 TP2

        # 计算收紧后的 TP2 位置
        mult = TP2_TIGHTEN_8H_MULT if hold_h >= 8.0 else TP2_TIGHTEN_4H_MULT
        if direction == "LONG":
            new_tp2 = entry * (1 + pnl_pct * mult)
        else:
            new_tp2 = entry * (1 - pnl_pct * mult)

        # 只在新 TP2 < 旧 TP2(更近)时才收紧(不往外推)
        if direction == "LONG" and new_tp2 >= old_tp2:
            return
        if direction == "SHORT" and new_tp2 <= old_tp2:
            return

        # 精度对齐
        try:
            from executor import _get_precision, _round_price
            prec = (await asyncio.to_thread(_get_precision, symbol))
            new_tp2 = _round_price(new_tp2, prec["tick"])
        except Exception:
            pass

        # 撤旧 TP2 + 挂新 TP2
        old_tp2_id = pos.get("tp2_order_id")
        qty_tp2    = float(pos.get("tp2_quantity", 0) or float(pos.get("quantity", 0)) * 0.4)
        if qty_tp2 <= 0:
            return

        close_side = "SELL" if direction == "LONG" else "BUY"
        loop = asyncio.get_running_loop()

        if old_tp2_id:
            try:
                await loop.run_in_executor(None, lambda: api.cancel_order(symbol, int(old_tp2_id)))
            except Exception:
                pass

        try:
            resp = await loop.run_in_executor(
                None, lambda: api.place_order(
                    symbol=symbol, side=close_side,
                    order_type="LIMIT", price=new_tp2,
                    quantity=qty_tp2,
                    position_side=direction,
                    time_in_force="GTC",
                    reduce_only=True,
                )
            )
            new_tp2_id = resp.get("orderId")
            if new_tp2_id:
                (await asyncio.to_thread(update_position, pid, {
                    "tp2_price":    new_tp2,
                    "tp2_order_id": int(new_tp2_id),
                    "tp2_tighten_ts": time.time(),
                }))
                logger.info(
                    f"[pm] {symbol} TP2动态收紧 hold={hold_h:.1f}H "
                    f"mult={mult} pnl={pnl_pct*100:.1f}% "
                    f"old_tp2={old_tp2:.6g} → new_tp2={new_tp2:.6g}"
                )
        except Exception as _te:
            logger.warning(f"[pm] {symbol} TP2收紧挂单失败: {_te}")
            # 失败了把旧 tp2 重挂回去
            try:
                _resp2 = await loop.run_in_executor(
                    None, lambda: api.place_order(
                        symbol=symbol, side=close_side,
                        order_type="LIMIT", price=old_tp2,
                        quantity=qty_tp2,
                        position_side=direction,
                        time_in_force="GTC",
                        reduce_only=True,
                    )
                )
                if _resp2.get("orderId"):
                    (await asyncio.to_thread(update_position, pid, {"tp2_order_id": int(_resp2["orderId"])}))
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"[pm] _maybe_tighten_tp2 {pos.get('symbol','')} 失败(非致命): {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 加仓逻辑(Pyramiding,v3.72)
# ═══════════════════════════════════════════════════════════════════════════════
def _safe_float(v, default: float = 0.0) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    return f if math.isfinite(f) else default


async def _confirm_market_fill(symbol: str, resp: dict, retries: int = 2):
    """
    [v3.89 #58] 市价加仓的成交确认：受理回执（orderId）不等于已成交。
    先查订单快照，非 FILLED 时间隔 0.5s 重查，共 1+retries 次。
    返回 (confirmed, status, executed_qty, avg_price)；只有 FILLED 且数量、均价均有效才算确认。
    均价缺失或为 0 时，用 cumQuote / executedQty 兜底。
    """
    oid = (resp or {}).get("orderId")
    snap = resp or {}
    for attempt in range(retries + 1):
        if oid:
            try:
                q = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.get_order(symbol, int(oid))
                )
                if q:
                    snap = q
            except Exception as e:
                logger.warning(f"[pm] {symbol} 加仓订单查询失败 orderId={oid}: {e}")
        status = snap.get("status", "")
        executed = _safe_float(snap.get("executedQty"))
        avg = _safe_float(snap.get("avgPrice"))
        if avg <= 0 and executed > 0:
            avg = _safe_float(snap.get("cumQuote")) / executed
        if status == "FILLED" and executed > 0 and avg > 0:
            return True, status, executed, avg
        if attempt < retries:
            await asyncio.sleep(0.5)
    return False, snap.get("status", ""), _safe_float(snap.get("executedQty")), _safe_float(snap.get("avgPrice"))


async def _maybe_pyramid(pos: dict) -> None:
    """
    浮盈 >= PYRAMID_FLOAT_PROFIT_ATR × ATR 时,顺势加一笔 PYRAMID_SIZE_RATIO × 原仓位。
    每个方向每个标的最多加一次(pyramid_addup=True 标记后不重复)。
    加仓条件:
      1 浮盈 >= 1.0 ATR
      2 RSI 未到超买/超卖极端(LONG: RSI1H < 75; SHORT: RSI1H > 25)
      3 总持仓未超上限
      4 未加过仓(pyramid_addup 标记)
    try/except 全包。
    """
    try:
        from config import (
            PYRAMID_ENABLED, PYRAMID_FLOAT_PROFIT_ATR,
            PYRAMID_SIZE_RATIO, PYRAMID_MAX_ADDUP,
            PYRAMID_RSI_LONG_MAX, PYRAMID_RSI_SHORT_MIN,
            RISK_MAX_POSITIONS,
        )
        if not PYRAMID_ENABLED:
            return

        pid       = pos.get("pos_id", "")
        symbol    = pos.get("symbol", "")
        direction = pos.get("direction", "long").upper()
        entry     = float(pos.get("entry_price", 0) or 0)
        atr       = float(pos.get("atr_at_entry", 0) or 0)
        qty       = float(pos.get("quantity", 0) or 0)

        if not pid or not symbol or entry <= 0 or atr <= 0 or qty <= 0:
            return
        if pos.get("pyramid_addup", 0) >= PYRAMID_MAX_ADDUP:
            return  # 已加过仓

        # 当前价格和浮盈
        price = _last_price.get(symbol)
        if price is None:
            return
        pnl_pct = (price - entry) / entry if direction == "LONG" else (entry - price) / entry
        atr_ratio = atr / entry if entry > 0 else 0
        profit_atr = pnl_pct / atr_ratio if atr_ratio > 0 else 0

        if profit_atr < PYRAMID_FLOAT_PROFIT_ATR:
            return  # 浮盈不足

        # RSI 检查
        try:
            from shared import binance_api as _api2
            from shared.indicators import rsi as _calc_rsi2
            _k1h = await _run_sync(_api2.get_klines, symbol, "1h", limit=20, timeout=10.0)   # [v3.89 #61]
            _closes = [float(k[4]) for k in _k1h]
            # [v3.89 #60] 空 K 线或样本不足时 rsi() 会返回 50，掩盖数据缺失而绕过失败封闭分支。
            #   这里只检测数据是否可用（RSI 周期 14 至少需要 15 根收盘价），RSI 阈值与开关判断不变。
            if len(_closes) < 15:
                raise ValueError(f"RSI 数据不足: {len(_closes)}/15")
            _rsi1h = _calc_rsi2(_closes, 14)
            if direction == "LONG" and _rsi1h > PYRAMID_RSI_LONG_MAX:
                logger.debug(f"[pm] {symbol} 加仓跳过: RSI1H={_rsi1h:.0f} > {PYRAMID_RSI_LONG_MAX}(超买)")
                return
            if direction == "SHORT" and _rsi1h < PYRAMID_RSI_SHORT_MIN:
                logger.debug(f"[pm] {symbol} 加仓跳过: RSI1H={_rsi1h:.0f} < {PYRAMID_RSI_SHORT_MIN}(超卖)")
                return
        except Exception:
            # [v3.85 FIX-2] 原注释写「保守允许加仓」,但数据缺失时继续加仓恰恰是不保守的。
            #   改为:RSI 拿不到就本轮不加仓(下一轮巡检会再试),开关 OPT_PYRAMID_FAIL_CLOSED。
            try:
                from config import OPT_PYRAMID_FAIL_CLOSED as _OPT_PFC
            except Exception:
                _OPT_PFC = False
            if _OPT_PFC:
                logger.debug(f"[pm] {symbol} 加仓跳过:RSI 数据获取失败(OPT_PYRAMID_FAIL_CLOSED)")
                return

        # 持仓槽位检查
        all_pos = await _run_sync(get_all_positions, timeout=10.0)   # [v3.89 #61]
        open_cnt = sum(1 for p in all_pos if p.get("status") == "open")
        if open_cnt >= RISK_MAX_POSITIONS:
            logger.debug(f"[pm] {symbol} 加仓跳过: 槽位已满({open_cnt}/{RISK_MAX_POSITIONS})")
            return

        # 计算加仓数量(基于原仓位 × PYRAMID_SIZE_RATIO)
        addup_qty = qty * PYRAMID_SIZE_RATIO
        addup_notional = addup_qty * price

        # 简单余额预检
        try:
            acc = await _run_sync(api.get_account, timeout=10.0)   # [v3.89 #61]
            avail = float(acc.get("availableBalance", 0) or 0)
            lev   = int(pos.get("leverage", 10) or 10)
            margin_needed = addup_notional / lev * 1.1
            if avail < margin_needed:
                logger.debug(f"[pm] {symbol} 加仓跳过: 余额不足 {avail:.0f}U < {margin_needed:.0f}U")
                return
        except Exception:
            pass

        # 精度处理
        try:
            from executor import _get_precision, _round_qty, _round_price
            prec     = (await asyncio.to_thread(_get_precision, symbol))
            addup_qty = _round_qty(addup_qty, prec["step"])
        except Exception:
            pass
        if addup_qty <= 0:
            return

        # [v3.89 #59] 区分原始数量与当前剩余数量：TP1 之后剩余量小于 quantity，
        #   加仓后的剩余量必须在当前剩余量上累加，不能直接写成 quantity + 加仓量。
        _rem_raw = pos.get("quantity_remaining")
        remaining = _safe_float(_rem_raw, qty) if _rem_raw not in (None, "") else qty
        if remaining <= 0:
            logger.debug(f"[pm] {symbol} 加仓跳过: 当前剩余数量为 0")
            return

        # 市价加仓(顺势,不挂限价单避免滑点窗口问题)
        buy_side = "BUY" if direction == "LONG" else "SELL"
        loop     = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            None, lambda: api.place_order(
                symbol=symbol, side=buy_side,
                order_type="MARKET",
                quantity=addup_qty,
                position_side=direction,
            )
        )
        if resp.get("orderId") or resp.get("status") == "FILLED":
            # [v3.89 #58] 受理 ≠ 成交：以订单快照里的实际成交量和均价记账。
            #   未确认 FILLED 时不改仓位、不补单，只保存订单号供人工核对。
            _ok, _st, _exec_q, _avg = await _confirm_market_fill(symbol, resp)
            if not _ok:
                _oid_p = resp.get("orderId")
                logger.warning(
                    f"[pm] {symbol} 加仓订单未确认成交 orderId={_oid_p} status={_st} "
                    f"executedQty={_exec_q} avgPrice={_avg}，不更新仓位，请人工核对"
                )
                (await asyncio.to_thread(update_position, pid, {"pyramid_pending_order_id": _oid_p, "pyramid_pending_status": _st}))
                await _notify_async(
                    f"⚠️ {symbol} 加仓订单 {_oid_p} 未确认成交（status={_st}，已成交={_exec_q}），"
                    f"本地仓位未更新，请核对交易所持仓。"
                )
                return
            addup_qty = _exec_q
            fill_p = _avg
            new_total_qty  = qty + addup_qty
            new_remaining  = remaining + addup_qty
            # 均价按当前剩余量加权：反映实际持仓成本，不含已止盈部分
            new_avg_entry  = (entry * remaining + fill_p * addup_qty) / new_remaining
            # 加仓后 SL 设到原入场价(保本线)
            new_sl = entry
            (await asyncio.to_thread(update_position, pid, {
                "quantity":          new_total_qty,
                "quantity_remaining": new_remaining,
                "entry_price":       round(new_avg_entry, 8),
                "sl_target_price":   new_sl,
                "sl_sync_status":    "pending",
                "pyramid_addup":     pos.get("pyramid_addup", 0) + 1,
            }))
            logger.info(
                f"[pm] {symbol} 加仓成功 qty={addup_qty} @ {fill_p:.6g} "
                f"profit_atr={profit_atr:.2f} 新均价={new_avg_entry:.6g} "
                f"新SL={new_sl:.6g}(原入场价保本)"
            )
            # 立刻同步把交易所 Algo SL 移到保本线(不等 trail_sl 下轮巡检)
            try:
                _pos_updated = dict(pos)
                _pos_updated["quantity_remaining"] = new_remaining
                synced = await _execute_trail_sl(_pos_updated, new_sl)
                if synced:
                    logger.info(f"[pm] {symbol} 加仓后 Algo SL 已同步移至保本线 {new_sl:.6g}")
                else:
                    logger.warning(f"[pm] {symbol} 加仓后 Algo SL 尚未同步，保留旧保护字段，目标价={new_sl:.6g}")
            except Exception as _sl_sync_e:
                logger.warning(f"[pm] {symbol} 加仓后 Algo SL 同步失败(非致命,trail_sl 下轮会修复): {_sl_sync_e}")
            await _notify_async(
                f"📈 {symbol} 顺势加仓\n"
                f"方向:{'多' if direction=='LONG' else '空'} 浮盈 {pnl_pct*100:.1f}%\n"
                f"加仓量:{addup_qty}(原仓位50%)均价→{new_avg_entry:.6g}"
            )
        else:
            logger.warning(f"[pm] {symbol} 加仓下单失败: {resp}")
    except Exception as e:
        logger.debug(f"[pm] _maybe_pyramid {pos.get('symbol','')} 失败(非致命): {e}")


def _calc_funding_cost_for_pos(pos: dict, equity: float) -> dict:
    """
    计算单个持仓的资金费率成本(调用方保证在资金费率结算窗口内调用)。
    返回 {funding_cost: float} 字段,写入 pos_state 的 funding_cost_total。
    LONG 持仓付出(负数),SHORT 持仓收取(正数)。
    """
    try:
        symbol    = pos.get("symbol", "")
        direction = pos.get("direction", "long").upper()
        quantity  = float(pos.get("quantity", 0) or 0)
        entry_p   = float(pos.get("entry_price", 0) or 0)
        if quantity <= 0 or entry_p <= 0:
            return {}
        md = api.get_mark_price(symbol)
        rate = float(md.get("lastFundingRate", 0) or 0)
        # LONG 付出(负),SHORT 收取(正)
        direction_sign = -1.0 if direction == "LONG" else 1.0
        cost = quantity * entry_p * rate * direction_sign
        prev_total = float(pos.get("funding_cost_total", 0) or 0)
        new_total  = round(prev_total + cost, 6)
        return {"funding_cost_total": new_total, "funding_cost_last": round(cost, 6)}
    except Exception as e:
        logger.debug(f"[pm] _calc_funding_cost_for_pos {pos.get('symbol')} 失败: {e}")
        return {}


async def _maybe_record_funding_costs() -> None:
    """
    在资金费率结算窗口内(UTC 00/08/16 整点后 30 分钟内)
    对所有 open 持仓计算并累积资金费率成本。
    try/except 包裹,任何失败不影响主策略。
    """
    try:
        from datetime import datetime, timezone
        from shared.state import get_all_positions, update_position
        now_utc = datetime.now(timezone.utc)
        if now_utc.hour not in {0, 8, 16}:
            return
        if now_utc.minute >= 30:
            return
        positions = (await asyncio.to_thread(get_all_positions))
        try:
            acc_data = (await asyncio.to_thread(api.get_account))
            equity   = float(acc_data.get("totalMarginBalance", 0) or 0)
        except Exception:
            equity = 0.0
        for pos in positions:
            if pos.get("status") != "open":
                continue
            # 每个结算窗口只记录一次(用 funding_last_settle_hour 防重复)
            last_settle_h = pos.get("funding_last_settle_hour", -1)
            if last_settle_h == now_utc.hour:
                continue
            updates = _calc_funding_cost_for_pos(pos, equity)
            if updates:
                updates["funding_last_settle_hour"] = now_utc.hour
                (await asyncio.to_thread(update_position, pos["pos_id"], updates))
                logger.debug(
                    f"[pm] {pos.get('symbol')} 资金费率成本记录: "
                    f"{updates.get('funding_cost_last',0):+.4f}U "
                    f"累计={updates.get('funding_cost_total',0):+.4f}U"
                )
    except Exception as e:
        logger.warning(f"[pm] _maybe_record_funding_costs 失败(非致命): {e}")


async def _generate_daily_report():
    """读取 trade_log.jsonl 里昨日记录,生成并发送日报"""
    now_utc   = datetime.now(timezone.utc)
    bj_now    = now_utc + timedelta(hours=8)
    yesterday = (bj_now - timedelta(days=1)).date()

    # 按北京时间昨天 00:00~23:59 过滤
    day_start_ms = int(datetime(yesterday.year, yesterday.month, yesterday.day,
                                 tzinfo=timezone(timedelta(hours=8))).timestamp() * 1000)
    day_end_ms   = day_start_ms + 86400000

    records = []
    try:
        if TRADE_LOG_PATH.exists():
            from shared.trade_log_io import read_lines as _read_lines   # v3.93: 移出事件循环
            if True:
                for line in await asyncio.to_thread(_read_lines, TRADE_LOG_PATH):
                    line = line.strip()
                    if not line: continue
                    r = json.loads(line)
                    if day_start_ms <= r.get("close_time", 0) < day_end_ms:
                        records.append(r)
    except Exception as e:
        logger.error(f"[pm] 日报读取 trade_log 失败: {e}")
        return

    total_pnl = sum(r["pnl_usdt"] for r in records)
    wins      = [r for r in records if r["pnl_usdt"] > 0]
    losses    = [r for r in records if r["pnl_usdt"] <= 0]
    win_rate  = len(wins) / len(records) * 100 if records else 0

    # 各模式统计
    mode_stats: dict[str, dict] = {}
    for r in records:
        m = r.get("mode", "unknown")
        if m not in mode_stats:
            mode_stats[m] = {"count": 0, "wins": 0, "pnl": 0.0}
        mode_stats[m]["count"] += 1
        mode_stats[m]["pnl"]   += r["pnl_usdt"]
        if r["pnl_usdt"] > 0:
            mode_stats[m]["wins"] += 1

    # 当前持仓
    open_pos  = [p for p in (await asyncio.to_thread(get_all_positions)) if p.get("status") == "open"]
    pos_lines = []
    for p in open_pos:
        sym   = p["symbol"]
        d     = "多" if p.get("direction", "").upper() == "LONG" else "空"
        price = _last_price.get(sym, float(p.get("entry_price", 0)))
        entry = float(p.get("entry_price", price))
        direction = p.get("direction", "long").upper()
        pnl_pct = ((price - entry) / entry * 100) if direction == "LONG" \
                  else ((entry - price) / entry * 100)
        pos_lines.append(f"  - {sym} {d}单 浮{'+' if pnl_pct>=0 else ''}{pnl_pct:.1f}%")

    from shared.risk_manager import get_state as rm_state
    rm = rm_state()
    circuit_status = "熔断中 🔴" if rm.get("circuit_open") else "正常 🟢"

    mode_lines = []
    for m, st in mode_stats.items():
        label = {"pullback": "回踩", "breakout": "突破", "reversal": "反转"}.get(m, m)
        mode_lines.append(
            f"  {label}:{st['count']}笔 胜{st['wins']}负{st['count']-st['wins']} "
            f"{'+' if st['pnl']>=0 else ''}{st['pnl']:.1f}U"
        )

    pos_section = "\n".join(pos_lines) if pos_lines else "  当前空仓"
    mode_section = "\n".join(mode_lines) if mode_lines else "  无交易记录"

    # ── 资金费率成本统计(当前持仓里累积的 funding_cost_total)────────────────
    funding_line = ""
    try:
        total_paid     = 0.0
        total_received = 0.0
        by_pos_list    = []
        for p in open_pos:
            fc = float(p.get("funding_cost_total", 0) or 0)
            if fc == 0:
                continue
            sym_label = p.get("symbol", "?") + "(" + (
                "LONG" if p.get("direction", "").upper() == "LONG" else "SHORT") + ")"
            if fc < 0:
                total_paid += fc
                by_pos_list.append(f"{sym_label}: {fc:+.2f}U")
            else:
                total_received += fc
                by_pos_list.append(f"{sym_label}: +{fc:.2f}U")
        net_cost = round(total_paid + total_received, 2)
        try:
            _acc_eq = (await asyncio.to_thread(api.get_account))
            _eq_v   = float(_acc_eq.get("totalMarginBalance", 0) or 0)
        except Exception:
            _eq_v = 1.0
        net_pct = round(net_cost / (_eq_v + 1e-9) * 100, 2) if _eq_v > 0 else 0.0
        if total_paid != 0 or total_received != 0:
            by_str = "  " + "\n  ".join(by_pos_list) if by_pos_list else ""
            funding_line = (
                f"\n💰 今日净资金费率成本:{net_cost:+.2f}U({net_pct:+.2f}% 权益)"
                + (f"\n{by_str}" if by_str else "")
            )
    except Exception as _fe:
        logger.debug(f"[pm] 日报 funding_cost 计算失败: {_fe}")

    report = (
        f"📊 每日交易日报 [{yesterday}]\n\n"
        f"昨日盈亏:{'+' if total_pnl>=0 else ''}{total_pnl:.1f}U\n"
        f"交易笔数:{len(records)} 笔\n"
        f"胜率:{win_rate:.0f}%({len(wins)}胜 {len(losses)}负){funding_line}\n\n"
        f"当前持仓:\n{pos_section}\n\n"
        f"今日已用仓位:{len(open_pos)} / {__import__('config').RISK_MAX_POSITIONS}\n"
        f"熔断器状态:{circuit_status}\n\n"
        f"昨日各模式表现:\n{mode_section}"
    )
    await _notify_async(report)
    logger.info(f"[pm] 日报已发送 {yesterday}")

    # 日报发送后立即触发归档
    try:
        from archiver import archive_daily
        loop = asyncio.get_running_loop()
        ok = await loop.run_in_executor(None, lambda: archive_daily(str(yesterday)))
        if ok:
            logger.info(f"[pm] 归档完成 {yesterday}")
        else:
            logger.warning(f"[pm] 归档失败 {yesterday}")
    except Exception as _ae:
        logger.warning(f"[pm] 归档异常(非致命): {_ae}")


# ─────────────────────────────────────────────────────────────────────────────
# 日报定时循环
# ─────────────────────────────────────────────────────────────────────────────
async def _daily_report_loop_static():
    global _last_daily_report_date
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            if now_utc.hour == DAILY_REPORT_UTC_HOUR and now_utc.minute < 5:
                today_str = now_utc.strftime("%Y-%m-%d")
                if today_str != _last_daily_report_date:
                    _last_daily_report_date = today_str
                    await _generate_daily_report()
        except Exception as e:
            logger.error(f"[pm] 日报循环异常: {e}")
        await asyncio.sleep(60)


# ─────────────────────────────────────────────────────────────────────────────
# 绑定到 PositionManager 类
# ─────────────────────────────────────────────────────────────────────────────
PositionManager._check_pending_expire   = lambda self, p, n: _check_pending_expire_static(p, n)
PositionManager._force_close            = lambda self, p, r: _force_close_static(p, r)
PositionManager._close_position_post    = lambda self, pid, sym, reason, ev, **kw: \
    _close_position_post_static(pid, sym, reason, ev, **kw)   # async 函数，调用方需 await
PositionManager._daily_report_loop      = lambda self: _daily_report_loop_static()


# ─────────────────────────────────────────────────────────────────────────────
# 模块级单例管理(供 main.py 注入和 executor 查询)
# ─────────────────────────────────────────────────────────────────────────────
_MANAGER: "PositionManager | None" = None


def _set_manager(pm: "PositionManager") -> None:
    global _MANAGER
    _MANAGER = pm


def get_manager() -> "PositionManager | None":
    return _MANAGER


# patrol_once:供 main.py 在不需要内部巡检循环时手动触发单次巡检
async def _补挂保护单(pos: dict, fill_price: float, fill_qty: float, pid: str, symbol: str):
    """
    对已成交但缺少止损止盈的 pending 仓位补挂保护单。
    复用 executor._place_guards_async 逻辑,补挂后更新 pos_state 为 open。

    互斥保护:同一 pos_id 同时只允许一条路径运行(防止 executor + patrol 并发补挂)。
    防重复:调用前先查交易所是否已有 TP 限价单,有则直接同步状态跳过挂单。
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    from executor import _place_guards_async, _get_precision, _round_price, _round_qty as _rq_exec
    from shared.state import update_position
    from shared.calc_sl_tp import calc_sl_tp
    from shared import binance_api as _api

    # ── 互斥锁:同一 pos_id 只允许一条路径执行补挂 ──────────────────────────
    lock = _guard_locks[pid]
    if lock.locked():
        logger.info(f"[pm] {symbol}(pid={pid}) 补挂已在进行中,跳过重复触发")
        return

    async with lock:
        direction = pos.get("direction","LONG").upper()
        pos_side  = "LONG" if direction == "LONG" else "SHORT"
        sl_price  = pos.get("sl_price")
        tp1_price = pos.get("tp1_price")
        tp2_price = pos.get("tp2_price")

        # ── 实际持仓量校验:持仓为 0 时就是已经被平仓,跳过补挂避免 -2022 ──────────
        try:
            real_positions = await asyncio.get_running_loop().run_in_executor(
                None, lambda: _api.get_position(symbol)
            ) or []
            real_pos = next(
                (p for p in real_positions
                 if p.get("positionSide","").upper() == pos_side),
                None
            )
            real_qty = abs(float(real_pos.get("positionAmt", 0))) if real_pos else 0.0
            if real_qty <= 0:
                logger.info(f"[pm] {symbol} 交易所实际持仓为0(已被平仓),跳过补挂")
                (await asyncio.to_thread(update_position, pid, {"status": "closed", "close_reason": "already_closed"}))
                (await asyncio.to_thread(_remove_position_checked, pid))
                return
            # 使用交易所实际量覆盖本地记录(防止部分成交导致数量偏差)
            if abs(real_qty - fill_qty) / max(fill_qty, 1e-9) > 0.01:
                logger.info(f"[pm] {symbol} 实际持仓量 {real_qty} 与成交量 {fill_qty} 有偏差,以实际量为准")
                fill_qty = real_qty
        except Exception as qty_err:
            logger.warning(f"[pm] {symbol} 查询实际持仓量失败(继续补挂): {qty_err}")

        # ── [fix #4] SL 状态只判一次，后续只走一条路径 ───────────────────────────
        #   存活 → 不重复挂；缺失 → 先撤旧单(若有 id)再统一补挂；查询失败 → 本轮不挂 SL（避免重复），
        #   由 patrol 下一轮重试。新 SL 的 id 一律写回 pos_state。
        sl_id_old = pos.get("sl_order_id")
        sl_alive = None   # None = 未知
        try:
            _algo_now = await asyncio.get_running_loop().run_in_executor(
                None, lambda: _api.get_algo_open_orders(symbol)
            )
            _algo_ids = {int(o.get("algoId", 0)) for o in (_algo_now or []) if o.get("algoId")}
            sl_alive = bool(sl_id_old and int(sl_id_old) in _algo_ids)
        except Exception as _sl_qe:
            logger.warning(f"[pm] {symbol} 查询 Algo SL 失败，本轮不补挂 SL（避免重复）: {_sl_qe}")
        if sl_alive is False and sl_id_old:
            try:   # 缺失判定后仍尝试撤旧 id（已失效则静默）
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: _api.cancel_algo_order(symbol, int(sl_id_old))
                )
            except Exception:
                pass

        async def _repair_sl_only() -> "int | None":
            """只补 SL（TP 已在交易所）。成功则把新 id 写回 pos_state。"""
            if sl_alive is not False or not sl_price:
                return None
            _prec = (await asyncio.to_thread(_get_precision, symbol))
            _resp = await asyncio.get_running_loop().run_in_executor(
                None, lambda: _api.place_sl(
                    symbol=symbol,
                    side="SELL" if direction == "LONG" else "BUY",
                    position_side=pos_side,
                    trigger_price=_round_price(float(sl_price), _prec["tick"]),
                    quantity=_rq_exec(float(fill_qty), _prec["step"]),
                )
            )
            _nid = (_resp or {}).get("algoId") or (_resp or {}).get("orderId")
            if _nid:
                (await asyncio.to_thread(update_position, pid, {"sl_order_id": int(_nid)}))
                logger.info(f"[pm] {symbol} SL 补挂成功 id={_nid}")
                return int(_nid)
            logger.error(f"[pm] {symbol} SL 补挂失败: {_resp}")
            return None

        # ── 防重复:查交易所是否已有 TP/SL 挂单 ──────────────────────────────
        try:
            open_orders = await asyncio.get_running_loop().run_in_executor(
                None, lambda: _api.get_open_orders(symbol)
            ) or []
            existing_tp = [
                o for o in open_orders
                if o.get("positionSide", "").upper() == pos_side
                and o.get("type") == "LIMIT"
                and o.get("side") != ("BUY" if direction == "LONG" else "SELL")  # 平仓方向
            ]
            if existing_tp:
                # 按价格匹配 pos_state 里的 tp1/tp2,防止顺序错位导致脏数据
                existing_tp_sorted = sorted(
                    existing_tp,
                    key=lambda o: float(o["price"]),
                    reverse=(pos.get("direction", "long").upper() == "SHORT")
                )
                tp1_id = int(existing_tp_sorted[0]["orderId"])
                tp2_id = int(existing_tp_sorted[1]["orderId"]) if len(existing_tp_sorted) > 1 else None

                # 检查 TP2 价格是否和 pos_state 吻合,若相同则说明挂单有误,强制重挂 TP2
                tp2_state_price = pos.get("tp2_price")
                if tp2_id and tp2_state_price:
                    actual_tp1_price = float(existing_tp_sorted[0]["price"])
                    actual_tp2_price = float(existing_tp_sorted[1]["price"])
                    if abs(actual_tp1_price - actual_tp2_price) < 1e-9:
                        # TP1 和 TP2 价格完全相同,TP2 挂错了,撤掉重挂
                        logger.warning(
                            f"[pm] {symbol} TP2 挂单价格与 TP1 相同 ({actual_tp2_price}),"
                            f"撤销并按正确价格 {tp2_state_price} 重挂"
                        )
                        try:
                            await asyncio.get_running_loop().run_in_executor(
                                None, lambda: _api.cancel_order(symbol, tp2_id)
                            )
                            tp2_qty = pos.get("tp2_quantity") or 0
                            prec = (await asyncio.to_thread(_get_precision, symbol))
                            tp2_price_r = round(float(tp2_state_price) / prec["tick"]) * prec["tick"]
                            tp2_qty_r   = _round_qty(float(tp2_qty), prec["step"])
                            resp = await asyncio.get_running_loop().run_in_executor(
                                None, lambda: _api.place_order(
                                    symbol=symbol,
                                    side="SELL" if direction == "LONG" else "BUY",
                                    order_type="LIMIT",
                                    quantity=tp2_qty_r,
                                    price=round(tp2_price_r, 8),
                                    timeInForce="GTC",
                                    positionSide=pos_side,
                                )
                            )
                            tp2_id = int(resp["orderId"]) if resp.get("orderId") else tp2_id
                            logger.info(f"[pm] {symbol} TP2 已重挂: orderId={tp2_id} @ {tp2_state_price}")
                        except Exception as fix_err:
                            logger.error(f"[pm] {symbol} TP2 重挂失败: {fix_err}")

                logger.info(
                    f"[pm] {symbol} 交易所已有 {len(existing_tp)} 个 TP 挂单 "
                    f"(tp1={tp1_id} tp2={tp2_id}),跳过补挂,同步 pos_state"
                )
                _new_sl = await _repair_sl_only()
                _sl_ok = (sl_alive is True) or bool(_new_sl)
                (await asyncio.to_thread(update_position, pid, {
                    "status":       "open" if _sl_ok else "pending_guard",
                    "fill_price":   fill_price,
                    "quantity":     fill_qty,
                    "tp1_order_id": tp1_id,
                    "tp2_order_id": tp2_id,
                    **({} if _sl_ok else {"missing_guards": ["sl"]}),
                }))
                return
        except Exception as chk_err:
            logger.warning(f"[pm] {symbol} 检查已有挂单失败(继续补挂): {chk_err}")

        # ── 如果 pos_state 里没有 sl/tp 价格,重新用 ATR 降级计算 ─────────────
        if not sl_price or not tp1_price:
            try:
                klines = (await asyncio.to_thread(_api.get_klines, symbol, "1h", 22))
                now_ms = __import__("time").time() * 1000
                closed = [k for k in klines if float(k[6]) < now_ms]
                trs = [max(float(closed[i][2])-float(closed[i][3]),
                           abs(float(closed[i][2])-float(closed[i-1][4])),
                           abs(float(closed[i][3])-float(closed[i-1][4])))
                       for i in range(1, len(closed))]
                atr = sum(trs[-14:])/14 if trs else fill_price * 0.01
                tier = pos.get("volatility_tier","mid")
                sl_mult = {"low":1.2,"mid":1.5,"high":2.0}.get(tier, 1.5)
                if direction == "LONG":
                    sl_price  = fill_price - atr * sl_mult
                    tp1_price = fill_price + atr * 1.5
                else:
                    sl_price  = fill_price + atr * sl_mult
                    tp1_price = fill_price - atr * 1.5
                # tp2_price 不在这里生成--由 calc_sl_tp 决定是否需要 TP2
                # 降级计算时保持 tp2_price=None(单TP),避免挂出无效 TP2
                tp2_price = None
                logger.info(f"[pm] {symbol} 降级计算: sl={sl_price:.6g} tp1={tp1_price:.6g} tp2=None(单TP)")
            except Exception as e:
                raise RuntimeError(f"计算止损价失败: {e}")

        precision = (await asyncio.to_thread(_get_precision, symbol))
        # SL 存活或状态未知 → 不重复挂 SL（skip_sl）
        _skip_sl = sl_alive is not False
        guard = await _place_guards_async(
            symbol, direction, fill_qty,
            sl_price, tp1_price, tp2_price,
            precision, skip_sl=_skip_sl,
        )

        # 更新 pos_state:pending → open,写入保护单 id
        # 注意:_place_guards_async 返回 key 为 sl_id / tp1_id / tp2_id
        # [fix #10] 保护单只部分成功 → 不写 open，写 pending_guard 并记录缺失项，由 patrol 重试
        import time as _time_fill
        _sl_id_final = guard.get("sl_id") or (int(sl_id_old) if (sl_alive is not False and sl_id_old) else None)
        _missing = []
        if not _sl_id_final:
            _missing.append("sl")
        if not guard.get("tp1_id"):
            _missing.append("tp1")
        if tp2_price is not None and not guard.get("tp2_id"):
            _missing.append("tp2")
        _upd = {
            "status":      "open" if not _missing else "pending_guard",
            "fill_price":  fill_price,
            "fill_time":   int(_time_fill.time()),  # v3.39: 补挂路径补写 fill_time,时间止损用真实成交时间
            "quantity":    fill_qty,
            "sl_order_id": _sl_id_final,
            "tp1_order_id":guard.get("tp1_id"),
            "tp2_order_id":guard.get("tp2_id"),
            "sl_price":    sl_price,
            "tp1_price":   tp1_price,
            "tp2_price":   tp2_price,
            "missing_guards": _missing,
        }
        (await asyncio.to_thread(update_position, pid, _upd))
        if _missing:
            logger.error(f"[pm] {symbol} 保护单不完整，状态置为 pending_guard，缺失: {_missing}")
            try:
                await _notify_async(f"⚠️ {symbol} 保护单不完整（缺失: {', '.join(_missing)}），已标记 pending_guard，patrol 将重试")
            except Exception:
                pass
        logger.info(
            f"[pm] {symbol} 补挂完成: "
            f"sl_id={guard.get('sl_id')} tp1_id={guard.get('tp1_id')} tp2_id={guard.get('tp2_id')}"
        )

        # ── v3.53 竞争开仓:成交后检查同方向槽位,满则撤销其余 pending 挂单 ──
        try:
            from shared.state import get_all_positions as _gap
            import config as _cfg_pm
            from shared.risk_manager import get_market_mode as _gmm_pm
            _mkt = _gmm_pm()
            if _mkt == "bear":
                _max_l = max(1, _cfg_pm.MAX_POSITIONS_LONG_BEAR)
                _max_s = _cfg_pm.MAX_POSITIONS_SHORT_BEAR
            elif _mkt == "bull":
                _max_l = _cfg_pm.MAX_POSITIONS_LONG_BULL
                _max_s = _cfg_pm.MAX_POSITIONS_SHORT_BULL
            else:
                _max_l = _cfg_pm.MAX_POSITIONS_LONG_RANGE
                _max_s = _cfg_pm.MAX_POSITIONS_SHORT_RANGE
            _all_pos = (await asyncio.to_thread(_gap))
            _open_long  = sum(1 for p in _all_pos if p.get("direction")=="LONG"  and p.get("status")=="open")
            _open_short = sum(1 for p in _all_pos if p.get("direction")=="SHORT" and p.get("status")=="open")
            # 检查刚成交的这个方向是否已达上限
            _dir_upper = direction.upper()
            _open_cnt  = _open_long if _dir_upper == "LONG" else _open_short
            _max_cnt   = _max_l if _dir_upper == "LONG" else _max_s
            if _open_cnt >= _max_cnt:
                # 槽位已满,撤销同方向其余所有 pending 挂单(排除刚成交的 symbol)
                _pending_same_dir = [
                    p for p in _all_pos
                    if p.get("direction","").upper() == _dir_upper
                    and p.get("status") == "pending"
                    and p.get("symbol") != symbol
                ]
                if _pending_same_dir:
                    logger.info(
                        f"[pm] {symbol} 成交后 {_dir_upper} 槽位满({_open_cnt}/{_max_cnt}),"
                        f"撤销 {len(_pending_same_dir)} 个同方向 pending 挂单"
                    )
                    for _pp in _pending_same_dir:
                        _pp_sym = _pp["symbol"]
                        try:
                            await asyncio.get_running_loop().run_in_executor(
                                None, lambda s=_pp_sym: api.cancel_all_orders(s)
                            )
                            # 清除 pos_state 里的 pending 记录
                            from shared.state import remove_position as _rm_pos
                            # [v3.89 remove-pos 检查] 检查返回值，失败只记日志
                            try:
                                if (await asyncio.to_thread(_rm_pos, _pp.get("pos_id", _pp_sym))) is False:
                                    logger.warning(f"[pm] remove_position({_pp_sym}) 返回 False，pos_state 未写入成功，请人工核查")
                            except Exception as _re:
                                logger.error(f"[pm] remove_position({_pp_sym}) 抛异常，持仓文件可能损坏，请人工核查: {_re}")
                            logger.info(f"[pm] 已撤销 {_pp_sym} 的竞争挂单")
                        except Exception as _ce:
                            logger.warning(f"[pm] 撤销 {_pp_sym} 竞争挂单失败: {_ce}")
        except Exception as _comp_e:
            logger.warning(f"[pm] 竞争开仓清理失败(不影响主流程): {_comp_e}")

        await _notify_async(
            f"✅ {symbol} 补挂止损止盈完成\n"
            f"入场:{fill_price:.6g} 止损:{sl_price:.6g} TP1:{tp1_price:.6g}\n"
            f"sl_id={guard.get('sl_id')} tp1_id={guard.get('tp1_id')}"
        )


async def patrol_once() -> dict:
    """单次执行所有巡检(时间止损 + 突破失败 + 健康检查 + 挂单过期 + 手动平仓检测 + TP REST 确认)。"""
    pm = get_manager()
    if pm is None:
        return {"error": "PositionManager 未初始化"}
    try:
        from shared.state import get_all_positions
        positions = (await asyncio.to_thread(get_all_positions))
        open_pos  = [p for p in positions if p.get("status") == "open"]
        now_ms = int(__import__("time").time() * 1000)

        # [fix #10] pending_guard：保护单曾只部分成功，节流重试补挂（有真实持仓、缺保护单）
        for _pg in [p for p in positions if p.get("status") == "pending_guard"]:
            _pgid = _pg.get("pos_id", _pg.get("symbol"))
            if time.time() - _pg_retry_ts.get(_pgid, 0) < 60:
                continue
            _pg_retry_ts[_pgid] = time.time()
            try:
                await _补挂保护单(_pg, float(_pg.get("fill_price") or 0),
                                  float(_pg.get("quantity_remaining") or _pg.get("quantity") or 0),
                                  _pgid, _pg["symbol"])
            except Exception as _pge:
                logger.error(f"[pm] {_pg.get('symbol')} pending_guard 重试补挂失败: {_pge}")
        positions = (await asyncio.to_thread(get_all_positions))
        open_pos  = [p for p in positions if p.get("status") == "open"]

        # 手动平仓检测(孤单清理)
        for pos in list(open_pos):
            await pm._check_manual_close(pos)
        # 重新加载(_check_manual_close 可能删除了记录)
        positions = (await asyncio.to_thread(get_all_positions))
        open_pos  = [p for p in positions if p.get("status") == "open"]

        # TP REST 确认保底
        for pos in open_pos:
            await pm._check_tp_filled_rest(pos)

        # 时间止损 + 突破失败 + 挂单过期(主策略仓位)
        for pos in open_pos:
            if pos.get("sig_type") == "funding_arb":
                continue   # funding_arb 仓位由专用巡检处理
            await pm._check_time_stop(pos, now_ms)
            await pm._check_breakout_fail(pos)
            await pm._check_exit_signal(pos)
            await pm._check_pending_expire(pos, now_ms)

        # funding_arb 专用巡检
        fa_pos = [p for p in open_pos if p.get("sig_type") == "funding_arb"]
        for pos in fa_pos:
            await _patrol_funding_arb(pos)

        # 资金费率成本记录(每次结算窗口触发一次)
        await _maybe_record_funding_costs()

        # TP2 动态收紧(每30分钟检查一次,基于持仓时间自适应调整 TP2 目标)
        try:
            from config import TP2_DYNAMIC_TIGHTEN_ENABLED
            if TP2_DYNAMIC_TIGHTEN_ENABLED:
                for pos in open_pos:
                    if pos.get("sig_type") == "funding_arb":
                        continue
                    await _maybe_tighten_tp2(pos)
        except Exception as _tp2e:
            logger.debug(f"[pm] TP2动态收紧跳过: {_tp2e}")

        # 加仓检查(浮盈 >= 1.0 ATR 时考虑加一笔顺势仓)
        try:
            from config import PYRAMID_ENABLED
            if PYRAMID_ENABLED:
                for pos in open_pos:
                    if pos.get("sig_type") == "funding_arb":
                        continue
                    await _maybe_pyramid(pos)
        except Exception as _pyre:
            logger.debug(f"[pm] 加仓检查跳过: {_pyre}")

        # 健康检查每 30 分钟一次,由 _patrol_loop 内部计数器控制
        return {"checked": len(positions)}
    except Exception as e:
        logger.error(f"[pm] patrol_once 异常: {e}", exc_info=True)
        return {"error": str(e)}


async def _maybe_daily_report_static() -> None:
    """检查是否到了日报时间(委托给 _daily_report_loop 逻辑的精简版)"""
    global _last_daily_report_date
    try:
        from datetime import datetime, timezone
        now_utc = datetime.now(timezone.utc)
        if now_utc.hour == DAILY_REPORT_UTC_HOUR and now_utc.minute < 5:
            today_str = now_utc.strftime("%Y-%m-%d")
            if today_str != _last_daily_report_date:
                _last_daily_report_date = today_str
                await _generate_daily_report()
    except Exception as e:
        logger.error(f"[pm] _maybe_daily_report 异常: {e}")


# =============================================================================
# 动态 TP/SL 调整(每30分钟,双向,静默,写日志供复盘)
# =============================================================================

async def _adjust_sl_tp_once(pos: dict, force: bool = False) -> dict | None:
    """
    对单个持仓重新拉数据+清算图,重算 SL/TP,决定是否更新挂单。

    规则:
      TP 双向调整(宽松下调门槛:新TP比旧TP差 > TP_SL_ADJUST_TP_SHRINK_ATR × ATR 才下调,防噪音)
      SL 只允许向有利方向移动(LONG 只上移、SHORT 只下移),且移动幅度需 > 0.3×ATR 才执行;
        向不利方向(外扩)的重算结果一律忽略。
        [v3.87 STEP7-3] 原文档写「SL 双向调整(外扩门槛...)」与实现不符(v3.6 起已禁止外扩),已更正。
      只更新发生实质变化的那一侧
      每次结果写 tp_sl_adjust_log.jsonl
      force=True:跳过持仓时长限制(BTC 剧烈波动时使用)

    返回调整摘要 dict,或 None(跳过)。
    """
    import json as _json, datetime, pathlib

    symbol    = pos["symbol"]
    pid       = pos.get("pos_id", symbol)
    direction = pos["direction"].upper()
    open_ms   = int(pos.get("open_time", 0))
    hold_min  = (time.time() * 1000 - open_ms) / 60000

    # 持仓时间太短不调(force=True 时跳过,BTC 极端波动需要立即重算)
    if not force and hold_min < TP_SL_ADJUST_MIN_HOLD_MIN:
        return None

    # v3.56: 正在平仓时跳过 SL/TP 调整,防止 adjust 和 force_close 并发撤挂单冲突
    if pid in _closing_set:
        logger.debug(f"[pm] {symbol} _adjust_sl_tp_once 跳过:仓位正在平仓中")
        return None

    # 问题五修复:与 trail_sl 互斥,避免同一仓位止损单叠加/冲突
    lock = _guard_locks[pid]
    if lock.locked():
        logger.debug(f"[pm] {symbol} _adjust_sl_tp_once 跳过:trail_sl 正在运行")
        return None

    tp1_filled = pos.get("tp1_filled", False)
    old_sl  = float(pos.get("sl_price", 0))
    old_tp1 = float(pos.get("tp1_price", 0))
    old_tp2 = pos.get("tp2_price")
    old_tp2 = float(old_tp2) if old_tp2 else None
    sl_id   = pos.get("sl_order_id")
    tp1_id  = pos.get("tp1_order_id")
    tp2_id  = pos.get("tp2_order_id")
    qty     = float(pos.get("quantity_remaining", pos.get("quantity", 0)))
    qty_tp1 = float(pos.get("tp1_quantity", qty * 0.6))
    qty_tp2 = float(pos.get("tp2_quantity", qty * 0.4)) if old_tp2 else 0.0

    if old_sl <= 0 or old_tp1 <= 0 or qty <= 0:
        return None

    # ── 拉最新数据 ────────────────────────────────────────────────────────────
    try:
        k1h_raw = await asyncio.get_running_loop().run_in_executor(
            None, lambda: api.get_klines(symbol, "1h", 55)
        )
        now_ms = time.time() * 1000
        k1h    = [k for k in k1h_raw if float(k[6]) < now_ms]
        if len(k1h) < 10:
            return None
        closes   = [float(k[4]) for k in k1h]
        atr      = _calc_atr_from_klines(k1h)
        ema20    = _calc_ema(closes, 20)
        md       = await asyncio.get_running_loop().run_in_executor(
            None, lambda: api.get_mark_price(symbol)
        )
        mark_price  = float(md.get("markPrice", closes[-1]))
        funding     = float(md.get("lastFundingRate", 0))
        entry_price = float(pos.get("fill_price") or pos.get("entry_price", mark_price))
    except Exception as e:
        logger.warning(f"[adjust_sltp] {symbol} 数据拉取失败: {e}")
        return None

    if atr <= 0:
        return None

    # ── 重算 TP/SL ────────────────────────────────────────────────────────────
    try:
        from shared.liq_cache import get_liq_zones_cached
        from shared.calc_sl_tp import calc_sl_tp
        ind_for_signal = {
            "rsi_1h":       _calc_rsi(closes),
            "vol_ratio":    1.0,
            "funding_rate": funding,
        }
        fake_signal = {
            "symbol":         symbol,
            "direction":      direction,
            "mode":           pos.get("mode", "pullback"),
            "volatility_tier": pos.get("volatility_tier", "mid"),
            "ind":            ind_for_signal,
            # 传入实际成交价,让 calc_sl_tp 跳过重新估算 entry
            # 保证 SL/TP 的 RR 基准始终锁定在原始开仓价,不随当前价滨漂移
            "fill_price":     pos.get("fill_price") or pos.get("entry_price"),
            # [v3.88 OPT-14e] 入场时锁定 TP1 清算区缓冲:沿用开仓时写入的值,不按最新清算图重算。
            #   仅在 OPT_LIQ_RELATIVE_STRENGTH 开启时生效;旧仓位没有该字段时为 None,回到原 0.3ATR。
            "tp1_buffer_locked": pos.get("tp1_liq_buffer_atr"),
        }
        # 止损/止盈调整参考大级别清算区(7d),不容易被短期波动穿透
        _adj_sig_type = pos.get("sig_type", pos.get("mode", "pullback"))
        # oi_divergence 是短线信号,用 24h 清算区(7d 范围过宽,锚点不够精准)
        _adj_range = "24h" if _adj_sig_type in ("reversal", "funding_arb", "oi_divergence") else "7d"
        liq_long  = await asyncio.get_running_loop().run_in_executor(
            None, lambda: get_liq_zones_cached(symbol, "LONG",  mark_price, range_=_adj_range)
        )
        liq_short = await asyncio.get_running_loop().run_in_executor(
            None, lambda: get_liq_zones_cached(symbol, "SHORT", mark_price, range_=_adj_range)
        )
        new_params = calc_sl_tp(
            signal    = fake_signal,
            mark_price= mark_price,
            atr       = atr,
            liq_zones = {"long": liq_long, "short": liq_short},
            k1h       = k1h,
            ema20     = ema20,
        )
        if "error" in new_params:
            return None
    except Exception as e:
        logger.warning(f"[adjust_sltp] {symbol} calc_sl_tp 失败: {e}")
        return None

    new_sl  = new_params["sl_price"]
    new_tp1 = new_params["tp1_price"]
    new_tp2 = new_params.get("tp2_price")

    precision = (await asyncio.to_thread(_get_precision, symbol))
    tick = precision["tick"]
    step = precision["step"]
    close_side = "SELL" if direction == "LONG" else "BUY"
    pos_side   = "LONG" if direction == "LONG" else "SHORT"

    sl_changed  = False
    tp1_changed = False
    tp2_changed = False
    sl_reason   = "no_change"
    tp1_reason  = "no_change"

    # ── SL 判断(双向,外扩有门槛;上移也加 ATR×0.3 门槛防止微小变化频繁撤挂单)─────
    _SL_TIGHTEN_MIN = 0.3  # 上移至少要有 ATR×0.3,才值得撤旧单挂新单
    if direction == "LONG":
        sl_move = new_sl - old_sl          # 正 = 上移(收紧),负 = 下移(外扩)
        if sl_move > 1e-8:
            # 上移(有利):只有变化幅度 > ATR×0.3 才执行,防止清算图微小波动触发频繁更新
            if sl_move > _SL_TIGHTEN_MIN * atr:
                sl_changed = True
                sl_reason  = f"tighten +{sl_move:.6g} (>{_SL_TIGHTEN_MIN}ATR)"
        elif sl_move < -1e-8:
            # v3.6: 禁止做多止损下移(向不利方向调整)
            # 重新计算时以 mark_price 为参考,价格大涨后新 SL 可能低于现有 SL,不允许回退
            pass  # LONG 止损只允许上移,不允许下移
    else:  # SHORT
        sl_move = old_sl - new_sl
        if sl_move > 1e-8:
            if sl_move > _SL_TIGHTEN_MIN * atr:
                sl_changed = True
                sl_reason  = f"tighten +{sl_move:.6g} (>{_SL_TIGHTEN_MIN}ATR)"
        elif sl_move < -1e-8:
            # v3.6: 禁止做空止损上移(向不利方向调整)
            pass  # SHORT 止损只允许下移,不允许上移

    # ── [fix #42] 止损上限检查：调整后的止损距离不得超过开仓时的初始风险上限 RISK_PER_TRADE × 入场价。
    #   当前实现只允许 SL 向有利方向移动，天然不会外扩；这里是防御性硬上限，
    #   防止以后放开外扩（TP_SL_ADJUST_SL_WIDEN_ATR）或 calc_sl_tp 异常时突破初始风险。
    if sl_changed:
        try:
            _ref_entry = float(entry_price or pos.get("entry_price") or 0)
            _new_sl_f = float(new_sl)
            # 已越过入场价（进入盈利区）→ 锁利上移，豁免风险上限
            _in_profit = (_new_sl_f > _ref_entry) if direction == "LONG" else (_new_sl_f < _ref_entry)
            if _ref_entry > 0 and not _in_profit:
                _new_dist = abs(_ref_entry - _new_sl_f)
                _old_dist = abs(_ref_entry - float(old_sl))
                # 基准 = 开仓时的止损距离；pos 未记录 initial_sl_distance 时退回当前止损距离
                _init_raw = pos.get("initial_sl_distance")
                _max_dist = float(_init_raw) if _init_raw else _old_dist
                if _new_dist > _max_dist and _new_dist > _old_dist:
                    sl_changed = False
                    sl_reason = (f"blocked: 新止损距离 {_new_dist:.6g} 超过开仓止损距离 {_max_dist:.6g}")
                    logger.warning(f"[adjust_sltp] {symbol} 拒绝调整 SL: {sl_reason}")
        except Exception as _rle:
            sl_changed = False
            logger.error(f"[adjust_sltp] {symbol} 止损上限检查异常，保守不调整 SL: {_rle}")

    # ── TP1 判断(双向,下调有门槛)────────────────────────────────────────────
    if not tp1_filled:
        if direction == "LONG":
            tp_move = new_tp1 - old_tp1   # 正 = 上移(更好)
        else:
            tp_move = old_tp1 - new_tp1
        if tp_move > 1e-8:
            tp1_changed = True
            tp1_reason  = f"improve +{tp_move:.6g}"
        elif tp_move < -1e-8:
            if abs(tp_move) > TP_SL_ADJUST_TP_SHRINK_ATR * atr:
                tp1_changed = True
                tp1_reason  = f"shrink {tp_move:.6g} (>{TP_SL_ADJUST_TP_SHRINK_ATR}ATR)"

        # ── 浮盈收紧 TP:价格已向有利方向移动但 TP 仍很远时,主动拉近 ──────────
        # 触发条件A(顺势浮盈):
        #   1. 价格已向有利方向移动 ≥ 1×ATR
        #   2. 当前 TP1 距现价 > 3×ATR
        #   3. 新算出的 TP1 比旧 TP1 近
        # 触发条件B(TP绝对距离过远):
        #   TP1 距现价 > 6×ATR(无论盈亏),新TP更近时直接拉近
        #   防止「开仓时TP算得过深,价格整个持仓期间都无法触达」
        if not tp1_changed and old_tp1 and new_tp1:
            if direction == "LONG":
                price_moved    = mark_price - entry_price
                tp_dist_now    = old_tp1 - mark_price
                tp_will_closer = new_tp1 < old_tp1
            else:
                price_moved    = entry_price - mark_price
                tp_dist_now    = mark_price - old_tp1
                tp_will_closer = new_tp1 > old_tp1

            # 条件A:顺势浮盈收紧
            cond_a = (price_moved >= atr * 1.0
                      and tp_dist_now >= atr * 3.0
                      and tp_will_closer)
            # 条件B:TP 绝对距离过远(>6ATR),无论盈亏都拉近
            # 3ATR 门槛:短线策略(4h以内)TP超过3ATR基本触达概率极低
            cond_b = (tp_dist_now >= atr * 3.0 and tp_will_closer)

            if cond_a or cond_b:
                tp1_changed = True
                reason_tag  = "profit_tighten" if cond_a else "tp_too_far"
                tp1_reason  = (f"{reason_tag}: price_moved={price_moved:.4g}"
                               f" tp_dist={tp_dist_now:.4g} ({tp_dist_now/atr:.1f}ATR)")

    # ── TP2 判断(仅当 TP1 已触发且 TP2 存在时才调整)──────────────────────────
    if tp1_filled and old_tp2 and new_tp2 and tp2_id:
        if direction == "LONG":
            tp2_move = new_tp2 - old_tp2
        else:
            tp2_move = old_tp2 - new_tp2
        if tp2_move > 1e-8:
            tp2_changed = True
        elif tp2_move < -1e-8 and abs(tp2_move) > TP_SL_ADJUST_TP_SHRINK_ATR * atr:
            tp2_changed = True

    if not sl_changed and not tp1_changed and not tp2_changed:
        # 无变化也写日志(记录"维持")
        _write_adjust_log(pid, symbol, direction, "no_change",
                          old_sl, new_sl, old_tp1, new_tp1,
                          old_tp2, new_tp2, atr,
                          new_params.get("consistency_score"),
                          new_params.get("momentum_coeff"),
                          sl_reason, tp1_reason)
        return None

    # ── 执行替换 ──────────────────────────────────────────────────────────────
    actions = []
    # 加系统撤单锁,防止 _check_manual_close 在此期间误判为手动平仓
    mark_system_order_update(symbol)

    # 实际生效价格（挂单成功后由交易所回报确认）；未改动的一侧沿用旧单价格
    eff_sl, eff_tp1 = old_sl, old_tp1
    sl_placed = tp1_placed = tp2_placed = False

    async def _replace_sl() -> bool:
        """先挂新 SL → 成功后只撤本仓位旧 SL(sl_order_id)；新单失败保留旧单不裸仓，返回 False。"""
        nonlocal sl_id, eff_sl, sl_placed
        new_sl_r = _round_price(new_sl, tick)
        old_sl_id = sl_id

        # 第一步:挂新 SL
        try:
            resp = await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.place_sl(
                    symbol=symbol, side=close_side,
                    position_side=pos_side,
                    trigger_price=new_sl_r,
                    quantity=_round_qty(qty, step),
                )
            )
            new_id = int(resp.get("algoId") or resp.get("orderId") or 0)
            if not new_id:
                raise RuntimeError(f"id=0, resp={resp}")
        except Exception as e:
            logger.warning(f"[adjust_sltp] ⚠️ {symbol} 新SL挂单失败，保留旧止损单 {old_sl_id} @ {old_sl:.6g}，本次不再继续: {e}")
            return False

        try:
            _eff = float(resp.get("triggerPrice") or resp.get("stopPrice") or 0) or float(new_sl_r)
        except (TypeError, ValueError):
            _eff = float(new_sl_r)

        # 第二步:新单已生效 → 只撤本仓位自己的旧 SL
        if old_sl_id and int(old_sl_id) != new_id:
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.cancel_algo_order(symbol, int(old_sl_id))
                )
            except Exception as e:
                logger.warning(f"[adjust_sltp] {symbol} 撤旧SL {old_sl_id} 失败(新SL {new_id} 已生效，请人工确认是否残留): {e}")

        (await asyncio.to_thread(update_position, pid, {"sl_price": _eff, "sl_order_id": new_id}))
        sl_id = new_id
        eff_sl = _eff
        sl_placed = True
        logger.info(f"[adjust_sltp] {symbol} SL {old_sl:.6g}→{_eff:.6g} ({sl_reason})")
        return True

    async def _replace_tp1():
        nonlocal tp1_id, eff_tp1, tp1_placed
        new_tp1_r = _round_price(new_tp1, tick)
        # ── 先撤旧单,再挂新单(避免 ReduceOnly 冲突)────────────────────
        if tp1_id:
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.cancel_order(symbol, int(tp1_id))
                )
            except Exception:
                pass  # 旧单可能已成交或已撤
        try:
            resp = await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.place_order(
                    symbol=symbol, side=close_side,
                    order_type="LIMIT",
                    quantity=_round_qty(qty_tp1, step),
                    price=new_tp1_r,
                    timeInForce="GTC",
                    positionSide=pos_side,
                )
            )
            new_id = int(resp.get("orderId") or resp.get("algoId") or 0)
            if not new_id:
                raise RuntimeError(f"id=0, resp={resp}")
        except Exception as e:
            # ── 挂单失败:记录失败次数,连续3次告警 ──
            fail_key = f"tp1_fail_count"
            prev_fail = int(pos.get(fail_key, 0)) + 1
            (await asyncio.to_thread(update_position, pid, {fail_key: prev_fail}))
            logger.warning(f"[adjust_sltp] {symbol} 新TP1挂单失败(第{prev_fail}次): {e}")
            if prev_fail >= 3 and prev_fail % 3 == 0:
                asyncio.ensure_future(_notify_async(
                    f"⚠️ {symbol} TP1调整挂单已连续失败 {prev_fail} 次\n"
                    f"目标价={new_tp1:.6g}\n"
                    f"错误:{e}\n请检查订单状态。"
                ))
            return
        # ── 挂单成功:更新状态,重置失败计数 ──
        try:
            _eff_tp = float(resp.get("price") or 0) or float(new_tp1_r)
        except (TypeError, ValueError):
            _eff_tp = float(new_tp1_r)
        (await asyncio.to_thread(update_position, pid, {"tp1_price": _eff_tp, "tp1_order_id": new_id, "tp1_fail_count": 0}))
        tp1_id = new_id
        eff_tp1 = _eff_tp
        tp1_placed = True
        logger.info(f"[adjust_sltp] {symbol} TP1 {old_tp1:.6g}→{new_tp1:.6g} ({tp1_reason})")

    async def _replace_tp2():
        nonlocal tp2_id, tp2_placed
        if not new_tp2: return
        new_tp2_r = _round_price(new_tp2, tick)
        # ── 先撤旧单,再挂新单(避免 ReduceOnly 冲突)────────────────────
        if tp2_id:
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.cancel_order(symbol, int(tp2_id))
                )
            except Exception:
                pass
        try:
            resp = await asyncio.get_running_loop().run_in_executor(
                None, lambda: api.place_order(
                    symbol=symbol, side=close_side,
                    order_type="LIMIT",
                    quantity=_round_qty(qty_tp2, step),
                    price=new_tp2_r,
                    timeInForce="GTC",
                    positionSide=pos_side,
                )
            )
            new_id = int(resp.get("orderId") or resp.get("algoId") or 0)
            if not new_id:
                raise RuntimeError(f"id=0, resp={resp}")
        except Exception as e:
            logger.warning(f"[adjust_sltp] {symbol} 新TP2挂单失败: {e}")
            return
        (await asyncio.to_thread(update_position, pid, {"tp2_price": new_tp2, "tp2_order_id": new_id}))
        tp2_id = new_id
        tp2_placed = True
        logger.info(f"[adjust_sltp] {symbol} TP2 {old_tp2:.6g}→{new_tp2:.6g}")

    # 串行执行(避免并发在同一标的上挂单冲突)；新 SL 挂失败 → 保留旧单，不再继续后续逻辑
    if sl_changed:
        if not await _replace_sl():
            _write_adjust_log(pid, symbol, direction, "sl_place_failed",
                              old_sl, new_sl, old_tp1, new_tp1,
                              old_tp2, new_tp2, atr,
                              new_params.get("consistency_score"),
                              new_params.get("momentum_coeff"),
                              sl_reason, tp1_reason)
            return {"sl_changed": False, "sl_reason": "place_failed_kept_old",
                    "tp1_changed": False, "tp1_reason": tp1_reason, "tp2_changed": False}
    if tp1_changed: await _replace_tp1()
    if tp2_changed: await _replace_tp2()
    sl_changed, tp1_changed, tp2_changed = sl_placed, tp1_placed, tp2_placed

    # 本次没有任何单实际改成功 → 跳过 RR 检查
    if not (sl_placed or tp1_placed or tp2_placed):
        _write_adjust_log(pid, symbol, direction, "no_change",
                          old_sl, new_sl, old_tp1, new_tp1,
                          old_tp2, new_tp2, atr,
                          new_params.get("consistency_score"),
                          new_params.get("momentum_coeff"),
                          sl_reason, tp1_reason)
        return None

    # ── v3.68 改动二:TP 收紧后 RR < 1.0 主动平仓 ──────────────────────────────
    # 如果 TP1 调整后 RR < 1.0,持仓期望收益为负,主动平仓
    # 用最新价格计算当前 RR(TP_MIN_RR_LIVE 已在模块顶部导入)
    current_mark = await asyncio.get_running_loop().run_in_executor(
        None, lambda: float(api.get_mark_price(symbol).get('markPrice', 0))
    )
    if current_mark > 0:
        sl_dist = abs(current_mark - eff_sl)     # 实际生效的止损价
        tp1_dist = abs(eff_tp1 - current_mark)   # 实际生效的 TP1 价
        if sl_dist > 0:
            rr_now = tp1_dist / sl_dist
            if rr_now < TP_MIN_RR_LIVE:
                logger.warning(
                    f"[adjust_sltp] {symbol} RR < {TP_MIN_RR_LIVE}: "
                    f"RR={rr_now:.2f}, sl_dist={sl_dist:.6g}, tp1_dist={tp1_dist:.6g},"
                    f"主动平仓"
                )
                # 调用 _force_close_static 平仓
                # 先从 pos_state 刷新 pos,确保 tp1/tp2_order_id 是最新值
                # (_replace_tp1 已 update_position,但传入的 pos dict 未同步)
                try:
                    from shared.state import get_all_positions as _gap
                    _matches = [p for p in (await asyncio.to_thread(_gap)) if p.get("pos_id") == pid]
                    if not _matches:
                        logger.info(f"[adjust_sltp] {symbol} RR平仓前发现仓位已关闭,跳过")
                        return {"closed": True, "reason": "already_closed"}
                    pos = _matches[0]  # 最新 pos,含最新 tp1/tp2_order_id
                except Exception as _fp_e:
                    logger.debug(f"[adjust_sltp] {symbol} 刷新 pos 失败,用原始 pos: {_fp_e}")
                await _force_close_static(pos, "rr_below_threshold")
                # 写 adjust log 后返回
                _write_adjust_log(pid, symbol, direction, "closed_rr_below_1",
                                  old_sl, new_sl, old_tp1, new_tp1,
                                  old_tp2, new_tp2, atr,
                                  new_params.get("consistency_score"),
                                  new_params.get("momentum_coeff"),
                                  sl_reason, tp1_reason)
                return {"closed": True, "reason": "rr_below_threshold", "rr": rr_now}

    result = {
        "sl_changed":  sl_changed,  "sl_reason":  sl_reason,
        "tp1_changed": tp1_changed, "tp1_reason": tp1_reason,
        "tp2_changed": tp2_changed,
    }
    _write_adjust_log(pid, symbol, direction,
                      "adjusted" if (sl_changed or tp1_changed or tp2_changed) else "no_change",
                      old_sl, new_sl, old_tp1, new_tp1,
                      old_tp2, new_tp2, atr,
                      new_params.get("consistency_score"),
                      new_params.get("momentum_coeff"),
                      sl_reason, tp1_reason)
    return result


def _write_adjust_log(pid, symbol, direction, action,
                      old_sl, new_sl, old_tp1, new_tp1,
                      old_tp2, new_tp2, atr,
                      consistency_score, momentum_coeff,
                      sl_reason, tp1_reason):
    """写 tp_sl_adjust_log.jsonl(每次调用都记录,含"维持不变")"""
    import json as _json, datetime, pathlib
    try:
        entry = {
            "ts":               datetime.datetime.utcnow().isoformat(),
            "pos_id":           pid,
            "symbol":           symbol,
            "direction":        direction,
            "action":           action,          # "adjusted" | "no_change"
            "atr":              round(atr, 8),
            # SL
            "old_sl":           round(old_sl, 8),
            "new_sl":           round(new_sl, 8),
            "sl_delta":         round(new_sl - old_sl, 8),
            "sl_delta_atr":     round((new_sl - old_sl) / (atr + 1e-9), 3),
            "sl_reason":        sl_reason,
            # TP1
            "old_tp1":          round(old_tp1, 8),
            "new_tp1":          round(new_tp1, 8),
            "tp1_delta":        round(new_tp1 - old_tp1, 8),
            "tp1_delta_atr":    round((new_tp1 - old_tp1) / (atr + 1e-9), 3),
            "tp1_reason":       tp1_reason,
            # TP2(可选)
            "old_tp2":          round(old_tp2, 8) if old_tp2 else None,
            "new_tp2":          round(new_tp2, 8) if new_tp2 else None,
            # 力度观测层
            "consistency_score": consistency_score,
            "momentum_coeff":    momentum_coeff,
        }
        log_path = pathlib.Path(__file__).parent / "logs" / "tp_sl_adjust_log.jsonl"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a") as f:
            f.write(_json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.debug(f"[adjust_sltp] 写日志失败: {e}")


# ── 每30分钟循环,遍历所有 open 仓位 ─────────────────────────────────────────
async def _adjust_sl_tp_loop_static():
    """静默 loop:每 TP_SL_ADJUST_INTERVAL_MIN 分钟对所有 open 仓位跑一次动态调整"""
    interval = TP_SL_ADJUST_INTERVAL_MIN * 60
    # 错开其他 loop 启动时间
    await asyncio.sleep(90)
    while True:
        try:
            positions = (await asyncio.to_thread(get_all_positions))
            open_pos  = [p for p in positions if p.get("status") == "open"]
            for pos in open_pos:
                sym = pos.get("symbol", "?")
                # liq_data_available=False 时,尝试重新拉清算数据
                # 开仓时可能清算图未就绪,但持仓期间已经可以拿到
                if not pos.get("liq_data_available", False):
                    try:
                        from shared.binance_api import get_mark_price as _gmp
                        from shared.liq_cache import get_liq_zones_cached as _lzc
                        _mp  = float((await asyncio.to_thread(_gmp, sym)).get("markPrice", 0))
                        _sig_type = pos.get("sig_type", pos.get("mode", "pullback"))
                        _rng = "24h" if _sig_type in ("reversal","funding_arb") else "3d"
                        _liq_l = _lzc(sym, "LONG",  _mp, range_=_rng)
                        _liq_s = _lzc(sym, "SHORT", _mp, range_=_rng)
                        if _liq_l or _liq_s:
                            # 清算数据已就绪,更新标记,继续调整
                            from shared.state import update_position as _upd
                            (await asyncio.to_thread(_upd, pos.get("pos_id", sym), {"liq_data_available": True}))
                            pos["liq_data_available"] = True
                            logger.info(f"[adjust_sltp] {sym} 清算数据已就绪,补标 liq_data_available=True")
                        else:
                            logger.debug(f"[adjust_sltp] {sym} 清算数据仍不可用,跳过")
                            await asyncio.sleep(1)
                            continue
                    except Exception as _e:
                        logger.debug(f"[adjust_sltp] {sym} 重拉清算数据失败: {_e}")
                        await asyncio.sleep(1)
                        continue
                try:
                    await _adjust_sl_tp_once(pos)
                except Exception as e:
                    logger.warning(f"[adjust_sltp] {sym} 异常: {e}")
                await asyncio.sleep(1)   # 每个标的间隔 1s,避免 API 连打
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"[adjust_sltp] loop 异常: {e}")
        await asyncio.sleep(interval)


# 绑定到 PositionManager(由 main.py 在 start() 后作为独立 task 创建)
PositionManager._maybe_daily_report = lambda self: _maybe_daily_report_static()
PositionManager.adjust_sl_tp_loop   = staticmethod(_adjust_sl_tp_loop_static)


# ══════════════════════════════════════════════════════════════════════════════
# funding_arb 专用巡检
# ══════════════════════════════════════════════════════════════════════════════
async def _check_arb_post_settle(pos: dict) -> None:
    """
    v3.37: 结算完成后5分钟内执行,重新评估费率,决定续持或平仓。
    支持 1h/4h/8h 不同结算周期的标的自动续持。
    """
    import time as _t
    from shared import binance_api as _bapi
    from shared.state import update_position as _upd_pos, remove_position as _rm_pos
    from shared.notifier import notify_async as _notify
    try:
        from config import FUNDING_ARB_CONTINUE_THRESH
    except ImportError:
        FUNDING_ARB_CONTINUE_THRESH = 0.005

    sym       = pos["symbol"]
    direction = pos.get("direction", "").upper()
    pid       = pos.get("pos_id", sym)
    now_ts    = _t.time()
    settle_cnt = pos.get("arb_settle_count", 1)

    try:
        mp = await asyncio.get_running_loop().run_in_executor(
            None, lambda: _bapi.get_mark_price(sym)
        )
        fr_now = float(mp.get("lastFundingRate", 0))
    except Exception as e:
        logger.warning(f"[arb_post_settle] {sym} 拉费率失败: {e}")
        return

    # 判断是否续持
    should_continue = False
    if direction == "LONG" and fr_now < -FUNDING_ARB_CONTINUE_THRESH:
        should_continue = True
    elif direction == "SHORT" and fr_now > FUNDING_ARB_CONTINUE_THRESH:
        should_continue = True

    if should_continue:
        # 计算下次结算时间
        interval_h = int(pos.get("funding_interval_h", 8))
        next_settle = _next_settle_ts(now_ts, interval_h=interval_h)
        (await asyncio.to_thread(_upd_pos, pid, {
            "cancel_before_ts":   next_settle - 300,
            "cooldown_end_ts":    next_settle + 300,
            "arb_settle_count":   settle_cnt + 1,
            "arb_post_check_done": False,   # 重置,供下次结算后再次评估
            "funding_arb_target_ts": next_settle,
            "funding_arb_hold_count": pos.get("funding_arb_hold_count", 0) + 1,
        }))
        logger.info(
            f"[arb_post_settle] {sym} {direction} 费率仍极端 {fr_now*100:+.4f}%,"
            f"续持至下次结算 interval={interval_h}h,已累计收费 {settle_cnt+1} 次"
        )
        await _notify(
            f"🔄 {sym} funding_arb 续持\n"
            f"费率 {fr_now*100:+.4f}%,已收 {settle_cnt+1} 次,等待下次结算"
        )
    else:
        # 费率收窄或反转,立刻平仓
        if ((direction == "LONG" and fr_now > 0)
                or (direction == "SHORT" and fr_now < 0)):
            reason = f"funding_rate_reversed({fr_now*100:+.4f}%)"
        else:
            reason = f"funding_rate_shrunk({fr_now*100:+.4f}%)"
        logger.info(
            f"[arb_post_settle] {sym} {direction} 费率不再极端 {fr_now*100:+.4f}%,"
            f"平仓锁定收益(已收 {settle_cnt} 次费率)"
        )
        # 触发平仓:更新 cancel_before_ts 为当前时间,让下次 patrol 触发平仓
        # 直接调用 _close 需要在 _patrol_funding_arb 的 closure 里,这里用状态标记
        (await asyncio.to_thread(_upd_pos, pid, {"arb_force_close_reason": reason}))


async def _patrol_funding_arb(pos: dict) -> None:
    """
    对单个 funding_arb 仓位执行四步巡检:
    1 止损  2 费率突变  3 结算后续持/退出  4 最大持有时间
    """
    import time as _t
    from shared import binance_api as api
    from shared.state import (update_position, remove_position,
                               inc_funding_arb_consec_sl, reset_funding_arb_consec_sl,
                               set_funding_arb_paused, add_cooldown)
    from shared.notifier import notify_async as _notify
    from config import (
        FUNDING_ARB_SL_ATR_MULT, FUNDING_ARB_COOLDOWN_AFTER_SL,
        FUNDING_ARB_CONSEC_SL_LIMIT,
        FUNDING_ARB_RATE_DECAY_THRESH,
        FUNDING_ARB_RSI4H_SHORT_MAX, FUNDING_ARB_RSI4H_LONG_MIN,
        FUNDING_ARB_OI_DROP_LIMIT,
    )

    symbol           = pos["symbol"]
    pid              = pos.get("pos_id", symbol)
    direction        = pos.get("direction", "").upper()
    sl_price         = float(pos.get("sl_price", 0))
    entry            = float(pos.get("fill_price", 0))
    qty              = float(pos.get("quantity", 0))
    fr_entry         = float(pos.get("funding_rate_at_entry", 0))
    target_ts        = float(pos.get("funding_arb_target_ts", 0))
    max_ts           = float(pos.get("funding_arb_max_hold_ts", 0))
    hold_cnt         = int(pos.get("funding_arb_hold_count", 0))
    cancel_before_ts = float(pos.get("cancel_before_ts", 0))   # 结算前5min撤单
    cooldown_end_ts  = float(pos.get("cooldown_end_ts", 0))     # 结算后30min冷却
    order_id         = pos.get("order_id") or pos.get("entry_order_id")  # 挂单ID
    is_pending       = pos.get("status") == "pending"           # 是否还是挂单状态
    now_ts           = _t.time()

    async def _close(reason: str, is_sl: bool = False):
        """市价平仓 + 写 trade_log + 更新 pos_state"""
        try:
            close_side = "SELL" if direction == "LONG" else "BUY"
            pos_side   = direction
            await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: api.place_order(
                    symbol=symbol, side=close_side,
                    order_type="MARKET", quantity=qty,
                    position_side=pos_side,
                )
            )
        except Exception as _e:
            logger.error(f"[arb_patrol] {symbol} 平仓失败: {_e}")
            return

        # 拉最新 mark_price 作为出场价
        try:
            mp = await asyncio.get_running_loop().run_in_executor(None, lambda: api.get_mark_price(symbol))
            exit_price = float(mp.get("markPrice", entry))
        except Exception:
            exit_price = entry

        fr_exit = await _get_current_fr(symbol)
        pnl = (exit_price - entry) * qty if direction == "LONG" else (entry - exit_price) * qty

        # 预估收取的资金费率(不含手续费)
        notional  = entry * qty
        fr_collected = notional * abs(fr_entry) * (hold_cnt + 1)

        _append_fa_trade_log(pos, exit_price, fr_exit, reason, hold_cnt, fr_collected, pnl)
        (await asyncio.to_thread(_remove_position_checked, pid))

        if is_sl:
            consec = inc_funding_arb_consec_sl()
            add_cooldown(symbol, FUNDING_ARB_COOLDOWN_AFTER_SL * 3600, "funding_arb_sl")
            if consec >= FUNDING_ARB_CONSEC_SL_LIMIT:
                set_funding_arb_paused(24)
                await _notify(f"⛔ funding_arb 连续止损{consec}次,模块暂停24小时")
        else:
            reset_funding_arb_consec_sl()

        await _notify(
            f"{'🟢' if pnl >= 0 else '🔴'} {symbol} funding_arb 平仓\n"
            f"原因: {reason}  PnL≈{pnl:+.2f}U  费率估收: {fr_collected:.4f}U"
        )
        logger.info(f"[arb_patrol] {symbol} 平仓 reason={reason} pnl={pnl:.2f}U")

    # ── 0-A 挂单状态:结算前5min未成交 → 撤单 ───────────────────────────────
    if is_pending and cancel_before_ts > 0 and now_ts >= cancel_before_ts:
        # 撤销挂单
        if order_id:
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: api.cancel_order(symbol, int(order_id))
                )
                logger.info(f"[arb_patrol] {symbol} 结算前5min未成交,已撤单 {order_id}")
            except Exception as _ce:
                logger.warning(f"[arb_patrol] {symbol} 撤单失败: {_ce}")
        (await asyncio.to_thread(_remove_position_checked, pid))
        await _notify(f"⏱ {symbol} funding_arb 挂单在结算前5min未成交,已撤单,冷却30min后重新评估")
        return

    # ── 0-B v3.37 结算后动态续持评估(冷却结束后立刻执行,不等下次30min巡检)───────
    _post_check_done = pos.get("arb_post_check_done", False)
    if (cooldown_end_ts > 0 and now_ts >= cooldown_end_ts
            and not is_pending and not _post_check_done):
        # 标记已执行,防止重复
        from shared.state import update_position as _upd_pos
        (await asyncio.to_thread(_upd_pos, pid, {"arb_post_check_done": True}))
        pos["arb_post_check_done"] = True
        await _check_arb_post_settle(pos)

        return  # _check_arb_post_settle 内部会决定续持或平仓

    # ── v3.37 强制平仓标记(_check_arb_post_settle 设置)──────────────────────────
    _force_close_reason = pos.get("arb_force_close_reason")
    if _force_close_reason:
        from shared.state import update_position as _upd_force
        (await asyncio.to_thread(_upd_force, pid, {"arb_force_close_reason": None}))
        await _close(_force_close_reason); return

    # ── 1 止损检查 ──────────────────────────────────────────────────────────
    try:
        mp = await asyncio.get_running_loop().run_in_executor(None, lambda: api.get_mark_price(symbol))
        mark = float(mp.get("markPrice", 0))
    except Exception:
        return   # 拉不到价格,本轮跳过

    if mark <= 0:
        return

    if is_pending:
        return   # 还是挂单状态,不做止损检查

    if direction == "LONG" and mark <= sl_price:
        await _close("sl_hit", is_sl=True); return
    if direction == "SHORT" and mark >= sl_price:
        await _close("sl_hit", is_sl=True); return

    # ── 2 费率突变监控 ──────────────────────────────────────────────────────
    fr_now = await _get_current_fr(symbol)
    if fr_now is not None and fr_entry != 0:
        decay = (fr_entry - fr_now) / (abs(fr_entry) + 1e-9)
        if direction == "SHORT":
            if fr_now < -0.0002 or decay > FUNDING_ARB_RATE_DECAY_THRESH:
                await _close("rate_reversed"); return
        else:
            if fr_now > 0.0002 or decay < -FUNDING_ARB_RATE_DECAY_THRESH:
                await _close("rate_reversed"); return

    # ── 4 最大持有时间(优先于结算评估)────────────────────────────────────
    if max_ts > 0 and now_ts >= max_ts:
        await _close("max_hold"); return

    # ── 3 结算后续持/退出评估 ───────────────────────────────────────────────
    if target_ts > 0 and now_ts >= target_ts + 1800:   # 给30分钟缓冲确认结算完成
        if fr_now is None:
            return

        if direction == "SHORT":
            if fr_now < 0.0002:
                await _close("rate_reversed"); return
            if fr_now < 0.0005:
                await _close("rate_normalized"); return
            # 费率仍高,考虑续持
            if hold_cnt < 1:
                try:
                    from shared import binance_api as _bapi
                    all_mp = await asyncio.get_running_loop().run_in_executor(None, _bapi.get_all_mark_prices)
                    oi_now_v = float((await asyncio.to_thread(_bapi.get_open_interest, symbol)).get("openInterest", 0))
                    oi_hist  = (await asyncio.to_thread(_bapi.get_open_interest_hist, symbol, "1h", 2))
                    oi_1h    = float(oi_hist[0].get("sumOpenInterest", 0)) if oi_hist else oi_now_v
                    oi_drop  = (oi_1h - oi_now_v) / (oi_1h + 1e-9) if oi_1h > 0 else 0
                    k4h_data = (await asyncio.to_thread(_bapi.get_klines, symbol, "4h", 20))
                    c4h = [float(k[4]) for k in k4h_data[:-1]] if k4h_data else []
                    rsi_4h_now = 50.0
                    if len(c4h) >= 15:
                        from shared.indicators import rsi as _rsi_fn
                        rsi_4h_now = _rsi_fn(c4h)
                except Exception:
                    oi_drop = 0; rsi_4h_now = 65

                renew_ok = (rsi_4h_now <= FUNDING_ARB_RSI4H_SHORT_MAX
                            and oi_drop <= FUNDING_ARB_OI_DROP_LIMIT)
                if renew_ok:
                    # 计算下次结算时间
                    next_ts = _next_settle_ts(now_ts, interval_h=int(pos.get("funding_interval_h", 8)))
                    (await asyncio.to_thread(update_position, pid, {
                        "funding_arb_target_ts": next_ts,
                        "funding_arb_hold_count": hold_cnt + 1,
                    }))
                    logger.info(f"[arb_patrol] {symbol} 续持到 next_settle={next_ts:.0f}")
                else:
                    await _close("max_renew"); return
            else:
                await _close("max_renew"); return

        else:   # LONG 镜像
            if fr_now > 0.0002:
                await _close("rate_reversed"); return
            if fr_now > -0.0005:
                await _close("rate_normalized"); return
            if hold_cnt < 1:
                try:
                    from shared import binance_api as _bapi
                    oi_now_v = float((await asyncio.to_thread(_bapi.get_open_interest, symbol)).get("openInterest", 0))
                    oi_hist  = (await asyncio.to_thread(_bapi.get_open_interest_hist, symbol, "1h", 2))
                    oi_1h    = float(oi_hist[0].get("sumOpenInterest", 0)) if oi_hist else oi_now_v
                    oi_drop  = (oi_1h - oi_now_v) / (oi_1h + 1e-9) if oi_1h > 0 else 0
                    k4h_data = (await asyncio.to_thread(_bapi.get_klines, symbol, "4h", 20))
                    c4h = [float(k[4]) for k in k4h_data[:-1]] if k4h_data else []
                    rsi_4h_now = 50.0
                    if len(c4h) >= 15:
                        from shared.indicators import rsi as _rsi_fn
                        rsi_4h_now = _rsi_fn(c4h)
                except Exception:
                    oi_drop = 0; rsi_4h_now = 35

                renew_ok = (rsi_4h_now >= FUNDING_ARB_RSI4H_LONG_MIN
                            and oi_drop <= FUNDING_ARB_OI_DROP_LIMIT)
                if renew_ok:
                    next_ts = _next_settle_ts(now_ts, interval_h=int(pos.get("funding_interval_h", 8)))
                    (await asyncio.to_thread(update_position, pid, {
                        "funding_arb_target_ts": next_ts,
                        "funding_arb_hold_count": hold_cnt + 1,
                    }))
                else:
                    await _close("max_renew"); return
            else:
                await _close("max_renew"); return


async def _get_current_fr(symbol: str) -> float | None:
    """异步拉当前资金费率"""
    try:
        from shared import binance_api as _bapi
        mp = await asyncio.get_running_loop().run_in_executor(None, lambda: _bapi.get_mark_price(symbol))
        return float(mp.get("lastFundingRate", 0))
    except Exception:
        return None


def _next_settle_ts(now_ts: float, interval_h: int = 8) -> float:
    """计算下一次资金费率结算时间戳,支持 1H/4H/8H 周期(v3.39)
    Binance 永续合约结算周期可以是 1H/4H/8H,默认 8H(UTC 00/08/16)
    """
    interval_sec = interval_h * 3600
    slot = int(now_ts / interval_sec)
    return (slot + 1) * interval_sec


def _append_fa_trade_log(pos: dict, exit_price: float, fr_exit: float | None,
                          reason: str, hold_cnt: int, fr_collected: float, pnl: float) -> None:
    """写入 funding_arb 平仓记录到 trade_log.jsonl"""
    import time as _t, json as _j
    try:
        from pathlib import Path as _P
        log_path = _P(__file__).parent / "logs" / "trade_log.jsonl"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "trade_id":              f"{int(_t.time()*1000)}_{pos['symbol']}",
            "symbol":                pos["symbol"],
            "direction":             pos.get("direction", ""),
            "mode":                  "funding_arb",
            "sig_type":              "funding_arb",
            "entry_price":           float(pos.get("fill_price", 0)),
            "exit_price":            exit_price,
            "quantity":              float(pos.get("quantity", 0)),
            "pnl_usdt":              round(pnl, 4),
            "open_time":             float(pos.get("open_time", 0)),
            "close_time":            _t.time() * 1000,
            "close_reason":          reason,
            "exit_reason":           reason,
            "funding_rate_at_entry": float(pos.get("funding_rate_at_entry", 0)),
            "funding_rate_at_exit":  fr_exit,
            "funding_arb_hold_count": hold_cnt,
            "funding_collected_est": round(fr_collected, 6),
        }
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(_j.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as _e:
        logger.warning(f"[arb_patrol] trade_log 写入失败: {_e}")


# ══════════════════════════════════════════════════════════════════════════════
# 挂单健康检查(B4,每30分钟,由 _patrol_loop 调用)
# ══════════════════════════════════════════════════════════════════════════════
async def _check_pending_health_static(pos: dict) -> None:
    """
    对单个 pending 挂单做健康检查:
    1. 信号是否仍有效 → 无效则撤单
    2. 入场价是否需要调整(变化 >= 0.2ATR)→ 撤单重挂
    3. 调整次数达上限(3次)→ 直接撤单
    """
    import time as _t
    from shared import binance_api as _api
    from shared.state import update_position, remove_position
    from shared.notifier import notify_async as _notify
    from shared.liq_cache import get_liq_zones_cached
    from shared.calc_sl_tp import calc_sl_tp

    try:
        from config import (
            PENDING_HEALTH_MIN_HOLD_MIN, PENDING_HEALTH_ENTRY_ADJUST_ATR,
            PENDING_HEALTH_MAX_ADJUST_COUNT, PENDING_HEALTH_LOG,
            BREAKOUT_ORDER_EXPIRE_MIN,
        )
    except Exception as _ce:
        logger.warning(f"[pending_health] config import 失败: {_ce}")
        return

    symbol    = pos.get("symbol", "")
    pid       = pos.get("pos_id", symbol)
    mode      = pos.get("mode", "pullback")
    direction = (pos.get("direction") or "long").upper()
    open_ms   = int(pos.get("open_time", 0))
    hold_min  = (_t.time() * 1000 - open_ms) / 60000
    adj_count = int(pos.get("pending_health_check_count", 0))

    # 基础过滤
    if hold_min < PENDING_HEALTH_MIN_HOLD_MIN:
        return
    if mode == "funding_arb":
        return   # funding_arb 是市价单,不应有 pending 状态,跳过

    # 调整次数上限 → 直接撤单
    if adj_count >= PENDING_HEALTH_MAX_ADJUST_COUNT:
        await _pending_cancel(pos, "adjust_limit_reached", pid, symbol, _api, remove_position, _notify)
        _write_pending_adj_log(symbol, mode, "cancelled", "adjust_limit_reached",
                                pos, None, adj_count, None, None, None, PENDING_HEALTH_LOG)
        return

    # ── 拉市场数据 ────────────────────────────────────────────────────────────
    try:
        k1h_raw  = _pm_klines(symbol, "1h", 60)   # EMA50 需至少 50 根,60 有余量
        k15_raw  = _pm_klines(symbol, "15m", 20)
        mp_data  = (await asyncio.to_thread(_pm_mark_price, symbol))
        now_ms_v = _t.time() * 1000
        k1h = [k for k in k1h_raw if float(k[6]) < now_ms_v]
        k15 = [k for k in k15_raw if float(k[6]) < now_ms_v]
        if len(k1h) < 14:
            return
        mark_price = float(mp_data.get("markPrice", 0))
        funding    = float(mp_data.get("lastFundingRate", 0))
    except Exception as _e:
        logger.warning(f"[pending_health] {symbol} 数据拉取失败: {_e}")
        return

    # 计算指标
    closes_1h = [float(k[4]) for k in k1h]
    atr       = _calc_atr_from_klines(k1h)
    ema20_1h  = _calc_ema(closes_1h, 20)
    ema50_1h  = _calc_ema(closes_1h, 50)
    rsi_1h    = _calc_rsi(closes_1h)

    # ── 信号有效性检查 ────────────────────────────────────────────────────────
    cancel_reason = None

    if mode == "pullback":
        # 1 价格有效性检查
        # LONG  - 价格跌破 entry 下方超过 0.5ATR:回踩过深,入场点失效
        # SHORT - 价格涨破 entry 上方超过 0.5ATR:反弹过深,入场点失效
        # SHORT - 价格已跌破 entry 下方超过 1.0ATR:回踩窗口已过,等待反弹到 entry 风险收益比变差
        # LONG  - 价格已涨破 entry 上方超过 1.0ATR:回踩窗口已过,同理
        old_entry = float(pos.get("entry_price", mark_price))
        if direction == "LONG":
            if mark_price < old_entry - atr * 0.5:
                cancel_reason = f"价格{mark_price:.4g}已跌破挂单价{old_entry:.4g}下方0.5ATR,回踩过深"
            elif mark_price > old_entry + atr * 1.0:
                cancel_reason = f"价格{mark_price:.4g}已涨超挂单价{old_entry:.4g}上方1ATR,回踩窗口已过"
        else:  # SHORT
            if mark_price > old_entry + atr * 0.5:
                cancel_reason = f"价格{mark_price:.4g}已涨破挂单价{old_entry:.4g}上方0.5ATR,反弹过深"
            elif mark_price < old_entry - atr * 1.0:
                cancel_reason = f"价格{mark_price:.4g}已跌破挂单价{old_entry:.4g}下方1ATR,超跌回踩窗口已过"
        # 2 EMA 结构
        if not cancel_reason:
            if direction == "LONG" and ema20_1h < ema50_1h:
                cancel_reason = "EMA多头结构已失效"
            elif direction == "SHORT" and ema20_1h > ema50_1h:
                cancel_reason = "EMA空头结构已失效"
        # 3 RSI 合理区间
        if not cancel_reason:
            if rsi_1h < 28 or rsi_1h > 72:
                cancel_reason = f"RSI1H={rsi_1h:.1f}超出合理区间[28,72]"
        # 4 资金费率
        if not cancel_reason:
            if direction == "LONG" and funding > 0.002:
                cancel_reason = f"资金费率{funding*100:.4f}%过高,多头成本大"
            elif direction == "SHORT" and funding < -0.002:
                cancel_reason = f"资金费率{funding*100:.4f}%极端负,空头被挤压"

    elif mode == "reversal":
        # 与 _survival_check 对齐:LONG 门槛 45(触发 RSI<28,反弹到 45 说明超卖消退)
        #                          SHORT 门槛 60(触发 RSI>72,回落到 60 说明超买消退)
        if direction == "LONG" and rsi_1h >= 45:
            cancel_reason = f"RSI1H={rsi_1h:.1f}>=45,超卖消退,反弹窗口关闭"
        elif direction == "SHORT" and rsi_1h <= 60:
            cancel_reason = f"RSI1H={rsi_1h:.1f}<=60,超买消退,回落窗口关闭"
        if not cancel_reason:
            if direction == "LONG" and funding > 0.001:
                cancel_reason = f"资金费率{funding*100:.4f}%,多头拥挤"
            elif direction == "SHORT" and funding < -0.002:
                cancel_reason = f"资金费率{funding*100:.4f}%,空头拥挤"

    elif mode == "breakout":
        # breakout 已有 _check_breakout_fail,这里只做二次确认
        ref = float(pos.get("breakout_ref_price") or 0)
        if ref > 0:
            if direction == "LONG" and mark_price < ref * 0.99:
                cancel_reason = f"价格{mark_price:.4g}已跌破突破位{ref:.4g}"
            elif direction == "SHORT" and mark_price > ref * 1.01:
                cancel_reason = f"价格{mark_price:.4g}已涨破突破位{ref:.4g}"

    elif mode == "oi_divergence":
        # OI 背离:挂单在回落位等待,若价格走反超过 1 ATR 则背离窗口关闭
        old_entry = float(pos.get("entry_price", mark_price))
        if direction == "LONG" and mark_price < old_entry - atr * 1.0:
            cancel_reason = f"oi_divergence LONG 价格走反超1ATR: {mark_price:.4g} < {old_entry - atr:.4g}"
        elif direction == "SHORT" and mark_price > old_entry + atr * 1.0:
            cancel_reason = f"oi_divergence SHORT 价格走反超1ATR: {mark_price:.4g} > {old_entry + atr:.4g}"
        # RSI 方向验证:OI 背离 LONG 要求 RSI 不过热
        if not cancel_reason:
            if direction == "LONG" and rsi_1h > 60:
                cancel_reason = f"oi_divergence LONG RSI1H={rsi_1h:.1f}>60,动能转向"
            elif direction == "SHORT" and rsi_1h < 40:
                cancel_reason = f"oi_divergence SHORT RSI1H={rsi_1h:.1f}<40,动能转向"

    elif mode == "ema_trend":
        # EMA 趋势跟随:价格跌破 EMA20(多头)或涨破 EMA20(空头)时趋势结构失效
        if direction == "LONG" and ema20_1h > 0 and mark_price < ema20_1h - atr * 0.5:
            cancel_reason = f"ema_trend LONG 价格跌破 EMA20-0.5ATR={ema20_1h - atr*0.5:.4g}"
        elif direction == "SHORT" and ema20_1h > 0 and mark_price > ema20_1h + atr * 0.5:
            cancel_reason = f"ema_trend SHORT 价格涨破 EMA20+0.5ATR={ema20_1h + atr*0.5:.4g}"
        # EMA 排列:4H 趋势方向翻转则撤单(ema50_1h 已计算,用 1H 替代简化)
        if not cancel_reason:
            if direction == "LONG" and ema20_1h < ema50_1h:
                cancel_reason = f"ema_trend LONG EMA多头排列失效 (EMA20={ema20_1h:.4g} < EMA50={ema50_1h:.4g})"
            elif direction == "SHORT" and ema20_1h > ema50_1h:
                cancel_reason = f"ema_trend SHORT EMA空头排列失效 (EMA20={ema20_1h:.4g} > EMA50={ema50_1h:.4g})"

    # 信号失效 → 撤单
    if cancel_reason:
        await _pending_cancel(pos, cancel_reason, pid, symbol, _api, remove_position, _notify)
        _write_pending_adj_log(symbol, mode, "cancelled", cancel_reason,
                                pos, None, adj_count, atr, rsi_1h, funding,
                                PENDING_HEALTH_LOG)
        return

    # ── 入场价调整评估 ────────────────────────────────────────────────────────
    try:
        from executor import _normalize_liq_zones
        _phc_range = "24h" if pos.get("sig_type") in ("reversal", "funding_arb", "oi_divergence") else "3d"
        liq_long  = _normalize_liq_zones(get_liq_zones_cached(symbol, "LONG",  mark_price, range_=_phc_range))
        liq_short = _normalize_liq_zones(get_liq_zones_cached(symbol, "SHORT", mark_price, range_=_phc_range))
        liq_zones = {"long": liq_long, "short": liq_short}
    except Exception:
        liq_zones = {}

    sig_copy = dict(pos)
    sig_copy["mode"]      = mode
    sig_copy["direction"] = direction
    sig_copy["symbol"]    = symbol
    new_params = calc_sl_tp(sig_copy, mark_price, atr, liq_zones, k1h, ema20_1h)
    if "error" in new_params:
        logger.debug(f"[pending_health] {symbol} calc_sl_tp 失败: {new_params['error']}")
        return

    old_entry = float(pos.get("entry_price", mark_price))
    new_entry = new_params["entry_price"]
    entry_delta = abs(new_entry - old_entry)

    if entry_delta < PENDING_HEALTH_ENTRY_ADJUST_ATR * atr:
        return   # 变化太小,不值得重挂

    # ── 撤旧单,重新提交 ────────────────────────────────────────────────────────
    old_order_id = pos.get("open_order_id")
    if old_order_id:
        try:
            (await asyncio.to_thread(_api.cancel_order, symbol, old_order_id))
        except Exception as _ce:
            logger.warning(f"[pending_health] {symbol} 撤旧单失败: {_ce}")

    # 确定 timeInForce 和 expire_ts
    if mode == "breakout":
        tif = "GTD"
        orig_expire = int(pos.get("expire_ts") or 0)
        expire_ts   = orig_expire if orig_expire > _t.time() * 1000 else int((_t.time() + BREAKOUT_ORDER_EXPIRE_MIN * 60) * 1000)
    elif mode == "reversal" and pos.get("entry_mode") == "pending_order":
        tif = "GTX"
        expire_ts = None
    else:
        tif = "GTC"
        expire_ts = None

    # 精度处理
    try:
        prec = (await asyncio.to_thread(_api.get_precision, symbol))
        tick = prec["tick"]; step = prec["step"]
    except Exception:
        tick = 0.0001; step = 0.001

    from executor import _round_price, _round_qty
    price_str = str(_round_price(new_entry, tick))
    qty_str   = str(_round_qty(float(pos.get("quantity", 0)), step))
    side      = "BUY" if direction == "LONG" else "SELL"

    try:
        order_kwargs = dict(
            symbol=symbol, side=side,
            order_type="LIMIT",
            quantity=float(qty_str),
            price=float(price_str),
            timeInForce=tif,
        )
        if tif == "GTD" and expire_ts:
            order_kwargs["goodTillDate"] = expire_ts
        new_order = (await asyncio.to_thread(_api.place_order, **order_kwargs))
        new_oid   = new_order.get("orderId")
    except Exception as _oe:
        logger.warning(f"[pending_health] {symbol} 重新下单失败: {_oe}")
        return

    # 更新 pos_state(v3.72: 标记为重挂单,保护不被竞争替换)
    (await asyncio.to_thread(update_position, pid, {
        "entry_price":               new_entry,
        "sl_price":                  new_params["sl_price"],
        "tp1_price":                 new_params["tp1_price"],
        "tp2_price":                 new_params["tp2_price"],
        "open_order_id":             new_oid,
        "pending_health_check_count": adj_count + 1,
        "is_reorder":                True,   # 重挂单标记,signal_watcher 竞争机制保护
        "reorder_count":             adj_count + 1,
    }))

    _write_pending_adj_log(symbol, mode, "price_adjusted", None,
                            pos, new_params, adj_count + 1, atr, rsi_1h, funding,
                            PENDING_HEALTH_LOG)
    await _notify(f"🔄 {symbol} 挂单价调整 {old_entry:.4g}→{new_entry:.4g} (第{adj_count+1}次)")
    logger.info(f"[pending_health] {symbol} 挂单调整 old={old_entry:.4g} new={new_entry:.4g} δ={entry_delta:.4g}")


async def _pending_cancel(pos: dict, reason: str, pid: str, symbol: str,
                           _api, remove_position, _notify) -> None:
    """撤销挂单并清除 pos_state 记录"""
    oid = pos.get("open_order_id")
    if oid:
        try:
            (await asyncio.to_thread(_api.cancel_order, symbol, oid))
        except Exception as _ce:
            logger.warning(f"[pending_health] {symbol} 撤单失败(忽略): {_ce}")
    # [v3.89 remove-pos 检查] 检查返回值；失败只记日志，不影响撤单后的流程
    try:
        if (await asyncio.to_thread(remove_position, pid)) is False:
            logger.warning(f"[pm] remove_position({symbol}) 返回 False，pos_state 未写入成功，请人工核查")
    except Exception as _re:
        logger.error(f"[pm] remove_position({symbol}) 抛异常，持仓文件可能损坏，请人工核查: {_re}")
    await _notify(f"🗑️ {symbol} 挂单已撤销 原因: {reason}")
    logger.info(f"[pending_health] {symbol} 挂单撤销 reason={reason}")

    # ── v3.63b P1:幽灵限价单统计(missed 打标)──────────────────────────────
    # oi_divergence 采用 pending_order 模式,信号触发但价格未继续回落导致挂单到期。
    # 这类"触发但未成交"的信号需要单独统计,以评估"幽灵信号率":
    #   若 > 30%,说明入场价设置过深或触发条件时机过早。
    # 写入 trade_log.jsonl,close_reason="missed_unfilled",pnl=0,供复盘分析。
    mode = pos.get("mode", "")
    sig_type = pos.get("sig_type", mode)
    if sig_type == "oi_divergence" or mode == "oi_divergence":
        try:
            import json as _json, time as _t
            from pathlib import Path as _P
            from datetime import datetime, timezone
            _log_path = _P(__file__).parent / "logs" / "trade_log.jsonl"
            _log_path.parent.mkdir(parents=True, exist_ok=True)
            _missed_entry = {
                "ts":           datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                "pos_id":       pid,
                "symbol":       symbol,
                "direction":    pos.get("direction", ""),
                "mode":         mode,
                "sig_type":     sig_type,
                "entry_price":  pos.get("entry_price"),
                "sl_price":     pos.get("sl_price"),
                "tp1_price":    pos.get("tp1_price"),
                "close_reason": "missed_unfilled",
                "cancel_reason": reason,
                "pnl_usdt":     0.0,
                "pnl_pct":      0.0,
                "open_time":    pos.get("open_time"),
                "close_time":   int(_t.time() * 1000),
                "liq_data_available": pos.get("liq_data_available", False),
                "sl_degraded":  pos.get("sl_degraded", False),
                "signal_strength": pos.get("signal_strength", "medium"),
            }
            from shared.trade_log_io import append_jsonl as _append_jsonl   # v3.93
            await asyncio.to_thread(_append_jsonl, _log_path, _missed_entry, None)
            logger.info(
                f"[pending_health] {symbol} oi_div 幽灵限价单记录: "
                f"entry={pos.get('entry_price')} reason={reason}"
            )
        except Exception as _me:
            logger.debug(f"[pending_health] missed 记录写入失败(跳过): {_me}")


def _write_pending_adj_log(
    symbol: str, mode: str, action: str, cancel_reason: str | None,
    old_pos: dict, new_params: dict | None, adj_count: int,
    atr: float | None, rsi_1h: float | None, funding: float | None,
    log_path,
) -> None:
    """写 pending_adjust_log.jsonl"""
    import time as _t, json as _j
    from datetime import datetime, timezone
    try:
        from pathlib import Path as _P
        log_path = _P(str(log_path))
        log_path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "ts":              datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "symbol":          symbol,
            "mode":            mode,
            "action":          action,
            "cancel_reason":   cancel_reason,
            "old_entry":       old_pos.get("entry_price"),
            "new_entry":       new_params.get("entry_price") if new_params else None,
            "old_sl":          old_pos.get("sl_price"),
            "new_sl":          new_params.get("sl_price") if new_params else None,
            "old_tp1":         old_pos.get("tp1_price"),
            "new_tp1":         new_params.get("tp1_price") if new_params else None,
            "entry_delta":     round(
                abs((new_params.get("entry_price", 0) if new_params else 0) - float(old_pos.get("entry_price", 0))), 6
            ) if new_params else None,
            "entry_delta_atr": round(
                abs((new_params.get("entry_price", 0) if new_params else 0) - float(old_pos.get("entry_price", 0))) / (atr + 1e-9), 3
            ) if new_params and atr else None,
            "adjust_count":    adj_count,
            "atr":             atr,
            "rsi_1h":          round(rsi_1h, 2) if rsi_1h is not None else None,
            "funding":         funding,
        }
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(_j.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as _e:
        logger.warning(f"[pending_health] 日志写入失败: {_e}")
