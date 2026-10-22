"""
fx/executor.py — 开仓执行器（完整版 V3）

主流程（11步）：
  1. 信号有效性预检
  2. 拉取最新市场数据
  3. 拉取 Coinglass 清算图
  4. calc_sl_tp 计算入场参数
  5. 账户状态终检（可用余额 + 总名义仓位上限）
  6. 查询并设置最大杠杆 + 全仓模式
  7. 提交开仓限价单
  8. WebSocket 监听开仓成交回报
  9. 成交后毫秒级并发提交止损 + TP1 + TP2
 10. WebSocket 监听止损止盈，一触发立刻撤另一个
 11. 写入持仓状态 + 通知

设计原则：
- 所有入场价实时计算（不复用 scanner 缓存价格）
- 全仓模式，每标的使用最大允许杠杆
- 止盈限价单，止损条件市价单，WebSocket 联动撤单
- TP1 成交后撤旧止损重挂（保护剩余40%），激活移动止损
- reduceOnly = True 强制要求
- 价格/数量精度由 exchange_info 缓存统一处理
"""

from __future__ import annotations
from shared.indicators import atr as _calc_atr, ema as _calc_ema, rsi as _calc_rsi
import asyncio
import json
import logging
import math
import os
import random
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent))

from config import (
    RISK_MAX_POSITIONS, RISK_MAX_LONG, RISK_MAX_SHORT,
    TP1_CLOSE_RATIO, TP2_CLOSE_RATIO,
    BREAKOUT_ORDER_EXPIRE_MIN,
    MIN_NOTIONAL_GUARD,
    DEFAULT_LEVERAGE,
    FUNDING_ARB_MAX_HOLD_HOURS,
)
from shared import binance_api as api
from shared.calc_sl_tp import calc_sl_tp
from shared.risk_manager import is_circuit_open, check_position_limits
from shared.state import (
    get_all_positions, add_position, update_position,
    is_in_cooldown, get_pending_orders,
)
from shared.notifier import (
    notify, notify_async, card_append, card_append_async, card_remove,
    flush_signal, buffer_signal,
    _hhmm, _CLOSE_REASON_CN, fmt_hold_time,
    fmt_node_immediate_close, fmt_node_open, fmt_node_close,
)

logger = logging.getLogger("fx.executor")


class PreOrderError(Exception):
    """
    [v3.89 #102] 开仓前的暂时性失败：确认还没有向交易所提交过任何订单，可安全重试。
    仅用于市场数据拉取失败 / set_leverage 失败 / 账户查询失败这类会自行恢复的故障。
    业务性拒绝（_precheck 拒绝、calc_sl_tp 校验不通过、余额不足、持仓上限、冷却期等）
    仍然 return False，不作为可重试错误——重试不会改变结果，只会持续空转。
    """

# 保证金状态全局缓存（由 _account_final_check 写入，开仓逻辑读取）
_MARGIN_TIGHT_HINT: list = [""]   # [0]=提示文字
_MARGIN_SCALE:      list = [1.0]   # [0]=仓位缩减系数（0.3–1.0）

# ─────────────────────────────────────────────────────────────────────────────
# 精度缓存（启动时一次性拉取，10分钟刷新）
# ─────────────────────────────────────────────────────────────────────────────
_PRECISION_CACHE: dict[str, dict] = {}    # symbol → {"tick": float, "step": float}
_PRECISION_TS: float = 0.0
_PRECISION_TTL: float = 600.0             # 10 分钟

_LEVERAGE_CACHE: dict[str, int] = {}      # symbol → max_leverage
_LEVERAGE_TS: dict[str, float] = {}       # symbol → last_fetch_ts
_LEVERAGE_TTL: float = 600.0


def _refresh_precision_cache() -> None:
    global _PRECISION_TS
    try:
        info = api.get_exchange_info()
        for sym in info.get("symbols", []):
            s = sym["symbol"]
            tick = step = None
            for f in sym.get("filters", []):
                if f["filterType"] == "PRICE_FILTER":
                    tick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
            if tick and step:
                _PRECISION_CACHE[s] = {"tick": tick, "step": step}
        _PRECISION_TS = time.time()
        logger.info(f"[executor] 精度缓存刷新，共 {len(_PRECISION_CACHE)} 个标的")
    except Exception as e:
        logger.warning(f"[executor] 精度缓存刷新失败: {e}")


class PrecisionUnavailableError(RuntimeError):
    """[fix #25] 交易所精度（tickSize/stepSize）获取失败。"""


def _get_precision(symbol: str) -> dict:
    if time.time() - _PRECISION_TS > _PRECISION_TTL or symbol not in _PRECISION_CACHE:
        _refresh_precision_cache()
    prec = _PRECISION_CACHE.get(symbol)
    if not prec or not prec.get("tick") or not prec.get("step"):
        # [fix #25] 取不到精度不再用默认值（0.01/0.001 会让价格/数量错位被拒或下错价）→ 拒绝下单
        raise PrecisionUnavailableError(f"{symbol} 精度信息不可用（tick/step），拒绝下单")
    return prec


def _round_price(price: float, tick: float) -> float:
    if tick <= 0:
        return round(price, 6)
    prec = max(0, -int(math.floor(math.log10(tick))))
    # [v3.88 OPT-16] 浮点修复：原写法 floor(price/tick) 受浮点误差影响（0.3/0.1=2.9999999999999996，floor 得 2，价格少一个 tick）。
    #   先 round(price/tick, 10) 消除误差再向下取整，与 _round_qty 的修法一致；正常价格结果不变。
    return round(math.floor(round(price / tick, 10)) * tick, prec)


def _round_sum_qty(a: float, b: float, step: float) -> float:
    """[v3.87 STEP6-3] 两个已按步长取整的数量相加后，按步长精度四舍五入，消除浮点噪声（不做向下取整，避免丢一个步长）。"""
    if step <= 0:
        return round(a + b, 3)
    prec = max(0, -int(math.floor(math.log10(step))))
    return round(a + b, prec)


def _round_qty(qty: float, step: float) -> float:
    if step <= 0:
        return round(qty, 3)
    prec = max(0, -int(math.floor(math.log10(step))))
    # [v3.87 STEP6-5] 原写法 floor(qty/step)*step：浮点除法会多丢一个步长（0.3/0.1=2.9999999999999996，floor 得 2，数量变成 0.2）。
    #   现在先 round(qty/step, 10) 消除浮点噪声，再 floor 保证不超过实际数量。影响开仓数量和止盈数量。
    return round(math.floor(round(qty / step, 10)) * step, prec)


def _get_max_leverage(symbol: str) -> int:
    now = time.time()
    if symbol in _LEVERAGE_CACHE and now - _LEVERAGE_TS.get(symbol, 0) < _LEVERAGE_TTL:
        return _LEVERAGE_CACHE[symbol]
    try:
        brackets = api.get_leverage_brackets(symbol)
        max_lev = int(brackets[0]["initialLeverage"])
        _LEVERAGE_CACHE[symbol]  = max_lev
        _LEVERAGE_TS[symbol]     = now
        return max_lev
    except Exception as e:
        logger.warning(f"[executor] {symbol} 查询最大杠杆失败: {e}，使用默认10")
        return 10


# ─────────────────────────────────────────────────────────────────────────────
# 步骤1：信号有效性预检
# ─────────────────────────────────────────────────────────────────────────────
def _check_correlation(symbol: str, direction: str, open_pos: list) -> tuple[bool, str, float]:
    """
    相关性过滤（已简化：不再逐标的拉K线计算皮尔逊，改用槽位规则）。
    规则：同方向已持仓 >= 3 个时拒绝，避免同向集中风险。
    返回 (rejected, reason, dummy_corr)
    """
    # DIR_CAP 静态检查已移除：_precheck 里的 check_position_limits 已做动态上限，
    # 此处保留函数签名供调用方兼容，直接返回不拒绝。
    return False, "", 0.0


def _precheck(signal: dict) -> tuple[bool, str]:
    symbol    = signal["symbol"]
    direction = signal["direction"].upper()

    # 1-1: 该标的是否已有机器人持仓
    positions = get_all_positions()
    for p in positions:
        if p["symbol"] == symbol:
            status = p.get("status", "")
            if status in ("open", "pending", "patching", "pending_guard"):
                return False, f"{symbol} 已有机器人{'持仓' if status=='open' else ('补挂中' if status=='patching' else '挂单')}"

    # 1-2: 检查 pending/patching orders（兼容不同字段名）
    pending = [p for p in positions if p.get("status") in ("pending", "patching", "pending_guard")]
    for p in pending:
        if p["symbol"] == symbol:
            return False, f"{symbol} 已有挂单"

    # 1-2b: 交易所实时挂单兜底检查
    # 防止手动挂单或 pos_state 未同步时系统重复开仓
    try:
        from shared.binance_api import get_open_orders as _get_open_orders
        live_orders = _get_open_orders(symbol) or []
        # 同向入场单：同方向 SELL SHORT 或 BUY LONG
        entry_side = "SELL" if direction == "SHORT" else "BUY"
        entry_ps   = direction
        dup = [o for o in live_orders
               if o.get("side") == entry_side
               and o.get("positionSide", "").upper() == entry_ps
               and o.get("type") in ("LIMIT", "MARKET")]
        if dup:
            return False, f"{symbol} 交易所已有同向入场挂单({len(dup)}笔)，跳过重复开仓"
    except Exception as _lo_e:
        # [fix #23] 兜底检查失败 → 拒绝开仓，不静默放行
        logger.error(f"[executor] {symbol} 查询交易所挂单失败，拒绝开仓: {_lo_e}")
        return False, f"{symbol} 交易所挂单查询失败，无法排除重复开仓，拒绝开仓"

    # 1-3/1-4: 持仓总量 + 方向上限（用 risk_manager.check_position_limits 而非静态上限，和 scanner 动态上限保持一致）
    open_pos  = [p for p in positions if p.get("status") in ("open", "pending")]
    long_cnt  = sum(1 for p in open_pos if p.get("direction", "").upper() == "LONG")
    short_cnt = sum(1 for p in open_pos if p.get("direction", "").upper() == "SHORT")

    if not check_position_limits(direction):
        # check_position_limits 内部已记录具体原因日志，这里补充简要描述
        d_cnt = long_cnt if direction == "LONG" else short_cnt
        return False, f"持仓上限（{direction} {d_cnt}仓，总{len(open_pos)}）"

    # 1-5: 熔断器
    if is_circuit_open():
        return False, "熔断中"

    # 1-6: 冷却期
    in_cd, cd_reason = is_in_cooldown(symbol)
    if in_cd:
        # [fix-1] crash_guard_short 只拦 SHORT；crash_guard（崩盘）只拦 LONG
        _cdr = str(cd_reason or "")
        if _crash_cd_allows(_cdr, direction):
            logger.info(f"[executor] {symbol} 冷却原因={_cdr} 与 {direction} 方向无关，放行")
        else:
            return False, f"{symbol} 冷却期: {cd_reason}"

    # 1-7: 相关性过滤（功能一 v3.3）
    # 待开仓标的与现有同方向持仓的皮尔逊系数 > 0.85 时拒绝，方向相反时跳过
    try:
        _corr_result = _check_correlation(symbol, direction, open_pos)
        if _corr_result[0]:
            return False, _corr_result[1]
        signal["_max_correlation"] = _corr_result[2]   # 写入 signal 供 trade_log 使用
    except Exception as _ce:
        logger.warning(f"[executor] {symbol} 相关性检查异常（跳过）: {_ce}")

    # 1-8: funding_arb 专用检查
    if signal.get("sig_type") == "funding_arb" or signal.get("mode") == "funding_arb":
        from shared.state import is_funding_arb_paused, get_funding_arb_count
        from config import FUNDING_ARB_MAX_POSITIONS
        if is_funding_arb_paused():
            return False, "funding_arb 熔断暂停中"
        if get_funding_arb_count() >= FUNDING_ARB_MAX_POSITIONS:
            return False, f"funding_arb 槽位已满({FUNDING_ARB_MAX_POSITIONS})"

    # 1-9: 快速余额预估（避免步骤2~4消耗后才发现余额不足）
    # 只做粗筛：availableBalance < 20U 直接拒绝，精确计算留给步骤5
    try:
        _acc_quick = api.get_account()
        _avail_quick = float(_acc_quick.get("availableBalance", 999))
        if _avail_quick < 20.0:
            return False, f"余额不足预检拒绝: availableBalance={_avail_quick:.2f}U < 20U"
    except Exception:
        pass   # 拉取失败不拦截，留给步骤5精确检查

    # 1-10: 流动性 Gate（v3.57）——检查 mark_price ±0.5% 挂单深度，过滤极薄标的
    _liq_ok, _liq_reason = _check_liquidity_gate(
        symbol, direction, notional=100.0,
        oi_now=signal.get("ind", {}).get("oi_now", 0.0),
    )
    if not _liq_ok:
        return False, _liq_reason

    # 1-11: 多空净 Delta 监控（v3.57）
    # 防止同方向暴露过高：净 Delta = 多头名义 - 空头名义
    # 超过账户权益 × NET_DELTA_MAX_PCT 时限制新增同向信号
    try:
        from config import NET_DELTA_MAX_PCT
    except ImportError:
        NET_DELTA_MAX_PCT = 0.30  # 净 Delta 不超过账户权益30%

    try:
        _all_pos = get_all_positions()
        _acc_d   = api.get_account()
        _equity  = float(_acc_d.get("totalMarginBalance", 0)) or float(_acc_d.get("totalWalletBalance", 1))
        # v3.59: 同时计入 open 和 pending 持仓，避免多个同向 pending 等待成交时 Delta 被低估
        _nom_statuses = ("open", "pending", "patching", "pending_guard")
        _long_nom  = sum(
            float(p.get("quantity", 0)) * float(p.get("entry_price", 0))
            for p in _all_pos if p.get("status") in _nom_statuses and p.get("direction","").upper() == "LONG"
        )
        _short_nom = sum(
            float(p.get("quantity", 0)) * float(p.get("entry_price", 0))
            for p in _all_pos if p.get("status") in _nom_statuses and p.get("direction","").upper() == "SHORT"
        )
        _net_delta   = _long_nom - _short_nom
        _delta_limit = _equity * NET_DELTA_MAX_PCT
        if direction == "LONG" and _net_delta > _delta_limit:
            return False, (
                f"净多头 Delta 过高: {_net_delta:.0f}U > {_delta_limit:.0f}U（权益{_equity:.0f}U×{NET_DELTA_MAX_PCT*100:.0f}%），"
                f"限制新增多头（已计入 open+pending）"
            )
        elif direction == "SHORT" and (-_net_delta) > _delta_limit:
            return False, (
                f"净空头 Delta 过高: {-_net_delta:.0f}U > {_delta_limit:.0f}U（权益{_equity:.0f}U×{NET_DELTA_MAX_PCT*100:.0f}%），"
                f"限制新增空头（已计入 open+pending）"
            )
    except Exception as _de:
        # [fix #28] 检查异常 → 拒绝开仓（原为降级放行）
        logger.error(f"[executor] {symbol} 净Delta检查失败，拒绝开仓: {_de}")
        return False, f"{symbol} 净Delta检查异常，拒绝开仓: {_de}"

    # 1-12: sl_degraded + score 双重过滤（v3.78 APR 复盘教诛）
    # 背景： sl_degraded=True 说明止损不在结构位（容易被噪音打到）
    #          score 小于 55 说明信号 edge 很薄、边界检验
    #          两个同时成立 → 期望值为负，直接带山拒绝
    _sig_ind    = signal.get("ind") or {}
    _sig_score  = _sig_ind.get("final_score", None)
    if _sig_score is None:
        _strength = signal.get("signal_strength", "medium")
        _sig_score = {"strong": 75, "medium": 55, "weak": 40}.get(_strength, 55)
    _sig_sl_deg = signal.get("sl_degraded", False) or (signal.get("params") or {}).get("sl_degraded", False)
    if _sig_sl_deg and _sig_score < 55:
        return False, (
            f"{symbol} sl_degraded=True 且 score={_sig_score:.0f}<55，"
            f"止损非结构位+信号边界双重风险，拒绝开仓"
        )

    return True, ""


# ─────────────────────────────────────────────────────────────────────────────
# 流动性 Gate：开仓前检查 ±0.5% 挂单深度，过滤极薄标的（v3.57）
# ─────────────────────────────────────────────────────────────────────────────
def _check_liquidity_gate(
    symbol: str,
    direction: str,
    notional: float,
    oi_now: float = 0.0,
) -> tuple[bool, str]:
    """
    检查订单簿深度是否足够承载本次开仓。
    - 拉 limit=20 深度，统计以 mark_price 为中心 ±0.5% 区间内的挂单金额
    - LONG 做多：看 ask 侧深度（需要有人卖给我们）
    - SHORT 做空：看 bid 侧深度（需要有人买我们卖出的仓）
    - 阈值 = max(OI×0.1%, 3000U, notional×DEPTH_MULT)，对小币相对化
      若 oi_now=0（未传入），退化为 max(MIN_LIQUIDITY_U, notional×DEPTH_MULT)
    - 查询失败时降级放行（不因网络问题阻断开仓）
    """
    try:
        from config import LIQUIDITY_GATE_MIN_U, LIQUIDITY_GATE_DEPTH_MULT
    except ImportError:
        LIQUIDITY_GATE_MIN_U   = 3_000    # 最小绝对深度 3000U（原 10000U，相对化后降低硬底）
        LIQUIDITY_GATE_DEPTH_MULT = 5     # 深度需 >= 名义仓位 × 5

    try:
        mark_p = float(api.get_mark_price(symbol).get("markPrice", 0))
        if mark_p <= 0:
            return True, ""  # 拉不到价格，降级放行

        center = mark_p
        low  = center * 0.995
        high = center * 1.005

        ob = api.get_order_book(symbol, limit=20)

        if direction == "LONG":
            # 做多：需要 ask 深度（卖方挂单，我方吃掉）
            depth_u = sum(
                float(a[0]) * float(a[1])
                for a in ob.get("asks", [])
                if float(a[0]) <= high
            )
        else:
            # 做空：需要 bid 深度（买方挂单，我方卖给他们）
            depth_u = sum(
                float(b[0]) * float(b[1])
                for b in ob.get("bids", [])
                if float(b[0]) >= low
            )

        # 阈值相对化：OI×0.1% 为动态底线
        # fix: oi_now 是币安返回的合约张数（非 USDT），必须乘以 mark_price 换算后再取 0.1%
        oi_usdt   = oi_now * mark_p if (oi_now > 0 and mark_p > 0) else 0.0
        oi_based  = oi_usdt * 0.001 if oi_usdt > 0 else 0.0
        threshold = max(LIQUIDITY_GATE_MIN_U, oi_based, notional * LIQUIDITY_GATE_DEPTH_MULT)
        if depth_u < threshold:
            return False, (
                f"流动性不足: {direction} 侧±0.5%深度={depth_u:.0f}U "
                f"< 阈值{threshold:.0f}U（mark={center:.6g} OI={oi_usdt:.0f}U notional={notional:.0f}U×{LIQUIDITY_GATE_DEPTH_MULT}）"
            )
        return True, ""
    except Exception as _lge:
        logger.debug(f"[executor] {symbol} 流动性检查失败，降级放行: {_lge}")
        return True, ""

# ─────────────────────────────────────────────────────────────────────────────
# 下单参数自校验（v3.57）— 防止误操作 / 代码 bug 导致参数越界
# ─────────────────────────────────────────────────────────────────────────────
def _validate_order_params(symbol: str, side: str, position_side: str,
                            quantity: float, price: float, notional: float) -> tuple[bool, str]:
    """
    下单前最后一道防线，验证参数是否在合理范围内。
    任何校验失败都会阻止下单，并记录详细日志。
    """
    try:
        from config import MAX_SINGLE_NOMINAL   # [v3.87 STEP6] 移除未使用的 LIQUIDITY_GATE_MIN_U
    except ImportError:
        MAX_SINGLE_NOMINAL = 1000

    errors = []

    # ① symbol 格式：必须以 USDT（或 USDC，取决于开关）结尾，且为大写
    from config import ENABLE_USDC_PAIRS
    _suffixes = ("USDT", "USDC") if ENABLE_USDC_PAIRS else ("USDT",)
    if not symbol or not symbol.endswith(_suffixes) or symbol != symbol.upper():
        errors.append(f"symbol 格式异常: {symbol!r}")

    # ② side 和 positionSide 方向一致性
    valid_combos = {("BUY", "LONG"), ("SELL", "SHORT")}
    if (side, position_side) not in valid_combos:
        errors.append(f"side/positionSide 方向不一致: side={side} positionSide={position_side}")

    # ③ quantity 范围：必须 > 0 且合理（不超过10倍MAX_SINGLE_NOMINAL的最保守估算）
    if quantity <= 0:
        errors.append(f"quantity <= 0: {quantity}")
    elif price > 0 and quantity * price > MAX_SINGLE_NOMINAL * 10:
        errors.append(
            f"quantity 过大: qty={quantity} × price={price:.4g} = {quantity*price:.0f}U "
            f"> {MAX_SINGLE_NOMINAL*10}U（MAX_SINGLE_NOMINAL×10）"
        )

    # ④ price 范围：必须 > 0
    if price <= 0:
        errors.append(f"price <= 0: {price}")

    # ⑤ notional 名义仓位上限
    if notional > MAX_SINGLE_NOMINAL * 1.05:   # 给5%误差容忍
        errors.append(f"notional 超限: {notional:.0f}U > {MAX_SINGLE_NOMINAL}U")

    if errors:
        msg = f"[validator] {symbol} 下单参数异常，阻止下单: " + "; ".join(errors)
        logger.error(msg)
        return False, msg

    return True, ""

# ─────────────────────────────────────────────────────────────────────────────
# 账户状态感知仓位调整辅助函数
# ─────────────────────────────────────────────────────────────────────────────
def _apply_account_state_scale(signal: dict, notional: float) -> float:
    """根据账户状态（亏损/连续亏损）缩放名义仓位，返回调整后的 notional。"""
    try:
        from shared.risk_manager import get_account_state_scale
        scale = get_account_state_scale()
        if scale < 1.0:
            symbol = signal.get("symbol", "")
            old_n  = notional
            notional = round(notional * scale, 2)
            logger.info(
                f"[executor] {symbol} 账户状态降仓 scale={scale:.2f}: "
                f"{old_n:.0f}U → {notional:.0f}U"
            )
    except Exception as e:
        logger.debug(f"[executor] _apply_account_state_scale 跳过: {e}")
    return notional


# ─────────────────────────────────────────────────────────────────────────────
# 步骤2：拉取最新市场数据
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_market(symbol: str) -> dict:
    mark_data = api.get_mark_price(symbol)
    klines_1h = api.get_klines(symbol, "1h", limit=22)  # 多取2根确保有20根已闭合
    oi_data   = api.get_open_interest(symbol)
    # v3.6: 删除 get_order_book 调用，返回值在主流程未被使用；
    # _orderbook_tp 内部自己拉并已加 30s 缓存

    mark_price = float(mark_data.get("markPrice", 0))
    funding    = float(mark_data.get("lastFundingRate", 0))
    oi_now     = float(oi_data.get("openInterest", 0))

    # 去除末尾未闭合 K 线
    now_ms = time.time() * 1000
    closed_k1h = [k for k in klines_1h if float(k[6]) < now_ms]

    # 计算 ATR14（1H，使用已闭合K线）
    atr = _calc_atr(closed_k1h)

    # EMA20（1H 收盘价）
    ema20 = _calc_ema([float(k[4]) for k in closed_k1h], 20)

    return {
        "mark_price": mark_price,
        "funding":    funding,
        "oi_now":     oi_now,
        "k1h":        closed_k1h,
        "atr":        atr,
        "ema20":      ema20,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 步骤3：拉取 Coinglass 清算图
# ─────────────────────────────────────────────────────────────────────────────
def _normalize_liq_zones(zones: list, atr: float = 0) -> list:
    """
    将 liq_cache 返回的单点格式 {price, amount, direction, source}
    统一转换为 calc_sl_tp 需要的区间格式 {price_low, price_high, liq_usd}。
    若已是区间格式则原样返回。
    """
    if not zones:
        return []
    sample = zones[0]
    if "price_low" in sample:
        return zones   # 已是区间格式
    result = []
    for z in zones:
        p = float(z.get("price", 0))
        if p <= 0:
            continue
        # 用价格的 0.5% 作为区间宽度（无更精确信息时的合理近似）
        half = p * 0.005
        result.append({
            "price_low":  p - half,
            "price_high": p + half,
            "liq_usd":    float(z.get("amount", z.get("liq_usd", 0))),
        })
    return result


def _fetch_liq(symbol: str, mark_price: float = 0.0) -> dict:
    """返回 {"long": [...], "short": [...]}，从 liq_cache 读取，失败返回 {}
    问题五修复：接收 mark_price 参数，避免重复调用 get_mark_price"""
    try:
        from shared.liq_cache import get_liq_zones_cached
        price = mark_price
        if price <= 0:
            # 尚未传入时才拉取（兼容旧调用方式）
            mp = api.get_mark_price(symbol)
            price = float(mp.get("markPrice", 0)) if mp else 0
        liq_long  = get_liq_zones_cached(symbol, "LONG",  price)
        liq_short = get_liq_zones_cached(symbol, "SHORT", price)
        # 格式标准化：liq_cache 返回 {price, amount} 单点格式
        # calc_sl_tp 需要 {price_low, price_high, liq_usd} 区间格式
        liq_long  = _normalize_liq_zones(liq_long,  atr=0)
        liq_short = _normalize_liq_zones(liq_short, atr=0)
        return {"long": liq_long, "short": liq_short}
    except Exception as e:
        logger.debug(f"[executor] {symbol} liq_cache 失败: {e}，止损止盈降级为ATR")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# 步骤5：账户状态终检
# ─────────────────────────────────────────────────────────────────────────────
def _account_final_check(notional: float, notifier_fn=None) -> tuple[bool, str]:
    try:
        acc = api.get_account()
        available_balance = float(acc.get("availableBalance", 0))
        # 用 USDT walletBalance 做上限基准，与开仓公式保持一致
        total_balance = 0.0
        for _a in acc.get("assets", []):
            if _a.get("asset") == "USDT":
                total_balance = float(_a.get("walletBalance", 0))
                break
        if total_balance <= 0:
            total_balance = float(acc.get("totalWalletBalance", 0))

        # 检查一：可用余额 >= 名义仓位 / 杠杆 * 1.1（手续费缓冲）
        # 此处先用保守估计，杠杆在后续步骤确定，这里用 10 估算
        margin_needed = notional / 10 * 1.1
        if available_balance < margin_needed:
            return False, f"可用余额不足: {available_balance:.2f}U < 需要{margin_needed:.2f}U"

        # 检查二：总名义仓位上限（v3.38.1: 改用 MAX_TOTAL_NOMINAL，遵从设计意图5倍而非杠杆10倍）
        try:
            from config import MAX_TOTAL_NOMINAL as _max_nom
        except ImportError:
            _max_nom = 5.0
        positions = get_all_positions()
        total_notional = sum(
            p.get("quantity", 0) * p.get("entry_price", 0)
            for p in positions if p.get("status") in ("open", "pending", "patching", "pending_guard")
        )
        max_notional = total_balance * _max_nom
        if total_notional + notional > max_notional:
            return False, f"总名义仓位超限: {total_notional+notional:.0f}U > {max_notional:.0f}U (上限={max_notional:.0f}U)"

        # 功能二-b：全局最大回撤保护（v3.57）
        try:
            from shared.risk_manager import check_max_drawdown as _cmd, update_drawdown_high_water as _udw
            _total_margin = float(acc.get("totalMarginBalance", total_balance))
            # v3.59: 高水位只在无活跃持仓时更新，避免含浮亏的权益虚假拉高高水位
            _open_pos_count = sum(1 for p in get_all_positions() if p.get("status") in ("open", "pending", "patching", "pending_guard"))
            if _open_pos_count == 0:
                _udw(_total_margin)
            _dd_ok, _dd_reason = _cmd(_total_margin, notifier_fn=notifier_fn)
            if not _dd_ok:
                return False, _dd_reason
        except Exception as _dd_e:
            # [fix #28] 回撤保护检查异常 → 拒绝开仓（原为静默放行）
            logger.error(f"[executor] {symbol} 回撤保护检查失败，拒绝开仓: {_dd_e}")
            return False, f"回撤保护检查异常，拒绝开仓: {_dd_e}"

        # 功能三：保证金安全垫检查
        try:
            from shared.risk_manager import check_margin_safety as _cms
            _ok_m, _reason_m, _mstats = _cms(notifier_fn=notifier_fn)
            if not _ok_m:
                return False, f"保证金安全垫不足: {_reason_m}"
            if _mstats.get("margin_tight_hint"):
                _MARGIN_TIGHT_HINT[0] = _mstats["margin_tight_hint"]
            else:
                _MARGIN_TIGHT_HINT[0] = ""
            # 保证金偏紧时将 scale 写入全局供开仓逻辑读取
            _MARGIN_SCALE[0] = _mstats.get("position_scale", 1.0)
        except Exception as _ms_e:
            # [fix #28/#29] 保证金安全垫检查异常 → 拒绝开仓
            logger.error(f"[executor] {symbol} 保证金安全垫检查失败，拒绝开仓: {_ms_e}")
            return False, f"保证金安全垫检查异常，拒绝开仓: {_ms_e}"

        return True, ""
    except Exception as e:
        return False, f"账户查询失败: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# 杠杆利用率预算（v3.90）
# ─────────────────────────────────────────────────────────────────────────────
def _apply_leverage_budget(symbol: str, direction: str, nominal: float, equity: float) -> tuple[bool, float, str]:
    """
    返回 (放行?, 截断后的名义仓位, 原因)。
    LEVERAGE_UTILIZATION_ENABLED 关闭时原样放行；剩余预算 <= 0 拒绝开仓；否则 nominal = min(nominal, 剩余预算)，
    截断后若低于 MIN_NOTIONAL_GUARD 同样拒绝开仓（"杠杆预算不足最小名义"）。
    计算本身出现任何异常都降级放行（不阻断开仓）。不调用交易所接口。
    """
    try:
        import config as _cfg
        if not hasattr(_cfg, "LEVERAGE_UTILIZATION_ENABLED"):
            # 参数缺失（曾因 config.py 被覆盖而整块丢失）≠ 显式关闭：不再静默放行，留一条告警
            logger.warning("[leverage] LEVERAGE_UTILIZATION_ENABLED 读取失败，杠杆预算已跳过，请检查 config.py")
            return True, nominal, ""
        if not getattr(_cfg, "LEVERAGE_UTILIZATION_ENABLED", False):
            return True, nominal, ""
        from shared.risk_manager import calc_leverage_remaining
        lev_remaining = calc_leverage_remaining(direction, equity)
        if lev_remaining <= 0:
            logger.warning(f"[leverage] {symbol} {direction} 杠杆预算已用满，拒绝开仓")
            return False, nominal, "杠杆预算已用满"
        nominal = min(nominal, lev_remaining)
        logger.info(f"[leverage] {symbol} {direction} remaining={lev_remaining:.0f}U → nominal={nominal:.0f}U")
        # 截断后低于最小名义（MIN_NOTIONAL_GUARD 的检查在本步骤之前，截断后需再核一次）→ 拒绝本次开仓
        if nominal < MIN_NOTIONAL_GUARD:
            logger.warning(f"[leverage] {symbol} {direction} 杠杆预算截断后低于最小名义，跳过开仓 "
                           f"(截断后={nominal:.2f}U < 最小名义={MIN_NOTIONAL_GUARD}U)")
            return False, nominal, "杠杆预算不足最小名义"
        return True, nominal, ""
    except Exception as e:
        logger.warning(f"[leverage] {symbol} 杠杆预算检查异常，降级放行: {e}")
        return True, nominal, ""


# ─────────────────────────────────────────────────────────────────────────────
# 步骤6：查询并设置最大杠杆
# ─────────────────────────────────────────────────────────────────────────────
def _setup_symbol(symbol: str) -> int:
    """设置全仓模式 + 最大杠杆，返回实际使用杠杆"""
    # 先设全仓（无论账户当前是否已全仓，都重新设一次）
    try:
        api.set_margin_type(symbol, "CROSSED")
    except Exception as e:
        if "No need" not in str(e):
            logger.warning(f"[executor] {symbol} 设全仓失败: {e}")

    # 再设杠杆
    max_lev = _get_max_leverage(symbol)
    try:
        api.set_leverage(symbol, max_lev)
        logger.info(f"[executor] {symbol} 设置杠杆 {max_lev}x")
    except Exception as e:
        # [v3.89 #102] 杠杆没设上会让保证金校验和数量计算失真，且此时尚未提交任何订单 → 可重试
        logger.error(f"[executor] {symbol} 设置杠杆 {max_lev}x 失败: {e}")
        raise PreOrderError(f"{symbol} 设置杠杆失败: {e}") from e
    return max_lev


# ─────────────────────────────────────────────────────────────────────────────
# 步骤7：提交开仓限价单
# ─────────────────────────────────────────────────────────────────────────────
def _place_open_order(symbol: str, direction: str, mode: str,
                       entry_price: float, quantity: float,
                       precision: dict,
                       notional: float = 0.0,
                       entry_mode_hint: str = "immediate",
                       expire_ts: int | None = None) -> tuple[int | None, str]:
    """
    提交开仓限价单，返回 (orderId, error_reason)。
    失败直接返回 None，不重试。
    """
    tick = precision["tick"]
    step = precision["step"]
    side = "BUY" if direction.upper() == "LONG" else "SELL"

    price_str = api.fmt_num(_round_price(entry_price, tick))   # [fix #25] 统一定点格式
    qty_str   = api.fmt_num(_round_qty(quantity, step))

    # [fix] -4024 预检：若计算价格低于历史已记录的最低限价，直接拒绝不提交
    _min_price_key = f"_min_price_{symbol}"
    _min_price_cached = _PRECISION_CACHE.get(_min_price_key, 0.0)
    if _min_price_cached > 0 and float(price_str) < _min_price_cached:
        logger.warning(
            f"[executor] {symbol} 入场价 {price_str} < 历史已记录最低限价 {_min_price_cached}，"
            f"跳过提交（-4024 预防）"
        )
        return None, f"-4024预防: 入场价 {price_str} < 最低限价 {_min_price_cached}"

    # v3.57: 下单参数自校验——最后一道防线
    _val_ok, _val_reason = _validate_order_params(
        symbol, side, direction.upper(), float(qty_str), float(price_str), notional
    )
    if not _val_ok:
        logger.error(f"[executor] {symbol} 下单参数校验失败，取消开仓: {_val_reason}")
        # [v3.87 STEP4-6] 原写法 return None，但调用方 `order_id, err = _place_open_order(...)` 需要二元组，
        #   参数校验失败时会抛 TypeError（被外层吞掉或中断本次开仓流程）。改为返回 (None, 原因)，与其它失败出口一致。
        return None, f"下单参数校验失败: {_val_reason}"

    try:
        if mode == "breakout":
            # v3.6: expire_ts 由外层 _execute_async 统一计算并传入，不在此重复计算
            order = api.place_order(
                symbol=symbol, side=side,
                order_type="LIMIT",
                quantity=float(qty_str),
                price=float(price_str),
                timeInForce="GTD",
                goodTillDate=expire_ts,
            )
        elif mode == "reversal" and entry_mode_hint == "pending_order":
            # v3.6: pending_order 改用 GTX（POST_ONLY）
            # GTC 限价单在挂单价 < mark_price 时会立刻撮合，等待回调意图失效；
            # GTX 若能立刻成交则被交易所直接拒绝，确保只在价格真正回调时才入场
            order = api.place_order(
                symbol=symbol, side=side,
                order_type="LIMIT",
                quantity=float(qty_str),
                price=float(price_str),
                timeInForce="GTX",
            )
        else:
            order = api.place_order(
                symbol=symbol, side=side,
                order_type="LIMIT",
                quantity=float(qty_str),
                price=float(price_str),
                timeInForce="GTC",
            )
        oid = order.get("orderId")
        if not oid:
            return None, f"接口返回无 orderId: {order}"
        logger.info(f"[executor] {symbol} 开仓单提交 orderId={oid} price={price_str} qty={qty_str}")
        return int(oid), ""
    except Exception as e:
        err_str = str(e)
        # ── -4024: Limit price can't be lower than X ─────────────────────────
        # 交易所拒绝原因：当前最低限价高于计算的入场价。
        # 不重试；提取最低限价并记录到 warning，供下次沃价参考。
        if "-4024" in err_str:
            import re as _re
            _m = _re.search(r"lower than ([\d.]+)", err_str)
            _min_price = _m.group(1) if _m else "unknown"
            logger.warning(
                f"[executor] {symbol} -4024 入场价 {price_str} 低于交易所最低限价 {_min_price}，"
                f"拒绝下单（不重试）"
            )
            # 将最低限价写入精度缓存，下次下单前预检可直接拉起
            if _m:
                try:
                    _PRECISION_CACHE[f"_min_price_{symbol}"] = float(_min_price)
                except Exception:
                    pass
            return None, f"-4024 入场价影下了交易所最低限价 {_min_price}，需调整入场条件"
        # ── -2019 保证金不足专项处理 ──────────────────────────────────
        if "-2019" in err_str or "insufficient" in err_str.lower():
            import asyncio as _aio
            logger.error(f"[executor] {symbol} -2019 保证金不足，暂停开仓15分钟")
            # 写入临时熔断标记
            try:
                import pathlib as _pl, json as _js, time as _tm
                _flag = Path(__file__).parent.parent / "ws_cache" / "margin_pause.json"
                _flag.write_text(_js.dumps({
                    "until": _tm.time() + 900,
                    "reason": f"-2019 {symbol}"
                }))
            except Exception:
                pass
            # 推送告警（fire-and-forget，不 await 避免阻塞）
            try:
                from shared.notifier import notify as _notify
                import threading as _thr
                _thr.Thread(
                    target=_notify,
                    args=(f"🚨 {symbol} 开仓失败: 保证金不足(-2019)，已暂停开仓15分钟",),
                    daemon=True
                ).start()
            except Exception:
                pass
        return None, err_str


# ─────────────────────────────────────────────────────────────────────────────
# 步骤8：WebSocket 等待开仓成交（含部分成交处理）
# ─────────────────────────────────────────────────────────────────────────────
async def _wait_fill_ws(symbol: str, order_id: int,
                         ws_listener: "WsOrderListener",
                         timeout_sec: int = 120) -> dict | None:
    """
    通过 WebSocket 监听 ORDER_TRADE_UPDATE，等待 orderId 的 FILLED 事件。
    timeout_sec 内未完全成交：撤销剩余，返回实际成交快照（含 partial fill）。
    返回 None 表示完全未成交（撤销干净）。
    """
    deadline = asyncio.get_event_loop().time() + timeout_sec
    filled_qty = 0.0
    fill_price = 0.0

    while asyncio.get_event_loop().time() < deadline:
        event = await ws_listener.wait_order_event(order_id, timeout=2.0)
        if event is None:
            continue

        status     = event.get("X", "")    # 订单状态
        # [v3.87 STEP6] 删除未使用的 exec_type
        f_qty      = float(event.get("l", 0))   # 本次成交数量
        f_price    = float(event.get("L", 0))   # 本次成交价

        if f_qty > 0:
            # 加权均价
            fill_price = (fill_price * filled_qty + f_price * f_qty) / (filled_qty + f_qty)
            filled_qty += f_qty

        if status == "FILLED":
            return {"fill_price": fill_price, "fill_qty": filled_qty}

        if status in ("CANCELED", "EXPIRED", "REJECTED"):
            if filled_qty > 0:
                # 有部分成交
                return {"fill_price": fill_price, "fill_qty": filled_qty}
            return None

    # 超时：撤单，处理部分成交
    try:
        api.cancel_order(symbol, order_id)
        logger.info(f"[executor] {symbol} orderId={order_id} 超时撤单")
    except Exception as e:
        logger.warning(f"[executor] 撤单失败: {e}")

    if filled_qty > 0:
        return {"fill_price": fill_price, "fill_qty": filled_qty}
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 步骤8.5：开仓后场景二次验证
# ─────────────────────────────────────────────────────────────────────────────

async def _entry_recheck(
    symbol: str, direction: str, mode: str,
    fill_price: float, fill_qty: float,
    entry_snap: dict,
    params: dict,
) -> tuple[str, dict]:
    """
    开仓后快速规则校验（已移除 LLM 分支，纯规则、无网络调用）。
    返回 ("HOLD" | "CLOSE", result_dict)

    CLOSE 条件（满足任一）：
      - RSI1H 多单>80 / 空单<20（极端超买/超卖，反向）
      - 资金费率方向与持仓相反且绝对值>0.5%
      - （v3.78 的「逆势拉盘扫入」规则已在 v3.87 删除：所需快照字段从未提供，规则从未生效）
    其余情况一律 HOLD，保持原有止损/止盈逻辑不变。
    """
    recheck = {
        "entry_mark_price": entry_snap.get("mark_price", fill_price),
        "verdict": "HOLD",
        "reason":  "快速规则通过",
    }
    try:
        funding = entry_snap.get("funding_rate", 0.0)   # [v3.87 STEP6] 删除未使用的 rsi_1h

        if direction == "LONG":
            # v3.6: 去掉 rsi_1h>80 CLOSE（成交后K线未更新，几乎不会触发，意义不大）
            # v3.6: 做多时负费率是有利的（空头付给多头），正费率>0.5%才是多头成本极高需要平仓
            if funding > 0.005:
                recheck.update({"verdict": "CLOSE", "reason": f"正资金费率{funding*100:.3f}%极高，多头持仓成本过大"})
        else:
            # v3.38.1: 去掉 rsi_1h<20 CLOSE（与 LONG 方向对称——成交后K线未更新，RSI 不可信）
            # v3.56 fix: 资金费率方向修正
            # 正费率(funding>0)：多头付给空头，空头收费，有利 → 不应平仓
            # 负费率(funding<0)：空头付给多头，空头付费，成本高 → 绝对值>0.5%时平仓
            if funding < -0.005:
                recheck.update({"verdict": "CLOSE", "reason": f"负资金费率{funding*100:.3f}%，空头付费成本过高"})

            # [v3.87 STEP6-1] 删除 v3.78 的「逆势拉盘扫入就平仓」规则（pullback/breakout SHORT，vol_ratio>3 且成交价高出触发价>1.5%）。
            #   该规则依赖入场快照里的 vol_ratio / prev_close / entry_price_snap，但调用方构造的快照从未包含这三个字段，
            #   所以 _snap_entry 恒为 0、价格跳升恒为 0，条件从未成立过；没有数据支撑，按评审决定删除，避免死代码误导。
            #   若以后要恢复，需先在 _execute_async 的快照里补齐这三个字段并用数据验证。
    except Exception as e:
        logger.debug(f"[recheck] {symbol} 规则检查异常，降级HOLD: {e}")
        recheck["reason"] = f"规则检查异常降级: {e}"

    logger.info(f"[recheck] {symbol} 结论={recheck['verdict']} 原因={recheck['reason']}")
    return recheck["verdict"], recheck


# 步骤9：成交后并发提交止损止盈单
# ─────────────────────────────────────────────────────────────────────────────
async def _place_guards_async(
    symbol: str, direction: str,
    fill_qty: float, sl_price: float, tp1_price: float, tp2_price: float | None,
    precision: dict,
    skip_sl: bool = False,
) -> dict:
    """
    提交保护单：SL + TP1（必选）+ TP2（可选，tp2_price=None 时不挂）。
    tp2_price=None 表示无二阶段止盈（无清算图或只有单一清算区时）。
    """
    step = precision["step"]
    tick = precision["tick"]
    close_side = "SELL" if direction.upper() == "LONG" else "BUY"
    pos_side   = "LONG" if direction.upper() == "LONG" else "SHORT"

    has_tp2 = tp2_price is not None

    if has_tp2:
        # 有 TP2：TP1 = 60%，TP2 = 40%
        tp1_qty_f = _round_qty(fill_qty * TP1_CLOSE_RATIO, step)
        tp2_qty_f = _round_qty(fill_qty - tp1_qty_f, step)
    else:
        # 无 TP2：TP1 = 100% 仓位全平
        tp1_qty_f = _round_qty(fill_qty, step)
        tp2_qty_f = 0.0

    # v3.6: sl_qty 改用取整后合计，防止 TP1+TP2 取整后略少，导致 TP 全成交后
    # 止损单剩余数量>0 触发 reduceOnly 拒绝
    # [v3.87 STEP6-3] place_sl 直接用 f"{quantity}" 拼接数量字符串，tp1+tp2 两个浮点数相加会产生
    #   0.30000000000000004 这类噪声，可能被交易所以精度错误拒绝（止损单提交失败）。相加后按步长精度再取一次整。
    sl_qty_f  = _round_sum_qty(tp1_qty_f, tp2_qty_f, step) if tp2_qty_f > 0 else tp1_qty_f
    sl_p  = _round_price(sl_price,  tick)
    tp1_p = _round_price(tp1_price, tick)
    tp2_p = _round_price(tp2_price, tick) if has_tp2 else None

    # TP1/TP2 顺序检查：LONG 下 TP2 必须 > TP1；SHORT 下 TP2 必须 < TP1
    if has_tp2 and tp2_p is not None:
        direction_up = direction.upper()
        if direction_up == "LONG" and tp2_p <= tp1_p:
            logger.warning(
                f"[executor] {symbol} TP2({tp2_p:.6g}) ≤ TP1({tp1_p:.6g})，"
                f"顺序错误，交换 TP1/TP2"
            )
            tp1_p, tp2_p = tp2_p, tp1_p
        elif direction_up == "SHORT" and tp2_p >= tp1_p:
            logger.warning(
                f"[executor] {symbol} TP2({tp2_p:.6g}) ≥ TP1({tp1_p:.6g})，"
                f"顺序错误，交换 TP1/TP2"
            )
            tp1_p, tp2_p = tp2_p, tp1_p

    results: dict = {}

    async def _place(tag: str, kwargs: dict):
        if kwargs.get("quantity", 0) <= 0:
            results[tag] = None
            await notify_async(f"⚠️ {symbol} {tag} 取整后数量为零，未提交；仓位处于部分保护状态")
            return
        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None, lambda: api.place_order(**kwargs)
            )
            # resp 为空 dict 表示 API 调用失败（_retry 耗尽后返回 {}）
            if not resp or "orderId" not in resp:
                err_msg = resp.get("error", "API返回空响应") if resp else "API返回空响应"
                raise RuntimeError(err_msg)
            results[tag] = int(resp["orderId"])
        except Exception as e:
            logger.error(f"[executor] {symbol} {tag} 提交失败: {e}")
            results[tag] = None
            await notify_async(
                f"⚠️ {symbol} {tag} 保护单提交失败！\n"
                f"仓位处于部分保护状态，请人工介入。\n"
                f"错误：{e}"
            )

    # ── SL：STOP_MARKET（普通合约接口，服务端触发，进程崩溃也有效）──────────────
    async def _place_algo_sl_with_verify():
        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None, lambda: api.place_sl(
                    symbol=symbol,
                    side=close_side,
                    position_side=pos_side,
                    trigger_price=sl_p,
                    quantity=sl_qty_f,
                )
            )
            order_id = resp.get("orderId") or resp.get("algoId")
            algo_status = resp.get("algoStatus", resp.get("status", ""))
            if not order_id or algo_status not in ("NEW", ""):
                raise RuntimeError(f"SL 状态异常: orderId={order_id} status={algo_status}")
            results["sl"] = int(order_id)
            logger.info(f"[executor] {symbol} SL 挂单成功: id={order_id} stopPrice={sl_p}")
        except Exception as e:
            logger.error(f"[executor] {symbol} SL 提交失败: {e}")
            results["sl"] = None
            await notify_async(
                f"⚠️ {symbol} 止损单提交失败！\n"
                f"请立即人工设置止损，当前价格接近止损位。\n"
                f"错误：{e}"
            )

    coros = [] if skip_sl else [_place_algo_sl_with_verify()]   # [fix #4] skip_sl: SL 已存活时不重复挂
    coros += [
        _place("tp1", dict(
            symbol=symbol, side=close_side,
            order_type="LIMIT",
            quantity=tp1_qty_f,
            price=tp1_p,
            timeInForce="GTC",
            position_side=pos_side,
            reduce_only=True,
        )),
    ]
    if has_tp2:
        coros.append(_place("tp2", dict(
            symbol=symbol, side=close_side,
            order_type="LIMIT",
            quantity=tp2_qty_f,
            price=tp2_p,
            timeInForce="GTC",
            position_side=pos_side,
            reduce_only=True,
        )))

    await asyncio.gather(*coros)

    tp_mode = "TP1+TP2" if has_tp2 else "单TP"
    logger.info(f"[executor] {symbol} 保护单模式: {tp_mode}")

    return {
        "protection_status": "complete" if (skip_sl or results.get("sl")) and results.get("tp1") and (not has_tp2 or results.get("tp2")) else "partial",
        "unallocated_qty": max(0.0, fill_qty - sl_qty_f),
        "sl_id":   results.get("sl"),
        "tp1_id":  results.get("tp1"),
        "tp2_id":  results.get("tp2"),   # None if no tp2
        "sl_price": sl_p,
        "tp1_price": tp1_p,
        "tp2_price": tp2_p,
        "tp_prices_equal": has_tp2 and tp1_p == tp2_p,
        "tp_mode": "dual" if has_tp2 else "single",
        "sl_qty":  sl_qty_f,
        "tp1_qty": tp1_qty_f,
        "tp2_quantity": tp2_qty_f,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 步骤10：WebSocket 止损止盈联动监听
# ─────────────────────────────────────────────────────────────────────────────
# _guard_listener 已删除（废代码）：
# 止损止盈联动由 position_manager._handle_order_filled 和 patrol_loop 负责，
# 不在 executor 里重复实现，避免维护混淆。


def _get_pos_by_id(pos_id: str) -> dict | None:
    for p in get_all_positions():
        if p.get("pos_id") == pos_id:
            return p
    return None


async def _on_position_closed(pos_id: str, symbol: str,
                                close_reason: str, event: dict) -> None:
    """平仓后处理：更新状态 + trade_log + 通知 + 冷却期/黑名单"""
    pnl = float(event.get("rp", 0))
    close_ts = int(time.time() * 1000)
    update_position(pos_id, {
        "status":       "closed",
        "close_reason": close_reason,
        "close_ts":     close_ts,
        "realized_pnl": pnl,
    })
    pos = _get_pos_by_id(pos_id)
    entry      = pos.get("entry_price", 0) if pos else 0
    exit_price = float(event.get("ap", 0) or event.get("L", 0) or 0)

    # WS路径平仓也写 trade_log.jsonl，统一数据源
    try:
        import datetime as _dt, pathlib as _pl, json as _json
        hold_min  = (close_ts - int(pos.get("open_time", close_ts))) / 60000 if pos else 0
        _direction = pos.get("direction", "long").upper() if pos else "LONG"
        if entry > 0:
            _pnl_pct = (round((exit_price - entry) / entry * 100, 4) if _direction == "LONG"
                        else round((entry - exit_price) / entry * 100, 4))
        else:
            _pnl_pct = 0.0
        _log_entry = {
            "trade_id":        f"{close_ts}_{symbol}",
            "symbol":          symbol,
            "direction":       _direction,
            "mode":            pos.get("mode", "") if pos else "",
            "volatility_tier": pos.get("volatility_tier", "") if pos else "",
            "entry_price":     entry,
            "exit_price":      exit_price,
            "quantity":        float(pos.get("quantity", 0)) if pos else 0,
            "leverage":        pos.get("leverage", 0) if pos else 0,
            "pnl_usdt":        pnl,
            "pnl_pct":         _pnl_pct,
            "holding_minutes": round(hold_min, 1),
            "open_time":       pos.get("open_time", 0) if pos else 0,
            "close_time":      close_ts,
            "close_reason":    close_reason,
            "tp1_filled":      pos.get("tp1_filled", False) if pos else False,
            "signal_strength": pos.get("signal_strength", "") if pos else "",
            "atr_at_entry":    pos.get("atr_at_entry", 0) if pos else 0,
            "source":          "ws",
        }
        _log_path = _pl.Path(__file__).parent / "logs" / "trade_log.jsonl"
        _log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(_log_path, "a") as _f:
            _f.write(_json.dumps(_log_entry, ensure_ascii=False) + "\n")
    except Exception as _le:
        logger.warning(f"[executor] WS平仓 trade_log 写入失败: {_le}")

    # WS 路径平仓通知：向对应信号卡片追加平仓行
    try:
        _sig_id_ws = pos.get("_sig_id", "") if pos else ""
        _hold_min  = (int(time.time() * 1000) - int(pos.get("open_time", int(time.time() * 1000)))) / 60000 if pos else 0
        _tp1_ok    = pos.get("tp1_filled", False) if pos else False
        _tp2_ok    = pos.get("tp2_filled", False) if pos else False
        # exit_price 从函数顶部算好的 exit_price 取（WS event ap/L 字段）
        # fix: 原代码误用了未定义的 close_price，改为已定义的 exit_price
        _exit_p    = exit_price if exit_price > 0 else float(pos.get("exit_price", 0) if pos else 0)
        _pnl_pct   = (pnl / float(pos.get("notional", 1))) * 100 if pos and pos.get("notional") else 0.0
        _close_line = fmt_node_close(
            close_reason = close_reason,
            exit_price   = _exit_p,
            hold_minutes = _hold_min,
            pnl_usdt     = pnl,
            pnl_pct      = _pnl_pct,
            tp1_filled   = _tp1_ok,
            tp2_filled   = _tp2_ok,
        )
        if _sig_id_ws:
            import asyncio as _aio_ws
            # fix: card_remove 必须在 card_append_async 完成后执行，否则缓存已空追加失败
            async def _append_then_remove(_sid=_sig_id_ws, _line=_close_line):
                await card_append_async(_sid, _line)
                card_remove(_sid)
            _aio_ws.ensure_future(_append_then_remove())
        else:
            import asyncio as _aio_ws
            _aio_ws.ensure_future(notify_async(_close_line))
    except Exception as _ne:
        logger.debug(f"[executor] WS平仓卡片通知失败: {_ne}")
    # 冷却期/黑名单统计
    if close_reason == "sl" and pnl < 0:
        try:
            from shared.risk_manager import record_loss
            from shared.state import add_cooldown
            record_loss(pnl)
            # 内联统计近 N 天止损次数（避免跨模块依赖 _count_recent_sl）
            import json as _j
            _tlog = Path(__file__).parent / "logs" / "trade_log.jsonl"
            _cutoff = time.time() - 7 * 86400
            loss_cnt = 0
            try:
                with open(_tlog, "r", encoding="utf-8") as _tf:
                    for _line in _tf:
                        try:
                            _r = _j.loads(_line)
                            if (_r.get("symbol") == symbol
                                    and _r.get("close_reason") == "sl"
                                    and _r.get("open_time", 0) >= _cutoff):
                                loss_cnt += 1
                        except Exception:
                            pass
            except FileNotFoundError:
                pass
            if loss_cnt >= 3:
                add_cooldown(symbol, 7 * 86400, "repeated_stop_loss")
                logger.warning(f"[pm] {symbol} WS路径: 止损{loss_cnt}次触发7天黑名单")
            else:
                add_cooldown(symbol, 4 * 3600, "stop_loss_cooldown")
                logger.info(f"[pm] {symbol} WS路径: 止损冷却4h ({loss_cnt}/3)")
        except Exception as _e:
            logger.error(f"[pm] _on_position_closed 冷却期处理失败: {_e}")

def _persist_position_record(rec: dict, kind: str) -> bool:
    """
    [v3.89 #52] 写入持仓/挂单记录并检查 add_position 的返回值。成功返回 True。
    失败分两类：
      - 同品种已有 open/pending/patching 记录（正常的防重复逻辑）：只记 warning，不进 unpersisted 文件；
      - 其他原因（如持仓文件损坏被拒、写入异常）：订单已在交易所，本地却无记录 →
        追加写 logs/unpersisted_positions.jsonl + logger.critical + 通知；不写 pos_state.json。
    失败时调用方拿到 None，不得继续发通知或打印「流程完成」。
    """
    sym = rec.get("symbol", "")
    err = None
    try:
        if add_position(rec):
            return True
    except Exception as e:
        err = repr(e)
    try:
        from shared import state as _st
        corrupted = bool(getattr(_st, "_POS_STATE_CORRUPTED", False))
        dup = [p for p in get_all_positions()
               if p.get("symbol") == sym and p.get("status") in ("open", "pending", "patching", "pending_guard")]
    except Exception:
        corrupted, dup = False, []
    if err is None and not corrupted and dup:
        logger.warning(f"[executor] {sym} {kind} 记录写入被拒：已有 open/pending/patching 记录"
                       f"（防重复逻辑），pos_id={rec.get('pos_id')} order_id={rec.get('open_order_id')}")
        return False
    reason = err or ("pos_state 已损坏，写入被拒" if corrupted else "add_position 返回 False，原因未知")
    try:
        p = Path(__file__).parent / "logs" / "unpersisted_positions.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.time(), "kind": kind, "symbol": sym, "reason": reason,
                                "open_order_id": rec.get("open_order_id"), "record": rec},
                               ensure_ascii=False, default=str) + "\n")
    except Exception as e:
        logger.error(f"[executor] 写 unpersisted_positions.jsonl 失败: {e}")
    logger.critical(f"[executor] {sym} {kind} 无法写入 pos_state（{reason}）：交易所订单 "
                    f"{rec.get('open_order_id')} 已存在但本地无记录，请人工核查；完整记录见 logs/unpersisted_positions.jsonl")
    try:
        import threading as _thr
        _thr.Thread(target=notify, daemon=True,
                    args=(f"🚨 {sym} {kind} 已在交易所生效但本地持仓记录写入失败（{reason}），请人工核查！",)).start()
    except Exception:
        pass
    return False


def _write_position(
    signal: dict, fill_price: float, fill_qty: float,
    leverage: int, guard: dict, params: dict,
    open_order_id: int, atr: float = 0.0,
) -> str | None:
    """写入 pos_state.json，返回 pos_id；写入失败返回 None（[v3.89 #52]）。问题一修复：加入 atr 参数写入 atr_at_entry"""
    pos_id = str(uuid.uuid4())[:8]
    tp1_qty = guard["tp1_qty"]
    tp2_qty = guard["tp2_quantity"]

    _pos_rec = {
        "pos_id":                pos_id,
        "symbol":                signal["symbol"],
        "direction":             signal["direction"].lower(),
        "mode":                  signal.get("mode", "pullback"),
        "volatility_tier":       signal.get("volatility_tier", "mid"),
        "entry_price":           fill_price,
        "quantity":              fill_qty,
        "quantity_remaining":    fill_qty,
        "leverage":              leverage,
        "sl_price":              guard.get("sl_price", params["sl_price"]),
        "sl_order_id":           guard["sl_id"],
        "tp1_price":             guard.get("tp1_price", params["tp1_price"]),
        "tp1_order_id":          guard["tp1_id"],
        "tp1_quantity":          tp1_qty,
        "tp1_filled":            False,
        "tp2_price":             guard.get("tp2_price", params["tp2_price"]),
        "tp2_order_id":          guard["tp2_id"],
        "tp2_quantity":          tp2_qty,
        "tp_prices_equal": guard.get("tp_prices_equal", False),
        "tp_mode": guard.get("tp_mode", "dual" if params.get("tp2_price") is not None else "single"),
        "tp2_filled":            False,
        "open_time":             int(time.time() * 1000),
        "fill_time":             int(time.time() * 1000),   # v3.6: 实际成交时间，供 breakout_fail 检查用
        "signal_strength":       signal.get("signal_strength", "medium"),
        "liq_data_available":    params.get("liq_data_available", False),
        "sl_degraded":           params.get("sl_degraded", False),
        "tp_degraded":           params.get("tp_degraded", False),
        "breakout_ref_price":    signal.get("breakout_entry_price"),
        "max_correlation":       signal.get("_max_correlation", 0.0),
        "position_size_cap":     signal.get("position_size_cap", 0.5),
        "trailing_stop_activated": False,
        "judge_detail":          signal.get("judge_detail", {}),
        "entry_snap":            signal.get("_entry_snap", {}),
        "entry_recheck_result":  signal.get("_recheck_result", {}),
        "open_order_id":         open_order_id,
        "status":                "open",
        # ── 力度观测层（不影响执行） ──────────────────────────────────────────
        "momentum_coeff":        params.get("momentum_coeff"),
        "dynamic_target":        params.get("dynamic_target"),
        "consistency_score":     params.get("consistency_score"),
        "tp_vs_dynamic_atr":     params.get("tp_vs_dynamic_atr"),
        "liq_metrics":           params.get("liq_metrics"),         # [v3.88 OPT-14d] 只记录
        "tp1_liq_buffer_atr":    params.get("tp1_liq_buffer_atr"),  # [v3.88 OPT-14e] 重算时锁定
        # ── 问题一修复：atr_at_entry，_calc_trail_sl 动态止损依赖此字段 ──────
        "atr_at_entry":          atr,
        # ── funding_arb 专属字段（pos_state 里必须有，_patrol_funding_arb 会读这些字段）
        "sig_type":              signal.get("sig_type", signal.get("mode")),
        "funding_rate_at_entry": signal.get("funding_rate"),
        "funding_arb_target_ts": signal.get("next_settle_ts"),
        "funding_arb_max_hold_ts": time.time() + FUNDING_ARB_MAX_HOLD_HOURS * 3600,
        "funding_arb_hold_count": 0,
        # ── 市场环境快照（开仓时记录，供平仓后 trade_log 标注用）──────────
        "market_env":            signal.get("market_env", {}),
        # ── v3.66 复盘字段：原始清算区入场价 vs 实际入场价 ──────────────
        "liq_entry_raw":        params.get("liq_entry_raw"),
        "liq_entry_dist_atr":  params.get("liq_entry_dist_atr"),
        "entry_degraded_reason": params.get("entry_degraded_reason"),
        # ── 滑点记录（v3.71）──────────────────────────────────────────────────
        "entry_expected_price": params.get("entry_price", fill_price),
        "entry_slippage_pct":   round(
            (fill_price - params.get("entry_price", fill_price))
            / (params.get("entry_price", fill_price) + 1e-9) * 100, 4
        ) if params.get("entry_price") else None,
        "entry_slippage_u":     round(
            abs(fill_price - params.get("entry_price", fill_price)) * fill_qty, 4
        ) if params.get("entry_price") else None,
        "funding_cost_total":   0.0,     # 初始值，由 _maybe_record_funding_costs 累积
        "funding_cost_last":    None,
        "funding_last_settle_hour": -1,
        # ── 推送卡片 ID（供 position_manager 追加进展行用）───────────────────
        "_sig_id":              signal.get("_sig_id", ""),
    }
    if not _persist_position_record(_pos_rec, "position"):
        return None   # [v3.89 #52] 未落盘：不写交易日志/冷却，不返回假 pos_id
    _append_trade_log(pos_id, signal, fill_price, fill_qty, params)
    # v3.44: 写入同方向开仓冷協（防止同标的同方向短时间内连开）
    try:
        from shared.state import set_open_dir_cooldown as _set_odc
        from config import PM_COOLDOWN_OPEN_DIR_HOURS
        _set_odc(signal["symbol"], signal["direction"], PM_COOLDOWN_OPEN_DIR_HOURS)
    except Exception as _odc_write_e:
        logger.warning(f"[executor] open_dir_cooldown 写入失败: {_odc_write_e}")
    return pos_id


def _append_trade_log(
    pos_id: str, signal: dict, fill_price: float, fill_qty: float, params: dict
) -> None:
    """写入 trade_log.jsonl（每笔开仓一条记录）"""
    import datetime, pathlib, json as _json
    try:
        entry = {
            "ts":                 datetime.datetime.utcnow().isoformat(),
            "pos_id":             pos_id,
            "symbol":             signal.get("symbol"),
            "direction":          signal.get("direction"),
            "sig_type":           signal.get("sig_type", signal.get("mode")),
            "fill_price":         fill_price,
            "fill_qty":           fill_qty,
            "sl_price":           params.get("sl_price"),
            "tp1_price":          params.get("tp1_price"),
            "tp2_price":          params.get("tp2_price"),
            "rr_tp1":             params.get("rr_tp1"),
            "sl_dist_pct":        params.get("sl_dist_pct"),
            "liq_data_available": params.get("liq_data_available"),
            "tp_degraded":        params.get("tp_degraded"),
            "signal_strength":    signal.get("signal_strength"),
            "position_size_cap":  signal.get("position_size_cap"),
            "momentum_coeff":     params.get("momentum_coeff"),
            "dynamic_target":     params.get("dynamic_target"),
            "momentum_dims":      params.get("momentum_dims"),
            "consistency_score":  params.get("consistency_score"),
            "consistency_note":   params.get("consistency_note"),
            "tp_vs_dynamic_atr":  params.get("tp_vs_dynamic_atr"),
            "liq_metrics":           params.get("liq_metrics"),         # [v3.88 OPT-14d] 只记录
            "tp1_liq_buffer_atr":    params.get("tp1_liq_buffer_atr"),  # [v3.88 OPT-14e] 重算时锁定
            # funding_arb 专属字段
            "funding_rate_at_entry":     signal.get("funding_rate"),
            "funding_arb_target_ts":     signal.get("next_settle_ts"),
            "funding_arb_max_hold_ts":   __import__("time").time() + signal.get("judge_detail", {}).get("max_hold_seconds", FUNDING_ARB_MAX_HOLD_HOURS * 3600),
            "funding_arb_hold_count":    0,
            # stop_hunt 专属：强平量数据（v3.50）
            "liq_volume_at_trigger":     signal.get("judge_detail", {}).get("liq_volume_at_trigger", 0.0),
            "liq_volume_baseline":        signal.get("judge_detail", {}).get("liq_volume_baseline",   0.0),
        }
        log_path = pathlib.Path(__file__).parent / "logs" / "trade_log.jsonl"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a") as f:
            f.write(_json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as _te:
        logger.warning(f"[executor] trade_log 写入失败: {_te}")


async def _write_pending(signal: dict, order_id: int, params: dict,
                          leverage: int, expire_ts: int | None,
                          atr: float = 0.0) -> str | None:
    """写入挂单状态（pending）。atr_at_entry 供 _calc_trail_sl 动态止损使用。写入失败返回 None（[v3.89 #52]）。"""
    pos_id = str(uuid.uuid4())[:8]
    _pend_rec = {
        "pos_id":             pos_id,
        "symbol":             signal["symbol"],
        "direction":          signal["direction"].lower(),
        "mode":               signal.get("mode", "pullback"),
        "sig_type":           signal.get("sig_type", signal.get("mode", "pullback")),
        "volatility_tier":    signal.get("volatility_tier", "mid"),
        "entry_price":        params["entry_price"],
        "sl_price":           params["sl_price"],
        "tp1_price":          params["tp1_price"],
        "tp2_price":          params["tp2_price"],
        "leverage":           leverage,
        "open_order_id":      order_id,
        "sl_order_id":        None,
        "tp1_order_id":       None,
        "tp2_order_id":       None,
        "quantity":           signal.get("quantity", 0),
        "quantity_remaining": signal.get("quantity", 0),
        "liq_data_available": params.get("liq_data_available", False),
        "sl_degraded":        params.get("sl_degraded", False),
        "signal_strength":    signal.get("signal_strength", "medium"),
        "position_size_cap":  signal.get("position_size_cap", 0.5),
        "judge_detail":       signal.get("judge_detail", {}),
        "atr_at_entry":       atr,    # 为 _calc_trail_sl 动态止损提供 ATR 基准值
        # ── v3.69 触发时快照（供 maintain_pending_order 构造 WatchEntry 存活检查）─
        "ema20_at_open":   signal.get("ind", {}).get("ema20_1h", 0),
        "rsi_at_entry":    signal.get("ind", {}).get("rsi_1h", 50.0),
        "status":             "pending",
        "expire_ts":          expire_ts,
        "open_time":          int(time.time() * 1000),
    }
    if not _persist_position_record(_pend_rec, "pending"):
        return None
    return pos_id


# ─────────────────────────────────────────────────────────────────────────────
# 主函数（纯 async 版本 + 向后兼容同步包装器）
# ─────────────────────────────────────────────────────────────────────────────
async def execute_signal(signal: dict, ws_listener: Any = None) -> bool:
    """
    纯 async 主入口，供 scanner / signal_watcher 直接 await。
    ws_listener: WsOrderListener 实例（由 main 传入）。
    如果 ws_listener 为 None，使用轮询降级模式（测试/调试用）。
    返回 True 表示信号已处理（无论成交与否）。
    """
    return await _execute_async(signal, ws_listener)


def execute_signal_sync(signal: dict, ws_listener: Any = None) -> bool:
    """
    同步包装器（向后兼容）：在独立子线程 event loop 中运行 execute_signal。
    仅在无法 await 的同步上下文（如 run_in_executor 子线程）中使用。
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(execute_signal(signal, ws_listener))
    finally:
        loop.close()
        asyncio.set_event_loop(None)


# ── [fix #7/#8] 按 symbol 的开仓互斥锁 ───────────────────────────────────────────
# 主扫描/signal_watcher（主事件循环）与 funding_arb（execute_signal_sync 在独立线程+独立 event loop 中运行）
# 共用同一把锁，因此用线程锁（跨 event loop 有效）；异步侧用非阻塞轮询获取，不阻塞事件循环。
# 锁覆盖「预检(读 state) → 下单 → add_position 写 state」完整区间，消除先检查后执行的竞态窗口。
_SYMBOL_OPEN_LOCKS: dict = {}
_SYMBOL_OPEN_LOCKS_GUARD = threading.Lock()
_SYMBOL_LOCK_TIMEOUT = 90.0


def _get_symbol_open_lock(symbol: str) -> "threading.Lock":
    with _SYMBOL_OPEN_LOCKS_GUARD:
        lk = _SYMBOL_OPEN_LOCKS.get(symbol)
        if lk is None:
            lk = _SYMBOL_OPEN_LOCKS[symbol] = threading.Lock()
        return lk


async def _execute_async(signal: dict, ws_listener: Any = None) -> bool:
    symbol = signal.get("symbol", "")
    lk = _get_symbol_open_lock(symbol)
    deadline = time.time() + _SYMBOL_LOCK_TIMEOUT
    while not lk.acquire(blocking=False):
        if time.time() >= deadline:
            logger.warning(f"[executor] {symbol} 等待开仓互斥锁超时({_SYMBOL_LOCK_TIMEOUT:.0f}s)，放弃本次开仓")
            return False
        await asyncio.sleep(0.05)
    try:
        return await _execute_async_locked(signal, ws_listener)
    finally:
        lk.release()


async def _execute_async_locked(signal: dict, ws_listener: Any = None) -> bool:
    symbol    = signal.get("symbol", "")
    direction = (signal.get("direction") or "LONG").upper()
    mode      = signal.get("mode", "pullback")

    logger.info(f"[executor] >>> {symbol} {direction} {mode} 开始执行")

    # ── 步骤1：预检 ───────────────────────────────────────────────────────────
    ok, reason = _precheck(signal)
    if not ok:
        logger.info(f"[executor] {symbol} 预检拒绝: {reason}")
        # [v3.89 SHADOW] C 类：下单前检查被拒写入影子日志（只记录）
        try:
            from shared import shadow_trace as _st
            _st.record_precheck_rejection(signal, str(reason))
        except Exception:
            pass
        # [fix] 预检拒绝：完善卡片追加失败通知，不再推送「限价挂出」
        _sig_id_pre = signal.get("_sig_id", "")
        if _sig_id_pre:
            try:
                # 注意：本函数后面还有 `from shared.notifier import card_append, ...`，会让 card_append 成为局部变量，
                #   此处必须本地 import，否则 UnboundLocalError 被下面的 except 吞掉，卡片追加不出去
                from shared.notifier import card_append, card_remove, _hhmm
                # 卡片已在价格触发时 flush 发出：追加预检拒绝原因并清理卡片缓存
                #（卡片已发出，缓冲区无内容，无需丢弃）
                card_append(_sig_id_pre, f"\n❌ {_hhmm()} 预检拒绝\n {reason}")
                card_remove(_sig_id_pre)
            except Exception:
                pass
        return False

    # ── 步骤1.5：-2019 保证金暂停检查 ─────────────────────────────────────────
    try:
        import pathlib as _pl, json as _js, time as _tm
        _flag = Path(__file__).parent.parent / "ws_cache" / "margin_pause.json"
        if _flag.exists():
            _pause = _js.loads(_flag.read_text())
            if _tm.time() < _pause.get("until", 0):
                _remain = int((_pause["until"] - _tm.time()) / 60)
                logger.info(f"[executor] {symbol} 保证金暂停中（{_remain}分钟后恢复），跳过")
                return False
            else:
                _flag.unlink(missing_ok=True)  # 过期，清除
    except Exception:
        pass

    # ── 步骤2：拉取最新市场数据 ───────────────────────────────────────────────
    try:
        mkt = _fetch_market(symbol)
    except Exception as e:
        logger.error(f"[executor] {symbol} 市场数据拉取失败: {e}")
        raise PreOrderError(f"{symbol} 市场数据拉取失败: {e}") from e   # [v3.89 #102] 暂时性失败

    mark_price = mkt["mark_price"]
    atr        = mkt["atr"]
    ema20      = mkt["ema20"]
    k1h        = mkt["k1h"]

    # ── 步骤3：Coinglass 清算图 ───────────────────────────────────────────────
    liq_zones = _fetch_liq(symbol, mark_price=mark_price)

    # ── 步骤4：计算入场参数 ───────────────────────────────────────────────────
    params = calc_sl_tp(
        signal=signal,
        mark_price=mark_price,
        atr=atr,
        liq_zones=liq_zones,
        k1h=k1h,
        ema20=ema20,
    )
    if "error" in params:
        logger.warning(f"[executor] {symbol} calc_sl_tp 拒绝: {params['error']}")
        return False

    entry_price = params["entry_price"]
    sl_price    = params["sl_price"]
    tp1_price   = params["tp1_price"]
    tp2_price   = params["tp2_price"]
    entry_mode  = params.get("entry_mode", "immediate")

    # v3.19: 开仓参数完整记录，方便复盘时确认 SL/TP 是否合理
    logger.info(
        f"[executor] {symbol} 开仓参数 "
        f"entry={entry_price:.6g} sl={sl_price:.6g}("
        f"{abs(entry_price-sl_price)/entry_price*100:.1f}%) "
        f"tp1={tp1_price:.6g}("
        f"{abs(tp1_price-entry_price)/entry_price*100:.1f}%) "
        f"rr={params.get('rr_tp1','?')}x "
        f"tier={signal.get('volatility_tier','?')} "
        f"liq_ok={params.get('liq_data_available',False)} "
        f"sl_deg={params.get('sl_degraded',False)} "
        f"entry_mode={entry_mode}"
    )

    # ── v3.36.3 盈亏比门槛检查（TP1距离 ≥ SL距离 × 1.5，否则拒绝开仓）────
    # 任何胜率的策略在盈亏比<1时都是负期望，这是开仓的最基本数学约束
    try:
        _rr_sl   = abs(entry_price - sl_price)
        _rr_tp1  = abs(tp1_price   - entry_price)
        _rr_ratio = _rr_tp1 / (_rr_sl + 1e-9)
        # v3.40: 改从 config 读 RR 门槛（high tier 用 RR_MIN_TP1_HIGH=1.5，其他用 RR_MIN_TP1=2.0）
        try:
            from config import RR_MIN_TP1 as _RR_NORM, RR_MIN_TP1_HIGH as _RR_HIGH
        except ImportError:
            _RR_NORM, _RR_HIGH = 2.0, 1.5
        _tier_rr = signal.get("volatility_tier", "mid")
        _rr_min  = _RR_HIGH if _tier_rr == "high" else _RR_NORM
        if _rr_ratio < _rr_min:
            logger.warning(
                f"[executor] {symbol} 盈亏比不足: TP1距离={_rr_tp1/entry_price*100:.1f}% "
                f"SL距离={_rr_sl/entry_price*100:.1f}% RR={_rr_ratio:.2f}x < {_rr_min:.1f}(tier={_tier_rr})，拒绝开仓"
            )
            return False
        logger.debug(f"[executor] {symbol} 盈亏比={_rr_ratio:.2f}x ✅")
    except Exception as _rre:
        logger.debug(f"[executor] 盈亏比检查跳过: {_rre}")

    # ── 计算名义仓位（v3.36 止损定仓 + 强度系数）────────────────────────────
    # 核心公式：本金 × RISK_PER_TRADE ÷ 止损距离% × 强度系数
    # 每笔止损金额固定在账户余额的 RISK_PER_TRADE 以内，强信号适当放大，弱信号缩小
    # 用 assets[USDT].walletBalance 而非 totalWalletBalance：
    #   - walletBalance = 已实现的真实余额（平仓止盈/止损后立即更新）
    #   - totalWalletBalance 含浮盈，会在行情好时虚高，导致新仓位偏大
    acc        = api.get_account()
    usdt_wallet = 0.0
    for _asset in acc.get("assets", []):
        if _asset.get("asset") == "USDT":
            usdt_wallet = float(_asset.get("walletBalance", 0))
            break
    if usdt_wallet <= 0:
        usdt_wallet = float(acc.get("totalWalletBalance", 0))
    total_bal  = usdt_wallet
    _max_lev_for_notional = _get_max_leverage(symbol)  # 步骤6用，此处只缓存
    _effective_lev = DEFAULT_LEVERAGE

    # 从 config 读取风险比例常量
    try:
        from config import RISK_PER_TRADE as _RISK_PCT
    except ImportError:
        _RISK_PCT = 0.05  # 降级默认值

    # 1. 止损距离反推基础名义
    _sl_p_calc  = params.get("sl_price", 0)
    _ent_p_calc = params.get("entry_price", 0)
    _sl_dist_pct = abs(_ent_p_calc - _sl_p_calc) / (_ent_p_calc + 1e-9) if _ent_p_calc > 0 else 0
    # v3.57: 止损距离过小时拒绝开仓——防止仓位被反推至危险水平
    # 止损距离 < 0.5% 意味着 100U 本金 × RISK_PCT / 0.5% = 极大名义值，实际上是噪音止损
    _SL_DIST_MIN_PCT = 0.005
    if 0 < _sl_dist_pct < _SL_DIST_MIN_PCT:
        mode_v = signal.get("mode", "")
        # funding_arb 无需此限制（市场单，无止损距离概念）
        if mode_v != "funding_arb":
            logger.warning(
                f"[executor] {symbol} 止损距离过小: {_sl_dist_pct*100:.3f}% < {_SL_DIST_MIN_PCT*100:.1f}%，"
                f"拒绝开仓（防止名义仓位被反推至危险水平）"
            )
            return False
    if _sl_dist_pct > 0:
        _risk_notional = (total_bal * _RISK_PCT) / _sl_dist_pct
    else:
        # 无止损距离时降级：保守公式
        _risk_notional = total_bal * _effective_lev / max(RISK_MAX_POSITIONS, 1) * 0.3
        logger.warning(f"[executor] {symbol} 无止损距离，降级保守公式: {_risk_notional:.0f}U")

    # 2. 信号强度系数：用 final_score 线性映射（v3.36.4）
    # score=80→0.8, score=60→0.6, score=50→0.5，比三档固定更连续合理
    # 下限 0.4（防止极低分信号仓位过小无意义），上限 1.0
    _score = (signal.get("ind") or {}).get("final_score", 50)
    if not _score or _score <= 0:
        # 降级：从 signal_strength 三档映射
        _strength = (signal.get("signal_strength")
                     or (signal.get("ind") or {}).get("signal_strength", "medium")
                     or "medium")
        _score = {"strong": 75, "medium": 55, "weak": 40}.get(_strength, 55)
    _strength_mult = min(1.0, max(0.4, _score / 100))

    # 3. position_size_cap 处理
    # funding_arb: cap 直接等于最终强度系数（judges 已按费率强度设定），不经过 score 映射
    #   strong=0.5 → mult=0.5；经情绪降仓×0.6后=0.3，符合保守套利预期
    # 其他策略: cap 作为 score-based mult 的上限约束，同时承载情绪降仓链路
    _cap = signal.get("position_size_cap")
    _is_arb = signal.get("mode") == "funding_arb" or signal.get("sig_type") == "funding_arb"
    if _is_arb:
        # funding_arb: cap 就是 mult，情绪降仓已通过 scanner 层写入 cap
        if _cap and float(_cap) > 0:
            _strength_mult = min(float(_cap), 1.0)
        # else: 保持 score 派生的 _strength_mult（降级兜底）
    else:
        # 普通策略: cap 作为上限约束
        if _cap and float(_cap) > 0:
            _cap_mult = min(float(_cap), 1.0)
            _strength_mult = min(_strength_mult, _cap_mult)

    # v3.59: sl_degraded=True 时降仓位 50%（Opus 4.5 发现：实盘 100% 套是降级，风控名存实亡）
    # 降级止损 = entry ± 1.5ATR，不在结构位上，风险更高，用小仓位补偿
    _sl_deg = params.get("sl_degraded", False) if params else False
    if _sl_deg and not _is_arb:
        _old_mult = _strength_mult
        _strength_mult = round(_strength_mult * 0.5, 4)
        logger.info(
            f"[executor] {symbol} sl_degraded=True，降级止损位置不在结构位，仓位降50%: "
            f"{_old_mult:.2f} → {_strength_mult:.2f}"
        )

    # ── Kelly 仓位动态调整（KELLY_ENABLED=False 时跳过）────────────────────────
    try:
        from tools.kelly_sizer import get_kelly_size_cap as _kelly_cap_fn
        _kelly_result = _kelly_cap_fn(
            mode      = mode,
            strength  = signal.get("signal_strength", ""),
            market_mode = signal.get("market_mode", ""),
            direction = direction,
        )
        if _kelly_result is not None:
            _old_mult = _strength_mult
            _strength_mult = min(_strength_mult, _kelly_result)
            signal["kelly_adjusted"] = True
            logger.info(
                f"[executor] {symbol} Kelly 调整: cap={_kelly_result:.3f} "
                f"mult {_old_mult:.2f}→{_strength_mult:.2f}"
            )
    except Exception as _ke:
        logger.debug(f"[executor] Kelly 调整跳过: {_ke}")

    notional = round(_risk_notional * _strength_mult, 2)

    logger.info(
        f"[executor] {symbol} 止损定仓: 余额={total_bal:.0f}U × {_RISK_PCT*100:.0f}% "
        f"÷ SL={_sl_dist_pct*100:.1f}% × mult={_strength_mult:.2f}"
        f"({'arb直用cap' if _is_arb else f'score={_score}'} cap={_cap}) = {notional:.0f}U"
    )

    # ── Vol Regime 仓位调节（BTC/ETH 适用，山寨自动返回 1.0）─────────────────
    try:
        from shared.vol_regime import get_vol_multipliers
        _vm = get_vol_multipliers(symbol)
        _size_mult = _vm["size"]
        if _size_mult != 1.0:
            notional = round(notional * _size_mult, 2)
            logger.info(f"[executor] {symbol} vol_regime 仓位乘数 ×{_size_mult} → notional={notional:.0f}U")
    except Exception as _ve:
        logger.debug(f"[executor] vol_regime 跳过: {_ve}")

    # ── 风险上限二次确认（防止 vol_regime 放大后超标）────────────────────────
    if _sl_dist_pct > 0:
        _max_risk_notional = round((total_bal * _RISK_PCT) / _sl_dist_pct, 2)
        if notional > _max_risk_notional:
            logger.info(
                f"[executor] {symbol} 风险上限二次截断: {notional:.0f}U → {_max_risk_notional:.0f}U"
            )
            notional = _max_risk_notional

    # ── 账户状态感知仓位调整（亏损连续/当日亏损过大时降仓）────────────────────
    notional = _apply_account_state_scale(signal, notional)

    # ── 保证金偏紧时自动缩小仓位（_MARGIN_SCALE 由 _account_final_check 写入）──
    _scale = _MARGIN_SCALE[0]
    if _scale < 1.0:
        old_n = notional
        notional = round(notional * _scale, 2)
        logger.info(f"[executor] {symbol} 保证金偏紧 scale={_scale:.2f}，仓位缩小 {old_n:.0f}→{notional:.0f}U")
    # 如果 notional 低于最小保护值，尝试提升——但不能违反风险上限（v3.36.3）
    if notional < MIN_NOTIONAL_GUARD:
        # 风险上限约束：提升后的名义仓位不能超过 RISK_PER_TRADE / sl_dist_pct
        _min_guard_cap = MIN_NOTIONAL_GUARD
        if _sl_dist_pct > 0:
            _max_risk_guard = round((total_bal * _RISK_PCT) / _sl_dist_pct, 2)
            _min_guard_cap  = min(MIN_NOTIONAL_GUARD, _max_risk_guard)
        min_margin = _min_guard_cap / max(_effective_lev, DEFAULT_LEVERAGE) * 1.1
        if float(acc.get("availableBalance", 0)) >= min_margin and _min_guard_cap >= 5.0:
            if _min_guard_cap < MIN_NOTIONAL_GUARD:
                logger.info(
                    f"[executor] {symbol} MIN_NOTIONAL_GUARD 受风险上限约束: "
                    f"{MIN_NOTIONAL_GUARD:.0f}U → {_min_guard_cap:.0f}U "
                    f"(SL={_sl_dist_pct*100:.1f}%，强制提升会超5%风险)"
                )
            notional = _min_guard_cap
            logger.debug(f"[executor] {symbol} 名义仓位提升至保护值 {_min_guard_cap:.0f}U")
        else:
            # 余额真的不够，跳过
            pass
    if notional < MIN_NOTIONAL_GUARD:
        logger.warning(
            f"[executor] {symbol} 名义仓位 {notional:.2f}U < 最小保护值 {MIN_NOTIONAL_GUARD}U，"
            f"账户余额不足以安全开仓，跳过（不报错）"
        )
        return False

    # ── 步骤5：账户终检 ───────────────────────────────────────────────────────
    ok, reason = _account_final_check(notional)
    if not ok:
        logger.warning(f"[executor] {symbol} 账户终检失败: {reason}")
        # [v3.89 #102] 账户查询失败是暂时性故障（可重试）；余额/名义/回撤等业务拒绝仍 return False
        if str(reason).startswith("账户查询失败"):
            raise PreOrderError(f"{symbol} {reason}")
        return False

    # ── 杠杆利用率预算（v3.90）：在终检通过之后、设置杠杆/下单之前，把名义仓位截断到该方向剩余预算 ──
    _lev_ok, notional, _lev_reason = _apply_leverage_budget(symbol, direction, notional, total_bal)
    if not _lev_ok:
        return False

    # ── 步骤6：设置全仓 + 最大杠杆 ───────────────────────────────────────────
    leverage = _setup_symbol(symbol)

    # 根据实际杠杆重新验证保证金（重新拉取余额，避免用步骤5的旧快照）
    _acc_fresh = api.get_account()
    margin_needed = notional / leverage * 1.1
    available_bal = float(_acc_fresh.get("availableBalance", 0))
    if available_bal < margin_needed:
        logger.warning(f"[executor] {symbol} 可用余额不足（杠杆{leverage}x）: "
                       f"{available_bal:.2f}U < {margin_needed:.2f}U")
        return False

    # ── 计算数量 ──────────────────────────────────────────────────────────────
    precision = _get_precision(symbol)
    quantity  = _round_qty(notional / entry_price, precision["step"])
    if quantity <= 0:
        logger.warning(f"[executor] {symbol} 计算数量异常: {quantity}")
        return False

    # ── 步骤7：提交开仓限价单 ─────────────────────────────────────────────────
    # 挂单随机延迟（反猎杀），防止价格精确落在技术位被做市商识别
    try:
        from config import ORDER_ENTRY_RANDOMIZE, ORDER_ENTRY_DELAY_MIN_S, ORDER_ENTRY_DELAY_MAX_S
        if ORDER_ENTRY_RANDOMIZE:
            _delay = random.uniform(ORDER_ENTRY_DELAY_MIN_S, ORDER_ENTRY_DELAY_MAX_S)
            logger.info(f"[executor] {symbol} 随机延迟挂单 {_delay:.0f}s（反猎杀）")
            await asyncio.sleep(_delay)
    except Exception:
        pass

    # v3.6: expire_ts 在此统一计算，传给 _place_open_order，不在函数内重复计算
    if mode == "breakout":
        expire_ts = int((time.time() + BREAKOUT_ORDER_EXPIRE_MIN * 60) * 1000)
    elif entry_mode == "pending_order" and mode == "oi_divergence":
        # v3.64: OI 背离限价单 GTD，有效期由 config.OI_DIV_ORDER_EXPIRE_MIN 控制
        try:
            from config import OI_DIV_ORDER_EXPIRE_MIN as _oi_exp_min
        except ImportError:
            _oi_exp_min = 60  # 默认 1H
        expire_ts = int((time.time() + _oi_exp_min * 60) * 1000)
    elif entry_mode == "pending_order":
        expire_ts = int((time.time() + 4 * 3600) * 1000)
    else:
        expire_ts = None

    order_id, err = _place_open_order(
        symbol, direction, mode, entry_price, quantity, precision,
        notional=notional,
        entry_mode_hint=entry_mode,
        expire_ts=expire_ts,
    )
    if order_id is None:
        logger.error(f"[executor] {symbol} 开仓单提交失败: {err}")
        # [fix] 下单失败：追加失败行并 flush，确保用户收到失败通知
        _sig_id_fail = signal.get("_sig_id", "")
        if _sig_id_fail:
            try:
                from shared.notifier import card_append, flush_signal
                card_append(_sig_id_fail, f"\n❌ 挂单失败：{err[:120]}")
                flush_signal(_sig_id_fail)
            except Exception:
                pass
        return False

    # 下单成功：现在 flush 「限价挂出」这一行（之前在 signal_watcher 延迟）
    _sig_id_ok = signal.get("_sig_id", "")
    if _sig_id_ok:
        try:
            from shared.notifier import flush_signal as _fsig
            _fsig(_sig_id_ok)
        except Exception:
            pass

    # ── 步骤8：REST 轮询确认开仓成交（WS不可靠时降级）────────────────────────
    # 限价单挂出后立刻查一次，未成交则不撤单，记 pending 等后续重试
    fill = None
    try:
        loop_ref = asyncio.get_event_loop()
        od = await loop_ref.run_in_executor(None, lambda: api.get_order(symbol, order_id))
        if od and od.get("status") == "FILLED":
            _fq_a = float(od.get("executedQty", 0))
            if _fq_a > 0:
                fill = {
                    "fill_price": float(od.get("avgPrice") or od.get("price", 0)),
                    "fill_qty":   _fq_a,
                }
                logger.info(f"[executor] {symbol} REST确认已成交: price={fill['fill_price']:.6g}")
            else:
                logger.warning(f"[executor] {symbol} REST返回FILLED但executedQty=0，视同未成交")
        # 不撤单！让限价单继续挂着等撮合
    except Exception as rest_err:
        logger.warning(f"[executor] {symbol} REST确认失败: {rest_err}")

    if fill is None:
        # REST 也未确认成交 → 写 pending + 启动 30s 异步重试
        logger.info(f"[executor] {symbol} orderId={order_id} 未成交，记录挂单，30s后重试确认")
        signal["quantity"]   = quantity
        jd = signal.get("judge_detail") or {}
        jd.setdefault("vol_ratio", signal.get("ind", {}).get("vol_ratio", 0))
        signal["judge_detail"] = jd
        _pend_id = await _write_pending(signal, order_id, params, leverage, expire_ts, atr=atr)
        if _pend_id is None:   # [v3.89 #52] 日志已在 _persist_position_record 内记录
            return False

        # v3.6: 删除 ensure_future(_retry_confirm_30s)
        # 原实现在子线程 loop（execute_signal_sync 用 new_event_loop），
        # loop.close() 后 task 永远不会执行，30s 重试实际无效。
        # patrol_once 每5分钟的 _check_tp_filled_rest 已完全覆盖此补救路径。
        return True

    fill_price = fill["fill_price"]
    fill_qty   = fill["fill_qty"]
    logger.info(f"[executor] {symbol} 成交 price={fill_price:.6g} qty={fill_qty}")

    # ── 步骤8.5：开仓后场景二次验证 ──────────────────────────────────────────
    # 开仓时快照（成交回报收到后立即采样真实市场状态）
    closes_1h_now = [float(k[4]) for k in k1h]
    entry_snap = {
        "ema20_1h":    ema20,
        "ema50_1h":    _calc_ema(closes_1h_now, 50) if len(closes_1h_now) >= 50 else ema20,
        "rsi_1h":      _calc_rsi(closes_1h_now, 14),
        "rsi_4h":      signal.get("judge_detail", {}).get("rsi_4h", 50.0),
        "oi":          mkt["oi_now"],
        "funding_rate": mkt["funding"],
        "mark_price":  mark_price,
    }

    verdict, recheck_result = await _entry_recheck(
        symbol=symbol, direction=direction, mode=mode,
        fill_price=fill_price, fill_qty=fill_qty,
        entry_snap=entry_snap, params=params,
    )

    if verdict == "CLOSE":
        # 市价平仓
        close_side = "SELL" if direction == "LONG" else "BUY"
        close_price = 0.0
        try:
            close_resp = api.place_order(
                symbol=symbol, side=close_side,
                order_type="MARKET",
                quantity=fill_qty,
                positionSide="LONG" if direction == "LONG" else "SHORT",
            )
            close_price = float(close_resp.get("avgPrice", mark_price))
        except Exception as ce:
            # [v3.87 STEP6-2] 平仓失败 → 仓位状态未知。原写法只发通知，然后继续往下算盈亏并 return True，
            #   调用方以为本次开仓流程已完成。现按评审决定：发告警后 return False，不写持仓记录、不挂保护单
            #   （仓位状态未知时挂保护单可能造成更大混乱，交人工处理）。
            #   注意：此时仓位不在系统记录里，机器人不会跟踪它，必须人工确认。
            logger.error(f"[executor] {symbol} 二次验证CLOSE 平仓失败: {ce}")
            await notify_async(
                f"🚨 {symbol} 二次验证平仓失败，需要人工确认！\n"
                f"方向:{direction} 成交价:{fill_price:.6g} 数量:{fill_qty}\n"
                f"复核结论:{recheck_result.get('reason', '')}\n"
                f"平仓错误:{ce}\n"
                f"本次未写入持仓记录、未挂止损止盈，请立即人工检查该仓位。"
            )
            return False

        pnl_est = (close_price - fill_price) * fill_qty if direction == "LONG"                   else (fill_price - close_price) * fill_qty
        logger.info(f"[executor] {symbol} 二次验证平仓 价格={close_price} PnL估={pnl_est:.2f}U")
        # 向开仓卡片追加节点
        try:
            _sig_id_r = signal.get("_sig_id", "")
            _recheck_line = fmt_node_immediate_close(recheck_result.get('reason', ''))
            if _sig_id_r:
                await card_append_async(_sig_id_r, _recheck_line)
            else:
                await notify_async(_recheck_line)
        except Exception:
            pass
        return True   # 信号处理完成，不进入正常持仓流程

    # HOLD 路径：继续挂止损止盈

    # ── 步骤9：并发提交止损止盈单 ─────────────────────────────────────────────
    guard = await _place_guards_async(
        symbol, direction, fill_qty,
        sl_price, tp1_price, tp2_price,
        precision,
    )

    # ── 步骤11：写入持仓状态 ──────────────────────────────────────────────────
    signal["quantity"] = fill_qty
    signal["_entry_snap"]     = entry_snap
    signal["_recheck_result"] = recheck_result
    # 把 vol_ratio 写入 judge_detail 方便事后分析
    jd = signal.get("judge_detail") or {}
    jd.setdefault("vol_ratio", signal.get("ind", {}).get("vol_ratio", 0))
    signal["judge_detail"] = jd
    pos_id = _write_position(
        signal, fill_price, fill_qty, leverage, guard, params, order_id, atr=atr
    )
    if pos_id is None:   # [v3.89 #52] 日志已在 _persist_position_record 内记录；不继续发通知，不打印「流程完成」
        return False

    # 开仓通知：向信号卡片追加开仓成交行
    try:
        _sig_id = signal.get("_sig_id", "")
        _open_line = fmt_node_open(
            fill_price  = fill_price,
            sl_price    = sl_price,
            tp1_price   = tp1_price,
            tp2_price   = tp2_price,
            leverage    = leverage,
            notional    = params.get("notional", fill_price * fill_qty),
            rr_tp1      = params.get("rr_tp1", 0),
            rr_tp2      = params.get("rr_tp2"),
            entry_price = params.get("entry_price"),
            sl_degraded = bool(params.get("sl_degraded") or params.get("tp_degraded")),
        )
        if _sig_id:
            await card_append_async(_sig_id, _open_line)
            # [v3.90 通知聚合] 开仓成交：确认值得推送，合并发出监控行 + 触发行 + 开仓行
            await asyncio.get_event_loop().run_in_executor(None, lambda: flush_signal(_sig_id))
        else:
            # immediate 模式没有卡片，用 imm_ 前缀创建一张临时卡片
            # [v3.90 通知聚合] 统一走缓冲区：先 buffer_signal 再立即 flush（行为等价，路径统一）
            from shared.notifier import make_card_header, buffer_signal
            _hdr = make_card_header(symbol, direction, mode, signal.get("signal_strength", "medium"))
            _imm_sig_id = f"{symbol}_imm_{int(time.time())}"
            signal["_sig_id"] = _imm_sig_id
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: buffer_signal(_imm_sig_id, _hdr)
            )
            await asyncio.get_event_loop().run_in_executor(None, lambda: flush_signal(_imm_sig_id))
            await card_append_async(_imm_sig_id, _open_line)
    except Exception as _ne:
        logger.debug(f"[executor] 开仓卡片通知失败（非致命）: {_ne}")

    # ── 步骤10：止损止盈联动监听 ─────────────────────────────────────────────
    # ws_listener 在主线程 loop 里，不能在子线程 loop 里 fire ensure_future
    # 联动监听由 PositionManager 的 patrol_loop 负责（每5分钟巡检）
    pass  # _guard_listener 暂由 patrol 机制接管

    logger.info(f"[executor] {symbol} 流程完成 pos_id={pos_id}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# 降级轮询（ws_listener=None 时使用）
# ─────────────────────────────────────────────────────────────────────────────
def _poll_fill(symbol: str, order_id: int, timeout_sec: int = 8) -> dict | None:
    """
    轮询检测订单是否已立刻成交（默认只等 8 秒）。
    超时后直接返回 None，由上层写 pending 记录，
    由 patrol 按 ORDER_EXPIRE_*_MIN 决定何时撤单。
    【不在此处撤单】
    """
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            orders = api.get_open_orders(symbol)
            ids = [int(o.get("orderId", 0)) for o in orders]
            if order_id not in ids:
                # 订单消失 → 检查持仓是否进来了（已成交）
                pos = api.get_position(symbol)
                for p in pos:
                    if abs(float(p.get("positionAmt", 0))) > 0:
                        ep = float(p.get("entryPrice", 0))
                        return {"fill_price": ep, "fill_qty": abs(float(p.get("positionAmt", 0)))}
                # 订单消失但无持仓 → 可能被外部撤销，返回 None
                return None
        except Exception as e:
            logger.warning(f"[executor] 轮询失败: {e}")
        time.sleep(2)
    # 超时：订单仍挂着，属于正常 pending 状态，不撤单
    logger.info(f"[executor] {symbol} orderId={order_id} 8s内未成交，保留挂单等待回踩")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# WsOrderListener — 从 shared.ws_listener 引入正式实现
# ─────────────────────────────────────────────────────────────────────────────
try:
    from shared.ws_listener import WsOrderListener  # noqa: F401
except ImportError:
    # 降级存根（测试环境 / 缺少 websockets 包时）
    class WsOrderListener:  # type: ignore
        async def wait_order_event(self, order_id: int, timeout: float = 2.0):
            return None
        async def wait_any_order_event(self, order_ids, timeout: float = 5.0):
            return None

# 由 main.py 在启动时注入全局监听器实例，供独立调用 execute_signal() 时使用
_LISTENER: "WsOrderListener | None" = None


def _crash_cd_allows(reason: str, direction: str) -> bool:
    """[fix-1] crash_guard 冷却只拦对应方向：crash_guard_short 只拦 SHORT，crash_guard 只拦 LONG。"""
    d = (direction or "").upper()
    return (reason == "crash_guard_short" and d == "LONG") or (reason == "crash_guard" and d == "SHORT")
