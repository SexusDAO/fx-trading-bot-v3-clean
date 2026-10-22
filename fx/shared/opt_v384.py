"""
shared/opt_v384.py — v3.84 第一轮优化的纯函数集合（无 I/O，便于单测与评审）

每个函数对应 OPTIMIZATION_CHANGELOG.md 中的一条优化，开关见 config.py「v3.84 优化开关」。
关闭开关即回到原行为，便于对照与回滚。
"""
from __future__ import annotations

import math as _math


def _finite(x) -> bool:
    """[v3.89 #73] 是有限实数（排除 NaN/Inf/bool/非数值）。"""
    return (isinstance(x, (int, float)) and not isinstance(x, bool)
            and _math.isfinite(x))


def stagnation_loss_threshold(sl_dist_pct: float, enabled: bool = True,
                              frac: float = 0.6, floor: float = 0.02) -> float:
    """
    提前滞涨平仓（early_stagnation）的「直接平仓」浮亏阈值（小数，0.02 = 2%）。

    【原逻辑】持仓满 6H（reversal 8H）且浮亏 > 固定 2% 时直接市价平仓。
    【问题】止损距离按波动层最小 2.5%/3%/5%，固定 2% 等于在止损之前又加了一道更紧的「隐形止损」，
            会把本来可能回归的单子提前亏掉，直接抬高实现亏损次数（金融上：时间止损应与止损距离成比例，
            否则宽止损标的被过度惩罚、窄止损标的又保护不足）。
    【新逻辑】阈值 = max(floor, frac × 止损距离)。止损 3% → 2%（不变）；止损 5% → 3%。
    sl_dist_pct 取不到（<=0）时回退到 floor，行为与原逻辑一致。
    """
    # [v3.89 #73] 非有限数（NaN/Inf）与非数值按「取不到」处理，回退 floor；合法输入结果不变
    if not enabled or not _finite(sl_dist_pct) or sl_dist_pct <= 0:
        return floor
    return max(floor, frac * sl_dist_pct)


def tp1_protect_action(direction: str, sl_now: float, entry: float,
                       fav: float, enabled: bool = True) -> str:
    """
    TP1 成交后的「保本保护」应执行的动作：'close' / 'move_sl' / 'none'。

    【原逻辑冲突】触发条件是「浮盈 fav < 0（价格已跌穿入场价）」，动作却是把止损移到入场价上方 0.1%
    （多单）/ 下方 0.1%（空单）。此时止损价在当前价的「错误一侧」，卖出止损单会被交易所当作立即触发，
    要么被拒单（-2021），要么以市价成交，行为不可预期。
    【新逻辑】价格已越过入场价且止损仍在亏损侧 → 直接市价平掉剩余仓位，锁定 TP1 已兑现的利润；
            价格仍在入场价有利一侧时才保持原来的「移动止损到保本」。
    止损已在保本侧（多单 sl>=entry / 空单 sl<=entry）→ 'none'，交给交易所止损单处理。
    """
    # [v3.89 #73] 价格/浮盈非有限数时无法判断，返回 'none'（交给交易所止损单），
    #   而不是让 NaN 比较为 False 后落入 'move_sl'。合法输入结果不变。
    if not (_finite(sl_now) and _finite(entry) and _finite(fav)):
        return "none"
    d = (direction or "").upper()
    if d == "LONG":
        sl_on_loss_side = sl_now < entry
    elif d == "SHORT":
        sl_on_loss_side = sl_now > entry
    else:
        return "none"
    if not sl_on_loss_side:
        return "none"
    if fav < 0 and enabled:
        return "close"
    return "move_sl"


def pullback_entry_params(enabled: bool = True, deg_atr: float = 0.5,
                          min_atr: float = 0.3, max_atr: float = 2.0) -> dict:
    """
    回踩入场价在「无清算区」降级时的 ATR 参数（做多/做空）。

    【原逻辑】做多降级 mark-0.3ATR（最小 0.15ATR，上限 1.5ATR）；做空降级 mark+0.8ATR（最小 0.4ATR，上限 2.5ATR）。
    【问题】多空不对称：做多只回踩 0.3ATR，落在正常噪声内，容易在趋势末端被动成交（归档回踩做多 5 笔仅 1 笔盈利，样本小仅作线索）；
            做空要等 0.8ATR，成交机会被明显压缩，开仓次数少。
    【新逻辑】两侧统一为 deg_atr=0.5、最小 min_atr=0.3、上限 max_atr=2.0：做多更深一点的回踩（价格更优），
            做空更容易成交（增加开仓次数）。需用回测/模拟盘评估成交率与胜率的变化。
    """
    # [v3.89 #73] 参数需为有限正数且 min<=max、min<=deg<=max；否则视为非法，回退到原（未启用）参数组，
    #   不把反向上下界或 NaN 当成新参数使用。合法输入结果不变。
    if enabled and not (all(_finite(v) and v > 0 for v in (deg_atr, min_atr, max_atr))
                        and min_atr <= deg_atr <= max_atr):
        enabled = False
    if enabled:
        return {"long_deg": deg_atr, "long_min": min_atr, "long_max": max_atr,
                "short_deg": deg_atr, "short_min": min_atr, "short_max": max_atr}
    return {"long_deg": 0.3, "long_min": 0.15, "long_max": 1.5,
            "short_deg": 0.8, "short_min": 0.4, "short_max": 2.5}
