"""
shared/opt_v387.py — v3.87 开仓场景重构（第一批）的纯函数，便于单测与评审。

统一原则（四要素）：触发 → 确认 → 在确认处入场 → 止损放在「开仓理由失效位」外侧。
"""
from __future__ import annotations

import math as _math


def _finite(x) -> bool:
    """[v3.89 #73] 是有限实数（排除 NaN/Inf/bool/非数值）。"""
    return (isinstance(x, (int, float)) and not isinstance(x, bool)
            and _math.isfinite(x))


def invalidation_sl(entry: float, atr: float, direction: str, k1h_closed: list,
                    lookback: int = 8, buf_atr: float = 0.3,
                    min_atr: float = 1.0, max_atr: float = 3.0) -> float | None:
    """
    失效位止损：做多取最近 lookback 根已闭合 1H 的最低点 - buf_atr×ATR（做空对称）。
    - 距入场 < min_atr×ATR：拉到 min_atr×ATR（避免被正常噪声扫掉）
    - 距入场 > max_atr×ATR：返回 None（结构太远，交给原止损逻辑）
    只使用币安官方 K 线数据。
    """
    # [v3.89 #73] 输入契约：价格/ATR/参数须为有限数，回看周期为正整数，上下界关系成立，K 线形状合法；
    #   不满足一律返回 None（沿用「交给原止损逻辑」），不抛异常、不产生异常止损价。合法输入结果不变。
    if not (_finite(entry) and _finite(atr) and _finite(buf_atr) and _finite(min_atr) and _finite(max_atr)):
        return None
    if entry <= 0 or atr <= 0 or not k1h_closed:
        return None
    if isinstance(lookback, bool) or not isinstance(lookback, int) or lookback <= 0 or not (0 < min_atr <= max_atr):
        return None
    bars = k1h_closed[-lookback:]
    try:
        if not all(_finite(float(k[2])) and _finite(float(k[3])) and float(k[2]) >= float(k[3]) for k in bars):
            return None
    except (TypeError, ValueError, IndexError):
        return None
    d = (direction or "").upper()
    if d == "LONG":
        sl = min(float(k[3]) for k in bars) - buf_atr * atr
        dist = (entry - sl) / atr
        if dist > max_atr:
            return None
        return entry - min_atr * atr if dist < min_atr else sl
    if d == "SHORT":
        sl = max(float(k[2]) for k in bars) + buf_atr * atr
        dist = (sl - entry) / atr
        if dist > max_atr:
            return None
        return entry + min_atr * atr if dist < min_atr else sl
    return None


def no_new_extreme(k1h_closed: list, direction: str, n: int = 3) -> bool:
    """
    OI 背离确认：做多要求最后一根已闭合 1H 的低点没有跌破前 n 根的最低点（卖压衰竭）；
    做空要求最后一根的高点没有突破前 n 根的最高点（买盘衰竭）。
    """
    # [v3.89 #73] n 须为正整数；K 线形状/数值非法按「无新极值」之外的保守结果 False 返回，不抛异常
    if isinstance(n, bool) or not isinstance(n, int) or n <= 0:
        return False
    if len(k1h_closed) < n + 1:
        return False
    try:
        if not all(_finite(float(k[2])) and _finite(float(k[3])) for k in k1h_closed[-(n + 1):]):
            return False
    except (TypeError, ValueError, IndexError):
        return False
    last, prev = k1h_closed[-1], k1h_closed[-(n + 1):-1]
    if (direction or "").upper() == "LONG":
        return float(last[3]) > min(float(k[3]) for k in prev)
    if (direction or "").upper() == "SHORT":
        return float(last[2]) < max(float(k[2]) for k in prev)
    return False
