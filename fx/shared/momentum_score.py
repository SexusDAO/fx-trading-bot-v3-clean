"""
shared/momentum_score.py — 行情力度系数计算

四个维度综合成一个力度系数 momentum_coeff（0.6 ~ 1.4），中性市场 = 1.0：

做多方向：
  1. RSI 位置与方向  （低位回升 → 加权；中段回升 → 中性；高位 → 降权）
  2. 成交量确认      （量比 > 1.5 → 加权；量比 < 0.8 → 降权）
  3. 价格动能        （近3根1H均涨幅 → 越高越强）
  4. 资金费率方向    （偏负 → 空头拥挤，反弹助推力更大 → 加权）

做空方向镜像。

输出：
{
    "momentum_coeff":    float,   # 0.6 ~ 1.4
    "dynamic_target":    float,   # 动态目标价
    "dim_rsi":           float,   # RSI 分项贡献
    "dim_vol":           float,   # 量比分项贡献
    "dim_price_momentum":float,   # 价格动能分项贡献
    "dim_funding":       float,   # 资金费率分项贡献
    "base_rr":           float,   # 基础 RR（固定 2.0）
}
"""

from __future__ import annotations
import logging

logger = logging.getLogger("fx.momentum_score")

_BASE_RR     = 2.0   # 基础盈亏比
_COEFF_MIN   = 0.6
_COEFF_MAX   = 1.4


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _rsi_dim(rsi: float, direction: str) -> float:
    """
    RSI 分项，输出 [-0.2, +0.2]
    做多：RSI 在 20~35 区间 → +0.2（超卖弹力大）
          RSI 在 35~50 区间 → +0.1（中等）
          RSI 在 50~65 区间 → 0.0（中性）
          RSI > 65          → -0.2（追多空间有限）
    做空镜像。
    """
    if direction == "LONG":
        if rsi <= 30:
            return 0.20
        if rsi <= 40:
            return 0.12
        if rsi <= 50:
            return 0.05
        if rsi <= 60:
            return 0.0
        if rsi <= 70:
            return -0.10
        return -0.20
    else:
        if rsi >= 70:
            return 0.20
        if rsi >= 60:
            return 0.12
        if rsi >= 50:
            return 0.05
        if rsi >= 40:
            return 0.0
        if rsi >= 30:
            return -0.10
        return -0.20


def _vol_dim(vol_ratio: float) -> float:
    """
    量比分项，输出 [-0.15, +0.15]
    量比 > 2.0 → +0.15（强放量）
    量比 1.5~2.0 → +0.08
    量比 0.8~1.5 → 0.0（正常）
    量比 < 0.8  → -0.15（缩量反弹，不可靠）
    """
    if vol_ratio >= 2.0:
        return 0.15
    if vol_ratio >= 1.5:
        return 0.08
    if vol_ratio >= 0.8:
        return 0.0
    return -0.15


def _price_momentum_dim(k1h: list, direction: str) -> float:
    """
    价格动能：近3根1H K线的平均涨/跌幅，输出 [-0.15, +0.15]
    做多：均涨 > 1.5% → +0.15；均涨 0.5%~1.5% → +0.08；负涨 → -0.10
    做空镜像。
    """
    if len(k1h) < 3:
        return 0.0
    recent3 = k1h[-3:]
    if direction == "LONG":
        changes = [
            (float(k[4]) - float(k[1])) / (float(k[1]) + 1e-9) * 100
            for k in recent3
        ]
    else:
        changes = [
            (float(k[1]) - float(k[4])) / (float(k[1]) + 1e-9) * 100
            for k in recent3
        ]
    avg = sum(changes) / 3
    if avg >= 1.5:
        return 0.15
    if avg >= 0.5:
        return 0.08
    if avg >= 0.0:
        return 0.0
    return -0.10


def _funding_dim(funding: float, direction: str) -> float:
    """
    资金费率分项，输出 [-0.10, +0.10]
    做多：funding < -0.001（偏负）→ +0.10（空头拥挤，反弹时空头被迫平仓助推）
          funding > +0.001（偏正）→ -0.10（多头拥挤，上行压力大）
          中性 → 0.0
    做空镜像。
    """
    if direction == "LONG":
        if funding < -0.001:
            return 0.10
        if funding < 0.0:
            return 0.05
        if funding <= 0.001:
            return 0.0
        return -0.10
    else:
        if funding > 0.001:
            return 0.10
        if funding > 0.0:
            return 0.05
        if funding >= -0.001:
            return 0.0
        return -0.10


def calc_momentum(
    direction: str,
    rsi_1h: float,
    vol_ratio: float,
    k1h: list,
    funding: float,
    entry_price: float,
    sl_price: float,
) -> dict:
    """
    计算力度系数和动态目标价。

    参数：
      direction    : "LONG" | "SHORT"
      rsi_1h       : 当前1H RSI
      vol_ratio    : 当前量比（当前量 / 近20根均量）
      k1h          : 已闭合1H K线列表（至少需要3根）
      funding      : 当前资金费率（小数，如 0.0001）
      entry_price  : 计划入场价
      sl_price     : 计划止损价

    返回 dict：
      momentum_coeff     : float  0.6 ~ 1.4
      dynamic_target     : float  动态目标价
      dim_rsi            : float  RSI 分项贡献
      dim_vol            : float  量比分项贡献
      dim_price_momentum : float  价格动能分项贡献
      dim_funding        : float  资金费率分项贡献
      base_rr            : float  基础 RR = 2.0
    """
    direction = direction.upper()
    # 防护：entry_price 或 sl_price 为 0（calc_sl_tp 降级失败场景），
    # sl_dist 会变成 entry_price 本身，dynamic_target 输出荒谬值污染复盘。
    if entry_price <= 0 or sl_price <= 0:
        return {
            "momentum_coeff":      1.0,
            "dynamic_target":      0.0,
            "dim_rsi":             0.0,
            "dim_vol":             0.0,
            "dim_price_momentum":  0.0,
            "dim_funding":         0.0,
            "base_rr":             2.0,
        }
    sl_dist   = abs(entry_price - sl_price) + 1e-9

    d_rsi  = _rsi_dim(rsi_1h, direction)
    d_vol  = _vol_dim(vol_ratio)
    d_mom  = _price_momentum_dim(k1h, direction)
    d_fund = _funding_dim(funding, direction)

    raw_coeff = 1.0 + d_rsi + d_vol + d_mom + d_fund
    coeff     = _clamp(raw_coeff, _COEFF_MIN, _COEFF_MAX)

    # 动态目标 = 入场价 ± sl_dist × BASE_RR × coeff
    if direction == "LONG":
        dynamic_target = entry_price + sl_dist * _BASE_RR * coeff
    else:
        dynamic_target = entry_price - sl_dist * _BASE_RR * coeff

    return {
        "momentum_coeff":      round(coeff, 3),
        "dynamic_target":      round(dynamic_target, 8),
        "dim_rsi":             round(d_rsi, 3),
        "dim_vol":             round(d_vol, 3),
        "dim_price_momentum":  round(d_mom, 3),
        "dim_funding":         round(d_fund, 3),
        "base_rr":             _BASE_RR,
    }


def calc_tp_consistency(
    direction: str,
    dynamic_target: float,
    tp1_price: float,
    atr: float,
) -> dict:
    """
    计算动态目标与 TP1 的一致性得分。

    重合标准（做多方向，做空镜像）：
      两者距离 ≤ 0.5 ATR → consistency_score = "high"   （双重验证，正常开仓）
      两者距离 ≤ 1.0 ATR → consistency_score = "medium" （可接受，记录）
      两者距离 > 1.0 ATR → consistency_score = "low"    （仓位减半信号）

    返回：
      tp_vs_dynamic_dist  : float  TP1 与动态目标的【绝对】距离，始终 >= 0（不含方向）
      tp_vs_dynamic_atr   : float  绝对距离折算成 ATR 倍数，始终 >= 0
      [v3.89 #74] 有方向的偏差另设新字段（原字段与评分计算不变）：
      tp_vs_dynamic_signed_dist : float  正=TP1 比动态目标更近（保守），负=TP1 更远（激进），0=重合
      tp_vs_dynamic_signed_atr  : float  同上，折算成 ATR 倍数
      tp_relative               : str    "conservative" | "aggressive" | "aligned"
      consistency_schema_version: int    字段口径版本（2 = 含有方向偏差字段）
      consistency_score   : str    "high" | "medium" | "low"
      consistency_note    : str    人类可读说明
    """
    dist     = abs(tp1_price - dynamic_target)
    dist_atr = dist / (atr + 1e-9)

    if direction == "LONG":
        # TP1 vs 动态目标的方向：TP1 更近（保守）还是更远（激进）
        relative = "conservative" if tp1_price < dynamic_target else "aggressive"
    else:
        relative = "conservative" if tp1_price > dynamic_target else "aggressive"

    if dist_atr <= 0.5:
        score = "high"
        note  = f"TP1({tp1_price:.6g}) 与动态目标({dynamic_target:.6g}) 高度重合 ({dist_atr:.2f} ATR)，双重验证"
    elif dist_atr <= 1.0:
        score = "medium"
        note  = f"TP1({tp1_price:.6g}) 与动态目标({dynamic_target:.6g}) 基本一致 ({dist_atr:.2f} ATR)，{relative}"
    else:
        score = "low"
        note  = (
            f"TP1({tp1_price:.6g}) 与动态目标({dynamic_target:.6g}) 偏差较大 ({dist_atr:.2f} ATR)，"
            f"{'TP1偏近，力度估算认为行情可以更远' if relative == 'conservative' else 'TP1偏远，力度估算认为动能不足以到达'}"
        )

    # [v3.89 #74] 有方向偏差：LONG 取 目标-TP1，SHORT 取 TP1-目标；正=TP1 更近（保守）
    signed = (dynamic_target - tp1_price) if direction == "LONG" else (tp1_price - dynamic_target)
    signed_atr = signed / (atr + 1e-9)
    tp_rel = "conservative" if signed > 0 else ("aggressive" if signed < 0 else "aligned")

    return {
        "tp_vs_dynamic_dist": round(dist, 8),
        "tp_vs_dynamic_atr":  round(dist_atr, 3),
        "consistency_score":  score,
        "consistency_note":   note,
        "tp_vs_dynamic_signed_dist": round(signed, 8),
        "tp_vs_dynamic_signed_atr":  round(signed_atr, 3),
        "tp_relative":               tp_rel,
        "consistency_schema_version": 2,
    }
