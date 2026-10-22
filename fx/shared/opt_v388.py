"""
shared/opt_v388.py — v3.88 清算区「相对强度」：用 24H 成交额做分母，替代按固定金额衡量清算区。

同样 500 万 U 的清算区，对日成交 30 亿 U 的 BTC 是噪声，对日成交 500 万 U 的山寨币却能推动整个市场。
相对强度 = 清算区金额 / 24H 成交额（USDT 计）。
"""
from __future__ import annotations

import math as _math


def _finite(x) -> bool:
    """[v3.89 #73] 是有限实数（排除 NaN/Inf/bool/非数值）。"""
    return (isinstance(x, (int, float)) and not isinstance(x, bool)
            and _math.isfinite(x))


def liq_relative_strength(liq_amount: float, vol24_usd: float) -> float | None:
    """返回相对强度；金额或成交额缺失 / 非正时返回 None（调用方应回退到原逻辑）。"""
    try:
        a = float(liq_amount or 0)
        v = float(vol24_usd or 0)
    except (TypeError, ValueError):
        return None
    # [v3.89 #73] NaN/Inf 视为缺失（旧版 NaN 会通过 <=0 判断并产出 NaN）；合法输入结果不变
    if not (_finite(a) and _finite(v)) or a <= 0 or v <= 0:
        return None
    return a / v


def liq_weight(rel: float | None, ref_ratio: float = 0.10,
               w_min: float = 0.5, w_max: float = 3.0, base: float = 2.0) -> float:
    """清算区锚点权重 = base × (相对强度 / 参考相对强度)，截断到 [w_min, w_max]；相对强度未知时返回 base（原权重）。"""
    # [v3.89 #73] 相对强度/参考值非有限，或上下界反向时返回 base。
    #   旧版 NaN 经 min/max 会落成 w_max（边界权重）；合法输入结果不变。
    if rel is None or not _finite(rel) or not _finite(ref_ratio) or ref_ratio <= 0:
        return base
    if not (_finite(w_min) and _finite(w_max)) or w_min > w_max:
        return base
    return max(w_min, min(w_max, base * rel / ref_ratio))


def tp1_buffer_atr(rel: float | None, strong: float = 0.20, mid: float = 0.05,
                   buf_strong: float = 0.20, buf_mid: float = 0.35, buf_weak: float = 0.50,
                   legacy: float = 0.30) -> float:
    """TP1 距清算区前沿的缓冲（ATR 倍数）。相对强度越大，价格越可能打到甚至穿过清算区，缓冲越小。未知时返回原值。"""
    # [v3.89 #73] NaN/Inf 按未知处理返回原值（旧版 NaN 会落到最宽缓冲 buf_weak）；合法输入结果不变
    if rel is None or not _finite(rel):
        return legacy
    if rel >= strong:
        return buf_strong
    if rel >= mid:
        return buf_mid
    return buf_weak


def _zsize(z: dict) -> float:
    return float(z.get("liq_usd") or z.get("amount") or 0.0)


def liq_concentration(amount: float, zones: list) -> float | None:
    """[v3.88 OPT-14d] 清算集中度 = 单区金额 / 同方向清算图总量。
    注意：只有一个区时恒为 1.0，且随清算图拉取范围（24h/3d/7d）变化，需结合金额一起看。"""
    try:
        total = sum(_zsize(z) for z in (zones or []))
        a = float(amount or 0)
    except (TypeError, ValueError, AttributeError):
        return None
    if not (_finite(a) and _finite(total)) or a <= 0 or total <= 0:   # [v3.89 #73]
        return None
    return a / total


def oi_relative(amount: float, oi_usd: float) -> float | None:
    """[v3.88 OPT-14d] OI 相对强度 = 单区金额 / 持仓名义价值（USDT）。
    注意：币安 /fapi/v1/openInterest 返回的是合约张数（币本位数量），调用方必须先乘以标记价换算成 USDT。"""
    return liq_relative_strength(amount, oi_usd)


def relative_by_denom(denom: str, amount: float, zones: list,
                      vol24_usd: float, oi_usd: float) -> float | None:
    """按配置的分母计算相对强度：vol24（24H 成交额）/ oi（持仓名义价值）/ concentration（清算图集中度）。"""
    if denom == "oi":
        return oi_relative(amount, oi_usd)
    if denom == "concentration":
        return liq_concentration(amount, zones)
    return liq_relative_strength(amount, vol24_usd)


def liq_metrics(zone: dict | None, zones: list, vol24_usd: float, oi_usd: float) -> dict:
    """[v3.88 OPT-14d] 只记录、不参与决策：同一清算区在三种分母下的相对强度，供积累数据后定阈值。
    三个分母各自独立计算，任一缺数据只让对应字段为 None，不影响其余两个。"""
    if not zone:
        return {}
    a = _zsize(zone)
    def _r(x):
        return round(x, 6) if x is not None else None
    return {
        "amount":        round(a, 2),
        "rel_vol24":     _r(liq_relative_strength(a, vol24_usd)),
        "rel_oi":        _r(oi_relative(a, oi_usd)),
        "concentration": _r(liq_concentration(a, zones)),
        "zone_count":    len(zones or []),
        # [v3.88 OPT-14d] 单区时 concentration 恒为 1.0，没有区分度；显式标记，分析时应剔除这类样本
        "concentration_valid": len(zones or []) >= 2,
    }


def liq_weight_gated(rel, ref_ratio, w_min, w_max, liq_edge, ema20, atr,
                     gate_atr=1.5, gate_cap=2.0, degraded=False, base=2.0):
    """[v3.88 OPT-14f/g] 在相对强度权重之上叠加两道门控：
    - 数据降级（清算图来自二级/三级数据源）：无论强度多少，权重强制回到 base（固定 2）；
    - 分歧度门控：清算区锚点与 EMA20 偏离超过 gate_atr × ATR 时，权重上限压制到 gate_cap。
    只会让权重更保守（不高于未门控的结果）；atr/ema20 缺失时不触发分歧度门控。"""
    if degraded:
        return base
    cap = w_max
    try:
        if atr and atr > 0 and ema20 and liq_edge and abs(float(liq_edge) - float(ema20)) > gate_atr * float(atr):
            cap = min(w_max, gate_cap)
    except (TypeError, ValueError):
        pass
    return liq_weight(rel, ref_ratio, w_min, cap, base)
