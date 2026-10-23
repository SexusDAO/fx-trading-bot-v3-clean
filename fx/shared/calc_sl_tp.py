"""
shared/calc_sl_tp.py — 入场参数计算（SL / TP 三层优先级体系）

外部唯一入口：calc_sl_tp(signal, mark_price, atr, liq_zones, k1h, ema20)

返回：entry_price, sl_price, tp1_price, tp2_price, sl_degraded, tp_degraded, entry_mode

──────────────────────────────────────────────────────────────────────────────
止损（SL）优先级（由高到低）：
  1. 清算图结构位（liq_long/liq_short）—— 最可靠，贴清算区外侧 ± buffer
  2. Volume Profile val/vah（v3.62）   —— 清算图无数据时的备用结构位
  3. ATR 倍数降级（sl_degraded=True）  —— 两者都无数据时的兜底
  * stop_hunt 专属：使用 sweep_extreme 外侧 0.3%
  * funding_arb 专属：entry ± 1.5 ATR，无清算图定位

止盈（TP1）优先级（由高到低）：
  1. 清算图结构位（liq_long/liq_short）—— 最可靠
  2. Volume Profile vah/val（v3.62）   —— 清算图无锚点时的备用（tp_degraded→False）
  3. 订单簿密集挂单区                  —— 进一步备用
  4. ATR × 2.0 倍（tp_degraded=True） —— 兜底
  * stop_hunt 专属：TP1 = ref_level（前高/前低）
  * range_bounce 专属：TP1 = range_ceiling × 0.98

sl_degraded=True 含义：止损不在结构位，executor 会自动降仓 50%（v3.59）
tp_degraded=True 含义：TP1 仅为 ATR 估算，无清算图/VP/订单簿结构支撑

入场价计算场景：
  pullback  — 三点加权均价 → EMA/清算区微调
  breakout  — 贴近现价 + 压力检查
  reversal  — 清算区挂单或普通入场
  stop_hunt — 当前 mark_price（二探确认时的实时价）
  ema_trend — 当前 mark_price（趋势回踩立即入场）
  oi_div    — 当前 mark_price
  others    — fallback 用 mark_price
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import logging
import random
from typing import Any
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from config import SL_MULT_LOW as _CFG_LOW, SL_MULT_MID as _CFG_MID, SL_MULT_HIGH as _CFG_HIGH, SL_MAX_PCT as _CFG_SL_MAX, TP1_MAX_PCT as _CFG_TP1_MAX
    from config import SL_MAX_PCT_LOW as _CFG_SL_LOW, SL_MAX_PCT_MID as _CFG_SL_MID, SL_MAX_PCT_HIGH as _CFG_SL_HIGH
    from config import TP1_MAX_PCT_HIGH as _CFG_TP1_HIGH
    from config import RR_MIN_TP1 as _CFG_RR_MIN_TP1, RR_MIN_TP1_HIGH as _CFG_RR_MIN_TP1_HIGH
except Exception as _cfg_e:
    _CFG_LOW, _CFG_MID, _CFG_HIGH = 1.5, 1.5, 2.0   # [v3.89] low 档 1.2→1.5，对齐 config.SL_MULT_LOW
    _CFG_SL_LOW, _CFG_SL_MID, _CFG_SL_HIGH = 0.12, 0.15, 0.20
    _CFG_TP1_HIGH = 0.25
    _CFG_RR_MIN_TP1 = 2.0   # [v3.89] 1.5→2.0，对齐 config.RR_MIN_TP1
    # [v3.89] 回退路径漏定义的三个名字（下方 _SL_MAX_PCT / _TP1_MAX_PCT / RR 高层级处会直接引用，导致导入时 NameError）。
    #   取值与 config.py 中同名项当前值一致；上面已有的回退值未改动。
    _CFG_SL_MAX = 0.12
    _CFG_TP1_MAX = 0.15
    _CFG_RR_MIN_TP1_HIGH = 1.5
    _cfg_fb_exc = _cfg_e   # logger 在下面才创建，延后输出

logger = logging.getLogger("fx.calc_sl_tp")


def _cfg_fallback(group: str, exc: BaseException, effective: dict) -> None:
    """
    [v3.89 #31] 配置导入回退的可见性：ImportError（可选项缺失）→ info；其他异常（config 模块损坏）→ error，
    并输出实际生效的内置默认值。只增加日志，不改变任何取值。
    """
    if isinstance(exc, ImportError):
        logger.info(f"[calc_sl_tp] 可选配置 {group} 未提供（{exc}），使用内置默认值: {effective}")
    else:
        logger.error(f"[calc_sl_tp] 配置模块加载异常 {group}（{type(exc).__name__}: {exc}），使用内置默认值: {effective}")


if "_cfg_fb_exc" in globals():
    _cfg_fallback("SL/TP/RR", _cfg_fb_exc, {
        "sl_mult": [_CFG_LOW, _CFG_MID, _CFG_HIGH], "sl_max_pct": [_CFG_SL_LOW, _CFG_SL_MID, _CFG_SL_HIGH],
        "tp1_max_pct_high": _CFG_TP1_HIGH, "rr_min_tp1": _CFG_RR_MIN_TP1})

# 动态 sl_mult 按波动层级（从 config.py 加载，与主配置同步）
_SL_MULT = {"low": _CFG_LOW, "mid": _CFG_MID, "high": _CFG_HIGH}
_SL_MAX_PCT  = _CFG_SL_MAX   # 兼容旧代码，使用动态表替代
_SL_MAX_MAP  = {"low": _CFG_SL_LOW, "mid": _CFG_SL_MID, "high": _CFG_SL_HIGH}
_TP1_MAX_PCT = _CFG_TP1_MAX  # low/mid tier TP1 距入场价上限
_TP1_MAX_PCT_HIGH = _CFG_TP1_HIGH  # high tier TP1 距入场价上限
_TP1_MIN_RR = _CFG_RR_MIN_TP1   # v3.14: 从 config.RR_MIN_TP1 加载，与配置同步（原硬编码 1.5）
try:
    from config import RR_MIN_TP2 as _TP2_MIN_RR   # [fix #41] 统一从 config 读取，删除硬编码 2.5
except Exception:
    _TP2_MIN_RR = 3.0                                # 与 config.RR_MIN_TP2 默认值一致


# ─────────────────────────────────────────────────────────────────────────────
# 公共工具
# ─────────────────────────────────────────────────────────────────────────────
def _vol_mid(k1h: list) -> float:
    """近20根1H K线里成交量最大的前5根K线价格中点（成交密集区近似）"""
    if len(k1h) < 5:
        closes = [float(k[4]) for k in k1h]
        return sum(closes) / len(closes) if closes else 0.0
    top5 = sorted(k1h[-20:], key=lambda k: float(k[5]), reverse=True)[:5]
    return sum((float(k[2]) + float(k[3])) / 2 for k in top5) / 5


def _nearest_liq_below(price: float, atr: float,
                        liq_long: list,
                        dist_min: float = 0.5, dist_max: float = 2.0) -> dict | None:
    """做多方向：找入场价下方 [dist_min*ATR, dist_max*ATR] 内最近的清算区"""
    candidates = []
    for z in liq_long:
        mid  = (z["price_low"] + z["price_high"]) / 2
        dist = price - mid
        atr_d = dist / (atr + 1e-9)
        if dist_min <= atr_d <= dist_max:
            candidates.append((atr_d, z))
    return candidates[0][1] if candidates else None


def _zone_size(z: dict) -> float:
    """兼容 liq_usd（liq_orders 路径）和 amount（BYK/CG heatmap 路径）"""
    return z.get("liq_usd") or z.get("amount") or 0.0


def _nearest_liq_above(price: float, atr: float,
                        liq_short: list,
                        dist_min: float = 0.5, dist_max: float = 2.0) -> dict | None:
    """做空方向：找入场价上方清算区"""
    candidates = []
    for z in liq_short:
        mid  = (z["price_low"] + z["price_high"]) / 2
        dist = mid - price
        atr_d = dist / (atr + 1e-9)
        if dist_min <= atr_d <= dist_max:
            candidates.append((atr_d, z))
    return candidates[0][1] if candidates else None

def _ob_density_entry(mark: float, atr: float, direction: str,
                       symbol: str = "") -> float | None:
    """
    挂单簿密度入场价（liq_cache 为空时的 fallback）。
    找现价附近 [0.1×ATR, 2×ATR] 范围内挂单最密集的价位。
    做多：找最密集买单（支撑位），在其稍上方入场
    做空：找最密集卖单（压力位），在其稍下方入场
    失败返回 None，由调用方降级到 ATR 公式。
    """
    try:
        from shared.binance_api import get_order_book
        ob = get_order_book(symbol or "", limit=20) if symbol else {}
        if not ob:
            return None
        if direction == "LONG":
            orders = [(float(p), float(q) * float(p)) for p, q in ob.get("bids", [])]
            candidates = [(p, usdt) for p, usdt in orders
                          if atr * 0.1 <= (mark - p) <= atr * 2.0]
            if not candidates:
                return None
            best_price, _ = max(candidates, key=lambda x: x[1])
            return round(best_price + atr * 0.05, 8)  # 密集支撑位稍上方
        else:
            orders = [(float(p), float(q) * float(p)) for p, q in ob.get("asks", [])]
            candidates = [(p, usdt) for p, usdt in orders
                          if atr * 0.1 <= (p - mark) <= atr * 2.0]
            if not candidates:
                return None
            best_price, _ = max(candidates, key=lambda x: x[1])
            return round(best_price - atr * 0.05, 8)  # 密集压力位稍下方
    except Exception:
        return None


def _farthest_liq_below(price: float, liq_long: list) -> dict | None:
    """做多方向：找入场价下方所有清算区里最远的一个（用于止损）"""
    below = [z for z in liq_long if (z["price_high"] + z["price_low"]) / 2 < price]
    if not below:
        return None
    return min(below, key=lambda z: (z["price_low"] + z["price_high"]) / 2)


def _farthest_liq_above(price: float, liq_short: list) -> dict | None:
    """做空方向：找入场价上方所有清算区里最远的一个（用于止损）"""
    above = [z for z in liq_short if (z["price_high"] + z["price_low"]) / 2 > price]
    if not above:
        return None
    return max(above, key=lambda z: (z["price_low"] + z["price_high"]) / 2)


def _densest_liq_for_tp(price: float, direction: str,
                         liq_long: list, liq_short: list) -> dict | None:
    """找密度最大的清算区（用于 TP2）"""
    if direction == "LONG":
        above = [z for z in liq_long if (z["price_low"] + z["price_high"]) / 2 > price]
        return max(above, key=_zone_size) if above else None
    else:
        below = [z for z in liq_short if (z["price_low"] + z["price_high"]) / 2 < price]
        return max(below, key=_zone_size) if below else None


def _tighten_sl_after_cap(entry: float, sl: float, tp1: float, direction: str,
                          rr_min: float, tier: str) -> tuple:
    """
    [v3.87 STEP4-2] TP1 被 cap 截断后 RR 不足时，尝试收紧止损使 RR 达标。
    原逻辑最小距离写死 2.5%，与按波动层的最小止损距离（低 2.5% / 中 3% / 高 5%）不一致，
    中高波动币的止损可能被收紧到噪声区间内。现按波动层取最小距离，达不到返回 (None, 收紧比例)，调用方应拒绝信号。
    收紧后的止损还必须不比原止损更宽。
    返回 (新止损或 None, 收紧后的止损距离比例)
    """
    tp1_dist = abs(tp1 - entry)
    needed = tp1_dist / rr_min
    sl_tight = entry - needed if direction == "LONG" else entry + needed
    pct = abs(entry - sl_tight) / (entry + 1e-9)
    min_pct = {"low": 0.025, "mid": 0.03, "high": 0.05}.get(tier, 0.025)
    not_wider = (sl_tight >= sl) if direction == "LONG" else (sl_tight <= sl)
    if pct >= min_pct and not_wider:
        return sl_tight, pct
    return None, pct


def _check_sl_distance(entry: float, sl: float, direction: str, tier: str = "mid") -> bool:
    """止损距离上限按波动层级动态调整: low=12% mid=15% high=20%"""
    if direction == "LONG":
        pct = (entry - sl) / (entry + 1e-9)
    else:
        pct = (sl - entry) / (entry + 1e-9)
    max_pct = _SL_MAX_MAP.get(tier, _SL_MAX_PCT)
    return pct <= max_pct


# ─────────────────────────────────────────────────────────────────────────────
# 入场价计算
# ─────────────────────────────────────────────────────────────────────────────
def _collect_liq_metrics(entry: float, atr: float, direction: str,
                         liq_long: list, liq_short: list, ctx: dict) -> dict:
    """[v3.88 OPT-14d] 记录用：入场侧（止损方向）最近清算区、TP1 侧最近清算区的相对强度。失败返回 {}。"""
    try:
        from shared.opt_v388 import liq_metrics
        if direction == "LONG":
            ez = _nearest_liq_below(entry, atr, liq_long)
            tps = sorted([z for z in liq_short if (z["price_low"] + z["price_high"]) / 2 > entry],
                         key=lambda z: z["price_low"])
            e_zones, t_zones = liq_long, liq_short
        else:
            ez = _nearest_liq_above(entry, atr, liq_short)
            tps = sorted([z for z in liq_long if (z["price_low"] + z["price_high"]) / 2 < entry],
                         key=lambda z: z["price_high"], reverse=True)
            e_zones, t_zones = liq_short, liq_long
        v, oi = ctx.get("vol24_usd", 0.0), ctx.get("oi_usd", 0.0)
        return {
            "entry_side": liq_metrics(ez, e_zones, v, oi),
            "tp_side":    liq_metrics(tps[0] if tps else None, t_zones, v, oi),
            "vol24_usd":  round(v, 0),
            "oi_usd":     round(oi, 0),
        }
    except Exception:
        return {}


def _oid_reject_all_one_side() -> bool:
    """[v3.88 OPT-15] OPT_OID_REJECT_ALL_BULL：oi_divergence 近5根全同向时是否拒绝（默认 True）。读取失败按 True。"""
    try:
        import config as _c
        return bool(getattr(_c, "OPT_OID_REJECT_ALL_BULL", True))
    except Exception as _cfg_e:
        _cfg_fallback("OPT_OID_REJECT_ALL_BULL", _cfg_e, {"enabled": True})
        return True


def _liq_rel_cfg():
    """[v3.88 OPT-14] 读取清算区相对强度相关配置；开关关闭或读取失败时返回 None（走原逻辑）。"""
    try:
        import config as _c
        if not getattr(_c, "OPT_LIQ_RELATIVE_STRENGTH", False):
            return None
        return {
            "strong": _c.LIQ_RELATIVE_STRONG, "mid": _c.LIQ_RELATIVE_MID,
            "ref": _c.LIQ_WEIGHT_REF_RATIO, "w_min": _c.LIQ_WEIGHT_MIN, "w_max": _c.LIQ_WEIGHT_MAX,
            "gate_atr": getattr(_c, "OPT_DIVERGENCE_GATE_ATR", 1.5),
            "gate_cap": getattr(_c, "OPT_DIVERGENCE_GATE_WEIGHT_CAP", 2.0),
            "b_strong": _c.TP1_LIQ_BUFFER_STRONG, "b_mid": _c.TP1_LIQ_BUFFER_MID, "b_weak": _c.TP1_LIQ_BUFFER_WEAK,
        }
    except Exception as _cfg_e:
        _cfg_fallback("OPT_LIQ_RELATIVE_STRENGTH", _cfg_e, {"source": "existing_logic"})
        return None


def _liq_rel(z: dict, zones: list, ctx: dict | None) -> float | None:
    """[v3.88 OPT-14d] 按 config.LIQ_REL_DENOM 选择分母计算相对强度；数据缺失返回 None（调用方回退原逻辑）。"""
    from shared.opt_v388 import relative_by_denom
    try:
        import config as _c
        denom = getattr(_c, "LIQ_REL_DENOM", "vol24")
    except Exception as _cfg_e:
        denom = "vol24"
        _cfg_fallback("LIQ_REL_DENOM", _cfg_e, {"denom": denom})
    ctx = ctx or {}
    return relative_by_denom(denom, _zone_size(z), zones,
                             ctx.get("vol24_usd", 0.0), ctx.get("oi_usd", 0.0))


def _entry_pullback(mark: float, atr: float, ema20: float,
                     liq_long: list, liq_short: list,
                     direction: str, k1h: list, vol24_usd: float = 0.0,
                     liq_ctx: dict | None = None) -> tuple[float, str]:
    """结构回踩入场价 — 三点加权均价
    v3.14: 加清算区距离上限保护——清算区上/下沿距 mark 超过 MAX_PULLBACK_LIQ_DIST_ATR 时降级
    """
    _MAX_PULLBACK_LIQ_DIST_ATR = 3.0  # 清算区距 mark 超过 3 ATR 则不用来计算 pullback entry
    # [v3.84 优化 OPT-3] 降级入场的 ATR 参数多空对称化（原：多 0.3/0.15/1.5，空 0.8/0.4/2.5）。
    #   多头回踩仅 0.3ATR 落在噪声内、易在趋势末端被动成交；空头要等 0.8ATR、成交机会被压缩。
    #   参数与依据见 shared/opt_v384.pullback_entry_params；关闭 OPT_PULLBACK_ENTRY_SYMMETRIC 即回到原值。
    try:
        from config import (OPT_PULLBACK_ENTRY_SYMMETRIC as _OPT_PB, PULLBACK_DEGRADED_ATR as _PB_DEG,
                            PULLBACK_MIN_DIST_ATR as _PB_MIN, PULLBACK_MAX_DIST_ATR as _PB_MAX)
    except Exception as _cfg_e:
        _OPT_PB, _PB_DEG, _PB_MIN, _PB_MAX = False, 0.5, 0.3, 2.0
        _cfg_fallback("PULLBACK_ENTRY", _cfg_e, {"symmetric": _OPT_PB, "degraded_atr": _PB_DEG,
                                                  "min_dist_atr": _PB_MIN, "max_dist_atr": _PB_MAX})
    from shared.opt_v384 import pullback_entry_params as _pb_params
    _pp = _pb_params(_OPT_PB, _PB_DEG, _PB_MIN, _PB_MAX)

    # [v3.87 OPT-8] 在确认处入场。回踩判定已要求「价格位于回踩区 + 15M/1H 止跌(止涨)信号」，
    #   原逻辑却在确认之后再把限价挂到更深的位置（清算区加权或 mark∓0.3~0.8ATR）：
    #   只有价格继续朝不利方向走才会成交，而这恰恰说明刚确认的止跌信号失效了——成交的天然偏向坏单（逆向选择）。
    #   新：限价挂在 mark∓0.1ATR（只留一点 maker 空间），确认在哪里就在哪里入场。开关 OPT_PULLBACK_ENTRY_AT_CONFIRM。
    try:
        from config import OPT_PULLBACK_ENTRY_AT_CONFIRM as _OPT_PBC
    except Exception as _cfg_e:
        _OPT_PBC = False
        _cfg_fallback("OPT_PULLBACK_ENTRY_AT_CONFIRM", _cfg_e, {"enabled": _OPT_PBC})
    if _OPT_PBC and atr > 0:
        if direction == "LONG":
            return mark - atr * 0.1, "at_confirm"
        return mark + atr * 0.1, "at_confirm"
    vol_mid = _vol_mid(k1h)

    if direction == "LONG":
        z = _nearest_liq_below(mark, atr, liq_long)
        liq_edge = None
        if z:
            mid_dist = (mark - (z["price_low"] + z["price_high"]) / 2) / (atr + 1e-9)
            if mid_dist > _MAX_PULLBACK_LIQ_DIST_ATR:
                # 清算区离现价太远，不合适作 pullback 入场参考
                logger.debug(
                    f"[entry_pullback] LONG 清算区过远 ({mid_dist:.1f} ATR)，降级入场"
                )
                z = None
        if z:
            liq_edge = z["price_high"]           # 清算区上沿（权重2）
            # [v3.88 OPT-14] 清算区锚点权重按「相对强度」（清算区金额 / 24H 成交额）动态化：
            #   原权重固定为 2，不区分清算区对该币的实际影响力。开启后权重 = 2 × 相对强度 / 10%，截断到 [0.5, 3.0]；
            #   成交额或清算金额缺失时权重回到 2，与原公式完全一致。开关 OPT_LIQ_RELATIVE_STRENGTH。
            _w = 2.0
            _rcfg = _liq_rel_cfg()
            if _rcfg:
                from shared.opt_v388 import liq_weight_gated as _lwg
                _ctx_e = liq_ctx or {"vol24_usd": vol24_usd}
                # [v3.88 OPT-14f/g] 叠加两道门控：数据降级（二/三级来源）→ 权重回到 2；
                #   清算区锚点与 EMA20 偏离 > OPT_DIVERGENCE_GATE_ATR×ATR → 权重上限压到 OPT_DIVERGENCE_GATE_WEIGHT_CAP。
                _w = _lwg(_liq_rel(z, liq_long if direction == "LONG" else liq_short, _ctx_e),
                          _rcfg["ref"], _rcfg["w_min"], _rcfg["w_max"], liq_edge, ema20, atr,
                          _rcfg["gate_atr"], _rcfg["gate_cap"], bool(_ctx_e.get("liq_degraded")))
            weighted = (liq_edge * _w + ema20 * 1 + vol_mid * 1) / (_w + 2)
            entry = weighted
        else:
            entry = mark - atr * _pp["long_deg"]   # 无清算区降级（OPT-3 参数化）

        # 合理性检查1：entry 不应高于 mark 或低于 mark-1.5ATR
        if entry >= mark or (mark - entry) > atr * _pp["long_max"]:
            logger.warning(
                f"[entry_pullback] LONG 降级ATR入场: "
                f"entry={entry:.6g} mark={mark:.6g} atr={atr:.6g} "
                f"liq_edge={liq_edge} ema20={ema20:.6g} vol_mid={vol_mid:.6g}"
            )
            entry = mark - atr * _pp["long_deg"]
        # 合理性检查2：最小距离保护（至少 0.15ATR）
        if (mark - entry) < atr * _pp["long_min"]:
            entry = mark - atr * _pp["long_min"]
        # 入场价随机化（反猎杀），LONG 方向在精度截断后偏移 ±ORDER_ENTRY_RANDOM_ATR × atr
        try:
            from config import ORDER_ENTRY_RANDOMIZE, ORDER_ENTRY_RANDOM_ATR
            if ORDER_ENTRY_RANDOMIZE and atr > 0:
                _offset = random.uniform(-ORDER_ENTRY_RANDOM_ATR, ORDER_ENTRY_RANDOM_ATR) * atr
                _raw = entry
                entry = entry + _offset
                entry = max(entry, _raw - 0.3 * atr)
                entry = min(entry, _raw + 0.3 * atr)
        except Exception:
            pass
        return entry, "weighted" if z else "degraded"
    else:
        z = _nearest_liq_above(mark, atr, liq_short)
        liq_edge = None
        if z:
            mid_dist = ((z["price_low"] + z["price_high"]) / 2 - mark) / (atr + 1e-9)
            if mid_dist > _MAX_PULLBACK_LIQ_DIST_ATR:
                logger.debug(
                    f"[entry_pullback] SHORT 清算区过远 ({mid_dist:.1f} ATR)，降级入场"
                )
                z = None
        if z:
            liq_edge = z["price_low"]
            # [v3.88 OPT-14] 清算区锚点权重按「相对强度」（清算区金额 / 24H 成交额）动态化：
            #   原权重固定为 2，不区分清算区对该币的实际影响力。开启后权重 = 2 × 相对强度 / 10%，截断到 [0.5, 3.0]；
            #   成交额或清算金额缺失时权重回到 2，与原公式完全一致。开关 OPT_LIQ_RELATIVE_STRENGTH。
            _w = 2.0
            _rcfg = _liq_rel_cfg()
            if _rcfg:
                from shared.opt_v388 import liq_weight_gated as _lwg
                _ctx_e = liq_ctx or {"vol24_usd": vol24_usd}
                # [v3.88 OPT-14f/g] 叠加两道门控：数据降级（二/三级来源）→ 权重回到 2；
                #   清算区锚点与 EMA20 偏离 > OPT_DIVERGENCE_GATE_ATR×ATR → 权重上限压到 OPT_DIVERGENCE_GATE_WEIGHT_CAP。
                _w = _lwg(_liq_rel(z, liq_long if direction == "LONG" else liq_short, _ctx_e),
                          _rcfg["ref"], _rcfg["w_min"], _rcfg["w_max"], liq_edge, ema20, atr,
                          _rcfg["gate_atr"], _rcfg["gate_cap"], bool(_ctx_e.get("liq_degraded")))
            weighted = (liq_edge * _w + ema20 * 1 + vol_mid * 1) / (_w + 2)
            entry = weighted
        else:
            entry = mark + atr * _pp["short_deg"]  # v3.20: 0.3→0.8 ATR（v3.84 OPT-3 起由开关参数化）

        if entry <= mark or (entry - mark) > atr * _pp["short_max"]:  # v3.20: 上限 1.5→2.5，匹配新降级弹幅
            logger.warning(
                f"[entry_pullback] SHORT 降级ATR入场: "
                f"entry={entry:.6g} mark={mark:.6g} atr={atr:.6g} "
                f"liq_edge={liq_edge} ema20={ema20:.6g} vol_mid={vol_mid:.6g}"
            )
            entry = mark + atr * _pp["short_deg"]  # v3.20: 共两处保持一致
        # 最小距离保护（至少 0.4 ATR，v3.20: 0.15→0.4，与新降级弹幅起点匹配）
        if (entry - mark) < atr * _pp["short_min"]:
            entry = mark + atr * _pp["short_deg"]  # 小于最小距离时，直接跳到降级弹幅
        # 入场价随机化（反猎杀），SHORT 方向偏移
        try:
            from config import ORDER_ENTRY_RANDOMIZE, ORDER_ENTRY_RANDOM_ATR
            if ORDER_ENTRY_RANDOMIZE and atr > 0:
                _offset = random.uniform(-ORDER_ENTRY_RANDOM_ATR, ORDER_ENTRY_RANDOM_ATR) * atr
                _raw = entry
                entry = entry + _offset
                entry = max(entry, _raw - 0.3 * atr)
                entry = min(entry, _raw + 0.3 * atr)
        except Exception:
            pass
        return entry, "weighted" if z else "degraded"


def _entry_breakout(mark: float, atr: float,
                     liq_long: list, liq_short: list,
                     direction: str) -> tuple[float, str]:
    """动量突破入场价 — 贴近现价立刻追入
    v3.6: 去掉清算压力调整：上方有压力时原代码把入场价降到 mark-0.1ATR，
    这与追突破逻辑相反（变成等回踩），清算压力信息已在 judge_breakout Step9 影响
    signal_strength，不需要在入场价里重复处理。
    """
    if direction == "LONG":
        entry = mark + atr * 0.05
        # LONG breakout 随机化：只往上偏（不能偏到突破位以下）
        try:
            from config import ORDER_ENTRY_RANDOMIZE, ORDER_ENTRY_RANDOM_ATR
            if ORDER_ENTRY_RANDOMIZE and atr > 0:
                entry = entry + random.uniform(-ORDER_ENTRY_RANDOM_ATR, ORDER_ENTRY_RANDOM_ATR) * atr   # 双向偏移
        except Exception:
            pass
    else:
        entry = mark - atr * 0.05
        # SHORT breakout 随机化：只往下偏
        try:
            from config import ORDER_ENTRY_RANDOMIZE, ORDER_ENTRY_RANDOM_ATR
            if ORDER_ENTRY_RANDOMIZE and atr > 0:
                entry = entry - random.uniform(-ORDER_ENTRY_RANDOM_ATR, ORDER_ENTRY_RANDOM_ATR) * atr   # 双向偏移
        except Exception:
            pass
    return entry, "immediate"


# reversal 最小入场距离：确保有足够的「冲高/杀跌确认」空间再成交
_REVERSAL_MIN_DIST_ATR = 0.4   # entry 至少离 mark 0.4 ATR
_REVERSAL_FALLBACK_ATR = 1.2   # 无清算区/挂单簿时的兜底距离 mark ± 1.2 ATR
_REVERSAL_MAX_DIST_ATR = 1.5   # entry 最多离 mark 1.5 ATR（immediate / pending_order 共用）


def _entry_reversal(mark: float, atr: float,
                     liq_long: list, liq_short: list,
                     direction: str, entry_mode: str) -> tuple[float, str, dict]:
    """极端反转入场价

    SHORT: 等价格冲高到清算区附近再空，入场价 = liq_short.price_low - 0.1*ATR
    LONG:  等价格杀低到清算区附近再多，入场价 = liq_long.price_high + 0.1*ATR

    最小距离保护：entry 与 mark 距离 < 0.4 ATR 时降级为 mark ± 1.2 ATR，
    防止清算区紧贴现价导致几乎立即成交，失去 reversal 的确认意义。

    返回: (entry_price, entry_mode, degrade_info)
      degrade_info 包含复盘字段：liq_entry_raw / liq_entry_dist_atr / entry_degraded_reason
    """
    degrade_info = {}  # 复盘字段：记录清算区降级信息
    if direction == "LONG":
        z = _nearest_liq_below(mark, atr, liq_long, dist_min=0.3, dist_max=4.0)
        if z:
            if entry_mode == "pending_order":
                entry = (z["price_low"] + z["price_high"]) / 2
            else:
                entry = z["price_high"] + atr * 0.1
            # 最小距离保护：清算区太靠近现价 → 降级
            if (mark - entry) < atr * _REVERSAL_MIN_DIST_ATR:
                logger.warning(
                    f"[entry_reversal] LONG 清算区过近 mark={mark:.6g} entry={entry:.6g} "
                    f"dist={(mark-entry):.4g} < {_REVERSAL_MIN_DIST_ATR}*ATR={atr*_REVERSAL_MIN_DIST_ATR:.4g}，"
                    f"降级为 mark-{_REVERSAL_MIN_DIST_ATR}*ATR"
                )
                entry = mark - atr * _REVERSAL_MIN_DIST_ATR
                z = None
            # ── v3.66 immediate 模式距离保护 ──────────────────────────────────────────
            # 修复：entry 距 mark 超过 1.5×ATR 时强制降级到 mark ± 0.5×ATR
            # 原逻辑（旧 pending_order）：等价格冲到上方清算区中点才成交
            # → 优点：如果庄家确实拉盘冲清算区，入场位置更精准（在清算瀑布顶部做空）
            # → 缺点：成交率极低，信号有效期内往往不成交，挂单过期撤销
            # 新逻辑（immediate 降级）：超距时改为 mark ± 0.5×ATR 附近成交
            # → 优点：成交率大幅提升
            # → 缺点：如果庄家确实冲了清算区，我们的入场比最优位差了 1~3%
            #
            # ⚠️ 复盘验证点（待数据积累后评估）：
            # 观察维度：liq_entry_dist_atr（原始清算区距离，单位 ATR）
            # 如果复盘发现「liq_entry_dist_atr > 1.5 的信号，价格最终有 X% 概率
            # 真的触达了原始清算区位置」，说明庄家大概率冲清算区，
            # 应考虑恢复 pending_order 模式或提高距离容忍阈值（如从 1.5→2.0）
            # 建议样本量 ≥ 30 笔再做结论
            elif (mark - entry) > atr * _REVERSAL_MAX_DIST_ATR:
                liq_entry_raw = entry
                liq_entry_dist_atr = round((mark - entry) / (atr + 1e-9), 3)
                logger.warning(
                    f"[entry_reversal] LONG {entry_mode} 挂单价过远: "
                    f"entry={entry:.6g} mark={mark:.6g} "
                    f"dist={(mark-entry):.4g} = {liq_entry_dist_atr}×ATR > {_REVERSAL_MAX_DIST_ATR}×ATR，"
                    f"降级为 mark-{_REVERSAL_MAX_DIST_ATR}×ATR"
                )
                entry = mark - atr * _REVERSAL_MAX_DIST_ATR
                if entry_mode != "pending_order":
                    z = None   # pending_order 只收紧距离，保留挂单模式（有效期等下游逻辑不变）
                degrade_info = {
                    "liq_entry_raw": round(liq_entry_raw, 8),
                    "liq_entry_dist_atr": liq_entry_dist_atr,
                    "entry_degraded_reason": "dist_too_far",
                }
        else:
            # liq_cache 为空时，从挂单簿找最密集买单支撑位（对新涨/小市值标的更有效）
            entry = _ob_density_entry(mark, atr, "LONG") or (mark - atr * _REVERSAL_FALLBACK_ATR)
    else:
        z = _nearest_liq_above(mark, atr, liq_short, dist_min=0.3, dist_max=4.0)
        if z:
            if entry_mode == "pending_order":
                entry = (z["price_low"] + z["price_high"]) / 2
            else:
                entry = z["price_low"] - atr * 0.1
            # 最小距离保护：清算区太靠近现价 → 降级
            if (entry - mark) < atr * _REVERSAL_MIN_DIST_ATR:
                logger.warning(
                    f"[entry_reversal] SHORT 清算区过近 mark={mark:.6g} entry={entry:.6g} "
                    f"dist={(entry-mark):.4g} < {_REVERSAL_MIN_DIST_ATR}*ATR={atr*_REVERSAL_MIN_DIST_ATR:.4g}，"
                    f"降级为 mark+{_REVERSAL_MIN_DIST_ATR}*ATR"
                )
                entry = mark + atr * _REVERSAL_MIN_DIST_ATR
                z = None
            # ── v3.66 immediate 模式距离保护（SHORT 方向）──────────────────────────────
            # 场景：pre_entry_price 由 scanner 在价格较高时预算，随后价格回落，
            # 执行时 mark 已大幅低于预算值，清算区仍在旧高位，
            # 导致 immediate 模式挂出距现价 3%+ 的 GTC 挂单，违背 immediate 设计意图。
            # 复盘字段同 LONG 方向，用于后续评估是否恢复 pending_order 模式。
            elif (entry - mark) > atr * _REVERSAL_MAX_DIST_ATR:
                liq_entry_raw = entry
                liq_entry_dist_atr = round((entry - mark) / (atr + 1e-9), 3)
                logger.warning(
                    f"[entry_reversal] SHORT {entry_mode} 挂单价过远: "
                    f"entry={entry:.6g} mark={mark:.6g} "
                    f"dist={(entry-mark):.4g} = {liq_entry_dist_atr}×ATR > {_REVERSAL_MAX_DIST_ATR}×ATR，"
                    f"降级为 mark+{_REVERSAL_MAX_DIST_ATR}×ATR"
                )
                entry = mark + atr * _REVERSAL_MAX_DIST_ATR
                if entry_mode != "pending_order":
                    z = None   # pending_order 只收紧距离，保留挂单模式（有效期等下游逻辑不变）
                degrade_info = {
                    "liq_entry_raw": round(liq_entry_raw, 8),
                    "liq_entry_dist_atr": liq_entry_dist_atr,
                    "entry_degraded_reason": "dist_too_far",
                }
        else:
            # liq_cache 为空时，从挂单簿找最密集卖单压力位
            entry = _ob_density_entry(mark, atr, "SHORT") or (mark + atr * _REVERSAL_FALLBACK_ATR)
    return entry, entry_mode if z else "immediate", degrade_info


# ─────────────────────────────────────────────────────────────────────────────
# 止损计算
# ─────────────────────────────────────────────────────────────────────────────
# SL 紧贴清算区外侧的缓冲（ATR 倍数，不需要整个 sl_mult）
_SL_LIQ_BUFFER_ATR = 0.2   # 清算区外侧缓冲，比 sl_mult 小得多


def _swing_point_sl(k1h: list, direction: str,
                    entry: float, atr: float) -> float | None:
    """
    v3.27: 结构止损——找近20根1H里最近的有效摆动点作为止损参考。
    SHORT: 最近的摆动高点（左右各1根都比它低）+ 0.3% 缓冲
    LONG:  最近的摆动低点（左右各1根都比它高）- 0.3% 缓冲
    额外约束：距 entry 0.5~3.0 ATR（太近=噪声，太远=无意义）
    找不到满足条件的摆动点返回 None → 降级到原 ATR 逻辑
    """
    if not k1h or len(k1h) < 5:
        return None
    recent = k1h[-20:]
    n = len(recent)
    highs = [float(k[2]) for k in recent]
    lows  = [float(k[3]) for k in recent]

    if direction == "SHORT":
        # 从最新往旧扫，取第一个满足摆动高点条件的
        for i in range(n - 2, 0, -1):
            if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
                swing = highs[i]
                dist  = (swing - entry) / (atr + 1e-9)
                if 0.5 <= dist <= 3.0:
                    return swing * 1.003   # 高点上方 0.3% 缓冲
    else:  # LONG
        for i in range(n - 2, 0, -1):
            if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
                swing = lows[i]
                dist  = (entry - swing) / (atr + 1e-9)
                if 0.5 <= dist <= 3.0:
                    return swing * 0.997   # 低点下方 0.3% 缓冲
    return None


def _calc_sl(entry: float, atr: float, direction: str,
              liq_long: list, liq_short: list,
              tier: str, k1h: list | None = None) -> tuple[float, bool]:
    """
    止损价 + 是否降级标记。
    v3.13: 有清算区时，SL 紧贴清算区外侧（清算区下/上沿 ± 0.2ATR），
           不再外推整个 sl_mult，避免止损距离虚高。
           无清算区时仍用 entry ± sl_mult*ATR 降级。
    """
    sl_mult = _SL_MULT.get(tier, 1.5)

    if direction == "LONG":
        # v3.13: 清算区下沿外侧小缓冲，而非清算区下沿再外推 sl_mult
        # 若清算区引用的 SL 超过上限，降级用 ATR 公式（degraded=True）
        z = _nearest_liq_below(entry, atr, liq_long, dist_min=0.5, dist_max=4.0)
        if z:
            sl_candidate = z["price_low"] - atr * _SL_LIQ_BUFFER_ATR
            max_pct = _SL_MAX_MAP.get(tier, _SL_MAX_PCT)
            if abs(entry - sl_candidate) / (entry + 1e-9) <= max_pct:
                sl = sl_candidate
                degraded = False
            else:
                # 清算区引用的 SL 远超上限，降级用 ATR 公式
                sl = entry - atr * sl_mult
                degraded = True
                logger.debug(
                    f"[calc_sl] LONG liq SL {abs(entry-sl_candidate)/entry*100:.1f}% "
                    f"超过{max_pct*100:.0f}% 上限，降级 ATR"
                )
        else:
            sl = entry - atr * sl_mult
            degraded = True
        sl = min(sl, entry - atr * 0.5)    # 至少离入场 0.5 ATR
    else:
        z = _nearest_liq_above(entry, atr, liq_short, dist_min=0.5, dist_max=4.0)
        if z:
            sl_candidate = z["price_high"] + atr * _SL_LIQ_BUFFER_ATR
            max_pct = _SL_MAX_MAP.get(tier, _SL_MAX_PCT)
            if abs(sl_candidate - entry) / (entry + 1e-9) <= max_pct:
                sl = sl_candidate
                degraded = False
            else:
                # 清算区引用的 SL 远超上限，降级用 ATR 公式
                sl = entry + atr * sl_mult
                degraded = True
                logger.debug(
                    f"[calc_sl] SHORT liq SL {abs(sl_candidate-entry)/entry*100:.1f}% "
                    f"超过{max_pct*100:.0f}% 上限，降级 ATR"
                )
        else:
            sl = entry + atr * sl_mult
            degraded = True
        sl = max(sl, entry + atr * 0.5)

    # ── v3.27: 结构止损保护（摆动高/低点）────────────────────────────────────
    # 在清算图/ATR止损基础上，若近期有效摆动点更宽，取更宽值
    # 目的：防止止损挂在「庄家已知的 entry+1ATR」而被精准扫损
    if k1h:
        _struct_sl = _swing_point_sl(k1h, direction, entry, atr)
        if _struct_sl is not None:
            if direction == "SHORT" and _struct_sl > sl:
                _old = sl
                sl = _struct_sl
                # [v3.87 STEP4-1] 删除无效赋值 sl_degraded = True：本函数最终返回的是变量 degraded，
                #   这里的 sl_degraded 是局部遮蔽变量，从未传出。摆动点保护加宽止损「有意不触发降级」（降级会让执行器减半仓位），
                #   删除赋值不改变任何行为，仅消除误导。
                logger.info(
                    f"[calc_sl] SHORT 结构止损保护: "
                    f"原={_old:.6g} → 摆动高点={sl:.6g} "
                    f"(+{(_struct_sl/_old-1)*100:.1f}%)"
                )
            elif direction == "LONG" and _struct_sl < sl:
                _old = sl
                sl = _struct_sl
                # [v3.87 STEP4-1] 同上：无效赋值已删除，摆动点保护有意不触发降级。
                logger.info(
                    f"[calc_sl] LONG 结构止损保护: "
                    f"原={_old:.6g} → 摆动低点={sl:.6g} "
                    f"(-{(1-_struct_sl/_old)*100:.1f}%)"
                )

    # ── SL 最小距离保护（v3.37.1: 按 tier 动态设置）────────────────────────
    # low=2.5%（低波动，噪音小）/ mid=3.0%（正常山寨）/ high=5.0%（高波动，ATR大）
    _sl_min_map = {"low": 0.025, "mid": 0.03, "high": 0.05}
    _sl_min = _sl_min_map.get(tier, 0.025)
    sl_pct = abs(entry - sl) / (entry + 1e-9)
    if sl_pct < _sl_min:
        if direction == "LONG":
            sl = entry * (1 - _sl_min)
        else:
            sl = entry * (1 + _sl_min)

        # 最小距离拉宽后，检查新止损价是否落在清算密集区内部
        # 如果落在内部，继续外推到清算区外侧
        _check_zones = liq_long if direction == "LONG" else liq_short
        for _z in _check_zones:
            _mid = (_z["price_low"] + _z["price_high"]) / 2
            if direction == "LONG" and _z["price_low"] <= sl <= _z["price_high"]:
                # sl 落在多头清算区内，外推到清算区下沿
                sl_new = _z["price_low"] * (1 - 0.001)  # 清算区下丿多一点
                logger.warning(
                    f"[calc_sl] LONG 最小距离保护后 SL={sl:.6g} 落在清算区内"
                    f"[{_z['price_low']:.6g}~{_z['price_high']:.6g}]，"
                    f"外推至 {sl_new:.6g}"
                )
                sl = sl_new
                break
            elif direction == "SHORT" and _z["price_low"] <= sl <= _z["price_high"]:
                # sl 落在空头清算区内，外推到清算区上沿
                sl_new = _z["price_high"] * (1 + 0.001)
                logger.warning(
                    f"[calc_sl] SHORT 最小距离保护后 SL={sl:.6g} 落在清算区内"
                    f"[{_z['price_low']:.6g}~{_z['price_high']:.6g}]，"
                    f"外推至 {sl_new:.6g}"
                )
                sl = sl_new
                break

    return sl, degraded


# ─────────────────────────────────────────────────────────────────────────────
# 止盈计算
# ─────────────────────────────────────────────────────────────────────────────
# 订单簿 TP 缓存：key=(symbol, direction, price_bucket) → (ts, tp_result)
_OB_TP_CACHE: dict = {}
_OB_TP_TTL   = 30   # 秒

def _orderbook_tp(entry: float, atr: float, direction: str, symbol: str) -> float | None:
    """
    从订单簿找最近的挂单密集区作为 TP 参考价。
    做多：找上方 asks 里挂单量最大的 1% 价格桶的前沿（bucket 低端 - 0.2 ATR）。
    做空：找下方 bids 里挂单量最大的 1% 价格桶的前沿（bucket 高端 + 0.2 ATR）。
    返回 None 表示订单簿没有明显密集区或调用失败。
    v3.6: 加 30s 本地缓存，避免每次信号都发同步 HTTP 请求。
    """
    import time as _t
    cache_key = (symbol, direction, entry, atr)  # #32 完整输入身份，保留精确入场价与 ATR
    cached = _OB_TP_CACHE.get(cache_key)
    if cached and _t.time() - cached[0] < _OB_TP_TTL:
        return cached[1]
    try:
        from shared import binance_api as _api
        book  = _api.get_order_book(symbol, limit=100)
        if not book:
            _OB_TP_CACHE[cache_key] = (_t.time(), None)  # #33 正常空结果负缓存
            return None

        bucket_pct = 0.01   # 1% 价格桶

        if direction == "LONG":
            raw = [(float(p), float(q)) for p, q in book.get("asks", [])
                   if float(p) > entry]
        else:
            raw = [(float(p), float(q)) for p, q in book.get("bids", [])
                   if float(p) < entry]

        if not raw:
            _OB_TP_CACHE[cache_key] = (_t.time(), None)  # #33 正常空结果负缓存
            return None

        # 按 1% 区间聚合 USD 挂单量
        base   = entry
        bucket_size = base * bucket_pct
        buckets: dict[float, tuple[float, float, float]] = {}  # key → (low, high, usd)
        for p, q in raw:
            key = round(p / bucket_size) * bucket_size
            lo, hi, usd = buckets.get(key, (p, p, 0.0))
            buckets[key] = (min(lo, p), max(hi, p), usd + p * q)

        # 找 USD 挂单最大的区间
        best = max(buckets.values(), key=lambda x: x[2])
        lo, hi, usd = best

        # 要求密集区 USD 量至少是平均桶的 1.5 倍，否则不算"密集"
        avg_usd = sum(b[2] for b in buckets.values()) / len(buckets)
        if usd < avg_usd * 1.5:
            _OB_TP_CACHE[cache_key] = (_t.time(), None)  # #33 正常空结果负缓存
            return None

        # TP 定在密集区前沿（做多：bucket 低端 - 缓冲；做空：bucket 高端 + 缓冲）
        result = (lo - atr * 0.2) if direction == "LONG" else (hi + atr * 0.2)
        _OB_TP_CACHE[cache_key] = (_t.time(), result)
        return result
    except Exception:
        _OB_TP_CACHE[cache_key] = (_t.time(), None)   # 缓存失败结果，避免重复请求
        return None


def _calc_tp(entry: float, sl: float, atr: float, direction: str,
              liq_long: list, liq_short: list,
              symbol: str = "", tier: str = "mid", vol24_usd: float = 0.0,
              liq_ctx: dict | None = None) -> tuple[float, float | None, bool]:
    """
    TP 计算策略：
      有清算图且 2 处以上密集区 → TP1 + TP2 分批止盈
      有清算图但只有 1 处        → 仅 TP1
      无清算图                  → 订单簿找密集区作 TP1；找不到则 ATR 降级单 TP
    返回 (tp1, tp2_or_None, degraded)
    """
    sl_dist  = abs(entry - sl) + 1e-9
    degraded = False
    tp2      = None   # 默认无 TP2

    # ── 收集清算图候选区 ─────────────────────────────────────────────────────
    if direction == "LONG":
        liq_candidates = sorted(
            [z for z in liq_short if (z["price_low"] + z["price_high"]) / 2 > entry],
            key=lambda z: z["price_low"]
        )
    else:
        liq_candidates = sorted(
            [z for z in liq_long if (z["price_low"] + z["price_high"]) / 2 < entry],
            key=lambda z: z["price_high"], reverse=True
        )

    has_liq = len(liq_candidates) >= 1

    # ── 情况一：有清算图 ─────────────────────────────────────────────────────
    if has_liq:
        # TP1：最近的满足 RR≥1.5 的清算区前沿
        tp1 = None
        # [v3.88 OPT-14] TP1 距清算区前沿的缓冲按「相对强度」分档：≥20% 用 0.2ATR、5%~20% 用 0.35ATR、<5% 用 0.5ATR。
        #   原缓冲固定 0.3ATR。成交额或清算金额缺失时仍用 0.3ATR。TP2 缓冲（0.5ATR）不变。开关 OPT_LIQ_RELATIVE_STRENGTH。
        _rcfg_tp = _liq_rel_cfg()

        _ctx_tp = liq_ctx if liq_ctx is not None else {"vol24_usd": vol24_usd}

        def _tp1_buf(_z):
            if not _rcfg_tp:
                return 0.3
            # [v3.88 OPT-14e] 入场时锁定：持仓重算时若带入开仓时的缓冲值，直接沿用，不按最新清算图重算，
            #   保证 TP1 依据与开仓时一致、便于归因。
            if _ctx_tp.get("tp1_buffer_locked") is not None:
                _b = float(_ctx_tp["tp1_buffer_locked"])
            else:
                from shared.opt_v388 import tp1_buffer_atr as _tb
                _zones_tp = liq_short if direction == "LONG" else liq_long
                _b = _tb(_liq_rel(_z, _zones_tp, _ctx_tp), _rcfg_tp["strong"], _rcfg_tp["mid"],
                         _rcfg_tp["b_strong"], _rcfg_tp["b_mid"], _rcfg_tp["b_weak"], 0.3)
            _ctx_tp["tp1_buffer_used"] = _b   # 循环 break 前最后一次调用即被选中的区
            return _b
        for z in liq_candidates:
            if direction == "LONG":
                candidate = z["price_low"] - atr * _tp1_buf(z)
                if (candidate - entry) >= sl_dist * _TP1_MIN_RR:
                    tp1 = candidate; break
            else:
                candidate = z["price_high"] + atr * _tp1_buf(z)
                if (entry - candidate) >= sl_dist * _TP1_MIN_RR:
                    tp1 = candidate; break

        if tp1 is None:
            _ctx_tp.pop("tp1_buffer_used", None)   # [v3.88] 未锚定清算区，不记录缓冲
            tp1 = (entry + sl_dist * _TP1_MIN_RR) if direction == "LONG"                   else (entry - sl_dist * _TP1_MIN_RR)
            degraded = True

        # TP2：仅当有 2 处以上清算区时才设
        # v3.17：改为「最远且RR达标」策略，替代旧的「体量最大」策略
        # 旧问题：_zone_size取体量最大的区往往集中在 entry 附近，RR<2.5被拒
        # 新逻辑：按价格由远到近遍历，取第一个 RR 达标且位于 tp1 远侧的区
        if len(liq_candidates) >= 2:
            liq_src = liq_short if direction == "LONG" else liq_long
            if direction == "LONG":
                z2_pool = sorted(
                    [z for z in liq_src if (z["price_low"]+z["price_high"])/2 > entry],
                    key=lambda z: -z["price_low"]   # 由远到近
                )
                for z2 in z2_pool:
                    c2 = z2["price_low"] - atr * 0.5
                    if (c2 - entry) >= sl_dist * _TP2_MIN_RR and c2 > tp1:
                        tp2 = c2
                        break
                if tp2 is None and z2_pool:
                    logger.debug(
                        f"[calc_sl_tp] {symbol} TP2所有候选区均未通过RR门槛，tp2置None"
                    )
            else:
                z2_pool = sorted(
                    [z for z in liq_src if (z["price_low"]+z["price_high"])/2 < entry],
                    key=lambda z: z["price_high"]   # 由远到近
                )
                for z2 in z2_pool:
                    c2 = z2["price_high"] + atr * 0.5
                    if (entry - c2) >= sl_dist * _TP2_MIN_RR and c2 < tp1:
                        tp2 = c2
                        break
                if tp2 is None and z2_pool:
                    logger.debug(
                        f"[calc_sl_tp] {symbol} TP2所有候选区均未通过RR门槛，tp2置None"
                    )
        # 少于 2 处清算区 → tp2 保持 None，只挂单 TP

    # ── 情况二：无清算图 → 订单簿降级 ──────────────────────────────────────
    else:
        # high tier 标的 ATR 很大，orderbook 30s 缓存误差>20%，直接跳过用清算图
        ob_tp = (_orderbook_tp(entry, atr, direction, symbol)
                 if symbol and tier != "high" else None)

        if ob_tp is not None:
            # 订单簿找到密集区，验证 RR
            meets_rr = (ob_tp - entry >= sl_dist * _TP1_MIN_RR) if direction == "LONG"                        else (entry - ob_tp >= sl_dist * _TP1_MIN_RR)
            if meets_rr:
                tp1 = ob_tp
                logger.debug(f"[calc_sl_tp] {symbol} 订单簿 TP1={ob_tp:.6g}（无清算图降级）")
            else:
                tp1 = (entry + sl_dist * _TP1_MIN_RR) if direction == "LONG"                       else (entry - sl_dist * _TP1_MIN_RR)
                degraded = True
        else:
            # 完全降级：ATR 计算单 TP
            tp1 = (entry + sl_dist * _TP1_MIN_RR) if direction == "LONG"                   else (entry - sl_dist * _TP1_MIN_RR)
            degraded = True
        # tp2 保持 None（无清算图不分批）

    return tp1, tp2, degraded


# ─────────────────────────────────────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────────────────────────────────────
def calc_sl_tp(
    signal: dict,
    mark_price: float,
    atr: float,
    liq_zones: dict,       # {"long": [...], "short": [...]} 或 {}
    k1h: list,
    ema20: float,
) -> dict:
    """
    根据信号 dict 计算完整入场参数。

    返回 dict（成功）或 {"error": str}（需拒绝信号）：
    {
        "entry_price":  float,
        "sl_price":     float,
        "tp1_price":    float,
        "tp2_price":    float,
        "sl_dist_pct":  float,          # 止损距离 %（供日志）
        "rr_tp1":       float,          # TP1 盈亏比
        "rr_tp2":       float,          # TP2 盈亏比
        "liq_data_available": bool,
        "sl_degraded":  bool,
        "tp_degraded":  bool,
        "entry_mode":   str,            # [v3.87 STEP4-5] 实际取值：immediate(市价/贴近现价) | pending_order(等待回调的限价单，
                                        #   reversal/oi_divergence 专用) | weighted(回踩清算区加权入场价) | limit(range_bounce/stop_hunt 限价)
                                        #   | fixed(仓位维护沿用已成交价) | at_confirm(OPT-8 开启时的回踩确认处入场，默认关闭)
                                        #   执行器只对 pending_order 做特殊处理，其余值统一按 GTC 限价单提交
    }
    """
    mode      = signal.get("mode", "pullback")
    direction = (signal.get("direction") or "LONG").upper()
    tier      = signal.get("volatility_tier", "mid")
    entry_mode_hint = signal.get("entry_mode", "immediate")

    liq_long  = liq_zones.get("long",  [])
    liq_short = liq_zones.get("short", [])
    liq_data_available = bool(liq_long or liq_short)

    # [v3.88 OPT-14d] 清算区相对强度的分母上下文。oi_now 是合约张数，乘以标记价换算成 USDT 名义价值。
    _ind_ctx = signal.get("ind") or {}
    try:
        _oi_qty = float(_ind_ctx.get("oi_now") or signal.get("oi_now") or 0)
    except (TypeError, ValueError):
        _oi_qty = 0.0
    _liq_ctx = {
        "vol24_usd": float(_ind_ctx.get("vol24_usd", 0) or 0),
        "oi_usd":    _oi_qty * float(mark_price or 0),
    }
    # [v3.88 OPT-14g] 清算图数据来源级别：>=2 表示已降级到二/三级数据源（只读缓存，不触发网络请求）
    try:
        from shared.liq_cache import get_liq_src_level as _gsl
        _liq_ctx["liq_degraded"] = _gsl(signal.get("symbol", "")) >= 2
    except Exception:
        _liq_ctx["liq_degraded"] = False
    if signal.get("tp1_buffer_locked") is not None:
        _liq_ctx["tp1_buffer_locked"] = float(signal["tp1_buffer_locked"])

    # ── 1. 入场价 ─────────────────────────────────────────────────────────────
    # 仓位维护场景：若 signal 里已携带实际成交价（fill_price/entry_price），
    # 直接用作 entry 基准，跳过重新估算——保证 SL/TP 的 RR 与原始开仓一致
    _fixed_entry = signal.get("fill_price") or signal.get("entry_price")
    if _fixed_entry and float(_fixed_entry) > 0:
        entry  = float(_fixed_entry)
        e_mode = "fixed"
    elif mode == "pullback":
        entry, e_mode = _entry_pullback(mark_price, atr, ema20,
                                         liq_long, liq_short, direction, k1h,
                                         vol24_usd=(signal.get("ind") or {}).get("vol24_usd", 0.0), liq_ctx=_liq_ctx)
    elif mode == "breakout":
        entry, e_mode = _entry_breakout(mark_price, atr,
                                         liq_long, liq_short, direction)
    elif mode == "reversal":
        entry, e_mode, _degrade_info = _entry_reversal(mark_price, atr,
                                         liq_long, liq_short, direction, entry_mode_hint)
    elif mode == "funding_arb":
        # funding_arb 市价立即入场，SL = entry ± 1.5 ATR，不设 TP
        entry  = mark_price
        e_mode = "immediate"
    elif mode == "range_bounce":
        # v3.37: 限价回踩入场，挂在当前价下方 0.3 ATR
        # 与系统整体「等触发」哲学一致，确认回踩企稳后再入场，不直接吃市价
        entry  = round(mark_price - atr * 0.3, 8)
        e_mode = "limit"
    elif mode == "stop_hunt":
        # v3.49: 止损猎杀限价单，入场价 = 当前 mark_price（收回确认时的实时价）
        # signal_watcher 会在 entry_price ±0.5% 区间内挂限价单
        # 如果 signal 里已有 fill_price / entry_price，上方已经走 _fixed_entry 分支
        entry  = mark_price
        e_mode = "limit"
    elif mode == "oi_divergence":
        # v3.63: P0 修复——入场价改为"近期高点回落 1.8%"逻辑
        #
        # 评审结论（2026-10-19）：
        #   v3.62 的 min(收阴K收盘, EMA20)*1.001 方向完全错误——
        #   EMA20 是拉升前的均线支撑，不是回落后的空单入场位；
        #   等价格跌回 EMA20 = 等回吐全部涨幅 = 在多头支撑区做空。
        #
        # 正确逻辑（评审建议）：
        #   SHORT：挂单价 = 近3根K最高价 × 0.982（从高点回落约 1.8%）
        #           下限 = 最近收阴K收盘（跌破此位说明回落过深，信号作废）
        #           上限 = mark_price × 0.9995（略低于市价，防立刻成交）
        #   LONG：挂单价 = 近3根K最低价 × 1.018
        #           上限 = 最近收阳K收盘
        #           下限 = mark_price × 1.0005
        #
        # 配合 judges.py #6c（价格从高点回落 ≥0.8% 确认后才发信号）：
        #   信号发出时价格已开始回落，挂单价在回落方向上略低一点，
        #   等价格继续下行触发，而不是在上行途中追空。
        _oid_entry = mark_price
        try:
            _closed_k = k1h[:-1] if len(k1h) > 1 else k1h
            if len(_closed_k) >= 3:
                # v3.63b: 入场价改为 ATR 动态回落，取代固定 1.8%
                # 逻辑：挂单价 = 近期极值 - max(0.6%, 0.6 × ATR_1H)
                # 系数选 0.6（评审建议 0.8，偏松；加密市场高波动性下用 0.6 更稳健）
                # 下限 0.6%（比评审建议 0.5% 略高，过滤极低波动噪声）
                _atr_offset = max(mark_price * 0.006, atr * 0.6)

                if direction == "SHORT":
                    _recent_high = max(float(_kk[2]) for _kk in _closed_k[-3:])
                    _entry_candidate = _recent_high - _atr_offset

                    # 下限保护：不低于最近一根收阴K的收盘价
                    # 无收阴K时用 mark_price*0.992 兜底（评审建议，防近5根全阳时无保护）
                    _bearish_close = next(
                        (float(_kk[4]) for _kk in reversed(_closed_k[-5:])
                         if float(_kk[4]) < float(_kk[1])),
                        None
                    )
                    # [v3.88 OPT-15] 近5根全阳（无收阴K）：直接拒绝，不再用 mark*0.992 人造下限。
                    #   原兜底得到的入场价低于市价，空单限价单会立刻成交（变市价追空），且与清算图锚点无关。
                    #   开关 OPT_OID_REJECT_ALL_BULL（默认开启）；关闭时保持原兜底。
                    if _bearish_close is None and _oid_reject_all_one_side():
                        return {"error": "oi_divergence SHORT 近5根全为阳线（无收阴K），拒绝信号（v3.88 OPT-15）"}
                    _floor = _bearish_close if _bearish_close is not None else mark_price * 0.992
                    _entry_candidate = max(_entry_candidate, _floor)

                    # 上限保护：不超过当前市价
                    _oid_entry = round(min(_entry_candidate, mark_price * 0.9995), 8)
                    if _oid_entry >= mark_price:
                        _oid_entry = round(mark_price * 0.9982, 8)

                elif direction == "LONG":
                    _recent_low = min(float(_kk[3]) for _kk in _closed_k[-3:])
                    _entry_candidate = _recent_low + _atr_offset

                    # 上限保护：不高于最近一根收阳K的收盘价
                    _bullish_close = next(
                        (float(_kk[4]) for _kk in reversed(_closed_k[-5:])
                         if float(_kk[4]) > float(_kk[1])),
                        None
                    )
                    # [v3.88 OPT-15] 近5根全阴（无收阳K）：LONG 同理直接拒绝。
                    if _bullish_close is None and _oid_reject_all_one_side():
                        return {"error": "oi_divergence LONG 近5根全为阴线（无收阳K），拒绝信号（v3.88 OPT-15）"}
                    _ceil = _bullish_close if _bullish_close is not None else mark_price * 1.008
                    _entry_candidate = min(_entry_candidate, _ceil)

                    # 下限保护：不低于当前市价
                    _oid_entry = round(max(_entry_candidate, mark_price * 1.0005), 8)
                    if _oid_entry <= mark_price:
                        _oid_entry = round(mark_price * 1.0018, 8)

        except Exception as _oid_err:
            import logging as _log
            _log.getLogger("fx.calc_sl_tp").debug(f"oi_divergence 入场价计算失败，降级到 mark_price: {_oid_err}")
            _oid_entry = mark_price
        entry  = _oid_entry
        e_mode = "pending_order"
    else:
        entry = mark_price
        e_mode = "immediate"

    # ── 1.5 Volume Profile 结构位（v3.62）────────────────────────────────────
    # 从 signal 里读取预算好的 VP 数据（由 scanner/analyze_symbol 注入）
    # 用于在清算图命中率不足时提供额外 SL/TP 结构位候选
    _vp = signal.get("vp") or {}
    _vp_valid = _vp.get("valid", False)
    _vp_poc = float(_vp.get("poc", 0.0))
    _vp_vah = float(_vp.get("vah", 0.0))
    _vp_val = float(_vp.get("val", 0.0))

    # ── 2. 止损 ──────────────────────────────────────────────────────────────
    sl, sl_degraded = _calc_sl(entry, atr, direction,
                                liq_long, liq_short, tier, k1h=k1h)

    # ── 2b. oi_divergence 止损回避清算密集带（v3.63b P1）────────────────────
    # 评审建议（P2→P1 提升）：止损线恰好落在清算密集带内时，做市商极易猎杀。
    # 当 mode=oi_divergence 时，主动检查 SL ±1% 范围内是否有 >$30万 同向清算。
    # 若有，将 SL 外移至清算带外侧 +0.3% 作为缓冲。
    # 阈值 $30万 比入场过滤（#6b $50万）更宽松：止损被猎杀的成本比不入场更高。
    if mode == "oi_divergence":
        try:
            _liq_sl_symbol = signal.get("symbol", "")
            _liq_sl_zones  = liq_short if direction == "SHORT" else liq_long
            if _liq_sl_symbol and not _liq_sl_zones:
                # 若 liq_zones 未传入，尝试从缓存补充
                from shared.liq_cache import get_liq_zones_cached as _glzc
                _liq_sl_zones = _glzc(_liq_sl_symbol,
                                       "SHORT" if direction == "SHORT" else "LONG",
                                       entry, range_="3d")
            _SL_LIQ_CHECK_RANGE = 0.01   # SL ±1% 检查范围
            _SL_LIQ_THRESHOLD   = 300_000  # $30万
            _sl_wall_usd = 0.0
            for _z in _liq_sl_zones:
                _zmid = (_z["price_low"] + _z["price_high"]) / 2
                if abs(_zmid - sl) / (sl + 1e-9) <= _SL_LIQ_CHECK_RANGE:
                    _sl_wall_usd += _z.get("liq_usd", 0)
            if _sl_wall_usd >= _SL_LIQ_THRESHOLD:
                _sl_old = sl
                # 外移：SHORT SL 上移（更高），LONG SL 下移（更低），各加 0.3% 缓冲
                if direction == "SHORT":
                    sl = round(sl * 1.003, 8)
                else:
                    sl = round(sl * 0.997, 8)
                logger.info(
                    f"[calc_sl_tp] oi_div SL 清算回避: {direction} "
                    f"原SL={_sl_old:.4g} 清算量=${_sl_wall_usd/1e3:.0f}K "
                    f"→ 新SL={sl:.4g} (+0.3%缓冲)"
                )
        except Exception as _sl_liq_err:
            logger.debug(f"[calc_sl_tp] oi_div SL清算检查异常（跳过）: {_sl_liq_err}")

    # [v3.87 OPT-9] 失效位止损（回踩 / EMA 趋势 / 反转）。
    #   原止损由清算图（非币安官方数据，经常缺失→降级）或固定 ATR/百分比决定，与开仓理由无关：
    #   太近会被正常波动扫掉，太远又在结构失效后多亏一段。
    #   新：止损放在最近 8 根已闭合 1H 的结构低点(高点)外 0.3ATR，距入场限制在 1.0~3.0ATR；
    #   结构太远时保留原止损。只用币安官方 K 线。开关 OPT_INVALIDATION_SL。
    #   注意：止损距离变化会通过 RISK_PER_TRADE 影响名义仓位（止损更近→仓位更大，仍受单仓上限约束）。
    try:
        from config import OPT_INVALIDATION_SL as _OPT_ISL
    except Exception as _cfg_e:
        _OPT_ISL = False
        _cfg_fallback("OPT_INVALIDATION_SL", _cfg_e, {"enabled": _OPT_ISL})
    if _OPT_ISL and mode in ("pullback", "ema_trend", "reversal") and k1h:
        from shared.opt_v387 import invalidation_sl as _inv_sl
        _k1h_closed = k1h[:-1] if len(k1h) > 1 else k1h
        _isl = _inv_sl(entry, atr, direction, _k1h_closed)
        if _isl is not None and _isl > 0:
            # 仍遵守原有「按波动层的最小止损距离」（low 2.5% / mid 3% / high 5%），失效位更近时取最小距离，
            #   避免把止损放进正常噪声区间；只有结构位更远时才采用结构位。
            _min_pct = {"low": 0.025, "mid": 0.03, "high": 0.05}.get(tier, 0.025)
            if direction == "LONG":
                _isl = min(_isl, entry * (1 - _min_pct))
            else:
                _isl = max(_isl, entry * (1 + _min_pct))
            logger.info(f"[calc_sl_tp] {mode} {direction} 失效位止损: {sl:.6g} → {_isl:.6g}（OPT-9）")
            sl = _isl
            # 不改 sl_degraded：executor 用它决定是否降仓/拒单，保持原语义，避免静默改变仓位。
            signal["_sl_source"] = "invalidation"

    # v3.62: VP 止损增强——sl_degraded 时（无清算图）用 VP val/vah 作为结构位候选
    # LONG：val（价值区下沿）在入场价下方时，SL 取 val - 0.3ATR（但不超过 ATR 止损）
    # SHORT：vah（价值区上沿）在入场价上方时，SL 取 vah + 0.3ATR（但不超过 ATR 止损）
    if sl_degraded and _vp_valid and mode not in ("stop_hunt", "funding_arb"):
        _max_sl_pct = {"low": 0.12, "mid": 0.15, "high": 0.20}.get(tier, 0.15)
        _vp_sym = signal.get("symbol", "")
        if direction == "LONG" and 0 < _vp_val < entry and _vp_val < entry * 0.5:
            logger.warning(
                f"[calc_sl_tp] {_vp_sym} VP val={_vp_val:.4g} "
                f"< entry*0.5={entry*0.5:.4g}，"
                f"疑似含暴涨前旧区间，跳过VP止损增强"
            )
        elif direction == "SHORT" and _vp_vah > entry > 0 and _vp_vah > entry * 1.5:
            logger.warning(
                f"[calc_sl_tp] {_vp_sym} VP vah={_vp_vah:.4g} "
                f"> entry*1.5={entry*1.5:.4g}，"
                f"疑似含暴跌前旧区间，跳过VP止损增强"
            )
        elif direction == "LONG" and 0 < _vp_val < entry:
            _vp_sl = _vp_val - atr * 0.3
            _vp_sl_pct = abs(entry - _vp_sl) / (entry + 1e-9)
            if _vp_sl_pct <= _max_sl_pct and _vp_sl > entry * 0.5:
                # 取两者更宽（更保守）的止损，VP 结构位可能比 ATR 更合理
                if _vp_sl < sl:
                    logger.info(
                        f"[calc_sl_tp] LONG VP止损增强: ATR_sl={sl:.6g} → VP_val={_vp_val:.6g} "
                        f"vp_sl={_vp_sl:.6g} ({_vp_sl_pct*100:.1f}%)"
                    )
                    sl = _vp_sl
                    sl_degraded = False   # VP 结构位有效，不再算降级
        elif direction == "SHORT" and _vp_vah > entry > 0:
            _vp_sl = _vp_vah + atr * 0.3
            _vp_sl_pct = abs(_vp_sl - entry) / (entry + 1e-9)
            if _vp_sl_pct <= _max_sl_pct:
                if _vp_sl > sl:
                    logger.info(
                        f"[calc_sl_tp] SHORT VP止损增强: ATR_sl={sl:.6g} → VP_vah={_vp_vah:.6g} "
                        f"vp_sl={_vp_sl:.6g} ({_vp_sl_pct*100:.1f}%)"
                    )
                    sl = _vp_sl
                    sl_degraded = False

    # v3.52: stop_hunt 专属 SL/TP1
    # SL = sweep_extreme 外侧 0.3%
    # TP1 = ref_level（前高/前低，猎杀完成点）
    # RR 用实际成交价动态计算：(tp1-entry)/(entry-sl)——二探越深 RR 越高自然过滤
    if mode == "stop_hunt":
        _jd            = signal.get("judge_detail") or {}
        _ref_level     = float(_jd.get("ref_level")     or _jd.get("pivot_level")    or 0)
        _sweep_extreme = float(_jd.get("sweep_extreme") or _jd.get("pierce_extreme") or
                               signal.get("sl_price_hint") or 0)
        if _ref_level > 0 and _sweep_extreme > 0:
            # SL：extreme 外侧 0.3%
            if direction == "LONG":
                sl  = round(_sweep_extreme * 0.997, 8)
                tp1 = round(_ref_level * 0.999, 8)     # 前低稍下方一点点，避免精确触处
            else:
                sl  = round(_sweep_extreme * 1.003, 8)
                tp1 = round(_ref_level * 1.001, 8)     # 前高稍上方一点点
            sl_degraded = False

            # 止损距离上限检查
            if not _check_sl_distance(entry, sl, direction, tier):
                sl_pct = abs(entry - sl) / (entry + 1e-9) * 100
                max_pct = _SL_MAX_MAP.get(tier, _SL_MAX_PCT) * 100
                return {"error": f"stop_hunt SL距离 {sl_pct:.1f}% 超过 {max_pct:.0f}%，拒绝"}

            # 动态 RR 检查（用实际成交价 entry 计算）
            # 二探越深（entry 离 pivot 越远）→ TP1 距离越大 → RR 越高 → 自然过滤浅回踩
            try:
                from config import RR_MIN_STOP_HUNT as _RR_SH
            except Exception as _cfg_e:
                _RR_SH = 0.8
                _cfg_fallback("RR_MIN_STOP_HUNT", _cfg_e, {"value": _RR_SH})
            _sl_dist_sh = abs(entry - sl) + 1e-9
            _tp1_dist   = abs(tp1 - entry)
            _rr_sh      = _tp1_dist / _sl_dist_sh
            if _rr_sh < _RR_SH:
                return {"error": (
                    f"stop_hunt RR={_rr_sh:.2f} < {_RR_SH}："
                    f"entry={entry:.4g} pivot={_ref_level:.4g} 距离仅 {_tp1_dist/entry*100:.2f}%，"
                    f"二探过浅或入场时机已过，拒绝"
                )}

            # TP2：清算图找更远目标（RR>=1.5，在 TP1 更远侧）
            _, tp2, tp_degraded = _calc_tp(entry, sl, atr, direction,
                                            liq_long, liq_short,
                                            symbol=signal.get("symbol",""), tier=tier,
                                            vol24_usd=(signal.get("ind") or {}).get("vol24_usd", 0.0), liq_ctx=_liq_ctx)
            if tp2 is not None:
                _rr2     = abs(tp2 - entry) / _sl_dist_sh
                _tp2_far = (tp2 > tp1) if direction == "LONG" else (tp2 < tp1)
                if not _tp2_far or _rr2 < 1.5:
                    tp2 = None

            logger.info(
                f"[calc_sl_tp] {signal.get('symbol','')} stop_hunt TP1=ref_level: "
                f"entry={entry:.4g} SL={sl:.4g}(sweep={_sweep_extreme:.4g}) "
                f"TP1={tp1:.4g}(pivot={_ref_level:.4g}) "
                f"RR={_rr_sh:.2f}x  TP2={'N/A' if tp2 is None else f'{tp2:.4g}'}"
            )
            sl_dist = abs(entry - sl) + 1e-9
            return {
                "entry_price":        round(entry, 8),
                "sl_price":           round(sl, 8),
                "tp1_price":          round(tp1, 8),
                "tp2_price":          round(tp2, 8) if tp2 else None,
                "sl_dist_pct":        round(sl_dist / (entry + 1e-9) * 100, 4),
                "rr_tp1":             round(_rr_sh, 4),
                "rr_tp2":             round(abs(tp2-entry)/_sl_dist_sh, 4) if tp2 else None,
                "liq_data_available": bool(liq_long or liq_short),
                "sl_degraded":        False,
                "tp_degraded":        tp_degraded,
                "entry_mode":         "limit",
            }
        else:
            # 降级：无 ref_level/sweep_extreme 时用旧逻辑
            _sl_hint = _jd.get("sl_price_hint") or signal.get("sl_price_hint")
            if _sl_hint and float(_sl_hint) > 0:
                sl = float(_sl_hint)
                sl_degraded = False

    # 止损距离动态上限检查（low=12% mid=15% high=20%）
    if not _check_sl_distance(entry, sl, direction, tier):
        sl_pct = abs(entry - sl) / (entry + 1e-9) * 100
        max_pct = _SL_MAX_MAP.get(tier, _SL_MAX_PCT) * 100
        return {"error": f"止损距离 {sl_pct:.1f}% 超过 {max_pct:.0f}%（tier={tier}），拒绝信号"}

    # ── funding_arb：止损 SL_ATR_MULT×ATR，不设 TP ──────────────────────────
    if mode == "funding_arb":
        try:
            from config import FUNDING_ARB_SL_ATR_MULT as _FA_SL_MULT
        except Exception as _cfg_e:
            _FA_SL_MULT = 1.5
            _cfg_fallback("FUNDING_ARB_SL_ATR_MULT", _cfg_e, {"value": _FA_SL_MULT})
        if direction == "LONG":
            sl = entry - atr * _FA_SL_MULT
        else:
            sl = entry + atr * _FA_SL_MULT
        return {
            "entry_price":       round(entry, 8),
            "sl_price":          round(sl, 8),
            "tp1_price":         None,   # funding_arb 无固定止盈，由时间和费率驱动
            "tp2_price":         None,
            "sl_dist_pct":       round(abs(entry - sl) / (entry + 1e-9) * 100, 4),
            "rr_tp1":            None,
            "rr_tp2":            None,
            "liq_data_available": False,
            "sl_degraded":       False,
            "tp_degraded":       False,
            "entry_mode":        "immediate",
            "liq_entry_raw":        None,
            "liq_entry_dist_atr":  None,
            "entry_degraded_reason": None,
        }

    # ── 3. 止盈 ──────────────────────────────────────────────────────────────
    symbol_hint = signal.get("symbol", "")

    # v3.36.4 range_bounce：TP1 优先锚定横盘区间上沿（range_ceiling）
    # 区间宽度通常5~15%，通用清算图TP可能设到20%+导致超时
    _range_ceiling = (signal.get("judge_detail") or signal.get("reason") or {}).get("c_high")
    if mode == "range_bounce" and _range_ceiling and float(_range_ceiling) > entry:
        _rc = float(_range_ceiling)
        _rc_dist = _rc - entry
        _sl_dist_rb = abs(entry - sl)
        if _rc_dist >= _sl_dist_rb * 1.5:  # 满足最低盈亏比 1.5
            tp1 = round(_rc * 0.98, 8)     # 稍低于区间上沿，避免精确触顶
            tp2 = None
            tp_degraded = False
            logger.debug(
                f"[calc_sl_tp] range_bounce TP1 锚定区间上沿: "
                f"ceiling={_rc:.6g} → tp1={tp1:.6g} RR={_rc_dist/_sl_dist_rb:.2f}x"
            )
        else:
            # 区间上沿太近，RR不足，降级用通用逻辑
            tp1, tp2, tp_degraded = _calc_tp(entry, sl, atr, direction,
                                              liq_long, liq_short,
                                              symbol=symbol_hint, tier=tier,
                                              vol24_usd=(signal.get("ind") or {}).get("vol24_usd", 0.0), liq_ctx=_liq_ctx)
    else:
        tp1, tp2, tp_degraded = _calc_tp(entry, sl, atr, direction,
                                          liq_long, liq_short,
                                          symbol=symbol_hint, tier=tier,
                                          vol24_usd=(signal.get("ind") or {}).get("vol24_usd", 0.0), liq_ctx=_liq_ctx)

    # v3.62: VP TP1 增强——tp_degraded 时（无清算图 TP 锚点）用 VP vah/val 作为候选
    # LONG：vah（价值区上沿）在入场价上方 + RR >= 1.2 时，替换 tp_degraded TP1
    # SHORT：val（价值区下沿）在入场价下方 + RR >= 1.2 时，替换 tp_degraded TP1
    if tp_degraded and _vp_valid and mode not in ("stop_hunt", "funding_arb", "range_bounce"):
        _sl_dist = abs(entry - sl)
        # [v3.87 STEP4-3] 原门槛写死 1.2，低于全局 TP1 最小 RR（RR_MIN_TP1=2.0，高波动层 RR_MIN_TP1_HIGH=1.5），
        #   VP 路径会放过本该被拒绝的低 RR 单。改为与全局一致。
        _vp_rr_min = _CFG_RR_MIN_TP1_HIGH if tier == "high" else _TP1_MIN_RR
        if direction == "LONG" and _vp_vah > entry and _vp_vah > entry * 1.5:
            logger.warning(
                f"[calc_sl_tp] {symbol_hint} VP vah={_vp_vah:.4g} "
                f"> entry*1.5={entry*1.5:.4g}，"
                f"疑似含暴跌前旧区间，跳过VP TP1增强"
            )
        elif direction == "SHORT" and 0 < _vp_val < entry and _vp_val < entry * 0.5:
            logger.warning(
                f"[calc_sl_tp] {symbol_hint} VP val={_vp_val:.4g} "
                f"< entry*0.5={entry*0.5:.4g}，"
                f"疑似含暴涨前旧区间，跳过VP TP1增强"
            )
        elif direction == "LONG" and _vp_vah > entry:
            _vp_rr = (_vp_vah - entry) / (_sl_dist + 1e-9)
            if _vp_rr >= _vp_rr_min:
                logger.info(
                    f"[calc_sl_tp] LONG VP TP1增强: degraded→vah={_vp_vah:.6g} "
                    f"RR={_vp_rr:.2f} (原tp1={tp1:.6g})"
                )
                tp1 = round(_vp_vah * 0.999, 8)   # 稍低于 vah，避免精确触顶
                tp_degraded = False
        elif direction == "SHORT" and 0 < _vp_val < entry:
            _vp_rr = (entry - _vp_val) / (_sl_dist + 1e-9)
            if _vp_rr >= _vp_rr_min:
                logger.info(
                    f"[calc_sl_tp] SHORT VP TP1增强: degraded→val={_vp_val:.6g} "
                    f"RR={_vp_rr:.2f} (原tp1={tp1:.6g})"
                )
                tp1 = round(_vp_val * 1.001, 8)   # 稍高于 val
                tp_degraded = False

    # ── TP1 上限保护：按 tier 动态截断（high tier 放宽至 25%，防止 RR 不足）──
    _tp1_cap = _TP1_MAX_PCT_HIGH if tier == "high" else _TP1_MAX_PCT
    # v3.14: high tier RR 要求放宽至 1.5（SL>12.5% 时 TP cap25% 与 RR2.0 互斥）
    _rr_min = _CFG_RR_MIN_TP1_HIGH if tier == "high" else _TP1_MIN_RR
    tp1_pct = abs(tp1 - entry) / (entry + 1e-9)
    if tp1_pct > _tp1_cap:
        if direction == "LONG":
            tp1 = entry * (1 + _tp1_cap)
        else:
            tp1 = entry * (1 - _tp1_cap)
        # v3.15.3: 截断后 RR 不足 → 尝试收紧 SL 使 RR 达标，仍不足则拒绝
        _sl_dist_check = abs(entry - sl) + 1e-9
        _rr_after_cap  = abs(tp1 - entry) / _sl_dist_check
        if _rr_after_cap < _rr_min:
            # 尝试收紧 SL：SL = entry ± tp1_dist / _rr_min（最小距离按波动层，见 _tighten_sl_after_cap）
            sl_tight, _sl_tight_pct = _tighten_sl_after_cap(entry, sl, tp1, direction, _rr_min, tier)
            if sl_tight is not None:
                logger.info(
                    f"[calc_sl_tp] {symbol_hint} TP1 cap 后 RR={_rr_after_cap:.2f} < {_rr_min}，"
                    f"收紧 SL: {sl:.6g} → {sl_tight:.6g} (-{_sl_tight_pct*100:.1f}%)，RR 修复为 {_rr_min}，"
                    f"SL 已非清算图位置，标记 sl_degraded=True"
                )
                sl = sl_tight
                sl_degraded = True  # v3.19: SL 被 TP cap 反推，不再来自清算图，强制降级
                _sl_dist_check = abs(entry - sl) + 1e-9
            else:
                # 无法收紧（原 SL 本就很近，或收紧后低于 2.5% 最小距离）→ 拒绝
                return {"error": (
                    f"TP1 cap {_tp1_cap*100:.0f}% 后 RR={_rr_after_cap:.2f} < {_rr_min}，"
                    f"收紧 SL 至 {_sl_tight_pct*100:.1f}% 不满足 {tier} 层最小止损距离，或比原止损更宽，拒绝信号"
                )}
        else:
            logger.debug(f"TP1 收窄至 {_tp1_cap*100:.0f}%（tier={tier}）: {tp1_pct*100:.1f}% → {_tp1_cap*100:.0f}%，RR={_rr_after_cap:.2f}")

    sl_dist = abs(entry - sl) + 1e-9
    rr1 = abs(tp1 - entry) / sl_dist
    rr2 = abs(tp2 - entry) / sl_dist if tp2 is not None else None

    # ── 力度系数 + 动态目标 + 一致性得分（观测层，不影响 TP 定价） ─────────────
    momentum_data = {}
    consistency_data = {}
    try:
        from shared.momentum_score import calc_momentum, calc_tp_consistency
        _ind      = signal.get("ind", {})
        rsi_1h   = _ind.get("rsi_1h", signal.get("rsi_1h", 50.0))
        funding  = _ind.get("funding_rate", signal.get("funding_rate", 0.0))
        # 量比：从 ind 取，干不到再从 k1h 计算
        vol_ratio = _ind.get("vol_ratio", signal.get("vol_ratio", 1.0))
        if vol_ratio == 1.0 and k1h and len(k1h) >= 5:
            vols    = [float(k[5]) for k in k1h]
            vol_ma  = sum(vols[-20:]) / min(20, len(vols))
            cur_vol = float(k1h[-1][5])
            if vol_ma > 0:
                vol_ratio = cur_vol / vol_ma
        momentum_data    = calc_momentum(
            direction   = direction,
            rsi_1h      = rsi_1h,
            vol_ratio   = vol_ratio,
            k1h         = k1h,
            funding     = funding,
            entry_price = entry,
            sl_price    = sl,
        )
        consistency_data = calc_tp_consistency(
            direction      = direction,
            dynamic_target = momentum_data["dynamic_target"],
            tp1_price      = tp1,
            atr            = atr,
        )
    except Exception as _me:
        logger.debug(f"[calc_sl_tp] momentum_score 计算失败（跳过）: {_me}")

    return {
        "entry_price":        round(entry, 8),
        "sl_price":           round(sl,    8),
        "tp1_price":          round(tp1,   8),
        "tp2_price":          round(tp2, 8) if tp2 is not None else None,
        "sl_dist_pct":        round(abs(entry - sl) / (entry + 1e-9) * 100, 3),
        "rr_tp1":             round(rr1, 2),
        "rr_tp2":             round(rr2, 2) if rr2 is not None else None,
        "liq_data_available": liq_data_available,
        "sl_degraded":        sl_degraded,
        "tp_degraded":        tp_degraded,
        "entry_mode":         e_mode,
        # ── 观测层字段（不影响执行逻辑） ──────────────────────────────────────
        "momentum_coeff":     momentum_data.get("momentum_coeff"),
        "dynamic_target":     momentum_data.get("dynamic_target"),
        "momentum_dims":      {
            "rsi":    momentum_data.get("dim_rsi"),
            "vol":    momentum_data.get("dim_vol"),
            "price":  momentum_data.get("dim_price_momentum"),
            "funding": momentum_data.get("dim_funding"),
        } if momentum_data else None,
        "consistency_score":  consistency_data.get("consistency_score"),
        "consistency_note":   consistency_data.get("consistency_note"),
        "tp_vs_dynamic_atr":  consistency_data.get("tp_vs_dynamic_atr"),   # 绝对距离（>=0）
        # [v3.89 #74] 有方向偏差字段，只记录、不参与任何决策（正=TP1 更近/保守，负=更远/激进）
        "tp_vs_dynamic_signed_dist": consistency_data.get("tp_vs_dynamic_signed_dist"),
        "tp_vs_dynamic_signed_atr":  consistency_data.get("tp_vs_dynamic_signed_atr"),
        "tp_relative":               consistency_data.get("tp_relative"),
        "consistency_schema_version": consistency_data.get("consistency_schema_version"),
        # [v3.88 OPT-14d] 只记录、不参与决策：入场侧最近清算区与 TP1 侧最近清算区在三种分母下的相对强度
        "liq_metrics":        _collect_liq_metrics(entry, atr, direction, liq_long, liq_short, _liq_ctx),
        # [v3.88 OPT-14e] 本次 TP1 实际采用的清算区缓冲（ATR 倍数），写入持仓供重算时锁定；None=未锚定清算区
        "tp1_liq_buffer_atr": _liq_ctx.get("tp1_buffer_used"),
    }
