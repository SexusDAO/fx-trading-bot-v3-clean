"""
shared/judges.py — 所有策略的信号判断函数

策略全图（按初始加入时间排列）：
  judge_pullback      — 结构回踩型（EMA 方向 + 价格进入区间）
  judge_breakout      — 动量突破型（新高/新低 + 量能放大）
  judge_reversal      — 极端反转型（chg24 大跌/大涨 + RSI 极端区）
  judge_funding_arb   — 资金费率套利（费率极端 + 长短对冲）
  judge_range_bounce  — 横盘区间弹升（大涨后进入横盘区间下半部）
  judge_stop_hunt     — 止损猎杀反转型（v3.49）（刷前高/前低 + 15M 成交量验证）
  judge_oi_divergence — OI 背离反转型（v3.60）（价格创新高/低 + OI 方向相反）
  judge_ema_trend     — EMA 趋势跟随型（v3.61）（四线排列回踩，只做多）

调用规范：
- 所有函数只使用已闭合 K 线（调用前外部已去掉未闭合根）
- 任何异常都被捕获，返回 (False, {"reject_reason": ...})
- 不写任何共享状态，线程安全
- 所有 ATR 统一用 1H ATR14

决策流程：
  scanner._is_xxx() 粗筛 → scanner.analyze_symbol() 路由 → judge_xxx() 精筛
  返回 (True, dict) 表示信号有效，dict 里包含方向/强度/入场提示等
  executor 读取 dict 里的内容计算入场参数（calc_sl_tp）
"""

from __future__ import annotations
import logging
import math
from shared.indicators import ema as _ema_raw, rsi as _rsi_raw, atr as _atr_raw


def _cfg_fallback(group: str, exc: BaseException, effective: dict) -> None:
    """
    [v3.89 #31] 配置导入回退时的可见性：
      ImportError（config 里没有这些可选项）→ 正常，info 记录实际生效值；
      其他异常（config 模块本身加载损坏）→ error，并输出实际生效的内置默认值。
    只增加日志，不改变任何取值。
    """
    lg = logging.getLogger("fx.judges")
    if isinstance(exc, ImportError):
        lg.info(f"[judges] 可选配置 {group} 未提供（{exc}），使用内置默认值: {effective}")
    else:
        lg.error(f"[judges] 配置模块加载异常 {group}（{type(exc).__name__}: {exc}），使用内置默认值: {effective}")

try:
    import sys as _sys, pathlib as _pl
    _sys.path.insert(0, str(_pl.Path(__file__).parent.parent))
    from config import PULLBACK_LOWER_LIMIT_LOW as _PB_LOW, PULLBACK_LOWER_LIMIT_MID as _PB_MID
except Exception as _cfg_e:
    _PB_LOW, _PB_MID = 1.2, 2.0
    _cfg_fallback("PULLBACK_LOWER_LIMIT", _cfg_e, {"_PB_LOW": _PB_LOW, "_PB_MID": _PB_MID})
try:
    from config import (
        PULSE_CANDLE_VOL_RATIO_1H as _PC_VOL_1H,
        PULSE_CANDLE_VOL_RATIO_4H as _PC_VOL_4H,
        PULSE_CANDLE_BODY_CHG_1H  as _PC_BODY_1H,
        PULSE_CANDLE_BODY_CHG_4H  as _PC_BODY_4H,
        PULSE_CANDLE_BODY_RATIO   as _PC_BODY_RATIO,
        PULSE_CANDLE_LOOKBACK_1H  as _PC_LB_1H,
        PULSE_CANDLE_LOOKBACK_4H  as _PC_LB_4H,
    )
except Exception as _cfg_e:
    _PC_VOL_1H, _PC_VOL_4H  = 3.0, 4.0
    _PC_BODY_1H, _PC_BODY_4H = 5.0, 8.0
    _PC_BODY_RATIO = 0.5
    _PC_LB_1H, _PC_LB_4H = 2, 3
    _cfg_fallback("PULSE_CANDLE", _cfg_e, {"vol_1h": _PC_VOL_1H, "vol_4h": _PC_VOL_4H, "body_1h": _PC_BODY_1H,
                                           "body_4h": _PC_BODY_4H, "body_ratio": _PC_BODY_RATIO,
                                           "lb_1h": _PC_LB_1H, "lb_4h": _PC_LB_4H})
try:
    from config import (
        PULLBACK_SHORT_CHG24_MIN    as _PB_SHORT_CHG24_MIN,
        PULLBACK_SHORT_VOL_RATIO_MIN as _PB_SHORT_VOL_RATIO_MIN,
    )
except Exception as _cfg_e:
    _PB_SHORT_CHG24_MIN    = 3.0
    _PB_SHORT_VOL_RATIO_MIN = 0.8
    _cfg_fallback("PULLBACK_SHORT", _cfg_e, {"chg24_min": _PB_SHORT_CHG24_MIN, "vol_ratio_min": _PB_SHORT_VOL_RATIO_MIN})
import time
from typing import Any

logger = logging.getLogger("fx.judges")

# 资金费率速度缓存:symbol -> (result_dict, expire_ts)
_fr_velocity_cache: dict[str, tuple[dict, float]] = {}
_FR_CACHE_TTL = 60  # 秒:同标的 60s 内不重复 HTTP 请求

# ── pandas_ta 已移除,始终使用内置实现 ──────────────────────────────────────
_USE_PTA = False


# ── 内置指标(pandas_ta 不可用时的 fallback) ──────────────────────────────────
# ── 公共工具 ──────────────────────────────────────────────────────────────────
def _closes(klines: list) -> list:
    return [float(k[4]) for k in klines]

def _opens(klines: list) -> list:
    return [float(k[1]) for k in klines]

def _highs(klines: list) -> list:
    return [float(k[2]) for k in klines]

def _lows(klines: list) -> list:
    return [float(k[3]) for k in klines]

def _vols(klines: list) -> list:
    return [float(k[5]) for k in klines]

def _vol_ma(klines: list, n: int = 20) -> float:
    vols = _vols(klines)
    if not vols: return 0.0
    return sum(vols[-n:]) / min(n, len(vols))

def _atr(klines: list) -> float:
    """统一用 1H ATR14(直接调用 shared/indicators)"""
    return _atr_raw(klines)

def _is_finite_num(x) -> bool:
    """[v3.89 #30] 是有限数值（非 NaN / Infinity / 非数字类型）。"""
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False

def _volatility_tier(atr: float, price: float) -> str:
    """v3.14: mid/high 阈值从 8% 降到 6%，避免 7-8% ATR 标的被误判为 mid
    [v3.89 #30] ATR 或价格不是有限数值时返回 "invalid"（数据错误标记），不再落入 low/mid/high 的正常分支。
    分层数值不变。"""
    if not (_is_finite_num(atr) and _is_finite_num(price)):
        logger.warning(f"[judges] _volatility_tier 输入非有限数值 atr={atr!r} price={price!r}，返回 invalid")
        return "invalid"
    if price <= 0: return "mid"
    r = atr / price * 100
    if r < 3:  return "low"
    if r <= 6: return "mid"   # v3.14: 原 <=8，高波动标的恢复期 ATR 更快被识别为 high
    return "high"

def _atr_ratio(atr: float, price: float) -> float:
    # [v3.89 #30] 非有限输入返回 NaN（明确的无效值，不把 Infinity 伪装成大波动比例），并记录
    if not (_is_finite_num(atr) and _is_finite_num(price)):
        logger.warning(f"[judges] _atr_ratio 输入非有限数值 atr={atr!r} price={price!r}，返回 NaN")
        return float("nan")
    return atr / price * 100 if price > 0 else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# judge_pullback - 结构回踩型
# ══════════════════════════════════════════════════════════════════════════════
def _judge_error_result(judge_name: str, mode: str, e: BaseException) -> tuple:
    """
    [v3.89 #26] 七个 judge 外层异常包装的统一返回契约：
      reject_reason  —— 文案与旧版一致（"内部异常: {e}"），下游按文案归类的统计不受影响
      mode           —— 路线名（旧版前四个路线缺失，现统一带上）
      internal_error —— True：与「正常拒绝」区分，统计可据此把程序错误从拒绝漏斗里剥离
      error_type     —— 异常类名
    只改异常路径，任何正常拒绝条件与阈值不变。
    """
    return False, {
        "reject_reason": f"内部异常: {e}",
        "mode": mode,
        "internal_error": True,
        "error_type": type(e).__name__,
    }



_JUDGE_ERR_LAST: dict = {}
_JUDGE_ERR_INTERVAL = 60.0   # 同一路线同一类异常的完整堆栈最小间隔（秒）


def _log_judge_failure(judge_name: str, e: BaseException, k1h=None, k4h=None, k15=None, ind=None) -> None:
    """
    [v3.89 #27] 判定异常日志：带标的与 K 线长度等定位上下文；完整堆栈按 (路线, 异常类型) 限频，
    其余只记一行摘要。不记录整个行情载荷。
    """
    import time as _t
    import traceback as _tb
    def _ln(x):
        try:
            return len(x)
        except Exception:
            return None
    symbol = ind.get("symbol", "?") if isinstance(ind, dict) else "?"
    ctx = f"symbol={symbol} k1h={_ln(k1h)} k4h={_ln(k4h)} k15={_ln(k15)}"
    key = (judge_name, type(e).__name__)
    now = _t.time()
    if now - _JUDGE_ERR_LAST.get(key, 0) >= _JUDGE_ERR_INTERVAL:
        _JUDGE_ERR_LAST[key] = now
        logger.warning(f"[{judge_name}] 异常 {type(e).__name__}: {e} | {ctx}\n{''.join(_tb.format_exception(type(e), e, e.__traceback__))}")
    else:
        logger.warning(f"[{judge_name}] 异常 {type(e).__name__}: {e} | {ctx}（堆栈限频，{_JUDGE_ERR_INTERVAL:.0f}s 内同类异常不重复）")



def _liq_volume_adj(trigger: float, baseline: float, mult: float = 2.5) -> int:
    """[opt-4] 强平量软加减分：基准>1000 时达标 +10、不足 -5；基准不足返回 0。"""
    if baseline > 1000:
        return 10 if trigger >= baseline * mult else -5
    return 0


def _btc_crash_reject(ind: dict, direction: str, strategy_name: str, out: dict):
    """[opt-5] BTC 1H 跌幅 <= -5% 时禁止 LONG；返回 (False, out) 或 None。"""
    if direction == "LONG":
        btc_1h_drop = float(ind.get("btc_1h_drop_pct", 0.0) or 0.0)
        if btc_1h_drop <= -5.0:
            out["reject_reason"] = (
                f"BTC急跌保护：BTC 1H跌幅={btc_1h_drop:.1f}%，"
                f"黑天鹅行情下禁止{strategy_name} LONG"
            )
            return False, out
    return None


def judge_pullback(k1h: list, k4h: list, k15: list, ind: dict,
                   chg24: float, funding: float) -> tuple[bool, dict]:
    """
    结构回踩型信号判断。
    k1h / k4h / k15 均为已闭合 K 线(末尾未闭合已去除)。
    """
    try:
        return _judge_pullback_impl(k1h, k4h, k15, ind, chg24, funding)
    except Exception as e:
        _log_judge_failure("judge_pullback", e, k1h, k4h, k15, ind)
        return _judge_error_result("judge_pullback", "pullback", e)


def _judge_pullback_impl(k1h, k4h, k15, ind, chg24, funding):
    # v3.3.6: 流动性门槛——vol24 < 1M 的极低流动性标的直接拒绝，防止滑点/操控风险
    _vol24 = ind.get("vol24_usd", 0)
    if _vol24 > 0 and _vol24 < 1_000_000:
        return False, {"reject_reason": f"流动性过低: vol24={_vol24/1e6:.1f}M<1M", "mode": "pullback"}
    price   = ind["price"]
    atr     = ind["atr_1h"]
    atr_r   = _atr_ratio(atr, price)
    tier    = _volatility_tier(atr, price)
    ema20_1h = ind["ema20_1h"]
    ema50_1h = ind["ema50_1h"]
    ema20_4h = ind.get("ema20_4h", 0)
    ema50_4h = ind.get("ema50_4h", 0)
    rsi_1h   = ind["rsi_1h"]

    out = {
        "mode":              "pullback",
        "volatility_tier":   tier,
        "atr_ratio":         round(atr_r, 4),
        "ema_structure":     None,
        "ema_direction":     None,
        "price_in_zone":     False,
        "reversal_signal":   None,
        "volume_shrink":     False,
        "liq_zone_valid":    False,
        "liq_zone_distance_atr": None,
        "liq_zone_amount":   None,
        "entry_mode":        "immediate",
        "signal_strength":   "medium",
        "position_size_cap": 0.5,
        "extreme_candle_flag": False,
        "funding_rate":      funding,
        "reject_reason":     None,
    }

    # ── 极端K线标记(三层通用) ───────────────────────────────────────────────
    extreme_flag = any(
        abs(float(k[4]) - float(k[1])) / (float(k[1]) + 1e-9) > 0.20
        for k in k1h[-20:] if float(k[1]) > 0
    )
    out["extreme_candle_flag"] = extreme_flag

    # ── 共同拒绝:BTC 15分钟剧烈波动 ─────────────────────────────────────────
    # (scanner 层面已检查,此处保留兜底)

    # 问题四修复:pullback 方向判断优先看 EMA,而非 chg24
    # 原逻辑用 chg24>=0 OR ema多头 判定 LONG,导致空头 EMA 下 chg24>=0 的币种
    # 被错误分到 LONG pullback 路径,然后在条件一再被 EMA 检查拒绝,无效计算
    direction = "LONG" if ema20_1h >= ema50_1h else "SHORT"

    # ── RSI 极端光速拒绝(入库第一道)───────────────────────────────
    # pullback 假设趋势正常回踩,RSI 超卖或超买属于异常波动,应走 reversal 路径
    # v3.6 门槛对齐：与 _is_reversal 分类层保持一致（28/72），消除 28~29/70~71 死区
    if direction == "LONG" and rsi_1h < 28:
        out["reject_reason"] = f"RSI1H={rsi_1h:.1f}<28,超卖状态应走reversal路径而非pullback"
        return False, out
    if direction == "SHORT" and rsi_1h > 72:
        out["reject_reason"] = f"RSI1H={rsi_1h:.1f}>72,超买状态应走reversal路径而非pullback"
        return False, out

    # ── [Fix 1 修订版] 峰値回撤保护（v3.48）───────────────────────
    # 原方案用 closes[-480] 做基准，短期反弹会重置基准价格，被反弹欺骗
    # 新方案：用 60日内最高点做基准，不受反弹影响
    if direction == "LONG" and len(k1h) >= 240:
        _closes_1h = [float(k[4]) for k in k1h]
        _lookback   = min(1440, len(_closes_1h) - 1)  # 1440根1H = 60天
        _price_now  = _closes_1h[-1]
        _peak_price = max(_closes_1h[-_lookback:])
        if _peak_price > 0:
            _drawdown = (_price_now - _peak_price) / _peak_price
            if _drawdown < -0.40:  # 距峰値跌超 40%
                out["reject_reason"] = (
                    f"峰値回撤保护：近{_lookback}根1H最高={_peak_price:.6g}，"
                    f"当前={_price_now:.6g}，"
                    f"回撤={_drawdown*100:.1f}%<-40%，"
                    f"禁止 pullback LONG（结构性熊市，反弹不改变趋势）"
                )
                return False, out

    # ── [Fix 3 修订版] 60日价格分位保护（v3.48）───────────────────
    # 原版：当前价是否N日新低（被反弹欺骗）
    # 新版：当前价处于60日价格区间的低位分位时，禁止 pullback LONG
    if direction == "LONG" and len(k1h) >= 240:
        _c1h_f3     = [float(k[4]) for k in k1h]
        _lk2        = min(1440, len(_c1h_f3) - 1)
        _range_high = max(_c1h_f3[-_lk2:])
        _range_low  = min(_c1h_f3[-_lk2:])
        _range_span = _range_high - _range_low
        if _range_span > 0:
            _percentile = (_c1h_f3[-1] - _range_low) / _range_span
            if _percentile < 0.30:  # 处于60日区间低 30% 分位
                out["reject_reason"] = (
                    f"价格分位保护：当前价处于60日区间 {_percentile*100:.1f}% 分位"
                    f"（高={_range_high:.6g} 低={_range_low:.6g} 当前={_c1h_f3[-1]:.6g}），"
                    f"禁止 pullback LONG"
                )
                return False, out

    # ── v3.46 动态波动率分档（替代固定门槛）───────────────────────────────
    from config import ATR_TIER_HIGH_MIN, ATR_TIER_MID_MIN, ATR_TIER_HIGH_WEEKEND, ATR_TIER_MID_WEEKEND
    _weekend_pb = ind.get("_weekend_mode", False)
    if atr_r >= ATR_TIER_HIGH_MIN:
        _vol_tier_pb = "high"
        _atr_min_pb  = ATR_TIER_HIGH_WEEKEND if _weekend_pb else ATR_TIER_HIGH_MIN
    elif atr_r >= ATR_TIER_MID_MIN:
        _vol_tier_pb = "mid_vol"
        _atr_min_pb  = ATR_TIER_MID_WEEKEND if _weekend_pb else ATR_TIER_MID_MIN
    else:
        out["reject_reason"] = f"波动率过低:ATR_ratio={atr_r:.2f}%<{ATR_TIER_MID_MIN}%，不适合当前策略"
        return False, out
    if atr_r < _atr_min_pb:
        out["reject_reason"] = f"波动率过低({_vol_tier_pb}档):ATR_ratio={atr_r:.2f}% < {_atr_min_pb}%"
        return False, out
    ind["_vol_tier"] = _vol_tier_pb  # 注入档位供评分层使用

    # ── v3.59 全方向量能硬性门槛（Pullback LONG/SHORT 都适用）──────────────────
    # GPT-5 与 Opus 4.5 均指出：实盘中 vol_ratio<0.1 的标的大量进场导致 early_stagnation
    # vol_ratio < 0.4 说明市场几乎无人参与，不是横盘震荡就是即将结束的回踩，直接拒绝
    _pb_vol_ratio_min = 0.40
    _pb_vol = ind.get("vol_ratio_closed", ind.get("vol_ratio", 1.0))  # [opt-1] 已收盘 1H 量比，缺失回退
    if _pb_vol < _pb_vol_ratio_min:
        out["reject_reason"] = (
            f"pullback 量能门槛：vol_ratio={_pb_vol:.2f} < {_pb_vol_ratio_min}，"
            f"市场参与度不足，非标准回踩结构"
        )
        return False, out

    # ─────────────────────────────────────────────────────────────────────────
    # 低 / 中波动层
    # ─────────────────────────────────────────────────────────────────────────
    if tier in ("low", "mid"):

        # 条件零-补：脉冲大阳保护（仅 SHORT，拦截庄家拉盘后追空）
        if direction == "SHORT":
            _pulse = _is_pulse_candle(k1h, k4h)
            if _pulse:
                out["reject_reason"] = f"脉冲大阳保护，拒绝 pullback SHORT: {_pulse}"
                return False, out

        # 条件零-补：pullback SHORT 弱信号过滤（v3.19）
        # chg24 过小说明标的未经历明显拉升，不是标准的 pullback 结构
        # vol_ratio 过小说明市场没有兴趣，横盘摆动而非单向趋势回踩
        if direction == "SHORT":
            _vol_ratio = ind.get("vol_ratio", ind.get("vol_ratio_1h", 1.0))
            # v3.46: 中波动档（主流币）涨 2% 就是有意义的涨幅，高波动档保持 _PB_SHORT_CHG24_MIN
            from config import ATR_TIER_MID_PULLBACK_CHG24
            _pb_short_chg24_thresh = (
                5.0 if ind.get("_weekend_mode") else
                (ATR_TIER_MID_PULLBACK_CHG24 if ind.get("_vol_tier") == "mid_vol" else _PB_SHORT_CHG24_MIN)
            )
            if chg24 < _pb_short_chg24_thresh:
                out["reject_reason"] = (
                    f"pullback SHORT 弱信号：chg24={chg24:.1f}%<{_pb_short_chg24_thresh}%，"
                    f"标的未有足够涨幅，非标准回踩空结构"
                )
                return False, out
            if _vol_ratio < _PB_SHORT_VOL_RATIO_MIN:
                out["reject_reason"] = (
                    f"pullback SHORT 弱信号：vol_ratio={_vol_ratio:.2f}<{_PB_SHORT_VOL_RATIO_MIN}，"
                    f"量能不足证明市场对回踩无兴趣"
                )
                return False, out
        ema_4h_diff_pct = abs(ema20_4h - ema50_4h) / (ema50_4h + 1e-9) * 100 if ema50_4h else 999
        ema_4h_neutral  = ema_4h_diff_pct <= 1.5  # v3.6: 1.0→1.5，山寨币 EMA 差距较大，1% 太窄误拒 weak 路径

        if direction == "LONG":
            if ema20_4h > 0 and ema20_4h < ema50_4h and not ema_4h_neutral:
                out["reject_reason"] = "4H空头结构,禁止做多"
                return False, out
            if ema20_1h > ema50_1h and ema20_4h > ema50_4h:
                out["ema_structure"] = "strong"; out["ema_direction"] = "bullish"
            elif ema20_1h > ema50_1h and ema_4h_neutral:
                out["ema_structure"] = "weak";   out["ema_direction"] = "bullish"
            else:
                out["reject_reason"] = "1H EMA不满足多头结构"
                return False, out
        else:  # SHORT
            if ema20_4h > 0 and ema20_4h > ema50_4h and not ema_4h_neutral:
                out["reject_reason"] = "4H多头结构,禁止做空"
                return False, out
            if ema20_1h < ema50_1h and ema20_4h < ema50_4h:
                out["ema_structure"] = "strong"; out["ema_direction"] = "bearish"
            elif ema20_1h < ema50_1h and ema_4h_neutral:
                out["ema_structure"] = "weak";   out["ema_direction"] = "bearish"
            else:
                out["reject_reason"] = "1H EMA不满足空头结构"
                return False, out

        # 条件二-补:反弹/回调幅度过大保护（v3.16）
        # 反弹超过 15% 追空 / 回调超过 15% 追多，都不是正常趋势内回踩，拒绝
        _rebound_limit = 15.0
        if direction == "SHORT" and len(k1h) >= 20:
            _recent_low_20 = min(float(k[3]) for k in k1h[-20:])
            if _recent_low_20 > 0:
                _rebound_pct = (price - _recent_low_20) / _recent_low_20 * 100
                if _rebound_pct > _rebound_limit:
                    out["reject_reason"] = (
                        f"从近20根1H低点{_recent_low_20:.4g}反弹{_rebound_pct:.1f}%"
                        f">{_rebound_limit}%，反弹幅度过大，非空头回踩"
                    )
                    return False, out
        elif direction == "LONG" and len(k1h) >= 20:
            _recent_high_20 = max(float(k[2]) for k in k1h[-20:])
            if _recent_high_20 > 0:
                _pullback_pct = (_recent_high_20 - price) / _recent_high_20 * 100
                if _pullback_pct > _rebound_limit:
                    out["reject_reason"] = (
                        f"从近20根1H高点{_recent_high_20:.4g}回调{_pullback_pct:.1f}%"
                        f">{_rebound_limit}%，回调幅度过大，非多头回踩"
                    )
                    return False, out

        # 条件二:价格在回踩区间(硬性)
        # 回踩区间从 config 读取(PULLBACK_LOWER_LIMIT_LOW/MID),与参数中心一致
        # [v3.87 STEP3] 删除未使用的 sl_mult（下边界已固定为 1.0 ATR，与止损系数解耦）
        # v3.6: 回踩区间下边界固定 1.0 ATR，与止损系数解耦
        # sl_mult 用于止损计算，回踩区间若也用 sl_mult（mid=2.0），会把破位的标的也放进来
        _zone_lower_mult = 1.0  # 固定下边界，不随 tier 变化
        if direction == "LONG":
            upper = ema20_1h + atr * 0.3
            lower = ema20_1h - atr * _zone_lower_mult
            in_zone = lower <= price <= upper
        else:
            lower = ema20_1h - atr * 0.3
            upper = ema20_1h + atr * _zone_lower_mult
            in_zone = lower <= price <= upper

        out["price_in_zone"] = in_zone
        if not in_zone:
            out["reject_reason"] = f"价格{price:.4g}不在回踩区间[{lower:.4g},{upper:.4g}]"
            return False, out

        # 条件二-补:近期高点距离检查(确保有过上涨再回踩,而非贴着EMA走)
        # 取最近 8 根 1H 最高价,要求最高点比当前价至少高 1.5%(多单)
        # 或最低点比当前价低 1.5%(空单),否则只是横盘贴着 EMA,不是回踩
        # v3.6: 窗口 8→12 根（12H 覆盖日线级别慢回踩），门槛 1.5% 固定→0.5 ATR 自适应
        _gap_min = atr * 0.5
        if direction == "LONG":
            recent_high = max(float(k[2]) for k in k1h[-12:]) if len(k1h) >= 12 else price
            gap_abs = recent_high - price
            if gap_abs < _gap_min:
                out["reject_reason"] = f"近期高点(12根){recent_high:.4g}距当前价{gap_abs:.4g}<0.5ATR({_gap_min:.4g}),无真实回踩空间"
                return False, out
        else:
            recent_low = min(float(k[3]) for k in k1h[-12:]) if len(k1h) >= 12 else price
            gap_abs = price - recent_low
            if gap_abs < _gap_min:
                out["reject_reason"] = f"近期低点(12根){recent_low:.4g}距当前价{gap_abs:.4g}<0.5ATR({_gap_min:.4g}),无真实反弹空间"
                return False, out

        # 条件三-补:1H价格方向确认(防止价格仍在快速下行中过早入场)
        # 要求:最近2根已闭合1H K线满足--
        #   多单:最后一根不创新低(low >= 前一根low),且至少1根收红
        #   空单:最后一根不创新高(high <= 前一根high),且至少1根收阴
        # v3.6: 1H方向确认增强：近3根已闭合K线中至少2根不创新低/高，防止单根反弹误判企稳
        if len(k1h) >= 4:
            _k3 = k1h[-4:-1]   # 最近3根已闭合（去掉末尾未闭合）
            if direction == "LONG":
                not_new_low_cnt = sum(
                    1 for i in range(1, len(_k3))
                    if float(_k3[i][3]) >= float(_k3[i-1][3])
                )
                # v3.21 动能确认：近3根中至少1根阳线（确认下跌动能减弱，非下跌途中）
                any_green = any(float(k[4]) >= float(k[1]) for k in _k3)
                if not (not_new_low_cnt >= 2 and any_green):
                    out["reject_reason"] = (
                        f"1H价格方向未确认:近3根中{not_new_low_cnt}根不创新低(<2),或近3根无收红(动能确认)"
                    )
                    return False, out
            else:  # SHORT
                not_new_high_cnt = sum(
                    1 for i in range(1, len(_k3))
                    if float(_k3[i][2]) <= float(_k3[i-1][2])
                )
                # v3.21 动能确认：近3根中至少1根阴线（确认上涨动能减弱）
                any_red = any(float(k[4]) <= float(k[1]) for k in _k3)
                if not (not_new_high_cnt >= 2 and any_red):
                    out["reject_reason"] = (
                        f"1H价格方向未确认:近3根中{not_new_high_cnt}根不创新高(<2),或近3根无收阴(动能确认)"
                    )
                    return False, out
        elif len(k1h) >= 2:
            # 数据不足4根时退化到原逻辑
            k_prev, k_last = k1h[-2], k1h[-1]
            if direction == "LONG":
                if not (float(k_last[3]) >= float(k_prev[3])):
                    out["reject_reason"] = f"1H价格方向未确认(数据不足):最后一根创新低"
                    return False, out
            else:
                if not (float(k_last[2]) <= float(k_prev[2])):
                    out["reject_reason"] = f"1H价格方向未确认(数据不足):最后一根创新高"
                    return False, out

        # 条件三:止跌 / 止涨信号(硬性,15M,至少一种)
        rev_sig = _reversal_signal_15m(k15, direction)
        out["reversal_signal"] = rev_sig
        if rev_sig is None:
            out["reject_reason"] = "无有效15M止跌/止涨信号"
            return False, out

        # 缩量辅助(加分项)
        out["volume_shrink"] = _volume_shrink_15m(k15)

        # 条件三-补:前高量检查(硬性)
        # 如果缩量是发生在近4根内有异常放量(量比>2.5)之后,
        # 说明刚经历过主力拉升/砸盘,缩量是消化而非抛压耗尽,不可信。
        # 只有当前量比确实极低(<0.25)时才触发此检查,避免误伤正常缩量。
        vol_ratio = ind.get("vol_ratio", 1.0)
        if vol_ratio < 0.25 and len(k1h) >= 6:
            closed_k1h = k1h[:-1]  # 已闭合K线
            if len(closed_k1h) >= 5:
                vol_ma_ref = sum(float(k[5]) for k in closed_k1h[-20:]) / min(len(closed_k1h), 20)
                recent_ratios = [
                    float(k[5]) / (vol_ma_ref + 1e-9)
                    for k in closed_k1h[-5:-1]  # 入场前4根(不含当前)
                ]
                surge_count = sum(1 for r in recent_ratios if r > 2.5)
                if surge_count >= 1:
                    out["reject_reason"] = (
                        f"缩量({vol_ratio:.2f})发生在近4根有{surge_count}根放量(>2.5x)之后,"
                        f"可能是主力消化而非抛压耗尽,跳过"
                    )
                    return False, out

        # 条件四:Coinglass清算区(软性)
        _fill_liq_zone(out, price, direction, atr, symbol_from_ind=ind)

    # ─────────────────────────────────────────────────────────────────────────
    # 高波动层
    # ─────────────────────────────────────────────────────────────────────────
    else:
        # 条件零-补：脉冲大阳保护（仅 SHORT，high tier 同样适用）
        if direction == "SHORT":
            _pulse_h = _is_pulse_candle(k1h, k4h)
            if _pulse_h:
                out["reject_reason"] = f"脉冲大阳保护，拒绝 high-tier pullback SHORT: {_pulse_h}"
                return False, out

            # 条件零-补2：high tier pullback SHORT 同样要求 chg24 > 阈值（方向判断，非 abs）
            # v3.78 fix: low/mid tier 有此过滤，high tier 遗漏，导致「跌中反弹」被误判为 pullback SHORT
            # pullback SHORT 核心逻辑：「标的先涨，回踩 EMA20 后做空」——chg24 为负说明标的根本没涨过
            _pb_chg24_thresh_h = _PB_SHORT_CHG24_MIN  # 同 low/mid 使用同一参数 3.0%
            if chg24 < _pb_chg24_thresh_h:
                out["reject_reason"] = (
                    f"high tier pullback SHORT 弱信号：chg24={chg24:.1f}%<{_pb_chg24_thresh_h}%，"
                    f"标的未有足够涨幅（当前为下跌/横盘），非标准回踩空结构"
                )
                return False, out

        # 条件零:4H EMA 逆势硬拦截（v3.16）
        # high tier 波动大、风险高，逆大周期趋势做单后果更严重
        # SHORT：4H 明确多头（gap>5%）→ 拒绝；LONG：4H 明确空头（gap>5%）→ 拒绝
        _4h_ema_gap_high = (ema20_4h - ema50_4h) / (ema50_4h + 1e-9) * 100 if ema50_4h else 0
        _4h_counter_threshold = 5.0
        if direction == "SHORT" and _4h_ema_gap_high > _4h_counter_threshold:
            out["reject_reason"] = (
                f"high tier 4H多头排列(EMA20={ema20_4h:.4g}>EMA50={ema50_4h:.4g}"
                f" gap={_4h_ema_gap_high:.1f}%>{_4h_counter_threshold}%)，高波动逆势做空拒绝"
            )
            return False, out
        if direction == "LONG" and _4h_ema_gap_high < -_4h_counter_threshold:
            out["reject_reason"] = (
                f"high tier 4H空头排列(EMA20={ema20_4h:.4g}<EMA50={ema50_4h:.4g}"
                f" gap={_4h_ema_gap_high:.1f}%<-{_4h_counter_threshold}%)，高波动逆势做多拒绝"
            )
            return False, out

        # EMA:参考加分,不拒绝
        if direction == "LONG":
            out["ema_structure"] = "ref_only"
            out["ema_direction"] = "bullish" if ema20_1h > ema50_1h else "bearish"
        else:
            out["ema_structure"] = "ref_only"
            out["ema_direction"] = "bearish" if ema20_1h < ema50_1h else "bullish"

        # 条件二:成交密集区(硬性)
        dense_low, dense_high = _vwap_dense_zone(k1h)
        margin = atr * 0.5
        # LONG：价格在密集区下方或区内（等待反弹）
        # SHORT：价格在密集区上方或区内（等待回落）
        # 两个方向的区间应有差异化，防止方向判断无意义
        if direction == "LONG":
            in_zone = (dense_low - margin) <= price <= (dense_high + margin)
        else:
            in_zone = (dense_low - margin) <= price <= (dense_high + margin)
        # 注：高波动层成交密集区是支撑/压力的核心区域，做多/做空都在密集区附近入场
        # 差异通过 EMA 方向 + 资金费率条件已经隐式区分了方向
        out["price_in_zone"] = in_zone
        if not in_zone:
            out["reject_reason"] = f"价格{price:.4g}不在成交密集区[{dense_low:.4g},{dense_high:.4g}]±{margin:.4g}"
            return False, out

        # 条件三:资金费率(硬性)
        if direction == "LONG":
            if funding > 0.001:        # > +0.1%
                out["reject_reason"] = f"高波动层多头:资金费率过高{funding*100:.4f}%"
                return False, out
        else:
            if funding < -0.001:       # < -0.1%
                out["reject_reason"] = f"高波动层空头:资金费率过低{funding*100:.4f}%"
                return False, out

        # 条件四:1H止跌止涨信号(硬性,周期提升到1H)
        rev_sig = _reversal_signal_1h(k1h, direction)
        out["reversal_signal"] = rev_sig
        if rev_sig is None:
            out["reject_reason"] = "无有效1H止跌/止涨信号(高波动层)"
            return False, out

        # 条件五:Coinglass(软性,范围更宽 0.5~3.0 ATR)
        _fill_liq_zone(out, price, direction, atr,
                       dist_min=0.5, dist_max=3.0,
                       liq_threshold_pending=2_000_000,
                       symbol_from_ind=ind)
        # 清算区密集 → 挂单守候模式
        if (out.get("liq_zone_amount") or 0) >= 2_000_000 and out.get("liq_zone_valid"):
            out["entry_mode"] = "pending_order"

    # ── 信号强度分级 ──────────────────────────────────────────────────────────
    # v3.6: ema_structure=weak 时强度上限为 medium，不能被缩量+清算区拉到 strong
    # 避免「4H中性仓位 0.3」却被评为 strong 的矛盾
    strong_count = 0
    if out["ema_structure"] == "strong": strong_count += 1
    if out.get("volume_shrink"):         strong_count += 1
    if out.get("liq_zone_valid"):        strong_count += 1
    if strong_count >= 2 and out["ema_structure"] != "weak":
        out["signal_strength"] = "strong"
    else:
        # 高波动层的 ema_structure = "ref_only",原逻辑会给 "weak" 直接拒绝。
        # 高波动层已经过了成交密集区/资金费率/1H反转信号三道硬性门槛,
        # 应至少给 "medium",门槛不应比低中波动层更严。
        out["signal_strength"] = "medium" if out["ema_structure"] in ("strong","weak","ref_only") else "weak"

    if out["signal_strength"] == "weak":
        out["reject_reason"] = "信号强度弱,跳过"
        return False, out

    # ── 仓位系数(position_size_cap) ────────────────────────────────────────
    # strong + 双周期对齐 → 0.75(最高仓位)
    # medium + 4H 强结构 → 0.5(正常仓位)
    # medium + 4H 中性(weak EMA结构)→ 0.3(弱信号降级仓位)
    if out["signal_strength"] == "strong":
        out["position_size_cap"] = 0.75
    elif out["ema_structure"] == "weak":   # medium 且 4H 中性
        out["position_size_cap"] = 0.3
    else:                                  # medium 且 4H 强
        out["position_size_cap"] = 0.5


    # -- v3.0: 四层标的分类乘数 --
    try:
        import sys as _sys, pathlib as _pl
        _sys.path.insert(0, str(_pl.Path(__file__).parent.parent.parent))
        from calc_position_size_real import classify_tier as _ct, _is_stock_token as _ist, _us_market_open as _umo
        _atr   = ind.get("atr_1h", 0)
        _price = ind.get("price", 1)
        _vol24 = ind.get("vol24_usd", 0)
        _sym   = ind.get("symbol", "")
        _tier  = _ct(_atr, _price, _vol24, _sym)
        _tmult = {"L1": 2.5, "L2": 1.8, "L3": 1.0, "L4": 0.6}.get(_tier, 1.0)
        if _ist(_sym) and not _umo():
            _tmult *= 0.5
        out["position_size_cap"] = round(out.get("position_size_cap", 0.5) * _tmult, 4)
        out["volatility_tier"]   = _tier
    except Exception:
        pass

    # ── 全局顶部放量出货检测（所有 tier 均生效，仅 LONG pullback）─────────────
    # 场景：主升浪后回落段最大单根量 > 上涨段平均量 * 1.5倍
    #       且近10根4H内整体涨幅 >= 15%（确认是脉冲主升浪，非横盘单根大阴）
    # 含义：大量筹码高位换手，是出货而非健康回调，不适合 pullback 多
    # 复盘 AKE：10-15 15:59 跌幅-2.18% 但量能1817M > 涨停1364M，典型出货
    if direction == "LONG" and len(k4h) >= 8:
        _rise_k = [k for k in k4h[-10:] if float(k[4]) >= float(k[1])]
        _drop_k = [k for k in k4h[-10:] if float(k[4]) < float(k[1])]
        if len(_rise_k) >= 2 and _drop_k:
            _avg_rise_vol  = sum(float(k[5]) for k in _rise_k) / len(_rise_k)
            _max_drop_vol  = max(float(k[5]) for k in _drop_k)
            _TOP_VOL_RATIO = 1.5
            if _max_drop_vol > _avg_rise_vol * _TOP_VOL_RATIO:
                _p_start = float(k4h[-10][1]) if len(k4h) >= 10 else float(k4h[0][1])
                _p_peak  = max(float(k[4]) for k in k4h[-10:])
                _rise_total_pct = (_p_peak - _p_start) / (_p_start + 1e-9) * 100
                if _rise_total_pct >= 15.0:
                    out["reject_reason"] = (
                        f"顶部放量出货：回落段最大量{_max_drop_vol/1e6:.0f}M"
                        f" > 上涨均量{_avg_rise_vol/1e6:.0f}M 的{_TOP_VOL_RATIO}x"
                        f"，近10根涨幅{_rise_total_pct:.0f}%，脉冲后高位出货，不适合 pullback 多"
                    )
                    return False, out

    return True, out


# ── 15M 止跌 / 止涨信号 ───────────────────────────────────────────────────────
# ── 脉冲大阳保护（pulse candle）──────────────────────────────────────────────────
def _is_pulse_candle(k1h: list, k4h: list) -> str | None:
    """检测最近 K 线中是否存在"脉冲大阳"（爆量+大实体涨幅）。

    用于拦截庄家拉盘/流动性猎杀很后实现的 pullback/reversal SHORT 信号。
    同时检查 1H（窗口 LOOKBACK_1H 根）和 4H（窗口 LOOKBACK_4H 根），任意一个触发即返回描述字符串。

    触发条件：
      1H: 近 _PC_LB_1H 根闭合K线中，任意一根满足：
          vol > _PC_VOL_1H 倍均量 且 实体涨幅 > _PC_BODY_1H% 且 实体/振幅 > _PC_BODY_RATIO
      4H: 近 _PC_LB_4H 根闭合K线中，任意一根满足：
          vol > _PC_VOL_4H 倍均量 且 实体涨幅 > _PC_BODY_4H% 且 实体/振幅 > _PC_BODY_RATIO

    返回值：None = 无脉冲K线；字符串 = 描述（用于日志和 reject_reason）
    """
    def _check(klines: list, lookback: int, vol_mult: float, body_chg_min: float, tf: str) -> str | None:
        if len(klines) < lookback + 5:
            return None
        recent  = klines[-(lookback + 1):-1]        # 最近 lookback 根已闭合
        avg_vol = sum(float(k[5]) for k in klines[-(lookback + 6):-(lookback + 1)]) / 5
        if avg_vol < 1e-9:
            return None
        for k in recent:
            o   = float(k[1]); h = float(k[2]); lo = float(k[3]); c = float(k[4])
            vol = float(k[5])
            body       = c - o                          # 正=阳线
            body_abs   = abs(body)
            candle_rng = h - lo + 1e-9
            body_chg   = body / (o + 1e-9) * 100       # 实体涨幅%
            body_ratio = body_abs / candle_rng
            vol_ratio  = vol / avg_vol
            if vol_ratio > vol_mult and body_chg > body_chg_min and body_ratio > _PC_BODY_RATIO:
                return (f"脉冲大阳{tf}: vol={vol_ratio:.1f}x均量 "
                        f"实体涨{body_chg:.1f}% 实体占振幅{body_ratio:.0%}")
        return None

    r1h = _check(k1h, _PC_LB_1H, _PC_VOL_1H, _PC_BODY_1H, "1H")
    if r1h:
        return r1h
    return _check(k4h, _PC_LB_4H, _PC_VOL_4H, _PC_BODY_4H, "4H")


def _reversal_signal_15m(k15: list, direction: str) -> str | None:
    """在最近已闭合3根15M K线里找至少一种止跌/止涨信号"""
    if len(k15) < 3: return None
    recent = k15[-3:]

    for i, k in enumerate(recent):
        o = float(k[1]); h = float(k[2]); lo = float(k[3]); c = float(k[4])
        body = abs(c - o)
        upper_shadow = h - max(o, c)
        lower_shadow = min(o, c) - lo

        if direction == "LONG":
            # 锤头:下影线 > 实体2倍,实体在上半部分
            if body > 0 and lower_shadow > body * 2 and c > (h + lo) / 2:
                return "hammer"
            # 阳包阴
            if i > 0:
                pc = float(recent[i-1][4]); po = float(recent[i-1][1])
                if c > po and o < pc:
                    return "engulfing"
            # two_candles 已移除：连续双阳出现频率过高，不具备有效止跌意义
        else:  # SHORT
            # 射击之星:上影线 > 实体2倍,实体在下半部分
            if body > 0 and upper_shadow > body * 2 and c < (h + lo) / 2:
                return "shooting_star"
            # 阴包阳
            if i > 0:
                pc = float(recent[i-1][4]); po = float(recent[i-1][1])
                if c < po and o > pc:
                    return "engulfing_bear"
            # two_bear_candles 已移除：连续双阴出现频率过高，不具备有效止涨意义
    return None


def _reversal_signal_1h(k1h: list, direction: str) -> str | None:
    """在最近已闭合2根1H K线里找止跌/止涨信号(高波动层)"""
    if len(k1h) < 2: return None
    recent = k1h[-2:]

    for i, k in enumerate(recent):
        o = float(k[1]); h = float(k[2]); lo = float(k[3]); c = float(k[4])
        body = abs(c - o) + 1e-9
        upper_shadow = h - max(o, c)
        lower_shadow = min(o, c) - lo

        if direction == "LONG":
            if lower_shadow > body * 3:                      return "long_lower_shadow"
            if i > 0:
                pc = float(recent[i-1][4]); po = float(recent[i-1][1])
                if c > po and o < pc:                        return "engulfing_1h"
            if i > 0 and abs(float(k[3])-float(k[4])) < abs(float(recent[i-1][3])-float(recent[i-1][4])):
                return "decel_1h"
        else:
            if upper_shadow > body * 3:                      return "long_upper_shadow"
            if i > 0:
                pc = float(recent[i-1][4]); po = float(recent[i-1][1])
                if c < po and o > pc:                        return "engulfing_bear_1h"
            if i > 0 and abs(float(k[2])-float(k[4])) < abs(float(recent[i-1][2])-float(recent[i-1][4])):
                return "decel_1h"
    return None


def _volume_shrink_15m(k15: list) -> bool:
    """最近3根15M成交量均低于20根均量的0.8倍"""
    if len(k15) < 4: return False
    vols = _vols(k15)
    ma = sum(vols[-20:]) / min(20, len(vols))
    return all(v < ma * 0.8 for v in vols[-3:])


def _vwap_dense_zone(k1h: list) -> tuple[float, float]:
    """
    近20根1H K线的成交量加权价格分布,找出成交量最集中的价格区段。
    简化:取成交量最大的前5根K线的最高价/最低价作为密集区边界。
    """
    if len(k1h) < 5:
        prices = _closes(k1h)
        return min(prices), max(prices)
    top5 = sorted(k1h[-20:], key=lambda k: float(k[5]), reverse=True)[:5]
    low  = min(float(k[3]) for k in top5)
    high = max(float(k[2]) for k in top5)
    return low, high


def _fill_liq_zone(out: dict, price: float, direction: str, atr: float,
                   dist_min: float = 0.5, dist_max: float = 2.0,
                   liq_threshold_pending: float = 2_000_000,
                   symbol_from_ind: dict = None) -> None:
    """填充清算区字段(从 liq_cache 读缓存,不直接调 Clawby 热路径)"""
    try:
        symbol = symbol_from_ind.get("symbol", "") if symbol_from_ind else ""
        if not symbol: return
        from shared.liq_cache import get_liq_zones_cached
        zones = get_liq_zones_cached(symbol, direction, price, range_="3d")
        if not zones: return

        total_liq = 0.0; best_dist = None
        for z in zones:
            mid   = (z["price_low"] + z["price_high"]) / 2
            dist  = abs(price - mid)
            atr_d = dist / (atr + 1e-9)
            if dist_min <= atr_d <= dist_max:
                total_liq += z.get("liq_usd", 0)
                if best_dist is None or atr_d < best_dist:
                    best_dist = round(atr_d, 2)

        # [v3.89 #29] 先局部算完再写回：只有全部字段计算成功才写入，
        #   避免异常时留下「部分字段已写、其余仍是默认值」的混合状态，
        #   使调用方无法区分真实零清算量、数据缺失与解析错误。
        _liq_valid  = total_liq > 0
        _liq_amount = round(total_liq, 0)
        out["liq_zone_valid"]        = _liq_valid
        out["liq_zone_distance_atr"] = best_dist
        out["liq_zone_amount"]       = _liq_amount
    except Exception as _liq_e:
        # [v3.89 #29] 不再静默：区分「取到数据但无有效清算区」与「计算/解析失败」
        out["liq_zone_valid"]  = False
        out["liq_zone_error"]  = type(_liq_e).__name__
        logger.debug(f"[judges] _fill_liq_zone 计算失败（清算字段按无效处理）: {_liq_e}")


# ══════════════════════════════════════════════════════════════════════════════
# judge_breakout - 动量突破型
# ══════════════════════════════════════════════════════════════════════════════
def judge_breakout(k1h: list, k4h: list, k15: list, ind: dict,
                   chg24: float, funding: float) -> tuple[bool, dict]:
    try:
        return _judge_breakout_impl(k1h, k4h, k15, ind, chg24, funding)
    except Exception as e:
        _log_judge_failure("judge_breakout", e, k1h, k4h, k15, ind)
        return _judge_error_result("judge_breakout", "breakout", e)


def _judge_breakout_impl(k1h, k4h, k15, ind, chg24, funding):
    price   = ind["price"]
    atr     = ind["atr_1h"]
    atr_r   = _atr_ratio(atr, price)
    tier    = _volatility_tier(atr, price)
    rsi_1h  = ind["rsi_1h"]
    rsi_4h  = ind.get("rsi_4h", 50)
    vol_ma  = ind["vol_ma20"]
    symbol  = ind.get("symbol", "")

    # 注意:k1h 末尾已去除未闭合 K 线(调用者保证)
    # 突破 K 线 = k1h[-1],近10根基准 = k1h[-11:-1](不含突破K线)
    # [v3.85 OPT-5] 只用已闭合 K 线判断突破。原注释写「已完全闭合，扫描周期保证」，
    #   但 scanner 拉到的 1H K 线最后一根通常是正在形成的那根（calc_indicators 自己也会剔除 k[:-1]）。
    #   用未闭合 K 线会：盘中触发、随后被重绘成假突破；成交量只算了一部分，量比失真。
    #   这里按收盘时间 k[6] 判断，未闭合就去掉最后一根。开关 OPT_BREAKOUT_CLOSED_BAR。
    try:
        from config import OPT_BREAKOUT_CLOSED_BAR as _OPT_BC
    except Exception:
        _OPT_BC = False
    if _OPT_BC and k1h:
        import time as _t_bc
        try:
            if float(k1h[-1][6]) > _t_bc.time() * 1000:
                k1h = k1h[:-1]
        except (IndexError, TypeError, ValueError):
            pass
    if len(k1h) < 12:
        return False, {"reject_reason": "K线数量不足"}

    breakout_k   = k1h[-1]               # 突破K线(已完全闭合,扫描周期保证)
    base_klines  = k1h[-11:-1]           # 近10根基准(不含突破K线本身)
    pre20_klines = k1h[-21:-1]           # 近20根均量基准

    pre20_vol_ma = sum(float(k[5]) for k in pre20_klines) / len(pre20_klines) if pre20_klines else vol_ma
    breakout_vol = float(breakout_k[5])

    # v3.6: ref 改用最高价/最低价（原为收盘价，偏低，真实突破应超过前期最高/低点）
    ref_high = max(float(k[2]) for k in base_klines)  # 近10根最高价
    ref_low  = min(float(k[3]) for k in base_klines)  # 近10根最低价
    breakout_close = float(breakout_k[4])

    # v3.6: direction 由实际突破方向决定，而非 chg24（chg24 可能因前期走势方向相反而误判）
    if breakout_close > ref_high:
        direction = "LONG"
    elif breakout_close < ref_low:
        direction = "SHORT"
    else:
        # 收盘价在近10根高低价区间内，没有真正突破
        return False, {"reject_reason": f"收盘价{breakout_close:.4g}未突破近10根区间[{ref_low:.4g},{ref_high:.4g}]"}

    vol_mult_req = 2.5 if tier == "high" else 2.0
    # [v3.85 FIX-1] 原写法 `65 if tier=="high" else 65` 两个分支相同，分层是假的，且没有读取配置
    #   （config 里 BREAKOUT_RSI_* 从未被引用）。改为读取配置；配置值已同步为原实际生效值，行为不变。
    from config import (BREAKOUT_RSI_MAX_LONG, BREAKOUT_RSI_MAX_LONG_HIGH,
                        BREAKOUT_RSI_MIN_SHORT, BREAKOUT_RSI_MIN_SHORT_HIGH)
    rsi_limit    = BREAKOUT_RSI_MAX_LONG_HIGH if tier == "high" else BREAKOUT_RSI_MAX_LONG
    rsi_limit_s  = BREAKOUT_RSI_MIN_SHORT_HIGH if tier == "high" else BREAKOUT_RSI_MIN_SHORT
    margin_mult  = 0.2 if tier == "high" else 0.15
    oi_req_pct   = 5.0 if tier == "high" else 3.0

    out = {
        "mode":               "breakout",
        "volatility_tier":    tier,
        "atr_ratio":          round(atr_r, 4),
        "breakout_direction": direction.lower(),
        "breakout_ref_high":  ref_high,
        "breakout_ref_low":   ref_low,
        "breakout_margin_atr": None,
        "volume_ratio":       round(breakout_vol / (pre20_vol_ma + 1e-9), 2),
        "volume_confirm":     False,
        "bar_15m_confirm":    False,
        "bar_15m_upper_shadow_flag": False,
        "rsi_1h":             rsi_1h,
        "rsi_4h":             rsi_4h,
        "oi_change_pct":      None,
        "oi_confirm":         False,
        "funding_rate":       funding,
        "liq_pressure_above": None,
        "liq_pressure_below": None,
        "extreme_candle_flag": False,
        "breakout_entry_price": None,
        "signal_strength":    "medium",
        "reject_reason":      None,
    }

    # 极端K线标记
    out["extreme_candle_flag"] = any(
        abs(float(k[4]) - float(k[1])) / (float(k[1]) + 1e-9) > 0.20
        for k in k1h[-20:] if float(k[1]) > 0
    )

    # ── v3.46 动态波动率分档（替代固定门槛）───────────────────────────────
    from config import ATR_TIER_HIGH_MIN, ATR_TIER_MID_MIN, ATR_TIER_HIGH_WEEKEND, ATR_TIER_MID_WEEKEND
    _weekend_brk = ind.get("_weekend_mode", False)
    if atr_r >= ATR_TIER_HIGH_MIN:
        _vol_tier_brk = "high"
        _atr_min_brk  = ATR_TIER_HIGH_WEEKEND if _weekend_brk else ATR_TIER_HIGH_MIN
    elif atr_r >= ATR_TIER_MID_MIN:
        _vol_tier_brk = "mid_vol"
        _atr_min_brk  = ATR_TIER_MID_WEEKEND if _weekend_brk else ATR_TIER_MID_MIN
    else:
        out["reject_reason"] = f"波动率过低:ATR_ratio={atr_r:.2f}%<{ATR_TIER_MID_MIN}%，不适合当前策略"
        return False, out
    ind["_vol_tier"] = _vol_tier_brk
    if atr_r < _atr_min_brk:
        out["reject_reason"] = f"波动率过低({_vol_tier_brk}档):ATR_ratio={atr_r:.2f}% < {_atr_min_brk}%"
        return False, out

    # ── 4H 方向过滤（v3.15.6）─────────────────────────────────────────────────
    # breakout 需要趋势配合，逆势 breakout 成功率极低且止损即时发生
    # LONG breakout 要求 4H 多头（ema20>=ema50）或中性（差距<=1.5%）
    # SHORT breakout 要求 4H 空头（ema20<=ema50）或中性
    _ema20_4h_bo = ind.get("ema20_4h", 0)
    _ema50_4h_bo = ind.get("ema50_4h", 0)
    if _ema20_4h_bo > 0 and _ema50_4h_bo > 0:
        _4h_gap_bo = (_ema20_4h_bo - _ema50_4h_bo) / (_ema50_4h_bo + 1e-9) * 100
        if direction == "LONG" and _4h_gap_bo < -2.0:
            out["reject_reason"] = (
                f"4H空头排列（EMA20={_ema20_4h_bo:.4g} < EMA50={_ema50_4h_bo:.4g}，"
                f"差距{abs(_4h_gap_bo):.1f}%>2%），逆势LONG breakout成功率低，拒绝"
            )
            return False, out
        if direction == "SHORT" and _4h_gap_bo > 2.0:
            out["reject_reason"] = (
                f"4H多头排列（EMA20={_ema20_4h_bo:.4g} > EMA50={_ema50_4h_bo:.4g}，"
                f"差距{_4h_gap_bo:.1f}%>2%），逆势SHORT breakout成功率低，拒绝"
            )
            return False, out
        out["4h_trend_gap"] = round(_4h_gap_bo, 2)

    # 条件一:有效突破
    if direction == "LONG":
        margin = atr * margin_mult
        if breakout_close <= ref_high or breakout_close < ref_high + margin:
            out["reject_reason"] = f"突破幅度不足:close={breakout_close:.4g} ref_high={ref_high:.4g} margin={margin:.4g}"
            return False, out
        out["breakout_margin_atr"] = round((breakout_close - ref_high) / (atr + 1e-9), 3)
        out["breakout_entry_price"] = ref_high
    else:
        margin = atr * margin_mult
        if breakout_close >= ref_low or breakout_close > ref_low - margin:
            out["reject_reason"] = f"突破幅度不足:close={breakout_close:.4g} ref_low={ref_low:.4g} margin={margin:.4g}"
            return False, out
        out["breakout_margin_atr"] = round((ref_low - breakout_close) / (atr + 1e-9), 3)
        out["breakout_entry_price"] = ref_low

    # 条件二:成交量验证
    vol_ratio = breakout_vol / (pre20_vol_ma + 1e-9)
    # v3.6: 量能验证改为「近20根前3大」，原「近3根最大」几乎必然满足，无区分度
    _pre20_sorted = sorted([float(k[5]) for k in k1h[-21:-1]], reverse=True)
    _vol_top3_thresh = _pre20_sorted[2] if len(_pre20_sorted) >= 3 else (_pre20_sorted[-1] if _pre20_sorted else 0)
    vol_ok = (vol_ratio >= vol_mult_req) and (breakout_vol >= _vol_top3_thresh)
    out["volume_confirm"] = vol_ok
    if not vol_ok:
        out["reject_reason"] = f"成交量不足:量比{vol_ratio:.2f}<{vol_mult_req} 或未入近20根前3大(阈值{_vol_top3_thresh:.0f})"
        return False, out

    # 条件三:15M确认站稳
    # 注意:15M确认分两轮扫描,此处检查是否已有确认K线闭合
    confirm_ok, shadow_flag = _check_15m_confirm(
        k15, direction, ref_high if direction=="LONG" else ref_low, atr,
        breakout_open_ts=float(breakout_k[0])   # v3.6: 传入突破K线开盘时间戳，确认时序
    )
    out["bar_15m_confirm"]          = confirm_ok
    out["bar_15m_upper_shadow_flag"] = shadow_flag
    if not confirm_ok:
        # 未确认:写入 pending_confirm 状态,本轮不输出信号
        out["reject_reason"] = "15M确认K线未闭合,等待下一轮"
        # 调用方(scanner)负责写入 pending_confirm_state
        out["_pending_confirm"] = True
        return False, out

    # 条件四:RSI
    # [v3.87 OPT-12] 放宽强势突破的 RSI 上限。真正的放量突破 RSI1H 常在 65~75，固定 65 会把最强的一类突破全部挡掉。
    #   仅当量比 ≥ 强量阈值（low 2.5 / 其它 3.0，与下方 vol_strong 一致）时，上限放宽到 BREAKOUT_RSI_MAX_LONG_STRONG(75)，
    #   做空下限放宽到 BREAKOUT_RSI_MIN_SHORT_STRONG(25)。量能不够强仍用原限制。开关 OPT_BREAKOUT_RSI_STRONG_RELAX。
    try:
        from config import (OPT_BREAKOUT_RSI_STRONG_RELAX as _OPT_BRR,
                            BREAKOUT_RSI_MAX_LONG_STRONG as _RSI_L_STR,
                            BREAKOUT_RSI_MIN_SHORT_STRONG as _RSI_S_STR)
    except Exception:
        _OPT_BRR = False
    if _OPT_BRR and vol_ratio >= (2.5 if tier == "low" else 3.0):
        rsi_limit   = max(rsi_limit, _RSI_L_STR)
        rsi_limit_s = min(rsi_limit_s, _RSI_S_STR)
    if direction == "LONG" and rsi_1h > rsi_limit:
        out["reject_reason"] = f"RSI1H={rsi_1h:.1f} > {rsi_limit},超买追多"
        return False, out
    if direction == "SHORT" and rsi_1h < rsi_limit_s:
        out["reject_reason"] = f"RSI1H={rsi_1h:.1f} < {rsi_limit_s},超卖追空"
        return False, out

    # 条件五:OI同步上升
    oi_now_val = ind.get("oi_now", 0)
    oi_15m_ago = _get_oi_15m_ago(symbol)
    if oi_15m_ago is not None and oi_now_val > 0:
        oi_chg = (oi_now_val - oi_15m_ago) / (oi_15m_ago + 1e-9) * 100
        out["oi_change_pct"] = round(oi_chg, 2)
        oi_ok = oi_chg >= oi_req_pct
        out["oi_confirm"] = oi_ok
        if not oi_ok:
            out["reject_reason"] = f"OI上升不足:{oi_chg:.1f}% < {oi_req_pct}%"
            return False, out
    else:
        out["oi_confirm"]    = False   # v3.6: 无 OI 缓存时不再 failopen，标记缺失
        out["oi_cache_miss"] = True    # 限制：此信号强度上限为 medium（在信号强度分级处处理）

    # v3.6: 资金费率检查提升为全波动层（原仅高层）
    # 通用上限 0.2%（低/中层），高波动层保持更严的 0.15%
    _fr_cap = 0.0015 if tier == "high" else 0.002
    if direction == "LONG" and funding > _fr_cap:
        out["reject_reason"] = f"资金费率过高({funding*100:.4f}%)>{_fr_cap*100:.3f}%，多头持仓成本过大"
        return False, out
    if direction == "SHORT" and funding < -_fr_cap:
        out["reject_reason"] = f"资金费率极端负值({funding*100:.4f}%)<-{_fr_cap*100:.3f}%，空头持仓成本过大"
        return False, out

    # 条件六:突破位上方清算压力(软性)
    _fill_breakout_liq(out, price, direction, atr, symbol)

    # 信号强度分级
    vol_strong  = vol_ratio >= (2.5 if tier == "low" else 3.0)
    oi_strong   = (out.get("oi_change_pct") or 0) >= (5 if tier != "high" else 8)
    rsi_healthy = (50 <= rsi_1h <= 65) if direction == "LONG" else (35 <= rsi_1h <= 50)
    # v3.6: 数据缺失时不计 liq_clear 分（原 None→0→True 默认乐观）
    # 做多看上方清算压力，做空看下方清算压力（按方向取对应字段）
    _liq_press = out.get("liq_pressure_above") if direction == "LONG" else out.get("liq_pressure_below")
    liq_clear  = (_liq_press is not None) and (_liq_press < 5_000_000)

    strong_count = sum([vol_strong, oi_strong, rsi_healthy, liq_clear])
    # v3.6: OI 缓存缺失时信号强度上限为 medium
    if out.get("oi_cache_miss"):
        out["signal_strength"] = "medium"
    else:
        out["signal_strength"] = "strong" if strong_count >= 3 else "medium"

    return True, out


def _check_15m_confirm(k15: list, direction: str, breakout_price: float, atr: float,
                       breakout_open_ts: float = 0) -> tuple[bool, bool]:
    """
    检查 15M 确认K线是否已闭合且站稳在突破位。
    breakout_open_ts: 突破K线开盘时间戳（ms），用于时序验证。
    返回 (confirm_ok, upper_shadow_flag)
    """
    if len(k15) < 2:
        return False, False
    confirm_k = k15[-1]    # 最新已闭合 15M K线
    # v3.6 时序验证：确认K线开盘时间必须晚于突破K线开盘时间，防止用突破前的K线做确认
    if breakout_open_ts > 0:
        confirm_open_ts = float(confirm_k[0])
        if confirm_open_ts <= breakout_open_ts:
            return False, False   # 确认K线在突破之前，无效
    c  = float(confirm_k[4])
    o  = float(confirm_k[1])
    h  = float(confirm_k[2])
    # [v3.87 STEP3] 删除未使用的 lo / v（成交量检查已由条件二对突破K线本身完成）

    # 成交量检查:条件二已对突破K线本身验证量能(vol_ratio>=2.0且是近3根最大)
    # 确认K线的职责是验证价格站稳,正常回踩时量能自然回落,0.8倍均量会误杀有效信号
    # 因此这里不做成交量过滤,只验证价格

    body = abs(c - o) + 1e-9
    upper_shadow_flag = (h - max(o, c)) > body * 1.5

    if direction == "LONG":
        confirm_ok = c >= breakout_price
        # 高波动层:上影线过长视为无效(调用方判断 tier)
    else:
        confirm_ok = c <= breakout_price

    return confirm_ok, upper_shadow_flag


def _get_oi_15m_ago(symbol: str) -> float | None:
    """从 OI 缓存中取约15分钟前的 OI

    直接委托 state.get_oi_15min_ago,避免时间戳单位不一致的 bug
    (旧实现 r["ts"] 是毫秒,target_ts 是秒,相减后量级为几十亿,> 600 永远成立,
    导致此函数永远返回 None,OI 确认检查永远被跳过)。
    """
    try:
        from shared.state import get_oi_15min_ago
        return get_oi_15min_ago(symbol)
    except Exception:
        return None


def _fill_breakout_liq(out: dict, price: float, direction: str, atr: float, symbol: str) -> None:
    """填充突破位压力(从 liq_cache 读缓存,不直接调 Clawby 热路径)"""
    try:
        if not symbol: return
        from shared.liq_cache import get_liq_zones_cached
        if direction == "LONG":
            zones = get_liq_zones_cached(symbol, "SHORT", price, range_="3d")
            # v3.6: 检查范围 1→2 ATR，breakout 目标价在2~3 ATR，1 ATR 太窄
            total = sum(z.get("liq_usd", 0) for z in zones
                        if 0 < z["price_low"] - price < atr * 2)
            out["liq_pressure_above"] = round(total, 0)
            if total > 5_000_000:
                out["signal_strength"] = "medium"
        else:
            zones = get_liq_zones_cached(symbol, "LONG", price, range_="3d")
            total = sum(z.get("liq_usd", 0) for z in zones
                        if 0 < price - z["price_high"] < atr * 2)
            out["liq_pressure_below"] = round(total, 0)
    except Exception as _bq_e:
        # [v3.89 #29] 不再静默：区分「取到数据但无有效清算区」与「计算/解析失败」，
        #   失败时不写入部分字段，只标记该次清算填充失败
        out["liq_pressure_error"] = type(_bq_e).__name__
        logger.debug(f"[judges] _fill_breakout_liq 计算失败（压力字段按无效处理）: {_bq_e}")


# ══════════════════════════════════════════════════════════════════════════════
# judge_reversal - 极端反转型
# ══════════════════════════════════════════════════════════════════════════════
def judge_reversal(k1h: list, k4h: list, k15: list, ind: dict,
                   chg24: float, funding: float) -> tuple[bool, dict]:
    try:
        return _judge_reversal_impl(k1h, k4h, k15, ind, chg24, funding)
    except Exception as e:
        _log_judge_failure("judge_reversal", e, k1h, k4h, k15, ind)
        return _judge_error_result("judge_reversal", "reversal", e)


def _judge_reversal_impl(k1h, k4h, k15, ind, chg24, funding):
    from config import CLASSIFY_REVERSAL_RSI4H_LONG, CLASSIFY_REVERSAL_RSI4H_SHORT
    # v3.3.6: 流动性门槛——vol24 < 1M 的极低流动性标的直接拒绝
    _vol24 = ind.get("vol24_usd", 0)
    if _vol24 > 0 and _vol24 < 1_000_000:
        return False, {"reject_reason": f"流动性过低: vol24={_vol24/1e6:.1f}M<1M", "mode": "reversal"}
    price   = ind["price"]
    atr     = ind["atr_1h"]
    atr_r   = _atr_ratio(atr, price)
    tier    = _volatility_tier(atr, price)
    rsi_4h  = ind.get("rsi_4h", 50)
    rsi_1h  = ind["rsi_1h"]
    bb_pct  = ind.get("bb_pct", 0.5)
    symbol  = ind.get("symbol", "")

    # 方向判断:以 chg24 为主,RSI4H 为辅助过滤
    # v3.5 修订:问题六修复,收紧 SHORT 方向触发条件,减少大量进入 reversal 路径后被幅度检查拒绝的无效计算
    # 主通道  做多:chg24 <= -8 且 RSI4H < 55
    #         做空:chg24 >= 8 且 RSI4H > 55(两个条件必须同时满足,移除 rsi_4h>=70 单独触发)
    # v3.6 补丁:_is_reversal 用 rsi_1h < 28/> 72 引导进此路径,但主通道要求 chg24 >= 8% 会在
    #           方向判断阶段直接拒绝小跌幅极端超卖标的(如跌5%、RSI1H=22)。
    #           RSI1H 极端超卖/超买通道:rsi_1h < 25 且 chg24 <= -5 → LONG
    #                                    rsi_1h > 75 且 chg24 >= 5  → SHORT
    #           放宽幅度门槛(-5% vs -8%),专门接住分类层路由进来的小跌幅极端信号。
    # v3.46: 根据波动率档位动态决定 reversal chg24 触发门槛
    # [v3.87 STEP3] 删除重复导入 ATR_TIER_MID_MIN（下方 1343 行附近同函数内再次导入，值相同）
    from config import ATR_TIER_MID_REVERSAL_CHG24, REVERSAL_CHG24_SHORT_MIN
    _rev_chg24_long  = -ATR_TIER_MID_REVERSAL_CHG24 if ind.get("_vol_tier") == "mid_vol" else -8.0
    # [v3.87 STEP3 DOC] _rev_chg24_short 是遗留死变量：v3.64 起做空主通道有意放宽并写死为 chg24 >= +5.0（见下方方向路由），
    #   不再读取本变量，因此中波动档的做空涨幅门槛配置不生效。做多仍使用 _rev_chg24_long，多空门槛不对称是有意的。保留不删。
    _rev_chg24_short =  ATR_TIER_MID_REVERSAL_CHG24 if ind.get("_vol_tier") == "mid_vol" else  8.0

    # v3.64: Reversal SHORT 门槛修复
    # 原问题：chg24>=+8% AND rsi4h>55 中，大涨后rsi4h几乎必然>55，条件形同虚设；
    #         真正瓶颈是chg24>=+8%出现频率极低，导致SHORT方向从未触发。
    # 修复：SHORT 主通道收紧 RSI 要求（>65），同时放宽 chg24 门槛（+5%）；
    #       RSI1H 补充通道也对称放宽（chg24>=4%，RSI1H>72仍保留）。
    if chg24 <= _rev_chg24_long and rsi_4h < 55:
        direction = "LONG"
        _rsi1h_channel = False
    elif chg24 >= REVERSAL_CHG24_SHORT_MIN and rsi_4h > 65:
        # v3.64: SHORT 主通道: chg24>=+5%（原8%）且 rsi4h>65（原55，提高要求补偿放宽的chg24）
        direction = "SHORT"
        _rsi1h_channel = False
    elif rsi_1h < 28 and chg24 <= -5 and rsi_4h < 55:
        direction = "LONG"    # RSI1H 极端超卖通道
        _rsi1h_channel = True
    elif rsi_1h > 72 and chg24 >= REVERSAL_CHG24_SHORT_MIN and rsi_4h > 55:   # [fix C2] 4%→10%，与主通道一致
        # v3.64: SHORT RSI1H 补充通道: chg24>=+4%（原5%，略放宽），RSI1H>72 保持
        direction = "SHORT"
        _rsi1h_channel = True
    else:
        return False, {"reject_reason": f"方向不明确:chg24={chg24:.1f}% RSI4H={rsi_4h:.1f} RSI1H={rsi_1h:.1f},不符合 LONG/SHORT 任一条件"}

    out = {
        "mode":                  "reversal",
        "volatility_tier":       tier,
        "atr_ratio":             round(atr_r, 4),
        "direction":             direction.lower(),
        "crash_guard_triggered": False,
        "window_24h_pct":        chg24,
        "window_4h_pct":         None,
        "dual_window_resonance": False,
        "signal_base_strength":  "medium",
        "funding_rate":          funding,
        "funding_extreme":       False,
        "rsi_4h":                rsi_4h,
        "rsi_1h":                rsi_1h,
        "rsi_extreme":           False,
        "reversal_candles_15m":  None,
        "deceleration_confirmed": False,
        "extra_structure_met":   False,
        "liq_zone_below_amount": None,
        "liq_zone_above_amount": None,
        "position_size_cap":     0.5,
        "entry_mode":            "immediate",
        "extreme_candle_flag":   False,
        "signal_strength":       "medium",
        "cooldown_triggered":    False,
        "reject_reason":         None,
        "pulse_short_penalty":   0,
    }
    _crash = _btc_crash_reject(ind, direction, "reversal", out)
    if _crash is not None:
        return _crash

    # ── 极端K线标记 ────────────────────────────────────────────────────────────
    extreme_flag = any(
        abs(float(k[4]) - float(k[1])) / (float(k[1]) + 1e-9) > 0.20
        for k in k1h[-20:] if float(k[1]) > 0
    )
    out["extreme_candle_flag"] = extreme_flag

    # ── v3.46 脉冲大涨中途做空保护（评分惩罚，非硬性禁止）──────────────────────
    # 场景：chg24 >= 15%（短期脉冲大涨）AND vol_ratio >= 2.5（量能配合）
    # 动能还强，reversal SHORT 进场太早；用评分惩罚让 medium 信号(base=50)降到 35
    # 只有 strong 信号(base=70)或有其他加分项才能通过
    if direction == "SHORT":
        _chg24_abs  = abs(chg24)
        _vol_ratio  = ind.get("vol_ratio", 0)
        _pulse_short = _chg24_abs >= 15.0 and _vol_ratio >= 2.5
        if _pulse_short:
            out["pulse_short_penalty"] = -26
            logger.info(
                f"[judge_reversal] {ind.get('symbol','')} SHORT 脉冲大涨惩罚: "
                f"chg24={chg24:+.1f}% vol_ratio={_vol_ratio:.2f} → score_adj=-26"
            )

    # ── v3.59 量能硬性门槛（Reversal 所有方向）─────────────────────────────
    # Opus 4.5 发现：实盘 vol_ratio 最低 0.012x，无量进场和 early_stagnation 直接挂钉
    # reversal 需要真正的市场异动（大跌/大涨），无量时评价信号没有意义
    _rev_vol_ratio_min = 0.40
    _rev_vol = ind.get("vol_ratio", ind.get("vol_ratio_1h", 1.0))
    if _rev_vol < _rev_vol_ratio_min:
        out["reject_reason"] = (
            f"reversal 量能门槛：vol_ratio={_rev_vol:.2f} < {_rev_vol_ratio_min}，"
            f"无量异动时 reversal 信号无意义"
        )
        return False, out

    # ── 0. 连续崩盘/急涨检测(最优先,触发立刻返回)──────────────────────────────
    if direction == "LONG":   # 检测下跌崩盘
        crash = _check_crash_guard(k1h, k4h, direction, ind)
        if crash:
            out["crash_guard_triggered"] = True
            out["cooldown_triggered"]    = True
            out["reject_reason"]         = "连续崩盘拦截"
            _set_cooldown_safe(symbol, 4, "crash_guard")
            return False, out
    elif direction == "SHORT":  # 检测急速拉升（防止追空被逼空）
        crash = _check_crash_guard(k1h, k4h, direction, ind)
        if crash:
            out["crash_guard_triggered"] = True
            out["cooldown_triggered"]    = True
            out["reject_reason"]         = "急速拉升熔断，不追空"
            _set_cooldown_safe(symbol, 4, "crash_guard_short")
            return False, out

    # ── 0.5 4H 多头结构保护（仅 SHORT 方向）────────────────────────────────────
    # 方向确认后立即检查，不浪费后续计算资源
    # ind 里已有 analyze_symbol 预计算的 ema20_4h / ema50_4h，直接读取即可
    # 阈值 1.5%（比 breakout 的 2% 稍严）：只要 4H EMA 有明确多头倾斜就拒绝逆势做空
    if direction == "SHORT":
        _ema20_4h_s = ind.get("ema20_4h", 0)
        _ema50_4h_s = ind.get("ema50_4h", 0)
        if _ema20_4h_s > 0 and _ema50_4h_s > 0:
            _4h_bull_gap_s = (_ema20_4h_s - _ema50_4h_s) / (_ema50_4h_s + 1e-9) * 100
            if _4h_bull_gap_s > 1.5:
                # [opt-2] 4H 多头不再直接拒绝：至少 2 条衰竭证据才允许做空，并降仓 ×0.7
                _ev, _miss = [], []
                try:
                    _div_ok = bool(_detect_rsi_divergence(k1h, "SHORT").get("divergence"))
                except Exception:
                    _div_ok = False
                (_ev if _div_ok else _miss).append("RSI顶背离")
                _mk, _ix = float(ind.get("markPrice", 0) or 0), float(ind.get("indexPrice", 0) or 0)
                (_ev if (_ix > 0 and (_mk - _ix) / _ix > 0.003) else _miss).append("基差>0.3%")
                (_ev if ind.get("taker_buy_ratio_now", 0.5) < ind.get("taker_buy_ratio", 0.5) - 0.03
                 else _miss).append("主动买入占比下降(1H口径)")
                _oi_n, _oi_h = float(ind.get("oi_now", 0) or 0), float(ind.get("oi_1h_ago", 0) or 0)
                (_ev if (_oi_h > 0 and _oi_n < _oi_h * 0.97) else _miss).append("OI 1H下降>3%")
                _miss.append("4H多头gap<=1.5%")   # 本分支 gap>1.5%，该条不成立
                out["exhaustion_evidence"] = _ev
                if len(_ev) < 2:
                    out["reject_reason"] = (
                        f"4H多头排列（gap={_4h_bull_gap_s:.1f}%>1.5%）且衰竭证据不足"
                        f"（{len(_ev)}/2，已有:{_ev or '无'}，缺:{_miss}），拒绝逆势做空"
                    )
                    return False, out
                out["exhaustion_short"] = True
            out["4h_ema_bull_gap"] = round(_4h_bull_gap_s, 2)

    # ── 0.6 脉冲大阳保护（仅 SHORT，拦截庄家拉盘后追空）──────────────────────
    if direction == "SHORT":
        _pulse_r = _is_pulse_candle(k1h, k4h)
        if _pulse_r:
            out["reject_reason"] = f"脉冲大阳保护，拒绝 reversal SHORT: {_pulse_r}"
            return False, out

    # ── 1. 4H 涨跌幅计算 ──────────────────────────────────────────────────────
    chg4h = 0.0
    if len(k1h) >= 5:
        p4h_ago = float(k1h[-5][1])   # 4根前的开盘价
        chg4h   = (price - p4h_ago) / (p4h_ago + 1e-9) * 100
    out["window_4h_pct"] = round(chg4h, 2)

    # ── 1.5 1 4H更高低点结构检测(最先运行,直接过滤下跌通道)──────────────────
    # 只对做多 reversal 强执行;做空 reversal 不需要更高低点
    hl_status, hl_cap_mult = "", 1.0
    if direction == "LONG" and k4h:
        hl_status, hl_cap_mult = _check_4h_higher_low(k4h)
        out["higher_low_status"]   = hl_status
        out["higher_low_cap_mult"] = hl_cap_mult
        if hl_status == "no_data":
            out["reject_reason"] = "4H K线数据不足,无法判断摆动低点结构"
            return False, out
        if hl_status == "reject":
            swing_lows = _swing_lows_4h(k4h)
            low_desc = f"{swing_lows[-2]:.6g}→{swing_lows[-1]:.6g}" if len(swing_lows) >= 2 else "?"
            out["reject_reason"] = f"4H下跌通道:摆动低点依次降低({low_desc}),不允许做多reversal"
            return False, out
        # degraded:仅找到一个摆动低点,仓位减半(后面统一应用 hl_cap_mult)
    else:
        out["higher_low_status"]   = "skipped"
        out["higher_low_cap_mult"] = 1.0


    # ── 2. 极端幅度双窗口判断 ─────────────────────────────────────────────────
    # RSI1H 极端通道进来的信号跳过幅度门槛（分类层已用 rsi_1h<28 确认极端超卖），
    # 直接标记 rsi_extreme=True + signal_base_strength=strong。
    # ⚠️ 注意：_rsi1h_channel=True 时不再在此处立即设 rsi_extreme/strong，
    #    而是在 15M 止涨/止跌形态确认后才标记，防止「涨势加速」被误判为「顶部做空」
    # （ICNT 复盘：23:00 大阳线 +5.4% 推高 RSI→89.3，系统误判为顶部，实为加速上涨）
    if not _rsi1h_channel:
        if direction == "LONG":

        # ── 4H 空头结构保护（v3.15.6）────────────────────────────────────────
        # reversal LONG 本来就是在大跌后做多，4H 空头是触发前提之一
        # 但若 4H 空头差距过大（> 3%），趋势过强，止跌只是反弹，降级仓位
        # 不硬拒：reversal LONG 的存在意义就是逆势抄底，硬拒会杀掉合理信号
        # 门槛用 3%（比 SHORT 的 2% 宽），因为 LONG 本来就在下跌中入场
            _ema20_4h_long = ind.get("ema20_4h", 0)
            _ema50_4h_long = ind.get("ema50_4h", 0)
            if _ema20_4h_long > 0 and _ema50_4h_long > 0:
                _4h_bear_gap = (_ema50_4h_long - _ema20_4h_long) / (_ema50_4h_long + 1e-9) * 100
                if _4h_bear_gap > 3.0:
                    out["counter_trend"]     = True
                    out["position_size_cap"] = min(out.get("position_size_cap", 0.5) * 0.6, 0.3)
                    out["signal_strength"]   = "medium"
                    out["4h_bear_gap"]       = round(_4h_bear_gap, 2)

            win24_ok = chg24 <= -15
            win4h_ok = chg4h <= -10
        else:
            # v3.64: SHORT 方向幅度要求同步放宽（触发门槛已降至+5%）
            win24_ok = chg24 >= 5.0  # 原20%→5%，与触发门槛对齐
            win4h_ok = chg4h >= 3.0  # 原12%→3%，4H涨3%+就算有效幅度

        if not (win24_ok or win4h_ok):
            out["reject_reason"] = f"幅度不足:24H={chg24:.1f}% 4H={chg4h:.1f}%"
            return False, out

        if win4h_ok:
            out["signal_base_strength"] = "strong"
        if win24_ok and win4h_ok:
            out["dual_window_resonance"] = True

    # ── 高波动层:RSI 门槛更严 ─────────────────────────────────────────────────
    # RSI4H 门槛:方向已由 chg24 确定,RSI 只做辅助过滤
    # LONG:RSI4H < 45(跌了10%以上价格已在低位,不要求极度超卖)
    # SHORT:RSI4H > 65(涨了20%以上,不要求极度超买)
    rsi_long_thresh  = 30 if tier == "high" else 40  # v3.6: 45→40，RSI4H<45 不够超卖
    rsi_short_thresh = 70 if tier == "high" else 65

    # ── v3.46 动态波动率分档（替代固定门槛）───────────────────────────────
    from config import ATR_TIER_HIGH_MIN, ATR_TIER_MID_MIN, ATR_TIER_HIGH_WEEKEND, ATR_TIER_MID_WEEKEND
    _weekend_rev = ind.get("_weekend_mode", False)
    if atr_r >= ATR_TIER_HIGH_MIN:
        _vol_tier_rev = "high"
        _atr_min_rev  = ATR_TIER_HIGH_WEEKEND if _weekend_rev else ATR_TIER_HIGH_MIN
    elif atr_r >= ATR_TIER_MID_MIN:
        _vol_tier_rev = "mid_vol"
        _atr_min_rev  = ATR_TIER_MID_WEEKEND if _weekend_rev else ATR_TIER_MID_MIN
    else:
        out["reject_reason"] = f"波动率过低:ATR_ratio={atr_r:.2f}%<{ATR_TIER_MID_MIN}%，不适合当前策略"
        return False, out
    ind["_vol_tier"] = _vol_tier_rev
    if atr_r < _atr_min_rev:
        out["reject_reason"] = f"波动率过低({_vol_tier_rev}档):ATR_ratio={atr_r:.2f}% < {_atr_min_rev}%"
        return False, out
    # 做多资金费率门槛:允许小正费率(市场轻度多头),超过 +0.1% 才认为多头拥挤
    fr_long_thresh   = 0.0015 if tier == "high" else 0.0010
    # [v3.87 STEP3 DOC] fr_short_thresh 是有意不接入的变量：做空只用下方硬编码的 funding < -0.002 拒绝（v3.6 放宽，
    #   熊市资金费率常偏负，若用 0.25%/0.35% 作硬门槛会拦死做空）。做多的 fr_long_thresh 则作为硬门槛使用，两侧不对称是有意的。保留不删。
    fr_short_thresh  = 0.0035 if tier == "high" else 0.0025

    # ── 做多条件 ──────────────────────────────────────────────────────────────
    if direction == "LONG":

        # 条件一:资金费率检查(超过门槛说明多头拥挤,拒绝)
        if funding > fr_long_thresh:
            out["reject_reason"] = f"资金费率过高,多头拥挤:{funding*100:.4f}% > {fr_long_thresh*100:.4f}%"
            return False, out
        if funding < -0.003:
            out["funding_extreme"] = True   # 负费率 < -0.3% 为强信号

        # 条件二:RSI极端超卖
        if rsi_4h >= rsi_long_thresh:
            out["reject_reason"] = f"RSI4H={rsi_4h:.1f} >= {rsi_long_thresh},未达超卖"
            return False, out
        if rsi_4h < 20:
            out["rsi_extreme"] = True
        # 优化:RSI_1h < 25 才算超卖(更严格)
        if rsi_1h < 25:
            out["signal_base_strength"] = "strong"
        elif rsi_1h >= 35:
            out["reject_reason"] = f"RSI1H={rsi_1h:.1f} >= 35,未达极端超卖"
            return False, out

        # 条件三:连续两根15M止跌K线(所有波动层都需要)
        consec = _two_consecutive_reversal_15m(k15, "LONG")
        out["reversal_candles_15m"] = consec
        if consec is None:
            out["reject_reason"] = "未找到连续两根15M止跌K线"
            return False, out

        # 条件三+:15M收盘价需站上最近20根15M均价(更严格的止跌确认)
        # 防止在下跌途中短暂出现止跌形态就入场
        if len(k15) >= 20:
            ma20_closes = [float(k[4]) for k in k15[-20:]]
            ma20 = sum(ma20_closes) / len(ma20_closes)
            last_15m_close = float(k15[-1][4])
            if last_15m_close < ma20 * 0.998:  # 给0.2%容差
                # v3.6: 改为软性——reversal 反弹初期价格常在均价以下，硬拒误杀率过高
                # 未站上均价时降低信号基础强度（不达 strong），但不直接拒绝
                if out.get("signal_base_strength") == "strong":
                    out["signal_base_strength"] = "medium"
                out["ma20_15m_miss"] = True  # 记录标记，供复盘分析

        # 条件四:跌势减速
        decel = _deceleration_1h(k1h, "LONG")
        out["deceleration_confirmed"] = decel
        if not decel:
            out["reject_reason"] = "跌势未减速"
            return False, out

        # 条件四+:1H RSI开始回升(最新RSI > 前一根RSI),防止RSI还在跌时入场
        if len(k1h) >= 16:
            closes_1h = [float(k[4]) for k in k1h[-16:]]
            def _rsi14(closes):
                diffs = [closes[i]-closes[i-1] for i in range(1,len(closes))]
                gains = [d if d>0 else 0 for d in diffs[-14:]]
                losses= [-d if d<0 else 0 for d in diffs[-14:]]
                ag = sum(gains)/14; al = sum(losses)/14
                return 100 - 100/(1+ag/al) if al > 0 else 100
            rsi_now  = _rsi14(closes_1h)
            rsi_prev = _rsi14(closes_1h[:-1])
            if rsi_now < rsi_prev - 1.0:  # RSI还在下跌(给1点容差)
                out["reject_reason"] = f"1H RSI仍在下跌({rsi_prev:.1f}->{rsi_now:.1f}),未出现回升迹象"
                return False, out

        # 高波动层:两根1H K线都减速
        if tier == "high":
            decel2 = _deceleration_2_consecutive_1h(k1h, "LONG")
            if not decel2:
                out["reject_reason"] = "高波动层:连续两根1H跌幅未递减"
                return False, out

        # 条件五:Coinglass清算区(软性,影响仓位上限)
        # ── 3 RSI底背离记录字段(数据积累,不拦截)──────────────────────────
        # 价格新低但RSI不创新低 = 下跌动能减弱,是真正的止跌信号
        rsi_div = _detect_rsi_divergence(k1h, "LONG")
        out["rsi_divergence"]        = rsi_div.get("divergence", False)
        out["rsi_divergence_detail"] = rsi_div.get("detail", "")
        # 有底背离时作为加分项(不拦截)
        if rsi_div.get("divergence"):
            out["signal_base_strength"] = "strong"

        # ── 4 资金费率变化速度记录字段(数据积累,不拦截)─────────────────────
        # 8小时内从0快速偏负 = 空头情绪化拥挤,挤空机会;持续偏负 = 趋势延续
        fr_vel = _funding_rate_velocity(symbol)
        out["funding_velocity"]        = fr_vel.get("velocity", None)
        out["funding_rapid_negative"]  = fr_vel.get("rapid_negative", False)
        # 快速偏负(挤空信号)= 加分
        if fr_vel.get("rapid_negative"):
            out["funding_extreme"] = True

        _fill_reversal_liq(out, price, direction, atr, symbol)

    # ── 做空条件(v3.5 放宽:熊市中能真正触发做空)────────────────────────────
    else:

        # ── 涨势加速保护（_rsi1h_channel 极端超买通道专属）────────────────────
        # 问题：大阳线推高 RSI→极端值，系统误以为「已在顶部」可做空
        # 修复：若最近1根已收盘1H K线涨幅 ≥ 3%，说明涨势仍在加速，不是顶部，拒绝
        if _rsi1h_channel and len(k1h) >= 2:
            _last_closed = k1h[-1]  # 已收盘的最新K线（调用者已去除未闭合根，-1即最新闭合）
            # v3.64 BUG修复：原代码用 k1h[-2] 跳过了最新闭合K线，导致刚刚加速的行情漏掉保护
            _last_chg = (float(_last_closed[4]) - float(_last_closed[1])) / (float(_last_closed[1]) + 1e-9) * 100
            if _last_chg >= 3.0:
                out["reject_reason"] = (
                    f"RSI极端通道但最近收盘1H K线仍在加速上涨({_last_chg:+.1f}%)，"
                    f"涨势未停，非顶部，拒绝做空"
                )
                return False, out

        # 条件一:  # （4H多头保护已在 Step 0.5 统一处理，此处不重复）资金费率(放宽:不再强制极端资金费率,只要 >= 0 且不是极端负费率就可以做空)
        # 原门槛 0.0025(0.25%)太高,熊市中资金费率往往偏负,完全拦死了做空
        # 新规:资金费率 > -0.001(-0.1%)即可,极端正费率(>0.003)加分
        if funding < -0.002:   # v3.6: -0.001→-0.002，熊市资金费率常在 -0.1% 附近，旧值误拒太多
            out["reject_reason"] = f"资金费率极端负值({funding*100:.4f}%),空头已被激烈挤压,不宜追空"
            return False, out
        if funding > 0.003:
            out["funding_extreme"] = True

        # 条件二:RSI超买(放宽:4H RSI > 65 即可,不再要求极端超买80+)
        # 原门槛 rsi_short_thresh=65(mid层),但配合 chg24>=20% 门槛,实际上极难同时满足
        # 新规:4H RSI >= 60 + 15M止涨形态即可开空
        rsi_short_thresh_new = 65  # v3.6: 60→65，50中性各距15点，做空须真正超买
        # [v3.86 OPT-7] 上方早已按波动层算好 rsi_short_thresh（高波动 70 / 其余 65），但从未被使用，
        #   高波动币和普通币用同一个 65。高波动币 RSI 更容易冲高，65 对它算不上超买，容易在加速段过早做空。
        #   启用后高波动层用 70，其余层仍是 65（行为不变）。开关 OPT_REVERSAL_SHORT_TIER_RSI。
        #   注：同处的 fr_short_thresh（0.25%/0.35%）若作为硬门槛会显著减少做空信号，与 v3.6 放宽意图冲突，暂不接入。
        try:
            from config import OPT_REVERSAL_SHORT_TIER_RSI as _OPT_RST
        except Exception:
            _OPT_RST = False
        if _OPT_RST:
            rsi_short_thresh_new = max(rsi_short_thresh_new, rsi_short_thresh)
        if rsi_4h < rsi_short_thresh_new:
            out["reject_reason"] = f"RSI4H={rsi_4h:.1f} < {rsi_short_thresh_new},超买力度不足"
            return False, out
        if rsi_4h > 80:
            out["rsi_extreme"] = True
        if rsi_1h > 70:
            out["signal_base_strength"] = "strong"

        # 条件三:连续两根15M止涨K线（_rsi1h_channel 也必须通过，不得豁免）
        # 防止「涨势加速」（大阳线推高RSI）被误判为顶部
        consec = _two_consecutive_reversal_15m(k15, "SHORT")
        out["reversal_candles_15m"] = consec
        if consec is None:
            out["reject_reason"] = "未找到连续两根15M止涨K线"
            return False, out

        # 15M 已确认 → _rsi1h_channel 信号在此处才标记 rsi_extreme + strong
        if _rsi1h_channel:
            out["rsi_extreme"]          = True
            out["signal_base_strength"] = "strong"

        # 条件四:涨势减速（_rsi1h_channel 豁免：极端RSI已能说明超买，减速不是必要条件）
        decel = _deceleration_1h(k1h, "SHORT")
        out["deceleration_confirmed"] = decel
        if not decel and not _rsi1h_channel:
            out["reject_reason"] = "涨势未减速"
            return False, out

        # 条件五:额外结构确认(做空专属,满足任意一项)- 保留但改为软性警告不强制拒绝
        extra = _extra_short_structure(k1h, price, atr, bb_pct, ind)
        out["extra_structure_met"] = extra
        # 不再强制拒绝,只记录;信号强度分级时使用

        # Coinglass
        _fill_reversal_liq(out, price, direction, atr, symbol)

    # ── 信号强度最终分级 ──────────────────────────────────────────────────────
    strong_items = [
        out["signal_base_strength"] == "strong",
        out["funding_extreme"],
        out["rsi_extreme"],
        out.get("dual_window_resonance", False),
        (out.get("liq_zone_below_amount") or 0) >= 3_000_000 if direction == "LONG"
        else (out.get("liq_zone_above_amount") or 0) >= 3_000_000,
        out.get("extra_structure_met", False),  # v3.6: 做空额外结构确认纳入加分
    ]
    strong_count = sum(strong_items)

    if strong_count >= 2:  # v3.6: 3→2，5个加分项中稳定触发的只有2~3个，阈值3过高
        out["signal_strength"]   = "strong"
        out["position_size_cap"] = 0.35 if extreme_flag else 0.5  # v3.58: 0.75→0.5，逆势策略控制风险
        # v3.6 bugfix: reversal 强制 immediate
        # reversal 已是极端行情，应立即入场；pending_order 会把挂单价算到
        # 上方清算区（SHORT 场景可能距现价 10%+），导致挂单永不成交
        out["entry_mode"] = "immediate"
    else:
        out["signal_strength"]   = "medium"
        out["position_size_cap"] = 0.5

    # ── 1 应用 4H更高低点仓位系数(degraded时减半)──────────────────────────────
    # 只对做多 reversal 生效;hl_cap_mult=1.0时不变,=0.5时减半
    _hl_mult = out.get("higher_low_cap_mult", 1.0)
    if _hl_mult < 1.0 and direction == "LONG":
        out["position_size_cap"] = round(out["position_size_cap"] * _hl_mult, 4)
        logger.info(f"[judge_reversal] {symbol} 仅有一个摆动低点(degraded),仓位上限 → {out['position_size_cap']}")
    # [opt-2] 4H 多头下凭衰竭证据放行的 SHORT：降仓 ×0.7
    if out.get("exhaustion_short") and direction == "SHORT":
        out["position_size_cap"] = round(out["position_size_cap"] * 0.7, 4)



    # -- v3.0: 四层标的分类乘数 --
    try:
        import sys as _sys, pathlib as _pl
        _sys.path.insert(0, str(_pl.Path(__file__).parent.parent.parent))
        from calc_position_size_real import classify_tier as _ct, _is_stock_token as _ist, _us_market_open as _umo
        _atr   = ind.get("atr_1h", 0)
        _price = ind.get("price", 1)
        _vol24 = ind.get("vol24_usd", 0)
        _sym   = ind.get("symbol", "")
        _tier  = _ct(_atr, _price, _vol24, _sym)
        _tmult = {"L1": 2.5, "L2": 1.8, "L3": 1.0, "L4": 0.6}.get(_tier, 1.0)
        if _ist(_sym) and not _umo():
            _tmult *= 0.5
        out["position_size_cap"] = round(out.get("position_size_cap", 0.5) * _tmult, 4)
        out["volatility_tier"]   = _tier
    except Exception:
        pass

    # 注：顶部放量出货检测仅适用于 judge_pullback，不在 reversal 场景执行
    # reversal LONG 本身就是极端下跌后做多，k4h 阴线居多，出货检测会大量误拒

    return True, out


# ── 4H 摆动低点 / 高点检测(1 4H更高低点结构) ────────────────────────────────
def _swing_lows_4h(k4h: list, wing: int = 2) -> list[float]:
    """
    从 4H K线中找出有效摆动低点。
    定义:某根K线的最低价,左右各至少 wing 根K线的最低价都高于它。
    返回价格列表(时间顺序,最旧在前)。
    """
    lows = [float(k[3]) for k in k4h]
    result = []
    for i in range(wing, len(lows) - wing):
        candidate = lows[i]
        if all(lows[i - j] > candidate for j in range(1, wing + 1)) and \
           all(lows[i + j] > candidate for j in range(1, wing + 1)):
            result.append(candidate)
    return result


def _swing_highs_4h(k4h: list, wing: int = 2) -> list[float]:
    """
    从 4H K线中找出有效摆动高点。
    定义:某根K线的最高价,左右各至少 wing 根K线的最高价都低于它。
    """
    highs = [float(k[2]) for k in k4h]
    result = []
    for i in range(wing, len(highs) - wing):
        candidate = highs[i]
        if all(highs[i - j] < candidate for j in range(1, wing + 1)) and \
           all(highs[i + j] < candidate for j in range(1, wing + 1)):
            result.append(candidate)
    return result


def _check_4h_higher_low(k4h: list) -> tuple[str, float]:
    """
    检查 4H 级别是否存在「更高低点」结构。
    返回 (status, position_size_cap_modifier)
      status:
        "pass"    → 找到两个摆动低点且第二个 > 第一个(更高低点成立)
        "degraded" → 只找到一个有效摆动低点,数据不足,仓位减半
        "reject"   → 找到两个摆动低点但第二个 <= 第一个(下跌通道)
        "no_data"  → 找不到任何有效摆动低点,直接拒绝
    position_size_cap_modifier: 乘以当前仓位上限
    """
    swing_lows = _swing_lows_4h(k4h, wing=1)  # v3.6: 2→1，快速下跌行情中 wing=2 常找不到有效低点
    if len(swing_lows) == 0:
        return "no_data", 0.0
    if len(swing_lows) == 1:
        return "degraded", 0.5   # 仓位上限减半(25% if 原来50%)
    # 取最近两个
    low1, low2 = swing_lows[-2], swing_lows[-1]
    if low2 > low1 * 1.003:    # v3.6: 容差 0.1%→0.3%，防止几乎持平被误判为上升通道
        return "pass", 1.0
    else:
        return "reject", 0.0   # 下跌通道,拒绝


# ── RSI 底背离检测(2 记录字段,不拦截) ──────────────────────────────────────
def _detect_rsi_divergence(k1h: list, direction: str) -> dict:
    """
    检测 RSI 底背离(做多)或顶背离(做空)。
    算法:找最近两个价格摆动低点/高点,对比对应时刻的 RSI 值。
      底背离:price低点2 < price低点1,但 RSI低点2 > RSI低点1
      顶背离:price高点2 > price高点1,但 RSI高点2 < RSI高点1
    返回 {"divergence": bool, "price_low1": float, "price_low2": float,
           "rsi_low1": float, "rsi_low2": float, "detail": str}
    只做记录,不拦截。
    """
    result = {"divergence": False, "detail": "data_insufficient"}
    if len(k1h) < 30:
        return result

    # 计算 RSI14 序列
    closes = [float(k[4]) for k in k1h]
    rsi_series = []
    for i in range(14, len(closes)):
        diffs  = [closes[j] - closes[j-1] for j in range(i-13, i+1)]
        gains  = [d if d > 0 else 0 for d in diffs]
        losses = [-d if d < 0 else 0 for d in diffs]
        ag = sum(gains) / 14; al = sum(losses) / 14
        rsi_series.append(100 - 100 / (1 + ag / al) if al > 0 else 100)
    # rsi_series[i] 对应 closes[14+i]

    if direction == "LONG":
        lows  = [float(k[3]) for k in k1h]
        # 找摆动低点(价格)
        swing_idx = []
        for i in range(1, len(lows) - 1):  # wing=1，与 _check_4h_higher_low 一致
            if all(lows[i] < lows[i-j] for j in range(1,2)) and \
               all(lows[i] < lows[i+j] for j in range(1,2)):
                swing_idx.append(i)
        if len(swing_idx) < 2:
            result["detail"] = "not_enough_swing_lows"
            return result
        i1, i2 = swing_idx[-2], swing_idx[-1]
        p1, p2 = lows[i1], lows[i2]
        # 对应 RSI(需要 i >= 14)
        if i1 < 14 or i2 < 14:
            result["detail"] = "swing_too_early_for_rsi"
            return result
        r1 = rsi_series[i1 - 14]
        r2 = rsi_series[i2 - 14]
        divergence = (p2 < p1 * 0.999) and (r2 > r1 + 1.0)   # 价格新低,RSI不创新低
        result = {"divergence": divergence,
                  "price_low1": round(p1, 6), "price_low2": round(p2, 6),
                  "rsi_low1": round(r1, 2),   "rsi_low2": round(r2, 2),
                  "detail": "bullish_divergence" if divergence else "no_divergence"}
    else:   # SHORT
        highs = [float(k[2]) for k in k1h]
        swing_idx = []
        for i in range(1, len(highs) - 1):  # wing=1，与 _check_4h_higher_low 一致
            if all(highs[i] > highs[i-j] for j in range(1,2)) and \
               all(highs[i] > highs[i+j] for j in range(1,2)):
                swing_idx.append(i)
        if len(swing_idx) < 2:
            result["detail"] = "not_enough_swing_highs"
            return result
        i1, i2 = swing_idx[-2], swing_idx[-1]
        p1, p2 = highs[i1], highs[i2]
        if i1 < 14 or i2 < 14:
            result["detail"] = "swing_too_early_for_rsi"
            return result
        r1 = rsi_series[i1 - 14]
        r2 = rsi_series[i2 - 14]
        divergence = (p2 > p1 * 1.001) and (r2 < r1 - 1.0)   # 价格新高,RSI不创新高
        result = {"divergence": divergence,
                  "price_high1": round(p1, 6), "price_high2": round(p2, 6),
                  "rsi_high1": round(r1, 2),   "rsi_high2": round(r2, 2),
                  "detail": "bearish_divergence" if divergence else "no_divergence"}
    return result


# ── 资金费率变化速度检测(3 记录字段,不拦截) ───────────────────────────────
def _funding_rate_velocity(symbol: str) -> dict:
    """
    拉取最近 3 条资金费率历史，计算变化速度。
    资金费率每 8 小时结算一次，3 条覆盖约最近 16 小时（2 个间隔）。
    velocity = 当前资金费率 - 16h 前资金费率。
    rapid_negative=True 表示 16h 内从接近 0 快速跌到偏负（空头拥挤信号）。
    拉取失败时返回空字典（不影响主流程）。
    结果缓存 60 秒，防止并发扫描时同标的重复 HTTP 请求。
    """
    now = time.time()
    cached = _fr_velocity_cache.get(symbol)
    if cached and now < cached[1]:
        return cached[0]
    try:
        from shared.binance_api import _get, _retry
        # limit=3 返回最近 3 条资金费率记录，覆盖约最近 16 小时（12h 跨度）。
        # 当前资金费率相对 16h 前的变化速度 = velocity。
        history = _retry(lambda: _get("/fapi/v1/fundingRate",
                                      {"symbol": symbol, "limit": 3})) or []
        if not isinstance(history, list) or len(history) < 2:
            return {"detail": "insufficient_data"}
        rates = [float(h.get("fundingRate", 0)) for h in history]
        current = rates[-1]
        oldest  = rates[0]
        velocity = current - oldest
        rapid_negative = velocity < -0.0005 and current < -0.0002
        result = {
            "velocity": round(velocity, 6),
            "current":  round(current, 6),
            "16h_ago":  round(oldest, 6),
            "rapid_negative": rapid_negative,
            "detail":   "rapid_short_squeeze_signal" if rapid_negative else "normal",
        }
        _fr_velocity_cache[symbol] = (result, now + _FR_CACHE_TTL)
        return result
    except Exception as e:
        return {"detail": f"fetch_failed:{e}"}


# ── 崩盘检测 ──────────────────────────────────────────────────────────────────
def _check_crash_guard(k1h: list, k4h: list, direction: str, ind: dict) -> bool:
    """
    崩盘/急涨检测：
    LONG：检测连续下跌崩盘（防止抄底被埋）
    SHORT：检测连续急涨逼空（防止追空被轧空）
    满足任意一个条件返回 True。
    """
    if len(k1h) < 4: return False

    if direction == "SHORT":
        # SHORT 方向：检测连续急涨（镜像逻辑）
        # 条件1：最近3根1H K线每根收盘均高于前一根，且每根涨幅超过3%
        recent3 = k1h[-3:]
        prev    = k1h[-4]
        cond1   = True
        for i, k in enumerate(recent3):
            c_prev = float(recent3[i-1][4]) if i > 0 else float(prev[4])
            c_curr = float(k[4])
            rise   = (c_curr - c_prev) / (c_prev + 1e-9) * 100
            if c_curr <= c_prev or rise < 3:
                cond1 = False; break

        # 条件2：4H涨幅超过20% 且最新1H涨幅加速
        price = ind["price"]
        chg4h = 0.0
        if len(k1h) >= 5:
            p4h   = float(k1h[-5][1])
            chg4h = (price - p4h) / (p4h + 1e-9) * 100
        rise_last = (float(k1h[-1][4]) - float(k1h[-1][1])) / (float(k1h[-1][1]) + 1e-9) * 100
        rise_prev = (float(k1h[-2][4]) - float(k1h[-2][1])) / (float(k1h[-2][1]) + 1e-9) * 100
        cond2 = (chg4h >= 20) and (rise_last > rise_prev)

        return cond1 or cond2

    # LONG 方向（原逻辑）
    # 条件1
    recent3 = k1h[-3:]
    prev    = k1h[-4]
    cond1   = True
    for i, k in enumerate(recent3):
        c_prev = float(recent3[i-1][4]) if i > 0 else float(prev[4])
        c_curr = float(k[4])
        drop   = (c_prev - c_curr) / (c_prev + 1e-9) * 100
        if c_curr >= c_prev or drop < 3:
            cond1 = False; break

    # 条件2
    price  = ind["price"]
    chg4h  = 0.0
    if len(k1h) >= 5:
        p4h    = float(k1h[-5][1])
        chg4h  = (price - p4h) / (p4h + 1e-9) * 100
    drop_last  = (float(k1h[-1][1]) - float(k1h[-1][4])) / (float(k1h[-1][1]) + 1e-9) * 100
    drop_prev  = (float(k1h[-2][1]) - float(k1h[-2][4])) / (float(k1h[-2][1]) + 1e-9) * 100
    cond2 = (chg4h <= -20) and (drop_last > drop_prev)

    return cond1 or cond2


def _two_consecutive_reversal_15m(k15: list, direction: str) -> list | None:
    """
    v3.3.5: 放宽为「最新一根满足止跌/止涨形态」即可（原要求连续两根）。
    保留额外要求：最后已闭合K线收盘价必须站上/站下最近8根15M K线的均价（止跌备件）。
    返回描述列表（信号名称），找不到返回 None。
    """
    if len(k15) < 3: return None
    recent = k15[-4:]  # 取最近4根备用

    def _is_reversal_candle(k, k_prev, direction):
        o  = float(k[1]); h = float(k[2]); lo = float(k[3]); c = float(k[4])
        po = float(k_prev[1]); pc = float(k_prev[4])
        body   = abs(c - o) + 1e-9
        l_shad = min(o,c) - lo
        u_shad = h - max(o,c)
        if direction == "LONG":
            if l_shad > body * 2 and c > (h+lo)/2: return "hammer"
            if c > po and o < pc:                   return "engulfing"
            body_r = abs(c - o) / (h - lo + 1e-9)
            if c > o and c > pc and body_r > 0.5: return "bullish_close"  # v3.6: 实体须占振幅50%+
        else:
            if u_shad > body * 2 and c < (h+lo)/2: return "shooting_star"
            if c < po and o > pc:                   return "engulfing_bear"
            body_r = abs(c - o) / (h - lo + 1e-9)
            if c < o and c < pc and body_r > 0.5: return "bearish_close"  # v3.6: 实体须占振幅50%+
        return None

    # v3.3.5: 只需最新已闭合一根（k15[-1]）满足即可
    if len(recent) < 2:
        return None
    last_sig = _is_reversal_candle(recent[-1], recent[-2], direction)
    if last_sig is None:
        return None

    result_signals = [last_sig]

    # ── 站稳确认：最后已闭合K线收盘价需站上/站下近12根15M均价 ───────────────
    # v3.64: 8→12根，急跌后第一根反弹K线收盘往往仍在8根MA下方（MA滞后），
    # 导致有效reversal信号被误杀。扩展到12根让MA更充分反映跌前基准价位。
    if len(k15) >= 12:
        ma12_closes = [float(k[4]) for k in k15[-12:]]
        ma12 = sum(ma12_closes) / len(ma12_closes)
        last_close = float(k15[-1][4])
        if direction == "LONG" and last_close < ma12:
            return None   # 价格未站上12根均价,不算真正止跌
        if direction == "SHORT" and last_close > ma12:
            return None   # 价格未站下12根均价,不算真正止涨

    return result_signals
def _deceleration_1h(k1h: list, direction: str) -> bool:
    """最近一根1H K线的涨/跌幅小于前一根(或为反向)"""
    if len(k1h) < 2: return True
    k_last = k1h[-1]; k_prev = k1h[-2]
    if direction == "LONG":
        # 阳线自动满足
        if float(k_last[4]) >= float(k_last[1]): return True
        drop_last = (float(k_last[1]) - float(k_last[4])) / (float(k_last[1]) + 1e-9)
        drop_prev = (float(k_prev[1]) - float(k_prev[4])) / (float(k_prev[1]) + 1e-9)
        return drop_last < drop_prev
    else:
        if float(k_last[4]) <= float(k_last[1]): return True
        rise_last = (float(k_last[4]) - float(k_last[1])) / (float(k_last[1]) + 1e-9)
        rise_prev = (float(k_prev[4]) - float(k_prev[1])) / (float(k_prev[1]) + 1e-9)
        return rise_last < rise_prev


def _deceleration_2_consecutive_1h(k1h: list, direction: str) -> bool:
    """高波动层:最近两根1H K线的涨/跌幅都在递减"""
    if len(k1h) < 3: return True
    k1 = k1h[-3]; k2 = k1h[-2]; k3 = k1h[-1]
    if direction == "LONG":
        d1 = max(0, (float(k1[1]) - float(k1[4])) / (float(k1[1]) + 1e-9))
        d2 = max(0, (float(k2[1]) - float(k2[4])) / (float(k2[1]) + 1e-9))
        d3 = max(0, (float(k3[1]) - float(k3[4])) / (float(k3[1]) + 1e-9))
        return d2 < d1 and d3 < d2
    else:
        r1 = max(0, (float(k1[4]) - float(k1[1])) / (float(k1[1]) + 1e-9))
        r2 = max(0, (float(k2[4]) - float(k2[1])) / (float(k2[1]) + 1e-9))
        r3 = max(0, (float(k3[4]) - float(k3[1])) / (float(k3[1]) + 1e-9))
        return r2 < r1 and r3 < r2


def _extra_short_structure(k1h: list, price: float, atr: float,
                            bb_pct: float, ind: dict) -> bool:
    """
    做空额外结构确认,满足以下任意一项:
    1. 价格从最近4H高点回落超过 1.0 ATR
    2. BB% > 90
    3. OI 最近1H下降超过5%
    """
    # 条件1
    high_4h = max(float(k[2]) for k in k1h[-4:]) if len(k1h) >= 4 else price
    if (high_4h - price) > atr * 1.0:
        return True
    # 条件2
    if bb_pct > 0.90:
        return True
    # 条件3:从 OI 缓存近似
    symbol = ind.get("symbol", "")
    if symbol:
        oi_1h_ago = _get_oi_1h_ago(symbol)
        oi_now    = ind.get("oi_now", 0)
        if oi_1h_ago and oi_now and oi_1h_ago > 0:
            oi_drop = (oi_1h_ago - oi_now) / oi_1h_ago * 100
            if oi_drop >= 5:
                return True
    return False


def _get_oi_1h_ago(symbol: str) -> float | None:
    """从 OI 缓存中取约1小时前的 OI"""
    try:
        from shared.state import get_oi_15min_ago as _helper  # noqa: F401
        from shared.state import _load_oi_cache
        cache  = _load_oi_cache()
        series = cache.get(symbol)
        if not series: return None
        target_ms = (time.time() - 3600) * 1000   # 统一毫秒,与 state.py update_oi 一致
        best   = min(series, key=lambda r: abs(r["timestamp"] - target_ms))
        if abs(best["timestamp"] - target_ms) > 1200 * 1000: return None
        return float(best["oi"])
    except Exception:
        return None


def _fill_reversal_liq(out: dict, price: float, direction: str,
                        atr: float, symbol: str) -> None:
    """填充 reversal 清算区字段(从 liq_cache 读缓存)"""
    try:
        if not symbol: return
        from shared.liq_cache import get_liq_zones_cached as get_liq_zones
        if direction == "LONG":
            zones = get_liq_zones(symbol, "LONG", price, range_="24h")
            total = sum(z.get("liq_usd", 0) for z in zones
                        if 0 < price - z["price_high"] < atr * 2)
            out["liq_zone_below_amount"] = round(total, 0)
            if total >= 3_000_000:
                out["position_size_cap"] = 0.5 if out.get("extreme_candle_flag") else 0.75
                # v3.6 bugfix: reversal SHORT 不设 pending_order（清算区可能在价格上方很远，永不成交）
                out["entry_mode"] = "immediate"
            elif total >= 1_000_000:
                out["position_size_cap"] = 0.5
        else:
            zones = get_liq_zones(symbol, "SHORT", price, range_="24h")
            total = sum(z.get("liq_usd", 0) for z in zones
                        if 0 < z["price_low"] - price < atr * 2)
            out["liq_zone_above_amount"] = round(total, 0)
            if total >= 3_000_000:
                out["position_size_cap"] = 0.5 if out.get("extreme_candle_flag") else 0.75
            elif total >= 1_000_000:
                out["position_size_cap"] = 0.5
    except Exception as _rv_e:
        # [v3.89 #29] 不再静默：清算字段按无效处理并记录失败原因。
        #   注意 position_size_cap / entry_mode 的既有分支逐行未动，
        #   本处只影响清算金额字段，不改变仓位上限与入场模式语义。
        out["liq_zone_error"] = type(_rv_e).__name__
        logger.debug(f"[judges] _fill_reversal_liq 计算失败（清算字段按无效处理）: {_rv_e}")


def _set_cooldown_safe(symbol: str, hours: float, reason: str) -> None:
    try:
        from shared.state import set_cooldown
        set_cooldown(symbol, hours, reason)
    except Exception as e:
        # [v3.89 #28] 冷却写入失败不再静默丢弃：调用方依赖的冷却可能没生效，必须留下记录
        logger.warning(f"[judges] 冷却写入失败，冷却可能未生效 {symbol} {hours}h reason={reason}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 评分内部一致性检查(v3.3.2 同步自 entry_logic.py)
# 只记录日志,不拒绝信号,供 analytics.py 分析矛盾信号实际表现
# ─────────────────────────────────────────────────────────────────────────────
def check_signal_consistency(symbol: str, ind: dict, chg24: float,
                              funding: float, sig_type: str) -> None:
    """检测评分矛盾信号,写入 ws_cache/consistency_log.jsonl"""
    try:
        import json, datetime, pathlib
        rsi_4h = ind.get("rsi_4h", ind.get("rsi_1h", 50))
        vr     = ind.get("vol_ratio", 1.0)
        flags  = []

        # 矛盾一:RSI超买 + 大涨幅
        if rsi_4h > 68 and chg24 > 5:
            flags.append(f"RSI超买({rsi_4h:.0f})+大涨幅({chg24:.1f}%)")
        # 矛盾二:RSI超卖 + 连续下跌
        if rsi_4h < 30 and chg24 < -5:
            flags.append(f"RSI超卖({rsi_4h:.0f})+连续下跌({chg24:.1f}%)")
        # 矛盾三:资金费率偏高 + 放量
        if funding > 0.001 and vr > 1.8:
            flags.append(f"资金费率偏高({funding*100:.3f}%)+放量({vr:.1f}x)")

        if not flags:
            return

        entry = {
            "ts":        datetime.datetime.utcnow().isoformat(),
            "sym":       symbol,
            "sig_type":  sig_type,
            "flags":     flags,
            "rsi_4h":    round(rsi_4h, 1),
            "chg24":     round(chg24, 2),
            "funding":   round(funding, 5),
            "vr":        round(vr, 2),
        }
        clog = pathlib.Path(__file__).parent.parent / "ws_cache" / "consistency_log.jsonl"
        clog.parent.mkdir(parents=True, exist_ok=True)
        with open(clog, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.debug(f"[consistency] {symbol} 矛盾信号: {flags}")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# judge_funding_arb - 资金费率套利型（第四套信号场景）
# ══════════════════════════════════════════════════════════════════════════════
def judge_funding_arb(symbol: str, funding: float, funding_hist: list,
                      ind: dict, next_settle_ts: float, k1h: list = None, k4h: list = None) -> tuple[bool, dict]:
    """[v3.89] 外层异常包装（与 #26/#27 一致）：内部异常统一返回 internal_error 结构并记录带上下文的日志，
    不再向调用方抛出；判定逻辑见 _judge_funding_arb_impl，拒绝条件未改。"""
    try:
        return _judge_funding_arb_impl(symbol, funding, funding_hist, ind, next_settle_ts, k1h, k4h)
    except Exception as e:
        _ctx = dict(ind) if isinstance(ind, dict) else {}
        _ctx.setdefault("symbol", symbol)
        _log_judge_failure("judge_funding_arb", e, k1h, k4h, None, _ctx)
        return _judge_error_result("judge_funding_arb", "funding_arb", e)


def _judge_funding_arb_impl(symbol: str, funding: float, funding_hist: list,
                            ind: dict, next_settle_ts: float, k1h: list = None, k4h: list = None) -> tuple[bool, dict]:
    """
    判断是否满足资金费率套利开仓条件。
    参数:
      symbol          : 标的符号
      funding         : 当前资金费率（小数，如 0.001 = 0.1%）
      funding_hist    : 近3期历史费率列表（旧→新），float 列表
      ind             : calc_indicators 返回的指标 dict
      next_settle_ts  : 下次资金费率结算时间戳（秒）
    返回:
      (ok, detail_dict)
    """
    try:
        from config import (
            FUNDING_ARB_MIN_RATE_SHORT,   # [v3.87 STEP3] STRONG_RATE_* 在后面强度分级处导入，此处重复且未使用，已移除
            FUNDING_ARB_RATE_HIST_MIN_SHORT,
            FUNDING_ARB_MIN_RATE_LONG,
            FUNDING_ARB_RATE_HIST_MIN_LONG,
            FUNDING_ARB_RSI4H_SHORT_MIN, FUNDING_ARB_RSI4H_SHORT_MAX,
            FUNDING_ARB_RSI4H_LONG_MIN,  FUNDING_ARB_RSI4H_LONG_MAX,
            FUNDING_ARB_OI_DROP_LIMIT,
            FUNDING_ARB_ENTRY_WINDOW_MIN_H,
            FUNDING_ARB_RSI4H_EXTREME, FUNDING_ARB_RSI4H_HIGH,
        )
    except Exception as _ce:
        return False, {"reject_reason": f"config import 失败: {_ce}"}

    import time as _t
    rsi_4h   = ind.get("rsi_4h", 50)
    atr      = ind.get("atr_1h", 0)
    oi_now   = ind.get("oi_now", 0)   # [v3.87 STEP3] 删除未使用的 price

    out = {
        "mode":              "funding_arb",
        "sig_type":          "funding_arb",
        "symbol":            symbol,
        "funding_rate":      funding,
        "funding_hist":      funding_hist,
        "rsi_4h":            rsi_4h,
        "next_settle_ts":    next_settle_ts,
        "signal_strength":   "medium",
        "position_size_cap": 0.3,
        "reject_reason":     None,
        "direction":         None,
    }

    # ── v3.40 距结算时间窗口检查 ───────────────────────────────────────────────
    # 逻辑：越接近结算入场越好（承受价格风险时间更短），但不能太近来不及挂单
    # 入场上限：结算前2h（普通费率） / 4h（极端费率≥1%，费率够高可承受更长风险）
    # 入场下限：结算前5min（来不及挂单）
    # 结算后冷却：5min 内不重新开仓（续持由 _check_arb_post_settle 处理）
    now_ts = _t.time()
    hours_to_settle = (next_settle_ts - now_ts) / 3600
    min_hours = FUNDING_ARB_ENTRY_WINDOW_MIN_H   # 默认 5/60 h ≈ 5min
    funding_interval_h = ind.get("funding_interval_h") or out.get("funding_interval_h", 8)
    out["funding_interval_h"] = funding_interval_h
    _fr_total_abs = abs(funding)  # 用于入场窗口放宽判断

    # ① 结算后冷却期：5min 内不开新仓（续持由 patrol 管理）
    if hours_to_settle < 0:
        cool_min = abs(hours_to_settle) * 60
        if cool_min < 5:
            out["reject_reason"] = f"结算后冷却期：刚结算 {cool_min:.0f}min，等待5min后再评估"
            return False, out

    # ② 距结算 < min_hours（默认5分钟），太近来不及挂单
    if 0 < hours_to_settle < min_hours:
        out["reject_reason"] = (
            f"距结算时间过短：{hours_to_settle*60:.0f}min < {min_hours*60:.0f}min，即将结算不适合入场"
        )
        return False, out

    # ③ v3.40 入场上限：结算前 N 小时内才入场，减少承受价格风险时间
    if hours_to_settle > 0:  # 只在结算前有效（结算后 = 新周期已开始，不受限）
        if _fr_total_abs >= 0.01:
            _max_entry_h = 4.0   # 极端费率（≥1%）：允许4h前入场
        else:
            _max_entry_h = 2.0   # 普通高费率：只允许2h前入场
        if hours_to_settle > _max_entry_h:
            out["reject_reason"] = (
                f"入场窗口限制：距结算 {hours_to_settle:.1f}h > {_max_entry_h:.0f}h，"
                f"等更近时机（费率{_fr_total_abs*100:.3f}%）"
            )
            return False, out

    # ── 方向判断 ───────────────────────────────────────────────────────────────
    if funding >= FUNDING_ARB_MIN_RATE_SHORT:
        direction = "SHORT"
    elif funding <= FUNDING_ARB_MIN_RATE_LONG:
        direction = "LONG"
    else:
        out["reject_reason"] = f"资金费率 {funding*100:.4f}% 不满足任何方向触发门槛"
        return False, out

    out["direction"] = direction

    # ── v3.58 盈亏比校验：费率% > ATR% × 0.5（费率需覆盖半个ATR的价格风险）────
    # 逻辑：atr_ratio 单位为%（如1.5 = 1.5%），funding 为小数（0.001 = 0.1%）
    #       要求：|funding| × 100 > atr_ratio × 0.5
    #       即费率收益率 > 0.5倍ATR，确保单次套利期望为正
    try:
        _atr_r_pct = ind.get("atr_ratio") or (atr / (ind.get("price", 1) + 1e-9) * 100)
        _ = _atr_r_pct
        # [opt-3] 改为固定手续费门槛：开+平 2×0.04% = 0.08%
        if abs(funding) <= 0.0008:
            out["reject_reason"] = f"资金费率{abs(funding)*100:.4f}%不足以覆盖手续费0.08%"
            return False, out
    except Exception:
        pass  # 计算失败降级放行

    # ── 历史费率均值验证 + v3.39 趋势判断 ────────────────────────────────────────
    if funding_hist:
        hist_avg = sum(funding_hist[-3:]) / len(funding_hist[-3:])
        if direction == "SHORT" and hist_avg < FUNDING_ARB_RATE_HIST_MIN_SHORT:
            out["reject_reason"] = f"近3期费率均值 {hist_avg*100:.4f}% < 门槛，可能是单次脉冲"
            return False, out
        if direction == "LONG" and hist_avg > FUNDING_ARB_RATE_HIST_MIN_LONG:
            out["reject_reason"] = f"近3期费率均值 {hist_avg*100:.4f}% > 门槛，可能是单次脉冲"
            return False, out

        # v3.39 费率趋势判断：加剧→升strong / 收窄→降weak
        # 避免在费率即将回归时入场（情况B：-1.5%→-0.8%→-1.33%，明显收窄）
        if len(funding_hist) >= 2:
            _trend = funding_hist[-1] - funding_hist[0]  # 最新 - 最旧（负=更负/加剧 for LONG）
            if direction == "LONG":
                if _trend < -0.001:        # 费率持续加剧（更负）→ 升 strong
                    out["fr_trend"]      = "加剧"
                    out["signal_strength"] = "strong"
                    out["position_size_cap"] = out.get("position_size_cap", 0.3)  # 待信号强度最终覆盖
                elif _trend > 0.002:       # 费率明显收窄（趋向0）→ 降 weak，但不拦截，让门槛过滤
                    out["fr_trend"]      = "收窄"
                    out["signal_strength"] = "weak"
                else:
                    out["fr_trend"]      = "稳定"
            else:  # SHORT
                if _trend > 0.001:         # 费率持续加剧（更正）→ 升 strong
                    out["fr_trend"]      = "加剧"
                    out["signal_strength"] = "strong"
                elif _trend < -0.002:      # 费率明显收窄 → 降 weak
                    out["fr_trend"]      = "收窄"
                    out["signal_strength"] = "weak"
                else:
                    out["fr_trend"]      = "稳定"
            logger.debug(
                f"[judge_arb] {symbol} 费率趋势={out['fr_trend']} "
                f"hist={[f'{r*100:.3f}%' for r in funding_hist]} "
                f"→ strength={out['signal_strength']}"
            )

    # ── RSI4H 动态门槛（v3.37）──────────────────────────────────────────────────
    # 原因：费率极端时价格往往在高低位，RSI4H 自然偏高/低，固定门槛会系统性拒绝高费率标的
    _fr_abs = abs(funding)
    if direction == "LONG":
        # 做多套利（收负费率）：RSI4H 不能太高（防止追顶）
        if _fr_abs >= FUNDING_ARB_RSI4H_EXTREME:   # 费率 ≥ 1%：极端，放宽到 70
            _rsi4h_max = 70
        elif _fr_abs >= FUNDING_ARB_RSI4H_HIGH:     # 费率 ≥ 0.5%：较高，放宽到 60
            _rsi4h_max = 60
        else:
            _rsi4h_max = FUNDING_ARB_RSI4H_LONG_MAX  # 正常费率，维持保守门槛
        if rsi_4h < FUNDING_ARB_RSI4H_LONG_MIN:
            out["reject_reason"] = (
                f"RSI4H={rsi_4h:.1f} < {FUNDING_ARB_RSI4H_LONG_MIN}，进入了极度超卖区间"
            )
            return False, out
        if rsi_4h > _rsi4h_max:
            out["reject_reason"] = (
                f"RSI4H={rsi_4h:.1f} > {_rsi4h_max}"
                f"（费率{_fr_abs*100:.3f}% 对应门槛{_rsi4h_max}）"
            )
            return False, out
    else:  # SHORT
        # 做空套利（收正费率）：RSI4H 不能太低（防止追底）
        if _fr_abs >= FUNDING_ARB_RSI4H_EXTREME:   # 费率 ≥ 1%：极端，放宽到 30
            _rsi4h_min = 30
        elif _fr_abs >= FUNDING_ARB_RSI4H_HIGH:     # 费率 ≥ 0.5%：较高，放宽到 40
            _rsi4h_min = 40
        else:
            _rsi4h_min = FUNDING_ARB_RSI4H_SHORT_MIN  # 正常费率，维持保守门槛
        if rsi_4h > FUNDING_ARB_RSI4H_SHORT_MAX:
            out["reject_reason"] = (
                f"RSI4H={rsi_4h:.1f} > {FUNDING_ARB_RSI4H_SHORT_MAX}，进入了极度超买区间"
            )
            return False, out
        if rsi_4h < _rsi4h_min:
            out["reject_reason"] = (
                f"RSI4H={rsi_4h:.1f} < {_rsi4h_min}"
                f"（费率{_fr_abs*100:.3f}% 对应门槛{_rsi4h_min}）"
            )
            return False, out

    # ── v3.40 OI 变化方向检查（双向）──────────────────────────────────────────
    # 做多套利（收负费率）：OI 上升=空头持续加仓，费率持续性强；OI 大幅下降=空头平仓，费率将回归
    # 做空套利（收正费率）：OI 下降=多头去杠杆，费率持续性强；OI 大幅上升=新多头进场，费率将收窄
    oi_1h_ago = ind.get("oi_1h_ago", 0)
    if oi_1h_ago > 0 and oi_now > 0:
        oi_change_pct = (oi_now - oi_1h_ago) / (oi_1h_ago + 1e-9) * 100
        out["oi_change_pct"] = round(oi_change_pct, 2)
        if direction == "LONG":
            # 做多套利：OI 大幅下降=空头在平仓，费率持续性弱 → 拦截
            if oi_change_pct < -(FUNDING_ARB_OI_DROP_LIMIT * 100):
                out["reject_reason"] = (
                    f"OI 近1H下降 {abs(oi_change_pct):.1f}% > {FUNDING_ARB_OI_DROP_LIMIT*100:.0f}%，"
                    f"空头在平仓，费率持续性弱"
                )
                return False, out
            # OI 上升=空头加仓，费率持续性强 → 记录为加分信号（不直接影响 strength，但写入 out）
            if oi_change_pct > 5:
                out["oi_trend"] = "空头加仓"
                logger.debug(f"[judge_arb] {symbol} OI+{oi_change_pct:.1f}% 空头加仓，费率持续性强")
        else:  # SHORT
            # 做空套利：OI 大幅上升=新多头进场，费率可能收窄 → 拦截
            _oi_rise_limit = FUNDING_ARB_OI_DROP_LIMIT * 100  # 复用同一阈值
            if oi_change_pct > _oi_rise_limit:
                out["reject_reason"] = (
                    f"OI 近1H上升 {oi_change_pct:.1f}% > {_oi_rise_limit:.0f}%，"
                    f"新多头进场，正费率可能快速收窄"
                )
                return False, out
            # 做空：OI 下降=多头去杠杆，做空套利持续性更强
            if oi_change_pct < -5:
                out["oi_trend"] = "多头去杠杆"
                logger.debug(f"[judge_arb] {symbol} OI{oi_change_pct:.1f}% 多头去杠杆，做空套利持续性强")

    # ── 过滤一：EMA 趋势过滤（防止接飞刀/追顶）────────────────────────────────────
    try:
        from config import FUNDING_ARB_TREND_FILTER
    except Exception:
        FUNDING_ARB_TREND_FILTER = True
    
    if FUNDING_ARB_TREND_FILTER:
        ema20_1h = ind.get("ema20_1h", 0)
        ema50_1h = ind.get("ema50_1h", 0)
        if ema20_1h > 0 and ema50_1h > 0:
            if direction == "LONG" and ema20_1h < ema50_1h:
                out["reject_reason"] = f"EMA趋势过滤：1H空头排列(EMA20={ema20_1h:.5g} < EMA50={ema50_1h:.5g})，不做多funding_arb（接飞刀风险）"
                return False, out
            if direction == "SHORT" and ema20_1h > ema50_1h:
                out["reject_reason"] = f"EMA趋势过滤：1H多头排列(EMA20={ema20_1h:.5g} > EMA50={ema50_1h:.5g})，不做空funding_arb（追顶风险）"
                return False, out

    # ── 过滤二：近期振幅过滤（极端波动行情，资费收益覆盖不了价格风险）────────────────
    try:
        from config import FUNDING_ARB_AMPLITUDE_LIMIT
    except Exception:
        FUNDING_ARB_AMPLITUDE_LIMIT = 30.0
    
    if FUNDING_ARB_AMPLITUDE_LIMIT > 0:
        high_20 = ind.get("high_20", 0)
        low_20 = ind.get("low_20", 0)
        if high_20 > 0 and low_20 > 0:
            amplitude = (high_20 - low_20) / (low_20 + 1e-9) * 100
            if amplitude > FUNDING_ARB_AMPLITUDE_LIMIT:
                out["reject_reason"] = f"振幅过滤：近20H振幅{amplitude:.1f}% > {FUNDING_ARB_AMPLITUDE_LIMIT}%，波动过大，费率收益无法覆盖价格风险"
                return False, out

    # ── 过滤三：脉冲大阳保护（庄家拉盘后暴跌，不接盘做多）─────────────────────────
    try:
        from config import FUNDING_ARB_PULSE_FILTER_ENABLED
    except Exception:
        FUNDING_ARB_PULSE_FILTER_ENABLED = True
    
    if FUNDING_ARB_PULSE_FILTER_ENABLED and k1h and k4h:
        _pulse = _is_pulse_candle(k1h, k4h)
        if _pulse:
            out["reject_reason"] = f"脉冲大阳保护：拒绝funding_arb {direction}, {_pulse}"
            return False, out

    # ── 信号强度分级（v3.39 整合趋势判断）────────────────────────────────────────
    # 优先级：费率绝对值 strong > 趋势 strong/weak > 费率绝对值 medium
    from config import FUNDING_ARB_STRONG_RATE_SHORT, FUNDING_ARB_STRONG_RATE_LONG, FUNDING_ARB_SIZE_STRONG, FUNDING_ARB_SIZE_MEDIUM
    _trend_strength = out.get("signal_strength")  # 趋势判断可能已设 strong/weak/medium
    if direction == "SHORT" and funding >= FUNDING_ARB_STRONG_RATE_SHORT:
        # 费率绝对值达到 strong 门槛，且趋势没有收窄 → strong
        if _trend_strength == "weak":
            out["signal_strength"]   = "medium"  # 费率强但趋势收窄，降为 medium
            out["position_size_cap"] = FUNDING_ARB_SIZE_MEDIUM
        else:
            out["signal_strength"]   = "strong"
            out["position_size_cap"] = FUNDING_ARB_SIZE_STRONG
    elif direction == "LONG" and funding <= FUNDING_ARB_STRONG_RATE_LONG:
        if _trend_strength == "weak":
            out["signal_strength"]   = "medium"
            out["position_size_cap"] = FUNDING_ARB_SIZE_MEDIUM
        else:
            out["signal_strength"]   = "strong"
            out["position_size_cap"] = FUNDING_ARB_SIZE_STRONG
    else:
        # 费率只到 medium 门槛：趋势加剧可以升 strong，趋势收窄维持 medium（weak 留给 judge 外部用）
        if _trend_strength == "strong":
            out["signal_strength"]   = "strong"
            out["position_size_cap"] = FUNDING_ARB_SIZE_STRONG
        else:
            out["signal_strength"]   = "medium"
            out["position_size_cap"] = FUNDING_ARB_SIZE_MEDIUM


    # [add] 补写 volatility_tier / atr_ratio（供 signal_analytics 完整记录，低优先级）
    # funding_arb 的 calc_sl_tp 分支在 tier 判断前已 return，不影响交易逻辑
    _fa_atr   = ind.get("atr_1h", 0)
    _fa_price = ind.get("price", 1) or 1
    out["volatility_tier"] = _volatility_tier(_fa_atr, _fa_price)
    out["atr_ratio"]       = round(_atr_ratio(_fa_atr, _fa_price), 4)
    return True, out


# ═══════════════════════════════════════════════════════════════════════════════
# judge_range_bounce - 区间底部反弹型（v3.35）
# 识别「大涨→横盘→从区间底部反弹」形态，chg24/RSI 极端值不是前提条件
# ═══════════════════════════════════════════════════════════════════════════════

def judge_range_bounce(k1h: list, k4h: list, k15: list, ind: dict,
                       chg24: float, funding: float):
    try:
        return _judge_range_bounce_impl(k1h, k4h, k15, ind, chg24, funding)
    except Exception as e:
        _log_judge_failure("judge_range_bounce", e, k1h, k4h, k15, ind)
        return _judge_error_result("judge_range_bounce", "range_bounce", e)


def _judge_range_bounce_impl(k1h, k4h, k15, ind, chg24, funding):
    price  = ind["price"]
    atr_r  = ind.get("atr_ratio", 0)
    rsi_1h = ind.get("rsi_1h", 50)

    out = {
        "mode":              "range_bounce",
        "direction":         "LONG",   # 只做多
        "signal_strength":   "medium",
        "position_size_cap": 0.3,
        "reject_reason":     None,
    }
    _crash = _btc_crash_reject(ind, "LONG", "range_bounce", out)
    if _crash is not None:
        return _crash

    closes = [float(k[4]) for k in k1h[:-1]]  # 已闭合 K 线
    if len(closes) < 15:
        out["reject_reason"] = "K线数据不足(<15根)"
        return False, out

    # ── 1. 近12根内找大涨高点（涨幅 ≥ 15%）──────────────────────────────
    recent12 = closes[-12:]
    peak_val = max(recent12)
    peak_local_idx = recent12.index(peak_val)
    peak_abs_idx   = len(closes) - 12 + peak_local_idx
    base_start     = max(0, peak_abs_idx - 6)
    base_val       = closes[base_start]
    pre_gain       = (peak_val - base_val) / (base_val + 1e-9) * 100
    if pre_gain < 15:
        out["reject_reason"] = f"大涨幅度不足: pre_gain={pre_gain:.1f}%<15%"
        return False, out

    # ── 2. 高点之后横盘区间（振幅 ≤ 20%，至少2根）───────────────────────
    post_peak_closes = closes[peak_abs_idx:]
    if len(post_peak_closes) < 2:
        out["reject_reason"] = "高点后K线数不足(<2根)，尚未形成横盘"
        return False, out
    c_high  = max(post_peak_closes)
    c_low   = min(post_peak_closes)
    c_range = (c_high - c_low) / (c_low + 1e-9) * 100
    if c_range > 20:
        out["reject_reason"] = f"横盘区间过宽: {c_range:.1f}%>20%，非横盘而是继续波动"
        return False, out

    # ── 3. 当前价在横盘区间下半部（距下沿 40% 以内）────────────────────
    zone_mid = (c_high + c_low) / 2
    if price > zone_mid:
        out["reject_reason"] = f"价格({price:.5g})在区间上半部，不是底部做多时机"
        return False, out

    # ── 4. ATR 门槛（有足够波动空间，周末降低到 1.5%）─────────────────
    _atr_min_rb = 1.5 if ind.get("_weekend_mode") else 2.0
    if atr_r < _atr_min_rb:
        out["reject_reason"] = f"ATR%={atr_r:.2f}%<{_atr_min_rb}%，波动不足"
        return False, out

    # ── 5. RSI 不超买不超卖（中性区间 30~75）────────────────────────────
    if rsi_1h > 75:
        out["reject_reason"] = f"RSI1H={rsi_1h:.1f}>75，价格偏强，已非底部"
        return False, out
    if rsi_1h < 25:
        out["reject_reason"] = f"RSI1H={rsi_1h:.1f}<25，已超卖，应走reversal路径"
        return False, out

    # ── 6. 4H 不能是强空头（防区间下沿变下跌中继）───────────────────────
    ema20_4h = ind.get("ema20_4h", 0)
    ema50_4h = ind.get("ema50_4h", 0)
    gap_4h   = 0.0
    if ema20_4h > 0 and ema50_4h > 0:
        gap_4h = (ema20_4h - ema50_4h) / (ema50_4h + 1e-9) * 100
        if gap_4h < -5.0:
            out["reject_reason"] = f"4H强空头(gap={gap_4h:.1f}%<-5%)，支撑可能是下跌中继"
            return False, out

    # ── 7. 脉冲大阳保护（不在庄家拉盘途中追多）──────────────────────────
    _pulse = _is_pulse_candle(k1h, k4h)
    if _pulse:
        out["reject_reason"] = f"脉冲大阳保护: {_pulse}"
        return False, out

    # ── 8. 15M 止跌信号确认（区间底部企稳）─────────────────────────────
    rev_sig = _reversal_signal_15m(k15, "LONG")
    if rev_sig is None:
        out["reject_reason"] = "15M无止跌企稳形态，等待确认"
        return False, out
    out["reversal_signal_15m"] = rev_sig

    # ── 9. 资金费率（多头不能太拥挤）────────────────────────────────────
    if funding > 0.001:
        out["reject_reason"] = f"资金费率过高({funding*100:.4f}%)，多头拥挤"
        return False, out

    # ── 信号强度分级 ─────────────────────────────────────────────────────
    # 4H 多头趋势 → strong(0.5)；4H 中性 → medium(0.3)
    if gap_4h > 0:
        out["signal_strength"]   = "strong"
        out["position_size_cap"] = 0.5
    else:
        out["signal_strength"]   = "medium"
        out["position_size_cap"] = 0.3

    out["pre_gain"]     = round(pre_gain, 1)
    out["c_range"]      = round(c_range, 1)
    out["c_high"]       = round(c_high, 8)   # 区间上沿，供 calc_sl_tp range_bounce TP1 锚定
    # [v3.87 STEP5-1] 新增 c_low（区间下沿）。signal_watcher._survival_check 的 range_bounce 做多撤单条件读取 reason['c_low']，
    #   但此前判定函数从未输出该字段，值恒为 0，「区间下沿被跌破则撤单」条件从未生效。只加字段，不改任何判定逻辑。
    out["c_low"]        = round(c_low, 8)
    out["zone_mid"]     = round(zone_mid, 6)
    out["gap_4h"]       = round(gap_4h, 1)
    # [fix] range_bounce 漏写 volatility_tier，导致 calc_sl_tp 回退 mid 规则
    # atr_ratio>6% 的高波动币被错误用 cap=15%/RR_min=2.0（应为 cap=25%/RR_min=1.5），
    # 致使所有通过 judge 的 range_bounce 信号在 calc_sl_tp 被静默丢弃。
    out["volatility_tier"] = _volatility_tier(ind.get("atr_1h", 0), price)
    out["atr_ratio"]       = round(atr_r, 4)
    return True, out


# ══════════════════════════════════════════════════════════════════════════════
# judge_stop_hunt - 止损猎杀反转型（v3.49）
# ══════════════════════════════════════════════════════════════════════════════

def judge_stop_hunt(k1h: list, k4h: list, k15: list, ind: dict,
                    chg24: float, funding: float) -> tuple[bool, dict]:
    """
    止损猎杀反转型信号判断。
    触发逻辑：
      1. 确认有效前高/前低（20根1H K线，左右各3根确认）
      2. 15M K线刺穿 > 0.3%，且成交量 > 均量 1.5x
      3. 刺穿后 ≤3根15M（工作日）/ ≤4根（周末）内收回
      4. 当前价距前高/前低 ≤ 1%
    """
    try:
        return _judge_stop_hunt_impl(k1h, k4h, k15, ind, chg24, funding)
    except Exception as e:
        _log_judge_failure("judge_stop_hunt", e, k1h, k4h, k15, ind)
        return _judge_error_result("judge_stop_hunt", "stop_hunt", e)


def _judge_stop_hunt_impl(k1h, k4h, k15, ind, chg24, funding):
    # ── 加载配置 ────────────────────────────────────────────────────────────
    try:
        import sys as _sys, pathlib as _pl
        _sys.path.insert(0, str(_pl.Path(__file__).parent.parent))
        from config import (
            STOP_HUNT_LOOKBACK_1H       as _LB1H,
            STOP_HUNT_CONFIRM_BARS      as _CONF_BARS,
            STOP_HUNT_PIERCE_PCT        as _PIERCE_PCT,
            STOP_HUNT_VOL_RATIO_MIN     as _VOL_MIN,
            STOP_HUNT_VOL_RATIO_MIN_WKND as _VOL_MIN_WKND,
            STOP_HUNT_VOL_LOOKBACK_15M  as _VOL_LB,
            STOP_HUNT_RECOVER_BARS      as _REC_BARS,
            STOP_HUNT_RECOVER_BARS_WKND as _REC_BARS_WKND,
            STOP_HUNT_MAX_DIST_PCT      as _MAX_DIST,
            STOP_HUNT_VOL_STRONG        as _VOL_STRONG,
            STOP_HUNT_RECOVER_STRONG    as _REC_STRONG,
            STOP_HUNT_SL_BUFFER         as _SL_BUF,
            STOP_HUNT_CHG24_MAX         as _CHG24_MAX,
        )
    except Exception:
        _LB1H       = 20
        _CONF_BARS  = 3
        _PIERCE_PCT = 0.003
        _VOL_MIN    = 1.5
        _VOL_MIN_WKND = 1.2
        _VOL_LB     = 20
        _REC_BARS   = 3
        _REC_BARS_WKND = 4
        _MAX_DIST   = 0.01
        _VOL_STRONG = 2.5
        _REC_STRONG = 2
        _SL_BUF     = 0.003
        _CHG24_MAX  = 15.0

    _is_weekend = ind.get("_weekend_mode", False)
    vol_min     = _VOL_MIN_WKND if _is_weekend else _VOL_MIN
    rec_bars    = _REC_BARS_WKND if _is_weekend else _REC_BARS

    out = {
        "mode":           "stop_hunt",
        "reject_reason":  None,
        "direction":      None,
        "signal_strength": None,
        "strong_count":   0,
        "position_size_cap": 0.3,
        "pierce_pct":     None,
        "vol_ratio":      None,
        "recover_bars":   None,
        "pivot_level":    None,
        "pierce_extreme": None,
        "sl_price_hint":  None,
        "strategy_adj":   0,
        "liq_volume_at_trigger": 0.0,
        "liq_volume_baseline":   0.0,
        "ref_level":             None,   # 被刺穿的前高/前低，最终由 out.update 写入
        "sweep_extreme":          None,   # 刺穿极端点，最终由 out.update 写入
    }

    price = ind.get("price", 0.0)

    # ── 0. chg24 上限（极端行情走 reversal，不走 stop_hunt）────────────────
    if abs(chg24) > _CHG24_MAX:
        out["reject_reason"] = f"chg24={chg24:.1f}% 绝对值>{_CHG24_MAX}%，应走reversal路径"
        return False, out

    # ── 1. 确认有效前高 / 前低（1H已闭合K线）──────────────────────────────
    if len(k1h) < _LB1H + _CONF_BARS + 1:
        out["reject_reason"] = f"k1h不足，len={len(k1h)}"
        return False, out

    closes_1h = _closes(k1h)
    recent_1h = closes_1h[-(  _LB1H + 1):-1]  # 最近_LB1H根已闭合（去掉最后一根未闭合/刺穿K）

    if len(recent_1h) < _LB1H:
        out["reject_reason"] = "1H K线历史不足"
        return False, out

    # 找前低：最低收盘价，且该索引前后各有 _CONF_BARS 根K线（不是边缘点）
    pivot_low_idx  = None
    pivot_low_val  = None
    pivot_high_idx = None
    pivot_high_val = None

    for i in range(_CONF_BARS, len(recent_1h) - _CONF_BARS):
        c = recent_1h[i]
        # 前低：该点是左右 _CONF_BARS 根中的最低
        if all(c <= recent_1h[i - j] for j in range(1, _CONF_BARS + 1)) and \
           all(c <= recent_1h[i + j] for j in range(1, _CONF_BARS + 1)):
            if pivot_low_val is None or c < pivot_low_val:
                pivot_low_val = c
                pivot_low_idx = i
        # 前高：该点是左右 _CONF_BARS 根中的最高
        if all(c >= recent_1h[i - j] for j in range(1, _CONF_BARS + 1)) and \
           all(c >= recent_1h[i + j] for j in range(1, _CONF_BARS + 1)):
            if pivot_high_val is None or c > pivot_high_val:
                pivot_high_val = c
                pivot_high_idx = i

    if pivot_low_val is None and pivot_high_val is None:
        out["reject_reason"] = "找不到有效前高/前低（需左右各3根确认）"
        return False, out

    # ── 1b. pivot 触碰次数检查（硬拒绝）───────────────────────────────────
    # 逻辑：被触碰 N 次以上但未突破的位置，止损单堆积更高，猎杀价値更大
    # 定义：K 线高点或收盘价进入 pivot ±0.2% 范围 = 一次触碰（排除 pivot 所在那根自身）
    _TOUCH_RANGE = 0.002
    _TOUCH_MIN   = 2
    _touch_count = 0
    # 取 k1h 里对应 recent_1h 的原始 K 线（包含 high/low）
    _k1h_window = k1h[-(  _LB1H + 2):-1]   # 与 recent_1h 对齐，多取一根干扰排除
    if pivot_low_val is not None:
        _pl_lo = pivot_low_val * (1 - _TOUCH_RANGE)
        _pl_hi = pivot_low_val * (1 + _TOUCH_RANGE)
        for _ki, _k in enumerate(_k1h_window):
            if _ki == pivot_low_idx:   # 排除 pivot 自身
                continue
            if _pl_lo <= float(_k[3]) <= _pl_hi or _pl_lo <= float(_k[4]) <= _pl_hi:
                _touch_count += 1
        if _touch_count < _TOUCH_MIN:
            out["reject_reason"] = (
                f"insufficient_pivot_touches: 前低 {pivot_low_val:.4g} "
                f"仅被触碰 {_touch_count} 次（需>={_TOUCH_MIN}）"
            )
            return False, out
    elif pivot_high_val is not None:
        _ph_lo = pivot_high_val * (1 - _TOUCH_RANGE)
        _ph_hi = pivot_high_val * (1 + _TOUCH_RANGE)
        for _ki, _k in enumerate(_k1h_window):
            if _ki == pivot_high_idx:  # 排除 pivot 自身
                continue
            if _ph_lo <= float(_k[2]) <= _ph_hi or _ph_lo <= float(_k[4]) <= _ph_hi:
                _touch_count += 1
        if _touch_count < _TOUCH_MIN:
            out["reject_reason"] = (
                f"insufficient_pivot_touches: 前高 {pivot_high_val:.4g} "
                f"仅被触碰 {_touch_count} 次（需>={_TOUCH_MIN}）"
            )
            return False, out
    logger.debug(
        f"[judge_stop_hunt] pivot 触碰次数通过: "
        f"pivot={'low='+str(pivot_low_val) if pivot_low_val else 'high='+str(pivot_high_val)} "
        f"触碰={_touch_count}次"
    )

    # ── 2. 15M K线刺穿检测 ──────────────────────────────────────────────────
    # 需要至少 rec_bars+2 根15M K线（最后一根是当前价格参考，往前推 rec_bars+1 根）
    min_k15 = _VOL_LB + rec_bars + 2
    if len(k15) < min_k15:
        out["reject_reason"] = f"k15不足，len={len(k15)}，需{min_k15}"
        return False, out

    # 最近的已闭合15M K线（去掉最末一根，因为可能未闭合）
    k15_closed = k15[:-1]
    if len(k15_closed) < _VOL_LB + rec_bars + 1:
        out["reject_reason"] = "k15已闭合K线不足"
        return False, out

    # 计算15M均量（用最近 _VOL_LB 根的均值，不含末尾的 rec_bars+1 根）
    k15_vol_slice = k15_closed[-(  _VOL_LB + rec_bars + 1):-(rec_bars + 1)]
    if len(k15_vol_slice) < 5:
        out["reject_reason"] = "k15量能历史不足"
        return False, out
    vol_ma_15m = sum(float(k[5]) for k in k15_vol_slice) / len(k15_vol_slice)
    if vol_ma_15m <= 0:
        out["reject_reason"] = "k15均量为0"
        return False, out

    # 在末尾 rec_bars+1 根里找刺穿K（倒数第 rec_bars+1 ~ 倒数第2根，留最后一根做收回确认）
    window = k15_closed[-(rec_bars + 1):]   # 长度 = rec_bars+1

    pierce_bar_idx = None   # 在 window 里的索引
    pierce_direction = None
    pierce_low  = None      # 刺穿时最低价（LONG方向）
    pierce_high = None      # 刺穿时最高价（SHORT方向）
    pierce_vol_ratio = None

    for wi in range(len(window) - 1):  # 不检查最后一根（留给收回判断）
        bar = window[wi]
        bar_low   = float(bar[3])
        bar_high  = float(bar[2])
        # [v3.87 STEP3] 删除未使用的 bar_close
        bar_vol   = float(bar[5])
        vol_ratio = bar_vol / vol_ma_15m

        # 多头止损猎杀：刺破前低
        if pivot_low_val is not None:
            pierce_threshold = pivot_low_val * (1 - _PIERCE_PCT)
            if bar_low <= pierce_threshold and vol_ratio >= vol_min:
                if pierce_bar_idx is None or vol_ratio > pierce_vol_ratio:
                    pierce_bar_idx      = wi
                    pierce_direction    = "LONG"
                    pierce_low          = bar_low
                    pierce_vol_ratio    = vol_ratio

        # 空头止损猎杀：刺破前高
        if pivot_high_val is not None:
            pierce_threshold = pivot_high_val * (1 + _PIERCE_PCT)
            if bar_high >= pierce_threshold and vol_ratio >= vol_min:
                if pierce_bar_idx is None or vol_ratio > pierce_vol_ratio:
                    pierce_bar_idx      = wi
                    pierce_direction    = "SHORT"
                    pierce_high         = bar_high
                    pierce_vol_ratio    = vol_ratio

    if pierce_bar_idx is None:
        out["reject_reason"] = (
            f"未检测到有效刺穿：前低={pivot_low_val} 前高={pivot_high_val} "
            f"均量={vol_ma_15m:.0f} 量能门槛={vol_min}x"
        )
        return False, out
    _crash = _btc_crash_reject(ind, pierce_direction, "stop_hunt", out)
    if _crash is not None:
        return _crash

    # ── 3. 快速收回确认 ─────────────────────────────────────────────────────
    # 刺穿之后的K线（在 window 里 pierce_bar_idx 后面）
    bars_after_pierce = window[pierce_bar_idx + 1:]
    # 需要至少1根收回K线
    if not bars_after_pierce:
        out["reject_reason"] = "刺穿后无后续K线，无法确认收回"
        return False, out

    recovered = False
    recover_bar_count = 0
    for rb in bars_after_pierce:
        recover_bar_count += 1
        rb_close = float(rb[4])
        if pierce_direction == "LONG" and pivot_low_val is not None:
            if rb_close >= pivot_low_val:
                recovered = True
                break
        elif pierce_direction == "SHORT" and pivot_high_val is not None:
            if rb_close <= pivot_high_val:
                recovered = True
                break

    if not recovered:
        out["reject_reason"] = (
            f"刺穿后{len(bars_after_pierce)}根15M K线内未收回至"
            f"{'前低' if pierce_direction=='LONG' else '前高'}"
            f"（窗口={rec_bars}根）"
        )
        return False, out

    # ── 4. 当前价格位置检查（距前高/前低不超过 1%）──────────────────────────
    pivot_val = pivot_low_val if pierce_direction == "LONG" else pivot_high_val
    dist_pct  = abs(price - pivot_val) / (pivot_val + 1e-9)
    if dist_pct > _MAX_DIST:
        out["reject_reason"] = (
            f"当前价={price:.6g} 距{'前低' if pierce_direction=='LONG' else '前高'}"
            f"={pivot_val:.6g} 偏离{dist_pct*100:.2f}%>{_MAX_DIST*100:.1f}%，信号时机已过"
        )
        return False, out

    # ── 5. BTC 趋势过滤（熊市不做多头止损猎杀）──────────────────────────────
    btc_trend = ind.get("btc_trend", "neutral")
    if btc_trend == "down" and pierce_direction == "LONG":
        out["reject_reason"] = f"BTC趋势空头(btc_trend=down)，熊市不做多头止损猎杀"
        return False, out

    # ── 6+7. Fix1/Fix3（依赖 60 日历史，k1h < 1440 根时强制拒绝 LONG）──────
    # 设计意图：数据不足时不能静默放行，否则对高跌幅标的等于关掉保护
    if pierce_direction == "LONG":
        if len(k1h) < 1440:
            out["reject_reason"] = (
                f"Fix1/Fix3数据不足: k1h仅{len(k1h)}根(<1440)，"
                f"无法判断60日回撤/分位，禁止 stop_hunt LONG"
            )
            return False, out

        closes_60d = _closes(k1h[-1440:])

        # Fix1：60 日峰值回撤 > 40% 禁止 LONG
        peak_60d = max(closes_60d)
        if peak_60d > 0:
            drawdown_60d = (peak_60d - price) / peak_60d
            if drawdown_60d > 0.40:
                out["reject_reason"] = (
                    f"Fix1: 60日峰值回撤{drawdown_60d*100:.1f}%>40%，"
                    f"禁止 stop_hunt LONG"
                )
                return False, out

        # Fix3：60 日分位 < 30% 禁止 LONG
        c_min      = min(closes_60d)
        c_max      = max(closes_60d)
        c_range_60 = c_max - c_min
        if c_range_60 > 0:
            pct_60d = (price - c_min) / c_range_60
            if pct_60d < 0.30:
                out["reject_reason"] = (
                    f"Fix3: 当前价处于60日区间低位{pct_60d*100:.1f}%<30%，"
                    f"禁止 stop_hunt LONG"
                )
                return False, out

    # ── 8a. 4H EMA 方向过滤（v3.50：硬拒绝 → 软惩罚）────────────────────────
    # 旧逻辑：逆势方向直接 return False（硬拒绝）
    # 新逻辑：逆势方向记录 strategy_adj=-15，仍允许进入后续验证（不再硬拒绝）
    #   - 4H 多头结构 + SHORT → strategy_adj=-15
    #   - 4H 空头结构 + LONG  → strategy_adj=-15
    #   - 中性（gap≤0.5%）    → strategy_adj=0
    ema20_4h = float(ind.get("ema20_4h", 0) or 0)
    ema50_4h = float(ind.get("ema50_4h", 0) or 0)
    _4h_strategy_adj = 0
    if ema20_4h > 0 and ema50_4h > 0:
        _ema_gap_pct = (ema20_4h - ema50_4h) / (ema50_4h + 1e-9)
        _4h_bull = _ema_gap_pct > 0.005
        _4h_bear = _ema_gap_pct < -0.005
        if (_4h_bull and pierce_direction == "SHORT") or \
           (_4h_bear and pierce_direction == "LONG"):
            _4h_strategy_adj = -15
            logger.info(
                f"[judge_stop_hunt] 4H逆势 strategy_adj=-15 "
                f"(ema_gap={_ema_gap_pct*100:.1f}% dir={pierce_direction})，允许继续"
            )
    out["strategy_adj"] = out.get("strategy_adj", 0) + _4h_strategy_adj  # [opt-4] 累加

    # ── 8b. ws_listener 缓存强平量验证（v3.50：取代 Coinglass API）──────────
    # 原理：
    #   - LONG 止损猎杀（刺破前低）→ 应有大量多头强平（被扫多单）
    #   - SHORT 止损猎杀（刺破前高）→ 应有大量空头强平（被扫空单）
    # 基准：过去 4H 每 15min 平均强平量（用缓存里最多 16 个 15min 窗口来估算）
    #   ws_listener 缓存是最近 15min 总量，没有历史切片；
    #   我们用「过去 4H 总量 ÷ 16」作为每 15min 基准均值。
    # 门槛：刺穿窗口（当前 15min）强平量 > 基准 × 2.5 才确认信号成立
    # 失败：日志记录 liquidation_volume_insufficient，signal 拒绝返回 False
    # 降级：ws_listener 未注入 / 异常 / 数据为0 → 不阻断，静默继续
    _liq_vol_trigger  = 0.0   # 触发时强平量（写入 out 供 trade_log）
    _liq_vol_baseline = 0.0   # 基准均值（写入 out 供 trade_log）
    try:
        _ws_listener = ind.get("_ws_listener")   # 由 scanner 注入
        if _ws_listener is not None and hasattr(_ws_listener, "get_liquidation_volume"):
            import asyncio as _aio, time as _t
            _sym = ind.get("symbol", "")

            # pierce_direction == 'LONG' 意味着多头被扫，强平方向也是 LONG
            _liq_side = pierce_direction  # 'LONG' or 'SHORT'

            # 统一用直接访问缓存方式（ws_listener._liq_cache）——
            # get_liquidation_volume 只是读 deque，无 IO，无需协程。
            # 这样可以在已运行的事件循环里安全调用。
            _lc = getattr(_ws_listener, "_liq_cache", {})
            _sym_cache = _lc.get(_sym.upper(), {})
            _dq = _sym_cache.get(_liq_side)  # deque[(ts, qty)] 或 None

            if _dq:
                _now = _t.time()
                _cutoff_15 = _now - 15 * 60
                _cutoff_4h = _now - 4 * 60 * 60
                _liq_vol_trigger  = sum(qty for ts, qty in _dq if ts >= _cutoff_15)
                _total_4h         = sum(qty for ts, qty in _dq if ts >= _cutoff_4h)
                _liq_vol_baseline = _total_4h / 16.0  # 16 个 15min 窗口，每窗口均值

            _LIQ_MULT = 2.5
            # [opt-4] 强平量从硬条件改为软加减分（基准不足 1000 不处理）
            _liq_delta = _liq_volume_adj(_liq_vol_trigger, _liq_vol_baseline, _LIQ_MULT)
            out["strategy_adj"] = out.get("strategy_adj", 0) + _liq_delta
            if _liq_delta < 0:
                logger.info(
                    f"[judge_stop_hunt] {_sym} {pierce_direction} "
                    f"强平量不足扣5分: trigger={_liq_vol_trigger/1000:.1f}k "
                    f"baseline={_liq_vol_baseline/1000:.1f}k "
                    f"ratio={_liq_vol_trigger/(_liq_vol_baseline+1e-9):.2f}x < {_LIQ_MULT}x"
                )
            elif _liq_delta > 0:
                logger.info(
                    f"[judge_stop_hunt] {_sym} {pierce_direction} "
                    f"强平量验证通过: trigger={_liq_vol_trigger/1000:.1f}k "
                    f"baseline={_liq_vol_baseline/1000:.1f}k "
                    f"ratio={_liq_vol_trigger/(_liq_vol_baseline+1e-9):.2f}x"
                )
        else:
            logger.debug("[judge_stop_hunt] ws_listener 未注入，强平量验证跳过（降级）")
    except Exception as _liq_e:
        logger.debug(f"[judge_stop_hunt] 强平量验证异常，降级: {_liq_e}")

    out["liq_volume_at_trigger"] = round(_liq_vol_trigger, 0)
    out["liq_volume_baseline"]   = round(_liq_vol_baseline, 0)

    # ── 8c. 横盘积累检查（硬拒绝）─────────────────────────────────────────────────
    # 刺穿前{STOP_HUNT_CONSOLIDATION_BARS}根 1H 已闭合收盘价，必须全部在 pivot_val±CONSOLIDATION_PCT 范围内
    try:
        from config import (
            STOP_HUNT_CONSOLIDATION_BARS as _CONS_BARS,
            STOP_HUNT_CONSOLIDATION_PCT  as _CONS_PCT,
        )
    except Exception:
        _CONS_BARS, _CONS_PCT = 8, 0.02

    if len(k1h) > _CONS_BARS + 4:
        _cons_k1h    = k1h[-(  _CONS_BARS + 2):-1]           # 已闭合的最后 _CONS_BARS 根
        _cons_closes = [float(k[4]) for k in _cons_k1h[-_CONS_BARS:]]
        _cons_low    = pivot_val * (1 - _CONS_PCT)
        _cons_high   = pivot_val * (1 + _CONS_PCT)
        _out_count   = sum(1 for c in _cons_closes if not (_cons_low <= c <= _cons_high))
        if _out_count > 0:
            out["reject_reason"] = (
                f"no_consolidation_before_sweep: 刺穿前{_CONS_BARS}根 1H 中有 {_out_count} 根"
                f"收盘价超出 pivot({pivot_val:.4g})±{_CONS_PCT*100:.0f}%"
            )
            logger.info(
                f"[judge_stop_hunt] 横盘积累不足拒绝: "
                f"pivot={pivot_val:.4g} 范围=[{_cons_low:.4g},{_cons_high:.4g}] "
                f"超出{_out_count}/{_CONS_BARS}根"
            )
            return False, out
    # k1h 数据不足时降级，不拒绝

    # ── 8d. 资金费率方向软惩罚──────────────────────────────────────────────────
    # LONG stop_hunt：资金费率 < -0.01% 为正常（空头付费，多头被猎杀后成本降低）
    # SHORT stop_hunt：资金费率 > +0.01% 为正常（多头付费，空头被猎杀后成本降低）
    try:
        from config import STOP_HUNT_FUNDING_THRESHOLD as _FD_THR
    except Exception:
        _FD_THR = 0.0001

    _cur_strategy_adj = out.get("strategy_adj", 0)
    if pierce_direction == "LONG" and funding >= -_FD_THR:
        _cur_strategy_adj -= 5
        logger.debug(
            f"[judge_stop_hunt] LONG 资金费率不利({funding:.5f}>=-{_FD_THR})，"
            f"strategy_adj-=5 → {_cur_strategy_adj}"
        )
    elif pierce_direction == "SHORT" and funding <= _FD_THR:
        _cur_strategy_adj -= 5
        logger.debug(
            f"[judge_stop_hunt] SHORT 资金费率不利({funding:.5f}<={_FD_THR})，"
            f"strategy_adj-=5 → {_cur_strategy_adj}"
        )
    out["strategy_adj"] = _cur_strategy_adj

    # ── 8. 强度分级 ──────────────────────────────────────────────────────────
    is_strong = (pierce_vol_ratio >= _VOL_STRONG) and (recover_bar_count <= _REC_STRONG)
    if is_strong:
        strength        = "strong"
        strong_count    = 2
    else:
        strength        = "medium"
        strong_count    = 1
    # v3.58: stop_hunt 观察期，固定 0.1x（负期望策略最小化损耗，积累胜率数据）
    pos_cap = 0.1

    # ── 9. 计算 SL 提示（供 calc_sl_tp 使用）────────────────────────────────
    if pierce_direction == "LONG" and pierce_low is not None:
        sl_hint = pierce_low * (1 - _SL_BUF)
    elif pierce_direction == "SHORT" and pierce_high is not None:
        sl_hint = pierce_high * (1 + _SL_BUF)
    else:
        sl_hint = None

    # 计算刺穿幅度（供日志）
    if pierce_direction == "LONG" and pivot_low_val:
        p_pct = (pivot_low_val - (pierce_low or pivot_low_val)) / pivot_low_val * 100
    elif pierce_direction == "SHORT" and pivot_high_val:
        p_pct = ((pierce_high or pivot_high_val) - pivot_high_val) / pivot_high_val * 100
    else:
        p_pct = 0.0

    out.update({
        "direction":        pierce_direction,
        "signal_strength":  strength,
        "strong_count":     strong_count,
        "position_size_cap": pos_cap,
        "pierce_pct":       round(p_pct, 3),
        "vol_ratio":        round(pierce_vol_ratio, 2),
        "recover_bars":     recover_bar_count,
        "pivot_level":      round(pivot_val, 8),
        "pierce_extreme":   round(pierce_low or pierce_high or pivot_val, 8),
        "sl_price_hint":    round(sl_hint, 8) if sl_hint else None,
        "reject_reason":    None,
        # strategy_adj / liq_volume 已在验证阶段写入 out，这里保留已有值不覆盖
        "strategy_adj":     out.get("strategy_adj", 0),
        "liq_volume_at_trigger": out.get("liq_volume_at_trigger", 0.0),
        "liq_volume_baseline":   out.get("liq_volume_baseline",   0.0),
        # ref_level: 被刺穿的前高/前低——stop_hunt TP1 锚点（v3.51）
        # LONG 刺穿前低后收回，猎杀目标即前低；SHORT 同理为前高
        "ref_level":             round(pivot_val, 8),
        # sweep_extreme: 刺穿的极端点（pierce_extreme 别名，供 calc_sl_tp 读取 SL）
        "sweep_extreme":          round(pierce_low or pierce_high or pivot_val, 8),
    })
    # [fix] stop_hunt 同样漏写 volatility_tier（STOP_HUNT_ENABLED=False，但一并补齐）
    out["volatility_tier"] = _volatility_tier(ind.get("atr_1h", 0), price)
    out["atr_ratio"]       = round(_atr_ratio(ind.get("atr_1h", 0), price), 4)
    logger.debug(
        f"[judge_stop_hunt] {pierce_direction} {strength} "
        f"pivot={pivot_val:.6g} pierce_pct={p_pct:.2f}% "
        f"vol={pierce_vol_ratio:.2f}x recover={recover_bar_count}bars "
        f"liq_trigger={out.get('liq_volume_at_trigger',0)/1000:.1f}k "
        f"liq_baseline={out.get('liq_volume_baseline',0)/1000:.1f}k "
        f"strategy_adj={out.get('strategy_adj',0)} "
        f"sl_hint={'N/A' if sl_hint is None else f'{sl_hint:.6g}'}"
    )
    return True, out


# ═══════════════════════════════════════════════════════════════════════════════
# OI Divergence 策略判断（v3.60）
# 触发逻辑：
#   LONG：价格接近 24H 新低 + OI 不降反升（空头拥挤） + taker 开始回暖 + RSI 偏低
#   SHORT：价格接近 24H 新高 + OI 不升反降（多头出逃） + taker 转弱 + RSI 偏高
# 与现有 _oi_divergence 过滤器的区别：
#   过滤器：做多信号出现时，发现 OI 背离 → 拦截（不要做多）
#   本策略：OI 背离本身就是入场信号 → 主动逆向布局
# ═══════════════════════════════════════════════════════════════════════════════

_oi_div_degrade_count: dict = {}  # symbol -> (count, last_ts)：oi_divergence 降级计数（模块级，跨调用累计）


def _oi_div_degrade(symbol: str, out: dict, tag: str) -> None:
    """记录一次 oi_divergence 降级；1 小时内累计 >= 2 次 → position_size_cap 压到 0.2。超过 1 小时清零重计。"""
    import time as _tw
    now = _tw.time()
    cnt, last_ts = _oi_div_degrade_count.get(symbol, (0, now))
    if now - last_ts > 3600:
        cnt = 0
    cnt += 1
    _oi_div_degrade_count[symbol] = (cnt, now)
    if cnt >= 2:
        logger.warning(f"[judge_oi_divergence] {symbol} 1小时内已降级 {cnt} 次（{tag}），pos_cap 降至 0.2")
        out["position_size_cap"] = min(out.get("position_size_cap", 0.5), 0.2)


def judge_oi_divergence(k1h: list, k4h: list, k15: list, ind: dict,
                        chg24: float, funding: float) -> tuple[bool, dict]:
    """
    OI 背离信号判断。
    k1h / k4h / k15 均为已闭合 K 线（末尾未闭合已去除）。
    """
    try:
        return _judge_oi_divergence_impl(k1h, k4h, k15, ind, chg24, funding)
    except Exception as e:
        _log_judge_failure("judge_oi_divergence", e, k1h, k4h, k15, ind)
        return _judge_error_result("judge_oi_divergence", "oi_divergence", e)


def _judge_oi_divergence_impl(k1h, k4h, k15, ind, chg24, funding):
    import sys as _sys, pathlib as _pl
    _sys.path.insert(0, str(_pl.Path(__file__).parent.parent))
    try:
        from config import (
            OI_DIV_PRICE_NEAR_LOW_PCT   as _NEAR_LOW,
            OI_DIV_PRICE_NEAR_HIGH_PCT  as _NEAR_HIGH,
            OI_DIV_OI_RISE_PCT          as _OI_RISE,
            OI_DIV_OI_DROP_PCT          as _OI_DROP,
            OI_DIV_TAKER_RECOVERY_MIN   as _TAKER_REC,
            OI_DIV_TAKER_WEAK_MAX       as _TAKER_WEAK,
            OI_DIV_RSI1H_LONG_MAX       as _RSI_LONG_MAX,
            OI_DIV_RSI1H_SHORT_MIN      as _RSI_SHORT_MIN,
            OI_DIV_BASE_STRONG          as _BASE_STRONG,
            OI_DIV_BASE_MEDIUM          as _BASE_MEDIUM,
            ATR_TIER_MID_MIN            as _ATR_MID_MIN,
        )
    except Exception:
        _NEAR_LOW    = 1.03
        _NEAR_HIGH   = 0.97
        _OI_RISE     = 0.10
        _OI_DROP     = 0.10
        _TAKER_REC   = 0.48
        _TAKER_WEAK  = 0.52
        _RSI_LONG_MAX  = 38
        _RSI_SHORT_MIN = 62
        _BASE_STRONG = 68
        _BASE_MEDIUM = 55
        _ATR_MID_MIN = 0.5

    price   = ind["price"]
    atr     = ind["atr_1h"]
    atr_r   = _atr_ratio(atr, price)
    tier    = _volatility_tier(atr, price)
    rsi_1h  = ind.get("rsi_1h", 50)
    symbol  = ind.get("symbol", "")

    out = {
        "mode":             "oi_divergence",
        "volatility_tier":  tier,
        "atr_ratio":        round(atr_r, 4),
        "direction":        None,
        "signal_strength":  "medium",
        "position_size_cap": 0.5,
        "oi_divergence_type": None,   # "oi_squeeze_long" / "oi_bleed_short"
        "oi_change_pct":    0.0,
        "taker_ratio_now":  ind.get("taker_buy_ratio_now", 0.5),
        "rsi_1h":           rsi_1h,
        "funding_rate":     funding,
        "reject_reason":    None,
    }

    # ── 0. 波动率前置检查（过滤无波动横盘）──────────────────────────────────
    if atr_r < _ATR_MID_MIN:
        out["reject_reason"] = f"波动率过低: ATR_ratio={atr_r:.2f}%<{_ATR_MID_MIN}%，不适合 OI 背离策略"
        return False, out

    # ── 1. 读取 OI 数据 ────────────────────────────────────────────────────
    oi_now    = ind.get("oi_now", 0.0)
    oi_1h_ago = ind.get("oi_1h_ago", 0.0)   # 由 scanner 注入（v3.58 已采集）

    if oi_now <= 0 or oi_1h_ago <= 0:
        out["reject_reason"] = "OI 数据不可用（oi_now 或 oi_1h_ago 为 0）"
        return False, out

    oi_change = (oi_now - oi_1h_ago) / (oi_1h_ago + 1e-9)  # 相对变化（正 = OI 上升）

    # ── 2. 价格位置 + OI 方向判断 ─────────────────────────────────────────
    high_24h = ind.get("high_24h", 0.0)
    low_24h  = ind.get("low_24h", 0.0)

    if high_24h <= 0 or low_24h <= 0:
        out["reject_reason"] = "high_24h / low_24h 数据无效"
        return False, out

    is_near_low  = (low_24h > 0) and (price <= low_24h * _NEAR_LOW)
    is_near_high = (high_24h > 0) and (price >= high_24h * _NEAR_HIGH)

    direction = None

    if is_near_low and oi_change >= _OI_RISE:
        # LONG：价格接近新低 + OI 上升（空头拥挤，逼空在即）
        direction = "LONG"
        out["oi_divergence_type"] = "oi_squeeze_long"
    elif is_near_high and oi_change <= -_OI_DROP:
        # SHORT：价格接近新高 + OI 下降（多头偷偷出逃）
        direction = "SHORT"
        out["oi_divergence_type"] = "oi_bleed_short"
    else:
        out["reject_reason"] = (
            f"OI背离条件未满足: 价格={price:.4g} 近低={is_near_low}({low_24h:.4g}) "
            f"近高={is_near_high}({high_24h:.4g}) OI变化={oi_change*100:.1f}%"
        )
        return False, out

    out["direction"]      = direction.lower()
    out["oi_change_pct"]  = round(oi_change * 100, 2)

    # ── 3. RSI 过滤（方向合理性） ─────────────────────────────────────────
    if direction == "LONG" and rsi_1h > _RSI_LONG_MAX:
        out["reject_reason"] = (
            f"LONG RSI 过滤: rsi_1h={rsi_1h:.1f} > {_RSI_LONG_MAX}，"
            f"价格虽低但 RSI 未到超卖区间，OI 背离可信度不足"
        )
        return False, out
    if direction == "SHORT" and rsi_1h < _RSI_SHORT_MIN:
        out["reject_reason"] = (
            f"SHORT RSI 过滤: rsi_1h={rsi_1h:.1f} < {_RSI_SHORT_MIN}，"
            f"价格虽高但 RSI 未到超买区间，OI 背离可信度不足"
        )
        return False, out

    # ── 4. Taker Buy Ratio 确认 ───────────────────────────────────────────
    taker_now = ind.get("taker_buy_ratio_now", 0.5)
    taker_ma  = ind.get("taker_buy_ratio", 0.5)   # 近 20 根均值

    taker_ok = False
    if direction == "LONG":
        # 买方开始回暖：当前 taker_now >= _TAKER_REC（买方不再完全主导悲观）
        taker_ok = taker_now >= _TAKER_REC
        if not taker_ok:
            out["reject_reason"] = (
                f"Taker 买方未回暖: taker_now={taker_now:.3f} < {_TAKER_REC}，"
                f"空头仍完全主导，入场太早"
            )
            return False, out
    elif direction == "SHORT":
        # 买方转弱：当前 taker_now <= _TAKER_WEAK
        taker_ok = taker_now <= _TAKER_WEAK
        if not taker_ok:
            out["reject_reason"] = (
                f"Taker 买方未转弱: taker_now={taker_now:.3f} > {_TAKER_WEAK}，"
                f"买方仍在主导，多头尚未出逃完成"
            )
            return False, out

    # [v3.87 OPT-10] OI 背离必须看到「衰竭」确认。原逻辑把「价格在 24H 低点附近 + OI 1H 增 10%」当作空头被困、做多，
    #   但这同样是空头在顺势加仓的典型形态，更常见的是继续下跌。真正的「被困」需要价格不再创新低、主动买入回升。
    #   新增两条（做空对称）：① 最后一根已闭合 1H 未创近 3 根新低（新高）② 当前主动买入占比高于（低于）近 20 根均值。
    #   开关 OPT_OI_DIV_EXHAUSTION_CONFIRM。
    try:
        from config import OPT_OI_DIV_EXHAUSTION_CONFIRM as _OPT_OIE
    except Exception:
        _OPT_OIE = False
    if _OPT_OIE:
        from shared.opt_v387 import no_new_extreme as _nne
        _k1h_c = k1h[:-1] if len(k1h) > 1 else k1h
        if not _nne(_k1h_c, direction):
            out["reject_reason"] = f"OI背离未见衰竭: 最后一根1H仍在创{'新低' if direction == 'LONG' else '新高'}"
            return False, out
        if direction == "LONG" and taker_now <= taker_ma:
            out["reject_reason"] = f"OI背离未见衰竭: 主动买入占比{taker_now:.3f}未高于均值{taker_ma:.3f}"
            return False, out
        if direction == "SHORT" and taker_now >= taker_ma:
            out["reject_reason"] = f"OI背离未见衰竭: 主动买入占比{taker_now:.3f}未低于均值{taker_ma:.3f}"
            return False, out

    # ── 5. 资金费率方向检查（与逆向布局方向一致性） ────────────────────────
    # LONG：资金费率越负越好（空头付费，说明空头拥挤）
    # SHORT：资金费率越正越好（多头付费，说明多头拥挤）
    if direction == "LONG" and funding > 0.0003:
        out["reject_reason"] = (
            f"资金费率不支持 LONG: funding={funding*100:.4f}% 为正，"
            f"多头付费，不是空头过度拥挤的迹象"
        )
        return False, out
    if direction == "SHORT" and funding < -0.0003:
        out["reject_reason"] = (
            f"资金费率不支持 SHORT: funding={funding*100:.4f}% 为负，"
            f"空头付费，不是多头过度拥挤的迹象"
        )
        return False, out

    # ── 6. 量能检查（vol_ratio 最低门槛，避免完全无量） ───────────────────
    vol_ratio = ind.get("vol_ratio", 1.0)
    if vol_ratio < 0.4:
        out["reject_reason"] = (
            f"量能过低: vol_ratio={vol_ratio:.2f} < 0.4，"
            f"完全无量时 OI 背离可能是假象"
        )
        return False, out

    # ── 6b. 清算墙方向检查（v3.62）──────────────────────────────────────────
    # SHORT：若入场价上方 1~5% 内存在密集空头清算带（liq_usd > 阈值），
    # 说明上方有大量空头止损单，价格上冲时形成逼空燃料，直接拒绝。
    # LONG：若入场价下方 1~5% 内存在密集多头清算带，同理拒绝。
    _OI_DIV_LIQ_WALL_THRESHOLD = 500_000   # $50万以上算密集清算墙
    _OI_DIV_LIQ_WALL_PCT_MIN   = 0.01      # 距入场价 1%
    _OI_DIV_LIQ_WALL_PCT_MAX   = 0.05      # 距入场价 5%
    try:
        _liq_wall_symbol = ind.get("symbol", "")
        if _liq_wall_symbol:
            from shared.liq_cache import get_liq_zones_cached as _get_liq
            _liq_check_dir = "SHORT" if direction == "SHORT" else "LONG"
            _liq_zones_wall = _get_liq(_liq_wall_symbol, _liq_check_dir, price, range_="3d")
            _wall_usd = 0.0
            for _z in _liq_zones_wall:
                _zmid = (_z["price_low"] + _z["price_high"]) / 2
                if direction == "SHORT":
                    _dist_pct = (_zmid - price) / (price + 1e-9)
                elif direction == "LONG":
                    _dist_pct = (price - _zmid) / (price + 1e-9)
                else:
                    continue
                if _OI_DIV_LIQ_WALL_PCT_MIN <= _dist_pct <= _OI_DIV_LIQ_WALL_PCT_MAX:
                    _wall_usd += _z.get("liq_usd", 0)
            out["liq_wall_usd"] = round(_wall_usd, 0)
            if _wall_usd >= _OI_DIV_LIQ_WALL_THRESHOLD:
                _dir_label = "上方" if direction == "SHORT" else "下方"
                out["reject_reason"] = (
                    f"清算墙过滤: {direction} 方向入场价 {price:.4g} "
                    f"{_dir_label} 1~5% 内存在 "
                    f"${_wall_usd/1e6:.2f}M 清算带（阈值 ${_OI_DIV_LIQ_WALL_THRESHOLD/1e6:.1f}M），"
                    f"止损单墙形成逼仓风险，拒绝入场"
                )
                return False, out
    except Exception as _liq_wall_err:
        logger.debug(f"[judge_oi_divergence] 清算墙检查异常（不阻断）: {_liq_wall_err}")
        out["liq_wall_usd"] = None
        # v3.63b P1: 降级计数器——频繁异常时打 WARNING 并降低 pos_cap
        _oi_div_degrade(ind.get("symbol", "UNKNOWN"), out, "#6b 清算墙检查")

    # ── 6b2. 清算图方向性验证（v3.65）──────────────────────────────────────────
    # 逻辑：做空信号要求下方多头清算量 >= 上方空头待清算量 * ratio_min
    #       做多信号要求上方空头清算量 >= 下方多头待清算量 * ratio_min
    # 防止在"下方清算已释放、上方待清算巨大"这类结构下逆势入场（SKUU 案例）
    try:
        from config import (
            OI_DIV_LIQ_RATIO_MIN as _LIQ_RATIO_MIN,
            OI_DIV_LIQ_ABS_MIN   as _LIQ_ABS_MIN,
        )
    except Exception:
        _LIQ_RATIO_MIN = 1.5
        _LIQ_ABS_MIN   = 300_000

    try:
        _liq_dir_symbol = ind.get("symbol", "")
        if _liq_dir_symbol:
            from shared.liq_cache import get_liq_zones_cached as _get_liq_dir
            _liq_long_zones  = _get_liq_dir(_liq_dir_symbol, "LONG",  price, range_="3d")
            _liq_short_zones = _get_liq_dir(_liq_dir_symbol, "SHORT", price, range_="3d")

            if direction == "SHORT":
                # 下方多头清算量（推动价格向下）
                _liq_support = sum(
                    _z.get("liq_usd", 0) for _z in _liq_long_zones
                    if ((_z.get("price_low", 0) + _z.get("price_high", 0)) / 2) < price
                )
                # 上方空头待清算量（可能推动价格向上，是做空的对手盘）
                _liq_against = sum(
                    _z.get("liq_usd", 0) for _z in _liq_short_zones
                    if ((_z.get("price_low", 0) + _z.get("price_high", 0)) / 2) > price
                )
                _dir_label_sup = "下方多头"
                _dir_label_opp = "上方空头待清算"

            elif direction == "LONG":
                # 上方空头清算量（推动价格向上）
                _liq_support = sum(
                    _z.get("liq_usd", 0) for _z in _liq_short_zones
                    if ((_z.get("price_low", 0) + _z.get("price_high", 0)) / 2) > price
                )
                # 下方多头待清算量（可能推动价格向下，是做多的对手盘）
                _liq_against = sum(
                    _z.get("liq_usd", 0) for _z in _liq_long_zones
                    if ((_z.get("price_low", 0) + _z.get("price_high", 0)) / 2) < price
                )
                _dir_label_sup = "上方空头"
                _dir_label_opp = "下方多头待清算"

            else:
                _liq_support = _liq_against = 0
                _dir_label_sup = _dir_label_opp = ""

            out["liq_dir_support"] = round(_liq_support, 0)
            out["liq_dir_against"] = round(_liq_against, 0)

            # 双方数据都太稀疏 → 跳过比值检查（降级，不阻断）
            if _liq_support < _LIQ_ABS_MIN and _liq_against < _LIQ_ABS_MIN:
                logger.debug(
                    f"[judge_oi_divergence] {_liq_dir_symbol} 6b2 双方清算量均不足"
                    f"(support={_liq_support/1000:.0f}k against={_liq_against/1000:.0f}k)"
                    f"，跳过方向性比值检查"
                )
            # 信号方向量不足而对手方有数据 → 拒绝
            elif _liq_support < _LIQ_ABS_MIN:
                out["reject_reason"] = (
                    f"清算图方向不利({direction}): {_dir_label_sup}清算量不足"
                    f"({_liq_support/1000:.0f}k USD < {_LIQ_ABS_MIN/1000:.0f}k)，"
                    f"缺乏方向催化，{_dir_label_opp}={_liq_against/1000:.0f}k"
                )
                return False, out
            # 比值不足 → 拒绝
            elif _liq_against > 0 and (_liq_support / _liq_against) < _LIQ_RATIO_MIN:
                _ratio = _liq_support / _liq_against
                out["reject_reason"] = (
                    f"清算图比值不足({direction}): {_dir_label_sup}={_liq_support/1000:.0f}k "
                    f"/ {_dir_label_opp}={_liq_against/1000:.0f}k "
                    f"= {_ratio:.2f} < {_LIQ_RATIO_MIN}，"
                    f"反向清算压力过大，市场更可能朝反方向扫仓"
                )
                return False, out
            else:
                _ratio_log = (_liq_support / _liq_against) if _liq_against > 0 else float("inf")
                logger.info(
                    f"[judge_oi_divergence] {_liq_dir_symbol} {direction} 6b2 清算图方向✅ "
                    f"{_dir_label_sup}={_liq_support/1000:.0f}k "
                    f"{_dir_label_opp}={_liq_against/1000:.0f}k "
                    f"ratio={_ratio_log:.2f}"
                )
    except Exception as _liq_dir_err:
        logger.debug(f"[judge_oi_divergence] 6b2 清算图方向性检查异常（不阻断）: {_liq_dir_err}")

    # ── 6c. 动量衰竭 + 回落确认（v3.63）────────────────────────────────────
    # 评审升级（P0-2）：原版只要"收阴K OR 收盘低于前根"，
    # 但这在价格仍在上行时也可能满足（如高点震荡根）。
    #
    # 新逻辑（两道门，均需通过）：
    #   门1：K线减速 —— 最近 1H 收阴 OR 收盘低于前根（保留原逻辑）
    #   门2：价格回落确认 —— 当前价格已从近3根最高点回落 ≥ 0.8%
    #         这确保信号发出时价格真正已经开始向下运动，而非高位震荡
    #
    # LONG 方向镜像：收阳 OR 收盘高于前根，且当前价从近3根低点回升 ≥ 0.8%
    _momentum_note = ""
    # 回落确认门槛已改为 ATR 动态（v3.63b），见下方 _atr_confirm 计算

    if len(k1h) >= 3:
        _k_last  = k1h[-1]
        _k_prev  = k1h[-2]
        _close_last = float(_k_last[4])
        _open_last  = float(_k_last[1])
        _close_prev = float(_k_prev[4])

        # 近3根已闭合K线的极值（触发时的参考高/低点）
        _recent_extreme_high = max(float(_k[2]) for _k in k1h[-4:-1]) if len(k1h) >= 4 else float(k1h[-2][2])
        _recent_extreme_low  = min(float(_k[3]) for _k in k1h[-4:-1]) if len(k1h) >= 4 else float(k1h[-2][3])

        # ATR 动态回落门槛（v3.63b）
        # 评审建议 0.4×ATR，我用 0.4×ATR 但下限提高到 0.5%（过滤极低波动噪声）
        _atr_confirm = max(price * 0.005, atr * 0.4)
        _atr_confirm_pct = _atr_confirm / (price + 1e-9)

        if direction == "SHORT":
            # 门1（v3.63b升级）：K线非强势
            # 评审建议用实体占比 < 0.6，我用 < 0.5（更严格，纯趋势K仍被拦截）
            # 好处：长上影阳线（实体小但有明显压制）也能通过
            _candle_range = float(_k_last[2]) - float(_k_last[3])  # high - low
            _candle_body  = abs(_close_last - _open_last)
            _body_ratio   = _candle_body / (_candle_range + 1e-9)
            _kline_ok = (_body_ratio < 0.5) or (_close_last < _close_prev)
            # 门2（ATR动态）：价格从近期高点回落 ≥ ATR动态门槛
            _pullback_abs = _recent_extreme_high - price
            _pullback_pct = _pullback_abs / (_recent_extreme_high + 1e-9)
            _pullback_ok  = _pullback_abs >= _atr_confirm

            if not _kline_ok:
                out["reject_reason"] = (
                    f"动量未衰竭: K线实体占比 {_body_ratio*100:.0f}%≥50% "
                    f"且收盘未低于前根({_close_prev:.4g})，强势K线，拉升动能仍在"
                )
                return False, out
            if not _pullback_ok:
                out["reject_reason"] = (
                    f"回落未确认: 当前价 {price:.4g} 距近期高点 {_recent_extreme_high:.4g} "
                    f"仅回落 {_pullback_pct*100:.2f}%/{_pullback_abs:.4g}"
                    f"（需 ≥{_atr_confirm_pct*100:.2f}% / {_atr_confirm:.4g} = 0.4×ATR）"
                )
                return False, out
            _momentum_note = (
                f"动量减速+回落确认: body_ratio={_body_ratio*100:.0f}% "
                f"decel={_close_last<_close_prev} pullback={_pullback_pct*100:.2f}%"
            )

        elif direction == "LONG":
            # 门1：K线非强势（镜像）
            _candle_range = float(_k_last[2]) - float(_k_last[3])
            _candle_body  = abs(_close_last - _open_last)
            _body_ratio   = _candle_body / (_candle_range + 1e-9)
            _kline_ok = (_body_ratio < 0.5) or (_close_last > _close_prev)
            # 门2（ATR动态）：价格从近期低点反弹 ≥ ATR动态门槛
            _rebound_abs = price - _recent_extreme_low
            _rebound_pct = _rebound_abs / (_recent_extreme_low + 1e-9)
            _rebound_ok  = _rebound_abs >= _atr_confirm

            if not _kline_ok:
                out["reject_reason"] = (
                    f"动量未回暖: K线实体占比 {_body_ratio*100:.0f}%≥50% "
                    f"且收盘未高于前根({_close_prev:.4g})，强势下跌K线"
                )
                return False, out
            if not _rebound_ok:
                out["reject_reason"] = (
                    f"反弹未确认: 当前价 {price:.4g} 距近期低点 {_recent_extreme_low:.4g} "
                    f"仅反弹 {_rebound_pct*100:.2f}%/{_rebound_abs:.4g}"
                    f"（需 ≥{_atr_confirm_pct*100:.2f}% / {_atr_confirm:.4g} = 0.4×ATR）"
                )
                return False, out
            _momentum_note = (
                f"动量回暖+反弹确认: body_ratio={_body_ratio*100:.0f}% "
                f"accel={_close_last>_close_prev} rebound={_rebound_pct*100:.2f}%"
            )
    out["momentum_note"] = _momentum_note

    # ── 6d. 4H 趋势过滤（v3.63 P1）─────────────────────────────────────────
    # 评审建议：在 4H 强势趋势中，1H OI 背离是噪声而非反转信号。
    # 用 4H EMA20 斜率近似趋势强度：
    #   SHORT：4H EMA20 斜率过陡（近5根均价快速上涨）→ 逆势做空胜率不足 → 拒绝
    #   LONG ：4H EMA20 斜率过陡向下 → 逆势做多胜率不足 → 拒绝
    # 斜率阈值：0.8%/bar（每根4H K线 EMA20 均价涨超0.8%算"强势"）
    _OI_DIV_4H_SLOPE_THRESHOLD = 0.008  # 每根4H K涨/跌 0.8% 以上算强趋势
    try:
        # v3.63b bugfix: 只用已闭合的 4H K 线计算斜率
        # 评审指出：若当前正处于4H K线开盘早期，未闭合K线的收盘=实时价，
        # 会导致斜率被人为拉高/拉低。k4h[-1] 通常是未闭合根，去掉。
        _k4h_closed = k4h[:-1] if len(k4h) > 1 else k4h
        if len(_k4h_closed) >= 10:
            _closes_4h = [float(_k[4]) for _k in _k4h_closed[-25:]]
            if len(_closes_4h) >= 10:
                _ema_now = sum(_closes_4h[-5:]) / 5
                _ema_old = sum(_closes_4h[-10:-5]) / 5
                _slope_per_bar = (_ema_now - _ema_old) / (_ema_old + 1e-9) / 5
                out["ema4h_slope_per_bar"] = round(_slope_per_bar * 100, 4)
                if direction == "SHORT" and _slope_per_bar > _OI_DIV_4H_SLOPE_THRESHOLD:
                    out["reject_reason"] = (
                        f"4H 趋势过强拒绝做空: EMA20 斜率 {_slope_per_bar*100:.3f}%/bar "
                        f"> {_OI_DIV_4H_SLOPE_THRESHOLD*100:.1f}%/bar，"
                        f"4H 强势上涨中 OI 背离为噪声，非反转信号"
                    )
                    return False, out
                elif direction == "LONG" and _slope_per_bar < -_OI_DIV_4H_SLOPE_THRESHOLD:
                    out["reject_reason"] = (
                        f"4H 趋势过强拒绝做多: EMA20 斜率 {_slope_per_bar*100:.3f}%/bar "
                        f"< -{_OI_DIV_4H_SLOPE_THRESHOLD*100:.1f}%/bar，"
                        f"4H 强势下跌中 OI 背离为噪声，非反转信号"
                    )
                    return False, out
    except Exception as _4h_err:
        logger.debug(f"[judge_oi_divergence] 4H趋势检查异常（不阻断）: {_4h_err}")
        out["ema4h_slope_per_bar"] = None
        # v3.63b P1: 4H K线异常降级计数
        _oi_div_degrade(ind.get("symbol", "UNKNOWN"), out, "#6d 4H趋势检查")

    # ── 6e. RSI 有效区间收窄（v3.63 P1）─────────────────────────────────────
    # 评审建议：RSI ≥55 的绝对值门槛太宽松，在 RSI 55~62 的普通上涨中也会触发。
    # 简化处理：
    #   SHORT：RSI 需 ≥62 才算"接近超买"，55~61 视为普通上涨区间，拒绝
    #   LONG ：RSI 需 ≤38 才算"接近超卖"，39~45 视为普通下跌区间，拒绝
    # 这比复杂的背离验证更稳定，不依赖 K 线 RSI 历史近似值。
    _OI_DIV_RSI_SHORT_MIN_HARD = _RSI_SHORT_MIN  # 读 config.OI_DIV_RSI1H_SHORT_MIN
    _OI_DIV_RSI_LONG_MAX_HARD  = _RSI_LONG_MAX   # 读 config.OI_DIV_RSI1H_LONG_MAX
    if direction == "SHORT" and rsi_1h < _OI_DIV_RSI_SHORT_MIN_HARD:
        out["reject_reason"] = (
            f"RSI 区间过低: rsi_1h={rsi_1h:.1f} < {_OI_DIV_RSI_SHORT_MIN_HARD}，"
            f"非超买衰竭信号，等 RSI 升至 {_OI_DIV_RSI_SHORT_MIN_HARD}+ 再评估"
        )
        return False, out
    elif direction == "LONG" and rsi_1h > _OI_DIV_RSI_LONG_MAX_HARD:
        out["reject_reason"] = (
            f"RSI 区间过高: rsi_1h={rsi_1h:.1f} > {_OI_DIV_RSI_LONG_MAX_HARD}，"
            f"非超卖修复信号，等 RSI 降至 {_OI_DIV_RSI_LONG_MAX_HARD}- 再评估"
        )
        return False, out
    out["rsi_zone_check"] = "passed"

    # ── 7. 4H EMA 结构确认（不强制，但影响强度评级） ─────────────────────
    ema20_4h = ind.get("ema20_4h", 0.0)
    ema50_4h = ind.get("ema50_4h", 0.0)
    ema_aligned = False
    if ema20_4h > 0 and ema50_4h > 0:
        if direction == "LONG" and ema20_4h < ema50_4h:
            ema_aligned = True   # 下跌趋势中做空头逼空 → 更有说服力
        elif direction == "SHORT" and ema20_4h > ema50_4h:
            ema_aligned = True   # 上涨趋势中做多头出逃 → 更有说服力

    # ── 8. 强度评级 ───────────────────────────────────────────────────────
    # strong：RSI 极端 + EMA 对齐 + OI 变化显著（>15%）
    rsi_extreme = (direction == "LONG" and rsi_1h < 35) or (direction == "SHORT" and rsi_1h > 65)
    oi_strong   = abs(oi_change) >= 0.15

    strong_count = sum([rsi_extreme, oi_strong, ema_aligned])
    if strong_count >= 2:
        strength = "strong"
        base_score = _BASE_STRONG
        pos_cap = 0.6
    else:
        strength = "medium"
        base_score = _BASE_MEDIUM
        pos_cap = 0.4

    out.update({
        "signal_strength":    strength,
        "position_size_cap":  pos_cap,
        "base_score":         base_score,
        "rsi_extreme":        rsi_extreme,
        "oi_strong":          oi_strong,
        "ema_aligned":        ema_aligned,
        "strong_count":       strong_count,
        "taker_ratio_now":    round(taker_now, 4),
        "taker_ratio_ma":     round(taker_ma, 4),
        "vol_ratio":          round(vol_ratio, 3),
        "oi_now":             round(oi_now, 2),
        "oi_1h_ago":          round(oi_1h_ago, 2),
    })

    logger.info(
        f"[judge_oi_divergence] {symbol} {direction} {strength} "
        f"oi_change={oi_change*100:.1f}% taker_now={taker_now:.3f} "
        f"rsi1h={rsi_1h:.1f} funding={funding*100:.4f}% "
        f"vol={vol_ratio:.2f} ema_aligned={ema_aligned} strong_count={strong_count}"
    )
    return True, out


# ═══════════════════════════════════════════════════════════════════════════════
# EMA Trend Follow 策略判断（v3.61）
# 触发逻辑：
#   4H EMA20 > EMA50（多头排列，差距 >= 0.5%）
#   + 1H RSI 在 48~68（健康区间，不超买不超卖）
#   + 价格在 4H EMA20 附近回踩（-1.5% ~ +2.0%）
#   + 量比 >= 0.5（市场有基本参与度）
#   → 方向固定为 LONG（EMA 趋势跟随只做多）
# ═══════════════════════════════════════════════════════════════════════════════

def judge_ema_trend(k1h: list, k4h: list, k15: list, ind: dict,
                    chg24: float, funding: float) -> tuple[bool, dict]:
    """
    EMA 趋势跟随信号判断（只做多）。
    k1h / k4h / k15 均为已闭合 K 线。
    """
    try:
        return _judge_ema_trend_impl(k1h, k4h, k15, ind, chg24, funding)
    except Exception as e:
        _log_judge_failure("judge_ema_trend", e, k1h, k4h, k15, ind)
        return _judge_error_result("judge_ema_trend", "ema_trend", e)


def _judge_ema_trend_impl(k1h, k4h, k15, ind, chg24, funding):
    import sys as _sys, pathlib as _pl
    _sys.path.insert(0, str(_pl.Path(__file__).parent.parent))
    try:
        from config import (
            EMA_TREND_EMA_GAP_MIN_PCT  as _EMA_GAP,
            EMA_TREND_RSI1H_MIN        as _RSI1H_MIN,
            EMA_TREND_RSI1H_MAX        as _RSI1H_MAX,
            EMA_TREND_RSI4H_MIN        as _RSI4H_MIN,
            EMA_TREND_RSI4H_MAX        as _RSI4H_MAX,
            EMA_TREND_PULLBACK_ABOVE_EMA as _PB_ABOVE,
            EMA_TREND_PULLBACK_BELOW_EMA as _PB_BELOW,
            EMA_TREND_VOL_RATIO_MIN    as _VOL_MIN,
            EMA_TREND_BASE_STRONG      as _BASE_STRONG,
            EMA_TREND_BASE_MEDIUM      as _BASE_MEDIUM,
            EMA_TREND_CHG24_MAX        as _CHG24_MAX,
            EMA_TREND_CHG24_MIN        as _CHG24_MIN,
            ATR_TIER_MID_MIN           as _ATR_MID_MIN,
        )
    except Exception:
        _EMA_GAP      = 0.005
        _RSI1H_MIN    = 48;  _RSI1H_MAX = 68
        _RSI4H_MIN    = 45;  _RSI4H_MAX = 72
        _PB_ABOVE     = 0.02; _PB_BELOW = 0.015
        _VOL_MIN      = 0.5
        _BASE_STRONG  = 65;  _BASE_MEDIUM = 52
        _CHG24_MAX    = 12.0; _CHG24_MIN = -8.0
        _ATR_MID_MIN  = 0.5

    price    = ind["price"]
    atr      = ind["atr_1h"]
    atr_r    = _atr_ratio(atr, price)
    tier     = _volatility_tier(atr, price)
    rsi_1h   = ind.get("rsi_1h", 50)
    rsi_4h   = ind.get("rsi_4h", 50)
    symbol   = ind.get("symbol", "")

    # v3.64: EMA Trend 增加 SHORT 方向
    # 4H 空头排列（EMA20 < EMA50，差距 >= 0.5%）时可做空
    ema20_4h = ind.get("ema20_4h", 0.0)
    ema50_4h = ind.get("ema50_4h", 0.0)
    _ema_gap_4h = (ema20_4h - ema50_4h) / (ema50_4h + 1e-9) if ema50_4h > 0 else 0
    if _ema_gap_4h >= _EMA_GAP:
        _ema_direction = "long"    # 4H 多头排列
    elif _ema_gap_4h <= -_EMA_GAP:
        _ema_direction = "short"   # 4H 空头排列
    else:
        return False, {"reject_reason": f"4H EMA 排列不明确: gap={_ema_gap_4h*100:.2f}% < ±{_EMA_GAP*100:.1f}%"}

    out = {
        "mode":            "ema_trend",
        "direction":       _ema_direction,
        "volatility_tier": tier,
        "atr_ratio":       round(atr_r, 4),
        "signal_strength": "medium",
        "position_size_cap": 0.5,
        "ema20_4h":        ind.get("ema20_4h", 0),
        "ema50_4h":        ind.get("ema50_4h", 0),
        "rsi_1h":          rsi_1h,
        "rsi_4h":          rsi_4h,
        "reject_reason":   None,
    }

    # ── 0. CHG24 范围约束 ─────────────────────────────────────────────────
    # [opt-6] SHORT 方向使用对称参数
    _rsi4h_min_eff = 28 if _ema_direction == "short" else _RSI4H_MIN
    _rsi4h_max_eff = 55 if _ema_direction == "short" else _RSI4H_MAX
    _chg24_min_eff = -12.0 if _ema_direction == "short" else _CHG24_MIN
    _chg24_max_eff = 8.0 if _ema_direction == "short" else _CHG24_MAX
    if chg24 > _chg24_max_eff:
        out["reject_reason"] = f"chg24={chg24:.1f}% > {_chg24_max_eff}%，当日涨幅过大，不追高"
        return False, out
    if chg24 < _chg24_min_eff:
        out["reject_reason"] = f"chg24={chg24:.1f}% < {_chg24_min_eff}%，大跌应走reversal路径"
        return False, out

    # ── 1. 波动率前置检查 ─────────────────────────────────────────────────
    if atr_r < _ATR_MID_MIN:
        out["reject_reason"] = f"波动率过低: ATR_ratio={atr_r:.2f}% < {_ATR_MID_MIN}%"
        return False, out

    # ── 2. 4H EMA 多头排列（核心条件） ───────────────────────────────────
    ema20_4h = ind.get("ema20_4h", 0.0)
    ema50_4h = ind.get("ema50_4h", 0.0)

    if ema20_4h <= 0 or ema50_4h <= 0:
        out["reject_reason"] = "4H EMA 数据不可用（需要先拉取4H K线）"
        return False, out

    ema_gap_pct = (ema20_4h - ema50_4h) / (ema50_4h + 1e-9)
    # v3.64：按方向检查 EMA 排列强度
    if _ema_direction == "long" and ema_gap_pct < _EMA_GAP:
        out["reject_reason"] = (
            f"EMA 多头排列不稳固: EMA20={ema20_4h:.4g} EMA50={ema50_4h:.4g} "
            f"差距={ema_gap_pct*100:.2f}% < {_EMA_GAP*100:.1f}%"
        )
        return False, out
    elif _ema_direction == "short" and ema_gap_pct > -_EMA_GAP:
        out["reject_reason"] = (
            f"EMA 空头排列不稳固: EMA20={ema20_4h:.4g} EMA50={ema50_4h:.4g} "
            f"差距={ema_gap_pct*100:.2f}% > -{_EMA_GAP*100:.1f}%"
        )
        return False, out

    # ── 3. MA99 检查（可选强化条件，数据不足时放宽） ─────────────────────
    # MA99 需要 4H K 线 >= 99 根；若不足则放宽（不因数据不足拒绝）
    ema99_4h   = 0.0
    ma99_ok    = False
    if len(k4h) >= 100:
        try:
            from shared.indicators import ema as _ema_calc
            _closes_4h = [float(k[4]) for k in k4h[:-1]]
            if len(_closes_4h) >= 99:
                ema99_4h = _ema_calc(_closes_4h, 99)
                ma99_ok  = (ema50_4h > ema99_4h > 0) if _ema_direction == "long" else (0 < ema50_4h < ema99_4h)
        except Exception:
            pass
    out["ema99_4h"] = round(ema99_4h, 6)
    out["ma99_ok"]  = ma99_ok

    # ── 4. RSI 健康区间检查（v3.64：SHORT 方向适配）────────────────────────
    # SHORT 上限放宽到 70（不追顶，但允许偏高区间回踩阻力）
    _rsi1h_max_eff = 70 if _ema_direction == "short" else _RSI1H_MAX
    if not (_RSI1H_MIN <= rsi_1h <= _rsi1h_max_eff):
        out["reject_reason"] = (
            f"1H RSI={rsi_1h:.1f} 不在健康区间 [{_RSI1H_MIN}, {_rsi1h_max_eff}]"
        )
        return False, out

    if not (_rsi4h_min_eff <= rsi_4h <= _rsi4h_max_eff):
        out["reject_reason"] = (
            f"4H RSI={rsi_4h:.1f} 不在健康区间 [{_rsi4h_min_eff}, {_rsi4h_max_eff}]"
        )
        return False, out

    # ── 5. 价格在 EMA20 附近回踩（v3.64：按方向分化）────────────────────
    # LONG：价格从上方靠近 EMA20 支撑（正常回踩）
    # SHORT：价格从下方靠近 EMA20 阻力（空头排列中反弹到 EMA20 下方再压制）
    pb_ratio = (price - ema20_4h) / (ema20_4h + 1e-9)
    if _ema_direction == "long":
        if pb_ratio > _PB_ABOVE:
            out["reject_reason"] = (
                f"价格追高: price={price:.4g} 在 EMA20={ema20_4h:.4g} 上方 "
                f"{pb_ratio*100:.1f}% > {_PB_ABOVE*100:.1f}%，非回踩入场"
            )
            return False, out
        if pb_ratio < -_PB_BELOW:
            out["reject_reason"] = (
                f"价格破位: price={price:.4g} 在 EMA20={ema20_4h:.4g} 下方 "
                f"{abs(pb_ratio)*100:.1f}% > {_PB_BELOW*100:.1f}%，EMA 支撑失效"
            )
            return False, out
    else:  # SHORT
        # 价格在 EMA20 下方（空头回踩阻力：-PB_ABOVE ~ 0）
        if pb_ratio > _PB_BELOW:
            out["reject_reason"] = (
                f"SHORT 价格高于 EMA20 阻力: {pb_ratio*100:.1f}% > {_PB_BELOW*100:.1f}%，"
                f"已突破阻力，不追空"
            )
            return False, out
        if pb_ratio < -_PB_ABOVE:
            out["reject_reason"] = (
                f"SHORT 价格过低: {abs(pb_ratio)*100:.1f}% > {_PB_ABOVE*100:.1f}%，"
                f"跌离阻力太远，非回踩入场"
            )
            return False, out

    # ── 6. 量能检查 ───────────────────────────────────────────────────────
    vol_ratio = ind.get("vol_ratio", 1.0)
    if vol_ratio < _VOL_MIN:
        out["reject_reason"] = (
            f"量能不足: vol_ratio={vol_ratio:.2f} < {_VOL_MIN}，"
            f"回踩无量可能继续跌破 EMA"
        )
        return False, out

    # [v3.87 OPT-11] EMA 趋势需要触发 K 线。原逻辑只要价格回到 4H EMA20 附近就以现价立即入场，
    #   等于「到了区域就买」，回踩可能还在进行中。新：要求 1H 或 15M 出现止跌(做多)/止涨(做空)形态才通过。
    #   开关 OPT_EMA_TREND_TRIGGER_BAR。
    try:
        from config import OPT_EMA_TREND_TRIGGER_BAR as _OPT_ETT
    except Exception:
        _OPT_ETT = False
    if _OPT_ETT:
        _et_dir = _ema_direction.upper()
        _k1h_c2 = k1h[:-1] if len(k1h) > 1 else k1h
        _k15_c2 = k15[:-1] if len(k15) > 1 else k15
        _trig = _reversal_signal_1h(_k1h_c2, _et_dir) or _reversal_signal_15m(_k15_c2, _et_dir)
        if not _trig:
            out["reject_reason"] = f"EMA趋势未见触发K线: 1H/15M 无{'止跌' if _et_dir == 'LONG' else '止涨'}形态"
            return False, out
        out["trigger_bar"] = _trig

    # ── 7. 资金费率不能极端负（空头挤压环境不适合做多趋势跟随） ───────────
    if _ema_direction == "long" and funding > 0.0005:
        out["reject_reason"] = (
            f"资金费率过高: {funding*100:.4f}% > 0.05%，多头持仓成本过大"
        )
        return False, out
    if _ema_direction == "short" and funding < -0.0005:
        out["reject_reason"] = (
            f"资金费率极端负: {funding*100:.4f}% < -0.05%，空头被挤压风险"
        )
        return False, out

    # ── 8. 强度评级 ───────────────────────────────────────────────────────
    # strong：MA99 三线排列 + RSI 在 52~62 黄金区间 + 量比 > 0.8
    rsi_golden = 52 <= rsi_1h <= 62
    vol_strong = vol_ratio >= 0.8
    strong_count = sum([ma99_ok, rsi_golden, vol_strong])

    if strong_count >= 2:
        strength  = "strong"
        base_score = _BASE_STRONG
        pos_cap   = 0.6
    else:
        strength  = "medium"
        base_score = _BASE_MEDIUM
        pos_cap   = 0.45

    out.update({
        "signal_strength":  strength,
        "position_size_cap": pos_cap,
        "base_score":       base_score,
        "ema_gap_pct":      round(ema_gap_pct * 100, 3),
        "pb_ratio_pct":     round(pb_ratio * 100, 3),
        "vol_ratio":        round(vol_ratio, 3),
        "strong_count":     strong_count,
        "rsi_golden":       rsi_golden,
        "vol_strong":       vol_strong,
    })

    logger.info(
        f"[judge_ema_trend] {symbol} {_ema_direction.upper()} {strength} "
        f"ema_gap={ema_gap_pct*100:.2f}% pb={pb_ratio*100:.2f}% "
        f"rsi1h={rsi_1h:.1f} rsi4h={rsi_4h:.1f} vol={vol_ratio:.2f} "
        f"ma99_ok={ma99_ok} strong_count={strong_count}"
    )
    return True, out
