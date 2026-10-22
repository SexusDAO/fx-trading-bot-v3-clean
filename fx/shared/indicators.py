"""
fx/shared/indicators.py — 统一指标计算库

规则：
  - 所有文件禁止自己实现 ema/rsi/atr，统一从这里 import
  - 函数只接收 list，不依赖 pandas，无副作用，线程安全
  - 参数名和返回值与原各文件保持兼容
"""

from __future__ import annotations

import math
import numbers


def _require_positive_int(value, name: str = "n") -> int:
    """
    [v3.89 #71] 周期/桶数必须是正整数（bool 不算）。零、负数、None、字符串、非整数浮点（20.5）、NaN/Inf
    一律抛 ValueError，避免到计算深处才以除零、空切片或静默的错误结果暴露。
    值为整数的浮点（如配置里写成 20.0）视为合法并转成 int 返回；调用方必须使用返回值参与切片。
    合法参数的计算结果完全不变。
    """
    if isinstance(value, bool):
        raise ValueError(f"{name} 必须是正整数，收到 {value!r}")
    if isinstance(value, float):
        if not (math.isfinite(value) and value.is_integer()):
            raise ValueError(f"{name} 必须是正整数，收到 {value!r}")
        value = int(value)
    if not isinstance(value, numbers.Integral) or value <= 0:
        raise ValueError(f"{name} 必须是正整数，收到 {value!r}")
    return int(value)


def ema(closes: list, n: int) -> float:
    """指数移动均线"""
    n = _require_positive_int(n, "ema.n")
    if len(closes) < n:
        return closes[-1] if closes else 0.0
    k = 2 / (n + 1)
    e = sum(closes[:n]) / n
    for c in closes[n:]:
        e = c * k + e * (1 - k)
    return e


def rsi(closes: list, n: int = 14) -> float:
    """RSI，使用已闭合K线收盘价列表"""
    n = _require_positive_int(n, "rsi.n")
    if len(closes) < n + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[-n:]) / n
    al = sum(losses[-n:]) / n
    if al == 0:
        # [v3.89 #69] 涨跌幅均为零（完全平价序列）→ 中性 50；仅有上涨无下跌才是 100
        return 50.0 if ag == 0 else 100.0
    return round(100.0 - 100.0 / (1 + ag / al), 2)


def atr_status(klines: list, n: int = 14) -> dict:
    """
    [v3.89 #68] ATR 及样本充足性：{"value": float, "samples": int, "sufficient": bool}。
    samples = 实际参与平均的有效 TR 个数；少于 n 个时 sufficient=False，调用方可据此识别预热期。
    """
    n = _require_positive_int(n, "atr.n")
    trs = []
    for i in range(1, len(klines)):
        h  = float(klines[i][2])
        lo = float(klines[i][3])
        pc = float(klines[i - 1][4])
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    window = trs[-n:]
    samples = len(window)
    return {"value": sum(window) / samples if samples else 0.0,
            "samples": samples, "sufficient": samples >= n}


def atr(klines: list, n: int = 14) -> float:
    """
    ATR14，klines 格式：[open_time, open, high, low, close, ...]
    [v3.89 #68] 有效 TR 少于 n 个时按实际个数求平均（不再除以完整周期 n 而压低数值）；
    需要识别样本不足请用 atr_status()。样本充足（>= n）时结果与之前完全一致。
    """
    return atr_status(klines, n)["value"]


def vol_ma(klines: list, n: int = 20) -> float:
    """成交量 N 周期均值"""
    vols = [float(k[5]) for k in klines]
    return sum(vols[-n:]) / n if len(vols) >= n else (sum(vols) / len(vols) if vols else 1.0)


def bb_pct(closes: list, n: int = 20) -> float:
    """布林带 %B（0=下轨，1=上轨）"""
    n = _require_positive_int(n, "bb_pct.n")
    if len(closes) < n:
        return 0.5
    w = closes[-n:]
    mid = sum(w) / n
    std = (sum((x - mid) ** 2 for x in w) / n) ** 0.5
    if std == 0:
        return 0.5
    upper = mid + 2 * std
    lower = mid - 2 * std
    return (closes[-1] - lower) / (upper - lower)


def bollinger(closes: list, n: int = 20) -> dict:
    """布林带上中下轨 + %B，返回 {upper, mid, lower, pct}"""
    n = _require_positive_int(n, "bollinger.n")
    if len(closes) < n:
        mid = closes[-1] if closes else 0
        return {"upper": mid, "mid": mid, "lower": mid, "pct": 0.5}
    w   = closes[-n:]
    mid = sum(w) / n
    std = (sum((x - mid) ** 2 for x in w) / n) ** 0.5
    upper = mid + 2 * std
    lower = mid - 2 * std
    pct   = (closes[-1] - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
    return {"upper": upper, "mid": mid, "lower": lower, "pct": pct}


# ── 向后兼容别名（旧代码使用下划线前缀版本） ────────────────────────────────
_ema  = ema
_rsi  = rsi
_atr  = atr
_ema_raw = ema
_rsi_raw = rsi
_atr_raw = atr
_calc_ema = ema
_calc_atr = atr
_calc_rsi = rsi
_calc_atr_from_klines = atr


def calc_volume_profile(klines: list, bins: int = 40) -> dict:
    """
    粗略 Volume Profile（v3.62）。
    用 1H K 线的高低价+成交量构建价格桶，找出：
      poc  — Point of Control（成交量最大价格桶的中价）
      vah  — Value Area High（70% 成交量覆盖区间上沿）
      val  — Value Area Low（70% 成交量覆盖区间下沿）

    算法：
    1. 将所有 K 线的成交量按高低价均匀分配到 bins 个价格桶
    2. 找到 poc（成交量最大桶）
    3. 从 poc 向两侧扩展，直到覆盖 70% 总成交量，得到 vah/val

    参数：
      klines — 1H 已闭合 K 线列表（OHLCV 格式，index 1=open 2=high 3=low 4=close 5=vol）
      bins   — 价格桶数量（默认 40）

    返回：
      {
        "poc": float,    # 主控价（最高成交量价格）
        "vah": float,    # 价值区上沿
        "val": float,    # 价值区下沿
        "bins": int,     # 实际桶数
        "valid": bool,   # 数据是否足够（< 20 根时为 False）
      }
    返回 valid=False 时，poc/vah/val 均为 0.0，调用方应跳过 VP 逻辑。
    """
    bins = _require_positive_int(bins, "calc_volume_profile.bins")
    if not klines or len(klines) < 20:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0, "bins": 0, "valid": False}

    # 取最近 120 根（约 5 天）已闭合 K 线，太久远的成交量参考价值低
    recent = klines[-120:] if len(klines) > 120 else klines

    # 计算全局价格范围
    all_highs = [float(k[2]) for k in recent]
    all_lows  = [float(k[3]) for k in recent]
    p_max = max(all_highs)
    p_min = min(all_lows)
    p_range = p_max - p_min

    if p_range <= 0 or p_min <= 0:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0, "bins": 0, "valid": False}

    bin_size = p_range / bins
    # 每个桶的成交量
    bucket_vol = [0.0] * bins

    for k in recent:
        hi  = float(k[2])
        lo  = float(k[3])
        vol = float(k[5])
        if vol <= 0 or hi <= lo:
            # 零量或无效 K 线：成交量全给中价桶
            mid_idx = min(int((( hi + lo) / 2 - p_min) / bin_size), bins - 1)
            bucket_vol[max(0, mid_idx)] += max(vol, 0.0)   # [v3.89 #70] 负成交量不计入
            continue
        # 均匀分配：计算该 K 线跨越的桶范围
        lo_idx = max(0,        int((lo - p_min) / bin_size))
        hi_idx = min(bins - 1, int((hi - p_min) / bin_size))
        span = hi_idx - lo_idx + 1
        vol_per_bin = vol / span
        for b in range(lo_idx, hi_idx + 1):
            bucket_vol[b] += vol_per_bin

    # [v3.89 #70] 总有效成交量为 0：没有成交证据，不生成任意桶的 POC/VAH/VAL
    if sum(bucket_vol) <= 0:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0, "bins": 0, "valid": False, "reason": "zero_volume"}

    # POC：成交量最大的桶
    poc_idx  = bucket_vol.index(max(bucket_vol))
    poc_price = p_min + (poc_idx + 0.5) * bin_size

    # Value Area：从 poc 向两侧扩展，覆盖 70% 总成交量
    total_vol  = sum(bucket_vol)
    target_vol = total_vol * 0.70
    va_lo_idx  = poc_idx
    va_hi_idx  = poc_idx
    va_vol     = bucket_vol[poc_idx]

    while va_vol < target_vol:
        can_expand_lo = va_lo_idx > 0
        can_expand_hi = va_hi_idx < bins - 1
        if not can_expand_lo and not can_expand_hi:
            break
        gain_lo = bucket_vol[va_lo_idx - 1] if can_expand_lo else 0.0
        gain_hi = bucket_vol[va_hi_idx + 1] if can_expand_hi else 0.0
        # 优先向成交量更大的一侧扩展
        if gain_hi >= gain_lo and can_expand_hi:
            va_hi_idx += 1
            va_vol += gain_hi
        elif can_expand_lo:
            va_lo_idx -= 1
            va_vol += gain_lo
        else:
            va_hi_idx += 1
            va_vol += gain_hi

    vah = p_min + (va_hi_idx + 1) * bin_size   # 价值区上沿（桶上边界）
    val = p_min + va_lo_idx * bin_size           # 价值区下沿（桶下边界）

    return {
        "poc":   round(poc_price, 8),
        "vah":   round(vah, 8),
        "val":   round(val, 8),
        "bins":  bins,
        "valid": True,
    }
