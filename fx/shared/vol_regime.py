"""
fx/shared/vol_regime.py — 波动率状态模块 (Vol Regime)

功能：
  - 从 Deribit 拉取 BTC/ETH DVOL（隐含波动率指数）
  - 用 Binance 日线 K 线计算历史已实现波动率（RV）
  - 输出 vol_regime：vol_cheap / neutral / vol_rich
  - regime 用于调整主工作流仓位上限和格距（乘数方式，不改逻辑）

集成方式：
  from shared.vol_regime import get_vol_regime, get_vol_multipliers
  multipliers = get_vol_multipliers()   # {"size": 1.1, "grid": 0.9}

资产适用范围：
  - BTC  ✅ 直接使用 BTC DVOL
  - ETH  ✅ 直接使用 ETH DVOL
  - 山寨 ❌ 跳过，不用 DVOL 调参
  - 美股 ❌ 跳过，完全隔离

数据来源：
  - IV: https://www.deribit.com/api/v2/public/get_volatility_index_data
  - RV: Binance FAPI /fapi/v1/klines（日线）

缓存：结果缓存 4 小时（与 health-check-4h 频率匹配）
"""

import json
import logging
import math
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("fx.vol_regime")

# ── 配置 ──────────────────────────────────────────────────────────────────────
DERIBIT_DVOL_URL  = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
BINANCE_KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
DVOL_RESOLUTION   = "1D"
RV_WINDOW         = 30        # 30日已实现波动率
KLINE_LIMIT       = 40        # 多取一些，保证够30天窗口
CACHE_TTL         = 4 * 3600  # 4小时缓存

# IV/RV 阈值
VOL_RICH_THRESHOLD  = 1.20   # IV > RV × 1.20 → vol_rich（波动率贵）
VOL_CHEAP_THRESHOLD = 0.85   # IV < RV × 0.85 → vol_cheap（波动率便宜）

# 仓位调整乘数
MULTIPLIERS = {
    "vol_rich":  {"size": 0.80, "grid": 1.20},   # 收缩
    "neutral":   {"size": 1.00, "grid": 1.00},   # 不动
    "vol_cheap": {"size": 1.10, "grid": 0.90},   # 扩张
}

# 适用资产白名单（只有这些币对才使用 DVOL 信号）
_DVOL_ELIGIBLE = {
    "BTCUSDT": "BTC",
    "ETHUSDT":  "ETH",
    "BTCUSDC":  "BTC",
    "ETHUSDC":  "ETH",
}

_CACHE_FILE = Path(__file__).parent.parent / "logs" / "vol_regime_cache.json"

# ── 内存缓存 ──────────────────────────────────────────────────────────────────
_mem_cache: dict = {}   # key: currency → {"regime": ..., "iv": ..., "rv": ..., "ts": ...}


# ── proxy 工具 ────────────────────────────────────────────────────────────────
def _make_opener():
    try:
        import socks, socket as _sock
        from config import PROXY_URL
        parts = PROXY_URL.replace("socks5://", "").split("@")
        auth, host_port = parts[0], parts[1]
        user, pwd = auth.split(":")
        host, port = host_port.split(":")
        socks.set_default_proxy(socks.SOCKS5, host, int(port), username=user, password=pwd)
        _sock.socket = socks.socksocket
    except Exception:
        pass
    return urllib.request.build_opener()


def _http_get(url: str, params: dict | None = None, timeout: int = 15) -> dict:
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"
    opener = _make_opener()
    req = urllib.request.Request(url, headers={"User-Agent": "fx-vol-regime/1.0"})
    with opener.open(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


# ── DVOL 拉取 ─────────────────────────────────────────────────────────────────
def _fetch_dvol(currency: str, days: int = 35) -> float | None:
    """返回最新一天的 DVOL 收盘值（年化%），失败返回 None"""
    try:
        now_ms   = int(time.time() * 1000)
        start_ms = now_ms - days * 86400 * 1000
        data = _http_get(DERIBIT_DVOL_URL, {
            "currency":        currency,
            "start_timestamp": start_ms,
            "end_timestamp":   now_ms,
            "resolution":      DVOL_RESOLUTION,
        })
        rows = data.get("result", {}).get("data", [])
        if rows:
            return float(rows[-1][4])   # close
    except Exception as e:
        logger.warning(f"[vol_regime] DVOL 拉取失败 ({currency}): {e}")
    return None


# ── RV 计算 ───────────────────────────────────────────────────────────────────
def _fetch_rv(symbol: str, window: int = RV_WINDOW) -> float | None:
    """
    返回 window 日已实现波动率（年化%），失败返回 None。

    公式：样本标准差（减均值 + Bessel n-1 校正），再乘年化因子。
    对于 30 日窗口，RMS 和模型标准差偏差只有 ~1.7%（问题32记录在案），
    但正确性上应使用样本标准差。
    """
    try:
        data = _http_get(BINANCE_KLINE_URL, {
            "symbol":   symbol,
            "interval": "1d",
            "limit":    window + 5,
        })
        closes = [float(row[4]) for row in data]
        if len(closes) < window + 1:
            return None
        log_rets = [math.log(closes[i] / closes[i-1]) for i in range(-window, 0)]
        n = len(log_rets)
        mean = sum(log_rets) / n
        # 样本标准差：减均值 + n-1 Bessel 校正
        variance = sum((r - mean) ** 2 for r in log_rets) / (n - 1)
        std = math.sqrt(variance)
        return std * math.sqrt(365) * 100
    except Exception as e:
        logger.warning(f"[vol_regime] RV 计算失败 ({symbol}): {e}")
    return None


# ── regime 判断 ───────────────────────────────────────────────────────────────
def _classify(iv: float, rv: float) -> str:
    if rv <= 0:
        return "neutral"
    ratio = iv / rv
    if ratio > VOL_RICH_THRESHOLD:
        return "vol_rich"
    if ratio < VOL_CHEAP_THRESHOLD:
        return "vol_cheap"
    return "neutral"


# ── 缓存读写 ──────────────────────────────────────────────────────────────────
def _load_cache() -> dict:
    try:
        if _CACHE_FILE.exists():
            return json.loads(_CACHE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_cache(data: dict):
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        logger.warning(f"[vol_regime] 缓存写入失败: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 公开接口
# ═══════════════════════════════════════════════════════════════════════════════

def get_vol_regime(symbol: str = "BTCUSDT", force_refresh: bool = False) -> dict:
    """
    返回指定标的的波动率状态。

    Parameters
    ----------
    symbol : str
        交易对，如 BTCUSDT / ETHUSDT / SOLUSDT 等
    force_refresh : bool
        强制忽略缓存重新拉取

    Returns
    -------
    dict:
        {
          "symbol":   "BTCUSDT",
          "currency": "BTC",          # 或 None（山寨不适用）
          "regime":   "vol_cheap",    # vol_cheap / neutral / vol_rich / n/a
          "iv":       37.4,           # Deribit DVOL，%；山寨为 None
          "rv":       45.5,           # 30日历史RV，%
          "ratio":    0.823,          # IV/RV；山寨为 None
          "eligible": True,           # False 表示该币种不适用 DVOL 信号
          "ts":       1234567890,     # unix 时间戳
          "date":     "2026-08-29",
        }
    """
    currency = _DVOL_ELIGIBLE.get(symbol.upper())
    now = time.time()
    date_str = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d")

    # 山寨/美股：直接返回 n/a，不拉 DVOL
    if currency is None:
        rv = _fetch_rv(symbol)
        return {
            "symbol": symbol, "currency": None,
            "regime": "n/a", "iv": None,
            "rv": round(rv, 2) if rv else None,
            "ratio": None, "eligible": False,
            "source": "not_eligible",   # 区分：n/a = 不适用 DVOL（非山寨中性，而是无数据）
            "ts": int(now), "date": date_str,
        }

    # 检查缓存
    cache_key = currency
    if not force_refresh:
        # 内存缓存
        cached = _mem_cache.get(cache_key)
        if cached and (now - cached["ts"]) < CACHE_TTL:
            return {**cached, "symbol": symbol}
        # 文件缓存（跨进程）
        file_cache = _load_cache()
        cached = file_cache.get(cache_key)
        if cached and (now - cached["ts"]) < CACHE_TTL:
            _mem_cache[cache_key] = cached
            return {**cached, "symbol": symbol}

    # 拉取数据
    iv = _fetch_dvol(currency)
    rv = _fetch_rv(symbol)

    if iv is None or rv is None:
        logger.warning(f"[vol_regime] 数据不完整 iv={iv} rv={rv}，降级为 neutral")
        regime = "neutral"
        ratio  = None
        source = "data_fallback"   # 数据拉取失败，降级中性
    else:
        regime = _classify(iv, rv)
        ratio  = round(iv / rv, 3)
        source = "dvol"             # 算起于真实 DVOL/RV 计算

    result = {
        "symbol": symbol, "currency": currency,
        "regime": regime,
        "iv":     round(iv, 2) if iv else None,
        "rv":     round(rv, 2) if rv else None,
        "ratio":  ratio, "eligible": True,
        "source": source,   # dvol | data_fallback
        "ts":     int(now), "date": date_str,
    }

    # 更新缓存
    _mem_cache[cache_key] = result
    file_cache = _load_cache()
    file_cache[cache_key] = result
    _save_cache(file_cache)

    logger.info(
        f"[vol_regime] {symbol} IV={iv:.1f}% RV={rv:.1f}% "
        f"ratio={ratio} → {regime.upper()}"
        if iv and rv else f"[vol_regime] {symbol} → {regime} (数据不足)"
    )
    return result


def get_vol_multipliers(symbol: str = "BTCUSDT") -> dict:
    """
    返回仓位调整乘数。
    {"size": float, "grid": float}
    - size: 仓位上限乘数（× MAX_POSITIONS or × 名义金额上限）
    - grid: 格距乘数（× ATR 止损倍数）

    山寨/不适用资产始终返回 neutral 乘数（{"size": 1.0, "grid": 1.0}）
    注意：调用方可检查 get_vol_regime() 返回的 info["source"] 字段区分：
      - "dvol"          → 真实 DVOL/RV 计算，regime 有一定置信度
      - "data_fallback" → DVOL 拉取失败，降级为 neutral（提示：非真正中性）
      - "not_eligible"  → 该币种不适用 DVOL（山寨），非市场信号
    """
    info = get_vol_regime(symbol)
    regime = info.get("regime", "neutral")
    return MULTIPLIERS.get(regime, MULTIPLIERS["neutral"]).copy()


def run_health_report() -> str:
    """
    供 health-check-4h 调用，返回格式化报告字符串。
    同时更新 BTC 和 ETH 的 regime 缓存。
    """
    lines = ["📊 波动率状态 (Vol Regime)"]
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines.append(f"更新时间: {now_str}\n")

    for sym in ["BTCUSDT", "ETHUSDT"]:
        info = get_vol_regime(sym, force_refresh=True)
        cur  = info["currency"]
        iv   = info["iv"]
        rv   = info["rv"]
        ratio = info["ratio"]
        regime = info["regime"]

        if regime == "vol_rich":
            emoji = "🔴"
            hint  = "波动率偏贵，收缩仓位（×0.8）"
        elif regime == "vol_cheap":
            emoji = "🟢"
            hint  = "波动率偏便宜，可扩张（×1.1）"
        else:
            emoji = "🟡"
            hint  = "波动率中性，维持现参"

        lines.append(
            f"{emoji} {sym}  IV={iv:.1f}%  RV={rv:.1f}%  "
            f"IV/RV={ratio:.2f}  [{regime.upper()}]\n"
            f"   → {hint}"
            if iv and rv else
            f"⚠️  {sym} 数据拉取失败，维持中性"
        )

    return "\n".join(lines)
