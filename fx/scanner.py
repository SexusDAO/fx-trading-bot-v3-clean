"""
fx/scanner.py — 扫描 + 分类 + 信号判断

每 5 分钟由 cron / main.py 触发，全程纯 Python 计算，零 LLM 调用。
同一标的同时满足 reversal 和 breakout 条件时，用固定规则（_llm_classify）决定模式。

输出：信号列表（不含入场价/止损价，由 executor 实时计算）
"""

import sys
import json
import time
import logging
import concurrent.futures
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent))

# ── 新闻信号模块（可选，失败自动降级）──────────────────────────────────────
try:
    from shared.news_signal import get_news_bias as _get_news_bias
    _NEWS_SIGNAL_OK = True
except Exception as _e:
    _NEWS_SIGNAL_OK = False
    def _get_news_bias(sym): return None  # type: ignore

# ── 从 config 统一 import，文件顶部一次性完成 ─────────────────────────────────
from config import (
    # 候选池
    POOL_VOLUME_TOP_N, POOL_GAINERS_N, POOL_LOSERS_N,
    POOL_FUNDING_EXTREME_N, POOL_FUNDING_EXTREME_MIN, POOL_LIQ_TOP_N,
    LIQ_BIAS_MIN_TOTAL_1H, LIQ_BIAS_MIN_RATIO,
    UNLOCK_FILTER_ENABLED, UNLOCK_FILTER_DAYS, UNLOCK_FILTER_PCT,
    POOL_MIN_VOLUME_USD, POOL_MAX_CHG24_PCT, POOL_STABLE_COINS,
    # 风控
    RISK_MAX_POSITIONS, RISK_MAX_LONG, RISK_MAX_SHORT,
    # 资金费率结算窗口
    FUNDING_SETTLE_HOURS, FUNDING_WINDOW_NORMAL, FUNDING_WINDOW_EXTREME,
    FUNDING_WINDOW_NORMAL_BEFORE, FUNDING_WINDOW_NORMAL_AFTER,
    FUNDING_WINDOW_EXTREME_BEFORE, FUNDING_WINDOW_EXTREME_AFTER,
    FUNDING_EXTREME_THRESH,
    # 分类阈值
    CLASSIFY_REVERSAL_CHG24_MIN, CLASSIFY_REVERSAL_FUNDING_MIN,
    CLASSIFY_REVERSAL_RSI4H_LONG, CLASSIFY_REVERSAL_RSI4H_SHORT,
    CLASSIFY_BREAKOUT_CHG24_MIN,
    # 信号判断参数（待填）
    # 前置过滤
    FILTER_MAX_FUNDING_RATE, FILTER_OI_DIVERGENCE_PRICE,
    FILTER_OI_DIVERGENCE_DROP, FILTER_MIN_LIQUIDITY_USD,
    # 挂单维护
    ORDER_MAINTAIN_EMA_CHANGE, ORDER_MAINTAIN_LIQ_ATR_MULT,
    ORDER_CANCEL_BTC_MOVE_PCT, ORDER_CANCEL_FUNDING_DELTA, ORDER_CANCEL_OI_DROP_PCT,
    ORDER_EXPIRE_BREAKOUT_MIN, ORDER_EXPIRE_PULLBACK_MIN,
    SCAN_SYMBOL_TIMEOUT_SEC, SCAN_TOTAL_TIMEOUT_SEC, SCAN_MAX_WORKERS, FUNDING_ARB_SCAN_WORKERS,
    # 信号排序
    SIGNAL_PRIORITY, SIGNAL_STRENGTH_ORDER,
    # 止损倍数（条件二验证用）
    SL_MULT_LOW, SL_MULT_MID, SL_MULT_HIGH,
)
from shared.risk_manager import is_circuit_open, check_market_shock, is_market_shock_active, is_post_shock_recovery
from shared.state import get_all_positions, update_oi_cache, get_oi_24h_ago
from shared import binance_api as api
from shared.judges import judge_pullback, judge_breakout, judge_reversal, judge_range_bounce, judge_stop_hunt, judge_oi_divergence, judge_ema_trend

logger = logging.getLogger("fx.scanner")

POOL_CACHE_FILE   = ROOT / "logs" / "candidate_pool.json"
SCANNER_LOG_FILE  = ROOT / "logs" / "scanner.log"
SIGNAL_ANALYTICS_FILE = ROOT / "logs" / "signal_analytics.jsonl"

# ── 路径质量评分（v3.81 质量择优）──────────────────────────────────────────────
_MARK_MAP_MAX_AGE_SEC = 30.0   # 批量标记价格用于「注册入场价」的最大允许年龄（秒），超过则单标的重取


# ── [P1 阶段0] K 线请求耗时统计（开关 config.SCAN_KLINE_TIMING）────────────────
#   analyze_symbol 在线程池里并发运行：每个线程用 threading.local 累计自己这次分析的 K 线请求次数/耗时，
#   分析结束时并入本轮汇总（加锁），run_scan 并行分析结束后输出一条 info 汇总。
import threading as _threading

_KL_TLS = _threading.local()
_KL_ROUND_LOCK = _threading.Lock()
_KL_ROUND: dict = {"calls": 0, "ms": 0.0, "per_sym": {}}


def _kline_timing_enabled() -> bool:
    try:
        import config as _c
        return bool(getattr(_c, "SCAN_KLINE_TIMING", True))
    except Exception:
        return True


def _kline_fetch(symbol: str, interval: str, limit: int) -> list:
    """api.get_klines 的计时包装：行为与直接调用完全一致，只额外累计耗时。"""
    if not _kline_timing_enabled():
        return api.get_klines(symbol, interval, limit)
    t0 = time.perf_counter()
    try:
        return api.get_klines(symbol, interval, limit)
    finally:
        acc = getattr(_KL_TLS, "acc", None)
        if acc is not None:
            acc[0] += 1
            acc[1] += (time.perf_counter() - t0) * 1000.0


_KL_CACHE_STATS0: dict = {"hit": 0, "miss": 0, "expired": 0}


def _kline_round_reset() -> None:
    """每轮扫描开始时清零汇总，并记录缓存统计快照（用于算本轮增量）。"""
    global _KL_CACHE_STATS0
    with _KL_ROUND_LOCK:
        _KL_ROUND["calls"] = 0
        _KL_ROUND["ms"] = 0.0
        _KL_ROUND["per_sym"] = {}
    cache = _KLINE_CACHE
    _KL_CACHE_STATS0 = cache.stats() if cache is not None else {"hit": 0, "miss": 0, "expired": 0}


def _liq_stats_line(hit: int, total: int, degraded: int) -> str:
    """[liq-stats] 本轮清算区统计日志行。hit/total 来自清算区缓存覆盖；degraded=本轮进入注册阶段的信号中 sl_degraded 的个数。"""
    pct = (hit / total * 100.0) if total else 0.0
    return f"[liq-stats] 本轮清算区: 命中{hit}个/扫描{total}个({pct:.0f}%) 缺失{degraded}个(sl_degraded)"


def _count_sl_degraded(signals) -> int:
    """统计信号列表中 sl_degraded=True 的个数（顶层或 params 内任一为真即计）。"""
    n = 0
    for sg in signals or []:
        try:
            if sg.get("sl_degraded") or (sg.get("params") or {}).get("sl_degraded"):
                n += 1
        except Exception:
            continue
    return n


def _kline_round_summary() -> list:
    """生成本轮 K 线请求汇总日志行（无请求时返回空列表）。"""
    with _KL_ROUND_LOCK:
        calls, ms, per = _KL_ROUND["calls"], _KL_ROUND["ms"], dict(_KL_ROUND["per_sym"])
    lines = []
    if calls:
        slow_sym, slow_ms = max(((k, v[1]) for k, v in per.items()), key=lambda x: x[1])
        lines.append(
            f"[scanner] 本轮K线请求: 总次数={calls} 总耗时={ms / 1000:.1f}s "
            f"平均每标的={ms / max(len(per), 1):.0f}ms 最慢标的={slow_sym}({slow_ms:.0f}ms)"
        )
    cache = _KLINE_CACHE
    if cache is not None:
        st = cache.stats()
        d = {k: st[k] - _KL_CACHE_STATS0.get(k, 0) for k in ("hit", "miss", "expired")}
        tot = d["hit"] + d["miss"] + d["expired"]
        if tot:
            lines.append(
                f"[scanner] K线缓存: 命中={d['hit']} 未命中={d['miss']} 过期={d['expired']} "
                f"命中率={d['hit'] / tot:.0%}"
            )
            try:
                sw = cache.swr_stats()
                lines[-1] += f" (其中旧值={sw['stale']} 后台刷新成功={sw['refresh_ok']} 失败={sw['refresh_fail']} 累计)"
            except Exception:
                pass
    return lines


# ── [P1 阶段1+2] K 线 REST 缓存（按收盘时间失效）────────────────────────────────
_KLINE_CACHE = None
_KLINE_CACHE_FAILED = False


def _get_kline_cache():
    """惰性加载缓存模块；import/构造失败则记录一次 warning 并永久降级为直接 REST。"""
    global _KLINE_CACHE, _KLINE_CACHE_FAILED
    if _KLINE_CACHE is not None:
        return _KLINE_CACHE
    if _KLINE_CACHE_FAILED:
        return None
    try:
        from shared.kline_cache import KlineCache
        _KLINE_CACHE = KlineCache()
    except Exception as e:
        _KLINE_CACHE_FAILED = True
        logger.warning(f"[scanner] K线缓存模块加载失败，降级为直接 REST: {e}")
        return None
    return _KLINE_CACHE


def _hub_enabled() -> bool:
    try:
        import config as _c
        return bool(getattr(_c, "MARKET_DATA_HUB_ENABLED", False))
    except Exception:
        return False


def _get_mark_price(symbol: str) -> dict:
    """单标的标记价：hub 开启且就绪且数据足够新时读 hub，否则回落 REST（开关关闭时与原来完全一致）。"""
    if _hub_enabled():
        try:
            from shared import market_data_hub as _hub
            if _hub.is_ready():
                data = _hub.get_mark_price(symbol)
                if data:
                    logger.debug(f"[hub] {symbol} price={data.get('markPrice')} age={data.get('age_sec')}s")
                    return data
        except Exception as e:
            logger.debug(f"[hub] 读取失败，回落 REST: {e}")
    return api.get_mark_price(symbol)


def _get_all_mark_prices_list() -> list:
    """全市场标记价列表：hub 开启且就绪且快照非空时读 hub，否则回落 REST。"""
    if _hub_enabled():
        try:
            from shared import market_data_hub as _hub
            if _hub.is_ready():
                snap = _hub.get_all_mark_prices()
                if snap:
                    logger.debug(f"[hub] 批量标记价 {len(snap)} 个标的（来自 WS 缓存）")
                    return list(snap.values())
        except Exception as e:
            logger.debug(f"[hub] 批量读取失败，跳过本轮: {e}")
    # hub未就绪时返回空，不fallback REST（避免WS断线时触发大量慢请求）
    return []


_HUB_SUBSCRIBE_HOLD = False   # 启动分批：P4 之前为 True，暂缓首次 K 线订阅（订阅会触发历史 K 线 REST 加载）


def set_hub_subscribe_hold(hold: bool) -> None:
    """main 启动分批用：True=暂缓 hub K 线订阅，False=放开。默认放开（run_once/回测等不受影响）。"""
    global _HUB_SUBSCRIBE_HOLD
    _HUB_SUBSCRIBE_HOLD = bool(hold)


def _hub_subscribe_klines(symbols) -> None:
    """把本轮候选池交给 hub 订阅 K 线（hub 关闭时不做任何事）。"""
    if not _hub_enabled():
        return
    if _HUB_SUBSCRIBE_HOLD:
        logger.debug("[hub] 启动分批 P4 前暂缓 K 线订阅")
        return
    try:
        from shared import market_data_hub as _hub
        _hub.subscribe_klines(symbols)
    except Exception as e:
        logger.debug(f"[hub] K线订阅注入失败（忽略）: {e}")


def _get_klines_hub_or_rest(symbol: str, interval: str, limit: int) -> list:
    """K 线（直接 REST 语义）：hub 开启且该序列就绪时读 hub，None 则回落 api.get_klines（开关关闭时与原来完全一致）。
    与 _get_klines_cached 的区别：不经过 kline_cache，回落时就是原来的单次 REST 调用。"""
    if _hub_enabled():
        try:
            from shared import market_data_hub as _hub
            _hd = _hub.get_klines(symbol, interval, limit)
            if _hd:
                logger.debug(f"[hub] {symbol} {interval} klines n={len(_hd)}（来自 WS 缓存）")
                return _hd
        except Exception as e:
            logger.debug(f"[hub] K线读取失败，回落 REST: {e}")
    return api.get_klines(symbol, interval, limit)


def _get_ticker_24h_cached(max_age_sec: float) -> dict:
    """全市场 24h 行情 {symbol: ticker}：hub 开启、就绪且覆盖率足够时读 hub，否则回落 REST 缓存（开关关闭时与原来完全一致）。
    miniTicker@arr 只推有变化的标的，所以新鲜条目数低于「已见过条目数」的 TICKER_MIN_COVERAGE 时不采用 hub，避免市场情绪等全量统计的口径变化。"""
    if _hub_enabled():
        try:
            from shared import market_data_hub as _hub
            if _hub.is_ticker_ready():
                snap = _hub.get_all_tickers()
                fresh, seen = _hub.ticker_coverage()
                if snap and seen and fresh / seen >= _hub.TICKER_MIN_COVERAGE:
                    logger.debug(f"[hub] 24h ticker {len(snap)} 个标的（来自 WS 缓存，覆盖率 {fresh}/{seen}）")
                    return snap
                logger.debug(f"[hub] 24h ticker 覆盖率不足({fresh}/{seen})，回落 REST")
        except Exception as e:
            logger.debug(f"[hub] 24h ticker 读取失败，回落 REST: {e}")
    from shared.binance_api import get_ticker_24h_cached
    return get_ticker_24h_cached(max_age_sec=max_age_sec)


def _get_klines_cached(symbol: str, interval: str, limit: int) -> list:
    """优先读缓存（命中时只额外请求 1 根未收盘 K 线）；未命中/失效/未启用/模块不可用时走 REST 并写回。
    返回值与 api.get_klines 格式一致。
    hub 开启且该 (标的, 周期) 的 WS K线就绪时优先读 hub；返回 None 时走下面原有的缓存/REST 逻辑。"""
    if _hub_enabled():
        try:
            from shared import market_data_hub as _hub
            _hd = _hub.get_klines(symbol, interval, limit)
            if _hd:
                logger.debug(f"[hub] {symbol} {interval} klines n={len(_hd)}（来自 WS 缓存）")
                return _hd
        except Exception as e:
            logger.debug(f"[hub] K线读取失败，回落 REST: {e}")
    try:
        import config as _c
        enabled = bool(getattr(_c, "KLINE_CACHE_ENABLED", True)) and \
            interval in tuple(getattr(_c, "KLINE_CACHE_INTERVALS", ("1h", "4h", "15m")))
    except Exception:
        enabled = False
    cache = _get_kline_cache() if enabled else None
    if cache is None:
        return _kline_fetch(symbol, interval, limit)
    try:
        data = cache.get(symbol, interval, limit, fetch_live=lambda: _kline_fetch(symbol, interval, 1),
                         refresh=lambda: _kline_fetch(symbol, interval, limit))   # 过期后先用旧值，后台刷新
    except Exception as e:
        logger.debug(f"[scanner] K线缓存读取异常（降级 REST）: {e}")
        data = None
    if data:
        return data
    data = _kline_fetch(symbol, interval, limit)
    if data:
        try:
            cache.set(symbol, interval, data)
        except Exception as e:
            logger.debug(f"[scanner] K线缓存写入异常（忽略）: {e}")
    return data


def _calc_route_quality(route: str, reason: dict, ind: dict, direction: str) -> int:
    """
    同一标的多条路径都通过 judge 时，用此函数评估相对质量，取分最高的路径输出。
    四个维度，满分 100（实际最高 40+25+15+20=100）：
      ① 信号强度        40 分
      ② 策略与市场模式适配 25 分
      ③ 方向与 BTC 趋势一致性 15 分
      ④ 清算图质量       20 分
    """
    # ── ① 信号强度 ──────────────────────────────────────────────────────────────
    _strength = (reason or {}).get("signal_strength", "medium")
    _strength_score = {"strong": 40, "medium": 25, "weak": 10}.get(_strength, 25)

    # ── ② 策略与市场模式适配 ───────────────────────────────────────────────────────
    # market_mode 优先从 ind 读（run_scan 注入），fallback 调 get_market_mode()（5min 缓存）
    _mkt = ind.get("market_mode", "")
    if not _mkt:
        try:
            from shared.risk_manager import get_market_mode as _gmm
            _mkt = _gmm() or "range"
        except Exception:
            _mkt = "range"
    _TYPE_MKT: dict[tuple, int] = {
        ("reversal",     "bull"): 5,
        ("reversal",     "bear"): 20,
        ("reversal",     "range"): 15,
        ("pullback",     "bull"): 25,
        ("pullback",     "bear"): 15,
        ("pullback",     "range"): 18,
        ("breakout",     "bull"): 22,
        ("breakout",     "bear"): 10,
        ("breakout",     "range"): 5,
        ("ema_trend",    "bull"): 25,
        ("ema_trend",    "bear"): 5,
        ("ema_trend",    "range"): 10,
        ("oi_divergence","bull"): 18,
        ("oi_divergence","bear"): 18,
        ("oi_divergence","range"): 18,
        ("stop_hunt",    "bull"): 18,
        ("stop_hunt",    "bear"): 18,
        ("stop_hunt",    "range"): 18,
        ("range_bounce", "bull"): 12,
        ("range_bounce", "bear"): 8,
        ("range_bounce", "range"): 22,
    }
    _mkt_for_type = _mkt
    if route == "ema_trend" and direction == "SHORT" and _mkt in ("bull", "bear"):
        _mkt_for_type = "bear" if _mkt == "bull" else "bull"   # v3.83：空头趋势跟随顺势环境互换
    _mkt_score = _TYPE_MKT.get((route, _mkt_for_type), 15)

    # ── ③ 方向与 BTC 趋势一致性 ───────────────────────────────────────────────────
    _btc = ind.get("btc_trend", "neutral")
    _dir_up  = direction == "LONG"
    _btc_up  = _btc == "up"
    _btc_dn  = _btc == "down"
    if _btc == "neutral":
        _trend_score = 8
    elif (_dir_up and _btc_up) or (not _dir_up and _btc_dn):
        _trend_score = 15   # 顺势
    else:
        _trend_score = 3    # 逆势

    # ── ④ 清算图质量 ──────────────────────────────────────────────────────────────
    _liq_ok  = bool((reason or {}).get("liq_zone_valid"))
    _liq_amt = float((reason or {}).get("liq_zone_amount") or 0)
    if not _liq_ok:
        _liq_score = 0
    elif _liq_amt >= 3_000_000:
        _liq_score = 20
    elif _liq_amt >= 1_000_000:
        _liq_score = 12
    else:
        _liq_score = 6

    return _strength_score + _mkt_score + _trend_score + _liq_score


# ── Signal Analytics 写入函数 ───────────────────────────────────────────────────
def _write_signal_analytics(
    symbol: str,
    mode: str,
    direction: str,
    judge_passed: bool,
    reject_reason: str | None,
    ind: dict,
    reason: dict,
    btc_trend: str = "",
    market_mode: str = "",
    entry_params: dict | None = None,
    route_quality: int | None = None,
    competing_routes: list | None = None,
    entry_params_ok: bool | None = None,
    final_score: float | None = None,
) -> None:
    """每次 judge 结果（通过或拒绝）写入一条结构化记录到 signal_analytics.jsonl。"""
    import time as _t
    try:
        from config import SIGNAL_ANALYTICS_MAX_SIZE_MB
        _max_bytes = SIGNAL_ANALYTICS_MAX_SIZE_MB * 1024 * 1024
    except Exception:
        _max_bytes = 10 * 1024 * 1024

    # 文件轮转
    _fp = SIGNAL_ANALYTICS_FILE
    if _fp.exists() and _fp.stat().st_size > _max_bytes:
        import shutil
        for _i in range(4, 0, -1):
            _src = _fp.parent / f"{_fp.stem}.{_i}{_fp.suffix}"
            _dst = _fp.parent / f"{_fp.stem}.{_i+1}{_fp.suffix}"
            if _src.exists(): shutil.move(str(_src), str(_dst))
        shutil.move(str(_fp), str(_fp.parent / f"{_fp.stem}.1{_fp.suffix}"))

    _now_ms = int(_t.time() * 1000)
    _ts_str = __import__('datetime').datetime.utcfromtimestamp(_now_ms / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")

    _rec = {
        "ts":            _now_ms,
        "ts_readable":   _ts_str,
        "symbol":        symbol,
        "mode":          mode,
        "direction":     direction,
        "btc_trend":     btc_trend,
        "market_mode":   market_mode,
        "volatility_tier": (reason or {}).get("volatility_tier") or ind.get("volatility_tier", ""),
        "judge_passed":  judge_passed,
        "reject_reason": reject_reason,
        "signal_strength": (reason or {}).get("signal_strength") if judge_passed else None,
        "route_quality":  route_quality,
        "competing_routes": competing_routes or [],
        # 通过记录写盘时评分层（_apply_score_*）尚未运行，ind 里没有 final_score；调用方传入基础分兜底，
        # final_score_stage 标明口径：base=信号强度基础分（评分层之前）；final=评分层之后的最终分。评分层最终分由 entry_params_patch 补写。
        "final_score":   ind.get("final_score") if ind.get("final_score") is not None else final_score,
        "final_score_stage": "final" if ind.get("final_score") is not None else ("base" if final_score is not None else None),
        "snapshot": {
            "price":        ind.get("price"),
            "chg24":        round(ind.get("chg24", 0), 2),
            "atr":          ind.get("atr_1h"),
            "atr_ratio":    round(ind.get("atr_ratio", 0), 3),
            "rsi_1h":       round(ind.get("rsi_1h", 0), 1),
            "rsi_4h":       round(ind.get("rsi_4h", 0), 1),
            "vol_ratio":    round(ind.get("vol_ratio", 0), 3),
            "funding":      ind.get("funding"),
            "ema20_1h":     ind.get("ema20_1h"),
            "ema50_1h":     ind.get("ema50_1h"),
            "liq_data_available": bool((reason or {}).get("liq_zone_valid")),
            "liq_zone_amount":    (reason or {}).get("liq_zone_amount"),
        },
        "entry_params": entry_params or {
            "entry_price":  None, "sl_price":   None,
            "tp1_price":    None, "tp2_price":  None,
            "sl_pct":       None, "rr_tp1":     None,
            "sl_degraded":  None, "entry_mode": None,
        },
        # [add] entry_params_ok: calc_sl_tp 是否成功返回完整参数
        # None = judge 通过但 calc_sl_tp 尚未运行（analyze_symbol 阶段写入时）
        # True = calc_sl_tp 成功，信号真正进入等待成交队列
        # False = calc_sl_tp 返回 {"error": ...}，信号被静默丢弃
        "entry_params_ok": entry_params_ok,
        "outcome": {
            "filled":            None,
            "fill_price":        None,
            "fill_ts":           None,
            "exit_reason":       None,
            "exit_price":        None,
            "pnl_pct":           None,
            "max_favorable_pct": None,
            "max_adverse_pct":   None,
            "checked_at":        None,
        },
    }
    try:
        with open(_fp, "a", encoding="utf-8") as _f:
            _f.write(json.dumps(_rec, ensure_ascii=False) + "\n")
    except Exception as _e:
        logger.debug(f"[analytics] 写入失败: {_e}")

# ── 技术指标（统一从 shared/indicators 导入）────────────────────────────────
from shared.indicators import ema as _ema, rsi as _rsi, atr as _atr, vol_ma as _vol_ma, bollinger as _bollinger


def _is_stock_token_tradeable(symbol: str) -> bool:
    """
    判断股票类代币当前是否在允许开仓的交易时段（UTC 小时，左闭右开；周六日全天不允许）。
    非股票类代币直接返回 True；开关 STOCK_TOKEN_FILTER_ENABLED=False 或读配置失败时一律返回 True。
    只用于开仓扫描候选池过滤，不影响持仓维护。
    """
    try:
        from config import (
            STOCK_TOKEN_FILTER_ENABLED,
            STOCK_TOKENS_US, STOCK_TOKENS_US_HOURS_UTC,
            STOCK_TOKENS_HK, STOCK_TOKENS_HK_HOURS_UTC,
            STOCK_TOKENS_KR, STOCK_TOKENS_KR_HOURS_UTC,
            STOCK_TOKENS_CN, STOCK_TOKENS_CN_HOURS_UTC,
        )
        if not STOCK_TOKEN_FILTER_ENABLED:
            return True
    except Exception:
        return True

    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc)
    now_hour = now_utc.hour
    now_weekday = now_utc.weekday()  # 0=周一, 5=周六, 6=周日（UTC）

    # 美/港/韩/A 股周末均不开市：周六日全天不允许开仓
    if symbol in STOCK_TOKENS_US:
        if now_weekday >= 5:
            return False
        start, end = STOCK_TOKENS_US_HOURS_UTC
        return start <= now_hour < end
    if symbol in STOCK_TOKENS_HK:
        if now_weekday >= 5:
            return False
        start, end = STOCK_TOKENS_HK_HOURS_UTC
        return start <= now_hour < end
    if symbol in STOCK_TOKENS_KR:
        if now_weekday >= 5:
            return False
        start, end = STOCK_TOKENS_KR_HOURS_UTC
        return start <= now_hour < end
    if symbol in STOCK_TOKENS_CN:
        if now_weekday >= 5:
            return False
        start, end = STOCK_TOKENS_CN_HOURS_UTC
        return start <= now_hour < end

    return True  # 非股票类代币，不过滤


def _is_weekend_period(now: datetime) -> bool:
    """
    周末低流动性时段（v3.42）：
    按实际市场流动性边界划分：
    - UTC 周五 22:00 起（美盘 13:00-22:00 收盘后，流动性真正下降）
    - UTC 周六、周日全天
    - UTC 周一 00:00 亚盘开盘后自然退出（不需要额外条件）
    北京时间对应：周六 06:00 → 周一 08:00
    原 v3.29 设定为周五 UTC 20:00 起，会错过周五 20:00-22:00 的欧美重叠尾盘
    和周一 UTC 00:00-08:00 的亚盘开盘时段（两段均为有效流动性区间）。
    """
    wd = now.isoweekday()  # 1=Mon ... 7=Sun
    h  = now.hour
    if wd == 5 and h >= 22:   # UTC 周五 22:00+ 美盘收盘
        return True
    if wd in (6, 7):           # 周六、周日全天
        return True
    return False


# ── v3.92 周末新开仓计数器（主扫描 + funding_arb 共用；按注册计，进程内存，不持久化）──
_WEEKEND_POS_COUNT = 0
_WEEKEND_WINDOW_START = 0.0   # 当前周末窗口起点（UTC 周五 22:00）时间戳


def _weekend_window_start_ts(now: datetime) -> float:
    """返回 now 所在周末窗口的起点（UTC 周五 22:00）时间戳；调用方需保证 now 处于周末。"""
    from datetime import timedelta as _td
    _n = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    _days_back = (_n.isoweekday() - 5) % 7
    _fri = (_n - _td(days=_days_back)).replace(hour=22, minute=0, second=0, microsecond=0)
    if _fri > _n:
        _fri -= _td(days=7)
    return _fri.timestamp()


def _weekend_counter_sync(now: datetime) -> int:
    """进入新周末窗口时归零计数器，返回当前计数。"""
    global _WEEKEND_POS_COUNT, _WEEKEND_WINDOW_START
    if not _is_weekend_period(now):
        return _WEEKEND_POS_COUNT
    _ws = _weekend_window_start_ts(now)
    if _ws != _WEEKEND_WINDOW_START:
        _WEEKEND_WINDOW_START = _ws
        _WEEKEND_POS_COUNT = 0
    return _WEEKEND_POS_COUNT


def _weekend_counter_inc() -> int:
    global _WEEKEND_POS_COUNT
    _WEEKEND_POS_COUNT += 1
    return _WEEKEND_POS_COUNT


def _is_weekend_cutoff(now: datetime) -> bool:
    """周日 UTC WEEKEND_CUTOFF_SUNDAY_UTC_HOUR 起为收尾时段，不再注册新信号。"""
    from config import WEEKEND_CUTOFF_SUNDAY_UTC_HOUR
    return (_is_weekend_period(now) and now.isoweekday() == 7
            and now.hour >= WEEKEND_CUTOFF_SUNDAY_UTC_HOUR)


def _vol_ratio_closed(k1hc: list) -> float:
    """最后一根已收盘 1H 量 ÷ 其前 20 根已收盘 1H 均量；数据不足返回 1.0。"""
    try:
        if len(k1hc) < 21:
            return 1.0
        base = [float(k[5]) for k in k1hc[-21:-1]]
        ma = sum(base) / len(base)
        return float(k1hc[-1][5]) / ma if ma > 0 else 1.0
    except Exception:
        return 1.0


_BTC_1H_DROP_PCT = 0.0   # [opt-5] 每轮扫描更新；触发器路径复用上轮值


def _update_btc_1h_drop() -> float:
    """BTC 1H 跌幅% = (当前价 - 上一根已收盘 1H 收盘价) / 后者 × 100；失败保留旧值。"""
    global _BTC_1H_DROP_PCT
    try:
        k = api.get_klines("BTCUSDT", "1h", 3) or []
        if len(k) >= 2:
            cur, prev = float(k[-1][4]), float(k[-2][4])
            if prev > 0:
                _BTC_1H_DROP_PCT = (cur - prev) / prev * 100
    except Exception as e:
        logger.debug(f"[scanner] BTC 1H 跌幅计算失败，沿用旧值: {e}")
    return _BTC_1H_DROP_PCT


def calc_indicators(k15: list, k1h: list, k4h: list) -> dict:
    """
    基于已闭合 K 线计算全部技术指标。
    注意：使用 k[:-1] 剔除当前未闭合 K 线（最后一根）。
    """
    # 只用已闭合 K 线
    k15c = k15[:-1] if len(k15) > 1 else k15
    k1hc = k1h[:-1] if len(k1h) > 1 else k1h
    k4hc = k4h[:-1] if len(k4h) > 1 else k4h

    c15  = [float(k[4]) for k in k15c]
    c1h  = [float(k[4]) for k in k1hc]
    c4h  = [float(k[4]) for k in k4hc]

    price = float(k1h[-1][4])  # 当前最新价（含未闭合）

    # EMA（三周期）
    ema9_15  = _ema(c15, 9)
    ema20_15 = _ema(c15, 20)
    ema50_15 = _ema(c15, 50) if len(c15) >= 50 else _ema(c15, len(c15))
    ema9_1h  = _ema(c1h, 9)
    ema20_1h = _ema(c1h, 20)
    ema50_1h = _ema(c1h, 50) if len(c1h) >= 50 else _ema(c1h, len(c1h))
    ema9_4h  = _ema(c4h, 9)  if len(c4h) >= 9  else 0.0
    ema20_4h = _ema(c4h, 20) if len(c4h) >= 20 else 0.0
    ema50_4h = _ema(c4h, 50) if len(c4h) >= 50 else 0.0

    # RSI14（1H 和 4H）
    rsi_1h = _rsi(c1h)
    rsi_4h = _rsi(c4h) if len(c4h) >= 15 else 50.0

    # ATR14（1H）
    atr_1h     = _atr(k1hc)
    atr_ratio  = atr_1h / price * 100 if price > 0 else 0  # 波动烈度 %

    # 布林带（1H）
    bb = _bollinger(c1h)

    # 成交量指标（1H）
    vols_1h    = [float(k[5]) for k in k1hc]
    vol_ma20   = sum(vols_1h[-20:]) / 20 if len(vols_1h) >= 20 else (sum(vols_1h) / len(vols_1h) if vols_1h else 0)
    vol_now    = float(k1h[-1][5])  # 当前 K 线成交量（含未闭合，只用于量比参考）
    vol_ratio  = vol_now / vol_ma20 if vol_ma20 > 0 else 0

    # v3.58: Taker Buy Volume（主动买入量）——K线第10字段（index 9）
    # k[9] = Taker Buy Base Asset Volume，k[5]-k[9] = 主动卖出量
    # taker_buy_ratio > 0.5 = 买方主导；< 0.5 = 卖方主导
    try:
        _taker_buy_vols = [float(k[9]) for k in k1hc if len(k) > 9]
        _taker_buy_ma20 = sum(_taker_buy_vols[-20:]) / min(20, len(_taker_buy_vols)) if _taker_buy_vols else 0
        _taker_buy_now  = float(k1h[-1][9]) if len(k1h[-1]) > 9 else 0
        # 买卖比：近20根均值（过滤单根噪音），范围 0~1
        taker_buy_ratio = _taker_buy_ma20 / (vol_ma20 + 1e-9) if vol_ma20 > 0 else 0.5
        # 当前K线买卖比（用于实时判断）
        taker_buy_ratio_now = _taker_buy_now / (vol_now + 1e-9) if vol_now > 0 else 0.5
    except Exception:
        taker_buy_ratio     = 0.5  # 降级：中性
        taker_buy_ratio_now = 0.5

    # 近20根高低点（1H 已闭合）
    highs_20 = [float(k[2]) for k in k1hc[-20:]]
    lows_20  = [float(k[3]) for k in k1hc[-20:]]
    high_20  = max(highs_20) if highs_20 else price
    low_20   = min(lows_20)  if lows_20  else price

    # 24H 高低点（近25根1H）
    high_24h = max(float(k[2]) for k in k1hc[-25:]) if len(k1hc) >= 25 else price
    low_24h  = min(float(k[3]) for k in k1hc[-25:]) if len(k1hc) >= 25 else price

    return {
        "price":     price,
        # EMA
        "ema9_15m":  ema9_15,  "ema20_15m": ema20_15, "ema50_15m": ema50_15,
        "ema9_1h":   ema9_1h,  "ema20_1h":  ema20_1h, "ema50_1h":  ema50_1h,
        "ema9_4h":   ema9_4h,  "ema20_4h":  ema20_4h, "ema50_4h":  ema50_4h,
        # RSI
        "rsi_1h":    rsi_1h,
        "rsi_4h":    rsi_4h,
        # ATR
        "atr_1h":    atr_1h,
        "atr_ratio": atr_ratio,
        # 布林带
        "bb_upper":  bb["upper"], "bb_mid": bb["mid"],
        "bb_lower":  bb["lower"], "bb_pct": bb["pct"],
        # 成交量
        "vol_ma20":  vol_ma20,
        "vol_now":   vol_now,
        "vol_ratio": vol_ratio,
        "vol_ratio_closed": _vol_ratio_closed(k1hc),   # [opt-1] 最后一根已收盘 / 前 20 根已收盘均量
        # v3.58: 主动买入比（Taker Buy Volume）
        "taker_buy_ratio":     round(taker_buy_ratio, 4),      # 近20根均值买卖比
        "taker_buy_ratio_now": round(taker_buy_ratio_now, 4),  # 当前K线买卖比
        # 关键价位
        "high_20":   high_20, "low_20":  low_20,
        "high_24h":  high_24h, "low_24h": low_24h,
        # EMA 方向标志（1H）
        "ema_bull_1h": ema20_1h > ema50_1h,
        "ema_bull_4h": ema20_4h > ema50_4h if ema20_4h and ema50_4h else None,
    }

# ── 标的分类 ──────────────────────────────────────────────────────────────────
def _is_reversal(chg24: float, funding: float, ind: dict) -> bool:
    """
    分类层粗筛（OR 逻辑）：满足任意一个进入 reversal 路径。
    - 24H涨跌幅大（≥ CLASSIFY_REVERSAL_CHG24_MIN）
    - 资金费率极端
    - RSI 1h 超卖（<28，做多候选）或超买（>72，做空候选）
      → 小跌幅但 RSI 已极端 = 超卖反弹/超买回落，属于 reversal 而非 pullback
    第二层精筛在 judge_reversal 内部完成（多维度验证，可能因方向不明确再次拒绝）。
    """
    chg_ok  = abs(chg24) > CLASSIFY_REVERSAL_CHG24_MIN
    fund_ok = abs(funding) > CLASSIFY_REVERSAL_FUNDING_MIN
    rsi_1h  = ind.get("rsi_1h", 50)
    rsi_ok  = rsi_1h < 28 or rsi_1h > 72   # v3.64: 与 judge_reversal 精筛门槛对齐（28/72），修复 25~27 RSI 死区——粗筛说"不是reversal"→pullback拒绝说"应走reversal"→零信号
    return chg_ok or fund_ok or rsi_ok

def _is_breakout(chg24: float, ind: dict) -> bool:
    """
    分类层只验证涨跌幅（5% 门槛）和价格突破高低点。
    成交量验证移至 judge_breakout 精筛，分类层不再拦截。
    """
    price   = ind["price"]
    high_20 = ind["high_20"]
    low_20  = ind["low_20"]
    if abs(chg24) <= CLASSIFY_BREAKOUT_CHG24_MIN:
        return False
    if chg24 > 0 and price >= high_20:
        return True
    if chg24 < 0 and price <= low_20:
        return True
    return False


def _is_stop_hunt(k1h: list, k15: list, ind: dict) -> bool:
    """
    分类层粗筛（v3.49.1）：前高/前低必须是"有效结构"才进 judge。

    B 层（前高前低精确识别）——三个条件全部满足：
      1. 是最近 20 根 1H 的全局最低/最高收盘价
      2. 该极值点左右各 3 根 1H 都没有更低/更高的收盘价（真正的局部拐点）
      3. 极值形成后，后续至少 4 根 1H 都没有再次触碰它（位置有记忆）

    A 层（量能预检查）——粗筛通过后叠加：
      最近 6 根已闭合 15M 里，至少一根同时满足：
        - 量比 > 1.5x 过去 20 根 15M 均量
        - 该根最低价低于 pivot_low * 0.997 或最高价高于 pivot_high * 1.003（刺穿 0.3%）
    """
    if len(k1h) < 28 or len(k15) < 28:
        return False

    closes_1h = [float(k[4]) for k in k1h[:-1]]
    if len(closes_1h) < 24:
        return False

    recent_1h = closes_1h[-20:]
    CONF_BARS  = 3   # 左右各需 3 根确认
    MEMORY_BARS = 4  # 极值后至少 4 根未触碰

    # ── B1: 找全局最低收盘价并做有效性检查 ─────────────────────────────────
    pivot_low  = None
    pivot_high = None

    global_min = min(recent_1h)
    global_max = max(recent_1h)

    # 找 global_min 的索引（取最早出现的）
    min_idx = next(i for i, v in enumerate(recent_1h) if v == global_min)
    # 条件2：左右各 CONF_BARS 根都比它高
    if (min_idx >= CONF_BARS and min_idx <= len(recent_1h) - CONF_BARS - 1):
        left_ok  = all(recent_1h[min_idx - j] > global_min for j in range(1, CONF_BARS + 1))
        right_ok = all(recent_1h[min_idx + j] > global_min for j in range(1, CONF_BARS + 1))
        if left_ok and right_ok:
            # 条件3：极值形成后（min_idx+1 之后），至少 MEMORY_BARS 根未再触碰
            after = recent_1h[min_idx + 1:]
            touch_threshold = global_min * 1.003  # 0.3% 以内算触碰
            if len(after) >= MEMORY_BARS:
                untouched = sum(1 for v in after if v > touch_threshold)
                if untouched >= MEMORY_BARS:
                    pivot_low = global_min

    # 找 global_max 的索引
    max_idx = next(i for i, v in enumerate(recent_1h) if v == global_max)
    if (max_idx >= CONF_BARS and max_idx <= len(recent_1h) - CONF_BARS - 1):
        left_ok  = all(recent_1h[max_idx - j] < global_max for j in range(1, CONF_BARS + 1))
        right_ok = all(recent_1h[max_idx + j] < global_max for j in range(1, CONF_BARS + 1))
        if left_ok and right_ok:
            after = recent_1h[max_idx + 1:]
            touch_threshold = global_max * 0.997
            if len(after) >= MEMORY_BARS:
                untouched = sum(1 for v in after if v < touch_threshold)
                if untouched >= MEMORY_BARS:
                    pivot_high = global_max

    if pivot_low is None and pivot_high is None:
        return False  # 没有有效结构，直接跳过

    # ── A 层：量能预检查 ────────────────────────────────────────────────────
    # 计算 15M 均量（用 k15 倒数 7~26 根，不含最近 6 根）
    k15_closed = k15[:-1]  # 去掉未闭合最后一根
    if len(k15_closed) < 26:
        return False
    vol_slice  = k15_closed[-(26):-6]  # 20 根
    if len(vol_slice) < 5:
        return False
    vol_ma = sum(float(k[5]) for k in vol_slice) / len(vol_slice)
    if vol_ma <= 0:
        return False

    recent_k15 = k15_closed[-6:]  # 最近 6 根已闭合
    pierce_vol_ratio_min = 1.5
    pierce_pct_min       = 0.003  # 0.3%

    for bar in recent_k15:
        bar_low   = float(bar[3])
        bar_high  = float(bar[2])
        bar_vol   = float(bar[5])
        vol_ratio = bar_vol / vol_ma
        if vol_ratio < pierce_vol_ratio_min:
            continue
        # 刺穿前低
        if pivot_low is not None and bar_low <= pivot_low * (1 - pierce_pct_min):
            return True
        # 刺穿前高
        if pivot_high is not None and bar_high >= pivot_high * (1 + pierce_pct_min):
            return True

    return False


def _is_ema_trend_coarse(ind: dict, chg24: float) -> bool:
    """
    EMA 趋势策略粗筛（不依赖4H数据）。
    只检查不需要4H K线的条件，满足才加入候选路由，
    触发4H懒加载后由 _is_ema_trend / judge 做精筛。
    条件：
    - CHG24 在 -8%~+12% 范围内
    - RSI_1h 在 45~72 范围内
    """
    if chg24 > 12.0 or chg24 < -8.0:
        return False
    rsi_1h = ind.get("rsi_1h", 50)
    if not (45 <= rsi_1h <= 72):
        return False
    return True


def _is_ema_trend(ind: dict, chg24: float) -> bool:
    """
    EMA 趋势跟随策略粗筛（v3.61；v3.83 起支持空头排列）。
    宽松条件快速过滤，第二层精筛在 judge_ema_trend 内完成。
    多头：4H EMA20 > EMA50，1H RSI 45~70，价格在 EMA20 的 -2.5%~+3%。
    空头：4H EMA20 < EMA50，1H RSI 45~72，价格在 EMA20 的 -3%~+2.5%（反弹到阻力附近）。
    CHG24 在 -8%~+12% 范围内（两个方向相同，与 judge 一致）。
    v3.83 修复：judge 在 v3.64 已加入 SHORT 分支，但粗筛只放行多头排列，SHORT 分支永远走不到。
    """
    ema20_4h = ind.get("ema20_4h", 0.0)
    ema50_4h = ind.get("ema50_4h", 0.0)
    if ema20_4h <= 0 or ema50_4h <= 0 or ema20_4h == ema50_4h:
        return False
    is_long = ema20_4h > ema50_4h
    rsi_1h = ind.get("rsi_1h", 50)
    if not (45 <= rsi_1h <= (70 if is_long else 72)):
        return False
    if chg24 > 12.0 or chg24 < -8.0:
        return False
    price = ind.get("price", 0.0)
    if price <= 0:
        return False
    pb = (price - ema20_4h) / (ema20_4h + 1e-9)
    if is_long:
        return -0.025 <= pb <= 0.03
    return -0.03 <= pb <= 0.025


def _is_oi_divergence(ind: dict) -> bool:
    """
    OI 背离策略粗筛（v3.60）。
    宽松条件快速过滤，第二层精筛在 judge_oi_divergence 内完成。
    粗筛条件（比 judge 宽松，防止过早拦截）：
      - OI 数据有效（oi_now > 0 且 oi_1h_ago > 0）
      - ATR_ratio >= 0.5%（波动率基本达标）
      - OI 变化幅度 >= 8%（judge 要求 >= 10%）
      - 价格接近 24H 新高（>= 97%）或新低（<= 103%）
    """
    oi_now    = ind.get("oi_now", 0.0)
    oi_1h_ago = ind.get("oi_1h_ago", 0.0)
    if oi_now <= 0 or oi_1h_ago <= 0:
        return False
    price    = ind.get("price", 0.0)
    high_24h = ind.get("high_24h", 0.0)
    low_24h  = ind.get("low_24h", 0.0)
    if price <= 0 or high_24h <= 0 or low_24h <= 0:
        return False
    # 波动率前置过滤（尽量快）
    atr_r = ind.get("atr_ratio", 0.0)
    if atr_r < 0.8:   # [v3.100] 0.5%→0.8%: 过滤低波动死鱼币，扫描/触发器对齐
        return False
    # OI 变化幅度前置检查（[v3.100] 8%→6.5%，与触发器4.5%保持差距）
    oi_change = abs(oi_now - oi_1h_ago) / (oi_1h_ago + 1e-9)
    if oi_change < 0.065:
        return False
    # 价格位置：必须按近新高或新低
    near_low  = price <= low_24h  * 1.03
    near_high = price >= high_24h * 0.97
    return near_low or near_high


def _is_range_bounce(k1h: list, ind: dict) -> bool:
    """
    分类层粗筛：识别「大涨→横盘→当前价在区间下半部」形态（v3.35）。
    不依赖 chg24/RSI 极端值，依赖价格结构。
    精筛在 judge_range_bounce 内完成。
    """
    if len(k1h) < 15:
        return False
    closes  = [float(k[4]) for k in k1h[:-1]]
    price   = ind.get("price", closes[-1])
    atr_r   = ind.get("atr_ratio", 0)
    _weekend = ind.get("_weekend_mode", False)
    _atr_min = 1.5 if _weekend else 2.0
    if atr_r < _atr_min:
        return False
    # 近12根内找大涨高点（涨幅 ≥ 15%）
    recent12   = closes[-12:]
    peak_val   = max(recent12)
    peak_local = recent12.index(peak_val)
    peak_abs   = len(closes) - 12 + peak_local
    base_val   = closes[max(0, peak_abs - 6)]
    pre_gain   = (peak_val - base_val) / (base_val + 1e-9) * 100
    if pre_gain < 15:
        return False
    # 高点之后横盘区间（振幅 ≤ 20%，≥2根）
    post_peak = closes[peak_abs:]
    if len(post_peak) < 2:
        return False
    c_high  = max(post_peak)
    c_low   = min(post_peak)
    c_range = (c_high - c_low) / (c_low + 1e-9) * 100
    if c_range > 20:
        return False
    # 当前价在区间下半部（zone_mid 以下）
    zone_mid = (c_high + c_low) / 2
    return price <= zone_mid

def _llm_classify(symbol: str, chg24: float, funding: float, ind: dict) -> str:
    """
    多模式冲突时的分类决策（纯固定规则，无 LLM 调用）。
    规则：优先 reversal；仅当 reversal 条件弱（跌幅<20% 且 RSI>20）时选 breakout。
    """
    rsi_4h = ind.get("rsi_4h", 50.0)
    chg_abs = abs(chg24)
    if chg_abs >= 20 or rsi_4h < 20 or rsi_4h > 80:
        return "reversal"
    return "breakout"


def classify(symbol: str, chg24: float, funding: float, ind: dict, stock_closed: bool | None = None) -> str:
    """
    按优先级分类，优先级：reversal > breakout > pullback（兜底）
    多模式冲突时触发固定规则分类（_llm_classify）。
    """
    # v3.1: stock closed guard（调用方显式传入 stock_closed 时不再读时钟/外部模块）
    if stock_closed is not None:
        if stock_closed:
            return "skip_stock_closed"
    else:
        try:
            import sys as _s, pathlib as _p
            _s.path.insert(0, str(_p.Path(__file__).parent.parent))
            from calc_position_size_real import _is_stock_token, _us_market_open
            if _is_stock_token(symbol) and not _us_market_open():
                return "skip_stock_closed"
        except Exception:
            pass
    is_rev = _is_reversal(chg24, funding, ind)
    is_brk = _is_breakout(chg24, ind)

    if is_rev and is_brk:
        return _llm_classify(symbol, chg24, funding, ind)
    if is_rev:
        return "reversal"
    if is_brk:
        return "breakout"
    return "pullback"

# ── 信号判断函数已迁移到 shared/judges.py，此处直接使用导入的函数 ──────────────

# ── 资金费率结算窗口检查 ──────────────────────────────────────────────────────
def _in_funding_window(funding_abs: float) -> tuple[bool, str]:
    """
    判断当前是否在资金费率结算窗口内。
    返回 (在窗口内: bool, 原因说明: str)
    """
    now     = datetime.now(timezone.utc)
    hour    = now.hour
    minute  = now.minute

    # FUNDING_WINDOW_*_BEFORE/AFTER 存储的是秒，转换为分钟
    if funding_abs >= FUNDING_EXTREME_THRESH:
        pre_min  = FUNDING_WINDOW_EXTREME_BEFORE // 60
        post_min = FUNDING_WINDOW_EXTREME_AFTER  // 60
    else:
        pre_min  = FUNDING_WINDOW_NORMAL_BEFORE // 60
        post_min = FUNDING_WINDOW_NORMAL_AFTER  // 60

    # 检查是否在某个结算时点的前后窗口内
    for settle_h in FUNDING_SETTLE_HOURS:
        # 结算后窗口：settle_h:00 ~ settle_h:post_min
        if hour == settle_h and minute < post_min:
            return True, f"资金费率结算后窗口 {settle_h:02d}:00~{settle_h:02d}:{post_min:02d}"
        # 结算前窗口：(settle_h-1):（60-pre_min） ~ settle_h:00
        prev_h = (settle_h - 1) % 24
        if hour == prev_h and minute >= (60 - pre_min):
            return True, f"资金费率结算前窗口 {prev_h:02d}:{60-pre_min:02d}~{settle_h:02d}:00"
    return False, ""

def _in_symbol_funding_window(symbol: str, mp: dict, funding_abs: float) -> tuple[bool, str]:
    """
    [v3.85 OPT-6] 逐标的资金费结算窗口。
    原逻辑只按 UTC 0/8/16 整点 + BTC 费率判断，并整轮跳过扫描；但大量合约是 1H/4H 结算，
    它们在 4/12/20 点等结算时刻完全没被保护，而 8H 标的又因 BTC 一个值被整体停扫。
    新逻辑：用该标的 premiumIndex 的 nextFundingTime + 自身结算周期判断；拿不到时回退到旧的整点逻辑。
    """
    import time as _t
    from shared.opt_v385 import symbol_funding_window
    if funding_abs >= FUNDING_EXTREME_THRESH:
        pre_s, post_s = FUNDING_WINDOW_EXTREME_BEFORE, FUNDING_WINDOW_EXTREME_AFTER
    else:
        pre_s, post_s = FUNDING_WINDOW_NORMAL_BEFORE, FUNDING_WINDOW_NORMAL_AFTER
    try:
        nft = int(mp.get("nextFundingTime") or 0) / 1000.0
    except Exception:
        nft = 0.0
    if nft <= 0:
        return _in_funding_window(funding_abs)   # 回退：与原行为一致
    try:
        from shared.binance_api import get_funding_interval
        ih = get_funding_interval(symbol) or 8
    except Exception:
        ih = 8
    return symbol_funding_window(_t.time(), nft, ih, pre_s, post_s)


# ── OI 背离检查 ───────────────────────────────────────────────────────────────
_OI_24H_UNSET = object()   # oi_24h 参数的"未传入"哨兵（传 None 表示缓存不足 24H）


def _oi_divergence(symbol: str, price: float, chg24: float,
                   high_24h: float, oi_now: float = 0, oi_24h=_OI_24H_UNSET) -> tuple[bool, str]:
    """
    做多方向：价格接近24H高点（>=98%）且 OI 同期下降超过20% → 背离。
    OI 对比基准取自本地缓存（oi_cache.json），精度优于接口历史数据。
    缓存不足24H（系统刚启动）时直接跳过，不作为拒绝条件。
    """
    if chg24 <= 0:
        return False, ""   # 下跌方向不检查多单OI背离
    if price < high_24h * FILTER_OI_DIVERGENCE_PRICE:
        return False, ""   # 未到接近高点，不检查
    if oi_now <= 0:
        return False, ""   # 当前OI无效，跳过

    if oi_24h is _OI_24H_UNSET:
        oi_24h = get_oi_24h_ago(symbol)
    if oi_24h is None:
        logger.debug(f"[scanner] {symbol} LONG OI背离保护跳过（缓存不足24H，重启后24H内此保护不生效）")
        return False, ""   # 缓存数据不足24H，跳过

    oi_change = (oi_now - oi_24h) / oi_24h if oi_24h > 0 else 0
    if oi_change <= -FILTER_OI_DIVERGENCE_DROP:
        return True, (
            f"OI背离: 价格{price:.4g}接近24H高点{high_24h:.4g}, "
            f"OI由{oi_24h:.0f}降至{oi_now:.0f}({oi_change*100:.1f}%)"
        )
    return False, ""


def _oi_divergence_short(symbol: str, price: float, chg24: float,
                         low_24h: float, oi_now: float = 0, oi_24h=_OI_24H_UNSET) -> tuple[bool, str]:
    """
    做空方向 OI 背离检查：
    价格接近24H最低点（<= low_24h * 102%）且 OI 同期上升超过20%
    说明空头过度拥挤，容易被逼空反弹，禁止做空。

    注意方向与多单相反：
    - LONG 背离 = 多头出逃，OI 下降
    - SHORT 背离 = 空头拥挤，OI 上升
    """
    if chg24 >= 0:
        return False, ""   # 上涨方向不检查空单OI背离
    if low_24h <= 0:
        return False, ""
    if price > low_24h * 1.02:
        return False, ""   # 未接近24H最低点，不检查
    if oi_now <= 0:
        return False, ""

    if oi_24h is _OI_24H_UNSET:
        oi_24h = get_oi_24h_ago(symbol)
    if oi_24h is None:
        logger.debug(f"[scanner] {symbol} SHORT OI背离保护跳过（缓存不足24H，重启后24H内此保护不生效）")
        return False, ""   # 缓存不足24H，跳过

    oi_change = (oi_now - oi_24h) / oi_24h if oi_24h > 0 else 0
    if oi_change >= FILTER_OI_DIVERGENCE_DROP:   # OI 上升超过20%
        return True, (
            f"SHORT OI背离: 价格{price:.4g}接近24H低点{low_24h:.4g}, "
            f"OI由{oi_24h:.0f}升至{oi_now:.0f}(+{oi_change*100:.1f}%)，空头过度拥挤"
        )
    return False, ""

# ── 分析单个标的 ──────────────────────────────────────────────────────────────
def _prescreen_ticker(symbol: str, ticker: dict, loss_blacklist, is_weekend: bool) -> bool:
    """零成本预筛（纯函数，不拉任何接口）：返回 False 表示直接跳过该标的。"""
    if loss_blacklist and symbol in loss_blacklist:
        return False   # 黑名单标的直接跳过，无需拉 K 线
    # v3.54 ticker 快速预筛（5ms，拦截明显不符合任何策略条件的标的）
    _vol   = float(ticker.get("quoteVolume", 0))
    _h24   = float(ticker.get("highPrice", 0))
    _l24   = float(ticker.get("lowPrice", 0))
    _amp   = (_h24 - _l24) / (_l24 + 1e-9) * 100 if _l24 > 0 else 0.0
    # 1. 成交量过低 → 流动性不足，所有策略都不适合
    if _vol < 500_000:   # 50万U，比候选池门槛（100万）低，给误差空间
        return False
    # 2. 振幅过低 → ATR 必然不达标，reversal/breakout/pullback 均会在 judge 被拒
    _amp_min = 0.6 if is_weekend else 1.0   # 周末门槛更低
    if _amp < _amp_min:
        return False
    # 周末 ATR 代理预筛（v3.28）：用日内振幅代理 ATR，单独放宽到 0.8%
    if is_weekend:
        _h = float(ticker.get("highPrice", 0))
        _l = float(ticker.get("lowPrice",  0))
        _p = float(ticker.get("lastPrice", 0))
        if _h > 0 and _l > 0 and _p > 0:
            _amp_proxy = (_h - _l) / (_l + 1e-9) * 100
            from config import WEEKEND_AMP_PROXY_MIN
            if _amp_proxy < WEEKEND_AMP_PROXY_MIN:
                return False  # v3.46: 周末振幅代理低于门槛，预筛拒绝
    return True


def _build_signal(symbol: str, direction: str, sig_type: str, reason, chg24: float, funding: float,
                  ind: dict, oi_now: float, k1h: list, now_ms: int) -> dict:
    """由选定的路线/方向/reason 拼出信号 dict（纯计算：仓位上限、VP、stop_hunt 止损提示、资金费率协同）。"""
    # 输出信号（不含入场价/止损价，由 executor 实时计算）
    _sig = {
        "symbol":    symbol,
        "direction": direction,
        "sig_type":  sig_type,
        "chg24":     round(chg24, 2),
        "funding":   funding,
        "ind":       ind,
        "reason":    reason,
        "oi_now":    oi_now,
        "ts":        int(now_ms),
    }
    # [fix] 提升 reason 里的 position_size_cap / signal_strength 到顶层
    _sig["position_size_cap"] = float(
        (reason or {}).get("position_size_cap") or 0.5
    )
    _sig["signal_strength"] = (reason or {}).get("signal_strength", "medium")
    # v3.62: Volume Profile 注入——用建好的 1H K 线计算 VP 结构位
    try:
        from shared.indicators import calc_volume_profile as _cvp
        _vp_result = _cvp(k1h)
        if _vp_result.get("valid"):
            _sig["vp"] = _vp_result
            logger.debug(
                f"[scanner] {symbol} VP: poc={_vp_result['poc']:.4g} "
                f"vah={_vp_result['vah']:.4g} val={_vp_result['val']:.4g}"
            )
    except Exception as _vp_e:
        logger.debug(f"[scanner] {symbol} VP 计算跳过: {_vp_e}")
    # v3.49: stop_hunt 将 sl_price_hint 传入 signal，供 calc_sl_tp 覆盖止损
    if sig_type == "stop_hunt" and isinstance(reason, dict):
        _sl_hint = reason.get("sl_price_hint")
        if _sl_hint:
            _sig["sl_price_hint"] = _sl_hint
        _sig["signal_strength"] = reason.get("signal_strength", "medium")
    # ── 资金费率+主策略协同（v3.72）：单标的精细化仓位调整 ────────────────────
    try:
        from config import (
            FUNDING_SYNERGY_BONUS, FUNDING_SYNERGY_PENALTY,
            FUNDING_SYNERGY_THRESH,
        )
        if sig_type != "funding_arb" and abs(funding) > FUNDING_SYNERGY_THRESH:
            _funding_favors_long  = funding < 0   # 负费率 = 空头付费 = 多头收费
            _direction_is_long    = direction.upper() == "LONG"
            _is_synergy   = (_direction_is_long and _funding_favors_long) or (not _direction_is_long and not _funding_favors_long)
            _is_against   = (_direction_is_long and not _funding_favors_long) or (not _direction_is_long and _funding_favors_long)

            _cur_cap = float(_sig.get("position_size_cap", 0.5) or 0.5)
            if _is_synergy:
                _new_cap = round(min(_cur_cap * FUNDING_SYNERGY_BONUS, 0.80), 3)
                if _new_cap != _cur_cap:
                    _sig["position_size_cap"] = _new_cap
                    _sig["funding_synergy"]   = "bonus"
                    logger.info(
                        f"[scanner] {symbol} 费率协同加权: "
                        f"funding={funding*100:.4f}% 方向{direction} cap {_cur_cap:.2f}→{_new_cap:.2f}"
                    )
            elif _is_against:
                _new_cap = round(_cur_cap * FUNDING_SYNERGY_PENALTY, 3)
                if _new_cap != _cur_cap:
                    _sig["position_size_cap"] = _new_cap
                    _sig["funding_synergy"]   = "penalty"
                    logger.info(
                        f"[scanner] {symbol} 费率逆向降权: "
                        f"funding={funding*100:.4f}% 方向{direction} cap {_cur_cap:.2f}→{_new_cap:.2f}"
                    )
    except Exception as _fsy_e:
        logger.debug(f"[scanner] {symbol} 费率协同计算跳过: {_fsy_e}")
    return _sig


_CHG24_DEPENDENT_ROUTES = ("pullback", "reversal")


def _chg24_missing(chg24) -> bool:
    """chg24 为 None / NaN / 0 视为数据缺失（真实 24H 涨跌几乎不可能恰好为 0）。"""
    try:
        v = float(chg24)
    except (TypeError, ValueError):
        return True
    return v != v or abs(v) < 1e-12


def analyze_one(symbol: str, ticker: dict,
                k15: list, k1h: list, k4h: list,
                active_syms: set, pending_syms: set,
                allowed_dirs: set | None,
                loss_blacklist: set | None,
                btc_trend: str,
                market_mode: str,
                mark_price_map: dict,
                prefetch: dict,
                events: list | None = None) -> dict | None:
    """
    此函数同时被以下两个入口调用：
    - run_scan（主动批量轮询，当前模式）
    - TriggerDetector（被动事件触发，待实现于阶段3+）
    参数必须显式传入，函数内部不做任何 IO。

    纯计算入口：不发 REST/网络请求，不写文件，不读 scanner 层的 state；所有外部数据来自参数。
    （judges.py 内部自己的 state/liq_cache/consistency_log 文件读写保持原样，不在此范围。）
    _over_budget / _KL_TLS 计时、资金费结算窗口检查（含时钟与 REST）留在 _analyze_symbol_impl。

    prefetch 字段：
      oi_now / oi_1h_ago: float；oi_24h_ago: float | None；oi_hist_6: list（get_oi_history limit=6）
      funding_velocity: dict（_funding_rate_velocity 的返回值，调用 judge 前写入 judges._fr_velocity_cache）
      mp: dict（该标的 premiumIndex）；is_weekend / stock_closed: bool
      dir_cooldown_long / dir_cooldown_short: bool；now_ms: int
    events: 可选输出列表。需要落盘/联网的副作用以 dict 追加到这里，由调用方（主进程）在拿到返回值后统一处理
      （见 _apply_post_events）：analytics / shadow_reject / llm / analytics_pass。
    返回信号 dict 或 None；妖币 LLM 判定不在此函数内调用（以 llm 事件交给主进程）。
    """
    if events is None:
        events = []
    try:
        is_weekend = bool(prefetch.get("is_weekend"))
        if not _prescreen_ticker(symbol, ticker, loss_blacklist, is_weekend):
            return None
        if not k1h or len(k1h) < 25:
            return None

        chg24_rough = float(ticker.get("priceChangePercent", 0))
        # v3.83：熊市禁止 reversal LONG 只在该路径 judge 通过后拒绝
        _bear_block_rev_long = (btc_trend == "down" and chg24_rough <= -8)

        price   = float(k1h[-1][4])
        p24     = float(k1h[-25][4]) if len(k1h) >= 25 else float(k1h[0][4])
        chg24   = (price - p24) / p24 * 100

        mp      = prefetch.get("mp") or (mark_price_map or {}).get(symbol) or {}
        funding = float(mp.get("lastFundingRate", 0))

        # judge 内部的 _funding_rate_velocity 会发 HTTP：调用 judge 前把主进程预取的结果写进它的缓存，让它命中缓存
        _fv = prefetch.get("funding_velocity")
        if _fv is not None:
            from shared import judges as _judges_mod
            _judges_mod._fr_velocity_cache[symbol] = (_fv, time.time() + _judges_mod._FR_CACHE_TTL)

        # ── B. 先用 k4h=[] 计算 1H/15M 指标，用于分类 ────────────────────────
        if not market_mode:   # 上游未传/传空：不把空串写进 ind 与 analytics，明确回退 range 并留 warning
            logger.warning(f"[scanner] {symbol} market_mode 为空，回退 range")
            market_mode = "range"
        ind = calc_indicators(k15, k1h, [])
        ind["vol24_usd"]     = float(ticker.get("quoteVolume", 0))
        ind["symbol"]        = symbol
        ind["_weekend_mode"] = is_weekend
        ind["market_mode"]   = market_mode
        ind["chg24"]         = chg24     # analytics 快照读 ind["chg24"]；此前 analyze_one 未写入，样本里恒为 0

        oi_now = float(prefetch.get("oi_now") or 0.0)

        # v3.58: OI 历史时序（过去6根1H）——计算 OI 趋势方向和斜率
        oi_trend  = "neutral"
        oi_slope  = 0.0
        try:
            _oi_hist = prefetch.get("oi_hist_6")
            if _oi_hist and len(_oi_hist) >= 3:
                _oi_vals = [float(h["sumOpenInterest"]) for h in _oi_hist]
                _oi_first = _oi_vals[0]
                _oi_last  = _oi_vals[-1]
                oi_slope  = (_oi_last - _oi_first) / (_oi_first + 1e-9) * 100 / len(_oi_vals)
                if oi_slope > 0.3:
                    oi_trend = "rising"
                elif oi_slope < -0.3:
                    oi_trend = "falling"
        except Exception:
            pass
        ind["oi_trend"] = oi_trend
        ind["oi_slope"] = round(oi_slope, 4)
        ind["oi_now"]    = oi_now
        ind["oi_1h_ago"] = float(prefetch.get("oi_1h_ago") or 0.0)
        # [opt-2/5] 基差与 BTC 急跌字段（纯计算：来自 mark_price_map / prefetch）
        _mpi = prefetch.get("mp") or (mark_price_map or {}).get(symbol) or {}   # 与上方 mp 同一来源
        ind["markPrice"]  = float(_mpi.get("markPrice") or 0.0)
        ind["indexPrice"] = float(_mpi.get("indexPrice") or 0.0)
        ind["btc_1h_drop_pct"] = float(prefetch.get("btc_1h_drop_pct") or 0.0)

        # ── C. 标的分类 ────────────────────────────────────────────────────────
        # v3.38: 股票代币休市检查提前到路由之前，防止 range_bounce 绕过休市保护
        if prefetch.get("stock_closed"):
            logger.debug(f"[scanner] {symbol} 股票代币休市期间，跳过")
            return None

        _sh_enabled  = getattr(__import__('config'), 'STOP_HUNT_ENABLED', True)
        _sh_excluded = getattr(__import__('config'), 'STOP_HUNT_EXCLUDED_SYMBOLS', [])

        # 按优先级收集所有粗筛通过的候选路径
        _candidate_routes = []
        if _sh_enabled and symbol not in _sh_excluded and _is_stop_hunt(k1h, k15, ind):
            _candidate_routes.append("stop_hunt")
        if _is_oi_divergence(ind):
            _candidate_routes.append("oi_divergence")
        if _is_ema_trend_coarse(ind, chg24):
            _candidate_routes.append("ema_trend")
        if _is_range_bounce(k1h, ind):
            _candidate_routes.append("range_bounce")
        # classify() 的三条路拆开独立尝试（pullback 永远是兜底）
        _classify_type = classify(symbol, chg24, funding, ind, stock_closed=bool(prefetch.get("stock_closed")))
        if _classify_type == "skip_stock_closed":
            logger.debug(f"[scanner] {symbol} 股票代币休市期间（classify降级），跳过")
            return None
        if _is_reversal(chg24, funding, ind) and "reversal" not in _candidate_routes:
            _candidate_routes.append("reversal")
        if _is_breakout(chg24, ind) and "breakout" not in _candidate_routes:
            _candidate_routes.append("breakout")
        # pullback 作为最低优先级兜底，始终加入
        if "pullback" not in _candidate_routes:
            _candidate_routes.append("pullback")

        # ── C.5 + D. 全路径 judge + 质量择优（v3.81）────────────────────────────
        # 4H K 线：主进程已全部预取；与原先懒加载保持同样的可见性（第一次需要 4H 的路线才生效）
        _NEED_4H = {"reversal", "breakout", "range_bounce", "stop_hunt", "oi_divergence", "ema_trend"}
        _k4h = []
        _4h_loaded = False

        _passed_signals: list[dict] = []  # 收集所有通过 judge 的路径

        for _route in _candidate_routes:
            if _route in _NEED_4H and not _4h_loaded:
                _k4h = k4h
                _4h_loaded = True
                if _k4h:
                    ind_4h = calc_indicators(k15, k1h, _k4h)
                    for _k4h_key in ("ema9_4h", "ema20_4h", "ema50_4h", "rsi_4h", "ema_bull_4h"):
                        ind[_k4h_key] = ind_4h.get(_k4h_key, ind.get(_k4h_key))

            # 数据质量门禁：pullback/reversal 依赖 chg24 做方向判断，chg24 缺失（None/NaN/0）时跳过这两条路径
            if _route in _CHG24_DEPENDENT_ROUTES and _chg24_missing(chg24):
                logger.debug(f"[scanner] {symbol} chg24数据缺失，跳过pullback/reversal（当前路径={_route}）")
                continue

            # 执行对应 judge
            if _route == "reversal":
                _ok, _reason = judge_reversal(k1h, _k4h, k15, ind, chg24, funding)
                _dir = _reason.get("direction", "long").upper()
            elif _route == "breakout":
                _ok, _reason = judge_breakout(k1h, _k4h, k15, ind, chg24, funding)
                _dir = _reason.get("breakout_direction", "long").upper()
            elif _route == "range_bounce":
                _ok, _reason = judge_range_bounce(k1h, _k4h, k15, ind, chg24, funding)
                _dir = "LONG"
            elif _route == "stop_hunt":
                _ok, _reason = judge_stop_hunt(k1h, _k4h, k15, ind, chg24, funding)
                _dir = _reason.get("direction", "LONG").upper()
            elif _route == "oi_divergence":
                # [v3.99] oi_divergence 开关（OI_DIVERGENCE_ENABLED，默认True=开启观察）
                _oi_div_on = getattr(__import__('config'), 'OI_DIVERGENCE_ENABLED', True)
                if not _oi_div_on:
                    _ok, _reason = False, {"reject_reason": "oi_divergence disabled"}
                else:
                    _ok, _reason = judge_oi_divergence(k1h, _k4h, k15, ind, chg24, funding)
                _dir = _reason.get("direction", "long").upper()
            elif _route == "ema_trend":
                _ok, _reason = judge_ema_trend(k1h, _k4h, k15, ind, chg24, funding)
                _dir = (_reason.get("direction") or "long").upper() if isinstance(_reason, dict) else "LONG"
            else:  # pullback
                _ok, _reason = judge_pullback(k1h, _k4h, k15, ind, chg24, funding)
                _dir = "LONG" if ind["ema_bull_1h"] else "SHORT"

            # v3.83：熊市禁止 reversal LONG（由原预筛下沉到此，仅影响该路径）
            if _ok and _route == "reversal" and _dir == "LONG" and _bear_block_rev_long:
                _ok = False
                _reason = dict(_reason) if isinstance(_reason, dict) else {}
                _reason["reject_reason"] = "熊市禁止 reversal LONG（BTC 下跌且 24H 跌幅>=8%）"

            if not _ok:
                logger.info(
                    f"[JUDGE_REJECT] {symbol} {_route} {_dir}: "
                    f"{_reason.get('reject_reason', 'unknown') if isinstance(_reason, dict) else _reason}"
                )
                # 拒绝记录 → 事件（主进程落盘）；ind 取当时快照，等价于原来的同步写入
                events.append({"kind": "analytics", "kwargs": dict(
                    symbol=symbol, mode=_route, direction=_dir,
                    judge_passed=False,
                    reject_reason=(_reason.get('reject_reason') if isinstance(_reason, dict) else str(_reason)),
                    ind=dict(ind), reason=_reason if isinstance(_reason, dict) else {},
                    btc_trend=btc_trend, market_mode=market_mode,
                )})
                _rd = _reason if isinstance(_reason, dict) else {}
                if _rd.get("set_cooldown"):   # [A-2] judge 请求冷却 → 交由主进程 _apply_post_events 写入
                    events.append({"kind": "cooldown", "symbol": symbol,
                                   "hours": _rd.get("cooldown_hours", 4),
                                   "reason": _rd.get("cooldown_reason", _route)})
                events.append({"kind": "shadow_reject", "kwargs": dict(
                    symbol=symbol, route=_route, direction=_dir,
                    reason=str(_rd.get("reject_reason", _reason)), ind=dict(ind),
                )})
                continue

            # judge 通过：计算质量分并加入候选列表
            _q = _calc_route_quality(
                _route, _reason if isinstance(_reason, dict) else {}, ind, _dir
            )
            # [fix #49] route_quality 仅保留用于日志 schema，不再参与决策；
            #   择优改用 final_score 的同源基础分（signal_strength → SCORE_BASE_*），
            #   完整 final_score 在后续评分层统一计算。
            _rd49 = _reason if isinstance(_reason, dict) else {}
            from config import SCORE_BASE_STRONG, SCORE_BASE_MEDIUM, SCORE_BASE_WEAK
            _fs_base = {"strong": SCORE_BASE_STRONG, "medium": SCORE_BASE_MEDIUM,
                        "weak": SCORE_BASE_WEAK}.get(_rd49.get("signal_strength", "medium"), SCORE_BASE_MEDIUM)
            _passed_signals.append({"route": _route, "reason": _reason, "dir": _dir, "quality": _q,
                                    "score": _fs_base})

            # 提前终止优化：已有 2 条 strong，或高质量特殊场景（>=75）直接定案
            _strong_count = sum(
                1 for s in _passed_signals
                if (s["reason"] or {}).get("signal_strength") == "strong"
            )
            if _strong_count >= 2:
                break
            # [fix #49] 原 route_quality>=75 提前定案已删除（不再以质量分做决策）

        # 所有路径均失败
        if not _passed_signals:
            return None

        # 择出质量最高的路径
        _best = max(_passed_signals, key=lambda x: x["score"])   # [fix #49] 统一按 final_score 口径择优（同分保持路径顺序）
        ok, reason, sig_type, direction = True, _best["reason"], _best["route"], _best["dir"]

        # 多路通过时记录择优决策日志
        if len(_passed_signals) > 1:
            _competing = [(s["route"], s["score"]) for s in _passed_signals if s is not _best]
            logger.info(
                f"[scanner] {symbol} 多路候选 {len(_passed_signals)} 条，"
                f"择优选择 {sig_type}(分={_best['score']}) "
                f"vs {_competing}"
            )

        # ── 一致性检查（仅记录，不拒绝）────────────────────────────────
        try:
            from shared.judges import check_signal_consistency
            check_signal_consistency(symbol, ind, chg24, funding, sig_type)
        except Exception:
            pass

        # ── 方向约束检查（对应方向槽位已满时跳过）──────────────────────
        if allowed_dirs is not None and direction not in allowed_dirs:
            return None

        # ── 方向冲突检查（同向排除，反向允许进入 judge）──────────────────────
        if (symbol, direction) in active_syms:
            logger.debug(f"[scanner] {symbol} {direction} 方向冲突: 已有同向持仓")
            return None

        # ── v3.44 同标的同方向开仓冷却检查（主进程预取，两个方向各一个布尔）──────────
        if prefetch.get("dir_cooldown_long" if direction == "LONG" else "dir_cooldown_short"):
            logger.info(f"[scanner] {symbol} {direction} 同向开仓冷却中，跳过")
            return None
        _oi_24h = prefetch.get("oi_24h_ago")
        if direction == "LONG":
            oi_div, oi_reason = _oi_divergence(symbol, price, chg24, ind["high_24h"], oi_now, oi_24h=_oi_24h)
            if oi_div:
                logger.info(f"[scanner] {symbol} LONG {oi_reason}")
                return None
        elif direction == "SHORT":
            low_24h = ind.get("low_24h", 0)
            if low_24h > 0:
                oi_div_s, oi_reason_s = _oi_divergence_short(
                    symbol, price, chg24, low_24h, oi_now, oi_24h=_oi_24h
                )
                if oi_div_s:
                    logger.info(f"[scanner] {symbol} SHORT {oi_reason_s}")
                    return None

        # ── 资金费率过高检查 ────────────────────────────────────────────────────
        if abs(funding) > FILTER_MAX_FUNDING_RATE:
            logger.debug(f"[scanner] {symbol} 资金费率过高: {funding*100:.4f}%")
            return None

        # ── v3.43 妖币识别：纯规则部分在这里，LLM 调用后置到主进程（llm 事件）────────
        if sig_type != "range_bounce":
            try:
                from config import LLM_JUDGE_ENABLED, LLM_JUDGE_EXPLOSIVE_COIN
                if LLM_JUDGE_ENABLED and LLM_JUDGE_EXPLOSIVE_COIN:
                    from shared.llm_judge import is_explosive_coin
                    _is_exp, _g3d, _g24h = is_explosive_coin(k1h)
                    if _is_exp:
                        events.append({"kind": "llm", "symbol": symbol, "g3d": _g3d, "g24h": _g24h,
                                       "rsi_1h": ind.get("rsi_1h", 50), "rsi_4h": ind.get("rsi_4h", 50),
                                       "atr_ratio": ind.get("atr_ratio", 0), "funding": funding,
                                       "chg24": chg24, "ind": ind, "oi_now": oi_now, "k1h": k1h,
                                       "now_ms": int(prefetch.get("now_ms") or 0)})
            except Exception as _exp_e:
                logger.debug(f"[scanner] {symbol} 妖币识别跳过: {_exp_e}")

        _sig = _build_signal(symbol, direction, sig_type, reason, chg24, funding, ind, oi_now, k1h,
                             int(prefetch.get("now_ms") or 0))

        # v3.28: 通过记录（含竞争路径信息）→ 事件；LLM 否决时主进程不写
        _competing_routes = [
            {"route": s["route"], "quality": s["quality"], "passed": True}
            for s in _passed_signals if s is not _best
        ] if len(_passed_signals) > 1 else []
        events.append({"kind": "analytics_pass", "kwargs": dict(
            symbol=symbol, mode=sig_type, direction=direction,
            judge_passed=True, reject_reason=None,
            ind=ind, reason=reason if isinstance(reason, dict) else {},
            btc_trend=btc_trend, market_mode=market_mode,
            route_quality=_best["quality"] if _passed_signals else None,
            competing_routes=_competing_routes,
            final_score=_best["score"],     # 基础分（SCORE_BASE_*），避免通过记录 final_score 为 null
        )})
        return _sig

    except Exception as e:
        logger.debug(f"[scanner] {symbol} analyze 失败: {e}")
        return None


_ROUTE_QUALITY: dict = {}  # 阶段3：symbol -> 本轮 route_quality（仅供 scan_signals.jsonl）


def _apply_post_events(sig: dict | None, events: list) -> dict | None:
    """主进程后置处理：按顺序落盘 analytics / shadow 记录，再处理妖币 LLM，最后写通过记录。
    返回最终信号（LLM 可能改写为 range_bounce LONG，或否决返回 None）。"""
    llm_ev = None
    pass_ev = None
    for ev in events:
        k = ev.get("kind")
        if k == "analytics":
            try:
                _write_signal_analytics(**ev["kwargs"])
            except Exception as _e:
                logger.debug(f"[scanner] analytics 写入失败（忽略）: {_e}")
        elif k == "shadow_reject":
            try:
                from shared import shadow_trace as _st1
                _st1.record_judge_rejection(**ev["kwargs"])
            except Exception:
                pass
        elif k == "cooldown":      # 进程池 worker 不写 state，冷却写入由主进程统一做
            try:
                from shared.state import set_cooldown as _sc
                _sc(ev["symbol"], ev["hours"], ev["reason"])
            except Exception as _e:
                logger.warning(f"[scanner] 冷却写入失败，冷却可能未生效 {ev.get('symbol')}: {_e}")
        elif k == "llm":
            llm_ev = ev
        elif k == "analytics_pass":
            pass_ev = ev
    if sig is None:
        return None

    pass_kwargs = dict(pass_ev["kwargs"]) if pass_ev else None
    if llm_ev is not None:
        symbol = llm_ev["symbol"]
        try:
            from config import LLM_JUDGE_MIN_CONFIDENCE
            from shared.llm_judge import judge_explosive_coin_llm
            logger.info(
                f"[scanner] {symbol} 检测到妖币：3日+{llm_ev['g3d']:.0f}% "
                f"24H{llm_ev['g24h']:+.1f}%，交给LLM判断"
            )
            _llm_v = judge_explosive_coin_llm(
                symbol, llm_ev["g3d"], llm_ev["g24h"],
                llm_ev["rsi_1h"], llm_ev["rsi_4h"], llm_ev["atr_ratio"], llm_ev["funding"]
            )
            if (_llm_v.get("decision") == "open"
                    and _llm_v.get("confidence", 0) >= LLM_JUDGE_MIN_CONFIDENCE):
                reason = {
                    "signal_strength":  "medium",
                    "position_size_cap": 0.3,
                    "llm_explosive":    True,
                    "llm_reason":       _llm_v.get("reason", ""),
                }
                sig = _build_signal(symbol, "LONG", "range_bounce", reason, llm_ev["chg24"], llm_ev["funding"],
                                    llm_ev["ind"], llm_ev["oi_now"], llm_ev["k1h"], llm_ev["now_ms"])
                if pass_kwargs is not None:
                    pass_kwargs.update(mode="range_bounce", direction="LONG", reason=reason)
                logger.info(
                    f"[scanner] {symbol} 妖币LLM判定蓄势，"
                    f"confidence={_llm_v['confidence']:.2f} "
                    f"reason={_llm_v.get('reason', '')}"
                )
            else:
                logger.info(f"[scanner] {symbol} 妖币LLM判定出货或不确定，跳过")
                return None
        except Exception as _exp_e:
            logger.debug(f"[scanner] {symbol} 妖币识别跳过: {_exp_e}")

    if pass_kwargs is not None:
        _ROUTE_QUALITY[pass_kwargs.get("symbol")] = pass_kwargs.get("route_quality")  # 阶段3 对账用，不进信号 dict
        try:
            _write_signal_analytics(**pass_kwargs)
        except Exception as _e:
            logger.debug(f"[scanner] analytics 写入失败（忽略）: {_e}")
    return sig


def _analyze_symbol_impl(symbol: str, ticker: dict,
                   active_syms: set, pending_syms: set,
                   allowed_directions: set | None = None,
                   loss_blacklist: set | None = None,
                   btc_trend: str = "",
                   market_mode: str = "",
                   deadline: float | None = None,
                   mark_price_map: dict | None = None) -> dict | None:
    """完整分析一个标的（线程池路径）：_prefetch_symbol 做全部 IO，analyze_one 纯计算，再处理落盘/LLM 事件。"""
    b = _prefetch_symbol(symbol, ticker, loss_blacklist, deadline, mark_price_map)
    if b is None:
        return None
    try:
        events: list = []
        sig = analyze_one(symbol, ticker, b["k15"], b["k1h"], b["k4h"], active_syms, pending_syms,
                          allowed_directions, loss_blacklist, btc_trend, market_mode,
                          mark_price_map or {}, b["prefetch"], events)
        return _apply_post_events(sig, events)
    except Exception as e:
        logger.debug(f"[scanner] {symbol} analyze 失败: {e}")
        return None


def _prefetch_liq(symbol: str, price: float) -> dict:
    """进程池路径：主进程读一次清算区，随 prefetch 序列化给 worker。LIQ_SCAN_CACHE_ONLY=True 时只读缓存不发网络。"""
    out: dict = {}
    try:
        from shared.liq_cache import get_liq_zones_cached as _g
        # 扫描只读缓存：Clawby 响应差时不在分析线程里等网络（缺失按无清算区降级，后台线程优先补）
        if getattr(__import__("config"), "LIQ_SCAN_CACHE_ONLY", True):
            from shared.liq_cache import get_liq_zones_nowait as _g
    except Exception:
        return out
    # [v3.95] 统一只读 7d（覆盖最宽），节省 CG 配额；3d/24h 字段复用 7d 数据
    for key, (d, r) in (("long", ("LONG", "7d")), ("short", ("SHORT", "7d")),
                        ("long_24h", ("LONG", "7d")), ("short_24h", ("SHORT", "7d"))):
        try:
            out[key] = list(_g(symbol, d, price, range_=r) or [])
        except Exception:
            out[key] = []
    return out


def _prefetch_symbol(symbol: str, ticker: dict,
                     loss_blacklist: set | None = None,
                     deadline: float | None = None,
                     mark_price_map: dict | None = None,
                     with_liq: bool = False,
                     klines: tuple | None = None) -> dict | None:
    """
    单标的全部 IO（REST、state 读取、时钟），打包成 {"k15","k1h","k4h","prefetch"}；
    预筛淘汰 / 超时 / 数据不足时返回 None。线程池路径和进程池路径共用。
    with_liq=True（进程池路径）时额外在 prefetch["liq_zones"] 放入清算区快照。
    deadline:          整轮截止时间（time.monotonic() 基准）；单标的另有 SCAN_SYMBOL_TIMEOUT_SEC 总时限，
                       两者取先到者。每次发起 HTTP 请求前检查，超时直接 return None（不再发新请求）。
    mark_price_map:    {symbol: premiumIndex 单项}（run_scan 每轮批量拉一次）；未命中降级为单标的 REST。
    active_syms:       (symbol, direction) 二元组集合，只排除同向仓位
    pending_syms:      当前有挂单的标的集合
    allowed_directions: 允许的方向集合（{"LONG"} / {"SHORT"} / None=不限制）
    loss_blacklist:    累计亏损黑名单（零成本预筛，不拉 K 线前直接跳过）
    btc_trend:         BTC 趋势（"up"/"down"/"neutral"），用于熊市粗筛
    """
    # ── 零成本预筛（不拉任何接口）────────────────────────────────────────────
    is_weekend = _is_weekend_period(datetime.now(timezone.utc))
    if not _prescreen_ticker(symbol, ticker, loss_blacklist, is_weekend):
        return None

    # [fix] 单标的总时限：以 monotonic 计时，发起每个 HTTP 请求前检查
    _t_sym0  = time.monotonic()
    _t_limit = _t_sym0 + SCAN_SYMBOL_TIMEOUT_SEC
    if deadline is not None:
        _t_limit = min(_t_limit, deadline)

    def _over_budget(stage: str) -> bool:
        if time.monotonic() > _t_limit:
            logger.info(
                f"[scanner] {symbol} 单标的超时(已用 {time.monotonic() - _t_sym0:.1f}s，"
                f"限 {SCAN_SYMBOL_TIMEOUT_SEC}s/整轮截止)，在「{stage}」前放弃"
            )
            return True
        return False

    try:
        # ── A. 拉取数据（4H 现在对所有通过预筛的标的预取）──
        if _over_budget("15m K线"):
            return None
        # 阶段5：klines=(k15, k1h, k4h) 由触发器传入 hub 快照时跳过 K 线 REST；不传时行为不变
        k15 = klines[0] if klines else _get_klines_cached(symbol, "15m", 100)
        if _over_budget("1h K线"):
            return None
        k1h = klines[1] if klines else _get_klines_cached(symbol, "1h",  100)
        if not k1h or len(k1h) < 25:
            return None

        # [fix] 优先复用 run_scan 批量拉好的标记价格/资金费率；无数据时降级为单标的 REST
        if _over_budget("标记价格"):
            return None
        mp      = (mark_price_map or {}).get(symbol) or _get_mark_price(symbol)
        funding = float(mp.get("lastFundingRate", 0))

        # [v3.85 OPT-6] 逐标的资金费结算窗口（含时钟与 REST，所以留在这里；命中则不再拉后续数据）
        try:
            from config import OPT_FUNDING_WINDOW_PER_SYMBOL as _OPT_FW
        except Exception:
            _OPT_FW = False
        if _OPT_FW:
            _fw_blk, _fw_why = _in_symbol_funding_window(symbol, mp, abs(funding))
            if _fw_blk:
                logger.debug(f"[scanner] {symbol} 资金费结算窗口内跳过: {_fw_why}")
                return None

        # OI（尽力获取，失败不阻塞）
        oi_now = 0.0
        try:
            if _over_budget("持仓量"):
                return None
            oi_data = api.get_open_interest(symbol)
            oi_now  = float(oi_data.get("openInterest", 0))
        except Exception:
            pass

        # OI 历史（过去6根1H）；oi_1h_ago 取其最后 3 根的第一条（与原先 limit=3 的请求等价）
        oi_hist_6: list = []
        try:
            if _over_budget("OI 历史(6)"):
                return None
            oi_hist_6 = _get_oi_history_cached(symbol, period="1h", limit=6) or []
        except Exception:
            pass
        oi_1h_ago = 0.0
        try:
            _oi_hist_3 = oi_hist_6[-3:]
            if _oi_hist_3 and len(_oi_hist_3) >= 2:
                oi_1h_ago = float(_oi_hist_3[0].get("sumOpenInterest", 0))
        except Exception:
            pass

        # 股票代币休市检查（时钟 + 外部模块）
        stock_closed = False
        try:
            from calc_position_size_real import _is_stock_token as _ist, _us_market_open as _umo
            stock_closed = bool(_ist(symbol) and not _umo())
        except Exception:
            pass

        # 4H K 线（预取）
        if _over_budget("4h K线"):
            return None
        k4h = klines[2] if klines else _get_klines_cached(symbol, "4h", 100)

        # judge 内部 _funding_rate_velocity 的 HTTP（60s 缓存）在主进程预取
        funding_velocity: dict = {}
        try:
            from shared.judges import _funding_rate_velocity
            funding_velocity = _funding_rate_velocity(symbol)
        except Exception:
            pass

        # state 读取：同向开仓冷却（两个方向）、24h 前 OI
        cd_long = cd_short = False
        try:
            from shared.state import is_open_dir_cooldown as _is_odc
            cd_long, cd_short = bool(_is_odc(symbol, "LONG")), bool(_is_odc(symbol, "SHORT"))
        except Exception as _odc_e:
            logger.debug(f"[scanner] open_dir_cooldown 检查异常: {_odc_e}")
        oi_24h_ago = None
        try:
            oi_24h_ago = get_oi_24h_ago(symbol)
        except Exception:
            pass

        prefetch = {
            "oi_now": oi_now, "oi_1h_ago": oi_1h_ago, "oi_24h_ago": oi_24h_ago,
            "btc_1h_drop_pct": _BTC_1H_DROP_PCT,
            "oi_hist_6": oi_hist_6, "funding_velocity": funding_velocity, "mp": mp,
            "is_weekend": is_weekend, "stock_closed": stock_closed,
            "dir_cooldown_long": cd_long, "dir_cooldown_short": cd_short,
            "now_ms": int(time.time() * 1000),
        }
        if with_liq:
            prefetch["liq_zones"] = _prefetch_liq(symbol, float(k1h[-1][4]))
        return {"k15": k15, "k1h": k1h, "k4h": k4h, "prefetch": prefetch}

    except Exception as e:
        logger.debug(f"[scanner] {symbol} prefetch 失败: {e}")
        return None


# ── 挂单维护 ──────────────────────────────────────────────────────────────────
import functools as _functools


@_functools.wraps(_analyze_symbol_impl)
def analyze_symbol(*args, **kwargs):
    """_analyze_symbol_impl 的统计外壳：累计本标的 K 线请求次数/耗时，结束时 debug 输出并并入本轮汇总。"""
    symbol = args[0] if args else kwargs.get("symbol", "")
    acc = [0, 0.0]
    _KL_TLS.acc = acc
    try:
        return _analyze_symbol_impl(*args, **kwargs)
    finally:
        _KL_TLS.acc = None
        if acc[0]:
            logger.debug(f"[scanner] {symbol} K线请求: 次数={acc[0]} 总耗时={acc[1]:.0f}ms")
            with _KL_ROUND_LOCK:
                _KL_ROUND["calls"] += acc[0]
                _KL_ROUND["ms"] += acc[1]
                _KL_ROUND["per_sym"][symbol] = (acc[0], acc[1])


def maintain_pending_order(pos: dict, ticker: dict) -> str:
    """
    对已有挂单的标的评估挂单是否仍合理。
    返回操作描述: 'keep' | 'reorder' | 'cancel'
    """
    symbol    = pos["symbol"]
    direction = pos["direction"]
    sig_type  = pos.get("sig_type", "pullback")
    # [fix #33] 原读不存在的 open_ts，elapsed 恒为 0，超时撤单是死代码。
    #   实际写入的字段是 open_time（毫秒，见 executor 写 pos_state）；兼容 create_ts / entry_ts / 旧名 open_ts。
    open_ts = 0
    for _k in ("open_time", "create_ts", "entry_ts", "open_ts"):
        try:
            _v = int(pos.get(_k) or 0)
        except (TypeError, ValueError):
            _v = 0
        if _v > 0:
            open_ts = _v if _v > 10**11 else _v * 1000   # 秒级时间戳自动换算成毫秒
            break
    elapsed_min = (time.time() * 1000 - open_ts) / 60_000 if open_ts else 0

    # 超时撤单
    if sig_type == "breakout":
        expire = ORDER_EXPIRE_BREAKOUT_MIN
    elif sig_type == "stop_hunt":
        from config import (
            STOP_HUNT_EXPIRE_MIN_WKND,
            STOP_HUNT_RETEST_EXPIRE_MIN,
            STOP_HUNT_RETEST_EXPIRE_HIGH,
            STOP_HUNT_RETEST_EXPIRE_MID,
        )
        _now_wknd = _is_weekend_period(time.time() * 1000)
        if _now_wknd:
            expire = STOP_HUNT_EXPIRE_MIN_WKND
        else:
            # v3.52: 有效期按 tier 动态设置
            # high tier（高波动，二探快）= 30min； mid = 60min； low/默认 = 90min
            _sig_tier = pos.get("tier", "mid")
            if _sig_tier == "high":
                expire = STOP_HUNT_RETEST_EXPIRE_HIGH
            elif _sig_tier == "mid":
                expire = STOP_HUNT_RETEST_EXPIRE_MID
            else:
                expire = STOP_HUNT_RETEST_EXPIRE_MIN
    else:
        expire = ORDER_EXPIRE_PULLBACK_MIN
    if elapsed_min >= expire:
        logger.info(f"[scanner] {symbol} 挂单超时 {elapsed_min:.0f}min >= {expire}min，撤销")
        return "cancel"

    try:
        k1h = _get_klines_hub_or_rest(symbol, "1h", 30)
        k15 = _get_klines_hub_or_rest(symbol, "15m", 20)
        if not k1h:
            return "keep"

        mp      = _get_mark_price(symbol)
        funding = float(mp.get("lastFundingRate", 0))

        # 情况三：异常触发条件
        # 1. BTC 15分钟涨跌超过3%
        btc_15 = _get_klines_hub_or_rest("BTCUSDT", "15m", 3)
        if btc_15 and len(btc_15) >= 2:
            btc_move = abs(float(btc_15[-1][4]) - float(btc_15[-2][4])) / float(btc_15[-2][4]) * 100
            if btc_move > ORDER_CANCEL_BTC_MOVE_PCT:
                logger.info(f"[scanner] {symbol} BTC 15min剧烈波动 {btc_move:.1f}%，撤单进入冷却")
                return "cancel"

        # 2. 资金费率变化超过0.2%（与上轮对比，从 pos 记录的上轮费率）
        last_funding = pos.get("last_funding", funding)
        if abs(funding - last_funding) > ORDER_CANCEL_FUNDING_DELTA:
            logger.info(f"[scanner] {symbol} 资金费率剧变 {(funding-last_funding)*100:.3f}%，撤单")
            return "cancel"

        # 3. OI 15分钟内下降超过15%（简化：对比当前 OI 和15分钟前）
        oi_data = api.get_open_interest(symbol)
        oi_now  = float(oi_data.get("openInterest", 0))
        oi_prev = pos.get("oi_snapshot", oi_now)
        if oi_prev > 0 and (oi_prev - oi_now) / oi_prev * 100 > ORDER_CANCEL_OI_DROP_PCT:
            logger.info(f"[scanner] {symbol} OI 骤降，撤单")
            return "cancel"

        # ── v3.69 使用 _survival_check 统一存活判断（与 signal_watcher 保持一致）──
        # 旧逻辑（EMA趋势翻转、距离超过1ATR）已被 _survival_check 的"价格破位"逻辑替代
        ind   = calc_indicators(k15, k1h, [])  # 4H 不拉节省请求
        atr   = ind["atr_1h"]
        price = ind["price"]
        rsi   = ind.get("rsi_1h", 50.0)

        try:
            from shared.signal_watcher import SignalWatcher as _SW, WatchEntry as _WE
            import dataclasses as _dc
            # 构造临时 WatchEntry，复用 _survival_check 的判断逻辑
            _tmp_entry = _WE(
                signal={"symbol": symbol, "direction": direction, "mode": sig_type},
                entry_low=0, entry_high=0,
                entry_price=float(pos.get("entry_price", price)),
                expire_ts=9e18,
                on_trigger=lambda: None,
                trigger_price=float(pos.get("entry_price", price)),
                trigger_rsi_1h=float(pos.get("rsi_at_entry", rsi)),
                breakout_ref=float((pos.get("reason") or {}).get(
                    "breakout_ref_high" if direction == "LONG" else "breakout_ref_low", 0)),
                ema20_at_trigger=float(pos.get("ema20_at_open", ind["ema20_1h"])),
                atr_at_trigger=float(pos.get("atr_at_entry", atr)),
            )
            should_cancel, cancel_reason = _SW._survival_check(_tmp_entry, price, rsi, atr)
            if should_cancel:
                logger.info(f"[scanner] {symbol} 存活检查不通过: {cancel_reason}")
                return "cancel"
        except Exception as _se:
            logger.debug(f"[scanner] {symbol} _survival_check 调用失败，降级旧逻辑: {_se}")
            # 降级：仍用旧的 EMA 趋势翻转检查
            ema_bull_now  = ind["ema_bull_1h"]
            ema_bull_orig = pos.get("ema_bull_at_open")
            if ORDER_MAINTAIN_EMA_CHANGE and ema_bull_orig is not None and ema_bull_now != ema_bull_orig:
                logger.info(f"[scanner] {symbol} EMA 方向改变（降级逻辑），撤单重挂")
                return "reorder"

        # 情况一：核心理由未被证伪，继续等
        return "keep"

    except Exception as e:
        logger.debug(f"[scanner] {symbol} maintain_pending 失败: {e}")
        return "keep"

# ── 候选池刷新（每30分钟） ────────────────────────────────────────────────────
_POOL_SCORE_VOL_TOP = 80          # 成交额排名前 N 加分
_POOL_SCORE_VOL = 30              # 40→30：避免候选池退化成成交额排名
_POOL_SCORE_MOVER = 25            # 涨幅 Top / 跌幅 Top（20→25）
_POOL_SCORE_FUNDING = 25          # 资金费率绝对值 > POOL_FUNDING_EXTREME_MIN（20→25）
_POOL_SCORE_LIQ = 20              # CG 清算量 Top
# v3.xx: 候选池 OI 过滤已停用，以下两个常量仅供 _pool_filter_by_oi 恢复时使用
try:   # [refactor-B2] OI 拉取线程数可配置（原 6），2 核机器减少线程争抢
    import config as _cfg_oi
    _POOL_OI_WORKERS = max(1, int(getattr(_cfg_oi, "POOL_OI_WORKERS", 2)))
except Exception:
    _POOL_OI_WORKERS = 2
_POOL_OI_TOTAL_TIMEOUT_SEC = 90.0


# ── 大户持仓多空比影子记录（只写日志，不影响评分、不影响信号是否发出）─────────────
def _log_top_trader_shadow(signals: list) -> int:
    """对最终通过的信号逐个拉大户持仓比并写一行 info 日志；任何异常都吞掉。返回成功写日志的条数。"""
    n = 0
    for sig in signals or []:
        try:
            from shared.clawby_adapter import get_top_trader_ratio
            sym = sig.get("symbol", "")
            r = get_top_trader_ratio(sym)
            if not r:
                continue
            logger.info(f"[top_trader] {sym} {sig.get('direction', '')} "
                        f"大户多头={r['top_position_long_percent']:.1f}% "
                        f"空头={r['top_position_short_percent']:.1f}% "
                        f"比值={r['top_position_long_short_ratio']:.2f}")
            n += 1
        except Exception as e:
            logger.debug(f"[top_trader] 影子记录异常（忽略）: {e}")
    return n


# ── 代币解锁风险过滤（只影响 LONG）─────────────────────────────────────────────
def _check_unlock_risk(symbol: str, direction: str, now_ms: float = None):
    """返回 (命中?, 说明)。窗口按 UTC 日历天（今天起 UNLOCK_FILTER_DAYS 天内，含今天）；
    占流通量百分比 >= UNLOCK_FILTER_PCT。SHORT / 没有解锁数据 / 取数失败 / 解锁日已过 → 一律 (False, "")。"""
    if (direction or "").upper() != "LONG":
        return False, ""
    try:
        from shared.clawby_adapter import get_upcoming_unlocks
        d = (get_upcoming_unlocks() or {}).get(str(symbol).upper().removesuffix("USDT"))
        if not d:
            return False, ""
        ts = float(d.get("next_unlock_date") or 0)
        if ts <= 0:
            return False, ""
        import datetime as _dt
        now_ms = time.time() * 1000 if now_ms is None else now_ms
        days = (_dt.datetime.fromtimestamp(ts / 1000, _dt.timezone.utc).date()
                - _dt.datetime.fromtimestamp(now_ms / 1000, _dt.timezone.utc).date()).days
        pct = float(d.get("next_unlock_of_circulating") or 0)
        if 0 <= days <= UNLOCK_FILTER_DAYS and pct >= UNLOCK_FILTER_PCT:
            usd = float(d.get("next_unlock_usd") or 0)
            return True, f"解锁预警: {days}天后解锁占流通量{pct:.2f}%（约{usd / 1e6:.1f}M USD）"
    except Exception as e:
        logger.debug(f"[unlock] {symbol} 解锁风险检查异常，放行: {e}")
    return False, ""


def _apply_unlock_filter(signals: list, result: dict, now_ms: float = None) -> list:
    """过滤链一环：开关关闭 → 只记录日志、信号原样放行；开启 → LONG 命中则写入 result["rejected"] 并剔除。"""
    kept = []
    for sig in signals:
        hit, msg = _check_unlock_risk(sig.get("symbol", ""), sig.get("direction", ""), now_ms)
        if hit:
            logger.info(f"[unlock] {sig.get('symbol')} LONG {msg}（{'已拒绝' if UNLOCK_FILTER_ENABLED else '仅记录，开关关闭未拒绝'}）")
            if UNLOCK_FILTER_ENABLED:
                result["rejected"].append({"symbol": sig.get("symbol"), "reason": msg})
                continue
        kept.append(sig)
    return kept


# ── coin-list 1h 多空失衡标记 ─────────────────────────────────────────────────
# 候选池阶段还不知道信号方向，所以这里只“标记”：{BTCUSDT: "LONG"/"SHORT"}，不加分。
# 后续分析单个标的时用 get_liq_bias(symbol) 取方向，与信号方向比对（加分逻辑尚未接入，先上线观察标记数量和方向分布）。
_LIQ_BIAS: dict = {}
_LIQ_BIAS_LOCK = _threading.Lock()


def _liq_bias_direction(long_1h: float, short_1h: float) -> str:
    """失衡方向，与 coinglass_api 现有 liq_signal 约定一致：
    多头被清算更多（long_1h > short_1h，heavy_long_liq，超卖反弹）→ "LONG"；
    空头被清算更多（short_1h > long_1h，heavy_short_liq，超涨回落）→ "SHORT"。
    要改方向约定只改这个函数。"""
    return "LONG" if long_1h > short_1h else "SHORT"


def _calc_liq_bias(items: list, min_total: float = None, min_ratio: float = None) -> dict:
    """遍历 coin-list 全部条目，返回 {symbol+USDT: "LONG"/"SHORT"}。
    条件（同时满足）：1h 多空清算合计 ≥ min_total；max/(min+1) ≥ min_ratio。数值可能是 int/float/None/字符串。"""
    min_total = LIQ_BIAS_MIN_TOTAL_1H if min_total is None else min_total
    min_ratio = LIQ_BIAS_MIN_RATIO if min_ratio is None else min_ratio
    out: dict = {}
    for it in items or []:
        if not isinstance(it, dict):
            continue
        coin = (it.get("symbol") or it.get("coin") or "").upper().strip()
        if not coin:
            continue
        try:
            long_1h = float(it.get("long_liquidation_usd_1h", 0) or 0)
            short_1h = float(it.get("short_liquidation_usd_1h", 0) or 0)
        except (TypeError, ValueError):
            continue
        if long_1h < 0 or short_1h < 0:
            continue
        if long_1h + short_1h < min_total:
            continue
        if max(long_1h, short_1h) / (min(long_1h, short_1h) + 1) < min_ratio:
            continue
        out[coin if coin.endswith("USDT") else f"{coin}USDT"] = _liq_bias_direction(long_1h, short_1h)
    return out


def get_liq_bias(symbol: str):
    """返回最近一次候选池刷新标记的 1h 清算失衡方向 "LONG"/"SHORT"；无标记返回 None。"""
    with _LIQ_BIAS_LOCK:
        return _LIQ_BIAS.get((symbol or "").upper())


_TRADIFI_CACHE = {"ts": 0.0, "syms": frozenset()}
_TRADIFI_TTL = 6 * 3600


def _tradifi_symbols() -> frozenset:
    """exchangeInfo 中 contractType == TRADIFI_PERPETUAL 的合约集合（6h 缓存）；失败返回上次结果/空集。"""
    now = time.time()
    if _TRADIFI_CACHE["syms"] and now - _TRADIFI_CACHE["ts"] < _TRADIFI_TTL:
        return _TRADIFI_CACHE["syms"]
    try:
        syms = (api.get_exchange_info() or {}).get("symbols") or []
        found = frozenset(x["symbol"] for x in syms if x.get("contractType") == "TRADIFI_PERPETUAL")
        if found:
            _TRADIFI_CACHE.update(ts=now, syms=found)
            return found
    except Exception as e:
        logger.debug(f"[scanner] 取 TRADIFI 合约列表失败，清算榜不过滤: {e}")
    return _TRADIFI_CACHE["syms"]


def _pool_scores(qualified: list, vol_top: set, mover_syms: set, liq_syms: set) -> dict:
    """候选池评分（0~100，各项独立加分可叠加）。qualified 为通过粗筛的 ticker 列表（带 _fr_abs）。"""
    out = {}
    for t in qualified:
        sym = t["symbol"]
        sc_ = 0
        if sym in vol_top:
            sc_ += _POOL_SCORE_VOL
        if sym in mover_syms:
            sc_ += _POOL_SCORE_MOVER
        if t.get("_fr_abs", 0) > POOL_FUNDING_EXTREME_MIN:
            sc_ += _POOL_SCORE_FUNDING
        if sym in liq_syms:
            sc_ += _POOL_SCORE_LIQ
        out[sym] = sc_
    return out


# v3.xx: 候选池 OI 过滤已停用，成交量双重过滤足够兜底，保留函数供需要时恢复
def _pool_filter_by_oi(candidates: list, min_oi_u: float):
    """对候选逐一查 OI（张数×价格≈U），OI < min_oi_u 的剔除；查询失败/超时视为通过。
    并发 _POOL_OI_WORKERS，同一次调用内每个标的只查一次。返回 (通过列表, {'excluded':n,'failed':n})。"""
    oi_usd: dict = {}

    def _one(t):
        sym = t["symbol"]
        try:
            d = api.get_open_interest(sym)
            return sym, float(d["openInterest"]) * float(t.get("_px") or 0)
        except Exception:
            return sym, None

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=_POOL_OI_WORKERS, thread_name_prefix="pool-oi")
    try:
        futs = [ex.submit(_one, t) for t in candidates]
        try:
            for f in concurrent.futures.as_completed(futs, timeout=_POOL_OI_TOTAL_TIMEOUT_SEC):
                sym, val = f.result()
                oi_usd[sym] = val
        except concurrent.futures.TimeoutError:
            logger.warning("[scanner] 候选池 OI 查询整体超时，未完成的标的视为通过")
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    passed, excluded, failed = [], 0, 0
    for t in candidates:
        v = oi_usd.get(t["symbol"])
        if v is None:
            failed += 1
            passed.append(t)
        elif v >= min_oi_u:
            passed.append(t)
        else:
            excluded += 1
    return passed, {"excluded": excluded, "failed": failed}


def refresh_pool() -> list:
    """
    刷新候选池并写入缓存文件。
    粗筛（USDT、非稳定币、成交额/涨跌幅/振幅/OI 门槛）后按评分排序取前 SCAN_POOL_MAX：
    成交额前80 +40，涨幅/跌幅 Top +20，资金费率极端 +20，CG 清算量 Top +20；同分按成交额降序。
    [v3.87 DOC] 原文档写「Top30 / 目标 70~80」与实际配置不符，已更正。
    """
    try:
        tickers  = api.get_ticker_24h()
        # hub优先，未就绪时用REST兜底（refresh_pool非高频，偶尔REST可接受）
        try:
            from shared import market_data_hub as _hub_rp
            if _hub_rp.is_ready():
                _snap = _hub_rp.get_all_mark_prices()
                mp_list = list(_snap.values()) if _snap else None
            else:
                mp_list = None
        except Exception:
            mp_list = None
        if not mp_list:
            mp_list = api.get_all_mark_prices()   # 含资金费率

        # [v3.100] 拉取失败（API 静默返回空列表）时直接退出，不覆盖缓存
        if not tickers:
            logger.warning("[scanner] refresh_pool: get_ticker_24h 返回空，跳过本次刷新（保留旧候选池）")
            return []
        if not mp_list:
            logger.warning("[scanner] refresh_pool: get_all_mark_prices 返回空，跳过本次刷新（保留旧候选池）")
            return []

        # 资金费率 map
        fr_map = {}
        for mp in (mp_list or []):
            sym = mp.get("symbol", "")
            if sym:
                fr_map[sym] = abs(float(mp.get("lastFundingRate", 0)))

        from config import ENABLE_USDC_PAIRS
        try:
            from config import SCAN_POOL_MAX, SCAN_POOL_MIN_OI_U
        except ImportError:
            SCAN_POOL_MAX, SCAN_POOL_MIN_OI_U = 200, 1_000_000
        _max_chg = POOL_MAX_CHG24_PCT   # 24h 涨跌幅上限（50%）
        _mark_px = {}
        for mp in (mp_list or []):
            try:
                _mark_px[mp.get("symbol", "")] = float(mp.get("markPrice", 0))
            except (TypeError, ValueError):
                pass

        # ── 第一步：粗筛 ──────────────────────────────────────────────────────
        _all_vol = []     # 全市场（USDT 后缀、非稳定币对）的 (symbol, 成交额)，用于成交额排名
        coarse = []
        for t in tickers:
            sym = t.get("symbol", "")
            _allow_usdc = ENABLE_USDC_PAIRS
            if not (sym.endswith("USDT") or (_allow_usdc and sym.endswith("USDC"))):
                continue
            if sym in POOL_STABLE_COINS:
                continue
            vol  = float(t.get("quoteVolume", 0))
            chg  = abs(float(t.get("priceChangePercent", 0)))
            _all_vol.append((sym, vol))
            if vol < POOL_MIN_VOLUME_USD:
                continue
            if chg > _max_chg:
                continue
            # ── v3.3.6: 波动率预筛：用日内振幅（highPrice-lowPrice）代替涨跌幅，避免误杀「大涨后回踩」的 pullback 场景 ──
            # chg24 在回踩日可能只有 0.5%，但 ATR 仍然很高；highPrice/lowPrice 更准确反映真实波动
            _price_now = float(t.get("lastPrice", 0))
            _high_24h  = float(t.get("highPrice", _price_now))
            _low_24h   = float(t.get("lowPrice",  _price_now))
            _intraday_range = (_high_24h - _low_24h) / (_low_24h + 1e-9) * 100
            _pool_amp_thresh = 0.8 if _is_weekend_period(datetime.now(timezone.utc)) else 1.0  # v3.46: 降低到中波动档下限
            if _intraday_range < _pool_amp_thresh:
                continue  # v3.46: 日内振幅低于门槛（工作日1.0%/周末0.8%），不进候选池
            t["_fr_abs"] = fr_map.get(sym, 0)
            t["_px"] = _mark_px.get(sym) or _price_now
            coarse.append(t)

        # v3.xx: 候选池 OI 过滤已停用（成交量双重过滤足够兜底），粗筛结果直接进入评分
        # usdt, _oi_stat = _pool_filter_by_oi(coarse, SCAN_POOL_MIN_OI_U)
        usdt = coarse

        # ── 第二步：评分（各项独立加分，可叠加，满分 100）────────────────────
        _vol_top = {s_ for s_, _ in sorted(_all_vol, key=lambda x: x[1], reverse=True)[:_POOL_SCORE_VOL_TOP]}
        _gain_syms = {t["symbol"] for t in sorted(usdt, key=lambda x: float(x.get("priceChangePercent", 0)), reverse=True)[:POOL_GAINERS_N]}
        _loss_syms = {t["symbol"] for t in sorted(usdt, key=lambda x: float(x.get("priceChangePercent", 0)))[:POOL_LOSERS_N]}
        _liq_syms = set()

        # ── CG 清算量 Top N（15min 缓存，失败静默跳过）────────────────────────
        try:
            from shared.clawby_adapter import get_liq_coin_list as _get_liq_list
            _liq_items = _get_liq_list()
            if _liq_items:
                # coin-list 真实字段：liquidation_usd_24h / _12h / _4h / _1h（旧名 liq_usd 等从未出现过）
                _LIQ_FIELD = ("liquidation_usd_24h", "liquidation_usd_1h")
                def _liq_val(item):
                    for f in _LIQ_FIELD:
                        v = item.get(f)
                        if v is not None:
                            try: return float(v)
                            except Exception: pass
                    return 0.0
                # 清算榜只保留加密货币合约：剔除 TRADIFI_PERPETUAL（股票ETF/外汇/大宗商品）。
                # coin-list 条目无 contractType，改查 exchangeInfo；查询失败 → 不过滤（保持旧行为）
                _tradifi = _tradifi_symbols()
                if _tradifi:
                    _liq_items = [it for it in _liq_items
                                  if f"{(it.get('symbol') or it.get('coin') or '').upper().strip().removesuffix('USDT')}USDT"
                                  not in _tradifi]
                _liq_sorted = sorted(_liq_items, key=_liq_val, reverse=True)[:POOL_LIQ_TOP_N]
                _usdt_set = {t["symbol"] for t in usdt}  # 已过粗筛（含 OI）的合约
                for _item in _liq_sorted:
                    # CG 返回 coin 名（BTC/ETH），需转成 BTCUSDT
                    _coin = (_item.get("symbol") or _item.get("coin") or "").upper().strip()
                    if not _coin:
                        continue
                    _sym_full = _coin if _coin.endswith("USDT") else f"{_coin}USDT"
                    if _sym_full in _usdt_set:
                        _liq_syms.add(_sym_full)
                logger.debug(f"[scanner] CG清算量来源 命中 {len(_liq_syms)} 个（≤{POOL_LIQ_TOP_N}）")
                # 1h 多空失衡：独立于 24h 总量 Top，只标记方向，不加分（候选池阶段无信号方向）
                _bias = _calc_liq_bias(_liq_items)
                with _LIQ_BIAS_LOCK:
                    _LIQ_BIAS.clear()
                    _LIQ_BIAS.update(_bias)
                logger.debug(f"[scanner] 1h 清算多空失衡标记 {len(_bias)} 个"
                             f"（LONG={sum(1 for v in _bias.values() if v == 'LONG')} "
                             f"SHORT={sum(1 for v in _bias.values() if v == 'SHORT')}）")
        except Exception as _liq_e:
            logger.debug(f"[scanner] CG清算量来源获取失败，跳过: {_liq_e}")

        _scores = _pool_scores(usdt, _vol_top, _gain_syms | _loss_syms, _liq_syms)

        # ── 第三步：按评分降序（同分按成交额降序），截断到 SCAN_POOL_MAX ───────
        # 评分>0 的标的优先全部入池（降序，同分按成交额），0 分标的只在有剩余槽位时按成交额补入
        _vol_key = lambda t: -float(t.get("quoteVolume", 0))
        pool_scored = sorted([t for t in usdt if _scores[t["symbol"]] > 0],
                             key=lambda t: (-_scores[t["symbol"]], _vol_key(t)))
        pool_zero   = sorted([t for t in usdt if _scores[t["symbol"]] <= 0], key=_vol_key)
        _ranked = list(pool_scored)
        if len(_ranked) < int(SCAN_POOL_MAX):
            _ranked += pool_zero[:int(SCAN_POOL_MAX) - len(_ranked)]
        _ranked = _ranked[:int(SCAN_POOL_MAX)]
        pool = [t["symbol"] for t in _ranked]
        logger.info(f"[scanner] 候选池评分: 粗筛通过 {len(coarse)}，合格 {len(usdt)}，入池 {len(pool)}，"
                    f"其中评分>0 {sum(1 for t in _ranked if _scores[t['symbol']] > 0)} 个"
                    f"（评分>0 共 {len(pool_scored)}，0 分补入 {max(0, len(pool) - len(pool_scored))}）")

        # [v3.100] 粗筛结果为 0 时，检查是否上次有有效池——若有则保留，不覆盖
        if not pool:
            try:
                _old = json.loads(POOL_CACHE_FILE.read_text()).get("pool", [])
            except Exception:
                _old = []
            if _old:
                logger.warning(f"[scanner] refresh_pool: 本次粗筛结果为 0，保留上次候选池（{len(_old)} 个），跳过写入")
                return _old
            logger.warning("[scanner] refresh_pool: 粗筛结果为 0 且无历史池，候选池将被清空")

        POOL_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        POOL_CACHE_FILE.write_text(json.dumps({
            "ts": time.time(), "ts_unit": "seconds", "pool": pool,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }, ensure_ascii=False))
        logger.info(f"[scanner] 候选池刷新完成: {len(pool)} 个")
        # 候选池刷新后立即让 hub 订阅全部候选池 + 持仓标的 + BTC（持仓/BTC 放最前，超出 KLINE_HUB_SYMBOLS_MAX 时截断池尾），
        # 触发器触发的标的大概率已有 hub K 线，分析时不必临时拉 REST。只扩大订阅范围，不改触发器逻辑。
        try:
            _held = sorted({p["symbol"] for p in get_all_positions() if p.get("symbol")})
        except Exception:
            _held = []
        # 非交易时段的股票代币（港/美/韩/A股）不订阅：它们没有行情，REST 响应极慢，白白占用加载线程池；
        # 持仓中的股票代币（_held）始终保留订阅，持仓维护要读它们的 K 线
        _held_set = set(_held)
        _sub_pool = [s for s in pool if s in _held_set or _is_stock_token_tradeable(s)]
        if len(_sub_pool) != len(pool):
            logger.debug(f"[scanner] hub 订阅跳过非交易时段股票代币 {len(pool) - len(_sub_pool)} 个")
        _hub_subscribe_klines(["BTCUSDT"] + _held + _sub_pool)
        return pool

    except Exception as e:
        logger.error(f"[scanner] 候选池刷新失败: {e}")
        return []

_POOL_STALE_ALERT_TS = 0.0
_POOL_CACHE_MAX_AGE_SEC = 45 * 60   # [v3.87 STEP2-1] 候选池缓存最长有效期


def _load_pool() -> list:
    # [v3.87 STEP2-1] 原逻辑不检查缓存时间：定时刷新线程挂掉后，会一直静默使用很久以前的候选池。
    #   新：缓存超过 45 分钟则尝试重新刷新；刷新失败（返回空）时保留旧池兜底，并发送一次 Telegram 告警（1 小时内不重复）。
    try:
        d = json.loads(POOL_CACHE_FILE.read_text())
        pool = d.get("pool", [])
        age = time.time() - float(d.get("ts", 0) or 0)
        if pool and age > _POOL_CACHE_MAX_AGE_SEC:
            logger.warning(f"[scanner] 候选池缓存已过期 {age/60:.0f} 分钟，尝试重新刷新")
            fresh = refresh_pool()
            if fresh:
                return fresh
            global _POOL_STALE_ALERT_TS
            if time.time() - _POOL_STALE_ALERT_TS > 3600:
                _POOL_STALE_ALERT_TS = time.time()
                try:
                    from shared.notifier import notify as _notify
                    _notify(f"⚠️ 候选池缓存已过期 {age/60:.0f} 分钟且刷新失败，扫描暂用旧池，请检查网络/接口")
                except Exception as _ne:
                    logger.debug(f"[scanner] 候选池过期告警发送失败: {_ne}")
        return pool
    except Exception:
        logger.warning("[scanner] 候选池缓存不存在，临时刷新")
        return refresh_pool()

# ── 主扫描函数 ────────────────────────────────────────────────────────────────
def prefetch_symbol(*args, **kwargs):
    """_prefetch_symbol 的统计外壳（与 analyze_symbol 相同：累计本标的 K 线请求次数/耗时并入本轮汇总）。"""
    symbol = args[0] if args else kwargs.get("symbol", "")
    acc = [0, 0.0]
    _KL_TLS.acc = acc
    try:
        return _prefetch_symbol(*args, **kwargs)
    finally:
        _KL_TLS.acc = None
        if acc[0]:
            logger.debug(f"[scanner] {symbol} K线请求: 次数={acc[0]} 总耗时={acc[1]:.0f}ms")
            with _KL_ROUND_LOCK:
                _KL_ROUND["calls"] += acc[0]
                _KL_ROUND["ms"] += acc[1]
                _KL_ROUND["per_sym"][symbol] = (acc[0], acc[1])


def _run_parallel_process_pool(jobs: dict, mark_price_map: dict, max_workers: int, total_timeout_sec: float):
    """SCAN_USE_PROCESS_POOL=True 时的并行分析：线程池并发预取（_prefetch_symbol）→ 每个标的预取完立刻提交给
    进程池 worker（analyze_one）→ 主进程收到 (signal, events) 后调 _apply_post_events。
    返回值与 _run_parallel_with_deadline 一致：(results, unfinished)。进程池不可用时本轮回落线程池路径。
    jobs: {标的: (sym, ticker, active_syms, pending_syms, allowed_dirs, loss_blacklist, btc_trend, market_mode)}"""
    from shared import analysis_worker as _aw
    deadline = time.monotonic() + total_timeout_sec
    try:
        pool = _aw.get_healthy_pool()
    except Exception as e:
        logger.warning(f"[scanner] 分析进程池不可用，本轮回落线程池: {e}")
        return _run_parallel_with_deadline(
            _functools.partial(analyze_symbol, mark_price_map=mark_price_map), jobs, max_workers, total_timeout_sec)

    pre_exe = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="scan-pre")
    pending: dict = {}        # future -> (sym, "pre" | "proc")
    results: list = []
    broken = False
    for sym, a in jobs.items():
        f = pre_exe.submit(prefetch_symbol, a[0], a[1], a[5], deadline=deadline,
                           mark_price_map=mark_price_map, with_liq=True)
        pending[f] = (sym, "pre", a)
    try:
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            done, _ = concurrent.futures.wait(list(pending), timeout=remaining,
                                              return_when=concurrent.futures.FIRST_COMPLETED)
            for f in done:
                sym, stage, a = pending.pop(f)
                try:
                    res = f.result()
                except Exception as e:
                    if stage == "proc" and type(e).__name__ == "BrokenProcessPool":
                        broken = True
                    logger.debug(f"[scanner] {sym} future 异常({stage}): {e!r}")
                    continue
                if stage == "pre":
                    if res is None:
                        results.append((sym, None))
                        continue
                    ticker = a[1]
                    try:
                        pf = pool.submit(_aw.analyze_task, sym, ticker, res["k15"], res["k1h"], res["k4h"],
                                         res["prefetch"], a[2], a[3], a[4], a[5], a[6], a[7],
                                         {sym: (mark_price_map or {}).get(sym) or res["prefetch"].get("mp")})
                        pending[pf] = (sym, "proc", a)
                    except Exception as e:
                        broken = True
                        logger.warning(f"[scanner] {sym} 提交进程池失败: {e!r}")
                else:
                    sig, events = res
                    try:
                        results.append((sym, _apply_post_events(sig, events)))
                    except Exception as e:
                        logger.debug(f"[scanner] {sym} 后置处理异常: {e!r}")
    finally:
        unfinished = [v[0] for v in pending.values()]
        pre_exe.shutdown(wait=False, cancel_futures=True)
        if any(v[1] == "proc" for v in pending.values()):
            _aw.reset_pool(kill=True)          # 有 worker 还在跑（卡住）：强制终止，下一轮重建
        elif broken:
            _aw.reset_pool(kill=True)
    if unfinished:
        logger.warning(
            f"[scanner] 整轮超时({total_timeout_sec}s)：{len(unfinished)}/{len(jobs)} 个标的未完成已放弃，"
            f"已采集 {len(results)} 个结果；未完成示例: {unfinished[:10]}"
        )
    return results, unfinished


def _run_parallel_with_deadline(fn, jobs: dict, max_workers: int, total_timeout_sec: float):
    """并行执行 jobs 并在整轮 deadline 到点后真正放弃剩余任务。

    jobs: {标的: 位置参数元组}；fn 以 fn(*args, deadline=<monotonic 截止时间>) 调用。
    返回 (results, unfinished)：results 为 [(标的, 返回值)]（完成顺序，异常的标的不在内），
    unfinished 为到点时仍未完成的标的列表。

    [fix] 不用 `with ThreadPoolExecutor`：其退出时 shutdown(wait=True) 会等所有线程结束，
    一个标的卡住就拖到外层 300s 超时。这里到点后 shutdown(wait=False, cancel_futures=True)，
    未开始的任务被取消；已在跑的线程无法强杀，只能放弃等待（由 fn 内的 deadline 检查让其尽快自行退出）。
    """
    deadline = time.monotonic() + total_timeout_sec
    exe = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="scan-sym")
    futures = {exe.submit(fn, *args, deadline=deadline): sym for sym, args in jobs.items()}
    results: list = []
    try:
        for fut in concurrent.futures.as_completed(futures, timeout=total_timeout_sec):
            sym = futures[fut]
            try:
                results.append((sym, fut.result()))
            except Exception as e:
                logger.debug(f"[scanner] {sym} future 异常: {e}")
            if time.monotonic() > deadline:
                break
    except concurrent.futures.TimeoutError:
        pass
    finally:
        unfinished = [sym for fut, sym in futures.items() if not fut.done()]
        exe.shutdown(wait=False, cancel_futures=True)
    if unfinished:
        logger.warning(
            f"[scanner] 整轮超时({total_timeout_sec}s)：{len(unfinished)}/{len(futures)} 个标的未完成已放弃，"
            f"已采集 {len(results)} 个结果；未完成示例: {unfinished[:10]}"
        )
    return results, unfinished


def _filter_and_register_signals(
    signals_raw: list,
    active_syms: set,
    pending_syms: set,
    market_mode: str,
    btc_trend: str,
    budget_info: dict,
    source: str,
    signal_watcher=None,
    dry_run: bool = False,
    result: dict | None = None,
) -> tuple:
    """第四步（阶段5 从 run_scan 原样抽出，判定逻辑逐行未改）：相关性/BTC 协同/黑名单/新闻/评分/门槛/
    槽位过滤 → 仓位缩放 → 入场区间 → signal_watcher.add_signal（或无 watcher 时直接下单）。
    扫描和触发器共用。budget_info 打包 run_scan 前置计算的上下文；result 就地更新计数。
    返回 (本次注册/下单成功数, avail_signals)。source 写入每条信号的 "source" 字段。"""
    if result is None:
        result = {"signals_final": 0, "placed": 0, "rejected": []}
    _bi = dict(budget_info or {})
    _bi.setdefault("_btc_trend", btc_trend)
    _bi.setdefault("_mkt_mode", market_mode)
    _btc_trend = _bi["_btc_trend"]
    _mkt_mode = _bi["_mkt_mode"]
    long_cnt = _bi["long_cnt"]
    short_cnt = _bi["short_cnt"]
    _dyn_max_long = _bi["_dyn_max_long"]
    _dyn_max_short = _bi["_dyn_max_short"]
    _global_long_scale = _bi["_global_long_scale"]
    _global_short_scale = _bi["_global_short_scale"]
    _macro_conservative = _bi["_macro_conservative"]
    _macro_event_desc = _bi["_macro_event_desc"]
    _post_shock = _bi["_post_shock"]
    _mark_price_map_ts = _bi["_mark_price_map_ts"]
    now = _bi["now"]
    # ── v3.92 周日收尾：周日 UTC WEEKEND_CUTOFF_SUNDAY_UTC_HOUR 起不再注册新信号 ──
    if _is_weekend_cutoff(now):
        logger.info("[scanner] 周日收尾时段，不注册新信号")
        return 0, []
    _weekend_counter_sync(now)
    _loss_blacklist = _bi["_loss_blacklist"]
    _LOSS_THRESHOLD = _bi["_LOSS_THRESHOLD"]
    _mark_price_map = _bi["_mark_price_map"]
    for _s in signals_raw:
        if isinstance(_s, dict):
            _s["source"] = source
    _placed0 = result.get("placed", 0)
    # ── 第四步：信号汇总与排序 ────────────────────────────────────────────────
    # v3.34: 槽位过滤已后移到评分过滤之后，先将 signals_raw 赋给 avail_signals
    avail_signals = list(signals_raw)

    # ── #1 相关性过滤：同方向同策略集中持仓保护 ─────────────────────────────────
    from config import CORR_MAX_REVERSAL_SAME_DIR, CORR_MAX_PULLBACK_SAME_DIR, CORR_MAX_SAME_DIR_TOTAL
    from shared.state import get_all_positions as _all_pos
    _open_pos = _all_pos()
    # 统计已有持仓的方向×策略分布
    _rev_long  = sum(1 for p in _open_pos if p.get("mode") == "reversal"  and p.get("direction","").upper() == "LONG"  and p.get("status") in ("open","pending"))
    _rev_short = sum(1 for p in _open_pos if p.get("mode") == "reversal"  and p.get("direction","").upper() == "SHORT" and p.get("status") in ("open","pending"))
    _pb_long   = sum(1 for p in _open_pos if p.get("mode") == "pullback"  and p.get("direction","").upper() == "LONG"  and p.get("status") in ("open","pending"))
    _pb_short  = sum(1 for p in _open_pos if p.get("mode") == "pullback"  and p.get("direction","").upper() == "SHORT" and p.get("status") in ("open","pending"))
    _corr_filtered = []
    for sig in avail_signals:
        _mode = sig.get("sig_type", "pullback")
        _dir  = sig["direction"].upper()
        _reject_reason = ""
        if _mode == "reversal":
            _cnt = _rev_long if _dir == "LONG" else _rev_short
            if _cnt >= CORR_MAX_REVERSAL_SAME_DIR:
                _reject_reason = f"相关性过滤: {_dir} reversal 已有{_cnt}个持仓>={CORR_MAX_REVERSAL_SAME_DIR}"
        elif _mode == "pullback":
            _cnt = _pb_long if _dir == "LONG" else _pb_short
            if _cnt >= CORR_MAX_PULLBACK_SAME_DIR:
                _reject_reason = f"相关性过滤: {_dir} pullback 已有{_cnt}个持仓>={CORR_MAX_PULLBACK_SAME_DIR}"
        elif _mode == "range_bounce":
            # range_bounce 只做多，和 pullback LONG 共享计数器
            _cnt = _pb_long
            if _cnt >= CORR_MAX_PULLBACK_SAME_DIR:
                _reject_reason = f"相关性过滤: LONG range_bounce+pullback 已有{_cnt}个>={CORR_MAX_PULLBACK_SAME_DIR}"
        if not _reject_reason:
            # 合计上限（reversal+pullback+range_bounce 同方向）
            _total_long  = _rev_long  + _pb_long
            _total_short = _rev_short + _pb_short
            _total = _total_long if _dir == "LONG" else _total_short
            if _mode in ("reversal", "pullback", "range_bounce") and _total >= CORR_MAX_SAME_DIR_TOTAL:
                _reject_reason = f"相关性过滤: {_dir} reversal+pullback+range_bounce 合计{_total}>={CORR_MAX_SAME_DIR_TOTAL}"
        if _reject_reason:
            result["rejected"].append({"symbol": sig["symbol"], "reason": _reject_reason})
            logger.info(f"[scanner] {sig['symbol']} {_reject_reason}")
            continue
        # 通过：更新计数器，供后续信号参考
        if _mode == "reversal" and _dir == "LONG":  _rev_long  += 1
        if _mode == "reversal" and _dir == "SHORT": _rev_short += 1
        if _mode == "pullback" and _dir == "LONG":  _pb_long   += 1
        if _mode == "pullback" and _dir == "SHORT": _pb_short  += 1
        if _mode == "range_bounce":                 _pb_long   += 1  # 共享 pullback LONG 计数器
        _corr_filtered.append(sig)
    avail_signals = _corr_filtered

    # ── 空单 BTC 协同过滤（v3.5 重写）──────────────────────────────────────────
    # 熊市（down）：顺势空单直接放行——这是最需要放宽的场景
    # 震荡（neutral）：空单需 RSI4H>65 或 funding>0
    # 牛市（up）：空单需 RSI4H>=75 且 funding>0.00005（真正超买）
    _btc_neutral = _btc_trend not in ("up", "down")
    filtered_shorts = []
    for sig in avail_signals:
        if sig["direction"] != "SHORT":
            filtered_shorts.append(sig)
            continue
        ind     = sig["ind"]
        # 问题四修复：funding 在 sig 顶层，不在 ind 里
        funding = float(sig.get("funding", ind.get("funding", ind.get("funding_rate", 0))))
        rsi_4h  = float(ind.get("rsi_4h", 50))
        if _btc_trend == "down":
            # 熊市顺势空，仅要求不是极端负费率
            # v3.41.1: 门槛从 -0.05% 放宽到 BTC_BEAR_SHORT_MIN_FUNDING (-0.15%)
            # [v3.101] 4H 多头排列 gap > 8% 时：即使熊市顺势，也要求至少 1/2 衰竭证据
            #          防止在强趋势逆势下盲目做空。gap <= 8% 不受影响，正常放行。
            try:
                from config import BTC_BEAR_SHORT_MIN_FUNDING as _bear_short_min_fr
            except ImportError:
                _bear_short_min_fr = -0.0015
            _4h_bull_gap_sc = float((sig.get("judge_detail") or sig.get("reason") or ind).get("4h_ema_bull_gap", 0) or 0)
            _exhaust_ev_sc  = (sig.get("judge_detail") or sig.get("reason") or {}).get("exhaustion_evidence", [])
            _bear_4h_blocked = _4h_bull_gap_sc > 8.0 and len(_exhaust_ev_sc) < 1
            if _bear_4h_blocked:
                result["rejected"].append({"symbol": sig["symbol"],
                    "reason": f"熊市顺势但4H多头排列过强(gap={_4h_bull_gap_sc:.1f}%>8%)且衰竭证据不足({len(_exhaust_ev_sc)}/1)，拒绝"})
                logger.info(f"[scanner] {sig['symbol']} 熊市顺势但4H gap过强被拒: gap={_4h_bull_gap_sc:.1f}% exhaust={_exhaust_ev_sc}")
            elif funding >= _bear_short_min_fr:
                filtered_shorts.append(sig)
                logger.info(f"[scanner] {sig['symbol']} 空单熊市顺势通过: funding={funding:.5f} rsi4h={rsi_4h:.1f}")
            else:
                result["rejected"].append({"symbol": sig["symbol"],
                    "reason": f"熊市空单被拒（费率极端负{funding*100:.4f}% < {_bear_short_min_fr*100:.2f}%）"})
        elif _btc_neutral:
            # 震荡：需要一定RSI或资金费率支撑
            if rsi_4h >= 65 or funding > 0:
                filtered_shorts.append(sig)
                logger.info(f"[scanner] {sig['symbol']} 空单震荡通过: rsi4h={rsi_4h:.1f} funding={funding:.5f}")
            else:
                result["rejected"].append({"symbol": sig["symbol"],
                    "reason": f"震荡空单被拒(RSI4H={rsi_4h:.1f}<65 且 funding={funding:.5f}<=0)"})
        else:  # btc up
            # 牛市：需要真正超买
            if rsi_4h >= 75 and funding >= 0.00005:
                filtered_shorts.append(sig)
                logger.info(f"[scanner] {sig['symbol']} 空单牛市超买通过: rsi4h={rsi_4h:.1f} funding={funding:.5f}")
            else:
                result["rejected"].append({"symbol": sig["symbol"],
                    "reason": f"牛市禁空(RSI4H={rsi_4h:.1f}<75 or funding={funding:.5f}<0.00005)"})
    avail_signals = filtered_shorts

    # 亏损黑名单已在并行扫描前计算，此处标记为已提前过滤（保留 rejected 记录）
    filtered_bl = []
    for sig in avail_signals:
        if sig["symbol"] in _loss_blacklist:
            result["rejected"].append({"symbol": sig["symbol"],
                "reason": f"累计亏损黑名单(>={_LOSS_THRESHOLD}U近期)"})
        else:
            filtered_bl.append(sig)
    avail_signals = filtered_bl

    # ── 代币解锁风险过滤（LONG；UNLOCK_FILTER_ENABLED=False 时只记录不拒绝）──
    avail_signals = _apply_unlock_filter(avail_signals, result)

    # ── ② 新闻信号过滤（v3.8，超跌解锁 v3.9）──────────────────────────────────
    # bearish 新闻 → LONG 一票否决（默认）
    # 但是有一种超跌解锁情况：利空新闻已发布超过48h且已跌 > 60%且 RSI_1h < 25，
    #   说明市场已充分定价利空，进入超卖反弹阶段，此时 reversal LONG 才合理
    # 参考：DEXE 2026年7月事件—事件后第3天从最低点反弹+232%
    if _NEWS_SIGNAL_OK:
        news_filtered = []
        for sig in avail_signals:
            sym_base = sig["symbol"].replace("USDT", "")
            try:
                nb = _get_news_bias(sym_base)
            except Exception:
                nb = None
            sig["ind"]["news_bias"] = nb  # 存入 ind，供后续日志 / recheck 使用

            if nb == "bearish" and sig["direction"] == "LONG":
                # 检查超跌解锁条件（v3.9）
                # 条件：必须是 reversal（超卖反弹）且同时满足：
                #   1. 新闻发布时间 > 48小时前（即 news 最新一条时间 < 当前-48h）
                #   2. 该币 1h RSI < 25（深度超卖）
                #   3. 24H 涨跌已转正（止跌信号）
                # 三项全部满足，才解锁，否则一票否决
                unlock = False
                unlock_reason = ""

                if sig.get("sig_type") == "reversal":
                    rsi_1h = float(sig["ind"].get("rsi_1h", sig["ind"].get("rsi", 100)))
                    chg24_v = float(sig.get("chg24", 0))  # 正值 = 涨
                    # 用 get_news_bias_detail 获取新闻发布时间
                    try:
                        from shared.news_signal import get_news_bias_detail as _ns_detail
                        _nd = _ns_detail(sym_base)
                        news_age_h = _nd.get("news_age_h", 0.0)
                    except Exception:
                        news_age_h = 0

                    if rsi_1h < 25 and chg24_v > 0 and news_age_h > 48:
                        unlock = True
                        unlock_reason = (
                            f"bearish新闻已{news_age_h:.0f}h前, "
                            f"RSI_1h={rsi_1h:.1f}<25 深度超卖+24H已转正={chg24_v:+.1f}%"
                        )

                if unlock:
                    sig["news_unlock"] = True
                    logger.warning(
                        f"[scanner] ② {sig['symbol']} LONG reversal 触发超跌解锁: {unlock_reason}"
                    )
                else:
                    result["rejected"].append({
                        "symbol": sig["symbol"],
                        "reason": "利空新闻一票否决 LONG（bearish news veto）",
                    })
                    logger.warning(f"[scanner] ② {sig['symbol']} LONG 被利空新闻一票否决")
                    continue

            # caution：攻击事件+已快速响应，不否决但评分-10
            # v3.41: LLM 复核——实际利好则降级为 neutral，比关键词更严重则升级为一票否决
            if nb == "caution" and sig["direction"] == "LONG":
                try:
                    from config import LLM_JUDGE_ENABLED, LLM_JUDGE_NEWS
                    if LLM_JUDGE_ENABLED and LLM_JUDGE_NEWS:
                        from shared.news_signal import _fetch_raw_news as _get_raw_txt
                        _news_items = _get_raw_txt(sym_base)
                        _raw_news = ' | '.join(
                            str(i.get('title', '') or i.get('content', ''))
                            for i in _news_items[:3]
                        ) if _news_items else ''
                        if _raw_news:
                            from shared.llm_judge import judge_news_sentiment_llm
                            _ns = judge_news_sentiment_llm(
                                sig['symbol'], _raw_news, sig['direction'])
                            if _ns == 'bullish':
                                nb = 'neutral'
                                logger.info(f"[scanner] ② {sig['symbol']} LLM复核 caution→neutral")
                            elif _ns == 'bearish':
                                result['rejected'].append({'symbol': sig['symbol'],
                                    'reason': 'LLM新闻语义: caution升级为bearish，一票否决'})
                                logger.warning(
                                    f"[scanner] ② {sig['symbol']} LLM caution→bearish 一票否决")
                                continue
                except Exception as _ne:
                    logger.debug(f"[scanner] {sym_base} 新闻LLM跳过: {_ne}")
                if nb == "caution":  # 未被 LLM 改变，执行原有评分惩罚
                    sig["ind"]["final_score"] = max(0, sig["ind"].get("final_score", 0) - 10)
                    logger.info(f"[scanner] ② {sig['symbol']} LONG caution新闻（攻击+快速响应），评分-10")

            if nb == "bullish" and sig["direction"] == "LONG":
                sig["news_exempt"] = True  # 标记：利好新闻豁免，后续 BTC 熊市过滤直接放行
                logger.info(f"[scanner] ② {sig['symbol']} LONG 有利好新闻，标记 news_exempt")

            if nb == "bearish" and sig["direction"] == "SHORT":
                # 只有明确利空（bearish）才豁免牛市限制
                # caution（攻击事件+已快速响应）不应豁免：项目正在处理，价格短暂下跌后往往反弹，做空豁免反而危险
                sig["news_exempt"] = True  # 利空新闻 → SHORT 豁免 BTC 牛市限制
                logger.info(f"[scanner] ② {sig['symbol']} SHORT 有利空新闻(bearish)，标记 news_exempt")

            news_filtered.append(sig)
        avail_signals = news_filtered
        logger.info(f"[scanner] ② 新闻过滤后剩余信号 {len(avail_signals)} 个")

    # ── ③ BTC 4H 空头排列时，硬性禁止做多 reversal（v3.5）────────────────────
    # 复用现有 _btc_trend 判断（EMA9<EMA20<EMA50 = down）
    # 熊市中做多 reversal = 对抗整体流动性收缩，系统性风险极高
    # 豁免（v3.7）：山寨币独立行情时豁免 BTC 空头过滤
    #   ① 24H 涨幅 > 25%（极端独立行情）
    #   ② 资金费率 > 0.05%（市场强烈压注多方向）
    #   ③ 量比 > 3 且 24H 涨幅 > 15%（量价共振）
    #   ④ 利好新闻（news_exempt=True）（v3.8）
    if _btc_trend == "down":
        btc_bear_filtered = []
        for sig in avail_signals:
            if sig["direction"] == "LONG" and sig.get("sig_type") == "reversal":
                chg24_s    = abs(float(sig.get("chg24", 0)))
                funding_s  = float(sig.get("funding", sig["ind"].get("funding", 0)))
                vol_ratio_s = float(sig["ind"].get("vol_ratio", 0))
                # 豁免检查
                exempt = False
                exempt_reason = ""
                if sig.get("news_exempt"):
                    exempt = True
                    exempt_reason = f"利好新闻豁免 (news_bias=bullish)"
                elif chg24_s > 30:   # v3.21: 25→30%，仅极端独立行情才豁免
                    exempt = True
                    exempt_reason = f"24H涨幅极端 {chg24_s:.1f}%>30%"
                elif funding_s > 0.002:  # v3.21: 0.0005→0.002，0.05%太常见，收紧到0.2%
                    exempt = True
                    exempt_reason = f"资金费率 {funding_s*100:.3f}%>0.2%"
                elif vol_ratio_s > 3 and chg24_s > 15:
                    exempt = True
                    exempt_reason = f"量比 {vol_ratio_s:.1f}>3 且涨幅 {chg24_s:.1f}%>15%"

                if exempt:
                    btc_bear_filtered.append(sig)
                    logger.info(
                        f"[scanner] ③ {sig['symbol']} LONG reversal 豁免 BTC熊市过滤: {exempt_reason}"
                    )
                else:
                    result["rejected"].append({
                        "symbol": sig["symbol"],
                        "reason": f"BTC 4H空头排列禁止做多reversal (btc_trend=down)",
                    })
                    logger.info(f"[scanner] ③ {sig['symbol']} LONG reversal 被BTC空头硬性拦截")
            else:
                btc_bear_filtered.append(sig)
        avail_signals = btc_bear_filtered
        logger.info(f"[scanner] ③ BTC空头：过滤后剩余信号 {len(avail_signals)} 个")

    # ── BTC 趋势加减分 + CG bonus（与 paper_scanner 对齐）─────────────────────
    _cg_import_ok = False
    try:
        from shared.coinglass_api import score_symbol_cg_bonus as _cg_bonus_fn
        _cg_import_ok = True
    except Exception:
        pass

    for sig in avail_signals:
        ind        = sig["ind"]
        direction  = sig["direction"]
        # signal_strength → base_score（strong=70, medium=50, weak=30）
        from config import SCORE_BASE_STRONG, SCORE_BASE_MEDIUM, SCORE_BASE_WEAK
        _strength = sig.get("signal_strength", ind.get("signal_strength", "medium"))
        _base_map = {"strong": SCORE_BASE_STRONG, "medium": SCORE_BASE_MEDIUM, "weak": SCORE_BASE_WEAK}
        base_score = _base_map.get(_strength, SCORE_BASE_MEDIUM)

        # BTC 趋势 ±5 分
        btc_adj = 0
        if   _btc_trend == "up"   and direction == "LONG":  btc_adj = +5
        elif _btc_trend == "up"   and direction == "SHORT": btc_adj = -5
        elif _btc_trend == "down" and direction == "LONG":  btc_adj = -5
        elif _btc_trend == "down" and direction == "SHORT": btc_adj = +5
        if btc_adj:
            logger.debug(f"[scanner] {sig['symbol']} btc_trend={_btc_trend} adj={btc_adj:+d}")

        # CG bonus（清算热力图 + 巨鲸 + KOL，失败自动跳过）
        cg_bonus = 0
        if _cg_import_ok:
            try:
                mark = float(_get_mark_price(sig["symbol"]).get("markPrice", 0))
                cg_bonus = _cg_bonus_fn(sig["symbol"], direction, mark, ind)
                cg_bonus = max(0, min(10, cg_bonus))  # [refactor-B11] 下限 -10→0：CG 只作加分项，不扣分（原 v3.65: ±10）
            except Exception:
                pass

        # ── v3.38 策略×市场模式适配评分（加入熊市强度感知）────────────────────
        # reversal=逆势（胜率低）pullback=顺势（趋势加持）range_bounce=区间依赖
        _sig_type = sig.get("sig_type", ind.get("sig_type", "pullback"))
        strategy_adj = 0

        if _mkt_mode == "bear":
            # v3.38: 读取熊市强度，区分三档惩罚/奖励
            try:
                from shared.risk_manager import get_bear_strength as _get_bear_str
                _bear_str = _get_bear_str() or "weak"
            except Exception:
                _bear_str = "weak"
            if _sig_type == "reversal" and direction == "LONG":
                # 熊市逆势做多：强熊惩罚更重
                # [refactor-B11] 收窄：原 -15/-12/-10 → -10/-8/-6（保留方向性，不清零）
                strategy_adj = -10 if _bear_str == "strong" else \
                               -8  if _bear_str == "moderate" else -6
            elif _sig_type == "pullback" and direction == "SHORT":
                # 熊市顺势做空：强熊加分更多
                strategy_adj = +10 if _bear_str == "strong" else \
                               +7  if _bear_str == "moderate" else +5
            elif _sig_type == "range_bounce":
                # 强熊下区间支撑失败概率更高
                # [refactor-B11] 收窄：原 -10/-7/-5 → -7/-5/-3
                strategy_adj = -7 if _bear_str == "strong" else \
                               -5 if _bear_str == "moderate" else -3
            if strategy_adj:
                logger.info(f"[scanner] {sig['symbol']} bear强度={_bear_str} "
                            f"strategy_adj={strategy_adj:+d} "
                            f"(type={_sig_type} dir={direction})")
        elif _mkt_mode == "bull":
            if _sig_type == "reversal" and direction == "SHORT":
                strategy_adj = -10  # 牛市逆势做空：系统性风险高
            elif _sig_type == "pullback" and direction == "LONG":
                strategy_adj = +5   # 牛市顺势做多：趋势加持
            elif _sig_type == "range_bounce":
                strategy_adj = +5   # 牛市支撑更可靠
            if strategy_adj:
                logger.info(f"[scanner] {sig['symbol']} strategy_adj={strategy_adj:+d} "
                            f"(mkt=bull type={_sig_type} dir={direction})")
        elif _mkt_mode == "range":
            if _sig_type == "breakout":
                strategy_adj = -5   # 震荡市突破信号假突破概率高
            elif _sig_type == "reversal":
                strategy_adj = +5   # 震荡市反转信号胜率更高
            elif _sig_type == "range_bounce":
                strategy_adj = +10  # 震荡市是 range_bounce 最适合的环境
            if strategy_adj:
                logger.info(f"[scanner] {sig['symbol']} strategy_adj={strategy_adj:+d} "
                            f"(mkt=range type={_sig_type} dir={direction})")

        # v3.49.2 stop_hunt 策略评分覆盖
        if _sig_type == "stop_hunt":
            from config import STOP_HUNT_BASE_STRONG, STOP_HUNT_BASE_MEDIUM
            _sh_strength = (sig.get("signal_strength")
                            or (sig.get("reason") or {}).get("signal_strength", "medium"))
            base_score = STOP_HUNT_BASE_STRONG if _sh_strength == "strong" else STOP_HUNT_BASE_MEDIUM
            # 修复3：stop_hunt 不受 btc_adj 影响
            # 止损猎杀是局部流动性事件，与 BTC 大趋势关联度低
            # btc_adj 会系统性推向 SHORT（BTC 偏跌时），导致逆势开仓
            btc_adj = 0
            # 覆盖前面通用 strategy_adj
            if _mkt_mode == "bull" and direction == "LONG":
                strategy_adj = +5
            elif _mkt_mode == "bull" and direction == "SHORT":
                strategy_adj = -5
            elif _mkt_mode == "range":
                strategy_adj = 0
            elif _mkt_mode == "bear" and direction == "SHORT":
                strategy_adj = +5
            elif _mkt_mode == "bear" and direction == "LONG":
                strategy_adj = -15   # 熊市不做多头猎杀
            logger.info(
                f"[scanner] {sig['symbol']} stop_hunt base={base_score} "
                f"btc_adj=0(cleared) strategy_adj={strategy_adj:+d} "
                f"(mkt={_mkt_mode} dir={direction} strength={_sh_strength})"
            )

        # v3.60 oi_divergence 策略评分覆盖
        elif _sig_type == "oi_divergence":
            from config import OI_DIV_BASE_STRONG, OI_DIV_BASE_MEDIUM
            _oi_strength = (sig.get("reason") or {}).get("signal_strength", "medium")
            base_score = OI_DIV_BASE_STRONG if _oi_strength == "strong" else OI_DIV_BASE_MEDIUM
            btc_adj = btc_adj // 2
            if _mkt_mode == "bear" and direction == "LONG":
                strategy_adj = -10
            elif _mkt_mode == "bull" and direction == "SHORT":
                strategy_adj = -10
            elif _mkt_mode == "range":
                strategy_adj = +8
            logger.info(
                f"[scanner] {sig['symbol']} oi_divergence base={base_score} "
                f"btc_adj={btc_adj:+d}(half) strategy_adj={strategy_adj:+d} "
                f"(mkt={_mkt_mode} dir={direction} strength={_oi_strength})"
            )

        # v3.61 ema_trend 策略评分覆盖
        elif _sig_type == "ema_trend":
            from config import EMA_TREND_BASE_STRONG, EMA_TREND_BASE_MEDIUM
            _et_strength = (sig.get("reason") or {}).get("signal_strength", "medium")
            base_score = EMA_TREND_BASE_STRONG if _et_strength == "strong" else EMA_TREND_BASE_MEDIUM
            # strategy_adj 市场环境（v3.83：按方向区分，做空在熊市顺势、牛市逆势）
            _et_is_short = str(sig.get("direction", "")).upper() == "SHORT"
            if _mkt_mode == "bull":
                strategy_adj = -15 if _et_is_short else +8
            elif _mkt_mode == "bear":
                strategy_adj = +8 if _et_is_short else -10   # [refactor-B11] 熊市 ema_trend LONG：-15→-10
            elif _mkt_mode == "range":
                strategy_adj = -5   # 震荡市 EMA 排列容易失效
            logger.info(
                f"[scanner] {sig['symbol']} ema_trend base={base_score} "
                f"btc_adj={btc_adj:+d} strategy_adj={strategy_adj:+d} "
                f"(mkt={_mkt_mode} strength={_et_strength})"
            )

        # ── v3.58 市场周期阶段加分（初期/中段/末期区分）
        # [fix] 移到模式分支与专属覆盖之后，以 += 叠加，避免被 strategy_adj = ... 冲掉───────────────────────
        # early_bull: breakout信号最佳(+5)，reversal SHORT最危险(-5)
        # mid_bull:   pullback信号最佳(+5)，趋势跟随效果最好
        # late_bull:  所有做多信号降权(-5)，趋势即将耗尽
        try:
            from shared.risk_manager import get_market_cycle_phase as _gcyc
            _cycle = _gcyc()
            ind["market_cycle"] = _cycle
            if _cycle == "early_bull":
                if _sig_type == "breakout" and direction == "LONG":
                    strategy_adj += 5
                elif _sig_type == "reversal" and direction == "SHORT":
                    strategy_adj -= 5
            elif _cycle == "mid_bull":
                if _sig_type == "pullback" and direction == "LONG":
                    strategy_adj += 5
            elif _cycle == "late_bull":
                if direction == "LONG" and _sig_type in ("pullback", "reversal", "breakout", "range_bounce"):
                    strategy_adj -= 5
                    logger.info(f"[scanner] {sig['symbol']} late_bull 做多降权 -5")
        except Exception:
            pass
        # ── v3.31 大行情恢复窗口：评分额外 -10 ─────────────────────────────
        post_shock_adj = -10 if _post_shock else 0
        if post_shock_adj:
            logger.info(f"[scanner] {sig['symbol']} post_shock_adj={post_shock_adj:+d}")

        # ── v3.46 中波动档评分惩罚 ──────────────────────────────────────────
        from config import ATR_TIER_MID_SCORE_ADJ, ATR_TIER_HIGH_MIN as _AHM_S, ATR_TIER_MID_MIN as _AMM_S
        # 用 atr_ratio 自算波动档（与 judge 分档一致），不读 ind["_vol_tier"] 残留值
        _atr_r_s = float(sig["ind"].get("atr_ratio", 0) or 0)
        _vol_tier = "high" if _atr_r_s >= _AHM_S else ("mid_vol" if _atr_r_s >= _AMM_S else "low")
        atr_tier_adj = ATR_TIER_MID_SCORE_ADJ if _vol_tier == "mid_vol" else 0
        if atr_tier_adj:
            logger.info(f"[scanner] {sig['symbol']} atr_tier_adj={atr_tier_adj:+d} (mid_vol档)")

        # ── v3.46 脉冲大涨做空惩罚 ──────────────────────────────────────────
        _pulse_penalty = (sig.get("reason") or {}).get("pulse_short_penalty", 0)
        if _pulse_penalty:
            logger.info(f"[scanner] {sig['symbol']} pulse_short_penalty={_pulse_penalty:+d}")

        # ── [fix #35a] 原 v3.58 在此处按 taker_buy_ratio 加减 ±3 分，而 _apply_score_bonuses 里的
        #   taker 加分（hub 数据，见 _calc_taker_bonus）读的是同一类信息 → 同一因子被重复计分。
        #   现删除这里的重复逻辑，只保留 _apply_score_bonuses 的 taker 加分。taker_adj 置 0 以保持日志/字段兼容。
        taker_adj = 0

        new_score = min(100, max(0, base_score + btc_adj + cg_bonus + strategy_adj + post_shock_adj + atr_tier_adj + _pulse_penalty + taker_adj))
        # 新增三个加分项（hub 数据；在现有评分之后叠加，总分上限 100）
        new_score, _bonus_info = _apply_score_bonuses(sig["symbol"], direction, _sig_type, new_score)
        if new_score != base_score:
            logger.info(f"[scanner] {sig['symbol']} score {base_score}→{new_score} "
                        f"(btc={btc_adj:+d} cg={cg_bonus:+d} strat={strategy_adj:+d} shock={post_shock_adj:+d} tier={atr_tier_adj:+d} pulse={_pulse_penalty:+d} hub_bonus={_bonus_info.get('hub_bonus', 0):+d})")
        ind["final_score"]    = new_score
        sig["final_score"]    = new_score     # 信号 dict 顶层也带上，下游/日志不必钻进 ind
        ind["btc_trend"]      = _btc_trend
        ind["btc_adj"]        = btc_adj
        ind["cg_bonus"]       = cg_bonus
        ind["strategy_adj"]   = strategy_adj
        ind["post_shock_adj"] = post_shock_adj
        ind["atr_tier_adj"]   = atr_tier_adj
        ind["taker_adj"]      = taker_adj
        ind["score_taker_bonus"]    = _bonus_info["taker"]
        ind["score_range_bonus"]    = _bonus_info["range"]
        ind["score_volprice_bonus"] = _bonus_info["volprice"]

    # ── 宏观保守模式：过滤 reversal/breakout，其余信号评分门槛+10 ────────────
    if _macro_conservative:
        _macro_filtered = []
        for sig in avail_signals:
            _st = sig.get("sig_type", "")
            if _st in ("reversal", "breakout"):
                result["rejected"].append({"symbol": sig["symbol"],
                    "reason": f"宏观保守模式：停止 {_st}（{_macro_event_desc}）"})
                logger.info(f"[scanner] {sig['symbol']} 宏观保守：拦截 {_st}")
                continue
            # 其余策略评分 -10（等效于门槛提高10分）
            sig["ind"]["final_score"] = max(0, sig["ind"].get("final_score", 0) - 10)
            sig["ind"]["macro_adj"]   = -10
            _macro_filtered.append(sig)
        avail_signals = _macro_filtered
        logger.info(f"[scanner] 宏观保守过滤后剩余信号 {len(avail_signals)} 个")

    # ── 工作日 final_score 最低门槛过滤 ────────────────────────────────────
    from config import MIN_SIGNAL_SCORE_WEEKDAY, MIN_SIGNAL_SCORE_WEEKEND
    _is_weekend = _is_weekend_period(now)
    if not _is_weekend:
        _weekday_filtered = []
        for sig in avail_signals:
            sc = sig["ind"].get("final_score", 0)
            if sc < MIN_SIGNAL_SCORE_WEEKDAY:
                # ── v3.41 边界信号：55~65 分交给 LLM，其余直接拒绝 ────────────
                _llm_rescued = False
                try:
                    from config import (LLM_JUDGE_ENABLED, LLM_JUDGE_BORDERLINE,
                                        LLM_BORDERLINE_SCORE_MIN, LLM_BORDERLINE_SCORE_MAX,
                                        LLM_JUDGE_MIN_CONFIDENCE)
                    if (LLM_JUDGE_ENABLED and LLM_JUDGE_BORDERLINE
                            and LLM_BORDERLINE_SCORE_MIN <= sc <= LLM_BORDERLINE_SCORE_MAX
                            and sig.get('sig_type') != 'funding_arb'):
                        from shared.llm_judge import judge_borderline_signal_llm
                        _bl_v = judge_borderline_signal_llm(
                            sig['symbol'], sig.get('sig_type', ''), sig.get('direction', ''),
                            sc, sig['ind'], float(sig.get('chg24', 0)), _mkt_mode
                        )
                        if (_bl_v.get('decision') == 'open'
                                and _bl_v.get('confidence', 0) >= LLM_JUDGE_MIN_CONFIDENCE):
                            sig['ind']['final_score']    = MIN_SIGNAL_SCORE_WEEKDAY + 1
                            sig['ind']['llm_bl_boosted'] = True
                            sig['ind']['llm_reason']     = _bl_v.get('reason', '')
                            logger.info(
                                f"[scanner] {sig['symbol']} 边界LLM提升: "
                                f"{sc}→{MIN_SIGNAL_SCORE_WEEKDAY+1} {_bl_v.get('reason','')}"
                            )
                            _llm_rescued = True
                        else:
                            result["rejected"].append({"symbol": sig["symbol"],
                                "reason": f"边界信号LLM拒绝(score={sc}): {_bl_v.get('reason','')}"})
                except Exception as _ble:
                    logger.debug(f"[scanner] {sig['symbol']} 边界LLM跳过: {_ble}")
                if not _llm_rescued:
                    result["rejected"].append({"symbol": sig["symbol"],
                        "reason": f"工作日低分过滤: score={sc}<{MIN_SIGNAL_SCORE_WEEKDAY}"})
                    logger.debug(f"[scanner] {sig['symbol']} 工作日低分过滤 score={sc}")
                    continue
            _weekday_filtered.append(sig)
        avail_signals = _weekday_filtered

    # ── 周末评分上限（v3.3.4）：strong=60 / medium=70 独立门槛，cap 80 ──
    # strong 信号豁免到 60（防止 BTC 下跌 -5 误杀 strong LONG），medium 保持 70 严格约束
    if _is_weekend:  # v3.29: 已由上方 _is_weekend_period() 统一计算
        _weekend_filtered = []
        for sig in avail_signals:
            sc = sig["ind"].get("final_score", 0)
            _detail = sig.get("judge_detail") or sig.get("reason") or {}
            _strength = _detail.get("signal_strength", "medium")
            # v3.92: 门槛参数化，medium 按策略分别控制
            from config import (WEEKEND_MIN_SCORE_STRONG, WEEKEND_MIN_SCORE_MEDIUM_DEFAULT,
                                WEEKEND_MIN_SCORE_MEDIUM_PULLBACK, WEEKEND_MIN_SCORE_MEDIUM_EMA,
                                WEEKEND_MIN_SCORE_MEDIUM_REVERSAL)
            if _strength == "strong":
                _min_sc = WEEKEND_MIN_SCORE_STRONG
            else:
                _sig_type_w = sig.get("sig_type", "")
                if _sig_type_w == "pullback":
                    _min_sc = WEEKEND_MIN_SCORE_MEDIUM_PULLBACK
                elif _sig_type_w == "ema_trend":
                    _min_sc = WEEKEND_MIN_SCORE_MEDIUM_EMA
                elif _sig_type_w == "reversal":
                    _min_sc = WEEKEND_MIN_SCORE_MEDIUM_REVERSAL
                else:
                    _min_sc = WEEKEND_MIN_SCORE_MEDIUM_DEFAULT
            if sc < _min_sc:
                result["rejected"].append({"symbol": sig["symbol"],
                    "reason": f"周末低分过滤: score={sc}<{_min_sc}({_strength})"})
                continue
            from config import WEEKEND_SCORE_CAP
            if sc > WEEKEND_SCORE_CAP:
                sig["ind"]["final_score"] = WEEKEND_SCORE_CAP
                sig["ind"]["weekend_capped"] = True
            _weekend_filtered.append(sig)
        avail_signals = _weekend_filtered
        logger.debug(f"[scanner] 周末模式：strong≥60/medium≥70 cap 80，过滤后 {len(avail_signals)} 个")

        # ── v3.28 周末 breakout 仅放行 strong 信号 ────────────────────────────────────
        # 周末成交量天然偏低，达标的 breakout 信号往往是「量能刚刚达标的边界信号」，质量天然偏弱
        # strong 信号保留（真正放量突破），medium/weak 周末直接过滤
        _weekend_filtered2 = []
        for sig in avail_signals:
            if sig.get("sig_type") == "breakout":
                _detail2 = sig.get("judge_detail") or sig.get("reason") or {}
                _strength2 = _detail2.get("signal_strength", "medium")
                if _strength2 != "strong":
                    # v3.92 [2-C] medium breakout 有条件放开（量比≥阈值 或 OI 确认）
                    from config import WEEKEND_BREAKOUT_MEDIUM_VOL_MIN, WEEKEND_BREAKOUT_MEDIUM_OI_REQD
                    _bo_vr = float(_detail2.get("volume_ratio", 0) or 0)
                    _bo_oi = bool(_detail2.get("oi_confirm", False))
                    _vol_ok = _bo_vr >= WEEKEND_BREAKOUT_MEDIUM_VOL_MIN
                    _oi_ok  = bool(WEEKEND_BREAKOUT_MEDIUM_OI_REQD) and _bo_oi
                    if _strength2 == "medium" and (_vol_ok or _oi_ok):
                        logger.info(f"[scanner] {sig['symbol']} 周末 breakout medium 有条件放开: "
                                    f"vol_ratio={_bo_vr:.2f} oi_confirm={_bo_oi}")
                        _weekend_filtered2.append(sig)
                        continue
                    result["rejected"].append({
                        "symbol": sig["symbol"],
                        "reason": f"周末 breakout 仅允许 strong 信号，当前={_strength2}"
                    })
                    logger.info(f"[scanner] 周末 breakout 过滤: {sig['symbol']} strength={_strength2}")
                    continue
            _weekend_filtered2.append(sig)
        avail_signals = _weekend_filtered2
        logger.debug(f"[scanner] 周末 breakout medium 过滤后 {len(avail_signals)} 个")

    # ── v3.53 槽位过滤（后移到评分过滤之后）────────────────────────────────────────
    # 只对「分数已达标」的信号消耗槽位，避免低分信号预占槽位导致高分信号被误拦
    # v3.53: long_cnt/short_cnt 已改为只计 open 持仓，pending 挂单不占槽位
    _scored_signals = avail_signals
    avail_signals = []
    _long  = long_cnt
    _short = short_cnt
    for sig in _scored_signals:
        d = sig["direction"]
        if d == "LONG"  and _long  >= _dyn_max_long:
            result["rejected"].append({"symbol": sig["symbol"], "reason": "多单槽位已满"})
            continue
        if d == "SHORT" and _short >= _dyn_max_short:
            result["rejected"].append({"symbol": sig["symbol"], "reason": "空单槽位已满"})
            continue
        avail_signals.append(sig)
        if d == "LONG":  _long  += 1
        else:            _short += 1

    # v3.54: 按 final_score 降序排序（高分优先），同分时再按模式优先级/强度/量比
    avail_signals.sort(key=lambda x: (
        -x["ind"].get("final_score", 0),
        SIGNAL_PRIORITY.get(x["sig_type"], 99),
        SIGNAL_STRENGTH_ORDER.get(
            (x.get("judge_detail") or x.get("reason") or {}).get("signal_strength", "medium"), 1
        ),
        -x["ind"].get("vol_ratio", 0)
    ))

    result["signals_final"] = len(avail_signals)

    # 大户持仓多空比影子记录：评分、槽位筛选都已完成，只写日志（失败不影响信号）
    _log_top_trader_shadow(avail_signals)

    # ── 市场环境快照：注入所有信号（用于 trade_log 标注）─────────────────────
    try:
        # [fix] 复用本轮批量拉好的 _mark_price_map（资金费率均值不需要最新一刻的数据）；批量失败导致 map 为空时才再请求一次
        _all_mp = list(_mark_price_map.values())
        if not _all_mp:
            if source == "trigger":
                # 触发器派发路径：只读 hub 内存快照，绝不发全量 REST（3-9s 慢请求）
                try:
                    from shared import market_data_hub as _hub_fr
                    _all_mp = list(_hub_fr.get_all_mark_prices().values()) if _hub_fr.is_ready() else []
                except Exception:
                    _all_mp = []
            else:
                _all_mp = api.get_all_mark_prices() or []
        _avg_fr = (sum(abs(float(x.get("lastFundingRate", 0))) for x in _all_mp) / len(_all_mp)
                   if _all_mp else 0.0)
    except Exception:
        _avg_fr = 0.0
    # BTC ATR ratio（波动率代理指标）
    try:
        from shared.indicators import atr as _atr_fn
        _btc_k1h   = api.get_klines("BTCUSDT", "1h", 20)
        _btc_atr   = _atr_fn(_btc_k1h)
        _btc_close = float(_btc_k1h[-1][4]) if _btc_k1h else 0.0
        _btc_vol   = round(_btc_atr / _btc_close * 100, 4) if _btc_close > 0 else 0.0
    except Exception:
        _btc_vol = 0.0
    _market_env = {
        "btc_trend":        _btc_trend,           # up/down/neutral
        "market_mode":      _mkt_mode,            # bull/bear/range
        "market_volatility": _btc_vol,            # BTC ATR/price %
        "funding_market":   round(_avg_fr * 100, 6),  # 全市场平均资费绝对值 %
    }
    for _sig in avail_signals:
        _sig["market_env"] = _market_env
        # ── v3.31 大行情恢复窗口：仓低 × 0.5 ───────────────────────────────────
        if _post_shock:
            _old_cap = _sig.get("position_size_cap", 0.5)
            _sig["position_size_cap"] = round(_old_cap * 0.5, 3)
            logger.info(f"[scanner] {_sig['symbol']} post_shock 降仓 cap {_old_cap}→{_sig['position_size_cap']}")
        # ── v3.63 宏观保守模式：仓位 × 0.5（叠加在其他降仓之后）────────────────
        if _macro_conservative:
            _old_cap = _sig.get("position_size_cap", 0.5)
            _sig["position_size_cap"] = round(_old_cap * 0.5, 3)
            logger.debug(f"[scanner] {_sig['symbol']} 宏观保守降仓 cap {_old_cap}→{_sig['position_size_cap']}")
        # 叠加市场情绪仓位系数（v3.38: 读当前 cap，含 post_shock 效果，不读原始值）
        if _sig["direction"].upper() == "LONG" and _global_long_scale < 1.0:
            _cur_cap = _sig.get("position_size_cap", 0.5)  # 已含 post_shock 降仓效果
            _sig["position_size_cap"] = round(_cur_cap * _global_long_scale, 3)
            logger.debug(f"[scanner] {_sig['symbol']} 市场情绪做多降仓 cap {_cur_cap}→{_sig['position_size_cap']}")
        elif _sig["direction"].upper() == "SHORT" and _global_short_scale < 1.0:
            _cur_cap = _sig.get("position_size_cap", 0.5)
            _sig["position_size_cap"] = round(_cur_cap * _global_short_scale, 3)
            logger.debug(f"[scanner] {_sig['symbol']} 市场情绪做空降仓 cap {_cur_cap}→{_sig['position_size_cap']}")
        # ── v3.92 [2-A] 周末仓位最后一乘（必须是本循环最后一次乘系数；强度按当前信号自身取）──
        if _is_weekend:
            from config import WEEKEND_POSITION_SCALE_STRONG, WEEKEND_POSITION_SCALE_MEDIUM
            _w_str = (_sig.get("judge_detail") or _sig.get("reason") or {}).get("signal_strength", "medium")
            _wknd_scale = WEEKEND_POSITION_SCALE_STRONG if _w_str == "strong" else WEEKEND_POSITION_SCALE_MEDIUM
            _old_cap = _sig.get("position_size_cap", 0.5)
            _sig["position_size_cap"] = round(_old_cap * _wknd_scale, 3)
            logger.debug(f"[scanner] {_sig.get('symbol','')} 周末仓位缩减 "
                         f"{_old_cap:.3f}→{_sig['position_size_cap']:.3f} (scale={_wknd_scale}, strength={_w_str})")
            if _sig["position_size_cap"] < 0.15:
                logger.warning(f"[scanner] {_sig.get('symbol','')} 周末仓位上限缩减后为 "
                               f"{_sig['position_size_cap']:.3f}，可能触发 executor MIN_NOTIONAL_GUARD 拉回")

    # ── 第五步：输出给 executor（通过 SignalWatcher 或直接调用）───────────────────
    if source == "trigger" and not dry_run:
        for _ts in avail_signals:
            logger.warning(f"[TRIGGER-DISPATCH] 触发器信号即将下单 symbol={_ts.get('symbol')} "
                           f"strategy={_ts.get('sig_type')} direction={_ts.get('direction')}")
    # ── v3.92 周末新开仓上限（按注册计）+ 周末标记 ────────────────────────
    if _is_weekend and avail_signals:
        from config import WEEKEND_MAX_NEW_POSITIONS
        _wk_left = max(0, int(WEEKEND_MAX_NEW_POSITIONS) - _WEEKEND_POS_COUNT)
        _wk_keep = []
        for sig in avail_signals:
            if len(_wk_keep) >= _wk_left:
                logger.warning(f"[scanner] 周末开仓已达上限 {WEEKEND_MAX_NEW_POSITIONS} 笔，跳过 {sig['symbol']}")
                result["rejected"].append({"symbol": sig["symbol"],
                    "reason": f"周末开仓上限 {WEEKEND_MAX_NEW_POSITIONS} 笔已满"})
                continue
            sig["weekend_mode"] = True
            _wk_keep.append(sig)
        avail_signals = _wk_keep

    if not dry_run and avail_signals:
        if signal_watcher is not None:
            # 新路径：注册到 SignalWatcher，由实时价格驱动触发
            import asyncio as _asyncio
            from shared.signal_watcher import compute_entry_range

            # ── 第一阶段（子线程）：耗时 IO 操作，预算每个信号的 entry_price ───────
            import fx.executor as _executor_mod
            from shared.calc_sl_tp import calc_sl_tp as _calc_sl_tp
            from shared.liq_cache import get_liq_zones_cached as _liq_zones
            from shared.indicators import atr as _calc_atr, ema as _calc_ema

            prepared = []   # [(sig, entry_low, entry_high, expire_ts, ws_ref)]
            _snap_by_sym = {}   # [v3.87 STEP2-2] 按标的保存触发快照，避免注册阶段读到上一个标的的残留变量
            _ws_ref = getattr(signal_watcher, "_ws_listener", None)

            for sig in avail_signals:
                sym  = sig["symbol"]
                mode = sig.get("sig_type", "pullback")
                sig["mode"] = mode
                try:
                    # [fix] 优先复用本轮批量标记价格；但这里的 markPrice 会成为 pre_entry_price（入场价基准），
                    #   批量数据取自扫描开始时，超过 _MARK_MAP_MAX_AGE_SEC 就视为过旧，改走单标的 REST 取最新价
                    mp = None
                    if time.monotonic() - _mark_price_map_ts <= _MARK_MAP_MAX_AGE_SEC:
                        mp = _mark_price_map.get(sym)
                    mp     = mp or api.get_mark_price(sym)
                    mark_p = float(mp.get("markPrice", 0))
                    sig["pre_entry_price"] = mark_p

                    if mode == "breakout":
                        from config import EXPIRE_BREAKOUT_MIN
                        expire_sec = EXPIRE_BREAKOUT_MIN * 60
                    else:
                        # 按 ATR ratio 动态调整：低波动等更久，高波动快速失效
                        from config import EXPIRE_LOW_VOL_H, EXPIRE_HIGH_VOL_H, EXPIRE_NORMAL_H, EXPIRE_LOW_VOL_THRESH, EXPIRE_HIGH_VOL_THRESH
                        _atr_r = sig.get("ind", {}).get("atr_ratio", 3.0)
                        if _atr_r < EXPIRE_LOW_VOL_THRESH:
                            expire_sec = EXPIRE_LOW_VOL_H * 3600
                            if _is_weekend:   # v3.92 周末低波动档缩短
                                from config import WEEKEND_EXPIRE_LOW_VOL_H
                                expire_sec = WEEKEND_EXPIRE_LOW_VOL_H * 3600
                        elif _atr_r > EXPIRE_HIGH_VOL_THRESH:
                            expire_sec = EXPIRE_HIGH_VOL_H * 3600
                        else:
                            expire_sec = EXPIRE_NORMAL_H * 3600
                    expire_ts  = time.time() + expire_sec
                    # v3.92 周日信号到期时间钳制（breakout 不参与）
                    if mode != "breakout" and _is_weekend and now.isoweekday() == 7:
                        from config import WEEKEND_SIGNAL_EXPIRE_CUTOFF_HOUR
                        _cut = now.replace(hour=WEEKEND_SIGNAL_EXPIRE_CUTOFF_HOUR, minute=0,
                                           second=0, microsecond=0).timestamp()
                        expire_ts = min(expire_ts, _cut)

                    # 预算 entry_price（耗时 IO，在子线程做）
                    try:
                        k1h    = api.get_klines(sym, "1h", 50)
                        closed = k1h[:-1] if len(k1h) > 1 else k1h
                        closes = [float(k[4]) for k in closed]
                        atr_v  = _calc_atr(closed)
                        ema20  = _calc_ema(closes, 20)
                        # [v3.87 STEP2-2] 在每个标的自己的循环里保存快照（RSI1H/ATR/EMA20）
                        try:
                            from shared.indicators import rsi as _calc_rsi_snap
                            _snap_by_sym[sym] = {
                                "rsi": _calc_rsi_snap(closes) if len(closes) > 14 else 50.0,
                                "atr": atr_v, "ema20": ema20,
                            }
                        except Exception:
                            _snap_by_sym[sym] = {"rsi": 50.0, "atr": atr_v, "ema20": ema20}
                        _range = "24h" if mode in ("reversal","funding_arb") else                                  "7d"  if mode == "breakout" else "3d"
                        params = _calc_sl_tp(
                            signal=sig, mark_price=mark_p, atr=atr_v,
                            liq_zones={"long":  _liq_zones(sym,"LONG", mark_p, range_="7d"),  # [v3.95] 统一用7d
                                       "short": _liq_zones(sym,"SHORT",mark_p, range_="7d")},
                            k1h=k1h, ema20=ema20,
                        )
                        if "error" not in params:
                            sig["pre_entry_price"] = params["entry_price"]
                            sig["pre_params"] = params
                            sig["_entry_params_ok"] = True
                        else:
                            sig["_entry_params_ok"] = False
                            sig["_calc_sl_tp_error"] = params.get("error", "")
                            logger.debug(
                                f"[scanner] {sym} calc_sl_tp 拒绝: {params.get('error','')}"
                            )
                        # [add] 追记 entry_params_ok 到 signal_analytics（patch 行， type=entry_params_patch）
                        try:
                            import json as _json
                            _patch = {
                                "type":            "entry_params_patch",
                                "ts":              int(__import__('time').time() * 1000),
                                "symbol":          sym,
                                "mode":            mode,
                                "direction":       sig.get("direction", ""),
                                "entry_params_ok": sig["_entry_params_ok"],
                                "calc_sl_tp_error": sig.get("_calc_sl_tp_error", None),
                                "final_score": (sig.get("ind") or {}).get("final_score", sig.get("final_score")),
                                "final_score_stage": "final",
                            }
                            if sig["_entry_params_ok"]:
                                # 入场/止损/止盈价：track_outcomes 事后追踪必须有这些价格才能判断是否成交与出场
                                _patch["entry_params"] = {
                                    k: params.get(k) for k in
                                    ("entry_price", "sl_price", "tp1_price", "tp2_price", "sl_pct", "rr_tp1",
                                     "sl_degraded", "entry_mode")
                                }
                            with open(SIGNAL_ANALYTICS_FILE, "a", encoding="utf-8") as _af:
                                _af.write(_json.dumps(_patch, ensure_ascii=False) + "\n")
                        except Exception as _wp:
                            logger.debug(f"[scanner] {sym} entry_params_patch 写入失败: {_wp}")
                    except Exception as _pe:
                        logger.debug(f"[scanner] {sym} 预算 entry_price 失败，用 mark_price: {_pe}")

                    # breakout: 传入 liq_zones 供清算区入场点计算
                    _liq_l = _liq_r = None
                    if mode == "breakout":
                        try:
                            _liq_l = _liq_zones(sym, "LONG",  mark_p, range_="7d")
                            _liq_r = _liq_zones(sym, "SHORT", mark_p, range_="7d")
                        except Exception:
                            pass  # 失败降级，compute_entry_range 内部处理

                    entry_low, entry_high, entry_p = compute_entry_range(
                        sig, mark_p, liq_long=_liq_l, liq_short=_liq_r
                    )

                    # ── 回踩距离检查：若扫描时价格已在触发区间内，不等回踩直接进
                    # 这说明信号是"贴着EMA20横走"而非"涨上去再回踩"
                    # 处理：把区间上限收窄到"当前价以下"，强制等价格先离开再回踩
                    # 等价于：只有价格先涨出 entry_high，然后回踩进入 [lo, mark_p*0.999] 才触发
                    mode_sig = sig.get("sig_type", "pullback")
                    direction_sig = sig.get("direction", "LONG").upper()
                    if mode_sig == "pullback" and direction_sig == "LONG":
                        if entry_low <= mark_p <= entry_high:
                            # 当前价就在区间内——把上限收窄到"低于当前价 0.5%"
                            # 让 signal_watcher 等价格先涨出去，再回踩才触发
                            adjusted_high = mark_p * 0.995
                            if adjusted_high > entry_low:
                                logger.info(
                                    f"[scanner] {sym} pullback: 扫描时价格已在触发区间内"
                                    f"({mark_p:.4g} in [{entry_low:.4g},{entry_high:.4g}])"
                                    f"，收窄上限到 {adjusted_high:.4g}，等回踩"
                                )
                                entry_high = adjusted_high
                    elif mode_sig == "pullback" and direction_sig == "SHORT":
                        if entry_low <= mark_p <= entry_high:
                            adjusted_low = mark_p * 1.005
                            if adjusted_low < entry_high:
                                logger.info(
                                    f"[scanner] {sym} pullback SHORT: 扫描时价格已在触发区间内"
                                    f"，收窄下限到 {adjusted_low:.4g}，等反弹"
                                )
                                entry_low = adjusted_low

                    prepared.append((sig, entry_low, entry_high, entry_p, expire_ts))
                except Exception as e:
                    logger.warning(f"[scanner] {sym} 预算失败: {e}")

            # ── 第二阶段（主 loop）：add_signal 是纯内存操作，极快 ─────────────────
            try:
                from config import MAX_PENDING_LONG as _MPL, MAX_PENDING_SHORT as _MPS
            except ImportError:
                _MPL = _MPS = 5
            _MAX_PENDING_PER_DIR = max(_MPL, _MPS)   # v3.54: 每方向最多同时挂 pending 单（读 config，原硬编码 5）

            async def _do_register():
                for sig, lo, hi, ep, exp in prepared:
                    sym        = sig["symbol"]
                    entry_p    = ep   # 限价单挂单价（来自 compute_entry_range）
                    _sig_dir   = sig.get("direction", "LONG").upper()
                    _sig_score = sig.get("ind", {}).get("final_score", 0)

                    # ── v3.54: 每方向 pending 上限检查，超出则与最低分已挂单比较替换 ──
                    async with signal_watcher._lock:
                        _dir_pending = [
                            (s, e) for s, e in signal_watcher._watched.items()
                            if not e.triggered and not e.is_expired()
                            and (e.signal or {}).get("direction", "").upper() == _sig_dir
                        ]
                    if len(_dir_pending) >= _MAX_PENDING_PER_DIR:
                        # 找分最低的已挂单
                        _min_sym, _min_entry = min(
                            _dir_pending,
                            key=lambda x: (x[1].signal or {}).get("ind", {}).get("final_score", 0)
                        )
                        _min_score = (_min_entry.signal or {}).get("ind", {}).get("final_score", 0)
                        if _sig_score <= _min_score:
                            logger.info(
                                f"[scanner] {sym} 新信号分({_sig_score}) ≤ 已有最低分({_min_score})，"
                                f"{_sig_dir} 挂单已满{_MAX_PENDING_PER_DIR}个，跳过"
                            )
                            continue
                        # 新信号分更高，撤掉最低分的再挂新的
                        logger.info(
                            f"[scanner] {sym} 替换 {_min_sym}: 新分{_sig_score} > 旧分{_min_score}，"
                            f"撤旧挂新"
                        )
                        # [fix #32] 经 signal_watcher 公开接口撤单并移除；撤单失败/结果未知/已成交时条目保留，
                        #   不占用该槽位，本次新信号跳过（下一轮扫描再比较）。
                        _removed = await signal_watcher.cancel_signal(_min_sym, "高分信号替换低分挂单")
                        if not _removed:
                            logger.warning(f"[scanner] 撤低分挂单 {_min_sym} 未确认，保留条目，{sym} 本轮跳过")
                            continue

                    async def _on_trigger(signal, _sw=signal_watcher):
                        """
                        价格进入触发区间时调用。
                        实时获取最新的 _ws_listener（防止断线重连后引用旧实例）。
                        直接调 execute_signal，让 executor 以 entry_price（EMA20）下限价单。
                        signal 里已有 pre_entry_price / pre_params，executor 会优先复用。
                        """
                        try:
                            _ws_live = getattr(_sw, "_ws_listener", None)
                            ok = await _executor_mod.execute_signal(signal, _ws_live)
                            if ok:
                                logger.info(f"[scanner] {signal['symbol']} 触发下单成功")
                        except Exception as _e:
                            logger.error(f"[scanner] {signal['symbol']} 触发下单失败: {_e}")

                    # ── v3.67/v3.72 计算触发快照 + 竞争评分字段 ─────────────────
                    _trigger_price = mark_p
                    _trigger_rsi_1h = 50.0
                    _breakout_ref = 0.0
                    _ema20_at_trigger = 0.0
                    _atr_at_trigger = 0.0
                    # [v3.87 STEP2-2] 原写法用 'k1h' in dir() 判断闭包变量：实际读到的是预算循环里「最后一个标的」的 k1h/atr_v/ema20，
                    #   且 breakout 判断用的 mode 也是残留值，导致所有信号的触发快照（RSI/ATR/EMA20/突破基准）都是别的币的数据。
                    #   新：按标的从 _snap_by_sym 取值，mode 取自信号自身。已实测确认原写法读到残留值。
                    try:
                        _snap = _snap_by_sym.get(sym, {})
                        if _snap:
                            _trigger_rsi_1h   = _snap["rsi"]
                            _atr_at_trigger   = _snap["atr"]
                            _ema20_at_trigger = _snap["ema20"]
                        _reason_dict = sig.get('reason', {}) or {}
                        _dir = sig.get('direction', 'LONG').upper()
                        if sig.get('sig_type') == 'breakout':
                            _breakout_ref = _reason_dict.get('breakout_ref_high' if _dir == 'LONG' else 'breakout_ref_low', 0)
                    except Exception as _snap_e:
                        logger.debug(f"[scanner] {sym} 触发快照字段计算失败: {_snap_e}")

                    await signal_watcher.add_signal(
                        signal=sig,
                        entry_low=lo,
                        entry_high=hi,
                        entry_price=entry_p,
                        expire_ts=exp,
                        on_trigger=_on_trigger,
                        # v3.67 触发快照字段
                        trigger_price=_trigger_price,
                        trigger_rsi_1h=_trigger_rsi_1h,
                        breakout_ref=_breakout_ref,
                        ema20_at_trigger=_ema20_at_trigger,
                        atr_at_trigger=_atr_at_trigger,
                        # v3.72 竞争评分字段（score_created_at/expire 在 add_signal 里设默认值）
                        score_created_at=time.time(),
                        score_original_expire=exp,
                    )
                    result["placed"] += 1
                    if sig.get("weekend_mode"):
                        sig["weekend_pos_seq"] = _weekend_counter_inc()
                    if _ws_ref is not None:
                        _ws_ref.subscribe(sym)

            try:
                main_loop = getattr(signal_watcher, "_main_loop", None)
                if main_loop and main_loop.is_running():
                    fut = _asyncio.run_coroutine_threadsafe(_do_register(), main_loop)
                    try:
                        fut.result(timeout=15)  # add_signal 是内存写，正常 <1s。如果 _trigger 持锁超过 15s 才会超时
                    except Exception as _timeout_err:
                        _batch_syms = [s["symbol"] for s, *_ in prepared]
                        logger.warning(
                            f"[scanner] SignalWatcher.add_signal 超时或异常，本轮信号批次注册失败"
                            f" (batch={_batch_syms}): {_timeout_err}"
                        )
                else:
                    _asyncio.run(_do_register())
            except Exception as _reg_err:
                logger.warning(f"[scanner] SignalWatcher 注册失败: {_reg_err}")
        else:
            # 兼容老路径：signal_watcher=None 时直接同步执行
            import executor
            # scanner 在 run_in_executor 子线程里运行，使用同步包装器
            for sig in avail_signals:
                try:
                    ok = executor.execute_signal_sync(sig, ws_listener=None)
                    if ok:
                        result["placed"] += 1
                        if sig.get("weekend_mode"):
                            sig["weekend_pos_seq"] = _weekend_counter_inc()
                except Exception as e:
                    logger.warning(f"[scanner] {sig['symbol']} 开仓失败: {e}")

    return result.get("placed", 0) - _placed0, avail_signals


# ── 阶段5：触发器复用的扫描上下文缓存（每轮扫描第四步前写入）────────────────────────
_TRIGGER_CTX: dict = {"ts": 0.0, "budget_info": None, "active_syms": set(), "pending_syms": set()}
_TRIGGER_CTX_LOCK = _threading.Lock()


def _set_trigger_context(budget_info: dict, active_syms, pending_syms) -> None:
    with _TRIGGER_CTX_LOCK:
        _TRIGGER_CTX.update(ts=time.time(), budget_info=dict(budget_info),
                            active_syms=set(active_syms or ()), pending_syms=set(pending_syms or ()))


def dispatch_trigger_analysis(symbol: str, ticker: dict, mark: dict | None, klines: tuple,
                              signal_watcher=None, dry_run: bool = False, max_ctx_age: float = 1800.0) -> dict:
    """阶段5 触发器入口（在线程里运行）：hub 快照 → _prefetch_symbol(klines) → analyze_one → 第四步共用函数。
    返回 {"analysis_result": "signal"/"rejected"/"error", "signal_strategy", "registered", "error"}。"""
    out = {"analysis_result": "rejected", "signal_strategy": None, "registered": 0, "error": None}
    ctx = get_trigger_context()
    if not ctx["budget_info"]:
        out.update(analysis_result="error", error="no_context")
        return out
    if time.time() - ctx["ts"] > max_ctx_age:
        out.update(analysis_result="error", error="stale_context")
        return out
    bi = ctx["budget_info"]
    positions = get_all_positions()
    bi["long_cnt"] = sum(1 for p in positions if p.get("direction") == "LONG" and p.get("status") == "open")
    bi["short_cnt"] = sum(1 for p in positions if p.get("direction") == "SHORT" and p.get("status") == "open")
    active_syms = {(p["symbol"], p.get("direction", "").upper()) for p in positions if p.get("status") == "open"}
    pending_syms = {p["symbol"] for p in positions if p.get("status") in ("pending", "patching", "pending_guard")}
    mpm = {symbol: mark} if mark else {}
    bi["_mark_price_map"] = mpm
    bi["_mark_price_map_ts"] = time.monotonic()
    b = _prefetch_symbol(symbol, ticker, bi.get("_loss_blacklist"), None, mpm, klines=klines)
    if b is None:
        return out
    events: list = []
    sig = analyze_one(symbol, ticker, b["k15"], b["k1h"], b["k4h"], active_syms, pending_syms,
                      bi.get("_allowed_dirs"), bi.get("_loss_blacklist"), bi["_btc_trend"], bi["_mkt_mode"],
                      mpm, b["prefetch"], events)
    sig = _apply_post_events(sig, events)
    if not sig:
        return out
    result = {"signals_final": 0, "placed": 0, "rejected": []}
    n, avail = _filter_and_register_signals([sig], active_syms, pending_syms, bi["_mkt_mode"], bi["_btc_trend"],
                                            bi, "trigger", signal_watcher=signal_watcher, dry_run=dry_run,
                                            result=result)
    if avail:
        out.update(analysis_result="signal", signal_strategy=avail[0].get("sig_type"), registered=n)
    return out


def get_trigger_context() -> dict:
    """返回 {"ts", "budget_info", "active_syms", "pending_syms"} 的拷贝；首轮扫描前 budget_info 为 None。"""
    with _TRIGGER_CTX_LOCK:
        return {"ts": _TRIGGER_CTX["ts"],
                "budget_info": dict(_TRIGGER_CTX["budget_info"]) if _TRIGGER_CTX["budget_info"] else None,
                "active_syms": set(_TRIGGER_CTX["active_syms"]), "pending_syms": set(_TRIGGER_CTX["pending_syms"])}


def run_scan(dry_run: bool = False, signal_watcher=None) -> dict:
    t0  = time.time()
    now = datetime.now(timezone.utc)
    # [v3.89 SHADOW] 每轮扫描重置影子记录计数
    try:
        from shared import shadow_trace as _st0
        _st0.new_round()
    except Exception:
        pass
    result = {
        "ts": now.isoformat(), "scanned": 0,
        "class_counts": {"reversal": 0, "breakout": 0, "pullback": 0},
        "signals_raw": 0, "signals_final": 0, "placed": 0,
        "rejected": [], "dry_run": dry_run,
    }

    # ── 第一步：前置状态检查 ───────────────────────────────────────────────────
    if is_circuit_open():
        logger.info("[scanner] 日交易熔断中，跳过扫描")
        return result

    # ── 宏观日历检查（v3.63）────────────────────────────────────────────────
    # 读取 macro_calendar.json，判断当前是否在重大宏观事件的保守窗口内。
    # 保守模式：仓位上限减半 + 停止 reversal/breakout + 评分门槛 +10
    # 日历由用户手动维护，系统自动读取，无需改代码。
    _macro_conservative = False   # 是否进入宏观保守模式
    _macro_event_desc   = ""      # 事件描述，用于日志
    try:
        import json as _mjson
        from pathlib import Path as _mPath
        _cal_file = _mPath(__file__).parent / "macro_calendar.json"
        if _cal_file.exists():
            _cal = _mjson.loads(_cal_file.read_text(encoding="utf-8"))
            _now_utc = datetime.now(timezone.utc)
            for _ev in _cal.get("events", []):
                if _ev.get("_example"):
                    continue   # 跳过示例条目
                _ev_date = _ev.get("date", "")
                if not _ev_date:
                    continue
                try:
                    _ev_dt = datetime.strptime(_ev_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                _hours = int(_ev.get("conservative_hours", 24))
                # 窗口设计：事件当天 UTC 00:00 开始，持续 conservative_hours 小时
                # conservative_hours=24 → 覆盖整个事件日（UTC 全天）
                # conservative_hours=48 → 覆盖事件日 + 次日（跨日事件或影响持续）
                # conservative_hours=0  → 等同于 24（全天）
                _win_hours  = _hours if _hours > 0 else 24
                _win_start  = _ev_dt   # 当天 UTC 00:00
                _win_end    = _ev_dt + __import__("datetime").timedelta(hours=_win_hours)
                if _win_start <= _now_utc <= _win_end:
                    _macro_conservative = True
                    _macro_event_desc   = _ev.get("event", _ev_date)
                    break
    except Exception as _me:
        logger.debug(f"[scanner] 宏观日历读取跳过: {_me}")

    if _macro_conservative:
        logger.info(f"[scanner] ⚠️ 宏观保守模式激活：{_macro_event_desc}（仓位减半/停reversal+breakout/评分+10）")

    # 功能一：大行情熔断检测（v3.1）
    try:
        from shared.notifier import send as _notify_fn
    except Exception:
        _notify_fn = None
    if check_market_shock(notifier_fn=_notify_fn):
        logger.info("[scanner] 大行情熔断激活，跳过本轮扫描")
        return result

    # ── 系统性风险检测（USDT脱锚 / API异常 / 极端费率）──────────────────────
    try:
        from shared.risk_manager import check_systemic_risk
        _sys_ok, _sys_reason = check_systemic_risk()
        if not _sys_ok:
            logger.warning(f"[scanner] 系统性风险检测触发：{_sys_reason}，本轮跳过开仓")
            try:
                from shared.notifier import notify as _sn
                _sn(f"🚨 系统性风险：{_sys_reason}")
            except Exception:
                pass
            return result
    except Exception as _sre:
        logger.debug(f"[scanner] check_systemic_risk 跳过: {_sre}")

    # ── v3.31 大行情恢复窗口检测 ──────────────────────────────────────────────
    # 熔断解除后10分钟内（约2轮扫描），ATR/EMA/RSI 处于失真状态，信号质量偏差。
    # 此期间：position_size_cap × 0.5，final_score -10（在评分层应用）
    _post_shock = is_post_shock_recovery()
    if _post_shock:
        logger.info("[scanner] 大行情恢复窗口：本轮信号将降级处理（仓位×0.5 / 评分-10）")

    # 问题一修复：_dyn_max_long/_dyn_max_short 先用静态默认值，
    # 等 BTC 趋势计算完成后再动态更新。避免 BTC 拉取失败时 NameError。
    import config as _cfg_early
    _dyn_max_long  = _cfg_early.RISK_MAX_LONG
    _dyn_max_short = _cfg_early.RISK_MAX_SHORT

    positions = get_all_positions()
    # v3.53: 槽位只计 open（已成交持仓），pending 挂单不占槽位
    # 原逻辑把 pending 也算槽位，导致挂单中的标的把槽位占满、后续高分信号全被拒
    long_cnt  = sum(1 for p in positions if p.get("direction") == "LONG"
                    and p.get("status") == "open")
    short_cnt = sum(1 for p in positions if p.get("direction") == "SHORT"
                    and p.get("status") == "open")

    if long_cnt >= _dyn_max_long and short_cnt >= _dyn_max_short:
        logger.info(f"[scanner] 持仓已满（多{long_cnt} 空{short_cnt}），跳过扫描")
        # 仓位满时撤销所有 SignalWatcher 里的入场挂单（避免持仓超限）
        if signal_watcher is not None and signal_watcher.pending_count() > 0:
            import asyncio as _asw
            main_loop = getattr(signal_watcher, "_main_loop", None)
            if main_loop and main_loop.is_running():
                _f = _asw.run_coroutine_threadsafe(
                    signal_watcher.cancel_all("仓位已满"),
                    main_loop,
                )
                try:
                    n = _f.result(timeout=5)
                    if n:
                        logger.info(f"[scanner] 仓位满，已撤销 {n} 个入场监控/挂单")
                except Exception:
                    pass
        return result

    # 全局资金费率结算窗口检查（用 BTC 资金费率代表市场费率水平）
    btc_mp   = api.get_mark_price("BTCUSDT")
    btc_fr   = abs(float(btc_mp.get("lastFundingRate", 0)))
    in_win, win_reason = _in_funding_window(btc_fr)
    # [v3.85 OPT-6] 启用逐标的窗口后，不再因 BTC 一个值整轮停扫；8H 标的仍会在 analyze_symbol 里被逐个拦截
    try:
        from config import OPT_FUNDING_WINDOW_PER_SYMBOL as _OPT_FW_G
    except Exception:
        _OPT_FW_G = False
    if in_win and _OPT_FW_G:
        logger.info(f"[scanner] {win_reason}：已启用逐标的窗口，不整轮跳过")
        in_win = False
    if in_win:
        logger.info(f"[scanner] {win_reason}，跳过扫描")
        return result

    # ── 第二步完成后：获取市场模式（统一调用 risk_manager.get_market_mode）────
    # v3.6 修订：原内联 EMA9/20/50 排列判断改为复用 risk_manager.get_market_mode()，
    # 确保 scanner 槽位和 executor._precheck 用同一套 BTC 趋势判断，消除两套逻辑不一致。
    import config as _cfg
    try:
        from shared.risk_manager import get_market_mode as _get_mkt_mode
        _mkt_mode = _get_mkt_mode()   # "bull" / "bear" / "range"，自带 5min 缓存
    except Exception as _be:
        _mkt_mode = "range"
        logger.debug(f"[scanner] get_market_mode 失败，降级 range: {_be}")

    # 持久化 market_mode 到文件（供自检脚本读取）
    try:
        import time as _mt
        _mm_path = ROOT / "logs" / "market_mode.json"
        _mm_path.write_text(
            __import__("json").dumps({"mode": _mkt_mode, "ts": int(_mt.time() * 1000)}),
            encoding="utf-8"
        )
    except Exception:
        pass

    # ── v3.5 市场方向感知：根据市场模式动态调整多空单槽位──────────────────────
    if _mkt_mode == "bear":
        # 熊市：多2空3
        _effective_max_long  = max(1, _cfg.MAX_POSITIONS_LONG_BEAR)
        _effective_max_short = _cfg.MAX_POSITIONS_SHORT_BEAR
        logger.info(f"[scanner] 熊市模式：多单上限{_effective_max_long} 空单上限{_effective_max_short}")
    elif _mkt_mode == "bull":
        # 牛市：多3空2
        _effective_max_long  = _cfg.MAX_POSITIONS_LONG_BULL
        _effective_max_short = _cfg.MAX_POSITIONS_SHORT_BULL
        logger.info(f"[scanner] 牛市模式：多单上限{_effective_max_long} 空单上限{_effective_max_short}")
    else:
        # 震荡/range：多2空2
        _effective_max_long  = _cfg.MAX_POSITIONS_LONG_RANGE
        _effective_max_short = _cfg.MAX_POSITIONS_SHORT_RANGE
        logger.info(f"[scanner] 震荡模式：多单上限{_effective_max_long} 空单上限{_effective_max_short}")
    # 实际槽位使用市场感知后的值
    _dyn_max_long  = _effective_max_long
    _dyn_max_short = _effective_max_short
    # 兼容后续代码使用 _btc_trend（"up"/"down"/"neutral"）
    _btc_trend = {"bull": "up", "bear": "down"}.get(_mkt_mode, "neutral")
    _update_btc_1h_drop()

    # ── 第二步：读取候选池 ────────────────────────────────────────────────────
    _perf_t = time.perf_counter()
    pool = _load_pool()
    _perf_pool = time.perf_counter() - _perf_t
    if not pool:
        logger.warning("[scanner] 候选池为空")
        return result

    # 当前持仓/挂单标的集合（用于方向冲突和挂单维护判断）
    # active_syms: (symbol, direction) 二元组集合，只排除同向，允许反向信号进入 judge
    active_syms  = {(p["symbol"], p.get("direction","").upper())
                    for p in positions if p.get("status") == "open"}
    # 兼容：active_sym_set 纯标的集合，用于 _patrol_funding_arb 和 scan_pool 过滤
    active_sym_set = {p["symbol"] for p in positions if p.get("status") == "open"}
    # patching 也排除：正在补挂止损止盈，不允许再次开仓
    pending_syms = {p["symbol"] for p in positions
                   if p.get("status") in ("pending", "patching", "pending_guard")}

    # 对已有挂单的标的执行维护
    pending_positions = [p for p in positions if p.get("status") == "pending"]
    for pos in pending_positions:
        sym = pos["symbol"]
        if sym not in pool:
            continue
        pool_ticker = {}  # maintain_pending_order 内部实际未使用 ticker，传空字典即可
        action = maintain_pending_order(pos, pool_ticker)
        if action in ("cancel", "reorder"):
            try:
                # 撤销该标的所有挂单（cancel_all_orders 返回撤销数量）
                cancelled = api.cancel_all_orders(sym)
                logger.info(f"[scanner] {sym} 撤单 {cancelled} 笔")
                if action == "reorder":
                    # reorder 逻辑：撤单后让下一轮扫描重新评估开仓
                    # 不立刻重挂，避免在不合适的价格补挂
                    logger.info(f"[scanner] {sym} reorder: 已撤单，等待下轮扫描重新评估")
            except Exception as e:
                logger.warning(f"[scanner] {sym} 撤单失败: {e}")

    # ── 实时注入：涨跌幅超过10%但不在候选池的标的 ──────────────────────────────
    # 使用已有的 ticker 缓存（不重新拉取），限时3秒，注入本轮不写回缓存
    try:
        inject_start = time.time()
        ticker_all = _get_ticker_24h_cached(300) or {}   # 5分钟内的缓存
        pool_set = set(pool)
        injected = []
        for sym2, tk in ticker_all.items():
            if time.time() - inject_start > 3:   # 超过3秒强制停止
                break
            if sym2 in pool_set:
                continue
            chg2 = float(tk.get("priceChangePercent", 0))
            vol2 = float(tk.get("quoteVolume", 0))
            if abs(chg2) >= 10 and vol2 >= POOL_MIN_VOLUME_USD:
                injected.append(sym2)
                pool_set.add(sym2)
        if injected:
            pool = pool + injected   # 只影响本轮，不写回缓存
            logger.info(f"[scanner] 实时注入 {len(injected)} 个大幅波动标的: {injected[:5]}{'...' if len(injected)>5 else ''}")
    except Exception as inj_err:
        logger.debug(f"[scanner] 实时注入跳过: {inj_err}")

    # v3.53 竞争开仓：pending 挂单不排除出候选池（允许同时给多个候选挂单）
    # 同标的同方向冲突由 analyze_symbol 内部的 active_syms 精细过滤
    # pending_syms 仍用于 analyze_symbol 内部判断，不在此处整体排除
    scan_pool = [s for s in pool if s not in active_sym_set]   # 只排除 open 持仓标的
    # 股票类代币交易时段过滤：非交易时段不进入本轮开仓扫描（只影响候选池，持仓维护不经过这里）
    _stock_skipped = []
    _tradeable_pool = []
    for _s in scan_pool:
        if _is_stock_token_tradeable(_s):
            _tradeable_pool.append(_s)
        else:
            _stock_skipped.append(_s)
            logger.debug(f"[scanner] {_s} 非交易时段，跳过")
    if _stock_skipped:
        logger.info(f"[scanner] 股票类代币非交易时段，本轮跳过 {len(_stock_skipped)} 个")
    scan_pool = _tradeable_pool
    result["scanned"] = len(scan_pool)
    # 持仓标的和 BTCUSDT 也订阅（patrol / 挂单维护读它们的 K 线），放最前：hub 对重复标的去重，超出 KLINE_HUB_SYMBOLS_MAX 时按顺序截断
    _hub_subscribe_klines(["BTCUSDT"] + sorted(active_sym_set) + list(scan_pool))

    # 方向约束：对应方向槽位已满时只扫描另一个方向（v3.53: 基于 open-only 计数）
    allowed_dirs: set | None = None
    if long_cnt >= _dyn_max_long and short_cnt < _dyn_max_short:
        allowed_dirs = {"SHORT"}
        logger.info(f"[scanner] 多单已满({long_cnt}/{_dyn_max_long})，本轮仅扫描空单信号")
    elif short_cnt >= _dyn_max_short and long_cnt < _dyn_max_long:
        allowed_dirs = {"LONG"}
        logger.info(f"[scanner] 空单已满({short_cnt}/{_dyn_max_short})，本轮仅扫描多单信号")

    # ── 累计亏损黑名单（提前到并行扫描前计算，作为零成本预筛传给 analyze_symbol）─
    # 同一标的近期累计亏损 > 15U 时，跳过该标的（避免反复踩雷）
    _loss_blacklist: set[str] = set()
    try:
        import json as _json
        _LOSS_THRESHOLD = 15.0
        from pathlib import Path as _Path
        _log_dir = _Path(__file__).parent / "logs"
        _trade_log_files = [str(_log_dir / "trade_log.jsonl")]
        import glob as _glob
        _trade_log_files += _glob.glob(str(_log_dir / "archive" / "trade_log_*.jsonl"))
        _sym_loss: dict[str, float] = {}
        for _tf in _trade_log_files:
            try:
                with open(_tf, encoding="utf-8") as _fh:
                    for _line in _fh:
                        _line = _line.strip()
                        if not _line:
                            continue
                        _rec = _json.loads(_line)
                        import time as _bk_tm
                        _cutoff_ms = (_bk_tm.time() - 7 * 86400) * 1000
                        _ct = _rec.get("close_time", _rec.get("open_time", 0))
                        if _ct and float(_ct) < _cutoff_ms:
                            continue
                        _s   = _rec.get("symbol", "")
                        _pnl = float(_rec.get("pnl_usdt", _rec.get("pnl_u", 0)) or 0)
                        if _pnl < 0:
                            _sym_loss[_s] = _sym_loss.get(_s, 0) + abs(_pnl)
            except Exception:
                pass
        for _s, _loss in _sym_loss.items():
            if _loss >= _LOSS_THRESHOLD:
                _loss_blacklist.add(_s)
    except Exception:
        pass
    if _loss_blacklist:
        logger.info(f"[scanner] 累计亏损黑名单（预筛）: {_loss_blacklist}")

        # ── 第三步：并行分析（单标的 SCAN_SYMBOL_TIMEOUT_SEC，整轮 SCAN_TOTAL_TIMEOUT_SEC）─
    # [fix] 全市场标记价格/资金费率批量拉一次（1 次请求），供所有并行任务复用；失败则为空 map，各标的降级走 REST
    _mark_price_map: dict = {}
    _perf_t = time.perf_counter()
    try:
        for _m in (_get_all_mark_prices_list() or []):
            _ms = _m.get("symbol", "")
            if _ms:
                _mark_price_map[_ms] = _m
    except Exception as _mpe:
        logger.warning(f"[scanner] 批量标记价格获取失败，各标的降级单独请求: {_mpe}")
    _perf_mark = time.perf_counter() - _perf_t
    _mark_price_map_ts = time.monotonic()   # 批量数据的取数时间；注册入场价时用它判断是否仍足够新
    # 拉取一次 ticker 数据供所有并行任务复用（修复 vol24_usd 永远为 0 的 bug）
    try:
        _ticker_all = _get_ticker_24h_cached(120) or {}
    except Exception as _te:
        logger.warning(f"[scanner] ticker 缓存获取失败，vol24 将退化为 0: {_te}")
        _ticker_all = {}
    # ── #2 市场情绪快照：全市场下跌比例 ──────────────────────────────────────
    from config import MARKET_SENTIMENT_BEAR_THRESH, MARKET_SENTIMENT_BULL_THRESH, MARKET_SENTIMENT_SCALE_DOWN
    _declining_pct = 0.5  # 默认中性
    try:
        if _ticker_all:
            _chg_list = [float(v.get("priceChangePercent", 0)) for v in _ticker_all.values() if v]
            _declining_pct = sum(1 for c in _chg_list if c < 0) / len(_chg_list) if _chg_list else 0.5
    except Exception:
        pass
    # 全局仓位系数：大多数在跌→做多降仓，大多数在涨→做空降仓
    _global_long_scale  = MARKET_SENTIMENT_SCALE_DOWN if _declining_pct > MARKET_SENTIMENT_BEAR_THRESH else 1.0
    _global_short_scale = MARKET_SENTIMENT_SCALE_DOWN if _declining_pct < MARKET_SENTIMENT_BULL_THRESH else 1.0
    if _global_long_scale < 1.0:
        logger.info(f"[scanner] 市场情绪偏空: {_declining_pct*100:.0f}%标的下跌>70%，做多仓位×{_global_long_scale}")
    if _global_short_scale < 1.0:
        logger.info(f"[scanner] 市场情绪偏多: {(1-_declining_pct)*100:.0f}%标的上涨>70%，做空仓位×{_global_short_scale}")

    # ── 全市场资金费率情绪感知（v3.71）──────────────────────────────────────
    _funding_sentiment = {}
    try:
        from shared.clawby_adapter import get_market_funding_sentiment as _gfs
        from config import MARKET_FUNDING_SCALE
        _funding_sentiment = _gfs()
        _fs_val = _funding_sentiment.get("sentiment", "neutral")
        if _fs_val == "long_crowded":
            # 多头拥挤：LONG 信号仓位额外降权
            _old_long = _global_long_scale
            _global_long_scale = round(_global_long_scale * MARKET_FUNDING_SCALE, 3)
            logger.info(
                f"[scanner] 全市场费率情绪多头拥挤 "
                f"(pct_pos={_funding_sentiment.get('pct_positive',0):.0%})，"
                f"LONG仓位系数 {_old_long:.2f}→{_global_long_scale:.2f}"
            )
        elif _fs_val == "short_crowded":
            _old_short = _global_short_scale
            _global_short_scale = round(_global_short_scale * MARKET_FUNDING_SCALE, 3)
            logger.info(
                f"[scanner] 全市场费率情绪空头拥挤 "
                f"(pct_pos={_funding_sentiment.get('pct_positive',0):.0%})，"
                f"SHORT仓位系数 {_old_short:.2f}→{_global_short_scale:.2f}"
            )
    except Exception as _fse:
        logger.debug(f"[scanner] 全市场资金费率情绪获取失败（降级忽略）: {_fse}")

    signals_raw = []
    _ROUTE_QUALITY.clear()
    _kline_round_reset()
    _jobs = {
        sym: (sym, _ticker_all.get(sym, {}), active_syms, pending_syms, allowed_dirs, _loss_blacklist, _btc_trend, _mkt_mode)
        for sym in scan_pool
    }
    # [fix] mark_price_map 用 partial 以关键字绑定：analyze_symbol 的第 9 个位置参数是 deadline，
    # _run_parallel_with_deadline 又以关键字传 deadline，位置传 map 会 TypeError(multiple values for 'deadline')
    # 并行分析前先有界预热本轮候选池里过期的清算区缓存：analyze_one 内 get_liq_zones_cached 尽量命中缓存，
    # 少在分析线程里做同步 SSL 请求；超时/失败直接进入分析（降级为分析时实时查询）
    try:
        import config as _cfg_pf
        if getattr(_cfg_pf, "LIQ_PRESCAN_PREFETCH_ENABLED", True):
            from shared.liq_cache import prefetch_liq_cache_bounded as _plb
            _pf_t = time.perf_counter()
            _pf = _plb(list(scan_pool)[:50], float(getattr(_cfg_pf, "LIQ_PRESCAN_PREFETCH_TIMEOUT_SEC", 30.0)),
                       price_map=_mark_price_map)
            if _pf.get("stale"):
                logger.info(f"[scanner] 扫描前清算区预热: 候选 {_pf['total']}，过期 {_pf['stale']}，"
                            f"完成 {_pf['done']}，写入 {_pf['updated']}，超时未处理 {_pf['timed_out']}，"
                            f"耗时 {time.perf_counter() - _pf_t:.1f}s")
    except Exception as _pfe:
        logger.debug(f"[scanner] 扫描前清算区预热跳过: {_pfe}")
    # 阶段3：主进程标记「分析中」，仅供影子触发器读取；scanner 不因此跳过任何标的
    try:
        from shared import trigger_detector as _tdm
        _tdm.mark_analyzing_many(_jobs.keys())
    except Exception:
        _tdm = None
    _perf_t = time.perf_counter()
    import config as _cfg_pp
    if getattr(_cfg_pp, "SCAN_USE_PROCESS_POOL", False):
        _par_results, _unfinished = _run_parallel_process_pool(
            _jobs, _mark_price_map, SCAN_MAX_WORKERS, SCAN_TOTAL_TIMEOUT_SEC
        )
    else:
        _par_results, _unfinished = _run_parallel_with_deadline(
            _functools.partial(analyze_symbol, mark_price_map=_mark_price_map), _jobs, SCAN_MAX_WORKERS, SCAN_TOTAL_TIMEOUT_SEC
        )
    _perf_analysis = time.perf_counter() - _perf_t
    _perf_post_t0 = time.perf_counter()
    if _tdm is not None:
        try:
            _tdm.clear_analyzing_many(_jobs.keys())
        except Exception:
            pass
    result["scan_unfinished"] = len(_unfinished)
    for sym, res in _par_results:
        if res:
            signals_raw.append(res)
            result["class_counts"][res["sig_type"]] = \
                result["class_counts"].get(res["sig_type"], 0) + 1

    result["signals_raw"] = len(signals_raw)
    # 阶段3：judge 通过的原始信号落盘，供影子触发器对账（无信号也写一行；失败不影响扫描）
    try:
        from shared.trigger_detector import write_scan_signals as _wss
        # [v3.100] 统一冷却去重：通知触发器本轮扫描已分析过的 (symbol, strategy)
        try:
            from shared import trigger_detector as _tdm
            _det = _tdm._GLOBAL_DETECTOR
            if _det is not None:
                for _sig in (avail or []):
                    _tdm_strat = _sig.get("sig_type") or _sig.get("mode", "")
                    if _tdm_strat:
                        _det.notify_scan_analyzed(_sig["symbol"], _tdm_strat)
        except Exception:
            pass
        _wss([{"symbol": r.get("symbol"), "sig_type": r.get("sig_type"), "direction": r.get("direction"),
               "route_quality": _ROUTE_QUALITY.get(r.get("symbol"))} for r in signals_raw])
    except Exception:
        pass
    for _kl_line in _kline_round_summary():
        logger.info(_kl_line)

    # ── 第四步～第五步：共用函数（阶段5 抽取，扫描与触发器共用）──────────────────
    _budget_info = {
        "_btc_trend": _btc_trend,
        "_mkt_mode": _mkt_mode,
        "long_cnt": long_cnt,
        "short_cnt": short_cnt,
        "_dyn_max_long": _dyn_max_long,
        "_dyn_max_short": _dyn_max_short,
        "_global_long_scale": _global_long_scale,
        "_global_short_scale": _global_short_scale,
        "_macro_conservative": _macro_conservative,
        "_macro_event_desc": _macro_event_desc,
        "_post_shock": _post_shock,
        "_mark_price_map_ts": _mark_price_map_ts,
        "now": now,
        "_loss_blacklist": _loss_blacklist,
        "_LOSS_THRESHOLD": locals().get("_LOSS_THRESHOLD", 15.0),  # 原值在 try 内赋值，异常时黑名单为空、不会被读到
        "_mark_price_map": _mark_price_map,
    }
    _budget_info["_allowed_dirs"] = allowed_dirs
    _set_trigger_context(_budget_info, active_syms, pending_syms)
    _n_reg, avail_signals = _filter_and_register_signals(
        signals_raw, active_syms, pending_syms, _mkt_mode, _btc_trend, _budget_info, "scan",
        signal_watcher=signal_watcher, dry_run=dry_run, result=result,
    )

    result["elapsed"] = round(time.time() - t0, 2)

    # ── 最后一步：写入 OI 缓存（不影响扫描速度）────────────────────────────
    _flush_oi_cache(scan_pool)

    # v3.82: 主动将空闲内存归还 OS（防止 pymalloc arena 水位无限抬高）
    # glibc malloc_trim 将堆顶空闲页归还 OS，不影响正常对象
    # [perf] 仅在 RSS 超过 800MB 时才归还
    try:
        import ctypes as _ct, os as _os
        _rss_kb = int(open(f"/proc/{_os.getpid()}/status").read()
                      .split("VmRSS:")[1].split()[0])
        if _rss_kb > 800 * 1024:  # 超过800MB才归还
            _libc = _ct.CDLL("libc.so.6")
            _libc.malloc_trim(_ct.c_size_t(0))
    except Exception:
        pass

    try:
        from shared.observability import format_scan_perf as _fmt_perf
        logger.info(_fmt_perf(_perf_pool, _perf_mark, _perf_analysis, time.perf_counter() - _perf_post_t0,
                              len(_unfinished), len(scan_pool)))
    except Exception:
        pass
    # [liq-stats] 每轮扫描结束输出清算区命中率；统计失败不影响扫描
    try:
        from shared.liq_cache import coverage as _liq_cov
        _lh, _lt = _liq_cov(scan_pool, "3d")
        logger.info(_liq_stats_line(_lh, _lt, _count_sl_degraded(avail_signals)))
    except Exception as _lse:
        logger.debug(f"[liq-stats] 统计失败（忽略）: {_lse}")
    _write_scan_log(result, avail_signals)
    # [v3.89 SHADOW] 影子穿透追踪：扫描后处理被拒的信号写入 rejected_log.jsonl（只记录）
    try:
        from shared import shadow_trace as _st
        _st.record_scanner_rejections(result, signals_raw)
    except Exception as _st_e:
        logger.debug(f"[shadow] 扫描后处理记录失败: {_st_e}")
    return result

# ── 评分附加加分项（hub 数据；hub 未就绪/数据不足时该项跳过，不加不减）──────────────
# 在现有 score 全部算完之后叠加，每项独立 try，开关见 config.SCORE_TAKER_BONUS / SCORE_RANGE_BONUS / SCORE_VOLPRICE_BONUS
_BONUS_TAKER_BARS = 6
_BONUS_VOL_RECENT = 4
_BONUS_VOL_BASE = 20
_BONUS_VOL_RATIO_MIN = 1.6
_BONUS_VOL_CHG_LIMIT = 3.0


def _bonus_hub_klines(symbol: str, interval: str, limit: int):
    """只读 hub（不回落 REST）：hub 关闭/未就绪/数据不足返回 None。"""
    if not _hub_enabled():
        return None
    try:
        from shared import market_data_hub as _hub
        return _hub.get_klines(symbol, interval, limit) or None
    except Exception:
        return None


def _bonus_hub_ticker(symbol: str):
    if not _hub_enabled():
        return None
    try:
        from shared import market_data_hub as _hub
        return _hub.get_ticker(symbol) or None
    except Exception:
        return None


def _calc_taker_bonus(direction: str, rows):
    """近 6 根 1h：Σ主动买入量(V,idx9) / Σ总量(v,idx5)。返回 (加分|None, 占比|None)。"""
    if not rows or len(rows) < _BONUS_TAKER_BARS:
        return None, None
    rows = rows[-_BONUS_TAKER_BARS:]
    tot = sum(float(r[5]) for r in rows)
    if tot <= 0:
        return None, None
    ratio = sum(float(r[9]) for r in rows) / tot
    if direction == "LONG":
        return (10 if ratio > 0.55 else -5 if ratio < 0.45 else 0), ratio
    return (10 if ratio < 0.45 else -5 if ratio > 0.55 else 0), ratio


def _calc_range_bonus(direction: str, tk):
    """position=(last-low)/(high-low)，来自 miniTicker。返回 (加分|None, position|None)。"""
    if not tk:
        return None, None
    h, l, c = float(tk["highPrice"]), float(tk["lowPrice"]), float(tk["lastPrice"])
    if h <= l:
        return None, None
    pos = (c - l) / (h - l)
    if direction == "LONG":
        return (8 if pos >= 0.8 else -5 if pos <= 0.2 else 0), pos
    return (8 if pos <= 0.2 else -5 if pos >= 0.8 else 0), pos


def _calc_volprice_bonus(direction: str, rows, tk):
    """近 4 根 1h 成交额(q,idx7)均值 / 前 20 根均值 = vol_ratio；放量且 24h 涨跌幅方向上有限 → +8。返回 (加分|None, vol_ratio|None)。"""
    need = _BONUS_VOL_RECENT + _BONUS_VOL_BASE
    if not rows or len(rows) < need or not tk:
        return None, None
    rows = rows[-need:]
    base = sum(float(r[7]) for r in rows[:_BONUS_VOL_BASE]) / _BONUS_VOL_BASE
    if base <= 0:
        return None, None
    ratio = (sum(float(r[7]) for r in rows[_BONUS_VOL_BASE:]) / _BONUS_VOL_RECENT) / base
    chg = float(tk["priceChangePercent"])
    if ratio >= _BONUS_VOL_RATIO_MIN:
        if direction == "LONG" and chg < _BONUS_VOL_CHG_LIMIT:
            return 8, ratio
        if direction == "SHORT" and chg > -_BONUS_VOL_CHG_LIMIT:
            return 8, ratio
    return 0, ratio


def _apply_score_bonuses(symbol: str, direction: str, sig_type: str, score: int):
    """在现有 score 之上叠加三个加分项，总分截断到 [0,100]。返回 (new_score, info)；info 各项为 None 表示跳过。"""
    info = {"taker": None, "taker_ratio": None, "range": None, "range_pos": None,
            "volprice": None, "vol_ratio": None, "hub_bonus": 0}
    if sig_type == "funding_arb" or direction not in ("LONG", "SHORT"):
        return score, info
    try:
        import config as _c
    except Exception:
        return score, info
    if getattr(_c, "SCORE_TAKER_BONUS", False):
        try:
            info["taker"], info["taker_ratio"] = _calc_taker_bonus(
                direction, _bonus_hub_klines(symbol, "1h", _BONUS_TAKER_BARS))
        except Exception as e:
            logger.debug(f"[score] {symbol} taker 加分失败（跳过）: {e}")
    tk = None
    if getattr(_c, "SCORE_RANGE_BONUS", False) or getattr(_c, "SCORE_VOLPRICE_BONUS", False):
        tk = _bonus_hub_ticker(symbol)
    if getattr(_c, "SCORE_RANGE_BONUS", False):
        try:
            info["range"], info["range_pos"] = _calc_range_bonus(direction, tk)
        except Exception as e:
            logger.debug(f"[score] {symbol} range 加分失败（跳过）: {e}")
    if getattr(_c, "SCORE_VOLPRICE_BONUS", False):
        try:
            info["volprice"], info["vol_ratio"] = _calc_volprice_bonus(
                direction, _bonus_hub_klines(symbol, "1h", _BONUS_VOL_RECENT + _BONUS_VOL_BASE), tk)
        except Exception as e:
            logger.debug(f"[score] {symbol} volprice 加分失败（跳过）: {e}")
    adds = [info[k] for k in ("taker", "range", "volprice") if info[k] is not None]
    _cap = getattr(_c, "SCORE_HUB_BONUS_CAP", 15)
    _floor = getattr(_c, "SCORE_HUB_PENALTY_FLOOR", -10)
    total_bonus = max(min(sum(adds), _cap), _floor)   # 双向 cap：正向上限 + 负向下限
    info["hub_bonus"] = total_bonus
    new_score = min(100, max(0, score + total_bonus))
    if any(v is not None for v in (info["taker"], info["range"], info["volprice"])):
        def f(v):
            return "NA" if v is None else f"{v:+d}"
        vr = "NA" if info["vol_ratio"] is None else f"{info['vol_ratio']:.1f}"
        logger.info(f"[score] {symbol} taker={f(info['taker'])} range={f(info['range'])} "
                    f"vol_ratio={vr}({f(info['volprice'])}) total={new_score}")
    return new_score, info


# ── OI 历史（1h 周期）进程内缓存 ───────────────────────────────────────────────
# key=symbol，value=(data, fetch_ts)。OI 历史 1 小时一根，TTL 55 分钟内复用；重启清空，不持久化。
# 只缓存非空结果（请求失败返回 [] 不写入，下次仍会重试）。limit=6 与 limit=3 共用同一份数据：
# 未命中时统一拉 _OI_HIST_FETCH_LIMIT 根，limit=3 取最后 3 根（接口返回按时间升序，等价于直接请求 limit=3）。
_OI_HIST_TTL_SEC = 55 * 60
_OI_HIST_FETCH_LIMIT = 6
_OI_HIST_CACHE_MAX = 2000
_oi_history_cache: dict = {}
_oi_history_lock = _threading.Lock()


def _oi_hist_clock() -> float:
    return time.time()


def _get_oi_history_cached(symbol: str, period: str = "1h", limit: int = 6) -> list:
    """api.get_oi_history 的缓存封装：命中直接返回，未命中才发 REST 并写入缓存。仅缓存 period=1h 且 limit<=6 的请求。"""
    if period != "1h" or limit > _OI_HIST_FETCH_LIMIT:
        return api.get_oi_history(symbol, period=period, limit=limit)
    now = _oi_hist_clock()
    with _oi_history_lock:
        hit = _oi_history_cache.get(symbol)
    if hit is not None and now - hit[1] < _OI_HIST_TTL_SEC and len(hit[0]) >= limit:
        return hit[0][-limit:]
    data = api.get_oi_history(symbol, period=period, limit=_OI_HIST_FETCH_LIMIT)
    if data:
        with _oi_history_lock:
            if len(_oi_history_cache) >= _OI_HIST_CACHE_MAX:          # 防止无限增长：先清过期，仍满则清空
                for k in [k for k, v in _oi_history_cache.items() if now - v[1] >= _OI_HIST_TTL_SEC]:
                    del _oi_history_cache[k]
                if len(_oi_history_cache) >= _OI_HIST_CACHE_MAX:
                    # [perf] 仍满时只淘汰最旧的 20%，不再整表清空
                    _sorted = sorted(_oi_history_cache.items(), key=lambda x: x[1][1])
                    for k, _ in _sorted[:len(_sorted) // 5]:
                        del _oi_history_cache[k]
            _oi_history_cache[symbol] = (data, now)
        return data[-limit:]
    return data


# ── OI 缓存写入 ──────────────────────────────────────────────────────────────
_OI_FLUSH_WORKERS = 10


def _flush_oi_cache(symbols: list) -> None:
    """
    并发拉取 symbols 中每个标的的当前 OI，写入缓存。
    放在每轮扫描末尾，并发拉取降低耗时。
    """
    oi_map = {}
    oi_lock = __import__("threading").Lock()

    def _fetch_one(sym):
        try:
            oi_data = api.get_open_interest(sym)
            oi_val  = float(oi_data.get("openInterest", 0))
            if oi_val > 0:
                with oi_lock:
                    oi_map[sym] = oi_val
        except Exception:
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=_OI_FLUSH_WORKERS) as _oi_exe:
        list(_oi_exe.map(_fetch_one, symbols))

    if oi_map:
        update_oi_cache(oi_map)
        logger.debug(f"[scanner] OI 缓存已更新: {len(oi_map)} 个标的")


# ── 日志 ──────────────────────────────────────────────────────────────────────
def _write_scan_log(result: dict, signals: list) -> None:
    cc = result["class_counts"]
    lines = [
        f"[scan] {result['ts']} "
        f"扫描{result['scanned']} "
        f"reversal={cc.get('reversal',0)} breakout={cc.get('breakout',0)} pullback={cc.get('pullback',0)} "
        f"原始信号{result['signals_raw']} 最终信号{result['signals_final']} "
        f"开仓{result['placed']} 耗时{result['elapsed']}s"
    ]
    for sig in signals:
        lines.append(
            f"  [signal] {sig['symbol']} {sig['direction']} {sig['sig_type']} "
            f"chg24={sig['chg24']:+.1f}% "
            f"rsi1h={sig['ind']['rsi_1h']:.1f} vol_ratio={sig['ind']['vol_ratio']:.2f}"
        )
    for r in result["rejected"]:
        lines.append(f"  [rejected] {r['symbol']}: {r['reason']}")

    log_text = "\n".join(lines)
    logger.info(log_text)

    # 同时写 scanner.log 文件（轮转写入，单文件上限 5MB，保留 3 份备份）
    # [v3.89 #21] 备份统一命名 scanner.log.1/.2/.3；原写法当前文件被改名为 scanner.1.log，
    #   而后续备份却按 scanner.log.N 查找，最近一份备份无法顺序滚动；轮转/写入失败也被静默吞掉。
    try:
        SCANNER_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        _MAX_LOG_BYTES = 5 * 1024 * 1024   # 5 MB
        _BACKUP_COUNT  = 3
        try:
            if SCANNER_LOG_FILE.exists() and SCANNER_LOG_FILE.stat().st_size > _MAX_LOG_BYTES:
                import os as _os
                _base = str(SCANNER_LOG_FILE)
                # 从最老的开始滚动：.2→.3（覆盖旧 .3），.1→.2，当前→.1
                for _i in range(_BACKUP_COUNT, 0, -1):
                    _src = Path(_base) if _i == 1 else Path(f"{_base}.{_i - 1}")
                    _dst = Path(f"{_base}.{_i}")
                    if _src.exists():
                        _os.replace(str(_src), str(_dst))
        except Exception as _re:
            logger.warning(f"[scanner] scanner.log 轮转失败: {_re}")
        with open(SCANNER_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_text + "\n")
    except Exception as _we:
        logger.warning(f"[scanner] scanner.log 写入失败: {_we}")


# ── CLI 入口 ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",      action="store_true", help="不实际下单")
    parser.add_argument("--refresh-pool", action="store_true", help="只刷新候选池")
    args = parser.parse_args()

    if args.refresh_pool:
        pool = refresh_pool()
        print(f"候选池已刷新: {len(pool)} 个")
    else:
        r = run_scan(dry_run=args.dry_run)
        if r["placed"] == 0 and r["signals_final"] == 0:
            print("HEARTBEAT_OK")
        else:
            print(f"扫描完成 | 信号{r['signals_final']} | 开仓{r['placed']}")


# ══════════════════════════════════════════════════════════════════════════════
# run_funding_arb_scan — 资金费率套利独立扫描（每30分钟运行一次）
# ══════════════════════════════════════════════════════════════════════════════
def run_funding_arb_scan(dry_run: bool = False, signal_watcher=None) -> dict:
    """
    独立的资金费率套利扫描：
    1. 检查 funding_arb 熔断和全局熔断
    2. 从 clawby 拿费率排行榜，取前 N 个 Binance 标的
    3. 对每个标的调 judge_funding_arb
    4. 通过则调 execute_signal（市价立即入场）
    返回本轮结果 dict。
    """
    import time as _t
    from datetime import datetime, timezone

    result = {
        "ts":        datetime.now(timezone.utc).isoformat(),
        "scanned":   0,
        "signals":   0,
        "placed":    0,
        "rejected":  [],
        "dry_run":   dry_run,
    }

    try:
        from config import (
            FUNDING_ARB_ENABLED, FUNDING_ARB_MAX_POSITIONS,
            FUNDING_ARB_CANDIDATE_TOP_N,
            FUNDING_ARB_ENTRY_WINDOW_MIN_H,
        )
    except Exception as _ce:
        result["error"] = f"config import 失败: {_ce}"
        return result

    if not FUNDING_ARB_ENABLED:
        return result

    # ── v3.92 周日收尾 + 周末计数器同步（与主扫描共用）──────────────────
    _fa_now = datetime.now(timezone.utc)
    if _is_weekend_cutoff(_fa_now):
        logger.info("[arb_scan] 周日收尾时段，不注册新信号")
        return result
    _weekend_counter_sync(_fa_now)
    _fa_weekend = _is_weekend_period(_fa_now)

    # ── 全局熔断 + funding_arb 专用熔断 ─────────────────────────────────────
    from shared.state import is_circuit_open, is_funding_arb_paused, get_funding_arb_count
    if is_circuit_open():
        logger.info("[arb_scan] 主策略熔断中，跳过 funding_arb 扫描")
        return result
    if is_funding_arb_paused():
        logger.info("[arb_scan] funding_arb 熔断暂停中，跳过")
        return result

    # ── v3.36 槽位检查：funding_arb 共享主策略总槽位 ────────────────────────
    # funding_arb 不再维护独立计数器，直接检查全局总持仓是否达到当前市场模式上限
    try:
        from shared.risk_manager import get_market_mode as _arb_mkt
        import config as _cfg
        _mkt_mode_arb = _arb_mkt()
        _max_total_arb = {
            "bull":  _cfg.MAX_POSITIONS_TOTAL_BULL,
            "bear":  _cfg.MAX_POSITIONS_TOTAL_BEAR,
            "range": _cfg.MAX_POSITIONS_TOTAL_RANGE,
        }.get(_mkt_mode_arb, _cfg.MAX_POSITIONS_TOTAL_BULL)
    except Exception:
        _max_total_arb = 5
    from shared.state import get_all_positions as _arb_all_pos
    _total_open_arb = sum(1 for p in _arb_all_pos()
                          if p.get("status") in ("open", "pending", "patching", "pending_guard"))
    if _total_open_arb >= _max_total_arb:
        logger.info(f"[arb_scan] 总仓位已满({_total_open_arb}/{_max_total_arb})，跳过 funding_arb 扫描")
        return result

    # ── 拉取费率排行榜：直接用 Binance 原生接口（稳定可靠）──────────────────
    # Coinglass 经常返回全 0 数据，不再依赖它作为主数据源
    candidates = []
    try:
        # hub优先获取标记价/费率，未就绪才fallback REST
        try:
            from shared import market_data_hub as _hub_arb
            if _hub_arb.is_ready():
                _snap = _hub_arb.get_all_mark_prices()
                all_mp = list(_snap.values()) if _snap else None
            else:
                all_mp = None
        except Exception:
            all_mp = None
        if not all_mp:
            all_mp = api.get_all_mark_prices()
        fr_list = [
            (item["symbol"], float(item.get("lastFundingRate", 0)))
            for item in all_mp
            if item.get("symbol", "").endswith("USDT")
               and abs(float(item.get("lastFundingRate", 0))) > 0
        ]
        fr_list.sort(key=lambda x: abs(x[1]), reverse=True)
        candidates = fr_list[:FUNDING_ARB_CANDIDATE_TOP_N * 2]  # 多取一些，后面再过滤
        logger.info(f"[arb_scan] Binance 原生费率: 找到 {len(fr_list)} 个非零标的，"
                    f"Top1={fr_list[0][0]} {fr_list[0][1]*100:+.4f}%" if fr_list else
                    f"[arb_scan] Binance 原生费率: 无非零标的")
    except Exception as _fe:
        logger.warning(f"[arb_scan] get_all_mark_prices 失败: {_fe}")

    # ── 结算时间改为逐标的动态获取（支持 1h/4h/8h 不同周期）─────────────────
    # [v3.87 STEP2] 删除未使用的 now_ts（随 hours_to_settle 一起清理）
    from shared.binance_api import get_next_funding_time, get_funding_interval
    # 下面逐标的计算 next_settle_ts，这里不再全局写死

    # ── 逐标的分析 ─────────────────────────────────────────────────────────────
    from shared.state import get_all_positions
    active_syms = {p["symbol"] for p in get_all_positions()
                   if p.get("status") in ("open", "pending", "patching", "pending_guard")}

    from shared.judges import judge_funding_arb
    from shared.indicators import rsi as _rsi_fn

    # [perf] 单标的分析抽成独立函数，线程池并发执行；下单仍在主线程串行（避免并发占槽/重复下单）
    def _analyze_one_arb(sym_full, fr_current):
        """返回 (sym_full, signal_or_None, reject_reason_or_None)。"""
        try:
            # ── 动态获取结算周期和下次结算时间 ──────────────────────────────
            funding_interval_h = get_funding_interval(sym_full)   # 1/4/8
            next_settle_ts     = get_next_funding_time(sym_full)
            # [v3.87 STEP2] 删除未使用的 hours_to_settle

            k1h = api.get_klines(sym_full, "1h", 100)
            k4h = api.get_klines(sym_full, "4h", 60)
            k15 = api.get_klines(sym_full, "15m", 40)
            if not k1h or len(k1h) < 25:
                return (sym_full, None, None)

            ind = calc_indicators(k15, k1h, k4h)
            ind["symbol"]             = sym_full
            ind["funding_interval_h"] = funding_interval_h  # 供 judge 使用
            ind["_weekend_mode"]      = _is_weekend_period(datetime.now(timezone.utc))  # v3.29: 与其他策略对齐

            # 计算 24H 涨跌幅（供趋势过滤使用）
            closes_1h = [float(k[4]) for k in k1h[:-1]]  # 只用已闭合 K线
            if len(closes_1h) >= 25:
                price_now = closes_1h[-1]
                price_24h_ago = closes_1h[-25]
                chg24 = (price_now - price_24h_ago) / price_24h_ago * 100 if price_24h_ago > 0 else 0
            else:
                chg24 = 0
            ind["chg24"] = chg24

            # 近3期资金费率历史
            try:
                from shared.binance_api import get_open_interest_hist as _oi_hist
                # [v3.87 STEP2] 删除结果从未使用的 get_open_interest_hist 调用（每个候选白白多一次接口请求）
                # 从 mark_price 历史拉近3期 funding rate
                fr_hist_raw2 = _get_recent_funding_rates(sym_full, limit=3)
                funding_hist = fr_hist_raw2 if fr_hist_raw2 else []
            except Exception:
                funding_hist = []

            # OI 1H 前的数据
            try:
                oi_now = float(api.get_open_interest(sym_full).get("openInterest", 0))
                oi_hist = api.get_open_interest_hist(sym_full, "1h", 2)
                oi_1h_ago = float(oi_hist[0].get("sumOpenInterest", 0)) if oi_hist else 0
            except Exception:
                oi_now = oi_1h_ago = 0
            ind["oi_now"]    = oi_now
            ind["oi_1h_ago"] = oi_1h_ago

            ind["funding_interval_h"] = funding_interval_h
            ok, detail = judge_funding_arb(sym_full, fr_current, funding_hist, ind, next_settle_ts, k1h, k4h)
            if not ok:
                return (sym_full, None, detail.get("reject_reason"))

            direction = detail["direction"]

            # ── 计算限价入场价：挂单簿密度 + ATR，取较近者 ─────────────────
            mark_price = ind.get("price", 0)
            atr_1h     = ind.get("atr_1h", mark_price * 0.02)

            # ATR 基准价
            atr_entry_long  = mark_price - atr_1h * 0.2
            atr_entry_short = mark_price + atr_1h * 0.2

            # 挂单簿密度：找最近的买/卖方支撑位（对于小市值币，挂单簿比清算图更有效）
            ob_entry = None
            try:
                ob = api.get_order_book(sym_full, limit=20)
                if direction == "LONG":
                    bids = [(float(p), float(q)*float(p)) for p, q in ob.get("bids", [])]
                    # 找现价下方最密集的买单区（USD 量最大）
                    bids_below = [(p, usdt) for p, usdt in bids if p < mark_price]
                    if bids_below:
                        densest = max(bids_below, key=lambda x: x[1])
                        ob_entry = densest[0]
                else:
                    asks = [(float(p), float(q)*float(p)) for p, q in ob.get("asks", [])]
                    asks_above = [(p, usdt) for p, usdt in asks if p > mark_price]
                    if asks_above:
                        densest = max(asks_above, key=lambda x: x[1])
                        ob_entry = densest[0]
            except Exception:
                pass

            # 取两者中更接近现价的那个（更容易成交，同时也是真实支撑）
            if direction == "LONG":
                if ob_entry and ob_entry > atr_entry_long:
                    # 挂单簿密度位比 ATR 位更高（更近），用更近的，成交概率更高
                    entry_price = ob_entry
                    logger.debug(f"[arb_scan] {sym_full} LONG 用挂单簿入场价 {ob_entry:.6g}（ATR={atr_entry_long:.6g}）")
                else:
                    entry_price = atr_entry_long
            else:
                if ob_entry and ob_entry < atr_entry_short:
                    entry_price = ob_entry
                    logger.debug(f"[arb_scan] {sym_full} SHORT 用挂单簿入场价 {ob_entry:.6g}（ATR={atr_entry_short:.6g}）")
                else:
                    entry_price = atr_entry_short

            entry_price = round(entry_price, 8)

            # 结算前5分钟（300s）为撤单截止时间
            cancel_before_ts = next_settle_ts - 300
            # v3.37: 结算后5分钟检查（原来是30分钟），让续持评估更及时
            cooldown_end_ts  = next_settle_ts + 300

            signal = {
                "symbol":            sym_full,
                "direction":         direction,
                "mode":              "funding_arb",
                "sig_type":          "funding_arb",
                "signal_strength":   detail["signal_strength"],
                "position_size_cap": detail["position_size_cap"],
                "funding_rate":      fr_current,
                "funding_hist":      funding_hist,
                "next_settle_ts":    next_settle_ts,
                "funding_interval_h": funding_interval_h,
                "cancel_before_ts":  cancel_before_ts,   # 结算前5min撤单
                "cooldown_end_ts":   cooldown_end_ts,    # 结算后5min续持检查（v3.37）
                "entry_price":       mark_price,         # 市价立即入场（不等限价回踩，确保结算前持仓）
                "entry_mode":        "immediate",        # 市价立即入场
                # v3.39: 止损按结算周期缩放——周期越短持仓越短，价格风险越小，止损收得越紧
                # 1h=0.5ATR（持仓≤55min）  4h=0.75ATR  8h=1.0ATR（标准）
                "sl_price":          round(
                    mark_price - atr_1h * {1: 0.5, 4: 0.75}.get(funding_interval_h, 1.0), 8
                ) if direction == "LONG" else round(
                    mark_price + atr_1h * {1: 0.5, 4: 0.75}.get(funding_interval_h, 1.0), 8
                ),
                "judge_detail":      detail,
                "ind":               ind,
                "chg24":             0.0,
                "arb_settle_count":  1,              # v3.37: 已收费次数，从1开始（本次入场算第1次）
                "arb_post_check_done": False,        # v3.37: 结算后检查标志
            }

            logger.info(
                f"[arb_scan] {sym_full} {direction} fr={fr_current*100:+.4f}% "
                f"interval={funding_interval_h}h  entry_market={mark_price:.6g}  "
                f"cancel_before={_t.strftime('%H:%M', _t.gmtime(cancel_before_ts))}UTC"
            )

            return (sym_full, signal, None)
        except Exception as _e:
            logger.warning(f"[arb_scan] {sym_full} 分析异常: {_e}")
            return (sym_full, None, None)

    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as _FutTimeout
    _todo = []
    for sym_full, fr_current in candidates[:FUNDING_ARB_CANDIDATE_TOP_N]:
        result["scanned"] += 1
        # 同标的已有主策略仓位，跳过
        if sym_full in active_syms:
            result["rejected"].append({"symbol": sym_full, "reason": "已有持仓"})
            continue
        _todo.append((sym_full, fr_current))

    _arb_signals = []
    _exe = ThreadPoolExecutor(max_workers=FUNDING_ARB_SCAN_WORKERS, thread_name_prefix="arb-sym")
    try:
        futs = {_exe.submit(_analyze_one_arb, sym, fr): sym for sym, fr in _todo}
        try:
            for fut in as_completed(futs, timeout=120):
                sym = futs[fut]
                try:
                    _sym, _sig, _rej = fut.result()
                except Exception as _fe2:   # 单标的异常不影响其他标的
                    logger.warning(f"[arb_scan] {sym} 分析线程异常: {_fe2}")
                    continue
                if _sig is not None:
                    _arb_signals.append(_sig)
                elif _rej is not None:
                    result["rejected"].append({"symbol": _sym, "reason": _rej})
        except _FutTimeout:
            _left = [s for f, s in futs.items() if not f.done()]
            logger.warning(f"[arb_scan] 并发分析超过 120s，放弃未完成标的: {_left}")
    finally:
        _exe.shutdown(wait=False, cancel_futures=True)

    for signal in _arb_signals:
        sym_full = signal["symbol"]
        result["signals"] += 1
        if dry_run:
            logger.info(f"[arb_scan] DRY RUN: 信号通过，跳过下单")
            continue
        # v3.92 周末新开仓上限（与主扫描共用计数器）
        if _fa_weekend:
            from config import WEEKEND_MAX_NEW_POSITIONS
            if _WEEKEND_POS_COUNT >= WEEKEND_MAX_NEW_POSITIONS:
                logger.warning(f"[arb_scan] 周末开仓已达上限 {WEEKEND_MAX_NEW_POSITIONS} 笔，跳过 {sym_full}")
                result["rejected"].append({"symbol": sym_full,
                    "reason": f"周末开仓上限 {WEEKEND_MAX_NEW_POSITIONS} 笔已满"})
                continue
            signal["weekend_mode"] = True
            # v3.92 [2-B] funding_arb 周末仓位缩减（只乘一次）
            from config import WEEKEND_FUNDING_ARB_SCALE
            _fa_old_cap = signal.get("position_size_cap", 0.3)
            signal["position_size_cap"] = round(_fa_old_cap * WEEKEND_FUNDING_ARB_SCALE, 3)
            logger.debug(f"[scanner][funding_arb] {sym_full} 周末仓位缩减 "
                         f"{_fa_old_cap:.3f}→{signal['position_size_cap']:.3f}")
        # 挂限价单入场（主线程串行）
        try:
            import executor as _exec
            _ws_listener = getattr(signal_watcher, "_ws_listener", None)
            ok_exec = _exec.execute_signal_sync(signal, _ws_listener)
            if ok_exec:
                result["placed"] += 1
                if _fa_weekend:
                    signal["weekend_pos_seq"] = _weekend_counter_inc()
                logger.info(f"[arb_scan] {sym_full} 限价单已挂，等待成交")
        except Exception as _ee:
            logger.error(f"[arb_scan] {sym_full} execute_signal 失败: {_ee}")

    logger.info(f"[arb_scan] 完成: scanned={result['scanned']} signals={result['signals']} placed={result['placed']}")
    # 打出有实质 reject_reason 的标的（排除"已有持仓"等槽位类拒绝，聚焦 judge 拒绝）
    _arb_judge_rejects = [r for r in result.get("rejected", []) if r.get("reason") and r["reason"] not in ("已有持仓",)]
    if _arb_judge_rejects:
        for _rj in _arb_judge_rejects[:10]:   # 最多打 10 条，避免刷屏
            logger.info(f"[arb_scan] REJECT {_rj['symbol']}: {_rj.get('reason','?')}")
    return result


def _get_recent_funding_rates(symbol: str, limit: int = 3) -> list:
    """拉近 N 次资金费率结算记录，返回 [float, ...]（旧→新）"""
    try:
        data = api._get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})  # 公开接口，无需签名
        if isinstance(data, list):
            return [float(d.get("fundingRate", 0)) for d in data]
    except Exception:
        pass
    return []
