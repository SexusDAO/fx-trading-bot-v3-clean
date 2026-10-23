"""
shared/liq_cache.py — 清算区数据后台缓存

三级降级（v3.15.2 调整优先级）：
  1. Coinglass heatmap（权威来源，精度最高，支持 range_）
  2. ByKaranteli（备选，CG 失败时兜底）
  3. Coinglass liq_orders（最后兜底）

get_liq_zones_cached() 是 judges / calc_sl_tp 的唯一调用入口。
"""
from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from typing import Any

logger = logging.getLogger("fx.liq_cache")

# ── 缓存存储 ───────────────────────────────────────────────────────────────
_CACHE: dict[str, dict] = {}
_CACHE_TTL = 15 * 60   # 15 分钟
_CACHE_LOCK = threading.Lock()
# [v3.89 #34] 容量上限：_CACHE 最多保留 500 个 key（key=「标的:区间」），写入后超出时按 ts 淘汰最旧的。
#   TTL 只决定数据是否需要刷新，不会删除条目；标的池长期变化时过期条目会一直驻留内存。
_CACHE_MAX_ENTRIES = 500
_EVICTED_TOTAL = 0   # 累计淘汰条数（监控用）

# ── 后台刷新线程状态 ───────────────────────────────────────────────────────
# [v3.89 #35] 按 key 合并在途请求：同一 key 同时只有一个调用真正去拉数据，其余等待同一结果；
#   写回只接受不比现有结果旧的数据，避免较早发出的慢响应覆盖较新的结果。网络请求期间不持有全局锁。
_INFLIGHT: dict = {}
_INFLIGHT_WAIT = 30.0   # 等待他人在途请求的上限（秒）
# [v3.89 #38] 三级数据源全失败：短期负缓存 + 告警节流，避免上游重复调用时反复重拉数据源、反复同步等告警进程
_FAIL_CACHE: dict = {}
_FAIL_TTL = 30.0           # 负缓存时长（秒）
_ALERT_LAST: dict = {}
_ALERT_INTERVAL = 1800.0   # 同一标的告警最小间隔（秒）
_bg_started = False
_bg_lock    = threading.Lock()

# 预热覆盖的 range_ 列表：确保所有场景均能命中缓存
_PREFETCH_RANGES = ("3d", "24h", "7d")


# ─────────────────────────────────────────────────────────────────────────────
# 内部工具
# ─────────────────────────────────────────────────────────────────────────────

def _liq_orders_to_zones(orders: list, direction: str, current_price: float) -> list:
    """把 liq_orders 原始列表转成 zone 格式（兜底路径用）。"""
    if not orders or current_price <= 0:
        return []
    bucket_pct = 0.05
    buckets: dict[int, float] = {}
    for o in orders:
        try:
            p   = float(o.get("price", 0) or o.get("liq_price", 0) or 0)
            usd = float(o.get("liqUsd", 0) or o.get("usd_size", 0) or 0)
            if p <= 0 or usd <= 0:
                continue
            if direction == "LONG"  and p >= current_price:
                continue
            if direction == "SHORT" and p <= current_price:
                continue
            idx = int(p / (current_price * bucket_pct))
            buckets[idx] = buckets.get(idx, 0.0) + usd
        except Exception:
            continue
    if not buckets:
        return []
    zones = []
    for idx, liq_usd in sorted(buckets.items(), key=lambda x: -x[1])[:10]:
        center = idx * current_price * bucket_pct
        zones.append({
            "price_low":  round(center * 0.975, 8),
            "price_high": round(center * 1.025, 8),
            "liq_usd":    round(liq_usd, 0),
        })
    return zones


def _fetch_from_bykaranteli(symbol: str, current_price: float) -> dict:
    """从 ByKaranteli 拉取清算区（备选路径，不区分 range）。"""
    try:
        from shared.bykaranteli_api import get_liq_zones
        long_zones  = get_liq_zones(symbol, "LONG",  current_price)
        short_zones = get_liq_zones(symbol, "SHORT", current_price)
        if long_zones or short_zones:
            logger.info(f"[liq_cache] {symbol} BYK liqmap: long={len(long_zones)} short={len(short_zones)} zones")
            return {"LONG": long_zones, "SHORT": short_zones, "ts": time.time(), "src_level": 2}
    except Exception as e:
        logger.debug(f"[liq_cache] {symbol} BYK 失败: {e}")
    return {}


def _alert_all_failed(symbol: str) -> None:
    """[v3.89 #38] 全失败告警：每标的 30 分钟最多一次，放后台线程发送，不阻塞数据路径；
    拉取失败（调用方已记 warning）与告警投递失败分开记录。"""
    now = time.time()
    with _CACHE_LOCK:
        if now - _ALERT_LAST.get(symbol, 0) < _ALERT_INTERVAL:
            return
        _ALERT_LAST[symbol] = now

    def _send():
        try:
            import subprocess as _sp
            r = _sp.run(
                ["openclaw", "message", "send", "--channel", "jarvis",
                 "--message", f"⚠️ liq_cache {symbol} 三级数据源全部失败，清算图不可用，止损将降级为ATR公式"],
                timeout=5, capture_output=True,
            )
            if r.returncode != 0:
                logger.warning(f"[liq_cache] {symbol} 告警投递失败 rc={r.returncode} "
                               f"stderr={(r.stderr or b'')[:200]!r}")
        except Exception as e:
            logger.warning(f"[liq_cache] {symbol} 告警投递异常: {e}")

    threading.Thread(target=_send, name="liq-alert", daemon=True).start()


def _fetch_from_clawby(symbol: str, current_price: float,
                        range_: str = "3d") -> dict:
    """
    清算区数据拉取，三级降级（v3.15.2：CG 升为主路径，BYK 降为备选）：
    1. Coinglass heatmap density（权威来源，支持 range_）
    2. ByKaranteli LiqMap（备选，CG 失败时兜底，不区分 range）
    3. Coinglass liq_orders 大额清算单（最后兜底）
    """
    # 主路径：Coinglass heatmap（权威来源，精度最高，支持 range_）
    try:
        from shared.clawby_adapter import get_liq_density
        density = get_liq_density(symbol, range_=range_)
        if density:
            long_zones  = []
            short_zones = []
            for price, val in sorted(density.items()):
                if val < 50_000:
                    continue
                zone = {"price_low": price * 0.998, "price_high": price * 1.002,
                        "amount": val}
                if price < current_price:
                    long_zones.append(zone)
                else:
                    short_zones.append(zone)
            if long_zones or short_zones:
                logger.info(f"[liq_cache] {symbol} CG heatmap: long={len(long_zones)} short={len(short_zones)} zones")
                return {"LONG": long_zones, "SHORT": short_zones, "ts": time.time(), "src_level": 1}
    except Exception as e:
        logger.debug(f"[liq_cache] {symbol} CG heatmap 失败: {e}")

    # 降级1：ByKaranteli（CG 失败时兜底）
    result = _fetch_from_bykaranteli(symbol, current_price)
    if result:
        logger.debug(f"[liq_cache] {symbol} CG失败，兜底用 BYK")
        return result

    # 降级2：Coinglass liq_orders
    try:
        from shared.coinglass_api import get_liq_orders
        raw = get_liq_orders(symbol, min_usd=50_000)
        if raw:
            long_zones  = _liq_orders_to_zones(raw, "LONG",  current_price)
            short_zones = _liq_orders_to_zones(raw, "SHORT", current_price)
            logger.debug(f"[liq_cache] {symbol} 降级到 CG liq_orders")
            return {"LONG": long_zones, "SHORT": short_zones, "ts": time.time(), "src_level": 3}
    except Exception as e:
        logger.debug(f"[liq_cache] {symbol} CG liq_orders 失败: {e}")

    logger.warning(f"[liq_cache] {symbol} 三级数据源全部失败，止损将降级为ATR公式")
    _alert_all_failed(symbol)
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# 公共接口
# ─────────────────────────────────────────────────────────────────────────────

def _store_if_newer(key: str, data: dict) -> bool:
    """[v3.89 #35] 版本化写回：现有缓存更新（ts 更大）时放弃写入。"""
    with _CACHE_LOCK:
        old = _CACHE.get(key)
        if old and old.get("ts", 0) > (data or {}).get("ts", 0):
            return False
        _CACHE[key] = data
        _evict_oldest_locked()
        return True


def _evict_oldest_locked() -> int:
    """[v3.89 #34] 调用方须已持有 _CACHE_LOCK。超出 _CACHE_MAX_ENTRIES 时按 ts 从旧到新淘汰，返回淘汰条数。"""
    global _EVICTED_TOTAL
    over = len(_CACHE) - _CACHE_MAX_ENTRIES
    if over <= 0:
        return 0
    victims = sorted(_CACHE, key=lambda k: (_CACHE[k] or {}).get("ts", 0))[:over]
    for k in victims:
        _CACHE.pop(k, None)
    _EVICTED_TOTAL += len(victims)
    logger.info(f"[liq_cache] 缓存超过 {_CACHE_MAX_ENTRIES} 条，按 ts 淘汰最旧 {len(victims)} 条（累计 {_EVICTED_TOTAL}）")
    return len(victims)


def _fetch_dedup(symbol: str, price: float, range_: str, key: str) -> dict:
    """[v3.89 #35] 同一 key 的在途请求合并；返回 {} 表示本次没拿到数据。"""
    with _CACHE_LOCK:
        fut = _INFLIGHT.get(key)
        owner = fut is None
        if owner:
            fut = concurrent.futures.Future()
            _INFLIGHT[key] = fut
    if not owner:
        try:
            return fut.result(timeout=_INFLIGHT_WAIT) or {}
        except Exception:
            logger.debug(f"[liq_cache] {key} 等待在途请求失败或超时")
            return {}
    data: dict = {}
    try:
        # [v3.89 #38] 30 秒内刚全失败过：直接返回空，不重复拉三个数据源
        with _CACHE_LOCK:
            _failed_at = _FAIL_CACHE.get(key, 0)
        if time.time() - _failed_at < _FAIL_TTL:
            return {}
        data = _fetch_from_clawby(symbol, price, range_=range_)
        if data:
            _store_if_newer(key, data)
            with _CACHE_LOCK:
                _FAIL_CACHE.pop(key, None)
        else:
            with _CACHE_LOCK:
                _FAIL_CACHE[key] = time.time()
        return data
    finally:
        fut.set_result(data)
        with _CACHE_LOCK:
            _INFLIGHT.pop(key, None)


def get_liq_zones_cached(symbol: str, direction: str,
                          current_price: float = 0.0,
                          range_: str = "3d") -> list:
    """
    返回指定方向的清算区列表。

    v3.64 逻辑调整：
    - 缓存未命中（包括重启后空缓存）→ 直接实时拉取，不走降级
    - 三级数据源全失败才返回空列表（对应 sl_degraded=True）
    - 这保证每次调用都能拿到最新清算图，不再因"缓存未预热"导致降级
    - 仓位维护场景（patrol/adjust_sl_tp）调用此函数时同样受益，
      若缓存已过期则自动刷新，确保止损动态跟随最新清算图结构
    """
    cache_key = f"{symbol}:{range_}"
    with _CACHE_LOCK:
        entry = _CACHE.get(cache_key)
        stale = (entry is None) or (time.time() - entry.get("ts", 0) > _CACHE_TTL)

    if stale:
        # 缓存过期或为空：实时拉取，而非直接返回空（v3.64 关键修改）
        price = current_price
        if price <= 0:
            try:
                from shared.binance_api import get_mark_price
                price = float(get_mark_price(symbol).get("markPrice", 0))
            except Exception:
                pass
        data = _fetch_dedup(symbol, price, range_, cache_key)   # [v3.89 #35] 在途合并 + 版本化写回
        if data:
            entry = data
            logger.debug(f"[liq_cache] {symbol}:{range_} 实时拉取成功，写入缓存")
        else:
            # 三级数据源全部失败，才真正返回空（对应 sl_degraded）
            # 不写入缓存，下次调用继续尝试实时拉取
            logger.debug(f"[liq_cache] {symbol}:{range_} 三级数据源全失败，本次返回空")
            return []

    return (entry or {}).get(direction.upper(), [])


def _prefetch_one(sym: str) -> bool:
    """预热单个标的的清算区缓存；返回是否写入了至少一个 range。"""
    try:
        # [v3.89 #36] 先判断是否需要刷新，缓存仍新鲜的标的不再白白请求一次 mark price
        needs_refresh = False
        for r in _PREFETCH_RANGES:
            with _CACHE_LOCK:
                entry = _CACHE.get(f"{sym}:{r}")
                ts = entry.get("ts", 0) if entry else 0
            if time.time() - ts >= _CACHE_TTL * 0.8:
                needs_refresh = True
                break
        if not needs_refresh:
            return False

        from shared.binance_api import get_mark_price
        price = float(get_mark_price(sym).get("markPrice", 0))
        if price <= 0:
            return False

        wrote = 0
        for r in _PREFETCH_RANGES:
            data = _fetch_dedup(sym, price, r, f"{sym}:{r}")   # [v3.89 #35]
            if data:
                wrote += 1
                # BYK 数据不区分 range，三个 range 共享，避免重复请求
                if data.get("LONG") and any(
                    z.get("source") == "byk_real_level"
                    for z in (data.get("LONG") or [])[:1]
                ):
                    for other_r in _PREFETCH_RANGES:
                        if other_r != r:
                            _store_if_newer(f"{sym}:{other_r}", data)
                    wrote = len(_PREFETCH_RANGES)
                    break

        if wrote > 0:
            logger.debug(f"[liq_cache] {sym} 预热写入 {wrote}/3 个 range key")
        time.sleep(0.5)   # 每个工作线程内保持 0.5s 节流
        return wrote > 0
    except Exception as e:
        logger.debug(f"[liq_cache] {sym} 预热失败: {e}")
        return False


def prefetch_liq_cache(symbols: list) -> None:
    """
    后台批量预热缓存（每 15 分钟由 main.py 触发）。
    [perf 4-B] 固定 5 个工作线程的线程池处理全部标的，线程数不随标的数增长。
    """
    from concurrent.futures import ThreadPoolExecutor
    logger.info(f"[liq_cache] 开始预热 {len(symbols)} 个标的的清算区缓存")
    with ThreadPoolExecutor(max_workers=5, thread_name_prefix="liq-prefetch") as pool:
        results = list(pool.map(_prefetch_one, symbols))
    updated = sum(1 for x in results if x)
    logger.info(f"[liq_cache] 预热完成，共 {len(symbols)} 个标的，更新 {updated} 个")


_bg_stop = threading.Event()
_bg_thread = None


def start_bg_refresh(symbols_fn) -> None:
    """#39 启动可停止的后台刷新线程；启动失败恢复状态。"""
    global _bg_started, _bg_thread
    with _bg_lock:
        if _bg_started:
            return
        _bg_stop.clear()
        def _loop():
            global _bg_started
            try:
                while not _bg_stop.is_set():
                    try:
                        syms = symbols_fn() or []
                        if syms and not _bg_stop.is_set():
                            prefetch_liq_cache(syms)
                    except Exception as e:
                        logger.warning(f"[liq_cache] bg_refresh 异常: {e}")
                    if _bg_stop.wait(_CACHE_TTL):
                        break
            finally:
                with _bg_lock:
                    _bg_started = False
        _bg_thread = threading.Thread(target=_loop, name="liq-cache-bg", daemon=True)
        _bg_started = True
        try:
            _bg_thread.start()
        except Exception:
            _bg_started = False
            _bg_thread = None
            raise
    logger.info("[liq_cache] 后台刷新线程已启动")


def stop_bg_refresh(timeout: float = 5.0) -> bool:
    """#39 取消等待并有界 join；在途同步请求不能强行取消，超时返回 False。"""
    _bg_stop.set()
    with _bg_lock:
        thread = _bg_thread
    if thread and thread is not threading.current_thread():
        thread.join(timeout=max(0.0, timeout))
    stopped = thread is None or not thread.is_alive()
    if not stopped:
        logger.warning("[liq_cache] 后台刷新仍有在途请求，停止等待超时")
    return stopped



def get_liq_src_level(symbol: str, range_: str = None) -> int:
    """[v3.88 OPT-14g] 返回该标的当前缓存中清算图的最差数据来源级别：
    1=Coinglass heatmap（主路径） / 2=ByKaranteli（二级降级） / 3=Coinglass liq_orders（三级降级）。
    无缓存或未记录来源时返回 0（视为未知，不触发降级回退）。只读，不触发网络请求。
    [v3.89 #37] 新增：
      · 超过 2×TTL 的缓存记录视为过期，不参与来源判定（旧记录的降级标记不再污染本次判断）；
      · range_ 可选：指定时只看 "{symbol}:{range_}" 这一份数据，不指定时行为同旧版（取各 range 最大值）。
    读取异常时返回 0，并记录 warning（不再静默）。"""
    lvl = 0
    try:
        now = time.time()
        with _CACHE_LOCK:
            for k, v in _CACHE.items():
                if not k.startswith(f"{symbol}:"):
                    continue
                if range_ is not None and k != f"{symbol}:{range_}":
                    continue
                if now - (v or {}).get("ts", 0) > _CACHE_TTL * 2:
                    continue
                lvl = max(lvl, int((v or {}).get("src_level", 0) or 0))
    except Exception as e:
        logger.warning(f"[liq_cache] get_liq_src_level({symbol}) 读取失败，按未知(0)处理: {e}")
        return 0
    return lvl
