"""
阶段1：扫描分析进程池（SCAN_USE_PROCESS_POOL=True 时由 scanner.run_scan 使用）。

- spawn 方式的全局单例 ProcessPoolExecutor；worker 数 = min(SCAN_MAX_WORKERS, cpu_count(), 3)
- worker 进程启动时预导入 scanner / judges，并：
    · judges._set_cooldown_safe 换成「记录」函数——冷却写入不在 worker 里做，以 {"kind": "cooldown"} 事件随结果传回主进程
    · liq_cache.get_liq_zones_cached 换成「从 prefetch 快照查找」——worker 内不发任何清算区网络请求
    · 日志通过共享 multiprocessing.Queue 的 QueueHandler 传回主进程，由 QueueListener（后台线程）统一输出
- 入口 analyze_task 返回 (signal | None, events)，events 只含可序列化的 dict
- 主进程侧：get_healthy_pool（健康检查 + BrokenProcessPool 后自动重建）、reset_pool（整轮超时强制终止）

spawn 子进程默认会重新导入主模块（main.py 有 pid 文件、tracemalloc 等模块级副作用），
所以创建进程池前给 multiprocessing.spawn.get_preparation_data 打补丁，去掉 init_main_* 字段（worker 只需要 sys.path）。
"""
from __future__ import annotations

import logging
import logging.handlers
import multiprocessing as mp
import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor

logger = logging.getLogger("analysis_worker")

HEALTH_TIMEOUT_SEC = 60.0      # 健康检查（含首次 spawn + 导入）的最长等待
_MAX_WORKERS_CAP = 3

# ═════════════════════ worker 进程内 ═════════════════════
_W = {"symbol": None, "liq": {}, "cooldowns": []}


def _record_cooldown(symbol, hours, reason) -> None:
    """替换 judges._set_cooldown_safe：只记录，不写 state（由主进程统一写）。"""
    _W["cooldowns"].append({"kind": "cooldown", "symbol": symbol, "hours": hours, "reason": reason})


def _liq_from_snapshot(symbol, direction, current_price=0.0, range_="3d"):
    """替换 liq_cache.get_liq_zones_cached：只查本任务 prefetch 带来的快照，没有就返回 []，绝不联网。"""
    if symbol != _W["symbol"]:
        return []
    return list(_W["liq"].get((str(direction).upper(), range_), []))


def _init_worker(log_queue, level) -> None:
    root = logging.getLogger()
    root.handlers[:] = [logging.handlers.QueueHandler(log_queue)]
    root.setLevel(level)
    import scanner  # noqa: F401  预导入（含全部 judge 模块）
    from shared import judges, liq_cache
    judges._set_cooldown_safe = _record_cooldown
    liq_cache.get_liq_zones_cached = _liq_from_snapshot


def _ping(delay: float = 0.0) -> int:
    if delay:
        time.sleep(delay)
    return os.getpid()


def _selftest_probe() -> dict:       # 仅供测试：确认 worker 内的替换与模块状态
    import sys
    from shared import judges, liq_cache
    _W["cooldowns"] = []
    judges._set_cooldown_safe("PROBEUSDT", 4, "crash_guard")
    _W["symbol"], _W["liq"] = "PROBEUSDT", {("LONG", "3d"): [{"p": 1.0}]}
    return {"cooldown_fn": judges._set_cooldown_safe.__name__, "liq_fn": liq_cache.get_liq_zones_cached.__name__,
            "cooldowns": list(_W["cooldowns"]),
            "liq_hit": liq_cache.get_liq_zones_cached("PROBEUSDT", "LONG", 1.0, range_="3d"),
            "liq_other": liq_cache.get_liq_zones_cached("OTHERUSDT", "LONG", 1.0, range_="3d"),
            "liq_miss": liq_cache.get_liq_zones_cached("PROBEUSDT", "SHORT", 1.0, range_="24h"),
            "main_imported": ("main" in sys.modules) or (getattr(sys.modules.get("__mp_main__"), "__file__", None) is not None),
            "root_handlers": [type(h).__name__ for h in logging.getLogger().handlers]}


def _selftest_log() -> None:         # 仅供测试：在 worker 里打一条日志
    logging.getLogger("analysis_worker.selftest").warning("hello-from-worker")


def _selftest_crash() -> None:       # 仅供测试：让 worker 异常退出，触发 BrokenProcessPool
    os._exit(1)


def _selftest_sleep(sec: float) -> int:   # 仅供测试：模拟 worker 卡住
    time.sleep(sec)
    return os.getpid()


def analyze_task(symbol, ticker, k15, k1h, k4h, prefetch, active_syms, pending_syms,
                 allowed_dirs, loss_blacklist, btc_trend, market_mode, mark_price_map):
    """worker 入口：调用 scanner.analyze_one，返回 (signal | None, events)。"""
    import scanner
    liq = (prefetch or {}).get("liq_zones") or {}
    _W["symbol"] = symbol
    _W["liq"] = {("LONG", "3d"): liq.get("long", []), ("SHORT", "3d"): liq.get("short", []),
                 ("LONG", "24h"): liq.get("long_24h", []), ("SHORT", "24h"): liq.get("short_24h", [])}
    _W["cooldowns"] = []
    events: list = []
    sig = scanner.analyze_one(symbol, ticker, k15, k1h, k4h, active_syms, pending_syms, allowed_dirs,
                              loss_blacklist, btc_trend, market_mode, mark_price_map, prefetch, events)
    events.extend(_W["cooldowns"])
    return sig, events


# ═════════════════════ 主进程侧 ═════════════════════
_lock = threading.Lock()
_state = {"pool": None, "queue": None, "listener": None}


class _ReEmit(logging.Handler):
    """QueueListener 的目标：把 worker 传来的 LogRecord 交给主进程同名 logger 输出。"""
    def emit(self, record):
        try:
            logging.getLogger(record.name).handle(record)
        except Exception:
            pass


def _patch_spawn_no_main() -> None:
    import multiprocessing.spawn as sp
    if getattr(sp.get_preparation_data, "_fx_nomain", False):
        return
    orig = sp.get_preparation_data

    def gpd(name):
        d = orig(name)
        d.pop("init_main_from_name", None)
        d.pop("init_main_from_path", None)
        return d
    gpd._fx_nomain = True
    sp.get_preparation_data = gpd


def worker_count() -> int:
    try:
        import config
        base = int(getattr(config, "SCAN_MAX_WORKERS", 4))
    except Exception:
        base = 4
    return max(1, min(base, os.cpu_count() or 1, _MAX_WORKERS_CAP))


def _make_pool():
    _patch_spawn_no_main()
    ctx = mp.get_context("spawn")
    if _state["queue"] is None:
        _state["queue"] = ctx.Queue(-1)
        lst = logging.handlers.QueueListener(_state["queue"], _ReEmit(), respect_handler_level=False)
        lst.start()
        _state["listener"] = lst
    n = worker_count()
    logger.info(f"[pool] 创建分析进程池 workers={n} (spawn)")
    return ProcessPoolExecutor(max_workers=n, mp_context=ctx, initializer=_init_worker,
                               initargs=(_state["queue"], logging.getLogger().getEffectiveLevel()))


def _is_broken(pool) -> bool:
    return bool(getattr(pool, "_broken", False)) or bool(getattr(pool, "_shutdown_thread", False))


def get_pool():
    with _lock:
        p = _state["pool"]
        if p is None or _is_broken(p):
            if p is not None:
                logger.warning("[pool] 进程池已损坏，重建")
                _kill(p)
            _state["pool"] = p = _make_pool()
        return p


def _kill(pool) -> None:
    try:
        for proc in list((getattr(pool, "_processes", None) or {}).values()):
            try:
                proc.terminate()
            except Exception:
                pass
    except Exception:
        pass
    try:
        pool.shutdown(wait=False, cancel_futures=True)
    except Exception:
        pass


def reset_pool(kill: bool = True) -> None:
    """丢弃当前进程池（整轮超时 / 损坏时调用）；下次 get_pool 会重建。"""
    with _lock:
        p, _state["pool"] = _state["pool"], None
    if p is not None:
        _kill(p) if kill else p.shutdown(wait=False, cancel_futures=True)


def get_healthy_pool():
    """返回可用的进程池：先发一个 ping 做健康检查，失败则重建一次再试。"""
    last = None
    for _ in range(2):
        pool = get_pool()
        try:
            pool.submit(_ping).result(timeout=HEALTH_TIMEOUT_SEC)
            return pool
        except Exception as e:
            last = e
            logger.warning(f"[pool] 健康检查失败，重建进程池: {e!r}")
            reset_pool()
    raise RuntimeError(f"进程池不可用: {last!r}")


def warmup(timeout: float = HEALTH_TIMEOUT_SEC) -> bool:
    """启动时预热：让每个 worker 都完成导入。"""
    try:
        pool = get_pool()
        futs = [pool.submit(_ping, 0.3) for _ in range(worker_count())]
        for f in futs:
            f.result(timeout=timeout)
        return True
    except Exception as e:
        logger.warning(f"[pool] 预热失败: {e!r}")
        reset_pool()
        return False


def shutdown() -> None:
    reset_pool()
    lst, _state["listener"] = _state["listener"], None
    if lst is not None:
        try:
            lst.stop()
        except Exception:
            pass
    _state["queue"] = None
