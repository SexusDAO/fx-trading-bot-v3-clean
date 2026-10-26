"""
shared/async_utils.py — 同步调用移出 asyncio 事件循环的统一封装（[v3.89 #61]，#6 复用）

约定：async 函数里凡是同步网络/磁盘调用，一律走 _run_sync(fn, *args, timeout=..., **kwargs)：
  · 使用独立的有界线程池，避免慢请求占满默认执行器；
  · 超时抛 RunSyncTimeout（TimeoutError 子类），调用方按失败处理；
  · 注意：超时只是停止等待，线程里的同步请求无法被强制取消，可能在后台继续完成。
"""
import asyncio
import functools
import logging
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("fx.async_utils")

_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="run-sync")
DEFAULT_TIMEOUT = 15.0


class RunSyncTimeout(TimeoutError):
    """同步调用在限定时间内未返回。"""


async def _run_sync(fn, *args, timeout: float = DEFAULT_TIMEOUT, **kwargs):
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(_POOL, functools.partial(fn, *args, **kwargs))
    try:
        return await asyncio.wait_for(fut, timeout)
    except asyncio.TimeoutError:
        name = getattr(fn, "__name__", repr(fn))
        logger.warning(f"[async_utils] {name} 超过 {timeout}s 未返回，停止等待（后台请求可能仍在执行）")
        raise RunSyncTimeout(f"{name} timeout after {timeout}s") from None


# ── patrol 专用读请求线程池 ─────────────────────────────────────────────────
# patrol 路径（孤单检测/TP 确认/时间止损/补挂/funding_arb 巡检）的 Binance 读请求走这里：
#   · 最多 PATROL_REST_POOL_WORKERS 个线程，慢请求最多占满这个小池，不会拖累默认 executor 上的其他协程；
#   · 等待超时 PATROL_REST_TIMEOUT_SEC（默认 10s）→ 抛 RunSyncTimeout，调用方按「查询失败」处理（原有 except 分支）；
#   · 仅用于读请求；写请求（place_*/cancel_*）保持原样，不进该池，避免被慢读请求排队延迟。
import threading
import time


def _patrol_cfg(name, default):
    try:
        import config as _c
        return getattr(_c, name, default)
    except Exception:
        return default


_PATROL_POOL = ThreadPoolExecutor(max_workers=max(int(_patrol_cfg("PATROL_REST_POOL_WORKERS", 2)), 1),
                                  thread_name_prefix="patrol-rest")
_PATROL_QUEUE_WARN_SEC = 1.0      # 在池内排队超过该秒数记 warning


async def _run_patrol(fn, *args, timeout: float | None = None, **kwargs):
    """在 patrol 专用池里执行同步读请求；超时抛 RunSyncTimeout（TimeoutError 子类）。"""
    if timeout is None:
        timeout = float(_patrol_cfg("PATROL_REST_TIMEOUT_SEC", 10.0))
    loop = asyncio.get_running_loop()
    t_submit = time.monotonic()
    name = getattr(fn, "__name__", repr(fn))

    def _job():
        waited = time.monotonic() - t_submit
        if waited >= _PATROL_QUEUE_WARN_SEC:
            logger.warning(f"[async_utils] patrol 池排队 {waited:.1f}s 才开始执行 {name} "
                           f"thread={threading.current_thread().name}")
        return fn(*args, **kwargs)

    fut = loop.run_in_executor(_PATROL_POOL, _job)
    try:
        return await asyncio.wait_for(fut, timeout)
    except asyncio.TimeoutError:
        logger.warning(f"[async_utils] patrol 读请求 {name} 超过 {timeout}s 未返回，按查询失败处理"
                       f"（后台请求可能仍在执行，占用 patrol 池线程）")
        raise RunSyncTimeout(f"{name} timeout after {timeout}s") from None
