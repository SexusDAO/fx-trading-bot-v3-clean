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
