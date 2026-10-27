"""退出兜底：收到 SIGTERM/SIGINT 的瞬间就武装「N 秒后强制退出」，不依赖事件循环。

旧实现的兜底计时器是在 `await stop_event.wait()` 返回之后才创建的，而 stop_event 只能由事件循环里的信号回调置位；
事件循环被卡住 >15s 时，信号回调跑不起来 → 计时器根本没创建 → daemon 15s 后 SIGKILL。
现在：专用线程预先启动，等 `request()`；Python 层信号处理器（主线程，字节码边界即可运行，不需要事件循环）
链式调用 request()，再调用原来的处理器（loop.add_signal_handler 装的那个），原有优雅退出路径不变。
"""
from __future__ import annotations

import signal
import threading

_requested = threading.Event()
_cancel = threading.Event()
_thread = None


def request() -> None:
    """武装兜底（幂等）。只做 Event.set，可在信号处理器里安全调用。"""
    _requested.set()


def cancel() -> None:
    _cancel.set()


def reset_for_tests() -> None:
    global _thread
    _requested.clear()
    _cancel.clear()
    _thread = None


def start(timeout_fn, on_fire) -> None:
    """启动兜底线程（只启动一次）。timeout_fn() 返回武装后的等待秒数；到点且未取消 → on_fire()。"""
    global _thread
    if _thread is not None:
        return

    def _run():
        _requested.wait()
        if _cancel.wait(float(timeout_fn())):
            return
        try:
            on_fire()
        finally:
            pass

    _thread = threading.Thread(target=_run, name="stop-backstop", daemon=True)
    _thread.start()


def chain_signal_handlers(sigs) -> None:
    """在已有处理器之前插入 request()；必须在 loop.add_signal_handler 之后、主线程调用。"""
    for sig in sigs:
        try:
            prev = signal.getsignal(sig)

            def _h(signum, frame, _prev=prev):
                request()
                if callable(_prev):
                    _prev(signum, frame)

            signal.signal(sig, _h)
        except (ValueError, OSError, RuntimeError):
            pass     # 非主线程 / 平台不支持：保持原处理器
