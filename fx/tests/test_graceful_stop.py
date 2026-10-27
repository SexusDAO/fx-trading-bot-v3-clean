"""优雅退出：停止流程总时限 + 超时强退（os._exit，不依赖 SystemExit）。全部 mock，不真正退出进程。"""
import asyncio
import inspect

import main as M


def test_fast_stop_returns_true_and_no_hard_exit(monkeypatch):
    called = []
    monkeypatch.setattr(M, "_hard_exit", lambda code=0: called.append(code))

    async def quick():
        await asyncio.sleep(0)

    assert asyncio.run(M._run_stop_sequence(quick(), timeout=1.0)) is True
    assert called == []


def test_slow_stop_times_out_and_hard_exits(monkeypatch):
    called = []
    monkeypatch.setattr(M, "_hard_exit", lambda code=0: called.append(code))

    async def slow():
        await asyncio.sleep(5)

    assert asyncio.run(M._run_stop_sequence(slow(), timeout=0.1)) is False
    assert called == [0]


def test_hard_exit_uses_os_exit_and_cleans_up(monkeypatch):
    seen = {}
    monkeypatch.setattr(M.os, "_exit", lambda c: seen.setdefault("code", c))
    monkeypatch.setattr(M, "_cleanup_main_pid", lambda: seen.setdefault("pid", True))

    class L:
        def stop(self):
            seen["listener"] = True
    monkeypatch.setattr(M, "_queue_listener", L())
    M._hard_exit(0)
    assert seen == {"pid": True, "listener": True, "code": 0}


def test_hard_exit_still_exits_when_cleanup_raises(monkeypatch):
    seen = {}
    monkeypatch.setattr(M.os, "_exit", lambda c: seen.setdefault("code", c))

    def boom():
        raise RuntimeError("x")
    monkeypatch.setattr(M, "_cleanup_main_pid", boom)

    class L:
        def stop(self):
            raise RuntimeError("y")
    monkeypatch.setattr(M, "_queue_listener", L())
    M._hard_exit(3)
    assert seen == {"code": 3}


def test_main_stop_sequence_wiring():
    src = inspect.getsource(M.main)
    assert "_SCAN_POOL.shutdown(wait=False, cancel_futures=True)" in src
    assert "_run_stop_sequence(_stop_all())" in src
    # 扫描线程池关闭发生在停止流程之前
    assert src.index("_SCAN_POOL.shutdown") < src.index("async def _stop_all")
    assert M._STOP_TIMEOUT_SEC == 10.0 and M._STOP_BACKSTOP_SEC > M._STOP_TIMEOUT_SEC
    assert "add_signal_handler(sig, _handle_signal)" in src and "signal.SIGTERM" in src
