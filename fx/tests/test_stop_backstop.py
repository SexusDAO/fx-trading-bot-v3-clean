"""退出兜底线程：信号一到就武装，不依赖事件循环；未武装不触发；可取消；信号处理器链保留原处理器。全部本地、无网络。"""
import signal
import threading

import pytest

from shared import stop_backstop as SB


@pytest.fixture(autouse=True)
def _reset():
    SB.reset_for_tests()
    yield
    SB.cancel()
    SB.reset_for_tests()


def test_fires_after_request_without_event_loop():
    fired = threading.Event()
    SB.start(lambda: 0.05, fired.set)
    assert not fired.wait(0.2)                      # 未武装：不触发
    SB.request()
    assert fired.wait(2.0)                          # 武装后到点触发（全程没有事件循环）


def test_cancel_prevents_fire():
    fired = threading.Event()
    SB.start(lambda: 0.3, fired.set)
    SB.request()
    SB.cancel()
    assert not fired.wait(0.6)


def test_start_is_idempotent():
    calls = []
    SB.start(lambda: 0.05, lambda: calls.append(1))
    SB.start(lambda: 0.05, lambda: calls.append(2))
    SB.request()
    threading.Event().wait(0.4)
    assert calls == [1]


def test_signal_chain_arms_backstop_and_calls_previous_handler():
    seen = []
    old = signal.signal(signal.SIGUSR2, lambda s, f: seen.append("prev"))
    try:
        SB.chain_signal_handlers((signal.SIGUSR2,))
        assert not SB._requested.is_set()
        signal.raise_signal(signal.SIGUSR2)
        assert SB._requested.is_set() and seen == ["prev"]
    finally:
        signal.signal(signal.SIGUSR2, old)


def test_signal_chain_with_default_handler_does_not_raise():
    old = signal.signal(signal.SIGUSR2, signal.SIG_IGN)
    try:
        SB.chain_signal_handlers((signal.SIGUSR2,))
        signal.raise_signal(signal.SIGUSR2)
        assert SB._requested.is_set()
    finally:
        signal.signal(signal.SIGUSR2, old)


def test_backstop_fire_calls_hard_exit(monkeypatch):
    import main as M
    got = []
    monkeypatch.setattr(M, "_hard_exit", lambda code=0: got.append(code))
    M._backstop_fire()
    assert got == [0]
    assert M._STOP_BACKSTOP_SEC < 15            # 必须早于 daemon 的 SIGKILL
