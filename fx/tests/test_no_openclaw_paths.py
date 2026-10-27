"""去 openclaw：liq_cache 告警只走钉钉、daemon notify 只写日志、llm_judge 路径 B 默认关闭。全部 mock，不起子进程。"""
import subprocess
import threading


def _forbid_subprocess(monkeypatch):
    def boom(*a, **k):
        raise AssertionError(f"不应启动子进程: {a[:1]}")
    monkeypatch.setattr(subprocess, "run", boom)
    monkeypatch.setattr(subprocess, "Popen", boom)


def test_liq_cache_alert_goes_to_dingtalk_only(monkeypatch):
    from shared import liq_cache as L
    from shared import notifier as N
    _forbid_subprocess(monkeypatch)
    got, done = [], threading.Event()
    monkeypatch.setattr(N, "dingtalk_notify", lambda text, wait=False: (got.append((text, wait)), done.set()))
    monkeypatch.setattr(L, "_ALERT_LAST", {})
    L._alert_all_failed("ABCUSDT")
    assert done.wait(3.0)
    assert "ABCUSDT" in got[0][0] and "ATR" in got[0][0]


def test_liq_cache_alert_swallows_dingtalk_errors(monkeypatch):
    from shared import liq_cache as L
    from shared import notifier as N
    _forbid_subprocess(monkeypatch)
    done = threading.Event()

    def bad(text, wait=False):
        done.set()
        raise RuntimeError("hook down")

    monkeypatch.setattr(N, "dingtalk_notify", bad)
    monkeypatch.setattr(L, "_ALERT_LAST", {})
    L._alert_all_failed("XYZUSDT")
    assert done.wait(3.0)          # 线程内异常被吞，不向外抛


def test_daemon_notify_only_logs(monkeypatch):
    import daemon_start as D
    _forbid_subprocess(monkeypatch)
    logs = []
    monkeypatch.setattr(D, "log", lambda m: logs.append(m))
    D.notify("hello")
    assert logs == ["hello"] and not hasattr(D, "notify_jarvis")


def test_llm_judge_path_b_disabled(monkeypatch):
    from shared import llm_judge as J
    _forbid_subprocess(monkeypatch)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    assert J._OPENCLAW_AGENT_ENABLED is False
    assert J._call_openclaw_lite("p", 10) is None
    assert J._call_llm_inner("p", 10) is None
