"""get_klines 总时限/重试/超时参数（全 mock，不联网）。"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest
from shared import binance_api as A


class _Resp:
    def __init__(self, data): self._d = data
    def raise_for_status(self): pass
    def json(self): return self._d


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setattr(A, "_DIRECT_DISABLED", False)
    monkeypatch.setattr(A, "_record_api_outcome", lambda ok: None)


def _sess(monkeypatch, fn):
    class S:
        def get(self, url, params=None, timeout=None): return fn(timeout)
    monkeypatch.setattr(A, "_SESSION_DIRECT", S()); monkeypatch.setattr(A, "_SESSION", S())


def test_timeout_tuple_passed_and_success(monkeypatch):
    seen = []
    _sess(monkeypatch, lambda t: seen.append(t) or _Resp([[1]]))
    assert A.get_klines("XUSDT", "1h", 100) == [[1]]
    assert seen[0][0] <= 3 and seen[0][1] <= 5


def test_total_budget_bounds_all_retries(monkeypatch):
    """每次请求都耗尽自己的超时：总耗时（含重试、直连转代理）不得超过总时限。"""
    clock = [0.0]
    monkeypatch.setattr(A.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(A.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    calls = []
    def fn(t):
        calls.append(t); clock[0] += t[0] + t[1]      # 模拟连接+读取都用满
        raise TimeoutError("timeout")
    _sess(monkeypatch, fn)
    assert A.get_klines("XUSDT", "1h", 100) == []
    assert clock[0] <= 8.0 + 1e-6
    assert len(calls) <= 4                            # (1+1次重试) × (直连+代理)


def test_retry_once_then_success(monkeypatch):
    n = []
    def fn(t):
        n.append(1)
        if len(n) == 1: raise TimeoutError("timeout")
        return _Resp([[2]])
    monkeypatch.setattr(A.time, "sleep", lambda s: None)
    _sess(monkeypatch, fn)
    assert A.get_klines("XUSDT", "1h", 100) == [[2]]


def test_retry_count_capped(monkeypatch):
    import config
    monkeypatch.setattr(config, "KLINE_REQUEST_MAX_RETRY", 0, raising=False)
    n = []
    def fn(t): n.append(1); raise TimeoutError("timeout")
    monkeypatch.setattr(A, "_DIRECT_DISABLED", True)
    _sess(monkeypatch, fn)
    assert A.get_klines("XUSDT", "1h", 100) == [] and len(n) == 1


def test_retry_sleep_not_over_half_second(monkeypatch):
    sl = []
    monkeypatch.setattr(A.time, "sleep", lambda s: sl.append(s))
    def fn(t): raise ConnectionError("x")
    _sess(monkeypatch, fn)
    A.get_klines("XUSDT", "1h", 100)
    assert sl and max(sl) <= 0.5


def test_other_endpoints_no_longer_use_timeout_10():
    src = (Path(__file__).parent.parent / "shared" / "binance_api.py").read_text()
    assert "timeout=10" not in src
    body = src[src.index("def get_klines("):src.index("def get_ticker(")]
    assert "_call" not in body and "_retry(" not in body
