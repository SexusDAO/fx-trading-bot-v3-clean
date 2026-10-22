"""全局 REST 超时/重试/下单超时（全 mock，不联网）。"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest
from shared import binance_api as A


class _R:
    ok = True
    def __init__(self, d=None): self._d = d if d is not None else {}
    def raise_for_status(self): pass
    def json(self): return self._d


def _sessions(monkeypatch, seen):
    class S:
        def _rec(self, kind, url, kw): seen.append((kind, url, kw.get("timeout"))); return _R()
        def get(self, url, **kw): return self._rec("get", url, kw)
        def post(self, url, **kw): return self._rec("post", url, kw)
        def delete(self, url, **kw): return self._rec("delete", url, kw)
    monkeypatch.setattr(A, "_SESSION", S()); monkeypatch.setattr(A, "_SESSION_DIRECT", S())
    monkeypatch.setattr(A, "_DIRECT_DISABLED", False)


def test_public_get_uses_api_timeout(monkeypatch):
    seen = []; _sessions(monkeypatch, seen)
    A._get("/fapi/v1/premiumIndex", {"symbol": "X"})
    assert seen[0][2] == (3, 5)


def test_signed_get_delete_use_api_timeout(monkeypatch):
    seen = []; _sessions(monkeypatch, seen)
    A._signed_get("/fapi/v2/account", {}); A._signed_delete("/fapi/v1/order", {})
    assert [s[2] for s in seen] == [(3, 5), (3, 5)]


@pytest.mark.parametrize("path", ["/fapi/v1/order", "/fapi/v1/batchOrders", "/fapi/v1/algoOrder"])
def test_order_paths_use_order_timeout(monkeypatch, path):
    seen = []; _sessions(monkeypatch, seen)
    A._signed_post(path, {})
    assert seen[0][2] == (5, 10)


@pytest.mark.parametrize("path", ["/fapi/v1/leverage", "/fapi/v1/marginType", "/fapi/v1/listenKey"])
def test_non_order_post_uses_api_timeout(monkeypatch, path):
    seen = []; _sessions(monkeypatch, seen)
    A._signed_post(path, {})
    assert seen[0][2] == (3, 5)


def test_retry_default_is_one_retry(monkeypatch):
    monkeypatch.setattr(A.time, "sleep", lambda s: None)
    monkeypatch.setattr(A, "reset_session", lambda: None)
    monkeypatch.setattr(A, "_record_api_outcome", lambda ok: None)
    n = []
    def f(): n.append(1); raise ValueError("boom")
    assert A._retry(f) is None and len(n) == 2        # 1 次尝试 + 1 次重试


def test_retry_reads_config(monkeypatch):
    import config
    monkeypatch.setattr(config, "API_REQUEST_MAX_RETRY", 0, raising=False)
    monkeypatch.setattr(A.time, "sleep", lambda s: None)
    monkeypatch.setattr(A, "_record_api_outcome", lambda ok: None)
    n = []
    def f(): n.append(1); raise ValueError("boom")
    A._retry(f); assert len(n) == 1


def test_explicit_retries_still_respected(monkeypatch):
    monkeypatch.setattr(A.time, "sleep", lambda s: None)
    monkeypatch.setattr(A, "_record_api_outcome", lambda ok: None)
    n = []
    def f(): n.append(1); raise ValueError("boom")
    A._retry(f, retries=3); assert len(n) == 3
