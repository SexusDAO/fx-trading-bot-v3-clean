"""收到 Binance 418 后全局冷却：冷却期内所有 REST 入口不发请求；非冷却期行为不变。全部 mock，无网络。"""
import logging
import time

import pytest

from shared import binance_api as B


class Resp:
    def __init__(self, status=200, body=None):
        self.status_code = status
        self._body = body if body is not None else []
        self.ok = status < 400

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._body


class Sess:
    def __init__(self, status=200, body=None):
        self.calls = 0
        self.status, self.body = status, body

    def _r(self, *a, **k):
        self.calls += 1
        return Resp(self.status, self.body)
    get = post = delete = _r


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    logging.disable(logging.CRITICAL)
    monkeypatch.setattr(B, "_418_COOLDOWN_UNTIL", 0.0)
    monkeypatch.setattr(B, "_DIRECT_DISABLED", False)
    yield
    logging.disable(logging.NOTSET)


def _sessions(monkeypatch, status=200, body=None):
    d, p = Sess(status, body), Sess(status, body)
    monkeypatch.setattr(B, "_SESSION_DIRECT", d)
    monkeypatch.setattr(B, "_SESSION", p)
    return d, p


def test_check_and_set_cooldown(monkeypatch):
    assert B._check_418_cooldown() == 0
    monkeypatch.setattr(B, "_418_cooldown_sec", lambda: 60.0)
    B._set_418_cooldown()
    assert 55 < B._check_418_cooldown() <= 60


def test_cooldown_only_extends_never_shortens(monkeypatch):
    monkeypatch.setattr(B, "_418_cooldown_sec", lambda: 60.0)
    B._set_418_cooldown()
    first = B._418_COOLDOWN_UNTIL
    monkeypatch.setattr(B, "_418_cooldown_sec", lambda: 1.0)
    B._set_418_cooldown()
    assert B._418_COOLDOWN_UNTIL == first


def test_418_response_triggers_cooldown_and_blocks_next_calls(monkeypatch):
    d, p = _sessions(monkeypatch, status=418)
    monkeypatch.setattr(B, "_418_cooldown_sec", lambda: 60.0)
    with pytest.raises(Exception):
        B._get("/fapi/v1/ticker/24hr", {"symbol": "X"})
    assert B._check_418_cooldown() > 0
    sent = d.calls + p.calls
    with pytest.raises(B.BinanceAPIError) as ei:
        B._get("/fapi/v1/ticker/24hr", {"symbol": "X"})
    assert ei.value.http == 418                       # _retry_impl 对 http=418 本来就不重试
    assert d.calls + p.calls == sent                  # 冷却期内没有再发请求


def test_418_does_not_flip_direct_disabled(monkeypatch):
    _sessions(monkeypatch, status=418)
    with pytest.raises(Exception):
        B._get("/x")
    assert B._DIRECT_DISABLED is False


def test_all_signed_entrypoints_blocked_during_cooldown(monkeypatch):
    d, p = _sessions(monkeypatch)
    monkeypatch.setattr(B, "_418_COOLDOWN_UNTIL", time.time() + 30)
    for fn in (lambda: B._get("/a"), lambda: B._signed_get("/a", {}),
               lambda: B._signed_post("/a", {}), lambda: B._signed_delete("/a", {})):
        with pytest.raises(B.BinanceAPIError):
            fn()
    assert d.calls + p.calls == 0


def test_get_klines_returns_empty_without_request_during_cooldown(monkeypatch):
    d, p = _sessions(monkeypatch, body=[[1]])
    monkeypatch.setattr(B, "_418_COOLDOWN_UNTIL", time.time() + 30)
    assert B.get_klines("BTCUSDT", "1h", 10) == []
    assert d.calls + p.calls == 0


def test_get_klines_418_stops_retry_and_enters_cooldown(monkeypatch):
    d, p = _sessions(monkeypatch, status=418)
    monkeypatch.setattr(B, "_418_cooldown_sec", lambda: 60.0)
    assert B.get_klines("BTCUSDT", "1h", 10) == []
    assert B._check_418_cooldown() > 0
    assert d.calls + p.calls == 1                     # 只发了一次，没有重试、没有切代理再发
    assert B._DIRECT_DISABLED is False


def test_normal_path_unchanged_when_not_in_cooldown(monkeypatch):
    d, p = _sessions(monkeypatch, body={"ok": 1})
    assert B._get("/a") == {"ok": 1}
    assert B._signed_get("/a", {}) == {"ok": 1}
    assert B._check_418_cooldown() == 0


def test_cooldown_expires(monkeypatch):
    monkeypatch.setattr(B, "_418_COOLDOWN_UNTIL", time.time() - 1)
    assert B._check_418_cooldown() == 0
    _sessions(monkeypatch, body={"ok": 1})
    assert B._get("/a") == {"ok": 1}
