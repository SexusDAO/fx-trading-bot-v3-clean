"""双通道自适应路由：选路决策、连续失败阈值、REST 层接入、写请求不重复发送、无代理时行为不变。全部 mock。"""
import asyncio
import logging

import pytest

from shared import route_selector as R
from shared import binance_api as A
from shared.ws_proxy import ProxyFallback

P = "socks5://user:secret@1.2.3.4:6688"


@pytest.fixture
def rt(monkeypatch):
    monkeypatch.setattr(R, "PROXY", P)
    monkeypatch.setattr(R, "_active_route", "proxy")
    monkeypatch.setattr(R, "_fail_streak", {"proxy": 0, "direct": 0})
    yield R


# ── decide 纯函数 ──
def test_decide_startup_picks_faster():
    assert R.decide(0.38, 0.65, "proxy", startup=True) == "proxy"
    assert R.decide(0.9, 0.3, "proxy", startup=True) == "direct"


def test_decide_one_side_failed():
    assert R.decide(None, 0.5, "proxy") == "direct"
    assert R.decide(0.5, None, "direct") == "proxy"
    assert R.decide(None, None, "direct") == "direct"


def test_decide_switch_needs_ratio_hysteresis():
    assert R.decide(0.9, 0.5, "proxy", ratio=2.0) == "proxy"      # 慢不到 2 倍 → 保持
    assert R.decide(1.2, 0.3, "proxy", ratio=2.0) == "direct"     # 慢 4 倍 → 切换
    assert R.decide(0.3, 1.2, "direct", ratio=2.0) == "proxy"


# ── 探测 / 监控 ──
def test_startup_probe_selects_and_logs_masked(rt, monkeypatch, caplog):
    monkeypatch.setattr(R, "probe_once", lambda route, timeout=(3, 5): 0.38 if route == "proxy" else 0.65)
    with caplog.at_level(logging.INFO, logger="route_selector"):
        r = asyncio.run(R.probe_routes(startup=True))
    assert r == "proxy" and "启动探测" in caplog.text and "选代理" in caplog.text
    assert "secret" not in caplog.text and "1.2.3.4:6688" in caplog.text


def test_periodic_probe_switches_when_slower_than_2x(rt, monkeypatch, caplog):
    monkeypatch.setattr(R, "probe_once", lambda route, timeout=(3, 5): 1.2 if route == "proxy" else 0.3)
    with caplog.at_level(logging.WARNING, logger="route_selector"):
        assert asyncio.run(R.probe_routes(startup=False)) == "direct"
    assert "切换通道 proxy → direct" in caplog.text


def test_no_proxy_never_probes(monkeypatch):
    monkeypatch.setattr(R, "PROXY", "")
    monkeypatch.setattr(R, "_active_route", "direct")
    monkeypatch.setattr(R, "probe_once", lambda *a, **k: (_ for _ in ()).throw(AssertionError("不应探测")))
    assert asyncio.run(R.probe_routes()) == "direct"
    assert R.get_proxy_for_request() == {} and R.ws_route() == "proxy"


def test_get_proxy_for_request_follows_route(rt):
    assert R.get_proxy_for_request() == {"http": P, "https": P}
    R._set_route("direct", "t")
    assert R.get_proxy_for_request() == {}


def test_three_consecutive_failures_needed(rt):
    assert R.record_failure("proxy") is False and R.record_failure("proxy") is False
    R.record_success("proxy")                                       # 成功清零
    assert R.record_failure("proxy") is False and R.record_failure("proxy") is False
    assert R.active_route() == "proxy"
    assert R.record_failure("proxy") is True and R.active_route() == "direct"


def test_failure_of_non_active_route_ignored(rt):
    for _ in range(5):
        assert R.record_failure("direct") is False
    assert R.active_route() == "proxy"


# ── WS 接入 ──
def test_ws_follows_route_to_direct_even_if_handshake_ok(rt):
    fb = ProxyFallback(P, name="t", route_fn=R.ws_route)
    assert fb.current() == P
    R._set_route("direct", "t")
    assert fb.current() is None                                      # 主动切换 → 直连
    R._set_route("proxy", "t")
    assert fb.current() == P


# ── REST 接入 ──
class Resp:
    def __init__(self, status=200, data=None):
        self.status_code, self._d, self.ok = status, data if data is not None else {}, status < 400
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")
    def json(self):
        return self._d


class Sess:
    def __init__(self, tag, log, fail=None):
        self.tag, self.log, self.fail = tag, log, fail
    def _do(self, verb, *a, **k):
        self.log.append((self.tag, verb))
        if self.fail:
            raise self.fail
        return Resp(200, {"ok": self.tag})
    def get(self, *a, **k): return self._do("get", *a, **k)
    def post(self, *a, **k): return self._do("post", *a, **k)
    def delete(self, *a, **k): return self._do("delete", *a, **k)
    def put(self, *a, **k): return self._do("put", *a, **k)


@pytest.fixture
def api(rt, monkeypatch):
    log = []
    monkeypatch.setattr(A, "API_KEY", "k"); monkeypatch.setattr(A, "API_SECRET", "s")
    monkeypatch.setattr(A, "_418_COOLDOWN_UNTIL", 0.0)
    monkeypatch.setattr(A, "_DIRECT_DISABLED", False)
    monkeypatch.setattr(A, "reset_session", lambda: None)
    monkeypatch.setattr(A.time, "sleep", lambda s: None)
    return log


def install(monkeypatch, log, proxy_fail=None, direct_fail=None):
    monkeypatch.setattr(A, "_SESSION", Sess("proxy", log, proxy_fail))
    monkeypatch.setattr(A, "_SESSION_DIRECT", Sess("direct", log, direct_fail))


def test_signed_get_uses_active_route(api, monkeypatch):
    install(monkeypatch, api)
    assert A._signed_get("/x", {})["ok"] == "proxy"
    R._set_route("direct", "t")
    assert A._signed_get("/x", {})["ok"] == "direct"


def test_signed_request_falls_back_immediately_on_proxy_connection_error(api, monkeypatch):
    install(monkeypatch, api, proxy_fail=RuntimeError("SOCKS5 Connection refused"))
    res = A._retry(lambda: A._signed_get("/x", {}))
    assert res["ok"] == "direct"                                     # 本次请求立即用直连重试
    assert [t for t, _ in api] == ["proxy", "direct"]
    assert R.active_route() == "proxy"                               # 仅 1 次失败，全局通道不变


def test_write_timeout_after_send_is_not_resent_on_other_route(api, monkeypatch):
    install(monkeypatch, api, proxy_fail=RuntimeError("Read timed out"))
    with pytest.raises(A.WriteTimeoutError):
        A._retry(lambda: A._signed_post("/fapi/v1/order", {}), is_write=True)
    assert [t for t, _ in api] == ["proxy"]                          # 结果未知 → 不换通道重发，避免重复下单


def test_write_not_sent_error_may_switch_route(api, monkeypatch):
    install(monkeypatch, api, proxy_fail=RuntimeError("NewConnectionError: Failed to establish a new connection"))
    res = A._retry(lambda: A._signed_post("/fapi/v1/order", {}), is_write=True)
    assert res["ok"] == "direct"


def test_override_cleared_after_call(api, monkeypatch):
    install(monkeypatch, api, proxy_fail=RuntimeError("SOCKS5 Connection refused"))
    A._retry(lambda: A._signed_get("/x", {}))
    assert getattr(A._ROUTE_TLS, "ov", None) is None


def test_public_get_proxy_first_then_direct_fallback(api, monkeypatch):
    install(monkeypatch, api, proxy_fail=RuntimeError("boom"))
    assert A._get("/p")["ok"] == "direct"
    assert [t for t, _ in api] == ["proxy", "direct"]


def test_public_get_unchanged_without_proxy(monkeypatch):
    monkeypatch.setattr(R, "PROXY", ""); monkeypatch.setattr(R, "_active_route", "direct")
    log = []
    monkeypatch.setattr(A, "_418_COOLDOWN_UNTIL", 0.0)
    monkeypatch.setattr(A, "_DIRECT_DISABLED", False)
    install(monkeypatch, log)
    assert A._get("/p")["ok"] == "direct" and [t for t, _ in log] == ["direct"]
    assert A._signed_session() is A._SESSION                         # 未配代理：签名仍沿用 _SESSION（旧行为）


def test_klines_proxy_first_when_route_proxy(api, monkeypatch):
    install(monkeypatch, api)
    monkeypatch.setattr(A, "_record_api_outcome", lambda ok: None)
    A.get_klines("BTCUSDT", "1h", 2)                                 # 假会话返回 dict（非 list）→ 结果为 []，这里只验证先走哪条通道
    assert api and api[0][0] == "proxy"
    api.clear()
    R._set_route("direct", "t")
    A.get_klines("BTCUSDT", "1h", 2)
    assert api and api[0][0] == "direct"
