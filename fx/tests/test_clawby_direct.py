"""Coinglass / Clawby 请求强制直连：即使环境里配了 BINANCE_PROXY 也不带 proxies。全部 mock，无网络。"""
import importlib


def test_clawby_proxies_empty_even_if_binance_proxy_set(monkeypatch):
    monkeypatch.setenv("BINANCE_PROXY", "socks5://u:secret@1.2.3.4:6688")
    from shared import clawby_adapter as C
    C = importlib.reload(C)
    assert C._PROXIES == {}


def test_requests_use_empty_proxies(monkeypatch):
    from shared import clawby_adapter as C
    seen = {}

    class R:
        status_code = 200
        text = "{}"
        def raise_for_status(self): pass
        def json(self): return {}

    def fake_get(url, **kw):
        seen["proxies"] = kw.get("proxies")
        return R()

    monkeypatch.setattr(C.requests, "get", fake_get)
    try:
        C._bn_get("/fapi/v1/ping")
    except Exception:
        pass
    assert seen.get("proxies") == {}
