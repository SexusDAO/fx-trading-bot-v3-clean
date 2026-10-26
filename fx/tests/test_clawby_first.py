"""Clawby 优先 + CG 兜底（CVD / 爆仓历史）与内层 code 拆壳。全部 mock，不联网。"""
import pytest
import shared.clawby_adapter as C


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    C._CACHE.clear() if hasattr(C, "_CACHE") else None
    monkeypatch.setattr(C, "_cache_get", lambda k: None)
    monkeypatch.setattr(C, "_cache_set", lambda k, v: None)
    yield


# ── _unwrap_clawby ──
def test_unwrap_ok_nested():
    assert C._unwrap_clawby({"source": "x", "data": {"code": "0", "data": [1, 2]}, "credits": {}}) == [1, 2]


def test_unwrap_inner_code_nonzero_is_none():
    assert C._unwrap_clawby({"data": {"code": "400", "msg": "Required String parameter"}}) is None
    assert C._unwrap_clawby({"data": {"code": "401", "msg": "Upgrade plan"}}) is None
    assert C._unwrap_clawby({"data": {"code": "400", "msg": "bad", "data": None}}) is None


def test_unwrap_null_data_and_non_dict():
    assert C._unwrap_clawby({"data": {"code": "0", "data": None}}) is None
    assert C._unwrap_clawby([1, 2]) is None
    assert C._unwrap_clawby(None) is None


def test_unwrap_flat_error_object_is_none():
    assert C._unwrap_clawby({"data": {"code": "500", "msg": "boom"}}) is None


# ── get_cvd_history ──
CVD_ROWS = [{"time": 1, "taker_buy_vol": "10.5", "taker_sell_vol": "4", "cum_vol_delta": "6.5"}]


def test_cvd_clawby_first_and_float(monkeypatch):
    calls = []
    monkeypatch.setattr(C, "clawby_relay", lambda name, params=None, **k: calls.append((name, params, k)) or CVD_ROWS)
    monkeypatch.setattr(C, "cg_get", lambda *a, **k: (_ for _ in ()).throw(AssertionError("Clawby 成功时不应调 CG")))
    r = C.get_cvd_history("ARB", "Binance", "1h", 5)
    assert calls[0][0] == "futures_cvd_history"
    assert calls[0][1] == {"symbol": "ARBUSDT", "exchange": "Binance", "interval": "1h", "limit": 5}
    assert calls[0][2].get("_max_retries") == 0
    assert r == [{"time": 1, "taker_buy_vol": 10.5, "taker_sell_vol": 4.0, "cum_vol_delta": 6.5}]


@pytest.mark.parametrize("relay_ret", [None, [], {"code": "400"}, [{"taker_buy_vol": "abc"}]])
def test_cvd_fallback_to_cg(monkeypatch, relay_ret):
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: relay_ret)
    seen = []
    monkeypatch.setattr(C, "cg_get", lambda path, params=None, **k: seen.append((path, params)) or CVD_ROWS)
    r = C.get_cvd_history("ARBUSDT")
    assert seen[0][0] == "api/futures/cvd/history" and seen[0][1]["symbol"] == "ARBUSDT"
    assert r[0]["cum_vol_delta"] == 6.5


def test_cvd_relay_exception_falls_back(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("429")
    monkeypatch.setattr(C, "clawby_relay", boom)
    monkeypatch.setattr(C, "cg_get", lambda *a, **k: CVD_ROWS)
    assert C.get_cvd_history("ARB")[0]["taker_buy_vol"] == 10.5


def test_cvd_both_fail_returns_empty_list(monkeypatch):
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: None)
    monkeypatch.setattr(C, "cg_get", lambda *a, **k: None)
    assert C.get_cvd_history("ARB") == []


def test_get_cvd_compat_entry(monkeypatch):
    monkeypatch.setattr(C, "get_cvd_history", lambda *a, **k: ["ok"])
    assert C.get_cvd("ARB") == ["ok"]


def test_cvd_cache_written(monkeypatch):
    store = {}
    monkeypatch.setattr(C, "_cache_get", lambda k: store.get(k))
    monkeypatch.setattr(C, "_cache_set", lambda k, v: store.__setitem__(k, v))
    n = []
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: n.append(1) or CVD_ROWS)
    C.get_cvd_history("ARB"); C.get_cvd_history("ARB")
    assert len(n) == 1 and "cvd:ARBUSDT:1h" in store
