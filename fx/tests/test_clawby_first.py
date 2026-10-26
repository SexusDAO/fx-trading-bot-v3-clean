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


# ── get_liquidation_history ──
LIQ_ROWS = [{"time": 1, "long_liquidation_usd": "1234.56", "short_liquidation_usd": "0"}]


def test_liq_history_clawby_first_and_str_to_float(monkeypatch):
    calls = []
    monkeypatch.setattr(C, "clawby_relay", lambda name, params=None, **k: calls.append((name, params, k)) or LIQ_ROWS)
    monkeypatch.setattr(C, "cg_get", lambda *a, **k: (_ for _ in ()).throw(AssertionError("Clawby 成功时不应调 CG")))
    r = C.get_liquidation_history("ARB", "Binance", "1h", 2)
    assert calls[0][0] == "futures_liquidation_history"
    assert calls[0][1] == {"symbol": "ARBUSDT", "exchange": "Binance", "interval": "1h", "limit": 2}
    assert calls[0][2].get("_max_retries") == 0
    assert r == [{"time": 1, "long_liquidation_usd": 1234.56, "short_liquidation_usd": 0.0}]
    assert isinstance(r[0]["long_liquidation_usd"], float)


@pytest.mark.parametrize("relay_ret", [None, [], {"code": "401", "msg": "Upgrade plan"},
                                       [{"long_liquidation_usd": "x", "short_liquidation_usd": "1"}]])
def test_liq_history_fallback_to_cg(monkeypatch, relay_ret):
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: relay_ret)
    seen = []
    monkeypatch.setattr(C, "cg_get", lambda path, params=None, **k: seen.append((path, params)) or LIQ_ROWS)
    r = C.get_liquidation_history("ARBUSDT")
    assert seen[0][0] == "api/futures/liquidation/history"
    assert r[0]["long_liquidation_usd"] == 1234.56


def test_liq_history_both_fail_empty(monkeypatch):
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: None)
    monkeypatch.setattr(C, "cg_get", lambda *a, **k: None)
    assert C.get_liquidation_history("ARB") == []


def test_liq_summary_1h_uses_float_values(monkeypatch):
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: [{"time": 1, "long_liquidation_usd": "50000", "short_liquidation_usd": "100"}])
    monkeypatch.setattr(C, "cg_get", lambda *a, **k: None)
    r = C.get_liq_summary_1h("ARB")
    assert r["long_liq_1h"] == 50000.0 and r["liq_signal"] == "heavy_long_liq"


def test_private_liq_history_compat(monkeypatch):
    monkeypatch.setattr(C, "get_liquidation_history", lambda *a, **k: ["ok"])
    assert C._get_liq_history("ARB") == ["ok"]


# ── 限速 / 429 退避 ──
class _Resp:
    def __init__(self, status=200, body=None, headers=None, bad_json=False):
        self.status_code = status; self._b = body; self.headers = headers or {}; self._bad = bad_json
    def json(self):
        if self._bad:
            raise ValueError("no json")
        return self._b
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_rate_limit_interval_matches_measured_5_per_sec():
    assert C.CLAWBY_RATE_LIMIT_S == 0.25


@pytest.mark.parametrize("resp,expect", [
    (_Resp(429, {"code": "rate_limited", "retry_after": 1}), 1.0),
    (_Resp(429, {"retry_after": 2.5}), 2.5),
    (_Resp(429, {"retry_after": 999}), 5.0),                       # 封顶 5s
    (_Resp(429, {"code": "rate_limited"}), 1.0),                   # 缺失 → 1s
    (_Resp(429, {"retry_after": "abc"}), 1.0),
    (_Resp(429, {"retry_after": -3}), 1.0),
    (_Resp(429, bad_json=True), 1.0),                              # 非 JSON → 1s
    (_Resp(429, bad_json=True, headers={"Retry-After": "3"}), 3.0),
    (_Resp(429, bad_json=True, headers={"Retry-After": "300"}), 5.0),
])
def test_clawby_429_wait(resp, expect):
    assert C._clawby_429_wait(resp) == expect


def test_relay_429_sleeps_body_retry_after_then_succeeds(monkeypatch):
    seq = [_Resp(429, {"code": "rate_limited", "retry_after": 1, "limit": 5}),
           _Resp(200, {"data": {"code": "0", "data": [1]}})]
    monkeypatch.setattr(C.requests, "post", lambda *a, **k: seq.pop(0))
    monkeypatch.setattr(C, "_wait_clawby_rate_limit", lambda: None)
    monkeypatch.setattr(C, "_CLAWBY_402_UNTIL", 0.0)
    sleeps = []
    monkeypatch.setattr(C.time, "sleep", lambda x: sleeps.append(x))
    assert C.clawby_relay("futures_cvd_history", {}) == [1]
    assert sleeps == [1.0]                                         # 不再是默认 10s


def test_relay_429_no_retry_when_max_retries_0(monkeypatch):
    monkeypatch.setattr(C.requests, "post", lambda *a, **k: _Resp(429, {"retry_after": 1}))
    monkeypatch.setattr(C, "_wait_clawby_rate_limit", lambda: None)
    monkeypatch.setattr(C, "_CLAWBY_402_UNTIL", 0.0)
    sleeps = []
    monkeypatch.setattr(C.time, "sleep", lambda x: sleeps.append(x))
    with pytest.raises(RuntimeError):
        C.clawby_relay("x", {}, _max_retries=0)
    assert sleeps == []
