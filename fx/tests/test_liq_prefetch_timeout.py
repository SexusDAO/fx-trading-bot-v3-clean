"""清算图预热：取数放独立线程、单次 5s 超时跳过且不缓存；晚到结果不写缓存；超时关闭时保持旧行为。全部 mock。"""
import threading
import time

import pytest

from shared import liq_cache as L

DATA = {"LONG": [{"price_low": 1, "price_high": 2, "amount": 1}], "SHORT": [], "ts": 0, "src_level": 1}


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.setattr(L, "_CACHE", {})
    monkeypatch.setattr(L, "_INFLIGHT", {})
    monkeypatch.setattr(L, "_FAIL_CACHE", {})
    monkeypatch.setattr(L, "_FETCH_POOL", None)


def test_config_default():
    import config
    assert config.LIQ_PREFETCH_FETCH_TIMEOUT_SEC == 30.0  # [v3.96] 5→30s：排队等限速+pool请求时间需要约16s+


def test_fast_fetch_returns_and_caches_in_other_thread(monkeypatch):
    where = {}

    def fake(sym, price, range_="3d"):
        where["t"] = threading.current_thread().name
        return dict(DATA, ts=time.time())

    monkeypatch.setattr(L, "_fetch_from_clawby", fake)
    out = L._fetch_with_timeout("AUSDT", 10.0, "3d", "AUSDT:3d", timeout=2.0)
    assert out and where["t"].startswith("liq-fetch")                  # 不在调用线程里等网络
    assert "AUSDT:3d" in L._CACHE


def test_slow_fetch_times_out_skips_and_never_caches(monkeypatch):
    gate = threading.Event()
    done = threading.Event()

    def slow(sym, price, range_="3d"):
        gate.wait(5)
        done.set()
        return dict(DATA, ts=time.time())

    monkeypatch.setattr(L, "_fetch_from_clawby", slow)
    t0 = time.monotonic()
    out = L._fetch_with_timeout("BUSDT", 10.0, "3d", "BUSDT:3d", timeout=0.2)
    assert out == {} and time.monotonic() - t0 < 1.5                    # 调用线程只等了超时时间
    gate.set()
    assert done.wait(3)
    time.sleep(0.2)
    assert "BUSDT:3d" not in L._CACHE                                   # 晚到结果不写缓存


def test_exception_in_fetch_returns_empty(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("net")

    monkeypatch.setattr(L, "_fetch_from_clawby", boom)
    assert L._fetch_with_timeout("CUSDT", 1.0, "3d", "CUSDT:3d", timeout=1.0) == {}


def test_timeout_zero_keeps_old_synchronous_behaviour(monkeypatch):
    where = {}

    def fake(sym, price, range_="3d"):
        where["t"] = threading.current_thread()
        return dict(DATA, ts=time.time())

    monkeypatch.setattr(L, "_fetch_from_clawby", fake)
    assert L._fetch_with_timeout("DUSDT", 1.0, "3d", "DUSDT:3d", timeout=0)
    assert where["t"] is threading.current_thread()


def test_prefetch_one_uses_timeout_path_and_skips_slow_symbol(monkeypatch):
    monkeypatch.setattr(L, "LIQ_PREFETCH_SLEEP_SEC", 0)
    monkeypatch.setattr(L, "LIQ_PREFETCH_FETCH_TIMEOUT_SEC", 0.2)
    gate = threading.Event()
    monkeypatch.setattr(L, "_fetch_from_clawby", lambda *a, **k: gate.wait(3) and {})
    t0 = time.monotonic()
    assert L._prefetch_one("SLOWUSDT", price=10.0) is False
    assert time.monotonic() - t0 < 2.5
    gate.set()


def test_abandoned_flag_blocks_store_in_fetch_dedup(monkeypatch):
    monkeypatch.setattr(L, "_fetch_from_clawby", lambda *a, **k: dict(DATA, ts=time.time()))
    ev = threading.Event()
    ev.set()
    assert L._fetch_dedup("EUSDT", 1.0, "3d", "EUSDT:3d", ev) == {}
    assert "EUSDT:3d" not in L._CACHE
    assert L._fetch_dedup("EUSDT", 1.0, "3d", "EUSDT:3d")               # 无 abandon 时正常缓存
    assert "EUSDT:3d" in L._CACHE
