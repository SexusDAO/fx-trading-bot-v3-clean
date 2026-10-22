"""OI 历史进程内缓存 + _flush_oi_cache 并发（全 mock，不联网）。"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import scanner as sc


def hist(n, base=100.0):
    return [{"symbol": "X", "sumOpenInterest": str(base + i), "timestamp": i} for i in range(n)]


@pytest.fixture
def env(monkeypatch):
    sc._oi_history_cache.clear()
    clock = {"t": 1000.0}
    monkeypatch.setattr(sc, "_oi_hist_clock", lambda: clock["t"])
    calls = []
    store = {"data": hist(6)}

    def fake(symbol, period="1h", limit=24):
        calls.append((symbol, period, limit))
        d = store["data"]
        return d[-limit:] if d else []
    monkeypatch.setattr(sc.api, "get_oi_history", fake)
    yield type("E", (), {"clock": clock, "calls": calls, "store": store})
    sc._oi_history_cache.clear()


def test_miss_then_hit_no_second_rest(env):
    a = sc._get_oi_history_cached("AUSDT", "1h", 6)
    b = sc._get_oi_history_cached("AUSDT", "1h", 6)
    assert a == b == hist(6) and len(env.calls) == 1


def test_limit3_derived_from_cached_limit6_and_equals_direct(env):
    sc._get_oi_history_cached("AUSDT", "1h", 6)
    three = sc._get_oi_history_cached("AUSDT", "1h", 3)
    assert three == hist(6)[-3:] and len(env.calls) == 1
    # 先请求 limit=3 的 miss 也统一拉 6 根，随后 limit=6 命中
    env.calls.clear(); sc._oi_history_cache.clear()
    assert sc._get_oi_history_cached("BUSDT", "1h", 3) == hist(6)[-3:]
    assert env.calls == [("BUSDT", "1h", 6)]
    assert sc._get_oi_history_cached("BUSDT", "1h", 6) == hist(6) and len(env.calls) == 1


def test_ttl_55_minutes(env):
    sc._get_oi_history_cached("AUSDT", "1h", 6)
    env.clock["t"] += 55 * 60 - 1
    sc._get_oi_history_cached("AUSDT", "1h", 6)
    assert len(env.calls) == 1
    env.clock["t"] += 2                                   # 超过 55 分钟 → 重新拉取
    sc._get_oi_history_cached("AUSDT", "1h", 6)
    assert len(env.calls) == 2


def test_per_symbol_keys(env):
    sc._get_oi_history_cached("AUSDT", "1h", 6); sc._get_oi_history_cached("BUSDT", "1h", 6)
    assert [c[0] for c in env.calls] == ["AUSDT", "BUSDT"]


def test_empty_result_not_cached(env):
    env.store["data"] = []
    assert sc._get_oi_history_cached("AUSDT", "1h", 6) == []
    assert sc._get_oi_history_cached("AUSDT", "1h", 6) == []
    assert len(env.calls) == 2 and "AUSDT" not in sc._oi_history_cache


def test_non_1h_or_large_limit_bypasses_cache(env):
    sc._get_oi_history_cached("AUSDT", "4h", 6); sc._get_oi_history_cached("AUSDT", "1h", 24)
    assert env.calls == [("AUSDT", "4h", 6), ("AUSDT", "1h", 24)] and not sc._oi_history_cache


def test_short_cached_data_refetches_when_limit_larger(env):
    env.store["data"] = hist(2)
    sc._get_oi_history_cached("AUSDT", "1h", 3)           # 缓存里只有 2 根
    sc._get_oi_history_cached("AUSDT", "1h", 3)           # 不够 3 根 → 不能当命中
    assert len(env.calls) == 2


def test_cache_size_bounded(env, monkeypatch):
    monkeypatch.setattr(sc, "_OI_HIST_CACHE_MAX", 5)
    for i in range(12):
        sc._get_oi_history_cached(f"S{i}USDT", "1h", 6)
    assert len(sc._oi_history_cache) <= 5


# ── _flush_oi_cache ───────────────────────────────────────────────────────
def test_flush_concurrency_capped_at_10_and_writes_cache(monkeypatch):
    active, peak, lock = [0], [0], threading.Lock()

    def slow(sym):
        with lock:
            active[0] += 1; peak[0] = max(peak[0], active[0])
        time.sleep(0.03)
        with lock:
            active[0] -= 1
        return {"openInterest": "123.5"}
    got = {}
    monkeypatch.setattr(sc.api, "get_open_interest", slow)
    monkeypatch.setattr(sc, "update_oi_cache", lambda m: got.update(m))
    syms = [f"S{i}USDT" for i in range(60)]
    sc._flush_oi_cache(syms)
    assert 1 < peak[0] <= 10 and sc._OI_FLUSH_WORKERS == 10
    assert got == {s: 123.5 for s in syms}


def test_flush_skips_failures_and_zero(monkeypatch):
    def fake(sym):
        if sym == "ERR": raise RuntimeError("x")
        return {"openInterest": "0"} if sym == "ZERO" else ({} if sym == "EMPTY" else {"openInterest": "5"})
    got = {}
    monkeypatch.setattr(sc.api, "get_open_interest", fake)
    monkeypatch.setattr(sc, "update_oi_cache", lambda m: got.update(m))
    sc._flush_oi_cache(["OK", "ERR", "ZERO", "EMPTY"])
    assert got == {"OK": 5.0}


def test_flush_no_update_when_nothing(monkeypatch):
    called = []
    monkeypatch.setattr(sc.api, "get_open_interest", lambda s: {})
    monkeypatch.setattr(sc, "update_oi_cache", lambda m: called.append(m))
    sc._flush_oi_cache(["A"])
    assert called == []
