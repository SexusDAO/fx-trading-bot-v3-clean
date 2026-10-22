"""K 线 REST 缓存。运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_kline_cache.py（mock）"""
import os, sys, threading
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from shared.kline_cache import KlineCache
import scanner as sc

H = 3_600_000
T0 = 1_700_000_000_000 - (1_700_000_000_000 % H)


def make(n=100, last_open=None):
    """生成 n 根 1h K 线，最后一根是未收盘的；last_open 为未收盘那根的开盘时间。"""
    last_open = T0 + 200 * H if last_open is None else last_open
    rows = []
    for i in range(n):
        o = last_open - (n - 1 - i) * H
        rows.append([o, "1", "2", "0.5", "1.5", "10", o + H - 1, "15", 5, "6", "9", "0"])
    return rows


def live_of(data):
    return [data[-1]]


# ── KlineCache ──────────────────────────────────────────────────────────────
def test_get_miss_returns_none():
    c = KlineCache()
    assert c.get("AUSDT", "1h", 100) is None and c.stats() == {"hit": 0, "miss": 1, "expired": 0}


def test_set_then_get_hits_and_same_format():
    c = KlineCache(); d = make()
    c.set("AUSDT", "1h", d)
    got = c.get("AUSDT", "1h", 100, fetch_live=lambda: live_of(d))
    assert got == d and len(got) == 100 and c.stats()["hit"] == 1


def test_new_candle_closed_expires_cache():
    c = KlineCache(); d = make()
    c.set("AUSDT", "1h", d)
    newer = make(last_open=T0 + 201 * H)                 # 下一根开盘 → 上一根的 k[-2][6] 变大 = 有新 K 线收盘
    assert newer[-2][6] > d[-2][6]
    assert c.get("AUSDT", "1h", 100, fetch_live=lambda: live_of(newer)) is None
    assert c.stats()["expired"] == 1 and c.size() == 0   # 失效条目被移除，调用方重拉后 set 写回
    c.set("AUSDT", "1h", newer)
    assert c.get("AUSDT", "1h", 100, fetch_live=lambda: live_of(newer)) == newer


def test_same_period_second_get_hits_with_updated_live_bar():
    c = KlineCache(); d = make()
    c.set("AUSDT", "1h", d)
    live2 = [list(d[-1])]; live2[0][4] = "9.9"           # 同一未收盘 K 线价格变了
    got = c.get("AUSDT", "1h", 100, fetch_live=lambda: live2)
    assert got[-1][4] == "9.9" and got[:-1] == d[:-1] and c.stats()["hit"] == 1


def test_smaller_limit_slices_and_larger_limit_misses():
    c = KlineCache(); d = make(100)
    c.set("AUSDT", "1h", d)
    assert c.get("AUSDT", "1h", 30, fetch_live=lambda: live_of(d)) == d[-30:]
    assert c.get("AUSDT", "1h", 200, fetch_live=lambda: live_of(d)) is None


def test_live_fetch_failure_or_inconsistent_is_miss():
    c = KlineCache(); d = make(); c.set("AUSDT", "1h", d)
    assert c.get("AUSDT", "1h", 100, fetch_live=lambda: []) is None
    def boom(): raise RuntimeError("x")
    assert c.get("AUSDT", "1h", 100, fetch_live=boom) is None
    old = make(last_open=T0 + 150 * H)
    assert c.get("AUSDT", "1h", 100, fetch_live=lambda: live_of(old)) is None


def test_set_rejects_bad_data():
    c = KlineCache()
    c.set("A", "1h", []); c.set("A", "1h", [[1]]); c.set("A", "1h", [["x"] * 12] * 5)
    assert c.size() == 0


def test_thread_safety_10_threads():
    c = KlineCache(max_entries=50); errors = []
    def work(i):
        try:
            for j in range(200):
                d = make(last_open=T0 + (200 + (j % 3)) * H)
                sym = f"S{(i * 7 + j) % 40}"
                c.set(sym, "1h", d)
                c.get(sym, "1h", 100, fetch_live=lambda: live_of(d))
                c.stats()
        except Exception as e:
            errors.append(e)
    ts = [threading.Thread(target=work, args=(i,)) for i in range(10)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert errors == [] and c.size() <= 50


# ── scanner._get_klines_cached ───────────────────────────────────────────────
@pytest.fixture
def rest(monkeypatch):
    calls = []
    state = {"last_open": T0 + 200 * H}
    def gk(symbol, interval, limit):
        calls.append((symbol, interval, limit))
        full = make(100, state["last_open"])
        return full[-limit:] if limit < 100 else full
    monkeypatch.setattr(sc.api, "get_klines", gk)
    monkeypatch.setattr(sc, "_KLINE_CACHE", None)
    monkeypatch.setattr(sc, "_KLINE_CACHE_FAILED", False)
    return calls, state


def test_second_call_same_period_only_fetches_one_bar(rest):
    calls, state = rest
    a = sc._get_klines_cached("AUSDT", "1h", 100)
    b = sc._get_klines_cached("AUSDT", "1h", 100)
    assert a == b and len(b) == 100
    assert calls == [("AUSDT", "1h", 100), ("AUSDT", "1h", 1)]     # 第二次只多一次 limit=1，不重复拉 100 根


def test_new_period_refetches_full(rest):
    calls, state = rest
    sc._get_klines_cached("AUSDT", "1h", 100)
    state["last_open"] += H
    out = sc._get_klines_cached("AUSDT", "1h", 100)
    assert out[-1][0] == state["last_open"]
    assert calls == [("AUSDT", "1h", 100), ("AUSDT", "1h", 1), ("AUSDT", "1h", 100)]


def test_disabled_flag_falls_back_to_direct_rest(rest, monkeypatch):
    import config
    monkeypatch.setattr(config, "KLINE_CACHE_ENABLED", False, raising=False)
    calls, _ = rest
    sc._get_klines_cached("AUSDT", "1h", 100); sc._get_klines_cached("AUSDT", "1h", 100)
    assert calls == [("AUSDT", "1h", 100)] * 2 and sc._KLINE_CACHE is None


def test_interval_not_listed_goes_direct(rest, monkeypatch):
    import config
    monkeypatch.setattr(config, "KLINE_CACHE_INTERVALS", ["4h"], raising=False)
    calls, _ = rest
    sc._get_klines_cached("AUSDT", "1h", 100); sc._get_klines_cached("AUSDT", "1h", 100)
    assert calls == [("AUSDT", "1h", 100)] * 2


def test_cache_module_import_failure_degrades(rest, monkeypatch):
    monkeypatch.setitem(sys.modules, "shared.kline_cache", None)      # 令 import 抛 ImportError
    calls, _ = rest
    out1 = sc._get_klines_cached("AUSDT", "1h", 100)
    out2 = sc._get_klines_cached("AUSDT", "1h", 100)
    assert len(out1) == 100 == len(out2) and calls == [("AUSDT", "1h", 100)] * 2
    assert sc._KLINE_CACHE is None and sc._KLINE_CACHE_FAILED is True   # 只尝试加载一次


def test_empty_rest_result_is_not_cached(rest, monkeypatch):
    monkeypatch.setattr(sc.api, "get_klines", lambda *a, **k: [])
    assert sc._get_klines_cached("AUSDT", "1h", 100) == []
    assert sc._get_kline_cache().size() == 0


def test_analyze_symbol_uses_cached_helper_at_three_sites():
    src = (Path(__file__).parent.parent / "scanner.py").read_text()
    for call in ('_get_klines_cached(symbol, "15m", 100)', '_get_klines_cached(symbol, "1h",  100)',
                 '_get_klines_cached(symbol, "4h", 100)'):
        assert src.count(call) == 1


def test_round_summary_reports_cache_hit_rate(rest):
    sc._get_klines_cached("AUSDT", "1h", 100)
    sc._kline_round_reset()
    sc._get_klines_cached("AUSDT", "1h", 100)
    line = [l for l in sc._kline_round_summary() if "K线缓存" in l][0]
    assert "命中=1 未命中=0 过期=0 命中率=100%" in line
