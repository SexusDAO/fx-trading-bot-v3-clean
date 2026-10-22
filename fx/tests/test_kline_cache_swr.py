"""K线缓存 stale-while-revalidate（全 mock，不联网）。"""
import os, sys, threading, time
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from shared import kline_cache as KC
from shared.kline_cache import KlineCache
import scanner as sc

M = 900_000                       # 15m
T0 = 1_700_000_000_000 - (1_700_000_000_000 % M)


def make(n=100, last_open=T0 + 200 * M):
    return [[last_open - (n - 1 - i) * M, "1", "2", "0.5", "1.5", "10", last_open - (n - 1 - i) * M + M - 1, "15", 5, "6", "9", "0"]
            for i in range(n)]


def wait(cond, t=3.0):
    end = time.time() + t
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


@pytest.fixture
def seeded():
    c = KlineCache()
    old = make()
    c.set("AUSDT", "15m", old)
    return c, old


def new_live(k):                  # 过期 k 个周期后的未收盘 K 线
    return [make(1, last_open=T0 + (200 + k) * M)[0]]


def test_without_refresh_behavior_unchanged(seeded):
    c, old = seeded
    assert c.get("AUSDT", "15m", 100, fetch_live=lambda: new_live(1)) is None
    assert c.stats()["expired"] == 1 and c.size() == 0


def test_expired_serves_stale_and_refreshes_in_background(seeded):
    c, old = seeded
    gate, calls = threading.Event(), []
    fresh = make(100, last_open=T0 + 201 * M)

    def refresh():
        calls.append(1); gate.wait(2); return fresh
    got = c.get("AUSDT", "15m", 100, fetch_live=lambda: new_live(1), refresh=refresh)
    assert got is not None and len(got) == 100
    assert got[:-1] == old[1:-1] + [] or got[-2] == old[-2]                 # 旧已收盘数据
    assert got[-1][0] == T0 + 201 * M                                       # 最后一根是最新未收盘 K 线
    assert c.size() == 1 and c.stats()["hit"] == 1 and c.swr_stats()["stale"] == 1   # 条目没被删除，算命中
    gate.set()
    assert wait(lambda: c.swr_stats()["refresh_ok"] == 1)
    after = c.get("AUSDT", "15m", 100, fetch_live=lambda: [fresh[-1]], refresh=refresh)
    assert after == fresh and c.swr_stats()["stale"] == 1                  # 刷新后恢复普通命中
    assert len(calls) == 1


def test_refresh_deduplicated_while_in_flight(seeded):
    c, _ = seeded
    gate, calls = threading.Event(), []

    def refresh():
        calls.append(1); gate.wait(2); return None
    for _ in range(5):
        assert c.get("AUSDT", "15m", 100, fetch_live=lambda: new_live(1), refresh=refresh) is not None
    gate.set()
    assert wait(lambda: c.swr_stats()["refresh_fail"] == 1)
    assert len(calls) == 1 and c.swr_stats()["stale"] == 5


def test_refresh_failure_keeps_stale_then_cooldown_then_retry(seeded, monkeypatch):
    c, _ = seeded
    calls = []

    def bad():
        calls.append(1); raise RuntimeError("rest down")
    assert c.get("AUSDT", "15m", 100, fetch_live=lambda: new_live(1), refresh=bad) is not None
    assert wait(lambda: c.swr_stats()["refresh_fail"] == 1)
    for _ in range(3):                                                         # 失败后仍继续使用旧值
        assert c.get("AUSDT", "15m", 100, fetch_live=lambda: new_live(1), refresh=bad) is not None
    assert len(calls) == 1 and c.size() == 1                                  # 冷却期内不重复打 REST
    monkeypatch.setattr(KC, "REFRESH_RETRY_SEC", 0.0)
    c.get("AUSDT", "15m", 100, fetch_live=lambda: new_live(1), refresh=bad)
    assert wait(lambda: len(calls) == 2)


def test_stale_window_is_two_extra_ttls(seeded):
    c, _ = seeded
    for k in (1, 2):                                                           # 过期 1、2 个周期：可用（15m 最多用到 45 分钟前）
        assert c.get("AUSDT", "15m", 100, fetch_live=lambda k=k: new_live(k), refresh=lambda: None) is not None
    assert c.get("AUSDT", "15m", 100, fetch_live=lambda: new_live(3), refresh=lambda: None) is None   # 第 3 个：真正过期
    assert c.stats()["expired"] == 1 and c.size() == 0


def test_same_period_still_normal_hit_and_no_refresh(seeded):
    c, old = seeded
    calls = []
    got = c.get("AUSDT", "15m", 100, fetch_live=lambda: [old[-1]], refresh=lambda: calls.append(1))
    assert got == old and calls == [] and c.swr_stats()["stale"] == 0


def test_stale_without_fetch_live_uses_clock_window(seeded, monkeypatch):
    c, old = seeded
    last_close = old[-2][6]
    monkeypatch.setattr(KC.time, "time", lambda: (last_close + M + M // 2) / 1000)       # 过期 0.5 个周期
    got = c.get("AUSDT", "15m", 100, refresh=lambda: None)
    assert got is not None and got[-1] == old[-1] and c.swr_stats()["stale"] == 1
    monkeypatch.setattr(KC.time, "time", lambda: (last_close + M + 3 * M) / 1000)         # 超出窗口
    assert c.get("AUSDT", "15m", 100, refresh=lambda: None) is None


def test_burst_expiry_no_foreground_full_fetch():
    """整点/15 分钟整：大量条目同时过期，前台全部命中旧值，不发全量 REST。"""
    c = KlineCache(max_entries=600)
    for i in range(200):
        c.set(f"S{i}USDT", "15m", make())
    full = []
    done = threading.Event()

    def mk(sym):
        def refresh():
            full.append(sym); return make(100, last_open=T0 + 201 * M)
        return refresh
    res = [c.get(f"S{i}USDT", "15m", 100, fetch_live=lambda: new_live(1), refresh=mk(i)) for i in range(200)]
    assert all(r is not None for r in res) and c.stats() == {"hit": 200, "miss": 0, "expired": 0}
    assert wait(lambda: c.swr_stats()["refresh_ok"] == 200, 5)
    assert len(full) == 200


# ── scanner 接入 ───────────────────────────────────────────────────────────
def test_scanner_passes_refresh_and_background_updates_cache(monkeypatch):
    import config
    monkeypatch.setattr(config, "KLINE_CACHE_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", False, raising=False)
    cache = KlineCache(); cache.set("AUSDT", "15m", make())
    monkeypatch.setattr(sc, "_KLINE_CACHE", cache)
    monkeypatch.setattr(sc, "_KLINE_CACHE_FAILED", False)
    calls = []
    fresh = make(100, last_open=T0 + 201 * M)

    def fake(sym, iv, limit):
        calls.append(limit)
        return new_live(1) if limit == 1 else fresh
    monkeypatch.setattr(sc, "_kline_fetch", fake)
    got = sc._get_klines_cached("AUSDT", "15m", 100)
    assert len(got) == 100 and got[-1][0] == T0 + 201 * M
    assert wait(lambda: cache.swr_stats()["refresh_ok"] == 1)
    assert calls.count(1) == 1 and calls.count(100) == 1                      # 前台只有 limit=1，全量在后台


def test_round_summary_mentions_stale(monkeypatch):
    cache = KlineCache(); cache.set("AUSDT", "15m", make())
    monkeypatch.setattr(sc, "_KLINE_CACHE", cache)
    sc._kline_round_reset()
    cache.get("AUSDT", "15m", 100, fetch_live=lambda: new_live(1), refresh=lambda: None)
    line = [l for l in sc._kline_round_summary() if "K线缓存" in l][0]
    assert "命中=1" in line and "旧值=1" in line
