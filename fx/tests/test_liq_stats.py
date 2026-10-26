"""[liq-stats] 每轮扫描清算区命中率日志：缓存覆盖统计 + sl_degraded 计数 + 日志格式。"""
import time

import pytest

import scanner
from shared import liq_cache as LC


@pytest.fixture
def clean_cache(monkeypatch):
    monkeypatch.setattr(LC, "_CACHE", {})
    return LC._CACHE


def test_coverage_counts_fresh_entries_only(clean_cache):
    now = time.time()
    clean_cache["AUSDT:3d"] = {"ts": now - 10}
    clean_cache["BUSDT:3d"] = {"ts": now - LC.LIQ_SCAN_STALE_MAX_SEC - 5}   # 过旧：不算命中
    clean_cache["CUSDT:24h"] = {"ts": now}                                    # range 不同：不算
    assert LC.coverage(["AUSDT", "BUSDT", "CUSDT", "DUSDT"], "3d") == (1, 4)


def test_coverage_empty_and_custom_age(clean_cache):
    assert LC.coverage([], "3d") == (0, 0)
    clean_cache["AUSDT:3d"] = {"ts": time.time() - 100}
    assert LC.coverage(["AUSDT"], "3d", max_age_sec=50) == (0, 1)
    assert LC.coverage(["AUSDT"], "3d", max_age_sec=500) == (1, 1)


def test_coverage_does_not_register_wanted(clean_cache):
    with LC._WANT_LOCK:
        LC._WANT.clear()
    LC.coverage(["ZZUSDT"], "3d")
    assert "ZZUSDT" not in LC._WANT


def test_count_sl_degraded_top_level_and_params():
    sigs = [{"sl_degraded": True}, {"params": {"sl_degraded": True}}, {"sl_degraded": False},
            {}, {"params": None}, None]
    assert scanner._count_sl_degraded(sigs) == 2
    assert scanner._count_sl_degraded(None) == 0


def test_stats_line_format():
    assert scanner._liq_stats_line(80, 100, 3) == "[liq-stats] 本轮清算区: 命中80个/扫描100个(80%) 缺失3个(sl_degraded)"
    assert scanner._liq_stats_line(0, 0, 0) == "[liq-stats] 本轮清算区: 命中0个/扫描0个(0%) 缺失0个(sl_degraded)"
