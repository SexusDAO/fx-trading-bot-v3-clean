"""get_liq_zones_nowait：扫描路径只读缓存，永不发网络请求；缺失/过旧时登记给后台预热。"""
import sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from shared import liq_cache as L


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    L._CACHE.clear(); L._drain_wanted()
    def boom(*a, **k):
        raise AssertionError("扫描只读路径不得发网络请求")
    monkeypatch.setattr(L, "_fetch_dedup", boom)
    monkeypatch.setattr(L, "_fetch_from_clawby", boom)
    yield
    L._CACHE.clear(); L._drain_wanted()


def _put(key, age):
    L._CACHE[key] = {"LONG": [{"price_low": 1, "price_high": 2, "amount": 9}], "SHORT": [], "ts": time.time() - age}


def test_fresh_hit_returns_data_no_queue():
    _put("AAAUSDT:3d", 60)
    assert L.get_liq_zones_nowait("AAAUSDT", "LONG", 1.0, "3d")[0]["amount"] == 9
    assert L._drain_wanted() == []


def test_past_ttl_but_within_max_still_served_and_queued():
    _put("BBBUSDT:3d", L._CACHE_TTL + 60)       # 已过 15 分钟 TTL，但在可用上限内
    assert L.get_liq_zones_nowait("BBBUSDT", "LONG", 1.0, "3d")
    assert L._drain_wanted() == ["BBBUSDT"]


def test_missing_returns_empty_and_queues_without_network():
    assert L.get_liq_zones_nowait("CCCUSDT", "LONG", 1.0, "3d") == []
    assert L._drain_wanted() == ["CCCUSDT"]


def test_too_old_treated_as_missing():
    _put("DDDUSDT:3d", L.LIQ_SCAN_STALE_MAX_SEC + 60)
    assert L.get_liq_zones_nowait("DDDUSDT", "LONG", 1.0, "3d") == []
    assert L._drain_wanted() == ["DDDUSDT"]


def test_queue_is_bounded():
    for i in range(L._WANT_MAX + 50):
        L.get_liq_zones_nowait(f"S{i}USDT", "LONG", 1.0, "3d")
    assert len(L._drain_wanted()) == L._WANT_MAX
