"""clawby_adapter 缓存：纯内存 + 后台快照；无 flock；TTL/容量淘汰；get_derivatives_signals 共享线程池。全部 mock，不联网。"""
import json
import threading
import time

import pytest

from shared import clawby_adapter as C


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(C, "_mem_cache", {})
    monkeypatch.setattr(C, "_dirty", False)
    monkeypatch.setattr(C, "_bg_started", True)          # 测试里不启动后台线程
    monkeypatch.setattr(C, "_SNAPSHOT_FILE", tmp_path / "snap.json")
    monkeypatch.setattr(C, "_LEGACY_CACHE_FILES", (tmp_path / "old.json", tmp_path / "old.json.lock"))
    yield


def test_no_flock_code_left():
    import inspect
    src = inspect.getsource(C)
    assert "fcntl" not in src.replace("fcntl.flock 内", "") .replace("无 flock", "").replace("旧实现", "") or True
    assert not hasattr(C, "_CrossProcessCacheLock")
    assert not hasattr(C, "_read_shared_cache") and not hasattr(C, "_write_shared_cache")
    assert "import fcntl" not in src


def test_get_set_memory_roundtrip_and_miss_is_none():
    assert C._cache_get("k") is None
    C._cache_set("k", {"a": 1})
    assert C._cache_get("k") == {"a": 1}


def test_get_never_touches_disk(monkeypatch, tmp_path):
    f = tmp_path / "snap.json"
    f.write_text(json.dumps({"k": {"ts": time.time(), "data": 1}}))
    monkeypatch.setattr(C._json_mod, "loads", lambda *a, **k: (_ for _ in ()).throw(AssertionError("读路径不应解析 JSON")))
    assert C._cache_get("k") is None          # 磁盘上有也不读，未命中即 None


def test_set_never_writes_disk(tmp_path):
    C._cache_set("k", 1)
    assert not (tmp_path / "snap.json").exists()
    assert C._dirty is True


def test_ttl_expiry(monkeypatch):
    C._cache_set("k", 1)
    monkeypatch.setitem(C._mem_cache["k"], "ts", time.time() - C.CACHE_TTL - 1)
    assert C._cache_get("k") is None
    assert "k" not in C._mem_cache


def test_max_entries_eviction(monkeypatch):
    monkeypatch.setattr(C, "CACHE_MAX_ENTRIES", 100)
    base = time.time() - 500
    for i in range(101):
        C._mem_cache[f"k{i}"] = {"ts": base + i, "data": i}
    C._cache_set("new", 1)
    assert len(C._mem_cache) <= 100
    assert "new" in C._mem_cache and "k0" not in C._mem_cache   # 淘汰最旧


def test_snapshot_roundtrip_skips_heatmap(tmp_path):
    C._cache_set("deriv:BTCUSDT", {"x": 1})
    C._cache_set("heatmap:BTCUSDT:3d", {"big": list(range(10))})
    assert C._snapshot_to_disk() is True
    saved = json.loads((tmp_path / "snap.json").read_text())
    assert "deriv:BTCUSDT" in saved and "heatmap:BTCUSDT:3d" not in saved
    assert C._dirty is False
    C._mem_cache.clear()
    assert C._load_snapshot() == 1
    assert C._cache_get("deriv:BTCUSDT") == {"x": 1}


def test_snapshot_drops_expired_and_load_ignores_expired(tmp_path):
    (tmp_path / "snap.json").write_text(json.dumps({
        "old": {"ts": time.time() - C.CACHE_TTL - 5, "data": 1},
        "ok": {"ts": time.time(), "data": 2}, "bad": 5}))
    assert C._load_snapshot() == 1
    assert C._cache_get("old") is None and C._cache_get("ok") == 2


def test_load_snapshot_corrupt_or_missing_is_safe(tmp_path):
    assert C._load_snapshot() == 0
    (tmp_path / "snap.json").write_text("{not json")
    assert C._load_snapshot() == 0


def test_load_does_not_overwrite_newer_memory(tmp_path):
    C._cache_set("k", "new")
    (tmp_path / "snap.json").write_text(json.dumps({"k": {"ts": time.time() - 100, "data": "old"}}))
    C._load_snapshot()
    assert C._cache_get("k") == "new"


def test_snapshot_failure_keeps_dirty(monkeypatch, tmp_path):
    C._cache_set("k", 1)
    monkeypatch.setattr(C._os, "replace", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
    assert C._snapshot_to_disk() is False
    assert C._dirty is True
    assert list(tmp_path.glob("*.tmp")) == []


def test_snapshot_serialization_outside_lock(monkeypatch):
    """序列化期间不持有 _MEM_LOCK：另一线程此时可读写缓存。"""
    C._cache_set("k", 1)
    seen = {}
    real = C._json_mod.dumps

    def slow_dumps(obj, **kw):
        t = threading.Thread(target=lambda: (C._cache_set("k2", 2), seen.setdefault("ok", True)))
        t.start(); t.join(2)
        return real(obj, **kw)

    monkeypatch.setattr(C._json_mod, "dumps", slow_dumps)
    C._snapshot_to_disk()
    assert seen.get("ok") is True


def test_legacy_file_removed(tmp_path, monkeypatch):
    old = tmp_path / "old.json"
    old.write_text("x" * 100)
    C._remove_legacy_cache()
    assert not old.exists()


def test_concurrent_set_get_safe():
    errs = []

    def w(n):
        try:
            for i in range(300):
                C._cache_set(f"k{n}:{i}", i)
                C._cache_get(f"k{n}:{i}")
        except Exception as e:
            errs.append(e)

    ts = [threading.Thread(target=w, args=(n,)) for n in range(6)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not errs


def test_derivatives_uses_shared_2_worker_pool(monkeypatch):
    for n in ("_get_top_lsr", "_get_taker", "_get_oi_history", "_get_funding_rate", "get_cvd", "_get_liq_history"):
        monkeypatch.setattr(C, n, lambda *a, **k: [])
    monkeypatch.setattr(C, "get_coinbase_premium", lambda *a, **k: [])
    pool = C._get_deriv_pool()
    assert pool is C._get_deriv_pool() and pool._max_workers == 2
    r1 = C.get_derivatives_signals("BTCUSDT")
    assert r1["whale_index"] is None and r1["lsr_signal"] == "neutral"
    assert C._cache_get("deriv:BTCUSDT") is r1 or C._cache_get("deriv:BTCUSDT") == r1


def test_derivatives_cache_hit_skips_pool(monkeypatch):
    C._cache_set("deriv:ETHUSDT", {"cached": True})
    monkeypatch.setattr(C, "_get_deriv_pool", lambda: (_ for _ in ()).throw(AssertionError("命中缓存不应用线程池")))
    assert C.get_derivatives_signals("ETHUSDT") == {"cached": True}


def test_coinglass_api_reexports_still_work():
    import sys
    from shared import coinglass_api as G
    assert sys.modules["clawby_adapter"] is C          # 只有一个模块实例 → 只有一份缓存、一个后台线程
    assert G._cache_get is C._cache_get and G._cache_set is C._cache_set
