"""阶段1：分析进程池（含真实 spawn 子进程，不 mock 进程池；业务数据为合成场景，不联网）。"""
import concurrent.futures as cf
import logging
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest
from concurrent.futures.process import BrokenProcessPool

import phase2_harness as H
import scanner as sc
from shared import analysis_worker as aw


@pytest.fixture(scope="module", autouse=True)
def _cleanup():
    yield
    aw.shutdown()


# ── 配置与分支 ─────────────────────────────────────────────────────────────
def test_flag_default_false_and_worker_count():
    import config
    assert isinstance(config.SCAN_USE_PROCESS_POOL, bool)  # [refactor-B2] 默认已开启
    n = aw.worker_count()
    assert 1 <= n <= min(config.SCAN_MAX_WORKERS, os.cpu_count() or 1, 3)


def test_run_scan_branches_on_flag():
    src = (Path(__file__).parent.parent / "scanner.py").read_text(encoding="utf-8")
    assert 'getattr(_cfg_pp, "SCAN_USE_PROCESS_POOL", False)' in src
    assert src.count("_run_parallel_process_pool(") >= 2 and "_run_parallel_with_deadline(" in src


# ── 假进程池（同进程执行 analyze_task）：验证调度、事件后置与线程池路径一致 ─────────
class FakePool:
    def __init__(self):
        self.ex = cf.ThreadPoolExecutor(max_workers=1)      # 单线程：analyze_task 用到的 _W 是进程级全局

    def submit(self, fn, *a, **k):
        return self.ex.submit(fn, *a, **k)


def _jobs(scn, **over):
    s = scn["symbol"]
    return {s: (s, scn["ticker"], set(), set(), None, None, "neutral", "range")}


CASES = [(s, fi) for s in range(0, 24) for fi in (0, 1, 2, 3)]


@pytest.mark.parametrize("seed,fi", CASES, ids=[f"{s}-{f}" for s, f in CASES])
def test_pool_path_matches_thread_path(seed, fi, monkeypatch):
    scn = H.make_scenario(seed)
    rec = H.Rec()
    H.install(sc, scn, monkeypatch, rec, force=H.FORCE_SETS[fi])
    from shared import liq_cache
    monkeypatch.setattr(liq_cache, "get_liq_zones_cached", lambda *a, **k: [])
    mp_map = {scn["symbol"]: dict(scn["mp"])}
    # 线程池路径（_analyze_symbol_impl）
    thread_sig = H.normalize(H.run_impl(sc, scn)); t_an, t_sh = list(rec.analytics), list(rec.shadow)
    rec.analytics.clear(); rec.shadow.clear()
    # 进程池路径（假池）
    monkeypatch.setattr(aw, "get_healthy_pool", lambda: FakePool())
    res, unf = sc._run_parallel_process_pool(_jobs(scn), mp_map, 2, 30)
    assert unf == [] and len(res) == 1
    pool_sig = H.normalize(res[0][1])
    for x in (thread_sig, pool_sig):
        if x:
            x.pop("ts", None)
    assert pool_sig == thread_sig and rec.analytics == t_an and rec.shadow == t_sh


def test_cooldown_event_applied_in_main(monkeypatch):
    calls = []
    import shared.state as st
    monkeypatch.setattr(st, "set_cooldown", lambda s, h, r: calls.append((s, h, r)))
    out = sc._apply_post_events(None, [{"kind": "cooldown", "symbol": "XUSDT", "hours": 4, "reason": "crash_guard"}])
    assert out is None and calls == [("XUSDT", 4, "crash_guard")]


def test_deadline_gives_up_and_resets_pool(monkeypatch):
    scn = H.make_scenario(1)
    H.install(sc, scn, monkeypatch, H.Rec(), force=H.FORCE_SETS[1])
    from shared import liq_cache
    monkeypatch.setattr(liq_cache, "get_liq_zones_cached", lambda *a, **k: [])
    never = cf.Future()

    class Stuck:
        def submit(self, fn, *a, **k):
            return never
    resets = []
    monkeypatch.setattr(aw, "get_healthy_pool", lambda: Stuck())
    monkeypatch.setattr(aw, "reset_pool", lambda kill=True: resets.append(kill))
    t0 = time.monotonic()
    res, unf = sc._run_parallel_process_pool(_jobs(scn), {scn["symbol"]: scn["mp"]}, 2, 1.0)
    assert unf == [scn["symbol"]] and res == [] and resets == [True]
    assert time.monotonic() - t0 < 5


def test_pool_unavailable_falls_back_to_thread_path(monkeypatch):
    scn = H.make_scenario(1)
    H.install(sc, scn, monkeypatch, H.Rec(), force=H.FORCE_SETS[1])
    monkeypatch.setattr(aw, "get_healthy_pool", lambda: (_ for _ in ()).throw(RuntimeError("no pool")))
    res, unf = sc._run_parallel_process_pool(_jobs(scn), {scn["symbol"]: scn["mp"]}, 2, 30)
    assert unf == [] and res[0][1] is not None


# ── 真实 spawn ─────────────────────────────────────────────────────────────
def test_real_spawn_probe_patches_and_no_main_reimport():
    pool = aw.get_healthy_pool()
    r = pool.submit(aw._selftest_probe).result(timeout=60)
    assert r["cooldown_fn"] == "_record_cooldown" and r["liq_fn"] == "_liq_from_snapshot"
    assert r["cooldowns"] == [{"kind": "cooldown", "symbol": "PROBEUSDT", "hours": 4, "reason": "crash_guard"}]
    assert r["liq_hit"] == [{"p": 1.0}] and r["liq_other"] == [] and r["liq_miss"] == []      # 快照命中 / 其他标的 / 未命中都不联网
    assert r["main_imported"] is False                        # 没有重新导入 main.py
    assert r["root_handlers"] == ["QueueHandler"]
    # 主进程的函数没被替换
    from shared import judges, liq_cache
    assert judges._set_cooldown_safe.__name__ == "_set_cooldown_safe"
    assert liq_cache.get_liq_zones_cached.__name__ == "get_liq_zones_cached"


def test_real_spawn_worker_logs_reach_main(caplog):
    caplog.set_level(logging.WARNING)
    aw.get_healthy_pool().submit(aw._selftest_log).result(timeout=60)
    end = time.time() + 5
    while time.time() < end and not any("hello-from-worker" in r.getMessage() for r in caplog.records):
        time.sleep(0.05)
    assert any("hello-from-worker" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4, 5])
def test_real_spawn_analyze_task_equals_in_process(seed, monkeypatch):
    scn = H.make_scenario(seed)
    k = scn["k"]
    pf = {"oi_now": scn["oi_now"], "oi_1h_ago": float(scn["oi_hist"][-3]["sumOpenInterest"]),
          "oi_24h_ago": scn["oi_24h_ago"], "oi_hist_6": scn["oi_hist"], "funding_velocity": dict(scn["fv"]),
          "mp": scn["mp"], "is_weekend": False, "stock_closed": False, "dir_cooldown_long": False,
          "dir_cooldown_short": False, "now_ms": 1_700_000_100_000, "liq_zones": {}}
    args = (scn["symbol"], scn["ticker"], k["15m"], k["1h"], k["4h"], pf, set(), set(), None, None,
            "neutral", "range", {scn["symbol"]: scn["mp"]})
    sig, events = aw.get_healthy_pool().submit(aw.analyze_task, *args).result(timeout=60)
    ev2 = []
    monkeypatch.setattr(sc, "_calc_route_quality", sc._calc_route_quality)
    sig2 = sc.analyze_one(*args[:5], *args[6:], pf, ev2) if False else sc.analyze_one(
        scn["symbol"], scn["ticker"], k["15m"], k["1h"], k["4h"], set(), set(), None, None, "neutral", "range",
        {scn["symbol"]: scn["mp"]}, pf, ev2)
    assert H.normalize(sig) == H.normalize(sig2)
    assert H._clean(events) == H._clean(ev2) and all(isinstance(e, dict) for e in events)


def test_real_spawn_broken_pool_rebuilds():
    pool1 = aw.get_healthy_pool()
    with pytest.raises(BrokenProcessPool):
        pool1.submit(aw._selftest_crash).result(timeout=60)
    time.sleep(0.5)
    pool2 = aw.get_healthy_pool()                            # 自动重建
    assert pool2 is not pool1
    assert isinstance(pool2.submit(aw._ping).result(timeout=60), int)


def test_real_spawn_stuck_worker_killed_and_main_continues():
    pool = aw.get_healthy_pool()
    fut = pool.submit(aw._selftest_sleep, 120)
    time.sleep(1.0)
    procs = list(pool._processes.values())
    t0 = time.monotonic()
    aw.reset_pool(kill=True)
    assert time.monotonic() - t0 < 10
    for p in procs:
        p.join(timeout=10)
    assert all(not p.is_alive() for p in procs)              # worker 被强制终止
    pool2 = aw.get_healthy_pool()                            # 主进程继续，拿到新池
    assert pool2 is not pool and isinstance(pool2.submit(aw._ping).result(timeout=60), int)


def test_real_spawn_run_parallel_end_to_end(monkeypatch):
    """prefetch 在主进程（mock），分析在真实 worker 进程，事件回主进程落盘（mock 记录）。"""
    scn = H.make_scenario(2)
    rec = H.Rec()
    H.install(sc, scn, monkeypatch, rec, force=None)         # 真实 judge；REST/state 全 mock
    from shared import liq_cache
    monkeypatch.setattr(liq_cache, "get_liq_zones_cached", lambda *a, **k: [])
    t_sig = H.normalize(H.run_impl(sc, scn)); t_an = list(rec.analytics)
    rec.analytics.clear(); rec.shadow.clear()
    res, unf = sc._run_parallel_process_pool(_jobs(scn), {scn["symbol"]: scn["mp"]}, 2, 60)
    assert unf == [] and len(res) == 1 and H.normalize(res[0][1]) == t_sig
    assert rec.analytics == t_an and len(t_an) > 0           # 拒绝记录由主进程写，内容与线程池路径一致
