"""扫描前有界预热 prefetch_liq_cache_bounded：只处理过期标的、超时不再等、复用已知标记价。"""
import time
import threading

import shared.liq_cache as L


def _fresh(sym, ranges=("3d",)):
    with L._CACHE_LOCK:
        for r in ranges:
            L._CACHE[f"{sym}:{r}"] = {"ts": time.time(), "LONG": [], "SHORT": []}


def _clear(*syms):
    with L._CACHE_LOCK:
        for s in syms:
            for r in L._PREFETCH_RANGES:
                L._CACHE.pop(f"{s}:{r}", None)


def test_only_stale_symbols_are_prefetched(monkeypatch):
    _clear("AAAUSDT", "BBBUSDT")
    _fresh("AAAUSDT")
    seen = []
    monkeypatch.setattr(L, "_prefetch_one", lambda sym, ranges=None, price=0.0: seen.append((sym, price)) or True)
    st = L.prefetch_liq_cache_bounded(["AAAUSDT", "BBBUSDT"], 5.0,
                                      price_map={"BBBUSDT": {"markPrice": "12.5"}})
    assert seen == [("BBBUSDT", 12.5)]          # 新鲜的不处理；已知标记价被传入
    assert st["total"] == 2 and st["stale"] == 1 and st["updated"] == 1 and st["timed_out"] == 0
    _clear("AAAUSDT", "BBBUSDT")


def test_all_fresh_returns_without_work(monkeypatch):
    _clear("CCCUSDT")
    _fresh("CCCUSDT")
    monkeypatch.setattr(L, "_prefetch_one", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not run")))
    st = L.prefetch_liq_cache_bounded(["CCCUSDT"], 5.0)
    assert st["stale"] == 0 and st["done"] == 0
    _clear("CCCUSDT")


def test_deadline_stops_waiting(monkeypatch):
    syms = [f"S{i}USDT" for i in range(6)]
    _clear(*syms)
    gate = threading.Event()
    monkeypatch.setattr(L, "_prefetch_one", lambda *a, **k: gate.wait(3) or True)
    t0 = time.time()
    st = L.prefetch_liq_cache_bounded(syms, 0.3)
    assert time.time() - t0 < 2.0                 # 超时后不再等在途请求
    assert st["timed_out"] >= 1 and st["done"] < len(syms)
    gate.set()
    _clear(*syms)


def test_failure_in_one_symbol_does_not_break_others(monkeypatch):
    syms = ["DDDUSDT", "EEEUSDT"]
    _clear(*syms)

    def fake(sym, ranges=None, price=0.0):
        if sym == "DDDUSDT":
            raise RuntimeError("boom")
        return True
    monkeypatch.setattr(L, "_prefetch_one", fake)
    st = L.prefetch_liq_cache_bounded(syms, 5.0)
    assert st["done"] == 2 and st["updated"] == 1
    _clear(*syms)
