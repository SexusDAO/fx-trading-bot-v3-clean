"""扫描超时/防重叠/事件循环延迟监控。
运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_scanner_timeout_overlap.py
全部 mock，不连交易所。main.py 导入时会执行 _ensure_deps，故用 ast 抽取函数而不 import main。"""
import ast, asyncio, os, sys, threading, time
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
_fx = Path(__file__).parent.parent
sys.path.insert(0, str(_fx))

import pytest
import scanner as sc

MAIN_SRC = (_fx / "main.py").read_text()


def _extract(name):
    tree = ast.parse(MAIN_SRC)
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            ns = {}
            exec(compile(ast.Module([n], []), "main_extract", "exec"), ns)
            return ns[name]
    raise AssertionError(name)


# ── Commit 1：线程池手动管理 + 真实 deadline ─────────────────────────────────
def test_stuck_symbol_does_not_block_round():
    release = threading.Event()
    def fn(sym, *a, deadline=None):
        if sym == "SLOW":
            release.wait(5)                       # 模拟卡住的标的
            return None
        return {"sym": sym}
    jobs = {s: (s,) for s in ["A", "B", "SLOW", "C"]}
    t0 = time.monotonic()
    results, unfinished = sc._run_parallel_with_deadline(fn, jobs, max_workers=4, total_timeout_sec=0.5)
    elapsed = time.monotonic() - t0
    release.set()
    assert elapsed < 2.0, f"卡住的标的拖住了整轮: {elapsed:.2f}s"       # 旧实现 with 块会等满 5s
    assert unfinished == ["SLOW"]
    assert sorted(s for s, r in results if r) == ["A", "B", "C"]


def test_queued_jobs_are_cancelled_after_deadline():
    started = []
    release = threading.Event()
    def fn(sym, *a, deadline=None):
        started.append(sym)
        release.wait(3)
        return None
    jobs = {s: (s,) for s in ["S1", "Q1", "Q2", "Q3"]}
    results, unfinished = sc._run_parallel_with_deadline(fn, jobs, max_workers=1, total_timeout_sec=0.3)
    release.set(); time.sleep(0.2)
    assert started == ["S1"]                       # 排队未开始的被 cancel_futures 取消，不会在放弃后继续跑
    assert set(unfinished) == {"S1", "Q1", "Q2", "Q3"}


def test_deadline_passed_to_fn_is_monotonic_future():
    seen = {}
    def fn(sym, *a, deadline=None):
        seen["d"] = deadline
        return 1
    t0 = time.monotonic()
    sc._run_parallel_with_deadline(fn, {"X": ("X",)}, 2, 10)
    assert t0 + 9 < seen["d"] < time.monotonic() + 11


def test_analyze_symbol_gives_up_before_any_request_when_over_budget(monkeypatch):
    calls = []
    monkeypatch.setattr(sc.api, "get_klines", lambda *a, **k: calls.append(a) or None)
    ticker = {"quoteVolume": "5e8", "highPrice": "110", "lowPrice": "90", "lastPrice": "100",
              "priceChangePercent": "12", "openPrice": "89"}
    # 整轮截止时间已过 → 不应发任何请求
    assert sc.analyze_symbol("XUSDT", ticker, set(), set(), None, None, "", "", deadline=time.monotonic() - 1) is None
    assert calls == []
    # 对照：给足时间则会发请求（若被预筛拦截则该对照无意义，用 xfail 提示而不是误报通过）
    sc.analyze_symbol("XUSDT", ticker, set(), set(), None, None, "", "", deadline=time.monotonic() + 60)
    if not calls:
        pytest.xfail("测试 ticker 被零成本预筛拦截，未能验证对照分支")


def test_scan_loop_no_longer_uses_with_threadpool_for_symbols():
    src = (_fx / "scanner.py").read_text()
    assert "_run_parallel_with_deadline(" in src
    assert "fut.result(timeout=SCAN_SYMBOL_TIMEOUT_SEC)" not in src
    assert "shutdown(wait=False, cancel_futures=True)" in src


# ── Commit 2：跳过未完成的上一轮 ─────────────────────────────────────────────
def test_wait_for_shield_keeps_inner_future_pending():
    """说明为什么必须用 shield：裸 wait_for 超时会取消 future → done()==True，防重叠检查会误判。"""
    async def go():
        loop = asyncio.get_running_loop()
        stop = threading.Event()
        f1 = loop.run_in_executor(None, stop.wait, 2)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(f1, 0.1)
        bare_done = f1.done()
        f2 = loop.run_in_executor(None, stop.wait, 2)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(f2), 0.1)
        shield_done = f2.done()
        stop.set(); await asyncio.sleep(0.1)
        return bare_done, shield_done, f2.done()
    bare, shielded, after = asyncio.run(go())
    assert bare is True and shielded is False and after is True


def test_loop_scan_source_has_skip_and_shield():
    assert "上一轮仍在运行，跳过本轮" in MAIN_SRC
    assert "_scan_future is not None and not _scan_future.done()" in MAIN_SRC
    assert "asyncio.wait_for(asyncio.shield(future), timeout=300)" in MAIN_SRC


# ── Commit 3：事件循环延迟监控 ───────────────────────────────────────────────
@pytest.mark.parametrize("actual,exp", [(0.1, None), (0.5, None), (0.51, "warn"), (2.0, "warn"), (2.01, "error")])
def test_classify_loop_latency(actual, exp):
    assert _extract("_classify_loop_latency")(actual) == exp


def test_latency_monitor_registered_in_tasks():
    assert "_loop_event_latency_monitor()" in MAIN_SRC and 'name="loop_latency"' in MAIN_SRC
    assert "事件循环延迟" in MAIN_SRC and "事件循环严重阻塞" in MAIN_SRC
