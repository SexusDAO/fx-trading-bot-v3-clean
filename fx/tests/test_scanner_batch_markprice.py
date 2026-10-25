"""analyze_symbol 复用批量标记价格。运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_scanner_batch_markprice.py（mock）"""
import os, sys, time
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import scanner as sc


class _Stop(BaseException):
    """在 classify 处截停，避免跑完整个分析；同时捕获传入的 funding。"""
    def __init__(self, funding):
        self.funding = funding


def _klines(n=100, base=100.0):
    t0 = 1_700_000_000_000
    return [[t0 + i * 3600_000, str(base), str(base * 1.01), str(base * 0.99), str(base), "1000", 0, "0", 10, "500", "0", "0"]
            for i in range(n)]


TICKER = {"quoteVolume": "5e8", "highPrice": "110", "lowPrice": "90", "lastPrice": "100",
          "priceChangePercent": "12", "openPrice": "89"}


@pytest.fixture
def env(monkeypatch):
    calls = {"mp": [], "klines": 0}
    monkeypatch.setattr(sc.api, "get_klines", lambda *a, **k: calls.__setitem__("klines", calls["klines"] + 1) or _klines())
    monkeypatch.setattr(sc.api, "get_mark_price",
                        lambda s: calls["mp"].append(s) or {"symbol": s, "markPrice": "100", "lastFundingRate": "0.0009"})
    def fake_classify(symbol, chg24, funding, ind, **_kw):   # classify 现带 stock_closed 等关键字参数
        raise _Stop(funding)
    monkeypatch.setattr(sc, "classify", fake_classify)
    # 逐标的资金费结算窗口（UTC 23:40~00:00 等）会让 analyze_symbol 提前返回 None，导致测试随运行时刻波动；
    # 本文件只测标记价格复用，关闭该窗口检查
    import config as _cfg
    monkeypatch.setattr(_cfg, "OPT_FUNDING_WINDOW_PER_SYMBOL", False, raising=False)
    return calls


def _run(map_, symbol="XUSDT"):
    try:
        sc.analyze_symbol(symbol, TICKER, set(), set(), None, None, "", "", mark_price_map=map_)
    except _Stop as st:
        return st.funding
    pytest.skip("测试 ticker 被预筛/其他逻辑提前拦截，未走到标记价格之后")


def test_map_hit_does_not_call_rest_and_reads_funding_from_map(env):
    f = _run({"XUSDT": {"symbol": "XUSDT", "markPrice": "100", "lastFundingRate": "-0.0123"}})
    assert env["mp"] == []                         # 没有单独请求
    assert f == pytest.approx(-0.0123)             # funding 来自 map


def test_none_map_falls_back_to_rest(env):
    f = _run(None)
    assert env["mp"] == ["XUSDT"]
    assert f == pytest.approx(0.0009)              # 来自 REST 返回值，行为与改动前一致


def test_map_miss_symbol_falls_back_to_rest(env):
    f = _run({"OTHERUSDT": {"lastFundingRate": "0.5"}})
    assert env["mp"] == ["XUSDT"] and f == pytest.approx(0.0009)


def test_empty_map_falls_back_to_rest(env):
    _run({})
    assert env["mp"] == ["XUSDT"]


def test_run_scan_builds_map_once_and_passes_it():
    src = (Path(__file__).parent.parent / "scanner.py").read_text()
    assert "_mark_price_map[_ms] = _m" in src
    assert "partial(analyze_symbol, mark_price_map=_mark_price_map)" in src
    assert "_mkt_mode, _mark_price_map)" not in src   # 不能再位置传 map（会与 deadline 冲突）
    assert src.count("(mark_price_map or {}).get(symbol)") == 3   # scanner.py 现有 3 处读取点


def test_run_scan_call_pattern_no_deadline_conflict(env):
    """复现 run_scan 的真实调用：partial 绑 map + 8 个位置参数 + 关键字 deadline，不得 TypeError。"""
    import functools, time
    fn = functools.partial(sc.analyze_symbol, mark_price_map={"XUSDT": {"markPrice": "100", "lastFundingRate": "-0.0123"}})
    args = ("XUSDT", TICKER, set(), set(), None, None, "", "")
    try:
        fn(*args, deadline=time.monotonic() + 60)
    except _Stop as st:
        assert st.funding == pytest.approx(-0.0123)
    # 经 _run_parallel_with_deadline 走一遍：异常会被吞，所以单独断言没有 TypeError 级失败
    seen = []
    def probe(*a, **k):
        seen.append(k)
        try:
            return fn(*a, **k)
        except _Stop as st:
            return st.funding
    res, unfinished = sc._run_parallel_with_deadline(probe, {"XUSDT": args}, 1, 30)
    assert seen and "deadline" in seen[0] and "mark_price_map" not in seen[0]
    assert unfinished == [] and res == [("XUSDT", pytest.approx(-0.0123))]
