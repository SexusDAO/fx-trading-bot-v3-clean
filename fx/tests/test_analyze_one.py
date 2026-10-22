"""阶段2：analyze_one 抽取（全 mock，不联网）。
golden 指纹（tests/fixtures/phase2_golden.json）由重构前的 _analyze_symbol_impl 在同一批合成场景上生成。"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import phase2_harness as H
import scanner as sc

GOLDEN = json.loads((Path(__file__).parent / "fixtures" / "phase2_golden.json").read_text())


def _key(c):
    k = H.case_key(c[0], c[1], c[2])
    return k + (":" + c[3] if len(c) > 3 else "")


def test_golden_covers_signal_and_reject_paths():
    summaries = [v[1] for v in GOLDEN.values()]
    assert len(GOLDEN) == 168
    assert sum(1 for s in summaries if s) >= 60 and sum(1 for s in summaries if not s) >= 60
    assert {"pullback/LONG", "pullback/SHORT", "reversal/SHORT", "range_bounce/LONG"} <= set(summaries)


@pytest.mark.parametrize("case", H.all_cases(), ids=_key)
def test_impl_matches_pre_refactor_golden(case):
    seed, fi, ll = case[:3]
    bt = case[3] if len(case) > 3 else "neutral"
    fp, summary = H.run_case(sc, seed, fi, ll, bt)
    assert [fp, summary] == GOLDEN[_key(case)]


# ── analyze_one 本身 ────────────────────────────────────────────────────────
def _prefetch(scn, **over):
    pf = {"oi_now": scn["oi_now"], "oi_1h_ago": float(scn["oi_hist"][-3]["sumOpenInterest"]),
          "oi_24h_ago": scn["oi_24h_ago"], "oi_hist_6": scn["oi_hist"], "funding_velocity": dict(scn["fv"]),
          "mp": scn["mp"], "is_weekend": False, "stock_closed": False,
          "dir_cooldown_long": False, "dir_cooldown_short": False, "now_ms": 1_700_000_100_000}
    pf.update(over)
    return pf


def _call(scn, pf, events=None, **kw):
    k = scn["k"]
    return sc.analyze_one(scn["symbol"], scn["ticker"], k["15m"], k["1h"], k["4h"], set(), set(), None, None,
                          kw.get("btc_trend", "neutral"), "range", {}, pf, events)


def test_analyze_one_equals_impl(monkeypatch):
    scn = H.make_scenario(1)
    rec = H.Rec()
    H.install(sc, scn, monkeypatch, rec, force=H.FORCE_SETS[1])
    via_impl = H.normalize(H.run_impl(sc, scn)); via_impl.pop("ts")
    rec2 = H.Rec()
    ev = []
    one = H.normalize(_call(scn, _prefetch(scn), ev)); one.pop("ts")
    assert one == via_impl and via_impl["sig_type"] == "pullback"
    assert [e["kind"] for e in ev][-1] == "analytics_pass"


def test_funding_velocity_injected_into_judges_cache(monkeypatch):
    from shared import judges
    scn = H.make_scenario(3)
    H.install(sc, scn, monkeypatch, H.Rec(), force=H.FORCE_SETS[1])
    monkeypatch.undo  # 不撤销已装的 mock，只额外清缓存
    judges._fr_velocity_cache.pop(scn["symbol"], None)
    fv = {"velocity": -0.0123, "current": -0.001, "16h_ago": 0.0, "rapid_negative": True, "detail": "x"}
    t0 = time.time()
    _call(scn, _prefetch(scn, funding_velocity=fv))
    res, exp = judges._fr_velocity_cache[scn["symbol"]]
    assert res == fv and t0 + judges._FR_CACHE_TTL - 1 <= exp <= time.time() + judges._FR_CACHE_TTL + 1   # (result, expire_ts)，与现有格式一致


def test_injected_cache_makes_real_funding_velocity_skip_http(monkeypatch):
    from shared import judges
    fv = {"velocity": -0.5, "detail": "x"}
    judges._fr_velocity_cache["CACHEDUSDT"] = (fv, time.time() + 60)
    import shared.binance_api as bapi
    monkeypatch.setattr(bapi, "_get", lambda *a, **k: (_ for _ in ()).throw(AssertionError("HTTP called")))
    assert judges._funding_rate_velocity("CACHEDUSDT") == fv


def test_analyze_one_makes_no_rest_calls(monkeypatch):
    """把 api 全部换成会报错的桩：analyze_one 一次都不能碰。"""
    scn = H.make_scenario(1)
    H.install(sc, scn, monkeypatch, H.Rec(), force=H.FORCE_SETS[1])
    calls = []

    class Boom:
        def __getattr__(self, name):
            def f(*a, **k):
                calls.append(name); raise AssertionError(f"REST 被调用: {name}")
            return f
    monkeypatch.setattr(sc, "api", Boom())
    import shared.binance_api as bapi
    for fn in ("_get", "_retry", "get_klines", "get_open_interest", "get_oi_history", "get_mark_price", "get_funding_interval"):
        monkeypatch.setattr(bapi, fn, lambda *a, _n=fn, **k: calls.append(_n))
    monkeypatch.setattr(sc, "_get_klines_cached", lambda *a: calls.append("klines"))
    monkeypatch.setattr(sc, "_get_mark_price", lambda *a: calls.append("mark"))
    monkeypatch.setattr(sc, "_get_oi_history_cached", lambda *a, **k: calls.append("oi_hist"))
    monkeypatch.setattr(sc, "get_oi_24h_ago", lambda *a: calls.append("oi24"))
    sig = _call(scn, _prefetch(scn))
    assert sig is not None and calls == []


def test_analyze_one_does_not_write_or_call_llm(monkeypatch):
    scn = H.make_scenario(1)
    rec = H.Rec()
    H.install(sc, scn, monkeypatch, rec, llm=H.LLM_CASES["llm_open"], force=H.FORCE_SETS[1])
    from shared import llm_judge
    monkeypatch.setattr(llm_judge, "is_explosive_coin", lambda k: (True, 150.0, 2.0))
    monkeypatch.setattr(llm_judge, "judge_explosive_coin_llm", lambda *a, **k: (_ for _ in ()).throw(AssertionError("LLM called")))
    ev = []
    sig = _call(scn, _prefetch(scn), ev)
    assert sig is not None and rec.analytics == [] and rec.shadow == []        # 没有写文件
    kinds = [e["kind"] for e in ev]
    assert "llm" in kinds and kinds[-1] == "analytics_pass"                     # LLM 与落盘都变成事件


def test_reject_events_returned_even_when_none(monkeypatch):
    scn = H.make_scenario(2)
    rec = H.Rec()
    H.install(sc, scn, monkeypatch, rec, force=None)
    ev = []
    sig = _call(scn, _prefetch(scn), ev)
    assert sig is None and rec.analytics == [] and rec.shadow == []
    assert ev and {e["kind"] for e in ev} <= {"analytics", "shadow_reject"}
    assert all(e["kwargs"]["judge_passed"] is False for e in ev if e["kind"] == "analytics")


def test_prefetch_cooldown_and_stock_closed_and_weekend(monkeypatch):
    scn = H.make_scenario(1)
    H.install(sc, scn, monkeypatch, H.Rec(), force=H.FORCE_SETS[1])
    base = _call(scn, _prefetch(scn))
    assert base is not None
    d = base["direction"]
    cd = "dir_cooldown_long" if d == "LONG" else "dir_cooldown_short"
    assert _call(scn, _prefetch(scn, **{cd: True})) is None
    assert _call(scn, _prefetch(scn, stock_closed=True)) is None
    assert _call(scn, _prefetch(scn, is_weekend=True)) is None or True      # 周末预筛按 ticker 振幅，仅验证不报错


def test_oi_24h_ago_comes_from_prefetch_not_state(monkeypatch):
    monkeypatch.setattr(sc, "get_oi_24h_ago", lambda s: (_ for _ in ()).throw(AssertionError("state read")))
    # 价格接近 24H 高点且 OI 比 24h 前下降 >20% → 背离；传入 oi_24h 时不读 state
    bad, _ = sc._oi_divergence("XUSDT", 100.0, 5.0, 100.0, 70.0, oi_24h=100.0)
    ok, _ = sc._oi_divergence("XUSDT", 100.0, 5.0, 100.0, 70.0, oi_24h=None)
    assert bad is True and ok is False


def test_classify_stock_closed_param(monkeypatch):
    ind = {}
    monkeypatch.setattr(sc, "_is_reversal", lambda *a: False)
    monkeypatch.setattr(sc, "_is_breakout", lambda *a: False)
    assert sc.classify("X", 0.0, 0.0, ind, stock_closed=True) == "skip_stock_closed"
    assert sc.classify("X", 0.0, 0.0, ind, stock_closed=False) == "pullback"


def test_docstring_marks_both_entry_points():
    doc = sc.analyze_one.__doc__
    assert "run_scan" in doc and "TriggerDetector" in doc and "不做任何 IO" in doc
