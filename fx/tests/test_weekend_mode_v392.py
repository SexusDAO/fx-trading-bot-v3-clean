"""v3.92 周末模式实盘改造：门槛参数化 / 收尾 / 上限 / 有效期 / 仓位缩减 / breakout medium 放开（全 mock）"""
import logging
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import scanner as sc
from test_phase5_dispatch import FIXED_T, offline, watcher, _budget  # noqa: F401  (fixtures)

WEEKDAY_T = FIXED_T                          # 2027-01-15 周五 08:00 UTC
SAT_T     = FIXED_T + 86400                  # 周六 08:00 UTC
SUN18_T   = FIXED_T + 2 * 86400 + 10 * 3600  # 周日 18:00 UTC


@pytest.fixture(autouse=True)
def _real_shared_modules(monkeypatch):
    """其他测试文件可能把 shared.state / shared.risk_manager 换成桩模块，这里确保用真实模块。"""
    import importlib
    import shared
    for name, attr in (("shared.state", "get_all_positions"), ("shared.risk_manager", "get_market_mode")):
        mod = sys.modules.get(name)
        if mod is None or not hasattr(mod, attr):
            monkeypatch.delitem(sys.modules, name, raising=False)
            real = importlib.import_module(name)
            monkeypatch.setitem(sys.modules, name, real)
            monkeypatch.setattr(shared, name.split(".")[1], real, raising=False)


def _mk(sym, sig_type="pullback", strength="medium", cap=0.5, atr_ratio=3.0, **reason):
    r = {"signal_strength": strength}
    r.update(reason)
    ind = {"price": 100.0, "atr_1h": 2.0, "atr_ratio": atr_ratio, "ema20_1h": 99.0, "ema50_1h": 98.0,
           "rsi_1h": 50.0, "rsi_4h": 55.0, "ema20_4h": 97.0, "ema50_4h": 95.0, "vol_ratio": 1.2,
           "high_24h": 105.0, "low_24h": 95.0, "funding": 0.0001}
    return {"symbol": sym, "direction": "LONG", "sig_type": sig_type, "chg24": 2.0, "funding": 0.0001,
            "ind": ind, "reason": r, "oi_now": 1e6, "ts": int(FIXED_T * 1000),
            "position_size_cap": cap, "signal_strength": strength}


@pytest.fixture
def run(monkeypatch, watcher):
    """固定最终分数后调用 _filter_and_register_signals，返回 (watcher.calls, result)。"""
    sc._WEEKEND_POS_COUNT = 0
    sc._WEEKEND_WINDOW_START = 0.0

    def _go(sigs, t, score, n_pre=0, bi_over=None):
        monkeypatch.setattr(sc.time, "time", lambda: t)
        import shared.coinglass_api as _cga   # 函数内局部导入，需在源模块打桩；防 time.time 固定后 CG 限速无限等待
        monkeypatch.setattr(_cga, "score_symbol_cg_bonus", lambda *a, **k: 0)
        monkeypatch.setattr(sc, "_apply_score_bonuses",
                            lambda *a, **k: (score, {"taker": 0, "range": 0, "volprice": 0, "hub_bonus": 0}))
        bi = _budget()
        bi["now"] = sc.datetime.fromtimestamp(t, sc.timezone.utc)
        bi["_dyn_max_long"] = 10
        bi["_mark_price_map"] = {s["symbol"]: {"markPrice": "100.0", "lastFundingRate": "0.0001"} for s in sigs}
        bi.update(bi_over or {})
        if n_pre:
            sc._weekend_counter_sync(bi["now"])
            sc._WEEKEND_POS_COUNT = n_pre
        watcher.calls.clear()
        res = {"signals_final": 0, "placed": 0, "rejected": []}
        sc._filter_and_register_signals(sigs, set(), set(), "range", "neutral", bi, "scan",
                                        signal_watcher=watcher, result=res)
        return [c[0] for c in watcher.calls], [c[1] for c in watcher.calls], res
    return _go


def _syms(calls):
    return {c["symbol"] for c in calls}


# ── 工作日回归 ──────────────────────────────────────────────
def test_T01_weekday_pullback_medium_55_pass(run):
    calls, _, _ = run([_mk("AAAUSDT")], WEEKDAY_T, 55)
    assert "AAAUSDT" in _syms(calls)


def test_T02_weekday_breakout_medium_pass_unchanged(run):
    calls, _, _ = run([_mk("AAAUSDT", "breakout", volume_ratio=1.0)], WEEKDAY_T, 55)
    assert "AAAUSDT" in _syms(calls)


def test_T03_weekday_reversal_medium_65_pass(run):
    calls, _, _ = run([_mk("AAAUSDT", "reversal")], WEEKDAY_T, 65)
    assert "AAAUSDT" in _syms(calls)


def test_T04_weekday_cap_not_scaled(run):
    calls, _, _ = run([_mk("AAAUSDT", cap=0.4)], WEEKDAY_T, 60)
    assert calls[0]["position_size_cap"] == 0.4
    assert not calls[0].get("weekend_mode")


# ── 周末评分门槛 ────────────────────────────────────────────
@pytest.mark.parametrize("st,ok", [("pullback", True), ("ema_trend", True), ("reversal", False),
                                   ("range_bounce", False), ("oi_divergence", False)])
def test_T05_T09_weekend_medium_threshold(run, st, ok):
    calls, _, res = run([_mk("AAAUSDT", st)], SAT_T, 62)
    assert ("AAAUSDT" in _syms(calls)) is ok
    if not ok:
        assert any("周末低分过滤" in r["reason"] for r in res["rejected"])


# ── 周末 breakout medium（2-C）──────────────────────────────
def test_T10_weekend_breakout_medium_vol_pass(run):
    calls, _, _ = run([_mk("AAAUSDT", "breakout", volume_ratio=3.5)], SAT_T, 72)
    assert "AAAUSDT" in _syms(calls)


def test_T11_weekend_breakout_medium_reject(run):
    calls, _, res = run([_mk("AAAUSDT", "breakout", volume_ratio=2.5, oi_confirm=False)], SAT_T, 72)
    assert not calls
    assert any("breakout" in r["reason"] for r in res["rejected"])


def test_T12_weekend_breakout_medium_oi_pass(run):
    calls, _, _ = run([_mk("AAAUSDT", "breakout", volume_ratio=2.5, oi_confirm=True)], SAT_T, 72)
    assert "AAAUSDT" in _syms(calls)


def test_T13_weekday_breakout_medium_pass_unchanged(run):
    calls, _, _ = run([_mk("AAAUSDT", "breakout", volume_ratio=3.5)], WEEKDAY_T, 55)
    assert "AAAUSDT" in _syms(calls)


# ── 保护机制 ────────────────────────────────────────────────
def test_T14_sunday_cutoff(run):
    calls, _, _ = run([_mk("AAAUSDT", strength="strong")], SUN18_T, 80)
    assert not calls


def test_T15_weekend_cap_3(run):
    calls, _, res = run([_mk("AAAUSDT", strength="strong")], SAT_T, 80, n_pre=3)
    assert not calls
    assert any("周末开仓上限" in r["reason"] for r in res["rejected"])


def test_T15b_weekend_counter_increments(run):
    sigs = [_mk(s, strength="strong") for s in ("AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT")]
    calls, _, _ = run(sigs, SAT_T, 80, bi_over={"_dyn_max_long": 10})
    assert len(calls) == 3
    assert sc._WEEKEND_POS_COUNT == 3
    assert all(c.get("weekend_mode") for c in calls)


def test_T16_weekend_low_vol_expire_4h(run):
    _, kws, _ = run([_mk("AAAUSDT", strength="strong", atr_ratio=1.5)], SAT_T, 80)
    assert kws[0]["expire_ts"] == pytest.approx(SAT_T + 4 * 3600)


def test_T16b_weekday_low_vol_expire_8h(run):
    _, kws, _ = run([_mk("AAAUSDT", strength="strong", atr_ratio=1.5)], WEEKDAY_T, 80)
    assert kws[0]["expire_ts"] == pytest.approx(WEEKDAY_T + 8 * 3600)


def test_T16c_sunday_expire_clamped_17(run):
    sun14 = FIXED_T + 2 * 86400 + 6 * 3600   # 周日 14:00，正常档 4H → 18:00，钳制到 17:00
    _, kws, _ = run([_mk("AAAUSDT", strength="strong", atr_ratio=3.0)], sun14, 80)
    assert kws[0]["expire_ts"] == pytest.approx(sun14 + 3 * 3600, abs=1)


# ── 仓位缩减（2-A）──────────────────────────────────────────
def test_T17_weekend_strong_scale(run):
    calls, _, _ = run([_mk("AAAUSDT", strength="strong", cap=0.60)], SAT_T, 80)
    assert calls[0]["position_size_cap"] == pytest.approx(0.45)


def test_T18_weekend_medium_scale(run):
    calls, _, _ = run([_mk("AAAUSDT", cap=0.40)], SAT_T, 72)
    assert calls[0]["position_size_cap"] == pytest.approx(0.20)


def test_T18b_scale_per_signal_strength(run):
    sigs = [_mk("AAAUSDT", strength="strong", cap=0.60), _mk("BBBUSDT", cap=0.40)]
    calls, _, _ = run(sigs, SAT_T, 80)
    caps = {c["symbol"]: c["position_size_cap"] for c in calls}
    assert caps == {"AAAUSDT": pytest.approx(0.45), "BBBUSDT": pytest.approx(0.20)}


def test_T19_weekend_small_cap_warning(run, caplog):
    with caplog.at_level(logging.WARNING):
        calls, _, _ = run([_mk("AAAUSDT", cap=0.20)], SAT_T, 72)
    assert calls and calls[0]["position_size_cap"] == pytest.approx(0.10)
    assert any("MIN_NOTIONAL_GUARD" in r.getMessage() for r in caplog.records)


# ── funding_arb（2-B）───────────────────────────────────────
@pytest.fixture
def arb(monkeypatch):
    import shared.state as st
    import shared.judges as jg
    import shared.binance_api as ba
    import shared.risk_manager as rm
    sc._WEEKEND_POS_COUNT = 0
    sc._WEEKEND_WINDOW_START = 0.0
    monkeypatch.setattr(config, "FUNDING_ARB_ENABLED", True)
    monkeypatch.setattr(st, "is_circuit_open", lambda: False)
    monkeypatch.setattr(st, "is_funding_arb_paused", lambda: False)
    monkeypatch.setattr(st, "get_all_positions", lambda: [])
    monkeypatch.setattr(sc, "get_all_positions", lambda: [])
    monkeypatch.setattr(rm, "get_market_mode", lambda: "range")
    monkeypatch.setattr(sc.api, "get_all_mark_prices",
                        lambda *a, **k: [{"symbol": "ARBUSDT", "lastFundingRate": "0.003"}])
    monkeypatch.setattr(ba, "get_funding_interval", lambda s: 8)
    monkeypatch.setattr(ba, "get_next_funding_time", lambda s: 0)
    monkeypatch.setattr(sc.api, "get_klines", lambda *a, **k: [[0, 1, 1, 1, 1, 1]] * 100)
    monkeypatch.setattr(sc.api, "get_open_interest", lambda s: {"openInterest": "1"}, raising=False)
    monkeypatch.setattr(sc.api, "get_open_interest_hist", lambda *a, **k: [], raising=False)
    monkeypatch.setattr(sc, "calc_indicators", lambda *a, **k: {})
    monkeypatch.setattr(sc, "_get_recent_funding_rates", lambda *a, **k: [])
    monkeypatch.setattr(jg, "judge_funding_arb",
                        lambda *a, **k: (True, {"direction": "SHORT", "position_size_cap": 0.30,
                                                "signal_strength": "strong"}))
    sent = []
    fake_exec = types.SimpleNamespace(execute_signal_sync=lambda s, w: sent.append(dict(s)) or True)
    monkeypatch.setitem(sys.modules, "executor", fake_exec)

    def _go(weekend):
        monkeypatch.setattr(sc, "_is_weekend_period", lambda now: weekend)
        monkeypatch.setattr(sc, "_is_weekend_cutoff", lambda now: False)
        sent.clear()
        sc.run_funding_arb_scan(dry_run=False, signal_watcher=None)
        return sent
    return _go


def test_T20_weekend_funding_arb_scale(arb):
    sent = arb(True)
    assert sent and sent[0]["position_size_cap"] == pytest.approx(0.15)
    assert sent[0].get("weekend_mode") is True


def test_T21_weekday_funding_arb_unchanged(arb):
    sent = arb(False)
    assert sent and sent[0]["position_size_cap"] == pytest.approx(0.30)
    assert not sent[0].get("weekend_mode")


# ── 纯函数 ─────────────────────────────────────────────────
def test_weekend_window_start_and_reset():
    sat = sc.datetime.fromtimestamp(SAT_T, sc.timezone.utc)
    fri22 = sc.datetime.fromtimestamp(FIXED_T + 14 * 3600, sc.timezone.utc)
    assert sc._weekend_window_start_ts(sat) == sc._weekend_window_start_ts(fri22) == fri22.timestamp()
    sc._WEEKEND_WINDOW_START = 0.0
    sc._WEEKEND_POS_COUNT = 2
    assert sc._weekend_counter_sync(sat) == 0       # 新窗口归零
    sc._WEEKEND_POS_COUNT = 2
    assert sc._weekend_counter_sync(sat) == 2       # 同窗口不归零
