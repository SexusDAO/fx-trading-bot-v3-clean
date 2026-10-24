"""tools/backtest.py（方向准确率回测）的纯函数：判定、基线、统计、网格、市值分组。不联网。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools import backtest as B  # noqa: E402

H = 3_600_000


def bar(i, o, h, l, c):
    return [i * H, str(o), str(h), str(l), str(c), "1", i * H + H - 1, "1", 0, "0", "0", "0"]


def flat(n, p=100.0):
    return [bar(i, p, p, p, p) for i in range(n)]


def test_long_hit_when_high_touches_one_atr():
    k = flat(6)
    k[2] = bar(2, 100, 101.0, 99.8, 100.5)          # atr=1 → 目标 101
    e = B.evaluate(k, 0, "LONG", 100.0, 1.0, 4)
    assert e["hit"] and e["hit_bar"] == 2 and e["win"] is (e["ret_pct"] > 0)


def test_short_hit_uses_low():
    k = flat(6)
    k[1] = bar(1, 100, 100.1, 98.9, 99.5)
    e = B.evaluate(k, 0, "SHORT", 100.0, 1.0, 3)
    assert e["hit"] and e["hit_bar"] == 1


def test_timestop_win_is_close_vs_entry_by_direction():
    k = flat(6)
    k[4] = bar(4, 100, 100.4, 99.6, 100.1)            # 没触及 1ATR，收盘略高于入场
    assert B.evaluate(k, 0, "LONG", 100.0, 1.0, 4)["win"] is True
    assert B.evaluate(k, 0, "SHORT", 100.0, 1.0, 4)["win"] is False
    assert B.evaluate(k, 0, "LONG", 100.0, 1.0, 4)["hit"] is False


def test_unfavorable_close_exactly_zero_is_not_win():
    assert B.evaluate(flat(6), 0, "LONG", 100.0, 1.0, 4)["win"] is False


def test_insufficient_data_returns_none():
    k = flat(4)
    assert B.evaluate(k, 0, "LONG", 100.0, 1.0, 4) is None           # idx+n 越界
    assert B.evaluate(flat(8), 0, "LONG", 100.0, 0.0, 2) is None     # ATR 无效


def test_gap_in_data_returns_none():
    k = flat(6)
    k.pop(3)                                                           # 缺一根 K 线 → 第 idx+n 根的 open_time 错位
    assert B.evaluate(k, 0, "LONG", 100.0, 1.0, 4) is None


def test_mfe_mae_in_atr_units():
    k = flat(5)
    k[1] = bar(1, 100, 100.5, 99.0, 100)
    e = B.evaluate(k, 0, "LONG", 100.0, 1.0, 2)
    assert abs(e["mfe_atr"] - 0.5) < 1e-9 and abs(e["mae_atr"] - 1.0) < 1e-9


def test_n_for_defaults_and_override():
    assert B.n_for("reversal", None) == 4 and B.n_for("pullback", None) == 2 and B.n_for("ema_trend", None) == 6
    assert B.n_for("reversal", 8) == 8 and B.n_for("stop_hunt", None) == B.OTHER_DEFAULT_N


def test_dedupe_blocks_overlap_within_n():
    s = lambda idx, st="reversal": {"symbol": "X", "strategy": st, "idx": idx, "ts": idx}
    out = B.dedupe([s(10), s(12), s(14), s(10, "pullback")], lambda st: 4)
    assert [(t["strategy"], t["idx"]) for t in out] == [("reversal", 10), ("pullback", 10), ("reversal", 14)]


def test_wilson_bounds():
    lo, hi = B.wilson(5, 10)
    assert 0.2 < lo < 0.5 < hi < 0.8 and B.wilson(0, 0) is None


def _t(direction="LONG", hit=True, win=True, ret=0.5, **kw):
    ev = {"hit": hit, "hit_bar": 1 if hit else None, "ret_pct": ret, "win": win, "mfe_atr": 1.0, "mae_atr": 0.2}
    t = {"strategy": "reversal", "direction": direction, "n_hours": 4, "evals": {4: ev, 2: ev}, "session": "亚盘(UTC0-8)",
         "mcap_group": "大市值(>10亿)", "rsi_1h": 18.0, "chg24": -6.0, "vol_ratio": 0.5}
    t.update(kw)
    return t


def test_block_accuracy_excess_and_insufficient():
    base = {("LONG", 4): {"acc": 0.4, "win": 0.5, "n": 100}}
    trades = [_t(), _t(hit=False, win=False, ret=-0.3), _t(evals={4: None})]
    b = B.block(trades, base)
    assert b["signals"] == 3 and b["valid"] == 2 and b["insufficient"] == 1
    assert b["accuracy"] == 0.5 and abs(b["excess_accuracy"] - 0.1) < 1e-9 and b["timestop_win_rate"] == 0.5


def test_block_empty_is_none_not_zero():
    assert B.block([], {})["accuracy"] is None


def test_sensitivity_filters_only_tighten():
    base = {("LONG", 4): {"acc": 0.4, "win": 0.5, "n": 10}, ("SHORT", 4): {"acc": 0.4, "win": 0.5, "n": 10}}
    trades = [_t(rsi_1h=12.0), _t(rsi_1h=22.0), _t(rsi_1h=28.0, chg24=-9.0), _t(direction="SHORT", rsi_1h=85.0, chg24=6.0)]
    s = B.sensitivity(trades, base)
    rsi = {r["value"]: r for r in s["grids"]["rsi_1h"]["rows"]}
    # LONG: rsi<X；SHORT(rsi=85): rsi>100-X → X=15 不含，X>=20 起含
    assert [rsi[x]["signals"] for x in (15, 20, 25, 30)] == [1, 2, 3, 4]
    assert rsi[30]["same_as_unfiltered"] is True
    chg = {r["value"]: r for r in s["grids"]["chg24"]["rows"]}
    assert chg[8]["signals"] == 1


def test_mcap_group_and_base_asset():
    assert B.mcap_group(5e7).startswith("小") and B.mcap_group(5e8).startswith("中") and B.mcap_group(5e9).startswith("大")
    assert B.mcap_group(None) == "未知"
    assert B.base_asset("1000PEPEUSDT") == "PEPE" and B.base_asset("BTCUSDT") == "BTC"


def test_aggregate_4h_requires_four_bars():
    k = [bar(i, 1, 1, 1, 1) for i in range(8)]
    assert len(B.aggregate_4h(k)) == 2 and len(B.aggregate_4h(k[:7])) == 1


def test_session_boundaries():
    assert B.session_of(0).startswith("亚盘") and B.session_of(8 * H).startswith("欧盘") and B.session_of(16 * H).startswith("美盘")
