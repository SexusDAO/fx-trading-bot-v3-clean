"""TP2 盈亏比下限（RR_MIN_TP2）兜底：_enforce_tp2_rr 与 calc_sl_tp 出口的 tp_mode。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from shared import calc_sl_tp as C

RR = C._TP2_MIN_RR
E, SL = 100.0, 99.0   # sl_dist = 1


def z(lo, hi):
    return {"price_low": lo, "price_high": hi, "liq_usd": 1e6}


def test_long_ok_unchanged():
    assert C._enforce_tp2_rr(E, SL, 102.0, E + RR + 1, "LONG", [], [z(110, 111)]) == E + RR + 1


def test_none_stays_none():
    assert C._enforce_tp2_rr(E, SL, 102.0, None, "LONG", [], [z(110, 111)]) is None


def test_long_low_rr_adjusted_to_min_when_within_farthest_zone():
    out = C._enforce_tp2_rr(E, SL, 101.5, E + 2.0, "LONG", [], [z(104, 105)])
    assert out == pytest.approx(E + RR * 1.0, abs=1e-6)       # 调整到刚好 RR=3.0


def test_long_low_rr_degrades_to_single_when_beyond_farthest_zone():
    assert C._enforce_tp2_rr(E, SL, 101.5, E + 2.0, "LONG", [], [z(101.8, 102.5)]) is None


def test_long_no_zones_degrades_to_single():
    assert C._enforce_tp2_rr(E, SL, 101.5, E + 2.0, "LONG", [], []) is None


def test_short_low_rr_adjusted_and_degrade():
    sl = 101.0
    out = C._enforce_tp2_rr(E, sl, 98.5, E - 2.0, "SHORT", [z(94, 95)], [])
    assert out == pytest.approx(E - RR * 1.0, abs=1e-6)
    assert C._enforce_tp2_rr(E, sl, 98.5, E - 2.0, "SHORT", [z(97.5, 98)], []) is None


def test_adjusted_price_not_beyond_tp1_degrades():
    # tp1 已在 RR=3.5 处，调整价（RR=3.0）不在 tp1 远侧 → 退化
    assert C._enforce_tp2_rr(E, SL, E + 3.5, E + 2.0, "LONG", [], [z(110, 111)]) is None


def test_exact_boundary_passes():
    assert C._enforce_tp2_rr(E, SL, 102.0, E + RR, "LONG", [], []) == E + RR


def test_calc_sl_tp_output_has_tp_mode_and_rr_floor():
    """端到端：所有成功返回的 params 都带 tp_mode，且有 TP2 时 rr_tp2 >= RR_MIN_TP2。"""
    from test_calc_sl_tp import _mock_signal, STANDARD_LIQ, K1H, MARK, ATR, EMA20
    for mode in ("pullback", "breakout", "reversal"):
        for d in ("LONG", "SHORT"):
            r = C.calc_sl_tp(_mock_signal(mode, d), MARK, ATR, STANDARD_LIQ, K1H, EMA20)
            if "error" in r or r.get("tp1_price") is None:
                continue
            assert r["tp_mode"] == ("single" if r["tp2_price"] is None else "dual")
            if r["tp2_price"] is not None:
                assert r["rr_tp2"] >= RR - 0.01
