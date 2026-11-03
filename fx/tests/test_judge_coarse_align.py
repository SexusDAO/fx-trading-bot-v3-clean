"""[fix-judge-coarse-align] 粗筛对齐精筛 + 量比改用已收盘 K 线（全 mock，不联网）。"""
import inspect
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import scanner as S
import shared.judges as J
from test_ema_trend_short_filters import _ind as _ema_ind, _run as _ema_run, _reason


# ═══ 1a ema_trend 粗筛：方向感知 ═══
def test_ema_coarse_unknown_4h_accepts_short_range():
    # analyze_one 调粗筛时 4H EMA 尚未计算（=0）：空头范围内的 RSI/CHG24 现在也能放行
    assert S._is_ema_trend_coarse({"rsi_1h": 38.0}, -10.0) is True


def test_ema_coarse_unknown_4h_accepts_long_range():
    assert S._is_ema_trend_coarse({"rsi_1h": 65.0}, 10.0) is True


def test_ema_coarse_unknown_4h_rejects_outside_both():
    assert S._is_ema_trend_coarse({"rsi_1h": 20.0}, 0.0) is False     # RSI 两个范围都不在
    assert S._is_ema_trend_coarse({"rsi_1h": 50.0}, -15.0) is False   # CHG24 两个范围都不在
    assert S._is_ema_trend_coarse({"rsi_1h": 50.0}, 15.0) is False


def test_ema_coarse_known_short_uses_short_range():
    ind = {"rsi_1h": 40.0, "ema20_4h": 99.0, "ema50_4h": 100.0}
    assert S._is_ema_trend_coarse(ind, -10.0) is True
    assert S._is_ema_trend_coarse(dict(ind, rsi_1h=60.0), -10.0) is False   # 空头 RSI 上限 55
    assert S._is_ema_trend_coarse(ind, 10.0) is False                       # 空头 CHG24 上限 +8


def test_ema_coarse_known_long_uses_long_range():
    ind = {"rsi_1h": 60.0, "ema20_4h": 101.0, "ema50_4h": 100.0}
    assert S._is_ema_trend_coarse(ind, 10.0) is True
    assert S._is_ema_trend_coarse(dict(ind, rsi_1h=40.0), 0.0) is False     # 多头 RSI 下限 45
    assert S._is_ema_trend_coarse(ind, -10.0) is False                      # 多头 CHG24 下限 -8


def test_ema_coarse_switch_off_restores_old_long_only_range(monkeypatch):
    monkeypatch.setattr(S, "_EMA_COARSE_DIR_AWARE", False)
    assert S._is_ema_trend_coarse({"rsi_1h": 38.0}, -10.0) is False
    assert S._is_ema_trend_coarse({"rsi_1h": 60.0}, 5.0) is True


def test_is_ema_trend_short_uses_short_range():
    # 空头排列，价格在 EMA20 附近
    ind = {"rsi_1h": 40.0, "ema20_4h": 100.0, "ema50_4h": 102.0, "price": 100.0}
    assert S._is_ema_trend(ind, -10.0) is True
    assert S._is_ema_trend(dict(ind, rsi_1h=60.0), -10.0) is False
    assert S._is_ema_trend(dict(ind, ema20_4h=102.0, ema50_4h=100.0, price=102.0, rsi_1h=40.0), 0.0) is False


# ═══ 1b oi_divergence 粗筛：近高/近低 8% ═══
def _oi_ind(price, low, high):
    return {"oi_now": 110.0, "oi_1h_ago": 100.0, "price": price, "low_24h": low, "high_24h": high, "atr_ratio": 1.0}


def test_oi_div_near_low_within_8pct():
    assert S._is_oi_divergence(_oi_ind(106.0, 100.0, 200.0)) is True      # 旧 3% 会拒绝
    assert S._is_oi_divergence(_oi_ind(109.0, 100.0, 200.0)) is False     # 超过 8%


def test_oi_div_near_high_within_8pct():
    assert S._is_oi_divergence(_oi_ind(94.0, 50.0, 100.0)) is True        # 旧 97% 会拒绝
    assert S._is_oi_divergence(_oi_ind(91.0, 50.0, 100.0)) is False       # 低于 92%


# ═══ 1c range_bounce 粗筛：ATR 下限 1.5 / 1.2 ═══
def _rb_k1h():
    closes = [100.0] * 10 + [100, 105, 110, 116, 120] + [118, 117, 116, 117, 116] + [116]
    return [[i, c, c, c, c, 1.0] for i, c in enumerate(closes)]


def test_range_bounce_atr_threshold_weekday():
    k = _rb_k1h()
    assert S._is_range_bounce(k, {"price": 116.0, "atr_ratio": 1.6}) is True
    assert S._is_range_bounce(k, {"price": 116.0, "atr_ratio": 1.4}) is False


def test_range_bounce_atr_threshold_weekend():
    k = _rb_k1h()
    assert S._is_range_bounce(k, {"price": 116.0, "atr_ratio": 1.3, "_weekend_mode": True}) is True
    assert S._is_range_bounce(k, {"price": 116.0, "atr_ratio": 1.1, "_weekend_mode": True}) is False
    # 平日 1.3 仍被拒
    assert S._is_range_bounce(k, {"price": 116.0, "atr_ratio": 1.3}) is False


# ═══ 2 judges 量比改用已收盘 K 线 ═══
def test_ema_long_vol_gate_reads_closed_ratio():
    # 未收盘量比很低(0.1)，已收盘量比正常(0.9) → 不应因量能被拒
    ok, out = _ema_run(_ema_ind("long", ema20_1h=101.0, ema50_1h=100.0, vol_ratio=0.1, vol_ratio_closed=0.9))
    assert "量能不足" not in _reason(out)
    # 反过来：未收盘高、已收盘低 → 被量能拒
    ok, out = _ema_run(_ema_ind("long", ema20_1h=101.0, ema50_1h=100.0, vol_ratio=0.9, vol_ratio_closed=0.1))
    assert ok is False and "量能不足" in _reason(out) and "0.10" in _reason(out)


def test_ema_long_vol_gate_falls_back_to_vol_ratio():
    ok, out = _ema_run(_ema_ind("long", ema20_1h=101.0, ema50_1h=100.0, vol_ratio=0.1))   # 无 closed 键
    assert ok is False and "量能不足" in _reason(out)


def test_ema_short_vol_gate_reads_closed_ratio():
    ok, out = _ema_run(_ema_ind(ema20_1h=99.0, ema50_1h=100.0, vol_ratio=0.1, vol_ratio_closed=0.9))
    assert "做空量能不足" not in _reason(out)
    ok, out = _ema_run(_ema_ind(ema20_1h=99.0, ema50_1h=100.0, vol_ratio=0.9, vol_ratio_closed=0.1))
    assert ok is False and "做空量能不足" in _reason(out)


def _reads_closed(func, ref_line_hint):
    """源码级兜底：该 judge 内带 vol_ratio 门槛的读取行必须先读 vol_ratio_closed。"""
    src = inspect.getsource(func)
    lines = [l for l in src.splitlines() if 'ind.get("vol_ratio' in l]
    assert lines, f"{func.__name__} 没找到 vol_ratio 读取"
    bad = [l.strip() for l in lines if 'ind.get("vol_ratio_closed"' not in l]
    assert not bad, f"{func.__name__} 仍有直接读 vol_ratio 的行: {bad}"


@pytest.mark.parametrize("fn", [
    J._judge_pullback_impl, J._judge_ema_trend_impl, J._judge_oi_divergence_impl,
])
def test_judge_vol_reads_use_closed_first(fn):
    _reads_closed(fn, None)


def test_reversal_vol_gate_reads_closed_first():
    src = inspect.getsource(J._judge_reversal_impl)
    gate = [l for l in src.splitlines() if l.strip().startswith("_rev_vol =")]
    assert len(gate) == 1 and 'ind.get("vol_ratio_closed"' in gate[0]


# ═══ [fix-judge-logic-v2] 改动一：4H EMA 已知为空头排列时的粗筛 ═══
def test_ema_coarse_known_bear_4h_rsi38_chg_m10_true():
    ind = {"rsi_1h": 38.0, "ema20_4h": 99.0, "ema50_4h": 100.0}     # 空头排列
    assert S._is_ema_trend_coarse(ind, -10.0) is True


def test_ema_coarse_known_bear_4h_rsi60_chg_m10_false():
    ind = {"rsi_1h": 60.0, "ema20_4h": 99.0, "ema50_4h": 100.0}     # RSI 60 > 空头上限 55
    assert S._is_ema_trend_coarse(ind, -10.0) is False


# ═══ [fix-judge-logic-v2] 改动二/三：reversal 做多 ═══
from unittest.mock import patch
import config
from test_judge import _k, _flat_k1h, _ind as _jind


def _rev_long(chg24, rsi_4h, tier_atr=1500, chg4h_drop=False):
    """构造与 test_judge.test_reversal_long_triggers 相同的做多反转场景，参数化 chg24 / rsi_4h。"""
    base = 50000
    k1h = _flat_k1h(base)[:-2] + [
        _k(base, base * 1.01, base * 0.969, base * 0.970, 2500),
        _k(base * 0.970, base * 0.972, base * 0.968, base * 0.969, 1800),
    ]
    k4h = [
        _k(50000, 50200, 49800, 50100), _k(50100, 50300, 49200, 49300),
        _k(49300, 49500, 49000, 49100), _k(49100, 49800, 49100, 49700), _k(49700, 50500, 49600, 50400),
        _k(50400, 50800, 50100, 50700), _k(50700, 51000, 50300, 50900), _k(50900, 51200, 49700, 49800),
        _k(49800, 50200, 49500, 50100), _k(50100, 50600, 49600, 50500), _k(50500, 51000, 50200, 50800),
        _k(50800, 51500, 50700, 51300),
    ]
    k15 = [
        _k(base, base * 1.01, base * 0.97, base * 0.975, 1800),
        _k(base * 0.975, base * 1.01, base * 0.97, base * 0.98, 1500),
        _k(base * 0.98, base * 1.005, base * 0.955, base * 1.002, 1400),
        _k(base * 1.002, base * 1.012, base * 1.001, base * 1.01, 1300),
    ]
    ind = _jind(price=50000, atr=tier_atr, rsi_1h=28, rsi_4h=rsi_4h, ema20_1h=50000, ema50_1h=50000)
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
         patch("shared.state._load_oi_cache", return_value={}):
        return J.judge_reversal(k1h, k4h, k15, ind, chg24=chg24, funding=-0.0015)


def _rr(out):
    return str((out or {}).get("reject_reason") or "")


def test_reversal_long_chg24_m6_no_longer_rejected_as_insufficient_magnitude():
    # 24H -6%（旧门槛要求 <=-15 才过幅度检查）。1H 末两根合计下跌与 chg4h 取决于 K 线；
    # 这里只断言：不再因「幅度不足」被拒（若 4H 窗口本身满足也不影响）。
    ok, out = _rev_long(chg24=-6.0, rsi_4h=22)
    assert "幅度不足" not in _rr(out), _rr(out)


def test_reversal_long_chg24_m4_still_not_enough():
    # -4% 低于任何入口门槛 → 方向不明确（或幅度不足），不会被识别为做多
    ok, out = _rev_long(chg24=-4.0, rsi_4h=22)
    assert ok is False


def test_reversal_rsi4h_main_entry_and_precision_use_config():
    assert config.REVERSAL_RSI4H_MAX_LONG == 55
    # RSI4H=50：旧 <40 会走不到做多（方向不明确/未达超卖）；新 <55 通过入口与精筛的 RSI4H 检查
    ok, out = _rev_long(chg24=-18.0, rsi_4h=50)
    r = _rr(out)
    assert "方向不明确" not in r and "未达超卖" not in r, r


def test_reversal_rsi4h_above_config_max_still_rejected():
    ok, out = _rev_long(chg24=-18.0, rsi_4h=58)      # >= 55
    assert ok is False
    r = _rr(out)
    assert "方向不明确" in r or "未达超卖" in r, r


def test_reversal_rsi4h_reads_config_value(monkeypatch):
    monkeypatch.setattr(config, "REVERSAL_RSI4H_MAX_LONG", 45, raising=False)
    ok, out = _rev_long(chg24=-18.0, rsi_4h=50)      # 50 >= 45 → 被拒
    assert ok is False
    r = _rr(out)
    assert "方向不明确" in r or "未达超卖" in r, r


# ═══ [fix-judge-logic-v2] 改动四：oi_divergence RSI 门槛 ═══
def test_oi_div_rsi_config_values():
    assert config.OI_DIV_RSI1H_LONG_MAX == 45
    assert config.OI_DIV_RSI1H_SHORT_MIN == 58


def _oi_judge(direction_long, rsi_1h):
    base = 50000
    k1h = _flat_k1h(base)
    ind = _jind(price=base * 1.01, atr=1200, rsi_1h=rsi_1h, rsi_4h=50)
    ind.update(low_24h=base, high_24h=base * 1.5, oi_now=115000 if direction_long else 85000, oi_1h_ago=100000,
               taker_buy_ratio=0.55, taker_buy_ratio_now=0.55, vol24_usd=50e6)
    if not direction_long:
        ind.update(price=base * 1.49, high_24h=base * 1.5, low_24h=base * 0.5)
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
         patch("shared.state._load_oi_cache", return_value={}):
        return J.judge_oi_divergence(k1h, [], [], ind, chg24=0.0, funding=0.0)


def test_oi_div_long_rsi_44_passes_rsi_filter_but_46_rejected():
    _, out44 = _oi_judge(True, 44.0)
    assert "RSI 过滤" not in _rr(out44), _rr(out44)
    _, out50 = _oi_judge(True, 50.0)
    assert "LONG RSI 过滤" in _rr(out50), _rr(out50)


def test_oi_div_short_rsi_59_passes_rsi_filter_but_55_rejected():
    _, out59 = _oi_judge(False, 59.0)
    assert "SHORT RSI 过滤" not in _rr(out59), _rr(out59)
    _, out55 = _oi_judge(False, 53.0)
    assert "SHORT RSI 过滤" in _rr(out55), _rr(out55)
