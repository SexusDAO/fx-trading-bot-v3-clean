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
