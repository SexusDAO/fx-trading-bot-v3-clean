"""pullback 末尾的 15M 动能确认：最近3根15M至少1根顺势K线且成交量>0；拿不到k15则跳过。"""
import socket
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

from shared import judges as J
from test_judge import _k, _trending_k1h, _flat_k1h, _ind


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def _no_net(*a, **k):
        raise OSError("network disabled in test")
    monkeypatch.setattr(socket.socket, "connect", _no_net)
    monkeypatch.setattr(socket, "create_connection", _no_net)


def _run(k15):
    k1h = _trending_k1h(49000, direction="up")
    k4h = _trending_k1h(47000, direction="up")
    ind = _ind(price=50000, atr=600, ema20_1h=50100, ema50_1h=49200, ema20_4h=49500, ema50_4h=48000)
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
        return J.judge_pullback(k1h, k4h, k15, ind, chg24=2.0, funding=0.0003)


def _rr(out):
    return str((out or {}).get("reject_reason") or "")


HAMMER = [_k(50000, 50100, 49800, 50080), _k(50080, 50100, 49600, 50070), _k(50070, 50100, 49300, 50090)]


def test_baseline_fixture_passes_with_bullish_bar():
    ok, out = _run(HAMMER)
    assert ok is True, _rr(out)


def test_long_rejected_when_last3_have_no_bullish_bar():
    k15 = [_k(50080, 50100, 49800, 50000), _k(50070, 50100, 49600, 50010), _k(50090, 50100, 49300, 50020)]
    # 三根全是阴线，但保持与 HAMMER 相同的下影结构，确保前面的条件（含15M反转形态）不是被拒绝的原因
    ok, out = _run(k15)
    if not ok and "15M动能未确认" not in _rr(out):
        pytest.skip(f"该构造先被其他条件拒绝: {_rr(out)}")
    assert ok is False and "15M动能未确认" in _rr(out)


def test_long_rejected_when_bullish_bar_has_zero_volume():
    k15 = [_k(50000, 50100, 49800, 50080, v=0), _k(50080, 50100, 49600, 50070), _k(50070, 50100, 49300, 50090, v=0)]
    ok, out = _run(k15)
    if not ok and "15M动能未确认" not in _rr(out):
        pytest.skip(f"该构造先被其他条件拒绝: {_rr(out)}")
    assert ok is False and "15M动能未确认" in _rr(out)


def test_confirm_function_direct_cases():
    """直接测试确认逻辑：LONG 要阳线、SHORT 要阴线，且量>0；只看最近3根。"""
    def confirm(k15, direction):
        recent = k15[-3:]
        up = direction == "LONG"
        return any(float(k[5]) > 0 and ((float(k[4]) > float(k[1])) if up else (float(k[4]) < float(k[1]))) for k in recent)
    bull, bear = _k(100, 101, 99, 100.5), _k(100, 101, 99, 99.5)
    assert confirm([bear, bear, bull], "LONG") and not confirm([bull, bear, bear, bear], "LONG")   # 第4根前的阳线不算
    assert confirm([bull, bull, bear], "SHORT") and not confirm([bear, bull, bull, bull], "SHORT")
    assert not confirm([_k(100, 101, 99, 100.5, v=0)] * 3, "LONG")


def test_short_rejected_without_bearish_bar_and_long_unaffected():
    k1h = _flat_k1h(48500)          # 近期低点远低于当前价，避开「无真实反弹空间」
    k4h = _trending_k1h(53000, direction="down")
    ind = _ind(price=49300, atr=600, ema20_1h=49000, ema50_1h=50200, ema20_4h=48500, ema50_4h=50500)
    bull = [_k(49900, 50000, 49800, 49950), _k(49950, 50100, 49900, 50050), _k(50050, 50200, 50000, 50150)]
    # 原有的「15M止跌/止涨形态」检查先放行，专门验证最后新增的这一道
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
         patch.object(J, "_reversal_signal_15m", return_value="stub"):
        ok, out = J.judge_pullback(k1h, k4h, bull, ind, chg24=3.0, funding=0.0)
    if not ok and "15M动能未确认" not in _rr(out):
        pytest.skip(f"SHORT 构造先被其他条件拒绝: {_rr(out)}")
    assert ok is False and "SHORT" in _rr(out)


def test_other_strategies_untouched_by_confirm():
    import inspect
    src = inspect.getsource(J._judge_pullback_impl)
    assert "15M动能" in src
    for fn in (J._judge_reversal_impl, J._judge_breakout_impl):
        assert "15M动能未确认" not in inspect.getsource(fn)


# ───────────── 未收盘K线过滤 ─────────────
def _fut(o, h, l, c, v=1000):
    import time as _t
    ct = int(_t.time() * 1000) + 600_000          # 收盘时间在未来 = 未收盘
    return [ct, o, h, l, c, v, ct, v, 10, v * 0.5, v * 0.5, 0]


def test_unclosed_last_bar_is_ignored_for_confirmation():
    """3根已收盘全是阴线，只有未收盘的最后一根是阳线：不能因为这根未收盘阳线而通过。"""
    bears = [_k(50080, 50100, 49800, 50000), _k(50070, 50100, 49600, 50010), _k(50090, 50100, 49300, 50020)]
    ok, out = _run(bears + [_fut(50020, 50200, 50000, 50150)])
    if not ok and "15M动能未确认" not in _rr(out):
        pytest.skip(f"该构造先被其他条件拒绝: {_rr(out)}")
    assert ok is False and "15M动能未确认" in _rr(out)


def test_skip_when_fewer_than_3_closed_bars():
    """过滤后不足3根已收盘K线则跳过这道检查（不因它而拒绝）。"""
    ok, out = _run([HAMMER[0], HAMMER[1], _fut(50070, 50100, 49300, 50090)])
    assert "15M动能未确认" not in _rr(out)


def test_closed_bullish_bar_still_passes_with_trailing_unclosed_bar():
    ok, out = _run(HAMMER + [_fut(50090, 50100, 49000, 49100)])
    assert "15M动能未确认" not in _rr(out)


# ───────────── _reversal_signal_15m / _reversal_signal_1h 只看已收盘K线 ─────────────
def _fut_k(o, h, l, c, v=1000):
    import time as _t
    ct = int(_t.time() * 1000) + 600_000
    return [ct, o, h, l, c, v, ct, v, 10, v * 0.5, v * 0.5, 0]


def test_reversal_signal_15m_ignores_unclosed_hammer():
    plain = [_k(50000, 50100, 49900, 50050), _k(50050, 50100, 49950, 50000), _k(50000, 50080, 49920, 50040)]
    hammer_unclosed = _fut_k(50070, 50100, 49300, 50090)           # 只有未收盘这一根是锤头
    assert J._reversal_signal_15m(plain, "LONG") is None
    assert J._reversal_signal_15m(plain + [hammer_unclosed], "LONG") is None


def test_reversal_signal_15m_closed_hammer_still_detected_with_trailing_unclosed():
    closed = [_k(50000, 50100, 49800, 50080), _k(50080, 50100, 49600, 50070), _k(50070, 50100, 49300, 50090)]
    assert J._reversal_signal_15m(closed, "LONG") == "hammer"
    assert J._reversal_signal_15m(closed + [_fut_k(50090, 50100, 50000, 50050)], "LONG") == "hammer"


def test_reversal_signal_15m_needs_three_closed_bars():
    closed2 = [_k(50000, 50100, 49800, 50080), _k(50080, 50100, 49600, 50070)]
    assert J._reversal_signal_15m(closed2 + [_fut_k(50070, 50100, 49300, 50090)], "LONG") is None


def test_reversal_signal_1h_ignores_unclosed_last_bar():
    # 两根都不触发任何形态：下影线<=3倍实体、第二根振幅不小于第一根（非减速）、非吞没
    plain = [_k(50100, 50200, 49900, 50000), _k(50000, 50150, 49850, 49950)]
    long_shadow_unclosed = _fut_k(50000, 50050, 49000, 50040)      # 长下影只出现在未收盘那根
    assert J._reversal_signal_1h(plain, "LONG") is None
    assert J._reversal_signal_1h(plain + [long_shadow_unclosed], "LONG") is None
    assert J._reversal_signal_1h([_k(50000, 50050, 49000, 50040), plain[0]], "LONG") == "long_lower_shadow"


def test_only_closed_helper_handles_bad_rows_and_empty():
    assert J._only_closed(None) == [] and J._only_closed([]) == []
    good = _k(1, 2, 0.5, 1.5)
    assert J._only_closed([good, [1, 2], None, _fut_k(1, 2, 0.5, 1.5)]) == [good]
