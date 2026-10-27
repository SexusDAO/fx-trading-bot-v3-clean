"""adjust_sltp：TP1 最小变动 0.5% 门槛 + 阈值注释修正。"""
import inspect

import position_manager as PM


def test_helper_below_threshold():
    assert PM._tp1_change_too_small(109.54, 109.532) is True          # 日志里的真实例子，变动约 0.007%
    assert PM._tp1_change_too_small(100.0, 100.4) is True             # 0.4%
    assert PM._tp1_change_too_small(100.0, 99.6) is True              # 下调 0.4%，对称


def test_helper_at_or_above_threshold():
    assert PM._tp1_change_too_small(100.0, 100.5) is False            # 恰好 0.5% 放行
    assert PM._tp1_change_too_small(100.0, 101.0) is False
    assert PM._tp1_change_too_small(100.0, 95.0) is False


def test_helper_invalid_input_does_not_block():
    assert PM._tp1_change_too_small(0, 1) is False
    assert PM._tp1_change_too_small(None, 1) is False
    assert PM._tp1_change_too_small("x", 1) is False


def test_constant_value():
    assert PM.MIN_TP_CHANGE_PCT == 0.005


def test_gate_sits_between_tp1_and_tp2_blocks_and_resets_flag_not_return():
    src = inspect.getsource(PM._adjust_sl_tp_once)
    gate = src.index("_tp1_change_too_small(old_tp1, new_tp1)")
    assert src.index('tp1_reason  = (f"{reason_tag}') < gate < src.index("# ── TP2 判断")
    seg = src[gate:gate + 400]
    assert "tp1_changed = False" in seg and "return" not in seg      # 只复位标志，不跳过 TP2/SL 的后续处理


def test_comments_say_3atr_not_6atr():
    src = inspect.getsource(PM._adjust_sl_tp_once)
    assert "6ATR" not in src and "6×ATR" not in src
    assert "tp_dist_now >= atr * 3.0 and tp_will_closer" in src
