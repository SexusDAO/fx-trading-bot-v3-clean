import config
import position_manager as pm


def _pos(direction="long", mode="pullback"):
    entry = 100.0
    return {"entry_price": entry, "direction": direction, "mode": mode, "atr_at_entry": 1.0,
            "sl_price": 95.0 if direction == "long" else 105.0, "tp1_filled": False,
            "rr_protection_triggered": True}   # 关闭 RR 保护候选,只看阶梯


def test_first_step_config_value():
    assert config.TRAIL_STEPS_TREND[0] == (2.0, 0.5)
    assert pm.TRAIL_STEPS_TREND[0] == (2.0, 0.5)
    assert config.TRAIL_STEPS_REVERSAL[0] == (0.5, 0.0)      # 反转档未动


def test_no_move_below_first_trigger_long_and_short():
    assert pm._calc_trail_sl(_pos(), 101.0) is None            # 浮盈1ATR:原先会移到+0.1ATR,现在不动
    assert pm._calc_trail_sl(_pos("short"), 99.0) is None
    assert pm._calc_trail_sl(_pos(), 101.7) is None            # 1.7ATR 仍不动


def test_buffer_half_atr_after_trigger():
    # 注意:阶梯第二档 (1.8, 0.5) 排在 (2.0, 0.5) 之后,倒序查找先命中它,
    # 所以实际 1.8ATR 起就移到 入场价±0.5ATR;(2.0, 0.5) 这一档被架空。
    assert abs(pm._calc_trail_sl(_pos(), 101.9) - 100.5) < 1e-9
    assert abs(pm._calc_trail_sl(_pos("short"), 98.1) - 99.5) < 1e-9
    assert abs(pm._calc_trail_sl(_pos(), 102.0) - 100.5) < 1e-9


def test_other_steps_unchanged():
    assert list(config.TRAIL_STEPS_TREND[1:]) == [(1.8, 0.5), (2.8, 1.0), (4.0, 1.5), (6.0, 2.0)]
    assert abs(pm._calc_trail_sl(_pos(), 103.0) - 101.0) < 1e-9   # 2.8ATR → +1.0ATR
