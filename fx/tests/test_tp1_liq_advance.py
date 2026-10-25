import config
import shared.calc_sl_tp as C


def _zones():
    # 做空 TP1 锚定下方多头清算区 price_high；做多锚定上方空头清算区 price_low
    liq_long = [{"price_high": 90.0, "price_low": 89.0, "amount": 1e7}]
    liq_short = [{"price_high": 111.0, "price_low": 110.0, "amount": 1e7}]
    return liq_long, liq_short


def _tp(direction, monkeypatch, adv=0.2):
    monkeypatch.setattr(config, "TP1_LIQ_ADVANCE_ATR", adv, raising=False)
    monkeypatch.setattr(C, "_liq_rel_cfg", lambda: None)      # 缓冲固定 0.3ATR
    ll, ls = _zones()
    entry, atr = 100.0, 1.0
    sl = 103.0 if direction == "SHORT" else 97.0
    return C._calc_tp(entry, sl, atr, direction, ll, ls)


def test_config_default():
    assert config.TP1_LIQ_ADVANCE_ATR == 0.2


def test_short_tp1_buffer_reduced_by_advance(monkeypatch):
    r = _tp("SHORT", monkeypatch)
    tp1 = r[0]
    assert abs(tp1 - (90.0 + 0.1)) < 1e-9          # 缓冲 0.3-0.2=0.1ATR


def test_short_advance_zero_keeps_old_behavior(monkeypatch):
    r = _tp("SHORT", monkeypatch, adv=0.0)
    tp1 = r[0]
    assert abs(tp1 - (90.0 + 0.3)) < 1e-9


def test_long_unchanged(monkeypatch):
    r = _tp("LONG", monkeypatch)
    tp1 = r[0]
    assert abs(tp1 - (110.0 - 0.3)) < 1e-9
