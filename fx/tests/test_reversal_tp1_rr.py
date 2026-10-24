import shared.calc_sl_tp as c


def _zones():
    # SHORT: 清算区在入场价下方, sl_dist=2 → RR1.2 需 >=2.4, RR2.0 需 >=4.0
    # 近区 price_high=96.0 → candidate≈96+0.3*atr(1)=96.3 → 距离3.7: 满足1.2 不满足2.0
    near = {"price_low": 95.0, "price_high": 96.0, "volume": 1.0}
    far = {"price_low": 85.0, "price_high": 86.0, "volume": 1.0}
    return [near, far]


def _tp(mode):
    return c._calc_tp(100.0, 102.0, 1.0, "SHORT", _zones(), [], symbol="X", tier="mid", mode=mode)


def test_reversal_uses_lower_rr_and_picks_nearest_zone():
    tp1, _, degraded = _tp("reversal")
    assert not degraded and 96.0 < tp1 < 97.0


def test_other_modes_keep_general_rr():
    for m in ("pullback", "", "ema_trend"):
        tp1, _, _ = _tp(m)
        assert tp1 < 90.0   # 近区 RR 不足 2.0,落到远区


def test_degraded_atr_tp_unchanged_for_reversal():
    tp1, _, degraded = c._calc_tp(100.0, 102.0, 1.0, "SHORT", [], [], symbol="", tier="mid", mode="reversal")
    assert degraded and abs(tp1 - (100.0 - 2.0 * c._TP1_MIN_RR)) < 1e-6


def test_config_value():
    import config
    assert config.RR_MIN_TP1_REVERSAL == 1.2 and config.RR_MIN_TP1 == 2.0


def _cap_signal(mode):
    return {"symbol": "XUSDT", "direction": "SHORT", "mode": mode, "volatility_tier": "mid",
            "fill_price": 100.0, "ind": {}}


def test_tp1_cap_rr_threshold_reversal_vs_general(monkeypatch):
    """TP1 被 cap 截断后 RR=1.5:reversal(门槛1.2)不应因 RR 被拒绝,通用(2.0)走收紧SL/拒绝路径。"""
    import inspect
    src = inspect.getsource(c.calc_sl_tp)
    assert 'if mode == "reversal":\n        _rr_min = _TP1_MIN_RR_REVERSAL' in src
