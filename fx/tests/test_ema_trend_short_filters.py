"""ema_trend 入场过滤加强：1H EMA 方向一致、做空量比下限 0.7、做空 1H RSI 上限 48、做空 4H RSI 上限 52。全部纯函数 mock。"""
import config
import shared.judges as j


def _ind(direction="short", **kw):
    ema50 = 100.0
    gap = -0.016 if direction == "short" else 0.01         # [v3.101] 空头 EMA gap 门槛 1.5%，测试用 -1.6% 确保能过 EMA 排列层
    price = 100.0 * (1 + gap)                              # 价格贴近 4H EMA20（回踩区内）
    ind = {"price": price, "atr_1h": price * 0.015, "rsi_1h": 44.0 if direction == "short" else 55.0,
           "rsi_4h": 40.0 if direction == "short" else 55.0,
           "ema20_4h": ema50 * (1 + gap), "ema50_4h": ema50, "vol_ratio": 1.0, "symbol": "XUSDT"}
    ind.update(kw)
    return ind


def _run(ind, chg24=-1.0):
    return j._judge_ema_trend_impl([], [], [], ind, chg24 if ind["ema20_4h"] < ind["ema50_4h"] else 1.0, 0.0)


def _reason(out):
    return str((out or {}).get("reject_reason"))


def _passes_filters(out):
    r = _reason(out)
    return not any(x in r for x in ("1H EMA", "做空量能不足", "1H RSI", "4H RSI"))


def test_config_values():
    assert config.EMA_TREND_RSI1H_SHORT_MAX == 48
    assert config.EMA_TREND_RSI4H_SHORT_MAX == 52
    assert config.EMA_TREND_VOL_RATIO_SHORT_MIN == 0.7
    assert config.EMA_TREND_VOL_RATIO_MIN == 0.5            # 做多门槛不变


def test_short_rejected_when_1h_ema_bullish():
    ok, out = _run(_ind(ema20_1h=101.0, ema50_1h=100.0))
    assert ok is False and "1H EMA 多头排列" in _reason(out)


def test_short_passes_1h_check_when_1h_ema_bearish():
    ok, out = _run(_ind(ema20_1h=99.0, ema50_1h=100.0))
    assert _passes_filters(out)


def test_long_rejected_when_1h_ema_bearish():
    ok, out = _run(_ind("long", ema20_1h=99.0, ema50_1h=100.0))
    assert ok is False and "1H EMA 空头排列" in _reason(out)


def test_long_passes_when_1h_ema_bullish():
    ok, out = _run(_ind("long", ema20_1h=101.0, ema50_1h=100.0))
    assert "1H EMA" not in _reason(out)


def test_missing_1h_ema_does_not_reject():
    ok, out = _run(_ind())                                   # 无 ema20_1h/ema50_1h
    assert "1H EMA" not in _reason(out)
    ok, out = _run(_ind(ema20_1h=0.0, ema50_1h=0.0))
    assert "1H EMA" not in _reason(out)


def test_1h_align_switch_off(monkeypatch):
    monkeypatch.setattr(config, "EMA_TREND_1H_EMA_ALIGN", False)
    ok, out = _run(_ind(ema20_1h=101.0, ema50_1h=100.0))
    assert "1H EMA" not in _reason(out)


def test_short_volume_floor_07():
    ok, out = _run(_ind(ema20_1h=99.0, ema50_1h=100.0, vol_ratio=0.67))
    assert ok is False and "做空量能不足" in _reason(out)
    ok, out = _run(_ind(ema20_1h=99.0, ema50_1h=100.0, vol_ratio=0.7))
    assert "做空量能不足" not in _reason(out)


def test_long_volume_floor_still_05():
    ok, out = _run(_ind("long", ema20_1h=101.0, ema50_1h=100.0, vol_ratio=0.55))
    assert "量能不足" not in _reason(out)
    ok, out = _run(_ind("long", ema20_1h=101.0, ema50_1h=100.0, vol_ratio=0.45))
    assert ok is False and "量能不足" in _reason(out) and "做空" not in _reason(out)


def test_short_rsi1h_upper_48():
    ok, out = _run(_ind(ema20_1h=99.0, ema50_1h=100.0, rsi_1h=52.4))
    assert ok is False and "1H RSI=52.4" in _reason(out) and "[32, 48]" in _reason(out)
    ok, out = _run(_ind(ema20_1h=99.0, ema50_1h=100.0, rsi_1h=47.9))
    assert "1H RSI" not in _reason(out)


def test_short_rsi4h_upper_52_parameterized(monkeypatch):
    ok, out = _run(_ind(ema20_1h=99.0, ema50_1h=100.0, rsi_4h=53.0))
    assert ok is False and "[28, 52]" in _reason(out)
    monkeypatch.setattr(config, "EMA_TREND_RSI4H_SHORT_MAX", 60, raising=False)
    ok, out = _run(_ind(ema20_1h=99.0, ema50_1h=100.0, rsi_4h=53.0))
    assert "4H RSI" not in _reason(out)


def test_soon_and_strk_like_cases_each_blocked_by_every_layer():
    # SOONUSDT：RSI1H 44.6、1H 多头排列、量比 0.58 → 被 1H EMA 拦（先于量能）
    ok, out = _run(_ind(rsi_1h=44.6, ema20_1h=101.0, ema50_1h=100.0, vol_ratio=0.58))
    assert ok is False and "1H EMA 多头排列" in _reason(out)
    # 关掉 1H EMA 检查后仍被量能拦
    config.EMA_TREND_1H_EMA_ALIGN, old = False, config.EMA_TREND_1H_EMA_ALIGN
    try:
        ok, out = _run(_ind(rsi_1h=44.6, ema20_1h=101.0, ema50_1h=100.0, vol_ratio=0.58))
        assert ok is False and "做空量能不足" in _reason(out)
        # STRKUSDT：RSI1H 52.4 → 先被 RSI 拦（RSI 检查在 4.5 之前）
        ok, out = _run(_ind(rsi_1h=52.4, ema20_1h=101.0, ema50_1h=100.0, vol_ratio=0.67))
        assert ok is False and "1H RSI=52.4" in _reason(out)
    finally:
        config.EMA_TREND_1H_EMA_ALIGN = old
