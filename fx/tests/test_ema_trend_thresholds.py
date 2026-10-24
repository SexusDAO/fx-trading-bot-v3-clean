import config
import shared.judges as j


def _ind(gap_pct, atr_ratio_pct, price=100.0):
    ema50 = 100.0
    return {"price": price, "atr_1h": price * atr_ratio_pct / 100.0, "rsi_1h": 55, "rsi_4h": 55,
            "ema20_4h": ema50 * (1 + gap_pct / 100.0), "ema50_4h": ema50, "vol_ratio": 1.0, "symbol": "XUSDT"}


def _judge(ind):
    return j._judge_ema_trend_impl([], [], [], ind, 0.0, 0.0)


def test_config_values():
    assert config.EMA_TREND_EMA_GAP_MIN_PCT == 0.005
    assert config.EMA_TREND_PULLBACK_BELOW_EMA_LONG == 0.02 and config.EMA_TREND_PULLBACK_BELOW_EMA == 0.015
    assert config.EMA_TREND_ATR_MIN == 0.8
    assert config.ATR_TIER_MID_MIN == 1.0   # 其他策略共用的门槛不变


def test_gap_between_03_and_05_still_rejected_as_unclear():
    ok, out = _judge(_ind(0.4, 1.5))
    assert ok is False and "排列不明确" in out["reject_reason"]


def test_gap_at_05_not_rejected_as_unclear():
    ok, out = _judge(_ind(0.6, 1.5))
    assert "排列不明确" not in str(out.get("reject_reason"))


def test_gap_below_03_rejected():
    ok, out = _judge(_ind(0.2, 1.5))
    assert ok is False and "排列不明确" in out["reject_reason"]


def test_atr_between_08_and_10_passes_atr_check():
    ok, out = _judge(_ind(0.6, 0.9))
    assert "波动率过低" not in str(out.get("reject_reason"))


def test_atr_below_08_rejected():
    ok, out = _judge(_ind(0.6, 0.7))
    assert ok is False and "波动率过低" in out["reject_reason"] and "0.8" in out["reject_reason"]


def _pb_ind(pb_pct, gap_pct=0.6):
    i = _ind(gap_pct, 1.5)
    i["price"] = 100.0 * (1 + pb_pct / 100.0) if False else i["ema50_4h"] * (1 + gap_pct / 100.0) * (1 + pb_pct / 100.0)
    i["atr_1h"] = i["price"] * 0.015
    return i


def test_long_below_ema20_between_15_and_20_not_rejected_as_broken():
    ok, out = _judge(_pb_ind(-1.8))
    assert "价格破位" not in str(out.get("reject_reason"))


def test_long_below_ema20_over_20_rejected():
    ok, out = _judge(_pb_ind(-2.3))
    assert ok is False and "价格破位" in out["reject_reason"] and "2.0%" in out["reject_reason"]


def test_short_upper_limit_unchanged_at_15():
    i = _pb_ind(1.8, gap_pct=-0.6)
    i["rsi_1h"], i["rsi_4h"] = 40, 40
    ok, out = _judge(i)
    assert ok is False and "SHORT 价格高于 EMA20" in out["reject_reason"] and "1.5%" in out["reject_reason"]
