import config
import shared.judges as j


def _ind(gap_pct, atr_ratio_pct, price=100.0):
    ema50 = 100.0
    return {"price": price, "atr_1h": price * atr_ratio_pct / 100.0, "rsi_1h": 55, "rsi_4h": 55,
            "ema20_4h": ema50 * (1 + gap_pct / 100.0), "ema50_4h": ema50, "vol_ratio": 1.0, "symbol": "XUSDT"}


def _judge(ind):
    return j._judge_ema_trend_impl([], [], [], ind, 0.0, 0.0)


def test_config_values():
    assert config.EMA_TREND_EMA_GAP_MIN_PCT == 0.003
    assert config.EMA_TREND_ATR_MIN == 0.8
    assert config.ATR_TIER_MID_MIN == 1.0   # 其他策略共用的门槛不变


def test_gap_between_03_and_05_no_longer_rejected_as_unclear():
    ok, out = _judge(_ind(0.4, 1.5))
    assert "排列不明确" not in str(out.get("reject_reason"))


def test_gap_below_03_still_rejected():
    ok, out = _judge(_ind(0.2, 1.5))
    assert ok is False and "排列不明确" in out["reject_reason"]


def test_atr_between_08_and_10_passes_atr_check():
    ok, out = _judge(_ind(0.6, 0.9))
    assert "波动率过低" not in str(out.get("reject_reason"))


def test_atr_below_08_rejected():
    ok, out = _judge(_ind(0.6, 0.7))
    assert ok is False and "波动率过低" in out["reject_reason"] and "0.8" in out["reject_reason"]
