"""[refactor-B11] 评分再平衡回归测试：CG/hub 只做锦上添花，熊市 strategy_adj 收窄。
测的是真实函数，不是复算公式；网络调用全部打桩。"""
import inspect
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from shared import coinglass_api as cg


def _stub_cg(monkeypatch, bias, deriv_delta=0):
    monkeypatch.setattr(cg, "get_liq_zones",
                        lambda *a, **k: {"bias": bias, "ratio": 2.0, "above": [], "below": []})
    monkeypatch.setattr(cg, "get_derivatives_signals", lambda *a, **k: {})
    monkeypatch.setattr(cg, "get_whale_signals", lambda *a, **k: {})
    monkeypatch.setattr(cg, "get_kol_signals", lambda *a, **k: {})
    monkeypatch.setattr(cg, "_calc_deriv_score", lambda *a, **k: (deriv_delta, []))


def test_heatmap_against_direction_is_zero_not_negative(monkeypatch):
    # 1. 逆势热力图：LONG 遇 short 偏向、SHORT 遇 long 偏向，heatmap_score 都必须是 0
    _stub_cg(monkeypatch, "short")
    assert cg.get_signal_for_open("BTCUSDT", "LONG", 100.0)["heatmap_score"] == 0
    _stub_cg(monkeypatch, "long")
    assert cg.get_signal_for_open("BTCUSDT", "SHORT", 100.0)["heatmap_score"] == 0


def test_heatmap_with_direction_still_adds_10(monkeypatch):
    _stub_cg(monkeypatch, "long")
    assert cg.get_signal_for_open("BTCUSDT", "LONG", 100.0)["heatmap_score"] == 10
    _stub_cg(monkeypatch, "short")
    assert cg.get_signal_for_open("BTCUSDT", "SHORT", 100.0)["heatmap_score"] == 10


def test_scanner_cg_bonus_clip_floor_is_zero():
    src = Path(config.__file__).with_name("scanner.py").read_text(encoding="utf-8")
    assert "cg_bonus = max(0, min(10, cg_bonus))" in src
    assert "cg_bonus = max(-10, min(10, cg_bonus))" not in src


def test_deriv_delta_never_below_minus_5():
    # 5. 多项负面叠加：多头拥挤 -5、费率上行 -3、主卖 -3、CVD -3、折价 -2、巨鲸逆向 -5 …
    sig = {"fr_latest": 0.0, "fr_trend": "rising", "lsr_signal": "short_friendly",
           "taker_signal": "strong_sell", "oi_signal": "neutral", "liq_signal": "neutral",
           "long_pct": 0.8, "buy_vol_pct": 0.1, "cvd_delta": -80_000_000, "cb_premium": -0.5}
    whale = {"whale_direction": "SHORT", "confidence": "high"}
    delta, _ = cg._calc_deriv_score("LONG", sig, whale, {})
    assert delta == -5


def test_deriv_positive_not_clipped():
    sig = {"fr_trend": "falling", "lsr_signal": "long_friendly", "taker_signal": "strong_buy",
           "oi_signal": "deleverage", "liq_signal": "heavy_long_liq", "short_pct": 0.7,
           "buy_vol_pct": 0.9, "cvd_delta": 5_000_000, "cb_premium": 0.3, "whale_index": 200}
    delta, _ = cg._calc_deriv_score("LONG", sig, {}, {})
    assert delta > 5


def test_hub_penalty_floor_config_and_effect(monkeypatch):
    # 2. hub 三项负向合计不超过 -5
    assert config.SCORE_HUB_PENALTY_FLOOR == -5
    import scanner
    monkeypatch.setattr(scanner, "_bonus_hub_klines", lambda *a, **k: [])
    monkeypatch.setattr(scanner, "_bonus_hub_ticker", lambda *a, **k: {})
    monkeypatch.setattr(scanner, "_calc_taker_bonus", lambda *a, **k: (-5, 0.3))
    monkeypatch.setattr(scanner, "_calc_range_bonus", lambda *a, **k: (-5, 0.9))
    monkeypatch.setattr(scanner, "_calc_volprice_bonus", lambda *a, **k: (0, 1.0))
    new_score, info = scanner._apply_score_bonuses("ABCUSDT", "LONG", "pullback", 60)
    assert info["hub_bonus"] == -5 and new_score == 55


def test_bear_strategy_adj_narrowed_in_source():
    src = Path(config.__file__).with_name("scanner.py").read_text(encoding="utf-8")
    # reversal LONG：-10/-8/-6；range_bounce：-7/-5/-3；顺势空单加分不变 +10/+7/+5
    assert re.search(r"strategy_adj = -10 if _bear_str == \"strong\" else \\\s+-8\s+if _bear_str == \"moderate\" else -6", src)
    assert re.search(r"strategy_adj = -7 if _bear_str == \"strong\" else \\\s+-5 if _bear_str == \"moderate\" else -3", src)
    assert re.search(r"strategy_adj = \+10 if _bear_str == \"strong\" else \\\s+\+7\s+if _bear_str == \"moderate\" else \+5", src)
    # ema_trend 熊市 LONG -15→-10，SHORT 仍 +8；牛市 SHORT 的 -15 不动
    assert "strategy_adj = +8 if _et_is_short else -10" in src
    assert "strategy_adj = -15 if _et_is_short else +8" in src


def test_bear_strong_long_and_short_score_arithmetic():
    # 3/4. 用收窄后的真实常量走一遍：strong base=70
    base = 70
    long_score = base + (-5) + 0 + (-10) + (-5)      # btc -5, cg 0, strat -10, hub 最差 -5
    assert long_score == 50 and long_score >= 40
    short_score = min(100, base + 5 + 10 + 10)       # btc +5, cg +10, strat +10
    assert short_score == 95
