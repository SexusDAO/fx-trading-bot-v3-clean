"""评分附加加分项：taker 净流向 / 区间位置 / 放量不涨不跌（全 mock，不联网）。"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import config
import scanner as sc


@pytest.fixture(autouse=True)
def _pin_hub_cap_floor(monkeypatch):
    # 用例按旧上下限 +15/-10 编写；生产已改对称 +12/-12，这里固定旧值测加减分叠加逻辑
    import config
    monkeypatch.setattr(config, "SCORE_HUB_BONUS_CAP", 15, raising=False)
    monkeypatch.setattr(config, "SCORE_HUB_PENALTY_FLOOR", -10, raising=False)


def k(v=100.0, buy=50.0, q=1000.0):
    # [t,o,h,l,c,v,T,q,n,V,Q,ignore]
    return [0, "1", "2", "0.5", "1.5", str(v), 0, str(q), 1, str(buy), "0", "0"]


def tk(c=95.0, h=100.0, l=0.0, chg=1.0):
    return {"symbol": "X", "lastPrice": str(c), "highPrice": str(h), "lowPrice": str(l), "priceChangePercent": str(chg)}


# ── taker ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("direction,buy,exp", [
    ("LONG", 60, 10), ("LONG", 40, -5), ("LONG", 50, 0), ("LONG", 55, 0), ("LONG", 45, 0),
    ("SHORT", 40, 10), ("SHORT", 60, -5), ("SHORT", 50, 0),
])


def test_taker(direction, buy, exp):
    b, r = sc._calc_taker_bonus(direction, [k(100, buy)] * 6)
    assert b == exp and r == pytest.approx(buy / 100)


def test_taker_uses_last_6_and_skips_when_insufficient():
    rows = [k(100, 0)] * 4 + [k(100, 70)] * 6
    assert sc._calc_taker_bonus("LONG", rows)[0] == 10
    assert sc._calc_taker_bonus("LONG", [k()] * 5) == (None, None)
    assert sc._calc_taker_bonus("LONG", None) == (None, None)
    assert sc._calc_taker_bonus("LONG", [k(0, 0)] * 6) == (None, None)


# ── range ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("direction,c,exp", [
    ("LONG", 80, 8), ("LONG", 95, 8), ("LONG", 20, -5), ("LONG", 5, -5), ("LONG", 50, 0),
    ("SHORT", 20, 8), ("SHORT", 5, 8), ("SHORT", 80, -5), ("SHORT", 95, -5), ("SHORT", 50, 0),
])
def test_range(direction, c, exp):
    b, pos = sc._calc_range_bonus(direction, tk(c=c))
    assert b == exp and pos == pytest.approx(c / 100)


def test_range_skip():
    assert sc._calc_range_bonus("LONG", None) == (None, None)
    assert sc._calc_range_bonus("LONG", tk(h=10, l=10)) == (None, None)


# ── volprice ─────────────────────────────────────────────────────────────
def vrows(recent_q, base_q=1000.0):
    return [k(q=base_q)] * 20 + [k(q=recent_q)] * 4


@pytest.mark.parametrize("direction,q,chg,exp", [
    ("LONG", 1600, 2.9, 8), ("LONG", 1599, 0, 0), ("LONG", 2000, 3.0, 0), ("LONG", 2000, 8, 0), ("LONG", 2000, -9, 8),
    ("SHORT", 1600, -2.9, 8), ("SHORT", 2000, -3.0, 0), ("SHORT", 2000, -8, 0), ("SHORT", 2000, 9, 8), ("SHORT", 1000, 0, 0),
])
def test_volprice(direction, q, chg, exp):
    b, r = sc._calc_volprice_bonus(direction, vrows(q), tk(chg=chg))
    assert b == exp and r == pytest.approx(q / 1000)


def test_volprice_skip():
    assert sc._calc_volprice_bonus("LONG", vrows(2000)[:23], tk()) == (None, None)
    assert sc._calc_volprice_bonus("LONG", vrows(2000), None) == (None, None)
    assert sc._calc_volprice_bonus("LONG", vrows(2000, base_q=0), tk()) == (None, None)


# ── 叠加 ─────────────────────────────────────────────────────────────────
@pytest.fixture
def hub(monkeypatch):
    st = {"k6": [k(100, 60)] * 6, "k24": vrows(1800), "tk": tk(c=90, chg=1.0)}
    monkeypatch.setattr(sc, "_bonus_hub_klines", lambda sym, iv, n: st["k6"] if n == 6 else st["k24"])
    monkeypatch.setattr(sc, "_bonus_hub_ticker", lambda sym: st["tk"])
    for n in ("SCORE_TAKER_BONUS", "SCORE_RANGE_BONUS", "SCORE_VOLPRICE_BONUS"):
        monkeypatch.setattr(config, n, True, raising=False)
    return st


def test_all_three_stack(hub, caplog):
    caplog.set_level(logging.INFO, logger=sc.logger.name)
    sc_, info = sc._apply_score_bonuses("XUSDT", "LONG", "pullback", 60)
    assert sc_ == 75 and (info["taker"], info["range"], info["volprice"]) == (10, 8, 8)   # 10+8+8=26 → 上限 15
    assert "[score] XUSDT taker=+10 range=+8 vol_ratio=1.8(+8) total=75" in caplog.text


def test_cap_100_and_floor_0(hub):
    assert sc._apply_score_bonuses("X", "LONG", "pullback", 90)[0] == 100
    hub["k6"] = [k(100, 40)] * 6; hub["tk"] = tk(c=5, chg=0); hub["k24"] = vrows(1000)
    assert sc._apply_score_bonuses("X", "LONG", "pullback", 7)[0] == 0          # -5 -5 → 不低于 0


def test_short_direction_symmetry(hub):
    hub["k6"] = [k(100, 40)] * 6; hub["tk"] = tk(c=10, chg=-1.0)
    s, info = sc._apply_score_bonuses("X", "SHORT", "reversal", 50)
    assert s == 65 and (info["taker"], info["range"], info["volprice"]) == (10, 8, 8)    # 上限 15


def test_switches_independent(hub, monkeypatch):
    monkeypatch.setattr(config, "SCORE_TAKER_BONUS", False, raising=False)
    s, info = sc._apply_score_bonuses("X", "LONG", "pullback", 60)
    assert info["taker"] is None and s == 75                                          # 8+8=16 → 上限 15
    monkeypatch.setattr(config, "SCORE_RANGE_BONUS", False, raising=False)
    monkeypatch.setattr(config, "SCORE_VOLPRICE_BONUS", False, raising=False)
    assert sc._apply_score_bonuses("X", "LONG", "pullback", 60) == (60, {**info, "range": None, "range_pos": None, "volprice": None, "vol_ratio": None, "hub_bonus": 0})


def test_one_failure_does_not_affect_others(hub, monkeypatch):
    hub["tk"] = {"symbol": "X", "lastPrice": "bad", "highPrice": "1", "lowPrice": "0", "priceChangePercent": "1"}
    s, info = sc._apply_score_bonuses("X", "LONG", "pullback", 60)
    assert info["range"] is None and info["taker"] == 10 and info["volprice"] == 8 and s == 75   # 18 → 上限 15


def test_hub_not_ready_skips_all(monkeypatch):
    monkeypatch.setattr(sc, "_bonus_hub_klines", lambda *a: None)
    monkeypatch.setattr(sc, "_bonus_hub_ticker", lambda s: None)
    s, info = sc._apply_score_bonuses("X", "LONG", "pullback", 60)
    assert s == 60 and all(info[x] is None for x in ("taker", "range", "volprice"))


def test_funding_arb_skipped(hub):
    assert sc._apply_score_bonuses("X", "LONG", "funding_arb", 60)[0] == 60


def test_hub_helpers_respect_flag_and_hub(monkeypatch):
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", False, raising=False)
    assert sc._bonus_hub_klines("BTCUSDT", "1h", 6) is None and sc._bonus_hub_ticker("BTCUSDT") is None
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", True, raising=False)
    from shared import market_data_hub as H
    monkeypatch.setattr(H, "get_klines", lambda s, i, n: [k()] * n)
    monkeypatch.setattr(H, "get_ticker", lambda s: tk())
    assert len(sc._bonus_hub_klines("BTCUSDT", "1h", 6)) == 6 and sc._bonus_hub_ticker("BTCUSDT")["lastPrice"]
    monkeypatch.setattr(H, "get_klines", lambda s, i, n: None)
    assert sc._bonus_hub_klines("BTCUSDT", "1h", 6) is None                  # 不回落 REST


def test_hub_cap_floor_defaults_symmetric():
    import importlib, config as _c
    src = open(_c.__file__, encoding="utf-8").read()
    assert "SCORE_HUB_BONUS_CAP            = 12" in src and "SCORE_HUB_PENALTY_FLOOR        = -12" in src


def test_config_switches_default_on():
    assert config.SCORE_TAKER_BONUS is True and config.SCORE_RANGE_BONUS is True and config.SCORE_VOLPRICE_BONUS is True


def test_hub_bonus_cap_only_limits_positive(hub, monkeypatch):
    monkeypatch.setattr(config, "SCORE_HUB_BONUS_CAP", 15, raising=False)
    hub["k6"] = [k(100, 40)] * 6; hub["tk"] = tk(c=5, chg=0); hub["k24"] = vrows(1000)
    assert sc._apply_score_bonuses("X", "LONG", "pullback", 60)[0] == 50     # -5 -5：负值照常扣，不受上限影响


def test_hub_penalty_floor(hub, monkeypatch):
    monkeypatch.setattr(config, "SCORE_HUB_PENALTY_FLOOR", -10, raising=False)
    monkeypatch.setattr(sc, "_calc_taker_bonus", lambda d, r: (-5, 0.4))
    monkeypatch.setattr(sc, "_calc_range_bonus", lambda d, t: (-5, 0.1))
    monkeypatch.setattr(sc, "_calc_volprice_bonus", lambda d, r, t: (-5, 1.0))
    s, info = sc._apply_score_bonuses("X", "LONG", "pullback", 60)
    assert info["hub_bonus"] == -10 and s == 50          # -15 → 下限 -10
