"""coin-list 1h 多空失衡：计算、阈值、方向、标记写入。全部 mock，不联网。"""
import sys
import types

import pytest
import scanner as sc


def it(sym, long_, short_):
    return {"symbol": sym, "long_liquidation_usd_1h": long_, "short_liquidation_usd_1h": short_}


def test_short_liquidated_more_marks_short():
    # 空头被清算多 = heavy_short_liq（超涨回落）→ SHORT，与 coinglass_api.liq_signal 一致
    assert sc._calc_liq_bias([it("ARB", 100, 30000)]) == {"ARBUSDT": "SHORT"}


def test_long_liquidated_more_marks_long():
    # 多头被清算多 = heavy_long_liq（超卖反弹）→ LONG
    assert sc._calc_liq_bias([it("OP", 30000, 100)]) == {"OPUSDT": "LONG"}


def test_total_below_threshold_filtered():
    assert sc._calc_liq_bias([it("A", 0, 9999)]) == {}
    assert sc._calc_liq_bias([it("A", 0, 10000)]) == {"AUSDT": "SHORT"}           # 恰好 1 万：通过（>=）


def test_ratio_below_threshold_filtered():
    # 大/(小+1)：7500/(2500+1)=2.9988 < 3；
    assert sc._calc_liq_bias([it("A", 7500, 2500)]) == {}
    # 7600/(2400+1)=3.165 >= 3
    assert sc._calc_liq_bias([it("A", 7600, 2400)]) == {"AUSDT": "LONG"}


def test_zero_on_one_side_no_div_by_zero():
    assert sc._calc_liq_bias([it("A", 0, 10000)]) == {"AUSDT": "SHORT"}
    assert sc._calc_liq_bias([it("A", 10000, 0)]) == {"AUSDT": "LONG"}


def test_balanced_not_marked():
    assert sc._calc_liq_bias([it("A", 20000, 20000)]) == {}


def test_int_float_str_none_and_bad_values():
    items = [it("I", 0, 20000), it("F", 20000.5, 1.0), it("S", "0", "20000"), it("N", None, 20000),
             it("BAD", "x", 20000), {"symbol": "", "long_liquidation_usd_1h": 0, "short_liquidation_usd_1h": 99999},
             {"long_liquidation_usd_1h": 0, "short_liquidation_usd_1h": 99999}, "junk", None,
             {"symbol": "NEG", "long_liquidation_usd_1h": -5, "short_liquidation_usd_1h": 99999}]
    assert sc._calc_liq_bias(items) == {"IUSDT": "SHORT", "FUSDT": "LONG", "SUSDT": "SHORT", "NUSDT": "SHORT"}


def test_symbol_suffix_normalized_and_missing_fields():
    out = sc._calc_liq_bias([{"symbol": "ethusdt", "long_liquidation_usd_1h": 0, "short_liquidation_usd_1h": 50000},
                             {"symbol": "NOFIELDS"}])
    assert out == {"ETHUSDT": "SHORT"}


def test_thresholds_come_from_config_and_can_be_overridden():
    assert sc.LIQ_BIAS_MIN_TOTAL_1H == 10000 and sc.LIQ_BIAS_MIN_RATIO == 3.0
    assert sc._calc_liq_bias([it("A", 0, 500)], min_total=100, min_ratio=2.0) == {"AUSDT": "SHORT"}


def test_direction_convention_documented_and_single_point():
    # 与 coinglass_api.liq_signal 一致：多头被清算多 → LONG；空头被清算多 → SHORT
    assert sc._liq_bias_direction(1.0, 0.0) == "LONG" and sc._liq_bias_direction(0.0, 1.0) == "SHORT"


# ── 接入 refresh_pool：标记写入、不影响评分 ──
def _env(monkeypatch, tmp_path, liq):
    import config
    monkeypatch.setattr(sc, "POOL_CACHE_FILE", tmp_path / "pool.json")
    monkeypatch.setattr(sc, "_is_weekend_period", lambda *_: False)
    monkeypatch.setattr(config, "ENABLE_USDC_PAIRS", False, raising=False)
    monkeypatch.setattr(config, "SCAN_POOL_MAX", 200, raising=False)
    monkeypatch.setattr(sc, "POOL_GAINERS_N", 0); monkeypatch.setattr(sc, "POOL_LOSERS_N", 0)
    monkeypatch.setattr(sc, "_POOL_SCORE_VOL_TOP", 0)
    monkeypatch.setattr(sc, "_tradifi_symbols", lambda: frozenset())
    tk = [{"symbol": s, "quoteVolume": "50000000", "priceChangePercent": "1", "lowPrice": "100",
           "highPrice": "102", "lastPrice": "101"} for s in ("ARBUSDT", "OPUSDT", "PLAINUSDT")]
    monkeypatch.setattr(sc.api, "get_ticker_24h", lambda: tk)
    monkeypatch.setattr(sc.api, "get_all_mark_prices",
                        lambda: [{"symbol": t["symbol"], "markPrice": "101", "lastFundingRate": "0.0001"} for t in tk])
    mod = types.ModuleType("shared.clawby_adapter")
    mod.get_liq_coin_list = lambda: liq
    monkeypatch.setitem(sys.modules, "shared.clawby_adapter", mod)


def test_refresh_pool_writes_bias_without_changing_scores(monkeypatch, tmp_path):
    liq = [{"symbol": "ARB", "liquidation_usd_24h": 5, "long_liquidation_usd_1h": 0, "short_liquidation_usd_1h": 50000},
           {"symbol": "OP", "liquidation_usd_24h": 4, "long_liquidation_usd_1h": 50000, "short_liquidation_usd_1h": 0},
           {"symbol": "PLAIN", "liquidation_usd_24h": 1, "long_liquidation_usd_1h": 10, "short_liquidation_usd_1h": 10}]
    _env(monkeypatch, tmp_path, liq)
    sc._LIQ_BIAS.clear()
    pool = sc.refresh_pool()
    assert sc.get_liq_bias("ARBUSDT") == "SHORT" and sc.get_liq_bias("opusdt") == "LONG"
    assert sc.get_liq_bias("PLAINUSDT") is None and sc.get_liq_bias("") is None
    assert set(pool) == {"ARBUSDT", "OPUSDT", "PLAINUSDT"}      # 池子成员/评分未被失衡标记改变（本提交只标记不加分）


def test_refresh_pool_bias_replaced_each_round(monkeypatch, tmp_path):
    liq = [{"symbol": "ARB", "liquidation_usd_24h": 5, "long_liquidation_usd_1h": 0, "short_liquidation_usd_1h": 50000}]
    _env(monkeypatch, tmp_path, liq)
    sc.refresh_pool()
    assert sc.get_liq_bias("ARBUSDT") == "SHORT"
    liq[0]["short_liquidation_usd_1h"] = 0                       # 下一轮失衡消失 → 旧标记必须被清掉
    sc.refresh_pool()
    assert sc.get_liq_bias("ARBUSDT") is None


def test_bias_includes_coins_outside_24h_top_n(monkeypatch, tmp_path):
    """独立于 24h 总量 Top30：24h 排名靠后但 1h 严重失衡的币也要被标记。"""
    monkeypatch.setattr(sc, "POOL_LIQ_TOP_N", 1)
    liq = [{"symbol": "ARB", "liquidation_usd_24h": 9e9, "long_liquidation_usd_1h": 1, "short_liquidation_usd_1h": 1},
           {"symbol": "OP", "liquidation_usd_24h": 1, "long_liquidation_usd_1h": 0, "short_liquidation_usd_1h": 50000}]
    _env(monkeypatch, tmp_path, liq)
    sc.refresh_pool()
    assert sc.get_liq_bias("OPUSDT") == "SHORT" and sc.get_liq_bias("ARBUSDT") is None
