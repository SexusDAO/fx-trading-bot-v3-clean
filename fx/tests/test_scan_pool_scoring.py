"""refresh_pool：粗筛 + OI 过滤 + 评分排序 + 截断（全 mock，不联网）。"""
import json
import sys
import threading
import time
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import scanner as sc


def tk(sym, vol=50_000_000, chg=1.0, lo=100.0, hi=102.0, last=101.0):
    return {"symbol": sym, "quoteVolume": str(vol), "priceChangePercent": str(chg),
            "lowPrice": str(lo), "highPrice": str(hi), "lastPrice": str(last)}


@pytest.fixture
def env(monkeypatch, tmp_path):
    import config
    monkeypatch.setattr(sc, "POOL_CACHE_FILE", tmp_path / "pool.json")
    monkeypatch.setattr(sc, "_is_weekend_period", lambda *_: False)
    monkeypatch.setattr(config, "ENABLE_USDC_PAIRS", False, raising=False)
    monkeypatch.setattr(config, "SCAN_POOL_MAX", 200, raising=False)
    monkeypatch.setattr(config, "SCAN_POOL_MIN_OI_U", 1_000_000, raising=False)
    monkeypatch.setattr(sc, "POOL_GAINERS_N", 0); monkeypatch.setattr(sc, "POOL_LOSERS_N", 0)
    st = types.SimpleNamespace(tickers=[], marks=[], oi={}, oi_calls=[], liq=[])
    monkeypatch.setattr(sc.api, "get_ticker_24h", lambda: st.tickers)
    monkeypatch.setattr(sc.api, "get_all_mark_prices", lambda: st.marks)

    def oi(sym):
        st.oi_calls.append(sym)
        v = st.oi.get(sym, 5_000_000 / 101.0)               # 默认 OI ≈ 500 万 U（价格 101）
        if isinstance(v, Exception):
            raise v
        return {"openInterest": str(v)} if v is not None else {}
    monkeypatch.setattr(sc.api, "get_open_interest", oi)
    mod = types.ModuleType("shared.clawby_adapter")
    mod.get_liq_coin_list = lambda: st.liq
    monkeypatch.setitem(sys.modules, "shared.clawby_adapter", mod)
    return st


def set_market(st, tickers, funding=None):
    st.tickers = tickers
    funding = funding or {}
    st.marks = [{"symbol": t["symbol"], "markPrice": "101", "lastFundingRate": str(funding.get(t["symbol"], 0.0001))} for t in tickers]


# ── 粗筛 ──────────────────────────────────────────────────────────────────
def test_coarse_filters(env):
    set_market(env, [
        tk("GOODUSDT"),
        tk("XUSDC"), tk("BTCUSDC"),                         # USDC 排除（ENABLE_USDC_PAIRS=False）
        tk("USDCUSDT"), tk("FDUSDUSDT"),                    # 稳定币黑名单
        tk("LOWVOLUSDT", vol=2_900_000),                    # 成交额 < 300 万
        tk("PUMPUSDT", chg=55), tk("DUMPUSDT", chg=-51),    # 涨跌幅 |x| > 50%
        tk("EDGEUSDT", chg=50),                             # 恰好 50% 仍通过
        tk("FLATUSDT", lo=100, hi=100.5),                   # 日内振幅 < 1%
        tk("BTCUSDT_250627"),                               # 交割合约（后缀不是 USDT）
    ])
    assert set(sc.refresh_pool()) == {"GOODUSDT", "EDGEUSDT"}


def test_usdc_allowed_when_flag_true(env, monkeypatch):
    import config
    monkeypatch.setattr(config, "ENABLE_USDC_PAIRS", True, raising=False)
    set_market(env, [tk("XUSDC"), tk("AUSDT")])
    assert set(sc.refresh_pool()) == {"XUSDC", "AUSDT"}


# ── OI ────────────────────────────────────────────────────────────────────
def test_oi_threshold_in_usd_failure_passes_and_each_symbol_queried_once(env):
    # 候选池 OI 过滤已停用；函数本体保留，单独验证其行为
    cands = [{"symbol": s, "_px": 101.0} for s in ("RICHUSDT", "POORUSDT", "ERRUSDT", "EMPTYUSDT", "EXACTUSDT")]
    env.oi.update({"POORUSDT": 9_000, "ERRUSDT": RuntimeError("boom"), "EMPTYUSDT": None, "EXACTUSDT": 1_000_000 / 101.0})
    kept, _ = sc._pool_filter_by_oi(cands, 1_000_000)
    assert {t["symbol"] for t in kept} == {"RICHUSDT", "ERRUSDT", "EMPTYUSDT", "EXACTUSDT"}
    assert sorted(env.oi_calls) == sorted(set(env.oi_calls)) and len(env.oi_calls) == 5


def test_refresh_pool_no_longer_queries_oi(env):
    set_market(env, [tk("OKUSDT"), tk("LOWVOLUSDT", vol=1_000_000), tk("XUSDC")])
    sc.refresh_pool()
    assert env.oi_calls == []                     # 候选池刷新不再逐标的查 OI


def test_oi_concurrency_capped_at_6(env, monkeypatch):
    active, peak, lock = [0], [0], threading.Lock()

    def slow(sym):
        with lock:
            active[0] += 1; peak[0] = max(peak[0], active[0])
        time.sleep(0.03)
        with lock:
            active[0] -= 1
        return {"openInterest": "100000"}
    monkeypatch.setattr(sc.api, "get_open_interest", slow)
    sc._pool_filter_by_oi([{"symbol": f"S{i}USDT", "_px": 101.0} for i in range(30)], 1_000_000)
    assert 1 < peak[0] <= 6


# ── 评分与排序 ─────────────────────────────────────────────────────────────
def test_score_components_and_stacking():
    q = [{"symbol": s, "_fr_abs": f} for s, f in
         [("ALL", 0.002), ("VOLONLY", 0.0), ("MOVER", 0.0), ("FUND", 0.002), ("LIQ", 0.0), ("NONE", 0.0)]]
    out = sc._pool_scores(q, vol_top={"ALL", "VOLONLY"}, mover_syms={"ALL", "MOVER"}, liq_syms={"ALL", "LIQ"})
    assert out == {"ALL": 100, "VOLONLY": 30, "MOVER": 25, "FUND": 25, "LIQ": 20, "NONE": 0}   # 30+25+25+20


def test_volume_rank_top80_gets_40_over_whole_market(env):
    set_market(env, [tk(f"V{i:02d}USDT", vol=900_000_000 - i * 1_000_000) for i in range(90)])
    pool = sc.refresh_pool()
    assert pool[:90] == [f"V{i:02d}USDT" for i in range(90)]            # 评分相同的部分按成交额降序
    out = sc._pool_scores([{"symbol": "a", "_fr_abs": 0}], {"a"}, set(), set())
    assert out["a"] == 30 and sc._POOL_SCORE_VOL_TOP == 80


def test_ranking_by_score_then_volume_and_cap(env, monkeypatch):
    import config
    monkeypatch.setattr(config, "SCAN_POOL_MAX", 3, raising=False)
    monkeypatch.setattr(sc, "_POOL_SCORE_VOL_TOP", 0)                   # 关掉成交额排名分，便于只看其他分项
    monkeypatch.setattr(sc, "POOL_FUNDING_EXTREME_MIN", 0.001)
    set_market(env, [tk("BIGUSDT", vol=900_000_000), tk("FUNDUSDT", vol=10_000_000), tk("LIQUSDT", vol=20_000_000),
                     tk("BOTHUSDT", vol=5_000_000), tk("PLAINUSDT", vol=800_000_000)],
               funding={"FUNDUSDT": 0.002, "BOTHUSDT": 0.002})
    env.liq = [{"symbol": "LIQ", "liq_usd": 5}, {"symbol": "BOTH", "liq_usd": 4}]
    assert sc.refresh_pool() == ["BOTHUSDT", "FUNDUSDT", "LIQUSDT"]      # 45 > 25(费率) > 20(清算)；BIG/PLAIN 评分为 0 被截断


def test_gainers_losers_get_mover_points(env, monkeypatch):
    monkeypatch.setattr(sc, "POOL_GAINERS_N", 1); monkeypatch.setattr(sc, "POOL_LOSERS_N", 1)
    monkeypatch.setattr(sc, "_POOL_SCORE_VOL_TOP", 0)
    set_market(env, [tk("MIDUSDT", chg=0.0, vol=900_000_000), tk("UPUSDT", chg=9.0, vol=1_000_000 * 5),
                     tk("DOWNUSDT", chg=-9.0, vol=1_000_000 * 4)])
    assert sc.refresh_pool() == ["UPUSDT", "DOWNUSDT", "MIDUSDT"]


def test_cache_written_and_returned_pool_matches(env, tmp_path):
    set_market(env, [tk("AUSDT"), tk("BUSDT")])
    pool = sc.refresh_pool()
    saved = json.loads((tmp_path / "pool.json").read_text())
    assert saved["pool"] == pool


def test_api_failure_returns_empty(env, monkeypatch):
    monkeypatch.setattr(sc.api, "get_ticker_24h", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert sc.refresh_pool() == []


def test_config_values():
    import config
    assert config.SCAN_POOL_MAX == 165 and config.SCAN_POOL_MIN_OI_U == 1_000_000
    assert config.POOL_MAX_CHG24_PCT == 50.0 and config.ENABLE_USDC_PAIRS is False
