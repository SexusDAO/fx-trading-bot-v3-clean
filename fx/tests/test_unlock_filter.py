"""代币解锁风险过滤：取数缓存/失败放行、UTC 日历天窗口、占比阈值、只拦 LONG、开关关闭只记录。全部 mock，不联网。"""
import datetime as dt
import logging

import pytest
import scanner as sc
import shared.clawby_adapter as C

NOW = dt.datetime(2026, 10, 26, 15, 30, tzinfo=dt.timezone.utc)      # 当天下午；解锁日是 UTC 0 点整
NOW_MS = NOW.timestamp() * 1000


def day_ms(offset_days):
    d = (NOW + dt.timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    return d.timestamp() * 1000


def row(sym, offset, pct, usd=15_000_000.0):
    return {"symbol": sym, "next_unlock_date": day_ms(offset), "next_unlock_usd": usd, "next_unlock_of_circulating": pct}


@pytest.fixture
def unlocks(monkeypatch):
    """替换 scanner 使用的 get_upcoming_unlocks。"""
    box = {"data": {}}
    monkeypatch.setattr(C, "get_upcoming_unlocks", lambda: box["data"])
    return box


# ── _check_unlock_risk ──
def test_hit_within_window_and_pct(unlocks):
    unlocks["data"] = {"ARB": row("ARB", 3, 1.34)}
    hit, msg = sc._check_unlock_risk("ARBUSDT", "LONG", NOW_MS)
    assert hit and "3天后" in msg and "1.34%" in msg and "15.0M" in msg


def test_window_boundary_7_days_inclusive_8_excluded(unlocks):
    unlocks["data"] = {"A": row("A", 7, 2.0), "B": row("B", 8, 2.0)}
    assert sc._check_unlock_risk("AUSDT", "LONG", NOW_MS)[0] is True
    assert sc._check_unlock_risk("BUSDT", "LONG", NOW_MS)[0] is False


def test_today_counts_even_after_midnight_utc(unlocks):
    unlocks["data"] = {"A": row("A", 0, 1.0)}                   # 今天 0 点已过，但按日历天仍算“今天解锁”
    hit, msg = sc._check_unlock_risk("AUSDT", "LONG", NOW_MS)
    assert hit and "0天后" in msg


def test_past_unlock_date_ignored(unlocks):
    unlocks["data"] = {"A": row("A", -1, 5.0)}
    assert sc._check_unlock_risk("AUSDT", "LONG", NOW_MS)[0] is False


def test_pct_threshold_boundary(unlocks):
    unlocks["data"] = {"A": row("A", 2, 0.99), "B": row("B", 2, 1.0), "C": row("C", 2, 1.01)}
    assert [sc._check_unlock_risk(f"{c}USDT", "LONG", NOW_MS)[0] for c in "ABC"] == [False, True, True]


def test_pct_is_percentage_not_fraction(unlocks):
    unlocks["data"] = {"A": row("A", 2, 0.0134)}                # 0.0134% 不应命中（防止把小数当百分数）
    assert sc._check_unlock_risk("AUSDT", "LONG", NOW_MS)[0] is False


def test_short_never_hit(unlocks):
    unlocks["data"] = {"ARB": row("ARB", 1, 9.0)}
    assert sc._check_unlock_risk("ARBUSDT", "SHORT", NOW_MS) == (False, "")
    assert sc._check_unlock_risk("ARBUSDT", "short", NOW_MS) == (False, "")


def test_symbol_not_in_list_passes(unlocks):
    unlocks["data"] = {"ARB": row("ARB", 1, 9.0)}
    assert sc._check_unlock_risk("OPUSDT", "LONG", NOW_MS) == (False, "")


def test_empty_data_and_exceptions_pass(unlocks, monkeypatch):
    unlocks["data"] = {}
    assert sc._check_unlock_risk("ARBUSDT", "LONG", NOW_MS) == (False, "")
    monkeypatch.setattr(C, "get_upcoming_unlocks", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    assert sc._check_unlock_risk("ARBUSDT", "LONG", NOW_MS) == (False, "")


def test_missing_or_zero_date_passes(unlocks):
    unlocks["data"] = {"A": {"next_unlock_date": 0, "next_unlock_of_circulating": 9.0},
                       "B": {"next_unlock_of_circulating": 9.0}}
    assert sc._check_unlock_risk("AUSDT", "LONG", NOW_MS)[0] is False
    assert sc._check_unlock_risk("BUSDT", "LONG", NOW_MS)[0] is False


# ── _apply_unlock_filter：开关 ──
def sigs():
    return [{"symbol": "ARBUSDT", "direction": "LONG"}, {"symbol": "ARBUSDT", "direction": "SHORT"},
            {"symbol": "OPUSDT", "direction": "LONG"}]


def test_switch_off_logs_only_and_keeps_all(unlocks, monkeypatch, caplog):
    unlocks["data"] = {"ARB": row("ARB", 3, 1.34)}
    monkeypatch.setattr(sc, "UNLOCK_FILTER_ENABLED", False)
    result = {"rejected": []}
    with caplog.at_level(logging.INFO, logger=sc.logger.name):
        out = sc._apply_unlock_filter(sigs(), result, NOW_MS)
    assert len(out) == 3 and result["rejected"] == []
    assert "[unlock] ARBUSDT LONG 解锁预警" in caplog.text and "仅记录" in caplog.text


def test_switch_on_rejects_only_hit_long(unlocks, monkeypatch):
    unlocks["data"] = {"ARB": row("ARB", 3, 1.34)}
    monkeypatch.setattr(sc, "UNLOCK_FILTER_ENABLED", True)
    result = {"rejected": []}
    out = sc._apply_unlock_filter(sigs(), result, NOW_MS)
    assert [(s["symbol"], s["direction"]) for s in out] == [("ARBUSDT", "SHORT"), ("OPUSDT", "LONG")]
    assert len(result["rejected"]) == 1 and result["rejected"][0]["symbol"] == "ARBUSDT"
    assert "解锁预警" in result["rejected"][0]["reason"]


def test_default_config_is_off():
    import config
    assert config.UNLOCK_FILTER_ENABLED is False
    assert config.UNLOCK_FILTER_DAYS == 7 and config.UNLOCK_FILTER_PCT == 1.0


# ── get_upcoming_unlocks：翻页 / 缓存 / 失败 ──
def page(n, start=0, sym_fmt="C{}"):
    return [{"symbol": sym_fmt.format(start + i), "next_unlock_date": 1e12, "next_unlock_usd": 1.0,
             "next_unlock_of_circulating": 2.0} for i in range(n)]


@pytest.fixture
def relay(monkeypatch):
    box = {"pages": [], "calls": []}
    def fake(name, params=None, **k):
        box["calls"].append((name, dict(params), k))
        p = box["pages"].pop(0)
        if isinstance(p, Exception):
            raise p
        return p
    monkeypatch.setattr(C, "clawby_relay", fake)
    C._UNLOCK_CACHE.update(ts=0.0, ttl=0.0, data={})
    yield box
    C._UNLOCK_CACHE.update(ts=0.0, ttl=0.0, data={})


def test_paginates_until_short_page_and_indexes_by_symbol(relay):
    relay["pages"] = [page(100, 0), page(100, 100), page(80, 200)]
    d = C.get_upcoming_unlocks()
    assert len(d) == 280 and len(relay["calls"]) == 3
    assert [c[1] for c in relay["calls"]] == [{"per_page": 100, "page": p} for p in (1, 2, 3)]
    assert all(c[0] == "coin_unlock_list" and c[2].get("_max_retries") == 0 for c in relay["calls"])
    assert set(d["C0"]) == {"next_unlock_date", "next_unlock_usd", "next_unlock_of_circulating"}


def test_cache_6h_no_refetch(relay, monkeypatch):
    relay["pages"] = [page(10)]
    d1 = C.get_upcoming_unlocks()
    d2 = C.get_upcoming_unlocks()
    assert d1 is d2 and len(relay["calls"]) == 1
    assert C._UNLOCK_CACHE["ttl"] == 6 * 3600
    real = C.time.time()
    monkeypatch.setattr(C.time, "time", lambda: real + 6 * 3600 + 1)    # 过期后重拉
    relay["pages"] = [page(5)]
    assert len(C.get_upcoming_unlocks()) == 5 and len(relay["calls"]) == 2


def test_failure_returns_empty_warns_and_short_cache(relay, caplog):
    relay["pages"] = [None]
    with caplog.at_level(logging.WARNING, logger="fx.clawby"):
        assert C.get_upcoming_unlocks() == {}
    assert "[unlock]" in caplog.text
    assert C._UNLOCK_CACHE["ttl"] == C.UNLOCK_FAIL_TTL
    C.get_upcoming_unlocks()
    assert len(relay["calls"]) == 1                                      # 失败也短缓存，不会每个信号重打


def test_partial_failure_keeps_fetched_pages_short_cache(relay):
    relay["pages"] = [page(100, 0), None]
    d = C.get_upcoming_unlocks()
    assert len(d) == 100 and C._UNLOCK_CACHE["ttl"] == C.UNLOCK_FAIL_TTL


def test_exception_and_no_key_return_empty(relay):
    relay["pages"] = [EnvironmentError("未配置任何 Clawby key")]
    assert C.get_upcoming_unlocks() == {}


def test_bad_rows_skipped_and_dedup(relay):
    relay["pages"] = [[{"symbol": "A", "next_unlock_date": "x"}, "junk", None, {"symbol": ""},
                       {"symbol": "b", "next_unlock_date": 5, "next_unlock_usd": None, "next_unlock_of_circulating": "1.5"},
                       {"symbol": "B", "next_unlock_date": 9}]]
    d = C.get_upcoming_unlocks()
    assert set(d) == {"B"} and d["B"]["next_unlock_of_circulating"] == 1.5 and d["B"]["next_unlock_usd"] == 0.0
    assert d["B"]["next_unlock_date"] == 5                               # 同名币只取第一条（列表按解锁时间升序，最近的在前）


def test_filter_wired_into_chain_after_loss_blacklist_before_news():
    import re
    src = open(sc.__file__, encoding="utf-8").read()
    i_bl = src.index("avail_signals = filtered_bl")
    i_un = src.index("_apply_unlock_filter(avail_signals, result)")
    i_news = src.index("② 新闻信号过滤（v3.8")
    assert i_bl < i_un < i_news
