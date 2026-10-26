"""大户持仓多空比影子记录：取数/缓存/失败返回 None；日志格式；任何异常都不影响信号。全部 mock，不联网。"""
import logging

import pytest
import scanner as sc
import shared.clawby_adapter as C

ROWS = [{"time": 1793012400000, "top_position_long_percent": 57.3, "top_position_short_percent": 42.7, "top_position_long_short_ratio": 1.34},
        {"time": 1793016000000, "top_position_long_percent": 57.49, "top_position_short_percent": 42.51, "top_position_long_short_ratio": 1.35}]


@pytest.fixture(autouse=True)
def _keys():
    old = list(C._CLAWBY_KEYS)
    C._configure_keys(["k-test-1"], announce=False)
    yield
    C._configure_keys(old, announce=False)


@pytest.fixture
def mem(monkeypatch):
    store = {}
    monkeypatch.setattr(C, "_cache_get", lambda k: store.get(k))
    monkeypatch.setattr(C, "_cache_set", lambda k, v: store.__setitem__(k, v))
    return store


def test_returns_latest_row_with_floats(monkeypatch, mem):
    calls = []
    monkeypatch.setattr(C, "clawby_relay", lambda name, params=None, **k: calls.append((name, params, k)) or ROWS)
    r = C.get_top_trader_ratio("ARB")
    assert calls[0][0] == "futures_top_long_short_position_ratio_history"
    assert calls[0][1] == {"exchange": "Binance", "symbol": "ARBUSDT", "interval": "1h", "limit": 5}
    assert calls[0][2].get("_max_retries") == 0
    assert r == {"top_position_long_percent": 57.49, "top_position_short_percent": 42.51,
                 "top_position_long_short_ratio": 1.35, "time": 1793016000000}


def test_latest_by_time_even_if_order_reversed(monkeypatch, mem):
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: list(reversed(ROWS)))
    assert C.get_top_trader_ratio("ARBUSDT")["top_position_long_percent"] == 57.49


def test_string_values_converted(monkeypatch, mem):
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: [{"time": 1, "top_position_long_percent": "60", "top_position_short_percent": "40", "top_position_long_short_ratio": "1.5"}])
    assert C.get_top_trader_ratio("A")["top_position_long_short_ratio"] == 1.5


@pytest.mark.parametrize("ret", [None, [], {"code": "400"}, ["x"], [{"time": 1}], [{"time": 1, "top_position_long_percent": "abc",
                                                                                     "top_position_short_percent": 1, "top_position_long_short_ratio": 1}]])
def test_failures_return_none_and_not_cached(monkeypatch, mem, ret):
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: ret)
    assert C.get_top_trader_ratio("ARB") is None and mem == {}


def test_exception_and_no_key_return_none(monkeypatch, mem):
    def boom(*a, **k):
        raise RuntimeError("429")
    monkeypatch.setattr(C, "clawby_relay", boom)
    assert C.get_top_trader_ratio("ARB") is None
    C._configure_keys([], announce=False)
    monkeypatch.undo()
    assert C.get_top_trader_ratio("ARB") is None


def test_cached_15min_no_refetch(monkeypatch, mem):
    n = []
    monkeypatch.setattr(C, "clawby_relay", lambda *a, **k: n.append(1) or ROWS)
    C.get_top_trader_ratio("ARB"); C.get_top_trader_ratio("ARB")
    assert len(n) == 1 and "top_trader:ARBUSDT:1h" in mem
    assert C.CACHE_TTL == 15 * 60


# ── scanner 影子日志 ──
def test_log_line_format(monkeypatch, caplog):
    monkeypatch.setattr(C, "get_top_trader_ratio", lambda s: {"top_position_long_percent": 57.49, "top_position_short_percent": 42.51,
                                                              "top_position_long_short_ratio": 1.35})
    with caplog.at_level(logging.INFO, logger=sc.logger.name):
        n = sc._log_top_trader_shadow([{"symbol": "ARBUSDT", "direction": "LONG"}])
    assert n == 1
    assert "[top_trader] ARBUSDT LONG 大户多头=57.5% 空头=42.5% 比值=1.35" in caplog.text


def test_shadow_never_mutates_signals_or_raises(monkeypatch):
    sigs = [{"symbol": "A", "direction": "LONG", "final_score": 77, "ind": {"final_score": 77}}, {"symbol": "B", "direction": "SHORT"}, None, "junk"]
    before = repr(sigs)
    def boom(s):
        if s == "A":
            raise RuntimeError("x")
        return None
    monkeypatch.setattr(C, "get_top_trader_ratio", boom)
    assert sc._log_top_trader_shadow(sigs) == 0
    assert repr(sigs) == before and len(sigs) == 4


def test_empty_inputs():
    assert sc._log_top_trader_shadow([]) == 0 and sc._log_top_trader_shadow(None) == 0


def test_hook_placed_after_slot_filter_before_registration():
    src = open(sc.__file__, encoding="utf-8").read()
    i_slot = src.index('result["signals_final"] = len(avail_signals)')
    i_hook = src.index("_log_top_trader_shadow(avail_signals)")
    i_reg = src.index("if not dry_run and avail_signals:")
    assert i_slot < i_hook < i_reg
    assert "final_score" not in src[src.index("def _log_top_trader_shadow"):src.index("# ── 代币解锁风险过滤")]   # 影子函数不碰评分
