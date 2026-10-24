"""signal_watcher v2 改动测试：全部 mock，不联网。"""
import asyncio
import logging
import time
from unittest.mock import MagicMock

import pytest

from shared import signal_watcher as sw
from shared import btc_guard


def run(c):
    return asyncio.new_event_loop().run_until_complete(c)


@pytest.fixture(autouse=True)
def _no_net(monkeypatch):
    from shared import binance_api as api
    for n in ("get_mark_price", "get_klines", "get_position", "get_open_orders",
              "place_sl", "cancel_order", "get_order", "place_order", "get_open_interest"):
        monkeypatch.setattr(api, n, MagicMock(side_effect=AssertionError(f"未 mock 的联网调用 {n}")), raising=False)
    btc_guard.get_guard().reset()
    yield api
    btc_guard.get_guard().reset()


def _entry(sym="ETHUSDT", d="LONG", mode="pullback", lo=100.0, hi=101.0, **kw):
    sig = {"symbol": sym, "direction": d, "mode": mode, "sig_type": mode,
           "ind": {"atr_1h": 10.0}, "pre_params": {"sl_price": 90.0}}
    e = sw.WatchEntry(signal=sig, entry_low=lo, entry_high=hi, entry_price=lo,
                      expire_ts=time.time() + 3600, on_trigger=MagicMock())
    for k, v in kw.items():
        setattr(e, k, v)
    return e


def test_trigger_skip_beyond_03atr(monkeypatch, caplog):
    monkeypatch.setattr(sw, "_get_hub_or_rest", lambda s, t, *a, **k: {"markPrice": "96.0"})
    called = []

    async def on_trig(sig):
        called.append(sig)
    e = _entry()
    e.on_trigger = on_trig
    w = sw.SignalWatcher()
    w._watched["ETHUSDT"] = e
    with caplog.at_level(logging.INFO):
        run(w._trigger("ETHUSDT", 100.5, e))
    assert not called
    assert "[trigger-skip]" in caplog.text


def test_hub_stale_falls_back_rest(monkeypatch, _no_net):
    hub = MagicMock()
    hub.get_mark_price.return_value = {"markPrice": "1", "age_sec": 10}
    import shared.market_data_hub as mdh
    monkeypatch.setattr(mdh, "get_hub", lambda: hub)
    _no_net.get_mark_price = MagicMock(return_value={"markPrice": "2"})
    monkeypatch.setattr(sw, "_get_hub_or_rest", sw._get_hub_or_rest)
    from shared import binance_api
    monkeypatch.setattr(binance_api, "get_mark_price", MagicMock(return_value={"markPrice": "2"}))
    assert sw._get_hub_or_rest("ETHUSDT", "mark")["markPrice"] == "2"
    hub.get_mark_price.return_value = {"markPrice": "1", "age_sec": 1}
    assert sw._get_hub_or_rest("ETHUSDT", "mark")["markPrice"] == "1"
    # K 线过旧
    old = int((time.time() - 9000) * 1000)
    hub.get_klines.return_value = [[0, 0, 0, 0, 0, 0, old]]
    monkeypatch.setattr(binance_api, "get_klines", MagicMock(return_value=[["rest"]]))
    assert sw._get_hub_or_rest("ETHUSDT", "klines", "1h", 50) == [["rest"]]


def test_cancel_unknown_places_sl(monkeypatch, caplog):
    from shared import binance_api
    import executor
    # 业务代码会调 executor._get_precision(内部请求 exchangeInfo);打桩避免依赖真实网络/限流
    monkeypatch.setattr(executor, "_get_precision", lambda sym: {"tick": 0.01, "step": 0.001})
    monkeypatch.setattr(binance_api, "get_position",
                        MagicMock(return_value=[{"positionAmt": "0.5", "positionSide": "LONG"}]))
    sl = MagicMock(return_value={"orderId": 1})
    monkeypatch.setattr(binance_api, "place_sl", sl)
    w = sw.SignalWatcher()
    with caplog.at_level(logging.WARNING):
        assert run(w._guard_unknown_fill("ETHUSDT", _entry()))
    sl.assert_called_once_with("ETHUSDT", "SELL", "LONG", 90.0, 0.5)
    assert "[CANCEL-UNKNOWN] ETHUSDT 可能已成交，已补止损" in caplog.text


def _clean_exchange(monkeypatch):
    from shared import binance_api
    monkeypatch.setattr(binance_api, "get_position", MagicMock(return_value=[]))
    monkeypatch.setattr(binance_api, "get_open_orders", MagicMock(return_value=[]))


def _add(w, d, score):
    sig = {"symbol": "ETHUSDT", "direction": d, "mode": "pullback", "sig_type": "pullback", "ind": {"atr_1h": 1}}
    return run(w.add_signal(sig, 100, 101, 100, time.time() + 3600, MagicMock(), pending_score=score))


def test_same_symbol_higher_replaces(monkeypatch):
    _clean_exchange(monkeypatch)
    w = sw.SignalWatcher()
    old = _entry(pending_score=40.0)
    w._watched["ETHUSDT"] = old
    monkeypatch.setattr(sw, "_calc_pending_score", lambda e, now=None: 60.0)
    _add(w, "LONG", 60)
    assert w._watched["ETHUSDT"] is not old


def test_same_symbol_low_score_dropped(monkeypatch):
    _clean_exchange(monkeypatch)
    w = sw.SignalWatcher()
    old = _entry(pending_score=55.0)
    w._watched["ETHUSDT"] = old
    monkeypatch.setattr(sw, "_calc_pending_score", lambda e, now=None: 60.0)
    _add(w, "LONG", 60)
    assert w._watched["ETHUSDT"] is old


def test_opposite_direction_rejected(monkeypatch, caplog):
    _clean_exchange(monkeypatch)
    w = sw.SignalWatcher()
    old = _entry(pending_score=50.0)
    w._watched["ETHUSDT"] = old
    monkeypatch.setattr(sw, "_calc_pending_score", lambda e, now=None: 65.0)
    with caplog.at_level(logging.INFO):
        _add(w, "SHORT", 65)
    assert w._watched["ETHUSDT"] is old
    assert "方向冲突" in caplog.text


def test_funding_arb_expired_rejected(monkeypatch):
    _clean_exchange(monkeypatch)
    w = sw.SignalWatcher()
    sig = {"symbol": "XUSDT", "direction": "LONG", "sig_type": "funding_arb", "mode": "funding_arb",
           "next_settle_ts": (time.time() + 30) * 1000, "ind": {"atr_1h": 1}}
    run(w.add_signal(sig, 1, 2, 1, time.time() + 3600, MagicMock()))
    assert "XUSDT" not in w._watched


def test_funding_arb_expire_capped(monkeypatch):
    _clean_exchange(monkeypatch)
    w = sw.SignalWatcher()
    nft = time.time() + 600
    sig = {"symbol": "XUSDT", "direction": "LONG", "sig_type": "funding_arb", "mode": "funding_arb",
           "next_settle_ts": nft * 1000, "ind": {"atr_1h": 1}}
    run(w.add_signal(sig, 1, 2, 1, time.time() + 7200, MagicMock()))
    assert w._watched["XUSDT"].expire_ts <= nft - 60 + 1e-6


@pytest.mark.parametrize("pts,victim,keeper", [
    ([(1000, 100.0), (1290, 98.9)], "LONG", "SHORT"),
    ([(1000, 100.0), (1290, 101.1)], "SHORT", "LONG"),
])
def test_btc_warn_cancels_direction(monkeypatch, pts, victim, keeper):
    w = sw.SignalWatcher()
    w._watched["AUSDT"] = _entry("AUSDT", victim)
    w._watched["BUSDT"] = _entry("BUSDT", keeper)
    cancelled = []

    async def fake_cancel(s, e, r):
        cancelled.append(s)
        w._watched.pop(s, None)
    monkeypatch.setattr(w, "_cancel_and_remove", fake_cancel)
    for ts, px in pts:
        run(w._on_btc_price(px, ts))
    assert cancelled == ["AUSDT"]
    assert "BUSDT" in w._watched


def test_sl_breach_alert_only(monkeypatch, caplog):
    w = sw.SignalWatcher()
    w._open_pos_cache = [{"symbol": "ETHUSDT", "status": "open", "direction": "LONG", "sl_price": 95}]
    w._open_pos_ts = time.time()
    with caplog.at_level(logging.ERROR):
        run(w._check_sl_breach("ETHUSDT", 94.0))
    assert "[SL-BREACH] ETHUSDT 止损价已被越过，请检查仓位" in caplog.text


def test_realtime_falsify_pullback():
    e = _entry(ema20_at_trigger=100.0, atr_at_trigger=10.0)
    assert sw.SignalWatcher._realtime_falsified(e, 89.0)[0]
    assert not sw.SignalWatcher._realtime_falsified(e, 91.0)[0]


def test_liq_freshness():
    from shared import liq_cache as lc
    now = time.time()
    with lc._CACHE_LOCK:
        lc._CACHE["ZZUSDT:x"] = {"ts": now - 600}
    try:
        assert abs(sw._liq_freshness("ZZUSDT", now) - 0.5) < 1e-6
    finally:
        with lc._CACHE_LOCK:
            lc._CACHE.pop("ZZUSDT:x", None)
