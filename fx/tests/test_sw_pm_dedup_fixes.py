"""SL-BREACH 脏数据过滤 / 补挂重入检查 / 退出通知去重。全部 mock。"""
import asyncio
import logging
import time

import pytest

import shared.signal_watcher as sw
import position_manager as pm


# ── 改动一：SL-BREACH ──
def _w(positions, monkeypatch):
    w = sw.SignalWatcher.__new__(sw.SignalWatcher)
    w._open_pos_ts = 0.0
    w._open_pos_cache = []
    w._sl_breach_alerted = {}
    import shared.state as ST
    monkeypatch.setattr(ST, "get_all_positions", lambda: positions)
    sent = []
    import shared.notifier as nt
    monkeypatch.setattr(nt, "notify", lambda t: sent.append(t))
    return w, sent


def _pos(**kw):
    p = {"symbol": "ETHUSDT", "status": "open", "direction": "long", "entry_price": 2000.0, "sl_price": 1900.0}
    p.update(kw)
    return p


def _run(w, price):
    async def go():
        await w._check_sl_breach("ETHUSDT", price)
        await asyncio.sleep(0.05)
    asyncio.run(go())


def test_valid_breach_alerts(monkeypatch):
    w, sent = _w([_pos()], monkeypatch)
    _run(w, 1890.0)
    assert len(sent) == 1 and "SL-BREACH" in sent[0]


def test_long_sl_above_entry_not_alerted(monkeypatch, caplog):
    """LONG 止损在入场价上方 → 脏数据，不告警，记 debug。"""
    w, sent = _w([_pos(sl_price=2100.0)], monkeypatch)
    with caplog.at_level(logging.DEBUG):
        _run(w, 2050.0)                       # price <= sl 成立，但方向不合理
    assert sent == []
    assert any("方向不合理" in r.getMessage() for r in caplog.records)


def test_short_sl_below_entry_not_alerted(monkeypatch):
    w, sent = _w([_pos(direction="short", entry_price=2000.0, sl_price=1900.0)], monkeypatch)
    _run(w, 1950.0)                           # price >= sl 成立，但 SHORT 止损在入场价下方
    assert sent == []


def test_short_valid_breach_alerts(monkeypatch):
    w, sent = _w([_pos(direction="short", entry_price=2000.0, sl_price=2100.0)], monkeypatch)
    _run(w, 2110.0)
    assert len(sent) == 1


def test_eth_sl_95_stale_record_ignored(monkeypatch):
    """复现线上：ETH 现价 2000+，历史脏记录 sl=95 entry=95.28（LONG）——现价高于止损，本就不会触发；
    反向脏数据（SHORT, sl=95<entry=2000）则被方向校验挡住。"""
    w, sent = _w([_pos(direction="short", entry_price=2000.0, sl_price=95.0)], monkeypatch)
    _run(w, 2100.0)
    assert sent == []


@pytest.mark.parametrize("bad", [{"status": "closed"}, {"sl_price": 0}, {"entry_price": 0}, {"entry_price": None},
                                 {"sl_price": "abc"}])
def test_cache_filters_invalid_records(monkeypatch, bad):
    w, sent = _w([_pos(**bad)], monkeypatch)
    _run(w, 1890.0)
    assert w._open_pos_cache == [] and sent == []


# ── 改动三：退出通知去重 ──
def _entry(sym="XUSDT"):
    return sw.WatchEntry(signal={"symbol": sym, "direction": "LONG", "mode": "pullback"},
                         entry_low=1, entry_high=2, entry_price=1.5, expire_ts=9e18, on_trigger=None)


def _watcher():
    w = sw.SignalWatcher.__new__(sw.SignalWatcher)
    w._lock = asyncio.Lock()
    w._watched = {}
    return w


def test_exit_notice_sent_once_per_entry_id(monkeypatch):
    import shared.notifier as nt
    sent = []
    monkeypatch.setattr(nt, "notify", lambda t: sent.append(t))
    w, e = _watcher(), _entry()

    async def go():
        await asyncio.gather(w._notify_exit("XUSDT", e, "BTC 1H急跌"), w._notify_exit("XUSDT", e, "recheck 失效"))
    asyncio.run(go())
    assert len(sent) == 1


def test_exit_notice_distinct_entries_both_sent(monkeypatch):
    import shared.notifier as nt
    sent = []
    monkeypatch.setattr(nt, "notify", lambda t: sent.append(t))
    w = _watcher()
    asyncio.run(w._notify_exit("XUSDT", _entry(), "a"))
    asyncio.run(w._notify_exit("XUSDT", _entry(), "b"))
    assert len(sent) == 2


def test_exit_notice_not_recorded_when_entry_still_watched(monkeypatch):
    """条目仍在监控中（撤单未确认）不发，也不能占用去重名额；之后真正移除时仍要能发。"""
    import shared.notifier as nt
    sent = []
    monkeypatch.setattr(nt, "notify", lambda t: sent.append(t))
    w, e = _watcher(), _entry()
    w._watched["XUSDT"] = e
    asyncio.run(w._notify_exit("XUSDT", e, "x"))
    assert sent == []
    w._watched.pop("XUSDT")
    asyncio.run(w._notify_exit("XUSDT", e, "x"))
    assert len(sent) == 1


def test_gc_cleans_notified_exit_older_than_1h(monkeypatch):
    w = _watcher()
    w._notified_exit = {"old": time.time() - 3700, "new": time.time() - 10}
    w._reconcile_orphans = lambda: asyncio.sleep(0)
    import shared.notifier as nt
    monkeypatch.setattr(nt, "tick_signal_buffer", lambda: None)
    asyncio.run(w._gc_pass())
    assert list(w._notified_exit) == ["new"]


# ── 改动二：补挂重入 ──
def _guard_pos(**kw):
    p = {"pos_id": "pid1", "symbol": "LTCUSDT", "direction": "LONG", "status": "pending_guard", "sl_price": 58.0,
         "tp1_price": 70.0}
    p.update(kw)
    return p


def _call_guard(monkeypatch, latest):
    called = {"api": 0}
    monkeypatch.setattr(pm, "get_all_positions", lambda: [latest] if latest is not None else [])
    import shared.binance_api as BA

    def spy(*a, **k):
        called["api"] += 1
        return []
    monkeypatch.setattr(BA, "get_position", spy)
    pm._guard_locks.pop("pid1", None)
    asyncio.run(pm._补挂保护单(_guard_pos(), 60.0, 1.0, "pid1", "LTCUSDT"))
    return called["api"]


def test_guard_skipped_when_other_path_already_completed(monkeypatch, caplog):
    latest = _guard_pos(status="open", sl_order_id=555, missing_guards=[])
    with caplog.at_level(logging.INFO):
        assert _call_guard(monkeypatch, latest) == 0
    assert any("已被其他路径处理" in r.getMessage() for r in caplog.records)


def test_guard_not_blocked_when_record_unreadable(monkeypatch):
    """读不到最新记录时不拦截：继续往下查交易所实际持仓（持仓为 0 会自行跳过）。"""
    assert _call_guard(monkeypatch, None) >= 1


def test_guard_skipped_when_closed(monkeypatch):
    assert _call_guard(monkeypatch, _guard_pos(status="closed")) == 0


def test_guard_not_skipped_for_pending_guard_retry(monkeypatch):
    """patrol 重试入口（pending_guard、保护单仍缺）不能被误跳过：应继续往下查交易所持仓。"""
    latest = _guard_pos(status="pending_guard", missing_guards=["sl"])
    assert _call_guard(monkeypatch, latest) >= 1


def test_guard_completion_notice_only_after_successful_write():
    import inspect
    src = inspect.getsource(pm._补挂保护单)
    assert "_upd_ok = (await asyncio.to_thread(update_position, pid, _upd))" in src
    i = src.index("补挂止损止盈完成")
    assert "if _upd_ok is not False" in src[i - 200:i]
