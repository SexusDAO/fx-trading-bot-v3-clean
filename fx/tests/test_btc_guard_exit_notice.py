import asyncio
import shared.signal_watcher as sw
from shared import btc_guard as bg


def _mk(monkeypatch, direction="LONG", removed=True):
    w = sw.SignalWatcher.__new__(sw.SignalWatcher)
    w._lock = asyncio.Lock()
    e = sw.WatchEntry(signal={"symbol": "XUSDT", "direction": direction, "mode": "pullback"},
                      entry_low=1, entry_high=2, entry_price=1.5, expire_ts=9e18, on_trigger=None)
    w._watched = {"XUSDT": e}
    monkeypatch.setattr(bg, "update_price", lambda p, t=None: "crash")
    monkeypatch.setattr(bg, "get_state", lambda: "crash")
    class G:
        def cancel_direction(self): return "LONG"
    monkeypatch.setattr(bg, "get_guard", lambda: G())
    cancelled, notified = [], []
    async def fake_cancel(self, sym, entry, reason):
        cancelled.append(reason)
        if removed:
            self._watched.pop(sym, None)
    async def fake_notify(self, sym, entry, reason):
        notified.append(reason)
    monkeypatch.setattr(sw.SignalWatcher, "_cancel_and_remove", fake_cancel)
    monkeypatch.setattr(sw.SignalWatcher, "_notify_exit", fake_notify)
    return w, e, cancelled, notified


def test_guard_cancel_then_notify(monkeypatch):
    w, e, cancelled, notified = _mk(monkeypatch)
    asyncio.run(w._on_btc_price(100.0, 1))
    assert cancelled == ["btc_guard crash"]
    assert notified == ["BTC急跌保护，触发撤单"]


def test_notify_exit_skipped_when_entry_not_removed(monkeypatch):
    """真实 _notify_exit:条目仍在 _watched(撤单未确认/已成交)时不发通知。"""
    sent = []
    import shared.notifier as nt
    monkeypatch.setattr(nt, "notify", lambda text: sent.append(text))
    w = sw.SignalWatcher.__new__(sw.SignalWatcher)
    w._lock = asyncio.Lock()
    e = sw.WatchEntry(signal={"symbol": "XUSDT", "direction": "LONG", "mode": "pullback"},
                      entry_low=1, entry_high=2, entry_price=1.5, expire_ts=9e18, on_trigger=None)
    w._watched = {"XUSDT": e}
    asyncio.run(w._notify_exit("XUSDT", e, "BTC急跌保护，触发撤单"))
    assert sent == []
    w._watched.pop("XUSDT")
    asyncio.run(w._notify_exit("XUSDT", e, "BTC急跌保护，触发撤单"))
    assert len(sent) == 1 and "BTC急跌保护" in sent[0] and "退出监控" in sent[0]
