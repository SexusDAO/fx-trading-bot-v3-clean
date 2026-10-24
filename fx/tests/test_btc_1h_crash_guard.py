import asyncio
import shared.signal_watcher as sw


def _k(prev, cur):
    return [[0, 0, 0, 0, 100.0], [0, 0, 0, 0, prev], [0, 0, 0, 0, cur]]


def test_drop_formula():
    assert abs(sw._btc_1h_drop_pct(_k(100.0, 94.0)) - (-6.0)) < 1e-9
    assert sw._btc_1h_drop_pct([]) is None
    assert sw._btc_1h_drop_pct([[0, 0, 0, 0, 1.0]]) is None
    assert sw._btc_1h_drop_pct(_k(0.0, 1.0)) is None


def _mk(monkeypatch, klines, entries):
    w = sw.SignalWatcher.__new__(sw.SignalWatcher)
    w._lock = asyncio.Lock()
    w._watched = dict(entries)
    monkeypatch.setattr(sw, "_get_hub_or_rest", lambda *a, **k: klines)
    cancelled, notified = [], []
    async def fc(self, sym, e, reason):
        cancelled.append((sym, reason)); self._watched.pop(sym, None)
    async def fn(self, sym, e, reason):
        notified.append((sym, reason))
    monkeypatch.setattr(sw.SignalWatcher, "_cancel_and_remove", fc)
    monkeypatch.setattr(sw.SignalWatcher, "_notify_exit", fn)
    return w, cancelled, notified


def _e(direction="LONG", **kw):
    e = sw.WatchEntry(signal={"symbol": "X", "direction": direction, "mode": "pullback"},
                      entry_low=1, entry_high=2, entry_price=1.5, expire_ts=9e18, on_trigger=None)
    for k, v in kw.items():
        setattr(e, k, v)
    return e


def _run(w):
    async def go():
        return await w._btc_1h_crash_guard(asyncio.get_running_loop())
    return asyncio.run(go())


def test_crash_cancels_only_eligible_longs(monkeypatch):
    ents = {"A": _e(), "B": _e("SHORT"), "C": _e(triggered=True), "D": _e(is_reorder=True),
            "E": _e(cancel_state="cancelling"), "F": _e()}
    w, cancelled, notified = _mk(monkeypatch, _k(100.0, 94.0), ents)
    assert _run(w) == 2
    assert sorted(s for s, _ in cancelled) == ["A", "F"]
    assert sorted(s for s, _ in notified) == ["A", "F"]
    assert notified[0][1] == "BTC 1H急跌 -6.0%，撤销LONG监控"


def test_no_action_above_threshold_or_no_data(monkeypatch):
    w, cancelled, notified = _mk(monkeypatch, _k(100.0, 95.5), {"A": _e()})   # -4.5%
    assert _run(w) == 0 and cancelled == [] and notified == []
    w, cancelled, _ = _mk(monkeypatch, [], {"A": _e()})
    assert _run(w) == 0 and cancelled == []


def test_exactly_minus_5_triggers(monkeypatch):
    w, cancelled, _ = _mk(monkeypatch, _k(100.0, 95.0), {"A": _e()})
    assert _run(w) == 1 and len(cancelled) == 1


def test_wired_before_snapshot():
    import inspect
    src = inspect.getsource(sw.SignalWatcher._recheck_pass)
    assert src.index("_btc_1h_crash_guard") < src.index("entries = [(sym, e)")
