import asyncio
import time
import position_manager as pm


def _pos(**kw):
    p = {"symbol": "XUSDT", "direction": "short", "orig_entry": 100.0, "orig_tp_dist": 10.0,
         "peak_price": 100.0, "phase": 1, "atr_at_entry": 1.0, "open_time": 1_000_000, "peak_ts": 1_000_000.0}
    p.update(kw)
    return p


def _run(pos, prices, monkeypatch):
    calls = []
    monkeypatch.setattr(pm, "update_position", lambda pid, u: calls.append(dict(u)) or True)
    pm._phase_state.clear()
    m = pm.PositionManager.__new__(pm.PositionManager)
    async def go():
        for p in prices:
            await m._update_phase_observe(pos, "p1", pos["direction"].upper(), p)
    asyncio.run(go())
    return calls


def test_peak_ts_updates_when_peak_refreshes(monkeypatch):
    t0 = time.time() * 1000
    calls = _run(_pos(), [92.0], monkeypatch)
    assert calls and calls[-1]["peak_price"] == 92.0
    assert calls[-1]["peak_ts"] >= t0 - 1


def test_peak_ts_unchanged_when_no_new_peak(monkeypatch):
    calls = _run(_pos(), [92.0, 94.0], monkeypatch)
    ts_after_peak = pm._phase_state["p1"]["peak_ts"]
    time.sleep(0.01)
    _run_calls = _run(_pos(peak_price=92.0, peak_ts=5_000.0), [95.0, 96.0], monkeypatch)
    assert pm._phase_state["p1"]["peak_ts"] == 5_000.0   # 未刷新峰值,时间不变
    assert ts_after_peak > 1_000_000.0


def test_old_position_without_peak_ts_falls_back_to_open_time(monkeypatch):
    p = _pos(); del p["peak_ts"]
    _run(p, [100.5], monkeypatch)   # 做空时 100.5 为不利方向,不刷新峰值
    assert pm._phase_state["p1"]["peak_ts"] == 1_000_000.0


def test_executor_writes_peak_ts_field():
    import inspect, executor
    assert '"peak_ts"' in inspect.getsource(executor._write_position)
