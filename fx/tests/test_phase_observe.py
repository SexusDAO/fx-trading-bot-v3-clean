import asyncio
import position_manager as pm


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


def _pos(d="short"):
    return {"symbol": "XUSDT", "direction": d, "orig_entry": 100.0, "orig_tp_dist": 10.0,
            "peak_price": 100.0, "phase": 1, "atr_at_entry": 1.0}


def test_short_phases(monkeypatch):
    # 峰值进度80%(价92) → 阶段2; 回撤到94(30%)仍2; 95(37.5%)→2; 96(50%)→3; 98.5(81%)→4
    calls = _run(_pos(), [99, 97, 92, 94, 96, 98.5], monkeypatch)
    phases = [c["phase"] for c in calls if "phase" in c]
    assert phases == [2, 3, 4]
    assert pm._phase_state["p1"]["peak"] == 92


def test_long_mirror(monkeypatch):
    calls = _run(_pos("long"), [101, 108, 104.5], monkeypatch)
    assert [c["phase"] for c in calls if "phase" in c] == [2, 3]


def test_no_write_when_small_move(monkeypatch):
    calls = _run(_pos(), [99.95, 99.9, 99.92], monkeypatch)
    assert calls == []


def test_peak_write_throttled(monkeypatch):
    calls = _run(_pos(), [99.0, 98.95, 98.9, 98.85, 98.0], monkeypatch)
    # 5个价格、峰值间隔>0.1ATR才写: 99.0 / 98.85 / 98.0 共3次, 98.95 与 98.9 被限频跳过
    assert [c['peak_price'] for c in calls] == [99.0, 98.85, 98.0]


def test_old_position_skipped(monkeypatch):
    p = _pos(); del p["orig_entry"]
    assert _run(p, [90], monkeypatch) == []
