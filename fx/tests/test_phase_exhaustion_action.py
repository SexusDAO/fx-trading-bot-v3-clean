import asyncio
import position_manager as pm


def _mk(monkeypatch, still_open=False):
    closed, upd = [], []
    async def fake_close(pos, reason):
        closed.append(reason)
    monkeypatch.setattr(pm, "_force_close_static", fake_close)
    monkeypatch.setattr(pm, "update_position", lambda pid, u: upd.append(dict(u)) or True)
    monkeypatch.setattr(pm, "_pos_still_open", lambda pos: still_open)
    m = pm.PositionManager.__new__(pm.PositionManager)
    return m, closed, upd


def _pos(mode="reversal", **kw):
    p = {"pos_id": "p1", "symbol": "XUSDT", "direction": "short", "mode": mode}
    p.update(kw)
    return p


def _act(m, pos, trans, fav=0.01):
    return asyncio.run(m._handle_phase_action(pos, "p1", trans, fav, 99.0))


def test_reversal_transition_to_4_closes(monkeypatch):
    m, closed, upd = _mk(monkeypatch)
    assert _act(m, _pos(), (3, 4)) is True
    assert closed == ["phase_exhaustion"]
    assert {"phase_action_done": "exhaustion"} in upd


def test_loss_does_not_close(monkeypatch):
    m, closed, _ = _mk(monkeypatch)
    assert _act(m, _pos(), (3, 4), fav=-0.002) is False
    assert closed == []


def test_other_modes_skipped(monkeypatch):
    for mode in ("pullback", "ema_trend", "breakout", "funding_arb", "range_bounce", "stop_hunt"):
        m, closed, _ = _mk(monkeypatch)
        assert _act(m, _pos(mode=mode), (3, 4)) is False
        assert closed == []


def test_only_on_transition(monkeypatch):
    m, closed, _ = _mk(monkeypatch)
    assert _act(m, _pos(), (4, 4)) is False       # 已在4,不再触发
    assert _act(m, _pos(), (2, 3)) is False       # 非4
    assert _act(m, _pos(), None) is False         # 无观测结果
    assert closed == []


def test_not_repeated_when_flag_set(monkeypatch):
    m, closed, _ = _mk(monkeypatch)
    assert _act(m, _pos(phase_action_done="exhaustion"), (3, 4)) is False
    assert closed == []


def test_close_failed_clears_flag(monkeypatch):
    m, closed, upd = _mk(monkeypatch, still_open=True)
    pos = _pos()
    assert _act(m, pos, (3, 4)) is False
    assert closed == ["phase_exhaustion"]
    assert {"phase_action_done": None} in upd
    assert "phase_action_done" not in pos


def test_observe_returns_transition(monkeypatch):
    monkeypatch.setattr(pm, "update_position", lambda pid, u: True)
    pm._phase_state.clear()
    m = pm.PositionManager.__new__(pm.PositionManager)
    pos = {"symbol": "X", "direction": "short", "orig_entry": 100.0, "orig_tp_dist": 10.0,
           "peak_price": 100.0, "phase": 1, "atr_at_entry": 1.0}
    async def go():
        return [await m._update_phase_observe(pos, "p1", "SHORT", p) for p in (92, 98.5)]
    r = asyncio.run(go())
    assert r[0] == (1, 2) and r[1] == (2, 4)
