"""Bug: 加仓后 tp2_order_id 已更新，但 _handle_order_filled 拿到旧快照 → tp2_id 对不上 → tp2_filled 未标记。"""
import asyncio

import position_manager as pm


def _mk(monkeypatch, latest):
    calls = {"updates": [], "closed": [], "cancelled": []}
    monkeypatch.setattr(pm, "get_all_positions", lambda: [dict(latest)])
    monkeypatch.setattr(pm, "update_position", lambda pid, u: calls["updates"].append((pid, dict(u))) or True)
    monkeypatch.setattr(pm, "_get_precision", lambda s: {"tick": 0.0001, "step": 0.01})

    class _Api:
        def cancel_algo_order(self, symbol, oid):
            calls["cancelled"].append(oid)
            return {}

        def cancel_order(self, symbol, oid):
            calls["cancelled"].append(oid)
            return {}

    monkeypatch.setattr(pm, "api", _Api())

    async def _notify(t):
        return None

    monkeypatch.setattr(pm, "_notify_async", _notify)
    mgr = object.__new__(pm.PositionManager)

    async def _close(pid, symbol, reason, event):
        calls["closed"].append((pid, reason))

    mgr._close_position_post = _close
    return mgr, calls


def _pos(**kw):
    p = {"pos_id": "p-pyr", "symbol": "ABCUSDT", "direction": "long", "status": "open",
         "sl_order_id": 900, "tp1_order_id": 100, "tp2_order_id": 200, "tp2_quantity": 3.0,
         "sl_price": 95.0, "tp1_filled": False, "tp_mode": "dual"}
    p.update(kw)
    return p


def test_stale_snapshot_tp2_marked_after_pyramid(monkeypatch):
    stale = _pos()                                              # 加仓前的旧快照：tp2=200
    latest = _pos(tp1_order_id=101, tp2_order_id=201)           # 加仓后重挂：tp2=201
    mgr, calls = _mk(monkeypatch, latest)
    asyncio.run(mgr._handle_order_filled(stale, 201, {}))
    assert ("p-pyr", {"tp2_filled": True}) in calls["updates"]
    assert calls["closed"] == [("p-pyr", "tp2")]


def test_stale_snapshot_tp1_recognised_after_pyramid(monkeypatch):
    stale = _pos()
    latest = _pos(tp1_order_id=101, tp2_order_id=201, tp_mode="single", tp2_price=None)
    latest["tp2_order_id"] = None
    mgr, calls = _mk(monkeypatch, latest)
    asyncio.run(mgr._handle_order_filled(stale, 101, {}))
    assert calls["closed"] == [("p-pyr", "tp1")]


def test_old_tp2_id_no_longer_matches_after_refresh(monkeypatch):
    stale = _pos()
    latest = _pos(tp1_order_id=101, tp2_order_id=201)
    mgr, calls = _mk(monkeypatch, latest)
    asyncio.run(mgr._handle_order_filled(stale, 200, {}))       # 已被取消的旧 TP2 单号，不应被当成 TP2 成交
    assert calls["closed"] == [] and not any(u.get("tp2_filled") for _, u in calls["updates"])


def test_refresh_failure_falls_back_to_passed_pos(monkeypatch):
    stale = _pos()
    mgr, calls = _mk(monkeypatch, stale)

    def _boom():
        raise RuntimeError("state io")

    monkeypatch.setattr(pm, "get_all_positions", _boom)
    asyncio.run(mgr._handle_order_filled(stale, 200, {}))
    assert calls["closed"] == [("p-pyr", "tp2")]


def test_pos_missing_in_state_keeps_passed_pos(monkeypatch):
    stale = _pos()
    mgr, calls = _mk(monkeypatch, _pos(pos_id="other"))
    asyncio.run(mgr._handle_order_filled(stale, 200, {}))
    assert calls["closed"] == [("p-pyr", "tp2")]
