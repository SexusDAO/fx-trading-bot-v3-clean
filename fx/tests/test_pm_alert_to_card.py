import asyncio, inspect
import position_manager as pm
import executor as X


def _setup(monkeypatch, open_ids):
    calls = []
    async def _app(sid, line): calls.append(("card", sid, line)); return True
    async def _nt(text): calls.append(("notify", text))
    monkeypatch.setattr(pm, "card_append_async", _app)
    monkeypatch.setattr(pm, "_notify_async", _nt)
    monkeypatch.setattr(pm, "card_is_open", lambda s: s in open_ids)
    return calls


def test_card_open_appends_to_card(monkeypatch):
    calls = _setup(monkeypatch, {"ABC_1"})
    asyncio.run(pm._alert_card_or_notify({"_sig_id": "ABC_1"}, "⚠️ x"))
    assert calls == [("card", "ABC_1", "\n⚠️ x")]


def test_card_closed_or_no_sig_id_falls_back_to_notify(monkeypatch):
    calls = _setup(monkeypatch, set())
    asyncio.run(pm._alert_card_or_notify({"_sig_id": "ABC_1"}, "a"))
    asyncio.run(pm._alert_card_or_notify({}, "b"))
    asyncio.run(pm._alert_card_or_notify({"_sig_id": ""}, "c"))
    assert calls == [("notify", "a"), ("notify", "b"), ("notify", "c")]


def test_card_append_error_falls_back(monkeypatch):
    calls = _setup(monkeypatch, {"ABC_1"})
    async def _boom(sid, line): raise RuntimeError("x")
    monkeypatch.setattr(pm, "card_append_async", _boom)
    asyncio.run(pm._alert_card_or_notify({"_sig_id": "ABC_1"}, "z"))
    assert calls == [("notify", "z")]


def test_five_sites_use_card_helper():
    assert "_alert_card_or_notify(pos, f\"⚠️ {symbol} 移动止损挂单失败" in inspect.getsource(pm._execute_trail_sl_inner)
    assert "_alert_card_or_notify" in inspect.getsource(pm._maybe_pyramid)
    src = inspect.getsource(pm._pyramid_replace_tp)
    assert src.count("_alert_card_or_notify") == 2          # 单项重挂失败 + 整体未完成（成功重挂提示不在范围内，保持独立消息）
    assert "_alert_card_or_notify" in inspect.getsource(pm._补挂保护单)
    assert 'sig_id=pos.get("_sig_id", "")' in inspect.getsource(pm._补挂保护单)
    assert 'sig_id=signal.get("_sig_id", "")' in inspect.getsource(X._execute_async_locked)


def test_guard_alert_executor(monkeypatch):
    calls = []
    async def _app(sid, line): calls.append(("card", sid)); return True
    async def _nt(t): calls.append(("notify",))
    monkeypatch.setattr(X, "card_append_async", _app)
    monkeypatch.setattr(X, "notify_async", _nt)
    import shared.notifier as N
    monkeypatch.setattr(N, "card_is_open", lambda s: s == "S1")
    asyncio.run(X._guard_alert("S1", "t")); asyncio.run(X._guard_alert("S2", "t")); asyncio.run(X._guard_alert("", "t"))
    assert calls == [("card", "S1"), ("notify",), ("notify",)]


def test_trail_fail_alert_throttled(monkeypatch):
    calls = _setup(monkeypatch, {"S1"})
    pm._trail_fail_alert_ts.clear()
    class _Api:
        def place_sl(self, **k): raise RuntimeError("boom")
    monkeypatch.setattr(pm, "api", _Api())
    monkeypatch.setattr(pm, "_get_precision", lambda s: {"tick": 0.01, "step": 0.1})
    monkeypatch.setattr(pm, "update_position", lambda *a, **k: True)
    pos = {"pos_id": "p1", "symbol": "ABCUSDT", "direction": "long", "_sig_id": "S1", "quantity_remaining": 1.0}
    for _ in range(3):
        assert asyncio.run(pm._execute_trail_sl_inner(pos, 99.0, "p1", 1, 1.0)) is False
    assert len([c for c in calls if c[0] == "card"]) == 1
