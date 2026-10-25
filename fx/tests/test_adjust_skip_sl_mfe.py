"""TRAIL_MFE_ANCHOR=True 时 _adjust_sl_tp_once 不更新 SL，TP1 照常。"""
import asyncio

import config
import position_manager as pm
from tests.test_adjust_tp_budget_rr import Env, _tp_orders


def _run(env):
    return asyncio.run(pm._adjust_sl_tp_once(env.pos(), force=True))


def test_sl_tighten_applied_when_anchor_off(monkeypatch):
    monkeypatch.setattr(config, "TRAIL_MFE_ANCHOR", False, raising=False)
    e = Env(monkeypatch, mark=98.0, new_tp1=90.0, new_sl=101.0, old_sl=105.0, old_tp1=90.0)
    res = _run(e)
    assert res and res["sl_changed"] is True


def test_sl_untouched_when_anchor_on(monkeypatch):
    monkeypatch.setattr(config, "TRAIL_MFE_ANCHOR", True, raising=False)
    e = Env(monkeypatch, mark=98.0, new_tp1=90.0, new_sl=101.0, old_sl=105.0, old_tp1=90.0)
    res = _run(e)
    assert res is None or res["sl_changed"] is False
    assert not any("sl" in o for o in e.placed)


def test_tp1_still_updates_when_anchor_on(monkeypatch):
    monkeypatch.setattr(config, "TRAIL_MFE_ANCHOR", True, raising=False)
    # 旧 SL=100.5(离现价4.5)、新 TP1=88(离现价8)→预算RR≈1.78，不会触发RR不足平仓；清算区新SL=100.0 本应收紧但被开关屏蔽
    e = Env(monkeypatch, mark=96.0, new_tp1=88.0, new_sl=100.0, old_sl=100.5, old_tp1=90.0)
    res = _run(e)
    assert res and res["tp1_changed"] is True and res["sl_changed"] is False
    assert [o["price"] for o in _tp_orders(e)] == [88.0]
