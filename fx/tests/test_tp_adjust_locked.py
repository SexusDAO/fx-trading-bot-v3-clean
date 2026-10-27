"""tp_adjust_locked=True：TP1/TP2 不被修改，SL 仍正常收紧；单止盈模式不补挂 TP2。"""
import asyncio

import position_manager as pm
from tests.test_adjust_tp_budget_rr import Env, _tp_orders


def _run_pos(env, **extra):
    p = env.pos()
    p.update(extra)
    return asyncio.run(pm._adjust_sl_tp_once(p, force=True))


def test_unlocked_baseline_changes_tp1_and_sl(monkeypatch):
    # 做空：新 TP1=88 比旧 90 更有利（improve，变动 2.2%）；新 SL=100.5 比旧 105 收紧
    e = Env(monkeypatch, mark=98.0, new_tp1=88.0, new_sl=100.5, old_sl=105.0, old_tp1=90.0)
    res = _run_pos(e)
    assert res and res["tp1_changed"] is True and res["sl_changed"] is True
    assert [o["price"] for o in _tp_orders(e)] == [88.0]


def test_locked_tp_untouched_but_sl_still_tightens(monkeypatch):
    e = Env(monkeypatch, mark=98.0, new_tp1=88.0, new_sl=100.5, old_sl=105.0, old_tp1=90.0)
    res = _run_pos(e, tp_adjust_locked=True)
    assert res and res["sl_changed"] is True          # SL 收紧不受影响
    assert res["tp1_changed"] is False and res["tp2_changed"] is False
    assert _tp_orders(e) == []                        # 没有任何 TP 撤单重挂
    assert {"sl": 100.5} in e.placed                  # 新 SL 已挂
    assert not any("tp1_price" in u or "tp2_price" in u for u in e.updates)   # pos_state 不写 TP 价格


def test_locked_blocks_shrink_and_cond_b_too(monkeypatch):
    # 条件 B（TP 距现价 ≥3ATR 且新 TP 更近，变动 1.05%）在锁定时同样不动
    e = Env(monkeypatch, mark=99.0, new_tp1=96.0, new_sl=100.5, old_sl=100.5, old_tp1=95.0)
    res = _run_pos(e, tp_adjust_locked=True)
    assert res is None and _tp_orders(e) == []


def test_tighten_tp2_returns_early_when_locked(monkeypatch):
    class Boom:
        def __getattr__(self, n):
            raise AssertionError("locked 时不应访问交易所")
    monkeypatch.setattr(pm, "api", Boom())
    pos = {"pos_id": "p", "symbol": "XUSDT", "direction": "long", "entry_price": 100.0, "tp2_price": 110.0,
           "open_time": 1, "tp_adjust_locked": True}
    assert asyncio.run(pm._maybe_tighten_tp2(pos)) is None


def test_single_mode_source_does_not_replant_tp2():
    import inspect
    src = inspect.getsource(pm._补挂保护单)
    assert 'pos.get("tp_mode") == "single" and not tp2_price' in src
    assert "tp2_price = None" in src
