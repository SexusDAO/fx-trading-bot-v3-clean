"""加仓后 TP 重挂：_pyramid_replace_tp（按新均价/新总量撤旧 TP、重挂 TP1/TP2、失败告警）。"""
import asyncio

import pytest

import position_manager as pm


def _klines(n=60, base=100.0):
    now = 1_700_000_000_000
    out = []
    for i in range(n):
        c = base + (i % 5) * 0.1
        out.append([now + i * 3600_000, c, c + 0.5, c - 0.5, c, 100, now + i * 3600_000 + 3599_999])
    return out


class Env:
    def __init__(self, monkeypatch, direction="SHORT", params=None, place_fail=None, order_status=None):
        self.cancelled, self.placed, self.updates, self.notes, self.locks = [], [], [], [], []
        self.params = params or {"sl_price": 105.0, "tp1_price": 95.0, "tp2_price": 90.0}
        self.place_fail = place_fail or set()      # 价格集合：这些价位的挂单抛异常
        self.order_status = order_status or {}     # order_id -> status
        self.calc_inputs = []
        env = self

        class _Api:
            def get_klines(self, *a, **k):
                return [[t, o, h, l, c, v, ct - 10**15] for t, o, h, l, c, v, ct in _klines()]

            def get_mark_price(self, symbol):
                return {"markPrice": "100.0", "lastFundingRate": "0.0001"}

            def get_order(self, symbol, oid):
                return {"status": env.order_status.get(oid, "NEW")}

            def cancel_order(self, symbol, oid):
                env.cancelled.append(oid)
                return {}

            def place_order(self, **kw):
                if kw["price"] in env.place_fail:
                    raise RuntimeError("exchange rejected")
                env.placed.append(kw)
                return {"orderId": 9000 + len(env.placed)}

        monkeypatch.setattr(pm, "api", _Api())
        # 固定指标依赖：其他测试文件可能把这些名字桩掉，这里要求结果与加载顺序无关
        monkeypatch.setattr(pm, "_calc_atr_from_klines", lambda k: 1.0)
        monkeypatch.setattr(pm, "_calc_rsi", lambda c: 50.0)
        monkeypatch.setattr(pm, "_calc_ema", lambda c, n: 100.0)
        monkeypatch.setattr(pm, "_round_price", lambda p, t: round(float(p), 2))
        monkeypatch.setattr(pm, "_round_qty", lambda q, s: round(float(q), 1))

        async def _notify(text):
            env.notes.append(text)

        monkeypatch.setattr(pm, "_notify_async", _notify)
        monkeypatch.setattr(pm, "update_position", lambda pid, u: env.updates.append((pid, dict(u))) or True)
        monkeypatch.setattr(pm, "mark_system_order_update", lambda s: env.locks.append(s))

        import shared.calc_sl_tp as C
        import shared.liq_cache as L
        import executor as X

        def _calc(**kw):
            env.calc_inputs.append(kw["signal"])
            return dict(env.params)

        monkeypatch.setattr(C, "calc_sl_tp", _calc)
        monkeypatch.setattr(L, "get_liq_zones_cached", lambda *a, **k: [])
        monkeypatch.setattr(X, "_get_precision", lambda s: {"tick": 0.01, "step": 0.1})
        self.direction = direction

    def pos(self, **kw):
        p = {"symbol": "ABCUSDT", "pos_id": "p1", "direction": self.direction.lower(),
             "tp1_price": 96.0, "tp2_price": 92.0, "tp1_order_id": 111, "tp2_order_id": 222,
             "tp1_filled": False, "quantity_remaining": 10.0}
        p.update(kw)
        return p


def _run(pos, avg=101.0, rem=15.0):
    return asyncio.run(pm._pyramid_replace_tp(pos, avg, rem))


def test_replaces_tp1_tp2_with_new_avg_and_60_40_split(monkeypatch):
    e = Env(monkeypatch)
    assert _run(e.pos()) is True
    assert e.cancelled == [111, 222]
    assert [(o["price"], o["quantity"]) for o in e.placed] == [(95.0, 9.0), (90.0, 6.0)]
    assert all(o["order_type"] == "LIMIT" and o["side"] == "BUY" and o["positionSide"] == "SHORT" for o in e.placed)
    assert e.calc_inputs[0]["fill_price"] == 101.0           # 以新均价为基准重算
    pid, upd = e.updates[-1]
    assert pid == "p1"
    assert upd["tp1_price"] == 95.0 and upd["tp1_order_id"] == 9001 and upd["tp1_quantity"] == 9.0
    assert upd["tp2_price"] == 90.0 and upd["tp2_order_id"] == 9002 and upd["tp2_quantity"] == 6.0
    assert e.locks and any("止盈已重挂" in n and "95" in n and "90" in n for n in e.notes)


def test_long_direction_sides(monkeypatch):
    e = Env(monkeypatch, direction="LONG", params={"sl_price": 95.0, "tp1_price": 105.0, "tp2_price": 110.0})
    assert _run(e.pos()) is True
    assert all(o["side"] == "SELL" and o["positionSide"] == "LONG" for o in e.placed)


def test_calc_failure_keeps_old_orders_and_alerts(monkeypatch):
    e = Env(monkeypatch, params={"error": "boom"})
    assert _run(e.pos()) is False
    assert e.cancelled == [] and e.placed == []               # 先算后撤：失败时旧单不动
    assert any("TP 重挂未完成" in n for n in e.notes)


def test_tp_on_wrong_side_of_price_aborts_before_cancel(monkeypatch):
    e = Env(monkeypatch, params={"sl_price": 105.0, "tp1_price": 101.0, "tp2_price": 90.0})   # SHORT 的 TP1 高于现价 100
    assert _run(e.pos()) is False
    assert e.cancelled == [] and e.placed == []
    assert any("有利一侧" in n for n in e.notes)


def test_place_failure_alerts_and_does_not_record_failed_leg(monkeypatch):
    e = Env(monkeypatch, place_fail={95.0})
    assert _run(e.pos()) is True                              # TP2 仍成功
    assert any("TP1 重挂失败" in n and "无止盈保护" in n for n in e.notes)
    _, upd = e.updates[-1]
    assert "tp1_order_id" not in upd and upd["tp2_order_id"] == 9001


def test_all_place_failed_returns_false_with_alerts(monkeypatch):
    e = Env(monkeypatch, place_fail={95.0, 90.0})
    assert _run(e.pos()) is False
    assert sum("重挂失败" in n for n in e.notes) == 2
    assert e.updates == []


def test_filled_old_order_is_not_cancelled_or_replaced(monkeypatch):
    e = Env(monkeypatch, order_status={111: "FILLED"})
    assert _run(e.pos()) is True
    assert 111 not in e.cancelled and e.cancelled == [222]
    assert [o["price"] for o in e.placed] == [90.0]


def test_tp1_filled_puts_whole_remaining_on_tp2(monkeypatch):
    e = Env(monkeypatch)
    assert _run(e.pos(tp1_filled=True, tp1_order_id=None), rem=6.0) is True
    assert e.cancelled == [222]
    assert [(o["price"], o["quantity"]) for o in e.placed] == [(90.0, 6.0)]


def test_single_tp_mode_places_full_quantity(monkeypatch):
    e = Env(monkeypatch, params={"sl_price": 105.0, "tp1_price": 95.0, "tp2_price": None})
    assert _run(e.pos(tp2_price=None, tp2_order_id=None)) is True
    assert [(o["price"], o["quantity"]) for o in e.placed] == [(95.0, 15.0)]


def test_pyramid_calls_replace_after_sl_sync():
    import inspect
    src = inspect.getsource(pm._maybe_pyramid)
    assert src.index("_execute_trail_sl") < src.index("_pyramid_replace_tp")
    assert "TODO(待办-加仓后TP重挂)" not in inspect.getsource(pm)
    assert "TODO(待办-TP收紧条件B缺陷)" in inspect.getsource(pm)       # 条件B的 TODO 按要求保留
