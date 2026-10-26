"""孤儿仓位建档读取 SL/TP + _补挂保护单自愈（sl=0 卡 pending_guard 的修复）。全部 mock，不联网。"""
import asyncio
import logging

import pytest

from shared import guard_match as G

try:
    import main as M
except SystemExit:
    M = None
import position_manager as PM


def algo(sym="LTCUSDT", side="BUY", ps="SHORT", trig="58.57", aid=111, typ="STOP_MARKET"):
    return {"symbol": sym, "side": side, "positionSide": ps, "orderType": typ, "triggerPrice": trig, "algoId": aid}


def lim(oid, price, side="BUY", ps="SHORT", sym="LTCUSDT", qty="1.0"):
    return {"symbol": sym, "side": side, "positionSide": ps, "type": "LIMIT", "price": price, "orderId": oid, "origQty": qty}


RP = {"entryPrice": "56.79", "positionAmt": "-2.0", "leverage": "5"}


# ── guard_match 纯函数 ──────────────────────────────────────────────────────
def test_find_sl_exactly_one():
    hits = G.find_exchange_sl([algo(), algo(sym="ETHUSDT", aid=2), algo(ps="LONG", side="SELL", aid=3)], "LTCUSDT", "SHORT")
    assert len(hits) == 1 and G.sl_fields(hits[0]) == {"sl_price": 58.57, "sl_order_id": 111}


def test_find_sl_zero_and_multiple_and_wrong_type():
    assert G.find_exchange_sl([], "LTCUSDT", "SHORT") == []
    assert G.find_exchange_sl([algo(typ="TAKE_PROFIT_MARKET")], "LTCUSDT", "SHORT") == []
    assert G.find_exchange_sl([algo(trig="0")], "LTCUSDT", "SHORT") == []
    assert len(G.find_exchange_sl([algo(aid=1), algo(aid=2, trig="59")], "LTCUSDT", "SHORT")) == 2


def test_find_tps_sorted_by_direction():
    shorts = G.find_exchange_tps([lim(1, "50"), lim(2, "53.2"), lim(3, "60", side="SELL")], "LTCUSDT", "SHORT")
    assert [o["orderId"] for o in shorts] == [2, 1]          # SHORT：价格降序，离入场近的是 TP1
    longs = G.find_exchange_tps([lim(1, "60", side="SELL", ps="LONG"), lim(2, "58", side="SELL", ps="LONG")], "LTCUSDT", "LONG")
    assert [o["orderId"] for o in longs] == [2, 1]           # LONG：价格升序


def test_tp_fields_single_and_dual():
    one = G.tp_fields([lim(9, "53.2")])
    assert one["tp1_order_id"] == 9 and one["tp1_price"] == 53.2 and "tp2_order_id" not in one
    two = G.tp_fields([lim(9, "53.2"), lim(8, "50")])
    assert two["tp2_order_id"] == 8 and two["tp2_price"] == 50.0


# ── 孤儿建档（main._build_orphan_record）────────────────────────────────────
@pytest.mark.skipif(M is None, reason="main 依赖缺失")
def test_orphan_record_reads_sl_and_tp():
    ps, notes = M._build_orphan_record("LTCUSDT", "SHORT", RP, [algo()], [lim(7, "53.23"), lim(8, "50")], 1000)
    assert ps["sl_price"] == 58.57 and ps["sl_order_id"] == 111
    assert ps["tp1_order_id"] == 7 and ps["tp1_price"] == 53.23 and ps["tp2_order_id"] == 8
    assert ps["mode"] == "recovered" and ps["direction"] == "short" and ps["quantity"] == 2.0
    assert any("SL@" in n for n in notes)


@pytest.mark.skipif(M is None, reason="main 依赖缺失")
def test_orphan_record_no_sl_leaves_fields_absent():
    ps, notes = M._build_orphan_record("LTCUSDT", "SHORT", RP, [], [], 1000)
    assert "sl_price" not in ps and "sl_order_id" not in ps and "tp1_order_id" not in ps and notes == []


@pytest.mark.skipif(M is None, reason="main 依赖缺失")
def test_orphan_record_multiple_sl_not_claimed():
    ps, notes = M._build_orphan_record("LTCUSDT", "SHORT", RP, [algo(aid=1), algo(aid=2, trig="59")], [], 1000)
    assert "sl_price" not in ps and any("歧义" in n for n in notes)


# ── _补挂保护单 自愈 ─────────────────────────────────────────────────────────
@pytest.fixture
def env(monkeypatch):
    import executor as E
    import shared.state as S
    from shared import binance_api as B
    logging.disable(logging.CRITICAL)
    st = {"updates": [], "guard_calls": [], "sl_placed": [], "alerts": [], "algo": [], "open_orders": []}

    async def run_patrol(fn, *a, **k):
        return fn()

    monkeypatch.setattr(PM, "_run_patrol", run_patrol)
    monkeypatch.setattr(PM, "_guard_locks", __import__("collections").defaultdict(asyncio.Lock))
    monkeypatch.setattr(B, "get_position", lambda sym=None: [{"positionSide": "SHORT", "positionAmt": "-2.0"}])
    monkeypatch.setattr(B, "get_algo_open_orders", lambda sym=None: list(st["algo"]))
    monkeypatch.setattr(B, "get_open_orders", lambda sym=None: list(st["open_orders"]))
    monkeypatch.setattr(B, "cancel_algo_order", lambda *a, **k: {})
    monkeypatch.setattr(B, "get_klines", lambda *a, **k: [[0, "1", "2", "0.5", "1.5", "1", 1, "1"] for _ in range(22)])
    monkeypatch.setattr(B, "place_sl", lambda **kw: st["sl_placed"].append(kw) or {"algoId": 999, "orderId": 999})
    monkeypatch.setattr(S, "update_position", lambda pid, u: st["updates"].append(u) or True)

    async def fake_guards(*a, **kw):
        st["guard_calls"].append((a, kw))
        return {"sl_id": None if kw.get("skip_sl") else 555, "tp1_id": 601, "tp2_id": None}

    monkeypatch.setattr(E, "_place_guards_async", fake_guards)
    monkeypatch.setattr(E, "_get_precision", lambda s: {"step": 0.01, "tick": 0.01})

    async def alert(pos, text):
        st["alerts"].append(text)

    monkeypatch.setattr(PM, "_alert_card_or_notify", alert)
    yield st
    logging.disable(logging.NOTSET)


def orphan_pos(**kw):
    p = {"pos_id": "rec_LTCUSDT_S_1", "symbol": "LTCUSDT", "direction": "short", "status": "open", "mode": "recovered",
         "fill_price": 56.79, "quantity": 2.0}
    p.update(kw)
    return p


def run_guard(pos):
    return asyncio.run(PM._补挂保护单(pos, 56.79, 2.0, pos["pos_id"], pos["symbol"]))


def test_heal_claims_existing_sl_with_existing_tp(env):
    env["algo"] = [algo()]
    env["open_orders"] = [lim(7, "53.23")]
    run_guard(orphan_pos())
    u = env["updates"][-1]
    assert u["status"] == "open" and u["sl_price"] == 58.57 and u["sl_order_id"] == 111 and u["missing_guards"] == []
    assert env["sl_placed"] == [] and env["guard_calls"] == []          # 没有重复挂 SL，也没走挂单


def test_heal_claims_sl_without_tp_skips_duplicate_sl_and_keeps_exchange_price(env):
    env["algo"] = [algo()]
    run_guard(orphan_pos())
    (a, kw), = env["guard_calls"]
    assert kw["skip_sl"] is True                                        # 交易所已有 SL → 不重复挂
    u = env["updates"][-1]
    assert u["sl_price"] == 58.57 and u["sl_order_id"] == 111           # ATR 降级不覆盖交易所止损价
    assert u["tp1_order_id"] == 601 and u["status"] == "open"


def test_multiple_sl_keeps_pending_guard_and_alerts(env):
    env["algo"] = [algo(aid=1), algo(aid=2, trig="59")]
    env["open_orders"] = [lim(7, "53.23")]
    run_guard(orphan_pos())
    u = env["updates"][-1]
    assert u["status"] == "pending_guard" and u["missing_guards"] == ["sl"]
    assert "sl_order_id" not in u and env["sl_placed"] == []            # 歧义：不认领、不挂
    assert env["alerts"] and "无法自动认领" in env["alerts"][0]


def test_no_sl_on_exchange_still_places_sl_as_before(env):
    run_guard(orphan_pos(sl_price=58.0, tp1_price=53.0))
    (a, kw), = env["guard_calls"]
    assert kw["skip_sl"] is False                                       # 交易所确实没有 SL → 仍按原逻辑挂


def test_existing_matching_sl_id_unchanged(env):
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(7, "53.23")]
    run_guard(orphan_pos(sl_order_id=111, sl_price=58.57))
    u = env["updates"][-1]
    assert u["status"] == "open" and env["sl_placed"] == []


def test_algo_query_failure_does_not_heal_or_place(env, monkeypatch):
    from shared import binance_api as B

    def boom(sym=None):
        raise RuntimeError("net")

    monkeypatch.setattr(B, "get_algo_open_orders", boom)
    run_guard(orphan_pos(sl_price=58.0, tp1_price=53.0))
    (a, kw), = env["guard_calls"]
    assert kw["skip_sl"] is True                                        # 状态未知：沿用原逻辑，不重复挂
