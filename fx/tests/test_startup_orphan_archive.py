"""startup_sync：孤儿仓位无论有无保护单都先建档到 pos_state；有保护单只认领 id 不补挂，无保护单才补挂。全部 mock。"""
import asyncio
import logging

import pytest

import main as M


def algo(aid=111, trig="58.57"):
    return {"symbol": "LTCUSDT", "side": "BUY", "positionSide": "SHORT", "orderType": "STOP_MARKET",
            "triggerPrice": trig, "algoId": aid}


def lim(oid, price):
    return {"symbol": "LTCUSDT", "side": "BUY", "positionSide": "SHORT", "type": "LIMIT", "price": price,
            "orderId": oid, "origQty": "1.0"}


RP = {"symbol": "LTCUSDT", "positionSide": "SHORT", "positionAmt": "-2.0", "entryPrice": "56.79",
      "unRealizedProfit": "0.5", "leverage": "5"}


@pytest.fixture
def env(monkeypatch):
    import position_manager as PM
    import shared.state as S
    from shared import binance_api as B
    logging.disable(logging.CRITICAL)
    st = {"added": [], "guard": [], "algo": [], "orders": [], "stored": []}
    monkeypatch.setattr(B, "get_position", lambda sym=None: [dict(RP)])
    monkeypatch.setattr(B, "get_open_orders", lambda sym=None: list(st["orders"]))
    monkeypatch.setattr(B, "get_algo_open_orders", lambda sym=None: list(st["algo"]))

    def add(rec):
        st["added"].append(dict(rec))
        st["stored"].append(dict(rec))
        return True

    monkeypatch.setattr(S, "add_position", add)
    monkeypatch.setattr(S, "get_all_positions", lambda: list(st["stored"]))
    monkeypatch.setattr(S, "list_orphan_patching", lambda: [])
    monkeypatch.setattr(S, "cleanup_orphan_patching", lambda: [])

    async def guard(pos, entry, qty, pid, sym):
        st["guard"].append(sym)

    monkeypatch.setattr(PM, "_补挂保护单", guard)
    yield st
    logging.disable(logging.NOTSET)


def run():
    asyncio.run(M._sync_positions_on_startup())


def test_orphan_with_sl_and_tp_is_archived_and_not_repatched(env):
    env["algo"] = [algo()]
    env["orders"] = [lim(7, "53.23"), lim(8, "50.0")]
    run()
    (rec,) = env["added"]                                     # 无论有无保护单都建档
    assert rec["source"] == "orphan_recovered" and rec["status"] == "open" and rec["direction"] == "short"
    assert rec["sl_order_id"] == 111 and rec["sl_price"] == 58.57
    assert rec["tp1_order_id"] == 7 and rec["tp2_order_id"] == 8
    assert env["guard"] == []                                 # 有保护单：不补挂


def test_orphan_without_protection_is_archived_then_patched(env):
    run()
    (rec,) = env["added"]
    assert "sl_order_id" not in rec and "tp1_order_id" not in rec
    assert env["guard"] == ["LTCUSDT"]                        # 无保护单才补挂


def test_orphan_with_sl_only_is_archived_and_patched_for_tp(env):
    env["algo"] = [algo()]
    run()
    (rec,) = env["added"]
    assert rec["sl_order_id"] == 111 and "tp1_order_id" not in rec
    assert env["guard"] == ["LTCUSDT"]


def test_existing_record_not_rebuilt(env):
    env["stored"].append({"pos_id": "p1", "symbol": "LTCUSDT", "direction": "short", "status": "open",
                          "sl_order_id": 111, "tp1_order_id": 7})
    env["algo"] = [algo()]
    env["orders"] = [lim(7, "53.23")]
    run()
    assert env["added"] == [] and env["guard"] == []
