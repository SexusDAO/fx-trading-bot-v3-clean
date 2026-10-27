"""WS 重连触发 sync_lite：本地已有 open / recovered 的仓位不得再被建成孤儿；建档前复核过期快照。全部 mock。"""
import asyncio
import logging

import pytest

import main as M


def rp(sym, side):
    return {"symbol": sym, "positionSide": side, "positionAmt": "-2.0" if side == "SHORT" else "2.0",
            "entryPrice": "10", "unRealizedProfit": "0", "leverage": "5", "updateTime": 1}


def local(sym, direction, status="open", mode="ema_trend", pid=None):
    return {"pos_id": pid or f"p_{sym}", "symbol": sym, "direction": direction, "status": status, "mode": mode,
            "sig_type": mode, "entry_price": 10.0, "quantity": 2.0, "open_time": 1700000000000,
            "sl_price": 9.0, "sl_order_id": 1, "tp1_price": 12.0, "tp1_order_id": 2, "tp2_price": None}


def algo(sym, side, aid=1):
    return {"symbol": sym, "side": "SELL" if side == "LONG" else "BUY", "positionSide": side,
            "orderType": "STOP_MARKET", "triggerPrice": "9", "algoId": aid}


def tp(sym, side, oid=2):
    return {"symbol": sym, "side": "SELL" if side == "LONG" else "BUY", "positionSide": side,
            "type": "LIMIT", "price": "12", "orderId": oid}


@pytest.fixture
def env(monkeypatch):
    import position_manager as PM
    import shared.state as S
    from shared import binance_api as B
    logging.disable(logging.CRITICAL)
    st = {"added": [], "guard": [], "stored": [], "exch": [], "reads": [], "chk_fail": False}
    monkeypatch.setattr(B, "get_position", lambda sym=None: _gp(st, sym))
    monkeypatch.setattr(B, "get_open_orders", lambda sym=None: [tp("BIRBUSDT", "LONG", 2), tp("FETUSDT", "SHORT", 2)])
    monkeypatch.setattr(B, "get_algo_open_orders", lambda sym=None: [algo("BIRBUSDT", "LONG", 1), algo("FETUSDT", "SHORT", 1)])
    monkeypatch.setattr(S, "add_position", lambda r: (st["added"].append(dict(r)), st["stored"].append(dict(r)), True)[2])
    monkeypatch.setattr(S, "update_position", lambda pid, u: True)

    def gap():
        if st["reads"]:
            return [dict(p) for p in st["reads"].pop(0)]
        return [dict(p) for p in st["stored"]]
    monkeypatch.setattr(S, "get_all_positions", gap)

    async def guard(pos, entry, qty, pid, sym):
        st["guard"].append(sym)
    monkeypatch.setattr(PM, "_补挂保护单", guard)
    monkeypatch.setattr(M, "_sync_lite_busy", False, raising=False)
    yield st
    logging.disable(logging.NOTSET)


def _gp(st, sym):
    if sym and st["chk_fail"]:
        raise RuntimeError("net")
    return [dict(p) for p in st["exch"] if not sym or p["symbol"] == sym]


def run():
    asyncio.run(M._startup_sync_lite())


def test_ws_reconnect_open_and_recovered_not_reorphaned(env):
    env["exch"] = [rp("BIRBUSDT", "LONG"), rp("FETUSDT", "SHORT")]
    env["stored"] = [local("BIRBUSDT", "long", "open"),
                     local("FETUSDT", "short", "recovered", mode="recovered", pid="rec_FETUSDT_S_1")]
    before = [dict(p) for p in env["stored"]]
    run()
    assert env["added"] == []
    assert env["stored"] == before                       # 两条原样不变
    assert env["stored"][1]["pos_id"] == "rec_FETUSDT_S_1" and env["stored"][1]["status"] == "recovered"


def test_status_open_with_mode_recovered_not_reorphaned(env):
    env["exch"] = [rp("BIRBUSDT", "LONG")]
    env["stored"] = [local("BIRBUSDT", "LONG", "open", mode="recovered", pid="rec_BIRBUSDT_L_1")]
    run()
    assert env["added"] == []


def test_match_whitelist_contains_all_five():
    for st in ("open", "pending", "patching", "pending_guard", "recovered"):
        assert M._match_local_position([local("XUSDT", "long", st)], "XUSDT", "LONG") is not None
    assert M._match_local_position([local("XUSDT", "long", "closed")], "XUSDT", "LONG") is None


def test_stale_snapshot_recheck_finds_local_record(env):
    """第一次读 pos_state 时没有记录（过期快照），建档前复核重读时已有 → 不建孤儿。"""
    env["exch"] = [rp("BIRBUSDT", "LONG")]
    env["reads"] = [[], [local("BIRBUSDT", "long", "open")]]
    run()
    assert env["added"] == []


def test_recheck_exchange_position_gone_skips(env):
    """快照里有仓位、复核时交易所已无 → 不建孤儿，也不触发超限平仓。"""
    env["exch"] = [rp("BIRBUSDT", "LONG")]
    calls = {"n": 0}
    from shared import binance_api as B

    def gp(sym=None):
        calls["n"] += 1
        return [dict(p) for p in env["exch"]] if sym is None else []
    B.get_position = gp
    run()
    assert env["added"] == []


def test_recheck_query_failure_skips(env):
    env["exch"] = [rp("BIRBUSDT", "LONG")]
    env["chk_fail"] = True
    run()
    assert env["added"] == []


def test_truly_unmatched_still_builds_orphan(env):
    env["exch"] = [rp("BIRBUSDT", "LONG")]
    run()
    assert len(env["added"]) == 1 and env["added"][0]["source"] == "orphan_recovered_sync_lite"
