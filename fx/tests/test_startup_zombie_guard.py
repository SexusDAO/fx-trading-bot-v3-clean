"""startup_sync 清僵尸记录：与 sync_lite 共用判定。冷却期内不删；非冷却期需连续 2 次查不到（计数与 sync_lite 共享）；
查询失败整轮中止不计数；查到则清零。全部 mock。"""
import asyncio
import logging
import time

import pytest

import main as M

SYM = "OPUSDT"
RP = {"symbol": SYM, "positionSide": "SHORT", "positionAmt": "-10", "entryPrice": "1.0",
      "unRealizedProfit": "0", "leverage": "5"}


def rec(age_s, **kw):
    now = int(time.time() * 1000)
    d = {"pos_id": "p1", "symbol": SYM, "direction": "short", "status": "open", "mode": "ema_trend",
         "entry_price": 1.0, "quantity": 10.0, "open_time": now - int(age_s * 1000),
         "sl_price": 1.1, "sl_order_id": 1, "tp1_price": 0.9, "tp1_order_id": 2, "tp2_price": None}
    d.update(kw)
    return d


@pytest.fixture
def env(monkeypatch):
    import position_manager as PM
    import shared.state as S
    from shared import binance_api as B
    logging.disable(logging.CRITICAL)
    st = {"stored": [], "exch": [], "removed": [], "fail": False, "updates": [], "added": []}

    def gp(sym=None):
        if st["fail"]:
            raise RuntimeError("timeout")
        return [dict(p) for p in st["exch"]]
    monkeypatch.setattr(B, "get_position", gp)
    monkeypatch.setattr(B, "get_open_orders", lambda sym=None: [])
    monkeypatch.setattr(B, "get_algo_open_orders", lambda sym=None: [])
    monkeypatch.setattr(S, "get_all_positions", lambda: [dict(p) for p in st["stored"]])
    monkeypatch.setattr(S, "add_position", lambda r: (st["added"].append(dict(r)), True)[1])
    monkeypatch.setattr(S, "list_orphan_patching", lambda: [])
    monkeypatch.setattr(S, "cleanup_orphan_patching", lambda: [])

    def upd(pid, u):
        st["updates"].append((pid, dict(u)))
        for p in st["stored"]:
            if p.get("pos_id") == pid:
                p.update(u)
        return True
    monkeypatch.setattr(S, "update_position", upd)

    def rm(pid):
        st["removed"].append(pid)
        st["stored"] = [p for p in st["stored"] if p.get("pos_id") != pid]
        return True
    monkeypatch.setattr(S, "remove_position", rm)
    monkeypatch.setattr(PM, "_remove_position_checked", lambda pid, sym="": rm(pid))

    async def guard(*a, **k):
        return None
    monkeypatch.setattr(PM, "_补挂保护单", guard)
    monkeypatch.setattr(M, "_sync_lite_busy", False, raising=False)
    yield st
    logging.disable(logging.NOTSET)


def startup():
    asyncio.run(M._sync_positions_on_startup())


def lite():
    asyncio.run(M._startup_sync_lite())


def test_startup_opened_6s_ago_not_removed_no_count(env):
    env["stored"] = [rec(6)]
    startup()
    startup()
    assert env["removed"] == [] and env["stored"][0]["status"] == "open"
    assert "_exchange_missing_count" not in env["stored"][0]


def test_startup_fill_time_seconds_cooldown(env):
    env["stored"] = [rec(7200, fill_time=int(time.time()) - 10)]
    startup()
    assert env["removed"] == []


def test_startup_two_misses_then_remove(env):
    env["stored"] = [rec(7200)]
    startup()
    assert env["removed"] == [] and env["stored"][0]["_exchange_missing_count"] == 1
    startup()
    assert env["removed"] == ["p1"]


def test_startup_and_sync_lite_share_the_count(env):
    env["stored"] = [rec(7200)]
    startup()                                   # 启动第 1 次
    assert env["stored"][0]["_exchange_missing_count"] == 1
    lite()                                      # sync_lite 第 2 次 → 删
    assert env["removed"] == ["p1"]


def test_sync_lite_then_startup_also_shares(env):
    env["stored"] = [rec(7200)]
    lite()
    assert env["removed"] == []
    startup()
    assert env["removed"] == ["p1"]


def test_startup_query_failure_aborts_without_count(env):
    env["stored"] = [rec(7200)]
    env["fail"] = True
    startup()
    startup()
    assert env["removed"] == [] and env["updates"] == []
    assert "_exchange_missing_count" not in env["stored"][0]


def test_startup_found_resets_count(env):
    env["stored"] = [rec(7200, _exchange_missing_count=1)]
    env["exch"] = [dict(RP)]
    startup()
    assert env["removed"] == [] and env["stored"][0]["_exchange_missing_count"] == 0
    assert env["added"] == []                    # 有本地记录，不建孤儿


def test_startup_pending_guard_also_protected(env):
    env["stored"] = [rec(7200, status="pending_guard")]
    startup()
    assert env["removed"] == [] and env["stored"][0]["_exchange_missing_count"] == 1
