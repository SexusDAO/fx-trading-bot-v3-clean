"""sync_lite 清理本地记录：开仓冷却期内不删；非冷却期需连续 2 次查不到才删；查询失败不删不计数；又查到则清零。全部 mock。"""
import asyncio
import logging
import time

import pytest

import main as M

SYM = "OPUSDT"


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
    st = {"stored": [], "exch": [], "removed": [], "fail": False, "updates": []}

    def gp(sym=None):
        if st["fail"]:
            raise RuntimeError("timeout")
        return [dict(p) for p in st["exch"]]
    monkeypatch.setattr(B, "get_position", gp)
    monkeypatch.setattr(B, "get_open_orders", lambda sym=None: [])
    monkeypatch.setattr(B, "get_algo_open_orders", lambda sym=None: [])
    monkeypatch.setattr(S, "get_all_positions", lambda: [dict(p) for p in st["stored"]])
    monkeypatch.setattr(S, "add_position", lambda r: True)

    def upd(pid, u):
        st["updates"].append((pid, dict(u)))
        for p in st["stored"]:
            if p.get("pos_id") == pid:
                p.update(u)
        return True
    monkeypatch.setattr(S, "update_position", upd)

    def rm(pid, symbol=""):
        st["removed"].append(pid)
        st["stored"] = [p for p in st["stored"] if p.get("pos_id") != pid]
        return True
    monkeypatch.setattr(PM, "_remove_position_checked", rm)

    async def guard(*a, **k):
        return None
    monkeypatch.setattr(PM, "_补挂保护单", guard)
    monkeypatch.setattr(M, "_sync_lite_busy", False, raising=False)
    yield st
    logging.disable(logging.NOTSET)


def run():
    asyncio.run(M._startup_sync_lite())


def test_opened_6s_ago_exchange_misses_not_deleted_no_count(env):
    env["stored"] = [rec(6)]
    run()
    assert env["removed"] == [] and env["stored"][0]["status"] == "open"
    assert "_exchange_missing_count" not in env["stored"][0]          # 冷却期内连计数都不加
    run()
    run()
    assert env["removed"] == []                                         # 冷却期内无论多少次都不删


def test_fill_time_in_seconds_also_counts_as_cooldown(env):
    env["stored"] = [rec(3600, fill_time=int(time.time()) - 5)]       # open_time 很早，但 5 秒前刚成交
    run()
    assert env["removed"] == []


def test_two_consecutive_misses_then_delete(env):
    env["stored"] = [rec(3600)]
    run()
    assert env["removed"] == [] and env["stored"][0]["_exchange_missing_count"] == 1
    assert env["stored"][0]["status"] == "open"
    run()
    assert env["removed"] == ["p1"]


def test_query_failure_no_delete_no_count(env):
    env["stored"] = [rec(3600)]
    env["fail"] = True
    for _ in range(3):
        run()
    assert env["removed"] == [] and env["updates"] == []
    assert "_exchange_missing_count" not in env["stored"][0]


def test_failure_between_misses_does_not_advance_count(env):
    env["stored"] = [rec(3600)]
    run()                                   # 第1次查不到 → 计数1
    env["fail"] = True
    run()                                   # 失败 → 不计数
    assert env["removed"] == [] and env["stored"][0]["_exchange_missing_count"] == 1
    env["fail"] = False
    run()                                   # 第2次查不到 → 删
    assert env["removed"] == ["p1"]


def test_found_again_resets_count(env):
    env["stored"] = [rec(3600)]
    run()
    assert env["stored"][0]["_exchange_missing_count"] == 1
    env["exch"] = [{"symbol": SYM, "positionSide": "SHORT", "positionAmt": "-10", "entryPrice": "1.0",
                    "unRealizedProfit": "0", "leverage": "5"}]
    run()
    assert env["stored"][0]["_exchange_missing_count"] == 0
    env["exch"] = []
    run()                                   # 重新从 1 开始，不会立即删
    assert env["removed"] == [] and env["stored"][0]["_exchange_missing_count"] == 1


def test_missing_open_time_treated_as_not_in_cooldown(env):
    r = rec(0)
    r.pop("open_time")
    env["stored"] = [r]
    run()
    assert env["removed"] == [] and env["stored"][0]["_exchange_missing_count"] == 1
    run()
    assert env["removed"] == ["p1"]


def test_local_age_helper():
    now = 2_000_000_000_000
    assert M._local_age_ms({"open_time": now - 6000}, now) == 6000
    assert M._local_age_ms({"open_time": now - 10**7, "fill_time": (now - 5000) / 1000}, now) == pytest.approx(5000)
    assert M._local_age_ms({}, now) is None
