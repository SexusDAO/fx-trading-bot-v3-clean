"""WS 就绪前禁止开仓 + WS 重连回调 + 轻量对齐（全部 mock，不联网）。"""
import asyncio
import logging
from types import SimpleNamespace

import pytest

import executor as E
import main as M
from shared import market_data_hub as H


# ── 改动一：预检 WS 就绪检查 ──
def test_precheck_blocked_when_hub_not_ready(monkeypatch):
    monkeypatch.setattr(H, "is_ready", lambda: False)
    ok, msg = E._precheck({"symbol": "BTCUSDT", "direction": "LONG"})
    assert ok is False and "WS行情未就绪" in msg


def test_precheck_passes_when_hub_ready(monkeypatch):
    monkeypatch.setattr(H, "is_ready", lambda: True)
    monkeypatch.setattr(E, "get_all_positions", lambda: [])
    monkeypatch.setattr("shared.binance_api.get_open_orders", lambda sym: [])
    monkeypatch.setattr(E, "check_position_limits", lambda d, c=None: True)
    monkeypatch.setattr(E, "is_circuit_open", lambda: True)      # 在此截断，只为证明 WS 检查已放行
    ok, msg = E._precheck({"symbol": "BTCUSDT", "direction": "LONG"})
    assert msg == "熔断中"


def test_precheck_ws_check_is_first():
    import inspect
    src = inspect.getsource(E._precheck)
    assert src.index("_hub_ready") < src.index("get_all_positions()")


def test_precheck_skips_when_hub_not_importable(monkeypatch):
    """hub 未启用/导入失败时不阻塞开仓（get_all_positions 仍会被调用）。"""
    import builtins
    called = {"n": 0}
    _real_import = builtins.__import__

    def _imp(name, *a, **kw):
        if "market_data_hub" in name:
            raise ImportError("hub disabled")
        return _real_import(name, *a, **kw)
    monkeypatch.setattr(builtins, "__import__", _imp)
    monkeypatch.setattr(E, "get_all_positions", lambda: called.__setitem__("n", 1) or [])
    monkeypatch.setattr("shared.binance_api.get_open_orders", lambda sym: [])
    monkeypatch.setattr(E, "check_position_limits", lambda d, c=None: True)
    monkeypatch.setattr(E, "is_circuit_open", lambda: True)
    ok, msg = E._precheck({"symbol": "BTCUSDT", "direction": "LONG"})
    assert called["n"] == 1 and msg == "熔断中"


# ── 改动二：hub 重连回调 ──
def _hub():
    return H.MarketDataHub(url="ws://x/market", market_url="ws://x/market2", kline_fetcher=lambda *a: [])


def test_reconnect_callback_fires_once_per_recovery():
    hub = _hub()
    hits = []
    hub.set_reconnect_callback(lambda: hits.append(1))
    frame = [{"e": "markPriceUpdate", "s": "BTCUSDT", "p": "1"}]
    hub._handle_frame(frame)                     # 首次就绪：不算「重连恢复」，不触发
    assert hits == []
    hub._on_disconnect()
    assert hub.is_ready() is False
    hub._handle_frame(frame)                     # 断线后恢复 → 触发
    hub._handle_frame(frame)                     # 之后每帧不重复触发
    assert len(hits) == 1


def test_reconnect_callback_exception_swallowed():
    hub = _hub()
    hub.set_reconnect_callback(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    hub._handle_frame([{"e": "markPriceUpdate", "s": "BTCUSDT", "p": "1"}])
    hub._on_disconnect()
    hub._handle_frame([{"e": "markPriceUpdate", "s": "BTCUSDT", "p": "1"}])   # 不抛
    assert hub.is_ready() is True


def test_no_callback_registered_is_noop():
    hub = _hub()
    hub._handle_frame([{"e": "markPriceUpdate", "s": "BTCUSDT", "p": "1"}])
    hud = hub._on_reconnect
    assert hud is None


# ── 改动三：_startup_sync_lite ──
@pytest.fixture(autouse=True)
def _restore_shared_state():
    """其他测试文件会把 shared.state 换成 stub 并留在 sys.modules 里；
    这里在本文件每个用例前恢复真实的 shared.state，避免 import 到残桩。"""
    import importlib, sys
    saved = sys.modules.get("shared.state")
    try:
        real = importlib.reload(importlib.import_module("shared.state"))
        sys.modules["shared.state"] = real
    except Exception:
        real = saved
    yield
    if saved is not None:
        sys.modules["shared.state"] = saved


def _rp(sym, side, amt="1", update=0):
    return {"symbol": sym, "positionSide": side, "positionAmt": amt,
            "entryPrice": "100", "markPrice": "100", "unRealizedProfit": "0",
            "leverage": "5", "updateTime": update}


@pytest.fixture
def lite(monkeypatch):
    st = {"positions": [], "added": [], "removed": [], "closed": [], "补挂": []}
    bapi = SimpleNamespace(
        get_position=lambda symbol=None: st["exchange"],
        get_open_orders=lambda symbol=None: [],
        get_algo_open_orders=lambda symbol=None: [],
        place_order=lambda *a, **kw: st["closed"].append((a, kw)),
    )
    import shared.binance_api as _BA
    # 注意：必须 patch 模块属性，不能 patch 字符串路径——全量跑时别的测试可能已替换 sys.modules 条目，
    # 字符串形式会打到另一个对象上，导致本文件单独跑通过、全量跑失败。
    monkeypatch.setattr(_BA, "get_position", bapi.get_position)
    monkeypatch.setattr(_BA, "get_open_orders", bapi.get_open_orders)
    monkeypatch.setattr(_BA, "get_algo_open_orders", bapi.get_algo_open_orders)
    monkeypatch.setattr(_BA, "place_order", bapi.place_order)
    # 业务代码里是 `from shared.state import ...`（函数内部导入），必须打到 sys.modules 里的真实模块上，
    # 否则全量跑时（其他测试留下过 stub）patched 的对象和被 import 的不是同一个。
    import sys
    import shared.state as _ST
    sys.modules["shared.state"] = _ST
    monkeypatch.setattr(_ST, "get_all_positions", lambda: list(st["positions"]))
    monkeypatch.setattr(_ST, "add_position", lambda r: st["added"].append(r) or True)
    monkeypatch.setattr(_ST, "update_position", lambda pid, d: None)
    import shared.risk_manager as RM
    monkeypatch.setattr(RM, "get_market_mode", lambda: "range")
    monkeypatch.setattr(RM, "_get_position_limits", lambda mode: {"total": 2, "long": 1, "short": 1})
    import position_manager as PM
    monkeypatch.setattr(PM, "_remove_position_checked", lambda pid, sym="": st["removed"].append(pid) or True)

    async def fake_guard(ps, entry, qty, pid, sym):
        st["补挂"].append(sym)
    monkeypatch.setattr(PM, "_补挂保护单", fake_guard)
    monkeypatch.setattr(M, "_sync_lite_busy", False, raising=False)
    st["exchange"] = []
    return st


def test_lite_adds_orphan_and_guards(lite):
    lite["exchange"] = [_rp("AAAUSDT", "LONG")]
    asyncio.run(M._startup_sync_lite())
    assert [r["symbol"] for r in lite["added"]] == ["AAAUSDT"]
    assert lite["added"][0]["direction"] == "long"
    assert lite["补挂"] == ["AAAUSDT"]


def test_lite_cleans_local_missing_on_exchange(lite):
    # 新规则：非冷却期、连续 2 次查不到才清理。这里模拟「上一轮已记 1 次查不到」，本轮是第 2 次。
    lite["positions"] = [{"symbol": "BBBUSDT", "direction": "SHORT", "status": "open", "pos_id": "p1",
                          "open_time": 1700000000000, "_exchange_missing_count": 1}]
    asyncio.run(M._startup_sync_lite())
    assert lite["removed"] == ["p1"]


def test_lite_first_miss_does_not_clean(lite):
    lite["positions"] = [{"symbol": "BBBUSDT", "direction": "SHORT", "status": "open", "pos_id": "p1",
                          "open_time": 1700000000000}]
    asyncio.run(M._startup_sync_lite())
    assert lite["removed"] == []


def test_lite_skips_cleanup_when_exchange_query_fails(lite, monkeypatch):
    lite["positions"] = [{"symbol": "BBBUSDT", "direction": "SHORT", "status": "open", "pos_id": "p1"}]

    def boom(symbol=None):
        raise RuntimeError("api down")
    monkeypatch.setattr("shared.binance_api.get_position", boom)
    asyncio.run(M._startup_sync_lite())
    assert lite["removed"] == [] and lite["added"] == []


def test_lite_respects_limits_and_force_closes_excess(lite):
    # 上限 total=2/long=1/short=1；本地已有 long+short 各 1 → 交易所多出来的孤儿全部超限被平
    lite["positions"] = [{"symbol": "L1", "direction": "LONG", "status": "open", "pos_id": "a"},
                         {"symbol": "S1", "direction": "SHORT", "status": "open", "pos_id": "b"}]
    lite["exchange"] = [_rp("X1USDT", "LONG"), _rp("X2USDT", "SHORT")]
    asyncio.run(M._startup_sync_lite())
    assert lite["added"] == []
    assert len(lite["closed"]) == 2


def test_lite_claims_newest_first_when_over_limit(lite):
    # 上限 long=1，交易所 2 个 long 孤儿，本地 0 仓 → 认领 updateTime 最新的那个
    lite["exchange"] = [_rp("OLDUSDT", "LONG", update=1000), _rp("NEWUSDT", "LONG", update=2000)]
    asyncio.run(M._startup_sync_lite())
    assert [r["symbol"] for r in lite["added"]] == ["NEWUSDT"]
    assert len(lite["closed"]) == 1 and lite["closed"][0][0][0] == "OLDUSDT"


def test_lite_force_close_uses_opposite_side_with_position_side(lite):
    lite["positions"] = [{"symbol": "L1", "direction": "SHORT", "status": "open", "pos_id": "a"}]
    lite["exchange"] = [_rp("X1USDT", "SHORT")]
    asyncio.run(M._startup_sync_lite())
    args, kw = lite["closed"][0]
    assert args[1] == "BUY" and kw["position_side"] == "SHORT"


def test_lite_skips_concurrent_run(lite, monkeypatch):
    monkeypatch.setattr(M, "_sync_lite_busy", True, raising=False)
    lite["exchange"] = [_rp("AAAUSDT", "LONG")]
    asyncio.run(M._startup_sync_lite())
    assert lite["added"] == []


def test_periodic_loop_calls_sync(monkeypatch):
    calls = []

    async def fake_sync():
        calls.append(1)
    monkeypatch.setattr(M, "_startup_sync_lite", fake_sync)
    real_sleep = asyncio.sleep
    stops = {"n": 0}

    async def fast_sleep(sec):
        stops["n"] += 1
        if stops["n"] >= 3:
            raise asyncio.CancelledError
        await real_sleep(0)
    monkeypatch.setattr(M.asyncio, "sleep", fast_sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(M._periodic_sync_loop(1800))
    assert len(calls) >= 1


def test_periodic_sync_task_started_in_main():
    import inspect
    assert 'name="periodic_sync"' in inspect.getsource(M.main)
