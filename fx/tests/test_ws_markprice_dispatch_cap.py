"""markPrice 有限并发分发：在途任务不超过上限，超出丢帧并按间隔汇总告警；回调结束后槽位释放。
运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_ws_markprice_dispatch_cap.py（mock，不连 WS）"""
import asyncio, json, logging, os, sys
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared import ws_listener as wl


def _frame(p=100.0):
    return json.dumps({"stream": "xusdt@markPrice", "data": {"e": "markPriceUpdate", "s": "XUSDT", "p": str(p), "T": 1}})


def test_burst_is_capped_and_drops_counted():
    async def go():
        L = wl.WsOrderListener()
        gate = asyncio.Event(); seen = []
        async def slow_cb(sym, p, ts):
            seen.append(p); await gate.wait()
        L._price_callbacks.append(slow_cb)
        for i in range(100):                      # 同步连发 100 帧（模拟接收循环不让出控制权）
            L._mp_try_dispatch(_frame(100 + i))
        peak, drops, tasks = L._mp_inflight, L._mp_drop_count, len(L._mp_tasks)
        gate.set(); await asyncio.sleep(0.05)
        return L, peak, drops, tasks, len(seen)
    L, peak, drops, tasks, n_seen = asyncio.run(go())
    assert peak == wl._MP_MAX_INFLIGHT == 32 and tasks == 32
    assert drops == 68 and n_seen == 32
    assert L._mp_inflight == 0 and not L._mp_tasks          # 槽位全部释放、任务引用已清理


def test_slot_released_even_if_callback_raises():
    async def go():
        L = wl.WsOrderListener()
        async def bad(sym, p, ts): raise RuntimeError("boom")
        L._price_callbacks.append(bad)
        for _ in range(5): L._mp_try_dispatch(_frame())
        await asyncio.sleep(0.05)
        return L._mp_inflight
    assert asyncio.run(go()) == 0


def test_drop_summary_warning_every_interval(caplog):
    L = wl.WsOrderListener()
    L._mp_drop_count = 7; L._mp_inflight = 32; L._mp_drop_last_log = 0.0
    with caplog.at_level(logging.WARNING):
        L._mp_log_drops(now=10.0)                  # 未到 60s：不输出
        assert not caplog.records
        L._mp_log_drops(now=61.0)
    assert any("丢弃 7 帧" in r.getMessage() and "当前活跃任务=32" in r.getMessage() for r in caplog.records)
    assert L._mp_drop_count == 0
    caplog.clear()
    L._mp_log_drops(now=130.0)                     # 无丢帧：不输出
    assert not caplog.records


def test_all_three_connects_use_max_queue_and_default_is_32():
    src = (Path(__file__).parent.parent / "shared" / "ws_listener.py").read_text()
    calls = src.split("websockets.connect(")[2:]
    assert len(calls) == 3 and all("max_queue=_MP_MAX_QUEUE" in c.split(") as ws")[0] for c in calls)
    import config
    assert config.WS_MARK_PRICE_MAX_QUEUE == 32
