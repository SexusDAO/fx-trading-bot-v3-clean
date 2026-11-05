"""markPrice 协议级动态订阅：subscribe/unsubscribe 不再重建连接（全 mock，不连 WS）。

对应改动：fx/shared/ws_listener.py
运行：cd fx && python3 -m pytest -q tests/test_ws_dynamic_subscribe.py
"""
import asyncio
import json
import os
import sys
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared import ws_listener as wl


def _listener():
    return wl.WsOrderListener()


def _frame(sym="XUSDT", p=100.0):
    return json.dumps({"stream": f"{sym.lower()}@markPrice",
                       "data": {"e": "markPriceUpdate", "s": sym, "p": str(p), "T": 1}})


# ═══ 1. 首次订阅（0→1）只触发 _rebuild_event ═══
def test_first_subscribe_triggers_rebuild_not_dynamic():
    async def go():
        L = _listener()
        L._sync_subscribe("btcusdt")            # 大小写归一
        return L
    L = asyncio.run(go())
    assert L._subscribed == {"BTCUSDT"}
    assert L._rebuild_event.is_set() is True
    assert L._dynamic_sub_event.is_set() is False
    assert L._pending_subscribe == {"BTCUSDT"}


# ═══ 2. 已有订阅时再订阅新标的 → 只触发 _dynamic_sub_event ═══
def test_second_subscribe_triggers_dynamic_not_rebuild():
    async def go():
        L = _listener()
        L._sync_subscribe("BTCUSDT")
        L._rebuild_event.clear()                # 模拟首连已完成
        L._sync_subscribe("ETHUSDT")
        return L
    L = asyncio.run(go())
    assert L._subscribed == {"BTCUSDT", "ETHUSDT"}
    assert L._rebuild_event.is_set() is False
    assert L._dynamic_sub_event.is_set() is True
    assert "ETHUSDT" in L._pending_subscribe


# ═══ 3. 退订只触发 _dynamic_sub_event ═══
def test_unsubscribe_triggers_dynamic_not_rebuild():
    async def go():
        L = _listener()
        L._sync_subscribe("BTCUSDT")
        L._sync_subscribe("ETHUSDT")
        L._rebuild_event.clear()
        L._dynamic_sub_event.clear()
        L._pending_subscribe.clear()
        L._sync_unsubscribe("ethusdt")
        return L
    L = asyncio.run(go())
    assert L._subscribed == {"BTCUSDT"}
    assert L._rebuild_event.is_set() is False
    assert L._dynamic_sub_event.is_set() is True
    assert L._pending_unsubscribe == {"ETHUSDT"}


# ═══ 4. _apply_dynamic_subscriptions 发出的消息格式 ═══
class _FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, msg):
        self.sent.append(json.loads(msg))


def test_apply_dynamic_subscriptions_message_format():
    async def go():
        L = _listener()
        ws = _FakeWS()
        L._pending_subscribe = {"BTCUSDT", "ETHUSDT"}
        L._pending_unsubscribe = {"SOLUSDT"}
        L._subscribed = {"SOLUSDT", "BTCUSDT", "ETHUSDT"}
        await L._apply_dynamic_subscriptions(ws)
        return L, ws
    L, ws = asyncio.run(go())
    subs = [m for m in ws.sent if m["method"] == "SUBSCRIBE"]
    unsubs = [m for m in ws.sent if m["method"] == "UNSUBSCRIBE"]
    assert len(subs) == 1 and len(unsubs) == 1
    assert subs[0]["id"] == 1
    assert sorted(subs[0]["params"]) == ["btcusdt@markPrice", "ethusdt@markPrice"]
    assert unsubs[0] == {"method": "UNSUBSCRIBE", "params": ["solusdt@markPrice"], "id": 2}
    # 队列被清空，不会重复发送
    assert L._pending_subscribe == set() and L._pending_unsubscribe == set()


def test_apply_dynamic_subscriptions_noop_when_queues_empty():
    async def go():
        L = _listener()
        ws = _FakeWS()
        await L._apply_dynamic_subscriptions(ws)
        return ws
    assert asyncio.run(go()).sent == []


def test_resubscribe_cancels_pending_unsubscribe_of_same_symbol():
    async def go():
        L = _listener()
        L._sync_subscribe("BTCUSDT")
        L._sync_subscribe("ETHUSDT")
        L._sync_unsubscribe("ETHUSDT")
        L._sync_subscribe("ETHUSDT")            # 又订回来：待退订应被抵消
        return L
    L = asyncio.run(go())
    assert "ETHUSDT" in L._subscribed
    assert L._pending_unsubscribe == set()
    assert "ETHUSDT" in L._pending_subscribe


# ═══ 5. 接收循环：动态订阅不断线；退订到空集合才断开 ═══
class _FakeConn:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *a):
        return False


class _ScriptedWS(_FakeWS):
    """按脚本产出帧，之后保持连接打开（迭代不结束，模拟空闲但未断开的连接）。

    每帧之间必须 await：否则接收循环拿不到控制权，测试里的 sleep / 取消都不会生效。
    """

    def __init__(self, frames, on_frame=None):
        super().__init__()
        self._frames = list(frames)
        self._on_frame = on_frame
        self.url = ""

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for i, f in enumerate(self._frames):
            await asyncio.sleep(0.005)
            if self._on_frame:
                self._on_frame(i, self)
            yield f
        await asyncio.Event().wait()


def _patch_connect(monkeypatch, factory):
    def _connect(url, **kw):
        ws = factory()
        ws.url = url
        return _FakeConn(ws)
    monkeypatch.setattr(wl.websockets, "connect", _connect)


def _run_loop(go, monkeypatch, factory):
    _patch_connect(monkeypatch, factory)
    return asyncio.run(go())


def test_loop_uses_dynamic_subscribe_without_reconnecting(monkeypatch):
    made = []
    sentinel = {}

    def on_frame(i, ws):
        if i == 0:
            sentinel["L"]._sync_subscribe("ETHUSDT")        # 连接已建立后订阅新标的
        if i == 1:
            sentinel["L"]._sync_unsubscribe("BTCUSDT")      # 再退订一个

    def factory():
        ws = _ScriptedWS([_frame("BTCUSDT")] * 4, on_frame)
        made.append(ws)
        return ws

    async def go():
        L = _listener()
        sentinel["L"] = L
        L._sync_subscribe("BTCUSDT")                        # 首次订阅 → 触发首连
        task = asyncio.ensure_future(L._mark_price_loop())
        await asyncio.sleep(0.1)
        L._stop_flag = True
        task.cancel()
        try:
            await task
        except BaseException:
            pass
        return L

    _run_loop(go, monkeypatch, factory)
    assert len(made) == 1, f"不应重连，实际连接次数={len(made)}"
    ws = made[0]
    assert "btcusdt@markPrice" in ws.url and "ethusdt@markPrice" not in ws.url   # 初连 URL 只含首个标的
    sub_msgs = [m for m in ws.sent if m["method"] == "SUBSCRIBE"]
    unsub_msgs = [m for m in ws.sent if m["method"] == "UNSUBSCRIBE"]
    assert len(sub_msgs) == 1 and sub_msgs[0]["params"] == ["ethusdt@markPrice"]
    assert len(unsub_msgs) == 1 and unsub_msgs[0]["params"] == ["btcusdt@markPrice"]


def test_loop_breaks_when_last_symbol_unsubscribed(monkeypatch):
    """退订到空集合后必须断开连接，否则内层循环永久阻塞，后续订阅无法生效。"""
    made = []
    sentinel = {}

    def on_frame(i, ws):
        if i == 0:
            sentinel["L"]._sync_unsubscribe("BTCUSDT")      # 唯一标的被退订 → 订阅集合变空

    def factory():
        ws = _ScriptedWS([_frame("BTCUSDT")] * 3, on_frame)
        made.append(ws)
        return ws

    async def go():
        L = _listener()
        sentinel["L"] = L
        L._sync_subscribe("BTCUSDT")
        task = asyncio.ensure_future(L._mark_price_loop())
        await asyncio.sleep(0.1)
        assert L._subscribed == set()
        L._sync_subscribe("ETHUSDT")                        # 断开后应能被重新唤醒
        await asyncio.sleep(0.1)
        L._stop_flag = True
        task.cancel()
        try:
            await task
        except BaseException:
            pass
        return L

    _run_loop(go, monkeypatch, factory)
    assert len(made) >= 2, "退订到空后应断开并重新建连"
    assert "ethusdt@markPrice" in made[-1].url


def test_reconnect_discards_stale_pending_queues(monkeypatch):
    """重连时 URL 已含全部订阅，建连前攒下的增量队列必须清空，避免重复 SUBSCRIBE。"""
    made = []

    def factory():
        ws = _ScriptedWS([_frame("BTCUSDT")] * 2)
        made.append(ws)
        return ws

    async def go():
        L = _listener()
        L._sync_subscribe("BTCUSDT")
        L._sync_subscribe("ETHUSDT")                        # 攒下 pending_subscribe={ETHUSDT}
        task = asyncio.ensure_future(L._mark_price_loop())
        await asyncio.sleep(0.1)
        L._stop_flag = True
        task.cancel()
        try:
            await task
        except BaseException:
            pass
        return L

    _run_loop(go, monkeypatch, factory)
    ws = made[0]
    assert "btcusdt@markPrice" in ws.url and "ethusdt@markPrice" in ws.url
    assert [m for m in ws.sent if m["method"] == "SUBSCRIBE"] == []
