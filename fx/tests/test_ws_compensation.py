"""运行：cd fx && python3 -m unittest tests.test_ws_compensation -v
[v3.89 #3/#7] userData 断线补偿任务、listenKey 续期失败/过期驱动重建。全部 mock，不连交易所。"""
import asyncio, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import ws_listener as W


class FakeWs:
    def __init__(self): self.closed = 0
    async def close(self): self.closed += 1


class TestCompensation(unittest.TestCase):
    def setUp(self):
        self._t, self._i = W.COMPENSATION_TIMEOUT, W.COMPENSATION_INTERVAL
        W.COMPENSATION_TIMEOUT, W.COMPENSATION_INTERVAL = 0.5, 0.05

    def tearDown(self):
        W.COMPENSATION_TIMEOUT, W.COMPENSATION_INTERVAL = self._t, self._i

    def test_starts_single_task_and_stops_after_reconnect(self):
        async def run():
            ws = W.WsOrderListener()
            calls = []

            async def cb(): calls.append(1)
            ws.on_disconnect(cb)
            ws._mark_user_stream_down()
            t1 = ws._compensation_task
            ws._mark_user_stream_down()               # 重复断线不应再起一个
            self.assertIs(ws._compensation_task, t1)
            await asyncio.sleep(0.2)
            n = len(calls)
            self.assertGreaterEqual(n, 1)
            ws._user_connected = True                 # 重连成功
            await asyncio.wait_for(t1, timeout=1)
            self.assertTrue(t1.done())
            await asyncio.sleep(0.15)
            self.assertLessEqual(len(calls), n + 1)   # 不再发起新一轮（最多是正在执行的那一轮）
        asyncio.run(run())

    def test_bounded_by_timeout(self):
        async def run():
            ws = W.WsOrderListener()
            ws.on_disconnect(lambda: asyncio.sleep(0))
            ws._mark_user_stream_down()
            await asyncio.wait_for(ws._compensation_task, timeout=2)
            self.assertTrue(ws._compensation_task.done())
        asyncio.run(run())

    def test_callback_error_does_not_kill_task(self):
        async def run():
            ws = W.WsOrderListener()
            calls = []

            async def bad(): calls.append(1); raise RuntimeError("x")
            ws.on_disconnect(bad)
            ws._mark_user_stream_down()
            await asyncio.sleep(0.2)
            self.assertGreaterEqual(len(calls), 2)    # 异常后仍继续下一轮
            ws._user_connected = True
            await asyncio.wait_for(ws._compensation_task, timeout=1)
        asyncio.run(run())

    def test_no_callbacks_no_task(self):
        async def run():
            ws = W.WsOrderListener()
            ws._mark_user_stream_down()
            self.assertIsNone(ws._compensation_task)
        asyncio.run(run())

    def test_stop_cancels_compensation(self):
        async def run():
            ws = W.WsOrderListener()
            ws.on_disconnect(lambda: asyncio.sleep(0))
            ws._mark_user_stream_down()
            await ws.stop()
            self.assertTrue(ws._compensation_task.done())
        asyncio.run(run())


class TestKeepaliveRebuild(unittest.TestCase):
    def setUp(self):
        self._k = W.KEEPALIVE_INTERVAL
        W.KEEPALIVE_INTERVAL = 0.01
        self._orig = W.binance_api.keepalive_listen_key

    def tearDown(self):
        W.KEEPALIVE_INTERVAL = self._k
        W.binance_api.keepalive_listen_key = self._orig

    def _run_keepalive(self, fn):
        W.binance_api.keepalive_listen_key = fn

        async def run():
            ws = W.WsOrderListener()
            ws._listen_key = "k"
            fake = FakeWs()
            ws._user_ws = fake
            task = asyncio.create_task(ws._keepalive_loop())
            await asyncio.sleep(0.1)
            ws._stop_flag = True
            await asyncio.wait_for(task, timeout=1)
            return ws, fake
        return asyncio.run(run())

    def test_keepalive_false_closes_connection(self):
        ws, fake = self._run_keepalive(lambda k: False)
        self.assertGreaterEqual(fake.closed, 1)
        self.assertIsNone(ws._listen_key)

    def test_keepalive_exception_closes_connection(self):
        def boom(k): raise OSError("net")
        ws, fake = self._run_keepalive(boom)
        self.assertGreaterEqual(fake.closed, 1)

    def test_keepalive_ok_keeps_connection(self):
        ws, fake = self._run_keepalive(lambda k: True)
        self.assertEqual(fake.closed, 0)
        self.assertEqual(ws._listen_key, "k")

    def test_listen_key_expired_event_closes_connection(self):
        async def run():
            ws = W.WsOrderListener()
            ws._listen_key = "k"
            fake = FakeWs()
            ws._user_ws = fake
            await ws._dispatch_user_data('{"e":"listenKeyExpired"}')
            return ws, fake
        ws, fake = asyncio.run(run())
        self.assertEqual(fake.closed, 1)
        self.assertIsNone(ws._listen_key)

    def test_no_connection_is_safe(self):
        async def run():
            ws = W.WsOrderListener()
            await ws._force_user_reconnect("x")
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
