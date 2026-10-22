"""运行：cd fx && python3 -m unittest tests.test_session_reset -v
[v3.89 #86] reset_session：加锁替换、先关旧会话、记录重建次数。全部 mock。"""
import sys, threading, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import binance_api as A


class FakeSession:
    made = []
    def __init__(self, fail_close=False):
        self.closed = 0
        self.fail_close = fail_close
        FakeSession.made.append(self)
    def close(self):
        self.closed += 1
        if self.fail_close:
            raise RuntimeError("close boom")


class TestResetSession(unittest.TestCase):
    def setUp(self):
        self._mk = A._make_session
        self._s, self._d, self._c = A._SESSION, A._SESSION_DIRECT, A._SESSION_RESET_COUNT
        FakeSession.made = []
        A._make_session = lambda use_proxy=True: FakeSession()
        A._SESSION, A._SESSION_DIRECT = FakeSession(), FakeSession()

    def tearDown(self):
        A._make_session = self._mk
        A._SESSION, A._SESSION_DIRECT, A._SESSION_RESET_COUNT = self._s, self._d, self._c

    def test_old_sessions_closed_and_replaced(self):
        old1, old2 = A._SESSION, A._SESSION_DIRECT
        c0 = A._SESSION_RESET_COUNT
        A.reset_session()
        self.assertEqual((old1.closed, old2.closed), (1, 1))
        self.assertIsNot(A._SESSION, old1)
        self.assertIsNot(A._SESSION_DIRECT, old2)
        self.assertEqual(A._SESSION_RESET_COUNT, c0 + 1)

    def test_close_failure_does_not_break_reset(self):
        A._SESSION = FakeSession(fail_close=True)
        A.reset_session()           # 不抛异常
        self.assertTrue(A._PROXY_HEALTHY)

    def test_concurrent_resets_each_session_closed_once(self):
        olds = [A._SESSION, A._SESSION_DIRECT]
        ts = [threading.Thread(target=A.reset_session) for _ in range(8)]
        [t.start() for t in ts]; [t.join() for t in ts]
        every = olds + FakeSession.made
        live = {id(A._SESSION), id(A._SESSION_DIRECT)}
        for s in every:
            self.assertEqual(s.closed, 0 if id(s) in live else 1)


if __name__ == "__main__":
    unittest.main()
