"""运行：cd fx && python3 -m unittest tests.test_opt_v387_step7 -v
[v3.87 STEP7-2] 强平前重读状态：仓位已不存在或非 open 时不再执行平仓。"""
import asyncio, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import position_manager as pm


class TestForceCloseGuard(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self._orig_get = pm.get_all_positions
        self._orig_inner = pm._force_close_static_inner

        async def fake_inner(pos, reason):
            self.calls.append((pos.get("pos_id"), reason))
        pm._force_close_static_inner = fake_inner
        pm._closing_set.clear()

    def tearDown(self):
        pm.get_all_positions = self._orig_get
        pm._force_close_static_inner = self._orig_inner
        pm._closing_set.clear()

    def _run(self, state, pos, reason="x"):
        pm.get_all_positions = lambda: state
        asyncio.run(pm._force_close_static(pos, reason))

    def test_open_position_closes(self):
        pos = {"pos_id": "A", "symbol": "XUSDT"}
        self._run([{"pos_id": "A", "symbol": "XUSDT", "status": "open"}], pos)
        self.assertEqual(self.calls, [("A", "x")])

    def test_removed_position_skipped(self):
        pos = {"pos_id": "A", "symbol": "XUSDT"}
        self._run([], pos)
        self.assertEqual(self.calls, [])

    def test_non_open_status_skipped(self):
        pos = {"pos_id": "A", "symbol": "XUSDT"}
        self._run([{"pos_id": "A", "symbol": "XUSDT", "status": "pending"}], pos)
        self.assertEqual(self.calls, [])

    def test_second_call_after_first_completes_is_skipped(self):
        """模拟两个巡检循环先后调用：第一次平仓后状态被删除，第二次不应再平仓。"""
        state = [{"pos_id": "A", "symbol": "XUSDT", "status": "open"}]
        pos = {"pos_id": "A", "symbol": "XUSDT"}

        async def inner_and_remove(p, r):
            self.calls.append((p.get("pos_id"), r))
            state.clear()
        pm._force_close_static_inner = inner_and_remove
        pm.get_all_positions = lambda: state

        async def both():
            await pm._force_close_static(pos, "time_stop")
            await pm._force_close_static(pos, "exit_signal")
        asyncio.run(both())
        self.assertEqual(self.calls, [("A", "time_stop")])

    def test_state_read_failure_does_not_block_close(self):
        def boom():
            raise RuntimeError("io")
        pm.get_all_positions = boom
        asyncio.run(pm._force_close_static({"pos_id": "A", "symbol": "XUSDT"}, "x"))
        self.assertEqual(self.calls, [("A", "x")])


if __name__ == "__main__":
    unittest.main()
