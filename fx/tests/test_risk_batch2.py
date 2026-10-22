"""运行：cd fx && python3 -m unittest tests.test_risk_batch2 -v
[v3.89 #63/#66/#67] 风险管理：盈亏持久化失败可识别、USDT 数据不可用状态、回撤状态损坏备份。全部 mock。"""
import json, sys, tempfile, time, types, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import risk_manager as R


class TestRecordPnl(unittest.TestCase):
    def test_persist_failure_is_not_reported_as_not_triggered(self):
        with mock.patch("shared.state.record_pnl", side_effect=OSError("disk")), \
             mock.patch("shared.state.get_daily_loss", return_value=12.0), \
             self.assertLogs("fx.risk_manager", level="ERROR") if False else mock.patch.object(R.logger, "error"):
            r = R.record_pnl(-5.0, "XUSDT", "t")
        self.assertIs(r["persisted"], False)
        self.assertIsNone(r["circuit_triggered"])            # 未知，而不是 False
        self.assertFalse(r["circuit_triggered"])             # 仍是假值，兼容既有 if 判断
        self.assertIn("disk", r["error"])

    def test_success_unchanged_shape(self):
        with mock.patch("shared.state.record_pnl", return_value=False), \
             mock.patch("shared.state.get_daily_loss", return_value=3.0):
            r = R.record_pnl(-1.0)
        self.assertEqual(r, {"circuit_triggered": False, "daily_loss": 3.0, "persisted": True})

    def test_get_daily_loss_failure_does_not_raise(self):
        with mock.patch("shared.state.record_pnl", return_value=False), \
             mock.patch("shared.state.get_daily_loss", side_effect=OSError("x")):
            r = R.record_pnl(-1.0)
        self.assertIsNone(r["daily_loss"])


class TestUsdtPeg(unittest.TestCase):
    def _obs(self, ret=None, exc=None):
        from shared import binance_api as api
        f = mock.Mock(side_effect=exc) if exc else mock.Mock(return_value=ret)
        with mock.patch.object(api, "get_mark_price", f):
            return R.get_usdt_peg_observation()

    def test_missing_zero_invalid_and_exception_are_unavailable(self):
        for ret in ({}, {"markPrice": None}, {"markPrice": "0"}, {"markPrice": ""}, {"markPrice": "nan"}, None):
            self.assertEqual(self._obs(ret)["status"], "unavailable", ret)
        self.assertEqual(self._obs(exc=OSError("net"))["status"], "unavailable")
        self.assertEqual(R.LAST_USDT_PEG_STATUS["status"], "unavailable")

    def test_valid_rate(self):
        o = self._obs({"markPrice": "0.9995"})
        self.assertEqual((o["status"], o["rate"]), ("ok", 0.9995))

    def test_unavailable_not_treated_as_peg_ok_nor_blocking(self):
        from shared import binance_api as api
        with mock.patch.object(api, "get_mark_price", return_value={}), \
             mock.patch.object(R, "_api_fail_rate_recent", return_value=0.0, create=True), \
             mock.patch.object(R.logger, "warning") as w:
            R._peg_last_warn = 0.0
            ok, reason = R.check_systemic_risk()
        self.assertTrue(w.called)                          # 数据不可用被记录
        self.assertNotIn("脱锚", reason)                    # 没有用 1.0 伪造观测，也没有因此阻断

    def test_depeg_still_detected(self):
        from shared import binance_api as api
        with mock.patch.object(api, "get_mark_price", return_value={"markPrice": "0.98"}):
            ok, reason = R.check_systemic_risk()
        self.assertFalse(ok)
        self.assertIn("脱锚", reason)


class TestDrawdownState(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self._f = R._DRAWDOWN_STATE_FILE
        R._DRAWDOWN_STATE_FILE = str(self.d / "drawdown_state.json")
        R._DRAWDOWN_STATE_CORRUPTED = False

    def tearDown(self):
        R._DRAWDOWN_STATE_FILE = self._f
        R._DRAWDOWN_STATE_CORRUPTED = False

    def test_first_init_is_default_and_not_corrupted(self):
        self.assertEqual(R._load_drawdown_state()["high_water"], 0.0)
        self.assertFalse(R._DRAWDOWN_STATE_CORRUPTED)

    def test_corrupt_file_backed_up_logged_and_flagged(self):
        p = Path(R._DRAWDOWN_STATE_FILE); p.write_text("{bad")
        with mock.patch.object(R.logger, "error") as e:
            st = R._load_drawdown_state()
        self.assertTrue(R._DRAWDOWN_STATE_CORRUPTED)
        self.assertTrue(e.called)
        bk = list(self.d.glob("drawdown_state.json.corrupted_*"))
        self.assertEqual(len(bk), 1)
        self.assertEqual(bk[0].read_text(), "{bad")           # 损坏文件原样保留
        self.assertEqual(st["high_water"], 0.0)

    def test_non_object_json_is_corrupt(self):
        Path(R._DRAWDOWN_STATE_FILE).write_text("[1,2]")
        R._load_drawdown_state()
        self.assertTrue(R._DRAWDOWN_STATE_CORRUPTED)

    def test_atomic_write_and_update(self):
        R.update_drawdown_high_water(1000.0)
        self.assertEqual(json.loads(Path(R._DRAWDOWN_STATE_FILE).read_text())["high_water"], 1000.0)
        self.assertEqual(list(self.d.glob("*.tmp_*")), [])
        R.update_drawdown_high_water(900.0)                   # 低于高水位不更新
        self.assertEqual(R._load_drawdown_state()["high_water"], 1000.0)

    def test_concurrent_updates_keep_max(self):
        import threading
        ts = [threading.Thread(target=R.update_drawdown_high_water, args=(float(v),)) for v in range(100, 140)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(R._load_drawdown_state()["high_water"], 139.0)


if __name__ == "__main__":
    unittest.main()
