"""运行：cd fx && python3 -m unittest tests.test_api_retry_stats -v
[v3.89 #64] binance_api._retry 的最终结果接入 risk_manager 失败率统计：成功记一次 True，重试耗尽只记一次 False。全部 mock。"""
import logging, sys, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import binance_api as A
from shared import risk_manager as RM


class TestRetryStats(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL); self.addCleanup(logging.disable, logging.NOTSET)
        for n in ("sleep",):
            p = mock.patch.object(A.time, n); p.start(); self.addCleanup(p.stop)
        p = mock.patch.object(A, "reset_session"); p.start(); self.addCleanup(p.stop)
        self.log0 = list(RM._api_fail_log); RM._api_fail_log.clear()
        self.addCleanup(lambda: (RM._api_fail_log.clear(), RM._api_fail_log.extend(self.log0)))

    def test_success_records_one_true(self):
        self.assertEqual(A._retry(lambda: {"ok": 1}), {"ok": 1})
        self.assertEqual([f for _, f in RM._api_fail_log], [False])          # failed=False

    def test_exhausted_records_single_failure(self):
        calls = []
        def boom():
            calls.append(1); raise RuntimeError("HTTP 500")
        self.assertIsNone(A._retry(boom))
        self.assertEqual(len(calls), 2)                                       # 默认 1 次尝试 + 1 次重试
        self.assertEqual([f for _, f in RM._api_fail_log], [True])           # 但只记 1 次失败

    def test_retry_then_success_counts_success_only(self):
        n = {"i": 0}
        def flaky():
            n["i"] += 1
            if n["i"] < 2: raise RuntimeError("HTTP 500")
            return [1]
        self.assertEqual(A._retry(flaky), [1])
        self.assertEqual([f for _, f in RM._api_fail_log], [False])

    def test_suppressed_code_not_counted(self):
        def boom(): raise RuntimeError("code=-4046 no change")
        self.assertIsNone(A._retry(boom, suppress_codes={-4046}))
        self.assertEqual(RM._api_fail_log, [])

    def test_write_timeout_not_counted(self):
        def boom(): raise RuntimeError("Read timed out")
        with self.assertRaises(A.WriteTimeoutError):
            A._retry(boom, is_write=True)
        self.assertEqual(RM._api_fail_log, [])

    def test_stats_failure_never_breaks_request(self):
        with mock.patch.object(RM, "record_api_call", side_effect=RuntimeError("stat down")):
            self.assertEqual(A._retry(lambda: 5), 5)
            def boom(): raise RuntimeError("HTTP 500")
            self.assertIsNone(A._retry(boom))

    def test_fail_rate_reflects_outcomes(self):
        def boom(): raise RuntimeError("HTTP 500")
        A._retry(lambda: 1); A._retry(boom); A._retry(boom); A._retry(lambda: 1)
        self.assertAlmostEqual(RM._api_fail_rate_recent(), 0.5)


if __name__ == "__main__":
    unittest.main()
