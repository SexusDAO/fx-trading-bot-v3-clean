"""运行：cd fx && python3 -m unittest tests.test_judge_routes_contract -v
[v3.89 #48/#26/#27] 七条路线 + 资金费率套利入口的返回契约：
  - 返回 (bool, dict)；被拒绝时 dict 含 reject_reason
  - 输入残缺/畸形时不向外抛异常；内部异常统一为 internal_error 结构（含 mode / error_type）
本文件只验证契约，不构造「会通过」的行情，也不修改任何阈值；stop_hunt 保持关闭状态，不在测试中启用。
"""
import logging, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import judges as J

ROUTES = {
    "pullback": J.judge_pullback,
    "breakout": J.judge_breakout,
    "reversal": J.judge_reversal,
    "range_bounce": J.judge_range_bounce,
    "stop_hunt": J.judge_stop_hunt,
    "oi_divergence": J.judge_oi_divergence,
    "ema_trend": J.judge_ema_trend,
}


def _kl(i, c=100.0):
    return [i * 3600_000, str(c), str(c + 1), str(c - 1), str(c), "10", i * 3600_000 + 3599_999]


def _check_contract(tc, res, name):
    tc.assertIsInstance(res, tuple, name)
    tc.assertEqual(len(res), 2, name)
    ok, detail = res
    tc.assertIsInstance(ok, bool, name)
    tc.assertIsInstance(detail, dict, name)
    if not ok:
        tc.assertIsInstance(detail.get("reject_reason"), str, f"{name} 拒绝时必须带 reject_reason")
    return ok, detail


class TestRouteContract(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def test_empty_inputs_never_raise_and_keep_contract(self):
        for name, fn in ROUTES.items():
            with self.subTest(route=name):
                ok, detail = _check_contract(self, fn([], [], [], {}, 0.0, 0.0), name)
                self.assertFalse(ok)

    def test_malformed_inputs_become_internal_error_result(self):
        """ind 不是 dict、K 线元素缺列：必须收敛成内部异常结构而不是抛出。"""
        bad_sets = [
            ([[1]] * 30, [[1]] * 30, [[1]] * 30, {}),
            ([_kl(i) for i in range(60)], [_kl(i) for i in range(60)], [_kl(i) for i in range(60)], None),
        ]
        for name, fn in ROUTES.items():
            for k1h, k4h, k15, ind in bad_sets:
                with self.subTest(route=name):
                    ok, detail = _check_contract(self, fn(k1h, k4h, k15, ind, 1.0, 0.0), name)
                    self.assertFalse(ok)
                    if str(detail.get("reject_reason", "")).startswith("内部异常"):
                        self.assertTrue(detail.get("internal_error"))
                        self.assertEqual(detail.get("mode"), name)
                        self.assertTrue(detail.get("error_type"))

    def test_funding_arb_contract(self):
        res = J.judge_funding_arb("XUSDT", 0.0, [0.0, 0.0, 0.0], {}, 0.0)
        ok, detail = _check_contract(self, res, "funding_arb")
        self.assertFalse(ok)

    def test_funding_arb_exception_wrapped(self):
        """[v3.89] funding_arb 外层异常包装：畸形输入不抛出，返回统一的 internal_error 结构。"""
        for ind in (None, "bad", 123):
            with self.subTest(ind=ind):
                ok, d = _check_contract(self, J.judge_funding_arb("XUSDT", None, None, ind, None), "funding_arb(bad)")
                self.assertFalse(ok)
                self.assertTrue(d.get("internal_error"))
                self.assertEqual(d.get("mode"), "funding_arb")
                self.assertTrue(d.get("error_type"))
                self.assertTrue(d["reject_reason"].startswith("内部异常"))

    def test_stop_hunt_stays_rejected(self):
        """stop_hunt 按现状关闭：这里只确认空输入不会通过，不启用开关。"""
        ok, _ = J.judge_stop_hunt([], [], [], {}, 0.0, 0.0)
        self.assertFalse(ok)


class TestInternalErrorHelper(unittest.TestCase):
    def test_error_result_shape_and_reason_text(self):
        ok, d = J._judge_error_result("judge_x", "x", ValueError("boom"))
        self.assertFalse(ok)
        self.assertEqual(d["reject_reason"], "内部异常: boom")    # 文案不变，下游按文案归类不受影响
        self.assertEqual(d["mode"], "x")
        self.assertTrue(d["internal_error"])
        self.assertEqual(d["error_type"], "ValueError")

    def test_wrapper_converts_impl_exception(self):
        orig = J._judge_range_bounce_impl
        J._judge_range_bounce_impl = lambda *a, **k: (_ for _ in ()).throw(KeyError("k"))
        try:
            logging.disable(logging.CRITICAL)
            ok, d = J.judge_range_bounce([], [], [], {"symbol": "XUSDT"}, 0.0, 0.0)
        finally:
            logging.disable(logging.NOTSET)
            J._judge_range_bounce_impl = orig
        self.assertFalse(ok)
        self.assertTrue(d["internal_error"])
        self.assertEqual(d["error_type"], "KeyError")
        self.assertEqual(d["mode"], "range_bounce")


class TestFailureLogging(unittest.TestCase):
    def test_log_has_context_and_traceback_is_rate_limited(self):
        J._JUDGE_ERR_LAST.clear()
        try:
            raise RuntimeError("x")
        except RuntimeError as exc:      # 真实抛出的异常才带 __traceback__
            err = exc
        with self.assertLogs(J.logger, level="WARNING") as cm:
            J._log_judge_failure("judge_t", err, [1, 2, 3], None, [1], {"symbol": "ABCUSDT"})
            J._log_judge_failure("judge_t", err, [1, 2, 3], None, [1], {"symbol": "ABCUSDT"})
        first, second = cm.output
        self.assertIn("symbol=ABCUSDT", first)
        self.assertIn("k1h=3", first)
        self.assertIn("k4h=None", first)
        self.assertIn("Traceback", first)            # 首次带堆栈
        self.assertNotIn("Traceback", second)        # 限频窗口内不重复堆栈
        self.assertIn("堆栈限频", second)

    def test_log_tolerates_non_dict_ind(self):
        J._JUDGE_ERR_LAST.clear()
        with self.assertLogs(J.logger, level="WARNING"):
            J._log_judge_failure("judge_t2", ValueError("v"), None, None, None, None)


if __name__ == "__main__":
    unittest.main()
