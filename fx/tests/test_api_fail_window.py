"""运行：cd fx && python3 -m unittest tests.test_api_fail_window -v
[v3.89 #65] API 失败率：次数窗口（50）+ 时间窗口（600s）；窗口内无样本返回 0.0 并标注 no_sample。"""
import sys, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import risk_manager as RM


class TestFailWindow(unittest.TestCase):
    def setUp(self):
        self.old = list(RM._api_fail_log); RM._api_fail_log.clear()
        self.addCleanup(lambda: (RM._api_fail_log.clear(), RM._api_fail_log.extend(self.old)))

    def test_constants(self):
        self.assertEqual(RM._API_FAIL_WINDOW_SEC, 600.0)
        self.assertEqual(RM._API_FAIL_TAIL, 50)

    def test_empty_is_no_sample(self):
        s = RM._api_fail_stats()
        self.assertEqual((s["rate"], s["samples"], s["status"], s["newest_age_s"]), (0.0, 0, "no_sample", None))
        self.assertEqual(RM._api_fail_rate_recent(), 0.0)

    def test_old_failures_not_counted(self):
        now = time.time()
        RM._api_fail_log.extend([(now - 3600, True)] * 20)
        s = RM._api_fail_stats(now)
        self.assertEqual(s["status"], "no_sample"); self.assertEqual(s["rate"], 0.0)
        self.assertAlmostEqual(s["newest_age_s"], 3600, delta=1)           # 仍输出最新样本年龄
        self.assertEqual(RM._api_fail_rate_recent(), 0.0)                   # 过期故障不再触发风控

    def test_only_fresh_samples_counted(self):
        now = time.time()
        RM._api_fail_log.extend([(now - 3600, True)] * 10 + [(now - 30, True), (now - 20, False), (now - 10, False), (now - 5, True)])
        s = RM._api_fail_stats(now)
        self.assertEqual(s["samples"], 4); self.assertEqual(s["rate"], 0.5); self.assertEqual(s["status"], "ok")

    def test_count_window_still_applies(self):
        now = time.time()
        RM._api_fail_log.extend([(now - 5, True)] * 100 + [(now - 1, False)] * 50)
        s = RM._api_fail_stats(now)
        self.assertEqual(s["samples"], 50); self.assertEqual(s["rate"], 0.0)  # 只看最近 50 次

    def test_boundary_inclusive(self):
        now = time.time()
        RM._api_fail_log.append((now - 600.0, True))
        self.assertEqual(RM._api_fail_stats(now)["samples"], 1)


if __name__ == "__main__":
    unittest.main()
