"""运行：cd fx && python3 -m unittest tests.test_news_order_log -v
[v3.89 #78] 聚合结果不变，日志如实描述利好与响应类利空的先后。全部 mock，不访问网络。"""
import logging, sys, time, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import news_signal as N


class TestAggregationLog(unittest.TestCase):
    def setUp(self):
        N._cache.clear(); self.addCleanup(N._cache.clear)

    def _run(self, resp_ms, bull_ms):
        items = [{"title": "RESP", "body": "", "date": resp_ms}, {"title": "BULL", "body": "", "date": bull_ms}]
        cls = lambda t: "bearish_with_response" if "RESP" in t else "bullish"
        with mock.patch.object(N, "_fetch_raw_news", return_value=items), mock.patch.object(N, "_classify_text", cls):
            with self.assertLogs(N.logger, level="INFO") as cm:
                res = N.get_news_bias("XYZUSDT")
        return res, "\n".join(cm.output)

    def test_result_unchanged_and_text_not_claiming_later(self):
        now = int(time.time() * 1000)
        res, log = self._run(now - 3600_000, now - 7200_000)      # 利好【早于】响应类利空
        self.assertEqual(res, "bullish")                           # 聚合规则不变
        self.assertNotIn("被后续利好", log)
        self.assertIn("同时存在", log)
        self.assertIn("利好早于响应类利空", log)

    def test_later_bullish_is_described_as_later(self):
        now = int(time.time() * 1000)
        res, log = self._run(now - 7200_000, now - 3600_000)
        self.assertEqual(res, "bullish")
        self.assertIn("利好晚于响应类利空", log)

    def test_missing_dates_say_cannot_compare(self):
        self.assertIn("无法比较先后", N._news_order_desc(0.0, 0.0))


if __name__ == "__main__":
    unittest.main()
