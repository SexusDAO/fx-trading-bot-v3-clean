"""运行：cd fx && python3 -m unittest tests.test_llm_cooldown -v
[v3.89 #80] 冷却内返回显式 unavailable 标注，决策不变。全部 mock，不调用任何模型。"""
import logging, sys, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import llm_judge as L

GOOD = {"decision": "open", "confidence": 0.8, "reason": "ok"}


class TestJudges(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL); self.addCleanup(logging.disable, logging.NOTSET)
        L._llm_call_cache.clear(); self.addCleanup(L._llm_call_cache.clear)
        for n, v in (("_is_enabled", True),):
            p = mock.patch.object(L, n, return_value=v); p.start(); self.addCleanup(p.stop)

    def _ind(self):
        return {"rsi_1h": 50, "rsi_4h": 50, "atr_ratio": 1.0, "ema_bull_1h": True}

    def test_invalid_result_degrades_to_existing_fallback(self):
        for bad in ([], "x", {"decision": "buy", "confidence": 0.9}, {"decision": "open", "confidence": 9}):
            with mock.patch.object(L, "_call_llm", return_value=bad):
                e = L.judge_explosive_coin_llm("XUSDT", 100, 5, 70, 70, 3.0, 0.001)
                b = L.judge_borderline_signal_llm("XUSDT", "breakout", "LONG", 50, self._ind(), 1.0, "bull")
                n = L.judge_news_sentiment_llm("XUSDT", "x" * 40, "LONG")
            self.assertEqual((e["decision"], e["confidence"], e["reason"]), ("skip", 0.5, "LLM降级"))
            self.assertEqual((b["decision"], b["confidence"], b["reason"]), ("skip", 0.5, "边界降级"))
            self.assertEqual(n, "neutral")
            self.assertEqual(L._llm_call_cache, {})           # 无效结果不进入冷却

    def test_valid_result_returned_and_cooldown_marked(self):
        with mock.patch.object(L, "_call_llm", return_value=GOOD):
            e = L.judge_explosive_coin_llm("XUSDT", 100, 5, 70, 70, 3.0, 0.001)
        self.assertIs(e, GOOD)
        self.assertIn("exp_XUSDT", L._llm_call_cache)

    def test_cooldown_result_is_labelled_but_decision_unchanged(self):
        with mock.patch.object(L, "_call_llm", return_value=GOOD) as m:
            L.judge_explosive_coin_llm("XUSDT", 100, 5, 70, 70, 3.0, 0.001)
            L.judge_borderline_signal_llm("XUSDT", "breakout", "LONG", 50, self._ind(), 1.0, "bull")
            m.reset_mock()
            e = L.judge_explosive_coin_llm("XUSDT", 100, 5, 70, 70, 3.0, 0.001)
            b = L.judge_borderline_signal_llm("XUSDT", "breakout", "LONG", 50, self._ind(), 1.0, "bull")
            m.assert_not_called()                              # 冷却内不调用模型
        for r in (e, b):
            self.assertEqual(r["decision"], "skip")            # 决策不变
            self.assertEqual(r["confidence"], 0.5)
            self.assertEqual(r["source"], "cooldown")
            self.assertTrue(r["unavailable"])

    def test_cooldown_result_is_a_copy(self):
        fb = {"decision": "skip", "confidence": 0.5, "reason": "x"}
        r = L._cooldown_result(fb)
        r["decision"] = "open"
        self.assertEqual(fb["decision"], "skip")
        self.assertNotIn("source", fb)

    def test_model_skip_is_distinguishable_from_cooldown(self):
        with mock.patch.object(L, "_call_llm", return_value={"decision": "skip", "confidence": 0.9, "reason": "no"}):
            r = L.judge_borderline_signal_llm("YUSDT", "breakout", "LONG", 50, self._ind(), 1.0, "bull")
        self.assertNotIn("unavailable", r)
        self.assertNotIn("source", r)


if __name__ == "__main__":
    unittest.main()
