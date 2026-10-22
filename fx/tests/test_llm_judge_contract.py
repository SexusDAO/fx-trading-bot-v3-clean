"""运行：cd fx && python3 -m unittest tests.test_llm_judge_contract -v
[v3.89 #79] LLM 结果结构校验，无效结构沿用原降级。全部 mock，不调用任何模型。"""
import logging, sys, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import llm_judge as L

GOOD = {"decision": "open", "confidence": 0.8, "reason": "ok"}


class TestValidate(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL); self.addCleanup(logging.disable, logging.NOTSET)

    def test_valid_passthrough(self):
        self.assertIs(L._validate_llm_result(GOOD, "t"), GOOD)
        self.assertIsNotNone(L._validate_llm_result({"decision": "skip", "confidence": 0, "reason": ""}, "t"))
        self.assertIsNotNone(L._validate_llm_result({"decision": "skip", "confidence": 1}, "t"))

    def test_invalid_shapes_rejected(self):
        bad = [[], "open", 1, None, {}, {"decision": "buy", "confidence": 0.5}, {"decision": None, "confidence": 0.5},
               {"decision": "open"}, {"decision": "open", "confidence": "0.9"}, {"decision": "open", "confidence": 1.5},
               {"decision": "open", "confidence": -0.1}, {"decision": "open", "confidence": float("nan")},
               {"decision": "open", "confidence": True}]
        n0 = L._invalid_result_count
        for b in bad:
            with self.subTest(b=b):
                self.assertIsNone(L._validate_llm_result(b, "t"))
        self.assertEqual(L._invalid_result_count, n0 + len(bad))


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


if __name__ == "__main__":
    unittest.main()
