"""运行：cd fx && python3 -m unittest tests.test_judges_extra -v
[v3.89 #28/#30/#31] judges 辅助函数：非有限数值、冷却写入失败、配置回退日志。"""
import logging, math, sys, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import judges as J


class TestVolatilityTier(unittest.TestCase):
    def test_nan_and_inf_not_in_normal_branches(self):
        for atr, price in ((float("nan"), 100.0), (1.0, float("nan")), (float("inf"), 100.0),
                           (1.0, float("inf")), (float("-inf"), 100.0), ("x", 100.0), (None, 100.0)):
            with self.assertLogs("fx.judges", level="WARNING"):
                self.assertEqual(J._volatility_tier(atr, price), "invalid")

    def test_tier_values_unchanged(self):
        self.assertEqual(J._volatility_tier(2.0, 100.0), "low")       # 2%
        self.assertEqual(J._volatility_tier(3.0, 100.0), "mid")       # 3%
        self.assertEqual(J._volatility_tier(6.0, 100.0), "mid")       # 6% 边界
        self.assertEqual(J._volatility_tier(6.1, 100.0), "high")
        self.assertEqual(J._volatility_tier(1.0, 0.0), "mid")         # price<=0 旧行为不变

    def test_atr_ratio_non_finite_returns_nan_and_logs(self):
        with self.assertLogs("fx.judges", level="WARNING"):
            self.assertTrue(math.isnan(J._atr_ratio(float("inf"), 100.0)))
        with self.assertLogs("fx.judges", level="WARNING"):
            self.assertTrue(math.isnan(J._atr_ratio(float("nan"), 100.0)))
        self.assertAlmostEqual(J._atr_ratio(2.0, 100.0), 2.0)
        self.assertEqual(J._atr_ratio(2.0, 0.0), 0.0)


class TestSetCooldownSafe(unittest.TestCase):
    def test_write_failure_logs_warning_not_silent(self):
        with mock.patch("shared.state.set_cooldown", side_effect=OSError("disk full")), \
             self.assertLogs("fx.judges", level="WARNING") as cm:
            J._set_cooldown_safe("XUSDT", 2.0, "r")                 # 不抛异常
        self.assertTrue(any("冷却写入失败" in m and "XUSDT" in m and "disk full" in m for m in cm.output))

    def test_success_no_warning(self):
        with mock.patch("shared.state.set_cooldown") as sc:
            J._set_cooldown_safe("XUSDT", 2.0, "r")
        sc.assert_called_once_with("XUSDT", 2.0, "r")


class TestCfgFallback(unittest.TestCase):
    def test_import_error_is_info_and_other_is_error(self):
        with self.assertLogs("fx.judges", level="INFO") as cm:
            J._cfg_fallback("G", ImportError("cannot import name X"), {"a": 1})
        self.assertTrue(cm.output[0].startswith("INFO"))
        self.assertIn("{'a': 1}", cm.output[0])
        with self.assertLogs("fx.judges", level="INFO") as cm:
            J._cfg_fallback("G", SyntaxError("bad"), {"a": 1})
        self.assertTrue(cm.output[0].startswith("ERROR"))
        self.assertIn("{'a': 1}", cm.output[0])


if __name__ == "__main__":
    unittest.main()
