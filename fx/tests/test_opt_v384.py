"""
tests/test_opt_v384.py — v3.84 第一轮优化的纯函数单测
运行：cd fx && python3 -m unittest tests.test_opt_v384 -v   （不依赖 pytest 与网络）
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.opt_v384 import (stagnation_loss_threshold, tp1_protect_action,
                             pullback_entry_params)


class TestStagnation(unittest.TestCase):
    def test_default_floor(self):
        # 止损 3% → 0.6×3%=1.8% < 2% 下限 → 2%（与原固定值一致）
        self.assertAlmostEqual(stagnation_loss_threshold(0.03), 0.02)

    def test_wide_stop_scales(self):
        # 止损 5% → 0.6×5%=3%
        self.assertAlmostEqual(stagnation_loss_threshold(0.05), 0.03)

    def test_missing_sl_falls_back(self):
        self.assertAlmostEqual(stagnation_loss_threshold(0.0), 0.02)
        self.assertAlmostEqual(stagnation_loss_threshold(None), 0.02)

    def test_disabled_equals_legacy(self):
        self.assertAlmostEqual(stagnation_loss_threshold(0.05, enabled=False), 0.02)


class TestTp1Protect(unittest.TestCase):
    def test_long_below_entry_closes(self):
        self.assertEqual(tp1_protect_action("LONG", 99.0, 100.0, -0.01), "close")

    def test_short_above_entry_closes(self):
        self.assertEqual(tp1_protect_action("SHORT", 101.0, 100.0, -0.01), "close")

    def test_sl_already_breakeven_noop(self):
        self.assertEqual(tp1_protect_action("LONG", 100.1, 100.0, -0.01), "none")
        self.assertEqual(tp1_protect_action("SHORT", 99.9, 100.0, -0.01), "none")

    def test_still_profitable_moves_sl(self):
        self.assertEqual(tp1_protect_action("LONG", 99.0, 100.0, 0.01), "move_sl")

    def test_disabled_keeps_legacy_move(self):
        self.assertEqual(tp1_protect_action("LONG", 99.0, 100.0, -0.01, enabled=False), "move_sl")

    def test_bad_direction(self):
        self.assertEqual(tp1_protect_action("", 99.0, 100.0, -0.01), "none")


class TestPullbackParams(unittest.TestCase):
    def test_symmetric(self):
        p = pullback_entry_params(True)
        self.assertEqual(p["long_deg"], p["short_deg"])
        self.assertEqual(p["long_min"], p["short_min"])
        self.assertEqual(p["long_max"], p["short_max"])

    def test_legacy_values(self):
        p = pullback_entry_params(False)
        self.assertEqual((p["long_deg"], p["short_deg"]), (0.3, 0.8))


if __name__ == "__main__":
    unittest.main()
