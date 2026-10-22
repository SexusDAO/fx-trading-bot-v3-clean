"""运行：cd fx && python3 -m unittest tests.test_opt_v387_step6 -v
[v3.87 STEP6] 止损数量取整、入场复核规则。"""
import asyncio, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import executor as ex


class TestRoundSumQty(unittest.TestCase):
    def test_float_noise_removed(self):
        self.assertEqual(ex._round_sum_qty(0.1, 0.2, 0.1), 0.3)
        self.assertEqual(str(ex._round_sum_qty(0.1, 0.2, 0.1)), "0.3")

    def test_integer_step(self):
        self.assertEqual(ex._round_sum_qty(60.0, 40.0, 1.0), 100.0)

    def test_fine_step(self):
        self.assertEqual(str(ex._round_sum_qty(0.006, 0.004, 0.001)), "0.01")


class TestRoundQty(unittest.TestCase):
    """[v3.87 STEP6-5] floor 前先消除浮点噪声，边界值不再少一个步长。"""
    def test_boundary_0_3_over_0_1(self):
        self.assertEqual(ex._round_qty(0.3, 0.1), 0.3)

    def test_other_float_boundaries(self):
        self.assertEqual(ex._round_qty(0.7, 0.1), 0.7)
        self.assertEqual(ex._round_qty(1.1, 0.1), 1.1)
        self.assertEqual(ex._round_qty(0.29, 0.01), 0.29)

    def test_still_rounds_down(self):
        self.assertEqual(ex._round_qty(0.39, 0.1), 0.3)
        self.assertEqual(ex._round_qty(123.456, 0.01), 123.45)
        self.assertEqual(ex._round_qty(5.9, 1.0), 5.0)

    def test_never_exceeds_input(self):
        for q in (0.3, 0.6, 1.2, 0.07, 12.34):
            self.assertLessEqual(ex._round_qty(q, 0.01), q + 1e-12)

    def test_zero_step_fallback(self):
        self.assertEqual(ex._round_qty(1.23456, 0), 1.235)


class TestPositionManagerRoundQty(unittest.TestCase):
    """[v3.87 STEP7-1] position_manager._round_qty 与 executor 保持一致。"""
    def test_boundary_and_consistency(self):
        import position_manager as pm
        self.assertEqual(pm._round_qty(0.3, 0.1), 0.3)
        for q, st in ((0.7, 0.1), (1.1, 0.1), (0.29, 0.01), (123.456, 0.01), (5.9, 1.0), (0.39, 0.1)):
            self.assertEqual(pm._round_qty(q, st), ex._round_qty(q, st))


class TestEntryRecheck(unittest.TestCase):
    def _run(self, direction, mode, snap):
        return asyncio.run(ex._entry_recheck("XUSDT", direction, mode, 100.0, 1.0, snap, {}))

    def test_short_pump_rule_removed(self):
        snap = {"funding_rate": 0.0, "vol_ratio": 5.0, "entry_price_snap": 90.0, "prev_close": 90.0}
        verdict, _ = self._run("SHORT", "pullback", snap)
        self.assertEqual(verdict, "HOLD")

    def test_extreme_funding_still_closes(self):
        self.assertEqual(self._run("LONG", "pullback", {"funding_rate": 0.006})[0], "CLOSE")
        self.assertEqual(self._run("SHORT", "pullback", {"funding_rate": -0.006})[0], "CLOSE")

    def test_normal_holds(self):
        self.assertEqual(self._run("LONG", "breakout", {"funding_rate": 0.0001})[0], "HOLD")


if __name__ == "__main__":
    unittest.main()
