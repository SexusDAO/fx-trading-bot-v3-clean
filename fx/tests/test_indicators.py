"""运行：cd fx && python3 -m unittest tests.test_indicators -v
[v3.89 #68/#69/#70] 指标边界：ATR 样本不足、RSI 全平序列、零成交量 VP。"""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.indicators import atr, atr_status, rsi, calc_volume_profile


def k(o, h, lo, c, v=1.0, t=0):
    return [t, str(o), str(h), str(lo), str(c), str(v)]


class TestAtr(unittest.TestCase):
    def test_insufficient_samples_flagged_and_not_divided_by_full_period(self):
        ks = [k(100, 101, 99, 100)] + [k(100, 102, 98, 100) for _ in range(4)]   # 4 个有效 TR，每个 = 4
        st = atr_status(ks, 14)
        self.assertEqual(st["samples"], 4)
        self.assertFalse(st["sufficient"])
        self.assertAlmostEqual(st["value"], 4.0)            # 按实际 4 个平均，而不是 16/14≈1.14
        self.assertAlmostEqual(atr(ks, 14), 4.0)

    def test_sufficient_samples_unchanged(self):
        ks = [k(100, 101, 99, 100)] + [k(100, 102, 98, 100) for _ in range(20)]
        st = atr_status(ks, 14)
        self.assertTrue(st["sufficient"])
        self.assertEqual(st["samples"], 14)
        self.assertAlmostEqual(atr(ks, 14), 4.0)

    def test_empty_and_single(self):
        self.assertEqual(atr([], 14), 0.0)
        self.assertEqual(atr([k(1, 1, 1, 1)], 14), 0.0)
        self.assertFalse(atr_status([], 14)["sufficient"])


class TestRsi(unittest.TestCase):
    def test_flat_series_is_neutral_50(self):
        self.assertEqual(rsi([100.0] * 30), 50.0)           # 原来是 100

    def test_only_gains_is_100(self):
        self.assertEqual(rsi([float(i) for i in range(1, 31)]), 100.0)

    def test_only_losses_is_0(self):
        self.assertEqual(rsi([float(i) for i in range(30, 0, -1)]), 0.0)

    def test_short_series_still_50(self):
        self.assertEqual(rsi([1.0, 2.0]), 50.0)


class TestVolumeProfile(unittest.TestCase):
    def _ks(self, vol):
        return [k(100, 100 + i % 5, 98 - i % 3, 100, vol) for i in range(60)]

    def test_zero_volume_invalid(self):
        r = calc_volume_profile(self._ks(0.0))
        self.assertIs(r["valid"], False)
        self.assertEqual((r["poc"], r["vah"], r["val"]), (0.0, 0.0, 0.0))

    def test_negative_volume_treated_as_no_evidence(self):
        self.assertIs(calc_volume_profile(self._ks(-5.0))["valid"], False)

    def test_normal_volume_valid(self):
        r = calc_volume_profile(self._ks(10.0))
        self.assertIs(r["valid"], True)
        self.assertLessEqual(r["val"], r["poc"])
        self.assertLessEqual(r["poc"], r["vah"])


if __name__ == "__main__":
    unittest.main()
