"""运行：cd fx && python3 -m unittest tests.test_opt_v387_step4 -v
[v3.87 STEP4-2] TP1 截断后收紧止损：最小距离按波动层。"""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.calc_sl_tp import _tighten_sl_after_cap


class TestTightenSlAfterCap(unittest.TestCase):
    def test_mid_ok_when_distance_ge_3pct(self):
        # entry 100, 原止损 90(10%), tp1 cap 在 112, rr_min 2.0 → 需要止损距离 6%
        sl, pct = _tighten_sl_after_cap(100, 90, 112, "LONG", 2.0, "mid")
        self.assertAlmostEqual(sl, 94.0)
        self.assertGreaterEqual(pct, 0.03)

    def test_mid_rejects_between_2_5_and_3pct(self):
        # tp1 距离 5.4% → 需要止损 2.7%：低波动层可接受，中波动层应拒绝
        sl_mid, _ = _tighten_sl_after_cap(100, 90, 105.4, "LONG", 2.0, "mid")
        sl_low, _ = _tighten_sl_after_cap(100, 90, 105.4, "LONG", 2.0, "low")
        self.assertIsNone(sl_mid)
        self.assertAlmostEqual(sl_low, 97.3)

    def test_high_requires_5pct(self):
        sl, _ = _tighten_sl_after_cap(100, 85, 109, "LONG", 1.5, "high")  # 需要 6%
        self.assertAlmostEqual(sl, 94.0)
        sl2, _ = _tighten_sl_after_cap(100, 85, 106, "LONG", 1.5, "high")  # 需要 4% < 5%
        self.assertIsNone(sl2)

    def test_not_wider_than_original(self):
        # 原止损只有 4%，收紧目标 6% 反而更宽 → 不允许
        sl, _ = _tighten_sl_after_cap(100, 96, 112, "LONG", 2.0, "mid")
        self.assertIsNone(sl)

    def test_short_symmetry(self):
        sl, pct = _tighten_sl_after_cap(100, 110, 88, "SHORT", 2.0, "mid")
        self.assertAlmostEqual(sl, 106.0)
        self.assertGreaterEqual(pct, 0.03)


if __name__ == "__main__":
    unittest.main()
