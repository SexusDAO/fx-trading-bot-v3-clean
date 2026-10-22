"""运行：cd fx && python3 -m unittest tests.test_tp_consistency_signed -v
[v3.89 #74] calc_tp_consistency：绝对距离字段与评分不变；新增有方向偏差字段（正=TP1 更近/保守）。"""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.momentum_score import calc_tp_consistency as C


class TestSigned(unittest.TestCase):
    def test_long_conservative_and_aggressive(self):
        c = C("LONG", dynamic_target=110.0, tp1_price=108.0, atr=1.0)       # TP1 比目标更近
        self.assertEqual(c["tp_vs_dynamic_dist"], 2.0)
        self.assertEqual(c["tp_vs_dynamic_signed_dist"], 2.0)
        self.assertEqual(c["tp_relative"], "conservative")
        a = C("LONG", 110.0, 113.0, 1.0)                                     # TP1 更远
        self.assertEqual(a["tp_vs_dynamic_dist"], 3.0)
        self.assertEqual(a["tp_vs_dynamic_signed_dist"], -3.0)
        self.assertEqual(a["tp_relative"], "aggressive")
        self.assertAlmostEqual(a["tp_vs_dynamic_signed_atr"], -3.0, places=2)

    def test_short_mirrors(self):
        c = C("SHORT", dynamic_target=90.0, tp1_price=92.0, atr=1.0)        # SHORT：TP1 高于目标 = 更近
        self.assertEqual(c["tp_vs_dynamic_signed_dist"], 2.0)
        self.assertEqual(c["tp_relative"], "conservative")
        a = C("SHORT", 90.0, 87.0, 1.0)
        self.assertEqual(a["tp_vs_dynamic_signed_dist"], -3.0)
        self.assertEqual(a["tp_relative"], "aggressive")

    def test_aligned(self):
        c = C("LONG", 100.0, 100.0, 1.0)
        self.assertEqual(c["tp_vs_dynamic_signed_dist"], 0.0)
        self.assertEqual(c["tp_relative"], "aligned")

    def test_original_fields_and_score_unchanged(self):
        for d, tgt, tp1, score in (("LONG", 110, 108, "low"), ("LONG", 110, 109.8, "high"), ("SHORT", 90, 90.8, "medium")):
            c = C(d, float(tgt), float(tp1), 1.0)
            self.assertGreaterEqual(c["tp_vs_dynamic_dist"], 0.0)           # 原字段仍是绝对值
            self.assertGreaterEqual(c["tp_vs_dynamic_atr"], 0.0)
            self.assertEqual(c["consistency_score"], score)
            self.assertEqual(c["tp_vs_dynamic_dist"], abs(c["tp_vs_dynamic_signed_dist"]))
        self.assertEqual(C("LONG", 110.0, 108.0, 1.0)["consistency_schema_version"], 2)


if __name__ == "__main__":
    unittest.main()
