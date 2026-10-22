"""运行：cd fx && python3 -m unittest tests.test_opt_v387 -v"""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.opt_v387 import invalidation_sl, no_new_extreme


def k(h, l):
    return [0, 0, h, l, 0]


class TestInvalidationSL(unittest.TestCase):
    def test_long_structure(self):
        bars = [k(105, 98)] + [k(104, 100)] * 7
        self.assertAlmostEqual(invalidation_sl(101, 2, "LONG", bars), 98 - 0.6)

    def test_long_floor(self):
        bars = [k(102, 100.5)] * 8
        self.assertAlmostEqual(invalidation_sl(101, 2, "LONG", bars), 99.0)

    def test_long_too_far(self):
        bars = [k(102, 90)] * 8
        self.assertIsNone(invalidation_sl(101, 2, "LONG", bars))

    def test_short_structure(self):
        bars = [k(103, 99)] * 8
        self.assertAlmostEqual(invalidation_sl(100, 2, "SHORT", bars), 103.6)


class TestNoNewExtreme(unittest.TestCase):
    def test_long_ok(self):
        self.assertTrue(no_new_extreme([k(10, 5), k(10, 6), k(10, 6), k(10, 5.5)], "LONG"))

    def test_long_new_low(self):
        self.assertFalse(no_new_extreme([k(10, 5), k(10, 6), k(10, 6), k(10, 4)], "LONG"))

    def test_short_new_high(self):
        self.assertFalse(no_new_extreme([k(10, 5), k(9, 5), k(9, 5), k(11, 5)], "SHORT"))


if __name__ == "__main__":
    unittest.main()
