"""运行：cd fx && python3 -m unittest tests.test_opt_v385 -v"""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.opt_v385 import symbol_funding_window as f


class T(unittest.TestCase):
    def test_before(self):
        self.assertTrue(f(1000 - 600, 1000, 4, 900, 300)[0])

    def test_after_4h(self):
        nxt = 100000
        self.assertTrue(f(nxt - 4 * 3600 + 120, nxt, 4, 900, 300)[0])

    def test_outside(self):
        self.assertFalse(f(1000 - 3600, 1000, 8, 900, 300)[0])

    def test_missing(self):
        self.assertFalse(f(1000, 0, 8, 900, 300)[0])


if __name__ == "__main__":
    unittest.main()
