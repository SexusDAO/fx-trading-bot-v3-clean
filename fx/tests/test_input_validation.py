"""运行：cd fx && python3 -m unittest tests.test_input_validation -v
[v3.89 #71/#73/#81/#82] 指标周期校验、opt_v384/387/388 非法输入、仓位模块上限冲突与参数校验。
合法输入的结果与旧版一致（用固定数值回归），只验证非法输入的新行为。"""
import logging, math, sys, unittest
from pathlib import Path
_fx = Path(__file__).parent.parent
sys.path.insert(0, str(_fx)); sys.path.insert(0, str(_fx.parent))
from shared import indicators as I
from shared import opt_v384 as O4, opt_v387 as O7, opt_v388 as O8

NAN, INF = float("nan"), float("inf")


def _k(i, o=100.0, h=101.0, l=99.0, c=100.0, v=10.0):
    return [i * 3600_000, str(o), str(h), str(l), str(c), str(v), i * 3600_000 + 3599_999]


class TestIndicatorParams(unittest.TestCase):
    BAD = [0, -1, 1.5, 20.5, 0.0, -2.0, NAN, INF, None, "14", True]

    def test_bad_period_raises(self):
        closes = [float(i) for i in range(1, 60)]
        kl = [_k(i) for i in range(60)]
        for n in self.BAD:
            with self.subTest(n=n):
                for fn in (I.ema, I.rsi, I.bb_pct, I.bollinger):
                    with self.assertRaises(ValueError):
                        fn(closes, n)
                with self.assertRaises(ValueError):
                    I.atr(kl, n)
                with self.assertRaises(ValueError):
                    I.calc_volume_profile(kl, n)

    def test_integral_float_accepted_same_result_as_int(self):
        """配置里写成 20.0 的周期视为合法，结果与整数 20 完全一致。"""
        closes = [float(i % 7 + i * 0.1) for i in range(1, 80)]
        kl = [_k(i, h=101 + i % 5, l=99 - i % 3) for i in range(80)]
        self.assertEqual(I.ema(closes, 20.0), I.ema(closes, 20))
        self.assertEqual(I.rsi(closes, 14.0), I.rsi(closes, 14))
        self.assertEqual(I.bb_pct(closes, 20.0), I.bb_pct(closes, 20))
        self.assertEqual(I.bollinger(closes, 20.0), I.bollinger(closes, 20))
        self.assertEqual(I.atr(kl, 14.0), I.atr(kl, 14))
        self.assertEqual(I.calc_volume_profile(kl, 40.0), I.calc_volume_profile(kl, 40))
        self.assertEqual(I._require_positive_int(20.0, "n"), 20)
        self.assertIsInstance(I._require_positive_int(20.0, "n"), int)

    def test_legal_params_unchanged(self):
        closes = [float(i) for i in range(1, 60)]
        self.assertAlmostEqual(I.ema([1.0, 2.0, 3.0], 2), 2.5)
        self.assertEqual(I.rsi(closes, 14), 100.0)
        self.assertEqual(I.rsi([1.0, 2.0], 14), 50.0)            # 数据不足仍返回 50
        self.assertEqual(I.ema([5.0], 20), 5.0)                  # 数据不足仍返回最后值


class TestOptV384(unittest.TestCase):
    def test_stagnation_nan_falls_back_to_floor(self):
        self.assertEqual(O4.stagnation_loss_threshold(NAN, True, 0.5, 0.02), 0.02)
        self.assertEqual(O4.stagnation_loss_threshold(INF, True, 0.5, 0.02), 0.02)
        self.assertEqual(O4.stagnation_loss_threshold("x", True, 0.5, 0.02), 0.02)
        self.assertEqual(O4.stagnation_loss_threshold(0.10, True, 0.5, 0.02), 0.05)    # 合法输入不变

    def test_tp1_protect_nan_is_none(self):
        for bad in (NAN, INF, None):
            self.assertEqual(O4.tp1_protect_action("LONG", 99.0, 100.0, bad, True), "none")
            self.assertEqual(O4.tp1_protect_action("LONG", bad, 100.0, 0.5, True), "none")

    def test_tp1_protect_legal_unchanged(self):
        # 与旧版一致：止损在亏损侧 + 浮盈为正 -> move_sl；浮盈为负且启用 -> close；未启用 -> move_sl；止损已在保本侧 -> none
        self.assertEqual(O4.tp1_protect_action("LONG", 99.0, 100.0, 0.5, True), "move_sl")
        self.assertEqual(O4.tp1_protect_action("LONG", 99.0, 100.0, -0.5, True), "close")
        self.assertEqual(O4.tp1_protect_action("LONG", 99.0, 100.0, -0.5, False), "move_sl")
        self.assertEqual(O4.tp1_protect_action("LONG", 100.5, 100.0, 0.5, True), "none")
        self.assertEqual(O4.tp1_protect_action("SHORT", 101.0, 100.0, -0.5, True), "close")

    def test_pullback_params_invalid_range_falls_back(self):
        legacy = O4.pullback_entry_params(False, 0.5, 0.2, 1.0)
        for args in ((True, 0.5, 1.0, 0.2), (True, NAN, 0.2, 1.0), (True, 0.5, -0.1, 1.0), (True, 2.0, 0.2, 1.0)):
            with self.subTest(args=args):
                self.assertEqual(O4.pullback_entry_params(*args), legacy)
        self.assertNotEqual(O4.pullback_entry_params(True, 0.5, 0.2, 1.0), legacy)   # 合法输入仍启用


class TestOptV387(unittest.TestCase):
    BARS = [_k(i, h=101 + i * 0.01, l=99 + i * 0.01) for i in range(10)]

    def test_invalidation_sl_legal_value(self):
        sl = O7.invalidation_sl(100.0, 1.0, "LONG", self.BARS)
        self.assertIsNotNone(sl)
        self.assertLess(sl, 100.0)

    def test_invalid_inputs_return_none(self):
        B = self.BARS
        cases = [
            dict(entry=NAN, atr=1.0), dict(entry=100.0, atr=INF), dict(entry=100.0, atr=1.0, lookback=0),
            dict(entry=100.0, atr=1.0, lookback=-3), dict(entry=100.0, atr=1.0, lookback=2.5),
            dict(entry=100.0, atr=1.0, min_atr=3.0, max_atr=1.0), dict(entry=100.0, atr=1.0, buf_atr=NAN),
        ]
        for c in cases:
            with self.subTest(c=c):
                kw = {k: v for k, v in c.items() if k not in ("entry", "atr")}
                self.assertIsNone(O7.invalidation_sl(c["entry"], c["atr"], "LONG", B, **kw))

    def test_bad_kline_shape_returns_none(self):
        self.assertIsNone(O7.invalidation_sl(100.0, 1.0, "LONG", [[1, 2]] * 8))
        self.assertIsNone(O7.invalidation_sl(100.0, 1.0, "LONG", [_k(0, h="abc")] * 8))
        self.assertIsNone(O7.invalidation_sl(100.0, 1.0, "LONG", [_k(0, h=99, l=101)] * 8))   # 高 < 低

    def test_no_new_extreme_bad_inputs_false_and_legal_unchanged(self):
        up = [_k(i, h=101, l=99 + i * 0.1) for i in range(5)]
        self.assertTrue(O7.no_new_extreme(up, "LONG", 3))
        for n in (0, -1, 1.5, True):
            self.assertFalse(O7.no_new_extreme(up, "LONG", n))
        self.assertFalse(O7.no_new_extreme([[1, 2]] * 6, "LONG", 3))
        self.assertFalse(O7.no_new_extreme([_k(i, l=NAN) for i in range(5)], "LONG", 3))


class TestOptV388(unittest.TestCase):
    def test_relative_strength(self):
        self.assertEqual(O8.liq_relative_strength(50.0, 500.0), 0.1)
        for a, v in ((NAN, 1.0), (1.0, NAN), (INF, 1.0), (1.0, INF), (0, 1.0), (1.0, 0), ("x", 1.0)):
            self.assertIsNone(O8.liq_relative_strength(a, v))

    def test_liq_weight_nan_returns_base_not_wmax(self):
        self.assertEqual(O8.liq_weight(NAN), 2.0)
        self.assertEqual(O8.liq_weight(INF), 2.0)
        self.assertEqual(O8.liq_weight(0.1, ref_ratio=NAN), 2.0)
        self.assertEqual(O8.liq_weight(0.1, w_min=3.0, w_max=0.5), 2.0)    # 上下界反向
        self.assertEqual(O8.liq_weight(0.1), 2.0)                           # 合法：base*0.1/0.1
        self.assertEqual(O8.liq_weight(1.0), 3.0)                           # 合法：截断到 w_max
        self.assertEqual(O8.liq_weight(0.001), 0.5)                         # 合法：截断到 w_min

    def test_tp1_buffer_nan_returns_legacy(self):
        self.assertEqual(O8.tp1_buffer_atr(NAN), 0.30)
        self.assertEqual(O8.tp1_buffer_atr(INF), 0.30)
        self.assertEqual(O8.tp1_buffer_atr(0.25), 0.20)
        self.assertEqual(O8.tp1_buffer_atr(0.10), 0.35)
        self.assertEqual(O8.tp1_buffer_atr(0.01), 0.50)


class TestPositionSizeReal(unittest.TestCase):
    def setUp(self):
        import calc_position_size_real as P
        self.P = P
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def test_dynamic_cap_invalid_params_raise(self):
        P = self.P
        for mp in (0, -1, 2.5, True):
            with self.subTest(mp=mp), self.assertRaises(ValueError):
                P._dynamic_cap(1000.0, 2000.0, mp)
        for nv in (0, -5.0, NAN, INF, True, "1000"):
            with self.subTest(nv=nv), self.assertRaises(ValueError):
                P._dynamic_cap(nv, 2000.0, 5)

    def test_dynamic_cap_legal_unchanged(self):
        # 1000 / 5 * 5 = 1000；30% 上限 = 1500；硬顶 2000 -> 1000
        self.assertEqual(self.P._dynamic_cap(1000.0, 2000.0, 5), 1000.0)
        self.assertEqual(self.P._dynamic_cap(1000.0, 400.0, 5), 400.0)

    def test_min_position_conflicts_with_cap_returns_zero(self):
        # 本金 100U：动态上限 100*... < POS_MIN_U(125) -> 不可执行，不再静默突破上限
        d = {"net_value": 100.0, "max_positions": 5, "tier": "L1", "symbol": "XUSDT"}
        cap = self.P._dynamic_cap(100.0, 2000.0, 5)
        self.assertLess(cap, self.P.POS_MIN_U)
        self.assertEqual(self.P.real_size("trend", 85, "LONG", d), 0.0)

    def test_normal_size_unchanged_when_cap_ok(self):
        d = {"net_value": 1000.0, "max_positions": 5, "tier": "L1", "symbol": "XUSDT"}
        v = self.P.real_size("trend", 85, "LONG", d)
        self.assertGreaterEqual(v, self.P.POS_MIN_U)
        self.assertLessEqual(v, self.P._dynamic_cap(1000.0, 2000.0, 5))


if __name__ == "__main__":
    unittest.main()
