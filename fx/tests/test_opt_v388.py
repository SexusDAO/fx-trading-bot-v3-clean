"""运行：cd fx && python3 -m unittest tests.test_opt_v388 -v"""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.opt_v388 import liq_relative_strength, liq_weight, tp1_buffer_atr
import shared.calc_sl_tp as cst


class TestRelativeStrength(unittest.TestCase):
    def test_btc_vs_alt(self):
        self.assertAlmostEqual(liq_relative_strength(5e6, 3e9), 5e6 / 3e9)
        self.assertAlmostEqual(liq_relative_strength(5e6, 5e6), 1.0)

    def test_missing_returns_none(self):
        self.assertIsNone(liq_relative_strength(0, 1e6))
        self.assertIsNone(liq_relative_strength(1e6, 0))
        self.assertIsNone(liq_relative_strength(None, None))


class TestWeight(unittest.TestCase):
    def test_examples(self):
        self.assertAlmostEqual(liq_weight(0.05), 1.0)
        self.assertAlmostEqual(liq_weight(0.10), 2.0)
        self.assertAlmostEqual(liq_weight(0.30), 3.0)
        self.assertAlmostEqual(liq_weight(0.001), 0.5)

    def test_unknown_keeps_original(self):
        self.assertEqual(liq_weight(None), 2.0)


class TestTp1Buffer(unittest.TestCase):
    def test_tiers(self):
        self.assertEqual(tp1_buffer_atr(0.25), 0.20)
        self.assertEqual(tp1_buffer_atr(0.20), 0.20)
        self.assertEqual(tp1_buffer_atr(0.10), 0.35)
        self.assertEqual(tp1_buffer_atr(0.05), 0.35)
        self.assertEqual(tp1_buffer_atr(0.01), 0.50)

    def test_unknown_keeps_legacy(self):
        self.assertEqual(tp1_buffer_atr(None), 0.30)


class TestFlagOffUnchanged(unittest.TestCase):
    """开关关闭时，回踩入场价与 TP1 必须与原公式完全一致。"""
    def test_entry_and_tp_unchanged(self):
        import config
        self.assertFalse(config.OPT_LIQ_RELATIVE_STRENGTH)
        k = [[0, 100, 101, 99, 100, 1000, 0, 0, 0, 0, 0, 0] for _ in range(30)]
        liq_long = [{"price_low": 97.0, "price_high": 98.0, "liq_usd": 9e6}]
        liq_short = [{"price_low": 110.0, "price_high": 111.0, "liq_usd": 9e6}]
        a = cst._entry_pullback(100.0, 2.0, 99.0, liq_long, liq_short, "LONG", k, vol24_usd=1e6)
        b = cst._entry_pullback(100.0, 2.0, 99.0, liq_long, liq_short, "LONG", k)
        self.assertEqual(a, b)
        t1 = cst._calc_tp(100.0, 96.0, 2.0, "LONG", liq_long, liq_short, vol24_usd=1e6)
        t2 = cst._calc_tp(100.0, 96.0, 2.0, "LONG", liq_long, liq_short)
        self.assertEqual(t1, t2)
        self.assertAlmostEqual(t1[0], 110.0 - 2.0 * 0.3)


if __name__ == "__main__":
    unittest.main()


class TestDenominators(unittest.TestCase):
    """[v3.88 OPT-14d] 三种分母与记录字段。"""
    def test_concentration(self):
        from shared.opt_v388 import liq_concentration
        zones = [{"liq_usd": 3e6}, {"liq_usd": 1e6}]
        self.assertAlmostEqual(liq_concentration(3e6, zones), 0.75)
        self.assertEqual(liq_concentration(5e4, [{"liq_usd": 5e4}]), 1.0)   # 单区恒为 1，已在文档注明
        self.assertIsNone(liq_concentration(1e6, []))

    def test_oi_requires_usd(self):
        from shared.opt_v388 import oi_relative
        self.assertAlmostEqual(oi_relative(1e6, 2e7), 0.05)
        self.assertIsNone(oi_relative(1e6, 0))

    def test_relative_by_denom(self):
        from shared.opt_v388 import relative_by_denom
        z = [{"liq_usd": 1e6}, {"liq_usd": 1e6}]
        self.assertAlmostEqual(relative_by_denom("vol24", 1e6, z, 1e7, 4e7), 0.1)
        self.assertAlmostEqual(relative_by_denom("oi", 1e6, z, 1e7, 4e7), 0.025)
        self.assertAlmostEqual(relative_by_denom("concentration", 1e6, z, 1e7, 4e7), 0.5)

    def test_metrics_record(self):
        from shared.opt_v388 import liq_metrics
        z = {"liq_usd": 2e6}
        r = liq_metrics(z, [z, {"liq_usd": 2e6}], 1e7, 4e7)
        self.assertEqual(r["rel_vol24"], 0.2)
        self.assertEqual(r["rel_oi"], 0.05)
        self.assertEqual(r["concentration"], 0.5)
        self.assertEqual(liq_metrics(None, [], 1, 1), {})


class TestTp1Lock(unittest.TestCase):
    """[v3.88 OPT-14e] 开关开启时，重算带入锁定缓冲则沿用；记录实际采用的缓冲。"""
    def setUp(self):
        import config
        self.cfg = config
        self.orig = config.OPT_LIQ_RELATIVE_STRENGTH
        config.OPT_LIQ_RELATIVE_STRENGTH = True

    def tearDown(self):
        self.cfg.OPT_LIQ_RELATIVE_STRENGTH = self.orig

    def test_locked_buffer_used(self):
        liq_short = [{"price_low": 110.0, "price_high": 111.0, "liq_usd": 9e6}]
        ctx = {"tp1_buffer_locked": 0.2}
        tp1, _, deg = cst._calc_tp(100.0, 96.0, 2.0, "LONG", [], liq_short, liq_ctx=ctx)
        self.assertAlmostEqual(tp1, 110.0 - 0.4)
        self.assertFalse(deg)
        self.assertEqual(ctx["tp1_buffer_used"], 0.2)

    def test_dynamic_buffer_recorded(self):
        liq_short = [{"price_low": 110.0, "price_high": 111.0, "liq_usd": 3e6}]
        ctx = {"vol24_usd": 1e7, "oi_usd": 0.0}           # 相对强度 30% → 强档 0.2ATR
        tp1, _, _ = cst._calc_tp(100.0, 96.0, 2.0, "LONG", [], liq_short, liq_ctx=ctx)
        self.assertAlmostEqual(tp1, 110.0 - 0.4)
        self.assertEqual(ctx["tp1_buffer_used"], 0.2)

    def test_missing_data_falls_back(self):
        liq_short = [{"price_low": 110.0, "price_high": 111.0, "liq_usd": 3e6}]
        ctx = {}
        tp1, _, _ = cst._calc_tp(100.0, 96.0, 2.0, "LONG", [], liq_short, liq_ctx=ctx)
        self.assertAlmostEqual(tp1, 110.0 - 0.6)


class TestSingleZoneFlag(unittest.TestCase):
    """[v3.88 OPT-14d] 单区标记，且三种分母互相独立。"""
    def test_flag(self):
        from shared.opt_v388 import liq_metrics
        z = {"liq_usd": 1e6}
        self.assertFalse(liq_metrics(z, [z], 1e7, 4e7)["concentration_valid"])
        self.assertTrue(liq_metrics(z, [z, z], 1e7, 4e7)["concentration_valid"])

    def test_independent(self):
        from shared.opt_v388 import liq_metrics
        z = {"liq_usd": 1e6}
        r = liq_metrics(z, [z, z], 0, 4e7)        # 成交额缺失
        self.assertIsNone(r["rel_vol24"])
        self.assertEqual(r["rel_oi"], 0.025)
        self.assertEqual(r["concentration"], 0.5)
        r = liq_metrics(z, [z, z], 1e7, 0)        # OI 缺失
        self.assertIsNone(r["rel_oi"])
        self.assertEqual(r["rel_vol24"], 0.1)


class TestGates(unittest.TestCase):
    """[v3.88 OPT-14f/g] 分歧度门控与数据降级回退。"""
    def test_degraded_forces_base(self):
        from shared.opt_v388 import liq_weight_gated
        self.assertEqual(liq_weight_gated(0.5, 0.1, 0.5, 3.0, 100, 100, 1, degraded=True), 2.0)

    def test_divergence_caps(self):
        from shared.opt_v388 import liq_weight_gated
        # 强度很高本应到 3.0；偏离 2 ATR > 1.5 → 压到 2.0
        self.assertEqual(liq_weight_gated(0.5, 0.1, 0.5, 3.0, 102, 100, 1), 2.0)

    def test_no_trigger_keeps_clamp(self):
        from shared.opt_v388 import liq_weight_gated
        self.assertEqual(liq_weight_gated(0.5, 0.1, 0.5, 3.0, 100.5, 100, 1), 3.0)
        self.assertEqual(liq_weight_gated(0.01, 0.1, 0.5, 3.0, 100.5, 100, 1), 0.5)

    def test_low_weight_not_raised_by_gate(self):
        from shared.opt_v388 import liq_weight_gated
        self.assertEqual(liq_weight_gated(0.01, 0.1, 0.5, 3.0, 110, 100, 1), 0.5)

    def test_missing_rel_or_atr(self):
        from shared.opt_v388 import liq_weight_gated
        self.assertEqual(liq_weight_gated(None, 0.1, 0.5, 3.0, 110, 100, 1), 2.0)
        self.assertEqual(liq_weight_gated(0.5, 0.1, 0.5, 3.0, 110, 100, 0), 3.0)

    def test_src_level(self):
        from shared import liq_cache as lc
        import time as _t
        # [v3.89 #37] 超过 2×TTL 的记录不参与来源判定，测试数据需带当前 ts
        lc._CACHE["XTEST:3d"] = {"src_level": 1, "ts": _t.time()}
        lc._CACHE["XTEST:7d"] = {"src_level": 3, "ts": _t.time()}
        self.assertEqual(lc.get_liq_src_level("XTEST"), 3)
        self.assertEqual(lc.get_liq_src_level("NOPE"), 0)
        lc._CACHE.pop("XTEST:3d"); lc._CACHE.pop("XTEST:7d")


class TestOidAllBull(unittest.TestCase):
    """[v3.88 OPT-15] oi_divergence 近5根全同向拒绝。"""
    def _k(self, bull=True, n=8):
        out = []
        for i in range(n):
            o = 100 + i; c = o + 0.5 if bull else o - 0.5
            out.append([0, o, o + 1, o - 1, c, 1000])
        return out

    def _sig(self, d):
        return {"symbol": "TESTUSDT", "direction": d, "mode": "oi_divergence", "volatility_tier": "mid", "ind": {}}

    def test_short_all_bull_rejected(self):
        r = cst.calc_sl_tp(self._sig("SHORT"), 108.0, 1.0, {}, self._k(True), 105.0)
        self.assertIn("error", r)

    def test_long_all_bear_rejected(self):
        r = cst.calc_sl_tp(self._sig("LONG"), 108.0, 1.0, {}, self._k(False), 105.0)
        self.assertIn("error", r)

    def test_flag_off_restores_fallback(self):
        import config
        orig = config.OPT_OID_REJECT_ALL_BULL
        config.OPT_OID_REJECT_ALL_BULL = False
        try:
            r = cst.calc_sl_tp(self._sig("SHORT"), 108.0, 1.0, {}, self._k(True), 105.0)
            self.assertFalse(r.get("error", "").startswith("oi_divergence SHORT 近5根"))
        finally:
            config.OPT_OID_REJECT_ALL_BULL = orig


class TestRoundFloat(unittest.TestCase):
    """[v3.88 OPT-16] 价格/数量取整的浮点修复。"""
    def test_price_float_hazard(self):
        import executor, position_manager
        from shared import binance_api as ba
        for fn in (executor._round_price, position_manager._round_price):
            self.assertEqual(fn(0.3, 0.1), 0.3)        # 原写法得 0.2
            self.assertEqual(fn(1.1, 0.1), 1.1)
            self.assertEqual(fn(100.07, 0.1), 100.0)   # 正常向下取整不变
            self.assertEqual(fn(0.29999, 0.0001), 0.2999)
        self.assertEqual(ba._math.floor(round(0.3 / 0.1, 10)), 3)

    def test_flags_default_on(self):
        import config
        self.assertTrue(config.OPT_BREAKOUT_CLOSED_BAR)
        self.assertTrue(config.OPT_FUNDING_WINDOW_PER_SYMBOL)
