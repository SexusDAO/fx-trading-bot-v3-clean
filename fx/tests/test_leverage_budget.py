"""运行：cd fx && python3 -m pytest tests/test_leverage_budget.py -v
[v3.90] 杠杆利用率预算：比例查表、used 计算（tp1_filled 计入 used 但不占槽位）、剩余预算截断/拒绝、
单仓上限、异常降级、TP1 成交后槽位与 used 的变化。全部 mock，不调用交易所。"""
import logging, math, sys, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
import config as C
from shared import risk_manager as RM
from shared import state as ST
import executor as EX


def pos(direction="LONG", qty=1.0, price=100.0, status="open", tp1=False, **kw):
    p = {"symbol": "XUSDT", "direction": direction, "quantity": qty, "quantity_remaining": qty,
         "entry_price": price, "status": status, "tp1_filled": tp1}
    p.update(kw)
    return p


class Base(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL); self.addCleanup(logging.disable, logging.NOTSET)

    def market(self, mode, phase="mid_bull", bear=None):
        for n, v in (("get_market_mode", mode), ("get_market_cycle_phase", phase), ("get_bear_strength", bear)):
            p = mock.patch.object(RM, n, return_value=v); p.start(); self.addCleanup(p.stop)

    def positions(self, plist):
        p = mock.patch.object(ST, "get_all_positions", return_value=plist); p.start(); self.addCleanup(p.stop)


class TestConfig(unittest.TestCase):
    def test_params_present_and_ratios_sum_to_one(self):
        self.assertTrue(C.LEVERAGE_UTILIZATION_ENABLED)
        self.assertEqual((C.LEVERAGE_TARGET, C.MAX_SINGLE_NOMINAL_RATIO), (5.0, 1.5))
        for k in ("BULL_EARLY", "BULL_MID", "BULL_LATE", "RANGE", "BEAR_WEAK", "BEAR_MODERATE", "BEAR_STRONG"):
            self.assertAlmostEqual(getattr(C, f"LEVERAGE_LONG_RATIO_{k}") + getattr(C, f"LEVERAGE_SHORT_RATIO_{k}"), 1.0, msg=k)


class TestRatioTable(Base):
    def test_bull_phases(self):
        for phase, (lo, sh) in {"early_bull": (0.75, 0.25), "mid_bull": (0.70, 0.30), "late_bull": (0.55, 0.45)}.items():
            self.market("bull", phase)
            self.assertEqual(RM._leverage_ratio("LONG"), lo, phase)
            self.assertEqual(RM._leverage_ratio("SHORT"), sh, phase)

    def test_bull_unknown_phase_falls_back_to_mid(self):
        for phase in ("range", "bear", "", None, "weird"):
            self.market("bull", phase)
            self.assertEqual(RM._leverage_ratio("LONG"), 0.70, phase)

    def test_range(self):
        self.market("range")
        self.assertEqual((RM._leverage_ratio("LONG"), RM._leverage_ratio("SHORT")), (0.5, 0.5))
        self.market("something_else")
        self.assertEqual(RM._leverage_ratio("LONG"), 0.5)

    def test_bear_strengths(self):
        for s, (lo, sh) in {"weak": (0.40, 0.60), "moderate": (0.30, 0.70), "strong": (0.20, 0.80)}.items():
            self.market("bear", bear=s)
            self.assertEqual((RM._leverage_ratio("LONG"), RM._leverage_ratio("SHORT")), (lo, sh), s)

    def test_bear_none_falls_back_to_moderate(self):
        self.market("bear", bear=None)
        self.assertEqual(RM._leverage_ratio("SHORT"), 0.70)

    def test_bad_direction_raises(self):
        self.market("range")
        with self.assertRaises(ValueError):
            RM._leverage_ratio("SIDEWAYS")


class TestRemaining(Base):
    def test_budget_minus_used_includes_tp1_filled(self):
        self.market("range")                                    # ratio 0.5 → budget = 1000×5×0.5 = 2500
        self.positions([pos(qty=2, price=100),                                   # 200
                        pos(qty=3, price=100, tp1=True),                         # 300：tp1 已成交也计入 used
                        pos(qty=1, price=100, status="pending"),                 # 100
                        pos(qty=1, price=100, status="patching"),                # 100
                        pos(qty=9, price=100, status="closed"),                  # 不计
                        pos(direction="SHORT", qty=50, price=100)])              # 方向不同，不计
        self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 1500.0)   # budget 2500 - used 700 = 1800 → 受单仓上限 1500

    def test_single_nominal_cap_applies(self):
        self.market("range"); self.positions([])
        self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 1500.0)       # budget 2500 > 单仓上限 1.5×1000
        self.assertEqual(RM.calc_leverage_remaining("SHORT", 400.0), 600.0)

    def test_remaining_below_single_cap(self):
        self.market("range"); self.positions([pos(qty=22, price=100)])             # used 2200 → remaining 300
        self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 300.0)

    def test_used_up_returns_zero(self):
        self.market("range"); self.positions([pos(qty=30, price=100)])             # used 3000 > budget 2500
        self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 0.0)

    def test_uses_quantity_remaining_not_quantity(self):
        self.market("range")
        self.positions([pos(qty=10, price=100, quantity_remaining=4)])             # 剩余 4 → used 400
        self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 1500.0)       # 2500-400=2100 → 受单仓上限 1500
        self.positions([pos(qty=22, price=100, quantity_remaining=10)])
        self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 1500.0)

    def test_missing_quantity_remaining_falls_back_to_quantity(self):
        self.market("range")
        p = pos(qty=24, price=100); p.pop("quantity_remaining")
        self.positions([p])
        self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 100.0)

    def test_bad_position_rows_ignored(self):
        self.market("range")
        self.positions([pos(qty="x", price=100), pos(qty=1, price=None), pos(qty=float("nan"), price=100), pos(qty=1, price=100)])
        self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 1500.0)

    def test_ratio_by_mode_changes_budget(self):
        self.positions([pos(qty=0, price=1)])
        self.market("bear", bear="strong")
        self.assertEqual(RM.calc_leverage_remaining("LONG", 100.0), 100.0)          # budget 100，单仓上限 150 → 100
        self.assertEqual(RM.calc_leverage_remaining("SHORT", 100.0), 150.0)         # budget 400，单仓上限 150

    def test_exception_fails_closed_to_zero(self):
        """[fix #29] 计算异常 → 剩余预算 0（拒绝开仓），不再降级放行"""
        self.market("range")
        with mock.patch.object(ST, "get_all_positions", side_effect=RuntimeError("state down")):
            self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 0.0)
        with mock.patch.object(RM, "get_market_mode", side_effect=RuntimeError("api down")):
            self.assertEqual(RM.calc_leverage_remaining("LONG", 1000.0), 0.0)
        self.assertEqual(RM.calc_leverage_remaining("SIDEWAYS", 1000.0), 0.0)   # 非法方向同样拒绝

    def test_unusable_equity_is_not_blocking(self):
        for eq in (None, "x", float("nan"), float("inf")):
            self.assertEqual(RM.calc_leverage_remaining("LONG", eq), float("inf"), eq)


class TestExecutorHelper(Base):
    def _call(self, nominal=800.0, equity=1000.0):
        return EX._apply_leverage_budget("XUSDT", "LONG", nominal, equity)

    def test_rejects_when_budget_used_up(self):
        with mock.patch.object(RM, "calc_leverage_remaining", return_value=0.0):
            ok, n, reason = self._call()
        self.assertFalse(ok); self.assertEqual(reason, "杠杆预算已用满"); self.assertEqual(n, 800.0)

    def test_truncates_nominal_when_remaining_smaller(self):
        with mock.patch.object(RM, "calc_leverage_remaining", return_value=300.0):
            ok, n, _ = self._call(800.0)
        self.assertTrue(ok); self.assertEqual(n, 300.0)

    def test_rejects_when_truncated_below_min_notional(self):
        """截断后低于 MIN_NOTIONAL_GUARD → 拒绝本次开仓，原因为「杠杆预算不足最小名义」。"""
        guard = EX.MIN_NOTIONAL_GUARD
        with mock.patch.object(RM, "calc_leverage_remaining", return_value=guard - 1):
            with self.assertLogs(EX.logger, level="WARNING") as cm:
                logging.disable(logging.NOTSET)
                ok, n, reason = self._call(guard * 8)
        self.assertFalse(ok)
        self.assertEqual(reason, "杠杆预算不足最小名义")
        self.assertEqual(n, guard - 1)
        self.assertTrue(any("杠杆预算截断后低于最小名义，跳过开仓" in m for m in cm.output))

    def test_truncated_exactly_at_min_notional_passes(self):
        guard = EX.MIN_NOTIONAL_GUARD
        with mock.patch.object(RM, "calc_leverage_remaining", return_value=guard):
            ok, n, reason = self._call(guard * 8)
        self.assertTrue(ok); self.assertEqual(n, guard); self.assertEqual(reason, "")

    def test_min_notional_check_end_to_end(self):
        # used 2480 → remaining 20 < guard（MIN_NOTIONAL_GUARD 已由 100 降到 50，原 remaining=50 不再低于门槛）
        self.market("range"); self.positions([pos(qty=24.8, price=100)])
        ok, n, reason = self._call(nominal=500.0, equity=1000.0)
        self.assertFalse(ok); self.assertEqual(reason, "杠杆预算不足最小名义"); self.assertEqual(n, 20.0)

    def test_nominal_kept_when_remaining_larger(self):
        with mock.patch.object(RM, "calc_leverage_remaining", return_value=1500.0):
            ok, n, _ = self._call(800.0)
        self.assertTrue(ok); self.assertEqual(n, 800.0)

    def test_single_position_cap_end_to_end(self):
        self.market("range"); self.positions([])
        ok, n, _ = self._call(nominal=5000.0, equity=1000.0)          # 单仓上限 1.5×1000
        self.assertTrue(ok); self.assertEqual(n, 1500.0)

    def test_end_to_end_used_up_rejected(self):
        self.market("range"); self.positions([pos(qty=30, price=100)])
        ok, _, reason = self._call(nominal=500.0)
        self.assertFalse(ok); self.assertIn("已用满", reason)

    def test_disabled_flag_passes_through(self):
        with mock.patch.object(C, "LEVERAGE_UTILIZATION_ENABLED", False), \
                mock.patch.object(RM, "calc_leverage_remaining", side_effect=AssertionError("不应被调用")):
            ok, n, _ = self._call(800.0)
        self.assertTrue(ok); self.assertEqual(n, 800.0)

    def test_exception_degrades_without_blocking(self):
        with mock.patch.object(RM, "calc_leverage_remaining", side_effect=RuntimeError("boom")):
            ok, n, _ = self._call(800.0)
        self.assertTrue(ok); self.assertEqual(n, 800.0)

    def test_call_site_is_after_final_check_and_before_setup_symbol(self):
        src = Path(EX.__file__).read_text(encoding="utf-8")
        i_check = src.index("ok, reason = _account_final_check(notional)")
        i_lev = src.index("_apply_leverage_budget(symbol, direction, notional, total_bal)")
        i_setup = src.index("leverage = _setup_symbol(symbol)", i_check)
        self.assertLess(i_check, i_lev); self.assertLess(i_lev, i_setup)


class TestSlotsVsUsed(Base):
    def test_tp1_filled_frees_slot_but_not_used(self):
        self.market("range")
        before = [pos(qty=2, price=100, tp1=False), pos(direction="SHORT", qty=1, price=100)]
        after = [pos(qty=2, price=100, tp1=True), pos(direction="SHORT", qty=1, price=100)]
        self.positions(before)
        cnt_b = ST.get_position_count(); rem_b = RM.calc_leverage_remaining("LONG", 100.0)
        self.positions(after)
        cnt_a = ST.get_position_count(); rem_a = RM.calc_leverage_remaining("LONG", 100.0)
        self.assertEqual(cnt_b, {"total": 2, "long": 1, "short": 1})
        self.assertEqual(cnt_a, {"total": 1, "long": 0, "short": 1})      # TP1 成交后槽位减少
        self.assertEqual(rem_b, rem_a)                                    # used 不变（名义照算）

    def test_position_count_other_logic_unchanged(self):
        self.positions([pos(status="open"), pos(status="patching", direction="SHORT"),
                        pos(status="pending"), pos(status="closed"), pos(status="open", tp1=True)])
        self.assertEqual(ST.get_position_count(), {"total": 2, "long": 1, "short": 1})

    def test_missing_tp1_filled_key_counts_as_slot(self):
        p = pos(); p.pop("tp1_filled")
        self.positions([p])
        self.assertEqual(ST.get_position_count()["total"], 1)


if __name__ == "__main__":
    unittest.main()
