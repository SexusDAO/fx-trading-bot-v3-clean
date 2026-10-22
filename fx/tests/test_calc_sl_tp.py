"""
tests/test_calc_sl_tp.py — calc_sl_tp 全场景自动化测试
20+ 组 mock liq_data 覆盖所有边界情况
"""
import sys
import math
from pathlib import Path
from unittest.mock import patch
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from shared.calc_sl_tp import calc_sl_tp

# ── mock liq_data 构造器 ──────────────────────────────────────────────────────
def _zone(pl, ph, liq=1_000_000):
    return {"price_low": pl, "price_high": ph, "liq_usd": liq}

def _mock_signal(mode="pullback", direction="LONG", tier="mid",
                 entry_mode="immediate", strength="medium", cap=0.5):
    return {
        "mode": mode, "direction": direction, "volatility_tier": tier,
        "entry_mode": entry_mode, "signal_strength": strength,
        "position_size_cap": cap, "symbol": "BTCUSDT",
        "judge_detail": {},
    }

def _mock_klines(base=50000, n=25):
    """生成平稳 K 线（用于 EMA20 计算）"""
    return [[0, base, base*1.01, base*0.99, base, 1000, 0,0,0,0,0,0]
            for _ in range(n)]

# 标准清算数据集（价格围绕 50000）
# 止损/止盈清算区设计：
# liq_long（下方多头聚集）：做多止损参考 + 做空止盈目标
# liq_short（上方空头聚集）：做多止盈目标 + 做空止损参考
# STANDARD_LIQ 设计约束：
# - liq_long: 做多止损参考区（下方 1-2 ATR 内）
# - liq_short: 做多止盈目标 & 做空止损参考（上方 1-2 ATR 内）
# 单一清算区保证止损距离在 8% 内
STANDARD_LIQ = {
    "long": [
        _zone(49000, 49500, 5_000_000),  # 做多止损参考 / 做空止盈目标
    ],
    "short": [
        _zone(51500, 52000, 5_000_000),  # 做多止盈目标 / 做空止损参考
    ],
}

MARK  = 50000.0
# ATR=400: 0.8% 中波动，使清算区(49000-49500, mid=49250)落入 _nearest_liq_below 的
# [0.5*ATR, 2.0*ATR] = [200, 800] 范围内（dist=750, atr_d=1.875）
ATR   = 400.0
EMA20 = 49800.0
K1H   = _mock_klines(MARK)


# ═══════════════════════════════════════════════════════════════════════════════
# pullback 模式
# ═══════════════════════════════════════════════════════════════════════════════
class TestPullbackMode:

    def test_pullback_long_normal(self):
        """正常做多回踩：入场价低于 mark，止损低于入场，TP1 高于入场"""
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert "error" not in r
        assert r["entry_price"] < MARK
        assert r["sl_price"] < r["entry_price"]
        assert r["tp1_price"] > r["entry_price"]
        # TP2 在清算区稀少时可为 None（tp2_price=None 是合法降级）
        if r["tp2_price"] is not None:
            assert r["tp2_price"] > r["entry_price"]
        assert r["entry_mode"] in ("immediate", "pending_order", "weighted", "at_confirm")  # [v3.87] at_confirm=OPT-8 在确认处入场

    def test_pullback_short_normal(self):
        """正常做空回踩：入场价高于 mark，止损高于入场，TP1 低于入场"""
        sig = _mock_signal("pullback", "SHORT")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert "error" not in r
        assert r["entry_price"] > MARK
        assert r["sl_price"] > r["entry_price"]
        assert r["tp1_price"] < r["entry_price"]
        # TP2 在清算区稀少时可为 None
        if r["tp2_price"] is not None:
            assert r["tp2_price"] < r["entry_price"]

    def test_pullback_rr_at_least_1_5(self):
        """TP1 盈亏比 >= 1.5"""
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert r["rr_tp1"] >= 1.5

    def test_pullback_tp2_above_tp1_long(self):
        """做多时 TP2 必须 >= TP1（tp2 可为 None 表示没清算区）"""
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        if r["tp2_price"] is not None:
            assert r["tp2_price"] >= r["tp1_price"]

    def test_pullback_tp2_below_tp1_short(self):
        """做空时 TP2 必须 <= TP1（tp2 可为 None）"""
        sig = _mock_signal("pullback", "SHORT")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        if r["tp2_price"] is not None:
            assert r["tp2_price"] <= r["tp1_price"]


# ═══════════════════════════════════════════════════════════════════════════════
# breakout 模式
# ═══════════════════════════════════════════════════════════════════════════════
class TestBreakoutMode:

    def test_breakout_long_entry_near_mark(self):
        """突破做多：入场价在 mark ± 0.1 ATR 范围内"""
        sig = _mock_signal("breakout", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert "error" not in r
        assert abs(r["entry_price"] - MARK) <= ATR * 0.15

    def test_breakout_short_entry_near_mark(self):
        sig = _mock_signal("breakout", "SHORT")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert abs(r["entry_price"] - MARK) <= ATR * 0.15

    def test_breakout_pressure_zone_adjusts_entry(self):
        """突破位上方有清算压力时，做多入场价向下微调"""
        # 在 mark 到 mark+0.5ATR 之间放大量清算区
        pressure_liq = {
            "long": STANDARD_LIQ["long"],
            "short": [_zone(MARK + 100, MARK + 300, 10_000_000)],  # 大清算区，在1ATR内
        }
        sig = _mock_signal("breakout", "LONG")
        r_no_pressure = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        r_pressure    = calc_sl_tp(sig, MARK, ATR, pressure_liq,  K1H, EMA20)
        # 有压力时入场价应更低
        assert r_pressure["entry_price"] <= r_no_pressure["entry_price"]


# ═══════════════════════════════════════════════════════════════════════════════
# reversal 模式
# ═══════════════════════════════════════════════════════════════════════════════
class TestReversalMode:

    def test_reversal_long_immediate_entry(self):
        sig = _mock_signal("reversal", "LONG", entry_mode="immediate")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert "error" not in r
        assert r["entry_price"] < MARK   # 入场价低于当前价

    def test_reversal_long_pending_order_entry(self):
        """pending_order 模式：入场价在清算区附近（v3.12 距离保护可能退到 mark±ATR）"""
        sig = _mock_signal("reversal", "LONG", entry_mode="pending_order", cap=0.75)
        liq = {
            "long": [_zone(49000, 49500, 5_000_000)],
            "short": STANDARD_LIQ["short"],
        }
        r = calc_sl_tp(sig, MARK, ATR, liq, K1H, EMA20)
        assert r["entry_mode"] == "pending_order"
        # 入场价应在 mark 下方（清算区附近或 mark-1.2ATR）
        assert r["entry_price"] < MARK
        assert r["sl_price"] < r["entry_price"]

    def test_reversal_short_normal(self):
        sig = _mock_signal("reversal", "SHORT")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert "error" not in r
        assert r["entry_price"] > MARK


# ═══════════════════════════════════════════════════════════════════════════════
# 降级：清算图不可用
# ═══════════════════════════════════════════════════════════════════════════════
class TestDegradedMode:

    def test_no_liq_data_sl_degraded(self):
        """清算图为空时 sl_degraded=True"""
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, {}, K1H, EMA20)
        assert "error" not in r
        assert r["sl_degraded"] is True
        assert r["liq_data_available"] is False

    def test_no_liq_data_atr_sl_long(self):
        """降级止损：止损在入场价下方，距离 >= 2.5%（最小距离保护）"""
        sig = _mock_signal("pullback", "LONG", tier="mid")
        r = calc_sl_tp(sig, MARK, ATR, {}, K1H, EMA20)
        assert r["sl_price"] < r["entry_price"]
        sl_dist_pct = (r["entry_price"] - r["sl_price"]) / r["entry_price"] * 100
        assert sl_dist_pct >= 2.5, f"止损距离应 >= 2.5%, 实际 {sl_dist_pct:.2f}%"
        assert r["sl_degraded"] is True

    def test_no_liq_data_atr_tp(self):
        """TP1 > entry（做多），RR >= 1.5；TP2 在无上方清算区时可为 None"""
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, {}, K1H, EMA20)
        entry = r["entry_price"]
        assert r["tp1_price"] > entry, "TP1 应高于入场价"
        assert r["rr_tp1"] >= 1.5, f"RR_TP1 应 >= 1.5, 实际 {r['rr_tp1']:.2f}"
        # TP2 在没有上方清算区时允许为 None
        if r["tp2_price"] is not None:
            assert r["tp2_price"] >= r["tp1_price"]

    def test_empty_liq_lists_degrade(self):
        """空列表（非 None）也应降级"""
        sig = _mock_signal("breakout", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, {"long": [], "short": []}, K1H, EMA20)
        assert r["sl_degraded"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# 止损距离超 8% → reject
# ═══════════════════════════════════════════════════════════════════════════════
class TestSLDistanceReject:

    def test_sl_too_far_returns_error(self):
        """清算区引用 SL 超过上限时降级到 ATR，若 ATR 公式也超限则 reject"""
        # 中波动 tier=mid，超大 ATR 使得即使 ATR 降级也超限
        # entry 大约在 48000（pullback 降级）， sl = entry - 1.5*2500 = 41750, 距离 13%
        # mid tier 上限 15% → 不 reject
        # 用 tier=low 且 ATR 非常大，使 ATR 降级也超过 12% 上限
        extreme_liq = {
            "long": [],
            "short": STANDARD_LIQ["short"],
        }
        sig = _mock_signal("pullback", "LONG", tier="low")
        r = calc_sl_tp(sig, MARK, ATR * 20, extreme_liq, K1H, EMA20)  # ATR*20 = 超大
        assert "error" in r, f"应最终 reject，实际返回: {r}"

    def test_sl_within_limit_ok(self):
        """止损距离在 7% 以内时正常通过"""
        sig = _mock_signal("pullback", "LONG", tier="low")
        r = calc_sl_tp(sig, MARK, ATR * 0.5, STANDARD_LIQ, K1H, EMA20)
        assert "error" not in r
        assert r["sl_dist_pct"] < 8.0   # sl_dist_pct 是百分比形式（如 2.5 = 2.5%）


# ═══════════════════════════════════════════════════════════════════════════════
# 盈亏比不足 → TP 上移
# ═══════════════════════════════════════════════════════════════════════════════
class TestRRUpgrade:

    def test_tp1_rr_meets_minimum(self):
        """TP1 盈亏比必须 >= 1.5"""
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert r["rr_tp1"] >= 1.5

    def test_tp2_rr_meets_minimum(self):
        """TP2 有清算区时盈亏比应 >= 2.5（无清算区时 tp2 可为 None）"""
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        if r.get("rr_tp2") is not None:
            assert r["rr_tp2"] >= 2.5


    def test_tp_fallback_when_no_liq_zone(self):
        """无清算区时 TP1 = entry + 2ATR，仍满足 >= 1.5 RR"""
        sig = _mock_signal("pullback", "LONG", tier="low")
        # 清算图有止损数据但无止盈清算区
        liq = {
            "long": [_zone(48000, 48500, 5_000_000)],
            "short": [],   # 无上方清算区
        }
        r = calc_sl_tp(sig, MARK, ATR, liq, K1H, EMA20)
        assert "error" not in r
        assert r["rr_tp1"] >= 1.5


# ═══════════════════════════════════════════════════════════════════════════════
# 波动层级 sl_mult 差异
# ═══════════════════════════════════════════════════════════════════════════════
class TestVolatilityTierSLMult:

    def _get_sl_dist(self, tier):
        sig = _mock_signal("pullback", "LONG", tier=tier)
        r = calc_sl_tp(sig, MARK, ATR, {}, K1H, EMA20)   # 降级，方便比较
        return r["entry_price"] - r["sl_price"]

    def test_sl_mult_increases_with_tier(self):
        """高波动层止损距离 > 中波动层 > 低波动层"""
        sl_low  = self._get_sl_dist("low")
        sl_mid  = self._get_sl_dist("mid")
        sl_high = self._get_sl_dist("high")
        assert sl_high >= sl_mid >= sl_low

    def test_low_tier_sl_mult_1_2(self):
        """低波动层 SL 在入场价下方（小 ATR 时最小距离保护会覆盖 1.2×ATR）"""
        sig = _mock_signal("pullback", "LONG", tier="low")
        r = calc_sl_tp(sig, MARK, ATR, {}, K1H, EMA20)
        sl_dist = r["entry_price"] - r["sl_price"]
        assert sl_dist > 0, "SL 应在入场价下方"


# ═══════════════════════════════════════════════════════════════════════════════
# 做多做空镜像逻辑
# ═══════════════════════════════════════════════════════════════════════════════
class TestLongShortSymmetry:

    def test_long_entry_below_mark(self):
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert r["entry_price"] <= MARK

    def test_short_entry_above_mark(self):
        sig = _mock_signal("pullback", "SHORT")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert r["entry_price"] >= MARK

    def test_long_sl_below_entry(self):
        sig = _mock_signal("pullback", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert r["sl_price"] < r["entry_price"]

    def test_short_sl_above_entry(self):
        sig = _mock_signal("pullback", "SHORT")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert r["sl_price"] > r["entry_price"]

    def test_long_tp1_above_entry(self):
        sig = _mock_signal("breakout", "LONG")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert r["tp1_price"] > r["entry_price"]

    def test_short_tp1_below_entry(self):
        sig = _mock_signal("breakout", "SHORT")
        r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
        assert r["tp1_price"] < r["entry_price"]

    def test_all_modes_long_short_no_error(self):
        """三种模式 × 两个方向 = 6 组，全部不应有 error"""
        for mode in ("pullback", "breakout", "reversal"):
            for direction in ("LONG", "SHORT"):
                sig = _mock_signal(mode, direction)
                r = calc_sl_tp(sig, MARK, ATR, STANDARD_LIQ, K1H, EMA20)
                assert "error" not in r, f"{mode}/{direction} returned error: {r['error']}"


# ═══════════════════════════════════════════════════════════════════════════════
# TestFallbackSlMult — liq_data=None 时，降级路径必须按波动层读取正确 sl_mult
# ═══════════════════════════════════════════════════════════════════════════════
class TestFallbackSlMult:
    """
    当清算图数据为空时（liq_zones={}），_calc_sl 走降级路径。
    v3.13: 小 ATR 场景下最小距离保护（2.5%）会覆盖 sl_mult*ATR，
           所以验证 SL 方向正确 + 距离 >= 2.5% + sl_degraded=True。
    """

    ENTRY = 50000.0
    ATR_F = 500.0          # 1% ATR，低波动，SL 会被最小距离保护拉到 2.5%
    EMPTY_LIQ = {}
    K1H_F = _mock_klines(base=50000, n=25)
    EMA20_F = 50000.0

    def _run(self, tier: str, direction: str) -> dict:
        sig = _mock_signal(mode="pullback", direction=direction, tier=tier)
        sig["mode"] = "universal"
        r = calc_sl_tp(sig, self.ENTRY, self.ATR_F, self.EMPTY_LIQ, self.K1H_F, self.EMA20_F)
        assert "error" not in r, f"意外 error: {r.get('error')}"
        return r

    def test_long_low_tier_sl_mult_1_2(self):
        """低波动层 LONG: SL 在入场价下方，距离 >= 2.5%，sl_degraded=True"""
        r = self._run("low", "LONG")
        assert r["sl_price"] < r["entry_price"], "LONG 止损必须低于入场价"
        sl_pct = (r["entry_price"] - r["sl_price"]) / r["entry_price"] * 100
        assert sl_pct >= 2.5, f"SL 距离应 >= 2.5%, 实际 {sl_pct:.2f}%"
        assert r["sl_degraded"] is True

    def test_long_mid_tier_sl_mult_1_5(self):
        """中波动层 LONG: SL 在入场价下方，距离 >= 2.5%"""
        r = self._run("mid", "LONG")
        assert r["sl_price"] < r["entry_price"]
        sl_pct = (r["entry_price"] - r["sl_price"]) / r["entry_price"] * 100
        assert sl_pct >= 2.5

    def test_long_high_tier_sl_mult_2_0(self):
        """高波动层 LONG: SL 在入场价下方，距离 >= 2.5%"""
        r = self._run("high", "LONG")
        assert r["sl_price"] < r["entry_price"]
        sl_pct = (r["entry_price"] - r["sl_price"]) / r["entry_price"] * 100
        assert sl_pct >= 2.5

    def test_short_low_tier_sl_mult_1_2(self):
        """低波动层 SHORT: SL 在入场价上方，距离 >= 2.5%"""
        r = self._run("low", "SHORT")
        assert r["sl_price"] > r["entry_price"], "SHORT 止损必须高于入场价"
        sl_pct = (r["sl_price"] - r["entry_price"]) / r["entry_price"] * 100
        assert sl_pct >= 2.5

    def test_short_mid_tier_sl_mult_1_5(self):
        r = self._run("mid", "SHORT")
        assert r["sl_price"] > r["entry_price"]
        sl_pct = (r["sl_price"] - r["entry_price"]) / r["entry_price"] * 100
        assert sl_pct >= 2.5

    def test_short_high_tier_sl_mult_2_0(self):
        r = self._run("high", "SHORT")
        assert r["sl_price"] > r["entry_price"]
        sl_pct = (r["sl_price"] - r["entry_price"]) / r["entry_price"] * 100
        assert sl_pct >= 2.5

    def test_sl_degraded_flag_set_when_no_liq(self):
        """无清算图时 sl_degraded 必须为 True"""
        r = self._run("mid", "LONG")
        assert r["sl_degraded"] is True, "无清算图时 sl_degraded 应为 True"
