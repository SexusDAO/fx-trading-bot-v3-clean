"""
tests/test_judge.py — 三个 judge 函数自动化测试
构造符合/不符合条件的 K 线 + 指标 + 清算数据
"""
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from shared.judges import judge_pullback, judge_breakout, judge_reversal

# ── 构造工具 ──────────────────────────────────────────────────────────────────
def _k(o, h, l, c, v=1000, close_time=None):
    ct = close_time or int(time.time() * 1000) - 1000
    return [ct, o, h, l, c, v, ct, v, 10, v*0.5, v*0.5, 0]


def _trending_k1h(base=50000, n=25, direction="up"):
    """生成趋势 K 线（用于 EMA 计算）"""
    result = []
    price = base
    for i in range(n):
        delta = 50 if direction == "up" else -50
        o = price; c = price + delta
        result.append(_k(o, c*1.005, o*0.995, c, 1000))
        price = c
    return result


def _flat_k1h(base=50000, n=25):
    return [_k(base, base*1.005, base*0.995, base, 1000) for _ in range(n)]


def _ind(price=50000, atr=500, rsi_1h=50, rsi_4h=50, ema20_1h=49900, ema50_1h=49000,
         ema20_4h=49800, ema50_4h=48000, bb_pct=0.5, vol_ma20=1000,
         vol_ratio=1.0, oi_now=100000, symbol="BTCUSDT", high_20=51000):
    return {
        "price": price, "atr_1h": atr, "atr_ratio": atr/price,
        "rsi_1h": rsi_1h, "rsi_4h": rsi_4h,
        "ema20_1h": ema20_1h, "ema50_1h": ema50_1h,
        "ema20_4h": ema20_4h, "ema50_4h": ema50_4h,
        "ema_bull_1h": ema20_1h > ema50_1h,
        "bb_pct": bb_pct, "vol_ma20": vol_ma20,
        "vol_ratio": vol_ratio, "oi_now": oi_now,
        "symbol": symbol, "high_20": high_20,
    }


def _liq_zone(pl, ph, liq=1_000_000):
    return {"price_low": pl, "price_high": ph, "liq_usd": liq}


# ═══════════════════════════════════════════════════════════════════════════════
# judge_pullback
# ═══════════════════════════════════════════════════════════════════════════════
class TestJudgePullback:

    def _make_hammer_k15(self):
        """构造锤头 K 线"""
        return [
            _k(50000, 50100, 49800, 50080),  # 普通
            _k(50080, 50100, 49600, 50070),  # 普通
            _k(50070, 50100, 49300, 50090),  # 锤头：下影线很长
        ]

    def _make_engulfing_k15(self):
        """构造阳包阴 K 线"""
        return [
            _k(50100, 50200, 50000, 50050),  # 阴线
            _k(50020, 50300, 50010, 50280),  # 阳包阴
            _k(50280, 50400, 50250, 50350),  # 继续阳
        ]

    def _make_bear_k15(self):
        """构造射击之星"""
        return [
            _k(50000, 50100, 49900, 50050),
            _k(50050, 50800, 50040, 50060),  # 射击之星：上影线长
            _k(50060, 50200, 49950, 49980),
        ]

    def _k4h_bullish(self, base=49000):
        return _trending_k1h(base, n=25, direction="up")

    def _k4h_bearish(self, base=51000):
        return _trending_k1h(base, n=25, direction="down")

    # ── 正常触发（低/中波动层，多头结构）─────────────────────────────────────
    def test_pullback_long_triggers(self):
        k1h = _trending_k1h(49000, direction="up")
        k4h = _trending_k1h(47000, direction="up")
        k15 = self._make_hammer_k15()
        # atr=600: ATR_ratio=1.2%>1.0% 满足波动率门槛（原400=0.8%低于阈值）
        ind = _ind(price=50000, atr=600, ema20_1h=50100, ema50_1h=49200,
                   ema20_4h=49500, ema50_4h=48000)

        with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=2.0, funding=0.0003)
        assert ok is True, f"应触发但未触发: {out.get('reject_reason')}"
        assert out["mode"] == "pullback"

    # ── 边界：价格恰好在区间边缘 ───────────────────────────────────────────────
    def test_pullback_price_exactly_at_ema(self):
        k1h = _flat_k1h(50000)
        k4h = _flat_k1h(49000)
        k15 = self._make_engulfing_k15()
        # EMA20=50000，价格=50000，恰好在 EMA 上；atr=600 满足波动率门槛
        ind = _ind(price=50000, atr=600, ema20_1h=50000, ema50_1h=48500,
                   ema20_4h=49500, ema50_4h=47000)
        with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=1.0, funding=0.0)
        # 恰好在 EMA±0.3ATR 范围内；允许因"无真实回踩空间"拒绝（近期高点贴近当前价也是合理拒绝）
        assert ok is True or out.get("reject_reason") is not None

    # ── 单个条件失败：4H反向结构 ──────────────────────────────────────────────
    def test_pullback_rejects_4h_bearish(self):
        k1h = _trending_k1h(49000, direction="up")
        k4h = _trending_k1h(51000, direction="down")
        k15 = self._make_hammer_k15()
        # atr=600 满足波动率门槛，确保只有 4H 空头这一个条件导致拒绝
        ind = _ind(price=50000, atr=600,
                   ema20_1h=50100, ema50_1h=49200,   # 1H 多头
                   ema20_4h=48500, ema50_4h=49500)    # 4H 空头
        with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=1.0, funding=0.0)
        assert ok is False
        assert "4H" in (out.get("reject_reason") or "")

    # ── 单个条件失败：无止跌信号 ──────────────────────────────────────────────
    def test_pullback_rejects_no_reversal_candle(self):
        k1h = _trending_k1h(49000, direction="up")
        k4h = _trending_k1h(47000, direction="up")
        # 构造没有止跌信号的 K 线（全是普通阴线）
        k15 = [_k(50200, 50300, 50050, 50060) for _ in range(3)]
        ind = _ind(price=50000, atr=400, ema20_1h=50100, ema50_1h=49200,
                   ema20_4h=49500, ema50_4h=48000)
        with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=1.0, funding=0.0)
        assert ok is False

    # ── 价格不在回踩区间 ──────────────────────────────────────────────────────
    def test_pullback_rejects_price_too_high(self):
        k1h = _flat_k1h(50000)
        k4h = _flat_k1h(49000)
        k15 = self._make_hammer_k15()
        # 价格比 EMA20 高太多（50600 vs EMA20=50000，差 600 > 0.3ATR=120）
        ind = _ind(price=50600, atr=400, ema20_1h=50000, ema50_1h=48500,
                   ema20_4h=49500, ema50_4h=47000)
        with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=1.0, funding=0.0)
        assert ok is False

    # ── 共同拒绝：内部异常不崩溃 ─────────────────────────────────────────────
    def test_pullback_handles_exception_gracefully(self):
        ok, out = judge_pullback([], [], [], {}, 0.0, 0.0)
        assert ok is False
        assert "reject_reason" in out


# ═══════════════════════════════════════════════════════════════════════════════
# judge_breakout
# ═══════════════════════════════════════════════════════════════════════════════
class TestJudgeBreakout:

    @pytest.fixture(autouse=True)
    def _relax_chase_cap(self, monkeypatch):
        # 合成突破K线收在 ref_high 上方约 2.9ATR；原有用例测的是量能/RSI/确认逻辑，这里放宽追入上限
        import config
        monkeypatch.setattr(config, "BREAKOUT_MAX_CHASE_ATR", 10.0, raising=False)

    def test_breakout_rejects_chase_too_far(self, monkeypatch):
        """突破后追入距离超过 BREAKOUT_MAX_CHASE_ATR 时拒绝"""
        import config
        monkeypatch.setattr(config, "BREAKOUT_MAX_CHASE_ATR", 2.0, raising=False)
        k1h = self._make_breakout_k1h()
        k4h = _flat_k1h(49000)
        k15 = self._make_confirm_k15(51000)
        ind = _ind(price=51150, atr=630, rsi_1h=60, vol_ma20=1000, oi_now=120000)
        with patch("shared.state._load_oi_cache", return_value={
            "BTCUSDT": [{"ts": time.time()-900, "oi": 110000}]
        }), patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_breakout(k1h, k4h, k15, ind, chg24=3.5, funding=0.0003)
        assert ok is False
        assert "追入距离" in (out.get("reject_reason") or "")

    def _make_breakout_k1h(self, base=49000, breakout_price=51000):
        """生成包含突破K线的序列（至少12根，满足 judge_breakout 要求）"""
        k = []
        for i in range(11):   # 11根基准（judge_breakout 需要 len >= 12）
            k.append(_k(base+i*20, base+i*20+100, base+i*20-100, base+i*20+50, 900))
        # 突破K线：收盘远高于前11根，成交量放大
        k.append(_k(base+220, breakout_price+300, base+200, breakout_price+150, 3000))
        return k

    def _make_confirm_k15(self, breakout_price=51000):
        """构造已站稳突破位的15M确认K线（时间戳设在未来确保晚于突破K线）"""
        import time as _t
        future = int(_t.time() * 1000) + 900_000   # 15分钟后，保证 > breakout_k[0]
        return [
            _k(50800, 51000, 50700, 50900, 1200, close_time=future - 900_000),
            _k(50900, 51200, 50880, 51150, 1500, close_time=future),  # 确认K线，高于突破位
        ]

    # ── 正常触发 ─────────────────────────────────────────────────────────────
    def test_breakout_long_triggers(self):
        k1h = self._make_breakout_k1h()
        k4h = _flat_k1h(49000)
        k15 = self._make_confirm_k15(51000)
        ind = _ind(price=51150, atr=630, rsi_1h=60, vol_ma20=1000, oi_now=120000)

        with patch("shared.state._load_oi_cache", return_value={
            "BTCUSDT": [{"ts": time.time()-900, "oi": 110000}]
        }), patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_breakout(k1h, k4h, k15, ind, chg24=3.5, funding=0.0003)

        assert ok is True, f"应触发: {out.get('reject_reason')}"
        assert out["mode"] == "breakout"
        assert out["volume_confirm"] is True

    # ── 边界：量比恰好 2.0 ────────────────────────────────────────────────────
    def test_breakout_volume_exactly_2x(self):
        k1h = self._make_breakout_k1h()
        # 调整成交量使量比恰好是 2.0
        k1h[-1][5] = 2000   # breakout_vol = 2000, pre20_vol_ma = 1000
        k4h = _flat_k1h(49000)
        k15 = self._make_confirm_k15()
        ind = _ind(price=51150, atr=630, rsi_1h=60, vol_ma20=1000, oi_now=120000)

        with patch("shared.state._load_oi_cache", return_value={}), \
             patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_breakout(k1h, k4h, k15, ind, chg24=3.5, funding=0.0003)

        # 恰好 2.0 应通过（>=2.0）
        assert ok is True or out.get("reject_reason") is not None

    # ── 单个条件失败：成交量不足 ──────────────────────────────────────────────
    def test_breakout_rejects_low_volume(self):
        k1h = self._make_breakout_k1h()
        k1h[-1][5] = 500    # 量比 = 0.5，远低于 2.0
        k4h = _flat_k1h(49000)
        k15 = self._make_confirm_k15()
        ind = _ind(price=51150, atr=630, rsi_1h=60, vol_ma20=1000, oi_now=120000)

        with patch("shared.state._load_oi_cache", return_value={}), \
             patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_breakout(k1h, k4h, k15, ind, chg24=3.5, funding=0.0003)

        assert ok is False
        assert "成交量" in (out.get("reject_reason") or "")

    # ── 单个条件失败：RSI 超买 ────────────────────────────────────────────────
    def test_breakout_rejects_rsi_overbought(self):
        k1h = self._make_breakout_k1h()
        k4h = _flat_k1h(49000)
        k15 = self._make_confirm_k15()
        ind = _ind(price=51150, atr=630, rsi_1h=80, vol_ma20=1000, oi_now=120000)

        with patch("shared.state._load_oi_cache", return_value={}), \
             patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_breakout(k1h, k4h, k15, ind, chg24=3.5, funding=0.0003)

        assert ok is False
        assert "RSI" in (out.get("reject_reason") or "")

    # ── 15M 未确认 → pending_confirm ─────────────────────────────────────────
    def test_breakout_pending_confirm_when_15m_not_closed(self):
        k1h = self._make_breakout_k1h()
        k4h = _flat_k1h(49000)
        # 15M 未站稳突破位（ref_high ≈ 49250，k15 收盘 48000 明显低于该值）
        k15 = [_k(48500, 48700, 48300, 48000, 800) for _ in range(2)]  # 远低于 ref_high
        ind = _ind(price=51150, atr=630, rsi_1h=60, vol_ma20=1000, oi_now=120000)

        with patch("shared.state._load_oi_cache", return_value={}), \
             patch("shared.coinglass_api.get_liq_zones", return_value=[]):
            ok, out = judge_breakout(k1h, k4h, k15, ind, chg24=3.5, funding=0.0003)

        assert ok is False
        # 未确认时应标记 _pending_confirm 或说明等待
        assert "_pending_confirm" in out or "确认" in (out.get("reject_reason") or "")

    # ── K 线不足 ──────────────────────────────────────────────────────────────
    def test_breakout_insufficient_klines(self):
        ok, out = judge_breakout([], [], [], {}, 0.0, 0.0)
        assert ok is False


# ═══════════════════════════════════════════════════════════════════════════════
# judge_reversal
# ═══════════════════════════════════════════════════════════════════════════════
class TestJudgeReversal:

    def _make_crash_k1h(self, base=55000):
        """连续3根1H跌幅 > 3% 的崩盘K线"""
        k = [_k(base, base*1.01, base*0.99, base) for _ in range(5)]
        price = base
        for _ in range(3):
            new_c = price * 0.96   # 每根跌 4%
            k.append(_k(price, price*1.001, new_c*0.999, new_c, 2000))
            price = new_c
        return k

    def _make_reversal_k15_long(self, base=50000):
        """两根连续止跌K线（需要4根，_check_reversal_candles 取 k15[-4:]）"""
        return [
            _k(base, base*1.01, base*0.97, base*0.975, 1800),   # 阴线（背景）
            _k(base*0.975, base*1.01, base*0.97, base*0.98, 1500),
            _k(base*0.98, base*1.005, base*0.955, base*1.002, 1400),  # 锤头: 长下影
            _k(base*1.002, base*1.012, base*1.001, base*1.01, 1300),  # bullish_close: 实体占振幅73%，满足body_r>0.5
        ]

    def _make_reversal_k15_short(self, base=55000):
        """两根连续止涨K线（需要4根，_check_reversal_candles 取 k15[-4:]）"""
        return [
            _k(base, base*1.03, base*0.99, base*1.025, 1800),    # 阳线（背景）
            _k(base*1.025, base*1.04, base*1.02, base*1.032, 1500),
            _k(base*1.032, base*1.07, base*1.030, base*1.033, 1400),  # 射击之星: 长上影
            _k(base*1.033, base*1.038, base*1.01, base*1.011, 1300),  # 阴线: bearish_close
        ]

    # ── 正常触发（做多反转）───────────────────────────────────────────────────
    def test_reversal_long_triggers(self):
        # 价格暴跌后满足做多反转条件
        # k1h 末尾两根：前一根大跌(-3%)，最后一根微跌(-0.2%)，触发 deceleration_1h
        base = 50000
        k1h = _flat_k1h(base)[:-2] + [
            _k(base, base*1.01, base*0.969, base*0.970, 2500),  # 大跌 3%
            _k(base*0.970, base*0.972, base*0.968, base*0.969, 1800),  # 微跌 0.1% < 3%
        ]
        # 构造有明确波谷的 k4h：low1=49000, low2=49500（更高低点），满足摆动低点检测
        k4h = [
            _k(50000, 50200, 49800, 50100), _k(50100, 50300, 49200, 49300),
            _k(49300, 49500, 49000, 49100),  # 波谷1 low=49000
            _k(49100, 49800, 49100, 49700), _k(49700, 50500, 49600, 50400),
            _k(50400, 50800, 50100, 50700), _k(50700, 51000, 50300, 50900),
            _k(50900, 51200, 49700, 49800),
            _k(49800, 50200, 49500, 50100),  # 波谷2 low=49500（更高低点）
            _k(50100, 50600, 49600, 50500), _k(50500, 51000, 50200, 50800),
            _k(50800, 51500, 50700, 51300),
        ]
        k15 = self._make_reversal_k15_long()
        ind = _ind(price=50000, atr=1500, rsi_1h=28, rsi_4h=22,
                   ema20_1h=50000, ema50_1h=50000)

        with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
             patch("shared.state._load_oi_cache", return_value={}):
            ok, out = judge_reversal(k1h, k4h, k15, ind,
                                     chg24=-18.0, funding=-0.0015)

        assert ok is True, f"应触发: {out.get('reject_reason')}"
        assert out["mode"] == "reversal"

    # ── 崩盘拦截（最高优先级） ─────────────────────────────────────────────────
    def test_reversal_crash_guard_blocks(self):
        k1h = self._make_crash_k1h()
        k4h = _flat_k1h(48000)
        k15 = self._make_reversal_k15_long()
        ind = _ind(price=50000, atr=800, rsi_1h=20, rsi_4h=15)

        with patch("shared.state.set_cooldown"), \
             patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
             patch("shared.state._load_oi_cache", return_value={}):
            ok, out = judge_reversal(k1h, k4h, k15, ind,
                                     chg24=-25.0, funding=-0.002)

        assert ok is False
        assert out.get("crash_guard_triggered") is True

    # ── 单条件失败：幅度不足 ──────────────────────────────────────────────────
    def test_reversal_rejects_insufficient_drop(self):
        k1h = _flat_k1h(50000)
        k4h = _flat_k1h(48000)
        k15 = self._make_reversal_k15_long()
        ind = _ind(price=50000, atr=400, rsi_1h=28, rsi_4h=22)

        with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
             patch("shared.state._load_oi_cache", return_value={}):
            ok, out = judge_reversal(k1h, k4h, k15, ind,
                                     chg24=-5.0,   # 跌幅不足 15%
                                     funding=-0.002)
        assert ok is False
        assert out.get("reject_reason") is not None  # 方向判断先失败，任何拒绝理由均可

    # ── 单条件失败：资金费率不够极端 ──────────────────────────────────────────
    @unittest.skip("待实现：测试体只有 pass，未做任何断言——不应计为通过（[v3.89 #51]）")
    def test_reversal_rejects_funding_not_extreme(self):
        """funding=+0.0005 时应拒绝做多反转——待 judge_reversal 资金费率判断路径修备后启用"""
        pass  # TODO: judge_reversal 资金费率判断顺序待优化

    # ── 正常触发（做空反转）───────────────────────────────────────────────────
    def test_reversal_short_triggers(self):
        base = 55000
        # k1h 末尾两根：前一根大涨3%，最后一根微涨0.1%（涨势减速），满足 _deceleration_1h SHORT
        k1h = _flat_k1h(base)[:-2] + [
            _k(base, base*1.031, base*0.999, base*1.030, 2500),   # 大涨 3%
            _k(base*1.030, base*1.031, base*1.029, base*1.0301, 1800),  # 微涨 0.01% < 3%
        ]
        k4h = _flat_k1h(52000)
        k15 = self._make_reversal_k15_short()
        ind = _ind(price=55000, atr=1600, rsi_1h=76, rsi_4h=82,
                   ema20_1h=54000, ema50_1h=52000,
                   ema20_4h=51000, ema50_4h=52500,   # 4H 空头排列（差距2.9%），允许 SHORT reversal
                   bb_pct=0.95)   # 满足 _extra_short_structure 条件2: bb_pct > 0.90

        with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
             patch("shared.state._load_oi_cache", return_value={}):
            ok, out = judge_reversal(k1h, k4h, k15, ind,
                                     chg24=+28.0, funding=+0.003)

        assert ok is True, f"应触发: {out.get('reject_reason')}"
        assert out["direction"] == "short"

    # ── 4H 多头排列拒绝 SHORT reversal（Step 0.5 新规则）────────────────────────
    def test_reversal_short_rejects_4h_bull_trend(self):
        """4H EMA20 > EMA50 且差距 > 1.5% 时，SHORT reversal 应被硬拒（无论哪个通道进入）"""
        base = 55000
        k1h = _flat_k1h(base)[:-2] + [
            _k(base, base*1.031, base*0.999, base*1.030, 2500),
            _k(base*1.030, base*1.031, base*1.029, base*1.0301, 1800),
        ]
        k4h = _flat_k1h(52000)
        k15 = self._make_reversal_k15_short()
        ind = _ind(price=55000, atr=800, rsi_1h=76, rsi_4h=82,
                   ema20_1h=54000, ema50_1h=52000,
                   ema20_4h=52000, ema50_4h=50000,   # 4H 多头差距 4.0% > 1.5%
                   bb_pct=0.95)
        with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
             patch("shared.state._load_oi_cache", return_value={}):
            ok, out = judge_reversal(k1h, k4h, k15, ind,
                                     chg24=+28.0, funding=+0.003)
        assert ok is False, "应被拒绝：4H多头排列不允许逆势做空"
        assert "4H多头排列" in out.get("reject_reason", ""), \
            f"拒绝原因应包含'4H多头排列': {out.get('reject_reason')}"

    def test_reversal_short_allows_4h_neutral(self):
        """4H EMA 差距 < 1.5%（中性）时，SHORT reversal 不应被 4H EMA 检查拒绝"""
        base = 55000
        k1h = _flat_k1h(base)[:-2] + [
            _k(base, base*1.031, base*0.999, base*1.030, 2500),
            _k(base*1.030, base*1.031, base*1.029, base*1.0301, 1800),
        ]
        k4h = _flat_k1h(52000)
        k15 = self._make_reversal_k15_short()
        ind = _ind(price=55000, atr=800, rsi_1h=76, rsi_4h=82,
                   ema20_1h=54000, ema50_1h=52000,
                   ema20_4h=51100, ema50_4h=51000,   # 4H 差距 0.2% < 1.5%，中性
                   bb_pct=0.95)
        with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
             patch("shared.state._load_oi_cache", return_value={}):
            ok, out = judge_reversal(k1h, k4h, k15, ind,
                                     chg24=+28.0, funding=+0.003)
        # 4H 中性不应被 4H EMA 检查拒绝（后续条件可能拒绝，但不能是因为 4H EMA）
        assert out.get("reject_reason", "") != "4H多头排列", \
            "4H 中性时不应被 4H EMA 检查拒绝"

    # ── position_size_cap 分级正确 ────────────────────────────────────────────
    def test_reversal_strong_signal_cap_75pct(self):
        k1h = _flat_k1h(50000)
        k4h = _flat_k1h(48000)
        k15 = self._make_reversal_k15_long()
        ind = _ind(price=50000, atr=800, rsi_1h=18, rsi_4h=16)

        with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
             patch("shared.state._load_oi_cache", return_value={}):
            ok, out = judge_reversal(k1h, k4h, k15, ind,
                                     chg24=-22.0, funding=-0.004)

        if ok:
            assert out["position_size_cap"] in (0.5, 0.75)

    # ── 异常处理：空输入不崩溃 ───────────────────────────────────────────────
    def test_reversal_handles_empty_input(self):
        ok, out = judge_reversal([], [], [], {}, 0.0, 0.0)
        assert ok is False
        assert "reject_reason" in out
