"""
tests/test_pullback_ema_retest.py
EMA Retest 收盘站回确认测试
"""
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.judges import judge_pullback


def _base_ind(direction="LONG"):
    """能通过 pullback 全部条件的基础 ind（参考已有测试夹具）"""
    ema20 = 50000.0
    ema50 = 49000.0 if direction == "LONG" else 51000.0
    price = 50100.0 if direction == "LONG" else 49900.0
    return {
        "price":       price,
        "atr_1h":      800.0,
        "rsi_1h":      55.0 if direction == "LONG" else 45.0,
        "rsi_4h":      55.0,
        "ema20_1h":    ema20 if direction == "LONG" else 49800.0,
        "ema50_1h":    ema50,
        "ema20_4h":    ema20 * 1.01 if direction == "LONG" else ema20 * 0.99,
        "ema50_4h":    ema50,
        "vol_ma20":    1000.0,
        "bb_pct":      0.5,
        "vol24_usd":   50_000_000,
        "high_24h":    51000.0,
        "low_24h":     48000.0,
        "symbol":      "BTCUSDT",
        "oi_now":      1.0,
        "oi_1h_ago":   1.0,
        "taker_buy_ratio_now": 0.5,
        "taker_buy_ratio":     0.5,
    }


def _make_k1h(n=20, close=50000.0, low=None, high=None, direction="LONG"):
    """生成 n 根 1H K 线，最后3根带指定 low/high"""
    ema20 = 50000.0
    low   = low  if low  is not None else close - 200
    high  = high if high is not None else close + 200

    def kline(c, l, h):
        o = c - 50
        v = 1000.0
        return [0, str(o), str(h), str(l), str(c), str(v), 0, 0, 0, 0, 0, 0]

    base = [kline(close, close - 300, close + 300) for _ in range(n - 3)]
    # 近3根：low 触碰 EMA20（LONG）或 high 触碰 EMA20（SHORT）
    recent = [kline(close, low, high) for _ in range(3)]
    return base + recent


def _make_k4h(direction="LONG"):
    ema20 = 50100.0 if direction == "LONG" else 49900.0
    ema50 = 49000.0 if direction == "LONG" else 51000.0
    c = 50200.0 if direction == "LONG" else 49800.0
    def k(close): return [0, str(close-50), str(close+300), str(close-300), str(close), "1000", 0, 0, 0, 0, 0, 0]
    return [k(c) for _ in range(50)]


def _make_k15(direction="LONG"):
    c = 50100.0
    def k(o, cl): return [0, str(o), str(max(o,cl)+50), str(min(o,cl)-50), str(cl), "500", int(1e13), 0, 0, 0, 0, 0]
    if direction == "LONG":
        return [k(c-10, c+20) for _ in range(5)]   # 阳线
    else:
        return [k(c+10, c-20) for _ in range(5)]   # 阴线


# ──────────────────────────────────────────────────────────────────────────────
# 测试1：LONG 价格触碰 EMA20 + 收盘站回 → ema_retest=True
# ──────────────────────────────────────────────────────────────────────────────
def test_ema_retest_long_touch_and_close_ok(monkeypatch):
    """近3根最低价触碰 EMA20，且收盘价站回 EMA20 → ema_retest=True"""
    monkeypatch.setattr("shared.judges.EMA_RETEST_ENABLED", True, raising=False)
    ind = _base_ind("LONG")
    ema20 = ind["ema20_1h"]  # 50000

    # 最低价 = ema20 - 50（在 ema20 ± 0.3% 范围内），收盘 = ema20 + 100（站回）
    k1h = _make_k1h(direction="LONG", close=ema20 + 100, low=ema20 - 50, high=ema20 + 400)
    k4h = _make_k4h("LONG")
    k15 = _make_k15("LONG")

    ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=2.0, funding=0.0)
    if ok:
        assert out.get("ema_retest") is True, f"期望 ema_retest=True，实际={out.get('ema_retest')}"


# ──────────────────────────────────────────────────────────────────────────────
# 测试2：LONG 价格触碰但收盘深度跌破 EMA20 → ema_retest=False
# ──────────────────────────────────────────────────────────────────────────────
def test_ema_retest_long_touch_but_close_below(monkeypatch):
    """近3根最低价触碰 EMA20，但收盘价深度跌破（-1%）→ ema_retest=False"""
    monkeypatch.setattr("shared.judges.EMA_RETEST_ENABLED", True, raising=False)
    ind = _base_ind("LONG")
    ema20 = ind["ema20_1h"]  # 50000

    # 最低价触碰，但收盘 = ema20 * 0.985（跌破 -1.5%，超出 0.3% 容差）
    broken_close = ema20 * 0.985
    ind["price"] = broken_close
    k1h = _make_k1h(direction="LONG", close=broken_close, low=ema20 - 50, high=ema20 + 200)
    k4h = _make_k4h("LONG")
    k15 = _make_k15("LONG")

    ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=2.0, funding=0.0)
    if ok:
        assert out.get("ema_retest") is False, f"收盘深度跌破应 ema_retest=False，实际={out.get('ema_retest')}"


# ──────────────────────────────────────────────────────────────────────────────
# 测试3：LONG 价格未触碰 EMA20（离得较远）→ ema_retest=False
# ──────────────────────────────────────────────────────────────────────────────
def test_ema_retest_long_no_touch(monkeypatch):
    """近3根最低价远离 EMA20（-2%）→ ema_retest=False"""
    monkeypatch.setattr("shared.judges.EMA_RETEST_ENABLED", True, raising=False)
    ind = _base_ind("LONG")
    ema20 = ind["ema20_1h"]  # 50000

    # 最低价 = ema20 * 0.98（低于 EMA20 2%，超出触碰容差）
    far_low = ema20 * 0.98
    ind["price"] = ema20 + 300
    k1h = _make_k1h(direction="LONG", close=ema20 + 300, low=far_low, high=ema20 + 600)
    k4h = _make_k4h("LONG")
    k15 = _make_k15("LONG")

    ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=2.0, funding=0.0)
    if ok:
        assert out.get("ema_retest") is False, f"未触碰应 ema_retest=False，实际={out.get('ema_retest')}"


# ──────────────────────────────────────────────────────────────────────────────
# 测试4：EMA_RETEST_ENABLED=False → ema_retest 不出现或为 False
# ──────────────────────────────────────────────────────────────────────────────
def test_ema_retest_disabled(monkeypatch):
    """总开关关闭时，即使触碰 EMA20 也不升级"""
    monkeypatch.setattr("shared.judges.EMA_RETEST_ENABLED", False, raising=False)
    ind = _base_ind("LONG")
    ema20 = ind["ema20_1h"]

    k1h = _make_k1h(direction="LONG", close=ema20 + 100, low=ema20 - 50, high=ema20 + 400)
    k4h = _make_k4h("LONG")
    k15 = _make_k15("LONG")

    ok, out = judge_pullback(k1h, k4h, k15, ind, chg24=2.0, funding=0.0)
    if ok:
        # 开关关闭时 ema_retest 应为 False
        assert out.get("ema_retest") is False, f"开关关闭时应 ema_retest=False，实际={out.get('ema_retest')}"
