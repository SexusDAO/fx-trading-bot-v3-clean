"""787f4fb 三处放宽的单元测试（全 mock，不调真实 API）：
reversal 通道3b（LONG/SHORT）、range_bounce 动态 ATR、oi_divergence 动态 RSI。

说明：judge 在方向/门槛判定之后还有 15M 确认、减速、清算等后续检查，
mock K 线不一定能全部通过。因此「通过」的断言口径是：
本次改动所在的那一道判定已放行（未被它拒绝，且带上了对应标记）；
「拒绝」的断言口径是：正是被这一道判定拒绝。
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import shared.judges as J


def _k(close, high=None, low=None, t=0):
    h = high if high is not None else close * 1.002
    l = low if low is not None else close * 0.998
    return [t, str(close), str(h), str(l), str(close), "1000", t + 1, "100000", 10, "500", "50000", "0"]


def _flat(n, px=100.0):
    return [_k(px, t=i) for i in range(n)]


@pytest.fixture(autouse=True)
def _no_btc_crash(monkeypatch):
    monkeypatch.setattr(J, "_btc_crash_reject", lambda *a, **k: None)


# ── 1/2. reversal 通道3b ────────────────────────────────────────────
def _rev(rsi_1h, chg24, rsi_4h):
    ind = {"price": 100.0, "atr_1h": 2.0, "rsi_1h": rsi_1h, "rsi_4h": rsi_4h,
           "bb_pct": 0.5, "symbol": "TESTUSDT", "vol24_usd": 50_000_000}
    return J._judge_reversal_impl(_flat(60), _flat(60), _flat(60), ind, chg24, 0.0001)


def _dir_rejected(out):
    return str(out.get("reject_reason") or "").startswith("方向不明确")


def test_rev_long_3b_pass():
    ok, out = _rev(19, -3.5, 35)
    assert not _dir_rejected(out)
    assert out.get("direction") == "long"
    assert out.get("reversal_channel") == "3b_extreme"
    assert out.get("position_size_cap", 0) <= 0.5


@pytest.mark.parametrize("rsi_1h,chg24,rsi_4h", [(21, -3.5, 35), (19, -2.5, 35), (19, -3.5, 45)])
def test_rev_long_3b_reject(rsi_1h, chg24, rsi_4h):
    ok, out = _rev(rsi_1h, chg24, rsi_4h)
    assert ok is False and _dir_rejected(out)


def test_rev_short_3b_pass():
    ok, out = _rev(81, 3.5, 65)
    assert not _dir_rejected(out)
    assert out.get("direction") == "short"


def test_rev_short_3b_reject():
    ok, out = _rev(79, 3.5, 65)
    assert ok is False and _dir_rejected(out)


# ── 3. range_bounce 动态 ATR ────────────────────────────────────────
def _rb_k1h(peak):
    """前 8 根 100 → 6 根拉升到 peak（第 13 根为高点）→ 6 根在 [peak/1.08, peak] 横盘；末尾 1 根未闭合。"""
    closes = [100.0] * 8
    closes += [100 + (peak - 100) * i / 6 for i in range(1, 7)]
    lo = peak / 1.08
    closes += [lo, peak * 0.99, lo * 1.01, peak * 0.98, lo * 1.005, lo * 1.01]
    return [_k(c, t=i) for i, c in enumerate(closes)] + [_k(closes[-1], t=len(closes))]


def _rb(peak, atr_r):
    k1h = _rb_k1h(peak)
    price = float(k1h[-2][4])
    ind = {"price": price, "atr_ratio": atr_r, "atr_1h": price * atr_r / 100, "rsi_1h": 45,
           "rsi_4h": 55, "symbol": "TESTUSDT", "_weekend_mode": False}
    return J._judge_range_bounce_impl(k1h, _flat(60), _flat(40), ind, 2.0, 0.0001)


def _atr_rejected(out):
    return str(out.get("reject_reason") or "").startswith("ATR%=")


def test_rb_dynamic_atr_pass():
    ok, out = _rb(135.0, 1.6)                     # pre_gain=35%，c_range≈8%
    assert "大涨幅度不足" not in str(out.get("reject_reason")) and "横盘区间过宽" not in str(out.get("reject_reason"))
    assert not _atr_rejected(out)


def test_rb_dynamic_atr_reject():
    ok, out = _rb(125.0, 1.6)                     # pre_gain=25%，门槛仍 2.0%
    assert ok is False and _atr_rejected(out)
    assert "<2.0%" in out["reject_reason"]


# ── 4. oi_divergence 动态 RSI ───────────────────────────────────────
def _oi(oi_change, rsi_1h):
    ind = {"price": 100.0, "atr_1h": 2.0, "rsi_1h": rsi_1h, "symbol": "TESTUSDT",
           "oi_now": 1_000_000 * (1 + oi_change), "oi_1h_ago": 1_000_000,
           "high_24h": 110.0, "low_24h": 99.0, "taker_buy_ratio_now": 0.55}
    return J._judge_oi_divergence_impl(_flat(60), _flat(60), _flat(40), ind, -5.0, 0.0001)


def _rsi_rejected(out):
    r = str(out.get("reject_reason") or "")
    return "RSI 过滤" in r or "RSI 区间" in r


def test_oi_dynamic_rsi_pass():
    ok, out = _oi(0.25, 40)
    assert out.get("direction") == "long"
    assert out.get("rsi_relax") == 4
    assert not _rsi_rejected(out)


def test_oi_dynamic_rsi_reject():
    ok, out = _oi(0.15, 40)
    assert ok is False and _rsi_rejected(out)
    assert "> 38" in out["reject_reason"]
