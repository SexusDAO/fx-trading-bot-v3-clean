"""judges 优化 6 处（全 mock，不联网）。golden 未覆盖【1】【3】【4】【6】的变化，由本文件验证。"""
import socket
import sys
import time
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import config
from shared import judges as J
from test_judge import _k, _trending_k1h, _flat_k1h, _ind


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def _no_net(*a, **k):
        raise OSError("network disabled in test")
    monkeypatch.setattr(socket.socket, "connect", _no_net)
    monkeypatch.setattr(socket, "create_connection", _no_net)


def _rr(out):
    return str((out or {}).get("reject_reason") or "")


# ═══ 【1】pullback 量比用已收盘 K 线 ═══
def _pullback(vol_closed):
    k1h = _trending_k1h(49000, direction="up")
    k4h = _trending_k1h(47000, direction="up")
    k15 = [_k(50000, 50100, 49800, 50080), _k(50080, 50100, 49600, 50070), _k(50070, 50100, 49300, 50090)]
    ind = _ind(price=50000, atr=600, ema20_1h=50100, ema50_1h=49200, ema20_4h=49500, ema50_4h=48000,
               vol_ratio=1.0)
    ind["vol_ratio_closed"] = vol_closed
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
        return J.judge_pullback(k1h, k4h, k15, ind, chg24=2.0, funding=0.0003)


def test_1_pullback_vol_closed_low_rejected():
    ok, out = _pullback(0.35)
    assert ok is False and "量能门槛" in _rr(out) and "0.35" in _rr(out)


def test_1_pullback_vol_closed_ok_passes_gate():
    ok, out = _pullback(0.45)
    assert "量能门槛" not in _rr(out)       # vol_ratio_closed 优先于 vol_ratio 被读取


# ═══ 【2】reversal SHORT：4H 多头下的衰竭证据 ═══
def _rev_short(**ev):
    k1h = _trending_k1h(45000, n=40, direction="up")
    k4h = _trending_k1h(40000, n=40, direction="up")
    k15 = _flat_k1h(50000, n=20)
    ind = _ind(price=50000, atr=800, rsi_1h=75, rsi_4h=70, ema20_4h=51000, ema50_4h=49000)
    ind.update(vol24_usd=50e6, taker_buy_ratio=0.55, taker_buy_ratio_now=0.55, oi_now=100000, oi_1h_ago=100000)
    ind.update(ev)
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
         patch("shared.state._load_oi_cache", return_value={}), \
         patch("shared.state.set_cooldown"), \
         patch.object(J, "_detect_rsi_divergence", return_value={"divergence": False}):
        return J.judge_reversal(k1h, k4h, k15, ind, chg24=8.0, funding=0.0003)


def test_2_reversal_short_two_evidence_allowed():
    ok, out = _rev_short(taker_buy_ratio_now=0.45, oi_now=95000)   # 买入占比下降 + OI 降 5%
    assert "衰竭证据不足" not in _rr(out) and "拒绝逆势做空" not in _rr(out)
    assert out.get("exhaustion_short") is True
    assert set(out["exhaustion_evidence"]) == {"主动买入占比下降(1H口径)", "OI 1H下降>3%"}
    if ok:
        assert out["position_size_cap"] <= 0.5 * 0.7 + 1e-9    # 降仓 ×0.7


def test_3_reversal_short_one_evidence_rejected():
    ok, out = _rev_short(taker_buy_ratio_now=0.45)
    assert ok is False and "衰竭证据不足" in _rr(out) and "1/2" in _rr(out) and "1H口径" in _rr(out)


# ═══ 【3】funding_arb ═══
def _arb(funding, **cfg):
    ind = {"rsi_4h": 60, "atr_1h": 1500, "atr_ratio": 3.0, "price": 50000, "oi_now": 1e6, "symbol": "XUSDT"}
    with patch.multiple(config, **cfg) if cfg else patch.object(config, "FUNDING_ARB_MIN_RATE_SHORT",
                                                                config.FUNDING_ARB_MIN_RATE_SHORT):
        return J.judge_funding_arb("XUSDT", funding, [funding] * 3, ind, time.time() + 3600)


def test_4_funding_arb_006_rejected_by_direction_gate():
    # 0.08% 手续费检查在方向门槛(±0.1%)之后，0.06% 先被方向门槛挡住
    ok, out = _arb(0.0006)
    assert ok is False and "不满足任何方向触发门槛" in _rr(out)


def test_4b_funding_arb_fee_floor_itself():
    # 临时把方向门槛降到 0.05%，让 0.06% 走到 0.08% 检查
    ok, out = _arb(0.0006, FUNDING_ARB_MIN_RATE_SHORT=0.0005)
    assert ok is False and _rr(out) == "资金费率0.0600%不足以覆盖手续费0.08%"
    ok, out = _arb(0.0009, FUNDING_ARB_MIN_RATE_SHORT=0.0005)
    assert "手续费" not in _rr(out)


def test_5_funding_arb_010_not_blocked_by_atr_formula():
    # 旧公式：0.10% < ATR 3%×0.5=1.5% 会被拒；删除后不再因盈亏比/手续费被拒
    ok, out = _arb(0.0010)
    assert "盈亏比" not in _rr(out) and "手续费" not in _rr(out)


# ═══ 【4】stop_hunt 强平量软加减分 ═══
def test_6_stop_hunt_liq_insufficient_minus5():
    assert J._liq_volume_adj(2000, 5000, 2.5) == -5
    src = Path(J.__file__).read_text(encoding="utf-8")
    assert 'f"liquidation_volume_insufficient: "' not in src       # 不再生成硬拒原因（注释里的旧说明不算）
    assert 'out["strategy_adj"] = out.get("strategy_adj", 0) + _4h_strategy_adj' in src   # 累加


def test_7_stop_hunt_liq_enough_plus10():
    assert J._liq_volume_adj(20000, 5000, 2.5) == 10
    assert J._liq_volume_adj(50, 500, 2.5) == 0                    # 基准不足不处理


# ═══ 【5】BTC 急跌保护 ═══
def test_8_btc_crash_blocks_reversal_long():
    k1h = _flat_k1h(50000, n=40)
    ind = _ind(price=50000, atr=1500, rsi_1h=28, rsi_4h=22)
    ind.update(vol24_usd=50e6, btc_1h_drop_pct=-5.5)
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]), \
         patch("shared.state._load_oi_cache", return_value={}), patch("shared.state.set_cooldown"):
        ok, out = J.judge_reversal(k1h, _flat_k1h(48000, n=40), _flat_k1h(50000, n=20), ind,
                                   chg24=-18.0, funding=-0.0015)
    assert ok is False and "BTC急跌保护" in _rr(out) and "reversal LONG" in _rr(out)


def test_9_btc_crash_blocks_range_bounce():
    ind = _ind(price=50000, atr=1500)
    ind["btc_1h_drop_pct"] = -5.5
    ok, out = J.judge_range_bounce(_flat_k1h(50000, n=30), _flat_k1h(50000), [], ind, 1.0, 0.0)
    assert ok is False and "BTC急跌保护" in _rr(out) and "range_bounce LONG" in _rr(out)


# ═══ 【6】ema_trend SHORT 对称参数 ═══
def _ema_short(rsi_4h):
    ind = _ind(price=48500, atr=700, rsi_1h=55, rsi_4h=rsi_4h, ema20_4h=48500, ema50_4h=50000)
    ind["vol24_usd"] = 50e6
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
        return J.judge_ema_trend(_trending_k1h(50000, n=60, direction="down"),
                                 _trending_k1h(52000, n=60, direction="down"), _flat_k1h(48500, n=20),
                                 ind, chg24=-2.0, funding=0.0)


def test_10_ema_trend_short_rsi4h_60_rejected():
    ok, out = _ema_short(60)
    assert ok is False and "4H RSI=60.0" in _rr(out) and "[28, 55]" in _rr(out)


def test_11_ema_trend_short_rsi4h_48_passes_gate():
    ok, out = _ema_short(48)
    assert "4H RSI" not in _rr(out)
