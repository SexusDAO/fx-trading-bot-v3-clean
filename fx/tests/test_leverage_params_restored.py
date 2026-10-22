"""PR A：杠杆预算参数已恢复，且缺失时有告警。运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_leverage_params_restored.py"""
import logging, os, sys
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import config
import executor

EXPECT = {
    "LEVERAGE_UTILIZATION_ENABLED": True, "LEVERAGE_TARGET": 5.0, "MAX_SINGLE_NOMINAL_RATIO": 1.5,
    "LEVERAGE_LONG_RATIO_BULL_EARLY": 0.75, "LEVERAGE_SHORT_RATIO_BULL_EARLY": 0.25,
    "LEVERAGE_LONG_RATIO_BULL_MID": 0.70, "LEVERAGE_SHORT_RATIO_BULL_MID": 0.30,
    "LEVERAGE_LONG_RATIO_BULL_LATE": 0.55, "LEVERAGE_SHORT_RATIO_BULL_LATE": 0.45,
    "LEVERAGE_LONG_RATIO_RANGE": 0.50, "LEVERAGE_SHORT_RATIO_RANGE": 0.50,
    "LEVERAGE_LONG_RATIO_BEAR_WEAK": 0.40, "LEVERAGE_SHORT_RATIO_BEAR_WEAK": 0.60,
    "LEVERAGE_LONG_RATIO_BEAR_MODERATE": 0.30, "LEVERAGE_SHORT_RATIO_BEAR_MODERATE": 0.70,
    "LEVERAGE_LONG_RATIO_BEAR_STRONG": 0.20, "LEVERAGE_SHORT_RATIO_BEAR_STRONG": 0.80,
}


@pytest.mark.parametrize("name,val", list(EXPECT.items()))
def test_param_restored(name, val):
    assert getattr(config, name) == val


@pytest.mark.parametrize("tier", ["BULL_EARLY", "BULL_MID", "BULL_LATE", "RANGE", "BEAR_WEAK", "BEAR_MODERATE", "BEAR_STRONG"])
def test_long_plus_short_is_one(tier):
    assert getattr(config, f"LEVERAGE_LONG_RATIO_{tier}") + getattr(config, f"LEVERAGE_SHORT_RATIO_{tier}") == pytest.approx(1.0)


def test_missing_switch_warns_and_still_lets_through(monkeypatch, caplog):
    monkeypatch.delattr(config, "LEVERAGE_UTILIZATION_ENABLED")
    with caplog.at_level(logging.WARNING):
        out = executor._apply_leverage_budget("XUSDT", "LONG", 500.0, 300.0)
    assert out == (True, 500.0, "")
    assert any("LEVERAGE_UTILIZATION_ENABLED 读取失败，杠杆预算已跳过" in r.getMessage() for r in caplog.records)


def test_explicit_false_is_silent(monkeypatch, caplog):
    monkeypatch.setattr(config, "LEVERAGE_UTILIZATION_ENABLED", False)
    with caplog.at_level(logging.WARNING):
        assert executor._apply_leverage_budget("XUSDT", "LONG", 500.0, 300.0) == (True, 500.0, "")
    assert not caplog.records


def test_enabled_truncates_to_single_cap_at_300u(monkeypatch):
    from shared import risk_manager as rm
    monkeypatch.setattr(rm, "get_market_mode", lambda: "range")
    monkeypatch.setattr("shared.state.get_all_positions", lambda: [])
    ok, nominal, _ = executor._apply_leverage_budget("XUSDT", "LONG", 1000.0, 300.0)
    assert ok and nominal == pytest.approx(450.0)           # min(预算 750, 单笔上限 450)
