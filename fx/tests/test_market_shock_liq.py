"""risk_manager 全市场 1H 清算量：真实 coin-list 字段名。全部 mock，不联网。"""
import sys
import types
import pytest
import shared.risk_manager as R


def _install(monkeypatch, coin_list, btc=None):
    mod = types.ModuleType("shared.clawby_adapter")
    mod.get_liq_coin_list = lambda: coin_list
    mod.get_liq_summary_1h = lambda sym: btc or {"long_liq_1h": 0.0, "short_liq_1h": 0.0}
    monkeypatch.setitem(sys.modules, "shared.clawby_adapter", mod)


def test_market_shock_liq_fields_case1_1h_sum(monkeypatch):
    _install(monkeypatch, [{"symbol": "BTC", "liquidation_usd_1h": "100", "liquidation_usd_24h": "9999"},
                           {"symbol": "ETH", "liquidation_usd_1h": 50.5}, {"symbol": "X"}, "bad"])
    assert R._market_liq_1h() == pytest.approx(150.5)


def test_market_shock_liq_fields_case2_24h_div_24(monkeypatch):
    _install(monkeypatch, [{"symbol": "BTC", "liquidation_usd_24h": "2400"}, {"symbol": "ETH", "liquidation_usd_24h": "2400"}])
    assert R._market_liq_1h() == pytest.approx(200.0)


def test_market_shock_liq_fields_case2b_old_names_ignored(monkeypatch):
    _install(monkeypatch, [{"symbol": "BTC", "liq_usd": 1e9, "liquidations": 1e9}])
    assert R._market_liq_1h() == 0.0


def test_market_shock_liq_fields_case3_empty_uses_btc_x5(monkeypatch):
    _install(monkeypatch, [], btc={"long_liq_1h": 100.0, "short_liq_1h": 20.0})
    assert R._market_liq_1h() == pytest.approx(600.0)


def test_check_market_shock_triggers_on_1h_liq(monkeypatch):
    _install(monkeypatch, [{"symbol": "BTC", "liquidation_usd_1h": "600000000"}])
    monkeypatch.setattr(R, "is_market_shock_active", lambda: False)
    monkeypatch.setattr(R, "_btc_klines", lambda *a, **k: [])
    saved = []
    monkeypatch.setattr(R, "_save_shock_state", lambda st: saved.append(st))
    assert R.check_market_shock() is True and "全市场1H清算量" in saved[0]["reason"]


def test_check_market_shock_not_triggered_below_threshold(monkeypatch):
    _install(monkeypatch, [{"symbol": "BTC", "liquidation_usd_1h": "8000000"}])
    monkeypatch.setattr(R, "is_market_shock_active", lambda: False)
    monkeypatch.setattr(R, "_btc_klines", lambda *a, **k: [])
    monkeypatch.setattr(R, "_save_shock_state", lambda st: (_ for _ in ()).throw(AssertionError("不应熔断")))
    assert R.check_market_shock() is False
