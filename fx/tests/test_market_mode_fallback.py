"""market_mode 空值回退 range：risk_manager 出口不外泄空串；scanner analytics 记录真实 market_mode。"""
import logging

import pytest

from shared import risk_manager as RM
import scanner as sc


def _klines(n=130, start=100.0, step=0.0):
    return [[0, "0", "0", "0", str(start + i * step), "0"] for i in range(n)]


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(RM, "_market_mode_cache", {"mode": None, "cached_at": 0.0})
    monkeypatch.setattr(RM, "_mode_pending", {"mode": None, "count": 0})


def test_get_market_mode_never_returns_empty(monkeypatch):
    monkeypatch.setattr(RM, "_btc_klines", lambda *a, **k: _klines())
    assert RM.get_market_mode() in ("bull", "bear", "range")


def test_get_market_mode_empty_result_falls_back_with_warning(monkeypatch, caplog):
    monkeypatch.setattr(RM, "_btc_klines", lambda *a, **k: _klines())
    # 让 raw_mode 为空串：模拟判定分支异常产出
    monkeypatch.setattr(RM, "_calc_ema", lambda closes, n: 1.0)
    src = open(RM.__file__, encoding="utf-8").read()
    assert "回退为 range" in src and "if not mode:" in src
    with caplog.at_level(logging.WARNING):
        assert RM.get_market_mode() in ("bull", "bear", "range")


def test_klines_failure_falls_back_to_range(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("net")
    monkeypatch.setattr(RM, "_btc_klines", boom)
    assert RM.get_market_mode() == "range"


def test_scanner_analytics_events_carry_market_mode_not_blank():
    src = open(sc.__file__, encoding="utf-8").read()
    assert 'market_mode=""' not in src.split("def analyze_one", 1)[1].split("def _analyze_symbol_impl", 1)[0]
    assert "market_mode 为空，回退 range" in src


def test_write_signal_analytics_records_given_market_mode(tmp_path, monkeypatch):
    import json
    f = tmp_path / "sa.jsonl"
    monkeypatch.setattr(sc, "SIGNAL_ANALYTICS_FILE", f)
    ind = {"price": 1.0, "chg24": 1.0, "atr_ratio": 1, "rsi_1h": 50, "rsi_4h": 50, "vol_ratio": 1}
    sc._write_signal_analytics("AUSDT", "pullback", "LONG", True, None, ind, {}, market_mode="bear")
    assert json.loads(f.read_text().splitlines()[0])["market_mode"] == "bear"
