"""K 线请求耗时统计（阶段0）。运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_scanner_kline_timing.py（mock）"""
import os, sys, time
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import scanner as sc


def _impl_calling(n_calls, delay=0.0):
    def impl(symbol, *a, **k):
        for _ in range(n_calls):
            sc._kline_fetch(symbol, "1h", 100)
        return None
    return impl


@pytest.fixture
def fake_api(monkeypatch):
    def gk(symbol, interval, limit):
        time.sleep(0.002)
        return [[0]] * 3
    monkeypatch.setattr(sc.api, "get_klines", gk)
    sc._kline_round_reset()


def test_wrapper_accumulates_per_symbol_and_round(fake_api, monkeypatch):
    monkeypatch.setattr(sc, "_analyze_symbol_impl", _impl_calling(3))
    sc.analyze_symbol("AUSDT"); sc.analyze_symbol("BUSDT")
    assert sc._KL_ROUND["calls"] == 6 and set(sc._KL_ROUND["per_sym"]) == {"AUSDT", "BUSDT"}
    assert sc._KL_ROUND["per_sym"]["AUSDT"][0] == 3 and sc._KL_ROUND["ms"] > 0


def test_summary_line_format(fake_api, monkeypatch):
    monkeypatch.setattr(sc, "_analyze_symbol_impl", _impl_calling(2))
    sc.analyze_symbol("AUSDT")
    line = sc._kline_round_summary()[0]
    assert line.startswith("[scanner] 本轮K线请求: 总次数=2 总耗时=") and "平均每标的=" in line and "最慢标的=AUSDT(" in line


def test_disabled_switch_skips_statistics(fake_api, monkeypatch):
    import config
    monkeypatch.setattr(config, "SCAN_KLINE_TIMING", False, raising=False)
    monkeypatch.setattr(sc, "_analyze_symbol_impl", _impl_calling(2))
    sc.analyze_symbol("AUSDT")
    assert sc._KL_ROUND["calls"] == 0 and sc._kline_round_summary() == []


def test_wrapper_returns_impl_result_and_clears_tls(fake_api, monkeypatch):
    monkeypatch.setattr(sc, "_analyze_symbol_impl", lambda *a, **k: {"ok": 1})
    assert sc.analyze_symbol("AUSDT") == {"ok": 1}
    assert getattr(sc._KL_TLS, "acc", None) is None


def test_analyze_symbol_still_accepts_mark_price_map_positionally():
    import inspect
    assert "mark_price_map" in inspect.signature(sc.analyze_symbol).parameters   # functools.wraps 保留原签名


def test_summary_logged_after_parallel_phase():
    src = (Path(__file__).parent.parent / "scanner.py").read_text()
    assert "_kline_round_reset()" in src and "for _kl_line in _kline_round_summary():" in src
