"""risk_manager 的 BTC 行情优先读 hub 缓存，hub 返回 None 才回退 REST。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from shared import risk_manager as R
from shared import market_data_hub as H
from shared import binance_api as api

K = [[0, "1", "2", "0.5", "1.5", "1", 1, "0", 0, "0", "0", "0"]]


def test_klines_hub_hit_no_rest(monkeypatch):
    monkeypatch.setattr(H, "get_klines", lambda s, i, n: K)
    monkeypatch.setattr(api, "get_klines", lambda *a, **k: (_ for _ in ()).throw(AssertionError("不应走 REST")))
    assert R._btc_klines("1h", 3) is K


def test_klines_hub_none_falls_back_to_rest(monkeypatch):
    seen = []
    monkeypatch.setattr(H, "get_klines", lambda s, i, n: None)
    monkeypatch.setattr(api, "get_klines", lambda s, i, limit=0: seen.append((s, i, limit)) or K)
    assert R._btc_klines("4h", 120) is K and seen == [("BTCUSDT", "4h", 120)]


def test_klines_hub_exception_falls_back(monkeypatch):
    monkeypatch.setattr(H, "get_klines", lambda *a: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(api, "get_klines", lambda *a, **k: K)
    assert R._btc_klines("1h", 3) is K


def test_mark_price_hub_hit_and_fallbacks(monkeypatch):
    hub_md = {"markPrice": "100", "lastFundingRate": "0.0001"}
    rest_md = {"markPrice": "99", "lastFundingRate": "0.0002"}
    monkeypatch.setattr(api, "get_mark_price", lambda s: rest_md)
    monkeypatch.setattr(H, "get_mark_price", lambda s: hub_md)
    assert R._btc_mark_price() is hub_md
    monkeypatch.setattr(H, "get_mark_price", lambda s: None)
    assert R._btc_mark_price() is rest_md
    monkeypatch.setattr(H, "get_mark_price", lambda s: {"markPrice": "100", "lastFundingRate": None})
    assert R._btc_mark_price() is rest_md      # hub 缺资金费率字段 → 回退 REST


def test_hub_keeps_only_100_rows_so_120_requests_miss():
    """已知限制：hub 每个周期只保留 KLINE_HISTORY 根，请求 120 根的 4h 永远回退 REST。"""
    assert H.KLINE_HISTORY < 120
