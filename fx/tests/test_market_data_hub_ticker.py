"""market_data_hub 的 miniTicker 部分 + scanner 读取 hub 24h 行情的回落逻辑（全 mock）。"""
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

from shared import market_data_hub as H
import scanner as sc


class Clock:
    def __init__(self): self.t = 1000.0
    def __call__(self): return self.t


def tick(sym, c="110", o="100", h="120", l="95", v="5", q="500"):
    return {"e": "24hrMiniTicker", "E": 1, "s": sym, "c": c, "o": o, "h": h, "l": l, "v": v, "q": q}


def mframe(*items):
    return json.dumps({"stream": "!miniTicker@arr", "data": list(items)})


@pytest.fixture
def hub():
    clk = Clock()
    h = H.MarketDataHub(url="wss://x", market_url="wss://y", clock=clk)
    h.clk = clk
    return h


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(H, "_default", None)


def test_ticker_parsed_with_rest_compatible_fields(hub):
    assert hub.is_ticker_ready() is False and hub.get_all_tickers() == {} and hub.get_ticker("BTCUSDT") is None
    hub._handle_market_frame(mframe(tick("BTCUSDT"), tick("ETHUSDT", c="90", o="100")))
    t = hub.get_ticker("BTCUSDT")
    assert t["symbol"] == "BTCUSDT" and t["lastPrice"] == "110" and t["openPrice"] == "100"
    assert t["highPrice"] == "120" and t["lowPrice"] == "95" and t["volume"] == "5" and t["quoteVolume"] == "500"
    assert t["priceChangePercent"] == "10.000" and hub.get_ticker("ETHUSDT")["priceChangePercent"] == "-10.000"
    assert t["age_sec"] == 0 and set(hub.get_all_tickers()) == {"BTCUSDT", "ETHUSDT"}


def test_ticker_age_filter_10s(hub):
    hub._handle_market_frame(mframe(tick("OLDUSDT")))
    hub.clk.t += 8
    hub._handle_market_frame(mframe(tick("NEWUSDT")))
    hub.clk.t += 3                                   # OLD age=11, NEW age=3
    assert set(hub.get_all_tickers()) == {"NEWUSDT"}
    assert hub.get_ticker("OLDUSDT") is None and hub.get_ticker("NEWUSDT") is not None
    assert hub.ticker_coverage() == (1, 2)           # 新鲜 1 / 见过 2


def test_ticker_partial_update_merges(hub):
    hub._handle_market_frame(mframe(tick("A"), tick("B")))
    hub._handle_market_frame(mframe(tick("A", c="111")))   # 只推有变化的
    assert hub.get_ticker("A")["lastPrice"] == "111" and hub.get_ticker("B")["lastPrice"] == "110"


def test_bad_market_frames_ignored(hub):
    for bad in ("nope", json.dumps({"result": None, "id": 1}), json.dumps({"stream": "x", "data": 5}),
                json.dumps({"error": {"code": 2}}), mframe({"s": "A"}), mframe({"s": "A", "c": "x", "o": "1"})):
        hub._handle_market_frame(bad)
    assert hub.is_ticker_ready() is False


def test_market_disconnect_clears_tickers(hub):
    hub._handle_market_frame(mframe(tick("A")))
    hub._on_market_disconnect()
    assert hub.is_ticker_ready() is False and hub.get_all_tickers() == {} and hub.get_ticker("A") is None
    assert hub.ticker_coverage() == (0, 0)


def test_subscribe_message_sent_on_connect(hub):
    import asyncio
    sent = []

    class WS:
        async def send(self, m): sent.append(json.loads(m))

    asyncio.run(hub._on_market_connected(WS()))
    assert sent[0]["method"] == "SUBSCRIBE" and sent[0]["params"] == ["!miniTicker@arr"]


# ── scanner 读取 ──────────────────────────────────────────────────────────
def _enable(monkeypatch, on=True):
    import config
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", on, raising=False)


def _rest(monkeypatch, tag="rest"):
    import shared.binance_api as A
    monkeypatch.setattr(A, "get_ticker_24h_cached", lambda max_age_sec=300: {"X": {"src": tag}})


def test_scanner_flag_off_uses_rest_and_never_touches_hub(monkeypatch):
    _enable(monkeypatch, False); _rest(monkeypatch)
    for n in ("is_ticker_ready", "get_all_tickers", "ticker_coverage"):
        monkeypatch.setattr(H, n, lambda *a, **k: (_ for _ in ()).throw(AssertionError("不应调用 hub")))
    assert sc._get_ticker_24h_cached(120) == {"X": {"src": "rest"}}


def test_scanner_uses_hub_when_ready_and_logs_debug(monkeypatch, hub, caplog):
    _enable(monkeypatch); _rest(monkeypatch)
    hub._handle_market_frame(mframe(tick("BTCUSDT"), tick("ETHUSDT")))
    monkeypatch.setattr(H, "_default", hub)
    with caplog.at_level(logging.DEBUG, logger="fx.scanner"), caplog.at_level(logging.DEBUG):
        out = sc._get_ticker_24h_cached(120)
    assert set(out) == {"BTCUSDT", "ETHUSDT"} and out["BTCUSDT"]["quoteVolume"] == "500"
    assert any("[hub] 24h ticker 2 个标的" in r.getMessage() for r in caplog.records)


def test_scanner_falls_back_when_not_ready_empty_or_low_coverage(monkeypatch, hub):
    _enable(monkeypatch); _rest(monkeypatch)
    monkeypatch.setattr(H, "_default", hub)
    assert sc._get_ticker_24h_cached(120) == {"X": {"src": "rest"}}            # 未就绪
    hub._handle_market_frame(mframe(*[tick(f"S{i}") for i in range(10)]))
    hub.clk.t += 8
    hub._handle_market_frame(mframe(tick("S0")))                                  # 之后只有 1 个更新
    hub.clk.t += 5                                                                # 其余 9 个已过旧，覆盖率 1/10
    assert sc._get_ticker_24h_cached(120) == {"X": {"src": "rest"}}
    hub._on_market_disconnect()                                                   # 断线后为空
    assert sc._get_ticker_24h_cached(120) == {"X": {"src": "rest"}}


def test_scanner_hub_exception_falls_back(monkeypatch):
    _enable(monkeypatch); _rest(monkeypatch)
    monkeypatch.setattr(H, "is_ticker_ready", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert sc._get_ticker_24h_cached(120) == {"X": {"src": "rest"}}


def test_scanner_call_sites_use_helper():
    src = (Path(__file__).parent.parent / "scanner.py").read_text()
    assert "_get_ticker_24h_cached(300)" in src and "_get_ticker_24h_cached(120)" in src
    assert "get_ticker_24h_cached(max_age_sec=300) or" not in src
