"""market_data_hub 阶段一验收（全 mock，不联网）。"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

from shared import market_data_hub as H
import scanner as sc


class Clock:
    def __init__(self): self.t = 1000.0
    def __call__(self): return self.t


def frame(*syms, price="100.5", rate="0.0001"):
    return json.dumps([{"e": "markPriceUpdate", "E": 1700000000000, "s": s, "p": price, "i": "100.4",
                        "P": "100.6", "r": rate, "T": 1700003600000} for s in syms])


@pytest.fixture
def hub():
    clk = Clock()
    h = H.MarketDataHub(url="wss://x", clock=clk)
    h.clk = clk
    return h


@pytest.fixture(autouse=True)
def _clean_default(monkeypatch):
    monkeypatch.setattr(H, "_default", None)


# ── 数据正确性 ──────────────────────────────────────────────────────────
def test_not_ready_before_first_frame(hub):
    assert hub.is_ready() is False
    assert hub.get_mark_price("BTCUSDT") is None and hub.get_all_mark_prices() == {}


def test_frame_parsed_with_rest_compatible_fields(hub):
    assert hub._handle_frame(frame("BTCUSDT", "ETHUSDT")) == 2
    d = hub.get_mark_price("BTCUSDT")
    assert hub.is_ready()
    assert d["symbol"] == "BTCUSDT" and d["markPrice"] == "100.5"
    assert d["lastFundingRate"] == "0.0001" and d["nextFundingTime"] == 1700003600000
    assert d["age_sec"] == 0 and hub.frame_count == 1
    assert set(hub.get_all_mark_prices()) == {"BTCUSDT", "ETHUSDT"}


def test_later_frame_updates_value_and_keeps_other_symbols(hub):
    hub._handle_frame(frame("BTCUSDT", "ETHUSDT"))
    hub._handle_frame(frame("BTCUSDT", price="101"))
    assert hub.get_mark_price("BTCUSDT")["markPrice"] == "101"
    assert hub.get_mark_price("ETHUSDT")["markPrice"] == "100.5"


def test_bad_frames_ignored(hub):
    for bad in ("not json", json.dumps({"x": 1}), json.dumps([{"e": "other", "s": "A", "p": "1"}]), json.dumps([])):
        assert hub._handle_frame(bad) == 0
    assert hub.is_ready() is False


def test_snapshot_returned_to_caller_is_isolated(hub):
    hub._handle_frame(frame("BTCUSDT"))
    d = hub.get_mark_price("BTCUSDT"); d["markPrice"] = "999"
    assert hub.get_mark_price("BTCUSDT")["markPrice"] == "100.5"


# ── age 阈值 ────────────────────────────────────────────────────────────
def test_single_symbol_older_than_5s_returns_none(hub):
    hub._handle_frame(frame("BTCUSDT"))
    hub.clk.t += 4.9
    assert hub.get_mark_price("BTCUSDT") is not None
    hub.clk.t += 0.2                                  # 5.1s
    assert hub.get_mark_price("BTCUSDT") is None


def test_all_snapshot_filters_older_than_10s(hub):
    hub._handle_frame(frame("OLDUSDT"))
    hub.clk.t += 8
    hub._handle_frame(frame("NEWUSDT"))
    hub.clk.t += 3                                    # OLD age=11, NEW age=3
    assert set(hub.get_all_mark_prices()) == {"NEWUSDT"}
    assert hub.get_all_mark_prices()["NEWUSDT"]["age_sec"] == 3


# ── 断线重连：期间不返回旧数据 ────────────────────────────────────────────
def test_disconnect_clears_cache_until_new_frame(hub):
    hub._handle_frame(frame("BTCUSDT"))
    assert hub.get_mark_price("BTCUSDT") is not None
    hub._on_disconnect()
    assert hub.is_ready() is False
    assert hub.get_mark_price("BTCUSDT") is None and hub.get_all_mark_prices() == {}
    hub._handle_frame(frame("BTCUSDT", price="102"))   # 重连后新帧
    assert hub.is_ready() and hub.get_mark_price("BTCUSDT")["markPrice"] == "102"


def test_reconnect_loop_counts_logs_warning_and_clears(monkeypatch, caplog):
    """用假 websockets：第 1 次连接收一帧后断线，第 2 次收到新帧；断线期间缓存为空，重连计数 +1 并记 warning。"""
    import asyncio, types
    h = H.MarketDataHub(url="wss://x")
    monkeypatch.setattr(H, "RECONNECT_BASE", 0.01)
    seen_during_gap = []
    conns = []

    class FakeWS:
        def __init__(self, frames): self.frames = list(frames)
        async def recv(self):
            if self.frames:
                return self.frames.pop(0)
            raise ConnectionError("closed")
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    def fake_connect(url, **kw):
        conns.append(url)
        if len(conns) == 1:
            return FakeWS([frame("BTCUSDT")])
        seen_during_gap.append((h.get_mark_price("BTCUSDT"), h.is_ready()))
        h._stopping = True                              # 第 2 次连接后让循环结束
        return FakeWS([frame("BTCUSDT", price="103")])

    fake_mod = types.SimpleNamespace(connect=fake_connect)
    monkeypatch.setitem(sys.modules, "websockets", fake_mod)

    async def go():
        h._stop_event = asyncio.Event()
        await h._connect_loop()

    with caplog.at_level("WARNING", logger="market_data_hub"):
        asyncio.run(go())
    assert h.reconnect_count == 1
    assert seen_during_gap == [(None, False)]           # 重连期间读不到旧数据
    assert any("重连第 1 次" in r.getMessage() for r in caplog.records)


# ── 模块级接口 / 默认实例 ────────────────────────────────────────────────
def test_module_functions_empty_when_not_started():
    assert H.is_ready() is False and H.get_mark_price("X") is None and H.get_all_mark_prices() == {}


def test_module_functions_read_registered_default(monkeypatch, hub):
    hub._handle_frame(frame("BTCUSDT"))
    monkeypatch.setattr(H, "_default", hub)
    assert H.is_ready() and H.get_mark_price("BTCUSDT")["markPrice"] == "100.5"


def test_stats_fields(hub):
    hub._handle_frame(frame("A", "B"))
    s = hub.stats()
    assert s["frame_count"] == 1 and s["symbols"] == 2 and s["reconnect_count"] == 0 and s["ready"]


# ── scanner 接入：开关关闭时 hub 接口不被调用 ─────────────────────────────
def _boom(*a, **k): raise AssertionError("开关关闭时不应调用 hub")


def test_flag_off_never_touches_hub(monkeypatch):
    import config
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", False, raising=False)
    for n in ("is_ready", "get_mark_price", "get_all_mark_prices"):
        monkeypatch.setattr(H, n, _boom)
    monkeypatch.setattr(sc.api, "get_mark_price", lambda s: {"symbol": s, "markPrice": "1", "src": "rest"})
    monkeypatch.setattr(sc.api, "get_all_mark_prices", lambda: [{"symbol": "A", "src": "rest"}])
    assert sc._get_mark_price("A")["src"] == "rest"
    assert sc._get_all_mark_prices_list() == [{"symbol": "A", "src": "rest"}]


def test_default_config_flag_is_true():
    import config
    assert config.MARKET_DATA_HUB_ENABLED is True


def test_stream_urls_use_market_path():
    import config
    from shared import ws_listener as W
    assert H.STREAM_PATH == "/market/ws/!markPrice@arr@1s"
    assert H.MarketDataHub()._url == config.BINANCE_WS_URL + "/market/ws/!markPrice@arr@1s"
    assert W.MARK_PRICE_URL_TPL.endswith("/market/stream?streams={streams}")
    assert W.FORCE_ORDER_URL.endswith("/market/ws/!forceOrder@arr")
    assert W.USER_DATA_URL_TPL.endswith("/ws/{listen_key}") and "/market/" not in W.USER_DATA_URL_TPL


def test_flag_on_ready_uses_hub(monkeypatch, hub):
    import config
    hub._handle_frame(frame("BTCUSDT"))
    monkeypatch.setattr(H, "_default", hub)
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", True, raising=False)
    monkeypatch.setattr(sc.api, "get_mark_price", lambda s: pytest.fail("不应走 REST"))
    monkeypatch.setattr(sc.api, "get_all_mark_prices", lambda: pytest.fail("不应走 REST"))
    assert sc._get_mark_price("BTCUSDT")["markPrice"] == "100.5"
    lst = sc._get_all_mark_prices_list()
    assert lst[0]["symbol"] == "BTCUSDT" and "lastFundingRate" in lst[0]


def test_flag_on_not_ready_or_stale_falls_back_to_rest(monkeypatch, hub):
    import config
    monkeypatch.setattr(H, "_default", hub)             # 未就绪
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", True, raising=False)
    monkeypatch.setattr(sc.api, "get_mark_price", lambda s: {"src": "rest"})
    monkeypatch.setattr(sc.api, "get_all_mark_prices", lambda: [{"src": "rest"}])
    assert sc._get_mark_price("BTCUSDT") == {"src": "rest"}
    assert sc._get_all_mark_prices_list() == [{"src": "rest"}]
    hub._handle_frame(frame("BTCUSDT")); hub.clk.t += 6   # 就绪但单标的已过旧
    assert sc._get_mark_price("BTCUSDT") == {"src": "rest"}
    hub.clk.t += 10                                       # 全量也过旧 → 快照为空 → REST
    assert sc._get_all_mark_prices_list() == [{"src": "rest"}]


def test_flag_on_hub_exception_falls_back(monkeypatch):
    import config
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", True, raising=False)
    monkeypatch.setattr(H, "is_ready", _boom)
    monkeypatch.setattr(sc.api, "get_mark_price", lambda s: {"src": "rest"})
    assert sc._get_mark_price("A") == {"src": "rest"}


def test_scanner_call_sites_use_helpers():
    src = (Path(__file__).parent.parent / "scanner.py").read_text()
    assert "or _get_mark_price(symbol)" in src
    assert "for _m in (_get_all_mark_prices_list() or [])" in src
    assert '_get_mark_price(sig["symbol"])' in src
