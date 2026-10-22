"""hub K 线 / 24h 行情的重连、回落路径与 scanner 接入（全 mock，不联网）。"""
import asyncio
import json
import logging
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

from shared import market_data_hub as H
import scanner as sc

HMS = 3_600_000
T0 = 1_700_000_000_000 - (1_700_000_000_000 % HMS)
LAST = T0 + 200 * HMS


def hist(n=100):
    return [[LAST - (n - 1 - i) * HMS, "1", "2", "0.5", "1.5", "10", LAST - (n - 1 - i) * HMS + HMS - 1, "15", 5, "4", "6", "0"]
            for i in range(n)]


class FakeWS:
    def __init__(self): self.sent = []
    async def send(self, m): self.sent.append(json.loads(m))


def make_hub(fetcher):
    return H.MarketDataHub(url="wss://x", market_url="wss://y", kline_fetcher=fetcher, wall=lambda: (LAST + 1000) / 1000)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(H, "_default", None)


def test_config_symbols_max_is_120():
    import config
    assert config.KLINE_HUB_SYMBOLS_MAX == 120


# ── 断线重连：重新订阅全部流 + 重拉历史 ─────────────────────────────────────
def test_reconnect_resubscribes_all_and_refetches_history():
    calls = []

    async def go():
        h = make_hub(lambda s, iv, n: calls.append((s, iv)) or hist(100))
        h._sync_lock, h._fetch_sem = asyncio.Lock(), asyncio.Semaphore(6)
        h.subscribe_klines(["BTCUSDT", "ETHUSDT"])
        ws1 = FakeWS(); h._market_connected = True
        await h._on_market_connected(ws1)
        await asyncio.gather(*list(h._load_tasks))
        assert h.get_klines("BTCUSDT", "1h", 100) is not None and len(calls) == 6
        h._on_market_disconnect()                                          # 断线：数据全部作废
        assert h.get_klines("BTCUSDT", "1h", 100) is None and h.kline_ready_counts() == (0, 6)
        ws2 = FakeWS(); h._market_connected = True
        await h._on_market_connected(ws2)                                   # 重连
        await asyncio.gather(*list(h._load_tasks))
        subs = {p for m in ws2.sent if m["method"] == "SUBSCRIBE" for p in m["params"]}
        assert "!miniTicker@arr" in subs and len(subs) == 1 + 6             # 重新订阅 ticker + 全部 K 线
        assert len(calls) == 12                                             # 历史重新拉了一遍
        assert h.get_klines("BTCUSDT", "1h", 100) is not None

    asyncio.run(go())


def test_history_result_arriving_after_disconnect_is_discarded():
    gate = threading.Event()

    def slow(s, iv, n):
        gate.wait(5)
        return hist(100)

    async def go():
        h = make_hub(slow)
        h._sync_lock, h._fetch_sem = asyncio.Lock(), asyncio.Semaphore(6)
        h.subscribe_klines(["BTCUSDT"])
        h._market_connected = True
        await h._on_market_connected(FakeWS())
        await asyncio.sleep(0.05)                                           # 拉取已发出但未返回
        h._on_market_disconnect()                                           # 此时断线
        gate.set()
        await asyncio.gather(*list(h._load_tasks))
        h._market_connected = True
        assert h.get_klines("BTCUSDT", "1h", 100) is None                   # 断线前发出的结果不能写入

    asyncio.run(go())


def test_failed_history_fetch_retries_later_not_immediately():
    n = []

    async def go():
        h = make_hub(lambda *a: n.append(1) or [])
        h._sync_lock, h._fetch_sem = asyncio.Lock(), asyncio.Semaphore(6)
        h.subscribe_klines(["BTCUSDT"]); h._market_connected = True
        await h._on_market_connected(FakeWS())
        await asyncio.gather(*list(h._load_tasks))
        first = len(n)
        await h._sync_once()                                                # 紧接着对账：还没到重试时间
        await asyncio.gather(*list(h._load_tasks))
        assert first == 3 and len(n) == first

    asyncio.run(go())


# ── scanner：K 线读取与回落 ────────────────────────────────────────────────
def _flag(monkeypatch, on):
    import config
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", on, raising=False)
    monkeypatch.setattr(config, "KLINE_CACHE_ENABLED", False, raising=False)   # 回落时直接走 REST 包装，便于断言


def _ready_hub(monkeypatch):
    h = make_hub(None)
    h.subscribe_klines(["BTCUSDT"]); h._market_connected = True
    h._series[("BTCUSDT", "1h")].rows = hist(100)
    monkeypatch.setattr(H, "_default", h)
    return h


def test_scanner_klines_prefers_hub_and_logs_debug(monkeypatch, caplog):
    _flag(monkeypatch, True); _ready_hub(monkeypatch)
    monkeypatch.setattr(sc, "_kline_fetch", lambda *a: pytest.fail("hub 就绪时不应走 REST"))
    with caplog.at_level(logging.DEBUG):
        out = sc._get_klines_cached("BTCUSDT", "1h", 100)
    assert len(out) == 100 and out[-1][0] == LAST
    assert any("[hub] BTCUSDT 1h klines n=100" in r.getMessage() for r in caplog.records)


def test_scanner_klines_falls_back_to_rest_when_hub_not_ready(monkeypatch):
    _flag(monkeypatch, True)
    h = make_hub(None); h.subscribe_klines(["BTCUSDT"])                    # 订阅了但未就绪
    monkeypatch.setattr(H, "_default", h)
    monkeypatch.setattr(sc, "_kline_fetch", lambda s, iv, n: [["rest"]])
    assert sc._get_klines_cached("BTCUSDT", "1h", 100) == [["rest"]]
    assert sc._get_klines_cached("ETHUSDT", "1h", 100) == [["rest"]]       # 未订阅
    monkeypatch.setattr(H, "_default", None)                               # hub 未启动
    assert sc._get_klines_cached("BTCUSDT", "1h", 100) == [["rest"]]


def test_scanner_klines_flag_off_never_touches_hub(monkeypatch):
    _flag(monkeypatch, False)
    for n in ("get_klines", "subscribe_klines"):
        monkeypatch.setattr(H, n, lambda *a, **k: (_ for _ in ()).throw(AssertionError("不应调用 hub")))
    monkeypatch.setattr(sc, "_kline_fetch", lambda s, iv, n: [["rest"]])
    assert sc._get_klines_cached("BTCUSDT", "1h", 100) == [["rest"]]
    sc._hub_subscribe_klines(["BTCUSDT"])                                  # 开关关闭：无操作


def test_scanner_klines_hub_exception_falls_back(monkeypatch):
    _flag(monkeypatch, True)
    monkeypatch.setattr(H, "get_klines", lambda *a: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(sc, "_kline_fetch", lambda s, iv, n: [["rest"]])
    assert sc._get_klines_cached("BTCUSDT", "1h", 100) == [["rest"]]


def test_scanner_injects_scan_pool_into_hub(monkeypatch):
    _flag(monkeypatch, True)
    got = []
    monkeypatch.setattr(H, "subscribe_klines", lambda syms: got.append(list(syms)))
    sc._hub_subscribe_klines(["AUSDT", "BUSDT"])
    assert got == [["AUSDT", "BUSDT"]]
    src = (Path(__file__).parent.parent / "scanner.py").read_text()
    assert 'result["scanned"] = len(scan_pool)' in src and '_hub_subscribe_klines(["BTCUSDT"] + sorted(active_sym_set) + list(scan_pool))' in src
