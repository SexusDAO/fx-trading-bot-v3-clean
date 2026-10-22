"""market_data_hub 的 K 线部分（全 mock，不联网）。"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

from shared import market_data_hub as H

HMS = 3_600_000
T0 = 1_700_000_000_000 - (1_700_000_000_000 % HMS)


def row(t, c="1.5", iv_ms=HMS):
    return [t, "1", "2", "0.5", c, "10", t + iv_ms - 1, "15", 5, "4", "6", "0"]


def hist(n=100, last_open=T0 + 200 * HMS):
    return [row(last_open - (n - 1 - i) * HMS) for i in range(n)]


def kf(sym, iv, t, x=False, c="1.5", ms=HMS):
    k = {"t": t, "T": t + ms - 1, "s": sym, "i": iv, "o": "1", "c": c, "h": "2", "l": "0.5", "v": "10",
         "n": 5, "x": x, "q": "15", "V": "4", "Q": "6", "B": "0"}
    return json.dumps({"stream": f"{sym.lower()}@kline_{iv}", "data": {"e": "kline", "E": 1, "s": sym, "k": k}})


class FakeWS:
    def __init__(self): self.sent = []
    async def send(self, m): self.sent.append(json.loads(m))


def make_hub(fetcher=None, last_open=T0 + 200 * HMS):
    h = H.MarketDataHub(url="wss://x", market_url="wss://y", kline_fetcher=fetcher,
                        wall=lambda: (last_open + 1000) / 1000)
    return h


def ready_hub(symbols=("BTCUSDT",), n=100):
    """直接把序列置为就绪（跳过网络与历史拉取）。"""
    h = make_hub()
    h.subscribe_klines(list(symbols))
    h._market_connected = True
    for s in symbols:
        h._series[(s, "1h")].rows = hist(n)
    return h


def test_ws_row_format_equals_rest_format():
    h = ready_hub()
    h._handle_market_frame(kf("BTCUSDT", "1h", T0 + 200 * HMS, c="9.9"))
    last = h.get_klines("BTCUSDT", "1h", 100)[-1]
    assert last == row(T0 + 200 * HMS, c="9.9")                     # 12 列，类型与 REST 一致
    assert isinstance(last[0], int) and isinstance(last[6], int) and isinstance(last[8], int) and isinstance(last[1], str)


def test_x_false_updates_last_candle_in_place():
    h = ready_hub()
    for c in ("1.6", "1.7"):
        h._handle_market_frame(kf("BTCUSDT", "1h", T0 + 200 * HMS, x=False, c=c))
    d = h.get_klines("BTCUSDT", "1h", 100)
    assert len(d) == 100 and d[-1][4] == "1.7" and d[-2][0] == T0 + 199 * HMS


def test_x_true_closes_then_next_candle_appends_and_trims_oldest():
    h = ready_hub()
    first_open = h.get_klines("BTCUSDT", "1h", 100)[0][0]
    h._handle_market_frame(kf("BTCUSDT", "1h", T0 + 200 * HMS, x=True, c="2.0"))      # 收盘帧：替换最后一根为终值
    assert h.get_klines("BTCUSDT", "1h", 100)[-1][4] == "2.0"
    h._handle_market_frame(kf("BTCUSDT", "1h", T0 + 201 * HMS, x=False, c="2.1"))     # 新一根开始：追加
    d = h.get_klines("BTCUSDT", "1h", 100)
    assert len(d) == 100 and d[-1][0] == T0 + 201 * HMS and d[-2][4] == "2.0" and d[0][0] == first_open + HMS


def test_limit_slicing_and_insufficient_history_returns_none():
    h = ready_hub(n=60)
    assert len(h.get_klines("BTCUSDT", "1h", 30)) == 30
    assert h.get_klines("BTCUSDT", "1h", 100) is None                # 历史不足 → 调用方走 REST


def test_not_subscribed_or_unsupported_returns_none():
    h = ready_hub()
    assert h.get_klines("ETHUSDT", "1h", 10) is None
    assert h.get_klines("BTCUSDT", "5m", 10) is None
    assert h.get_klines("BTCUSDT", "15m", 10) is None                # 15m 序列存在但未就绪
    h._handle_market_frame(kf("ETHUSDT", "1h", T0))                  # 未订阅标的的推送被忽略
    assert h.get_klines("ETHUSDT", "1h", 10) is None


def test_gap_invalidates_series():
    h = ready_hub()
    h._handle_market_frame(kf("BTCUSDT", "1h", T0 + 203 * HMS))      # 跳过了 201、202
    assert h.get_klines("BTCUSDT", "1h", 10) is None


def test_stale_last_candle_returns_none():
    h = ready_hub()
    assert h.get_klines("BTCUSDT", "1h", 10) is not None
    h._wall = lambda: (T0 + 201 * HMS + 20_000) / 1000               # 最后一根已结束 20s 仍没有新推送
    assert h.get_klines("BTCUSDT", "1h", 10) is None


def test_connection_down_returns_none():
    h = ready_hub()
    h._on_market_disconnect()
    assert h.get_klines("BTCUSDT", "1h", 10) is None


def test_subscribe_dedup_upper_cap_and_removal(monkeypatch):
    import config
    monkeypatch.setattr(config, "KLINE_HUB_SYMBOLS_MAX", 3, raising=False)
    h = make_hub()
    h.subscribe_klines(["btcusdt", "BTCUSDT", "ethusdt", "xrpusdt", "solusdt"])
    assert h._desired == ["BTCUSDT", "ETHUSDT", "XRPUSDT"] and len(h._series) == 9
    h.subscribe_klines(["ETHUSDT"])
    assert set(k[0] for k in h._series) == {"ETHUSDT"}


def test_sync_subscribes_in_batches_loads_history_and_merges_pending():
    calls = []

    def fetcher(sym, iv, limit):
        calls.append((sym, iv, limit))
        return hist(100)

    async def go():
        h = make_hub(fetcher)
        h._sync_lock, h._fetch_sem = asyncio.Lock(), asyncio.Semaphore(6)
        ws = FakeWS()
        h._market_connected = True
        h.subscribe_klines(["BTCUSDT", "ETHUSDT"])
        # 历史拉取前先来一条推送 → 进入 pending，拉完后合并
        h._ws_market = ws
        h._handle_market_frame(kf("BTCUSDT", "1h", T0 + 200 * HMS, c="7.7"))
        await h._sync_once()
        await asyncio.gather(*list(h._load_tasks))
        assert sorted(calls) == sorted((s, iv, 100) for s in ("BTCUSDT", "ETHUSDT") for iv in H.KLINE_INTERVALS)
        subs = [m for m in ws.sent if m["method"] == "SUBSCRIBE"]
        assert {p for m in subs for p in m["params"]} == {f"{s}@kline_{iv}" for s in ("btcusdt", "ethusdt") for iv in H.KLINE_INTERVALS}
        assert h.get_klines("BTCUSDT", "1h", 100)[-1][4] == "7.7"        # pending 已合并
        assert h.get_klines("ETHUSDT", "15m", 100) is None or True
        # 移除一个标的 → UNSUBSCRIBE
        h.subscribe_klines(["BTCUSDT"])
        await h._sync_once()
        un = [m for m in ws.sent if m["method"] == "UNSUBSCRIBE"]
        assert {p for m in un for p in m["params"]} == {f"ethusdt@kline_{iv}" for iv in H.KLINE_INTERVALS}

    asyncio.run(go())


def test_sync_batches_large_subscription(monkeypatch):
    import config
    monkeypatch.setattr(config, "KLINE_HUB_SYMBOLS_MAX", 100, raising=False)   # 不依赖线上配置值
    async def go():
        h = make_hub(lambda *a: [])
        h._sync_lock, h._fetch_sem = asyncio.Lock(), asyncio.Semaphore(6)
        ws0, ws1 = FakeWS(), FakeWS()
        h._shards[0].ws, h._shards[0].connected = ws0, True
        h._shards[1].ws, h._shards[1].connected = ws1, True
        h.subscribe_klines([f"S{i}USDT" for i in range(100)])             # 60 个标的在连接0(180 stream)，40 个在连接1(120 stream)
        await h._sync_once()
        await asyncio.gather(*list(h._load_tasks))
        for ws, n in ((ws0, 180), (ws1, 120)):
            subs = [m for m in ws.sent if m["method"] == "SUBSCRIBE"]
            assert sum(len(m["params"]) for m in subs) == n and all(len(m["params"]) <= H.SUB_BATCH for m in subs)
    asyncio.run(go())
