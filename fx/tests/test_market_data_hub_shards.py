"""hub K 线分片：多条 /market/stream 连接，每条最多 KLINE_STREAM_PER_CONN 个 stream（全 mock，不联网）。"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from shared import market_data_hub as H

T0 = 1_700_000_000_000
HMS = 3_600_000


class FakeWS:
    def __init__(self): self.sent = []
    async def send(self, m): self.sent.append(json.loads(m))
    def streams(self, method="SUBSCRIBE"):
        return [p for m in self.sent if m["method"] == method for p in m["params"]]


def rows_for(iv):
    ms = H._INTERVAL_MS[iv]
    return [[T0 + i * ms, "1", "2", "0.5", "1.5", "10", T0 + (i + 1) * ms - 1] for i in range(100)]


def make_hub(fetcher=None):
    last = rows_for("15m")[-1][6]
    return H.MarketDataHub(url="wss://x", market_url="wss://y", kline_fetcher=fetcher,
                           wall=lambda: (last - 5000) / 1000)


def syms(n, p="S"):
    return [f"{p}{i:03d}USDT" for i in range(n)]


async def setup(h):
    h._sync_lock, h._fetch_sem = asyncio.Lock(), asyncio.Semaphore(6)
    wss = [FakeWS() for _ in h._shards]
    for sh, ws in zip(h._shards, wss):
        sh.ws, sh.connected = ws, True
    return wss


def test_config_and_shard_count():
    assert config.KLINE_HUB_SYMBOLS_MAX == 120 and H.KLINE_STREAM_PER_CONN == 180
    h = make_hub()
    assert h._nshards == 2 and h._syms_per_conn == 60


def test_one_shard_when_cap_is_60(monkeypatch):
    monkeypatch.setattr(config, "KLINE_HUB_SYMBOLS_MAX", 60, raising=False)
    assert make_hub()._nshards == 1


def test_assignment_first_60_on_conn0_next_60_on_conn1_rest_dropped():
    h = make_hub()
    order = ["BTCUSDT", "POSUSDT"] + syms(150)
    h.subscribe_klines(order)
    assert len(h._assign) == 120
    assert all(h._assign[s] == 0 for s in order[:60]) and all(h._assign[s] == 1 for s in order[60:120])
    assert order[120] not in h._assign
    per = [sum(1 for v in h._assign.values() if v == i) * 3 for i in (0, 1)]
    assert per == [180, 180] and max(per) <= H.KLINE_STREAM_PER_CONN


def test_sync_each_connection_subscribes_only_its_own_streams_ticker_only_on_conn0():
    async def go():
        h = make_hub(lambda *a: [])
        wss = await setup(h)
        await h._on_market_connected(wss[0], 0)
        await h._on_market_connected(wss[1], 1)
        order = syms(120)
        h.subscribe_klines(order)
        await h._sync_once()
        await asyncio.gather(*list(h._load_tasks))
        s0, s1 = wss[0].streams(), wss[1].streams()
        assert H.TICKER_STREAM in s0 and H.TICKER_STREAM not in s1
        k0 = {x for x in s0 if "@kline_" in x}; k1 = {x for x in s1 if "@kline_" in x}
        assert len(k0) == 180 and len(k1) == 180 and not (k0 & k1)
        assert all(x.split("@")[0].upper() in order[:60] for x in k0)
        assert all(x.split("@")[0].upper() in order[60:] for x in k1)
        assert all(len(m["params"]) <= H.SUB_BATCH for w in wss for m in w.sent)
    asyncio.run(go())


def test_sticky_assignment_no_resubscribe_for_existing_symbols():
    async def go():
        h = make_hub(lambda *a: [])
        wss = await setup(h)
        order = syms(120)
        h.subscribe_klines(order)
        await h._sync_once()
        before = dict(h._assign)
        n0, n1 = len(wss[0].sent), len(wss[1].sent)
        # 新增一个优先标的放最前，列表被截成 120：末尾的 S119 被挤掉，新标的占它在连接1的位置，其余不动
        h.subscribe_klines(["NEWUSDT"] + order)
        assert all(h._assign[s] == before[s] for s in order[:119])
        assert h._assign["NEWUSDT"] == before["S119USDT"] == 1 and "S119USDT" not in h._assign
        await h._sync_once()
        assert len(wss[0].sent) == n0                                   # 连接0 没有任何增删订阅
        assert set(wss[1].streams("UNSUBSCRIBE")) == {f"s119usdt@kline_{iv}" for iv in H.KLINE_INTERVALS}
    asyncio.run(go())


def test_disconnect_one_shard_only_invalidates_its_series():
    async def go():
        h = make_hub(lambda sym, iv, n: rows_for(iv))
        wss = await setup(h)
        order = syms(120)
        h.subscribe_klines(order)
        await h._sync_once()
        await asyncio.gather(*list(h._load_tasks))
        a, b = order[0], order[100]
        assert h.get_klines(a, "15m", 50) is not None and h.get_klines(b, "15m", 50) is not None
        h._tickers = {"X": ({}, 1.0)}; h._ticker_ready = True
        h._on_market_disconnect(1)
        assert h.get_klines(a, "15m", 50) is not None                    # 连接0 不受影响
        assert h.get_klines(b, "15m", 50) is None
        assert h._ticker_ready and h._shards[0].connected and not h._shards[1].connected
        assert h.stats()["kline_conns_up"] == 1
        h._on_market_disconnect(0)                                       # 连接0 断线才清 ticker
        assert not h._ticker_ready and h.get_klines(a, "15m", 50) is None
    asyncio.run(go())


def test_reconnect_shard1_resubscribes_and_reloads_only_its_symbols():
    async def go():
        calls = []
        def fetcher(sym, iv, n):
            calls.append(sym); return rows_for(iv)
        h = make_hub(fetcher)
        wss = await setup(h)
        order = syms(120)
        h.subscribe_klines(order)
        await h._sync_once()
        await asyncio.gather(*list(h._load_tasks))
        assert len(calls) == 360
        calls.clear()
        h._on_market_disconnect(1)
        new_ws = FakeWS()
        h._shards[1].connected = True
        await h._on_market_connected(new_ws, 1)
        await asyncio.gather(*list(h._load_tasks))
        assert len(new_ws.streams()) == 180 and set(calls) == set(order[60:]) and len(calls) == 180
        assert wss[0].sent and all(m["method"] == "SUBSCRIBE" for m in wss[0].sent)
        assert h.get_klines(order[100], "15m", 50) is not None
    asyncio.run(go())


def test_inflight_history_of_disconnected_shard_discarded_other_shard_kept():
    async def go():
        h = make_hub(lambda *a: [])
        await setup(h)
        order = syms(120)
        h.subscribe_klines(order)
        with h._kl_lock:
            k0, k1 = (order[0], "15m"), (order[100], "15m")
            for k in (k0, k1):
                h._series[k].loading = True
        g0, g1 = h._shards[0].gen, h._shards[1].gen
        h._on_market_disconnect(1)                                        # 仅连接1 的 gen 变化
        assert h._shards[0].gen == g0 and h._shards[1].gen == g1 + 1
        h._fetch = lambda *a: rows_for("15m")
        h._fetch_sem = asyncio.Semaphore(6)
        await h._load_history(k0, g0); await h._load_history(k1, g1)
        assert h._series[k0].rows is not None and h._series[k1].rows is None
    asyncio.run(go())


def test_stats_reports_connections():
    h = make_hub()
    st = h.stats()
    assert st["kline_conns"] == 2 and st["kline_conns_up"] == 0
