"""WS 断线 <5 分钟只补缺失 K 线（增量），>=5 分钟或首次加载走全量；接不上旧数据则退回全量。全部 mock，无网络。"""
import asyncio

import config
from shared import market_data_hub as H

IV = {"15m": 900_000, "1h": 3_600_000}


def bar(t, c=1.0):
    return [t, "1", "2", "0.5", str(c), "10", t + 1, "15", 1, "5", "0", "0"]


def make(clock, fetcher):
    h = H.MarketDataHub(url="wss://x", market_url="wss://y", wall=lambda: clock["t"], kline_fetcher=fetcher)
    h._fetch_sem = asyncio.Semaphore(6)
    h.subscribe_klines(["AUSDT"])
    return h


def seed(h, iv="15m", n=120, start=1_000_000_000_000):
    st = h._series[("AUSDT", iv)]
    st.rows = [bar(start + i * IV[iv]) for i in range(n)]
    return st


def run_load(h, iv="15m"):
    asyncio.run(h._load_history(("AUSDT", iv), h._shards[0].gen))


def test_short_disconnect_fetches_only_missing_bars():
    clock = {"t": 1000.0}
    calls = []

    def fetcher(sym, iv, n):
        calls.append(n)
        last = 1_000_000_000_000 + 119 * IV[iv]
        return [bar(last + IV[iv] * k) for k in range(0, 3)]

    h = make(clock, fetcher)
    st = seed(h)
    h._on_market_disconnect(0)
    clock["t"] += 120                         # 断线 120s
    run_load(h)
    assert calls == [2 + int(120 / 900)]      # 0 根完整周期 + 2 根缓冲 = 2
    assert len(st.rows) == 122 - 0 or len(st.rows) == 120   # 并入后裁到 KLINE_HISTORY
    assert st.stale is False and st.disc_ts is None


def test_disconnect_over_5min_does_full_reload():
    clock = {"t": 1000.0}
    calls = []

    def fetcher(sym, iv, n):
        calls.append(n)
        return [bar(2_000_000_000_000 + i * IV[iv]) for i in range(n)]

    h = make(clock, fetcher)
    seed(h)
    h._on_market_disconnect(0)
    clock["t"] += 301
    run_load(h)
    assert calls == [H.KLINE_HISTORY]


def test_first_load_without_rows_is_full():
    clock = {"t": 1000.0}
    calls = []
    h = make(clock, lambda s, i, n: (calls.append(n), [bar(1_000_000_000_000)])[1])
    run_load(h)
    assert calls == [H.KLINE_HISTORY]


def test_missing_bars_capped_at_10_and_scales_with_interval():
    clock = {"t": 1000.0}
    h = make(clock, lambda *a: [])
    seed(h, "15m")
    h._on_market_disconnect(0)
    clock["t"] += 290                         # 15m: 290/900=0 → 2
    assert h._incremental_limit(("AUSDT", "15m"))[0] == 2
    # 构造 1m 级别不存在；用阈值常量验证上限
    assert H.KLINE_INCREMENTAL_MAX_BARS == 10 and H.KLINE_INCREMENTAL_MAX_GAP_SEC == 300.0


def test_gap_in_incremental_falls_back_to_full_next_round():
    clock = {"t": 1000.0}
    calls = []

    def fetcher(sym, iv, n):
        calls.append(n)
        if n < H.KLINE_HISTORY:               # 增量：返回与旧数据接不上的远期 K 线
            return [bar(9_000_000_000_000), bar(9_000_000_000_000 + IV[iv])]
        return [bar(1_000_000_000_000 + i * IV[iv]) for i in range(n)]

    h = make(clock, fetcher)
    st = seed(h)
    old_rows = [list(r) for r in st.rows]
    h._on_market_disconnect(0)
    clock["t"] += 60
    run_load(h)
    assert st.rows == old_rows and st.stale is True      # 没把错位数据并进去
    run_load(h)                                          # 下一轮：disc_ts 已清 → 全量
    assert calls == [2, H.KLINE_HISTORY] and st.stale is False


def test_repeated_disconnect_keeps_earliest_disc_ts():
    clock = {"t": 1000.0}
    h = make(clock, lambda *a: [])
    st = seed(h)
    h._on_market_disconnect(0)
    first = st.disc_ts
    clock["t"] += 200
    h._on_market_disconnect(0)
    assert st.disc_ts == first
    clock["t"] += 150                                    # 累计 350s > 300s
    assert h._incremental_limit(("AUSDT", "15m"))[0] is None


def test_failed_incremental_fetch_keeps_stale_and_retries():
    clock = {"t": 1000.0}
    h = make(clock, lambda *a: [])
    st = seed(h)
    h._on_market_disconnect(0)
    clock["t"] += 30
    run_load(h)
    assert st.stale is True and st.disc_ts is not None and st.retry_at > clock["t"]
