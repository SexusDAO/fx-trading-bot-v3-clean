"""hub 历史拉取使用专用线程池；SCAN_MAX_WORKERS=4（mock）。"""
import asyncio
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from shared import market_data_hub as H


def test_config_scan_workers_is_4():
    assert config.SCAN_MAX_WORKERS == 4


def test_load_pool_definition():
    assert H._LOAD_POOL._max_workers == config.HUB_LOAD_MAX_WORKERS and H._LOAD_POOL._thread_name_prefix == "hub-load"


def test_load_history_runs_in_load_pool_not_default_executor():
    seen = []

    def fetcher(sym, iv, n):
        seen.append(threading.current_thread().name)
        return [[0, "1", "2", "0.5", "1.5", "10", 1, "15", 1, "5", "0", "0"]]

    async def go():
        h = H.MarketDataHub(url="wss://x", market_url="wss://y", kline_fetcher=fetcher)
        h._fetch_sem = asyncio.Semaphore(6)
        h.subscribe_klines(["AUSDT"])
        await h._load_history(("AUSDT", "15m"), h._shards[0].gen)
    asyncio.run(go())
    assert seen and all(n.startswith("hub-load") for n in seen)


def test_load_history_gives_up_after_fetch_timeout(monkeypatch):
    """单次历史拉取超过 KLINE_FETCH_TIMEOUT 就放弃（按空结果走重试），不长期占住加载协程。"""
    release = threading.Event()

    def slow_fetcher(sym, iv, n):
        release.wait(5)
        return [[0, "1", "2", "0.5", "1.5", "10", 1, "15", 1, "5", "0", "0"]]

    monkeypatch.setattr(H, "KLINE_FETCH_TIMEOUT", 0.2)

    async def go():
        h = H.MarketDataHub(url="wss://x", market_url="wss://y", kline_fetcher=slow_fetcher)
        h._fetch_sem = asyncio.Semaphore(1)
        h.subscribe_klines(["AUSDT"])
        t0 = asyncio.get_running_loop().time()
        await h._load_history(("AUSDT", "15m"), h._shards[0].gen)
        el = asyncio.get_running_loop().time() - t0
        st = h._series[("AUSDT", "15m")]
        return el, st.rows, st.retry_at
    try:
        el, rows, retry_at = asyncio.run(go())
    finally:
        release.set()
    assert el < 2.0                     # 约 0.2s 就返回，没有等满 5s
    assert rows is None and retry_at > 0   # 视同空结果，进入重试


def test_fetch_timeout_config_default_8s():
    assert config.HUB_KLINE_FETCH_TIMEOUT == 8.0 and H.KLINE_FETCH_TIMEOUT == 8.0
