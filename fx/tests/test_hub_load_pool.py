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
    assert H._LOAD_POOL._max_workers == 2 and H._LOAD_POOL._thread_name_prefix == "hub-load"


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
