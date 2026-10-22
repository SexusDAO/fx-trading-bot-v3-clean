"""运行：cd fx && python3 -m unittest tests.test_liq_cache_capacity -v
[v3.89 #34] liq_cache 容量上限 500 个 key，超出按 ts 淘汰最旧；不影响 #35 的「只接受不更旧数据」规则。"""
import logging, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import liq_cache as L


class TestCapacity(unittest.TestCase):
    def setUp(self):
        L._CACHE.clear(); L._EVICTED_TOTAL = 0
        logging.disable(logging.CRITICAL); self.addCleanup(logging.disable, logging.NOTSET)
        self.addCleanup(L._CACHE.clear)

    def test_limit_is_500(self):
        self.assertEqual(L._CACHE_MAX_ENTRIES, 500)

    def test_evicts_oldest_by_ts(self):
        for i in range(500):
            L._store_if_newer(f"S{i}:24h", {"ts": 1000.0 + i})
        self.assertEqual(len(L._CACHE), 500)
        L._store_if_newer("NEW:24h", {"ts": 5000.0})
        self.assertEqual(len(L._CACHE), 500)
        self.assertNotIn("S0:24h", L._CACHE)             # ts 最小的被淘汰
        self.assertIn("S1:24h", L._CACHE)
        self.assertIn("NEW:24h", L._CACHE)
        self.assertEqual(L._EVICTED_TOTAL, 1)

    def test_overwrite_existing_key_does_not_evict(self):
        for i in range(500):
            L._store_if_newer(f"S{i}:24h", {"ts": 1000.0 + i})
        L._store_if_newer("S10:24h", {"ts": 9000.0})
        self.assertEqual(len(L._CACHE), 500)
        self.assertEqual(L._EVICTED_TOTAL, 0)

    def test_stale_write_still_rejected(self):
        L._store_if_newer("A:24h", {"ts": 100.0})
        self.assertFalse(L._store_if_newer("A:24h", {"ts": 50.0}))
        self.assertEqual(L._CACHE["A:24h"]["ts"], 100.0)

    def test_evicts_multiple_when_far_over(self):
        for i in range(520):
            L._CACHE[f"S{i}:24h"] = {"ts": float(i)}      # 绕过写入口直接塞满
        L._store_if_newer("N:24h", {"ts": 9999.0})
        self.assertEqual(len(L._CACHE), 500)
        self.assertEqual(L._EVICTED_TOTAL, 21)


if __name__ == "__main__":
    unittest.main()
