"""运行：cd fx && python3 -m unittest tests.test_oi_retention -v
[v3.89 #99] OI 历史按时间窗口（25H）保留：高频写入不挤掉 24H 前样本，过期记录写入时清理。全部 mock/临时文件。"""
import sys, tempfile, time, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import state as S

H = 3600 * 1000


class TestOiRetention(unittest.TestCase):
    def setUp(self):
        self.f = Path(tempfile.mkdtemp()) / "oi_cache.json"
        p = mock.patch.object(S, "_OI_FILE", self.f); p.start(); self.addCleanup(p.stop)

    def _seed(self, d):
        S._save_oi(d)

    def test_window_constant(self):
        self.assertEqual(S.OI_RETAIN_SEC, 25 * 3600)

    def test_old_records_pruned_on_update_oi(self):
        now = int(time.time() * 1000)
        self._seed({"XUSDT": [{"timestamp": now - 26 * H, "oi": 1.0}, {"timestamp": now - 24 * H, "oi": 2.0},
                              {"timestamp": now - 1 * H, "oi": 3.0}]})
        S.update_oi("XUSDT", 4.0)
        ois = [r["oi"] for r in S._load_oi_cache()["XUSDT"]]
        self.assertEqual(ois, [2.0, 3.0, 4.0])          # 26H 前的被清理，25H 内全部保留

    def test_high_frequency_writes_do_not_evict_24h_sample(self):
        """旧逻辑最多 48 条：每分钟写入一次时 24H 前的样本很快被挤掉。新逻辑按时间保留。"""
        now = int(time.time() * 1000)
        series = [{"timestamp": now - 24 * H, "oi": 100.0}] + \
                 [{"timestamp": now - (23 * H) + i * 60_000, "oi": 101.0 + i} for i in range(200)]
        self._seed({"XUSDT": series})
        for _ in range(60):
            S.update_oi("XUSDT", 999.0)
        s = S._load_oi_cache()["XUSDT"]
        self.assertEqual(s[0]["oi"], 100.0)              # 24H 前样本仍在
        self.assertGreater(len(s), S.OI_CACHE_KEEP)      # 条数已超过旧上限
        self.assertEqual(S.get_oi_24h_ago("XUSDT"), 100.0)

    def test_update_all_prunes_all_symbols_and_drops_stale_symbols(self):
        now = int(time.time() * 1000)
        self._seed({"OLDUSDT": [{"timestamp": now - 30 * H, "oi": 1.0}],
                    "AUSDT": [{"timestamp": now - 27 * H, "oi": 5.0}, {"timestamp": now - 2 * H, "oi": 6.0}]})
        S.update_all_oi({"AUSDT": 7.0, "BUSDT": 8.0, "BADUSDT": None, "ZUSDT": 0})
        d = S._load_oi_cache()
        self.assertNotIn("OLDUSDT", d)                   # 整段过期的标的整体移除
        self.assertEqual([r["oi"] for r in d["AUSDT"]], [6.0, 7.0])
        self.assertEqual([r["oi"] for r in d["BUSDT"]], [8.0])
        self.assertNotIn("BADUSDT", d)
        self.assertNotIn("ZUSDT", d)                     # 非正值仍被过滤（原行为不变）

    def test_corrupt_records_dropped(self):
        now = int(time.time() * 1000)
        out = S._prune_oi_series([{"oi": 1.0}, {"timestamp": "x", "oi": 2.0}, {"timestamp": now, "oi": 3.0}], now)
        self.assertEqual([r["oi"] for r in out], [3.0])

    def test_get_oi_24h_ago_conditions_unchanged(self):
        now = int(time.time() * 1000)
        self._seed({"XUSDT": [{"timestamp": now - 10 * H, "oi": 1.0}, {"timestamp": now - 1 * H, "oi": 2.0}]})
        self.assertIsNone(S.get_oi_24h_ago("XUSDT"))    # 不足 ~20H：仍返回 None
        self.assertIsNone(S.get_oi_24h_ago("NOPE"))


if __name__ == "__main__":
    unittest.main()
