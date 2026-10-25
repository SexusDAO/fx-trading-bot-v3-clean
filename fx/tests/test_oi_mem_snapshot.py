"""OI 缓存内存快照：TTL 内不读盘、写穿、路径切换作废、并发只读一次盘、紧凑格式可读回。全部临时文件，不联网。
运行：cd fx && python3 -m pytest tests/test_oi_mem_snapshot.py -v"""
import json, sys, tempfile, threading, time, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import state as S


class TestOiMemSnapshot(unittest.TestCase):
    def setUp(self):
        self.f = Path(tempfile.mkdtemp()) / "oi_cache.json"
        p = mock.patch.object(S, "_OI_FILE", self.f); p.start(); self.addCleanup(p.stop)
        S._oi_mem_reset(); self.addCleanup(S._oi_mem_reset)
        now = int(time.time() * 1000)
        self.seed = {"AUSDT": [{"timestamp": now - 900_000, "oi": 100.0}, {"timestamp": now, "oi": 110.0}]}
        self.f.write_text(json.dumps(self.seed))

    def _count_disk_reads(self):
        calls = []
        orig = S._read_json
        def spy(path, default):
            calls.append(path); return orig(path, default)
        return calls, mock.patch.object(S, "_read_json", spy)

    def test_repeat_reads_within_ttl_hit_disk_once(self):
        calls, p = self._count_disk_reads()
        with p:
            for _ in range(20):
                S.get_oi_15min_ago("AUSDT"); S.get_oi_24h_ago("AUSDT"); S._load_oi_cache()
        self.assertEqual(len(calls), 1)

    def test_read_after_ttl_reloads(self):
        S._load_oi_cache()
        calls, p = self._count_disk_reads()
        with p, mock.patch.object(S, "_OI_MEM_TTL", 0.0):
            S._load_oi_cache(); S._load_oi_cache()
        self.assertEqual(len(calls), 2)

    def test_write_through_reader_sees_new_value_without_disk_read(self):
        S._load_oi_cache()
        S.update_oi("AUSDT", 999.0)
        calls, p = self._count_disk_reads()
        with p:
            series = S._load_oi_cache()["AUSDT"]
        self.assertEqual(series[-1]["oi"], 999.0)
        self.assertEqual(calls, [])                       # 写穿后读侧直接命中新快照
        self.assertEqual(json.loads(self.f.read_text())["AUSDT"][-1]["oi"], 999.0)

    def test_writer_does_not_mutate_published_snapshot(self):
        snap = S._load_oi_cache()
        n = len(snap["AUSDT"])
        S.update_oi("AUSDT", 1.0); S.update_all_oi({"AUSDT": 2.0, "BUSDT": 3.0})
        self.assertEqual(len(snap["AUSDT"]), n)           # 旧快照对象未被原地修改
        self.assertNotIn("BUSDT", snap)

    def test_path_switch_invalidates_snapshot(self):
        S._load_oi_cache()
        other = Path(tempfile.mkdtemp()) / "oi_cache.json"
        other.write_text(json.dumps({"ZUSDT": [{"timestamp": 1, "oi": 1.0}]}))
        with mock.patch.object(S, "_OI_FILE", other):
            self.assertIn("ZUSDT", S._load_oi_cache())
        self.assertIn("AUSDT", S._load_oi_cache())

    def test_reset_forces_reload_after_external_file_change(self):
        S._load_oi_cache()
        self.f.write_text(json.dumps({"CUSDT": []}))
        self.assertIn("AUSDT", S._load_oi_cache())        # TTL 内读到旧快照（已知取舍）
        S._oi_mem_reset()
        self.assertIn("CUSDT", S._load_oi_cache())

    def test_concurrent_cold_reads_hit_disk_once(self):
        calls, p = self._count_disk_reads()
        orig = S._read_json
        def slow(path, default):
            time.sleep(0.15); return orig(path, default)
        res = []
        with mock.patch.object(S, "_read_json", lambda path, default: (calls.append(path), slow(path, default))[1]):
            ts = [threading.Thread(target=lambda: res.append(S._load_oi_cache())) for _ in range(8)]
            [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(res), 8)
        self.assertTrue(all(r is res[0] for r in res))

    def test_compact_format_roundtrip(self):
        S.update_oi("AUSDT", 5.0)
        raw = self.f.read_text()
        self.assertNotIn("\n", raw.strip())               # 无缩进
        self.assertNotIn(": ", raw)                       # 紧凑分隔符
        S._oi_mem_reset()
        self.assertEqual(S._load_oi_cache()["AUSDT"][-1]["oi"], 5.0)

    def test_save_failure_does_not_publish_snapshot(self):
        before = S._load_oi_cache()
        with mock.patch.object(S, "_write_json", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                S._save_oi({"XUSDT": []})
        self.assertIs(S._load_oi_cache(), before)

    def test_write_json_default_still_indented(self):
        p = Path(tempfile.mkdtemp()) / "x.json"
        S._write_json(p, {"a": 1})
        self.assertIn("\n  ", p.read_text())


if __name__ == "__main__":
    unittest.main()
