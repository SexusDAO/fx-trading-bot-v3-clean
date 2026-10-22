"""运行：cd fx && python3 -m unittest tests.test_state_cooldown -v
[v3.89 #97/#98] 冷却写入：短期事件不覆盖长期冷却的原因/期限；方向冷却重复写入取较长期限。"""
import sys, tempfile, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import state as S


class _Base(unittest.TestCase):
    def setUp(self):
        self._cd = S._CD_FILE
        S._CD_FILE = Path(tempfile.mkdtemp()) / "cooldown_state.json"
        S._write_json(S._CD_FILE, {})

    def tearDown(self):
        S._CD_FILE = self._cd

    def entry(self, key):
        return S._read_json(S._CD_FILE, {}).get(key)


class TestAddCooldown(_Base):
    def test_short_event_does_not_overwrite_long_cooldown_reason(self):
        S.add_cooldown("XUSDT", 24 * 3600, "long_reason")
        first = self.entry("XUSDT")
        time.sleep(0.01)
        S.add_cooldown("XUSDT", 600, "short_reason")
        e = self.entry("XUSDT")
        self.assertEqual(e["reason"], "long_reason")                 # 原因仍是决定期限的那个
        self.assertEqual(e["cooldown_until"], first["cooldown_until"])
        self.assertEqual(e["set_at"], first["set_at"])

    def test_longer_event_replaces_reason_and_until(self):
        S.add_cooldown("XUSDT", 600, "short_reason")
        S.add_cooldown("XUSDT", 24 * 3600, "long_reason")
        e = self.entry("XUSDT")
        self.assertEqual(e["reason"], "long_reason")
        self.assertGreater(e["cooldown_until"], int(time.time() * 1000) + 23 * 3600 * 1000)

    def test_first_write(self):
        S.add_cooldown("YUSDT", 60, "r")
        self.assertEqual(self.entry("YUSDT")["reason"], "r")

    def test_legacy_entry_without_fields(self):
        S._write_json(S._CD_FILE, {"XUSDT": {"cooldown_until": 0}})
        S.add_cooldown("XUSDT", 60, "r")
        self.assertEqual(self.entry("XUSDT")["reason"], "r")


class TestOpenDirCooldown(_Base):
    def test_repeat_write_keeps_longer_deadline(self):
        S.set_open_dir_cooldown("XUSDT", "LONG", 24)
        key = S._open_dir_key("XUSDT", "LONG")
        long_until = self.entry(key)["cooldown_until"]
        S.set_open_dir_cooldown("XUSDT", "LONG", 1)                   # 较短的重复/旧事件
        self.assertEqual(self.entry(key)["cooldown_until"], long_until)

    def test_longer_write_extends(self):
        S.set_open_dir_cooldown("XUSDT", "LONG", 1)
        key = S._open_dir_key("XUSDT", "LONG")
        short_until = self.entry(key)["cooldown_until"]
        S.set_open_dir_cooldown("XUSDT", "LONG", 24)
        self.assertGreater(self.entry(key)["cooldown_until"], short_until)

    def test_directions_independent(self):
        S.set_open_dir_cooldown("XUSDT", "LONG", 24)
        S.set_open_dir_cooldown("XUSDT", "SHORT", 1)
        self.assertNotEqual(self.entry(S._open_dir_key("XUSDT", "LONG"))["cooldown_until"],
                            self.entry(S._open_dir_key("XUSDT", "SHORT"))["cooldown_until"])


if __name__ == "__main__":
    unittest.main()
