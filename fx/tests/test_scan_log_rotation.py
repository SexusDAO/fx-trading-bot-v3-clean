"""运行：cd fx && python3 -m unittest tests.test_scan_log_rotation -v
[v3.89 #21] scanner.log 轮转：备份统一命名 .1/.2/.3，连续多轮滚动；轮转/写入失败记日志。scanner 导入太重，按源码抽取函数。"""
import ast, logging, tempfile, unittest
from pathlib import Path
from unittest import mock

SRC = (Path(__file__).parent.parent / "scanner.py").read_text(encoding="utf-8")
_node = next(n for n in ast.parse(SRC).body if isinstance(n, ast.FunctionDef) and n.name == "_write_scan_log")
RESULT = {"class_counts": {}, "ts": "t", "scanned": 1, "signals_raw": 0, "signals_final": 0,
          "placed": 0, "elapsed": 0.1, "rejected": []}


def _load(logfile):
    ns = {"SCANNER_LOG_FILE": logfile, "Path": Path, "logger": logging.getLogger("t21")}
    exec(ast.get_source_segment(SRC, _node), ns)
    return ns["_write_scan_log"]


def _big(p, tag):
    p.write_bytes((tag.encode() * (5 * 1024 * 1024 + 10)))


class TestRotation(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.log = self.d / "scanner.log"
        self.f = _load(self.log)

    def test_four_consecutive_rotations_chain_and_cap(self):
        for tag in "ABCD":
            _big(self.log, tag)
            self.f(RESULT, [])
        names = sorted(p.name for p in self.d.iterdir())
        self.assertEqual(names, ["scanner.log", "scanner.log.1", "scanner.log.2", "scanner.log.3"])
        self.assertFalse((self.d / "scanner.1.log").exists())        # 旧的错误命名不再出现
        first = lambda n: (self.d / n).read_bytes()[:1]
        self.assertEqual((first("scanner.log.1"), first("scanner.log.2"), first("scanner.log.3")), (b"D", b"C", b"B"))
        self.assertIn("[scan]", self.log.read_text(encoding="utf-8"))

    def test_small_file_not_rotated(self):
        self.log.write_text("x")
        self.f(RESULT, [])
        self.assertFalse((self.d / "scanner.log.1").exists())

    def test_rotation_failure_is_logged_and_write_continues(self):
        _big(self.log, "A")
        with mock.patch("os.replace", side_effect=OSError("boom")), \
             self.assertLogs("t21", level="WARNING") as cm:
            self.f(RESULT, [])
        self.assertTrue(any("轮转失败" in m for m in cm.output))

    def test_write_failure_is_logged(self):
        f = _load(self.d / "nodir" / "x" / "scanner.log")
        with mock.patch("builtins.open", side_effect=OSError("disk")), \
             self.assertLogs("t21", level="WARNING") as cm:
            f(RESULT, [])
        self.assertTrue(any("写入失败" in m for m in cm.output))


if __name__ == "__main__":
    unittest.main()
