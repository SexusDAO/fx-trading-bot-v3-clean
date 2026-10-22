"""运行：cd fx && python3 -m unittest tests.test_unpersisted_position -v
[v3.89 #52] _persist_position_record：add_position 失败不再被忽略。全部 mock，不依赖交易所。
executor 模块导入依赖太多，这里按源码抽取目标函数后在桩环境里执行。"""
import ast, json, logging, sys, tempfile, textwrap, time, types, unittest
from pathlib import Path

SRC = (Path(__file__).parent.parent / "executor.py").read_text(encoding="utf-8")


def _load(tmpdir, add_position, positions, corrupted, notified):
    node = next(n for n in ast.parse(SRC).body
                if isinstance(n, ast.FunctionDef) and n.name == "_persist_position_record")
    code = ast.get_source_segment(SRC, node)
    st = types.ModuleType("shared.state")
    st._POS_STATE_CORRUPTED = corrupted
    pkg = sys.modules.setdefault("shared", types.ModuleType("shared"))
    pkg.state = st
    sys.modules["shared.state"] = st
    ns = {"add_position": add_position, "get_all_positions": lambda: positions, "json": json, "time": time,
          "Path": Path, "logger": logging.getLogger("t52"), "notify": lambda m: notified.append(m),
          "__file__": str(Path(tmpdir) / "executor.py")}
    exec(code, ns)
    return ns["_persist_position_record"]


REC = {"pos_id": "p1", "symbol": "XUSDT", "open_order_id": 123, "status": "open"}


class TestPersistPosition(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.notified = []
        self.jl = Path(self.tmp) / "logs" / "unpersisted_positions.jsonl"

    def test_success(self):
        f = _load(self.tmp, lambda r: True, [], False, self.notified)
        self.assertTrue(f(dict(REC), "position"))
        self.assertFalse(self.jl.exists())

    def test_corrupted_goes_to_unpersisted_file_with_critical_and_notify(self):
        f = _load(self.tmp, lambda r: False, [], True, self.notified)
        with self.assertLogs("t52", level="CRITICAL") as cm:
            self.assertFalse(f(dict(REC), "position"))
        rows = [json.loads(l) for l in self.jl.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["open_order_id"], 123)
        self.assertEqual(rows[0]["record"]["pos_id"], "p1")
        self.assertIn("损坏", rows[0]["reason"])
        time.sleep(0.2)   # 通知在守护线程里发
        self.assertEqual(len(self.notified), 1)
        self.assertTrue(any("人工核查" in m for m in cm.output))

    def test_duplicate_only_warns_no_file(self):
        existing = [{"symbol": "XUSDT", "status": "pending"}]
        f = _load(self.tmp, lambda r: False, existing, False, self.notified)
        with self.assertLogs("t52", level="WARNING") as cm:
            self.assertFalse(f(dict(REC), "pending"))
        self.assertFalse(self.jl.exists())
        self.assertTrue(all(x.startswith("WARNING") for x in cm.output))
        time.sleep(0.1)
        self.assertEqual(self.notified, [])

    def test_exception_goes_to_unpersisted_file(self):
        def boom(r): raise OSError("disk full")
        f = _load(self.tmp, boom, [], False, self.notified)
        with self.assertLogs("t52", level="CRITICAL"):
            self.assertFalse(f(dict(REC), "position"))
        self.assertIn("disk full", json.loads(self.jl.read_text(encoding="utf-8"))["reason"])

    def test_unknown_false_without_duplicate_is_unpersisted(self):
        f = _load(self.tmp, lambda r: False, [], False, self.notified)
        with self.assertLogs("t52", level="CRITICAL"):
            self.assertFalse(f(dict(REC), "position"))
        self.assertTrue(self.jl.exists())


if __name__ == "__main__":
    unittest.main()
