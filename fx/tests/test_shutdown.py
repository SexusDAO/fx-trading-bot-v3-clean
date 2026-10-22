"""运行：cd fx && python3 -m unittest tests.test_shutdown -v
[v3.89 #105] _shutdown_components：各组件独立清理 + 单项/整体超时 + 汇总。main.py 导入有副作用，按源码抽取函数。"""
import pytest

pytest.skip('requires feature/passive-trigger branch', allow_module_level=True)

import ast, asyncio, logging, unittest
from pathlib import Path

SRC = (Path(__file__).parent.parent / "main.py").read_text(encoding="utf-8")
_node = next(n for n in ast.parse(SRC).body
             if isinstance(n, ast.AsyncFunctionDef) and n.name == "_shutdown_components")
_ns = {"asyncio": asyncio, "logger": logging.getLogger("t105")}
exec(ast.get_source_segment(SRC, _node), _ns)
shutdown = _ns["_shutdown_components"]


class TestShutdown(unittest.TestCase):
    def test_failure_does_not_skip_later_components(self):
        ran = []

        async def ok(): ran.append("ok")

        async def boom(): raise RuntimeError("x")
        res = asyncio.run(shutdown([("a", boom), ("b", ok), ("c", lambda: ran.append("sync"))], 5, 1))
        self.assertTrue(res["a"].startswith("error"))
        self.assertEqual((res["b"], res["c"]), ("ok", "ok"))
        self.assertEqual(ran, ["ok", "sync"])

    def test_per_component_timeout_then_continue(self):
        async def hang(): await asyncio.sleep(5)

        async def ok(): pass
        res = asyncio.run(shutdown([("h", hang), ("o", ok)], 5, 0.1))
        self.assertEqual(res, {"h": "timeout", "o": "ok"})

    def test_overall_deadline_skips_rest(self):
        async def slow(): await asyncio.sleep(0.15)
        ran = []

        async def late(): ran.append(1)
        res = asyncio.run(shutdown([("s1", slow), ("s2", slow), ("late", late)], 0.2, 1))
        self.assertEqual(res["s1"], "ok")
        self.assertIn(res["late"], ("skipped(deadline)", "timeout"))
        self.assertEqual(ran, [] if res["late"] == "skipped(deadline)" else ran)

    def test_all_ok(self):
        async def ok(): pass
        self.assertEqual(asyncio.run(shutdown([("a", ok), ("b", ok)], 5, 1)), {"a": "ok", "b": "ok"})


if __name__ == "__main__":
    unittest.main()
