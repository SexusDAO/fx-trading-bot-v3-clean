"""运行：cd fx && python3 -m unittest tests.test_task_exception -v
[v3.89 #106] 事件循环异常处理器：上下文缺 message / exception 时不再 KeyError。main.py 导入有副作用，按源码抽取。"""
import ast, logging, unittest
from pathlib import Path

SRC = (Path(__file__).parent.parent / "main.py").read_text(encoding="utf-8")
_node = next(n for n in ast.parse(SRC).body if isinstance(n, ast.FunctionDef) and n.name == "_handle_task_exception")
_ns = {"logger": logging.getLogger("t106")}
exec(ast.get_source_segment(SRC, _node), _ns)
handler = _ns["_handle_task_exception"]


class TestHandler(unittest.TestCase):
    def _run(self, ctx):
        with self.assertLogs("t106", level="ERROR") as cm:
            handler(None, ctx)
        return "\n".join(cm.output)

    def test_exception_without_message(self):
        out = self._run({"exception": RuntimeError("boom")})        # 原写法这里会 KeyError
        self.assertIn("boom", out)

    def test_message_only(self):
        self.assertIn("hello", self._run({"message": "hello"}))

    def test_neither_field(self):
        out = self._run({"foo": 1})
        self.assertIn("foo", out)

    def test_both_fields_prefers_exception(self):
        out = self._run({"exception": ValueError("e1"), "message": "m1"})
        self.assertIn("e1", out)


if __name__ == "__main__":
    unittest.main()
