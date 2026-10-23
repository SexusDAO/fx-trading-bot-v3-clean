"""运行：cd fx && python3 -m unittest tests.test_fill_avg_price -v
[v3.89 #2] pending 入场单 WS 成交感知：补挂止损必须用累计均价 ap，不得用最后成交价 L；ap 缺失时跳过交给 patrol。"""
import ast, asyncio, logging, textwrap, unittest
from pathlib import Path

SRC = (Path(__file__).parent.parent / "position_manager.py").read_text(encoding="utf-8")
_tree = ast.parse(SRC)
_cls = next(n for n in _tree.body if isinstance(n, ast.ClassDef)
            and any(isinstance(m, ast.AsyncFunctionDef) and m.name == "_order_event_loop" for m in n.body))
_fn = next(m for m in _cls.body if isinstance(m, ast.AsyncFunctionDef) and m.name == "_order_event_loop")
_CODE = textwrap.dedent(ast.get_source_segment(SRC, _fn))


def run_loop(event):
    calls, updates = [], []
    ns = {"asyncio": asyncio, "logger": logging.getLogger("t2"),
          "get_all_positions": lambda: [{"status": "pending", "open_order_id": 9, "symbol": "XUSDT", "pos_id": "p1"}],
          "update_position": lambda pid, u: updates.append((pid, u)),
          "mark_system_order_update": lambda s: None,
          "_notify_async": None}

    async def patch(pos, price, qty, pid, sym):
        calls.append((price, qty))
    ns["_补挂保护单"] = patch

    async def notify(m): pass
    ns["_notify_async"] = notify
    exec(_CODE, ns)

    class WS:
        async def wait_any_order_event(self, ids, timeout=0):
            return event if ids == [9] else None

    class Self:
        _running = True
        _ws_listener = WS()
    s = Self()

    async def stop_sleep(_):
        s._running = False
    async def fake_to_thread(fn, *a, **k):   # 被测代码已改为 asyncio.to_thread 包装同步调用
        return fn(*a, **k)
    ns["asyncio"] = type("A", (), {"sleep": staticmethod(stop_sleep), "to_thread": staticmethod(fake_to_thread),
                                   "CancelledError": asyncio.CancelledError})
    asyncio.run(ns["_order_event_loop"](s))
    return calls, updates


class TestFillAvg(unittest.TestCase):
    def test_uses_ap_not_L(self):
        calls, _ = run_loop({"X": "FILLED", "i": 9, "L": "101.5", "ap": "100.2", "z": "3"})
        self.assertEqual(calls, [(100.2, 3.0)])

    def test_missing_ap_warns_and_skips(self):
        for ap in (None, "0", "0.00000", ""):
            ev = {"X": "FILLED", "i": 9, "L": "101.5", "z": "3"}
            if ap is not None:
                ev["ap"] = ap
            with self.assertLogs("t2", level="WARNING") as cm:
                calls, updates = run_loop(ev)
            self.assertEqual(calls, [])
            self.assertEqual(updates, [])        # 没有置 patching，patrol 仍可接管
            self.assertTrue(any("不回退到最后成交价 L" in m for m in cm.output))

    def test_non_filled_ignored(self):
        calls, _ = run_loop({"X": "PARTIALLY_FILLED", "i": 9, "L": "1", "ap": "1", "z": "1"})
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
