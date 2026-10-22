"""运行：cd fx && python3 -m unittest tests.test_opt_v387_step5 -v
[v3.87 STEP5-8] 价格位移重挂。
[v3.89 #42] 顺序改为：先撤旧单并确认终态，再注册新单；撤单未确认不注册新单；撤后注册失败则旧条目保留（limit_order_id 置空）。"""
import asyncio, sys, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.signal_watcher import SignalWatcher


async def _noop(*a, **k):
    return None


def _kwargs(**over):
    kw = dict(entry_low=99.0, entry_high=101.0, entry_price=100.0,
              expire_ts=time.time() + 600, on_trigger=_noop,
              trigger_price=100.0, trigger_rsi_1h=50.0, breakout_ref=0.0,
              ema20_at_trigger=100.0, atr_at_trigger=1.0, is_reorder=True)
    kw.update(over)
    return kw


CANCELED = {"status": "CANCELED", "executedQty": "0"}


class TestReorderReplace(unittest.TestCase):
    def _watcher(self, events, cancel=CANCELED, query=None):
        w = SignalWatcher()
        w.recs = []
        w._append_reconcile_log = lambda rec: w.recs.append(rec)

        async def fake_cancel(symbol, order_id):
            # 记录撤单瞬间队列里的是哪个条目 / 其状态
            e = w._watched[symbol]
            events.append(("cancel", symbol, order_id, e.signal.get("tag"), e.cancel_state))
            return cancel

        async def fake_query(symbol, order_id):
            return query or {}
        w._cancel_order = fake_cancel
        w._query_order = fake_query
        return w

    async def _setup(self, w):
        await w.add_signal({"symbol": "XUSDT", "mode": "breakout", "direction": "LONG", "tag": "old"},
                           **_kwargs(is_reorder=False))
        old = w._watched["XUSDT"]
        old.limit_order_id = 777
        return old

    def _new_sig(self):
        return {"symbol": "XUSDT", "mode": "breakout", "direction": "LONG", "tag": "new"}

    def test_old_cancelled_before_new_registered(self):
        """[v3.89 #42] 先撤旧单并确认，再注册新单（顺序与 v3.87 相反）。"""
        events = []
        w = self._watcher(events)

        async def run():
            old = await self._setup(w)
            await w._reorder_replace("XUSDT", old, self._new_sig(), **_kwargs())
        asyncio.run(run())
        # 撤旧单发生时，监控队列里仍是旧条目（cancelling），新条目尚未注册
        self.assertEqual(events, [("cancel", "XUSDT", 777, "old", "cancelling")])
        self.assertEqual(w._watched["XUSDT"].signal["tag"], "new")
        self.assertIsNone(w._watched["XUSDT"].limit_order_id)
        self.assertIs(w._watched["XUSDT"].on_trigger, _noop)

    def test_add_signal_failure_after_cancel_keeps_old_entry_without_order_id(self):
        """[v3.89 #42] 方案甲：旧单已撤、新单注册失败 → 旧条目留在队列，limit_order_id 置空，不静默丢信号。"""
        events = []
        w = self._watcher(events)

        async def run():
            old = await self._setup(w)
            bad = _kwargs()
            bad["bogus_param"] = 1          # 模拟 add_signal 因参数错误抛 TypeError
            with self.assertLogs("fx.signal_watcher", level="ERROR"):
                with self.assertRaises(TypeError):
                    await w._reorder_replace("XUSDT", old, self._new_sig(), **bad)
            return old
        old = asyncio.run(run())
        self.assertEqual(len(events), 1)
        self.assertIs(w._watched["XUSDT"], old)
        self.assertEqual(w._watched["XUSDT"].signal["tag"], "old")
        self.assertIsNone(old.limit_order_id)
        self.assertEqual(old.cancel_state, "none")

    def test_unknown_cancel_does_not_register_new_and_registers_reconcile(self):
        events = []
        w = self._watcher(events, cancel={}, query={"status": "NEW", "executedQty": "0"})

        async def run():
            old = await self._setup(w)
            with self.assertRaises(RuntimeError):
                await w._reorder_replace("XUSDT", old, self._new_sig(), **_kwargs())
            return old
        old = asyncio.run(run())
        self.assertIs(w._watched["XUSDT"], old)             # 没有注册新单
        self.assertEqual(old.limit_order_id, 777)
        self.assertEqual(old.cancel_state, "cancel_unknown")
        self.assertIn(("XUSDT", 777), w._orphan_orders)
        self.assertEqual(w._orphan_orders[("XUSDT", 777)]["on_confirmed"], "keep_watching")

    def test_unknown_then_confirmed_keeps_watching_without_order_id(self):
        events = []
        q = {"v": {"status": "NEW"}}
        w = self._watcher(events, cancel={})
        async def fq(symbol, order_id): return q["v"]
        w._query_order = fq

        async def run():
            old = await self._setup(w)
            with self.assertRaises(RuntimeError):
                await w._reorder_replace("XUSDT", old, self._new_sig(), **_kwargs())
            q["v"] = {"status": "CANCELED", "executedQty": "0"}
            await w._reconcile_orphans()
            return old
        old = asyncio.run(run())
        self.assertIs(w._watched["XUSDT"], old)
        self.assertIsNone(old.limit_order_id)
        self.assertEqual(old.cancel_state, "none")
        self.assertEqual(w._orphan_orders, {})

    def test_filled_old_order_does_not_register_new(self):
        events = []
        w = self._watcher(events, cancel={}, query={"status": "FILLED", "executedQty": "1"})

        async def run():
            old = await self._setup(w)
            with self.assertLogs("fx.signal_watcher", level="ERROR"):
                with self.assertRaises(RuntimeError):
                    await w._reorder_replace("XUSDT", old, self._new_sig(), **_kwargs())
            return old
        old = asyncio.run(run())
        self.assertIs(w._watched["XUSDT"], old)
        self.assertEqual(w._orphan_orders, {})

    def test_stale_old_entry_is_not_replaced(self):
        events = []
        w = self._watcher(events)

        async def run():
            old = await self._setup(w)
            await w.add_signal(self._new_sig(), replace=True, **_kwargs())
            with self.assertRaises(RuntimeError):
                await w._reorder_replace("XUSDT", old, {"symbol": "XUSDT", "mode": "breakout",
                                                        "direction": "LONG", "tag": "third"}, **_kwargs())
        asyncio.run(run())
        self.assertEqual(events, [])
        self.assertEqual(w._watched["XUSDT"].signal["tag"], "new")

    def test_duplicate_without_replace_is_skipped(self):
        w = SignalWatcher()

        async def run():
            await w.add_signal({"symbol": "XUSDT", "mode": "breakout", "direction": "LONG", "tag": "old"},
                               **_kwargs(is_reorder=False))
            await w.add_signal({"symbol": "XUSDT", "mode": "breakout", "direction": "LONG", "tag": "new"},
                               **_kwargs())
        asyncio.run(run())
        self.assertEqual(w._watched["XUSDT"].signal["tag"], "old")


if __name__ == "__main__":
    unittest.main()
