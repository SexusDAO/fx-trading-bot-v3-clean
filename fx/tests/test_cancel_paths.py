"""运行：cd fx && python3 -m unittest tests.test_cancel_paths -v
[v3.89 #49] 撤单路径测试（全部 mock，不访问交易所）：撤单异常/超时 → cancel_unknown + 登记对账；
并发撤单/重挂只有一个执行；过期撤单未知时条目保留；GC 跳过撤单中/触发未知条目；cancel_all 按结果计数。"""
import asyncio, sys, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.signal_watcher import SignalWatcher

CANCELED = {"status": "CANCELED", "executedQty": "0"}
FILLED = {"status": "FILLED", "executedQty": "1"}


async def _noop(*a, **k):
    return None


def _kwargs(**over):
    kw = dict(entry_low=99.0, entry_high=101.0, entry_price=100.0,
              expire_ts=time.time() + 600, on_trigger=_noop,
              trigger_price=100.0, trigger_rsi_1h=50.0, breakout_ref=0.0,
              ema20_at_trigger=100.0, atr_at_trigger=1.0)
    kw.update(over)
    return kw


def _sig(sym, tag="t"):
    return {"symbol": sym, "mode": "breakout", "direction": "LONG", "tag": tag}


class Base(unittest.TestCase):
    def _watcher(self, cancel=CANCELED, query=None, cancel_exc=None, delay=0.0):
        w = SignalWatcher()
        w.recs = []
        w.cancel_calls = []
        w._append_reconcile_log = lambda rec: w.recs.append(rec)

        async def fake_cancel(symbol, order_id):
            w.cancel_calls.append((symbol, order_id))
            if delay:
                await asyncio.sleep(delay)
            if cancel_exc:
                raise cancel_exc
            return cancel(symbol) if callable(cancel) else cancel

        async def fake_query(symbol, order_id):
            return query if query is not None else {}
        w._cancel_order = fake_cancel
        w._query_order = fake_query
        return w

    async def _add(self, w, sym, oid=100, **over):
        await w.add_signal(_sig(sym), **_kwargs(**over))
        e = w._watched[sym]
        e.limit_order_id = oid
        return e


class TestCancelUnknown(Base):
    def _check_unknown(self, w, e, oid):
        self.assertEqual(e.cancel_state, "cancel_unknown")
        self.assertIn(e.symbol if hasattr(e, "symbol") else "XUSDT", [k[0] for k in w._orphan_orders])
        self.assertIn(("XUSDT", oid), w._orphan_orders)
        self.assertIs(w._watched.get("XUSDT"), e)           # 条目保留，不丢失交易所订单的跟踪

    def test_cancel_exception_is_unknown_and_registers_orphan(self):
        w = self._watcher(cancel_exc=RuntimeError("net"), query={})

        async def run():
            e = await self._add(w, "XUSDT", 11)
            await w._cancel_and_remove("XUSDT", e, "t")
            return e
        e = asyncio.run(run())
        self._check_unknown(w, e, 11)

    def test_cancel_timeout_is_unknown(self):
        w = self._watcher(cancel_exc=asyncio.TimeoutError(), query={})

        async def run():
            e = await self._add(w, "XUSDT", 12)
            await w._cancel_and_remove("XUSDT", e, "t")
            return e
        self._check_unknown(w, asyncio.run(run()), 12)

    def test_exception_then_query_confirms_removes(self):
        """撤单抛异常但随后查询确认已撤 → 视为 confirmed，条目移除，不登记对账。"""
        w = self._watcher(cancel_exc=RuntimeError("net"), query=CANCELED)

        async def run():
            e = await self._add(w, "XUSDT", 13)
            await w._cancel_and_remove("XUSDT", e, "t")
        asyncio.run(run())
        self.assertNotIn("XUSDT", w._watched)
        self.assertEqual(w._orphan_orders, {})

    def test_filled_keeps_entry_without_orphan(self):
        w = self._watcher(cancel=FILLED)

        async def run():
            e = await self._add(w, "XUSDT", 14)
            await w._cancel_and_remove("XUSDT", e, "t")
            return e
        e = asyncio.run(run())
        self.assertIn("XUSDT", w._watched)
        self.assertEqual(e.cancel_state, "none")
        self.assertEqual(w._orphan_orders, {})
        self.assertTrue(any(r.get("event") == "filled_on_cancel" for r in w.recs))

    def test_register_orphan_is_idempotent(self):
        w = self._watcher()

        async def run():
            e = await self._add(w, "XUSDT", 15)
            w._register_orphan("XUSDT", 15, "a", e, on_confirmed="remove")
            w._register_orphan("XUSDT", 15, "b", e, on_confirmed="remove")
        asyncio.run(run())
        self.assertEqual(len(w._orphan_orders), 1)
        self.assertEqual(len([r for r in w.recs if r.get("event") == "registered"]), 1)


class TestConcurrency(Base):
    def test_concurrent_cancel_and_remove_cancels_once(self):
        w = self._watcher(delay=0.05)

        async def run():
            e = await self._add(w, "XUSDT", 21)
            await asyncio.gather(w._cancel_and_remove("XUSDT", e, "a"), w._cancel_and_remove("XUSDT", e, "b"))
        asyncio.run(run())
        self.assertEqual(w.cancel_calls, [("XUSDT", 21)])
        self.assertNotIn("XUSDT", w._watched)

    def test_concurrent_reorder_replace_only_one_wins(self):
        w = self._watcher(delay=0.05)

        async def run():
            old = await self._add(w, "XUSDT", 22)
            res = await asyncio.gather(
                w._reorder_replace("XUSDT", old, _sig("XUSDT", "n1"), **_kwargs(is_reorder=True)),
                w._reorder_replace("XUSDT", old, _sig("XUSDT", "n2"), **_kwargs(is_reorder=True)),
                return_exceptions=True)
            return res
        asyncio.run(run())
        self.assertEqual(w.cancel_calls, [("XUSDT", 22)])          # 旧单只撤一次
        self.assertEqual(w._watched["XUSDT"].signal.get("tag") in ("n1", "n2"), True)

    def test_reorder_unknown_registers_orphan_keep_watching(self):
        w = self._watcher(cancel={}, query={})

        async def run():
            old = await self._add(w, "XUSDT", 23)
            try:
                await w._reorder_replace("XUSDT", old, _sig("XUSDT", "new"), **_kwargs(is_reorder=True))
            except Exception:
                pass
            return old
        old = asyncio.run(run())
        self.assertEqual(old.cancel_state, "cancel_unknown")
        self.assertEqual(w._orphan_orders[("XUSDT", 23)]["on_confirmed"], "keep_watching")
        self.assertEqual(w._watched["XUSDT"].signal.get("tag"), "t")   # 新单未注册


class TestExpiredAndGc(Base):
    def test_handle_expired_unknown_keeps_entry(self):
        w = self._watcher(cancel={}, query={})

        async def run():
            e = await self._add(w, "XUSDT", 31, expire_ts=time.time() - 1)
            await w._handle_expired("XUSDT", e)
            return e
        e = asyncio.run(run())
        self.assertIn("XUSDT", w._watched)
        self.assertEqual(e.cancel_state, "cancel_unknown")
        self.assertIn(("XUSDT", 31), w._orphan_orders)

    def test_gc_skips_cancel_in_progress_and_trigger_unknown(self):
        w = self._watcher()

        async def run():
            a = await self._add(w, "AUSDT", 41, expire_ts=time.time() - 1)
            b = await self._add(w, "BUSDT", 42, expire_ts=time.time() - 1)
            a.cancel_state = "cancel_unknown"
            b.trigger_state = "unknown"
            await w._gc_pass()
        asyncio.run(run())
        self.assertEqual(w.cancel_calls, [])
        self.assertIn("AUSDT", w._watched)
        self.assertIn("BUSDT", w._watched)

    def test_gc_expired_normal_entry_is_cancelled_and_removed(self):
        w = self._watcher()

        async def run():
            await self._add(w, "CUSDT", 43, expire_ts=time.time() - 1)
            await w._gc_pass()
        asyncio.run(run())
        self.assertEqual(w.cancel_calls, [("CUSDT", 43)])
        self.assertNotIn("CUSDT", w._watched)


class TestCancelAll(Base):
    def test_counts_only_confirmed(self):
        outcomes = {"AUSDT": CANCELED, "BUSDT": {}, "CUSDT": FILLED}
        w = self._watcher(cancel=lambda s: outcomes[s], query={})

        async def run():
            for i, s in enumerate(outcomes):
                await self._add(w, s, 50 + i)
            return await w.cancel_all("test")
        n = asyncio.run(run())
        self.assertEqual(n, 1)
        self.assertNotIn("AUSDT", w._watched)
        self.assertIn("BUSDT", w._watched)         # unknown 保留
        self.assertIn("CUSDT", w._watched)         # filled 保留
        self.assertEqual(len(w._orphan_orders), 1)

    def test_one_failure_does_not_block_others(self):
        w = self._watcher()
        orig = w._cancel_and_remove

        async def flaky(sym, entry, reason):
            if sym == "AUSDT":
                raise RuntimeError("boom")
            return await orig(sym, entry, reason)
        w._cancel_and_remove = flaky

        async def run():
            await self._add(w, "AUSDT", 61)
            await self._add(w, "BUSDT", 62)
            return await w.cancel_all("test")
        self.assertEqual(asyncio.run(run()), 1)
        self.assertNotIn("BUSDT", w._watched)


if __name__ == "__main__":
    unittest.main()
