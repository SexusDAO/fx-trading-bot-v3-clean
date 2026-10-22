"""运行：cd fx && python3 -m unittest tests.test_cancel_state_batch1 -v
[v3.89 #40/#41] 撤单确认状态机、对账、条目身份复核（全部 mock，不访问交易所）。"""
import asyncio, sys, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.signal_watcher import SignalWatcher, WatchEntry
from shared import binance_api as _api

CANCELED = {"status": "CANCELED", "executedQty": "0"}


async def _noop(*a, **k):
    return None


def _kwargs(**over):
    kw = dict(entry_low=99.0, entry_high=101.0, entry_price=100.0,
              expire_ts=time.time() + 600, on_trigger=_noop,
              trigger_price=100.0, trigger_rsi_1h=50.0, breakout_ref=0.0,
              ema20_at_trigger=100.0, atr_at_trigger=1.0, is_reorder=True)
    kw.update(over)
    return kw


def _sig(sym="XUSDT", tag="a"):
    return {"symbol": sym, "mode": "breakout", "direction": "LONG", "tag": tag}


def make(cancel_result=None, query_result=None):
    """cancel_result / query_result: dict 或 callable；记录调用与撤单瞬间的条目状态。"""
    w = SignalWatcher()
    w.log, w.calls, w.at_cancel = [], [], []

    async def fake_cancel(sym, oid):
        e = w._watched.get(sym)
        w.at_cancel.append((e.cancel_state if e else None, e.signal.get("tag") if e else None))
        w.calls.append(("cancel", sym, oid))
        r = cancel_result() if callable(cancel_result) else cancel_result
        return r

    async def fake_query(sym, oid):
        w.calls.append(("query", sym, oid))
        return query_result() if callable(query_result) else (query_result or {})
    w._cancel_order, w._query_order = fake_cancel, fake_query
    w._append_reconcile_log = lambda rec: w.log.append(rec)
    return w


async def _add(w, sym="XUSDT", oid=777, tag="a"):
    await w.add_signal(_sig(sym, tag), **_kwargs(is_reorder=False))
    e = w._watched[sym]
    e.limit_order_id = oid
    return e


class TestWatchEntryDefaults(unittest.TestCase):
    def test_defaults_do_not_break_construction(self):
        a = WatchEntry(signal=_sig(), entry_low=1, entry_high=2, entry_price=1.5,
                       expire_ts=time.time() + 5, on_trigger=_noop)
        b = WatchEntry(signal=_sig(), entry_low=1, entry_high=2, entry_price=1.5,
                       expire_ts=time.time() + 5, on_trigger=_noop)
        self.assertEqual(a.cancel_state, "none")
        self.assertNotEqual(a.entry_id, b.entry_id)


class TestCancelAndRemove(unittest.TestCase):
    def test_confirmed_removes_after_cancel(self):
        w = make(cancel_result=CANCELED)

        async def run():
            e = await _add(w)
            await w._cancel_and_remove("XUSDT", e, "t")
        asyncio.run(run())
        self.assertNotIn("XUSDT", w._watched)
        # 撤单瞬间条目还在队列里，且处于 cancelling
        self.assertEqual(w.at_cancel, [("cancelling", "a")])

    def test_cancel_returns_empty_then_query_canceled_is_confirmed(self):
        w = make(cancel_result={}, query_result={"status": "CANCELED", "executedQty": "0"})

        async def run():
            e = await _add(w)
            await w._cancel_and_remove("XUSDT", e, "t")
        asyncio.run(run())
        self.assertNotIn("XUSDT", w._watched)
        self.assertEqual(w._orphan_orders, {})

    def test_unknown_keeps_entry_and_registers_reconcile(self):
        w = make(cancel_result={}, query_result={"status": "NEW", "executedQty": "0"})

        async def run():
            e = await _add(w)
            await w._cancel_and_remove("XUSDT", e, "t")
            return e
        e = asyncio.run(run())
        self.assertIn("XUSDT", w._watched)
        self.assertEqual(e.cancel_state, "cancel_unknown")
        self.assertIn(("XUSDT", 777), w._orphan_orders)
        self.assertEqual(w.log[0]["event"], "registered")

    def test_query_failure_is_unknown_not_success(self):
        w = make(cancel_result={}, query_result={})

        async def run():
            e = await _add(w)
            await w._cancel_and_remove("XUSDT", e, "t")
            return e
        e = asyncio.run(run())
        self.assertIn("XUSDT", w._watched)
        self.assertEqual(e.cancel_state, "cancel_unknown")

    def test_filled_keeps_entry_no_orphan(self):
        w = make(cancel_result={}, query_result={"status": "FILLED", "executedQty": "1"})

        async def run():
            e = await _add(w)
            await w._cancel_and_remove("XUSDT", e, "t")
            return e
        e = asyncio.run(run())
        self.assertIn("XUSDT", w._watched)
        self.assertEqual(e.cancel_state, "none")
        self.assertEqual(w._orphan_orders, {})
        self.assertEqual(w.log[-1]["event"], "filled_on_cancel")

    def test_partial_fill_on_cancel_treated_as_filled(self):
        w = make(cancel_result={"status": "CANCELED", "executedQty": "0.3"})

        async def run():
            e = await _add(w)
            await w._cancel_and_remove("XUSDT", e, "t")
        asyncio.run(run())
        self.assertIn("XUSDT", w._watched)

    def test_no_order_id_removed_without_exchange_call(self):
        w = make()

        async def run():
            e = await _add(w, oid=None)
            await w._cancel_and_remove("XUSDT", e, "t")
        asyncio.run(run())
        self.assertNotIn("XUSDT", w._watched)
        self.assertEqual(w.calls, [])


class TestTriggerGate(unittest.TestCase):
    def test_no_trigger_while_cancelling_or_unknown(self):
        for st in ("cancelling", "cancel_unknown"):
            w = make()
            fired = []

            async def on_trig(sig):
                fired.append(1)

            async def run():
                await w.add_signal(_sig(), **_kwargs(is_reorder=False, on_trigger=on_trig))
                e = w._watched["XUSDT"]
                e.cancel_state = st
                await w._trigger("XUSDT", 100.0, e)
                return e
            e = asyncio.run(run())
            self.assertEqual(fired, [])
            self.assertFalse(e.triggered)


class TestReconcile(unittest.TestCase):
    def _setup(self, query, cancel):
        w = make(cancel_result=cancel, query_result=query)

        async def run():
            e = await _add(w)
            await w._cancel_and_remove("XUSDT", e, "t")
            return e
        return w, asyncio.run(run())

    def test_later_confirmed_removes_entry(self):
        state = {"q": {"status": "NEW"}}
        w, e = self._setup(lambda: state["q"], {})
        self.assertIn(("XUSDT", 777), w._orphan_orders)
        state["q"] = {"status": "CANCELED", "executedQty": "0"}
        asyncio.run(w._reconcile_orphans())
        self.assertEqual(w._orphan_orders, {})
        self.assertNotIn("XUSDT", w._watched)

    def test_gives_up_after_5_attempts(self):
        w, e = self._setup({"status": "NEW"}, {})
        with self.assertLogs("fx.signal_watcher", level="ERROR") as cm:
            for _ in range(5):
                self.assertIn(("XUSDT", 777), w._orphan_orders)
                asyncio.run(w._reconcile_orphans())
        self.assertEqual(w._orphan_orders, {})
        self.assertTrue(any("放弃自动对账" in m for m in cm.output))
        self.assertEqual(w.log[-1]["event"], "gave_up")
        self.assertEqual(w.log[-1]["attempts"], 5)

    def test_later_filled_logs_error_and_clears(self):
        state = {"q": {"status": "NEW"}}
        w, e = self._setup(lambda: state["q"], {})
        state["q"] = {"status": "FILLED", "executedQty": "1"}
        with self.assertLogs("fx.signal_watcher", level="ERROR"):
            asyncio.run(w._reconcile_orphans())
        self.assertEqual(w._orphan_orders, {})
        self.assertEqual(e.cancel_state, "none")


class TestIdentityChecks(unittest.TestCase):
    """[v3.89 #41] 快照过期：条目被替换/触发/撤单中时，不得误删新条目、不得误撤、不得误触发。"""

    def test_cancel_skips_when_entry_was_replaced(self):
        w = make(cancel_result=CANCELED)

        async def run():
            old = await _add(w, tag="old")
            await w.add_signal(_sig(tag="new"), replace=True, **_kwargs())
            new = w._watched["XUSDT"]
            await w._cancel_and_remove("XUSDT", old, "stale")
            return old, new
        old, new = asyncio.run(run())
        self.assertIs(w._watched["XUSDT"], new)       # 新条目没被误删
        self.assertEqual(w.calls, [])                  # 也没有对旧快照的订单发撤单

    def test_cancel_skips_triggered_entry(self):
        w = make(cancel_result=CANCELED)

        async def run():
            e = await _add(w)
            e.triggered = True
            await w._cancel_and_remove("XUSDT", e, "t")
        asyncio.run(run())
        self.assertIn("XUSDT", w._watched)
        self.assertEqual(w.calls, [])

    def test_cancel_skips_when_already_cancelling(self):
        w = make(cancel_result=CANCELED)

        async def run():
            e = await _add(w)
            e.cancel_state = "cancelling"
            await w._cancel_and_remove("XUSDT", e, "t")
        asyncio.run(run())
        self.assertEqual(w.calls, [])

    def test_stale_reference_cannot_trigger(self):
        w = make()
        fired = []

        async def on_trig(sig):
            fired.append(sig["tag"])

        async def run():
            await w.add_signal(_sig(tag="old"), **_kwargs(is_reorder=False, on_trigger=on_trig))
            old = w._watched["XUSDT"]
            await w.add_signal(_sig(tag="new"), replace=True, **_kwargs(on_trigger=on_trig))
            await w._trigger("XUSDT", 100.0, old)
            return w._watched["XUSDT"]
        new = asyncio.run(run())
        self.assertEqual(fired, [])
        self.assertFalse(new.triggered)

    def test_recheck_pass_snapshot_excludes_non_normal_entries(self):
        w = make()
        seen = []

        async def fake_one(sym, entry, bull, loop):
            seen.append(sym)
        w._recheck_one = fake_one
        orig = _api.get_klines
        _api.get_klines = lambda *a, **k: (_ for _ in ()).throw(Exception("no net"))

        async def run():
            for sym, st in (("AUSDT", "none"), ("BUSDT", "cancelling"), ("CUSDT", "cancel_unknown")):
                await w.add_signal(_sig(sym), **_kwargs(is_reorder=False))
                w._watched[sym].cancel_state = st
            await w._recheck_pass()
        try:
            asyncio.run(run())
        finally:
            _api.get_klines = orig
        self.assertEqual(seen, ["AUSDT"])
