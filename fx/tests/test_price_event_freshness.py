"""运行：cd fx && python3 -m unittest tests.test_price_event_freshness -v
[v3.89 #101] on_price_update 事件新鲜度：过期 / 超前 / 乱序帧不触发；缺失 ts 按本地时间处理；秒级 ts 兼容。全部 mock。"""
import asyncio, logging, sys, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.signal_watcher import SignalWatcher


async def _noop(*a, **k):
    return None


def _kwargs(**over):
    kw = dict(entry_low=99.0, entry_high=101.0, entry_price=100.0, expire_ts=time.time() + 600, on_trigger=_noop,
              trigger_price=100.0, trigger_rsi_1h=50.0, breakout_ref=0.0, ema20_at_trigger=100.0, atr_at_trigger=1.0)
    kw.update(over)
    return kw


def _ms(offset_s=0.0):
    return int((time.time() + offset_s) * 1000)


class TestFreshness(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL); self.addCleanup(logging.disable, logging.NOTSET)

    def _w(self):
        w = SignalWatcher(); w.triggered = []

        async def fake_trigger(symbol, price, entry):
            w.triggered.append((symbol, price))
        w._trigger = fake_trigger
        return w

    def _run(self, w, frames):
        async def go():
            await w.add_signal({"symbol": "XUSDT", "mode": "breakout", "direction": "LONG"}, **_kwargs())
            for price, ts in frames:
                await w.on_price_update("XUSDT", price, ts)
        asyncio.run(go())

    def test_fresh_event_triggers(self):
        w = self._w(); self._run(w, [(100.0, _ms(-1))])
        self.assertEqual(len(w.triggered), 1)

    def test_stale_event_dropped(self):
        w = self._w(); self._run(w, [(100.0, _ms(-30))])
        self.assertEqual(w.triggered, []); self.assertEqual(w._evt_drop.get("stale"), 1)

    def test_boundary_age(self):
        w = self._w()
        self.assertTrue(w._event_acceptable("A", _ms(-9)))
        self.assertFalse(w._event_acceptable("B", _ms(-11)))

    def test_future_tolerance(self):
        w = self._w()
        self.assertTrue(w._event_acceptable("A", _ms(+1)))        # 容差内（_EVT_FUTURE_TOL_MS=15s，Binance 服务器时间可比本机快约 13s）
        self.assertFalse(w._event_acceptable("B", _ms(+20)))
        self.assertEqual(w._evt_drop.get("future"), 1)

    def test_out_of_order_dropped_but_equal_ts_ok(self):
        w = self._w()
        t = _ms(-2)
        self.assertTrue(w._event_acceptable("A", t))
        self.assertFalse(w._event_acceptable("A", t - 500))
        self.assertTrue(w._event_acceptable("A", t))               # 同一时间戳不算乱序
        self.assertTrue(w._event_acceptable("B", t - 500))         # 按标的独立
        self.assertEqual(w._evt_drop.get("out_of_order"), 1)

    def test_old_frame_after_reconnect_does_not_trigger(self):
        w = self._w()
        self._run(w, [(105.0, _ms(-1)), (100.0, _ms(-3))])         # 先新价（区间外），后到的旧帧（区间内）
        self.assertEqual(w.triggered, [])

    def test_missing_or_invalid_ts_uses_local_time(self):
        w = self._w()
        for ts in (None, 0, -5, "x", float("nan")):
            self.assertTrue(w._event_acceptable("A", ts), ts)
        self.assertEqual(w._last_event_ts, {})

    def test_seconds_ts_normalized_to_ms(self):
        w = self._w()
        self.assertEqual(SignalWatcher._normalize_event_ts(1_700_000_000), 1_700_000_000_000)
        self.assertEqual(SignalWatcher._normalize_event_ts(1_700_000_000_123), 1_700_000_000_123)
        self.assertTrue(w._event_acceptable("A", int(time.time())))   # 秒级当前时间不被误判为极旧

    def test_dropped_btc_frame_does_not_update_snapshot(self):
        w = self._w()
        async def go():
            await w.add_signal({"symbol": "XUSDT", "mode": "breakout", "direction": "LONG"}, **_kwargs())
            await w.on_price_update("BTCUSDT", 60000.0, _ms(-60))
        asyncio.run(go())
        self.assertEqual(w._btc_price_prev, 0.0)                   # 过期帧未初始化/更新 BTC 基准


if __name__ == "__main__":
    unittest.main()
