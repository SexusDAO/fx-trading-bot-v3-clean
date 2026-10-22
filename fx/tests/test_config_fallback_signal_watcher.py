"""运行：cd fx && python3 -m unittest tests.test_config_fallback_signal_watcher -v
[v3.89] config 导入失败时 signal_watcher 的回退路径必须能完整导入并工作（此前 add_signal 抛 NameError）。
用一个空的 config 模块模拟「config 缺名字 / 加载异常」。不改任何取值。"""
import asyncio, importlib.util, sys, time, types, unittest
from pathlib import Path

_fx = Path(__file__).parent.parent
sys.path.insert(0, str(_fx))


def _load_with_broken_config(name, rel):
    saved = sys.modules.get("config")
    sys.modules["config"] = types.ModuleType("config")          # 空模块：from config import X → ImportError
    try:
        spec = importlib.util.spec_from_file_location(name, _fx / rel)
        m = importlib.util.module_from_spec(spec)
        sys.modules[name] = m                                    # dataclass 需要模块已登记
        spec.loader.exec_module(m)
        return m
    finally:
        if saved is not None:
            sys.modules["config"] = saved
        else:
            sys.modules.pop("config", None)


async def _noop(*a, **k):
    return None


class TestSignalWatcherFallback(unittest.TestCase):
    def setUp(self):
        self.m = _load_with_broken_config("sw_fallback_probe", "shared/signal_watcher.py")

    def test_defaults_defined_and_equal_inner_fallback(self):
        self.assertEqual((self.m._SURVIVAL_RSI_SC, self.m._SURVIVAL_RSI_LC), (60, 45))
        self.assertEqual(self.m._SCORE_TYPE_MAP, {"reversal": 20, "breakout": 15, "stop_hunt": 15, "pullback": 12,
                                                  "oi_divergence": 12, "ema_trend": 10, "range_bounce": 8, "funding_arb": 5})
        self.assertEqual((self.m._BREAKOUT_EXP_MIN, self.m._PULLBACK_EXP_MIN, self.m._GC_INTERVAL), (30, 240, 60.0))

    def test_add_signal_works_on_fallback_path(self):
        w = self.m.SignalWatcher()

        async def go():
            await w.add_signal({"symbol": "XUSDT", "mode": "breakout", "direction": "LONG"},
                               entry_low=99.0, entry_high=101.0, entry_price=100.0, expire_ts=time.time() + 600,
                               on_trigger=_noop, trigger_price=100.0, trigger_rsi_1h=50.0, breakout_ref=0.0,
                               ema20_at_trigger=100.0, atr_at_trigger=1.0)
            return w._watched["XUSDT"].pending_score
        self.assertIsInstance(asyncio.run(go()), (int, float))        # 不再 NameError: _SCORE_TYPE_MAP


if __name__ == "__main__":
    unittest.main()
