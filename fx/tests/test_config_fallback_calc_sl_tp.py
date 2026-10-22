"""运行：cd fx && python3 -m unittest tests.test_config_fallback_calc_sl_tp -v
[v3.89] config 导入失败时 calc_sl_tp 的回退路径必须能完整导入（此前导入时 NameError）。
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


class TestCalcSlTpFallback(unittest.TestCase):
    def setUp(self):
        self.m = _load_with_broken_config("csl_fallback_probe", "shared/calc_sl_tp.py")

    def test_module_imports_and_new_names_match_config_values(self):
        m = self.m
        self.assertEqual((m._CFG_SL_MAX, m._CFG_TP1_MAX, m._CFG_RR_MIN_TP1_HIGH), (0.12, 0.15, 1.5))
        self.assertEqual(m._SL_MAX_PCT, 0.12)
        self.assertEqual(m._TP1_MAX_PCT, 0.15)

    def test_fallback_values_aligned_with_config(self):
        """[v3.89] 回退值对齐 config.py：SL 倍数 1.5/1.5/2.0，RR_MIN_TP1=2.0；其余回退值不变。"""
        m = self.m
        self.assertEqual((m._CFG_LOW, m._CFG_MID, m._CFG_HIGH), (1.5, 1.5, 2.0))
        self.assertEqual(m._CFG_RR_MIN_TP1, 2.0)
        self.assertEqual((m._CFG_SL_LOW, m._CFG_SL_MID, m._CFG_SL_HIGH), (0.12, 0.15, 0.20))
        self.assertEqual((m._CFG_TP1_HIGH, m._CFG_RR_MIN_TP1_HIGH), (0.25, 1.5))
        self.assertEqual(m._SL_MULT, {"low": 1.5, "mid": 1.5, "high": 2.0})
        self.assertTrue(callable(m.calc_sl_tp))

    def test_fallback_equals_real_config_when_available(self):
        """回归：只要 config 能加载，回退值必须与其当前值一致，防止两处再次漂移。"""
        try:
            import config as C
        except Exception:
            self.skipTest("config 无法加载（缺环境变量）")
        m = self.m
        self.assertEqual((m._CFG_LOW, m._CFG_MID, m._CFG_HIGH), (C.SL_MULT_LOW, C.SL_MULT_MID, C.SL_MULT_HIGH))
        self.assertEqual((m._CFG_SL_LOW, m._CFG_SL_MID, m._CFG_SL_HIGH), (C.SL_MAX_PCT_LOW, C.SL_MAX_PCT_MID, C.SL_MAX_PCT_HIGH))
        self.assertEqual((m._CFG_SL_MAX, m._CFG_TP1_MAX, m._CFG_TP1_HIGH), (C.SL_MAX_PCT, C.TP1_MAX_PCT, C.TP1_MAX_PCT_HIGH))
        self.assertEqual((m._CFG_RR_MIN_TP1, m._CFG_RR_MIN_TP1_HIGH), (C.RR_MIN_TP1, C.RR_MIN_TP1_HIGH))


if __name__ == "__main__":
    unittest.main()
