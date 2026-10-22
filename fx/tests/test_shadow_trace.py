"""影子追踪：记录、计数上限、分类与离线判定。"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
from shared import shadow_trace as st  # noqa: E402
from tools import shadow_verify as sv  # noqa: E402

T0 = 1_700_000_000_000


def _k(t_min, hi, lo):
    return [T0 + t_min * 60_000, 0, hi, lo, 0]


class TestRecord(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._old = st.LOG_PATH
        st.LOG_PATH = Path(self.tmp.name) / "rejected_log.jsonl"
        st.new_round()

    def tearDown(self):
        st.LOG_PATH = self._old
        self.tmp.cleanup()

    def _lines(self):
        return [json.loads(x) for x in st.LOG_PATH.read_text(encoding="utf-8").splitlines()]

    def test_write_fields(self):
        self.assertTrue(st.record(symbol="ETHUSDT", signal_type="pullback", direction="long",
                                  first_rejected_at="judge_pullback", entry_candidate=100, atr=2, stage="A"))
        r = self._lines()[0]
        self.assertEqual(r["direction"], "LONG")
        self.assertTrue(r["would_pass_all"])
        self.assertIsNone(r["sl"])

    def test_cap_per_stage(self):
        for _ in range(st.CAP_BY_STAGE["A"] + 5):
            st.record(symbol="X", signal_type="pullback", direction="LONG", first_rejected_at="a", stage="A")
        self.assertEqual(len(self._lines()), st.CAP_BY_STAGE["A"])
        # A 类满了不影响 B 类
        self.assertTrue(st.record(symbol="X", signal_type="pullback", direction="LONG", first_rejected_at="b", stage="B"))
        st.new_round()
        self.assertTrue(st.record(symbol="X", signal_type="pullback", direction="LONG", first_rejected_at="a", stage="A"))

    def test_would_pass_all(self):
        self.assertTrue(st.would_pass_all([]))
        self.assertFalse(st.would_pass_all(["precheck:liquidity_gate"]))
        self.assertIsNone(st.would_pass_all([st.SKIP_LLM]))

    def test_categorize(self):
        self.assertEqual(st.categorize_scanner_reason("利空新闻一票否决 LONG"), "news_veto")
        self.assertEqual(st.categorize_scanner_reason("LLM新闻语义: caution升级为bearish"), "llm_filter")
        self.assertEqual(st.categorize_scanner_reason("多单槽位已满"), "slot_limit")
        self.assertEqual(st.categorize_scanner_reason("相关性过滤: LONG 合计3>=3"), "corr_filter")
        self.assertEqual(st.categorize_precheck_reason("流动性不足: 深度 100U"), "liquidity_gate")

    def test_scanner_rejections_llm_skip(self):
        st.probe_liquidity = lambda sig: []   # 不访问网络
        n = st.record_scanner_rejections(
            {"rejected": [{"symbol": "A", "reason": "LLM新闻语义: 一票否决"}, {"symbol": "ZZZ", "reason": "x"}]},
            [{"symbol": "A", "mode": "breakout", "direction": "LONG", "pre_entry_price": 10}])
        self.assertEqual(n, 1)                 # 未通过判定的 ZZZ 不记录
        r = self._lines()[0]
        self.assertEqual(r["first_rejected_at"], "llm_filter")
        self.assertEqual(r["would_also_fail"], [st.SKIP_LLM])
        self.assertIsNone(r["would_pass_all"])

    def test_disabled(self):
        import config
        old = getattr(config, "SHADOW_TRACE_ENABLED", True)
        config.SHADOW_TRACE_ENABLED = False
        try:
            self.assertFalse(st.record(symbol="X", signal_type="p", direction="LONG", first_rejected_at="a"))
        finally:
            config.SHADOW_TRACE_ENABLED = old


class TestV2(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._old = st.LOG_PATH
        st.LOG_PATH = Path(self.tmp.name) / "r.jsonl"
        self._probe = st.probe_liquidity
        st.new_round()

    def tearDown(self):
        st.LOG_PATH = self._old
        st.probe_liquidity = self._probe
        self.tmp.cleanup()

    def _last(self):
        return json.loads(st.LOG_PATH.read_text(encoding="utf-8").splitlines()[-1])

    def test_judge_unknown_and_mark(self):
        st.record_judge_rejection(symbol="A", route="pullback", direction="LONG", reason="x",
                                  ind={"price": 10, "atr_1h": 1})
        r = self._last()
        self.assertIsNone(r["would_pass_all"])
        self.assertEqual(r["price_kind"], "mark")
        self.assertEqual(r["would_also_fail"], [st.SKIP_UNPROBED])

    def test_precheck_probes_liquidity(self):
        st.probe_liquidity = lambda sig: ["precheck:liquidity_gate"]
        st.record_precheck_rejection({"symbol": "A", "mode": "pullback", "direction": "LONG"}, "相关性过滤")
        r = self._last()
        self.assertEqual(r["first_rejected_at"], "precheck:correlation")
        self.assertFalse(r["would_pass_all"])

    def test_precheck_liquidity_not_reprobed(self):
        called = []
        st.probe_liquidity = lambda sig: called.append(1) or []
        st.record_precheck_rejection({"symbol": "A", "mode": "pullback", "direction": "LONG"}, "流动性不足")
        self.assertEqual(called, [])
        self.assertTrue(self._last()["would_pass_all"])

    def test_scanner_ambiguous(self):
        st.probe_liquidity = lambda sig: []
        st.record_scanner_rejections({"rejected": [{"symbol": "A", "reason": "多单槽位已满"}]},
                                     [{"symbol": "A", "mode": "pullback"}, {"symbol": "A", "mode": "breakout"}])
        self.assertTrue(self._last()["match_ambiguous"])


class TestVerify(unittest.TestCase):
    def rec(self, **kw):
        base = {"ts": T0, "signal_type": "breakout", "direction": "LONG", "entry_candidate": 100.0,
                "sl": 95.0, "tp1": 110.0, "atr": 3.0}
        base.update(kw)
        return base

    def test_tp_first(self):
        r = sv.label_record(self.rec(), [_k(0, 101, 99), _k(5, 111, 100)])
        self.assertEqual(r["label"], "先止盈")

    def test_sl_first(self):
        r = sv.label_record(self.rec(), [_k(0, 101, 99), _k(5, 100, 94)])
        self.assertEqual(r["label"], "先止损")

    def test_unfilled(self):
        r = sv.label_record(self.rec(), [_k(0, 105, 101), _k(35, 99, 97)])   # 30 分钟内没到 100
        self.assertEqual(r["label"], "未成交")

    def test_same_bar(self):
        r = sv.label_record(self.rec(), [_k(0, 112, 94)])
        self.assertEqual(r["label"], "无法判定")

    def test_timeout(self):
        bars = [_k(0, 101, 99)] + [_k(m, 102, 98) for m in range(5, 8 * 60 + 10, 5)]
        self.assertEqual(sv.label_record(self.rec(), bars)["label"], "超时")

    def test_window_not_over(self):
        self.assertEqual(sv.label_record(self.rec(), [_k(0, 101, 99), _k(5, 102, 98)])["why"], "结果窗口尚未结束")

    def test_fallback_atr(self):
        r = sv.label_record(self.rec(sl=None, tp1=None), [_k(0, 101, 99), _k(5, 103.5, 100)])
        self.assertEqual(r["label"], "先止盈")
        self.assertTrue(r["fallback"])

    def test_short(self):
        r = sv.label_record(self.rec(direction="SHORT", sl=105.0, tp1=90.0), [_k(0, 101, 99), _k(5, 100, 89)])
        self.assertEqual(r["label"], "先止盈")

    def test_stop_hunt_fill_90(self):
        r = sv.label_record(self.rec(signal_type="stop_hunt", sl=99.0, tp1=101.0),
                            [_k(0, 105, 102), _k(80, 100.5, 99.5), _k(85, 101.5, 100)])
        self.assertEqual(r["label"], "先止盈")       # 第 80 分钟成交仍在 90 分钟成交窗口内


class TestPremerge(unittest.TestCase):
    """[v3.89 PREMERGE] 关闭零请求、名额前置、三态和轮转。"""
    def setUp(self):
        from unittest.mock import patch
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "rejected_log.jsonl"
        self.patches = [patch.object(st, "LOG_PATH", self.path),
                        patch.object(st, "enabled", return_value=True),
                        patch.object(st, "probe_liquidity", return_value=[])]
        self.mocks = [p.start() for p in self.patches]
        st.new_round()
        self.sig = {"symbol":"ETHUSDT", "mode":"pullback", "direction":"LONG"}

    def tearDown(self):
        for p in reversed(self.patches): p.stop()
        self.tmp.cleanup()

    def test_switch_off_no_probe(self):
        self.mocks[1].return_value = False
        self.assertFalse(st.record_precheck_rejection(self.sig, "相关性过滤"))
        self.assertEqual(st.record_scanner_rejections({"rejected":[{"symbol":"ETHUSDT","reason":"低分"}]}, [self.sig]), 0)
        self.mocks[2].assert_not_called()

    def test_cap_before_probe(self):
        for stage in ("B", "C"): st._round[stage] = st.CAP_BY_STAGE[stage]
        self.assertFalse(st.record_precheck_rejection(self.sig, "相关性过滤"))
        self.assertEqual(st.record_scanner_rejections({"rejected":[{"symbol":"ETHUSDT","reason":"低分"}]}, [self.sig]), 0)
        self.mocks[2].assert_not_called()

    def test_would_pass_all_three_states(self):
        for xs, expected in [([], True), ([st.SKIP_UNPROBED], None), (["gate"], False), (["gate",st.SKIP_CAP], None), (["skip_llm"], None)]:
            self.assertIs(st.would_pass_all(xs), expected)
            st.record(symbol="X",signal_type="pullback",direction="LONG",first_rejected_at="x",would_also_fail=xs,stage="A")
            row=json.loads(self.path.read_text().splitlines()[-1])
            self.assertIs(row["would_pass_all"],expected)

    def test_rotation_keeps_current(self):
        self.path.write_bytes(b"x"*(5*1024*1024))
        for n in (1,2,3): Path(str(self.path)+f".{n}").write_text(str(n))
        self.assertTrue(st.record_precheck_rejection(self.sig,"相关性过滤"))
        self.assertEqual(json.loads(self.path.read_text())["symbol"],"ETHUSDT")
        self.assertEqual(Path(str(self.path)+".3").read_text(),"2")
        self.assertFalse(Path(str(self.path)+".4").exists())

    def test_probe_once_no_double_reservation(self):
        self.assertTrue(st.record_precheck_rejection(self.sig,"相关性过滤"))
        self.mocks[2].assert_called_once()
        self.assertEqual(st._round["C"],1)


if __name__ == "__main__":
    unittest.main()
