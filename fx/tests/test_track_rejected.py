"""运行：cd fx && python3 -m unittest tests.test_track_rejected -v
[v3.89 #44/#45] 拒绝追踪：K 线窗口校验与方向触达率输出。全部 mock，不访问网络。"""
import importlib.util, sys, time, unittest
from pathlib import Path
from unittest import mock

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root))
_spec = importlib.util.spec_from_file_location("track_rejected", _root / "tools" / "track_rejected.py")
TR = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(TR)

K = TR.KLINE_MS
H8 = TR.TRACK_WINDOW_H * 3600 * 1000
START = (int(time.time() * 1000) // K) * K - H8 - 3 * 3600 * 1000     # 窗口已结束


def kl(open_ms, hi, lo):
    return [open_ms, "100", str(hi), str(lo), "100", "1"]


def full(hi=101, lo=99):
    return [kl(START + i * K, hi, lo) for i in range(32)]


def rec(direction="LONG"):
    return {"symbol": "XUSDT", "ts": START, "direction": direction, "snapshot": {"price": 100.0}}


class TestValidateWindow(unittest.TestCase):
    NOW = START + H8 + 3 * 3600 * 1000

    def test_complete(self):
        self.assertIsNone(TR._validate_window(full(), START, START + H8, self.NOW))

    def test_immature_when_window_not_finished(self):
        self.assertEqual(TR._validate_window(full(), START, START + H8, START + H8 - 1), "immature")

    def test_gap_is_insufficient(self):
        ks = full(); del ks[10]
        self.assertEqual(TR._validate_window(ks, START, START + H8, self.NOW), "insufficient")

    def test_truncated_tail_is_insufficient(self):
        self.assertEqual(TR._validate_window(full()[:20], START, START + H8, self.NOW), "insufficient")

    def test_late_start_is_insufficient(self):
        ks = [kl(START + 5 * K + i * K, 101, 99) for i in range(27)]
        self.assertEqual(TR._validate_window(ks, START, START + H8, self.NOW), "insufficient")

    def test_empty_is_insufficient(self):
        self.assertEqual(TR._validate_window([], START, START + H8, self.NOW), "insufficient")


class TestOutcome(unittest.TestCase):
    def setUp(self):
        TR._clear_kline_cache()      # [v3.89 #47] 行情合并缓存跨用例会复用上一个用例的 K 线
        self.addCleanup(TR._clear_kline_cache)

    def test_complete_touch(self):
        with mock.patch.object(TR, "_get_klines", return_value=full(hi=103)):
            r = TR._check_rejected_outcome(rec("LONG"))
        self.assertEqual(r["status"], "complete")
        self.assertTrue(r[TOUCH])
        self.assertEqual(r["max_move_pct"], 3.0)

    def test_complete_no_touch_short(self):
        with mock.patch.object(TR, "_get_klines", return_value=full(lo=99)):
            r = TR._check_rejected_outcome(rec("SHORT"))
        self.assertFalse(r[TOUCH])

    def test_gap_not_counted_as_complete(self):
        ks = full(hi=110); del ks[3]
        with mock.patch.object(TR, "_get_klines", return_value=ks):
            r = TR._check_rejected_outcome(rec("LONG"))
        self.assertEqual(r["status"], "insufficient")

    def test_no_klines_and_invalid_record(self):
        with mock.patch.object(TR, "_get_klines", return_value=[]):
            self.assertEqual(TR._check_rejected_outcome(rec())["status"], "insufficient")
        self.assertEqual(TR._check_rejected_outcome({"symbol": "X", "ts": START})["status"], "invalid")


class TestRunDenominator(unittest.TestCase):
    def _run(self, outcomes):
        import json, tempfile
        d = Path(tempfile.mkdtemp()); f = d / "a.jsonl"
        recs = [{"ts": START + i, "symbol": "X", "direction": "LONG", "judge_passed": False,
                 "reject_reason": "R=1", "snapshot": {"price": 100}} for i in range(len(outcomes))]
        f.write_text("\n".join(json.dumps(r) for r in recs), encoding="utf-8")
        it = iter(outcomes)
        cfg = type(sys)("config"); cfg.SIGNAL_ANALYTICS_FILE = f
        with mock.patch.dict(sys.modules, {"config": cfg}), \
             mock.patch.object(TR, "_check_rejected_outcome", side_effect=lambda r: next(it)), \
             mock.patch.object(TR.time, "sleep"):
            return TR.run(analytics_file=f, days=3650)

    def test_insufficient_excluded_from_denominator(self):
        c = {"status": "complete", TOUCH: True, "max_move_pct": 3.0}
        n = {"status": "complete", TOUCH: False, "max_move_pct": 0.5}
        rep = self._run([c, n, {"status": "insufficient"}, {"status": "immature"}])
        d = rep["R"]
        self.assertEqual(d["samples"], 2)                 # 只有完整样本进分母
        self.assertEqual(d[RATE], 50.0)
        self.assertEqual((d["insufficient"], d["immature"]), (1, 1))

    def test_all_insufficient_has_no_rate(self):
        rep = self._run([{"status": "insufficient"}])
        self.assertEqual(rep["R"]["samples"], 0)
        self.assertIsNone(rep["R"][RATE])


class TestOutputWording(unittest.TestCase):
    def test_no_causal_conclusion_words(self):
        if TOUCH != "direction_touched":
            self.skipTest("#45 之前的输出命名")
        import io, contextlib
        rep = {"R": {"samples": 5, RATE: 80.0, "avg_move_pct": 3.0, "immature": 0, "insufficient": 1, "invalid": 0}}
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            TR.print_report(rep)
        out = buf.getvalue()
        self.assertIn("方向触达率", out)
        self.assertNotIn("误拒", out)
        self.assertNotIn("拒绝合理", out)
        self.assertNotIn("assessment", rep["R"])


TOUCH = "direction_touched"
RATE = "direction_touch_pct"

if __name__ == "__main__":
    unittest.main()
