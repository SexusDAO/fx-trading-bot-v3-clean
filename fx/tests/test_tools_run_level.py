"""运行：cd fx && python3 -m unittest tests.test_tools_run_level -v
[v3.89 #50] 离线工具的 run()/main() 级测试（#46/#47/#43）：全部 mock，不访问网络。
  track_rejected.run：非法方向/参考价/标的单独计 invalid、单条异常不中断；同标的同窗口只请求一次；sleep 只按真实请求计。
  reject_funnel.main：通过率与拒绝原因聚合、补丁合并、数据质量输出。"""
import contextlib, importlib.util, io, json, sys, tempfile, time, unittest
from pathlib import Path
from unittest import mock

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root))


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, _root / rel)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


TR = _load("track_rejected_rl", "tools/track_rejected.py")
RF = _load("reject_funnel_rl", "tools/reject_funnel.py")

K = TR.KLINE_MS
H8 = TR.TRACK_WINDOW_H * 3600 * 1000
START = (int(time.time() * 1000) // K) * K - H8 - 3 * 3600 * 1000     # 窗口已结束且已成熟


def _kl(open_ms, hi=101, lo=99):
    return [open_ms, "100", str(hi), str(lo), "100", "1"]


def _full(hi=101, lo=99):
    return [_kl(START + i * K, hi, lo) for i in range(32)]


def _rec(**over):
    r = {"symbol": "XUSDT", "ts": START, "direction": "LONG", "judge_passed": False,
         "reject_reason": "RSI过高: 80", "snapshot": {"price": 100.0}}
    r.update(over)
    return r


def _file(recs):
    f = Path(tempfile.mkdtemp()) / "signal_analytics.jsonl"
    f.write_text("\n".join(x if isinstance(x, str) else json.dumps(x) for x in recs), encoding="utf-8")
    return f


class TestTrackRejectedRun(unittest.TestCase):
    def _run(self, recs, klines=None):
        calls = []

        def fake(symbol, start_ms, limit=32):
            calls.append((symbol, start_ms, limit))
            return _full() if klines is None else klines
        with mock.patch.object(TR, "_get_klines", fake), mock.patch.object(TR.time, "sleep") as sl:
            rpt = TR.run(analytics_file=_file(recs), days=7)
        return rpt, calls, sl

    def test_missing_file_returns_empty(self):
        self.assertEqual(TR.run(analytics_file=Path(tempfile.mkdtemp()) / "none.jsonl"), {})

    def test_invalid_records_isolated_and_counted(self):
        recs = [
            _rec(reject_reason="好记录: 1"),
            _rec(reject_reason="坏方向: 1", direction="SIDEWAYS"),
            _rec(reject_reason="坏参考价: 1", snapshot={"price": 0}),
            _rec(reject_reason="坏参考价: 2", snapshot={"price": "abc"}),
            _rec(reject_reason="坏标的: 1", symbol=None),
            "{not json",
        ]
        rpt, calls, _ = self._run(recs)
        self.assertEqual(rpt["好记录"]["samples"], 1)
        self.assertEqual(rpt["坏方向"]["invalid"], 1)
        self.assertEqual(rpt["坏参考价"]["invalid"], 2)
        self.assertEqual(rpt["坏标的"]["invalid"], 1)
        self.assertEqual(rpt["坏方向"]["samples"], 0)

    def test_single_record_exception_does_not_abort_report(self):
        orig = TR._check_rejected_outcome

        def flaky(rec):
            if rec.get("reject_reason", "").startswith("炸"):
                raise RuntimeError("boom")
            return orig(rec)
        with mock.patch.object(TR, "_check_rejected_outcome", flaky):
            rpt, _, _ = self._run([_rec(reject_reason="炸: 1"), _rec(reject_reason="好: 1")])
        self.assertEqual(rpt["炸"]["invalid"], 1)
        self.assertEqual(rpt["好"]["samples"], 1)

    def test_same_symbol_window_requested_once_and_sleep_per_request(self):
        recs = [_rec(reject_reason="A: 1"), _rec(reject_reason="B: 1"), _rec(reject_reason="C: 1"),
                _rec(reject_reason="D: 1", symbol="YUSDT")]
        rpt, calls, sl = self._run(recs)
        self.assertEqual(sorted(c[0] for c in calls), ["XUSDT", "YUSDT"])   # 4 条记录只发 2 次请求
        self.assertEqual(sl.call_count, 2)                                  # sleep 只按真实请求计
        self.assertEqual(sum(v["samples"] for v in rpt.values()), 4)

    def test_cache_cleared_between_runs(self):
        _, calls1, _ = self._run([_rec()])
        _, calls2, _ = self._run([_rec()])
        self.assertEqual(len(calls1), 1)
        self.assertEqual(len(calls2), 1)       # 第二次 run 不复用上一次的缓存

    def test_immature_records_skipped(self):
        young = _rec(ts=int(time.time() * 1000) - 3600 * 1000)
        rpt, calls, _ = self._run([young])
        self.assertEqual(rpt, {})
        self.assertEqual(calls, [])


class TestRejectFunnelMain(unittest.TestCase):
    def _main(self, recs, days="7"):
        d = Path(tempfile.mkdtemp())
        (d / "signal_analytics.jsonl").write_text(
            "\n".join(x if isinstance(x, str) else json.dumps(x) for x in recs), encoding="utf-8")
        buf = io.StringIO()
        with mock.patch.object(RF, "LOG_DIR", d), mock.patch.object(sys, "argv", ["rf", "--days", days]), \
                contextlib.redirect_stdout(buf):
            RF.main()
        return buf.getvalue()

    def test_pass_rate_and_reject_aggregation(self):
        now = int(time.time() * 1000)
        recs = [
            {"ts": now - 1000, "symbol": "AUSDT", "mode": "breakout", "judge_passed": True},
            {"ts": now - 2000, "symbol": "BUSDT", "mode": "breakout", "judge_passed": False, "reject_reason": "RSI过高: 80"},
            {"ts": now - 3000, "symbol": "CUSDT", "mode": "breakout", "judge_passed": False, "reject_reason": "RSI过高: 82"},
            {"ts": now - 4000, "symbol": "DUSDT", "mode": "pullback", "judge_passed": False, "reject_reason": "量能不足"},
            "{broken",
        ]
        out = self._main(recs)
        self.assertIn("breakout", out)
        self.assertIn("通过 ", out)
        self.assertIn("1 / 3", out.replace("   ", " ").replace("  ", " "))   # breakout 3 条判定中 1 条通过
        self.assertIn("量能不足", out)
        self.assertIn("损坏记录（JSON/时间戳无效）: 1", out)

    def test_no_records_prints_notice_and_quality(self):
        out = self._main([])
        self.assertIn("没有找到 signal_analytics 判定记录", out)
        self.assertIn("数据质量", out)

    def test_old_records_ignored_by_days(self):
        old = int(time.time() * 1000) - 30 * 86400 * 1000
        out = self._main([{"ts": old, "symbol": "AUSDT", "mode": "breakout", "judge_passed": True}], days="7")
        self.assertIn("没有找到", out)


if __name__ == "__main__":
    unittest.main()
