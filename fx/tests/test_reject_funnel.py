"""运行：cd fx && python3 -m unittest tests.test_reject_funnel -v
[v3.89 #43] 拒绝漏斗只统计含 judge_passed 的判定事件；补丁合并；缺字段/损坏单独计数。"""
import importlib.util, json, tempfile, time, unittest
from pathlib import Path

_p = Path(__file__).parent.parent / "tools" / "reject_funnel.py"
_spec = importlib.util.spec_from_file_location("reject_funnel", _p)
RF = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(RF)


def _write(lines):
    d = Path(tempfile.mkdtemp()); f = d / "signal_analytics.jsonl"
    f.write_text("\n".join(x if isinstance(x, str) else json.dumps(x) for x in lines), encoding="utf-8")
    return [f]


NOW = int(time.time() * 1000)


class TestLoadRecords(unittest.TestCase):
    def test_only_verdict_events_counted_patches_not(self):
        files = _write([
            {"ts": NOW, "symbol": "A", "mode": "pullback", "judge_passed": True},
            {"ts": NOW + 1000, "symbol": "A", "mode": "pullback", "type": "entry_params_patch", "entry_params_ok": False},
            {"ts": NOW, "symbol": "B", "mode": "breakout", "judge_passed": False, "reject_reason": "x"},
        ])
        verdicts, st = RF._load_records(files, NOW - 10)
        self.assertEqual(len(verdicts), 2)                    # 补丁没有被当成判定事件
        self.assertEqual(st["patch_merged"], 1)
        self.assertEqual(st["missing_judge_passed"], 0)
        a = next(v for v in verdicts if v["symbol"] == "A")
        self.assertIs(a["entry_params_ok"], False)            # 补丁字段合并进判定事件

    def test_exact_symbol_ts_merge(self):
        files = _write([
            {"ts": NOW, "symbol": "A", "mode": "pullback", "judge_passed": True},
            {"ts": NOW, "symbol": "A", "type": "entry_params_patch", "entry_params_ok": True},
        ])
        verdicts, st = RF._load_records(files, NOW - 10)
        self.assertEqual(st["patch_merged"], 1)

    def test_orphan_patch_counted_separately(self):
        files = _write([{"ts": NOW, "symbol": "Z", "mode": "m", "type": "entry_params_patch"}])
        verdicts, st = RF._load_records(files, NOW - 10)
        self.assertEqual((len(verdicts), st["orphan_patch"]), (0, 1))

    def test_patch_outside_window_not_merged(self):
        files = _write([
            {"ts": NOW, "symbol": "A", "mode": "m", "judge_passed": True},
            {"ts": NOW + RF.PATCH_WINDOW_MS + 1, "symbol": "A", "mode": "m", "type": "entry_params_patch"},
        ])
        _, st = RF._load_records(files, NOW - 10)
        self.assertEqual(st["orphan_patch"], 1)

    def test_missing_field_and_corrupt_counted_separately(self):
        files = _write([
            {"ts": NOW, "symbol": "A", "mode": "m"},                 # 缺 judge_passed，不是补丁
            "{not json",                                              # 损坏
            "[1,2,3]",                                                # 不是对象
            {"symbol": "A", "judge_passed": True},                   # 缺 ts
            {"ts": "x", "judge_passed": True},                        # ts 非数值
            {"ts": NOW, "symbol": "C", "mode": "m", "judge_passed": False},
        ])
        verdicts, st = RF._load_records(files, NOW - 10)
        self.assertEqual(len(verdicts), 1)
        self.assertEqual(st["missing_judge_passed"], 1)
        self.assertEqual(st["corrupt"], 4)

    def test_old_records_ignored(self):
        files = _write([{"ts": NOW - 10 ** 9, "symbol": "A", "mode": "m", "judge_passed": True}])
        verdicts, st = RF._load_records(files, NOW - 10)
        self.assertEqual(len(verdicts), 0)


if __name__ == "__main__":
    unittest.main()
