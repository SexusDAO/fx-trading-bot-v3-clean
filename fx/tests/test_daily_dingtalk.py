"""00:00 钉钉摘要 + 触发器 08:00 日报 + notify 钉钉开关（全部桩/临时文件，不联网）。
运行：cd fx && python3 -m pytest tests/test_daily_dingtalk.py -v"""
import json, sys, tempfile, unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from tools import daily_dingtalk as DD
from shared import notifier as N
import main as M


class TestDailyText(unittest.TestCase):
    def test_format_normal(self):
        t = DD.build_daily_text("2026-10-25", 288, 14, 6, 3, 12.345, False)
        self.assertEqual(t, "📊 每日报告 2026-10-25\n扫描次数：288 轮\n原始信号：14 个\n进入监控：6 个\n"
                            "开仓成交：3 笔\n今日盈亏：+12.35 U\n熔断状态：正常")

    def test_format_loss_and_circuit(self):
        t = DD.build_daily_text("2026-10-25", 0, 0, 0, 0, -7.5, True)
        self.assertIn("今日盈亏：-7.50 U", t)
        self.assertTrue(t.endswith("熔断状态：触发"))

    def test_read_circuit_open(self):
        d = Path(tempfile.mkdtemp())
        (d / "cb.json").write_text(json.dumps({"is_open": True}))
        self.assertTrue(DD.read_circuit_open(d / "cb.json"))
        (d / "cb.json").write_text(json.dumps({"is_open": False}))
        self.assertFalse(DD.read_circuit_open(d / "cb.json"))
        self.assertFalse(DD.read_circuit_open(d / "missing.json"))
        (d / "bad.json").write_text("{")
        self.assertFalse(DD.read_circuit_open(d / "bad.json"))


class TestTriggerText(unittest.TestCase):
    def test_first_report_since_start(self):
        t = DD.build_trigger_text({"miss": 3, "fp": 2, "suppressed": 5}, None, 40)
        self.assertEqual(t, "🔍 触发器日报（自启动以来）\n触发次数：40\nmiss型1：3（扫描有信号触发器没触发）\n误触发：2\n抑制：5")

    def test_delta_against_previous_snapshot(self):
        t = DD.build_trigger_text({"miss": 10, "fp": 4, "suppressed": 9}, {"miss": 3, "fp": 4, "suppressed": 5}, 7)
        self.assertTrue(t.startswith("🔍 触发器日报\n"))
        self.assertIn("miss型1：7", t); self.assertIn("误触发：0", t); self.assertIn("抑制：4", t)

    def test_counter_reset_after_restart_never_negative(self):
        t = DD.build_trigger_text({"miss": 1, "fp": 0, "suppressed": 0}, {"miss": 50, "fp": 9, "suppressed": 9}, 0)
        self.assertIn("miss型1：0", t)

    def test_count_triggers_window_and_row_types(self):
        d = Path(tempfile.mkdtemp()) / "t.jsonl"
        rows = [{"trigger_id": "a", "ts": 1000}, {"trigger_id": "b", "ts": 2000},
                {"type": "analysis", "trigger_id": "a", "ts": 1500},
                {"type": "post_price", "trigger_id": "a", "ts": 1600},
                {"trigger_id": "c", "ts": 9999}, {"ts": 1200}]
        d.write_text("\n".join(json.dumps(r) for r in rows) + "\nnot json\n")
        self.assertEqual(DD.count_triggers(d, 1000, 3000), 2)
        self.assertEqual(DD.count_triggers(d.parent / "none.jsonl", 0, 10 ** 12), 0)


class TestNotifyDingtalkSwitch(unittest.TestCase):
    def test_default_sends_dingtalk(self):
        with mock.patch.object(N, "_send"), mock.patch.object(N, "_dingtalk_send_bg") as b:
            N.notify("x"); b.assert_called_once_with("x")

    def test_dingtalk_false_skips(self):
        with mock.patch.object(N, "_send") as s, mock.patch.object(N, "_dingtalk_send_bg") as b:
            N.notify("x", dingtalk=False)
            s.assert_called_once_with("x"); b.assert_not_called()

    def test_notify_async_passes_flag(self):
        import asyncio
        with mock.patch.object(N, "_send"), mock.patch.object(N, "_dingtalk_send_bg") as b:
            asyncio.run(N.notify_async("y", dingtalk=False)); b.assert_not_called()
            asyncio.run(N.notify_async("z")); b.assert_called_once_with("z")


class TestSchedule(unittest.TestCase):
    def test_seconds_until_utc_hour(self):
        t = lambda h, m=0: datetime(2026, 10, 26, h, m, tzinfo=timezone.utc)
        self.assertEqual(M._seconds_until_next_utc_hour(8, t(7, 0)), 3600)
        self.assertEqual(M._seconds_until_next_utc_hour(8, t(8, 0)), 86400)      # 整点取明天
        self.assertEqual(M._seconds_until_next_utc_hour(8, t(9, 30)), 22.5 * 3600)

    def test_trigger_report_registered_only_with_detector(self):
        import inspect
        src = inspect.getsource(M)
        self.assertIn('_loop_trigger_report(_trigger_det)', src)
        i = src.index("if _trigger_det is not None:")
        self.assertLess(i, src.index('name="trigger_report"'))


if __name__ == "__main__":
    unittest.main()
