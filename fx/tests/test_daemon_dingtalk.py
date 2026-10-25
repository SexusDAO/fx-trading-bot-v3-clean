"""daemon 启动/异常退出钉钉通知：纯函数 + 发送路径（全部桩，不联网）。
运行：cd fx && python3 -m pytest tests/test_daemon_dingtalk.py -v"""
import sys, unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
import daemon_start as D


class TestDaemonDingtalk(unittest.TestCase):
    def test_startup_text_format(self):
        t = D._startup_text(datetime(2026, 10, 26, 3, 7, tzinfo=timezone.utc), "abcdef12")
        self.assertEqual(t, "🚀 FX 交易系统已启动\n时间：03:07 UTC\n版本：abcdef12")

    def test_git_commit8_failure_returns_unknown(self):
        with mock.patch.object(D.subprocess, "run", side_effect=OSError("no git")):
            self.assertEqual(D._git_commit8(), "unknown")

    def test_git_commit8_ok(self):
        r = mock.Mock(returncode=0, stdout="1a2b3c4d\n")
        with mock.patch.object(D.subprocess, "run", return_value=r):
            self.assertEqual(D._git_commit8(), "1a2b3c4d")

    def test_abnormal_exit_rules(self):
        self.assertTrue(D._is_abnormal_exit(1, False))
        self.assertTrue(D._is_abnormal_exit(-9, False))        # 被 SIGKILL
        self.assertFalse(D._is_abnormal_exit(0, False))
        self.assertFalse(D._is_abnormal_exit(-15, True))       # 内存守护主动重启
        self.assertFalse(D._is_abnormal_exit(1, True))

    def test_notify_dingtalk_sends_sync_and_swallows_errors(self):
        from shared import notifier as N
        with mock.patch.object(N, "_dingtalk_send") as s:
            D.notify_dingtalk("hi")
            s.assert_called_once_with("hi")
        with mock.patch.object(N, "_dingtalk_send", side_effect=RuntimeError("boom")), \
             mock.patch.object(D, "log") as lg:
            D.notify_dingtalk("x")                              # 不抛异常
            self.assertTrue(lg.called)

    def test_dingtalk_notify_wait_flag(self):
        from shared import notifier as N
        with mock.patch.object(N, "_dingtalk_send") as s, mock.patch.object(N, "_dingtalk_send_bg") as b:
            N.dingtalk_notify("a", wait=True); s.assert_called_once_with("a"); b.assert_not_called()
            N.dingtalk_notify("b"); b.assert_called_once_with("b")


if __name__ == "__main__":
    unittest.main()
