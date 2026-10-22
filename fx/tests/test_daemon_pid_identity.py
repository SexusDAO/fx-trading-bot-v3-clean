"""运行：cd fx && python3 -m unittest tests.test_daemon_pid_identity -v
[v3.89 #104] PID 身份校验：区分 absent / alive / alive_no_permission / mismatch，身份不符不发信号。"""
import json, os, signal, subprocess, sys, tempfile, time, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
import daemon_start as D


class TestPidStatus(unittest.TestCase):
    def test_absent(self):
        self.assertEqual(D._pid_status(2 ** 22 + 12345), "absent")
        self.assertEqual(D._pid_status(None), "absent")

    def test_alive_with_matching_identity(self):
        me = os.getpid()
        exp = {"start": D._proc_start_token(me), "cmd": "python"}
        self.assertEqual(D._pid_status(me, exp), "alive")

    def test_cmd_mismatch(self):
        self.assertEqual(D._pid_status(os.getpid(), {"cmd": "definitely-not-this-program"}), "mismatch")

    def test_start_time_mismatch(self):
        self.assertEqual(D._pid_status(os.getpid(), {"start": "1"}), "mismatch")

    def test_permission_error_is_alive_not_absent(self):
        with mock.patch("os.kill", side_effect=PermissionError):
            self.assertEqual(D._pid_status(os.getpid()), "alive_no_permission")
            self.assertTrue(D._is_alive(os.getpid()))

    def test_mismatch_is_not_alive(self):
        self.assertFalse(D._is_alive(os.getpid(), {"cmd": "definitely-not-this-program"}))


class TestKillPid(unittest.TestCase):
    def test_mismatch_sends_no_signal(self):
        with mock.patch("os.kill") as k:
            k.side_effect = lambda pid, sig: None
            ok = D._kill_pid(os.getpid(), "x", wait_sec=1, expect={"cmd": "definitely-not-this-program"})
        self.assertTrue(ok)
        sent = [c.args[1] for c in k.call_args_list if c.args[1] != 0]
        self.assertEqual(sent, [])

    def test_no_permission_returns_false(self):
        with mock.patch("os.kill", side_effect=PermissionError):
            self.assertFalse(D._kill_pid(os.getpid(), "x", wait_sec=1))

    def test_kills_real_matching_process(self):
        p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            exp = {"start": D._proc_start_token(p.pid), "cmd": "time.sleep"}
            t = threading_reaper(p)
            self.assertTrue(D._kill_pid(p.pid, "child", wait_sec=5, expect=exp))
            t.join(2)
            self.assertIsNotNone(p.poll())
        finally:
            if p.poll() is None:
                p.kill()

    def test_does_not_kill_reused_pid(self):
        p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
        try:
            exp = {"start": "0", "cmd": "some-other-program"}      # 期望的是另一个进程
            self.assertTrue(D._kill_pid(p.pid, "stale", wait_sec=1, expect=exp))
            time.sleep(0.3)
            self.assertIsNone(p.poll())                            # 无关进程还活着
        finally:
            p.kill(); p.wait()


def threading_reaper(p):
    import threading
    t = threading.Thread(target=p.wait, daemon=True); t.start()
    return t


class TestPidFiles(unittest.TestCase):
    def test_write_pid_file_with_meta_roundtrip(self):
        d = Path(tempfile.mkdtemp()); f = d / "x.pid"
        D._write_pid_file(f, os.getpid(), "python")
        meta = json.loads(D._meta_path(f).read_text())
        self.assertEqual(meta["pid"], os.getpid())
        self.assertEqual(D._read_pid(f), os.getpid())
        self.assertEqual(D._pid_status(os.getpid(), D._expected_identity(f, "x")), "alive")

    def test_legacy_pid_file_without_meta_uses_default_cmd(self):
        d = Path(tempfile.mkdtemp()); f = d / "old.pid"
        f.write_text(str(os.getpid()))
        self.assertEqual(D._expected_identity(f, "python")["start"], None)
        self.assertEqual(D._pid_status(os.getpid(), D._expected_identity(f, "no-such-cmd")), "mismatch")


if __name__ == "__main__":
    unittest.main()
