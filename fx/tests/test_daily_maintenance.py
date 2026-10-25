import asyncio
import sys
from datetime import datetime, timezone

import pytest

try:
    import main
except SystemExit:   # main.py 启动时检查依赖，缺包会 sys.exit(1)；缺包环境下跳过本文件
    pytest.skip("main.py 依赖缺失（python_socks/aiohttp/websockets）", allow_module_level=True)


def test_seconds_until_midnight():
    t = datetime(2026, 10, 25, 23, 59, 30, tzinfo=timezone.utc)
    assert abs(main._seconds_until_next_utc_midnight(t) - 30) < 1e-6
    t = datetime(2026, 10, 25, 0, 0, 0, tzinfo=timezone.utc)
    assert abs(main._seconds_until_next_utc_midnight(t) - 86400) < 1e-6


def test_run_script_writes_log_and_returns_code(tmp_path, monkeypatch):
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "ok.py").write_text("import sys;print('hello', sys.argv[1:])\n")
    (tmp_path / "tools" / "bad.py").write_text("import sys;print('boom');sys.exit(3)\n")
    monkeypatch.setattr(main, "ROOT", tmp_path)
    assert asyncio.run(main._run_maintenance_script("ok.py", ["--x"], "a.log")) == 0
    assert asyncio.run(main._run_maintenance_script("bad.py", [], "a.log")) == 3
    txt = (tmp_path / "logs" / "a.log").read_text()
    assert "hello ['--x']" in txt and "boom" in txt and "ok.py --x" in txt


def test_run_script_timeout_kills(tmp_path, monkeypatch):
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "slow.py").write_text("import time;time.sleep(30)\n")
    monkeypatch.setattr(main, "ROOT", tmp_path)
    monkeypatch.setattr(main, "_MAINT_TIMEOUT_SEC", 1)
    assert asyncio.run(main._run_maintenance_script("slow.py", [], "s.log")) == -1
    assert "已终止" in (tmp_path / "logs" / "s.log").read_text()


def test_run_script_missing_file_never_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "ROOT", tmp_path)
    rc = asyncio.run(main._run_maintenance_script("nope.py", [], "m.log"))
    assert rc != 0


def test_loop_runs_both_scripts_with_expected_args(monkeypatch):
    calls = []
    async def fake_run(script, args, log):
        calls.append((script, tuple(args), log)); return 0
    state = {"n": 0}
    async def fake_sleep(sec):
        state["n"] += 1
        if state["n"] >= 3:
            raise asyncio.CancelledError()
    monkeypatch.setattr(main, "_run_maintenance_script", fake_run)
    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(main._loop_daily_maintenance())
    assert calls == [("reconcile_triggers.py", ("--dry-run",), "trigger_reconcile_auto.log"),
                     ("cleanup_rejected_log.py", ("--days", "7"), "cleanup_rejected_log_auto.log"),
                     ("shadow_verify.py", (), "shadow_verify_auto.log"),
                     ("daily_dingtalk.py", (), "daily_dingtalk_auto.log")]


def test_registered_in_main_tasks():
    import inspect
    src = inspect.getsource(main)
    assert 'asyncio.create_task(_loop_daily_maintenance()' in src and 'name="daily_maintenance"' in src
