"""阶段0 可观测性（mock，不联网）：心跳延迟、扫描耗时分解日志、WS 流健康。"""
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

from shared import observability as ob

FX = Path(__file__).parent.parent


# ── 心跳延迟 ──────────────────────────────────────────────────────────────
def test_heartbeat_delay_is_gap_minus_interval():
    assert ob.heartbeat_delay(100.0, 105.0, 5.0) == 0.0
    assert ob.heartbeat_delay(100.0, 106.2, 5.0) == pytest.approx(1.2)
    assert ob.heartbeat_delay(100.0, 104.9, 5.0) == 0.0          # 不出现负数


@pytest.mark.parametrize("d,exp", [(0.0, None), (1.0, None), (1.01, "warn"), (5.0, "warn"), (5.01, "error"), (30, "error")])
def test_heartbeat_level_thresholds(d, exp):
    assert ob.heartbeat_level(d) == exp


def test_monitor_uses_heartbeat_helpers_and_keeps_task_name():
    src = (FX / "main.py").read_text(encoding="utf-8")
    assert "heartbeat_delay" in src and "heartbeat_level" in src
    assert "_loop_event_latency_monitor()" in src and 'name="loop_latency"' in src
    assert "_classify_loop_latency" in src                         # 旧探针保留


def test_monitor_logs_warning_and_error(monkeypatch, caplog):
    """用假的 monotonic 驱动 monitor：心跳间隔 6.5s → warn，11s → error。"""
    import types, importlib
    src = (FX / "main.py").read_text(encoding="utf-8")
    start = src.index("def _classify_loop_latency")
    end = src.index("async def _loop_patrol")
    ns = {"asyncio": asyncio, "logger": logging.getLogger("main_test")}
    exec(src[start:end], ns)
    clock = {"t": 0.0}
    seq = iter([6.5, 0.0, 11.0, 0.0])                              # 每次 sleep 实际耗时（心跳, 探针, 心跳, 探针）

    class Stop(Exception):
        pass

    async def fake_sleep(s):
        try:
            clock["t"] += next(seq) if s == 5.0 or s == 0.1 else s
        except StopIteration:
            raise Stop()
    real_import = ns["_loop_event_latency_monitor"]
    ns["asyncio"] = types.SimpleNamespace(sleep=fake_sleep)
    import time as _time
    monkeypatch.setattr(_time, "monotonic", lambda: clock["t"])
    caplog.set_level(logging.DEBUG, logger="main_test")
    with pytest.raises(Stop):
        asyncio.run(ns["_loop_event_latency_monitor"]())
    msgs = [(r.levelname, r.getMessage()) for r in caplog.records]
    assert ("WARNING", "[loop] 心跳延迟 1.50s") in msgs
    assert any(l == "ERROR" and "心跳延迟 6.00s" in m for l, m in msgs)


# ── 扫描耗时分解 ───────────────────────────────────────────────────────────
def test_format_scan_perf():
    s = ob.format_scan_perf(0.123, 0.456, 12.3, 0.9, 7, 200)
    assert s == "[scan-perf] 候选池加载=0.12s 批量标记价=0.46s 并行分析=12.30s 信号后处理=0.90s scan_unfinished=7 候选池=200"


def test_run_scan_records_four_stages():
    src = (FX / "scanner.py").read_text(encoding="utf-8")
    for key in ("_perf_pool", "_perf_mark", "_perf_analysis", "_perf_post_t0", "format_scan_perf", "len(_unfinished)"):
        assert key in src


# ── WS 流健康 ──────────────────────────────────────────────────────────────
def test_stream_health_states():
    rows = ob.stream_health({"userData": 100.0, "markPrice": 10.0, "forceOrder": 0.0}, now=110.0, stale_sec=60)
    assert rows == [("forceOrder", None, "never"), ("markPrice", 100.0, "stale"), ("userData", 10.0, "ok")]
    assert ob.format_stream_health(rows) == "[ws-health] forceOrder=never markPrice=stale(100s前) userData=ok(10s前)"


def test_listener_health_and_loop(monkeypatch, caplog):
    from shared import ws_listener as W
    async def go():
        l = W.WsOrderListener()
        assert [r[2] for r in l.get_stream_health()] == ["never"] * 3
        now = W.time.monotonic()
        l._stream_last_msg.update({"userData": now, "markPrice": now, "forceOrder": now})
        assert [r[2] for r in l.get_stream_health()] == ["ok"] * 3
        n = {"c": 0}
        real_sleep = asyncio.sleep

        async def fast_sleep(s):
            n["c"] += 1
            if n["c"] > 1:
                l._stop_flag = True
            await real_sleep(0)
        monkeypatch.setattr(W.asyncio, "sleep", fast_sleep)
        caplog.set_level(logging.INFO, logger=W.logger.name)
        await l._health_loop(300.0)
    asyncio.run(go())
    assert any("[ws-health]" in r.getMessage() and "userData=ok" in r.getMessage() for r in caplog.records)


def test_listener_marks_stream_timestamps_in_loops():
    src = (FX / "shared" / "ws_listener.py").read_text(encoding="utf-8")
    for name in ("userData", "markPrice", "forceOrder"):
        assert f'self._stream_last_msg["{name}"] = time.monotonic()' in src
    assert "_health_task" in src and "ws_health" in src
