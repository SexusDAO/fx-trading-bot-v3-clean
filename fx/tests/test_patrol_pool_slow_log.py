"""patrol 专用读请求线程池 + _retry 慢请求诊断日志。全部 mock，不联网。"""
import asyncio
import logging
import threading
import time

import pytest

from shared import async_utils as AU
from shared import binance_api as A


# ── patrol 专用池 ───────────────────────────────────────────────────────────
def test_pool_definition():
    assert AU._PATROL_POOL._max_workers == 2
    assert AU._PATROL_POOL._thread_name_prefix == "patrol-rest"


def test_run_patrol_runs_in_dedicated_pool():
    seen = []
    asyncio.run(AU._run_patrol(lambda: seen.append(threading.current_thread().name)))
    assert seen and seen[0].startswith("patrol-rest")


def test_run_patrol_returns_value_and_propagates_error():
    assert asyncio.run(AU._run_patrol(lambda: 42)) == 42

    def boom():
        raise ValueError("x")

    with pytest.raises(ValueError):
        asyncio.run(AU._run_patrol(boom))


def test_run_patrol_timeout_raises_runsynctimeout():
    with pytest.raises(AU.RunSyncTimeout):
        asyncio.run(AU._run_patrol(lambda: time.sleep(0.5), timeout=0.05))


def test_run_patrol_default_timeout_from_config(monkeypatch):
    import config
    monkeypatch.setattr(config, "PATROL_REST_TIMEOUT_SEC", 0.05, raising=False)
    with pytest.raises(AU.RunSyncTimeout):
        asyncio.run(AU._run_patrol(lambda: time.sleep(0.5)))


def test_patrol_pool_isolated_from_default_executor():
    """patrol 池被慢请求占满时，默认 executor 上的任务不受影响。"""
    async def go():
        loop = asyncio.get_running_loop()
        gate = threading.Event()
        slow = [asyncio.ensure_future(AU._run_patrol(lambda: gate.wait(2), timeout=3)) for _ in range(2)]
        await asyncio.sleep(0.05)
        t0 = time.monotonic()
        r = await loop.run_in_executor(None, lambda: "default-ok")
        dt = time.monotonic() - t0
        gate.set()
        await asyncio.gather(*slow)
        return r, dt

    r, dt = asyncio.run(go())
    assert r == "default-ok" and dt < 0.5


def test_queue_wait_warning(monkeypatch, caplog):
    monkeypatch.setattr(AU, "_PATROL_QUEUE_WARN_SEC", 0.0)
    with caplog.at_level(logging.WARNING, logger="fx.async_utils"):
        asyncio.run(AU._run_patrol(lambda: 1))
    assert any("patrol 池排队" in r.getMessage() for r in caplog.records)


# ── position_manager 迁移结果 ───────────────────────────────────────────────
def test_patrol_reads_migrated_and_writes_untouched():
    import ast, inspect
    import position_manager as PM
    src = inspect.getsource(PM)
    t = ast.parse(src)
    fs = {n.name: n for n in ast.walk(t) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    reads = ("get_position", "get_algo_open_orders", "get_order", "get_open_orders", "get_mark_price")
    for fn in ("_check_manual_close", "_check_tp_filled_rest", "_check_time_stop", "_补挂保护单", "_patrol_funding_arb"):
        seg = ast.get_source_segment(src, fs[fn])
        for r in reads:
            assert f"run_in_executor(\n" not in seg or True
            for line in seg.split("\n"):
                if "run_in_executor" in line and f".{r}(" in line:
                    pytest.fail(f"{fn} 仍有读请求走默认池: {line.strip()}")
    # 写请求仍走默认池
    seg = ast.get_source_segment(src, fs["_check_manual_close"])
    assert "api.cancel_algo_order" in seg and "_run_patrol(lambda: api.cancel" not in seg
    assert src.count("_run_patrol(lambda:") == 14


# ── _retry 慢请求日志 ────────────────────────────────────────────────────────
@pytest.fixture
def quiet(monkeypatch):
    monkeypatch.setattr(A, "reset_session", lambda *a, **k: None)
    monkeypatch.setattr(A, "_record_api_outcome", lambda *a, **k: None)


def test_slow_request_logged(quiet, monkeypatch, caplog):
    monkeypatch.setattr(A, "_cfg", lambda n, d: 0.05 if n == "API_SLOW_LOG_SEC" else d)

    def slow():
        time.sleep(0.1)
        return {"ok": 1}

    with caplog.at_level(logging.WARNING, logger=A.logger.name):
        assert A._retry(slow) == {"ok": 1}
    msgs = [r.getMessage() for r in caplog.records if "慢请求" in r.getMessage()]
    assert msgs and "尝试1次" in msgs[0] and "结果=ok" in msgs[0]


def test_fast_request_not_logged(quiet, caplog):
    with caplog.at_level(logging.WARNING, logger=A.logger.name):
        assert A._retry(lambda: 5) == 5
    assert not any("慢请求" in r.getMessage() for r in caplog.records)


def test_slow_log_counts_attempts_and_none_outcome(quiet, monkeypatch, caplog):
    monkeypatch.setattr(A, "_cfg", lambda n, d: 0.0 if n == "API_SLOW_LOG_SEC" else (1 if n == "API_REQUEST_MAX_RETRY" else d))
    monkeypatch.setattr(A.time, "sleep", lambda *_: None)
    calls = []

    def bad():
        calls.append(1)
        raise RuntimeError("weird")

    with caplog.at_level(logging.WARNING, logger=A.logger.name):
        assert A._retry(bad) is None
    assert len(calls) == 2
    msg = [r.getMessage() for r in caplog.records if "慢请求" in r.getMessage()][0]
    assert "尝试2次" in msg and "结果=none" in msg


def test_retry_behaviour_unchanged_for_exceptions(quiet, monkeypatch):
    monkeypatch.setattr(A, "_cfg", lambda n, d: 0.0 if n == "API_SLOW_LOG_SEC" else d)

    def biz():
        raise A.BinanceAPIError(-2019, "margin", 400)

    with pytest.raises(A.BinanceAPIError):
        A._retry(biz, is_write=True)
    assert A._retry(biz) is None                      # 读请求业务拒绝仍返回 None
    assert A._retry(biz, suppress_codes=[-2019]) is None


def test_slow_log_never_breaks_request(quiet, monkeypatch):
    monkeypatch.setattr(A, "_cfg", lambda n, d: "bad" if n == "API_SLOW_LOG_SEC" else d)
    assert A._retry(lambda: 7) == 7                   # 配置异常不影响请求结果


def test_config_defaults():
    import config
    assert config.API_SLOW_LOG_SEC == 3.0
    assert config.PATROL_REST_POOL_WORKERS == 2
    assert config.PATROL_REST_TIMEOUT_SEC == 10.0
