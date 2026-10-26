"""触发器分析前 hub 快照未就绪：等待重试，到点仍不齐再单次 REST 补拉，而不是直接报 hub_snapshot_incomplete。"""
import asyncio
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import config
import shared.trigger_detector as td
from test_trigger_detector import FakeHub, Clock

SYM = "XUSDT"


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "TRIGGER_HUB_WAIT_SEC", 0.6, raising=False)
    monkeypatch.setattr(config, "TRIGGER_HUB_WAIT_POLL_SEC", 0.05, raising=False)
    calls = []

    def analyzer(symbol, tk, mp, kl, **k):
        calls.append((symbol, tk, kl))
        return {"analysis_result": "rejected", "signal_strategy": None, "registered": 0, "error": None}

    hub = FakeHub(Clock(time.time()))
    det = td.TriggerDetector(hub=hub, clock=time.time, analyzer=analyzer,
                             trigger_log=tmp_path / "t.jsonl", scan_log=tmp_path / "s.jsonl",
                             state_file=tmp_path / "st.json")
    rec = {"trigger_id": "t1", "symbol": SYM, "strategy": "pullback"}
    return det, hub, calls, rec


def _run(det, rec):
    async def go():
        det.mark_analyzing(rec["symbol"])
        await det._dispatch_one(rec)
        if det._wait_tasks:
            await asyncio.gather(*list(det._wait_tasks))
    asyncio.run(go())


def test_ready_runs_immediately_without_wait(env):
    det, hub, calls, rec = env
    hub.set(SYM, c1h=[100.0] * 100)
    t0 = time.time(); _run(det, rec)
    assert len(calls) == 1 and time.time() - t0 < 0.5
    assert det.dispatch_stats["rejected"] == 1 and det.dispatch_stats["error"] == 0
    assert not det.is_analyzing(SYM)


def test_not_ready_then_ready_within_wait_uses_hub_no_rest(env, monkeypatch):
    det, hub, calls, rec = env
    from shared import binance_api as api
    monkeypatch.setattr(api, "get_klines", lambda *a, **k: pytest.fail("hub 在等待期内就绪，不应走 REST"))
    monkeypatch.setattr(api, "get_ticker", lambda *a, **k: pytest.fail("不应走 REST"))
    threading.Timer(0.2, lambda: hub.set(SYM, c1h=[100.0] * 100)).start()
    _run(det, rec)
    assert len(calls) == 1
    assert det.dispatch_stats["error"] == 0 and det.dispatch_stats["rejected"] == 1
    assert not det.is_analyzing(SYM)


def test_never_ready_falls_back_to_single_rest_then_analyzes(env, monkeypatch):
    det, hub, calls, rec = env
    from shared import binance_api as api
    rows = [[i, "1", "2", "0.5", "1.5", "10", i + 1, "15", 1, "5", "0", "0"] for i in range(100)]
    rest = {"k": [], "t": 0}
    monkeypatch.setattr(api, "get_klines", lambda s, iv, n=100: rest["k"].append(iv) or rows)
    monkeypatch.setattr(api, "get_ticker", lambda s: rest.__setitem__("t", rest["t"] + 1) or {"lastPrice": "100"})
    monkeypatch.setattr(api, "get_mark_price", lambda s: {"markPrice": "100", "lastFundingRate": "0"})
    t0 = time.time(); _run(det, rec)
    assert time.time() - t0 >= 0.5                      # 确实等满了
    assert sorted(rest["k"]) == ["15m", "1h", "4h"] and rest["t"] == 1   # 每个缺失项只补拉一次
    assert len(calls) == 1 and det.dispatch_stats["error"] == 0
    assert not det.is_analyzing(SYM)


def test_rest_also_fails_reports_incomplete(env, monkeypatch):
    det, hub, calls, rec = env
    from shared import binance_api as api
    monkeypatch.setattr(api, "get_klines", lambda *a, **k: [])
    monkeypatch.setattr(api, "get_ticker", lambda *a, **k: {})
    monkeypatch.setattr(api, "get_mark_price", lambda *a, **k: {})
    _run(det, rec)
    assert calls == [] and det.dispatch_stats["error"] == 1
    assert not det.is_analyzing(SYM)


def test_wait_disabled_keeps_old_behavior(env, monkeypatch):
    det, hub, calls, rec = env
    monkeypatch.setattr(config, "TRIGGER_HUB_WAIT_SEC", 0, raising=False)
    _run(det, rec)
    assert calls == [] and det.dispatch_stats["error"] == 1 and not det._wait_tasks
