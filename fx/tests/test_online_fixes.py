"""线上 4 处修复测试：全部 mock，不联网。"""
import asyncio
import time
from unittest.mock import MagicMock

import pytest

import executor
from shared import state
from shared import signal_watcher as sw


def run(c):
    return asyncio.new_event_loop().run_until_complete(c)


# ── 【修复1】 ─────────────────────────────────────────────────────────────
def test_crash_guard_short_not_block_long():
    assert executor._crash_cd_allows("crash_guard_short", "LONG")
    assert not executor._crash_cd_allows("crash_guard_short", "SHORT")


def test_crash_guard_long_not_block_short():
    assert executor._crash_cd_allows("crash_guard", "SHORT")
    assert not executor._crash_cd_allows("crash_guard", "LONG")
    assert not executor._crash_cd_allows("stop_loss_cooldown", "SHORT")


# ── 【修复2】 ─────────────────────────────────────────────────────────────
def test_notifier_not_blocking_trigger(monkeypatch):
    import shared.notifier as nt
    monkeypatch.setattr(nt, "card_append", lambda *a, **k: time.sleep(5))
    monkeypatch.setattr(nt, "flush_signal", lambda *a, **k: time.sleep(5))
    monkeypatch.setattr(nt, "fmt_node_trigger", lambda *a, **k: "x")
    monkeypatch.setattr(sw, "_get_hub_or_rest", lambda *a, **k: {"markPrice": "100.5"})
    hit = {}

    async def on_trig(sig):
        hit["t"] = time.monotonic()
    sig = {"symbol": "ETHUSDT", "direction": "LONG", "mode": "pullback", "_sig_id": "S1",
           "ind": {"atr_1h": 10.0}}
    e = sw.WatchEntry(signal=sig, entry_low=100, entry_high=101, entry_price=100,
                      expire_ts=time.time() + 3600, on_trigger=on_trig)
    w = sw.SignalWatcher()
    w._watched["ETHUSDT"] = e

    async def go():
        t0 = time.monotonic()
        await w._trigger("ETHUSDT", 100.5, e)
        return t0
    t0 = run(go())
    assert "t" in hit and hit["t"] - t0 < 1.0


# ── 【修复3】 ─────────────────────────────────────────────────────────────
def test_future_event_13s_accepted():
    assert sw.SignalWatcher._EVT_FUTURE_TOL_MS == 15_000
    w = sw.SignalWatcher()
    assert w._event_acceptable("ETHUSDT", int((time.time() + 13) * 1000))
    assert not w._event_acceptable("BTCUSDT", int((time.time() + 20) * 1000))


# ── 【修复4】 ─────────────────────────────────────────────────────────────
@pytest.fixture
def cd_store(monkeypatch):
    store = {}
    monkeypatch.setattr(state, "_load_cd", lambda: store)
    monkeypatch.setattr(state, "_save_cd", lambda d: None)
    return store


def test_crash_cd_capped_8h(cd_store, monkeypatch):
    t = [1_700_000_000.0]
    monkeypatch.setattr(state.time, "time", lambda: t[0])
    state.add_cooldown("XUSDT", 4 * 3600, "crash_guard")
    first = cd_store["XUSDT"]["first_trigger_ts"]
    for _ in range(5):           # 每 3 小时刷新一次 4 小时冷却
        t[0] += 3 * 3600
        if t[0] * 1000 >= first + 8 * 3600 * 1000:
            break
        state.add_cooldown("XUSDT", 4 * 3600, "crash_guard_short")
    assert cd_store["XUSDT"]["cooldown_until"] <= first + 8 * 3600 * 1000
    assert cd_store["XUSDT"]["first_trigger_ts"] == first


def test_non_crash_cd_unaffected(cd_store, monkeypatch):
    monkeypatch.setattr(state.time, "time", lambda: 1_700_000_000.0)
    state.add_cooldown("YUSDT", 7 * 86400, "repeated_stop_loss")
    assert cd_store["YUSDT"]["cooldown_until"] == (1_700_000_000 + 7 * 86400) * 1000
    assert "first_trigger_ts" not in cd_store["YUSDT"]
