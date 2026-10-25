"""信号卡片 flush 时机：add_signal 只缓冲，_trigger 价格触发时 flush（进入监控 + 价格触发合并），
executor 预检拒绝在已发出的卡片上追加「预检拒绝」行，不产生幽灵卡片。
运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_notifier_flush_timing.py
全部 mock，不连交易所、不发真实消息。"""
import asyncio, os, re, sys, time
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
_fx = Path(__file__).parent.parent
sys.path.insert(0, str(_fx))

import pytest
from shared import notifier as N
from shared import signal_watcher as sw


@pytest.fixture
def tg(monkeypatch):
    """记录「新发消息 / 编辑消息 / 独立通知」，并隔离缓冲区与卡片缓存。"""
    rec = {"sent": [], "edits": [], "notify": []}
    monkeypatch.setattr(N, "_send", lambda t: (rec["sent"].append(t) or 7))
    monkeypatch.setattr(N, "_edit", lambda m, t: (rec["edits"].append(t) or True))
    monkeypatch.setattr(N, "notify", lambda t, *a, **k: rec["notify"].append(t))
    monkeypatch.setattr(N, "_save_card_state", lambda *a, **k: None)
    N._signal_buffer._buf.clear(); N._signal_buffer._ts.clear(); N._card_cache.clear()
    from shared import binance_api
    monkeypatch.setattr(binance_api, "get_mark_price", lambda s: {"markPrice": "100"})
    yield rec
    N._signal_buffer._buf.clear(); N._signal_buffer._ts.clear(); N._card_cache.clear()


async def _noop(*a, **k):
    return None


def _sig(scan_ts=1):
    return {"symbol": "XUSDT", "mode": "reversal", "direction": "SHORT", "pre_entry_price": 100.0,
            "_scan_ts": scan_ts, "ind": {"atr_1h": 1.0}}


async def _register():
    w = sw.SignalWatcher()
    sig = _sig()
    await w.add_signal(sig, 99.5, 100.5, 100.0, time.time() + 600, _noop)
    return w, sig, sig["_sig_id"]


def test_add_signal_only_buffers_no_message(tg):
    async def go():
        w, sig, sid = await _register()
        return sid
    sid = asyncio.run(go())
    assert tg["sent"] == [] and tg["edits"] == [] and tg["notify"] == []      # 注册时一条都不发
    lines = N._signal_buffer._buf[sid]
    assert len(lines) == 2 and "进入监控" in lines[1]                          # 标题 + 进入监控行都在缓冲区


def test_trigger_flushes_monitor_and_trigger_lines_into_one_card(tg):
    async def go():
        w, sig, sid = await _register()
        await w._trigger("XUSDT", 100.0, w._watched["XUSDT"])
        await asyncio.sleep(0.3)   # 触发卡片追加/flush 现为后台线程任务（2s 超时），等其完成
        return sid
    sid = asyncio.run(go())
    assert len(tg["sent"]) == 1                                                 # 只发出一条消息（一张卡片）
    assert tg["notify"] == []                                                   # 没有降级成独立消息
    final = tg["sent"][0]                                                       # 卡片最终文本（flush 现为合并成一条消息一次发出，不再先发标题再编辑追加）
    assert "进入监控" in final and "价格触发" in final
    assert final.index("进入监控") < final.index("价格触发")
    assert sid not in N._signal_buffer._buf                                     # 缓冲区已清空


def test_unfired_signal_discard_leaves_no_ghost_card(tg):
    async def go():
        w, sig, sid = await _register()
        N.discard_signal(sid)                                                   # 过期/被淘汰路径
        return sid
    sid = asyncio.run(go())
    assert tg["sent"] == [] and tg["edits"] == [] and sid not in N._card_cache


def test_signal_watcher_no_direct_card_create():
    src = (_fx / "shared" / "signal_watcher.py").read_text()
    assert not re.search(r"\bcard_create\b", src)
    assert "buffer_signal(_sig_id, _header)" in src
    # 触发卡片追加+flush 现封装在后台线程任务 _card_job 内：先 card_append 再 flush_signal
    assert re.search(r"card_append\(_sid, _ln\)\n(?:\s*#[^\n]*\n)*\s+flush_signal\(_sid\)", src)


def test_precheck_reject_appends_line_to_flushed_card(tg, monkeypatch):
    import executor
    monkeypatch.setattr(executor, "_precheck", lambda s: (False, "流动性不足"))
    async def go():
        w, sig, sid = await _register()
        await w._trigger("XUSDT", 100.0, w._watched["XUSDT"])
        n_sent = len(tg["sent"])
        ok = await executor._execute_async(sig, None)
        return sid, n_sent, ok
    sid, n_sent, ok = asyncio.run(go())
    assert ok is False
    assert len(tg["sent"]) == n_sent == 1                                       # 预检拒绝没有新发卡片（无幽灵卡片）
    assert tg["notify"] == []
    final = tg["edits"][-1]
    assert "预检拒绝" in final and "流动性不足" in final and "进入监控" in final and "价格触发" in final
    assert sid not in N._card_cache                                             # card_remove 已清理缓存


def test_executor_precheck_path_does_not_call_discard(tg, monkeypatch):
    import executor
    monkeypatch.setattr(executor, "_precheck", lambda s: (False, "余额不足"))
    called = []
    assert not hasattr(executor, "discard_signal")                      # executor 已不再持有 discard_signal
    monkeypatch.setattr(N, "discard_signal", lambda sid: called.append(sid))
    sig = _sig(); sig["_sig_id"] = "XUSDT_1"
    N._card_cache["XUSDT_1"] = {"message_id": 7, "text": "H", "ts": time.time()}
    assert asyncio.run(executor._execute_async(sig, None)) is False
    assert called == []
    assert "预检拒绝" in tg["edits"][-1] and "余额不足" in tg["edits"][-1]
