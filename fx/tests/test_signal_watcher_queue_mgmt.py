"""signal_watcher 监控队列主动管理：重挂去重(机制一)、重挂单受保护/槽位候选池(机制二、三)、持续有效性检查(机制四)。"""
import asyncio
import time

import pytest

import shared.signal_watcher as SW
from shared.signal_watcher import SignalWatcher, WatchEntry


async def _noop(*a, **k):
    return None


def _entry(direction="LONG", mode="pullback", entry_price=100.0, low=99.0, high=101.0, **kw):
    sig = {"symbol": kw.pop("symbol", "AAAUSDT"), "direction": direction, "mode": mode,
           "signal_strength": "medium"}
    sig.update(kw.pop("sig_extra", {}))
    return WatchEntry(signal=sig, entry_low=low, entry_high=high, entry_price=entry_price,
                      expire_ts=time.time() + 3600, on_trigger=_noop, **kw)


# ───────────── 机制一：参数没变不重挂 ─────────────
def test_unchanged_within_0_1_atr():
    e = _entry(sig_extra={"pre_params": {"sl_price": 95.0, "tp1_price": 110.0, "tp2_price": 120.0}})
    new = {"sl_price": 95.05, "tp1_price": 110.05, "tp2_price": 120.0}
    assert SW._reorder_params_unchanged(e, 100.05, new, atr=1.0) is True


def test_changed_entry_or_tp_beyond_0_1_atr():
    e = _entry(sig_extra={"pre_params": {"sl_price": 95.0, "tp1_price": 110.0}})
    assert SW._reorder_params_unchanged(e, 100.2, {"sl_price": 95.0, "tp1_price": 110.0}, atr=1.0) is False
    assert SW._reorder_params_unchanged(e, 100.0, {"sl_price": 95.0, "tp1_price": 110.3}, atr=1.0) is False


def test_missing_old_params_only_compares_entry_and_atr_zero_is_conservative():
    e = _entry()
    assert SW._reorder_params_unchanged(e, 100.05, {"sl_price": 1}, atr=1.0) is True
    assert SW._reorder_params_unchanged(e, 100.0, {}, atr=0.0) is False


# ───────────── 机制四：持续有效性检查 ─────────────
def _vc(e, **kw):
    args = dict(price=100.0, rsi_1h=50.0, atr=1.0, ema20=100.0, close_1h=100.0, funding=0.0)
    args.update(kw)
    return SignalWatcher._validity_check(e, **args)


def test_validity_passes_in_normal_conditions():
    assert _vc(_entry()) == ""


def test_ema20_structure_flip_long_and_short():
    assert "EMA20" in _vc(_entry("LONG"), close_1h=98.9)
    assert "EMA20" in _vc(_entry("SHORT"), close_1h=101.1)
    assert _vc(_entry("LONG"), close_1h=99.5) == ""


def test_ema20_check_skipped_for_reversal():
    assert _vc(_entry("LONG", mode="reversal"), close_1h=95.0, price=99.5) == ""


def test_rsi_extremes():
    assert "RSI" in _vc(_entry("LONG"), rsi_1h=73)
    assert "RSI" in _vc(_entry("LONG"), rsi_1h=27)
    assert "RSI" in _vc(_entry("SHORT"), rsi_1h=27)
    assert "RSI" in _vc(_entry("SHORT"), rsi_1h=73)
    assert _vc(_entry("SHORT", mode="reversal"), rsi_1h=75) == ""    # 反转做空以超买为前提，豁免
    assert "RSI" in _vc(_entry("SHORT", mode="reversal"), rsi_1h=25)  # 但追空过度仍退出


def test_price_beyond_entry_zone_more_than_1_atr():
    assert "跌破入场区间" in _vc(_entry("LONG"), price=97.9, close_1h=100, ema20=0)
    assert "涨破入场区间" in _vc(_entry("SHORT"), price=102.1, close_1h=100, ema20=0)
    assert _vc(_entry("LONG"), price=98.5, close_1h=100, ema20=0) == ""


def test_funding_thresholds():
    assert "资金费率" in _vc(_entry("LONG"), funding=0.0021)
    assert "资金费率" in _vc(_entry("SHORT"), funding=-0.0021)
    assert _vc(_entry("LONG"), funding=0.0019) == ""
    assert _vc(_entry("SHORT"), funding=0.0021) == ""


def test_atr_zero_skips_all():
    assert _vc(_entry(), atr=0.0, rsi_1h=99, funding=1.0) == ""


# ───────────── 退出通知 ─────────────
def test_notify_exit_only_when_removed(monkeypatch):
    import shared.notifier as N
    sent, dropped = [], []
    monkeypatch.setattr(N, "notify", lambda t: sent.append(t))
    monkeypatch.setattr(N, "discard_signal", lambda s: dropped.append(s))
    w = SignalWatcher()
    e = _entry(sig_extra={"_sig_id": "AAAUSDT_1"})
    w._watched["AAAUSDT"] = e                          # 仍在队列 → 撤单未确认，不发退出通知
    asyncio.run(w._notify_exit("AAAUSDT", e, "测试原因"))
    assert sent == []
    w._watched.pop("AAAUSDT")                          # 已移除 → 发通知并丢弃缓冲
    asyncio.run(w._notify_exit("AAAUSDT", e, "测试原因"))
    assert len(sent) == 1 and "退出监控" in sent[0] and "测试原因" in sent[0] and "AAAUSDT" in sent[0]
    assert dropped == ["AAAUSDT_1"]


# ───────────── 机制二、三：add_signal ─────────────
@pytest.fixture
def watcher(monkeypatch):
    w = SignalWatcher()

    async def _no_conflict(self, symbol, own_order_id):
        return None

    monkeypatch.setattr(SignalWatcher, "_exchange_conflict", _no_conflict)
    monkeypatch.setattr(SW, "_calc_pending_score", lambda e, now=None: float(e.signal.get("_score", 60)))
    return w


def _add(w, symbol, direction="LONG", score=60, **kw):
    sig = {"symbol": symbol, "direction": direction, "mode": "pullback", "_score": score,
           "signal_strength": "medium", "_scan_ts": 1}
    asyncio.run(w.add_signal(sig, 99.0, 101.0, 100.0, time.time() + 3600, _noop, **kw))


def test_reorder_entry_is_protected_from_same_symbol_competition(watcher):
    _add(watcher, "AAAUSDT", score=40, is_reorder=True, pending_score=40)
    assert watcher._watched["AAAUSDT"].is_reorder
    _add(watcher, "AAAUSDT", score=95)                  # 高很多的新分也不能替换重挂单
    assert watcher._watched["AAAUSDT"].is_reorder and watcher._watched["AAAUSDT"].pending_score == 40


def test_same_symbol_needs_more_than_8_points_to_replace(watcher):
    _add(watcher, "AAAUSDT", score=60)
    first = watcher._watched["AAAUSDT"]
    _add(watcher, "AAAUSDT", score=66)                  # 差 6 分：保持现有 +8 门槛，丢弃
    assert watcher._watched["AAAUSDT"] is first


def test_total_full_evicts_lowest_across_directions_but_never_reorder(watcher, monkeypatch):
    import config
    monkeypatch.setattr(config, "MAX_PENDING_TOTAL", 3, raising=False)
    monkeypatch.setattr(config, "MAX_PENDING_LONG", 5, raising=False)
    monkeypatch.setattr(config, "MAX_PENDING_SHORT", 5, raising=False)
    _add(watcher, "S1USDT", "SHORT", score=45)
    _add(watcher, "S2USDT", "SHORT", score=80)
    _add(watcher, "R1USDT", "SHORT", score=20, is_reorder=True, pending_score=20)   # 最低分但受保护
    assert len(watcher._watched) == 3
    _add(watcher, "L1USDT", "LONG", score=75)           # 总数满；候选池跨方向，只能踢 S1(45)
    assert set(watcher._watched) == {"S2USDT", "R1USDT", "L1USDT"}


def test_total_full_new_score_not_high_enough_is_dropped_without_clearing_queue(watcher, monkeypatch):
    import config
    monkeypatch.setattr(config, "MAX_PENDING_TOTAL", 2, raising=False)
    monkeypatch.setattr(config, "MAX_PENDING_LONG", 5, raising=False)
    monkeypatch.setattr(config, "MAX_PENDING_SHORT", 5, raising=False)
    _add(watcher, "S1USDT", "SHORT", score=70)
    _add(watcher, "S2USDT", "SHORT", score=72)
    _add(watcher, "L1USDT", "LONG", score=75)           # 75 <= 70+8 → 丢弃，队列不被清空
    assert set(watcher._watched) == {"S1USDT", "S2USDT"}


def test_triggered_entries_never_evicted(watcher, monkeypatch):
    import config
    monkeypatch.setattr(config, "MAX_PENDING_TOTAL", 2, raising=False)
    monkeypatch.setattr(config, "MAX_PENDING_LONG", 5, raising=False)
    monkeypatch.setattr(config, "MAX_PENDING_SHORT", 5, raising=False)
    _add(watcher, "S1USDT", "SHORT", score=45)
    _add(watcher, "S2USDT", "SHORT", score=80)
    watcher._watched["S1USDT"].triggered = True
    _add(watcher, "L1USDT", "LONG", score=95)
    assert watcher._watched["S1USDT"].triggered and "S1USDT" in watcher._watched
