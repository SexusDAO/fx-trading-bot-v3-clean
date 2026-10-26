"""reversal 触发区间改用 ATR 维度 + 存活检查反向阈值 0.6ATR（SHORT 涨破 / LONG 跌破触发价才撤单）。
运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_reversal_entry_range.py
"""
import os, sys, time
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from shared import signal_watcher as sw

ATR = 10.0
ENTRY = 1000.0

import config
HALF = config.SIGNAL_WATCH_ENTRY_BUFFER_ATR_REVERSAL      # reversal 触发半宽（ATR 倍数），随配置变化


async def _noop(*a, **k):
    return None


def _sig(direction, entry=ENTRY, atr=ATR, mode="reversal"):
    return {"symbol": "XUSDT", "mode": mode, "direction": direction,
            "pre_entry_price": entry, "ind": {"atr_1h": atr}}


def _entry(direction, trigger_price=ENTRY, atr_at_trigger=ATR):
    low, high, ep = sw.compute_entry_range(_sig(direction), mark_price=ENTRY)
    return sw.WatchEntry(signal=_sig(direction), entry_low=low, entry_high=high, entry_price=ep,
                         expire_ts=time.time() + 600, on_trigger=_noop,
                         trigger_price=trigger_price, atr_at_trigger=atr_at_trigger)


# ── compute_entry_range ──────────────────────────────────────────────────────
@pytest.mark.parametrize("direction", ["SHORT", "LONG"])
def test_reversal_range_is_half_atr(direction):
    low, high, ep = sw.compute_entry_range(_sig(direction), mark_price=ENTRY)
    assert ep == ENTRY
    assert low == pytest.approx(ENTRY - HALF * ATR)
    assert high == pytest.approx(ENTRY + HALF * ATR)


def test_reversal_range_fallback_when_atr_missing():
    s = _sig("SHORT"); s["ind"] = {}
    low, high, _ = sw.compute_entry_range(s, mark_price=2000.0)   # atr = mark*0.02 = 40 → 半宽 = 40*HALF
    assert low == pytest.approx(ENTRY - 40.0 * HALF)
    assert high == pytest.approx(ENTRY + 40.0 * HALF)


def test_pullback_range_unchanged_pct():
    low, high, ep = sw.compute_entry_range(_sig("LONG", mode="pullback"), mark_price=ENTRY)
    assert low == pytest.approx(ENTRY * (1 - sw._BUF_PULLBACK))
    assert high == pytest.approx(ENTRY * (1 + sw._BUF_PULLBACK))


def test_reversal_uses_config_atr_multiplier(monkeypatch):
    import config
    monkeypatch.setattr(config, "SIGNAL_WATCH_ENTRY_BUFFER_ATR_REVERSAL", 1.0, raising=False)
    low, high, _ = sw.compute_entry_range(_sig("SHORT"), mark_price=ENTRY)
    assert (low, high) == (pytest.approx(ENTRY - ATR), pytest.approx(ENTRY + ATR))


# ── 注册后不秒触发 / 进入 ±0.5ATR 才触发 ─────────────────────────────────────
def test_short_mark_2atr_below_entry_not_instant_trigger():
    e = _entry("SHORT")
    assert not e.price_in_range(ENTRY - 2 * ATR)


def test_long_mark_2atr_above_entry_not_instant_trigger():
    e = _entry("LONG")
    assert not e.price_in_range(ENTRY + 2 * ATR)


@pytest.mark.parametrize("direction", ["SHORT", "LONG"])
def test_trigger_only_inside_half_atr(direction):
    e = _entry(direction)
    assert not e.price_in_range(ENTRY - HALF * ATR - 0.01)
    assert e.price_in_range(ENTRY - HALF * ATR + 0.01)
    assert e.price_in_range(ENTRY)
    assert e.price_in_range(ENTRY + HALF * ATR - 0.01)
    assert not e.price_in_range(ENTRY + HALF * ATR + 0.01)


def test_old_pct_range_would_have_instant_triggered():
    """回归：旧 ±3% 区间会让 entry 下方 2ATR(=2%) 的 mark 秒触发，新区间不会。"""
    mark = ENTRY - 2 * ATR
    assert ENTRY * (1 - 0.03) <= mark <= ENTRY * (1 + 0.03)
    assert not _entry("SHORT").price_in_range(mark)


# ── _survival_check 反向阈值 0.6ATR（[v2-9]：SHORT 涨破触发价+0.6ATR、LONG 跌破触发价-0.6ATR 才撤单）──────
def _surv(entry, price, rsi):
    return sw.SignalWatcher._survival_check(entry, price, rsi, ATR)


def test_short_up_0_5atr_keeps_order():
    cancel, _ = _surv(_entry("SHORT"), ENTRY + 0.5 * ATR, rsi=70)
    assert cancel is False


def test_short_up_0_7atr_cancels():
    cancel, reason = _surv(_entry("SHORT"), ENTRY + 0.7 * ATR, rsi=70)
    assert cancel is True and "0.6ATR" in reason and "1ATR" not in reason.replace("0.6ATR", "")


def test_short_down_move_does_not_cancel():
    """SHORT 价格往有利方向（下跌）走不会被存活检查撤单（RSI 仍高于门槛时）。"""
    assert _surv(_entry("SHORT"), ENTRY - 1.1 * ATR, rsi=70)[0] is False


def test_long_down_0_5atr_keeps_order():
    cancel, _ = _surv(_entry("LONG"), ENTRY - 0.5 * ATR, rsi=30)
    assert cancel is False


def test_long_down_0_7atr_cancels():
    cancel, reason = _surv(_entry("LONG"), ENTRY - 0.7 * ATR, rsi=30)
    assert cancel is True and "0.6ATR" in reason and "1ATR" not in reason.replace("0.6ATR", "")


def test_long_up_move_does_not_cancel():
    assert _surv(_entry("LONG"), ENTRY + 1.1 * ATR, rsi=30)[0] is False


def test_survival_uses_trigger_atr_not_current():
    e = _entry("SHORT", atr_at_trigger=20.0)       # 触发时 ATR=20 → 阈值 ENTRY+12
    assert _surv(e, ENTRY + 10, rsi=70)[0] is False
    assert _surv(e, ENTRY + 13, rsi=70)[0] is True


def test_default_half_width_below_degraded_offset():
    """降级偏移为 0.5ATR，触发半宽必须严格小于它，否则降级后 mark 落在区间边界上。"""
    assert HALF == 0.3 and HALF < 0.5


def test_degraded_entry_no_instant_trigger():
    """挂单价过远 / recheck 重挂降级时 entry = mark + 0.5ATR，
    触发半宽 0.3ATR，mark 价应在区间外，不会秒触发。"""
    mark = 1000.0
    atr = 10.0
    entry = mark + 0.5 * atr  # = 1005，降级后的入场价
    sig = {"symbol": "XUSDT", "mode": "reversal", "direction": "SHORT",
           "pre_entry_price": entry, "ind": {"atr_1h": atr}}
    low, high, ep = sw.compute_entry_range(sig, mark_price=mark)
    # 触发区间 = [1005-3, 1005+3] = [1002, 1008]
    # mark=1000 应在区间外
    e = sw.WatchEntry(
        signal=sig, entry_low=low, entry_high=high, entry_price=ep,
        expire_ts=time.time() + 600, on_trigger=_noop
    )
    assert not e.price_in_range(mark), \
        f"mark={mark} 不应在触发区间 [{low}, {high}] 内"
    # 同时验证正好在触发区间内的价格能触发
    assert e.price_in_range(entry)


def test_degraded_entry_long_mirror_no_instant_trigger():
    mark, atr = 1000.0, 10.0
    entry = mark - 0.5 * atr
    sig = {"symbol": "XUSDT", "mode": "reversal", "direction": "LONG",
           "pre_entry_price": entry, "ind": {"atr_1h": atr}}
    low, high, ep = sw.compute_entry_range(sig, mark_price=mark)
    e = sw.WatchEntry(signal=sig, entry_low=low, entry_high=high, entry_price=ep,
                      expire_ts=time.time() + 600, on_trigger=_noop)
    assert not e.price_in_range(mark) and e.price_in_range(entry)


def test_old_half_width_0_5_would_instant_trigger_on_degraded_entry(monkeypatch):
    """回归：半宽 0.5ATR 时 mark 恰在区间下沿（闭区间），会秒触发；这是要避免的。"""
    monkeypatch.setattr(config, "SIGNAL_WATCH_ENTRY_BUFFER_ATR_REVERSAL", 0.5, raising=False)
    sig = {"symbol": "XUSDT", "mode": "reversal", "direction": "SHORT",
           "pre_entry_price": 1005.0, "ind": {"atr_1h": 10.0}}
    low, high, ep = sw.compute_entry_range(sig, mark_price=1000.0)
    e = sw.WatchEntry(signal=sig, entry_low=low, entry_high=high, entry_price=ep,
                      expire_ts=time.time() + 600, on_trigger=_noop)
    assert e.price_in_range(1000.0)


def test_fallback_when_config_key_missing_matches_config(monkeypatch):
    """config 缺该键时 compute_entry_range 的回退值必须与 config 默认值一致（0.3），且仍 < 0.5 降级偏移。"""
    monkeypatch.delattr(config, "SIGNAL_WATCH_ENTRY_BUFFER_ATR_REVERSAL")
    low, high, _ = sw.compute_entry_range(_sig("SHORT"), mark_price=ENTRY)
    assert low == pytest.approx(ENTRY - 0.3 * ATR) and high == pytest.approx(ENTRY + 0.3 * ATR)
    assert 0.3 == HALF
