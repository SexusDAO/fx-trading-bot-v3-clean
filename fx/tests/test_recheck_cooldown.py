import inspect
import shared.signal_watcher as sw


def _entry():
    return sw.WatchEntry(signal={"symbol": "X", "mode": "reversal"}, entry_low=1, entry_high=2,
                         entry_price=1.5, expire_ts=9e18, on_trigger=None)


def test_fields_default():
    e = _entry()
    assert e.last_recheck_skip_ts == 0.0 and e.last_recheck_skip_mark == 0.0


def test_never_skipped_not_cooling():
    assert sw._recheck_cooling(_entry(), 100.0, 1.0, now=1000.0) is False


def test_cooling_within_5min_and_small_shift():
    e = _entry(); sw._mark_recheck_skip(e, 100.0, now=1000.0)
    assert sw._recheck_cooling(e, 100.5, 1.0, now=1100.0) is True
    assert sw._recheck_cooling(e, 99.1, 1.0, now=1299.0) is True


def test_cooldown_expires_after_5min():
    e = _entry(); sw._mark_recheck_skip(e, 100.0, now=1000.0)
    assert sw._recheck_cooling(e, 100.0, 1.0, now=1300.0) is False


def test_mark_shift_ge_1atr_resets():
    e = _entry(); sw._mark_recheck_skip(e, 100.0, now=1000.0)
    assert sw._recheck_cooling(e, 101.0, 1.0, now=1010.0) is False
    assert sw._recheck_cooling(e, 98.5, 1.0, now=1010.0) is False


def test_atr_nonpositive_not_cooling():
    e = _entry(); sw._mark_recheck_skip(e, 100.0, now=1000.0)
    assert sw._recheck_cooling(e, 100.0, 0.0, now=1010.0) is False


def test_b1_wired_and_b2_untouched():
    src = inspect.getsource(sw.SignalWatcher._recheck_one)
    assert "_recheck_cooling(entry, mark_mp, atr_v)" in src and "_mark_recheck_skip(entry, mark_mp)" in src
    b2 = src[src.index("B. 清算区位移检查"):]
    assert "_recheck_cooling" not in b2
