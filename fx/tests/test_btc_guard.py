"""btc_guard 状态机测试（不联网）。"""
from shared.btc_guard import BtcGuard, NORMAL, CRASH_WARN, PUMP_WARN


def _feed(g, pts):
    for ts, px in pts:
        g.update_price(px, ts)


def test_crash_warn_blocks_long():
    g = BtcGuard()
    _feed(g, [(1000, 100.0), (1100, 99.5), (1290, 98.9)])
    assert g.get_state() == CRASH_WARN
    assert g.cancel_direction() == "LONG"
    assert not g.is_allowed("LONG")
    assert g.is_allowed("SHORT")


def test_pump_warn_blocks_short():
    g = BtcGuard()
    _feed(g, [(1000, 100.0), (1290, 101.1)])
    assert g.get_state() == PUMP_WARN
    assert g.cancel_direction() == "SHORT"
    assert not g.is_allowed("SHORT")


def test_ms_timestamp_and_recover():
    g = BtcGuard()
    B = 1_700_000_000
    _feed(g, [(B * 1000, 100.0), ((B + 290) * 1000, 98.9)])   # 毫秒
    assert g.get_state() == CRASH_WARN
    g.update_price(98.9, B + 290 + 1801)
    assert g.get_state() == NORMAL


def test_flat_stays_normal():
    g = BtcGuard()
    _feed(g, [(1000 + i * 60, 100.0 + (i % 2) * 0.1) for i in range(30)])
    assert g.get_state() == NORMAL and g.cancel_direction() is None
