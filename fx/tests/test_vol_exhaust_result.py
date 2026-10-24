import asyncio
import logging
import inspect
import position_manager as pm


def _pos(direction="short", price=100.0, ts=1_000_000.0, **kw):
    p = {"pos_id": "p1", "symbol": "XUSDT", "direction": direction,
         "vol_exhaust_signal_price": price, "vol_exhaust_signal_ts": ts}
    p.update(kw)
    return p


def test_short_favorable_positive(caplog):
    with caplog.at_level(logging.INFO):
        m = pm._log_vol_exhaust_result(_pos(), "tp1", 98.0, 1_000_000.0 + 90 * 60000)
    assert "[vol_exhaust_result] XUSDT 平仓原因=tp1" in m
    assert "信号价格=100 平仓价格=98" in m and "价格变化=+2.00%" in m and "持续时间=90分钟" in m
    assert "[vol_exhaust_result]" in caplog.text


def test_short_unfavorable_negative_and_long():
    assert "价格变化=-3.00%" in pm._log_vol_exhaust_result(_pos(), "sl", 103.0, 2_000_000.0)
    assert "价格变化=+5.00%" in pm._log_vol_exhaust_result(_pos("long"), "tp1", 105.0, 2_000_000.0)
    assert "价格变化=-5.00%" in pm._log_vol_exhaust_result(_pos("long"), "sl", 95.0, 2_000_000.0)


def test_no_signal_or_bad_data_returns_none():
    assert pm._log_vol_exhaust_result({"symbol": "X", "direction": "short"}, "tp1", 98.0, 1.0) is None
    assert pm._log_vol_exhaust_result(_pos(price=0.0), "tp1", 98.0, 1.0) is None
    assert pm._log_vol_exhaust_result(_pos(), "tp1", 0.0, 1.0) is None
    assert pm._log_vol_exhaust_result({}, "tp1", 98.0, 1.0) is None


def test_missing_signal_ts_gives_zero_minutes():
    assert "持续时间=0分钟" in pm._log_vol_exhaust_result(_pos(ts=0.0), "tp1", 98.0, 5_000_000.0)


def test_wired_into_close_and_adjust():
    close_src = inspect.getsource(pm._close_position_post_static)
    assert "_log_vol_exhaust_result(pos, close_reason, exit_price, close_ms)" in close_src
    adj = inspect.getsource(pm._adjust_sl_tp_once)
    assert '"vol_exhaust_signal_price"' in adj and '"vol_exhaust_signal_ts"' in adj
    assert "update_position, pid, _vs_upd" in adj


def test_adjust_persists_only_first_signal(monkeypatch):
    """信号落盘逻辑:首次写入 signal_price/ts;已有信号时不覆盖。通过同一段代码路径的等价抽取验证。"""
    calls = []
    monkeypatch.setattr(pm, "update_position", lambda pid, u: calls.append(dict(u)) or True)
    monkeypatch.setattr(pm, "_check_volume_exhaustion_shadow", lambda pos, k, px: {"x": 1})
    src = inspect.getsource(pm._adjust_sl_tp_once)
    seg = src[src.index("    try:\n        _vs = _check_volume_exhaustion_shadow"):src.index("    # ── 重算 TP/SL")]
    import textwrap, time
    code = "async def _t(pos, k1h, mark_price, pid):\n" + seg
    ns = {"pm": pm, "asyncio": asyncio, "time": time, "float": float, "logger": pm.logger,
          "_check_volume_exhaustion_shadow": pm._check_volume_exhaustion_shadow,
          "update_position": pm.update_position}
    exec(code, ns)
    p1 = {"mode": "reversal"}
    asyncio.run(ns["_t"](p1, [], 96.0, "p1"))
    assert p1["vol_exhaust_signal_price"] == 96.0 and calls and calls[0]["vol_exhaust_signal_price"] == 96.0
    n = len(calls)
    asyncio.run(ns["_t"](p1, [], 95.0, "p1"))
    assert p1["vol_exhaust_signal_price"] == 96.0 and len(calls) == n   # 不覆盖首次信号
