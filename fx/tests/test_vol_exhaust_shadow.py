import logging
import position_manager as pm

H = 3_600_000


def _klines(vols, t0=0):
    return [[t0 + i * H, 1, 1, 1, 1, v, t0 + (i + 1) * H - 1] for i, v in enumerate(vols)]


def _vols(n_after_ratios, climax=3.0, n_before=24):
    """前面 n_before 根基础量 100,MFE 那根为 climax×100,之后按给定倍数序列。"""
    return [100.0] * n_before + [100.0 * climax] + [100.0 * r for r in n_after_ratios]


def _pos(peak_idx, **kw):
    p = {"pos_id": "p1", "symbol": "XUSDT", "mode": "reversal", "direction": "short",
         "orig_entry": 100.0, "orig_tp_dist": 10.0, "peak_price": 94.0, "peak_ts": float(peak_idx * H + 1000),
         "phase": 3}
    p.update(kw)
    return p


def _setup():
    pm._phase_state.clear(); pm._vol_shadow_logged.clear()


def test_triggers_and_logs_once(caplog):
    _setup()
    k = _klines(_vols([1.0, 0.3, 0.1]))          # MFE 在 idx=24;之后 3 根
    with caplog.at_level(logging.INFO):
        r = pm._check_volume_exhaustion_shadow(_pos(24), k, 96.0)
        r2 = pm._check_volume_exhaustion_shadow(_pos(24), k, 96.0)   # 同一根 K 线不重复
    assert r and r["recent_vol_ratios"][0] < 0.5 and r["mfe_vol_ratio"] >= 2.0
    assert r2 is None
    assert caplog.text.count("[vol_exhaust_shadow]") == 1 and "量能耗尽信号" in caplog.text


def test_not_reversal_or_wrong_phase_or_loss_or_low_progress():
    k = _klines(_vols([1.0, 0.3, 0.1]))
    for kw, price in [({"mode": "pullback"}, 96.0), ({"phase": 2}, 96.0), ({"phase": 4}, 96.0),
                      ({}, 101.0), ({"peak_price": 96.0}, 97.0)]:
        _setup()
        assert pm._check_volume_exhaustion_shadow(_pos(24, **kw), k, price) is None, kw


def test_no_climax_or_no_shrink_or_too_few_after():
    _setup()
    assert pm._check_volume_exhaustion_shadow(_pos(24), _klines(_vols([1.0, 0.3, 0.1], climax=1.5)), 96.0) is None
    _setup()
    assert pm._check_volume_exhaustion_shadow(_pos(24), _klines(_vols([1.0, 0.3, 0.8])), 96.0) is None   # 最近一根 0.8>0.5
    _setup()
    assert pm._check_volume_exhaustion_shadow(_pos(24), _klines(_vols([0.2])), 96.0) is None              # MFE 后只有 1 根


def test_mfe_in_unclosed_candle_skipped():
    _setup()
    k = _klines(_vols([1.0, 0.3, 0.1]))
    p = _pos(24, peak_ts=float(len(k) * H + 5))   # peak 在最后一根已收盘 K 线之后
    assert pm._check_volume_exhaustion_shadow(p, k, 96.0) is None


def test_prefers_live_phase_state_and_never_raises():
    _setup()
    k = _klines(_vols([1.0, 0.3, 0.1]))
    pm._phase_state["p1"] = {"phase": 2, "peak": 94.0, "peak_ts": 24 * H + 1000.0}
    assert pm._check_volume_exhaustion_shadow(_pos(24), k, 96.0) is None   # 内存阶段=2 优先于快照的 3
    assert pm._check_volume_exhaustion_shadow({}, k, 96.0) is None
    assert pm._check_volume_exhaustion_shadow(_pos(24), [], 96.0) is None


def test_wired_into_adjust_and_cleanup():
    import inspect
    src = inspect.getsource(pm._adjust_sl_tp_once)
    assert "_check_volume_exhaustion_shadow(pos, k1h, mark_price)" in src
    assert src.index("_check_volume_exhaustion_shadow") < src.index("重算 TP/SL")
    assert "_vol_shadow_logged.pop(pos_id" in inspect.getsource(pm)
