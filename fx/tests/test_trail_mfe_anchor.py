"""Trail SL MFE 锚点（开关 TRAIL_MFE_ANCHOR）+ 维护任务在锚点模式下不动 SL。"""
import asyncio
import time

import config
import position_manager as pm


def _pos(direction="long", sl=95.0, pid="p-mfe-a", **kw):
    p = {"pos_id": pid, "symbol": "ABCUSDT", "direction": direction, "entry_price": 100.0,
         "atr_at_entry": 1.0, "sl_price": sl, "mode": "pullback", "tp1_filled": False,
         "rr_protection_triggered": True}
    p.update(kw)
    return p


def _on(monkeypatch, on=True):
    monkeypatch.setattr(config, "TRAIL_MFE_ANCHOR", on, raising=False)
    monkeypatch.setattr(config, "TRAIL_MFE_ATR_MULT", 2.5, raising=False)
    monkeypatch.setattr(config, "TRAIL_MFE_HARD_MULT", 3.5, raising=False)


def test_config_defaults():
    assert config.TRAIL_MFE_ANCHOR is False
    assert config.TRAIL_MFE_ATR_MULT == 2.5 and config.TRAIL_MFE_HARD_MULT == 3.5


def test_switch_off_uses_old_ladder(monkeypatch):
    _on(monkeypatch, False)
    pm._peak["p-mfe-a"] = 110.0
    assert abs(pm._calc_trail_sl(_pos(), 103.0) - 101.0) < 1e-9     # 原阶梯：2.8ATR → +1.0ATR


def test_long_hard_sl_follows_peak_minus_hard_mult(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 110.0
    assert abs(pm._calc_trail_sl(_pos(), 109.0) - 106.5) < 1e-9     # 110 - 3.5


def test_short_hard_sl_follows_peak_plus_hard_mult(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 90.0
    assert abs(pm._calc_trail_sl(_pos("short", sl=105.0), 91.0) - 93.5) < 1e-9


def test_never_wider_than_old_sl(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 100.5                    # 峰值-3.5=97 < 旧止损 99
    assert pm._calc_trail_sl(_pos(sl=99.0), 100.0) is None
    pm._peak["p-mfe-a"] = 99.5
    assert pm._calc_trail_sl(_pos("short", sl=101.0), 100.0) is None


def test_only_moves_favorably(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 110.0
    assert pm._calc_trail_sl(_pos(sl=107.0), 109.0) is None          # 新 106.5 比旧 107 低 → 不动


def test_sl_not_set_beyond_current_price(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 110.0
    assert pm._calc_trail_sl(_pos(), 106.0) is None                  # 新 106.5 ≥ 现价 → 交给软止损


def test_no_peak_returns_none(monkeypatch):
    _on(monkeypatch)
    pm._peak.pop("p-mfe-a", None)
    assert pm._calc_trail_sl(_pos(), 103.0) is None


def test_soft_stop_uses_1h_close_only(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 110.0
    pm._last_1h_close.pop("p-mfe-a", None)
    assert pm._mfe_soft_stop_hit(_pos()) is False                      # 无收盘价记录(如刚重启) → 不触发
    pm._last_1h_close["p-mfe-a"] = (107.6, 0, 0)
    assert pm._mfe_soft_stop_hit(_pos()) is False                      # 110-2.5=107.5，收盘 107.6 未触及
    pm._last_1h_close["p-mfe-a"] = (107.5, 0, 0)
    assert pm._mfe_soft_stop_hit(_pos()) is True
    pm._peak["p-mfe-a"] = 90.0
    pm._last_1h_close["p-mfe-a"] = (92.5, 0, 0)
    assert pm._mfe_soft_stop_hit(_pos("short", sl=105.0)) is True
    pm._last_1h_close["p-mfe-a"] = (92.4, 0, 0)
    assert pm._mfe_soft_stop_hit(_pos("short", sl=105.0)) is False


def test_soft_stop_off_when_switch_off(monkeypatch):
    _on(monkeypatch, False)
    pm._peak["p-mfe-a"] = 110.0
    pm._last_1h_close["p-mfe-a"] = (100.0, 0, 0)
    assert pm._mfe_soft_stop_hit(_pos()) is False


def test_refresh_1h_close_memory_only(monkeypatch):
    now = int(time.time() * 1000)
    ks = [[now - 7200_000, 0, 0, 0, 101.0, 0, now - 3600_000 - 1],
          [now - 3600_000, 0, 0, 0, 102.0, 0, now - 1],
          [now - 100, 0, 0, 0, 999.0, 0, now + 3_599_000]]          # 未收盘，必须忽略
    upd, calls = [], []
    monkeypatch.setattr(pm, "_pm_klines", lambda s, i, n: calls.append(1) or ks)
    monkeypatch.setattr(pm, "update_position", lambda pid, u: upd.append(dict(u)) or True)
    pm._last_1h_close.pop("p-mfe-a", None)
    p = _pos()
    asyncio.run(pm._mfe_refresh_1h_close(p, "p-mfe-a"))
    assert pm._last_1h_close["p-mfe-a"][0] == 102.0
    assert upd == [] and "last_1h_close" not in p                     # 不持久化
    asyncio.run(pm._mfe_refresh_1h_close(p, "p-mfe-a"))
    assert len(calls) == 1                                            # 同一小时只请求一次


def test_notifier_has_trail_mfe_soft_mapping():
    import inspect
    from shared import notifier
    assert "trail_mfe_soft" in inspect.getsource(notifier)


def test_source_hooks_present():
    import inspect
    src = inspect.getsource(pm._adjust_sl_tp_once)
    assert "TRAIL_MFE_ANCHOR as _mfe_anchor" in src and "sl_changed = False   # SL交给实时trail管理" in src


# ─────────────── 修改1：硬止损最小移动 0.3ATR ───────────────
def test_hard_sl_min_move_long(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 110.0                                    # 目标 106.5
    assert pm._calc_trail_sl(_pos(sl=106.3), 109.0) is None        # 只移 0.2ATR → 不动
    assert abs(pm._calc_trail_sl(_pos(sl=106.1), 109.0) - 106.5) < 1e-9   # 移 0.4ATR → 动


def test_hard_sl_min_move_short(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 90.0                                     # 目标 93.5
    assert pm._calc_trail_sl(_pos("short", sl=93.7), 91.0) is None
    assert abs(pm._calc_trail_sl(_pos("short", sl=93.9), 91.0) - 93.5) < 1e-9


# ─────────────── 修改2：软止损需有盈利 ───────────────
def test_soft_stop_requires_profit_long(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 102.0                                    # 软线 99.5
    pm._last_1h_close["p-mfe-a"] = (99.0, 0, 0)                    # 收盘 99 < 入场 100：没盈利
    assert pm._mfe_soft_stop_hit(_pos()) is False
    pm._peak["p-mfe-a"] = 110.0
    pm._last_1h_close["p-mfe-a"] = (107.0, 0, 0)                   # 盈利且穿越
    assert pm._mfe_soft_stop_hit(_pos()) is True


def test_soft_stop_requires_profit_short(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 98.0                                     # 软线 100.5
    pm._last_1h_close["p-mfe-a"] = (101.0, 0, 0)                   # 收盘 101 > 入场 100：没盈利
    assert pm._mfe_soft_stop_hit(_pos("short", sl=105.0)) is False
    pm._peak["p-mfe-a"] = 90.0
    pm._last_1h_close["p-mfe-a"] = (93.0, 0, 0)
    assert pm._mfe_soft_stop_hit(_pos("short", sl=105.0)) is True


def test_soft_stop_uses_fill_price_and_bad_entry(monkeypatch):
    _on(monkeypatch)
    pm._peak["p-mfe-a"] = 110.0
    pm._last_1h_close["p-mfe-a"] = (107.0, 0, 0)
    assert pm._mfe_soft_stop_hit(_pos(fill_price=108.0)) is False  # 以成交价 108 为准：收盘 107 亏损
    assert pm._mfe_soft_stop_hit(_pos(entry_price=0.0)) is False   # 入场价无效
