import asyncio
import config
import position_manager as pm


def _pos(**kw):
    p = {"pos_id": "p1", "symbol": "XUSDT", "direction": "short", "mode": "reversal",
         "entry_price": 100.0, "orig_entry": 100.0, "orig_tp_dist": 10.0, "tp1_price": 90.0}
    p.update(kw)
    return p


def _hit(mfe_pct, fav_pct, **kw):
    r = pm._mfe_drawback_progress_hit(_pos(**kw), mfe_pct / 100, fav_pct / 100, 100.0)
    return r[0] if r else r


def test_triggers_when_all_conditions_met():
    # 峰值 7%=进度70%,当前 2%=进度20%,回撤71%
    assert _hit(7, 2) is True


def test_small_peak_not_triggered_even_if_drawdown_big():
    assert _hit(2, 0.5) is False        # 峰值进度20%<60%:旧逻辑会误平的场景


def test_current_progress_must_be_at_most_half_of_peak():
    assert _hit(7, 3.5) is True         # 峰值70%、当前35%:恰为一半,触发
    assert _hit(7, 3.6) is False        # 当前36%>35%,未回撤到一半
    assert _hit(10, 5.0) is True and _hit(10, 5.1) is False   # 峰值100%:当前50%为界(旧版需<30%)
    assert _hit(8, 3.0) is True         # 峰值80%、当前30%,回撤62.5%


def test_zbtusdt_numbers_follow_formula():
    # ZBTUSDT:峰值进度72%、平仓时进度33%。门槛=72%×0.5=36%,33%<=36% → 按公式【会触发】
    # (回撤54%≥50%)。若要该笔不触发,需把回撤要求提高到>54%,而不是改这条公式。
    r = pm._mfe_drawback_progress_hit(_pos(), 0.072, 0.033, 100.0)
    assert abs(r[1] - 0.72) < 1e-9 and abs(r[2] - 0.33) < 1e-9
    assert r[0] is True and abs(r[3] - (0.72 - 0.33) / 0.72) < 1e-9
    # 当前进度37%(>36%)才不触发
    assert pm._mfe_drawback_progress_hit(_pos(), 0.072, 0.037, 100.0)[0] is False


def test_drawdown_below_50_not_triggered():
    assert _hit(6, 3.1) is False        # 回撤48%


def test_not_profitable_not_triggered():
    assert _hit(7, 0.0) is False and _hit(7, -1.0) is False


def test_peak_progress_boundary_60():
    assert _hit(6, 1) is True           # 峰值进度恰为60%
    assert _hit(5.9, 1) is False


def test_no_orig_tp_dist_returns_none_for_fallback():
    p = _pos(); del p["orig_tp_dist"]
    assert pm._mfe_drawback_progress_hit(p, 0.07, 0.02, 100.0) is None
    assert pm._mfe_drawback_progress_hit(_pos(orig_tp_dist=0), 0.07, 0.02, 100.0) is None


def test_config_switch_default_true():
    assert config.MFE_DRAWBACK_USE_PROGRESS is True


def _run_update(monkeypatch, pos, mfe, price, use_progress):
    closed = []
    async def fake_close(p, reason): closed.append(reason)
    monkeypatch.setattr(pm, "_force_close_static", fake_close)
    monkeypatch.setattr(pm, "update_position", lambda pid, u: True)
    monkeypatch.setattr(config, "MFE_DRAWBACK_USE_PROGRESS", use_progress)
    pm._mfe.clear(); pm._mae.clear(); pm._peak.clear(); pm._phase_state.clear()
    pm._mfe[pos["pos_id"]] = mfe
    m = pm.PositionManager.__new__(pm.PositionManager)
    async def go():
        try:
            await m._on_price_update(pos, price)
        except Exception:
            pass   # 后续无关逻辑依赖环境,只关心是否发起了平仓
    asyncio.run(go())
    return closed


def test_on_price_update_new_logic_ignores_small_peak(monkeypatch):
    # 峰值2%(进度20%)、当前0.5%:旧逻辑回撤75%但浮盈被绝对门槛挡住;新逻辑峰值进度不足,同样不平
    pos = _pos()
    assert _run_update(monkeypatch, pos, 0.02, 99.5, True) == []
    # 开关关闭:旧逻辑下浮盈0.5%<TP1距离10%的30%(3%),被绝对浮盈门槛挡住,同样不平仓
    assert _run_update(monkeypatch, _pos(), 0.02, 99.5, False) == []


def test_on_price_update_new_logic_closes_on_deep_drawdown(monkeypatch):
    assert _run_update(monkeypatch, _pos(), 0.07, 98.0, True) == ["mfe_drawback_50pct"]


def test_switch_off_restores_old_logic(monkeypatch):
    # 构造新旧逻辑结论不同的场景:TP1 价格很近(旧逻辑绝对浮盈门槛仅0.9%),峰值5%仅占 orig_tp_dist 的50%
    p_off = _pos(tp1_price=97.0)
    assert _run_update(monkeypatch, p_off, 0.05, 98.5, False) == ["mfe_drawback_50pct"]   # 旧:回撤70%、浮盈1.5%≥0.9%
    assert _run_update(monkeypatch, _pos(tp1_price=97.0), 0.05, 98.5, True) == []         # 新:峰值进度50%<60%


def test_old_position_without_orig_dist_falls_back_to_old_logic(monkeypatch):
    p = _pos(); del p["orig_tp_dist"]
    assert _run_update(monkeypatch, p, 0.07, 96.5, True) == ["mfe_drawback_50pct"]
