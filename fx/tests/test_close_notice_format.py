"""平仓通知可读性：fmt_node_close 新格式、原因映射、TP 状态、持仓时长。"""
import shared.notifier as N


def _fmt(**kw):
    args = dict(close_reason="rr_below_threshold", exit_price=0.006587, hold_minutes=135,
                pnl_usdt=4.5, pnl_pct=6.14, tp1_filled=False, tp2_filled=False,
                symbol="BIGTIMEUSDT", direction="short")
    args.update(kw)
    return N.fmt_node_close(**args)


def test_target_format_lines():
    t = _fmt()
    assert "🟢 BIGTIMEUSDT 空单平仓" in t
    assert "出场价：0.006587" in t
    assert "持仓时长：2小时15分" in t
    assert "盈亏：+4.5U（+6.14%）" in t
    assert "出场原因：盈亏比不足，主动锁利" in t
    assert "止盈情况：TP1未触发 ✗ / TP2未触发 ✗" in t


def test_loss_uses_red_and_negative_sign():
    t = _fmt(pnl_usdt=-3.2, pnl_pct=-4.1, close_reason="sl_hit", direction="LONG")
    assert "🔴 BIGTIMEUSDT 多单平仓" in t
    assert "盈亏：-3.2U（-4.10%）" in t
    assert "出场原因：止损触发" in t


def test_reason_mapping_all_requested_codes():
    expect = {
        "rr_below_threshold": "盈亏比不足，主动锁利", "tp1_hit": "止盈1触发", "tp2_hit": "止盈2触发",
        "sl_hit": "止损触发", "time_limit": "持仓超时，强制离场",
        "health_check": "健康检查不通过，主动离场", "manual": "手动平仓",
        "funding_cost": "资金费率过高，主动离场",
    }
    for code, cn in expect.items():
        assert f"出场原因：{cn}" in _fmt(close_reason=code), code


def test_unknown_reason_shown_raw():
    assert "出场原因：some_new_reason" in _fmt(close_reason="some_new_reason")


def test_legacy_reasons_unchanged():
    assert "出场原因：止损" in _fmt(close_reason="sl")
    assert "出场原因：突破失败，主动离场" in _fmt(close_reason="breakout_fail")


def test_tp_status_filled_unfilled_and_absent():
    assert "止盈情况：TP1已触发 ✅ / TP2未触发 ✗" in _fmt(tp1_filled=True)
    t = _fmt(tp2_exists=False)               # 无 TP2 挂单且未成交：不显示 TP2
    assert "TP1未触发 ✗" in t and "TP2" not in t
    t = _fmt(tp1_exists=False, tp2_exists=False)   # 都无：整行不显示
    assert "止盈情况" not in t
    assert "止盈情况：TP1已触发 ✅" in _fmt(tp1_filled=True, tp2_exists=False)
    # 已成交即使 exists=False 也显示（以成交事实为准）
    assert "TP2已触发 ✅" in _fmt(tp2_filled=True, tp2_exists=False)


def test_hold_time_formats():
    assert N._fmt_hold_cn(45) == "45分钟"
    assert N._fmt_hold_cn(60) == "1小时0分"
    assert N._fmt_hold_cn(135.9) == "2小时15分"
    assert N._fmt_hold_cn(-5) == "0分钟"
    assert N._fmt_hold_cn(None) == "0分钟"


def test_backward_compatible_call_without_symbol():
    t = N.fmt_node_close("sl", 1.0, 30, -1.0, -2.0, False, False)
    assert "平仓完成" in t and "出场原因：止损" in t and "持仓时长：30分钟" in t


def test_ends_with_separator():
    assert _fmt().rstrip().endswith(N._SEP)


def test_pos_arg_fills_symbol_direction_and_tp_presence():
    pos = {"symbol": "ABCUSDT", "direction": "short", "tp1_order_id": 11, "tp2_order_id": None}
    t = N.fmt_node_close("sl_hit", 1.0, 30, -1.0, -2.0, False, False, pos=pos)
    assert "ABCUSDT 空单平仓" in t
    assert "TP1未触发 ✗" in t and "TP2" not in t          # 无 TP2 挂单：不显示


def test_pos_arg_explicit_values_take_priority_and_filled_wins():
    pos = {"symbol": "ABCUSDT", "direction": "short", "tp1_order_id": None, "tp2_order_id": None}
    t = N.fmt_node_close("tp1_hit", 1.0, 30, 1.0, 2.0, True, False, symbol="XYZUSDT", pos=pos)
    assert "XYZUSDT 空单平仓" in t
    assert "止盈情况：TP1已触发 ✅" in t and "TP2" not in t   # 已成交即使挂单号已清空也显示
    t2 = N.fmt_node_close("manual", 1.0, 30, 1.0, 2.0, False, False, pos={"symbol": "ABCUSDT"})
    assert "止盈情况" not in t2                                # pos 里没有任何 TP 挂单


def test_extra_reason_mappings():
    expect = {
        "health_force_close": "健康评分不足，主动离场",
        "mfe_drawback_50pct": "浮盈回撤过大，主动锁利",
        "soft_sl_fallback": "止损保护触发",
        "exit_signal_reversal": "反向信号触发，主动离场",
        "early_stagnation": "早期横盘，主动离场",
        "time_stop_no_progress": "持仓无进展，超时离场",
        "time_stop_loss": "亏损超时，强制离场",
        "time_stop_hard": "持仓达上限，强制离场",
        "breakout_fail": "突破失败，主动离场",
        "tp1_protect_exit": "TP1后回撤保护，主动锁利",
    }
    for code, cn in expect.items():
        assert N._CLOSE_REASON_CN[code] == cn, code
        assert f"出场原因：{cn}" in _fmt(close_reason=code), code
