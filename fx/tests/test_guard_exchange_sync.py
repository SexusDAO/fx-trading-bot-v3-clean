"""补挂前先查交易所同步 order_id：价格由本地决定、TP 按本地价格最接近匹配、TP 查询失败不盲挂。全部 mock。"""
import pytest

from shared import guard_match as G
from test_orphan_sl_heal import env, algo, lim, orphan_pos, run_guard  # noqa: F401  复用夹具


# ── assign_tps_by_price 纯函数 ──
def test_assign_both_by_closest_price():
    a, b, bp = G.assign_tps_by_price([lim(8, "50.0"), lim(7, "53.2")], 53.23, 50.0, "SHORT")
    assert a["orderId"] == 7 and b["orderId"] == 8 and bp


def test_assign_only_tp2_left_not_mistaken_as_tp1():
    a, b, bp = G.assign_tps_by_price([lim(8, "50.0")], 53.23, 50.0, "SHORT")
    assert a is None and b["orderId"] == 8 and bp


def test_assign_only_tp1_left_when_single_tp_mode():
    a, b, _ = G.assign_tps_by_price([lim(7, "53.2")], 53.23, None, "SHORT")
    assert a["orderId"] == 7 and b is None


def test_assign_each_order_used_once():
    a, b, _ = G.assign_tps_by_price([lim(7, "53.2")], 53.23, 53.0, "SHORT")
    assert (a is None) != (b is None)                       # 一张单只能占一个槽位


def test_assign_no_local_prices_falls_back_to_sorted():
    a, b, bp = G.assign_tps_by_price([lim(8, "50.0"), lim(7, "53.2")], None, 0, "SHORT")
    assert a["orderId"] == 7 and b["orderId"] == 8 and not bp
    a, b, _ = G.assign_tps_by_price([lim(1, "10"), lim(2, "12")], None, None, "LONG")
    assert a["orderId"] == 1 and b["orderId"] == 2


# ── SL：保留本地价，只同步 id ──
def test_heal_keeps_local_sl_price_when_positive(env):
    env["algo"] = [algo(trig="57.00", aid=111)]            # 交易所价与本地不同
    env["open_orders"] = [lim(7, "53.23")]
    run_guard(orphan_pos(sl_price=58.0, tp1_price=53.23))
    u = env["updates"][-1]
    assert u["sl_order_id"] == 111 and "sl_price" not in u  # 不写交易所价
    assert u["status"] == "open" and env["sl_placed"] == [] and env["guard_calls"] == []


def test_heal_uses_exchange_price_only_when_local_zero_or_empty(env):
    env["algo"] = [algo(trig="57.00", aid=111)]
    env["open_orders"] = [lim(7, "53.23")]
    run_guard(orphan_pos(sl_price=0))
    u = env["updates"][-1]
    assert u["sl_price"] == 57.0 and u["sl_order_id"] == 111
    env["updates"].clear()
    run_guard(orphan_pos())                                  # 无 sl_price 字段
    assert env["updates"][-1]["sl_price"] == 57.0


def test_local_sl_price_kept_on_atr_degrade_path(env):
    env["algo"] = [algo(trig="57.00", aid=111)]
    run_guard(orphan_pos(sl_price=58.0))                      # 无 tp1 → 走 ATR 降级
    (a, kw), = env["guard_calls"]
    assert a[3] == 58.0 and kw["skip_sl"] is True            # 传给挂单函数的是本地价
    assert env["updates"][-1]["sl_price"] == 58.0 and env["updates"][-1]["sl_order_id"] == 111


# ── TP：按本地价格匹配 ──
def test_only_tp2_remains_after_tp1_filled_keeps_slots(env):
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(8, "50.0")]
    run_guard(orphan_pos(sl_price=58.57, sl_order_id=111, tp1_price=53.23, tp2_price=50.0, tp1_filled=True))
    u = env["updates"][-1]
    assert u["tp2_order_id"] == 8 and u["tp1_order_id"] is None
    assert u["status"] == "open" and u["missing_guards"] == []


def test_both_tp_matched_by_price_even_if_exchange_order_is_swapped(env):
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(8, "50.0"), lim(7, "53.23")]
    run_guard(orphan_pos(sl_price=58.57, sl_order_id=111, tp1_price=53.23, tp2_price=50.0))
    u = env["updates"][-1]
    assert u["tp1_order_id"] == 7 and u["tp2_order_id"] == 8 and u["missing_guards"] == []


def test_expected_tp_not_found_keeps_local_id_and_reports_missing(env):
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(7, "53.23")]                    # 本地有 tp2 期望，交易所只有 tp1
    run_guard(orphan_pos(sl_price=58.57, sl_order_id=111, tp1_price=53.23, tp2_price=50.0, tp2_order_id=99))
    u = env["updates"][-1]
    assert u["tp1_order_id"] == 7 and "tp2_order_id" not in u   # 不覆盖本地 tp2 id
    assert "tp2" in u["missing_guards"] and env["guard_calls"] == []


# ── TP 查询失败：不盲挂 ──
def test_open_orders_query_failure_does_not_place_tp(env, monkeypatch):
    from shared import binance_api as B

    def boom(sym=None):
        raise RuntimeError("net")

    monkeypatch.setattr(B, "get_open_orders", boom)
    env["algo"] = [algo(aid=111)]
    run_guard(orphan_pos(sl_price=58.57, sl_order_id=111, tp1_price=53.23))
    assert env["guard_calls"] == []                             # 没走挂单
    u = env["updates"][-1]
    assert u["status"] == "pending_guard" and "tp_query_failed" in u["missing_guards"]


def test_open_orders_query_failure_still_repairs_missing_sl(env, monkeypatch):
    from shared import binance_api as B

    def boom(sym=None):
        raise RuntimeError("net")

    monkeypatch.setattr(B, "get_open_orders", boom)
    run_guard(orphan_pos(sl_price=58.0, tp1_price=53.0))        # 交易所无 SL
    assert len(env["sl_placed"]) == 1 and env["guard_calls"] == []
    u = env["updates"][-1]
    assert u["status"] == "pending_guard" and u["missing_guards"] == ["tp_query_failed"]
