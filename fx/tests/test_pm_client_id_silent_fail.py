"""确定性 clientId（make_client_id / 保护单 / place_sl client_id）+ 静默失败留痕（_silent_fail）。全部 mock。"""
import asyncio
import logging

import pytest

from shared import binance_api as A
import position_manager as PM


# ── make_client_id ─────────────────────────────────────────────────────────
def test_cid_deterministic_same_bucket(monkeypatch):
    monkeypatch.setattr(A.time, "time", lambda: 7200.0 * 10 + 5)
    a = A.make_client_id("sl", "p1", "BTCUSDT", "LONG", 100.5, 0.01)
    b = A.make_client_id("sl", "p1", "BTCUSDT", "LONG", 100.5, 0.01)
    assert a == b


def test_cid_changes_with_price_qty_role_pos(monkeypatch):
    monkeypatch.setattr(A.time, "time", lambda: 1000.0)
    base = A.make_client_id("sl", "p1", "BTCUSDT", "LONG", 100.5, 0.01)
    assert A.make_client_id("sl", "p1", "BTCUSDT", "LONG", 100.6, 0.01) != base    # 改价
    assert A.make_client_id("sl", "p1", "BTCUSDT", "LONG", 100.5, 0.02) != base    # 改量
    assert A.make_client_id("tp1", "p1", "BTCUSDT", "LONG", 100.5, 0.01) != base   # 换角色
    assert A.make_client_id("sl", "p2", "BTCUSDT", "LONG", 100.5, 0.01) != base    # 换仓位


def test_cid_time_bucket_hourly(monkeypatch):
    t = {"v": 3600.0 * 100 + 10}
    monkeypatch.setattr(A.time, "time", lambda: t["v"])
    a = A.make_client_id("tp1", "p1", "X", 1.0, 2.0)
    t["v"] = 3600.0 * 100 + 3599
    assert A.make_client_id("tp1", "p1", "X", 1.0, 2.0) == a       # 同一小时
    t["v"] = 3600.0 * 101 + 1
    assert A.make_client_id("tp1", "p1", "X", 1.0, 2.0) != a       # 跨小时换新 id


def test_cid_binance_constraints():
    import re
    c = A.make_client_id("tp1", "pid", "BTCUSDT", "LONG", 1e-7, 123456789.123)
    assert len(c) <= 36 and re.fullmatch(r"[A-Za-z0-9_-]+", c)
    assert A.make_client_id("a-b_c!", "x").startswith("fxabc")


# ── place_sl / place_order 使用调用方 id ────────────────────────────────────
@pytest.fixture
def quiet(monkeypatch):
    logging.disable(logging.CRITICAL)
    monkeypatch.setattr(A.time, "sleep", lambda *_: None)
    monkeypatch.setattr(A, "reset_session", lambda *a, **k: None)
    monkeypatch.setattr(A, "is_hedge_mode", lambda: True)
    yield
    logging.disable(logging.NOTSET)


def test_place_sl_uses_caller_client_id(quiet, monkeypatch):
    seen = []
    monkeypatch.setattr(A, "_signed_post", lambda path, p: seen.append(dict(p)) or {"algoId": 9})
    A.place_sl("BTCUSDT", "SELL", "LONG", 100.0, 1.0, client_id="fxslMYID")
    assert seen[0]["clientAlgoId"] == "fxslMYID"


def test_place_order_dup_client_id_returns_live_order(quiet, monkeypatch):
    def post(path, p):
        raise A.BinanceAPIError(-4116, "ClientOrderId is duplicated", 400)

    monkeypatch.setattr(A, "_signed_post", post)
    monkeypatch.setattr(A, "_signed_get", lambda path, p=None: {"orderId": 55, "status": "NEW"})
    r = A.place_order("BTCUSDT", "SELL", "LIMIT", 1.0, price=100.0, newClientOrderId="fxtp1abc", position_side="LONG")
    assert r["orderId"] == 55


def test_place_order_dup_client_id_finished_order_still_raises(quiet, monkeypatch):
    def post(path, p):
        raise A.BinanceAPIError(-4116, "dup", 400)

    monkeypatch.setattr(A, "_signed_post", post)
    monkeypatch.setattr(A, "_signed_get", lambda path, p=None: {"orderId": 55, "status": "FILLED"})
    with pytest.raises(A.BinanceAPIError):
        A.place_order("BTCUSDT", "SELL", "LIMIT", 1.0, price=100.0, newClientOrderId="fxtp1abc", position_side="LONG")


# ── 保护单（executor._place_guards_async）带确定性 id ─────────────────────────
def test_place_guards_pass_deterministic_ids(monkeypatch):
    import executor as E
    calls = {"sl": [], "orders": []}

    def fake_sl(**kw):
        calls["sl"].append(kw)
        return {"algoId": 1, "orderId": 1, "algoStatus": "NEW"}

    def fake_order(**kw):
        calls["orders"].append(kw)
        return {"orderId": 100 + len(calls["orders"])}

    monkeypatch.setattr(E.api, "place_sl", fake_sl)
    monkeypatch.setattr(E.api, "place_order", fake_order)
    prec = {"step": 0.001, "tick": 0.1}

    def run(sl):
        calls["sl"].clear(); calls["orders"].clear()
        asyncio.run(E._place_guards_async("BTCUSDT", "LONG", 1.0, sl, 110.0, 120.0, prec, guard_key="pid1"))
        return (calls["sl"][0]["client_id"],
                sorted(o["newClientOrderId"] for o in calls["orders"]))

    a, b = run(90.0), run(90.0)
    assert a == b and len(a[1]) == 2 and len(set(a[1])) == 2   # 同价同量 → 同 id
    assert run(89.0)[0] != a[0]                                  # 改止损价 → 新 id


# ── _silent_fail ───────────────────────────────────────────────────────────
@pytest.fixture
def clean_counts():
    PM._SILENT_FAIL_COUNTS.clear()
    yield
    PM._SILENT_FAIL_COUNTS.clear()


def test_silent_fail_counts_and_warns(clean_counts, caplog):
    with caplog.at_level(logging.DEBUG, logger="fx.position_manager"):
        PM._silent_fail("t1", "BTCUSDT", RuntimeError("boom"))
        PM._silent_fail("t1", "ETHUSDT", RuntimeError("boom2"))
    assert PM._SILENT_FAIL_COUNTS["t1"] == 2
    assert any(r.levelno == logging.WARNING and "t1" in r.getMessage() for r in caplog.records)


def test_silent_fail_cancel_order_gone_is_debug_not_counted(clean_counts, caplog):
    with caplog.at_level(logging.DEBUG, logger="fx.position_manager"):
        PM._silent_fail("c1", "BTCUSDT", A.BinanceAPIError(-2011, "Unknown order sent", 400), cancel=True)
        PM._silent_fail("c1", "BTCUSDT", RuntimeError("code=-2013 Order does not exist"), cancel=True)
    assert "c1" not in PM._SILENT_FAIL_COUNTS
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_silent_fail_cancel_other_error_counted(clean_counts):
    PM._silent_fail("c2", "BTCUSDT", A.BinanceAPIError(-1003, "too many", 429), cancel=True)
    assert PM._SILENT_FAIL_COUNTS["c2"] == 1


def test_silent_fail_non_cancel_gone_error_still_counted(clean_counts):
    PM._silent_fail("n1", "BTCUSDT", A.BinanceAPIError(-2011, "x", 400), cancel=False)
    assert PM._SILENT_FAIL_COUNTS["n1"] == 1


def test_silent_fail_never_raises(clean_counts):
    PM._silent_fail("x", None, None)


def test_report_line_and_reset(clean_counts):
    assert PM._silent_fail_report_line() == ""
    PM._silent_fail("a", "S", RuntimeError("e"))
    PM._silent_fail("a", "S", RuntimeError("e"))
    PM._silent_fail("b", "S", RuntimeError("e"))
    line = PM._silent_fail_report_line()
    assert "a×2" in line and "b×1" in line and line.index("a×2") < line.index("b×1")
    assert PM._silent_fail_report_line() == ""      # 读取后清零


def test_critical_except_pass_replaced():
    """守卫：11 处关键位置不再是裸 except-pass（改回去会被发现）。"""
    import ast, inspect
    t = ast.parse(inspect.getsource(PM))
    tags = {n.args[0].value for n in ast.walk(t)
            if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "_silent_fail"
            and n.args and isinstance(n.args[0], ast.Constant)}
    assert {"record_pnl_manual_close", "tp2_restore_old_failed", "trail_cancel_old_sl", "filled_cancel_sl",
            "cleanup_cancel_algo", "cleanup_algo_query", "tp_filled_cancel_old_sl", "guard_cancel_old_sl",
            "replace_tp1_cancel_old", "replace_tp2_cancel_old", "tighten_tp2_cancel_old"} <= tags
