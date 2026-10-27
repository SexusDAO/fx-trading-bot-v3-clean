"""missing_guards 含 tp2（本地期望 TP2、交易所只剩 1 个 TP）时强制补挂 TP2，不因「已有 1 个 TP」跳过。全部 mock。"""
from test_orphan_sl_heal import env, algo, lim, orphan_pos, run_guard  # noqa: F401  复用夹具


def _patch_place(monkeypatch, store, fail=False, oid=777):
    from shared import binance_api as B

    def place(**kw):
        store.append(kw)
        if fail:
            raise RuntimeError("place fail")
        return {"orderId": oid}

    monkeypatch.setattr(B, "place_order", place)


def _pos(**kw):
    base = dict(sl_price=58.57, sl_order_id=111, tp1_price=53.23, tp2_price=50.0, tp2_quantity=1.0)
    base.update(kw)
    return orphan_pos(**base)


def test_missing_tp2_is_replaced_and_recorded(env, monkeypatch):
    placed = []
    _patch_place(monkeypatch, placed)
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(7, "53.23", qty="1.0")]            # 只剩 TP1
    run_guard(_pos())
    (kw,) = placed
    assert kw["price"] == 50.0 and kw["quantity"] == 1.0
    assert kw["side"] == "BUY" and kw["positionSide"] == "SHORT" and kw["order_type"] == "LIMIT"
    u = env["updates"][-1]
    assert u["tp1_order_id"] == 7 and u["tp2_order_id"] == 777
    assert u["status"] == "open" and u["missing_guards"] == []


def test_replace_failure_keeps_pending_guard_for_patrol_retry(env, monkeypatch):
    placed = []
    _patch_place(monkeypatch, placed, fail=True)
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(7, "53.23", qty="1.0")]
    run_guard(_pos(tp2_order_id=99))
    assert len(placed) == 1
    u = env["updates"][-1]
    assert u["status"] == "pending_guard" and "tp2" in u["missing_guards"]
    assert "tp2_order_id" not in u                                # 不覆盖本地 id


def test_qty_capped_by_position_minus_tp1(env, monkeypatch):
    placed = []
    _patch_place(monkeypatch, placed)
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(7, "53.23", qty="1.5")]            # 持仓 2.0，TP1 占 1.5 → TP2 最多 0.5
    run_guard(_pos(tp2_quantity=1.0))
    assert placed[0]["quantity"] == 0.5


def test_both_tp_present_does_not_place(env, monkeypatch):
    placed = []
    _patch_place(monkeypatch, placed)
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(7, "53.23"), lim(8, "50.0")]
    run_guard(_pos())
    assert placed == [] and env["updates"][-1]["missing_guards"] == []


def test_single_tp_mode_never_places_tp2(env, monkeypatch):
    placed = []
    _patch_place(monkeypatch, placed)
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(7, "53.23")]
    run_guard(orphan_pos(sl_price=58.57, sl_order_id=111, tp1_price=53.23, tp_mode="single"))
    assert placed == [] and env["updates"][-1]["status"] == "open"


def test_tp1_filled_only_tp2_left_does_not_place(env, monkeypatch):
    placed = []
    _patch_place(monkeypatch, placed)
    env["algo"] = [algo(aid=111)]
    env["open_orders"] = [lim(8, "50.0")]
    run_guard(_pos(tp1_filled=True))
    assert placed == [] and env["updates"][-1]["tp2_order_id"] == 8
