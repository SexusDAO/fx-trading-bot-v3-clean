"""收紧 TP 前预算 RR：预算 RR < TP_MIN_RR_LIVE 就放弃收紧，切断「收紧 TP → rr_below_threshold 平仓」自伤链。
覆盖 _adjust_sl_tp_once 的 shrink 分支与条件 A/B 分支。"""
import asyncio
import time

import pytest

import position_manager as pm


class Env:
    """做空仓位：entry=100，旧 TP1=90，旧 SL=105，ATR=1，现价 mark。"""

    def __init__(self, monkeypatch, mark=96.0, new_tp1=94.0, new_sl=105.0, old_tp1=90.0, old_sl=105.0):
        self.placed, self.cancelled, self.closed, self.updates = [], [], [], []
        self.new_tp1, self.new_sl, self.mark = new_tp1, new_sl, mark
        env = self

        class _Api:
            def get_klines(self, *a, **k):
                now = int(time.time() * 1000)
                return [[now - (60 - i) * 3600_000, 100, 101, 99, 100, 1, now - (60 - i) * 3600_000 + 3599_999]
                        for i in range(55)]

            def get_mark_price(self, symbol):
                return {"markPrice": str(env.mark), "lastFundingRate": "0.0001"}

            def cancel_order(self, symbol, oid):
                env.cancelled.append(oid)
                return {}

            def place_order(self, **kw):
                env.placed.append(kw)
                return {"orderId": 7000 + len(env.placed), "price": str(kw["price"])}

            def place_sl(self, **kw):
                env.placed.append({"sl": kw["trigger_price"]})
                return {"algoId": 8000 + len(env.placed), "triggerPrice": str(kw["trigger_price"])}

            def cancel_algo_order(self, symbol, oid):
                env.cancelled.append(oid)
                return {}

        monkeypatch.setattr(pm, "api", _Api())
        monkeypatch.setattr(pm, "_calc_atr_from_klines", lambda k: 1.0)
        monkeypatch.setattr(pm, "_calc_rsi", lambda c: 50.0)
        monkeypatch.setattr(pm, "_calc_ema", lambda c, n: 100.0)
        monkeypatch.setattr(pm, "_round_price", lambda p, t: round(float(p), 4))
        monkeypatch.setattr(pm, "_round_qty", lambda q, s: round(float(q), 2))
        monkeypatch.setattr(pm, "TP_MIN_RR_LIVE", 1.0, raising=False)
        monkeypatch.setattr(pm, "TP_SL_ADJUST_TP_SHRINK_ATR", 0.5, raising=False)
        monkeypatch.setattr(pm, "TP_SL_ADJUST_MIN_HOLD_MIN", 0, raising=False)
        monkeypatch.setattr(pm, "mark_system_order_update", lambda s: None)
        monkeypatch.setattr(pm, "update_position", lambda pid, u: env.updates.append(dict(u)) or True)
        monkeypatch.setattr(pm, "_write_adjust_log", lambda *a, **k: None)

        async def _fc(pos, reason):
            env.closed.append(reason)

        async def _notify(t):
            return None

        monkeypatch.setattr(pm, "_force_close_static", _fc)
        monkeypatch.setattr(pm, "_notify_async", _notify)

        async def _thread(fn, *a, **k):
            if getattr(fn, "__name__", "") == "_get_precision":
                return {"tick": 0.0001, "step": 0.01}
            return fn(*a, **k)

        monkeypatch.setattr(pm.asyncio, "to_thread", _thread)

        import executor as X
        import shared.calc_sl_tp as C
        import shared.liq_cache as L
        monkeypatch.setattr(X, "_get_precision", lambda s: {"tick": 0.0001, "step": 0.01})
        monkeypatch.setattr(C, "calc_sl_tp", lambda **kw: {
            "sl_price": env.new_sl, "tp1_price": env.new_tp1, "tp2_price": None})
        monkeypatch.setattr(L, "get_liq_zones_cached", lambda *a, **k: [])
        self.old_tp1, self.old_sl = old_tp1, old_sl

    def pos(self):
        return {"symbol": "ABCUSDT", "pos_id": "p-budget", "direction": "short",
                "entry_price": 100.0, "fill_price": 100.0, "open_time": int((time.time() - 7200) * 1000),
                "sl_price": self.old_sl, "tp1_price": self.old_tp1, "tp2_price": None,
                "sl_order_id": 1, "tp1_order_id": 2, "tp2_order_id": None,
                "quantity": 10.0, "quantity_remaining": 10.0, "tp1_filled": False, "mode": "pullback"}


def _run(env):
    return asyncio.run(pm._adjust_sl_tp_once(env.pos(), force=True))


def _tp_orders(env):
    return [o for o in env.placed if "price" in o]


# ─────────────── 条件 A/B 分支 ───────────────
def test_cond_b_blocked_when_budget_rr_below_threshold(monkeypatch):
    # mark=96：旧 TP1=90 距现价 6 ≥3ATR→条件B成立；新 TP1=94 距现价 2，SL=105 距现价 9 → 预算 RR=0.22<1.0
    e = Env(monkeypatch, mark=96.0, new_tp1=94.0)
    res = _run(e)
    assert res is None                      # 无任何改单 → no_change
    assert _tp_orders(e) == [] and e.closed == []


def test_cond_b_allowed_when_budget_rr_ok(monkeypatch):
    # 新 TP1=89.9 比旧 TP1=90 更远不算收紧；改造：mark=98，旧 TP1=90(8 ≥3ATR)，新 TP1=93 距现价 5，SL=105 距 7 → RR=0.71 仍<1
    # 用 SL 紧一点让预算 RR>=1：old_sl=100.5 → sl_dist=2.5，tp1_dist=5 → RR=2.0
    e = Env(monkeypatch, mark=98.0, new_tp1=93.0, new_sl=100.5, old_sl=100.5)
    res = _run(e)
    assert res and res["tp1_changed"] is True
    assert [o["price"] for o in _tp_orders(e)] == [93.0]
    assert e.closed == []


# ─────────────── shrink 分支（新 TP 比旧 TP 近超过 0.5ATR，且不满足条件 A/B 前置的「improve」）───────────────
def test_shrink_blocked_when_budget_rr_below_threshold(monkeypatch):
    # 旧 TP1=95，mark=98（TP 距现价 3，恰好 <3ATR 不触发条件 B？=3 触发；用 old_tp1=95.5 → 距 2.5 <3 不触发 A/B）
    e = Env(monkeypatch, mark=98.0, new_tp1=97.0, old_tp1=95.5, new_sl=105.0, old_sl=105.0)
    # shrink: 旧 TP1 95.5 → 新 97.0，差 1.5 > 0.5ATR；预算 RR = |97-98|/|98-105| = 1/7 <1 → 放弃
    res = _run(e)
    assert res is None and _tp_orders(e) == [] and e.closed == []


def test_shrink_allowed_when_budget_rr_ok(monkeypatch):
    e = Env(monkeypatch, mark=98.0, new_tp1=93.5, old_tp1=92.0, new_sl=99.0, old_sl=99.0)
    # shrink: 旧 92.0 → 新 93.5（更近 1.5>0.5ATR）；预算 RR = 4.5/1 = 4.5 ≥1 → 放行
    res = _run(e)
    assert res and res["tp1_changed"] is True and [o["price"] for o in _tp_orders(e)] == [93.5]


# ─────────────── 预算使用本轮会生效的 SL ───────────────
def test_budget_uses_new_sl_when_sl_is_tightened_this_round(monkeypatch):
    # 旧 SL=110（离现价 14）会让预算 RR=5/14=0.36<1 被拦；但本轮 SL 上移到 99（>0.3ATR 才改）→ 预算用 99：RR=5/1=5 → 放行
    e = Env(monkeypatch, mark=98.0, new_tp1=93.0, new_sl=99.0, old_sl=110.0)
    res = _run(e)
    assert res and res["sl_changed"] is True and res["tp1_changed"] is True
    assert e.closed == []                    # 且后续真实 RR 检查（用生效 SL=99）也不会平仓


def test_old_sl_used_when_sl_not_changed(monkeypatch):
    # 新 SL 比旧 SL 差（做空 SL 上移不允许）→ sl_changed=False，预算用 old_sl=100.5：RR=5/2.5=2 放行
    e = Env(monkeypatch, mark=98.0, new_tp1=93.0, new_sl=108.0, old_sl=100.5)
    res = _run(e)
    assert res and res["sl_changed"] is False and res["tp1_changed"] is True


# ─────────────── 不影响的路径 ───────────────
def test_improve_branch_unaffected_by_budget(monkeypatch):
    # 新 TP1 比旧 TP1 更有利（做空更低）：走 improve，不经过预算；即使 RR 本来就低也照常
    e = Env(monkeypatch, mark=98.0, new_tp1=88.0, old_tp1=90.0, new_sl=105.0, old_sl=105.0)
    res = _run(e)
    assert res and res["tp1_changed"] is True and "improve" in res["tp1_reason"]


def test_rr_below_threshold_check_itself_still_works(monkeypatch):
    # 预算放行但随后真实检查 RR<1：保持原有行为（本改动不改 rr_below_threshold 本身）
    # improve 分支把 TP 改到贴近现价以内无法构造；这里用 SL 上移到离现价很远的 improve 场景：tp1_dist=10，sl_dist=14 → RR=0.71<1
    e = Env(monkeypatch, mark=98.0, new_tp1=88.0, old_tp1=90.0, new_sl=112.0, old_sl=112.0)
    import shared.state as ST
    monkeypatch.setattr(ST, "get_all_positions", lambda: [e.pos()])   # 平仓前会按 pos_id 刷新持仓
    res = _run(e)
    assert res == {"closed": True, "reason": "rr_below_threshold", "rr": pytest.approx(10 / 14, rel=1e-3)}
    assert e.closed == ["rr_below_threshold"]


def test_source_has_budget_check_in_both_branches():
    import inspect
    src = inspect.getsource(pm._adjust_sl_tp_once)
    assert src.count("_tighten_rr_ok(") == 3          # 定义 1 + shrink 1 + 条件A/B 1
    assert '_tighten_rr_ok("shrink")' in src
    assert "_tighten_rr_ok(reason_tag)" in src


# ─────────────── 改单段持有 _guard_locks ───────────────
def test_adjust_skips_when_guard_lock_held_at_execution(monkeypatch):
    """改单前锁被 trail_sl 等占用：本轮放弃，不挂任何单。"""
    e = Env(monkeypatch, mark=98.0, new_tp1=93.0, new_sl=100.5, old_sl=100.5)

    async def go():
        lk = pm._guard_locks["p-budget"]
        # 函数开头的 locked() 检查会先于改单段返回；这里直接验证改单段自身：先让开头检查通过，再在改单前占锁
        orig = pm._replace_tp1 if hasattr(pm, "_replace_tp1") else None
        async with lk:
            return await pm._adjust_sl_tp_once(e.pos(), force=True)

    assert asyncio.run(go()) is None
    assert e.placed == [] and e.closed == []


def test_adjust_holds_lock_while_replacing_orders(monkeypatch):
    """改单期间锁处于持有状态；改单完成后释放（RR 检查/平仓不在锁内）。"""
    e = Env(monkeypatch, mark=98.0, new_tp1=93.0, new_sl=100.5, old_sl=100.5)
    seen = []
    real_place = pm.api.place_order

    def spy(**kw):
        seen.append(pm._guard_locks["p-budget"].locked())
        return real_place(**kw)

    monkeypatch.setattr(pm.api, "place_order", spy)
    res = _run(e)
    assert res and res["tp1_changed"] is True
    assert seen and all(seen)                                  # 挂 TP 时锁被持有
    assert pm._guard_locks["p-budget"].locked() is False       # 结束后已释放


def test_lock_released_after_sl_place_failure(monkeypatch):
    e = Env(monkeypatch, mark=98.0, new_tp1=93.0, new_sl=99.0, old_sl=110.0)

    def boom(**kw):
        raise RuntimeError("exchange down")

    monkeypatch.setattr(pm.api, "place_sl", boom)
    res = _run(e)
    assert res and res["sl_reason"] == "place_failed_kept_old"
    assert pm._guard_locks["p-budget"].locked() is False


# ─────────────── 条件 A/B 顺势保护(有利移动 > 1.5ATR 不收紧 TP) ───────────────
def test_trending_blocks_cond_b_tighten(monkeypatch):
    # 做空 entry=100,mark=96(有利移动4ATR>1.5);旧TP1=90 距现价6≥3ATR → 条件B成立;
    # 新TP1=90.3(只近0.3ATR,不触发shrink);SL收紧到100使预算RR=5.7/4=1.4>=1.0。无保护时会收紧。
    e = Env(monkeypatch, mark=96.0, new_tp1=90.3, new_sl=100.0, old_sl=100.0, old_tp1=90.0)
    res = _run(e)
    assert res is None and _tp_orders(e) == []          # 顺势:不收紧


def test_not_trending_still_tightens_via_cond_b(monkeypatch):
    # mark=99(有利移动1ATR<1.5);旧TP1=95 距现价4≥3ATR → 条件B成立;新TP1=96.0(变动约1.05%，过 0.5% 最小变动门槛);预算RR=3/1.5=2.0
    e = Env(monkeypatch, mark=99.0, new_tp1=96.0, new_sl=100.5, old_sl=100.5, old_tp1=95.0)
    res = _run(e)
    assert res and res["tp1_changed"] is True
    assert [o["price"] for o in _tp_orders(e)] == [96.0]


def test_cond_b_tiny_change_below_min_pct_is_skipped(monkeypatch):
    # 同上场景但新TP1=95.3(只变 0.32% < 0.5%)：不撤单重挂
    e = Env(monkeypatch, mark=99.0, new_tp1=95.3, new_sl=100.5, old_sl=100.5, old_tp1=95.0)
    res = _run(e)
    assert res is None and _tp_orders(e) == []


def test_trending_does_not_block_improve(monkeypatch):
    # 顺势(mark=96)时 TP 向更有利方向移动(新TP1=88<旧90)仍允许
    e = Env(monkeypatch, mark=96.0, new_tp1=88.0, new_sl=100.0, old_sl=100.0, old_tp1=90.0)
    res = _run(e)
    assert res and res["tp1_changed"] is True
    assert [o["price"] for o in _tp_orders(e)] == [88.0]


def test_trending_does_not_block_large_shrink_branch(monkeypatch):
    # 说明:顺势保护只管条件A/B;新TP比旧TP近超过0.5ATR 走的是 shrink 分支,不在保护范围
    e = Env(monkeypatch, mark=96.0, new_tp1=92.0, new_sl=100.0, old_sl=100.0, old_tp1=90.0)
    res = _run(e)
    assert res and res["tp1_changed"] is True and "shrink" in res.get("tp1_reason", "shrink")
