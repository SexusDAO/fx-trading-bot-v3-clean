"""重启对齐：本地已有记录的仓位原样保留（只核对保护单），只有真正无记录才建孤儿档案；
position_review 对 mode=recovered 回溯原始信号。全部 mock，无网络。"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import pytest

import main as M

SYM = "LTCUSDT"


def algo(aid=111, trig="58.57", side="SHORT"):
    return {"symbol": SYM, "side": "BUY" if side == "SHORT" else "SELL", "positionSide": side,
            "orderType": "STOP_MARKET", "triggerPrice": trig, "algoId": aid}


def lim(oid, price, side="SHORT"):
    return {"symbol": SYM, "side": "BUY" if side == "SHORT" else "SELL", "positionSide": side,
            "type": "LIMIT", "price": price, "orderId": oid, "origQty": "1.0"}


RP = {"symbol": SYM, "positionSide": "SHORT", "positionAmt": "-2.0", "entryPrice": "56.79",
      "unRealizedProfit": "0.5", "leverage": "5"}

LOCAL = {"pos_id": "ab12cd34", "symbol": SYM, "direction": "short", "mode": "ema_trend", "sig_type": "ema_trend",
         "status": "open", "entry_price": 56.5, "quantity": 2.0, "leverage": 5, "open_time": 1700000000000,
         "sl_price": 58.57, "sl_order_id": 111, "tp1_price": 53.23, "tp1_order_id": 7,
         "tp2_price": 50.0, "tp2_order_id": 8, "judge_detail": {"rsi_1h": 61.2, "ema20_4h": 1.0},
         "signal_strength": 77}


@pytest.fixture
def env(monkeypatch):
    import position_manager as PM
    import shared.state as S
    from shared import binance_api as B
    logging.disable(logging.CRITICAL)
    st = {"added": [], "guard": [], "algo": [], "orders": [], "stored": [], "updates": [], "pos": [dict(RP)]}
    monkeypatch.setattr(B, "get_position", lambda sym=None: [dict(p) for p in st["pos"]])
    monkeypatch.setattr(B, "get_open_orders", lambda sym=None: list(st["orders"]))
    monkeypatch.setattr(B, "get_algo_open_orders", lambda sym=None: list(st["algo"]))

    def add(rec):
        st["added"].append(dict(rec))
        st["stored"].append(dict(rec))
        return True

    def upd(pid, u):
        st["updates"].append((pid, dict(u)))
        for p in st["stored"]:
            if p.get("pos_id") == pid:
                p.update(u)
        return True

    monkeypatch.setattr(S, "add_position", add)
    monkeypatch.setattr(S, "update_position", upd)
    monkeypatch.setattr(S, "get_all_positions", lambda: [dict(p) for p in st["stored"]])
    monkeypatch.setattr(S, "list_orphan_patching", lambda: [])
    monkeypatch.setattr(S, "cleanup_orphan_patching", lambda: [])

    async def guard(pos, entry, qty, pid, sym):
        st["guard"].append(sym)

    monkeypatch.setattr(PM, "_补挂保护单", guard)
    monkeypatch.setattr(M, "_sync_lite_busy", False, raising=False)
    yield st
    logging.disable(logging.NOTSET)


def run():
    asyncio.run(M._sync_positions_on_startup())


def run_lite():
    asyncio.run(M._startup_sync_lite())


# ── (a) 本地有记录：原样保留 ──────────────────────────────────────────────
def test_local_record_kept_untouched_on_startup(env):
    env["stored"] = [dict(LOCAL)]
    env["algo"] = [algo(111)]
    env["orders"] = [lim(7, "53.23"), lim(8, "50.0")]
    run()
    assert env["added"] == []                       # 没建孤儿
    assert env["guard"] == []                       # 保护单都在，不补挂
    rec = env["stored"][0]
    assert rec["pos_id"] == "ab12cd34" and rec["mode"] == "ema_trend" and rec["sig_type"] == "ema_trend"
    assert rec["judge_detail"] == {"rsi_1h": 61.2, "ema20_4h": 1.0}
    assert rec["entry_price"] == 56.5 and rec["sl_price"] == 58.57       # 不被交易所均价 56.79 覆盖
    assert "source" not in rec


@pytest.mark.parametrize("status", ["pending", "patching", "pending_guard"])
def test_local_record_any_live_status_not_orphaned(env, status):
    rec = dict(LOCAL, status=status)
    env["stored"] = [rec]
    env["algo"] = [algo(111)]
    env["orders"] = [lim(7, "53.23"), lim(8, "50.0")]
    run()
    assert env["added"] == []
    assert env["stored"][0]["pos_id"] == "ab12cd34" and env["stored"][0]["mode"] == "ema_trend"


def test_match_helper_direction_case_and_status():
    ps = [dict(LOCAL, direction="SHORT"), dict(LOCAL, pos_id="x", symbol="OPUSDT", status="closed")]
    assert M._match_local_position(ps, SYM, "SHORT")["pos_id"] == "ab12cd34"
    assert M._match_local_position(ps, SYM, "LONG") is None
    assert M._match_local_position(ps, "OPUSDT", "SHORT") is None


# ── (b) 无本地记录：建孤儿档案 ────────────────────────────────────────────
def test_no_local_record_builds_orphan(env):
    env["algo"] = [algo(111)]
    env["orders"] = [lim(7, "53.23"), lim(8, "50.0")]
    run()
    assert len(env["added"]) == 1
    o = env["added"][0]
    assert o["mode"] == "recovered" and o["source"] == "orphan_recovered"
    assert o["pos_id"].startswith(f"rec_{SYM}_S_") and o["direction"] == "short"


def test_other_direction_local_does_not_hide_orphan(env):
    env["stored"] = [dict(LOCAL, direction="long")]      # 同标的另一个方向，不算匹配
    env["algo"] = [algo(111)]
    run()
    # add_position 对同标的已有记录会拒绝（真实实现）；这里 mock 不拒绝，只验证确实走了孤儿分支
    assert len(env["added"]) == 1 and env["added"][0]["mode"] == "recovered"


# ── (c) SL 缺失 → 自动补挂；SL 在但 id 变了 → 只回写订单号 ────────────────
def test_sl_missing_triggers_reattach_without_touching_record(env):
    env["stored"] = [dict(LOCAL)]
    env["algo"] = []                                      # SL 不在交易所
    env["orders"] = [lim(7, "53.23"), lim(8, "50.0")]
    run()
    assert env["guard"] == [SYM]                          # 委托 _补挂保护单
    assert env["added"] == []
    assert env["stored"][0]["mode"] == "ema_trend" and env["stored"][0]["pos_id"] == "ab12cd34"


def test_sl_exists_with_new_algo_id_only_updates_order_id(env):
    env["stored"] = [dict(LOCAL, sl_order_id=999)]       # 本地 id 过期
    env["algo"] = [algo(222)]
    env["orders"] = [lim(7, "53.23"), lim(8, "50.0")]
    run()
    assert env["guard"] == []
    assert env["updates"] == [("ab12cd34", {"sl_order_id": 222})]     # 只写订单号
    assert env["stored"][0]["sl_price"] == 58.57


def test_tp_claimed_by_price_proximity_only_order_ids(env):
    env["stored"] = [dict(LOCAL, tp1_order_id=1, tp2_order_id=2)]
    env["algo"] = [algo(111)]
    env["orders"] = [lim(70, "53.3"), lim(80, "50.1")]
    run()
    assert env["guard"] == []
    assert env["updates"] == [("ab12cd34", {"tp1_order_id": 70, "tp2_order_id": 80})]


def test_tp_missing_triggers_reattach_on_startup(env):
    env["stored"] = [dict(LOCAL)]
    env["algo"] = [algo(111)]
    env["orders"] = [lim(7, "53.23")]                     # TP2 没了
    run()
    assert env["guard"] == [SYM]


def test_reattach_helper_query_failure_does_nothing(env, monkeypatch):
    from shared import binance_api as B

    def boom(sym=None):
        raise RuntimeError("net")
    monkeypatch.setattr(B, "get_algo_open_orders", boom)
    r = asyncio.run(M._check_and_reattach_guards(dict(LOCAL), dict(RP)))
    assert r["sl"] == "query_failed" and env["guard"] == [] and env["updates"] == []


# ── sync_lite 同逻辑 ──────────────────────────────────────────────────────
def test_sync_lite_keeps_local_record(env):
    env["stored"] = [dict(LOCAL)]
    env["algo"] = [algo(111)]
    env["orders"] = [lim(7, "53.23"), lim(8, "50.0")]
    run_lite()
    assert env["added"] == [] and env["guard"] == []
    assert env["stored"][0]["pos_id"] == "ab12cd34" and env["stored"][0]["mode"] == "ema_trend"


def test_sync_lite_sl_missing_reattaches_but_tp_missing_does_not(env):
    env["stored"] = [dict(LOCAL)]
    env["algo"] = [algo(111)]
    env["orders"] = []                                    # TP 全无：lite 不补 TP
    run_lite()
    assert env["guard"] == []
    env["algo"] = []                                      # SL 也没了：lite 补
    run_lite()
    assert env["guard"] == [SYM] and env["added"] == []


def test_sync_lite_no_local_builds_orphan(env):
    env["algo"] = [algo(111)]
    run_lite()
    assert len(env["added"]) == 1 and env["added"][0]["source"] == "orphan_recovered_sync_lite"


# ── (d) position_review 回溯 recovered ────────────────────────────────────
def _log_line(dt, text, lv="INFO", name="x"):
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')},123 {lv} {name} {text}\n"


def _orphan_rec(created_ms=1793093047155, entry=7.5):
    return {"pos_id": f"rec_UNITREEUSDT_L_{created_ms}", "symbol": "UNITREEUSDT", "direction": "long",
            "mode": "recovered", "sig_type": "recovered", "source": "orphan_recovered",
            "entry_price": entry, "quantity": 100, "open_time": created_ms, "judge_detail": {},
            "_status": "open"}


def test_position_review_traces_recovered_back_to_original_signal(tmp_path):
    from tools import position_review as PR
    created = datetime.fromtimestamp(1793093047.155, tz=timezone.utc)
    fill = created - timedelta(hours=5)
    lines = [
        _log_line(fill - timedelta(minutes=12), "[scanner] UNITREEUSDT score 72→78 (btc=+0 cg=+0 strat=+3 shock=+0 tier=+0 pulse=+0 hub_bonus=+3)"),
        _log_line(fill - timedelta(minutes=10), "[signal_watcher] UNITREEUSDT 注册监控 mode=ema_trend 触发区间=[7.4,7.6]"),
        _log_line(fill - timedelta(minutes=30), "[JUDGE_REJECT] UNITREEUSDT ema_trend LONG: 回踩不足"),
        _log_line(fill, "[executor] UNITREEUSDT 成交 price=7.5 qty=100"),
        _log_line(created - timedelta(minutes=1), "[executor] OTHERUSDT 成交 price=1 qty=1"),
    ]
    (tmp_path / "system.log").write_text("".join(lines), encoding="utf-8")
    out = PR.review(_orphan_rec(), tmp_path, 0.0, net=False, now=created + timedelta(hours=1))
    assert "原始信号回溯" in out
    assert "回溯原策略: ema_trend" in out
    assert "72" in out and "78" in out            # 原始评分
    assert "回踩不足" in out                        # 原始拒绝历史
    assert "未在建档前" not in out


def test_position_review_recovered_without_original_fill_shows_unknown(tmp_path):
    from tools import position_review as PR
    created = datetime.fromtimestamp(1793093047.155, tz=timezone.utc)
    (tmp_path / "system.log").write_text(_log_line(created - timedelta(hours=100),
                                                   "[executor] UNITREEUSDT 成交 price=7.5 qty=100"), encoding="utf-8")
    out = PR.review(_orphan_rec(), tmp_path, 0.0, net=False, now=created + timedelta(hours=1))
    assert "未在建档前" in out and "→  ?" in out


def test_position_review_price_mismatch_flagged(tmp_path):
    from tools import position_review as PR
    created = datetime.fromtimestamp(1793093047.155, tz=timezone.utc)
    (tmp_path / "system.log").write_text(_log_line(created - timedelta(hours=3),
                                                   "[executor] UNITREEUSDT 成交 price=9.9 qty=100"), encoding="utf-8")
    out = PR.review(_orphan_rec(entry=7.5), tmp_path, 0.0, net=False, now=created)
    assert "偏差>1%" in out
