"""tools/sync_trades.py + shared/trade_dedup + binance_api.get_income/get_user_trades_range（全程 mock，不联网）。"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import trade_dedup as D
from tools import sync_trades as T

NOW = 1_800_000_000_000
H = 3600_000


def fill(i, t, side, ps, px, qty, pnl=0.0, fee=0.01):
    return {"id": i, "time": t, "side": side, "positionSide": ps, "price": str(px), "qty": str(qty),
            "realizedPnl": str(pnl), "commission": str(fee), "commissionAsset": "USDT"}


def short_trip(base, sym="LTCUSDT"):
    # 开空 10 @56.8，TP1 平 6 @54（+），TP2 平 4 @53（+）
    return [fill(1, base, "SELL", "SHORT", 56.8, 10),
            fill(2, base + 60_000, "BUY", "SHORT", 54, 6, 16.8),
            fill(3, base + 120_000, "BUY", "SHORT", 53, 4, 15.2)]


# ── trade_dedup ──
def test_same_trade_rules():
    a = {"trade_id": "x", "symbol": "A", "close_time": 1000, "direction": "long"}
    assert D.same_trade(a, {"trade_id": "x", "symbol": "A"})
    b = {"trade_id": "y", "symbol": "A", "close_time": 1000 + 5000, "direction": "LONG"}
    assert D.same_trade(a, b)                               # 5 秒内、方向大小写不敏感
    assert not D.same_trade(a, dict(b, close_time=1000 + 5001))
    assert not D.same_trade(a, dict(b, symbol="B"))
    assert not D.same_trade(a, dict(b, direction="short"))
    assert not D.same_trade({"symbol": "A"}, {"symbol": "A"})   # 无 close_time（开仓记录）不参与


def test_dedup_keeps_first_and_counts():
    rs = [{"trade_id": "1", "symbol": "A", "close_time": 100000},
          {"trade_id": "2", "symbol": "A", "close_time": 102000},
          {"trade_id": "3", "symbol": "A", "close_time": 200000},
          {"trade_id": "4", "symbol": "B", "close_time": 100000}]
    kept, dups = D.dedup(rs)
    assert [r["trade_id"] for r in kept] == ["1", "3", "4"] and dups == 1


# ── build_trips ──
def test_build_trips_short_two_tp():
    trips, sk = T.build_trips("LTCUSDT", short_trip(NOW - 5 * H))
    assert sk == 0 and len(trips) == 1
    t = trips[0]
    assert t["direction"] == "SHORT" and t["open_time"] == NOW - 5 * H
    assert t["close_time"] == NOW - 5 * H + 120_000
    assert t["quantity"] == pytest.approx(10) and t["pnl_usdt"] == pytest.approx(32.0)
    assert t["exit_price"] == pytest.approx((54 * 6 + 53 * 4) / 10) and not t["partial_window"]
    assert t["fees_usdt"] == pytest.approx(0.03)


def test_build_trips_open_position_not_reported_and_two_trips():
    f = short_trip(NOW - 9 * H) + [fill(10, NOW - 3 * H, "SELL", "SHORT", 57, 5),
                                   fill(11, NOW - 2 * H, "BUY", "SHORT", 56, 2, 2.0)]
    trips, _ = T.build_trips("LTCUSDT", f)
    assert len(trips) == 1                                   # 第二笔只平了一部分，仍持仓


def test_build_trips_window_started_mid_position():
    f = [fill(5, NOW - H, "SELL", "LONG", 100, 3, -4.0)]      # 窗口前开的多单，窗口内被平
    trips, _ = T.build_trips("X", f)
    assert len(trips) == 1 and trips[0]["partial_window"] and trips[0]["open_time"] == 0


def test_build_trips_both_mode_skipped():
    trips, sk = T.build_trips("X", [fill(1, NOW, "BUY", "BOTH", 1, 1)])
    assert trips == [] and sk == 1


# ── merge ──
def local_rec(trade_id, close, **kw):
    r = {"trade_id": trade_id, "symbol": "LTCUSDT", "direction": "short", "mode": "ema_trend",
         "close_reason": "tp2", "signal_strength": "strong", "entry_price": 56.8,
         "open_time": NOW - 5 * H, "close_time": close, "exit_price": 53.4, "pnl_usdt": 30.0}
    r.update(kw)
    return r


def trips_one():
    return T.build_trips("LTCUSDT", short_trip(NOW - 5 * H))[0]


def test_merge_complete_local_record_untouched():
    close = NOW - 5 * H + 121_000                             # 与交易所差 1 秒
    raw = [json.dumps(local_rec("a", close)) + "\n"]
    res = T.merge(raw, trips_one(), set())
    assert res["lines"] == [raw[0].rstrip("\n")] and not res["appended"] and not res["filled"]
    assert res["matched"] == 1


def test_merge_fills_only_empty_fields_keeps_strategy_fields():
    close = NOW - 5 * H + 121_000
    raw = [json.dumps(local_rec("a", close, pnl_usdt=0, exit_price=0)) + "\n"]
    res = T.merge(raw, trips_one(), set())
    r = json.loads(res["lines"][0])
    assert r["pnl_usdt"] == pytest.approx(32.0) and r["exit_price"] == pytest.approx(53.4, abs=0.01) or r["exit_price"] > 0
    assert r["mode"] == "ema_trend" and r["close_reason"] == "tp2" and r["signal_strength"] == "strong"
    assert r["close_time"] == close and r["exchange_filled"] == ["pnl_usdt", "exit_price"]


def test_merge_only_pnl_missing_keeps_local_exit_price():
    close = NOW - 5 * H + 121_000
    raw = [json.dumps(local_rec("a", close, pnl_usdt=0)) + "\n"]
    r = json.loads(T.merge(raw, trips_one(), set())["lines"][0])
    assert r["exit_price"] == 53.4 and r["pnl_usdt"] == pytest.approx(32.0)


def test_merge_matches_by_open_close_interval_when_close_time_far():
    # 本地 close_time 比交易所晚很多（如 patrol 补记），但交易所平仓落在本地 [open, close] 内
    raw = [json.dumps(local_rec("a", NOW - 5 * H + 600_000)) + "\n"]
    res = T.merge(raw, trips_one(), set())
    assert res["matched"] == 1 and not res["appended"]


def test_merge_appends_missing_with_exchange_sync_source():
    res = T.merge([], trips_one(), set())
    assert len(res["appended"]) == 1
    r = json.loads(res["lines"][-1])
    assert r["source"] == "exchange_sync" and r["close_reason"] == "exchange_sync" and r["mode"] == "unknown"
    assert r["pnl_usdt"] == pytest.approx(32.0) and r["trade_id"] == f"{r['close_time']}_LTCUSDT"
    assert r["open_time"] == NOW - 5 * H


def test_merge_idempotent_second_run_no_change():
    first = T.merge([], trips_one(), set())
    second = T.merge([l + "\n" for l in first["lines"]], trips_one(), set())
    assert not second["appended"] and not second["filled"] and second["lines"] == first["lines"]


def test_merge_keeps_garbage_and_open_records_and_duplicates():
    openrec = {"pos_id": "p", "symbol": "LTCUSDT", "fill_price": 56.8}
    raw = ["not json\n", json.dumps(openrec) + "\n", json.dumps(local_rec("a", NOW - 5 * H + 121_000)) + "\n",
           json.dumps(local_rec("a", NOW - 5 * H + 121_000)) + "\n"]
    res = T.merge(raw, trips_one(), set())
    assert res["lines"][:4] == [l.rstrip("\n") for l in raw] and not res["appended"]


def test_merge_skips_partial_of_live_position_but_appends_if_not_live():
    f = [fill(5, NOW - H, "SELL", "LONG", 100, 3, -4.0)]
    trips = T.build_trips("X", f)[0]
    assert T.merge([], trips, {("X", "LONG")})["skipped_live"] and not T.merge([], trips, {("X", "LONG")})["appended"]
    assert len(T.merge([], trips, set())["appended"]) == 1


# ── sync() 端到端 ──
@pytest.fixture
def root(tmp_path):
    (tmp_path / "logs").mkdir()
    return tmp_path


def test_sync_writes_backup_and_atomically_replaces(root):
    log = root / "logs" / "trade_log.jsonl"
    log.write_text(json.dumps(local_rec("old", NOW - 20 * H, symbol="OLDUSDT")) + "\n")
    out = []
    res = T.sync(root, now_ms=NOW,
                 fetch_trades=lambda s, a, b: short_trip(NOW - 5 * H) if s == "LTCUSDT" else [],
                 fetch_income=lambda a, b: [{"symbol": "LTCUSDT", "income": "32.0", "incomeType": "REALIZED_PNL"}],
                 out=out.append)
    lines = log.read_text().splitlines()
    assert len(lines) == 2 and json.loads(lines[1])["source"] == "exchange_sync"
    assert res["discovered"] == ["LTCUSDT"] and res["mismatch"] == []
    baks = list((root / "logs").glob("trade_log.jsonl.bak.*"))
    assert len(baks) == 1 and len(baks[0].read_text().splitlines()) == 1
    assert not list((root / "logs").glob("*.tmp"))
    assert any("新增 1 笔" in o for o in out)


def test_sync_dry_run_writes_nothing(root):
    log = root / "logs" / "trade_log.jsonl"
    T.sync(root, now_ms=NOW, dry_run=True, fetch_trades=lambda s, a, b: short_trip(NOW - 5 * H),
           fetch_income=lambda a, b: [{"symbol": "LTCUSDT", "income": "32"}], out=lambda *_: None)
    assert not log.exists() and not list((root / "logs").glob("*.bak.*"))


def test_sync_one_symbol_failure_does_not_stop_others_nor_delete(root):
    def ft(s, a, b):
        if s == "BADUSDT":
            raise RuntimeError("boom")
        return short_trip(NOW - 5 * H)
    res = T.sync(root, now_ms=NOW, fetch_trades=ft, out=lambda *_: None,
                 fetch_income=lambda a, b: [{"symbol": "BADUSDT", "income": "1"}, {"symbol": "LTCUSDT", "income": "32"}])
    assert [s for s, _ in res["failed"]] == ["BADUSDT"] and len(res["appended"]) == 1


def test_sync_income_failure_raises_without_touching_file(root):
    log = root / "logs" / "trade_log.jsonl"
    log.write_text("keep\n")
    def bad(a, b):
        raise RuntimeError("income down")
    with pytest.raises(RuntimeError):
        T.sync(root, now_ms=NOW, fetch_trades=lambda s, a, b: [], fetch_income=bad, out=lambda *_: None)
    assert log.read_text() == "keep\n"


def test_sync_reports_income_mismatch(root):
    res = T.sync(root, now_ms=NOW, dry_run=True, fetch_trades=lambda s, a, b: short_trip(NOW - 5 * H),
                 fetch_income=lambda a, b: [{"symbol": "LTCUSDT", "income": "10"}], out=lambda *_: None)
    assert res["mismatch"] and res["mismatch"][0][0] == "LTCUSDT"


# ── binance_api 新接口 ──
@pytest.fixture
def api(monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    from shared import binance_api as a
    return a


def test_get_user_trades_range_paginates_dedups_and_raises(api, monkeypatch):
    calls = []
    pages = [[{"id": i, "time": 1000 + i} for i in range(1, 4)],      # 满页（limit=3）
             [{"id": 3, "time": 1003}, {"id": 4, "time": 1004}]]      # 含重复 id=3
    def fake(path, params):
        calls.append(dict(params))
        return pages[len(calls) - 1]
    monkeypatch.setattr(api, "_signed_get", fake)
    out = api.get_user_trades_range("X", 1000, 2000, limit=3)
    assert [t["id"] for t in out] == [1, 2, 3, 4] and calls[1]["startTime"] == 1003
    monkeypatch.setattr(api, "_signed_get", lambda p, q: (_ for _ in ()).throw(RuntimeError("net")))
    monkeypatch.setattr(api, "_retry", lambda fn, *a, **k: None)
    with pytest.raises(api.ApiQueryError):
        api.get_user_trades_range("X", 1000, 2000)


def test_get_user_trades_range_splits_windows_over_6_days(api, monkeypatch):
    calls = []
    monkeypatch.setattr(api, "_signed_get", lambda p, q: calls.append(dict(q)) or [])
    api.get_user_trades_range("X", 0, 10 * 86400 * 1000)
    assert len(calls) == 2 and calls[0]["endTime"] - calls[0]["startTime"] <= 6 * 86400 * 1000


def test_get_income_params_and_paging(api, monkeypatch):
    calls = []
    pages = [[{"tranId": 1, "time": 1, "income": "1", "incomeType": "REALIZED_PNL", "symbol": "A"},
              {"tranId": 2, "time": 2, "income": "2", "incomeType": "REALIZED_PNL", "symbol": "A"}],
             [{"tranId": 3, "time": 3, "income": "3", "incomeType": "REALIZED_PNL", "symbol": "B"}]]
    def fake(path, params):
        calls.append((path, dict(params)))
        return pages[len(calls) - 1]
    monkeypatch.setattr(api, "_signed_get", fake)
    out = api.get_income("REALIZED_PNL", start_time=0, end_time=99, limit=2)
    assert [r["tranId"] for r in out] == [1, 2, 3]
    assert calls[0][0] == "/fapi/v1/income" and calls[0][1]["incomeType"] == "REALIZED_PNL"
    assert calls[1][1]["startTime"] == 2
