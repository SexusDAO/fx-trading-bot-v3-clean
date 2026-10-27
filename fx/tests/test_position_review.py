"""tools/position_review.py：只读复盘工具。全部用 tmp_path 合成日志，不联网。"""
import importlib.util
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

_P = Path(__file__).resolve().parent.parent / "tools" / "position_review.py"
_spec = importlib.util.spec_from_file_location("position_review", _P)
PR = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(PR)

OPEN_MS = int(datetime(2026, 10, 27, 13, 20, tzinfo=timezone.utc).timestamp() * 1000)
NOW = datetime(2026, 10, 27, 15, 0, tzinfo=timezone.utc)

SCORE_LINE = ("2026-10-27 13:19:40,100 INFO fx.scanner [scanner] UNITREEUSDT score 65→53 "
              "(btc=-5 cg=+10 strat=-15 shock=+0 tier=-5 pulse=+0 hub_bonus=+3)")


def _w(p, lines):
    Path(p).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _pos(**kw):
    p = {"pos_id": "93b41731", "symbol": "UNITREEUSDT", "direction": "long", "status": "open", "mode": "ema_trend",
         "entry_price": 95.28, "sl_price": 90.69, "tp1_price": 107.39, "tp2_price": 110.0, "quantity": 0.78,
         "leverage": 20, "open_time": OPEN_MS, "signal_strength": "strong", "position_size_cap": 0.53,
         "judge_detail": {"ema20_4h": 102.0, "ema50_4h": 93.35, "rsi_1h": 50.2, "rsi_4h": 59.3,
                          "vol_ratio": 1.69, "chg24": -6.4}}
    p.update(kw)
    return p


@pytest.fixture
def logs(tmp_path):
    _w(tmp_path / "system.log", [
        "2026-10-27 10:51:02,000 INFO fx.scanner [JUDGE_REJECT] UNITREEUSDT ema_trend LONG: 价格追高: price=97.21 在EMA20上方5.9%",
        "2026-10-27 13:06:02,000 INFO fx.scanner [JUDGE_REJECT] UNITREEUSDT ema_trend LONG: 4H RSI=76.0 超买",
        "2026-10-27 13:06:02,000 INFO fx.scanner [JUDGE_REJECT] OTHERUSDT ema_trend LONG: 不应出现",
        "2026-10-27 13:18:00,000 INFO fx.scanner [scanner] UNITREEUSDT ema_trend base=65 btc_adj=-5 strategy_adj=-15 (mkt=bull strength=medium)",
        SCORE_LINE,
        "2026-10-27 13:19:50,000 INFO fx.sw [signal_watcher] UNITREEUSDT 注册监控 mode=ema_trend 触发区间=[1,2]",
        "无法解析的行 UNITREEUSDT",
    ])
    _w(tmp_path / "pos_state.json", [json.dumps([_pos()])])
    return tmp_path


def test_regex_score_line_matches():
    m = PR.SCORE_RE.search(SCORE_LINE)
    d = m.groupdict()
    assert (d["sym"], d["base"], d["final"], d["btc"], d["cg"], d["strat"], d["tier"], d["hub"]) == \
           ("UNITREEUSDT", "65", "53", "-5", "+10", "-15", "-5", "+3")


def test_review_core_fields(logs):
    rec = PR.pick(PR.load_positions(logs), "unitreeusdt")[0]
    out = PR.review(rec, logs, net=False, now=NOW)
    assert "UNITREEUSDT LONG 开仓复盘" in out
    assert "2026-10-27 13:20 UTC" in out and "93b41731" in out
    assert "base_score:    65" in out and "最终得分:      53" in out and "cg_bonus:      +10" in out
    assert "10:51 价格追高" in out and "13:06 4H RSI=76.0 超买" in out and "✅ 通过" in out
    assert "OTHERUSDT" not in out and "不应出现" not in out
    assert "ema_gap=9.27%" in out
    assert "RSI 1H:    50.2" in out and "量比:       1.69" in out
    assert "距离4.8%" in out and "RR=2.64x" in out and "TP2:      存在" in out
    assert "持仓时长:  1h 40m" in out


def test_unparsable_fields_show_question_mark(tmp_path):
    _w(tmp_path / "pos_state.json", [json.dumps([_pos(judge_detail={}, sl_price=None, tp1_price=None)])])
    rec = PR.pick(PR.load_positions(tmp_path), "UNITREEUSDT")[0]
    out = PR.review(rec, tmp_path, net=False, now=NOW)       # 无 system.log 也不报错
    assert "ema_gap=?" in out and "最终得分" not in out and "?" in out


# ── 路径识别 ──
def _anchor():
    return datetime(2026, 10, 27, 13, 19, 40, tzinfo=timezone.utc)


def test_path_trigger_dispatch(logs):
    with open(logs / "system.log", "a", encoding="utf-8") as f:
        f.write("2026-10-27 13:19:30,000 INFO fx.td [trigger-dispatch] UNITREEUSDT ema_trend → signal signal=ema_trend\n")
    rec = PR.pick(PR.load_positions(logs), "UNITREEUSDT")[0]
    assert "触发器派发" in PR.review(rec, logs, net=False, now=NOW)


def test_path_main_scan_via_scan_signals(logs):
    ts = int(datetime(2026, 10, 27, 13, 19, 0, tzinfo=timezone.utc).timestamp() * 1000)
    (logs / "scan_signals.jsonl").write_text(json.dumps({"ts": ts, "symbol": "UNITREEUSDT", "strategy": "ema_trend",
                                                         "direction": "LONG"}) + "\n", encoding="utf-8")
    rec = PR.pick(PR.load_positions(logs), "UNITREEUSDT")[0]
    assert "主扫描" in PR.review(rec, logs, net=False, now=NOW)


def test_path_unknown_when_no_evidence(logs):
    rec = PR.pick(PR.load_positions(logs), "UNITREEUSDT")[0]
    assert "unknown（可能是孤儿仓位）" in PR.review(rec, logs, net=False, now=NOW)


def test_path_orphan_recovered(tmp_path):
    _w(tmp_path / "pos_state.json", [json.dumps([_pos(mode="recovered", source="orphan_recovered")])])
    rec = PR.pick(PR.load_positions(tmp_path), "UNITREEUSDT")[0]
    out = PR.review(rec, tmp_path, net=False, now=NOW)
    assert "orphan" in out and "距软限(12H)" in out


def test_path_ignores_other_symbol_and_old_events(logs):
    with open(logs / "system.log", "a", encoding="utf-8") as f:
        f.write("2026-10-27 13:19:30,000 INFO fx.td [trigger-dispatch] OTHERUSDT ema_trend → signal\n")
        f.write("2026-10-27 08:00:00,000 INFO fx.td [trigger-dispatch] UNITREEUSDT ema_trend → signal\n")   # 窗口外
    rec = PR.pick(PR.load_positions(logs), "UNITREEUSDT")[0]
    assert "unknown" in PR.review(rec, logs, net=False, now=NOW)


# ── 轮转日志 ──
def test_rotated_logs_searched(tmp_path):
    _w(tmp_path / "system.log", ["2026-10-27 14:00:00,000 INFO fx.x 无关"])
    _w(tmp_path / "system.log.3", [SCORE_LINE])
    _w(tmp_path / "pos_state.json", [json.dumps([_pos()])])
    rec = PR.pick(PR.load_positions(tmp_path), "UNITREEUSDT")[0]
    assert "最终得分:      53" in PR.review(rec, tmp_path, net=False, now=NOW)


# ── 多笔 / 方向 / 已平仓 ──
def test_pick_latest_direction_and_all(tmp_path):
    older = {"symbol": "UNITREEUSDT", "direction": "LONG", "open_time": OPEN_MS - 86400000, "pnl_usdt": -1.0,
             "close_time": OPEN_MS - 80000000, "close_reason": "sl", "holding_minutes": 100}
    short = {"symbol": "UNITREEUSDT", "direction": "SHORT", "open_time": OPEN_MS + 1000, "close_time": OPEN_MS + 5000}
    _w(tmp_path / "trade_log.jsonl", [json.dumps(older), json.dumps(short)])
    _w(tmp_path / "pos_state.json", [json.dumps([_pos()])])
    recs = PR.load_positions(tmp_path)
    assert PR.pick(recs, "UNITREEUSDT")[0]["direction"] == "SHORT"
    assert PR.pick(recs, "UNITREEUSDT", "long")[0]["pos_id"] == "93b41731"
    assert len(PR.pick(recs, "UNITREEUSDT", show_all=True)) == 3


def test_closed_record_review(tmp_path):
    t = {"trade_id": "1_X", "symbol": "CAPUSDT", "direction": "LONG", "mode": "range_bounce", "entry_price": 0.1,
         "open_time": OPEN_MS, "close_time": OPEN_MS + 8100000, "close_reason": "sl_triggered", "pnl_usdt": -3.98,
         "pnl_pct": -4.0, "holding_minutes": 135}
    _w(tmp_path / "trade_log.jsonl", [json.dumps(t)])
    rec = PR.pick(PR.load_positions(tmp_path), "CAPUSDT")[0]
    out = PR.review(rec, tmp_path, net=False, now=NOW)
    assert "已平仓" in out and "sl_triggered" in out and "-3.98U" in out and "2h 15m" in out


def test_adjust_history(logs):
    rows = [{"ts": "2026-10-27T14:02:00", "pos_id": "93b41731", "symbol": "UNITREEUSDT", "direction": "LONG",
             "action": "adjusted", "old_sl": 90.69, "new_sl": 90.69, "old_tp1": 105.79, "new_tp1": 107.39},
            {"ts": "2026-10-27T14:33:00", "pos_id": "93b41731", "symbol": "UNITREEUSDT", "direction": "LONG",
             "action": "no_change", "old_sl": 90.69, "new_sl": 90.69, "old_tp1": 107.39, "new_tp1": 107.39}]
    _w(logs / "tp_sl_adjust_log.jsonl", [json.dumps(r) for r in rows])
    out = PR.review(PR.pick(PR.load_positions(logs), "UNITREEUSDT")[0], logs, net=False, now=NOW)
    assert "14:02 SL 90.69→90.69（无变化） | TP1 105.79→107.39" in out and "TP1 107.39→107.39（无变化）" in out


# ── 列表 / CLI / 只读 ──
def test_today_and_closed_tables(logs):
    out = PR.list_today(PR.load_positions(logs), logs, now=NOW)
    assert "UNITREEUSDT" in out and "53" in out and "strong" in out
    t = {"symbol": "CAPUSDT", "direction": "LONG", "mode": "range_bounce", "close_time": int(NOW.timestamp() * 1000) - 3600000,
         "close_reason": "sl_triggered", "pnl_usdt": -3.98, "holding_minutes": 135}
    _w(logs / "trade_log.jsonl", [json.dumps(t)])
    c = PR.list_closed(PR.load_positions(logs), now=NOW)
    assert "CAPUSDT" in c and "-3.98U" in c and "2h 15m" in c


def test_cli_not_found_and_readonly(logs, capsys):
    before = {p.name: p.read_bytes() for p in logs.iterdir()}
    assert PR.main(["NOPEUSDT", "--log-dir", str(logs), "--no-funding"]) == 1
    assert PR.main(["UNITREEUSDT", "--log-dir", str(logs), "--no-funding"]) == 0
    assert PR.main(["--today", "--log-dir", str(logs)]) == 0
    assert PR.main(["--closed", "--log-dir", str(logs)]) == 0
    assert {p.name: p.read_bytes() for p in logs.iterdir()} == before      # 没改任何文件


def test_no_network_when_disabled(logs, monkeypatch):
    monkeypatch.setattr(PR, "funding_and_price", lambda *a, **k: (_ for _ in ()).throw(AssertionError("network")))
    rec = PR.pick(PR.load_positions(logs), "UNITREEUSDT")[0]
    PR.review(rec, logs, net=False, now=NOW)


def test_funding_direction_text(logs, monkeypatch):
    monkeypatch.setattr(PR, "funding_and_price", lambda *a, **k: (-0.0096, 100.0))
    rec = PR.pick(PR.load_positions(logs), "UNITREEUSDT")[0]
    out = PR.review(rec, logs, net=True, now=NOW)
    assert "-0.9600%（LONG收钱，有利）" in out
    monkeypatch.setattr(PR, "funding_and_price", lambda *a, **k: (0.0096, 100.0))
    assert "LONG付钱，不利" in PR.review(rec, logs, net=True, now=NOW)


# ── 三个修复 ──
def test_default_tz_offset_auto_detected():
    import time
    expect = -(time.altzone if time.localtime().tm_isdst > 0 else time.timezone) / 3600
    assert PR._local_tz_offset == expect


def test_cli_default_tz_offset_is_local(monkeypatch, logs):
    seen = {}
    monkeypatch.setattr(PR, "list_today", lambda recs, d, tz, **k: seen.setdefault("tz", tz) or "")
    PR.main(["--today", "--log-dir", str(logs)])
    assert seen["tz"] == PR._local_tz_offset
    seen.clear()
    PR.main(["--today", "--log-dir", str(logs), "--log-tz-offset", "8"])
    assert seen["tz"] == 8.0


def test_tz_offset_shifts_log_time(tmp_path):
    # 日志是东八区本地时间 21:19 → UTC 13:19，应命中 13:20 UTC 的开仓
    _w(tmp_path / "system.log", [SCORE_LINE.replace("13:19:40", "21:19:40")])
    _w(tmp_path / "pos_state.json", [json.dumps([_pos()])])
    rec = PR.pick(PR.load_positions(tmp_path), "UNITREEUSDT")[0]
    assert "最终得分:      53" in PR.review(rec, tmp_path, tz_off_h=8.0, net=False, now=NOW)
    assert "最终得分" not in PR.review(rec, tmp_path, tz_off_h=0.0, net=False, now=NOW)


def test_closed_requires_close_time(tmp_path):
    good = {"trade_id": "1_A", "symbol": "AAAUSDT", "direction": "LONG", "close_time": OPEN_MS, "pnl_usdt": 1.0}
    bad1 = {"trade_id": "2_B", "symbol": "BBBUSDT", "direction": "LONG"}                  # 无 close_time
    bad2 = {"trade_id": "3_C", "symbol": "CCCUSDT", "direction": "LONG", "close_time": 0}  # close_time 为 0
    _w(tmp_path / "trade_log.jsonl", [json.dumps(r) for r in (good, bad1, bad2)])
    recs = PR.load_positions(tmp_path)
    assert [r["symbol"] for r in recs] == ["AAAUSDT"]
    out = PR.list_closed(recs, now=NOW)
    assert "BBBUSDT" not in out and "CCCUSDT" not in out


def test_closed_dedup_by_trade_id(tmp_path):
    t = {"trade_id": "9_CHIP", "symbol": "CHIPUSDT", "direction": "SHORT", "mode": "ema_trend",
         "close_time": int(NOW.timestamp() * 1000) - 3600000, "close_reason": "sl", "pnl_usdt": -5.0, "holding_minutes": 270}
    dup = dict(t)
    _w(tmp_path / "trade_log.jsonl", [json.dumps(t), json.dumps(dup)])
    out = PR.list_closed(PR.load_positions(tmp_path), now=NOW)
    assert out.count("CHIPUSDT") == 1
    assert "合计 1 笔，盈亏 -5.00U" in out


def test_closed_dedup_fallback_key_without_trade_id(tmp_path):
    ct = int(NOW.timestamp() * 1000) - 3600000
    a = {"symbol": "XUSDT", "direction": "LONG", "close_time": ct, "pnl_usdt": -2.0}
    _w(tmp_path / "trade_log.jsonl", [json.dumps(a), json.dumps(a)])
    out = PR.list_closed(PR.load_positions(tmp_path), now=NOW)
    assert out.count("XUSDT") == 1 and "合计 1 笔" in out


def test_closed_dedup_ignores_different_trade_id(tmp_path):
    """trade_id 不同但 symbol + close_time 相同 → 视为同一笔，只显示一次。"""
    ct = int(NOW.timestamp() * 1000) - 3600000
    a = {"trade_id": f"{ct}_CHIPUSDT", "symbol": "CHIPUSDT", "direction": "SHORT", "mode": "ema_trend",
         "close_time": ct, "close_reason": "sl", "pnl_usdt": -5.0, "holding_minutes": 270}
    b = dict(a, trade_id="another-id-123")
    _w(tmp_path / "trade_log.jsonl", [json.dumps(a), json.dumps(b)])
    out = PR.list_closed(PR.load_positions(tmp_path), now=NOW)
    assert out.count("CHIPUSDT") == 1 and "合计 1 笔，盈亏 -5.00U" in out


def test_closed_same_symbol_different_close_time_both_shown(tmp_path):
    ct = int(NOW.timestamp() * 1000) - 3600000
    a = {"trade_id": "1", "symbol": "CHIPUSDT", "direction": "SHORT", "close_time": ct, "pnl_usdt": -5.0}
    b = {"trade_id": "2", "symbol": "CHIPUSDT", "direction": "SHORT", "close_time": ct - 600000, "pnl_usdt": -1.0}
    _w(tmp_path / "trade_log.jsonl", [json.dumps(a), json.dumps(b)])
    out = PR.list_closed(PR.load_positions(tmp_path), now=NOW)
    assert out.count("CHIPUSDT") == 2 and "合计 2 笔" in out
