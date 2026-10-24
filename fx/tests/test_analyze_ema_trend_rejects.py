import json
from tools import analyze_ema_trend_rejects as a


def _rej(ts, reason, first="judge_ema_trend", sym="AUSDT", **kw):
    r = {"ts": ts, "symbol": sym, "signal_type": "ema_trend", "direction": "LONG",
         "first_rejected_at": first, "reason": reason, "entry_candidate": 100.0, "sl": 98.0, "tp1": 104.0}
    r.update(kw)
    return r


def test_rule_mapping():
    assert a.rule_of(_rej(1, "1H RSI=70.0 不在健康区间 [48, 68]")) == "6.1H RSI不在区间"
    assert a.rule_of(_rej(1, "量能不足: vol_ratio=0.30 < 0.5，回踩无量")) == "9.量比过低"
    assert a.rule_of(_rej(1, "x", first="corr_filter")) == "[corr_filter]"
    assert a.rule_of(_rej(1, "完全未知的原因 12%")).startswith("其他:")


def test_counts_sorted_and_ev():
    logs = [_rej(1, "1H RSI=70 不在健康区间"), _rej(2, "1H RSI=71 不在健康区间", sym="BUSDT"),
            _rej(3, "量能不足: vol_ratio=0.3"),
            {"ts": 4, "symbol": "C", "signal_type": "pullback", "first_rejected_at": "judge_pullback", "reason": "x"}]
    outs = [{**logs[0], "outcome": {"label": "先止盈"}},   # RR=2 → +2R
            {**logs[1], "outcome": {"label": "先止损"}},   # -1R
            {**logs[2], "outcome": {"label": "未成交"}}]
    rows = a.analyze(logs, outs)
    assert [r["rule"] for r in rows] == ["6.1H RSI不在区间", "9.量比过低"]   # 按次数降序,非 ema_trend 被忽略
    r0 = rows[0]
    assert (r0["n"], r0["tp"], r0["sl"]) == (2, 1, 1)
    assert abs(r0["ev_r"] - 0.5) < 1e-9 and abs(r0["win_rate"] - 0.5) < 1e-9
    assert rows[1]["nofill"] == 1 and rows[1]["ev_r"] is None


def test_days_filter():
    now = 10 * 86400_000
    logs = [_rej(now - 86400_000, "量能不足"), _rej(now - 5 * 86400_000, "量能不足")]
    assert a.analyze(logs, [], days=2, now_ms=now)[0]["n"] == 1


def test_main_readonly_and_missing_outcomes(tmp_path, capsys):
    lp = tmp_path / "rejected_log.jsonl"
    lp.write_text(json.dumps(_rej(1, "量能不足")) + "\n坏行\n", encoding="utf-8")
    before = lp.read_text(encoding="utf-8")
    rows = a.main(["--log", str(lp), "--outcomes", str(tmp_path / "none.jsonl")])
    assert rows[0]["n"] == 1 and "只统计次数" in capsys.readouterr().out
    assert lp.read_text(encoding="utf-8") == before and not (tmp_path / "none.jsonl").exists()
