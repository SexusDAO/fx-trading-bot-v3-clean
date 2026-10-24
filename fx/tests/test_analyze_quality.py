"""tools/analyze_quality.py：空日志、坏行、净盈亏口径、滑点方向、止损质量、MFE/MAE、平仓原因。"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools import analyze_quality as Q  # noqa: E402


def _t(**kw):
    d = {"symbol": "X", "direction": "long", "mode": "pullback", "pnl_usdt": 1.0, "fees_usdt": 0.1,
         "pnl_net_usdt": 0.9, "nominal_value": 100.0, "holding_minutes": 60, "close_reason": "tp1"}
    d.update(kw)
    return d


def test_missing_file_and_empty_file(tmp_path):
    assert Q.render(Q.analyze(Q.load_records(tmp_path / "nope.jsonl"))) == "暂无成交记录"
    p = tmp_path / "e.jsonl"
    p.write_text("", encoding="utf-8")
    assert Q.render(Q.analyze(Q.load_records(p))) == "暂无成交记录"


def test_bad_lines_skipped(tmp_path):
    p = tmp_path / "t.jsonl"
    p.write_text("{bad\n\n" + json.dumps(_t()) + "\n[1,2]\n", encoding="utf-8")
    assert len(Q.load_records(p)) == 1


def test_overall_uses_net_pnl_and_fee_ratio():
    r = Q.analyze([_t(), _t(pnl_usdt=-2.0, pnl_net_usdt=-2.1, fees_usdt=0.1, close_reason="sl")])
    o = r["overall"]
    assert o["count"] == 2 and abs(o["win_rate"] - 0.5) < 1e-9
    assert abs(o["avg_pnl"] - (0.9 - 2.1) / 2) < 1e-9 and abs(o["avg_fee_ratio_pct"] - 0.1) < 1e-9


def test_net_fallback_and_missing_fee_count():
    r = Q.analyze([_t(pnl_net_usdt=None, pnl_usdt=1.0, fees_usdt=0.25), _t(pnl_net_usdt=None, fees_usdt=None)])
    assert abs(Q.net_pnl(_t(pnl_net_usdt=None, fees_usdt=0.25)) - 0.75) < 1e-9
    assert r["overall"]["missing_fees"] == 1


def test_by_strategy_includes_empty_known_ones():
    r = Q.analyze([_t(mode="reversal"), _t(mode="pullback")])
    assert r["by_strategy"]["reversal"]["count"] == 1 and r["by_strategy"]["breakout"]["count"] == 0


def test_sl_slippage_adverse_sign_by_direction():
    long_sl = _t(direction="long", close_reason="sl", sl_slippage_pct=-0.3)     # 低于止损价 = 不利
    short_sl = _t(direction="short", close_reason="sl", sl_slippage_pct=0.5)    # 高于止损价 = 不利
    s = Q.analyze([long_sl, short_sl, _t(entry_slippage_pct=0.1), _t(entry_slippage_pct=-0.3)])["slippage"]
    assert abs(s["sl_avg_adverse"] - 0.4) < 1e-9 and s["sl_n"] == 2
    assert abs(s["entry_avg"] - (-0.1)) < 1e-9 and s["entry_n"] == 2


def test_sl_quality_ratio_only_over_labeled():
    r = Q.analyze([_t(close_reason="sl", sl_quality="noise"), _t(close_reason="sl", sl_quality="valid"),
                   _t(close_reason="sl"), _t(close_reason="tp1", sl_quality="noise")])["sl_quality"]
    assert r["sl_trades"] == 3 and r["labeled"] == 2 and r["noise"] == 1 and r["valid"] == 1


def test_mfe_mae_ratio_uses_abs_mae():
    m = Q.analyze([_t(mfe_pct=2.0, mae_pct=-1.0), _t(mfe_pct=1.0, mae_pct=-1.0)])["mfe_mae"]
    assert abs(m["mfe_avg"] - 1.5) < 1e-9 and abs(m["mae_avg"] - 1.0) < 1e-9 and abs(m["ratio"] - 1.5) < 1e-9


def test_close_reason_alias_and_shares():
    r = Q.analyze([_t(close_reason="sl_triggered"), _t(close_reason="sl"), _t(close_reason="time_stop"),
                   _t(close_reason="manual_close")])["by_reason"]
    assert r["sl"]["count"] == 2 and abs(r["sl"]["share"] - 0.5) < 1e-9 and r["tp2"]["count"] == 0


def test_render_runs_on_sparse_records():
    out = Q.render(Q.analyze([{"pnl_usdt": 1.0}]))
    assert "总成交笔数" in out and "暂无 mfe_pct" in out
