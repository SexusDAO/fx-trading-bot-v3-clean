"""signal_analytics 的 final_score 不再为 null：落盘带基础分(stage=base)，评分层后由 patch 补最终分(stage=final)。"""
import json

import scanner as sc
from tools import reject_funnel as RF

IND = {"price": 1.0, "chg24": 1.0, "atr_ratio": 1, "rsi_1h": 50, "rsi_4h": 50, "vol_ratio": 1}


def _write(tmp_path, monkeypatch, **kw):
    f = tmp_path / "sa.jsonl"
    monkeypatch.setattr(sc, "SIGNAL_ANALYTICS_FILE", f)
    sc._write_signal_analytics("AUSDT", "pullback", "LONG", True, None, kw.pop("ind", dict(IND)), {}, **kw)
    return json.loads(f.read_text().splitlines()[0])


def test_pass_record_gets_base_score_when_ind_has_none(tmp_path, monkeypatch):
    r = _write(tmp_path, monkeypatch, final_score=60)
    assert r["final_score"] == 60 and r["final_score_stage"] == "base"


def test_ind_final_score_wins_and_marked_final(tmp_path, monkeypatch):
    r = _write(tmp_path, monkeypatch, ind=dict(IND, final_score=72), final_score=60)
    assert r["final_score"] == 72 and r["final_score_stage"] == "final"


def test_reject_record_without_any_score_stays_null(tmp_path, monkeypatch):
    r = _write(tmp_path, monkeypatch)
    assert r["final_score"] is None and r["final_score_stage"] is None


def test_scanner_wiring():
    src = open(sc.__file__, encoding="utf-8").read()
    assert "final_score=_best[\"score\"]" in src
    assert 'sig["final_score"]    = new_score' in src
    assert '"final_score_stage": "final"' in src


def test_reject_funnel_patch_overrides_base_score_and_fills_null(tmp_path):
    f = tmp_path / "signal_analytics.jsonl"
    base = {"ts": 1_000, "symbol": "AUSDT", "mode": "pullback", "judge_passed": True, "final_score": 60,
            "final_score_stage": "base", "entry_params_ok": None}
    patch = {"type": "entry_params_patch", "ts": 1_000, "symbol": "AUSDT", "mode": "pullback",
             "entry_params_ok": True, "final_score": 78, "final_score_stage": "final"}
    f.write_text(json.dumps(base) + "\n" + json.dumps(patch) + "\n")
    verdicts, stats = RF._load_records([f], 0)
    v = [x for x in verdicts if x["symbol"] == "AUSDT"][0]
    assert v["final_score"] == 78 and v["final_score_stage"] == "final" and v["entry_params_ok"] is True
