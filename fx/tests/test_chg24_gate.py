"""数据质量门禁：chg24 缺失时跳过 pullback/reversal；analytics 快照里 chg24 不再恒为 0。"""
import json
import logging
import math

import pytest

import scanner as sc


@pytest.mark.parametrize("v,missing", [(None, True), (0, True), (0.0, True), (float("nan"), True), ("x", True),
                                       (1.5, False), (-0.01, False), ("2.5", False)])
def test_chg24_missing(v, missing):
    assert sc._chg24_missing(v) is missing


def test_dependent_routes_are_pullback_and_reversal():
    assert set(sc._CHG24_DEPENDENT_ROUTES) == {"pullback", "reversal"}


def test_gate_present_in_route_loop_before_judges():
    src = open(sc.__file__, encoding="utf-8").read()
    i_gate = src.index("chg24数据缺失，跳过pullback/reversal")
    i_judge = src.index("_ok, _reason = judge_reversal(k1h, _k4h, k15, ind, chg24, funding)")
    assert i_gate < i_judge
    assert 'ind["chg24"]         = chg24' in src


def test_analytics_snapshot_uses_ind_chg24(tmp_path, monkeypatch):
    f = tmp_path / "sa.jsonl"
    monkeypatch.setattr(sc, "SIGNAL_ANALYTICS_FILE", f)
    ind = {"price": 10.0, "chg24": 3.456, "atr_ratio": 1, "rsi_1h": 50, "rsi_4h": 50, "vol_ratio": 1}
    sc._write_signal_analytics("AUSDT", "pullback", "LONG", True, None, ind, {})
    rec = json.loads(f.read_text().splitlines()[0])
    assert rec["snapshot"]["chg24"] == 3.46
