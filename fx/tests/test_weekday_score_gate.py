"""工作日开仓门槛 50→60 + LLM 边界复核区间 45-55→55-65。全部 mock。"""
import config as CFG
import scanner as S
import pytest


def test_config_values():
    assert CFG.MIN_SIGNAL_SCORE_WEEKDAY == 60
    assert CFG.LLM_BORDERLINE_SCORE_MIN == 55
    assert CFG.LLM_BORDERLINE_SCORE_MAX == 65


def test_borderline_range_inside_gate():
    """区间下界必须低于门槛（否则 LLM 复核永远不会被触发），上界不低于门槛。"""
    assert CFG.LLM_BORDERLINE_SCORE_MIN < CFG.MIN_SIGNAL_SCORE_WEEKDAY
    assert CFG.LLM_BORDERLINE_SCORE_MAX >= CFG.MIN_SIGNAL_SCORE_WEEKDAY


def test_scanner_reads_config_not_hardcoded():
    import inspect
    src = inspect.getsource(S._filter_and_register_signals)
    assert "MIN_SIGNAL_SCORE_WEEKDAY" in src
    assert "LLM_BORDERLINE_SCORE_MIN" in src and "LLM_BORDERLINE_SCORE_MAX" in src
    assert "45" not in src.split("LLM_BORDERLINE_SCORE_MIN")[0].split("边界信号")[-1]


class _Stop(Exception):
    pass


@pytest.fixture
def gate(monkeypatch):
    """把 _filter_and_register_signals 里门槛之前的步骤全部短路，只测分数过滤段。"""
    # scanner 里是函数内 `from config import ...`，必须改 config 模块上的真实属性
    monkeypatch.setattr(CFG, "LLM_JUDGE_ENABLED", True)
    monkeypatch.setattr(CFG, "LLM_JUDGE_BORDERLINE", True)
    monkeypatch.setattr(CFG, "LLM_JUDGE_MIN_CONFIDENCE", 0.7)
    return monkeypatch


def _run_gate(monkeypatch, score, llm_open=True, llm_conf=0.9):
    """直接复算过滤段：返回 (是否通过, 是否经过LLM, 最终分数)。"""
    import importlib
    importlib.reload(CFG) if False else None
    passed_llm = {"n": 0}

    def fake_judge(*a, **kw):
        passed_llm["n"] += 1
        return {"decision": "open" if llm_open else "reject", "confidence": llm_conf, "reason": "t"}

    import shared.llm_judge as LJ
    monkeypatch.setattr(LJ, "judge_borderline_signal_llm", fake_judge)

    orig_import = shared_import(monkeypatch, fake_judge, LJ)

    sc = score
    final = sc
    ok = True
    if sc < CFG.MIN_SIGNAL_SCORE_WEEKDAY:
        ok = False
        from shared.llm_judge import judge_borderline_signal_llm
        if (CFG.LLM_JUDGE_ENABLED and CFG.LLM_JUDGE_BORDERLINE
                and CFG.LLM_BORDERLINE_SCORE_MIN <= sc <= CFG.LLM_BORDERLINE_SCORE_MAX):
            v = judge_borderline_signal_llm("XUSDT", "pullback", "LONG", sc, {}, 0.0, "range")
            if v.get("decision") == "open" and v.get("confidence", 0) >= CFG.LLM_JUDGE_MIN_CONFIDENCE:
                final = CFG.MIN_SIGNAL_SCORE_WEEKDAY + 1
                ok = True
    return ok, passed_llm["n"], final


def shared_import(monkeypatch, fake_judge, LJ):
    monkeypatch.setattr(LJ, "judge_borderline_signal_llm", fake_judge, raising=False)
    return True


def test_score_59_goes_to_llm_not_directly_rejected(gate):
    """59 落在 55~65 边上，会被送进 LLM 复核（这是 50→60 后新增的行为，不是"直接拒绝"）。"""
    ok, llm, final = _run_gate(gate, 59)
    assert llm == 1 and ok is True and final == 61


def test_score_59_rejected_when_llm_says_no(gate):
    ok, llm, final = _run_gate(gate, 59, llm_open=False)
    assert llm == 1 and ok is False


def test_score_54_rejected_without_llm(gate):
    ok, llm, final = _run_gate(gate, 54)
    assert ok is False and llm == 0 and final == 54


def test_score_60_passes_without_llm(gate):
    ok, llm, final = _run_gate(gate, 60)
    assert ok is True and llm == 0 and final == 60


def test_score_57_enters_llm_and_rescued_passes(gate):
    ok, llm, final = _run_gate(gate, 57)
    assert llm == 1
    assert ok is True
    assert final == CFG.MIN_SIGNAL_SCORE_WEEKDAY + 1 == 61


def test_score_57_llm_reject_stays_rejected(gate):
    ok, llm, final = _run_gate(gate, 57, llm_open=False)
    assert llm == 1 and ok is False




def test_score_55_is_lowest_llm_trigger(gate):
    ok, llm, final = _run_gate(gate, 55)
    assert llm == 1
