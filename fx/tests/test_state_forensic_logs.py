"""取证日志只加日志、不改行为：remove_position / update_position(→closed) / _save_pos(pos_id 消失) 打 [state-forensic]。"""
import json
import logging

import pytest

import shared.state as S


@pytest.fixture
def posfile(tmp_path, monkeypatch):
    f = tmp_path / "pos_state.json"
    monkeypatch.setattr(S, "_POS_FILE", f)
    monkeypatch.setattr(S, "_POS_STATE_CORRUPTED", False)
    f.write_text(json.dumps([
        {"pos_id": "a1", "symbol": "XUSDT", "status": "open", "direction": "long"},
        {"pos_id": "b2", "symbol": "YUSDT", "status": "open", "direction": "short"},
    ]), encoding="utf-8")
    return f


def _ids(f):
    return [p["pos_id"] for p in json.loads(f.read_text(encoding="utf-8"))]


def test_remove_position_logs_stack_and_still_removes(posfile, caplog):
    with caplog.at_level(logging.WARNING, logger="fx.state"):
        assert S.remove_position("a1") is True
    txt = caplog.text
    assert "[state-forensic] remove_position pos_id=a1" in txt
    assert "调用栈" in txt and "写入后以下pos_id将消失" in txt
    assert _ids(posfile) == ["b2"]                      # 行为不变


def test_update_to_closed_logs_transition(posfile, caplog):
    with caplog.at_level(logging.WARNING, logger="fx.state"):
        assert S.update_position("a1", {"status": "closed", "close_reason": "t"}) is True
    assert "a1 XUSDT status open→closed close_reason=t" in caplog.text
    assert _ids(posfile) == ["a1", "b2"]                # 只是改状态，不丢记录
    assert "pos_id将消失" not in caplog.text


def test_update_other_field_no_forensic(posfile, caplog):
    with caplog.at_level(logging.WARNING, logger="fx.state"):
        S.update_position("a1", {"sl_order_id": 5})
    assert "state-forensic" not in caplog.text


def test_closed_to_closed_not_logged(posfile, caplog):
    S.update_position("a1", {"status": "closed"})
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="fx.state"):
        S.update_position("a1", {"status": "closed", "close_reason": "again"})
    assert "→closed" not in caplog.text


def test_add_position_no_loss_log(posfile, caplog):
    with caplog.at_level(logging.WARNING, logger="fx.state"):
        assert S.add_position({"pos_id": "c3", "symbol": "ZUSDT", "status": "open"}) is True
    assert "pos_id将消失" not in caplog.text
    assert _ids(posfile) == ["a1", "b2", "c3"]


def test_forensic_failure_never_breaks_remove(posfile, monkeypatch):
    def boom():
        raise RuntimeError("boom")
    monkeypatch.setattr(S, "_forensic_stack", boom)
    assert S.remove_position("a1") is True               # 取证代码抛错也不影响删除
    assert _ids(posfile) == ["b2"]
