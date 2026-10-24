"""改动A：拒绝通知 4h 去重；改动B：notifier._http_edit 失败日志增强。"""
import asyncio
import logging

import pytest

import executor as E
import shared.notifier as N


@pytest.fixture(autouse=True)
def _clean_state():
    E._reject_notify_last.clear()
    yield
    E._reject_notify_last.clear()


@pytest.fixture
def calls(monkeypatch):
    c = {"append": [], "remove": [], "notify": []}

    async def _append(sid, line):
        c["append"].append((sid, line))

    async def _notify(text):
        c["notify"].append(text)

    monkeypatch.setattr(E, "card_append_async", _append)
    monkeypatch.setattr(E, "notify_async", _notify)
    monkeypatch.setattr(E, "card_remove", lambda sid: c["remove"].append(sid))
    return c


def _run(sig, title="止损计算拒绝", reason_key="sl_tp_reject"):
    asyncio.run(E._reject_card(sig, title, "detail", reason_key=reason_key))


def test_same_symbol_same_reason_pushed_once_within_4h(calls):
    sig = {"symbol": "AAAUSDT", "_sig_id": "s1"}
    _run(sig)
    _run({"symbol": "AAAUSDT", "_sig_id": "s2"})
    assert len(calls["append"]) == 1
    # 被去重的那次仍清理卡片缓存，避免残留
    assert calls["remove"] == ["s1", "s2"]


def test_push_again_after_4h(calls, monkeypatch):
    _run({"symbol": "AAAUSDT", "_sig_id": "s1"})
    key = "AAAUSDT:sl_tp_reject"
    E._reject_notify_last[key] -= E._REJECT_NOTIFY_TTL_SEC + 1
    _run({"symbol": "AAAUSDT", "_sig_id": "s2"})
    assert len(calls["append"]) == 2


def test_still_suppressed_just_before_4h(calls):
    _run({"symbol": "AAAUSDT", "_sig_id": "s1"})
    E._reject_notify_last["AAAUSDT:sl_tp_reject"] -= E._REJECT_NOTIFY_TTL_SEC - 60
    _run({"symbol": "AAAUSDT", "_sig_id": "s2"})
    assert len(calls["append"]) == 1


def test_different_symbol_or_reason_not_deduped(calls):
    _run({"symbol": "AAAUSDT", "_sig_id": "s1"})
    _run({"symbol": "BBBUSDT", "_sig_id": "s2"})
    _run({"symbol": "AAAUSDT", "_sig_id": "s3"}, "盈亏比不足", "rr_insufficient")
    assert len(calls["append"]) == 3


def test_standalone_message_path_also_deduped(calls):
    _run({"symbol": "AAAUSDT"})
    _run({"symbol": "AAAUSDT"})
    assert len(calls["notify"]) == 1 and calls["remove"] == []


def test_no_reason_key_never_deduped(calls):
    for _ in range(2):
        _run({"symbol": "AAAUSDT", "_sig_id": "s"}, reason_key="")
    assert len(calls["append"]) == 2
    assert E._reject_notify_last == {}


def test_failed_push_does_not_record_timestamp(monkeypatch):
    async def _boom(sid, line):
        raise RuntimeError("x")

    monkeypatch.setattr(E, "card_append_async", _boom)
    monkeypatch.setattr(E, "card_remove", lambda sid: None)
    _run({"symbol": "AAAUSDT", "_sig_id": "s1"})
    assert E._reject_notify_last == {}   # 推送失败不占去重额度


def test_all_five_call_sites_pass_known_reason_keys():
    import inspect
    src = inspect.getsource(E)
    for k in ("sl_tp_reject", "rr_insufficient", "min_notional", "liquidity_gate", "account_check"):
        assert f'reason_key="{k}"' in src, k


# ───────────── 改动B ─────────────
class _Resp:
    def __init__(self, code, text):
        self.status_code = code
        self.text = text


def test_http_edit_non_200_logs_status_and_truncated_body(monkeypatch, caplog):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _Resp(500, "E" * 500))
    with caplog.at_level(logging.WARNING):
        assert N._http_edit("m1", "t") is False
    rec = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert rec and "500" in rec[0].getMessage()
    assert "E" * 200 in rec[0].getMessage() and "E" * 201 not in rec[0].getMessage()


def test_http_edit_200_returns_true_without_warning(monkeypatch, caplog):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _Resp(200, "ok"))
    with caplog.at_level(logging.WARNING):
        assert N._http_edit("m1", "t") is True
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_http_edit_exception_logged_as_warning(monkeypatch, caplog):
    import requests

    def _raise(*a, **k):
        raise requests.exceptions.ConnectionError("down")

    monkeypatch.setattr(requests, "post", _raise)
    with caplog.at_level(logging.WARNING):
        assert N._http_edit("m1", "t") is False
    assert [r for r in caplog.records if r.levelno == logging.WARNING and "down" in r.getMessage()]
