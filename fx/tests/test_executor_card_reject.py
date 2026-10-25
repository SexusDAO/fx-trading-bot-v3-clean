import asyncio, inspect
import executor as X
import shared.signal_watcher as SW


def test_delta_limit_includes_max_leverage():
    src = inspect.getsource(X._precheck)
    assert "_max_lev_delta = _get_max_leverage(symbol)" in src
    assert "_delta_limit = _equity * _max_lev_delta * NET_DELTA_MAX_PCT" in src
    assert "×{_max_lev_delta}x×" in src
    assert "_delta_limit = _equity * NET_DELTA_MAX_PCT" not in src


def test_sl_dist_and_qty_rejects_append_card():
    src = inspect.getsource(X._execute_async_locked)
    assert 'reason_key="sl_dist_too_small"' in src and 'reason_key="qty_zero"' in src
    i = src.index("止损距离过小，拒绝开仓")
    assert "return False" in src[i:i + 400]
    j = src.index("数量计算异常，拒绝开仓")
    assert "return False" in src[j:j + 300]


def test_reject_card_appends_and_removes(monkeypatch):
    calls = []
    async def _app(sid, line): calls.append(("append", sid, line)); return True
    monkeypatch.setattr(X, "card_append_async", _app)
    monkeypatch.setattr(X, "card_remove", lambda sid: calls.append(("remove", sid)))
    monkeypatch.setattr(X, "_reject_notify_due", lambda s, k: True)
    asyncio.run(X._reject_card({"_sig_id": "ABC_1", "symbol": "ABCUSDT"}, "数量计算异常，拒绝开仓",
                               "quantity=0", reason_key="qty_zero"))
    assert calls[0][0] == "append" and "数量计算异常" in calls[0][2] and calls[1] == ("remove", "ABC_1")


def test_signal_watcher_sets_sig_id_even_if_card_fails():
    src = inspect.getsource(SW)
    assert 'entry.signal.setdefault(' in src and '"_sig_id"' in src
