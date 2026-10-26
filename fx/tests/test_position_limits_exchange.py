"""开仓前持仓上限接入交易所实际数量：取 max(本地, 交易所)；查询在线程池、失败 fallback 本地并告警。全部 mock。"""
import asyncio
import logging
from unittest.mock import patch

import pytest

from shared import risk_manager as rm
import shared.state as st


def P(side, amt="1.0"):
    return {"symbol": "X", "positionSide": side, "positionAmt": amt}


def test_exchange_position_counts():
    c = rm.exchange_position_counts([P("LONG"), P("SHORT"), P("SHORT", "-2"), P("LONG", "0"), P("BOTH"), {"positionAmt": "x"}])
    assert c == {"total": 3, "long": 1, "short": 2}
    assert rm.exchange_position_counts(None) == {"total": 0, "long": 0, "short": 0}


@pytest.fixture
def empty_local(monkeypatch):
    monkeypatch.setattr(st, "get_position_count", lambda: {"total": 0, "long": 0, "short": 0})
    monkeypatch.setattr(st, "get_pending_count", lambda: {"total": 0, "long": 0, "short": 0})


def test_local_empty_but_exchange_full_blocks(empty_local):
    with patch.object(rm, "get_market_mode", return_value="bull"), patch("config.MAX_POSITIONS_TOTAL_BULL", 2):
        assert rm.check_position_limits("LONG") is True                                   # 只看本地：放行（旧行为）
        assert rm.check_position_limits("LONG", {"total": 2, "long": 2, "short": 0}) is False


def test_direction_limit_uses_exchange(empty_local):
    with patch.object(rm, "get_market_mode", return_value="bull"), patch("config.MAX_POSITIONS_LONG_BULL", 1):
        assert rm.check_position_limits("LONG", {"total": 1, "long": 1, "short": 0}) is False
        assert rm.check_position_limits("SHORT", {"total": 1, "long": 1, "short": 0}) is True


def test_local_larger_than_exchange_still_wins(monkeypatch):
    monkeypatch.setattr(st, "get_position_count", lambda: {"total": 3, "long": 3, "short": 0})
    monkeypatch.setattr(st, "get_pending_count", lambda: {"total": 0, "long": 0, "short": 0})
    with patch.object(rm, "get_market_mode", return_value="bull"), patch("config.MAX_POSITIONS_TOTAL_BULL", 3):
        assert rm.check_position_limits("LONG", {"total": 1, "long": 1, "short": 0}) is False


def test_none_or_empty_exchange_counts_use_local_only(empty_local):
    with patch.object(rm, "get_market_mode", return_value="bull"):
        assert rm.check_position_limits("LONG", None) is True
        assert rm.check_position_limits("LONG", {}) is True


# ── executor 接线 ──
@pytest.fixture
def ex(monkeypatch):
    import executor as E
    seen = {}

    def fake_precheck(signal):
        seen["counts"] = signal.get("_exchange_pos_counts")
        return False, "stop-here"                       # 预检拒绝即可，不往下走真实下单

    monkeypatch.setattr(E, "_precheck", fake_precheck)
    logging.disable(logging.CRITICAL)
    yield E, seen
    logging.disable(logging.NOTSET)


def run(E, sig=None):
    return asyncio.run(E._execute_async_locked(sig or {"symbol": "XUSDT", "direction": "LONG", "mode": "pullback"}))


def test_executor_passes_exchange_counts_via_executor_thread(ex, monkeypatch):
    E, seen = ex
    import threading
    where = {}

    def gp(symbol=None):
        where["thread"] = threading.current_thread()
        where["arg"] = symbol
        return [P("LONG"), P("SHORT")]

    monkeypatch.setattr(E.api, "get_position", gp)
    run(E)
    assert seen["counts"] == {"total": 2, "long": 1, "short": 1}
    assert where["arg"] == "" and where["thread"] is not threading.main_thread()   # 不在事件循环线程里发请求


def test_executor_query_failure_falls_back_to_local_with_warning(ex, monkeypatch, caplog):
    E, seen = ex

    def boom(symbol=None):
        raise RuntimeError("net down")

    monkeypatch.setattr(E.api, "get_position", boom)
    logging.disable(logging.NOTSET)
    with caplog.at_level(logging.WARNING):
        run(E)
    assert seen["counts"] is None
    assert any("查询交易所实际持仓数失败" in r.getMessage() for r in caplog.records)


def test_executor_does_not_reuse_stale_counts(ex, monkeypatch):
    E, seen = ex
    monkeypatch.setattr(E.api, "get_position", lambda symbol=None: (_ for _ in ()).throw(RuntimeError("x")))
    run(E, {"symbol": "XUSDT", "direction": "LONG", "_exchange_pos_counts": {"total": 99, "long": 99, "short": 0}})
    assert seen["counts"] is None                        # 上一次的旧值不会被沿用


def test_precheck_passes_counts_to_check_position_limits():
    import inspect
    import executor as E
    assert 'check_position_limits(direction, signal.get("_exchange_pos_counts"))' in inspect.getsource(E._precheck)
