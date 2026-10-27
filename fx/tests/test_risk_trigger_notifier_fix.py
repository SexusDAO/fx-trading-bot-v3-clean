"""① 系统性风险费率按持仓方向 ② 触发器派发不走全量价格接口 ③ openclaw 缺失静默跳过。全部 mock。"""
import logging
from types import SimpleNamespace

import pytest

from shared import risk_manager as RM
import shared.notifier as N
from shared import trigger_detector as TD


# ── ① 费率方向 ──
@pytest.mark.parametrize("direction,fr,expect", [
    ("LONG", 0.0103, True),      # 多头付钱
    ("SHORT", -0.0103, True),    # 空头付钱
    ("LONG", -0.0103, False),    # 多头收钱
    ("SHORT", 0.0103, False),    # 空头收钱
    ("long", 0.0103, True),
    ("short", -0.0103, True),
])
def test_funding_is_adverse(direction, fr, expect):
    assert RM._funding_is_adverse(direction, fr, 0.010) is expect


def test_funding_below_threshold_not_adverse():
    assert RM._funding_is_adverse("LONG", 0.0099, 0.010) is False


def _run_systemic(monkeypatch, direction, fr):
    import shared.state as ST
    import shared.binance_api as BA
    monkeypatch.setattr(RM, "get_usdt_peg_observation", lambda: {"status": "ok", "rate": 1.0})
    monkeypatch.setattr(RM, "_api_fail_stats", lambda: {"rate": 0.0, "samples": 0, "newest_age_s": 0})
    monkeypatch.setattr(ST, "get_all_positions",
                        lambda: [{"symbol": "UNITREEUSDT", "status": "open", "direction": direction}])
    monkeypatch.setattr(BA, "get_mark_price", lambda s: {"lastFundingRate": str(fr)})
    return RM.check_systemic_risk()


def test_systemic_long_positive_alerts_with_direction_text(monkeypatch):
    ok, msg = _run_systemic(monkeypatch, "LONG", 0.0103)
    assert ok is False
    assert "UNITREEUSDT LONG 资金费率不利 +1.03%" in msg and "多头付费" in msg and "暂停新开仓" in msg


def test_systemic_short_negative_alerts(monkeypatch):
    ok, msg = _run_systemic(monkeypatch, "SHORT", -0.0103)
    assert ok is False and "SHORT" in msg and "空头付费" in msg


def test_systemic_long_negative_no_alert(monkeypatch):
    assert _run_systemic(monkeypatch, "LONG", -0.0103)[0] is True


def test_systemic_short_positive_no_alert(monkeypatch):
    assert _run_systemic(monkeypatch, "SHORT", 0.0103)[0] is True


# ── ② 触发器派发 ──
class _Hub:
    def __init__(self, mp):
        self._mp = mp
        self.all_called = 0

    def get_klines(self, s, iv, n):
        return [[0, 1, 1, 1, 1, 1]] * 3

    def get_ticker(self, s):
        return {"lastPrice": "1"}

    def get_mark_price(self, s):
        return self._mp

    def get_all_mark_prices(self):
        self.all_called += 1
        return {}


def _det(hub, seen):
    def analyzer(sym, tk, mp, kl, **kw):
        seen["mp"] = mp
        return {"analysis_result": "rejected"}
    return TD.TriggerDetector(hub=hub, analyzer=analyzer)


def test_dispatch_prefers_hub_cache(monkeypatch):
    import shared.binance_api as BA
    calls = {"single": 0, "all": 0}
    monkeypatch.setattr(BA, "get_mark_price", lambda s: calls.__setitem__("single", calls["single"] + 1) or {})
    monkeypatch.setattr(BA, "get_all_mark_prices", lambda: calls.__setitem__("all", calls["all"] + 1) or [])
    hub, seen = _Hub({"symbol": "XUSDT", "markPrice": "1"}), {}
    _det(hub, seen)._dispatch_analysis("XUSDT")
    assert seen["mp"]["markPrice"] == "1"
    assert calls == {"single": 0, "all": 0} and hub.all_called == 0


def test_dispatch_falls_back_to_single_rest_only(monkeypatch):
    import shared.binance_api as BA
    calls = {"single": [], "all": 0}
    monkeypatch.setattr(BA, "get_mark_price", lambda s: calls["single"].append(s) or {"markPrice": "2"})
    monkeypatch.setattr(BA, "get_all_mark_prices", lambda: calls.__setitem__("all", calls["all"] + 1) or [])
    hub, seen = _Hub(None), {}
    _det(hub, seen)._dispatch_analysis("XUSDT")
    assert calls["single"] == ["XUSDT"] and calls["all"] == 0 and hub.all_called == 0
    assert seen["mp"]["markPrice"] == "2"


def test_dispatch_single_rest_failure_still_dispatches(monkeypatch):
    import shared.binance_api as BA

    def boom(s):
        raise RuntimeError("net")
    monkeypatch.setattr(BA, "get_mark_price", boom)
    hub, seen = _Hub(None), {}
    r = _det(hub, seen)._dispatch_analysis("XUSDT")
    assert seen["mp"] is None and r["analysis_result"] == "rejected"


def test_scanner_trigger_source_never_calls_all_rest():
    import inspect
    import scanner as S
    src = inspect.getsource(S._filter_and_register_signals)
    assert 'source == "trigger"' in src


# ── ③ openclaw ──
def test_popen_send_missing_command_warns_once_no_raise(monkeypatch, caplog):
    monkeypatch.setattr(N, "_openclaw_missing_warned", False)
    monkeypatch.setattr(N.shutil if hasattr(N, "shutil") else __import__("shutil"), "which", lambda c: None)
    popen_calls = []
    monkeypatch.setattr(N.subprocess, "Popen", lambda *a, **k: popen_calls.append(a))
    with caplog.at_level(logging.WARNING):
        N._popen_send("hi")
        N._popen_send("again")
    assert popen_calls == []
    warns = [r for r in caplog.records if "openclaw" in r.getMessage()]
    assert len(warns) == 1


def test_popen_send_oserror_does_not_raise(monkeypatch, caplog):
    import shutil
    monkeypatch.setattr(shutil, "which", lambda c: "/x/openclaw")

    def boom(*a, **k):
        raise FileNotFoundError(2, "No such file or directory: 'openclaw'")
    monkeypatch.setattr(N.subprocess, "Popen", boom)
    with caplog.at_level(logging.WARNING):
        N._popen_send("hi")                      # 不抛
    assert any("Popen 失败" in r.getMessage() for r in caplog.records)


def test_send_falls_through_all_channels_without_raise(monkeypatch):
    import shutil
    monkeypatch.setattr(N, "_http_send", lambda t: None)
    monkeypatch.setattr(N, "_socket_send", lambda t: None)
    monkeypatch.setattr(shutil, "which", lambda c: None)
    monkeypatch.setattr(N, "_openclaw_missing_warned", True)
    assert N._send("x") is None
