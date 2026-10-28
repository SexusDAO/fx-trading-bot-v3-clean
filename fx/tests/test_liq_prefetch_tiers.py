"""清算图预热分档（持仓优先 + 候选池前N）与 cg_get 429 指数退避。全部 mock，不联网。"""
import time

import pytest

from shared import liq_cache as L
from shared import clawby_adapter as C


def POOL(n=165):
    return [f"S{i}USDT" for i in range(n)]


@pytest.fixture
def positions(monkeypatch):
    holder = {"pos": []}
    monkeypatch.setattr(L, "_position_symbols", lambda: list(holder["pos"]))
    monkeypatch.setattr(L, "LIQ_PREFETCH_MAX_SYMBOLS", 60)  # [v3.95] 候选池扩大到60
    monkeypatch.setattr(L, "LIQ_PREFETCH_PRIORITY_POSITIONS", True)
    return holder


def test_config_defaults():
    import config
    assert config.LIQ_PREFETCH_MAX_SYMBOLS == 60 and config.LIQ_PREFETCH_PRIORITY_POSITIONS is True  # [v3.95] 30→60


def test_positions_first_then_top_n_and_request_count(positions):
    positions["pos"] = [f"P{i}USDT" for i in range(7)]
    syms, st = L.select_prefetch_symbols(POOL())
    assert syms[:7] == positions["pos"] and syms[7:] == POOL()[:60]  # [v3.95] 上限60
    # [v3.95] _PREFETCH_RANGES 缩减为1个("7d")，节省2/3 CG配额；上限60
    assert len(syms) == 67 and len(syms) * len(L._PREFETCH_RANGES) == 67   # 67×1=67
    assert st["positions"] == 7 and st["tier2"] == 60 and st["skipped"] == 165 - 60  # [v3.95]


def test_position_symbol_in_pool_does_not_take_extra_quota(positions):
    positions["pos"] = ["S3USDT", "S100USDT"]                                 # 一个在前30、一个在池尾
    syms, _ = L.select_prefetch_symbols(POOL())
    assert syms[:2] == ["S3USDT", "S100USDT"]
    assert len(syms) == 2 + 60 and syms.count("S3USDT") == 1  # [v3.95] 上限60
    assert "S29USDT" in syms and "S30USDT" in syms                               # 前30里去掉 S3 后顺延补位，仍凑满30


def test_positions_not_limited_by_cap(positions):
    positions["pos"] = [f"P{i}USDT" for i in range(40)]
    syms, _ = L.select_prefetch_symbols(POOL())
    assert syms[:40] == positions["pos"] and len(syms) == 100  # [v3.95] 40持仓+60上限=100


def test_wanted_symbols_count_inside_cap_and_go_before_pool(positions):
    syms, _ = L.select_prefetch_symbols(POOL(), wanted=["W1USDT", "W2USDT"])
    assert syms[:2] == ["W1USDT", "W2USDT"] and len(syms) == 60 and syms[2] == "S0USDT"  # [v3.95] 上限60


def test_no_positions_and_empty_pool(positions):
    assert L.select_prefetch_symbols([])[0] == []
    syms, st = L.select_prefetch_symbols(POOL(10))
    assert syms == POOL(10) and st["skipped"] == 0


def test_priority_switch_off_restores_full_pool(positions, monkeypatch):
    monkeypatch.setattr(L, "LIQ_PREFETCH_PRIORITY_POSITIONS", False)
    syms, _ = L.select_prefetch_symbols(POOL(), wanted=["W1USDT"])
    assert syms == ["W1USDT"] + POOL()


def test_position_read_failure_falls_back_to_pool_only(monkeypatch):
    import shared.state as S
    monkeypatch.setattr(L, "LIQ_PREFETCH_MAX_SYMBOLS", 30)
    monkeypatch.setattr(L, "LIQ_PREFETCH_PRIORITY_POSITIONS", True)
    monkeypatch.setattr(S, "get_all_positions", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert L._position_symbols() == []
    assert len(L.select_prefetch_symbols(POOL())[0]) == 30


def test_position_symbols_only_live_statuses(monkeypatch):
    import shared.state as S
    monkeypatch.setattr(S, "get_all_positions", lambda: [
        {"symbol": "A", "status": "open"}, {"symbol": "B", "status": "pending_guard"},
        {"symbol": "C", "status": "pending"}, {"symbol": "D", "status": "closed"}, {"status": "open"}])
    assert L._position_symbols() == ["A", "B"]


def test_prefetch_one_skips_during_backoff_without_requests(monkeypatch):
    monkeypatch.setattr(L, "_CACHE", {})
    monkeypatch.setattr(L, "_cg_backoff_left", lambda: 30.0)
    called = []
    monkeypatch.setattr(L, "_fetch_with_timeout", lambda *a, **k: called.append(1) or {})
    assert L._prefetch_one("AUSDT", price=1.0) is False and called == []


# ── cg_get 指数退避 ──
class Resp:
    def __init__(self, code, headers=None, body=None):
        self.status_code, self.headers, self._b = code, headers or {}, body

    def json(self):
        return self._b


@pytest.fixture
def cg(monkeypatch):
    monkeypatch.setattr(C, "_CG_429_UNTIL", 0.0)
    monkeypatch.setattr(C, "_CG_429_COUNT", 0)
    monkeypatch.setattr(C, "_wait_cg_rate_limit", lambda: None)
    sleeps = []
    monkeypatch.setattr(C.time, "sleep", lambda s: sleeps.append(s))
    calls = []
    q = {"resp": []}

    def fake_get(url, **k):
        calls.append(url)
        return q["resp"].pop(0)

    monkeypatch.setattr(C.requests, "get", fake_get)
    return calls, q, sleeps


def test_backoff_sequence_10_20_40_60_60(monkeypatch):
    monkeypatch.setattr(C, "_CG_429_UNTIL", 0.0)
    monkeypatch.setattr(C, "_CG_429_COUNT", 0)
    out = []
    for _ in range(5):
        out.append(C._cg_note_429())
        monkeypatch.setattr(C, "_CG_429_UNTIL", 0.0)               # 模拟上一轮退避已过期（退避期内的重复 429 不再升级，见 test_cg_backoff_global.py）
    assert out == [10, 20, 40, 60, 60]


def test_retry_after_longer_wins_but_capped_at_60(monkeypatch):
    monkeypatch.setattr(C, "_CG_429_UNTIL", 0.0)
    monkeypatch.setattr(C, "_CG_429_COUNT", 0)
    assert C._cg_note_429(25) == 25
    monkeypatch.setattr(C, "_CG_429_UNTIL", 0.0)
    assert C._cg_note_429(500) == 60


def test_429_returns_none_does_not_sleep_or_retry_and_blocks_next_requests(cg):
    calls, q, sleeps = cg
    q["resp"] = [Resp(429)]
    assert C.cg_get("api/x") is None
    assert len(calls) == 1 and sleeps == []                       # 不再原地 sleep+重试
    assert 0 < C.cg_backoff_remaining() <= 10
    assert C.cg_get("api/y") is None and len(calls) == 1          # 退避期内不发新请求


def test_consecutive_429_escalates_and_success_resets(cg, monkeypatch):
    calls, q, _ = cg
    q["resp"] = [Resp(429)]
    C.cg_get("a")
    monkeypatch.setattr(C, "_CG_429_UNTIL", 0.0)                  # 模拟退避期已过
    q["resp"] = [Resp(429)]
    C.cg_get("a")
    assert 10 < C.cg_backoff_remaining() <= 20                    # 第 2 次 20s
    monkeypatch.setattr(C, "_CG_429_UNTIL", 0.0)
    q["resp"] = [Resp(200, body={"code": "0", "data": [1]})]
    C.cg_get("a")
    assert C._CG_429_COUNT == 0                                   # 成功清零
    q["resp"] = [Resp(429)]
    C.cg_get("a")
    assert C.cg_backoff_remaining() <= 10                         # 重新从 10s 起


def test_backoff_expires_then_requests_resume(cg, monkeypatch):
    calls, q, _ = cg
    q["resp"] = [Resp(429)]
    C.cg_get("a")
    monkeypatch.setattr(C, "_CG_429_UNTIL", time.time() - 1)
    q["resp"] = [Resp(200, body={"code": "0", "data": {"k": 1}})]
    C.cg_get("a")
    assert len(calls) == 2
