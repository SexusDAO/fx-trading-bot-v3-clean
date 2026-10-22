"""maintain_pending_order 优先读 hub（标记价/K线），OI 仍走 REST；判断逻辑不变（全 mock）。"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

from shared import market_data_hub as H
import scanner as sc

POS = {"symbol": "XUSDT", "direction": "LONG", "sig_type": "pullback", "last_funding": 0.0001, "oi_snapshot": 1000}


def kl(closes, step=900_000):
    return [[i * step, "1", "2", "0.5", str(c), "10", i * step + step - 1, "15", 5, "4", "6", "0"] for i, c in enumerate(closes)]


class Rec:
    def __init__(self): self.rest = []


@pytest.fixture
def env(monkeypatch):
    import config
    r = Rec()
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", True, raising=False)
    monkeypatch.setattr(sc.api, "get_klines", lambda s, iv, n: r.rest.append(("klines", s, iv, n)) or kl([100] * n))
    monkeypatch.setattr(sc.api, "get_mark_price", lambda s: r.rest.append(("mark", s)) or {"lastFundingRate": "0.0001"})
    monkeypatch.setattr(sc.api, "get_open_interest", lambda s: r.rest.append(("oi", s)) or {"openInterest": "1000"})
    monkeypatch.setattr(H, "_default", None)
    return r


def hub_with(monkeypatch, klines=None, mark=None):
    """假 hub：按 (symbol, interval) 返回 K 线，按 symbol 返回标记价。"""
    klines = klines or {}
    mark = mark or {}
    monkeypatch.setattr(H, "get_klines", lambda s, iv, n: klines.get((s, iv)))
    monkeypatch.setattr(H, "is_ready", lambda: True)
    monkeypatch.setattr(H, "get_mark_price", lambda s: mark.get(s))


def test_uses_hub_for_klines_and_mark_price_but_oi_stays_rest(env, monkeypatch):
    hub_with(monkeypatch,
             klines={("XUSDT", "1h"): kl([100] * 30, 3_600_000), ("XUSDT", "15m"): kl([100] * 20), ("BTCUSDT", "15m"): kl([100, 100, 100])},
             mark={"XUSDT": {"lastFundingRate": "0.0001", "age_sec": 0.5}})
    sc.maintain_pending_order(dict(POS), {})
    assert [x[0] for x in env.rest] == ["oi"]                      # 只剩 OI 走 REST


def test_falls_back_to_rest_when_hub_returns_none(env, monkeypatch):
    hub_with(monkeypatch)                                          # hub 什么都没有
    sc.maintain_pending_order(dict(POS), {})
    kinds = [(x[0], x[1:]) for x in env.rest]
    assert ("klines", ("XUSDT", "1h", 30)) in kinds and ("klines", ("XUSDT", "15m", 20)) in kinds
    assert ("klines", ("BTCUSDT", "15m", 3)) in kinds and ("mark", ("XUSDT",)) in kinds and ("oi", ("XUSDT",)) in kinds


def test_flag_off_never_touches_hub(env, monkeypatch):
    import config
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", False, raising=False)
    for n in ("get_klines", "get_mark_price", "is_ready"):
        monkeypatch.setattr(H, n, lambda *a, **k: (_ for _ in ()).throw(AssertionError("不应调用 hub")))
    sc.maintain_pending_order(dict(POS), {})
    assert {x[0] for x in env.rest} == {"klines", "mark", "oi"}


def test_hub_exception_falls_back_to_rest(env, monkeypatch):
    monkeypatch.setattr(H, "get_klines", lambda *a: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(H, "is_ready", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    sc.maintain_pending_order(dict(POS), {})
    assert {x[0] for x in env.rest} == {"klines", "mark", "oi"}


# ── 判断逻辑不变：用 hub 数据得到与 REST 数据相同的结论 ───────────────────────
def test_btc_move_cancel_decision_from_hub_data(env, monkeypatch):
    monkeypatch.setattr(sc, "ORDER_CANCEL_BTC_MOVE_PCT", 3.0, raising=False)
    hub_with(monkeypatch,
             klines={("XUSDT", "1h"): kl([100] * 30, 3_600_000), ("XUSDT", "15m"): kl([100] * 20), ("BTCUSDT", "15m"): kl([100, 100, 104])},
             mark={"XUSDT": {"lastFundingRate": "0.0001"}})
    assert sc.maintain_pending_order(dict(POS), {}) == "cancel"
    assert not any(x[0] == "oi" for x in env.rest)                 # 在 OI 之前就已撤单，行为与原来一致


def test_funding_delta_cancel_decision_from_hub_mark_price(env, monkeypatch):
    monkeypatch.setattr(sc, "ORDER_CANCEL_FUNDING_DELTA", 0.002, raising=False)
    hub_with(monkeypatch,
             klines={("XUSDT", "1h"): kl([100] * 30, 3_600_000), ("XUSDT", "15m"): kl([100] * 20), ("BTCUSDT", "15m"): kl([100, 100, 100])},
             mark={"XUSDT": {"lastFundingRate": "0.0100"}})        # 与 last_funding=0.0001 相差 0.99%
    assert sc.maintain_pending_order(dict(POS), {}) == "cancel"


def test_oi_drop_cancel_still_uses_rest_oi(env, monkeypatch):
    monkeypatch.setattr(sc, "ORDER_CANCEL_OI_DROP_PCT", 15.0, raising=False)
    monkeypatch.setattr(sc.api, "get_open_interest", lambda s: {"openInterest": "700"})   # 1000 -> 700，降 30%
    hub_with(monkeypatch,
             klines={("XUSDT", "1h"): kl([100] * 30, 3_600_000), ("XUSDT", "15m"): kl([100] * 20), ("BTCUSDT", "15m"): kl([100, 100, 100])},
             mark={"XUSDT": {"lastFundingRate": "0.0001"}})
    assert sc.maintain_pending_order(dict(POS), {}) == "cancel"


def test_hub_debug_logs_when_used(env, monkeypatch, caplog):
    hub_with(monkeypatch, klines={("XUSDT", "1h"): kl([100] * 30, 3_600_000)})
    with caplog.at_level(logging.DEBUG):
        sc.maintain_pending_order(dict(POS), {})
    assert any("[hub] XUSDT 1h klines n=30" in r.getMessage() for r in caplog.records)


def test_no_remaining_direct_rest_for_price_and_klines_in_function():
    src = (Path(__file__).parent.parent / "scanner.py").read_text()
    i = src.index("def maintain_pending_order")
    body = src[i:src.index("# ── 候选池刷新", i)]
    assert "api.get_klines" not in body and "api.get_mark_price" not in body
    assert "api.get_open_interest(symbol)" in body                 # OI 保留 REST
