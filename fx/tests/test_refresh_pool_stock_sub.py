"""refresh_pool 订阅 hub 时：休市的股票代币不订阅，持仓中的股票代币保留，非股票代币不受影响。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import scanner as sc
from test_scan_pool_scoring import env, set_market, tk  # noqa: F401  复用 refresh_pool 的 mock 环境


def _run(env, monkeypatch, held, closed):
    monkeypatch.setattr(sc, "_is_stock_token_tradeable", lambda s: s not in closed)
    monkeypatch.setattr(sc, "get_all_positions", lambda: [{"symbol": x, "status": "open"} for x in held])
    seen = []
    monkeypatch.setattr(sc, "_hub_subscribe_klines", lambda syms: seen.append(list(syms)))
    set_market(env, [tk("AAAUSDT"), tk("MINIMAXUSDT"), tk("HK0700USDT")])
    pool = sc.refresh_pool()
    return pool, seen[-1]


def test_closed_stock_tokens_not_subscribed(env, monkeypatch):
    pool, subs = _run(env, monkeypatch, held=[], closed={"MINIMAXUSDT", "HK0700USDT"})
    assert set(pool) == {"AAAUSDT", "MINIMAXUSDT", "HK0700USDT"}     # 候选池本身不变
    assert "MINIMAXUSDT" not in subs and "HK0700USDT" not in subs and "AAAUSDT" in subs
    assert subs[0] == "BTCUSDT"


def test_held_closed_stock_token_still_subscribed(env, monkeypatch):
    _, subs = _run(env, monkeypatch, held=["MINIMAXUSDT"], closed={"MINIMAXUSDT", "HK0700USDT"})
    assert "MINIMAXUSDT" in subs and "HK0700USDT" not in subs


def test_all_open_subscribes_everything(env, monkeypatch):
    _, subs = _run(env, monkeypatch, held=[], closed=set())
    assert {"AAAUSDT", "MINIMAXUSDT", "HK0700USDT"} <= set(subs)
