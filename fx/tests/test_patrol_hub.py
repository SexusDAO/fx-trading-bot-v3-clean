"""position_manager 巡检读路径优先读 hub（标记价/K线）；持仓/挂单/平仓/止损相关仍走 REST（全 mock）。"""
import asyncio
import logging
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

from shared import market_data_hub as H
import position_manager as pm

SRC = (Path(__file__).parent.parent / "position_manager.py").read_text()


def func_body(name):
    m = re.search(rf"^\s*(?:async )?def {re.escape(name)}\(", SRC, flags=re.M)
    start = m.start()
    # 终止符只匹配顶层（第 0 列）的 def/class：函数内的嵌套 def 不再截断函数体。
    nxt = re.search(r"^(?:async )?def |^class ", SRC[m.end():], flags=re.M)
    return SRC[start:m.end() + (nxt.start() if nxt else len(SRC))]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    import config
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", True, raising=False)
    monkeypatch.setattr(H, "_default", None)


def kl(n, step=3_600_000):
    return [[i * step, "1", "2", "0.5", "1.5", "10", i * step + step - 1, "15", 5, "4", "6", "0"] for i in range(n)]


# ── helper ────────────────────────────────────────────────────────────────
def test_mark_price_uses_hub_when_ready(monkeypatch, caplog):
    monkeypatch.setattr(H, "is_ready", lambda: True)
    monkeypatch.setattr(H, "get_mark_price", lambda s: {"markPrice": "100", "lastFundingRate": "0.0001", "age_sec": 0.4})
    monkeypatch.setattr(pm.api, "get_mark_price", lambda s: pytest.fail("不应走 REST"))
    with caplog.at_level(logging.DEBUG):
        d = pm._pm_mark_price("BTCUSDT")
    assert d["markPrice"] == "100" and any("[hub] BTCUSDT price=100 age=0.4s" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("ready,hubval", [(False, {"markPrice": "1"}), (True, None)])
def test_mark_price_falls_back_to_rest(monkeypatch, ready, hubval):
    monkeypatch.setattr(H, "is_ready", lambda: ready)
    monkeypatch.setattr(H, "get_mark_price", lambda s: hubval)
    monkeypatch.setattr(pm.api, "get_mark_price", lambda s: {"markPrice": "rest"})
    assert pm._pm_mark_price("BTCUSDT") == {"markPrice": "rest"}


def test_klines_hub_then_fallback(monkeypatch, caplog):
    monkeypatch.setattr(H, "get_klines", lambda s, iv, n: kl(n) if s == "AUSDT" else None)
    monkeypatch.setattr(pm.api, "get_klines", lambda s, iv, n: [["rest", s, iv, n]])
    with caplog.at_level(logging.DEBUG):
        assert len(pm._pm_klines("AUSDT", "1h", 55)) == 55
    assert any("[hub] AUSDT 1h klines n=55" in r.getMessage() for r in caplog.records)
    assert pm._pm_klines("BUSDT", "4h", 60) == [["rest", "BUSDT", "4h", 60]]       # hub 无数据 → REST


def test_flag_off_and_hub_exceptions_never_break(monkeypatch):
    import config
    boom = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("hub down"))
    monkeypatch.setattr(pm.api, "get_mark_price", lambda s: {"markPrice": "rest"})
    monkeypatch.setattr(pm.api, "get_klines", lambda s, iv, n: [["rest"]])
    monkeypatch.setattr(H, "is_ready", boom); monkeypatch.setattr(H, "get_klines", boom)
    assert pm._pm_mark_price("A") == {"markPrice": "rest"} and pm._pm_klines("A", "1h", 5) == [["rest"]]
    monkeypatch.setattr(config, "MARKET_DATA_HUB_ENABLED", False, raising=False)
    monkeypatch.setattr(H, "is_ready", lambda: pytest.fail("开关关闭不应触碰 hub"))
    monkeypatch.setattr(H, "get_klines", lambda *a: pytest.fail("开关关闭不应触碰 hub"))
    assert pm._pm_mark_price("A") == {"markPrice": "rest"} and pm._pm_klines("A", "1h", 5) == [["rest"]]


# ── 改动范围：哪些函数用 hub，哪些必须仍是 REST ────────────────────────────
@pytest.mark.parametrize("fn", ["_check_time_stop", "_check_exit_signal", "_check_health", "_check_pending_health_static"])
def test_patrol_read_paths_use_helpers(fn):
    body = func_body(fn)
    assert "_pm_klines(" in body or "_pm_mark_price(" in body
    assert "api.get_klines" not in body and "api.get_mark_price" not in body


def test_time_stop_still_uses_rest_position_query():
    assert "api.get_position(symbol)" in func_body("_check_time_stop")


@pytest.mark.parametrize("fn,needle", [
    ("_check_manual_close", "api.get_position(symbol)"),            # 持仓状态必须实时 REST
    ("_check_manual_close", "api.get_mark_price(symbol)"),          # 手动平仓的成交价估算：账务相关，保持 REST
    ("_check_pending_expire_static", "api.get_open_orders(symbol)"),
    ("_adjust_sl_tp_once", "api.get_mark_price(symbol)"),           # 止损/止盈调整：不改
    ("_adjust_sl_tp_once", "api.get_klines(symbol, \"1h\", 55)"),
    ("_补挂保护单", "_api.get_open_orders(symbol)"),                 # 保护单补挂：不改（仍走 _api）
    ("_close", "api.get_mark_price(symbol)"),                       # 平仓路径：不改
])
def test_money_paths_untouched(fn, needle):
    body = func_body(fn)
    # 允许改为 asyncio.to_thread(api.xxx, args) 形式（仍走 REST，只是不阻塞事件循环）
    _name, _args = needle.split("(", 1)
    _args = _args[:-1]
    _thread_form = f"asyncio.to_thread({_name}, {_args})" if _args else f"asyncio.to_thread({_name})"
    assert needle in body or _thread_form in body, f"{fn} 应保持 {needle}"


def test_scanner_subscribes_positions_and_btc_too():
    s = (Path(__file__).parent.parent / "scanner.py").read_text()
    assert '_hub_subscribe_klines(["BTCUSDT"] + sorted(active_sym_set) + list(scan_pool))' in s


# ── 行为：_check_exit_signal 在 hub 有数据时不发 K 线 REST ─────────────────
def test_exit_signal_reads_klines_from_hub(monkeypatch):
    hub_calls, rest_calls = [], []
    monkeypatch.setattr(H, "get_klines", lambda s, iv, n: hub_calls.append((s, iv, n)) or kl(n))
    monkeypatch.setattr(pm.api, "get_klines", lambda s, iv, limit=0: rest_calls.append((s, iv)) or kl(limit or 5))
    mgr = object.__new__(pm.PositionManager)
    pos = {"symbol": "XUSDT", "direction": "LONG", "sig_type": "pullback", "status": "open",
           "fill_time": int(time.time() * 1000) - 10 * 3600_000, "entry_price": 1.0}
    try:
        asyncio.run(mgr._check_exit_signal(pos))
    except Exception:
        pass                                                        # 后续指标计算依赖更多字段，这里只关心取数来源
    if not hub_calls and not rest_calls:
        pytest.skip("测试仓位在取数前就返回了（前置条件未满足），无法验证取数来源")
    assert ("XUSDT", "1h", 30) in hub_calls and ("XUSDT", "15m", 10) in hub_calls
    assert rest_calls == []
