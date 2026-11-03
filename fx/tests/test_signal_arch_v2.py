"""[refactor-signal-arch-v2] 粗筛 + 触发后实时精筛：judge 延后开关 / hub 订阅名单 / _trigger 精筛分支。全 mock，不联网。"""
import asyncio
import socket
import sys
import time
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import config
import scanner as sc
from shared import judges as J
from shared import signal_watcher as SW
from test_judge import _k, _trending_k1h, _ind


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def _no_net(*a, **k):
        raise OSError("network disabled in test")
    monkeypatch.setattr(socket.socket, "connect", _no_net)
    monkeypatch.setattr(socket, "create_connection", _no_net)


HAMMER = [_k(50000, 50100, 49800, 50080), _k(50080, 50100, 49600, 50070), _k(50070, 50100, 49300, 50090)]


def _pb(price, defer, k15=HAMMER):
    k1h = _trending_k1h(49000, direction="up")
    k4h = _trending_k1h(47000, direction="up")
    ind = _ind(price=price, atr=600, ema20_1h=50100, ema50_1h=49200, ema20_4h=49500, ema50_4h=48000)
    if defer:
        ind["_defer_rt"] = True
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
        return J.judge_pullback(k1h, k4h, k15, ind, chg24=2.0, funding=0.0003)


# ── judge：开关关闭时与原行为一致 ──────────────────────────────────────────────
def test_rt_defer_off_still_rejects_price_out_of_zone():
    ok, out = _pb(price=53000, defer=False)
    assert ok is False and "不在回踩区间" in str(out.get("reject_reason"))
    assert "deferred_checks" not in out


def test_rt_defer_on_skips_price_in_zone_and_records_it():
    ok, out = _pb(price=53000, defer=True)
    assert ok is True, out.get("reject_reason")
    assert "price_in_zone" in out["deferred_checks"]


def test_rt_defer_does_not_skip_structural_rejects():
    # 4H 空头结构禁止做多：属于粗筛条件，延后模式下也必须拒绝
    k1h = _trending_k1h(49000, direction="up")
    k4h = _trending_k1h(47000, direction="up")
    ind = _ind(price=50000, atr=600, ema20_1h=50100, ema50_1h=49200, ema20_4h=47000, ema50_4h=49500)
    ind["_defer_rt"] = True
    with patch("shared.coinglass_api.get_liq_zones", return_value=[]):
        ok, out = J.judge_pullback(k1h, k4h, HAMMER, ind, chg24=2.0, funding=0.0003)
    assert ok is False


def test_rt_defer_helper_without_flag_is_false():
    out = {}
    assert J._rt_defer({}, out, "x") is False and out == {}
    assert J._rt_defer({"_defer_rt": True}, out, "x") is True and out["deferred_checks"] == ["x"]


# ── scanner：hub 订阅名单 ─────────────────────────────────────────────────────
def test_sub_list_held_and_btc_first_and_deduped(monkeypatch):
    monkeypatch.setattr(config, "HUB_SUBSCRIBE_TOP_N", 300, raising=False)
    monkeypatch.setattr(config, "KLINE_HUB_SYMBOLS_MAX", 300, raising=False)
    got = sc._build_hub_sub_list(["ZUSDT", "BTCUSDT"], ["AUSDT", "ZUSDT", "BUSDT"])
    assert got == ["BTCUSDT", "ZUSDT", "AUSDT", "BUSDT"]


def test_sub_list_top_n_truncates_pool_but_never_held(monkeypatch):
    monkeypatch.setattr(config, "HUB_SUBSCRIBE_TOP_N", 2, raising=False)
    monkeypatch.setattr(config, "KLINE_HUB_SYMBOLS_MAX", 300, raising=False)
    pool = [f"S{i}USDT" for i in range(10)]
    got = sc._build_hub_sub_list(["HELD1USDT", "HELD2USDT"], pool)
    assert got[:3] == ["BTCUSDT", "HELD1USDT", "HELD2USDT"] and got[3:] == pool[:2]


def test_sub_list_total_capped_by_hub_max_held_survive(monkeypatch):
    monkeypatch.setattr(config, "HUB_SUBSCRIBE_TOP_N", 300, raising=False)
    monkeypatch.setattr(config, "KLINE_HUB_SYMBOLS_MAX", 4, raising=False)
    got = sc._build_hub_sub_list(["HUSDT"], [f"S{i}USDT" for i in range(10)])
    assert len(got) == 4 and got[:2] == ["BTCUSDT", "HUSDT"]


def test_config_values():
    assert config.SCAN_POOL_MAX == 300 and config.KLINE_HUB_SYMBOLS_MAX == 300
    assert config.SW_REALTIME_JUDGE is True
    # 调整 1：breakout 不参与实时精筛（触发时最新K线不再是突破K线，会被误杀）
    assert tuple(config.SW_REALTIME_JUDGE_MODES) == ("pullback", "ema_trend", "reversal")
    # 调整 2：hub 无K线默认不放行
    assert config.SW_REALTIME_JUDGE_NO_HUB == "reject"
    # 调整 3：整轮扫描超时 180 -> 300
    assert config.SCAN_TOTAL_TIMEOUT_SEC == 300


def test_breakout_signal_not_marked_for_realtime_check(monkeypatch):
    """breakout 不在名单内：analyze_one 不应给它打 needs_realtime_check，judge 也不应被延后。"""
    modes = tuple(config.SW_REALTIME_JUDGE_MODES)
    assert "breakout" not in modes


def test_main_scan_wait_follows_total_timeout():
    src = (Path(__file__).parent.parent / "main.py").read_text(encoding="utf-8")
    assert "timeout=_scan_wait_sec" in src
    # 整轮分析上限 300 时，main 的等待上限必须更大，否则会先于分析超时到点
    assert max(300, int(config.SCAN_TOTAL_TIMEOUT_SEC) + 120) > config.SCAN_TOTAL_TIMEOUT_SEC


# ── signal_watcher：实时精筛函数 ───────────────────────────────────────────────
def test_realtime_judge_sync_skips_mode_not_listed():
    v, d = SW._realtime_judge_sync({"symbol": "XUSDT", "sig_type": "funding_arb", "direction": "LONG"})
    assert v == "skip"


def test_realtime_judge_sync_skips_when_hub_has_no_klines(monkeypatch):
    class FakeHub:
        def get_klines(self, *a): return None
        def get_mark_price(self, s): return {}
    import shared.market_data_hub as H
    monkeypatch.setattr(H, "get_hub", lambda: FakeHub())
    v, d = SW._realtime_judge_sync({"symbol": "XUSDT", "sig_type": "pullback", "direction": "LONG"})
    assert v == "skip" and "hub" in d


def _fake_hub(monkeypatch):
    k = _trending_k1h(49000, direction="up")

    class FakeHub:
        def get_klines(self, s, iv, n): return list(k)
        def get_mark_price(self, s): return {"markPrice": "50000", "lastFundingRate": "0.0001"}
    import shared.market_data_hub as H
    monkeypatch.setattr(H, "get_hub", lambda: FakeHub())


@pytest.mark.parametrize("judge_ret,direction,expect", [
    ((False, {"reject_reason": "RSI不对"}), "LONG", "reject"),
    ((True, {"direction": "long"}), "LONG", "pass"),
    ((True, {"direction": "short"}), "LONG", "reject"),     # 方向变了 → 拒绝
])
def test_realtime_judge_sync_verdicts(monkeypatch, judge_ret, direction, expect):
    _fake_hub(monkeypatch)
    monkeypatch.setattr(J, "judge_ema_trend", lambda *a, **k: judge_ret)
    v, d = SW._realtime_judge_sync({"symbol": "XUSDT", "sig_type": "ema_trend", "direction": direction, "ind": {}})
    assert v == expect, d


# ── signal_watcher：_trigger 分支 ─────────────────────────────────────────────
def _mk_entry(w, called, needs=True, mode="pullback"):
    async def on_trig(sig):
        called.append(sig["symbol"])
    sig = {"symbol": "XUSDT", "mode": mode, "sig_type": mode, "direction": "LONG"}
    if needs:
        sig["needs_realtime_check"] = True
    return sig, dict(entry_low=99.0, entry_high=101.0, entry_price=100.0, expire_ts=time.time() + 600,
                     on_trigger=on_trig, trigger_price=100.0, trigger_rsi_1h=50.0, breakout_ref=0.0,
                     ema20_at_trigger=100.0, atr_at_trigger=1.0, is_reorder=False)


@pytest.fixture
def no_net_api(monkeypatch):
    from shared import binance_api as api
    monkeypatch.setattr(api, "get_mark_price", lambda s: (_ for _ in ()).throw(Exception("no net")))
    monkeypatch.setattr(api, "get_position", lambda *a, **k: [], raising=False)
    monkeypatch.setattr(api, "get_open_orders", lambda *a, **k: [], raising=False)


def _run_trigger(monkeypatch, verdict, flag, needs=True):
    monkeypatch.setattr(config, "SW_REALTIME_JUDGE", flag, raising=False)
    monkeypatch.setattr(SW, "_realtime_judge_sync", lambda sig: verdict)
    w, called = SW.SignalWatcher(), []

    async def run():
        sig, kw = _mk_entry(w, called, needs)
        await w.add_signal(sig, **kw)
        await w._trigger("XUSDT", 100.0, w._watched["XUSDT"])
    asyncio.run(run())
    return w, called


def test_trigger_realtime_reject_drops_without_order(monkeypatch, no_net_api):
    w, called = _run_trigger(monkeypatch, ("reject", "RSI不对"), True)
    assert called == [] and "XUSDT" not in w._watched
    assert w._dir_last_trigger == {}          # 被拒绝的触发不消耗方向冷却


def test_trigger_realtime_pass_places_order(monkeypatch, no_net_api):
    w, called = _run_trigger(monkeypatch, ("pass", ""), True)
    assert called == ["XUSDT"]


def test_trigger_realtime_skip_passes_by_default(monkeypatch, no_net_api):
    monkeypatch.setattr(config, "SW_REALTIME_JUDGE_NO_HUB", "pass", raising=False)
    w, called = _run_trigger(monkeypatch, ("skip", "hub 无K线"), True)
    assert called == ["XUSDT"]


def test_trigger_realtime_skip_rejects_when_configured(monkeypatch, no_net_api):
    monkeypatch.setattr(config, "SW_REALTIME_JUDGE_NO_HUB", "reject", raising=False)
    w, called = _run_trigger(monkeypatch, ("skip", "hub 无K线"), True)
    assert called == []


def test_trigger_switch_off_ignores_realtime_judge(monkeypatch, no_net_api):
    # SW_REALTIME_JUDGE=False：即使精筛函数会拒绝，也必须与改动前一致（直接下单）
    w, called = _run_trigger(monkeypatch, ("reject", "x"), False)
    assert called == ["XUSDT"]


def test_trigger_signal_without_marker_not_rechecked(monkeypatch, no_net_api):
    w, called = _run_trigger(monkeypatch, ("reject", "x"), True, needs=False)
    assert called == ["XUSDT"]
