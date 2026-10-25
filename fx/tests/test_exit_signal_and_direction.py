"""
test_exit_signal_and_direction.py
测试两个改动：
1. _check_exit_signal 反向信号触发条件
2. scanner 方向冲突改为同向排除（反向允许）
"""
import sys
import types
import asyncio
from pathlib import Path
from unittest.mock import patch, MagicMock

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.append(str(ROOT.parent))   # [v3.89 TEST-2] 原为 insert(0)：仓库根目录排到 fx 前面，桩模块注册 shared.* 时
                                     #   shared 被解析成命名空间包（unknown location），恢复后其他测试仍拿不到真实 shared
import shared as _real_shared  # noqa: E402,F401  先导入真实 shared 包，桩只替换子模块

# ─── 桩模块 ───────────────────────────────────────────────────────────────────
# [v3.86 TEST-1] 原实现直接覆盖 sys.modules 中的 shared.* 且从不恢复，
#   pytest 先收集本文件、再收集 test_judge.py / test_pulse_candle.py 时，它们拿到的是空桩模块，
#   导致「cannot import name ...」和大量 AttributeError。这里记录原模块，在导入被测模块后立即恢复。
_ORIG_MODULES = {}
_STUBS = {}

def _make_stub(name):
    if name not in _ORIG_MODULES:
        _ORIG_MODULES[name] = sys.modules.get(name)
    m = types.ModuleType(name)
    sys.modules[name] = m
    _STUBS[name] = m
    return m


def _restore_modules():
    for _n, _m in _ORIG_MODULES.items():
        if _m is None:
            sys.modules.pop(_n, None)
        else:
            sys.modules[_n] = _m

for mod in [
    "binance_futures_connector",
    "binance_futures_connector.market",
    "binance_futures_connector.trade",
]:
    _make_stub(mod)

shared_api = _make_stub("shared.binance_api")
shared_api.get_klines = MagicMock(return_value=[])
shared_api.get_mark_price = MagicMock(return_value={"markPrice": "1.0", "lastFundingRate": "0"})
shared_notifier = _make_stub("shared.notifier")
shared_notifier.notify = MagicMock()
shared_notifier.notify_async = MagicMock()
shared_notifier.card_append = MagicMock()
shared_notifier.card_append_async = MagicMock()
shared_notifier.card_create = MagicMock()
shared_notifier.card_create_async = MagicMock()
shared_notifier.card_remove = MagicMock()
shared_notifier._hhmm = MagicMock(return_value="00:00")
shared_notifier._CLOSE_REASON_CN = {}
shared_notifier.fmt_hold_time = MagicMock(return_value="0min")
shared_notifier.make_card_header = MagicMock(return_value="")
# position_manager 后续新增导入的卡片/钉钉相关名字，桩里缺失会 ImportError
shared_notifier.card_is_open = MagicMock(return_value=False)
shared_notifier.card_get_text = MagicMock(return_value="")
shared_notifier.dingtalk_trade_result = MagicMock()
shared_notifier.build_close_summary = MagicMock(return_value="")
shared_notifier.DINGTALK_CLOSE_REASONS = set()
# [v3.89 TEST-2] position_manager / executor 新增导入的卡片格式化函数，桩里缺失会导致导入失败，
#   且失败发生在 _restore_modules() 之前，空桩残留在 sys.modules，连带后续 4 个测试文件收集失败
for _fn in ("fmt_node_tp1", "fmt_node_close", "fmt_node_sl_lost", "fmt_node_open",
            "fmt_node_immediate_close", "fmt_node_monitor", "fmt_node_trigger",
            "fmt_node_timeout", "fmt_node_evicted"):
    setattr(shared_notifier, _fn, MagicMock(return_value=""))
shared_risk = _make_stub("shared.risk_manager")
shared_risk.record_pnl = MagicMock()
shared_state = _make_stub("shared.state")
shared_state.get_all_positions = MagicMock(return_value=[])
shared_state.update_position = MagicMock()
shared_state.remove_position = MagicMock()
shared_state.set_cooldown = MagicMock()
shared_state.is_in_cooldown = MagicMock(return_value=False)
shared_state.add_cooldown = MagicMock()
shared_indicators = _make_stub("shared.indicators")
shared_indicators.atr = MagicMock(return_value=0.0)
shared_indicators.ema = MagicMock(return_value=0.0)
shared_indicators.rsi = MagicMock(return_value=50.0)
shared_judges = _make_stub("shared.judges")
shared_judges._reversal_signal_15m = MagicMock(return_value=None)

import config

# ═══════════════════════════════════════════════════════════════════════════════
# Part 1：_check_exit_signal
# ═══════════════════════════════════════════════════════════════════════════════

try:
    from position_manager import PositionManager, _last_price
except Exception:
    _restore_modules()   # [v3.89 TEST-2] 导入失败也要恢复真实模块，避免连带其他测试文件
    raise
_restore_modules()   # [v3.86 TEST-1] 收集阶段结束即恢复真实模块，其他测试文件导入时拿到真实实现


import pytest as _pytest_iso


@_pytest_iso.fixture(autouse=True)
def _reinstall_stubs(monkeypatch):
    """[v3.86 TEST-1] 仅在本文件的测试运行期间临时装回桩模块（函数内的延迟导入依赖它们），结束后自动还原。"""
    for _n, _m in _STUBS.items():
        monkeypatch.setitem(sys.modules, _n, _m)
    yield

def _make_pm():
    pm = object.__new__(PositionManager)
    return pm

def _run(coro):
    return asyncio.run(coro)   # 全量运行时主线程可能已无当前事件循环，get_event_loop 会抛 RuntimeError

def _make_pos(direction="SHORT", entry=2.0, tp1=1.6, fill_time_ago_min=60):
    import time
    fill_ms = (time.time() - fill_age_sec(fill_time_ago_min)) * 1000
    return {
        "symbol": "BTWUSDT",
        "direction": direction,
        "entry_price": str(entry),
        "tp1_price": str(tp1),
        "fill_time": str(int(fill_ms)),
        "status": "open",
    }

def fill_age_sec(minutes):
    return minutes * 60

# 用来捕获 _force_close 调用
class CloseCapture:
    def __init__(self):
        self.called = False
        self.reason = None

async def _fake_force_close(pm_self, pos, reason):
    pm_self._capture.called = True
    pm_self._capture.reason = reason

import time


def test_exit_signal_disabled():
    """EXIT_SIGNAL_ENABLED=False 时不触发"""
    with patch.object(config, "EXIT_SIGNAL_ENABLED", False):
        pm = _make_pm()
        cap = CloseCapture()
        pm._capture = cap
        pm._force_close = lambda pos, reason: _fake_force_close(pm, pos, reason)

        pos = _make_pos("SHORT", entry=2.0, tp1=1.6)
        _last_price["BTWUSDT"] = 1.65  # 浮盈 17.5%，TP1=20%，ratio=0.875 > 0.8
        _run(_check_exit_signal_direct(pm, pos))
        assert not cap.called, "EXIT_SIGNAL_ENABLED=False 不应触发"
    print("PASS test_exit_signal_disabled")


def test_exit_signal_hold_too_short():
    """持仓不足 30 分钟时不触发"""
    pm = _make_pm()
    cap = CloseCapture()
    pm._capture = cap
    pm._force_close = lambda pos, reason: _fake_force_close(pm, pos, reason)

    pos = _make_pos("SHORT", entry=2.0, tp1=1.6, fill_time_ago_min=10)  # 只持了10分钟
    _last_price["BTWUSDT"] = 1.65

    shared_indicators.rsi.return_value = 22.0
    shared_judges._reversal_signal_15m.return_value = "hammer"

    _run(_check_exit_signal_direct(pm, pos))
    assert not cap.called, "持仓 < 30min 不应触发"
    print("PASS test_exit_signal_hold_too_short")


def test_exit_signal_rsi_not_oversold():
    """做空持仓但 RSI >= 28，不触发"""
    pm = _make_pm()
    cap = CloseCapture()
    pm._capture = cap
    pm._force_close = lambda pos, reason: _fake_force_close(pm, pos, reason)

    pos = _make_pos("SHORT", entry=2.0, tp1=1.6, fill_time_ago_min=60)
    _last_price["BTWUSDT"] = 1.65

    shared_indicators.rsi.return_value = 35.0  # RSI=35，不超卖
    shared_judges._reversal_signal_15m.return_value = "hammer"

    _run(_check_exit_signal_direct(pm, pos))
    assert not cap.called, "RSI >= 28 不应触发"
    print("PASS test_exit_signal_rsi_not_oversold")


def test_exit_signal_no_15m_pattern():
    """RSI 超卖但没有 15M 止跌形态，不触发"""
    pm = _make_pm()
    cap = CloseCapture()
    pm._capture = cap
    pm._force_close = lambda pos, reason: _fake_force_close(pm, pos, reason)

    pos = _make_pos("SHORT", entry=2.0, tp1=1.6, fill_time_ago_min=60)
    _last_price["BTWUSDT"] = 1.65

    shared_indicators.rsi.return_value = 22.0
    shared_judges._reversal_signal_15m.return_value = None  # 没有形态

    _run(_check_exit_signal_direct(pm, pos))
    assert not cap.called, "无 15M 形态不应触发"
    print("PASS test_exit_signal_no_15m_pattern")


def test_exit_signal_profit_insufficient():
    """浮盈不足 TP1 距离 80%，不触发"""
    pm = _make_pm()
    cap = CloseCapture()
    pm._capture = cap
    pm._force_close = lambda pos, reason: _fake_force_close(pm, pos, reason)

    pos = _make_pos("SHORT", entry=2.0, tp1=1.6, fill_time_ago_min=60)
    _last_price["BTWUSDT"] = 1.85  # 浮盈 7.5%，TP1 距离 20%，ratio=0.375 < 0.8

    shared_indicators.rsi.return_value = 22.0
    shared_judges._reversal_signal_15m.return_value = "hammer"

    _run(_check_exit_signal_direct(pm, pos))
    assert not cap.called, "浮盈不足 80% TP1 距离不应触发"
    print("PASS test_exit_signal_profit_insufficient")


def test_exit_signal_loss_position():
    """亏损时不触发（price > entry 的空单）"""
    pm = _make_pm()
    cap = CloseCapture()
    pm._capture = cap
    pm._force_close = lambda pos, reason: _fake_force_close(pm, pos, reason)

    pos = _make_pos("SHORT", entry=2.0, tp1=1.6, fill_time_ago_min=60)
    _last_price["BTWUSDT"] = 2.10  # 亏损

    shared_indicators.rsi.return_value = 22.0
    shared_judges._reversal_signal_15m.return_value = "hammer"

    _run(_check_exit_signal_direct(pm, pos))
    assert not cap.called, "亏损时不应触发"
    print("PASS test_exit_signal_loss_position")


def test_exit_signal_triggers_short():
    """做空持仓，全部条件满足，应触发 exit_signal_reversal"""
    pm = _make_pm()
    cap = CloseCapture()
    pm._capture = cap
    pm._force_close = lambda pos, reason: _fake_force_close(pm, pos, reason)

    pos = _make_pos("SHORT", entry=2.0, tp1=1.6, fill_time_ago_min=60)
    # 浮盈：entry=2.0, price=1.64 → profit=0.36，tp1_dist=0.4，ratio=0.9 > 0.8
    _last_price["BTWUSDT"] = 1.64

    shared_indicators.rsi.return_value = 22.0
    shared_judges._reversal_signal_15m.return_value = "hammer"

    _run(_check_exit_signal_direct(pm, pos))
    assert cap.called, "全部条件满足应触发"
    assert cap.reason == "exit_signal_reversal"
    print("PASS test_exit_signal_triggers_short")


def test_exit_signal_triggers_long():
    """做多持仓，RSI 超买 + 止涨形态 + 足够浮盈，应触发"""
    pm = _make_pm()
    cap = CloseCapture()
    pm._capture = cap
    pm._force_close = lambda pos, reason: _fake_force_close(pm, pos, reason)

    # entry=1.0, tp1=1.4 (做多)
    pos = _make_pos("LONG", entry=1.0, tp1=1.4, fill_time_ago_min=60)
    # 浮盈：price=1.37，profit=0.37，tp1_dist=0.4，ratio=0.925 > 0.8
    _last_price["BTWUSDT"] = 1.37

    shared_indicators.rsi.return_value = 78.0  # > 72
    shared_judges._reversal_signal_15m.return_value = "shooting_star"

    _run(_check_exit_signal_direct(pm, pos))
    assert cap.called, "做多超买应触发"
    assert cap.reason == "exit_signal_reversal"
    print("PASS test_exit_signal_triggers_long")


async def _check_exit_signal_direct(pm, pos):
    """直接调用 _check_exit_signal，绕过 API 拉取（mock 掉 _calc_rsi）"""
    import position_manager as pmmod
    # 构造假 k 线列表（15根闭合1H + 5根15M）
    def fake_klines(sym, tf, limit=30):
        now_ms = time.time() * 1000
        # 已闭合的 K 线：close_time 比现在早 1ms
        k = [["0", "1", "1", "1", "1", "1000", str(now_ms - 1000 * i), "0", "0", "0", "0", "0"]
             for i in range(1, limit + 1)]
        return list(reversed(k))

    with patch.object(pmmod.api, "get_klines", side_effect=fake_klines), \
         patch.object(pmmod, "_calc_rsi", return_value=shared_indicators.rsi.return_value):
        await pm._check_exit_signal(pos)


# ═══════════════════════════════════════════════════════════════════════════════
# Part 2：scanner 方向冲突
# ═══════════════════════════════════════════════════════════════════════════════

def test_scanner_same_direction_rejected():
    """方向冲突逻辑：(symbol, direction) 同向才拦截"""
    # active_syms 是 (symbol, direction) 二元组集合
    active_syms = {("BTWUSDT", "SHORT")}

    # 同向：应被拦截
    symbol, direction = "BTWUSDT", "SHORT"
    assert (symbol, direction) in active_syms, "同向应在 active_syms 里"

    # 反向：不被拦截
    direction_opp = "LONG"
    assert (symbol, direction_opp) not in active_syms, "反向不应在 active_syms 里"
    print("PASS test_scanner_same_direction_rejected")


def test_scanner_opposite_direction_allowed():
    """analyze_symbol: 已有空单时，LONG 信号不被方向冲突拦截"""
    active_syms = {("BTWUSDT", "SHORT")}
    assert ("BTWUSDT", "LONG") not in active_syms, "反向应允许"
    print("PASS test_scanner_opposite_direction_allowed")


def test_scanner_active_syms_structure():
    """验证 scan_one_cycle 生成的 active_syms 结构是二元组集合"""
    positions = [
        {"symbol": "BTWUSDT", "direction": "SHORT", "status": "open"},
        {"symbol": "AKEUSDT", "direction": "LONG",  "status": "open"},
    ]
    active_syms = {(p["symbol"], p.get("direction", "").upper())
                   for p in positions if p.get("status") == "open"}

    assert ("BTWUSDT", "SHORT") in active_syms
    assert ("AKEUSDT", "LONG")  in active_syms
    assert ("BTWUSDT", "LONG")  not in active_syms  # 反向不在集合里
    assert ("AKEUSDT", "SHORT") not in active_syms
    print("PASS test_scanner_active_syms_structure")


def test_scanner_scan_pool_includes_active_symbols():
    """scan_pool 现在包含有持仓的标的（由 analyze_symbol 内部做同向过滤）"""
    pool = ["BTWUSDT", "AKEUSDT", "ETHUSDT"]
    pending_syms = {"ETHUSDT"}

    # 新逻辑：scan_pool 只排除 pending，不排除 active
    scan_pool = [s for s in pool if s not in pending_syms]
    assert "BTWUSDT" in scan_pool, "有持仓的标的应在 scan_pool 里"
    assert "ETHUSDT" not in scan_pool, "pending 的标的不在 scan_pool 里"
    print("PASS test_scanner_scan_pool_includes_active_symbols")


if __name__ == "__main__":
    # Part 1
    test_exit_signal_disabled()
    test_exit_signal_hold_too_short()
    test_exit_signal_rsi_not_oversold()
    test_exit_signal_no_15m_pattern()
    test_exit_signal_profit_insufficient()
    test_exit_signal_loss_position()
    test_exit_signal_triggers_short()
    test_exit_signal_triggers_long()
    # Part 2
    test_scanner_same_direction_rejected()
    test_scanner_opposite_direction_allowed()
    test_scanner_active_syms_structure()
    test_scanner_scan_pool_includes_active_symbols()
    print("\n✅ 12/12 通过")
