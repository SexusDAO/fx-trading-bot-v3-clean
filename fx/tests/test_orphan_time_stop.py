"""孤儿仓位（recovered）8H 时间止损（RECOVERED_MAX_HOLD_HOURS）；以及预检 get_position 参数。全部 mock，不联网。"""
import asyncio
import re
from pathlib import Path

import pytest
import position_manager as pm

SRC = Path(pm.__file__).read_text(encoding="utf-8")
EXE_SRC = (Path(pm.__file__).parent / "executor.py").read_text(encoding="utf-8")
H12 = 8 * 60 * 60000        # 名称沿用，实际为 8H（RECOVERED_MAX_HOLD_HOURS）
NOW = 1_800_000_000_000


def orphan(**kw):
    p = {"pos_id": "rec_XUSDT_s_1", "symbol": "XUSDT", "direction": "SHORT", "status": "open",
         "mode": "recovered", "sig_type": "recovered", "entry_price": 100.0, "quantity": 1.0,
         "open_time": NOW - H12, "tp1_filled": False, "source": "orphan_recovered"}
    p.update(kw)
    return p


@pytest.fixture
def pm_obj(monkeypatch):
    closed = []

    async def fake_close(pos, reason):
        closed.append((pos["symbol"], reason))

    obj = pm.PositionManager.__new__(pm.PositionManager)     # 不跑 __init__（会读状态文件）
    monkeypatch.setattr(obj, "_force_close", fake_close, raising=False)
    monkeypatch.setattr(pm, "_last_price", {"XUSDT": 99.0}, raising=False)
    return obj, closed


def test_orphan_closes_at_8h(pm_obj, caplog):
    import logging
    obj, closed = pm_obj
    with caplog.at_level(logging.INFO, logger="fx.position_manager"):
        asyncio.run(obj._check_time_stop(orphan(), NOW))
    assert closed == [("XUSDT", "orphan_timeout")]
    assert "孤儿仓位持仓满8H强制平仓" in caplog.text


def test_orphan_just_under_8h_not_closed(pm_obj):
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(open_time=NOW - H12 + 60_000), NOW))
    assert closed == []


def test_orphan_skips_other_time_stop_paths(pm_obj, monkeypatch):
    """孤儿仓位即使已远超普通软上限(8H)/硬上限(16H)，也只可能因孤儿规则平仓，且平仓原因只能是 orphan_timeout。"""
    obj, closed = pm_obj
    pos = orphan(open_time=NOW - 40 * 60 * 60000, entry_price=100.0)   # 40H，浮亏
    asyncio.run(obj._check_time_stop(pos, NOW))
    assert closed == [("XUSDT", "orphan_timeout")]


def test_orphan_early_not_touched_by_other_rules(pm_obj):
    """持有 3H 的孤儿仓位：不触发任何时间止损（孤儿上限 8H）。"""
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(open_time=NOW - 3 * 60 * 60000), NOW))
    assert closed == []


def test_sig_type_only_recovered_also_matched(pm_obj):
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(mode="pullback", sig_type="recovered"), NOW))
    assert closed == [("XUSDT", "orphan_timeout")]


def test_mode_only_recovered_also_matched(pm_obj):
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(mode="recovered", sig_type="pullback"), NOW))
    assert closed == [("XUSDT", "orphan_timeout")]


def test_funding_arb_still_skipped_before_orphan_block():
    body = SRC[SRC.index("    async def _check_time_stop"):]
    i_fa = body.index('if pos.get("sig_type") == "funding_arb":')
    i_or = body.index('pos.get("mode") == "recovered"')
    assert i_fa < i_or                                   # funding_arb 判断在前
    assert body[i_fa:i_or].count("return") == 1


def test_orphan_branch_returns_before_normal_limits():
    body = SRC[SRC.index("    async def _check_time_stop"):]
    i_or = body.index('pos.get("mode") == "recovered"')
    i_soft = body.index("_soft_limit = TIME_STOP_REVERSAL_HOURS * 60")
    seg = body[i_or:i_soft]
    assert re.search(r'\n\s+return\s+#\s*孤儿仓位|orphan_timeout"\)\n\s+return', seg)


def test_orphan_isolation_marked_in_log():
    """上限读 config.RECOVERED_MAX_HOLD_HOURS（默认 8），不再写死 12。"""
    body = SRC[SRC.index('pos.get("mode") == "recovered"'):][:1800]
    assert "RECOVERED_MAX_HOLD_HOURS" in body and "12 * 60" not in body and '"orphan_timeout"' in body


# ── 预检 get_position ──
def test_executor_precheck_uses_none_not_empty_string():
    assert 'api.get_position(None)' in EXE_SRC
    assert 'api.get_position("")' not in EXE_SRC


def test_get_position_omits_symbol_param_for_falsey_values(monkeypatch):
    from shared import binance_api as B
    seen = []

    def fake_signed_get(path, params):
        seen.append((path, dict(params)))
        return []
    monkeypatch.setattr(B, "_signed_get", fake_signed_get)
    for arg in (None, ""):
        seen.clear()
        B.get_position(arg)
        assert seen[0][1] == {}                    # 两种写法当前行为一致：都不带 symbol
    seen.clear()
    B.get_position("ARBUSDT")
    assert seen[0][1] == {"symbol": "ARBUSDT"}


# ── 8H 改动新增 ──
def test_orphan_open_time_missing_uses_fill_time_seconds(pm_obj):
    obj, closed = pm_obj
    p = orphan(fill_time=(NOW - H12) // 1000)           # fill_time 是秒
    p.pop("open_time")
    asyncio.run(obj._check_time_stop(p, NOW))
    assert closed == [("XUSDT", "orphan_timeout")]


def test_orphan_no_time_fields_skips_instead_of_closing(pm_obj):
    obj, closed = pm_obj
    p = orphan()
    p.pop("open_time")
    asyncio.run(obj._check_time_stop(p, NOW))
    assert closed == []                                  # 原先缺失按 0 算 → 立刻误平仓


def test_orphan_open_time_zero_falls_back_to_fill_time(pm_obj):
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(open_time=0, fill_time=NOW - 60_000), NOW))
    assert closed == []                                  # fill_time 仅 1 分钟前


def test_recovered_limit_follows_config(pm_obj, monkeypatch):
    import config
    obj, closed = pm_obj
    monkeypatch.setattr(config, "RECOVERED_MAX_HOLD_HOURS", 12, raising=False)
    asyncio.run(obj._check_time_stop(orphan(open_time=NOW - 9 * 60 * 60000), NOW))
    assert closed == []
    asyncio.run(obj._check_time_stop(orphan(open_time=NOW - 12 * 60 * 60000), NOW))
    assert closed == [("XUSDT", "orphan_timeout")]


def test_config_values():
    import config
    assert config.RECOVERED_MAX_HOLD_HOURS == 8 and config.EMA_TREND_MAX_HOLD_HOURS == 12
    assert config.EMA_TREND_MAX_HOLD_HOURS < config.TIME_STOP_HARD_HOURS


def _ema_trend_pos(hours, **kw):
    p = {"pos_id": "p1", "symbol": "XUSDT", "direction": "LONG", "status": "open", "mode": "ema_trend",
         "sig_type": "ema_trend", "entry_price": 100.0, "quantity": 1.0, "open_time": NOW - int(hours * 3600_000),
         "tp1_filled": False, "sl_price": 0}
    p.update(kw)
    return p


def test_ema_trend_soft_limit_is_12h_and_closes_flat_position(pm_obj, monkeypatch):
    obj, closed = pm_obj
    monkeypatch.setattr(pm, "_last_price", {"XUSDT": 100.1}, raising=False)      # 浮盈 0.1% < 0.5%
    asyncio.run(obj._check_time_stop(_ema_trend_pos(11.5), NOW))
    assert closed == []
    asyncio.run(obj._check_time_stop(_ema_trend_pos(12.1), NOW))
    assert closed == [("XUSDT", "time_stop_no_progress")]


def test_ema_trend_between_6h_and_12h_not_closed_by_soft_limit(pm_obj, monkeypatch):
    obj, closed = pm_obj
    monkeypatch.setattr(pm, "_last_price", {"XUSDT": 100.1}, raising=False)
    asyncio.run(obj._check_time_stop(_ema_trend_pos(9), NOW))
    assert closed == []
