"""孤儿仓位（recovered）12H 时间止损；以及预检 get_position 参数。全部 mock，不联网。"""
import asyncio
import re
from pathlib import Path

import pytest
import position_manager as pm

SRC = Path(pm.__file__).read_text(encoding="utf-8")
EXE_SRC = (Path(pm.__file__).parent / "executor.py").read_text(encoding="utf-8")
H12 = 12 * 60 * 60000
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


def test_orphan_closes_at_12h(pm_obj, caplog):
    import logging
    obj, closed = pm_obj
    with caplog.at_level(logging.INFO, logger="fx.position_manager"):
        asyncio.run(obj._check_time_stop(orphan(), NOW))
    assert closed == [("XUSDT", "orphan_timeout_12h")]
    assert "孤儿仓位持仓满12H强制平仓" in caplog.text


def test_orphan_just_under_12h_not_closed(pm_obj):
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(open_time=NOW - H12 + 60_000), NOW))
    assert closed == []


def test_orphan_skips_other_time_stop_paths(pm_obj, monkeypatch):
    """孤儿仓位即使已远超普通软上限(8H)/硬上限(16H)，也只可能因 12H 规则平仓，且平仓原因只能是 orphan_timeout_12h。"""
    obj, closed = pm_obj
    pos = orphan(open_time=NOW - 40 * 60 * 60000, entry_price=100.0)   # 40H，浮亏
    asyncio.run(obj._check_time_stop(pos, NOW))
    assert closed == [("XUSDT", "orphan_timeout_12h")]


def test_orphan_early_not_touched_by_other_rules(pm_obj):
    """持有 3H 的孤儿仓位：不触发任何时间止损（普通规则在 8H 才生效）。"""
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(open_time=NOW - 3 * 60 * 60000), NOW))
    assert closed == []


def test_sig_type_only_recovered_also_matched(pm_obj):
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(mode="pullback", sig_type="recovered"), NOW))
    assert closed == [("XUSDT", "orphan_timeout_12h")]


def test_mode_only_recovered_also_matched(pm_obj):
    obj, closed = pm_obj
    asyncio.run(obj._check_time_stop(orphan(mode="recovered", sig_type="pullback"), NOW))
    assert closed == [("XUSDT", "orphan_timeout_12h")]


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
    assert re.search(r"\n\s+return\s+#\s*孤儿仓位|orphan_timeout_12h\)\n\s+return", seg)


def test_orphan_isolation_marked_in_log():
    """12H 上限:12*60 分钟，不是 8H/16H。"""
    body = SRC[SRC.index('pos.get("mode") == "recovered"'):][:600]
    assert "12 * 60" in body and "orphan_timeout_12h" in body


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
