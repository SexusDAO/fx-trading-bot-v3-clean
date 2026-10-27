"""持仓上限 5→8、挂单上限同步扩展；funding_arb 槽位独立不变；代码侧不再硬编码上限。"""
import inspect

import config as C
import scanner as S
from shared import risk_manager as RM


def test_total_limits_are_8():
    assert (C.MAX_POSITIONS_TOTAL_BULL, C.MAX_POSITIONS_TOTAL_BEAR, C.MAX_POSITIONS_TOTAL_RANGE) == (8, 8, 8)


def test_direction_split_per_mode():
    assert (C.MAX_POSITIONS_LONG_BULL, C.MAX_POSITIONS_SHORT_BULL) == (5, 3)
    assert (C.MAX_POSITIONS_LONG_BEAR, C.MAX_POSITIONS_SHORT_BEAR) == (3, 5)
    assert (C.MAX_POSITIONS_LONG_RANGE, C.MAX_POSITIONS_SHORT_RANGE) == (4, 4)


def test_direction_caps_not_exceed_total():
    for m in ("BULL", "BEAR", "RANGE"):
        t = getattr(C, f"MAX_POSITIONS_TOTAL_{m}")
        assert getattr(C, f"MAX_POSITIONS_LONG_{m}") + getattr(C, f"MAX_POSITIONS_SHORT_{m}") == t


def test_legacy_aliases_follow_bull():
    assert (C.RISK_MAX_POSITIONS, C.RISK_MAX_LONG, C.RISK_MAX_SHORT) == (8, 5, 3)


def test_pending_limits():
    assert (C.MAX_PENDING_LONG, C.MAX_PENDING_SHORT, C.MAX_PENDING_TOTAL) == (5, 5, 8)


def test_funding_arb_slot_unchanged():
    assert C.FUNDING_ARB_MAX_POSITIONS == 1


def test_risk_manager_reads_config():
    assert RM._get_position_limits("bull") == {"total": 8, "long": 5, "short": 3}
    assert RM._get_position_limits("bear") == {"total": 8, "long": 3, "short": 5}
    assert RM._get_position_limits("range") == {"total": 8, "long": 4, "short": 4}


def test_scanner_pending_cap_not_hardcoded():
    src = inspect.getsource(S)
    assert "_MAX_PENDING_PER_DIR = 5" not in src
    assert "MAX_PENDING_LONG as _MPL" in src
