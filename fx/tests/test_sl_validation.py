"""
tests/test_sl_validation.py — binance_api._validate_sl_order 单测

覆盖：
  1. 非止损单类型 → 跳过（返回 None）
  2. LONG 止损价高于入场价 → 拒绝
  3. SHORT 止损价低于入场价 → 拒绝
  4. LONG 止损距离 < ATR×0.5 → 拒绝（太近）
  5. SHORT 止损距离 < ATR×0.5 → 拒绝（太近）
  6. LONG 合法止损 → 通过（返回 None）
  7. SHORT 合法止损 → 通过
  8. 没传 entry_price / atr → 跳过距离验证，只做方向验证
  9. 没传 atr → 方向验证仍然执行
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "shared"))

from binance_api import _validate_sl_order

ATR = 100.0
ENTRY = 50000.0


class TestValidateSlOrderSkip:

    def test_non_stop_order_skipped(self):
        """LIMIT 订单不走止损验证"""
        assert _validate_sl_order("LIMIT", "SELL", 49000, ENTRY, ATR, "LONG") is None

    def test_take_profit_skipped(self):
        assert _validate_sl_order("TAKE_PROFIT_MARKET", "SELL", 51000, ENTRY, ATR, "LONG") is None

    def test_no_entry_price_skipped(self):
        """没传 entry_price 跳过验证"""
        assert _validate_sl_order("STOP_MARKET", "SELL", 49000, None, ATR, "LONG") is None

    def test_no_atr_direction_still_checked(self):
        """没传 ATR 时，方向验证仍然执行"""
        # LONG 止损价比入场价高 → 应该报错
        result = _validate_sl_order("STOP_MARKET", "SELL", 51000, ENTRY, None, "LONG")
        assert result is not None and "方向错误" in result


class TestValidateSlOrderLong:

    def test_long_sl_above_entry_rejected(self):
        """LONG 止损 >= 入场价 → 方向错误"""
        err = _validate_sl_order("STOP_MARKET", "SELL", 50100, ENTRY, ATR, "LONG")
        assert err is not None
        assert "方向错误" in err or ">=" in err

    def test_long_sl_equal_entry_rejected(self):
        err = _validate_sl_order("STOP_MARKET", "SELL", ENTRY, ENTRY, ATR, "LONG")
        assert err is not None

    def test_long_sl_too_close_rejected(self):
        """LONG 止损距离 < ATR×0.5（=50）→ 太近"""
        sl_too_close = ENTRY - ATR * 0.4   # 距离=40 < 50
        err = _validate_sl_order("STOP_MARKET", "SELL", sl_too_close, ENTRY, ATR, "LONG")
        assert err is not None
        assert "太近" in err

    def test_long_sl_exact_half_atr_rejected(self):
        """恰好 = ATR×0.5 的边界：距离不"大于"0.5倍 → 也应拒绝"""
        sl_boundary = ENTRY - ATR * 0.5
        err = _validate_sl_order("STOP_MARKET", "SELL", sl_boundary, ENTRY, ATR, "LONG")
        assert err is not None

    def test_long_sl_valid_passes(self):
        """LONG 止损距离 = ATR×1.5 → 合法"""
        sl_ok = ENTRY - ATR * 1.5
        assert _validate_sl_order("STOP_MARKET", "SELL", sl_ok, ENTRY, ATR, "LONG") is None

    def test_long_sl_valid_stop_type_passes(self):
        """STOP 类型同样走验证"""
        sl_ok = ENTRY - ATR * 1.2
        assert _validate_sl_order("STOP", "SELL", sl_ok, ENTRY, ATR, "LONG") is None

    def test_long_detected_from_side_no_position_side(self):
        """position_side=None，SELL 推断为 LONG 止损"""
        sl_ok = ENTRY - ATR * 1.5
        assert _validate_sl_order("STOP_MARKET", "SELL", sl_ok, ENTRY, ATR, None) is None

    def test_long_direction_from_position_side(self):
        """position_side='LONG' + SELL 明确 LONG 止损"""
        sl_ok = ENTRY - ATR * 1.5
        assert _validate_sl_order("STOP_MARKET", "SELL", sl_ok, ENTRY, ATR, "LONG") is None


class TestValidateSlOrderShort:

    def test_short_sl_below_entry_rejected(self):
        """SHORT 止损 <= 入场价 → 方向错误"""
        err = _validate_sl_order("STOP_MARKET", "BUY", 49000, ENTRY, ATR, "SHORT")
        assert err is not None
        assert "方向错误" in err or "<=" in err

    def test_short_sl_too_close_rejected(self):
        sl_too_close = ENTRY + ATR * 0.3   # 距离=30 < 50
        err = _validate_sl_order("STOP_MARKET", "BUY", sl_too_close, ENTRY, ATR, "SHORT")
        assert err is not None
        assert "太近" in err

    def test_short_sl_valid_passes(self):
        sl_ok = ENTRY + ATR * 1.5
        assert _validate_sl_order("STOP_MARKET", "BUY", sl_ok, ENTRY, ATR, "SHORT") is None

    def test_short_detected_from_side_no_position_side(self):
        """position_side=None，BUY 推断为 SHORT 止损"""
        sl_ok = ENTRY + ATR * 1.5
        assert _validate_sl_order("STOP_MARKET", "BUY", sl_ok, ENTRY, ATR, None) is None
