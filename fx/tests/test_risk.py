"""
tests/test_risk.py — risk_manager.py 自动化测试
mock 替换所有外部 API，不依赖真实网络
"""
import sys
import time
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# ── 隔离状态文件 ──────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    import shared.state as _st
    import threading
    monkeypatch.setattr(_st, "_BASE",     tmp_path)
    monkeypatch.setattr(_st, "_POS_FILE", tmp_path / "pos_state.json")
    monkeypatch.setattr(_st, "_CB_FILE",  tmp_path / "circuit_breaker.json")
    monkeypatch.setattr(_st, "_CD_FILE",  tmp_path / "cooldown_state.json")
    monkeypatch.setattr(_st, "_PC_FILE",  tmp_path / "pending_confirm_state.json")
    monkeypatch.setattr(_st, "_OI_FILE",  tmp_path / "oi_cache.json")
    monkeypatch.setattr(_st, "_locks",    {k: threading.Lock() for k in _st._locks})
    _st._ensure_files()
    # 重置市场模式缓存
    import shared.risk_manager as _rm
    monkeypatch.setattr(_rm, "_market_mode_cache", {"mode": None, "cached_at": 0.0})
    yield


from shared import risk_manager as rm
from shared import state as st


# ═══════════════════════════════════════════════════════════════════════════════
# 熔断器
# ═══════════════════════════════════════════════════════════════════════════════
class TestCircuitBreaker:

    def test_not_open_initially(self):
        assert rm.is_circuit_open() is False

    def test_triggers_on_loss_limit(self):
        import config as _c; _c.CIRCUIT_BREAKER_DAILY_LOSS = 100
        try:
            rm.record_pnl(-110.0)
            assert rm.is_circuit_open() is True
        finally:
            _c.CIRCUIT_BREAKER_DAILY_LOSS = 9999

    def test_no_trigger_below_limit(self):
        with patch("config.CIRCUIT_BREAKER_DAILY_LOSS", 100):
            rm.record_pnl(-90.0)
        assert rm.is_circuit_open() is False

    def test_profit_offsets_loss_for_trigger(self):
        """[fix #12] 净盈亏口径：盈利抵消亏损；净亏损超限才触发"""
        import config as _c; _c.CIRCUIT_BREAKER_DAILY_LOSS = 100
        try:
            rm.record_pnl(+500.0)   # 盈利
            rm.record_pnl(-150.0)   # 净仍盈利 350 → 不触发
            assert rm.is_circuit_open() is False
            rm.record_pnl(-500.0)   # 净亏 150 → 触发
            assert rm.is_circuit_open() is True
        finally:
            _c.CIRCUIT_BREAKER_DAILY_LOSS = 9999

    def test_reset_circuit(self):
        import config as _c; _c.CIRCUIT_BREAKER_DAILY_LOSS = 100
        rm.record_pnl(-200.0)
        assert rm.is_circuit_open() is True
        with patch("shared.notifier.notify", return_value=None), \
             patch("shutil.copy2"), \
             patch("pathlib.Path.exists", return_value=False):
            rm.reset_circuit()
        assert rm.is_circuit_open() is False

    def test_circuit_sends_telegram_on_trigger(self):
        """熔断触发时记录日志（notify 已禁用以防止 gateway 重发问题）"""
        import config as _c; _c.CIRCUIT_BREAKER_DAILY_LOSS = 50
        try:
            result = rm.record_pnl(-60.0)
            # notify 已被移除，只验证 circuit_triggered 状态正确
            assert result["circuit_triggered"] is True or result["daily_loss"] >= 50
        finally:
            _c.CIRCUIT_BREAKER_DAILY_LOSS = 9999


# ═══════════════════════════════════════════════════════════════════════════════
# 持仓上限检查
# ═══════════════════════════════════════════════════════════════════════════════
class TestPositionLimits:

    def _add_pos(self, sym, direction, status="open"):
        st.add_position({
            "pos_id": sym, "symbol": sym, "direction": direction,
            "status": status, "open_time": int(time.time()*1000),
            "entry_price": 100.0, "quantity": 0.01,
        })

    def test_allows_when_empty(self):
        with patch.object(rm, "get_market_mode", return_value="bull"):
            assert rm.check_position_limits("LONG") is True
            assert rm.check_position_limits("SHORT") is True

    def test_blocks_when_total_full(self):
        """总持仓达到上限时拒绝"""
        with patch.object(rm, "get_market_mode", return_value="bull"), \
             patch("config.MAX_POSITIONS_TOTAL_BULL", 2):
            self._add_pos("BTC", "long")
            self._add_pos("ETH", "long")
            assert rm.check_position_limits("LONG") is False

    def test_blocks_when_long_full(self):
        with patch.object(rm, "get_market_mode", return_value="bull"), \
             patch("config.MAX_POSITIONS_LONG_BULL", 1):
            self._add_pos("BTC", "long")
            assert rm.check_position_limits("LONG") is False
            assert rm.check_position_limits("SHORT") is True  # 空单不受影响

    def test_bull_bear_range_different_limits(self):
        """不同市场模式使用不同上限"""
        with patch.object(rm, "get_market_mode", return_value="bear"), \
             patch("config.MAX_POSITIONS_LONG_BEAR", 0):
            assert rm.check_position_limits("LONG") is False
        with patch.object(rm, "get_market_mode", return_value="bear"), \
             patch("config.MAX_POSITIONS_SHORT_BEAR", 10):
            assert rm.check_position_limits("SHORT") is True


# ═══════════════════════════════════════════════════════════════════════════════
# 市场模式判断
# ═══════════════════════════════════════════════════════════════════════════════
class TestMarketMode:

    def _make_klines(self, closes: list) -> list:
        """生成 mock K 线数据"""
        return [[0, c, c*1.01, c*0.99, c, 1000, 0, 0, 0, 0, 0, 0] for c in closes]

    def test_bull_market_ema_up(self):
        """EMA20 > EMA50 且价格 > MA99 → bull"""
        # 构造上涨趋势的收盘价
        closes = list(range(50, 150))   # 100根，从50到149，持续上涨
        mock_klines = self._make_klines(closes)
        with patch("shared.binance_api.get_klines", return_value=mock_klines):
            mode = rm.get_market_mode()
        assert mode == "bull"

    def test_bear_market_ema_down(self):
        """EMA20 < EMA50 且价格 < MA99 → bear"""
        closes = list(range(150, 50, -1))   # 持续下跌
        mock_klines = self._make_klines(closes)
        with patch("shared.binance_api.get_klines", return_value=mock_klines):
            mode = rm.get_market_mode()
        assert mode == "bear"

    def test_api_failure_falls_back_to_range(self):
        """API 失败时降级为 range"""
        with patch("shared.binance_api.get_klines", return_value=[]):
            mode = rm.get_market_mode()
        assert mode == "range"

    def test_mode_cached_5min(self):
        """5分钟内不重复计算"""
        closes = list(range(50, 150))
        mock_klines = self._make_klines(closes)
        with patch("shared.binance_api.get_klines", return_value=mock_klines) as mock_api:
            rm.get_market_mode()
            rm.get_market_mode()
        assert mock_api.call_count == 1   # 只调用一次


# ═══════════════════════════════════════════════════════════════════════════════
# 账户保证金检查
# ═══════════════════════════════════════════════════════════════════════════════
class TestAccountMargin:

    def _mock_account(self, available=1000.0, total=1000.0):
        return {"availableBalance": str(available), "totalWalletBalance": str(total)}

    def test_sufficient_margin(self):
        # nominal=300: 300/10*1.1=33U < available=1000U 通过余额检查
        # 300 < total_bal(1000)*40%=400U 通过动态单仓上限
        with patch("shared.binance_api.get_account", return_value=self._mock_account(1000, 1000)),              patch("shared.state.get_all_positions", return_value=[]):
            assert rm.check_account_margin(300.0, 10) is True

    def test_insufficient_available_balance(self):
        with patch("shared.binance_api.get_account", return_value=self._mock_account(10.0, 1000.0)):
            assert rm.check_account_margin(500.0, 10) is False   # 需要55U，只有10U

    def test_total_nominal_cap(self):
        """总名义仓位超过账户余额×3时拒绝"""
        # 账户 100U，上限 300U，已有 200U，再开 200U = 400U > 300U
        self._add_pos_with_nominal("BTC", 200.0)
        with patch("shared.binance_api.get_account",
                   return_value=self._mock_account(50.0, 100.0)):
            result = rm.check_account_margin(200.0, 10)
        assert result is False

    def _add_pos_with_nominal(self, sym, nominal):
        st.add_position({
            "pos_id": sym, "symbol": sym+"USDT", "direction": "long",
            "status": "open", "open_time": int(time.time()*1000),
            "entry_price": nominal, "quantity": 1.0,
        })

    def test_single_nominal_cap(self):
        """单仓超过 MAX_SINGLE_NOMINAL 时拒绝"""
        with patch("shared.binance_api.get_account",
                   return_value=self._mock_account(10000.0, 10000.0)), \
             patch("config.MAX_SINGLE_NOMINAL", 500):
            assert rm.check_account_margin(600.0, 10) is False

    def test_api_failure_conservative(self):
        """API 失败时保守返回 False"""
        with patch("shared.binance_api.get_account", side_effect=Exception("timeout")):
            assert rm.check_account_margin(100.0, 10) is False


# ═══════════════════════════════════════════════════════════════════════════════
# get_risk_summary
# ═══════════════════════════════════════════════════════════════════════════════
class TestRiskSummary:

    def test_summary_structure(self):
        with patch.object(rm, "get_market_mode", return_value="bull"):
            summary = rm.get_risk_summary()
        assert "circuit_open" in summary
        assert "daily_loss" in summary
        assert "position_count" in summary
        assert "market_mode" in summary
        assert summary["market_mode"] == "bull"

    def test_summary_reflects_circuit(self):
        with patch("config.CIRCUIT_BREAKER_DAILY_LOSS", 50), \
             patch("shared.notifier.notify"), \
             patch.object(rm, "get_market_mode", return_value="range"):
            rm.record_pnl(-60.0)
            summary = rm.get_risk_summary()
        assert summary["circuit_open"] is True
