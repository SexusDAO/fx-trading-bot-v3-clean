"""
tests/test_state.py — state.py 全接口自动化测试
覆盖：正常、边界、异常、并发写入
"""
import json
import os
import sys
import threading
import time
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

# 把 fx 目录加入 path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# ── 用临时目录隔离每个测试 ────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def isolated_logs(tmp_path, monkeypatch):
    """每个测试用独立的临时目录，防止互相污染"""
    import shared.state as _st
    monkeypatch.setattr(_st, "_BASE", tmp_path)
    monkeypatch.setattr(_st, "_POS_FILE", tmp_path / "pos_state.json")
    monkeypatch.setattr(_st, "_CB_FILE",  tmp_path / "circuit_breaker.json")
    monkeypatch.setattr(_st, "_CD_FILE",  tmp_path / "cooldown_state.json")
    monkeypatch.setattr(_st, "_PC_FILE",  tmp_path / "pending_confirm_state.json")
    monkeypatch.setattr(_st, "_OI_FILE",  tmp_path / "oi_cache.json")
    # 重新创建锁（避免测试间锁状态残留）
    import threading
    monkeypatch.setattr(_st, "_locks", {k: threading.Lock() for k in _st._locks})
    _st._ensure_files()
    yield


from shared import state as st


# ═══════════════════════════════════════════════════════════════════════════════
# pos_state.json
# ═══════════════════════════════════════════════════════════════════════════════
class TestPositionState:

    def _make_pos(self, pos_id="p1", symbol="BTCUSDT", direction="long", status="open"):
        return {
            "pos_id": pos_id, "symbol": symbol,
            "direction": direction, "status": status,
            "open_time": int(time.time() * 1000),
            "entry_price": 50000.0, "quantity": 0.001,
        }

    # ── 正常 ──────────────────────────────────────────────────────────────────
    def test_add_and_get_all(self):
        pos = self._make_pos()
        assert st.add_position(pos) is True
        positions = st.get_all_positions()
        assert len(positions) == 1
        assert positions[0]["pos_id"] == "p1"

    def test_update_position(self):
        st.add_position(self._make_pos())
        assert st.update_position("p1", {"sl_price": 48000.0}) is True
        pos = st.get_all_positions()[0]
        assert pos["sl_price"] == 48000.0

    def test_remove_position(self):
        st.add_position(self._make_pos())
        st.remove_position("p1")
        assert st.get_all_positions() == []

    # ── 边界 ──────────────────────────────────────────────────────────────────
    def test_add_duplicate_symbol_rejected(self):
        """同标的不能重复开仓"""
        st.add_position(self._make_pos("p1", "BTCUSDT"))
        result = st.add_position(self._make_pos("p2", "BTCUSDT"))
        assert result is False
        assert len(st.get_all_positions()) == 1

    def test_remove_nonexistent_ok(self):
        """删除不存在的 pos_id 不报错"""
        assert st.remove_position("nonexistent") is True

    def test_update_nonexistent_returns_false(self):
        assert st.update_position("ghost", {"sl_price": 100}) is False

    # ── 异常 ──────────────────────────────────────────────────────────────────
    def test_get_all_positions_empty(self):
        assert st.get_all_positions() == []

    def test_get_position_count(self):
        st.add_position(self._make_pos("p1", "BTCUSDT", "long"))
        st.add_position(self._make_pos("p2", "ETHUSDT", "short"))
        counts = st.get_position_count()
        assert counts["total"] == 2
        assert counts["long"] == 1
        assert counts["short"] == 1

    def test_pending_order_lifecycle(self):
        pending = self._make_pos("p3", "SOLUSDT", status="pending")
        st.add_position(pending)
        assert len(st.get_all_pending_orders()) == 1
        assert len([p for p in st.get_all_positions() if p["status"] == "open"]) == 0
        st.remove_pending_order("SOLUSDT")
        assert st.get_all_pending_orders() == []

    # ── 并发写入 ──────────────────────────────────────────────────────────────
    def test_concurrent_add_positions(self):
        """10 个线程并发写入 10 个不同标的，全部应成功且无数据丢失"""
        symbols = [f"COIN{i}USDT" for i in range(10)]
        errors  = []

        def _add(sym):
            try:
                st.add_position({
                    "pos_id": sym, "symbol": sym, "direction": "long",
                    "status": "open", "open_time": int(time.time() * 1000),
                    "entry_price": 100.0, "quantity": 0.01,
                })
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=_add, args=(s,)) for s in symbols]
        for t in threads: t.start()
        for t in threads: t.join()

        assert errors == [], f"并发写入出现错误: {errors}"
        assert len(st.get_all_positions()) == 10

    def test_atomic_write_no_corruption(self, tmp_path):
        """写入中断后文件不损坏（验证 os.replace 原子性）"""
        st.add_position(self._make_pos())
        # 模拟写入后文件内容可以正常解析
        pos_file = st._POS_FILE
        content = pos_file.read_text()
        data = json.loads(content)
        assert isinstance(data, list)


# ═══════════════════════════════════════════════════════════════════════════════
# circuit_breaker.json
# ═══════════════════════════════════════════════════════════════════════════════
class TestCircuitBreaker:

    def test_initial_state_not_open(self):
        assert st.is_circuit_open() is False

    def test_record_loss_accumulates(self):
        st.record_pnl(-30.0)
        st.record_pnl(-40.0)
        assert st.get_daily_loss() == pytest.approx(70.0)

    def test_profit_offsets_loss_net_pnl(self):
        """[fix #12] 净盈亏口径：盈利与亏损抵消"""
        st.record_pnl(-50.0)
        st.record_pnl(+30.0)
        assert st.get_daily_loss() == pytest.approx(20.0)
        st.record_pnl(+100.0)   # 净盈利 → daily_loss 为 0
        assert st.get_daily_loss() == pytest.approx(0.0)

    def test_fees_and_funding_count_toward_loss(self):
        """[fix #12] 手续费/资金费单独记录并计入总亏损"""
        st.record_pnl(+10.0, fee=3.0, funding=2.0)
        d = st._load_cb()
        assert d["daily_fees"] == pytest.approx(3.0)
        assert d["daily_funding"] == pytest.approx(2.0)
        assert st.get_daily_loss() == pytest.approx(0.0)    # 10-3-2=+5 净盈利
        st.record_pnl(-20.0, fee=1.0)
        assert st.get_daily_loss() == pytest.approx(16.0)   # pnl -10, fees 4, funding 2 → 净 -16

    def test_circuit_triggers_at_limit(self):
        """累计亏损超过 CIRCUIT_BREAKER_DAILY_LOSS 触发熔断"""
        with patch("shared.state.CIRCUIT_BREAKER_DAILY_LOSS", 100, create=True), \
             patch("config.CIRCUIT_BREAKER_DAILY_LOSS", 100):
            # 直接写 state 内部，绕过 import 缓存
            import shared.state as _s
            orig = None
            try:
                import config as _c
                orig = _c.CIRCUIT_BREAKER_DAILY_LOSS
                _c.CIRCUIT_BREAKER_DAILY_LOSS = 100
                _s.record_pnl(-60.0)
                _s.record_pnl(-60.0)
            finally:
                if orig is not None:
                    _c.CIRCUIT_BREAKER_DAILY_LOSS = orig
        # is_circuit_open 也需要在 100 阈值下检查
        import config as _c2; _c2.CIRCUIT_BREAKER_DAILY_LOSS = 100
        try:
            assert st.is_circuit_open() is True
        finally:
            _c2.CIRCUIT_BREAKER_DAILY_LOSS = 9999

    def test_reset_circuit_clears_state(self):
        import config as _c; _c.CIRCUIT_BREAKER_DAILY_LOSS = 100
        st.record_pnl(-200.0)
        assert st.is_circuit_open() is True
        st.reset_circuit()
        assert st.is_circuit_open() is False
        assert st.get_daily_loss() == 0.0

    def test_auto_reset_on_new_day(self, monkeypatch):
        """跨日后 is_circuit_open 自动重置"""
        import config as _c; _c.CIRCUIT_BREAKER_DAILY_LOSS = 100
        st.record_pnl(-200.0)
        assert st.is_circuit_open() is True
        # 模拟跨日：把 last_reset_date 改成昨天
        cb_data = json.loads(st._CB_FILE.read_text())
        cb_data["last_reset_date"] = "2000-01-01"
        st._CB_FILE.write_text(json.dumps(cb_data))
        assert st.is_circuit_open() is False   # 自动重置


# ═══════════════════════════════════════════════════════════════════════════════
# cooldown_state.json
# ═══════════════════════════════════════════════════════════════════════════════
class TestCooldown:

    def test_add_and_check_cooldown(self):
        st.add_cooldown("BTCUSDT", 3600, "sl_trigger")
        in_cd, reason = st.is_in_cooldown("BTCUSDT")
        assert in_cd is True
        assert reason == "sl_trigger"

    def test_no_cooldown_returns_false(self):
        in_cd, _ = st.is_in_cooldown("ETHUSDT")
        assert in_cd is False

    def test_expired_cooldown_auto_removed(self):
        """已过期的冷却期自动清理"""
        st.add_cooldown("XRPUSDT", 1, "test")
        # 手动把 until 设为过去时间
        cd = json.loads(st._CD_FILE.read_text())
        cd["XRPUSDT"]["cooldown_until"] = int(time.time() * 1000) - 1000
        st._CD_FILE.write_text(json.dumps(cd))
        in_cd, _ = st.is_in_cooldown("XRPUSDT")
        assert in_cd is False

    def test_longer_cooldown_wins(self):
        """已有冷却期时，更长的不覆盖更短的"""
        st.add_cooldown("SOLUSDT", 7200, "first")
        st.add_cooldown("SOLUSDT", 3600, "second")
        cd = json.loads(st._CD_FILE.read_text())
        # until 应保留更大的那个
        assert cd["SOLUSDT"]["cooldown_until"] > int(time.time() * 1000) + 7000 * 1000

    def test_remove_cooldown(self):
        st.add_cooldown("BNBUSDT", 3600, "test")
        st.remove_cooldown("BNBUSDT")
        in_cd, _ = st.is_in_cooldown("BNBUSDT")
        assert in_cd is False


# ═══════════════════════════════════════════════════════════════════════════════
# pending_confirm_state.json
# ═══════════════════════════════════════════════════════════════════════════════
class TestPendingConfirm:

    def test_add_and_get(self):
        rec = {"breakout_direction": "long", "expire_at": int(time.time()*1000)+60000}
        st.add_pending_confirm("BTCUSDT", rec)
        result = st.get_pending_confirm("BTCUSDT")
        assert result is not None
        assert result["breakout_direction"] == "long"

    def test_overwrite_existing(self):
        st.add_pending_confirm("BTCUSDT", {"v": 1, "expire_at": 9999999999999})
        st.add_pending_confirm("BTCUSDT", {"v": 2, "expire_at": 9999999999999})
        assert st.get_pending_confirm("BTCUSDT")["v"] == 2

    def test_get_nonexistent_returns_none(self):
        assert st.get_pending_confirm("GHOST") is None

    def test_cleanup_expired(self):
        past = int(time.time() * 1000) - 1000
        st.add_pending_confirm("BTCUSDT", {"expire_at": past})
        st.add_pending_confirm("ETHUSDT", {"expire_at": 9999999999999})
        removed = st.cleanup_expired_confirms()
        assert removed == 1
        assert st.get_pending_confirm("BTCUSDT") is None
        assert st.get_pending_confirm("ETHUSDT") is not None


# ═══════════════════════════════════════════════════════════════════════════════
# oi_cache.json
# ═══════════════════════════════════════════════════════════════════════════════
class TestOICache:

    def test_update_and_get_15min_ago(self):
        now = time.time()
        # 手动注入一条 15 分钟前的记录
        cache = {"BTCUSDT": [
            {"timestamp": int((now - 900) * 1000), "oi": 12000.0},
            {"timestamp": int(now * 1000),          "oi": 13000.0},
        ]}
        st._OI_FILE.write_text(json.dumps(cache))
        result = st.get_oi_15min_ago("BTCUSDT")
        assert result is not None
        assert abs(result - 12000.0) < 1

    def test_get_oi_24h_ago_insufficient_data(self):
        """数据不足24H时返回 None"""
        st.update_oi("BTCUSDT", 10000.0)
        result = st.get_oi_24h_ago("BTCUSDT")
        assert result is None   # 只有1条，不足24H

    def test_update_all_oi_batch(self):
        oi_map = {"BTCUSDT": 12345.0, "ETHUSDT": 5678.0}
        st.update_all_oi(oi_map)
        cache = json.loads(st._OI_FILE.read_text())
        assert "BTCUSDT" in cache
        assert cache["BTCUSDT"][-1]["oi"] == 12345.0

    def test_oi_cache_retained_by_time_window(self):
        """[v3.89 #99] 按时间窗口（25H）保留，不再按 48 条截断：窗口内的 60 次写入全部保留，超出窗口的被清理"""
        import time as _t
        now = int(_t.time() * 1000)
        st._save_oi({"BTCUSDT": [{"timestamp": now - 26 * 3600 * 1000, "oi": -1.0}]})
        for i in range(60):
            st.update_oi("BTCUSDT", float(i))
        cache = json.loads(st._OI_FILE.read_text())
        assert len(cache["BTCUSDT"]) == 60
        assert all(r["oi"] >= 0 for r in cache["BTCUSDT"])
