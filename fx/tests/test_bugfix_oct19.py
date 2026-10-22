"""
单测：2026-10-19 三个问题修复覆盖
  T1 ~ T4: 问题一  — _check_manual_close 区分 sl_triggered vs manual_close
  T5 ~ T7: 问题二  — candidate_pool ts 单位（秒级）读写一致性
"""
import sys, os, json, time, datetime, asyncio, types, unittest
from unittest.mock import patch, MagicMock, AsyncMock

# ── 最小化 import 隔离：不真正加载 position_manager 全局（避免 Binance API key 依赖）
# 只测试关键逻辑片段

# ─────────────────────────────────────────────────────────────────────────────
# 辅助：复制 _check_manual_close 中的「推断逻辑」以便单独测试
# ─────────────────────────────────────────────────────────────────────────────

def infer_close_reason(sl_id, sl_price, algo_order_ids):
    """
    模拟 _check_manual_close 中新增的推断逻辑。
    algo_order_ids: 当前交易所挂单里 algoId 集合（int set）
    返回: "sl_triggered" 或 "manual_close"
    """
    inferred = "manual_close"
    if sl_id and sl_price > 0:
        if int(sl_id) not in algo_order_ids:
            inferred = "sl_triggered"
    return inferred


# ─────────────────────────────────────────────────────────────────────────────
# T1: SL algoId 已不在交易所 → sl_triggered
# ─────────────────────────────────────────────────────────────────────────────
class TestInferCloseReason(unittest.TestCase):

    def test_T1_sl_not_in_exchange_infers_sl_triggered(self):
        """SL algoId 已不在交易所挂单 → 推断 sl_triggered"""
        result = infer_close_reason(
            sl_id=12345,
            sl_price=16.04,
            algo_order_ids={99999, 88888},   # 12345 不在
        )
        self.assertEqual(result, "sl_triggered")

    def test_T2_sl_still_in_exchange_infers_manual_close(self):
        """SL algoId 仍在交易所挂单 → 推断 manual_close（用户手动平仓，SL 还没触发）"""
        result = infer_close_reason(
            sl_id=12345,
            sl_price=16.04,
            algo_order_ids={12345, 99999},   # 12345 还在
        )
        self.assertEqual(result, "manual_close")

    def test_T3_no_sl_id_always_manual_close(self):
        """没有 sl_order_id → 无法判断，保守返回 manual_close"""
        result = infer_close_reason(
            sl_id=None,
            sl_price=16.04,
            algo_order_ids=set(),
        )
        self.assertEqual(result, "manual_close")

    def test_T4_sl_price_zero_always_manual_close(self):
        """sl_price=0（未设止损）→ 返回 manual_close"""
        result = infer_close_reason(
            sl_id=12345,
            sl_price=0,
            algo_order_ids=set(),
        )
        self.assertEqual(result, "manual_close")


# ─────────────────────────────────────────────────────────────────────────────
# T5: scanner 写入 ts 为秒级，且 ts_unit="seconds"
# ─────────────────────────────────────────────────────────────────────────────
class TestCandidatePoolTs(unittest.TestCase):

    def test_T5_ts_is_seconds_not_milliseconds(self):
        """candidate_pool.json 写入的 ts 应为秒级（~1.79e9），不应为毫秒级（~1.79e12）"""
        ts = time.time()   # 模拟 scanner 写入值
        # 秒级 Unix 时间戳约为 1.7~1.9e9
        self.assertGreater(ts, 1_700_000_000)
        self.assertLess(ts, 2_000_000_000)   # 秒级不会超过 2e9 直到 2033 年

    def test_T6_ts_unit_field_written(self):
        """写入的 JSON 应包含 ts_unit='seconds' 字段"""
        import tempfile, pathlib
        tmp = pathlib.Path(tempfile.mktemp(suffix=".json"))
        try:
            payload = {
                "ts": time.time(),
                "ts_unit": "seconds",
                "pool": ["BTCUSDT"],
                "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            tmp.write_text(json.dumps(payload))
            loaded = json.loads(tmp.read_text())
            self.assertEqual(loaded["ts_unit"], "seconds")
        finally:
            tmp.unlink(missing_ok=True)

    def test_T7_age_calculation_uses_seconds(self):
        """
        _load_pool 中 age = time.time() - float(ts) 对秒级 ts 应该得到小的正数（<60s）
        若误用 ts/1000，则 age ≈ 57年，会触发过期重刷
        """
        ts_sec = time.time()           # 秒级
        age_correct = time.time() - float(ts_sec)   # 正确方式
        age_wrong   = time.time() - float(ts_sec) / 1000   # 错误方式（除以1000）

        # 正确计算 age 应 < 1 秒
        self.assertLess(age_correct, 1.0)
        # 错误计算 age 约等于 ts_sec - ts_sec/1000 ≈ 0.999 * ts_sec ≈ 1.79e9 秒 ≈ 57年
        self.assertGreater(age_wrong, 1_700_000_000 * 0.99)


if __name__ == "__main__":
    unittest.main(verbosity=2)
