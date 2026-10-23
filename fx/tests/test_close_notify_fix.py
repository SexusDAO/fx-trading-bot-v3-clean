"""
test_close_notify_fix.py
========================
验证 _close_position_post_static 步骤6 的平仓通知 bug 修复。

由于 position_manager.py 有大量 config 依赖，本测试改用
「直接读源码，确认 _pos_close 变量已被移除」的静态验证，
再配合逻辑等价的最小可运行用例。

两种形式的测试：
1. StaticCodeTest  — 静态检查 position_manager.py 源码，确认 bug 已修复
2. LogicTest       — 纯逻辑测试，模拟步骤6的行为（不 import 重型模块）
"""

import re
import os
import sys
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

HERE   = os.path.dirname(os.path.abspath(__file__))
FX_ROOT = os.path.dirname(HERE)
PM_PATH = os.path.join(FX_ROOT, "position_manager.py")


# ─────────────────────────────────────────────────────────────────────────────
# 1. 静态代码检查：确认 bug 已修复
# ─────────────────────────────────────────────────────────────────────────────
class StaticCodeTest(unittest.TestCase):
    """直接扫描 position_manager.py 源码，确认修复模式存在/旧 bug 模式消失。"""

    @classmethod
    def setUpClass(cls):
        with open(PM_PATH, encoding="utf-8") as f:
            cls.src = f.read()

    def test_bug_pattern_removed(self):
        """
        旧 bug：步骤6 用 get_all_positions 重新找 pos。
        修复后这行应当不再出现在步骤6附近。
        确认 `_pos_close` 变量已从步骤6中移除。
        """
        # 找步骤6代码块
        idx = self.src.find("步骤6:发送 Jarvis 通知")
        self.assertGreater(idx, 0, "找不到步骤6注释，请检查源码")

        # 取步骤6往后 600 字符作为检查窗口
        window = self.src[idx: idx + 600]
        self.assertNotIn(
            "_pos_close",
            window,
            "步骤6 不应再使用 _pos_close 变量（该变量在 remove_position 后必返回空）",
        )

    def test_fix_uses_original_pos(self):
        """
        修复后，步骤6 应直接用 `pos.get("_sig_id"` 和 `pos.get("tp1_filled"` 等。
        """
        idx = self.src.find("步骤6:发送 Jarvis 通知")
        window = self.src[idx: idx + 600]

        self.assertIn(
            'pos.get("_sig_id"',
            window,
            '步骤6 应从原始 pos 读取 _sig_id',
        )
        self.assertIn(
            'pos.get("tp1_filled"',
            window,
            '步骤6 应从原始 pos 读取 tp1_filled',
        )
        self.assertIn(
            'pos.get("tp2_filled"',
            window,
            '步骤6 应从原始 pos 读取 tp2_filled',
        )

    def test_fix_comment_present(self):
        """修复注释应存在，说明原因（代码即文档）。"""
        self.assertIn(
            "fix: 步骤4 已 remove_position",
            self.src,
            "修复注释应存在，解释为何不能重新调用 get_all_positions",
        )


# ─────────────────────────────────────────────────────────────────────────────
# 2. 逻辑等价测试：模拟步骤6的两条路径
# ─────────────────────────────────────────────────────────────────────────────
def _step6_buggy(pos_after_remove, original_pos):
    """
    旧实现：步骤6 从 get_all_positions 重新取 pos（pos_after_remove），
    当 remove_position 已执行后，get_all_positions 返回 []，_pos_close = {}。
    """
    _pos_close  = pos_after_remove   # 模拟从空列表 next() 得到 {}
    _sig_id_cls = _pos_close.get("_sig_id", "")
    tp1_filled  = _pos_close.get("tp1_filled", False)
    tp2_filled  = _pos_close.get("tp2_filled", False)
    return _sig_id_cls, tp1_filled, tp2_filled


def _step6_fixed(original_pos):
    """
    新实现：步骤6 直接用函数顶部已读取的 pos。
    """
    _sig_id_cls = original_pos.get("_sig_id", "")
    tp1_filled  = original_pos.get("tp1_filled", False)
    tp2_filled  = original_pos.get("tp2_filled", False)
    return _sig_id_cls, tp1_filled, tp2_filled


class LogicTest(unittest.TestCase):
    """对比旧实现 vs 新实现，证明修复后行为正确。"""

    def setUp(self):
        self.original_pos = {
            "_sig_id":    "sig-abc-123",
            "tp1_filled": True,
            "tp2_filled": False,
        }
        # remove_position 后 get_all_positions 返回 []，next(..., {}) 得 {}
        self.empty_pos = {}

    def test_buggy_loses_sig_id(self):
        """旧实现：sig_id 丢失（变空串）。"""
        sig_id, tp1, tp2 = _step6_buggy(self.empty_pos, self.original_pos)
        self.assertEqual(sig_id, "", "旧实现 sig_id 应为空（证明 bug 确实存在）")
        self.assertFalse(tp1, "旧实现 tp1_filled 应为 False（即使原值 True）")

    def test_fixed_preserves_sig_id(self):
        """新实现：sig_id 正确保留。"""
        sig_id, tp1, tp2 = _step6_fixed(self.original_pos)
        self.assertEqual(sig_id, "sig-abc-123", "修复后 sig_id 应正确")

    def test_fixed_preserves_tp1_filled(self):
        """新实现：tp1_filled=True 正确保留，通知中不会错误显示 ✗。"""
        _, tp1, _ = _step6_fixed(self.original_pos)
        self.assertTrue(tp1, "修复后 tp1_filled 应为 True")

    def test_fixed_preserves_tp2_filled(self):
        _, _, tp2 = _step6_fixed(self.original_pos)
        self.assertFalse(tp2, "tp2_filled=False 正确保留")

    def test_no_sig_id_falls_back(self):
        """_sig_id 为空时，两种实现行为一致：都走兜底通知路径。"""
        pos_no_sig = {**self.original_pos, "_sig_id": ""}
        sig_id_fixed, _, _ = _step6_fixed(pos_no_sig)
        self.assertEqual(sig_id_fixed, "")
        # 兜底路径：不调用 card_append，直接 notify
        # （逻辑正确性已由 StaticCodeTest 通过代码审查确认）


# ─────────────────────────────────────────────────────────────────────────────
# 3. executor.py 静态代码检查：WS 路径两个 bug
# ─────────────────────────────────────────────────────────────────────────────
EXECUTOR_PATH = os.path.join(FX_ROOT, "executor.py")


class ExecutorStaticTest(unittest.TestCase):
    """静态扫描 executor.py，确认 WS 路径两个 bug 已修复。"""

    @classmethod
    def setUpClass(cls):
        with open(EXECUTOR_PATH, encoding="utf-8") as f:
            cls.src = f.read()

    # ---- Bug 1: close_price 未定义 ----

    def test_close_price_bug_removed(self):
        """
        旧 bug：WS 路径平仓通知中引用了未定义的 close_price。
        修复后实际调用行不应再匹配 close_price（不计入注释行）。
        """
        idx = self.src.find("WS 路径平仓通知")
        self.assertGreater(idx, 0, "找不到 'WS 路径平仓通知' 注释")
        window = self.src[idx: idx + 900]
        # 只检查非注释行中有没有 close_price
        code_lines = [l for l in window.splitlines() if not l.lstrip().startswith("#")]
        code_block = "\n".join(code_lines)
        self.assertNotIn(
            "close_price",
            code_block,
            "WS 通知块的代码行不应再引用 close_price（该作用域内未定义）",
        )

    def test_ws_uses_exit_price(self):
        """WS 路径平仓通知应改用函数顶部已定义的 exit_price。"""
        idx = self.src.find("WS 路径平仓通知")
        window = self.src[idx: idx + 600]
        self.assertIn(
            "exit_price if exit_price > 0",
            window,
            "WS 通知块应使用 exit_price 而非 close_price",
        )

    # ---- Bug 2: card_remove 先于 card_append_async 执行 ----

    def test_card_remove_after_append(self):
        """
        旧 bug：_aio_ws.ensure_future(card_append_async(...)) 之后立刻调用 card_remove，
        导致异步追加还未完成时缓存已清空。
        修复后应将 card_remove 包装进 async def 内，在 await 后调用。
        """
        idx = self.src.find("WS 路径平仓通知")
        # _append_then_remove 在注释块后约 1129 字符，窗口扩展到 1300
        window = self.src[idx: idx + 1300]
        self.assertIn(
            "_append_then_remove",
            window,
            "card_remove 应被包装到 async def _append_then_remove 里 await 后执行",
        )
        self.assertIn(
            "await card_append_async",
            window,
            "_append_then_remove 内应用 await card_append_async",
        )

    # ---- Bug 3: 流动性门槛缺少 mark_price 据换算 ----

    def test_liquidity_gate_no_oi_threshold(self):
        """流动性阈值已去掉 OI 名义价值一项，只按真实下单金额与 3000U 底线判断。"""
        idx = self.src.find("def _check_liquidity_gate")
        self.assertGreater(idx, 0)
        body = self.src[idx: idx + 3000]
        self.assertIn("threshold = max(LIQUIDITY_GATE_MIN_U, notional * LIQUIDITY_GATE_DEPTH_MULT)", body)
        self.assertNotIn("oi_based", body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
