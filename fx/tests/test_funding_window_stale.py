"""运行：cd fx && python3 -m unittest tests.test_funding_window_stale -v
[v3.89 #72] symbol_funding_window：结算前 / 恰好结算 / 刚结算后 / 过期快照。窗口大小不变。"""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.opt_v385 import symbol_funding_window as W

H = 3600.0
PRE, POST = 300.0, 600.0       # 5 分钟前 / 10 分钟后
NFT = 1_000_000_000.0


class TestWindow(unittest.TestCase):
    def test_before_settlement(self):
        self.assertTrue(W(NFT - 120, NFT, 8, PRE, POST)[0])
        self.assertFalse(W(NFT - 900, NFT, 8, PRE, POST)[0])

    def test_exactly_at_settlement_is_pre_window(self):
        ok, why = W(NFT, NFT, 8, PRE, POST)
        self.assertTrue(ok); self.assertIn("距本标的结算", why)

    def test_just_after_with_fresh_next_funding(self):
        # 正常快照：下次结算已更新为 +8h，刚结算 2 分钟
        ok, why = W(NFT + 120, NFT + 8 * H, 8, PRE, POST)
        self.assertTrue(ok); self.assertIn("结算后", why)
        self.assertFalse(W(NFT + 1200, NFT + 8 * H, 8, PRE, POST)[0])        # 超出 post 窗口

    def test_stale_snapshot_just_after_settlement(self):
        """快照里的 nextFundingTime 仍是刚过去的结算点：旧实现漏掉结算后窗口。"""
        ok, why = W(NFT + 120, NFT, 8, PRE, POST)
        self.assertTrue(ok); self.assertIn("结算后 2 分钟", why); self.assertIn("过期", why)
        self.assertFalse(W(NFT + 1200, NFT, 8, PRE, POST)[0])                 # 窗口大小未变

    def test_stale_snapshot_far_behind_normalizes_by_periods(self):
        # 快照落后 2 个周期又 2 分钟（1H 标的）
        ok, why = W(NFT + 2 * H + 120, NFT, 1, PRE, POST)
        self.assertTrue(ok); self.assertIn("结算后 2 分钟", why)
        # 落后 2 个周期又 58 分钟 → 距下次结算 2 分钟，落入结算前窗口
        ok, why = W(NFT + 2 * H + 58 * 60, NFT, 1, PRE, POST)
        self.assertTrue(ok); self.assertIn("距本标的结算还有 2 分钟", why)

    def test_stale_snapshot_in_quiet_period_is_false(self):
        self.assertFalse(W(NFT + 3 * H, NFT, 8, PRE, POST)[0])

    def test_invalid_next_funding_unchanged(self):
        self.assertEqual(W(NFT, 0, 8, PRE, POST), (False, ""))


if __name__ == "__main__":
    unittest.main()
