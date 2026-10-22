"""运行：cd fx && python3 -m unittest tests.test_news_signal_entities -v
[v3.89 #77] 新闻实体词边界匹配、别名表、宏观关键词词边界。全部 mock，不访问网络。"""
import logging, sys, time, unittest
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import news_signal as N


class TestWordBoundary(unittest.TestCase):
    def test_short_ticker_inside_word_not_mentioned(self):
        for base, text in (("ONE", "the money market"), ("ARB", "arbitrage desk"), ("SUN", "sunday report"),
                           ("ETC", "etching process"), ("AVAX", "avaxx unrelated")):
            with self.subTest(base=base):
                self.assertFalse(N._ticker_mentioned(text, base))

    def test_real_mentions_hit(self):
        for base, text in (("ONE", "harmony one rallies"), ("SUN", "$sun jumps"), ("ARB", "arb token up"),
                           ("PEPE", "pepe is trending"), ("ARB", "(arb) listed"), ("SOL", "sols everywhere")):
            with self.subTest(base=base):
                self.assertTrue(N._ticker_mentioned(text, base))

    def test_1000_prefix_aliases(self):
        for base, word in (("1000PEPE", "pepe"), ("1000SHIB", "shib"), ("1000BONK", "bonk"), ("1000FLOKI", "floki")):
            with self.subTest(base=base):
                self.assertTrue(N._ticker_mentioned(f"{word} surges", base))
                self.assertFalse(N._ticker_mentioned("unrelated news", base))
        self.assertTrue(N._ticker_mentioned("1000pepe perp listed", "1000PEPE"))

    def test_macro_keywords_use_boundaries(self):
        # 旧写法：method 含 eth、together 含 eth → 被判成宏观新闻
        self.assertFalse(N._is_btc_macro_irrelevant("a new method works together", "XYZ"))
        # 真正的宏观新闻、且未提到本 ticker → 无关
        self.assertTrue(N._is_btc_macro_irrelevant("Marathon Digital moves 1000 BTC to exchange", "XYZ"))
        self.assertTrue(N._is_btc_macro_irrelevant("bitcoins and ethereum slide", "XYZ"))
        self.assertTrue(N._is_btc_macro_irrelevant("MARA sends BTC to exchange", "XYZ"))
        # 提到了本 ticker → 相关，不过滤
        self.assertFalse(N._is_btc_macro_irrelevant("BTC miner news and xyz hacked", "XYZ"))
        # BTC/ETH 自身不过滤
        self.assertFalse(N._is_btc_macro_irrelevant("bitcoin whale moves", "BTC"))

    def test_short_name_inside_other_word_no_longer_counts_as_mentioned(self):
        # 旧写法 "one" in "money" 为真 → 宏观新闻被误判为与 ONE 相关而保留 bearish
        self.assertTrue(N._is_btc_macro_irrelevant("BTC miner sells, big money moves", "ONE"))


if __name__ == "__main__":
    unittest.main()
