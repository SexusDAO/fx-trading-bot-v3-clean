"""
news_signal.py — Binance Square 新闻信号模块 v1.0
==================================================
用途：
  1. 拉取 square search（官方新闻）和 square news（专案公告）
  2. 按关键词分类，输出 {symbol: "bullish" | "bearish" | None}
  3. 供 scanner / signal_watcher 使用

豁免逻辑：
  - bullish  → LONG 方向额外豁免（BTC 熊市豁免加一条）
  - bearish  → LONG 一票否决；SHORT 额外豁免

更新方式：
  - 每次调用 fetch_news_bias(symbols) 返回 dict
  - 内部带 TTL 缓存（默认 900s），避免高频请求
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import subprocess
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
#  关键词配置
#  设计原则：
#  利好（bullish）= 对代币有直接价格催化的事件
#  利空（bearish）= 对代币有直接砸盘/信任损失的事件
#  模糊词（功能更新、路线图等）= 无权重，忽略
# ──────────────────────────────────────────────

BULLISH_PATTERNS: list[tuple[str, str]] = [
    # (正则, 分类说明)

    # 上架/交易所
    (r"\b(listed|listing|launch(?:es|ed|ing)? on)\b.{0,40}\b(exchange|binance|coinbase|okx|bybit|kraken|bitget|gate\.io|mexc|kucoin|huobi)\b",
     "listed_on_exchange"),
    (r"\b(spot listing|spot launch|spot pair)\b", "spot_listing"),
    (r"\b(perpetual|perp|futures)\b.{0,20}\b(listed|launch|add)\b", "perp_listing"),

    # 现货直接上币安
    (r"binance.{0,30}\b(list|add|launch|spot|pairs?)\b", "binance_listing"),
    (r"\b(added to|available on)\b.{0,30}binance", "binance_listing"),

    # 机构买入 / 大额持仓
    (r"\b(bought?|purchased?|acqui[re|red|ring]+|holding[s]?|adds?)\b.{0,50}\b(million|billion|[0-9,]+\s*(btc|eth|sol|usdt))\b",
     "institution_buy"),
    (r"\b(treasury|reserve|fund)\b.{0,30}\b(buys?|acqui|add|holds?)\b", "institution_buy"),
    (r"\bcrypto reserve\b", "institution_buy"),

    # 融资 / 战略投资
    # 注意：要排除 'bounty to attacker / return funds' 这类误判——赏金是黑客之后的局面，不是融资
    (r"\b(raise\w*|funding|investment|invest\w*|series [abcABC])\b.{0,80}\b(million|billion|[0-9]+[MBK])\b"
     r"(?!.{0,60}\b(attacker|hacker|exploit|bounty|return))",
     "funding"),
    (r"[\$\£\€][0-9]+[\.,]?[0-9]*\s*(M|B|million|billion).{0,60}\b(raise|round|fund|invest)"
     r"(?!.{0,60}\b(attacker|hacker|exploit|bounty|return))",
     "funding"),
    (r"\bstrategic investment\b", "funding"),
    (r"\bvaluation.{0,30}\b(billion|million)\b", "funding"),
    (r"\b(ipo|initial public offering)\b", "funding"),

    # 大额销毁
    (r"\b(burn[s|ed|ing]+|burnt|destroyed)\b.{0,50}\b(million|billion|[0-9,]+\s*(token|coin))\b",
     "token_burn"),
    (r"\btoken burn\b", "token_burn"),

    # 回购
    (r"\b(buyback|buy.?back|repurchase)\b", "buyback"),

    # ETF / 合规产品
    (r"\b(etf|etf approval|spot etf|futures etf)\b", "etf"),
    (r"\b(sec approval|sec approves?|sec clears?|cftc approves?)\b", "regulation_positive"),

    # 合作 / 整合大厂
    (r"\b(partnership|partner(?:s|ed|ing)|collaboration|integrat[es|ed|ing]+)\b.{0,50}"
     r"\b(microsoft|google|apple|amazon|meta|nvidia|jp morgan|blackrock|fidelity|paypal|visa|mastercard|samsung|sony)\b",
     "bigtech_partnership"),

    # 巨鲸大量买入（Lookonchain 报道买入）
    (r"\b(whale|smart money|large wallet)\b.{0,80}\b(buy|bought|long|accumulate)\b",
     "whale_buy"),
    (r"lookonchain.{0,80}\b(buy|bought|long|accumulate|add)\b", "whale_buy"),
    # 多钱包/地址志廷仿位/持有未实现利润（肚气聚集信号）
    (r"\b(wallet[s]?|address[es]*)\b.{0,80}\b(long[s]?|accumulate|unrealized profit)\b",
     "whale_buy"),

    # 聪明钱加仓
    (r"\b(smart money|institutional)\b.{0,40}\b(add|buy|long|accumulate)\b", "smart_money_buy"),
]

BEARISH_PATTERNS: list[tuple[str, str]] = [
    # (正则, 分类说明)

    # 黑客攻击 / 漏洞
    (r"\b(hacked?|exploit\w*|breach\w*)\b",
     "hack_exploit"),
    # 'attack' 需要配合币圈语境，避免 'team confirms attack' 这类确认消息误判
    (r"\b(suffer\w*|under)\b.{0,40}\b(attack|exploit|hack)",
     "hack_exploit"),
    (r"\b(security vulnerability|security incident|security breach)\b", "hack_exploit"),
    (r"\b(stolen|theft|drained)\b.{0,40}\b(million|billion|[0-9,]+\s*(usdt|usd|eth|btc))\b",
     "hack_exploit"),
    (r"\b(funds?|token[s]?)\b.{0,30}\b(stolen|drained|compromised)\b",
     "hack_exploit"),

    # 大量转入交易所（砸盘信号）
    (r"\b(deposit\w*|transfer\w*|send\w*|sent|move\w*)\b.{0,60}\b(exchange\w*|binance|coinbase|okx|bybit|bitget|gate\.io|kraken|mexc|kucoin)",
     "exchange_inflow"),
    (r"\b(dev|team|project|founder|whale|wallet)\b.{0,60}\b(deposit\w*|transfer\w*|send\w*|move\w*)\b.{0,40}"
     r"\b(exchange\w*|cex)", "exchange_inflow"),
    (r"lookonchain.{0,120}\b(deposit\w*|transfer\w*|sell|sold|dump|move\w*)\b.{0,80}\b(exchange\w*|million|billion)",
     "exchange_inflow"),

    # 项目方卖出
    (r"\b(dev|team|project|founder|insider)\b.{0,60}\b(sell|sold|dump[ed|s|ing]*|close position|exit)\b",
     "dev_sell"),
    (r"\bdev close position\b", "dev_sell"),
    (r"\b(rug|rugpull|rug pull|exit scam)\b", "dev_sell"),

    # 下架
    (r"\b(delist[ed|s|ing]*|removed? from|suspend[ed|s|ing]* trading)\b", "delist"),
    (r"\btrading suspension\b", "delist"),

    # 监管打压
    (r"\b(crackdown|enforcement action|sec charges?|doj charges?|arrested?)\b",
     "regulation_negative"),
    # 'ban' 需要和币圈/交易行为共现才算（避免 'ban on office' 这类误判）
    (r"\b(ban[s]?|banned?)\b.{0,60}\b(crypto|token|trading|exchange|blockchain|defi|nft|stablecoin)\b",
     "regulation_negative"),
    (r"\b(crypto|token|trading|exchange|blockchain)\b.{0,60}\b(ban[s]?|banned?|prohibited?|illegal)\b",
     "regulation_negative"),
    (r"\b(money laundering|fraud|scam|ponzi|pyramid)\b.{0,40}\b(charged?|arrested?|sued?|investigation)\b",
     "regulation_negative"),

    # 严重 Bug / 系统崩溃
    (r"\b(critical bug|critical vulnerability|zero.?day|rce|remote code execution)\b",
     "critical_bug"),
    (r"\b(network down|chain halt[ed]?|validator jail[ed]?|consensus fail[ure|s|ed]+)\b",
     "critical_bug"),
    (r"\btoken supply (increased|inflated|manipulated)\b", "critical_bug"),

    # 巨鲸大量卖出
    (r"\b(whale|smart money|large wallet)\b.{0,80}\b(sell|sold|dump|short|exit)\b",
     "whale_sell"),
    (r"lookonchain.{0,80}\b(sell|sold|dump|short|deposit|transfer|move)\b.{0,60}\b(exchang|million|billion)",
     "whale_sell"),

    # wash trading / 操纵
    (r"\b(wash trading|market manipulation|insider trading)\b", "wash_trading"),
    (r"\b(zach.?xbt|zachxbt)\b.{0,80}\b(accuse|expose|allege|manipulate|scam)\b",
     "wash_trading"),

    # 合约审计严重问题
    (r"\b(audit fail[ed]?|audit reveals?|critical (issue|flaw|risk))\b", "audit_fail"),
]

# 明确无效（过滤掉避免误判）的噪音关键词
NOISE_PATTERNS: list[str] = [
    r"\b(roadmap|roadmap update|feature update|product update|ui update|app update)\b",
    r"\b(rebrand[ing]?|logo change|name change)\b",
    r"\b(airdrop|giveaway|contest|competition)\b",
    r"\b(testnet|devnet|beta|alpha version)\b",

    # 攻击后的快速响应措施：本身不影响币价，不当成利好信号
    # （用户沈沉成本已发生，这些是追赋阶段的天花板）
    r"\b(pause[sd]?|suspend[ed]?|halt[ed]?)\b.{0,40}\b(contract[s]?|protocol|trading|withdraw)\b",
    # patch/fix 双向匹配：vulnerability patched 和 patch deployed 均可命中
    r"\b(patch\w*|fix\w*|resolv\w*)\b.{0,60}\b(vulnerability|bug|service|operation)\b",
    r"\b(vulnerability|bug)\b.{0,40}\b(patch\w*|fix\w*|resolv\w*)\b",
    r"\b(emergency patch|patch deployed|hotfix)\b",
    r"\b(compensat\w*|reimburse\w*|refund\w*)\b.{0,40}\b(user[s]?|victim[s]?|affected)\b",
    r"\b(restor\w*|resume[sd]?|reopen\w*|back online)\b.{0,50}\b(operation|service|trading|network|protocol)\b",
    r"\b(fully restor\w*|network restor\w*|service restor\w*|fully operational)\b",
    r"\b(bounty|bug bounty|whitehack)\b",
    r"\b(post.?mortem|post mortem|incident report)\b",
]

# ──────────────────────────────────────────────
#  内部缓存
# ──────────────────────────────────────────────
# 结构: symbol -> (query_ts, bias, oldest_news_ts)
#   query_ts:       本次查询时间（用于 TTL 过期判断）
#   bias:           "bullish" / "bearish" / None
#   oldest_news_ts: bearish类新闻里最早一条的发布时间（第三元素，为0表示未知）
_cache: dict[str, tuple[float, str | None, float]] = {}
_CACHE_TTL = 900  # 15 分钟
_GLOBAL_NEWS_CACHE: tuple[float, list] = (0.0, [])
_GLOBAL_NEWS_TTL = 600  # 10 分钟全量新闻缓存


def _run_cli(args: list[str], timeout: int = 30) -> dict:
    """同步运行 CLI，返回解析后的 JSON dict 或 {}"""
    try:
        r = subprocess.run(
            ["binance-pro-cli"] + args,
            capture_output=True, text=True, timeout=timeout
        )
        if r.returncode != 0:
            logger.debug(f"[news_signal] CLI 非零返回: {r.stderr[:200]}")
            return {}
        return json.loads(r.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
        logger.debug(f"[news_signal] CLI 失败: {e}")
        return {}


def _fetch_raw_news(symbol_base: str) -> list[dict]:
    """
    拉取与 symbol_base（如 BAT / DEXE）相关的官方新闻。
    优先用 square search 按关键词搜，再用 square news tickers 过滤。
    """
    all_items: list[dict] = []

    # 1. square search —— 关键词搜索，命中率最高
    for window in ("4h", "24h"):
        data = _run_cli([
            "square", "search",
            "--keyword", symbol_base,
            "--window", window,
            "--format", "json", "--no-compact",
            "--page-size", "20",
        ])
        items = data.get("items", [])
        # 只取官方新闻（contentType=2 = Binance News；authorRole 7）
        news = [i for i in items if i.get("contentType") == 2]
        all_items.extend(news)
        if window == "4h" and len(news) >= 3:
            break  # 4h 已够用，不继续拉 24h

    # 2. 补充 square news（带 ticker 标签的公告流）
    global _GLOBAL_NEWS_CACHE
    now = time.time()
    if now - _GLOBAL_NEWS_CACHE[0] > _GLOBAL_NEWS_TTL:
        data2 = _run_cli([
            "square", "news",
            "--format", "json", "--no-compact",
            "--page-size", "50",
        ])
        _GLOBAL_NEWS_CACHE = (now, data2.get("items", []))

    sym_upper = symbol_base.upper() + "USDT"
    ticker_news = [
        i for i in _GLOBAL_NEWS_CACHE[1]
        if sym_upper in (i.get("tickers") or [])
        or symbol_base.upper() in (i.get("tickers") or [])
    ]
    all_items.extend(ticker_news)

    # 3. 额外用 Lookonchain 关键词搜（最相关的大额转入预警）
    lc_data = _run_cli([
        "square", "search",
        "--keyword", f"Lookonchain {symbol_base}",
        "--window", "24h",
        "--format", "json", "--no-compact",
        "--page-size", "10",
    ])
    lc_news = [i for i in lc_data.get("items", []) if i.get("contentType") == 2]
    all_items.extend(lc_news)

    # 去重（按 id）
    seen: set[str] = set()
    deduped: list[dict] = []
    for item in all_items:
        _id = item.get("id", "")
        if _id and _id not in seen:
            seen.add(_id)
            deduped.append(item)

    return deduped


def _classify_text(text: str) -> str | None:
    """
    对单条新闻文本进行分类。
    返回 "bullish" / "bearish" / "bearish_with_response" / None

    bearish_with_response：攻击事件但同一条新闻同时包含快速响应措施词
    优先级：bearish > bullish（利空一票否决利好）
    """
    t = text.lower()

    # 先过滤噪音（避免功能更新等误判为利好）
    is_noise = any(re.search(p, t) for p in NOISE_PATTERNS)
    # 如果更多的噪音词具备快速响应特征，标记
    is_response = is_noise  # 噪音正则包含了快速响应词，直接复用

    # 检测利空
    is_bearish = any(re.search(p, t, re.IGNORECASE) for p, _ in BEARISH_PATTERNS)

    if is_bearish:
        # 如果同一条新闻同时包含快速响应词，降级为 bearish_with_response
        if is_response:
            logger.debug(f"[news_signal] bearish_with_response: {text[:80]}")
            return "bearish_with_response"
        logger.debug(f"[news_signal] bearish: {text[:80]}")
        return "bearish"

    if is_noise:
        return None

    # 检测利好
    for pattern, category in BULLISH_PATTERNS:
        if re.search(pattern, t, re.IGNORECASE):
            logger.debug(f"[news_signal] bullish hit: {category} | {text[:80]}")
            return "bullish"

    return None


# [v3.89 #77] 实体匹配：原写法 `ticker_base.lower() in text` 与宏观关键词都是裸子串，
#   短 ticker 出现在普通单词里（如 ONE 在 money、ETH 在 method）也算命中。改为词边界匹配。
_BTC_MACRO_KW = (
    "bitcoin", "btc", "1000 btc", "ethereum", "eth",
    "mara", "marathon digital", "microstrategy", "riot platform",
    "bitcoin miner", "btc miner", "mining company",
)
_BTC_ETH_BASES = {"BTC", "ETH", "WBTC", "WETH", "BTCB"}
# Binance 合约带 1000 前缀的 base，正文里通常写不带前缀的名字
_TICKER_ALIASES = {
    "1000PEPE": ("pepe",),
    "1000SHIB": ("shib",),
    "1000BONK": ("bonk",),
    "1000FLOKI": ("floki",),
}


def _word_in(text_lower: str, word: str) -> bool:
    """词边界匹配：前后不得紧贴字母/数字；允许复数 s（bitcoins / miners）。$TICKER 写法的 $ 属于边界字符，可命中。"""
    w = word.strip().lower()
    if not w:
        return False
    return re.search(r"(?<![a-z0-9])" + re.escape(w) + r"s?(?![a-z0-9])", text_lower) is not None


def _ticker_mentioned(text_lower: str, ticker_base: str) -> bool:
    names = [ticker_base.lower()] + list(_TICKER_ALIASES.get(ticker_base.upper(), ()))
    return any(_word_in(text_lower, n) for n in names)


def _is_btc_macro_irrelevant(text: str, ticker_base: str) -> bool:
    """True = 文章是 BTC/ETH 宏观新闻且与本 ticker 无关，应跳过 bearish 标记。"""
    if ticker_base.upper() in _BTC_ETH_BASES:
        return False  # BTC/ETH 自身，不过滤
    tl = text.lower()
    if not any(_word_in(tl, kw) for kw in _BTC_MACRO_KW):
        return False
    return not _ticker_mentioned(tl, ticker_base)


def _news_order_desc(resp_ts: float, bull_ts: float) -> str:
    """[v3.89 #78] 描述「响应类利空」与「利好」最新发布时间的先后（仅用于日志证据）。"""
    def _f(t: float) -> str:
        return time.strftime("%m-%d %H:%M", time.localtime(t)) if t and t > 0 else "时间未知"
    if resp_ts > 0 and bull_ts > 0:
        order = "利好晚于响应类利空" if bull_ts > resp_ts else ("利好早于响应类利空" if bull_ts < resp_ts else "两者同时")
    else:
        order = "无法比较先后"
    return f"响应类利空最新 {_f(resp_ts)}，利好最新 {_f(bull_ts)}，{order}"


def get_news_bias(symbol: str) -> str | None:
    """
    主接口：返回 symbol 的新闻偏向。
    - "bullish"  利好，可作为 LONG 豁免条件
    - "bearish"  利空，LONG 一票否决，SHORT 豁免
    - None       无有效信号

    带 TTL 缓存（_CACHE_TTL 秒）。
    symbol 格式：BATUSDT 或 BAT 均可。
    """
    base = symbol.upper().replace("USDT", "").replace("BUSD", "")

    now = time.time()
    if base in _cache:
        ts, bias, _ = _cache[base]
        if now - ts < _CACHE_TTL:
            return bias

    news_items = _fetch_raw_news(base)
    if not news_items:
        _cache[base] = (now, None, 0.0)
        return None

    # 聚合逻辑：
    #   bearish(纯利空)  > bearish_with_response(利空+快速响应) > bullish > None
    #
    # bearish_with_response 的含义：
    #   一条新闻里同时包含攻击词 + 响应措施词
    #   → 项目方已主动进行封控/补偿，市场影响局限
    #   → 不做一票否决，而是降级为 "caution"（谨慎）
    #
    # 但是：如果同一 symbol 关联新闻里
    #   存在任何一条「纯 bearish」，即为 bearish
    # v3.79 fix: BTC/ETH 宏观新闻误伤过滤
    # 背景：MARA/MicroStrategy 等 BTC 矿企向交易所转 BTC 的新闻命中 exchange_inflow 规则，
    #       但 Binance Square 把这篇文章打上了无关山寨币的 ticker 标签，导致误判。
    # 过滤逻辑：如果文章主体是 BTC/ETH 宏观转账事件，且被查询 ticker 本身不是 BTC/ETH，
    #           且 ticker 名称未出现在文章文本中 → 视为无关新闻，不触发 bearish。
    # [v3.89 #77] BTC/ETH 宏观无关新闻判断已提到模块级 _is_btc_macro_irrelevant（词边界匹配 + 别名表）。

    bias_list = []
    oldest_ts = 0.0
    # [v3.89 #78] 记录两类新闻各自最新的发布时间（秒），只用于日志证据，不参与聚合结果
    _resp_latest = 0.0   # bearish_with_response 最新发布时间
    _bull_latest = 0.0   # bullish 最新发布时间
    for item in news_items:
        text = (item.get("title") or "") + " " + (item.get("body") or "")
        b = _classify_text(text)
        if b in ("bearish", "bearish_with_response") and _is_btc_macro_irrelevant(text, base):
            # BTC 宏观噪音，跳过，记日志
            snippet = text[:120].replace("\n", " ")
            logger.info(
                f"[news_signal] {base}: bearish 命中但判定为 BTC 宏观无关新闻，已过滤 "
                f"| snippet='{snippet}'"
            )
            continue
        if b:
            bias_list.append(b)
            try:
                _its = float(item.get("date", 0) or 0) / 1000
            except (TypeError, ValueError):
                _its = 0.0
            if b == "bearish_with_response":
                _resp_latest = max(_resp_latest, _its)
            elif b == "bullish":
                _bull_latest = max(_bull_latest, _its)
            if b in ("bearish", "bearish_with_response"):
                item_ts = item.get("date", 0) / 1000
                if item_ts > 0 and (oldest_ts == 0.0 or item_ts < oldest_ts):
                    oldest_ts = item_ts

    if "bearish" in bias_list:
        # 有纯利空新闻，一票否决
        result: str | None = "bearish"
    elif "bearish_with_response" in bias_list and "bullish" in bias_list:
        # 攻击事件但已快速响应 + 其他利好新闻（如融资/ETF流入）
        # → 利好在超越利空，整体偏向利好
        result = "bullish"
        # [v3.89 #78] 聚合规则不变（同时存在即输出 bullish）。原文案「被后续利好覆盖」暗示利好晚于利空，
        #   但代码并未比较发布时间；现在如实描述并附上两类新闻最新发布时间与先后关系。
        logger.info(f"[news_signal] {base}: 同时存在 bearish_with_response 与 bullish 新闻，按现有规则输出 bullish "
                    f"({_news_order_desc(_resp_latest, _bull_latest)})")
    elif "bearish_with_response" in bias_list:
        # 攻击+快速响应，但无其他利好新闻→ caution，不做一票否决
        result = "caution"
    elif "bullish" in bias_list:
        result = "bullish"
    else:
        result = None

    logger.info(
        f"[news_signal] {base}: {len(news_items)} 条新闻 → bias={result} "
        f"(bullish={bias_list.count('bullish')}, "
        f"bearish={bias_list.count('bearish')}, "
        f"bearish_w_resp={bias_list.count('bearish_with_response')}, "
        f"oldest_ts={oldest_ts:.0f})"
    )
    _cache[base] = (now, result, oldest_ts)
    return result


def get_news_bias_detail(symbol: str) -> dict:
    """
    扩展接口：返回包含新闻年龄的详细信息。
    返回格式: {"bias": str|None, "oldest_news_ts": float, "news_age_h": float}
    """
    base = symbol.upper().replace("USDT", "").replace("BUSD", "")
    get_news_bias(symbol)  # 确保缓存已填充
    entry = _cache.get(base)
    if not entry:
        return {"bias": None, "oldest_news_ts": 0.0, "news_age_h": 0.0}
    _, bias, oldest_ts = entry
    news_age_h = (time.time() - oldest_ts) / 3600 if oldest_ts > 0 else 0.0
    return {"bias": bias, "oldest_news_ts": oldest_ts, "news_age_h": news_age_h}


def bulk_get_news_bias(symbols: list[str]) -> dict[str, str | None]:
    """批量查询（串行，带缓存）"""
    return {sym: get_news_bias(sym) for sym in symbols}


def clear_cache(symbol: str | None = None) -> None:
    """清除缓存（测试用）"""
    if symbol:
        base = symbol.upper().replace("USDT", "")
        _cache.pop(base, None)
    else:
        _cache.clear()
