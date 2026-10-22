#!/usr/bin/env python3
"""
prepare_data.py — 回测数据准备脚本（阶段一）

功能：拉取历史数据并保存到 fx/backtest_data/ 目录，供 backtest_btw.py 使用。
所有数据均通过 Binance 官方接口获取，完全可历史回放。

用法：
    python3 fx/backtest_data/prepare_data.py [--days 30] [--symbol BTWUSDT]

注意：
    - CG 清算图数据无历史接口，不拉取（回测时固定为 0）
    - 新闻数据无法精确历史化，不拉取（回测时固定为 None）
"""

import os
import sys
import json
import time
import argparse
import logging
from datetime import datetime, timezone

# ── 路径修复：允许从任意目录运行 ─────────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_FX_DIR   = os.path.dirname(_THIS_DIR)
_ROOT_DIR = os.path.dirname(_FX_DIR)
for _p in [_FX_DIR, _ROOT_DIR]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── dotenv 加载（API Key 等） ─────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(_FX_DIR, ".env")
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
    else:
        _env_path2 = os.path.join(_ROOT_DIR, ".env")
        if os.path.exists(_env_path2):
            load_dotenv(_env_path2)
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("prepare_data")

DATA_DIR = _THIS_DIR  # fx/backtest_data/

# ── Binance REST 基础（不依赖 binance_api.py，避免初始化副作用） ─────────────
import urllib.request
import urllib.parse

PROXY = os.environ.get("BINANCE_PROXY", "")

def _build_opener():
    if PROXY and "socks5" in PROXY:
        try:
            import socks
            import socket
            # 解析 socks5://user:pass@host:port
            url = PROXY.replace("socks5://", "")
            if "@" in url:
                creds, hostport = url.rsplit("@", 1)
                user, pwd = creds.split(":", 1)
            else:
                hostport = url
                user = pwd = None
            host, port = hostport.rsplit(":", 1)
            socks.set_default_proxy(socks.SOCKS5, host, int(port), username=user, password=pwd)
            socket.socket = socks.socksocket
            logger.info(f"SOCKS5 代理已设置: {host}:{port}")
        except Exception as e:
            logger.warning(f"SOCKS5 代理设置失败，直连: {e}")
    return urllib.request.build_opener()

_opener = None

def _get(url: str, params: dict = None, retries: int = 3) -> any:
    global _opener
    if _opener is None:
        _opener = _build_opener()
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "backtest-prepare/1.0"})
            with _opener.open(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.warning(f"请求失败 (attempt {attempt+1}/{retries}): {url[:80]}... {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None

BASE = "https://fapi.binance.com"


# ════════════════════════════════════════════════════════════════════════════
# K 线拉取（分批，避免单次 limit 限制）
# ════════════════════════════════════════════════════════════════════════════

def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    """
    拉取 [start_ms, end_ms) 区间内的完整 K 线（自动分批）。
    返回已排序的 K 线列表（每根 K 线为原始 Binance 列表格式）。
    """
    LIMIT = 1500  # Binance 最大单次 1500 根
    all_k = []
    cur_start = start_ms

    while cur_start < end_ms:
        batch = _get(f"{BASE}/fapi/v1/klines", {
            "symbol": symbol,
            "interval": interval,
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": LIMIT,
        })
        if not batch or not isinstance(batch, list):
            logger.error(f"K线拉取失败: {symbol} {interval} start={cur_start}")
            break
        all_k.extend(batch)
        logger.info(f"  [{symbol} {interval}] 已拉 {len(all_k)} 根，最新: "
                    f"{datetime.fromtimestamp(batch[-1][0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")
        if len(batch) < LIMIT:
            break
        # 下一批从最后一根的收盘时间+1ms 开始
        cur_start = int(batch[-1][6]) + 1
        time.sleep(0.2)

    # 去重（按开盘时间）并排序
    seen = set()
    result = []
    for k in all_k:
        t = k[0]
        if t not in seen:
            seen.add(t)
            result.append(k)
    result.sort(key=lambda k: k[0])
    return result


# ════════════════════════════════════════════════════════════════════════════
# 资金费率历史
# ════════════════════════════════════════════════════════════════════════════

def fetch_funding_rate(symbol: str, start_ms: int, end_ms: int) -> list:
    """
    拉取历史资金费率列表，格式：[{"fundingTime": ms, "fundingRate": "0.0001", ...}, ...]
    按 fundingTime 升序。
    """
    LIMIT = 1000
    all_f = []
    cur_start = start_ms

    while cur_start < end_ms:
        batch = _get(f"{BASE}/fapi/v1/fundingRate", {
            "symbol": symbol,
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": LIMIT,
        })
        if not batch or not isinstance(batch, list):
            logger.warning(f"资金费率拉取返回空: {symbol} start={cur_start}")
            break
        all_f.extend(batch)
        logger.info(f"  [{symbol} funding] 已拉 {len(all_f)} 条，最新: "
                    f"{datetime.fromtimestamp(batch[-1]['fundingTime']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")
        if len(batch) < LIMIT:
            break
        cur_start = int(batch[-1]["fundingTime"]) + 1
        time.sleep(0.2)

    # 去重并排序
    seen = set()
    result = []
    for f in all_f:
        t = f["fundingTime"]
        if t not in seen:
            seen.add(t)
            result.append(f)
    result.sort(key=lambda x: x["fundingTime"])
    return result


# ════════════════════════════════════════════════════════════════════════════
# OI 历史（每小时快照）
# ════════════════════════════════════════════════════════════════════════════

def fetch_oi_hist(symbol: str, start_ms: int, end_ms: int) -> list:
    """
    拉取历史持仓量（1h 快照）。
    接口限制：单次最多500条，且只保留近30天数据。
    """
    LIMIT = 500
    all_oi = []
    cur_start = start_ms

    while cur_start < end_ms:
        batch = _get(f"{BASE}/futures/data/openInterestHist", {
            "symbol": symbol,
            "period": "1h",
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": LIMIT,
        })
        if not batch or not isinstance(batch, list):
            logger.warning(f"OI历史拉取返回空: {symbol} start={cur_start}")
            break
        all_oi.extend(batch)
        logger.info(f"  [{symbol} OI] 已拉 {len(all_oi)} 条，最新: "
                    f"{datetime.fromtimestamp(batch[-1]['timestamp']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")
        if len(batch) < LIMIT:
            break
        cur_start = int(batch[-1]["timestamp"]) + 1
        time.sleep(0.2)

    # 去重并排序
    seen = set()
    result = []
    for o in all_oi:
        t = o["timestamp"]
        if t not in seen:
            seen.add(t)
            result.append(o)
    result.sort(key=lambda x: x["timestamp"])
    return result


# ════════════════════════════════════════════════════════════════════════════
# 主流程
# ════════════════════════════════════════════════════════════════════════════

def save_json(path: str, data: any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info(f"  已保存 {path}（{len(data)} 条）")


def main():
    parser = argparse.ArgumentParser(description="回测数据准备脚本")
    parser.add_argument("--days", type=int, default=30, help="回测天数（默认30）")
    parser.add_argument("--symbol", type=str, default="BTWUSDT", help="目标标的（默认BTWUSDT）")
    args = parser.parse_args()

    symbol    = args.symbol.upper()
    days      = args.days
    now_ms    = int(time.time() * 1000)
    start_ms  = now_ms - days * 24 * 3600 * 1000

    logger.info(f"═══ 回测数据准备 ═══")
    logger.info(f"标的: {symbol}  天数: {days}")
    logger.info(f"时间范围: {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')} → {datetime.fromtimestamp(now_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')}")
    logger.info(f"存储目录: {DATA_DIR}")
    logger.info("")

    # ── 1. 目标标的 K 线 ──────────────────────────────────────────────────
    for interval in ["15m", "1h", "4h"]:
        logger.info(f"拉取 {symbol} {interval} K线...")
        data = fetch_klines(symbol, interval, start_ms, now_ms)
        fname = os.path.join(DATA_DIR, f"{symbol}_{interval}.json")
        save_json(fname, data)

    # ── 2. BTC K 线 ───────────────────────────────────────────────────────
    for interval in ["1h", "4h"]:
        logger.info(f"拉取 BTCUSDT {interval} K线...")
        data = fetch_klines("BTCUSDT", interval, start_ms, now_ms)
        fname = os.path.join(DATA_DIR, f"BTCUSDT_{interval}.json")
        save_json(fname, data)

    # ── 3. 资金费率历史 ───────────────────────────────────────────────────
    logger.info(f"拉取 {symbol} 历史资金费率...")
    funding = fetch_funding_rate(symbol, start_ms, now_ms)
    fname = os.path.join(DATA_DIR, f"{symbol}_funding.json")
    save_json(fname, funding)

    # ── 4. OI 历史 ────────────────────────────────────────────────────────
    logger.info(f"拉取 {symbol} 历史OI（1h快照）...")
    oi = fetch_oi_hist(symbol, start_ms, now_ms)
    fname = os.path.join(DATA_DIR, f"{symbol}_oi_1h.json")
    save_json(fname, oi)

    # ── 5. 元数据 ─────────────────────────────────────────────────────────
    meta = {
        "symbol":      symbol,
        "days":        days,
        "start_ms":    start_ms,
        "end_ms":      now_ms,
        "prepared_at": datetime.now(tz=timezone.utc).isoformat(),
        "cg_bonus_note": "CG清算图无历史接口，回测时 cg_bonus 固定为 0",
        "news_note":     "新闻信号无法精确历史化，回测时 news_bias 固定为 None（不拦截不加分）",
        "llm_note":      "LLM判断跳过，回测时边界信号全部走规则系统原始结果",
    }
    fname = os.path.join(DATA_DIR, "meta.json")
    save_json(fname, meta)

    logger.info("")
    logger.info("═══ 数据准备完成 ═══")
    logger.info("数据污染说明：")
    logger.info("  ✅ K线/资金费率/OI/BTC — 完全历史可回放，无未来数据污染")
    logger.info("  ❌ CG清算图 — 无历史接口，回测结果低估（实际胜率可能更高）")
    logger.info("  ❌ 新闻信号 — 无法精确历史化，回测结果偏保守（部分拦截缺失）")
    logger.info("  ⚠️  LLM判断 — 跳过，用规则系统原始结果替代")
    logger.info("")
    logger.info("下一步：python3 fx/backtest_btw.py --days 30")


if __name__ == "__main__":
    main()
