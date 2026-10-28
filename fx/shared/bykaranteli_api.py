"""
shared/bykaranteli_api.py — ByKaranteli API 客户端

替代 Clawby 的清算热力图接口，提供：
  - get_liqmap()       : 清算热力图（LiqMap），与 Clawby liq_heatmap 等价
  - get_liq_zones()    : 从 LiqMap 提取清算区列表（供 judges/calc_sl_tp 使用）
  - get_pressure()     : 衍生品压力分数（OI+资金费率+基差合成）
  - get_liquidations() : 历史清算数据
  - get_top_movers()   : OI 大幅变动/极端资金费率
  - get_rsi_heatmap()  : 多时间框架 RSI（辅助扫描入场时机）
  - get_open_interest(): 主流币 OI 变化 + 杠杆状态
  - get_market_indices(): Fear & Greed、BTC 主导率

API Key: 从环境变量 BYKARANTELI_API_KEY 读取。
直接走 REST（不用 npx），响应更快。
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger("fx.bykaranteli")

# 多 Key 轮询池：主 key 失败或触发限速时自动切换
_API_KEYS: list[str] = [
    os.environ.get("BYKARANTELI_API_KEY", "bk_OXn6K6nkekUKOw1VFwoVdjV3JRQQTh_47eos_3IkNaM"),
    "bk_khKGhVaM7b5I5w_dw6OFi6hvpKNcGQQhxPfTOu0a2mI",
    "bk_6pVXTCkUa3lUPHbRA0sven5jI55vGhGX4saqqkZ58V8",
    "bk_ubn5O0DEct5RYePTayxiMlNQu-RaZepvV4Watmr5Fh4",
    "bk_9bjPpYuMD4UDDHGrZ3mWwRHUQ5yWjaiYdh0lHID8A1o",
    "bk_uJHH-WorP0PSIH6EW5xAm_kSSym3mZZRiH4S1i0-usc",
]
_key_index = 0          # 当前使用的 key 下标
_key_cooldown: dict[int, float] = {}   # key下标 → 冷却到期时间戳
_KEY_COOLDOWN_SEC = 60  # 429 触发后该 key 冷却 60 秒

_BASE = "https://bykaranteli.com/api"

# 本地内存缓存
_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 15 * 60   # 15 分钟


def _pick_key() -> str:
    """从 key 池中选一个当前不在冷却期的 key，按轮询顺序返回。"""
    global _key_index
    now = time.time()
    for _ in range(len(_API_KEYS)):
        idx = _key_index % len(_API_KEYS)
        if _key_cooldown.get(idx, 0) <= now:
            _key_index = idx
            return _API_KEYS[idx]
        _key_index += 1
    # 全部冷却中，强制用第一个
    return _API_KEYS[0]


def _mark_key_rate_limited(key: str) -> None:
    """标记某个 key 触发了限速，进入冷却。"""
    global _key_index
    now = time.time()
    for i, k in enumerate(_API_KEYS):
        if k == key:
            _key_cooldown[i] = now + _KEY_COOLDOWN_SEC
            logger.warning(f"[byk] key[{i}] 触发限速，冷却 {_KEY_COOLDOWN_SEC}s")
            # 切换到下一个可用 key
            _key_index = (i + 1) % len(_API_KEYS)
            break


def _get(path: str, params: dict | None = None, ttl: int = _CACHE_TTL) -> Any:
    """
    带缓存 + 多 Key 轮询的 GET 请求。
    429 限速时自动切换到下一个 key 重试一次。
    """
    import requests
    key_str = f"{path}:{params}"
    now = time.time()
    if key_str in _cache:
        ts, val = _cache[key_str]
        if now - ts < ttl:
            return val

    for attempt in range(len(_API_KEYS)):
        api_key = _pick_key()
        headers = {"X-API-Key": api_key}
        try:
            r = requests.get(f"{_BASE}{path}", params=params, headers=headers, timeout=3)  # [v3.95] BYK长期超时，快速失败进三级
            if r.status_code == 200:
                data = r.json()
                _cache[key_str] = (now, data)
                return data
            elif r.status_code == 429:
                _mark_key_rate_limited(api_key)
                # 切换 key 后继续循环重试
                continue
            else:
                logger.debug(f"[byk] GET {path} HTTP {r.status_code}: {r.text[:200]}")
                return None
        except Exception as e:
            logger.warning(f"[byk] GET {path} 异常: {e}")
            return None
    logger.warning(f"[byk] GET {path} 所有 key 均限速，放弃")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 清算热力图
# ─────────────────────────────────────────────────────────────────────────────

def get_liqmap(symbol: str = "BTCUSDT", timeframe: str = "24h") -> dict | None:
    """
    获取 LiqMap 清算热力图。
    Free Plan 仅支持 timeframe="24h"。
    返回: {ok, band_prices, heatmap_cells, real_levels{levels,totals}, ...}
    """
    sym = symbol.upper()
    if not sym.endswith("USDT"):
        sym += "USDT"
    return _get("/public/liqmap", {"symbol": sym, "timeframe": timeframe})


def get_liq_zones(symbol: str, direction: str, current_price: float,
                  timeframe: str = "24h") -> list[dict]:
    """
    从 LiqMap 提取清算区列表，格式与原 Clawby 接口兼容。
    direction: "LONG"（看价格下方） 或 "SHORT"（看价格上方）
    返回: [{"price": float, "amount": float, "direction": str, "source": str}, ...]
    """
    data = get_liqmap(symbol, timeframe)
    if not data or not data.get("ok"):
        return []

    zones: list[dict] = []

    # real_levels 是字典，内有 levels 列表
    real_levels_data = data.get("real_levels", {})
    levels_list: list = []
    if isinstance(real_levels_data, dict):
        levels_list = real_levels_data.get("levels", [])
    elif isinstance(real_levels_data, list):
        levels_list = real_levels_data

    for lvl in levels_list:
        try:
            price = float(lvl.get("price", 0))
            notional = float(lvl.get("notional_usd", 0))
            cumulative = float(lvl.get("cumulative_usd", 0))
            # 如果 notional=0，用 cumulative 的 1% 估算
            amount = notional if notional > 0 else cumulative * 0.01
            if price <= 0 or amount <= 0:
                continue
            if direction == "LONG" and price >= current_price:
                continue
            if direction == "SHORT" and price <= current_price:
                continue
            zones.append({
                "price": price,
                "amount": amount,
                "direction": direction,
                "source": "byk_real_level",
            })
        except Exception:
            continue

    # 如果 real_levels 没数据，从 heatmap_cells 估算
    if not zones:
        band_prices = data.get("band_prices", [])
        heatmap = data.get("heatmap_cells", [])
        if isinstance(band_prices, list) and heatmap:
            band_totals: dict[int, float] = {}
            for cell in heatmap:
                if len(cell) >= 3:
                    price_idx = int(cell[1])
                    intensity = abs(float(cell[2]))
                    band_totals[price_idx] = band_totals.get(price_idx, 0) + intensity
            for price_idx, total in band_totals.items():
                if price_idx < len(band_prices):
                    price = float(band_prices[price_idx])
                    if direction == "LONG" and price >= current_price:
                        continue
                    if direction == "SHORT" and price <= current_price:
                        continue
                    est_usd = total * current_price * 500_000
                    if est_usd > 500_000:
                        zones.append({
                            "price": price,
                            "amount": est_usd,
                            "direction": direction,
                            "source": "byk_heatmap",
                        })

    zones.sort(key=lambda z: z["amount"], reverse=True)
    return zones[:20]


# ─────────────────────────────────────────────────────────────────────────────
# 衍生品分析
# ─────────────────────────────────────────────────────────────────────────────

def get_pressure_scores(symbol: str | None = None, limit: int = 20) -> dict | None:
    """获取衍生品压力分数（资金费率+OI+基差合成，0-100）。"""
    params: dict = {"limit": limit}
    if symbol:
        params["symbol"] = symbol.upper()
    return _get("/public/pressure", params)


def get_top_movers() -> dict | None:
    """获取 OI 大幅变动 / 极端资金费率 top10。"""
    return _get("/public/top-movers", {})


def get_liquidations(symbol: str | None = None, days: int = 7) -> dict | None:
    """获取历史清算数据（多空分别，按日统计）。"""
    params: dict = {"days": days}
    if symbol:
        params["symbol"] = symbol.upper()
    return _get("/public/liquidations", params)


def get_funding_heatmap(symbol: str | None = None) -> dict | None:
    """获取资金费率热力图（Top30 Binance perps）。"""
    params: dict = {}
    if symbol:
        params["symbol"] = symbol.upper()
    return _get("/public/funding-heatmap", params)


def get_open_interest() -> dict | None:
    """获取主流币种盘中 OI 变化 + 杠杆状态（多空建仓/减仓判断）。"""
    return _get("/public/open-interest", {})


def get_rsi_heatmap(symbol: str | None = None,
                    sort: str = "4h", order: str = "asc",
                    limit: int = 30) -> dict | None:
    """
    多时间框架 RSI 热力图（Top-400 Binance perps）。
    sort: "15m"/"1h"/"4h"/"12h"/"1d"
    order: "asc"（最超卖）或 "desc"（最超买）
    """
    params: dict = {"sort": sort, "order": order, "limit": limit}
    if symbol:
        params["symbol"] = symbol.upper()
    return _get("/public/rsi-heatmap", params)


def get_market_indices() -> dict | None:
    """获取 Fear & Greed、BTC 主导率、总市值、Euphoria 指数。"""
    return _get("/public/market-indices", {})


def get_liq_cascades() -> dict | None:
    """获取最近清算连锁事件（崩盘诱因分析）。"""
    return _get("/public/liquidation-cascades", {})
