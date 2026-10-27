"""双通道（代理 / 直连）自适应路由。

  · 启动探测：并发 ping 代理与直连，选更快的；
  · 后台每 ROUTE_PROBE_INTERVAL_SEC 重新探测，当前通道比另一条慢 ROUTE_SWITCH_RATIO 倍以上才切换（防抖）；
  · 单次请求失败由调用方 record_failure()：连续 ROUTE_FAIL_SWITCH_N 次失败才切换；
  · 探测只打公开接口 /fapi/v1/ping，不带签名；日志里的代理地址一律脱敏。
代理地址只从环境变量读（BINANCE_PROXY / PROXY_URL），代码里不写任何默认值。
没有配置代理时 active_route() 恒为 "direct"，行为与未启用本模块一致。
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
import time

from shared.ws_proxy import mask_proxy

logger = logging.getLogger("route_selector")

PROBE_URL = "https://fapi.binance.com/fapi/v1/ping"
PROXY = (os.environ.get("BINANCE_PROXY") or os.environ.get("PROXY_URL") or "").strip()

_LOCK = threading.Lock()
_active_route = "proxy" if PROXY else "direct"
_fail_streak = {"proxy": 0, "direct": 0}
_last_probe = {"proxy": None, "direct": None}     # 最近一次探测耗时（秒），None=失败/未测


def _cfg(name, default):
    try:
        import config as _c
        return getattr(_c, name, default)
    except Exception:
        return default


def has_proxy() -> bool:
    return bool(PROXY)


def active_route() -> str:
    return _active_route


def ws_route() -> str:
    """WS 用：有代理 → 当前通道；没配代理 → 返回 "proxy"（不干预各 WS 自己的代理设置）。"""
    return _active_route if PROXY else "proxy"


def other(route: str) -> str:
    return "direct" if route == "proxy" else "proxy"


def get_proxy_for_request() -> dict:
    """当前应使用的 requests proxies dict（直连返回 {}）。"""
    if _active_route == "proxy" and PROXY:
        return {"http": PROXY, "https": PROXY}
    return {}


def last_probe() -> dict:
    return dict(_last_probe)


# ── 纯函数：选路决策 ─────────────────────────────────────────────────────────
def decide(proxy_lat, direct_lat, current: str, ratio: float = 2.0, startup: bool = False) -> str:
    """proxy_lat/direct_lat：探测耗时（秒），None=失败。
    startup=True：谁快选谁（都失败 → 保持 current）。
    否则：current 失败而另一条成功 → 切换；current 比另一条慢 ratio 倍以上 → 切换；其余保持。"""
    lat = {"proxy": proxy_lat, "direct": direct_lat}
    if lat["proxy"] is None and lat["direct"] is None:
        return current
    if lat["proxy"] is None:
        return "direct"
    if lat["direct"] is None:
        return "proxy"
    if startup:
        return "proxy" if lat["proxy"] <= lat["direct"] else "direct"
    cur, oth = lat[current], lat[other(current)]
    if cur > oth * ratio:
        return other(current)
    return current


def _set_route(new: str, reason: str) -> bool:
    global _active_route
    with _LOCK:
        if new == _active_route or (new == "proxy" and not PROXY):
            return False
        old, _active_route = _active_route, new
        _fail_streak["proxy"] = _fail_streak["direct"] = 0
    logger.warning(f"[route] 切换通道 {old} → {new}（{reason}）")
    return True


def record_success(route: str) -> None:
    _fail_streak[route] = 0


def record_failure(route: str) -> bool:
    """记录一次请求失败；当前通道连续失败达到阈值才切换。返回是否发生了切换。"""
    if not PROXY or route != _active_route:
        return False
    _fail_streak[route] += 1
    n = int(_cfg("ROUTE_FAIL_SWITCH_N", 3))
    if _fail_streak[route] >= n:
        return _set_route(other(route), f"{route} 连续失败{_fail_streak[route]}次")
    return False


# ── 探测 ─────────────────────────────────────────────────────────────────────
def probe_once(route: str, timeout=(3, 5)):
    """同步探测一条通道，返回耗时秒数；失败返回 None。只请求公开 ping。"""
    import requests
    proxies = {"http": PROXY, "https": PROXY} if (route == "proxy" and PROXY) else {}
    t0 = time.monotonic()
    try:
        r = requests.get(PROBE_URL, proxies=proxies, timeout=timeout, headers={"Accept-Encoding": "identity"})
        if r.status_code != 200:
            return None
        return time.monotonic() - t0
    except Exception:
        return None


def _fmt(x) -> str:
    return "失败" if x is None else f"{x:.2f}s"


async def probe_routes(startup: bool = True) -> str:
    """并发探测两条通道并按规则选路；返回当前通道。没配代理时直接返回 direct，不发请求。"""
    if not PROXY:
        return "direct"
    p, d = await asyncio.gather(asyncio.to_thread(probe_once, "proxy"), asyncio.to_thread(probe_once, "direct"))
    _last_probe["proxy"], _last_probe["direct"] = p, d
    cur = _active_route
    new = decide(p, d, cur, ratio=float(_cfg("ROUTE_SWITCH_RATIO", 2.0)), startup=startup)
    if startup:
        with _LOCK:
            globals()["_active_route"] = new
        logger.info(f"[route] 启动探测: 代理={_fmt(p)} 直连={_fmt(d)} → 选{'代理' if new == 'proxy' else '直连'}"
                    f"（{mask_proxy(PROXY)}）")
    elif new != cur:
        _set_route(new, f"代理={_fmt(p)} 直连={_fmt(d)}，{'代理' if cur == 'proxy' else '直连'}变慢")
    else:
        logger.debug(f"[route] 复测: 代理={_fmt(p)} 直连={_fmt(d)} 保持{cur}")
    return _active_route


async def start_route_monitor() -> None:
    """后台循环：每 ROUTE_PROBE_INTERVAL_SEC 重新探测一次；异常只记日志，不退出。"""
    if not PROXY:
        return
    while True:
        await asyncio.sleep(float(_cfg("ROUTE_PROBE_INTERVAL_SEC", 600)))
        try:
            await probe_routes(startup=False)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[route] 复测异常（忽略）: {e}")
