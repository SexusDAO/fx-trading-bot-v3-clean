"""WebSocket 代理降级：代理在「握手阶段」失败 → 本条流改走直连；过 retry_sec 后再试一次代理。

只负责决定「下一次连接用哪个 proxy」，不自己建连接，也不改变各流原有的退避重连：
  fb = ProxyFallback(proxy_url, name="hub")
  async with websockets.connect(url, proxy=fb.current(), ...) as ws:
      fb.opened()                  # 握手成功（代理可用）
      ...
  except Exception as e:
      fb.failed(e)                 # 只有「还没握手成功」的失败才会触发降级；连上之后的断线不算代理故障
"""
from __future__ import annotations

import logging
import time

logger = logging.getLogger("ws_proxy")

DEFAULT_RETRY_SEC = 300.0   # 降级直连后，多久再尝试一次代理


def mask_proxy(url) -> str:
    """日志脱敏：只保留 scheme://host:port，去掉账号密码。"""
    s = str(url or "")
    if "@" in s:
        scheme = s.split("://", 1)[0] + "://" if "://" in s else ""
        return scheme + s.rsplit("@", 1)[1]
    return s


class ProxyFallback:
    def __init__(self, proxy, name: str = "ws", retry_sec: float = DEFAULT_RETRY_SEC, clock=time.monotonic):
        self._proxy = (str(proxy).strip() or None) if proxy else None
        self._name = name
        self._retry = float(retry_sec)
        self._clock = clock
        self._direct_since = None      # 非 None = 当前降级为直连
        self._opened = False           # 自上次 current() 以来是否握手成功过

    @property
    def degraded(self) -> bool:
        return self._direct_since is not None

    def current(self):
        """本次连接应使用的 proxy（None = 直连）。降级超过 retry_sec 后返回代理再试一次。"""
        self._opened = False
        if not self._proxy:
            return None
        if self._direct_since is not None:
            if self._clock() - self._direct_since < self._retry:
                return None
            logger.info(f"[{self._name}] 直连已持续 {self._retry:g}s，重新尝试代理 {mask_proxy(self._proxy)}")
            self._direct_since = None
        return self._proxy

    def opened(self) -> None:
        self._opened = True

    def failed(self, exc=None) -> bool:
        """连接异常时调用；握手前失败且正在用代理 → 降级直连，返回 True。"""
        if self._opened or not self._proxy or self._direct_since is not None:
            return False
        self._direct_since = self._clock()
        logger.warning(f"[{self._name}] 代理 {mask_proxy(self._proxy)} 连接失败，降级直连"
                       f"（{self._retry:g}s 后再试代理）: {str(exc)[:120]}")
        return True
