"""
fx/shared/ws_listener.py — WebSocket 监听器（WsOrderListener）

职责：
1. 维护一个 userData stream 连接（listenKey），接收 ORDER_TRADE_UPDATE 事件。
2. 维护一组 markPrice stream 连接，实时推送当前持仓标的的标记价格。
3. 提供回调注册接口：
   - on_order_event(callback)  — 订单成交/状态变化
   - on_price_event(callback)  — 标记价格更新
4. 支持动态订阅/取消标的的 markPrice。
5. 自动 keepalive listenKey（每 20 分钟），自动断线重连（指数退避）。

用法（供 executor / position_manager 使用）：

    listener = WsOrderListener()
    listener.on_order_event(my_order_callback)
    listener.on_price_event(my_price_callback)
    await listener.start()                   # 启动，不阻塞
    listener.subscribe("BTCUSDT")            # 动态加标的
    listener.unsubscribe("BTCUSDT")          # 动态移除标的
    await listener.stop()                    # 停止

回调签名：
    async def my_order_callback(event: dict) -> None: ...
    async def my_price_callback(symbol: str, mark_price: float, ts: int) -> None: ...
"""

import asyncio
import collections
import json
import logging
import time
from typing import Callable, Awaitable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

# SOCKS5 代理（与 REST API 保持一致）
try:
    import sys, os
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from config import PROXY_URL as _WS_PROXY_URL_RAW
    # 空字符串传给 websockets.connect(proxy="") 会触发 "scheme isn't supported" 报错
    # None = 不使用代理，True = 自动从系统环境读取
    _WS_PROXY_URL = _WS_PROXY_URL_RAW if _WS_PROXY_URL_RAW else None
except Exception:
    _WS_PROXY_URL = None

from shared.ws_proxy import ProxyFallback
from shared import route_selector as _rs
_FB_USER = ProxyFallback(_WS_PROXY_URL, name="ws-userData", route_fn=_rs.ws_route)     # 握手失败 → 降级直连，一段时间后再试代理
_FB_MARK = ProxyFallback(_WS_PROXY_URL, name="ws-markPrice", route_fn=_rs.ws_route)
_FB_FORCE = ProxyFallback(_WS_PROXY_URL, name="ws-forceOrder", route_fn=_rs.ws_route)

import config
from shared import binance_api
from shared.async_utils import _run_sync, RunSyncTimeout   # [v3.89 #6] 同步 HTTP 移出事件循环

logger = logging.getLogger("ws_listener")

# ═══════════════════════════════════════════════════════════════════════════════
# 常量（可在 config.py 覆盖）
# ═══════════════════════════════════════════════════════════════════════════════
KEEPALIVE_INTERVAL   = getattr(config, "WS_KEEPALIVE_INTERVAL",   20 * 60)  # 秒
# [fix v3.91] max_queue：websockets 15.x（纯 Python asyncio 实现）的 max_queue 是接收缓冲高水位，
#   超过后会暂停读 socket（TCP 背压），并不会丢帧；低水位 = 高水位 // 4 时恢复读取。默认 16。
#   markPrice 的「丢旧帧」由下面的有限并发分发实现（_MP_MAX_INFLIGHT），不依赖 max_queue。
try:
    from config import WS_MARK_PRICE_MAX_QUEUE as _MP_MAX_QUEUE
except ImportError:
    _MP_MAX_QUEUE = 256

_MP_MAX_INFLIGHT = 32          # markPrice 同时在途的 dispatch 任务上限，超出直接丢帧
_MP_DROP_LOG_SEC = 60.0        # 丢帧统计 warning 的输出间隔

RECONNECT_BASE       = getattr(config, "WS_RECONNECT_BASE",        3)        # 秒
RECONNECT_MAX        = getattr(config, "WS_RECONNECT_MAX",         60)       # 秒
COMPENSATION_TIMEOUT = getattr(config, "WS_COMPENSATION_TIMEOUT",  30)       # [v3.89 #3] 断线 REST 补偿任务最长运行秒数
COMPENSATION_INTERVAL = getattr(config, "WS_COMPENSATION_INTERVAL", 10)      # [v3.89 #3] 补偿任务两次执行间隔秒数
LIQ_WINDOW_SEC       = getattr(config, "WS_LIQ_WINDOW_SEC",        14400)    # 4 小时缓存窗口（基准计算需要）
MARK_PRICE_URL_TPL   = config.BINANCE_WS_URL + "/market/stream?streams={streams}"   # 行情新路径；userData 仍走 /ws/{listenKey}
USER_DATA_URL_TPL    = config.BINANCE_WS_URL + "/ws/{listen_key}"
FORCE_ORDER_URL      = "wss://fstream.binance.com/market/ws/!forceOrder@arr"

OrderCallback = Callable[[dict], Awaitable[None]]
PriceCallback = Callable[[str, float, int], Awaitable[None]]
LiqCallback   = Callable[[dict], Awaitable[None]]


class WsOrderListener:
    """
    统一管理 userData + markPrice WebSocket 连接。

    线程安全说明：
    所有操作（subscribe/unsubscribe/stop）均通过 asyncio 事件循环完成；
    若从同步上下文调用，请使用 asyncio.run_coroutine_threadsafe。
    """

    def __init__(self) -> None:
        # 回调列表
        self._order_callbacks: List[OrderCallback] = []
        self._price_callbacks: List[PriceCallback] = []
        self._liq_callbacks:   List[LiqCallback]   = []
        self._reconnect_callbacks: list = []  # v3.33 userData 重连成功后触发

        # 当前订阅的标的
        self._subscribed: set[str] = set()
        self._sub_lock = asyncio.Lock()

        # 内部任务
        self._user_data_task:   Optional[asyncio.Task] = None
        self._mark_price_task:  Optional[asyncio.Task] = None
        self._keepalive_task:   Optional[asyncio.Task] = None
        self._force_order_task: Optional[asyncio.Task] = None
        self._rebuild_event:    asyncio.Event = asyncio.Event()

        # [fix v3.91] markPrice 有限并发分发：在途任务计数 + 丢帧统计（仅事件循环线程访问，无需加锁）
        #   不用 Semaphore._value 判断：接收循环在缓冲有帧时可能不让出控制权，
        #   连续创建的任务尚未开始执行、还没 acquire，会绕过上限；计数器在创建时同步 +1，才是真上限。
        self._mp_inflight:      int   = 0
        self._mp_drop_count:    int   = 0
        self._mp_drop_last_log: float = time.monotonic()
        self._mp_tasks:         set   = set()   # 持有任务引用，避免被 GC 提前回收

        # listenKey 缓存
        self._listen_key: Optional[str] = None

        # 强平事件缓存：{symbol: {"LONG": deque[(ts, qty)], "SHORT": deque[(ts, qty)]}}
        self._liq_cache: Dict[str, Dict[str, collections.deque]] = {}
        self._liq_lock = asyncio.Lock()

        # 停止标志
        self._stop_flag = False

        # [阶段0] 各 WS 流最后一次收到消息的 monotonic 时间（0 = 从未收到）；每 5 分钟输出一次存活状态
        self._stream_last_msg: Dict[str, float] = {"userData": 0.0, "markPrice": 0.0, "forceOrder": 0.0}
        self._health_task: Optional[asyncio.Task] = None

        # [v3.89 #3] userData 断线即时 REST 补偿：状态与回调
        self._disconnect_callbacks: list = []
        self._user_connected: bool = False
        self._compensation_task: Optional[asyncio.Task] = None
        # [v3.89 #7] 当前 userData 连接对象，续期失败 / listenKey 过期时用来主动关闭
        self._user_ws = None

    # ─────────────────────────────────────────────────────────────────────────
    # 公共 API
    # ─────────────────────────────────────────────────────────────────────────

    def on_order_event(self, callback: OrderCallback) -> None:
        """注册订单事件回调（可多次调用，支持多个回调）"""
        self._order_callbacks.append(callback)

    def on_price_event(self, callback: PriceCallback) -> None:
        """注册标记价格回调（可多次调用）"""
        self._price_callbacks.append(callback)

    def on_liq_event(self, callback: LiqCallback) -> None:
        """注册强平事件回调（可多次调用）。
        回调签名：async def cb(event: dict) -> None
        event 字段：symbol / side（LONG/SHORT 表示被强平方向） / qty / price / ts
        """
        self._liq_callbacks.append(callback)

    async def get_liquidation_volume(
        self, symbol: str, direction: str, minutes: int = 15
    ) -> float:
        """
        查询 symbol 在过去 minutes 分钟内的强平总量（USDT 名义额）。
        direction: 'LONG' 或 'SHORT'（被强平的仓位方向）。
        返回 float，无数据时返回 0.0。
        """
        sym  = symbol.upper()
        side = direction.upper()
        cutoff = time.time() - minutes * 60

        async with self._liq_lock:
            sym_cache = self._liq_cache.get(sym)
            if not sym_cache:
                return 0.0
            dq = sym_cache.get(side)
            if not dq:
                return 0.0
            total = sum(qty for ts, qty in dq if ts >= cutoff)
        return total

    def on_reconnect(self, callback) -> None:
        """注册 userData WS 重连成功回调（v3.33）。
        每次 userData 重连成功后，异步调用所有注册的回调，用于主动触发持仓对齐。"""
        self._reconnect_callbacks.append(callback)

    def on_disconnect(self, callback) -> None:
        """
        [v3.89 #3] 注册 userData 断线补偿回调（async，无参数）。
        userData 断线后立即启动唯一一个有界（COMPENSATION_TIMEOUT 秒）的 REST 补偿任务，
        每 COMPENSATION_INTERVAL 秒调用一次这些回调；userData 重连成功后任务自行结束。
        任务在独立协程里运行，不阻塞 WS 重连本身。
        """
        self._disconnect_callbacks.append(callback)

    def get_channel_status(self) -> dict:
        """返回三个 WS 通道的连接状态（供诊断快照使用）。"""
        return {
            "userData":   self._user_connected,
            "forceOrder": self._force_order_task is not None and not self._force_order_task.done(),
            "markPrice":  self._mark_price_task is not None and not self._mark_price_task.done(),
        }

    def _mark_user_stream_down(self) -> None:
        """[v3.89 #3] userData 流不可用：置断线状态并启动（唯一的）补偿任务。"""
        self._user_connected = False
        self._user_ws = None
        if self._stop_flag or not self._disconnect_callbacks:
            return
        if self._compensation_task and not self._compensation_task.done():
            return   # 已有补偿任务在运行，不重复启动
        self._compensation_task = asyncio.create_task(
            self._compensation_loop(), name="ws_rest_compensation")

    async def _compensation_loop(self) -> None:
        """
        [v3.89 #3] 有界 REST 补偿：断线期间主动对齐订单/持仓状态。
        结束条件：userData 已重连 / 超过 COMPENSATION_TIMEOUT 秒 / 监听器停止。
        重连后不取消正在执行的回调（避免对齐做到一半被打断），只是不再发起下一轮。
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + COMPENSATION_TIMEOUT
        rounds = 0
        try:
            while not self._stop_flag and not self._user_connected:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    logger.warning(f"[ws] REST 补偿任务达到 {COMPENSATION_TIMEOUT}s 上限，停止（userData 仍未恢复）")
                    return
                rounds += 1
                for cb in list(self._disconnect_callbacks):
                    try:
                        await asyncio.wait_for(cb(), timeout=max(remaining, 1.0))
                    except asyncio.TimeoutError:
                        logger.warning("[ws] REST 补偿回调超时")
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.warning(f"[ws] REST 补偿回调异常: {e}")
                await asyncio.sleep(min(COMPENSATION_INTERVAL, max(deadline - loop.time(), 0)))
            if self._user_connected:
                logger.info(f"[ws] userData 已恢复，REST 补偿任务结束（共 {rounds} 轮）")
        except asyncio.CancelledError:
            return

    def get_stream_health(self) -> list:
        """[(流名, 距最后一条消息的秒数|None, ok/stale/never)]。"""
        from shared.observability import stream_health
        return stream_health(dict(self._stream_last_msg), time.monotonic())

    async def _health_loop(self, interval: float = 300.0) -> None:
        """每 5 分钟输出各 WS 流的存活状态（有 stale/never 时用 WARNING）。"""
        from shared.observability import format_stream_health
        while not self._stop_flag:
            await asyncio.sleep(interval)
            try:
                rows = self.get_stream_health()
                msg = format_stream_health(rows)
                if any(st != "ok" for _, _, st in rows):
                    logger.warning(msg)
                else:
                    logger.info(msg)
            except Exception as e:
                logger.debug(f"[ws-health] 输出失败: {e}")

    async def start(self) -> None:
        """启动所有后台任务，不阻塞调用方。"""
        self._stop_flag = False
        self._health_task = asyncio.create_task(self._health_loop(), name="ws_health")
        self._user_data_task   = asyncio.create_task(self._user_data_loop(),    name="ws_user_data")
        self._mark_price_task  = asyncio.create_task(self._mark_price_loop(),   name="ws_mark_price")
        self._keepalive_task   = asyncio.create_task(self._keepalive_loop(),    name="ws_keepalive")
        self._force_order_task = asyncio.create_task(self._force_order_loop(),  name="ws_force_order")
        logger.info("[ws] WsOrderListener 已启动（含强平事件流）")

    async def stop(self) -> None:
        """优雅停止所有后台任务。"""
        self._stop_flag = True
        for task in (
            self._user_data_task, self._mark_price_task,
            self._keepalive_task, self._force_order_task,
            self._compensation_task,   # [v3.89 #3]
            self._health_task,
        ):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        logger.info("[ws] WsOrderListener 已停止")

    def subscribe(self, symbol: str) -> None:
        """动态订阅某标的的 markPrice stream（线程安全，投递到事件循环）。"""
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(self._sync_subscribe, symbol)

    def unsubscribe(self, symbol: str) -> None:
        """动态取消某标的的 markPrice stream（线程安全，投递到事件循环）。"""
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(self._sync_unsubscribe, symbol)

    def _sync_subscribe(self, symbol: str) -> None:
        sym = symbol.upper()
        if sym not in self._subscribed:
            self._subscribed.add(sym)
            logger.info(f"[ws] 订阅 markPrice: {sym}")
            self._rebuild_event.set()

    def _sync_unsubscribe(self, symbol: str) -> None:
        sym = symbol.upper()
        if sym in self._subscribed:
            self._subscribed.discard(sym)
            logger.info(f"[ws] 取消订阅 markPrice: {sym}")
            self._rebuild_event.set()

    # ─────────────────────────────────────────────────────────────────────────
    # userData stream
    # ─────────────────────────────────────────────────────────────────────────

    async def _user_data_loop(self) -> None:
        """
        持续维护 userData WebSocket。
        断线后指数退避重连，每次重连重新申请 listenKey。
        """
        delay = RECONNECT_BASE
        while not self._stop_flag:
            try:
                # [v3.89 #6] get_listen_key 是同步 requests（内部重试还会同步 sleep），
                #   直接在协程里调用会卡住整个事件循环；改走有界线程池，超时按「拿不到 key」处理。
                try:
                    self._listen_key = await _run_sync(binance_api.get_listen_key, timeout=20.0)
                except RunSyncTimeout:
                    logger.warning("[ws] 申请 listenKey 超过 20s 未返回，按失败处理（后台请求可能仍在执行）")
                    self._listen_key = None
                if not self._listen_key:
                    logger.error("[ws] 无法获取 listenKey，10s 后重试")
                    self._mark_user_stream_down()   # [v3.89 #3]
                    await asyncio.sleep(10)
                    continue

                url = USER_DATA_URL_TPL.format(listen_key=self._listen_key)
                _lk = str(self._listen_key or "")
                logger.info(f"[ws] userData 连接: {USER_DATA_URL_TPL.format(listen_key=_lk[:4] + '****')}")  # [fix #39] listenKey 脱敏

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    proxy=_FB_USER.current(),
                    max_queue=_MP_MAX_QUEUE,   # [fix] 限制接收队列，防止消息堆积导致内存泄漏
                ) as ws:
                    _FB_USER.opened()
                    delay = RECONNECT_BASE  # 连接成功后重置退避
                    self._user_ws = ws                # [v3.89 #7]
                    self._user_connected = True       # [v3.89 #3] 补偿任务见此状态后不再发起下一轮
                    # v3.33 重连回调：通知主进程触发持仓对齐
                    if self._reconnect_callbacks:
                        logger.info("[ws] userData 重连成功，触发重连回调")
                        for _cb in self._reconnect_callbacks:
                            try:
                                import asyncio as _aio
                                _aio.ensure_future(_cb())
                            except Exception as _cbe:
                                logger.warning(f"[ws] 重连回调异常: {_cbe}")
                    async for raw in ws:
                        self._stream_last_msg["userData"] = time.monotonic()
                        if self._stop_flag:
                            return
                        await self._dispatch_user_data(raw)

            except asyncio.CancelledError:
                return
            except (ConnectionClosed, WebSocketException) as e:
                logger.warning(f"[ws] userData 断线: {e}，{delay}s 后重连")
                _FB_USER.failed(e)
            except Exception as e:
                logger.error(f"[ws] userData 异常: {e}，{delay}s 后重连")
                _FB_USER.failed(e)

            self._mark_user_stream_down()   # [v3.89 #3] 断线立即启动 REST 补偿，不等重连退避
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX)

    async def _dispatch_user_data(self, raw: str) -> None:
        """解析 userData 帧，触发订单回调。"""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        event_type = msg.get("e")
        if event_type == "listenKeyExpired":
            # [v3.89 #7] listenKey 过期事件：旧流已失效，显式关闭连接驱动重建（重连后由重连回调完成对齐）
            logger.warning("[ws] 收到 listenKeyExpired，主动关闭 userData 连接以重建")
            self._listen_key = None
            await self._force_user_reconnect("listenKeyExpired")
            return
        if event_type != "ORDER_TRADE_UPDATE":
            return

        order = msg.get("o", {})
        for cb in self._order_callbacks:
            try:
                await cb(order)
            except Exception as e:
                logger.error(f"[ws] order_callback 异常: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # markPrice stream
    # ─────────────────────────────────────────────────────────────────────────

    async def _mark_price_loop(self) -> None:
        """
        持续维护 markPrice 组合 stream。
        当 _rebuild_event 触发（subscribe/unsubscribe）或断线时，重建连接。
        无持仓时保持等待，有持仓后立刻连接。
        """
        delay = RECONNECT_BASE
        ws_conn = None

        while not self._stop_flag:
            # 等待有标的可订阅
            if not self._subscribed:
                self._rebuild_event.clear()
                try:
                    await asyncio.wait_for(self._rebuild_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                continue

            # 构造当前订阅列表的 URL
            streams = "/".join(
                f"{sym.lower()}@markPrice" for sym in sorted(self._subscribed)
            )
            url = MARK_PRICE_URL_TPL.format(streams=streams)
            logger.info(f"[ws] markPrice 连接: {len(self._subscribed)} 个标的")

            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    proxy=_FB_MARK.current(),
                    max_queue=_MP_MAX_QUEUE,   # [fix] 限制接收队列，防止消息堆积导致内存泄漏
                ) as ws:
                    _FB_MARK.opened()
                    ws_conn = ws
                    delay = RECONNECT_BASE

                    async for raw in ws:
                        self._stream_last_msg["markPrice"] = time.monotonic()
                        if self._stop_flag:
                            return
                        # 检查是否需要重建（订阅列表变化）
                        if self._rebuild_event.is_set():
                            self._rebuild_event.clear()
                            break  # 退出内层循环，重新连接
                        # [fix] 有限并发分发：最多 _MP_MAX_INFLIGHT 个 dispatch 同时在途，超出直接丢弃（只保留最新价）
                        # 避免回调慢时任务无限堆积，同时不阻塞接收循环
                        self._mp_try_dispatch(raw)

            except asyncio.CancelledError:
                return
            except (ConnectionClosed, WebSocketException) as e:
                logger.warning(f"[ws] markPrice 断线: {e}，{delay}s 后重连")
                _FB_MARK.failed(e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)
            except Exception as e:
                logger.error(f"[ws] markPrice 异常: {e}，{delay}s 后重连")
                _FB_MARK.failed(e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)
            finally:
                ws_conn = None

    def _mp_try_dispatch(self, raw: str) -> None:
        """有空余槽位才创建 dispatch 任务；槽位满则丢弃该帧（markPrice 高频流，下一帧很快就来）。"""
        if self._mp_inflight >= _MP_MAX_INFLIGHT:
            self._mp_drop_count += 1
            logger.debug("[ws] markPrice 分发槽位满，丢弃帧")
            self._mp_log_drops()
            return
        self._mp_inflight += 1                      # 创建前同步占位
        task = asyncio.ensure_future(self._mp_dispatch_sem_wrap(raw))
        self._mp_tasks.add(task)
        task.add_done_callback(self._mp_tasks.discard)

    async def _mp_dispatch_sem_wrap(self, raw: str) -> None:
        try:
            await self._dispatch_mark_price(raw)
        finally:
            self._mp_inflight -= 1

    def _mp_log_drops(self, now: Optional[float] = None) -> None:
        """每 _MP_DROP_LOG_SEC 秒汇总一次丢帧数（warning），汇总后清零。"""
        now = time.monotonic() if now is None else now
        if now - self._mp_drop_last_log >= _MP_DROP_LOG_SEC and self._mp_drop_count > 0:
            logger.warning(
                f"[ws] markPrice 过去{int(now - self._mp_drop_last_log)}秒丢弃 {self._mp_drop_count} 帧"
                f"（并发槽位不足），当前活跃任务={self._mp_inflight}"
            )
            self._mp_drop_count = 0
            self._mp_drop_last_log = now

    async def _dispatch_mark_price(self, raw: str) -> None:
        """
        解析 markPrice 帧，触发价格回调。
        Binance 组合 stream 格式：{"stream":"btcusdt@markPrice","data":{...}}
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # 组合 stream 有 data 层，单一 stream 没有
        data = msg.get("data", msg)
        if data.get("e") != "markPriceUpdate":
            return

        symbol     = data.get("s", "")
        mark_price = float(data.get("p", 0))
        ts         = int(data.get("E", 0))  # 使用事件时间 E，而不是下次资金费时间 T

        if not symbol or mark_price <= 0:
            return

        for cb in self._price_callbacks:
            try:
                await cb(symbol, mark_price, ts)
            except Exception as e:
                logger.error(f"[ws] price_callback 异常: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # forceOrder stream（全市场强平事件）
    # ─────────────────────────────────────────────────────────────────────────

    async def _force_order_loop(self) -> None:
        """
        持续维护 !forceOrder@arr WebSocket，接收全市场强平事件。
        断线后指数退避重连，缓存最近 LIQ_WINDOW_SEC 秒的强平量。
        """
        delay = RECONNECT_BASE
        while not self._stop_flag:
            try:
                logger.info(f"[ws] forceOrder 连接: {FORCE_ORDER_URL}")
                async with websockets.connect(
                    FORCE_ORDER_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    proxy=_FB_FORCE.current(),
                    max_queue=_MP_MAX_QUEUE,
                ) as ws:
                    _FB_FORCE.opened()
                    delay = RECONNECT_BASE
                    logger.info("[ws] 强平事件流已连接")
                    async for raw in ws:
                        self._stream_last_msg["forceOrder"] = time.monotonic()
                        if self._stop_flag:
                            return
                        await self._dispatch_force_order(raw)

            except asyncio.CancelledError:
                return
            except (ConnectionClosed, WebSocketException) as e:
                logger.warning(f"[ws] forceOrder 断线: {e}，{delay}s 后重连")
                _FB_FORCE.failed(e)
            except Exception as e:
                logger.error(f"[ws] forceOrder 异常: {e}，{delay}s 后重连")
                _FB_FORCE.failed(e)

            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX)

    async def _dispatch_force_order(self, raw: str) -> None:
        """
        解析强平帧，写入缓存并触发回调。

        Binance !forceOrder@arr 帧格式：
          {"e": "forceOrder", "E": <ts_ms>, "o": {
              "s": "BTCUSDT",   # symbol
              "S": "SELL",      # 被强平单方向（SELL = 多头被强平，BUY = 空头被强平）
              "q": "0.01",      # 成交量（合约数量）
              "ap": "29000",    # 成交均价
              ... }}
        我们统一转换为：
          side = 'LONG'  当 S == 'SELL'（多头仓位被强平，产生卖出强平单）
          side = 'SHORT' 当 S == 'BUY' （空头仓位被强平，产生买入强平单）
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        if msg.get("e") != "forceOrder":
            return

        o      = msg.get("o", {})
        symbol = o.get("s", "")
        raw_side = o.get("S", "").upper()  # SELL or BUY
        qty    = float(o.get("q", 0))
        price  = float(o.get("ap", 0))
        ts_ms  = int(msg.get("E", time.time() * 1000))
        ts_sec = ts_ms / 1000.0

        if not symbol or qty <= 0:
            return

        # SELL 强平单 = 多头被爆（LONG 方向），BUY 强平单 = 空头被爆（SHORT 方向）
        side = "LONG" if raw_side == "SELL" else "SHORT"
        notional = qty * price  # USDT 名义额

        # 写缓存
        cutoff = ts_sec - LIQ_WINDOW_SEC
        async with self._liq_lock:
            if symbol not in self._liq_cache:
                self._liq_cache[symbol] = {
                    "LONG":  collections.deque(),
                    "SHORT": collections.deque(),
                }
            dq = self._liq_cache[symbol][side]
            dq.append((ts_sec, notional))
            # 清理过期条目（只保留窗口内）
            while dq and dq[0][0] < cutoff:
                dq.popleft()

        event = {
            "symbol": symbol,
            "side":   side,
            "qty":    qty,
            "price":  price,
            "ts":     ts_ms,
        }

        if notional >= 10_000:  # 仅打印 1W+ USDT 的强平，避免日志刷屏
            logger.debug(
                f"[ws] 强平: {symbol} {side} {notional/1000:.1f}k USDT "
                f"qty={qty} price={price}"
            )

        for cb in self._liq_callbacks:
            try:
                await cb(event)
            except Exception as e:
                logger.error(f"[ws] liq_callback 异常: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # listenKey keepalive
    # ─────────────────────────────────────────────────────────────────────────

    async def _force_user_reconnect(self, reason: str) -> None:
        """
        [v3.89 #7] 显式驱动 userData 连接重建：关闭当前连接，使接收循环退出并走重连路径
        （重新申请 listenKey，重连成功后触发重连回调做订单对齐）。
        原写法只把 _listen_key 置 None，接收循环不检查它，旧连接可能一直挂着直到服务端断开。
        """
        w = self._user_ws
        if w is None:
            return   # 连接已不存在，重连循环自己会重新申请 listenKey
        try:
            await w.close()
            logger.info(f"[ws] 已主动关闭 userData 连接以重建: {reason}")
        except Exception as e:
            logger.warning(f"[ws] 主动关闭 userData 连接失败: {e}")

    async def _keepalive_loop(self) -> None:
        """每 KEEPALIVE_INTERVAL 秒续期 listenKey，防止 userData stream 超时断开。"""
        while not self._stop_flag:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            if self._stop_flag:
                return
            if not self._listen_key:
                continue
            try:
                # [v3.89 #6] 续期同样是同步 HTTP，改走有界线程池；
                #   超时只记日志、下一轮再试，不触发强制重连（真正返回失败仍走原有重建逻辑）。
                try:
                    ok = await _run_sync(binance_api.keepalive_listen_key, self._listen_key, timeout=15.0)
                except RunSyncTimeout:
                    logger.warning("[ws] listenKey 续期超过 15s 未返回，本轮跳过，下一轮再试")
                    continue
                if ok:
                    logger.debug("[ws] listenKey keepalive 成功")
                else:
                    logger.warning("[ws] listenKey keepalive 失败，主动关闭 userData 连接以重建")
                    self._listen_key = None
                    await self._force_user_reconnect("keepalive 失败")   # [v3.89 #7]
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[ws] keepalive 异常，主动关闭 userData 连接以重建: {e}")
                self._listen_key = None
                await self._force_user_reconnect("keepalive 异常")   # [v3.89 #7]

    # ─────────────────────────────────────────────────────────────────────────
    # 等待订单事件（供 executor 同步阻塞式等待成交）
    # ─────────────────────────────────────────────────────────────────────────

    async def wait_order_event(
        self, order_id: int, timeout: float = 120.0
    ) -> dict | None:
        """
        等待指定 order_id 出现 FILLED / CANCELED 事件。
        返回原始 order dict，超时返回 None。
        """
        return await self._wait_for_orders([order_id], timeout)

    async def wait_any_order_event(
        self, order_ids: list, timeout: float = 5.0
    ) -> dict | None:
        """
        等待 order_ids 中任意一个出现 FILLED / CANCELED 事件。
        返回第一个触发的原始 order dict，超时返回 None。
        """
        return await self._wait_for_orders(order_ids, timeout)

    async def _wait_for_orders(
        self, order_ids: list, timeout: float
    ) -> dict | None:
        """内部实现：注册一次性 Future，订单事件触发后 resolve。"""
        if not order_ids:
            return None

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        ids = set(order_ids)

        async def _one_shot_cb(order: dict) -> None:
            oid = int(order.get("i", 0))
            status = order.get("X", "")
            if oid in ids and status in ("FILLED", "CANCELED", "EXPIRED"):
                if not future.done():
                    future.set_result(order)

        # 临时注册
        self._order_callbacks.append(_one_shot_cb)
        try:
            result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
            return result
        except asyncio.TimeoutError:
            return None
        finally:
            try:
                self._order_callbacks.remove(_one_shot_cb)
            except ValueError:
                pass
