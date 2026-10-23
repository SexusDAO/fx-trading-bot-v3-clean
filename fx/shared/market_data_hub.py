"""
fx/shared/market_data_hub.py — 公共市场数据 WS 中心（阶段一：全市场标记价格 + 资金费率）

订阅 `!markPrice@arr@1s`（无需签名），本地缓存全市场最新标记价/资金费率/下次结算时间，
供 scanner 同步读取，减少扫描路径上的 REST 请求。

与 ws_listener 完全隔离：本模块自己持有一条独立的公共数据连接，不碰 userData / forceOrder / 持仓 markPrice。
默认关闭（config.MARKET_DATA_HUB_ENABLED = False）；关闭时本模块不会被 scanner / main 调用。

并发模型：WS 线程写、扫描线程读。缓存是 {symbol: (entry, recv_monotonic)} 的不可变快照字典，
写入时在锁内复制并整体替换引用；读取不加锁（拿到引用后只读）。
返回给调用方的字典与 REST premiumIndex 同字段名（symbol/markPrice/indexPrice/lastFundingRate/nextFundingTime/time），
额外带 age_sec。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import statistics
import threading
import time
from typing import Callable, Optional

import config

logger = logging.getLogger("market_data_hub")

# 代理只由环境变量驱动；未设置或为空 → None（直连）
_PROXY = (os.environ.get("PROXY_URL") or os.environ.get("BINANCE_PROXY") or "").strip() or None
try:
    from config import HUB_WS_PROXY as _HUB_WS_PROXY
    _PROXY = (str(_HUB_WS_PROXY).strip() or None) if _HUB_WS_PROXY != "__env__" else _PROXY   # 空字符串 → None（直连）
except ImportError:
    pass

RECONNECT_BASE = getattr(config, "WS_RECONNECT_BASE", 3)
RECONNECT_MAX = getattr(config, "WS_RECONNECT_MAX", 60)
PING_INTERVAL = 20          # 秒：每 20 秒发 ping
try:
    from config import HUB_NO_MSG_TIMEOUT as NO_MSG_TIMEOUT   # 秒：无任何消息 → 断开重连
except Exception:
    NO_MSG_TIMEOUT = 120
SINGLE_MAX_AGE_SEC = 5.0    # 单标的读取：超过则返回 None
ALL_MAX_AGE_SEC = 10.0      # 全量快照：超过的条目被过滤
STATS_LOG_INTERVAL = 300    # 秒：5 分钟一条 info
STREAM_PATH = "/market/ws/!markPrice@arr@1s"   # 币安合约行情新路径（旧路径 /ws/ 已不推送行情）
MARKET_STREAM_PATH = "/market/stream"          # 组合流：!miniTicker@arr（后续还有 K 线），连上后用 SUBSCRIBE 订阅
TICKER_STREAM = "!miniTicker@arr"
TICKER_MAX_AGE_SEC = 10.0
KLINE_INTERVALS = ("15m", "1h", "4h")
_INTERVAL_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}
KLINE_HISTORY = 100                            # 每个 (标的, 周期) 保留/初始化拉取的 K 线根数
# 按周期动态 grace：最后一根 K 线的结束时间已过去这么久仍无新推送 → 视为过旧，读取返回 None
_KLINE_STALE_GRACE_MS = {"15m": 60_000, "1h": 300_000, "4h": 600_000}
# 历史 K 线拉取专用线程池：不占用 asyncio 默认线程池，避免与扫描等线程竞争
from concurrent.futures import ThreadPoolExecutor
try:
    from config import HUB_LOAD_MAX_WORKERS as _HUB_LOAD_MAX_WORKERS
except Exception:
    _HUB_LOAD_MAX_WORKERS = 2
_LOAD_POOL = ThreadPoolExecutor(max_workers=int(_HUB_LOAD_MAX_WORKERS), thread_name_prefix="hub-load")
KLINE_FETCH_CONCURRENCY = 6                    # 历史 K 线 REST 拉取并发上限
KLINE_RETRY_SEC = 30.0                         # 历史拉取失败后的重试间隔
KLINE_STREAM_PER_CONN = 180                    # 单条 /market/stream 连接上的 K 线 stream 上限（币安单连接 200，留出 ticker 等余量）
SUB_BATCH = 100                                # 每条 SUBSCRIBE/UNSUBSCRIBE 的 stream 数
TICKER_MIN_COVERAGE = 0.9                      # 新鲜条目数 / 已见过的条目数 低于此值时，scanner 不采用 hub 的全量 ticker


class _Series:
    """一个 (标的, 周期) 的 K 线序列。rows=None 表示尚未就绪（历史未拉到/断线/出现缺口）。"""
    __slots__ = ("rows", "pending", "loading", "retry_at", "shard")

    def __init__(self, shard: int = 0):
        self.shard = shard      # 该序列所在的组合流连接编号
        self.rows = None
        self.pending = []      # 历史拉取期间收到的 WS 行，拉完后合并
        self.loading = False
        self.retry_at = 0.0


class _Shard:
    """一条 /market/stream 组合流连接的状态。shard 0 同时承载 !miniTicker@arr。"""
    __slots__ = ("idx", "ws", "subscribed", "connected", "gen")

    def __init__(self, idx: int):
        self.idx = idx
        self.ws = None
        self.subscribed: set = set()     # 当前连接上已订阅的 K 线 stream 名
        self.connected = False
        self.gen = 0                     # 该连接每次断线 +1，用来丢弃断线前发出的历史拉取结果


def _upsert(rows: list, row: list, iv_ms: int) -> bool:
    """把一根 K 线并入 rows（最后一根是未收盘的）。返回 False 表示出现缺口（调用方应作废并重拉历史）。"""
    t, last_t = row[0], rows[-1][0]
    if t == last_t:
        rows[-1] = row
        return True
    if t < last_t:                                   # 迟到的旧根：只替换最近几根里同开盘时间的那根
        for i in range(len(rows) - 2, max(len(rows) - 4, -1), -1):
            if rows[i][0] == t:
                rows[i] = row
                break
        return True
    if t == last_t + iv_ms:                          # 下一根：追加并裁掉最旧的一根
        rows.append(row)
        if len(rows) > KLINE_HISTORY:
            del rows[0:len(rows) - KLINE_HISTORY]
        return True
    return False


class MarketDataHub:
    def __init__(self, url: Optional[str] = None, clock: Callable[[], float] = time.monotonic,
                 market_url: Optional[str] = None, wall: Callable[[], float] = time.time,
                 kline_fetcher: Optional[Callable] = None):
        self._url = url or (config.BINANCE_WS_URL + STREAM_PATH)
        self._market_url = market_url or (config.BINANCE_WS_URL + MARKET_STREAM_PATH)
        self._clock = clock
        self._lock = threading.Lock()            # 仅保护写入与计数器
        self._prices: dict = {}                  # {symbol: (entry_dict, recv_ts)}，整体替换，不原地改
        self._ready = False
        self._connected = False
        # miniTicker 缓存：{symbol: (entry, recv_ts)}，整体替换；_ticker_seen 记录连接以来见过的标的
        self._tickers: dict = {}
        self._ticker_seen: set = set()
        self._ticker_ready = False
        # K 线分片：每条连接最多 KLINE_STREAM_PER_CONN 个 stream；标的数上限 KLINE_HUB_SYMBOLS_MAX 决定连接数
        self._syms_per_conn = max(KLINE_STREAM_PER_CONN // len(KLINE_INTERVALS), 1)
        _mx = max(int(getattr(config, "KLINE_HUB_SYMBOLS_MAX", self._syms_per_conn)), 1)
        self._nshards = max(-(-_mx // self._syms_per_conn), 1)
        self._shards = [_Shard(i) for i in range(self._nshards)]
        self._assign: dict = {}                  # {SYMBOL: shard 编号}，已分配的标的保持不动（避免重订阅）
        self._wall = wall
        self._kline_fetcher = kline_fetcher
        self._series: dict = {}                  # {(SYMBOL, interval): _Series}
        self._kl_lock = threading.Lock()
        self._desired: list = []
        self._sync_event: Optional[asyncio.Event] = None
        self._sync_lock: Optional[asyncio.Lock] = None
        self._fetch_sem: Optional[asyncio.Semaphore] = None
        self._load_tasks: set = set()
        self.market_reconnect_count: int = 0
        self.ticker_frame_count: int = 0
        # 可观测字段
        self.last_msg_ts: float = 0.0            # 最近一次收到消息的 monotonic 时间
        self.reconnect_count: int = 0
        self.frame_count: int = 0
        # 线程 / 事件循环
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._stopping = False

    # ── 对外读取接口（不加锁） ─────────────────────────────────────────────
    # shard 0 的兼容属性（ticker 所在连接；历史代码/测试按单连接读写）
    @property
    def _market_connected(self) -> bool:
        return self._shards[0].connected

    @_market_connected.setter
    def _market_connected(self, v: bool) -> None:
        self._shards[0].connected = bool(v)

    @property
    def _ws_market(self):
        return self._shards[0].ws

    @_ws_market.setter
    def _ws_market(self, v) -> None:
        self._shards[0].ws = v

    @property
    def _subscribed(self) -> set:
        return self._shards[0].subscribed

    @_subscribed.setter
    def _subscribed(self, v: set) -> None:
        self._shards[0].subscribed = v

    def is_ready(self) -> bool:
        """至少收到一帧全市场数据，且当前连接有效（断线后为 False，直到重连后收到新帧）。"""
        return self._ready

    def get_mark_price(self, symbol: str) -> Optional[dict]:
        """返回 {symbol, markPrice, lastFundingRate, nextFundingTime, ..., age_sec}；未就绪/缺失/超过 5 秒返回 None。"""
        if not self._ready:
            return None
        item = self._prices.get(symbol)
        if item is None:
            return None
        entry, recv = item
        age = self._clock() - recv
        if age > SINGLE_MAX_AGE_SEC:
            return None
        out = dict(entry)
        out["age_sec"] = round(age, 3)
        return out

    def get_all_mark_prices(self) -> dict:
        """全量快照 {symbol: dict(同上)}；age_sec 超过 10 秒的条目被过滤；未就绪返回 {}。"""
        if not self._ready:
            return {}
        now = self._clock()
        snap = self._prices                       # 拿引用即可，写入方只会替换不会原地改
        out = {}
        for sym, (entry, recv) in snap.items():
            age = now - recv
            if age <= ALL_MAX_AGE_SEC:
                d = dict(entry)
                d["age_sec"] = round(age, 3)
                out[sym] = d
        return out

    # ── miniTicker 读取接口 ────────────────────────────────────────────────
    def is_ticker_ready(self) -> bool:
        return self._ticker_ready

    def get_ticker(self, symbol: str) -> Optional[dict]:
        """返回与 REST /fapi/v1/ticker/24hr 同字段名的 dict（含 age_sec）；未就绪/缺失/超过 10 秒返回 None。"""
        if not self._ticker_ready:
            return None
        item = self._tickers.get(symbol)
        if item is None:
            return None
        entry, recv = item
        age = self._clock() - recv
        if age > TICKER_MAX_AGE_SEC:
            return None
        out = dict(entry)
        out["age_sec"] = round(age, 3)
        return out

    def get_all_tickers(self) -> dict:
        """全量快照 {symbol: dict}；age_sec 超过 10 秒的条目被过滤；未就绪返回 {}。"""
        if not self._ticker_ready:
            return {}
        now = self._clock()
        out = {}
        for sym, (entry, recv) in self._tickers.items():
            age = now - recv
            if age <= TICKER_MAX_AGE_SEC:
                d = dict(entry)
                d["age_sec"] = round(age, 3)
                out[sym] = d
        return out

    # ── K 线读取 / 订阅接口 ────────────────────────────────────────────────
    def subscribe_klines(self, symbols) -> None:
        """设置要订阅的标的（整体替换，自动增删；线程安全）。周期固定 15m/1h/4h，标的数上限 KLINE_HUB_SYMBOLS_MAX。"""
        syms, seen = [], set()
        for x in symbols or []:
            u = str(x).upper()
            if u and u not in seen:
                seen.add(u)
                syms.append(u)
        mx = min(int(getattr(config, "KLINE_HUB_SYMBOLS_MAX", 200)), self._nshards * self._syms_per_conn)
        if len(syms) > mx:
            logger.warning(f"[hub] K线订阅标的 {len(syms)} 个超过上限 {mx}，只订阅前 {mx} 个")
            syms = syms[:mx]
        want = {(x, iv) for x in syms for iv in KLINE_INTERVALS}
        with self._kl_lock:
            self._desired = syms
            # 分片：保留旧标的所在连接；新标的按顺序填入最靠前的有空位的连接（BTC/持仓在前 → 落在连接 0）
            counts = [0] * self._nshards
            assign = {x: self._assign[x] for x in syms if self._assign.get(x, self._nshards) < self._nshards}
            for i in assign.values():
                counts[i] += 1
            for x in syms:
                if x not in assign:
                    i = next(j for j in range(self._nshards) if counts[j] < self._syms_per_conn)
                    assign[x] = i
                    counts[i] += 1
            self._assign = assign
            for k in [k for k in self._series if k not in want]:
                del self._series[k]
            for k in want:
                self._series.setdefault(k, _Series(assign[k[0]]))
        self._kick()

    def _kick(self) -> None:
        loop, ev = self._loop, self._sync_event
        if loop is not None and ev is not None:
            try:
                loop.call_soon_threadsafe(ev.set)
            except RuntimeError:
                pass

    def get_klines(self, symbol: str, interval: str, limit: int) -> Optional[list]:
        """与 binance_api.get_klines 同格式（最后一根是未收盘的）；未就绪/未订阅/历史不足 limit/数据过旧返回 None。"""
        n = max(int(limit), 1)
        with self._kl_lock:
            st = self._series.get((str(symbol).upper(), interval))
            if st is None or st.rows is None or len(st.rows) < n:
                return None
            if not self._shards[min(st.shard, self._nshards - 1)].connected:
                return None
            if self._wall() * 1000 > float(st.rows[-1][6]) + _KLINE_STALE_GRACE_MS.get(interval, 60_000):
                return None
            return [list(r) for r in st.rows[-n:]]

    def kline_ready_counts(self) -> tuple:
        with self._kl_lock:
            return (sum(1 for st in self._series.values() if st.rows is not None), len(self._series))

    def ticker_coverage(self) -> tuple:
        """(新鲜条目数, 本次连接以来见过的条目数)。miniTicker@arr 只推有变化的标的，冷门标的可能长时间不更新。"""
        if not self._ticker_ready:
            return (0, 0)
        now = self._clock()
        fresh = sum(1 for _, r in self._tickers.values() if now - r <= TICKER_MAX_AGE_SEC)
        return (fresh, len(self._ticker_seen))

    def stats(self) -> dict:
        snap = self._prices
        now = self._clock()
        ages = [now - r for _, r in snap.values()]
        return {
            "ready": self._ready, "connected": self._connected,
            "ticker_ready": self._ticker_ready, "ticker_symbols": len(self._tickers),
            "kline_ready": self.kline_ready_counts()[0], "kline_total": self.kline_ready_counts()[1],
            "kline_conns_up": sum(1 for sh in self._shards if sh.connected), "kline_conns": self._nshards,
            "ticker_frame_count": self.ticker_frame_count, "market_reconnect_count": self.market_reconnect_count,
            "frame_count": self.frame_count, "reconnect_count": self.reconnect_count,
            "symbols": len(snap),
            "last_msg_age_sec": (now - self.last_msg_ts) if self.last_msg_ts else None,
            "age_median_sec": statistics.median(ages) if ages else None,
            "age_max_sec": max(ages) if ages else None,
        }

    # ── 帧解析 / 断线处理（同步，可直接测试） ───────────────────────────────
    def _handle_frame(self, raw) -> int:
        """解析一帧 `!markPrice@arr`（list[dict]），更新缓存；返回更新的标的数。非法帧返回 0。"""
        try:
            data = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        except Exception:
            return 0
        if isinstance(data, dict):                # 兼容组合 stream 的 {"stream":..., "data":[...]}
            data = data.get("data")
        if not isinstance(data, list):
            return 0
        now = self._clock()
        updates = {}
        for it in data:
            if not isinstance(it, dict) or it.get("e") != "markPriceUpdate":
                continue
            sym = it.get("s")
            if not sym or it.get("p") is None:
                continue
            updates[sym] = ({
                "symbol": sym,
                "markPrice": it.get("p"),
                "indexPrice": it.get("i"),
                "lastFundingRate": it.get("r"),
                "nextFundingTime": it.get("T"),
                "time": it.get("E"),
            }, now)
        with self._lock:
            self.last_msg_ts = now
            self.frame_count += 1
            if updates:
                new = dict(self._prices)          # 复制 + 整体替换，读端不受影响
                new.update(updates)
                self._prices = new
                self._ready = True
        return len(updates)

    def _handle_market_frame(self, raw) -> None:
        """处理 /market/stream 组合流的帧：{"stream": name, "data": ...}；订阅确认/错误帧单独处理。"""
        try:
            msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        except Exception:
            return
        if not isinstance(msg, dict):
            return
        if "error" in msg:
            logger.warning(f"[hub] 组合流返回错误: {msg.get('error')}")
            return
        stream, data = msg.get("stream"), msg.get("data")
        if stream is None or data is None:
            return                                   # {"result":null,"id":..} 订阅确认
        if stream == TICKER_STREAM:
            self._handle_tickers(data)
        elif "@kline_" in str(stream):
            self._handle_kline(data)

    def _handle_kline(self, data) -> None:
        """处理 `<sym>@kline_<iv>`：k 对象 → REST 同格式行 [t,o,h,l,c,v,T,q,n,V,Q,"0"]；x=false 更新最后一根，下一根开盘时追加。"""
        k = data.get("k") if isinstance(data, dict) else None
        if not isinstance(k, dict):
            return
        try:
            key = (str(k.get("s") or data.get("s")).upper(), k["i"])
            row = [int(k["t"]), k["o"], k["h"], k["l"], k["c"], k["v"], int(k["T"]), k["q"],
                   int(k.get("n", 0)), k.get("V", "0"), k.get("Q", "0"), "0"]
        except (KeyError, TypeError, ValueError):
            return
        iv_ms = _INTERVAL_MS.get(key[1])
        if iv_ms is None:
            return
        gap = False
        with self._kl_lock:
            st = self._series.get(key)
            if st is None:
                return
            if st.rows is None:
                st.pending.append(row)
                if len(st.pending) > 60:
                    del st.pending[0:len(st.pending) - 60]
                return
            if not _upsert(st.rows, row, iv_ms):
                logger.warning(f"[hub] K线缺口 {key[0]} {key[1]}，作废并重拉历史")
                st.rows, st.pending, gap = None, [row], True
                st.retry_at = 0.0
        if gap:
            self._kick()

    def _handle_tickers(self, data) -> int:
        """解析 !miniTicker@arr（list[dict]）并合并进缓存。字段：s,c,o,h,l,v,q。返回更新的标的数。"""
        if not isinstance(data, list):
            return 0
        now = self._clock()
        updates = {}
        for it in data:
            if not isinstance(it, dict):
                continue
            sym = it.get("s")
            if not sym or it.get("c") is None or it.get("o") is None:
                continue
            try:
                last, opn = float(it["c"]), float(it["o"])
            except (TypeError, ValueError):
                continue
            chg = (last - opn) / opn * 100 if opn else 0.0
            updates[sym] = ({
                "symbol": sym,
                "lastPrice": it.get("c"),
                "openPrice": it.get("o"),
                "highPrice": it.get("h"),
                "lowPrice": it.get("l"),
                "volume": it.get("v"),
                "quoteVolume": it.get("q"),
                "priceChange": f"{last - opn:.8f}",
                "priceChangePercent": f"{chg:.3f}",
            }, now)
        with self._lock:
            self.ticker_frame_count += 1
            if updates:
                new = dict(self._tickers)
                new.update(updates)
                self._tickers = new
                self._ticker_seen.update(updates)
                self._ticker_ready = True
        return len(updates)

    def _on_market_disconnect(self, idx: int = 0) -> None:
        """第 idx 条组合流断线：只作废该连接上的 K 线序列；连接 0 同时清空 ticker 缓存。重连期间读取返回 None/{}。"""
        sh = self._shards[idx]
        if idx == 0:
            with self._lock:
                self._ticker_ready = False
                self._tickers = {}
                self._ticker_seen = set()
        with self._kl_lock:
            sh.connected = False
            sh.gen += 1
            for st in self._series.values():
                if st.shard == idx:
                    st.rows, st.pending, st.loading, st.retry_at = None, [], False, 0.0
        sh.ws = None
        sh.subscribed = set()

    def _on_disconnect(self) -> None:
        """断线：清空缓存并置未就绪，重连期间读取一律返回 None/{}（不返回旧数据）。"""
        with self._lock:
            self._ready = False
            self._connected = False
            self._prices = {}

    # ── 后台线程 / 事件循环 ────────────────────────────────────────────────
    def start(self) -> None:
        """在后台线程启动 WS 连接，非阻塞；并注册为模块级默认实例。"""
        global _default
        if self._thread is not None and self._thread.is_alive():
            return
        self._stopping = False
        _default = self
        self._thread = threading.Thread(target=self._thread_main, name="market-data-hub", daemon=True)
        self._thread.start()
        logger.info(f"[hub] 已启动: {self._url}")

    def stop(self, timeout: float = 5.0) -> None:
        """优雅关闭：通知循环退出并等待线程结束。"""
        global _default
        self._stopping = True
        loop, ev = self._loop, self._stop_event
        if loop is not None and ev is not None:
            try:
                loop.call_soon_threadsafe(ev.set)
            except RuntimeError:
                pass
        if self._thread is not None:
            self._thread.join(timeout)
        self._on_disconnect()
        if _default is self:
            _default = None
        logger.info("[hub] 已停止")

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run())
        except Exception as e:
            logger.error(f"[hub] 线程异常退出: {e}")
        finally:
            try:
                loop.close()
            except Exception:
                pass

    async def _run(self) -> None:
        self._stop_event = asyncio.Event()
        self._sync_event = asyncio.Event()
        self._sync_lock = asyncio.Lock()
        self._fetch_sem = asyncio.Semaphore(KLINE_FETCH_CONCURRENCY)
        stats_task = asyncio.ensure_future(self._stats_loop())
        market_tasks = [asyncio.ensure_future(self._market_loop(i)) for i in range(self._nshards)]
        maint_task = asyncio.ensure_future(self._kline_maintainer())
        try:
            await self._connect_loop()
        finally:
            stats_task.cancel()
            for t in market_tasks:
                t.cancel()
            maint_task.cancel()

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(STATS_LOG_INTERVAL)
            s = self.stats()
            logger.info(
                f"[hub] 5min统计: frame_count={s['frame_count']} 覆盖标的={s['symbols']} "
                f"age_median={_fmt(s['age_median_sec'])}s age_max={_fmt(s['age_max_sec'])}s "
                f"reconnect_count={s['reconnect_count']} ready={s['ready']} "
                f"ticker_ready={s['ticker_ready']} ticker覆盖={s['ticker_symbols']} ticker帧={s['ticker_frame_count']} "
                f"kline就绪={s['kline_ready']}/{s['kline_total']}"
            )

    async def _connect_loop(self) -> None:
        import websockets
        delay = RECONNECT_BASE
        first = True
        while not self._stopping:
            if not first:
                self.reconnect_count += 1
                logger.warning(f"[hub] 重连第 {self.reconnect_count} 次（等待 {delay}s 后连接）")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                    return
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, RECONNECT_MAX)
            first = False
            try:
                async with websockets.connect(
                    self._url, ping_interval=20, ping_timeout=120, proxy=_PROXY, max_queue=64,
                    compression=None,   # 禁用 permessage-deflate：免解压，缓解 GIL 争抢
                ) as ws:
                    self._connected = True
                    delay = RECONNECT_BASE
                    logger.info("[hub] WS 已连接")
                    while not self._stopping:
                        # 30 秒无消息 → 超时异常 → 走断线重连
                        raw = await asyncio.wait_for(ws.recv(), timeout=NO_MSG_TIMEOUT)
                        self._handle_frame(raw)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.warning(f"[hub] {NO_MSG_TIMEOUT}s 无消息，断开重连")
            except Exception as e:
                logger.warning(f"[hub] 断线: {e}")
            finally:
                self._on_disconnect()


    async def _market_loop(self, idx: int = 0) -> None:
        """第 idx 条 /market/stream 组合流连接（连接 0 带 miniTicker；每条连接各自承载一部分 K 线）：连上后 SUBSCRIBE；断线清缓存并退避重连。"""
        import websockets
        delay = RECONNECT_BASE
        first = True
        while not self._stopping:
            if not first:
                self.market_reconnect_count += 1
                logger.warning(f"[hub] 组合流#{idx} 重连第 {self.market_reconnect_count} 次（等待 {delay}s 后连接）")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                    return
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, RECONNECT_MAX)
            first = False
            try:
                async with websockets.connect(
                    self._market_url, ping_interval=20, ping_timeout=120, proxy=_PROXY, max_queue=32,
                    compression=None,   # 禁用 permessage-deflate：免解压，缓解 GIL 争抢
                ) as ws:
                    self._shards[idx].connected = True
                    delay = RECONNECT_BASE
                    logger.info(f"[hub] 组合流#{idx} 已连接")
                    await self._on_market_connected(ws, idx)
                    while not self._stopping:
                        raw = await asyncio.wait_for(ws.recv(), timeout=NO_MSG_TIMEOUT)
                        self._handle_market_frame(raw)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.warning(f"[hub] 组合流#{idx} {NO_MSG_TIMEOUT}s 无消息，断开重连")
            except Exception as e:
                logger.warning(f"[hub] 组合流#{idx} 断线: {e}")
            finally:
                self._on_market_disconnect(idx)

    async def _send_subscribe(self, ws, params: list, method: str = "SUBSCRIBE") -> None:
        self._sub_id = getattr(self, "_sub_id", 0) + 1
        await ws.send(json.dumps({"method": method, "params": params, "id": self._sub_id}))

    async def _on_market_connected(self, ws, idx: int = 0) -> None:
        sh = self._shards[idx]
        sh.ws = ws
        sh.subscribed = set()
        if idx == 0:
            await self._send_subscribe(ws, [TICKER_STREAM])
        await self._sync_once()                      # 重连后：重新订阅全部 K 线 stream 并重拉历史

    async def _kline_maintainer(self) -> None:
        """每 2 秒（或被唤醒时）对账：订阅增删、重试/补拉历史。"""
        while not self._stopping:
            try:
                await asyncio.wait_for(self._sync_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            self._sync_event.clear()
            try:
                await self._sync_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[hub] K线对账异常: {e}")

    async def _sync_once(self) -> None:
        if not any(sh.ws is not None and sh.connected for sh in self._shards):
            return
        async with self._sync_lock:
            for sh in self._shards:
                await self._sync_shard(sh)

    async def _sync_shard(self, sh: "_Shard") -> None:
        ws = sh.ws
        if ws is None or not sh.connected:
            return
        with self._kl_lock:
            desired = {f"{s.lower()}@kline_{iv}" for s in self._desired
                       if self._assign.get(s, 0) == sh.idx for iv in KLINE_INTERVALS}
        to_sub = sorted(desired - sh.subscribed)
        to_unsub = sorted(sh.subscribed - desired)
        for i in range(0, len(to_unsub), SUB_BATCH):
            await self._send_subscribe(ws, to_unsub[i:i + SUB_BATCH], "UNSUBSCRIBE")
            await asyncio.sleep(0.2)                 # 币安限制：每条连接每秒最多 10 条入站消息
        sh.subscribed -= set(to_unsub)
        for i in range(0, len(to_sub), SUB_BATCH):
            await self._send_subscribe(ws, to_sub[i:i + SUB_BATCH])
            sh.subscribed.update(to_sub[i:i + SUB_BATCH])
            await asyncio.sleep(0.2)
        if to_sub or to_unsub:
            logger.info(f"[hub] K线订阅对账#{sh.idx}: +{len(to_sub)} -{len(to_unsub)}，当前 {len(sh.subscribed)} 个 stream")
        now = self._wall()
        todo = []
        with self._kl_lock:
            for key, st in self._series.items():
                if (st.shard == sh.idx and st.rows is None and not st.loading and now >= st.retry_at
                        and f"{key[0].lower()}@kline_{key[1]}" in sh.subscribed):
                    st.loading = True
                    todo.append(key)
            gen = sh.gen
        for key in todo:
            t = asyncio.ensure_future(self._load_history(key, gen))
            self._load_tasks.add(t)
            t.add_done_callback(self._load_tasks.discard)

    def _fetch(self, symbol: str, interval: str, limit: int):
        fn = self._kline_fetcher
        if fn is None:
            from shared import binance_api
            fn = binance_api.get_klines
        return fn(symbol, interval, limit)

    async def _load_history(self, key: tuple, gen: int) -> None:
        sym, iv = key
        rows = None
        try:
            async with self._fetch_sem:
                rows = await asyncio.get_running_loop().run_in_executor(_LOAD_POOL, self._fetch, sym, iv, KLINE_HISTORY)
        except Exception as e:
            # 日志丢线程池：避免 RotatingFileHandler 文件 I/O 阻塞 hub 事件循环
            asyncio.get_running_loop().run_in_executor(None, logger.warning, f"[hub] 历史K线拉取失败 {sym} {iv}: {e}")
        _empty = False
        with self._kl_lock:
            st = self._series.get(key)
            if st is None or gen != self._shards[min(st.shard, self._nshards - 1)].gen:
                return                               # 标的已被移除，或期间发生过断线：结果作废
            st.loading = False
            if not isinstance(rows, list) or not rows:
                st.retry_at = self._wall() + KLINE_RETRY_SEC
                _empty = True                        # 锁内只记标志，日志在锁外丢线程池
            else:
                new = [list(r) for r in rows]
                iv_ms = _INTERVAL_MS[iv]
                for pr in st.pending:                # 合并拉取期间收到的推送
                    _upsert(new, pr, iv_ms)
                st.rows, st.pending = new, []
        _loop = asyncio.get_running_loop()
        if _empty:
            _loop.run_in_executor(None, logger.warning, f"[hub] 历史K线拉取返回空 {sym} {iv}，30s后重试")
            return
        _loop.run_in_executor(None, logger.info, f"[hub] 历史K线就绪 {sym} {iv} ({len(new)}根)")


def _fmt(v) -> str:
    return "n/a" if v is None else f"{v:.1f}"


# ── 模块级便捷接口（读取 start() 注册的默认实例；未启动时返回空） ──────────────
_default: Optional[MarketDataHub] = None


def get_hub() -> Optional[MarketDataHub]:
    return _default


def start() -> None:
    global _default
    if _default is None:
        _default = MarketDataHub()
    _default.start()


def stop() -> None:
    if _default is not None:
        _default.stop()


def is_ready() -> bool:
    return _default is not None and _default.is_ready()


def get_mark_price(symbol: str) -> Optional[dict]:
    return _default.get_mark_price(symbol) if _default is not None else None


def get_all_mark_prices() -> dict:
    return _default.get_all_mark_prices() if _default is not None else {}


def subscribe_klines(symbols) -> None:
    if _default is not None:
        _default.subscribe_klines(symbols)


def get_klines(symbol: str, interval: str, limit: int) -> Optional[list]:
    return _default.get_klines(symbol, interval, limit) if _default is not None else None


def is_ticker_ready() -> bool:
    return _default is not None and _default.is_ticker_ready()


def get_ticker(symbol: str) -> Optional[dict]:
    return _default.get_ticker(symbol) if _default is not None else None


def get_all_tickers() -> dict:
    return _default.get_all_tickers() if _default is not None else {}


def ticker_coverage() -> tuple:
    return _default.ticker_coverage() if _default is not None else (0, 0)
