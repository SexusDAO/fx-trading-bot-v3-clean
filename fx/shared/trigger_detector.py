"""阶段3.1：影子触发器（只观测，不下单；默认关闭 config.TRIGGER_DETECTOR_ENABLED=False）。

运行方式：main 事件循环里的 asyncio task（TriggerDetector.run），每秒轮询 market_data_hub 内存。
- 收盘判断：K 线最后一根 open_time 变化；指标（EMA/RSI/ATR/高低点/结构位）只在收盘时重算并缓存，
  每秒只做「当前价 vs 缓存区间」比较；pullback / range_bounce / ema_trend 只在收盘帧运行。
- 当前价统一用 miniTicker.lastPrice；markPrice 只用于资金费率 / nextFundingTime / exchange_ts。
- 标的分两类：hub 有 K 线（15m >= 50 根）→ 8 种策略；无 K 线 → 只跑 reversal(无 RSI)/funding_arb/oi_divergence(无 ATR)。
  有 K 线但未就绪（根数/新鲜度/缺口）或全局门控未开 → 同样只跑这 3 种。
- 冷却键 symbol+strategy+direction（stop_hunt 额外带结构位 1H K 线 open_time）；状态每 60 秒落盘 logs/trigger_state.json。
- 触发经持仓/挂起信号/分析中过滤，再经令牌桶；trigger_log.jsonl 由后台线程写入；post_price 由后台协程追加新行回填。
- 对账：scan_signals.jsonl 有信号但触发器窗口内同标的无触发 → [trigger-miss-type1]；反之 → [trigger-fp]。
外部 REST：只有 exchangeInfo（启动一次 + 每 6 小时，放在线程里执行）。
"""
import asyncio
import hashlib
import json
import logging
import math
import os
import queue
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger("fx.trigger")

_FX = Path(__file__).parent.parent
TRIGGER_LOG = _FX / "logs" / "trigger_log.jsonl"
SCAN_SIGNALS_LOG = _FX / "logs" / "scan_signals.jsonl"
STATE_FILE = _FX / "logs" / "trigger_state.json"
POOL_FILE = _FX / "logs" / "candidate_pool.json"
LOG_WARN_BYTES = 10 * 1024 * 1024

IV_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}
READY_BARS = {"15m": 100, "1h": 99, "4h": 60}   # 1h 99：hub 只保留 100 根
KLINE_MIN_15M = 50
GATE_RATIO = 0.8
GATE_SEC = 120
FUNDING_ARB_INTERVAL = 300
POS_REFRESH_SEC = 30
STATE_SAVE_SEC = 60
EXINFO_REFRESH_SEC = 6 * 3600
ANALYZING_TTL_SEC = 600
STARTUP_TOKEN_MAX = 30
YIELD_EVERY = 10
RECONCILE_EXCLUDE = {"funding_arb"}
NO_KLINE_STRATS = ("reversal", "funding_arb", "oi_divergence")
POST_HORIZONS = (("15m", 900), ("1h", 3600), ("4h", 14400))

_COOL = {
    "pullback": ("TRIGGER_COOL_PULLBACK", 900), "reversal": ("TRIGGER_COOL_REVERSAL", 900),
    "breakout": ("TRIGGER_COOL_BREAKOUT", 1800), "funding_arb": ("TRIGGER_COOL_FUNDING_ARB", 3600),
    "range_bounce": ("TRIGGER_COOL_RANGE_BOUNCE", 3600), "stop_hunt": ("TRIGGER_COOL_STOP_HUNT", 900),
    "oi_divergence": ("TRIGGER_COOL_OI_DIV", 1200), "ema_trend": ("TRIGGER_COOL_EMA_TREND", 14400),
}


def _cfg(name, default):
    try:
        import config
        return getattr(config, name, default)
    except Exception:
        return default


def cooldown(strategy: str) -> float:
    k, d = _COOL[strategy]
    return float(_cfg(k, d))


def _config_version() -> str:
    try:
        h = hashlib.sha1((_FX / "config.py").read_bytes()).hexdigest()[:8]
    except Exception:
        h = "unknown"
    return f"{_cfg('TRIGGER_CONFIG_VERSION', 'phase3')}+{h}"


def _ema(c, n):
    from shared.indicators import ema
    return ema(c, n)


def _rsi(c):
    from shared.indicators import rsi
    return rsi(c)


def _atr(k):
    from shared.indicators import atr
    return atr(k)


# ── 默认数据源（测试里全部可注入）────────────────────────────────────────────
def _load_oi_snapshot() -> dict:
    """一次性读取 oi_cache.json（同步磁盘 IO，由 apoll 每轮经 to_thread 调用一次）。"""
    try:
        from shared.state import _load_oi_cache
        return _load_oi_cache() or {}
    except Exception:
        return {}


def _default_oi(symbol, snapshot=None):
    """oi_cache.json → (oi_now, oi_1h_ago, oi_4h_ago)；无效为 0。
    snapshot 不为 None 时只读内存快照，不访问磁盘。"""
    try:
        if snapshot is None:
            snapshot = _load_oi_snapshot()
        s = snapshot.get(symbol) or []
        if len(s) < 2:
            return 0.0, 0.0, 0.0
        now_ms = int(time.time() * 1000)
        last = max(s, key=lambda r: r["timestamp"])
        if now_ms - last["timestamp"] > 3600 * 1000:
            return 0.0, 0.0, 0.0

        def at(sec, tol):
            tgt = now_ms - sec * 1000
            r = min(s, key=lambda x: abs(x["timestamp"] - tgt))
            return float(r["oi"]) if abs(r["timestamp"] - tgt) <= tol * 1000 else 0.0
        return float(last["oi"]), at(3600, 900), at(14400, 1800)
    except Exception:
        return 0.0, 0.0, 0.0


def _default_pool():
    try:
        return list(json.loads(POOL_FILE.read_text(encoding="utf-8")).get("pool", []))
    except Exception:
        return []


def _default_exinfo():
    """REST exchangeInfo → 可交易永续合约集合；失败返回 None（调用方保留旧值）。"""
    try:
        from shared.binance_api import get_exchange_info
        d = get_exchange_info() or {}
        syms = d.get("symbols") or []
        if not syms:
            return None
        return {s["symbol"] for s in syms
                if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"}
    except Exception:
        return None


def _default_positions():
    from shared.state import get_all_positions
    return get_all_positions()


# ── 后台写文件 ───────────────────────────────────────────────────────────────
class _Writer:
    def __init__(self):
        self.q = queue.Queue()
        self._warned = set()
        threading.Thread(target=self._run, name="trigger-writer", daemon=True).start()

    def put(self, path, obj):
        self.q.put((Path(path), obj))

    def flush(self):
        self.q.join()

    def _run(self):
        while True:
            path, obj = self.q.get()
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                if path not in self._warned and os.path.getsize(path) > LOG_WARN_BYTES:
                    self._warned.add(path)
                    logger.warning(f"[trigger] {path.name} 超过 10MB（不轮转）")
            except Exception as e:
                logger.debug(f"[trigger] 写 {path.name} 失败: {e}")
            finally:
                self.q.task_done()


class _St:
    __slots__ = ("triggered", "last_trigger_ts", "last_value", "inner_triggered", "last_price")

    def __init__(self, v=None):
        v = v or [False, 0.0, 0.0, False, 0.0]
        self.triggered, self.last_trigger_ts, self.last_value, self.inner_triggered, self.last_price = \
            bool(v[0]), float(v[1]), float(v[2]), bool(v[3]), float(v[4])

    def dump(self):
        return [self.triggered, self.last_trigger_ts, self.last_value, self.inner_triggered, self.last_price]


class TriggerDetector:
    def __init__(self, hub=None, clock: Callable[[], float] = time.time,
                 oi_reader: Callable = _default_oi, pool_reader: Callable = _default_pool,
                 exinfo_reader: Callable = _default_exinfo, positions_reader: Callable = _default_positions,
                 signal_watcher=None, trigger_log: Path = TRIGGER_LOG, scan_log: Path = SCAN_SIGNALS_LOG,
                 state_file: Path = STATE_FILE, analyzer: Optional[Callable] = None):
        self._hub = hub
        self._clock = clock
        self._oi = oi_reader
        self._oi_snapshot: dict = {}   # 每轮 apoll 开头刷新一次的 OI 内存快照
        self._pool = pool_reader
        self._exinfo = exinfo_reader
        self._positions = positions_reader
        self._watcher = signal_watcher
        self._tlog, self._slog, self._sfile = Path(trigger_log), Path(scan_log), Path(state_file)
        self._writer = _Writer()
        self._ver = _config_version()
        now = clock()
        self._start_ts = now
        self._st = {}            # (sym, strategy, dir_key) -> _St
        self._seen = set()       # (sym, strategy) 本次运行已评估过
        self._loaded = False
        self._open = {}          # (sym, iv) -> 最后一根 open_time
        self._closed = {}        # (sym, iv) -> 本轮检测到收盘
        self._ready = {}         # (sym, iv) -> (根数, 无缺口)
        self._ind = {}           # sym -> 缓存指标
        self._fund = {}          # sym -> {"nft": int, "period": int}
        self._last_arb = -math.inf
        self._tradable = None
        self._exinfo_ts = -math.inf
        self._pos_cache = []
        self._pos_ts = -math.inf
        self._analyzing = {}     # sym -> ts
        self._lock = threading.Lock()
        cap = float(_cfg("TRIGGER_TOKEN_CAPACITY", 60))
        self._tokens, self._tok_ts = cap, now
        self._queue = []
        self._strat_hist = []    # (ts, strategy) 已放行
        self._startup_used = 0
        self._scan_pos = 0
        self._triggers = []      # (ts, sym, strategy)
        self._pending_scan = []  # (ts, set(sym))
        self._post = []
        self.stats = {"miss": 0, "fp": 0, "oi_miss": 0, "oi_fp": 0, "suppressed": 0, "expired": 0}
        self.gate_open = False
        self._analyzer = analyzer          # 测试注入；默认 scanner.dispatch_trigger_analysis
        self._dispatch_q: Optional[asyncio.Queue] = None
        self._dispatch_tasks = []
        self.dispatch_stats = {"queued": 0, "signal": 0, "rejected": 0, "error": 0, "skipped": 0}

    # ── 公共接口 ─────────────────────────────────────────────────────────
    def hub(self):
        if self._hub is not None:
            return self._hub
        try:
            from shared.market_data_hub import get_hub
            return get_hub()
        except Exception:
            return None

    def is_cooling_down(self, symbol, strategy, direction) -> bool:
        now, cd = self._clock(), cooldown(strategy)
        for (s, st_, d), st in list(self._st.items()):
            if s == symbol and st_ == strategy and (d == direction or d.startswith(direction + "|")):
                if now - st.last_trigger_ts < cd:
                    return True
        return False

    def mark_analyzing(self, symbol):
        with self._lock:
            self._analyzing[symbol] = self._clock()

    def clear_analyzing(self, symbol=None):
        with self._lock:
            if symbol is None:
                self._analyzing.clear()
            else:
                self._analyzing.pop(symbol, None)

    def is_analyzing(self, symbol) -> bool:
        with self._lock:
            ts = self._analyzing.get(symbol)
        return ts is not None and self._clock() - ts < ANALYZING_TTL_SEC

    def flush(self):
        self._writer.flush()

    # ── 持久化 ───────────────────────────────────────────────────────────
    def save_state(self):
        try:
            d = {"ts": self._clock(), "st": [[list(k), v.dump()] for k, v in list(self._st.items())],
                 "fund": self._fund}
            self._sfile.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._sfile.with_suffix(".tmp")
            tmp.write_text(json.dumps(d), encoding="utf-8")
            os.replace(tmp, self._sfile)
        except Exception as e:
            logger.debug(f"[trigger] 保存状态失败: {e}")

    _save_state = save_state

    def load_state(self) -> bool:
        try:
            d = json.loads(self._sfile.read_text(encoding="utf-8"))
            self._st = {tuple(k): _St(v) for k, v in d.get("st", [])}
            self._fund = {k: dict(v) for k, v in (d.get("fund") or {}).items()}
            self._loaded = True
            logger.info(f"[trigger] 已恢复状态 {len(self._st)} 条")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"[trigger] 状态文件无法解析，忽略: {e}")
        return self._loaded

    _load_state = load_state

    # ── 主循环 ───────────────────────────────────────────────────────────
    async def run(self):
        self.load_state()
        post = asyncio.create_task(self._post_loop(), name="trigger_post_price")
        self.start_dispatch_workers()
        last_save = self._clock()
        try:
            while True:
                try:
                    await self.apoll()
                except Exception as e:
                    logger.debug(f"[trigger] poll 异常: {e}")
                if self._clock() - last_save >= STATE_SAVE_SEC:
                    last_save = self._clock()
                    await asyncio.to_thread(self.save_state)
                await asyncio.sleep(1.0)
        finally:
            post.cancel()
            for t in self._dispatch_tasks:
                t.cancel()
            self.save_state()

    async def _post_loop(self):
        while True:
            await asyncio.sleep(5.0)
            try:
                self.update_post_prices()
            except Exception as e:
                logger.debug(f"[trigger] post_price 异常: {e}")

    def poll_once(self):
        return asyncio.run(self.apoll())

    async def apoll(self):
        hub = self.hub()
        if hub is None:
            return []
        if self._oi is _default_oi:   # 默认数据源：每轮只读一次磁盘，之后各标的从内存取
            self._oi_snapshot = await asyncio.to_thread(_load_oi_snapshot)
        now = self._clock()
        if now - self._exinfo_ts >= EXINFO_REFRESH_SEC:
            self._exinfo_ts = now
            got = await asyncio.to_thread(self._exinfo)
            if got is not None:
                self._tradable = set(got)
        if now - self._pos_ts >= POS_REFRESH_SEC:
            self._pos_ts = now
            try:
                self._pos_cache = list(await asyncio.to_thread(self._positions) or [])
            except Exception as e:
                logger.debug(f"[trigger] 读持仓失败: {e}")
        if self._tradable is None:
            return []
        syms = set(self._pool() or [])
        try:
            with hub._kl_lock:
                syms |= {s for (s, iv) in hub._series.keys() if iv == "1h"}
        except Exception:
            pass
        syms = sorted(syms)

        info = {}
        for i, sym in enumerate(syms):
            info[sym] = self._refresh_klines(hub, sym, now)
            if (i + 1) % YIELD_EVERY == 0:
                await asyncio.sleep(0)
        kl = [s for s, v in info.items() if v[0]]
        ratio = (sum(1 for s in kl if info[s][1]) / len(kl)) if kl else 0.0
        self.gate_open = ratio >= GATE_RATIO or now - self._start_ts > GATE_SEC

        arb_due = now - self._last_arb >= FUNDING_ARB_INTERVAL
        out = []
        for i, sym in enumerate(syms):
            if sym in self._tradable:
                try:
                    has_k, ready = info[sym]
                    self._check_symbol(hub, sym, now, has_k and ready and self.gate_open, arb_due)  # 结果进令牌桶队列
                except Exception as e:
                    logger.debug(f"[trigger] {sym} 检查失败: {e}")
            if (i + 1) % YIELD_EVERY == 0:
                await asyncio.sleep(0)
        if arb_due:
            self._last_arb = now
        out += self._drain(now)
        self.reconcile(now)
        return out

    # ── K 线 / 就绪 / 指标缓存 ───────────────────────────────────────────
    def _refresh_klines(self, hub, sym, now):
        """返回 (有K线, 三周期全部就绪)；检测收盘并在收盘时重算缓存指标。"""
        now_ms = now * 1000
        ready_all = True
        for iv in ("15m", "1h", "4h"):
            self._closed[(sym, iv)] = False
            last = hub.get_klines(sym, iv, 1)
            if not last:
                self._ready.pop((sym, iv), None)
                ready_all = False
                continue
            ot = last[-1][0]
            prev = self._open.get((sym, iv))
            if prev != ot:
                rows = None
                for n in (100, READY_BARS[iv], KLINE_MIN_15M if iv == "15m" else None):
                    if n and rows is None:
                        rows = hub.get_klines(sym, iv, n)
                if rows:
                    step = IV_MS[iv]
                    no_gap = all(rows[j + 1][0] - rows[j][0] == step for j in range(len(rows) - 1))
                    self._ready[(sym, iv)] = (len(rows), no_gap)
                    self._recompute(sym, iv, rows)
                    self._open[(sym, iv)] = ot
                    self._closed[(sym, iv)] = prev is not None
                else:
                    self._ready.pop((sym, iv), None)
            cnt, no_gap = self._ready.get((sym, iv), (0, False))
            fresh = now_ms - float(last[-1][6]) <= 2 * IV_MS[iv]
            ind = self._ind.setdefault(sym, {})
            if iv == "15m":
                ind["k15_cur"] = last[-1]
            if iv == "1h":
                ind["last_close_time"] = int(last[-1][6])
            if not (cnt >= READY_BARS[iv] and no_gap and fresh):
                ready_all = False
        has_k = self._ready.get((sym, "15m"), (0, False))[0] >= KLINE_MIN_15M
        return has_k, ready_all and has_k

    def _recompute(self, sym, iv, rows):
        ind = self._ind.setdefault(sym, {})
        closed = rows[:-1]
        if iv == "1h":
            c = [float(k[4]) for k in closed]
            if len(c) < 21:
                ind.pop("ema20_1h", None)
                return
            ind.update(
                closes_1h=c, ema20_1h=_ema(c, 20), ema50_1h=_ema(c, 50) if len(c) >= 50 else _ema(c, len(c)),
                rsi_1h=_rsi(c), atr_1h=_atr(closed), last_close_1h=c[-1],
                high_20=max(float(k[2]) for k in closed[-20:]), low_20=min(float(k[3]) for k in closed[-20:]),
            )
            ind["pivots"] = self._pivots(closed)
        elif iv == "15m":
            ind["k15c"] = closed[-26:]
            vs = closed[-26:-6]
            ind["vma15"] = (sum(float(k[5]) for k in vs) / len(vs)) if len(vs) >= 5 else 0.0
        else:
            c = [float(k[4]) for k in closed]
            if len(c) >= 50:
                ind["ema20_4h"], ind["ema50_4h"] = _ema(c, 20), _ema(c, 50)

    @staticmethod
    def _pivots(closed):
        """B 层：近 20 根已收盘 1H 收盘价的全局极值 + 左右各 3 根确认 + 之后至少 4 根未触碰(0.3%)。"""
        out = {"low": None, "low_ot": None, "high": None, "high_ot": None}
        if len(closed) < 24:
            return out
        tail = closed[-20:]
        r = [float(k[4]) for k in tail]
        mn, mx = min(r), max(r)
        i = r.index(mn)
        if 3 <= i <= len(r) - 4 and all(r[i - j] > mn and r[i + j] > mn for j in (1, 2, 3)):
            if sum(1 for v in r[i + 1:] if v > mn * 1.003) >= 4:
                out["low"], out["low_ot"] = mn, int(tail[i][0])
        i = r.index(mx)
        if 3 <= i <= len(r) - 4 and all(r[i - j] < mx and r[i + j] < mx for j in (1, 2, 3)):
            if sum(1 for v in r[i + 1:] if v < mx * 0.997) >= 4:
                out["high"], out["high_ot"] = mx, int(tail[i][0])
        return out

    # ── 单标的 ───────────────────────────────────────────────────────────
    def _check_symbol(self, hub, sym, now, kmode, arb_due):
        tk = hub.get_ticker(sym)
        if not tk or tk.get("lastPrice") is None:
            return []
        price = float(tk["lastPrice"])
        if price <= 0:
            return []
        mp = hub.get_mark_price(sym) or {}
        funding = float(mp.get("lastFundingRate") or 0)
        chg = float(tk.get("priceChangePercent") or 0) / 100.0
        ind = self._ind.get(sym, {})
        if kmode and "ema20_1h" not in ind:
            kmode = False
        atr = ind.get("atr_1h", 0.0) if kmode else 0.0
        ctx = {
            "price": price, "chg": chg, "funding": funding,
            "exchange_ts": int(mp["time"]) if mp.get("time") else ind.get("last_close_time"),
            "freshness_ms": int(float(tk.get("age_sec") or 0) * 1000),
            "snap": {"chg24": chg * 100, "rsi_1h": ind.get("rsi_1h") if kmode else None,
                     "ema20_1h": ind.get("ema20_1h") if kmode else None,
                     "atr_1h_pct": (atr / price * 100) if kmode else None, "funding": funding,
                     "last_kline_close_time": ind.get("last_close_time")},
        }
        out = []
        self._reversal(sym, now, ctx, ind if kmode else None, out)
        self._track_funding(sym, mp)
        if arb_due:
            self._funding_arb(sym, now, ctx, mp, out)
        self._oi_div(sym, now, ctx, atr / price if kmode else None, out)
        if not kmode:
            return out
        c15, c1h, c4h = (self._closed.get((sym, iv)) for iv in ("15m", "1h", "4h"))
        self._breakout(sym, now, ctx, ind, out)
        self._stop_hunt(sym, now, ctx, ind, out)
        if c15 or (sym, "pullback") not in self._seen:
            self._pullback(sym, now, ctx, ind, out)
        if c1h or (sym, "range_bounce") not in self._seen:
            ok = self._range_bounce(ind.get("closes_1h", []), price, atr / price)
            self._run(sym, "range_bounce", now, ctx, out, [("LONG", ok, "peak→range_lower")], edge=False)
        if "ema20_4h" in ind:
            if c4h or (sym, "ema_trend") not in self._seen:
                self._ema_trend(sym, now, ctx, ind, out)
            elif c1h:
                self._ema_inner(sym, now, ctx, ind, out)
        return out

    def _run(self, sym, strat, now, ctx, out, conds, edge=True):
        """conds: [(dir_key, cond, reason)]；同一 (sym,strat) 首次评估只初始化（重启恢复时按 startup_existing 触发）。"""
        first = (sym, strat) not in self._seen
        for dkey, cond, reason in conds:
            key = (sym, strat, dkey)
            st = self._st.get(key)
            if st is None:
                st = self._st[key] = _St()
            was, st.triggered = st.triggered, bool(cond)
            if not cond:
                continue
            cooling = now - st.last_trigger_ts < cooldown(strat)
            if first:
                if not self._loaded or was or cooling:
                    continue
                startup = True
            else:
                if (edge and was) or cooling:
                    continue
                startup = False
            direction = dkey.split("|")[0]
            if self._suppressed(sym, strat, direction):
                continue
            st.last_trigger_ts, st.last_price, st.inner_triggered = now, ctx["price"], False
            out.append(self._request(sym, strat, direction, reason, ctx, now, startup))
        self._seen.add((sym, strat))

    # ── 8 种策略 ─────────────────────────────────────────────────────────
    def _reversal(self, sym, now, ctx, ind, out):
        on, off = _cfg("TRIGGER_REVERSAL_CHG24_ON", 0.10), _cfg("TRIGGER_REVERSAL_CHG24_OFF", 0.08)
        f_on, f_off = _cfg("TRIGGER_REVERSAL_FUNDING_THRESH", 0.0005), _cfg("TRIGGER_REVERSAL_FUNDING_OFF", 0.0004)
        lo_on, lo_off = _cfg("TRIGGER_REVERSAL_RSI_LOW_ON", 32), _cfg("TRIGGER_REVERSAL_RSI_LOW_OFF", 36)
        hi_on, hi_off = _cfg("TRIGGER_REVERSAL_RSI_HIGH_ON", 68), _cfg("TRIGGER_REVERSAL_RSI_HIGH_OFF", 64)
        chg, fr = ctx["chg"], ctx["funding"]
        rsi = ind.get("rsi_1h") if ind else None
        conds = []
        for d, sg in (("LONG", -1), ("SHORT", 1)):
            lk = (sym, "reversal_latch", d)
            latch = self._st.get(lk) or _St()
            self._st[lk] = latch
            m = int(latch.last_value)
            x = sg * chg                      # LONG 看跌幅，SHORT 看涨幅
            m = (m | 1) if x > on else (m & ~1 if x < off else m)
            xf = sg * fr
            m = (m | 4) if xf > f_on else (m & ~4 if xf < f_off else m)
            if rsi is None:
                m &= ~2
            elif d == "LONG":
                m = (m | 2) if rsi < lo_on else (m & ~2 if rsi > lo_off else m)
            else:
                m = (m | 2) if rsi > hi_on else (m & ~2 if rsi < hi_off else m)
            latch.last_value = float(m)
            parts = [n for b, n in ((1, "chg24"), (2, "rsi"), (4, "funding")) if m & b]
            conds.append((d, m != 0, f"{'+'.join(parts)} chg24={chg*100:.2f}% fr={fr:.5f}"
                          + (f" rsi={rsi:.1f}" if rsi is not None else "")))
        self._run(sym, "reversal", now, ctx, out, conds)

    def _track_funding(self, sym, mp):
        nft = int(mp.get("nextFundingTime") or 0)
        if not nft:
            return
        f = self._fund.setdefault(sym, {})
        old = int(f.get("nft") or 0)
        if old and nft > old:
            f["period"] = nft - old
        f["nft"] = nft

    def _funding_arb(self, sym, now, ctx, mp, out):
        fr, f = ctx["funding"], self._fund.get(sym) or {}
        nft = int(f.get("nft") or 0)
        period_h = (f.get("period") or 8 * 3_600_000) / 3_600_000
        window = 3 * 3600 if period_h >= 6 else (1.5 * 3600 if period_h >= 2 else 1800)
        remain = nft / 1000 - now if nft else -1
        ok = 0 < remain <= window and abs(fr) > _cfg("TRIGGER_FUNDING_THRESH", 0.0005)
        r = f"fr={fr:.5f} period={period_h:.0f}h remain={remain/60:.0f}m"
        self._run(sym, "funding_arb", now, ctx, out,
                  [("LONG", ok and fr < 0, r), ("SHORT", ok and fr > 0, r)], edge=False)

    def _oi_div(self, sym, now, ctx, atr_pct, out):
        _oi_v = _default_oi(sym, self._oi_snapshot) if self._oi is _default_oi else self._oi(sym)
        oi_now, oi_1h, oi_4h = (tuple(_oi_v) + (0.0, 0.0, 0.0))[:3]
        hub = self.hub()
        tk = hub.get_ticker(sym) if hub else None
        hi24 = float((tk or {}).get("highPrice") or 0)
        lo24 = float((tk or {}).get("lowPrice") or 0)
        price = ctx["price"]
        ch1 = (oi_now - oi_1h) / oi_1h if oi_now > 0 and oi_1h > 0 else 0.0
        ch4 = (oi_now - oi_4h) / oi_4h if oi_now > 0 and oi_4h > 0 else 0.0
        if abs(ch1) >= _cfg("TRIGGER_OI_DIV_CHANGE_1H", 0.03):
            sign = ch1
        elif abs(ch4) >= _cfg("TRIGGER_OI_DIV_CHANGE_4H", 0.05):
            sign = ch4
        else:
            sign = 0.0
        atr_ok = atr_pct is None or atr_pct >= _cfg("TRIGGER_OI_DIV_ATR_MIN", 0.005)
        dist, dmax = _cfg("TRIGGER_OI_DIV_PRICE_DIST", 0.035), _cfg("TRIGGER_OI_DIV_PRICE_DIST_MAX", 0.05)

        def near(d, key):
            st = self._st.get((sym, "oi_divergence", key))
            lim = dmax if (st and st.triggered) else dist   # 进入 3.5% 触发，离开 5% 才复位
            return d <= lim

        ok = hi24 > 0 and lo24 > 0 and atr_ok and sign != 0
        dh = (hi24 - price) / hi24 if hi24 > 0 else 1.0
        dl = (price - lo24) / lo24 if lo24 > 0 else 1.0
        r = f"oi_src=cache oi1h={ch1*100:+.1f}% oi4h={ch4*100:+.1f}%"
        self._run(sym, "oi_divergence", now, ctx, out, [
            ("SHORT", ok and sign < 0 and near(dh, "SHORT"), r + f" near_high={dh*100:.2f}%"),
            ("LONG", ok and sign > 0 and near(dl, "LONG"), r + f" near_low={dl*100:.2f}%"),
        ])

    def _breakout(self, sym, now, ctx, ind, out):
        b, k = _cfg("TRIGGER_BREAKOUT_CHG24", 0.03), _cfg("TRIGGER_BREAKOUT_ATR_DIST", 0.3)
        p, chg, atr = ctx["price"], ctx["chg"], ind["atr_1h"]
        h20, l20, lc = ind["high_20"], ind["low_20"], ind["last_close_1h"]
        up = chg > b and (lc >= h20 or p >= h20 - k * atr)
        dn = chg < -b and (lc <= l20 or p <= l20 + k * atr)
        r = f"h20={h20:.6g} l20={l20:.6g} close={lc:.6g}"
        self._run(sym, "breakout", now, ctx, out, [("LONG", up, r), ("SHORT", dn, r)])

    def _stop_hunt(self, sym, now, ctx, ind, out):
        pv, vma = ind.get("pivots") or {}, ind.get("vma15", 0.0)
        bars = list(ind.get("k15c", [])[-6:])
        if ind.get("k15_cur"):
            bars.append(ind["k15_cur"])          # 15M 实时帧（未收盘）
        atr = ind["atr_1h"]
        vr_min = _cfg("TRIGGER_STOP_HUNT_VOL_RATIO", 1.2)
        ka, kp = _cfg("TRIGGER_STOP_HUNT_PIERCE_ATR", 0.1), _cfg("TRIGGER_STOP_HUNT_PIERCE_PCT", 0.0015)
        conds = []
        for d, side in (("LONG", "low"), ("SHORT", "high")):
            piv, ot = pv.get(side), pv.get(side + "_ot")
            ok = False
            if piv and vma > 0:
                th = max(ka * atr, kp * piv)
                for bar in bars:
                    if float(bar[5]) / vma < vr_min:
                        continue
                    if (d == "LONG" and float(bar[3]) <= piv - th) or (d == "SHORT" and float(bar[2]) >= piv + th):
                        ok = True
                        break
            conds.append((f"{d}|{ot}", ok, f"pivot_{side}={piv} pivot_ot={ot}"))
        self._run(sym, "stop_hunt", now, ctx, out, conds)

    def _pullback(self, sym, now, ctx, ind, out):
        m = _cfg("TRIGGER_PULLBACK_ATR_MULT", 1.5)
        e20, e50, atr, p = ind["ema20_1h"], ind["ema50_1h"], ind["atr_1h"], ctx["price"]
        if e20 >= e50:
            lo, hi, d = e20 - m * atr, e20 + 0.3 * atr, "LONG"
        else:
            lo, hi, d = e20 - 0.3 * atr, e20 + m * atr, "SHORT"
        ok = lo <= p <= hi
        r = f"zone[{lo:.6g},{hi:.6g}]"
        self._run(sym, "pullback", now, ctx, out,
                  [("LONG", ok and d == "LONG", r), ("SHORT", ok and d == "SHORT", r)], edge=False)

    def _range_bounce(self, closes, price, atr_pct):
        n = int(_cfg("TRIGGER_RANGE_BOUNCE_LOOKBACK", 24))
        if len(closes) < n + 2 or atr_pct < _cfg("TRIGGER_RANGE_BOUNCE_ATR_MIN", 0.015):
            return False
        rec = closes[-n:]
        pv = max(rec)
        pa = len(closes) - n + rec.index(pv)
        base = closes[max(0, pa - 6)]
        gain = (pv - base) / (base + 1e-12)
        if gain < _cfg("TRIGGER_RANGE_BOUNCE_GAIN", 0.10):
            return False
        post = closes[pa:]
        if len(post) < 2:
            return False
        ch, cl = max(post), min(post)
        if (ch - cl) / (cl + 1e-12) > gain * _cfg("TRIGGER_RANGE_BOUNCE_ZONE", 0.60):
            return False
        return price <= (ch + cl) / 2

    def _ema_conds(self, ind, ctx):
        e20, e50, p = ind["ema20_4h"], ind["ema50_4h"], ctx["price"]
        th = _cfg("TRIGGER_EMA_TREND_EMA_THRESH", 0.995)
        rsi, chg = ind.get("rsi_1h", 50.0), ctx["chg"] * 100
        pb = (p - e20) / e20
        chg_ok = -8.0 <= chg <= 12.0
        lg = e20 >= e50 * th and 45 <= rsi <= 70 and chg_ok and -0.025 <= pb <= 0.03
        sh = e20 <= e50 * (2 - th) and 45 <= rsi <= 72 and chg_ok and -0.03 <= pb <= 0.025
        r = f"ema20_4h={e20:.6g} ema50_4h={e50:.6g} pb={pb*100:+.2f}%"
        return lg, sh, r

    def _ema_trend(self, sym, now, ctx, ind, out):
        lg, sh, r = self._ema_conds(ind, ctx)
        self._run(sym, "ema_trend", now, ctx, out, [("LONG", lg, r), ("SHORT", sh, r)], edge=False)

    def _ema_inner(self, sym, now, ctx, ind, out):
        """1H 收盘：冷却期内、上次在外圈(1.5%~4%)、现在进入内圈(<=1%) → 每个冷却周期最多再触发一次。"""
        e20, p = ind["ema20_4h"], ctx["price"]
        th = _cfg("TRIGGER_EMA_TREND_EMA_THRESH", 0.995)
        e50 = ind["ema50_4h"]
        for d, trend in (("LONG", e20 >= e50 * th), ("SHORT", e20 <= e50 * (2 - th))):
            st = self._st.get((sym, "ema_trend", d))
            if not st or not trend or st.inner_triggered or st.last_price <= 0:
                continue
            if now - st.last_trigger_ts >= cooldown("ema_trend"):
                continue
            last_d, cur_d = abs(st.last_price - e20) / e20, abs(p - e20) / e20
            if 0.015 <= last_d <= 0.04 and cur_d <= 0.01 and not self._suppressed(sym, "ema_trend", d):
                st.inner_triggered = True
                out.append(self._request(sym, "ema_trend", d,
                                         f"inner_retrigger outer={last_d*100:.2f}% inner={cur_d*100:.2f}%",
                                         ctx, now, False))

    # ── 过滤 / 令牌桶 / 输出 ─────────────────────────────────────────────
    def _suppressed(self, sym, strat, direction) -> bool:
        # [v2-5] 与 signal_watcher 共用 btc_guard 状态
        try:
            from shared import btc_guard as _bg
            if not _bg.is_allowed(direction, strat):
                return True
        except Exception:
            pass
        code = None
        if self.is_analyzing(sym):
            code = "suppressed:analyzing"
        else:
            for p in self._pos_cache:
                if p.get("symbol") != sym or p.get("status") not in (None, "open", "pending"):
                    continue
                pd = str(p.get("direction", "")).upper()
                code = "suppressed:same_dir" if pd == direction else "suppressed:hedge_risk"
                if pd == direction:
                    break
            if code is None and self._watcher is not None:
                try:
                    if sym in set(self._watcher.pending_symbols()):
                        code = "suppressed:pending_signal"
                except Exception:
                    pass
        if code:
            self.stats["suppressed"] += 1
            logger.info(f"[trigger-suppressed] {sym} {strat} {direction} {code}")
            return True
        return False

    def _request(self, sym, strat, direction, reason, ctx, now, startup):
        rec = {
            "trigger_id": str(uuid.uuid4()), "ts": int(now * 1000), "exchange_ts": ctx.get("exchange_ts"),
            "symbol": sym, "strategy": strat, "direction": direction, "reason": reason, "price": ctx["price"],
            "config_version": self._ver, "startup_existing": bool(startup), "suppressed": None,
            "token_result": None, "data_freshness_ms": ctx.get("freshness_ms"),
            "hub_snapshot": dict(ctx["snap"]),
            "post_price": {"15m": None, "1h": None, "4h": None, "max_favorable": None, "max_adverse": None},
        }
        self._queue.append({"rec": rec, "ts": now, "startup": startup})
        return rec

    def _drain(self, now):
        """令牌桶：每分钟 RATE 个，容量 CAPACITY；单策略 TTL 窗口内最多 CAPACITY×40%；
        startup_existing 排最后且最多用 30 个；排队超 TTL 记 expired。返回本轮放行的记录。"""
        rate, cap = float(_cfg("TRIGGER_TOKEN_RATE", 20)), float(_cfg("TRIGGER_TOKEN_CAPACITY", 60))
        ttl, pct = float(_cfg("TRIGGER_TOKEN_TTL", 300)), float(_cfg("TRIGGER_TOKEN_STRATEGY_MAX_PCT", 0.40))
        self._tokens = min(cap, self._tokens + (now - self._tok_ts) * rate / 60.0)
        self._tok_ts = now
        self._strat_hist = [(t, s) for t, s in self._strat_hist if now - t < ttl]
        self._queue.sort(key=lambda q: (q["startup"], q["ts"]))
        done, keep = [], []
        for q in self._queue:
            rec = q["rec"]
            if now - q["ts"] > ttl:
                rec["token_result"] = "expired"
                self.stats["expired"] += 1
                self._writer.put(self._tlog, rec)
                continue
            used = sum(1 for _, s in self._strat_hist if s == rec["strategy"])
            if (self._tokens < 1 or used >= cap * pct
                    or (q["startup"] and self._startup_used >= STARTUP_TOKEN_MAX)):
                keep.append(q)
                continue
            self._tokens -= 1
            if q["startup"]:
                self._startup_used += 1
            self._strat_hist.append((now, rec["strategy"]))
            rec["token_result"] = "allowed" if q["ts"] == now else "queued"
            self._finalize(rec, now)
            done.append(rec)
        self._queue = keep
        return done

    def _finalize(self, rec, now):
        logger.info(f"[trigger] {rec['symbol']} {rec['strategy']} {rec['direction']} {rec['reason']} "
                    f"price={rec['price']}" + (" startup_existing" if rec["startup_existing"] else ""))
        self._writer.put(self._tlog, rec)
        self._triggers.append((now, rec["symbol"], rec["strategy"]))
        self._post.append({"id": rec["trigger_id"], "sym": rec["symbol"], "dir": rec["direction"],
                           "p0": rec["price"], "t0": now, "vals": {}, "fav": 0.0, "adv": 0.0})
        self._enqueue_dispatch(rec)

    # ── 阶段5：触发 → 真实分析 → 共用第四步注册 ───────────────────────────
    @staticmethod
    def dispatch_enabled() -> bool:
        return bool(_cfg("TRIGGER_DETECTOR_ENABLED", False) and _cfg("TRIGGER_DISPATCH_ENABLED", False))

    def start_dispatch_workers(self):
        if self._dispatch_q is None:
            self._dispatch_q = asyncio.Queue()
        if not self._dispatch_tasks:
            for i in range(int(_cfg("TRIGGER_DISPATCH_WORKERS", 2))):
                self._dispatch_tasks.append(asyncio.create_task(self._dispatch_worker(), name=f"trigger_dispatch_{i}"))

    def _enqueue_dispatch(self, rec):
        if not self.dispatch_enabled():
            return
        sym = rec["symbol"]
        if self.is_analyzing(sym):
            self._dispatch_done(rec, "skipped", None, "analyzing")
            return
        if self._watcher is not None:
            try:
                if sym in set(self._watcher.pending_symbols()):
                    self._dispatch_done(rec, "skipped", None, "pending_signal")
                    return
            except Exception:
                pass
        if self._dispatch_q is None:
            self._dispatch_q = asyncio.Queue()
        self.mark_analyzing(sym)              # 入队即占位，同标的不会并发分析
        self.dispatch_stats["queued"] += 1
        self._dispatch_q.put_nowait(rec)

    async def _dispatch_worker(self):
        while True:
            rec = await self._dispatch_q.get()
            try:
                await self._dispatch_one(rec)
            finally:
                self._dispatch_q.task_done()

    async def _dispatch_one(self, rec):
        sym = rec["symbol"]
        timeout = float(_cfg("TRIGGER_DISPATCH_TIMEOUT", 30))
        try:
            r = await asyncio.wait_for(asyncio.to_thread(self._dispatch_analysis, sym, [rec["strategy"]]), timeout)
            self._dispatch_done(rec, r.get("analysis_result", "error"), r.get("signal_strategy"), r.get("error"))
        except asyncio.TimeoutError:
            self._dispatch_done(rec, "error", None, "timeout")
        except Exception as e:
            self._dispatch_done(rec, "error", None, f"{type(e).__name__}: {e}")
        finally:
            self.clear_analyzing(sym)

    def _dispatch_analysis(self, symbol, strategy_hints=None) -> dict:
        """线程内运行：hub 快照（零 REST）→ scanner.dispatch_trigger_analysis（预取补 REST → analyze_one → 第四步）。"""
        hub = self.hub()
        if hub is None:
            return {"analysis_result": "error", "error": "no_hub"}
        kl = tuple(hub.get_klines(symbol, iv, 100) for iv in ("15m", "1h", "4h"))
        tk = hub.get_ticker(symbol)
        mp = hub.get_mark_price(symbol)
        if not all(kl) or not tk:
            return {"analysis_result": "error", "error": "hub_snapshot_incomplete"}
        fn = self._analyzer
        if fn is None:
            import scanner as _sc
            fn = _sc.dispatch_trigger_analysis
        return fn(symbol, tk, mp, kl, signal_watcher=self._watcher,
                  max_ctx_age=float(_cfg("SCAN_INTERVAL_PASSIVE", 1800)))

    def _dispatch_done(self, rec, result, strategy, err):
        key = result if result in self.dispatch_stats else "error"
        self.dispatch_stats[key] += 1
        line = {"type": "analysis", "trigger_id": rec["trigger_id"], "ts": int(self._clock() * 1000),
                "symbol": rec["symbol"], "strategy": rec["strategy"], "analysis_result": result,
                "signal_strategy": strategy, "error": err}
        self._writer.put(self._tlog, line)
        lvl = logging.WARNING if result == "error" else logging.INFO
        logger.log(lvl, f"[trigger-dispatch] {rec['symbol']} {rec['strategy']} → {result}"
                        + (f" signal={strategy}" if strategy else "") + (f" err={err}" if err else ""))

    async def drain_dispatch(self):
        if self._dispatch_q is not None:
            await self._dispatch_q.join()

    def update_post_prices(self):
        """定义2：触发后 15m/1h/4h 价格与期间最大有利/不利波动（比例，按方向）。到点追加一行，原行不改。"""
        hub, now = self.hub(), self._clock()
        if hub is None:
            return
        keep = []
        for t in self._post:
            tk = hub.get_ticker(t["sym"])
            if tk and tk.get("lastPrice") is not None:
                p = float(tk["lastPrice"])
                ret = (p - t["p0"]) / t["p0"] * (1 if t["dir"] == "LONG" else -1)
                t["fav"], t["adv"] = max(t["fav"], ret), min(t["adv"], ret)
                for h, sec in POST_HORIZONS:
                    if h not in t["vals"] and now - t["t0"] >= sec:
                        t["vals"][h] = p
                        pp = {k: t["vals"].get(k) for k, _ in POST_HORIZONS}
                        pp.update(max_favorable=round(t["fav"], 6), max_adverse=round(t["adv"], 6))
                        self._writer.put(self._tlog, {"type": "post_price", "trigger_id": t["id"],
                                                      "ts": int(now * 1000), "horizon": h, "post_price": pp})
            if len(t["vals"]) < len(POST_HORIZONS) and now - t["t0"] < POST_HORIZONS[-1][1] + 3600:
                keep.append(t)
        self._post = keep

    # ── 对账 ─────────────────────────────────────────────────────────────
    def _read_new_scan(self):
        rows = []
        try:
            if not self._slog.exists():
                return rows
            with open(self._slog, encoding="utf-8") as f:
                f.seek(self._scan_pos)
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            rows.append(json.loads(line))
                        except Exception:
                            pass
                self._scan_pos = f.tell()
        except Exception:
            pass
        return rows

    def reconcile(self, now):
        win = lambda s: max(cooldown(s) if s in _COOL else 300, 300)
        batches = {}
        for r in self._read_new_scan():
            batches.setdefault(r.get("ts", 0), [])
            if "symbol" in r:
                batches[r.get("ts", 0)].append(r)
        for ts, sigs in batches.items():
            ts_s = ts / 1000
            for s in sigs:
                st = s.get("strategy")
                if st in RECONCILE_EXCLUDE:
                    continue
                hit = any(sym == s["symbol"] and abs(t - ts_s) <= win(st) for t, sym, _ in self._triggers)
                if not hit:
                    self.stats["miss"] += 1
                    if st == "oi_divergence":
                        self.stats["oi_miss"] += 1
                    logger.info(f"[trigger-miss-type1] {s['symbol']} {st} {s.get('direction')}"
                                + (" (OI来自缓存)" if st == "oi_divergence" else ""))
            self._pending_scan.append((ts_s, {s["symbol"] for s in sigs}))
        keep = []
        for t, sym, st in self._triggers:
            w = win(st)
            if st in RECONCILE_EXCLUDE:
                if now - t < w:
                    keep.append((t, sym, st))
                continue
            if any(sym in syms and abs(ts - t) <= w for ts, syms in self._pending_scan):
                if now - t < w:
                    keep.append((t, sym, st))
                continue
            if now - t >= w:
                self.stats["fp"] += 1
                if st == "oi_divergence":
                    self.stats["oi_fp"] += 1
                tag = " (OI来自缓存)" if st == "oi_divergence" else (" (仅记录)" if st == "pullback" else "")
                logger.info(f"[trigger-fp] {sym} {st}{tag}")
            else:
                keep.append((t, sym, st))
        self._triggers = keep
        self._pending_scan = [(ts, s) for ts, s in self._pending_scan if now - ts < 14400 + 300]


# ── 模块级入口 ───────────────────────────────────────────────────────────────
_default: Optional[TriggerDetector] = None


def get_detector() -> Optional[TriggerDetector]:
    return _default


def create(**kwargs) -> Optional[TriggerDetector]:
    """开关关闭返回 None；打开时创建（不启动），由 main 以 asyncio task 运行 det.run()。"""
    global _default
    if not _cfg("TRIGGER_DETECTOR_ENABLED", False):
        return None
    if _default is None:
        _default = TriggerDetector(**kwargs)
    return _default


start = create


def mark_analyzing_many(symbols) -> None:
    d = _default
    if d is not None:
        for s in symbols:
            d.mark_analyzing(s)


def clear_analyzing_many(symbols) -> None:
    d = _default
    if d is not None:
        for s in symbols:
            d.clear_analyzing(s)


def write_scan_signals(signals: list, path: Path = SCAN_SIGNALS_LOG, ts_ms: Optional[int] = None) -> None:
    """scanner 调用：每轮 judge 通过的原始信号；无信号写 {"ts", "signals": []}。异常吞掉。"""
    try:
        ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            if not signals:
                f.write(json.dumps({"ts": ts, "signals": []}) + "\n")
            for s in signals:
                f.write(json.dumps({"ts": ts, "symbol": s.get("symbol"), "strategy": s.get("sig_type"),
                                    "direction": s.get("direction"),
                                    "route_quality": s.get("route_quality")}, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.debug(f"[trigger] 写 scan_signals 失败: {e}")
