"""
clawby_adapter.py — 双轨数据适配层（Clawby + CG中转站）

数据源优先级（2026-10 修订）:
  A. CG中转站  https://vip2.coinglass.site  (professional, 30次/分钟)
     → 核心合约接口，实时性更好，直接映射 CG 路径
  B. Clawby    https://api.openclawby.com/api/relay (1200次/分钟)
     → 次要/套利/宏观接口，以及 CG中转 不支持的接口
  C. Binance fapi 公开（无速率限制）
     → LSR / Taker / OI历史 / 资金费率

=== 接口路由表 ===

核心清算（→ CG中转 GET）:
  get_liq_heatmap()            → GET /api/futures/liquidation/heatmap/model1?symbol=XXXUSDT
  get_liq_aggregated_heatmap() → GET /api/futures/liquidation/aggregated-heatmap/model1?symbol=XXX(base)
  _get_liq_history()           → GET /api/futures/liquidation/history?symbol=XXXUSDT&interval=1h
  get_liq_orders()             → GET /api/futures/liquidation/order?symbol=XXXUSDT
  get_liq_coin_list()          → GET /api/futures/liquidation/coin-list

CVD / Coinbase 溢价（→ CG中转 GET，已验证可用）:
  get_cvd()                    → GET /api/futures/cvd/history?symbol=XXXUSDT
  get_coinbase_premium()       → GET /api/coinbase-premium-index

Binance fapi 公开（保持不变）:
  _get_top_lsr()     → fapi globalLongShortAccountRatio
  _get_taker()       → fapi takerlongshortRatio
  _get_oi_history()  → fapi openInterestHist
  _get_funding_rate() → fapi fundingRate

次要信号（→ Clawby，不那么重要，流量低）:
  get_fear_greed()      → index_fear_greed_history
  get_liq_max_pain()    → futures_liquidation_max_pain
  get_bull_market_peak() → bull_market_peak_indicator
  get_economic_calendar() → calendar_economic_data
  get_btc_etf_flow/aum() → etf_bitcoin_*
  get_hl_whale_*()       → hyperliquid_*
  get_altcoin_season()   → index_altcoin_season
  get_funding_arb_opps() → futures_funding_rate_arbitrage
  get_funding_exchange_list() → futures_funding_rate_exchange_list
  get_oi_exchange_list() → futures_open_interest_exchange_list

❌ 已停用（中转站和Clawby均不可用）:
  get_whale_index()  → futures_whale_index_history (404 on all paths)
  get_boll()         → futures_indicators_boll     (400 pair does not exist)

LSR 限速保护:
  CG中转 LSR 限额 2次/分钟。scanner 扫描 50+ 标的时不应逐个调用。
  get_lsr_signal() 内置 60s 滑动窗口缓存 + 全局令牌桶，超限返回 None（不阻塞）。
"""

import os
import time
import threading
import logging
import requests

logger = logging.getLogger("fx.clawby")

# ─────────────────────────────────────────────────────────────────────────────
# 配置
# ─────────────────────────────────────────────────────────────────────────────
# ―― Clawby（次要/套利/宏观接口）
CLAWBY_BASE_URL = "https://api.openclawby.com/api/relay"
CLAWBY_API_KEY  = os.environ.get("CLAWBY_API_KEY", "pk_xBgG9wi3q1ZnIe6peyFrBHQP_6v2hG2C")
CLAWBY_HEADERS  = {"X-API-Key": CLAWBY_API_KEY, "Content-Type": "application/json",
                   "Accept-Encoding": "identity"}  # 禁用压缩，避免urllib3 decompress持GIL阻塞事件循环

# ―― CG 中转站（核心合约接口，30次/分钟）
CG_BASE_URL  = os.environ.get("CG_BASE_URL",  "https://vip2.coinglass.site")
CG_API_KEY   = os.environ.get("CG_API_KEY",   "cg_998dad4b22ea88573763952d7b9e0944de6addfe7e5ec216")
CG_HEADERS   = {"X-API-Key": CG_API_KEY, "Accept-Encoding": "identity"}  # 禁用响应压缩
# 30次/分钟 → 理论最小间隔 2.0s；取 2.2s（约 27 次/分钟，留约 10% 余量）。此前 1.0s 实际放行 ~60 次/分钟，是 429 的直接原因
CG_RATE_LIMIT_S  = 2.2
CG_LSR_LIMIT_S   = 31.0   # LSR 专用：2次/分钟 → 31s（安全间隔）

# Clawby Pro 套餐 1200次/分钟 → 0.052s 间隔
CLAWBY_RATE_LIMIT_S = 0.052
CACHE_TTL    = 15 * 60   # 15 分钟

import os as _os
_PROXY_URL = _os.environ.get("BINANCE_PROXY", "")
_PROXIES   = ({"http": _PROXY_URL, "https": _PROXY_URL} if _PROXY_URL else {})

# ─────────────────────────────────────────────────────────────────────────────
# 健康监控（Clawby 专用）
# ─────────────────────────────────────────────────────────────────────────────
_CB_HEALTH     = {"ok": 0, "fail_404": 0, "fail_timeout": 0, "hour_start": time.time()}
_CB_HEALTH_LOCK = threading.Lock()

def _cb_record(result: str, notifier_fn=None):
    global _CB_HEALTH
    now = time.time()
    with _CB_HEALTH_LOCK:
        if now - _CB_HEALTH["hour_start"] >= 3600:
            total = _CB_HEALTH["ok"] + _CB_HEALTH["fail_404"] + _CB_HEALTH["fail_timeout"]
            if total > 0:
                rate = _CB_HEALTH["ok"] / total * 100
                msg  = (f"📊 Clawby 过去1H成功率: {rate:.0f}% "
                        f"(成功{_CB_HEALTH['ok']}/404:{_CB_HEALTH['fail_404']}/超时:{_CB_HEALTH['fail_timeout']})")
                logger.info(msg)
                if rate < 50:
                    warn = f"⚠️ Clawby 可用性下降\n{msg}\n当前开仓止损精度受影响"
                    logger.warning(warn)
                    if notifier_fn:
                        try: notifier_fn(warn)
                        except Exception: pass
            _CB_HEALTH = {"ok": 0, "fail_404": 0, "fail_timeout": 0, "hour_start": now}
        _CB_HEALTH[result] = _CB_HEALTH.get(result, 0) + 1

# ─────────────────────────────────────────────────────────────────────────────
# 速率控制（Clawby 专用）
# ─────────────────────────────────────────────────────────────────────────────
_CLAWBY_RATE_LOCK = threading.Lock()
_CLAWBY_LAST_CALL = 0.0

def _wait_clawby_rate_limit():
    global _CLAWBY_LAST_CALL
    with _CLAWBY_RATE_LOCK:
        now     = time.time()
        elapsed = now - _CLAWBY_LAST_CALL
        if elapsed < CLAWBY_RATE_LIMIT_S:
            time.sleep(CLAWBY_RATE_LIMIT_S - elapsed)
        _CLAWBY_LAST_CALL = time.time()

# ─────────────────────────────────────────────────────────────────────────────
# CG 中转站速率控制（核心接口用）30次/分钟）
# ─────────────────────────────────────────────────────────────────────────────
_CG_RATE_LOCK = threading.Lock()
_CG_LAST_CALL = 0.0

def _wait_cg_rate_limit():
    global _CG_LAST_CALL
    with _CG_RATE_LOCK:
        now     = time.time()
        elapsed = now - _CG_LAST_CALL
        if elapsed < CG_RATE_LIMIT_S:
            time.sleep(CG_RATE_LIMIT_S - elapsed)
        _CG_LAST_CALL = time.time()

# ─────────────────────────────────────────────────────────────────────────────
# CG LSR 专用令牌桶（2次/分钟，全局共享）
# scanner 扫描 50+ 标的时，每个标的都会尝试调用 LSR。
# 令牌桶保证每 30s 才消耗一个令牌，超限时返回 None（不阻塞扫描）。
# ─────────────────────────────────────────────────────────────────────────────
_LSR_TOKEN_LOCK   = threading.Lock()
_LSR_TOKENS       = 2          # 当前可用令牌数
_LSR_LAST_REFILL  = time.time()  # 上次补充时间
_LSR_REFILL_EVERY = 30.0       # 每 30s 补充 1 个令牌（安全上限 2/min）
_LSR_MAX_TOKENS   = 2

def _lsr_try_acquire() -> bool:
    """尝试获取 LSR 令牌。返回 True=可用, False=限速中（调用方应直接用缓存或跳过）"""
    global _LSR_TOKENS, _LSR_LAST_REFILL
    with _LSR_TOKEN_LOCK:
        now = time.time()
        # 按经过时间补充令牌
        elapsed_refills = int((now - _LSR_LAST_REFILL) / _LSR_REFILL_EVERY)
        if elapsed_refills > 0:
            _LSR_TOKENS = min(_LSR_MAX_TOKENS, _LSR_TOKENS + elapsed_refills)
            _LSR_LAST_REFILL += elapsed_refills * _LSR_REFILL_EVERY
        if _LSR_TOKENS > 0:
            _LSR_TOKENS -= 1
            return True
        return False

# ─────────────────────────────────────────────────────────────────────────────
# 缓存：纯内存 + 后台异步快照落盘
# ─────────────────────────────────────────────────────────────────────────────
# 旧实现每次 _cache_get(未命中)/_cache_set 都在 fcntl.flock 内整文件读写 JSON（线上文件达 39MB），
# JSON 编解码持有 GIL，事件循环被反复阻塞 2~8s。线上确认只有 main.py 一个进程使用该文件，跨进程锁没有必要。
# 现在：
#   · _cache_get 只查内存，未命中返回 None，绝不碰磁盘；
#   · _cache_set 只写内存（带 TTL 淘汰 + 最大条目数限制）；
#   · 单个后台线程：启动时读一次快照填充内存，之后每 CACHE_SNAPSHOT_INTERVAL 秒在有变更时落盘一次（临时文件 + os.replace，无 flock）；
#   · 体积大的 heatmap 原始数据不入快照（liq_cache 自带 liq_cache_snapshot.json），避免快照再次变成大 JSON。
import pathlib as _pathlib
import json    as _json_mod
import os      as _os

_WS_CACHE_DIR       = _pathlib.Path(__file__).parent.parent.parent / "ws_cache"
_SNAPSHOT_FILE      = _WS_CACHE_DIR / "clawby_mem_snapshot.json"
_LEGACY_CACHE_FILES = (_WS_CACHE_DIR / "clawby_shared_cache.json",
                       _WS_CACHE_DIR / "clawby_shared_cache.json.lock")   # 旧版跨进程缓存（启动时一次性清理）

CACHE_SNAPSHOT_INTERVAL = 60.0      # 后台落盘间隔（秒）
CACHE_MAX_ENTRIES       = 3000      # 内存缓存最大条目数
_SNAPSHOT_SKIP_PREFIXES = ("heatmap:",)   # 体积大且 liq_cache 已有自己的快照，不入本快照
_SNAPSHOT_MAX_LOAD_BYTES = 8 * 1024 * 1024  # 快照超过该大小不加载（防止异常大文件在启动时拖慢进程）

_mem_cache: dict = {}
_MEM_LOCK = threading.Lock()        # 仅保护内存 dict 的短临界区（无 IO、无 JSON）
_dirty = False
_bg_started = False
_bg_start_lock = threading.Lock()
_bg_stop = threading.Event()
_PERSIST_ENABLED = _os.environ.get("FX_CLAWBY_CACHE_PERSIST", "1") != "0"


def _evict_locked(now: float) -> None:
    """调用方须持有 _MEM_LOCK：先清过期，仍超限则按时间戳从旧到新淘汰到 90% 水位。"""
    for k in [k for k, v in _mem_cache.items() if now - v.get("ts", 0) >= CACHE_TTL]:
        del _mem_cache[k]
    if len(_mem_cache) > CACHE_MAX_ENTRIES:
        keep = int(CACHE_MAX_ENTRIES * 0.9)
        for k, _ in sorted(_mem_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(_mem_cache) - keep]:
            del _mem_cache[k]


def _cache_get(key: str):
    """只查内存；过期或未命中返回 None。不读磁盘、不加文件锁。"""
    _ensure_bg_started()
    now = time.time()
    with _MEM_LOCK:
        c = _mem_cache.get(key)
        if c is None:
            return None
        if now - c["ts"] < CACHE_TTL:
            return c["data"]
        del _mem_cache[key]
    return None


def _cache_set(key: str, data):
    """只写内存（超限时 TTL/最旧淘汰）；落盘由后台线程批量完成。"""
    global _dirty
    _ensure_bg_started()
    now = time.time()
    with _MEM_LOCK:
        _mem_cache[key] = {"ts": now, "data": data}
        if len(_mem_cache) > CACHE_MAX_ENTRIES:
            _evict_locked(now)
        _dirty = True


def _snapshot_to_disk() -> bool:
    """把内存缓存写成快照：持锁只做浅拷贝，序列化与写盘都在锁外；临时文件 + os.replace 原子替换。"""
    global _dirty
    now = time.time()
    with _MEM_LOCK:
        items = {k: v for k, v in _mem_cache.items()
                 if now - v.get("ts", 0) < CACHE_TTL and not k.startswith(_SNAPSHOT_SKIP_PREFIXES)}
        _dirty = False
    tmp = None
    try:
        _SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SNAPSHOT_FILE.with_name(f"{_SNAPSHOT_FILE.name}.{_os.getpid()}.tmp")
        tmp.write_text(_json_mod.dumps(items, ensure_ascii=False), encoding="utf-8")
        _os.replace(tmp, _SNAPSHOT_FILE)
        return True
    except Exception as e:
        with _MEM_LOCK:
            _dirty = True          # 写失败，下个周期重试
        logger.warning(f"[clawby_cache] 快照写入失败: {e}")
        try:
            if tmp is not None and tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return False


def _load_snapshot() -> int:
    """读一次快照填充内存（只填充内存中不存在或更旧的未过期条目）。返回加载条数。"""
    try:
        if not _SNAPSHOT_FILE.exists() or _SNAPSHOT_FILE.stat().st_size > _SNAPSHOT_MAX_LOAD_BYTES:
            return 0
        raw = _SNAPSHOT_FILE.read_text(encoding="utf-8")
        data = _json_mod.loads(raw) if raw.strip() else {}
        if not isinstance(data, dict):
            return 0
    except Exception as e:
        logger.warning(f"[clawby_cache] 快照读取失败（按空缓存继续）: {e}")
        return 0
    now = time.time()
    n = 0
    with _MEM_LOCK:
        for k, v in data.items():
            if not isinstance(v, dict) or "data" not in v or now - v.get("ts", 0) >= CACHE_TTL:
                continue
            cur = _mem_cache.get(k)
            if cur is None or cur.get("ts", 0) < v["ts"]:
                _mem_cache[k] = v
                n += 1
        if len(_mem_cache) > CACHE_MAX_ENTRIES:
            _evict_locked(now)
    return n


def _remove_legacy_cache() -> None:
    for f in _LEGACY_CACHE_FILES:
        try:
            if f.exists():
                sz = f.stat().st_size
                f.unlink()
                logger.info(f"[clawby_cache] 已清理旧版跨进程缓存文件 {f.name} ({sz / 1024 / 1024:.1f}MB)")
        except Exception as e:
            logger.warning(f"[clawby_cache] 清理旧缓存文件 {f.name} 失败: {e}")


def _bg_loop() -> None:
    _remove_legacy_cache()
    n = _load_snapshot()
    logger.info(f"[clawby_cache] 后台线程启动：快照加载 {n} 条，之后每 {CACHE_SNAPSHOT_INTERVAL:.0f}s 在有变更时落盘")
    while not _bg_stop.wait(CACHE_SNAPSHOT_INTERVAL):
        if _dirty:
            _snapshot_to_disk()


def _ensure_bg_started() -> None:
    """首次读写缓存时懒启动后台线程（加载快照 + 周期落盘）。FX_CLAWBY_CACHE_PERSIST=0 时只用内存。"""
    global _bg_started
    if _bg_started:
        return
    with _bg_start_lock:
        if _bg_started:
            return
        _bg_started = True
        if not _PERSIST_ENABLED:
            return
        threading.Thread(target=_bg_loop, name="clawby-cache-bg", daemon=True).start()
        try:
            import atexit
            atexit.register(lambda: _dirty and _snapshot_to_disk())
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────────────────────
# CG 中转站 GET 请求（核心合约接口用）
# ─────────────────────────────────────────────────────────────────────────────

def _parse_retry_after(value, default: float = 30.0, max_wait: float = 60.0) -> float:
    """
    [v3.89 #76] 解析 Retry-After 头，支持两种合法格式：
      · 纯秒数，如 "30" / "1.5"
      · HTTP 日期，如 "Wed, 21 Oct 2026 07:28:00 GMT"
    无法解析时返回 default；结果统一夹在 [0, max_wait]。
    """
    if value is None:
        return default
    v = str(value).strip()
    if not v:
        return default
    try:
        return max(0.0, min(float(v), max_wait))
    except (TypeError, ValueError):
        pass
    try:
        from email.utils import parsedate_to_datetime
        import datetime as _dt
        dt = parsedate_to_datetime(v)
        if dt is None:
            return default
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        delta = (dt - _dt.datetime.now(_dt.timezone.utc)).total_seconds()
        return max(0.0, min(delta, max_wait))
    except Exception:
        logger.debug(f"[cg_get] 无法解析 Retry-After={v!r}，使用默认 {default}s")
        return default


# ── 429 指数退避（全局，cg_get 所有端点共用同一限额）───────────────────────────
#   第 1 次 429 等 10s，第 2 次 20s，第 3 次 40s，之后最大 60s；退避期内 cg_get 直接返回 None，不发新请求、也不阻塞调用线程。
#   任一请求成功（200）即清零连续计数。Retry-After 若更长则取更长者（同样封顶 60s）。
_CG_429_LOCK = threading.Lock()
_CG_429_UNTIL: float = 0.0
_CG_429_COUNT: int = 0
CG_429_BASE_WAIT = 10.0
CG_429_MAX_WAIT = 60.0


def cg_backoff_remaining() -> float:
    """429 退避剩余秒数；0 表示可发请求。"""
    return max(0.0, _CG_429_UNTIL - time.time())


def _cg_note_429(retry_after: float = 0.0) -> float:
    """记录一次 429，返回本次退避秒数。"""
    global _CG_429_UNTIL, _CG_429_COUNT
    with _CG_429_LOCK:
        _CG_429_COUNT += 1
        wait = min(CG_429_BASE_WAIT * (2 ** (_CG_429_COUNT - 1)), CG_429_MAX_WAIT)
        wait = min(max(wait, float(retry_after or 0.0)), CG_429_MAX_WAIT)
        _CG_429_UNTIL = max(_CG_429_UNTIL, time.time() + wait)
        return wait


def _cg_note_ok() -> None:
    global _CG_429_COUNT
    with _CG_429_LOCK:
        _CG_429_COUNT = 0


def cg_get(path: str, params: dict = None, timeout: int = 8) -> "list | dict | None":
    """
    GET https://vip2.coinglass.site/<path>
    返回 data 字段（成功）或 None（失败/限速）。
    自动处理 429 Retry-After（单次重试，最多等待 60s）。
    """
    _left = cg_backoff_remaining()
    if _left > 0:        # 429 退避期内不发新请求（调用方按"拿不到数据"处理，清算区走原有降级）
        logger.debug(f"[cg_get] 429 退避中（剩 {_left:.0f}s），跳过 {path}")
        return None
    _wait_cg_rate_limit()
    url = f"{CG_BASE_URL}/{path.lstrip('/')}"
    for attempt in range(2):
        try:
            r = requests.get(url, params=params or {}, headers=CG_HEADERS,
                             timeout=timeout, proxies=_PROXIES)
        except requests.exceptions.RequestException as e:
            logger.warning(f"[cg_get] {path} 请求异常: {e}")
            return None

        if r.status_code == 429:
            # [v3.89 #76] Retry-After 可能是秒数，也可能是 HTTP 日期格式；
            ra = _parse_retry_after(r.headers.get("Retry-After"), default=0.0)
            wait = _cg_note_429(ra)
            logger.warning(f"[cg_get] 429 {path}, 指数退避 {wait:.0f}s（退避期内不发新请求）")
            return None

        if r.status_code != 200:
            logger.debug(f"[cg_get] {path} HTTP {r.status_code}")
            return None
        _cg_note_ok()      # 200：清零连续 429 计数，下次 429 从 10s 重新起算

        try:
            body = r.json()
        except Exception:
            return None

        # [v3.89 #76] 顶层必须是 dict；合法 JSON 也可能是 list/str，旧实现直接 .get 会抛异常
        if not isinstance(body, dict):
            logger.debug(f"[cg_get] {path} 顶层结构非 dict: {type(body).__name__}")
            return None

        code = str(body.get("code", ""))
        if code not in ("0", "200", ""):
            logger.debug(f"[cg_get] {path} business code={code} msg={body.get('msg','')}")
            return None
        return body.get("data")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Clawby relay 调用
# ─────────────────────────────────────────────────────────────────────────────

# 402 全局投死开关：一旦收到 402，就停止尝试 Clawby，防止每个请求都资耗时间
_CLAWBY_402_UNTIL: float = 0.0   # 熟举时间戳，0表示未被熔断
_CLAWBY_402_COOLDOWN = 600       # 402后冷却 10 分钟再尝试

def _unwrap_clawby(raw, name: str = ""):
    """统一拆 Clawby 响应外壳，内层 code 非 0/200 → None（不把错误对象当数据）。
    两种返回格式兼容:
      1. {"source":..., "data":{"code":"0","data":[...]}, "credits":{...}}  ← 标准 Clawby 格式
      2. {"source":..., "data":[...]}                                       ← 直接数据
    返回内层数据；非 dict、code 异常、升级提示（401/402/Upgrade plan）、null 数据一律返回 None。
    """
    if not isinstance(raw, dict):
        return None
    raw_data = raw.get("data")
    # 嵌套的上游响应 {"code":"0","data":[...]}
    if isinstance(raw_data, dict) and "data" in raw_data:
        code = str(raw_data.get("code", "0"))
        if code in ("0", "200"):
            return raw_data["data"]          # code=0 但 data 为 null → None（视为空结果）
        logger.debug(f"[clawby] {name} upstream code={code} msg={raw_data.get('msg','')}")
        return None
    # 无嵌套 data 的 dict：先过滤升级提示 / 错误对象
    if isinstance(raw_data, dict):
        code = str(raw_data.get("code", ""))
        msg  = str(raw_data.get("msg", "")).lower()
        if code in ("401", "402") or "upgrade" in msg or "plan" in msg:
            logger.debug(f"[clawby] {name} 需要更高套餐 (code={code} msg={raw_data.get('msg','')})，静默降级")
            return None
        if code not in ("", "0", "200"):
            logger.debug(f"[clawby] {name} 返回错误对象 code={code} msg={raw_data.get('msg','')}")
            return None
    return raw_data


def clawby_relay(name: str, params: dict = None, timeout: int = 15,
                 _max_retries: int = 3):
    """
    POST https://api.openclawby.com/api/relay
    {"name": "<name>", "params": {...}}
    返回 data 字段（成功），None（404/空/402降级），抛异常（授权失败）
    """
    global _CLAWBY_402_UNTIL

    # 402 投死期内直接返回 None，不再费时间重试
    if time.time() < _CLAWBY_402_UNTIL:
        return None
    payload = {"name": name, "params": params or {}}
    for attempt in range(_max_retries + 1):
        _wait_clawby_rate_limit()
        try:
            r = requests.post(
                CLAWBY_BASE_URL,
                headers=CLAWBY_HEADERS,
                json=payload,
                timeout=timeout,
                proxies=_PROXIES,
            )
        except requests.exceptions.Timeout:
            _cb_record("fail_timeout")
            if attempt < _max_retries:
                time.sleep(3)
                continue
            raise

        if r.status_code == 404:
            _cb_record("fail_404")
            return None

        if r.status_code == 402:
            # 套餐额度不足：设置投死期，降级返回 None 不抛异常
            _CLAWBY_402_UNTIL = time.time() + _CLAWBY_402_COOLDOWN
            logger.warning(f"[clawby] 402 套餐额度不足，{_CLAWBY_402_COOLDOWN//60}分钟内不再尝试 Clawby API，开仓将使用兑底地首")
            return None

        # 处理成功返回中的升级提示（某些币种需更高套餐）
        try:
            body = r.json()
            code = str(body.get("code", ""))
            msg  = body.get("msg", "")
            if code in ("401", "402") or "upgrade" in str(msg).lower() or "plan" in str(msg).lower():
                # 这个币种没有权限，静默降级不报错
                return None
        except Exception:
            pass

        if r.status_code == 429:
            retry_after = float(r.headers.get("Retry-After", 10))
            wait = min(retry_after, 60)
            logger.warning(f"[clawby] 429, waiting {wait:.1f}s (attempt {attempt+1})")
            if attempt < _max_retries:
                time.sleep(wait)
                continue
            r.raise_for_status()

        if r.status_code == 401:
            raise RuntimeError(f"[clawby] 401 未授权 — key 无效或套餐过期")

        r.raise_for_status()
        _cb_record("ok")

        return _unwrap_clawby(r.json(), name)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# 辅助
# ─────────────────────────────────────────────────────────────────────────────
def _sym_full(symbol: str) -> str:
    s = symbol.upper()
    return s if s.endswith("USDT") else f"{s}USDT"

def _sym_base(symbol: str) -> str:
    return symbol.upper().replace("USDT", "")


# ═══════════════════════════════════════════════════════════════════════════════
# ① 清算热力图（替换原 CG V3）
# ═══════════════════════════════════════════════════════════════════════════════

def get_liq_heatmap(symbol: str, exchange: str = "Binance",
                    range_: str = "3d") -> dict | None:
    """清算热力图 → CG中转 GET（实时性更好，已验证可用）
    返回 {"y_axis": [...prices], "liquidation_leverage_data": [...], ...}
    """
    sym = _sym_full(symbol)
    key = f"heatmap:{sym}:{range_}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data = cg_get("api/futures/liquidation/heatmap/model1",
                  {"symbol": sym, "exchange": exchange, "range": range_})
    if data:
        _cache_set(key, data)
    return data


def get_liq_density(symbol: str, exchange: str = "Binance",
                    range_: str = "3d") -> dict:
    """各价位清算密度。{price_float: density_float}"""
    data = get_liq_heatmap(symbol, exchange, range_)
    if not data:
        return {}
    # CG中转返回 y_axis / liquidation_leverage_data；兼容旧 y / liq 字段
    prices_y = data.get("y_axis", data.get("y", []))
    liq_key  = "liquidation_leverage_data" if "liquidation_leverage_data" in data else "liq"
    density  = {}
    for entry in data.get(liq_key, []):
        if len(entry) < 3:
            continue
        p_idx = int(entry[1])
        val   = float(entry[2])
        if 0 <= p_idx < len(prices_y):
            price = float(prices_y[p_idx])
            density[price] = density.get(price, 0.0) + val
    return density


def get_liq_zones(symbol: str, current_price: float,
                  exchange: str = "Binance", range_: str = "3d",
                  top_n: int = 5) -> dict:
    """上下方清算密集区（签名与原 coinglass_api.py 完全一致）"""
    density = get_liq_density(symbol, exchange, range_)
    if not density:
        return {"above": [], "below": [], "above_total": 0,
                "below_total": 0, "ratio": 1.0, "bias": "neutral", "score_adjust": 0}

    above = {p: v for p, v in density.items() if p > current_price}
    below = {p: v for p, v in density.items() if p < current_price}
    above_total = sum(above.values())
    below_total = sum(below.values())
    ratio = below_total / (above_total + 1e-9)

    bias = "short" if ratio > 1.3 else ("long" if ratio < 0.77 else "neutral")
    score_adjust = -20 if bias == "short" else (10 if bias == "long" else 0)

    above_sorted = sorted(above.items(), key=lambda x: -x[1])[:top_n]
    below_sorted = sorted(below.items(), key=lambda x: -x[1])[:top_n]
    above_sorted.sort(key=lambda x: x[0])
    below_sorted.sort(key=lambda x: -x[0])

    return {
        "above": above_sorted, "below": below_sorted,
        "above_total": round(above_total, 2),
        "below_total": round(below_total, 2),
        "ratio": round(ratio, 3),
        "bias": bias, "score_adjust": score_adjust,
    }


def get_liquidation_zone(symbol: str, price: float, direction: str,
                         search_range_pct: float = 5.0,
                         min_volume: float = 500_000) -> dict:
    """Q3 标准化清算区查询（签名与原 coinglass_api.py 完全一致）"""
    empty = {"has_liq_zone": False, "liq_zone_price": 0.0,
             "liq_zone_volume": 0.0, "liq_zone_side": ""}
    if price <= 0:
        return empty
    try:
        liq      = get_liq_zones(symbol, price)
        side_key = "below" if direction == "LONG" else "above"
        zones    = liq.get(side_key, [])
        if not zones:
            return empty

        range_lo = price * (1 - search_range_pct / 100)
        range_hi = price * (1 + search_range_pct / 100)
        valid    = []
        for z in zones:
            zp, zv = float(z[0]), float(z[1])
            if zv < min_volume:
                continue
            if direction == "LONG"  and range_lo <= zp < price:
                valid.append((zp, zv))
            elif direction == "SHORT" and price < zp <= range_hi:
                valid.append((zp, zv))

        if not valid:
            return empty
        best = max(valid, key=lambda x: x[1])
        return {"has_liq_zone": True, "liq_zone_price": best[0],
                "liq_zone_volume": best[1], "liq_zone_side": side_key}
    except Exception:
        return empty


def get_liq_zones_list(symbol: str, direction: str,
                       current_price: float = 0.0) -> list:
    """fx 系统适配层（签名与原 coinglass_api.py 完全一致）"""
    if not current_price:
        try:
            from shared.binance_api import get_mark_price
            mp = get_mark_price(symbol)
            current_price = float(mp.get("markPrice", 0))
        except Exception:
            return []
    try:
        data = get_liq_zones(symbol, current_price)
    except Exception as e:
        logger.warning(f"[clawby] {symbol} get_liq_zones 失败: {e}")
        return []
    if not data:
        return []
    raw = data.get("below" if direction == "LONG" else "above", [])
    zones = []
    for price_pt, liq_usd in raw:
        price_f = float(price_pt)
        half    = price_f * 0.005
        zones.append({"price_low":  round(price_f - half, 8),
                      "price_high": round(price_f + half, 8),
                      "liq_usd":    float(liq_usd)})
    return zones


# ═══════════════════════════════════════════════════════════════════════════════
# ② 爆仓历史（替换原 CG V4）
# ═══════════════════════════════════════════════════════════════════════════════

def _get_liq_history(symbol: str, exchange: str = "Binance",
                     interval: str = "1h", limit: int = 6) -> list:
    """爆仓历史 → CG中转 GET（需要 interval 参数，已验证可用）
    字段: long_liquidation_usd / short_liquidation_usd
    """
    sym = _sym_full(symbol)
    key = f"liq1h:{sym}:{interval}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data = cg_get("api/futures/liquidation/history",
                  {"symbol": sym, "exchange": exchange,
                   "interval": interval, "limit": limit})
    result = data if isinstance(data, list) else []
    if result:
        _cache_set(key, result)
    return result

def get_liq_summary_1h(symbol: str, exchange: str = "Binance") -> dict:
    """最近1h多空爆仓量摘要（供外部调用）"""
    default = {"long_liq_1h": 0.0, "short_liq_1h": 0.0, "liq_signal": "neutral"}
    try:
        rows = _get_liq_history(symbol, exchange, interval="1h", limit=2)
        if not rows:
            return default
        latest    = rows[-1] if isinstance(rows[-1], dict) else {}
        long_liq  = float(latest.get("long_liquidation_usd",  0) or 0)
        short_liq = float(latest.get("short_liquidation_usd", 0) or 0)
        total     = long_liq + short_liq
        if total < 1000:
            signal = "neutral"
        elif long_liq > short_liq * 2:
            signal = "heavy_long_liq"
        elif short_liq > long_liq * 2:
            signal = "heavy_short_liq"
        else:
            signal = "neutral"
        return {"long_liq_1h": long_liq, "short_liq_1h": short_liq, "liq_signal": signal}
    except Exception as e:
        logger.warning(f"[clawby] get_liq_summary_1h({symbol}) 失败: {e}")
        return default

def get_liq_orders(symbol: str, exchange: str = "Binance",
                   min_usd: float = 100_000) -> list:
    """大额清算订单 → CG中转 GET（已验证可用）"""
    sym = _sym_full(symbol)
    key = f"liq_orders:{sym}:{min_usd}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data   = cg_get("api/futures/liquidation/order",
                    {"symbol": sym, "exchange": exchange})
    result = data if isinstance(data, list) else []
    if result:
        _cache_set(key, result)
    return result

def get_liq_coin_list(exchange: str = "Binance") -> list:
    """全市场清算量快照 → CG中转 GET"""
    key = f"liq_coin_list:{exchange}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data   = cg_get("api/futures/liquidation/coin-list", {"exchange": exchange})
    result = data if isinstance(data, list) else []
    if result:
        _cache_set(key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# ③ Binance fapi 公开接口（保持不变，无需 Clawby）
# ═══════════════════════════════════════════════════════════════════════════════

FAPI_BASE = "https://fapi.binance.com"

def _bn_get(path: str, params: dict = None, timeout: int = 8):
    r = requests.get(f"{FAPI_BASE}{path}", params=params or {}, timeout=timeout, proxies=_PROXIES,
                     headers={"Accept-Encoding": "identity"})  # 禁用响应压缩
    r.raise_for_status()
    return r.json()

def _get_top_lsr(sym_full: str, exchange: str = "Binance",
                 interval: str = "1h", limit: int = 6) -> list:
    key = f"top_lsr:{sym_full}:{interval}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data = _bn_get("/futures/data/globalLongShortAccountRatio",
                       {"symbol": sym_full, "period": interval, "limit": limit})
        if isinstance(data, list):
            mapped = [{"top_account_long_percent":  float(d.get("longAccount",  0.5)) * 100,
                       "top_account_short_percent": float(d.get("shortAccount", 0.5)) * 100,
                       "timestamp": d.get("timestamp")} for d in data]
            _cache_set(key, mapped)
            return mapped
    except Exception:
        pass
    return []

def _get_taker(sym_full: str, exchange: str = "Binance",
               interval: str = "1h", limit: int = 6) -> list:
    key = f"taker:{sym_full}:{interval}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data = _bn_get("/futures/data/takerlongshortRatio",
                       {"symbol": sym_full, "period": interval, "limit": limit})
        if isinstance(data, list):
            mapped = [{"taker_buy_volume_usd":  float(d.get("buyVol",  0)),
                       "taker_sell_volume_usd": float(d.get("sellVol", 0)),
                       "timestamp": d.get("timestamp")} for d in data]
            _cache_set(key, mapped)
            return mapped
    except Exception:
        pass
    return []

def _get_oi_history(sym_full: str, exchange: str = "Binance",
                    interval: str = "1h", limit: int = 6) -> list:
    key = f"oi:{sym_full}:{interval}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data = _bn_get("/futures/data/openInterestHist",
                       {"symbol": sym_full, "period": interval, "limit": limit})
        if isinstance(data, list):
            # 转换为 OHLC 格式（close = 当期 OI）
            mapped = [{"close": float(d.get("sumOpenInterestValue", 0)),
                       "timestamp": d.get("timestamp")} for d in data]
            _cache_set(key, mapped)
            return mapped
    except Exception:
        pass
    return []

def _get_funding_rate(sym_full: str, exchange: str = "Binance",
                      limit: int = 6) -> list:
    key = f"fr:{sym_full}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data = _bn_get("/fapi/v1/fundingRate",
                       {"symbol": sym_full, "limit": limit})
        if isinstance(data, list):
            mapped = [{"close": float(d.get("fundingRate", 0)),
                       "timestamp": d.get("fundingTime")} for d in data]
            _cache_set(key, mapped)
            return mapped
    except Exception:
        pass
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# ✨ 新增 — 合约工作流信号
# ═══════════════════════════════════════════════════════════════════════════════

def get_whale_index(symbol: str, exchange: str = "Binance",
                    interval: str = "1h", limit: int = 6) -> list:
    """
    巨鲸指数 — 已停用。
    经测试 Clawby futures_whale_index_history 和 CG中转 所有路径均 404。
    保留函数签名以免调用方报错，直接返回空列表（调用方已有 if whale_data 保护）。
    """
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Clawby 优先 + CG 兜底（通用）
# ─────────────────────────────────────────────────────────────────────────────
def _rows_to_float(rows, fields: tuple):
    """把列表行里的指定字段转 float（Clawby 爆仓量是字符串）。任何行/字段无法转换 → 返回 None（触发降级）。"""
    if not isinstance(rows, list) or not rows:
        return None
    out = []
    for row in rows:
        if not isinstance(row, dict):
            return None
        nr = dict(row)
        for f in fields:
            if f in nr and nr[f] is not None:
                try:
                    nr[f] = float(nr[f])
                except (TypeError, ValueError):
                    return None
        out.append(nr)
    return out


def _clawby_first_list(relay_name: str, cg_path: str, sym: str, exchange: str,
                       interval: str, limit: int, float_fields: tuple) -> list:
    """先 Clawby（串行、不重试：429/超时/内层 code≠0/空/字段异常 → 立即降级），再 CG；
    两条路径都过同一套 float 规整，返回结构一致（list[dict]），失败返回 []。"""
    params = {"symbol": sym, "exchange": exchange, "interval": interval, "limit": limit}
    try:
        raw = clawby_relay(relay_name, dict(params), timeout=8, _max_retries=0)
        rows = _rows_to_float(raw if isinstance(raw, list) else None, float_fields)
        if rows:
            return rows
    except Exception as e:
        logger.debug(f"[clawby] {relay_name}({sym}) 失败，降级 CG: {e}")
    data = cg_get(cg_path, dict(params))
    rows = _rows_to_float(data if isinstance(data, list) else None, float_fields)
    return rows if rows else (data if isinstance(data, list) else [])


def get_cvd_history(symbol: str, exchange: str = "Binance",
                    interval: str = "1h", limit: int = 6) -> list:
    """CVD 历史：Clawby(futures_cvd_history) 优先，CG 兜底。
    字段: time / taker_buy_vol / taker_sell_vol / cum_vol_delta（数值已转 float）"""
    sym = _sym_full(symbol)
    key = f"cvd:{sym}:{interval}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    result = _clawby_first_list("futures_cvd_history", "api/futures/cvd/history", sym, exchange,
                                interval, limit,
                                ("taker_buy_vol", "taker_sell_vol", "cum_vol_delta"))
    if result:
        _cache_set(key, result)
    return result


def get_cvd(symbol: str, exchange: str = "Binance",
            interval: str = "1h", limit: int = 6) -> list:
    """
    累计成交量差 (CVD) 历史 → 兼容入口，转调 get_cvd_history（Clawby 优先，CG 兜底）
    字段: time / taker_buy_vol / taker_sell_vol / cum_vol_delta
    """
    return get_cvd_history(symbol, exchange, interval, limit)


def get_coinbase_premium(interval: str = "1h", limit: int = 6) -> list:
    """
    Coinbase 溢价指数 → CG中转 GET（Clawby relay 返回空，CG GET 已验证可用）
    字段: time(s) / premium / premium_rate / coinbase_price
    注意: CG中转返回的 time 单位是秒，非毫秒。
    """
    key = f"cb_premium:{interval}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data   = cg_get("api/coinbase-premium-index",
                    {"interval": interval, "limit": limit})
    result = data if isinstance(data, list) else []
    if result:
        _cache_set(key, result)
    return result


def get_fear_greed(limit: int = 7) -> dict:
    """
    贪婪恐惧指数历史。
    极端恐惧（<15）时慎开空；极端贪婪（>85）时慎追多。
    返回: {"data_list": [float, ...], "timestamps": [...]}
    """
    key = f"fear_greed:{limit}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data = clawby_relay("index_fear_greed_history", {"limit": limit})
        if data:
            _cache_set(key, data)
        return data or {}
    except Exception as e:
        logger.debug(f"[clawby] get_fear_greed 失败: {e}")
        return {}


def get_boll(symbol: str, exchange: str = "Binance",
             interval: str = "1h", limit: int = 20) -> list:
    """
    布林带 — 已停用。
    经测试 Clawby futures_indicators_boll 返回 400 "pair does not exist"（BTC 也失败）。
    保留函数签名，直接返回空列表。
    """
    return []


def get_liq_max_pain(range_: str = "7d") -> dict | None:
    """
    清算 Max Pain — 当前杠杆最痛点价格。
    可辅助判断目标价。原 CG 代理无此接口。
    """
    key = f"liq_max_pain:{range_}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data = clawby_relay("futures_liquidation_max_pain", {"range": range_})
        if data:
            _cache_set(key, data)
        return data
    except Exception as e:
        logger.debug(f"[clawby] get_liq_max_pain 失败: {e}")
        return None


def get_liq_aggregated_heatmap(symbol: str, range_: str = "3d") -> dict | None:
    """
    多交易所聚合清算热力图（Binance+OKX+Bybit） → CG中转 GET
    ⚠️ 重要: symbol 必须是 coin base name（如 BTC、EPIC），不能带 USDT 后缀！
    CG中转 aggregated-heatmap 接收不带 USDT 的 symbol，带 USDT 会返回 500。
    """
    base = _sym_base(symbol)   # 确保去掉 USDT
    key  = f"agg_heatmap:{base}:{range_}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data = cg_get("api/futures/liquidation/aggregated-heatmap/model1",
                  {"symbol": base, "range": range_})
    if data:
        _cache_set(key, data)
    return data


def get_bull_market_peak() -> list:
    """
    牛市顶峰多维指标（AHR999/Puell/Stock-to-Flow 等）。
    大周期判断，宏观方向参考。
    返回: [{"indicator_name": str, "current_value": str,
             "target_value": str, "previous_value": str}, ...]
    """
    key = "bull_market_peak"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data   = clawby_relay("bull_market_peak_indicator", {})
        result = data if isinstance(data, list) else []
        if result:
            _cache_set(key, result)
        return result
    except Exception as e:
        logger.debug(f"[clawby] get_bull_market_peak 失败: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# ✨ 新增 — 套利工作流信号
# ═══════════════════════════════════════════════════════════════════════════════

def get_funding_arb_opps(top_n: int = 20) -> list:
    """
    全市场资金费率套利机会排行（跨交易所）。
    返回按费率差降序排列的套利标的列表。
    字段: symbol / buy{exchange, funding_rate, open_interest_usd} /
          sell{exchange, funding_rate} / rate_diff
    """
    key = "funding_arb"
    cached = _cache_get(key)
    if cached is not None:
        return (cached or [])[:top_n]
    try:
        data   = clawby_relay("futures_funding_rate_arbitrage", {})
        result = data if isinstance(data, list) else []
        if result:
            _cache_set(key, result)
        return result[:top_n]
    except Exception as e:
        logger.warning(f"[clawby] get_funding_arb_opps 失败: {e}")
        return []


def get_funding_exchange_list(symbol: str = "BTCUSDT") -> list:
    """
    各交易所同一标的当前资金费率快照。
    用于监控 Gate/OKX/Bybit 间费率差，辅助套利方向决策。
    """
    sym = _sym_full(symbol)
    key = f"fr_xlist:{sym}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data = clawby_relay("futures_funding_rate_exchange_list", {"symbol": sym})
        # 返回格式: {"symbol":"BTC","stablecoin_margin_list":[{exchange, funding_rate,...},...]}
        if isinstance(data, list):
            result = data
        elif isinstance(data, dict):
            result = data.get("stablecoin_margin_list", [])
        else:
            result = []
        if result:
            _cache_set(key, result)
        return result
    except Exception as e:
        logger.debug(f"[clawby] get_funding_exchange_list({sym}) 失败: {e}")
        return []


def get_oi_exchange_list(symbol: str = "BTC") -> list:
    """
    各交易所 OI 对比快照。
    用于判断流动性，避开 OI 过小的标的。
    """
    base = _sym_base(symbol)
    key  = f"oi_xlist:{base}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data   = clawby_relay("futures_open_interest_exchange_list", {"symbol": base})
        result = data if isinstance(data, list) else []
        if result:
            _cache_set(key, result)
        return result
    except Exception as e:
        logger.debug(f"[clawby] get_oi_exchange_list({base}) 失败: {e}")
        return []


def get_economic_calendar(limit: int = 10) -> list:
    """
    宏观经济日历（非农/CPI/FOMC 等）。
    重要事件前暂停套利，规避黑天鹅风险。
    返回: [{"calendar_name": str, "country_code": str,
             "event_time": ms, "actual": str, "forecast": str,
             "previous": str, "importance": int}, ...]
    """
    key = f"econ_cal:{limit}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data   = clawby_relay("calendar_economic_data", {})
        result = (data or [])[:limit] if isinstance(data, list) else []
        if result:
            _cache_set(key, result)
        return result
    except Exception as e:
        logger.debug(f"[clawby] get_economic_calendar 失败: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# ✨ 新增 — bstocks 工作流信号
# ═══════════════════════════════════════════════════════════════════════════════

def get_btc_etf_flow(limit: int = 7) -> list:
    """
    BTC ETF 每日净流入历史。
    大量净流入 → 机构看多，可作为 bstocks 方向过滤信号。
    返回: [{"timestamp": ms, "flow_usd": float, "price_usd": float,
             "etf_flows": [{ticker, flow_usd}, ...]}, ...]
    """
    key = f"btc_etf_flow:{limit}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data   = clawby_relay("etf_bitcoin_flow_history", {"limit": limit})
        result = data if isinstance(data, list) else []
        if result:
            _cache_set(key, result)
        return result
    except Exception as e:
        logger.debug(f"[clawby] get_btc_etf_flow 失败: {e}")
        return []


def get_btc_etf_aum(limit: int = 7) -> list:
    """
    BTC ETF 总 AUM 变化历史。
    中长期机构情绪趋势参考。
    返回: [{"net_assets_usd": float, "change_usd": float,
             "timestamp": ms, "price_usd": float}, ...]
    """
    key = f"btc_etf_aum:{limit}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data   = clawby_relay("etf_bitcoin_net_assets_history", {"limit": limit})
        result = data if isinstance(data, list) else []
        if result:
            _cache_set(key, result)
        return result
    except Exception as e:
        logger.debug(f"[clawby] get_btc_etf_aum 失败: {e}")
        return []


def get_hl_whale_alerts(limit: int = 20) -> list:
    """
    Hyperliquid 巨鲸新开仓/平仓警报。
    顶级交易者方向参考，可辅助 bstocks 信号确认。
    返回: [{"user": addr, "symbol": str, "position_size": float,
             "entry_price": float, "side": str, "leverage": int,
             "created_at": ms}, ...]
    """
    key = "hl_whale_alerts"
    cached = _cache_get(key)
    if cached is not None:
        return (cached or [])[:limit]
    try:
        data   = clawby_relay("hyperliquid_whale_alert", {})
        result = data if isinstance(data, list) else []
        if result:
            _cache_set(key, result)
        return result[:limit]
    except Exception as e:
        logger.debug(f"[clawby] get_hl_whale_alerts 失败: {e}")
        return []


def get_hl_whale_positions(limit: int = 20) -> list:
    """
    Hyperliquid 巨鲸当前持仓快照。
    大仓位方向统计，辅助判断主力方向。
    返回: [{"user": addr, "symbol": str, "position_size": float,
             "entry_price": float, "side": str, "unrealized_pnl": float}, ...]
    """
    key = "hl_whale_pos"
    cached = _cache_get(key)
    if cached is not None:
        return (cached or [])[:limit]
    try:
        data   = clawby_relay("hyperliquid_whale_position", {})
        result = data if isinstance(data, list) else []
        if result:
            _cache_set(key, result)
        return result[:limit]
    except Exception as e:
        logger.debug(f"[clawby] get_hl_whale_positions 失败: {e}")
        return []


def get_altcoin_season(limit: int = 3) -> list:
    """
    山寨季指数历史。
    指数高 → 资金流入山寨（利好 bstocks），指数低 → BTC 主导。
    返回: [{"timestamp": ms, "altcoin_index": float}, ...]
    """
    key = f"altcoin_season:{limit}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        data = clawby_relay("index_altcoin_season", {})
        result = (data[-limit:] if isinstance(data, list) and len(data) > limit
                  else (data if isinstance(data, list) else []))
        if result:
            _cache_set(key, result)
        return result
    except Exception as e:
        logger.debug(f"[clawby] get_altcoin_season 失败: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# CG中转 LSR 信号（专用限速保护：2次/分钟）
# ═══════════════════════════════════════════════════════════════════════════════

def get_lsr_signal(symbol: str, mode: str = "trader") -> dict | None:
    """
    从 CG中转 获取 LSR 多空信号（/api/lsr/signal/<BASE>）。
    ⚠️ 限速 2次/分钟（全局令牌桶）：超限时直接返回 None，不阻塞扫描主流程。
    调用方应先检查缓存，仅在缓存 miss 时调用。

    返回字段示例:
      ratio, ratio_30m, long_traders, short_traders,
      long_whales, short_whales, ov_long_avg_entry, ov_short_avg_entry,
      ov_long_profit_traders, ov_short_profit_traders, delta_4h, ...
    """
    base = _sym_base(symbol)
    key  = f"lsr:{base}:{mode}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    if not _lsr_try_acquire():
        logger.debug(f"[cg_lsr] {base} LSR 令牌不足，跳过（避免 429）")
        return None

    url = f"{CG_BASE_URL}/api/lsr/signal/{base}"
    try:
        r = requests.get(url, params={"mode": mode}, headers=CG_HEADERS,
                         timeout=12, proxies=_PROXIES)
    except requests.exceptions.RequestException as e:
        logger.warning(f"[cg_lsr] {base} 请求异常: {e}")
        return None

    if r.status_code == 429:
        retry_after = float(r.headers.get("Retry-After", 30))
        logger.warning(f"[cg_lsr] 429 {base}, Retry-After={retry_after:.0f}s，不阻塞，下轮再试")
        return None

    if r.status_code != 200:
        logger.debug(f"[cg_lsr] {base} HTTP {r.status_code}")
        return None

    try:
        body = r.json()
    except Exception:
        return None

    # LSR 接口 success=true 时 code 字段可能不存在
    if not body.get("success", False) and str(body.get("code", "")) not in ("0", "200", ""):
        logger.debug(f"[cg_lsr] {base} business error: {body.get('msg','')}")
        return None

    # 直接返回完整响应（不只是 data 字段）
    _cache_set(key, body)
    return body


# ═══════════════════════════════════════════════════════════════════════════════
# 综合衍生品信号（兼容原 coinglass_api.get_derivatives_signals）
# ═══════════════════════════════════════════════════════════════════════════════

_DERIV_POOL = None
_DERIV_POOL_LOCK = threading.Lock()


def _get_deriv_pool():
    """get_derivatives_signals 专用的模块级共享线程池（2 线程）。子任务内部不再向该池提交任务，不会死锁。"""
    global _DERIV_POOL
    if _DERIV_POOL is None:
        with _DERIV_POOL_LOCK:
            if _DERIV_POOL is None:
                from concurrent.futures import ThreadPoolExecutor
                _DERIV_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="deriv")
    return _DERIV_POOL


def get_derivatives_signals(symbol: str, current_price: float = None,
                             exchange: str = "Binance") -> dict:
    """
    获取全量衍生品信号（与原 coinglass_api.py 签名完全一致）。
    多空比/大单/OI/资金费率 → Binance fapi（无速率限制）
    爆仓                   → Clawby
    ✨ 新增: whale_index / cvd / coinbase_premium 字段
    """
    sym = _sym_full(symbol)
    key = f"deriv:{sym}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    result = {
        "long_pct":      None, "short_pct":   None, "lsr_signal":   "neutral",
        "buy_vol_pct":   None, "taker_signal": "neutral",
        "oi_change_pct": None, "oi_signal":    "neutral",
        "fr_latest":     None, "fr_trend":     "neutral",
        "long_liq_1h":   0.0,  "short_liq_1h": 0.0, "liq_signal": "neutral",
        # ✨ 新增字段
        "whale_index":   None,   # 最新巨鲸指数值
        "cvd_delta":     None,   # 最新 CVD delta（正=主买强，负=主卖强）
        "cb_premium":    None,   # 最新 Coinbase 溢价率
        "_raw": {}
    }

    # 模块级共享 2 线程池（不再每次新建 8 线程）；已停用的 whale_index 不再提交
    ex = _get_deriv_pool()
    f_lsr     = ex.submit(_get_top_lsr,      sym, exchange)
    f_tkr     = ex.submit(_get_taker,        sym, exchange)
    f_oi      = ex.submit(_get_oi_history,   sym, exchange)
    f_fr      = ex.submit(_get_funding_rate, sym, exchange)
    f_cvd     = ex.submit(get_cvd,           sym, exchange)
    f_cb      = ex.submit(get_coinbase_premium)
    f_liq     = ex.submit(_get_liq_history,  sym, exchange)
    lsr_data   = f_lsr.result()
    taker_data = f_tkr.result()
    oi_hist    = f_oi.result()
    fr_list    = f_fr.result()
    whale_data = []            # get_whale_index 已停用（恒返回 []）
    cvd_data   = f_cvd.result()
    cb_data    = f_cb.result()
    liq_hist_prefetch = f_liq.result()

    # ── 1. 多空比 ─────────────────────────────────────────────────────────────
    if lsr_data:
        latest    = lsr_data[-1]
        long_pct  = float(latest.get("top_account_long_percent",  50)) / 100
        short_pct = float(latest.get("top_account_short_percent", 50)) / 100
        result["long_pct"]  = round(long_pct, 4)
        result["short_pct"] = round(short_pct, 4)
        if short_pct > 0.65:
            result["lsr_signal"] = "long_friendly"
        elif long_pct > 0.75:
            result["lsr_signal"] = "short_friendly"
        result["_raw"]["lsr"] = latest

    # ── 2. 大单流向 ───────────────────────────────────────────────────────────
    if taker_data:
        latest   = taker_data[-1]
        buy_vol  = float(latest.get("taker_buy_volume_usd",  0))
        sell_vol = float(latest.get("taker_sell_volume_usd", 0))
        total    = buy_vol + sell_vol
        buy_pct  = buy_vol / total if total > 0 else 0.5
        result["buy_vol_pct"] = round(buy_pct, 4)
        if buy_pct > 0.60:
            result["taker_signal"] = "strong_buy"
        elif buy_pct < 0.40:
            result["taker_signal"] = "strong_sell"
        result["_raw"]["taker"] = latest

    # ── 3. OI 变化 ────────────────────────────────────────────────────────────
    if len(oi_hist) >= 2:
        oi_old = float(oi_hist[-2].get("close", 0))
        oi_new = float(oi_hist[-1].get("close", 0))
        oi_chg = (oi_new - oi_old) / (oi_old + 1e-9)
        result["oi_change_pct"] = round(oi_chg, 4)
        if oi_chg > 0.005:
            result["oi_signal"] = "accumulate"
        elif oi_chg < -0.005:
            result["oi_signal"] = "deleverage"
        result["_raw"]["oi"] = oi_hist[-2:]

    # ── 4. 资金费率 ───────────────────────────────────────────────────────────
    if fr_list:
        rates = [float(f.get("close", 0)) for f in fr_list]
        result["fr_latest"] = round(rates[-1], 6)
        if len(rates) >= 3:
            recent = rates[-3:]
            if recent[-1] > recent[0] + 0.0001:
                result["fr_trend"] = "rising"
            elif recent[-1] < recent[0] - 0.0001:
                result["fr_trend"] = "falling"
        result["_raw"]["fr"] = rates[-3:]

    # ── 5. 爆仓（Clawby）─────────────────────────────────────────────────────
    liq_hist = liq_hist_prefetch
    if liq_hist:
        latest    = liq_hist[-1]
        long_liq  = float(latest.get("long_liquidation_usd",  0))
        short_liq = float(latest.get("short_liquidation_usd", 0))
        result["long_liq_1h"]  = round(long_liq,  2)
        result["short_liq_1h"] = round(short_liq, 2)
        total_liq = long_liq + short_liq
        if total_liq > 10_000:
            if long_liq > short_liq * 2:
                result["liq_signal"] = "heavy_long_liq"
            elif short_liq > long_liq * 2:
                result["liq_signal"] = "heavy_short_liq"
        result["_raw"]["liq"] = latest

    # ── 6. ✨ 巨鲸指数 ────────────────────────────────────────────────────────
    if whale_data:
        result["whale_index"] = float(whale_data[-1].get("whale_index_value", 0))
        result["_raw"]["whale"] = whale_data[-1]

    # ── 7. ✨ CVD ─────────────────────────────────────────────────────────────
    if cvd_data:
        latest_cvd = cvd_data[-1]
        buy_v  = float(latest_cvd.get("taker_buy_vol",  0))
        sell_v = float(latest_cvd.get("taker_sell_vol", 0))
        result["cvd_delta"] = round(buy_v - sell_v, 2)
        result["_raw"]["cvd"] = latest_cvd

    # ── 8. ✨ Coinbase 溢价 ───────────────────────────────────────────────────
    if cb_data:
        result["cb_premium"] = float(cb_data[-1].get("premium_rate", 0))
        result["_raw"]["cb_premium"] = cb_data[-1]

    _cache_set(key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 全市场资金费率情绪感知（v3.71）
# ═══════════════════════════════════════════════════════════════════════════════

def get_market_funding_sentiment() -> dict:
    """
    计算全市场资金费率分布，感知整体多空情绪。

    返回:
    {
        "pct_positive":         float,  # 正费率标的占比（0~1）
        "pct_negative":         float,  # 负费率标的占比
        "median_rate":          float,  # 中位数费率
        "extreme_positive_count": int,  # 费率 > 0.1% 的标的数
        "extreme_negative_count": int,  # 费率 < -0.1% 的标的数
        "sentiment":            str,    # "long_crowded" / "short_crowded" / "neutral"
        "error":                str,    # 失败时有值
    }
    """
    _EXTREME_THRESH = 0.001   # 0.1%

    try:
        from config import (
            MARKET_FUNDING_LONG_THRESH,
            MARKET_FUNDING_SHORT_THRESH,
        )
    except Exception:
        MARKET_FUNDING_LONG_THRESH  = 0.70
        MARKET_FUNDING_SHORT_THRESH = 0.30

    try:
        from shared import binance_api as _api
        all_mp = _api.get_all_mark_prices()
        if not all_mp:
            return {"error": "get_all_mark_prices 返回空"}

        # 过滤稳定币和无效数据
        _STABLE = {"USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDTUSDT", "FDUSDUSDT"}
        rates = [
            float(t.get("lastFundingRate", 0) or 0)
            for t in all_mp
            if isinstance(t, dict)
            and t.get("symbol", "").endswith("USDT")
            and t.get("symbol") not in _STABLE
            and t.get("lastFundingRate") is not None
        ]

        if not rates:
            return {"error": "无有效费率数据"}

        n = len(rates)
        pct_positive = sum(1 for r in rates if r > 0) / n
        pct_negative = sum(1 for r in rates if r < 0) / n
        sorted_rates = sorted(rates)
        median_rate  = sorted_rates[n // 2]
        extreme_pos  = sum(1 for r in rates if r > _EXTREME_THRESH)
        extreme_neg  = sum(1 for r in rates if r < -_EXTREME_THRESH)

        if pct_positive > MARKET_FUNDING_LONG_THRESH:
            sentiment = "long_crowded"
        elif pct_positive < MARKET_FUNDING_SHORT_THRESH:
            sentiment = "short_crowded"
        else:
            sentiment = "neutral"

        logger.debug(
            f"[clawby] 全市场资金费率情绪: {sentiment} "
            f"pct_pos={pct_positive:.0%} median={median_rate*100:.4f}% "
            f"extreme_pos={extreme_pos} extreme_neg={extreme_neg}"
        )

        return {
            "pct_positive":           round(pct_positive, 4),
            "pct_negative":           round(pct_negative, 4),
            "median_rate":            round(median_rate, 6),
            "extreme_positive_count": extreme_pos,
            "extreme_negative_count": extreme_neg,
            "sentiment":              sentiment,
        }
    except Exception as e:
        logger.warning(f"[clawby] get_market_funding_sentiment 失败: {e}")
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# 连通性测试
# ═══════════════════════════════════════════════════════════════════════════════

def test_connection() -> dict:
    try:
        r = requests.post(
            CLAWBY_BASE_URL,
            headers=CLAWBY_HEADERS,
            json={"name": "futures_liquidation_coin_list", "params": {"exchange": "Binance"}},
            timeout=15,
            proxies=_PROXIES,
        )
        if r.status_code == 401:
            return {"ok": False, "message": "401 未授权 — key 无效或套餐过期", "credits": {}}
        r.raise_for_status()
        d       = r.json()
        credits = d.get("credits", {})
        plan    = credits.get("plan", "?")
        rate    = credits.get("rate_limit", "?")
        return {
            "ok": True,
            "message": f"✅ Clawby 连接成功 | 套餐: {plan} | 速率上限: {rate}/min",
            "credits": credits,
        }
    except requests.exceptions.Timeout:
        return {"ok": False, "message": "⏱️ 请求超时（代理/网络问题）", "credits": {}}
    except Exception as e:
        return {"ok": False, "message": f"❌ 连接失败: {e}", "credits": {}}


# ═══════════════════════════════════════════════════════════════════════════════
# 命令行测试
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys, json
    logging.basicConfig(level=logging.INFO)
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"

    print("=" * 60)
    conn = test_connection()
    print(f"[连通性] {conn['message']}")
    if not conn["ok"]:
        sys.exit(1)

    tests = [
        ("热力图",        lambda: get_liq_heatmap(sym)),
        ("爆仓历史",      lambda: _get_liq_history(sym)),
        ("巨鲸指数",      lambda: get_whale_index(sym)),
        ("CVD",           lambda: get_cvd(sym)),
        ("Coinbase溢价",  lambda: get_coinbase_premium()),
        ("贪婪恐惧",      lambda: get_fear_greed()),
        ("布林带",        lambda: get_boll(sym)),
        ("费率套利榜",    lambda: get_funding_arb_opps(3)),
        ("BTC ETF流入",   lambda: get_btc_etf_flow(3)),
        ("HL巨鲸警报",    lambda: get_hl_whale_alerts(3)),
        ("HL巨鲸持仓",    lambda: get_hl_whale_positions(3)),
        ("山寨季指数",    lambda: get_altcoin_season()),
        ("宏观日历",      lambda: get_economic_calendar(3)),
        ("牛市顶峰",      lambda: get_bull_market_peak()),
    ]

    for name, fn in tests:
        try:
            result = fn()
            ok = bool(result)
            icon = "✅" if ok else "⚠️ "
            preview = json.dumps(result, ensure_ascii=False)[:80] if ok else "空数据"
            print(f"  {icon} {name}: {preview}")
        except Exception as e:
            print(f"  ❌ {name}: {e}")

    print("=" * 60)
