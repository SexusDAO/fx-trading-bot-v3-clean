"""
shared/binance_api.py — 所有币安 API 请求唯一出口

规则：其他文件禁止直接调用 Binance SDK，全部通过本模块。
重试逻辑：失败等 1 秒重试，最多 3 次；3 次全失败记录错误日志，不向上抛异常。
"""

import time
import uuid
import threading
import logging
import os
import sys
from pathlib import Path

# 读取 API Key（从 workspace TOOLS.md 注入）
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
try:
    from binance.um_futures import UMFutures  # binance-futures-connector
    _SDK = "connector"
except ImportError:
    UMFutures = None
    _SDK = "rest"

import requests as _requests

logger = logging.getLogger("fx.binance_api")

# ── 从 .env 或系统环境变量加载敏感配置 ──────────────────────────────────────
# 优先级：系统环境变量 > fx/.env 文件 > 上级目录 .env
def _load_env_file():
    """简单的 .env 解析，不依赖 python-dotenv 第三方库。"""
    _fx_root = Path(__file__).parent.parent
    for _env_path in [_fx_root / ".env", _fx_root.parent / ".env"]:
        if _env_path.exists():
            try:
                with open(_env_path) as _f:
                    for _line in _f:
                        _line = _line.strip()
                        if not _line or _line.startswith("#") or "=" not in _line:
                            continue
                        _k, _, _v = _line.partition("=")
                        _k = _k.strip()
                        _v = _v.strip().strip('"').strip("'")
                        if _k:
                            os.environ[_k] = _v  # .env 文件优先（每次重载都覆盖）
            except Exception:
                pass

_load_env_file()

# ── API Key ──────────────────────────────────────────────────────────────────
API_KEY    = os.environ.get("BINANCE_API_KEY", "")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
if not API_KEY or not API_SECRET:
    logger.warning("[binance_api] BINANCE_API_KEY / BINANCE_API_SECRET 未设置，签名接口将失败")
BASE_URL   = "https://fapi.binance.com"

# ── 代理配置 ─────────────────────────────────────────────────────────────────
# 签名请求（账户/下单）必须走代理（API key IP 白名单绑定代理出口 IP）
# 公开行情接口（mark_price / klines / funding rate 等）直连即可，节省代理配额
PROXY_URL  = (os.environ.get("BINANCE_PROXY") or os.environ.get("PROXY_URL") or "").strip()   # 未设置 → 直连
PROXIES    = ({"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else {})
# 直连 Session（行情接口用）
PROXIES_DIRECT = {}

def _make_session(use_proxy: bool = True) -> _requests.Session:
    """创建 requests Session。
    use_proxy=True  → 走 SOCKS5 代理（签名接口）
    use_proxy=False → 直连（公开行情接口）
    连接池扩到 30，避免并发扫描时 pool is full 警告。
    """
    s = _requests.Session()
    adapter = _requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=30)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    if use_proxy and PROXIES:
        s.proxies.update(PROXIES)
    s.headers["Accept-Encoding"] = "identity"  # 禁用压缩，避免urllib3 decompress持GIL阻塞事件循环
    return s

_SESSION        = _make_session(use_proxy=True)   # 签名接口（代理）
_SESSION_DIRECT = _make_session(use_proxy=False)  # 公开行情（直连）
_PROXY_HEALTHY  = True   # 代理健康标志；False 时签名接口也降级直连（告警）

_SESSION_LOCK        = threading.Lock()   # [v3.89 #86] 保护 Session 替换过程，多线程同时 reset 只有一个在换
_SESSION_RESET_COUNT = 0                  # [v3.89 #86] 累计重建次数（排查连接池反复重建用）

def reset_session():
    """
    重建 Session（代理配置变更后 / 连接类错误后调用）。
    [v3.89 #86] 加锁串行化替换；先创建新会话并替换引用，再关闭旧会话释放连接池。
    说明：旧会话上仍在途的请求在 close() 后可能报连接错误，由 _retry 的重试路径兜底。
    """
    global _SESSION, _SESSION_DIRECT, _PROXY_HEALTHY, _SESSION_RESET_COUNT
    with _SESSION_LOCK:
        old_sessions    = (_SESSION, _SESSION_DIRECT)
        _SESSION        = _make_session(use_proxy=True)
        _SESSION_DIRECT = _make_session(use_proxy=False)
        _PROXY_HEALTHY  = True
        _SESSION_RESET_COUNT += 1
        count = _SESSION_RESET_COUNT
        for _old in old_sessions:
            try:
                _old.close()
            except Exception as e:
                logger.warning(f"[api] 关闭旧 Session 失败（忽略）: {e}")
    logger.info(f"[api] Session 已重建（累计第 {count} 次），旧会话已关闭")

# ── 内部重试装饰器 ────────────────────────────────────────────────────────────
def fmt_num(value) -> str:
    """[fix #25] 统一数字格式化：定点 8 位小数并去掉末尾 0 / 小数点，杜绝科学计数法（如 1e-05）。"""
    out = f"{float(value):.8f}".rstrip("0").rstrip(".")
    return out if out not in ("", "-") else "0"


class BinanceAPIError(Exception):
    """[fix #9] 带原始错误码的业务错误。str() 保持 "BinanceError code=.. msg=.. http=.." 旧格式，
    旧代码里 `"-4024" in str(e)` 的判断继续有效。"""

    def __init__(self, code, msg="", http=None):
        self.code = code
        self.msg = msg
        self.http = http
        super().__init__(f"BinanceError code={code} msg={msg} http={http}")


def _is_business_error(e: Exception) -> bool:
    """业务拒绝（参数/余额/价格/持仓状态等，重试无意义）：Binance 负数错误码，
    但 -1000/-1001/-1003/-1006/-1007/-1008/-1015/-1021 属服务端繁忙/限频/时间戳类，不算业务拒绝。"""
    code = getattr(e, "code", None)
    try:
        code = int(code)
    except (TypeError, ValueError):
        return False
    return code < 0 and code not in (-1000, -1001, -1003, -1006, -1007, -1008, -1015, -1021)


def _is_rate_limited(e: Exception) -> bool:
    code = getattr(e, "code", None)
    http = getattr(e, "http", None)
    return code in (-1003, "-1003") or http in (429, "429")


class WriteTimeoutError(Exception):
    """[v3.89 #83] 写请求超时/断连，交易所是否已受理未知，需调用方对账，禁止盲目重发。"""


# 写请求「结果未知」的异常特征（请求可能已送达）。ConnectTimeout / NewConnectionError 表示
# 连接未建立、请求必然未送达，不在此列，仍可安全重试。
_UNKNOWN_RESULT_MARKERS = ("ReadTimeout", "Read timed out", "RemoteDisconnected",
                           "timeout", "Timeout", "timed out", "Connection aborted")
_NOT_SENT_MARKERS = ("ConnectTimeout", "NewConnectionError", "Connection refused",
                     "Failed to establish a new connection")


def _record_api_outcome(success: bool) -> None:
    """
    [v3.89 #64] 把一次 _retry 调用的最终结果记入 risk_manager 的失败率统计（唯一统计入口）：
      成功返回 → True；重试耗尽 → False（每次调用只记一次，不按每次尝试记）。
    suppress_codes 命中的静默返回、写请求「结果未知」抛 WriteTimeoutError 都不计入。
    延迟导入并吞掉一切异常：统计失败绝不能影响请求本身。
    """
    try:
        from shared.risk_manager import record_api_call
        record_api_call(success)
    except Exception:
        pass


def _cfg(name, default):
    try:
        import config as _c
        return getattr(_c, name, default)
    except Exception:
        return default


def _api_timeout():
    return tuple(_cfg("API_REQUEST_TIMEOUT", (3, 5)))


def _order_timeout():
    return tuple(_cfg("ORDER_REQUEST_TIMEOUT", (5, 10)))


_ORDER_PATHS = ("/fapi/v1/order", "/fapi/v1/batchOrders", "/fapi/v1/algoOrder")


def _retry(fn, *args, retries=None, suppress_codes=None, is_write: bool = False, raise_business: bool = False, **kwargs):
    """
    suppress_codes: 包含这些 Binance 错误码时不打 ERROR，直接返回 None
    连接类错误（SOCKS/Timeout）自动重建 Session 后重试，最多降级一次。
    is_write: [v3.89 #83] 写请求（下单）。遇到「结果未知」的超时/断连时不重试，
              抛 WriteTimeoutError 由调用方对账；默认 False，读请求行为不变。
    """
    global _PROXY_HEALTHY
    if retries is None:
        retries = 1 + max(int(_cfg("API_REQUEST_MAX_RETRY", 1)), 0)   # 总尝试次数 = 1 + 最大重试次数
    last_err = None
    for attempt in range(retries):
        try:
            _res = fn(*args, **kwargs)
            _record_api_outcome(True)
            return _res
        except Exception as e:
            last_err = e
            err_str = str(e)
            # [fix #9/#24a] 业务拒绝（负数错误码，如 -4024/-2019/-2022）：不重试，保留原始错误码。
            #   写请求直接抛 BinanceAPIError 让调用方按错误码处理；读请求仍返回 None（旧行为）。
            #   suppress_codes 命中的仍按原逻辑静默返回 None。
            if isinstance(e, BinanceAPIError) and _is_business_error(e):
                if suppress_codes and any(str(c) == str(e.code) for c in suppress_codes):
                    return None
                _record_api_outcome(True)    # 交易所已正常响应，不计入链路失败率
                logger.warning(f"[api] 业务拒绝，不重试: {err_str[:160]}")
                if is_write or raise_business:
                    raise
                return None
            # [fix #24a] 限频（429/-1003）：退避后重试；418（IP 封禁）不重试
            if isinstance(e, BinanceAPIError) and getattr(e, "http", None) in (418, "418"):
                logger.error(f"[api] IP 被临时封禁(418)，不重试: {err_str[:160]}")
                break
            if isinstance(e, BinanceAPIError) and _is_rate_limited(e):
                logger.warning(f"[api] 触发限频，退避重试: {err_str[:120]}")
                if attempt < retries - 1:
                    time.sleep(2.0 * (attempt + 1))
                    continue
            # 静默抑制指定错误码
            if suppress_codes and any(
                str(c) in err_str or f"code={c}" in err_str
                for c in suppress_codes
            ):
                return None
            # [v3.89 #83] 写请求超时：请求可能已送达交易所，不重试，交给调用方对账
            if (is_write
                    and any(x in err_str for x in _UNKNOWN_RESULT_MARKERS)
                    and not any(x in err_str for x in _NOT_SENT_MARKERS)):
                logger.warning(f"[api] 写请求超时/断连，结果未知，不重试: {err_str[:120]}")
                reset_session()
                raise WriteTimeoutError(err_str[:200])
            # 连接类错误（代理故障/超时）→ 重建 Session 后继续重试
            is_conn_err = any(x in err_str for x in (
                "SOCKS", "ConnectionError", "NewConnectionError",
                "Max retries exceeded", "RemoteDisconnected", "timeout", "Timeout"
            ))
            if is_conn_err and attempt == 0:
                logger.warning(f"[api] 代理连接异常，重建 Session 后重试: {err_str[:120]}")
                reset_session()
                _PROXY_HEALTHY = False
                time.sleep(0.5)
                continue
            logger.warning(f"[api] _call 第{attempt+1}次失败: {e}")
            if attempt < retries - 1:
                time.sleep(1)
    _record_api_outcome(False)   # [v3.89 #64] 重试耗尽，记一次失败
    logger.error(f"[api] _call 全部重试失败: {last_err}")
    return None

_DIRECT_DISABLED = False   # 直连一旦失败，本进程内全部走代理，避免每次重试都双发

def _get(path, params=None):
    """公开行情接口，优先直连，直连失败后本进程内全部走代理（节省代理配额+避免重复双发）。"""
    global _DIRECT_DISABLED
    if not _DIRECT_DISABLED:
        try:
            r = _SESSION_DIRECT.get(f"{BASE_URL}{path}", params=params, timeout=_api_timeout())
            r.raise_for_status()
            return r.json()
        except Exception as _e:
            # 直连失败（网络隔离环境），本进程内标记为不可用，后续直接走代理
            logger.debug(f"[api] 直连失败，本进程内切换为全代理模式: {_e}")
            _DIRECT_DISABLED = True
    r = _SESSION.get(f"{BASE_URL}{path}", params=params, timeout=_api_timeout())
    r.raise_for_status()
    return r.json()

def _signed_post(path, params: dict):
    import hmac, hashlib, urllib.parse
    # 每次调用重新生成 timestamp 和 signature，避免重试时签名失效
    clean = {k: v for k, v in params.items() if k not in ("timestamp", "signature")}
    clean["timestamp"] = int(time.time() * 1000)
    qs = urllib.parse.urlencode(clean)
    clean["signature"] = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": API_KEY}
    r = _SESSION.post(f"{BASE_URL}{path}", params=clean, headers=headers,
                      timeout=_order_timeout() if path in _ORDER_PATHS else _api_timeout())
    if not r.ok:
        try:
            body = r.json()
            code = body.get("code", "")
            msg  = body.get("msg", "")
            raise BinanceAPIError(code, msg, r.status_code)
        except BinanceAPIError:
            raise
        except Exception:
            r.raise_for_status()
    return r.json()

def _signed_delete(path, params: dict):
    import hmac, hashlib, urllib.parse
    # 每次调用重新生成 timestamp 和 signature，避免重试时签名被旧字段污染（对齐 _signed_post）
    clean = {k: v for k, v in params.items() if k not in ("timestamp", "signature")}
    clean["timestamp"] = int(time.time() * 1000)
    qs = urllib.parse.urlencode(clean)
    clean["signature"] = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": API_KEY}
    r = _SESSION.delete(f"{BASE_URL}{path}", params=clean, headers=headers, timeout=_api_timeout())
    r.raise_for_status()
    return r.json()

def _signed_get(path, params: dict):
    import hmac, hashlib, urllib.parse
    # 每次调用重新生成 timestamp 和 signature，避免重试时签名被旧字段污染（对齐 _signed_post）
    clean = {k: v for k, v in params.items() if k not in ("timestamp", "signature")}
    clean["timestamp"] = int(time.time() * 1000)
    qs = urllib.parse.urlencode(clean)
    clean["signature"] = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": API_KEY}
    r = _SESSION.get(f"{BASE_URL}{path}", params=clean, headers=headers, timeout=_api_timeout())
    r.raise_for_status()
    return r.json()

# ── 公开接口 ──────────────────────────────────────────────────────────────────

def _kline_cfg():
    try:
        import config as _c
        to = tuple(getattr(_c, "KLINE_REQUEST_TIMEOUT", (3, 5)))
        return (float(to[0]), float(to[1])), int(getattr(_c, "KLINE_REQUEST_MAX_RETRY", 1)), \
            float(getattr(_c, "KLINE_REQUEST_TOTAL_SEC", 8.0))
    except Exception:
        return (3.0, 5.0), 1, 8.0


_KLINE_RETRY_SLEEP = 0.5   # 重试间隔（秒）


def get_klines(symbol: str, interval: str, limit: int = 100) -> list:
    """拉 K 线，返回 list[list]，失败返回 []。

    单次调用总耗时有上限（KLINE_REQUEST_TOTAL_SEC，含重试与重试间隔）：
    每次 HTTP 请求用 (connect, read) 超时，并被剩余预算截断；重试最多 KLINE_REQUEST_MAX_RETRY 次，间隔 0.5s。
    其他接口仍走 _retry / _get，不受影响。
    """
    global _DIRECT_DISABLED
    (t_conn, t_read), max_retry, total = _kline_cfg()
    deadline = time.monotonic() + total
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    url = f"{BASE_URL}/fapi/v1/klines"
    last_err = None
    for attempt in range(max_retry + 1):
        # 本次尝试内：直连失败后再试代理，同样受剩余预算约束
        for use_direct in ((True, False) if not _DIRECT_DISABLED else (False,)):
            remaining = deadline - time.monotonic()
            if remaining <= 0.05:
                break
            tmo = (min(t_conn, remaining), min(t_read, remaining))
            try:
                r = (_SESSION_DIRECT if use_direct else _SESSION).get(url, params=params, timeout=tmo)
                r.raise_for_status()
                d = r.json()
                _record_api_outcome(True)
                return d if isinstance(d, list) else []
            except Exception as e:
                last_err = e
                if use_direct:
                    logger.debug(f"[api] K线直连失败，本进程内切换为全代理模式: {e}")
                    _DIRECT_DISABLED = True
        if attempt < max_retry and deadline - time.monotonic() > _KLINE_RETRY_SLEEP + 0.05:
            time.sleep(_KLINE_RETRY_SLEEP)
        else:
            break
    _record_api_outcome(False)
    logger.warning(f"[api] get_klines {symbol} {interval} 失败/超时(总时限{total}s): {last_err}")
    return []

def get_ticker(symbol: str) -> dict:
    """拉单标的 24H ticker，失败返回 {}"""
    def _call():
        return _get("/fapi/v1/ticker/24hr", {"symbol": symbol}) or {}
    return _retry(_call) or {}

def get_ticker_24h(symbol: str = None) -> list:
    """拉全市场或单标的 24H ticker 列表"""
    def _call():
        params = {"symbol": symbol} if symbol else {}
        d = _get("/fapi/v1/ticker/24hr", params)
        return d if isinstance(d, list) else [d]
    return _retry(_call) or []


_ticker_cache: dict = {}          # symbol -> ticker dict
_ticker_cache_ts: float = 0.0     # 最后全量更新时间戳

def get_ticker_24h_cached(max_age_sec: float = 300) -> dict:
    """
    返回全量 ticker dict，格式为 {symbol: ticker_dict}。
    max_age_sec 秒内直接返回缓存，超时后重新拉取。
    用于扫描各轮短时间内多次读取，避免重复 HTTP 请求。
    """
    global _ticker_cache, _ticker_cache_ts
    now = __import__('time').time()
    if now - _ticker_cache_ts < max_age_sec and _ticker_cache:
        return _ticker_cache
    tickers = get_ticker_24h()   # 拉全量
    if tickers:
        _ticker_cache = {t["symbol"]: t for t in tickers if "symbol" in t}
        _ticker_cache_ts = now
    return _ticker_cache

def get_mark_price(symbol: str) -> dict:
    """拉标记价格 + 资金费率，失败返回 {}"""
    def _call():
        return _get("/fapi/v1/premiumIndex", {"symbol": symbol}) or {}
    return _retry(_call) or {}

def get_order_book(symbol: str, limit: int = 20) -> dict:
    """拉订单簿，失败返回 {}"""
    def _call():
        return _get("/fapi/v1/depth", {"symbol": symbol, "limit": limit}) or {}
    return _retry(_call) or {}

def get_open_interest(symbol: str) -> dict:
    """拉未平仓量，失败返回 {}。400 错误（如 EPICUSDT 不支持）静默处理不重试。"""
    def _call():
        return _get("/fapi/v1/openInterest", {"symbol": symbol}) or {}
    return _retry(_call, suppress_codes={400, -1121}) or {}

def get_oi_history(symbol: str, period: str = "1h", limit: int = 24) -> list:
    """
    拉 OI 历史时序（v3.58）。
    period: 5m/15m/30m/1h/2h/4h/6h/12h/1d
    limit:  最多 500 条，默认 24 根（24H）
    返回 list of {"symbol", "sumOpenInterest", "sumOpenInterestValue", "timestamp"}
    失败返回 []。400 错误静默处理不重试。
    """
    def _call():
        return _get("/futures/data/openInterestHist", {
            "symbol": symbol,
            "period": period,
            "limit":  limit,
        }) or []
    return _retry(_call, suppress_codes={400, -1121}) or []

# ── 账户持仓模式缓存（避免重复查询）─────────────────────────────────────────
_HEDGE_MODE: bool = True  # 确认该账号为 Hedge Mode，直接确定，不再动态查询

def is_hedge_mode() -> bool:
    """Hedge Mode 确认函数（该账号确认为 Hedge Mode）"""
    return _HEDGE_MODE


def get_position_mode() -> bool:
    """[fix #16] 向交易所查询账户真实持仓模式。True=对冲模式(dualSidePosition)；失败抛 ApiQueryError。"""
    def _call():
        d = _signed_get("/fapi/v1/positionSide/dual", {})
        if not isinstance(d, dict) or "dualSidePosition" not in d:
            raise ApiQueryError(f"positionSide/dual 返回异常: {d!r}"[:200])
        return d
    res = _retry(_call)
    if res is None:
        raise ApiQueryError("get_position_mode 查询失败（重试耗尽）")
    v = res.get("dualSidePosition")
    return v is True or str(v).lower() == "true"


def _validate_sl_order(
    order_type: str, side: str,
    stop_price: float, entry_price: float | None,
    atr: float | None,
    position_side: str | None,
) -> str | None:
    """
    止损单最后一道防线验证。
    返回 None 表示通过；返回错误字符串表示拒绝。

    规则：
      LONG 止损（STOP/STOP_MARKET + SELL 或 position_side=LONG + SELL）
        → stop_price < entry_price
        → entry_price - stop_price > atr * 0.5（止损不能太近）
      SHORT 止损（STOP/STOP_MARKET + BUY 或 position_side=SHORT + BUY）
        → stop_price > entry_price
        → stop_price - entry_price > atr * 0.5

    仅在以下条件都成立时执行校验：
      1. order_type 包含 STOP（STOP / STOP_MARKET）
      2. entry_price 和 atr 均被传入且合法（>0）
    """
    if "STOP" not in order_type.upper():
        return None  # 非止损单，跳过
    if not entry_price or entry_price <= 0:
        return None  # 没有参考入场价，无法校验
    if not stop_price or stop_price <= 0:
        return "止损价不合法（<=0）"
    if not atr or atr <= 0:
        # 没有 ATR 时只做方向验证，不做距离验证
        atr_check = False
    else:
        atr_check = True

    # 判断方向
    ps = (position_side or "").upper()
    s  = (side or "").upper()
    if ps == "LONG" or (s == "SELL" and ps != "SHORT"):
        direction = "LONG"
    elif ps == "SHORT" or (s == "BUY" and ps != "LONG"):
        direction = "SHORT"
    else:
        return None  # 无法确定方向，跳过

    if direction == "LONG":
        if stop_price >= entry_price:
            return (f"LONG止损价 {stop_price} >= 入场价 {entry_price}，"
                    f"方向错误，拒绝挂单")
        if atr_check:
            dist = entry_price - stop_price
            if dist <= atr * 0.5:
                return (f"LONG止损距离 {dist:.4f} <= ATR×0.5={atr*0.5:.4f}，"
                        f"止损太近（可能是计算错误），拒绝挂单")
    else:  # SHORT
        if stop_price <= entry_price:
            return (f"SHORT止损价 {stop_price} <= 入场价 {entry_price}，"
                    f"方向错误，拒绝挂单")
        if atr_check:
            dist = stop_price - entry_price
            if dist <= atr * 0.5:
                return (f"SHORT止损距离 {dist:.4f} <= ATR×0.5={atr*0.5:.4f}，"
                        f"止损太近（可能是计算错误），拒绝挂单")

    return None  # 通过


def _num_close(a, b, rel: float = 1e-4) -> bool:
    """[v3.89 #83] 数值近似相等（相对误差），任一为 None 视为不相等。"""
    fa, fb = _to_float(a), _to_float(b)
    if fa is None or fb is None:
        return False
    return abs(fa - fb) <= max(abs(fb) * rel, 1e-12)


def _find_unique_open_order(open_orders: list, spec: dict, since_ms: int, exclude_ids=None):
    """
    [v3.89 #83] 在挂单列表中按 symbol/side/type/数量/价格/触发价/positionSide 匹配本次提交的订单。
    只认领「提交时间之后创建」的订单，且必须唯一；0 个或多个匹配均返回 None。
    返回 (order_or_None, 匹配个数)。
    """
    exclude_ids = exclude_ids or set()
    hits = []
    for o in open_orders or []:
        if o.get("orderId") in exclude_ids:
            continue
        if o.get("symbol") != spec.get("symbol") or o.get("side") != spec.get("side"):
            continue
        if o.get("type") != spec.get("type"):
            continue
        if not _num_close(o.get("origQty"), spec.get("quantity")):
            continue
        if spec.get("price") not in (None, "", 0, "0") and not _num_close(o.get("price"), spec.get("price")):
            continue
        if spec.get("stopPrice") not in (None, "", 0, "0") and not _num_close(o.get("stopPrice"), spec.get("stopPrice")):
            continue
        if spec.get("positionSide") and o.get("positionSide") not in (None, spec.get("positionSide")):
            continue
        t = _to_float(o.get("time") if o.get("time") is not None else o.get("updateTime"))
        if t is not None and t < since_ms:
            continue   # 提交之前就存在的同参数订单，不是本次的
        hits.append(o)
    return (hits[0] if len(hits) == 1 else None), len(hits)


def place_order(symbol: str, side: str, order_type: str,
                quantity: float, price: float = None,
                stop_price: float = None, reduce_only: bool = False,
                timeInForce: str = "GTC",
                time_in_force: str = None,  # 已废弃别名，兼容旧调用方
                position_side: str = None,  # LONG/SHORT/BOTH；None=自动判断
                entry_price: float = None,  # 止损验证用：对应开仓价
                atr: float = None,          # 止损验证用：当前 ATR
                **extra) -> dict:
    """
    下单统一入口。
    order_type: LIMIT / MARKET / STOP_MARKET / TAKE_PROFIT_MARKET
    Hedge Mode 下必须传 positionSide；单向模式可省略。
    失败返回 {}

    对止损单（STOP / STOP_MARKET）额外执行方向+距离验证：
      - 如传入 entry_price + atr，则强制校验，不通过返回 {"error": ...}
    """
    # ── 止损单验证（最后一道防线） ──────────────────────────────────────────────────
    if "STOP" in order_type.upper() and stop_price:
        err = _validate_sl_order(
            order_type=order_type,
            side=side,
            stop_price=float(stop_price),
            entry_price=float(entry_price) if entry_price else None,
            atr=float(atr) if atr else None,
            position_side=position_side,
        )
        if err:
            logger.error(f"[place_order] 止损验证失败 [{symbol}]: {err}")
            return {"error": err}

    params = {
        "symbol":    symbol,
        "side":      side,
        "type":      order_type,
        "quantity":  quantity,
    }
    params["quantity"] = fmt_num(quantity)          # [fix #25] 避免浮点科学计数法
    if price:
        params["price"] = fmt_num(price)
    if stop_price:
        params["stopPrice"] = fmt_num(stop_price)

    # [fix #26] 对冲模式下的平仓单（reduceOnly 语义）必须显式指定 positionSide=LONG/SHORT。
    #   对冲模式不发 reduceOnly，若缺少 positionSide 交易所会按错误方向处理，可能反向开仓。
    hedge = is_hedge_mode()
    if hedge and reduce_only and str(position_side or "").upper() not in ("LONG", "SHORT"):
        logger.error(f"[place_order] {symbol} 对冲模式平仓单缺少 positionSide，拒绝下单")
        return {"error": "hedge 模式下平仓单必须显式指定 positionSide=LONG/SHORT"}

    if hedge:
        # 明确传入的 position_side 优先；否则根据 side+reduceOnly 推断
        if position_side:
            params["positionSide"] = position_side
        elif reduce_only:
            # 平仓：BUY→平空(SHORT)，SELL→平多(LONG)
            params["positionSide"] = "SHORT" if side == "BUY" else "LONG"
        else:
            # 开仓：BUY→做多(LONG)，SELL→做空(SHORT)
            params["positionSide"] = "LONG" if side == "BUY" else "SHORT"
        # Hedge Mode 不支持 reduceOnly 参数
    else:
        if reduce_only:
            params["reduceOnly"] = "true"

    if order_type == "LIMIT":
        params["timeInForce"] = time_in_force or timeInForce

    # 注：已删除条件单降级逻辑（STOP_MARKET→LIMIT 在闪崩时无法成交，有害无益）
    # 本账号止损走 place_sl（/fapi/v1/algoOrder），TP 走 TAKE_PROFIT_MARKET，均不经此路径

    params.update(extra)
    # [fix #24b] 幂等：每笔下单带 clientOrderId（uuid），任何重发/对账都先按它查单
    params.setdefault("newClientOrderId", "fx" + uuid.uuid4().hex[:30])
    _cid = params["newClientOrderId"]

    def _call():
        # 重发前先按 clientOrderId 查单：已被交易所接收则直接返回，避免重复下单
        if _call.sent:
            try:
                _o = _signed_get("/fapi/v1/order", {"symbol": symbol, "origClientOrderId": _cid})
                if isinstance(_o, dict) and _o.get("orderId"):
                    logger.warning(f"[place_order] {symbol} 重发前按 clientOrderId 查到已提交订单 {_o.get('orderId')}，不重复下单")
                    return _o
            except Exception:
                pass   # -2013 订单不存在：确认未提交，继续发送
        _call.sent = True
        return _signed_post("/fapi/v1/order", params)
    _call.sent = False
    # [v3.89 #83] 写请求超时不盲目重发：先查挂单对账，找到唯一匹配视为已提交
    since_ms = int(time.time() * 1000) - 5000   # 留 5 秒本地时钟偏差余量
    try:
        result = _retry(_call, is_write=True)
    except WriteTimeoutError as e:
        try:
            _o = _signed_get("/fapi/v1/order", {"symbol": symbol, "origClientOrderId": _cid})
            if isinstance(_o, dict) and _o.get("orderId"):
                logger.warning(f"[place_order] {symbol} 写超时，按 clientOrderId 对账找到订单 orderId={_o.get('orderId')} status={_o.get('status')}")
                return _o
        except Exception:
            pass
        try:
            found, n = _find_unique_open_order(get_open_orders(symbol), params, since_ms)
        except Exception as qe:
            found, n = None, -1
            logger.warning(f"[place_order] {symbol} 写超时后查询挂单失败: {qe}")
        if found:
            logger.warning(f"[place_order] {symbol} 写请求超时，对账找到已提交订单 orderId={found.get('orderId')}")
            return found
        logger.warning(
            f"[place_order] {symbol} 写请求超时（{e}），挂单中未找到唯一匹配订单(匹配数={n})，"
            f"结果未知，按未提交返回；MARKET/已成交单不会出现在挂单里，请调用方核对持仓"
        )
        return {}
    return result or {}

def cancel_order(symbol: str, order_id: int) -> dict:
    """撤单，失败返回 {}"""
    def _call():
        return _signed_delete("/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
    return _retry(_call) or {}

class ApiQueryError(RuntimeError):
    """查询类接口失败（重试耗尽 / 返回格式异常）。

    [fix #2] 区分「交易所确实没有持仓/挂单（返回 []）」与「查询失败（抛本异常）」，
    调用方不得把失败当作空结果处理。
    """


def get_position(symbol: str = None) -> list:
    """查当前持仓。成功返回列表（可为空 = 交易所确实无持仓）；失败抛 ApiQueryError。"""
    def _call():
        params = {}
        if symbol:
            params["symbol"] = symbol
        d = _signed_get("/fapi/v2/positionRisk", params)
        if not isinstance(d, list):
            raise ApiQueryError(f"positionRisk 返回格式异常: {type(d).__name__}")
        return d
    res = _retry(_call)
    if res is None:
        raise ApiQueryError("get_position 查询失败（重试耗尽）")
    return res

def get_account() -> dict:
    """查账户余额，失败返回 {}"""
    def _call():
        return _signed_get("/fapi/v2/account", {}) or {}
    return _retry(_call) or {}


def get_order(symbol: str, order_id: int) -> dict:
    """查询单笔订单状态（用于 WS 超时后 REST 确认成交）。
    [fix #46] 成功返回订单 dict；查询失败（重试耗尽/格式异常）抛 ApiQueryError，
    让调用方区分「查到了」与「查询本身失败」。"""
    def _call():
        d = _signed_get("/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
        if not isinstance(d, dict):
            raise ApiQueryError(f"order 返回格式异常: {type(d).__name__}")
        return d
    res = _retry(_call)
    if res is None:
        raise ApiQueryError(f"get_order({symbol},{order_id}) 查询失败（重试耗尽）")
    return res


def get_open_orders(symbol: str = None) -> list:
    """查所有未成交挂单。成功返回列表（可为空）；失败抛 ApiQueryError。"""
    def _call():
        params = {}
        if symbol:
            params["symbol"] = symbol
        d = _signed_get("/fapi/v1/openOrders", params)
        if not isinstance(d, list):
            raise ApiQueryError(f"openOrders 返回格式异常: {type(d).__name__}")
        return d
    res = _retry(_call)
    if res is None:
        raise ApiQueryError("get_open_orders 查询失败（重试耗尽）")
    return res

def get_user_trades(symbol: str, start_time: int = None, limit: int = 20) -> list:
    """查某标的最近成交记录（含 realizedPnl），失败返回 []"""
    def _call():
        params: dict = {"symbol": symbol, "limit": limit}
        if start_time:
            params["startTime"] = start_time
        d = _signed_get("/fapi/v1/userTrades", params)
        return d if isinstance(d, list) else []
    return _retry(_call) or []

def set_leverage(symbol: str, leverage: int) -> dict:
    """设置杠杆，失败返回 {}"""
    def _call():
        return _signed_post("/fapi/v1/leverage",
                            {"symbol": symbol, "leverage": leverage}) or {}
    return _retry(_call) or {}

def set_margin_type(symbol: str, margin_type: str) -> dict:
    """设置保证金模式（ISOLATED/CROSSED）；-4046 表示已是目标模式，视为成功"""
    def _call():
        return _signed_post("/fapi/v1/marginType",
                            {"symbol": symbol, "marginType": margin_type}) or {}
    # suppress_codes=-4046: "No need to change margin type" 不打 ERROR 日志
    result = _retry(_call, suppress_codes={-4046})
    return result if result else {"msg": "already_set"}


def get_all_mark_prices() -> list:
    """拉全市场标记价格+资金费率，失败返回 []"""
    def _call():
        d = _get("/fapi/v1/premiumIndex")
        return d if isinstance(d, list) else []
    return _retry(_call) or []

_funding_info_cache: dict = {}   # symbol → {interval_h, ts}
_FUNDING_INFO_TTL = 3600 * 6     # 6小时刷新一次

def get_funding_interval(symbol: str) -> int:
    """
    返回标的的资金费率结算周期（小时），典型值 1/4/8。
    数据来源：/fapi/v1/fundingInfo（带全量缓存，6小时TTL）。
    失败时默认返回 8。
    """
    import time as _t
    now = _t.time()
    cached = _funding_info_cache.get(symbol)
    if cached and now - cached["ts"] < _FUNDING_INFO_TTL:
        return cached["interval_h"]
    try:
        data = _retry(lambda: _get("/fapi/v1/fundingInfo")) or []
        if isinstance(data, list):
            for item in data:
                sym = item.get("symbol", "")
                h   = int(item.get("fundingIntervalHours", 8) or 8)
                _funding_info_cache[sym] = {"interval_h": h, "ts": now}
        return _funding_info_cache.get(symbol, {}).get("interval_h", 8)
    except Exception:
        return 8


def get_next_funding_time(symbol: str) -> float:
    """
    返回标的下次资金费率结算的 Unix 时间戳（秒）。
    优先用 premiumIndex 的 nextFundingTime 字段（最精确）。
    失败时用 fundingInterval 推算。
    """
    import time as _t
    try:
        d = _retry(lambda: _get("/fapi/v1/premiumIndex", {"symbol": symbol})) or {}
        nft = d.get("nextFundingTime")
        if nft and int(nft) > 0:
            return int(nft) / 1000.0
    except Exception:
        pass
    interval_h = get_funding_interval(symbol)
    now = _t.time()
    interval_s = interval_h * 3600
    return now + (interval_s - now % interval_s)


def get_open_interest_hist(symbol: str, period: str = "1h", limit: int = 2) -> list:
    """
    拉 OI 历史（用于计算24H前OI）。
    返回列表，最新在最后。失败返回 []。400 错误静默处理不重试。
    """
    def _call():
        d = _get("/futures/data/openInterestHist",
                 {"symbol": symbol, "period": period, "limit": limit})
        return d if isinstance(d, list) else []
    return _retry(_call, suppress_codes={400, -1121}) or []


def get_exchange_info() -> dict:
    """拉取 exchangeInfo，含所有标的的 filters（tickSize/stepSize 等）。失败返回 {}"""
    def _call():
        return _get("/fapi/v1/exchangeInfo")
    return _retry(_call) or {}


def get_leverage_brackets(symbol: str) -> list:
    """
    查询该标的的杠杆档位列表（GET /fapi/v1/leverageBracket）。
    返回第一个元素含 initialLeverage（最大杠杆）。失败返回 []。
    """
    def _call():
        d = _signed_get("/fapi/v1/leverageBracket", {"symbol": symbol})
        # 返回结构: [{"symbol": ..., "brackets": [...]}] 或直接 brackets list
        if isinstance(d, list) and d:
            brackets = d[0].get("brackets", d)
            return brackets
        return []
    return _retry(_call) or []


# ── 精度工具（exchange_info 缓存）─────────────────────────────────────────────
import math as _math

_EX_INFO_CACHE: dict = {}
_EX_INFO_TS: float   = 0.0
_EX_INFO_TTL: float  = 3600.0   # 1 小时

def _get_exchange_info_cached() -> dict:
    global _EX_INFO_TS
    import time as _time
    if _EX_INFO_CACHE and (_time.time() - _EX_INFO_TS) < _EX_INFO_TTL:
        return _EX_INFO_CACHE
    info = get_exchange_info()
    if info:
        _EX_INFO_CACHE.clear()
        for sym in info.get("symbols", []):
            s = sym["symbol"]
            tick = step = min_notional = 0.0
            for f in sym.get("filters", []):
                if f["filterType"] == "PRICE_FILTER":
                    tick = float(f.get("tickSize", 0.01))
                elif f["filterType"] == "LOT_SIZE":
                    step = float(f.get("stepSize", 0.001))
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional", 5.0))
            _EX_INFO_CACHE[s] = {"tick": tick, "step": step, "min_notional": min_notional}
        _EX_INFO_TS = _time.time()
    return _EX_INFO_CACHE


def get_precision(symbol: str) -> dict:
    """返回 {tick_size, step_size, min_notional, price_precision, qty_precision}"""
    info = _get_exchange_info_cached()
    entry = info.get(symbol, {"tick": 0.01, "step": 0.001, "min_notional": 5.0})
    tick = entry["tick"]; step = entry["step"]
    def _prec(x):
        if x <= 0: return 6
        return max(0, -int(_math.floor(_math.log10(x))))
    return {
        "tick_size":       tick,
        "step_size":       step,
        "min_notional":    entry["min_notional"],
        "price_precision": _prec(tick),
        "qty_precision":   _prec(step),
    }


def round_price(symbol: str, price: float) -> float:
    p = get_precision(symbol)
    tick = p["tick_size"]
    if tick <= 0: return round(price, 6)
    prec = p["price_precision"]
    # [v3.88 OPT-16] 浮点修复：原写法 floor(price/tick) 受浮点误差影响（0.3/0.1=2.9999999999999996，floor 得 2，价格少一个 tick）。
    #   先 round(price/tick, 10) 消除误差再向下取整，与 round_quantity 的修法一致；正常价格结果不变。
    return round(_math.floor(round(price / tick, 10)) * tick, prec)


def round_quantity(symbol: str, quantity: float) -> float | None:
    p = get_precision(symbol)
    step = p["step_size"]
    if step <= 0: return round(quantity, 3)
    prec = p["qty_precision"]
    # [v3.88 OPT-16] 同上：先 round(quantity/step, 10) 再向下取整，避免数量少一个步长。
    qty  = round(_math.floor(round(quantity / step, 10)) * step, prec)
    if qty * 1 < p["min_notional"]:   # 粗略检查（调用方应传入更精确的价格）
        logger.warning(f"[api] {symbol} 数量 {qty} 可能低于最小名义仓位")
    return qty


def place_batch_orders(orders: list) -> list:
    """
    批量提交最多 5 个订单（POST /fapi/v1/batchOrders）。
    返回 list，每项含 orderId 或 error_code。
    """
    import json as _json
    if not orders:
        return []
    batch = orders[:5]
    params = {"batchOrders": _json.dumps(batch)}
    def _call():
        return _signed_post("/fapi/v1/batchOrders", params)
    # [v3.89 #83] 写超时：逐单按挂单对账，找到多少算多少，其余标记结果未知
    since_ms = int(time.time() * 1000) - 5000
    try:
        result = _retry(_call, is_write=True)
    except WriteTimeoutError as e:
        out, used = [], set()
        cache: dict = {}
        for spec in batch:
            sym = spec.get("symbol")
            try:
                if sym not in cache:
                    cache[sym] = get_open_orders(sym)
                found, n = _find_unique_open_order(cache[sym], spec, since_ms, used)
            except Exception as qe:
                found, n = None, -1
                logger.warning(f"[place_batch_orders] {sym} 写超时后查询挂单失败: {qe}")
            if found:
                used.add(found.get("orderId"))
                out.append(found)
            else:
                out.append({"error_code": "WRITE_TIMEOUT_UNKNOWN",
                            "msg": f"写请求超时，挂单未找到唯一匹配(匹配数={n})，结果未知"})
        logger.warning(
            f"[place_batch_orders] 写请求超时（{e}），对账找到 "
            f"{sum(1 for x in out if x.get('orderId'))}/{len(batch)} 单，其余结果未知"
        )
        return out
    return result if isinstance(result, list) else []


def get_listen_key() -> str | None:
    """获取 userData stream listenKey"""
    def _call():
        return _signed_post("/fapi/v1/listenKey", {})
    result = _retry(_call)
    return (result or {}).get("listenKey")


def keepalive_listen_key(listen_key: str) -> bool:
    """续期 listenKey（每 30 分钟调用一次）"""
    import hmac, hashlib, urllib.parse
    headers = {"X-MBX-APIKEY": API_KEY}
    try:
        r = _SESSION.put(f"{BASE_URL}/fapi/v1/listenKey",
                         params={"listenKey": listen_key},
                         headers=headers, timeout=_api_timeout())
        r.raise_for_status()
        return True
    except Exception as e:
        logger.warning(f"[api] keepalive_listen_key 失败: {e}")
        return False


def cancel_all_orders(symbol: str) -> int:
    """撤销指定标的所有挂单，返回实际撤销数量"""
    def _call():
        return _signed_delete("/fapi/v1/allOpenOrders", {"symbol": symbol})
    result = _retry(_call)
    if isinstance(result, list):
        return len(result)   # allOpenOrders 返回被撤销的订单列表
    return 0 if result is None else 1


def get_server_time() -> int:
    """获取币安服务器时间（毫秒时间戳）"""
    def _call():
        return _get("/fapi/v1/time")
    result = _retry(_call)
    return int((result or {}).get("serverTime", 0))


# ─── Stop Loss Order（STOP_MARKET via /fapi/v1/order）──────────────────────────
# 普通合约接口，比 Algo 更稳定，orderId 直接可撤，全账户可见

def _to_float(v):
    """[v3.89 #12] 安全转 float，失败返回 None。"""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def place_sl(symbol: str, side: str, position_side: str,
             trigger_price: float, quantity: float) -> dict:
    """
    挂 STOP_MARKET 止损单，走 /fapi/v1/algoOrder（本账号 STOP_MARKET 必须用 Algo 接口）。
    side:          SELL（LONG仓止损）/ BUY（SHORT仓止损）
    workingType:   MARK_PRICE 防止插针误触发
    返回：{"algoId": int, "orderId": <algoId>, ...} 兼容字段，使调用方统一用 orderId/algoId 均可
    """
    hedge = is_hedge_mode()
    params: dict = {
        "symbol":       symbol,
        "side":         side,
        "type":         "STOP_MARKET",
        "triggerPrice": fmt_num(trigger_price),
        "quantity":     fmt_num(quantity),
        "workingType":  "MARK_PRICE",
        "algoType":     "CONDITIONAL",
    }
    if hedge:
        params["positionSide"] = position_side
    else:
        params["reduceOnly"] = "true"
    def _call():
        return _signed_post("/fapi/v1/algoOrder", params)
    # v3.56: suppress -2021（同标的重复止损单），超时重试时第一次可能已在交易所生效
    # 若返回 None（被抑制），说明已有同价止损单存在，主动查出真实 algoId
    result = _retry(_call, suppress_codes=[-2021]) or {}
    if not result.get("algoId") and not result.get("orderId"):
        # 可能是超时后重试触发 -2021（第一次已成功），尝试查出已有的止损单
        # [v3.89 #12] 原逻辑只按 symbol+side 取最新一单认领，可能认领到别的保护单（如旧仓位/其他价位的止损）。
        #   现要求 symbol/side/positionSide(Hedge)/type/触发价(±0.1%)/数量(±1%) 全部匹配且唯一才认领；
        #   多个匹配或字段缺失无法核对 → 不认领，返回 {}（未确认），由调用方重新处理。
        try:
            existing = _signed_get("/fapi/v1/algo/orders/open", {"symbol": symbol}) or {}
            orders   = existing.get("orders", []) if isinstance(existing, dict) else []
            matched  = []
            for o in orders:
                if o.get("symbol") != symbol or o.get("side") != side:
                    continue
                if hedge and o.get("positionSide") != position_side:
                    continue
                o_type = o.get("orderType") or o.get("type")
                if o_type not in ("STOP_MARKET", "STOP"):
                    continue
                o_trig = _to_float(o.get("triggerPrice") if o.get("triggerPrice") is not None else o.get("stopPrice"))
                o_qty  = _to_float(o.get("quantity") if o.get("quantity") is not None else o.get("origQty"))
                if o_trig is None or o_qty is None:
                    continue   # 字段缺失，无法核对，不认领
                if abs(o_trig - float(trigger_price)) > abs(float(trigger_price)) * 0.001:
                    continue
                if abs(o_qty - float(quantity)) > abs(float(quantity)) * 0.01:
                    continue
                matched.append(o)
            if len(matched) == 1:
                o = matched[0]
                result = {"algoId": int(o["algoId"]), "orderId": int(o["algoId"]),
                          "_recovered": True}
                logger.info(f"[api] place_sl {symbol} -2021重复，按完整属性恢复已有algoId={o['algoId']}")
            elif len(matched) > 1:
                logger.warning(
                    f"[api] place_sl {symbol} 恢复失败：{len(matched)} 个止损单同时匹配 "
                    f"(side={side} trigger={trigger_price} qty={quantity})，不认领，返回未确认状态"
                )
            else:
                logger.warning(
                    f"[api] place_sl {symbol} 未找到属性匹配的已有止损单 "
                    f"(side={side} trigger={trigger_price} qty={quantity})，不认领"
                )
        except Exception as _re:
            logger.warning(f"[api] place_sl {symbol} 恢复已有止损单查询异常: {_re}")
    # 兼容：让调用方用 orderId 也能取到 algoId
    if result.get("algoId") and not result.get("orderId"):
        result["orderId"] = result["algoId"]
    return result


# ─── Algo Order（保留仅供旧单清理）──────────────────────────────────────────────
# 旧 Algo 接口（/fapi/v1/algoOrder）存在 algoId 难以追踪、撤单不稳定等问题
# 已由上方 place_sl 替代；以下函数仅供紧急回退或旧仓位清理使用

def place_algo_sl(symbol: str, side: str, position_side: str,
                  trigger_price: float, quantity: float) -> dict:
    """[已废弃] 请改用 place_sl。保留仅供紧急回退。"""
    return place_sl(symbol, side, position_side, trigger_price, quantity)


def cancel_algo_order(symbol: str, algo_id: int) -> dict:
    """尝试撤销旧 Algo 单；如已失效则静默忽略"""
    try:
        r = _signed_delete("/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id})
        return r or {}
    except Exception:
        return {}


def get_algo_open_orders(symbol: str = None) -> list:
    """查询当前 Algo 挂单，仅用于旧单清理。symbol=None 查全量。"""
    def _call():
        params = {"symbol": symbol} if symbol else {}
        return _signed_get("/fapi/v1/openAlgoOrders", params)
    result = _retry(_call)
    if result is None:   # [fix #4] 查询失败≠无 Algo 单，抛出由调用方决定，避免据此重复挂 SL
        raise ApiQueryError("get_algo_open_orders 查询失败（重试耗尽）")
    if isinstance(result, dict):
        return result.get("orders", [])
    return result or []
