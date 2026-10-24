"""
shared/notifier.py — 推送通知（Jarvis）

动态卡片设计：一个信号 = 一条消息，后续事件编辑同一条消息追加内容。

底层推送优先级：
  1. tools/invoke HTTP API  — 能发 + 能编辑，可拿到 message_id  ← 主路径
  2. IPC Socket             — 备用（socket 存在时）
  3. Popen fire-and-forget  — 兜底，不阻塞，但拿不到 message_id

公共接口：
  notify(text)                    → 发一条全局独立消息
  notify_async(text)              → 异步版 notify
  card_create(sig_id, header)     → 创建卡片（发第一条），返回 message_id
  card_append(sig_id, line)       → 编辑原消息追加行；无法编辑则发独立消息
  card_append_async(sig_id, line) → 异步版 card_append
  card_remove(sig_id)             → 信号结束后清理缓存
"""
import json
import logging
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("fx.notifier")

# ── Jarvis 推送目标（userId:thread:threadId）──────────────────────────────────
_JARVIS_TARGET = "432401404:thread:01a157e7-5c36-76fa-9e3f-70de22a64c3a"

# ── Gateway HTTP API ────────────────────────────────────────────────────────
_GW_PORT  = os.environ.get("OPENCLAW_GATEWAY_PORT", "18789")
_GW_TOKEN = "560e77c0982afef774c11776cb5ac38052b027afbda9778f"
_GW_URL   = f"http://127.0.0.1:{_GW_PORT}/tools/invoke"
_GW_HDR   = {"Authorization": f"Bearer {_GW_TOKEN}"}

# ── 卡片状态持久化路径（进程重启后能续接同一张卡片）────────────────────────────
_CARD_STATE_PATH = Path(__file__).parent.parent / "logs" / "card_state.json"

# ── 内存缓存：sig_id → {"message_id": str|None, "text": str, "ts": float} ───
_card_cache: dict = {}

# [v3.90 #18] 卡片缓存锁改为 16 个分片锁：按 sig_id 分片，不同信号互不阻塞。
#   原实现是一把全局 _card_lock，多个标的同时追加卡片行时会串行等待，延迟叠加。
#   注意：_card_cache 仍是单个共享 dict，_card_locks 只保证同一 sig_id 的互斥；
#   快照落盘需要整表一致视图，因此另用 _card_state_lock 串行化 snapshot（很短的临界区）。
_CARD_LOCK_SHARDS = 16
_card_locks = {i: threading.Lock() for i in range(_CARD_LOCK_SHARDS)}
_card_state_lock = threading.Lock()


def _card_lock_for(sig_id: str) -> threading.Lock:
    """[v3.90 #18] 取 sig_id 对应的分片锁（稳定哈希，避免受 PYTHONHASHSEED 影响）。"""
    import zlib
    return _card_locks[zlib.crc32(str(sig_id).encode()) % _CARD_LOCK_SHARDS]


def _card_snapshot() -> dict:
    """[v3.90 #18] 取 _card_cache 的一致性浅快照，避免落盘时遍历到正在变动的 dict。"""
    with _card_state_lock:
        return dict(_card_cache)


# =============================================================================
# 底层：HTTP invoke（主路径）
# =============================================================================

def _http_send(text: str) -> Optional[str]:
    """
    通过 tools/invoke 发消息，返回 message_id（字符串），失败返回 None。
    """
    try:
        import requests as _req
        resp = _req.post(_GW_URL, json={
            "tool": "message",
            "args": {
                "action":  "send",
                "channel": "jarvis",
                "target":  _JARVIS_TARGET,
                "message": text,
            },
        }, headers=_GW_HDR, timeout=6)

        if resp.status_code == 200:
            body = resp.json()
            inner = json.loads(body["result"]["content"][0]["text"])
            mid = inner.get("result", {}).get("messageId") or inner.get("messageId")
            return mid if mid else None
    except Exception as e:
        logger.debug(f"[notifier] _http_send 失败: {e}")
    return None


def _http_edit(message_id: str, new_text: str) -> bool:
    """
    通过 tools/invoke 编辑已有消息，返回是否成功。
    """
    try:
        import requests as _req
        resp = _req.post(_GW_URL, json={
            "tool": "message",
            "args": {
                "action":    "edit",
                "channel":   "jarvis",
                "target":    _JARVIS_TARGET,
                "messageId": message_id,
                "message":   new_text,
            },
        }, headers=_GW_HDR, timeout=6)
        if resp.status_code != 200:
            logger.warning(
                f"[notifier] _http_edit 失败: status={resp.status_code} "
                f"body={(resp.text or '')[:200]}"
            )
        return resp.status_code == 200
    except Exception as e:
        logger.warning(f"[notifier] _http_edit 异常: {e}")
    return False


# =============================================================================
# 底层：IPC Socket（备用）
# =============================================================================

def _socket_send(text: str) -> Optional[str]:
    try:
        import socket as _sock
        sock_path = os.path.expanduser("~/.openclaw/gateway.sock")
        if not os.path.exists(sock_path):
            return None
        payload = json.dumps({
            "action": "send", "channel": "jarvis",
            "target": _JARVIS_TARGET, "message": text,
        }).encode() + b"\n"
        with _sock.socket(_sock.AF_UNIX, _sock.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect(sock_path)
            s.sendall(payload)
            resp_raw = b""
            try:
                s.settimeout(3)
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    resp_raw += chunk
            except Exception:
                pass
        if resp_raw:
            try:
                resp = json.loads(resp_raw.decode())
                mid = resp.get("message_id") or resp.get("id") or resp.get("messageId")
                if mid and mid != "ok":
                    return mid
            except Exception:
                pass
        return None
    except Exception:
        return None


# =============================================================================
# 底层：Popen 兜底（最后手段，无 message_id）
# =============================================================================

def _popen_send(text: str) -> None:
    """Fire-and-forget，不等待，不阻塞，拿不到 message_id。"""
    try:
        env = dict(os.environ)
        env.pop("OPENCLAW_SHELL", None)
        subprocess.Popen(
            ["openclaw", "message", "send",
             "--channel", "jarvis",
             "--target",  _JARVIS_TARGET,
             "--message", text],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
    except Exception as e:
        logger.warning(f"[notifier] Popen 失败: {e}")


# =============================================================================
# 统一发送入口
# =============================================================================

def _send(text: str) -> Optional[str]:
    """
    尝试发消息，按优先级降级：HTTP → Socket → Popen。
    返回 message_id（字符串）或 None（Popen 兜底时）。
    """
    mid = _http_send(text)
    if mid:
        return mid

    mid = _socket_send(text)
    if mid:
        return mid

    _popen_send(text)
    return None


def _edit(message_id: str, new_text: str) -> bool:
    """尝试编辑消息：HTTP → Socket。"""
    if _http_edit(message_id, new_text):
        return True
    # Socket edit（如果有）
    try:
        import socket as _sock
        sock_path = os.path.expanduser("~/.openclaw/gateway.sock")
        if os.path.exists(sock_path):
            payload = json.dumps({
                "action": "edit", "channel": "jarvis",
                "target": _JARVIS_TARGET,
                "message_id": message_id, "message": new_text,
            }).encode() + b"\n"
            with _sock.socket(_sock.AF_UNIX, _sock.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(sock_path)
                s.sendall(payload)
                try:
                    s.settimeout(2); s.recv(4096)
                except Exception:
                    pass
            return True
    except Exception:
        pass
    return False


# =============================================================================
# 卡片持久化（进程重启续接）
# =============================================================================

def _load_card_state() -> dict:
    try:
        if _CARD_STATE_PATH.exists():
            data = json.loads(_CARD_STATE_PATH.read_text(encoding="utf-8"))
            cutoff = time.time() - 86400
            # 只保留有真实 message_id（非 None/"ok"）且未过期的卡片
            return {
                k: v for k, v in data.items()
                if v.get("ts", 0) > cutoff
                and v.get("message_id") not in (None, "ok", "")
            }
    except Exception:
        pass
    return {}


def _save_card_state(state: dict) -> None:
    """
    [v3.90 #19] 卡片状态落盘改为原子写入：先写同目录 .tmp，再 os.replace 覆盖。
    os.replace 在 POSIX 上是原子操作，进程若在写中途崩溃，旧文件仍然完整可用，
    不会留下半截 JSON（旧实现 write_text 直接覆盖，崩溃即损坏）。
    写入失败从静默 pass 改为 warn 日志，便于发现磁盘/权限问题。
    """
    tmp_path = _CARD_STATE_PATH.with_suffix(".json.tmp")
    try:
        _CARD_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(tmp_path, _CARD_STATE_PATH)
    except Exception as e:
        logger.warning(f"[notifier] card_state 写入失败: {e}")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def _init_card_cache() -> None:
    global _card_cache
    with _card_state_lock:
        _card_cache = _load_card_state()


_init_card_cache()


# =============================================================================
# 信号卡片缓冲（#信号聚合）：延迟 card_create，短命信号不再留下空气卡片
# =============================================================================

class SignalBuffer:
    """
    [v3.90 通知聚合] 线程安全的信号卡片暂存区。

    card_create 不再立刻发消息，而是把 header 暂存在这里；
    同一个 sig_id 的后续 card_append 也进缓冲区，直到：
      · flush_signal(sig_id)   —— 确认值得推送（开仓成交 / 价格触发）时一次性合并发出
      · discard_signal(sig_id) —— 确认不该推送（竞争淘汰 / 过期 / 预检拒绝）时静默丢弃
      · tick_signal_buffer()   —— 超过 TTL 仍未 flush/discard 的兜底 flush（默认 30 分钟）
    缓冲只存文本，不放锁跨网络调用，所以 flush/discard 都是 O(缓冲区长度)。
    """

    def __init__(self, ttl: float = 1800.0):
        self._buf: dict = {}       # sig_id -> [header(line0), line, ...]
        self._ts:  dict = {}       # sig_id -> 首次入缓冲时间
        self._lock = threading.Lock()
        self._ttl  = ttl

    def has(self, sig_id: str) -> bool:
        with self._lock:
            return sig_id in self._buf

    def start(self, sig_id: str, header: str) -> None:
        with self._lock:
            self._buf[sig_id] = [header]
            self._ts.setdefault(sig_id, time.time())

    def append(self, sig_id: str, line: str) -> bool:
        with self._lock:
            if sig_id not in self._buf:
                return False
            self._buf[sig_id].append(line)
            return True

    def pop(self, sig_id: str) -> list:
        with self._lock:
            self._ts.pop(sig_id, None)
            return self._buf.pop(sig_id, [])

    def discard(self, sig_id: str) -> bool:
        with self._lock:
            self._ts.pop(sig_id, None)
            return self._buf.pop(sig_id, None) is not None

    def stale(self, now: float = None) -> list:
        now = time.time() if now is None else now
        with self._lock:
            return [k for k, t in self._ts.items() if now - t >= self._ttl]

    def size(self) -> int:
        with self._lock:
            return len(self._buf)


_signal_buffer = SignalBuffer()


def buffer_signal(sig_id: str, header: str) -> None:
    """[v3.90 通知聚合] 把卡片标题放入缓冲区，暂不推送。"""
    _signal_buffer.start(sig_id, header)


# [v3.97] 在途 flush 登记：flush_signal 先清空缓冲区、后发送消息并登记 _card_cache，这段时间里
#   缓冲区已空、_card_cache 也还没有该 sig_id。此时 executor 立刻拒绝并 card_append，会命中
#   「无卡片缓存 → 降级独立推送」，变成一条没有标的名的孤立消息（随后 card_remove 又被 flush 的
#   登记覆盖，留下幽灵缓存）。因此 card_append / card_remove 遇到在途 flush 先等它完成。
_flushing: dict = {}                 # sig_id -> threading.Event（flush 完成时 set）
_flushing_lock = threading.Lock()
_FLUSH_WAIT_SEC = 10.0               # 等在途 flush 的上限（_send 最坏 HTTP6s+socket5s 之内通常已返回）


def _wait_inflight_flush(sig_id: str) -> None:
    with _flushing_lock:
        ev = _flushing.get(sig_id)
    if ev is not None and not ev.wait(_FLUSH_WAIT_SEC):
        logger.warning(f"[card] sig_id={sig_id} 等待在途 flush 超过 {_FLUSH_WAIT_SEC:.0f}s，继续（可能降级独立消息）")


def flush_signal(sig_id: str) -> bool:
    """[v3.90 通知聚合] 把缓冲区内容合并一次性推送（标题 + 所有追加行）。"""
    with _flushing_lock:                      # 先登记再 pop：保证 pop 之后的 card_append 一定能看到在途标记
        own = sig_id not in _flushing
        if own:
            _flushing[sig_id] = threading.Event()
    try:
        return _flush_signal_inner(sig_id)
    finally:
        if own:
            with _flushing_lock:
                ev = _flushing.pop(sig_id, None)
            if ev is not None:
                ev.set()


def _flush_signal_inner(sig_id: str) -> bool:
    lines = _signal_buffer.pop(sig_id)
    if not lines:
        return False
    # 合并成一条消息一次发出（标题 + 全部已缓冲行，与编辑追加时 "\n" 拼接的排版一致）。
    # 旧写法先单独发标题、再逐行编辑追加：一旦拿不到 message_id 或编辑失败，每一行都会降级成独立消息。
    _card_create_now(sig_id, "\n".join(lines))
    logger.debug(f"[card] flush sig_id={sig_id} 合并推送 {len(lines)} 行")
    return True


def discard_signal(sig_id: str) -> bool:
    """[v3.90 通知聚合] 静默丢弃缓冲区内容（信号被淘汰/过期/预检拒绝）。"""
    dropped = _signal_buffer.discard(sig_id)
    if dropped:
        logger.debug(f"[card] discard sig_id={sig_id} 缓冲区已静默丢弃")
    return dropped


def tick_signal_buffer() -> int:
    """[v3.90 通知聚合] 兜底：把超过 TTL 仍未 flush/discard 的条目 flush 出去。"""
    stale = _signal_buffer.stale()
    for sid in stale:
        logger.warning(f"[card] sig_id={sid} 缓冲超过 TTL 未决，兜底 flush")
        try:
            flush_signal(sid)
        except Exception as e:
            logger.warning(f"[card] 兜底 flush 失败 sig_id={sid}: {e}")
    return len(stale)


# =============================================================================
# 公共 API
# =============================================================================

def notify(text: str) -> None:
    """发一条全局独立消息。失败只记日志，不抛异常。"""
    try:
        _send(text)
    except Exception as e:
        logger.info(f"[notify] 推送失败: {e} | 内容: {text[:80]}")


async def notify_async(text: str) -> None:
    """异步版 notify，不阻塞事件循环。"""
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: notify(text))


# ─────────────────────────────────────────────────────────────────────────────
# 卡片接口
# ─────────────────────────────────────────────────────────────────────────────

def card_create(sig_id: str, header: str) -> Optional[str]:
    """
    [v3.90 通知聚合] 创建信号卡片：只把标题放入缓冲区，暂不推送。
    真正发出消息在 flush_signal(sig_id) 时（开仓成交 / 价格触发）。
    返回 None —— 因为此时还没有 message_id（延迟推送后由 _card_create_now 决定）。
    """
    buffer_signal(sig_id, header)
    logger.debug(f"[card] 缓冲卡片 sig_id={sig_id}（等待 flush）")
    return None


def _card_create_now(sig_id: str, header: str) -> Optional[str]:
    """实际发出标题行并登记缓存（原 card_create 的即时行为）。"""
    mid = _send(header)

    with _card_lock_for(sig_id):
        _card_cache[sig_id] = {
            "message_id": mid,
            "text": header,
            "ts": time.time(),
        }
    if mid:
        _save_card_state(_card_snapshot())

    if mid:
        logger.debug(f"[card] 创建卡片 sig_id={sig_id} mid={mid} ← 编辑模式")
    else:
        logger.debug(f"[card] 创建卡片 sig_id={sig_id} ← 独立消息模式（无 mid）")
    return mid


def card_append(sig_id: str, line: str) -> bool:
    """
    向信号卡片追加一行进展。

    · 有 message_id → 编辑原消息追加（动态卡片）
    · 无 message_id → 直接发一条独立消息
    · 卡片不存在（进程重启丢失）→ 降级独立消息

    返回 True 表示消息已发出（不论编辑还是新消息）。
    """
    # [v3.90 通知聚合] 在缓冲区里就直接追加，等 flush 时一次发出
    if _signal_buffer.append(sig_id, line):
        logger.debug(f"[card] 缓冲追加 sig_id={sig_id}")
        return True

    _wait_inflight_flush(sig_id)   # [v3.97] 缓冲区已被 flush 取走但消息还在发送：等卡片登记后再追加
    return _card_append_now(sig_id, line)


def _card_append_now(sig_id: str, line: str) -> bool:
    """实际执行追加（编辑原消息或独立消息）。"""
    with _card_lock_for(sig_id):
        entry = dict(_card_cache.get(sig_id, {})) if sig_id in _card_cache else None

    if entry is None:
        logger.debug(f"[card] sig_id={sig_id} 无卡片缓存，降级独立推送")
        notify(line)
        return False

    message_id = entry.get("message_id")

    if message_id:
        new_text = entry["text"] + "\n" + line
        ok = _edit(message_id, new_text)
        if ok:
            with _card_lock_for(sig_id):
                if sig_id in _card_cache:
                    _card_cache[sig_id]["text"] = new_text
                    _card_cache[sig_id]["ts"]   = time.time()
            _save_card_state(_card_snapshot())
            logger.debug(f"[card] 编辑追加 sig_id={sig_id}")
            return True
        else:
            # 编辑失败，清除 mid 防止后续重试，降级独立消息
            logger.warning(f"[card] 编辑失败 sig_id={sig_id} mid={message_id}，降级并清除 mid")
            with _card_lock_for(sig_id):
                if sig_id in _card_cache:
                    _card_cache[sig_id]["message_id"] = None
            notify(line)
            return False
    else:
        # Popen 兜底模式：直接发独立消息
        notify(line)
        return True


async def card_create_async(sig_id: str, header: str) -> Optional[str]:
    """异步版 card_create。"""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: card_create(sig_id, header))


async def card_append_async(sig_id: str, line: str) -> bool:
    """异步版 card_append。"""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: card_append(sig_id, line))


def card_remove(sig_id: str) -> None:
    """信号结束后清理卡片缓存。"""
    _wait_inflight_flush(sig_id)   # [v3.97] 避免在途 flush 稍后又把已移除的卡片登记回来
    with _card_lock_for(sig_id):
        _card_cache.pop(sig_id, None)
    _save_card_state(_card_snapshot())


# =============================================================================
# 格式化工具（供各模块使用）
# =============================================================================

_SEP = "━━━━━━━━━━━━━━━━━━━"


def _hhmm() -> str:
    """当前 UTC 时间 HH:MM。"""
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M")


def _fmt_price(price) -> str:
    """价格加千位分隔符，保留原始小数精度。"""
    try:
        p = float(price)
        # 判断小数位数
        s = f"{p:.10f}".rstrip("0").rstrip(".")
        if "." in s:
            dec = len(s.split(".")[1])
        else:
            dec = 0
        # 整数部分加千分位
        if dec == 0:
            return f"{int(p):,}"
        fmt = f"{{:,.{dec}f}}"
        return fmt.format(p)
    except Exception:
        return str(price)


# mode + direction 组合中文
_MODE_DIR_CN: dict = {
    ("breakout",     "LONG"):  "突破做多",
    ("breakout",     "SHORT"): "突破做空",
    ("pullback",     "LONG"):  "回踩做多",
    ("pullback",     "SHORT"): "回踩做空",
    ("reversal",     "LONG"):  "反转做多",
    ("reversal",     "SHORT"): "反转做空",
    ("oi_divergence","LONG"):  "OI背离做多",
    ("oi_divergence","SHORT"): "OI背离做空",
    ("ema_trend",    "LONG"):  "EMA趋势做多",
    ("ema_trend",    "SHORT"): "EMA趋势做空",
    ("stop_hunt",    "LONG"):  "猎杀止损做多",
    ("stop_hunt",    "SHORT"): "猎杀止损做空",
    ("funding_arb",  "LONG"):  "资费套利做多",
    ("funding_arb",  "SHORT"): "资费套利做空",
}

_STRENGTH_CN = {"strong": "强信号", "medium": "中等信号", "weak": "弱信号"}

_CLOSE_REASON_CN = {
    "tp2":               "TP2止盈",
    "sl":                "止损",
    "sl_triggered":      "止损",
    "manual_close":      "手动平仓",
    "time_stop":         "时间止损",
    "time_stop_no_progress": "时间止损",
    "time_stop_loss":    "时间止损",
    "time_stop_hard":    "时间止损",
    "time_stop_hard_72h": "时间止损",
    "trailing_stop":     "移动止损锁利",
    "pnl_lock":          "浮盈保护平仓",
    "early_stagnation":  "时间止损",
    "pnl_protection":    "浮盈保护平仓",
    "tp1_protect_exit":  "浮盈保护平仓",
    "partial_time_stop": "时间止损",
    "breakout_fail":     "突破失败",
    "exit_signal_reversal": "反向信号平仓",
    # [v3.98] 平仓通知可读性：新增原因映射（未收录的原因码保持显示原始英文）
    "rr_below_threshold": "盈亏比不足，主动锁利",
    "tp1_hit":           "止盈1触发",
    "tp2_hit":           "止盈2触发",
    "sl_hit":            "止损触发",
    "time_limit":        "持仓超时，强制离场",
    "health_check":      "健康检查不通过，主动离场",
    "manual":            "手动平仓",
    "funding_cost":      "资金费率过高，主动离场",
}


def make_card_header(symbol: str, direction: str, mode: str, signal_strength: str) -> str:
    """卡片标题（带头部分隔线）。"""
    mode_dir = _MODE_DIR_CN.get((mode, direction.upper()),
                                f"{mode} {direction.upper()}")
    str_cn   = _STRENGTH_CN.get(signal_strength, signal_strength)
    return f"{_SEP}\n📋 {symbol} · {mode_dir} · {str_cn}\n{_SEP}"


def fmt_hold_time(hold_minutes: float) -> str:
    h = int(hold_minutes // 60)
    m = int(hold_minutes % 60)
    if h > 0:
        return f"{h}h {m}min"
    return f"{m}min"


# ─────────────────────────────────────────────────────────────────────────────
# 节点格式化函数（供各模块调用，统一在此维护）
# ─────────────────────────────────────────────────────────────────────────────

def fmt_node_monitor(score, pre_entry_price, expire_min: int, sl_degraded: bool = False) -> str:
    """节点1：进入监控"""
    score_str = f"{int(score)}" if score is not None else "—"
    lines = [
        f"\n✅ {_hhmm()} 进入监控",
        f" 评分 {score_str} | 预估入场 {_fmt_price(pre_entry_price)} | {expire_min}min后过期",
    ]
    if sl_degraded:
        lines.append(" ⚠️ 止损非结构位（降级），风险略高")
    return "\n".join(lines)


def fmt_node_trigger(trigger_price, entry_price) -> str:
    """节点2：价格触发"""
    return (
        f"\n⏳ {_hhmm()} 价格触发"
        f"\n 市价 {_fmt_price(trigger_price)} → 限价挂出 {_fmt_price(entry_price)}"
    )


def fmt_node_open(fill_price, sl_price, tp1_price, tp2_price,
                  leverage, notional, rr_tp1, rr_tp2=None,
                  entry_price=None, sl_degraded: bool = False) -> str:
    """节点3：开仓成交"""
    # 滑点
    slip_str = ""
    if entry_price and float(entry_price) > 0:
        slip_pct = abs(float(fill_price) - float(entry_price)) / float(entry_price) * 100
        if slip_pct >= 0.05:
            slip_str = f" 滑点 +{slip_pct:.2f}%"
    # TP2
    tp2_str = _fmt_price(tp2_price) if tp2_price else "单TP"
    # RR（去掉 1: 前缀）
    rr_str = f"{float(rr_tp1):.1f}"
    if rr_tp2:
        rr_str += f" / {float(rr_tp2):.1f}"
    # 止损降级标记
    sl_deg = " ⚠️降级" if sl_degraded else ""
    return (
        f"\n✅ {_hhmm()} 开仓成交"
        f"\n 成交价 {_fmt_price(fill_price)}{slip_str}"
        f"\n 止 损 {_fmt_price(sl_price)}{sl_deg}"
        f"\n TP1 {_fmt_price(tp1_price)}"
        f"\n TP2 {tp2_str}"
        f"\n 杠杆 {leverage}x · 仓位 {int(float(notional))}U · RR {rr_str}"
    )


def fmt_node_immediate_close(reason: str) -> str:
    """节点4：开仓后立即平仓"""
    return (
        f"\n🟡 {_hhmm()} 开仓后立即平仓"
        f"\n 原因：{reason}"
    )


def fmt_node_tp1(tp1_fill_price, new_sl_price) -> str:
    """节点5：TP1止盈"""
    return (
        f"\n🎯 {_hhmm()} TP1止盈"
        f"\n 成交价 {_fmt_price(tp1_fill_price)} · 锁利 60%"
        f"\n 止损上移至 {_fmt_price(new_sl_price)}（保本）"
        f"\n 剩余仓位 40%"
    )


def _fmt_hold_cn(hold_minutes: float) -> str:
    """持仓时长中文展示：2小时15分 / 45分钟（负数或异常按 0 处理）。"""
    try:
        total = max(int(float(hold_minutes)), 0)
    except (TypeError, ValueError):
        total = 0
    h, m = divmod(total, 60)
    if h > 0:
        return f"{h}小时{m}分"
    return f"{m}分钟"


def _fmt_tp_status(filled: bool, exists: bool, label: str) -> str:
    """单个止盈单状态：已触发 ✅ / 未触发 ✗ / 无此挂单 → 空串（不显示）。"""
    if filled:
        return f"{label}已触发 ✅"
    if not exists:
        return ""
    return f"{label}未触发 ✗"


def fmt_node_close(close_reason: str, exit_price, hold_minutes: float,
                   pnl_usdt: float, pnl_pct: float,
                   tp1_filled: bool, tp2_filled: bool,
                   symbol: str = "", direction: str = "",
                   tp1_exists: bool | None = None, tp2_exists: bool | None = None,
                   pos: dict | None = None) -> str:
    """节点6：平仓完成（含尾部分隔线）

    [v3.98] 可读性优化：带标的/方向标题，逐行展示出场价/持仓时长/盈亏/出场原因/止盈情况。
      · 出场原因：映射表有则显示中文，未知原因码原样显示（不翻译）
      · 止盈情况：TP1/TP2 已触发 ✅ / 未触发 ✗；无此挂单（exists=False 且未成交）则不显示该项，两项都无则整行不显示
      · 可直接传 pos（持仓记录）：自动取 symbol/direction，并以 tp1/tp2_order_id 是否存在判断「有无此挂单」；
        显式传入的 symbol/direction/tp1_exists/tp2_exists 优先。未传 pos 且未传 *_exists 时按「有挂单」处理。
    新增参数均有默认值，旧调用方式不受影响。"""
    pos = pos or {}
    symbol = symbol or pos.get("symbol", "") or ""
    direction = direction or pos.get("direction", "") or ""
    if tp1_exists is None:
        tp1_exists = bool(pos.get("tp1_order_id")) if pos else True
    if tp2_exists is None:
        tp2_exists = bool(pos.get("tp2_order_id")) if pos else True
    reason_cn = _CLOSE_REASON_CN.get(close_reason, close_reason)
    emoji = "🟢" if pnl_usdt >= 0 else "🔴"
    dir_cn = {"LONG": "多单", "SHORT": "空单"}.get(str(direction or "").upper(), "")
    title = f"{symbol} {dir_cn}平仓".strip() if symbol else "平仓完成"
    tp_parts = [x for x in (
        _fmt_tp_status(bool(tp1_filled), bool(tp1_exists), "TP1"),
        _fmt_tp_status(bool(tp2_filled), bool(tp2_exists), "TP2"),
    ) if x]
    lines = [
        f"\n{emoji} {title} {_hhmm()}",
        f"出场价：{_fmt_price(exit_price)}",
        f"持仓时长：{_fmt_hold_cn(hold_minutes)}",
        f"盈亏：{pnl_usdt:+.1f}U（{pnl_pct:+.2f}%）",
        f"出场原因：{reason_cn}",
    ]
    if tp_parts:
        lines.append("止盈情况：" + " / ".join(tp_parts))
    lines.append(_SEP)
    return "\n".join(lines)


def fmt_node_timeout(waited_min: int) -> str:
    """节点7：超时撤销"""
    return (
        f"\n⏰ {_hhmm()} 超时撤销"
        f"\n 等待 {waited_min}min 未触发，监控结束"
    )


def fmt_node_evicted(new_symbol: str, new_score) -> str:
    """节点8：竞争驱逐"""
    return (
        f"\n⬇️ {_hhmm()} 被驱逐"
        f"\n {new_symbol}（评分{int(float(new_score))}）替换，本信号结束"
    )


def fmt_node_sl_lost() -> str:
    """节点9：止损丢失告警"""
    return (
        f"\n🚨 {_hhmm()} 止损丢失"
        f"\n 保护单补挂失败，请手动检查持仓！"
    )


def fmt_global_btc_guard(direction: str, minutes: int, pct: float,
                         cancelled_count: int, symbols: list) -> str:
    """全局事件：BTC急涨/跌保护撤单"""
    dir_str  = "涨" if direction == "UP" else "跌"
    sym_str  = " · ".join(symbols[:6])  # 最多显示6个
    return (
        f"{_SEP}"
        f"\n⚡ BTC {minutes}分钟急{dir_str} {pct:.1f}%"
        f"\n已撤销全部 {cancelled_count} 个待触发监控"
        f"\n{sym_str}"
        f"\n{_SEP}"
    )


def fmt_global_heartbeat(hhmm: str, candidate_count: int,
                         raw_signals: int, monitored: int, filled: int,
                         ws_ok: bool) -> str:
    """全局事件：系统心跳"""
    ws_str = "✅ 全部连接" if ws_ok else "⚠️ 部分断开"
    return (
        f"{_SEP}"
        f"\n💓 系统心跳 · {hhmm} UTC"
        f"\n扫描正常 | {candidate_count}个标的 | 每5分钟一轮"
        f"\n过去6小时：原始信号{raw_signals}个 | 进入监控{monitored}个 | 成交{filled}笔"
        f"\nWS：{ws_str}"
        f"\n{_SEP}"
    )
