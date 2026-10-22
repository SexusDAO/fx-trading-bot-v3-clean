"""阶段0 可观测性：纯函数（无副作用），供 main.py / scanner.py / ws_listener.py 调用，便于单测。"""
from __future__ import annotations

HEARTBEAT_WARN_SEC = 1.0
HEARTBEAT_ERROR_SEC = 5.0
WS_STALE_SEC = 60.0          # 流超过此秒数没收到消息，健康报告里标记为可疑


def heartbeat_delay(prev_ts: float, now_ts: float, interval: float) -> float:
    """相邻两次心跳的实际间隔减去期望间隔 = 事件循环延迟（不小于 0）。"""
    return max((now_ts - prev_ts) - interval, 0.0)


def heartbeat_level(delay: float, warn: float = HEARTBEAT_WARN_SEC, err: float = HEARTBEAT_ERROR_SEC):
    """返回 None / "warn" / "error"：>1s warn，>5s error。"""
    if delay > err:
        return "error"
    if delay > warn:
        return "warn"
    return None


def format_scan_perf(pool_load: float, mark_price: float, analysis: float, post: float,
                     unfinished: int, scanned: int) -> str:
    return (f"[scan-perf] 候选池加载={pool_load:.2f}s 批量标记价={mark_price:.2f}s "
            f"并行分析={analysis:.2f}s 信号后处理={post:.2f}s scan_unfinished={unfinished} 候选池={scanned}")


def stream_health(last_msg: dict, now: float, stale_sec: float = WS_STALE_SEC) -> list:
    """last_msg: {流名: 最后收到消息的 monotonic 时间，0/None 表示从未收到}。
    返回 [(流名, 距今秒数|None, 状态)]，状态 ok / stale / never。"""
    out = []
    for name in sorted(last_msg):
        ts = last_msg[name]
        if not ts:
            out.append((name, None, "never"))
            continue
        age = max(now - ts, 0.0)
        out.append((name, age, "stale" if age > stale_sec else "ok"))
    return out


def format_stream_health(rows: list) -> str:
    parts = []
    for name, age, st in rows:
        parts.append(f"{name}=never" if age is None else f"{name}={st}({age:.0f}s前)")
    return "[ws-health] " + " ".join(parts)
