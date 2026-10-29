"""trade_log 等 jsonl 日志的线程安全读写（v3.93）。

所有对 trade_log.jsonl 的追加写 / 整体回写都经过同一把进程内锁，
避免「读出整个文件 → 改一行 → 回写」期间并发追加的记录被覆盖。
这些函数都是同步阻塞的：async 调用方统一用 asyncio.to_thread 调用。
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path

logger = logging.getLogger(__name__)
_LOG_LOCK = threading.RLock()


def append_jsonl(path, record: dict, encoding: str | None = "utf-8") -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with _LOG_LOCK:
        with open(p, "a", encoding=encoding) as f:
            f.write(line)


_DEDUP_TAIL_BYTES = 512 * 1024      # 去重只回看文件尾部这么多字节（近期记录），避免大文件整读


def _same_open_trade(rec: dict, new: dict) -> bool:
    """同一笔平仓：open_time 相同（>0）且 symbol、direction（忽略大小写）相同。open_time 缺失/为 0 的不判重。"""
    try:
        o_new = int(float(new.get("open_time") or 0))
        o_old = int(float(rec.get("open_time") or 0))
    except (TypeError, ValueError):
        return False
    if o_new <= 0 or o_new != o_old:
        return False
    if rec.get("symbol") != new.get("symbol"):
        return False
    return str(rec.get("direction") or "").lower() == str(new.get("direction") or "").lower()


def append_trade_record_dedup(path, record: dict, encoding: str | None = "utf-8") -> bool:
    """追加一条「平仓」记录；若文件近期已有相同 open_time + symbol + direction 的记录则跳过（返回 False）。

    判重与追加在同一把锁内完成，WS 平仓路径与 position_manager 路径并发写同一笔时只会留下先到的一条。
    只对带 open_time(>0) 的平仓记录判重；没有 open_time 的记录（如 missed_unfilled）照常追加。
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with _LOG_LOCK:
        try:
            has_key = int(float(record.get("open_time") or 0)) > 0
        except (TypeError, ValueError):
            has_key = False
        if has_key and p.exists():
            try:
                with open(p, "rb") as f:
                    f.seek(0, 2)
                    size = f.tell()
                    f.seek(max(size - _DEDUP_TAIL_BYTES, 0))
                    tail = f.read().decode("utf-8", errors="replace").splitlines()
                if size > _DEDUP_TAIL_BYTES and tail:
                    tail = tail[1:]            # 第一行可能被截断
                for ln in reversed(tail):
                    try:
                        old = json.loads(ln)
                    except Exception:
                        continue
                    if isinstance(old, dict) and _same_open_trade(old, record):
                        logger.debug(f"[trade_log] 跳过重复平仓记录 {record.get('symbol')} {record.get('direction')} "
                                     f"open_time={record.get('open_time')}（已有 trade_id={old.get('trade_id')}，"
                                     f"本次 trade_id={record.get('trade_id')}）")
                        return False
            except OSError as e:
                logger.warning(f"[trade_log] 去重读取失败，按正常追加处理: {e}")
        with open(p, "a", encoding=encoding) as f:
            f.write(line)
    return True


def read_lines(path) -> list:
    """锁内读取全部行（保留原始文本，由调用方解析）；文件不存在返回 []。"""
    p = Path(path)
    with _LOG_LOCK:
        if not p.exists():
            return []
        with open(p, "r", encoding="utf-8") as f:
            return f.readlines()


def update_jsonl_field(path, match_key: str, match_val, field: str, value) -> bool:
    """锁内读-改-写：把 match_key==match_val 的记录写入 field=value；
    回写先写 .tmp 再 os.replace 原子替换。返回是否命中。"""
    p = Path(path)
    hit = False
    with _LOG_LOCK:
        if not p.exists():
            return False
        lines = p.read_text().splitlines()
        new = []
        for line in lines:
            try:
                rec = json.loads(line)
                if rec.get(match_key) == match_val:
                    rec[field] = value
                    line = json.dumps(rec, ensure_ascii=False)
                    hit = True
            except Exception:
                pass
            new.append(line)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text("\n".join(new) + "\n")
        os.replace(tmp, p)
    return hit


# ── [refactor-B3] 后台写入：调用方只把记录放进队列立即返回，由单独线程按顺序追加写盘 ──
import queue as _queue
_BG_Q: "_queue.Queue" = _queue.Queue(maxsize=10000)
_BG_THREAD = None
_BG_START_LOCK = threading.Lock()


def _bg_worker() -> None:
    while True:
        path, record, encoding = _BG_Q.get()
        try:
            append_jsonl(path, record, encoding)
        except Exception as e:
            logger.error(f"[trade_log_io] 后台写入失败 {path}: {e}")
        finally:
            _BG_Q.task_done()


def append_jsonl_bg(path, record: dict, encoding: str | None = "utf-8") -> None:
    """非阻塞追加。开关 BG_LOG_WRITER=False、或队列满时，退回同步写，保证不丢记录。"""
    global _BG_THREAD
    try:
        import config as _c
        enabled = bool(getattr(_c, "BG_LOG_WRITER", True))
    except Exception:
        enabled = True
    if not enabled:
        return append_jsonl(path, record, encoding)
    if _BG_THREAD is None or not _BG_THREAD.is_alive():
        with _BG_START_LOCK:
            if _BG_THREAD is None or not _BG_THREAD.is_alive():
                _BG_THREAD = threading.Thread(target=_bg_worker, daemon=True, name="log-writer")
                _BG_THREAD.start()
    try:
        _BG_Q.put_nowait((path, record, encoding))
    except _queue.Full:
        append_jsonl(path, record, encoding)


def flush_bg(timeout: float = 5.0) -> bool:
    """等待后台队列写完（退出前调用）。返回是否在超时前写完。"""
    import time as _t
    end = _t.monotonic() + timeout
    while _BG_Q.unfinished_tasks and _t.monotonic() < end:
        _t.sleep(0.05)
    return not _BG_Q.unfinished_tasks


import atexit as _atexit
_atexit.register(flush_bg, 5.0)   # [refactor-B3] 正常退出前把队列里的记录写完
