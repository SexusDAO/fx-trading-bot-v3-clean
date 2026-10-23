"""trade_log 等 jsonl 日志的线程安全读写（v3.93）。

所有对 trade_log.jsonl 的追加写 / 整体回写都经过同一把进程内锁，
避免「读出整个文件 → 改一行 → 回写」期间并发追加的记录被覆盖。
这些函数都是同步阻塞的：async 调用方统一用 asyncio.to_thread 调用。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

_LOG_LOCK = threading.RLock()


def append_jsonl(path, record: dict, encoding: str | None = "utf-8") -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with _LOG_LOCK:
        with open(p, "a", encoding=encoding) as f:
            f.write(line)


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
