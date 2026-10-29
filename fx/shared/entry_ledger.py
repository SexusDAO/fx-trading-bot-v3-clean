"""[refactor-B5] 开仓意图预写账本（write-ahead ledger）。

孤儿仓位的根因：开仓单在交易所成交时，本地记录可能因为重启、断线、撤单与成交撞车、写档失败而丢失。
本模块在「提交开仓单之前」先把意图同步写盘（含 fsync），并给开仓单打上可识别的 clientOrderId（前缀 fxent）。
之后对账发现「交易所有、本地无」的仓位时，可以凭账本确认它是系统自己开的，按正常仓位认领，
而不是当作来历不明的孤儿处理（不再被均线过滤 / 配额平仓误伤）。
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)
_LOCK = threading.Lock()
LEDGER_PATH = Path(__file__).resolve().parent.parent / "logs" / "entry_intents.jsonl"
CID_PREFIX = "fxent"


def new_entry_client_id() -> str:
    """开仓单 clientOrderId：fxent + 27 位随机串（Binance 上限 36 字符）。"""
    return CID_PREFIX + uuid.uuid4().hex[:27]


def record_intent(intent: dict, path: Path | None = None) -> bool:
    """同步写盘 + fsync。失败只记 error，不阻断下单（账本是补充保护，不是前置条件）。"""
    p = Path(path or LEDGER_PATH)
    rec = dict(intent)
    rec.setdefault("ts", time.time())
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(rec, ensure_ascii=False, default=str) + "\n"
        with _LOCK:
            with open(p, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
        return True
    except Exception as e:
        logger.error(f"[entry_ledger] 写开仓意图失败: {e}")
        return False


def find_intent(symbol: str, pos_side: str, max_age_hours: float = 48.0,
                path: Path | None = None, now: float | None = None) -> dict | None:
    """按 symbol + 方向找最近一条开仓意图（只读文件尾部）。找不到返回 None。"""
    p = Path(path or LEDGER_PATH)
    if not p.exists():
        return None
    now = now or time.time()
    side = str(pos_side).upper()
    try:
        with open(p, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 1024 * 1024))
            tail = f.read().decode("utf-8", errors="replace").splitlines()
    except Exception as e:
        logger.warning(f"[entry_ledger] 读账本失败: {e}")
        return None
    for line in reversed(tail):
        try:
            r = json.loads(line)
        except Exception:
            continue
        if r.get("symbol") != symbol or str(r.get("direction", "")).upper() != side:
            continue
        if now - float(r.get("ts", 0) or 0) > max_age_hours * 3600:
            return None
        return r
    return None
