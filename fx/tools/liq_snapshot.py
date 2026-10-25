"""清算区每小时快照（回测数据基础设施，步骤0）。

读取当前候选池，对每个标的取当前清算区（LONG/SHORT 两个方向），带时间戳追加写入
fx/data/liq_snapshots/{YYYYMMDD}.jsonl（UTC 日期，只追加不覆盖）。
一行一个标的：{"ts": 秒级时间戳, "symbol": "BTCUSDT", "zones": [{...zone, "side": "LONG"|"SHORT"}, ...]}
历史清算图拿不到，这份快照是以后回测清算区锚定逻辑的唯一数据来源。

失败只记日志，永不抛出到调用方。可命令行单次运行：python3 tools/liq_snapshot.py
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = logging.getLogger("fx.liq_snapshot")

SNAPSHOT_DIR = ROOT / "data" / "liq_snapshots"
POOL_FILE = ROOT / "logs" / "candidate_pool.json"
DEFAULT_BUDGET_SEC = 50 * 60      # 单次运行时间上限，避免压到下一个整点


def load_pool(pool_file: Path = POOL_FILE) -> list:
    """候选池标的列表；文件不存在/损坏返回空列表。"""
    try:
        d = json.loads(Path(pool_file).read_text(encoding="utf-8"))
        return [s for s in (d.get("pool") or []) if isinstance(s, str)]
    except Exception as e:
        logger.warning(f"[liq_snapshot] 读取候选池失败: {e}")
        return []


def snapshot_path(ts: float, base: Path = SNAPSHOT_DIR) -> Path:
    return Path(base) / (datetime.fromtimestamp(ts, timezone.utc).strftime("%Y%m%d") + ".jsonl")


def _fetch_zones(symbol: str) -> list:
    """取两个方向的清算区并合并为一个列表，每个 zone 带 side 标记。"""
    from shared.liq_cache import get_liq_zones_cached
    out = []
    for side in ("LONG", "SHORT"):
        for z in (get_liq_zones_cached(symbol, side) or []):
            if isinstance(z, dict):
                out.append({**z, "side": side})
    return out


def run(symbols: list | None = None, fetch=None, base: Path = SNAPSHOT_DIR,
        budget_sec: float = DEFAULT_BUDGET_SEC, now_fn=time.time) -> dict:
    """执行一次快照。返回 {"ok": 成功标的数, "empty": 无数据标的数, "failed": 失败数, "total": 总数, "path": 文件}。
    empty（三级数据源全失败）的标的也写一行 zones=[]，便于回测区分“没数据”和“没快照”。"""
    res = {"ok": 0, "empty": 0, "failed": 0, "total": 0, "path": ""}
    try:
        syms = list(symbols) if symbols is not None else load_pool()
        res["total"] = len(syms)
        if not syms:
            logger.warning("[liq_snapshot] 候选池为空，本次不写快照")
            return res
        fetch = fetch or _fetch_zones
        start = now_fn()
        ts = int(start)
        path = snapshot_path(ts, base)
        path.parent.mkdir(parents=True, exist_ok=True)
        for sym in syms:
            if now_fn() - start > budget_sec:
                logger.warning(f"[liq_snapshot] 超过时间预算 {budget_sec:.0f}s，剩余标的跳过")
                break
            try:
                zones = fetch(sym)
                line = json.dumps({"ts": ts, "symbol": sym, "zones": zones}, ensure_ascii=False)
                with open(path, "a", encoding="utf-8") as f:   # 追加，每个标的写完即落盘
                    f.write(line + "\n")
                res["ok" if zones else "empty"] += 1
            except Exception as e:
                res["failed"] += 1
                logger.warning(f"[liq_snapshot] {sym} 失败: {e}")
        res["path"] = str(path)
        logger.info(f"[liq_snapshot] 完成 {res}")
    except Exception as e:
        logger.error(f"[liq_snapshot] 运行异常: {e}", exc_info=True)
    return res


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    print(run())
