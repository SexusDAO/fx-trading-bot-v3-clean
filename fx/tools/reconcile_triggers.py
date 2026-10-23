"""trigger_reconcile.jsonl 离线对账工具（手动执行，不随系统实时运行）。

用法（在仓库根目录）：
    python3 fx/tools/reconcile_triggers.py            # 查询并对账
    python3 fx/tools/reconcile_triggers.py --dry-run  # 只查询打印，不改任何文件

背景：signal_watcher 在 on_trigger 异常、触发结果未知时，会把记录追加到
fx/logs/trigger_reconcile.jsonl（[v3.89 #102]），本工具逐条查询币安实际订单状态：
  - filled   ：订单已成交（FILLED）
  - canceled ：订单已撤销/过期/被拒（CANCELED / EXPIRED / REJECTED / EXPIRED_IN_MATCH）
  - unknown  ：无订单号、查询失败、或订单仍在挂单/部分成交（NEW / PARTIALLY_FILLED 等）
filled / canceled 的记录追加写入 fx/logs/trigger_reconcile_done.jsonl（附 reconcile_status 等字段），
并从原文件移除；unknown 的记录原样保留在原文件，下次再对账。
注意：本工具只查询、不下单、不撤单；已成交订单是否已登记持仓需人工核对。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

FX_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(FX_DIR))

LOG_DIR = FX_DIR / "logs"
IN_PATH = LOG_DIR / "trigger_reconcile.jsonl"
DONE_PATH = LOG_DIR / "trigger_reconcile_done.jsonl"

_FILLED = {"FILLED"}
_CANCELED = {"CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH"}


def _order_id(rec: dict):
    for k in ("order_id_if_known", "order_id", "orderId"):
        v = rec.get(k)
        if v not in (None, "", 0, "0"):
            try:
                return int(v)
            except (TypeError, ValueError):
                return None
    return None


def classify(rec: dict, get_order) -> tuple:
    """返回 (status, order_dict_or_None, note)。status ∈ filled / canceled / unknown。"""
    symbol = str(rec.get("symbol") or "").upper()
    oid = _order_id(rec)
    if not symbol or oid is None:
        return "unknown", None, "缺少 symbol 或订单号，无法查询"
    try:
        od = get_order(symbol, oid)
    except Exception as e:
        return "unknown", None, f"查询失败: {e!r}"
    st = str((od or {}).get("status", "")).upper()
    if st in _FILLED:
        return "filled", od, st
    if st in _CANCELED:
        return "canceled", od, st
    return "unknown", od, f"订单状态={st or '空'}，尚未终态"


def _read(path: Path) -> list:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            out.append({"_raw": line})          # 坏行原样保留
    return out


def _dump(rec: dict) -> str:
    if set(rec) == {"_raw"}:
        return rec["_raw"]
    return json.dumps(rec, ensure_ascii=False, default=str)


def run(dry_run: bool = False, get_order=None, in_path: Path = IN_PATH, done_path: Path = DONE_PATH) -> dict:
    if get_order is None:
        from shared.binance_api import get_order as _go
        get_order = _go
    recs = _read(in_path)
    if not recs:
        print(f"没有待对账记录: {in_path}")
        return {"filled": 0, "canceled": 0, "unknown": 0}
    counts = {"filled": 0, "canceled": 0, "unknown": 0}
    keep, done = [], []
    for rec in recs:
        if set(rec) == {"_raw"}:
            counts["unknown"] += 1
            keep.append(rec)
            print(f"[unknown ] 无法解析的行，保留: {rec['_raw'][:80]}")
            continue
        status, od, note = classify(rec, get_order)
        counts[status] += 1
        sym, oid = rec.get("symbol", ""), _order_id(rec)
        print(f"[{status:8s}] {sym} order_id={oid} {note}")
        if status == "unknown":
            keep.append(rec)
            continue
        r2 = dict(rec)
        r2.update({
            "reconcile_status": status,
            "reconcile_ts": time.time(),
            "order_status": (od or {}).get("status"),
            "executed_qty": (od or {}).get("executedQty"),
            "avg_price": (od or {}).get("avgPrice"),
        })
        done.append(r2)
    print(f"\n合计 {len(recs)} 条：filled={counts['filled']} canceled={counts['canceled']} unknown={counts['unknown']}")
    if dry_run:
        print("--dry-run：未修改任何文件")
        return counts
    if done:
        done_path.parent.mkdir(parents=True, exist_ok=True)
        with open(done_path, "a", encoding="utf-8") as f:
            for r2 in done:
                f.write(_dump(r2) + "\n")
        # 原文件只保留仍未确认的记录（临时文件 + 原子替换）
        tmp = in_path.with_name(in_path.name + ".tmp")
        tmp.write_text("".join(_dump(r) + "\n" for r in keep), encoding="utf-8")
        os.replace(tmp, in_path)
        print(f"已处理 {len(done)} 条 → {done_path}；原文件保留 {len(keep)} 条")
    else:
        print("没有可确认的记录，原文件未改动")
    return counts


def main():
    ap = argparse.ArgumentParser(description="trigger_reconcile.jsonl 离线对账")
    ap.add_argument("--dry-run", action="store_true", help="只查询打印，不改任何文件")
    args = ap.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
