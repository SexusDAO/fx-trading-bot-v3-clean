"""rejected_log.jsonl 清理工具：只保留最近 N 天（默认 7 天）的被拒信号记录。

用法（在仓库根目录）：
    python3 fx/tools/cleanup_rejected_log.py --dry-run   # 只打印将删除/保留多少条，不改文件
    python3 fx/tools/cleanup_rejected_log.py             # 实际清理
    python3 fx/tools/cleanup_rejected_log.py --days 14   # 自定义保留天数

说明：
  - 以记录的 ts 字段判断时间（shadow_trace 写入的是毫秒时间戳，秒级时间戳也兼容）。
  - 缺少 ts、ts 无法解析、或无法解析的坏行一律保留，不删除。
  - 写回时先写 .tmp 再原子替换；运行中的系统可能同时在追加写，建议在低峰期执行。
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "rejected_log.jsonl"


def _ts_seconds(rec: dict):
    v = rec.get("ts")
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v / 1000.0 if v > 1e11 else v      # 毫秒 → 秒


def run(days: float = 7, dry_run: bool = False, path: Path = LOG_PATH, now: float | None = None) -> dict:
    if not path.exists():
        print(f"文件不存在: {path}")
        return {"delete": 0, "keep": 0}
    cutoff = (now if now is not None else time.time()) - days * 86400
    keep, delete = [], 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            ts = _ts_seconds(json.loads(line))
        except Exception:
            ts = None
        if ts is not None and ts < cutoff:
            delete += 1
        else:
            keep.append(line)
    print(f"{path}: 将删除 {delete} 条（早于 {days:g} 天），保留 {len(keep)} 条")
    if dry_run:
        print("--dry-run：未修改文件")
    elif delete:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text("".join(l + "\n" for l in keep), encoding="utf-8")
        os.replace(tmp, path)
        print("清理完成")
    else:
        print("无需清理")
    return {"delete": delete, "keep": len(keep)}


def main():
    ap = argparse.ArgumentParser(description="清理 rejected_log.jsonl，只保留最近 N 天")
    ap.add_argument("--days", type=float, default=7, help="保留天数（默认 7）")
    ap.add_argument("--dry-run", action="store_true", help="只打印不实际删除")
    args = ap.parse_args()
    run(days=args.days, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
