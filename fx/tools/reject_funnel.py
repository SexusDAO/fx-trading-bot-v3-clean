#!/usr/bin/env python3
"""
tools/reject_funnel.py — 信号拒绝漏斗统计（只读，不改任何状态）

用途：评估「放宽/修复某条过滤」前后，信号数量与拒绝原因分布的变化。
输入：fx/logs/signal_analytics.jsonl（含轮转文件 .1~.5）。
用法：python3 tools/reject_funnel.py [--days 7] [--top 15]
输出：每个策略路径的通过率，以及按「拒绝原因（归一化）」统计的 Top N。
"""
import argparse
import json
import re
import time
from collections import Counter, defaultdict
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent / "logs"
PATCH_WINDOW_MS = 5 * 60 * 1000   # [v3.89 #43] 补丁行与其判定事件的最大时间差（补丁在判定之后另行追加，ts 不相同）


def _norm(reason: str) -> str:
    """把数字/价格归一化，使同类拒绝原因能聚合到一起。"""
    r = re.sub(r"-?\d+(\.\d+)?(e-?\d+)?%?", "#", reason or "未知")
    return r[:60]


def _load_records(files, cutoff):
    """
    [v3.89 #43] 读取并分类 signal_analytics 记录。
      verdict      含 judge_passed 字段的判定事件（只有它们进入通过率/拒绝统计）
      patch        type=entry_params_patch 等补丁记录：按 symbol(+mode)+ts 合并进最近的判定事件，不单独计数
      missing      有时间戳、非补丁，但缺 judge_passed 的记录（单独计数，不当作拒绝）
      corrupt      JSON 解析失败 / 不是对象 / 时间戳缺失或非数值（单独计数）
    """
    verdicts, patches = [], []
    stats = Counter()
    for f in files:
        for line in f.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                r = json.loads(line)
            except Exception:
                stats["corrupt"] += 1
                continue
            if not isinstance(r, dict):
                stats["corrupt"] += 1
                continue
            ts = r.get("ts")
            if isinstance(ts, bool) or not isinstance(ts, (int, float)):
                stats["corrupt"] += 1
                continue
            if ts < cutoff:
                continue
            if "judge_passed" in r:
                verdicts.append(r)
            elif r.get("type") == "entry_params_patch":
                patches.append(r)
            else:
                stats["missing_judge_passed"] += 1
    # 补丁合并：先精确 symbol+ts，其次同 symbol+mode 且在补丁之前 PATCH_WINDOW_MS 内最近的判定事件
    by_key = {(v.get("symbol"), v.get("ts")): v for v in verdicts}
    by_sym = defaultdict(list)
    for v in verdicts:
        by_sym[(v.get("symbol"), v.get("mode"))].append(v)
    for lst in by_sym.values():
        lst.sort(key=lambda x: x["ts"])
    for p in patches:
        tgt = by_key.get((p.get("symbol"), p.get("ts")))
        if tgt is None:
            cands = [v for v in by_sym.get((p.get("symbol"), p.get("mode")), [])
                     if 0 <= p["ts"] - v["ts"] <= PATCH_WINDOW_MS]
            tgt = cands[-1] if cands else None
        if tgt is None:
            stats["orphan_patch"] += 1
            continue
        for k, v in p.items():
            if k not in ("type", "ts", "symbol", "mode", "direction"):
                tgt.setdefault(k, v)
        stats["patch_merged"] += 1
    return verdicts, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=7)
    ap.add_argument("--top", type=int, default=15)
    a = ap.parse_args()
    cutoff = (time.time() - a.days * 86400) * 1000
    files = sorted(LOG_DIR.glob("signal_analytics*.jsonl"))
    total = defaultdict(lambda: [0, 0])          # mode -> [通过, 总数]
    rej = defaultdict(Counter)                   # mode -> Counter(归一化原因)
    verdicts, stats = _load_records(files, cutoff)
    for r in verdicts:
        m = r.get("mode", "?")
        total[m][1] += 1
        if r.get("judge_passed"):
            total[m][0] += 1
        else:
            rej[m][_norm(r.get("reject_reason"))] += 1
    if not total:
        print("没有找到 signal_analytics 判定记录（检查 fx/logs/ 或 --days）")
        print_data_quality(stats)
        return
    print(f"== 最近 {a.days:g} 天各路径通过率 ==")
    for m, (p, n) in sorted(total.items(), key=lambda x: -x[1][1]):
        print(f"{m:<14} 通过 {p:>5} / {n:<6} = {p / n * 100:5.1f}%")
    for m in total:
        print(f"\n== {m} 拒绝原因 Top {a.top} ==")
        for reason, c in rej[m].most_common(a.top):
            print(f"{c:>6}  {reason}")
    print_data_quality(stats)


def print_data_quality(stats) -> None:
    print("\n== 数据质量（不计入上面的通过率/拒绝统计）==")
    print(f"损坏记录（JSON/时间戳无效）: {stats.get('corrupt', 0)}")
    print(f"缺 judge_passed 的非补丁记录: {stats.get('missing_judge_passed', 0)}")
    print(f"已合并的补丁记录: {stats.get('patch_merged', 0)}")
    print(f"找不到对应判定事件的孤立补丁: {stats.get('orphan_patch', 0)}")


if __name__ == "__main__":
    main()
