#!/usr/bin/env python3
"""
fx/weekly_review.py — 每周复盘脚本
每周一 UTC 02:00 由 cron 触发，汇总过去 7 天的策略胜率、拒绝原因分布、
并自动扫描潜在漏掉的机会（高涨幅但未开仓标的）。
"""

import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT    = Path(__file__).parent
LOG_DIR = ROOT / "logs"
SCANNER_LOG = LOG_DIR / "scanner.log"
TRADE_LOG   = LOG_DIR / "trade_log.jsonl"


def _utc_window(days_back: int = 7):
    now   = datetime.now(timezone.utc)
    end   = now
    start = now - timedelta(days=days_back)
    return start, end


# ── 复用 daily_review 的解析函数 ─────────────────────────────────────────────
def parse_scanner_log_range(start: datetime, end: datetime) -> dict:
    result = {
        "scan_rounds":   0,
        "scanned_total": 0,
        "raw_signals":   0,
        "final_signals": 0,
        "placed":        0,
        "by_type": defaultdict(int),
        "rejected": defaultdict(int),
        "arb_placed": 0,
    }
    if not SCANNER_LOG.exists():
        return result

    scan_pat   = re.compile(
        r"\[scan\] (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})[^扫]*"
        r"扫描(\d+) reversal=(\d+) breakout=(\d+) pullback=(\d+) "
        r"原始信号(\d+) 最终信号(\d+) 开仓(\d+)"
    )
    reject_pat = re.compile(r"\[rejected\] [^:]+: (.+)$")
    arb_pat    = re.compile(r"\[arb_scan\].*已挂，等待成交")
    ts_pat     = re.compile(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})")

    def _parse_ts(line):
        m = ts_pat.search(line)
        if not m:
            return None
        try:
            s  = m.group(1).replace("T", " ").split(".")[0]
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None

    _cur_dt = None
    with open(SCANNER_LOG, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            dt = _parse_ts(line)
            if dt is not None:
                _cur_dt = dt
            effective_dt = _cur_dt
            if effective_dt is None or not (start <= effective_dt <= end):
                continue
            m_scan = scan_pat.search(line)
            if m_scan:
                result["scan_rounds"]   += 1
                result["scanned_total"] += int(m_scan.group(2))
                result["by_type"]["reversal"]  += int(m_scan.group(3))
                result["by_type"]["breakout"]  += int(m_scan.group(4))
                result["by_type"]["pullback"]  += int(m_scan.group(5))
                result["raw_signals"]   += int(m_scan.group(6))
                result["final_signals"] += int(m_scan.group(7))
                result["placed"]        += int(m_scan.group(8))
                continue
            m_rej = reject_pat.search(line)
            if m_rej:
                raw = m_rej.group(1).strip()
                if raw.startswith("周末低分过滤"):
                    key = "周末低分过滤（各分值）"
                elif raw.startswith("累计亏损黑名单"):
                    key = "累计亏损黑名单"
                else:
                    key = raw
                result["rejected"][key] += 1
                continue
            if arb_pat.search(line):
                result["arb_placed"] += 1

    return result


def parse_trade_log_range(start: datetime, end: datetime) -> dict:
    result = {
        "closed":       [],
        "opened":       [],
        "total_pnl":    0.0,
        "win":          0,
        "loss":         0,
        "by_mode":      defaultdict(lambda: {"win": 0, "loss": 0, "pnl": 0.0, "count": 0,
                                              "hold_min": [], "pnl_pct": []}),
        "by_strength":  defaultdict(lambda: {"win": 0, "loss": 0, "pnl": 0.0, "count": 0}),
        "by_direction": defaultdict(lambda: {"win": 0, "loss": 0, "pnl": 0.0, "count": 0}),
        "close_reasons": defaultdict(int),
        "missed_candidates": [],   # 标的出现过信号但全部被拒绝
    }
    if not TRADE_LOG.exists():
        return result

    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(end.timestamp() * 1000)

    with open(TRADE_LOG, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
            except Exception:
                continue

            close_ts = t.get("close_time", 0)
            open_ts  = t.get("open_time", 0)

            if start_ms <= open_ts <= end_ms:
                result["opened"].append(t)

            if start_ms <= close_ts <= end_ms:
                result["closed"].append(t)
                pnl  = float(t.get("pnl_usdt", 0))
                mode = t.get("mode", "unknown")
                dire = t.get("direction", "unknown")
                st   = t.get("signal_strength", "unknown")
                won  = pnl > 0

                result["total_pnl"] += pnl
                if won:
                    result["win"] += 1
                else:
                    result["loss"] += 1

                for d_key, group_val in [
                    (result["by_mode"][mode], mode),
                    (result["by_strength"][st], st),
                    (result["by_direction"][dire], dire),
                ]:
                    d_key["count"] += 1
                    d_key["pnl"]   += pnl
                    d_key["win" if won else "loss"] += 1

                result["by_mode"][mode]["hold_min"].append(
                    float(t.get("holding_minutes", 0))
                )
                result["by_mode"][mode]["pnl_pct"].append(
                    float(t.get("pnl_pct", 0))
                )
                result["close_reasons"][t.get("close_reason", "unknown")] += 1

    return result


def find_missed_opportunities(scan: dict) -> list:
    """
    从拒绝原因里找「系统性漏掉机会」的信号：
    某标的持续被槽位满/评分低拒绝，但其实价格后来大涨/大跌
    当前实现：返回槽位相关拒绝最集中的时段（暗示那段时间机会密度高但槽位满了）
    """
    rej = scan["rejected"]
    slot_full = rej.get("多单槽位已满", 0) + rej.get("空单槽位已满", 0)
    score_low = rej.get("周末低分过滤（各分值）", 0)
    corr      = sum(v for k, v in rej.items() if "相关性" in k)
    hints     = []
    total_rej = sum(rej.values())

    if total_rej == 0:
        return hints

    slot_pct = slot_full / total_rej * 100 if total_rej else 0
    if slot_pct > 40:
        hints.append(
            f"🔴 槽位占满比例高达 {slot_pct:.0f}%（{slot_full}/{total_rej}），"
            f"可能错过行情密集期的跟进机会。"
            f"建议：检查 MAX_POSITIONS 是否过低，或调整相关性上限。"
        )
    if corr > 20:
        hints.append(
            f"🟡 相关性过滤拒绝 {corr} 次，说明同方向信号集中。"
            f"如果这些信号后来都盈利，CORR_MAX 可以适当放宽。"
        )
    if score_low > 50:
        hints.append(
            f"🟡 周末低分过滤拒绝 {score_low} 次，"
            f"若其中不少是 strong 信号降分被拦，可考虑适当降低周末 strong 门槛。"
        )

    return hints


def format_weekly_report(scan: dict, trade: dict, start: datetime, end: datetime) -> str:
    week_str = f"{start.strftime('%m/%d')}–{end.strftime('%m/%d')}"
    lines = []
    lines.append(f"📅 **每周复盘 {week_str}**")
    lines.append("")

    # ── 1. 扫描汇总 ──────────────────────────────────────────────────────────
    rounds = scan["scan_rounds"]
    avg_s  = scan["scanned_total"] // rounds if rounds else 0
    lines.append("**① 扫描汇总（7天）**")
    lines.append(
        f"• 扫描轮次: {rounds}  均扫标的: {avg_s}  "
        f"原始信号: {scan['raw_signals']}  最终信号: {scan['final_signals']}"
    )
    by = scan["by_type"]
    lines.append(
        f"• 策略原始信号: reversal={by['reversal']}  "
        f"breakout={by['breakout']}  pullback={by['pullback']}"
    )
    lines.append("")

    # ── 2. 各策略胜率 ────────────────────────────────────────────────────────
    by_mode = trade["by_mode"]
    closed  = trade["closed"]
    total_n = len(closed)
    total_pnl = trade["total_pnl"]
    win       = trade["win"]
    loss      = trade["loss"]
    wr_total  = win / total_n * 100 if total_n else 0

    lines.append(f"**② 各策略胜率**（本周平仓 {total_n} 笔）")
    pnl_em = "🟢" if total_pnl >= 0 else "🔴"
    lines.append(
        f"  {pnl_em} 总盈亏: {total_pnl:+.2f}U  "
        f"整体胜率: {wr_total:.0f}%  盈{win}/亏{loss}"
    )
    lines.append("")
    if by_mode:
        lines.append("  策略  |  笔数 | 胜率  | 盈亏    | 均持（min）| 均盈亏%")
        lines.append("  " + "─" * 54)
        for mode, d in sorted(by_mode.items()):
            n    = d["count"]
            wr   = d["win"] / n * 100 if n else 0
            pnl  = d["pnl"]
            note = " ⚠️ 样本少" if n < 10 else ""
            hold = (sum(d["hold_min"]) / len(d["hold_min"])) if d["hold_min"] else 0
            avg_pct = (sum(d["pnl_pct"]) / len(d["pnl_pct"])) if d["pnl_pct"] else 0
            lines.append(
                f"  {mode:12s} | {n:4d}  | {wr:4.0f}% | {pnl:+6.2f}U | "
                f"{hold:6.0f}      | {avg_pct:+5.1f}%{note}"
            )
    else:
        lines.append("  本周暂无平仓记录")
    lines.append("")

    # ── 3. 平仓原因分布 ──────────────────────────────────────────────────────
    cr = trade["close_reasons"]
    total_cr = sum(cr.values())
    lines.append(f"**③ 平仓原因分布**（共 {total_cr} 笔）")
    for reason, cnt in sorted(cr.items(), key=lambda x: -x[1]):
        pct = cnt / total_cr * 100 if total_cr else 0
        label_map = {
            "sl_triggered": "止损触发",
            "tp1_triggered": "TP1 触发",
            "tp2_triggered": "TP2 触发",
            "time_stop": "时间止损",
            "max_hold": "最大持仓时间",
            "health_score": "健康评分平仓",
            "rate_reversed": "费率反转（arb）",
            "rate_normalized": "费率收窄（arb）",
            "max_renew": "arb 最大续持",
            "manual": "手动平仓",
        }
        label = label_map.get(reason, reason)
        lines.append(f"  {cnt:4d} ({pct:4.1f}%)  {label}")
    lines.append("")

    # ── 4. 拒绝原因分布 ──────────────────────────────────────────────────────
    rej       = scan["rejected"]
    total_rej = sum(rej.values())
    lines.append(f"**④ 信号拒绝原因 Top15**（共 {total_rej} 次）")
    if rej:
        for reason, cnt in sorted(rej.items(), key=lambda x: -x[1])[:15]:
            pct = cnt / total_rej * 100 if total_rej else 0
            lines.append(f"  {cnt:4d} ({pct:4.1f}%)  {reason}")
    lines.append("")

    # ── 5. 漏掉机会分析 ──────────────────────────────────────────────────────
    hints = find_missed_opportunities(scan)
    lines.append("**⑤ 潜在漏机会分析**")
    if hints:
        for h in hints:
            lines.append(f"  {h}")
    else:
        lines.append("  未发现明显系统性漏机会")
    lines.append("")

    # ── 6. 信号强度分布 ──────────────────────────────────────────────────────
    by_st = trade["by_strength"]
    if by_st:
        lines.append("**⑥ 信号强度分布**")
        for st, d in sorted(by_st.items()):
            n   = d["count"]
            wr  = d["win"] / n * 100 if n else 0
            lines.append(
                f"  {st:8s}  {n}笔  胜率{wr:.0f}%  盈亏{d['pnl']:+.2f}U"
            )
        lines.append("")

    lines.append("─" * 30)
    lines.append("⚠️ 各策略 <10 笔胜率数据仅供参考")
    lines.append("📌 建议每周 check: 槽位满占比是否 >40%，reversal 逆势信号是否过多被过滤")

    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    start, end = _utc_window(args.days * 24)

    scan  = parse_scanner_log_range(start, end)
    trade = parse_trade_log_range(start, end)
    report = format_weekly_report(scan, trade, start, end)

    print(report)

    if not args.dry_run:
        try:
            import subprocess
            tmp = "/tmp/_weekly_review_msg.txt"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(report)
            subprocess.run(
                ["python3", "-c",
                 f"""
import sys
_FX = Path(__file__).resolve().parent
sys.path.insert(0, str(_FX))
sys.path.insert(0, str(_FX / 'shared'))
with open('{tmp}') as f:
    msg = f.read()
import asyncio
from shared.notifier import notify_async
asyncio.run(notify_async(msg))
"""],
                check=True
            )
        except Exception as e:
            print(f"[weekly_review] 发送失败: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
