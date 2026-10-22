#!/usr/bin/env python3
"""
fx/daily_review.py — 每日复盘脚本
每天 UTC 02:00 由 cron 触发，汇总过去 24 小时的扫描/信号/开仓/盈亏数据，
推送结构化日报到 Jarvis。

数据来源：
  - fx/logs/scanner.log      → 扫描统计、拒绝原因
  - fx/logs/trade_log.jsonl  → 成交记录（pnl、策略、方向）
"""

import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).parent
LOG_DIR = ROOT / "logs"
SCANNER_LOG = LOG_DIR / "scanner.log"
TRADE_LOG   = LOG_DIR / "trade_log.jsonl"


def _utc_window(hours_back: int = 24):
    """返回过去 N 小时的起止 UTC timestamp（秒）"""
    now = datetime.now(timezone.utc)
    end = now
    start = now - timedelta(hours=hours_back)
    return start, end


def parse_scanner_log(start: datetime, end: datetime) -> dict:
    """从 scanner.log 解析扫描统计和拒绝原因"""
    result = {
        "scan_rounds":   0,
        "scanned_total": 0,
        "raw_signals":   0,    # 原始信号（judge 通过但可能被后续过滤）
        "final_signals": 0,    # 最终信号（进入执行队列）
        "placed":        0,    # 实际下单数
        "by_type": defaultdict(int),   # reversal/breakout/pullback 原始信号
        "rejected": defaultdict(int),  # 拒绝原因 → 次数
        "rejected_detail": [],          # 带 symbol 的明细（用于异常分析）
        "arb_placed": 0,
    }

    if not SCANNER_LOG.exists():
        return result

    # 时间戳格式：2026-10-18T05:14:56.482565+00:00
    scan_pat    = re.compile(
        r"\[scan\] (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})[^扫]*"
        r"扫描(\d+) reversal=(\d+) breakout=(\d+) pullback=(\d+) "
        r"原始信号(\d+) 最终信号(\d+) 开仓(\d+)"
    )
    reject_pat  = re.compile(r"\[rejected\] ([^:]+): (.+)$")
    arb_pat     = re.compile(r"\[arb_scan\].*已挂，等待成交")
    # timestamp on reject lines: e.g. "2026-10-18 05:14:56,123 ..."
    ts_pat      = re.compile(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})")

    def _parse_ts(line: str):
        m = ts_pat.search(line)
        if not m:
            return None
        try:
            s = m.group(1).replace("T", " ").split(".")[0]
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    # [rejected] 行无自身时间戳，用最近一次 [scan] 行的时间来判断是否在窗口内
    _cur_dt = None   # 当前 scan 段的时间戳，rejected 行沿用
    with open(SCANNER_LOG, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # 如果行本身有时间戳（[scan] / [arb_scan] 等），更新 _cur_dt
            dt = _parse_ts(line)
            if dt is not None:
                _cur_dt = dt

            # 决定当前行是否在统计窗口内
            effective_dt = _cur_dt
            if effective_dt is None or not (start <= effective_dt <= end):
                m_scan = scan_pat.search(line)
                if m_scan:
                    pass  # 时间不在窗口，跳过 scan 行
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
                symbol_rej = m_rej.group(1).strip()
                raw        = m_rej.group(2).strip()
                if raw.startswith("周末低分过滤"):
                    key = "周末低分过滤"
                elif raw.startswith("累计亏损黑名单"):
                    key = "累计亏损黑名单"
                else:
                    key = raw
                result["rejected"][key] += 1
                # 保存关键类别的明细（带 symbol）
                if any(k in raw for k in ("熊市空单被拒", "边界信号LLM", "LLM新闻", "一票否决")):
                    result["rejected_detail"].append({"symbol": symbol_rej, "reason": raw})
                continue

            if arb_pat.search(line):
                result["arb_placed"] += 1

    return result


def parse_trade_log(start: datetime, end: datetime) -> dict:
    """从 trade_log.jsonl 解析当日成交、盈亏"""
    result = {
        "trades":       [],   # 完整 trade 对象列表
        "opened":       [],   # 当日开仓（open_time 在窗口内）
        "closed":       [],   # 当日平仓（close_time 在窗口内）
        "total_pnl":    0.0,
        "win":          0,
        "loss":         0,
        "by_mode":      defaultdict(lambda: {"win": 0, "loss": 0, "pnl": 0.0, "count": 0}),
        "by_direction": defaultdict(lambda: {"win": 0, "loss": 0, "pnl": 0.0, "count": 0}),
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

            if start_ms <= close_ts <= end_ms:
                result["closed"].append(t)
                pnl = float(t.get("pnl_usdt", 0))
                result["total_pnl"] += pnl
                mode = t.get("mode", "unknown")
                dire = t.get("direction", "unknown")
                won  = pnl > 0

                if won:
                    result["win"] += 1
                else:
                    result["loss"] += 1

                result["by_mode"][mode]["count"] += 1
                result["by_mode"][mode]["pnl"]   += pnl
                result["by_mode"][mode]["win" if won else "loss"] += 1

                result["by_direction"][dire]["count"] += 1
                result["by_direction"][dire]["pnl"]   += pnl
                result["by_direction"][dire]["win" if won else "loss"] += 1

            if start_ms <= open_ts <= end_ms:
                result["opened"].append(t)

    return result


def format_daily_report(scan: dict, trade: dict, date_str: str) -> str:
    """生成中文日报文本"""
    lines = []
    lines.append(f"📊 **每日复盘 {date_str}**")
    lines.append("")

    # ── 1. 扫描统计 ──────────────────────────────────────────────────────────
    rounds = scan["scan_rounds"]
    avg_scanned = scan["scanned_total"] // rounds if rounds else 0
    lines.append("**① 扫描概览**")
    lines.append(f"• 扫描轮次: {rounds} 轮  均扫标的: {avg_scanned}")
    lines.append(
        f"• 原始信号: {scan['raw_signals']}  最终信号: {scan['final_signals']}  "
        f"实际开仓: {scan['placed'] + scan['arb_placed']}"
        + (f"（含套利 {scan['arb_placed']}）" if scan["arb_placed"] else "")
    )
    by = scan["by_type"]
    lines.append(
        f"• 策略分布: reversal={by['reversal']}  breakout={by['breakout']}  pullback={by['pullback']}"
    )
    lines.append("")

    # ── 2. 拒绝原因分布 ──────────────────────────────────────────────────────
    rej = scan["rejected"]
    total_rej = sum(rej.values())
    lines.append(f"**② 拒绝原因 Top10**（共 {total_rej} 次）")
    if rej:
        for reason, cnt in sorted(rej.items(), key=lambda x: -x[1])[:10]:
            pct = cnt / total_rej * 100 if total_rej else 0
            lines.append(f"  {cnt:4d} ({pct:4.1f}%)  {reason}")
    else:
        lines.append("  无拒绝记录（窗口内无数据）")
    lines.append("")

    # ── 2b. 关键拒绝明细（熊市空单/LLM 相关）────────────────────────────────
    details = scan.get("rejected_detail", [])
    bear_short_rej = [d for d in details if "熊市空单" in d["reason"]]
    if bear_short_rej:
        lines.append(f"**② 熊市空单费率拦截明细**（共 {len(bear_short_rej)} 笔）")
        # 去重：同一标的只取最极端的费率记录
        seen = {}
        for d in bear_short_rej:
            sym = d["symbol"]
            if sym not in seen:
                seen[sym] = d["reason"]
        for sym, reason in sorted(seen.items()):
            lines.append(f"  {sym}: {reason}")
        lines.append("  💡 若以上标的后续大幅下跌，考虑放宽 BTC_BEAR_SHORT_MIN_FUNDING 门槛")
    lines.append("")

    # ── 3. 开仓记录 ──────────────────────────────────────────────────────────
    opened = trade["opened"]
    lines.append(f"**③ 当日开仓 {len(opened)} 笔**")
    if opened:
        for t in opened[-10:]:  # 最多显示10笔
            sym  = t.get("symbol", "?")
            mode = t.get("mode", "?")
            dire = t.get("direction", "?")
            st   = t.get("signal_strength", "?")
            ep   = t.get("entry_price", 0)
            lines.append(f"  {sym}  {mode}/{dire}  {st}  @{ep:.6g}")
    else:
        lines.append("  无开仓")
    lines.append("")

    # ── 4. 当日盈亏 ──────────────────────────────────────────────────────────
    closed = trade["closed"]
    total_pnl = trade["total_pnl"]
    win  = trade["win"]
    loss = trade["loss"]
    total_trades = win + loss
    wr = win / total_trades * 100 if total_trades else 0

    lines.append(f"**④ 当日盈亏**（平仓 {total_trades} 笔）")
    pnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
    lines.append(f"  {pnl_emoji} 总盈亏: {total_pnl:+.2f}U  胜率: {wr:.0f}%  盈{win}/亏{loss}")

    if closed:
        lines.append("  平仓明细:")
        for t in sorted(closed, key=lambda x: x.get("close_time", 0))[-10:]:
            sym    = t.get("symbol", "?")
            pnl    = float(t.get("pnl_usdt", 0))
            reason = t.get("close_reason", "?")
            mode   = t.get("mode", "?")
            dire   = t.get("direction", "?")
            em     = "✅" if pnl > 0 else "❌"
            lines.append(f"  {em} {sym} {mode}/{dire}  {pnl:+.2f}U  [{reason}]")

    lines.append("")

    # ── 5. 策略维度 ──────────────────────────────────────────────────────────
    by_mode = trade["by_mode"]
    if by_mode:
        lines.append("**⑤ 各策略表现**")
        for mode, d in sorted(by_mode.items()):
            n   = d["count"]
            wr_ = d["win"] / n * 100 if n else 0
            lines.append(
                f"  {mode:12s}  {n}笔  胜率{wr_:.0f}%  盈亏{d['pnl']:+.2f}U"
            )

    lines.append("")
    lines.append("─" * 30)
    lines.append("⚠️ 胜率数据 <10 笔仅供参考，需累积更多样本")

    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=24, help="回溯小时数（默认24）")
    parser.add_argument("--dry-run", action="store_true", help="只打印，不发送")
    args = parser.parse_args()

    start, end = _utc_window(args.hours)
    date_str = (end - timedelta(hours=1)).strftime("%Y-%m-%d")  # 报告日期用昨天

    scan  = parse_scanner_log(start, end)
    trade = parse_trade_log(start, end)
    report = format_daily_report(scan, trade, date_str)

    print(report)

    if not args.dry_run:
        # 通过 Jarvis 推送
        try:
            import subprocess
            # 把报告写入临时文件再让 jarvis 发送，避免 shell 转义问题
            tmp = "/tmp/_daily_review_msg.txt"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(report)
            # 使用 openclaw message send（如果可用）
            subprocess.run(
                ["python3", "-c",
                 f"""
import json, subprocess, sys
with open('{tmp}') as f:
    msg = f.read()
# 直接用 notifier
_FX = Path(__file__).resolve().parent
sys.path.insert(0, str(_FX))
sys.path.insert(0, str(_FX / 'shared'))
import asyncio
from shared.notifier import notify_async
asyncio.run(notify_async(msg))
"""],
                check=True
            )
        except Exception as e:
            print(f"[daily_review] 发送失败: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
