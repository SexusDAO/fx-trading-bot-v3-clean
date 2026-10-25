#!/usr/bin/env python3
"""tools/daily_dingtalk.py — 每日 UTC 00:00 钉钉摘要（前一个 UTC 日）+ 触发器日报文本。

复用 daily_review.parse_scanner_log / parse_trade_log，不重写解析。只发钉钉（Jarvis 的完整日报另有）。
用法：python3 tools/daily_dingtalk.py [--date YYYY-MM-DD] [--dry-run]   （--date 默认前一个 UTC 日）
口径说明：
  · 进入监控 = 最终信号（进入执行队列的信号数）；开仓成交 = 扫描日志的实际下单数（含套利），非交易所成交回报
  · 熔断状态只读 circuit_breaker.json（不调用 is_circuit_open，避免跨日重置副作用）
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def build_daily_text(date_str: str, rounds: int, raw: int, final: int, placed: int, pnl: float, circuit_open: bool) -> str:
    return (f"📊 每日报告 {date_str}\n"
            f"扫描次数：{rounds} 轮\n"
            f"原始信号：{raw} 个\n"
            f"进入监控：{final} 个\n"
            f"开仓成交：{placed} 笔\n"
            f"今日盈亏：{pnl:+.2f} U\n"
            f"熔断状态：{'触发' if circuit_open else '正常'}")


def read_circuit_open(cb_file: Path) -> bool:
    """只读 circuit_breaker.json 的 is_open；读不到按未触发。"""
    try:
        return bool(json.loads(Path(cb_file).read_text(encoding="utf-8")).get("is_open", False))
    except Exception:
        return False


def build_trigger_text(stats: dict, prev: dict | None, trigger_count: int) -> str:
    """stats/prev 为 TriggerDetector.stats（进程内累计）；prev=None 表示自启动以来。"""
    prev = prev or {}
    d = lambda k: max(int(stats.get(k, 0)) - int(prev.get(k, 0)), 0)
    title = "🔍 触发器日报" + ("" if prev else "（自启动以来）")
    return (f"{title}\n"
            f"触发次数：{trigger_count}\n"
            f"miss型1：{d('miss')}（扫描有信号触发器没触发）\n"
            f"误触发：{d('fp')}\n"
            f"抑制：{d('suppressed')}")


def count_triggers(trigger_log: Path, since_ms: int, until_ms: int) -> int:
    """trigger_log.jsonl 中触发记录数：有 trigger_id 且无 type 字段（analysis / post_price 行带 type），ts 在窗口内。"""
    n = 0
    try:
        with open(trigger_log, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if "type" in r or "trigger_id" not in r:
                    continue
                if since_ms <= int(r.get("ts", 0) or 0) < until_ms:
                    n += 1
    except FileNotFoundError:
        return 0
    return n


def build_for_date(day: datetime) -> str:
    import daily_review as DR
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    scan = DR.parse_scanner_log(start, end)
    trade = DR.parse_trade_log(start, end)
    from shared import state as st
    return build_daily_text(start.strftime("%Y-%m-%d"), scan["scan_rounds"], scan["raw_signals"], scan["final_signals"],
                            scan["placed"] + scan["arb_placed"], trade["total_pnl"], read_circuit_open(st._CB_FILE))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="UTC 日期 YYYY-MM-DD，默认前一个 UTC 日")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    now = datetime.now(timezone.utc)
    day = (datetime.strptime(a.date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if a.date
           else (now - timedelta(days=1)))
    text = build_for_date(day)
    print(text)
    if not a.dry_run:
        from shared.notifier import dingtalk_notify
        dingtalk_notify(text, wait=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
