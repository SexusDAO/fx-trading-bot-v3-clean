#!/usr/bin/env python3
"""
analyze_trades.py — 逐策略 P&L 聚合分析
用法: python3 analyze_trades.py [--days 30] [--mode pullback] [--json]

功能:
  1. 逐策略胜率/期望值/平均盈亏
  2. 出场类型分布（tp1/tp2/sl/time_stop/mfe_lock）
  3. 信号强度分层对比（strong vs medium）
  4. 市场模式分层（如有 market_mode 字段）
  5. 滚动 7 日净值曲线
  6. 简易回测框架：读历史 trade_log，输出每策略当前期望值
"""
import json
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

LOG_PATH = Path(__file__).parent / "logs" / "trade_log.jsonl"


def load_trades(days: int = 90, mode_filter: str = None) -> list[dict]:
    if not LOG_PATH.exists():
        return []
    cutoff_ms = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000
    trades = []
    with open(LOG_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
                # 时间过滤
                ts = float(t.get("close_time") or t.get("open_time") or 0)
                if ts < cutoff_ms:
                    continue
                # sig_type / mode 优先级
                sig = t.get("sig_type") or t.get("mode") or "unknown"
                t["_strategy"] = sig
                if mode_filter and sig != mode_filter:
                    continue
                trades.append(t)
            except Exception:
                pass
    return trades


def calc_strategy_stats(trades: list[dict]) -> dict:
    """按策略分组，计算胜率/期望值/平均 RR"""
    groups: dict[str, list] = defaultdict(list)
    for t in trades:
        groups[t["_strategy"]].append(t)

    results = {}
    for strategy, ts in sorted(groups.items()):
        pnls = [float(t.get("pnl_usdt") or 0) for t in ts]
        pnl_pcts = [float(t.get("pnl_pct") or 0) * 100 for t in ts]
        wins  = [p for p in pnls if p > 0]
        loses = [p for p in pnls if p <= 0]
        n = len(ts)
        win_rate = len(wins) / n if n > 0 else 0
        avg_win  = sum(wins)  / len(wins)  if wins  else 0
        avg_loss = sum(loses) / len(loses) if loses else 0
        ev = win_rate * avg_win + (1 - win_rate) * avg_loss  # 期望值 USDT

        # 出场类型分布
        exit_types: dict[str, int] = defaultdict(int)
        for t in ts:
            reason = t.get("close_reason") or t.get("exit_reason") or "unknown"
            # 归一化
            if "tp1" in reason or "take_profit_1" in reason:
                key = "tp1"
            elif "tp2" in reason or "take_profit_2" in reason:
                key = "tp2"
            elif reason.startswith("sl") or "stop_loss" in reason:
                key = "sl"
            elif "time_stop" in reason:
                key = "time_stop"
            elif "mfe" in reason or "drawback" in reason:
                key = "mfe_lock"
            elif "funding" in reason and "arb" in reason.lower():
                key = "funding_settle"
            else:
                key = reason[:20]
            exit_types[key] += 1

        # RR（粗算：avg_win / avg_loss）
        rr = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

        results[strategy] = {
            "n":          n,
            "win_rate":   round(win_rate * 100, 1),
            "ev_usdt":    round(ev, 2),
            "avg_win":    round(avg_win, 2),
            "avg_loss":   round(avg_loss, 2),
            "rr":         round(rr, 2),
            "total_pnl":  round(sum(pnls), 2),
            "exit_types": dict(exit_types),
        }
    return results


def calc_strength_breakdown(trades: list[dict]) -> dict:
    """信号强度（strong/medium）分层"""
    groups: dict[str, list] = defaultdict(list)
    for t in trades:
        s = t.get("signal_strength") or "unknown"
        groups[s].append(float(t.get("pnl_usdt") or 0))
    result = {}
    for strength, pnls in groups.items():
        wins = [p for p in pnls if p > 0]
        n = len(pnls)
        result[strength] = {
            "n":        n,
            "win_rate": round(len(wins) / n * 100, 1) if n > 0 else 0,
            "avg_pnl":  round(sum(pnls) / n, 2) if n > 0 else 0,
            "total":    round(sum(pnls), 2),
        }
    return result


def rolling_equity(trades: list[dict], window_days: int = 30) -> list[tuple]:
    """按日期聚合 P&L，输出 (date_str, cumulative_pnl)"""
    daily: dict[str, float] = defaultdict(float)
    for t in trades:
        ts = float(t.get("close_time") or 0) / 1000
        if ts > 0:
            day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            daily[day] += float(t.get("pnl_usdt") or 0)

    sorted_days = sorted(daily.keys())[-window_days:]
    cum = 0.0
    curve = []
    for d in sorted_days:
        cum += daily[d]
        curve.append((d, round(cum, 2)))
    return curve


def print_report(trades: list[dict], days: int, as_json: bool = False):
    stats   = calc_strategy_stats(trades)
    strength = calc_strength_breakdown(trades)
    curve   = rolling_equity(trades)

    if as_json:
        print(json.dumps({
            "strategy_stats": stats,
            "strength_breakdown": strength,
            "rolling_equity": curve,
        }, ensure_ascii=False, indent=2))
        return

    total_pnl = sum(float(t.get("pnl_usdt") or 0) for t in trades)
    print(f"\n{'='*65}")
    print(f"  P&L 分析报告  |  过去 {days} 天  |  共 {len(trades)} 笔  |  总盈亏 {total_pnl:+.2f}U")
    print(f"{'='*65}")

    # ── 逐策略 ──
    print(f"\n{'策略':<16} {'笔数':>5} {'胜率':>7} {'期望值':>8} {'RR':>6} {'总P&L':>9}")
    print("-" * 55)
    for strat, s in stats.items():
        ev_flag = "🟢" if s["ev_usdt"] > 0 else "🔴"
        print(
            f"  {strat:<14} {s['n']:>5} {s['win_rate']:>6.1f}% "
            f"{ev_flag}{s['ev_usdt']:>+7.2f}U  {s['rr']:>5.2f}x  {s['total_pnl']:>+8.2f}U"
        )

    # ── 出场类型汇总 ──
    all_exits: dict[str, int] = defaultdict(int)
    for s in stats.values():
        for k, v in s["exit_types"].items():
            all_exits[k] += v
    print(f"\n出场类型分布:")
    total_exits = sum(all_exits.values()) or 1
    for k, v in sorted(all_exits.items(), key=lambda x: -x[1]):
        print(f"  {k:<18}  {v:>4} ({v/total_exits*100:.1f}%)")

    # ── 信号强度分层 ──
    print(f"\n信号强度分层:")
    for s, d in strength.items():
        print(f"  {s:<10}  n={d['n']:>4}  胜率={d['win_rate']:>5.1f}%  均盈亏={d['avg_pnl']:>+7.2f}U  总={d['total']:>+8.2f}U")

    # ── 滚动净值 ──
    if curve:
        print(f"\n最近 {len(curve)} 天净值曲线:")
        for day, cum in curve[-14:]:
            bar = "█" * max(0, int((cum / max(abs(c) for _, c in curve) * 15)))
            sign = "+" if cum >= 0 else ""
            print(f"  {day}  {sign}{cum:>8.2f}U  {bar}")

    # ── 策略建议 ──
    print(f"\n{'='*65}")
    print("  建议:")
    for strat, s in stats.items():
        if s["n"] >= 5:
            if s["ev_usdt"] < 0:
                print(f"  🔴  {strat}: 负期望值 ({s['ev_usdt']:+.2f}U/笔)，考虑暂停或降仓")
            elif s["ev_usdt"] < 0.5 and s["n"] >= 10:
                print(f"  🟡  {strat}: 期望值偏低 ({s['ev_usdt']:+.2f}U/笔)，观察中")
        else:
            print(f"  ⚪  {strat}: 样本不足 ({s['n']} 笔)，数据积累中")
    print()


def main():
    parser = argparse.ArgumentParser(description="FX 交易策略 P&L 分析")
    parser.add_argument("--days",  type=int, default=90, help="分析最近 N 天 (默认90)")
    parser.add_argument("--mode",  type=str, default=None, help="只看某策略 (pullback/reversal/breakout/stop_hunt)")
    parser.add_argument("--json",  action="store_true", help="输出 JSON 格式")
    args = parser.parse_args()

    trades = load_trades(days=args.days, mode_filter=args.mode)
    if not trades:
        print(f"⚠️  在 {LOG_PATH} 中未找到最近 {args.days} 天的交易记录")
        sys.exit(0)

    print_report(trades, days=args.days, as_json=args.json)


if __name__ == "__main__":
    main()
