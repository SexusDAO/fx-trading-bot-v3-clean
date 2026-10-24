"""ema_trend 被拒信号分析（只读）：按拒绝规则分组，统计拒绝次数；若有影子结果则统计止盈/止损次数与 EV。

用法（在仓库根目录）：
    python3 fx/tools/analyze_ema_trend_rejects.py
    python3 fx/tools/analyze_ema_trend_rejects.py --days 7          # 只看最近 7 天
    python3 fx/tools/analyze_ema_trend_rejects.py --log PATH --outcomes PATH   # 指定文件（默认 fx/logs/ 下）

数据来源：
    logs/rejected_log.jsonl       影子追踪写入的被拒记录（shared/shadow_trace.py）
    logs/rejected_outcomes.jsonl  由 tools/shadow_verify.py 补标的结果（可选，不存在则只统计次数）
脚本只读文件，不修改任何东西，不联网。

EV 口径（务必注意）：
    - 单位是 R（1R = 入场到止损的距离），不含手续费与滑点，也不是百分比收益。
    - 先止盈记 +RR，先止损记 -1R。RR = |tp1-入场| / |止损-入场|；记录没有止损止盈价（兜底 ±1ATR）时 RR=1。
    - 未成交 / 超时 / 无法判定 不计入 EV，只单独列出次数。
    - 同一标的短时间内可能被反复拒绝并重复记录，样本并不独立，次数会被放大；解读时请结合标的数。
"""
from __future__ import annotations

import argparse
import json
import re
import time
from collections import defaultdict
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_PATH = LOG_DIR / "rejected_log.jsonl"
OUT_PATH = LOG_DIR / "rejected_outcomes.jsonl"

# judge_ema_trend 的拒绝原因 → 规则名（按 shared/judges.py 中 reject_reason 文案的关键片段匹配，顺序即优先级）
_RULES = (
    ("4H EMA 排列不明确", "1.4H EMA排列不明确"),
    ("不追高", "2a.chg24过大"),
    ("大跌应走reversal", "2b.chg24过小"),
    ("波动率过低", "3.波动率过低(ATR比例)"),
    ("4H EMA 数据不可用", "4.4H EMA数据不可用"),
    ("排列不稳固", "5.EMA排列不稳固"),
    ("1H RSI", "6.1H RSI不在区间"),
    ("4H RSI", "7.4H RSI不在区间"),
    ("价格追高", "8.价格偏离EMA20(过高)"),
    ("价格破位", "8.价格偏离EMA20(破位)"),
    ("SHORT 价格高于", "8.价格偏离EMA20(过高)"),
    ("SHORT 价格过低", "8.价格偏离EMA20(过低)"),
    ("量能不足", "9.量比过低"),
    ("未见触发K线", "10.无触发K线"),
    ("资金费率", "11.资金费率"),
)


def _norm_reason(reason: str) -> str:
    return re.sub(r"-?\d+(\.\d+)?(e-?\d+)?%?", "#", reason or "")[:30]


def rule_of(rec: dict) -> str:
    """judge_ema_trend 拒绝 → 规则名；其他阶段（扫描后处理/下单前）按拦截关卡名。"""
    first = str(rec.get("first_rejected_at") or "?")
    if first != "judge_ema_trend":
        return f"[{first}]"
    reason = str(rec.get("reason") or "")
    for frag, name in _RULES:
        if frag in reason:
            return name
    return "其他:" + _norm_reason(reason)


def _read_jsonl(path: Path) -> list:
    out = []
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _rr(rec: dict) -> float:
    try:
        e, sl, tp = float(rec["entry_candidate"]), float(rec["sl"]), float(rec["tp1"])
        d = abs(e - sl)
        return abs(tp - e) / d if d > 0 else 1.0
    except Exception:
        return 1.0   # 无止损止盈（兜底 ±1ATR）


def analyze(log_recs: list, outcome_recs: list, days: float | None = None, now_ms: float | None = None) -> list:
    """返回按拒绝次数从高到低排序的规则统计列表。纯函数，不做任何 IO。"""
    cutoff = None
    if days is not None:
        cutoff = (now_ms if now_ms is not None else time.time() * 1000) - days * 86400_000
    outcomes = {}
    for r in outcome_recs:
        if r.get("signal_type") == "ema_trend" and isinstance(r.get("outcome"), dict):
            outcomes[(r.get("ts"), r.get("symbol"))] = r
    stats = defaultdict(lambda: {"n": 0, "tp": 0, "sl": 0, "nofill": 0, "timeout": 0, "unk": 0,
                                 "pnl_r": 0.0, "labeled": 0})
    for rec in log_recs:
        if rec.get("signal_type") != "ema_trend":
            continue
        if cutoff is not None and float(rec.get("ts") or 0) < cutoff:
            continue
        st = stats[rule_of(rec)]
        st["n"] += 1
        o = outcomes.get((rec.get("ts"), rec.get("symbol")))
        if not o:
            continue
        label = o["outcome"].get("label")
        st["labeled"] += 1
        if label == "先止盈":
            st["tp"] += 1
            st["pnl_r"] += _rr(o)
        elif label == "先止损":
            st["sl"] += 1
            st["pnl_r"] -= 1.0
        elif label == "未成交":
            st["nofill"] += 1
        elif label == "超时":
            st["timeout"] += 1
        else:
            st["unk"] += 1
    rows = []
    for rule, st in stats.items():
        done = st["tp"] + st["sl"]
        rows.append({**st, "rule": rule,
                     "win_rate": (st["tp"] / done) if done else None,
                     "ev_r": (st["pnl_r"] / done) if done else None})
    rows.sort(key=lambda r: (-r["n"], r["rule"]))
    return rows


def render(rows: list, has_outcomes: bool) -> str:
    if not rows:
        return "没有 ema_trend 被拒记录"
    lines = []
    if has_outcomes:
        lines.append(f"{'规则':<26}{'拒绝次数':>8}{'已标注':>7}{'止盈':>6}{'止损':>6}{'未成交':>7}{'超时':>6}{'未判':>6}{'胜率':>8}{'EV(R)':>9}")
        for r in rows:
            wr = f"{r['win_rate']*100:.0f}%" if r["win_rate"] is not None else "-"
            ev = f"{r['ev_r']:+.2f}" if r["ev_r"] is not None else "-"
            lines.append(f"{r['rule']:<26}{r['n']:>8}{r['labeled']:>7}{r['tp']:>6}{r['sl']:>6}"
                         f"{r['nofill']:>7}{r['timeout']:>6}{r['unk']:>6}{wr:>8}{ev:>9}")
    else:
        lines.append(f"{'规则':<26}{'拒绝次数':>8}")
        for r in rows:
            lines.append(f"{r['rule']:<26}{r['n']:>8}")
        lines.append("（未找到 rejected_outcomes.jsonl，只统计次数；可先运行 tools/shadow_verify.py 生成结果）")
    lines.append(f"合计 {sum(r['n'] for r in rows)} 条。EV 单位为 R，不含手续费/滑点；"
                 f"同一标的重复拒绝会放大次数，样本不独立。")
    return "\n".join(lines)


def main(argv=None) -> list:
    ap = argparse.ArgumentParser(description="ema_trend 被拒原因分组统计（只读）")
    ap.add_argument("--log", default=str(LOG_PATH))
    ap.add_argument("--outcomes", default=str(OUT_PATH))
    ap.add_argument("--days", type=float, default=None, help="只统计最近 N 天")
    a = ap.parse_args(argv)
    log_p, out_p = Path(a.log), Path(a.outcomes)
    if not log_p.exists():
        print(f"没有 {log_p}")
        return []
    rows = analyze(_read_jsonl(log_p), _read_jsonl(out_p), days=a.days)
    print(render(rows, has_outcomes=out_p.exists()))
    return rows


if __name__ == "__main__":
    main()
