#!/usr/bin/env python3
"""
tools/param_sensitivity.py — 参数敏感性分析

读取 signal_analytics.jsonl 中已有 outcome 的记录，
输出多维度参数敏感性报告。

调用方式：
    python3 tools/param_sensitivity.py           # 手动运行（过去14天）
    python3 tools/param_sensitivity.py --days 30 # 指定天数
    daily_report()                                # 从 archiver.py 调用
"""
import sys, json, time, logging
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger("fx.param_sensitivity")


def _load_filled(fp: Path, days: int = 14) -> list[dict]:
    """加载已有 outcome（filled=True 且 checked_at 不为 null）的记录。"""
    now_ms    = int(time.time() * 1000)
    cutoff_ms = now_ms - days * 86400 * 1000
    records   = []
    with open(fp, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if not rec.get("judge_passed"):
                continue
            out = rec.get("outcome") or {}
            if not out.get("filled") or out.get("checked_at") is None:
                continue
            if rec.get("ts", 0) < cutoff_ms:
                continue
            records.append(rec)
    return records


def _win(rec: dict) -> bool:
    return (rec.get("outcome") or {}).get("pnl_pct", -999) > 0


def _pnl(rec: dict) -> float:
    return (rec.get("outcome") or {}).get("pnl_pct", 0) or 0


def _snap(rec: dict, key: str, default=None):
    return (rec.get("snapshot") or {}).get(key, default)


def _analyze_dimension(records: list[dict], label: str,
                        key_fn, bins: list, bin_labels: list) -> list[dict]:
    """按连续值分段分析。"""
    rows = []
    for (lo, hi), lbl in zip(bins, bin_labels):
        sub = [r for r in records if lo <= (key_fn(r) or 0) < hi]
        if not sub:
            continue
        wins = [r for r in sub if _win(r)]
        avg_w = sum(_pnl(r) for r in wins) / len(wins) if wins else 0
        losses = [r for r in sub if not _win(r)]
        avg_l = sum(_pnl(r) for r in losses) / len(losses) if losses else 0
        wr = len(wins) / len(sub) * 100
        exp = sum(_pnl(r) for r in sub) / len(sub)
        rows.append({
            "label":    lbl,
            "n":        len(sub),
            "win_rate": round(wr, 0),
            "exp":      round(exp, 2),
            "avg_win":  round(avg_w, 2),
            "avg_loss": round(avg_l, 2),
        })
    return rows


def _table(rows: list[dict], cols: list[tuple]) -> str:
    """渲染 ASCII 表格。"""
    lines = []
    header = "  " + "  ".join(f"{name:<{w}}" for name, w, _ in cols)
    lines.append(header)
    lines.append("  " + "-" * (sum(w + 2 for _, w, _ in cols)))
    for row in rows:
        lines.append("  " + "  ".join(f"{row.get(k, ''):<{w}}" for _, w, k in cols))
    return "\n".join(lines)


def daily_report(days: int = 14, analytics_file: Path | None = None) -> str:
    """
    生成参数敏感性报告文本，供 archiver.py 调用或直接打印。
    """
    from config import SIGNAL_ANALYTICS_FILE
    fp = analytics_file or (ROOT / SIGNAL_ANALYTICS_FILE)
    if not fp.exists() or fp.stat().st_size < 100:
        return "signal_analytics.jsonl 数据不足，跳过参数敏感性分析。"

    records = _load_filled(fp, days=days)
    if len(records) < 10:
        return f"有效记录仅 {len(records)} 条（需≥10），暂不生成报告。"

    import datetime as _dt
    now_str  = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    d_start  = _dt.datetime.utcfromtimestamp(
        (int(time.time()) - days * 86400)).strftime("%Y-%m-%d")
    d_end    = _dt.datetime.utcnow().strftime("%Y-%m-%d")

    lines = [
        "═" * 60,
        f"参数敏感性分析报告（{d_start} ~ {d_end}，{len(records)} 条）",
        f"生成时间: {now_str}",
        "═" * 60,
    ]

    # ── 1. 按策略类型 ────────────────────────────────────────────────────────
    lines.append("\n【策略类型总览】\n")
    cols = [("策略", 12, "label"), ("信号数", 6, "n"),
            ("胜率%", 6, "win_rate"), ("期望收益%", 10, "exp"),
            ("均盈%", 8, "avg_win"), ("均亏%", 8, "avg_loss")]
    modes = sorted(set(r.get("mode","?") for r in records))
    rows_mode = []
    for m in modes:
        sub = [r for r in records if r.get("mode") == m]
        wins = [r for r in sub if _win(r)]
        losses = [r for r in sub if not _win(r)]
        rows_mode.append({
            "label": m,
            "n": len(sub),
            "win_rate": round(len(wins)/len(sub)*100, 0) if sub else 0,
            "exp": round(sum(_pnl(r) for r in sub)/len(sub), 2) if sub else 0,
            "avg_win": round(sum(_pnl(r) for r in wins)/len(wins), 2) if wins else 0,
            "avg_loss": round(sum(_pnl(r) for r in losses)/len(losses), 2) if losses else 0,
        })
    lines.append(_table(rows_mode, cols))

    # ── 2. vol_ratio 敏感性（reversal SHORT）──────────────────────────────────
    rev_short = [r for r in records if r.get("mode") == "reversal"
                 and r.get("direction","").upper() == "SHORT"]
    if rev_short:
        lines.append(f"\n\n【reversal SHORT · vol_ratio 门槛分析（{len(rev_short)} 条）】\n")
        bins   = [(0,0.3),(0.3,0.4),(0.4,0.5),(0.5,0.6),(0.6,0.8),(0.8,99)]
        labels = ["<0.3","0.3~0.4","0.4~0.5","0.5~0.6","0.6~0.8",">0.8"]
        rows = _analyze_dimension(rev_short, "vol_ratio",
                                  lambda r: _snap(r, "vol_ratio", 0), bins, labels)
        lines.append(_table(rows, [("vol_ratio", 10, "label"), ("信号数", 6, "n"),
                                   ("胜率%", 6, "win_rate"), ("期望%", 8, "exp")]))

    # ── 3. bear 市 LONG pullback signal_strength ─────────────────────────────
    bear_long_pb = [r for r in records
                    if r.get("mode") == "pullback"
                    and r.get("direction","").upper() == "LONG"
                    and r.get("btc_trend") == "down"]
    if bear_long_pb:
        lines.append(f"\n\n【pullback LONG · bear 市 · signal_strength 分析（{len(bear_long_pb)} 条）】\n")
        rows_str = []
        for strength in ["strong", "medium", "weak"]:
            sub = [r for r in bear_long_pb
                   if (r.get("reason") or {}).get("signal_strength") == strength]
            if not sub: continue
            wins = [r for r in sub if _win(r)]
            losses = [r for r in sub if not _win(r)]
            rows_str.append({
                "label":    strength,
                "n":        len(sub),
                "win_rate": round(len(wins)/len(sub)*100, 0),
                "exp":      round(sum(_pnl(r) for r in sub)/len(sub), 2),
                "avg_win":  round(sum(_pnl(r) for r in wins)/len(wins), 2) if wins else 0,
                "avg_loss": round(sum(_pnl(r) for r in losses)/len(losses), 2) if losses else 0,
            })
        if rows_str:
            cols2 = [("强度", 8, "label"), ("信号数", 6, "n"),
                     ("胜率%", 6, "win_rate"), ("期望%", 8, "exp"),
                     ("均盈%", 8, "avg_win"), ("均亏%", 8, "avg_loss")]
            lines.append(_table(rows_str, cols2))
            # 自动结论
            med = next((r for r in rows_str if r["label"] == "medium"), None)
            if med and med["exp"] < 0:
                lines.append(f"\n  ⚠️ bear 市 medium LONG pullback 期望收益 {med['exp']:.2f}% < 0，建议过滤")
            elif med:
                lines.append(f"\n  ✅ bear 市 medium LONG pullback 期望收益 {med['exp']:.2f}% > 0，暂不需要过滤")

    # ── 4. SL 距离 vs 胜率 ───────────────────────────────────────────────────
    lines.append(f"\n\n【止损距离 vs 胜率（全策略）】\n")
    def sl_dist(r):
        ep = r.get("entry_params") or {}
        return abs(ep.get("sl_pct") or 0)
    sl_bins   = [(0,2),(2,4),(4,7),(7,99)]
    sl_labels = ["<2%","2~4%","4~7%",">7%"]
    rows_sl = _analyze_dimension(records, "sl_dist", sl_dist, sl_bins, sl_labels)
    lines.append(_table(rows_sl, [("SL距离", 8, "label"), ("信号数", 6, "n"),
                                   ("胜率%", 6, "win_rate"), ("期望%", 8, "exp")]))

    # ── 5. ATR ratio 分布 ────────────────────────────────────────────────────
    lines.append(f"\n\n【ATR ratio vs 胜率】\n")
    atr_bins   = [(0,1.5),(1.5,2.5),(2.5,4.0),(4.0,99)]
    atr_labels = ["<1.5%","1.5~2.5%","2.5~4.0%",">4.0%"]
    rows_atr = _analyze_dimension(records, "atr_ratio",
                                   lambda r: _snap(r, "atr_ratio", 0),
                                   atr_bins, atr_labels)
    lines.append(_table(rows_atr, [("ATR", 12, "label"), ("信号数", 6, "n"),
                                    ("胜率%", 6, "win_rate"), ("期望%", 8, "exp")]))

    lines.append("\n" + "═" * 60)
    report = "\n".join(lines)
    logger.info(f"[param_sensitivity] 报告生成完毕，{len(records)} 条记录")
    return report


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    print(daily_report(days=args.days))
