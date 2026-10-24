#!/usr/bin/env python3
"""
执行质量分析（只读 fx/logs/trade_log.jsonl，不依赖实盘账户，不改任何数据）。

用法：
    python3 fx/tools/analyze_quality.py                 # 默认读 fx/logs/trade_log.jsonl
    python3 fx/tools/analyze_quality.py --log /path/to/trade_log.jsonl

口径说明：
  · 胜率 / 平均盈亏 / 期望值 一律按「净盈亏」算：pnl_net_usdt；缺失时用 pnl_usdt - fees_usdt；
    再缺失（例如 WS 路径平仓的记录没有手续费字段）就用 pnl_usdt，并在输出里标出缺手续费的笔数
  · 期望值/笔 = 平均净盈亏（等价于 胜率×平均盈利 - 负率×平均亏损）
  · 平均手续费占比 = fees_usdt / 名义仓位(nominal_value)，只统计两个字段都有的记录
  · 滑点：入场滑点取 entry_slippage_pct；止损滑点取 sl_slippage_pct（仅止损出场的单子）。
    sl_slippage_pct 在日志里是 (出场价-止损价)/止损价 的原始符号，这里同时给出「按方向换算」的不利滑点
    （多头价格更低、空头价格更高 = 不利，记为正数）
  · 止损质量：sl_quality 由止损后 2 小时的行情事后回写，缺失说明尚未回写或不是止损单，不计入占比分母
  · MFE/MAE 单位是 %（日志里已乘 100）；MAE 取绝对值；MFE/MAE 比值 = 平均 MFE ÷ 平均 |MAE|
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_FX = Path(__file__).resolve().parent.parent
DEFAULT_LOG = _FX / "logs" / "trade_log.jsonl"
STRATEGIES = ["reversal", "pullback", "ema_trend", "breakout"]
REASONS = ["sl", "tp1", "tp2", "time_stop", "manual_close"]
_REASON_ALIAS = {"sl_triggered": "sl", "tp1_hit": "tp1", "tp2_hit": "tp2"}


def _num(v):
    """转 float；None / 空 / 非数字返回 None。"""
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x else None          # 过滤 NaN


def load_records(path: Path) -> list:
    """逐行读 jsonl；坏行（含并发写入的半行）直接跳过。文件不存在返回空列表。"""
    path = Path(path)
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if isinstance(d, dict):
                out.append(d)
    return out


def is_trade(r: dict) -> bool:
    """只统计有盈亏字段的成交记录。"""
    return _num(r.get("pnl_usdt")) is not None or _num(r.get("pnl_net_usdt")) is not None


def net_pnl(r: dict):
    n = _num(r.get("pnl_net_usdt"))
    if n is not None:
        return n
    g = _num(r.get("pnl_usdt"))
    if g is None:
        return None
    f = _num(r.get("fees_usdt"))
    return g - abs(f) if f is not None else g


def norm_reason(r: dict) -> str:
    c = str(r.get("close_reason") or r.get("exit_reason") or "unknown")
    return _REASON_ALIAS.get(c, c)


def norm_strategy(r: dict) -> str:
    return str(r.get("mode") or r.get("strategy") or "unknown")


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _basic(trades: list) -> dict:
    nets = [net_pnl(t) for t in trades]
    nets = [x for x in nets if x is not None]
    n = len(nets)
    out = {"count": n}
    if n == 0:
        return out
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x <= 0]
    out.update({"wins": len(wins), "win_rate": len(wins) / n,
                "avg_pnl": sum(nets) / n,
                "avg_win": (sum(wins) / len(wins)) if wins else None,
                "avg_loss": (sum(losses) / len(losses)) if losses else None,
                "net_total": sum(nets)})
    return out


def analyze(records: list) -> dict:
    trades = [r for r in records if is_trade(r)]
    res = {"records": len(records), "trades": len(trades)}
    if not trades:
        return res
    gross = [_num(t.get("pnl_usdt")) for t in trades]
    fees = [abs(_num(t.get("fees_usdt"))) for t in trades if _num(t.get("fees_usdt")) is not None]
    ratios = []
    for t in trades:
        f, nv = _num(t.get("fees_usdt")), _num(t.get("nominal_value") if t.get("nominal_value") is not None
                                                else t.get("notional"))
        if f is not None and nv and nv > 0:
            ratios.append(abs(f) / nv * 100)
    hold = [_num(t.get("holding_minutes") if t.get("holding_minutes") is not None else t.get("hold_min"))
            for t in trades]
    overall = _basic(trades)
    overall.update({"gross_total": sum(g for g in gross if g is not None),
                    "fees_total": sum(fees), "missing_fees": len(trades) - len(fees),
                    "avg_hold_min": _mean(hold), "avg_fee_ratio_pct": _mean(ratios), "fee_ratio_n": len(ratios)})
    res["overall"] = overall

    # 按策略
    by_s, seen = {}, set(STRATEGIES)
    for s in STRATEGIES:
        sub = [t for t in trades if norm_strategy(t) == s]
        by_s[s] = _basic(sub)
    others = sorted({norm_strategy(t) for t in trades} - seen)
    for s in others:
        by_s[s] = _basic([t for t in trades if norm_strategy(t) == s])
    res["by_strategy"] = by_s

    # 滑点
    ent = [_num(t.get("entry_slippage_pct")) for t in trades]
    ent = [x for x in ent if x is not None]
    sl_raw, sl_adv = [], []
    for t in trades:
        if norm_reason(t) != "sl":
            continue
        v = _num(t.get("sl_slippage_pct"))
        if v is None:
            continue
        sl_raw.append(v)
        sl_adv.append(-v if str(t.get("direction", "")).lower() == "long" else v)
    res["slippage"] = {"entry_avg": _mean(ent), "entry_n": len(ent),
                       "entry_abs_avg": _mean([abs(x) for x in ent]),
                       "sl_avg_raw": _mean(sl_raw), "sl_avg_adverse": _mean(sl_adv),
                       "sl_abs_avg": _mean([abs(x) for x in sl_raw]), "sl_n": len(sl_raw)}

    # 止损质量
    q = [str(t.get("sl_quality")) for t in trades if norm_reason(t) == "sl" and t.get("sl_quality") in ("noise", "valid")]
    sl_total = sum(1 for t in trades if norm_reason(t) == "sl")
    res["sl_quality"] = {"sl_trades": sl_total, "labeled": len(q), "noise": q.count("noise"), "valid": q.count("valid")}

    # MFE / MAE
    mfe = [_num(t.get("mfe_pct")) for t in trades]
    mae = [_num(t.get("mae_pct")) for t in trades]
    pairs = [(a, abs(b)) for a, b in zip(mfe, mae) if a is not None and b is not None]
    mfe_avg = _mean([a for a, _ in pairs])
    mae_avg = _mean([b for _, b in pairs])
    res["mfe_mae"] = {"n": len(pairs), "mfe_avg": mfe_avg, "mae_avg": mae_avg,
                      "ratio": (mfe_avg / mae_avg) if (mfe_avg is not None and mae_avg and mae_avg > 0) else None}

    # 按平仓原因
    cnt = {}
    for t in trades:
        cnt[norm_reason(t)] = cnt.get(norm_reason(t), 0) + 1
    order = REASONS + sorted(set(cnt) - set(REASONS))
    res["by_reason"] = {k: {"count": cnt.get(k, 0), "share": cnt.get(k, 0) / len(trades)} for k in order
                        if k in REASONS or cnt.get(k)}
    return res


# ───────────────────────── 输出 ─────────────────────────
def _f(v, d=2, suf=""):
    return "-" if v is None else f"{v:.{d}f}{suf}"


def _pct(v, d=1):
    return "-" if v is None else f"{v * 100:.{d}f}%"


def render(res: dict) -> str:
    if not res.get("trades"):
        return "暂无成交记录"
    o = res["overall"]
    L = ["=" * 78, f"执行质量分析  成交 {res['trades']} 笔（日志共 {res['records']} 行）", "=" * 78, "【1. 总体】",
         f"  总成交笔数     {o['count']}",
         f"  胜率           {_pct(o.get('win_rate'))}  （按净盈亏>0 计）",
         f"  平均净盈亏     {_f(o.get('avg_pnl'), 4)} U   平均盈利 {_f(o.get('avg_win'), 4)} U   平均亏损 {_f(o.get('avg_loss'), 4)} U",
         f"  期望值/笔      {_f(o.get('avg_pnl'), 4)} U",
         f"  毛盈亏合计     {_f(o.get('gross_total'), 2)} U   手续费合计 {_f(o.get('fees_total'), 2)} U   "
         f"净盈亏合计 {_f(o.get('net_total'), 2)} U",
         f"  平均持仓       {_f(o.get('avg_hold_min'), 1)} 分钟",
         f"  平均手续费占名义仓位  {_f(o.get('avg_fee_ratio_pct'), 4, '%')}  （样本 {o.get('fee_ratio_n', 0)} 笔）"]
    if o.get("missing_fees"):
        L.append(f"  注意：{o['missing_fees']} 笔没有手续费字段（如 WS 路径平仓），这些笔的净盈亏按毛盈亏计")
    L.append("【2. 按策略】")
    L.append(f"  {'策略':<12}{'笔数':>6}{'胜率':>9}{'平均净盈亏(U)':>16}{'净盈亏合计(U)':>16}")
    for s, b in res["by_strategy"].items():
        if b.get("count"):
            L.append(f"  {s:<12}{b['count']:>6}{_pct(b['win_rate']):>9}{_f(b['avg_pnl'], 4):>16}{_f(b['net_total'], 2):>16}")
        else:
            L.append(f"  {s:<12}{0:>6}{'-':>9}{'-':>16}{'-':>16}")
    sl = res["slippage"]
    L += ["【3. 滑点】",
          f"  入场滑点  平均 {_f(sl['entry_avg'], 4, '%')}  平均绝对值 {_f(sl['entry_abs_avg'], 4, '%')}  （样本 {sl['entry_n']}）",
          f"  止损滑点  平均(原始符号) {_f(sl['sl_avg_raw'], 4, '%')}  平均不利滑点 {_f(sl['sl_avg_adverse'], 4, '%')}  "
          f"平均绝对值 {_f(sl['sl_abs_avg'], 4, '%')}  （样本 {sl['sl_n']}）",
          "  说明：不利滑点为正 = 止损成交比止损价更差；原始符号是 (出场价-止损价)/止损价"]
    q = res["sl_quality"]
    L.append("【4. 止损质量】")
    if q["labeled"]:
        L.append(f"  止损单 {q['sl_trades']} 笔，已标注 {q['labeled']} 笔：noise(噪音止损) {q['noise']} 笔 "
                 f"{_pct(q['noise'] / q['labeled'])}；valid(止损正确) {q['valid']} 笔 {_pct(q['valid'] / q['labeled'])}")
    else:
        L.append(f"  止损单 {q['sl_trades']} 笔，暂无 sl_quality 标注（止损后 2 小时才回写）")
    m = res["mfe_mae"]
    L.append("【5. MFE / MAE】")
    if m["n"]:
        L.append(f"  平均 MFE {_f(m['mfe_avg'], 3, '%')}   平均 |MAE| {_f(m['mae_avg'], 3, '%')}   "
                 f"MFE/MAE {_f(m['ratio'], 2)}  （样本 {m['n']}；>1 表示平均有利偏移大于不利偏移）")
    else:
        L.append("  暂无 mfe_pct / mae_pct 数据")
    L.append("【6. 按平仓原因】")
    for k, b in res["by_reason"].items():
        L.append(f"  {k:<14}{b['count']:>6} 笔  {_pct(b['share'])}")
    L.append("=" * 78)
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="执行质量分析（读 trade_log.jsonl）")
    ap.add_argument("--log", default=str(DEFAULT_LOG), help="trade_log.jsonl 路径")
    a = ap.parse_args(argv)
    print(render(analyze(load_records(Path(a.log)))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
