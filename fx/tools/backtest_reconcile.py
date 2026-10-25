#!/usr/bin/env python3
"""
backtest_reconcile.py — 回测出场逻辑对账（步骤3）

用 trade_log.jsonl 里真实的入场（入场价/止损/TP1/入场时间），在本地 1H K 线上用回测的出场逻辑
（backtest_ema_trend.manage_bar：止损 / TP1 / 时间止损 / 移动止损）重新模拟出场，
再与真实出场对比：出场原因、出场时间（K线根数）、盈亏。

用法（在 fx/ 目录或仓库根目录均可）：
    python3 fx/tools/backtest_reconcile.py                    # 默认 ema_trend；<10 笔自动改为全部策略
    python3 fx/tools/backtest_reconcile.py --mode pullback
    python3 fx/tools/backtest_reconcile.py --mode all

数据来源与合并（重要）：
    - 平仓记录（含 trade_id / close_reason / exit_price / open_time / close_time）里没有 sl_price、tp1_price；
      这两个价格只在开仓记录（含 pos_id / fill_price / sl_price / tp1_price / ts）里。
      本工具按「同标的 + 同方向 + 开仓记录时间落在 [open_time-5分钟, close_time+5分钟] + 成交价与入场价偏差<1%」
      把开仓记录配给平仓记录；配不上的笔数在报告里列出并跳过。
    - 入场时刻优先用开仓记录的成交时间 ts（限价单 open_time 可能早于成交），没有则用 open_time。
    - 价格数据：fx/data/klines/{symbol}_1h.parquet（由 tools/data_downloader.py 下载）。

只调用业务纯函数（经 backtest_ema_trend 复用）：_calc_trail_sl、scanner.calc_indicators（仅用于估算 ATR）；
不修改任何业务模块；不联网。
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from bisect import bisect_right
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import backtest_ema_trend as B  # noqa: E402

HOUR_MS = B.HOUR_MS
MIN_N_DEFAULT = 10            # ema_trend 可对账笔数不足此值 → 自动切换为全部策略
MATCH_SLACK_MS = 5 * 60_000   # 开仓记录与平仓记录的时间配对容差
MATCH_PRICE_TOL = 0.01        # 成交价与入场价相对偏差上限
TRUST_REASON_MISMATCH_MAX = 0.20   # 出场原因不一致率 < 20%
TRUST_PNL_DIFF_R_MAX = 0.20        # 平均 |盈亏差| < 0.2R（R = 入场到初始止损的距离）
SIM_MAX_BARS = 300            # 模拟最多向后推进的 K 线根数
SKIP_REASONS = {"missed_unfilled", "already_closed"}   # 未成交/已平：无真实出场可比


# ───────────────────────── 读取与配对 ─────────────────────────
def read_jsonl(path: Path) -> list:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _ts_ms(s) -> int | None:
    """开仓记录的 ts 是 utcnow().isoformat()（无时区，UTC）。"""
    try:
        d = datetime.fromisoformat(str(s))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return int(d.timestamp() * 1000)
    except Exception:
        return None


def _f(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def split_records(recs: list) -> tuple:
    """→ (平仓记录, 开仓记录)。平仓：有 close_reason 与 trade_id；开仓：有 pos_id 与 fill_price。"""
    closes, opens = [], []
    for r in recs:
        if "close_reason" in r and ("trade_id" in r or "close_time" in r):
            closes.append(r)
        elif "pos_id" in r and "fill_price" in r:
            o = dict(r)
            o["_ts_ms"] = _ts_ms(r.get("ts"))
            if o["_ts_ms"]:
                opens.append(o)
    return closes, opens


def rec_mode(r: dict) -> str:
    return str(r.get("mode") or r.get("sig_type") or "unknown")


def merge_close_with_open(closes: list, opens: list) -> tuple:
    """给每条平仓记录配开仓记录，取 sl_price/tp1_price/成交时间。返回 (候选列表, 统计 dict)。"""
    st = {"close_total": len(closes), "skipped_reason": 0, "bad_fields": 0, "no_open_match": 0}
    used: set = set()
    by_sym: dict = {}
    for n, o in enumerate(opens):
        by_sym.setdefault(o.get("symbol"), []).append((o["_ts_ms"], n, o))
    for v in by_sym.values():
        v.sort(key=lambda x: x[0])
    cands = []
    for c in sorted(closes, key=lambda r: _f(r.get("open_time"))):
        if str(c.get("close_reason", "")) in SKIP_REASONS:
            st["skipped_reason"] += 1
            continue
        entry, exit_p = _f(c.get("entry_price")), _f(c.get("exit_price"))
        t_open, t_close = int(_f(c.get("open_time"))), int(_f(c.get("close_time")))
        sym = c.get("symbol")
        d = str(c.get("direction", "")).upper()
        if not (entry > 0 and exit_p > 0 and t_open > 0 and t_close > t_open and sym and d in ("LONG", "SHORT")):
            st["bad_fields"] += 1
            continue
        sl, tp1 = _f(c.get("sl_price")), _f(c.get("tp1_price"))
        fill_ts, atr = t_open, _f(c.get("atr_at_entry"))
        if not (sl > 0 and tp1 > 0):                      # 平仓记录自身没带 → 去开仓记录里找
            hit = None
            for ts, n, o in by_sym.get(sym, []):
                if n in used or ts < t_open - MATCH_SLACK_MS or ts > t_close + MATCH_SLACK_MS:
                    continue
                if str(o.get("direction", "")).upper() != d:
                    continue
                fp = _f(o.get("fill_price"))
                if fp <= 0 or abs(fp - entry) / entry > MATCH_PRICE_TOL:
                    continue
                hit = (n, o)
                break
            if hit is None:
                st["no_open_match"] += 1
                continue
            used.add(hit[0])
            o = hit[1]
            sl, tp1 = _f(o.get("sl_price")), _f(o.get("tp1_price"))
            fill_ts = max(o["_ts_ms"], t_open) if o["_ts_ms"] <= t_close else t_open
            if not (sl > 0 and tp1 > 0):
                st["bad_fields"] += 1
                continue
        cands.append({"c": c, "symbol": sym, "direction": d, "mode": rec_mode(c), "entry": entry, "exit": exit_p,
                      "sl": sl, "tp1": tp1, "t_open": t_open, "t_fill": fill_ts, "t_close": t_close, "atr": atr})
    st["candidates"] = len(cands)
    return cands, st


# ───────────────────────── 模拟 ─────────────────────────
def real_category(reason: str) -> str:
    r = str(reason or "").lower()
    if r.startswith("sl"):
        return "止损"
    if r.startswith("tp"):
        return "止盈"
    if r.startswith("time_stop") or r == "max_hold":
        return "时间止损"
    return "其他"          # manual_close / crash_guard / mfe_drawback_* 等回测未建模


def sim_category(reason: str) -> str:
    if reason.startswith("sl"):
        return "止损"
    if reason.startswith("tp"):
        return "止盈"
    if reason == "time":
        return "时间止损"
    return "未平仓"


def estimate_atr(mods: dict, rows: list, k: int, k4_all: list, k4_ends: list, symbol: str) -> float:
    """入场时的 ATR（对账记录没带 atr_at_entry 时用本地K线估算；失败返回 0）。"""
    try:
        k1h, k4h = B.build_windows(rows, k, k4_all, k4_ends)
        ind = mods["calc_indicators"](k1h, k1h, k4h)
        return float(ind.get("atr_1h") or 0.0)
    except Exception:
        return 0.0


def simulate_one(mods: dict, cand: dict, rows: list, opens: list, k4_all: list, k4_ends: list,
                 funding: list, tp1_ratio: float) -> dict:
    """返回 {"status": "ok"/"skip", ...}。"""
    k = bisect_right(opens, cand["t_fill"]) - 1
    if k < 0 or cand["t_fill"] >= rows[k][0] + HOUR_MS:
        return {"status": "skip", "why": "入场时间不在本地K线范围内"}
    if k + 1 >= len(rows):
        return {"status": "skip", "why": "入场后无K线数据"}
    d, entry = cand["direction"], cand["entry"]
    sl_frac = abs(entry - cand["sl"]) / entry
    if sl_frac <= 0:
        return {"status": "skip", "why": "止损价等于入场价"}
    atr, atr_src = cand["atr"], "trade_log"
    if atr <= 0:
        atr, atr_src = estimate_atr(mods, rows, k, k4_all, k4_ends, cand["symbol"]), "本地K线估算"
    pos = {"symbol": cand["symbol"], "direction": d, "tier": str(cand["c"].get("volatility_tier", "")),
           "strength": str(cand["c"].get("signal_strength", "")), "entry_idx": k, "entry_time": cand["t_fill"],
           "entry_price": entry, "sl_initial": cand["sl"], "sl": cand["sl"], "tp1": cand["tp1"], "atr": atr,
           "sl_frac": sl_frac, "rr_tp1": None, "sl_degraded": False, "tp_degraded": False,
           "risk_amount": 1.0, "notional": entry, "qty0": 1.0, "qty": 1.0, "tp1_filled": False,
           "gross": 0.0, "fees": 0.0, "slippage": 0.0, "funding": 0.0, "legs": [],
           "trail": {"entry_price": entry, "direction": d, "mode": cand["mode"], "atr_at_entry": atr,
                     "sl_price": cand["sl"], "tp1_filled": False, "rr_protection_triggered": False}}
    done = False
    for j in range(k + 1, min(len(rows), k + 1 + SIM_MAX_BARS)):
        if B.manage_bar(pos, rows[j], j, funding, mods["trail"], tp1_ratio):
            done = True
            break
    if not done:
        return {"status": "skip", "why": "模拟未在可用K线内平仓（数据不足）"}
    sim_pct = pos["gross"] / entry * 100.0                   # 价格口径收益率（不含手续费/资金费）
    return {"status": "ok", "sim_reason": pos["exit_reason"], "sim_cat": sim_category(pos["exit_reason"]),
            "sim_exit_bar": pos["legs"][-1]["time"], "sim_pct": sim_pct, "sim_tp1_hit": pos["tp1_filled"],
            "sl_dist_pct": sl_frac * 100.0, "atr": atr, "atr_src": atr_src,
            "sim_exit_price": pos["legs"][-1]["price"]}


def compare(cand: dict, sim: dict) -> dict:
    c = cand["c"]
    d, entry, ex = cand["direction"], cand["entry"], cand["exit"]
    real_pct = (ex - entry) / entry * 100.0 if d == "LONG" else (entry - ex) / entry * 100.0
    real_cat = real_category(c.get("close_reason"))
    real_bar = cand["t_close"] // HOUR_MS * HOUR_MS
    diff_bars = (sim["sim_exit_bar"] - real_bar) / HOUR_MS
    diff_pp = sim["sim_pct"] - real_pct
    return {"symbol": cand["symbol"], "direction": d, "mode": cand["mode"], "tier": c.get("volatility_tier", ""),
            "t_fill": cand["t_fill"], "t_close_real": cand["t_close"], "entry": entry,
            "sl": cand["sl"], "tp1": cand["tp1"], "sl_dist_pct": round(sim["sl_dist_pct"], 4),
            "real_reason": c.get("close_reason"), "real_cat": real_cat, "real_tp1_filled": bool(c.get("tp1_filled")),
            "real_exit_price": ex, "real_pct": round(real_pct, 4),
            "sim_reason": sim["sim_reason"], "sim_cat": sim["sim_cat"], "sim_exit_price": sim["sim_exit_price"],
            "sim_exit_bar_open": sim["sim_exit_bar"], "sim_pct": round(sim["sim_pct"], 4),
            "reason_match": real_cat == sim["sim_cat"], "modelable": real_cat != "其他",
            "diff_bars": diff_bars, "diff_pp": round(diff_pp, 4),
            "diff_R": round(diff_pp / sim["sl_dist_pct"], 4) if sim["sl_dist_pct"] else 0.0,
            "atr": round(sim["atr"], 8), "atr_src": sim["atr_src"]}


# ───────────────────────── 报告 ─────────────────────────
def _mean(xs):
    return statistics.fmean(xs) if xs else 0.0


def verdict(rows: list, min_n: int) -> tuple:
    """返回 (文字, 指标 dict)。只对「出场原因回测可建模」的笔数评估。"""
    m = [r for r in rows if r["modelable"]]
    if len(m) < min_n:
        return f"样本不足（可建模笔数 {len(m)} < {min_n}），无法判定回测是否可信", {}
    mismatch = 1 - sum(r["reason_match"] for r in m) / len(m)
    mean_abs_r = _mean([abs(r["diff_R"]) for r in m])
    ok = mismatch < TRUST_REASON_MISMATCH_MAX and mean_abs_r < TRUST_PNL_DIFF_R_MAX
    txt = (f"{'可信' if ok else '不可信'}：出场原因不一致率 {mismatch*100:.1f}%（阈值 <{TRUST_REASON_MISMATCH_MAX*100:.0f}%），"
           f"平均 |盈亏差| {mean_abs_r:.3f}R（阈值 <{TRUST_PNL_DIFF_R_MAX}R）")
    return txt, {"mismatch": mismatch, "mean_abs_diff_R": mean_abs_r, "trusted": ok}


def build_report(rows: list, st: dict, mode_used: str, fallback: bool, tp1_ratio: float,
                 sim_skips: dict, top: int, min_n: int) -> str:
    L = ["═" * 60, f"回测出场对账报告｜策略范围：{mode_used}{'（ema_trend 不足，自动改为全部策略）' if fallback else ''}", "═" * 60]
    L.append(f"平仓记录 {st['close_total']} 条 → 不可比(未成交/已平) {st['skipped_reason']}、字段不全 {st['bad_fields']}、"
             f"配不上开仓记录(无 sl/tp1) {st['no_open_match']} → 候选 {st['candidates']} → "
             f"策略过滤后 {st.get('after_mode', st['candidates'])} → 模拟成功 {len(rows)}")
    for why, n in sorted(sim_skips.items()):
        L.append(f"  · 模拟跳过 {n} 笔：{why}")
    if not rows:
        L.append("\n没有可对账的交易。")
        return "\n".join(L)
    m = [r for r in rows if r["modelable"]]
    L.append(f"其中出场原因回测可建模 {len(m)} 笔；真实出场为 manual_close/crash_guard/mfe_drawback 等回测未建模的 {len(rows)-len(m)} 笔"
             f"（只计入时间/盈亏统计的『全部』口径，不计入一致率与结论）")
    L.append("\n## 出场原因一致率")
    if m:
        agree = sum(r["reason_match"] for r in m)
        L.append(f"- 一致 {agree}/{len(m)} = {agree/len(m)*100:.1f}%")
        cats = ["止损", "止盈", "时间止损"]
        L.append("- 真实 \\ 模拟（行=真实，列=模拟）：")
        sims = cats + ["未平仓"]
        L.append("  | 真实\\模拟 | " + " | ".join(sims) + " |")
        L.append("  |---|" + "---|" * len(sims))
        for rc in cats:
            L.append(f"  | {rc} | " + " | ".join(str(sum(1 for r in m if r['real_cat'] == rc and r['sim_cat'] == sc)) for sc in sims) + " |")
    for lab, sub in (("可建模", m), ("全部", rows)):
        if not sub:
            continue
        bars = [r["diff_bars"] for r in sub]
        pp = [r["diff_pp"] for r in sub]
        rr = [r["diff_R"] for r in sub]
        L.append(f"\n## 时间与盈亏差异（{lab} {len(sub)} 笔；差 = 模拟 − 真实，价格口径，不含手续费/资金费）")
        L.append(f"- 出场时间：平均 |差| {_mean([abs(x) for x in bars]):.2f} 根1H，平均有符号差 {_mean(bars):+.2f} 根，"
                 f"中位 {statistics.median(bars):+.1f} 根；同一根K线内 {sum(1 for x in bars if x == 0)}/{len(bars)}")
        L.append(f"- 盈亏：平均 |差| {_mean([abs(x) for x in pp]):.3f} 个百分点（{_mean([abs(x) for x in rr]):.3f}R），"
                 f"平均有符号差 {_mean(pp):+.3f}pp；真实平均 {_mean([r['real_pct'] for r in sub]):+.3f}%，模拟平均 {_mean([r['sim_pct'] for r in sub]):+.3f}%")
    L.append(f"\n## 差异最大的 {min(top, len(rows))} 笔（按 |盈亏差|R 排序）")
    for r in sorted(rows, key=lambda x: -abs(x["diff_R"]))[:top]:
        ft = datetime.fromtimestamp(r["t_fill"] / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
        L.append(f"- {r['symbol']} {r['direction']} {r['mode']} 入场 {ft}UTC @ {r['entry']:.6g}（SL {r['sl']:.6g} / TP1 {r['tp1']:.6g}）\n"
                 f"    真实：{r['real_reason']} @ {r['real_exit_price']:.6g}（{r['real_pct']:+.2f}%）｜"
                 f"模拟：{r['sim_reason']} @ {r['sim_exit_price']:.6g}（{r['sim_pct']:+.2f}%）｜"
                 f"时间差 {r['diff_bars']:+.0f} 根，盈亏差 {r['diff_pp']:+.2f}pp（{r['diff_R']:+.2f}R）")
    vt, _ = verdict(rows, min_n)
    L.append("\n## 结论")
    L.append(f"- {vt}")
    L.append(f"- 判定口径（我对『差异<20%』的解读，可改）：出场原因不一致率 <{TRUST_REASON_MISMATCH_MAX*100:.0f}% 且 平均 |盈亏差| <{TRUST_PNL_DIFF_R_MAX}R；仅用可建模笔数")
    L.append("\n## 对账口径与局限")
    L.append(f"- 模拟用 TP1 平仓比例 {tp1_ratio:g}（默认 1.0 = 对齐实盘无 TP2 时 TP1 全平）；真实记录的 exit_price 对分批/手动平仓只代表最后成交")
    L.append("- 模拟入场价取真实入场价、不再加滑点；SL/时间止损等市价出场仍加 0.05% 固定滑点（回测假设），真实滑点由实盘决定")
    L.append("- 模拟从入场所在K线的下一根开始（入场K线内的触及无法判断先后）；入场K线内就平仓的交易会被判成时间/原因不一致")
    L.append("- 开仓记录里的 sl/tp1 是下单时的初始值；实盘后续的 _adjust_sl_tp_once、MFE 回撤、保本保护等改动不在其中")
    L.append("- 非 ema_trend 策略：只套用止损/TP1/移动止损，不模拟时间止损与各策略专用出场，一致率仅供参考")
    L.append("- 1H K线粒度：同一根K线内先后顺序未知，按止损优先；移动止损每根K线收盘评估一次（实盘按tick）")
    L.append("- 本对账只验证『已知入场后的出场逻辑』，不验证信号/入场本身；通过也不代表未来表现")
    return "\n".join(L)


def write_rows(rows: list, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"reconcile_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"
    with open(p, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return p


# ───────────────────────── 主流程 ─────────────────────────
def run(trade_log: Path, data_dir: Path, mode: str, tp1_ratio: float, top: int, min_n: int, mods: dict | None = None) -> dict:
    recs = read_jsonl(trade_log)
    closes, opens_rec = split_records(recs)
    cands, st = merge_close_with_open(closes, opens_rec)
    mode_used, fallback = mode, False
    sel = cands if mode == "all" else [c for c in cands if c["mode"] == mode]
    if mode == "ema_trend" and len(sel) < min_n:
        mode_used, fallback, sel = "all", True, cands
    elif mode != "all":
        mode_used = mode
    st["after_mode"] = len(sel)
    if mods is None:
        mods = B.load_modules()
    rows_out, skips = [], {}
    cache: dict = {}
    saved_hold = B.MAX_HOLD_BARS
    try:
        for cand in sel:
            sym = cand["symbol"]
            if sym not in cache:
                kr = B.load_klines_local(sym, data_dir)
                if kr:
                    k4 = B.aggregate_4h(kr)
                    cache[sym] = {"rows": kr, "opens": [r[0] for r in kr], "k4": k4, "k4_ends": [x[6] + 1 for x in k4],
                                  "funding": B.load_funding_local(sym, data_dir)}
                else:
                    cache[sym] = None
            p = cache[sym]
            if p is None:
                skips["本地无该标的K线（先运行 tools/data_downloader.py）"] = skips.get("本地无该标的K线（先运行 tools/data_downloader.py）", 0) + 1
                continue
            B.MAX_HOLD_BARS = saved_hold if cand["mode"] == "ema_trend" else 10 ** 9   # 非 ema_trend 不套用 16 根时间止损
            s = simulate_one(mods, cand, p["rows"], p["opens"], p["k4"], p["k4_ends"], p["funding"], tp1_ratio)
            if s["status"] != "ok":
                skips[s["why"]] = skips.get(s["why"], 0) + 1
                continue
            rows_out.append(compare(cand, s))
    finally:
        B.MAX_HOLD_BARS = saved_hold
    return {"rows": rows_out, "stats": st, "mode_used": mode_used, "fallback": fallback, "skips": skips}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="用 trade_log 真实入场回放回测出场逻辑并对账")
    ap.add_argument("--mode", default="ema_trend", help="策略 mode；all=全部。默认 ema_trend，可对账笔数<10 时自动改为 all")
    ap.add_argument("--trade-log", default=str(ROOT / "logs" / "trade_log.jsonl"))
    ap.add_argument("--data-dir", default=str(ROOT / "data"))
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--tp1-ratio", type=float, default=1.0, help="TP1 平仓比例，默认 1.0（对齐实盘无 TP2 时的全平）")
    ap.add_argument("--top", type=int, default=5, help="列出差异最大的笔数")
    ap.add_argument("--min-n", type=int, default=MIN_N_DEFAULT)
    a = ap.parse_args(argv)
    tl = Path(a.trade_log)
    if not tl.exists():
        print(f"找不到 trade_log：{tl}")
        return 2
    res = run(tl, Path(a.data_dir), a.mode, a.tp1_ratio, a.top, a.min_n)
    print(build_report(res["rows"], res["stats"], res["mode_used"], res["fallback"], a.tp1_ratio, res["skips"], a.top, a.min_n))
    if res["rows"]:
        out = write_rows(res["rows"], Path(a.out_dir) if a.out_dir else Path(a.data_dir) / "backtest")
        print(f"\n逐笔对账记录：{out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
