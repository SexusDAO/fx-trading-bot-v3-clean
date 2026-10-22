#!/usr/bin/env python3
"""tools/shadow_verify.py — 影子信号结果离线补标签（只读，不改任何交易状态）。

读取 logs/rejected_log.jsonl，按路线的「成交窗口 / 结果窗口 / K 线粒度」拉币安公开 K 线，判定：
  未成交 / 先止盈 / 先止损 / 超时 / 无法判定
没有止损止盈价的记录，用「入场价 ±1 ATR 先碰到哪边」代替，并标记 fallback=True。
输出 logs/rejected_outcomes.jsonl，并按 first_rejected_at 打印各标签比例。

用法：python3 tools/shadow_verify.py [--days 7] [--only-pass-all]
"""
import argparse
import json
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
IN_PATH = LOG_DIR / "rejected_log.jsonl"
OUT_PATH = LOG_DIR / "rejected_outcomes.jsonl"

# 路线窗口（分钟）与 K 线粒度。写死在此，对应 config 中的：
#   STOP_HUNT_RETEST_EXPIRE_MIN(90) / SIGNAL_WATCH_BREAKOUT_EXPIRE_MIN(30) / OI_DIV_ORDER_EXPIRE_MIN(60)
#   SIGNAL_WATCH_PULLBACK_EXPIRE_MIN(240) / TIME_STOP_HOURS(8) / TIME_STOP_REVERSAL_HOURS(12) / EMA_TREND_MAX_HOLD_HOURS(16)
# 改了上述配置时请同步这里。
WINDOWS = {
    "stop_hunt":     {"fill": 90,  "result": 30,      "interval": "1m"},
    "breakout":      {"fill": 30,  "result": 8 * 60,  "interval": "5m"},
    "oi_divergence": {"fill": 60,  "result": 8 * 60,  "interval": "5m"},
    "pullback":      {"fill": 240, "result": 8 * 60,  "interval": "15m"},
    "reversal":      {"fill": 240, "result": 12 * 60, "interval": "15m"},
    "ema_trend":     {"fill": 240, "result": 16 * 60, "interval": "15m"},
    "range_bounce":  {"fill": 240, "result": 8 * 60,  "interval": "15m"},
}
LABELS = ("未成交", "先止盈", "先止损", "超时", "无法判定")


def _norm_reason(reason: str) -> str:
    import re
    return re.sub(r"-?\d+(\.\d+)?(e-?\d+)?%?", "#", reason or "")[:30]


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    """币安公开 K 线，[open_time, open, high, low, close, ...]。"""
    out, cur = [], start_ms
    while cur < end_ms:
        url = (f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}"
               f"&startTime={cur}&endTime={end_ms}&limit=1500")
        rows = json.load(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=20))
        if not rows:
            break
        out.extend(rows)
        nxt = int(rows[-1][0]) + 1
        if nxt <= cur or len(rows) < 1500:
            break
        cur = nxt
    return out


def label_record(rec: dict, klines: list) -> dict:
    """纯函数：根据 K 线给一条影子记录打标签。klines 需覆盖 [ts, ts+fill+result]。"""
    w = WINDOWS.get(rec.get("signal_type"))
    if not w:
        return {"label": "无法判定", "why": "未知路线"}
    entry, d = rec.get("entry_candidate"), rec.get("direction")
    sl, tp, atr = rec.get("sl"), rec.get("tp1"), rec.get("atr")
    if not entry or d not in ("LONG", "SHORT"):
        return {"label": "无法判定", "why": "缺入场价或方向"}
    fallback = not (sl and tp)
    if fallback:
        if not atr:
            return {"label": "无法判定", "why": "缺止损止盈且缺 ATR"}
        sl = entry - atr if d == "LONG" else entry + atr
        tp = entry + atr if d == "LONG" else entry - atr
    ts = rec["ts"]
    fill_end, res_ms = ts + w["fill"] * 60_000, w["result"] * 60_000
    bars = [(int(k[0]), float(k[2]), float(k[3])) for k in klines if int(k[0]) >= ts - 60_000]
    if not bars:
        return {"label": "无法判定", "why": "无 K 线", "fallback": fallback}
    fill_t = None
    for t, hi, lo in bars:
        if t > fill_end:
            break
        if lo <= entry <= hi:
            fill_t = t
            break
    if fill_t is None:
        return {"label": "未成交", "fallback": fallback}
    for t, hi, lo in bars:
        if t < fill_t:
            continue
        if t > fill_t + res_ms:
            break
        hit_tp = hi >= tp if d == "LONG" else lo <= tp
        hit_sl = lo <= sl if d == "LONG" else hi >= sl
        if hit_tp and hit_sl:
            return {"label": "无法判定", "why": "同一根 K 线同时碰到止盈止损", "fallback": fallback}
        if hit_tp:
            return {"label": "先止盈", "fallback": fallback, "minutes": round((t - fill_t) / 60000)}
        if hit_sl:
            return {"label": "先止损", "fallback": fallback, "minutes": round((t - fill_t) / 60000)}
    if bars[-1][0] < fill_t + res_ms:
        return {"label": "无法判定", "why": "结果窗口尚未结束", "fallback": fallback}
    return {"label": "超时", "fallback": fallback}


def main(argv=None, fetch=fetch_klines) -> dict:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=7)
    ap.add_argument("--only-pass-all", action="store_true", help="只统计 would_pass_all=True 的记录")
    ap.add_argument("--include-ambiguous", action="store_true", help="包含 match_ambiguous 的 B 类记录（默认剔除）")
    a = ap.parse_args(argv)
    if not IN_PATH.exists():
        print("没有 rejected_log.jsonl")
        return {}
    now = int(time.time() * 1000)
    cutoff = now - int(a.days * 86400_000)
    stats = defaultdict(Counter)
    outs = []
    for line in IN_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            rec = json.loads(line)
        except Exception:
            continue
        if rec.get("ts", 0) < cutoff or (a.only_pass_all and rec.get("would_pass_all") is not True):
            continue
        if rec.get("match_ambiguous") and not a.include_ambiguous:
            continue
        w = WINDOWS.get(rec.get("signal_type"))
        if not w:
            continue
        end = rec["ts"] + (w["fill"] + w["result"]) * 60_000
        try:
            kl = fetch(rec["symbol"], w["interval"], rec["ts"] - 60_000, min(end, now))
        except Exception as e:
            kl = []
            rec["_err"] = str(e)[:80]
        res = label_record(rec, kl)
        outs.append({**rec, "outcome": res})
        key = rec.get("first_rejected_at", "?")
        if key.endswith("other"):            # 未分类的原因按原文（数字归一化）细分，避免全挤在 other
            key += ":" + _norm_reason(rec.get("reason", ""))
        if res.get("fallback"):
            key += " (兜底)"
        stats[key][res["label"]] += 1
    OUT_PATH.write_text("\n".join(json.dumps(o, ensure_ascii=False) for o in outs) + ("\n" if outs else ""), encoding="utf-8")
    print(f"共 {len(outs)} 条；结果写入 {OUT_PATH}")
    for k, c in sorted(stats.items(), key=lambda x: -sum(x[1].values())):
        n = sum(c.values())
        print(f"{k:<36} n={n:<5} " + "  ".join(f"{lb}{c[lb] / n * 100:4.0f}%" for lb in LABELS))
    return stats


if __name__ == "__main__":
    main()
