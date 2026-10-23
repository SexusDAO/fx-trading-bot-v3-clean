"""reversal 通道3b RR 达标率统计（离线只读工具，不改任何现有代码、不下单）。

用法（在仓库根目录）：
    python3 fx/tools/check_reversal_3b_rr.py
    python3 fx/tools/check_reversal_3b_rr.py --workers 8 --limit 0

流程：
  1. 拉全部 USDT 永续（TRADING）的 24H 涨跌幅；chg24 <= -3% 的才继续拉 K 线（减少请求）
  2. 计算 ATR14(1H) / RSI1H / RSI4H（均用已闭合 K 线，指标函数与系统一致：shared.indicators）
  3. 筛选通道3b：RSI1H < 20 且 chg24 <= -3 且 RSI4H < 40
  4. 读清算区（liq_cache.get_liq_zones_cached，失败记为空），调用 calc_sl_tp
     signal = {"mode":"reversal","direction":"LONG","volatility_tier":"mid"}
  5. 逐条打印 + 汇总（被拒比例、rr_tp1 分布、RR≥2.0 / ≥1.5 占比、sl/tp 降级占比）
单个标的的任何 API 失败都只跳过该标的。
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

FX_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(FX_DIR))

BASE = "https://fapi.binance.com"


def _get(path, params=None, timeout=10):
    q = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    url = f"{BASE}{path}" + (f"?{q}" if q else "")
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def _symbols():
    info = _get("/fapi/v1/exchangeInfo")
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"}


def _analyze(sym, chg24, mark):
    from shared.indicators import atr as _atr, rsi as _rsi, ema as _ema
    try:
        k1h = _get("/fapi/v1/klines", {"symbol": sym, "interval": "1h", "limit": 100})
        k4h = _get("/fapi/v1/klines", {"symbol": sym, "interval": "4h", "limit": 60})
        c1h, c4h = k1h[:-1], k4h[:-1]
        if len(c1h) < 30 or len(c4h) < 20:
            return None
        cl1 = [float(k[4]) for k in c1h]
        cl4 = [float(k[4]) for k in c4h]
        return {"symbol": sym, "chg24": chg24, "mark": mark, "atr": _atr(c1h, 14),
                "rsi_1h": _rsi(cl1, 14), "rsi_4h": _rsi(cl4, 14), "ema20": _ema(cl1, 20), "k1h": c1h}
    except Exception as e:
        return {"symbol": sym, "skip": f"K线失败: {e!r}"}


def _calc(d):
    from shared.calc_sl_tp import calc_sl_tp
    liq = {}
    try:
        from shared.liq_cache import get_liq_zones_cached
        liq = {"long": get_liq_zones_cached(d["symbol"], "long", d["mark"]) or [],
               "short": get_liq_zones_cached(d["symbol"], "short", d["mark"]) or []}
    except Exception:
        liq = {}
    d["liq_ok"] = bool(liq.get("long") or liq.get("short"))
    sig = {"mode": "reversal", "direction": "LONG", "volatility_tier": "mid", "symbol": d["symbol"]}
    try:
        return calc_sl_tp(sig, d["mark"], d["atr"], liq, d["k1h"], d["ema20"])
    except Exception as e:
        return {"error": f"calc_sl_tp 异常: {e!r}"}


def _pct(n, m):
    return f"{n}/{m} ({n / m * 100:.1f}%)" if m else "0/0"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    syms = _symbols()
    tick = {t["symbol"]: t for t in _get("/fapi/v1/ticker/24hr") if t.get("symbol") in syms}
    marks = {m["symbol"]: float(m["markPrice"]) for m in _get("/fapi/v1/premiumIndex") if m.get("symbol") in syms}
    pre = [(s, float(t["priceChangePercent"])) for s, t in tick.items()
           if float(t.get("priceChangePercent", 0)) <= -3 and marks.get(s, 0) > 0]
    print(f"USDT 永续 {len(syms)} 个；chg24<=-3% 的候选 {len(pre)} 个，开始拉 K 线…")

    with ThreadPoolExecutor(args.workers) as ex:
        rows = list(ex.map(lambda x: _analyze(x[0], x[1], marks[x[0]]), pre))
    skipped = [r for r in rows if r and "skip" in r]
    hits = [r for r in rows if r and "skip" not in r
            and r["rsi_1h"] < 20 and r["chg24"] <= -3 and r["rsi_4h"] < 40]
    print(f"K线失败跳过 {len(skipped)} 个；满足通道3b {len(hits)} 个\n")

    ok, err = [], []
    print(f"{'symbol':14s} {'chg24':>7s} {'RSI1H':>6s} {'RSI4H':>6s} {'liq':>4s}  结果")
    for d in sorted(hits, key=lambda x: x["rsi_1h"]):
        p = _calc(d)
        head = f"{d['symbol']:14s} {d['chg24']:7.2f} {d['rsi_1h']:6.1f} {d['rsi_4h']:6.1f} {'Y' if d['liq_ok'] else 'N':>4s}  "
        if not isinstance(p, dict) or p.get("error"):
            err.append(d)
            print(head + f"拒绝: {(p or {}).get('error') if isinstance(p, dict) else p}")
            continue
        ok.append(p)
        print(head + f"rr_tp1={p.get('rr_tp1')} rr_tp2={p.get('rr_tp2')} sl_deg={p.get('sl_degraded')} "
                     f"tp_deg={p.get('tp_degraded')} entry={p.get('entry_price')} mode={p.get('entry_mode')}")

    n = len(hits)
    rr = [float(p["rr_tp1"]) for p in ok if p.get("rr_tp1") is not None]
    print("\n================ 汇总 ================")
    print(f"满足通道3b条件      : {n}")
    print(f"calc_sl_tp 拒绝     : {_pct(len(err), n)}")
    print(f"成功                : {_pct(len(ok), n)}")
    if rr:
        print(f"rr_tp1 最小/平均/中位/最大: {min(rr):.2f} / {statistics.mean(rr):.2f} / "
              f"{statistics.median(rr):.2f} / {max(rr):.2f}")
    else:
        print("rr_tp1 分布        : 无成功样本")
    print(f"rr_tp1 >= 2.0 (mid) : {_pct(sum(r >= 2.0 for r in rr), len(ok))}")
    print(f"rr_tp1 >= 1.5 (high): {_pct(sum(r >= 1.5 for r in rr), len(ok))}")
    print(f"sl_degraded=True    : {_pct(sum(bool(p.get('sl_degraded')) for p in ok), len(ok))}")
    print(f"tp_degraded=True    : {_pct(sum(bool(p.get('tp_degraded')) for p in ok), len(ok))}")


if __name__ == "__main__":
    main()
