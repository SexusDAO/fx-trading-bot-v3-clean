#!/usr/bin/env python3
"""
回测脚本 v4：2026-10-17 全天（00:00 ~ 24:00 UTC）
使用最新 v3.29 逻辑：_is_weekend_period() 动态切换 ATR/振幅门槛
  - 00:00~19:59 UTC：工作日模式（ATR 2.0/2.5%，振幅 2.5%）
  - 20:00~24:00 UTC：周末模式（ATR 1.0/1.5%，振幅 1.0%）
"""
from pathlib import Path
import sys, json, datetime, time, os
_FX = Path(__file__).resolve().parent
sys.path.insert(0, str(_FX))

# ── 参数 ──────────────────────────────────────────────────────────────
START_UTC = datetime.datetime(2026, 10, 17,  0, 0, 0, tzinfo=datetime.timezone.utc)
END_UTC   = datetime.datetime(2026, 10, 18,  0, 0, 0, tzinfo=datetime.timezone.utc)
VOL_MIN   = 1_000_000
API_TIMEOUT = 6
LOG = str(_FX / "logs" / "backtest_oct17.log")
OUT = str(_FX / "logs" / "backtest_oct17.json")

# ── weekend 判断（与 scanner.py v3.29 完全一致）─────────────────────
def _is_weekend_period(now: datetime.datetime) -> bool:
    wd = now.isoweekday()
    h  = now.hour
    if wd == 5 and h >= 20: return True
    if wd in (6, 7):        return True
    if wd == 1 and h < 8:   return True
    return False

# ── 不 patch ATR——让 judges.py 里的 _weekend_mode 标志自己控制 ───────
from shared import binance_api as api
from shared.judges import judge_pullback, judge_reversal, judge_breakout
from shared.calc_sl_tp import calc_sl_tp
from shared.indicators import atr as _atr_fn, ema as _ema_fn
from shared.liq_cache import get_liq_zones_cached as _liq_zones
from scanner import calc_indicators, classify, _is_weekend_period as _scanner_weekend

# ── 带超时的历史 K 线 ─────────────────────────────────────────────────
def _klines(sym, interval, limit, end_ms):
    import requests
    try:
        params = {"symbol": sym, "interval": interval, "limit": limit, "endTime": end_ms}
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        proxies = {"https": proxy, "http": proxy} if proxy else None
        r = requests.get("https://fapi.binance.com/fapi/v1/klines",
                         params=params, timeout=API_TIMEOUT, proxies=proxies)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

# ── 候选池（当前实时，代表昨天市场范围）─────────────────────────────
def get_candidates(amp_thresh: float):
    """返回满足振幅门槛的标的列表"""
    info    = api._get("/fapi/v1/exchangeInfo") or {}
    tickers = api.get_ticker_24h() or []
    vol_map  = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tickers}
    amp_map  = {}
    for t in tickers:
        sym = t.get("symbol", "")
        lo  = float(t.get("lowPrice",  0))
        hi  = float(t.get("highPrice", 0))
        if lo > 0:
            amp_map[sym] = (hi - lo) / lo * 100
    return [s["symbol"] for s in info.get("symbols", [])
            if s.get("status") == "TRADING"
            and s["symbol"].endswith("USDT")
            and s.get("contractType") == "PERPETUAL"
            and vol_map.get(s["symbol"], 0) >= 3_000_000
            and amp_map.get(s["symbol"], 0) >= amp_thresh]

# ── 模拟 SL/TP 触发 ──────────────────────────────────────────────────
def simulate_trade(sym, direction, entry, sl, tp1, tp2, signal_ts_ms):
    future = _klines(sym, "15m", 192, signal_ts_ms + 192 * 15 * 60 * 1000) or []
    future = [k for k in future if int(k[0]) >= signal_ts_ms]
    for i, k in enumerate(future):
        lo, hi = float(k[3]), float(k[2])
        if direction == "LONG":
            if lo <= sl:           return {"outcome": "sl",  "pnl_pct": (sl -entry)/entry*100, "bars": i+1}
            if tp2 and hi >= tp2:  return {"outcome": "tp2", "pnl_pct": (tp2-entry)/entry*100, "bars": i+1}
            if tp1 and hi >= tp1:  return {"outcome": "tp1", "pnl_pct": (tp1-entry)/entry*100, "bars": i+1}
        else:
            if hi >= sl:           return {"outcome": "sl",  "pnl_pct": (entry-sl )/entry*100, "bars": i+1}
            if tp2 and lo <= tp2:  return {"outcome": "tp2", "pnl_pct": (entry-tp2)/entry*100, "bars": i+1}
            if tp1 and lo <= tp1:  return {"outcome": "tp1", "pnl_pct": (entry-tp1)/entry*100, "bars": i+1}
    return {"outcome": "timeout", "pnl_pct": 0, "bars": len(future)}

# ── 单标的单时间点 ────────────────────────────────────────────────────
def analyze_one(sym, ts_ms, is_weekend: bool):
    k1h = _klines(sym, "1h",  100, ts_ms)
    k4h = _klines(sym, "4h",  100, ts_ms)
    k15 = _klines(sym, "15m", 100, ts_ms)
    if not k1h or len(k1h) < 30 or not k4h or not k15:
        return None
    k1h, k4h, k15 = k1h[:-1], k4h[:-1], k15[:-1]
    if len(k1h) < 28 or len(k4h) < 10 or len(k15) < 20:
        return None

    ind = calc_indicators(k15, k1h, k4h)
    c1h = [float(k[4]) for k in k1h]
    price = c1h[-1]
    chg24 = (price - float(k1h[-24][1])) / float(k1h[-24][1]) * 100
    ind["chg24"]      = chg24
    ind["symbol"]     = sym
    ind["vol24_usd"]  = sum(float(k[7]) for k in k1h[-24:])
    ind["_weekend_mode"] = is_weekend   # ← 关键：注入周末标志，judges.py 动态切 ATR

    if ind["vol24_usd"] < VOL_MIN:
        return None

    # 日内振幅预筛（与 refresh_pool 逻辑一致）
    highs = [float(k[2]) for k in k1h[-24:]]
    lows  = [float(k[3]) for k in k1h[-24:]]
    amp   = (max(highs) - min(lows)) / min(lows) * 100 if min(lows) > 0 else 0
    amp_thresh = 1.0 if is_weekend else 2.5
    if amp < amp_thresh:
        return None

    # 分类 + judge
    route = classify(sym, chg24, 0.0, ind)
    if route == "reversal":
        ok, d = judge_reversal(k1h, k4h, k15, ind, chg24, 0.0)
    elif route == "pullback":
        ok, d = judge_pullback(k1h, k4h, k15, ind, chg24, 0.0)
    elif route == "breakout":
        ok, d = judge_breakout(k1h, k4h, k15, ind, chg24, 0.0)
    else:
        return None
    if not ok:
        return None

    # 推断 direction
    rsi1h    = ind.get("rsi_1h", 50)
    ema20_1h = ind.get("ema20_1h", 0)
    ema50_1h = ind.get("ema50_1h", 0)
    direction = d.get("direction", "").upper()
    if not direction:
        if route == "reversal":
            direction = "LONG" if rsi1h < 50 else "SHORT"
        elif route == "pullback":
            direction = "LONG" if (ema20_1h and ema50_1h and ema20_1h < ema50_1h) else "SHORT"
        else:
            direction = "LONG" if chg24 > 0 else "SHORT"

    strength = d.get("signal_strength", "medium")

    # 周末评分过滤（简化：strong≥60/medium≥70，这里用 strength 代理）
    if is_weekend and strength == "weak":
        return None
    # breakout 周末只保留 strong
    if is_weekend and route == "breakout" and strength != "strong":
        return None

    # calc_sl_tp
    atr_v = _atr_fn(k1h)
    ema20 = _ema_fn(c1h, 20)
    sig   = {"symbol": sym, "direction": direction, "sig_type": route, "mode": route, **d}
    try:
        lz = {
            "long":  _liq_zones(sym, "LONG",  price, range_="24h"),
            "short": _liq_zones(sym, "SHORT", price, range_="24h"),
        }
        params = calc_sl_tp(signal=sig, mark_price=price, atr=atr_v,
                            liq_zones=lz, k1h=k1h, ema20=ema20)
    except Exception:
        return None
    if not params or "error" in params:
        return None

    entry = params.get("entry_price")
    sl    = params.get("sl_price")
    tp1   = params.get("tp1_price")
    tp2   = params.get("tp2_price")
    if not entry or not sl:
        return None

    sim = simulate_trade(sym, direction, entry, sl, tp1, tp2, ts_ms)
    atr_ratio = ind.get("atr_1h", 0) / price * 100 if price > 0 else 0

    return {
        "ts":        datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%m-%d %H:%M"),
        "mode":      "weekend" if is_weekend else "weekday",
        "symbol":    sym,
        "route":     route,
        "direction": direction,
        "strength":  strength,
        "chg24":     round(chg24, 1),
        "atr_ratio": round(atr_ratio, 2),
        "entry":     round(entry, 6),
        "sl":        round(sl, 6),
        "tp1":       round(tp1, 6) if tp1 else None,
        "tp2":       round(tp2, 6) if tp2 else None,
        "outcome":   sim["outcome"],
        "pnl_pct":   round(sim["pnl_pct"], 2),
        "bars_15m":  sim["bars"],
    }

# ── 主循环 ────────────────────────────────────────────────────────────
def main():
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    flog = open(LOG, "w", buffering=1)
    def log(msg):
        print(msg, flush=True)
        flog.write(msg + "\n"); flog.flush()

    log("=" * 60)
    log("回测 v4：2026-10-17 全天  v3.29 最新逻辑（动态周末模式）")
    log("  工作日（00:00~19:59）：振幅≥2.5% ATR工作日门槛")
    log("  周末段（20:00~23:59）：振幅≥1.0% ATR周末门槛")
    log("=" * 60)

    # 用工作日候选池（主力），周末段复用同一池子
    log("拉候选池（振幅≥2.5% 工作日基准）...")
    candidates = get_candidates(2.5)
    log(f"工作日候选池: {len(candidates)} 个")

    # 额外拉周末扩充池（振幅 1.0~2.5%）
    candidates_wk_extra = [s for s in get_candidates(1.0) if s not in set(candidates)]
    log(f"周末扩充候选（振幅1.0~2.5%额外）: {len(candidates_wk_extra)} 个")

    ts      = START_UTC
    total_h = int((END_UTC - START_UTC).total_seconds() // 3600)
    results = []
    hour_n  = 0

    while ts < END_UTC:
        ts_ms      = int(ts.timestamp() * 1000)
        is_wk      = _is_weekend_period(ts)
        mode_label = "🌙周末" if is_wk else "☀工作日"
        hour_n    += 1

        # 周末时段用扩充候选池，工作日用标准池
        pool = (candidates + candidates_wk_extra) if is_wk else candidates

        if hour_n % 4 == 1 or is_wk:
            log(f"  [{hour_n:02d}/{total_h}] {ts.strftime('%m-%d %H:%M')} UTC {mode_label}  pool={len(pool)}")

        for sym in pool:
            rec = None
            try:
                rec = analyze_one(sym, ts_ms, is_wk)
            except Exception:
                pass
            if rec:
                results.append(rec)
                with open(OUT, "w") as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                tag = "✅" if rec["outcome"] in ("tp1","tp2") else ("❌" if rec["outcome"]=="sl" else "⏱")
                log(f"    {tag} {rec['ts']} {rec['mode']:<8} {sym:<16} {rec['direction']} {rec['route']:<10} "
                    f"str={rec['strength']:<6} chg={rec['chg24']:+.1f}% → {rec['outcome']} {rec['pnl_pct']:+.1f}%")
            time.sleep(0.02)

        ts += datetime.timedelta(hours=1)

    # ── 汇总 ──────────────────────────────────────────────────────────
    with open(OUT, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    log(f"\n{'='*60}")
    log(f"回测完成: 共触发信号 {len(results)} 个")

    for mode in ("weekday", "weekend"):
        rs = [r for r in results if r["mode"] == mode]
        if not rs:
            log(f"  [{mode}] 无信号")
            continue
        wins   = [r for r in rs if r["outcome"] in ("tp1","tp2")]
        losses = [r for r in rs if r["outcome"] == "sl"]
        tos    = [r for r in rs if r["outcome"] == "timeout"]
        wr = len(wins)/(len(wins)+len(losses))*100 if (wins or losses) else 0
        log(f"\n  [{mode}] 信号{len(rs)}  止盈{len(wins)}  止损{len(losses)}  超时{len(tos)}  胜率{wr:.1f}%")
        if wins:   log(f"    平均盈利: {sum(r['pnl_pct'] for r in wins)/len(wins):+.2f}%")
        if losses: log(f"    平均亏损: {sum(r['pnl_pct'] for r in losses)/len(losses):+.2f}%")
        by_route = {}
        for r in rs:
            by_route.setdefault(r["route"], []).append(r)
        for route, rr in sorted(by_route.items()):
            w = sum(1 for x in rr if x["outcome"] in ("tp1","tp2"))
            l = sum(1 for x in rr if x["outcome"] == "sl")
            log(f"    {route:<12} {len(rr)}个  胜{w}负{l}" + (f"  胜率{w/(w+l)*100:.0f}%" if w+l else "  全超时"))

    log(f"\n输出文件: {OUT}")
    flog.close()

if __name__ == "__main__":
    main()
