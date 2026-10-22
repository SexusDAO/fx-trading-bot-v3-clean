#!/usr/bin/env python3
"""
回测 v5：2026-10-17 全天，v3.29 最新逻辑
两阶段：① 并发预拉 K 线缓存 → ② 读缓存跑逻辑，全天跑完预计 10~15 分钟
"""
from pathlib import Path
import sys, json, datetime, os, time, concurrent.futures, requests
_FX = Path(__file__).resolve().parent
sys.path.insert(0, str(_FX))

START_UTC = datetime.datetime(2026, 10, 17,  0, 0, 0, tzinfo=datetime.timezone.utc)
END_UTC   = datetime.datetime(2026, 10, 18,  0, 0, 0, tzinfo=datetime.timezone.utc)
CACHE_DIR = str(_FX / "logs" / "bt17_cache")
LOG       = str(_FX / "logs" / "backtest_oct17.log")
OUT       = str(_FX / "logs" / "backtest_oct17.json")
VOL_MIN   = 1_000_000
API_TIMEOUT = 8
MAX_WORKERS = 20   # 并发拉取线程数

proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
PROXIES = {"https": proxy, "http": proxy} if proxy else None

# ── weekend 判断 ──────────────────────────────────────────────────────
def _is_weekend_period(now):
    wd, h = now.isoweekday(), now.hour
    return (wd == 5 and h >= 20) or wd in (6, 7) or (wd == 1 and h < 8)

# ── K 线拉取（带缓存）────────────────────────────────────────────────
os.makedirs(CACHE_DIR, exist_ok=True)

def _fetch_klines_raw(sym, interval, limit, end_ms):
    """直接调 API，无缓存"""
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": sym, "interval": interval, "limit": limit, "endTime": end_ms},
            timeout=API_TIMEOUT, proxies=PROXIES)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def _cache_key(sym, interval, end_ms):
    return os.path.join(CACHE_DIR, f"{sym}_{interval}_{end_ms}.json")

def _klines(sym, interval, limit, end_ms):
    """读缓存优先，缓存不存在则拉 API"""
    path = _cache_key(sym, interval, end_ms)
    if os.path.exists(path):
        try:
            return json.load(open(path))
        except Exception:
            pass
    data = _fetch_klines_raw(sym, interval, limit, end_ms)
    if data:
        json.dump(data, open(path, "w"))
    return data

# ── 阶段一：并发预拉所有标的的所有时间点 K 线 ─────────────────────
from shared import binance_api as api

def get_candidates():
    info    = api._get("/fapi/v1/exchangeInfo") or {}
    tickers = api.get_ticker_24h() or []
    vol_map = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tickers}
    amp_map = {}
    for t in tickers:
        sym = t.get("symbol", "")
        lo  = float(t.get("lowPrice",  0))
        hi  = float(t.get("highPrice", 0))
        amp_map[sym] = (hi - lo) / lo * 100 if lo > 0 else 0
    # 用 2.5% 振幅门槛（~210个标的），周末 ATR 放宽由 judges _weekend_mode 控制
    return [s["symbol"] for s in info.get("symbols", [])
            if s.get("status") == "TRADING"
            and s["symbol"].endswith("USDT")
            and s.get("contractType") == "PERPETUAL"
            and vol_map.get(s["symbol"], 0) >= VOL_MIN
            and amp_map.get(s["symbol"], 0) >= 2.5]

def prefetch_all(candidates, timestamps_ms, log):
    """并发预拉所有 (sym, interval, ts_ms) 组合"""
    tasks = []
    for ts_ms in timestamps_ms:
        for sym in candidates:
            for interval, limit in [("1h", 100), ("4h", 100), ("15m", 100)]:
                path = _cache_key(sym, interval, ts_ms)
                if not os.path.exists(path):
                    tasks.append((sym, interval, limit, ts_ms))
    log(f"需要预拉 {len(tasks)} 个 K 线请求（已缓存的跳过）")
    if not tasks:
        log("全部已缓存，跳过预拉")
        return

    done = [0]
    def fetch_one(args):
        sym, interval, limit, end_ms = args
        _klines(sym, interval, limit, end_ms)
        done[0] += 1
        if done[0] % 200 == 0:
            log(f"  预拉进度: {done[0]}/{len(tasks)}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(fetch_one, tasks))
    log(f"预拉完成: {len(tasks)} 个")

# ── 阶段二：回测逻辑 ──────────────────────────────────────────────────
from shared.judges import judge_pullback, judge_reversal, judge_breakout
from shared.calc_sl_tp import calc_sl_tp
from shared.indicators import atr as _atr_fn, ema as _ema_fn
from shared.liq_cache import get_liq_zones_cached as _liq_zones
from scanner import calc_indicators, classify

def simulate_trade(sym, direction, entry, sl, tp1, tp2, signal_ts_ms):
    # 拉未来 K 线（也走缓存）
    end_ms = signal_ts_ms + 192 * 15 * 60 * 1000
    future = _klines(sym, "15m", 192, end_ms) or []
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

def analyze_one(sym, ts_ms, is_weekend):
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
    if price <= 0:
        return None
    chg24 = (price - float(k1h[-24][1])) / float(k1h[-24][1]) * 100
    ind["chg24"]         = chg24
    ind["symbol"]        = sym
    ind["vol24_usd"]     = sum(float(k[7]) for k in k1h[-24:])
    ind["_weekend_mode"] = is_weekend  # 注入周末标志

    if ind["vol24_usd"] < VOL_MIN:
        return None

    # 振幅预筛
    highs = [float(k[2]) for k in k1h[-24:]]
    lows  = [float(k[3]) for k in k1h[-24:]]
    amp_thresh = 1.0 if is_weekend else 2.5
    amp = (max(highs) - min(lows)) / min(lows) * 100 if min(lows) > 0 else 0
    if amp < amp_thresh:
        return None

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

    strength  = d.get("signal_strength", "medium")
    direction = (d.get("direction") or "").upper()
    if not direction:
        rsi1h = ind.get("rsi_1h", 50)
        if route == "reversal":
            direction = "LONG" if rsi1h < 50 else "SHORT"
        elif route == "pullback":
            e20 = ind.get("ema20_1h", 0); e50 = ind.get("ema50_1h", 0)
            direction = "LONG" if (e20 and e50 and e20 < e50) else "SHORT"
        else:
            direction = "LONG" if chg24 > 0 else "SHORT"

    # 周末过滤：breakout 只留 strong
    if is_weekend and route == "breakout" and strength != "strong":
        return None
    if strength == "weak":
        return None

    atr_v = _atr_fn(k1h)
    ema20 = _ema_fn(c1h, 20)
    sig   = {"symbol": sym, "direction": direction, "sig_type": route, "mode": route, **d}
    try:
        lz = {"long":  _liq_zones(sym, "LONG",  price, range_="24h"),
              "short": _liq_zones(sym, "SHORT", price, range_="24h")}
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
    atr_ratio = ind.get("atr_1h", 0) / price * 100

    return {
        "ts":        datetime.datetime.utcfromtimestamp(ts_ms/1000).strftime("%m-%d %H:%M"),
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

# ── main ──────────────────────────────────────────────────────────────
def main():
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    flog = open(LOG, "w", buffering=1)
    def log(msg):
        print(msg, flush=True)
        flog.write(msg + "\n"); flog.flush()

    log("=" * 60)
    log("回测 v5：2026-10-17 全天  v3.29（周末模式全天生效用于验证）")
    log("=" * 60)

    # 10-17 全天统一用周末模式（验证周末逻辑的效果）
    # 时间切片：每小时一个点
    timestamps = []
    ts = START_UTC
    while ts < END_UTC:
        timestamps.append(int(ts.timestamp() * 1000))
        ts += datetime.timedelta(hours=1)

    log(f"时间切片: {len(timestamps)} 个（每小时一个）")

    # 候选池——用 2.5% 振幅门槛（约210个），周末模式 ATR 门槛由 _weekend_mode 控制
    log("拉候选池...")
    candidates = get_candidates()
    log(f"候选池: {len(candidates)} 个标的")

    # 阶段一：并发预拉
    log("\n── 阶段一：并发预拉 K 线缓存 ──")
    t0 = time.time()
    prefetch_all(candidates, timestamps, log)
    log(f"预拉耗时: {time.time()-t0:.0f}s")

    # 额外预拉模拟交易需要的未来 K 线（信号时间 + 48h）
    # 这部分在阶段二按需拉，走缓存后也很快

    # 阶段二：逻辑跑批
    log("\n── 阶段二：回测逻辑 ──")
    results = []
    total_h = len(timestamps)

    for i, ts_ms in enumerate(timestamps):
        dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000)
        is_wk = True  # 全天强制周末模式（验证用）
        if (i % 4 == 0) or i == total_h - 1:
            log(f"  [{i+1:02d}/{total_h}] {dt.strftime('%m-%d %H:%M')} UTC  weekend={is_wk}")

        for sym in candidates:
            try:
                rec = analyze_one(sym, ts_ms, is_wk)
            except Exception:
                rec = None
            if rec:
                results.append(rec)
                tag = "✅" if rec["outcome"] in ("tp1","tp2") else ("❌" if rec["outcome"]=="sl" else "⏱")
                log(f"    {tag} {rec['ts']} {sym:<16} {rec['direction']} {rec['route']:<10} "
                    f"str={rec['strength']:<6} chg={rec['chg24']:+.1f}% → {rec['outcome']} {rec['pnl_pct']:+.1f}%")

        # 中间结果实时写入
        with open(OUT, "w") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

    # ── 汇总 ──────────────────────────────────────────────────────────
    log(f"\n{'='*60}")
    log(f"回测完成: 共触发信号 {len(results)} 个")

    if results:
        wins   = [r for r in results if r["outcome"] in ("tp1","tp2")]
        losses = [r for r in results if r["outcome"] == "sl"]
        tos    = [r for r in results if r["outcome"] == "timeout"]
        wr = len(wins)/(len(wins)+len(losses))*100 if (wins or losses) else 0
        log(f"  止盈={len(wins)}  止损={len(losses)}  超时={len(tos)}  胜率={wr:.1f}%")
        if wins:   log(f"  平均盈利: {sum(r['pnl_pct'] for r in wins)/len(wins):+.2f}%")
        if losses: log(f"  平均亏损: {sum(r['pnl_pct'] for r in losses)/len(losses):+.2f}%")

        by_route = {}
        for r in results:
            by_route.setdefault(r["route"], []).append(r)
        for route, rs in sorted(by_route.items()):
            w = sum(1 for r in rs if r["outcome"] in ("tp1","tp2"))
            l = sum(1 for r in rs if r["outcome"] == "sl")
            log(f"  {route:<12} {len(rs)}个  胜{w}负{l}" +
                (f"  胜率{w/(w+l)*100:.0f}%" if w+l else "  全超时"))

        # 按小时分布
        log("\n  按小时信号分布:")
        by_hour = {}
        for r in results:
            h = r["ts"][6:8]
            by_hour.setdefault(h, []).append(r)
        for h in sorted(by_hour.keys()):
            rs = by_hour[h]
            w = sum(1 for r in rs if r["outcome"] in ("tp1","tp2"))
            l = sum(1 for r in rs if r["outcome"] == "sl")
            log(f"    {h}:00  {len(rs)}个信号  胜{w}负{l}")
    else:
        log("  无信号触发")

    log(f"\n输出: {OUT}")
    flog.close()

if __name__ == "__main__":
    main()
