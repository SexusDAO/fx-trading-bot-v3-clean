#!/usr/bin/env python3
"""
回测脚本 v3：上周末（2026-10-11 00:00 ~ 2026-10-13 00:00 UTC）
ATR/振幅门槛 1.0%，RSI极端通道 chg24 -3%，calc_sl_tp 接口修正
"""
from pathlib import Path
import sys, json, datetime, time, re, types, os, signal as _signal
_FX = Path(__file__).resolve().parent
sys.path.insert(0, str(_FX))

# ── 参数 ──────────────────────────────────────────────────────────────
START_UTC    = datetime.datetime(2026, 10, 11, 0,  0, 0, tzinfo=datetime.timezone.utc)
END_UTC      = datetime.datetime(2026, 10, 13, 0,  0, 0, tzinfo=datetime.timezone.utc)
AMP_MIN      = 1.0    # 日内振幅预筛
ATR_OVERRIDE = 1.0    # judge 层 ATR 门槛
VOL_MIN      = 1_000_000
API_TIMEOUT  = 6      # 每次 API 请求超时秒数
LOG          = str(_FX / "logs" / "backtest_weekend.log")
OUT          = str(_FX / "logs" / "backtest_weekend.json")

# ── 打 ATR patch（在 import judges 之前）─────────────────────────────
import shared.judges as _jm

def _patch_judges_atr(thr):
    src = open(_FX / 'shared' / 'judges.py', encoding='utf-8').read()
    src_p = re.sub(r'(if atr_r <) (2\.0|2\.5)(:)', lambda m: f'{m.group(1)} {thr}{m.group(3)}', src)
    ns = {**_jm.__dict__}
    exec(compile(src_p, 'judges_patched', 'exec'), ns)
    for k, v in ns.items():
        if isinstance(v, types.FunctionType) and not k.startswith('__'):
            setattr(_jm, k, v)

_patch_judges_atr(ATR_OVERRIDE)

from shared import binance_api as api
from shared.judges import judge_pullback, judge_reversal, judge_breakout
from shared.calc_sl_tp import calc_sl_tp
from shared.indicators import atr as _atr_fn, ema as _ema_fn
from shared.liq_cache import get_liq_zones_cached as _liq_zones
from scanner import calc_indicators, classify

# ── 带超时的 K 线拉取 ─────────────────────────────────────────────────
def _klines(sym, interval, limit, end_ms):
    """带 timeout 的历史 K 线，失败返回 None"""
    import requests, urllib.parse
    try:
        params = {"symbol": sym, "interval": interval, "limit": limit, "endTime": end_ms}
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or None
        proxies = {"https": proxy, "http": proxy} if proxy else None
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/klines",
            params=params, timeout=API_TIMEOUT, proxies=proxies
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

# ── 候选池 ───────────────────────────────────────────────────────────
def get_candidates():
    info    = api._get("/fapi/v1/exchangeInfo") or {}
    tickers = api.get_ticker_24h() or []
    vol_map = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tickers}
    return [s["symbol"] for s in info.get("symbols", [])
            if s.get("status") == "TRADING"
            and s["symbol"].endswith("USDT")
            and s.get("contractType") == "PERPETUAL"
            and vol_map.get(s["symbol"], 0) >= 3_000_000]

# ── 模拟 SL/TP 触发 ──────────────────────────────────────────────────
def simulate_trade(sym, direction, entry, sl, tp1, tp2, signal_ts_ms):
    future = _klines(sym, "15m", 192, signal_ts_ms + 192 * 15 * 60 * 1000) or []
    # 去掉 endTime 之前的 K 线（应从 signal_ts_ms 开始）
    future = [k for k in future if int(k[0]) >= signal_ts_ms]
    for i, k in enumerate(future):
        lo, hi = float(k[3]), float(k[2])
        if direction == "LONG":
            if lo <= sl:              return {"outcome": "sl",      "pnl_pct": (sl -entry)/entry*100, "bars": i+1}
            if tp2 and hi >= tp2:    return {"outcome": "tp2",     "pnl_pct": (tp2-entry)/entry*100, "bars": i+1}
            if tp1 and hi >= tp1:    return {"outcome": "tp1",     "pnl_pct": (tp1-entry)/entry*100, "bars": i+1}
        else:
            if hi >= sl:              return {"outcome": "sl",      "pnl_pct": (entry-sl )/entry*100, "bars": i+1}
            if tp2 and lo <= tp2:    return {"outcome": "tp2",     "pnl_pct": (entry-tp2)/entry*100, "bars": i+1}
            if tp1 and lo <= tp1:    return {"outcome": "tp1",     "pnl_pct": (entry-tp1)/entry*100, "bars": i+1}
    return {"outcome": "timeout", "pnl_pct": 0, "bars": len(future)}

# ── 单标的单时间点 ────────────────────────────────────────────────────
def analyze_one(sym, ts_ms):
    k1h = _klines(sym, "1h",  100, ts_ms)
    k4h = _klines(sym, "4h",  100, ts_ms)
    k15 = _klines(sym, "15m", 100, ts_ms)
    if not k1h or len(k1h) < 30 or not k4h or not k15:
        return None
    k1h, k4h, k15 = k1h[:-1], k4h[:-1], k15[:-1]
    if len(k1h) < 28 or len(k4h) < 10 or len(k15) < 20:
        return None

    ind  = calc_indicators(k15, k1h, k4h)
    c1h  = [float(k[4]) for k in k1h]
    price = c1h[-1]
    chg24 = (price - float(k1h[-24][1])) / float(k1h[-24][1]) * 100
    ind["chg24"]    = chg24
    ind["symbol"]   = sym
    ind["vol24_usd"] = sum(float(k[7]) for k in k1h[-24:])

    if ind["vol24_usd"] < VOL_MIN:
        return None

    # 日内振幅预筛
    highs = [float(k[2]) for k in k1h[-24:]]
    lows  = [float(k[3]) for k in k1h[-24:]]
    amp   = (max(highs) - min(lows)) / min(lows) * 100 if min(lows) > 0 else 0
    if amp < AMP_MIN:
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

    # 推断 direction（pullback/reversal/breakout 不写入 out["direction"]）
    rsi1h    = ind.get("rsi_1h", 50)
    ema20_1h = ind.get("ema20_1h", 0)
    ema50_1h = ind.get("ema50_1h", 0)
    if route == "reversal":
        direction = "LONG" if rsi1h < 50 else "SHORT"
    elif route == "pullback":
        # ema_direction: bullish=SHORT candidate, bearish=LONG candidate
        ema_dir = d.get("ema_direction", "")
        if ema_dir == "bearish" or (ema20_1h and ema50_1h and ema20_1h < ema50_1h):
            direction = "LONG"
        else:
            direction = "SHORT"
    else:  # breakout
        direction = "LONG" if chg24 > 0 else "SHORT"

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
    if "error" in params:
        return None

    entry = params["entry_price"]
    sl    = params["sl_price"]
    tp1   = params.get("tp1_price")
    tp2   = params.get("tp2_price")
    if not sl:
        return None

    # 周末 score 简化：signal_strength 判断
    strength = d.get("signal_strength", "medium")
    if strength == "weak":
        return None  # 弱信号直接跳过

    # 模拟触发
    sim = simulate_trade(sym, direction, entry, sl, tp1, tp2, ts_ms)

    return {
        "ts":        datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%m-%d %H:%M"),
        "symbol":    sym,
        "route":     route,
        "direction": direction,
        "strength":  strength,
        "chg24":     round(chg24, 1),
        "atr_ratio": round(ind.get("atr_ratio", 0), 2),
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

    log(f"回测参数: ATR={ATR_OVERRIDE}%  AMP={AMP_MIN}%  RSI极端通道=-3%")
    log("拉候选池...")
    candidates = get_candidates()
    log(f"候选池: {len(candidates)} 个标的")

    ts         = START_UTC
    total_h    = int((END_UTC - START_UTC).total_seconds() // 3600)
    results    = []
    hour_n     = 0

    while ts < END_UTC:
        ts_ms   = int(ts.timestamp() * 1000)
        hour_n += 1
        if hour_n % 4 == 1:
            log(f"  [{hour_n}/{total_h}] {ts.strftime('%m-%d %H:%M')} UTC ...")

        for sym in candidates:
            rec = None
            try:
                rec = analyze_one(sym, ts_ms)
            except Exception:
                pass
            if rec:
                results.append(rec)
                # 及时写入，防止进程被 kill 丢失数据
                with open(OUT, "w") as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                tag = "✅" if rec["outcome"] in ("tp1", "tp2") else ("❌" if rec["outcome"] == "sl" else "⏱")
                log(f"    {tag} {rec['ts']} {sym:<16} {rec['direction']} {rec['route']:<10} "
                    f"entry={rec['entry']:.5g} sl={rec['sl']:.5g} → {rec['outcome']} {rec['pnl_pct']:+.1f}%")
            time.sleep(0.03)

        ts += datetime.timedelta(hours=1)

    # 汇总
    with open(OUT, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    log(f"\n{'='*60}")
    log(f"回测完成: 共 {len(results)} 个信号")
    if results:
        wins   = [r for r in results if r["outcome"] in ("tp1", "tp2")]
        losses = [r for r in results if r["outcome"] == "sl"]
        tos    = [r for r in results if r["outcome"] == "timeout"]
        wr     = len(wins) / (len(wins) + len(losses)) * 100 if (wins or losses) else 0
        log(f"  止盈={len(wins)}  止损={len(losses)}  超时={len(tos)}  胜率={wr:.1f}%")
        if wins:   log(f"  平均盈利: {sum(r['pnl_pct'] for r in wins)  / len(wins):+.2f}%")
        if losses: log(f"  平均亏损: {sum(r['pnl_pct'] for r in losses)/ len(losses):+.2f}%")
        by_route = {}
        for r in results:
            by_route.setdefault(r["route"], []).append(r)
        for route, rs in by_route.items():
            w = sum(1 for r in rs if r["outcome"] in ("tp1", "tp2"))
            l = sum(1 for r in rs if r["outcome"] == "sl")
            log(f"  {route:<12} {len(rs)}个  胜{w}负{l}  胜率{w/(w+l)*100:.0f}%" if w+l
                else f"  {route:<12} {len(rs)}个  全超时")
    flog.close()

if __name__ == "__main__":
    main()
