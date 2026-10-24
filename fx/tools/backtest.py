#!/usr/bin/env python3
"""
方向准确率回测框架 v1（纯离线计算，不依赖实盘账户，不改任何现有业务代码）

核心问题：信号触发后，价格是否朝信号方向运动？
  · 每根 1H K 线收盘时复用 calc_indicators / judge_* 生成信号
  · 入场方式 --entry-mode：direct = 该根收盘价市价入场（默认）；
    limit = 限价回踩：挂单价 = 收盘价×(1∓offset)（LONG 下方、SHORT 上方，--limit-offset 默认 0.005），
    之后 N 根 1H 内 LONG 的 low<=挂单价 / SHORT 的 high>=挂单价 才成交，成交价=挂单价，
    从成交那根的下一根开始观察 N 小时；N 根内没触及算「未成交」，不计入准确率统计，只计入成交率
  · 清算图 / 盘口在回测里全部为空（liq_zones 不模拟），不调用 calc_sl_tp，不模拟挂单与止盈止损
  · 方向正确：入场后 N 根 1H 内，最高/最低价朝信号方向触及 ≥ 1×ATR（入场时的 1H ATR）
  · 时间止损：入场后第 N 根 1H 收盘价平仓，收益（按方向）> 0 记盈利
  · 基线：同一区间所有小时、同方向、同 N 的无条件概率，用来判断「准确率」是不是只是波动本身带来的

用法：
    python3 fx/tools/backtest.py --symbols BTCUSDT ETHUSDT --days 180 --strategy reversal pullback \
        --time-stop 4 --output results/

参数敏感性（RSI / chg24 / vol_ratio）说明：这些门槛写死在 judges.py 里且不允许改，所以网格是对
「已通过现行 judge 的 reversal 信号」做二次过滤，只能评估比现行更严的门槛；宽于现行门槛的取值等同不过滤。
"""
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as _dt
import json
import math
import os
import sys
import time
from pathlib import Path

_FX = Path(__file__).resolve().parent.parent
if str(_FX) not in sys.path:
    sys.path.insert(0, str(_FX))

H1 = 3_600_000
M15 = 900_000
H4 = 14_400_000
ALL_STRATEGIES = ["reversal", "pullback", "breakout", "range_bounce", "stop_hunt", "oi_divergence", "ema_trend"]
DEFAULT_N = {"reversal": 4, "pullback": 2, "ema_trend": 6}     # 其余策略未指定，取 OTHER_DEFAULT_N
OTHER_DEFAULT_N = 4
N_GRID = (2, 4, 6, 8, 12)
RSI_GRID = (15, 20, 25, 30)
CHG_GRID = (2, 3, 5, 8)
VOL_GRID = (0.3, 0.4, 0.5, 0.6)
MCAP_BINS = [("小市值(<1亿)", 0, 1e8), ("中市值(1-10亿)", 1e8, 1e9), ("大市值(>10亿)", 1e9, float("inf"))]
_CACHE_DIR = _FX / "logs" / "backtest_cache"
_FAPI = "https://fapi.binance.com"


# ═════════════════════════ 数据层 ═════════════════════════
def _http_get(path: str, params: dict, retries: int = 4):
    import requests
    last = None
    for k in range(retries):
        try:
            r = requests.get(_FAPI + path, params=params, timeout=15)
            if r.status_code in (418, 429):
                time.sleep(2 + 3 * k)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:   # noqa: BLE001
            last = e
            time.sleep(1 + k)
    raise RuntimeError(f"请求失败 {path}: {last}")


def _fetch_klines_range(sym: str, interval: str, start_ms: int, end_ms: int) -> list:
    out, cur = [], start_ms
    while cur < end_ms:
        batch = _http_get("/fapi/v1/klines", {"symbol": sym, "interval": interval, "startTime": cur,
                                              "endTime": end_ms, "limit": 1500})
        if not batch:
            break
        out.extend(batch)
        nxt = int(batch[-1][0]) + 1
        if nxt <= cur:
            break
        cur = nxt
        if len(batch) < 1500:
            break
    return out


def load_klines(sym: str, interval: str, start_ms: int, end_ms: int, use_cache: bool = True) -> list:
    """返回 [start_ms, end_ms) 内的已收盘 K 线；本地缓存按 open_time 合并，只补缺失部分。"""
    step = {"1h": H1, "15m": M15}[interval]
    path = _CACHE_DIR / f"{sym}_{interval}.json"
    cached: list = []
    if use_cache and path.exists():
        try:
            cached = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            cached = []
    have = {int(k[0]): k for k in cached}
    now_ms = int(time.time() * 1000)
    last_closed_open = (now_ms // step) * step - step
    want_end = min(end_ms - step, last_closed_open)
    changed = False
    if have:
        lo, hi = min(have), max(have)
        if lo > start_ms + step:
            for k in _fetch_klines_range(sym, interval, start_ms, lo):
                have[int(k[0])] = k
            changed = True
        if hi < want_end:
            for k in _fetch_klines_range(sym, interval, hi + step, want_end + step):
                have[int(k[0])] = k
            changed = True
    else:
        for k in _fetch_klines_range(sym, interval, start_ms, want_end + step):
            have[int(k[0])] = k
        changed = True
    rows = [have[t] for t in sorted(have) if int(have[t][6]) < now_ms]
    if use_cache and changed:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows), encoding="utf-8")
        os.replace(tmp, path)
    return [k for k in rows if start_ms <= int(k[0]) < end_ms]


def top_volume_symbols(n: int = 10) -> list:
    data = _http_get("/fapi/v1/ticker/24hr", {})
    usdt = [t for t in data if t.get("symbol", "").endswith("USDT") and "_" not in t["symbol"]]
    usdt.sort(key=lambda t: float(t.get("quoteVolume", 0)), reverse=True)
    return [t["symbol"] for t in usdt[:n]]


def aggregate_4h(k1h: list) -> list:
    """由 1h 聚合 4h（按 UTC 4h 对齐，只保留 4 根齐全的）。"""
    by_open = {int(k[0]): k for k in k1h}
    out = []
    for s in sorted({(t // H4) * H4 for t in by_open}):
        parts = [by_open.get(s + i * H1) for i in range(4)]
        if any(p is None for p in parts):
            continue
        out.append([s, parts[0][1], str(max(float(p[2]) for p in parts)), str(min(float(p[3]) for p in parts)),
                    parts[3][4], str(sum(float(p[5]) for p in parts)), s + H4 - 1,
                    str(sum(float(p[7]) for p in parts)), 0, "0", "0", "0"])
    return out


def _forming(open_ms: int, price: float, step: int, vol: float = 0.0) -> list:
    """合成「未收盘 K 线」：价格只含开盘价（= 上一根收盘价），不含任何未来数据。
    成交量用上一根已收盘 K 线的量做代理（vol）：calc_indicators 的 vol_ratio 取最后一根（含未收盘）的量，
    写 0 会让 vol_ratio 恒为 0，被 reversal 量能门槛全部拒掉。"""
    p = repr(float(price))
    v = repr(float(vol))
    return [open_ms, p, p, p, p, v, open_ms + step - 1, v, 0, "0", "0", "0"]


# ═════════════════════════ 市值（当前市值，非历史）═════════════════════════
def base_asset(sym: str) -> str:
    b = sym[:-4] if sym.endswith("USDT") else sym
    for p in ("1000000", "10000", "1000"):
        if b.startswith(p) and len(b) > len(p):
            return b[len(p):]
    return b


def load_market_caps(use_cache: bool = True, max_age_sec: float = 7 * 86400) -> dict:
    """CoinGecko 当前市值前 1000（同 symbol 取最大者）；缓存 7 天；失败时回落到过期缓存，再失败返回空。"""
    path = _CACHE_DIR / "market_caps.json"
    stale: dict = {}
    if use_cache and path.exists():
        try:
            d = json.loads(path.read_text(encoding="utf-8"))
            stale = d.get("caps") or {}
            if time.time() - float(d.get("ts", 0)) < max_age_sec and stale:
                return stale
        except Exception:
            pass
    caps: dict = {}
    try:
        import requests
        for page in range(1, 5):
            r = requests.get("https://api.coingecko.com/api/v3/coins/markets",
                             params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250,
                                     "page": page}, timeout=20)
            r.raise_for_status()
            for c in r.json():
                sym = str(c.get("symbol", "")).upper()
                mc = float(c.get("market_cap") or 0)
                if sym and mc > caps.get(sym, 0):
                    caps[sym] = mc
            time.sleep(2)
    except Exception as e:   # noqa: BLE001
        print(f"[backtest] 市值数据获取失败（{str(e)[:80]}），{'使用过期缓存' if stale else '市值分组将标为 unknown'}",
              file=sys.stderr)
        return stale
    if caps:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"ts": time.time(), "caps": caps}), encoding="utf-8")
    return caps


def mcap_of(sym: str, caps: dict):
    return caps.get(base_asset(sym)) if caps else None


def mcap_group(mc) -> str:
    if not mc:
        return "未知"
    for name, lo, hi in MCAP_BINS:
        if lo <= mc < hi:
            return name
    return "未知"


# ═════════════════════════ 离线环境（运行时替换网络出口，不改现有代码）═════════════════════════
def install_offline_stubs():
    from shared import liq_cache as _lc
    from shared import binance_api as _api
    _lc.get_liq_zones_cached = lambda *a, **k: []          # liq_zones 全部为空
    _api.get_order_book = lambda *a, **k: {}                # 盘口无数据


# ═════════════════════════ 信号生成层 ═════════════════════════
def session_of(ts_ms: int) -> str:
    h = _dt.datetime.fromtimestamp(ts_ms / 1000, _dt.timezone.utc).hour
    return "亚盘(UTC0-8)" if h < 8 else ("欧盘(UTC8-16)" if h < 16 else "美盘(UTC16-24)")


def n_for(strategy: str, override: int | None) -> int:
    return int(override) if override else DEFAULT_N.get(strategy, OTHER_DEFAULT_N)


def _judge_dispatch(route: str, k1h, k4h, k15, ind, chg24):
    from shared import judges as J
    fn = {"reversal": J.judge_reversal, "pullback": J.judge_pullback, "breakout": J.judge_breakout,
          "range_bounce": J.judge_range_bounce, "stop_hunt": J.judge_stop_hunt,
          "oi_divergence": J.judge_oi_divergence, "ema_trend": J.judge_ema_trend}[route]
    ok, d = fn(k1h, k4h, k15, ind, chg24, 0.0)
    if not isinstance(d, dict):
        d = {"reason": d}
    if route == "reversal":
        direction = str(d.get("direction", "long")).upper()
    elif route == "breakout":
        direction = str(d.get("breakout_direction", "long")).upper()
    elif route == "range_bounce":
        direction = "LONG"
    elif route == "stop_hunt":
        direction = str(d.get("direction", "LONG")).upper()
    elif route == "oi_divergence":
        direction = str(d.get("direction", "long")).upper()
    elif route == "ema_trend":
        direction = str(d.get("direction") or "long").upper()
    else:
        direction = "LONG" if ind.get("ema_bull_1h") else "SHORT"
    if direction not in ("LONG", "SHORT"):
        direction = "LONG"
    return bool(ok), d, direction


def generate_signals(sym: str, k1h_all: list, k15_all: list, strategies: list, start_ms: int, warmup: int = 120):
    """
    在每根已收盘 1H K 线 i 的收盘时刻 T 生成信号（入场价=第 i 根收盘价）。
    返回 (signals, hours)：hours 为所有判定小时 (idx, atr, price)，用于计算无条件基线。
    """
    from scanner import calc_indicators, _is_weekend_period
    from shared.indicators import atr as _atr

    k4h_all = aggregate_4h(k1h_all)
    t4 = [int(k[0]) for k in k4h_all]
    t15 = [int(k[0]) for k in k15_all]
    sigs, hours, counters = [], [], {"hours": 0, "judge_pass": 0}
    for i in range(warmup, len(k1h_all)):
        T = int(k1h_all[i][0]) + H1                                   # 第 i 根收盘时刻
        if T <= start_ms:
            continue
        if int(k1h_all[i][0]) - int(k1h_all[i - 1][0]) != H1:         # 数据断档
            continue
        k1h = k1h_all[i - warmup + 1:i + 1]                            # 已收盘
        j4 = bisect.bisect_right(t4, T - H4)
        k4h = k4h_all[max(0, j4 - 100):j4]
        j15 = bisect.bisect_left(t15, T)
        k15 = k15_all[max(0, j15 - 120):j15]
        if len(k1h) < 60 or len(k4h) < 20 or len(k15) < 40:
            continue
        price = float(k1h_all[i][4])
        ind = calc_indicators(k15 + [_forming(T, price, M15, float(k15[-1][5]))],
                              k1h + [_forming(T, price, H1, float(k1h[-1][5]))],
                              k4h + [_forming(T, price, H4, float(k4h[-1][5]))])
        c1h = [float(k[4]) for k in k1h]
        chg24 = (c1h[-1] - float(k1h[-24][1])) / float(k1h[-24][1]) * 100
        wk = _is_weekend_period(_dt.datetime.fromtimestamp(T / 1000, _dt.timezone.utc))
        ind.update({"chg24": chg24, "symbol": sym, "vol24_usd": sum(float(k[7]) for k in k1h[-24:]),
                    "_weekend_mode": wk})
        atr_v = float(ind.get("atr_1h") or 0) or float(_atr(k1h) or 0)
        counters["hours"] += 1
        hours.append((i, atr_v, price))
        for route in strategies:
            ok, d, direction = _judge_dispatch(route, k1h, k4h, k15, ind, chg24)
            if not ok:
                continue
            counters["judge_pass"] += 1
            vr = ind.get("vol_ratio", ind.get("vol_ratio_1h"))
            sigs.append({"symbol": sym, "strategy": route, "direction": direction, "idx": i, "ts": T,
                         "session": session_of(T), "weekend": wk, "price": price, "atr": atr_v,
                         "atr_pct": (atr_v / price * 100) if price > 0 else None,
                         "rsi_1h": ind.get("rsi_1h"), "rsi_4h": ind.get("rsi_4h"), "chg24": chg24,
                         "vol_ratio": vr, "strength": d.get("signal_strength")})
    return sigs, hours, counters


def dedupe(sigs: list, n_hours_of) -> list:
    """同一 (标的,策略) 在上一个信号的观察窗口（N 根）内不再计新信号，避免重叠样本。"""
    out, last = [], {}
    for s in sorted(sigs, key=lambda x: x["ts"]):
        key = (s["symbol"], s["strategy"])
        n = n_hours_of(s["strategy"])
        if key in last and s["idx"] - last[key] < n:
            continue
        last[key] = s["idx"]
        out.append(s)
    return out


# ═════════════════════════ 结果判定层 ═════════════════════════
def evaluate(k1h_all: list, idx: int, direction: str, entry: float, atr: float, n: int):
    """
    入场=第 idx 根收盘价；观察第 idx+1 .. idx+n 根 1H。数据不足/断档/ATR 无效返回 None。
    hit：窗口内最高(多)/最低(空)价触及 entry±1×ATR；ret_pct：第 n 根收盘价平仓的按方向收益%；win：ret_pct>0。
    """
    if n < 1 or atr <= 0 or entry <= 0 or idx + n >= len(k1h_all):
        return None
    if int(k1h_all[idx + n][0]) - int(k1h_all[idx][0]) != n * H1:
        return None
    seg = k1h_all[idx + 1:idx + n + 1]
    long_ = direction == "LONG"
    target = entry + atr if long_ else entry - atr
    hit_bar = None
    for j, b in enumerate(seg, 1):
        if (float(b[2]) >= target) if long_ else (float(b[3]) <= target):
            hit_bar = j
            break
    close = float(seg[-1][4])
    sign = 1.0 if long_ else -1.0
    ret = (close - entry) / entry * 100 * sign
    hi, lo = max(float(b[2]) for b in seg), min(float(b[3]) for b in seg)
    mfe = ((hi - entry) if long_ else (entry - lo)) / atr
    mae = ((entry - lo) if long_ else (hi - entry)) / atr
    return {"hit": hit_bar is not None, "hit_bar": hit_bar, "ret_pct": ret, "win": ret > 0,
            "mfe_atr": mfe, "mae_atr": mae}


def limit_price(direction: str, price: float, offset: float) -> float:
    return price * (1 - offset) if direction == "LONG" else price * (1 + offset)


def entry_for(k1h_all: list, idx: int, direction: str, price: float, n: int, mode: str = "direct",
              offset: float = 0.005):
    """
    返回 (fill_idx, entry_price)。fill_idx：成交 K 线下标；None=窗口内未成交（限价模式）；"na"=窗口数据不足/断档。
    direct：fill_idx=idx，入场价=收盘价。limit：在 idx+1..idx+n 内首根触及挂单价的 K 线，入场价=挂单价。
    """
    if n < 1 or idx + n >= len(k1h_all) or int(k1h_all[idx + n][0]) - int(k1h_all[idx][0]) != n * H1:
        return "na", price
    if mode != "limit":
        return idx, price
    lim = limit_price(direction, price, offset)
    for j in range(idx + 1, idx + n + 1):
        if (float(k1h_all[j][3]) <= lim) if direction == "LONG" else (float(k1h_all[j][2]) >= lim):
            return j, lim
    return None, lim


def accumulate_baseline(acc: dict, k1h_all: list, hours: list, ns, mode: str = "direct", offset: float = 0.005) -> None:
    """所有判定小时、LONG/SHORT 两个方向、各 N 下的无条件命中/盈利计数（与信号用同一入场规则）。
    acc[(d,n)] = [命中数, 盈利数, 有效数, 可判定的挂单数, 成交数]"""
    for idx, atr, price in hours:
        for d in ("LONG", "SHORT"):
            for n in ns:
                fi, ep = entry_for(k1h_all, idx, d, price, n, mode, offset)
                if fi == "na":
                    continue
                a = acc.setdefault((d, n), [0, 0, 0, 0, 0])
                a[3] += 1
                if fi is None:
                    continue
                a[4] += 1
                e = evaluate(k1h_all, fi, d, ep, atr, n)
                if e is None:
                    continue
                a[0] += 1 if e["hit"] else 0
                a[1] += 1 if e["win"] else 0
                a[2] += 1


def finalize_baseline(acc: dict) -> dict:
    return {k: {"acc": v[0] / v[2], "win": v[1] / v[2], "n": v[2],
                "fill_rate": (v[4] / v[3]) if v[3] else None} for k, v in acc.items() if v[2] > 0}


# ═════════════════════════ 统计层 ═════════════════════════
def wilson(k: int, n: int, z: float = 1.96):
    if n == 0:
        return None
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return [round((c - m) / d, 4), round((c + m) / d, 4)]


def block(trades: list, base_rates: dict, n: int | None = None) -> dict:
    """n=None：每笔按自己的 n_hours 评估；否则统一用 n 评估（用于 N 曲线）。
    限价模式：未成交的信号只计入 signals/unfilled 与成交率，不进入准确率；窗口数据不足的计入 insufficient。"""
    nn = (lambda t: n) if n is not None else (lambda t: t["n_hours"])

    def status(t):
        if t.get("fills") is not None and t["fills"].get(nn(t), "na") is None:
            return "unfilled", None
        e = t["evals"].get(nn(t))
        return ("ok", e) if e else ("na", None)
    st = [(t,) + status(t) for t in trades]
    valid = [(t, e) for t, k, e in st if k == "ok"]
    unfilled = sum(1 for _, k, _ in st if k == "unfilled")
    na = sum(1 for _, k, _ in st if k == "na")
    judged = len(trades) - na
    out = {"signals": len(trades), "valid": len(valid), "insufficient": na, "unfilled": unfilled,
           "filled": judged - unfilled, "fill_rate": ((judged - unfilled) / judged) if judged > 0 else None}
    if n is not None:
        out["n_hours"] = n
    if not valid:
        out.update({"accuracy": None, "accuracy_ci95": None, "baseline_accuracy": None, "excess_accuracy": None,
                    "timestop_win_rate": None, "baseline_win_rate": None, "baseline_fill_rate": None,
                    "avg_ret_pct": None, "avg_mfe_atr": None, "avg_mae_atr": None, "long": 0, "short": 0})
        return out
    m = len(valid)
    hits = sum(1 for _, e in valid if e["hit"])
    wins = sum(1 for _, e in valid if e["win"])
    keys = [(t["direction"], nn(t)) for t, _ in valid if (t["direction"], nn(t)) in base_rates]
    b_acc = [base_rates[k]["acc"] for k in keys]
    b_win = [base_rates[k]["win"] for k in keys]
    b_fill = [base_rates[k]["fill_rate"] for k in keys if base_rates[k].get("fill_rate") is not None]
    acc = hits / m
    b_acc_m = sum(b_acc) / len(b_acc) if b_acc else None
    out.update({"accuracy": round(acc, 4), "accuracy_ci95": wilson(hits, m),
                "baseline_accuracy": round(b_acc_m, 4) if b_acc_m is not None else None,
                "excess_accuracy": round(acc - b_acc_m, 4) if b_acc_m is not None else None,
                "timestop_win_rate": round(wins / m, 4),
                "baseline_win_rate": round(sum(b_win) / len(b_win), 4) if b_win else None,
                "baseline_fill_rate": round(sum(b_fill) / len(b_fill), 4) if b_fill else None,
                "avg_ret_pct": round(sum(e["ret_pct"] for _, e in valid) / m, 4),
                "avg_mfe_atr": round(sum(e["mfe_atr"] for _, e in valid) / m, 3),
                "avg_mae_atr": round(sum(e["mae_atr"] for _, e in valid) / m, 3),
                "long": sum(1 for t, _ in valid if t["direction"] == "LONG"),
                "short": sum(1 for t, _ in valid if t["direction"] == "SHORT")})
    return out


def _rsi_ok(x):
    return lambda t: (t["rsi_1h"] is not None) and ((t["rsi_1h"] < x) if t["direction"] == "LONG"
                                                    else (t["rsi_1h"] > 100 - x))


def _chg_ok(x):
    return lambda t: (t["chg24"] <= -x) if t["direction"] == "LONG" else (t["chg24"] >= x)


def _vol_ok(z):
    return lambda t: (t["vol_ratio"] is not None) and t["vol_ratio"] >= z


def sensitivity(trades: list, base_rates: dict) -> dict:
    """reversal 信号上的二次过滤网格 + N 曲线。"""
    rev = [t for t in trades if t["strategy"] == "reversal"]
    out = {"note": "对已通过现行 judge 的 reversal 信号做二次过滤，只能评估比现行更严的门槛；"
                   "宽于现行门槛的取值与『未过滤』相同。RSI 做空侧取对称 100-X；chg24 做空侧取 +X。",
           "reversal_signals": len(rev), "unfiltered": block(rev, base_rates), "grids": {}}
    specs = {"rsi_1h": ("RSI门槛", RSI_GRID, _rsi_ok), "chg24": ("chg24门槛(%)", CHG_GRID, _chg_ok),
             "vol_ratio": ("vol_ratio门槛", VOL_GRID, _vol_ok)}
    for key, (label, grid, mk) in specs.items():
        rows = []
        for v in grid:
            sub = [t for t in rev if mk(v)(t)]
            b = block(sub, base_rates)
            b.update({"value": v, "same_as_unfiltered": len(sub) == len(rev)})
            rows.append(b)
        out["grids"][key] = {"label": label, "rows": rows}
    return out


def n_curve(trades: list, base_rates: dict, ns=N_GRID) -> dict:
    out = {}
    for s in sorted({t["strategy"] for t in trades}):
        sub = [t for t in trades if t["strategy"] == s]
        out[s] = [block(sub, base_rates, n) for n in ns]
    return out


def build_report(trades: list, base_rates: dict) -> dict:
    rep = {"overall": block(trades, base_rates), "by_strategy": {}, "by_session": {}, "by_direction": {},
           "by_mcap": {}, "by_mcap_strategy": {}}
    for s in sorted({t["strategy"] for t in trades}):
        rep["by_strategy"][s] = block([t for t in trades if t["strategy"] == s], base_rates)
    for s in ("亚盘(UTC0-8)", "欧盘(UTC8-16)", "美盘(UTC16-24)"):
        sub = [t for t in trades if t["session"] == s]
        if sub:
            rep["by_session"][s] = block(sub, base_rates)
    for d in ("LONG", "SHORT"):
        sub = [t for t in trades if t["direction"] == d]
        if sub:
            rep["by_direction"][d] = block(sub, base_rates)
    for g in [b[0] for b in MCAP_BINS] + ["未知"]:
        sub = [t for t in trades if t["mcap_group"] == g]
        if sub:
            rep["by_mcap"][g] = block(sub, base_rates)
            rep["by_mcap_strategy"][g] = {s: block([t for t in sub if t["strategy"] == s], base_rates)
                                          for s in sorted({t["strategy"] for t in sub})}
    return rep


# ═════════════════════════ 输出层 ═════════════════════════
def _p(v, d=1):
    return "   -  " if v is None else f"{v * 100:.{d}f}%"


def _row(name: str, b: dict, show_n=True, show_fill=False) -> str:
    ci = b.get("accuracy_ci95")
    cis = f"[{ci[0] * 100:.0f}-{ci[1] * 100:.0f}]" if ci else "-"
    ex = b.get("excess_accuracy")
    exs = "-" if ex is None else f"{ex * 100:+.1f}pp"
    ret = b.get("avg_ret_pct")
    rets = "-" if ret is None else f"{ret:+.3f}%"
    nn = f"{b.get('n_hours', ''):>2}" if show_n and b.get("n_hours") else "  "
    fill = ""
    if show_fill:
        fill = f" 成交{b.get('filled', 0):>4}({_p(b.get('fill_rate'))},基线{_p(b.get('baseline_fill_rate'))})"
    return (f"{name:<16}{nn} 信号{b['signals']:>4} 有效{b['valid']:>4}{fill} 准确率{_p(b['accuracy']):>7} {cis:<9}"
            f"基线{_p(b['baseline_accuracy']):>7} 超额{exs:>8} | 时间止损胜率{_p(b['timestop_win_rate']):>7}"
            f"(基线{_p(b['baseline_win_rate'])}) 均收益{rets:>9}")


def print_report(rep: dict, sens: dict, curve: dict, meta: dict) -> None:
    line = "=" * 150
    lim = meta.get("entry_mode") == "limit"
    row = lambda name, b, show_n=True: _row(name, b, show_n, lim)   # noqa: E731
    print(line)
    print(f"方向准确率回测 v1  标的={','.join(meta['symbols'])}  天数={meta['days']}  策略={','.join(meta['strategies'])}")
    entry_desc = (f"限价回踩(挂单价=收盘价∓{meta['limit_offset'] * 100:.2f}%，N根内未触及=未成交，不计入准确率)" if lim
                  else "信号K线收盘价直接入场")
    print(f"区间 {meta['range_utc'][0]} ~ {meta['range_utc'][1]} (UTC)  入场={entry_desc}  方向正确=N小时内触及±1×ATR  "
          f"时间止损=第N根收盘  liq_zones为空")
    print(line)
    print("【总体】")
    print(row("总计", rep["overall"], False))
    print("【按策略】（N=时间止损小时数）")
    for k, v in rep["by_strategy"].items():
        print(row(k, dict(v, n_hours=meta["n_by_strategy"].get(k))))
    print("【按时段】")
    for k, v in rep["by_session"].items():
        print(row(k, v, False))
    print("【按方向】")
    for k, v in rep["by_direction"].items():
        print(row(k, v, False))
    print("【市值分组】（当前市值，非历史）")
    for k, v in rep["by_mcap"].items():
        print(row(k, v, False))
    print("【参数敏感性 — reversal（现行门槛上的二次过滤）】")
    if not sens["reversal_signals"]:
        print("  reversal 在本次区间内没有信号，无法做敏感性分析")
    else:
        print(row("未过滤", sens["unfiltered"]))
        for key, g in sens["grids"].items():
            for r in g["rows"]:
                tag = "(=未过滤)" if r["same_as_unfiltered"] else ""
                print(row(f"{g['label']}={r['value']}{tag}", r))
    print("【时间止损 N 曲线】" + ("（限价模式下每个 N 用各自的成交窗口）" if lim else ""))
    for s, rows in curve.items():
        for r in rows:
            print(row(f"{s}", r))
    print(line)
    print("读法：准确率=N小时内触及±1×ATR 的比例；基线=同区间所有小时同方向同N、同一入场规则下的无条件概率；"
          "超额=准确率-基线。有效样本较少时请看 95% 置信区间 [下-上]，区间很宽说明结论不可靠。")
    if lim:
        print("限价模式提示：未成交的信号被排除，留下的是「先反向走 offset 才成交」的信号；"
              "成交率与准确率要一起看，不能拿成交后的准确率绝对值直接和直接入场的结果比。")


def write_outputs(out_dir: Path, stamp: str, payload: dict, trades: list):
    out_dir.mkdir(parents=True, exist_ok=True)
    jp, cp = out_dir / f"backtest_{stamp}.json", out_dir / f"backtest_{stamp}.csv"
    slim = [{k: v for k, v in t.items() if k not in ("evals", "fills")} for t in trades]
    payload = dict(payload, trades=slim)
    jp.write_text(json.dumps(payload, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    cols = ["ts_utc", "symbol", "strategy", "direction", "session", "weekend", "mcap_group", "mcap_usd", "n_hours",
            "entry_mode", "signal_close", "entry_price", "filled", "fill_delay_bars", "atr", "atr_pct", "rsi_1h",
            "rsi_4h", "chg24", "vol_ratio", "strength", "hit_1atr", "hit_bar", "ret_pct", "win", "mfe_atr", "mae_atr"]
    with cp.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for t in trades:
            e = t.get("eval") or {}
            w.writerow([_dt.datetime.fromtimestamp(t["ts"] / 1000, _dt.timezone.utc).strftime("%Y-%m-%d %H:%M"),
                        t["symbol"], t["strategy"], t["direction"], t["session"], int(t["weekend"]), t["mcap_group"],
                        t.get("mcap_usd") or "", t["n_hours"], t.get("entry_mode", "direct"), t["price"],
                        t.get("entry_price", t["price"]), int(t.get("filled", True)),
                        "" if t.get("fill_delay") is None else t["fill_delay"], round(t["atr"], 8),
                        "" if t["atr_pct"] is None else round(t["atr_pct"], 4),
                        "" if t["rsi_1h"] is None else round(float(t["rsi_1h"]), 2),
                        "" if t["rsi_4h"] is None else round(float(t["rsi_4h"]), 2), round(t["chg24"], 3),
                        "" if t["vol_ratio"] is None else round(float(t["vol_ratio"]), 3), t.get("strength") or "",
                        "" if not e else int(e["hit"]), "" if not e else (e["hit_bar"] or ""),
                        "" if not e else round(e["ret_pct"], 4), "" if not e else int(e["win"]),
                        "" if not e else round(e["mfe_atr"], 3), "" if not e else round(e["mae_atr"], 3)])
    return jp, cp


CAVEATS = [
    "入场：direct=信号 1H K 线收盘价市价入场；limit=收盘价∓offset 限价回踩（N 根内触及才成交，成交价=挂单价，"
    "成交那根只确认成交、观察从下一根开始）；均不计手续费、滑点、资金费率",
    "限价模式：未成交的信号被排除出准确率，选择效应会让成交样本偏向「先反向走」的信号；基线按同一规则计算",
    "liq_zones、盘口全部为空；资金费率、OI、BTC 趋势等实盘上下文为默认值（0/空），依赖它们的策略分支可能不触发或偏离实盘",
    "k15 取自历史 15m K 线，4h 由 1h 聚合；『未收盘 K 线』只合成开盘价，不含未来数据",
    "方向正确按 high/low 触及判定（不论先后顺序）；时间止损胜率只看第 N 根收盘价是否有利（>0），与是否曾触及 1ATR 无关",
    "同一 (标的,策略) 在上一信号的 N 小时观察窗口内不重复计信号；同一小时不同策略独立计数",
    "参数敏感性是对现行 judge 已通过信号的二次过滤，只能评估更严的门槛，不能评估放宽；RSI 做空侧取对称 100-X",
    "市值分组用当前 CoinGecko 市值（非历史），同 symbol 取市值最大者；BTC+ETH 全属大市值组，无法比较小市值",
    "基线=同区间所有判定小时、同方向、同 N 的无条件概率；样本重叠，仅用于判断准确率是否只是波动所致",
]


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="方向准确率回测框架 v1")
    ap.add_argument("--symbols", nargs="+", default=None, help="标的，默认 24h 成交额 Top10")
    ap.add_argument("--days", type=int, default=180, help="回测天数（最多 365）")
    ap.add_argument("--strategy", nargs="+", default=None, choices=ALL_STRATEGIES, help="默认全部")
    ap.add_argument("--time-stop", type=int, default=None, help="时间止损小时数；默认 reversal=4/pullback=2/ema_trend=6/其他=4")
    ap.add_argument("--entry-mode", choices=["direct", "limit"], default="direct",
                    help="入场方式：direct=信号K线收盘价直接入场（默认）；limit=限价回踩，N根内触及挂单价才成交")
    ap.add_argument("--limit-offset", type=float, default=0.005,
                    help="limit 模式的挂单偏移比例，默认 0.005（LONG 收盘价下方 0.5%%、SHORT 上方 0.5%%）")
    ap.add_argument("--output", default="results/", help="结果输出目录（默认 results/）")
    ap.add_argument("--no-cache", action="store_true", help="不使用本地 K 线缓存")
    ap.add_argument("--no-mcap", action="store_true", help="不拉市值（分组全部记为未知）")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    a = parse_args(argv)
    if not (0.0 <= a.limit_offset < 0.2):
        print("--limit-offset 需在 [0, 0.2) 内（比例，0.005=0.5%）", file=sys.stderr)
        return 2
    days = max(1, min(int(a.days), 365))
    strategies = a.strategy or list(ALL_STRATEGIES)
    n_hours_of = lambda s: n_for(s, a.time_stop)           # noqa: E731
    ns_all = sorted(set(N_GRID) | {n_hours_of(s) for s in strategies})
    install_offline_stubs()
    symbols = [s.upper() for s in a.symbols] if a.symbols else top_volume_symbols(10)
    caps = {} if a.no_mcap else load_market_caps(not a.no_cache)
    now_ms = int(time.time() * 1000)
    end_ms = (now_ms // H1) * H1
    start_ms = end_ms - days * 24 * H1
    warm_ms = start_ms - 600 * H1
    all_trades, per_symbol, base_acc = [], {}, {}
    for sym in symbols:
        try:
            k1h = load_klines(sym, "1h", warm_ms, end_ms, not a.no_cache)
            k15 = load_klines(sym, "15m", warm_ms, end_ms, not a.no_cache)
        except Exception as e:   # noqa: BLE001
            print(f"[backtest] {sym} 取数失败，跳过: {e}", file=sys.stderr)
            continue
        if len(k1h) < 200:
            print(f"[backtest] {sym} 历史数据不足（{len(k1h)} 根 1h），跳过", file=sys.stderr)
            continue
        t0 = time.time()
        sigs, hours, cnt = generate_signals(sym, k1h, k15, strategies, start_ms)
        sigs = dedupe(sigs, n_hours_of)
        mc = mcap_of(sym, caps)
        for s in sigs:
            s["n_hours"] = n_hours_of(s["strategy"])
            s["fills"], s["evals"] = {}, {}
            for n in ns_all:      # 限价窗口=该 N（与基线一致）；direct 模式成交下标=信号下标
                fi, ep = entry_for(k1h, s["idx"], s["direction"], s["price"], n, a.entry_mode, a.limit_offset)
                s["fills"][n] = fi
                s["evals"][n] = (evaluate(k1h, fi, s["direction"], ep, s["atr"], n)
                                 if fi not in (None, "na") else None)
                if n == s["n_hours"]:
                    s["entry_price"] = ep
            fi = s["fills"][s["n_hours"]]
            s["eval"] = s["evals"][s["n_hours"]]
            s["entry_mode"] = a.entry_mode
            s["filled"] = fi not in (None, "na")
            s["fill_delay"] = (fi - s["idx"]) if s["filled"] else None
            s["mcap_usd"], s["mcap_group"] = mc, mcap_group(mc)
        accumulate_baseline(base_acc, k1h, hours, ns_all, a.entry_mode, a.limit_offset)
        all_trades.extend(sigs)
        per_symbol[sym] = {**cnt, "signals_after_dedupe": len(sigs), "bars_1h": len(k1h), "bars_15m": len(k15),
                           "mcap_usd": mc, "mcap_group": mcap_group(mc), "seconds": round(time.time() - t0, 1)}
        by_s = {}
        for s in sigs:
            by_s[s["strategy"]] = by_s.get(s["strategy"], 0) + 1
        print(f"[backtest] {sym}: 1h={len(k1h)} 15m={len(k15)} 判定小时={cnt['hours']} judge通过={cnt['judge_pass']} "
              f"去重后信号={len(sigs)} {by_s} ({time.time() - t0:.0f}s)", flush=True)
    base_rates = finalize_baseline(base_acc)
    report = build_report(all_trades, base_rates)
    sens = sensitivity(all_trades, base_rates)
    curve = n_curve(all_trades, base_rates)
    fmt = lambda ms: _dt.datetime.fromtimestamp(ms / 1000, _dt.timezone.utc).strftime("%Y-%m-%d %H:%M")   # noqa: E731
    meta = {"symbols": list(per_symbol), "days": days, "strategies": strategies,
            "n_by_strategy": {s: n_hours_of(s) for s in strategies}, "time_stop_override": a.time_stop,
            "range_utc": [fmt(start_ms), fmt(end_ms)], "generated_at_utc": fmt(now_ms),
            "market_caps_loaded": bool(caps), "entry_mode": a.entry_mode, "limit_offset": a.limit_offset}
    print_report(report, sens, curve, meta)
    out = Path(a.output)
    if not out.is_absolute():
        out = Path.cwd() / out
    stamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    payload = {"meta": meta, "report": report, "sensitivity": sens, "n_curve": curve,
               "baseline": {f"{k[0]}|{k[1]}": v for k, v in base_rates.items()},
               "per_symbol": per_symbol, "caveats": CAVEATS}
    jp, cp = write_outputs(out, stamp, payload, all_trades)
    print(f"结果已保存: {jp}\n逐笔明细: {cp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
