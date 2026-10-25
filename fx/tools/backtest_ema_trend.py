"""ema_trend 最小回测（步骤2）。

用法：
  python3 tools/backtest_ema_trend.py --symbols BTCUSDT ETHUSDT --days 180
  python3 tools/backtest_ema_trend.py --top30 --days 180

只复用纯函数：scanner.calc_indicators / scanner._is_ema_trend_coarse、judges.judge_ema_trend、
calc_sl_tp.calc_sl_tp、position_manager._calc_trail_sl。不修改任何业务模块。
清算区全部传空；calc_sl_tp._orderbook_tp 在本进程内打桩为返回 None（不会发任何网络请求）；
TRAIL_MFE_ANCHOR 强制为 False（只用入场价阶梯 TRAIL_STEPS_TREND）。

决策时刻 = 每根已收盘 1H K 线的收盘点；入场价 = 该根收盘价；按固定风险（账户净值 2% ÷ 止损距离）定仓。
同一标的同时只有一个持仓；不同标的之间不限并发、不限总名义。
全部数据来自本地（fx/data/klines、fx/data/funding），回测期间不访问交易所。

保真度说明（报告里也会打印）：
  · 无清算区降级路径：SL=ATR 倍数 + 摆动点结构止损，TP=ATR/RR 降级，无 TP2；
  · 量比用决策点“收盘后的完整 K 线”成交量，实盘决策时那根 K 线通常未走完，量比被高估 → 信号偏多（偏乐观）；
  · 无 _adjust_sl_tp_once（30 分钟维护）、无多路由竞争、无冷却/黑名单、无 24h 成交额预筛；
  · 不模拟限价挂单是否成交；不模拟 executor 的降仓/杠杆/总名义上限；
  · trail 在 K 线收盘后用收盘价评估一次（实盘按 tick）；
  · k15 仅为占位（judge 在 OPT_EMA_TREND_TRIGGER_BAR 关闭时不使用它）。
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import sys
from bisect import bisect_right
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

HOUR_MS = 3_600_000
FOUR_H_MS = 4 * HOUR_MS
INIT_EQUITY = 10_000.0
RISK_PCT = 0.02            # 每笔风险 = 当前已实现净值的 2%
FEE_RATE = 0.0005          # Taker 0.05%，开仓、平仓各收一次
SLIP_RATE = 0.0005         # 固定滑点 0.05%（入场与止损/时间止损等市价出场；TP 限价成交不加滑点）
TP1_RATIO = 0.6
MAX_HOLD_BARS = 16
WIN_1H = 150
WIN_4H = 130
MIN_GROUP_N = 20
KLINE_COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
              "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]
FIDELITY_NOTES = [
    "无清算区降级路径：SL=ATR倍数(+摆动点结构止损)，TP=ATR/RR降级，无TP2；对有清算区时的实盘行为没有代表性",
    "量比用决策点收盘后的完整K线 → 偏乐观口径（实盘决策时该K线未走完，量比通常更低）",
    "未模拟：_adjust_sl_tp_once(30分钟维护)、多路由竞争、冷却/黑名单、24h成交额预筛、限价挂单成交、降仓/杠杆/总名义上限",
    "trail 每根K线收盘后用收盘价评估一次（实盘按tick）；同一根K线内止损与止盈同时触及按止损优先",
    "实盘无TP2时(单TP模式) TP1触发会全部平仓；本回测按 --tp1-ratio 分批（默认60%），设为1.0可对齐实盘单TP",
    "各阈值均为待校准的假设；回测结果不代表未来收益",
]

logger = logging.getLogger("fx.backtest_ema_trend")


# ───────────────────────── 模块加载与打桩 ─────────────────────────
def load_modules() -> dict:
    """导入业务纯函数并完成打桩。只在这里 import，避免 --help 时拉起重模块。"""
    logging.getLogger("fx").setLevel(logging.WARNING)
    import config
    config.TRAIL_MFE_ANCHOR = False                       # 强制关闭 MFE 锚点
    import shared.calc_sl_tp as C
    C._orderbook_tp = lambda *a, **k: None                # 订单簿降级打桩，绝不发真实请求
    import scanner as S
    import position_manager as PM
    from shared.judges import judge_ema_trend
    logging.getLogger("fx").setLevel(logging.WARNING)
    return {"config": config, "calc_sl_tp": C.calc_sl_tp, "calc_indicators": S.calc_indicators,
            "coarse": S._is_ema_trend_coarse, "judge": judge_ema_trend, "trail": PM._calc_trail_sl}


# ───────────────────────── 数据加载 ─────────────────────────
def load_klines_local(symbol: str, data_dir: Path) -> list:
    """读本地 1H K 线 → list[list]（币安原始列顺序，数值为 float/int）。parquet 优先，降级 SQLite。"""
    p = Path(data_dir) / "klines" / f"{symbol}_1h.parquet"
    recs = []
    if p.exists():
        import pandas as pd
        recs = pd.read_parquet(p).sort_values("open_time").to_dict("records")
    elif (Path(data_dir) / "klines.db").exists():
        with sqlite3.connect(Path(data_dir) / "klines.db") as con:
            try:
                cur = con.execute(f"SELECT {','.join(KLINE_COLS)} FROM klines_1h WHERE symbol=? ORDER BY open_time", (symbol,))
                recs = [dict(zip(KLINE_COLS, r)) for r in cur.fetchall()]
            except sqlite3.OperationalError:
                recs = []
    return [[int(r["open_time"]), float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
             float(r["volume"]), int(r["close_time"]), float(r["quote_volume"]), int(r["trades"]),
             float(r["taker_buy_base"]), float(r["taker_buy_quote"])] for r in recs]


def load_funding_local(symbol: str, data_dir: Path) -> list:
    """读本地资金费率 → [(fundingTime, rate)] 升序；跳过损坏行。"""
    p = Path(data_dir) / "funding" / f"{symbol}_funding.jsonl"
    out = []
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            try:
                r = json.loads(line)
                out.append((int(r["fundingTime"]), float(r["fundingRate"])))
            except Exception:
                continue
    return sorted(out)


def aggregate_4h(rows: list) -> list:
    """1H → 4H（UTC 对齐），只保留4根齐全的完整 4H K 线。"""
    buckets: dict = {}
    for r in rows:
        buckets.setdefault(r[0] // FOUR_H_MS * FOUR_H_MS, []).append(r)
    out = []
    for b in sorted(buckets):
        rs = buckets[b]
        if len(rs) < 4:
            continue
        out.append([b, rs[0][1], max(x[2] for x in rs), min(x[3] for x in rs), rs[-1][4], sum(x[5] for x in rs),
                    b + FOUR_H_MS - 1, sum(x[7] for x in rs), sum(x[8] for x in rs),
                    sum(x[9] for x in rs), sum(x[10] for x in rs)])
    return out


def build_windows(rows: list, i: int, k4h_all: list, k4h_ends: list) -> tuple:
    """决策点 = 第 i 根 1H 收盘。返回 (k1h, k4h)：
    k1h 末根 = 刚收盘的这根（对应实盘“当前根”，指标用 [:-1]，价格/量比用末根完整数据）；
    k4h 末根 = 当前进行中的 4H（部分聚合，或刚开盘的平盘占位），其余为已收盘 4H。"""
    t_i = rows[i][0]
    decision = t_i + HOUR_MS
    k1h = rows[max(0, i + 1 - WIN_1H): i + 1]
    n_closed = bisect_right(k4h_ends, decision)
    closed = k4h_all[max(0, n_closed - (WIN_4H - 1)): n_closed]
    b = t_i // FOUR_H_MS * FOUR_H_MS
    if decision == b + FOUR_H_MS:                       # 刚好是 4H 边界：新 4H 刚开盘，放平盘占位
        c = rows[i][4]
        cur = [b + FOUR_H_MS, c, c, c, c, 0.0, b + 2 * FOUR_H_MS - 1, 0.0, 0, 0.0, 0.0]
    else:
        part = []
        k = i
        while k >= 0 and rows[k][0] >= b:
            part.append(rows[k]); k -= 1
        part.reverse()
        cur = [b, part[0][1], max(x[2] for x in part), min(x[3] for x in part), part[-1][4],
               sum(x[5] for x in part), b + FOUR_H_MS - 1, sum(x[7] for x in part), sum(x[8] for x in part),
               sum(x[9] for x in part), sum(x[10] for x in part)]
    return k1h, closed + [cur]


# ───────────────────────── 信号与入场参数 ─────────────────────────
def evaluate_signal(mods: dict, symbol: str, rows: list, i: int, k4h: list, k1h: list, funding_hist: list) -> dict | None:
    """返回 {"direction","tier","strength","params","ind","detail"} 或 None（粗筛/judge/calc_sl_tp 未通过）。"""
    ind = mods["calc_indicators"](k1h, k1h, k4h)       # k15 仅为占位（judge 默认不用）
    ind["symbol"] = symbol
    price = float(k1h[-1][4])
    p24 = float(k1h[-25][4]) if len(k1h) >= 25 else float(k1h[0][4])
    chg24 = (price - p24) / p24 * 100
    if len(k1h) < 25 or not mods["coarse"](ind, chg24):
        return None
    t_dec = rows[i][0] + HOUR_MS
    j = bisect_right([t for t, _ in funding_hist], t_dec) - 1
    funding = funding_hist[j][1] if j >= 0 else 0.0     # 决策点之前最近一次已结算费率
    ok, detail = mods["judge"](k1h, k4h, k1h, ind, chg24, funding)
    if not ok:
        return None
    sig = {"mode": "ema_trend", "direction": (detail.get("direction") or "long").upper(),
           "volatility_tier": detail.get("volatility_tier", "mid"), "symbol": symbol,
           "signal_strength": detail.get("signal_strength", "medium"), "ind": ind, "judge_detail": detail}
    params = mods["calc_sl_tp"](sig, price, float(ind["atr_1h"]), {}, k1h, float(ind.get("ema20_1h", 0.0)))
    if not isinstance(params, dict) or "error" in params:
        return None
    return {"direction": sig["direction"], "tier": sig["volatility_tier"], "strength": sig["signal_strength"],
            "params": params, "ind": ind, "atr": float(ind["atr_1h"])}


# ───────────────────────── 持仓模拟 ─────────────────────────
def _slip_in(direction: str, px: float) -> float:       # 入场：买多更贵 / 卖空更便宜
    return px * (1 + SLIP_RATE) if direction == "LONG" else px * (1 - SLIP_RATE)


def _slip_out(direction: str, px: float) -> float:      # 市价出场：卖多更便宜 / 买回空更贵
    return px * (1 - SLIP_RATE) if direction == "LONG" else px * (1 + SLIP_RATE)


def open_position(sig: dict, symbol: str, i: int, rows: list, equity: float) -> dict | None:
    p = sig["params"]
    d = sig["direction"]
    entry_fill = _slip_in(d, float(p["entry_price"]))
    sl = float(p["sl_price"])
    sl_frac = abs(entry_fill - sl) / entry_fill
    if sl_frac <= 0 or not p.get("tp1_price"):
        return None
    risk_amount = equity * RISK_PCT
    notional = risk_amount / sl_frac
    qty = notional / entry_fill
    entry_fee = notional * FEE_RATE
    return {
        "symbol": symbol, "direction": d, "tier": sig["tier"], "strength": sig["strength"], "entry_idx": i,
        "entry_time": rows[i][0] + HOUR_MS, "entry_price": entry_fill, "sl_initial": sl, "sl": sl,
        "tp1": float(p["tp1_price"]), "atr": sig["atr"], "sl_frac": sl_frac, "rr_tp1": p.get("rr_tp1"),
        "sl_degraded": bool(p.get("sl_degraded")), "tp_degraded": bool(p.get("tp_degraded")),
        "risk_amount": risk_amount, "notional": notional, "qty0": qty, "qty": qty, "tp1_filled": False,
        "gross": 0.0, "fees": entry_fee, "slippage": notional * SLIP_RATE, "funding": 0.0, "legs": [],
        "trail": {"entry_price": entry_fill, "direction": d, "mode": "ema_trend", "atr_at_entry": sig["atr"],
                  "sl_price": sl, "tp1_filled": False, "rr_protection_triggered": False},
    }


def _leg(pos: dict, qty: float, px: float, bar_time: int, reason: str, market: bool) -> None:
    d = pos["direction"]
    gross = (px - pos["entry_price"]) * qty if d == "LONG" else (pos["entry_price"] - px) * qty
    pos["gross"] += gross
    pos["fees"] += qty * px * FEE_RATE
    if market:
        pos["slippage"] += qty * px * SLIP_RATE / (1 - SLIP_RATE if d == "LONG" else 1 + SLIP_RATE)
    pos["qty"] -= qty
    pos["legs"].append({"time": bar_time, "reason": reason, "qty": round(qty, 8), "price": round(px, 8)})


def manage_bar(pos: dict, bar: list, j: int, funding_hist: list, trail_fn, tp1_ratio: float) -> bool:
    """用第 j 根 1H K 线推进持仓。返回 True = 已全部平仓。顺序：资金费 → 盘中(止损优先，其次TP1) → 时间止损 → trail。"""
    o, h, lo, c = bar[1], bar[2], bar[3], bar[4]
    t0, t1 = bar[0], bar[0] + HOUR_MS
    d = pos["direction"]
    long_ = d == "LONG"
    # 资金费：多头付正费率，空头收（规则：多 -notional×rate，空 +notional×rate）
    for ft, rate in funding_hist:
        if pos["entry_time"] < ft <= t1 and t0 < ft <= t1:
            pay = pos["qty"] * c * rate
            pos["funding"] += pay if long_ else -pay
    sl = pos["sl"]
    # 止损优先（含跳空：开盘已越过止损按开盘价成交）
    if (lo <= sl) if long_ else (h >= sl):
        fill = min(o, sl) if long_ else max(o, sl)
        reason = ("sl_after_tp1" if pos["tp1_filled"] else ("sl" if sl == pos["sl_initial"] else "sl_trailed"))
        _leg(pos, pos["qty"], _slip_out(d, fill), t0, reason, True)
        pos["exit_reason"], pos["exit_time"] = reason, t1
        return True
    # TP1
    if not pos["tp1_filled"] and ((h >= pos["tp1"]) if long_ else (lo <= pos["tp1"])):
        fill = max(o, pos["tp1"]) if long_ else min(o, pos["tp1"])      # 限价单，跳空更优价成交，不加滑点
        part = pos["qty"] if tp1_ratio >= 1.0 else pos["qty0"] * tp1_ratio
        _leg(pos, part, fill, t0, "tp1", False)
        pos["tp1_filled"] = True
        pos["trail"]["tp1_filled"] = True
        if pos["qty"] <= 1e-12:
            pos["exit_reason"], pos["exit_time"] = "tp1", t1
            return True
        # TP1 后剩余仓位 SL 移到入场价（保本）；同根K线若又回到入场价 → 悲观地按保本止损出
        pos["sl"] = max(pos["sl"], pos["entry_price"]) if long_ else min(pos["sl"], pos["entry_price"])
        if (lo <= pos["sl"]) if long_ else (h >= pos["sl"]):
            _leg(pos, pos["qty"], _slip_out(d, pos["sl"]), t0, "sl_after_tp1", True)
            pos["exit_reason"], pos["exit_time"] = "sl_after_tp1", t1
            return True
    # 时间止损：满 16 根收盘平仓
    if j - pos["entry_idx"] >= MAX_HOLD_BARS:
        _leg(pos, pos["qty"], _slip_out(d, c), t0, "time", True)
        pos["exit_reason"], pos["exit_time"] = "time", t1
        return True
    # trail：收盘后评估一次，生效于下一根K线
    pos["trail"]["sl_price"] = pos["sl"]
    new_sl = trail_fn(pos["trail"], c)
    if new_sl is not None and ((new_sl > pos["sl"]) if long_ else (new_sl < pos["sl"])):
        pos["sl"] = float(new_sl)
    return False


def finalize(pos: dict, equity_before: float) -> dict:
    net = pos["gross"] - pos["fees"] - pos["funding"]
    return {
        "symbol": pos["symbol"], "direction": pos["direction"], "tier": pos["tier"], "strength": pos["strength"],
        "entry_time": pos["entry_time"], "entry_price": round(pos["entry_price"], 8),
        "sl_initial": round(pos["sl_initial"], 8), "tp1": round(pos["tp1"], 8),
        "sl_dist_pct": round(pos["sl_frac"] * 100, 4), "rr_tp1": pos["rr_tp1"],
        "sl_degraded": pos["sl_degraded"], "tp_degraded": pos["tp_degraded"],
        "notional": round(pos["notional"], 2), "risk_amount": round(pos["risk_amount"], 2),
        "exit_time": pos["exit_time"], "exit_reason": pos["exit_reason"], "tp1_hit": pos["tp1_filled"],
        "hold_hours": round((pos["exit_time"] - pos["entry_time"]) / HOUR_MS, 1),
        "gross_pnl": round(pos["gross"], 4), "fees": round(pos["fees"], 4), "slippage": round(pos["slippage"], 4),
        "funding": round(pos["funding"], 4), "net_pnl": round(net, 4),
        "r_multiple": round(net / pos["risk_amount"], 4) if pos["risk_amount"] else 0.0,
        "equity_before": round(equity_before, 2), "legs": pos["legs"],
    }


# ───────────────────────── 回测主循环 ─────────────────────────
def run_backtest(mods: dict, data: dict, days: int, tp1_ratio: float = TP1_RATIO, progress: bool = False) -> dict:
    """data: {symbol: {"rows": [...], "funding": [...]}}。按时间轴在所有标的上同步推进，净值按已实现盈亏滚动。"""
    prep = {}
    for sym, d in data.items():
        rows = d["rows"]
        if len(rows) < 60:
            continue
        k4 = aggregate_4h(rows)
        prep[sym] = {"rows": rows, "k4": k4, "k4_ends": [x[6] + 1 for x in k4], "funding": d["funding"],
                     "idx": {r[0]: n for n, r in enumerate(rows)}}
    if not prep:
        return {"trades": [], "equity": INIT_EQUITY, "skipped": {}, "start": 0, "end": 0}
    last_t = max(p["rows"][-1][0] for p in prep.values())
    start_t = last_t - days * 24 * HOUR_MS
    timeline = sorted({r[0] for p in prep.values() for r in p["rows"] if r[0] >= start_t - 0})
    equity = INIT_EQUITY
    open_pos: dict = {}
    trades: list = []
    stats = {"signals_checked": 0, "signals": 0, "calc_error": 0}
    for n_t, t in enumerate(timeline):
        for sym, p in prep.items():
            j = p["idx"].get(t)
            if j is None:
                continue
            rows = p["rows"]
            closed_now = False
            pos = open_pos.get(sym)
            if pos is not None:
                if manage_bar(pos, rows[j], j, p["funding"], mods["trail"], tp1_ratio):
                    rec = finalize(pos, equity)
                    equity += rec["net_pnl"]
                    rec["equity_after"] = round(equity, 2)
                    trades.append(rec)
                    del open_pos[sym]
                    closed_now = True
            if sym in open_pos or closed_now or j < 30:
                continue                                    # 有持仓、或本根刚平仓（不在同根立即再入场）
            if t + HOUR_MS > last_t + HOUR_MS:
                continue
            stats["signals_checked"] += 1
            k1h, k4h = build_windows(rows, j, p["k4"], p["k4_ends"])
            sig = evaluate_signal(mods, sym, rows, j, k4h, k1h, p["funding"])
            if sig is None:
                continue
            stats["signals"] += 1
            newp = open_position(sig, sym, j, rows, equity)
            if newp is None:
                stats["calc_error"] += 1
                continue
            open_pos[sym] = newp
        if progress and n_t % 500 == 0:
            print(f"  进度 {n_t}/{len(timeline)} 笔数={len(trades)}", flush=True)
    # 数据结束仍持仓：按最后一根收盘价平仓，标注 end_of_data
    for sym, pos in list(open_pos.items()):
        last = prep[sym]["rows"][-1]
        _leg(pos, pos["qty"], _slip_out(pos["direction"], last[4]), last[0], "end_of_data", True)
        pos["exit_reason"], pos["exit_time"] = "end_of_data", last[0] + HOUR_MS
        rec = finalize(pos, equity)
        equity += rec["net_pnl"]
        rec["equity_after"] = round(equity, 2)
        trades.append(rec)
    trades.sort(key=lambda r: r["exit_time"])
    return {"trades": trades, "equity": equity, "stats": stats, "start": start_t, "end": last_t,
            "symbols": sorted(prep)}


# ───────────────────────── 指标与报告 ─────────────────────────
def group_stats(trades: list) -> dict:
    n = len(trades)
    if n == 0:
        return {"n": 0}
    wins = [t["net_pnl"] for t in trades if t["net_pnl"] > 0]
    losses = [t["net_pnl"] for t in trades if t["net_pnl"] <= 0]
    avg_w = sum(wins) / len(wins) if wins else 0.0
    avg_l = sum(losses) / len(losses) if losses else 0.0
    return {"n": n, "win_rate": len(wins) / n, "avg_win": avg_w, "avg_loss": avg_l,
            "payoff": (avg_w / abs(avg_l)) if avg_l else float("inf") if avg_w else 0.0,
            "expectancy_r": sum(t["r_multiple"] for t in trades) / n,
            "net": sum(t["net_pnl"] for t in trades), "low_sample": n < MIN_GROUP_N}


def max_drawdown(trades: list) -> float:
    peak, eq, mdd = INIT_EQUITY, INIT_EQUITY, 0.0
    for t in trades:
        eq += t["net_pnl"]
        peak = max(peak, eq)
        mdd = max(mdd, (peak - eq) / peak)
    return mdd


def sharpe_daily(trades: list) -> float | None:
    """按 UTC 日已实现盈亏（占当日期初净值）算日收益，年化 sqrt(365)；不足 3 天返回 None。"""
    if not trades:
        return None
    by_day: dict = {}
    for t in trades:
        by_day[t["exit_time"] // (24 * HOUR_MS)] = by_day.get(t["exit_time"] // (24 * HOUR_MS), 0.0) + t["net_pnl"]
    d0, d1 = min(by_day), max(by_day)
    if d1 - d0 < 2:
        return None
    eq, rets = INIT_EQUITY, []
    for d in range(d0, d1 + 1):
        pnl = by_day.get(d, 0.0)
        rets.append(pnl / eq)
        eq += pnl
    m = sum(rets) / len(rets)
    var = sum((r - m) ** 2 for r in rets) / (len(rets) - 1)
    return (m / math.sqrt(var) * math.sqrt(365)) if var > 0 else None


def _fmt_group(name: str, g: dict) -> str:
    if g["n"] == 0:
        return f"  {name:<10} 0 笔"
    s = (f"  {name:<10} {g['n']:>4} 笔  胜率 {g['win_rate']*100:5.1f}%  盈亏比 {g['payoff']:.2f}  "
         f"期望 {g['expectancy_r']:+.3f}R  净收益 {g['net']:+.1f}U")
    return s + ("   ⚠ 样本不足，结论仅供参考" if g["low_sample"] else "")


def build_report(res: dict, days: int, tp1_ratio: float) -> str:
    tr = res["trades"]
    L = ["=" * 64, f"ema_trend 回测报告（最近{days}天，{len(res.get('symbols', []))} 个标的，TP1平仓比例 {tp1_ratio:.0%}）", "=" * 64]
    if not tr:
        L.append("没有产生任何交易：可能是数据不足（4H EMA50 至少需要约 9 天预热）或信号条件过严。")
    else:
        g = group_stats(tr)
        sh = sharpe_daily(tr)
        L += ["【总体】",
              f"  交易笔数 {g['n']}   胜率 {g['win_rate']*100:.1f}%   盈亏比 {g['payoff']:.2f}   期望 {g['expectancy_r']:+.3f}R",
              f"  总收益 {res['equity']-INIT_EQUITY:+.1f}U（{(res['equity']/INIT_EQUITY-1)*100:+.2f}%，初始 {INIT_EQUITY:.0f}U，按已实现盈亏滚动）",
              f"  最大回撤 {max_drawdown(tr)*100:.2f}%（已实现口径）   夏普 {('%.2f' % sh) if sh is not None else '样本天数不足'}"]
        if g["low_sample"]:
            L.append("  ⚠ 总体样本不足，结论仅供参考")
        L.append("【分组】")
        for name, key, vals in (("方向", "direction", ("LONG", "SHORT")), ("波动层", "tier", ("low", "mid", "high"))):
            L.append(f" 按{name}：")
            for v in vals:
                L.append(_fmt_group(v, group_stats([t for t in tr if t[key] == v])))
        n = len(tr)
        cnt = lambda f: sum(1 for t in tr if f(t))
        L += ["【出场归因】",
              f"  止损率(含移动/保本后止损) {cnt(lambda t: t['exit_reason'] in ('sl','sl_trailed','sl_after_tp1'))/n*100:.1f}%"
              f"（初始止损 {cnt(lambda t: t['exit_reason']=='sl')/n*100:.1f}% / 移动止损 {cnt(lambda t: t['exit_reason']=='sl_trailed')/n*100:.1f}% / TP1后保本止损 {cnt(lambda t: t['exit_reason']=='sl_after_tp1')/n*100:.1f}%）",
              f"  TP1触发率 {cnt(lambda t: t['tp1_hit'])/n*100:.1f}%   时间止损率 {cnt(lambda t: t['exit_reason']=='time')/n*100:.1f}%"
              f"   数据末尾强平 {cnt(lambda t: t['exit_reason']=='end_of_data')}笔",
              "【成本】",
              f"  手续费 {sum(t['fees'] for t in tr):.1f}U   滑点 {sum(t['slippage'] for t in tr):.1f}U（已含在成交价里）   "
              f"资金费 {sum(t['funding'] for t in tr):+.1f}U（正=支出）",
              f"  毛利(含滑点) {sum(t['gross_pnl'] for t in tr):+.1f}U → 扣费用/资金费后净 {sum(t['net_pnl'] for t in tr):+.1f}U",
              f"  降级止损 {cnt(lambda t: t['sl_degraded'])}/{n} 笔（实盘此类单会被降仓50%，回测未降）"]
    st = res.get("stats") or {}
    if st:
        L.append(f"【信号】检查 {st.get('signals_checked', 0)} 个决策点，通过 {st.get('signals', 0)} 个，入场参数无效 {st.get('calc_error', 0)} 个")
    L.append("【保真度说明】")
    L += [f"  · {x}" for x in FIDELITY_NOTES]
    return "\n".join(L)


def write_trades(trades: list, out_dir: Path) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"ema_trend_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"
    with open(p, "w", encoding="utf-8") as f:
        for t in trades:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    return p


# ───────────────────────── 入口 ─────────────────────────
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="ema_trend 最小回测（本地数据，无清算区降级路径）")
    ap.add_argument("--symbols", nargs="+")
    ap.add_argument("--top30", action="store_true", help="扫描池24h成交额前30（仅保留本地已有数据的标的）")
    ap.add_argument("--days", type=int, default=180)
    ap.add_argument("--data-dir", default=str(ROOT / "data"))
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--tp1-ratio", type=float, default=TP1_RATIO)
    a = ap.parse_args(argv)
    data_dir = Path(a.data_dir)
    syms = a.symbols
    if not syms and a.top30:
        from tools.data_downloader import top_symbols
        syms = top_symbols(30)
    if not syms:
        print("未指定标的：用 --symbols 或 --top30")
        return 2
    data, missing = {}, []
    for s in syms:
        rows = load_klines_local(s, data_dir)
        if rows:
            data[s] = {"rows": rows, "funding": load_funding_local(s, data_dir)}
        else:
            missing.append(s)
    if missing:
        print(f"⚠ 本地无K线数据，已跳过：{missing}（先运行 tools/data_downloader.py）")
    if not data:
        print("没有可用数据")
        return 2
    mods = load_modules()
    res = run_backtest(mods, data, a.days, tp1_ratio=a.tp1_ratio, progress=True)
    print(build_report(res, a.days, a.tp1_ratio))
    out = write_trades(res["trades"], Path(a.out_dir) if a.out_dir else data_dir / "backtest")
    print(f"\n逐笔记录：{out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
