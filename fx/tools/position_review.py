#!/usr/bin/env python3
"""
fx/tools/position_review.py — 单笔仓位开仓逻辑复盘（只读）

用法：
  python3 tools/position_review.py UNITREEUSDT [--direction long|short] [--all]
  python3 tools/position_review.py --today
  python3 tools/position_review.py --closed
可选：--log-dir DIR（默认 fx/logs）、--log-tz-offset H（system.log 本地时间相对 UTC 的小时偏移，默认 0）、
      --no-funding（不查交易所，完全离线）

数据来源（全部本地文件，只读，不改任何状态）：
  system.log[.1~.5]  评分拆解 / JUDGE_REJECT / 注册监控 / trigger-dispatch
  pos_state.json     持仓中记录（止盈止损/pos_id/judge_detail）
  trade_log.jsonl    已平仓记录
  tp_sl_adjust_log.jsonl 动态调整历史
  trigger_log.jsonl / scan_signals.jsonl  开仓路径识别
唯一的网络请求：资金费率/现价（可选，失败或超时静默跳过；--no-funding 关闭）。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from pathlib import Path

FX_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG_DIR = FX_ROOT / "logs"
Q = "?"
# 服务器本地时区相对 UTC 的小时偏移（东八区返回 8.0）；用作 --log-tz-offset 的默认值
_local_tz_offset = -(_time.altzone if _time.localtime().tm_isdst > 0 else _time.timezone) / 3600

# ── 日志行解析 ───────────────────────────────────────────────────────────
LINE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ (\w+) (\S+) (.*)$")
# scanner.py: [scanner] SYM score {base}→{final} (btc=+0 cg=+0 strat=+0 shock=+0 tier=+0 pulse=+0 hub_bonus=+0)
SCORE_RE = re.compile(
    r"\[scanner\] (?P<sym>\S+) score (?P<base>\d+)→(?P<final>\d+) "
    r"\(btc=(?P<btc>[+-]\d+) cg=(?P<cg>[+-]\d+) strat=(?P<strat>[+-]\d+) shock=(?P<shock>[+-]\d+) "
    r"tier=(?P<tier>[+-]\d+) pulse=(?P<pulse>[+-]\d+) hub_bonus=(?P<hub>[+-]\d+)\)")
# scanner.py: [scanner] SYM ema_trend base=65 btc_adj=-5 strategy_adj=-15 (...)
BASE_RE = re.compile(r"\[scanner\] (?P<sym>\S+) (?P<st>\w+) base=(?P<base>\d+) btc_adj=(?P<btc>[+-]\d+)")
# scanner.py: [JUDGE_REJECT] SYM route DIR: reason
REJECT_RE = re.compile(r"\[JUDGE_REJECT\] (?P<sym>\S+) (?P<route>\S+) (?P<dir>LONG|SHORT): (?P<reason>.*)$")
# signal_watcher.py: [signal_watcher] SYM 注册监控 mode=xxx
REG_RE = re.compile(r"\[signal_watcher\] (?P<sym>\S+) 注册监控 mode=(?P<mode>\S+)")
# trigger_detector.py: [trigger-dispatch] SYM strategy → result
DISPATCH_RE = re.compile(r"\[trigger-dispatch\] (?P<sym>\S+) (?P<st>\S+) → (?P<res>\w+)")

PATH_WINDOW_MIN = 90          # 开仓路径：在评分/注册事件前多少分钟内找来源
EVENT_WINDOW_H = 6            # judge 拒绝历史/评分：开仓前多少小时内


def _num(x, default=None):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _fmt(x, nd=None, suffix=""):
    if x is None or x == "":
        return Q
    if nd is not None:
        v = _num(x)
        return Q if v is None else f"{v:.{nd}f}{suffix}"
    return f"{x}{suffix}"


def _g(x, nd=6):
    v = _num(x)
    return Q if v is None else f"{v:.{nd}g}"


def _utc(ms):
    v = _num(ms)
    return datetime.fromtimestamp(v / 1000, tz=timezone.utc) if v else None


def _hm(dt):
    return dt.strftime("%H:%M") if dt else Q


def _dur(minutes):
    v = _num(minutes)
    if v is None:
        return Q
    h, m = divmod(int(round(v)), 60)
    return f"{h}h {m:02d}m" if h else f"{m}m"


def _dt_iso(s):
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ── 读文件 ───────────────────────────────────────────────────────────────
def read_jsonl(path: Path) -> list:
    rows = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                    if isinstance(o, dict):
                        rows.append(o)
                except Exception:
                    continue
    except OSError:
        pass
    return rows


def read_json(path: Path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None


def log_files(log_dir: Path) -> list:
    """system.log + system.log.1 ~ .5，存在的全部读取"""
    out = []
    for name in ["system.log"] + [f"system.log.{i}" for i in range(1, 6)]:
        p = Path(log_dir) / name
        if p.exists():
            out.append(p)
    return out


def scan_system_logs(log_dir: Path, sym: str, tz_off_h: float = 0.0) -> list:
    """返回含该标的的日志事件 [(utc_dt, level, text)]，按时间升序。逐行流式读取，先做子串预过滤。"""
    ev = []
    off = timedelta(hours=tz_off_h)
    for p in log_files(log_dir):
        try:
            with open(p, encoding="utf-8", errors="replace") as f:
                for line in f:
                    if sym not in line:
                        continue
                    m = LINE_RE.match(line.rstrip("\n"))
                    if not m:
                        continue
                    try:
                        dt = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) - off
                    except ValueError:
                        continue
                    ev.append((dt, m.group(2), m.group(4)))
        except OSError:
            continue
    ev.sort(key=lambda x: x[0])
    return ev


# ── 仓位记录 ─────────────────────────────────────────────────────────────
def load_positions(log_dir: Path) -> list:
    """合并 pos_state(持仓) 与 trade_log(已平仓)，统一为 dict 列表（带 _status）。"""
    out = []
    ps = read_json(Path(log_dir) / "pos_state.json")
    for p in (ps if isinstance(ps, list) else []):
        if isinstance(p, dict) and p.get("symbol") and p.get("status") in ("open", "pending_guard", "patching", None):
            q = dict(p)
            q["_status"] = "open"
            out.append(q)
    for t in read_jsonl(Path(log_dir) / "trade_log.jsonl"):
        if t.get("symbol") and t.get("close_time"):      # 无 close_time 的残缺记录不显示
            q = dict(t)
            q["_status"] = "closed"
            out.append(q)
    return out


def pick(recs, symbol, direction=None, show_all=False):
    symbol = symbol.upper()
    c = [r for r in recs if str(r.get("symbol", "")).upper() == symbol
         and (not direction or str(r.get("direction", "")).upper() == direction.upper())]
    c.sort(key=lambda r: _num(r.get("open_time"), 0), reverse=True)
    return c if show_all else c[:1]


# ── 开仓路径识别 ─────────────────────────────────────────────────────────
def detect_path(sym, ev, scan_rows, trig_rows, anchor, orphan=False):
    """anchor：评分/注册事件时间（找不到则用开仓时间）。返回 (路径文案, 证据)。
    规则：anchor 前 PATH_WINDOW_MIN 分钟内——
      system.log 有该标的 [trigger-dispatch] ... → signal，或 trigger_log 有该标的触发记录 → 触发器派发；
      scan_signals.jsonl 有该标的信号 → 主扫描；
      同时命中时取离 anchor 最近的一条；都没有 → unknown。"""
    if orphan:
        return "orphan（孤儿仓位，非本系统开仓路径）", ""
    if anchor is None:
        return "unknown（可能是孤儿仓位）", ""
    lo = anchor - timedelta(minutes=PATH_WINDOW_MIN)
    hi = anchor + timedelta(minutes=2)
    cands = []
    for dt, _lv, txt in ev:
        if lo <= dt <= hi:
            m = DISPATCH_RE.search(txt)
            if m and m.group("sym") == sym and m.group("res") == "signal":
                cands.append((dt, "触发器派发", f"trigger-dispatch {m.group('st')}"))
    for r in trig_rows:
        if r.get("symbol") == sym and r.get("type") not in ("analysis", "post_price"):
            d = _utc(r.get("ts"))
            if d and lo <= d <= hi:
                cands.append((d, "触发器派发", "trigger_log"))
    for r in scan_rows:
        if r.get("symbol") == sym:
            d = _utc(r.get("ts"))
            if d and lo <= d <= hi:
                cands.append((d, "主扫描", "scan_signals"))
    if not cands:
        return "unknown（可能是孤儿仓位）", ""
    cands.sort(key=lambda c: abs((anchor - c[0]).total_seconds()))
    return cands[0][1], cands[0][2]


# ── 复盘 ─────────────────────────────────────────────────────────────────
def funding_and_price(sym, timeout=5.0):
    """可选网络：返回 (funding_rate, mark_price)；失败/超时 → (None, None)。"""
    res = {}

    def run():
        try:
            sys.path.insert(0, str(FX_ROOT))
            from shared import binance_api as api
            mp = api.get_mark_price(sym) or {}
            res["fr"] = _num(mp.get("lastFundingRate"))
            res["px"] = _num(mp.get("markPrice"))
        except Exception:
            pass
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout)
    return res.get("fr"), res.get("px")


def _cfg(name, default=None):
    try:
        sys.path.insert(0, str(FX_ROOT))
        import config
        return getattr(config, name, default)
    except Exception:
        return default


def review(rec, log_dir, tz_off_h=0.0, net=True, now=None) -> str:
    log_dir = Path(log_dir)
    now = now or datetime.now(timezone.utc)
    sym = str(rec.get("symbol", "")).upper()
    direction = str(rec.get("direction", "")).upper() or Q
    d = rec.get("judge_detail") if isinstance(rec.get("judge_detail"), dict) else {}
    open_dt = _utc(rec.get("open_time"))
    mode = rec.get("mode") or rec.get("sig_type") or Q
    orphan = mode == "recovered" or rec.get("source", "") == "orphan_recovered"
    pid = rec.get("pos_id") or Q
    closed = rec["_status"] == "closed"
    ev = scan_system_logs(log_dir, sym, tz_off_h)

    lo = (open_dt - timedelta(hours=EVENT_WINDOW_H)) if open_dt else None
    hi = (open_dt + timedelta(minutes=2)) if open_dt else None

    def inwin(dt):
        return lo is None or lo <= dt <= hi

    score = base_only = reg = None
    regs, rejects = [], []
    for dt, _lv, txt in ev:
        if not inwin(dt):
            continue
        m = SCORE_RE.search(txt)
        if m and m.group("sym") == sym:
            score = (dt, m)
        m = BASE_RE.search(txt)
        if m and m.group("sym") == sym:
            base_only = (dt, m)
        m = REG_RE.search(txt)
        if m and m.group("sym") == sym:
            regs.append(dt)
            reg = dt
        m = REJECT_RE.search(txt)
        if m and m.group("sym") == sym and (direction == Q or m.group("dir") == direction):
            rejects.append((dt, m.group("reason").strip()))
    anchor = (score[0] if score else None) or reg or open_dt
    scan_rows = read_jsonl(log_dir / "scan_signals.jsonl")
    trig_rows = read_jsonl(log_dir / "trigger_log.jsonl")
    path, evid = detect_path(sym, ev, scan_rows, trig_rows, anchor, orphan)

    L = []
    L.append(f"═══ {sym} {direction} 开仓复盘 " + "═" * 24)
    L.append("")
    L.append("【基本信息】")
    L.append(f"开仓时间:   {open_dt.strftime('%Y-%m-%d %H:%M') + ' UTC' if open_dt else Q}")
    L.append(f"开仓路径:   {path}" + (f"（{evid}）" if evid else ""))
    L.append(f"策略:       {mode}")
    L.append(f"方向:       {direction}")
    L.append(f"强度:       {_fmt(rec.get('signal_strength') or d.get('signal_strength'))}")
    L.append(f"pos_id:     {pid}" if not closed else f"trade_id:   {_fmt(rec.get('trade_id'))}")
    L.append("")

    L.append("【评分拆解】（从 system.log 解析）")
    if score:
        m = score[1]
        L += [f"base_score:    {m.group('base')}", f"btc_adj:       {m.group('btc')}",
              f"strategy_adj:  {m.group('strat')}", f"tier_adj:      {m.group('tier')}",
              f"hub_bonus:     {m.group('hub')}", f"cg_bonus:      {m.group('cg')}",
              f"shock_adj:     {m.group('shock')}   pulse: {m.group('pulse')}",
              f"最终得分:      {m.group('final')}"]
    elif base_only:
        m = base_only[1]
        L += [f"base_score:    {m.group('base')}", f"btc_adj:       {m.group('btc')}",
              "其余分项:      ?（未找到 score 汇总行；仅有 base 行）"]
    else:
        L.append("未在 system.log 中找到该标的评分行（日志已轮转出范围，或为孤儿仓位）  →  ?")
    L.append("")

    L.append("【judge 通过条件】（取自开仓时 judge_detail；缺失字段显示 ?）")
    e20, e50 = _num(d.get("ema20_4h")), _num(d.get("ema50_4h"))
    gap = (e20 - e50) / e50 * 100 if e20 is not None and e50 else None
    L.append(f"EMA排列(4H): {'✅' if gap is not None else ''} ema_gap={_fmt(gap, 2, '%')}")
    pb = d.get("pb", d.get("pullback_pct", d.get("pullback_depth")))
    L.append(f"回踩深度:   {_fmt(pb, 2, '%')}")
    L.append(f"RSI 1H:    {_fmt(d.get('rsi_1h'), 1)}")
    L.append(f"RSI 4H:    {_fmt(d.get('rsi_4h'), 1)}")
    L.append(f"量比:       {_fmt(d.get('vol_ratio', d.get('volume_ratio')), 2)}")
    L.append(f"chg24:     {_fmt(d.get('chg24'), 1, '%')}")
    L.append("")

    L.append("【历史 judge 拒绝记录】")
    ev_lines = [(dt, f"{_hm(dt)} {r}") for dt, r in rejects]
    for rg in regs:
        ev_lines.append((rg, f"{_hm(rg)} ✅ 通过（注册监控）"))
    if ev_lines:
        for _dt, s in sorted(ev_lines, key=lambda x: x[0])[-12:]:
            L.append(s)
    else:
        L.append("无（窗口内未找到 JUDGE_REJECT/注册记录）")
    L.append("")

    entry = _num(rec.get("entry_price"))
    sl, tp1 = _num(rec.get("sl_price")), _num(rec.get("tp1_price"))
    close_dt = _utc(rec.get("close_time")) if closed else None
    adj = []
    for a in read_jsonl(log_dir / "tp_sl_adjust_log.jsonl"):
        if pid != Q and a.get("pos_id") == pid:
            adj.append(a)
            continue
        t = _dt_iso(a.get("ts"))
        if (a.get("symbol") == sym and str(a.get("direction", "")).upper() == direction and open_dt and t
                and t >= open_dt and (close_dt is None or t <= close_dt)):
            adj.append(a)
    if (sl is None or tp1 is None) and adj:      # 已平仓记录没有 sl/tp，用调整日志最后一次补
        last = adj[-1]
        sl = sl if sl is not None else _num(last.get("new_sl"))
        tp1 = tp1 if tp1 is not None else _num(last.get("new_tp1"))
    L.append("【止盈止损】")
    L.append(f"入场价:   {_g(entry)}")
    sld = abs(entry - sl) / entry * 100 if entry and sl else None
    L.append(f"止损:     {_g(sl)}（距离{_fmt(sld, 1, '%')}）")
    rr = abs(tp1 - entry) / abs(entry - sl) if entry and sl and tp1 and entry != sl else None
    L.append(f"TP1:      {_g(tp1)}（RR={_fmt(rr, 2, 'x')}）")
    if rec.get("tp2_price"):
        L.append(f"TP2:      存在 {_g(rec.get('tp2_price'))}")
    else:
        L.append("TP2:      无" if "tp2_price" in rec else "TP2:      ?")
    L.append("")

    qty, lev = _num(rec.get("quantity")), _num(rec.get("leverage"))
    notional = qty * entry if qty and entry else None
    L.append("【仓位计算】")
    L.append("余额:      ?（本地无开仓时余额记录）")
    L.append(f"风险比例:  {_fmt(_cfg('RISK_PER_TRADE'), 3)}（当前 config，非开仓时快照）")
    L.append(f"SL距离:   {_fmt(sld, 1, '%')}")
    L.append(f"仓位系数:  {_fmt(rec.get('position_size_cap'), 2)}")
    L.append(f"名义仓位:  {_fmt(notional, 0, 'U')}（杠杆{_fmt(lev, 0, 'x')}）")
    L.append("")

    fr = px = None
    if net and not closed:
        fr, px = funding_and_price(sym)
    L.append("【资金费率】")
    if fr is None:
        L.append("当前费率:  ?（未查询或查询失败）")
    elif fr == 0:
        L.append("当前费率:  0（无资金费）")
    else:
        # 正费率：多头付空头；负费率：空头付多头
        pays = (direction == "LONG" and fr > 0) or (direction == "SHORT" and fr < 0)
        L.append(f"当前费率:  {fr * 100:+.4f}%（{direction}{'付钱，不利' if pays else '收钱，有利'}）")
    L.append("")

    L.append("【动态调整历史】（tp_sl_adjust_log.jsonl）")
    if adj:
        for a in adj[-10:]:
            parts = []
            for k, lab in (("sl", "SL"), ("tp1", "TP1")):
                o, n = _num(a.get(f"old_{k}")), _num(a.get(f"new_{k}"))
                if o is not None and n is not None:
                    parts.append(f"{lab} {_g(o)}→{_g(n)}" + ("（无变化）" if o == n else ""))
            L.append(f"{_hm(_dt_iso(a.get('ts')))} " + (" | ".join(parts) or Q) + f"  [{a.get('action', Q)}]")
    else:
        L.append("无记录")
    L.append("")

    L.append("【当前状态 / 平仓记录】")
    if closed:
        L.append("状态:      已平仓")
        L.append(f"平仓时间:  {close_dt.strftime('%Y-%m-%d %H:%M') + ' UTC' if close_dt else Q}")
        L.append(f"平仓原因:  {_fmt(rec.get('close_reason'))}")
        L.append(f"最终盈亏:  {_fmt(rec.get('pnl_usdt'), 2, 'U')}（{_fmt(rec.get('pnl_pct'), 2, '%')}）  持仓 {_dur(rec.get('holding_minutes'))}")
    else:
        hold = (now - open_dt).total_seconds() / 60 if open_dt else None
        L.append("状态:      持仓中")
        if px and entry:
            pnl_pct = (px - entry) / entry * 100 * (1 if direction == "LONG" else -1)
            pnl_u = pnl_pct / 100 * notional if notional else None
            L.append(f"浮动盈亏:  {pnl_pct:+.2f}%" + (f"（约 {pnl_u:+.2f}U）" if pnl_u is not None else "") + f"  现价 {_g(px)}")
        else:
            L.append("浮动盈亏:  ?（未查询现价）")
        L.append(f"持仓时长:  {_dur(hold)}")
        soft = 12.0 if orphan else _num(_cfg("PM_MAX_HOLD_HOURS", 8))
        hard = _num(_cfg("PM_HARD_MAX_HOLD_HOURS", 16))
        if hold is not None and soft:
            tail = f"；硬限({hard:g}H)还剩 {_dur(max(0, hard * 60 - hold))}" if hard and not orphan else ""
            L.append(f"时间止损:  距软限({soft:g}H)还剩 {_dur(max(0, soft * 60 - hold))}{tail}"
                     "（通用上限估算；reversal/ema_trend 另有独立上限，以实盘代码为准）")
    return "\n".join(L)


# ── 列表模式 ─────────────────────────────────────────────────────────────
def _row_path(rec, log_dir, tz_off_h, cache):
    sym = str(rec.get("symbol", "")).upper()
    if (rec.get("mode") or rec.get("sig_type")) == "recovered":
        return "孤儿"
    if "scan" not in cache:
        cache["scan"] = read_jsonl(Path(log_dir) / "scan_signals.jsonl")
        cache["trig"] = read_jsonl(Path(log_dir) / "trigger_log.jsonl")
    if sym not in cache:
        cache[sym] = scan_system_logs(log_dir, sym, tz_off_h)
    p, _ = detect_path(sym, cache[sym], cache["scan"], cache["trig"], _utc(rec.get("open_time")), False)
    return "unknown" if p.startswith("unknown") else p


def list_today(recs, log_dir, tz_off_h=0.0, now=None) -> str:
    now = now or datetime.now(timezone.utc)
    day = now.date()
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    rows = [r for r in recs if (_utc(r.get("open_time")) or epoch).date() == day]
    rows.sort(key=lambda r: _num(r.get("open_time"), 0))
    out = [f"# --today  (UTC {day})",
           f"{'时间':<6} {'标的':<14} {'方向':<6} {'策略':<14} {'评分':<5} {'强度':<7} {'入场价':<10} {'止损':<10} {'TP1':<10} 路径"]
    cache = {}
    for r in rows:
        sym = str(r.get("symbol", "")).upper()
        if sym not in cache:
            cache[sym] = scan_system_logs(log_dir, sym, tz_off_h)
        od = _utc(r.get("open_time"))
        sc = Q
        for dt, _lv, txt in reversed(cache[sym]):
            m = SCORE_RE.search(txt)
            if m and m.group("sym") == sym and od and od - timedelta(hours=EVENT_WINDOW_H) <= dt <= od + timedelta(minutes=2):
                sc = m.group("final")
                break
        out.append(f"{_hm(od):<6} {sym:<14} {str(r.get('direction', Q)).upper():<6} "
                   f"{str(r.get('mode') or r.get('sig_type') or Q):<14} {sc:<5} {str(r.get('signal_strength') or Q):<7} "
                   f"{_g(r.get('entry_price')):<10} {_g(r.get('sl_price')):<10} {_g(r.get('tp1_price')):<10} "
                   f"{_row_path(r, log_dir, tz_off_h, cache)}")
    if not rows:
        out.append("（今日无开仓记录）")
    return "\n".join(out)


def list_closed(recs, days=7, now=None) -> str:
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=days)
    rows = [r for r in recs if r["_status"] == "closed" and (_utc(r.get("close_time")) or since) >= since]
    rows.sort(key=lambda r: _num(r.get("close_time"), 0), reverse=True)
    out = [f"# --closed  (最近{days}天)",
           f"{'日期时间':<12} {'标的':<14} {'方向':<6} {'策略':<14} {'平仓原因':<16} {'盈亏':<10} 持仓时长"]
    total = 0.0
    seen = set()
    shown = 0
    for r in rows:
        tid = r.get("trade_id") or f"{r.get('symbol')}_{r.get('close_time')}"
        if tid in seen:
            continue
        seen.add(tid)
        shown += 1
        cdt = _utc(r.get("close_time"))
        pnl = _num(r.get("pnl_usdt"))
        total += pnl or 0.0
        out.append(f"{(cdt.strftime('%m-%d %H:%M') if cdt else Q):<12} {str(r.get('symbol', Q)):<14} "
                   f"{str(r.get('direction', Q)).upper():<6} {str(r.get('mode') or Q):<14} {str(r.get('close_reason') or Q):<16} "
                   f"{(f'{pnl:+.2f}U' if pnl is not None else Q):<10} {_dur(r.get('holding_minutes'))}")
    out.append(f"合计 {shown} 笔，盈亏 {total:+.2f}U" if shown else "（无平仓记录）")
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="单笔仓位开仓复盘（只读）")
    ap.add_argument("symbol", nargs="?")
    ap.add_argument("--direction", choices=["long", "short", "LONG", "SHORT"])
    ap.add_argument("--all", action="store_true", help="同一标的显示全部历史")
    ap.add_argument("--today", action="store_true")
    ap.add_argument("--closed", action="store_true")
    ap.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    ap.add_argument("--log-tz-offset", type=float, default=_local_tz_offset,
                    help=f"system.log 本地时间相对 UTC 的小时偏移（自动检测为 {_local_tz_offset:g}）")
    ap.add_argument("--no-funding", action="store_true")
    a = ap.parse_args(argv)
    recs = load_positions(Path(a.log_dir))
    if a.today:
        print(list_today(recs, a.log_dir, a.log_tz_offset))
        return 0
    if a.closed:
        print(list_closed(recs))
        return 0
    if not a.symbol:
        ap.print_help()
        return 2
    hit = pick(recs, a.symbol, a.direction, a.all)
    if not hit:
        print(f"未找到 {a.symbol.upper()} 的持仓/平仓记录（pos_state.json 与 trade_log.jsonl 均无）。")
        return 1
    print("\n\n".join(review(r, a.log_dir, a.log_tz_offset, net=not a.no_funding) for r in hit))
    return 0


if __name__ == "__main__":
    sys.exit(main())
