#!/usr/bin/env python3
"""tools/sync_trades.py — 从交易所补全 trade_log.jsonl 里缺失的平仓记录。

用法：python3 tools/sync_trades.py [--days 7] [--dry-run] [--root <fx目录>]
数据：GET /fapi/v1/userTrades（成交，含 realizedPnl/commission）+ GET /fapi/v1/income（REALIZED_PNL，用来发现本地完全没有的标的）。

原则（本地主导，交易所只用于发现和修复缺失）：
  · 本地已有的记录：只在 pnl_usdt 或 exit_price 为空/0 时，用交易所值补上；其余字段（mode/close_reason/signal_strength…）一律不动；
  · 交易所有、本地没有的平仓：补一条 source="exchange_sync" 的记录（mode/close_reason 记为 unknown/exchange_sync）；
  · 「同一笔」判定见 shared/trade_dedup：trade_id 优先，其次同标的+同方向+close_time 相差 ≤5 秒，
    或交易所这一笔的平仓时间落在本地记录的 [open_time, close_time+5s] 区间内；
  · 不删除、不合并本地已有的重复行；写回前先备份，再 临时文件 + os.replace 原子替换。
限制：open_time 只能用同方向开仓成交的最早时间近似；窗口开始前就已开的仓位，open_time 未知（记为 0）。
      同步与 bot 是两个进程，进程内锁互不可见：写回前会校验文件未变化，变化则重算（最多 3 次），仍有极小竞态窗口。
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.trade_dedup import TOL_MS, same_trade, _f  # noqa: E402

DAY_MS = 86400 * 1000
EPS = 1e-9
BACKUP_KEEP = 5


# ── 成交 → 往返仓位 ────────────────────────────────────────────────────────

def _is_open_fill(f: dict, pos_side: str) -> bool:
    side = str(f.get("side", "")).upper()
    return side == "BUY" if pos_side == "LONG" else side == "SELL"


def _finalize(sym: str, pos_side: str, cur: dict) -> dict:
    opens, closes = cur["opens"], cur["closes"]
    cq = sum(_f(x.get("qty")) for x in closes)
    oq = sum(_f(x.get("qty")) for x in opens)
    exit_px = sum(_f(x.get("price")) * _f(x.get("qty")) for x in closes) / cq if cq > 0 else 0.0
    entry_px = sum(_f(x.get("price")) * _f(x.get("qty")) for x in opens) / oq if oq > 0 else 0.0
    fees = sum(abs(_f(x.get("commission"))) for x in opens + closes
               if str(x.get("commissionAsset", "USDT")).upper() in ("USDT", "USDC", "BUSD"))
    return {
        "symbol": sym, "direction": pos_side,
        "open_time": int(min((_f(x.get("time")) for x in opens), default=0)),
        "close_time": int(max(_f(x.get("time")) for x in closes)),
        "entry_price": entry_px, "exit_price": exit_px, "quantity": cq,
        "pnl_usdt": sum(_f(x.get("realizedPnl")) for x in closes),
        "fees_usdt": fees,
        "partial_window": not opens,       # 开仓发生在查询窗口之前，open_time 未知
    }


def build_trips(symbol: str, fills: list, since_ms: int = 0) -> tuple:
    """成交流水 → 已平仓的往返仓位列表。返回 (trips, skipped_both)。
    只处理双向持仓（positionSide=LONG/SHORT）；BOTH（单向模式）的成交计入 skipped_both。"""
    trips, skipped = [], 0
    groups: dict = {"LONG": [], "SHORT": []}
    for f in sorted(fills, key=lambda x: (_f(x.get("time")), _f(x.get("id")))):
        ps = str(f.get("positionSide", "")).upper()
        if ps in groups:
            groups[ps].append(f)
        else:
            skipped += 1
    for ps, fs in groups.items():
        qty, cur = 0.0, None
        for f in fs:
            q = _f(f.get("qty"))
            if cur is None:
                cur = {"opens": [], "closes": []}
            if _is_open_fill(f, ps):
                if cur["closes"] and not cur["opens"]:      # 窗口前开的仓位的收尾平仓，先结算
                    trips.append(_finalize(symbol, ps, cur))
                    cur, qty = {"opens": [], "closes": []}, 0.0
                cur["opens"].append(f)
                qty += q
            else:
                cur["closes"].append(f)
                qty = max(qty - q, 0.0) if cur["opens"] else 0.0
                if cur["opens"] and qty <= EPS:
                    trips.append(_finalize(symbol, ps, cur))
                    cur, qty = None, 0.0
        if cur and cur["closes"] and not cur["opens"]:
            trips.append(_finalize(symbol, ps, cur))
        # cur 有开仓且 qty>0：仍在持仓中，不是平仓记录，丢弃
    return [t for t in trips if t["close_time"] >= since_ms], skipped


# ── 合并 ────────────────────────────────────────────────────────────────────

def _trip_record(t: dict) -> dict:
    cm = t["close_time"]
    hold = round((cm - t["open_time"]) / 60000, 1) if t["open_time"] else 0.0
    return {
        "trade_id": f"{cm}_{t['symbol']}", "symbol": t["symbol"], "direction": t["direction"],
        "mode": "unknown", "entry_price": t["entry_price"], "exit_price": round(t["exit_price"], 8),
        "quantity": t["quantity"], "pnl_usdt": round(t["pnl_usdt"], 4),
        "fees_usdt": round(t["fees_usdt"], 6), "pnl_net_usdt": round(t["pnl_usdt"] - t["fees_usdt"], 4),
        "holding_minutes": hold, "open_time": t["open_time"], "close_time": cm,
        "close_reason": "exchange_sync", "source": "exchange_sync",
    }


def _match(trip_rec: dict, t: dict, local: list, tol_ms: int):
    """在本地记录里找这一笔；返回行下标或 None。"""
    for i, r in enumerate(local):
        if r is None or r.get("symbol") != t["symbol"]:
            continue
        if same_trade(r, trip_rec, tol_ms):
            return i
        d = str(r.get("direction") or "").upper()
        if d and d != t["direction"]:
            continue
        o, c = _f(r.get("open_time")), _f(r.get("close_time"))
        if o > 0 and c > 0 and o - tol_ms <= t["close_time"] <= c + tol_ms:
            return i
    return None


def merge(raw_lines: list, trips: list, live_keys: set, tol_ms: int = TOL_MS) -> dict:
    """纯函数：raw_lines 为原始文本行，返回 {lines, appended, filled, matched, skipped_live}。
    未改动的行原样保留（含无法解析的行）。"""
    parsed = []
    for ln in raw_lines:
        try:
            r = json.loads(ln)
            parsed.append(r if isinstance(r, dict) else None)
        except Exception:
            parsed.append(None)
    lines = [ln.rstrip("\n") for ln in raw_lines]
    appended, filled, matched, skipped_live = [], [], 0, []
    extra: list = []
    for t in sorted(trips, key=lambda x: x["close_time"]):
        rec = _trip_record(t)
        idx = _match(rec, t, parsed + extra, tol_ms)
        if idx is not None:
            matched += 1
            if idx >= len(parsed):
                continue                                  # 已被本次新增的记录覆盖
            r = parsed[idx]
            ch = []
            if not _f(r.get("pnl_usdt")) and _f(t["pnl_usdt"]):
                r["pnl_usdt"] = rec["pnl_usdt"]
                if t["fees_usdt"] and not _f(r.get("fees_usdt")):
                    r["fees_usdt"] = rec["fees_usdt"]
                    r["pnl_net_usdt"] = rec["pnl_net_usdt"]
                ch.append("pnl_usdt")
            if not _f(r.get("exit_price")) and t["exit_price"] > 0:
                r["exit_price"] = rec["exit_price"]
                ch.append("exit_price")
            if ch:
                r["exchange_filled"] = ch
                lines[idx] = json.dumps(r, ensure_ascii=False)
                filled.append((rec["trade_id"], ch))
            continue
        if t["partial_window"] and (t["symbol"], t["direction"]) in live_keys:
            skipped_live.append(rec["trade_id"])         # 仓位还在，这只是部分平仓
            continue
        extra.append(rec)
        appended.append(rec)
    lines += [json.dumps(r, ensure_ascii=False) for r in appended]
    return {"lines": lines, "appended": appended, "filled": filled, "matched": matched,
            "skipped_live": skipped_live}


# ── 本地文件 ────────────────────────────────────────────────────────────────

def _read_raw(path: Path) -> list:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.readlines()


def _local_symbols(raw: list, since_ms: int) -> set:
    out = set()
    for ln in raw:
        try:
            r = json.loads(ln)
        except Exception:
            continue
        if isinstance(r, dict) and r.get("symbol") and (
                _f(r.get("close_time")) >= since_ms or _f(r.get("open_time")) >= since_ms):
            out.add(r["symbol"])
    return out


def _pos_state(root: Path) -> list:
    try:
        d = json.loads((root / "logs" / "pos_state.json").read_text(encoding="utf-8"))
        return d if isinstance(d, list) else list(d.values()) if isinstance(d, dict) else []
    except Exception:
        return []


def _backup(path: Path, now_ms: int) -> Path | None:
    if not path.exists():
        return None
    stamp = datetime.fromtimestamp(now_ms / 1000).strftime("%Y%m%d%H%M%S")
    bak = path.with_name(f"{path.name}.bak.{stamp}")
    bak.write_bytes(path.read_bytes())
    old = sorted(path.parent.glob(f"{path.name}.bak.*"))
    for p in old[:-BACKUP_KEEP]:
        try:
            p.unlink()
        except OSError:
            pass
    return bak


def _atomic_write(path: Path, lines: list) -> None:
    tmp = path.with_name(path.name + ".sync.tmp")
    tmp.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    os.replace(tmp, path)


# ── 主流程 ──────────────────────────────────────────────────────────────────

def sync(root: Path, days: int = 7, dry_run: bool = False, now_ms: int | None = None,
         fetch_trades=None, fetch_income=None, out=print) -> dict:
    """fetch_trades(symbol, start_ms, end_ms)->list；fetch_income(start_ms, end_ms)->list。默认走 shared.binance_api。"""
    root = Path(root)
    now_ms = int(now_ms or time.time() * 1000)
    since = now_ms - int(days) * DAY_MS
    log = root / "logs" / "trade_log.jsonl"
    if fetch_trades is None or fetch_income is None:
        from shared import binance_api as api
        fetch_trades = fetch_trades or (lambda s, a, b: api.get_user_trades_range(s, a, b))
        fetch_income = fetch_income or (lambda a, b: api.get_income("REALIZED_PNL", start_time=a, end_time=b))

    raw = _read_raw(log)
    pos = _pos_state(root)
    live_keys = {(p.get("symbol"), str(p.get("direction", "")).upper()) for p in pos
                 if p.get("status") in ("open", "pending_guard", "patching")}
    symbols = _local_symbols(raw, since) | {p.get("symbol") for p in pos if p.get("symbol")}
    income = fetch_income(since, now_ms) or []
    inc_by_sym: dict = {}
    for r in income:
        if r.get("symbol"):
            inc_by_sym[r["symbol"]] = inc_by_sym.get(r["symbol"], 0.0) + _f(r.get("income"))
    discovered = set(inc_by_sym) - symbols
    symbols |= set(inc_by_sym)

    trips, skipped_both, failed = [], 0, []
    for s in sorted(x for x in symbols if x):
        try:
            fills = fetch_trades(s, since, now_ms)
        except Exception as e:          # 单个标的失败不影响其他；失败的标的不会被当作「没有成交」
            failed.append((s, str(e)[:80]))
            continue
        t, sk = build_trips(s, fills or [], since)
        trips += t
        skipped_both += sk

    res = None
    for _ in range(3):
        raw = _read_raw(log)
        sig = (log.stat().st_size, log.stat().st_mtime_ns) if log.exists() else None
        res = merge(raw, trips, live_keys)
        changed = bool(res["appended"] or res["filled"])
        if dry_run or not changed:
            break
        bak = _backup(log, now_ms)
        log.parent.mkdir(parents=True, exist_ok=True)
        sig2 = (log.stat().st_size, log.stat().st_mtime_ns) if log.exists() else None
        if sig2 != sig:                 # 读之后文件被 bot 追加了：重算
            continue
        _atomic_write(log, res["lines"])
        res["backup"] = str(bak) if bak else None
        break
    else:
        out("⚠️ trade_log 在同步期间持续被改写，已放弃写回，请稍后重试")
        res["aborted"] = True

    # 与 income 对账（仅提示）
    ex_pnl: dict = {}
    for t in trips:
        ex_pnl[t["symbol"]] = ex_pnl.get(t["symbol"], 0.0) + t["pnl_usdt"]
    mism = [(s, inc_by_sym[s], ex_pnl.get(s, 0.0)) for s in inc_by_sym
            if abs(inc_by_sym[s] - ex_pnl.get(s, 0.0)) > 0.05]

    res.update({"trips": trips, "failed": failed, "discovered": sorted(discovered),
                "skipped_both": skipped_both, "mismatch": mism, "dry_run": dry_run, "days": days})
    _report(res, out)
    return res


def _fmt_t(ms) -> str:
    return datetime.fromtimestamp(_f(ms) / 1000).strftime("%m-%d %H:%M:%S") if _f(ms) else "?"


def _report(res: dict, out) -> None:
    out(f"═══ 交易所同步（最近{res['days']}天{'，试运行不写入' if res['dry_run'] else ''}）")
    out(f"交易所平仓 {len(res['trips'])} 笔 | 本地已有 {res['matched']} 笔 | 补全字段 {len(res['filled'])} 笔 | "
        f"新增 {len(res['appended'])} 笔")
    for r in res["appended"]:
        out(f"  + {_fmt_t(r['close_time'])} {r['symbol']:<10} {r['direction']:<5} entry={r['entry_price']:.6g} "
            f"exit={r['exit_price']:.6g} pnl={r['pnl_usdt']:+.2f}U"
            + ("  (开仓时间未知)" if not r["open_time"] else ""))
    for tid, ch in res["filled"]:
        out(f"  ~ {tid} 补全 {','.join(ch)}")
    if res.get("backup"):
        out(f"备份: {res['backup']}")
    if res["discovered"]:
        out(f"从 income 新发现的标的: {', '.join(res['discovered'])}")
    if res["skipped_live"]:
        out(f"跳过 {len(res['skipped_live'])} 笔窗口前开仓、仍在持仓中的部分平仓")
    if res["skipped_both"]:
        out(f"⚠️ 跳过 {res['skipped_both']} 条 positionSide=BOTH 的成交（单向持仓模式不支持）")
    for s, e in res["failed"]:
        out(f"⚠️ {s} 拉取失败，本次未处理: {e}")
    for s, inc, ex in res["mismatch"]:
        out(f"⚠️ {s} 对账差异: income REALIZED_PNL={inc:+.2f} vs 还原平仓合计={ex:+.2f}（可能有窗口前开仓或窗口边界成交）")


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    root, days = ROOT, 7
    if "--root" in argv:
        root = Path(argv[argv.index("--root") + 1])
    if "--days" in argv:
        days = max(1, min(int(argv[argv.index("--days") + 1]), 7))
    try:
        sync(root, days=days, dry_run="--dry-run" in argv)
    except Exception as e:
        print(f"❌ 同步失败: {type(e).__name__}: {str(e)[:200]}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
