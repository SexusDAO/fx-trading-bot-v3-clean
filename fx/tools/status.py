#!/usr/bin/env python3
"""tools/status.py — 一键查询系统状态（只读本地文件，不连接交易所）。

用法：python3 tools/status.py [--sync] [--root <fx目录>]
  --sync  先调 tools/sync_trades.py 从交易所补全本地缺失的平仓记录（此时才会联网），再显示；不加则只读本地文件
数据来源：
  · logs/pos_state.json          持仓
  · logs/trade_log.jsonl         今日平仓（UTC 日期过滤；trade_id 优先 + 同标的同方向 close_time 相差 ≤5 秒视为同一笔）
  · logs/system.log              最后 2000 行：事件循环阻塞次数、最近一轮 scan-perf / liq-stats
  · daemon_start.py status 输出   daemon 是否运行 + PID（uptime 用 ps 取）
说明：持仓盈亏需要实时价格，本脚本不联网，pnl 一律显示 ?。
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_ROOT = Path(__file__).resolve().parent.parent
if str(DEFAULT_ROOT) not in sys.path:
    sys.path.insert(0, str(DEFAULT_ROOT))
from shared.trade_dedup import same_trade  # noqa: E402  纯函数，不联网
TAIL_LINES = 2000
LIVE_STATUSES = ("open", "pending_guard", "patching")
_LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def bar(title: str, width: int = 36) -> str:
    return f"═══ {title} " + "═" * max(width - len(title) * 2, 4)


# ── 读取 ───────────────────────────────────────────────────────────────────
def read_json(path: Path, default):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return default


def read_jsonl(path: Path) -> list:
    out = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    if isinstance(d, dict):
                        out.append(d)
                except Exception:
                    continue
    except Exception:
        pass
    return out


def tail_lines(path: Path, n: int = TAIL_LINES) -> list:
    """读文件最后 n 行（按块从尾部读，不整文件加载）。"""
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            block, data = 65536, b""
            while size > 0 and data.count(b"\n") <= n:
                step = min(block, size)
                size -= step
                f.seek(size)
                data = f.read(step) + data
        return data.decode("utf-8", errors="replace").splitlines()[-n:]
    except Exception:
        return []


# ── 持仓 ───────────────────────────────────────────────────────────────────
def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def live_positions(pos_state) -> list:
    return [p for p in (pos_state if isinstance(pos_state, list) else []) if p.get("status") in LIVE_STATUSES]


def has_sl(p: dict) -> bool:
    return _num(p.get("sl_price")) > 0 and bool(p.get("sl_order_id"))


def fmt_px(v) -> str:
    x = _num(v)
    return f"{x:.6g}" if x else "0"


def position_line(p: dict) -> str:
    sl_ok = has_sl(p)
    tp_ok = bool(p.get("tp1_order_id"))
    tp1 = fmt_px(p.get("tp1_price")) if _num(p.get("tp1_price")) else "?"
    guard = f"{'✅SL' if sl_ok else '❌无SL'} {'✅TP' if tp_ok else '❌无TP'}"
    return (f"{str(p.get('symbol', '?')):<10} {str(p.get('direction', '?')).upper():<5} "
            f"{str(p.get('mode') or p.get('sig_type') or '?'):<10} entry={fmt_px(p.get('entry_price')):<9} "
            f"sl={fmt_px(p.get('sl_price')):<9} tp1={tp1:<9} pnl=?  {guard}  status={p.get('status', '?')}")


# ── 今日平仓 ───────────────────────────────────────────────────────────────
def _utc_date(ms) -> str | None:
    try:
        return datetime.fromtimestamp(float(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def today_closes(records: list, today_utc: str) -> tuple:
    """返回 (去重后的今日记录, 重复条数)。同一笔 = trade_id 相同，或同标的同方向 close_time 相差 ≤5 秒。"""
    out, dups = [], 0
    by_sym: dict = {}
    for r in records:
        if _utc_date(r.get("close_time")) != today_utc:
            continue
        if any(same_trade(r, k) for k in by_sym.get(r.get("symbol"), ())):
            dups += 1
            continue
        by_sym.setdefault(r.get("symbol"), []).append(r)
        out.append(r)
    out.sort(key=lambda r: _num(r.get("close_time")))
    return out, dups


def close_line(r: dict) -> str:
    t = datetime.fromtimestamp(_num(r.get("close_time")) / 1000, tz=timezone.utc).strftime("%H:%M")
    pnl = r.get("pnl_usdt")
    pnl_s = f"{float(pnl):+.2f}U" if pnl is not None and str(pnl) != "" else "?"
    return (f"{t}  {str(r.get('symbol', '?')):<10} {str(r.get('direction', '?')).upper():<5} "
            f"{str(r.get('mode') or '?'):<10} {str(r.get('close_reason') or '?'):<18} {pnl_s}")


# ── system.log ─────────────────────────────────────────────────────────────
def _parse_ts(line: str):
    m = _LOG_TS.match(line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def count_loop_blocks(lines: list, now: datetime, hours: float = 1.0) -> int:
    """最近 hours 小时内事件循环阻塞/延迟告警条数（日志时间为服务器本地时间，now 须同口径的本地 naive 时间）。
    只在 system.log 最后 2000 行内统计——日志刷得快时，实际覆盖的时间窗可能短于 1 小时。"""
    since = now - timedelta(hours=hours)
    n = 0
    for ln in lines:
        if "[loop]" not in ln and "[watchdog] 事件循环阻塞" not in ln:
            continue
        ts = _parse_ts(ln)
        if ts is not None and ts >= since:
            n += 1
    return n


_PERF = re.compile(r"候选池加载=([\d.]+)s.*?批量标记价=([\d.]+)s.*?并行分析=([\d.]+)s.*?信号后处理=([\d.]+)s")
_LIQ = re.compile(r"命中(\d+)个/扫描(\d+)个\((\d+)%\)")


def last_scan(lines: list) -> dict:
    """取最后一条 scan-perf 和最后一条 liq-stats。返回 {'time','elapsed','liq_pct','liq_hit','liq_total'}，缺失项为 None。"""
    out = {"time": None, "elapsed": None, "liq_pct": None, "liq_hit": None, "liq_total": None}
    for ln in reversed(lines):
        if out["elapsed"] is None and "[scan-perf]" in ln:
            m = _PERF.search(ln)
            if m:
                out["elapsed"] = sum(float(x) for x in m.groups())
                ts = _parse_ts(ln)
                out["time"] = ts.strftime("%H:%M") if ts else None
        if out["liq_pct"] is None and "[liq-stats]" in ln:
            m = _LIQ.search(ln)
            if m:
                out["liq_hit"], out["liq_total"], out["liq_pct"] = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if out["elapsed"] is not None and out["liq_pct"] is not None:
            break
    return out


# ── daemon ─────────────────────────────────────────────────────────────────
def parse_daemon_status(text: str) -> dict:
    """解析 daemon_start.py status 的输出。"""
    d = {"daemon": None, "pid": None, "main": None}
    for ln in (text or "").splitlines():
        if ln.startswith("daemon"):
            d["daemon"] = "运行中" in ln
            m = re.search(r"PID=(\d+)", ln)
            d["pid"] = int(m.group(1)) if m else None
        elif ln.startswith("main.py"):
            d["main"] = "运行中" in ln
    return d


def run_daemon_status(root: Path) -> str:
    try:
        r = subprocess.run([sys.executable, str(Path(root) / "daemon_start.py"), "status"],
                           capture_output=True, text=True, timeout=15, cwd=str(root))
        return r.stdout
    except Exception:
        return ""


def uptime_str(pid) -> str:
    if not pid:
        return "?"
    try:
        r = subprocess.run(["ps", "-o", "etimes=", "-p", str(pid)], capture_output=True, text=True, timeout=5)
        sec = int(r.stdout.strip())
    except Exception:
        return "?"
    return f"{sec // 3600}h {sec % 3600 // 60}m"


# ── 渲染 ───────────────────────────────────────────────────────────────────
def render(root: Path, now_utc: datetime, now_local: datetime, daemon_text: str, uptime: str) -> str:
    root = Path(root)
    logs = root / "logs"
    pos = live_positions(read_json(logs / "pos_state.json", []))
    closes, dups = today_closes(read_jsonl(logs / "trade_log.jsonl"), now_utc.strftime("%Y-%m-%d"))
    lines = tail_lines(logs / "system.log", TAIL_LINES)
    scan = last_scan(lines)
    blocks = count_loop_blocks(lines, now_local, 1.0) if lines else None
    ds = parse_daemon_status(daemon_text)

    out = [bar("系统状态")]
    if ds["daemon"] is None:
        out.append("daemon: ? （未取到 daemon_start.py status 输出）")
    else:
        out.append(f"daemon: {'✅运行中' if ds['daemon'] else '❌未运行'} | main.py: "
                   f"{'✅' if ds['main'] else '❌'} | uptime: {uptime if ds['daemon'] else '-'}")
    out.append(f"最近事件循环阻塞: {'?' if blocks is None else blocks}次（最近1小时，统计范围 system.log 最后{TAIL_LINES}行）")

    out += ["", bar(f"持仓（{len(pos)}笔）")]
    out += [position_line(p) for p in pos] or ["（无）"]

    out += ["", bar(f"今日平仓（{len(closes)}笔）"), "时间   标的       方向  策略       原因               盈亏"]
    out += [close_line(r) for r in closes] or ["（无）"]
    if closes:
        out.append(f"合计: {sum(_num(r.get('pnl_usdt')) for r in closes):+.2f}U（UTC {now_utc.strftime('%Y-%m-%d')}）")

    out += ["", bar("扫描状态")]
    if scan["elapsed"] is None and scan["liq_pct"] is None:
        out.append("最近一轮: ?（system.log 最后2000行里没有 scan-perf / liq-stats）")
    else:
        liq = f"{scan['liq_pct']}%（{scan['liq_hit']}/{scan['liq_total']}）" if scan["liq_pct"] is not None else "?"
        el = f"{scan['elapsed']:.0f}s" if scan["elapsed"] is not None else "?"
        out.append(f"最近一轮: {scan['time'] or '?'} | 耗时{el} | 清算区命中{liq}")

    alerts = []
    no_sl = [p for p in pos if not has_sl(p)]
    if no_sl:
        pg = sum(1 for p in no_sl if p.get("status") == "pending_guard")
        alerts.append(f"⚠️ {len(no_sl)}个持仓 sl=0 或无止损单id（其中pending_guard {pg}个）: "
                      + ", ".join(f"{p.get('symbol')}({str(p.get('direction', '?')).upper()})" for p in no_sl))
    if dups:
        alerts.append(f"⚠️ trade_log 有重复记录（今日 {dups} 条重复记录，已在上表去重）")
    out += ["", bar("告警")] + (alerts or ["✅ 无告警"])
    return "\n".join(out)


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    root = DEFAULT_ROOT
    if "--root" in argv:
        root = Path(argv[argv.index("--root") + 1])
    if "--sync" in argv:
        try:
            from tools.sync_trades import sync
            sync(root)
        except Exception as e:      # 同步失败不影响状态显示
            print(f"⚠️ 交易所同步失败，以下仅显示本地记录: {type(e).__name__}: {str(e)[:160]}")
        print()
    ds_text = run_daemon_status(root)
    print(render(root, datetime.now(timezone.utc), datetime.now(), ds_text, uptime_str(parse_daemon_status(ds_text)["pid"])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
