#!/usr/bin/env python3
"""
tools/track_outcomes.py — 事后追踪脚本

对 signal_analytics.jsonl 里所有 outcome.checked_at=null
且距离触发时间 > SIGNAL_ANALYTICS_TRACK_AFTER_H 的记录，
拉 K 线填充 outcome 字段，并回写文件。

调用方式：
    python3 tools/track_outcomes.py          # 手动运行
    run()                                     # 从 main.py / archiver.py 调用
"""
import sys, json, time, os, logging
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger("fx.track_outcomes")

# ── 时间止损阈值（按策略类型）──────────────────────────────────────────────────
TIME_STOP_HOURS = {
    "pullback":  8,
    "reversal": 12,
    "breakout":  8,
    "default":  12,
}

def _get_klines(symbol: str, interval: str, start_ms: int, limit: int = 192):
    """拉 Binance 历史 K 线，带超时保护。"""
    try:
        import requests, os as _os
        proxy = _os.environ.get("HTTPS_PROXY") or _os.environ.get("https_proxy")
        proxies = {"https": proxy, "http": proxy} if proxy else None
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval,
                    "startTime": start_ms, "limit": limit},
            timeout=8, proxies=proxies,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.debug(f"[track_outcomes] {symbol} K线拉取失败: {e}")
        return []


def _track_one(rec: dict) -> dict:
    """
    对单条记录填充 outcome 字段并返回。
    仅处理 judge_passed=True 的信号（拒绝信号单独由 track_rejected 处理）。
    """
    out = rec["outcome"]
    ep  = rec.get("entry_params") or {}

    entry  = ep.get("entry_price")
    sl     = ep.get("sl_price")
    tp1    = ep.get("tp1_price")
    tp2    = ep.get("tp2_price")
    mode   = rec.get("mode", "pullback")
    direction = (rec.get("direction") or "LONG").upper()
    ts_ms  = rec["ts"]

    # entry_price 缺失时降级：用快照价格作为参考，标记为未成交
    if not entry or not sl:
        out["filled"]     = False
        out["checked_at"] = int(time.time() * 1000)
        return rec

    entry_buf_pct = 0.003  # ±0.3% 认为成交

    # 拉信号后 24H 的 15M K 线
    klines = _get_klines(rec["symbol"], "15m", ts_ms, limit=96)  # 96根 = 24H
    if not klines:
        out["checked_at"] = int(time.time() * 1000)
        return rec

    # ── Step 1: 判断是否成交 ──────────────────────────────────────────────────
    fill_ts = None
    fill_price = None
    for k in klines:
        lo, hi = float(k[3]), float(k[2])
        bar_ts = int(k[0])
        if direction == "LONG":
            # 做多：价格触及 entry 或更低（限价单逻辑：价格跌到 entry 才成交）
            if lo <= entry * (1 + entry_buf_pct):
                fill_price = entry
                fill_ts    = bar_ts
                break
        else:
            # 做空：价格触及 entry 或更高
            if hi >= entry * (1 - entry_buf_pct):
                fill_price = entry
                fill_ts    = bar_ts
                break

    if fill_ts is None:
        out["filled"]     = False
        out["checked_at"] = int(time.time() * 1000)
        return rec

    out["filled"]     = True
    out["fill_price"] = fill_price
    out["fill_ts"]    = fill_ts

    # ── Step 2: 从成交点开始判断出场 ─────────────────────────────────────────
    time_stop_h   = TIME_STOP_HOURS.get(mode, TIME_STOP_HOURS["default"])
    time_stop_ms  = fill_ts + time_stop_h * 3600 * 1000

    exit_reason  = None
    exit_price   = None
    max_fav_pct  = 0.0
    max_adv_pct  = 0.0
    pnl_pct      = None

    dir_sign = 1 if direction == "LONG" else -1

    for k in klines:
        bar_ts = int(k[0])
        if bar_ts < fill_ts:
            continue
        lo, hi = float(k[3]), float(k[2])

        # 当前浮盈浮亏
        if direction == "LONG":
            fav = (hi - fill_price) / fill_price * 100
            adv = (lo - fill_price) / fill_price * 100
        else:
            fav = (fill_price - lo) / fill_price * 100
            adv = (fill_price - hi) / fill_price * 100

        max_fav_pct = max(max_fav_pct, fav)
        max_adv_pct = min(max_adv_pct, adv)  # 负值

        # 止损检查
        if direction == "LONG" and lo <= sl:
            exit_reason = "sl"
            exit_price  = sl
            break
        if direction == "SHORT" and hi >= sl:
            exit_reason = "sl"
            exit_price  = sl
            break

        # TP2 检查（优先级高于 TP1）
        if tp2:
            if direction == "LONG" and hi >= tp2:
                exit_reason = "tp2"
                exit_price  = tp2
                break
            if direction == "SHORT" and lo <= tp2:
                exit_reason = "tp2"
                exit_price  = tp2
                break

        # TP1 检查
        if tp1:
            if direction == "LONG" and hi >= tp1:
                exit_reason = "tp1"
                exit_price  = tp1
                break
            if direction == "SHORT" and lo <= tp1:
                exit_reason = "tp1"
                exit_price  = tp1
                break

        # 时间止损
        if bar_ts >= time_stop_ms:
            close = float(k[4])
            exit_reason = "time_stop"
            exit_price  = close
            break

    # 如果 K 线跑完都没有出场（不足 24H 数据）
    if exit_reason is None:
        out["checked_at"] = int(time.time() * 1000)
        return rec  # 稍后再查

    pnl_pct = dir_sign * (exit_price - fill_price) / fill_price * 100

    out["exit_reason"]       = exit_reason
    out["exit_price"]        = exit_price
    out["pnl_pct"]           = round(pnl_pct, 4)
    out["max_favorable_pct"] = round(max_fav_pct, 4)
    out["max_adverse_pct"]   = round(max_adv_pct, 4)
    out["checked_at"]        = int(time.time() * 1000)
    return rec


PATCH_WINDOW_MS = 30 * 60 * 1000   # patch 行在判定记录之后多久内算同一条信号


def _has_prices(rec: dict) -> bool:
    ep = rec.get("entry_params") or {}
    return bool(ep.get("entry_price") and ep.get("sl_price"))


def _merge_patches(records: list) -> int:
    """把 entry_params_patch 行里的价格并进对应的通过记录（symbol+mode 相同，patch 在记录之后 PATCH_WINDOW_MS 内、取最近的一条）。
    只补缺：记录自己已有价格则不动。返回合并条数。"""
    patches = [r for r in records if r.get("type") == "entry_params_patch" and isinstance(r.get("entry_params"), dict)]
    if not patches:
        return 0
    n = 0
    for rec in records:
        if not rec.get("judge_passed") or _has_prices(rec):
            continue
        ts = rec.get("ts") or 0
        cands = [p for p in patches
                 if p.get("symbol") == rec.get("symbol") and p.get("mode") == rec.get("mode")
                 and 0 <= (p.get("ts") or 0) - ts <= PATCH_WINDOW_MS]
        if not cands:
            continue
        best = min(cands, key=lambda p: p["ts"] - ts)
        rec["entry_params"] = dict(best["entry_params"])
        rec["entry_params_ok"] = best.get("entry_params_ok", rec.get("entry_params_ok"))
        n += 1
    return n


def _rec_key(rec: dict) -> tuple:
    return (rec.get("ts"), rec.get("symbol"), rec.get("mode"), str(rec.get("direction") or "").upper())


def _write_back(fp: Path, outcomes: dict, entry_params: dict) -> int:
    """把本次算出的 outcome（及合并来的价格）写回 fp：重新读取当前文件内容后按 key 套用，再 临时文件+os.replace 原子替换。
    这样 scanner 在追踪期间追加的新行不会被覆盖；读到替换之间文件又变了则重试（最多 3 次）。返回写回条数。"""
    for _ in range(3):
        st0 = (fp.stat().st_size, fp.stat().st_mtime_ns)
        lines = fp.read_text(encoding="utf-8").splitlines()
        out_lines, applied = [], 0
        for ln in lines:
            try:
                r = json.loads(ln)
            except Exception:
                out_lines.append(ln)
                continue
            if isinstance(r, dict) and r.get("judge_passed"):
                k = _rec_key(r)
                oc = outcomes.get(k)
                if oc is not None and (r.get("outcome") or {}).get("checked_at") is None:
                    r["outcome"] = oc
                    if k in entry_params:
                        r["entry_params"] = entry_params[k]
                    ln = json.dumps(r, ensure_ascii=False)
                    applied += 1
            out_lines.append(ln)
        tmp = fp.with_name(fp.name + ".track.tmp")
        tmp.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
        if (fp.stat().st_size, fp.stat().st_mtime_ns) != st0:   # 读之后被追加了：放弃这次，重读重算
            tmp.unlink(missing_ok=True)
            continue
        os.replace(tmp, fp)
        return applied
    logger.warning("[track_outcomes] signal_analytics 在回写期间持续被追加，本轮放弃回写（下轮重试）")
    return 0


def run(analytics_file: Path | None = None) -> dict:
    """
    主入口：遍历 signal_analytics.jsonl，填充未追踪记录的 outcome 并回写同一个文件。
    返回 {"processed": int, "skipped": int, "errors": int}
    """
    from config import SIGNAL_ANALYTICS_TRACK_AFTER_H, SIGNAL_ANALYTICS_FILE
    fp = analytics_file or (ROOT / SIGNAL_ANALYTICS_FILE)
    if not fp.exists():
        return {"processed": 0, "skipped": 0, "errors": 0}

    now_ms       = int(time.time() * 1000)
    min_age_ms   = SIGNAL_ANALYTICS_TRACK_AFTER_H * 3600 * 1000
    stats        = {"processed": 0, "skipped": 0, "errors": 0}

    records = []
    with open(fp, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                stats["errors"] += 1

    merged = _merge_patches(records)
    outcomes: dict = {}
    eparams: dict = {}
    for rec in records:
        out = rec.get("outcome") or {}
        # 只处理 judge_passed=True 且未追踪的记录
        if not rec.get("judge_passed"):
            continue
        if out.get("checked_at") is not None:
            stats["skipped"] += 1
            continue
        if now_ms - rec.get("ts", 0) < min_age_ms:
            stats["skipped"] += 1
            continue
        try:
            rec.setdefault("outcome", {})
            _track_one(rec)
            if rec["outcome"].get("checked_at") is not None:
                stats["processed"] += 1
                outcomes[_rec_key(rec)] = rec["outcome"]
                if _has_prices(rec):
                    eparams[_rec_key(rec)] = rec["entry_params"]
        except Exception as e:
            logger.warning(f"[track_outcomes] {rec.get('symbol')} 追踪失败: {e}")
            stats["errors"] += 1
        time.sleep(0.05)  # 防 429

    written = _write_back(fp, outcomes, eparams) if outcomes else 0
    logger.info(f"[track_outcomes] 完成: processed={stats['processed']} written={written} merged_patch={merged} "
                f"skipped={stats['skipped']} errors={stats['errors']}")
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(f"追踪完成: {result}")
