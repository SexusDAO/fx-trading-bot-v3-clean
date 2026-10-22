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


def run(analytics_file: Path | None = None) -> dict:
    """
    主入口：遍历 signal_analytics.jsonl，填充未追踪记录的 outcome。
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

    changed = False
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
            _track_one(rec)
            if rec["outcome"].get("checked_at") is not None:
                stats["processed"] += 1
                changed = True
        except Exception as e:
            logger.warning(f"[track_outcomes] {rec.get('symbol')} 追踪失败: {e}")
            stats["errors"] += 1
        time.sleep(0.05)  # 防 429

    # 回写文件
    if changed:
        with open(fp, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    logger.info(f"[track_outcomes] 完成: processed={stats['processed']} "
                f"skipped={stats['skipped']} errors={stats['errors']}")
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(f"追踪完成: {result}")
