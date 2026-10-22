#!/usr/bin/env python3
"""
tools/track_rejected.py — 拒绝信号事后追踪（对照组）

对 judge_passed=false 的记录，统计信号产生后 8H 内价格的「方向触达率」，
仅作为观察证据（[v3.89 #45]：不输出「误拒/拒绝合理」之类的因果结论）。

「方向触达」定义：
  - SHORT：参考价之后 8H 内最大跌幅 >= 2%
  - LONG ：参考价之后 8H 内最大涨幅 >= 2%
注意：它不检验候选入场价是否被触达、先后路径，也不代表成交或盈亏。
[v3.89 #44] 只有 K 线完整覆盖 8H 窗口的记录才计入样本分母；数据不足/未成熟单独计数。
"""
import sys, json, time, logging
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger("fx.track_rejected")

TRACK_WINDOW_H  = 8     # 追踪窗口
CORRECT_MOVE    = 2.0   # 方向触达幅度门槛（%）
KLINE_MS        = 15 * 60 * 1000   # [v3.89 #44] 15m K 线间隔

# [v3.89 #46] 方向必须是明确枚举，其余值一律按 invalid 处理，
#   不得落入 LONG 分支（旧实现任何非空且非 SHORT 的值都会走 LONG）。
VALID_DIRECTIONS = ("LONG", "SHORT")


def _get_klines(symbol, start_ms, limit=32):
    try:
        import requests, os
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        proxies = {"https": proxy, "http": proxy} if proxy else None
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": symbol, "interval": "15m",
                    "startTime": start_ms, "limit": limit},
            timeout=8, proxies=proxies,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.debug(f"[track_rejected] {symbol} K线失败: {e}")
        return []


def _validate_window(klines: list, start_ms: int, window_end: int, now_ms: int) -> str | None:
    """
    [v3.89 #44] 校验 K 线对观察窗口 [start_ms, window_end] 的覆盖。
    返回 None 表示完整；否则返回原因：
      "immature"      窗口尚未结束，或窗口末端的 K 线还没收盘
      "insufficient"  K 线缺失 / 不连续 / 起点或末端覆盖不足
    """
    if now_ms < window_end + KLINE_MS:
        return "immature"
    try:
        opens = [int(k[0]) for k in klines]
    except Exception:
        return "insufficient"
    if not opens:
        return "insufficient"
    # 起点：第一根 K 线的开盘时间不能比信号时间晚超过一根
    if opens[0] - start_ms > KLINE_MS:
        return "insufficient"
    # 连续性：相邻 K 线间隔必须恰好一根
    for a, b in zip(opens, opens[1:]):
        if b - a != KLINE_MS:
            return "insufficient"
    # 末端：最后一根 K 线需覆盖到窗口结束
    if opens[-1] + KLINE_MS < window_end:
        return "insufficient"
    return None


# [v3.89 #47] 行情请求合并缓存：key=(symbol, start_ms, limit)。
#   同一次 run() 内相同标的、相同追踪窗口的记录复用同一份 K 线，
#   节流（time.sleep）只在真正发出请求时执行，不再按记录条数计。
_KLINE_CACHE: dict = {}


def _get_klines_cached(symbol: str, start_ms: int, limit: int) -> list:
    key = (symbol, start_ms, limit)
    if key not in _KLINE_CACHE:
        _KLINE_CACHE[key] = _get_klines(symbol, start_ms, limit=limit)
        time.sleep(0.03)   # [v3.89 #47] 节流按实际发出的请求数计
    return _KLINE_CACHE[key]


def _clear_kline_cache() -> None:
    """每次 run() 开始时清空，避免跨批次复用过期行情。"""
    _KLINE_CACHE.clear()


def _check_rejected_outcome(rec: dict) -> dict:
    """
    追踪一条拒绝记录。始终返回 dict，字段 status：
      complete      窗口完整，附带 direction_touched / max_move_pct
      immature      窗口未结束
      insufficient  K 线数据不足或有缺口（不纳入完成样本分母）
      invalid       记录缺参考价或方向
    [v3.89 #45] direction_touched 仅表示「参考价之后的最大有利波动达到门槛」，
    不检验候选入场是否触达、先后路径，也不代表成交或盈亏。
    """
    direction = (rec.get("direction") or "").strip().upper()
    # [v3.89 #46] 校验方向枚举：非法/未知方向单独计为 invalid，不再默认走 LONG
    if direction not in VALID_DIRECTIONS:
        return {"status": "invalid", "reason": "bad_direction"}

    snap = rec.get("snapshot") or {}
    ref_price = snap.get("price")
    # [v3.89 #46] 参考价需为正有限数，否则后续 (hi-ref)/ref 会除零或失真
    try:
        ref_price = float(ref_price)
    except (TypeError, ValueError):
        return {"status": "invalid", "reason": "bad_ref_price"}
    if not (ref_price > 0) or ref_price != ref_price or ref_price in (float("inf"), float("-inf")):
        return {"status": "invalid", "reason": "bad_ref_price"}

    # [v3.89 #46] ts / symbol 由直接索引改为安全取值，缺字段的记录不再中断整份报告
    try:
        ts = int(rec.get("ts"))
    except (TypeError, ValueError):
        return {"status": "invalid", "reason": "bad_ts"}
    symbol = rec.get("symbol")
    if not symbol or not isinstance(symbol, str):
        return {"status": "invalid", "reason": "bad_symbol"}

    window_end = ts + TRACK_WINDOW_H * 3600 * 1000
    # [v3.89 #47] 走合并缓存（同标的+同窗口只请求一次），节流按实际请求数
    klines = _get_klines_cached(symbol, ts, TRACK_WINDOW_H * 4)
    if not klines:
        return {"status": "insufficient", "reason": "no_klines"}
    klines = [k for k in klines if int(k[0]) < window_end]
    why = _validate_window(klines, ts, window_end, int(time.time() * 1000))
    if why:
        return {"status": why}

    max_up_pct   = 0.0
    max_down_pct = 0.0
    for k in klines:
        hi = float(k[2])
        lo = float(k[3])
        max_up_pct   = max(max_up_pct,   (hi - ref_price) / ref_price * 100)
        max_down_pct = max(max_down_pct, (ref_price - lo) / ref_price * 100)

    if direction == "SHORT":
        touched = max_down_pct >= CORRECT_MOVE
        max_move = max_down_pct
    else:
        touched = max_up_pct >= CORRECT_MOVE
        max_move = max_up_pct

    return {"status": "complete", "direction_touched": touched, "max_move_pct": round(max_move, 2)}


def run(analytics_file: Path | None = None, days: int = 7) -> dict:
    """
    主入口：统计各拒绝原因的「方向正确率」，返回分析 dict。
    """
    from config import SIGNAL_ANALYTICS_FILE
    fp = analytics_file or (ROOT / SIGNAL_ANALYTICS_FILE)
    if not fp.exists():
        return {}

    now_ms     = int(time.time() * 1000)
    cutoff_ms  = now_ms - days * 86400 * 1000
    min_age_ms = TRACK_WINDOW_H * 3600 * 1000  # 至少等 8H 才追踪

    rejected_recs = []
    with open(fp, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("judge_passed"):
                continue
            if rec.get("ts", 0) < cutoff_ms:
                continue
            if now_ms - rec.get("ts", 0) < min_age_ms:
                continue
            rejected_recs.append(rec)

    # [v3.89 #47] 每次重跑清空行情缓存，仅在同一次分析内复用相同窗口的 K 线
    _clear_kline_cache()

    # 按拒绝原因分组统计
    groups: dict[str, list] = defaultdict(list)       # 仅 complete
    skipped: dict[str, Counter] = defaultdict(Counter)  # immature / insufficient / invalid
    for rec in rejected_recs:
        reason = rec.get("reject_reason") or "unknown"
        # 归类：取前半段（去掉动态数值部分）
        import re
        key = re.sub(r'[=:：].*', '', reason).strip()[:60]
        # [v3.89 #46] 逐条隔离：单条记录的字段类型异常不得中断整份报告，
        #   记为 invalid 并继续处理后续记录。
        try:
            result = _check_rejected_outcome(rec)
        except Exception as e:
            logger.debug(f"[track_rejected] 记录处理失败(t={rec.get('ts')}): {e}")
            result = {"status": "invalid", "reason": "exception"}
        if result.get("status") == "complete":
            groups[key].append(result)
        else:
            skipped[key][result.get("status", "invalid")] += 1

    report = {}
    for reason in sorted(set(groups) | set(skipped), key=lambda k: -(len(groups.get(k, [])))):
        results = groups.get(reason, [])
        n       = len(results)       # [v3.89 #44] 分母只含完整窗口样本
        n_touch = sum(1 for r in results if r["direction_touched"])
        sk      = skipped.get(reason, Counter())
        report[reason] = {
            "samples":             n,
            "direction_touch_pct": round(n_touch / n * 100, 1) if n else None,
            "avg_move_pct":        round(sum(r["max_move_pct"] for r in results) / n, 2) if n else None,
            "immature":            sk.get("immature", 0),
            "insufficient":        sk.get("insufficient", 0),
            "invalid":             sk.get("invalid", 0),
        }

    logger.info(f"[track_rejected] 分析完成: {len(rejected_recs)} 条拒绝记录，{len(report)} 种原因")
    return report


def print_report(report: dict) -> None:
    print(f"\n{'═'*74}")
    print(f"  被拒绝信号的事后表现（方向触达率，仅作观察证据）")
    print(f"{'═'*74}")
    print(f"  {'拒绝原因':<35} {'样本':>4} {'方向触达率':>9} {'平均幅度':>8} {'数据不足':>6} {'未成熟':>5}")
    print(f"  {'-'*72}")
    for reason, d in report.items():
        rate = f"{d['direction_touch_pct']:>8.0f}%" if d["direction_touch_pct"] is not None else f"{'-':>9}"
        mv   = f"{d['avg_move_pct']:>7.2f}%" if d["avg_move_pct"] is not None else f"{'-':>8}"
        print(f"  {reason:<35} {d['samples']:>4} {rate} {mv} {d['insufficient']:>6} {d['immature']:>5}")
    print("  说明：方向触达率 = 参考价之后 8H 内最大有利波动 ≥ 2% 的样本占比。")
    print("        仅为观察证据：不检验候选入场是否触达，也不代表成交、盈利或拒绝规则对错；")
    print("        样本分母只含窗口完整的记录，数据不足/未成熟的单独列出。")
    print(f"{'═'*74}\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    rpt = run(days=7)
    print_report(rpt)
