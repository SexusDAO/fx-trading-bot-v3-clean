#!/usr/bin/env python3
"""
tools/kelly_sizer.py — Kelly 仓位动态调整（v3.71）

前提：signal_analytics.jsonl 积累每种策略/强度/市场模式的组合 ≥ KELLY_MIN_SAMPLE 笔数据。
初始关闭（KELLY_ENABLED=False），数据足够后由用户主动开启。

调用方式：
    from tools.kelly_sizer import get_kelly_size_cap
    cap = get_kelly_size_cap(mode, strength, market_mode, direction)
    # 返回 None 表示样本不足，沿用固定 cap；返回 float 表示 Kelly 最优 cap
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from statistics import mean
import sys

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger("fx.kelly_sizer")


def _load_analytics(
    mode: str, strength: str, market_mode: str, direction: str
) -> list[dict]:
    """
    从 signal_analytics.jsonl 读取符合 (mode, strength, market_mode, direction)
    且 outcome.filled=True 的记录，返回列表。
    """
    try:
        from config import SIGNAL_ANALYTICS_FILE as _sa_file
        fp = ROOT / _sa_file
    except Exception:
        fp = ROOT / "logs" / "signal_analytics.jsonl"

    if not fp.exists():
        return []

    records = []
    try:
        with open(fp, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue

                # 过滤条件
                if rec.get("mode") != mode:
                    continue
                if strength and rec.get("signal_strength") != strength:
                    continue
                if market_mode and rec.get("market_mode") != market_mode:
                    continue
                if direction and (rec.get("direction") or "").upper() != direction.upper():
                    continue

                # 只取已追踪完成的记录（[v3.89 #89] filled 存在即代表追踪完成）
                outcome = rec.get("outcome") or {}
                if outcome.get("filled") is None:
                    continue

                records.append(rec)
    except Exception as e:
        logger.debug(f"[kelly] 读取 analytics 失败: {e}")

    return records


def get_kelly_size_cap(
    mode: str,
    strength: str = "",
    market_mode: str = "",
    direction: str = "",
) -> float | None:
    """
    根据历史 signal_analytics 数据用 Kelly 公式计算最优仓位系数。

    参数：
        mode:        策略类型（pullback / breakout / reversal ...）
        strength:    信号强度（strong / medium / weak），可留空（不过滤）
        market_mode: 市场模式（bull / bear / range），可留空
        direction:   方向（LONG / SHORT），可留空

    返回：
        float  — Kelly 最优 cap（经 1/4 Kelly + [KELLY_MIN_CAP, KELLY_MAX_CAP] 约束）
        None   — 样本不足或 KELLY_ENABLED=False，调用方继续用固定 cap
    """
    # 注意：此处兜底默认值掩盖了「配置模块损坏」与「可选项缺失」的区别（见审查 #31）。
    #   本 commit 不改动该行为，仅登记；按 #31 处理时会输出实际配置来源。
    try:
        from config import (
            KELLY_ENABLED,
            KELLY_FRACTION,
            KELLY_MIN_CAP,
            KELLY_MAX_CAP,
            SIGNAL_ANALYTICS_MIN_SAMPLE,
        )
    except Exception:
        KELLY_ENABLED             = False
        KELLY_FRACTION            = 0.25
        KELLY_MIN_CAP             = 0.15
        KELLY_MAX_CAP             = 0.75
        SIGNAL_ANALYTICS_MIN_SAMPLE = 30

    if not KELLY_ENABLED:
        return None

    history = _load_analytics(mode, strength, market_mode, direction)
    filled  = [h for h in history if h.get("outcome", {}).get("filled")]

    # [v3.89 #89] 收益有效性校验：pnl_pct 必须是有限实数。
    #   旧写法 `(pnl_pct or 0) > 0` 把 None / 缺失 / 空串一律当成 0（亏损），
    #   字符串型 pnl_pct 还会在比较时抛 TypeError；进统计后再用
    #   h["outcome"]["pnl_pct"] 直接索引，缺失值继续污染均值。
    #   这里把「未知收益」单独计数：既不计入胜率分母，也不当作亏损样本，
    #   避免数据缺失变相压低 Kelly 系数（不改任何阈值与公式）。
    known, unknown = [], 0
    for h in filled:
        _v = (h.get("outcome") or {}).get("pnl_pct")
        if isinstance(_v, str):
            try:
                _v = float(_v)
            except (TypeError, ValueError):
                _v = None
        if isinstance(_v, bool) or not isinstance(_v, (int, float)):
            unknown += 1
            continue
        if _v != _v or _v in (float("inf"), float("-inf")):   # NaN / ±Inf
            unknown += 1
            continue
        h["_pnl"] = float(_v)
        known.append(h)

    if unknown:
        logger.info(
            f"[kelly] ({mode}/{strength}/{market_mode}/{direction}) "
            f"{unknown} 笔已成交记录缺少有效 pnl_pct，已排除出统计（不计为亏损）"
        )

    if len(known) < SIGNAL_ANALYTICS_MIN_SAMPLE:
        logger.debug(
            f"[kelly] ({mode}/{strength}/{market_mode}/{direction}) "
            f"有效样本 {len(known)}/{len(filled)} < {SIGNAL_ANALYTICS_MIN_SAMPLE}，跳过 Kelly"
        )
        return None

    won  = [h for h in known if h["_pnl"] > 0]
    lost = [h for h in known if h["_pnl"] <= 0]

    if not won:
        # 胜率为零，直接降到最低仓位
        logger.info(f"[kelly] ({mode}) 胜率为0，返回 KELLY_MIN_CAP={KELLY_MIN_CAP}")
        return KELLY_MIN_CAP

    p = len(won) / len(known)  # 胜率（分母只含收益有效的样本，[v3.89 #89]）

    avg_win  = mean(abs(h["_pnl"]) for h in won)
    avg_loss = mean(abs(h["_pnl"]) for h in lost) if lost else avg_win

    if avg_loss < 1e-9:
        avg_loss = avg_win  # 防零除

    b = avg_win / avg_loss  # 赔率（盈亏比）

    # Kelly 公式: f* = (p*b - (1-p)) / b
    kelly_f = (p * b - (1 - p)) / b
    if kelly_f <= 0:
        logger.info(
            f"[kelly] ({mode}) Kelly={kelly_f:.3f} ≤ 0（负期望），"
            f"返回 KELLY_MIN_CAP={KELLY_MIN_CAP}"
        )
        return KELLY_MIN_CAP

    # 1/4 Kelly 保守系数
    conservative_f = kelly_f * KELLY_FRACTION

    # 约束到合理范围
    result = max(KELLY_MIN_CAP, min(KELLY_MAX_CAP, conservative_f))

    logger.info(
        f"[kelly] ({mode}/{strength}/{market_mode}/{direction}) "
        f"n={len(known)}/{len(filled)} p={p:.2f} b={b:.2f} "
        f"kelly_f={kelly_f:.3f} × {KELLY_FRACTION} → {conservative_f:.3f} "
        f"→ clamp [{KELLY_MIN_CAP},{KELLY_MAX_CAP}] = {result:.3f}"
    )
    return result


if __name__ == "__main__":
    # 简单测试
    logging.basicConfig(level=logging.INFO)
    for _mode in ("pullback", "breakout", "reversal"):
        cap = get_kelly_size_cap(_mode)
        print(f"{_mode}: {cap}")
