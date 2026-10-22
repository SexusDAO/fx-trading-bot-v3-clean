"""
shared/opt_v385.py — v3.85 第二轮优化的纯函数（无 I/O，便于单测）
"""
from __future__ import annotations


def symbol_funding_window(now_s: float, next_funding_s: float, interval_h: float,
                          pre_s: float, post_s: float) -> tuple[bool, str]:
    """
    按「该标的自己的」下次结算时间判断是否处于结算前/后禁开仓窗口。
    now_s / next_funding_s 为 Unix 秒；interval_h 为结算周期（1/4/8 小时）。
    next_funding_s<=0 时返回 (False, "")，由调用方回退到旧的全局整点逻辑。
    """
    if not next_funding_s or next_funding_s <= 0:
        return False, ""
    period = max(interval_h, 1) * 3600
    # [v3.89 #72] next_funding_s 已在过去（缓存的 premiumIndex 刚跨过结算点）：按整数个结算周期向后归一化，
    #   使「上次结算」取到真实刚发生的那次，否则会用「过期的下次结算 - 一个周期」把结算后窗口算到一个周期之前。
    #   窗口大小不变；now_s 恰好等于 next_funding_s 时仍按「结算前窗口（to_next=0）」处理，与旧行为一致。
    stale = False
    if now_s > next_funding_s:
        k = int((now_s - next_funding_s) // period) + 1
        next_funding_s += k * period
        stale = True
    to_next = next_funding_s - now_s
    if 0 <= to_next <= pre_s:
        return True, f"距本标的结算还有 {to_next/60:.0f} 分钟（窗口 {pre_s/60:.0f} 分钟）" + ("（快照已过期，按周期归一化）" if stale else "")
    last = next_funding_s - period
    since = now_s - last
    if 0 <= since <= post_s:
        return True, f"本标的结算后 {since/60:.0f} 分钟（窗口 {post_s/60:.0f} 分钟）" + ("（快照已过期，按周期归一化）" if stale else "")
    return False, ""
