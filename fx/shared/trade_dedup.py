"""trade_log 平仓记录的「同一笔」判定（纯函数，无网络/IO依赖）。

规则：trade_id 相同 → 同一笔；否则 symbol 相同、方向相同（任一方缺失方向则不比较）、
close_time 相差不超过 TOL_MS（默认 5 秒）→ 同一笔。没有有效 close_time 的记录（如开仓记录）不参与。
"""
from __future__ import annotations

TOL_MS = 5000


def _f(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def close_ms(rec: dict) -> float:
    return _f(rec.get("close_time"))


def _dir(rec: dict) -> str:
    return str(rec.get("direction") or "").lower()


def same_trade(a: dict, b: dict, tol_ms: int = TOL_MS) -> bool:
    ta, tb = a.get("trade_id"), b.get("trade_id")
    if ta and tb and ta == tb:
        return True
    if a.get("symbol") != b.get("symbol"):
        return False
    ca, cb = close_ms(a), close_ms(b)
    if ca <= 0 or cb <= 0:
        return False
    da, db = _dir(a), _dir(b)
    if da and db and da != db:
        return False
    return abs(ca - cb) <= tol_ms


def dedup(records: list, tol_ms: int = TOL_MS) -> tuple:
    """返回 (去重后记录, 重复条数)。保留先出现者；顺序不变。仅比较同 symbol 的记录。"""
    kept: list = []
    by_sym: dict = {}
    dups = 0
    for r in records:
        sym = r.get("symbol")
        if any(same_trade(r, k, tol_ms) for k in by_sym.get(sym, ())):
            dups += 1
            continue
        kept.append(r)
        by_sym.setdefault(sym, []).append(r)
    return kept, dups
