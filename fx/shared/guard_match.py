"""交易所保护单（止损 Algo 单 / 止盈限价单）按属性匹配的纯函数。

用途：孤儿仓位建档（main._sync_positions_on_startup）与补挂自愈（position_manager._补挂保护单）共用，
从交易所已有挂单里读出止损价/止盈价与订单 id，避免本地记录 sl=0 → 反复补挂 → 卡 pending_guard。
只做匹配，不发任何请求。
"""
from __future__ import annotations


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _close_side(direction: str) -> str:
    return "SELL" if str(direction).upper() == "LONG" else "BUY"


def find_exchange_sl(algo_orders, symbol: str, direction: str) -> list:
    """在 Algo 挂单里找该标的/方向的止损单（STOP_MARKET/STOP、平仓方向、positionSide 一致、有触发价和 algoId）。
    返回匹配列表：0 个=无，1 个=可认领，多个=歧义（调用方不得自动选择）。"""
    pos_side = str(direction).upper()
    close_side = _close_side(direction)
    hits = []
    for o in algo_orders or []:
        if not isinstance(o, dict) or o.get("symbol") != symbol:
            continue
        ps = o.get("positionSide")
        if ps and ps not in (pos_side, "BOTH"):
            continue
        if o.get("side") != close_side:
            continue
        if (o.get("orderType") or o.get("type")) not in ("STOP_MARKET", "STOP"):
            continue
        if not o.get("algoId") or _f(o.get("triggerPrice", o.get("stopPrice"))) <= 0:
            continue
        hits.append(o)
    return hits


def sl_fields(order: dict) -> dict:
    """止损单 → pos_state 字段。"""
    return {"sl_price": _f(order.get("triggerPrice", order.get("stopPrice"))),
            "sl_order_id": int(order["algoId"])}


def find_exchange_tps(open_orders, symbol: str, direction: str) -> list:
    """在普通挂单里找该标的/方向的止盈限价单（LIMIT、平仓方向、positionSide 一致），按「离入场近 → 远」排序：
    LONG 价格升序，SHORT 价格降序（与 position_manager 补挂路径的排序规则一致）。"""
    pos_side = str(direction).upper()
    close_side = _close_side(direction)
    tps = []
    for o in open_orders or []:
        if not isinstance(o, dict) or o.get("symbol") != symbol:
            continue
        if str(o.get("positionSide", "")).upper() != pos_side:
            continue
        if o.get("type") != "LIMIT" or o.get("side") != close_side:
            continue
        if _f(o.get("price")) <= 0 or not o.get("orderId"):
            continue
        tps.append(o)
    return sorted(tps, key=lambda o: _f(o.get("price")), reverse=(pos_side == "SHORT"))


def tp_fields(tps: list) -> dict:
    """止盈单列表 → pos_state 字段：第 1 个 → tp1_*，第 2 个 → tp2_*；只有 1 个则不写 tp2（单 TP 模式）。"""
    out = {}
    if len(tps) >= 1:
        out.update({"tp1_order_id": int(tps[0]["orderId"]), "tp1_price": _f(tps[0].get("price")),
                    "tp1_quantity": _f(tps[0].get("origQty"))})
    if len(tps) >= 2:
        out.update({"tp2_order_id": int(tps[1]["orderId"]), "tp2_price": _f(tps[1].get("price")),
                    "tp2_quantity": _f(tps[1].get("origQty"))})
    return out


def assign_tps_by_price(orders: list, tp1_price, tp2_price, direction: str) -> tuple:
    """把交易所已有的止盈限价单按「本地 tp1_price/tp2_price 最接近」分配给 tp1/tp2 两个槽位。

    返回 (tp1_order|None, tp2_order|None, by_price)。
      · 本地价格只用于选择，不修改任何价格；每张单只分给一个槽位，每个槽位至多一张；
      · 只传了一个本地价格（另一个为 None/0）则只填那个槽位（如 tp1 已成交、只剩 tp2；或单 TP 模式）；
      · 两个本地价格都没有 → 退回旧规则：按价格「离入场近→远」排序，第 1 张→tp1、第 2 张→tp2，by_price=False（调用方不应据此补挂）。
    """
    valid = [o for o in orders or [] if isinstance(o, dict) and o.get("orderId") and _f(o.get("price")) > 0]
    p1, p2 = _f(tp1_price), _f(tp2_price)
    if p1 <= 0 and p2 <= 0:
        s = sorted(valid, key=lambda o: _f(o.get("price")), reverse=(str(direction).upper() == "SHORT"))
        return (s[0] if s else None), (s[1] if len(s) > 1 else None), False
    pairs = []
    for i, o in enumerate(valid):
        for slot, p in ((1, p1), (2, p2)):
            if p > 0:
                pairs.append((abs(_f(o.get("price")) - p), slot, i))
    pairs.sort()
    got, used = {}, set()
    for _d, slot, i in pairs:
        if slot in got or i in used:
            continue
        got[slot] = valid[i]
        used.add(i)
    return got.get(1), got.get(2), True
