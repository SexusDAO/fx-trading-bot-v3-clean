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
