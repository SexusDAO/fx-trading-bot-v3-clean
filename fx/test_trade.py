#!/usr/bin/env python3
"""
交易功能测试：限价开仓 / 市价开仓 / 止盈止损挂单 / 市价平仓
测试标的：BTCUSDT（流动性最好，滑点最小）
仓位：约 10U 名义（0.001 BTC @ ~80000）
"""
import sys, os, time, json
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.binance_api import (
    get_account, get_mark_price, get_precision,
    place_order, cancel_order, get_order, get_position,
    set_leverage, reset_session
)

reset_session()

SYMBOL    = "BTCUSDT"
LEVERAGE  = 10
SIDE_LONG = "BUY"
SIDE_CLOSE= "SELL"
POS_SIDE  = "LONG"

def sep(title):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print('='*55)

def check_account():
    sep("账户状态")
    acc   = get_account()
    avail = float(acc.get("availableBalance", 0))
    total = float(acc.get("totalWalletBalance", 0))
    print(f"  可用余额 : {avail:.2f} USDT")
    print(f"  钱包余额 : {total:.2f} USDT")
    mp = float(get_mark_price(SYMBOL).get("markPrice", 0))
    print(f"  BTC 标记价: ${mp:,.2f}")
    return avail, mp

def get_qty_and_price(mp, notional_usdt=10):
    prec     = get_precision(SYMBOL)
    tick     = prec.get("tick_size", prec.get("tick", 0.1))
    step     = prec.get("step_size", prec.get("step", 0.001))
    min_not  = prec.get("min_notional", 50.0)
    # 名义仓位至少满足交易所最小要求
    notional_usdt = max(notional_usdt * LEVERAGE, min_not) / LEVERAGE
    qty_raw  = notional_usdt * LEVERAGE / mp
    import math
    step_dec = len(str(step).rstrip("0").split(".")[-1]) if "." in str(step) else 0
    qty      = round(math.floor(qty_raw / step) * step, step_dec + 1)
    qty      = max(qty, step)   # 至少1个最小单位
    tick_dec = len(str(tick).rstrip("0").split(".")[-1]) if "." in str(tick) else 0
    return qty, tick, tick_dec

# ──────────────────────────────────────────────────────────────────────────────
avail, mp = check_account()
if avail < 30:
    print("❌ 余额不足 30U，终止测试")
    sys.exit(1)

# 设置杠杆
set_leverage(SYMBOL, LEVERAGE)
print(f"\n  杠杆已设置: {LEVERAGE}x")

qty, tick, tick_dec = get_qty_and_price(mp, notional_usdt=10)
print(f"  测试数量 : {qty} BTC  (≈10U名义 @{LEVERAGE}x)")

# ══════════════════════════════════════════════════════════════════════════════
# 测试 1：限价开仓（比现价低 0.3% 挂单，立刻可能成交）
# ══════════════════════════════════════════════════════════════════════════════
sep("TEST 1 — 限价开仓 (LIMIT BUY LONG)")
limit_price = round(mp * 0.997, tick_dec)   # 现价 -0.3%，作为限价挂单
print(f"  挂单价 : ${limit_price:,.2f}  (现价 {mp:,.2f} × 99.7%)")

order1 = place_order(
    symbol=SYMBOL, side=SIDE_LONG,
    order_type="LIMIT", quantity=qty,
    price=limit_price, timeInForce="GTC",
    position_side=POS_SIDE
)
oid1 = order1.get("orderId") if order1 else None
print(f"  下单结果: orderId={oid1}  status={order1.get('status') if order1 else 'FAIL'}")
if not oid1:
    print("  ❌ 限价下单失败，终止")
    sys.exit(1)
print("  ✅ 限价单提交成功")

# 查询订单状态
time.sleep(1)
od = get_order(SYMBOL, oid1)
print(f"  订单状态: {od.get('status')}  executedQty={od.get('executedQty')}")

# 撤销限价单（不等成交，继续测试市价）
cancel_order(SYMBOL, oid1)
print(f"  ✅ 限价单已撤销 (orderId={oid1})")

# ══════════════════════════════════════════════════════════════════════════════
# 测试 2：市价开仓
# ══════════════════════════════════════════════════════════════════════════════
sep("TEST 2 — 市价开仓 (MARKET BUY LONG)")
order2 = place_order(
    symbol=SYMBOL, side=SIDE_LONG,
    order_type="MARKET", quantity=qty,
    position_side=POS_SIDE
)
oid2 = order2.get("orderId") if order2 else None
# Hedge Mode 市价成交后 avgPrice 可能在 order 回报里为 0，需轮询订单获取
import time as _time
_time.sleep(1)
from shared.binance_api import get_order as _go
_od2 = _go(SYMBOL, oid2) or {}
fill_price = float(_od2.get("avgPrice") or _od2.get("price") or mp)
fill_qty   = float(_od2.get("executedQty") or qty)
print(f"  下单结果: orderId={oid2}  status={order2.get('status') if order2 else 'FAIL'}")
if not oid2:
    print("  ❌ 市价开仓失败，终止")
    sys.exit(1)
print(f"  ✅ 市价开仓成功  成交价≈${fill_price:,.2f}  qty={fill_qty}")

time.sleep(2)
# 确认持仓
positions = get_position()
pos = next((p for p in positions
            if p["symbol"] == SYMBOL and p.get("positionSide") == POS_SIDE
            and abs(float(p.get("positionAmt", 0))) > 0), None)
if pos:
    print(f"  持仓确认: amt={pos['positionAmt']}  entryPrice={pos['entryPrice']}  upnl={float(pos.get('unrealizedProfit',0)):+.4f}U")
else:
    print("  ⚠️  持仓未出现，可能延迟")

# ══════════════════════════════════════════════════════════════════════════════
# 测试 3：挂止损单 + 止盈单
# ══════════════════════════════════════════════════════════════════════════════
sep("TEST 3 — 挂止损 + 止盈")
entry = float(pos["entryPrice"]) if pos else fill_price
sl_price  = round(entry * 0.985, tick_dec)   # 止损 -1.5%
tp1_price = round(entry * 1.020, tick_dec)   # 止盈 +2.0%
print(f"  入场价: ${entry:,.2f}")
print(f"  止损价: ${sl_price:,.2f}  (-1.5%)")
print(f"  止盈价: ${tp1_price:,.2f}  (+2.0%)")

# 止损单（STOP_MARKET）
sl_order = place_order(
    symbol=SYMBOL, side=SIDE_CLOSE,
    order_type="STOP_MARKET",
    quantity=fill_qty, stop_price=sl_price,
    position_side=POS_SIDE,
    reduce_only=True
)
sl_oid = sl_order.get("orderId") if sl_order else None
print(f"  止损单: orderId={sl_oid}  status={sl_order.get('status') if sl_order else 'FAIL'}")
if sl_oid:
    print("  ✅ 止损单挂单成功")
else:
    print("  ❌ 止损单失败")

# 止盈单（TAKE_PROFIT_MARKET）
tp_order = place_order(
    symbol=SYMBOL, side=SIDE_CLOSE,
    order_type="TAKE_PROFIT_MARKET",
    quantity=fill_qty, stop_price=tp1_price,
    position_side=POS_SIDE,
    reduce_only=True
)
tp_oid = tp_order.get("orderId") if tp_order else None
print(f"  止盈单: orderId={tp_oid}  status={tp_order.get('status') if tp_order else 'FAIL'}")
if tp_oid:
    print("  ✅ 止盈单挂单成功")
else:
    print("  ❌ 止盈单失败")

# ══════════════════════════════════════════════════════════════════════════════
# 测试 4：撤销止损/止盈，市价平仓
# ══════════════════════════════════════════════════════════════════════════════
sep("TEST 4 — 撤止损止盈 + 市价平仓")
if sl_oid:
    cancel_order(SYMBOL, sl_oid)
    print(f"  ✅ 止损单已撤销 (orderId={sl_oid})")
if tp_oid:
    cancel_order(SYMBOL, tp_oid)
    print(f"  ✅ 止盈单已撤销 (orderId={tp_oid})")

time.sleep(1)
# Hedge Mode 平仓：不用 reduce_only，直接指定 positionSide 即可
close_order = place_order(
    symbol=SYMBOL, side=SIDE_CLOSE,
    order_type="MARKET", quantity=fill_qty,
    position_side=POS_SIDE
)
close_oid   = close_order.get("orderId") if close_order else None
close_price = float(close_order.get("avgPrice") or close_order.get("price") or 0)
print(f"  平仓单: orderId={close_oid}  status={close_order.get('status') if close_order else 'FAIL'}")
if close_oid:
    pnl = (close_price - entry) * fill_qty
    print(f"  ✅ 市价平仓成功  平仓价≈${close_price:,.2f}  PnL≈{pnl:+.4f}U")
else:
    print("  ❌ 市价平仓失败")

time.sleep(2)
positions2 = get_position()
pos2 = next((p for p in positions2
             if p["symbol"] == SYMBOL and p.get("positionSide") == POS_SIDE
             and abs(float(p.get("positionAmt", 0))) > 0), None)
if not pos2:
    print("  ✅ 持仓已清零，测试完成")
else:
    print(f"  ⚠️  持仓未归零: amt={pos2.get('positionAmt')}")

# ══════════════════════════════════════════════════════════════════════════════
sep("测试汇总")
results = {
    "限价单提交+撤销": "✅" if oid1 else "❌",
    "市价开仓":        "✅" if oid2 else "❌",
    "止损单挂单":      "✅" if sl_oid else "❌",
    "止盈单挂单":      "✅" if tp_oid else "❌",
    "市价平仓":        "✅" if close_oid else "❌",
    "持仓归零":        "✅" if not pos2 else "⚠️",
}
for k, v in results.items():
    print(f"  {v}  {k}")
print()
